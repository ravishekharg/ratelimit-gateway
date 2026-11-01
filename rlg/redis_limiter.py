"""Distributed rate limiting: every gateway replica shares counters in Redis.

Each check is ONE Lua script, executed atomically by Redis, so concurrent requests on different replicas can
never both take the last token (no read-modify-write race). The script reads the clock with redis.call('TIME'),
so replicas with skewed clocks still agree. Keys are spread over several standalone Redis shards with the
consistent-hash ring (hashring.py); with Redis Cluster you would use hash tags instead.

Each shard has a circuit breaker (breaker.py). A call to a shard whose breaker is open, or a call that fails
(connection error, timeout), raises ShardUnavailable at once; the gateway then applies its degraded-mode policy.
"""
from __future__ import annotations

import logging
import os

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from rlg.breaker import CircuitBreaker
from rlg.hashring import HashRing
from rlg.limiters import Decision

log = logging.getLogger("rlg")

TOKEN_BUCKET = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = (cost - tokens) / rate
end
redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)
return {allowed, math.floor(tokens), tostring(retry)}
"""

SLIDING_WINDOW = """
local prefix = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local idx = math.floor(now / window)
local cur_key = prefix .. ':' .. idx
local cur = tonumber(redis.call('GET', cur_key) or '0')
local prev = tonumber(redis.call('GET', prefix .. ':' .. (idx - 1)) or '0')
local overlap = 1 - (now - idx * window) / window
local est = cur + prev * overlap
if est + 1 <= limit then
  redis.call('INCR', cur_key)
  redis.call('EXPIRE', cur_key, math.ceil(window * 2))
  return {1, math.floor(limit - est - 1), '0'}
end
return {0, 0, tostring((idx + 1) * window - now)}
"""


class ShardUnavailable(Exception):
    def __init__(self, shard: str, reason: str) -> None:
        self.shard = shard
        super().__init__(f"{shard}: {reason}")


class RedisLimiter:
    def __init__(self, shards: dict[str, aioredis.Redis], vnodes: int = 160,
                 breakers: dict[str, CircuitBreaker] | None = None) -> None:
        self.shards = shards
        self.ring = HashRing(list(shards), vnodes)
        self.breakers = breakers or {name: CircuitBreaker() for name in shards}
        self._tb = {name: r.register_script(TOKEN_BUCKET) for name, r in shards.items()}
        self._sw = {name: r.register_script(SLIDING_WINDOW) for name, r in shards.items()}

    @classmethod
    def from_urls(cls, urls: list[str]) -> RedisLimiter:
        # short timeouts: a rate-limit check that waits seconds for a dead shard is worse than a degraded answer
        timeout = float(os.environ.get("REDIS_TIMEOUT", "1.0"))
        threshold = int(os.environ.get("BREAKER_THRESHOLD", "3"))
        reset = float(os.environ.get("BREAKER_RESET_SECONDS", "5"))
        # a bounded, blocking pool per shard: a burst queues for a connection (up to POOL_WAIT_SECONDS) instead of
        # opening hundreds of new connections at once, which is what made calls time out under load
        size = int(os.environ.get("REDIS_POOL_SIZE", "32"))
        wait = float(os.environ.get("POOL_WAIT_SECONDS", "5"))
        clients = {u: aioredis.Redis(connection_pool=aioredis.BlockingConnectionPool.from_url(
            u, decode_responses=True, socket_timeout=timeout, socket_connect_timeout=timeout,
            max_connections=size, timeout=wait)) for u in urls}
        return cls(clients, breakers={u: CircuitBreaker(threshold, reset) for u in urls})

    def shard_for(self, key: str) -> str:
        return self.ring.get(key)

    async def _run(self, scripts: dict, prefix: str, key: str, args: list) -> Decision:
        shard = self.shard_for(key)
        breaker = self.breakers[shard]
        if not breaker.allow():
            raise ShardUnavailable(shard, "circuit open")
        probing = breaker.opened_at is not None
        try:
            allowed, remaining, retry = await scripts[shard](keys=[f"{prefix}:{key}"], args=args)
        except (RedisError, OSError) as e:
            was_open = breaker.opened_at is not None
            breaker.failure()
            if not was_open and breaker.opened_at is not None:
                log.warning("circuit OPEN for %s after %d failures (last: %s: %s)", shard, breaker.threshold,
                            type(e).__name__, e)
            raise ShardUnavailable(shard, type(e).__name__) from e
        breaker.success()
        if probing:
            log.warning("circuit CLOSED for %s: probe succeeded", shard)
        return Decision(bool(int(allowed)), int(remaining), float(retry))

    async def token_bucket(self, key: str, capacity: int, rate: float, cost: int = 1) -> Decision:
        return await self._run(self._tb, "tb", key, [capacity, rate, cost])

    async def sliding_window(self, key: str, limit: int, window: float) -> Decision:
        return await self._run(self._sw, "sw", key, [limit, window])

    async def close(self) -> None:
        for r in self.shards.values():
            await r.aclose()
