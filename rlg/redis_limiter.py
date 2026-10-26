"""Distributed rate limiting: every gateway replica shares counters in Redis.

Each check is ONE Lua script, executed atomically by Redis, so concurrent requests on different replicas can
never both take the last token (no read-modify-write race). The script reads the clock with redis.call('TIME'),
so replicas with skewed clocks still agree. Keys are spread over several standalone Redis shards with the
consistent-hash ring (hashring.py); with Redis Cluster you would use hash tags instead.
"""
from __future__ import annotations

import redis.asyncio as aioredis

from rlg.hashring import HashRing
from rlg.limiters import Decision

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


class RedisLimiter:
    def __init__(self, shards: dict[str, aioredis.Redis], vnodes: int = 160) -> None:
        self.shards = shards
        self.ring = HashRing(list(shards), vnodes)
        self._tb = {name: r.register_script(TOKEN_BUCKET) for name, r in shards.items()}
        self._sw = {name: r.register_script(SLIDING_WINDOW) for name, r in shards.items()}

    @classmethod
    def from_urls(cls, urls: list[str]) -> RedisLimiter:
        return cls({u: aioredis.from_url(u, decode_responses=True) for u in urls})

    def shard_for(self, key: str) -> str:
        return self.ring.get(key)

    async def token_bucket(self, key: str, capacity: int, rate: float, cost: int = 1) -> Decision:
        shard = self.shard_for(key)
        allowed, remaining, retry = await self._tb[shard](keys=[f"tb:{key}"], args=[capacity, rate, cost])
        return Decision(bool(int(allowed)), int(remaining), float(retry))

    async def sliding_window(self, key: str, limit: int, window: float) -> Decision:
        shard = self.shard_for(key)
        allowed, remaining, retry = await self._sw[shard](keys=[f"sw:{key}"], args=[limit, window])
        return Decision(bool(int(allowed)), int(remaining), float(retry))

    async def close(self) -> None:
        for r in self.shards.values():
            await r.aclose()
