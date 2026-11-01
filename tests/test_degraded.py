"""A Redis shard goes down: the circuit breaker, and the gateway's local / open / closed policies."""
import asyncio

import fakeredis
import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from rlg import gateway
from rlg.breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker
from rlg.redis_limiter import RedisLimiter, ShardUnavailable


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_breaker_opens_after_threshold_and_probes_once_when_half_open():
    clk = Clock()
    b = CircuitBreaker(threshold=3, reset_after=5, clock=clk)
    for _ in range(2):
        assert b.allow()
        b.failure()
    assert b.state == CLOSED                      # 2 failures < threshold
    b.failure()
    assert b.state == OPEN and not b.allow() and b.trips == 1
    clk.t = 5
    assert b.state == HALF_OPEN
    assert b.allow() and not b.allow()            # exactly one probe at a time
    b.failure()                                   # the probe failed: open for another reset period
    assert b.state == OPEN
    clk.t = 9.9
    assert not b.allow()
    clk.t = 10
    assert b.allow()
    b.success()
    assert b.state == CLOSED and b.allow() and b.failures == 0


def test_a_success_resets_the_failure_count():
    b = CircuitBreaker(threshold=2, reset_after=1, clock=Clock())
    b.failure()
    b.success()
    b.failure()
    assert b.state == CLOSED


def _shards():
    return {f"redis-{i}": fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), decode_responses=True)
            for i in range(3)}


def kill(lim: RedisLimiter, shard: str) -> list[int]:
    """Make every script call to `shard` fail like a dead server; returns a list counting the attempts."""
    calls = []

    async def dead(**_):
        calls.append(1)
        raise RedisConnectionError("connection refused")
    lim._tb[shard] = dead
    lim._sw[shard] = dead
    return calls


def key_on(lim: RedisLimiter, shard: str) -> str:
    return next(k for k in (f"user-{i}" for i in range(10_000)) if lim.shard_for(k) == shard)


def test_open_breaker_fails_fast_without_calling_the_dead_shard():
    lim = RedisLimiter(_shards(), breakers={f"redis-{i}": CircuitBreaker(3, 60) for i in range(3)})
    calls = kill(lim, "redis-1")
    key = key_on(lim, "redis-1")

    async def run():
        for _ in range(10):
            with pytest.raises(ShardUnavailable):
                await lim.token_bucket(key, 5, 1)
        return await lim.token_bucket(key_on(lim, "redis-2"), 5, 1)    # other shards unaffected
    ok = asyncio.run(run())
    assert len(calls) == 3                        # after 3 failures the shard isn't even tried
    assert ok.allowed


def make_app(mode: str, replicas: int = 2):
    shards = _shards()
    st = gateway.State.__new__(gateway.State)
    st.limiter = RedisLimiter(shards)
    st.meta = shards["redis-0"]
    st.plans = {"free": {"algo": "token_bucket", "capacity": 10, "rate": 0.001},
                "pro": {"algo": "sliding_window", "limit": 6, "window": 60}}
    st.cache = gateway.LRUCache(100, ttl=30)
    st.bloom = gateway.BloomFilter(1000, 0.001)
    st.lookups = {}
    st.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, text="ok")),
                                base_url="http://up")
    st.configure_degraded(mode, replicas)
    return st


async def _drive(st, keys_and_counts):
    original = gateway.State
    gateway.State = lambda: st
    try:
        application = gateway.create_app()
    finally:
        gateway.State = original
    out = {}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://gw") as c:
        for key, n in keys_and_counts:
            out[key] = [await c.get("/api/x", headers={"x-api-key": key}) for _ in range(n)]
        out["healthz"] = (await c.get("/healthz")).json()
    return out


@pytest.mark.parametrize("plan", ["free", "pro"])
def test_local_mode_enforces_this_replicas_share_and_marks_responses(plan):
    st = make_app("local", replicas=2)
    dead_key = key_on(st.limiter, "redis-2")
    live_key = key_on(st.limiter, "redis-1")

    async def run():
        await st.meta.hset("api_keys", mapping={dead_key: plan, live_key: plan})
        kill(st.limiter, "redis-2")
        return await _drive(st, [(dead_key, 12), (live_key, 12)])
    out = asyncio.run(run())
    share = 5 if plan == "free" else 3                         # ceil(limit / 2 replicas)
    limit = 10 if plan == "free" else 6
    dead = out[dead_key]
    assert [r.status_code for r in dead] == [200] * share + [429] * (12 - share)
    assert all(r.headers.get("X-RateLimit-Degraded") == "local" for r in dead)
    live = out[live_key]
    assert [r.status_code for r in live] == [200] * limit + [429] * (12 - limit)   # exact, through Redis
    assert all("X-RateLimit-Degraded" not in r.headers for r in live)
    assert out["healthz"]["ok"] and out["healthz"]["shards"]["redis-2"] == OPEN


def test_open_mode_allows_and_closed_mode_refuses():
    for mode, code in (("open", 200), ("closed", 503)):
        st = make_app(mode)
        key = key_on(st.limiter, "redis-2")

        async def run(st=st, key=key):
            await st.meta.hset("api_keys", mapping={key: "free"})
            kill(st.limiter, "redis-2")
            return await _drive(st, [(key, 15)])
        res = asyncio.run(run())[key]
        assert {r.status_code for r in res} == {code}, mode
        if mode == "open":
            assert res[0].headers["X-RateLimit-Degraded"] == "open"
        else:
            assert res[0].headers["Retry-After"] == "5"


def test_meta_shard_down_never_lets_an_unverified_key_through():
    st = make_app("open")                                      # even in fail-open mode

    async def run():
        await st.meta.hset("api_keys", mapping={"cached": "free"})
        st.cache.put("cached", "free")

        async def boom(*_a, **_k):
            raise RedisConnectionError("down")
        st.meta.hget = boom
        st.meta.sismember = boom
        return await _drive(st, [("cached", 1), ("never-seen", 1)])
    out = asyncio.run(run())
    assert out["never-seen"][0].status_code == 503             # can't authenticate: refuse, don't guess
    # a key already in the cache still has a plan, but the blocklist can't be checked when the Bloom filter is empty
    # (no "maybe"), so it goes on to the limiter
    assert out["cached"][0].status_code == 200


def test_invalid_fail_mode_is_rejected():
    st = make_app("local")
    with pytest.raises(ValueError):
        st.configure_degraded("sometimes", 1)


def test_concurrent_cache_misses_share_one_redis_lookup():
    st = make_app("local")

    async def run():
        await st.meta.hset("api_keys", mapping={"burst": "free"})
        calls = []
        real = st.meta.hget

        async def counting(*a, **k):
            calls.append(1)
            await asyncio.sleep(0.05)              # a slow lookup: everyone else arrives while it's in flight
            return await real(*a, **k)
        st.meta.hget = counting
        plans = await asyncio.gather(*(st.plan_for("burst") for _ in range(100)))
        return plans, len(calls)
    plans, calls = asyncio.run(run())
    assert plans == ["free"] * 100 and calls == 1


def test_a_failed_shared_lookup_fails_every_waiter_and_is_retried():
    st = make_app("local")

    async def run():
        async def boom(*_a, **_k):
            await asyncio.sleep(0.01)
            raise RedisConnectionError("down")
        st.meta.hget = boom
        res = await asyncio.gather(*(st.plan_for("k") for _ in range(10)), return_exceptions=True)
        return res, st.lookups
    res, pending = asyncio.run(run())
    assert all(isinstance(r, RedisConnectionError) for r in res) and pending == {}   # nothing left stuck
