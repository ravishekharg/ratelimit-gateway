"""Lua scripts and the FastAPI gateway against fakeredis (a real Lua interpreter via lupa) and a mock upstream."""
import asyncio

import fakeredis
import httpx
import pytest

from rlg import gateway
from rlg.redis_limiter import RedisLimiter


@pytest.fixture
def shards():
    """Three independent in-process Redis servers (fakeredis runs the Lua scripts with a real Lua VM)."""
    return {f"redis-{i}": fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer(), decode_responses=True)
            for i in range(3)}


async def _limiter(shards):
    return RedisLimiter(shards)


def test_token_bucket_lua_is_exact_under_concurrency(shards):
    async def run():
        lim = await _limiter(shards)
        results = await asyncio.gather(*(lim.token_bucket("user-1", capacity=25, rate=0.0001) for _ in range(100)))
        return sum(d.allowed for d in results), [d.retry_after for d in results if not d.allowed]
    allowed, retries = asyncio.run(run())
    assert allowed == 25
    assert all(r > 0 for r in retries)


def test_sliding_window_lua(shards):
    async def run():
        lim = await _limiter(shards)
        return [(await lim.sliding_window("user-2", limit=5, window=60)).allowed for _ in range(7)]
    assert asyncio.run(run()) == [True] * 5 + [False] * 2


def test_keys_are_spread_over_shards(shards):
    async def run():
        lim = await _limiter(shards)
        return {lim.shard_for(f"user-{i}") for i in range(200)}
    assert len(asyncio.run(run())) == 3


@pytest.fixture
def app(shards, monkeypatch):
    st = gateway.State.__new__(gateway.State)
    lim = RedisLimiter(shards)
    st.limiter, st.meta = lim, lim.shards["redis-0"]
    st.plans = {"free": {"algo": "token_bucket", "capacity": 3, "rate": 0.001},
                "pro": {"algo": "sliding_window", "limit": 5, "window": 60}}
    st.cache = gateway.LRUCache(100, ttl=30)
    st.bloom = gateway.BloomFilter(1000, 0.001)

    def upstream(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"path": request.url.path, "q": request.url.query.decode()})
    st.http = httpx.AsyncClient(transport=httpx.MockTransport(upstream), base_url="http://up")
    monkeypatch.setattr(gateway, "State", lambda: st)
    return gateway.create_app(), st


def test_gateway_end_to_end(app):
    application, st = app

    async def run():
        await st.meta.hset("api_keys", mapping={"k-free": "free", "k-pro": "pro", "k-bad": "free"})
        await st.meta.sadd("blocked_keys", "k-bad")
        await st.refresh_blocklist()
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as c:
            unknown = await c.get("/api/orders", headers={"x-api-key": "nope"})
            blocked = await c.get("/api/orders", headers={"x-api-key": "k-bad"})
            free = [await c.get("/api/orders", params={"id": 7}, headers={"x-api-key": "k-free"}) for _ in range(4)]
            pro = [await c.get("/api/orders", headers={"x-api-key": "k-pro"}) for _ in range(6)]
        return unknown, blocked, free, pro

    unknown, blocked, free, pro = asyncio.run(run())
    assert unknown.status_code == 401 and blocked.status_code == 403
    assert [r.status_code for r in free] == [200, 200, 200, 429]
    assert free[0].json() == {"path": "/orders", "q": "id=7"}
    assert free[0].headers["X-RateLimit-Remaining"] == "2" and int(free[3].headers["Retry-After"]) >= 1
    assert [r.status_code for r in pro] == [200] * 5 + [429]
    assert st.cache.hits >= 8                            # plan lookups served from the LRU after the first
