"""The gateway: authenticate the API key, reject blocked keys, rate-limit per key and plan, proxy upstream.

Request path (hot path has at most two Redis round trips):
  1. X-API-Key -> plan via the LRU cache (Redis HGET only on a miss)
  2. Bloom filter "is this key blocked?"; only a 'maybe' triggers the exact SISMEMBER check
  3. one atomic Lua call on the key's shard (token bucket or sliding window, per plan)
  4. proxy to the upstream with X-RateLimit-* headers; 429 + Retry-After when limited

Config (environment):
  UPSTREAM_URL   http://upstream:5678
  REDIS_URLS     redis://redis-0:6379,redis://redis-1:6379,...   (first one also stores keys, plans, blocklist)
  PLANS          JSON, e.g. {"free": {"algo": "token_bucket", "capacity": 10, "rate": 1},
                             "pro": {"algo": "sliding_window", "limit": 600, "window": 60}}
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import math
import os
import time

import httpx
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from rlg.bloom import BloomFilter
from rlg.lru import LRUCache
from rlg.redis_limiter import RedisLimiter

DEFAULT_PLANS = {
    "free": {"algo": "token_bucket", "capacity": 10, "rate": 1},
    "pro": {"algo": "sliding_window", "limit": 600, "window": 60},
}
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "host", "content-length"}

REQUESTS = Counter("gateway_requests_total", "Requests by outcome", ["outcome"])
DECISION = Histogram("gateway_limit_decision_seconds", "Time to decide (cache + bloom + Redis)",
                     buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1))
CACHE_HIT = Gauge("gateway_plan_cache_hit_ratio", "LRU hit ratio for API-key lookups")


class State:
    def __init__(self) -> None:
        urls = os.environ.get("REDIS_URLS", "redis://localhost:6379").split(",")
        self.limiter = RedisLimiter.from_urls(urls)
        self.meta = self.limiter.shards[urls[0]]          # keys, plans and blocklist live on the first shard
        self.plans = json.loads(os.environ.get("PLANS", "")) if os.environ.get("PLANS") else DEFAULT_PLANS
        self.cache: LRUCache[str, str] = LRUCache(capacity=10_000, ttl=30)
        self.bloom = BloomFilter(100_000, 0.001)
        self.http = httpx.AsyncClient(base_url=os.environ.get("UPSTREAM_URL", "http://localhost:5678"), timeout=10)

    async def refresh_blocklist(self) -> None:
        bloom = BloomFilter(max(100_000, 2 * await self.meta.scard("blocked_keys")), 0.001)
        async for k in self.meta.sscan_iter("blocked_keys", count=1000):
            bloom.add(k)
        self.bloom = bloom                                 # atomic swap: readers never see a half-built filter


def create_app() -> FastAPI:
    st = State()

    async def refresher() -> None:
        while True:
            with contextlib.suppress(Exception):
                await st.refresh_blocklist()
            await asyncio.sleep(10)

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        task = asyncio.create_task(refresher())
        yield
        task.cancel()
        await st.http.aclose()
        await st.limiter.close()

    app = FastAPI(title="ratelimit-gateway", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict:
        await st.meta.ping()
        return {"ok": True}

    @app.get("/metrics")
    async def metrics() -> Response:
        total = st.cache.hits + st.cache.misses
        CACHE_HIT.set(st.cache.hits / total if total else 0)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request) -> Response:
        t0 = time.perf_counter()
        key = request.headers.get("x-api-key", "")
        plan_name = st.cache.get(key) if key else None
        if key and plan_name is None:
            plan_name = await st.meta.hget("api_keys", key)
            if plan_name:
                st.cache.put(key, plan_name)
        if not plan_name or plan_name not in st.plans:
            REQUESTS.labels("unauthorized").inc()
            return Response('{"error":"unknown API key"}', 401, media_type="application/json")
        if key in st.bloom and await st.meta.sismember("blocked_keys", key):
            REQUESTS.labels("blocked").inc()
            return Response('{"error":"API key blocked"}', 403, media_type="application/json")

        plan = st.plans[plan_name]
        if plan["algo"] == "token_bucket":
            d = await st.limiter.token_bucket(key, plan["capacity"], plan["rate"])
            limit = plan["capacity"]
        else:
            d = await st.limiter.sliding_window(key, plan["limit"], plan["window"])
            limit = plan["limit"]
        DECISION.observe(time.perf_counter() - t0)
        headers = {"X-RateLimit-Limit": str(limit), "X-RateLimit-Remaining": str(max(d.remaining, 0)),
                   "X-RateLimit-Plan": plan_name}
        if not d.allowed:
            REQUESTS.labels("limited").inc()
            headers["Retry-After"] = str(max(1, math.ceil(d.retry_after)))
            return Response('{"error":"rate limit exceeded"}', 429, headers, media_type="application/json")

        fwd = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP | {"x-api-key"}}
        upstream = await st.http.request(request.method, f"/{path}", params=request.query_params,
                                         content=await request.body(), headers=fwd)
        REQUESTS.labels("allowed").inc()
        out = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP}
        return Response(upstream.content, upstream.status_code, {**out, **headers})

    app.state.st = st
    return app


app = create_app()
