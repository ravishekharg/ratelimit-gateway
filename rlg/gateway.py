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
  FAIL_MODE      what to do when a key's Redis shard is down (its circuit breaker is open):
                   local   (default) enforce the plan in this process with limit / GATEWAY_REPLICAS, so all replicas
                           together still admit at most about the plan's limit; header X-RateLimit-Degraded: local
                   open    allow everything (availability first; header X-RateLimit-Degraded: open)
                   closed  503 with Retry-After (abuse protection first)
  GATEWAY_REPLICAS  replica count used to split the plan in local mode (default 1)
  REDIS_TIMEOUT, BREAKER_THRESHOLD, BREAKER_RESET_SECONDS   see redis_limiter.py / breaker.py

Keys, plans and the blocklist live on the first shard. If it is down, keys already in the LRU cache keep working;
unknown keys get 503 (they can't be authenticated), never a free pass.
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
from redis.exceptions import RedisError

from rlg.bloom import BloomFilter
from rlg.breaker import OPEN
from rlg.limiters import Decision, SlidingWindowLog, TokenBucket
from rlg.lru import LRUCache
from rlg.redis_limiter import RedisLimiter, ShardUnavailable

DEFAULT_PLANS = {
    "free": {"algo": "token_bucket", "capacity": 10, "rate": 1},
    "pro": {"algo": "sliding_window", "limit": 600, "window": 60},
}
HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "te", "trailer", "upgrade", "host", "content-length"}

REQUESTS = Counter("gateway_requests_total", "Requests by outcome", ["outcome"])
DECISION = Histogram("gateway_limit_decision_seconds", "Time to decide (cache + bloom + Redis)",
                     buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1))
CACHE_HIT = Gauge("gateway_plan_cache_hit_ratio", "LRU hit ratio for API-key lookups")
DEGRADED = Counter("gateway_degraded_decisions_total", "Decisions made without Redis, by fail mode", ["mode"])
CIRCUIT = Gauge("gateway_shard_circuit_open", "1 while a Redis shard's circuit breaker is open", ["shard"])
FAIL_MODES = ("local", "open", "closed")


def _unavailable(msg: str, retry: int = 1) -> Response:
    return Response(json.dumps({"error": msg}), 503, {"Retry-After": str(retry)}, media_type="application/json")


class State:
    def __init__(self) -> None:
        urls = os.environ.get("REDIS_URLS", "redis://localhost:6379").split(",")
        self.limiter = RedisLimiter.from_urls(urls)
        self.meta = self.limiter.shards[urls[0]]          # keys, plans and blocklist live on the first shard
        self.plans = json.loads(os.environ.get("PLANS", "")) if os.environ.get("PLANS") else DEFAULT_PLANS
        self.cache: LRUCache[str, str] = LRUCache(capacity=10_000, ttl=30)
        self.bloom = BloomFilter(100_000, 0.001)
        self.http = httpx.AsyncClient(base_url=os.environ.get("UPSTREAM_URL", "http://localhost:5678"), timeout=10)
        self.lookups: dict[str, asyncio.Future] = {}
        self.configure_degraded(os.environ.get("FAIL_MODE", "local"), int(os.environ.get("GATEWAY_REPLICAS", "1")))

    async def plan_for(self, key: str) -> str | None:
        """API key -> plan, through the LRU cache. Concurrent misses for the same key share one Redis call
        (single flight): a burst for a key no replica has cached yet must not become a burst on Redis."""
        plan = self.cache.get(key)
        if plan is not None:
            return plan
        pending = self.lookups.get(key)
        if pending is not None:
            return await asyncio.shield(pending)
        fut = asyncio.get_running_loop().create_future()
        self.lookups[key] = fut
        try:
            plan = await self.meta.hget("api_keys", key)
            if plan:
                self.cache.put(key, plan)
            fut.set_result(plan)
            return plan
        except BaseException as e:
            fut.set_exception(e)
            fut.exception()                     # mark retrieved: waiters re-raise it, no "never retrieved" warning
            raise
        finally:
            del self.lookups[key]

    def configure_degraded(self, mode: str, replicas: int) -> None:
        if mode not in FAIL_MODES or replicas < 1:
            raise ValueError(f"FAIL_MODE must be one of {FAIL_MODES} and GATEWAY_REPLICAS >= 1")
        self.fail_mode, self.replicas = mode, replicas
        self.local: dict[str, TokenBucket | SlidingWindowLog] = {}

    def local_limiter(self, plan_name: str) -> TokenBucket | SlidingWindowLog:
        """This replica's share of the plan: limit / replicas (rounded up), so N replicas admit about the limit."""
        if plan_name not in self.local:
            p, n = self.plans[plan_name], self.replicas
            self.local[plan_name] = (TokenBucket(math.ceil(p["capacity"] / n), p["rate"] / n)
                                     if p["algo"] == "token_bucket"
                                     else SlidingWindowLog(math.ceil(p["limit"] / n), p["window"]))
        return self.local[plan_name]

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
        # liveness must not depend on Redis: restarting every gateway because a shard is down would turn a
        # partial outage into a full one. Shard state is reported for humans and dashboards.
        return {"ok": True, "fail_mode": st.fail_mode,
                "shards": {s: b.state for s, b in st.limiter.breakers.items()}}

    @app.get("/metrics")
    async def metrics() -> Response:
        total = st.cache.hits + st.cache.misses
        CACHE_HIT.set(st.cache.hits / total if total else 0)
        for s, b in st.limiter.breakers.items():
            CIRCUIT.labels(s).set(1 if b.state == OPEN else 0)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(path: str, request: Request) -> Response:
        t0 = time.perf_counter()
        key = request.headers.get("x-api-key", "")
        plan_name = None
        try:
            if key:
                plan_name = await st.plan_for(key)
            if plan_name and key in st.bloom and await st.meta.sismember("blocked_keys", key):
                REQUESTS.labels("blocked").inc()
                return Response('{"error":"API key blocked"}', 403, media_type="application/json")
        except (RedisError, OSError):
            REQUESTS.labels("auth_unavailable").inc()           # can't check the key: never let it through
            return _unavailable("cannot verify the API key right now")
        if not plan_name or plan_name not in st.plans:
            REQUESTS.labels("unauthorized").inc()
            return Response('{"error":"unknown API key"}', 401, media_type="application/json")

        plan = st.plans[plan_name]
        limit = plan["capacity"] if plan["algo"] == "token_bucket" else plan["limit"]
        degraded = ""
        try:
            if plan["algo"] == "token_bucket":
                d = await st.limiter.token_bucket(key, plan["capacity"], plan["rate"])
            else:
                d = await st.limiter.sliding_window(key, plan["limit"], plan["window"])
        except ShardUnavailable:
            degraded = st.fail_mode
            DEGRADED.labels(degraded).inc()
            if degraded == "closed":
                REQUESTS.labels("unavailable").inc()
                return _unavailable("rate limiter unavailable", 5)
            d = st.local_limiter(plan_name).check(key) if degraded == "local" else Decision(True, 0, 0.0)
        DECISION.observe(time.perf_counter() - t0)
        headers = {"X-RateLimit-Limit": str(limit), "X-RateLimit-Remaining": str(max(d.remaining, 0)),
                   "X-RateLimit-Plan": plan_name}
        if degraded:
            headers["X-RateLimit-Degraded"] = degraded
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
