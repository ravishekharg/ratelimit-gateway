"""Integration + load test against the compose stack (or any deployment: pass --gateways / --redis).

Proves, with real Redis and two gateway replicas:
  * exactness: N concurrent requests for one key, spread over both replicas, allow exactly `capacity`
  * auth: unknown key 401, blocked key 403 (Bloom filter + exact check)
  * latency: gateway overhead vs calling the upstream directly (p50/p95/p99, requests/s)
  * shard balance: how the consistent-hash ring spread 1,000 keys over 3 Redis shards

  python scripts/loadtest.py --out results.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time

import httpx
import redis.asyncio as aioredis


def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    return round(s[min(len(s) - 1, int(len(s) * p / 100))] * 1000, 2)


async def timed(c: httpx.AsyncClient, url: str, key: str | None) -> tuple[int, float]:
    t = time.perf_counter()
    r = await c.get(url, headers={"x-api-key": key} if key else {})
    return r.status_code, time.perf_counter() - t


async def burst(c, gws, key, n):
    res = await asyncio.gather(*(timed(c, f"{gws[i % len(gws)]}/api/x", key) for i in range(n)))
    return sum(code == 200 for code, _ in res), sum(code == 429 for code, _ in res)


async def load(c, url, keys, total, conc):
    sem = asyncio.Semaphore(conc)
    lat: list[float] = []

    async def one(i):
        async with sem:
            code, dt = await timed(c, url, keys[i % len(keys)] if keys else None)
            assert code == 200, code
            lat.append(dt)
    t = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(total)))
    wall = time.perf_counter() - t
    return {"requests": total, "concurrency": conc, "rps": round(total / wall), "p50_ms": pct(lat, 50),
            "p95_ms": pct(lat, 95), "p99_ms": pct(lat, 99), "mean_ms": round(statistics.mean(lat) * 1000, 2)}


async def main(a) -> dict:
    gws = a.gateways.split(",")
    shards = [aioredis.from_url(u, decode_responses=True) for u in a.redis.split(",")]
    meta = shards[0]
    for s in shards:
        await s.flushall()
    perf_keys = [f"perf-{i}" for i in range(1000)]
    await meta.hset("api_keys", mapping={"burst-key": "burst", "window-key": "window", "bad-key": "free",
                                         **dict.fromkeys(perf_keys, "perf")})
    await meta.sadd("blocked_keys", "bad-key")
    print("seeded; waiting for the gateways to refresh their Bloom filters")
    await asyncio.sleep(12)

    out: dict = {}
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=200)
    async with httpx.AsyncClient(timeout=30, limits=limits) as c:
        allowed, limited = await burst(c, gws, "burst-key", 200)
        out["token_bucket_exactness"] = {"capacity": 50, "sent": 200, "replicas": len(gws),
                                         "allowed": allowed, "limited": limited, "exact": allowed == 50}
        allowed, limited = await burst(c, gws, "window-key", 90)
        out["sliding_window_exactness"] = {"limit": 30, "sent": 90, "allowed": allowed, "exact": allowed == 30}
        out["auth"] = {"unknown_key": (await c.get(f"{gws[0]}/api/x", headers={"x-api-key": "nope"})).status_code,
                       "blocked_key": (await c.get(f"{gws[1]}/api/x", headers={"x-api-key": "bad-key"})).status_code}
        await load(c, f"{gws[0]}/api/x", perf_keys, 500, 50)                       # warm-up
        out["direct_upstream"] = await load(c, f"{a.upstream}/", None, a.requests, a.concurrency)
        out["through_gateway"] = await load(c, f"{gws[0]}/api/x", perf_keys, a.requests, a.concurrency)
    counts = [len([k async for k in s.scan_iter("tb:perf-*")]) for s in shards]
    out["shard_balance"] = {"keys_per_shard": counts, "max_over_mean": round(max(counts) / statistics.mean(counts), 3)}
    for s in shards:
        await s.aclose()
    checks = {
        "token bucket exact across replicas": out["token_bucket_exactness"]["exact"],
        "sliding window exact across replicas": out["sliding_window_exactness"]["exact"],
        "unknown key -> 401": out["auth"]["unknown_key"] == 401,
        "blocked key -> 403": out["auth"]["blocked_key"] == 403,
        "every shard used": all(counts),
    }
    out["checks"] = checks
    out["passed"] = all(checks.values())
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateways", default="http://localhost:8081,http://localhost:8082")
    ap.add_argument("--redis", default="redis://localhost:6379,redis://localhost:6380,redis://localhost:6381")
    ap.add_argument("--upstream", default="http://localhost:5678")
    ap.add_argument("--requests", type=int, default=5000)
    ap.add_argument("--concurrency", type=int, default=50)
    ap.add_argument("--out", default="results.json")
    args = ap.parse_args()
    result = asyncio.run(main(args))
    print(json.dumps(result, indent=2))
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    raise SystemExit(0 if result["passed"] else 1)
