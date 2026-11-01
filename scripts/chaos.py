"""Chaos test against the compose stack: stop one Redis shard during traffic, then bring it back.

Expected (FAIL_MODE=local, GATEWAY_REPLICAS=2, plan "chaos" = token bucket of 20):
  * keys on the stopped shard keep being served, marked X-RateLimit-Degraded: local, and the two replicas together
    admit at most 2 x ceil(20 / 2) = 20 of a 60-request burst: the limit holds without Redis
  * once the breaker is open, degraded decisions are fast (no waiting for the socket timeout on every request)
  * keys on healthy shards stay exact and are not marked degraded
  * after the shard returns and the breaker's reset time passes, the same shard is exact again

  python scripts/chaos.py --out chaos.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import time

import httpx
import redis.asyncio as aioredis

from rlg.hashring import HashRing

GATEWAY_RING = ["redis://redis-0:6379", "redis://redis-1:6379", "redis://redis-2:6379"]   # names the gateways use
VICTIM = "redis://redis-2:6379"
HEALTHY = "redis://redis-1:6379"


def keys_on(ring: HashRing, shard: str, n: int, prefix: str) -> list[str]:
    out, i = [], 0
    while len(out) < n:
        k = f"{prefix}-{i}"
        if ring.get(k) == shard:
            out.append(k)
        i += 1
    return out


async def burst(c: httpx.AsyncClient, gws: list[str], key: str, n: int) -> dict:
    async def one(i):
        t = time.perf_counter()
        r = await c.get(f"{gws[i % len(gws)]}/api/x", headers={"x-api-key": key})
        return r.status_code, r.headers.get("x-ratelimit-degraded", ""), time.perf_counter() - t
    res = await asyncio.gather(*(one(i) for i in range(n)))
    lat = sorted(dt for _, _, dt in res)
    return {"sent": n, "allowed": sum(code == 200 for code, _, _ in res),
            "limited": sum(code == 429 for code, _, _ in res),
            "other": sorted({code for code, _, _ in res} - {200, 429}),
            "degraded": sum(bool(d) for _, d, _ in res), "p50_ms": round(lat[len(lat) // 2] * 1000, 1)}


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


async def main(a) -> dict:
    gws = a.gateways.split(",")
    ring = HashRing(GATEWAY_RING)
    victim_keys = keys_on(ring, VICTIM, 4, "chaos-dead")
    healthy_key = keys_on(ring, HEALTHY, 1, "chaos-live")[0]
    meta = aioredis.from_url(a.meta, decode_responses=True)
    await meta.hset("api_keys", mapping=dict.fromkeys([*victim_keys, healthy_key], "chaos"))
    await meta.aclose()

    out: dict = {}
    async with httpx.AsyncClient(timeout=10) as c:
        # warm the gateways' key caches so the outage only affects rate-limit state
        for k in [*victim_keys[:3], healthy_key]:
            for g in gws:
                await c.get(f"{g}/api/x", headers={"x-api-key": k})
        compose("stop", "redis-2")
        out["trip"] = await burst(c, gws, victim_keys[0], 10)              # these trip the breakers
        out["victim_shard_down"] = await burst(c, gws, victim_keys[1], 60)
        out["healthy_shard_meanwhile"] = await burst(c, gws, healthy_key, 60)
        health = (await c.get(f"{gws[0]}/healthz")).json()
        out["breaker_state_while_down"] = health["shards"].get(VICTIM)
        compose("start", "redis-2")
        compose("up", "-d", "--wait", "redis-2")
        await asyncio.sleep(a.reset + 1)                                    # breaker reset time
        # probe until every replica's breaker has closed. The first call after the restart can hit a pooled
        # connection the old server closed, which re-opens the breaker once more: that's correct behaviour, so
        # keep probing (on a key of its own) rather than assuming one probe per replica is enough
        for _ in range(30):
            await burst(c, gws, victim_keys[2], len(gws))
            states = [(await c.get(f"{g}/healthz")).json()["shards"].get(VICTIM) for g in gws]
            if all(s == "closed" for s in states):
                break
            await asyncio.sleep(1)
        out["victim_shard_back"] = await burst(c, gws, victim_keys[3], 60)  # a fresh key on the restarted shard
        out["breaker_state_after"] = (await c.get(f"{gws[0]}/healthz")).json()["shards"].get(VICTIM)

    down, live, back = out["victim_shard_down"], out["healthy_shard_meanwhile"], out["victim_shard_back"]
    checks = {
        "keys on the dead shard are still served": down["allowed"] > 0 and not down["other"],
        "every decision for them is marked degraded": down["degraded"] == down["sent"],
        "replicas together admit at most the plan's limit (20)": down["allowed"] <= 20,
        "degraded decisions are fast (p50 < 100 ms)": down["p50_ms"] < 100,
        # 2 of its 20 tokens went on warming the caches, so exactly 18 of the burst may pass
        "healthy shard stays exact (18 of 60) and unmarked": live["allowed"] == 18 and live["degraded"] == 0,
        "breaker was open while the shard was down": out["breaker_state_while_down"] == "open",
        # a key never used before, on the restarted shard: its full 20 tokens, decided by Redis again
        "after recovery the shard is exact again (20 of 60)": back["allowed"] == 20
        and back["degraded"] == 0,
        "breaker closed after recovery": out["breaker_state_after"] == "closed",
    }
    out["checks"] = checks
    out["passed"] = all(checks.values())
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateways", default="http://localhost:8081,http://localhost:8082")
    ap.add_argument("--meta", default="redis://localhost:6379")
    ap.add_argument("--reset", type=float, default=5.0, help="BREAKER_RESET_SECONDS of the gateways")
    ap.add_argument("--out", default="chaos.json")
    args = ap.parse_args()
    result = asyncio.run(main(args))
    print(json.dumps(result, indent=2))
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    raise SystemExit(0 if result["passed"] else 1)
