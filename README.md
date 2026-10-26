# ratelimit-gateway

An API gateway in Python (FastAPI) that authenticates API keys, rejects blocked keys and enforces **per-plan rate
limits that stay exact across many gateway replicas**, using Redis Lua scripts sharded with a consistent-hash ring.
Each data structure is implemented by hand and tested, because each one solves a real part of the problem:

| Problem | Data structure / algorithm | Cost |
|---|---|---|
| Allow bursts, then a steady rate | **token bucket** with lazy refill | O(1) time and memory per key |
| Hard "N per window" without boundary bursts | **sliding window log** (deque of timestamps) / **sliding window counter** (weighted previous window) | O(1) amortised / O(1) |
| Replicas must not over-admit under concurrency | **atomic Redis Lua script** per check, clock from `redis.call('TIME')` | one round trip |
| Spread keys over several Redis shards, add shards without resetting everyone | **consistent hashing ring** with 160 virtual nodes, `bisect` lookup | O(log V); adding a shard moves ~1/(n+1) keys |
| API-key → plan lookup on every request | **LRU cache** (hash map + doubly linked list, TTL) | O(1) get/put |
| "Is this key blocked?" for millions of keys | **Bloom filter** (k hashes by double hashing, sized from n and p) | O(k), no network call unless "maybe" |

## Results

Measured in CI on every push (GitHub-hosted `ubuntu-24.04`, results in each run's job summary).

**Correctness under concurrency: the main result**

| Setup | Test | Sent (concurrently) | Allowed | Expected |
|---|---|---|---|---|
| Docker Compose: 2 gateway replicas, 3 Redis shards | token bucket, capacity 50 | 200, alternating replicas | **50** | 50 |
| | sliding window, 30 / 60 s | 90 | **30** | 30 |
| Kubernetes (kind): 3 gateway pods, 3 Redis pods, load test as an in-cluster Job | token bucket, capacity 50 | 200, spread over the 3 pods | **50** | 50 |
| | sliding window, 30 / 60 s | 90 | **30** | 30 |

Unknown key → 401, blocked key → 403 (Bloom filter + exact check), in both setups.

**Consistent hashing:** 1,000 API keys over 3 Redis shards landed as 344 / 294 / 362 (max/mean 1.09). In the unit
tests, adding a 4th shard moves about 25% of keys (ideal 1/4), all of them to the new shard; `hash % n` would move
about 75%.

**Data structures:** the LRU passes a stateful property test against a reference model; the Bloom filter (1%
target) measured < 2% false positives on 100,000 non-members with zero false negatives.

**Latency (indicative only):** one single-worker uvicorn gateway on a shared 4-vCPU CI runner that also runs the
load generator, the upstream and 3 Redis servers, at 50 concurrent requests: median 231–252 ms through the
gateway vs 101–110 ms direct, 139–146 req/s. These are not production numbers. The fixed cost per request is one
LRU lookup, one Bloom check and one Redis round trip; scaling is horizontal (HPA), because state lives in Redis.

## Request path

```mermaid
sequenceDiagram
  participant C as Client
  participant G as Gateway replica (FastAPI)
  participant L as LRU cache (in process)
  participant B as Bloom filter (in process)
  participant R as Redis shard (ring.get(key))
  participant U as Upstream
  C->>G: GET /api/orders (X-API-Key)
  G->>L: plan for key?
  alt cache miss
    G->>R: HGET api_keys key (meta shard)
  end
  G->>B: key possibly blocked?
  opt Bloom says maybe
    G->>R: SISMEMBER blocked_keys key (exact check)
  end
  G->>R: EVALSHA token_bucket / sliding_window (atomic)
  alt limited
    G-->>C: 429 + Retry-After + X-RateLimit-*
  else allowed
    G->>U: proxy request
    U-->>G: response
    G-->>C: response + X-RateLimit-*
  end
```

## Deployment

```mermaid
flowchart LR
  users["Clients"] --> lb["Load balancer / Ingress"]
  lb --> g1["gateway pod"] & g2["gateway pod"] & g3["gateway pod (HPA 3-20)"]
  g1 & g2 & g3 -->|"consistent hash of the key"| r0[("Redis shard 0")] & r1[("Redis shard 1")] & r2[("Redis shard 2")]
  g1 & g2 & g3 --> up["Upstream services"]
```

- **Local / CI:** `compose.yaml` (3 Redis, 2 gateway replicas, an echo upstream).
- **Kubernetes:** `deploy/k8s/ratelimit.yaml`: Deployment with HPA, PDB, topology spread and a hardened security
  context; Redis as a StatefulSet with one shard per pod behind a headless Service.
- **AWS:** `terraform/aws`: one ElastiCache replication group per shard (Multi-AZ, automatic failover, TLS and
  at-rest encryption, reachable only from the gateway's security group). The `redis_urls` output plugs into
  `REDIS_URLS`.

## Run it

```bash
pip install -e ".[dev]" && pytest -q              # unit, property and stateful tests (no Docker needed)
docker compose up -d --build --wait
python scripts/loadtest.py                        # exactness, auth, latency, shard balance
```

## Design notes (interview talking points)

- **Why Lua and not GET + SET?** Two replicas can both read "1 token left" and both allow. A Lua script runs
  atomically on the Redis server, so check-and-decrement is one indivisible step.
- **Why `TIME` inside the script?** Replicas' clocks drift; using the Redis server clock makes refills consistent.
- **Why consistent hashing?** `hash(key) % n` remaps about 75% of keys when going from 3 to 4 shards, resetting their
  limits; the ring moves about 25%, and only to the new shard (tested).
- **Why a Bloom filter in front of Redis?** Almost no request uses a blocked key; the filter answers "definitely not
  blocked" locally. False positives only cost one extra Redis call, and there are no false negatives.
- **Failure mode:** if Redis is unreachable the gateway returns an error (fail closed). Fail-open with a local
  token bucket is a one-line policy change, and a trade-off to discuss: availability vs abuse protection.
