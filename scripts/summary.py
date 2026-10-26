"""Render loadtest results as Markdown for the GitHub Actions job summary.

  python scripts/summary.py results.json "Docker Compose"
"""
import json
import os
import sys

path, title = sys.argv[1], sys.argv[2]
if not os.path.exists(path):
    print(f"## {title}: no results")
    raise SystemExit(0)
r = json.load(open(path))
tb, sw = r["token_bucket_exactness"], r["sliding_window_exactness"]
d, g = r["direct_upstream"], r["through_gateway"]
print(f"## {title}: {'PASSED' if r['passed'] else 'FAILED'}\n")
print("| check | result |\n|---|---|")
for k, v in r["checks"].items():
    print(f"| {k} | {'pass' if v else 'FAIL'} |")
print(f"\n- Token bucket: capacity {tb['capacity']}, {tb['sent']} concurrent requests over {tb['replicas']} "
      f"gateway endpoint(s): **{tb['allowed']} allowed**, {tb['limited']} limited")
print(f"- Sliding window: limit {sw['limit']}, {sw['sent']} requests: **{sw['allowed']} allowed**")
print(f"- Shard balance (1,000 keys): {r['shard_balance']['keys_per_shard']} "
      f"(max/mean {r['shard_balance']['max_over_mean']})\n")
print("| path | requests | conc. | req/s | p50 ms | p95 ms | p99 ms |\n|---|---|---|---|---|---|---|")
for name, x in (("direct to upstream", d), ("through gateway", g)):
    print(f"| {name} | {x['requests']} | {x['concurrency']} | {x['rps']} | {x['p50_ms']} | {x['p95_ms']} | {x['p99_ms']} |")
