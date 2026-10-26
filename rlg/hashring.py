"""Consistent hashing ring with virtual nodes: spreads rate-limit keys over several Redis shards.

With plain `hash(key) % n`, adding one shard remaps almost every key (all counters reset at once). On a ring,
adding a shard moves only about 1/(n+1) of the keys. Virtual nodes (many points per shard) even out the load.

lookup: binary search (bisect) on the sorted point list -> O(log(n * vnodes)).
add/remove: O(vnodes * log(n * vnodes)) with insort / rebuild.
"""
from __future__ import annotations

import bisect
import hashlib


def _h(s: str) -> int:
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "big")


class HashRing:
    def __init__(self, nodes: list[str] | None = None, vnodes: int = 160) -> None:
        self.vnodes = vnodes
        self.points: list[int] = []
        self.owner: dict[int, str] = {}
        self.nodes: set[str] = set()
        for n in nodes or []:
            self.add(n)

    def add(self, node: str) -> None:
        if node in self.nodes:
            return
        self.nodes.add(node)
        for i in range(self.vnodes):
            p = _h(f"{node}#{i}")
            if p in self.owner:                     # astronomically rare 64-bit collision: skip the point
                continue
            self.owner[p] = node
            bisect.insort(self.points, p)

    def remove(self, node: str) -> None:
        if node not in self.nodes:
            return
        self.nodes.remove(node)
        self.points = [p for p in self.points if self.owner[p] != node]
        self.owner = {p: self.owner[p] for p in self.points}

    def get(self, key: str) -> str:
        if not self.points:
            raise LookupError("empty ring")
        i = bisect.bisect_right(self.points, _h(key)) % len(self.points)   # first point clockwise, wrapping
        return self.owner[self.points[i]]

    def distribution(self, keys: list[str]) -> dict[str, int]:
        out = dict.fromkeys(sorted(self.nodes), 0)
        for k in keys:
            out[self.get(k)] += 1
        return out
