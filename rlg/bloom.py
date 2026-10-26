"""Bloom filter: a compact set that can say "definitely not present" or "probably present".

The gateway keeps revoked/blocked API keys in one: almost every request has a valid key, and the filter rejects
"is it blocked?" in O(k) with no network call. Only a "probably" answer triggers the exact check in Redis.

Sizing for n items and target false-positive rate p:   m = -n ln p / (ln 2)^2 bits,   k = (m / n) ln 2 hashes.
k positions come from one 128-bit BLAKE2b digest via double hashing: h_i = h1 + i * h2 (Kirsch-Mitzenmacher).
"""
from __future__ import annotations

import hashlib
import math


class BloomFilter:
    def __init__(self, expected_items: int, fp_rate: float = 0.001) -> None:
        if expected_items <= 0 or not 0 < fp_rate < 1:
            raise ValueError("expected_items > 0 and 0 < fp_rate < 1")
        self.m = max(8, math.ceil(-expected_items * math.log(fp_rate) / math.log(2) ** 2))
        self.k = max(1, round(self.m / expected_items * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.count = 0

    def _positions(self, item: str):
        d = hashlib.blake2b(item.encode(), digest_size=16).digest()
        h1, h2 = int.from_bytes(d[:8], "little"), int.from_bytes(d[8:], "little") | 1
        return ((h1 + i * h2) % self.m for i in range(self.k))

    def add(self, item: str) -> None:
        for p in self._positions(item):
            self.bits[p >> 3] |= 1 << (p & 7)
        self.count += 1

    def __contains__(self, item: str) -> bool:
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(item))

    def expected_fp_rate(self) -> float:
        return (1 - math.exp(-self.k * self.count / self.m)) ** self.k
