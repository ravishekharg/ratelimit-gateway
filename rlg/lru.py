"""LRU cache from scratch: hash map for O(1) lookup + doubly linked list for O(1) recency updates and eviction.

The gateway caches API-key -> plan lookups here so the hot path does not hit Redis for every request.
Sentinel head/tail nodes remove all edge cases from insert and unlink. A TTL makes stale plans expire.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class _Node(Generic[K, V]):
    __slots__ = ("key", "value", "expires", "prev", "next")

    def __init__(self, key, value, expires: float) -> None:
        self.key, self.value, self.expires = key, value, expires
        self.prev: _Node | None = None
        self.next: _Node | None = None


class LRUCache(Generic[K, V]):
    def __init__(self, capacity: int, ttl: float = float("inf"), clock: Callable[[], float] = time.monotonic) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity, self.ttl, self.clock = capacity, ttl, clock
        self.map: dict[K, _Node[K, V]] = {}
        self.head: _Node = _Node(None, None, 0)       # most recently used side
        self.tail: _Node = _Node(None, None, 0)       # least recently used side
        self.head.next, self.tail.prev = self.tail, self.head
        self.hits = self.misses = self.evictions = 0

    def _unlink(self, n: _Node) -> None:
        n.prev.next, n.next.prev = n.next, n.prev

    def _push_front(self, n: _Node) -> None:
        n.prev, n.next = self.head, self.head.next
        self.head.next.prev = n
        self.head.next = n

    def get(self, key: K) -> V | None:
        n = self.map.get(key)
        if n is None or n.expires <= self.clock():
            if n is not None:                          # expired: drop it
                self._unlink(n)
                del self.map[key]
            self.misses += 1
            return None
        self._unlink(n)
        self._push_front(n)
        self.hits += 1
        return n.value

    def put(self, key: K, value: V) -> None:
        n = self.map.get(key)
        if n is not None:
            self._unlink(n)
        elif len(self.map) >= self.capacity:
            lru = self.tail.prev
            self._unlink(lru)
            del self.map[lru.key]
            self.evictions += 1
        n = _Node(key, value, self.clock() + self.ttl)
        self.map[key] = n
        self._push_front(n)

    def __len__(self) -> int:
        return len(self.map)

    def keys_mru(self) -> list[K]:
        out, n = [], self.head.next
        while n is not self.tail:
            out.append(n.key)
            n = n.next
        return out
