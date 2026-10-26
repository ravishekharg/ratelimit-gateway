"""In-process rate-limiting algorithms (single gateway instance). The distributed versions are in redis_limiter.py.

| Algorithm              | Memory per key | Time per check | Behaviour |
|------------------------|----------------|----------------|-----------|
| TokenBucket            | O(1)           | O(1)           | allows bursts up to `capacity`, then `rate` per second |
| FixedWindow            | O(1)           | O(1)           | simple, but allows 2x the limit across a window boundary |
| SlidingWindowLog       | O(limit)       | O(1) amortised | exact: keeps each request timestamp in a deque |
| SlidingWindowCounter   | O(1)           | O(1)           | approximates the log by weighting the previous window |

All take a `clock` (seconds as float) so tests are deterministic.
"""
from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

Clock = Callable[[], float]


@dataclass
class Decision:
    allowed: bool
    remaining: int
    retry_after: float      # seconds until the next request could be allowed (0 when allowed)


class TokenBucket:
    def __init__(self, capacity: int, rate: float, clock: Clock = time.monotonic) -> None:
        if capacity <= 0 or rate <= 0:
            raise ValueError("capacity and rate must be positive")
        self.capacity, self.rate, self.clock = capacity, rate, clock
        self.state: dict[str, tuple[float, float]] = {}       # key -> (tokens, last refill time)

    def check(self, key: str, cost: int = 1) -> Decision:
        now = self.clock()
        tokens, last = self.state.get(key, (float(self.capacity), now))
        tokens = min(self.capacity, tokens + (now - last) * self.rate)     # lazy refill: no background timer
        if tokens >= cost:
            self.state[key] = (tokens - cost, now)
            return Decision(True, int(tokens - cost), 0.0)
        self.state[key] = (tokens, now)
        return Decision(False, int(tokens), (cost - tokens) / self.rate)


class FixedWindow:
    def __init__(self, limit: int, window: float, clock: Clock = time.monotonic) -> None:
        self.limit, self.window, self.clock = limit, window, clock
        self.state: dict[str, tuple[int, int]] = {}            # key -> (window index, count)

    def check(self, key: str) -> Decision:
        now = self.clock()
        idx = int(now // self.window)
        w, count = self.state.get(key, (idx, 0))
        if w != idx:
            w, count = idx, 0
        if count < self.limit:
            self.state[key] = (w, count + 1)
            return Decision(True, self.limit - count - 1, 0.0)
        return Decision(False, 0, (idx + 1) * self.window - now)


class SlidingWindowLog:
    def __init__(self, limit: int, window: float, clock: Clock = time.monotonic) -> None:
        self.limit, self.window, self.clock = limit, window, clock
        self.logs: dict[str, deque[float]] = {}

    def check(self, key: str) -> Decision:
        now = self.clock()
        log = self.logs.setdefault(key, deque())
        while log and log[0] <= now - self.window:       # each timestamp is appended and popped once: O(1) amortised
            log.popleft()
        if len(log) < self.limit:
            log.append(now)
            return Decision(True, self.limit - len(log), 0.0)
        return Decision(False, 0, log[0] + self.window - now)


class SlidingWindowCounter:
    """estimate = count(current window) + count(previous window) * (share of the previous window still in range)"""

    def __init__(self, limit: int, window: float, clock: Clock = time.monotonic) -> None:
        self.limit, self.window, self.clock = limit, window, clock
        self.state: dict[str, tuple[int, int, int]] = {}       # key -> (window index, current count, previous count)

    def check(self, key: str) -> Decision:
        now = self.clock()
        idx = int(now // self.window)
        w, cur, prev = self.state.get(key, (idx, 0, 0))
        if idx == w + 1:
            w, cur, prev = idx, 0, cur
        elif idx > w + 1:
            w, cur, prev = idx, 0, 0
        overlap = 1 - (now - idx * self.window) / self.window
        estimate = cur + prev * overlap
        if estimate + 1 <= self.limit:
            self.state[key] = (w, cur + 1, prev)
            return Decision(True, int(self.limit - estimate - 1), 0.0)
        self.state[key] = (w, cur, prev)
        if cur + 1 <= self.limit and prev:
            # the previous window's weight shrinks over time: wait until one more request fits
            target = (self.limit - 1 - cur) / prev
            wait = (overlap - target) * self.window
        else:
            wait = (idx + 1) * self.window - now
        return Decision(False, 0, max(wait, 0.0))
