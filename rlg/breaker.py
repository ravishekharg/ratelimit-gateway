"""Circuit breaker for one Redis shard.

Without it, every request for a key on a dead shard waits for the socket timeout before failing, so a shard
outage turns into slow requests on every replica, piling up connections. With it, after `threshold`
consecutive failures the shard is treated as down (OPEN) and requests take the degraded path at once. After
`reset_after` seconds one request is let through as a probe (HALF_OPEN); success closes the breaker, failure
re-opens it for another `reset_after`.

  CLOSED --threshold failures--> OPEN --reset_after--> HALF_OPEN --success--> CLOSED
                                   ^                       |
                                   +-------failure---------+
"""
from __future__ import annotations

import time
from collections.abc import Callable

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"


class CircuitBreaker:
    def __init__(self, threshold: int = 3, reset_after: float = 5.0, clock: Callable[[], float] = time.monotonic):
        if threshold < 1 or reset_after <= 0:
            raise ValueError("threshold must be >= 1 and reset_after > 0")
        self.threshold, self.reset_after, self.clock = threshold, reset_after, clock
        self.failures = 0
        self.opened_at: float | None = None
        self.probing = False                 # a half-open probe is in flight: everyone else stays degraded
        self.trips = 0                       # times it opened (for metrics)

    @property
    def state(self) -> str:
        if self.opened_at is None:
            return CLOSED
        return HALF_OPEN if self.clock() - self.opened_at >= self.reset_after else OPEN

    def allow(self) -> bool:
        """May this request try the shard? Closed: yes. Open: no. Half-open: only one probe at a time."""
        s = self.state
        if s == CLOSED:
            return True
        if s == HALF_OPEN and not self.probing:
            self.probing = True
            return True
        return False

    def success(self) -> None:
        self.failures, self.opened_at, self.probing = 0, None, False

    def failure(self) -> None:
        self.probing = False
        if self.opened_at is not None:       # the half-open probe failed (or a late failure): stay open longer
            self.opened_at = self.clock()
            return
        self.failures += 1
        if self.failures >= self.threshold:
            self.opened_at = self.clock()
            self.trips += 1
