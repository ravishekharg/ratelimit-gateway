from hypothesis import given, settings
from hypothesis import strategies as st

from rlg.limiters import FixedWindow, SlidingWindowCounter, SlidingWindowLog, TokenBucket


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def test_token_bucket_burst_then_refill():
    c = FakeClock()
    tb = TokenBucket(capacity=5, rate=1, clock=c)
    assert [tb.check("k").allowed for _ in range(6)] == [True] * 5 + [False]
    d = tb.check("k")
    assert not d.allowed and abs(d.retry_after - 1.0) < 1e-9
    c.t += 2.5
    assert [tb.check("k").allowed for _ in range(3)] == [True, True, False]


def test_keys_are_independent():
    tb = TokenBucket(capacity=1, rate=1, clock=FakeClock())
    assert tb.check("a").allowed and tb.check("b").allowed and not tb.check("a").allowed


def test_fixed_window_allows_double_burst_at_boundary():
    c = FakeClock(59.9)
    fw = FixedWindow(limit=10, window=60, clock=c)
    first = sum(fw.check("k").allowed for _ in range(10))
    c.t = 60.1                                         # 0.2 s later, new window
    second = sum(fw.check("k").allowed for _ in range(10))
    assert first + second == 20                        # the known weakness: 2x the limit in 0.2 s


def test_sliding_log_has_no_boundary_burst():
    c = FakeClock(59.9)
    sw = SlidingWindowLog(limit=10, window=60, clock=c)
    first = sum(sw.check("k").allowed for _ in range(10))
    c.t = 60.1
    second = sum(sw.check("k").allowed for _ in range(10))
    assert (first, second) == (10, 0)


def test_sliding_counter_weights_previous_window():
    c = FakeClock(0.0)
    sc = SlidingWindowCounter(limit=10, window=60, clock=c)
    assert sum(sc.check("k").allowed for _ in range(10)) == 10
    c.t = 90.0                                          # half of the previous window still counts: 10 * 0.5 = 5
    assert sum(sc.check("k").allowed for _ in range(10)) == 5


times = st.lists(st.floats(0, 100, allow_nan=False), min_size=1, max_size=200).map(sorted)


@settings(max_examples=200, deadline=None)
@given(times, st.integers(1, 20), st.floats(0.1, 10))
def test_token_bucket_never_exceeds_capacity_plus_refill(ts, capacity, rate):
    c = FakeClock(0.0)
    tb = TokenBucket(capacity, rate, clock=c)
    allowed = []
    for t in ts:
        c.t = t
        if tb.check("k").allowed:
            allowed.append(t)
    # in ANY interval [a, b] the bucket admits at most capacity + rate * (b - a) requests
    for i in range(len(allowed)):
        for j in range(i, len(allowed)):
            assert j - i + 1 <= capacity + rate * (allowed[j] - allowed[i]) + 1e-6


@settings(max_examples=200, deadline=None)
@given(times, st.integers(1, 15), st.floats(1, 30))
def test_sliding_log_is_exact(ts, limit, window):
    c = FakeClock(0.0)
    sw = SlidingWindowLog(limit, window, clock=c)
    allowed = []
    for t in ts:
        c.t = t
        if sw.check("k").allowed:
            allowed.append(t)
    for i, t in enumerate(allowed):                     # never more than `limit` in any window of length `window`
        assert sum(1 for u in allowed[i:] if u < t + window) <= limit
