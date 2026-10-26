import random
import statistics
from collections import OrderedDict

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from rlg.bloom import BloomFilter
from rlg.hashring import HashRing
from rlg.lru import LRUCache


class LRUMatchesModel(RuleBasedStateMachine):
    """Stateful test: random get/put sequences must behave exactly like an OrderedDict-based reference LRU."""

    def __init__(self) -> None:
        super().__init__()
        self.cap = 5
        self.lru: LRUCache[int, int] = LRUCache(self.cap)
        self.model: OrderedDict[int, int] = OrderedDict()

    @rule(k=st.integers(0, 12), v=st.integers())
    def put(self, k, v):
        self.lru.put(k, v)
        if k in self.model:
            self.model.move_to_end(k)
        self.model[k] = v
        if len(self.model) > self.cap:
            self.model.popitem(last=False)

    @rule(k=st.integers(0, 12))
    def get(self, k):
        want = self.model.get(k)
        if k in self.model:
            self.model.move_to_end(k)
        assert self.lru.get(k) == want

    @invariant()
    def same_order(self):
        assert self.lru.keys_mru() == list(reversed(self.model))


TestLRU = LRUMatchesModel.TestCase
TestLRU.settings = settings(max_examples=150, stateful_step_count=60, deadline=None)


def test_lru_ttl_expires_entries():
    now = [0.0]
    c = LRUCache(3, ttl=10, clock=lambda: now[0])
    c.put("a", 1)
    now[0] = 9.9
    assert c.get("a") == 1
    now[0] = 20
    assert c.get("a") is None and len(c) == 0


def test_bloom_has_no_false_negatives_and_low_false_positives():
    bf = BloomFilter(10_000, fp_rate=0.01)
    members = [f"key-{i}" for i in range(10_000)]
    for m in members:
        bf.add(m)
    assert all(m in bf for m in members)
    others = [f"other-{i}" for i in range(100_000)]
    fp = sum(o in bf for o in others) / len(others)
    assert fp < 0.02, fp                                # target 1%, allow statistical slack
    assert abs(bf.expected_fp_rate() - 0.01) < 0.005


def test_bloom_sizing_formulas():
    bf = BloomFilter(1_000_000, 0.001)
    assert 14_300_000 < bf.m < 14_400_000              # about 14.38 bits per item for p = 0.1%
    assert bf.k == 10


def test_ring_is_deterministic_and_balanced():
    ring = HashRing([f"redis-{i}" for i in range(4)], vnodes=160)
    keys = [f"user:{i}" for i in range(40_000)]
    assert [ring.get(k) for k in keys[:100]] == [HashRing(sorted(ring.nodes), 160).get(k) for k in keys[:100]]
    counts = list(ring.distribution(keys).values())
    assert statistics.pstdev(counts) / statistics.mean(counts) < 0.10    # within ~10% of perfectly even


def test_adding_a_shard_moves_only_about_one_over_n_plus_one_of_keys():
    keys = [f"user:{i}" for i in range(40_000)]
    ring = HashRing([f"redis-{i}" for i in range(3)])
    before = {k: ring.get(k) for k in keys}
    ring.add("redis-3")
    moved = [k for k in keys if ring.get(k) != before[k]]
    assert 0.20 < len(moved) / len(keys) < 0.30                          # ideal 1/4
    assert all(ring.get(k) == "redis-3" for k in moved)                  # keys only move TO the new shard
    # compare: modulo hashing remaps about 3/4 of the keys
    mod_moved = sum(hash(k) % 3 != hash(k) % 4 for k in keys) / len(keys)
    assert mod_moved > 0.6


def test_removing_a_shard_only_moves_its_own_keys():
    keys = [f"k{random.Random(i).random()}" for i in range(10_000)]
    ring = HashRing(["a", "b", "c", "d"])
    before = {k: ring.get(k) for k in keys}
    ring.remove("b")
    assert all(ring.get(k) == before[k] for k in keys if before[k] != "b")
