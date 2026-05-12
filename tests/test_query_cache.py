"""Tests for cellarbrain.query_cache — LRU query-result cache."""

from __future__ import annotations

import threading

import pytest

from cellarbrain.query_cache import CacheStats, QueryCache, _stable_key

# ---------------------------------------------------------------------------
# _stable_key
# ---------------------------------------------------------------------------


class TestStableKey:
    def test_deterministic(self):
        k1 = _stable_key("find_wine", {"query": "barolo", "limit": 10})
        k2 = _stable_key("find_wine", {"query": "barolo", "limit": 10})
        assert k1 == k2

    def test_param_order_independent(self):
        k1 = _stable_key("find_wine", {"query": "barolo", "limit": 10})
        k2 = _stable_key("find_wine", {"limit": 10, "query": "barolo"})
        assert k1 == k2

    def test_different_name_different_key(self):
        k1 = _stable_key("find_wine", {"query": "barolo"})
        k2 = _stable_key("cellar_stats", {"query": "barolo"})
        assert k1 != k2

    def test_different_params_different_key(self):
        k1 = _stable_key("find_wine", {"query": "barolo"})
        k2 = _stable_key("find_wine", {"query": "brunello"})
        assert k1 != k2

    def test_non_serializable_params_use_str(self):
        """Non-JSON params fall back to str() via default=str."""
        from datetime import datetime

        k = _stable_key("test", {"ts": datetime(2024, 1, 1)})
        assert isinstance(k, str) and len(k) == 64


# ---------------------------------------------------------------------------
# CacheStats
# ---------------------------------------------------------------------------


class TestCacheStats:
    def test_hit_ratio_zero_when_empty(self):
        s = CacheStats(hits=0, misses=0, evictions=0, invalidations=0, size=0, max_size=128)
        assert s.hit_ratio == 0.0

    def test_hit_ratio(self):
        s = CacheStats(hits=3, misses=7, evictions=0, invalidations=0, size=5, max_size=128)
        assert s.hit_ratio == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# QueryCache — basic operations
# ---------------------------------------------------------------------------


class TestQueryCacheBasic:
    def test_put_and_get(self):
        cache = QueryCache(max_size=10)
        cache.put("k1", "value1")
        found, val = cache.get("k1")
        assert found is True
        assert val == "value1"

    def test_get_miss(self):
        cache = QueryCache(max_size=10)
        found, val = cache.get("nonexistent")
        assert found is False
        assert val is None

    def test_put_overwrites(self):
        cache = QueryCache(max_size=10)
        cache.put("k1", "v1")
        cache.put("k1", "v2")
        found, val = cache.get("k1")
        assert found is True
        assert val == "v2"

    def test_stats_tracking(self):
        cache = QueryCache(max_size=10)
        cache.put("k1", "v1")
        cache.get("k1")  # hit
        cache.get("k1")  # hit
        cache.get("k2")  # miss
        s = cache.stats()
        assert s.hits == 2
        assert s.misses == 1
        assert s.size == 1

    def test_make_key(self):
        cache = QueryCache(max_size=10)
        key = cache.make_key("tool", {"a": 1})
        assert isinstance(key, str) and len(key) == 64


# ---------------------------------------------------------------------------
# QueryCache — LRU eviction
# ---------------------------------------------------------------------------


class TestQueryCacheLRU:
    def test_evicts_oldest(self):
        cache = QueryCache(max_size=3)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)
        cache.put("d", 4)  # evicts "a"
        found, _ = cache.get("a")
        assert found is False
        s = cache.stats()
        assert s.evictions == 1
        assert s.size == 3

    def test_access_refreshes_lru(self):
        cache = QueryCache(max_size=3)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)
        cache.get("a")  # refresh "a"
        cache.put("d", 4)  # evicts "b" (oldest untouched)
        found_a, _ = cache.get("a")
        found_b, _ = cache.get("b")
        assert found_a is True
        assert found_b is False

    def test_put_refreshes_lru(self):
        cache = QueryCache(max_size=3)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)
        cache.put("a", 10)  # refresh "a" via overwrite
        cache.put("d", 4)  # evicts "b"
        found_a, val = cache.get("a")
        found_b, _ = cache.get("b")
        assert found_a is True
        assert val == 10
        assert found_b is False


# ---------------------------------------------------------------------------
# QueryCache — invalidation
# ---------------------------------------------------------------------------


class TestQueryCacheInvalidation:
    def test_invalidate_clears_all(self):
        cache = QueryCache(max_size=10)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.invalidate()
        s = cache.stats()
        assert s.size == 0
        assert s.invalidations == 1

    def test_invalidate_empty_no_count(self):
        cache = QueryCache(max_size=10)
        cache.invalidate()
        assert cache.stats().invalidations == 0

    def test_reset_stats(self):
        cache = QueryCache(max_size=10)
        cache.put("a", 1)
        cache.get("a")
        cache.get("b")
        cache.reset_stats()
        s = cache.stats()
        assert s.hits == 0
        assert s.misses == 0
        assert s.size == 1  # entries preserved


# ---------------------------------------------------------------------------
# QueryCache — get_or_compute
# ---------------------------------------------------------------------------


class TestGetOrCompute:
    def test_computes_on_miss(self):
        cache = QueryCache(max_size=10)
        val, hit = cache.get_or_compute("tool", {"x": 1}, lambda: 42)
        assert val == 42
        assert hit is False

    def test_returns_cached_on_hit(self):
        cache = QueryCache(max_size=10)
        cache.get_or_compute("tool", {"x": 1}, lambda: 42)
        val, hit = cache.get_or_compute("tool", {"x": 1}, lambda: 99)
        assert val == 42
        assert hit is True

    def test_compute_not_called_on_hit(self):
        cache = QueryCache(max_size=10)
        cache.get_or_compute("tool", {"x": 1}, lambda: 42)
        called = []
        cache.get_or_compute("tool", {"x": 1}, lambda: called.append(1) or 99)
        assert called == []


# ---------------------------------------------------------------------------
# QueryCache — disabled (max_size=0)
# ---------------------------------------------------------------------------


class TestQueryCacheDisabled:
    def test_disabled_always_misses(self):
        cache = QueryCache(max_size=0)
        assert cache.enabled is False
        cache.put("k", "v")
        found, val = cache.get("k")
        assert found is False

    def test_get_or_compute_always_computes(self):
        cache = QueryCache(max_size=0)
        counter = [0]

        def compute():
            counter[0] += 1
            return counter[0]

        v1, h1 = cache.get_or_compute("t", {}, compute)
        v2, h2 = cache.get_or_compute("t", {}, compute)
        assert v1 == 1
        assert v2 == 2
        assert h1 is False
        assert h2 is False


# ---------------------------------------------------------------------------
# QueryCache — thread safety
# ---------------------------------------------------------------------------


class TestQueryCacheThreadSafety:
    def test_concurrent_put_get(self):
        cache = QueryCache(max_size=100)
        errors = []

        def writer(start: int):
            try:
                for i in range(start, start + 50):
                    cache.put(f"k{i}", i)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(100):
                    cache.get("k25")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer, args=(0,)),
            threading.Thread(target=writer, args=(50,)),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        s = cache.stats()
        assert s.size <= 100
