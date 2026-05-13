"""LRU query-result cache for expensive read-only MCP tool operations.

Used by the MCP server to memoise results of ``cellar_stats``, ``find_wine``,
and similar pure-read queries.  Entries are invalidated when the underlying
Parquet data changes (via ``invalidate()`` after ETL reload) or when the
configured ``max_size`` is exceeded (LRU eviction).

Cache keys are deterministic hashes of the tool name plus normalised keyword
arguments, so identical calls collapse to a single lookup.

Entries automatically expire after ``ttl_seconds`` (default 300s / 5 min).
Set ``ttl_seconds=0`` to disable TTL-based expiry.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class CacheStats:
    """Snapshot of cache metrics."""

    hits: int
    misses: int
    evictions: int
    invalidations: int
    size: int
    max_size: int

    @property
    def hit_ratio(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


def _stable_key(name: str, params: dict[str, Any]) -> str:
    """Build a deterministic cache key from a tool name and kwargs."""
    payload = json.dumps(
        {"name": name, "params": params},
        sort_keys=True,
        default=str,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class QueryCache:
    """Thread-safe LRU cache with hit/miss/eviction tracking.

    The cache is intentionally simple: a single lock protects the
    ``OrderedDict``; entries are moved to the end on access; the oldest
    entry is evicted when ``max_size`` is exceeded.

    Args:
        max_size: Maximum number of entries before LRU eviction.
                  Set to 0 to disable caching entirely.
        ttl_seconds: Time-to-live for entries in seconds.
                     Set to 0 to disable TTL-based expiry.
    """

    def __init__(self, max_size: int = 128, ttl_seconds: int = 300) -> None:
        self._max_size = max(0, max_size)
        self._ttl = max(0, ttl_seconds)
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._invalidations = 0

    @property
    def enabled(self) -> bool:
        return self._max_size > 0

    def make_key(self, name: str, params: dict[str, Any]) -> str:
        """Return the canonical cache key for a (name, params) pair."""
        return _stable_key(name, params)

    def _is_expired(self, stored_at: float) -> bool:
        """Return True if the entry has exceeded its TTL."""
        if self._ttl == 0:
            return False
        return (time.monotonic() - stored_at) > self._ttl

    def get(self, key: str) -> tuple[bool, Any]:
        """Look up a key.  Returns ``(found, value)``."""
        if not self.enabled:
            return False, None
        with self._lock:
            if key in self._store:
                value, stored_at = self._store[key]
                if self._is_expired(stored_at):
                    del self._store[key]
                    self._misses += 1
                    return False, None
                self._hits += 1
                self._store.move_to_end(key)
                return True, value
            self._misses += 1
            return False, None

    def put(self, key: str, value: Any) -> None:
        """Insert a value, evicting the LRU entry if over capacity."""
        if not self.enabled:
            return
        with self._lock:
            now = time.monotonic()
            if key in self._store:
                self._store.move_to_end(key)
                self._store[key] = (value, now)
                return
            self._store[key] = (value, now)
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)
                self._evictions += 1

    def get_or_compute(
        self,
        name: str,
        params: dict[str, Any],
        compute: Callable[[], T],
    ) -> tuple[T, bool]:
        """Return cached value or compute and store it.

        Returns ``(value, cache_hit)``.
        """
        key = self.make_key(name, params)
        found, value = self.get(key)
        if found:
            return value, True
        value = compute()
        self.put(key, value)
        return value, False

    def invalidate(self) -> None:
        """Clear all cached entries (call after ETL reload)."""
        with self._lock:
            if self._store:
                self._invalidations += 1
            self._store.clear()

    def stats(self) -> CacheStats:
        with self._lock:
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                invalidations=self._invalidations,
                size=len(self._store),
                max_size=self._max_size,
            )

    def reset_stats(self) -> None:
        """Reset hit/miss counters (does not affect cached entries)."""
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._evictions = 0
            self._invalidations = 0
