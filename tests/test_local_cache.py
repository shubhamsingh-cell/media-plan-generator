"""Tests for local_cache.py -- File-based cache (Tier 3 fallback)."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from local_cache import (
    LocalFileCache,
    cache_get,
    cache_set,
    cache_delete,
    cache_clear,
    cache_stats,
)


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    """Provide a temporary cache directory for each test."""
    d = tmp_path / "test_cache"
    d.mkdir()
    return d


@pytest.fixture
def cache(cache_dir: Path) -> LocalFileCache:
    """Provide a fresh LocalFileCache instance for each test."""
    LocalFileCache.reset_instance()
    return LocalFileCache(cache_dir=str(cache_dir), max_size_bytes=1024 * 1024)


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    """Reset the singleton before and after each test."""
    LocalFileCache.reset_instance()
    yield
    LocalFileCache.reset_instance()


class TestBasicOperations:
    """Tests for get/set/delete."""

    def test_set_and_get(self, cache: LocalFileCache) -> None:
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_key_returns_none(self, cache: LocalFileCache) -> None:
        assert cache.get("nonexistent") is None

    def test_set_overwrites_existing(self, cache: LocalFileCache) -> None:
        cache.set("key1", "old_value")
        cache.set("key1", "new_value")
        assert cache.get("key1") == "new_value"

    def test_delete_existing_key(self, cache: LocalFileCache) -> None:
        cache.set("key1", "value1")
        result = cache.delete("key1")
        assert result is True
        assert cache.get("key1") is None

    def test_delete_nonexistent_key(self, cache: LocalFileCache) -> None:
        result = cache.delete("nonexistent")
        assert result is True  # unlink(missing_ok=True)

    def test_set_returns_true_on_success(self, cache: LocalFileCache) -> None:
        assert cache.set("key1", "value1") is True

    def test_stores_complex_json(self, cache: LocalFileCache) -> None:
        data = {"name": "test", "items": [1, 2, 3], "nested": {"a": True}}
        cache.set("complex_key", data)
        result = cache.get("complex_key")
        assert result == data

    def test_stores_list(self, cache: LocalFileCache) -> None:
        cache.set("list_key", [1, "two", 3.0, None])
        assert cache.get("list_key") == [1, "two", 3.0, None]

    def test_stores_numeric_values(self, cache: LocalFileCache) -> None:
        cache.set("int_key", 42)
        cache.set("float_key", 3.14)
        assert cache.get("int_key") == 42
        assert cache.get("float_key") == 3.14

    def test_stores_none_value(self, cache: LocalFileCache) -> None:
        cache.set("none_key", None)
        # None is a valid value -- get returns None for missing AND stored None
        # but we can check the file exists
        filepath = cache._key_to_path("none_key")
        assert filepath.exists()

    def test_stores_empty_string(self, cache: LocalFileCache) -> None:
        cache.set("empty_key", "")
        assert cache.get("empty_key") == ""


class TestTTL:
    """Tests for TTL expiration."""

    def test_expired_entry_returns_none(self, cache: LocalFileCache) -> None:
        cache.set("expire_me", "value", ttl_seconds=1)
        time.sleep(1.1)
        assert cache.get("expire_me") is None

    def test_non_expired_entry_returns_value(self, cache: LocalFileCache) -> None:
        cache.set("keep_me", "value", ttl_seconds=60)
        assert cache.get("keep_me") == "value"

    def test_zero_ttl_never_expires(self, cache: LocalFileCache) -> None:
        cache.set("forever", "value", ttl_seconds=0)
        assert cache.get("forever") == "value"

    def test_expired_entry_is_deleted_on_access(
        self, cache: LocalFileCache, cache_dir: Path
    ) -> None:
        cache.set("expire_me", "value", ttl_seconds=1)
        filepath = cache._key_to_path("expire_me")
        assert filepath.exists()
        time.sleep(1.1)
        cache.get("expire_me")
        assert not filepath.exists()


class TestClear:
    """Tests for clear()."""

    def test_clear_removes_all_entries(self, cache: LocalFileCache) -> None:
        for i in range(5):
            cache.set(f"key_{i}", f"value_{i}")
        removed = cache.clear()
        assert removed == 5
        for i in range(5):
            assert cache.get(f"key_{i}") is None

    def test_clear_empty_cache(self, cache: LocalFileCache) -> None:
        removed = cache.clear()
        assert removed == 0

    def test_clear_resets_stats(self, cache: LocalFileCache) -> None:
        cache.set("k", "v")
        cache.get("k")
        cache.get("missing")
        cache.clear()
        stats = cache.get_stats()
        assert stats["hit_count"] == 0
        assert stats["miss_count"] == 0


class TestCleanupExpired:
    """Tests for cleanup_expired()."""

    def test_removes_expired_entries(self, cache: LocalFileCache) -> None:
        cache.set("expired1", "val", ttl_seconds=1)
        cache.set("expired2", "val", ttl_seconds=1)
        cache.set("alive", "val", ttl_seconds=3600)
        time.sleep(1.1)
        removed = cache.cleanup_expired()
        assert removed == 2
        assert cache.get("alive") == "val"

    def test_handles_corrupt_files(
        self, cache: LocalFileCache, cache_dir: Path
    ) -> None:
        # Write a corrupt file
        corrupt_file = (
            cache_dir
            / "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.json"
        )
        corrupt_file.write_text("not valid json{{{", encoding="utf-8")
        removed = cache.cleanup_expired()
        assert removed >= 1  # corrupt file removed


class TestStats:
    """Tests for get_stats()."""

    def test_hit_and_miss_tracking(self, cache: LocalFileCache) -> None:
        cache.set("k1", "v1")
        cache.get("k1")  # hit
        cache.get("k1")  # hit
        cache.get("missing")  # miss
        stats = cache.get_stats()
        assert stats["hit_count"] == 2
        assert stats["miss_count"] == 1
        assert stats["hit_rate"] == pytest.approx(66.67, abs=0.1)

    def test_entry_count(self, cache: LocalFileCache) -> None:
        for i in range(3):
            cache.set(f"k{i}", f"v{i}")
        stats = cache.get_stats()
        assert stats["entry_count"] == 3

    def test_size_bytes(self, cache: LocalFileCache) -> None:
        cache.set("k1", "a" * 1000)
        stats = cache.get_stats()
        assert stats["size_bytes"] > 1000

    def test_empty_cache_stats(self, cache: LocalFileCache) -> None:
        stats = cache.get_stats()
        assert stats["hit_count"] == 0
        assert stats["miss_count"] == 0
        assert stats["entry_count"] == 0
        assert stats["size_bytes"] == 0
        assert stats["hit_rate"] == 0.0


class TestLRUEviction:
    """Tests for LRU eviction."""

    def test_evicts_oldest_when_over_limit(self, cache_dir: Path) -> None:
        # Create a cache with a very small max size (10KB)
        cache = LocalFileCache(cache_dir=str(cache_dir), max_size_bytes=10 * 1024)

        # Write entries that exceed 10KB
        for i in range(20):
            cache.set(f"key_{i}", "x" * 1024)  # ~1KB each
            time.sleep(0.01)  # ensure different mtimes

        stats = cache.get_stats()
        # Should have evicted some entries to stay under 10KB
        assert stats["size_bytes"] <= 10 * 1024 + 2048  # small buffer for metadata


class TestSingleton:
    """Tests for singleton pattern."""

    def test_instance_returns_same_object(self, cache_dir: Path) -> None:
        a = LocalFileCache.instance(cache_dir=str(cache_dir))
        b = LocalFileCache.instance()
        assert a is b

    def test_reset_instance_allows_new_creation(self, cache_dir: Path) -> None:
        a = LocalFileCache.instance(cache_dir=str(cache_dir))
        LocalFileCache.reset_instance()
        b = LocalFileCache.instance(cache_dir=str(cache_dir))
        assert a is not b


class TestConvenienceFunctions:
    """Tests for module-level convenience functions."""

    def test_cache_get_set(self, cache_dir: Path) -> None:
        LocalFileCache.reset_instance()
        # Initialize singleton with test dir
        LocalFileCache.instance(cache_dir=str(cache_dir))

        cache_set("conv_key", "conv_value")
        assert cache_get("conv_key") == "conv_value"

    def test_cache_delete(self, cache_dir: Path) -> None:
        LocalFileCache.reset_instance()
        LocalFileCache.instance(cache_dir=str(cache_dir))

        cache_set("del_key", "del_value")
        cache_delete("del_key")
        assert cache_get("del_key") is None

    def test_cache_clear(self, cache_dir: Path) -> None:
        LocalFileCache.reset_instance()
        LocalFileCache.instance(cache_dir=str(cache_dir))

        cache_set("k1", "v1")
        cache_set("k2", "v2")
        removed = cache_clear()
        assert removed == 2

    def test_cache_stats(self, cache_dir: Path) -> None:
        LocalFileCache.reset_instance()
        LocalFileCache.instance(cache_dir=str(cache_dir))

        stats = cache_stats()
        assert isinstance(stats, dict)
        assert "hit_count" in stats


class TestThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_writes(self, cache: LocalFileCache) -> None:
        errors: list[str] = []

        def writer(start: int) -> None:
            try:
                for i in range(20):
                    cache.set(f"thread_{start}_{i}", f"value_{i}")
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0

    def test_concurrent_reads_and_writes(self, cache: LocalFileCache) -> None:
        cache.set("shared_key", "initial_value")
        errors: list[str] = []

        def reader() -> None:
            try:
                for _ in range(50):
                    cache.get("shared_key")
            except Exception as exc:
                errors.append(str(exc))

        def writer() -> None:
            try:
                for i in range(50):
                    cache.set("shared_key", f"value_{i}")
            except Exception as exc:
                errors.append(str(exc))

        threads = [
            threading.Thread(target=reader),
            threading.Thread(target=reader),
            threading.Thread(target=writer),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0


class TestKeyToPath:
    """Tests for _key_to_path."""

    def test_deterministic_hashing(self, cache: LocalFileCache) -> None:
        p1 = cache._key_to_path("test_key")
        p2 = cache._key_to_path("test_key")
        assert p1 == p2

    def test_different_keys_different_paths(self, cache: LocalFileCache) -> None:
        p1 = cache._key_to_path("key_a")
        p2 = cache._key_to_path("key_b")
        assert p1 != p2

    def test_path_ends_with_json(self, cache: LocalFileCache) -> None:
        p = cache._key_to_path("my_key")
        assert p.suffix == ".json"
