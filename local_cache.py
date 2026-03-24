"""local_cache.py -- File-based cache (Tier 3 fallback).

A thread-safe, file-based cache that serves as the last-resort fallback
when Upstash Redis and Supabase caching are unavailable.

Features:
    - Singleton LocalFileCache with thread-safe operations
    - get/set/delete with automatic TTL expiration
    - Hit/miss statistics tracking
    - Background cleanup of expired entries
    - LRU eviction when cache directory exceeds 100MB

Storage: /tmp/nova_cache/ directory
    - One JSON file per key (filename = SHA256 hex digest of key)
    - Each file: {"value": ..., "created_at": ts, "expires_at": ts, "key": original}

Configuration:
    NOVA_CACHE_DIR       -- Override cache directory (default: /tmp/nova_cache)
    NOVA_CACHE_MAX_MB    -- Override max size in MB (default: 100)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# -- Configuration -----------------------------------------------------------

_DEFAULT_CACHE_DIR = "/tmp/nova_cache"
_DEFAULT_MAX_SIZE_MB = 100
_DEFAULT_TTL = 3600  # 1 hour

_CACHE_DIR: str = os.environ.get("NOVA_CACHE_DIR") or _DEFAULT_CACHE_DIR
_MAX_SIZE_BYTES: int = (
    int(os.environ.get("NOVA_CACHE_MAX_MB") or _DEFAULT_MAX_SIZE_MB) * 1024 * 1024
)


class LocalFileCache:
    """Thread-safe, file-based cache with TTL and LRU eviction.

    Implements a singleton pattern. Use LocalFileCache.instance() to get
    the shared instance.
    """

    _instance: Optional[LocalFileCache] = None
    _instance_lock = threading.Lock()

    def __init__(
        self, cache_dir: Optional[str] = None, max_size_bytes: Optional[int] = None
    ) -> None:
        """Initialize the file cache.

        Args:
            cache_dir: Directory for cache files. Defaults to /tmp/nova_cache.
            max_size_bytes: Max total size before LRU eviction. Defaults to 100MB.
        """
        self._cache_dir = Path(cache_dir or _CACHE_DIR)
        self._max_size_bytes = max_size_bytes or _MAX_SIZE_BYTES
        self._lock = threading.Lock()
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0

        # Ensure cache directory exists
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.error(
                "local_cache: failed to create cache dir %s: %s",
                self._cache_dir,
                exc,
                exc_info=True,
            )

    @classmethod
    def instance(
        cls, cache_dir: Optional[str] = None, max_size_bytes: Optional[int] = None
    ) -> LocalFileCache:
        """Get the singleton instance, creating it if necessary.

        Args:
            cache_dir: Directory for cache files (only used on first call).
            max_size_bytes: Max size (only used on first call).

        Returns:
            The shared LocalFileCache instance.
        """
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(
                        cache_dir=cache_dir, max_size_bytes=max_size_bytes
                    )
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (for testing)."""
        with cls._instance_lock:
            cls._instance = None

    def _key_to_path(self, key: str) -> Path:
        """Convert a cache key to its file path using SHA256."""
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self._cache_dir / f"{digest}.json"

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a cached value by key.

        Returns None if the key does not exist or has expired.
        Expired entries are deleted on access.

        Args:
            key: The cache key.

        Returns:
            The cached value, or None if missing/expired.
        """
        with self._lock:
            return self._get_impl(key)

    def _get_impl(self, key: str) -> Optional[Any]:
        """Inner get implementation (must be called under _lock)."""
        filepath = self._key_to_path(key)
        if not filepath.exists():
            self._misses += 1
            return None

        try:
            raw = filepath.read_text(encoding="utf-8")
            entry = json.loads(raw)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("local_cache: corrupt entry for key=%s: %s", key[:60], exc)
            self._safe_delete(filepath)
            self._misses += 1
            return None

        expires_at = entry.get("expires_at") or 0
        if expires_at > 0 and time.time() > expires_at:
            self._safe_delete(filepath)
            self._misses += 1
            return None

        self._hits += 1
        # Touch file to update atime for LRU
        try:
            filepath.touch(exist_ok=True)
        except OSError:
            pass

        return entry.get("value")

    def set(self, key: str, value: Any, ttl_seconds: int = _DEFAULT_TTL) -> bool:
        """Store a value in the cache with a TTL.

        Args:
            key: The cache key.
            value: The value to cache (must be JSON-serializable).
            ttl_seconds: Time-to-live in seconds. 0 means no expiration.

        Returns:
            True if stored successfully, False on error.
        """
        with self._lock:
            return self._set_impl(key, value, ttl_seconds)

    def _set_impl(self, key: str, value: Any, ttl_seconds: int) -> bool:
        """Inner set implementation (must be called under _lock)."""
        now = time.time()
        expires_at = now + ttl_seconds if ttl_seconds > 0 else 0

        entry = {
            "key": key,
            "value": value,
            "created_at": now,
            "expires_at": expires_at,
        }

        filepath = self._key_to_path(key)
        try:
            serialized = json.dumps(entry, default=str)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "local_cache: cannot serialize value for key=%s: %s", key[:60], exc
            )
            return False

        try:
            filepath.write_text(serialized, encoding="utf-8")
        except OSError as exc:
            logger.error(
                "local_cache: failed to write cache file %s: %s",
                filepath,
                exc,
                exc_info=True,
            )
            return False

        # Check if eviction is needed
        self._maybe_evict()
        return True

    def delete(self, key: str) -> bool:
        """Remove a cached entry by key.

        Args:
            key: The cache key to delete.

        Returns:
            True if the entry was deleted, False if it didn't exist.
        """
        with self._lock:
            filepath = self._key_to_path(key)
            return self._safe_delete(filepath)

    def clear(self) -> int:
        """Remove all cached files from the cache directory.

        Returns:
            The number of files removed.
        """
        with self._lock:
            return self._clear_impl()

    def _clear_impl(self) -> int:
        """Inner clear implementation (must be called under _lock)."""
        count = 0
        try:
            for filepath in self._cache_dir.glob("*.json"):
                if self._safe_delete(filepath):
                    count += 1
        except OSError as exc:
            logger.error("local_cache: error during clear: %s", exc, exc_info=True)
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Return cache statistics.

        Returns:
            Dictionary with hit_count, miss_count, hit_rate, miss_rate,
            entry_count, size_bytes, size_mb, eviction_count.
        """
        with self._lock:
            return self._get_stats_impl()

    def _get_stats_impl(self) -> Dict[str, Any]:
        """Inner stats implementation (must be called under _lock)."""
        total_size = 0
        entry_count = 0

        try:
            for filepath in self._cache_dir.glob("*.json"):
                try:
                    total_size += filepath.stat().st_size
                    entry_count += 1
                except OSError:
                    pass
        except OSError:
            pass

        total_requests = self._hits + self._misses
        hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0.0
        miss_rate = (self._misses / total_requests * 100) if total_requests > 0 else 0.0

        return {
            "hit_count": self._hits,
            "miss_count": self._misses,
            "hit_rate": round(hit_rate, 2),
            "miss_rate": round(miss_rate, 2),
            "entry_count": entry_count,
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2),
            "max_size_mb": round(self._max_size_bytes / (1024 * 1024), 2),
            "eviction_count": self._evictions,
            "cache_dir": str(self._cache_dir),
        }

    def cleanup_expired(self) -> int:
        """Remove all expired entries from the cache.

        This is safe to call from a background thread.

        Returns:
            The number of expired entries removed.
        """
        with self._lock:
            return self._cleanup_expired_impl()

    def _cleanup_expired_impl(self) -> int:
        """Inner cleanup implementation (must be called under _lock)."""
        removed = 0
        now = time.time()

        try:
            for filepath in self._cache_dir.glob("*.json"):
                try:
                    raw = filepath.read_text(encoding="utf-8")
                    entry = json.loads(raw)
                    expires_at = entry.get("expires_at") or 0
                    if expires_at > 0 and now > expires_at:
                        if self._safe_delete(filepath):
                            removed += 1
                except (OSError, json.JSONDecodeError, ValueError):
                    # Corrupt file -- remove it
                    self._safe_delete(filepath)
                    removed += 1
        except OSError as exc:
            logger.error("local_cache: error during cleanup: %s", exc, exc_info=True)

        if removed > 0:
            logger.info("local_cache: cleaned up %d expired entries", removed)
        return removed

    def _maybe_evict(self) -> None:
        """Run LRU eviction if total cache size exceeds the limit.

        Must be called under _lock. Evicts oldest-accessed files first.
        """
        files_with_stats: list[tuple[Path, float, int]] = []
        total_size = 0

        try:
            for filepath in self._cache_dir.glob("*.json"):
                try:
                    stat = filepath.stat()
                    # Use modification time as access proxy (touch updates mtime)
                    files_with_stats.append((filepath, stat.st_mtime, stat.st_size))
                    total_size += stat.st_size
                except OSError:
                    pass
        except OSError:
            return

        if total_size <= self._max_size_bytes:
            return

        # Sort by mtime ascending (oldest first = LRU candidates)
        files_with_stats.sort(key=lambda x: x[1])

        evicted = 0
        for filepath, _mtime, size in files_with_stats:
            if total_size <= self._max_size_bytes:
                break
            if self._safe_delete(filepath):
                total_size -= size
                evicted += 1
                self._evictions += 1

        if evicted > 0:
            logger.info(
                "local_cache: LRU evicted %d entries, size now %.1fMB",
                evicted,
                total_size / (1024 * 1024),
            )

    def _safe_delete(self, filepath: Path) -> bool:
        """Safely delete a file, returning True if it was removed."""
        try:
            filepath.unlink(missing_ok=True)
            return True
        except OSError as exc:
            logger.warning("local_cache: failed to delete %s: %s", filepath, exc)
            return False


# -- Module-level convenience functions --------------------------------------


def cache_get(key: str) -> Optional[Any]:
    """Get a value from the local file cache (convenience function).

    Args:
        key: The cache key.

    Returns:
        The cached value, or None.
    """
    return LocalFileCache.instance().get(key)


def cache_set(key: str, value: Any, ttl_seconds: int = _DEFAULT_TTL) -> bool:
    """Set a value in the local file cache (convenience function).

    Args:
        key: The cache key.
        value: JSON-serializable value.
        ttl_seconds: Time-to-live in seconds.

    Returns:
        True if stored, False on error.
    """
    return LocalFileCache.instance().set(key, value, ttl_seconds)


def cache_delete(key: str) -> bool:
    """Delete a value from the local file cache (convenience function).

    Args:
        key: The cache key.

    Returns:
        True if deleted, False otherwise.
    """
    return LocalFileCache.instance().delete(key)


def cache_clear() -> int:
    """Clear all entries from the local file cache.

    Returns:
        Number of entries removed.
    """
    return LocalFileCache.instance().clear()


def cache_stats() -> Dict[str, Any]:
    """Get cache statistics.

    Returns:
        Dictionary with hit/miss rates, size, and entry count.
    """
    return LocalFileCache.instance().get_stats()
