#!/usr/bin/env python3
"""resilience_router.py -- Unified resilience layer with priority-based fallback chains.

Wraps every external service dependency in a tiered fallback chain with
per-tier circuit breakers.  Products can gradually migrate to use the router
instead of calling individual service modules directly.

Service Categories:
    1. CACHING    -- Upstash Redis -> Supabase cache -> Memory dict -> File cache
    2. DATABASE   -- Supabase PostgREST -> Local JSON -> Memory KB
    3. EMAIL      -- Resend -> Slack -> Sentry Breadcrumb -> Local log
    4. ANALYTICS  -- PostHog -> Local file -> Memory counter
    5. ERRORS     -- Sentry -> Local file -> Email -> Stderr
    6. LOGGING    -- Local file -> Stderr

Thread-safe, stdlib-only (no new pip packages).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_BASE_DIR = Path(__file__).resolve().parent
_DATA_DIR = _BASE_DIR / "data"
_CACHE_DIR = _DATA_DIR / "cache"
_LOCAL_DB_DIR = _DATA_DIR / "local_db"
_ANALYTICS_DIR = _DATA_DIR / "analytics"
_ERRORS_DIR = _DATA_DIR / "errors"
_LOGS_DIR = _DATA_DIR / "logs"

# Ensure directories exist at import time
for _dir in (_CACHE_DIR, _LOCAL_DB_DIR, _ANALYTICS_DIR, _ERRORS_DIR, _LOGS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================


class CircuitBreaker:
    """Per-tier circuit breaker with configurable failure threshold and cooldown.

    When failure count reaches ``max_failures``, the breaker opens and
    the tier is disabled until ``cooldown_seconds`` have elapsed.

    Thread-safe: all mutations are guarded by a lock.
    """

    __slots__ = (
        "_lock",
        "failures",
        "max_failures",
        "cooldown_seconds",
        "disabled_until",
        "total_successes",
        "total_failures",
        "last_failure_time",
        "last_failure_reason",
        "_consecutive_failures",
        "_on_open_callbacks",
        "_on_close_callbacks",
    )

    def __init__(self, max_failures: int = 3, cooldown_seconds: int = 3600) -> None:
        self._lock = threading.Lock()
        self.failures: int = 0
        self.max_failures: int = max_failures
        self.cooldown_seconds: int = cooldown_seconds
        self.disabled_until: float = 0.0
        self.total_successes: int = 0
        self.total_failures: int = 0
        self.last_failure_time: float = 0.0
        self.last_failure_reason: str = ""
        self._consecutive_failures: int = 0
        self._on_open_callbacks: list = []
        self._on_close_callbacks: list = []

    def on_open(self, callback: Any) -> None:
        """Register a callback fired when the circuit opens."""
        self._on_open_callbacks.append(callback)

    def on_close(self, callback: Any) -> None:
        """Register a callback fired when the circuit closes (recovers)."""
        self._on_close_callbacks.append(callback)

    def is_open(self) -> bool:
        """Return True if the circuit is open (tier should be skipped)."""
        fire_close = False
        with self._lock:
            if self.failures < self.max_failures:
                return False
            if time.time() >= self.disabled_until:
                # Cooldown expired -- half-open: reset and allow one attempt
                self.failures = 0
                self.disabled_until = 0.0
                self._consecutive_failures = 0
                fire_close = True
            else:
                return True
        # Fire close callbacks outside the lock to avoid deadlocks
        if fire_close:
            for cb in self._on_close_callbacks:
                try:
                    cb(self)
                except Exception as exc:
                    logger.debug("Circuit breaker on_close callback failed: %s", exc)
        return False

    def record_success(self) -> None:
        """Record a successful call -- resets failure count."""
        fire_close = False
        with self._lock:
            was_open = self.failures >= self.max_failures
            self.failures = 0
            self.disabled_until = 0.0
            self._consecutive_failures = 0
            self.total_successes += 1
            if was_open:
                fire_close = True
        if fire_close:
            for cb in self._on_close_callbacks:
                try:
                    cb(self)
                except Exception as exc:
                    logger.debug("Circuit breaker on_close callback failed: %s", exc)

    def _get_cooldown_with_jitter(self) -> float:
        """Exponential backoff with jitter for circuit recovery.

        Returns:
            Cooldown period in seconds with exponential backoff and random jitter.
        """
        import random

        base = self.cooldown_seconds
        multiplier = min(2**self._consecutive_failures, 8)
        jitter = random.uniform(0, base * 0.3)
        return base * multiplier + jitter

    def record_failure(self, reason: str = "") -> None:
        """Record a failed call. Opens the circuit after max_failures."""
        fire_open = False
        with self._lock:
            self.failures += 1
            self.total_failures += 1
            self._consecutive_failures += 1
            self.last_failure_time = time.time()
            self.last_failure_reason = reason[:200]
            if self.failures >= self.max_failures:
                cooldown = self._get_cooldown_with_jitter()
                self.disabled_until = time.time() + cooldown
                fire_open = True
        if fire_open:
            for cb in self._on_open_callbacks:
                try:
                    cb(self)
                except Exception as exc:
                    logger.debug("Circuit breaker on_open callback failed: %s", exc)

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self.failures = 0
            self.disabled_until = 0.0

    def snapshot(self) -> Dict[str, Any]:
        """Return a serializable snapshot of the breaker state."""
        with self._lock:
            is_open = (
                self.failures >= self.max_failures and time.time() < self.disabled_until
            )
            return {
                "failures": self.failures,
                "max_failures": self.max_failures,
                "is_open": is_open,
                "disabled_until_iso": (
                    datetime.fromtimestamp(
                        self.disabled_until, tz=timezone.utc
                    ).isoformat()
                    if self.disabled_until > 0
                    else None
                ),
                "cooldown_seconds": self.cooldown_seconds,
                "total_successes": self.total_successes,
                "total_failures": self.total_failures,
                "last_failure_time": self.last_failure_time or None,
                "last_failure_reason": self.last_failure_reason or None,
            }


# =============================================================================
# SERVICE TIER
# =============================================================================


class ServiceTier:
    """Represents one fallback tier for a service.

    Attributes:
        name: Human-readable tier name (e.g., 'Upstash Redis').
        provider: Provider identifier (e.g., 'upstash').
        priority: Lower number = higher priority.
        is_configured: Whether env vars / deps are present.
        cost_label: One of 'free', 'freemium', 'paid', 'local'.
        rate_limit_info: Human-readable rate limit description.
        circuit_breaker: Circuit breaker instance for this tier.
    """

    __slots__ = (
        "name",
        "provider",
        "priority",
        "is_configured",
        "cost_label",
        "rate_limit_info",
        "circuit_breaker",
    )

    def __init__(
        self,
        name: str,
        provider: str,
        priority: int,
        is_configured: bool,
        max_failures: int = 3,
        cooldown_seconds: int = 3600,
        cost_label: str = "free",
        rate_limit_info: str = "unlimited",
    ) -> None:
        self.name = name
        self.provider = provider
        self.priority = priority
        self.is_configured = is_configured
        self.cost_label = cost_label
        self.rate_limit_info = rate_limit_info
        self.circuit_breaker = CircuitBreaker(
            max_failures=max_failures, cooldown_seconds=cooldown_seconds
        )

    def is_available(self) -> bool:
        """Return True if this tier can be attempted right now."""
        return self.is_configured and not self.circuit_breaker.is_open()

    def status_label(self) -> str:
        """Return a human-readable status label for dashboards."""
        if not self.is_configured:
            return "NOT CONFIGURED"
        if self.circuit_breaker.is_open():
            return "CIRCUIT OPEN"
        return "OK"

    def snapshot(self) -> Dict[str, Any]:
        """Return a serializable snapshot of this tier."""
        return {
            "name": self.name,
            "provider": self.provider,
            "priority": self.priority,
            "is_configured": self.is_configured,
            "cost_label": self.cost_label,
            "rate_limit_info": self.rate_limit_info,
            "status": self.status_label(),
            "circuit_breaker": self.circuit_breaker.snapshot(),
        }


# =============================================================================
# IN-MEMORY CACHE (Tier 3 for Caching)
# =============================================================================


class MemoryCache:
    """Thread-safe in-memory dict with TTL support.

    Lost on restart but always available.
    """

    def __init__(self, max_entries: int = 10000) -> None:
        self._lock = threading.Lock()
        self._store: Dict[str, tuple[float, Any]] = {}
        self._max_entries = max_entries

    def get(self, key: str) -> Optional[Any]:
        """Get a value if not expired. Returns None on miss."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at > 0 and time.time() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, data: Any, ttl_seconds: int = 86400) -> None:
        """Store a value with TTL."""
        with self._lock:
            expires_at = time.time() + ttl_seconds if ttl_seconds > 0 else 0.0
            self._store[key] = (expires_at, data)
            # Evict oldest entries if over capacity
            if len(self._store) > self._max_entries:
                oldest_key = min(self._store, key=lambda k: self._store[k][0])
                del self._store[oldest_key]

    def delete(self, key: str) -> None:
        """Delete a key."""
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> int:
        """Clear all entries, return count cleared."""
        with self._lock:
            count = len(self._store)
            self._store.clear()
            return count

    def size(self) -> int:
        """Return number of entries."""
        with self._lock:
            return len(self._store)


# =============================================================================
# FILE-BASED CACHE (Tier 4 for Caching)
# =============================================================================


class FileCache:
    """File-based cache in data/cache/ directory.

    Survives restarts. Each key is a separate JSON file.
    """

    def __init__(self, cache_dir: Path) -> None:
        self._dir = cache_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _key_to_path(self, key: str) -> Path:
        """Convert a cache key to a safe file path."""
        safe_key = "".join(c if c.isalnum() or c in "-_." else "_" for c in key)
        return self._dir / f"{safe_key[:200]}.json"

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value from file. Returns None on miss/expired."""
        path = self._key_to_path(key)
        with self._lock:
            if not path.exists():
                return None
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                expires_at = raw.get("expires_at", 0)
                if expires_at > 0 and time.time() > expires_at:
                    path.unlink(missing_ok=True)
                    return None
                return raw.get("data")
            except (json.JSONDecodeError, OSError, KeyError):
                return None

    def set(self, key: str, data: Any, ttl_seconds: int = 86400) -> None:
        """Store a value as a JSON file with TTL metadata."""
        path = self._key_to_path(key)
        expires_at = time.time() + ttl_seconds if ttl_seconds > 0 else 0.0
        payload = {
            "key": key,
            "data": data,
            "expires_at": expires_at,
            "created_at": time.time(),
        }
        with self._lock:
            try:
                path.write_text(
                    json.dumps(payload, ensure_ascii=False, default=str),
                    encoding="utf-8",
                )
            except (OSError, TypeError, ValueError) as exc:
                logger.debug(f"FileCache write error for {key}: {exc}")

    def delete(self, key: str) -> None:
        """Delete a cached file."""
        path = self._key_to_path(key)
        with self._lock:
            path.unlink(missing_ok=True)


# =============================================================================
# LOCAL JSON DATABASE (Tier 2 for Database)
# =============================================================================


class LocalJSONDB:
    """Local JSON file storage for database fallback.

    Each table is stored as a separate JSON file in data/local_db/.
    """

    def __init__(self, db_dir: Path) -> None:
        self._dir = db_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _table_path(self, table: str) -> Path:
        """Get the file path for a table."""
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in table)
        return self._dir / f"{safe}.json"

    def read_table(self, table: str) -> list[dict[str, Any]]:
        """Read all rows from a table file."""
        path = self._table_path(table)
        with self._lock:
            if not path.exists():
                return []
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    return data
                return [data] if isinstance(data, dict) else []
            except (json.JSONDecodeError, OSError):
                return []

    def write_table(self, table: str, rows: list[dict[str, Any]]) -> bool:
        """Write rows to a table file (overwrites)."""
        path = self._table_path(table)
        with self._lock:
            try:
                path.write_text(
                    json.dumps(rows, ensure_ascii=False, default=str, indent=2),
                    encoding="utf-8",
                )
                return True
            except (OSError, TypeError, ValueError) as exc:
                logger.error(
                    f"LocalJSONDB write error for {table}: {exc}", exc_info=True
                )
                return False

    def append_row(self, table: str, row: dict[str, Any]) -> bool:
        """Append a single row to a table file."""
        rows = self.read_table(table)
        rows.append(row)
        return self.write_table(table, rows)


# =============================================================================
# EVENT LOGGER (Tier 2 for Analytics)
# =============================================================================


class LocalEventLogger:
    """Writes analytics events to a JSON-lines file in data/analytics/."""

    def __init__(self, log_dir: Path) -> None:
        self._dir = log_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _current_file(self) -> Path:
        """Get today's event log file."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self._dir / f"events_{date_str}.jsonl"

    def log_event(
        self, event: str, properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Append an event to the local log file."""
        entry = {
            "event": event,
            "properties": properties or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            try:
                with open(self._current_file(), "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, default=str) + "\n")
                return True
            except OSError as exc:
                logger.debug(f"LocalEventLogger write error: {exc}")
                return False


# =============================================================================
# ERROR LOGGER (Tier 2 for Errors)
# =============================================================================


class LocalErrorLogger:
    """Writes structured error reports to data/errors/ as JSON files."""

    def __init__(self, error_dir: Path) -> None:
        self._dir = error_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _current_file(self) -> Path:
        """Get today's error log file."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self._dir / f"errors_{date_str}.jsonl"

    def log_error(
        self,
        error: BaseException,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Append a structured error entry to the log file."""
        entry = {
            "type": type(error).__name__,
            "message": str(error)[:1000],
            "context": context or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            try:
                with open(self._current_file(), "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, default=str) + "\n")
                return True
            except OSError as exc:
                logger.debug(f"LocalErrorLogger write error: {exc}")
                return False


# =============================================================================
# STRUCTURED LOG WRITER (Tier 2 for Logging)
# =============================================================================


class LocalStructuredLogger:
    """Writes structured JSON log lines to data/logs/, rotated daily."""

    def __init__(self, log_dir: Path) -> None:
        self._dir = log_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _current_file(self) -> Path:
        """Get today's structured log file."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self._dir / f"structured_{date_str}.jsonl"

    def log(self, level: str, message: str, **kwargs: Any) -> bool:
        """Write a structured log entry."""
        entry = {
            "level": level,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        with self._lock:
            try:
                with open(self._current_file(), "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, default=str) + "\n")
                return True
            except OSError as exc:
                logger.debug(f"LocalStructuredLogger write error: {exc}")
                return False


# =============================================================================
# MEMORY COUNTER (Tier 3 for Analytics)
# =============================================================================


class MemoryCounter:
    """In-memory event counter for basic analytics when all else fails."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: Dict[str, int] = {}
        self._started_at = time.time()

    def increment(self, event: str) -> None:
        """Increment the counter for an event type."""
        with self._lock:
            self._counts[event] = self._counts.get(event, 0) + 1

    def get_counts(self) -> Dict[str, int]:
        """Return a copy of all counters."""
        with self._lock:
            return dict(self._counts)

    def snapshot(self) -> Dict[str, Any]:
        """Return a serializable snapshot."""
        with self._lock:
            return {
                "counts": dict(self._counts),
                "total": sum(self._counts.values()),
                "uptime_seconds": round(time.time() - self._started_at, 1),
            }


# =============================================================================
# BULKHEAD LIMITER
# =============================================================================


class BulkheadLimiter:
    """Limits concurrent calls to a service to prevent resource starvation.

    Uses a semaphore-based approach to cap the number of in-flight requests
    per service category. Thread-safe.
    """

    def __init__(self, max_concurrent: int = 10) -> None:
        self._semaphore = threading.Semaphore(max_concurrent)
        self._max = max_concurrent
        self._active = 0
        self._lock = threading.Lock()

    def acquire(self, timeout: float = 5.0) -> bool:
        """Acquire a slot. Returns True if acquired within timeout."""
        acquired = self._semaphore.acquire(timeout=timeout)
        if acquired:
            with self._lock:
                self._active += 1
        return acquired

    def release(self) -> None:
        """Release a slot."""
        self._semaphore.release()
        with self._lock:
            self._active = max(0, self._active - 1)

    @property
    def active_count(self) -> int:
        """Return the number of active concurrent calls."""
        with self._lock:
            return self._active

    def get_status(self) -> Dict[str, Any]:
        """Return bulkhead status for dashboards."""
        return {"max_concurrent": self._max, "active": self.active_count}


# =============================================================================
# ADAPTIVE TIMEOUT TRACKER
# =============================================================================


class AdaptiveTimeoutTracker:
    """Track per-tier latency and dynamically adjust timeouts.

    Maintains a rolling window of latencies per tier name and computes
    p95 to set adaptive timeouts. The timeout is clamped between a
    configurable minimum and maximum.
    """

    def __init__(
        self,
        min_timeout: float = 2.0,
        max_timeout: float = 30.0,
        multiplier: float = 1.5,
    ) -> None:
        self._lock = threading.Lock()
        self._latencies: Dict[str, deque] = {}
        self._min_timeout = min_timeout
        self._max_timeout = max_timeout
        self._multiplier = multiplier

    def record_latency(self, tier_name: str, latency_seconds: float) -> None:
        """Record a request latency for a tier."""
        with self._lock:
            if tier_name not in self._latencies:
                self._latencies[tier_name] = deque(maxlen=100)
            self._latencies[tier_name].append(latency_seconds)

    def get_timeout(self, tier_name: str) -> float:
        """Get the adaptive timeout for a tier based on p95 latency.

        Returns:
            Timeout in seconds, clamped between min and max.
        """
        with self._lock:
            lats = self._latencies.get(tier_name)
            if not lats or len(lats) < 5:
                return self._max_timeout
            sorted_lats = sorted(lats)
            idx = int(len(sorted_lats) * 0.95)
            p95 = sorted_lats[min(idx, len(sorted_lats) - 1)]
            adaptive = p95 * self._multiplier
            return max(self._min_timeout, min(self._max_timeout, adaptive))

    def get_all_timeouts(self) -> Dict[str, float]:
        """Return adaptive timeouts for all tracked tiers."""
        with self._lock:
            result: Dict[str, float] = {}
            for tier_name in self._latencies:
                lats = self._latencies[tier_name]
                if not lats or len(lats) < 5:
                    result[tier_name] = self._max_timeout
                    continue
                sorted_lats = sorted(lats)
                idx = int(len(sorted_lats) * 0.95)
                p95 = sorted_lats[min(idx, len(sorted_lats) - 1)]
                adaptive = p95 * self._multiplier
                result[tier_name] = max(
                    self._min_timeout, min(self._max_timeout, adaptive)
                )
            return result

    def get_status(self) -> Dict[str, Any]:
        """Return tracker status for dashboards."""
        timeouts = self.get_all_timeouts()
        with self._lock:
            counts = {k: len(v) for k, v in self._latencies.items()}
        return {
            "adaptive_timeouts": timeouts,
            "sample_counts": counts,
            "min_timeout": self._min_timeout,
            "max_timeout": self._max_timeout,
        }


# =============================================================================
# RESILIENCE ROUTER
# =============================================================================


class ResilienceRouter:
    """Master router that manages all service fallback chains.

    Instantiate once at app startup. Each service category has a list
    of tiers sorted by priority.  The router tries each tier in order
    until one succeeds, recording circuit-breaker state along the way.
    """

    def __init__(self) -> None:
        # -- Local fallback instances (always available) -----------------------
        self._memory_cache = MemoryCache()
        self._file_cache = FileCache(_CACHE_DIR)
        self._local_db = LocalJSONDB(_LOCAL_DB_DIR)
        self._event_logger = LocalEventLogger(_ANALYTICS_DIR)
        self._error_logger = LocalErrorLogger(_ERRORS_DIR)
        self._structured_logger = LocalStructuredLogger(_LOGS_DIR)
        self._memory_counter = MemoryCounter()

        # -- Fallback quality tracking -----------------------------------------
        self._fallback_lock = threading.Lock()
        self._fallback_usage: Dict[str, int] = {}  # tier_name -> usage count

        # -- Bulkhead limiters per service category ----------------------------
        self._bulkheads: Dict[str, BulkheadLimiter] = {}

        # -- Adaptive timeout tracker ------------------------------------------
        self._adaptive_timeouts = AdaptiveTimeoutTracker()

        # -- Build service tier definitions -----------------------------------
        self._tiers: Dict[str, List[ServiceTier]] = {}
        self._init_tiers()

        # -- Initialize bulkheads for each service category -------------------
        for category in self._tiers:
            self._bulkheads[category] = BulkheadLimiter(max_concurrent=10)

        logger.info(
            "[resilience_router] Initialized with %d service categories",
            len(self._tiers),
        )

    # ------------------------------------------------------------------
    # Tier initialization
    # ------------------------------------------------------------------

    def _init_tiers(self) -> None:
        """Build all service fallback chains from environment config."""

        # -- Shared env checks ---
        upstash_url = (
            os.environ.get("UPSTASH_REDIS_REST_URL")
            or os.environ.get("UPSTASH_REDIS_URL")
            or ""
        ).strip()
        upstash_token = (
            os.environ.get("UPSTASH_REDIS_REST_TOKEN")
            or os.environ.get("UPSTASH_REDIS_TOKEN")
            or ""
        ).strip()
        supabase_url = (os.environ.get("SUPABASE_URL") or "").strip()
        supabase_key = (os.environ.get("SUPABASE_ANON_KEY") or "").strip()
        resend_key = (os.environ.get("RESEND_API_KEY") or "").strip()
        sentry_dsn = (os.environ.get("SENTRY_DSN") or "").strip()

        # 1. CACHING
        self._tiers["caching"] = [
            ServiceTier(
                "Upstash Redis",
                "upstash",
                1,
                bool(upstash_url and upstash_token),
                max_failures=5,
                cooldown_seconds=300,
                cost_label="freemium",
                rate_limit_info="10K req/day free",
            ),
            ServiceTier(
                "Supabase Cache",
                "supabase_cache",
                2,
                bool(supabase_url and supabase_key),
                max_failures=3,
                cooldown_seconds=600,
                cost_label="freemium",
                rate_limit_info="500MB free tier",
            ),
            ServiceTier(
                "Memory Dict",
                "memory",
                3,
                True,
                max_failures=999,
                cooldown_seconds=60,
                cost_label="local",
                rate_limit_info="unlimited (ephemeral)",
            ),
            ServiceTier(
                "File Cache",
                "file",
                4,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="local",
                rate_limit_info="unlimited (disk)",
            ),
        ]

        # 2. DATABASE
        self._tiers["database"] = [
            ServiceTier(
                "Supabase PostgREST",
                "supabase",
                1,
                bool(supabase_url and supabase_key),
                max_failures=3,
                cooldown_seconds=600,
                cost_label="freemium",
                rate_limit_info="500MB / 50K rows free",
            ),
            ServiceTier(
                "Local JSON",
                "local_json",
                2,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="local",
                rate_limit_info="unlimited (disk)",
            ),
            ServiceTier(
                "Memory KB",
                "memory_kb",
                3,
                True,
                max_failures=999,
                cooldown_seconds=60,
                cost_label="local",
                rate_limit_info="unlimited (ephemeral)",
            ),
        ]

        # 3. EMAIL
        slack_configured = bool((os.environ.get("SLACK_BOT_TOKEN") or "").strip())

        self._tiers["email"] = [
            ServiceTier(
                "Resend API",
                "resend",
                1,
                bool(resend_key),
                max_failures=3,
                cooldown_seconds=1800,
                cost_label="freemium",
                rate_limit_info="100 emails/day free",
            ),
            ServiceTier(
                "Slack Notification",
                "slack",
                2,
                slack_configured,
                max_failures=5,
                cooldown_seconds=900,
                cost_label="freemium",
                rate_limit_info="1 msg/sec",
            ),
            ServiceTier(
                "Sentry Breadcrumb",
                "sentry_breadcrumb",
                3,
                bool(sentry_dsn),
                max_failures=999,
                cooldown_seconds=60,
                cost_label="freemium",
                rate_limit_info="piggyback on error tracking",
            ),
            ServiceTier(
                "Local Log File",
                "local_log",
                4,
                True,
                max_failures=999,
                cooldown_seconds=60,
                cost_label="local",
                rate_limit_info="unlimited (disk)",
            ),
        ]

        # 4. ANALYTICS
        posthog_key = (os.environ.get("POSTHOG_API_KEY") or "").strip()

        self._tiers["analytics"] = [
            ServiceTier(
                "PostHog",
                "posthog",
                1,
                bool(posthog_key),
                max_failures=5,
                cooldown_seconds=600,
                cost_label="freemium",
                rate_limit_info="1M events/mo free",
            ),
            ServiceTier(
                "Sentry Breadcrumbs",
                "sentry_breadcrumb",
                2,
                bool(sentry_dsn),
                max_failures=5,
                cooldown_seconds=600,
                cost_label="freemium",
                rate_limit_info="piggyback on error tracking",
            ),
            ServiceTier(
                "Local Event File",
                "local_file",
                3,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="local",
                rate_limit_info="unlimited (disk)",
            ),
            ServiceTier(
                "Memory Counter",
                "memory_counter",
                4,
                True,
                max_failures=999,
                cooldown_seconds=60,
                cost_label="local",
                rate_limit_info="unlimited (ephemeral)",
            ),
        ]

        # 5. ERRORS
        self._tiers["errors"] = [
            ServiceTier(
                "Sentry",
                "sentry",
                1,
                bool(sentry_dsn),
                max_failures=5,
                cooldown_seconds=600,
                cost_label="freemium",
                rate_limit_info="5K errors/mo free",
            ),
            ServiceTier(
                "Local Error File",
                "local_file",
                2,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="local",
                rate_limit_info="unlimited (disk)",
            ),
            ServiceTier(
                "Email Alert",
                "email_alert",
                3,
                bool(resend_key),
                max_failures=5,
                cooldown_seconds=1800,
                cost_label="freemium",
                rate_limit_info="100/day free",
            ),
            ServiceTier(
                "Stderr Log",
                "stderr",
                4,
                True,
                max_failures=999,
                cooldown_seconds=60,
                cost_label="local",
                rate_limit_info="unlimited",
            ),
        ]

        # 6. LOGGING
        self._tiers["logging"] = [
            ServiceTier(
                "Local Structured File",
                "local_file",
                1,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="local",
                rate_limit_info="unlimited (disk)",
            ),
            ServiceTier(
                "Stderr Log",
                "stderr",
                2,
                True,
                max_failures=999,
                cooldown_seconds=60,
                cost_label="local",
                rate_limit_info="unlimited",
            ),
        ]

        # 7. WEB SCRAPING (delegates to web_scraper_router.py)
        # S72: Firecrawl tier removed (module deleted, credits exhausted).
        # Apify (handled inside web_scraper_router) is now the paid Tier 1;
        # this resilience-router view summarizes the free / fallback tiers.
        jina_key = (os.environ.get("JINA_API_KEY") or "").strip()
        tavily_key = (os.environ.get("TAVILY_API_KEY") or "").strip()
        self._tiers["web_scraping"] = [
            ServiceTier(
                "Jina AI Reader",
                "jina",
                1,
                True,
                max_failures=5,
                cooldown_seconds=600,
                cost_label="free",
                rate_limit_info="no auth needed for basic",
            ),
            ServiceTier(
                "Tavily Search",
                "tavily",
                2,
                bool(tavily_key),
                max_failures=3,
                cooldown_seconds=1800,
                cost_label="freemium",
                rate_limit_info="1K free/mo",
            ),
            ServiceTier(
                "Direct urllib",
                "urllib",
                3,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="free",
                rate_limit_info="unlimited (stdlib)",
            ),
        ]

        # 8. DECK GENERATION (delegates to deck_generator.py)
        gamma_key = (os.environ.get("GAMMA_API_KEY") or "").strip()
        presenton_url = (os.environ.get("PRESENTON_URL") or "").strip()

        self._tiers["deck_generation"] = [
            ServiceTier(
                "Gamma MCP",
                "gamma",
                1,
                bool(gamma_key),
                max_failures=3,
                cooldown_seconds=3600,
                cost_label="freemium",
                rate_limit_info="~10 free/mo",
            ),
            ServiceTier(
                "python-pptx",
                "pptx",
                2,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="free",
                rate_limit_info="unlimited (offline)",
            ),
            ServiceTier(
                "HTML-to-PDF",
                "html_pdf",
                3,
                True,
                max_failures=10,
                cooldown_seconds=120,
                cost_label="free",
                rate_limit_info="unlimited (stdlib)",
            ),
        ]

    # ------------------------------------------------------------------
    # 1. CACHING
    # ------------------------------------------------------------------

    def cache_get(self, key: str) -> Optional[Any]:
        """Retrieve a cached value via the best available tier.

        Tries tiers in priority order: Upstash -> Supabase -> Memory -> File.
        Returns None on complete miss.
        """
        attempted = 0
        for tier in self._tiers["caching"]:
            if not tier.is_available():
                continue
            attempted += 1
            try:
                t0 = time.time()
                result = self._cache_get_tier(tier, key)
                self._adaptive_timeouts.record_latency(tier.name, time.time() - t0)
                if result is not None:
                    tier.circuit_breaker.record_success()
                    if attempted > 1:
                        self._record_fallback_usage(tier.name)
                    return result
                # None means cache miss, not a failure
                tier.circuit_breaker.record_success()
                continue
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Cache GET failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return None

    def cache_set(
        self, key: str, data: Any, ttl_seconds: int = 86400, category: str = "api"
    ) -> bool:
        """Store a value via the best available cache tier.

        Returns True if at least one tier succeeded.
        """
        success = False
        attempted = 0
        for tier in self._tiers["caching"]:
            if not tier.is_available():
                continue
            attempted += 1
            try:
                t0 = time.time()
                self._cache_set_tier(tier, key, data, ttl_seconds, category)
                self._adaptive_timeouts.record_latency(tier.name, time.time() - t0)
                tier.circuit_breaker.record_success()
                if attempted > 1:
                    self._record_fallback_usage(tier.name)
                success = True
                break  # Only write to the highest-priority available tier
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Cache SET failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return success

    def cache_delete(self, key: str) -> bool:
        """Delete from all available cache tiers."""
        success = False
        for tier in self._tiers["caching"]:
            if not tier.is_available():
                continue
            try:
                self._cache_delete_tier(tier, key)
                tier.circuit_breaker.record_success()
                success = True
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
        return success

    def _cache_get_tier(self, tier: ServiceTier, key: str) -> Optional[Any]:
        """Dispatch cache GET to the appropriate tier implementation."""
        if tier.provider == "upstash":
            from upstash_cache import cache_get as upstash_get

            return upstash_get(key)
        elif tier.provider == "supabase_cache":
            from supabase_data import _cache_get as supa_cache_get

            return supa_cache_get(f"resilience:{key}")
        elif tier.provider == "memory":
            return self._memory_cache.get(key)
        elif tier.provider == "file":
            return self._file_cache.get(key)
        return None

    def _cache_set_tier(
        self,
        tier: ServiceTier,
        key: str,
        data: Any,
        ttl_seconds: int,
        category: str,
    ) -> None:
        """Dispatch cache SET to the appropriate tier implementation."""
        if tier.provider == "upstash":
            from upstash_cache import cache_set as upstash_set

            upstash_set(key, data, ttl_seconds, category)
        elif tier.provider == "supabase_cache":
            from supabase_data import _cache_set as supa_cache_set

            supa_cache_set(f"resilience:{key}", data)
        elif tier.provider == "memory":
            self._memory_cache.set(key, data, ttl_seconds)
        elif tier.provider == "file":
            self._file_cache.set(key, data, ttl_seconds)

    def _cache_delete_tier(self, tier: ServiceTier, key: str) -> None:
        """Dispatch cache DELETE to the appropriate tier implementation."""
        if tier.provider == "upstash":
            from upstash_cache import cache_delete as upstash_del

            upstash_del(key)
        elif tier.provider == "memory":
            self._memory_cache.delete(key)
        elif tier.provider == "file":
            self._file_cache.delete(key)

    # ------------------------------------------------------------------
    # 2. DATABASE
    # ------------------------------------------------------------------

    def db_query(self, table: str, **filters: Any) -> list[dict[str, Any]]:
        """Query a database table via the best available tier.

        Args:
            table: Table name (e.g., 'knowledge_base', 'channel_benchmarks').
            **filters: Key-value filters (implementation depends on tier).

        Returns:
            List of row dicts, or empty list on total failure.
        """
        attempted = 0
        for tier in self._tiers["database"]:
            if not tier.is_available():
                continue
            attempted += 1
            try:
                t0 = time.time()
                result = self._db_query_tier(tier, table, **filters)
                self._adaptive_timeouts.record_latency(tier.name, time.time() - t0)
                if result:
                    tier.circuit_breaker.record_success()
                    if attempted > 1:
                        self._record_fallback_usage(tier.name)
                    return result
                # Empty result is not a failure -- try next tier for data
                tier.circuit_breaker.record_success()
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] DB query failed on {tier.name} for {table}: {exc}",
                    exc_info=True,
                )
        return []

    def db_write(self, table: str, row: dict[str, Any]) -> bool:
        """Write a row to the best available database tier.

        Returns True if at least one tier succeeded.
        """
        attempted = 0
        for tier in self._tiers["database"]:
            if not tier.is_available():
                continue
            attempted += 1
            try:
                t0 = time.time()
                success = self._db_write_tier(tier, table, row)
                self._adaptive_timeouts.record_latency(tier.name, time.time() - t0)
                if success:
                    tier.circuit_breaker.record_success()
                    if attempted > 1:
                        self._record_fallback_usage(tier.name)
                    return True
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] DB write failed on {tier.name} for {table}: {exc}",
                    exc_info=True,
                )
        return False

    def _db_query_tier(
        self, tier: ServiceTier, table: str, **filters: Any
    ) -> list[dict[str, Any]]:
        """Dispatch DB query to the appropriate tier."""
        if tier.provider == "supabase":
            # Use the existing supabase_data module
            from supabase_data import _query_supabase
            import urllib.parse

            params_parts = ["select=*"]
            for key, value in filters.items():
                params_parts.append(
                    f"{key}=eq.{urllib.parse.quote(str(value), safe='')}"
                )
            params_parts.append("limit=100")
            return _query_supabase(table, "&".join(params_parts))
        elif tier.provider == "local_json":
            return self._local_db.read_table(table)
        elif tier.provider == "memory_kb":
            # Read from data/ knowledge base JSON files
            return self._read_memory_kb(table)
        return []

    def _db_write_tier(
        self, tier: ServiceTier, table: str, row: dict[str, Any]
    ) -> bool:
        """Dispatch DB write to the appropriate tier."""
        if tier.provider == "supabase":
            # Supabase write via PostgREST
            import json as _json
            import urllib.request
            import ssl

            supabase_url = (os.environ.get("SUPABASE_URL") or "").strip()
            supabase_key = (
                os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                or os.environ.get("SUPABASE_ANON_KEY")
                or ""
            ).strip()
            if not supabase_url or not supabase_key:
                return False
            url = f"{supabase_url.rstrip('/')}/rest/v1/{table}"
            data = _json.dumps(row, ensure_ascii=False, default=str).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                method="POST",
                headers={
                    "apikey": supabase_key,
                    "Authorization": f"Bearer {supabase_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
            )
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=5, context=ctx) as resp:
                resp.read()
            return True
        elif tier.provider == "local_json":
            return self._local_db.append_row(table, row)
        return False

    def _read_memory_kb(self, table: str) -> list[dict[str, Any]]:
        """Read from in-memory knowledge base files for the given table."""
        # Map table names to local JSON files
        _table_file_map: Dict[str, str] = {
            "knowledge_base": "recruitment_industry_knowledge.json",
            "channel_benchmarks": "live_market_data.json",
            "vendor_profiles": "joveo_publishers.json",
            "supply_repository": "global_supply.json",
        }
        filename = _table_file_map.get(table)
        if not filename:
            return []
        filepath = _DATA_DIR / filename
        if not filepath.exists():
            return []
        try:
            data = json.loads(filepath.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
            return []
        except (json.JSONDecodeError, OSError):
            return []

    # ------------------------------------------------------------------
    # 3. EMAIL ALERTS
    # ------------------------------------------------------------------

    def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        severity: str = "warning",
    ) -> bool:
        """Send an email alert via the best available tier.

        Tries: Resend -> Slack -> Sentry Breadcrumb -> Local log.
        Returns True if any tier succeeded.
        """
        for tier in self._tiers["email"]:
            if not tier.is_available():
                continue
            try:
                success = self._send_email_tier(tier, to, subject, body, severity)
                if success:
                    tier.circuit_breaker.record_success()
                    return True
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Email failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return False

    def _send_email_tier(
        self,
        tier: ServiceTier,
        to: str,
        subject: str,
        body: str,
        severity: str,
    ) -> bool:
        """Dispatch email send to the appropriate tier."""
        if tier.provider == "resend":
            from alert_manager import send_alert

            return send_alert(subject, body, severity)
        elif tier.provider == "slack":
            return self._send_slack_notification(subject, body, severity)
        elif tier.provider == "local_log":
            # Write to local alert log file
            alert_path = Path("/tmp/nova_alerts.log")
            try:
                with open(alert_path, "a", encoding="utf-8") as f:
                    ts = datetime.now(timezone.utc).isoformat()
                    f.write(f"[{ts}] [{severity.upper()}] To={to} Subject={subject}\n")
                    f.write(f"  Body: {body[:500]}\n\n")
                return True
            except OSError:
                return False
        elif tier.provider == "sentry_breadcrumb":
            try:
                import sentry_sdk

                sentry_sdk.add_breadcrumb(
                    category="email_alert",
                    message=f"[{severity}] {subject}",
                    data={"to": to, "body": body[:200]},
                    level=severity or "warning",
                )
                return True
            except ImportError:
                return False
        return False

    def _send_slack_notification(self, subject: str, body: str, severity: str) -> bool:
        """Send a notification to Slack as a fallback for email."""
        slack_token = (os.environ.get("SLACK_BOT_TOKEN") or "").strip()
        slack_channel = (os.environ.get("SLACK_ALERT_CHANNEL") or "#alerts").strip()

        if not slack_token:
            return False

        import urllib.request

        severity_emoji = {"critical": "🔴", "warning": "🟡", "info": "🔵"}.get(
            severity, "🟡"
        )
        text = f"{severity_emoji} *[{severity.upper()}] {subject}*\n{body[:2000]}"

        payload = json.dumps({"channel": slack_channel, "text": text}).encode("utf-8")
        req = urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {slack_token}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return bool(result.get("ok"))

    # ------------------------------------------------------------------
    # 4. ANALYTICS TRACKING
    # ------------------------------------------------------------------

    def track_event(
        self,
        event: str,
        properties: Optional[Dict[str, Any]] = None,
        distinct_id: str = "server",
    ) -> bool:
        """Track an analytics event via the best available tier.

        Tries: PostHog -> Local file -> Memory counter.
        Returns True if any tier succeeded.
        """
        attempted = 0
        for tier in self._tiers["analytics"]:
            if not tier.is_available():
                continue
            attempted += 1
            try:
                t0 = time.time()
                success = self._track_event_tier(tier, event, properties, distinct_id)
                self._adaptive_timeouts.record_latency(tier.name, time.time() - t0)
                if success:
                    tier.circuit_breaker.record_success()
                    if attempted > 1:
                        self._record_fallback_usage(tier.name)
                    return True
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Analytics failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return False

    def _track_event_tier(
        self,
        tier: ServiceTier,
        event: str,
        properties: Optional[Dict[str, Any]],
        distinct_id: str,
    ) -> bool:
        """Dispatch analytics event to the appropriate tier."""
        if tier.provider == "posthog":
            from posthog_tracker import capture

            capture(event, distinct_id=distinct_id, properties=properties)
            return True  # Fire-and-forget, assume success
        elif tier.provider == "sentry_breadcrumb":
            try:
                import sentry_sdk

                sentry_sdk.add_breadcrumb(
                    category="analytics",
                    message=event,
                    data=dict(properties or {}, distinct_id=distinct_id),
                    level="info",
                )
                return True
            except ImportError:
                return False
        elif tier.provider == "local_file":
            merged = dict(properties or {})
            merged["distinct_id"] = distinct_id
            return self._event_logger.log_event(event, merged)
        elif tier.provider == "memory_counter":
            self._memory_counter.increment(event)
            return True
        return False

    # ------------------------------------------------------------------
    # 5. ERROR REPORTING
    # ------------------------------------------------------------------

    def report_error(
        self,
        error: BaseException,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Report an error via the best available tier.

        Tries: Sentry -> Local file -> Email alert -> Stderr.
        Returns True if any tier succeeded.
        """
        for tier in self._tiers["errors"]:
            if not tier.is_available():
                continue
            try:
                success = self._report_error_tier(tier, error, context)
                if success:
                    tier.circuit_breaker.record_success()
                    return True
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                # Don't use logger.error here to avoid recursion if logging is broken
                sys.stderr.write(
                    f"[resilience] Error reporting failed on {tier.name}: {exc}\n"
                )
        return False

    def _report_error_tier(
        self,
        tier: ServiceTier,
        error: BaseException,
        context: Optional[Dict[str, Any]],
    ) -> bool:
        """Dispatch error reporting to the appropriate tier."""
        if tier.provider == "sentry":
            # Use Sentry SDK if available
            try:
                import sentry_sdk

                sentry_sdk.capture_exception(error)
                return True
            except ImportError:
                return False
        elif tier.provider == "local_file":
            return self._error_logger.log_error(error, context)
        elif tier.provider == "email_alert":
            return self.send_email(
                to=os.environ.get("ALERT_EMAIL") or "shubhamsingh@joveo.com",
                subject=f"Error: {type(error).__name__}: {str(error)[:100]}",
                body=f"<pre>{type(error).__name__}: {str(error)[:1000]}\n\nContext: {json.dumps(context or {}, default=str)}</pre>",
                severity="critical",
            )
        elif tier.provider == "stderr":
            sys.stderr.write(
                f"[ERROR] {type(error).__name__}: {error} | context={context}\n"
            )
            sys.stderr.flush()
            return True
        return False

    # ------------------------------------------------------------------
    # 6. STRUCTURED LOGGING
    # ------------------------------------------------------------------

    def log_structured(self, level: str, message: str, **kwargs: Any) -> bool:
        """Send a structured log via the best available tier.

        Tries: Local file -> Stderr.
        Returns True if any tier succeeded.
        """
        for tier in self._tiers["logging"]:
            if not tier.is_available():
                continue
            try:
                success = self._log_structured_tier(tier, level, message, **kwargs)
                if success:
                    tier.circuit_breaker.record_success()
                    return True
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                sys.stderr.write(f"[resilience] Logging failed on {tier.name}: {exc}\n")
        return False

    def _log_structured_tier(
        self, tier: ServiceTier, level: str, message: str, **kwargs: Any
    ) -> bool:
        """Dispatch structured log to the appropriate tier."""
        if tier.provider == "local_file":
            return self._structured_logger.log(level, message, **kwargs)
        elif tier.provider == "stderr":
            ts = datetime.now(timezone.utc).isoformat()
            sys.stderr.write(f"[{ts}] [{level.upper()}] {message}")
            if kwargs:
                sys.stderr.write(f" | {json.dumps(kwargs, default=str)}")
            sys.stderr.write("\n")
            sys.stderr.flush()
            return True
        return False

    # ------------------------------------------------------------------
    # 7. WEB SCRAPING
    # ------------------------------------------------------------------

    def scrape_url(self, url: str, **kwargs: Any) -> Optional[str]:
        """Scrape a URL via the web_scraper_router fallback chain.

        Delegates to the existing web_scraper_router module which has
        its own 6-tier fallback. This method wraps it with the
        resilience router's circuit breaker on the web_scraping category.

        Returns:
            Markdown content string or None on failure.
        """
        for tier in self._tiers["web_scraping"]:
            if not tier.is_available():
                continue
            try:
                result = self._scrape_url_tier(tier, url, **kwargs)
                if result:
                    tier.circuit_breaker.record_success()
                    return result
                tier.circuit_breaker.record_success()
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Scrape failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return None

    def search_web(self, query: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Search the web via the web_scraper_router fallback chain.

        Returns:
            List of search result dicts, or empty list on failure.
        """
        for tier in self._tiers["web_scraping"]:
            if not tier.is_available():
                continue
            try:
                result = self._search_web_tier(tier, query, **kwargs)
                if result:
                    tier.circuit_breaker.record_success()
                    return result
                tier.circuit_breaker.record_success()
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Search failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return []

    def _scrape_url_tier(
        self, tier: ServiceTier, url: str, **kwargs: Any
    ) -> Optional[str]:
        """Dispatch URL scrape to the web_scraper_router."""
        try:
            from web_scraper_router import scrape_url as _wsr_scrape

            return _wsr_scrape(url, **kwargs)
        except ImportError:
            logger.debug("[resilience] web_scraper_router not available")
            return None

    def _search_web_tier(
        self, tier: ServiceTier, query: str, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Dispatch web search to the web_scraper_router."""
        try:
            from web_scraper_router import search_web as _wsr_search

            return _wsr_search(query, **kwargs)
        except ImportError:
            logger.debug("[resilience] web_scraper_router not available")
            return []

    # ------------------------------------------------------------------
    # 8. DECK GENERATION
    # ------------------------------------------------------------------

    def generate_deck(self, data: Dict[str, Any], **kwargs: Any) -> Optional[bytes]:
        """Generate a presentation deck via the best available tier.

        Tries: Gamma -> python-pptx -> HTML-to-PDF.

        Args:
            data: Presentation data dict (title, slides, etc.).

        Returns:
            File content as bytes, or None on total failure.
        """
        for tier in self._tiers["deck_generation"]:
            if not tier.is_available():
                continue
            try:
                result = self._generate_deck_tier(tier, data, **kwargs)
                if result:
                    tier.circuit_breaker.record_success()
                    return result
            except Exception as exc:
                tier.circuit_breaker.record_failure(str(exc))
                logger.error(
                    f"[resilience] Deck generation failed on {tier.name}: {exc}",
                    exc_info=True,
                )
        return None

    def _generate_deck_tier(
        self, tier: ServiceTier, data: Dict[str, Any], **kwargs: Any
    ) -> Optional[bytes]:
        """Dispatch deck generation to the appropriate tier."""
        if tier.provider == "gamma":
            try:
                from deck_generator import DeckGenerator

                gen = DeckGenerator()
                return gen.generate(data, **kwargs)
            except (ImportError, Exception) as exc:
                logger.debug(f"[resilience] Gamma deck gen failed: {exc}")
                raise
        elif tier.provider == "pptx":
            try:
                from ppt_generator import generate_pptx

                return generate_pptx(data)
            except ImportError:
                logger.debug("[resilience] ppt_generator not available")
                return None
        elif tier.provider == "html_pdf":
            return self._generate_html_pdf(data)
        return None

    def _generate_html_pdf(self, data: Dict[str, Any]) -> Optional[bytes]:
        """Generate a simple HTML summary as a PDF-like HTML file (bytes).

        This is the last-resort fallback. Produces an HTML document
        that can be saved as .html and printed to PDF by the browser.
        """
        title = data.get("title") or data.get("client_name") or "Media Plan"
        html_parts = [
            "<!DOCTYPE html><html><head>",
            f"<title>{title}</title>",
            "<style>body{font-family:Inter,sans-serif;margin:40px;color:#202058;}",
            "h1{color:#5A54BD;}table{border-collapse:collapse;width:100%;}",
            "th,td{border:1px solid #ccc;padding:8px;text-align:left;}",
            "th{background:#f0f0f5;}</style></head><body>",
            f"<h1>{title}</h1>",
        ]
        # Render channels table if present
        channels = data.get("channels") or data.get("channel_mix") or []
        if channels:
            html_parts.append("<h2>Channel Mix</h2><table><tr>")
            if channels and isinstance(channels[0], dict):
                headers = list(channels[0].keys())
                for h in headers:
                    html_parts.append(f"<th>{h}</th>")
                html_parts.append("</tr>")
                for ch in channels:
                    html_parts.append("<tr>")
                    for h in headers:
                        html_parts.append(f"<td>{ch.get(h) or ''}</td>")
                    html_parts.append("</tr>")
            html_parts.append("</table>")
        html_parts.append(
            "<p style='color:#999;font-size:12px;'>Generated by Nova AI Suite</p>"
        )
        html_parts.append("</body></html>")
        return "".join(html_parts).encode("utf-8")

    # ------------------------------------------------------------------
    # UNIFIED API
    # ------------------------------------------------------------------

    def execute(self, category: str, operation: str, **kwargs: Any) -> Any:
        """Execute an operation against a service category with automatic fallback.

        This is the unified entry point that dispatches to category-specific
        methods based on the category and operation names.

        Args:
            category: Service category (e.g., 'caching', 'database', 'email').
            operation: Operation name (e.g., 'get', 'set', 'query', 'send').
            **kwargs: Operation-specific keyword arguments.

        Returns:
            Operation result, or None/False/[] on total failure.

        Raises:
            ValueError: If category or operation is unknown.
        """
        dispatch: Dict[str, Dict[str, Any]] = {
            "caching": {
                "get": lambda: self.cache_get(kwargs.get("key", "")),
                "set": lambda: self.cache_set(
                    kwargs.get("key", ""),
                    kwargs.get("data"),
                    kwargs.get("ttl_seconds", 86400),
                    kwargs.get("category", "api"),
                ),
                "delete": lambda: self.cache_delete(kwargs.get("key", "")),
            },
            "database": {
                "query": lambda: self.db_query(
                    kwargs.get("table", ""),
                    **{k: v for k, v in kwargs.items() if k != "table"},
                ),
                "write": lambda: self.db_write(
                    kwargs.get("table", ""),
                    kwargs.get("row", {}),
                ),
            },
            "email": {
                "send": lambda: self.send_email(
                    kwargs.get("to", ""),
                    kwargs.get("subject", ""),
                    kwargs.get("body", ""),
                    kwargs.get("severity", "warning"),
                ),
            },
            "analytics": {
                "track": lambda: self.track_event(
                    kwargs.get("event", ""),
                    kwargs.get("properties"),
                    kwargs.get("distinct_id", "server"),
                ),
            },
            "errors": {
                "report": lambda: self.report_error(
                    kwargs.get("error", RuntimeError("unknown")),
                    kwargs.get("context"),
                ),
            },
            "logging": {
                "log": lambda: self.log_structured(
                    kwargs.get("level", "info"),
                    kwargs.get("message", ""),
                    **{
                        k: v for k, v in kwargs.items() if k not in ("level", "message")
                    },
                ),
            },
            "web_scraping": {
                "scrape": lambda: self.scrape_url(
                    kwargs.get("url", ""),
                    **{k: v for k, v in kwargs.items() if k != "url"},
                ),
                "search": lambda: self.search_web(
                    kwargs.get("query", ""),
                    **{k: v for k, v in kwargs.items() if k != "query"},
                ),
            },
            "deck_generation": {
                "generate": lambda: self.generate_deck(
                    kwargs.get("data", {}),
                    **{k: v for k, v in kwargs.items() if k != "data"},
                ),
            },
        }

        if category not in dispatch:
            raise ValueError(
                f"Unknown category '{category}'. Available: {', '.join(sorted(dispatch))}"
            )
        ops = dispatch[category]
        if operation not in ops:
            raise ValueError(
                f"Unknown operation '{operation}' for category '{category}'. "
                f"Available: {', '.join(sorted(ops))}"
            )
        return ops[operation]()

    def get_status(self) -> Dict[str, Any]:
        """Alias for get_health_dashboard(). Returns full health status JSON."""
        return self.get_health_dashboard()

    def reset(self, category: Optional[str] = None) -> None:
        """Reset circuit breakers for a category (or all if None).

        Args:
            category: Service category name, or None to reset all.
        """
        if category:
            tiers = self._tiers.get(category, [])
            for tier in tiers:
                tier.circuit_breaker.reset()
            logger.info(f"[resilience] Reset circuit breakers for {category}")
        else:
            for cat_name, tiers in self._tiers.items():
                for tier in tiers:
                    tier.circuit_breaker.reset()
            logger.info("[resilience] Reset all circuit breakers")

    # ------------------------------------------------------------------
    # FALLBACK & RESILIENCE HELPERS
    # ------------------------------------------------------------------

    def _record_fallback_usage(self, tier_name: str) -> None:
        """Record that a fallback tier was used."""
        with self._fallback_lock:
            self._fallback_usage[tier_name] = self._fallback_usage.get(tier_name, 0) + 1

    def get_fallback_usage(self) -> Dict[str, int]:
        """Return fallback tier usage counts."""
        with self._fallback_lock:
            return dict(self._fallback_usage)

    def get_bulkhead_status(self) -> Dict[str, Dict[str, Any]]:
        """Return bulkhead status for all service categories."""
        return {cat: bh.get_status() for cat, bh in self._bulkheads.items()}

    def get_adaptive_timeout_status(self) -> Dict[str, Any]:
        """Return adaptive timeout tracker status."""
        return self._adaptive_timeouts.get_status()

    # ------------------------------------------------------------------
    # HEALTH DASHBOARD
    # ------------------------------------------------------------------

    def get_health_dashboard(self) -> Dict[str, Any]:
        """Return health status of all tiers across all services.

        Suitable for JSON API responses and dashboard rendering.
        """
        dashboard: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "services": {},
            "summary": {
                "total_services": len(self._tiers),
                "total_tiers": sum(len(tiers) for tiers in self._tiers.values()),
                "configured_tiers": 0,
                "healthy_tiers": 0,
                "degraded_services": [],
                "down_services": [],
            },
        }

        for service_name, tiers in self._tiers.items():
            service_data = {
                "tiers": [t.snapshot() for t in tiers],
                "active_tier": None,
                "status": "down",
            }
            has_available = False
            for t in tiers:
                if t.is_configured:
                    dashboard["summary"]["configured_tiers"] += 1
                if t.is_available():
                    dashboard["summary"]["healthy_tiers"] += 1
                    if not has_available:
                        service_data["active_tier"] = t.name
                        has_available = True

            if has_available:
                # Check if we're on a fallback tier
                first_tier = tiers[0]
                if first_tier.is_available():
                    service_data["status"] = "healthy"
                else:
                    service_data["status"] = "degraded"
                    dashboard["summary"]["degraded_services"].append(service_name)
            else:
                service_data["status"] = "down"
                dashboard["summary"]["down_services"].append(service_name)

            dashboard["services"][service_name] = service_data

        # Overall health score (0-100)
        total = dashboard["summary"]["total_tiers"]
        healthy = dashboard["summary"]["healthy_tiers"]
        dashboard["summary"]["health_score"] = (
            round((healthy / total) * 100) if total > 0 else 0
        )

        # Include fallback usage, bulkhead status, and adaptive timeouts
        dashboard["fallback_usage"] = self.get_fallback_usage()
        dashboard["bulkheads"] = self.get_bulkhead_status()
        dashboard["adaptive_timeouts"] = self.get_adaptive_timeout_status()

        return dashboard

    def get_priority_matrix(self) -> str:
        """Return an ASCII priority matrix for the HTML dashboard.

        Returns a string suitable for rendering in a <pre> block.
        """
        # Determine max number of tiers across services
        max_tiers = max(len(tiers) for tiers in self._tiers.values())

        # Header
        headers = ["Service"]
        for i in range(1, max_tiers + 1):
            label = {1: "Primary", 2: "Secondary", 3: "Tertiary", 4: "Emergency"}.get(
                i, f"Tier {i}"
            )
            headers.append(f"Tier {i} ({label})")

        # Build rows
        rows: list[list[str]] = []
        for service_name, tiers in sorted(self._tiers.items()):
            row = [service_name.title()]
            for i in range(max_tiers):
                if i < len(tiers):
                    t = tiers[i]
                    row.append(f"{t.name} [{t.status_label()}]")
                else:
                    row.append("--")
            rows.append(row)

        # Calculate column widths
        all_rows = [headers] + rows
        col_widths = [max(len(row[i]) for row in all_rows) for i in range(len(headers))]

        # Format
        lines: list[str] = []
        header_line = " | ".join(
            headers[i].ljust(col_widths[i]) for i in range(len(headers))
        )
        lines.append(header_line)
        lines.append("-" * len(header_line))
        for row in rows:
            lines.append(
                " | ".join(row[i].ljust(col_widths[i]) for i in range(len(row)))
            )

        return "\n".join(lines)

    def get_dashboard_html(self) -> str:
        """Return an HTML dashboard page showing the resilience priority matrix."""
        dashboard = self.get_health_dashboard()
        matrix = self.get_priority_matrix()

        # Color mapping for statuses
        status_colors = {
            "OK": "#22c55e",
            "NOT CONFIGURED": "#6b7280",
            "CIRCUIT OPEN": "#ef4444",
        }

        # Build tier rows for HTML table
        table_rows = ""
        for service_name, service_data in sorted(dashboard["services"].items()):
            tiers_html = ""
            for t in service_data["tiers"]:
                color = status_colors.get(t["status"], "#f59e0b")
                tiers_html += (
                    f'<td style="padding:8px 12px;border:1px solid #333;">'
                    f'<span style="font-weight:600;">{t["name"]}</span><br>'
                    f'<span style="color:{color};font-size:12px;">[{t["status"]}]</span>'
                    f"</td>"
                )
            # Pad empty cells
            while len(service_data["tiers"]) < 4:
                tiers_html += '<td style="padding:8px 12px;border:1px solid #333;color:#555;">--</td>'
                service_data["tiers"].append({"name": "--", "status": "--"})

            svc_status = service_data["status"]
            svc_color = {
                "healthy": "#22c55e",
                "degraded": "#f59e0b",
                "down": "#ef4444",
            }.get(svc_status, "#6b7280")
            table_rows += (
                f"<tr>"
                f'<td style="padding:8px 12px;border:1px solid #333;font-weight:700;">'
                f"{service_name.title()} "
                f'<span style="color:{svc_color};font-size:11px;">({svc_status.upper()})</span>'
                f"</td>"
                f"{tiers_html}"
                f"</tr>"
            )

        health_score = dashboard["summary"]["health_score"]
        score_color = (
            "#22c55e"
            if health_score >= 80
            else ("#f59e0b" if health_score >= 50 else "#ef4444")
        )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Resilience Dashboard - Nova AI Suite</title>
<style>
body {{ background:#0a0a1a; color:#e4e4e7; font-family:'Inter',system-ui,sans-serif; margin:0; padding:20px; }}
.container {{ max-width:1200px; margin:0 auto; }}
h1 {{ color:#5A54BD; font-size:24px; margin-bottom:4px; }}
h2 {{ color:#6BB3CD; font-size:18px; margin-top:32px; }}
.score {{ font-size:48px; font-weight:700; color:{score_color}; }}
.score-label {{ font-size:14px; color:#9ca3af; }}
table {{ border-collapse:collapse; width:100%; margin-top:16px; background:#111827; border-radius:8px; overflow:hidden; }}
th {{ padding:10px 12px; text-align:left; background:#1f2937; color:#9ca3af; font-size:12px; text-transform:uppercase; border:1px solid #333; }}
td {{ font-size:13px; }}
pre {{ background:#111827; padding:16px; border-radius:8px; overflow-x:auto; font-size:12px; color:#e4e4e7; }}
.meta {{ color:#6b7280; font-size:12px; margin-top:24px; }}
</style>
</head>
<body>
<div class="container">
<h1>Resilience Dashboard</h1>
<p style="color:#9ca3af;margin-top:0;">Nova AI Suite -- Service Fallback Priority Matrix</p>

<div style="display:flex;gap:40px;margin:24px 0;">
<div>
<div class="score">{health_score}/100</div>
<div class="score-label">Health Score</div>
</div>
<div>
<div class="score" style="font-size:32px;">{dashboard["summary"]["configured_tiers"]}/{dashboard["summary"]["total_tiers"]}</div>
<div class="score-label">Configured Tiers</div>
</div>
<div>
<div class="score" style="font-size:32px;color:{'#22c55e' if not dashboard['summary']['degraded_services'] else '#f59e0b'};">{len(dashboard["summary"]["degraded_services"])}</div>
<div class="score-label">Degraded Services</div>
</div>
</div>

<h2>Priority Matrix</h2>
<table>
<thead>
<tr>
<th>Service</th>
<th>Tier 1 (Primary)</th>
<th>Tier 2 (Secondary)</th>
<th>Tier 3 (Tertiary)</th>
<th>Tier 4 (Emergency)</th>
</tr>
</thead>
<tbody>
{table_rows}
</tbody>
</table>

<h2>ASCII Matrix</h2>
<pre>{matrix}</pre>

<div class="meta">
Generated at {dashboard["timestamp"]} | Resilience Router v1.0
</div>
</div>
</body>
</html>"""
        return html

    # ------------------------------------------------------------------
    # ACCESSORS
    # ------------------------------------------------------------------

    @property
    def memory_cache(self) -> MemoryCache:
        """Access the in-memory cache instance directly."""
        return self._memory_cache

    @property
    def memory_counter(self) -> MemoryCounter:
        """Access the memory counter instance directly."""
        return self._memory_counter

    @property
    def local_db(self) -> LocalJSONDB:
        """Access the local JSON DB instance directly."""
        return self._local_db

    def get_tiers(self, service: str) -> List[ServiceTier]:
        """Get the tier list for a given service category."""
        return self._tiers.get(service, [])


# =============================================================================
# SINGLETON
# =============================================================================

_router: Optional[ResilienceRouter] = None
_router_lock = threading.Lock()


def get_router() -> ResilienceRouter:
    """Get the global ResilienceRouter singleton (lazy-initialized).

    Thread-safe. Creates the router on first call.
    """
    global _router
    if _router is not None:
        return _router
    with _router_lock:
        if _router is None:
            _router = ResilienceRouter()
    return _router


def reset_router() -> None:
    """Reset the singleton (for testing only)."""
    global _router
    with _router_lock:
        _router = None


# =============================================================================
# SUMMARY FACADE
# =============================================================================


def get_resilience_summary() -> Dict[str, Any]:
    """Get a compact summary of all service health for dashboards.

    Returns:
        Dict mapping service name to {healthy_tiers, active_tier, status}.
        Status is one of: 'healthy', 'degraded', 'critical'.
    """
    router = get_router()
    dashboard = router.get_health_dashboard()
    summary: Dict[str, Any] = {}
    services = dashboard.get("services", {})
    for service_name, service_data in services.items():
        if isinstance(service_data, dict):
            tiers = service_data.get("tiers", [])
            healthy = sum(
                1 for t in tiers if isinstance(t, dict) and t.get("status") == "OK"
            )
            total = len(tiers)
            active_tier = service_data.get("active_tier") or "none"
            status = service_data.get("status", "down")
            # Normalize to healthy/degraded/critical
            if status == "healthy":
                normalized = "healthy"
            elif status == "degraded":
                normalized = "degraded"
            else:
                normalized = "critical"
            summary[service_name] = {
                "healthy_tiers": f"{healthy}/{total}",
                "active_tier": active_tier,
                "status": normalized,
            }
    return summary
