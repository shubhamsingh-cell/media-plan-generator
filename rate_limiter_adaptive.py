#!/usr/bin/env python3
"""Adaptive Rate Limiting with Backpressure.

Token-bucket rate limiter per tenant/IP with configurable rates, priority
queuing for paying users, and graceful degradation under load.  When the
system is under backpressure (>80% of concurrent-request capacity), read
endpoints are served from a short-lived response cache while write
operations are queued with priority ordering.

Thread-safe throughout -- all shared state protected by locks.

Usage::

    from rate_limiter_adaptive import check_rate_limit, get_rate_limiter_stats

    allowed, retry_after, from_cache = check_rate_limit(
        client_id="tenant_123",
        priority="STANDARD",
        endpoint="/api/chat",
    )
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Priority tiers
# ---------------------------------------------------------------------------


class Priority(Enum):
    """Client priority levels with associated rate limits."""

    FREE = "FREE"
    STANDARD = "STANDARD"
    PREMIUM = "PREMIUM"


# Requests per minute by priority
_RATE_LIMITS: dict[Priority, int] = {
    Priority.FREE: 10,
    Priority.STANDARD: 30,
    Priority.PREMIUM: 100,
}

# Priority weights for queue ordering (higher = processed first)
_PRIORITY_WEIGHTS: dict[Priority, int] = {
    Priority.PREMIUM: 30,
    Priority.STANDARD: 20,
    Priority.FREE: 10,
}


def _resolve_priority(value: str | Priority) -> Priority:
    """Resolve a string or Priority enum to a Priority value.

    Args:
        value: Priority as a string name (case-insensitive) or enum member.

    Returns:
        The resolved Priority enum member, defaulting to FREE for unknowns.
    """
    if isinstance(value, Priority):
        return value
    try:
        return Priority(value.upper())
    except (ValueError, AttributeError):
        return Priority.FREE


# ---------------------------------------------------------------------------
# Token Bucket (per-client)
# ---------------------------------------------------------------------------


@dataclass
class _TokenBucket:
    """Sliding-window token bucket for a single client."""

    capacity: int
    tokens: float = field(init=False)
    refill_rate: float = field(init=False)  # tokens per second
    last_refill: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        self.tokens = float(self.capacity)
        self.refill_rate = self.capacity / 60.0  # per-minute -> per-second
        self.last_refill = time.monotonic()

    def try_consume(self) -> tuple[bool, int]:
        """Try to consume one token.

        Returns:
            (allowed, retry_after_seconds).  retry_after is 0 when allowed.
        """
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last_refill = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True, 0

            # How long until 1 token is available?
            deficit = 1.0 - self.tokens
            wait_s = deficit / self.refill_rate
            return False, max(1, int(wait_s + 0.5))

    def remaining(self) -> int:
        """Current tokens available (snapshot)."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            current = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            return int(current)


# ---------------------------------------------------------------------------
# Backpressure response cache
# ---------------------------------------------------------------------------


@dataclass
class _CachedResponse:
    """A cached API response with expiry."""

    data: Any
    created_at: float
    ttl: float = 30.0  # seconds

    @property
    def is_expired(self) -> bool:
        """Check whether this cached entry has expired."""
        return (time.monotonic() - self.created_at) > self.ttl


# ---------------------------------------------------------------------------
# Read vs. write endpoint classification
# ---------------------------------------------------------------------------

# Prefixes that are considered read-only (safe to serve from cache)
_READ_PREFIXES: tuple[str, ...] = (
    "/api/health",
    "/api/config",
    "/api/features",
    "/api/channels",
    "/api/insights",
    "/api/metrics",
    "/api/deck/status",
    "/api/integrations/status",
    "/api/rate-limits",
    "/api/elevenlabs/health",
    "/api/firecrawl/status",  # S72: returns a "removed" stub (kept for back-compat)
    "/api/scraper/status",
    "/api/resilience",
    "/api/dashboard",
    "/api/docs",
    "/api/morning-brief",
    "/api/market-pulse",
    "/api/llm/costs",
    "/api/observability",
    "/health",
    "/ready",
)


def _is_read_endpoint(endpoint: str) -> bool:
    """Return True if the endpoint is read-heavy / cacheable."""
    return any(endpoint.startswith(prefix) for prefix in _READ_PREFIXES)


# ---------------------------------------------------------------------------
# Adaptive Rate Limiter (singleton)
# ---------------------------------------------------------------------------


class AdaptiveRateLimiter:
    """Centralized rate limiter with backpressure and priority queuing."""

    def __init__(
        self,
        max_concurrent: int = 50,
        backpressure_threshold: float = 0.80,
        cache_ttl: float = 30.0,
        bucket_cleanup_interval: float = 300.0,
    ) -> None:
        """Initialize the adaptive rate limiter.

        Args:
            max_concurrent: Maximum concurrent requests before overload.
            backpressure_threshold: Fraction of max_concurrent that triggers
                backpressure mode (0.0-1.0).
            cache_ttl: Default TTL in seconds for backpressure cache entries.
            bucket_cleanup_interval: Seconds between stale bucket eviction.
        """
        self._max_concurrent = max_concurrent
        self._bp_threshold = backpressure_threshold
        self._cache_ttl = cache_ttl
        self._cleanup_interval = bucket_cleanup_interval

        # -- Concurrent request tracking --
        self._active_requests = 0
        self._active_lock = threading.Lock()

        # -- Per-client token buckets --
        self._buckets: dict[str, _TokenBucket] = {}
        self._buckets_lock = threading.Lock()

        # -- Backpressure response cache --
        self._cache: dict[str, _CachedResponse] = {}
        self._cache_lock = threading.Lock()

        # -- Write queue (priority-ordered) --
        self._write_queue: list[tuple[int, float, str, str]] = (
            []
        )  # (-weight, ts, client, endpoint)
        self._queue_lock = threading.Lock()

        # -- Stats --
        self._stats_lock = threading.Lock()
        self._total_allowed = 0
        self._total_denied = 0
        self._total_cache_hits = 0
        self._total_backpressure_activations = 0
        self._backpressure_active = False
        self._last_bp_activation: Optional[float] = None

        # -- Background cleanup --
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, daemon=True, name="rate-limiter-cleanup"
        )
        self._cleanup_thread.start()
        logger.info(
            "AdaptiveRateLimiter initialized: max_concurrent=%d, "
            "backpressure_threshold=%.0f%%",
            max_concurrent,
            backpressure_threshold * 100,
        )

    # ----- Concurrent request tracking -----

    def enter_request(self) -> None:
        """Called when a new request starts processing."""
        with self._active_lock:
            self._active_requests += 1
            load = self._active_requests / max(1, self._max_concurrent)
            if load >= self._bp_threshold and not self._backpressure_active:
                self._backpressure_active = True
                self._last_bp_activation = time.monotonic()
                with self._stats_lock:
                    self._total_backpressure_activations += 1
                logger.warning(
                    "Backpressure ACTIVATED: %d/%d active requests (%.0f%%)",
                    self._active_requests,
                    self._max_concurrent,
                    load * 100,
                )

    def exit_request(self) -> None:
        """Called when a request finishes processing."""
        with self._active_lock:
            self._active_requests = max(0, self._active_requests - 1)
            load = self._active_requests / max(1, self._max_concurrent)
            # Hysteresis: deactivate at 60% to prevent flapping
            if load < 0.60 and self._backpressure_active:
                self._backpressure_active = False
                logger.info(
                    "Backpressure DEACTIVATED: %d/%d active requests (%.0f%%)",
                    self._active_requests,
                    self._max_concurrent,
                    load * 100,
                )

    @property
    def is_under_backpressure(self) -> bool:
        """Whether the system is currently in backpressure mode."""
        return self._backpressure_active

    @property
    def load_percent(self) -> float:
        """Current load as a percentage of max concurrent capacity."""
        with self._active_lock:
            return (self._active_requests / max(1, self._max_concurrent)) * 100

    # ----- Token bucket management -----

    def _get_bucket(self, client_id: str, priority: Priority) -> _TokenBucket:
        """Get or create a token bucket for the given client."""
        with self._buckets_lock:
            bucket = self._buckets.get(client_id)
            if bucket is None:
                capacity = _RATE_LIMITS.get(priority, _RATE_LIMITS[Priority.FREE])
                bucket = _TokenBucket(capacity=capacity)
                self._buckets[client_id] = bucket
            return bucket

    # ----- Cache management -----

    def cache_response(self, endpoint: str, data: Any) -> None:
        """Store a response in the backpressure cache.

        Args:
            endpoint: The API endpoint path.
            data: The JSON-serializable response data.
        """
        with self._cache_lock:
            self._cache[endpoint] = _CachedResponse(
                data=data, created_at=time.monotonic(), ttl=self._cache_ttl
            )

    def get_cached_response(self, endpoint: str) -> Optional[Any]:
        """Retrieve a non-expired cached response for the endpoint.

        Args:
            endpoint: The API endpoint path.

        Returns:
            Cached response data or None if not available/expired.
        """
        with self._cache_lock:
            entry = self._cache.get(endpoint)
            if entry is not None and not entry.is_expired:
                return entry.data
            # Evict expired
            if entry is not None:
                del self._cache[endpoint]
            return None

    # ----- Core rate-limit check -----

    def check(
        self,
        client_id: str,
        priority: str | Priority,
        endpoint: str,
    ) -> tuple[bool, int, bool]:
        """Check whether a request should be allowed.

        Args:
            client_id: Tenant ID or IP address.
            priority: Client priority level (FREE/STANDARD/PREMIUM).
            endpoint: The API endpoint being accessed.

        Returns:
            Tuple of (allowed, retry_after_seconds, from_cache).
            - allowed: True if the request may proceed.
            - retry_after: Seconds to wait before retrying (0 if allowed).
            - from_cache: True if a cached response is available under
              backpressure (caller should use it instead of hitting backend).
        """
        resolved_priority = _resolve_priority(priority)

        # --- Step 1: Token bucket check ---
        bucket = self._get_bucket(client_id, resolved_priority)
        allowed, retry_after = bucket.try_consume()

        if not allowed:
            with self._stats_lock:
                self._total_denied += 1
            return False, retry_after, False

        # --- Step 2: Backpressure handling ---
        if self._backpressure_active:
            is_read = _is_read_endpoint(endpoint)

            if is_read:
                # Try to serve from cache
                cached = self.get_cached_response(endpoint)
                if cached is not None:
                    with self._stats_lock:
                        self._total_cache_hits += 1
                        self._total_allowed += 1
                    return True, 0, True

            # For write endpoints under backpressure, still allow but
            # premium/standard clients get priority (they skip the queue).
            # Free-tier writes may be delayed.
            if not is_read and resolved_priority == Priority.FREE:
                # Queue the request info for observability; actual blocking
                # is left to the caller (middleware can choose to delay).
                with self._queue_lock:
                    weight = _PRIORITY_WEIGHTS[resolved_priority]
                    self._write_queue.append(
                        (-weight, time.monotonic(), client_id, endpoint)
                    )
                    # Trim queue to prevent unbounded growth
                    if len(self._write_queue) > 500:
                        self._write_queue = self._write_queue[-250:]

        with self._stats_lock:
            self._total_allowed += 1
        return True, 0, False

    # ----- Statistics -----

    def get_stats(self) -> dict[str, Any]:
        """Return current rate limiter statistics for health endpoints.

        Returns:
            Dictionary with counters, backpressure state, and per-tier info.
        """
        with self._stats_lock:
            stats_snapshot = {
                "total_allowed": self._total_allowed,
                "total_denied": self._total_denied,
                "total_cache_hits": self._total_cache_hits,
                "total_backpressure_activations": self._total_backpressure_activations,
            }

        with self._active_lock:
            active = self._active_requests
            bp_active = self._backpressure_active

        with self._buckets_lock:
            num_clients = len(self._buckets)
            # Per-client usage snapshot (top 20 busiest)
            client_usage: list[dict[str, Any]] = []
            for cid, bucket in sorted(
                self._buckets.items(),
                key=lambda kv: kv[1].remaining(),
            )[:20]:
                client_usage.append(
                    {
                        "client_id": cid,
                        "remaining_tokens": bucket.remaining(),
                        "capacity": bucket.capacity,
                    }
                )

        with self._cache_lock:
            cache_size = len(self._cache)
            cached_endpoints = list(self._cache.keys())

        with self._queue_lock:
            queue_depth = len(self._write_queue)

        return {
            "backpressure_active": bp_active,
            "active_requests": active,
            "max_concurrent": self._max_concurrent,
            "load_percent": round((active / max(1, self._max_concurrent)) * 100, 1),
            "backpressure_threshold_percent": round(self._bp_threshold * 100, 1),
            "counters": stats_snapshot,
            "tracked_clients": num_clients,
            "busiest_clients": client_usage,
            "cache_entries": cache_size,
            "cached_endpoints": cached_endpoints,
            "write_queue_depth": queue_depth,
            "tiers": {
                tier.value: {
                    "requests_per_minute": _RATE_LIMITS[tier],
                    "priority_weight": _PRIORITY_WEIGHTS[tier],
                }
                for tier in Priority
            },
        }

    def get_client_limits(
        self, client_id: str, priority: str | Priority
    ) -> dict[str, Any]:
        """Return current rate limit status for a specific client.

        Args:
            client_id: The client identifier.
            priority: The client's priority tier.

        Returns:
            Dictionary with limit, remaining, and reset information.
        """
        resolved = _resolve_priority(priority)
        bucket = self._get_bucket(client_id, resolved)
        remaining = bucket.remaining()
        capacity = bucket.capacity

        return {
            "client_id": client_id,
            "tier": resolved.value,
            "limit_per_minute": capacity,
            "remaining": remaining,
            "used": capacity - remaining,
            "backpressure_active": self._backpressure_active,
        }

    # ----- Background cleanup -----

    def _cleanup_loop(self) -> None:
        """Periodically evict stale buckets and expired cache entries."""
        while True:
            try:
                time.sleep(self._cleanup_interval)
                self._evict_stale()
            except Exception as exc:
                logger.error("Rate limiter cleanup error: %s", exc, exc_info=True)

    def _evict_stale(self) -> None:
        """Remove idle buckets and expired cache entries."""
        now = time.monotonic()

        # Evict buckets that have been full (idle) for >10 minutes
        with self._buckets_lock:
            stale_keys = [
                cid
                for cid, bucket in self._buckets.items()
                if (now - bucket.last_refill) > 600
                and bucket.remaining() >= bucket.capacity
            ]
            for key in stale_keys:
                del self._buckets[key]
            if stale_keys:
                logger.debug("Evicted %d stale rate-limit buckets", len(stale_keys))

        # Evict expired cache entries
        with self._cache_lock:
            expired = [ep for ep, entry in self._cache.items() if entry.is_expired]
            for ep in expired:
                del self._cache[ep]
            if expired:
                logger.debug("Evicted %d expired cache entries", len(expired))


# ---------------------------------------------------------------------------
# Module-level singleton & public API
# ---------------------------------------------------------------------------

_instance: Optional[AdaptiveRateLimiter] = None
_instance_lock = threading.Lock()


def get_rate_limiter() -> AdaptiveRateLimiter:
    """Get or create the global AdaptiveRateLimiter singleton.

    Returns:
        The singleton AdaptiveRateLimiter instance.
    """
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = AdaptiveRateLimiter()
    return _instance


def check_rate_limit(
    client_id: str,
    priority: str | Priority = "FREE",
    endpoint: str = "/",
) -> tuple[bool, int, bool]:
    """Check whether a request should be allowed through the rate limiter.

    Convenience wrapper around the singleton's check() method.

    Args:
        client_id: Tenant ID or client IP address.
        priority: Priority tier name (FREE, STANDARD, PREMIUM).
        endpoint: The API endpoint path being accessed.

    Returns:
        Tuple of (allowed, retry_after_seconds, from_cache).
    """
    return get_rate_limiter().check(client_id, priority, endpoint)


def get_rate_limiter_stats() -> dict[str, Any]:
    """Get current rate limiter statistics for /api/health.

    Returns:
        Dictionary of rate limiter metrics and state.
    """
    return get_rate_limiter().get_stats()
