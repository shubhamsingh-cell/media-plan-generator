#!/usr/bin/env python3
"""PostHog analytics integration -- stdlib-only HTTP client.

Sends events to PostHog via the /capture HTTP API using fire-and-forget
daemon threads with batching.  Gracefully degrades to a no-op when
POSTHOG_API_KEY is not set.

Thread-safe: uses a Lock for the event queue and a dedicated flush thread.
Rate-limited: max 100 events/minute to avoid overwhelming the API.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
POSTHOG_API_KEY: str = os.environ.get("POSTHOG_API_KEY") or ""
POSTHOG_HOST: str = os.environ.get("POSTHOG_HOST") or "https://us.posthog.com"
CAPTURE_URL: str = f"{POSTHOG_HOST}/batch/"

_FLUSH_INTERVAL_S: float = 5.0  # Flush every 5 seconds
_BATCH_SIZE: int = 10  # Flush when batch hits 10 events
_RATE_LIMIT_MAX: int = 100  # Max events per minute
_RATE_LIMIT_WINDOW_S: float = 60.0
_API_TIMEOUT_S: int = 5  # HTTP timeout for PostHog API


# ═══════════════════════════════════════════════════════════════════════════════
# POSTHOG CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
class PostHogClient:
    """Lightweight, stdlib-only PostHog event tracker with batching."""

    def __init__(self) -> None:
        self._enabled: bool = bool(POSTHOG_API_KEY)
        self._queue: List[Dict[str, Any]] = []
        self._lock: threading.Lock = threading.Lock()
        self._flush_thread: Optional[threading.Thread] = None
        self._shutdown: bool = False

        # Stats
        self._stats_lock: threading.Lock = threading.Lock()
        self._total_events: int = 0
        self._events_by_type: Dict[str, int] = defaultdict(int)
        self._last_flush_time: str = ""
        self._flush_count: int = 0

        # Rate limiting: track timestamps of sent events
        self._rate_timestamps: List[float] = []
        self._rate_lock: threading.Lock = threading.Lock()

        if self._enabled:
            self._start_flush_thread()
            logger.info(
                "PostHog integration initialized (host=%s, key=%s...)",
                POSTHOG_HOST,
                POSTHOG_API_KEY[:8],
            )
        else:
            logger.warning("PostHog integration disabled: POSTHOG_API_KEY not set")

    # ── Public API ──────────────────────────────────────────────────────────

    def track_event(
        self,
        distinct_id: str,
        event: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Queue an event for batched delivery to PostHog.

        Fire-and-forget: never raises, never blocks the caller.
        """
        if not self._enabled:
            return
        try:
            self._enqueue(
                {
                    "event": event,
                    "properties": {
                        **(properties or {}),
                        "distinct_id": distinct_id,
                        "$lib": "nova-posthog-stdlib",
                    },
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as exc:
            logger.error("PostHog track_event failed: %s", exc, exc_info=True)

    def track_page_view(
        self,
        distinct_id: str,
        path: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Track a $pageview event."""
        merged: Dict[str, Any] = {
            **(properties or {}),
            "$current_url": path,
        }
        self.track_event(distinct_id, "$pageview", merged)

    def identify_user(
        self,
        distinct_id: str,
        properties: Dict[str, Any],
    ) -> None:
        """Send an $identify event to set user properties."""
        if not self._enabled:
            return
        try:
            self._enqueue(
                {
                    "event": "$identify",
                    "properties": {
                        "distinct_id": distinct_id,
                        "$set": properties,
                        "$lib": "nova-posthog-stdlib",
                    },
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as exc:
            logger.error("PostHog identify_user failed: %s", exc, exc_info=True)

    def get_stats(self) -> Dict[str, Any]:
        """Return internal stats for the admin endpoint."""
        with self._stats_lock:
            return {
                "enabled": self._enabled,
                "total_events_tracked": self._total_events,
                "events_by_type": dict(self._events_by_type),
                "flush_queue_size": self._queue_size(),
                "last_flush_time": self._last_flush_time,
                "total_flushes": self._flush_count,
                "posthog_host": POSTHOG_HOST,
            }

    def shutdown(self) -> None:
        """Flush remaining events and stop the flush thread."""
        self._shutdown = True
        if self._queue_size() > 0:
            self._flush()

    # ── Internal ────────────────────────────────────────────────────────────

    def _enqueue(self, event_payload: Dict[str, Any]) -> None:
        """Add event to queue; trigger flush if batch is full."""
        if not self._is_rate_allowed():
            logger.debug(
                "PostHog rate limit reached, dropping event: %s",
                event_payload.get("event"),
            )
            return

        event_name = event_payload.get("event") or "unknown"
        with self._lock:
            self._queue.append(event_payload)
            queue_len = len(self._queue)

        with self._stats_lock:
            self._total_events += 1
            self._events_by_type[event_name] += 1

        # Record for rate limiting
        with self._rate_lock:
            self._rate_timestamps.append(time.monotonic())

        if queue_len >= _BATCH_SIZE:
            threading.Thread(
                target=self._flush,
                daemon=True,
                name="posthog-flush-batch",
            ).start()

    def _queue_size(self) -> int:
        """Return current queue length (thread-safe)."""
        with self._lock:
            return len(self._queue)

    def _is_rate_allowed(self) -> bool:
        """Check if we are within the rate limit window."""
        now = time.monotonic()
        with self._rate_lock:
            cutoff = now - _RATE_LIMIT_WINDOW_S
            self._rate_timestamps = [t for t in self._rate_timestamps if t > cutoff]
            return len(self._rate_timestamps) < _RATE_LIMIT_MAX

    def _flush(self) -> None:
        """Send all queued events to PostHog in a single batch."""
        with self._lock:
            if not self._queue:
                return
            batch = self._queue[:]
            self._queue.clear()

        try:
            payload = json.dumps(
                {
                    "api_key": POSTHOG_API_KEY,
                    "batch": batch,
                }
            ).encode("utf-8")

            req = urllib.request.Request(
                CAPTURE_URL,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=_API_TIMEOUT_S) as resp:
                status = resp.getcode()
                if status and status >= 400:
                    body_snippet = resp.read(200).decode("utf-8", errors="replace")
                    logger.error(
                        "PostHog batch flush HTTP %d: %s", status, body_snippet
                    )
                else:
                    logger.debug("PostHog batch flush OK: %d events sent", len(batch))
        except urllib.error.HTTPError as http_err:
            body_snippet = ""
            try:
                body_snippet = http_err.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            logger.error(
                "PostHog flush HTTP %d: %s",
                http_err.code,
                body_snippet,
                exc_info=True,
            )
        except urllib.error.URLError as url_err:
            logger.error("PostHog flush URL error: %s", url_err.reason, exc_info=True)
        except OSError as os_err:
            logger.error("PostHog flush OS error: %s", os_err, exc_info=True)
        finally:
            with self._stats_lock:
                self._last_flush_time = datetime.now(timezone.utc).isoformat()
                self._flush_count += 1

    def _start_flush_thread(self) -> None:
        """Start a daemon thread that flushes on a timer."""

        def _flush_loop() -> None:
            while not self._shutdown:
                time.sleep(_FLUSH_INTERVAL_S)
                if self._queue_size() > 0:
                    try:
                        self._flush()
                    except Exception as exc:
                        logger.error(
                            "PostHog flush thread error: %s", exc, exc_info=True
                        )

        self._flush_thread = threading.Thread(
            target=_flush_loop,
            daemon=True,
            name="posthog-flush-timer",
        )
        self._flush_thread.start()


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON + MODULE-LEVEL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
_client: Optional[PostHogClient] = None
_client_lock: threading.Lock = threading.Lock()


def _get_client() -> PostHogClient:
    """Lazy-init singleton PostHogClient (thread-safe)."""
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        _client = PostHogClient()
    return _client


def track_event(
    distinct_id: str,
    event: str,
    properties: Optional[Dict[str, Any]] = None,
) -> None:
    """Track a named event for a user (fire-and-forget)."""
    _get_client().track_event(distinct_id, event, properties)


def track_page_view(
    distinct_id: str,
    path: str,
    properties: Optional[Dict[str, Any]] = None,
) -> None:
    """Track a page view event."""
    _get_client().track_page_view(distinct_id, path, properties)


def identify_user(
    distinct_id: str,
    properties: Dict[str, Any],
) -> None:
    """Send an identify event to set user properties in PostHog."""
    _get_client().identify_user(distinct_id, properties)


def get_stats() -> Dict[str, Any]:
    """Return PostHog client stats for the admin endpoint."""
    return _get_client().get_stats()


def hash_ip(ip: str) -> str:
    """Hash an IP address for use as an anonymous distinct_id.

    Uses SHA-256 with a static salt so the same IP always maps to
    the same distinct_id within this deployment, but cannot be
    reversed back to the original IP.
    """
    salted = f"nova-posthog-{ip}"
    return hashlib.sha256(salted.encode("utf-8")).hexdigest()[:16]


def shutdown() -> None:
    """Flush remaining events before process exit."""
    if _client is not None:
        _client.shutdown()
