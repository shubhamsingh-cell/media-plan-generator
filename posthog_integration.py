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
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
def _resolve_capture_key() -> tuple[str, str]:
    """Return ``(key, env_var_name)`` for the configured capture key.

    ``POSTHOG_PROJECT_API_KEY`` wins over ``POSTHOG_API_KEY``. Returns
    ``("", "")`` when neither is set.
    """
    for name in ("POSTHOG_PROJECT_API_KEY", "POSTHOG_API_KEY"):
        value = (os.environ.get(name) or "").strip()
        if value:
            return value, name
    return "", ""


POSTHOG_API_KEY: str
_POSTHOG_KEY_ENV: str
POSTHOG_API_KEY, _POSTHOG_KEY_ENV = _resolve_capture_key()
POSTHOG_HOST: str = os.environ.get("POSTHOG_HOST") or "https://us.i.posthog.com"
CAPTURE_URL: str = f"{POSTHOG_HOST}/batch/"

_FLUSH_INTERVAL_S: float = 5.0  # Flush every 5 seconds
_BATCH_SIZE: int = 10  # Flush when batch hits 10 events
_RATE_LIMIT_MAX: int = 100  # Max events per minute
_RATE_LIMIT_WINDOW_S: float = 60.0
_API_TIMEOUT_S: int = 5  # HTTP timeout for PostHog API

# Permanent auth failures (HTTP 401/403) back off exponentially: the first
# failure pauses flushing for _AUTH_BACKOFF_BASE_S, every further consecutive
# failure doubles the pause, capped at _AUTH_BACKOFF_MAX_S (1 h). One log line
# per backoff window instead of one per flush attempt.
_AUTH_BACKOFF_BASE_S: float = 30.0
_AUTH_BACKOFF_MAX_S: float = 3600.0

# PostHog PERSONAL api keys start with "phx_"; the /batch capture endpoint only
# accepts a PROJECT key ("phc_..."). Using a personal key here 401s on every
# flush (prod 2026-10-01: ~286 ERROR lines/hour, 100% of analytics dropped).
_PERSONAL_KEY_PREFIX: str = "phx_"


def _is_personal_api_key(key: str) -> bool:
    """True when ``key`` is a PostHog personal api key (``phx_`` prefix)."""
    return (key or "").strip().lower().startswith(_PERSONAL_KEY_PREFIX)


# PostHog PROJECT keys ("phc_...") are write-only capture keys that PostHog's own
# snippet ships to every browser. They are the ONLY PostHog key a browser may see.
_PROJECT_KEY_PREFIX: str = "phc_"


# Warn-once registry (bounded): log a given warning key at most once per
# process so a chatty caller cannot flood the log.
_WARN_ONCE_MAX: int = 200
_warned_once: set = set()
_warned_once_lock: threading.Lock = threading.Lock()


def _warn_once(key: str, message: str, *args: Any) -> None:
    """Log ``message`` at WARNING the first time ``key`` is seen (bounded set)."""
    with _warned_once_lock:
        if key in _warned_once:
            return
        if len(_warned_once) >= _WARN_ONCE_MAX:
            return
        _warned_once.add(key)
    logger.warning(message, *args)


def get_browser_capture_key() -> str:
    """Return the PostHog key that is safe to serve to a browser, else ``""``.

    Resolves the key exactly like the server-side client (``_resolve_capture_key``:
    ``POSTHOG_PROJECT_API_KEY`` wins over ``POSTHOG_API_KEY``), but at CALL time,
    and returns it only when it is a PROJECT key (``phc_`` prefix). Anything else
    -- above all a PERSONAL api key (``phx_``), which grants read/admin access to
    the PostHog account -- yields ``""`` and one warning that names the env var
    and never the value. An allowlist on purpose: an unknown prefix is withheld.

    This is the single gate for ``GET /api/config`` (public, unauthenticated), so
    the browser receives a key iff the server-side client could also use it.
    """
    key, env_name = _resolve_capture_key()
    if key.startswith(_PROJECT_KEY_PREFIX):
        return key
    if key:
        _warn_once(
            f"browser-key-withheld:{env_name}",
            "PostHog key in %s is not a project key (expected the %s prefix): "
            "NOT served to browsers. If it is a personal api key (%s prefix) it "
            "was exposed before this guard -- rotate it in PostHog.",
            env_name,
            _PROJECT_KEY_PREFIX,
            _PERSONAL_KEY_PREFIX,
        )
    return ""


# Dead-letter queue for events that failed to flush (Phase 6)
_dead_letter_queue: deque = deque(maxlen=500)


# ═══════════════════════════════════════════════════════════════════════════════
# POSTHOG CLIENT
# ═══════════════════════════════════════════════════════════════════════════════
class PostHogClient:
    """Lightweight, stdlib-only PostHog event tracker with batching."""

    def __init__(self) -> None:
        self._enabled: bool = bool(POSTHOG_API_KEY)
        self._disabled_reason: str = "" if self._enabled else "no_api_key"
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

        # Auth-failure backoff (HTTP 401/403): monotonic deadline before which
        # no flush is attempted, plus the consecutive-failure streak that
        # drives the exponential window.
        self._backoff_lock: threading.Lock = threading.Lock()
        self._backoff_until: float = 0.0
        self._auth_fail_streak: int = 0
        self._dropped_in_backoff: int = 0

        if self._enabled and _is_personal_api_key(POSTHOG_API_KEY):
            # A personal key can never capture events. Disable up front with
            # ONE clear warning instead of 401-ing on every flush forever.
            # The key itself is never logged.
            self._enabled = False
            self._disabled_reason = "personal_api_key"
            logger.warning(
                "PostHog analytics DISABLED: env var %s holds a PostHog "
                "PERSONAL api key (phx_ prefix), which the capture API "
                "rejects with HTTP 401. A PROJECT api key (phc_...) from "
                "PostHog > Project settings is required: set it in "
                "POSTHOG_PROJECT_API_KEY (takes precedence over "
                "POSTHOG_API_KEY).",
                _POSTHOG_KEY_ENV or "POSTHOG_API_KEY",
            )
        elif self._enabled:
            self._start_flush_thread()
            logger.info(
                "PostHog integration initialized (host=%s, key=%s)",
                POSTHOG_HOST,
                "phc_..." if POSTHOG_API_KEY.startswith("phc_") else "set",
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
                "dead_letter_queue_size": len(_dead_letter_queue),
                "last_flush_time": self._last_flush_time,
                "total_flushes": self._flush_count,
                "posthog_host": POSTHOG_HOST,
                "disabled_reason": self._disabled_reason,
                "auth_backoff_active": self._in_backoff(),
                "auth_fail_streak": self._auth_fail_streak,
                "dropped_in_backoff": self._dropped_in_backoff,
            }

    def shutdown(self) -> None:
        """Flush remaining events and stop the flush thread."""
        self._shutdown = True
        if self._queue_size() > 0:
            self._flush()

    # ── Internal ────────────────────────────────────────────────────────────

    def _enqueue(self, event_payload: Dict[str, Any]) -> None:
        """Add event to queue; trigger flush if batch is full."""
        if self._in_backoff():
            # Permanent auth failure window: nothing can be delivered, so do
            # not let the queue grow. Dropped events are counted in stats.
            with self._backoff_lock:
                self._dropped_in_backoff += 1
            return
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

    def _in_backoff(self) -> bool:
        """True while a permanent-auth-failure backoff window is open."""
        return time.monotonic() < self._backoff_until

    def _open_backoff_window(self, code: int, body_snippet: str) -> None:
        """Open (or extend the streak of) the exponential backoff window.

        Logs ONCE per window: if another flush thread already opened the
        current window, this call is silent.
        """
        now = time.monotonic()
        with self._backoff_lock:
            if now < self._backoff_until:
                return  # a concurrent flush already opened + logged this window
            self._auth_fail_streak += 1
            streak = self._auth_fail_streak
            delay = min(
                _AUTH_BACKOFF_MAX_S,
                _AUTH_BACKOFF_BASE_S * (2 ** min(streak - 1, 16)),
            )
            self._backoff_until = now + delay
        logger.error(
            "PostHog flush HTTP %d (permanent auth failure #%d): pausing "
            "flushes for %ds; events in the window are dropped. Check the "
            "PostHog PROJECT key (phc_...) in %s: %s",
            code,
            streak,
            int(delay),
            _POSTHOG_KEY_ENV or "POSTHOG_PROJECT_API_KEY",
            body_snippet,
        )

    def _reset_backoff(self) -> None:
        """A successful flush closes the window and resets the streak."""
        with self._backoff_lock:
            self._auth_fail_streak = 0
            self._backoff_until = 0.0

    def _flush(self) -> None:
        """Send all queued events to PostHog in a single batch."""
        if self._in_backoff():
            # Inside an auth-failure window: drop whatever is queued rather
            # than hitting the API (and the log) again.
            with self._lock:
                dropped = len(self._queue)
                self._queue.clear()
            if dropped:
                with self._backoff_lock:
                    self._dropped_in_backoff += dropped
            return

        # Retry dead-letter events first (Phase 6)
        retried: List[Dict[str, Any]] = []
        while _dead_letter_queue:
            try:
                retried.append(_dead_letter_queue.popleft())
            except IndexError:
                break

        with self._lock:
            if not self._queue and not retried:
                return
            batch = retried + self._queue[:]
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
                    self._reset_backoff()
        except urllib.error.HTTPError as http_err:
            body_snippet = ""
            try:
                body_snippet = http_err.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            if http_err.code in (401, 403):
                # Permanent auth failure (invalid / forbidden API key):
                # retrying can never succeed. Drop the batch (no dead-letter)
                # AND stop calling the API for an exponentially growing window
                # (30 s doubling, cap 1 h), logging once per window -- before
                # this, every flush attempt logged an ERROR (~286/h in prod).
                self._open_backoff_window(http_err.code, body_snippet)
            elif http_err.code == 400:
                # Bad request: this batch is unsendable as-is. Log and drop it
                # instead of dead-lettering (see test_posthog_dead_letter).
                logger.error(
                    "PostHog flush HTTP %d (permanent failure, not retrying): %s",
                    http_err.code,
                    body_snippet,
                )
            else:
                logger.error(
                    "PostHog flush HTTP %d: %s",
                    http_err.code,
                    body_snippet,
                    exc_info=True,
                )
                # Dead-letter queue: preserve failed events for retry (Phase 6)
                for evt in batch:
                    _dead_letter_queue.append(evt)
                logger.info(
                    "[PostHog] %d events moved to dead-letter queue (HTTP error)",
                    len(batch),
                )
        except urllib.error.URLError as url_err:
            logger.error("PostHog flush URL error: %s", url_err.reason, exc_info=True)
            for evt in batch:
                _dead_letter_queue.append(evt)
            logger.info(
                "[PostHog] %d events moved to dead-letter queue (URL error)", len(batch)
            )
        except OSError as os_err:
            logger.error("PostHog flush OS error: %s", os_err, exc_info=True)
            for evt in batch:
                _dead_letter_queue.append(evt)
            logger.info(
                "[PostHog] %d events moved to dead-letter queue (OS error)", len(batch)
            )
        finally:
            with self._stats_lock:
                self._last_flush_time = datetime.now(timezone.utc).isoformat()
                self._flush_count += 1

    def _start_flush_thread(self) -> None:
        """Start a daemon thread that flushes on a timer."""

        def _flush_loop() -> None:
            while not self._shutdown:
                time.sleep(_FLUSH_INTERVAL_S)
                if self._queue_size() > 0 or len(_dead_letter_queue) > 0:
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
# CONSENT, SAMPLING, AND SCHEMA REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════
_consent_granted: bool = True


def set_consent(allowed: bool) -> None:
    """Enable/disable all tracking based on user consent."""
    global _consent_granted
    _consent_granted = allowed


_SAMPLE_RATES: dict[str, float] = {}  # event_prefix -> rate (0.0-1.0)


def set_sample_rate(event_prefix: str, rate: float) -> None:
    """Set sampling rate for events with this prefix.

    rate=1.0 means track all, rate=0.0 means drop all.
    """
    _SAMPLE_RATES[event_prefix] = max(0.0, min(1.0, rate))


_EVENT_SCHEMAS: dict[str, list[str]] = {
    "plan.generated": ["plan_type", "budget", "channels"],
    "intelligence.scraped": ["source", "query", "results_count"],
    "compliance.checked": ["audit_type", "score"],
    "nova.chat.sent": ["message_length", "module_context"],
    "page.viewed": ["path", "referrer"],
}


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
    # Consent gate
    if not _consent_granted:
        return

    # Sampling gate
    import random

    for prefix, rate in _SAMPLE_RATES.items():
        if event.startswith(prefix):
            if random.random() > rate:
                logger.debug("[PostHog] Event %s sampled out (rate=%.2f)", event, rate)
                return
            break

    # Event taxonomy enforcement (Phase 6)
    VALID_PREFIXES = (
        "plan.",
        "intelligence.",
        "compliance.",
        "nova.",
        "platform.",
        "page.",
        "auth.",
        "api.",
        "system.",
    )
    if not any(event.startswith(p) for p in VALID_PREFIXES):
        _warn_once(
            f"event-name:{event}",
            "[PostHog] Non-standard event name: %s (should start with %s)",
            event,
            "/".join(VALID_PREFIXES),
        )

    # Property schema validation
    if event in _EVENT_SCHEMAS and properties:
        expected = _EVENT_SCHEMAS[event]
        missing = [k for k in expected if k not in properties]
        if missing:
            _warn_once(
                f"event-props:{event}:{','.join(missing)}",
                "[PostHog] Event %s missing expected properties: %s",
                event,
                ", ".join(missing),
            )

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


def track_group(
    group_type: str,
    group_key: str,
    properties: Optional[Dict[str, Any]] = None,
) -> None:
    """Track group-level analytics (team/company)."""
    track_event(
        "system",
        "$group_identify",
        {
            "$group_type": group_type,
            "$group_key": group_key,
            "$group_set": properties or {},
        },
    )


def alias(distinct_id: str, alias_id: str) -> None:
    """Link anonymous user to authenticated user."""
    track_event(
        distinct_id,
        "$create_alias",
        {
            "distinct_id": distinct_id,
            "alias": alias_id,
        },
    )


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


def is_feature_enabled(
    flag_name: str, distinct_id: str = "default", default: bool = False
) -> bool:
    """Check if a PostHog feature flag is enabled.

    Falls back to default if PostHog is unavailable or flag doesn't exist.
    Caches results for 5 minutes to avoid excessive API calls.
    """
    if not POSTHOG_API_KEY or not POSTHOG_HOST:
        return default
    if _is_personal_api_key(POSTHOG_API_KEY):
        return default  # /decide needs a PROJECT key; see PostHogClient.__init__

    cache_key = f"ff:{flag_name}:{distinct_id}"
    now = time.monotonic()

    # Check cache
    cached = getattr(is_feature_enabled, "_cache", {}).get(cache_key)
    if cached and now - cached["ts"] < 300:  # 5 min TTL
        return cached["value"]

    try:
        url = f"{POSTHOG_HOST}/decide/?v=3"
        payload = json.dumps(
            {
                "api_key": POSTHOG_API_KEY,
                "distinct_id": distinct_id,
                "groups": {},
            }
        ).encode()
        req = urllib.request.Request(url, data=payload, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            flags = data.get("featureFlags", {})
            result = bool(flags.get(flag_name, default))
            # Cache result
            if not hasattr(is_feature_enabled, "_cache"):
                is_feature_enabled._cache = {}
            is_feature_enabled._cache[cache_key] = {"value": result, "ts": now}
            return result
    except Exception as e:
        logger.debug("[PostHog] Feature flag check failed for %s: %s", flag_name, e)
        return default


def shutdown() -> None:
    """Flush remaining events before process exit."""
    if _client is not None:
        _client.shutdown()
