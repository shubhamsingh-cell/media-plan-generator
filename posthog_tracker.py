#!/usr/bin/env python3
"""PostHog event tracking via HTTP API (stdlib-only, no SDK dependency).

Captures key product events:
  - plan_generated: media plan created successfully
  - plan_failed: generation failed with error
  - chat_message: Nova chatbot interaction
  - file_downloaded: user downloaded deliverables
  - file_uploaded: user uploaded brief/transcript/historical data

All calls are fire-and-forget (non-blocking background thread).
Events are batched and flushed every 10 seconds or 20 events.

Environment variables:
    POSTHOG_API_KEY - Project API key (phc_xxx)
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POSTHOG_API_KEY = (os.environ.get("POSTHOG_API_KEY") or "").strip()
POSTHOG_HOST = "https://us.i.posthog.com"  # US cloud instance
_ENABLED = bool(POSTHOG_API_KEY)

# Batching config
_BATCH_SIZE = 20
_FLUSH_INTERVAL = 10.0  # seconds
_TIMEOUT = 5  # HTTP request timeout

# Event queue (bounded to prevent memory issues)
_event_queue: queue.Queue = queue.Queue(maxsize=500)
_flush_thread: Optional[threading.Thread] = None
_shutdown = threading.Event()

# ---------------------------------------------------------------------------
# Runtime stats (thread-safe counters for observability)
# ---------------------------------------------------------------------------
_stats_lock = threading.Lock()
_stats: Dict[str, Any] = {
    "total_queued": 0,
    "total_sent": 0,
    "total_dropped": 0,
    "total_send_errors": 0,
    "events_by_type": {},
}


# ---------------------------------------------------------------------------
# Internal: batch sender
# ---------------------------------------------------------------------------


def _send_batch(events: list) -> None:
    """POST a batch of events to PostHog /batch endpoint."""
    if not events or not _ENABLED:
        return
    payload = json.dumps(
        {
            "api_key": POSTHOG_API_KEY,
            "batch": events,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{POSTHOG_HOST}/batch",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            resp.read()  # Drain response
        with _stats_lock:
            _stats["total_sent"] += len(events)
    except Exception as e:
        logger.debug("PostHog batch send failed: %s", e)
        with _stats_lock:
            _stats["total_send_errors"] += 1


def _flush_loop() -> None:
    """Background thread: flush events every FLUSH_INTERVAL or when batch is full."""
    batch = []
    last_flush = time.time()
    while not _shutdown.is_set():
        try:
            event = _event_queue.get(timeout=1.0)
            batch.append(event)
        except queue.Empty:
            pass

        now = time.time()
        if len(batch) >= _BATCH_SIZE or (batch and now - last_flush >= _FLUSH_INTERVAL):
            _send_batch(batch)
            batch = []
            last_flush = now

    # Final flush on shutdown
    while not _event_queue.empty():
        try:
            batch.append(_event_queue.get_nowait())
        except queue.Empty:
            break
    if batch:
        _send_batch(batch)


def _ensure_flush_thread() -> None:
    """Start the background flush thread if not already running."""
    global _flush_thread
    if _flush_thread is not None and _flush_thread.is_alive():
        return
    _flush_thread = threading.Thread(
        target=_flush_loop, daemon=True, name="posthog-flush"
    )
    _flush_thread.start()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def capture(
    event: str, distinct_id: str = "server", properties: Optional[Dict[str, Any]] = None
) -> None:
    """Enqueue a PostHog event (non-blocking).

    Args:
        event: Event name (e.g. "plan_generated", "chat_message")
        distinct_id: User identifier (email or "anonymous")
        properties: Additional event properties
    """
    if not _ENABLED:
        return

    _ensure_flush_thread()

    evt = {
        "event": event,
        "distinct_id": distinct_id,
        "properties": {
            **(properties or {}),
            "$lib": "media-plan-generator",
            "$lib_version": "3.5.1",
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
    }

    try:
        _event_queue.put_nowait(evt)
        with _stats_lock:
            _stats["total_queued"] += 1
            _stats["events_by_type"][event] = _stats["events_by_type"].get(event, 0) + 1
    except queue.Full:
        logger.debug("PostHog event queue full, dropping event: %s", event)
        with _stats_lock:
            _stats["total_dropped"] += 1


def shutdown() -> None:
    """Flush remaining events and stop the background thread."""
    _shutdown.set()
    if _flush_thread and _flush_thread.is_alive():
        _flush_thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Convenience wrappers for common events
# ---------------------------------------------------------------------------


def track_plan_generated(
    email: str,
    client: str,
    industry: str,
    budget: str,
    roles: list,
    gen_time: float,
    file_size: int,
) -> None:
    """Track successful media plan generation."""
    capture(
        "plan_generated",
        distinct_id=email,
        properties={
            "client_name": client,
            "industry": industry,
            "budget": budget,
            "num_roles": len(roles) if roles else 0,
            "generation_time_seconds": round(gen_time, 2),
            "file_size_bytes": file_size,
        },
    )


def track_plan_failed(email: str, client: str, error: str) -> None:
    """Track failed media plan generation."""
    capture(
        "plan_failed",
        distinct_id=email,
        properties={
            "client_name": client,
            "error": error[:200],  # Truncate long errors
        },
    )


def track_chat_message(session_id: str, message_type: str, tokens: int = 0) -> None:
    """Track Nova chatbot interaction."""
    capture(
        "chat_message",
        distinct_id=session_id,
        properties={
            "message_type": message_type,  # "user" or "assistant"
            "tokens": tokens,
        },
    )


def track_file_upload(email: str, upload_type: str, file_count: int) -> None:
    """Track file upload (brief, transcript, historical)."""
    capture(
        "file_uploaded",
        distinct_id=email,
        properties={
            "upload_type": upload_type,
            "file_count": file_count,
        },
    )


# ---------------------------------------------------------------------------
# Stats API (for observability dashboard)
# ---------------------------------------------------------------------------


def get_posthog_stats() -> Dict[str, Any]:
    """Return PostHog tracking runtime statistics.

    Returns dict with: enabled, total_queued, total_sent, total_dropped,
    total_send_errors, events_by_type, queue_size.
    """
    with _stats_lock:
        snapshot = dict(_stats)
        snapshot["events_by_type"] = dict(_stats["events_by_type"])
    snapshot["enabled"] = _ENABLED
    snapshot["queue_size"] = _event_queue.qsize()
    snapshot["queue_max"] = 500
    return snapshot


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if _ENABLED:
    logger.info("PostHog analytics enabled (project key: %s...)", POSTHOG_API_KEY[:10])
else:
    logger.info("PostHog analytics disabled (POSTHOG_API_KEY not set)")
