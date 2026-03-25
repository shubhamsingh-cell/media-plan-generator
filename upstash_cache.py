#!/usr/bin/env python3
"""Upstash Redis HTTP API wrapper for persistent caching (L3 layer).

Replaces the Supabase persistent cache with Upstash Redis, using only stdlib
(urllib.request).  Provides GET/SET/DEL with automatic JSON serialization
and TTL support.

Environment variables:
    UPSTASH_REDIS_URL   - REST API endpoint (e.g. https://xxx.upstash.io)
    UPSTASH_REDIS_TOKEN - Bearer token for authentication

The module exposes 3 functions that mirror the Supabase cache interface:
    cache_get(key) -> Optional[Any]
    cache_set(key, data, ttl_seconds, category) -> None
    cache_delete(key) -> None
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Accept both Upstash naming conventions: UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_URL
UPSTASH_URL = (
    (
        os.environ.get("UPSTASH_REDIS_REST_URL")
        or os.environ.get("UPSTASH_REDIS_URL")
        or ""
    )
    .strip()
    .rstrip("/")
)
UPSTASH_TOKEN = (
    os.environ.get("UPSTASH_REDIS_REST_TOKEN")
    or os.environ.get("UPSTASH_REDIS_TOKEN")
    or ""
).strip()

# Connection validation
_ENABLED = bool(UPSTASH_URL and UPSTASH_TOKEN)

# Rate-limit protection: max 1000 cache ops per minute
_op_count = 0
_op_window_start = 0.0
_op_lock = threading.Lock()
_MAX_OPS_PER_MINUTE = 1000

# Request timeout (seconds)
_TIMEOUT = 5

# Circuit breaker: disable after N consecutive failures to prevent error spam
_consecutive_failures = 0
_failure_lock = threading.Lock()
_MAX_CONSECUTIVE_FAILURES = 5
_circuit_open_until = 0.0  # timestamp when circuit breaker resets


def _rate_ok() -> bool:
    """Check if we're within the rate limit window."""
    global _op_count, _op_window_start
    now = time.time()
    with _op_lock:
        if now - _op_window_start > 60:
            _op_count = 0
            _op_window_start = now
        if _op_count >= _MAX_OPS_PER_MINUTE:
            return False
        _op_count += 1
        return True


def _execute(command: list) -> Any:
    """Execute an Upstash Redis REST command and return the result.

    Upstash REST API: POST {url}  with body = JSON array of command parts.
    Headers: Authorization: Bearer {token}
    Response: {"result": <value>}
    """
    global _consecutive_failures, _circuit_open_until

    if not _ENABLED:
        return None

    # Circuit breaker: skip requests when circuit is open
    now = time.time()
    if _consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
        if now < _circuit_open_until:
            return None  # Circuit open -- silently skip
        # Reset circuit breaker after cooldown
        with _failure_lock:
            _consecutive_failures = 0
            logger.info("Upstash circuit breaker reset -- retrying")

    if not _rate_ok():
        return None

    payload = json.dumps(command).encode("utf-8")
    req = urllib.request.Request(
        UPSTASH_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {UPSTASH_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            # Success -- reset failure counter
            with _failure_lock:
                _consecutive_failures = 0
            return body.get("result")
    except urllib.error.HTTPError as e:
        logger.debug("Upstash HTTP error %d for %s", e.code, command[0])
        return None
    except (urllib.error.URLError, OSError, ConnectionError) as e:
        # DNS/network failures -- increment circuit breaker
        with _failure_lock:
            _consecutive_failures += 1
            if _consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                _circuit_open_until = time.time() + 60.0  # 60s cooldown
                logger.warning(
                    "Upstash circuit breaker OPEN after %d failures (cooldown 60s): %s",
                    _consecutive_failures,
                    e,
                )
        return None
    except Exception as e:
        logger.debug("Upstash request failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Public API (compatible with supabase_cache interface)
# ---------------------------------------------------------------------------


def cache_get(key: str) -> Optional[Any]:
    """Retrieve a cached value by key. Returns None on miss or error."""
    raw = _execute(["GET", f"cache:{key}"])
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw


def cache_set(
    key: str, data: Any, ttl_seconds: int = 86400, category: str = "api"
) -> None:
    """Store a value with optional TTL (default 24h). Category is for namespacing."""
    try:
        value = json.dumps(data, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return
    # SET with EX (TTL in seconds)
    _execute(["SET", f"cache:{key}", value, "EX", str(int(ttl_seconds))])


def cache_delete(key: str) -> None:
    """Delete a cached key."""
    _execute(["DEL", f"cache:{key}"])


# ---------------------------------------------------------------------------
# Health check (used by data_matrix_monitor extended_health)
# ---------------------------------------------------------------------------


def ping() -> bool:
    """Return True if Upstash is reachable."""
    if not _ENABLED:
        return False
    result = _execute(["PING"])
    return result == "PONG"


def get_stats() -> dict:
    """Return basic cache stats for monitoring."""
    if not _ENABLED:
        return {"status": "disabled", "detail": "UPSTASH_REDIS_URL not configured"}
    try:
        info = _execute(["DBSIZE"])
        reachable = ping()
        return {
            "status": "ok" if reachable else "error",
            "detail": f"{info or 0} keys, {'connected' if reachable else 'unreachable'}",
            "keys": info or 0,
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


if _ENABLED:
    logger.info("Upstash Redis cache enabled: %s", UPSTASH_URL[:40] + "...")
else:
    logger.info("Upstash Redis cache disabled (env vars not set)")
