"""Shared Supabase client singleton for Nova AI Suite.

Provides a thread-safe, lazily-initialized Supabase client used by all
modules that need SDK access (nova_persistence, nova_memory, seeder, etc.).

Features:
    - Singleton pattern with double-checked locking
    - Connection health check before reuse
    - Retry logic with exponential backoff (max 3 retries)
    - Metrics for connection reuse vs new connections

Usage:
    from supabase_client import get_client, get_connection_metrics

    client = get_client()
    if client:
        result = client.table("my_table").select("*").execute()

Environment variables required:
    SUPABASE_URL -- Supabase project URL (e.g., https://xxxxx.supabase.co)
    SUPABASE_SERVICE_ROLE_KEY -- Service role key (full access, server-side only)

Thread-safe via double-checked locking.
"""

import logging
import os
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_client: Optional[Any] = None
_lock = threading.Lock()
_init_attempted: bool = False

# ── Connection metrics ─────────────────────────────────────────────────────
_metrics_lock = threading.Lock()
_connection_metrics: Dict[str, int] = {
    "reuse_count": 0,
    "new_count": 0,
    "health_check_pass": 0,
    "health_check_fail": 0,
    "retry_count": 0,
    "retry_success": 0,
    "retry_exhausted": 0,
}

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 0.5  # seconds; exponential: 0.5, 1.0, 2.0


def _increment_metric(key: str, amount: int = 1) -> None:
    """Thread-safe metric increment."""
    with _metrics_lock:
        _connection_metrics[key] = _connection_metrics.get(key, 0) + amount


def _health_check(client: Any) -> bool:
    """Verify the Supabase client can reach the server.

    Performs a lightweight query to confirm the connection is alive.
    Returns True if healthy, False otherwise.
    """
    try:
        # A minimal query that should always succeed if the connection is alive
        client.table("cache").select("key").limit(1).execute()
        _increment_metric("health_check_pass")
        return True
    except Exception as exc:
        logger.warning(f"[supabase_client] Health check failed: {exc}")
        _increment_metric("health_check_fail")
        return False


def _create_client() -> Optional[Any]:
    """Create a new Supabase client instance.

    Returns:
        A fresh Supabase client, or None on failure.
    """
    url = os.environ.get("SUPABASE_URL") or ""
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""

    if not url or not key:
        logger.info(
            "[supabase_client] Disabled: SUPABASE_URL or "
            "SUPABASE_SERVICE_ROLE_KEY not set"
        )
        return None

    try:
        from supabase import create_client

        client = create_client(url, key)
        logger.info("[supabase_client] Created new client successfully")
        _increment_metric("new_count")
        return client
    except ImportError:
        logger.warning(
            "[supabase_client] supabase-py not installed; " "SDK features unavailable"
        )
        return None
    except Exception as e:
        logger.error(f"[supabase_client] Failed to create client: {e}", exc_info=True)
        return None


def get_client() -> Optional[Any]:
    """Get or initialize the shared Supabase client (lazy, thread-safe).

    Performs a health check on the existing client before reuse.
    If the health check fails, attempts to recreate the client with
    exponential backoff retry (up to 3 attempts).

    Returns:
        Supabase client instance or None.
    """
    global _client, _init_attempted

    # Fast path: existing healthy client
    if _client is not None:
        if _health_check(_client):
            _increment_metric("reuse_count")
            return _client
        # Health check failed -- force re-creation
        logger.warning("[supabase_client] Existing client unhealthy, recreating")
        with _lock:
            _client = None
            _init_attempted = False

    if _init_attempted and _client is None:
        return None

    with _lock:
        # Double-check after acquiring lock
        if _client is not None:
            _increment_metric("reuse_count")
            return _client

        if _init_attempted:
            return None

        _init_attempted = True

        # Retry loop with exponential backoff
        for attempt in range(1, _MAX_RETRIES + 1):
            new_client = _create_client()
            if new_client is not None:
                _client = new_client
                if attempt > 1:
                    _increment_metric("retry_success")
                    logger.info(
                        f"[supabase_client] Connected on retry attempt {attempt}"
                    )
                return _client

            if attempt < _MAX_RETRIES:
                _increment_metric("retry_count")
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    f"[supabase_client] Attempt {attempt}/{_MAX_RETRIES} failed, "
                    f"retrying in {delay:.1f}s"
                )
                time.sleep(delay)

        _increment_metric("retry_exhausted")
        logger.error(f"[supabase_client] All {_MAX_RETRIES} connection attempts failed")
        return None


def get_connection_metrics() -> Dict[str, Any]:
    """Return current connection pool metrics.

    Returns:
        Dict with reuse_count, new_count, health_check stats, retry stats.
    """
    with _metrics_lock:
        total_requests = (
            _connection_metrics["reuse_count"] + _connection_metrics["new_count"]
        )
        reuse_rate = (
            round(_connection_metrics["reuse_count"] / total_requests * 100, 1)
            if total_requests > 0
            else 0.0
        )
        return {
            **_connection_metrics,
            "total_requests": total_requests,
            "reuse_rate_pct": reuse_rate,
            "client_alive": _client is not None,
        }


def reset_client() -> None:
    """Reset the client singleton (for testing only).

    Forces re-initialization on next get_client() call.
    """
    global _client, _init_attempted
    with _lock:
        _client = None
        _init_attempted = False


def reset_metrics() -> None:
    """Reset connection metrics (for testing only)."""
    with _metrics_lock:
        for key in _connection_metrics:
            _connection_metrics[key] = 0
