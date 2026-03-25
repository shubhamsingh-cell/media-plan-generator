"""Shared Supabase client singleton for Nova AI Suite.

Provides a thread-safe, lazily-initialized Supabase client used by all
modules that need SDK access (nova_persistence, nova_memory, seeder, etc.).

Usage:
    from supabase_client import get_client

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
from typing import Any, Optional

logger = logging.getLogger(__name__)

_client: Optional[Any] = None
_lock = threading.Lock()
_init_attempted: bool = False


def get_client() -> Optional[Any]:
    """Get or initialize the shared Supabase client (lazy, thread-safe).

    Returns the Supabase client on success, or None if:
    - Environment variables are not set
    - The supabase-py SDK is not installed
    - Client initialization failed

    Returns:
        Supabase client instance or None.
    """
    global _client, _init_attempted

    if _client is not None:
        return _client

    if _init_attempted:
        return None

    with _lock:
        if _client is not None:
            return _client

        if _init_attempted:
            return None

        _init_attempted = True

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

            _client = create_client(url, key)
            logger.info("[supabase_client] Initialized successfully")
            return _client
        except ImportError:
            logger.warning(
                "[supabase_client] supabase-py not installed; "
                "SDK features unavailable"
            )
            return None
        except Exception as e:
            logger.error("[supabase_client] Failed to initialize: %s", e, exc_info=True)
            return None


def reset_client() -> None:
    """Reset the client singleton (for testing only).

    Forces re-initialization on next get_client() call.
    """
    global _client, _init_attempted
    with _lock:
        _client = None
        _init_attempted = False
