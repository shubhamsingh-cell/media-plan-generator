#!/usr/bin/env python3
"""Request coalescing for identical Nova chat queries using Upstash Redis.

When multiple users submit the same query within a short window, only one
request (the "leader") actually calls the LLM.  All other identical requests
("followers") wait for the leader's result via a threading.Event and a
shared Redis key.

Architecture:
    1. Normalize the query (lowercase, strip, collapse punctuation).
    2. Hash it with SHA-256 (first 16 hex chars) for the Redis key.
    3. Atomically SET NX in Redis with a 5-second coalescing window.
       - If SET NX succeeds -> this request is the leader.
       - If SET NX fails   -> this request is a follower; poll Redis
         for the result (up to 5 s) using a local threading.Event.
    4. When the leader finishes, it stores the result in Redis with a
       30-second TTL and signals all local followers.

Thread safety:
    - A module-level lock protects the in-flight registry.
    - Each in-flight hash gets its own threading.Event.

Environment variables (already set on Render):
    UPSTASH_REDIS_REST_URL   - Upstash REST endpoint
    UPSTASH_REDIS_REST_TOKEN - Bearer token
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_COALESCE_WINDOW_S = 5  # seconds a leader slot stays open
_RESULT_TTL_S = 30  # seconds a completed result stays in Redis
_POLL_INTERVAL_S = 0.25  # follower poll interval
_REDIS_TIMEOUT_S = 3  # HTTP timeout for Upstash calls
_KEY_PREFIX = "coal:"  # Redis key namespace

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

_ENABLED = bool(UPSTASH_URL and UPSTASH_TOKEN)

# ---------------------------------------------------------------------------
# Stats (thread-safe counters)
# ---------------------------------------------------------------------------

_stats_lock = threading.Lock()
_stats: dict[str, int] = {
    "leader_count": 0,
    "coalesced_count": 0,
    "timeout_count": 0,
}


def get_stats() -> dict[str, int]:
    """Return a snapshot of coalescing statistics."""
    with _stats_lock:
        return dict(_stats)


def _inc(key: str) -> None:
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + 1


# ---------------------------------------------------------------------------
# Query normalisation & hashing
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_MULTI_WS = re.compile(r"\s+")


def _normalize(query: str) -> str:
    """Lowercase, strip, collapse whitespace, remove punctuation."""
    q = query.lower().strip()
    q = _PUNCT_RE.sub(" ", q)
    q = _MULTI_WS.sub(" ", q).strip()
    return q


def _hash_query(query: str) -> str:
    """Return first 16 hex chars of SHA-256 of normalised query."""
    normalised = _normalize(query)
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Upstash Redis helpers (stdlib only)
# ---------------------------------------------------------------------------


def _redis_cmd(cmd: list[str]) -> Any:
    """Execute a single Upstash Redis REST command and return the result.

    Args:
        cmd: Redis command as a list of strings, e.g. ["SET", "key", "val"].

    Returns:
        The parsed ``result`` field from the Upstash JSON response.

    Raises:
        RuntimeError: If Redis is not configured or the HTTP call fails.
    """
    if not _ENABLED:
        raise RuntimeError("Upstash Redis not configured")

    url = f"{UPSTASH_URL}"
    body = json.dumps(cmd).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {UPSTASH_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_REDIS_TIMEOUT_S) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("result")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
        logger.error("Upstash Redis command failed: %s", exc, exc_info=True)
        raise RuntimeError(f"Redis command failed: {exc}") from exc


def _redis_set_nx(key: str, value: str, ttl_s: int) -> bool:
    """SET key value EX ttl NX -- returns True if the key was set (leader)."""
    result = _redis_cmd(["SET", key, value, "EX", str(ttl_s), "NX"])
    return result == "OK"


def _redis_get(key: str) -> Optional[str]:
    """GET key -- returns the value or None."""
    return _redis_cmd(["GET", key])


def _redis_set_ex(key: str, value: str, ttl_s: int) -> None:
    """SET key value EX ttl -- unconditional set with expiry."""
    _redis_cmd(["SET", key, value, "EX", str(ttl_s)])


def _redis_del(key: str) -> None:
    """DEL key -- remove the key."""
    _redis_cmd(["DEL", key])


# ---------------------------------------------------------------------------
# In-flight registry (local process)
# ---------------------------------------------------------------------------

_inflight_lock = threading.Lock()
_inflight: dict[str, threading.Event] = {}


def _register_local(qhash: str) -> threading.Event:
    """Register or retrieve a local Event for an in-flight query hash."""
    with _inflight_lock:
        if qhash not in _inflight:
            _inflight[qhash] = threading.Event()
        return _inflight[qhash]


def _unregister_local(qhash: str) -> None:
    """Remove the local Event once the leader has stored the result."""
    with _inflight_lock:
        _inflight.pop(qhash, None)


# ---------------------------------------------------------------------------
# Core coalescer class
# ---------------------------------------------------------------------------


class RequestCoalescer:
    """Coalesce identical Nova chat requests via Upstash Redis.

    Usage in the chat handler:

        coalescer = get_coalescer()
        is_leader, qhash, cached = coalescer.check_or_register(user_msg)
        if not is_leader and cached:
            return cached          # follower got the leader's result
        # ... run the LLM ...
        coalescer.complete(qhash, result_dict)
    """

    def check_or_register(self, query: str) -> tuple[bool, str, Optional[dict]]:
        """Check if this query is already in-flight; register if not.

        Args:
            query: Raw user message.

        Returns:
            (is_leader, query_hash, cached_result)
            - is_leader=True  -> caller must process the request and call complete().
            - is_leader=False -> cached_result contains the leader's response
              (or None if the wait timed out).
        """
        if not _ENABLED:
            return True, "", None

        qhash = _hash_query(query)
        lock_key = f"{_KEY_PREFIX}lock:{qhash}"
        result_key = f"{_KEY_PREFIX}res:{qhash}"

        # -- Check if a result already exists (previous leader finished) --
        try:
            existing = _redis_get(result_key)
            if existing:
                _inc("coalesced_count")
                logger.info("Coalesce HIT (cached result) for hash %s", qhash)
                return False, qhash, json.loads(existing)
        except (RuntimeError, json.JSONDecodeError) as exc:
            logger.debug("Coalesce result check failed: %s", exc)

        # -- Try to become the leader --
        try:
            got_lock = _redis_set_nx(lock_key, "1", _COALESCE_WINDOW_S)
        except RuntimeError:
            # Redis down -- fall through as leader (no coalescing)
            return True, qhash, None

        if got_lock:
            # This request is the leader
            _inc("leader_count")
            _register_local(qhash)
            logger.info("Coalesce LEADER for hash %s", qhash)
            return True, qhash, None

        # -- Follower path: wait for the leader's result --
        logger.info("Coalesce FOLLOWER waiting for hash %s", qhash)
        event = _register_local(qhash)
        deadline = time.monotonic() + _COALESCE_WINDOW_S

        while time.monotonic() < deadline:
            # Check local event first (fastest for same-process coalescing)
            if event.wait(timeout=_POLL_INTERVAL_S):
                try:
                    raw = _redis_get(result_key)
                    if raw:
                        _inc("coalesced_count")
                        logger.info("Coalesce FOLLOWER got result for hash %s", qhash)
                        return False, qhash, json.loads(raw)
                except (RuntimeError, json.JSONDecodeError) as exc:
                    logger.debug("Follower result read failed: %s", exc)
                    break

            # Also poll Redis (handles cross-process coalescing)
            try:
                raw = _redis_get(result_key)
                if raw:
                    _inc("coalesced_count")
                    logger.info(
                        "Coalesce FOLLOWER got result (poll) for hash %s", qhash
                    )
                    return False, qhash, json.loads(raw)
            except (RuntimeError, json.JSONDecodeError):
                pass

        # Timed out -- let this request proceed independently
        _inc("timeout_count")
        logger.warning(
            "Coalesce TIMEOUT for hash %s -- proceeding independently", qhash
        )
        return True, qhash, None

    def complete(self, qhash: str, result: dict) -> None:
        """Store the leader's result in Redis and signal local followers.

        Args:
            qhash: The query hash returned by check_or_register.
            result: The response dict to store.
        """
        if not _ENABLED or not qhash:
            return

        result_key = f"{_KEY_PREFIX}res:{qhash}"

        try:
            _redis_set_ex(result_key, json.dumps(result, default=str), _RESULT_TTL_S)
            logger.info(
                "Coalesce result stored for hash %s (TTL=%ds)", qhash, _RESULT_TTL_S
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            logger.error("Coalesce result store failed: %s", exc, exc_info=True)

        # Signal any local followers
        with _inflight_lock:
            event = _inflight.get(qhash)
            if event:
                event.set()

        # Clean up after a short delay so late followers can still read
        def _cleanup() -> None:
            time.sleep(1.0)
            _unregister_local(qhash)

        threading.Thread(
            target=_cleanup, daemon=True, name=f"coal-cleanup-{qhash}"
        ).start()


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_coalescer: Optional[RequestCoalescer] = None
_coalescer_lock = threading.Lock()


def get_coalescer() -> RequestCoalescer:
    """Return the module-level RequestCoalescer singleton."""
    global _coalescer
    if _coalescer is None:
        with _coalescer_lock:
            if _coalescer is None:
                _coalescer = RequestCoalescer()
                if _ENABLED:
                    logger.info("Request coalescer initialised (Upstash Redis)")
                else:
                    logger.info("Request coalescer initialised (DISABLED -- no Redis)")
    return _coalescer
