"""
supabase_cache.py -- Persistent cache backed by Supabase Postgres (L3).

Provides an L3 cache layer (after in-memory L1 and disk L2) using Supabase's
REST API (PostgREST). Gracefully disabled when SUPABASE_URL or SUPABASE_ANON_KEY
environment variables are not set.

Cache hierarchy (managed by api_enrichment.py):
    L1: In-memory dict  -- fastest, lost on restart
    L2: Disk JSON files  -- survives restart, limited by disk
    L3: Supabase Postgres -- survives redeployments, shared across instances

All operations:
    - Use only urllib.request (stdlib, no third-party dependencies)
    - Have a 3-second timeout per HTTP call (cache must not slow the pipeline)
    - Retry once on 5xx errors with 500ms delay
    - Fail gracefully: never raise, always return None/False/0/{}
    - Are thread-safe via threading.Lock on shared state

SETUP:
    1. Set environment variables:
        SUPABASE_URL=https://xxxxx.supabase.co
        SUPABASE_ANON_KEY=eyJhbGciOi...

    2. Create the cache table in your Supabase project SQL editor:

        CREATE TABLE cache (
            key TEXT PRIMARY KEY,
            data JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL,
            category TEXT DEFAULT 'general',
            hit_count INTEGER DEFAULT 0
        );

        -- Index for TTL cleanup
        CREATE INDEX idx_cache_expires ON cache (expires_at);

        -- Index for category queries
        CREATE INDEX idx_cache_category ON cache (category);

        -- Enable Row Level Security (RLS) - required for anon key access
        ALTER TABLE cache ENABLE ROW LEVEL SECURITY;

        -- Policy: allow all operations with anon key (server-side only)
        CREATE POLICY "Allow all operations" ON cache
            FOR ALL USING (true) WITH CHECK (true);

Integration points (do NOT modify these files until instructed):
    - api_enrichment.py: in _get_cached(), after disk miss, call
        supabase_cache.cache_get(key) as L3 fallback.
      In _set_cached(), after disk write, call
        supabase_cache.cache_set(key, data) to persist to L3.
      Or use supabase_cache.get_or_set(key, fetch_fn) for a single call.
    - app.py: call supabase_cache.start_cleanup_thread() at server startup.
    - monitoring.py: include supabase_cache.get_supabase_stats() in
        health_check_readiness() checks dict.

Stdlib-only, thread-safe.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_SUPABASE_URL: Optional[str] = os.environ.get("SUPABASE_URL", "").strip() or None
_SUPABASE_ANON_KEY: Optional[str] = os.environ.get("SUPABASE_ANON_KEY", "").strip() or None

# Module is enabled only when both env vars are set.
_ENABLED: bool = bool(_SUPABASE_URL and _SUPABASE_ANON_KEY)

# HTTP timeout for cache operations (seconds). Cache should never block the
# main pipeline, so we keep this tight.
_HTTP_TIMEOUT = 3

# Retry configuration for transient errors (5xx).
_MAX_RETRIES = 1
_RETRY_DELAY = 0.5  # seconds

# Default TTL for cache entries.
DEFAULT_TTL = 86400  # 24 hours in seconds

# SSL context for Supabase HTTPS calls.
_SSL_CTX = ssl.create_default_context()

# Table name in Supabase.
_TABLE = "cache"


# ---------------------------------------------------------------------------
# Local stats tracking (thread-safe)
# ---------------------------------------------------------------------------

_stats_lock = threading.Lock()
_stats: Dict[str, Any] = {
    "hits": 0,
    "misses": 0,
    "writes": 0,
    "deletes": 0,
    "errors": 0,
    "batch_gets": 0,
    "batch_sets": 0,
    "cleanups": 0,
    "cleanup_deleted": 0,
    "last_cleanup_time": None,
    "last_error": None,
    "last_error_time": None,
}


def _stat_inc(key: str, amount: int = 1) -> None:
    """Increment a stat counter. Thread-safe."""
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount


def _stat_set(key: str, value: Any) -> None:
    """Set a stat value. Thread-safe."""
    with _stats_lock:
        _stats[key] = value


def _record_error(msg: str) -> None:
    """Record an error in stats and log it."""
    _stat_inc("errors")
    _stat_set("last_error", msg)
    _stat_set("last_error_time", _now_iso())
    try:
        logger.warning("[supabase_cache] %s", msg)
    except Exception:
        pass


def _log_info(msg: str) -> None:
    """Log an info message. Never crashes."""
    try:
        logger.info("[supabase_cache] %s", msg)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Current UTC time as ISO 8601 string with timezone."""
    return datetime.now(timezone.utc).isoformat()


def _expires_iso(ttl_seconds: int) -> str:
    """UTC expiration time as ISO 8601 string."""
    return (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()


def _is_expired(expires_at_str: str) -> bool:
    """Check if an ISO 8601 timestamp is in the past.

    Handles both timezone-aware and naive timestamps. Returns True if the
    timestamp cannot be parsed (treat unparseable as expired).
    """
    try:
        # Python 3.7+ fromisoformat handles basic ISO formats.
        # Supabase returns timestamps like '2024-01-15T10:30:00+00:00'.
        exp = datetime.fromisoformat(expires_at_str)
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) >= exp
    except (ValueError, TypeError):
        return True


# ---------------------------------------------------------------------------
# HTTP transport layer
# ---------------------------------------------------------------------------

def _build_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Build standard Supabase REST API headers."""
    headers = {
        "apikey": _SUPABASE_ANON_KEY or "",
        "Authorization": f"Bearer {_SUPABASE_ANON_KEY or ''}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


def _rest_url(path: str) -> str:
    """Build full REST URL for the given path (relative to /rest/v1/)."""
    base = (_SUPABASE_URL or "").rstrip("/")
    return f"{base}/rest/v1/{path}"


def _http_request(
    url: str,
    method: str = "GET",
    body: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[Any]:
    """Execute an HTTP request with retry on 5xx errors.

    Returns parsed JSON on success, None on any failure.
    Never raises exceptions.

    Args:
        url: Full URL to request.
        method: HTTP method (GET, POST, PATCH, DELETE).
        body: Request body bytes (for POST/PATCH).
        headers: HTTP headers dict.

    Returns:
        Parsed JSON response or None on failure.
    """
    if not _ENABLED:
        return None

    req_headers = headers or _build_headers()

    for attempt in range(_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, data=body, method=method, headers=req_headers
            )
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                raw = resp.read().decode("utf-8")
                if not raw or not raw.strip():
                    return None
                return json.loads(raw)

        except urllib.error.HTTPError as exc:
            # Retry on 5xx (server error) if we have retries left.
            if 500 <= exc.code < 600 and attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY)
                continue
            # Read error body for diagnostics.
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            _record_error(
                f"HTTP {exc.code} {method} {url}: {error_body}"
            )
            return None

        except urllib.error.URLError as exc:
            _record_error(f"URLError {method} {url}: {exc.reason}")
            return None

        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            _record_error(f"Decode error {method} {url}: {exc}")
            return None

        except Exception as exc:
            _record_error(f"Unexpected error {method} {url}: {exc}")
            return None

    return None


def _http_request_status(
    url: str,
    method: str = "DELETE",
    body: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[int]:
    """Execute an HTTP request and return the status code.

    Used for DELETE/PATCH operations where the response body may be empty
    but we need to know the status code.

    Returns:
        HTTP status code on success, None on any failure.
    """
    if not _ENABLED:
        return None

    req_headers = headers or _build_headers()

    for attempt in range(_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, data=body, method=method, headers=req_headers
            )
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                # Consume body to release connection.
                resp.read()
                return resp.status

        except urllib.error.HTTPError as exc:
            if 500 <= exc.code < 600 and attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY)
                continue
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            _record_error(
                f"HTTP {exc.code} {method} {url}: {error_body}"
            )
            return exc.code

        except Exception as exc:
            _record_error(f"Error {method} {url}: {exc}")
            return None

    return None


# ---------------------------------------------------------------------------
# Core cache operations
# ---------------------------------------------------------------------------

def cache_get(key: str) -> Optional[Any]:
    """Fetch a value from the Supabase cache.

    Checks that the entry exists and has not expired. On a valid hit,
    increments hit_count in the background (fire-and-forget) to track
    access frequency.

    Args:
        key: Cache key string.

    Returns:
        The cached data (deserialized from JSONB) on hit, or None on miss,
        expiry, or if the module is disabled.
    """
    if not _ENABLED:
        return None

    encoded_key = urllib.parse.quote(key, safe="")
    url = _rest_url(
        f"{_TABLE}?key=eq.{encoded_key}&select=data,expires_at"
    )

    result = _http_request(url, method="GET")

    if not result or not isinstance(result, list) or len(result) == 0:
        _stat_inc("misses")
        return None

    entry = result[0]

    # Check expiration.
    expires_at = entry.get("expires_at")
    if expires_at and _is_expired(expires_at):
        _stat_inc("misses")
        return None

    # Valid hit -- increment hit_count in background.
    _stat_inc("hits")
    _fire_and_forget_hit_increment(key)

    return entry.get("data")


def _fire_and_forget_hit_increment(key: str) -> None:
    """Increment hit_count for a cache key in a background thread.

    This is a non-blocking operation. If it fails, we silently ignore the
    error -- hit tracking is nice-to-have, not critical.
    """
    def _increment():
        try:
            encoded_key = urllib.parse.quote(key, safe="")
            url = _rest_url(f"{_TABLE}?key=eq.{encoded_key}")
            headers = _build_headers({"Prefer": "return=minimal"})
            # PostgREST supports raw SQL in headers for computed updates,
            # but the simplest approach is to read + write. However, for
            # fire-and-forget we use the RPC-style increment.
            # PostgREST PATCH with a computed column isn't directly supported,
            # so we use a simple approach: fetch current, increment, write back.
            # This has a minor race condition on hit_count, which is acceptable
            # for analytics-only data.
            fetch_url = _rest_url(
                f"{_TABLE}?key=eq.{encoded_key}&select=hit_count"
            )
            result = _http_request(fetch_url, method="GET")
            if result and isinstance(result, list) and len(result) > 0:
                current = result[0].get("hit_count", 0) or 0
                body = json.dumps({"hit_count": current + 1}).encode("utf-8")
                _http_request(url, method="PATCH", body=body, headers=headers)
        except Exception:
            pass  # fire-and-forget: silently ignore errors

    t = threading.Thread(target=_increment, daemon=True)
    t.start()


def cache_set(
    key: str,
    data: Any,
    ttl_seconds: int = DEFAULT_TTL,
    category: str = "general",
) -> bool:
    """Store a value in the Supabase cache with a TTL.

    Uses PostgREST upsert (Prefer: resolution=merge-duplicates) to insert
    or update the entry atomically.

    Args:
        key: Cache key string.
        data: Data to cache (must be JSON-serializable).
        ttl_seconds: Time-to-live in seconds (default: 86400 = 24h).
        category: Category tag for grouping/filtering (default: "general").

    Returns:
        True on success, False on failure or if the module is disabled.
    """
    if not _ENABLED:
        return False

    url = _rest_url(_TABLE)
    headers = _build_headers({
        "Prefer": "resolution=merge-duplicates,return=minimal",
    })

    payload = {
        "key": key,
        "data": data,
        "created_at": _now_iso(),
        "expires_at": _expires_iso(ttl_seconds),
        "category": category,
        "hit_count": 0,
    }

    try:
        body = json.dumps(payload).encode("utf-8")
    except (TypeError, ValueError) as exc:
        _record_error(f"JSON encode failed for key '{key}': {exc}")
        return False

    # Use _http_request_status because return=minimal yields an empty body.
    # A 2xx status confirms the upsert succeeded.
    status = _http_request_status(
        url, method="POST", body=body, headers=headers
    )

    if status is not None and 200 <= status < 300:
        _stat_inc("writes")
        return True

    return False


def cache_delete(key: str) -> bool:
    """Delete a cache entry by key.

    Args:
        key: Cache key to delete.

    Returns:
        True if the request succeeded (or module is disabled), False on HTTP error.
    """
    if not _ENABLED:
        return False

    encoded_key = urllib.parse.quote(key, safe="")
    url = _rest_url(f"{_TABLE}?key=eq.{encoded_key}")
    headers = _build_headers({"Prefer": "return=minimal"})

    status = _http_request_status(url, method="DELETE", headers=headers)

    if status is not None and 200 <= status < 300:
        _stat_inc("deletes")
        return True

    return False


def cache_cleanup() -> int:
    """Delete all expired cache entries.

    Should be called periodically (e.g., every 6 hours) to prevent the
    cache table from growing unbounded.

    Returns:
        Number of entries deleted, or 0 on failure/disabled.
    """
    if not _ENABLED:
        return 0

    now_iso = _now_iso()
    encoded_now = urllib.parse.quote(now_iso, safe="")
    url = _rest_url(f"{_TABLE}?expires_at=lt.{encoded_now}")

    # Use Prefer: return=representation to get deleted rows back so we
    # can count them.
    headers = _build_headers({
        "Prefer": "return=representation",
    })

    result = _http_request(url, method="DELETE", headers=headers)

    count = 0
    if isinstance(result, list):
        count = len(result)

    _stat_inc("cleanups")
    _stat_inc("cleanup_deleted", count)
    _stat_set("last_cleanup_time", now_iso)
    _log_info(f"Cache cleanup: deleted {count} expired entries")

    return count


def cache_stats() -> Dict[str, Any]:
    """Return cache statistics from Supabase.

    Queries the cache table for total entries, expired count, and
    per-category breakdown.

    Returns:
        Dict with keys: total, expired, by_category, enabled.
        Returns {"enabled": False} if module is disabled.
    """
    if not _ENABLED:
        return {"enabled": False}

    result: Dict[str, Any] = {"enabled": True}

    # Total entries.
    url_total = _rest_url(
        f"{_TABLE}?select=key&limit=0"
    )
    # PostgREST supports count via Prefer: count=exact header.
    headers_count = _build_headers({"Prefer": "count=exact"})

    try:
        req = urllib.request.Request(
            url_total, method="HEAD", headers=headers_count
        )
        with urllib.request.urlopen(
            req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
        ) as resp:
            # PostgREST returns count in Content-Range header:
            # "0-N/total" or "*/total" for HEAD.
            content_range = resp.headers.get("Content-Range", "")
            resp.read()  # consume body
            if "/" in content_range:
                total_str = content_range.split("/")[-1]
                result["total"] = int(total_str) if total_str != "*" else -1
            else:
                result["total"] = -1
    except Exception as exc:
        result["total"] = -1
        result["total_error"] = str(exc)

    # Expired entries count.
    now_iso = _now_iso()
    encoded_now = urllib.parse.quote(now_iso, safe="")
    url_expired = _rest_url(
        f"{_TABLE}?expires_at=lt.{encoded_now}&select=key&limit=0"
    )
    try:
        req = urllib.request.Request(
            url_expired, method="HEAD", headers=headers_count
        )
        with urllib.request.urlopen(
            req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
        ) as resp:
            content_range = resp.headers.get("Content-Range", "")
            resp.read()
            if "/" in content_range:
                expired_str = content_range.split("/")[-1]
                result["expired"] = int(expired_str) if expired_str != "*" else -1
            else:
                result["expired"] = -1
    except Exception as exc:
        result["expired"] = -1
        result["expired_error"] = str(exc)

    # Per-category breakdown.
    url_cats = _rest_url(
        f"{_TABLE}?select=category"
    )
    cat_data = _http_request(url_cats, method="GET")
    if isinstance(cat_data, list):
        by_category: Dict[str, int] = {}
        for row in cat_data:
            cat = row.get("category", "general")
            by_category[cat] = by_category.get(cat, 0) + 1
        result["by_category"] = by_category
    else:
        result["by_category"] = {}

    return result


# ---------------------------------------------------------------------------
# Batch operations
# ---------------------------------------------------------------------------

def cache_get_many(keys: List[str]) -> Dict[str, Any]:
    """Fetch multiple cache entries in a single request.

    Uses PostgREST's ``in`` filter to fetch all matching keys at once.
    Filters out expired entries client-side.

    Args:
        keys: List of cache key strings to fetch.

    Returns:
        Dict mapping key -> data for valid (non-expired) hits.
        Returns empty dict if disabled or on failure.
    """
    if not _ENABLED or not keys:
        return {}

    # PostgREST in-filter syntax: key=in.(val1,val2,val3)
    # Keys are quoted to handle special characters.
    quoted_keys = ",".join(
        f'"{k}"' for k in keys
    )
    url = _rest_url(
        f"{_TABLE}?key=in.({quoted_keys})&select=key,data,expires_at"
    )

    result = _http_request(url, method="GET")
    _stat_inc("batch_gets")

    if not result or not isinstance(result, list):
        # Fallback to individual gets on batch failure
        fallback: Dict[str, Any] = {}
        for key in keys:
            val = cache_get(key)
            if val is not None:
                fallback[key] = val
        return fallback

    hits: Dict[str, Any] = {}
    for entry in result:
        key = entry.get("key")
        expires_at = entry.get("expires_at")
        if key and expires_at and not _is_expired(expires_at):
            hits[key] = entry.get("data")
            _stat_inc("hits")
            # Update hit count (best effort, fire-and-forget)
            try:
                _fire_and_forget_hit_increment(key)
            except Exception:
                pass
        else:
            _stat_inc("misses")

    return hits


def cache_set_many(entries: List[Dict[str, Any]]) -> int:
    """Store multiple cache entries in a single request.

    Uses PostgREST upsert to insert or update all entries atomically.

    Args:
        entries: List of dicts, each with keys:
            - key (str, required): Cache key.
            - data (Any, required): Data to cache.
            - ttl (int, optional): TTL in seconds (default: DEFAULT_TTL).
            - category (str, optional): Category tag (default: "general").

    Returns:
        Number of entries successfully stored, or 0 on failure/disabled.
    """
    if not _ENABLED or not entries:
        return 0

    url = _rest_url(_TABLE)
    headers = _build_headers({
        "Prefer": "resolution=merge-duplicates,return=minimal",
    })

    now = _now_iso()
    rows = []
    for entry in entries:
        key = entry.get("key")
        data = entry.get("data")
        if key is None or data is None:
            continue
        ttl = entry.get("ttl", DEFAULT_TTL)
        category = entry.get("category", "general")
        rows.append({
            "key": key,
            "data": data,
            "created_at": now,
            "expires_at": _expires_iso(ttl),
            "category": category,
            "hit_count": 0,
        })

    if not rows:
        return 0

    try:
        body = json.dumps(rows).encode("utf-8")
    except (TypeError, ValueError) as exc:
        _record_error(f"JSON encode failed for batch set: {exc}")
        return 0

    # Use _http_request_status because return=minimal yields an empty body.
    status = _http_request_status(
        url, method="POST", body=body, headers=headers
    )

    if status is not None and 200 <= status < 300:
        count = len(rows)
        _stat_inc("writes", count)
        _stat_inc("batch_sets")
        return count

    return 0


# ---------------------------------------------------------------------------
# Integration helper
# ---------------------------------------------------------------------------

def get_or_set(
    key: str,
    fetch_fn: Callable[[], Any],
    ttl_seconds: int = DEFAULT_TTL,
    category: str = "api",
) -> Any:
    """Try the Supabase cache first; on miss, call fetch_fn and cache the result.

    This is the primary integration point for api_enrichment.py. Use it to
    wrap any expensive API call with Supabase-backed caching.

    Example usage in api_enrichment.py::

        # Instead of:
        result = _expensive_api_call(params)

        # Use:
        result = supabase_cache.get_or_set(
            key=_cache_key("bls_oes", f"{soc_code}:{area_code}"),
            fetch_fn=lambda: _expensive_api_call(params),
            ttl_seconds=86400,
            category="bls",
        )

    Args:
        key: Cache key string.
        fetch_fn: Zero-argument callable that fetches the data on cache miss.
        ttl_seconds: TTL for the cached entry (default: 86400 = 24h).
        category: Category tag (default: "api").

    Returns:
        Cached data on hit, or the result of fetch_fn() on miss.
        Returns None if both cache and fetch_fn fail.
    """
    # Try cache first.
    cached = cache_get(key)
    if cached is not None:
        return cached

    # Cache miss -- call the fetch function.
    try:
        data = fetch_fn()
    except Exception as exc:
        _record_error(f"fetch_fn failed for key '{key}': {exc}")
        return None

    # Cache the result if we got something.
    if data is not None:
        cache_set(key, data, ttl_seconds=ttl_seconds, category=category)

    return data


# ---------------------------------------------------------------------------
# Local stats (in-process counters, not Supabase table stats)
# ---------------------------------------------------------------------------

def get_supabase_stats() -> Dict[str, Any]:
    """Return local in-process stats for Supabase cache operations.

    Includes hit/miss/write/error counters and timing info. This does NOT
    query Supabase; it returns counters tracked in this process.

    For remote table stats (total rows, expired count, categories), use
    cache_stats() instead.

    Returns:
        Dict with keys: enabled, hits, misses, writes, errors, etc.
    """
    with _stats_lock:
        snapshot = dict(_stats)

    snapshot["enabled"] = _ENABLED
    snapshot["supabase_url"] = (
        _SUPABASE_URL[:30] + "..." if _SUPABASE_URL and len(_SUPABASE_URL) > 30
        else _SUPABASE_URL
    )

    # Compute hit rate.
    total_lookups = snapshot.get("hits", 0) + snapshot.get("misses", 0)
    if total_lookups > 0:
        snapshot["hit_rate"] = round(snapshot["hits"] / total_lookups, 4)
    else:
        snapshot["hit_rate"] = 0.0

    return snapshot


# ---------------------------------------------------------------------------
# Periodic cleanup thread
# ---------------------------------------------------------------------------

_cleanup_thread: Optional[threading.Thread] = None
_cleanup_stop_event = threading.Event()


def start_cleanup_thread(interval_hours: int = 6) -> Optional[threading.Thread]:
    """Start a daemon thread that periodically cleans up expired cache entries.

    The thread runs cache_cleanup() every ``interval_hours`` hours. It is a
    daemon thread and will automatically stop when the main thread exits.

    Calling this multiple times is safe: subsequent calls are no-ops if the
    cleanup thread is already running.

    Args:
        interval_hours: Hours between cleanup runs (default: 6).

    Returns:
        The cleanup thread, or None if the module is disabled.
    """
    global _cleanup_thread

    if not _ENABLED:
        _log_info("Supabase cache disabled (missing env vars), skipping cleanup thread")
        return None

    if _cleanup_thread is not None and _cleanup_thread.is_alive():
        _log_info("Cleanup thread already running")
        return _cleanup_thread

    interval_seconds = interval_hours * 3600

    def _cleanup_loop():
        _log_info(
            f"Cleanup thread started (interval: {interval_hours}h)"
        )
        while not _cleanup_stop_event.is_set():
            try:
                deleted = cache_cleanup()
                _log_info(f"Periodic cleanup complete: {deleted} entries removed")
            except Exception as exc:
                _record_error(f"Cleanup thread error: {exc}")

            # Wait for the interval, but wake up immediately if stop is signaled.
            _cleanup_stop_event.wait(timeout=interval_seconds)

        _log_info("Cleanup thread stopped")

    _cleanup_stop_event.clear()
    _cleanup_thread = threading.Thread(
        target=_cleanup_loop,
        name="supabase-cache-cleanup",
        daemon=True,
    )
    _cleanup_thread.start()
    return _cleanup_thread


def stop_cleanup_thread() -> None:
    """Signal the cleanup thread to stop.

    The thread will finish its current sleep/cleanup cycle and then exit.
    This is primarily useful for testing.
    """
    global _cleanup_thread
    _cleanup_stop_event.set()
    if _cleanup_thread is not None:
        _cleanup_thread.join(timeout=5)
        _cleanup_thread = None


# ---------------------------------------------------------------------------
# Module initialization
# ---------------------------------------------------------------------------

if _ENABLED:
    _log_info(
        f"Supabase cache enabled: {_SUPABASE_URL[:40]}..."
        if _SUPABASE_URL and len(_SUPABASE_URL) > 40
        else f"Supabase cache enabled: {_SUPABASE_URL}"
    )
else:
    _log_info(
        "Supabase cache disabled: SUPABASE_URL or SUPABASE_ANON_KEY not set"
    )
