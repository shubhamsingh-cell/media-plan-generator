"""
http_pool.py -- Thread-safe HTTPS connection pool using stdlib only.

Replaces per-request urllib.request.urlopen() calls (each creating a new
TCP + TLS handshake) with persistent keep-alive connections pooled by host.

Typical savings: ~100-200ms per call when the connection already exists.
With 30+ API calls in the enrichment pipeline, this is significant.

Architecture:
    - Dict of {host: deque[HTTPSConnection]} guarded by a threading.Lock
    - Max 20 total connections, max 4 per host
    - Stale connections (>60s idle) are discarded on checkout
    - All public functions match the urllib.request.urlopen() return contract
    - SSL contexts are passed through (verified / unverified)
"""

from __future__ import annotations

import http.client
import io
import logging
import ssl
import threading
import time
import urllib.parse
from collections import deque
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pool configuration
# ---------------------------------------------------------------------------

_MAX_POOL_SIZE: int = 20  # Total connections across all hosts
_MAX_PER_HOST: int = 4  # Max idle connections per host
_IDLE_TIMEOUT: float = 60.0  # Seconds before a connection is considered stale
_CONNECT_TIMEOUT: float = 10.0  # TCP connect timeout

# ---------------------------------------------------------------------------
# Internal pool state (module-level, shared across threads)
# ---------------------------------------------------------------------------

# {host: deque[(connection, last_used_timestamp)]}
_pool: dict[str, deque[tuple[http.client.HTTPSConnection, float]]] = {}
_pool_lock: threading.Lock = threading.Lock()
_pool_total: int = 0  # Current total connections in pool


class _PooledResponse:
    """Thin wrapper so callers that expect .read()/.status work seamlessly."""

    __slots__ = ("status", "reason", "headers", "_data")

    def __init__(
        self, status: int, reason: str, headers: http.client.HTTPMessage, data: bytes
    ) -> None:
        self.status = status
        self.reason = reason
        self.headers = headers
        self._data = data

    def read(self) -> bytes:
        """Return the full response body."""
        return self._data

    def getheader(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Get a single response header value."""
        return self.headers.get(name, default)


# ---------------------------------------------------------------------------
# Pool internals
# ---------------------------------------------------------------------------


def _evict_stale(host: str) -> None:
    """Remove stale connections for a host. Caller must hold _pool_lock."""
    global _pool_total
    if host not in _pool:
        return
    q = _pool[host]
    now = time.monotonic()
    while q:
        conn, ts = q[0]
        if now - ts > _IDLE_TIMEOUT:
            q.popleft()
            _pool_total -= 1
            try:
                conn.close()
            except Exception:
                pass
        else:
            break
    if not q:
        del _pool[host]


def _checkout(
    host: str, port: int, ssl_ctx: ssl.SSLContext
) -> http.client.HTTPSConnection:
    """Get a connection from the pool, or create a new one.

    Args:
        host: Target hostname.
        port: Target port (usually 443).
        ssl_ctx: SSL context to use for new connections.

    Returns:
        An HTTPSConnection ready to use.
    """
    global _pool_total

    with _pool_lock:
        _evict_stale(host)
        if host in _pool and _pool[host]:
            conn, _ts = _pool[host].popleft()
            _pool_total -= 1
            if not _pool[host]:
                del _pool[host]
            # Quick health check -- if the socket is gone, discard
            try:
                if conn.sock is not None:
                    return conn
            except Exception:
                pass
            # Socket was closed, fall through to create new

        # Evict oldest across all hosts if at capacity
        while _pool_total >= _MAX_POOL_SIZE:
            # Find host with oldest entry
            oldest_host = None
            oldest_ts = float("inf")
            for h, q in _pool.items():
                if q and q[0][1] < oldest_ts:
                    oldest_ts = q[0][1]
                    oldest_host = h
            if oldest_host is None:
                break
            old_conn, _ = _pool[oldest_host].popleft()
            _pool_total -= 1
            if not _pool[oldest_host]:
                del _pool[oldest_host]
            try:
                old_conn.close()
            except Exception:
                pass

    # Create new connection outside lock
    conn = http.client.HTTPSConnection(
        host, port=port, context=ssl_ctx, timeout=_CONNECT_TIMEOUT
    )
    return conn


def _checkin(host: str, conn: http.client.HTTPSConnection) -> None:
    """Return a healthy connection to the pool.

    Args:
        host: The host this connection belongs to.
        conn: The connection to return.
    """
    global _pool_total

    with _pool_lock:
        if host not in _pool:
            _pool[host] = deque()
        q = _pool[host]
        if len(q) >= _MAX_PER_HOST or _pool_total >= _MAX_POOL_SIZE:
            # Pool full, just close it
            try:
                conn.close()
            except Exception:
                pass
            return
        q.append((conn, time.monotonic()))
        _pool_total += 1


def _discard(conn: http.client.HTTPSConnection) -> None:
    """Close and discard a connection (don't return to pool)."""
    try:
        conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def pooled_request(
    url: str,
    *,
    method: str = "GET",
    body: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: float = 10.0,
    ssl_ctx: Optional[ssl.SSLContext] = None,
) -> _PooledResponse:
    """Perform an HTTP(S) request using a pooled connection.

    This is the single entry point replacing urllib.request.urlopen().
    Connections are reused for same-host calls (BLS, Census, FRED, etc.).

    Args:
        url: Fully-qualified URL.
        method: HTTP method (GET, POST, etc.).
        body: Optional request body bytes.
        headers: Optional HTTP headers dict.
        timeout: Per-request read timeout in seconds.
        ssl_ctx: SSL context; uses default verified context if None.

    Returns:
        _PooledResponse with .status, .read(), .headers, .getheader().

    Raises:
        http.client.HTTPException: On HTTP-level errors.
        OSError: On network-level errors.
        TimeoutError: On timeout.
    """
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    if ssl_ctx is None:
        ssl_ctx = ssl.create_default_context()

    all_headers = {
        "Host": host,
        "Connection": "keep-alive",
        "Accept-Encoding": "identity",
    }
    if headers:
        all_headers.update(headers)

    conn = _checkout(host, port, ssl_ctx)
    try:
        conn.timeout = timeout
        conn.request(method, path, body=body, headers=all_headers)
        resp = conn.getresponse()

        # Read body fully so the connection can be reused
        data = resp.read()
        status = resp.status
        reason = resp.reason or ""
        resp_headers = resp.headers

        # Only return to pool if server allows keep-alive
        conn_header = (resp.getheader("Connection") or "").lower()
        if conn_header == "close" or status >= 500:
            _discard(conn)
        else:
            _checkin(host, conn)

        return _PooledResponse(status, reason, resp_headers, data)

    except Exception:
        _discard(conn)
        raise


def pool_stats() -> dict[str, Any]:
    """Return current pool statistics for monitoring/debugging.

    Returns:
        Dict with total connections, per-host counts, and pool config.
    """
    with _pool_lock:
        per_host = {h: len(q) for h, q in _pool.items()}
        return {
            "total_connections": _pool_total,
            "per_host": per_host,
            "max_pool_size": _MAX_POOL_SIZE,
            "max_per_host": _MAX_PER_HOST,
            "idle_timeout_s": _IDLE_TIMEOUT,
        }


def drain_pool() -> int:
    """Close all pooled connections. Returns count of connections closed.

    Useful for graceful shutdown or testing.
    """
    global _pool_total
    closed = 0
    with _pool_lock:
        for host, q in _pool.items():
            for conn, _ts in q:
                try:
                    conn.close()
                except Exception:
                    pass
                closed += 1
        _pool.clear()
        _pool_total = 0
    return closed
