#!/usr/bin/env python3
"""WSGI adapter for the stdlib BaseHTTPRequestHandler-based app.

This module wraps the MediaPlanHandler (http.server.BaseHTTPRequestHandler subclass)
into a WSGI-compatible application so it can be served by gunicorn in production
while keeping the ThreadedHTTPServer for local development.

Architecture:
  - The handler writes HTTP responses to self.wfile (status line + headers + body).
  - For SSE/streaming endpoints, the handler calls wfile.write() + wfile.flush()
    incrementally. To support this, we use a pipe: the handler writes to the write
    end, and the WSGI response iterator reads from the read end.
  - The handler runs in a background thread so the WSGI iterator can yield chunks
    as they arrive (true streaming).

Key design decisions:
  - SSE/streaming: gunicorn --timeout 120 handles long-running SSE streams.
  - Concurrency: gunicorn --worker-class gevent uses greenlets for cooperative
    multitasking. monkey.patch_all() replaces stdlib threading/socket/select with
    gevent-compatible equivalents so all locks and threads work transparently.
  - Multipart uploads: self.rfile is set to a BytesIO with the request body,
    so cgi.FieldStorage works unchanged.
  - --preload: gunicorn preloads the app module, sharing the knowledge base data
    across forked workers (copy-on-write memory savings).
"""

from __future__ import annotations

# ---- gevent monkey-patching MUST happen before other stdlib imports ----
from gevent import monkey  # noqa: E402  isort:skip

monkey.patch_all()  # noqa: E402  isort:skip
# ---------------------------------------------------------------------

import collections
import io
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler
from typing import Any, Callable, Iterator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import the handler class and run deferred startup exactly once
# ---------------------------------------------------------------------------
from app import MediaPlanHandler  # noqa: E402

# Deferred startup (knowledge base, vector index, etc.) must run once
# when gunicorn preloads the module. We gate it with a flag so it only
# fires once even if multiple workers import this module.
_startup_done = False
_startup_lock = threading.Lock()


def _run_deferred_startup() -> None:
    """Run the same deferred startup logic that app.py __main__ runs.

    This mirrors the _deferred_startup() function in app.py's __main__ block.
    It pre-warms the knowledge base, vector index, data refresh pipeline,
    and other background services.
    """
    global _startup_done
    with _startup_lock:
        if _startup_done:
            return
        _startup_done = True

    logger.info(
        "[wsgi] Running deferred startup for gunicorn worker (PID %d)", os.getpid()
    )

    # Pre-warm knowledge base
    try:
        from app import load_knowledge_base

        kb = load_knowledge_base()
        logger.info("[wsgi] Knowledge base pre-warmed: %d keys", len(kb))
    except Exception as kb_err:
        logger.warning("[wsgi] Knowledge base pre-warm failed: %s", kb_err)

    # Build vector search index
    try:
        from app import _vector_search_available, _vector_index_kb

        if _vector_search_available and _vector_index_kb:
            count = _vector_index_kb()
            logger.info("[wsgi] Vector search index built: %d documents", count)
    except Exception as ve:
        logger.warning("[wsgi] Vector index build failed: %s", ve)

    # Data Refresh Pipeline
    try:
        from data_refresh import start_data_refresh

        start_data_refresh()
        logger.info("[wsgi] Data refresh pipeline started")
    except ImportError:
        pass
    except Exception as e:
        logger.warning("[wsgi] Data refresh failed: %s", e)

    # Proactive Health Checker
    try:
        from sentry_integration import start_proactive_health as _start_proactive_health

        _start_proactive_health()
        logger.info("[wsgi] Proactive health checker started")
    except ImportError:
        pass

    # Proactive Intelligence Engine
    try:
        from nova_proactive import start_proactive_engine

        start_proactive_engine()
        logger.info("[wsgi] Proactive intelligence engine started")
    except ImportError:
        pass

    # Feature Store Init
    try:
        from feature_store import get_feature_store

        get_feature_store().initialize()
        logger.info("[wsgi] Feature store initialized")
    except ImportError:
        pass
    except Exception as _fs_err:
        logger.warning("[wsgi] Feature store init failed: %s", _fs_err)

    # API Key Authentication Init
    try:
        from auth import init as _init_auth

        _init_auth()
    except ImportError:
        pass

    # Preload modules used in health checks to avoid lazy import delays
    try:
        import vector_search as _vs_preload

        logger.info("[wsgi] Vector search preloaded")
    except ImportError:
        logger.debug("[wsgi] Vector search not available")

    try:
        import slack_alerts as _sa_preload

        logger.info("[wsgi] Slack alerts preloaded")
    except ImportError:
        logger.debug("[wsgi] Slack alerts not available")

    try:
        import calendar_sync as _cs_preload

        logger.info("[wsgi] Calendar sync preloaded")
    except ImportError:
        logger.debug("[wsgi] Calendar sync not available")

    try:
        import chroma_rag as _cr_preload

        logger.info("[wsgi] Chroma RAG preloaded")
    except ImportError:
        logger.debug("[wsgi] Chroma RAG not available")

    # Mark deploy warmup as complete
    try:
        import app as _app_module

        _app_module._DEPLOY_WARMUP_COMPLETE = True
        logger.info("[wsgi] Deploy warmup marked READY")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Fake socket / server to satisfy BaseHTTPRequestHandler
# ---------------------------------------------------------------------------
class _FakeSocket:
    """Minimal socket stand-in so BaseHTTPRequestHandler can call makefile()."""

    def makefile(self, mode: str = "rb", buffering: int = -1) -> io.BytesIO:
        """Return an empty BytesIO; rfile/wfile are overridden before use."""
        return io.BytesIO()

    def getpeername(self) -> tuple[str, int]:
        """Return a dummy peer address."""
        return ("127.0.0.1", 0)


class _FakeServer:
    """Minimal server stand-in providing server_name and server_port."""

    def __init__(self) -> None:
        self.server_name = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "localhost")
        self.server_port = int(os.environ.get("PORT", "10000"))


# ---------------------------------------------------------------------------
# Streaming-capable wfile wrapper
# ---------------------------------------------------------------------------
class _StreamingWfile:
    """A wfile replacement that captures the HTTP response and supports streaming.

    When the handler calls flush(), the accumulated data is transferred to a
    queue that the WSGI iterator reads from. This enables SSE/streaming without
    buffering the entire response.
    """

    def __init__(self) -> None:
        self._buffer = io.BytesIO()
        self._chunks: collections.deque[bytes] = collections.deque()
        self._chunk_event = threading.Event()
        self._done = False
        self._lock = threading.Lock()
        # First write contains the HTTP status + headers; we need to parse them
        self._headers_parsed = False
        self._status_line: str = "200 OK"
        self._headers: list[tuple[str, str]] = []
        self._body_prefix: bytes = b""

    def write(self, data: bytes) -> int:
        """Buffer data. Chunks are released on flush() or when done."""
        with self._lock:
            self._buffer.write(data)
        return len(data)

    def flush(self) -> None:
        """Release buffered data as a chunk for the WSGI iterator."""
        with self._lock:
            data = self._buffer.getvalue()
            if data:
                self._buffer = io.BytesIO()
                self._chunks.append(data)
                self._chunk_event.set()

    def close_stream(self) -> None:
        """Signal that the handler is done writing."""
        self.flush()  # flush any remaining data
        with self._lock:
            self._done = True
            self._chunk_event.set()

    def get_chunk(self, timeout: float = 1.0) -> bytes | None:
        """Get next chunk, or None if done. Blocks until data or timeout."""
        while True:
            with self._lock:
                if self._chunks:
                    return self._chunks.popleft()
                if self._done:
                    return None
                self._chunk_event.clear()
            self._chunk_event.wait(timeout=timeout)
            with self._lock:
                if self._chunks:
                    return self._chunks.popleft()
                if self._done:
                    return None


# ---------------------------------------------------------------------------
# Handler adapter (skip auto-handling in __init__)
# ---------------------------------------------------------------------------
class _WSGIHandlerAdapter(MediaPlanHandler):
    """Subclass that suppresses __init__ auto-handling.

    BaseHTTPRequestHandler.__init__ calls handle() -> handle_one_request()
    immediately. We skip that and call do_GET/do_POST manually.
    """

    def __init__(self) -> None:
        # Intentionally skip BaseHTTPRequestHandler.__init__
        self.server = _FakeServer()
        self.client_address = ("127.0.0.1", 0)
        self.close_connection = True
        self._headers_buffer: list[bytes] = []

    def setup(self) -> None:
        """No-op; rfile/wfile are injected by the WSGI app function."""
        pass

    def finish(self) -> None:
        """No-op; cleanup handled by the WSGI app function."""
        pass


# ---------------------------------------------------------------------------
# HTTP response parser
# ---------------------------------------------------------------------------
def _parse_raw_headers(raw: bytes) -> tuple[str, list[tuple[str, str]], bytes]:
    """Parse raw HTTP response bytes into (status_line, headers, remaining_body).

    Args:
        raw: Raw HTTP response bytes (status line + headers + possibly body start).

    Returns:
        Tuple of (status_string, header_list, remaining_body_bytes).
    """
    if not raw:
        return "200 OK", [], b""

    # Split headers from body at the first \r\n\r\n
    header_end = raw.find(b"\r\n\r\n")
    if header_end == -1:
        # No complete header block yet -- might be partial
        # Try to parse what we have
        header_section = raw.decode("latin-1", errors="replace")
        body = b""
    else:
        header_section = raw[:header_end].decode("latin-1", errors="replace")
        body = raw[header_end + 4 :]

    lines = header_section.split("\r\n")

    # First line is the status line: "HTTP/1.1 200 OK"
    status_line = "200 OK"
    if lines and lines[0].startswith("HTTP/"):
        parts = lines[0].split(" ", 2)
        if len(parts) >= 3:
            status_line = f"{parts[1]} {parts[2]}"
        elif len(parts) == 2:
            status_line = f"{parts[1]} OK"
        lines = lines[1:]

    # Parse headers
    headers: list[tuple[str, str]] = []
    for line in lines:
        if ":" in line:
            key, _, value = line.partition(":")
            # Skip hop-by-hop headers that WSGI servers manage themselves
            key_lower = key.strip().lower()
            if key_lower in ("transfer-encoding", "connection"):
                continue
            headers.append((key.strip(), value.strip()))

    return status_line, headers, body


# ---------------------------------------------------------------------------
# WSGI application
# ---------------------------------------------------------------------------
def application(
    environ: dict[str, Any],
    start_response: Callable[..., Any],
) -> Iterator[bytes]:
    """PEP-3333 WSGI application wrapping MediaPlanHandler.

    For each request:
    1. Create a handler instance with rfile/wfile set up from WSGI environ.
    2. Run the handler in a thread so SSE streaming can yield chunks.
    3. Parse the HTTP response (status + headers) from the first chunk.
    4. Yield body chunks as they arrive.

    Args:
        environ: WSGI environment dictionary.
        start_response: WSGI start_response callable.

    Returns:
        Iterator of response body bytes.
    """
    # Fast-path for health ping -- skip full WSGI pipeline (~350ms saving)
    # NOTE: This function is a generator (yield below), so we must use
    # yield+return, not return [b'...']. In a generator, return [x] raises
    # StopIteration(value=[x]) and the value is silently lost.
    if environ.get("PATH_INFO", "") == "/api/health/ping":
        start_response(
            "200 OK",
            [
                ("Content-Type", "application/json"),
                ("Cache-Control", "no-cache"),
            ],
        )
        yield b'{"status":"ok","service":"nova-ai"}'
        return

    handler = _WSGIHandlerAdapter()

    # -- Build the HTTP request line + headers for parse_request() --
    method = environ.get("REQUEST_METHOD", "GET")
    path = environ.get("PATH_INFO", "/")
    query = environ.get("QUERY_STRING", "")
    if query:
        path = f"{path}?{query}"

    # Construct raw HTTP/1.1 request line
    request_line = f"{method} {path} HTTP/1.1\r\n"

    # Collect headers from WSGI environ
    header_lines: list[str] = []
    if environ.get("CONTENT_TYPE"):
        header_lines.append(f"Content-Type: {environ['CONTENT_TYPE']}")
    if environ.get("CONTENT_LENGTH"):
        header_lines.append(f"Content-Length: {environ['CONTENT_LENGTH']}")

    # HTTP_* headers
    for key, value in environ.items():
        if key.startswith("HTTP_"):
            # Convert HTTP_ACCEPT_ENCODING -> Accept-Encoding
            header_name = key[5:].replace("_", "-").title()
            header_lines.append(f"{header_name}: {value}")

    raw_headers = "\r\n".join(header_lines)
    raw_request_head = f"{request_line}{raw_headers}\r\n\r\n"

    # -- Set up rfile (request body) --
    wsgi_input = environ.get("wsgi.input")
    body_data = b""
    if wsgi_input:
        content_length = environ.get("CONTENT_LENGTH")
        if content_length:
            try:
                body_data = wsgi_input.read(int(content_length))
            except (ValueError, IOError):
                body_data = wsgi_input.read()
        else:
            body_data = wsgi_input.read()

    # rfile = headers + body (parse_request reads headers, handler reads body)
    # NOTE: Do NOT include the request line -- parse_request() already has it
    # from raw_requestline and reads headers starting at rfile position 0.
    headers_and_body = f"{raw_headers}\r\n\r\n".encode("latin-1") + body_data
    handler.rfile = io.BytesIO(headers_and_body)

    # -- Set up wfile (streaming response capture) --
    streaming_wfile = _StreamingWfile()
    handler.wfile = streaming_wfile

    # -- Parse the request line + headers --
    handler.raw_requestline = request_line.encode("latin-1")
    if not handler.parse_request():
        start_response("400 Bad Request", [("Content-Type", "text/plain")])
        yield b"Bad Request"
        return

    # -- Set client address from WSGI environ for logging --
    remote_addr = environ.get("REMOTE_ADDR", "127.0.0.1")
    handler.client_address = (remote_addr, 0)

    # -- Dispatch in a thread so we can stream the response --
    handler_error: list[Exception] = []

    def _run_handler() -> None:
        method_name = f"do_{method}"
        handler_method = getattr(handler, method_name, None)
        if handler_method is None:
            handler.send_error(405, "Method Not Allowed")
            streaming_wfile.close_stream()
            return
        try:
            handler_method()
        except BrokenPipeError:
            logger.debug("Client disconnected during %s %s", method, path)
        except Exception as exc:
            handler_error.append(exc)
            logger.error(
                "Unhandled exception in WSGI handler: %s %s",
                method,
                path,
                exc_info=True,
            )
            # Try to send a 500 if headers haven't been sent yet
            try:
                handler.send_error(500, "Internal Server Error")
            except Exception:
                pass
        finally:
            streaming_wfile.close_stream()

    handler_thread = threading.Thread(
        target=_run_handler, daemon=True, name=f"wsgi-handler-{method}-{path[:50]}"
    )
    handler_thread.start()

    # -- Read the first chunk to extract HTTP status + headers --
    first_chunk = streaming_wfile.get_chunk(
        timeout=115.0
    )  # 115s -- safely under gunicorn 120s timeout

    if first_chunk is None:
        # Handler produced no output
        if handler_error:
            start_response(
                "500 Internal Server Error", [("Content-Type", "text/plain")]
            )
            yield b"Internal Server Error"
            return
        start_response("204 No Content", [])
        return

    # Parse HTTP response headers from the first chunk
    status_line, headers, body_remainder = _parse_raw_headers(first_chunk)

    start_response(status_line, headers)

    # Yield the body portion from the first chunk
    if body_remainder:
        yield body_remainder

    # Yield subsequent chunks (streaming/SSE data)
    while True:
        chunk = streaming_wfile.get_chunk(timeout=130.0)
        if chunk is None:
            break
        yield chunk

    # Wait for handler thread to finish (should already be done)
    handler_thread.join(timeout=5.0)


# ---------------------------------------------------------------------------
# Module-level init: run deferred startup when gunicorn --preload imports this
# ---------------------------------------------------------------------------
_run_deferred_startup()

# Alias for gunicorn: `gunicorn wsgi:app`
app = application
