"""
grafana_logger.py -- Grafana Cloud Loki logging handler.

Ships structured JSON logs to Grafana Cloud Loki for centralized observability.
Gracefully disabled when GRAFANA_LOKI_URL is not set.
Stdlib-only, thread-safe.

Environment variables:
    GRAFANA_LOKI_URL   -- Loki push base URL (e.g., https://logs-prod-us-central1.grafana.net).
                          Handler is completely disabled (no-op) when unset.
    GRAFANA_API_KEY    -- Grafana Cloud API key (used as Basic auth password).
    GRAFANA_USER_ID    -- Numeric user ID from Grafana Cloud (used as Basic auth username).
                          Defaults to empty string if unset.
    RENDER_ENV         -- Deployment environment label (defaults to "development").

Integration:
    In app.py, after configure_logging() has been called, add::

        try:
            from grafana_logger import setup_grafana_logging
            if setup_grafana_logging(logging.getLogger()):
                logger.info("Grafana Loki logging enabled")
            else:
                logger.debug("Grafana Loki logging not configured (env vars missing)")
        except ImportError:
            pass

    All existing logger.warning(), logger.error(), and logger.critical() calls
    will automatically be shipped to Loki once the handler is attached.  The
    default level threshold is WARNING, so DEBUG/INFO records stay local-only
    unless you lower the level via setup_grafana_logging(level=logging.DEBUG).

Loki push format:
    POST {GRAFANA_LOKI_URL}/loki/api/v1/push
    Authorization: Basic base64(GRAFANA_USER_ID:GRAFANA_API_KEY)
    Content-Type: application/json

    {
      "streams": [
        {
          "stream": {
            "app": "media-plan-generator",
            "env": "production",
            "level": "error"
          },
          "values": [
            ["<unix_nanoseconds_string>", "<log_line_json>"]
          ]
        }
      ]
    }

Dependencies: stdlib only (no new packages).
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_APP_LABEL = "media-plan-generator"
_FLUSH_INTERVAL_SECONDS = 5.0
_FLUSH_THRESHOLD_RECORDS = 50
_MAX_BUFFER_SIZE = 500

# Log level names for Loki stream labels (lowercase for consistency)
_LEVEL_LABEL_MAP = {
    logging.DEBUG: "debug",
    logging.INFO: "info",
    logging.WARNING: "warning",
    logging.ERROR: "error",
    logging.CRITICAL: "critical",
}


# ---------------------------------------------------------------------------
# Stats tracking (module-level, thread-safe)
# ---------------------------------------------------------------------------


class _Stats:
    """Internal counters for shipped/dropped records and flush errors."""

    __slots__ = (
        "_lock",
        "records_shipped",
        "records_dropped",
        "flush_errors",
        "last_flush_time",
        "last_error",
        "last_error_time",
        "last_error_status",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records_shipped: int = 0
        self.records_dropped: int = 0
        self.flush_errors: int = 0
        self.last_flush_time: Optional[float] = None
        self.last_error: Optional[str] = None
        self.last_error_time: Optional[float] = None
        self.last_error_status: Optional[int] = None  # HTTP status code

    def add_shipped(self, count: int) -> None:
        with self._lock:
            self.records_shipped += count

    def add_dropped(self, count: int) -> None:
        with self._lock:
            self.records_dropped += count

    def add_flush_error(
        self, error_msg: str = "", http_status: Optional[int] = None
    ) -> None:
        with self._lock:
            self.flush_errors += 1
            self.last_error = error_msg[:500] if error_msg else None
            self.last_error_time = time.time()
            self.last_error_status = http_status

    def set_last_flush(self) -> None:
        with self._lock:
            self.last_flush_time = time.time()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "records_shipped": self.records_shipped,
                "records_dropped": self.records_dropped,
                "flush_errors": self.flush_errors,
                "last_flush_time": self.last_flush_time,
                "last_flush_iso": (
                    datetime.fromtimestamp(
                        self.last_flush_time, tz=timezone.utc
                    ).isoformat()
                    if self.last_flush_time
                    else None
                ),
                "last_error": self.last_error,
                "last_error_time": self.last_error_time,
                "last_error_iso": (
                    datetime.fromtimestamp(
                        self.last_error_time, tz=timezone.utc
                    ).isoformat()
                    if self.last_error_time
                    else None
                ),
                "last_error_status": self.last_error_status,
            }


_stats = _Stats()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_request_id() -> str:
    """Retrieve the current request ID from monitoring, or empty string.

    Isolated in a helper so that a missing monitoring module does not
    prevent the logger from functioning.
    """
    try:
        from monitoring import get_request_id

        return get_request_id()
    except (ImportError, AttributeError):
        return ""


def _build_auth_header(user_id: str, api_key: str) -> str:
    """Build HTTP Basic auth header value for Grafana Cloud.

    Grafana Cloud Loki accepts Basic auth where:
        username = numeric user ID (or any non-empty string)
        password = API key / token

    Returns the full header value, e.g. "Basic dXNlcjpwYXNz".
    """
    credentials = f"{user_id}:{api_key}"
    encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
    return f"Basic {encoded}"


def _format_record_to_json(record: logging.LogRecord) -> str:
    """Serialize a LogRecord into a JSON string for the Loki log line.

    Fields included: message, level, logger_name, timestamp_iso, module,
    funcName, lineno, request_id, and any extra fields attached via
    ``logger.error("msg", extra={...})``.
    """
    entry: Dict[str, Any] = {
        "message": record.getMessage(),
        "level": record.levelname,
        "logger_name": record.name,
        "timestamp_iso": datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z",
        "module": record.module,
        "funcName": record.funcName,
        "lineno": record.lineno,
    }

    # Request ID from monitoring (thread-local)
    request_id = _get_request_id()
    if request_id:
        entry["request_id"] = request_id

    # Exception info
    if record.exc_info and record.exc_info[0] is not None:
        formatter = logging.Formatter()
        entry["exception"] = formatter.formatException(record.exc_info)

    # Extra fields (skip standard LogRecord attributes)
    _standard_attrs = {
        "name",
        "msg",
        "args",
        "created",
        "relativeCreated",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "pathname",
        "filename",
        "module",
        "levelno",
        "levelname",
        "msecs",
        "thread",
        "threadName",
        "process",
        "processName",
        "message",
        "taskName",
    }
    extras = {
        k: v
        for k, v in record.__dict__.items()
        if k not in _standard_attrs and not k.startswith("_")
    }
    if extras:
        entry["extra"] = extras

    try:
        return json.dumps(entry, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps(
            {
                "message": record.getMessage(),
                "level": record.levelname,
                "error": "log_serialization_failed",
            }
        )


# ---------------------------------------------------------------------------
# GrafanaLokiHandler
# ---------------------------------------------------------------------------


class GrafanaLokiHandler(logging.Handler):
    """Ships structured JSON logs to Grafana Cloud Loki.

    Records are buffered in memory and flushed periodically (every
    ``flush_interval`` seconds) or when the buffer reaches ``flush_threshold``
    records.  A background daemon thread handles timed flushes.

    The buffer has a hard cap (``max_buffer_size``).  When the cap is
    exceeded the *oldest* records are dropped to prevent unbounded memory
    growth.

    All network errors are caught, logged locally to stderr, and counted --
    they never propagate to the caller.

    Args:
        loki_url:         Loki push base URL (without /loki/api/v1/push).
        auth_header:      Pre-built ``Authorization`` header value (Basic ...).
        env_label:        Value for the ``env`` stream label.
        level:            Minimum log level to ship (default WARNING).
        flush_interval:   Seconds between timed flushes (default 5).
        flush_threshold:  Records in buffer to trigger an immediate flush
                          (default 50).
        max_buffer_size:  Hard cap on buffer length; oldest records are
                          dropped if exceeded (default 500).
    """

    def __init__(
        self,
        loki_url: str,
        auth_header: str,
        env_label: str = "development",
        level: int = logging.WARNING,
        flush_interval: float = _FLUSH_INTERVAL_SECONDS,
        flush_threshold: int = _FLUSH_THRESHOLD_RECORDS,
        max_buffer_size: int = _MAX_BUFFER_SIZE,
    ) -> None:
        super().__init__(level=level)

        # Loki endpoint
        self._push_url = loki_url.rstrip("/") + "/loki/api/v1/push"
        self._auth_header = auth_header
        self._env_label = env_label

        # Buffer (thread-safe via lock; deque with maxlen for overflow safety)
        # (level_label, timestamp_ns, log_line, retry_count)
        self._buffer: deque[Tuple[str, str, str, int]] = deque()
        self._buffer_lock = threading.Lock()
        self._max_buffer_size = max_buffer_size

        # Flush settings
        self._flush_interval = flush_interval
        self._flush_threshold = flush_threshold

        # Background flush thread
        self._stop_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._flush_loop,
            name="grafana-loki-flush",
            daemon=True,
        )
        self._flush_thread.start()

    # -- logging.Handler interface ------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        """Buffer a log record for later flushing to Loki.

        If the buffer exceeds max_buffer_size, the oldest records are
        discarded to prevent memory exhaustion.
        """
        try:
            level_label = _LEVEL_LABEL_MAP.get(record.levelno, "info")
            log_line = _format_record_to_json(record)
            timestamp_ns = str(int(record.created * 1_000_000_000))

            with self._buffer_lock:
                self._buffer.append((level_label, timestamp_ns, log_line, 0))
                # Enforce hard cap: prioritise keeping ERROR/CRITICAL entries
                overflow = len(self._buffer) - self._max_buffer_size
                if overflow > 0:
                    # Partition into high-priority (error/critical) and normal
                    _high = [r for r in self._buffer if r[0] in ("error", "critical")]
                    _low = [
                        r for r in self._buffer if r[0] not in ("error", "critical")
                    ]
                    # Drop from low-priority (newest first from low) to free space
                    drop_count = min(overflow, len(_low))
                    if drop_count > 0:
                        _low = _low[:-drop_count]
                    remaining_drop = overflow - drop_count
                    # If still need to drop, take from high-priority (oldest first)
                    if remaining_drop > 0 and _high:
                        _high = _high[remaining_drop:]
                    self._buffer.clear()
                    self._buffer.extend(_high + _low)
                    _stats.add_dropped(overflow)

                buffer_len = len(self._buffer)

            # Check if we should flush immediately (threshold reached)
            if buffer_len >= self._flush_threshold:
                self._do_flush()

        except Exception:
            # Never let a logging failure crash the application
            self.handleError(record)

    def flush(self) -> None:
        """Flush buffered records to Loki immediately."""
        self._do_flush()

    def close(self) -> None:
        """Final flush and stop the background thread."""
        self._stop_event.set()
        # Perform a final flush of any remaining records
        self._do_flush()
        # Wait for the background thread to finish (with timeout)
        if self._flush_thread.is_alive():
            self._flush_thread.join(timeout=5.0)
        super().close()

    # -- Internal -----------------------------------------------------------

    def _requeue_records(
        self, records: list, max_retries: int, should_retry: bool
    ) -> None:
        """Put failed records back into the buffer for retry.

        Args:
            records: The failed batch of records to potentially re-queue.
            max_retries: Maximum number of retry attempts per record.
            should_retry: If False, all records are dropped (e.g. auth errors).
        """
        if not should_retry:
            _stats.add_dropped(len(records))
            return

        retryable = []
        dropped_retries = 0
        for rec in records:
            retry_count = rec[3] if len(rec) > 3 else 0
            if retry_count < max_retries:
                retryable.append((rec[0], rec[1], rec[2], retry_count + 1))
            else:
                dropped_retries += 1
        if dropped_retries:
            _stats.add_dropped(dropped_retries)
        if retryable:
            with self._buffer_lock:
                for item in reversed(retryable):
                    self._buffer.appendleft(item)
                overflow = len(self._buffer) - self._max_buffer_size
                if overflow > 0:
                    # Pop from the right (oldest existing records) to keep retryable ones
                    for _ in range(overflow):
                        self._buffer.pop()
                    _stats.add_dropped(overflow)

    def _flush_loop(self) -> None:
        """Background loop: flush every ``_flush_interval`` seconds."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._flush_interval)
            if not self._stop_event.is_set():
                self._do_flush()

    def _do_flush(self) -> None:
        """Drain the buffer and push records to Loki.

        Records are grouped by level label into separate Loki streams for
        efficient querying (Loki indexes on stream labels).
        """
        # Atomically drain the buffer
        with self._buffer_lock:
            if not self._buffer:
                return
            records = list(self._buffer)
            self._buffer.clear()

        # Group records by level label
        # Each record is (level_label, timestamp_ns, log_line, retry_count)
        # Legacy 3-tuples (no retry_count) are handled for safety.
        _MAX_FLUSH_RETRIES = 3
        streams_map: Dict[str, List[List[str]]] = {}
        for rec in records:
            level_label, timestamp_ns, log_line = rec[0], rec[1], rec[2]
            if level_label not in streams_map:
                streams_map[level_label] = []
            streams_map[level_label].append([timestamp_ns, log_line])

        # Build the Loki push payload
        streams: List[Dict[str, Any]] = []
        for level_label, values in streams_map.items():
            streams.append(
                {
                    "stream": {
                        "app": _APP_LABEL,
                        "env": self._env_label,
                        "level": level_label,
                    },
                    "values": values,
                }
            )

        payload = json.dumps({"streams": streams}).encode("utf-8")

        # POST to Loki
        try:
            req = urllib.request.Request(
                self._push_url,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": self._auth_header,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                # 2xx = success; Loki returns 204 on success
                _ = resp.read()

            total_sent = len(records)
            _stats.add_shipped(total_sent)
            _stats.set_last_flush()

        except urllib.error.HTTPError as http_err:
            # Capture the actual HTTP status and response body for diagnosis
            error_body = ""
            try:
                error_body = http_err.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                pass
            error_msg = (
                f"HTTP {http_err.code}: {error_body}"
                if error_body
                else f"HTTP {http_err.code}"
            )
            _stats.add_flush_error(error_msg=error_msg, http_status=http_err.code)
            logger.warning(
                "grafana_logger: Loki push failed (%d records) -- HTTP %d: %s",
                len(records),
                http_err.code,
                error_body[:200],
            )
            # Retry on transient server errors (5xx, 429); skip retry on
            # auth errors (401/403) which won't resolve on retry
            should_retry = http_err.code >= 500 or http_err.code == 429
            self._requeue_records(records, _MAX_FLUSH_RETRIES, should_retry)

        except (urllib.error.URLError, OSError, ValueError) as exc:
            # Network/URL errors (DNS, connection refused, timeout, etc.)
            error_msg = f"{type(exc).__name__}: {exc}"
            _stats.add_flush_error(error_msg=error_msg)
            logger.warning(
                "grafana_logger: Loki push failed (%d records) -- %s: %s",
                len(records),
                type(exc).__name__,
                exc,
            )
            # Network errors are always retryable
            self._requeue_records(records, _MAX_FLUSH_RETRIES, should_retry=True)

        except Exception as exc:
            # Catch-all for unexpected errors -- drop records (no retry for unknown errors)
            error_msg = f"{type(exc).__name__}: {exc}"
            _stats.add_flush_error(error_msg=error_msg)
            _stats.add_dropped(len(records))
            logger.warning(
                "grafana_logger: unexpected Loki push error (%d records dropped): %s",
                len(records),
                exc,
            )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def setup_grafana_logging(
    root_logger: Optional[logging.Logger] = None,
    level: int = logging.WARNING,
) -> bool:
    """Attach a GrafanaLokiHandler to the root logger (or a specified logger).

    Reads configuration from environment variables.  If ``GRAFANA_LOKI_URL``
    or ``GRAFANA_API_KEY`` is not set, the function is a no-op and returns
    False -- making it safe to call unconditionally at startup.

    Args:
        root_logger: Logger to attach the handler to.  Defaults to the
                     Python root logger (``logging.getLogger()``).
        level:       Minimum log level shipped to Loki.  Default is
                     ``logging.WARNING`` so that only WARNING, ERROR, and
                     CRITICAL records are sent, keeping volume manageable.

    Returns:
        True if the handler was successfully attached, False if the
        required environment variables are missing.

    Usage::

        import logging
        from grafana_logger import setup_grafana_logging

        if setup_grafana_logging(logging.getLogger()):
            print("Grafana Loki logging active")
    """
    loki_url = (os.environ.get("GRAFANA_LOKI_URL") or "").strip()
    api_key = (os.environ.get("GRAFANA_API_KEY") or "").strip()

    if not loki_url or not api_key:
        return False

    user_id = (os.environ.get("GRAFANA_USER_ID") or "").strip()
    env_label = os.environ.get("RENDER_ENV", "development")

    auth_header = _build_auth_header(user_id, api_key)

    handler = GrafanaLokiHandler(
        loki_url=loki_url,
        auth_header=auth_header,
        env_label=env_label,
        level=level,
    )

    target_logger = root_logger if root_logger is not None else logging.getLogger()
    target_logger.addHandler(handler)

    return True


def get_grafana_stats() -> Dict[str, Any]:
    """Return operational statistics for the Grafana Loki handler.

    Useful for the ``/api/health`` or ``/api/admin/stats`` endpoint to
    expose observability pipeline health.

    Returns a dict with:
        records_shipped  -- Total log records successfully pushed to Loki.
        records_dropped  -- Records discarded due to buffer overflow.
        flush_errors     -- Number of failed push attempts.
        last_flush_time  -- Unix timestamp of the most recent successful
                            flush, or None if no flush has occurred yet.
        last_flush_iso   -- ISO-8601 formatted version of last_flush_time.
        last_error       -- Most recent error message (if any).
        last_error_status -- HTTP status code of last error (if applicable).
    """
    return _stats.snapshot()


def diagnose_grafana() -> Dict[str, Any]:
    """Run a lightweight diagnostic check on Grafana Loki connectivity.

    Sends a single test log entry and reports success/failure with
    detailed error information. Useful for startup validation and
    the /api/health/integrations/diagnose endpoint.

    Returns a dict with: ok (bool), detail (str), http_status (int|None).
    """
    loki_url = (os.environ.get("GRAFANA_LOKI_URL") or "").strip()
    api_key = (os.environ.get("GRAFANA_API_KEY") or "").strip()
    user_id = (os.environ.get("GRAFANA_USER_ID") or "").strip()

    # Check env vars first
    if not loki_url:
        return {"ok": False, "detail": "GRAFANA_LOKI_URL not set", "http_status": None}
    if not api_key:
        return {"ok": False, "detail": "GRAFANA_API_KEY not set", "http_status": None}
    if not user_id:
        # GRAFANA_USER_ID is optional (defaults to empty), but may cause auth failures
        pass  # Will be caught by the HTTP request below if auth fails

    # Validate URL format
    push_url = loki_url.rstrip("/") + "/loki/api/v1/push"
    auth_header = _build_auth_header(user_id, api_key)

    # Build a minimal test payload
    test_payload = json.dumps(
        {
            "streams": [
                {
                    "stream": {
                        "app": _APP_LABEL,
                        "env": os.environ.get("RENDER_ENV", "development"),
                        "level": "info",
                    },
                    "values": [
                        [
                            str(int(time.time() * 1_000_000_000)),
                            json.dumps(
                                {
                                    "message": "grafana_logger diagnostic ping",
                                    "level": "INFO",
                                }
                            ),
                        ]
                    ],
                }
            ]
        }
    ).encode("utf-8")

    try:
        req = urllib.request.Request(
            push_url,
            data=test_payload,
            method="POST",
            headers={"Content-Type": "application/json", "Authorization": auth_header},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read()
            status_code = resp.status
        return {
            "ok": True,
            "detail": f"Loki push OK (URL: {push_url})",
            "http_status": status_code,
        }

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        detail = f"HTTP {http_err.code}"
        if http_err.code == 401:
            detail += " Unauthorized -- check GRAFANA_API_KEY and GRAFANA_USER_ID"
        elif http_err.code == 403:
            detail += " Forbidden -- API key may lack push permissions"
        elif http_err.code == 404:
            detail += f" Not Found -- check GRAFANA_LOKI_URL ({loki_url})"
        if error_body:
            detail += f" | body: {error_body[:200]}"
        return {"ok": False, "detail": detail, "http_status": http_err.code}

    except urllib.error.URLError as url_err:
        return {
            "ok": False,
            "detail": f"Connection failed: {url_err.reason}",
            "http_status": None,
        }

    except Exception as exc:
        return {
            "ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
            "http_status": None,
        }
