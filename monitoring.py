"""
Monitoring & Observability Module for AI Media Planner.

Provides:
- Deep health checks (liveness + readiness + dependency checks)
- Structured request metrics (latency, error rates, throughput)
- Memory and disk usage tracking
- API dependency reachability probes
- Metrics export endpoint for dashboards
- Graceful shutdown coordination
- Structured JSON logging with request tracing (v3.1)
- SLO monitoring and error budget tracking (v3.1)
- Audit trail for data transformation decisions (v3.1)

This module has no external dependencies (stdlib only).
"""

from __future__ import annotations

import gc
import json
import logging
import os
import platform
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "3.5.0"
_START_TIME = time.time()
DATA_DIR = Path(__file__).resolve().parent / "data"
PERSISTENT_DIR = Path(__file__).resolve().parent / "data" / "persistent"
CACHE_DIR = DATA_DIR / "api_cache"
DOCS_DIR = DATA_DIR / "generated_docs"
METRICS_WINDOW = 3600  # 1 hour rolling window for rate calculations


# ---------------------------------------------------------------------------
# Request Context (Thread-Local)
# ---------------------------------------------------------------------------

_request_context = threading.local()


def set_request_id(request_id: str) -> None:
    """Set the current request ID for this thread."""
    _request_context.request_id = request_id
    _request_context.request_start = time.time()


def get_request_id() -> str:
    """Get the current request ID for this thread, or empty string."""
    return getattr(_request_context, "request_id", "")


def get_request_elapsed_ms() -> float:
    """Get elapsed time since request start in milliseconds."""
    start = getattr(_request_context, "request_start", None)
    if start is None:
        return 0.0
    return (time.time() - start) * 1000


def clear_request_context() -> None:
    """Clear request context at end of request."""
    _request_context.request_id = ""
    _request_context.request_start = None


def generate_request_id() -> str:
    """Generate a new unique request ID (12-char hex)."""
    return uuid.uuid4().hex[:12]


class RequestContext:
    """Context manager for request-scoped tracing.

    Usage::

        with RequestContext() as rid:
            # rid is the generated request ID
            logger.info("Processing request %s", rid)
    """

    def __init__(self, request_id: str = "") -> None:
        self._rid = request_id or generate_request_id()

    def __enter__(self) -> str:
        set_request_id(self._rid)
        return self._rid

    def __exit__(self, *exc_info) -> None:
        clear_request_context()


# ---------------------------------------------------------------------------
# Structured JSON Log Formatter
# ---------------------------------------------------------------------------


class StructuredJsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects.

    Each log entry includes: timestamp, level, logger name, message,
    request_id (from thread-local context), duration_ms, and any extra fields.

    Example output::

        {"ts":"2025-03-09T14:23:01Z","level":"INFO","logger":"nova","msg":"Tool executed","request_id":"a1b2c3d4e5f6","extra":{}}
    """

    def format(self, record: logging.LogRecord) -> str:
        entry: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Inject request context from thread-local
        rid = get_request_id()
        if rid:
            entry["request_id"] = rid
            entry["elapsed_ms"] = round(get_request_elapsed_ms(), 1)

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)

        # Include any extra fields set via logger.info("msg", extra={...})
        standard_attrs = {
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
            if k not in standard_attrs and not k.startswith("_")
        }
        if extras:
            entry["extra"] = extras

        try:
            return json.dumps(entry, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            return json.dumps(
                {
                    "ts": entry.get("ts") or "",
                    "level": "ERROR",
                    "msg": f"Log serialization failed: {record.getMessage()}",
                }
            )


# ---------------------------------------------------------------------------
# SLO Definitions
# ---------------------------------------------------------------------------

SLO_TARGETS: Dict[str, Dict[str, Any]] = {
    "generate_p99_ms": {
        "target": 30000,  # 30 seconds
        "description": "99th percentile generation latency",
        "endpoint": "/api/generate",
    },
    "chat_p99_ms": {
        "target": 8000,  # 8 seconds
        "description": "99th percentile chat latency",
        "endpoint": "/api/chat",
    },
    "error_rate_pct": {
        "target": 1.0,  # 1% error budget
        "description": "Error rate across all endpoints",
    },
    "availability_pct": {
        "target": 99.5,
        "description": "Service availability (uptime)",
    },
}


# ---------------------------------------------------------------------------
# Singleton Metrics Collector
# ---------------------------------------------------------------------------


class MetricsCollector:
    """Thread-safe singleton for collecting application metrics.

    Tracks request counts, latencies, error rates, and endpoint-level
    breakdowns over a rolling 1-hour window.
    """

    _instance: Optional["MetricsCollector"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "MetricsCollector":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._initialized = False
                    cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._req_lock = threading.Lock()
        # Counters
        self.total_requests: int = 0
        self.total_errors: int = 0
        self.total_generations: int = 0
        self.total_chat_requests: int = 0
        self.total_slack_events: int = 0
        # v3.5 routing metrics
        self.chat_conversational_count: int = 0  # Path A (no tools)
        self.chat_tool_count: int = 0  # Path B (free tools)
        self.chat_claude_count: int = 0  # Path C (paid fallback)
        self.chat_suppressed_count: int = 0  # Suppression gate triggered
        # Rolling window for rate calculations
        self._recent_requests: deque = deque()
        self._recent_errors: deque = deque()
        # Per-endpoint latency tracking (last 200 per endpoint)
        self._latencies: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
        # Status code distribution
        self._status_codes: Dict[int, int] = defaultdict(int)
        # Active request counter (concurrency gauge)
        self._active_requests: int = 0
        self._peak_active: int = 0
        # Generation timing
        self._generation_times: deque = deque(maxlen=100)
        # API enrichment tracking
        self._api_success_count: int = 0
        self._api_failure_count: int = 0
        self._api_latencies: deque = deque(maxlen=200)

    def record_request(
        self, endpoint: str, method: str, status_code: int, latency_ms: float
    ) -> None:
        """Record a completed HTTP request."""
        now = time.time()
        with self._req_lock:
            self.total_requests += 1
            self._recent_requests.append(now)
            self._status_codes[status_code] += 1
            self._latencies[endpoint].append(latency_ms)

            if status_code >= 400:
                self.total_errors += 1
                self._recent_errors.append(now)

            # Prune old entries from rolling windows
            cutoff = now - METRICS_WINDOW
            while self._recent_requests and self._recent_requests[0] < cutoff:
                self._recent_requests.popleft()
            while self._recent_errors and self._recent_errors[0] < cutoff:
                self._recent_errors.popleft()

    def record_generation(self, duration_seconds: float) -> None:
        """Record a media plan generation event."""
        with self._req_lock:
            self.total_generations += 1
            self._generation_times.append(duration_seconds)

    def record_chat(self, path: str = "") -> None:
        with self._req_lock:
            self.total_chat_requests += 1
            # v3.5 routing path tracking
            if path == "conversational":
                self.chat_conversational_count += 1
            elif path == "tool":
                self.chat_tool_count += 1
            elif path == "claude":
                self.chat_claude_count += 1
            elif path == "suppressed":
                self.chat_suppressed_count += 1

    def record_slack_event(self) -> None:
        with self._req_lock:
            self.total_slack_events += 1

    def record_api_call(self, success: bool, latency_ms: float) -> None:
        """Record an external API enrichment call."""
        with self._req_lock:
            if success:
                self._api_success_count += 1
            else:
                self._api_failure_count += 1
            self._api_latencies.append(latency_ms)

    def enter_request(self) -> None:
        with self._req_lock:
            self._active_requests += 1
            if self._active_requests > self._peak_active:
                self._peak_active = self._active_requests

    def exit_request(self) -> None:
        with self._req_lock:
            self._active_requests = max(0, self._active_requests - 1)

    def get_metrics(self) -> Dict[str, Any]:
        """Return current metrics snapshot."""
        now = time.time()
        uptime = now - _START_TIME

        with self._req_lock:
            # Requests per minute (RPM) over rolling window
            window_requests = len(self._recent_requests)
            window_errors = len(self._recent_errors)
            window_duration = min(uptime, METRICS_WINDOW)
            rpm = (window_requests / window_duration * 60) if window_duration > 0 else 0
            error_rate = (
                (window_errors / window_requests * 100) if window_requests > 0 else 0
            )

            # Latency percentiles across all endpoints
            all_latencies = []
            for lat_deque in self._latencies.values():
                all_latencies.extend(lat_deque)
            all_latencies.sort()

            p50 = _percentile(all_latencies, 50)
            p95 = _percentile(all_latencies, 95)
            p99 = _percentile(all_latencies, 99)

            # Generation time stats
            gen_times = list(self._generation_times)
            gen_times.sort()
            avg_gen = (sum(gen_times) / len(gen_times)) if gen_times else 0

            # Per-endpoint breakdown
            endpoint_stats = {}
            for ep, lats in self._latencies.items():
                lat_list = sorted(lats)
                endpoint_stats[ep] = {
                    "count": len(lat_list),
                    "avg_ms": (
                        round(sum(lat_list) / len(lat_list), 1) if lat_list else 0
                    ),
                    "p95_ms": round(_percentile(lat_list, 95), 1),
                }

            return {
                "uptime_seconds": round(uptime, 1),
                "uptime_human": _format_duration(uptime),
                "total_requests": self.total_requests,
                "total_errors": self.total_errors,
                "total_generations": self.total_generations,
                "total_chat_requests": self.total_chat_requests,
                "total_slack_events": self.total_slack_events,
                "requests_per_minute": round(rpm, 2),
                "error_rate_pct": round(error_rate, 2),
                "active_requests": self._active_requests,
                "peak_concurrent": self._peak_active,
                "latency_ms": {
                    "p50": round(p50, 1),
                    "p95": round(p95, 1),
                    "p99": round(p99, 1),
                },
                "generation_time_seconds": {
                    "avg": round(avg_gen, 2),
                    "p95": round(_percentile(gen_times, 95), 2),
                    "total_generated": len(gen_times),
                },
                "status_codes": dict(self._status_codes),
                "endpoints": endpoint_stats,
                "api_enrichment": {
                    "success_count": self._api_success_count,
                    "failure_count": self._api_failure_count,
                    "success_rate_pct": round(
                        self._api_success_count
                        / max(1, self._api_success_count + self._api_failure_count)
                        * 100,
                        1,
                    ),
                    "avg_latency_ms": (
                        round(
                            sum(self._api_latencies) / max(1, len(self._api_latencies)),
                            1,
                        )
                        if self._api_latencies
                        else 0
                    ),
                },
                # v3.5 routing breakdown
                "chat_routing": {
                    "conversational_count": self.chat_conversational_count,
                    "tool_count": self.chat_tool_count,
                    "claude_count": self.chat_claude_count,
                    "suppressed_count": self.chat_suppressed_count,
                    "tool_pct": round(
                        self.chat_tool_count / max(1, self.total_chat_requests) * 100, 1
                    ),
                    "claude_pct": round(
                        self.chat_claude_count / max(1, self.total_chat_requests) * 100,
                        1,
                    ),
                },
            }

    def check_slo_compliance(self) -> Dict[str, Dict[str, Any]]:
        """Check current metrics against SLO targets.

        Returns a dict of SLO name -> {target, actual, compliant, budget_remaining_pct}.
        """
        results: Dict[str, Dict[str, Any]] = {}
        now = time.time()
        uptime = now - _START_TIME

        with self._req_lock:
            # Generate P99 latency
            gen_lats = sorted(self._latencies.get("/api/generate") or [])
            gen_p99 = _percentile(gen_lats, 99) if gen_lats else 0.0
            results["generate_p99_ms"] = {
                "target": SLO_TARGETS["generate_p99_ms"]["target"],
                "actual": round(gen_p99, 1),
                "compliant": gen_p99 <= SLO_TARGETS["generate_p99_ms"]["target"],
                "sample_size": len(gen_lats),
            }

            # Chat P99 latency
            chat_lats = sorted(self._latencies.get("/api/chat") or [])
            chat_p99 = _percentile(chat_lats, 99) if chat_lats else 0.0
            results["chat_p99_ms"] = {
                "target": SLO_TARGETS["chat_p99_ms"]["target"],
                "actual": round(chat_p99, 1),
                "compliant": chat_p99 <= SLO_TARGETS["chat_p99_ms"]["target"],
                "sample_size": len(chat_lats),
            }

            # Error rate
            total = max(1, self.total_requests)
            error_rate = (self.total_errors / total) * 100
            target_err = SLO_TARGETS["error_rate_pct"]["target"]
            results["error_rate_pct"] = {
                "target": target_err,
                "actual": round(error_rate, 3),
                "compliant": error_rate <= target_err,
                "budget_remaining_pct": round(max(0, target_err - error_rate), 3),
            }

            # Availability (based on uptime -- simple heuristic)
            # If we're running, we're available. Track non-5xx as available.
            total_5xx = sum(
                count for code, count in self._status_codes.items() if 500 <= code < 600
            )
            avail = ((total - total_5xx) / total) * 100 if total > 0 else 100.0
            target_avail = SLO_TARGETS["availability_pct"]["target"]
            results["availability_pct"] = {
                "target": target_avail,
                "actual": round(avail, 3),
                "compliant": avail >= target_avail,
                "budget_remaining_pct": round(max(0, avail - target_avail), 3),
            }

        # Overall compliance
        all_compliant = all(r.get("compliant", True) for r in results.values())
        return {
            "slos": results,
            "all_compliant": all_compliant,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": round(uptime, 1),
        }


def get_metrics() -> MetricsCollector:
    """Get the singleton MetricsCollector instance."""
    return MetricsCollector()


# ---------------------------------------------------------------------------
# Supabase Metrics Persistence
# ---------------------------------------------------------------------------
#
# Required Supabase table (run once via SQL editor):
#
# CREATE TABLE IF NOT EXISTS metrics_snapshot (
#     id TEXT PRIMARY KEY DEFAULT 'singleton',
#     data JSONB NOT NULL,
#     updated_at TIMESTAMPTZ DEFAULT now()
# );

# Persistable counter fields in MetricsCollector that survive deploys.
_PERSIST_COUNTERS = (
    "total_requests",
    "total_errors",
    "total_generations",
    "total_chat_requests",
    "total_slack_events",
    "chat_conversational_count",
    "chat_tool_count",
    "chat_claude_count",
    "chat_suppressed_count",
    "_api_success_count",
    "_api_failure_count",
)

_PERSIST_SAVE_INTERVAL_SEC = 300  # 5 minutes


class SupabasePersistence:
    """Persist and restore MetricsCollector counters via Supabase REST API.

    Reads ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` from environment
    variables.  If either is missing, persistence is silently disabled and the
    app continues with in-memory-only metrics.

    Lifecycle:
        1. ``load()`` -- called once at startup to restore counters.
        2. ``start_background_save()`` -- launches a daemon thread that
           upserts the current snapshot every 5 minutes.
        3. ``save()`` -- can be called manually (e.g. on graceful shutdown).

    All operations are thread-safe and will never crash the main application.
    """

    def __init__(self, collector: MetricsCollector) -> None:
        self._collector = collector
        self._lock = threading.Lock()
        self._enabled: bool = False
        self._last_save_ok: bool = False
        self._last_save_ts: float = 0.0
        self._base_url: str = ""
        self._api_key: str = ""
        self._save_thread: Optional[threading.Thread] = None

        url = os.environ.get("SUPABASE_URL") or ""
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
        if url and key:
            self._base_url = url.rstrip("/")
            self._api_key = key
            self._enabled = True
            logger.info("Supabase metrics persistence enabled")
        else:
            logger.info(
                "Supabase metrics persistence disabled (SUPABASE_URL or "
                "SUPABASE_SERVICE_ROLE_KEY not set)"
            )

    @property
    def is_enabled(self) -> bool:
        """Return True if Supabase credentials are configured."""
        return self._enabled

    @property
    def is_persisted(self) -> bool:
        """Return True if the most recent save succeeded."""
        return self._enabled and self._last_save_ok

    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers for Supabase REST API calls."""
        return {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }

    def _rest_url(self) -> str:
        """Return the full REST endpoint for the metrics_snapshot table."""
        return f"{self._base_url}/rest/v1/metrics_snapshot"

    # -- Load ----------------------------------------------------------------

    def load(self) -> bool:
        """Load previous metrics snapshot from Supabase and apply to collector.

        Returns True if counters were restored, False otherwise.
        """
        if not self._enabled:
            return False
        try:
            url = f"{self._rest_url()}?id=eq.singleton&select=data"
            req = urllib.request.Request(
                url, headers=self._build_headers(), method="GET"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
            rows = json.loads(body)
            if not rows:
                logger.info("No existing metrics snapshot in Supabase")
                return False
            data = rows[0].get("data") or {}
            if not isinstance(data, dict):
                return False
            self._apply_snapshot(data)
            logger.info(
                "Restored metrics from Supabase: %d total_requests, %d total_errors",
                data.get("total_requests", 0),
                data.get("total_errors", 0),
            )
            return True
        except urllib.error.URLError as e:
            logger.warning("Failed to load metrics from Supabase: %s", e)
            return False
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
            logger.warning("Malformed Supabase metrics response: %s", e)
            return False

    def _apply_snapshot(self, data: Dict[str, Any]) -> None:
        """Apply saved counter values to the collector (additive merge)."""
        with self._collector._req_lock:
            for field in _PERSIST_COUNTERS:
                saved_val = data.get(field, 0)
                if isinstance(saved_val, (int, float)) and saved_val > 0:
                    current = getattr(self._collector, field, 0)
                    setattr(self._collector, field, current + int(saved_val))

    # -- Save ----------------------------------------------------------------

    def save(self) -> bool:
        """Save current metrics snapshot to Supabase (upsert).

        Returns True on success, False on failure.
        """
        if not self._enabled:
            return False
        try:
            snapshot = self._build_snapshot()
            payload = json.dumps(
                [
                    {
                        "id": "singleton",
                        "data": snapshot,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                ],
                default=str,
            )
            req = urllib.request.Request(
                self._rest_url(),
                data=payload.encode("utf-8"),
                headers=self._build_headers(),
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()  # consume response
            with self._lock:
                self._last_save_ok = True
                self._last_save_ts = time.time()
            logger.debug("Metrics snapshot saved to Supabase")
            return True
        except urllib.error.URLError as e:
            with self._lock:
                self._last_save_ok = False
            logger.warning("Failed to save metrics to Supabase: %s", e)
            return False
        except (TypeError, ValueError) as e:
            with self._lock:
                self._last_save_ok = False
            logger.warning("Metrics serialization failed: %s", e)
            return False

    def _build_snapshot(self) -> Dict[str, Any]:
        """Extract persistable counter values from the collector."""
        snapshot: Dict[str, Any] = {}
        with self._collector._req_lock:
            for field in _PERSIST_COUNTERS:
                snapshot[field] = getattr(self._collector, field, 0)
        snapshot["saved_at"] = datetime.now(timezone.utc).isoformat()
        return snapshot

    # -- Background save loop ------------------------------------------------

    def start_background_save(self) -> None:
        """Start a daemon thread that saves metrics every 5 minutes."""
        if not self._enabled:
            return
        if self._save_thread is not None and self._save_thread.is_alive():
            return  # already running
        self._save_thread = threading.Thread(
            target=self._save_loop, name="metrics-persist", daemon=True
        )
        self._save_thread.start()
        logger.info(
            "Metrics persistence background thread started " "(interval=%ds)",
            _PERSIST_SAVE_INTERVAL_SEC,
        )

    def _save_loop(self) -> None:
        """Periodically save metrics until the process exits."""
        while True:
            time.sleep(_PERSIST_SAVE_INTERVAL_SEC)
            try:
                self.save()
            except Exception as e:
                logger.error(
                    "Unexpected error in metrics persistence loop: %s",
                    e,
                    exc_info=True,
                )

    # -- Status ---------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Return persistence status for health endpoints."""
        with self._lock:
            return {
                "metrics_persisted": self.is_persisted,
                "persistence_enabled": self._enabled,
                "last_save_ok": self._last_save_ok,
                "last_save_ts": (
                    datetime.fromtimestamp(
                        self._last_save_ts, tz=timezone.utc
                    ).isoformat()
                    if self._last_save_ts > 0
                    else None
                ),
            }


# Module-level persistence singleton (initialized after MetricsCollector).
_persistence: Optional[SupabasePersistence] = None
_persistence_init_lock = threading.Lock()


def get_persistence() -> Optional[SupabasePersistence]:
    """Get or create the SupabasePersistence singleton.

    Returns None if Supabase credentials are not configured.
    """
    global _persistence
    if _persistence is not None:
        return _persistence
    with _persistence_init_lock:
        if _persistence is not None:
            return _persistence
        collector = MetricsCollector()
        _persistence = SupabasePersistence(collector)
        return _persistence


def init_metrics_persistence() -> None:
    """Initialize Supabase persistence: load snapshot and start background saves.

    Call this once at application startup (after MetricsCollector is ready).
    Safe to call even if Supabase is not configured -- will be a no-op.
    """
    p = get_persistence()
    if p is None or not p.is_enabled:
        return
    try:
        p.load()
    except Exception as e:
        logger.error("Failed to load persisted metrics: %s", e, exc_info=True)
    p.start_background_save()


# ---------------------------------------------------------------------------
# Health Checks
# ---------------------------------------------------------------------------


def health_check_liveness() -> Dict[str, Any]:
    """Lightweight liveness probe -- confirms the process is alive.

    Suitable for Render.com / Kubernetes liveness probes.
    Should return quickly (< 100ms).
    """
    result: Dict[str, Any] = {
        "status": "ok",
        "version": VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(time.time() - _START_TIME, 1),
    }
    p = get_persistence()
    if p is not None:
        result["metrics_persisted"] = p.is_persisted
    else:
        result["metrics_persisted"] = False
    return result


def health_check_readiness() -> Dict[str, Any]:
    """Readiness probe -- checks that the service can handle requests.

    Verifies:
    - Knowledge base files are loadable
    - Data directory is writable
    - Memory usage is within bounds
    - Required modules are importable
    """
    checks: Dict[str, Dict[str, Any]] = {}
    overall_healthy = True

    # 1. Knowledge base files
    kb_files = [
        "recruitment_industry_knowledge.json",
        "platform_intelligence_deep.json",
        "recruitment_benchmarks_deep.json",
        "channels_db.json",
    ]
    kb_status = "ok"
    kb_details = []
    for fname in kb_files:
        fpath = DATA_DIR / fname
        if fpath.exists():
            size_kb = fpath.stat().st_size / 1024
            kb_details.append(f"{fname}: {size_kb:.0f}KB")
        else:
            kb_status = "degraded"
            kb_details.append(f"{fname}: MISSING")
    checks["knowledge_base"] = {"status": kb_status, "files": kb_details}
    if kb_status != "ok":
        overall_healthy = False

    # 2. Data directory writable
    try:
        test_file = DATA_DIR / ".health_check_write_test"
        test_file.write_text("ok")
        test_file.unlink()
        checks["disk_write"] = {"status": "ok"}
    except Exception as e:
        checks["disk_write"] = {"status": "error", "detail": str(e)}
        overall_healthy = False

    # 2b. Disk space health (auto-recovery on low space)
    try:
        disk_health = check_disk_health()
        checks["disk_health"] = disk_health
        if disk_health.get("status") == "critical":
            overall_healthy = False
    except Exception as e:
        checks["disk_health"] = {"status": "unknown", "detail": str(e)}

    # 3. Disk usage (generated docs)
    try:
        docs_dir = DOCS_DIR
        if docs_dir.exists():
            doc_count = len(list(docs_dir.glob("*.zip")))
            total_size_mb = sum(f.stat().st_size for f in docs_dir.glob("*.zip")) / (
                1024 * 1024
            )
            checks["document_storage"] = {
                "status": "ok" if doc_count < 500 else "warning",
                "document_count": doc_count,
                "total_size_mb": round(total_size_mb, 1),
            }
        else:
            checks["document_storage"] = {"status": "ok", "document_count": 0}
    except Exception as e:
        checks["document_storage"] = {"status": "error", "detail": str(e)}

    # 4. API cache status
    try:
        cache_dir = CACHE_DIR
        if cache_dir.exists():
            cache_count = len(list(cache_dir.glob("*.json")))
            cache_size_mb = sum(f.stat().st_size for f in cache_dir.glob("*.json")) / (
                1024 * 1024
            )
            checks["api_cache"] = {
                "status": "ok",
                "cached_responses": cache_count,
                "total_size_mb": round(cache_size_mb, 1),
            }
        else:
            checks["api_cache"] = {"status": "ok", "cached_responses": 0}
    except Exception as e:
        checks["api_cache"] = {"status": "error", "detail": str(e)}

    # 5. Memory usage
    try:
        mem_info = _get_memory_usage()
        mem_mb = mem_info["rss_mb"]
        mem_status = "ok"
        if mem_mb > 1024:
            mem_status = "warning"
        if mem_mb > 2048:
            mem_status = "critical"
            overall_healthy = False
        checks["memory"] = {
            "status": mem_status,
            **mem_info,
        }
    except Exception as e:
        checks["memory"] = {"status": "unknown", "detail": str(e)}

    # 6. Required modules
    required_modules = {
        "openpyxl": "Excel generation",
        "pptx": "PowerPoint generation",
    }
    module_status = "ok"
    module_details = {}
    for mod_name, purpose in required_modules.items():
        try:
            __import__(mod_name)
            module_details[mod_name] = "loaded"
        except ImportError:
            module_details[mod_name] = "MISSING"
            module_status = "degraded"
    checks["modules"] = {"status": module_status, "details": module_details}

    # 7. Process info
    checks["process"] = {
        "status": "ok",
        "pid": os.getpid(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "thread_count": threading.active_count(),
    }

    # 8. Orchestrator health
    try:
        import data_orchestrator

        orch_stats = {}
        if hasattr(data_orchestrator, "get_cache_stats"):
            orch_stats["cache"] = data_orchestrator.get_cache_stats()
        if hasattr(data_orchestrator, "get_fallback_telemetry"):
            orch_stats["fallback_telemetry"] = (
                data_orchestrator.get_fallback_telemetry()
            )
        checks["orchestrator"] = {"status": "ok", **orch_stats}
    except Exception as e:
        checks["orchestrator"] = {"status": "degraded", "detail": str(e)}

    # 9. Metrics persistence
    p = get_persistence()
    if p is not None:
        checks["metrics_persistence"] = p.get_status()
    else:
        checks["metrics_persistence"] = {
            "metrics_persisted": False,
            "persistence_enabled": False,
        }

    result: Dict[str, Any] = {
        "status": "healthy" if overall_healthy else "unhealthy",
        "version": VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(time.time() - _START_TIME, 1),
        "uptime_human": _format_duration(time.time() - _START_TIME),
        "checks": checks,
        "metrics_persisted": p.is_persisted if p is not None else False,
    }
    return result


# ---------------------------------------------------------------------------
# Structured Logging
# ---------------------------------------------------------------------------


def configure_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure structured logging for production.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        json_format: If True, use JSON formatter for machine-readable logs.
                     If False, use human-readable format (useful for local dev).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Root logger configuration
    root = logging.getLogger()
    root.setLevel(log_level)

    # Clear existing handlers to prevent duplicates on re-import
    root.handlers.clear()

    # Console handler with structured format
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)

    if json_format:
        formatter = StructuredJsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)

    logger.info(
        "Logging configured: level=%s, format=%s, python=%s, pid=%d",
        level,
        "json" if json_format else "text",
        platform.python_version(),
        os.getpid(),
    )


# ---------------------------------------------------------------------------
# Graceful Shutdown
# ---------------------------------------------------------------------------


class GracefulShutdown:
    """Coordinates graceful server shutdown.

    Tracks active requests and waits for them to complete before
    allowing the server to stop, with a configurable timeout.
    """

    def __init__(self, timeout: float = 30.0) -> None:
        self._shutdown_event = threading.Event()
        self._timeout = timeout
        self._active_count = 0
        self._lock = threading.Lock()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown_event.is_set()

    def request_shutdown(self) -> None:
        """Signal that shutdown has been requested."""
        logger.info("Graceful shutdown requested")
        self._shutdown_event.set()

    def enter_request(self) -> bool:
        """Register an incoming request. Returns False if shutting down."""
        if self._shutdown_event.is_set():
            return False
        with self._lock:
            self._active_count += 1
        return True

    def exit_request(self) -> None:
        with self._lock:
            self._active_count = max(0, self._active_count - 1)

    def wait_for_completion(self) -> bool:
        """Wait for active requests to complete.

        Returns True if all requests completed, False if timed out.
        """
        deadline = time.time() + self._timeout
        while time.time() < deadline:
            with self._lock:
                if self._active_count == 0:
                    logger.info("All active requests completed")
                    return True
                count = self._active_count
            logger.info("Waiting for %d active request(s) to complete...", count)
            time.sleep(0.5)
        logger.warning(
            "Shutdown timeout after %.1fs with %d active requests",
            self._timeout,
            self._active_count,
        )
        return False


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _percentile(sorted_data: list, pct: float) -> float:
    """Calculate percentile from pre-sorted list."""
    if not sorted_data:
        return 0.0
    idx = int(len(sorted_data) * pct / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def _format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        m, s = divmod(int(seconds), 60)
        return f"{m}m {s}s"
    h, remainder = divmod(int(seconds), 3600)
    m, s = divmod(remainder, 60)
    if h < 24:
        return f"{h}h {m}m"
    d, h = divmod(h, 24)
    return f"{d}d {h}h {m}m"


def check_disk_health() -> Dict[str, Any]:
    """Check disk free space and take auto-recovery actions if low.

    Thresholds:
        < 500 MB free: auto-rotate/truncate largest log files in data/
        < 100 MB free: clear all disk cache files in data/api_cache/

    Returns dict with status, free space, and any actions taken.
    """
    actions: List[str] = []
    try:
        usage = shutil.disk_usage(str(DATA_DIR))
        free_mb = usage.free / (1024 * 1024)
        total_mb = usage.total / (1024 * 1024)
        used_pct = round((usage.used / usage.total) * 100, 1) if usage.total else 0

        status = "ok"
        if free_mb < 500:
            status = "warning"
            # Auto-rotate: truncate largest .log / .json files in data/
            try:
                log_files = sorted(
                    (f for f in DATA_DIR.glob("*.log") if f.is_file()),
                    key=lambda f: f.stat().st_size,
                    reverse=True,
                )
                for lf in log_files[:3]:  # truncate top 3 largest
                    size_before = lf.stat().st_size
                    if size_before > 1024 * 1024:  # only if > 1MB
                        lf.write_text("")  # truncate
                        actions.append(f"truncated {lf.name} ({size_before // 1024}KB)")
            except Exception as e:
                logger.debug("Disk heal: log rotation failed: %s", e)

        if free_mb < 100:
            status = "critical"
            # Clear all disk cache files
            try:
                if CACHE_DIR.exists():
                    removed = 0
                    for cf in CACHE_DIR.glob("*.json"):
                        try:
                            cf.unlink()
                            removed += 1
                        except OSError:
                            pass
                    if removed:
                        actions.append(f"cleared {removed} cache files from api_cache/")
            except Exception as e:
                logger.debug("Disk heal: cache clear failed: %s", e)

        if actions:
            logger.warning(
                "Disk health recovery (free=%.0fMB): %s", free_mb, "; ".join(actions)
            )

        return {
            "status": status,
            "free_mb": round(free_mb, 1),
            "total_mb": round(total_mb, 1),
            "used_percent": used_pct,
            "actions_taken": actions,
        }
    except Exception as e:
        return {"status": "unknown", "detail": str(e), "actions_taken": []}


def _get_memory_usage() -> Dict[str, Any]:
    """Get process memory usage using OS-specific methods."""
    result: Dict[str, Any] = {}

    # Try /proc/self/status (Linux)
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_kb = int(line.split()[1])
                    result["rss_mb"] = round(rss_kb / 1024, 1)
                elif line.startswith("VmSize:"):
                    vm_kb = int(line.split()[1])
                    result["virtual_mb"] = round(vm_kb / 1024, 1)
        if result:
            return result
    except (FileNotFoundError, PermissionError, ValueError):
        pass

    # Try resource module (Unix)
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        # maxrss is in KB on Linux, bytes on macOS
        rss = usage.ru_maxrss
        if sys.platform == "darwin":
            rss = rss / 1024  # bytes to KB on macOS
        result["rss_mb"] = round(rss / 1024, 1)
        return result
    except (ImportError, AttributeError):
        pass

    # Fallback: use gc stats
    gc_stats = gc.get_stats()
    result["rss_mb"] = -1  # Unknown
    result["gc_collections"] = sum(s.get("collections") or 0 for s in gc_stats)
    result["gc_collected"] = sum(s.get("collected") or 0 for s in gc_stats)
    return result


def get_system_info() -> Dict[str, Any]:
    """Return system information for diagnostics."""
    return {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "pid": os.getpid(),
        "thread_count": threading.active_count(),
        "cpu_count": os.cpu_count(),
        "cwd": os.getcwd(),
    }


# ---------------------------------------------------------------------------
# Audit Trail Logger
# ---------------------------------------------------------------------------


class AuditLogger:
    """Records data transformation decisions for traceability.

    Every significant decision in the media plan generation pipeline
    (input validation, standardization, enrichment source selection,
    budget allocation, etc.) is logged with inputs, outputs, and rationale.

    Audit entries are persisted to a rolling JSON file and queryable by
    request_id through the admin API.

    Usage::

        audit = AuditLogger.instance()
        audit.log_decision(
            request_id="a1b2c3d4e5f6",
            stage="standardization",
            decision="industry_normalized",
            inputs={"raw": "Information Technology"},
            outputs={"canonical": "tech_engineering"},
            rationale="Matched alias 'Information Technology' -> 'tech_engineering'"
        )
    """

    _instance: Optional["AuditLogger"] = None
    _lock = threading.Lock()
    _MAX_ENTRIES = 500
    _AUDIT_FILE = "audit_log.json"

    def __new__(cls) -> "AuditLogger":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._initialized = False
                    cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._entries: deque = deque(maxlen=self._MAX_ENTRIES)
        self._write_lock = threading.Lock()
        self._audit_path = PERSISTENT_DIR / self._AUDIT_FILE
        self._load_existing()

    @classmethod
    def instance(cls) -> "AuditLogger":
        return cls()

    def _load_existing(self) -> None:
        """Load existing audit entries from disk on startup."""
        try:
            if self._audit_path.exists():
                with open(self._audit_path, "r") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for entry in data[-self._MAX_ENTRIES :]:
                        self._entries.append(entry)
        except (json.JSONDecodeError, OSError, TypeError):
            pass  # Start fresh if file is corrupted

    def log_decision(
        self,
        request_id: str = "",
        stage: str = "",
        decision: str = "",
        inputs: Optional[Dict[str, Any]] = None,
        outputs: Optional[Dict[str, Any]] = None,
        rationale: str = "",
    ) -> None:
        """Record a decision in the audit trail.

        Args:
            request_id: The X-Request-ID for this request (from thread-local if empty).
            stage: Pipeline stage (input_validation, standardization, enrichment,
                   synthesis, budget_allocation, generation).
            decision: Short decision identifier (e.g., "industry_normalized",
                      "api_fallback_used", "collar_classified").
            inputs: Input data that led to the decision.
            outputs: Output/result of the decision.
            rationale: Human-readable explanation of why this decision was made.
        """
        rid = request_id or get_request_id()
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": rid,
            "stage": stage,
            "decision": decision,
            "inputs": _safe_serialize(inputs) if inputs else {},
            "outputs": _safe_serialize(outputs) if outputs else {},
            "rationale": rationale,
        }
        self._entries.append(entry)

        # Async persist (non-blocking)
        t = threading.Thread(target=self._persist, daemon=True)
        t.start()

    def get_by_request_id(self, request_id: str) -> List[Dict[str, Any]]:
        """Retrieve all audit entries for a specific request."""
        return [e for e in self._entries if e.get("request_id") == request_id]

    def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return most recent audit entries."""
        entries = list(self._entries)
        return entries[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Return audit trail statistics."""
        entries = list(self._entries)
        stages: Dict[str, int] = defaultdict(int)
        for e in entries:
            stages[e.get("stage", "unknown")] += 1
        return {
            "total_entries": len(entries),
            "max_entries": self._MAX_ENTRIES,
            "stages": dict(stages),
            "oldest": entries[0]["timestamp"] if entries else None,
            "newest": entries[-1]["timestamp"] if entries else None,
        }

    def _persist(self) -> None:
        """Write audit log to disk (called in background thread)."""
        with self._write_lock:
            try:
                os.makedirs(self._audit_path.parent, exist_ok=True)
                entries = list(self._entries)
                with open(self._audit_path, "w") as f:
                    json.dump(entries, f, default=str, ensure_ascii=False)
            except (OSError, TypeError) as e:
                # Never crash on audit write failure
                pass


def _safe_serialize(obj: Any, max_depth: int = 3) -> Any:
    """Safely serialize an object for JSON, truncating large values."""
    if max_depth <= 0:
        return str(obj)[:200] if obj is not None else None
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        return obj[:500] if len(obj) > 500 else obj
    if isinstance(obj, (list, tuple)):
        if len(obj) > 20:
            return [_safe_serialize(v, max_depth - 1) for v in obj[:20]] + [
                f"... ({len(obj)} total)"
            ]
        return [_safe_serialize(v, max_depth - 1) for v in obj]
    if isinstance(obj, dict):
        if len(obj) > 30:
            items = list(obj.items())[:30]
            result = {k: _safe_serialize(v, max_depth - 1) for k, v in items}
            result["__truncated__"] = f"{len(obj)} total keys"
            return result
        return {k: _safe_serialize(v, max_depth - 1) for k, v in obj.items()}
    return str(obj)[:200]
