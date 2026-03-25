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

VERSION = "4.0.0"
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
# Request Span Tracking
# ---------------------------------------------------------------------------


class RequestSpan:
    """Lightweight span for request-level tracing."""

    __slots__ = ("name", "start_ts", "end_ts", "metadata")

    def __init__(self, name: str) -> None:
        self.name = name
        self.start_ts = time.monotonic()
        self.end_ts: Optional[float] = None
        self.metadata: Dict[str, Any] = {}

    def end(self, **meta: Any) -> None:
        """End the span and attach optional metadata."""
        self.end_ts = time.monotonic()
        self.metadata.update(meta)

    @property
    def duration_ms(self) -> float:
        """Return span duration in milliseconds."""
        if self.end_ts is None:
            return (time.monotonic() - self.start_ts) * 1000
        return (self.end_ts - self.start_ts) * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Return a serializable representation of the span."""
        return {
            "name": self.name,
            "duration_ms": round(self.duration_ms, 1),
            "metadata": self.metadata,
        }


_request_spans = threading.local()


def start_span(name: str) -> RequestSpan:
    """Start a new span for the current request."""
    span = RequestSpan(name)
    if not hasattr(_request_spans, "spans"):
        _request_spans.spans = []
    _request_spans.spans.append(span)
    return span


def get_request_spans() -> List[Dict[str, Any]]:
    """Get all spans for current request."""
    return [s.to_dict() for s in getattr(_request_spans, "spans", [])]


def clear_request_spans() -> None:
    """Clear spans for current request."""
    _request_spans.spans = []


# ---------------------------------------------------------------------------
# Structured JSON Log Formatter
# ---------------------------------------------------------------------------


class StructuredJsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects.

    Each log entry includes: timestamp, level, logger name, module, function,
    message, request_id (from thread-local context), latency_ms, status_code,
    and any extra fields.

    Compatible with Grafana Loki label extraction (flat top-level keys).

    Example output::

        {"ts":"2025-03-09T14:23:01.123Z","level":"INFO","logger":"nova","module":"app","function":"do_GET","msg":"Request handled","request_id":"a1b2c3d4e5f6","latency_ms":12.3,"status_code":200}
    """

    def format(self, record: logging.LogRecord) -> str:
        entry: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "module": record.module or "",
            "function": record.funcName or "",
            "msg": record.getMessage(),
        }

        # Inject request context from thread-local
        rid = get_request_id()
        if rid:
            entry["request_id"] = rid
            latency = round(get_request_elapsed_ms(), 1)
            entry["latency_ms"] = latency
            # Legacy alias for backward compat with Grafana dashboards
            entry["elapsed_ms"] = latency

        # Include status_code if set via extra kwarg
        _status = getattr(record, "status_code", None)
        if _status is not None:
            entry["status_code"] = _status

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
            "status_code",
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
        "target": 45000,  # 45 seconds (tool-use queries 20-30s + cold-start can spike to 40s)
        "description": "99th percentile chat latency",
        "endpoint": "/api/chat",
        "grace_after_deploy_s": 300,  # Exclude first 5 min after deploy from SLO
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
# Module-Level SLO Definitions (3-Module Architecture v4.0)
# ---------------------------------------------------------------------------

MODULE_NAMES = ("command_center", "intelligence_hub", "nova_ai")

MODULE_SLO_TARGETS: Dict[str, Dict[str, Any]] = {
    "command_center": {
        "p95_latency_ms": 10000,
        "error_rate_pct": 5.0,
        "availability_pct": 99.0,
        "description": "Campaign planning & execution module",
    },
    "intelligence_hub": {
        "p95_latency_ms": 15000,
        "error_rate_pct": 5.0,
        "availability_pct": 99.0,
        "description": "Market/competitive/talent research module (web scraping)",
    },
    "nova_ai": {
        "p95_latency_ms": 15000,
        "error_rate_pct": 5.0,
        "availability_pct": 99.0,
        "description": "Persistent chat assistant module (LLM + tool-use loops)",
    },
}

# Route -> module mapping for automatic classification
_ROUTE_MODULE_MAP: Dict[str, str] = {
    # Command Center routes
    "/api/generate": "command_center",
    "/api/quick-plan": "command_center",
    "/api/budget": "command_center",
    "/api/deck": "command_center",
    "/api/export": "command_center",
    "/api/sheets": "command_center",
    "/fragment/command-center": "command_center",
    # Intelligence Hub routes
    "/api/research": "intelligence_hub",
    "/api/competitive": "intelligence_hub",
    "/api/market": "intelligence_hub",
    "/api/talent": "intelligence_hub",
    "/api/scrape": "intelligence_hub",
    "/api/enrich": "intelligence_hub",
    "/fragment/intelligence-hub": "intelligence_hub",
    # Nova AI routes
    "/api/chat": "nova_ai",
    "/api/nova": "nova_ai",
    "/api/conversations": "nova_ai",
    "/api/voice": "nova_ai",
    "/api/tts": "nova_ai",
    "/fragment/nova-ai": "nova_ai",
}


def classify_route_to_module(endpoint: str) -> str:
    """Classify an endpoint to its owning module.

    Args:
        endpoint: The request path (e.g., '/api/chat').

    Returns:
        Module name string, or empty string if unclassified.
    """
    if not endpoint:
        return ""
    # Exact match first
    module = _ROUTE_MODULE_MAP.get(endpoint)
    if module:
        return module
    # Prefix match
    for route_prefix, mod in _ROUTE_MODULE_MAP.items():
        if endpoint.startswith(route_prefix):
            return mod
    return ""


# ---------------------------------------------------------------------------
# Module Health Tracker (v4.0)
# ---------------------------------------------------------------------------


class ModuleHealthTracker:
    """Track per-module health metrics for the 3-module architecture.

    Each module (command_center, intelligence_hub, nova_ai) has independent
    request counts, error rates, latency percentiles, and active user counts.
    Thread-safe via a per-instance lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._modules: Dict[str, Dict[str, Any]] = {}
        for mod_name in MODULE_NAMES:
            self._modules[mod_name] = {
                "request_count": 0,
                "error_count": 0,
                "latencies": deque(maxlen=500),
                "recent_requests": deque(),
                "recent_errors": deque(),
                "active_users": set(),
                "user_last_seen": {},
                "health_score": 100.0,
                "status": "healthy",
                "degraded_since": None,
            }

    def record_request(
        self,
        module: str,
        latency_ms: float,
        is_error: bool = False,
        user_id: str = "",
    ) -> None:
        """Record a request for a specific module.

        Args:
            module: Module name (command_center, intelligence_hub, nova_ai).
            latency_ms: Request latency in milliseconds.
            is_error: Whether the request resulted in an error.
            user_id: PostHog distinct_id or session identifier.
        """
        if module not in self._modules:
            return
        now = time.time()
        with self._lock:
            m = self._modules[module]
            m["request_count"] += 1
            m["latencies"].append(latency_ms)
            m["recent_requests"].append(now)
            if is_error:
                m["error_count"] += 1
                m["recent_errors"].append(now)
            if user_id:
                m["active_users"].add(user_id)
                m["user_last_seen"][user_id] = now
            # Prune rolling windows (1 hour)
            cutoff = now - METRICS_WINDOW
            while m["recent_requests"] and m["recent_requests"][0] < cutoff:
                m["recent_requests"].popleft()
            while m["recent_errors"] and m["recent_errors"][0] < cutoff:
                m["recent_errors"].popleft()
            # Prune stale users (inactive > 30 minutes)
            user_cutoff = now - 1800
            stale_users = [
                uid for uid, ts in m["user_last_seen"].items() if ts < user_cutoff
            ]
            for uid in stale_users:
                m["active_users"].discard(uid)
                del m["user_last_seen"][uid]

    def compute_health_scores(self) -> Dict[str, Dict[str, Any]]:
        """Compute health scores and SLO compliance for all modules.

        Returns:
            Dict mapping module name to health data including score,
            status, SLO compliance, and metrics.
        """
        result: Dict[str, Dict[str, Any]] = {}
        now = time.time()

        # Check LLM router degradation for circuit breaker awareness
        llm_degradation_pct = _get_llm_degradation_pct()

        with self._lock:
            for mod_name in MODULE_NAMES:
                m = self._modules[mod_name]
                slo = MODULE_SLO_TARGETS[mod_name]

                # Calculate metrics
                window_requests = len(m["recent_requests"])
                window_errors = len(m["recent_errors"])
                error_rate = (
                    (window_errors / window_requests * 100)
                    if window_requests > 0
                    else 0.0
                )
                latencies = sorted(m["latencies"])
                p50 = _percentile(latencies, 50)
                p95 = _percentile(latencies, 95)
                p99 = _percentile(latencies, 99)
                avg_latency = (sum(latencies) / len(latencies)) if latencies else 0.0

                # SLO compliance checks
                latency_compliant = p95 <= slo["p95_latency_ms"]
                error_compliant = error_rate <= slo["error_rate_pct"]
                # Availability: percentage of non-error requests
                total = max(1, m["request_count"])
                availability = ((total - m["error_count"]) / total) * 100
                availability_compliant = availability >= slo["availability_pct"]

                # Compute health score (0-100)
                score = 100.0
                if not latency_compliant:
                    overshoot = (p95 - slo["p95_latency_ms"]) / slo["p95_latency_ms"]
                    score -= min(30, overshoot * 30)
                if not error_compliant:
                    overshoot = (error_rate - slo["error_rate_pct"]) / max(
                        0.1, slo["error_rate_pct"]
                    )
                    score -= min(40, overshoot * 40)
                if not availability_compliant:
                    deficit = slo["availability_pct"] - availability
                    score -= min(30, deficit * 10)

                # Circuit breaker awareness: Nova AI degrades if LLM router is degraded
                if mod_name == "nova_ai" and llm_degradation_pct > 50:
                    score = min(score, 50.0)

                score = max(0.0, round(score, 1))

                # Determine status
                if score >= 90:
                    status = "healthy"
                elif score >= 60:
                    status = "degraded"
                else:
                    status = "critical"

                # Track degraded_since
                prev_status = m["status"]
                if status != "healthy" and prev_status == "healthy":
                    m["degraded_since"] = datetime.now(timezone.utc).isoformat()
                elif status == "healthy":
                    m["degraded_since"] = None

                m["health_score"] = score
                m["status"] = status

                result[mod_name] = {
                    "health_score": score,
                    "status": status,
                    "degraded_since": m["degraded_since"],
                    "metrics": {
                        "request_count": m["request_count"],
                        "error_count": m["error_count"],
                        "error_rate_pct": round(error_rate, 2),
                        "active_users": len(m["active_users"]),
                        "latency_ms": {
                            "p50": round(p50, 1),
                            "p95": round(p95, 1),
                            "p99": round(p99, 1),
                            "avg": round(avg_latency, 1),
                        },
                        "window_requests": window_requests,
                    },
                    "slo": {
                        "p95_latency": {
                            "target_ms": slo["p95_latency_ms"],
                            "actual_ms": round(p95, 1),
                            "compliant": latency_compliant,
                        },
                        "error_rate": {
                            "target_pct": slo["error_rate_pct"],
                            "actual_pct": round(error_rate, 2),
                            "compliant": error_compliant,
                        },
                        "availability": {
                            "target_pct": slo["availability_pct"],
                            "actual_pct": round(availability, 2),
                            "compliant": availability_compliant,
                        },
                    },
                    "llm_degradation_flag": (
                        mod_name == "nova_ai" and llm_degradation_pct > 50
                    ),
                }

        return result

    def get_module_summary(self) -> Dict[str, Any]:
        """Return a lightweight summary of all module health for API responses."""
        scores = self.compute_health_scores()
        overall_healthy = all(v["status"] == "healthy" for v in scores.values())
        return {
            "modules": scores,
            "overall_healthy": overall_healthy,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }


def _get_llm_degradation_pct() -> float:
    """Check LLM router for percentage of degraded providers.

    Returns:
        Percentage of providers that are degraded/circuit-open (0-100).
    """
    try:
        import sys

        if "llm_router" not in sys.modules:
            return 0.0
        lr = sys.modules["llm_router"]
        states = getattr(lr, "_provider_states", {})
        if not states:
            return 0.0
        now = time.time()
        degraded = 0
        total = 0
        for pid, state in states.items():
            total += 1
            try:
                with state.lock:
                    if (
                        state.consecutive_failures >= 3
                        or state.circuit_open_until > now
                    ):
                        degraded += 1
            except (AttributeError, RuntimeError):
                continue
        return (degraded / max(1, total)) * 100
    except Exception:
        return 0.0


# Module-level tracker singleton
_module_tracker: Optional["ModuleHealthTracker"] = None
_module_tracker_lock = threading.Lock()


def get_module_tracker() -> ModuleHealthTracker:
    """Get or create the singleton ModuleHealthTracker (thread-safe)."""
    global _module_tracker
    if _module_tracker is None:
        with _module_tracker_lock:
            if _module_tracker is None:
                _module_tracker = ModuleHealthTracker()
    return _module_tracker


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
        self.total_errors: int = 0  # 5xx server errors only
        self.total_client_errors: int = (
            0  # 4xx client errors (not counted in error rate)
        )
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
        self,
        endpoint: str,
        method: str,
        status_code: int,
        latency_ms: float,
        user_id: str = "",
    ) -> None:
        """Record a completed HTTP request.

        Also feeds the module-level health tracker if the endpoint
        maps to a known module.
        """
        # Guard against None/invalid args
        endpoint = endpoint or ""
        method = method or ""
        status_code = status_code if isinstance(status_code, int) else 0
        latency_ms = latency_ms if isinstance(latency_ms, (int, float)) else 0.0

        now = time.time()
        with self._req_lock:
            self.total_requests += 1
            self._recent_requests.append(now)
            self._status_codes[status_code] += 1
            self._latencies[endpoint].append(latency_ms)

            if status_code >= 500:
                # Only 5xx server errors count toward error rate
                self.total_errors += 1
                self._recent_errors.append(now)
            elif status_code >= 400:
                # 4xx client errors tracked separately (not in error rate)
                self.total_client_errors += 1

            # Prune old entries from rolling windows
            cutoff = now - METRICS_WINDOW
            while self._recent_requests and self._recent_requests[0] < cutoff:
                self._recent_requests.popleft()
            while self._recent_errors and self._recent_errors[0] < cutoff:
                self._recent_errors.popleft()

        # Feed module tracker (outside collector lock to avoid deadlock)
        module = classify_route_to_module(endpoint)
        if module:
            try:
                tracker = get_module_tracker()
                tracker.record_request(
                    module=module,
                    latency_ms=latency_ms,
                    is_error=status_code >= 500,
                    user_id=user_id,
                )
            except Exception:
                pass  # never break request recording

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
                "total_client_errors": self.total_client_errors,
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

            # Chat P99 latency (with post-deploy grace period)
            chat_lats = sorted(self._latencies.get("/api/chat") or [])
            chat_p99 = _percentile(chat_lats, 99) if chat_lats else 0.0
            _chat_slo = SLO_TARGETS["chat_p99_ms"]
            _chat_grace = _chat_slo.get("grace_after_deploy_s", 300)
            _chat_in_grace = uptime < _chat_grace
            results["chat_p99_ms"] = {
                "target": _chat_slo["target"],
                "actual": round(chat_p99, 1),
                "compliant": _chat_in_grace or chat_p99 <= _chat_slo["target"],
                "sample_size": len(chat_lats),
                "in_grace_period": _chat_in_grace,
            }

            # Error rate -- use ROLLING WINDOW (1h) for SLO compliance,
            # not cumulative totals which include inherited pre-deploy errors
            window_req = max(1, len(self._recent_requests))
            window_err = len(self._recent_errors)
            error_rate = (window_err / window_req) * 100 if window_req > 0 else 0.0
            target_err = SLO_TARGETS["error_rate_pct"]["target"]
            # Also compute cumulative for dashboard display
            cumulative_total = max(1, self.total_requests)
            cumulative_error_rate = (self.total_errors / cumulative_total) * 100
            results["error_rate_pct"] = {
                "target": target_err,
                "actual": round(error_rate, 3),
                "compliant": error_rate <= target_err,
                "budget_remaining_pct": round(max(0, target_err - error_rate), 3),
                "window_seconds": METRICS_WINDOW,
                "cumulative_error_rate_pct": round(cumulative_error_rate, 3),
            }

            # Availability (based on uptime -- simple heuristic)
            # If we're running, we're available. Track non-5xx as available.
            total_5xx = sum(
                count for code, count in self._status_codes.items() if 500 <= code < 600
            )
            avail = (
                ((cumulative_total - total_5xx) / cumulative_total) * 100
                if cumulative_total > 0
                else 100.0
            )
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

    def compute_burn_rate(self, window_hours: int = 1) -> Dict[str, Any]:
        """Compute SLO error budget burn rate.

        A burn rate of 1.0 means consuming budget exactly at the sustainable rate.
        >1.0 means consuming faster than sustainable (alert at >2.0).

        Args:
            window_hours: Observation window in hours (unused currently, reserved
                for future multi-window support).

        Returns:
            Dict mapping SLO name to burn rate data.
        """
        slo_compliance = self.check_slo_compliance()
        slos = slo_compliance.get("slos", slo_compliance)
        results: Dict[str, Any] = {}
        for slo_name, slo_data in slos.items():
            if (
                isinstance(slo_data, dict)
                and "target" in slo_data
                and "actual" in slo_data
            ):
                target = slo_data["target"]
                current = slo_data["actual"]
                if (
                    isinstance(target, (int, float))
                    and isinstance(current, (int, float))
                    and target > 0
                ):
                    # For error rate: burn_rate = actual_error_rate / allowed_error_rate
                    allowed = 1.0 - target if target <= 1.0 else target
                    actual = (
                        current if isinstance(current, float) and current <= 1.0 else 0
                    )
                    burn_rate = actual / allowed if allowed > 0 else 0
                    results[slo_name] = {
                        "burn_rate": round(burn_rate, 2),
                        "budget_remaining_pct": round(max(0, (1 - burn_rate)) * 100, 1),
                        "status": (
                            "critical"
                            if burn_rate > 5
                            else "warning" if burn_rate > 2 else "ok"
                        ),
                    }
        return results

    def check_anomalies(self) -> List[Dict[str, Any]]:
        """Detect anomalies by comparing current metrics to rolling baselines.

        Uses a 2-sigma threshold over a 60-sample rolling window.

        Returns:
            List of anomaly dicts with metric name, current value, baseline
            mean/std, and deviation in sigma units.
        """
        import statistics as _stats

        anomalies: List[Dict[str, Any]] = []
        metrics = self.get_metrics()
        if not isinstance(metrics, dict):
            return anomalies
        # Track baselines (stored as instance attribute)
        if not hasattr(self, "_baselines"):
            self._baselines: Dict[str, Dict[str, Any]] = {}
        for key in ["avg_latency_ms", "error_rate_pct", "requests_per_minute"]:
            current: float = 0.0
            if key == "avg_latency_ms":
                latency_data = metrics.get("latency_ms", {})
                current = (
                    latency_data.get("p50", 0)
                    if isinstance(latency_data, dict)
                    else 0.0
                )
            else:
                current = metrics.get(key, 0)
            if not isinstance(current, (int, float)):
                continue
            if key not in self._baselines:
                self._baselines[key] = {"values": [], "mean": current, "std": 0.0}
            bl = self._baselines[key]
            bl["values"].append(current)
            if len(bl["values"]) > 60:  # 1 hour of data at 1/min
                bl["values"] = bl["values"][-60:]
            if len(bl["values"]) >= 10:
                bl["mean"] = _stats.mean(bl["values"])
                raw_std = _stats.stdev(bl["values"]) if len(bl["values"]) > 1 else 0.0
                # Apply minimum std floors to prevent false positives on stable metrics
                # Latency: min 50ms floor (sub-ms jitter is normal)
                # RPM: min 2.0 floor (traffic naturally fluctuates)
                # Error rate: min 0.5% floor
                _MIN_STD_FLOORS: Dict[str, float] = {
                    "avg_latency_ms": 50.0,
                    "requests_per_minute": 2.0,
                    "error_rate_pct": 0.5,
                }
                bl["std"] = max(raw_std, _MIN_STD_FLOORS.get(key, 0.1))
                if abs(current - bl["mean"]) > 3 * bl["std"]:
                    anomalies.append(
                        {
                            "metric": key,
                            "current": current,
                            "baseline_mean": round(bl["mean"], 2),
                            "baseline_std": round(bl["std"], 2),
                            "deviation_sigma": round(
                                abs(current - bl["mean"]) / bl["std"], 1
                            ),
                        }
                    )
        return anomalies

    def export_prometheus(self) -> str:
        """Export metrics in Prometheus exposition format.

        Returns:
            Multi-line string in Prometheus text format.
        """
        metrics = self.get_metrics()
        latency = metrics.get("latency_ms", {})
        lines: List[str] = [
            "# HELP nova_request_latency_ms Average request latency",
            "# TYPE nova_request_latency_ms gauge",
            f"nova_request_latency_ms {{quantile=\"0.5\"}} {latency.get('p50', 0)}",
            f"nova_request_latency_ms {{quantile=\"0.95\"}} {latency.get('p95', 0)}",
            f"nova_request_latency_ms {{quantile=\"0.99\"}} {latency.get('p99', 0)}",
            "# HELP nova_error_rate Current error rate percentage",
            "# TYPE nova_error_rate gauge",
            f"nova_error_rate {metrics.get('error_rate_pct', 0)}",
            "# HELP nova_rpm Requests per minute",
            "# TYPE nova_rpm gauge",
            f"nova_rpm {metrics.get('requests_per_minute', 0)}",
            "# HELP nova_total_requests Total requests since startup",
            "# TYPE nova_total_requests counter",
            f"nova_total_requests {metrics.get('total_requests', 0)}",
            "# HELP nova_total_errors Total errors since startup",
            "# TYPE nova_total_errors counter",
            f"nova_total_errors {metrics.get('total_errors', 0)}",
            "# HELP nova_active_requests Currently active requests",
            "# TYPE nova_active_requests gauge",
            f"nova_active_requests {metrics.get('active_requests', 0)}",
        ]
        return "\n".join(lines) + "\n"


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

    # Grace period (seconds) after daemon start during which the health
    # endpoint should not report a persistence warning.  The daemon does
    # a 10-second warm-up sleep then one save, so 30s covers even a slow
    # first Supabase round-trip.
    _STARTUP_GRACE_SEC: float = 30.0

    def __init__(self, collector: MetricsCollector) -> None:
        self._collector = collector
        self._lock = threading.Lock()
        self._enabled: bool = False
        self._last_save_ok: bool = False
        self._last_save_ts: float = 0.0
        self._first_save_attempted: bool = False
        self._daemon_started_ts: float = 0.0
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

    @property
    def is_within_startup_grace(self) -> bool:
        """Return True if the daemon is still within the cold-start grace window.

        During this window the health endpoint should not report a warning
        because the first save has not had time to complete yet.
        """
        if not self._enabled:
            return False
        if self._first_save_attempted:
            return False  # grace period ends once first save is attempted
        if self._daemon_started_ts <= 0:
            return False  # daemon not started yet
        return (time.time() - self._daemon_started_ts) < self._STARTUP_GRACE_SEC

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

        The table uses a row-per-metric schema:
        ``metric_key TEXT PK, metric_value INT, updated_at TIMESTAMPTZ``.

        Returns True if counters were restored, False otherwise.
        """
        if not self._enabled:
            return False
        try:
            url = f"{self._rest_url()}?select=metric_key,metric_value"
            req = urllib.request.Request(
                url, headers=self._build_headers(), method="GET"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode("utf-8")
            rows = json.loads(body)
            if not rows:
                logger.info("No existing metrics snapshot in Supabase")
                return False
            data: Dict[str, int] = {}
            for row in rows:
                mk = row.get("metric_key") or ""
                mv = row.get("metric_value") or 0
                if mk and isinstance(mv, (int, float)):
                    data[mk] = int(mv)
            if not data:
                return False
            self._apply_snapshot(data)
            logger.info(
                "Restored metrics from Supabase: %d total_requests, %d total_errors",
                data.get("total_requests", 0),
                data.get("total_errors", 0),
            )
            return True
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            logger.debug("Failed to load metrics from Supabase (transient): %s", e)
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

        Stores each counter as a separate row keyed by ``metric_key``
        with the current value in ``metric_value``.

        Returns True on success, False on failure.
        """
        if not self._enabled:
            return False
        try:
            rows = self._build_rows()
            payload = json.dumps(rows, default=str)
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
            logger.debug("Metrics snapshot saved to Supabase (%d rows)", len(rows))
            return True
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            with self._lock:
                self._last_save_ok = False
            logger.debug("Metrics save to Supabase failed (transient): %s", e)
            return False
        except (TypeError, ValueError) as e:
            with self._lock:
                self._last_save_ok = False
            logger.warning("Metrics serialization failed: %s", e)
            return False

    def _build_rows(self) -> list:
        """Build row-per-metric payload for Supabase upsert."""
        now = datetime.now(timezone.utc).isoformat()
        rows: list = []
        with self._collector._req_lock:
            for field in _PERSIST_COUNTERS:
                val = getattr(self._collector, field, 0)
                rows.append(
                    {
                        "metric_key": field,
                        "metric_value": (
                            int(val) if isinstance(val, (int, float)) else 0
                        ),
                        "updated_at": now,
                    }
                )
        return rows

    # -- Background save loop ------------------------------------------------

    def start_background_save(self) -> None:
        """Start a daemon thread that saves metrics every 5 minutes."""
        if not self._enabled:
            return
        if self._save_thread is not None and self._save_thread.is_alive():
            return  # already running
        self._daemon_started_ts = time.time()
        self._save_thread = threading.Thread(
            target=self._save_loop, name="metrics-persist", daemon=True
        )
        self._save_thread.start()
        logger.info(
            "Metrics persistence background thread started (interval=%ds, grace=%ds)",
            _PERSIST_SAVE_INTERVAL_SEC,
            int(self._STARTUP_GRACE_SEC),
        )

    def _save_loop(self) -> None:
        """Periodically save metrics until the process exits.

        Performs an immediate first save (after a short 10s warm-up) so that
        ``metrics_persisted`` becomes True quickly, then continues at the
        normal 5-minute interval.
        """
        time.sleep(10)  # short warm-up to let startup finish
        self._first_save_attempted = True
        try:
            self.save()
        except Exception as e:
            logger.error(
                "Unexpected error in initial metrics save: %s", e, exc_info=True
            )
        while True:
            time.sleep(_PERSIST_SAVE_INTERVAL_SEC)
            try:
                self.save()
            except Exception as e:
                logger.warning(
                    "Metrics persistence loop error (non-fatal): %s",
                    e,
                    exc_info=True,
                )

    # -- Status ---------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Return persistence status for health endpoints."""
        with self._lock:
            within_grace = self.is_within_startup_grace
            return {
                "metrics_persisted": self.is_persisted or within_grace,
                "persistence_enabled": self._enabled,
                "last_save_ok": self._last_save_ok,
                "startup_grace_active": within_grace,
                "first_save_attempted": self._first_save_attempted,
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
        result["metrics_persisted"] = p.is_persisted or p.is_within_startup_grace
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
        "metrics_persisted": (
            (p.is_persisted or p.is_within_startup_grace) if p is not None else False
        ),
    }
    return result


# ---------------------------------------------------------------------------
# Structured Logging
# ---------------------------------------------------------------------------


def configure_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure structured logging for production.

    In production (json_format=True), emits single-line JSON with fields:
    timestamp, level, module, function, request_id, latency_ms, status_code.

    In development (json_format=False or LOG_FORMAT=text), uses human-readable
    format with module and function name for easy debugging.

    Auto-detects dev mode via RENDER_ENV or LOG_FORMAT env vars.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        json_format: If True, use JSON formatter for machine-readable logs.
                     If False, use human-readable format (useful for local dev).
                     Overridden by LOG_FORMAT=text env var.
    """
    # Allow env var override: LOG_FORMAT=text forces human-readable
    env_format = os.environ.get("LOG_FORMAT", "").lower()
    if env_format == "text":
        json_format = False
    elif env_format == "json":
        json_format = True

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
            fmt="%(asctime)s [%(levelname)s] %(name)s.%(module)s.%(funcName)s: %(message)s",
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
                self._audit_path.parent.mkdir(parents=True, exist_ok=True)
                entries = list(self._entries)
                with self._audit_path.open("w") as f:
                    json.dump(entries, f, default=str, ensure_ascii=False)
            except (OSError, TypeError) as e:
                logger.warning("Audit persist failed: %s", e)


# ---------------------------------------------------------------------------
# Platform Observability Endpoint (v4.0)
# ---------------------------------------------------------------------------


def get_platform_observability() -> Dict[str, Any]:
    """Return comprehensive platform observability data for /api/observability/platform.

    Aggregates:
    - Module health scores and SLO compliance (from ModuleHealthTracker)
    - Self-healing stats per module (from sentry_integration)
    - Dependency matrix status
    - LLM Router v4 stats (provider health, rate limits, cache)
    - Web scraper tier usage distribution
    - Data API cross-fallback activation count
    - PostHog event counts by type

    Returns:
        Dict with all observability data, suitable for JSON serialization.
    """
    result: Dict[str, Any] = {
        "version": VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(time.time() - _START_TIME, 1),
    }

    # 1. Module health scores
    try:
        tracker = get_module_tracker()
        result["module_health"] = tracker.get_module_summary()
    except Exception as e:
        result["module_health"] = {"error": str(e)}

    # 2. Self-healing stats
    try:
        from sentry_integration import get_sentry_status, get_module_heal_stats

        sentry_status = get_sentry_status()
        result["self_healing"] = {
            "module_stats": get_module_heal_stats(),
            "fixes_this_hour": sentry_status.get("stats", {}).get("fixes_this_hour", 0),
            "known_patterns": sentry_status.get("known_patterns", 0),
            "recent_heals": sentry_status.get("recent_heals", [])[-5:],
        }
    except ImportError:
        result["self_healing"] = {"error": "sentry_integration not available"}
    except Exception as e:
        result["self_healing"] = {"error": str(e)}

    # 3. Dependency matrix status
    try:
        dep_status: Dict[str, Dict[str, str]] = {}
        import sys as _sys

        dep_checks = {
            "command_center": {
                "llm_router": "llm_router" in _sys.modules,
                "data_orchestrator": "data_orchestrator" in _sys.modules,
                "supabase": bool(os.environ.get("SUPABASE_URL")),
                "knowledge_base": (
                    DATA_DIR / "recruitment_industry_knowledge.json"
                ).exists(),
            },
            "intelligence_hub": {
                "web_scraper_router": "web_scraper_router" in _sys.modules,
                "search_clients": bool(
                    os.environ.get("TAVILY_API_KEY") or os.environ.get("SERPER_API_KEY")
                ),
                "data_apis": "api_integrations" in _sys.modules,
                "supabase": bool(os.environ.get("SUPABASE_URL")),
            },
            "nova_ai": {
                "llm_router_streaming": "llm_router" in _sys.modules,
                "supabase": bool(os.environ.get("SUPABASE_URL")),
                "elevenlabs": bool(os.environ.get("ELEVENLABS_API_KEY")),
                "knowledge_base": (
                    DATA_DIR / "recruitment_industry_knowledge.json"
                ).exists(),
            },
        }
        for mod_name, deps in dep_checks.items():
            dep_status[mod_name] = {}
            for dep_name, available in deps.items():
                dep_status[mod_name][dep_name] = (
                    "available" if available else "unavailable"
                )
        result["dependency_matrix"] = dep_status
    except Exception as e:
        result["dependency_matrix"] = {"error": str(e)}

    # 4. LLM Router v4 stats
    try:
        import sys as _sys

        if "llm_router" in _sys.modules:
            lr = _sys.modules["llm_router"]
            provider_health: Dict[str, Any] = {}
            states = getattr(lr, "_provider_states", {})
            now = time.time()
            for pid, state in states.items():
                try:
                    with state.lock:
                        provider_health[pid] = {
                            "consecutive_failures": state.consecutive_failures,
                            "circuit_open": state.circuit_open_until > now,
                            "health_score": getattr(state, "health_score", 0),
                        }
                except (AttributeError, RuntimeError):
                    provider_health[pid] = {"status": "unknown"}

            # Response cache stats
            cache = getattr(lr, "_response_cache", {})
            cache_size = len(cache) if hasattr(cache, "__len__") else 0

            result["llm_router"] = {
                "provider_count": len(states),
                "provider_health": provider_health,
                "degradation_pct": round(_get_llm_degradation_pct(), 1),
                "response_cache_size": cache_size,
            }
        else:
            result["llm_router"] = {"status": "not_loaded"}
    except Exception as e:
        result["llm_router"] = {"error": str(e)}

    # 5. Web scraper tier usage
    try:
        import sys as _sys

        if "web_scraper_router" in _sys.modules:
            wsr = _sys.modules["web_scraper_router"]
            tier_stats = getattr(wsr, "_tier_usage_counts", {})
            if callable(getattr(wsr, "get_tier_stats", None)):
                tier_stats = wsr.get_tier_stats()
            result["web_scraper"] = {
                "tier_usage": dict(tier_stats) if tier_stats else {},
                "preferred_tier": getattr(wsr, "_preferred_tier", 0),
            }
        else:
            result["web_scraper"] = {"status": "not_loaded"}
    except Exception as e:
        result["web_scraper"] = {"error": str(e)}

    # 6. Data API cross-fallback count
    try:
        import sys as _sys

        if "data_orchestrator" in _sys.modules:
            do = _sys.modules["data_orchestrator"]
            fallback_telemetry = {}
            if callable(getattr(do, "get_fallback_telemetry", None)):
                fallback_telemetry = do.get_fallback_telemetry()
            cache_stats = {}
            if callable(getattr(do, "get_cache_stats", None)):
                cache_stats = do.get_cache_stats()
            result["data_api"] = {
                "fallback_telemetry": fallback_telemetry,
                "cache_stats": cache_stats,
            }
        else:
            result["data_api"] = {"status": "not_loaded"}
    except Exception as e:
        result["data_api"] = {"error": str(e)}

    # 7. PostHog event counts (if available)
    try:
        posthog_key = os.environ.get("POSTHOG_API_KEY") or ""
        result["posthog"] = {
            "configured": bool(posthog_key),
            "note": "Event counts available via PostHog dashboard API",
        }
    except Exception as e:
        result["posthog"] = {"error": str(e)}

    # 8. Global metrics summary
    try:
        collector = MetricsCollector()
        metrics = collector.get_metrics()
        result["global_metrics"] = {
            "total_requests": metrics.get("total_requests", 0),
            "total_errors": metrics.get("total_errors", 0),
            "error_rate_pct": metrics.get("error_rate_pct", 0),
            "rpm": metrics.get("requests_per_minute", 0),
            "latency_p95_ms": metrics.get("latency_ms", {}).get("p95", 0),
            "chat_routing": metrics.get("chat_routing", {}),
        }
    except Exception as e:
        result["global_metrics"] = {"error": str(e)}

    return result


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


# ═══════════════════════════════════════════════════════════════════════════════
# MONITORING-TO-ALERTING BRIDGE (Phase 6: closes the observability loop)
# ═══════════════════════════════════════════════════════════════════════════════


class MonitoringAlertBridge:
    """Bridges monitoring metrics to alert_manager for proactive alerting.

    Checks SLO compliance every 60 seconds and fires alerts when:
    - Error rate exceeds SLO target for any module
    - Latency p99 exceeds SLO target
    - Module health score drops below threshold
    """

    def __init__(self, check_interval: int = 60) -> None:
        self._interval = check_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._alert_cooldowns: dict[str, float] = {}  # alert_key -> last_fired_ts
        self._cooldown_period = 300  # 5 min between same alerts
        self._lock = threading.Lock()
        self._last_known_version: str = VERSION  # deploy detection

    def start(self) -> None:
        """Start the alerting bridge background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="monitor-alert-bridge"
        )
        self._thread.start()
        logger.info("[MonitorAlertBridge] Started (interval: %ds)", self._interval)

    def stop(self) -> None:
        """Stop the alerting bridge."""
        self._running = False

    def _should_alert(self, key: str) -> bool:
        """Check if we should fire this alert (respects cooldown)."""
        with self._lock:
            last_fired = self._alert_cooldowns.get(key, 0)
            if time.time() - last_fired < self._cooldown_period:
                return False
            self._alert_cooldowns[key] = time.time()
            return True

    def _fire_alert(self, severity: str, subject: str, body: str) -> None:
        """Send alert via alert_manager and email_alerts for maximum reliability.

        Tries both alert_manager (4-tier: Resend, SMTP, Slack, logfile)
        and email_alerts (dedicated Resend) in parallel. Either path
        succeeding is sufficient.
        """
        delivered = False
        # Path 1: alert_manager (4-tier fallback chain)
        try:
            if "alert_manager" in sys.modules:
                am = sys.modules["alert_manager"]
                if hasattr(am, "send_alert"):
                    am.send_alert(subject=subject, body=body, severity=severity.lower())
                    delivered = True
        except Exception as e:
            logger.warning("[MonitorAlertBridge] alert_manager delivery failed: %s", e)
        # Path 2: email_alerts (dedicated Resend with dedup/backoff)
        try:
            from email_alerts import send_error_alert

            severity_map = {
                "CRITICAL": "critical",
                "WARNING": "warning",
                "INFO": "info",
            }
            send_error_alert(
                error_type=subject,
                error_message=body,
                context={"severity": severity_map.get(severity, "warning")},
            )
            delivered = True
        except ImportError:
            logger.debug("[MonitorAlertBridge] email_alerts not importable")
        except Exception as e:
            logger.warning("[MonitorAlertBridge] email_alerts delivery failed: %s", e)
        if not delivered:
            # Fallback: log it
            logger.warning(
                "[MonitorAlertBridge] ALERT (%s): %s -- %s", severity, subject, body
            )

    def _check_cycle(self) -> None:
        """Run one check cycle across all modules."""
        try:
            # Check each module's health via ModuleHealthTracker
            tracker = get_module_tracker()
            module_scores = tracker.compute_health_scores()

            for module_id, module_data in module_scores.items():
                score = module_data.get("health_score", 100)
                status = module_data.get("status", "healthy")

                # Critical: health score below 40
                if score < 40:
                    alert_key = f"health_critical_{module_id}"
                    if self._should_alert(alert_key):
                        self._fire_alert(
                            "CRITICAL",
                            f"[Nova] CRITICAL: {module_id} health score {score}/100",
                            f"Module {module_id} health has dropped to {score}/100 "
                            f"(status: {status}). Immediate investigation required.",
                        )

                # Warning: health score below 70
                elif score < 70:
                    alert_key = f"health_degraded_{module_id}"
                    if self._should_alert(alert_key):
                        self._fire_alert(
                            "WARNING",
                            f"[Nova] DEGRADED: {module_id} health score {score}/100",
                            f"Module {module_id} health has degraded to {score}/100 "
                            f"(status: {status}). Monitor closely.",
                        )

            # Check SLO compliance
            collector = MetricsCollector()
            slo_status = collector.check_slo_compliance()
            slos = slo_status.get("slos", {})
            for slo_name, slo_data in slos.items():
                if isinstance(slo_data, dict) and not slo_data.get("compliant", True):
                    alert_key = f"slo_violation_{slo_name}"
                    if self._should_alert(alert_key):
                        current = slo_data.get("actual", "unknown")
                        target = slo_data.get("target", "unknown")
                        self._fire_alert(
                            "WARNING",
                            f"[Nova] SLO Violation: {slo_name}",
                            f"SLO '{slo_name}' is non-compliant. "
                            f"Current: {current}, Target: {target}.",
                        )

            # Check error rate across all endpoints (error_rate is a percentage)
            metrics = collector.get_metrics()
            if isinstance(metrics, dict):
                error_rate_pct = metrics.get("error_rate_pct", 0)
                if error_rate_pct > 10:  # >10% error rate
                    alert_key = "global_error_rate"
                    if self._should_alert(alert_key):
                        self._fire_alert(
                            "CRITICAL",
                            f"[Nova] High error rate: {error_rate_pct:.1f}%",
                            f"Global error rate is {error_rate_pct:.1f}% "
                            f"(threshold: 10%). Check /api/health/integrations "
                            f"for failing services.",
                        )
                elif error_rate_pct > 5:  # >5% error rate
                    alert_key = "elevated_error_rate"
                    if self._should_alert(alert_key):
                        self._fire_alert(
                            "WARNING",
                            f"[Nova] Elevated error rate: {error_rate_pct:.1f}%",
                            f"Global error rate is {error_rate_pct:.1f}% "
                            f"(threshold: 5%).",
                        )

            # Check burn rates for SLO budget exhaustion
            try:
                burn_rates = collector.compute_burn_rate()
                for slo_name, br_data in burn_rates.items():
                    if not isinstance(br_data, dict):
                        continue
                    br_status = br_data.get("status", "ok")
                    burn_rate = br_data.get("burn_rate", 0)
                    if br_status == "critical":
                        alert_key = f"burn_rate_critical_{slo_name}"
                        if self._should_alert(alert_key):
                            self._fire_alert(
                                "CRITICAL",
                                f"[Nova] Burn rate critical: {slo_name} ({burn_rate}x)",
                                f"SLO '{slo_name}' error budget burn rate is {burn_rate}x "
                                f"(>5x threshold). Budget remaining: "
                                f"{br_data.get('budget_remaining_pct', 0)}%.",
                            )
                    elif br_status == "warning":
                        alert_key = f"burn_rate_warning_{slo_name}"
                        if self._should_alert(alert_key):
                            self._fire_alert(
                                "WARNING",
                                f"[Nova] Burn rate elevated: {slo_name} ({burn_rate}x)",
                                f"SLO '{slo_name}' error budget burn rate is {burn_rate}x "
                                f"(>2x threshold). Budget remaining: "
                                f"{br_data.get('budget_remaining_pct', 0)}%.",
                            )
            except Exception as burn_err:
                logger.debug("[MonitorAlertBridge] Burn rate check error: %s", burn_err)

            # Check anomaly detection
            try:
                anomalies = collector.check_anomalies()
                for anomaly in anomalies:
                    if not isinstance(anomaly, dict):
                        continue
                    metric_name = anomaly.get("metric", "unknown")
                    deviation = anomaly.get("deviation_sigma", 0)
                    alert_key = f"anomaly_{metric_name}"
                    # Only alert on WORSE performance (higher latency), not improvements
                    current_val = anomaly.get("current", 0)
                    baseline_mean = anomaly.get("baseline_mean", 0)
                    is_worse = (
                        current_val > baseline_mean
                        if isinstance(current_val, (int, float))
                        and isinstance(baseline_mean, (int, float))
                        else True
                    )
                    if deviation > 5 and is_worse and self._should_alert(alert_key):
                        self._fire_alert(
                            "INFO",
                            f"[Nova] Anomaly detected: {metric_name} ({deviation:.1f} sigma)",
                            f"Metric '{metric_name}' is {deviation:.1f} standard deviations "
                            f"from baseline. Current: {anomaly.get('current')}, "
                            f"Baseline: {anomaly.get('baseline_mean')} +/- "
                            f"{anomaly.get('baseline_std')}.",
                        )
            except Exception as anom_err:
                logger.debug("[MonitorAlertBridge] Anomaly check error: %s", anom_err)

            # Deploy detection: check if VERSION has changed
            try:
                current_version = VERSION
                if current_version != self._last_known_version:
                    old_version = self._last_known_version
                    self._last_known_version = current_version
                    alert_key = f"deploy_detected_{current_version}"
                    if self._should_alert(alert_key):
                        self._fire_alert(
                            "INFO",
                            f"[Nova] Deploy detected: v{old_version} -> v{current_version}",
                            f"Server version changed from {old_version} to "
                            f"{current_version}. Instance: "
                            f"{os.environ.get('RENDER_INSTANCE_ID', 'local')}.",
                        )
            except Exception as deploy_err:
                logger.debug("[MonitorAlertBridge] Deploy check error: %s", deploy_err)

            # Module DOWN detection: any module with status 'critical' or score < 20
            try:
                for module_id, module_data in module_scores.items():
                    score = module_data.get("health_score", 100)
                    status = module_data.get("status", "healthy")
                    if score < 20 or status == "critical":
                        alert_key = f"module_down_{module_id}"
                        if self._should_alert(alert_key):
                            self._fire_alert(
                                "CRITICAL",
                                f"[Nova] Module DOWN: {module_id}",
                                f"Module {module_id} appears DOWN "
                                f"(score: {score}/100, status: {status}). "
                                f"Requires immediate attention.",
                            )
            except Exception as down_err:
                logger.debug(
                    "[MonitorAlertBridge] Module DOWN check error: %s", down_err
                )

        except Exception as e:
            logger.error("[MonitorAlertBridge] Check cycle error: %s", e, exc_info=True)

    def _run_loop(self) -> None:
        """Background check loop."""
        # Wait 120s after startup before first check (let things stabilize)
        time.sleep(120)
        while self._running:
            self._check_cycle()
            time.sleep(self._interval)

    def get_status(self) -> dict:
        """Get bridge status for admin dashboard."""
        with self._lock:
            return {
                "running": self._running,
                "interval_s": self._interval,
                "active_cooldowns": len(self._alert_cooldowns),
                "cooldown_period_s": self._cooldown_period,
            }


# Global bridge instance
_alert_bridge: Optional[MonitoringAlertBridge] = None


def start_alert_bridge() -> None:
    """Start the monitoring-to-alerting bridge."""
    global _alert_bridge
    if _alert_bridge is None:
        _alert_bridge = MonitoringAlertBridge()
    _alert_bridge.start()


def stop_alert_bridge() -> None:
    """Stop the monitoring-to-alerting bridge."""
    global _alert_bridge
    if _alert_bridge:
        _alert_bridge.stop()


def get_alert_bridge_status() -> dict:
    """Get alert bridge status."""
    if _alert_bridge:
        return _alert_bridge.get_status()
    return {"running": False, "status": "not_initialized"}
