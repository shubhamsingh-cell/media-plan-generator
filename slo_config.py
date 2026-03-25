"""SLO Configuration and Per-Module Compliance Tracker.

Defines SLO targets for all 22 Session-15 modules plus existing modules,
categorized by endpoint type. Tracks rolling 1-hour metrics windows and
reports per-module compliance status.

Thread-safe: all shared state guarded by locks.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SLO target categories
# ---------------------------------------------------------------------------

CATEGORY_CHAT_LLM = "chat_llm"
CATEGORY_ENRICHMENT = "enrichment"
CATEGORY_DATA = "data"
CATEGORY_BACKGROUND = "background"

# Default SLO thresholds per category
CATEGORY_DEFAULTS: dict[str, dict[str, float]] = {
    CATEGORY_CHAT_LLM: {
        "p95_latency_ms": 5000.0,
        "error_rate_pct": 2.0,
    },
    CATEGORY_ENRICHMENT: {
        "p95_latency_ms": 10000.0,
        "error_rate_pct": 5.0,
    },
    CATEGORY_DATA: {
        "p95_latency_ms": 1000.0,
        "error_rate_pct": 1.0,
    },
    CATEGORY_BACKGROUND: {
        "completion_rate_pct": 95.0,
    },
}

# ---------------------------------------------------------------------------
# Module -> category mapping (all 22 S15 modules + existing)
# ---------------------------------------------------------------------------

MODULE_CATEGORIES: dict[str, str] = {
    # -- Chat / LLM endpoints --
    "plan_copilot": CATEGORY_CHAT_LLM,
    "morning_brief": CATEGORY_CHAT_LLM,
    "nova_ai": CATEGORY_CHAT_LLM,
    # -- Enrichment endpoints --
    "market_pulse": CATEGORY_ENRICHMENT,
    "market_signals": CATEGORY_ENRICHMENT,
    "feature_store": CATEGORY_ENRICHMENT,
    "outcome_engine": CATEGORY_ENRICHMENT,
    "outcome_pipeline": CATEGORY_ENRICHMENT,
    "prediction_model": CATEGORY_ENRICHMENT,
    "benchmarking": CATEGORY_ENRICHMENT,
    "attribution_dashboard": CATEGORY_ENRICHMENT,
    "scorecard_generator": CATEGORY_ENRICHMENT,
    "canvas_engine": CATEGORY_ENRICHMENT,
    # -- Data endpoints (fast reads) --
    "role_taxonomy": CATEGORY_DATA,
    "plan_templates": CATEGORY_DATA,
    "plan_events": CATEGORY_DATA,
    "event_store": CATEGORY_DATA,
    "ats_widget": CATEGORY_DATA,
    "edge_router": CATEGORY_DATA,
    "edge_routing": CATEGORY_DATA,
    "rate_limiter_adaptive": CATEGORY_DATA,
    # -- Background jobs --
    "request_coalescing": CATEGORY_BACKGROUND,
    "circuit_breaker_mesh": CATEGORY_BACKGROUND,
    # -- Existing modules (pre-S15) --
    "command_center": CATEGORY_ENRICHMENT,
    "intelligence_hub": CATEGORY_ENRICHMENT,
}

# Route prefix -> module name (for automatic classification)
ROUTE_MODULE_MAP: dict[str, str] = {
    "/api/copilot": "plan_copilot",
    "/api/morning-brief": "morning_brief",
    "/api/chat": "nova_ai",
    "/api/nova": "nova_ai",
    "/api/market-pulse": "market_pulse",
    "/api/signals": "market_signals",
    "/api/features/store": "feature_store",
    "/api/outcome": "outcome_engine",
    "/api/pipeline": "outcome_pipeline",
    "/api/predict": "prediction_model",
    "/api/benchmarks": "benchmarking",
    "/api/attribution": "attribution_dashboard",
    "/api/scorecard": "scorecard_generator",
    "/api/canvas": "canvas_engine",
    "/api/taxonomy": "role_taxonomy",
    "/api/templates": "plan_templates",
    "/api/plan-events": "plan_events",
    "/api/events": "event_store",
    "/api/ats": "ats_widget",
    "/api/edge": "edge_router",
    "/api/routing": "edge_routing",
    "/api/rate-limits": "rate_limiter_adaptive",
    "/api/coalesce": "request_coalescing",
    "/api/circuit": "circuit_breaker_mesh",
    "/api/generate": "command_center",
    "/api/quick-plan": "command_center",
    "/api/research": "intelligence_hub",
    "/api/competitive": "intelligence_hub",
    "/api/enrich": "intelligence_hub",
}

# Rolling window size in seconds
METRICS_WINDOW_S: int = 3600  # 1 hour
# Max data points per module (prevents unbounded memory)
MAX_SAMPLES: int = 2000


def classify_endpoint_to_module(endpoint: str) -> str:
    """Map a request path to its owning module name.

    Uses longest-prefix matching against ROUTE_MODULE_MAP.

    Args:
        endpoint: The URL path (e.g. '/api/chat').

    Returns:
        Module name string, or empty string if unclassified.
    """
    if not endpoint:
        return ""
    # Exact match first
    mod = ROUTE_MODULE_MAP.get(endpoint)
    if mod:
        return mod
    # Prefix match (longest wins)
    best_match = ""
    best_len = 0
    for prefix, mod_name in ROUTE_MODULE_MAP.items():
        if endpoint.startswith(prefix) and len(prefix) > best_len:
            best_match = mod_name
            best_len = len(prefix)
    return best_match


# ---------------------------------------------------------------------------
# Per-module rolling metrics store
# ---------------------------------------------------------------------------


class _ModuleMetrics:
    """Rolling-window metrics for a single module. NOT thread-safe on its own."""

    __slots__ = ("latencies", "errors", "successes", "jobs_started", "jobs_completed")

    def __init__(self) -> None:
        self.latencies: deque[tuple[float, float]] = deque(maxlen=MAX_SAMPLES)
        self.errors: deque[float] = deque(maxlen=MAX_SAMPLES)
        self.successes: deque[float] = deque(maxlen=MAX_SAMPLES)
        self.jobs_started: deque[float] = deque(maxlen=MAX_SAMPLES)
        self.jobs_completed: deque[float] = deque(maxlen=MAX_SAMPLES)

    def prune(self, cutoff: float) -> None:
        """Remove entries older than cutoff timestamp."""
        while self.latencies and self.latencies[0][0] < cutoff:
            self.latencies.popleft()
        while self.errors and self.errors[0] < cutoff:
            self.errors.popleft()
        while self.successes and self.successes[0] < cutoff:
            self.successes.popleft()
        while self.jobs_started and self.jobs_started[0] < cutoff:
            self.jobs_started.popleft()
        while self.jobs_completed and self.jobs_completed[0] < cutoff:
            self.jobs_completed.popleft()


class SLOTracker:
    """Thread-safe singleton that tracks per-module SLO compliance.

    Usage:
        tracker = SLOTracker.instance()
        tracker.record_request("plan_copilot", latency_ms=1200.0, success=True)
        report = tracker.get_compliance_report()
    """

    _instance: Optional["SLOTracker"] = None
    _init_lock = threading.Lock()

    def __new__(cls) -> "SLOTracker":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._initialized = False
                    cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._lock = threading.Lock()
        self._modules: dict[str, _ModuleMetrics] = defaultdict(_ModuleMetrics)

    @classmethod
    def instance(cls) -> "SLOTracker":
        """Return the singleton instance."""
        return cls()

    def record_request(
        self,
        module: str,
        latency_ms: float,
        success: bool = True,
    ) -> None:
        """Record an HTTP request for a module.

        Args:
            module: Module name (e.g. 'plan_copilot').
            latency_ms: Request latency in milliseconds.
            success: True if 2xx/3xx, False if 5xx.
        """
        now = time.time()
        with self._lock:
            m = self._modules[module]
            m.latencies.append((now, latency_ms))
            if success:
                m.successes.append(now)
            else:
                m.errors.append(now)

    def record_job(self, module: str, completed: bool = True) -> None:
        """Record a background job start/completion.

        Args:
            module: Module name.
            completed: True if the job finished successfully.
        """
        now = time.time()
        with self._lock:
            m = self._modules[module]
            m.jobs_started.append(now)
            if completed:
                m.jobs_completed.append(now)

    def record_from_endpoint(
        self,
        endpoint: str,
        latency_ms: float,
        status_code: int,
    ) -> None:
        """Auto-classify an endpoint and record the request.

        This is the main integration point -- call from the request
        lifecycle in app.py after each request completes.

        Args:
            endpoint: URL path (e.g. '/api/chat').
            latency_ms: Request duration in ms.
            status_code: HTTP response status code.
        """
        module = classify_endpoint_to_module(endpoint)
        if not module:
            return
        success = status_code < 500
        self.record_request(module, latency_ms, success)

    def get_compliance_report(self) -> dict[str, Any]:
        """Generate SLO compliance report for all tracked modules.

        Returns:
            Dict with per-module compliance, overall status, and timestamp.
        """
        now = time.time()
        cutoff = now - METRICS_WINDOW_S
        modules_report: dict[str, dict[str, Any]] = {}
        violations: list[str] = []

        with self._lock:
            for mod_name, metrics in self._modules.items():
                metrics.prune(cutoff)
                category = MODULE_CATEGORIES.get(mod_name, CATEGORY_DATA)
                targets = CATEGORY_DEFAULTS.get(
                    category, CATEGORY_DEFAULTS[CATEGORY_DATA]
                )
                mod_report = self._evaluate_module(mod_name, metrics, category, targets)
                modules_report[mod_name] = mod_report
                if not mod_report["compliant"]:
                    violations.append(mod_name)

        # Add modules with no traffic yet
        for mod_name, cat in MODULE_CATEGORIES.items():
            if mod_name not in modules_report:
                targets = CATEGORY_DEFAULTS.get(cat, CATEGORY_DEFAULTS[CATEGORY_DATA])
                modules_report[mod_name] = {
                    "category": cat,
                    "targets": targets,
                    "compliant": True,
                    "status": "no_traffic",
                    "sample_size": 0,
                    "metrics": {},
                }

        all_compliant = len(violations) == 0
        return {
            "all_compliant": all_compliant,
            "total_modules": len(modules_report),
            "violations": violations,
            "modules": modules_report,
            "window_seconds": METRICS_WINDOW_S,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _evaluate_module(
        mod_name: str,
        metrics: _ModuleMetrics,
        category: str,
        targets: dict[str, float],
    ) -> dict[str, Any]:
        """Evaluate SLO compliance for a single module."""
        result: dict[str, Any] = {
            "category": category,
            "targets": dict(targets),
            "compliant": True,
            "status": "healthy",
            "metrics": {},
        }

        total_requests = len(metrics.successes) + len(metrics.errors)
        result["sample_size"] = total_requests

        if total_requests == 0 and not metrics.jobs_started:
            result["status"] = "no_traffic"
            return result

        # -- Latency check (for non-background categories) --
        if "p95_latency_ms" in targets and metrics.latencies:
            lats = sorted(v for _, v in metrics.latencies)
            p95 = _percentile(lats, 95)
            target_p95 = targets["p95_latency_ms"]
            lat_ok = p95 <= target_p95
            result["metrics"]["p95_latency_ms"] = {
                "actual": round(p95, 1),
                "target": target_p95,
                "compliant": lat_ok,
            }
            if not lat_ok:
                result["compliant"] = False
                result["status"] = "latency_violation"

        # -- Error rate check --
        if "error_rate_pct" in targets and total_requests > 0:
            err_count = len(metrics.errors)
            err_rate = (err_count / total_requests) * 100
            target_err = targets["error_rate_pct"]
            err_ok = err_rate <= target_err
            result["metrics"]["error_rate_pct"] = {
                "actual": round(err_rate, 2),
                "target": target_err,
                "compliant": err_ok,
            }
            if not err_ok:
                result["compliant"] = False
                result["status"] = "error_rate_violation"

        # -- Completion rate check (background jobs only) --
        if "completion_rate_pct" in targets and metrics.jobs_started:
            started = len(metrics.jobs_started)
            completed = len(metrics.jobs_completed)
            comp_rate = (completed / started) * 100 if started > 0 else 100.0
            target_comp = targets["completion_rate_pct"]
            comp_ok = comp_rate >= target_comp
            result["metrics"]["completion_rate_pct"] = {
                "actual": round(comp_rate, 2),
                "target": target_comp,
                "compliant": comp_ok,
            }
            if not comp_ok:
                result["compliant"] = False
                result["status"] = "completion_rate_violation"

        return result


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Calculate the p-th percentile from a sorted list.

    Args:
        sorted_values: Pre-sorted list of numeric values.
        pct: Percentile (0-100).

    Returns:
        The percentile value, or 0.0 for empty lists.
    """
    if not sorted_values:
        return 0.0
    k = (pct / 100.0) * (len(sorted_values) - 1)
    f = math.floor(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    d = k - f
    return sorted_values[f] * (1 - d) + sorted_values[c] * d


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------


def get_slo_tracker() -> SLOTracker:
    """Return the global SLOTracker singleton."""
    return SLOTracker.instance()
