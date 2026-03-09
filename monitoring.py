"""
Monitoring & Observability Module for AI Media Planner.

Provides:
- Deep health checks (liveness + readiness + dependency checks)
- Structured request metrics (latency, error rates, throughput)
- Memory and disk usage tracking
- API dependency reachability probes
- Metrics export endpoint for dashboards
- Graceful shutdown coordination

This module has no external dependencies (stdlib only).
"""

from __future__ import annotations

import gc
import logging
import os
import platform
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "3.0.0"
_START_TIME = time.time()
DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_DIR = DATA_DIR / "api_cache"
DOCS_DIR = DATA_DIR / "generated_docs"
METRICS_WINDOW = 3600  # 1 hour rolling window for rate calculations


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

    def record_request(self, endpoint: str, method: str, status_code: int,
                       latency_ms: float) -> None:
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

    def record_chat(self) -> None:
        with self._req_lock:
            self.total_chat_requests += 1

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
            error_rate = (window_errors / window_requests * 100) if window_requests > 0 else 0

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
                    "avg_ms": round(sum(lat_list) / len(lat_list), 1) if lat_list else 0,
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
                        self._api_success_count / max(1, self._api_success_count + self._api_failure_count) * 100, 1
                    ),
                    "avg_latency_ms": round(
                        sum(self._api_latencies) / max(1, len(self._api_latencies)), 1
                    ) if self._api_latencies else 0,
                },
            }


def get_metrics() -> MetricsCollector:
    """Get the singleton MetricsCollector instance."""
    return MetricsCollector()


# ---------------------------------------------------------------------------
# Health Checks
# ---------------------------------------------------------------------------

def health_check_liveness() -> Dict[str, Any]:
    """Lightweight liveness probe -- confirms the process is alive.

    Suitable for Render.com / Kubernetes liveness probes.
    Should return quickly (< 100ms).
    """
    return {
        "status": "ok",
        "version": VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(time.time() - _START_TIME, 1),
    }


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

    # 3. Disk usage (generated docs)
    try:
        docs_dir = DOCS_DIR
        if docs_dir.exists():
            doc_count = len(list(docs_dir.glob("*.zip")))
            total_size_mb = sum(f.stat().st_size for f in docs_dir.glob("*.zip")) / (1024 * 1024)
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
            cache_size_mb = sum(f.stat().st_size for f in cache_dir.glob("*.json")) / (1024 * 1024)
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
        if hasattr(data_orchestrator, 'get_cache_stats'):
            orch_stats["cache"] = data_orchestrator.get_cache_stats()
        if hasattr(data_orchestrator, 'get_fallback_telemetry'):
            orch_stats["fallback_telemetry"] = data_orchestrator.get_fallback_telemetry()
        checks["orchestrator"] = {"status": "ok", **orch_stats}
    except Exception as e:
        checks["orchestrator"] = {"status": "degraded", "detail": str(e)}

    return {
        "status": "healthy" if overall_healthy else "unhealthy",
        "version": VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(time.time() - _START_TIME, 1),
        "uptime_human": _format_duration(time.time() - _START_TIME),
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# Structured Logging
# ---------------------------------------------------------------------------

def configure_logging(level: str = "INFO") -> None:
    """Configure structured logging for production.

    Sets up a consistent format across all modules with timestamps,
    log levels, module names, and request context.
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
        "Logging configured: level=%s, python=%s, pid=%d",
        level, platform.python_version(), os.getpid()
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
            self._timeout, self._active_count
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
    result["gc_collections"] = sum(s.get("collections", 0) for s in gc_stats)
    result["gc_collected"] = sum(s.get("collected", 0) for s in gc_stats)
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
