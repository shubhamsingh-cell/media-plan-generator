#!/usr/bin/env python3
"""Runtime QC -- periodic health checks for Nova AI Suite.

Runs every 60 seconds as a background thread:
1. Checks all product endpoints respond
2. Verifies LLM router health
3. Checks data source availability
4. Triggers alerts via alert_manager on degradation

Startup behavior:
- Waits _STARTUP_GRACE_PERIOD (90s) before first check to let server warm up
- First _WARMUP_GRACE_CHECKS (5) cycles use relaxed thresholds and suppress alerts
- Reports "warming_up" state instead of "critical" during warmup
- Only fires alerts after the warmup window has passed
"""

import logging
import threading
import time
import urllib.request
import urllib.error
from collections import deque
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 60  # seconds
_DEGRADED_THRESHOLD = 0.7  # health score below this triggers alert
_CRITICAL_THRESHOLD = 0.4
_STARTUP_GRACE_PERIOD = 90  # seconds to wait before first check
_WARMUP_GRACE_CHECKS = 5  # first N checks suppress alerts (server still loading KB)
_running = False
_thread: Optional[threading.Thread] = None
_last_results: dict[str, Any] = {}
# S57 FIX: RLock (reentrant) instead of Lock. get_status() acquires _lock
# then calls get_sla_report() which re-acquires it -- this deadlocked
# /api/health/auto-qc in production (verified: 30s timeout with 0 bytes
# returned on every request to this endpoint). Single-line fix.
_lock = threading.RLock()
_check_history: deque = deque(maxlen=1440)  # 24h at 60s intervals
_start_time: float = 0.0  # set when start() is called
_check_count: int = 0  # number of completed check cycles

# Configurable check definitions: list of (name, path) tuples
# S63 FIX: Swapped /api/health (8s deep check) -> /api/health/ping (instant).
# /api/health has an 8-second internal time-box (routes/health.py:77), which
# frequently raced the QC 10s probe timeout and produced false-positive
# "Health score 0.0" alerts. Ping is designed for liveness.
_check_definitions: list[tuple[str, str]] = [
    ("homepage", "/"),
    ("health", "/api/health/ping"),
    ("health_ready", "/api/health/ready"),
    ("channels", "/api/channels"),
    ("dashboard_widgets", "/api/dashboard/widgets"),
]


def _is_warming_up() -> bool:
    """Check if the server is still in the warmup window."""
    if _start_time == 0.0:
        return True
    elapsed = time.time() - _start_time
    # Warmup = grace period + (grace_checks * interval)
    warmup_window = _STARTUP_GRACE_PERIOD + (_WARMUP_GRACE_CHECKS * _CHECK_INTERVAL)
    return elapsed < warmup_window or _check_count < _WARMUP_GRACE_CHECKS


def _probe(path: str, timeout: int = 15) -> tuple[bool, float]:
    """Probe a local endpoint. Returns (ok, latency_ms).

    S63: timeout raised 10s -> 15s to buffer above /api/health's internal
    8s time-box. One silent retry on first failure to absorb transient stalls.
    """
    import os

    base = f"http://127.0.0.1:{os.environ.get('PORT', '10000')}"
    url = f"{base}{path}"

    for attempt in (1, 2):
        start = time.monotonic()
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                _ = resp.read()
                latency = (time.monotonic() - start) * 1000
                if resp.status == 200:
                    return True, latency
        except Exception:
            pass
        if attempt == 1:
            time.sleep(0.5)  # brief pause before retry

    latency = (time.monotonic() - start) * 1000
    return False, latency


def set_check_definitions(definitions: list[tuple[str, str]]) -> None:
    """Replace the default check definitions with a custom list.

    Each entry is a (name, path) tuple, e.g. ("health", "/api/health").
    """
    global _check_definitions
    _check_definitions = list(definitions)


def _check_cycle() -> dict[str, Any]:
    """Run one full health check cycle."""
    global _check_count
    warming_up = _is_warming_up()

    results: dict[str, Any] = {"ts": time.time(), "checks": {}, "overall": "healthy"}

    # Product endpoints (configurable)
    failed = 0
    for name, path in _check_definitions:
        ok, latency = _probe(path)
        results["checks"][name] = {"ok": ok, "latency_ms": round(latency, 1)}
        if not ok:
            failed += 1

    # Health score
    total = len(_check_definitions)
    score = (total - failed) / total if total > 0 else 0
    results["health_score"] = round(score, 2)
    results["warming_up"] = warming_up
    results["check_number"] = _check_count + 1

    if warming_up:
        # During warmup, report "warming_up" instead of critical/degraded
        if score < 1.0:
            results["overall"] = "warming_up"
        # else: leave as "healthy" if everything passes
    else:
        if score < _CRITICAL_THRESHOLD:
            results["overall"] = "critical"
        elif score < _DEGRADED_THRESHOLD:
            results["overall"] = "degraded"

    # LLM Router health (if available)
    try:
        import sys as _sys

        if "llm_router" in _sys.modules:
            llm_mod = _sys.modules["llm_router"]
            if hasattr(llm_mod, "get_health_status"):
                llm_health = llm_mod.get_health_status()
                results["llm_router"] = {
                    "healthy_providers": llm_health.get("healthy_count", 0),
                    "total_providers": llm_health.get("total_count", 0),
                    "degraded": llm_health.get("degraded", False),
                }
    except Exception as e:
        logger.debug("LLM router health check skipped: %s", e)

    # Dependency probes
    deps: dict[str, Any] = {}
    try:
        import sys

        if "supabase_data" in sys.modules:
            deps["supabase"] = {"ok": True}
        else:
            deps["supabase"] = {"ok": False}
    except Exception:
        deps["supabase"] = {"ok": False}
    try:
        import sys

        if "api_integrations" in sys.modules:
            deps["external_apis"] = {"ok": True}
        else:
            deps["external_apis"] = {"ok": False}
    except Exception:
        deps["external_apis"] = {"ok": False}
    results["dependencies"] = deps

    # Append to check history
    with _lock:
        _check_history.append(results)

    _check_count += 1

    return results


def _alert_if_needed(results: dict[str, Any]) -> None:
    """Send alert if health is degraded or critical.

    S63: Requires 2 consecutive failing cycles before alerting, and emits
    a stable subject (no score value) so the alert_manager 4h dedup window
    actually suppresses duplicates instead of firing on every flap.
    """
    # Never alert during warmup
    if results.get("warming_up", False):
        return

    if results["overall"] not in ("degraded", "critical"):
        return

    # S63: Require 2 consecutive non-healthy cycles before alerting.
    # Prevents single-probe flaps from paging.
    with _lock:
        recent = list(_check_history)[-2:]
    if len(recent) < 2:
        return
    if any(r.get("overall") not in ("degraded", "critical") for r in recent):
        return

    try:
        import sys as _sys

        if "alert_manager" in _sys.modules:
            am = _sys.modules["alert_manager"]
            if hasattr(am, "send_alert"):
                failed_checks = [
                    name for name, check in results["checks"].items() if not check["ok"]
                ]
                severity = "CRITICAL" if results["overall"] == "critical" else "WARNING"
                # S63: Stable subject -- drop the score so dedup catches repeats.
                am.send_alert(
                    subject=f"[Nova QC] {severity}: {len(failed_checks)} check(s) failing",
                    body=(
                        f"Failed checks: {', '.join(failed_checks) or 'none'}\n"
                        f"Score: {results['health_score']}\n"
                        f"Two consecutive failing cycles confirmed."
                    ),
                    severity=severity,
                )
    except Exception as e:
        logger.warning("Auto-QC alert failed: %s", e)


def _run_loop() -> None:
    """Background loop with startup grace period."""
    global _running, _last_results
    logger.info(
        "[AutoQC] Background health monitor started (grace: %ds, interval: %ds)",
        _STARTUP_GRACE_PERIOD,
        _CHECK_INTERVAL,
    )

    # Wait for server to finish startup before probing endpoints
    logger.info(
        "[AutoQC] Waiting %ds for server startup before first health check...",
        _STARTUP_GRACE_PERIOD,
    )
    grace_elapsed = 0
    while _running and grace_elapsed < _STARTUP_GRACE_PERIOD:
        time.sleep(5)
        grace_elapsed += 5

    if not _running:
        return

    logger.info("[AutoQC] Grace period complete, starting health checks")

    while _running:
        try:
            results = _check_cycle()
            with _lock:
                _last_results = results
            _alert_if_needed(results)

            if results["overall"] == "warming_up":
                logger.info(
                    "[AutoQC] Warming up (check #%d, score: %s) -- alerts suppressed",
                    results.get("check_number", 0),
                    results["health_score"],
                )
            elif results["overall"] != "healthy":
                logger.warning(
                    "[AutoQC] Health: %s (score: %s)",
                    results["overall"],
                    results["health_score"],
                )
        except Exception as e:
            logger.error("[AutoQC] Check cycle error: %s", e, exc_info=True)
        time.sleep(_CHECK_INTERVAL)


def start() -> None:
    """Start the background QC monitor."""
    global _running, _thread, _start_time
    if _running:
        return
    _running = True
    _start_time = time.time()
    _thread = threading.Thread(target=_run_loop, daemon=True, name="auto-qc")
    _thread.start()


def stop() -> None:
    """Stop the background QC monitor."""
    global _running
    _running = False


def get_sla_report() -> dict[str, Any]:
    """Compute rolling SLA from check history.

    Excludes warming_up checks from SLA calculation since they represent
    expected startup behavior, not real degradation.
    """
    with _lock:
        if not _check_history:
            return {"uptime_24h": None, "uptime_7d": None}
        now = time.time()
        checks_24h = [c for c in _check_history if now - c.get("ts", 0) < 86400]
        # Exclude warmup checks from SLA -- they are expected to fail
        steady_checks = [c for c in checks_24h if not c.get("warming_up", False)]
        healthy = sum(1 for c in steady_checks if c.get("overall") == "healthy")
        uptime = healthy / len(steady_checks) if steady_checks else None
        return {
            "uptime_24h": round(uptime * 100, 2) if uptime is not None else None,
            "total_checks_24h": len(checks_24h),
            "steady_checks_24h": len(steady_checks),
            "healthy_checks_24h": healthy,
            "warmup_checks_excluded": len(checks_24h) - len(steady_checks),
        }


def get_status() -> dict[str, Any]:
    """Get latest QC results, including history summary and SLA data."""
    with _lock:
        if not _last_results:
            warming = _is_warming_up()
            return {
                "status": "warming_up" if warming else "not_started",
                "warming_up": warming,
                "message": (
                    f"Server is warming up. First health check runs after "
                    f"{_STARTUP_GRACE_PERIOD}s grace period."
                    if warming
                    else "QC monitor has not started."
                ),
            }
        result = dict(_last_results)
        result["history_size"] = len(_check_history)
        result["sla"] = get_sla_report()
        return result


# ===============================================================================
# BACKWARD-COMPATIBLE CLASS WRAPPER
# app.py uses: get_auto_qc() -> AutoQC with .start_background() and .get_status()
# ===============================================================================


class AutoQC:
    """Wrapper class for backward compatibility with app.py."""

    def start_background(self) -> None:
        """Start the background QC monitor."""
        start()

    def get_status(self) -> Dict[str, Any]:
        """Get latest QC results."""
        return get_status()

    def get_sla_report(self) -> Dict[str, Any]:
        """Compute rolling SLA from check history."""
        return get_sla_report()

    def stop(self) -> None:
        """Stop the background QC monitor."""
        stop()


_auto_qc_instance: Optional[AutoQC] = None
_auto_qc_lock = threading.Lock()


def get_auto_qc() -> AutoQC:
    """Get or create the singleton AutoQC instance (thread-safe)."""
    global _auto_qc_instance
    if _auto_qc_instance is None:
        with _auto_qc_lock:
            if _auto_qc_instance is None:
                _auto_qc_instance = AutoQC()
    return _auto_qc_instance
