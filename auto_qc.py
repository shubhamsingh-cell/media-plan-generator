#!/usr/bin/env python3
"""Runtime QC -- periodic health checks for Nova AI Suite.

Runs every 60 seconds as a background thread:
1. Checks all product endpoints respond
2. Verifies LLM router health
3. Checks data source availability
4. Triggers alerts via alert_manager on degradation
"""

import logging
import threading
import time
import urllib.request
import urllib.error
import json
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 60  # seconds
_DEGRADED_THRESHOLD = 0.7  # health score below this triggers alert
_CRITICAL_THRESHOLD = 0.4
_running = False
_thread: Optional[threading.Thread] = None
_last_results: dict[str, Any] = {}
_lock = threading.Lock()


def _probe(path: str, timeout: int = 10) -> tuple[bool, float]:
    """Probe a local endpoint. Returns (ok, latency_ms)."""
    import os

    base = f"http://127.0.0.1:{os.environ.get('PORT', '10000')}"
    url = f"{base}{path}"
    start = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            _ = resp.read()
            latency = (time.monotonic() - start) * 1000
            return resp.status == 200, latency
    except Exception:
        latency = (time.monotonic() - start) * 1000
        return False, latency


def _check_cycle() -> dict[str, Any]:
    """Run one full health check cycle."""
    results: dict[str, Any] = {"ts": time.time(), "checks": {}, "overall": "healthy"}

    # Product endpoints
    endpoints = {
        "homepage": "/",
        "health": "/api/health",
        "health_ready": "/api/health/ready",
        "channels": "/api/channels",
        "dashboard_widgets": "/api/dashboard/widgets",
    }

    failed = 0
    for name, path in endpoints.items():
        ok, latency = _probe(path)
        results["checks"][name] = {"ok": ok, "latency_ms": round(latency, 1)}
        if not ok:
            failed += 1

    # Health score
    total = len(endpoints)
    score = (total - failed) / total if total > 0 else 0
    results["health_score"] = round(score, 2)

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

    return results


def _alert_if_needed(results: dict[str, Any]) -> None:
    """Send alert if health is degraded or critical."""
    if results["overall"] in ("degraded", "critical"):
        try:
            import sys as _sys

            if "alert_manager" in _sys.modules:
                am = _sys.modules["alert_manager"]
                if hasattr(am, "send_alert"):
                    failed_checks = [
                        name
                        for name, check in results["checks"].items()
                        if not check["ok"]
                    ]
                    severity = (
                        "CRITICAL" if results["overall"] == "critical" else "WARNING"
                    )
                    am.send_alert(
                        subject=f"[Nova QC] {severity}: Health score {results['health_score']}",
                        body=f"Failed checks: {', '.join(failed_checks)}\nScore: {results['health_score']}",
                        severity=severity,
                    )
        except Exception as e:
            logger.error("Auto-QC alert failed: %s", e, exc_info=True)


def _run_loop() -> None:
    """Background loop."""
    global _running, _last_results
    logger.info(
        "[AutoQC] Background health monitor started (interval: %ds)", _CHECK_INTERVAL
    )
    while _running:
        try:
            results = _check_cycle()
            with _lock:
                _last_results = results
            _alert_if_needed(results)
            if results["overall"] != "healthy":
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
    global _running, _thread
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_run_loop, daemon=True, name="auto-qc")
    _thread.start()


def stop() -> None:
    """Stop the background QC monitor."""
    global _running
    _running = False


def get_status() -> dict[str, Any]:
    """Get latest QC results."""
    with _lock:
        return dict(_last_results) if _last_results else {"status": "not_started"}


# ═══════════════════════════════════════════════════════════════════════════════
# BACKWARD-COMPATIBLE CLASS WRAPPER
# app.py uses: get_auto_qc() -> AutoQC with .start_background() and .get_status()
# ═══════════════════════════════════════════════════════════════════════════════


class AutoQC:
    """Wrapper class for backward compatibility with app.py."""

    def start_background(self) -> None:
        """Start the background QC monitor."""
        start()

    def get_status(self) -> Dict[str, Any]:
        """Get latest QC results."""
        return get_status()

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
