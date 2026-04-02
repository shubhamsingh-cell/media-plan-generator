"""
data_matrix_monitor.py -- Data Matrix Health Monitor

Tracks whether all 4 products (Excel/PPT, Nova Chat, Slack Bot, PPT Generator)
use all 9 data layers as expected.  Runs probes every 12 hours in a background
daemon thread, attempts self-healing on failures, and exposes results via
/api/health/data-matrix.

Self-healing actions:
    - Re-import failed modules via importlib
    - Reset data_orchestrator lazy-load sentinels (_IMPORT_FAILED -> None)
    - Reset Nova's _orchestrator sentinel (False -> None)
    - Evict stale entries from orchestrator API cache

Dependencies: stdlib only (no new packages).
"""

from __future__ import annotations

import gc
import hashlib
import importlib
import json
import logging
import os
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from alert_manager import send_alert
except ImportError:
    send_alert = lambda *a, **kw: False

_CHECK_INTERVAL = 12 * 3600  # 12 hours
_INITIAL_DELAY = (
    300  # wait 5min after startup before first check (KB needs 5-10min to load)
)
_STARTUP_ALERT_SUPPRESSION_CHECKS = (
    1  # suppress alerts for first N checks (startup failures expected)
)
_MAX_HEAL_LOG = 20  # keep last N heal actions
_API_HEALTH_TIMEOUT = 5  # seconds for API health-check requests

DATA_DIR = Path(__file__).resolve().parent / "data"

# Env vars to track for config drift detection (hash only, never store values)
_TRACKED_ENV_VARS = [
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "GROQ_API_KEY",
    "CEREBRAS_API_KEY",
    "SENTRY_DSN",
    "POSTHOG_API_KEY",
    "UPSTASH_REDIS_REST_URL",
    "SUPABASE_URL",
]

# Required KB JSON files that all products depend on
_REQUIRED_KB_FILES = [
    "recruitment_industry_knowledge.json",
    "platform_intelligence_deep.json",
    "recruitment_benchmarks_deep.json",
    "channels_db.json",
    "joveo_2026_benchmarks.json",
    "global_supply.json",
    "industry_white_papers.json",
    "joveo_publishers.json",
    "linkedin_guidewire_data.json",
    "recruitment_strategy_intelligence.json",
    "regional_hiring_intelligence.json",
    "supply_ecosystem_intelligence.json",
    "workforce_trends_intelligence.json",
]

# ═══════════════════════════════════════════════════════════════════════════════
# EXPECTED STATE MATRIX
# ═══════════════════════════════════════════════════════════════════════════════
# Each cell: YES, NO, PARTIAL, VIA_ORCHESTRATOR
# YES = direct import/usage expected
# VIA_ORCHESTRATOR = accessible through data_orchestrator.py
# PARTIAL = receives pre-computed data from upstream (not direct import)
# NO = intentionally excluded

EXPECTED_MATRIX: Dict[str, Dict[str, str]] = {
    "excel_ppt": {
        "json_files": "YES",
        "api_enrichment": "YES",
        "research": "YES",
        "data_synthesizer": "YES",
        "budget_engine": "YES",
        "standardizer": "YES",
        "claude_api": "NO",
        "trend_engine": "YES",
        "collar_intelligence": "YES",
    },
    "nova_chat": {
        "json_files": "YES",
        "api_enrichment": "VIA_ORCHESTRATOR",
        "research": "VIA_ORCHESTRATOR",
        "data_synthesizer": "NO",
        "budget_engine": "VIA_ORCHESTRATOR",
        "standardizer": "VIA_ORCHESTRATOR",
        "claude_api": "YES",
        "trend_engine": "VIA_ORCHESTRATOR",
        "collar_intelligence": "VIA_ORCHESTRATOR",
    },
    "slack_bot": {
        "json_files": "YES",
        "api_enrichment": "VIA_ORCHESTRATOR",
        "research": "VIA_ORCHESTRATOR",
        "data_synthesizer": "NO",
        "budget_engine": "VIA_ORCHESTRATOR",
        "standardizer": "VIA_ORCHESTRATOR",
        "claude_api": "YES",
        "trend_engine": "VIA_ORCHESTRATOR",
        "collar_intelligence": "VIA_ORCHESTRATOR",
    },
    "ppt_generator": {
        "json_files": "YES",
        "api_enrichment": "PARTIAL",
        "research": "YES",
        "data_synthesizer": "PARTIAL",
        "budget_engine": "PARTIAL",
        "standardizer": "NO",
        "claude_api": "NO",
        "trend_engine": "PARTIAL",
        "collar_intelligence": "PARTIAL",
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# TIER 2/3 MODULE HEALTH CHECKS
# ═══════════════════════════════════════════════════════════════════════════════
# These modules don't consume data layers like products do -- they ARE
# infrastructure. Track them separately via importability + key attribute checks.

_TIER2_MODULE_CHECKS: Dict[str, Dict[str, str]] = {
    "eval_framework": {
        "module": "eval_framework",
        "check_attr": "EvalSuite",
        "description": "AI eval framework -- budget/collar/geographic/chat scoring",
    },
    "data_contracts": {
        "module": "data_contracts",
        "check_attr": "validate_kb_file",
        "description": "KB schema validation and enrichment output contracts",
    },
    "regression_detector": {
        "module": "regression_detector",
        "check_attr": "run_regression_check",
        "description": "Reference scenario drift detection",
    },
    "llm_router": {
        "module": "llm_router",
        "check_attr": "call_llm",
        "description": "Multi-provider LLM routing (Gemini/Groq/Cerebras/Claude)",
    },
    "monitoring": {
        "module": "monitoring",
        "check_attr": "MetricsCollector",
        "description": "Structured logging, SLO monitoring, request tracing",
    },
}

# Map data layers to their underlying module names for VIA_ORCHESTRATOR probes.
# The DataOrchestrator class (get_orchestrator()) provides access to these
# through registered source handlers, but the simplest health check is to
# verify the underlying module is loaded in sys.modules (same check excel_ppt
# uses for direct imports -- if the module is loaded, it's accessible via
# orchestrator too).
_ORCH_LAYER_MAP = {
    "api_enrichment": "api_enrichment",
    "research": "research",
    "budget_engine": "budget_engine",
    "standardizer": "standardizer",
    "trend_engine": "trend_engine",
    "collar_intelligence": "collar_intelligence",
}


# ═══════════════════════════════════════════════════════════════════════════════
# MONITOR CLASS
# ═══════════════════════════════════════════════════════════════════════════════


class DataMatrixMonitor:
    """Probes all 4 products x 9 data layers and tracks health status."""

    def __init__(self):
        self._lock = threading.Lock()
        self._last_result: Optional[Dict[str, Any]] = None
        self._last_check_time: float = 0
        self._check_count: int = 0
        self._heal_log: List[Dict[str, Any]] = []
        self._thread: Optional[threading.Thread] = None
        self._json_probe_cache: Optional[Dict[str, Any]] = None
        # Config drift: snapshot env var hashes at startup
        self._env_snapshot: Dict[str, str] = self._snapshot_env_hashes()
        self._last_drift_check: float = time.time()
        # Supabase fallback flag: when True, products should use local JSON
        self.supabase_fallback_to_local: bool = False

    # ── Public API ────────────────────────────────────────────────────────

    def start_background(self) -> None:
        """Start the background daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._background_loop,
            name="data-matrix-monitor",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "DataMatrixMonitor: background thread started (interval=%ds)",
            _CHECK_INTERVAL,
        )

    def get_status(self) -> Dict[str, Any]:
        """Return the latest matrix check result for the endpoint."""
        with self._lock:
            if self._last_result is None:
                return {
                    "status": "pending",
                    "message": (
                        "First check has not completed yet. "
                        f"Initial check runs {_INITIAL_DELAY // 60}min after startup "
                        f"to allow KB to load."
                    ),
                    "check_interval_hours": _CHECK_INTERVAL / 3600,
                }
            result = dict(self._last_result)
            result["age_seconds"] = round(time.time() - self._last_check_time, 1)
            result["next_check_in_seconds"] = max(
                0, round(_CHECK_INTERVAL - (time.time() - self._last_check_time), 1)
            )
        # Add self-healing subsystem status (outside lock for safety)
        try:
            result["config_drift"] = self._check_config_drift()
        except Exception:
            result["config_drift"] = {"status": "unknown"}
        return result

    def run_check(self) -> Dict[str, Any]:
        """Run a full 4x7 matrix probe, attempt self-healing, return results."""
        start = time.time()
        self._json_probe_cache = None  # reset per-check cache
        matrix_results: Dict[str, Dict[str, Any]] = {}
        counts = {"ok": 0, "error": 0, "partial": 0, "ok_expected_no": 0, "healed": 0}

        for product, layers in EXPECTED_MATRIX.items():
            product_results: Dict[str, Any] = {}
            for layer, expected in layers.items():
                probe = self._probe_layer(product, layer)
                actual = probe.get("status", "error")

                if expected == "NO":
                    cell = "ok_expected_no"
                    counts["ok_expected_no"] += 1
                    product_results[layer] = {
                        "expected": "NO",
                        "actual": "n/a",
                        "health": cell,
                        "detail": "Not used by this product",
                    }
                    continue
                elif expected == "PARTIAL":
                    cell = "partial" if actual in ("ok", "partial") else "error"
                    counts["partial" if cell == "partial" else "error"] += 1
                elif expected in ("YES", "VIA_ORCHESTRATOR"):
                    if actual == "ok":
                        cell = "ok"
                        counts["ok"] += 1
                    else:
                        healed = self._self_heal(product, layer, probe)
                        if healed:
                            reprobe = self._probe_layer(product, layer)
                            if reprobe.get("status") == "ok":
                                cell = "healed"
                                counts["healed"] += 1
                                probe = reprobe
                            else:
                                cell = "error"
                                counts["error"] += 1
                        else:
                            cell = "error"
                            counts["error"] += 1
                else:
                    cell = "unknown"

                product_results[layer] = {
                    "expected": expected,
                    "actual": probe.get("status", "error"),
                    "health": cell,
                    "detail": probe.get("detail") or "",
                }
            matrix_results[product] = product_results

        # Probe Tier 2/3 infrastructure modules
        tier2_results: Dict[str, Any] = {}
        for mod_key, spec in _TIER2_MODULE_CHECKS.items():
            probe = self._probe_tier2_module(spec["module"], spec.get("check_attr"))
            status = probe.get("status", "error")
            if status == "ok":
                counts["ok"] += 1
            else:
                # Attempt reimport heal
                try:
                    if spec["module"] in sys.modules:
                        importlib.reload(sys.modules[spec["module"]])
                    else:
                        importlib.import_module(spec["module"])
                    reprobe = self._probe_tier2_module(
                        spec["module"], spec.get("check_attr")
                    )
                    if reprobe.get("status") == "ok":
                        counts["healed"] += 1
                        probe = reprobe
                        self._record_heal("tier2", mod_key, "reimport", True)
                    else:
                        counts["error"] += 1
                except Exception:
                    counts["error"] += 1
            tier2_results[mod_key] = {
                "status": probe.get("status", "error"),
                "detail": probe.get("detail") or "",
                "description": spec.get("description") or "",
            }

        # Probe extended v3.1 health indicators
        extended_health = self._probe_extended_health()
        for indicator, probe_result in extended_health.items():
            status = probe_result.get("status", "error")
            if status == "ok":
                counts["ok"] += 1
            elif status == "partial":
                counts["partial"] += 1
            else:
                counts["error"] += 1

        elapsed = round(time.time() - start, 3)
        total_cells = sum(counts.values())
        healthy = (
            counts["ok"]
            + counts["ok_expected_no"]
            + counts["partial"]
            + counts["healed"]
        )
        health_pct = round(healthy / total_cells * 100, 1) if total_cells else 0

        result = {
            "status": "healthy" if counts["error"] == 0 else "degraded",
            "health_pct": health_pct,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "check_number": self._check_count + 1,
            "check_duration_seconds": elapsed,
            "summary": counts,
            "matrix": matrix_results,
            "tier2_modules": tier2_results,
            "extended_health": extended_health,
            "recent_heal_actions": list(self._heal_log[-10:]),
        }

        with self._lock:
            self._last_result = result
            self._last_check_time = time.time()
            self._check_count += 1

        logger.info(
            "DataMatrixMonitor: check #%d -- %s (%.1f%% healthy, %d errors, %d healed, %.3fs)",
            self._check_count,
            result["status"],
            health_pct,
            counts["error"],
            counts["healed"],
            elapsed,
        )

        # Alert on unhealed errors (suppress during startup -- KB takes 5-10min to load)
        error_count = counts.get("error") or 0
        is_startup_check = self._check_count <= _STARTUP_ALERT_SUPPRESSION_CHECKS
        if error_count > 0 and is_startup_check:
            logger.info(
                "DataMatrixMonitor: suppressing alert for check #%d "
                "(%d errors) -- startup grace period, KB still loading",
                self._check_count,
                error_count,
            )
        elif error_count > 0:
            # Collect error cells for the alert body
            error_cells: list[str] = []
            for product, layers in matrix_results.items():
                for layer, info in layers.items():
                    if isinstance(info, dict) and info.get("health") == "error":
                        error_cells.append(f"{product}/{layer}")
            for mod_key, info in tier2_results.items():
                if isinstance(info, dict) and info.get("status") == "error":
                    error_cells.append(f"tier2/{mod_key}")
            send_alert(
                subject=f"Data Matrix: {error_count} check(s) failed after self-heal",
                body=(
                    f"<p><b>{error_count}</b> data matrix check(s) failed and "
                    f"could not be self-healed.</p>"
                    f"<p>Failed cells: {', '.join(error_cells[:15]) or 'unknown'}</p>"
                    f"<p>Health: {health_pct}% | Healed: {counts.get('healed') or 0} | "
                    f"Duration: {elapsed}s</p>"
                    f"<p>Check: <code>/api/health/data-matrix</code></p>"
                ),
                severity="critical" if error_count >= 5 else "warning",
            )

        return result

    # ── Background loop ───────────────────────────────────────────────────

    def _background_loop(self) -> None:
        """Sleep -> check -> heal -> repeat (daemon thread)."""
        logger.info("DataMatrixMonitor: background loop started")
        time.sleep(_INITIAL_DELAY)
        while True:
            try:
                self.run_check()
            except Exception as e:
                logger.error("DataMatrixMonitor: check failed: %s", e, exc_info=True)
            # Memory pressure check every cycle
            try:
                self._check_memory_pressure()
            except Exception as e:
                logger.debug("DataMatrixMonitor: memory pressure check failed: %s", e)
            # Config drift check every cycle
            try:
                self._check_config_drift()
            except Exception as e:
                logger.debug("DataMatrixMonitor: config drift check failed: %s", e)
            time.sleep(_CHECK_INTERVAL)

    # ── Probes ────────────────────────────────────────────────────────────

    def _probe_layer(self, product: str, layer: str) -> Dict[str, Any]:
        """Probe a single cell in the matrix."""
        try:
            if layer == "json_files":
                return self._probe_json_files()
            elif layer == "claude_api":
                return self._probe_claude_api(product)
            elif layer == "data_synthesizer":
                return self._probe_direct_module(
                    product, "data_synthesizer", check_attr="synthesize"
                )
            elif layer in _ORCH_LAYER_MAP:
                return self._probe_data_layer(product, layer)
            return {"status": "error", "detail": f"Unknown layer: {layer}"}
        except Exception as e:
            return {"status": "error", "detail": f"Probe exception: {e}"}

    def _probe_json_files(self) -> Dict[str, Any]:
        """Check KB JSON files exist and are parseable (cached per run_check)."""
        if self._json_probe_cache is not None:
            return self._json_probe_cache
        missing, corrupt = [], []
        for fname in _REQUIRED_KB_FILES:
            fpath = DATA_DIR / fname
            if not fpath.exists():
                missing.append(fname)
            else:
                try:
                    with open(fpath, "r") as f:
                        json.load(f)
                except (json.JSONDecodeError, OSError):
                    corrupt.append(fname)
        ok = not missing and not corrupt
        detail = f"{len(_REQUIRED_KB_FILES)}/{len(_REQUIRED_KB_FILES)} files valid"
        if missing:
            detail = f"Missing: {', '.join(missing)}"
        elif corrupt:
            detail = f"Corrupt: {', '.join(corrupt)}"
        result = {"status": "ok" if ok else "error", "detail": detail}
        self._json_probe_cache = result
        return result

    def _probe_claude_api(self, product: str) -> Dict[str, Any]:
        """Check Claude API availability."""
        if product in ("excel_ppt", "ppt_generator"):
            return {"status": "ok_expected_no", "detail": "Not used by this product"}
        key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        if key:
            return {"status": "ok", "detail": "ANTHROPIC_API_KEY is set"}
        return {"status": "error", "detail": "ANTHROPIC_API_KEY not set"}

    def _probe_data_layer(self, product: str, layer: str) -> Dict[str, Any]:
        """Probe a data layer for a specific product."""
        if product == "excel_ppt":
            # Direct import expected
            module_name = layer
            return self._check_sys_module(module_name)
        elif product in ("nova_chat", "slack_bot"):
            # Via orchestrator -- check module is loaded (orchestrator accesses
            # it through registered handlers; if module is in sys.modules it's
            # available to the orchestrator)
            underlying_module = _ORCH_LAYER_MAP.get(layer, layer)
            return self._check_orchestrator_module(underlying_module, layer)
        elif product == "ppt_generator":
            if layer == "research":
                return self._check_sys_module("research")
            # PARTIAL for others: PPT receives pre-computed data from app.py
            return {
                "status": "partial",
                "detail": "Receives pre-computed data from app.py pipeline",
            }
        return {"status": "error", "detail": f"Unknown product: {product}"}

    def _probe_direct_module(
        self, product: str, module_name: str, check_attr: Optional[str] = None
    ) -> Dict[str, Any]:
        """Probe a module that's directly imported (not via orchestrator)."""
        if product == "excel_ppt":
            return self._check_sys_module(module_name, check_attr)
        elif product in ("nova_chat", "slack_bot"):
            return {
                "status": "ok_expected_no",
                "detail": "Intentionally excluded (too heavy for real-time chat)",
            }
        elif product == "ppt_generator":
            # PPT receives synthesized data from app.py
            return {
                "status": "partial",
                "detail": "Receives pre-computed synthesized data from app.py",
            }
        return {"status": "error", "detail": f"Unknown product: {product}"}

    # ── Probe helpers ─────────────────────────────────────────────────────

    def _check_sys_module(
        self, module_name: str, check_attr: Optional[str] = None
    ) -> Dict[str, Any]:
        """Check if a module is loaded in sys.modules."""
        if module_name not in sys.modules:
            return {"status": "error", "detail": f"{module_name} not in sys.modules"}
        mod = sys.modules[module_name]
        if check_attr and not hasattr(mod, check_attr):
            return {"status": "error", "detail": f"{module_name}.{check_attr} missing"}
        return {"status": "ok", "detail": f"{module_name} loaded"}

    def _check_orchestrator_module(
        self, module_name: str, layer_name: str
    ) -> Dict[str, Any]:
        """Check if a module is accessible via the DataOrchestrator.

        The DataOrchestrator class uses registered source handlers to access
        data modules. The simplest and most reliable health check is verifying
        (1) data_orchestrator itself is loaded, and (2) the underlying module
        is in sys.modules (meaning it was successfully imported at startup).
        """
        try:
            if "data_orchestrator" not in sys.modules:
                return {"status": "error", "detail": "data_orchestrator not loaded"}
            # Check the underlying module is loaded
            if module_name in sys.modules:
                return {
                    "status": "ok",
                    "detail": f"{layer_name} available via orchestrator ({module_name} loaded)",
                }
            # Not in sys.modules -- try importing to see if it's importable
            try:
                importlib.import_module(module_name)
                return {
                    "status": "ok",
                    "detail": f"{layer_name} importable via orchestrator",
                }
            except ImportError:
                return {
                    "status": "error",
                    "detail": f"{module_name} not loaded and not importable",
                }
        except Exception as e:
            return {"status": "error", "detail": str(e)}

    def _probe_tier2_module(
        self, module_name: str, check_attr: Optional[str] = None
    ) -> Dict[str, Any]:
        """Probe a Tier 2/3 infrastructure module by importability."""
        try:
            if module_name in sys.modules:
                mod = sys.modules[module_name]
                if check_attr and not hasattr(mod, check_attr):
                    return {
                        "status": "error",
                        "detail": f"{module_name} loaded but {check_attr} missing",
                    }
                return {"status": "ok", "detail": f"{module_name} loaded and healthy"}
            # Try importing
            mod = importlib.import_module(module_name)
            if check_attr and not hasattr(mod, check_attr):
                return {
                    "status": "error",
                    "detail": f"{module_name} importable but {check_attr} missing",
                }
            return {"status": "ok", "detail": f"{module_name} importable and healthy"}
        except Exception as e:
            return {"status": "error", "detail": f"{module_name} import failed: {e}"}

    def _probe_extended_health(self) -> Dict[str, Dict[str, Any]]:
        """Probe v3.1 extended health indicators beyond the product x layer matrix."""
        results: Dict[str, Dict[str, Any]] = {}

        # 1. LLM Router provider health
        try:
            if "llm_router" in sys.modules:
                lr = sys.modules["llm_router"]
                status = lr.get_router_status()
                providers = status.get("providers", {})
                available = sum(
                    1
                    for p in providers.values()
                    if isinstance(p, dict) and p.get("available", False)
                )
                results["llm_providers"] = {
                    "status": "ok" if available > 0 else "error",
                    "detail": f"{available}/{len(providers)} providers available",
                    "providers": {
                        k: v.get("available", False)
                        for k, v in providers.items()
                        if isinstance(v, dict)
                    },
                }
            else:
                results["llm_providers"] = {
                    "status": "partial",
                    "detail": "llm_router not loaded yet",
                }
        except Exception as e:
            logger.error(
                "extended_health: llm_providers probe failed: %s", e, exc_info=True
            )
            results["llm_providers"] = {"status": "error", "detail": str(e)}

        # 2. Async job queue depth
        try:
            if "app" in sys.modules:
                app_mod = sys.modules["app"]
                jobs = getattr(app_mod, "_generation_jobs", {})
                total = len(jobs)
                by_status = {}
                for jdata in jobs.values():
                    s = (
                        jdata.get("status", "unknown")
                        if isinstance(jdata, dict)
                        else "unknown"
                    )
                    by_status[s] = by_status.get(s, 0) + 1
                results["async_job_queue"] = {
                    "status": "ok" if total < 100 else "error",
                    "detail": f"{total} jobs ({by_status})",
                    "total": total,
                    "by_status": by_status,
                }
            else:
                results["async_job_queue"] = {
                    "status": "ok",
                    "detail": "app not loaded (0 jobs)",
                    "total": 0,
                }
        except Exception as e:
            logger.error(
                "extended_health: async_job_queue probe failed: %s", e, exc_info=True
            )
            results["async_job_queue"] = {"status": "error", "detail": str(e)}

        # 3. API key usage tracking
        try:
            keys_configured = 0
            key_names = [
                ("ANTHROPIC_API_KEY", "claude"),
                ("GEMINI_API_KEY", "gemini"),
                ("GROQ_API_KEY", "groq"),
                ("CEREBRAS_API_KEY", "cerebras"),
            ]
            provider_status = {}
            for env_key, name in key_names:
                has_key = bool(os.environ.get(env_key, "").strip())
                provider_status[name] = has_key
                if has_key:
                    keys_configured += 1
            results["api_keys"] = {
                "status": (
                    "ok"
                    if keys_configured >= 2
                    else ("partial" if keys_configured >= 1 else "error")
                ),
                "detail": f"{keys_configured}/4 API keys configured",
                "providers": provider_status,
            }
        except Exception as e:
            logger.warning("extended_health: api_keys probe failed: %s", e)
            results["api_keys"] = {"status": "error", "detail": str(e)}

        # 4. KB file freshness
        try:
            stale_files = []
            now = time.time()
            stale_threshold = 90 * 86400  # 90 days
            for fname in _REQUIRED_KB_FILES:
                fpath = DATA_DIR / fname
                if fpath.exists():
                    age = now - fpath.stat().st_mtime
                    if age > stale_threshold:
                        stale_files.append(f"{fname} ({int(age/86400)}d old)")
            results["kb_freshness"] = {
                "status": "ok" if not stale_files else "partial",
                "detail": f"{len(_REQUIRED_KB_FILES)} files, {len(stale_files)} stale (>90d)",
                "stale_files": stale_files[:5],
            }
        except Exception as e:
            logger.error(
                "extended_health: kb_freshness probe failed: %s", e, exc_info=True
            )
            results["kb_freshness"] = {"status": "error", "detail": str(e)}

        # 5. Eval score trend
        try:
            if "eval_framework" in sys.modules:
                ef = sys.modules["eval_framework"]
                suite = ef.EvalSuite()
                scores = suite.run_full_eval()
                overall = scores.get("overall_score") or 0
                # categories is Dict[str, float] from run_full_eval() --
                # type-guard: if values are dicts (future change), extract score_pct;
                # if floats (current), use directly.
                raw_cats = scores.get("categories", {})
                safe_cats = {}
                for k, v in raw_cats.items():
                    if isinstance(v, (int, float)):
                        safe_cats[k] = v
                    elif isinstance(v, dict):
                        safe_cats[k] = v.get("score_pct", v.get("score") or 0)
                    else:
                        safe_cats[k] = 0
                results["eval_score"] = {
                    "status": (
                        "ok"
                        if overall >= 85
                        else ("partial" if overall >= 70 else "error")
                    ),
                    "detail": f"Overall eval score: {overall}%",
                    "score": overall,
                    "categories": safe_cats,
                }
            else:
                results["eval_score"] = {
                    "status": "partial",
                    "detail": "eval_framework not loaded",
                }
        except Exception as e:
            logger.error(
                "extended_health: eval_score probe failed: %s", e, exc_info=True
            )
            results["eval_score"] = {"status": "error", "detail": str(e)}

        # 6. Regression baseline age
        try:
            baseline_path = (
                Path(__file__).resolve().parent
                / "data"
                / "persistent"
                / "regression_baseline.json"
            )
            if baseline_path.exists():
                age_days = (time.time() - baseline_path.stat().st_mtime) / 86400
                results["regression_baseline"] = {
                    "status": (
                        "ok"
                        if age_days < 30
                        else ("partial" if age_days < 60 else "error")
                    ),
                    "detail": f"Baseline age: {age_days:.1f} days",
                    "age_days": round(age_days, 1),
                }
            else:
                results["regression_baseline"] = {
                    "status": "partial",
                    "detail": "No baseline saved yet (first run will create it)",
                }
        except Exception as e:
            logger.error(
                "extended_health: regression_baseline probe failed: %s",
                e,
                exc_info=True,
            )
            results["regression_baseline"] = {"status": "error", "detail": str(e)}

        # 7. v3.5 Conversational routing health
        try:
            if "nova" not in sys.modules:
                try:
                    import nova as _nova_import  # noqa: F811,F401
                except Exception:
                    pass
            if "nova" in sys.modules:
                nova_mod = sys.modules["nova"]
                nova_cls = getattr(nova_mod, "Nova", None)
                if nova_cls:
                    has_conv = hasattr(nova_cls, "_query_is_conversational")
                    has_patterns = hasattr(nova_cls, "_CONVERSATIONAL_PATTERNS")
                    has_tool_check = hasattr(nova_mod, "_response_uses_tool_data")
                    all_present = has_conv and has_patterns and has_tool_check
                    results["v35_routing"] = {
                        "status": "ok" if all_present else "error",
                        "detail": (
                            "v3.5 inverted routing active"
                            if all_present
                            else f"missing: conv={has_conv}, patterns={has_patterns}, tool_check={has_tool_check}"
                        ),
                        "version": "3.5" if all_present else "3.4",
                    }
                else:
                    results["v35_routing"] = {
                        "status": "partial",
                        "detail": "Nova class not found",
                    }
            else:
                results["v35_routing"] = {
                    "status": "partial",
                    "detail": "nova module not loaded",
                }
        except Exception as e:
            logger.error(
                "extended_health: v35_routing probe failed: %s", e, exc_info=True
            )
            results["v35_routing"] = {"status": "error", "detail": str(e)}

        # 8. API health (FRED, BLS, Adzuna, O*NET)
        try:
            results["api_health"] = self._check_api_health()
        except Exception as e:
            logger.error(
                "extended_health: api_health probe failed: %s",
                e,
                exc_info=True,
            )
            results["api_health"] = {
                "name": "api_health",
                "status": "error",
                "detail": str(e),
                "healed": False,
            }

        # 9. Supabase connectivity
        try:
            results["supabase_health"] = self._check_supabase_health()
        except Exception as e:
            logger.error(
                "extended_health: supabase_health probe failed: %s",
                e,
                exc_info=True,
            )
            results["supabase_health"] = {
                "name": "supabase_health",
                "status": "error",
                "detail": str(e),
                "healed": False,
            }

        # 10. Cache staleness (data/ freshness vs FRESHNESS_THRESHOLDS)
        try:
            results["cache_staleness"] = self._check_cache_staleness()
        except Exception as e:
            logger.error(
                "extended_health: cache_staleness probe failed: %s",
                e,
                exc_info=True,
            )
            results["cache_staleness"] = {
                "name": "cache_staleness",
                "status": "error",
                "detail": str(e),
                "healed": False,
            }

        # 11. Disk space
        try:
            results["disk_space"] = self._check_disk_space()
        except Exception as e:
            logger.error(
                "extended_health: disk_space probe failed: %s",
                e,
                exc_info=True,
            )
            results["disk_space"] = {
                "name": "disk_space",
                "status": "error",
                "detail": str(e),
                "healed": False,
            }

        return results

    # ── Extended self-healing checks (v4) ─────────────────────────────────

    def _check_api_health(self) -> Dict[str, Any]:
        """Test each configured external API key with a minimal request.

        Checks FRED, BLS, Adzuna, and O*NET APIs. Skips any API whose
        key is not set. Reports 'degraded' (not 'error') on failure so
        the system keeps running with reduced capability.

        Returns:
            Dict with name, status, detail, and healed keys.
        """
        api_results: Dict[str, str] = {}
        failed: List[str] = []

        # -- FRED --
        fred_key = os.environ.get("FRED_API_KEY") or ""
        if fred_key:
            try:
                url = (
                    f"https://api.stlouisfed.org/fred/series"
                    f"?series_id=GNPCA&api_key={fred_key}&file_type=json"
                )
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=_API_HEALTH_TIMEOUT) as resp:
                    if resp.status == 200:
                        api_results["fred"] = "ok"
                    else:
                        api_results["fred"] = "degraded"
                        failed.append("fred")
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                logger.warning("API health: FRED check failed: %s", e)
                api_results["fred"] = "degraded"
                failed.append("fred")
        else:
            api_results["fred"] = "skipped"

        # -- BLS --
        bls_key = os.environ.get("BLS_API_KEY") or ""
        if bls_key:
            try:
                payload = json.dumps(
                    {
                        "seriesid": ["CUUR0000SA0"],
                        "registrationkey": bls_key,
                        "latest": True,
                    }
                ).encode("utf-8")
                req = urllib.request.Request(
                    "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=_API_HEALTH_TIMEOUT) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                    if body.get("status") == "REQUEST_SUCCEEDED":
                        api_results["bls"] = "ok"
                    else:
                        api_results["bls"] = "degraded"
                        failed.append("bls")
            except (
                urllib.error.URLError,
                OSError,
                TimeoutError,
                json.JSONDecodeError,
            ) as e:
                logger.warning("API health: BLS check failed: %s", e)
                api_results["bls"] = "degraded"
                failed.append("bls")
        else:
            api_results["bls"] = "skipped"

        # -- Adzuna --
        adzuna_id = os.environ.get("ADZUNA_APP_ID") or ""
        adzuna_key = os.environ.get("ADZUNA_APP_KEY") or ""
        if adzuna_id and adzuna_key:
            try:
                url = (
                    f"https://api.adzuna.com/v1/api/jobs/us/search/1"
                    f"?app_id={adzuna_id}&app_key={adzuna_key}"
                    f"&results_per_page=1"
                )
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=_API_HEALTH_TIMEOUT) as resp:
                    if resp.status == 200:
                        api_results["adzuna"] = "ok"
                    else:
                        api_results["adzuna"] = "degraded"
                        failed.append("adzuna")
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                logger.warning("API health: Adzuna check failed: %s", e)
                api_results["adzuna"] = "degraded"
                failed.append("adzuna")
        else:
            api_results["adzuna"] = "skipped"

        # -- O*NET --
        onet_key = (
            os.environ.get("ONET_API_KEY") or os.environ.get("ONET_PASSWORD") or ""
        )
        if onet_key:
            try:
                url = "https://api-v2.onetcenter.org/online/occupations/15-1252.00"
                req = urllib.request.Request(url, method="GET")
                req.add_header("X-API-Key", onet_key)
                req.add_header("Accept", "application/json")
                with urllib.request.urlopen(req, timeout=_API_HEALTH_TIMEOUT) as resp:
                    if resp.status == 200:
                        api_results["onet"] = "ok"
                    else:
                        api_results["onet"] = "degraded"
                        failed.append("onet")
            except urllib.error.HTTPError as e:
                if e.code in (401, 403):
                    logger.warning(
                        "API health: O*NET auth failed (HTTP %s) -- check ONET_API_KEY",
                        e.code,
                    )
                    api_results["onet"] = "auth_error"
                else:
                    logger.warning("API health: O*NET check failed: HTTP %s", e.code)
                    api_results["onet"] = "degraded"
                    failed.append("onet")
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                logger.warning("API health: O*NET check failed: %s", e)
                api_results["onet"] = "degraded"
                failed.append("onet")
        else:
            api_results["onet"] = "skipped"

        configured = [k for k, v in api_results.items() if v != "skipped"]
        ok_count = sum(1 for v in api_results.values() if v == "ok")

        if not configured:
            status = "warning"
            detail = "No external API keys configured"
        elif failed:
            status = "warning"
            detail = (
                f"{ok_count}/{len(configured)} APIs healthy; "
                f"degraded: {', '.join(failed)}"
            )
        else:
            status = "ok"
            detail = f"{ok_count}/{len(configured)} APIs healthy"

        return {
            "name": "api_health",
            "status": status,
            "detail": detail,
            "healed": False,
            "apis": api_results,
        }

    def _check_supabase_health(self) -> Dict[str, Any]:
        """Test Supabase connectivity with a minimal REST query.

        Self-heals by setting ``supabase_fallback_to_local`` so products
        fall back to local JSON files when Supabase is unreachable.

        Returns:
            Dict with name, status, detail, and healed keys.
        """
        sb_url = os.environ.get("SUPABASE_URL") or ""
        sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""

        if not sb_url or not sb_key:
            return {
                "name": "supabase_health",
                "status": "warning",
                "detail": "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set",
                "healed": False,
            }

        try:
            url = (
                f"{sb_url.rstrip('/')}/rest/v1/knowledge_base" f"?select=count&limit=1"
            )
            req = urllib.request.Request(url, method="GET")
            req.add_header("apikey", sb_key)
            req.add_header("Authorization", f"Bearer {sb_key}")
            with urllib.request.urlopen(req, timeout=_API_HEALTH_TIMEOUT) as resp:
                if resp.status == 200:
                    self.supabase_fallback_to_local = False
                    return {
                        "name": "supabase_health",
                        "status": "ok",
                        "detail": "Supabase reachable",
                        "healed": False,
                    }
                else:
                    self.supabase_fallback_to_local = True
                    self._record_heal(
                        "system",
                        "supabase",
                        "fallback_to_local_json",
                        True,
                    )
                    return {
                        "name": "supabase_health",
                        "status": "warning",
                        "detail": (
                            f"Supabase returned status {resp.status}; "
                            f"fell back to local JSON"
                        ),
                        "healed": True,
                    }
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            logger.warning("Supabase health check failed: %s", e)
            self.supabase_fallback_to_local = True
            self._record_heal("system", "supabase", "fallback_to_local_json", True)
            return {
                "name": "supabase_health",
                "status": "warning",
                "detail": (f"Supabase unreachable ({e}); fell back to local JSON"),
                "healed": True,
            }

    def _check_cache_staleness(self) -> Dict[str, Any]:
        """Check data/ JSON freshness against FRESHNESS_THRESHOLDS.

        Compares each file's modification time to the threshold defined in
        ``data_enrichment.FRESHNESS_THRESHOLDS``. Self-heals by triggering
        the enrichment engine's ``run_cycle`` for stale sources.

        Returns:
            Dict with name, status, detail, and healed keys.
        """
        stale_sources: List[str] = []
        checked = 0

        try:
            from data_enrichment import FRESHNESS_THRESHOLDS
        except ImportError:
            return {
                "name": "cache_staleness",
                "status": "warning",
                "detail": (
                    "data_enrichment module not importable; " "skipped staleness check"
                ),
                "healed": False,
            }

        now = time.time()
        for source, threshold_hours in FRESHNESS_THRESHOLDS.items():
            # Convention: data/<source>.json or data/enrichment_<source>.json
            candidates = [
                DATA_DIR / f"{source}.json",
                DATA_DIR / f"enrichment_{source}.json",
            ]
            found = False
            for fpath in candidates:
                if fpath.exists():
                    found = True
                    checked += 1
                    age_hours = (now - fpath.stat().st_mtime) / 3600
                    if age_hours > threshold_hours:
                        stale_sources.append(
                            f"{source} ({age_hours:.0f}h > {threshold_hours}h)"
                        )
                    break
            if not found:
                stale_sources.append(f"{source} (file missing)")

        healed = False
        if stale_sources:
            # Self-heal: trigger enrichment cycle in background thread
            try:
                from data_enrichment import get_engine

                engine = get_engine()
                threading.Thread(
                    target=engine.run_cycle,
                    name="heal-cache-staleness",
                    daemon=True,
                ).start()
                healed = True
                self._record_heal(
                    "system",
                    "cache_staleness",
                    f"triggered enrichment for {len(stale_sources)} stale sources",
                    True,
                )
            except (ImportError, RuntimeError, TypeError) as e:
                logger.warning("Cache staleness self-heal failed: %s", e)
                self._record_heal(
                    "system",
                    "cache_staleness",
                    f"enrichment trigger failed: {e}",
                    False,
                )

        if not stale_sources:
            status = "ok"
            detail = f"{checked} sources within freshness thresholds"
        else:
            status = "warning"
            detail = f"{len(stale_sources)} stale: " f"{', '.join(stale_sources[:5])}"

        return {
            "name": "cache_staleness",
            "status": status,
            "detail": detail,
            "healed": healed,
            "stale_sources": stale_sources,
        }

    def _check_disk_space(self) -> Dict[str, Any]:
        """Check free disk space and clear old cache files if low.

        Warns if less than 500 MB free. Self-heals by removing files
        from ``data/cache/`` (if that directory exists).

        Returns:
            Dict with name, status, detail, and healed keys.
        """
        try:
            usage = shutil.disk_usage(DATA_DIR)
        except OSError as e:
            return {
                "name": "disk_space",
                "status": "error",
                "detail": f"Could not read disk usage: {e}",
                "healed": False,
            }

        free_mb = usage.free / (1024 * 1024)
        healed = False

        if free_mb >= 500:
            return {
                "name": "disk_space",
                "status": "ok",
                "detail": f"{free_mb:.0f} MB free",
                "healed": False,
            }

        # Self-heal: clear old cache files (oldest first)
        cache_dir = DATA_DIR / "cache"
        cleared = 0
        freed_bytes = 0
        if cache_dir.is_dir():
            try:
                for entry in sorted(
                    cache_dir.iterdir(),
                    key=lambda p: p.stat().st_mtime,
                ):
                    if entry.is_file():
                        sz = entry.stat().st_size
                        entry.unlink()
                        cleared += 1
                        freed_bytes += sz
                if cleared:
                    healed = True
                    self._record_heal(
                        "system",
                        "disk_space",
                        (
                            f"cleared {cleared} cache files "
                            f"({freed_bytes / 1024:.0f} KB)"
                        ),
                        True,
                    )
            except OSError as e:
                logger.warning("Disk space self-heal failed: %s", e)
                self._record_heal(
                    "system",
                    "disk_space",
                    f"cache cleanup failed: {e}",
                    False,
                )

        return {
            "name": "disk_space",
            "status": "warning",
            "detail": (
                f"{free_mb:.0f} MB free (< 500 MB)"
                + (f"; cleared {cleared} cache files" if cleared else "")
            ),
            "healed": healed,
        }

    # ── Self-healing ──────────────────────────────────────────────────────

    def _self_heal(
        self, product: str, layer: str, probe_result: Dict[str, Any]
    ) -> bool:
        """Attempt to fix a broken connection. Returns True if action taken."""
        healed = False

        # Strategy 1: Re-import failed modules (only for direct-import products)
        if layer in (
            "api_enrichment",
            "research",
            "data_synthesizer",
            "budget_engine",
            "standardizer",
            "trend_engine",
            "collar_intelligence",
        ) and product in ("excel_ppt", "ppt_generator"):
            try:
                if layer in sys.modules:
                    importlib.reload(sys.modules[layer])
                else:
                    importlib.import_module(layer)
                healed = True
                self._record_heal(product, layer, "reimport", True)
            except Exception as e:
                self._record_heal(product, layer, "reimport", False)
                logger.warning("Self-heal reimport %s failed: %s", layer, e)

        # Strategy 2a: Import data_orchestrator itself if missing
        if layer in _ORCH_LAYER_MAP and product in ("nova_chat", "slack_bot"):
            if "data_orchestrator" not in sys.modules:
                try:
                    importlib.import_module("data_orchestrator")
                    healed = True
                    self._record_heal(product, layer, "import_data_orchestrator", True)
                except Exception as e:
                    self._record_heal(product, layer, "import_data_orchestrator", False)
                    logger.warning("Self-heal import data_orchestrator failed: %s", e)

        # Strategy 2b: Re-import the underlying module for orchestrator layers
        if layer in _ORCH_LAYER_MAP and product in ("nova_chat", "slack_bot"):
            module_name = _ORCH_LAYER_MAP[layer]
            try:
                if module_name in sys.modules:
                    importlib.reload(sys.modules[module_name])
                else:
                    importlib.import_module(module_name)
                healed = True
                self._record_heal(product, layer, "reimport_orchestrator_module", True)
            except Exception as e:
                self._record_heal(product, layer, "reimport_orchestrator_module", False)
                logger.warning(
                    "Self-heal reimport %s for orchestrator failed: %s", module_name, e
                )

        # Strategy 3: Reset Nova's _orchestrator if it's False
        if product in ("nova_chat", "slack_bot"):
            try:
                if "nova" in sys.modules:
                    nova_mod = sys.modules["nova"]
                    if getattr(nova_mod, "_orchestrator", None) is False:
                        lock = getattr(nova_mod, "_orchestrator_lock", None)
                        if lock:
                            with lock:
                                nova_mod._orchestrator = None
                        else:
                            nova_mod._orchestrator = None
                        healed = True
                        self._record_heal(
                            product, layer, "reset_nova_orchestrator", True
                        )
            except Exception as e:
                self._record_heal(product, layer, "reset_nova_orchestrator", False)
                logger.warning("Self-heal reset nova orchestrator failed: %s", e)

        # Strategy 4: Evict stale orchestrator API cache entries
        if layer == "api_enrichment":
            try:
                if "data_orchestrator" in sys.modules:
                    do = sys.modules["data_orchestrator"]
                    now = time.time()
                    expired = []
                    with do._api_cache_lock:
                        for k, v in do._api_result_cache.items():
                            if now >= v.get("expires") or 0:
                                expired.append(k)
                        for k in expired:
                            do._api_result_cache.pop(k, None)
                    if expired:
                        self._record_heal(
                            product,
                            layer,
                            f"cleared_{len(expired)}_stale_cache_entries",
                            True,
                        )
            except Exception as e:
                logger.warning("Self-heal cache clear failed: %s", e)

        return healed

    def _record_heal(
        self, product: str, layer: str, action: str, success: bool
    ) -> None:
        """Record a healing action (bounded, thread-safe)."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "product": product,
            "layer": layer,
            "action": action,
            "success": success,
        }
        with self._lock:
            self._heal_log.append(entry)
            if len(self._heal_log) > _MAX_HEAL_LOG:
                self._heal_log = self._heal_log[-_MAX_HEAL_LOG:]
        level = logging.INFO if success else logging.WARNING
        logger.log(
            level,
            "DataMatrixMonitor: heal %s/%s -- %s (success=%s)",
            product,
            layer,
            action,
            success,
        )

    # ── Memory pressure auto-recovery ─────────────────────────────────────

    def _check_memory_pressure(self) -> Dict[str, Any]:
        """Check RSS memory and take recovery actions if thresholds exceeded.

        Thresholds:
            > 1 GB: gc.collect, evict ALL expired cache entries, compact audit log
            > 1.5 GB: additionally clear entire L1 memory cache
        Returns dict with actions taken (empty if no pressure detected).
        """
        actions: List[str] = []
        rss_mb = -1.0
        try:
            # Get RSS via /proc (Linux) or resource module (Unix)
            try:
                with open("/proc/self/status", "r") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            rss_mb = int(line.split()[1]) / 1024.0
                            break
            except (FileNotFoundError, PermissionError, ValueError):
                import resource as _res

                rss = _res.getrusage(_res.RUSAGE_SELF).ru_maxrss
                if sys.platform == "darwin":
                    rss_mb = rss / (1024 * 1024)  # bytes -> MB on macOS
                else:
                    rss_mb = rss / 1024.0  # KB -> MB on Linux

            if rss_mb < 0 or rss_mb < 1024:
                return {"rss_mb": round(rss_mb, 1), "actions": actions}

            # ── Tier 1: RSS > 1 GB ──
            collected = gc.collect()
            actions.append(f"gc.collect freed {collected} objects")

            # Evict ALL expired orchestrator cache entries
            try:
                if "data_orchestrator" in sys.modules:
                    do = sys.modules["data_orchestrator"]
                    now = time.time()
                    with do._api_cache_lock:
                        expired = [
                            k
                            for k, v in do._api_result_cache.items()
                            if now >= v.get("expires") or 0
                        ]
                        for k in expired:
                            do._api_result_cache.pop(k, None)
                    if expired:
                        actions.append(f"evicted {len(expired)} expired cache entries")
            except Exception as e:
                logger.debug("Memory heal: cache eviction failed: %s", e)

            # Compact heal log
            with self._lock:
                if len(self._heal_log) > 10:
                    self._heal_log = self._heal_log[-10:]
                    actions.append("compacted audit/heal log to 10 entries")

            # ── Tier 2: RSS > 1.5 GB ──
            if rss_mb > 1536:
                try:
                    if "api_enrichment" in sys.modules:
                        ae = sys.modules["api_enrichment"]
                        with ae._cache_lock:
                            cleared = len(ae._memory_cache)
                            ae._memory_cache.clear()
                        actions.append(f"cleared L1 memory cache ({cleared} entries)")
                except Exception as e:
                    logger.debug("Memory heal: L1 cache clear failed: %s", e)

            for action in actions:
                self._record_heal("system", "memory", action, True)
            logger.warning(
                "DataMatrixMonitor: memory pressure recovery (RSS=%.0fMB): %s",
                rss_mb,
                "; ".join(actions),
            )

        except Exception as e:
            logger.debug("Memory pressure check failed: %s", e)
            actions.append(f"check_error: {e}")

        return {"rss_mb": round(rss_mb, 1), "actions": actions}

    # ── Config drift detection ────────────────────────────────────────────

    @staticmethod
    def _snapshot_env_hashes() -> Dict[str, str]:
        """Hash (not store) the current values of tracked env vars."""
        snap: Dict[str, str] = {}
        for var in _TRACKED_ENV_VARS:
            val = os.environ.get(var, "")
            snap[var] = hashlib.sha256(val.encode()).hexdigest()[:16]
        return snap

    def _check_config_drift(self) -> Dict[str, Any]:
        """Compare current env var hashes against startup snapshot.

        If any changed, log the drift and optionally reload affected modules.
        Returns dict with drift details.
        """
        current = self._snapshot_env_hashes()
        drifted: List[str] = []
        for var in _TRACKED_ENV_VARS:
            if current.get(var) != self._env_snapshot.get(var):
                drifted.append(var)

        self._last_drift_check = time.time()

        if not drifted:
            return {"status": "ok", "drifted_vars": []}

        logger.warning(
            "DataMatrixMonitor: config drift detected for: %s", ", ".join(drifted)
        )

        # Attempt module reload for affected components
        _VAR_TO_MODULE = {
            "ANTHROPIC_API_KEY": "llm_router",
            "GEMINI_API_KEY": "llm_router",
            "GROQ_API_KEY": "llm_router",
            "CEREBRAS_API_KEY": "llm_router",
            "SENTRY_DSN": None,  # sentry SDK inits once; skip reload
            "POSTHOG_API_KEY": None,  # analytics; skip reload
            "UPSTASH_REDIS_REST_URL": "upstash_cache",
            "SUPABASE_URL": "supabase_cache",
        }
        reloaded: List[str] = []
        for var in drifted:
            mod_name = _VAR_TO_MODULE.get(var)
            if mod_name and mod_name in sys.modules:
                try:
                    importlib.reload(sys.modules[mod_name])
                    reloaded.append(mod_name)
                    self._record_heal(
                        "system",
                        "config_drift",
                        f"reloaded {mod_name} (env {var} changed)",
                        True,
                    )
                except Exception as e:
                    logger.warning("Config drift: failed to reload %s: %s", mod_name, e)
                    self._record_heal(
                        "system",
                        "config_drift",
                        f"reload {mod_name} failed: {e}",
                        False,
                    )

        # Update snapshot to avoid re-alerting
        self._env_snapshot = current

        return {
            "status": "drifted",
            "drifted_vars": drifted,
            "modules_reloaded": reloaded,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL CONVENIENCE
# ═══════════════════════════════════════════════════════════════════════════════

_monitor: Optional[DataMatrixMonitor] = None
_monitor_lock = threading.Lock()


def get_data_matrix_monitor() -> DataMatrixMonitor:
    """Get or create the singleton DataMatrixMonitor (thread-safe)."""
    global _monitor
    if _monitor is None:
        with _monitor_lock:
            if _monitor is None:
                _monitor = DataMatrixMonitor()
    return _monitor
