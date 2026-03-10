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

import importlib
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 12 * 3600   # 12 hours
_INITIAL_DELAY = 60           # wait 60s after startup before first check
_MAX_HEAL_LOG = 20            # keep last N heal actions

DATA_DIR = Path(__file__).resolve().parent / "data"

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
        "json_files":           "YES",
        "api_enrichment":       "YES",
        "research":             "YES",
        "data_synthesizer":     "YES",
        "budget_engine":        "YES",
        "standardizer":         "YES",
        "claude_api":           "NO",
        "trend_engine":         "YES",
        "collar_intelligence":  "YES",
    },
    "nova_chat": {
        "json_files":           "YES",
        "api_enrichment":       "VIA_ORCHESTRATOR",
        "research":             "VIA_ORCHESTRATOR",
        "data_synthesizer":     "NO",
        "budget_engine":        "VIA_ORCHESTRATOR",
        "standardizer":         "VIA_ORCHESTRATOR",
        "claude_api":           "YES",
        "trend_engine":         "VIA_ORCHESTRATOR",
        "collar_intelligence":  "VIA_ORCHESTRATOR",
    },
    "slack_bot": {
        "json_files":           "YES",
        "api_enrichment":       "VIA_ORCHESTRATOR",
        "research":             "VIA_ORCHESTRATOR",
        "data_synthesizer":     "NO",
        "budget_engine":        "VIA_ORCHESTRATOR",
        "standardizer":         "VIA_ORCHESTRATOR",
        "claude_api":           "YES",
        "trend_engine":         "VIA_ORCHESTRATOR",
        "collar_intelligence":  "VIA_ORCHESTRATOR",
    },
    "ppt_generator": {
        "json_files":           "YES",
        "api_enrichment":       "PARTIAL",
        "research":             "YES",
        "data_synthesizer":     "PARTIAL",
        "budget_engine":        "PARTIAL",
        "standardizer":         "NO",
        "claude_api":           "NO",
        "trend_engine":         "PARTIAL",
        "collar_intelligence":  "PARTIAL",
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

# Map data layers to orchestrator lazy-loader function names and global var names
_ORCH_LAYER_MAP = {
    "api_enrichment":      ("_lazy_api", "_api_enrichment"),
    "research":            ("_lazy_research", "_research"),
    "budget_engine":       ("_lazy_budget", "_budget_engine"),
    "standardizer":        ("_lazy_standardizer", "_standardizer"),
    "trend_engine":        ("_lazy_trend_engine", "_trend_engine"),
    "collar_intelligence": ("_lazy_collar_intel", "_collar_intel"),
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
        logger.info("DataMatrixMonitor: background thread started (interval=%ds)",
                     _CHECK_INTERVAL)

    def get_status(self) -> Dict[str, Any]:
        """Return the latest matrix check result for the endpoint."""
        with self._lock:
            if self._last_result is None:
                return {
                    "status": "pending",
                    "message": ("First check has not completed yet. "
                                f"Initial check runs {_INITIAL_DELAY}s after startup."),
                    "check_interval_hours": _CHECK_INTERVAL / 3600,
                }
            result = dict(self._last_result)
            result["age_seconds"] = round(time.time() - self._last_check_time, 1)
            result["next_check_in_seconds"] = max(
                0, round(_CHECK_INTERVAL - (time.time() - self._last_check_time), 1)
            )
            return result

    def run_check(self) -> Dict[str, Any]:
        """Run a full 4x7 matrix probe, attempt self-healing, return results."""
        start = time.time()
        self._json_probe_cache = None  # reset per-check cache
        matrix_results: Dict[str, Dict[str, Any]] = {}
        counts = {"ok": 0, "error": 0, "partial": 0,
                  "ok_expected_no": 0, "healed": 0}

        for product, layers in EXPECTED_MATRIX.items():
            product_results: Dict[str, Any] = {}
            for layer, expected in layers.items():
                probe = self._probe_layer(product, layer)
                actual = probe.get("status", "error")

                if expected == "NO":
                    cell = "ok_expected_no"
                    counts["ok_expected_no"] += 1
                    product_results[layer] = {
                        "expected": "NO", "actual": "n/a",
                        "health": cell, "detail": "Not used by this product",
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
                    "detail": probe.get("detail", ""),
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
                    reprobe = self._probe_tier2_module(spec["module"], spec.get("check_attr"))
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
                "detail": probe.get("detail", ""),
                "description": spec.get("description", ""),
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
        healthy = counts["ok"] + counts["ok_expected_no"] + counts["partial"] + counts["healed"]
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
            self._check_count, result["status"], health_pct,
            counts["error"], counts["healed"], elapsed,
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
                return self._probe_direct_module(product, "data_synthesizer",
                                                  check_attr="synthesize")
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
        key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
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
            # Via orchestrator
            lazy_fn_name, _ = _ORCH_LAYER_MAP[layer]
            return self._check_orchestrator_lazy(lazy_fn_name, layer)
        elif product == "ppt_generator":
            if layer == "research":
                return self._check_sys_module("research")
            # PARTIAL for others: PPT receives pre-computed data from app.py
            return {"status": "partial",
                    "detail": "Receives pre-computed data from app.py pipeline"}
        return {"status": "error", "detail": f"Unknown product: {product}"}

    def _probe_direct_module(self, product: str, module_name: str,
                              check_attr: Optional[str] = None) -> Dict[str, Any]:
        """Probe a module that's directly imported (not via orchestrator)."""
        if product == "excel_ppt":
            return self._check_sys_module(module_name, check_attr)
        elif product in ("nova_chat", "slack_bot"):
            return {"status": "ok_expected_no",
                    "detail": "Intentionally excluded (too heavy for real-time chat)"}
        elif product == "ppt_generator":
            # PPT receives synthesized data from app.py
            return {"status": "partial",
                    "detail": "Receives pre-computed synthesized data from app.py"}
        return {"status": "error", "detail": f"Unknown product: {product}"}

    # ── Probe helpers ─────────────────────────────────────────────────────

    def _check_sys_module(self, module_name: str,
                           check_attr: Optional[str] = None) -> Dict[str, Any]:
        """Check if a module is loaded in sys.modules."""
        if module_name not in sys.modules:
            return {"status": "error",
                    "detail": f"{module_name} not in sys.modules"}
        mod = sys.modules[module_name]
        if check_attr and not hasattr(mod, check_attr):
            return {"status": "error",
                    "detail": f"{module_name}.{check_attr} missing"}
        return {"status": "ok", "detail": f"{module_name} loaded"}

    def _check_orchestrator_lazy(self, lazy_fn_name: str,
                                  underlying_module: str) -> Dict[str, Any]:
        """Check if a data_orchestrator lazy-loader returns a valid module."""
        try:
            if "data_orchestrator" not in sys.modules:
                return {"status": "error",
                        "detail": "data_orchestrator not loaded"}
            do = sys.modules["data_orchestrator"]
            lazy_fn = getattr(do, lazy_fn_name, None)
            if lazy_fn is None:
                return {"status": "error",
                        "detail": f"data_orchestrator.{lazy_fn_name} not found"}
            result = lazy_fn()
            if result is None:
                return {"status": "error",
                        "detail": f"{underlying_module} failed via orchestrator"}
            return {"status": "ok",
                    "detail": f"{underlying_module} available via orchestrator"}
        except Exception as e:
            return {"status": "error", "detail": str(e)}

    def _probe_tier2_module(self, module_name: str,
                             check_attr: Optional[str] = None) -> Dict[str, Any]:
        """Probe a Tier 2/3 infrastructure module by importability."""
        try:
            if module_name in sys.modules:
                mod = sys.modules[module_name]
                if check_attr and not hasattr(mod, check_attr):
                    return {"status": "error",
                            "detail": f"{module_name} loaded but {check_attr} missing"}
                return {"status": "ok", "detail": f"{module_name} loaded and healthy"}
            # Try importing
            mod = importlib.import_module(module_name)
            if check_attr and not hasattr(mod, check_attr):
                return {"status": "error",
                        "detail": f"{module_name} importable but {check_attr} missing"}
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
                available = sum(1 for p in providers.values()
                                if isinstance(p, dict) and p.get("available", False))
                results["llm_providers"] = {
                    "status": "ok" if available > 0 else "error",
                    "detail": f"{available}/{len(providers)} providers available",
                    "providers": {k: v.get("available", False) for k, v in providers.items()
                                  if isinstance(v, dict)},
                }
            else:
                results["llm_providers"] = {
                    "status": "partial",
                    "detail": "llm_router not loaded yet",
                }
        except Exception as e:
            results["llm_providers"] = {"status": "error", "detail": str(e)}

        # 2. Async job queue depth
        try:
            if "app" in sys.modules:
                app_mod = sys.modules["app"]
                jobs = getattr(app_mod, "_generation_jobs", {})
                total = len(jobs)
                by_status = {}
                for jdata in jobs.values():
                    s = jdata.get("status", "unknown") if isinstance(jdata, dict) else "unknown"
                    by_status[s] = by_status.get(s, 0) + 1
                results["async_job_queue"] = {
                    "status": "ok" if total < 100 else "error",
                    "detail": f"{total} jobs ({by_status})",
                    "total": total,
                    "by_status": by_status,
                }
            else:
                results["async_job_queue"] = {
                    "status": "ok", "detail": "app not loaded (0 jobs)", "total": 0,
                }
        except Exception as e:
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
                "status": "ok" if keys_configured >= 2 else ("partial" if keys_configured >= 1 else "error"),
                "detail": f"{keys_configured}/4 API keys configured",
                "providers": provider_status,
            }
        except Exception as e:
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
            results["kb_freshness"] = {"status": "error", "detail": str(e)}

        # 5. Eval score trend
        try:
            if "eval_framework" in sys.modules:
                ef = sys.modules["eval_framework"]
                suite = ef.EvalSuite()
                scores = suite.run_full_eval()
                overall = scores.get("overall_score", 0)
                results["eval_score"] = {
                    "status": "ok" if overall >= 85 else ("partial" if overall >= 70 else "error"),
                    "detail": f"Overall eval score: {overall}%",
                    "score": overall,
                    "categories": {k: v.get("score_pct", 0) for k, v in scores.get("categories", {}).items()},
                }
            else:
                results["eval_score"] = {
                    "status": "partial",
                    "detail": "eval_framework not loaded",
                }
        except Exception as e:
            results["eval_score"] = {"status": "error", "detail": str(e)}

        # 6. Regression baseline age
        try:
            baseline_path = Path(__file__).resolve().parent / "data" / "persistent" / "regression_baseline.json"
            if baseline_path.exists():
                age_days = (time.time() - baseline_path.stat().st_mtime) / 86400
                results["regression_baseline"] = {
                    "status": "ok" if age_days < 30 else ("partial" if age_days < 60 else "error"),
                    "detail": f"Baseline age: {age_days:.1f} days",
                    "age_days": round(age_days, 1),
                }
            else:
                results["regression_baseline"] = {
                    "status": "partial",
                    "detail": "No baseline saved yet (first run will create it)",
                }
        except Exception as e:
            results["regression_baseline"] = {"status": "error", "detail": str(e)}

        # 7. v3.5 Conversational routing health
        try:
            if "nova" not in sys.modules:
                try:
                    import nova as _nova_import  # noqa: F811
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
                            "v3.5 inverted routing active" if all_present
                            else f"missing: conv={has_conv}, patterns={has_patterns}, tool_check={has_tool_check}"
                        ),
                        "version": "3.5" if all_present else "3.4",
                    }
                else:
                    results["v35_routing"] = {"status": "partial", "detail": "Nova class not found"}
            else:
                results["v35_routing"] = {"status": "partial", "detail": "nova module not loaded"}
        except Exception as e:
            results["v35_routing"] = {"status": "error", "detail": str(e)}

        return results

    # ── Self-healing ──────────────────────────────────────────────────────

    def _self_heal(self, product: str, layer: str,
                    probe_result: Dict[str, Any]) -> bool:
        """Attempt to fix a broken connection. Returns True if action taken."""
        healed = False

        # Strategy 1: Re-import failed modules (only for direct-import products)
        if (layer in ("api_enrichment", "research", "data_synthesizer",
                       "budget_engine", "standardizer",
                       "trend_engine", "collar_intelligence")
                and product in ("excel_ppt", "ppt_generator")):
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
                    self._record_heal(product, layer,
                                      "import_data_orchestrator", True)
                except Exception as e:
                    self._record_heal(product, layer,
                                      "import_data_orchestrator", False)
                    logger.warning("Self-heal import data_orchestrator failed: %s", e)

        # Strategy 2b: Reset orchestrator lazy-load sentinel
        if layer in _ORCH_LAYER_MAP and product in ("nova_chat", "slack_bot"):
            try:
                if "data_orchestrator" in sys.modules:
                    do = sys.modules["data_orchestrator"]
                    _, global_name = _ORCH_LAYER_MAP[layer]
                    with do._load_lock:
                        current = getattr(do, global_name, None)
                        if current is do._IMPORT_FAILED:
                            setattr(do, global_name, None)
                        healed = True
                        self._record_heal(product, layer,
                                          "reset_orchestrator_sentinel", True)
            except Exception as e:
                self._record_heal(product, layer,
                                  "reset_orchestrator_sentinel", False)
                logger.warning("Self-heal reset sentinel for %s failed: %s",
                               layer, e)

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
                        self._record_heal(product, layer,
                                          "reset_nova_orchestrator", True)
            except Exception as e:
                self._record_heal(product, layer,
                                  "reset_nova_orchestrator", False)
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
                            if now >= v.get("expires", 0):
                                expired.append(k)
                        for k in expired:
                            do._api_result_cache.pop(k, None)
                    if expired:
                        self._record_heal(
                            product, layer,
                            f"cleared_{len(expired)}_stale_cache_entries", True)
            except Exception as e:
                logger.warning("Self-heal cache clear failed: %s", e)

        return healed

    def _record_heal(self, product: str, layer: str,
                      action: str, success: bool) -> None:
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
        logger.log(level, "DataMatrixMonitor: heal %s/%s -- %s (success=%s)",
                   product, layer, action, success)


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
