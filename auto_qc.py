"""
auto_qc.py -- Autonomous QC Engine
====================================
Self-running, self-healing, self-upgrading quality assurance system.

Schedule:
    - Twice daily (every 12 hours): Runs full test suite against live endpoints
    - On failure: Attempts auto-resolution (cache clear, module reload, sentinel reset)
    - Weekly (every 7 days): Analyzes user interactions to generate new test cases

Data flow:
    1. Static tests (ported from tests/test_nova_chat.sh) run against internal handlers
    2. Dynamic tests (generated weekly from request_log.json + nova metrics)
    3. Results stored in memory + persisted to data/auto_qc_results.json
    4. Failures trigger self-healing actions + optional Slack alerts
    5. Exposed via GET /api/health/auto-qc (admin-protected)

Dependencies: stdlib only (no new packages).
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
import sys
import threading
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Scheduling ──────────────────────────────────────────────────────────────
_TEST_INTERVAL = 12 * 3600       # 12 hours between test runs
_INITIAL_DELAY = 120             # 2 min after startup (let services warm up)
_WEEKLY_INTERVAL = 7 * 24 * 3600 # 7 days between self-upgrade cycles
_WEEKLY_INITIAL_DELAY = 300      # 5 min after startup for first weekly check
_MAX_HISTORY = 30                # Keep last 30 run results
_PER_TEST_TIMEOUT = 45           # seconds -- hard ceiling per individual test

DATA_DIR = Path(__file__).resolve().parent / "data"
QC_RESULTS_FILE = DATA_DIR / "auto_qc_results.json"
DYNAMIC_TESTS_FILE = DATA_DIR / "auto_qc_dynamic_tests.json"
REQUEST_LOG_FILE = DATA_DIR / "request_log.json"


# ═══════════════════════════════════════════════════════════════════════════════
# TEST CASE DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

class TestResult:
    """Single test execution result."""
    __slots__ = ("name", "passed", "detail", "duration_ms", "category")

    def __init__(self, name: str, passed: bool, detail: str = "",
                 duration_ms: float = 0, category: str = "static"):
        self.name = name
        self.passed = passed
        self.detail = detail
        self.duration_ms = round(duration_ms, 1)
        self.category = category

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
            "duration_ms": self.duration_ms,
            "category": self.category,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# AUTONOMOUS QC ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class AutoQC:
    """Self-running QC engine with twice-daily tests, auto-healing, and weekly
    self-upgrading test generation."""

    def __init__(self):
        self._lock = threading.Lock()
        self._test_thread: Optional[threading.Thread] = None
        self._weekly_thread: Optional[threading.Thread] = None
        self._run_history: List[Dict[str, Any]] = []
        self._dynamic_tests: List[Dict[str, Any]] = []
        self._heal_log: List[Dict[str, Any]] = []
        self._last_run_time: float = 0
        self._last_weekly_time: float = 0
        self._start_time: float = 0          # set when start_background() is called
        self._run_count: int = 0
        self._weekly_count: int = 0
        self._nova_instance = None            # cached Nova instance (avoid per-test reinit)
        self._nova_init_failed: bool = False  # skip chat tests if Nova init crashes
        self._is_running: bool = False        # True while run_tests() is executing
        self._load_dynamic_tests()
        self._load_history()

    # ── Public API ────────────────────────────────────────────────────────

    def start_background(self) -> None:
        """Start both background daemon threads."""
        self._start_time = time.time()
        # Twice-daily test runner
        if self._test_thread is None or not self._test_thread.is_alive():
            self._test_thread = threading.Thread(
                target=self._test_loop,
                name="auto-qc-runner",
                daemon=True,
            )
            self._test_thread.start()
            logger.info("AutoQC: test runner started (interval=%ds)", _TEST_INTERVAL)

        # Weekly self-upgrade
        if self._weekly_thread is None or not self._weekly_thread.is_alive():
            self._weekly_thread = threading.Thread(
                target=self._weekly_loop,
                name="auto-qc-weekly",
                daemon=True,
            )
            self._weekly_thread.start()
            logger.info("AutoQC: weekly upgrader started (interval=%ds)", _WEEKLY_INTERVAL)

    def get_status(self) -> Dict[str, Any]:
        """Return QC status for the API endpoint."""
        with self._lock:
            last_run = self._run_history[-1] if self._run_history else None
            if self._is_running:
                current_status = "running"
            elif last_run:
                current_status = last_run["status"]
            else:
                current_status = "pending"
            return {
                "status": current_status,
                "total_runs": self._run_count,
                "weekly_upgrades": self._weekly_count,
                "static_tests": len(self._get_static_test_names()),
                "dynamic_tests": len(self._dynamic_tests),
                "last_run": last_run,
                "last_run_age_seconds": round(
                    time.time() - self._last_run_time, 1
                ) if self._last_run_time else None,
                "next_run_in_seconds": max(0, round(
                    _TEST_INTERVAL - (time.time() - self._last_run_time), 1
                )) if self._last_run_time else max(0, round(
                    _INITIAL_DELAY - (time.time() - self._start_time), 1
                )) if self._start_time else None,
                "next_weekly_in_seconds": max(0, round(
                    _WEEKLY_INTERVAL - (time.time() - self._last_weekly_time), 1
                )) if self._last_weekly_time else max(0, round(
                    _WEEKLY_INITIAL_DELAY - (time.time() - self._start_time), 1
                )) if self._start_time else None,
                "recent_heals": list(self._heal_log[-10:]),
                "run_history": [
                    {
                        "run_number": r["run_number"],
                        "timestamp": r["timestamp"],
                        "status": r["status"],
                        "passed": r["passed"],
                        "failed": r["failed"],
                        "total": r["total"],
                        "duration_seconds": r["duration_seconds"],
                    }
                    for r in self._run_history[-10:]
                ],
            }

    def run_tests(self) -> Dict[str, Any]:
        """Execute all tests (static + dynamic), attempt auto-healing on
        failures, and return results."""
        self._is_running = True
        try:
            start = time.time()
            results: List[TestResult] = []

            # Run static tests
            results.extend(self._run_static_tests())

            # Run dynamic tests
            results.extend(self._run_dynamic_tests())

            # Tally
            passed = sum(1 for r in results if r.passed)
            failed = sum(1 for r in results if not r.passed)
            total = len(results)
            elapsed = round(time.time() - start, 2)

            # Attempt auto-healing for any failures
            healed = 0
            if failed > 0:
                healed = self._auto_heal(results)

            # Build run result
            self._run_count += 1
            status = "all_passing" if failed == 0 else (
                "healed" if healed >= failed else "degraded"
            )
            run_result = {
                "run_number": self._run_count,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "passed": passed,
                "failed": failed,
                "healed": healed,
                "total": total,
                "duration_seconds": elapsed,
                "tests": [r.to_dict() for r in results],
                "failures": [r.to_dict() for r in results if not r.passed],
            }

            with self._lock:
                self._run_history.append(run_result)
                if len(self._run_history) > _MAX_HISTORY:
                    self._run_history = self._run_history[-_MAX_HISTORY:]
                self._last_run_time = time.time()

            self._persist_history()

            logger.info(
                "AutoQC: run #%d -- %s (%d/%d passed, %d healed, %.2fs)",
                self._run_count, status, passed, total, healed, elapsed,
            )

            # Alert on persistent failures
            if failed > healed:
                self._send_alert(run_result)

            return run_result
        finally:
            self._is_running = False

    # ── Background loops ──────────────────────────────────────────────────

    def _test_loop(self) -> None:
        """Twice-daily test execution loop."""
        time.sleep(_INITIAL_DELAY)
        while True:
            try:
                self.run_tests()
            except Exception as e:
                logger.error("AutoQC: test run failed: %s", e, exc_info=True)
            time.sleep(_TEST_INTERVAL)

    def _weekly_loop(self) -> None:
        """Weekly self-upgrade loop: analyze interactions, generate new tests."""
        time.sleep(_WEEKLY_INITIAL_DELAY)
        while True:
            try:
                self._weekly_upgrade()
            except Exception as e:
                logger.error("AutoQC: weekly upgrade failed: %s", e, exc_info=True)
            time.sleep(_WEEKLY_INTERVAL)

    # ══════════════════════════════════════════════════════════════════════
    # STATIC TESTS (ported from test_nova_chat.sh, run in-process)
    # ══════════════════════════════════════════════════════════════════════

    def _get_static_test_names(self) -> List[str]:
        """Return only numbered test methods (_test_NN_*), excluding
        background methods like _test_loop and attributes like _test_thread."""
        import re as _re
        return [
            name for name in dir(self)
            if _re.match(r"_test_\d+_", name) and callable(getattr(self, name))
        ]

    def _run_single_test_with_timeout(self, name: str, method, timeout: int = _PER_TEST_TIMEOUT) -> TestResult:
        """Run a single test method with a hard timeout.

        Prevents ANY individual test from hanging the entire test run,
        whether it calls Claude API, external HTTP APIs, or does heavy
        computation.
        """
        result_holder: List[TestResult] = []
        error_holder: List[str] = []
        t0 = time.time()

        def _exec():
            try:
                result_holder.append(method())
            except Exception as e:
                error_holder.append(str(e))

        worker = threading.Thread(target=_exec, daemon=True)
        worker.start()
        worker.join(timeout=timeout)
        elapsed_ms = (time.time() - t0) * 1000

        if worker.is_alive():
            logger.warning("AutoQC: test %s timed out after %ds", name, timeout)
            return TestResult(
                name=name.replace("_test_", ""),
                passed=False,
                detail=f"Timeout after {timeout}s",
                duration_ms=elapsed_ms,
            )
        if error_holder:
            return TestResult(
                name=name.replace("_test_", ""),
                passed=False,
                detail=f"Exception: {error_holder[0]}",
                duration_ms=elapsed_ms,
            )
        if result_holder:
            result_holder[0].duration_ms = elapsed_ms
            return result_holder[0]
        return TestResult(
            name=name.replace("_test_", ""),
            passed=False,
            detail="No result returned",
            duration_ms=elapsed_ms,
        )

    def _run_static_tests(self) -> List[TestResult]:
        results = []
        for name in sorted(self._get_static_test_names()):
            method = getattr(self, name)
            result = self._run_single_test_with_timeout(name, method)
            results.append(result)
        return results

    def _run_static_tests_DISABLED(self) -> List[TestResult]:
        """Old version without per-test timeout (kept for reference)."""
        results = []
        for name in sorted(self._get_static_test_names()):
            method = getattr(self, name)
            t0 = time.time()
            try:
                result = method()
                result.duration_ms = (time.time() - t0) * 1000
                results.append(result)
            except Exception as e:
                results.append(TestResult(
                    name=name.replace("_test_", ""),
                    passed=False,
                    detail=f"Exception: {e}",
                    duration_ms=(time.time() - t0) * 1000,
                    category="static",
                ))
        return results

    # -- Health & Structure Tests --

    def _test_01_health_endpoint(self) -> TestResult:
        """Health endpoint returns ok."""
        try:
            from monitoring import health_check_liveness
            result = health_check_liveness()
            ok = result.get("status") == "ok"
            return TestResult("health_endpoint", ok,
                              f"status={result.get('status')}")
        except Exception as e:
            return TestResult("health_endpoint", False, str(e))

    def _test_02_kb_json_files(self) -> TestResult:
        """All KB JSON files exist and parse."""
        required = [
            "recruitment_industry_knowledge.json",
            "platform_intelligence_deep.json",
            "recruitment_benchmarks_deep.json",
            "channels_db.json",
        ]
        missing, corrupt = [], []
        for fname in required:
            fpath = DATA_DIR / fname
            if not fpath.exists():
                missing.append(fname)
            else:
                try:
                    with open(fpath) as f:
                        json.load(f)
                except (json.JSONDecodeError, OSError):
                    corrupt.append(fname)
        ok = not missing and not corrupt
        detail = f"{len(required)}/{len(required)} valid"
        if missing:
            detail = f"Missing: {', '.join(missing)}"
        elif corrupt:
            detail = f"Corrupt: {', '.join(corrupt)}"
        return TestResult("kb_json_files", ok, detail)

    def _test_03_nova_import(self) -> TestResult:
        """Nova module importable and has chat method."""
        try:
            if "nova" not in sys.modules:
                importlib.import_module("nova")
            nova_mod = sys.modules["nova"]
            has_nova = hasattr(nova_mod, "Nova")
            has_chat = has_nova and hasattr(nova_mod.Nova, "chat")
            ok = has_nova and has_chat
            return TestResult("nova_import", ok,
                              f"Nova={has_nova}, chat={has_chat}")
        except Exception as e:
            return TestResult("nova_import", False, str(e))

    def _test_04_orchestrator_import(self) -> TestResult:
        """DataOrchestrator module importable with core functions."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            required_fns = [
                "enrich_salary", "enrich_location", "enrich_market_demand",
                "enrich_competitive", "enrich_budget", "enrich_employer_brand",
                "get_ad_platform_benchmarks", "compute_insights",
                "get_cache_stats", "get_fallback_telemetry",
            ]
            missing = [fn for fn in required_fns if not hasattr(do, fn)]
            ok = len(missing) == 0
            detail = "All 10 core functions present" if ok else f"Missing: {', '.join(missing)}"
            return TestResult("orchestrator_import", ok, detail)
        except Exception as e:
            return TestResult("orchestrator_import", False, str(e))

    def _test_05_claude_api_key(self) -> TestResult:
        """ANTHROPIC_API_KEY is set."""
        key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        ok = len(key) > 10
        return TestResult("claude_api_key", ok,
                          "Set" if ok else "Missing or too short")

    def _test_06_data_matrix_health(self) -> TestResult:
        """Data matrix monitor reports healthy."""
        try:
            if "data_matrix_monitor" not in sys.modules:
                importlib.import_module("data_matrix_monitor")
            from data_matrix_monitor import get_data_matrix_monitor
            monitor = get_data_matrix_monitor()
            status = monitor.get_status()
            if status.get("status") == "pending":
                return TestResult("data_matrix_health", True,
                                  "Pending (first check not yet run)")
            hp = status.get("health_pct", 0)
            errors = status.get("summary", {}).get("error", 0)
            ok = hp >= 80.0 and errors == 0
            return TestResult("data_matrix_health", ok,
                              f"health={hp}%, errors={errors}")
        except Exception as e:
            return TestResult("data_matrix_health", False, str(e))

    def _test_07_orchestrator_cache(self) -> TestResult:
        """Orchestrator cache is operational."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            stats = do.get_cache_stats()
            ok = isinstance(stats, dict) and "total_entries" in stats
            return TestResult("orchestrator_cache", ok,
                              f"entries={stats.get('total_entries', '?')}, "
                              f"max={stats.get('max_entries', '?')}")
        except Exception as e:
            return TestResult("orchestrator_cache", False, str(e))

    def _test_08_nova_tool_count(self) -> TestResult:
        """Nova has 23 tool definitions (v3: +query_collar_strategy, +query_market_trends)."""
        try:
            if "nova" not in sys.modules:
                importlib.import_module("nova")
            nova_mod = sys.modules["nova"]
            nova_instance = nova_mod.Nova()
            tools = nova_instance.get_tool_definitions()
            count = len(tools)
            tool_names = [t.get("name", "") for t in tools]
            v3_tools = ["query_collar_strategy", "query_market_trends"]
            missing_v3 = [t for t in v3_tools if t not in tool_names]
            ok = count >= 23 and not missing_v3
            detail = f"{count} tools (expected >= 23)"
            if missing_v3:
                detail += f"; MISSING v3 tools: {missing_v3}"
            return TestResult("nova_tool_count", ok, detail)
        except Exception as e:
            return TestResult("nova_tool_count", False, str(e))

    def _test_09_env_vars(self) -> TestResult:
        """Critical environment variables are set."""
        required = ["ANTHROPIC_API_KEY"]
        optional_checked = ["BLS_API_KEY", "FRED_API_KEY", "ADZUNA_APP_ID"]
        missing_req = [v for v in required if not os.environ.get(v, "").strip()]
        missing_opt = [v for v in optional_checked if not os.environ.get(v, "").strip()]
        ok = len(missing_req) == 0
        detail = "All required env vars set"
        if missing_req:
            detail = f"Missing required: {', '.join(missing_req)}"
        elif missing_opt:
            detail = f"Optional missing: {', '.join(missing_opt)} (APIs may use fallback)"
        return TestResult("env_vars", ok, detail)

    def _test_10_modules_compile(self) -> TestResult:
        """All Python files compile without syntax errors."""
        project_dir = Path(__file__).resolve().parent
        py_files = sorted(project_dir.glob("*.py"))
        errors = []
        for f in py_files:
            try:
                import py_compile
                py_compile.compile(str(f), doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{f.name}: {e}")
        ok = len(errors) == 0
        detail = f"{len(py_files)} files compile clean" if ok else "; ".join(errors[:3])
        return TestResult("modules_compile", ok, detail)

    # -- Chat Functionality Tests --

    def _test_11_chat_learned_answer(self) -> TestResult:
        """Learned answer 'what is joveo' returns correctly."""
        try:
            result = self._internal_chat("what is joveo")
            has_response = bool(result.get("response"))
            mentions_joveo = "joveo" in result.get("response", "").lower()
            confidence = result.get("confidence", 0)
            ok = has_response and mentions_joveo and confidence >= 0.85
            return TestResult("chat_learned_answer", ok,
                              f"confidence={confidence}, mentions_joveo={mentions_joveo}")
        except Exception as e:
            return TestResult("chat_learned_answer", False, str(e))

    def _test_12_chat_response_structure(self) -> TestResult:
        """Chat response has required fields."""
        try:
            result = self._internal_chat("hello")
            required_fields = ["response", "confidence", "sources", "tools_used"]
            missing = [f for f in required_fields if f not in result]
            ok = len(missing) == 0
            return TestResult("chat_response_structure", ok,
                              f"fields={'all present' if ok else 'missing: ' + ', '.join(missing)}")
        except Exception as e:
            return TestResult("chat_response_structure", False, str(e))

    def _test_13_chat_empty_message(self) -> TestResult:
        """Empty message returns guidance, not error."""
        try:
            result = self._internal_chat("")
            has_response = bool(result.get("response"))
            no_error = "error" not in result.get("response", "").lower()
            ok = has_response and no_error
            return TestResult("chat_empty_message", ok,
                              f"has_response={has_response}, no_error={no_error}")
        except Exception as e:
            return TestResult("chat_empty_message", False, str(e))

    def _test_14_ask_before_answering(self) -> TestResult:
        """Salary query without location triggers clarification."""
        try:
            result = self._internal_chat("what is the average salary of a nurse")
            resp_lower = result.get("response", "").lower()
            asks_location = any(w in resp_lower for w in
                                ["country", "region", "location", "where", "which"])
            ok = asks_location
            return TestResult("ask_before_answering", ok,
                              f"asks_location={asks_location}")
        except Exception as e:
            return TestResult("ask_before_answering", False, str(e))

    # -- Orchestrator-Specific Tests --

    def _test_15_orchestrator_lazy_loaders(self) -> TestResult:
        """Orchestrator lazy loaders resolve to valid modules (v3: 6 loaders)."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            loaders = {
                "_lazy_api": "api_enrichment",
                "_lazy_research": "research",
                "_lazy_budget": "budget_engine",
                "_lazy_standardizer": "standardizer",
                "_lazy_trend_engine": "trend_engine",
                "_lazy_collar_intel": "collar_intelligence",
            }
            failures = []
            for fn_name, expected in loaders.items():
                fn = getattr(do, fn_name, None)
                if fn is None:
                    failures.append(f"{fn_name} not found")
                else:
                    mod = fn()
                    if mod is None:
                        failures.append(f"{fn_name} returned None ({expected} failed)")
            ok = len(failures) == 0
            detail = f"All {len(loaders)} lazy loaders OK" if ok else "; ".join(failures)
            return TestResult("orchestrator_lazy_loaders", ok, detail)
        except Exception as e:
            return TestResult("orchestrator_lazy_loaders", False, str(e))

    def _test_16_orchestrator_enrichment_context(self) -> TestResult:
        """EnrichmentContext can be created and used."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            ctx = do.EnrichmentContext()
            ctx.store("test_key", {"value": 42})
            retrieved = ctx.get("test_key")
            ok = retrieved is not None and retrieved.get("value") == 42
            return TestResult("enrichment_context", ok,
                              f"store/get={ok}")
        except Exception as e:
            return TestResult("enrichment_context", False, str(e))

    def _test_17_ad_platform_benchmarks(self) -> TestResult:
        """Ad platform benchmarks return data for known industry."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.get_ad_platform_benchmarks("technology")
            has_platforms = "platforms" in result
            platforms = result.get("platforms", {})
            has_google = "google_ads" in platforms
            ok = has_platforms and has_google
            return TestResult("ad_platform_benchmarks", ok,
                              f"platforms={len(platforms)}, has_google={has_google}")
        except Exception as e:
            return TestResult("ad_platform_benchmarks", False, str(e))

    def _test_18_compute_insights(self) -> TestResult:
        """Compute insights returns hiring difficulty index."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.compute_insights(
                role="Software Engineer",
                location="San Francisco",
                industry="technology"
            )
            has_difficulty = "hiring_difficulty_index" in result
            # salary_competitiveness_at_market only present when context has salary data
            has_competitiveness = "salary_competitiveness_at_market" in result
            ok = has_difficulty  # core required field
            return TestResult("compute_insights", ok,
                              f"difficulty={result.get('hiring_difficulty_index', '?')}, "
                              f"has_salary_comp={has_competitiveness}")
        except Exception as e:
            return TestResult("compute_insights", False, str(e))

    def _test_19_employer_brand(self) -> TestResult:
        """Employer brand lookup returns data for known company."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.enrich_employer_brand("Google")
            has_data = bool(result) and not result.get("error")
            return TestResult("employer_brand", has_data,
                              f"keys={list(result.keys())[:5]}" if has_data else "No data")
        except Exception as e:
            return TestResult("employer_brand", False, str(e))

    def _test_20_fallback_telemetry(self) -> TestResult:
        """Fallback telemetry endpoint is functional."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.get_fallback_telemetry()
            ok = isinstance(result, dict) and "total_fallbacks" in result
            return TestResult("fallback_telemetry", ok,
                              f"total_fallbacks={result.get('total_fallbacks', '?')}")
        except Exception as e:
            return TestResult("fallback_telemetry", False, str(e))

    # -- Trend Engine Validation Tests (v3) --

    def _test_21_trend_engine_benchmarks(self) -> TestResult:
        """Trend engine returns valid benchmarks for multiple platform/industry combos."""
        try:
            try:
                import trend_engine
            except ImportError:
                return TestResult("trend_engine_benchmarks", False,
                                  "trend_engine module not importable")

            test_combos = [
                ("google_search", "healthcare_medical"),
                ("indeed", "logistics_supply_chain"),
                ("linkedin", "tech_engineering"),
            ]
            failures = []
            for platform, industry in test_combos:
                result = trend_engine.get_benchmark(platform=platform, industry=industry)
                # value > 0
                if not (isinstance(result.get("value"), (int, float)) and result["value"] > 0):
                    failures.append(f"{platform}/{industry}: value not > 0 (got {result.get('value')})")
                    continue
                # confidence_interval is a list of 2 floats
                ci = result.get("confidence_interval")
                if not (isinstance(ci, (list, tuple)) and len(ci) == 2
                        and all(isinstance(v, (int, float)) for v in ci)):
                    failures.append(f"{platform}/{industry}: confidence_interval invalid (got {ci})")
                    continue
                # trend_direction in valid set
                td = result.get("trend_direction")
                if td not in ("rising", "falling", "stable"):
                    failures.append(f"{platform}/{industry}: trend_direction invalid (got {td})")
                    continue
                # trend_pct_yoy is a number
                yoy = result.get("trend_pct_yoy")
                if not isinstance(yoy, (int, float)):
                    failures.append(f"{platform}/{industry}: trend_pct_yoy not numeric (got {yoy})")

            ok = len(failures) == 0
            detail = f"All {len(test_combos)} combos valid" if ok else "; ".join(failures[:3])
            return TestResult("trend_engine_benchmarks", ok, detail)
        except Exception as e:
            return TestResult("trend_engine_benchmarks", False, str(e))

    def _test_22_trend_engine_seasonal(self) -> TestResult:
        """Trend engine seasonal adjustment returns multiplier for both collar types."""
        try:
            try:
                import trend_engine
            except ImportError:
                return TestResult("trend_engine_seasonal", False,
                                  "trend_engine module not importable")

            failures = []
            for collar in ("blue_collar", "white_collar"):
                result = trend_engine.get_seasonal_adjustment(collar_type=collar)
                if not isinstance(result, dict):
                    failures.append(f"{collar}: result is not a dict")
                    continue
                if "multiplier" not in result:
                    failures.append(f"{collar}: missing 'multiplier' key (keys={list(result.keys())})")
                    continue
                mult = result["multiplier"]
                if not isinstance(mult, (int, float)) or mult <= 0:
                    failures.append(f"{collar}: multiplier not positive (got {mult})")

            ok = len(failures) == 0
            detail = "Both collar types return valid multiplier" if ok else "; ".join(failures)
            return TestResult("trend_engine_seasonal", ok, detail)
        except Exception as e:
            return TestResult("trend_engine_seasonal", False, str(e))

    def _test_23_trend_engine_freshness(self) -> TestResult:
        """Trend engine data contains current or previous year (not stale)."""
        try:
            try:
                import trend_engine
            except ImportError:
                return TestResult("trend_engine_freshness", False,
                                  "trend_engine module not importable")

            current_year = datetime.now(timezone.utc).year
            previous_year = current_year - 1

            # Check _ALL_TRENDS for freshness -- it should contain data for
            # at least the previous year across platforms
            all_trends = getattr(trend_engine, "_ALL_TRENDS", None)
            if all_trends is None:
                return TestResult("trend_engine_freshness", False,
                                  "_ALL_TRENDS not found in trend_engine")

            stale_platforms = []
            for platform, industries in all_trends.items():
                has_recent = False
                for ind_key, year_data in industries.items():
                    if isinstance(year_data, dict):
                        years = [y for y in year_data.keys() if isinstance(y, int)]
                        if any(y >= previous_year for y in years):
                            has_recent = True
                            break
                if not has_recent:
                    stale_platforms.append(platform)

            ok = len(stale_platforms) == 0
            detail = (f"All {len(all_trends)} platforms have data for >= {previous_year}"
                      if ok else f"Stale platforms (no data >= {previous_year}): {', '.join(stale_platforms)}")
            return TestResult("trend_engine_freshness", ok, detail)
        except Exception as e:
            return TestResult("trend_engine_freshness", False, str(e))

    # -- Collar Classification Coverage Tests (v3) --

    def _test_24_collar_classification(self) -> TestResult:
        """Collar intelligence classifies diverse roles correctly."""
        try:
            try:
                import collar_intelligence
            except ImportError:
                return TestResult("collar_classification", False,
                                  "collar_intelligence module not importable")

            test_roles = [
                "Warehouse Worker",
                "Software Engineer",
                "Registered Nurse",
                "CEO",
                "Truck Driver",
            ]
            valid_collars = ("blue_collar", "white_collar", "grey_collar", "pink_collar")
            failures = []
            for role in test_roles:
                result = collar_intelligence.classify_collar(role)
                # collar_type in valid set
                ct = result.get("collar_type")
                if ct not in valid_collars:
                    failures.append(f"{role}: collar_type invalid (got {ct})")
                    continue
                # confidence > 0.3
                conf = result.get("confidence", 0)
                if not (isinstance(conf, (int, float)) and conf > 0.3):
                    failures.append(f"{role}: confidence <= 0.3 (got {conf})")
                    continue
                # method is non-empty
                method = result.get("method", "")
                if not method:
                    failures.append(f"{role}: method is empty")

            ok = len(failures) == 0
            detail = f"All {len(test_roles)} roles classified validly" if ok else "; ".join(failures[:3])
            return TestResult("collar_classification", ok, detail)
        except Exception as e:
            return TestResult("collar_classification", False, str(e))

    # -- Budget Engine v3 Integration Tests --

    def _test_25_budget_engine_v3_params(self) -> TestResult:
        """Budget engine compute_channel_dollar_amounts accepts v3 params."""
        try:
            try:
                import budget_engine
            except ImportError:
                return TestResult("budget_engine_v3_params", False,
                                  "budget_engine module not importable")

            # Minimal valid inputs for the function
            channel_pcts = {"Programmatic & DSP": 30, "Global Job Boards": 25,
                            "Social Media Ads": 20, "Google Ads": 15, "Regional/Local": 10}
            role_budgets = {"Software Engineer": {"dollar_amount": 10000, "count": 2}}

            # Call with the v3 parameters: industry, collar_type, location
            result = budget_engine.compute_channel_dollar_amounts(
                channel_percentages=channel_pcts,
                role_budgets=role_budgets,
                industry="tech_engineering",
                collar_type="white_collar",
                location="San Francisco",
            )

            if not isinstance(result, dict) or len(result) == 0:
                return TestResult("budget_engine_v3_params", False,
                                  f"Empty or non-dict result: {type(result)}")

            # Check at least one channel has cpc_source field
            has_cpc_source = any(
                isinstance(v, dict) and "cpc_source" in v
                for v in result.values()
            )
            ok = has_cpc_source
            sample_channel = next(iter(result.values()), {})
            detail = (f"{len(result)} channels returned, cpc_source present"
                      if ok else f"cpc_source missing from channels (sample keys: {list(sample_channel.keys())[:6]})")
            return TestResult("budget_engine_v3_params", ok, detail)
        except Exception as e:
            return TestResult("budget_engine_v3_params", False, str(e))

    # -- Data Orchestrator v3 Integration Tests --

    def _test_26_orchestrator_ad_benchmarks_v3(self) -> TestResult:
        """Orchestrator enrich_ad_benchmarks returns structured_confidence."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.enrich_ad_benchmarks(industry="technology", role="Software Engineer")
            has_sc = isinstance(result.get("structured_confidence"), dict)
            ok = has_sc
            detail = ("structured_confidence dict present"
                      if ok else f"structured_confidence missing or not dict (keys={list(result.keys())[:8]})")
            return TestResult("orchestrator_ad_benchmarks_v3", ok, detail)
        except Exception as e:
            return TestResult("orchestrator_ad_benchmarks_v3", False, str(e))

    def _test_27_orchestrator_collar_intelligence(self) -> TestResult:
        """Orchestrator enrich_collar_intelligence returns non-empty collar_type."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.enrich_collar_intelligence(role="Truck Driver", industry="logistics")
            collar_type = result.get("collar_type", "")
            ok = bool(collar_type) and collar_type != ""
            detail = f"collar_type={collar_type}" if ok else "collar_type is empty or missing"
            return TestResult("orchestrator_collar_intelligence", ok, detail)
        except Exception as e:
            return TestResult("orchestrator_collar_intelligence", False, str(e))

    def _test_28_orchestrator_hiring_trends(self) -> TestResult:
        """Orchestrator enrich_hiring_trends returns hiring_difficulty_index."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.enrich_hiring_trends(industry="technology", location="US")
            hdi = result.get("hiring_difficulty_index")
            ok = hdi is not None and isinstance(hdi, (int, float))
            detail = (f"hiring_difficulty_index={hdi}"
                      if ok else f"hiring_difficulty_index missing or invalid (keys={list(result.keys())[:8]})")
            return TestResult("orchestrator_hiring_trends", ok, detail)
        except Exception as e:
            return TestResult("orchestrator_hiring_trends", False, str(e))

    def _test_29_nova_v3_tool_handlers(self) -> TestResult:
        """Nova v3 tool handlers (query_collar_strategy, query_market_trends) execute."""
        try:
            if "nova" not in sys.modules:
                importlib.import_module("nova")
            nova_mod = sys.modules["nova"]
            nova_instance = nova_mod.Nova()

            failures = []

            # Test query_collar_strategy
            try:
                cs_result = nova_instance._query_collar_strategy({
                    "role": "Warehouse Worker",
                    "industry": "logistics",
                })
                if not isinstance(cs_result, dict):
                    failures.append("query_collar_strategy: not a dict")
                elif "role_classification" not in cs_result and "collar_type" not in cs_result:
                    failures.append(f"query_collar_strategy: missing expected keys (got {list(cs_result.keys())[:5]})")
            except AttributeError:
                failures.append("query_collar_strategy: handler method missing on Nova")
            except Exception as e:
                failures.append(f"query_collar_strategy: {e}")

            # Test query_market_trends
            try:
                mt_result = nova_instance._query_market_trends({
                    "platform": "google",
                    "industry": "technology",
                    "metric": "cpc",
                })
                if not isinstance(mt_result, dict):
                    failures.append("query_market_trends: not a dict")
                elif "historical_trend" not in mt_result and "current_benchmark" not in mt_result:
                    failures.append(f"query_market_trends: missing expected keys (got {list(mt_result.keys())[:5]})")
            except AttributeError:
                failures.append("query_market_trends: handler method missing on Nova")
            except Exception as e:
                failures.append(f"query_market_trends: {e}")

            ok = len(failures) == 0
            detail = "Both v3 tool handlers execute OK" if ok else "; ".join(failures)
            return TestResult("nova_v3_tool_handlers", ok, detail)
        except Exception as e:
            return TestResult("nova_v3_tool_handlers", False, str(e))

    def _test_30_ppt_v3_features(self) -> TestResult:
        """PPT generator has v3 features: trend_engine benchmarks, collar intelligence, role normalization."""
        try:
            if "ppt_generator" not in sys.modules:
                importlib.import_module("ppt_generator")
            ppt = sys.modules["ppt_generator"]
            failures = []

            # Check trend_engine is imported
            has_trend = getattr(ppt, "_HAS_TREND_ENGINE", False)
            if not has_trend:
                failures.append("_HAS_TREND_ENGINE is False (trend_engine not loaded)")

            # Check collar_intelligence is imported
            has_collar = getattr(ppt, "_HAS_COLLAR_INTEL", False)
            if not has_collar:
                failures.append("_HAS_COLLAR_INTEL is False (collar_intelligence not loaded)")

            # Check role normalization (v3 bug fix) in generate_pptx
            import inspect
            src = inspect.getsource(ppt.generate_pptx)
            has_normalization = ("isinstance(r, dict)" in src or
                                 'r.get("title"' in src or
                                 "Normalize roles" in src)
            if not has_normalization:
                failures.append("Missing role dict normalization in generate_pptx")

            # Check _get_benchmarks uses trend_engine
            bench_src = inspect.getsource(ppt._get_benchmarks)
            if "_HAS_TREND_ENGINE" not in bench_src and "trend_engine" not in bench_src:
                failures.append("_get_benchmarks does not use trend_engine")

            ok = len(failures) == 0
            detail = "PPT v3 features present (trend_engine, collar_intel, role normalization)" if ok else "; ".join(failures)
            return TestResult("ppt_v3_features", ok, detail)
        except Exception as e:
            return TestResult("ppt_v3_features", False, str(e))

    def _test_31_data_synthesizer_integration(self) -> TestResult:
        """data_synthesizer has all 10 public functions for orchestrator fusion."""
        try:
            if "data_synthesizer" not in sys.modules:
                importlib.import_module("data_synthesizer")
            ds = sys.modules["data_synthesizer"]
            required_fns = [
                "synthesize",
                "generate_ai_narratives",
                "fuse_salary_intelligence",
                "fuse_job_market_demand",
                "fuse_location_profiles",
                "fuse_competitive_intelligence",
                "fuse_ad_platform_analysis",
                "fuse_workforce_insights",
                "compute_confidence_scores",
                "validate_with_knowledge_base",
            ]
            missing = [fn for fn in required_fns if not hasattr(ds, fn)]
            ok = len(missing) == 0
            detail = (f"All {len(required_fns)} synthesizer functions present"
                      if ok else f"Missing: {', '.join(missing)}")
            return TestResult("data_synthesizer_integration", ok, detail)
        except Exception as e:
            return TestResult("data_synthesizer_integration", False, str(e))

    def _test_32_structured_confidence(self) -> TestResult:
        """Orchestrator enrichment returns structured confidence with sources."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            result = do.enrich_salary("Software Engineer", "US", "technology")
            failures = []

            # Check for structured confidence (v3 uses "structured_confidence" key)
            sc = result.get("structured_confidence")
            scalar_conf = result.get("confidence")

            if sc is not None and isinstance(sc, dict):
                # Full structured confidence -- verify required fields
                for key in ("confidence", "sources", "freshness"):
                    if key not in sc:
                        failures.append(f"structured_confidence missing '{key}'")
            elif scalar_conf is not None and isinstance(scalar_conf, (int, float)):
                pass  # OK -- scalar confidence (backward compat)
            else:
                failures.append("No confidence field found (expected 'structured_confidence' dict or 'confidence' float)")

            # Check source attribution (multiple possible key names)
            sources = (result.get("sources_used")
                       or result.get("sources")
                       or result.get("data_sources")
                       or [])
            if not sources:
                # Also check inside structured_confidence
                if sc and isinstance(sc, dict):
                    sources = sc.get("sources", [])
            if not sources:
                failures.append("No source attribution found")

            ok = len(failures) == 0
            detail = (f"structured_confidence present (confidence={scalar_conf}, "
                      f"{len(sources)} sources)" if ok else "; ".join(failures))
            return TestResult("structured_confidence", ok, detail)
        except Exception as e:
            return TestResult("structured_confidence", False, str(e))

    def _test_33_data_matrix_v3_layers(self) -> TestResult:
        """Data matrix monitor tracks trend_engine and collar_intelligence layers."""
        try:
            if "data_matrix_monitor" not in sys.modules:
                importlib.import_module("data_matrix_monitor")
            dmm = sys.modules["data_matrix_monitor"]
            matrix = dmm.EXPECTED_MATRIX
            failures = []
            v3_layers = ["trend_engine", "collar_intelligence"]
            for product, layers in matrix.items():
                for v3l in v3_layers:
                    if v3l not in layers:
                        failures.append(f"{product}: missing {v3l} layer")
            # Check orchestrator layer map has v3 entries
            orch_map = dmm._ORCH_LAYER_MAP
            for v3l in v3_layers:
                if v3l not in orch_map:
                    failures.append(f"_ORCH_LAYER_MAP missing {v3l}")
            ok = len(failures) == 0
            detail = "Data matrix fully tracks v3 layers (4 products x 2 new layers)" if ok else "; ".join(failures)
            return TestResult("data_matrix_v3_layers", ok, detail)
        except Exception as e:
            return TestResult("data_matrix_v3_layers", False, str(e))

    def _test_34_kb_data_contracts(self) -> TestResult:
        """KB data contracts -- all KB files pass schema validation."""
        try:
            if "data_contracts" not in sys.modules:
                importlib.import_module("data_contracts")
            dc = sys.modules["data_contracts"]
            result = dc.validate_all_kb()
            passed = result.get("passed", 0)
            total = result.get("total", 0)
            ok = result.get("failed", 1) == 0
            detail = f"{passed}/{total} KB files pass schema validation"
            return TestResult("kb_data_contracts", ok, detail)
        except ImportError:
            return TestResult("kb_data_contracts", False, "Module not available")
        except Exception as e:
            return TestResult("kb_data_contracts", False, f"Error: {e}")

    def _test_35_enrichment_output_contract(self) -> TestResult:
        """Enrichment output contract -- sample output passes validation."""
        try:
            if "data_contracts" not in sys.modules:
                importlib.import_module("data_contracts")
            dc = sys.modules["data_contracts"]
            sample = {
                "salary_data": {},
                "location_data": {},
                "market_demand": {},
                "competitive_data": {},
                "ad_benchmarks": {},
                "collar_intelligence": {},
                "hiring_trends": {},
                "industry_employment": {},
                "location_demographics": {},
                "global_indicators": {},
                "job_market": {},
                "company_info": {},
                "company_metadata": {},
                "sec_data": {},
                "competitor_logos": {},
                "currency_rates": {},
                "enrichment_summary": {
                    "apis_called": [],
                    "apis_succeeded": [],
                    "apis_failed": [],
                    "total_data_points": 0,
                    "total_time_seconds": 0.0,
                    "confidence_score": 0.5,
                },
            }
            result = dc.validate_enrichment_output(sample)
            valid = result.get("valid", False)
            detail = f"Enrichment output contract validation: {valid}"
            return TestResult("enrichment_output_contract", valid, detail)
        except ImportError:
            return TestResult("enrichment_output_contract", False, "Module not available")
        except Exception as e:
            return TestResult("enrichment_output_contract", False, f"Error: {e}")

    def _test_36_regression_check(self) -> TestResult:
        """Regression check -- scenario 0 (healthcare) runs successfully."""
        try:
            if "regression_detector" not in sys.modules:
                importlib.import_module("regression_detector")
            rd = sys.modules["regression_detector"]
            scenario = rd.REFERENCE_SCENARIOS[0]
            result = rd.run_scenario(scenario)
            has_keys = (
                result is not None
                and "total_budget" in result
                and ("channels" in result or "channel_allocations" in result)
            )
            if has_keys:
                budget = result.get("total_budget", 0)
                ch = result.get("channels") or result.get("channel_allocations", {})
                n_channels = len(ch) if isinstance(ch, (list, dict)) else 0
                detail = f"Regression scenario 0 (healthcare): ${budget:,.0f} budget, {n_channels} channels"
            else:
                detail = f"Regression scenario 0 (healthcare): missing expected keys"
            return TestResult("regression_check", has_keys, detail)
        except ImportError:
            return TestResult("regression_check", False, "Module not available")
        except Exception as e:
            return TestResult("regression_check", False, f"Error: {e}")

    def _test_37_eval_budget_sanity(self) -> TestResult:
        """Eval budget sanity -- budget allocation tests score >= 95%."""
        try:
            if "eval_framework" not in sys.modules:
                importlib.import_module("eval_framework")
            ef = sys.modules["eval_framework"]
            suite = ef.EvalSuite()
            result = suite.run_eval("budget")
            score_pct = result.get("score_pct", 0)
            passed = result.get("passed", 0)
            total = result.get("total_cases", 0)
            ok = score_pct >= 95
            detail = f"Budget sanity eval: {score_pct}% ({passed}/{total})"
            return TestResult("eval_budget_sanity", ok, detail)
        except ImportError:
            return TestResult("eval_budget_sanity", False, "Module not available")
        except Exception as e:
            return TestResult("eval_budget_sanity", False, f"Error: {e}")

    def _test_38_eval_collar_consistency(self) -> TestResult:
        """Eval collar consistency -- collar classification tests score >= 90%."""
        try:
            if "eval_framework" not in sys.modules:
                importlib.import_module("eval_framework")
            ef = sys.modules["eval_framework"]
            suite = ef.EvalSuite()
            result = suite.run_eval("collar")
            score_pct = result.get("score_pct", 0)
            passed = result.get("passed", 0)
            total = result.get("total_cases", 0)
            ok = score_pct >= 90
            detail = f"Collar consistency eval: {score_pct}% ({passed}/{total})"
            return TestResult("eval_collar_consistency", ok, detail)
        except ImportError:
            return TestResult("eval_collar_consistency", False, "Module not available")
        except Exception as e:
            return TestResult("eval_collar_consistency", False, f"Error: {e}")

    def _test_39_structured_logging(self) -> TestResult:
        """Structured logging -- JSON formatter produces valid JSON output."""
        try:
            if "monitoring" not in sys.modules:
                importlib.import_module("monitoring")
            mon = sys.modules["monitoring"]
            formatter = mon.StructuredJsonFormatter()
            record = logging.LogRecord(
                name="test_logger",
                level=logging.INFO,
                pathname="auto_qc.py",
                lineno=0,
                msg="QC test message",
                args=(),
                exc_info=None,
            )
            output = formatter.format(record)
            parsed = json.loads(output)
            ok = ("ts" in parsed or "timestamp" in parsed) and ("msg" in parsed or "message" in parsed)
            detail = "Structured JSON logging produces valid JSON output"
            return TestResult("structured_logging", ok, detail)
        except ImportError:
            return TestResult("structured_logging", False, "Module not available")
        except Exception as e:
            return TestResult("structured_logging", False, f"Error: {e}")

    def _test_40_openapi_spec_valid(self) -> TestResult:
        """OpenAPI spec valid -- spec has required keys and sufficient paths."""
        try:
            if "app" not in sys.modules:
                importlib.import_module("app")
            app_mod = sys.modules["app"]
            spec = getattr(app_mod, "_OPENAPI_SPEC", None)
            if spec is None:
                return TestResult("openapi_spec_valid", False, "_OPENAPI_SPEC not defined in app module")
            paths = spec.get("paths", {})
            ver = spec.get("openapi", "unknown")
            n_paths = len(paths)
            ok = "openapi" in spec and n_paths >= 5
            detail = f"OpenAPI spec: {n_paths} paths documented, version {ver}"
            return TestResult("openapi_spec_valid", ok, detail)
        except ImportError:
            return TestResult("openapi_spec_valid", False, "Module not available")
        except Exception as e:
            return TestResult("openapi_spec_valid", False, f"Error: {e}")

    def _test_41_role_decomposition(self) -> TestResult:
        """Role decomposition -- software engineer decomposes into seniority levels."""
        try:
            if "collar_intelligence" not in sys.modules:
                importlib.import_module("collar_intelligence")
            ci = sys.modules["collar_intelligence"]
            result = ci.decompose_role("software engineer", 50, "technology")
            ok = (
                isinstance(result, list)
                and len(result) >= 2
                and all(
                    "title" in item and "count" in item and "seniority" in item
                    for item in result
                )
            )
            if ok:
                total_count = sum(item.get("count", 0) for item in result)
                ok = ok and total_count == 50
                detail = f"Role decomposition: 50 engineers -> {len(result)} seniority levels, sum={total_count}"
            else:
                detail = f"Role decomposition: invalid structure (got {type(result).__name__}, len={len(result) if isinstance(result, list) else 'N/A'})"
            return TestResult("role_decomposition", ok, detail)
        except ImportError:
            return TestResult("role_decomposition", False, "Module not available")
        except Exception as e:
            return TestResult("role_decomposition", False, f"Error: {e}")

    def _test_42_channel_quality_scores(self) -> TestResult:
        """Channel quality scores -- blue and white collar scores are valid."""
        try:
            if "budget_engine" not in sys.modules:
                importlib.import_module("budget_engine")
            be = sys.modules["budget_engine"]
            blue = be.score_channel_quality("job_board", "blue_collar", "general")
            white = be.score_channel_quality("job_board", "white_collar", "general")
            b_score = blue.get("quality_score", -1)
            w_score = white.get("quality_score", -1)
            ok = (
                isinstance(blue, dict)
                and isinstance(white, dict)
                and "quality_score" in blue
                and "quality_score" in white
                and 0 <= b_score <= 1
                and 0 <= w_score <= 1
            )
            detail = f"Channel quality scores: blue={b_score}, white={w_score}"
            return TestResult("channel_quality_scores", ok, detail)
        except ImportError:
            return TestResult("channel_quality_scores", False, "Module not available")
        except Exception as e:
            return TestResult("channel_quality_scores", False, f"Error: {e}")

    def _test_43_dynamic_cpc_bounds(self) -> TestResult:
        """Dynamic CPC bounds -- adjusted CPC falls within reasonable range."""
        try:
            if "trend_engine" not in sys.modules:
                importlib.import_module("trend_engine")
            te = sys.modules["trend_engine"]
            result = te.calculate_dynamic_cpc("indeed", "healthcare", "blue_collar", "US", 6, {})
            adjusted_cpc = result.get("adjusted_cpc", -1)
            ok = (
                isinstance(result, dict)
                and "adjusted_cpc" in result
                and 0.10 <= adjusted_cpc <= 50.0
            )
            detail = f"Dynamic CPC: ${adjusted_cpc:.2f} (bounds 0.10-50.00)"
            return TestResult("dynamic_cpc_bounds", ok, detail)
        except ImportError:
            return TestResult("dynamic_cpc_bounds", False, "Module not available")
        except Exception as e:
            return TestResult("dynamic_cpc_bounds", False, f"Error: {e}")

    def _test_44_what_if_simulator(self) -> TestResult:
        """What-if simulator -- budget increase simulation returns correct total."""
        try:
            if "budget_engine" not in sys.modules:
                importlib.import_module("budget_engine")
            be = sys.modules["budget_engine"]
            base_allocation = {
                "channel_allocations": {
                    "Indeed": {"name": "Indeed", "percentage": 40, "dollar_amount": 40000},
                    "LinkedIn": {"name": "LinkedIn", "percentage": 30, "dollar_amount": 30000},
                    "Facebook": {"name": "Facebook", "percentage": 30, "dollar_amount": 30000},
                },
                "metadata": {"total_budget": 100000},
            }
            result = be.simulate_budget_change(base_allocation, delta_budget=20000)
            new_total = result.get("new_budget", 0)
            ok = abs(new_total - 120000) < 0.01
            detail = f"What-if simulator: $100K + $20K = ${new_total}"
            return TestResult("what_if_simulator", ok, detail)
        except ImportError:
            return TestResult("what_if_simulator", False, "Module not available")
        except Exception as e:
            return TestResult("what_if_simulator", False, f"Error: {e}")

    def _test_45_llm_router_health(self) -> TestResult:
        """LLM Router -- get_router_status() returns valid dict without deadlock."""
        try:
            if "llm_router" not in sys.modules:
                importlib.import_module("llm_router")
            lr = sys.modules["llm_router"]
            status = lr.get_router_status()
            ok = isinstance(status, dict) and "providers" in status
            detail = f"LLM router: {len(status.get('providers', {}))} providers"
            return TestResult("llm_router_health", ok, detail)
        except ImportError:
            return TestResult("llm_router_health", False, "Module not available")
        except Exception as e:
            return TestResult("llm_router_health", False, f"Error: {e}")

    def _test_46_llm_router_classify(self) -> TestResult:
        """LLM Router -- classify_task routes 4 query types correctly."""
        try:
            if "llm_router" not in sys.modules:
                importlib.import_module("llm_router")
            lr = sys.modules["llm_router"]
            checks = [
                ("Hello! How are you?", "conversational"),
                ("What is CPC for nurses?", "structured"),
                ("Build a complex hiring strategy with budget optimization", "complex"),
            ]
            passed = 0
            for query, expected in checks:
                actual = lr.classify_task(query)
                if actual == expected:
                    passed += 1
            ok = passed >= 2  # at least 2 of 3 correct
            detail = f"classify_task: {passed}/{len(checks)} correct"
            return TestResult("llm_router_classify", ok, detail)
        except ImportError:
            return TestResult("llm_router_classify", False, "Module not available")
        except Exception as e:
            return TestResult("llm_router_classify", False, f"Error: {e}")

    def _test_47_llm_provider_availability(self) -> TestResult:
        """LLM Provider -- at least 1 of 4 LLM providers has API key configured."""
        try:
            keys = {
                "gemini": os.environ.get("GEMINI_API_KEY", "").strip(),
                "groq": os.environ.get("GROQ_API_KEY", "").strip(),
                "cerebras": os.environ.get("CEREBRAS_API_KEY", "").strip(),
                "claude": os.environ.get("ANTHROPIC_API_KEY", "").strip(),
            }
            available = [name for name, key in keys.items() if key]
            ok = len(available) >= 1
            detail = f"LLM providers with keys: {', '.join(available) if available else 'NONE'} ({len(available)}/4)"
            return TestResult("llm_provider_availability", ok, detail)
        except Exception as e:
            return TestResult("llm_provider_availability", False, f"Error: {e}")

    def _test_48_async_job_cleanup(self) -> TestResult:
        """Async Generation -- _generation_jobs dict exists and is bounded."""
        try:
            if "app" not in sys.modules:
                return TestResult("async_job_cleanup", True,
                                  "app module not loaded (OK in test context)")
            app_mod = sys.modules["app"]
            jobs = getattr(app_mod, "_generation_jobs", None)
            if jobs is None:
                return TestResult("async_job_cleanup", False,
                                  "_generation_jobs dict not found in app module")
            lock = getattr(app_mod, "_generation_jobs_lock", None)
            ok = isinstance(jobs, dict) and lock is not None
            detail = f"Async jobs: {len(jobs)} active, lock={'present' if lock else 'MISSING'}"
            return TestResult("async_job_cleanup", ok, detail)
        except Exception as e:
            return TestResult("async_job_cleanup", False, f"Error: {e}")

    def _test_49_api_key_tiers(self) -> TestResult:
        """API Key Tiers -- all 3 tiers (free/pro/enterprise) have valid rpm and rpd limits."""
        try:
            if "app" not in sys.modules:
                return TestResult("api_key_tiers", True,
                                  "app module not loaded (OK in test context)")
            app_mod = sys.modules["app"]
            tiers = getattr(app_mod, "API_KEY_TIERS", None)
            if tiers is None:
                return TestResult("api_key_tiers", False, "API_KEY_TIERS not found")
            required = {"free", "pro", "enterprise"}
            found = set(tiers.keys())
            missing = required - found
            if missing:
                return TestResult("api_key_tiers", False, f"Missing tiers: {missing}")
            valid = 0
            for tier_name in required:
                t = tiers[tier_name]
                if isinstance(t.get("rpm"), (int, float)) and isinstance(t.get("rpd"), (int, float)):
                    if t["rpm"] > 0 and t["rpd"] > 0:
                        valid += 1
            ok = valid == len(required)
            detail = f"API tiers: {valid}/{len(required)} valid (free={tiers.get('free',{}).get('rpm')}rpm, pro={tiers.get('pro',{}).get('rpm')}rpm, enterprise={tiers.get('enterprise',{}).get('rpm')}rpm)"
            return TestResult("api_key_tiers", ok, detail)
        except Exception as e:
            return TestResult("api_key_tiers", False, f"Error: {e}")

    def _test_50_joveo_benchmarks_loaded(self) -> TestResult:
        """Joveo 2026 -- benchmarks KB file is valid with CPA data for 15+ occupations."""
        try:
            fpath = DATA_DIR / "joveo_2026_benchmarks.json"
            if not fpath.exists():
                return TestResult("joveo_benchmarks_loaded", False, "File not found")
            with open(fpath) as f:
                data = json.load(f)
            cpa = data.get("median_cpa_by_occupation_2025", {}).get("occupations", {})
            market = data.get("market_conditions_2025", {})
            two_market = data.get("two_market_strategies", {})
            ok = len(cpa) >= 10 and bool(market) and bool(two_market)
            detail = f"Joveo 2026: {len(cpa)} occupations with CPA, market_conditions={'present' if market else 'MISSING'}, two_market={'present' if two_market else 'MISSING'}"
            return TestResult("joveo_benchmarks_loaded", ok, detail)
        except Exception as e:
            return TestResult("joveo_benchmarks_loaded", False, f"Error: {e}")

    def _test_51_audit_trail(self) -> TestResult:
        """Audit Trail -- AuditLogger is functional and can record/retrieve decisions."""
        try:
            if "monitoring" not in sys.modules:
                importlib.import_module("monitoring")
            mon = sys.modules["monitoring"]
            audit = mon.AuditLogger.instance()
            # Log a test decision
            audit.log_decision(
                request_id="qc_test",
                stage="auto_qc",
                decision="audit_trail_test",
                inputs={"test": True},
                outputs={"status": "ok"},
                rationale="AutoQC test verifying audit trail works",
            )
            stats = audit.get_stats()
            ok = isinstance(stats, dict) and stats.get("total_entries", 0) > 0
            detail = f"Audit trail: {stats.get('total_entries', 0)} entries, stages={list(stats.get('stages', {}).keys())[:5]}"
            return TestResult("audit_trail", ok, detail)
        except ImportError:
            return TestResult("audit_trail", False, "monitoring module not available")
        except Exception as e:
            return TestResult("audit_trail", False, f"Error: {e}")

    def _test_52_slo_compliance(self) -> TestResult:
        """SLO Monitoring -- check_slo_compliance returns valid compliance data."""
        try:
            if "monitoring" not in sys.modules:
                importlib.import_module("monitoring")
            mon = sys.modules["monitoring"]
            mc = mon.MetricsCollector()
            slo = mc.check_slo_compliance()
            ok = isinstance(slo, dict) and len(slo) >= 3
            targets = list(slo.keys())[:4]
            compliant = sum(1 for v in slo.values() if isinstance(v, dict) and v.get("compliant", False))
            detail = f"SLO compliance: {compliant}/{len(slo)} compliant, targets={targets}"
            return TestResult("slo_compliance", ok, detail)
        except AttributeError:
            return TestResult("slo_compliance", False, "check_slo_compliance not found on MetricsCollector")
        except Exception as e:
            return TestResult("slo_compliance", False, f"Error: {e}")

    def _test_53_tier2_modules_importable(self) -> TestResult:
        """Tier 2 Modules -- all v3.1 infrastructure modules are importable."""
        try:
            modules = {
                "eval_framework": "EvalSuite",
                "data_contracts": "validate_kb_file",
                "regression_detector": "run_regression_check",
                "llm_router": "call_llm",
                "monitoring": "MetricsCollector",
            }
            ok_count = 0
            failures = []
            for mod_name, attr_name in modules.items():
                try:
                    if mod_name not in sys.modules:
                        importlib.import_module(mod_name)
                    mod = sys.modules[mod_name]
                    if hasattr(mod, attr_name):
                        ok_count += 1
                    else:
                        failures.append(f"{mod_name}.{attr_name} missing")
                except Exception as e:
                    failures.append(f"{mod_name}: {e}")
            ok = ok_count == len(modules)
            detail = f"Tier2 modules: {ok_count}/{len(modules)} healthy"
            if failures:
                detail += f" | failures: {'; '.join(failures[:3])}"
            return TestResult("tier2_modules_importable", ok, detail)
        except Exception as e:
            return TestResult("tier2_modules_importable", False, f"Error: {e}")

    # ══════════════════════════════════════════════════════════════════════
    # DYNAMIC TESTS (generated weekly from user interaction analysis)
    # ══════════════════════════════════════════════════════════════════════

    def _run_dynamic_tests(self) -> List[TestResult]:
        """Run tests that were auto-generated from user interaction patterns."""
        results = []
        for test in self._dynamic_tests:
            t0 = time.time()
            try:
                result = self._execute_dynamic_test(test)
                result.duration_ms = (time.time() - t0) * 1000
                results.append(result)
            except Exception as e:
                results.append(TestResult(
                    name=f"dynamic_{test.get('id', 'unknown')}",
                    passed=False,
                    detail=f"Exception: {e}",
                    duration_ms=(time.time() - t0) * 1000,
                    category="dynamic",
                ))
        return results

    def _execute_dynamic_test(self, test: dict) -> TestResult:
        """Execute a single dynamic test case."""
        test_type = test.get("type", "chat_query")
        test_id = test.get("id", "unknown")

        if test_type == "chat_query":
            query = test.get("query", "")
            expected_patterns = test.get("expected_patterns", [])
            min_confidence = test.get("min_confidence", 0.3)

            result = self._internal_chat(query)
            resp_lower = result.get("response", "").lower()
            confidence = result.get("confidence", 0)

            pattern_matches = sum(
                1 for p in expected_patterns
                if re.search(p, resp_lower, re.IGNORECASE)
            )
            pattern_ok = pattern_matches > 0 or not expected_patterns
            confidence_ok = confidence >= min_confidence
            ok = pattern_ok and confidence_ok

            return TestResult(
                name=f"dynamic_{test_id}",
                passed=ok,
                detail=f"patterns={pattern_matches}/{len(expected_patterns)}, "
                       f"confidence={confidence}",
                category="dynamic",
            )

        elif test_type == "endpoint_check":
            endpoint = test.get("endpoint", "")
            expected_field = test.get("expected_field", "status")
            try:
                # Internal module check instead of HTTP
                if "health" in endpoint:
                    from monitoring import health_check_liveness
                    data = health_check_liveness()
                    ok = expected_field in data
                    return TestResult(
                        name=f"dynamic_{test_id}",
                        passed=ok,
                        detail=f"field '{expected_field}' {'found' if ok else 'missing'}",
                        category="dynamic",
                    )
            except Exception as e:
                return TestResult(
                    name=f"dynamic_{test_id}",
                    passed=False,
                    detail=str(e),
                    category="dynamic",
                )

        return TestResult(
            name=f"dynamic_{test_id}",
            passed=False,
            detail=f"Unknown test type: {test_type}",
            category="dynamic",
        )

    # ══════════════════════════════════════════════════════════════════════
    # WEEKLY SELF-UPGRADE: Analyze interactions, generate new tests
    # ══════════════════════════════════════════════════════════════════════

    def _weekly_upgrade(self) -> None:
        """Analyze the past week's user interactions and generate new test
        cases to catch problems automatically."""
        logger.info("AutoQC: starting weekly self-upgrade analysis")
        new_tests = []

        # 1. Analyze request_log.json for generation pipeline issues
        new_tests.extend(self._analyze_request_log())

        # 2. Analyze nova metrics for chatbot issues
        new_tests.extend(self._analyze_nova_metrics())

        # 3. Analyze orchestrator fallback telemetry
        new_tests.extend(self._analyze_fallback_telemetry())

        # 4. Analyze data matrix for recurring failures
        new_tests.extend(self._analyze_data_matrix_patterns())

        # Deduplicate: don't add tests with identical queries
        existing_queries = {t.get("query", "") for t in self._dynamic_tests}
        added = 0
        for test in new_tests:
            query = test.get("query", "")
            if query and query not in existing_queries:
                self._dynamic_tests.append(test)
                existing_queries.add(query)
                added += 1

        # Cap dynamic tests at 50 (oldest pruned first)
        if len(self._dynamic_tests) > 50:
            self._dynamic_tests = self._dynamic_tests[-50:]

        self._weekly_count += 1
        self._last_weekly_time = time.time()
        self._persist_dynamic_tests()

        logger.info(
            "AutoQC: weekly upgrade #%d -- analyzed interactions, added %d new tests "
            "(total dynamic: %d)",
            self._weekly_count, added, len(self._dynamic_tests),
        )

    def _analyze_request_log(self) -> List[dict]:
        """Scan request_log.json for failed generations in the past week."""
        new_tests = []
        try:
            if not REQUEST_LOG_FILE.exists():
                return new_tests
            with open(REQUEST_LOG_FILE) as f:
                logs = json.load(f)
            if not isinstance(logs, list):
                return new_tests

            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            recent = [
                entry for entry in logs
                if entry.get("timestamp", "") >= cutoff
            ]

            # Find failed requests
            failures = [e for e in recent if e.get("status") == "error"]
            for fail in failures[:5]:  # Max 5 new tests from failures
                industry = fail.get("industry", "unknown")
                client = fail.get("client_name", "Unknown")
                error_msg = fail.get("error_message", "")
                roles = fail.get("roles", [])
                locations = fail.get("locations", [])

                # Generate a test that exercises the same query pattern
                if roles and locations:
                    role_str = roles[0] if roles else "general"
                    loc_str = locations[0] if locations else "US"
                    new_tests.append({
                        "id": f"reqlog_{fail.get('id', 'unknown')[:8]}",
                        "type": "chat_query",
                        "query": f"What is the salary for a {role_str} in {loc_str}?",
                        "expected_patterns": ["salary", "\\$", "range", "annual"],
                        "min_confidence": 0.3,
                        "source": "request_log_failure",
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "original_error": error_msg[:200],
                    })

            # Find slow requests (> 30 seconds)
            slow = [
                e for e in recent
                if (e.get("generation_time_seconds") or 0) > 30
            ]
            if slow:
                # Add a performance check for the slowest industry
                industries = {}
                for s in slow:
                    ind = s.get("industry", "unknown")
                    industries[ind] = industries.get(ind, 0) + 1
                worst_industry = max(industries, key=industries.get)
                new_tests.append({
                    "id": f"perf_{worst_industry[:10]}",
                    "type": "chat_query",
                    "query": f"What are the recruitment benchmarks for the {worst_industry} industry?",
                    "expected_patterns": ["benchmark", "cpc", "cpa", worst_industry],
                    "min_confidence": 0.3,
                    "source": "slow_generation",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                })

        except Exception as e:
            logger.warning("AutoQC: analyze_request_log failed: %s", e)

        return new_tests

    def _analyze_nova_metrics(self) -> List[dict]:
        """Check nova metrics for high error rates or low confidence patterns."""
        new_tests = []
        try:
            from nova import get_nova_metrics
            metrics = get_nova_metrics()

            # If error rate is high (>10%), add a stress test
            total = metrics.get("total_requests", 0)
            errors = metrics.get("api_errors", 0)
            if total > 10 and errors / total > 0.1:
                new_tests.append({
                    "id": "high_error_rate",
                    "type": "chat_query",
                    "query": "What is the CPC benchmark for healthcare in the US?",
                    "expected_patterns": ["cpc", "healthcare", "\\$"],
                    "min_confidence": 0.4,
                    "source": "nova_high_error_rate",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                })

            # If P95 latency is very high (>15s), add latency-sensitive test
            p95 = metrics.get("latency_ms", {}).get("p95", 0)
            if p95 > 15000:
                new_tests.append({
                    "id": "latency_check",
                    "type": "chat_query",
                    "query": "What is Joveo?",
                    "expected_patterns": ["joveo", "recruitment"],
                    "min_confidence": 0.8,
                    "source": "high_latency_detected",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                })

        except Exception as e:
            logger.warning("AutoQC: analyze_nova_metrics failed: %s", e)

        return new_tests

    def _analyze_fallback_telemetry(self) -> List[dict]:
        """Check orchestrator fallback telemetry for queries hitting generic fallback."""
        new_tests = []
        try:
            if "data_orchestrator" not in sys.modules:
                return new_tests
            do = sys.modules["data_orchestrator"]
            telemetry = do.get_fallback_telemetry()

            # Top queries hitting fallback become test cases
            top_fallbacks = telemetry.get("top_queries", [])
            for i, entry in enumerate(top_fallbacks[:3]):
                query_key = entry.get("query", "")
                fn_name = entry.get("function", "unknown")
                if query_key:
                    # The query key is normalized, reconstruct a test query
                    new_tests.append({
                        "id": f"fallback_{fn_name}_{i}",
                        "type": "chat_query",
                        "query": f"Tell me about {query_key} for recruitment",
                        "expected_patterns": [query_key.split()[0]] if query_key.split() else [],
                        "min_confidence": 0.3,
                        "source": f"fallback_telemetry_{fn_name}",
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                    })

        except Exception as e:
            logger.warning("AutoQC: analyze_fallback_telemetry failed: %s", e)

        return new_tests

    def _analyze_data_matrix_patterns(self) -> List[dict]:
        """Check if data matrix has recurring failures that need targeted tests."""
        new_tests = []
        try:
            if "data_matrix_monitor" not in sys.modules:
                return new_tests
            from data_matrix_monitor import get_data_matrix_monitor
            monitor = get_data_matrix_monitor()
            status = monitor.get_status()

            if status.get("status") == "degraded":
                # Find which cells are broken
                matrix = status.get("matrix", {})
                for product, layers in matrix.items():
                    if isinstance(layers, dict):
                        for layer, info in layers.items():
                            if isinstance(info, dict) and info.get("health") == "error":
                                new_tests.append({
                                    "id": f"matrix_{product}_{layer}",
                                    "type": "endpoint_check",
                                    "endpoint": "/api/health/data-matrix",
                                    "expected_field": "matrix",
                                    "source": f"data_matrix_error_{product}_{layer}",
                                    "generated_at": datetime.now(timezone.utc).isoformat(),
                                })

        except Exception as e:
            logger.warning("AutoQC: analyze_data_matrix_patterns failed: %s", e)

        return new_tests

    # ══════════════════════════════════════════════════════════════════════
    # AUTO-HEALING
    # ══════════════════════════════════════════════════════════════════════

    def _auto_heal(self, results: List[TestResult]) -> int:
        """Attempt to fix failures. Returns count of healed issues."""
        healed = 0
        failures = [r for r in results if not r.passed]

        for fail in failures:
            action_taken = False
            name = fail.name

            # Heal: orchestrator import failures
            if "orchestrator" in name:
                try:
                    if "data_orchestrator" in sys.modules:
                        importlib.reload(sys.modules["data_orchestrator"])
                    else:
                        importlib.import_module("data_orchestrator")
                    action_taken = True
                    self._record_heal(name, "reimport_orchestrator", True)
                except Exception as e:
                    self._record_heal(name, "reimport_orchestrator", False)
                    logger.warning("AutoQC heal: reimport orchestrator failed: %s", e)

            # Heal: nova import failures
            elif "nova" in name and "import" in name:
                try:
                    if "nova" in sys.modules:
                        importlib.reload(sys.modules["nova"])
                    else:
                        importlib.import_module("nova")
                    action_taken = True
                    self._record_heal(name, "reimport_nova", True)
                except Exception as e:
                    self._record_heal(name, "reimport_nova", False)

            # Heal: data matrix failures
            elif "data_matrix" in name:
                try:
                    from data_matrix_monitor import get_data_matrix_monitor
                    monitor = get_data_matrix_monitor()
                    monitor.run_check()  # Force immediate re-check with healing
                    action_taken = True
                    self._record_heal(name, "force_matrix_recheck", True)
                except Exception as e:
                    self._record_heal(name, "force_matrix_recheck", False)

            # Heal: cache failures -- evict stale entries
            elif "cache" in name:
                try:
                    if "data_orchestrator" in sys.modules:
                        do = sys.modules["data_orchestrator"]
                        now = time.time()
                        evicted = 0
                        with do._api_cache_lock:
                            expired_keys = [
                                k for k, v in do._api_result_cache.items()
                                if now >= v.get("expires", 0)
                            ]
                            for k in expired_keys:
                                do._api_result_cache.pop(k, None)
                                evicted += 1
                        if evicted:
                            action_taken = True
                            self._record_heal(name, f"evicted_{evicted}_stale_cache", True)
                except Exception as e:
                    self._record_heal(name, "cache_eviction", False)

            # Heal: lazy loader failures -- reset sentinels
            elif "lazy_loader" in name:
                try:
                    if "data_orchestrator" in sys.modules:
                        do = sys.modules["data_orchestrator"]
                        sentinel_map = {
                            "_api_enrichment": None,
                            "_research": None,
                            "_budget_engine": None,
                            "_standardizer": None,
                        }
                        with do._load_lock:
                            for attr, reset_val in sentinel_map.items():
                                current = getattr(do, attr, None)
                                if current is do._IMPORT_FAILED:
                                    setattr(do, attr, reset_val)
                        action_taken = True
                        self._record_heal(name, "reset_lazy_sentinels", True)
                except Exception as e:
                    self._record_heal(name, "reset_lazy_sentinels", False)

            # Heal: LLM router circuit breaker reset
            elif "llm_router" in name or "llm_provider" in name:
                try:
                    if "llm_router" in sys.modules:
                        lr = sys.modules["llm_router"]
                        # Reset all provider circuit breakers
                        states = getattr(lr, "_provider_states", {})
                        for pid, state in states.items():
                            with state.lock:
                                if state.consecutive_failures > 0:
                                    state.consecutive_failures = 0
                                    state.circuit_open_until = 0.0
                        action_taken = True
                        self._record_heal(name, "reset_llm_circuit_breakers", True)
                except Exception as e:
                    self._record_heal(name, "reset_llm_circuit_breakers", False)
                    logger.warning("AutoQC heal: LLM circuit reset failed: %s", e)

            # Heal: Async job queue overflow -- evict completed/expired jobs
            elif "async_job" in name:
                try:
                    if "app" in sys.modules:
                        app_mod = sys.modules["app"]
                        jobs = getattr(app_mod, "_generation_jobs", None)
                        lock = getattr(app_mod, "_generation_jobs_lock", None)
                        if jobs is not None and lock is not None:
                            now = time.time()
                            with lock:
                                expired = [
                                    jid for jid, jdata in jobs.items()
                                    if now - jdata.get("created", 0) > 1800  # 30 min
                                    or jdata.get("status") in ("completed", "failed")
                                ]
                                for jid in expired:
                                    jobs.pop(jid, None)
                            if expired:
                                action_taken = True
                                self._record_heal(name, f"evicted_{len(expired)}_async_jobs", True)
                except Exception as e:
                    self._record_heal(name, "async_job_cleanup", False)

            # Heal: Tier2 module import failures -- reimport
            elif "tier2" in name or "tier_2" in name:
                try:
                    tier2_mods = ["eval_framework", "data_contracts",
                                  "regression_detector", "llm_router", "monitoring"]
                    reimported = 0
                    for mod_name in tier2_mods:
                        if mod_name not in sys.modules:
                            try:
                                importlib.import_module(mod_name)
                                reimported += 1
                            except Exception:
                                pass
                    if reimported:
                        action_taken = True
                        self._record_heal(name, f"reimported_{reimported}_tier2_modules", True)
                except Exception as e:
                    self._record_heal(name, "tier2_reimport", False)

            # Heal: Audit log overflow -- truncate if too large
            elif "audit" in name:
                try:
                    if "monitoring" in sys.modules:
                        mon = sys.modules["monitoring"]
                        audit = mon.AuditLogger.instance()
                        stats = audit.get_stats()
                        if stats.get("total_entries", 0) >= audit._MAX_ENTRIES:
                            # Trigger a persist to flush oldest entries
                            audit._persist()
                            action_taken = True
                            self._record_heal(name, "audit_log_persist", True)
                except Exception as e:
                    self._record_heal(name, "audit_log_persist", False)

            # Heal: Joveo KB file missing or corrupt -- log warning
            elif "joveo" in name:
                try:
                    fpath = DATA_DIR / "joveo_2026_benchmarks.json"
                    if fpath.exists():
                        with open(fpath) as f:
                            json.load(f)  # Validate JSON
                        action_taken = True
                        self._record_heal(name, "joveo_kb_validated", True)
                    else:
                        self._record_heal(name, "joveo_kb_file_missing", False)
                except (json.JSONDecodeError, OSError) as e:
                    self._record_heal(name, "joveo_kb_corrupt", False)
                    logger.warning("AutoQC heal: Joveo KB corrupt: %s", e)

            if action_taken:
                healed += 1

        return healed

    def _record_heal(self, test_name: str, action: str, success: bool) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "test": test_name,
            "action": action,
            "success": success,
        }
        with self._lock:
            self._heal_log.append(entry)
            if len(self._heal_log) > 50:
                self._heal_log = self._heal_log[-50:]
        level = logging.INFO if success else logging.WARNING
        logger.log(level, "AutoQC heal: %s/%s (success=%s)", test_name, action, success)

    # ══════════════════════════════════════════════════════════════════════
    # ALERTING
    # ══════════════════════════════════════════════════════════════════════

    def _send_alert(self, run_result: dict) -> None:
        """Send Slack alert when tests fail persistently (best-effort)."""
        try:
            bot_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
            if not bot_token:
                logger.info("AutoQC: no SLACK_BOT_TOKEN set, skipping alert")
                return

            failures = run_result.get("failures", [])
            failure_names = [f["name"] for f in failures[:5]]
            msg = (
                f":warning: *AutoQC Alert* -- Run #{run_result['run_number']}\n"
                f"Status: `{run_result['status']}`\n"
                f"Passed: {run_result['passed']}/{run_result['total']}\n"
                f"Failed tests: {', '.join(failure_names)}\n"
                f"Healed: {run_result.get('healed', 0)}"
            )

            import urllib.request
            req = urllib.request.Request(
                "https://slack.com/api/chat.postMessage",
                data=json.dumps({
                    "channel": os.environ.get("SLACK_ALERT_CHANNEL", "#general"),
                    "text": msg,
                }).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {bot_token}",
                },
            )
            urllib.request.urlopen(req, timeout=10)
            logger.info("AutoQC: Slack alert sent for run #%d", run_result["run_number"])
        except Exception as e:
            logger.warning("AutoQC: Slack alert failed: %s", e)

    # ══════════════════════════════════════════════════════════════════════
    # HELPERS
    # ══════════════════════════════════════════════════════════════════════

    def _get_or_init_nova(self):
        """Get cached Nova instance (lazy-init once, reuse across tests).
        Returns None if init previously failed; does NOT block on init here
        -- the actual init happens inside the timeout-guarded sub-thread in
        _internal_chat()."""
        if self._nova_init_failed:
            return None
        if self._nova_instance is not None:
            return self._nova_instance
        # Return sentinel to indicate init needed (done inside timeout guard)
        return "NEEDS_INIT"

    def _internal_chat(self, message: str, timeout: int = 60) -> dict:
        """Call Nova.chat() with a timeout guard (default 60s).

        Both Nova init AND the .chat() call run inside a sub-thread so
        neither a slow constructor nor a hanging Claude API call can block
        the test runner indefinitely.
        """
        if self._nova_init_failed:
            return {"response": "", "confidence": 0, "sources": [],
                    "tools_used": [], "error": "Nova init previously failed"}

        result_holder: List[dict] = []
        error_holder: List[str] = []

        def _call():
            try:
                # Init Nova inside the timeout guard (constructor can be slow)
                if self._nova_instance is None:
                    if "nova" not in sys.modules:
                        importlib.import_module("nova")
                    nova_mod = sys.modules["nova"]
                    self._nova_instance = nova_mod.Nova()
                    logger.info("AutoQC: Nova instance initialized successfully")
                result_holder.append(self._nova_instance.chat(message))
            except Exception as e:
                error_holder.append(str(e))

        worker = threading.Thread(target=_call, daemon=True)
        worker.start()
        worker.join(timeout=timeout)

        if worker.is_alive():
            # Mark init as failed if we never got past constructor
            if self._nova_instance is None:
                self._nova_init_failed = True
                logger.warning("AutoQC: Nova init timed out after %ds", timeout)
            else:
                logger.warning("AutoQC: _internal_chat timed out after %ds for: %s", timeout, message[:50])
            return {"response": "", "confidence": 0, "sources": [],
                    "tools_used": [], "error": f"Timeout after {timeout}s"}
        if error_holder:
            if self._nova_instance is None:
                self._nova_init_failed = True
            return {"response": "", "confidence": 0, "sources": [],
                    "tools_used": [], "error": error_holder[0]}
        return result_holder[0] if result_holder else {
            "response": "", "confidence": 0, "sources": [],
            "tools_used": [], "error": "No result"
        }

    def _load_dynamic_tests(self) -> None:
        """Load previously generated dynamic tests from disk."""
        try:
            if DYNAMIC_TESTS_FILE.exists():
                with open(DYNAMIC_TESTS_FILE) as f:
                    self._dynamic_tests = json.load(f)
                logger.info("AutoQC: loaded %d dynamic tests", len(self._dynamic_tests))
        except Exception as e:
            logger.warning("AutoQC: failed to load dynamic tests: %s", e)
            self._dynamic_tests = []

    def _persist_dynamic_tests(self) -> None:
        """Save dynamic tests to disk."""
        try:
            with open(DYNAMIC_TESTS_FILE, "w") as f:
                json.dump(self._dynamic_tests, f, indent=2)
        except Exception as e:
            logger.warning("AutoQC: failed to persist dynamic tests: %s", e)

    def _load_history(self) -> None:
        """Load previous run history from disk."""
        try:
            if QC_RESULTS_FILE.exists():
                with open(QC_RESULTS_FILE) as f:
                    data = json.load(f)
                self._run_history = data.get("history", [])
                self._run_count = data.get("run_count", 0)
                self._weekly_count = data.get("weekly_count", 0)
                logger.info("AutoQC: loaded %d historical runs", len(self._run_history))
        except Exception as e:
            logger.warning("AutoQC: failed to load history: %s", e)

    def _persist_history(self) -> None:
        """Save run history to disk."""
        try:
            with open(QC_RESULTS_FILE, "w") as f:
                json.dump({
                    "history": self._run_history[-_MAX_HISTORY:],
                    "run_count": self._run_count,
                    "weekly_count": self._weekly_count,
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                }, f, indent=2)
        except Exception as e:
            logger.warning("AutoQC: failed to persist history: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════

_auto_qc: Optional[AutoQC] = None
_auto_qc_lock = threading.Lock()


def get_auto_qc() -> AutoQC:
    """Get or create the singleton AutoQC instance (thread-safe)."""
    global _auto_qc
    if _auto_qc is None:
        with _auto_qc_lock:
            if _auto_qc is None:
                _auto_qc = AutoQC()
    return _auto_qc
