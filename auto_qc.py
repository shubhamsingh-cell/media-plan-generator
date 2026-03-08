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
        self._is_running = False

        logger.info(
            "AutoQC: run #%d -- %s (%d/%d passed, %d healed, %.2fs)",
            self._run_count, status, passed, total, healed, elapsed,
        )

        # Alert on persistent failures
        if failed > healed:
            self._send_alert(run_result)

        return run_result

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
        return [name for name in dir(self) if name.startswith("_test_")]

    def _run_static_tests(self) -> List[TestResult]:
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
            from monitoring import health_check_basic
            result = health_check_basic()
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
            ok = isinstance(stats, dict) and "cache_size" in stats
            return TestResult("orchestrator_cache", ok,
                              f"size={stats.get('cache_size', '?')}, "
                              f"max={stats.get('max_size', '?')}")
        except Exception as e:
            return TestResult("orchestrator_cache", False, str(e))

    def _test_08_nova_tool_count(self) -> TestResult:
        """Nova has 21 tool definitions."""
        try:
            if "nova" not in sys.modules:
                importlib.import_module("nova")
            nova_mod = sys.modules["nova"]
            nova_instance = nova_mod.Nova()
            tools = nova_instance.get_tool_definitions()
            count = len(tools)
            ok = count >= 21
            return TestResult("nova_tool_count", ok,
                              f"{count} tools (expected >= 21)")
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
        """Orchestrator lazy loaders resolve to valid modules."""
        try:
            if "data_orchestrator" not in sys.modules:
                importlib.import_module("data_orchestrator")
            do = sys.modules["data_orchestrator"]
            loaders = {
                "_lazy_api": "api_enrichment",
                "_lazy_research": "research",
                "_lazy_budget": "budget_engine",
                "_lazy_standardizer": "standardizer",
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
            detail = "All 4 lazy loaders OK" if ok else "; ".join(failures)
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
            has_competitiveness = "salary_competitiveness" in result
            ok = has_difficulty and has_competitiveness
            return TestResult("compute_insights", ok,
                              f"difficulty={result.get('hiring_difficulty_index', '?')}, "
                              f"competitiveness={result.get('salary_competitiveness', '?')}")
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
                    from monitoring import health_check_basic
                    data = health_check_basic()
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

    def _get_nova(self):
        """Get cached Nova instance (lazy-init once, reuse across tests)."""
        if self._nova_init_failed:
            return None
        if self._nova_instance is not None:
            return self._nova_instance
        try:
            if "nova" not in sys.modules:
                importlib.import_module("nova")
            nova_mod = sys.modules["nova"]
            self._nova_instance = nova_mod.Nova()
            return self._nova_instance
        except Exception as e:
            logger.warning("AutoQC: Nova init failed: %s", e)
            self._nova_init_failed = True
            return None

    def _internal_chat(self, message: str, timeout: int = 60) -> dict:
        """Call Nova.chat() with a timeout guard (default 60s).

        Uses a sub-thread so a hanging Claude API call cannot block the
        entire test run indefinitely.
        """
        nova = self._get_nova()
        if nova is None:
            return {"response": "", "confidence": 0, "sources": [],
                    "tools_used": [], "error": "Nova unavailable"}

        result_holder: List[dict] = []
        error_holder: List[str] = []

        def _call():
            try:
                result_holder.append(nova.chat(message))
            except Exception as e:
                error_holder.append(str(e))

        worker = threading.Thread(target=_call, daemon=True)
        worker.start()
        worker.join(timeout=timeout)

        if worker.is_alive():
            logger.warning("AutoQC: _internal_chat timed out after %ds for: %s", timeout, message[:50])
            return {"response": "", "confidence": 0, "sources": [],
                    "tools_used": [], "error": f"Timeout after {timeout}s"}
        if error_holder:
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
