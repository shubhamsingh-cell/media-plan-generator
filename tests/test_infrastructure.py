"""Unit tests for infrastructure modules: auto_qc, data_matrix_monitor, data_enrichment.

Tests are designed to run offline (no API keys, no network access).
All external calls are mocked where needed.
"""

from __future__ import annotations

import importlib
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest

# ── Ensure project root is importable ────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# auto_qc.py tests
# =============================================================================


class TestAutoQcModule:
    """Tests for the auto_qc infrastructure module."""

    def test_testresult_class_exists(self) -> None:
        """TestResult class must be importable from auto_qc."""
        from auto_qc import TestResult

        assert TestResult is not None
        assert callable(TestResult)

    def test_testresult_fields(self) -> None:
        """TestResult instances expose the expected slot attributes."""
        from auto_qc import TestResult

        result = TestResult(
            name="sample",
            passed=True,
            detail="ok",
            duration_ms=1.23,
            category="static",
        )
        assert result.name == "sample"
        assert result.passed is True
        assert result.detail == "ok"
        assert result.duration_ms == 1.2  # rounded to 1 decimal
        assert result.category == "static"

    def test_testresult_to_dict(self) -> None:
        """TestResult.to_dict returns a dict with all expected keys."""
        from auto_qc import TestResult

        result = TestResult(name="t1", passed=False, detail="fail")
        d = result.to_dict()
        assert isinstance(d, dict)
        for key in ("name", "passed", "detail", "duration_ms", "category"):
            assert key in d, f"Missing key: {key}"
        assert d["name"] == "t1"
        assert d["passed"] is False

    def test_qc_has_test_definitions(self) -> None:
        """AutoQC must have internal test definitions (static test names)."""
        from auto_qc import AutoQC

        qc = AutoQC()
        names = qc._get_static_test_names()
        assert isinstance(names, list)
        assert len(names) > 0, "AutoQC should define at least one static test"

    def test_qc_functions_importable(self) -> None:
        """Key public functions and classes must be importable."""
        from auto_qc import AutoQC, TestResult, get_auto_qc

        assert callable(get_auto_qc)
        assert callable(AutoQC)
        assert callable(TestResult)

    def test_qc_result_format(self) -> None:
        """AutoQC.run_tests returns a dict with expected top-level keys."""
        from auto_qc import AutoQC

        qc = AutoQC()
        # Mock heavy internal methods to avoid real module probing
        with mock.patch.object(
            qc, "_run_static_tests", return_value=[]
        ), mock.patch.object(
            qc, "_run_dynamic_tests", return_value=[]
        ), mock.patch.object(
            qc, "_persist_history"
        ), mock.patch.object(
            qc, "_auto_heal", return_value=0
        ):
            result = qc.run_tests()

        assert isinstance(result, dict)
        assert "status" in result
        assert result["status"] == "all_passing"
        assert "total" in result
        assert "tests" in result

    def test_qc_constants_defined(self) -> None:
        """Scheduling constants must be defined and positive."""
        from auto_qc import (
            _INITIAL_DELAY,
            _MAX_HISTORY,
            _PER_TEST_TIMEOUT,
            _TEST_INTERVAL,
            _WEEKLY_INTERVAL,
        )

        assert _TEST_INTERVAL > 0
        assert _INITIAL_DELAY > 0
        assert _WEEKLY_INTERVAL > 0
        assert _MAX_HISTORY > 0
        assert _PER_TEST_TIMEOUT > 0

    def test_qc_data_paths_defined(self) -> None:
        """QC data file paths must be Path objects."""
        from auto_qc import DATA_DIR, DYNAMIC_TESTS_FILE, QC_RESULTS_FILE

        assert isinstance(DATA_DIR, Path)
        assert isinstance(QC_RESULTS_FILE, Path)
        assert isinstance(DYNAMIC_TESTS_FILE, Path)

    def test_autoqc_get_status_returns_dict(self) -> None:
        """get_status should return a well-formed dict."""
        from auto_qc import AutoQC

        qc = AutoQC()
        status = qc.get_status()
        assert isinstance(status, dict)
        assert "status" in status
        # Valid statuses: pending, running, all_passing, degraded, healed
        valid = ("pending", "running", "all_passing", "degraded", "healed")
        assert status["status"] in valid, f"Unexpected status: {status['status']}"


# =============================================================================
# data_matrix_monitor.py tests
# =============================================================================


class TestDataMatrixMonitor:
    """Tests for the data_matrix_monitor infrastructure module."""

    def test_monitor_importable(self) -> None:
        """Module and key classes must be importable."""
        from data_matrix_monitor import DataMatrixMonitor, get_data_matrix_monitor

        assert DataMatrixMonitor is not None
        assert callable(get_data_matrix_monitor)

    def test_monitor_has_run_check(self) -> None:
        """DataMatrixMonitor must expose a run_check method."""
        from data_matrix_monitor import DataMatrixMonitor

        assert hasattr(DataMatrixMonitor, "run_check")
        assert callable(getattr(DataMatrixMonitor, "run_check"))

    def test_monitor_check_returns_dict(self) -> None:
        """run_check must return a dict (mocking heavy probes)."""
        from data_matrix_monitor import DataMatrixMonitor

        monitor = DataMatrixMonitor()
        # Stub out the actual probing to avoid imports / network
        with mock.patch.object(monitor, "_probe_layer", return_value={"status": "ok"}):
            result = monitor.run_check()

        assert isinstance(result, dict)
        # Should have a status key at minimum
        assert "status" in result or "matrix" in result or "counts" in result

    def test_heal_log_max_size(self) -> None:
        """_MAX_HEAL_LOG must be a positive integer."""
        from data_matrix_monitor import _MAX_HEAL_LOG

        assert isinstance(_MAX_HEAL_LOG, int)
        assert _MAX_HEAL_LOG > 0

    def test_expected_matrix_defined(self) -> None:
        """EXPECTED_MATRIX must be a non-empty dict of dicts."""
        from data_matrix_monitor import EXPECTED_MATRIX

        assert isinstance(EXPECTED_MATRIX, dict)
        assert len(EXPECTED_MATRIX) > 0
        for product, layers in EXPECTED_MATRIX.items():
            assert isinstance(product, str)
            assert isinstance(layers, dict)
            assert len(layers) > 0

    def test_required_kb_files_defined(self) -> None:
        """_REQUIRED_KB_FILES must be a non-empty list of filenames."""
        from data_matrix_monitor import _REQUIRED_KB_FILES

        assert isinstance(_REQUIRED_KB_FILES, list)
        assert len(_REQUIRED_KB_FILES) > 0
        for name in _REQUIRED_KB_FILES:
            assert name.endswith(".json"), f"Expected .json file, got: {name}"

    def test_tracked_env_vars_defined(self) -> None:
        """_TRACKED_ENV_VARS must list known env var names."""
        from data_matrix_monitor import _TRACKED_ENV_VARS

        assert isinstance(_TRACKED_ENV_VARS, list)
        assert len(_TRACKED_ENV_VARS) > 0
        for var in _TRACKED_ENV_VARS:
            assert isinstance(var, str)
            assert var == var.upper(), f"Env var {var} should be uppercase"

    def test_monitor_get_status_before_check(self) -> None:
        """get_status should return a dict even before first check."""
        from data_matrix_monitor import DataMatrixMonitor

        monitor = DataMatrixMonitor()
        status = monitor.get_status()
        assert isinstance(status, dict)
        assert "status" in status or "message" in status

    def test_check_interval_positive(self) -> None:
        """_CHECK_INTERVAL must be a positive number."""
        from data_matrix_monitor import _CHECK_INTERVAL

        assert _CHECK_INTERVAL > 0

    def test_monitor_thread_safety(self) -> None:
        """DataMatrixMonitor must use a threading lock."""
        from data_matrix_monitor import DataMatrixMonitor

        monitor = DataMatrixMonitor()
        assert hasattr(monitor, "_lock")
        assert isinstance(monitor._lock, type(threading.Lock()))


# =============================================================================
# data_enrichment.py tests
# =============================================================================


class TestDataEnrichment:
    """Tests for the data_enrichment infrastructure module."""

    def test_enrichment_importable(self) -> None:
        """Module and key classes/functions must be importable."""
        from data_enrichment import (
            DataEnrichmentEngine,
            get_engine,
            get_enrichment_status,
            start_enrichment,
        )

        assert DataEnrichmentEngine is not None
        assert callable(get_engine)
        assert callable(get_enrichment_status)
        assert callable(start_enrichment)

    def test_freshness_thresholds_defined(self) -> None:
        """FRESHNESS_THRESHOLDS must be a non-empty dict."""
        from data_enrichment import FRESHNESS_THRESHOLDS

        assert isinstance(FRESHNESS_THRESHOLDS, dict)
        assert len(FRESHNESS_THRESHOLDS) > 0

    def test_all_thresholds_are_positive_ints(self) -> None:
        """Every threshold value must be a positive integer (hours)."""
        from data_enrichment import FRESHNESS_THRESHOLDS

        for source, hours in FRESHNESS_THRESHOLDS.items():
            assert isinstance(
                hours, int
            ), f"Threshold for {source} should be int, got {type(hours).__name__}"
            assert hours > 0, f"Threshold for {source} must be positive, got {hours}"

    def test_data_dir_exists(self) -> None:
        """DATA_DIR must point to an existing directory."""
        from data_enrichment import DATA_DIR

        assert isinstance(DATA_DIR, Path)
        assert DATA_DIR.exists(), f"DATA_DIR does not exist: {DATA_DIR}"
        assert DATA_DIR.is_dir(), f"DATA_DIR is not a directory: {DATA_DIR}"

    def test_enrichment_state_file_path(self) -> None:
        """ENRICHMENT_STATE_FILE must be a Path inside DATA_DIR."""
        from data_enrichment import DATA_DIR, ENRICHMENT_STATE_FILE

        assert isinstance(ENRICHMENT_STATE_FILE, Path)
        assert ENRICHMENT_STATE_FILE.parent == DATA_DIR

    def test_supabase_config_defined(self) -> None:
        """Supabase config vars must be defined (may be empty without env vars)."""
        from data_enrichment import SUPABASE_KEY, SUPABASE_URL

        assert isinstance(SUPABASE_URL, str)
        assert isinstance(SUPABASE_KEY, str)
        # URL should be non-empty (has a fallback default)
        assert len(SUPABASE_URL) > 0

    def test_enrichment_interval_positive(self) -> None:
        """ENRICHMENT_INTERVAL must be a positive number."""
        from data_enrichment import ENRICHMENT_INTERVAL

        assert ENRICHMENT_INTERVAL > 0

    def test_max_log_entries_positive(self) -> None:
        """_MAX_LOG_ENTRIES must be a positive integer."""
        from data_enrichment import _MAX_LOG_ENTRIES

        assert isinstance(_MAX_LOG_ENTRIES, int)
        assert _MAX_LOG_ENTRIES > 0

    def test_engine_init_loads_state(self) -> None:
        """DataEnrichmentEngine.__init__ must populate _state."""
        from data_enrichment import DataEnrichmentEngine

        # Mock Supabase and file reads to avoid I/O
        with mock.patch("data_enrichment._SUPABASE_ENABLED", False), mock.patch(
            "data_enrichment.ENRICHMENT_STATE_FILE",
            mock.MagicMock(exists=mock.MagicMock(return_value=False)),
        ):
            engine = DataEnrichmentEngine()

        assert isinstance(engine._state, dict)
        assert "last_runs" in engine._state or "stats" in engine._state

    def test_engine_thread_safety(self) -> None:
        """DataEnrichmentEngine must use a threading lock."""
        from data_enrichment import DataEnrichmentEngine

        with mock.patch("data_enrichment._SUPABASE_ENABLED", False), mock.patch(
            "data_enrichment.ENRICHMENT_STATE_FILE",
            mock.MagicMock(exists=mock.MagicMock(return_value=False)),
        ):
            engine = DataEnrichmentEngine()

        assert hasattr(engine, "_lock")
        assert isinstance(engine._lock, type(threading.Lock()))

    def test_freshness_threshold_sources_nonempty_keys(self) -> None:
        """All source keys in FRESHNESS_THRESHOLDS must be non-empty strings."""
        from data_enrichment import FRESHNESS_THRESHOLDS

        for source in FRESHNESS_THRESHOLDS:
            assert isinstance(source, str)
            assert len(source.strip()) > 0, "Empty source key in FRESHNESS_THRESHOLDS"

    def test_retry_with_backoff_exists(self) -> None:
        """Check if retry_with_backoff helper exists in the module.

        This test is conditional: it passes whether the function exists or not,
        but validates its callability if present.
        """
        import data_enrichment

        if hasattr(data_enrichment, "retry_with_backoff"):
            assert callable(data_enrichment.retry_with_backoff)
        else:
            pytest.skip("retry_with_backoff not yet added to data_enrichment")
