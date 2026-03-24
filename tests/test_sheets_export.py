"""Tests for sheets_export module.

Validates CSV/XLSX fallback generation, sheet-builder logic,
status reporting, and NoneType safety across all functions.
"""

import csv
import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch, MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from sheets_export import (
    export_to_csv,
    export_to_xlsx,
    export_media_plan,
    get_status,
    _safe_str,
    _format_currency,
    _format_number,
    _build_summary_sheet,
    _build_channels_sheet,
    _build_budget_sheet,
    _build_benchmarks_sheet,
    _build_timeline_sheet,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_plan_data() -> Dict[str, Any]:
    """A realistic media plan data dict for testing."""
    return {
        "client_name": "Acme Corp",
        "industry": "tech_engineering",
        "job_title": "Senior Software Engineer",
        "budget": "25000",
        "locations": ["San Francisco, CA", "New York, NY"],
        "channels": [
            {
                "name": "LinkedIn",
                "category": "Professional Network",
                "cpc": 3.50,
                "cpa": 45.00,
                "budget": 10000,
                "estimated_clicks": 2857,
                "estimated_applies": 222,
                "confidence": "High",
                "notes": "Best for tech roles",
            },
            {
                "name": "Indeed",
                "category": "Job Board",
                "cpc": 1.20,
                "cpa": 18.50,
                "budget": 8000,
                "estimated_clicks": 6667,
                "estimated_applies": 432,
                "confidence": "High",
                "notes": "High volume channel",
            },
            {
                "name": "Stack Overflow",
                "category": "Niche Board",
                "cpc": 5.00,
                "cpa": 62.00,
                "budget": 7000,
                "estimated_clicks": 1400,
                "estimated_applies": 113,
                "confidence": "Medium",
                "notes": "Developer-focused",
            },
        ],
        "benchmarks": {
            "cpc": {"industry_avg": "$2.80", "plan_value": "$3.23", "source": "BLS"},
            "cpa": {
                "industry_avg": "$35.00",
                "plan_value": "$41.83",
                "source": "Internal",
            },
        },
        "summary": {
            "total_budget": "$25,000",
            "estimated_total_clicks": "10,924",
            "estimated_total_applies": "767",
        },
    }


@pytest.fixture
def minimal_plan_data() -> Dict[str, Any]:
    """Minimal plan data to test NoneType safety."""
    return {"client_name": "Test"}


@pytest.fixture
def empty_plan_data() -> Dict[str, Any]:
    """Empty plan data to test NoneType safety."""
    return {}


# ---------------------------------------------------------------------------
# Utility function tests
# ---------------------------------------------------------------------------


class TestSafeStr:
    """Tests for _safe_str()."""

    def test_none_returns_empty(self) -> None:
        assert _safe_str(None) == ""

    def test_string_passthrough(self) -> None:
        assert _safe_str("hello") == "hello"

    def test_int_to_string(self) -> None:
        assert _safe_str(42) == "42"

    def test_float_to_string(self) -> None:
        assert _safe_str(3.14) == "3.14"

    def test_empty_string(self) -> None:
        assert _safe_str("") == ""


class TestFormatCurrency:
    """Tests for _format_currency()."""

    def test_none_returns_zero(self) -> None:
        assert _format_currency(None) == "$0"

    def test_millions(self) -> None:
        result = _format_currency(2500000)
        assert "$2.5M" in result

    def test_thousands(self) -> None:
        result = _format_currency(25000)
        assert "$25,000" in result

    def test_small_value(self) -> None:
        result = _format_currency(3.50)
        assert result == "$3.50"

    def test_string_input(self) -> None:
        # Non-numeric string returns as-is
        result = _format_currency("N/A")
        assert result == "N/A"

    def test_zero(self) -> None:
        result = _format_currency(0)
        assert "$0" in result


class TestFormatNumber:
    """Tests for _format_number()."""

    def test_none_returns_zero(self) -> None:
        assert _format_number(None) == "0"

    def test_integer(self) -> None:
        assert _format_number(2857) == "2,857"

    def test_float_integer(self) -> None:
        assert _format_number(1000.0) == "1,000"

    def test_decimal(self) -> None:
        result = _format_number(3.14)
        assert "3.14" in result


# ---------------------------------------------------------------------------
# Sheet builder tests
# ---------------------------------------------------------------------------


class TestBuildSummarySheet:
    """Tests for _build_summary_sheet()."""

    def test_has_header_row(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_summary_sheet(sample_plan_data)
        assert rows[0] == ["Field", "Value"]

    def test_client_name_present(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_summary_sheet(sample_plan_data)
        values = [row[1] for row in rows if len(row) > 1]
        assert "Acme Corp" in values

    def test_locations_joined(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_summary_sheet(sample_plan_data)
        values = [row[1] for row in rows if len(row) > 1]
        assert any("San Francisco" in v for v in values)

    def test_empty_data(self, empty_plan_data: Dict[str, Any]) -> None:
        rows = _build_summary_sheet(empty_plan_data)
        assert len(rows) >= 2  # At least header + some rows

    def test_summary_dict(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_summary_sheet(sample_plan_data)
        # Summary keys should appear
        flat = str(rows)
        assert "Total Budget" in flat or "total_budget" in flat.lower()


class TestBuildChannelsSheet:
    """Tests for _build_channels_sheet()."""

    def test_header_row(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_channels_sheet(sample_plan_data)
        assert "Channel" in rows[0]
        assert "CPC" in rows[0]

    def test_channel_count(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_channels_sheet(sample_plan_data)
        # 1 header + 3 channels
        assert len(rows) == 4

    def test_channel_names(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_channels_sheet(sample_plan_data)
        names = [row[0] for row in rows[1:]]
        assert "LinkedIn" in names
        assert "Indeed" in names

    def test_empty_channels(self, empty_plan_data: Dict[str, Any]) -> None:
        rows = _build_channels_sheet(empty_plan_data)
        assert len(rows) == 1  # Just header


class TestBuildBudgetSheet:
    """Tests for _build_budget_sheet()."""

    def test_header_row(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_budget_sheet(sample_plan_data)
        assert "Monthly Budget" in rows[0]

    def test_totals_row_present(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_budget_sheet(sample_plan_data)
        flat = [cell for row in rows for cell in row]
        assert "TOTAL" in flat

    def test_percentage_format(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_budget_sheet(sample_plan_data)
        # Second column of data rows should have percentages
        pcts = [row[2] for row in rows[1:] if len(row) > 2 and "%" in str(row[2])]
        assert len(pcts) > 0


class TestBuildBenchmarksSheet:
    """Tests for _build_benchmarks_sheet()."""

    def test_header_row(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_benchmarks_sheet(sample_plan_data)
        assert "Metric" in rows[0]

    def test_benchmarks_present(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_benchmarks_sheet(sample_plan_data)
        assert len(rows) >= 3  # header + 2 benchmark rows

    def test_empty_benchmarks(self, empty_plan_data: Dict[str, Any]) -> None:
        rows = _build_benchmarks_sheet(empty_plan_data)
        assert len(rows) >= 1  # At least header


class TestBuildTimelineSheet:
    """Tests for _build_timeline_sheet()."""

    def test_header_row(self, sample_plan_data: Dict[str, Any]) -> None:
        rows = _build_timeline_sheet(sample_plan_data)
        assert "Week" in rows[0]

    def test_default_timeline_generated(self, sample_plan_data: Dict[str, Any]) -> None:
        # No explicit timeline in sample data -> default 4-week plan
        rows = _build_timeline_sheet(sample_plan_data)
        assert len(rows) == 5  # header + 4 weeks


# ---------------------------------------------------------------------------
# CSV export tests
# ---------------------------------------------------------------------------


class TestExportToCsv:
    """Tests for export_to_csv()."""

    def test_returns_bytes(self, sample_plan_data: Dict[str, Any]) -> None:
        result = export_to_csv(sample_plan_data)
        assert isinstance(result, bytes)

    def test_utf8_bom(self, sample_plan_data: Dict[str, Any]) -> None:
        result = export_to_csv(sample_plan_data)
        assert result.startswith(b"\xef\xbb\xbf")

    def test_parseable_csv(self, sample_plan_data: Dict[str, Any]) -> None:
        result = export_to_csv(sample_plan_data)
        # Strip BOM then parse
        text = result[3:].decode("utf-8")
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        assert len(rows) > 5

    def test_client_name_in_csv(self, sample_plan_data: Dict[str, Any]) -> None:
        result = export_to_csv(sample_plan_data)
        text = result.decode("utf-8")
        assert "Acme Corp" in text

    def test_channels_in_csv(self, sample_plan_data: Dict[str, Any]) -> None:
        result = export_to_csv(sample_plan_data)
        text = result.decode("utf-8")
        assert "LinkedIn" in text
        assert "Indeed" in text

    def test_empty_data_no_crash(self, empty_plan_data: Dict[str, Any]) -> None:
        result = export_to_csv(empty_plan_data)
        assert isinstance(result, bytes)
        assert len(result) > 10

    def test_none_values_safe(self) -> None:
        """Plan data with None values should not crash."""
        data = {
            "client_name": None,
            "industry": None,
            "budget": None,
            "locations": None,
            "channels": [{"name": None, "cpc": None}],
        }
        result = export_to_csv(data)
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# XLSX export tests
# ---------------------------------------------------------------------------


class TestExportToXlsx:
    """Tests for export_to_xlsx()."""

    def test_returns_bytes_or_none(self, sample_plan_data: Dict[str, Any]) -> None:
        result = export_to_xlsx(sample_plan_data)
        # openpyxl may or may not be installed
        if result is not None:
            assert isinstance(result, bytes)
            # XLSX files start with PK zip header
            assert result[:2] == b"PK"

    def test_empty_data_no_crash(self, empty_plan_data: Dict[str, Any]) -> None:
        result = export_to_xlsx(empty_plan_data)
        if result is not None:
            assert isinstance(result, bytes)

    def test_none_values_safe(self) -> None:
        data = {
            "client_name": None,
            "channels": [{"name": None}],
        }
        result = export_to_xlsx(data)
        if result is not None:
            assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# Status tests
# ---------------------------------------------------------------------------


class TestGetStatus:
    """Tests for get_status()."""

    def test_returns_dict(self) -> None:
        result = get_status()
        assert isinstance(result, dict)

    def test_has_configured_key(self) -> None:
        result = get_status()
        assert "configured" in result

    def test_has_fallback_key(self) -> None:
        result = get_status()
        assert "fallback" in result

    def test_has_formats_key(self) -> None:
        result = get_status()
        assert "formats" in result
        assert isinstance(result["formats"], list)

    def test_csv_always_available(self) -> None:
        result = get_status()
        assert "csv" in result["formats"]

    @patch.dict("os.environ", {"GOOGLE_SHEETS_CREDENTIALS": ""})
    def test_unconfigured_no_sheets(self) -> None:
        result = get_status()
        assert result["configured"] is False
        assert "sheets" not in result["formats"]


# ---------------------------------------------------------------------------
# export_media_plan fallback tests
# ---------------------------------------------------------------------------


class TestExportMediaPlan:
    """Tests for export_media_plan()."""

    @patch.dict("os.environ", {"GOOGLE_SHEETS_CREDENTIALS": ""})
    def test_returns_none_when_not_configured(
        self, sample_plan_data: Dict[str, Any]
    ) -> None:
        result = export_media_plan(sample_plan_data)
        assert result is None

    @patch.dict("os.environ", {"GOOGLE_SHEETS_CREDENTIALS": ""})
    def test_empty_data_returns_none(self, empty_plan_data: Dict[str, Any]) -> None:
        result = export_media_plan(empty_plan_data)
        assert result is None


# ---------------------------------------------------------------------------
# NoneType safety sweep
# ---------------------------------------------------------------------------


class TestNoneTypeSafety:
    """Ensure no function crashes on None or missing values."""

    _NONE_DATA: Dict[str, Any] = {
        "client_name": None,
        "industry": None,
        "job_title": None,
        "budget": None,
        "locations": None,
        "channels": None,
        "benchmarks": None,
        "summary": None,
        "timeline": None,
    }

    def test_summary_sheet_none_safe(self) -> None:
        rows = _build_summary_sheet(self._NONE_DATA)
        assert len(rows) >= 2

    def test_channels_sheet_none_safe(self) -> None:
        rows = _build_channels_sheet(self._NONE_DATA)
        assert len(rows) >= 1

    def test_budget_sheet_none_safe(self) -> None:
        rows = _build_budget_sheet(self._NONE_DATA)
        assert len(rows) >= 1

    def test_benchmarks_sheet_none_safe(self) -> None:
        rows = _build_benchmarks_sheet(self._NONE_DATA)
        assert len(rows) >= 1

    def test_timeline_sheet_none_safe(self) -> None:
        rows = _build_timeline_sheet(self._NONE_DATA)
        assert len(rows) >= 1

    def test_csv_none_safe(self) -> None:
        result = export_to_csv(self._NONE_DATA)
        assert isinstance(result, bytes)

    def test_xlsx_none_safe(self) -> None:
        result = export_to_xlsx(self._NONE_DATA)
        # May be None if openpyxl not installed -- that's fine
        if result is not None:
            assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# Credential loading tests
# ---------------------------------------------------------------------------


class TestCredentialLoading:
    """Tests for _load_credentials() edge cases."""

    @patch.dict("os.environ", {"GOOGLE_SHEETS_CREDENTIALS": ""})
    def test_empty_env_returns_none(self) -> None:
        from sheets_export import _load_credentials

        assert _load_credentials() is None

    @patch.dict("os.environ", {"GOOGLE_SHEETS_CREDENTIALS": "/nonexistent/path.json"})
    def test_missing_file_returns_none(self) -> None:
        from sheets_export import _load_credentials

        assert _load_credentials() is None

    @patch.dict("os.environ", {}, clear=False)
    def test_unset_env_returns_none(self) -> None:
        import os

        os.environ.pop("GOOGLE_SHEETS_CREDENTIALS", None)
        from sheets_export import _load_credentials

        assert _load_credentials() is None
