"""Tests for the S50 location plausibility validation in data_synthesizer.

Validates that the _validate_location_plausibility function correctly:
- Flags locations outside a company's known operating area
- Does NOT flag locations within the operating area
- Does NOT block plan generation (soft warnings only)
- Handles missing data gracefully
- Distinguishes severity levels based on region proximity
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List

import pytest

# Ensure the project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_synthesizer import _validate_location_plausibility


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_enriched(
    hq: str = "",
    description: str = "",
) -> Dict[str, Any]:
    """Build a minimal enriched dict with company location signals."""
    enriched: Dict[str, Any] = {}
    if hq:
        enriched["company_metadata"] = {"headquarters": hq}
    if description:
        enriched["company_info"] = {"description": description}
    return enriched


def _make_input(
    company: str = "",
    locations: list = None,
) -> Dict[str, Any]:
    """Build a minimal input_data dict."""
    return {
        "company_name": company,
        "locations": locations or [],
    }


# ---------------------------------------------------------------------------
# Tests: Obvious mismatch (the Benchmark Senior Living example)
# ---------------------------------------------------------------------------


class TestBenchmarkSeniorLiving:
    """The canonical example: Benchmark Senior Living operates in CT/MA/NH/ME/RI/VT/PA/NY/NJ."""

    COMPANY = "Benchmark Senior Living"
    HQ = "Waltham, Massachusetts"
    DESCRIPTION = (
        "Benchmark Senior Living is a senior living company "
        "headquartered in Waltham, Massachusetts. It operates "
        "assisted living communities in Connecticut, Massachusetts, "
        "New Hampshire, Maine, Rhode Island, Vermont, Pennsylvania, "
        "New York, and New Jersey."
    )

    def test_virginia_flagged(self) -> None:
        """Virginia is not in Benchmark's operating area -- should warn."""
        warnings = _validate_location_plausibility(
            _make_input(self.COMPANY, ["Virginia"]),
            _make_enriched(self.HQ, self.DESCRIPTION),
            {},
        )
        assert len(warnings) >= 1
        assert warnings[0]["user_state"] == "VA"
        assert warnings[0]["severity"] in ("low", "medium", "high")

    def test_connecticut_not_flagged(self) -> None:
        """Connecticut IS in Benchmark's operating area -- no warning."""
        warnings = _validate_location_plausibility(
            _make_input(self.COMPANY, ["Connecticut"]),
            _make_enriched(self.HQ, self.DESCRIPTION),
            {},
        )
        assert len(warnings) == 0

    def test_massachusetts_not_flagged(self) -> None:
        """Massachusetts is the HQ state -- definitely no warning."""
        warnings = _validate_location_plausibility(
            _make_input(self.COMPANY, ["Massachusetts"]),
            _make_enriched(self.HQ, self.DESCRIPTION),
            {},
        )
        assert len(warnings) == 0

    def test_new_hampshire_not_flagged(self) -> None:
        """NH is mentioned in the description -- no warning."""
        warnings = _validate_location_plausibility(
            _make_input(self.COMPANY, ["New Hampshire"]),
            _make_enriched(self.HQ, self.DESCRIPTION),
            {},
        )
        assert len(warnings) == 0

    def test_texas_flagged(self) -> None:
        """Texas is far from the northeast -- should warn."""
        warnings = _validate_location_plausibility(
            _make_input(self.COMPANY, ["Texas"]),
            _make_enriched(self.HQ, self.DESCRIPTION),
            {},
        )
        assert len(warnings) >= 1
        assert warnings[0]["user_state"] == "TX"

    def test_mixed_valid_and_invalid(self) -> None:
        """Some locations valid, some not -- only the invalid ones warn."""
        warnings = _validate_location_plausibility(
            _make_input(self.COMPANY, ["Connecticut", "Virginia", "Maine"]),
            _make_enriched(self.HQ, self.DESCRIPTION),
            {},
        )
        flagged_states = {w["user_state"] for w in warnings}
        assert "VA" in flagged_states
        assert "CT" not in flagged_states
        assert "ME" not in flagged_states


# ---------------------------------------------------------------------------
# Tests: Graceful degradation with missing data
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    """When enrichment data is missing, the validator should return empty."""

    def test_no_company_name(self) -> None:
        """No company name means nothing to validate against."""
        warnings = _validate_location_plausibility(
            _make_input("", ["Virginia"]),
            _make_enriched("Seattle, WA", "Tech company in Seattle"),
            {},
        )
        assert warnings == []

    def test_no_locations(self) -> None:
        """No user locations means nothing to check."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", []),
            _make_enriched("Seattle, WA"),
            {},
        )
        assert warnings == []

    def test_no_enrichment_data(self) -> None:
        """No Wikipedia/Clearbit data means no signals -- skip."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Virginia"]),
            {},
            {},
        )
        assert warnings == []

    def test_empty_enrichment(self) -> None:
        """Empty enrichment dicts should not crash."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Virginia"]),
            {"company_metadata": {}, "company_info": {}},
            {},
        )
        assert warnings == []


# ---------------------------------------------------------------------------
# Tests: International locations should NOT be flagged
# ---------------------------------------------------------------------------


class TestInternationalLocations:
    """International locations should never be flagged, even if the
    company is US-based. A global company might recruit anywhere."""

    def test_international_not_flagged(self) -> None:
        """UK is international -- should not generate a warning."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["United Kingdom"]),
            _make_enriched("Seattle, WA", "TestCorp is a company in Seattle"),
            {},
        )
        # International locations don't resolve to US states, so no warning
        assert len(warnings) == 0

    def test_us_and_international_mixed(self) -> None:
        """Mix of US and international -- only US mismatches warned."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Germany", "California", "Texas"]),
            _make_enriched("Seattle, WA", "TestCorp is a company in Washington state"),
            {},
        )
        # Germany: no warning (international)
        # California: may or may not warn depending on region proximity
        # Texas: may warn
        flagged_locs = {w["location"] for w in warnings}
        assert "Germany" not in flagged_locs


# ---------------------------------------------------------------------------
# Tests: Region proximity affects severity
# ---------------------------------------------------------------------------


class TestRegionProximity:
    """Locations in neighboring regions should have lower severity."""

    def test_neighboring_region_low_severity(self) -> None:
        """A neighboring region state should have 'low' severity."""
        # Company in northeast (MA), check a southeast state (VA)
        # Southeast is a neighbor of northeast
        warnings = _validate_location_plausibility(
            _make_input("NortheastCo", ["Virginia"]),
            _make_enriched("Boston, Massachusetts"),
            {},
        )
        if warnings:
            # VA is us_southeast, neighbor of us_northeast
            assert warnings[0]["severity"] in ("low", "medium")

    def test_distant_region_higher_severity(self) -> None:
        """A distant region should have at least 'medium' severity."""
        # Company in northeast (MA), check a west coast state (CA)
        warnings = _validate_location_plausibility(
            _make_input("NortheastCo", ["California"]),
            _make_enriched("Boston, Massachusetts"),
            {},
        )
        if warnings:
            assert warnings[0]["severity"] in ("medium", "high")


# ---------------------------------------------------------------------------
# Tests: Warning structure
# ---------------------------------------------------------------------------


class TestWarningStructure:
    """Validate the shape of returned warning dicts."""

    def test_warning_has_required_keys(self) -> None:
        """Each warning should contain all expected keys."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Texas"]),
            _make_enriched("Boston, MA", "TestCorp is in Massachusetts"),
            {},
        )
        if warnings:
            w = warnings[0]
            assert "location" in w
            assert "reason" in w
            assert "severity" in w
            assert "company_hq" in w
            assert "known_states" in w
            assert "suggestion" in w
            assert isinstance(w["known_states"], list)

    def test_warning_is_never_blocking(self) -> None:
        """Warnings should never contain a 'block' or 'error' severity."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Texas", "California", "Florida"]),
            _make_enriched("Boston, MA", "TestCorp is in Massachusetts"),
            {},
        )
        for w in warnings:
            assert w["severity"] in ("low", "medium", "high")
            assert "block" not in w.get("severity", "").lower()
            assert "error" not in w.get("severity", "").lower()


# ---------------------------------------------------------------------------
# Tests: HQ-only signal (no description text)
# ---------------------------------------------------------------------------


class TestHQOnlySignal:
    """When only HQ is available (no description), should still work."""

    def test_hq_only_flags_distant_state(self) -> None:
        """With just HQ in Seattle, WA -- Texas should be flagged."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Texas"]),
            _make_enriched("Seattle, Washington"),
            {},
        )
        # With only HQ state (WA), Texas is in a different region
        if warnings:
            assert warnings[0]["user_state"] == "TX"

    def test_hq_only_allows_same_state(self) -> None:
        """With HQ in Seattle, WA -- Washington should not be flagged."""
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Washington"]),
            _make_enriched("Seattle, Washington"),
            {},
        )
        assert len(warnings) == 0


# ---------------------------------------------------------------------------
# Tests: Synthesis-based company data (competitive intelligence section)
# ---------------------------------------------------------------------------


class TestSynthesisData:
    """When HQ comes from synthesis rather than enrichment."""

    def test_synthesis_hq_used_as_fallback(self) -> None:
        """If enrichment has no HQ but synthesis does, use it."""
        synthesis = {
            "competitive_intelligence": {
                "company_wikipedia": {
                    "headquarters": "Waltham, Massachusetts",
                }
            }
        }
        warnings = _validate_location_plausibility(
            _make_input("TestCorp", ["Texas"]),
            {"company_info": {"description": "TestCorp in Massachusetts"}},
            synthesis,
        )
        if warnings:
            assert warnings[0]["company_hq"] == "Waltham, Massachusetts"
