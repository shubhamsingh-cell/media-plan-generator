"""The footprint plausibility check reads locations through geo_market.

RTI audit (2026-10-11): "Washington (US West Coast) appears outside RTI
International's known operating region" -- the check read bare "Washington" as
Washington State while every location table read it as Washington, DC. It also
warned about Remote / Nationwide and about states the client itself entered.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import data_synthesizer  # noqa: E402
import geo_audit_helpers as h  # noqa: E402

ENRICHED = {
    "company_metadata": {"headquarters": "Research Triangle Park, North Carolina"},
    "company_info": {"description": "RTI is a nonprofit research institute based in North Carolina."},
}


def _warnings(raw_locations):
    locations, sidecar = h.canonical_plan_locations(raw_locations)
    data = {
        "client_name": "RTI International",
        "locations": locations,
        "_location_resolution": sidecar,
    }
    return data_synthesizer._validate_location_plausibility(data, ENRICHED, {})


def test_bare_washington_is_dc_not_the_west_coast_state():
    warns = [w for w in _warnings(["Washington"]) if w["location"] == "Washington"]
    for w in warns:
        assert "West Coast" not in w["reason"]
        assert w["user_state"] == "DC"
        assert "Washington, DC" in w["reason"]  # the assumption is stated


def test_bare_washington_defers_to_a_footprint_that_includes_washington_state():
    locations, sidecar = h.canonical_plan_locations(["Washington"])
    data = {"client_name": "TestCorp", "locations": locations, "_location_resolution": sidecar}
    enriched = {"company_metadata": {"headquarters": "Seattle, Washington"}}
    assert data_synthesizer._validate_location_plausibility(data, enriched, {}) == []


def test_explicit_wa_stays_the_state():
    warns = [w for w in _warnings(["WA"]) if w["location"] == "Washington"]
    assert [w["user_state"] for w in warns] == ["WA"]
    assert "Assumed" not in warns[0]["reason"]


def test_remote_and_nationwide_never_warn():
    assert _warnings(["Remote", "Nationwide"]) == []


def test_a_city_outside_the_footprint_still_warns():
    warns = _warnings(["Seattle, WA"])
    assert [w["location"] for w in warns] == ["Seattle, WA"]
