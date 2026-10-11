"""R3 (review 2026-10-11): bare "New York" is assumed NYC and flagged; "NY" is the state.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import gold_standard  # noqa: E402
import plan_location  # noqa: E402
import research  # noqa: E402


def _res(loc):
    return plan_location.resolve_locations([loc])[0].to_dict()


# ── R3: bare "New York" is assumed NYC, flagged; "NY" stays the state ───────
def test_bare_new_york_is_assumed_nyc_with_a_note():
    wb = h.workbook(h.build_plan(["New York", "NY"], industry="healthcare_medical"))
    # "NY" and the bare name both canonicalise to the display "New York"; the
    # resolver verdict (input "New York" vs "NY") tells them apart
    res_bare = _res("New York")
    info = research.get_location_info("New York", res_bare)
    assert info["geo_level"] == "metro"
    assert info["assumed_location"].startswith("New York, NY")
    assert "New York State" in info["ambiguity_note"]
    info_ny = research.get_location_info("New York", _res("NY"))
    assert info_ny["geo_level"] == "state" and info_ny["metro_name"] == "New York (statewide)"
    assert wb is not None


def test_bare_new_york_salary_path_stays_nyc():
    bare = gold_standard.enrich_city_level_data(
        {"locations": ["New York"], "_location_resolution": [_res("New York")], "target_roles": ["RN"]}
    )["New York"]
    state = gold_standard.enrich_city_level_data(
        {"locations": ["New York"], "_location_resolution": [_res("NY")], "target_roles": ["RN"]}
    )["New York"]
    assert bare["geo_basis"] == "city"
    assert state["geo_basis"] == "state"
    assert bare["salary_multiplier"] != state["salary_multiplier"]


def test_workbook_notes_the_new_york_assumption():
    data = h.build_plan(["New York"])
    wb = h.workbook(data)
    row = h.context_table(wb)["New York"]
    assert "Assumed New York, NY" in row["Context"]


