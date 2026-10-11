"""G7: a row's label names the figure that is actually state-level.

RTI audit (2026-10-11): "Pittsburgh [PA state-level estimate]" at 0.94x while PA
is 1.02x -- the label borrowed the DIFFICULTY's state basis (the less specific of
the two) for the city-level SALARY multiplier.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import excel_v2  # noqa: E402
import geo_audit_helpers as h  # noqa: E402
import gold_standard  # noqa: E402


def _rows(locations):
    return gold_standard.enrich_city_level_data({"locations": locations, "target_roles": ["Cook"]})


def test_salary_and_difficulty_bases_are_recorded_separately():
    pitt = _rows(["Pittsburgh, PA"])["Pittsburgh"]
    assert pitt["salary_basis"] == "city"  # 0.94x is Pittsburgh's own multiplier
    assert pitt["difficulty_basis"] == "state"  # 5.5 is Pennsylvania's
    assert pitt["geo_basis"] == "state"  # combined = the less specific (unchanged)
    hershey = _rows(["Hershey, PA"])["Hershey"]
    assert (hershey["salary_basis"], hershey["difficulty_basis"]) == ("state", "state")


def test_label_names_only_the_figure_that_is_actually_state_level():
    rows = _rows(["Pittsburgh, PA", "Hershey, PA", "Philadelphia, PA"])
    assert excel_v2._geo_basis_suffix(rows["Pittsburgh"]) == " [PA state-level difficulty estimate]"
    assert excel_v2._geo_basis_suffix(rows["Hershey"]) == " [PA state-level estimate]"
    assert excel_v2._geo_basis_suffix(rows["Philadelphia"]) == ""


def test_workbook_does_not_call_pittsburghs_city_salary_a_state_estimate():
    wb = h.workbook(h.build_plan(["Pittsburgh, PA", "Hershey, PA"]))
    text = h.sheet_text(wb["Quality Intelligence"])
    assert any(t.startswith("Pittsburgh [PA state-level difficulty estimate]") for t in text)
    assert not any(t.startswith("Pittsburgh [PA state-level estimate]") for t in text)
    assert any(t.startswith("Hershey [PA state-level estimate]") for t in text)
