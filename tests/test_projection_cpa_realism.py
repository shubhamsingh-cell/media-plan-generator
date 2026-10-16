"""CPA realism: the engine has no CPA floor, so its CPA ($6-$18) can sit far
below the repo's own industry benchmarks. Decision (owner standing directive,
2026-10-16): do NOT clamp the engine -- widen the HIGH end of the CPA range to
the knowledge base's low end and say so, citing file + key.

KB (data/recruitment_benchmarks_deep.json industry_benchmarks.<ind>.cpa):
healthcare_medical range "$35-$58+"; logistics_supply_chain.cpa.warehouse_hourly
"$12-$18".
"""

from __future__ import annotations

import logging
import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
logging.disable(logging.CRITICAL)

import plan_projection  # noqa: E402


def _project(role, loc, budget=60_000, industry=""):
    return plan_projection.project_plan(
        {"budget": budget, "roles": [role], "locations": [loc], "industry": industry}
    )


def test_rn_cpa_range_high_reaches_the_healthcare_benchmark():
    p = _project("Registered Nurse", "Columbus, OH")
    assert p["cpa"]["high"] >= 35
    # the engine figure is untouched: CPA low stays what the engine computed
    assert p["cpa"]["low"] == pytest.approx(p["engine"]["cost_per_application"], abs=0.01)
    note = " ".join(p["notes"])
    assert "Industry benchmarks run higher" in note
    assert "healthcare_medical.cpa.range" in note  # cites file + key
    assert "recruitment_benchmarks_deep.json" in note


def test_warehouse_uses_the_sub_role_row_not_the_industry_row():
    p = _project("Warehouse Associate", "Dallas, TX")
    assert p["cpa"]["high"] >= 12
    assert "warehouse_hourly" in " ".join(p["notes"])


def test_applications_range_is_budget_over_the_cpa_range():
    p = _project("Registered Nurse", "Columbus, OH")
    assert p["applications"]["low"] == pytest.approx(60_000 / p["cpa"]["high"], abs=1)
    assert p["applications"]["high"] == p["engine"]["applications"]


def test_engine_cpa_is_never_clamped_up():
    p = _project("Registered Nurse", "Columbus, OH")
    assert p["cpa"]["low"] < 35  # engine figure stays below the KB benchmark


def test_sub_role_match_needs_every_content_word():
    kb = plan_projection.kb_cpa_range("logistics_supply_chain", ["Warehouse Associate"])
    assert kb and kb["scope"] == "sub_role" and kb["low"] == 12 and kb["high"] == 18
    # "Supply Chain Analyst" matches supply_chain_management? only if BOTH
    # 'supply' and 'chain' are in the title
    kb2 = plan_projection.kb_cpa_range("logistics_supply_chain", ["Supply Analyst"])
    assert kb2 and kb2["scope"] == "industry"
    kb3 = plan_projection.kb_cpa_range("logistics_supply_chain", ["Supply Chain Analyst"])
    assert kb3 and kb3["scope"] == "sub_role" and kb3["low"] == 30


def test_engine_above_the_kb_range_flips_the_note_not_the_engine():
    lo, hi, note = plan_projection._cpa_range(
        80.0, {"low": 35.0, "high": 58.0, "scope": "industry", "source": "x"}
    )
    assert (lo, hi) == (35.0, 80.0) and "run lower" in note


def test_workbook_roi_sheet_has_no_unsourced_cpa_floor_table():
    src = open(os.path.join(PROJECT_ROOT, "excel_v2.py"), encoding="utf-8").read()
    assert "_CPA_FLOORS" not in src


def test_workbook_fallback_channel_cpa_reads_the_kb():
    import excel_v2

    alloc = {"metadata": {"industry": "healthcare_medical"}}
    assert excel_v2._fallback_channel_cpa({"role": "Registered Nurse"}, alloc) == 35.0
    alloc = {"metadata": {"industry": "logistics_supply_chain"}}
    assert excel_v2._fallback_channel_cpa({"role": "Warehouse Associate"}, alloc) == 12.0
