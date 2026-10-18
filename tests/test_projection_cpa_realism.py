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


# Healthcare used to be the fixture for "the engine sits below the KB band"
# (RN engine CPA ~$14 vs KB $35-$58). The healthcare apply-rate re-level
# (Appcast 2026 healthcare apply rate 3.22%; CPA-realism C1, 2026-10-18) lifts
# the RN engine CPA to ~$35, inside the band, so the below-the-band guards run
# on truck driving (engine ~$6.5 vs the logistics KB industry band $12-$35) and
# the healthcare case is pinned separately below.
def test_below_band_cpa_range_high_reaches_the_industry_benchmark():
    p = _project("Truck Driver", "Dallas, TX", industry="logistics_supply_chain")
    assert p["cpa"]["high"] >= 35
    # the engine figure is untouched: CPA low stays what the engine computed
    assert p["cpa"]["low"] == pytest.approx(p["engine"]["cost_per_application"], abs=0.01)
    note = " ".join(p["notes"])
    assert "Industry benchmarks run higher" in note
    assert "logistics_supply_chain.cpa.range" in note  # cites file + key
    assert "recruitment_benchmarks_deep.json" in note


def test_rn_engine_cpa_sits_inside_the_healthcare_kb_band_after_the_relevel():
    p = _project("Registered Nurse", "Columbus, OH")
    eng = p["engine"]["cost_per_application"]
    assert 35 <= eng <= 58, eng  # was ~$14 (8% generic job-board apply rate)
    assert (p["cpa"]["low"], p["cpa"]["high"]) == (35.0, 58.0)
    note = " ".join(p["notes"])
    assert "sits inside it" in note and "Industry benchmarks run higher" not in note
    assert "healthcare_medical.cpa.range" in note  # still cites file + key
    # the hull property: applications low/high are budget over the CPA high/low
    assert p["applications"]["low"] == pytest.approx(60_000 / 58, abs=1)
    assert p["applications"]["high"] == pytest.approx(60_000 / 35, abs=1)


def test_warehouse_uses_the_sub_role_row_not_the_industry_row():
    p = _project("Warehouse Associate", "Dallas, TX")
    assert p["cpa"]["high"] >= 12
    assert "warehouse_hourly" in " ".join(p["notes"])


def test_applications_range_is_budget_over_the_cpa_range():
    p = _project("Truck Driver", "Dallas, TX", industry="logistics_supply_chain")
    assert p["applications"]["low"] == pytest.approx(60_000 / p["cpa"]["high"], abs=1)
    assert p["applications"]["high"] == p["engine"]["applications"]


def test_engine_cpa_is_never_clamped_up():
    p = _project("Truck Driver", "Dallas, TX", industry="logistics_supply_chain")
    assert p["cpa"]["low"] < 12  # engine figure stays below the KB benchmark's low
    # healthcare: the re-levelled engine figure is reported as computed, not floored to the KB
    rn = _project("Registered Nurse", "Columbus, OH")
    eng = rn["engine"]
    assert eng["cost_per_application"] == pytest.approx(60_000 / eng["applications"], abs=0.01)


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
