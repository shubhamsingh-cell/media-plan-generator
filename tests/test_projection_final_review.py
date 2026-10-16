"""Final review of the projection work: headline parity for generated bundles,
labels never stripped, roi_projector honouring suppression, one currency for
the chat tools, truthful scenario labels, ranges (not points) in the narrative,
and no unlabelled US CPA band on a non-US plan."""

from __future__ import annotations

import io
import logging
import os
import re
import sys
import types

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
for _n in ("anthropic", "openai", "supabase", "redis", "qdrant_client", "sentence_transformers", "posthog"):
    sys.modules.setdefault(_n, types.ModuleType(_n))
logging.disable(logging.CRITICAL)

import plan_projection  # noqa: E402

NOTE = plan_projection.NO_LOCAL_HIRES_NOTE
DASH = "–"
_M = ("applications", "hires", "cpa", "cph")
US_SHAPES = [
    ("Warehouse Associate", "Dallas, TX", "logistics_supply_chain"),
    ("Registered Nurse", "Columbus, OH", "healthcare_medical"),
    ("Software Engineer", "Remote, US", "tech_engineering"),
    ("Customer Service Representative", "United States", "general_entry_level"),
]


def _band(p):
    return {m: (p[m]["low"], p[m]["high"]) for m in _M}


def _brief(role, loc, ind, budget="$60,000"):
    return {"client_name": "Acme", "industry": ind, "budget": budget, "campaign_duration": "3 months",
            "hire_volume": "Not specified", "work_environment": "onsite", "locations": [loc],
            "roles": [role], "target_roles": [role]}


def _bundle(brief):
    import excel_v2
    import ppt_generator
    import tools_regen_bundles as T
    from kb_loader import load_knowledge_base
    from openpyxl import load_workbook
    from pptx import Presentation

    data = T.build_plan_data(brief)
    raw = excel_v2.generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    raw = raw[0] if isinstance(raw, tuple) else raw
    wb = load_workbook(io.BytesIO(raw))
    prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(data))))
    return data, wb, prs


def _deck(prs):
    for i, sl in enumerate(prs.slides, 1):
        for sh in sl.shapes:
            if sh.has_text_frame:
                for p in sh.text_frame.paragraphs:
                    yield i, p.text
            if getattr(sh, "has_table", False) and sh.has_table:
                for r in sh.table.rows:
                    for c in r.cells:
                        yield i, c.text


def _cells(wb):
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for c in row:
                if c.value is not None:
                    yield ws.title, c.coordinate, c.value


# 1 ------------------------------------------------------------------------
@pytest.mark.parametrize("role,loc,ind", US_SHAPES)
def test_generated_bundle_headline_equals_the_estimate(role, loc, ind):
    import app
    import ppt_generator

    est = app._compute_plan_estimate(
        {"budget": "$60,000", "industry": ind, "roles": [role], "locations": [loc],
         "campaign_duration": "3 months", "client_name": "Acme"}
    )["projection"]
    data, _wb, _prs = _bundle(_brief(role, loc, ind))
    head = data["_budget_allocation"].get("headline_projection")
    assert head, "the bundle carries no headline projection"
    assert _band(head) == _band(est)
    assert _band(ppt_generator._deck_projection(data)) == _band(est)


# 2 ------------------------------------------------------------------------
def test_strip_never_removes_labels():
    s = plan_projection.strip_hire_claims
    for label in ("Projected Hires", "Total Proj. Hires", "Proj. Hires", "Cost / Hire", "Hires"):
        assert s(label) == label, label


def test_london_labels_survive_with_n_a_values():
    _data, wb, prs = _bundle(_brief("Warehouse Associate", "London, UK", "logistics_supply_chain", "£60,000"))
    deck = list(_deck(prs))
    assert any(t.strip() == "Projected Hires" for i, t in deck if i == 2)
    assert any(t.strip() == "n/a" for i, t in deck if i == 2)
    assert any(t.strip() == "Projected Hires" for i, t in deck if i == 6)
    cells = list(_cells(wb))
    assert any(s == "Executive Summary" and v == "Projected Hires" for s, _c, v in cells)
    assert any(s == "ROI Projections" and v == "Total Proj. Hires" for s, _c, v in cells)
    assert any(NOTE in str(v) for _s, _c, v in cells)


# 3 ------------------------------------------------------------------------
def test_roi_projector_projects_no_hires_for_london_and_keeps_them_for_the_us():
    import roi_projector

    ldn = roi_projector.project_roi("Indeed", 60000, "logistics", "Warehouse Associate", ["London, UK"])
    for k in ("pessimistic", "expected", "optimistic"):
        assert ldn["projections"][k]["hires"] is None, ldn["projections"]
    assert ldn["roi_metrics"]["cost_per_hire"] == {"low": None, "expected": None, "high": None}
    assert NOTE in ldn["recommendation"] and not re.search(r"\d\s+hires?", ldn["recommendation"])
    assert "channel_detail" not in ldn and ldn["hires_note"] == NOTE
    us = roi_projector.project_roi("Indeed", 60000, "healthcare", "Registered Nurse", ["Columbus, OH"])
    assert us["projections"]["expected"]["hires"]


# 4 ------------------------------------------------------------------------
def test_chat_tools_price_a_dollar_budget_in_dollars_in_india():
    import roi_projector
    from nova import Nova

    n = Nova.__new__(Nova)
    n._data_cache = {}
    res = n._query_budget_projection(
        {"budget": 60000, "roles": ["Software Engineer"], "locations": ["Bengaluru, India"], "industry": "tech_engineering"}
    )
    assert res["projection"]["currency"]["code"] == "USD"
    assert res["projection"]["text"]["cpa"].startswith("$")
    assert res["projection"]["applications"]["high"] > 1000  # not INR-priced (was 58)
    roi = roi_projector.project_roi("Indeed", 60000, "technology", "Software Engineer", ["Bengaluru, India"])
    assert roi["headline_projection"]["currency"]["code"] == "USD"


# 5 ------------------------------------------------------------------------
def test_slide6_scenario_label_is_truthful():
    for role, loc, ind in US_SHAPES[2:]:
        _d, _w, prs = _bundle(_brief(role, loc, ind))
        s6 = " || ".join(t for i, t in _deck(prs) if i == 6)
        assert "upper scenario" not in s6, s6
        assert "engine scenario" in s6


# 6 ------------------------------------------------------------------------
def test_narrative_tiles_and_risk_quote_ranges_not_the_upper_point():
    data, wb, prs = _bundle(_brief("Registered Nurse", "Columbus, OH", "healthcare_medical"))
    cells = list(_cells(wb))
    flat = [str(v) for _s, _c, v in cells]
    assert not any("projected to deliver 11 hire" in v for v in flat)
    assert any("projected to deliver 5" + DASH + "11 hires" in v for v in flat), [v for v in flat if "deliver" in v]
    ws_vals = [v for s, _c, v in cells if s == "Executive Summary"]
    assert "5" + DASH + "11" in [str(v) for v in ws_vals], "hero Projected Hires tile is not a range"
    assert any(str(v).startswith("$5,250") and DASH in str(v) for v in ws_vals), "hero Cost/Hire tile"
    risk = " || ".join(t for i, t in _deck(prs) if i == 9)
    assert "drop from 11 to" not in risk and "5" + DASH + "11" in risk, risk
    assert not any("drop from 11 to" in v for v in flat)


# 7 ------------------------------------------------------------------------
def test_us_cpa_band_is_not_applied_to_a_non_us_dollar_plan():
    p = plan_projection.project_plan(
        {"budget": 60000, "roles": ["Software Engineer"], "locations": ["Bengaluru, India"],
         "industry": "tech_engineering", "budget_text": "$60,000"}
    )
    assert p["cpa"]["low"] == p["cpa"]["high"]  # no US band widening
    assert "US" in " ".join(p["notes"]) and "single estimate" in " ".join(p["notes"])
    us = plan_projection.project_plan(
        {"budget": 60000, "roles": ["Software Engineer"], "locations": ["Remote, US"],
         "industry": "tech_engineering", "budget_text": "$60,000"}
    )
    assert us["cpa"]["low"] < us["cpa"]["high"]
