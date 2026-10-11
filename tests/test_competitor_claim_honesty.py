"""Plan-output audit (2026-10-11) -- competitor / claim honesty (C1-C6).

``hiring_intensity`` in the gold-standard competitor map is a static
hiring-difficulty proxy (gold_standard.py), not an observed count of
employers, so no surface may phrase it as "Fortune 500+ companies are
actively hiring" or "high hiring volume".
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402

import excel_v2  # noqa: E402


def _sheet_text(ws) -> str:
    return "\n".join(
        str(c.value) for row in ws.iter_rows() for c in row if c.value not in (None, "")
    )


def _data_with_high_intensity() -> dict:
    return {
        "client_name": "Hershey Co",
        "industry": "food_beverage",
        "industry_label": "Food & Beverage",
        "locations": ["Hershey, PA", "Chicago, IL"],
        "roles": ["Production Operator"],
        "target_roles": ["Production Operator"],
        "budget": "$200,000",
        "competitors": [],
        "_synthesized": {},
        "_enriched": {},
        "_budget_allocation": {},
        "_gold_standard": {
            "competitor_mapping": {
                "Hershey": {
                    "top_employers": ["Acme Foods"],
                    "hiring_intensity": "high",
                },
                "Chicago": {
                    "top_employers": ["Acme Foods"],
                    "hiring_intensity": "very_high",
                },
                "Dallas": {"top_employers": [], "hiring_intensity": "moderate"},
            }
        },
    }


# -- C1 ---------------------------------------------------------------------


def _risk_texts_excel(data: dict) -> str:
    wb = openpyxl.Workbook()
    ws = wb.active
    excel_v2._build_sheet_executive_summary(ws, data)
    return _sheet_text(ws)


def test_c1_excel_risk_row_does_not_claim_fortune_500_hiring():
    text = _risk_texts_excel(_data_with_high_intensity())
    assert "Competitive Pressure" in text, "risk row did not render (vacuous test)"
    assert "Fortune 500" not in text
    assert "actively hiring" not in text
    assert "2 markets score high hiring difficulty" in text


def test_c1_deck_risk_slide_does_not_claim_fortune_500_hiring():
    from pptx import Presentation

    import ppt_generator

    prs = Presentation()
    ppt_generator._build_slide_risk_analysis(prs, _data_with_high_intensity())
    texts = []
    for slide in prs.slides:
        for sh in slide.shapes:
            if sh.has_text_frame:
                texts.append(sh.text_frame.text)
            if getattr(sh, "has_table", False) and sh.has_table:
                for r in sh.table.rows:
                    texts.extend(c.text for c in r.cells)
    blob = "\n".join(texts)
    assert "COMPETITION" in blob or "High Competition" in blob, "risk row missing"
    assert "Fortune 500" not in blob
    assert "2 markets score high on modeled hiring difficulty" in blob


# -- C2 ---------------------------------------------------------------------


def _quality_intel_text(data: dict) -> str:
    wb = openpyxl.Workbook()
    ws = wb.active
    excel_v2._build_sheet_quality_intelligence(ws, data, data["_gold_standard"])
    return _sheet_text(ws)


def test_c2_quality_intelligence_says_hiring_difficulty_not_hiring_volume():
    for brief in ([], ["Acme Foods"]):
        data = _data_with_high_intensity()
        data["competitors"] = brief
        text = _quality_intel_text(data)
        assert "Why They Matter" in text, "competitive table missing (vacuous)"
        assert "High hiring volume" not in text
        assert "Hiring Intensity" not in text
        assert "Hiring Difficulty" in text
        assert "Hard-to-fill market in Hershey (hiring-difficulty score)" in text
