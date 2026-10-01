"""Competitor claims need evidence -- and the client is not a competitor.

Prod (all 4 Hershey runs, 2026-09-24): bundle_qa raised a critical
``unsourced_competitor_claim`` on the LLM executive summary (Executive
Summary!B58) after every competitor search tier failed (Tavily 432, Jina
401, DDG timeouts). Two shapes, both verbatim from the Render log:
  * "...this risk is compounded by named competitors (Nestle Purina,
    Campbell's, Land O'Lakes, Treehouse Foods) drawing from the same..."
    -- model knowledge asserted as fact about client-typed competitors;
  * "...labor market where Hershey is drawing from the same electrical,
    HVAC, and industrial maintenance talent" -- the CLIENT itself, which the
    gate misreported as competitor 'Hershey' (and, one finding per cell,
    that false positive masked the real competitor claim).
Slide 7 cards also printed template claims ("X is a major employer in this
industry and a likely competitor...") for competitors with no evidence
record at all.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import competitor_claims  # noqa: E402
import excel_v2  # noqa: E402

CLIENT_SENTENCE = (
    "Against a competitive skilled-trades labor market where Hershey is "
    "drawing from the same electrical, HVAC, and industrial maintenance "
    "talent as other regional manufacturers, this plan concentrates spend "
    "on programmatic and job-board channels near each plant."
)
COMPETITOR_SENTENCE = (
    "The key risk to monitor is qualified-to-interview conversion, and this "
    "risk is compounded by named competitors Nestle Purina, Campbell's, Land "
    "O'Lakes, Treehouse Foods drawing from the same maintenance and "
    "sanitation talent pools."
)
TYPED = ["Nestle Purina", "Campbell's", "Land O'Lakes", "Treehouse Foods"]


def _narrative_data(**overrides) -> dict:
    data = {
        "client_name": "The Hershey Company",
        "company_name": "The Hershey Company",
        "industry": "food_beverage",
        "budget": "$150,000",
        "locations": ["Hershey, PA"],
        "roles": ["Machine Operator"],
        "target_roles": ["Machine Operator"],
        "campaign_duration": "3 months",
        "hire_volume": "50",
        "work_environment": "onsite",
        "competitors": list(TYPED),
        "_enriched": {},
        "_synthesized": {},
        "_budget_allocation": {},
    }
    data.update(overrides)
    return data


def _exec_summary_text(xlsx_bytes: bytes) -> str:
    """The Executive Strategic Summary narrative cell (the competitor NAMES
    legitimately appear elsewhere -- the client-typed competitor tables)."""
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))
    ws = wb["Executive Summary"]
    cells = [c for row in ws.iter_rows() for c in row if isinstance(c.value, str)]
    for i, c in enumerate(cells):
        if c.value.strip().upper() == "EXECUTIVE STRATEGIC SUMMARY":
            return cells[i + 1].value
    raise AssertionError("no Executive Strategic Summary section")


def test_llm_narrative_competitor_claims_are_removed_before_render():
    """The prod narrative shape through excel_v2's real narrative path (LLM
    client faked the same way tests/test_excel_narrative_reliability.py
    does). The grounded figures survive; the competitor sentence does not;
    the client sentence stays."""
    narrative = (
        f"{CLIENT_SENTENCE} Programmatic (DSP) is the core engine of the mix. "
        f"{COMPETITOR_SENTENCE} We recommend reviewing channel performance "
        "after the first month."
    )
    prompts: list = []

    def _fake_call_llm(**kwargs):
        prompts.append(kwargs["messages"][0]["content"])
        return {"text": narrative, "provider": "deepseek", "model": "m", "attempts": []}

    data = _narrative_data()
    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        text = _exec_summary_text(excel_v2.generate_excel_v2(data))
    assert "Treehouse" not in text and "Nestle Purina" not in text
    assert "where Hershey is drawing from the same" in text
    assert data["_narrative_status"]["status"] == "llm_grounded"
    assert data["_narrative_status"]["competitor_sentences_removed"] == 1
    # the prompt now tells the model it has no competitor evidence
    assert "NO verified data about any competitor" in prompts[0]


def test_narrative_that_is_mostly_competitor_claims_falls_back_to_template():
    narrative = f"{COMPETITOR_SENTENCE} Campbell's is drawing from the same pool too."

    def _fake_call_llm(**kwargs):
        return {"text": narrative, "provider": "deepseek", "model": "m", "attempts": []}

    data = _narrative_data()
    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        text = _exec_summary_text(excel_v2.generate_excel_v2(data))
    assert "drawing from the same" not in text
    assert data["_narrative_status"]["status"] == "llm_rejected_fabrication"


def _slide7_why_lines(competitors) -> list:
    import ppt_generator
    from pptx import Presentation
    from tools_regen_bundles import build_plan_data

    data = build_plan_data(
        {
            "client_name": "The Hershey Company",
            "industry": "food_beverage",
            "budget": "$150,000",
            "campaign_duration": "6 months",
            "hire_volume": "50-100 hires",
            "locations": ["Hershey, PA"],
            "roles": ["Machine Operator"],
            "target_roles": ["Machine Operator"],
            "competitors": competitors,
        }
    )
    prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(data)))
    lines = []
    for slide in prs.slides:
        for sh in slide.shapes:
            if getattr(sh, "has_text_frame", False) and sh.text_frame.text.startswith("Why:"):
                lines.append(sh.text_frame.text)
    return lines


def test_typed_competitor_without_evidence_gets_the_neutral_line():
    lines = _slide7_why_lines(list(TYPED[:3]))
    assert len(lines) == 3
    for line in lines:
        assert competitor_claims.NO_EVIDENCE_LINE in line, line
        assert "major employer" not in line and "likely competitor" not in line


def test_evidence_backed_competitor_still_renders_its_sourced_description():
    sourced = {
        "name": "Mars Wrigley",
        "description": "Confectionery manufacturer with plants in Hackettstown, NJ",
        "source_url": "https://example.com/mars-careers",
    }
    lines = _slide7_why_lines([sourced, "Campbell's"])
    assert any("Confectionery manufacturer with plants" in ln for ln in lines), lines
    assert any(competitor_claims.NO_EVIDENCE_LINE in ln for ln in lines), lines
