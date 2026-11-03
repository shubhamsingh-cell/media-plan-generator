"""An App Install goal says on the plan that installs are not modelled.

Independent review 2026-11-03: deck slide 2 printed "Client goals: App
Install" while the projection slides count applications and hires, and
nothing on the deck or workbook said installs are not modelled. One line now
sits beside the goal on the deck and in the workbook's Executive Summary.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import openpyxl
import pytest
from pptx import Presentation

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import excel_v2  # noqa: E402
import ppt_generator  # noqa: E402
import research  # noqa: E402
import tools_regen_bundles as T  # noqa: E402
import wizard_inputs  # noqa: E402

NOTE = "App installs are not modelled; figures are applications."


def _surfaces(goals):
    data = T.build_plan_data(
        {
            "client_name": "Install Test Co",
            "budget": "$60,000",
            "campaign_duration": "3 months",
            "campaign_start_month": 10,
            "industry": "logistics_supply_chain",
            "locations": ["Columbus, OH"],
            "roles": ["Delivery Driver"],
            "target_roles": [{"title": "Delivery Driver", "count": 5}],
        }
    )
    data["campaign_goals"] = list(goals)
    pptx = ppt_generator.generate_pptx(dict(data))
    pptx = pptx.getvalue() if hasattr(pptx, "getvalue") else pptx
    deck = [
        sh.text_frame.text
        for sl in Presentation(io.BytesIO(pptx)).slides
        for sh in sl.shapes
        if sh.has_text_frame
    ]
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research)
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    ws = openpyxl.load_workbook(io.BytesIO(xlsx))["Executive Summary"]
    summary = [str(c.value) for row in ws.iter_rows() for c in row if c.value]
    return deck, summary


def test_the_note_is_defined_once():
    assert wizard_inputs.APP_INSTALL_NOTE == NOTE
    assert wizard_inputs.goal_basis_note(["app_install"]) == NOTE
    assert wizard_inputs.goal_basis_note(["Direct Hiring"]) == ""


def test_an_app_install_goal_carries_the_note_on_deck_and_workbook():
    deck, summary = _surfaces(["App Install", "Direct Hiring"])
    goal_frames = [t for t in deck if "Client goals:" in t]
    assert goal_frames and any(NOTE in t for t in goal_frames), goal_frames
    assert any(NOTE in v for v in summary)


def test_without_app_install_there_is_no_note():
    deck, summary = _surfaces(["Direct Hiring"])
    assert not any(NOTE in t for t in deck)
    assert not any(NOTE in v for v in summary)


def test_the_wizard_preview_shows_the_note_from_the_one_definition():
    from tests.wizard_js_harness import PARTIALS

    assert wizard_inputs.page_config()["app_install_note"] == NOTE
    content = (PARTIALS / "body_content.html").read_text(encoding="utf-8")
    assert 'id="npGoalNote"' in content
    preview = (PARTIALS / "body_preview_js.html").read_text(encoding="utf-8")
    assert "NOVA_INPUTS.app_install_note" in preview
    assert 'input[name="campaignGoals"][value="App Install"]' in preview


def test_the_workbook_names_the_goals_with_the_note():
    _deck, summary = _surfaces(["App Install", "Direct Hiring"])
    assert any("Client goals: App Install" in v and NOTE in v for v in summary), summary[-5:]


def test_the_workbook_goal_line_sits_in_campaign_overview_in_body_type():
    data = T.build_plan_data(
        {
            "client_name": "Install Test Co",
            "budget": "$60,000",
            "campaign_duration": "3 months",
            "campaign_start_month": 10,
            "industry": "logistics_supply_chain",
            "locations": ["Columbus, OH"],
            "roles": ["Delivery Driver"],
            "target_roles": [{"title": "Delivery Driver", "count": 5}],
        }
    )
    data["campaign_goals"] = ["App Install"]
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research)
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    ws = openpyxl.load_workbook(io.BytesIO(xlsx))["Executive Summary"]
    cells = [(c.row, c) for row in ws.iter_rows() for c in row if isinstance(c.value, str)]
    overview = min(r for r, c in cells if c.value.upper() == "CAMPAIGN OVERVIEW")
    # the next section header (uppercase, 14pt bold), e.g. BUDGET ALLOCATION
    nxt = min(r for r, c in cells if r > overview and c.value.isupper()
              and (c.font.sz or 0) >= 14 and c.font.b and not c.value[0].isdigit()
              and not c.value.startswith("$"))
    goal = [(r, c) for r, c in cells if c.value.startswith("Client goals: App Install")]
    assert len(goal) == 1, [c.value for _, c in goal]
    r, c = goal[0]
    assert overview < r < nxt, (overview, r, nxt)
    assert NOTE in c.value
    assert (c.font.sz or 0) >= 10  # body type, not the 9pt footnote
