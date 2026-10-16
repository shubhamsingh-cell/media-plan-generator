"""Orchestrator decision 2026-10-17 (declare, never convert): a market with no
local cost-per-hire benchmark in its own currency projects NO hires and NO cost
per hire on ANY surface -- one short note instead; applications and cost per
application still show, identically, in the plan currency. A US plan and a plan
with a cited local benchmark (India) are unchanged."""

from __future__ import annotations

import io
import logging
import os
import sys
import types

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
for _n in ("anthropic", "openai", "supabase", "redis", "qdrant_client", "sentence_transformers", "posthog"):
    sys.modules.setdefault(_n, types.ModuleType(_n))
logging.disable(logging.CRITICAL)

import plan_projection  # noqa: E402

NOTE = "Hires: no local cost-per-hire benchmark for this market — not projected."
ROLE, LOC, IND = "Warehouse Associate", "London, UK", "logistics_supply_chain"


def _wizard(budget_text="£60,000", loc=LOC, role=ROLE, ind=IND):
    import app

    return app._compute_plan_estimate(
        {"budget": budget_text, "industry": ind, "roles": [role], "locations": [loc],
         "campaign_duration": "3 months", "client_name": "Acme"}
    )


def _quick(budget="£60,000", loc=LOC, role=ROLE, ind=IND):
    import quick_plan

    return quick_plan.generate_quick_plan(role, loc, budget, ind)


def _nova(budget=60000, loc=LOC, role=ROLE, ind=IND):
    from nova import Nova

    n = Nova.__new__(Nova)
    n._data_cache = {}
    return n._query_budget_projection({"budget": budget, "roles": [role], "locations": [loc], "industry": ind})


def test_note_is_one_shared_sentence():
    assert plan_projection.NO_LOCAL_HIRES_NOTE == NOTE


def test_london_gbp_hires_and_cph_are_absent_on_wizard_quick_plan_and_nova():
    wiz = _wizard()["projection"]
    quick = _quick()["total_projected"]
    nova = _nova()
    for proj in (wiz, quick["projection"], nova["projection"]):
        assert proj["hires"] == {"low": None, "mid": None, "high": None}
        assert proj["cph"] == {"low": None, "mid": None, "high": None}
        assert proj["assumptions"]["hires_suppressed"] is True
        assert proj["text"]["hires_note"] == NOTE
        assert proj["text"]["hires"] == "n/a" and proj["text"]["cph"] == "n/a"
    assert quick["total_hires"] is None and quick["blended_cph"] is None
    assert quick["hires_low"] is None
    for k in ("hires", "cost_per_hire", "hires_low", "hires_high"):
        assert k not in nova["total_projected"]
    assert nova["hires_note"] == NOTE and "do not state hires" in nova["presentation_note"].lower()


def test_london_applications_and_cpa_are_identical_and_in_pounds_everywhere():
    # (Nova's tool budget is US dollars by schema, so compare in dollars)
    wiz, quick, nova = (
        _wizard("$60,000")["projection"],
        _quick("$60,000")["total_projected"]["projection"],
        _nova()["projection"],
    )
    band = lambda p: {m: (p[m]["low"], p[m]["high"]) for m in ("applications", "cpa")}  # noqa: E731
    assert band(quick) == band(wiz) == band(nova)
    assert wiz["applications"]["high"] > 0 and wiz["cpa"]["low"] > 0
    assert wiz["text"]["cpa"].startswith("$")
    assert _wizard()["projection"]["text"]["cpa"].startswith("£")  # a typed £ stays £


def test_nova_formatter_says_the_note_and_prints_no_hires_figure():
    import nova

    out = nova._format_budget_response(_nova(), 60000)
    assert NOTE in out
    assert "Estimated Hires" not in out and "Cost per Hire" not in out
    assert "Cost per Application: $" in out


def test_a_us_dollar_plan_in_london_follows_the_same_rule():
    wiz = _wizard("$60,000")["projection"]
    assert wiz["assumptions"]["hires_suppressed"] is True
    assert wiz["text"]["hires_note"] == NOTE


def test_us_plan_is_unchanged_and_india_with_a_cited_local_benchmark_still_projects():
    us = _wizard("$60,000", "Columbus, OH", "Registered Nurse", "healthcare_medical")["projection"]
    assert us["assumptions"]["hires_suppressed"] is False
    assert us["text"]["hires"] == "5–11" and us["text"]["cph"] == "$5,250–$10,500"
    india = _wizard("₹25,000,000", "Bengaluru, India", "Java Developer", "tech_engineering")["projection"]
    assert india["assumptions"]["hires_suppressed"] is False
    assert india["hires"]["low"] and india["hires"]["high"] > india["hires"]["low"]


def test_wizard_and_quick_plan_pages_read_the_suppression():
    prev = open(os.path.join(PROJECT_ROOT, "templates/partials/index/body_preview_js.html"), encoding="utf-8").read()
    qp = open(os.path.join(PROJECT_ROOT, "templates/quick-plan.html"), encoding="utf-8").read()
    assert prev.count("hires_suppressed") >= 4 and "proj.text.hires_note" in prev
    assert "hires_suppressed" in qp and "pj.hires_note" in qp


# ---- deliverables -----------------------------------------------------------
def _bundle(budget, loc, role, industry):
    import excel_v2
    import ppt_generator
    import tools_regen_bundles as T
    from kb_loader import load_knowledge_base
    from openpyxl import load_workbook
    from pptx import Presentation

    data = T.build_plan_data(
        {"client_name": "Probe", "industry": industry, "budget": budget, "campaign_duration": "3 months",
         "hire_volume": "100 hires", "work_environment": "onsite", "locations": [loc],
         "roles": [role], "target_roles": [role]}
    )
    prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(data))))
    deck = {i: [sh.text_frame.text for sh in sl.shapes if sh.has_text_frame] for i, sl in enumerate(prs.slides, 1)}
    raw = excel_v2.generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    raw = raw[0] if isinstance(raw, tuple) else raw
    ws = load_workbook(io.BytesIO(raw))["Executive Summary"]
    rows = [[c.value for c in r if c.value not in (None, "")] for r in ws.iter_rows()]
    return deck, rows


@pytest.mark.parametrize("budget", ["£60,000", "$150,000"], ids=["gbp", "usd-in-london"])
def test_workbook_and_deck_project_no_hires_for_a_market_without_a_local_benchmark(budget):
    deck, rows = _bundle(budget, "London, UK", "Registered Nurse", "Healthcare & Medical")
    flat = [str(v) for r in rows for v in r]
    # hero cards: hires and cost per hire read n/a
    labels = next(i for i, r in enumerate(rows) if "Cost / Hire" in r)
    assert rows[labels - 1][-2:] == ["n/a", "n/a"], rows[labels - 1]
    assert any(NOTE in v for v in flat)
    assert not any(v.startswith("Projected hires") for v in flat)
    assert not any("Hiring-goal gap" in v and "/hire" in v for v in flat)
    s2 = " || ".join(deck[2])
    assert NOTE in s2 and "n/a" in s2 and "This plan projects" not in s2
    s6 = " || ".join(deck[6])
    assert "hires not projected" in s6 and "Hires: no local cost-per-hire benchmark" in s6
    assert "average cost-per-hire" not in s6


def test_us_deck_and_workbook_still_project_hires():
    deck, rows = _bundle("$150,000", "Dallas, TX", "Registered Nurse", "Healthcare & Medical")
    flat = [str(v) for r in rows for v in r]
    assert any(v.startswith("Projected hires: ") for v in flat)
    assert NOTE not in " || ".join(deck[2]) and not any(NOTE in v for v in flat)
