"""Client story review fixes (trust-and-proof):

1. channel reasons shown to a client / in Nova's Why column are client-safe:
   never an efficiency flag, an internal ROI score, or an above/below-average
   comparison;
2. no commitment wording ("What we will do", "we confirm the launch date");
3. "recruiting for <role> roles in <market>" grammar;
4. the copy fallback restores keyboard focus;
5. docs follow the slide shift.
"""

from __future__ import annotations

import os
import re
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import budget_engine  # noqa: E402
import client_story  # noqa: E402
import nova  # noqa: E402
import tools_regen_bundles as T  # noqa: E402
from tests.wizard_js_harness import NODE, PARTIALS, js_block, run_node  # noqa: E402

BANNED = re.compile(
    r"ROI|flagged|efficien|(?:above|below) the plan average|plan average|CPA \d+%",
    re.I,
)


def _brief(name, industry, budget, locations, roles):
    return {
        "client_name": name,
        "industry": industry,
        "budget": budget,
        "campaign_duration": "6 months",
        "hire_volume": "100 hires",
        "work_environment": "onsite",
        "locations": locations,
        "roles": roles,
        "target_roles": roles,
    }


@pytest.fixture(scope="module")
def plans():
    return {
        "tiny": T.build_plan_data(
            _brief("Probe Tiny", "Healthcare & Medical", "$1,000", ["Dallas, TX"], ["Registered Nurse"])
        ),
        "india": T.build_plan_data(
            _brief(
                "Probe Infotech", "tech_engineering", "₹25,000,000",
                ["Bengaluru, Karnataka, India", "Pune, Maharashtra, India"],
                ["Java Developer", "QA Engineer"],
            )
        ),
        "us": T.build_plan_data(
            _brief(
                "Probe Health", "Healthcare & Medical", "$250,000",
                ["Dallas, TX", "Austin, TX"], ["Registered Nurse", "Pharmacist"],
            )
        ),
    }


KEYS = ["tiny", "india", "us"]


@pytest.mark.parametrize("key", KEYS)
def test_story_carries_no_internal_scores_flags_or_comparisons(plans, key):
    text = client_story.build_client_story(plans[key])["text"]
    assert not BANNED.search(text), BANNED.search(text).group(0)


@pytest.mark.parametrize("key", KEYS)
def test_nova_why_column_is_client_safe(plans, key):
    allocs = {
        k: dict(v) for k, v in plans[key]["_budget_allocation"]["channel_allocations"].items()
    }
    nova._annotate_channel_reasons(allocs)
    whys = [v["why"] for v in allocs.values() if v.get("why")]
    assert whys, "no channel got a reason"
    for why in whys:
        assert not BANNED.search(why), why


def test_client_reason_never_emits_flags_scores_or_comparisons():
    entry = {
        "dollar_amount": 100.0,
        "percentage": 10.0,
        "efficiency_flag": "Low Efficiency",
        "roi_score": 1,
        "cpa": 625.0,
        "cpc_source": "trend_engine",
        "trend_direction": "rising",
        "trend_pct_yoy": 9.2,
    }
    why = budget_engine.channel_reason_client(entry)
    assert why == "10% of budget"
    assert not BANNED.search(why)


def test_client_reason_uses_budget_share_and_brand_rationale_only():
    base = {"percentage": 20.8, "trend_direction": "rising", "trend_pct_yoy": 9.2}
    assert budget_engine.channel_reason_client(base) == "20.8% of budget"
    # The trend engine's YoY is location-blind CPC inflation, not demand: never cite it.
    sourced = dict(base, cpc_source="trend_engine")
    assert budget_engine.channel_reason_client(sourced) == "20.8% of budget"
    assert "trend" not in budget_engine.channel_reason_client(dict(sourced, trend_direction="falling"))
    brand = {"channel_role": "brand", "percentage": 5.0, "rationale": "Brand investment"}
    assert budget_engine.channel_reason_client(brand) == "5% of budget; Brand investment"
    # a rationale that smuggles in an internal comparison is dropped
    leaky = {"channel_role": "brand", "percentage": 5.0, "rationale": "ROI score 9/10, above average"}
    assert budget_engine.channel_reason_client(leaky) == "5% of budget"
    # nothing supportable -> omitted
    assert budget_engine.channel_reason_client({}) == ""
    assert budget_engine.channel_reason_client(None) == ""  # type: ignore[arg-type]


def test_media_plan_template_copies_the_client_safe_why():
    template = nova._RESPONSE_TEMPLATES["media_plan"]
    assert "Never invent a reason" in template


# -- 2. commitment wording ----------------------------------------------------
@pytest.mark.parametrize("key", KEYS)
def test_no_commitment_wording(plans, key):
    story = client_story.build_client_story(plans[key])
    assert story["sections"][0]["heading"] == "What this plan does"
    text = story["text"].lower()
    for phrase in ("we will", "we confirm", "we'll", "we promise", "we deliver"):
        assert phrase not in text, phrase
    assert "launch date to be confirmed together" in text


# -- 3. role grammar ------------------------------------------------------------
def test_roles_read_as_roles_one_or_many(plans):
    one = client_story.build_client_story(plans["tiny"])["text"]
    assert "recruiting for Registered Nurse roles in Dallas, TX" in one
    many = client_story.build_client_story(plans["us"])["text"]
    assert "recruiting for Registered Nurse and Pharmacist roles in Dallas, TX; Austin, TX" in many
    plan = dict(plans["us"], target_roles=["A", "B", "C", "D"], roles=["A", "B", "C", "D"])
    assert "for A, B and C roles (and 1 more) in" in client_story.build_client_story(plan)["text"]


# -- 4. focus restore -------------------------------------------------------------
@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_copy_fallback_restores_focus_to_the_previous_element():
    src = (PARTIALS / "body_app_js.html").read_text(encoding="utf-8")
    fn = js_block(src, "async function novaCopyText(")
    script = (
        fn
        + """
const log = [];
const prev = { focus() { log.push("prev.focus"); } };
let removed = false;
global.document = {
  activeElement: prev,
  body: { appendChild() { log.push("append"); } },
  createElement() {
    return {
      style: {}, setAttribute() {}, select() { log.push("select"); },
      remove() { removed = true; log.push("remove"); },
    };
  },
  execCommand() { log.push("exec"); return true; },
};
Object.defineProperty(globalThis, "navigator", { configurable: true, value: { clipboard: { writeText() { return Promise.reject(new Error("denied")); } } } });
novaCopyText("story").then((ok) => { console.log(JSON.stringify({ ok, log, removed })); });
"""
    )
    got = run_node(script)
    assert got["ok"] is True and got["removed"] is True
    assert got["log"][-2:] == ["remove", "prev.focus"], got["log"]


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_copy_uses_the_clipboard_api_without_touching_focus():
    src = (PARTIALS / "body_app_js.html").read_text(encoding="utf-8")
    fn = js_block(src, "async function novaCopyText(")
    script = (
        fn
        + """
global.document = { activeElement: null, createElement() { throw new Error("no fallback"); } };
Object.defineProperty(globalThis, "navigator", { configurable: true, value: { clipboard: { writeText() { return Promise.resolve(); } } } });
novaCopyText("story").then((ok) => console.log(JSON.stringify({ ok })));
"""
    )
    assert run_node(script) == {"ok": True}


# -- 5. docs follow the slide shift --------------------------------------------------
def test_deck_qa_docs_name_the_shifted_slides():
    readme = open(os.path.join(PROJECT_ROOT, "scripts", "deck_qa", "README.md"), encoding="utf-8").read()
    assert "[--slides 6,8]" in readme and "slides 6 and 8 never get stressed" in readme
    assert "slide 6's benchmark table and slide 8's" in readme
    matrix = open(os.path.join(PROJECT_ROOT, "scripts", "deck_qa", "matrix.py"), encoding="utf-8").read()
    assert "slide-6 benchmark table" in matrix and "slide-5 benchmark table" not in matrix
