"""Quick Brief -> "Generate Full Plan" must hand the wizard the brief it read.

Brendan (RTI International, 2026-10-09): the handoff link carried
roles=/locations=/hires=/timeline= and industry=Healthcare, but the wizard
prefill read only role=/location= and a card key, so the plan arrived with
just a budget and was built as "General / Entry-Level". Quick Brief also
invented an RN role from "RTI Inte-rn-ational" (substring keyword scan).

These run the REAL extraction functions from templates/quick-brief.html and
the REAL prefill helpers from body_app_js.html under node.
"""

from __future__ import annotations

import json

import pytest

from tests.wizard_js_harness import APP_JS, NODE, ROOT, js_block, run_node

QUICK_BRIEF = ROOT / "templates" / "quick-brief.html"

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


def _qb_fn(name: str, table: str) -> str:
    src = QUICK_BRIEF.read_text(encoding="utf-8")
    t0 = src.index(f"var {table}")
    t1 = src.index("];" if table == "ROLE_KEYWORDS" else "};", t0) + 2
    return src[t0:t1] + "\n" + js_block(src, f"function {name}(")


def _extract_roles(text: str) -> list:
    js = _qb_fn("extractRoles", "ROLE_KEYWORDS")
    return run_node(js + f"\nconsole.log(JSON.stringify(extractRoles({json.dumps(text)}).raw));")


def _infer_industry(text: str) -> str:
    js = _qb_fn("inferIndustry", "ROLE_INDUSTRY_MAP")
    return run_node(js + f"\nconsole.log(JSON.stringify(inferIndustry({json.dumps(text)}, []).value));")


def test_client_name_never_becomes_a_role():
    roles = _extract_roles(
        "RTI International needs 200 coordinators, field workers and RNs across MD, NY, PA"
    )
    assert roles == ["Coordinator", "Worker", "RN"]
    assert _extract_roles("internal government analyst") == ["Analyst"]


def test_acronym_roles_keep_their_casing():
    assert sorted(_extract_roles("hiring an RN and two LPNs")) == ["LPN", "RN"]


def test_industry_is_not_inferred_from_inside_a_word():
    assert _infer_industry("RTI International needs field interviewers") == "General"
    assert _infer_industry("hiring 20 RNs in Ohio") == "Healthcare"


def test_handoff_joins_lists_with_a_hard_separator():
    """', ' made "Jacksonville, Washington" one City, State site."""
    src = QUICK_BRIEF.read_text(encoding="utf-8")
    assert 'brief.locations.raw.join("; ")' in src
    assert 'brief.roles.raw.join("; ")' in src


def _prefill_helpers() -> str:
    src = APP_JS.read_text(encoding="utf-8")
    labels = src[src.index("var _PREFILL_INDUSTRY_LABELS") : src.index("function _prefillIndustryKey")]
    return (
        labels
        + js_block(src, "function _prefillIndustryKey(")
        + "\n"
        + js_block(src, "function _prefillDurationOption(")
    )


def test_prefill_maps_quick_brief_industry_labels():
    js = (
        "var CARDS = ['healthcare_medical','tech_engineering','general_entry_level'];\n"
        "var document = { querySelector: function (sel) {"
        "  var m = sel.match(/data-industry=\"([^\"]*)\"/);"
        "  return m && CARDS.indexOf(m[1]) >= 0 ? {} : null; },"
        "  getElementById: function () { return null; } };\n"
        + _prefill_helpers()
        + "\nconsole.log(JSON.stringify(['Healthcare','Technology','healthcare_medical','Sales','',"
        "'Hospitality & Food Service'].map(_prefillIndustryKey)));"
    )
    assert run_node(js) == [
        "healthcare_medical",
        "tech_engineering",
        "healthcare_medical",
        "",  # ambiguous label: left for the user, never guessed
        "",
        "hospitality_travel",
    ]


def test_prefill_picks_the_nearest_duration_option():
    opts = ["2 weeks", "1 month", "2 months", "3 months", "6 months", "12 months", "1-3 months"]
    js = (
        "var OPTS = " + json.dumps(opts) + ";\n"
        "var document = { getElementById: function () {"
        "  return { options: OPTS.map(function (v) { return { value: v }; }) }; } };\n"
        + _prefill_helpers()
        + "\nconsole.log(JSON.stringify(['8 weeks','3 months','1 year','2 weeks','ASAP','summer']"
        ".map(_prefillDurationOption)));"
    )
    assert run_node(js) == ["2 months", "3 months", "12 months", "2 weeks", "", ""]
