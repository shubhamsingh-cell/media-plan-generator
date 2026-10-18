"""Nova and the plan generator quote ONE set of plan figures for one brief.

Investigation 2026-10-18: a 60k, 3-month brief read 900 applications / 14 hires /
$66.67 CPA in chat while the plan generator said 2,400-7,500 / 17-35 / $8-$25.
What the offline reproduction showed:

* ``app._compute_plan_estimate`` (the wizard / generator headline) and Nova's
  ``query_budget_projection`` call the same ``plan_projection.project_plan``, but
  Nova handed it its OWN 11-key knowledge-base slice. ``plan_projection._load_kb``
  honours any non-empty dict, so the benchmark CPA band vanished from chat only
  (a collapsed "7,247-7,247 applications / $8.28-$8.28"). The older parity test
  built Nova with ``_data_cache = {}``, which falls back to the full KB, so it
  never saw the production shape.
* Six more Nova tools computed their OWN plan totals from private tables:
  ``recommend_channels`` (917 applications / 17 hires / $65.43 avg CPA for 60k:
  the same family as the chat figures above), ``optimize_campaign`` (96 hires),
  ``predict_hiring_outcome`` ($60,000 per hire), ``simulate_what_if`` (a hidden
  $50k baseline), ``suggest_smart_defaults`` (a hard-coded cost-per-hire table).
* The system prompt told the model to compute hires itself ("hires from
  budget / avg_CPH", "CPH = salary x 4.4%, silently") and the number verifier
  never looked at hire / application counts and accepted ANY tool's number.

These tests drive the real tool functions and the real post-processor with a
production-shaped ``Nova()`` (no LLM, no network).
"""

from __future__ import annotations

import json
import re
import sys
import types
from pathlib import Path

import pytest

for _name in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    sys.modules.setdefault(_name, types.ModuleType(_name))

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402
import nova  # noqa: E402

BUDGET = 60_000
# (label, wizard industry key, role, location)
SHAPES = [
    ("unspecified", "", "Customer Service Representative", "Chicago, IL"),
    ("healthcare", "healthcare_medical", "Registered Nurse", "Chicago, IL"),
    ("technology", "tech_engineering", "Software Engineer", "Chicago, IL"),
    ("retail", "retail_consumer", "Retail Associate", "Chicago, IL"),
    ("logistics", "logistics_supply_chain", "Warehouse Associate", "Chicago, IL"),
]


@pytest.fixture(scope="module")
def engine():
    """A fully initialised Nova: the production ``_data_cache`` shape."""
    return nova.Nova()


@pytest.fixture(autouse=True)
def _verifier_on(monkeypatch):
    monkeypatch.setenv("NOVA_NUMBER_VERIFIER_ENABLED", "1")
    monkeypatch.setenv("NOVA_CITATIONS_ENABLED", "0")


def _estimate(
    industry, role, loc, *, budget="$60,000", period="campaign", months="3 months"
):
    return app._compute_plan_estimate(
        {
            "budget_range": budget,
            "budget_period": period,
            "campaign_duration": months,
            "industry": industry,
            "target_roles": [role],
            "locations": [loc],
        }
    )


def _tool(engine_, industry, role, loc, **extra):
    return engine_._query_budget_projection(
        {
            "budget": BUDGET,
            "roles": [role],
            "locations": [loc],
            "industry": industry,
            **extra,
        }
    )


# ---------------------------------------------------------------------------
# 1. The projection tool reads the same engine inputs as /api/estimate
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("label,industry,role,loc", SHAPES)
def test_nova_projection_block_equals_the_wizard_estimate(
    engine, label, industry, role, loc
):
    wizard = _estimate(industry, role, loc)["projection"]
    chat = _tool(engine, industry, role, loc)["projection"]
    # every number AND every label: ranges, assumptions, notes, text. Only the
    # provenance word differs: Nova says "classified_from_roles" for an industry
    # the user did not state, where the wizard (which classifies first) says "brief".
    wizard.pop("industry_source", None)
    chat.pop("industry_source", None)
    assert chat == wizard, f"{label}: Nova's projection differs from /api/estimate"


@pytest.mark.parametrize("label,industry,role,loc", SHAPES)
def test_nova_funds_the_same_channels_as_the_wizard(engine, label, industry, role, loc):
    est = _estimate(industry, role, loc)
    chat = _tool(engine, industry, role, loc)
    amounts = {
        k: round(float(v["dollar_amount"]), 2)
        for k, v in chat["channel_allocations"].items()
        if float(v.get("dollar_amount") or 0) > 0
    }
    assert amounts == {c["key"]: c["amount"] for c in est["channels"]}


def test_application_and_cpa_ranges_are_not_collapsed_to_the_engine_point(engine):
    """Pre-fix chat read '3,846-3,846 applications, $15.60-$15.60' for a nurse
    plan where the wizard read '1,034-3,846, $15.60-$58.00'."""
    p = _tool(engine, "healthcare_medical", "Registered Nurse", "Chicago, IL")[
        "projection"
    ]
    assert p["applications"]["low"] < p["applications"]["high"]
    assert p["cpa"]["low"] < p["cpa"]["high"]


# ---------------------------------------------------------------------------
# 2. Period / total-vs-monthly is explicit
# ---------------------------------------------------------------------------
def test_default_budget_is_read_as_a_campaign_total_and_says_so(engine):
    out = _tool(engine, "healthcare_medical", "Registered Nurse", "Chicago, IL")
    reading = out["budget_reading"]
    assert reading["period"] == "campaign"
    assert reading["campaign_total"] == BUDGET
    assert "$60,000" in reading["text"] and "total" in reading["text"].lower()


def test_monthly_budget_is_scaled_by_the_stated_months_exactly_like_the_wizard(engine):
    out = _tool(
        engine,
        "healthcare_medical",
        "Registered Nurse",
        "Chicago, IL",
        budget_period="monthly",
        campaign_months=3,
    )
    assert out["total_budget"] == 180_000
    assert out["budget_reading"]["campaign_total"] == 180_000
    assert "per month" in out["budget_reading"]["text"]
    wizard = _estimate(
        "healthcare_medical", "Registered Nurse", "Chicago, IL", period="monthly"
    )["projection"]
    out["projection"].pop("industry_source", None)
    wizard.pop("industry_source", None)
    assert out["projection"] == wizard


def test_a_per_period_budget_without_months_states_the_assumed_length(engine):
    out = _tool(
        engine,
        "healthcare_medical",
        "Registered Nurse",
        "Chicago, IL",
        budget_period="quarterly",
    )
    reading = out["budget_reading"]
    assert reading["months_assumed"] is True
    assert "assum" in reading["text"].lower()


def test_budget_below_the_generator_minimum_is_the_generators_message(engine):
    out = engine._query_budget_projection({"budget": 50, "roles": ["Retail Associate"]})
    assert "at least" in (out.get("error") or "")


def test_presentation_note_forbids_own_math_and_labels_the_best_case(engine):
    note = _tool(engine, "healthcare_medical", "Registered Nurse", "Chicago, IL")[
        "presentation_note"
    ].lower()
    assert "best case" in note
    assert "never" in note and "yourself" in note


# ---------------------------------------------------------------------------
# 3. No second calculator may publish a plan total
# ---------------------------------------------------------------------------
def _plan_texts(engine_, industry, role, loc):
    return _estimate(industry, role, loc)["projection"]["text"]


def test_recommend_channels_no_longer_publishes_its_own_plan_totals(engine):
    out = engine._recommend_channels_tool(
        {
            "industry": "",
            "role": "Customer Service Representative",
            "budget": BUDGET,
            "locations": ["Chicago, IL"],
        }
    )
    blob = json.dumps(out, default=str)
    assert "total_projected_applications" not in blob
    assert "total_projected_hires" not in blob
    assert not re.search(
        r"Expected: ~", blob
    ), "the 917 applications / 17 hires sentence"
    assert not re.search(r"Apps: ~", blob)
    assert "917" not in blob
    # the plan figures are the engine's, stated as ranges
    assert out["plan_projection"]["text"] == _plan_texts(
        engine, "", "Customer Service Representative", "Chicago, IL"
    )
    # and the tool still tells the model WHERE to spend
    assert out["must_have"] and out["must_have"][0]["allocation_pct"] > 0
    assert out["must_have"][0]["expected_cpc"] > 0


def test_optimize_campaign_no_longer_publishes_its_own_plan_totals(engine):
    out = engine._optimize_campaign(
        {
            "budget": BUDGET,
            "industry": "",
            "role": "Customer Service Representative",
            "location": "Chicago, IL",
        }
    )
    assert "total_projected" not in out and "scenarios" not in out
    for ch in out["recommended_allocation"].values():
        assert "projected_hires" not in ch and "projected_applies" not in ch
        assert "cph" not in ch
    assert out["plan_projection"]["text"] == _plan_texts(
        engine, "", "Customer Service Representative", "Chicago, IL"
    )


def test_predict_hiring_outcome_no_longer_prints_sixty_thousand_dollars_per_hire(
    engine,
):
    out = engine._predict_hiring_outcome(
        {
            "budget": BUDGET,
            "roles": ["Customer Service Representative"],
            "locations": ["Chicago, IL"],
            "industry": "",
        }
    )
    pred = out["prediction"]
    for key in (
        "predicted_applications",
        "predicted_cost_per_hire",
        "predicted_qualified",
    ):
        assert key not in pred
    assert out["plan_projection"]["text"]["hires"]


def test_simulate_what_if_does_not_invent_a_fifty_thousand_dollar_baseline(engine):
    out = engine._simulate_what_if(
        {"scenario_description": "what if we add 20% to the budget", "delta_pct": 0.2}
    )
    blob = json.dumps(out, default=str)
    assert out.get("needs_base_budget") is True
    assert "29 -> 116" not in blob and "projected_hires" not in blob
    assert "hires" not in " ".join(out.get("recommendations") or []).lower()


def test_simulate_what_if_with_a_base_budget_uses_the_projection_engine(engine):
    out = engine._simulate_what_if(
        {
            "scenario_description": "+20% budget",
            "delta_pct": 0.2,
            "base_budget": BUDGET,
            "roles": ["Registered Nurse"],
            "locations": ["Chicago, IL"],
            "industry": "healthcare_medical",
        }
    )
    impact = out["budget_impact"]
    assert impact["original_budget"] == BUDGET and impact["new_budget"] == 72_000
    before = _estimate("healthcare_medical", "Registered Nurse", "Chicago, IL")[
        "projection"
    ]
    after = _estimate(
        "healthcare_medical", "Registered Nurse", "Chicago, IL", budget="$72,000"
    )["projection"]
    assert impact["hires_before"] == before["text"]["hires"]
    assert impact["hires_after"] == after["text"]["hires"]


def test_suggest_smart_defaults_budget_comes_from_the_engine_not_a_tier_table(engine):
    out = engine._suggest_smart_defaults(
        {
            "roles": ["Customer Service Representative"],
            "hire_count": 14,
            "locations": ["Chicago, IL"],
        }
    )
    blob = json.dumps(out, default=str)
    assert "estimated_cph" not in blob and "per_hire" not in blob
    solved = engine._query_budget_projection(
        {
            "target_hires": 14,
            "roles": ["Customer Service Representative"],
            "locations": ["Chicago, IL"],
        }
    )
    assert out["required_budget"] == solved["required_budget"]


# ---------------------------------------------------------------------------
# 4. The prompt no longer tells the model to do the math itself
# ---------------------------------------------------------------------------
def test_prompt_makes_the_projection_tool_the_only_source_of_plan_figures(engine):
    prompt = engine.get_system_prompt(
        "make a $60k media plan for 3 months, CPA and CPH"
    )
    assert "query_budget_projection" in prompt
    low = prompt.lower()
    assert "estimate hires from budget / avg_cph" not in low
    assert "annual salary x 4.4%" not in low and "salary x 4.4%" not in low
    assert "back-calculate projected hires" not in low
    assert "never compute" in low or "do not compute" in low
    assert "best case" in low
    assert "total or monthly" in low or "per month" in low


def test_media_plan_template_asks_for_the_engines_ranges_not_one_number():
    template = nova._RESPONSE_TEMPLATES["media_plan"]
    assert "projection.text" in template
    assert "range" in template.lower()


# ---------------------------------------------------------------------------
# 5. The guard: figures the model typed that are not the engine's
# ---------------------------------------------------------------------------
def _turn(tool_name, result):
    nova._set_turn_tool_capture(
        {
            "conversation_id": "",
            "tools": {tool_name: json.dumps(result, default=str)},
            "tools_used": [tool_name],
            "stateless": True,
        }
    )


@pytest.fixture
def healthcare_turn(engine):
    result = _tool(engine, "healthcare_medical", "Registered Nurse", "Chicago, IL")
    _turn("query_budget_projection", result)
    yield result
    nova._set_turn_tool_capture(None)


def _post(
    engine_,
    text,
    tool_result,
    message="make a $60k, 3-month media plan for nurses in Chicago",
):
    return engine_._apply_t3_postprocessing(
        {
            "response": text,
            "sources": [],
            "confidence": 0.8,
            "tools_used": ["query_budget_projection"],
            "_tool_results_raw": [json.dumps(tool_result, default=str)],
        },
        message,
    )["response"]


def test_the_reported_figures_are_rewritten_to_the_engines_ranges(
    engine, healthcare_turn
):
    text = (
        "For $60,000 over three months Nova projects 900 applications, 14 hires and "
        "a CPA of $66.67, a cost per hire of $4,286."
    )
    out = _post(engine, text, healthcare_turn)
    t = healthcare_turn["projection"]["text"]
    assert t["applications"] in out and t["hires"] in out
    assert t["cpa"] in out and t["cph"] in out
    for gone in ("900 applications", "14 hires", "$66.67", "$4,286"):
        assert gone not in out
    assert "corrected to Nova's planning engine" in out


def test_a_single_absolute_that_is_only_the_best_case_end_becomes_the_range(
    engine, healthcare_turn
):
    out = _post(engine, "You can expect 11 hires from this plan.", healthcare_turn)
    assert healthcare_turn["projection"]["text"]["hires"] in out
    assert "11 hires" not in out.replace(
        healthcare_turn["projection"]["text"]["hires"], ""
    )


def test_a_correct_range_is_left_alone_and_gets_the_basis_line(engine, healthcare_turn):
    t = healthcare_turn["projection"]["text"]
    text = f"Expect {t['hires']} hires and a cost per hire of {t['cph']}."
    out = _post(engine, text, healthcare_turn)
    assert text in out
    assert "corrected" not in out
    assert "no client hire history" in out
    assert "best case" in out.lower()


def test_per_channel_figures_from_the_tool_are_not_rewritten(engine, healthcare_turn):
    ch = healthcare_turn["channel_allocations"]["programmatic_dsp"]
    text = (
        f"Programmatic alone is modelled at {ch['projected_hires']} hires "
        f"and a CPA of ${ch['cpa']:.2f}."
    )
    out = _post(engine, text, healthcare_turn)
    assert f"{ch['projected_hires']} hires" in out and f"${ch['cpa']:.2f}" in out
    assert "corrected" not in out


def test_numbers_the_user_typed_are_not_treated_as_model_inventions(
    engine, healthcare_turn
):
    out = _post(
        engine,
        "To reach your goal of 40 hires you need a bigger budget.",
        healthcare_turn,
        message="we want to hire 40 nurses on $60k",
    )
    assert "40 hires" in out and "corrected" not in out


def test_a_number_in_the_users_wording_does_not_excuse_an_unrelated_claim(
    engine, healthcare_turn
):
    """'3-month' must not make '3 hires' look like an echo of the user."""
    out = _post(
        engine,
        "That gives you 3 hires.",
        healthcare_turn,
        message="make a $60k, 3-month media plan for nurses in Chicago",
    )
    assert (
        "3 hires" not in out and healthcare_turn["projection"]["text"]["hires"] in out
    )


def test_a_table_cell_that_matches_no_engine_figure_is_marked(engine, healthcare_turn):
    text = (
        "| Channel | Budget | % | Est. CPA | Projected Hires |\n"
        "|---|---|---|---|---|\n"
        "| Programmatic | $14,238 | 23.7% | $66.67 | 99 |\n"
    )
    out = _post(engine, text, healthcare_turn)
    assert "99†" in out or "99 †" in out
    assert nova._T3_UNVERIFIED_FOOTER in out


def test_a_plan_answer_without_the_ranges_gets_the_engines_block(
    engine, healthcare_turn
):
    out = engine._apply_t3_postprocessing(
        {
            "response": "### Media Plan: Nurse Recruitment -- Chicago\n\n| Channel | Budget |\n|---|---|\n| A | $1 |\n",
            "sources": [],
            "confidence": 0.8,
            "tools_used": ["query_budget_projection"],
            "query_type": "media_plan",
            "_tool_results_raw": [json.dumps(healthcare_turn, default=str)],
        },
        "make a $60k, 3-month media plan for nurses in Chicago",
    )["response"]
    t = healthcare_turn["projection"]["text"]
    assert "Projected outcomes" in out
    assert t["applications"] in out and t["hires"] in out and t["cpa"] in out
    assert "$60,000" in out  # the budget reading


def test_guard_is_idempotent(engine, healthcare_turn):
    once = _post(engine, "That is 14 hires at a CPA of $66.67.", healthcare_turn)
    twice = _post(engine, once, healthcare_turn)
    assert twice == once


def test_without_a_projection_in_the_turn_plan_figures_are_marked_not_trusted(engine):
    nova._set_turn_tool_capture(
        {"conversation_id": "", "tools": {}, "tools_used": [], "stateless": True}
    )
    try:
        out = engine._apply_t3_postprocessing(
            {
                "response": "With $60,000 you get 900 applications and 14 hires.",
                "sources": [],
                "confidence": 0.8,
                "tools_used": [],
            },
            "plan a $60k 3-month campaign",
        )["response"]
    finally:
        nova._set_turn_tool_capture(None)
    assert "900† applications" in out and "14† hires" in out
    assert "$60,000†" not in out  # the user's own budget is not an invention
    assert nova._T3_UNVERIFIED_FOOTER in out


def test_a_general_benchmark_answer_is_left_alone(engine):
    nova._set_turn_tool_capture(
        {"conversation_id": "", "tools": {}, "tools_used": [], "stateless": True}
    )
    try:
        out = engine._apply_t3_postprocessing(
            {
                "response": "Nurses are in short supply.",
                "sources": [],
                "confidence": 0.8,
                "tools_used": [],
            },
            "how hard is it to hire nurses",
        )["response"]
    finally:
        nova._set_turn_tool_capture(None)
    assert out == "Nurses are in short supply."


def test_direct_dispatch_answers_are_verified_against_the_turn_tool_results(
    engine, healthcare_turn
):
    """The direct-dispatch route returns no _tool_results_raw: the verifier used
    to see an empty tool list and flag every engine figure it had just quoted."""
    t = healthcare_turn["projection"]["text"]
    out = engine._apply_t3_postprocessing(
        {
            "response": f"Cost per hire runs {t['cph']}.",
            "sources": [],
            "confidence": 0.8,
            "tools_used": ["query_budget_projection"],
        },
        "what will it cost per hire for $60k",
    )["response"]
    assert "†" not in out


# ---------------------------------------------------------------------------
# 6. Direct dispatch reads "$20k a month for 3 months" as $60,000
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "message,expected",
    [
        ("which channels should we use with $60k for a 3-month campaign", 60_000),
        ("which channels should we use with $20k a month for 3 months", 60_000),
        ("which channels should we use with $20k per month over 3 months", 60_000),
        ("which channels should we use with $5k per quarter for 6 months", 10_000),
    ],
)
def test_direct_dispatch_budget_is_the_campaign_total(
    engine, monkeypatch, message, expected
):
    seen = {}

    def fake_execute(name, params):
        seen["name"], seen["params"] = name, dict(params)
        return json.dumps({"source": "stub", "summary": "ok"})

    monkeypatch.setattr(engine, "execute_tool", fake_execute)
    import llm_router

    monkeypatch.setattr(
        llm_router,
        "call_llm",
        lambda **kw: {"text": "ok", "provider": "stub", "model": "stub"},
    )
    out = engine._try_direct_tool_dispatch(message, message.lower(), [], 0.0, "", None)
    assert out is not None and seen["name"] == "recommend_channels"
    assert seen["params"]["budget"] == expected


# ---------------------------------------------------------------------------
# 7. Small budgets: a useful, honest answer, never a refusal
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("budget", [1_000, 2_500, 5_000, 10_000])
def test_small_budgets_still_get_a_plan_and_an_honest_reading(engine, budget):
    out = engine._query_budget_projection(
        {
            "budget": budget,
            "roles": ["Retail Associate"],
            "locations": ["Chicago, IL"],
            "industry": "retail_consumer",
        }
    )
    assert "error" not in out
    assert out["channel_allocations"], "no channels"
    p = out["projection"]
    assert p["text"]["applications"]
    note = out.get("small_budget")
    if budget <= 5_000:
        assert note, f"${budget:,}: no small-budget guidance"
        assert note["text"].count(p["text"]["applications"]) == 1
        assert 1 <= len(note["focus_channels"]) <= 3
        assert note["one_hire_budget"]["low"] <= note["one_hire_budget"]["high"]
        assert "do not refuse" in note["instruction"].lower()
        assert "minimum" in note["instruction"].lower()
        # the guidance quotes the engine's own cost-per-hire basis, nothing else
        a = p["assumptions"]
        assert note["one_hire_budget"] == {
            "low": a["industry_cph_floor"],
            "high": a["industry_cph_avg"],
        }
    if budget == 1_000:
        assert "fewer than one hire" in note["text"].lower()


def test_a_large_budget_carries_no_small_budget_note(engine):
    assert "small_budget" not in _tool(
        engine, "healthcare_medical", "Registered Nurse", "Chicago, IL"
    )


def test_the_wizard_copilot_does_not_tell_a_tiny_budget_to_raise_itself():
    """There is no minimum budget: the nudge shapes the plan, it does not send
    the user away."""
    import plan_copilot

    nudge = plan_copilot.get_copilot_nudge(
        "budget", "$1,000", {"roles": ["Software Engineer"]}
    )
    assert nudge, "the heuristic still speaks for a sub-hire budget"
    assert "raising" not in nudge["message"].lower()
    assert "tight" not in nudge["message"].lower()
    assert nudge["type"] != "warning"
    assert "one to three channels" in nudge["message"]


# ---------------------------------------------------------------------------
# More guard edges
# ---------------------------------------------------------------------------
@pytest.fixture
def generic_turn(engine):
    result = _tool(engine, "", "Customer Service Representative", "Chicago, IL")
    _turn("query_budget_projection", result)
    yield result
    nova._set_turn_tool_capture(None)


def test_a_plan_claim_does_not_borrow_a_channels_integer(engine, generic_turn):
    """The reported 14 hires equals ONE channel's modelled hires for this brief:
    that must not excuse it as the plan's."""
    per_channel = {
        c["projected_hires"] for c in generic_turn["channel_allocations"].values()
    }
    assert 14 in per_channel, "premise: a channel really models 14 hires"
    out = _post(engine, "That works out to 14 hires.", generic_turn)
    assert "14 hires" not in out
    assert generic_turn["projection"]["text"]["hires"] in out


def test_another_tools_channel_count_does_not_excuse_a_plan_total(engine, generic_turn):
    meta = {"estimated_applies": 1200, "estimated_cpa": 50, "source": "Meta Ads"}
    raw = [json.dumps(generic_turn, default=str), json.dumps(meta)]

    def post(text):
        return engine._apply_t3_postprocessing(
            {
                "response": text,
                "sources": [],
                "confidence": 0.8,
                "tools_used": [],
                "_tool_results_raw": raw,
            },
            "make a $60k, 3-month media plan",
        )["response"]

    t = generic_turn["projection"]["text"]["applications"]
    assert t in post("The plan brings 1,200 applications in total.")
    assert "Meta alone would bring 1,200 applications" in post(
        "Meta alone would bring 1,200 applications."
    )


def test_a_benchmark_sentence_is_marked_not_rewritten(engine, healthcare_turn):
    out = _post(
        engine, "The typical nurse CPA across the market is $120.", healthcare_turn
    )
    assert "$120†" in out
    assert healthcare_turn["projection"]["text"]["cpa"] not in out.split("\n\n")[0]
    assert nova._T3_UNVERIFIED_FOOTER in out


def test_two_different_projections_in_one_turn_are_never_rewritten(engine):
    a = _tool(engine, "healthcare_medical", "Registered Nurse", "Chicago, IL")
    b = engine._query_budget_projection(
        {
            "budget": 120_000,
            "roles": ["Registered Nurse"],
            "locations": ["Chicago, IL"],
            "industry": "healthcare_medical",
        }
    )
    out = engine._apply_t3_postprocessing(
        {
            "response": "Either way you get 14 hires.",
            "sources": [],
            "confidence": 0.8,
            "tools_used": [],
            "_tool_results_raw": [
                json.dumps(a, default=str),
                json.dumps(b, default=str),
            ],
        },
        "compare $60k with $120k",
    )["response"]
    assert "14† hires" in out and "corrected" not in out


def test_a_market_without_a_cost_per_hire_basis_marks_hires_instead_of_inventing_them(
    engine,
):
    res = engine._query_budget_projection(
        {
            "budget": BUDGET,
            "roles": ["Warehouse Associate"],
            "locations": ["Mumbai, India"],
            "industry": "technology",
        }
    )
    assert res["projection"]["assumptions"]["hires_suppressed"]
    out = engine._apply_t3_postprocessing(
        {
            "response": "That is about 20 hires.",
            "sources": [],
            "confidence": 0.8,
            "tools_used": [],
            "_tool_results_raw": [json.dumps(res, default=str)],
        },
        "plan a $60k campaign in Mumbai",
    )["response"]
    assert "20† hires" in out
    assert res["projection"]["text"]["hires_note"] in out


def test_the_block_states_a_monthly_reading(engine):
    res = _tool(
        engine,
        "healthcare_medical",
        "Registered Nurse",
        "Chicago, IL",
        budget_period="monthly",
        campaign_months=3,
    )
    out = engine._apply_t3_postprocessing(
        {
            "response": "### Media Plan\n\n| Channel | Budget |\n|---|---|\n| A | $1 |\n",
            "sources": [],
            "confidence": 0.8,
            "tools_used": [],
            "query_type": "media_plan",
            "_tool_results_raw": [json.dumps(res, default=str)],
        },
        "plan $60k a month for 3 months",
    )["response"]
    assert "Planning on $180,000: $60,000 per month over 3 months." in out


def test_the_guard_can_be_switched_off(engine, healthcare_turn, monkeypatch):
    monkeypatch.setenv("NOVA_PLAN_FIGURE_GUARD_ENABLED", "0")
    out = _post(engine, "That is 14 hires.", healthcare_turn)
    assert "14 hires" in out and "corrected" not in out


# ---------------------------------------------------------------------------
# End to end: the real tool loop, the real post-processing, a scripted model
# ---------------------------------------------------------------------------
E2E_MESSAGE = (
    "Make a $60k, 3 month media plan for customer service reps in Chicago, "
    "excluding Indeed and ZipRecruiter (e2e {tag})"
)
WRONG_ANSWER = (
    "### Media Plan: Customer Service -- Chicago\n**Budget: $60,000**\n\n"
    "Start with Indeed and ZipRecruiter for volume. Expect 900 applications, "
    "14 hires and a CPA of $66.67 (cost per hire $4,286).\n\n"
    "| Channel | Budget | % | Est. CPA | Projected Hires | Why |\n"
    "|---|---|---|---|---|---|\n"
    "| Indeed Sponsored Jobs | $12,000 | 20% | $29 | 3 | -- |\n"
    "| Programmatic | $21,721 | 36.2% | $8.28 | 14 | -- |\n"
)


def _scripted_llm(calls, seen_exclusions, engine_):
    """A model that asks for the projection AND the channel recommender, then
    answers with figures that are not the engine's."""
    real_execute = nova.Nova.execute_tool

    def spy(name, params):
        seen_exclusions.append((name, nova._turn_exclusions()))
        return real_execute(engine_, name, params)

    def fake_call_llm(*a, **kw):
        msgs = kw.get("messages") or (a[0] if a else [])
        calls.append(bool(kw.get("tools")))
        if kw.get("tools"):
            if not any(m.get("role") == "tool" for m in msgs):
                return {
                    "provider": "stub",
                    "model": "stub",
                    "text": "",
                    "raw_message": {"content": None},
                    "tool_calls": [
                        {
                            "id": "t1",
                            "type": "function",
                            "function": {
                                "name": "query_budget_projection",
                                "arguments": json.dumps(
                                    {
                                        "budget": 60000,
                                        "roles": ["Customer Service Representative"],
                                        "locations": ["Chicago, IL"],
                                    }
                                ),
                            },
                        },
                        {
                            "id": "t2",
                            "type": "function",
                            "function": {
                                "name": "recommend_channels",
                                "arguments": json.dumps(
                                    {
                                        "industry": "general",
                                        "budget": 60000,
                                        "role": "Customer Service Representative",
                                    }
                                ),
                            },
                        },
                    ],
                }
            return {"provider": "stub", "model": "stub", "text": WRONG_ANSWER}
        return {"provider": "stub", "model": "stub", "text": "{}"}

    return spy, fake_call_llm


@pytest.fixture
def isolated_chat(monkeypatch, tmp_path):
    import llm_router

    monkeypatch.setattr(nova, "_response_cache", {})
    monkeypatch.setattr(nova, "RESPONSE_CACHE_FILE", tmp_path / "resp.json")
    monkeypatch.setattr(nova, "_upstash_enabled", False)
    monkeypatch.setattr(nova, "_intelligent_cache_get", lambda *a, **k: None)
    monkeypatch.setattr(nova, "_intelligent_cache_set", lambda *a, **k: False)
    return llm_router


def test_end_to_end_reported_figures_are_corrected_and_the_cache_replays_the_corrected_answer(
    engine, isolated_chat, monkeypatch
):
    import uuid

    llm_router = isolated_chat
    calls, seen = [], []
    spy, fake = _scripted_llm(calls, seen, engine)
    monkeypatch.setattr(
        engine, "execute_tool", spy
    )  # on the instance: an earlier test may have shadowed the class
    monkeypatch.setattr(llm_router, "call_llm", fake)
    message = E2E_MESSAGE.format(tag=uuid.uuid4().hex[:8])

    out = engine.chat(message, [], None)
    text = out["response"]
    est = _estimate("", "Customer Service Representative", "Chicago, IL")["projection"][
        "text"
    ]

    # the plan figures are the generator's, not the model's
    for kind in ("applications", "hires", "cpa", "cph"):
        assert est[kind] in text, kind
    for gone in ("900 applications", "14 hires", "$66.67", "$4,286"):
        assert gone not in text
    assert "corrected to Nova's planning engine" in text
    assert "no client hire history" in text
    # the excluded boards are gone from the reply (said once, as the disclosure)
    body = text.split("*Excluded at client request")[0]
    assert "Indeed" not in body and "ZipRecruiter" not in body
    assert "Excluded at client request: Indeed, ZipRecruiter" in text
    # every tool ran in a pooled worker thread and still saw the exclusions
    assert seen and all(ex == ["Indeed", "ZipRecruiter"] for _, ex in seen), seen

    # a second asker is served the CORRECTED answer, not the model's raw text
    calls.clear()
    again = engine.chat(message, [], None)
    assert not calls, "the second request must be served from the cache"
    assert est["hires"] in again["response"] and "14 hires" not in again["response"]
    assert "Indeed" not in again["response"].split("*Excluded at client request")[0]
