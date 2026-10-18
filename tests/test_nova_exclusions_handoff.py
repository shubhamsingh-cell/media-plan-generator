"""Excluding job boards in Nova chat: honoured by the answer AND carried to the wizard.

A custom request such as excluding specific job boards was not honoured end to
end in Nova. The wizard has an ``excluded_publishers`` field and Nova's two
supply tools filter on it, but (found 2026-10-18):

* nothing read "excluding Indeed and ZipRecruiter" out of the message: the LLM had
  to remember to pass ``excluded_publishers`` to a tool, every time;
* ``query_channels``, ``recommend_channels`` and ``optimize_campaign`` had no
  exclusion support at all (the recommender listed "Indeed Sponsored Jobs" and
  "ZipRecruiter Sponsored" as MUST-HAVE for a client who had just excluded them);
* the final answer was never swept, and the "Build the full media plan" handoff
  dropped the exclusions, so the wizard opened with the field empty.
"""

from __future__ import annotations

import json
import re
import sys
import types
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

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

import nova  # noqa: E402
import nova_handoff  # noqa: E402
import publisher_filter  # noqa: E402


@pytest.fixture(scope="module")
def engine():
    return nova.Nova()


@pytest.fixture(autouse=True)
def _clean_turn_state():
    yield
    nova._set_turn_tool_capture(None)


def _turn(excluded=None):
    nova._set_turn_tool_capture(
        {
            "conversation_id": "",
            "tools": {},
            "tools_used": [],
            "stateless": True,
            "excluded_publishers": list(excluded or []),
        }
    )


# ---------------------------------------------------------------------------
# Reading the exclusion out of the message
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "message,expected",
    [
        (
            "make a media plan for 50 nurses in Boston excluding Indeed and ZipRecruiter",
            ["Indeed", "ZipRecruiter"],
        ),
        ("plan this without indeed.com", ["indeed.com"]),
        (
            "Exclude Indeed, Monster & Glassdoor. Then build the plan.",
            ["Indeed", "Monster", "Glassdoor"],
        ),
        ("we don't use Indeed or Monster for this client", ["Indeed", "Monster"]),
        ("leave out CDLlife and Craigslist please", ["CDLlife", "Craigslist"]),
        (
            "exclude Indeed and ZipRecruiter (per the client)",
            ["Indeed", "ZipRecruiter"],
        ),
        ("skip Monster (ref 42) and Dice, thanks", ["Monster", "Dice"]),
        # spelling variants of one board are ONE exclusion
        ("excluding Indeed, indeed.com and www.indeed.com", ["Indeed"]),
        # not exclusions
        ("there is no minimum budget threshold for this client", []),
        ("avoid job boards that charge per click", []),
        ("what is the cost per hire excluding agency fees", []),
        ("hire nurses excluding Texas", []),
        ("make a media plan for 50 nurses in Boston", []),
    ],
)
def test_exclusions_are_read_from_the_message(message, expected):
    assert nova_handoff.extract_exclusions(message) == expected


def test_exclusions_accumulate_across_the_conversation():
    history = [
        {"role": "user", "content": "plan for 20 drivers in Dallas, exclude Indeed"},
        {"role": "assistant", "content": "Sure."},
    ]
    assert nova_handoff.extract_exclusions("also leave out ZipRecruiter", history) == [
        "ZipRecruiter",
        "Indeed",
    ]


def test_the_matcher_treats_spelling_variants_as_the_same_board():
    data = {
        "excluded_publishers": nova_handoff.extract_exclusions("without indeed.com")
    }
    for spelling in ("Indeed", "indeed.com", "INDEED", "Indeed Sponsored Jobs"):
        assert publisher_filter.is_excluded(spelling, data), spelling
    assert not publisher_filter.is_excluded("Reed", data)


# ---------------------------------------------------------------------------
# Handoff to the wizard
# ---------------------------------------------------------------------------
def _handoff(message, history=None):
    result = {
        "response": "### Media Plan\n\n| Channel | Budget | % |\n|---|---|---|\n| X | $1 | 1% |\n"
        + ("Details. " * 20)
    }
    return nova_handoff.build_media_plan_handoff(
        message, history or [], None, result, "media_plan", {}
    )


def test_handoff_carries_the_exclusions_into_the_wizard_url():
    h = _handoff(
        "make a media plan for 50 nurses in Boston excluding Indeed and ZipRecruiter"
    )
    assert h["params"]["excluded_publishers"] == ["Indeed", "ZipRecruiter"]
    qs = parse_qs(urlsplit(h["url"]).query)
    assert qs["excluded_publishers"] == ["Indeed; ZipRecruiter"]
    # every other field still parses
    assert qs["roles"] == ["Nurse"] or qs["roles"][0].lower().startswith("nurse")


def test_handoff_without_exclusions_emits_no_exclusion_param():
    h = _handoff("make a media plan for 50 nurses in Boston")
    assert "excluded_publishers" not in h["params"]
    assert "excluded_publishers" not in h["url"]


def test_handoff_dedupes_spelling_variants():
    h = _handoff("plan for nurses in Boston excluding Indeed and indeed.com")
    assert h["params"]["excluded_publishers"] == ["Indeed"]


def test_handoff_budget_is_the_campaign_total_when_a_period_is_stated():
    h = _handoff(
        "media plan for 30 nurses in Boston, $20k a month for a 3 month campaign"
    )
    assert h["params"]["budget"] == 60_000
    assert h["params"]["timeline"] == "3 months"


@pytest.mark.parametrize(
    "message,period",
    [
        ("$20k a month for 3 months", "monthly"),
        ("a monthly budget of $20k", "monthly"),
        ("$5k per quarter", "quarterly"),
        ("$100k/year", "annual"),
        ("a $60k, 3-month campaign", None),
        ("plan for $60k and send monthly reports to the team afterwards", None),
    ],
)
def test_budget_period_is_read_only_from_next_to_the_amount(message, period):
    assert nova_handoff.parse_budget_period(message) == period


def test_handoff_drops_a_per_period_budget_it_cannot_resolve():
    """A monthly figure with no campaign length would be read by the wizard as a
    campaign TOTAL (3x too small): better to leave the field for the user."""
    h = _handoff("media plan for 30 nurses in Boston, $20k a month")
    assert "budget" not in h["params"]


# ---------------------------------------------------------------------------
# Tools honour the turn's exclusions without the model having to remember
# ---------------------------------------------------------------------------
def _without_disclosure(out):
    """A tool result minus the keys that NAME what was excluded (the disclosure)."""
    return {
        k: v
        for k, v in out.items()
        if k not in ("excluded_at_client_request", "exclusion_note")
    }


def test_execute_tool_injects_the_turns_exclusions_into_supply_tools(engine):
    _turn(["indeed.com"])
    out = json.loads(engine.execute_tool("query_publishers", {"search_term": "indeed"}))
    assert out["match_count"] == 0 and out["search_results"] == []


def test_query_channels_removes_excluded_boards(engine):
    base = json.dumps(
        engine._query_channels({"industry": "healthcare_medical"}), default=str
    )
    assert "Indeed" in base  # the premise: it was listed before
    _turn(["Indeed"])
    out = json.loads(
        engine.execute_tool("query_channels", {"industry": "healthcare_medical"})
    )
    assert "Indeed" not in json.dumps(_without_disclosure(out))


def test_recommend_channels_never_recommends_an_excluded_platform(engine):
    _turn(["Indeed", "ZipRecruiter"])
    out = json.loads(
        engine.execute_tool(
            "recommend_channels", {"industry": "healthcare", "budget": 60000}
        )
    )
    platforms = [
        c["platform"]
        for tier in ("must_have", "should_have", "test_and_learn")
        for c in out.get(tier, [])
    ]
    assert platforms and not [
        p for p in platforms if "Indeed" in p or "ZipRecruiter" in p
    ]
    blob = json.dumps(_without_disclosure(out))
    assert "Indeed" not in blob and "ZipRecruiter" not in blob
    # the allocation still sums to the whole budget
    spend = sum(
        c["projected_spend"] for tier in ("must_have", "should_have") for c in out[tier]
    )
    assert abs(spend - 60000) < 5


def test_optimize_campaign_redistributes_the_budget_without_excluded_boards(engine):
    _turn(["Indeed", "ZipRecruiter"])
    out = json.loads(
        engine.execute_tool(
            "optimize_campaign",
            {"budget": 60000, "industry": "healthcare", "role": "Registered Nurse"},
        )
    )
    alloc = out["recommended_allocation"]
    assert alloc and not [k for k in alloc if k in ("indeed", "ziprecruiter")]
    assert abs(sum(c["budget"] for c in alloc.values()) - 60000) < 5


def test_exclusions_given_by_the_model_are_merged_with_the_turns(engine):
    _turn(["Indeed"])
    out = json.loads(
        engine.execute_tool(
            "query_publishers",
            {"search_term": "ziprecruiter", "excluded_publishers": ["ZipRecruiter"]},
        )
    )
    assert out["match_count"] == 0


# ---------------------------------------------------------------------------
# The reply itself never recommends an excluded board
# ---------------------------------------------------------------------------
def test_chat_boundary_sweeps_a_recommendation_of_an_excluded_board():
    seen = {}

    @nova._honour_turn_exclusions
    def fake_chat(self, user_message, conversation_history=None, *a, **k):
        seen["state"] = dict(nova._get_turn_tool_capture() or {})
        return {
            "response": (
                "Start with Indeed and Reed for volume. "
                "Reed is strongest for UK hourly roles. "
                "Budget is $60,000."
            ),
            "sources": [],
        }

    out = fake_chat(None, "plan for 50 drivers in London, exclude Indeed")
    body, _, disclosure = out["response"].partition("\n\n*Excluded")
    assert "Indeed" not in body, "the recommendation is gone"
    assert "Reed" in body and "$60,000" in body
    assert disclosure.startswith(" at client request: Indeed")  # said once, on purpose


def test_chat_boundary_leaves_replies_alone_when_nothing_was_excluded():
    @nova._honour_turn_exclusions
    def fake_chat(self, user_message, conversation_history=None, *a, **k):
        return {"response": "Start with Indeed and Reed for volume.", "sources": []}

    out = fake_chat(None, "plan for 50 drivers in London")
    assert out["response"] == "Start with Indeed and Reed for volume."


def test_the_turn_state_created_by_chat_holds_the_exclusions():
    state = nova._new_turn_state(
        "plan nurses excluding Indeed",
        [{"role": "user", "content": "also skip Monster"}],
        session_id="abc",
        stateless=False,
    )
    assert state["excluded_publishers"] == ["Indeed", "Monster"]


# ---------------------------------------------------------------------------
# Parsing stays linear on hostile input; the wizard half of the handoff
# ---------------------------------------------------------------------------
def test_budget_and_hire_parsing_is_linear_on_long_number_runs():
    """A 4,000-character message of '1,1,1,...' used to cost ~0.2s per parse
    (quadratic backtracking in the budget regexes); every plan turn parses."""
    import time

    text = "1," * 2000 + " hires"
    start = time.perf_counter()
    nova_handoff.parse_budget(text)
    nova_handoff.parse_hires(text, [])
    nova_handoff.campaign_budget_total(text)
    assert time.perf_counter() - start < 0.1


@pytest.mark.xfail(
    strict=False,
    reason=(
        "wizard half pending: _applyUrlParamPrefill in templates/partials/index/"
        "body_app_js.html does not yet read the excluded_publishers query param "
        "Nova's handoff now emits"
    ),
)
def test_wizard_prefill_reads_the_exclusions_the_handoff_emits():
    body = (
        REPO_ROOT / "templates" / "partials" / "index" / "body_app_js.html"
    ).read_text(encoding="utf-8")
    assert 'params.get("excluded_publishers")' in body
