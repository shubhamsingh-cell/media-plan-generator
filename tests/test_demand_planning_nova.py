"""Nova's projection tool plans backward from a goal with demand_planning
(never a free-text "goal x cost per hire" guess).

Generic fixtures only; the repo is public.
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import demand_planning as dp  # noqa: E402
import nova  # noqa: E402
import plan_projection as pp  # noqa: E402


@pytest.fixture(autouse=True)
def _quiet():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def _nova() -> "nova.Nova":
    n = nova.Nova.__new__(nova.Nova)
    n._data_cache = {}
    return n


BASE = {
    "roles": ["Warehouse Associate"],
    "locations": ["Dallas, TX"],
    "industry": "logistics_supply_chain",
}


def test_goal_without_budget_solves_the_budget_from_the_engine():
    out = _nova()._query_budget_projection(dict(BASE, target_hires=200))
    assert "error" not in out
    req = out["required_budget"]
    assert req["low"] < req["high"]
    assert out["budget_source"] == "solved_for_goal"
    assert out["total_budget"] == req["high"]
    # the SAME inversion a caller can run on project_plan directly
    brief = {
        "roles": BASE["roles"],
        "locations": BASE["locations"],
        "industry": BASE["industry"],
        "budget_text": "$1",
    }
    direct = dp.required_budget(
        200,
        "hires",
        lambda b: pp.project_plan(dict(brief, budget=b), plan_currency="USD"),
    )
    assert (req["low"], req["high"]) == (direct["low"], direct["high"])
    # the projection printed at that budget reaches the goal conservatively
    assert out["projection"]["hires"]["low"] >= 200
    assert out["goal_plan"]["solved"] is True and out["goal_plan"]["options"] == []
    assert "needs" in out["goal_plan"]["text"]["callout"]
    # the model is told to quote the range, not to compute one
    assert "Do NOT compute a budget yourself" in out["presentation_note"]


def test_goal_with_budget_returns_the_goal_plan_and_keeps_the_projection():
    plain = _nova()._query_budget_projection(dict(BASE, budget=100_000))
    out = _nova()._query_budget_projection(
        dict(BASE, budget=100_000, target_hires=300)
    )
    gp = out["goal_plan"]
    assert gp["status"] == "short" and gp["goal"] == 300
    assert [o["key"] for o in gp["options"]] == ["A", "B", "C"]
    assert out["projection"]["hires"] == plain["projection"]["hires"]
    assert "required_budget" not in out  # a budget was given: nothing solved
    assert out["total_budget"] == 100_000


def test_no_goal_no_budget_is_still_an_error_and_plain_calls_are_unchanged():
    assert "error" in _nova()._query_budget_projection(dict(BASE))
    plain = _nova()._query_budget_projection(dict(BASE, budget=60_000))
    assert "goal_plan" not in plain and "required_budget" not in plain


def test_apply_start_goal_without_budget():
    out = _nova()._query_budget_projection(dict(BASE, target_apply_starts=5_000))
    assert out["goal_plan"]["kind"] == "apply_starts"
    assert out["projection"]["applications"]["low"] >= 5_000
    assert out["required_budget"]["low"] > 0


def test_unpriceable_hires_goal_declares_and_does_not_estimate():
    out = _nova()._query_budget_projection(
        {
            "roles": ["Warehouse Associate"],
            "locations": ["London, UK"],
            "industry": "logistics_supply_chain",
            "target_hires": 200,
        }
    )
    assert out["goal_plan"]["status"] == "not_projected"
    assert "required_budget" not in out
    assert "cost-per-hire benchmark" in out["goal_plan"]["text"]["callout"]
    assert "Do NOT estimate a budget" in out["presentation_note"]


def test_goal_cap():
    assert "exceeds" in _nova()._query_budget_projection(
        dict(BASE, target_hires=100_001)
    ).get("error", "")


def test_tool_schema_and_prompt_no_longer_tell_the_model_to_guess():
    tool = next(
        t
        for t in _all_tools()
        if t.get("name") == "query_budget_projection"
    )
    schema = tool["input_schema"]
    assert schema["required"] == []
    assert "target_apply_starts" in schema["properties"]
    assert "OMIT budget" in tool["description"]
    prompt = Path(PROJECT_ROOT / "nova.py").read_text(encoding="utf-8")
    assert "budget = target_hires x industry_avg_CPH" not in prompt
    assert "call query_budget_projection with target_hires and NO budget" in prompt


def _all_tools() -> list:
    return list(_nova().get_tool_definitions())


def test_rule_based_chat_solves_instead_of_asking_for_a_budget():
    n = _nova()
    resp = n._chat_rule_based(
        "What budget do I need to hire 200 warehouse workers in Dallas?"
    )["response"]
    assert "I need a few more details" not in resp
    assert resp.startswith("*Budget needed for the goal: $")
    assert "Goal plan" in resp and "Current constraint" not in resp
    assert re.search(r"needs \$[\d,]+.\$[\d,]+", resp)


def test_rule_based_chat_with_a_budget_adds_the_goal_plan():
    n = _nova()
    resp = n._chat_rule_based(
        "How should I allocate a $100K budget to hire 300 warehouse workers in Dallas?"
    )["response"]
    assert "Goal plan" in resp and "Current constraint" in resp
