"""Demand-first planning: the inverse of project_plan.

Generic fixtures only (made-up sites, no client or company names -- the repo is
public).  The engine under test is ``demand_planning``; everything it returns
must agree with ``plan_projection.project_plan``, which stays the one
projection engine.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import demand_planning as dp  # noqa: E402
import plan_projection as pp  # noqa: E402

BRIEF = {
    "budget": 100_000,
    "roles": ["Warehouse Associate"],
    "locations": ["Dallas, TX", "Houston, TX"],
    "industry": "logistics",
}
NON_US_BRIEF = dict(BRIEF, locations=["London, UK"])


@pytest.fixture(autouse=True)
def _quiet_engine():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def projector_for(brief):
    return lambda b: pp.project_plan(dict(brief, budget=b))


# ---------------------------------------------------------------------------
# D1: inversion consistency
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("goal", [1, 7, 120, 800, 4000])
def test_hires_inversion_is_minimal_and_consistent(goal):
    proj = projector_for(BRIEF)
    req = dp.required_budget(goal, "hires", proj, hint=100_000)
    assert req["ok"] and req["low"] <= req["high"]
    # low end: the optimistic end of the projection reaches the goal ...
    assert proj(req["low"])["hires"]["high"] >= goal
    # ... and one planning step below does not (it is the smallest quoted budget)
    assert proj(req["low"] - req["step_low"])["hires"]["high"] < goal
    # high end: the conservative end reaches it, minimally
    assert proj(req["high"])["hires"]["low"] >= goal
    assert proj(req["high"] - req["step_high"])["hires"]["low"] < goal


def test_apply_start_inversion_is_minimal_and_consistent():
    proj = projector_for(BRIEF)
    goal = 20_000
    req = dp.required_budget(goal, "apply_starts", proj, hint=100_000)
    assert req["ok"] and req["low"] <= req["high"]
    assert proj(req["low"])["applications"]["high"] >= goal
    assert proj(req["low"] - req["step_low"])["applications"]["high"] < goal
    assert proj(req["high"])["applications"]["low"] >= goal
    assert proj(req["high"] - req["step_high"])["applications"]["low"] < goal


def test_required_budget_is_monotone_in_the_goal():
    proj = projector_for(BRIEF)
    lows = [
        dp.required_budget(g, "hires", proj, hint=50_000)["low"] for g in (10, 50, 200)
    ]
    assert lows == sorted(lows) and len(set(lows)) == 3


def test_hint_does_not_change_the_answer():
    proj = projector_for(BRIEF)
    a = dp.required_budget(100, "hires", proj, hint=0)
    b = dp.required_budget(100, "hires", proj, hint=10_000_000)
    assert (a["low"], a["high"]) == (b["low"], b["high"])


def test_inversion_is_cheap_and_quiet():
    calls = []

    def counting(b):
        calls.append(b)
        return pp.project_plan(dict(BRIEF, budget=b))

    records = []

    class _H(logging.Handler):
        def emit(self, record):
            records.append(record)

    h = _H()
    lg = logging.getLogger("budget_engine")
    lg.addHandler(h)
    try:
        logging.disable(logging.NOTSET)
        lg.setLevel(logging.INFO)
        dp.required_budget(250, "hires", counting, hint=100_000)
    finally:
        lg.removeHandler(h)
    assert len(calls) < 250  # a coarse grid plus a bisection, not a dollar scan
    assert records == []  # the search mutes its own engine log lines


def _stub(fn):
    """A projector whose hires and applications both follow ``fn(budget)``
    (low end = 60% of it) -- a stand-in engine with a shape we control."""

    def proj(b):
        v = fn(b)
        rng = {"low": int(v * 0.6), "mid": int(v * 0.8), "high": int(v)}
        return {"ok": True, "hires": dict(rng), "applications": dict(rng)}

    return proj


def _dipping(b):
    # count = budget / 100, but a band of budgets projects 20% fewer (a channel
    # floor changing): $5,200-$6,199 falls back below what $5,000 reached
    n = b / 100.0
    return n * 0.8 if 5200 <= b < 6200 else n


def test_dipping_projection_quotes_the_lowest_budget_it_stays_above():
    proj = _stub(_dipping)
    req = dp.required_budget(50, "hires", proj, hint=5000)
    assert req["ok"]
    # $5,000 reaches 50 (optimistic end) -- but $5,200 does not, so the quote is
    # the first budget from which everything above also reaches it
    assert proj(5000)["hires"]["high"] >= 50 > proj(5300)["hires"]["high"]
    assert 6200 <= req["low"] <= 6200 + req["step_low"]
    assert req["first_reach_low"] <= 5000 + 1
    assert req["non_monotone"] is True
    # the guarantee: nothing above the quoted low falls below the goal
    for b in range(int(req["low"]), int(req["low"] * 3), 10):
        assert proj(b)["hires"]["high"] >= 50, b
    # the conservative end (60%) needs a bigger budget and also never dips below
    for b in range(int(req["high"]), int(req["high"] * 3), 10):
        assert proj(b)["hires"]["low"] >= 50, b


def test_dip_is_stated_in_the_goal_plan():
    plan = dp.build_goal_plan(goal=50, budget=3000, projector=_stub(_dipping))
    assert any("not smooth" in n and "already reaches" in n for n in plan["notes"])


def test_smooth_projection_is_not_flagged():
    req = dp.required_budget(50, "hires", _stub(lambda b: b / 100.0), hint=1000)
    assert req["non_monotone"] is False
    assert req["low"] == req["first_reach_low"] or abs(
        req["low"] - req["first_reach_low"]
    ) <= req["step_low"]


@pytest.mark.parametrize(
    "location,goals",
    [
        ("London, UK", [1000, 1840, 1860]),
        ("Columbus, OH", [700, 1690, 520]),
    ],
)
def test_real_apply_start_projection_never_dips_below_the_quote(location, goals):
    brief = dict(BRIEF, locations=[location], industry="logistics_supply_chain")
    proj = projector_for(brief)
    for goal in goals:
        req = dp.required_budget(goal, "apply_starts", proj, hint=10_000)
        assert req["ok"], (location, goal)
        for b in range(int(req["low"]), int(req["low"] * 1.5), 25):
            assert proj(b)["applications"]["high"] >= goal, (location, goal, b)
        for b in range(int(req["high"]), int(req["high"] * 1.5), 100):
            assert proj(b)["applications"]["low"] >= goal, (location, goal, b)
        # and it is the smallest such budget, to within a planning step
        below = req["low"] - req["step_low"]
        assert proj(below)["applications"]["high"] < goal or any(
            proj(b)["applications"]["high"] < goal
            for b in range(int(below), int(req["low"] * 1.5), 25)
        )


def test_bad_goal_is_refused():
    proj = projector_for(BRIEF)
    assert not dp.required_budget(0, "hires", proj)["ok"]
    assert not dp.required_budget(5, "weeks", proj)["ok"]


def test_hires_suppressed_market_cannot_price_a_hires_goal():
    proj = projector_for(NON_US_BRIEF)
    assert proj(100_000)["assumptions"]["hires_suppressed"] is True
    req = dp.required_budget(100, "hires", proj)
    assert not req["ok"] and req["status"] == "not_projected"
    plan = dp.build_goal_plan(goal=100, budget=100_000, projector=proj)
    assert plan["ok"] and plan["status"] == "not_projected"
    assert plan["options"] == []
    assert "cost-per-hire benchmark" in plan["text"]["callout"]
    assert "hires goal" in plan["text"]["callout"]


def test_hires_suppressed_market_still_plans_in_apply_starts():
    proj = projector_for(NON_US_BRIEF)
    plan = dp.build_goal_plan(
        goal=5_000, budget=100_000, projector=proj, kind="apply_starts"
    )
    assert plan["ok"] and plan["unit"] == "apply starts"
    assert plan["required"]["low"] and plan["required"]["high"]
    assert [o["key"] for o in plan["options"]] == ["A", "B", "C"]
    assert "hires" not in plan["text"]["callout"].replace("apply starts", "")


# ---------------------------------------------------------------------------
# D1: shortfall + options A / B / C
# ---------------------------------------------------------------------------
def test_short_plan_options_are_sane():
    proj = projector_for(BRIEF)
    plan = dp.build_goal_plan(goal=300, budget=100_000, projector=proj, weeks=10)
    assert plan["status"] == "short"
    a, b, c = plan["options"]
    here = proj(100_000)["hires"]
    # B is exactly what the actual budget buys
    assert (b["delivers_low"], b["delivers_high"]) == (here["low"], here["high"])
    assert b["shortfall_low"] == 300 - here["high"]
    assert b["shortfall_high"] == 300 - here["low"]
    # A is the required range and the $ gap is range - budget
    assert (a["budget_low"], a["budget_high"]) == (
        plan["required"]["low"],
        plan["required"]["high"],
    )
    assert a["extra_budget_low"] == a["budget_low"] - 100_000
    assert a["budget_low"] > 100_000
    # C: same weekly spend, longer window; at the original deadline the
    # shortfall is stated and unchanged
    assert c["kind"] == "extend_deadline"
    assert c["weeks_low"] == pytest.approx(10 * a["budget_low"] / 100_000)
    assert c["weeks_high"] == pytest.approx(10 * a["budget_high"] / 100_000)
    assert c["shortfall_low"] == b["shortfall_low"]
    assert "short" in c["summary"]


def test_covered_goal_has_no_gap():
    proj = projector_for(BRIEF)
    plan = dp.build_goal_plan(goal=5, budget=100_000, projector=proj, weeks=10)
    assert plan["status"] == "meets"
    assert plan["shortfall"]["low"] == 0 == plan["shortfall"]["high"]
    assert plan["shortfall"]["budget_gap_high"] == 0
    assert "Not needed" in plan["options"][2]["summary"]
    assert "covered" in plan["text"]["callout"]


def test_goal_between_the_two_ends_is_borderline():
    proj = projector_for(BRIEF)
    here = proj(100_000)["hires"]
    goal = (here["low"] + here["high"]) // 2 + 1
    assert here["low"] < goal <= here["high"]
    plan = dp.build_goal_plan(goal=goal, budget=100_000, projector=proj)
    assert plan["status"] == "borderline"
    assert plan["shortfall"]["low"] == 0 < plan["shortfall"]["high"]


def test_no_campaign_length_gives_a_second_flight_not_an_invented_deadline():
    proj = projector_for(BRIEF)
    plan = dp.build_goal_plan(goal=300, budget=100_000, projector=proj, weeks=None)
    c = plan["options"][2]
    assert c["kind"] == "second_flight" and "weeks_low" not in c
    assert "No campaign length was stated" in c["summary"]


def test_goal_plan_never_raises_on_bad_input():
    proj = projector_for(BRIEF)
    assert not dp.build_goal_plan(goal=0, budget=1, projector=proj)["ok"]
    assert not dp.build_goal_plan(goal=5, budget=-1, projector=proj)["ok"]

    def boom(_b):
        raise RuntimeError("engine down")

    out = dp.build_goal_plan(goal=5, budget=100, projector=boom)
    assert out["ok"] is False


def test_per_site_table_sums_to_the_plan():
    proj = projector_for(BRIEF)
    goals = {"Dallas, TX": 200, "Houston, TX": 100}
    weights, _ = dp.site_weights(["Dallas, TX", "Houston, TX"], goals)
    total = sum(weights)
    shares = [w / total for w in weights]
    plan = dp.build_goal_plan(
        goal=300,
        budget=100_000,
        projector=proj,
        sites=["Dallas, TX", "Houston, TX"],
        shares=shares,
        site_goals=goals,
    )
    rows = plan["sites"]
    assert [r["goal"] for r in rows] == [200, 100]
    assert sum(r["budget"] for r in rows) == pytest.approx(100_000)
    assert sum(r["projected_low"] for r in rows) == pytest.approx(
        plan["projected"]["low"]
    )
    assert sum(r["goal"] for r in rows) == plan["goal"]


# ---------------------------------------------------------------------------
# D2: site goals
# ---------------------------------------------------------------------------
def test_site_weights_follow_the_goals():
    sites = ["Dallas, TX", "Houston, TX", "Austin, TX"]
    w, basis = dp.site_weights(sites, {"Dallas, TX": 300, "houston": 100})
    assert w[0] == 300 and w[1] == 100 and w[2] == 0
    assert "site hiring goals" in basis and "no spend" in basis


def test_site_weights_spread_an_unassigned_remainder_by_fallback():
    sites = ["Dallas, TX", "Houston, TX", "Austin, TX"]
    w, basis = dp.site_weights(
        sites, {"Dallas, TX": 100}, total_goal=400, fallback=[1.0, 3.0, 1.0]
    )
    assert w[0] == 100
    assert w[1] == pytest.approx(225) and w[2] == pytest.approx(75)
    assert sum(w) == pytest.approx(400)
    assert "location weight" in basis


def test_site_weights_empty_when_nothing_matches():
    assert dp.site_weights(["Dallas, TX"], {"Elsewhere": 50}) == ([], "")
    assert dp.site_weights(["Dallas, TX"], {}) == ([], "")


@pytest.mark.parametrize(
    "bad",
    [
        [1, 2],
        {"": 5},
        {"Dallas": "many"},
        {"Dallas": 1.5},
        {"Dallas": -3},
        {"Dallas": True},
        {"Dallas": float("nan")},
        {"Dallas": 100_001},
        {"Dallas": 60_000, "Houston": 60_000},
        {"x" * 121: 5},
        {f"Site {i}": 1 for i in range(41)},
    ],
)
def test_site_goals_validation_rejects(bad):
    code, msg = dp.site_goals_problem(bad)
    assert code and msg


@pytest.mark.parametrize(
    "ok", [None, "", {}, {"Dallas, TX": 250}, {"Dallas": "250", "Houston": 3.0}]
)
def test_site_goals_validation_accepts(ok):
    assert dp.site_goals_problem(ok) == ("", "")


def test_clean_site_goals_canonicalises():
    out = dp.clean_site_goals(
        {" Dallas,  TX ": "250", "dallas, tx": 5, "Houston": 0, "Austin": 1.0}
    )
    assert out == {"Dallas, TX": 255, "Austin": 1}


# ---------------------------------------------------------------------------
# Public-repo hygiene
# ---------------------------------------------------------------------------
_BANNED_SHA256 = {
    "cbc62794911ff31b2864ecd3dbbbee7ebcb7ea41c5a42e2cba377f3cfdb42811",
    "77909277a1ffc4b06a39d95f00d94b495a5c099351155c60d552c4dc8822cf9a",
    "800a7de021cad393effb24d5817c31d10361c56c1131d73672813f2312a35a88",
    "222f930b87524636da58b1987eabd52f1285cb0ef7dad72f28fcd026fbb3f73f",
    "adccfa16b4fc2c779ad3c361d448aedddb000cd45ab3f91bef0bc4de706306c9",
    "38816a4bbe78aa3dbff468dc0d08c217df8cc3b4f3c3fd9f139bab84d69dff5c",
}


def test_no_client_names_in_new_files():
    files = [
        PROJECT_ROOT / "demand_planning.py",
        PROJECT_ROOT / "tests" / "test_demand_planning.py",
    ]
    files += sorted((PROJECT_ROOT / "tests").glob("test_demand_*.py"))
    bad = []
    for path in dict.fromkeys(files):
        for word in set(re.findall(r"[A-Za-z0-9]+", path.read_text("utf-8").lower())):
            if hashlib.sha256(word.encode()).hexdigest() in _BANNED_SHA256:
                bad.append((path.name, "<redacted>"))
    assert not bad, bad
