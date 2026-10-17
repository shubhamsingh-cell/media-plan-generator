"""Demand-first outputs: site goals drive allocation, the Goal Plan sheet and
deck slide, the /api/estimate ``goal_plan`` and its request boundary.

Generic fixtures only (made-up client, Texas sites): the repo is public.
"""

from __future__ import annotations

import copy
import io
import logging
import subprocess
import sys
import tempfile
from pathlib import Path

import openpyxl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import app  # noqa: E402
import demand_planning as dp  # noqa: E402
import enterprise_pack as ep  # noqa: E402
import plan_projection as pp  # noqa: E402
from _xl_eval import Evaluator  # noqa: E402
from test_enterprise_pack import (  # noqa: E402,F401
    TODAY,
    _fixed_today,
    build_wb,
    find_row,
    make_plan,
    sheet_values,
)

GOALS = {"Dallas": 150, "Houston": 100, "Austin": 50}


@pytest.fixture(autouse=True)
def _quiet():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def with_goal_plan(data: dict, budget: float = 150_000) -> dict:
    """Stamp the goal plan exactly as app._attach_headline_projection does."""
    ep.normalize_request(data)
    brief = {
        "budget": budget,
        "roles": ["Warehouse Associate"],
        "locations": data["locations"],
        "industry": "logistics",
    }
    gp = ep.compute_goal_plan(
        data,
        lambda b: pp.project_plan(dict(brief, budget=b)),
        budget=budget,
    )
    if gp:
        data["_budget_allocation"]["goal_plan"] = gp
    return data


def goal_plan_plan(**over) -> dict:
    over.setdefault("site_goals", dict(GOALS))
    over.setdefault("hire_volume", "300 hires")
    return with_goal_plan(make_plan(**over))


# ---------------------------------------------------------------------------
# D2: site goals drive allocation
# ---------------------------------------------------------------------------
def test_site_goals_drive_the_shares_instead_of_location_weights():
    plain = ep.build_model(make_plan(), TODAY)
    goal = ep.build_model(goal_plan_plan(), TODAY)
    assert goal["sites"] == plain["sites"] == ["Dallas, TX", "Houston, TX", "Austin, TX"]
    assert goal["shares"] == [0.5, 0.3333, 0.1667]
    assert goal["shares"] != plain["shares"]
    assert sum(goal["shares"]) == pytest.approx(1.0, abs=1e-9)
    assert "site hiring goals" in goal["share_basis"]
    assert goal["hire_goal"] == 300  # the site goals' total is the plan's goal


def test_site_without_a_goal_gets_no_spend_unless_a_goal_remains():
    data = make_plan(site_goals={"Dallas": 100, "Houston": 100})
    ep.normalize_request(data)
    m = ep.build_model(data, TODAY)
    assert m["shares"] == [0.5, 0.5, 0.0]
    assert "no spend" in m["share_basis"]
    # a plan-wide goal larger than the site goals' total is spread over the rest
    data["hire_volume"] = "300 hires"
    # the site goals still win as THE goal (stated_goal) so no remainder: same
    assert ep.stated_goal(data) == 200


def test_pacing_matrix_sums_to_the_plan_and_shows_the_goal_row():
    data = goal_plan_plan()
    wb = build_wb(data, data_only=True)
    ws = wb[ep.SHEET_PACING]
    r = find_row(ws, "Site hiring goal (hires)")
    assert [ws.cell(row=r, column=c).value for c in (3, 4, 5)] == [150, 100, 50]
    share_row = find_row(ws, "Share of each week's spend")
    shares = [ws.cell(row=share_row, column=c).value for c in (3, 4, 5)]
    assert shares == [0.5, 0.3333, 0.1667]
    assert ws.cell(row=share_row, column=6).value == "OK"
    total_row = [
        i for i in range(1, ws.max_row + 1) if ws.cell(row=i, column=2).value == "Total"
    ][-1]
    assert ws.cell(row=total_row, column=6).value == "OK"
    grid = [
        sum(ws.cell(row=total_row, column=c).value for c in (3, 4, 5)),
    ]
    # site columns add to the plan's recommended spend, and the biggest-goal
    # site carries the biggest column
    cols = [ws.cell(row=total_row, column=c).value for c in (3, 4, 5)]
    assert cols[0] > cols[1] > cols[2]
    assert cols[0] == pytest.approx(grid[0] * 0.5, rel=1e-3)


def test_no_goals_plan_is_untouched_by_the_feature():
    base = sheet_values(build_wb(make_plan()))
    # an enabled plan with an EMPTY goals object and a stamped nothing
    same = make_plan(site_goals={})
    ep.normalize_request(same)
    assert "site_goals" not in same
    assert sheet_values(build_wb(same)) == base
    assert ep.SHEET_GOALPLAN not in base


def test_toggle_off_drops_goals_and_matches_a_standard_plan():
    base = sheet_values(build_wb(make_plan(enterprise=False)))
    off = make_plan(enterprise=False, site_goals=dict(GOALS))
    off["_budget_allocation"]["goal_plan"] = {"ok": True, "status": "short"}
    ep.normalize_request(off)
    assert "site_goals" not in off
    assert sheet_values(build_wb(off)) == base
    assert ep.goal_plan_of(off) is None


def test_clean_request_layer_validates_and_caps():
    for bad in ({"Dallas": -1}, {"Dallas": "x"}, {"Dallas": 100_001}, [1]):
        assert ep.request_problem({"site_goals": bad})[0] == "site_goals"
    assert ep.problem_field("site_goals") == "site_goals"
    ok = {"enterprise_pack": True, "site_goals": {" Dallas ": "5", "Houston": 0}}
    assert ep.request_problem(ok) == ("", "")
    ep.normalize_request(ok)
    assert ok["site_goals"] == {"Dallas": 5}


def test_sanitizer_strips_markup_from_site_goal_names():
    clean = app._sanitize_generate_request(
        {"enterprise_pack": True, "site_goals": {"<b>Dallas</b>": 5, "Houston": "7"}}
    )
    assert clean["site_goals"] == {"Dallas": "5", "Houston": "7"}
    ep.normalize_request(clean)
    assert clean["site_goals"] == {"Dallas": 5, "Houston": 7}


# ---------------------------------------------------------------------------
# D3: Goal Plan sheet
# ---------------------------------------------------------------------------
def test_goal_plan_sheet_present_only_with_pack_and_a_goal():
    on = build_wb(goal_plan_plan())
    assert on.sheetnames.index(ep.SHEET_GOALPLAN) < on.sheetnames.index(ep.SHEET_PACING)
    no_goal = build_wb(with_goal_plan(make_plan(hire_volume="")))
    assert ep.SHEET_GOALPLAN not in no_goal.sheetnames
    off = goal_plan_plan()
    off["enterprise_pack"] = False
    assert ep.SHEET_GOALPLAN not in build_wb(off).sheetnames


def test_goal_plan_sheet_contents_and_site_table():
    data = goal_plan_plan()
    gp = data["_budget_allocation"]["goal_plan"]
    ws = build_wb(data, data_only=True)[ep.SHEET_GOALPLAN]
    assert ws.cell(row=find_row(ws, "Goal (hires)"), column=3).value == 300
    assert ws.cell(
        row=find_row(ws, "Required budget, low"), column=3
    ).value == pytest.approx(gp["required"]["low"])
    assert ws.cell(
        row=find_row(ws, "Required budget, high"), column=3
    ).value == pytest.approx(gp["required"]["high"])
    assert ws.cell(row=find_row(ws, "Status"), column=3).value == "SHORT"
    for opt in (
        "A. Full objective",
        "B. Current constraint",
        "C. Extend the deadline (not viable alone)",
    ):
        find_row(ws, opt)
    # per-site goal vs projected
    first = find_row(ws, "Dallas, TX")
    assert [ws.cell(row=first + i, column=3).value for i in range(3)] == [150, 100, 50]
    tot = find_row(ws, "Total", 2)
    assert ws.cell(row=tot, column=3).value == 300
    assert ws.cell(row=tot, column=4).value == pytest.approx(1.0)
    assert ws.cell(row=tot, column=9).value == "OK"


def test_goal_plan_cached_values_equal_what_excel_computes():
    data = goal_plan_plan()
    data_wb = build_wb(data, data_only=True)
    wb = build_wb(data)
    ev = Evaluator(wb)
    ws = wb[ep.SHEET_GOALPLAN]
    checked = 0
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell.value, str) and cell.value.startswith("="):
                want = ev.cell(ep.SHEET_GOALPLAN, cell.coordinate)
                got = data_wb[ep.SHEET_GOALPLAN][cell.coordinate].value
                if isinstance(want, str):
                    assert got == want, (cell.coordinate, cell.value, got, want)
                else:
                    assert got == pytest.approx(float(want), rel=1e-6, abs=1e-6), (
                        cell.coordinate,
                        cell.value,
                        got,
                        want,
                    )
                checked += 1
    assert checked > 25


def test_editing_the_budget_input_recalculates_the_gap():
    wb = build_wb(goal_plan_plan())
    ws = wb[ep.SHEET_GOALPLAN]
    gap_row = find_row(ws, "Budget gap to the goal")
    before = Evaluator(wb).cell(ep.SHEET_GOALPLAN, f"C{gap_row}")
    ws.cell(row=find_row(ws, "Planned budget"), column=3).value += 100_000
    after = Evaluator(wb).cell(ep.SHEET_GOALPLAN, f"C{gap_row}")
    assert after == pytest.approx(before - 100_000)


def test_goal_plan_without_a_priceable_goal_says_so():
    data = goal_plan_plan()
    data["_budget_allocation"]["goal_plan"] = {
        "ok": True,
        "status": "not_projected",
        "unit": "hires",
        "text": {"callout": "No local cost-per-hire benchmark for this market."},
    }
    ws = build_wb(data)[ep.SHEET_GOALPLAN]
    text = " ".join(
        str(c.value) for row in ws.iter_rows() for c in row if c.value is not None
    )
    assert "No local cost-per-hire benchmark" in text
    assert "Required budget" not in text


# ---------------------------------------------------------------------------
# D3: deck slide
# ---------------------------------------------------------------------------
def _slide_texts(prs) -> list:
    out = []
    for slide in prs.slides:
        out.append(
            " | ".join(
                sh.text_frame.text for sh in slide.shapes if getattr(sh, "has_text_frame", False)
            )
        )
    return out


def _deck(data: dict):
    import ppt_generator
    from pptx import Presentation

    blob = ppt_generator.generate_pptx(copy.deepcopy(data))
    return blob, Presentation(io.BytesIO(blob))


def test_deck_gets_one_goal_slide_and_nothing_else_changes():
    with_goal = goal_plan_plan()
    _, prs = _deck(with_goal)
    plain = goal_plan_plan()
    plain["_budget_allocation"].pop("goal_plan")
    _, prs0 = _deck(plain)
    assert len(prs.slides) == len(prs0.slides) + 1
    texts, base = _slide_texts(prs), _slide_texts(prs0)
    assert texts[:-1] == base  # every earlier slide is byte-identical text
    last = texts[-1]
    assert "Goal Plan" in last and "Full objective" in last
    assert "Current constraint" in last and "Extend the deadline" in last
    assert "Not viable alone" in last  # a 300-hire goal on this budget
    assert "50-100 weeks" not in last
    assert with_goal["_budget_allocation"]["goal_plan"]["text"]["callout"] in last
    assert "Dallas, TX" in last


def test_goal_slide_has_no_ink_collisions():
    blob, _ = _deck(goal_plan_plan())
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "goal.pptx"
        path.write_bytes(blob)
        res = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "scripts/deck_qa/collide.py"), str(path)],
            capture_output=True,
            text=True,
            timeout=300,
        )
    assert res.returncode == 0, res.stdout[-800:]
    assert "TOTAL FINDINGS: 0" in res.stdout


def test_toggle_off_deck_text_is_identical_with_goals_and_a_stamped_plan():
    base = _slide_texts(_deck(make_plan(enterprise=False, hire_volume="300 hires"))[1])
    off = make_plan(enterprise=False, site_goals=dict(GOALS), hire_volume="300 hires")
    off["_budget_allocation"]["goal_plan"] = {"ok": True, "status": "short"}
    assert _slide_texts(_deck(off)[1]) == base


# ---------------------------------------------------------------------------
# D3: /api/estimate goal_plan
# ---------------------------------------------------------------------------
BRIEF = {
    "client_name": "Example Client",
    "industry": "logistics",
    "budget_range": "$150,000",
    "campaign_duration": "2 months",
    "target_roles": ["Warehouse Associate"],
    "locations": ["Dallas, TX", "Houston, TX", "Austin, TX"],
}


def test_estimate_carries_goal_plan_only_when_asked():
    plain = app._compute_plan_estimate(dict(BRIEF))
    assert "goal_plan" not in plain
    # a goal without the pack: nothing
    assert "goal_plan" not in app._compute_plan_estimate(
        dict(BRIEF, hire_volume="300 hires")
    )
    # the pack without a goal: nothing
    assert "goal_plan" not in app._compute_plan_estimate(
        dict(BRIEF, enterprise_pack=True)
    )
    got = app._compute_plan_estimate(
        dict(BRIEF, enterprise_pack=True, hire_volume="300 hires")
    )
    gp = got["goal_plan"]
    assert gp["ok"] and gp["goal"] == 300 and gp["kind"] == "hires"
    assert gp["status"] == "short"
    # the inversion agrees with the headline projection the same response carries
    proj = got["projection"]
    assert gp["projected"]["low"] == proj["hires"]["low"]
    assert gp["projected"]["high"] == proj["hires"]["high"]
    assert gp["budget"] == pytest.approx(150_000)
    assert "Goal of 300 hires needs" in gp["text"]["callout"]
    # every other key of the response is untouched by the goal
    without = {k: v for k, v in got.items() if k != "goal_plan"}
    assert without["est_hires"] == plain["est_hires"]
    assert without["projection"]["hires"] == plain["projection"]["hires"]


def test_estimate_goal_plan_is_consistent_with_project_plan():
    got = app._compute_plan_estimate(
        dict(BRIEF, enterprise_pack=True, hire_volume="300 hires")
    )
    gp = got["goal_plan"]
    # re-run the SAME engine at the required budgets
    lo, hi = gp["required"]["low"], gp["required"]["high"]
    redo = lambda b: app._compute_plan_estimate(  # noqa: E731
        dict(BRIEF, budget_range=f"${b:,.0f}")
    )["projection"]["hires"]
    assert redo(lo)["high"] >= 300
    assert redo(hi)["low"] >= 300
    assert redo(lo - 200)["high"] < 300


def test_estimate_site_goals_total_is_the_goal_and_rows_follow_them():
    got = app._compute_plan_estimate(
        dict(
            BRIEF,
            enterprise_pack=True,
            site_goals={"Dallas, TX": 200, "Houston, TX": 100, "Austin, TX": 100},
        )
    )
    gp = got["goal_plan"]
    assert gp["goal"] == 400
    rows = {r["site"]: r for r in gp["sites"]}
    assert rows["Dallas, TX"]["share"] == 0.5
    assert rows["Dallas, TX"]["goal"] == 200
    assert sum(r["share"] for r in gp["sites"]) == pytest.approx(1.0)


@pytest.mark.parametrize(
    "bad",
    [{"Dallas, TX": -5}, {"Dallas, TX": "lots"}, {"Dallas, TX": 100_001}, ["a"]],
)
def test_estimate_validation_names_site_goals(bad):
    with pytest.raises(app._EstimateValidationError) as exc:
        app._compute_plan_estimate(dict(BRIEF, enterprise_pack=True, site_goals=bad))
    assert exc.value.field == "site_goals"


def test_estimate_pack_off_ignores_site_goals_entirely():
    a = app._compute_plan_estimate(dict(BRIEF))
    b = app._compute_plan_estimate(dict(BRIEF, site_goals={"Dallas, TX": 5}))
    assert a == b


def test_estimate_hires_suppressed_market_declares_and_plans_in_apply_starts_only():
    brief = dict(
        BRIEF,
        locations=["London, UK"],
        budget_range="£150,000",
        enterprise_pack=True,
        hire_volume="300 hires",
    )
    gp = app._compute_plan_estimate(brief)["goal_plan"]
    assert gp["status"] == "not_projected" and gp["options"] == []
    assert "cost-per-hire benchmark" in gp["text"]["callout"]


def test_hire_volume_range_is_flagged_as_its_low_end():
    got = app._compute_plan_estimate(
        dict(BRIEF, enterprise_pack=True, hire_volume="100-500 hires")
    )["goal_plan"]
    assert got["goal"] == 100
    assert any("low end" in n for n in got["notes"])


# ---------------------------------------------------------------------------
# generate: the goal plan is stamped on the plan the deliverables read
# ---------------------------------------------------------------------------
def test_headline_stamp_adds_goal_plan_for_an_enabled_goal_plan():
    plan = dict(
        BRIEF,
        enterprise_pack=True,
        hire_volume="300 hires",
        budget="$150,000",
        locations=["Dallas, TX", "Houston, TX"],
        roles=["Warehouse Associate"],
    )
    ep.normalize_request(plan)
    br = {"total_projected": {"hires": 1}, "metadata": {}}
    app._attach_headline_projection(
        br,
        plan,
        budget=150_000.0,
        roles=[],
        locations=[],
        channel_pcts={},
        vendor_availability=None,
        kb=None,
    )
    assert br["goal_plan"]["goal"] == 300 and br["goal_plan"]["ok"]
    off = dict(plan, enterprise_pack=False)
    br2 = {"total_projected": {"hires": 1}, "metadata": {}}
    app._attach_headline_projection(
        br2,
        off,
        budget=150_000.0,
        roles=[],
        locations=[],
        channel_pcts={},
        vendor_availability=None,
        kb=None,
    )
    assert "goal_plan" not in br2
