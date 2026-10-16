"""Review fixes for the enterprise pack: per-market publishers, exact site
shares, Google Slides fallback, real campaign-window weeks, the no-CPA path,
draft persistence and the SLA field error.  Each test failed on 034ab51."""

from __future__ import annotations

import copy
import datetime
import io
import os
import re
import sys
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import budget_engine  # noqa: E402
import enterprise_pack as ep  # noqa: E402
from _xl_eval import Evaluator  # noqa: E402
from test_enterprise_pack import (  # noqa: E402
    TODAY,
    build_deck,
    build_wb,
    find_row,
    make_plan,
    slide_texts,
)

UK_CHANNELS = {"Indeed": 30, "ZipRecruiter": 20, "Reed": 20, "Google Search Ads": 30}


@pytest.fixture(autouse=True)
def _fixed_today(monkeypatch):
    monkeypatch.setattr(ep, "_today", lambda: TODAY)


def sheet_text(ws) -> str:
    return " ".join(str(c.value) for row in ws.iter_rows() for c in row if c.value)


def deck_text(prs, last: int = 3) -> str:
    """Text of the enterprise appendix (the last slides); the rest of the deck
    is the existing builders' business."""
    return " ".join(" ".join(t) for t in slide_texts(prs)[-last:])


def uk_plan(extra_us: bool = False, **kw) -> dict:
    cities = [("London", "United Kingdom"), ("Manchester", "United Kingdom")]
    if extra_us:
        cities = [("Dallas", "United States")] + cities
    roles = [{"title": "Warehouse Associate", "count": 40, "tier": "mid"}]
    locs = [{"city": c, "state": "", "country": k} for c, k in cities]
    alloc = budget_engine.calculate_budget_allocation(
        total_budget=150_000,
        roles=roles,
        locations=locs,
        industry="logistics",
        channel_percentages=UK_CHANNELS,
        collar_type="blue",
        campaign_start_month=4,
    )
    data = make_plan(**kw)
    data["locations"] = [
        f"{c}, UK" if k != "United States" else "Dallas, TX" for c, k in cities
    ]
    data["_budget_allocation"] = alloc
    return data


# -- 1: publishers that do not run in the plan's markets ------------------------
def test_uk_plan_drops_a_us_only_publisher_everywhere():
    plan = uk_plan()
    model = ep.build_model(plan, TODAY)
    assert "ZipRecruiter" not in [c["name"] for c in model["channels"]]
    assert model["dropped_budget"] > 0 and model["dropped_count"] == 1
    wb = build_wb(plan)
    ws = wb[ep.SHEET_TIERS]
    hdr = find_row(ws, "Publisher")
    names = []
    r = hdr + 1
    while ws.cell(row=r, column=2).value != "Total":
        names.append(ws.cell(row=r, column=2).value)
        r += 1
    assert names and all(names), f"nameless row in {names}"
    assert "ziprecruiter" not in sheet_text(wb[ep.SHEET_TRACKING]).lower()
    assert "ziprecruiter" not in deck_text(build_deck(plan)).lower()
    assert "do not run in this plan" in sheet_text(ws).lower()


def test_mixed_plan_keeps_the_publisher_for_the_market_where_it_runs():
    plan = uk_plan(extra_us=True)
    model = ep.build_model(plan, TODAY)
    zr = [c for c in model["channels"] if c["name"] == "ZipRecruiter"]
    assert zr and zr[0]["markets"] == ["United States"]
    ws = build_wb(plan)[ep.SHEET_TIERS]
    row = find_row(ws, "ZipRecruiter")
    runs_in = [
        ws.cell(row=find_row(ws, "Publisher"), column=c).value for c in range(2, 16)
    ]
    col = runs_in.index("Runs in") + 2
    assert ws.cell(row=row, column=col).value == "United States only"


# -- 2: site shares are exact ---------------------------------------------------
def test_equal_site_shares_sum_to_exactly_one_and_the_matrix_matches():
    plan = make_plan()
    plan["_budget_allocation"] = copy.deepcopy(plan["_budget_allocation"])
    plan["_budget_allocation"].pop("location_adjustments", None)
    model = ep.build_model(plan, TODAY)
    assert sum(model["shares"]) == pytest.approx(1.0, abs=1e-12)
    wb = build_wb(plan, data_only=True)
    ws = wb[ep.SHEET_PACING]
    total_row = [
        r for r in range(1, ws.max_row + 1) if ws.cell(row=r, column=2).value == "Total"
    ][-1]
    assert ws.cell(row=total_row, column=6).value == "OK"
    assert "DOES NOT MATCH" not in sheet_text(ws)


# -- 3: Google Slides path -------------------------------------------------------
def test_enterprise_pack_skips_google_slides_and_says_so(monkeypatch):
    import deck_generator

    monkeypatch.setenv("GOOGLE_SLIDES_CREDENTIALS_B64", "x")
    gen = deck_generator.DeckGenerator()
    plan = make_plan()
    with mock.patch.object(
        gen, "_try_google_slides", return_value=b"G" * 5000
    ) as g, mock.patch.object(gen, "_try_pptx", return_value=b"P" * 5000):
        blob, provider = gen.generate(plan)
        assert provider == "pptx" and blob.startswith(b"P") and not g.called
        assert "PowerPoint version" in plan["_enterprise_deck_note"]
        off = make_plan(enterprise=False)
        blob, provider = gen.generate(off)
        assert provider == "google_slides" and g.called
        assert "_enterprise_deck_note" not in off
    import app

    summary = app._plan_results_summary(plan)
    assert "PowerPoint version" in summary["enterprise_note"]


# -- 4: weeks follow the real campaign window ------------------------------------
def test_week_one_is_the_week_containing_the_start_and_partials_are_flagged():
    plan = make_plan(weeks=2, month=4, campaign_duration="2 weeks")  # Wed 1 Apr 2026
    model = ep.build_model(plan, TODAY)
    weeks = model["weeks"]
    assert weeks[0]["start"] == datetime.date(2026, 3, 29)
    assert [w["days"] for w in weeks] == [4, 7, 3]
    assert [w["partial"] for w in weeks] == [True, False, True]
    assert sum(w["days"] for w in weeks) == 14
    full = weeks[1]["weight"]
    assert weeks[0]["weight"] == pytest.approx(full * 4 / 7, abs=0.02)
    ws = build_wb(plan)[ep.SHEET_PACING]
    text = sheet_text(ws)
    assert "partial" in text.lower() and "4 of 7 days" in text
    hdr = find_row(ws, "Week")
    assert ws.cell(row=hdr + 1, column=2).value.startswith("W01  Sun 29 Mar")


def test_twelve_month_plan_covers_every_day_across_the_year_boundary():
    plan = make_plan(month=1, campaign_duration="12 months", campaign_weeks=0)
    model = ep.build_model(plan, TODAY)
    weeks = model["weeks"]
    assert weeks[0]["start"] == datetime.date(2026, 12, 27)  # contains Fri 1 Jan 2027
    assert weeks[0]["days"] == 2
    assert weeks[-1]["start"] == datetime.date(2027, 12, 26) and weeks[-1]["days"] == 6
    assert len(weeks) == 53 and sum(w["days"] for w in weeks) == 365
    assert not model["weeks_truncated"]


def test_a_sunday_start_gives_whole_weeks_only():
    model = ep.build_model(make_plan(weeks=8, month=11), TODAY)  # Sun 1 Nov 2026
    assert [w["days"] for w in model["weeks"]] == [7] * 8
    assert not any(w["partial"] for w in model["weeks"])


def test_weekly_spend_still_adds_to_the_plan_budget_with_partial_weeks():
    plan = make_plan(weeks=2, month=4, campaign_duration="2 weeks")
    wb = build_wb(plan, data_only=True)
    ws = wb[ep.SHEET_PACING]
    hdr = find_row(ws, "Week")
    assert ws.cell(row=hdr + 4, column=2).value == "Total"
    assert ws.cell(row=hdr + 4, column=4).value == pytest.approx(150_000)
    assert ws.cell(row=hdr + 1, column=4).value < ws.cell(row=hdr + 2, column=4).value


# -- 5: the no-CPA path ------------------------------------------------------------
def test_one_publisher_plan_falls_back_to_the_blended_cost_per_application():
    plan = make_plan(channels={"Indeed": 100})
    model = ep.build_model(plan, TODAY)
    assert model["cpa_range"] is not None and model["cpa_basis"] == "blended"
    ws = build_wb(plan)[ep.SHEET_PACING]
    hdr = find_row(ws, "Week")
    heads = [ws.cell(row=hdr, column=c).value for c in range(2, 13)]
    assert "Apply starts, low" in heads and "Status" in heads
    assert "Enter one in the assumptions" not in sheet_text(ws)


def test_with_no_cost_per_application_at_all_the_sheet_and_deck_agree():
    plan = make_plan(channels={"Indeed": 100})
    plan["_budget_allocation"] = copy.deepcopy(plan["_budget_allocation"])
    plan["_budget_allocation"]["total_projected"].pop("cost_per_application", None)
    for info in plan["_budget_allocation"]["channel_allocations"].values():
        info.pop("cpa", None)
    model = ep.build_model(plan, TODAY)
    assert model["cpa_range"] is None
    ws = build_wb(plan)[ep.SHEET_PACING]
    text = sheet_text(ws)
    assert "Enter one in the assumptions" not in text
    hdr = find_row(ws, "Week")
    heads = [ws.cell(row=hdr, column=c).value for c in range(2, 13)]
    assert "Apply starts, low" not in heads
    # bands stay usable: the client's weekly goal is an input column
    assert "Weekly goal (enter)" in heads and "Status" in heads
    deck = deck_text(build_deck(plan))
    assert "estimated from the plan's cost-per-application" not in deck
    assert "weekly goal" in deck.lower()


# -- consistency: pacing governs, tiers carry week 1 -------------------------------
def test_tier_sheet_states_that_pacing_governs_and_week_one_ties_out():
    wb = build_wb(make_plan(weeks=8, month=11))
    ws = wb[ep.SHEET_TIERS]
    assert "Weekly Pacing governs" in sheet_text(ws)
    hdr = find_row(ws, "Publisher")
    heads = [ws.cell(row=hdr, column=c).value for c in range(2, 16)]
    col = heads.index("Week 1 spend") + 2
    ev = Evaluator(wb)
    total = 0.0
    r = hdr + 1
    while ws.cell(row=r, column=2).value != "Total":
        total += float(ev.cell(ep.SHEET_TIERS, f"{ep._col(col)}{r}") or 0)
        r += 1
    pacing = wb[ep.SHEET_PACING]
    first = find_row(pacing, "Week") + 1
    assert total == pytest.approx(float(ev.cell(ep.SHEET_PACING, f"D{first}")))


# -- 6 + 7: wizard drafts and the SLA field error -----------------------------------
needs_node = pytest.mark.skipif(
    __import__("tests.wizard_js_harness", fromlist=["NODE"]).NODE is None,
    reason="node not installed",
)


def _enterprise_js() -> str:
    from tests.wizard_js_harness import APP_JS, js_block

    src = APP_JS.read_text(encoding="utf-8")
    return "\n".join(
        js_block(src, f"function {n}(")
        for n in (
            "novaEnterpriseSlaMessage",
            "novaEnterpriseInputs",
            "novaEnterpriseState",
            "novaEnterpriseRestore",
            "novaEnterpriseReset",
            "novaEnterpriseCheckSla",
            "novaEnterpriseValidate",
        )
    )


def _stub(extra: str) -> str:
    return "\n".join(
        [
            "var ELS = {};",
            "function el(id){ if(!(id in ELS)) ELS[id]={id:id,value:'',checked:false,hidden:true,attrs:{},"
            "closest:function(){return null;},setAttribute:function(k,v){this.attrs[k]=v;},"
            "removeAttribute:function(k){delete this.attrs[k];}}; return ELS[id]; }",
            "var document={getElementById:function(id){return el(id);}};",
            "var ERR=[];",
            "function _showFieldError(e,m){ERR.push([e.id,m]);}",
            "function _clearFieldError(e){ERR.push([e.id,null]);}",
            _enterprise_js(),
            extra,
        ]
    )


@needs_node
def test_enterprise_fields_round_trip_through_the_draft_helpers():
    from tests.wizard_js_harness import run_node

    got = run_node(
        _stub(
            """
el('enterpriseEnabled').checked=true; el('enterpriseWeekStart').value='monday';
el('enterpriseSystemOfRecord').value='client ATS'; el('enterpriseLockDay').value='friday';
el('enterpriseSetupSla').value='4';
var saved = JSON.parse(JSON.stringify(novaEnterpriseState()));
novaEnterpriseReset();
var cleared = {on: el('enterpriseEnabled').checked, hidden: el('enterpriseFields').hidden, sor: el('enterpriseSystemOfRecord').value, week: el('enterpriseWeekStart').value};
novaEnterpriseRestore(saved);
process.stdout.write(JSON.stringify({saved: saved, cleared: cleared, restored: {on: el('enterpriseEnabled').checked,
 hidden: el('enterpriseFields').hidden, week: el('enterpriseWeekStart').value, sor: el('enterpriseSystemOfRecord').value,
 lock: el('enterpriseLockDay').value, sla: el('enterpriseSetupSla').value}}));
"""
        )
    )
    assert got["saved"]["on"] is True
    assert got["cleared"] == {"on": False, "hidden": True, "sor": "", "week": "sunday"}
    r = got["restored"]
    assert r == {
        "on": True,
        "hidden": False,
        "week": "monday",
        "sor": "client ATS",
        "lock": "friday",
        "sla": "4",
    }


def test_draft_and_session_state_carry_the_enterprise_block():
    from tests.wizard_js_harness import APP_JS, js_block

    src = APP_JS.read_text(encoding="utf-8")
    assert "enterprise: novaEnterpriseState()" in js_block(
        src, "function _collectDraftState()"
    )
    assert "novaEnterpriseRestore(s.enterprise)" in js_block(
        src, "function _restoreDraftState("
    )
    assert "novaEnterpriseState()" in js_block(src, "function saveFormState()")
    assert "novaEnterpriseRestore(s.enterprise)" in js_block(
        src, "function restoreFormState()"
    )
    assert "novaEnterpriseReset()" in js_block(src, "function clearFormState()")


@needs_node
@pytest.mark.parametrize(
    "typed,bad",
    [
        ("45", True),
        ("-1", True),
        ("1.5", True),
        ("30", False),
        ("0", False),
        ("", False),
    ],
)
def test_out_of_range_sla_shows_the_field_error_and_is_never_clamped(typed, bad):
    from tests.wizard_js_harness import run_node

    got = run_node(
        _stub(
            f"""
el('enterpriseEnabled').checked=true; el('enterpriseSetupSla').value={typed!r};
var msg = novaEnterpriseValidate(); novaEnterpriseCheckSla();
process.stdout.write(JSON.stringify({{msg: msg, errors: ERR, payload: novaEnterpriseInputs()}}));
"""
        )
    )
    assert bool(got["msg"]) is bad
    if bad:
        assert got["errors"][-1][0] == "enterpriseSetupSla" and got["errors"][-1][1]
        assert (
            got["payload"].get("enterprise_setup_sla_days") != 30
        )  # not silently clamped
    else:
        assert not got["errors"] or got["errors"][-1][1] is None


def test_results_screen_shows_the_enterprise_deck_note():
    from tests.wizard_js_harness import APP_JS

    assert "res.enterprise_note" in APP_JS.read_text(encoding="utf-8")


# -- round 3: clear-form order, per-week averages, Excel rounding ---------------
def test_clear_form_resets_the_enterprise_section_before_anything_saves():
    from tests.test_wizard_clear_cancel_jobgone import _DOM_JS, _clear_form_script
    from tests.wizard_js_harness import run_node

    body = (
        _enterprise_js()
        + """
['enterpriseEnabled','enterpriseWeekStart','enterpriseSystemOfRecord','enterpriseLockDay',
 'enterpriseSetupSla','enterpriseFields','enterpriseGroup'].forEach(function (id) {
  ELS[id] = ROOT.appendChild(mkEl(id)); });
ELS.enterpriseEnabled.checked = true; ELS.enterpriseSystemOfRecord.value = 'client ATS';
ELS.enterpriseFields.hidden = false;
var SAVED = [];
function clearNaicsSelection() { SAVED.push(JSON.parse(JSON.stringify(novaEnterpriseState()))); }
function saveFormState() { SAVED.push(JSON.parse(JSON.stringify(novaEnterpriseState()))); }
function _clearFieldError() {}
clearFormState();
process.stdout.write(JSON.stringify({saved: SAVED}));
"""
    )
    got = run_node(_clear_form_script(body))
    assert got["saved"], "nothing saved during clear"
    assert all(
        s["on"] is False and s["systemOfRecord"] == "" for s in got["saved"]
    ), got


def test_recommended_weekly_is_per_full_week_not_per_week_row():
    plan = make_plan(weeks=2, month=4, campaign_duration="2 weeks")  # 14 active days
    model = ep.build_model(plan, TODAY)
    assert model["effective_weeks"] == pytest.approx(2.0)
    rec = ep.ladder(model)[1]
    assert rec["weekly"] == pytest.approx(150_000 / 2.0)  # was 150000 / 3 rows
    wb = build_wb(plan, data_only=True)
    ws = wb[ep.SHEET_PACING]
    row = find_row(ws, "Recommended weekly")
    assert ws.cell(row=row, column=4).value == pytest.approx(75_000)
    assert ws.cell(
        row=find_row(ws, "Campaign weeks (active days / 7)"), column=3
    ).value == pytest.approx(2.0)
    live = build_wb(plan)
    ev = Evaluator(live)
    assert ev.cell(ep.SHEET_PACING, f"D{row}") == pytest.approx(75_000)
    # tiers: average weekly spend uses the same denominator
    tiers = live[ep.SHEET_TIERS]
    hdr = find_row(tiers, "Publisher")
    first = ev.cell(ep.SHEET_TIERS, f"H{hdr + 1}")
    assert first == pytest.approx(tiers.cell(row=hdr + 1, column=4).value / 2.0)


def test_excel_round_is_half_up_not_bankers():
    assert [ep._xl_round(v, 0) for v in (0.5, 1.5, 2.5, 3.5, -2.5)] == [1, 2, 3, 4, -3]
    plan = make_plan(weeks=8, month=11)  # 8 whole weeks: weekly = budget / 8
    plan["_budget_allocation"] = copy.deepcopy(plan["_budget_allocation"])
    plan["_budget_allocation"]["channel_allocations"]["Indeed"][
        "dollar_amount"
    ] = 19752.0
    data_wb = build_wb(plan, data_only=True)
    wb = build_wb(plan)
    ev = Evaluator(wb)
    ws = wb[ep.SHEET_TIERS]
    hdr = find_row(ws, "Publisher")
    row = next(
        r
        for r in range(hdr + 1, hdr + 10)
        if ws.cell(row=r, column=2).value == "Indeed"
    )
    cached = data_wb[ep.SHEET_TIERS].cell(row=row, column=9).value
    assert ev.cell(ep.SHEET_TIERS, f"I{row}") == 1235  # 2469 * 0.5 = 1234.5 -> 1235
    assert cached == 1235
