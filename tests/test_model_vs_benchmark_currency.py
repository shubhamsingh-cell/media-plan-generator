"""Regression: the "Model vs. benchmark" note never compares plan-currency
CPA against the US-dollar KB figure.

Found 2026-09-24 on a London GBP healthcare plan: the note read "this plan's
blended CPA (£11.14) sits below the cited benchmark floor (£35)". The $35
figure is the KB's US-dollar CPA range, relabelled with the plan's symbol
and compared without FX -- the cross-currency comparison declare-not-convert
forbids. The delivery gate cannot see it (the glyph matches the plan).

Contract (updated 2026-10-18, phase B): the note compares the plan's blended
CPA with the industry CPA the benchmark table prints as its headline -- the
cited MEDIAN, else the range midpoint -- never the range FLOOR or the
apply-rate CEILING (the least-sourced values in the row). On a non-USD plan
there is no note at all (no FX is applied, so no money comparison exists);
USD plans compare as above.
"""

from __future__ import annotations

import pytest

import excel_v2

_BENCH = {
    "cpa": {"range": "$35-$58+", "median": "$40.00"},
    "apply_rate": {"2025": "2.0-3.0%"},
}

def _plan(budget, currency_code):
    return {
        "currency": currency_code,
        "budget": budget,
        "_budget_allocation": {
            "channel_allocations": {
                "programmatic_dsp": {
                    "dollar_amount": 10000.0,
                    "projected_applications": 1000,  # CPA 10 < floor 35
                    "projected_clicks": 10000,  # apply 10% > ceiling 3%
                }
            }
        },
    }


@pytest.fixture(autouse=True)
def _reset_currency():
    if hasattr(excel_v2._currency_tls, "code"):
        del excel_v2._currency_tls.code
    yield
    if hasattr(excel_v2._currency_tls, "code"):
        del excel_v2._currency_tls.code


def test_gbp_plan_has_no_cross_currency_note():
    data = _plan("£100,000", "GBP")
    excel_v2._set_active_currency(data)
    # CPA 10 is far below the cited $40 median, and apply 10% is above the
    # cited 3% ceiling -- on a GBP plan neither produces a note.
    assert excel_v2._model_vs_benchmark_note(data, _BENCH) is None


def test_usd_plan_compares_against_the_median_not_the_floor():
    data = _plan("$100,000", "USD")
    excel_v2._set_active_currency(data)
    note = excel_v2._model_vs_benchmark_note(data, _BENCH)
    assert note is not None
    assert "blended CPA ($10.00)" in note, note
    assert "industry median ($40.00)" in note, note
    assert "floor" not in note.lower() and "ceiling" not in note.lower(), note
    # the old provenance claim about what the ranges "reflect" is gone
    assert "post-and-pray" not in note and "deliberate modeling position" not in note


def test_usd_plan_apply_rate_ceiling_no_longer_triggers_a_note():
    data = _plan("$100,000", "USD")
    # CPA 100 is ABOVE the median: only the (unsourced) apply-rate ceiling
    # would have fired the note before.
    ch = data["_budget_allocation"]["channel_allocations"]["programmatic_dsp"]
    ch["projected_applications"] = 100
    ch["projected_clicks"] = 1000  # apply 10% > ceiling 3%
    excel_v2._set_active_currency(data)
    assert excel_v2._model_vs_benchmark_note(data, _BENCH) is None


def test_plan_above_the_median_gets_no_note():
    data = _plan("$100,000", "USD")
    ch = data["_budget_allocation"]["channel_allocations"]["programmatic_dsp"]
    ch["projected_applications"] = 100  # CPA 100 > median 40
    excel_v2._set_active_currency(data)
    assert excel_v2._model_vs_benchmark_note(data, _BENCH) is None


def test_row_with_only_a_range_uses_the_range_midpoint():
    data = _plan("$100,000", "USD")
    excel_v2._set_active_currency(data)
    note = excel_v2._model_vs_benchmark_note(data, {"cpa": {"range": "$20-$60"}})
    assert note is not None and "range midpoint ($40.00)" in note, note
