"""Regression: the "Model vs. benchmark" note never compares plan-currency
CPA against the US-dollar KB floor.

Found 2026-09-24 on a London GBP healthcare plan: the note read "this plan's
blended CPA (£11.14) sits below the cited benchmark floor (£35)". The $35
floor is the KB's US-dollar CPA range, relabelled with the plan's symbol
and compared without FX -- the cross-currency comparison declare-not-convert
forbids. The delivery gate cannot see it (the glyph matches the plan).

Contract: on a non-USD plan the CPA clause is omitted; the unit-free
apply-rate clause still appears; USD plans are unchanged.
"""

from __future__ import annotations

import pytest

import excel_v2

_BENCH = {"cpa": {"range": "$35-$58+"}, "apply_rate": {"2025": "2.0-3.0%"}}


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


def test_gbp_plan_omits_cross_currency_cpa_comparison():
    data = _plan("£100,000", "GBP")
    excel_v2._set_active_currency(data)
    note = excel_v2._model_vs_benchmark_note(data, _BENCH)
    assert note is not None
    assert "CPA" not in note, note
    assert "£35" not in note and "$35" not in note, note
    assert "apply rate" in note


def test_gbp_plan_with_only_cpa_gap_emits_no_note():
    data = _plan("£100,000", "GBP")
    data["_budget_allocation"]["channel_allocations"]["programmatic_dsp"][
        "projected_clicks"
    ] = 100000  # apply 1% -- inside the ceiling
    excel_v2._set_active_currency(data)
    assert excel_v2._model_vs_benchmark_note(data, _BENCH) is None


def test_usd_plan_still_compares_cpa():
    data = _plan("$100,000", "USD")
    excel_v2._set_active_currency(data)
    note = excel_v2._model_vs_benchmark_note(data, _BENCH)
    assert "blended CPA ($10.00)" in note and "floor ($35)" in note, note
