"""Delta verifier on mpg-input-parity @5a59c1b (2026-10-01), items 1, 2
and 4d -- the plan currency the preview states is the one the deck,
workbook and delivery gate print.

1.  "AUD$100k" + Sydney priced in USD with an empty note (also CAD$ /
    NZD$ / USD$ ...): plan_currency had no XXX$ entries, so only the "$"
    was read and the canonical budget became "$100,000".
2.  A bare "30000" per month for Toronto said "plan currency is CAD from
    the currency you typed": the per-period canonical budget got a "$"
    prefix (the legacy normalisation default) that declared a currency
    nobody typed -- and made a bare per-month London budget a USD plan.
4d. "Rs 50 lakh" + Mumbai became a $5,000,000 USD plan: "Rs" / "INR" are
    words plan_currency never reads, so lakh / crore amounts lost their
    currency in the canonical string.
"""

from __future__ import annotations

import pytest

import app
import bundle_qa
import excel_v2
import plan_currency
import ppt_generator
import scorecard_generator
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)


def _generate_view(payload: dict) -> dict:
    data = app._sanitize_generate_request(dict(payload))
    assert app._resolve_request_budget(data).ok
    app._normalize_request_budget(data, log=False)
    return data


def _estimate(port: int, body: dict) -> dict:
    status, est = post_json(port, "/api/estimate", dict(body, budget_only=True))
    assert status == 200, est
    return est


DESIGNATORS = [
    ("AUD$100k", "campaign", ["Sydney, NSW"], "AUD", 100000),
    ("CAD$50,000", "campaign", ["Dallas, TX"], "CAD", 50000),
    ("NZD$75,000", "campaign", ["Auckland"], "NZD", 75000),
    ("HKD$200,000", "campaign", ["Hong Kong"], "HKD", 200000),
    ("SGD$80,000", "campaign", ["Singapore"], "SGD", 80000),
    ("USD$90,000", "campaign", ["London, UK"], "USD", 90000),
    ("AUD$8,000", "monthly", ["Sydney, NSW"], "AUD", 72000),
]


@pytest.mark.parametrize("budget,period,locs,code,total", DESIGNATORS)
def test_xxx_dollar_designators_declare_their_currency(live_port, budget, period, locs, code, total):  # noqa: F811
    body = {"budget_range": budget, "budget_period": period,
            "campaign_duration": "9 months", "locations": locs}
    est = _estimate(live_port, body)
    assert est["plan_currency"]["code"] == code and est["plan_currency"]["note"] == ""
    assert est["budget"]["total"] == total
    data = _generate_view(body)
    assert data["budget"] == f"{budget[:4]}{total:,}"  # "AUD$72,000": the symbol survives
    summary = app._plan_budget_summary(data)  # the results screen
    assert summary["currency"] == code
    assert summary["display"].startswith(plan_currency.symbol_for_code(code).strip())


PREVIEW_CASES = DESIGNATORS + [
    ("Rs 50 lakh", "campaign", ["Mumbai"], "INR", 5_000_000),
    ("INR 2 crore", "campaign", ["Pune"], "INR", 20_000_000),
    ("30000", "monthly", ["Toronto, ON"], "CAD", 270_000),
    ("30000", "monthly", ["London, UK"], "GBP", 270_000),
]


@pytest.mark.parametrize("budget,period,locs,code,total", PREVIEW_CASES)
def test_deck_workbook_scorecard_and_gate_print_the_preview_currency(budget, period, locs, code, total):
    """The four currency readers of a generated bundle all resolve from the
    request /api/generate built -- and must land on the preview's code."""
    data = _generate_view({"budget_range": budget, "budget_period": period,
                           "campaign_duration": "9 months", "locations": locs})
    preview = app._compute_plan_estimate({"budget_range": budget, "budget_period": period,
                                          "campaign_duration": "9 months", "locations": locs,
                                          "budget_only": True})
    assert preview["plan_currency"]["code"] == code
    assert ppt_generator._plan_currency_code(dict(data)) == code
    assert excel_v2._plan_currency_code(dict(data)) == code
    assert bundle_qa._resolve_plan_currency(dict(data))[0] == code
    assert scorecard_generator._currency_symbol(dict(data)) == plan_currency.symbol_for_code(code)


def test_note_shows_when_the_typed_symbol_is_read_as_another_currency():
    data = _generate_view({"budget_range": "$50,000", "locations": ["Toronto, ON"]})
    note = data["_budget_resolution"]["plan_currency"]["note"]
    assert note == "plan currency is CAD (“$” is read as CAD for your locations)"


@pytest.mark.parametrize(
    "locs,code",
    [(["Toronto, ON"], "CAD"), (["London, UK"], "GBP"), (["Dallas, TX"], "USD")],
)
def test_bare_per_period_budget_declares_nothing(live_port, locs, code):  # noqa: F811
    body = {"budget_range": "30000", "budget_period": "monthly",
            "campaign_duration": "9 months", "locations": locs}
    data = _generate_view(body)
    assert data["budget"] == "270,000"  # no "$" invented for a bare number
    reading = data["_budget_resolution"]["plan_currency"]
    assert reading["code"] == code and reading["basis"] in ("market", "default")
    est = _estimate(live_port, body)
    assert est["plan_currency"] == reading
    if code != "USD":
        assert reading["note"] == f"plan currency is {code} for your locations"
    assert "you typed" not in reading["note"]


@pytest.mark.parametrize(
    "budget,locs,canonical",
    [
        ("Rs 50 lakh", ["Mumbai"], "₹5,000,000"),
        ("Rs 12,50,000", ["Mumbai"], "₹1,250,000"),
        ("INR 2 crore", ["Pune"], "₹20,000,000"),
        ("₹50 lakh", ["Mumbai"], "₹5,000,000"),
        ("50 lakh rupees", ["Delhi"], "₹5,000,000"),
        ("Rs 50 lakh", ["Dallas, TX"], "₹5,000,000"),  # declared beats market
    ],
)
def test_rupee_amounts_with_indian_units_declare_inr(live_port, budget, locs, canonical):  # noqa: F811
    data = _generate_view({"budget_range": budget, "locations": locs})
    assert data["budget"] == canonical
    assert plan_currency.currency_for_plan_with_basis(data) == ("INR", "declared")
    est = _estimate(live_port, {"budget_range": budget, "locations": locs})
    assert est["plan_currency"]["code"] == "INR" and est["plan_currency"]["note"] == ""


def test_rs_without_indian_units_is_not_guessed():
    # "Rs" is also Pakistan's / Sri Lanka's / Nepal's abbreviation: only
    # lakh / crore words or lakh grouping make it Indian rupees.
    data = _generate_view({"budget_range": "Rs 50,000", "locations": ["Dallas, TX"]})
    assert data["budget"] == "50,000"
    assert data["_budget_resolution"]["plan_currency"]["code"] == "USD"
