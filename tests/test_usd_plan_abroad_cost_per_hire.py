"""A US$-priced plan whose markets are all outside the US (international audit #8).

It used to keep the US cost per hire as a presentable figure, so a Vietnam plan
was graded "Underfunded" against US$3,350 while its own workbook said there was
no local benchmark.  It now behaves like the euro case: the claim is withheld and
labelled, while the engine's own math (a US$ plan is at parity) is unchanged.
A US plan, and a plan with a US market beside the foreign one, are unchanged.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
for _n in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    sys.modules.setdefault(_n, types.ModuleType(_n))

import budget_engine  # noqa: E402
from tests.intl_plan_fixture import _bundle, _sheet_text  # noqa: E402


def test_usd_plan_in_a_non_us_market_is_suppressed_like_the_euro_case():
    basis = budget_engine._resolve_intl_cpc_basis(
        ["Ho Chi Minh City, Vietnam"], "USD", "$80,000"
    )
    usd = budget_engine.resolve_industry_cph(
        "general_entry_level", None, "USD", basis, non_us_market=True
    )
    assert usd["claim_suppressed"] is True and usd["value"] is None
    assert usd["us_benchmark_non_us_market"] is True
    # the engine's own math is the US midpoint, exactly as before
    us = budget_engine.resolve_industry_cph("general_entry_level", None, "USD", None)
    assert usd["math_value"] == us["math_value"] == 3350.0
    assert us["claim_suppressed"] is False and us["value"] == 3350.0
    # a plan that also has a US market keeps the labelled US figure (it has a
    # US benchmark to stand on): the caller says whether the plan is all-abroad
    mixed = budget_engine.resolve_industry_cph(
        "general_entry_level", None, "USD", basis, non_us_market=False
    )
    assert mixed["claim_suppressed"] is False and mixed["value"] == 3350.0
    assert mixed["us_benchmark_non_us_market"] is True
    # a market the benchmark dataset does not cover (Spain) is judged the same way
    spain = budget_engine.resolve_industry_cph(
        "general_entry_level", None, "USD", None, non_us_market=True
    )
    assert spain["claim_suppressed"] is True and spain["value"] is None


def test_sufficiency_is_not_graded_against_the_us_figure_for_a_usd_plan_abroad():
    abroad = budget_engine.assess_budget_sufficiency(
        80_000, 50, "general_entry_level", {}, plan_currency="USD", non_us_market=True
    )
    home = budget_engine.assess_budget_sufficiency(
        80_000, 50, "general_entry_level", {}, plan_currency="USD"
    )
    assert abroad["budget_reality_check"]["feasibility_tier"] == "not_assessed"
    assert home["budget_reality_check"]["feasibility_tier"] != "not_assessed"


def test_vietnam_usd_plan_is_not_flagged_underfunded_from_a_us_cost_per_hire():
    bundle = _bundle("vn")
    alloc = bundle["data"]["_budget_allocation"]
    check = alloc["sufficiency"]["budget_reality_check"]
    assert check["feasibility_tier"] == "not_assessed"
    review = _sheet_text(bundle["wb"], "Review before sending")
    assert "Budget reality check flagged" not in review
    assert "Markets without local benchmarks: Vietnam" in review


def test_us_plan_budget_check_is_unchanged():
    alloc = _bundle("us")["data"]["_budget_allocation"]
    check = alloc["sufficiency"]["budget_reality_check"]
    assert check["feasibility_tier"] != "not_assessed"
    assert alloc["metadata"]["industry_cph"]["claim_suppressed"] is False


def test_review_checklist_does_not_call_unprojected_hires_estimates():
    text = _sheet_text(_bundle("vn")["wb"], "Review before sending")
    assert "Projected hires and costs are estimates" not in text
    assert "Hires and cost per hire are not projected for this market" in text
    us = _sheet_text(_bundle("us")["wb"], "Review before sending")
    assert "Projected hires and costs are estimates" in us


def _estimate(*locs: str) -> Dict[str, Any]:
    import app

    return app._compute_plan_estimate(
        {
            "budget": "$80,000",
            "industry": "general_entry_level",
            "roles": ["Agent"],
            "locations": list(locs),
            "campaign_duration": "3 months",
            "client_name": "Test Co",
        }
    )


def test_live_estimate_withholds_the_us_cost_per_hire_for_a_usd_plan_abroad():
    abroad = _estimate("Ho Chi Minh City, Vietnam")
    assert abroad["est_hires_midpoint"] is None
    assert abroad["industry_midpoint_cph"] is None
    home = _estimate("Dallas, TX")
    assert home["est_hires_midpoint"] and home["industry_midpoint_cph"] == 3350.0
    both = _estimate("Dallas, TX", "Hanoi, Vietnam")  # a US market keeps its benchmark
    assert both["industry_midpoint_cph"] == 3350.0
