"""Regression tests: the Activation Event Calendar's hiring_intensity WORD
must always rank-agree with its own budget_weight NUMBER, within the same
calendar.

Real defect (strategy:hershey#2, found by the verifier while checking the
already-fixed 90-Day Forecast / Activation Calendar narrative-contradiction
bug): ``gold_standard.build_activation_calendar`` computed the word and the
number from two different sources for non-subvertical months. The word came
straight from the generic ``_HIRING_EVENTS_CALENDAR`` table; the number
started from that same word's canonical weight but was then blended (S50)
with an industry-specific ``seasonal_hiring_trends.json`` peak/low
multiplier -- a blend the word never saw. A sweep of 15 industries x 12
start months (3,240 month/industry cases) found 51 months where the two
disagreed, e.g. retail March: word "Very High" but blended weight 0.98,
below April's word "High" / weight 1.05 -- a reader comparing the
calendar's own WORDS could see a contradiction even though the numbers
were internally consistent.

Fix: the word is now always derived FROM the final (post-blend) weight via
one fixed threshold rule anchored at the midpoints between the canonical
intensity_weights (0.7/1.0/1.1/1.3), applied after the blend in the generic
path and applied identically in the sub-vertical path -- so word and number
can never diverge for any month in any calendar.
"""

from __future__ import annotations

import gold_standard as gs

_INTENSITY_RANK = {"low": 0, "moderate": 1, "high": 2, "very_high": 3}

# Mirrors the industry set the codebase's own forecast/calendar sweep tests
# use (tests/test_seasonality_narrative_consistency.py,
# tests/test_forecast_short_plan_calendar_phasing.py) plus the raw
# _INDUSTRY_MONTHLY_EVENTS / seasonal_hiring_trends.json keys, so both the
# blended and unblended code paths are exercised.
_SWEEP_INDUSTRIES = [
    "manufacturing",
    "retail_consumer",
    "healthcare_medical",
    "tech_engineering",
    "hospitality_travel",
    "construction_real_estate",
    "finance_banking",
    "logistics_supply_chain",
    "education",
    "government",
    "food_beverage",
    "automotive",
    "insurance",
    "telecommunications",
    "restaurant",
    "retail",
    "healthcare",
    "finance",
    "hospitality",
    "construction",
    "trucking",
    "defense",
    "blue_collar_trades",
    "tech",
]


def _ranking_violations(timeline: list[dict]) -> list[tuple]:
    """No month whose word ranks LOWER may carry a STRICTLY HIGHER weight
    than a month whose word ranks higher, within the same calendar."""
    out = []
    for a in timeline:
        for b in timeline:
            if (
                _INTENSITY_RANK[a["hiring_intensity"]]
                < _INTENSITY_RANK[b["hiring_intensity"]]
                and a["budget_weight"] > b["budget_weight"]
            ):
                out.append(
                    (
                        a["month_name"],
                        a["hiring_intensity"],
                        a["budget_weight"],
                        b["month_name"],
                        b["hiring_intensity"],
                        b["budget_weight"],
                    )
                )
    return out


def test_retail_march_no_longer_outranked_by_april_in_word_but_not_weight():
    """The exact repro from the bug report: retail March's word ("very_high")
    must never sit below April's word ("high") while March's weight is also
    below April's -- word and weight must move together."""
    cal = gs.build_activation_calendar(
        {"campaign_start_month": 1, "industry": "retail"}
    )
    by_name = {m["month_name"]: m for m in cal["timeline"]}
    march, april = by_name["March"], by_name["April"]

    assert march["budget_weight"] < april["budget_weight"]
    # Given March's weight is the lower one, its word must not outrank
    # April's -- the old bug had March "very_high" (rank 3) and April
    # "high" (rank 2) despite March's weight being lower.
    assert (
        _INTENSITY_RANK[march["hiring_intensity"]]
        <= _INTENSITY_RANK[april["hiring_intensity"]]
    )


def test_word_always_derivable_from_weight_via_fixed_thresholds():
    """The word is a pure function of the weight: same weight -> same
    word, everywhere, confirming it is no longer sourced independently."""
    cal = gs.build_activation_calendar(
        {"campaign_start_month": 1, "industry": "retail"}
    )
    seen: dict[float, str] = {}
    for m in cal["timeline"]:
        w, word = m["budget_weight"], m["hiring_intensity"]
        if w in seen:
            assert seen[w] == word, (w, seen[w], word)
        seen[w] = word


def test_no_word_weight_ranking_violations_across_industries_and_months():
    """Sweep across industries x start months: within any single calendar,
    no month with a lower intensity word may carry a strictly higher
    budget_weight than a month with a higher intensity word."""
    violations = []
    for industry in _SWEEP_INDUSTRIES:
        for start_month in range(1, 13):
            data = {
                "client_name": "Sweep Co",
                "industry": industry,
                "campaign_start_month": start_month,
                "roles": ["Generic Role"],
            }
            cal = gs.build_activation_calendar(data)
            v = _ranking_violations(cal["timeline"])
            if v:
                violations.append((industry, start_month, v[:3]))

    assert not violations, (
        f"{len(violations)} calendars have a word/weight ranking "
        f"inversion: {violations[:5]}"
    )


def test_subvertical_override_word_weight_still_agree():
    """The sub-vertical override path (e.g. propane/heating-fuel) already
    derived budget_weight straight from its own hiring_intensity word, so it
    must remain internally consistent under the shared derivation rule too."""
    data = {
        "client_name": "Manpower - Amerigas",
        "industry": "logistics_supply_chain",
        "campaign_start_month": 7,
        "roles": ["CDL A Driver"],
        "notes": "Blue Collar talent profile. On-Site work model.",
    }
    cal = gs.build_activation_calendar(data)
    assert cal["subvertical"] == "fuel_heating_delivery"
    violations = _ranking_violations(cal["timeline"])
    assert not violations, violations
