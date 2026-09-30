"""Regression: 70e65a6 extended data_synthesizer's per_role_salaries
override from driver roles to EVERY priced role and gold_standard used it
verbatim, so Quality Intelligence city rows for one role were identical
across cities while the sheet's City Multiplier column and its footnote
("Per-role salaries adjusted by city multiplier") claimed otherwise; the
override also dropped the band's ``currency`` field.

Now: ONE base band per role (the figure Market Intelligence shows), scaled
by each city's multiplier on Quality Intelligence, currency preserved.
Driver-family bands (market-level resolver) stay verbatim and report the
1.0 multiplier actually applied.
"""

from __future__ import annotations

import pytest

import data_synthesizer
import gold_standard as gs


def _pipeline(plan: dict):
    synth = data_synthesizer.synthesize({}, {}, plan)
    data = dict(plan)
    data["_synthesized"] = synth
    return synth, gs.enrich_city_level_data(data)


_US_PLAN = {
    "roles": ["Registered Nurse", "Software Engineer"],
    "locations": ["New York, NY", "Memphis, TN"],
    "industry": "healthcare_medical",
}
_UK_PLAN = {
    "roles": ["Software Engineer", "Accountant"],
    "locations": ["London, UK", "Manchester, UK"],
    "industry": "tech_engineering",
}
# A GBP-budgeted plan with a US market: its US rows keep the base x
# multiplier contract and their USD currency. (A UK-only plan's rows were
# the uk case here until audit F 3.4, 2026-10-01: they are now "Local
# salary data n/a" -- see test_uk_only_plan_rows_are_withheld below.)
_MIXED_PLAN = {
    "roles": ["Software Engineer", "Accountant"],
    "locations": ["New York, NY", "Memphis, TN", "London, UK"],
    "industry": "tech_engineering",
    "budget": "£500,000",
}


def test_uk_only_plan_rows_are_withheld():
    synth, city_data = _pipeline(_UK_PLAN)
    assert "per_role_salaries" not in synth
    for info in city_data.values():
        for role, row in info["per_role_salary"].items():
            assert row.get("local_salary_na") is True, role
            assert row["median"] == 0


@pytest.mark.parametrize("plan", [_US_PLAN, _MIXED_PLAN], ids=["us", "mixed"])
def test_city_rows_are_base_times_their_multiplier(plan):
    synth, city_data = _pipeline(plan)
    base = synth["per_role_salaries"]
    # London (mixed plan) is withheld, not scaled -- see the test above.
    us_cities = {
        c: info for c, info in city_data.items() if not info.get("local_salary_na")
    }
    multipliers = {c: info["salary_multiplier"] for c, info in us_cities.items()}
    assert len(set(round(m, 2) for m in multipliers.values())) > 1, multipliers
    for city, info in us_cities.items():
        for role, row in info["per_role_salary"].items():
            if role not in base:
                continue
            assert row["multiplier"] == round(multipliers[city], 2)
            for k in ("min", "p25", "median", "p75", "max"):
                assert row[k] == round(base[role][k] * multipliers[city]), (city, role, k)
    for role in base:
        medians = {info["per_role_salary"][role]["median"] for info in us_cities.values()}
        assert len(medians) == len(us_cities), (role, medians)


@pytest.mark.parametrize("plan", [_US_PLAN, _MIXED_PLAN], ids=["us", "mixed"])
def test_currency_is_preserved_on_every_city_row(plan):
    synth, city_data = _pipeline(plan)
    sal_intel = synth["salary_intelligence"]
    for role, band in synth["per_role_salaries"].items():
        assert band["currency"] == (sal_intel[role].get("currency") or "")
        for info in city_data.values():
            if info.get("local_salary_na"):
                continue
            assert info["per_role_salary"][role]["currency"] == band["currency"]


def test_base_band_still_equals_market_intelligence():
    synth, _ = _pipeline(_US_PLAN)
    for role, band in synth["per_role_salaries"].items():
        assert band["median"] == synth["salary_intelligence"][role]["median"]


def test_driver_bands_stay_verbatim_and_report_applied_multiplier():
    # US market: the driver resolver's US-basis band is withheld on a plan
    # with no US market since audit F 3.4 (was "United Kingdom" here).
    plan = {
        "roles": ["Commercial Cab Driver"],
        "target_roles": [{"title": "Commercial Cab Driver", "count": 5}],
        "locations": ["Chicago, IL"],
        "industry": "hospitality_travel",
    }
    synth, city_data = _pipeline(plan)
    mi = synth["salary_intelligence"]["Commercial Cab Driver"]
    qi = next(iter(city_data.values()))["per_role_salary"]["Commercial Cab Driver"]
    assert qi["median"] == mi["median"]
    assert qi["multiplier"] == 1.0
