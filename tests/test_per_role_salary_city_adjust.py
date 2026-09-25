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


@pytest.mark.parametrize("plan", [_US_PLAN, _UK_PLAN], ids=["us", "uk"])
def test_city_rows_are_base_times_their_multiplier(plan):
    synth, city_data = _pipeline(plan)
    base = synth["per_role_salaries"]
    multipliers = {c: info["salary_multiplier"] for c, info in city_data.items()}
    assert len(set(round(m, 2) for m in multipliers.values())) > 1, multipliers
    for city, info in city_data.items():
        for role, row in info["per_role_salary"].items():
            if role not in base:
                continue
            assert row["multiplier"] == round(multipliers[city], 2)
            for k in ("min", "p25", "median", "p75", "max"):
                assert row[k] == round(base[role][k] * multipliers[city]), (city, role, k)
    for role in base:
        medians = {info["per_role_salary"][role]["median"] for info in city_data.values()}
        assert len(medians) == len(city_data), (role, medians)


@pytest.mark.parametrize("plan", [_US_PLAN, _UK_PLAN], ids=["us", "uk"])
def test_currency_is_preserved_on_every_city_row(plan):
    synth, city_data = _pipeline(plan)
    sal_intel = synth["salary_intelligence"]
    for role, band in synth["per_role_salaries"].items():
        assert band["currency"] == (sal_intel[role].get("currency") or "")
        for info in city_data.values():
            assert info["per_role_salary"][role]["currency"] == band["currency"]


def test_base_band_still_equals_market_intelligence():
    synth, _ = _pipeline(_US_PLAN)
    for role, band in synth["per_role_salaries"].items():
        assert band["median"] == synth["salary_intelligence"][role]["median"]


def test_driver_bands_stay_verbatim_and_report_applied_multiplier():
    plan = {
        "roles": ["Commercial Cab Driver"],
        "target_roles": [{"title": "Commercial Cab Driver", "count": 5}],
        "locations": ["United Kingdom"],
        "industry": "hospitality_travel",
    }
    synth, city_data = _pipeline(plan)
    mi = synth["salary_intelligence"]["Commercial Cab Driver"]
    qi = next(iter(city_data.values()))["per_role_salary"]["Commercial Cab Driver"]
    assert qi["median"] == mi["median"]
    assert qi["multiplier"] == 1.0
