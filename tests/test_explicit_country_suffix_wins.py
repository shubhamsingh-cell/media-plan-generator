"""An explicit country suffix always wins over a US city-name match.

Found 2026-11-02: "Birmingham, United Kingdom" and "Birmingham, UK" resolved
to Birmingham, AL. research._detect_country's US-metro guard (Birmingham is
a US metro in METRO_DATA) returned None before it ever read the suffix, so
get_location_info fell to the US metro table, the UK workbook printed US
metro data and the QA gate flagged us_data_on_non_us_plan. The same class:
Richmond (VA), Manchester (NH), Cambridge (MA), Portland (ME / OR), Paris
(TX) vs Paris, France, and London, ON (Canada) vs London, UK.

One table, every resolver a plan reads a location through: the country
detector, get_location_info (what the workbook's market rows print),
publisher_filter.plan_countries (board funding), plan_geo.location_is_us
(the US/non-US gate) and the plan currency of the detected country.
"""

from __future__ import annotations

import pytest

import plan_currency
import plan_geo
import publisher_filter
import research

UK, US, CA, FR, DE = "United Kingdom", "United States", "Canada", "France", "Germany"

CASES = [
    # with an explicit country (or UK nation / Canadian province) suffix
    ("Birmingham, United Kingdom", UK),
    ("Birmingham, UK", UK),
    ("Birmingham, England", UK),
    ("Birmingham (UK)", UK),
    ("Richmond, England", UK),
    ("Richmond, United Kingdom", UK),
    ("Manchester, UK", UK),
    ("Manchester, England", UK),
    ("Cambridge, United Kingdom", UK),
    ("Cambridge, England", UK),
    ("Portland, United Kingdom", UK),
    ("Paris, France", FR),
    ("London, UK", UK),
    ("London, United Kingdom", UK),
    ("London, ON", CA),
    ("London, Ontario", CA),
    ("London, Canada", CA),
    ("Berlin, Germany", DE),
    # with a US state suffix: the US place
    ("Birmingham, AL", US),
    ("Richmond, VA", US),
    ("Manchester, NH", US),
    ("Cambridge, MA", US),
    ("Portland, ME", US),
    ("Portland, OR", US),
    ("Paris, TX", US),
    ("Berlin, NH", US),
]


@pytest.mark.parametrize("loc,country", CASES)
def test_every_resolver_agrees_on_the_country(loc, country):
    detected = research.detect_country(loc)
    assert (detected or US) == country, f"detect_country({loc!r}) = {detected!r}"
    assert research.get_location_info(loc)["country"] == country
    assert publisher_filter.plan_countries({"locations": [loc]}) == [country]
    assert plan_geo.location_is_us(loc) is (country == US)
    expected_cur = plan_currency.currency_for_country(country)
    assert plan_currency.currency_for_country(detected or US) == expected_cur


@pytest.mark.parametrize("loc", ["Birmingham, United Kingdom", "Birmingham, UK"])
def test_a_uk_birmingham_gets_uk_market_rows_not_the_us_metro(loc):
    info = research.get_location_info(loc)
    assert info["is_international"] is True
    assert info["currency"] == "GBP"
    assert "AL" not in (info.get("metro_name") or "")


@pytest.mark.parametrize(
    "loc,state,country",
    [
        ("Birmingham, UK", "", UK),
        ("Richmond, England", "", UK),
        ("London, ON", "ON", CA),
        ("London, United Kingdom", "", UK),
        ("Paris, TX", "TX", US),
        ("Portland, ME", "ME", US),
    ],
)
def test_the_budget_engine_location_split_names_the_country(loc, state, country):
    import app

    got = app._split_city_state_country(loc)
    assert (got["state"], got["country"]) == (state, country)
