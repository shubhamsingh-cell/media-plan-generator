"""K-08: a US "City, ST" location must never resolve to a foreign country.

Two-letter US state codes collide with ISO country codes (CA = Canada,
IN = India, DE = Germany, ...). The intl-benchmark and industry-report
normalizers used to read the trailing token of "Los Angeles, CA" as Canada,
so a US client deck carried Canadian cited statistics and a Canadian CPA.
The same family of bug let a raw substring test read "Indianapolis" and
"Fort Wayne, Indiana" as India (INR plan currency for a US client).

The ambiguous codes are derived from each module's own country table, so a
future alias addition that re-introduces a collision fails here.
"""

from __future__ import annotations

import pytest

import cited_data_block
import industry_reports_lookup as irl
import intl_benchmark_lookup as ibl
import plan_currency
import plan_geo

_STATES = sorted(plan_geo.US_STATE_ABBR)
_STATE_NAMES = sorted(plan_geo.US_STATE_NAME_TO_ABBR)

_INTL_AMBIGUOUS = sorted(
    a.upper() for a in ibl._COUNTRY_TO_SLUG if a.upper() in plan_geo.US_STATE_ABBR
)
_REPORTS_AMBIGUOUS = sorted(
    {
        a.upper()
        for aliases in irl._GEO_ALIASES.values()
        for a in aliases
        if a.upper() in plan_geo.US_STATE_ABBR
    }
)
_CURRENCY_AMBIGUOUS = sorted(
    a.upper()
    for a in plan_currency._COUNTRY_TO_CODE
    if a.upper() in plan_geo.US_STATE_ABBR
)


def test_ambiguous_code_sets_are_non_trivial():
    """The derived collision sets must contain the audited traps, or the
    parametrized tests below would be vacuous."""
    for code in ("CA", "IN", "DE"):
        assert code in _INTL_AMBIGUOUS
        assert code in _REPORTS_AMBIGUOUS


@pytest.mark.parametrize("state", _STATES)
def test_city_state_code_is_a_us_location(state):
    loc = f"Springfield, {state}"
    assert plan_geo.us_state_for_location(loc) == state
    assert plan_geo.location_is_us(loc) is True
    assert ibl._normalize_country(loc) is None, loc
    assert irl._normalize_country_to_iso(loc) is None, loc
    assert plan_currency.currency_for_country(loc) == "USD", loc


@pytest.mark.parametrize("name", _STATE_NAMES)
def test_city_state_name_is_a_us_location(name):
    loc = f"Springfield, {name.title()}"
    assert plan_geo.us_state_for_location(loc) == plan_geo.US_STATE_NAME_TO_ABBR[name]
    assert ibl._normalize_country(loc) is None, loc
    assert irl._normalize_country_to_iso(loc) is None, loc
    assert plan_currency.currency_for_country(loc) == "USD", loc
    assert plan_geo.is_us_plan({"locations": [loc]}) is True, loc


@pytest.mark.parametrize("code", _INTL_AMBIGUOUS)
def test_intl_bare_country_code_still_resolves(code):
    """A bare code is a country-field value -- "CA" alone stays Canada."""
    assert ibl._normalize_country(code) == ibl._COUNTRY_TO_SLUG[code.lower()]
    assert plan_geo.us_state_for_location(code) is None


@pytest.mark.parametrize("code", _REPORTS_AMBIGUOUS)
def test_reports_bare_country_code_still_resolves(code):
    assert irl._normalize_country_to_iso(code) is not None


@pytest.mark.parametrize("code", _CURRENCY_AMBIGUOUS)
def test_currency_bare_country_code_still_resolves(code):
    assert plan_currency.currency_for_country(code) == plan_currency._COUNTRY_TO_CODE[
        code.lower()
    ]


@pytest.mark.parametrize(
    "loc",
    [
        "Los Angeles, CA 90001",
        "Arlington, VA, United States",
        "Washington, DC",
        "Hartford, CT 06103",
    ],
)
def test_state_variants(loc):
    assert plan_geo.us_state_for_location(loc) is not None
    assert ibl._normalize_country(loc) is None


@pytest.mark.parametrize(
    "loc,slug,iso,code",
    [
        ("Mumbai, India", "india", "India", "INR"),
        ("London, UK", "uk", "UK", "GBP"),
        ("Toronto, ON, Canada", "canada", "Canada", "CAD"),
        ("Berlin, Germany", "germany", "Germany", "EUR"),
        ("Bengaluru India", "india", "India", "INR"),
    ],
)
def test_true_country_inputs_still_resolve(loc, slug, iso, code):
    assert ibl._normalize_country(loc) == slug
    assert irl._normalize_country_to_iso(loc) == iso
    assert plan_currency.currency_for_country(loc) == code
    assert plan_geo.us_state_for_location(loc) is None


@pytest.mark.parametrize(
    "loc", ["Indianapolis", "Indianapolis, Indiana", "Fort Wayne, Indiana", "Indiana"]
)
def test_indiana_is_not_india(loc):
    """Substring matching read "india" inside "Indianapolis"/"Indiana"."""
    assert plan_currency.currency_for_country(loc) != "INR", loc
    assert ibl._normalize_country(loc) is None, loc
    assert irl._normalize_country_to_iso(loc) is None, loc
    plan = {"locations": [loc], "budget": "250000"}
    assert plan_currency.currency_for_plan_with_basis(plan)[0] == "USD", loc
    assert plan_geo.is_us_plan(plan) is True, loc


@pytest.mark.parametrize(
    "loc,wrong_publisher",
    [
        ("Los Angeles, CA", "Canada"),
        ("Indianapolis, IN", "Naukri"),
        ("Wilmington, DE", "Stepstone"),
    ],
)
def test_cited_block_carries_no_foreign_data_for_us_city(loc, wrong_publisher):
    """The python-pptx exec-summary "2026 Market Data" block and the Google
    Slides salary/CPA lines read these normalizers with locations[0]."""
    block = cited_data_block.build_cited_2026_block(
        {"locations": [loc], "industry": "healthcare"}
    )
    assert block["salary_line"] == ""
    joined = " ".join(block["metric_lines"])
    assert wrong_publisher not in joined
    assert ibl.get_cpa_median_usd("technology", loc) == ibl.get_cpa_median_usd(
        "technology", "Dallas, TX"
    )
