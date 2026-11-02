"""A bare city that also names a US city is asked about, never guessed.

Re-review 2026-11-03: 57eedd7 resolved bare "Berlin" / "Dublin" / "Melbourne"
straight through country_registry.world_city_country, so a Dublin, OH user got
an Ireland plan with "market currency is EUR", disagreeing with the plan
generator's rule (plan_geo.bare_foreign_city_country: a name with a US
namesake stays unresolved). Chat now asks first, and when a plan is built on
the bare name anyway it says what it assumed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402


@pytest.mark.parametrize(
    "city,foreign,us",
    [
        ("Berlin", "Germany", "NH"),
        ("Dublin", "Ireland", "OH"),
        ("Melbourne", "Australia", "FL"),
        ("Paris", "France", "TX"),
    ],
)
def test_bare_ambiguous_city_gets_the_clarifier(city: str, foreign: str, us: str) -> None:
    res = nova._detect_ambiguous_clarification(
        f"Create a media plan for 8 data engineers in {city} with a 60000 budget"
    )
    assert res, city
    text = res["response"]
    assert f"{city}, {foreign}" in text and f"{city}, {us}" in text


@pytest.mark.parametrize("where", ["Paris, France", "Paris, TX", "Berlin, Germany", "Dublin, OH"])
def test_explicit_location_skips_the_clarifier(where: str) -> None:
    assert nova._detect_ambiguous_clarification(f"media plan for 8 nurses in {where}") is None


def _plan(loc: str) -> dict:
    return nova.Nova()._query_budget_projection(
        {"budget": 60000, "roles": ["Data Engineer"], "locations": [loc], "industry": "technology"}
    )


@pytest.mark.parametrize("city", ["Dublin", "Melbourne", "Berlin", "Paris"])
def test_bare_ambiguous_city_is_not_priced_as_foreign(city: str) -> None:
    data = _plan(city)
    assert data["country"] == "cross-market", data["country"]
    assert data.get("market_currency") is None
    note = data.get("location_assumption") or ""
    assert city in note and "US" in note
    out = nova._format_budget_response(data, 60000)
    assert note in out  # the answer states the assumption


def test_foreign_only_city_and_explicit_forms_resolve() -> None:
    assert _plan("Dortmund")["country"] == "Germany"
    assert _plan("Paris, France")["country"] == "France"
    assert _plan("Paris, TX")["country"] != "France"
