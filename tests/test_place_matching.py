"""Regression tests: the ACS place matcher never returns a DIFFERENT place.

VERIFIER FIND (live scan of ~115 common employer cities): ``Wayne, PA`` (Chester
County, FIPS 42029) resolved to ``Wayne Heights, PA`` (Franklin County, 42055,
~180 km away) and published its population (3,293) with ``geo_matches_location``
True. ROOT CAUSE: ``match_place_member`` accepted a UNIQUE ``<city> ...`` prefix
match without looking at what the extra text was. A prefix match is now accepted
only when the extra words are a legal-form suffix of the SAME place ("city
(balance)", "metro government", ...); a candidate that carries a county
qualifier ("Burbank (Santa Clara County)") is accepted only when that county is
the target's. Anything else falls back to the labelled county figure.

The member lists are the REAL Data USA responses recorded in
``tests/fixtures/public_data_sources/datausa_members_place.json``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import public_data_sources as pds  # noqa: E402
from tests.public_data_fakes import FakeNet, load  # noqa: E402


def _members(search: str) -> List[Dict[str, Any]]:
    return load("datausa_members_place.json")["by_search"][search]["members"]


def _match(city: str, state: str, county: str, search: str = "") -> Any:
    hit = pds.match_place_member(city, state, _members(search or city), county)
    return hit[1] if hit else None


def test_wayne_pa_never_becomes_wayne_heights() -> None:
    """The verifier's find: a different place in another county."""
    names = {m["caption"] for m in _members("Wayne")}
    assert "Wayne Heights, PA" in names  # the trap really is in the recorded result
    assert _match("Wayne", "PA", "Chester County") is None


@pytest.mark.parametrize(
    "city, state, county, expected",
    [
        # exact names win over look-alikes (New Columbia, Franklin Park, ...)
        ("Lancaster", "PA", "Lancaster County", "Lancaster, PA"),
        ("Columbia", "PA", "Lancaster County", "Columbia, PA"),
        ("Springfield", "MO", "Greene County", "Springfield, MO"),
        # same-name places: the county qualifier picks the target's own
        ("Franklin", "PA", "Venango County", "Franklin, PA"),
        ("Franklin", "PA", "Cambria County", "Franklin (Cambria County), PA"),
        ("Burbank", "CA", "Los Angeles County", "Burbank, CA"),
        ("Burbank", "CA", "Santa Clara County", "Burbank (Santa Clara County), CA"),
        ("Mountain View", "CA", "Santa Clara County", "Mountain View, CA"),
        (
            "Mountain View",
            "CA",
            "Contra Costa County",
            "Mountain View (Contra Costa County), CA",
        ),
        # legal-name suffixes of the SAME place are still accepted
        (
            "Louisville",
            "KY",
            "Jefferson County",
            "Louisville/Jefferson County metro government (balance), KY",
        ),
        ("Indianapolis", "IN", "Marion County", "Indianapolis city (balance), IN"),
    ],
)
def test_same_name_siblings_resolve_to_the_targets_own_place(
    city: str, state: str, county: str, expected: str
) -> None:
    assert _match(city, state, county) == expected


def test_nashville_uses_its_census_legal_name() -> None:
    hit = pds.match_place_member(
        "Nashville-Davidson metropolitan government",
        "TN",
        _members("Nashville"),
        "Davidson County",
    )
    assert hit and hit[1] == "Nashville-Davidson metropolitan government (balance), TN"


def test_a_state_with_no_such_place_matches_nothing() -> None:
    # recorded search for "Springfield" has no Pennsylvania place at all
    assert _match("Springfield", "PA", "Delaware County") is None


def test_an_unverifiable_county_qualified_place_is_rejected() -> None:
    """Only a county-qualified candidate in the list, and it is NOT the target's
    county: it must not be returned (Franklin PA, Cambria sibling only)."""
    only_qualified = [
        m for m in _members("Franklin") if m["caption"] == "Franklin (Cambria County), PA"
    ]
    assert pds.match_place_member("Franklin", "PA", only_qualified, "Venango County") is None
    assert pds.match_place_member("Franklin", "PA", only_qualified, "") is None
    assert pds.match_place_member("Franklin", "PA", only_qualified, "Cambria County")


@pytest.mark.parametrize(
    "leftover",
    [
        "city",
        "town",
        "village",
        "borough",
        "township",
        "cdp",
        "municipality",
        "city and borough",
        "jefferson county metro government",
        "davidson metropolitan government",
        "clarke county unified government",
        "richmond county consolidated government",
    ],
)
def test_legal_form_suffixes_are_accepted(leftover: str) -> None:
    assert pds.is_legal_name_suffix(leftover)


@pytest.mark.parametrize(
    "leftover",
    ["heights", "park", "acres", "hills", "north", "junction", "boro", "heights city", ""],
)
def test_other_extra_words_mean_a_different_place(leftover: str) -> None:
    assert not pds.is_legal_name_suffix(leftover)


# ---------------------------------------------------------------------------
# Full stack: the plan location falls back to the labelled county
# ---------------------------------------------------------------------------


@pytest.fixture
def net(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    pds.clear_caches()
    fake = FakeNet().install(monkeypatch)
    yield fake
    pds.clear_caches()


def test_wayne_pa_plan_location_gets_chester_county_labelled_not_wayne_heights(
    net: FakeNet,
) -> None:
    entry = api_enrichment.fetch_location_demographics(["Wayne, PA"])["Wayne, PA"]

    assert entry["geo_level"] == "County"
    assert entry["geo_name"] == "Chester County, PA"
    assert entry["geo_matches_location"] is False
    assert entry["area_population"] == 547840  # Chester County, recorded
    assert entry["fallback_reason"] == "place_not_in_acs"
    assert "population" not in entry  # never a city number
    # the other county's place was never even queried for data
    assert not any("4281808" in u for u in net.urls("api.datausa.io"))
