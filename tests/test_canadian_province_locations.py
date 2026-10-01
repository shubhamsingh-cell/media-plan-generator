"""Canadian "City, PROV" locations stay Canadian (independent verifier on
2ad8904, 2026-10-01).

Pre-fix: "Victoria, BC" was fuzzy-"corrected" to "Victoria Vera, TX" and
written into the plan, "Paris, ON" to "Pearson, GA", "Columbia, BC" to
"Columbia City, IN", "Whitehorse, YT" to "White Horse, NJ", "St. John's,
NL" to "St. Johnsville, NY"; and every "City, PROV" left the plan US-only
in USD because plan_currency did not know province codes.
"""

from __future__ import annotations

import pytest

import app
import plan_currency
import plan_geo
import plan_location

CANADIAN = [
    "Victoria, BC", "Columbia, BC", "Vancouver, BC", "Calgary, AB", "Edmonton, AB",
    "Winnipeg, MB", "Moncton, NB", "St. John's, NL", "Halifax, NS", "Yellowknife, NT",
    "Iqaluit, NU", "Paris, ON", "London, ON", "Toronto, ON", "Charlottetown, PE",
    "Montreal, QC", "Regina, SK", "Whitehorse, YT", "Victoria, British Columbia",
    "London, Ontario", "Quebec City, Quebec", "Halifax, Nova Scotia",
]
US_LOOKALIKES = ["Columbia, SC", "Paris, TX", "Victoria, TX", "Ontario, CA", "London, KY", "Windsor, CT"]


@pytest.mark.parametrize("loc", CANADIAN)
def test_province_locations_are_never_rewritten_to_us_towns(loc):
    res = plan_location.resolve_location(loc)
    assert res.matched_via == "non_us_signal" and not res.state_usps, (loc, res.to_dict())
    data = {"locations": [loc]}
    app._resolve_and_rewrite_locations(data)
    assert data["locations"] == [loc]


@pytest.mark.parametrize("loc", [c for c in CANADIAN if not c.endswith(", NL")])
def test_province_locations_plan_in_canada(loc):
    assert plan_currency.currency_for_country(loc) == "CAD"
    assert plan_geo.is_us_plan({"locations": [loc]}) is False
    code, basis = plan_currency.currency_for_plan_with_basis({"locations": [loc], "budget": "100,000"})
    assert (code, basis) == ("CAD", "market")


@pytest.mark.parametrize("loc", US_LOOKALIKES)
def test_us_state_codes_still_win(loc):
    res = plan_location.resolve_location(loc)
    assert res.state_usps == loc.rsplit(", ", 1)[1], (loc, res.to_dict())
    assert plan_currency.currency_for_country(loc) == "USD"


def test_nl_stays_the_netherlands_for_currency():
    # Documented ambiguity: NL is also the Netherlands' ISO code. Both
    # readings are non-US, so the resolver no longer rewrites either; the
    # currency table keeps EUR (write "Newfoundland" to plan in CAD).
    assert plan_currency.currency_for_country("Amsterdam, NL") == "EUR"
    assert plan_currency.currency_for_country("St. John's, Newfoundland") == "CAD"
