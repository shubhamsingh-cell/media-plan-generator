"""R2 (review 2026-10-11): secondary cities named in an MSA title keep the metro.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import gold_standard  # noqa: E402
import plan_location  # noqa: E402
import research  # noqa: E402


def _res(loc):
    return plan_location.resolve_locations([loc])[0].to_dict()


# ── R2: secondary cities named in an MSA title keep the metro figures ──────
SECONDARY = {
    "Fort Worth, TX": "Dallas-Fort Worth-Arlington MSA",
    "Arlington, TX": "Dallas-Fort Worth-Arlington MSA",
    "Oakland, CA": "San Francisco-Oakland-Berkeley MSA",
    "Jersey City, NJ": "New York-Newark-Jersey City MSA",
    "St. Petersburg, FL": "Tampa-St. Petersburg-Clearwater MSA",
    "Mesa, AZ": "Phoenix-Mesa-Chandler MSA",
    "Aurora, CO": "Denver-Aurora-Lakewood MSA",
    "Round Rock, TX": "Austin-Round Rock-Georgetown MSA",
    "Long Beach, CA": "Los Angeles-Long Beach-Anaheim MSA",
    "Cambridge, MA": "Boston-Cambridge-Newton MSA",
    "Alexandria, VA": "Washington-Arlington-Alexandria MSA",
    "Virginia Beach, VA": "Virginia Beach-Norfolk-Newport News MSA",
    "Vancouver, WA": "Portland-Vancouver-Hillsboro MSA",
    "Hillsboro, OR": "Portland-Vancouver-Hillsboro MSA",
}


@pytest.mark.parametrize("city,msa", sorted(SECONDARY.items()))
def test_secondary_city_gets_its_msas_figures(city, msa):
    info = research.get_location_info(city, _res(city))
    assert info["metro_name"] == msa
    assert info["geo_level"] == "metro"


def test_cbsa_match_does_not_cross_states():
    # same principal-city name, different state: no borrowed metro
    assert research.get_location_info("Columbus, GA", _res("Columbus, GA"))["geo_level"] != "metro"
    assert research.get_location_info("Portland, ME", _res("Portland, ME"))["metro_name"].startswith(
        "Portland-South Portland"
    )


