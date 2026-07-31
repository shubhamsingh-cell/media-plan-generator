"""Integrity checks for the committed US location-resolution data files.

Background: `scripts/build_geo_data.py` builds `data/geo/*.tsv` from US
Census public files (place/county/ZCTA gazetteers + 2020 relationship
files -- see `data/geo/README.md` for sources, vintage, and the judgment
calls made during the join). This suite validates the COMMITTED tsv
output directly -- it never re-downloads or re-runs the build -- so it
stays fast, deterministic, and offline.

Covers:
    1. Row-count sanity ranges (generous, not exact -- Census updates the
       source files periodically and exact counts will drift).
    2. Zero-padding invariants: county_fips and zip5 must stay 5-char,
       all-digit STRINGS. This is the #1 way this kind of dataset gets
       silently corrupted (e.g. via a spreadsheet or `int()` coercion
       somewhere upstream) -- see MEMORY.md
       "MPG id-width validator fanout" for a prior incident of this
       exact class of bug in this repo.
    3. Referential integrity: every county_fips referenced from
       us_places.tsv / us_zips.tsv exists in us_counties.tsv.
    4. Pinned known-good spot checks (Atlanta/NYC/SF ZIPs, multi-state
       Springfield).
    5. Lat/lng plausibility for the 50 states + DC + territories.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GEO_DIR = PROJECT_ROOT / "data" / "geo"


def _read_tsv(name: str) -> List[Dict[str, str]]:
    path = GEO_DIR / name
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.fixture(scope="module")
def states() -> List[Dict[str, str]]:
    return _read_tsv("us_states.tsv")


@pytest.fixture(scope="module")
def counties() -> List[Dict[str, str]]:
    return _read_tsv("us_counties.tsv")


@pytest.fixture(scope="module")
def places() -> List[Dict[str, str]]:
    return _read_tsv("us_places.tsv")


@pytest.fixture(scope="module")
def zips() -> List[Dict[str, str]]:
    return _read_tsv("us_zips.tsv")


@pytest.fixture(scope="module")
def county_fips_set(counties: List[Dict[str, str]]) -> set:
    return {c["county_fips"] for c in counties}


# ---------------------------------------------------------------------
# Files exist
# ---------------------------------------------------------------------


def test_all_four_tsv_files_exist():
    for name in ("us_states.tsv", "us_counties.tsv", "us_places.tsv", "us_zips.tsv"):
        path = GEO_DIR / name
        assert path.is_file(), f"missing {path}"
        assert path.stat().st_size > 0, f"{path} is empty"


def test_readme_exists_and_documents_sources():
    readme = GEO_DIR / "README.md"
    assert readme.is_file()
    text = readme.read_text(encoding="utf-8")
    assert "public domain" in text.lower()
    assert "census.gov" in text.lower()
    assert "build_geo_data.py" in text


# ---------------------------------------------------------------------
# Row-count sanity ranges (generous, not exact)
# ---------------------------------------------------------------------


def test_state_row_count_in_range(states):
    assert 50 <= len(states) <= 60, len(states)


def test_county_row_count_in_range(counties):
    assert 3100 <= len(counties) <= 3300, len(counties)


def test_place_row_count_in_range(places):
    assert 29000 <= len(places) <= 33000, len(places)


def test_zip_row_count_in_range(zips):
    assert 33000 <= len(zips) <= 34000, len(zips)


# ---------------------------------------------------------------------
# Zero-padding invariants
# ---------------------------------------------------------------------


def test_every_county_fips_is_5_digit_zero_padded_string(counties):
    for row in counties:
        fips = row["county_fips"]
        assert isinstance(fips, str)
        assert len(fips) == 5, f"county_fips {fips!r} is not 5 chars"
        assert fips.isdigit(), f"county_fips {fips!r} is not all-digit"


def test_every_zip5_is_5_digit_zero_padded_string(zips):
    for row in zips:
        zip5 = row["zip5"]
        assert isinstance(zip5, str)
        assert len(zip5) == 5, f"zip5 {zip5!r} is not 5 chars"
        assert zip5.isdigit(), f"zip5 {zip5!r} is not all-digit"


def test_places_county_fips_is_5_digit_zero_padded_string(places):
    for row in places:
        fips = row["county_fips"]
        assert len(fips) == 5 and fips.isdigit(), f"bad county_fips {fips!r}"


def test_zips_county_fips_is_5_digit_zero_padded_string(zips):
    for row in zips:
        fips = row["county_fips"]
        assert len(fips) == 5 and fips.isdigit(), f"bad county_fips {fips!r}"


def test_leading_zero_fips_and_zips_present():
    """Regression guard: a naive int() coercion anywhere upstream would
    have silently dropped leading zeros. Assert we actually have rows
    that WOULD expose that bug (e.g. Connecticut '09xxx' counties,
    Puerto Rico zips starting with '0')."""
    counties_rows = _read_tsv("us_counties.tsv")
    zips_rows = _read_tsv("us_zips.tsv")
    assert any(r["county_fips"].startswith("0") for r in counties_rows)
    assert any(r["zip5"].startswith("0") for r in zips_rows)


# ---------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------


def test_every_place_county_fips_exists_in_counties(places, county_fips_set):
    missing = {r["county_fips"] for r in places} - county_fips_set
    assert not missing, f"places reference county_fips not in us_counties.tsv: {sorted(missing)[:10]}"


def test_every_zip_county_fips_exists_in_counties(zips, county_fips_set):
    missing = {r["county_fips"] for r in zips} - county_fips_set
    assert not missing, f"zips reference county_fips not in us_counties.tsv: {sorted(missing)[:10]}"


def test_every_place_and_zip_state_usps_exists_in_states(places, zips, states):
    valid = {s["state_usps"] for s in states}
    missing_places = {r["state_usps"] for r in places} - valid
    missing_zips = {r["state_usps"] for r in zips} - valid
    assert not missing_places, missing_places
    assert not missing_zips, missing_zips


# ---------------------------------------------------------------------
# Never dedupe places by name alone -- (name, state) must be preserved,
# and cross-state collisions must NOT collapse into one row.
# ---------------------------------------------------------------------


def test_places_keep_every_name_state_pair_as_its_own_row(places):
    seen = set()
    for row in places:
        key = (row["display_name"], row["state_usps"])
        # Not asserting global uniqueness of (name, state) -- Census does
        # occasionally have two same-named CDPs in one state -- but the
        # composite place_key + state must not have been collapsed across
        # different states, which is the actual bug class this test guards.
        seen.add(key)
    springfield_states = {state for (name, state) in seen if name == "Springfield"}
    assert len(springfield_states) >= 5, springfield_states


def test_springfield_appears_in_at_least_5_distinct_states(places):
    states_with_springfield = {r["state_usps"] for r in places if r["display_name"] == "Springfield"}
    assert len(states_with_springfield) >= 5, states_with_springfield


def test_place_key_is_scoped_per_state_not_globally_deduped(places):
    """Two different states' Springfields must have DIFFERENT place_key
    values (state-scoped), proving the matching key can't collide two
    real, distinct cities into one record."""
    springfield_keys = {r["place_key"] for r in places if r["display_name"] == "Springfield"}
    assert len(springfield_keys) >= 5, springfield_keys


# ---------------------------------------------------------------------
# Pinned known-good spot checks
# ---------------------------------------------------------------------


def _zip_row(zips: List[Dict[str, str]], zip5: str) -> Dict[str, str]:
    matches = [r for r in zips if r["zip5"] == zip5]
    assert matches, f"zip {zip5} not found in us_zips.tsv"
    return matches[0]


def test_zip_10001_is_new_york_ny(zips):
    row = _zip_row(zips, "10001")
    assert row["primary_city"] == "New York"
    assert row["state_usps"] == "NY"


def test_zip_94103_is_san_francisco_ca(zips):
    row = _zip_row(zips, "94103")
    assert row["primary_city"] == "San Francisco"
    assert row["state_usps"] == "CA"


def test_zip_30303_is_atlanta_ga(zips):
    """The brief's original pin (30301 -> Atlanta, GA) does not resolve:
    30301 is a real USPS ZIP (a downtown-Atlanta PO-Box zip) but has no
    corresponding Census ZCTA in the 2024 Gazetteer -- confirmed absent
    from 2024_Gaz_zcta_national.txt at build time. Since Census's ZCTA is
    the geographic proxy this whole dataset is built on, ZIPs without a
    ZCTA (mostly PO-Box-only/unique-purpose ZIPs) cannot appear here; see
    data/geo/README.md "Judgment calls" #5. 30303 is the nearest
    ZCTA-backed, still-Atlanta-GA zip and is used for this spot check
    instead so the assertion pins a real row rather than a fabricated one.
    """
    row = _zip_row(zips, "30303")
    assert row["primary_city"] == "Atlanta"
    assert row["state_usps"] == "GA"


MULTI_COUNTY_CITY_PINS = [
    # (display_name, state_usps, expected county_fips)
    ("Atlanta", "GA", "13121"),          # Fulton, not DeKalb (13089)
    ("Houston", "TX", "48201"),          # Harris, not Fort Bend (48157)
    ("Dallas", "TX", "48113"),           # Dallas, not Collin (48085)
    ("Kansas City", "MO", "29095"),      # Jackson, not Cass (29037)
    ("Oklahoma City", "OK", "40109"),    # Oklahoma, not Canadian (40017)
    ("Columbus", "OH", "39049"),         # Franklin, not Delaware (39041)
    ("Chicago", "IL", "17031"),          # Cook
    ("Los Angeles", "CA", "06037"),      # Los Angeles
    ("Phoenix", "AZ", "04013"),          # Maricopa
    ("Seattle", "WA", "53033"),          # King
]


def _place_row(places: List[Dict[str, str]], display_name: str, state_usps: str) -> Dict[str, str]:
    matches = [r for r in places if r["display_name"] == display_name and r["state_usps"] == state_usps]
    assert matches, f"{display_name}, {state_usps} not found in us_places.tsv"
    return matches[0]


@pytest.mark.parametrize("display_name,state_usps,expected_fips", MULTI_COUNTY_CITY_PINS)
def test_multi_county_city_county_fips_regression_pins(places, display_name, state_usps, expected_fips):
    """Regression pin for the "first-listed county" bug: national_place2020's
    COUNTIES column lists a multi-county place's constituent counties in
    essentially alphabetical-FIPS order, not population/area order, so
    naively taking the first entry silently picked the wrong county for
    every major multi-county city (Atlanta -> DeKalb instead of Fulton,
    Houston -> Fort Bend instead of Harris, etc.). Fixed in
    scripts/build_geo_data.py by refine_place_counties_by_zip_majority(),
    which votes over each place's own (already-correct) ZIP county_fips
    values instead of trusting Census's COUNTIES ordering. See
    data/geo/README.md judgment call #2 for the full writeup."""
    row = _place_row(places, display_name, state_usps)
    assert row["county_fips"] == expected_fips, (
        f"{display_name}, {state_usps}: got county_fips={row['county_fips']!r}, "
        f"expected {expected_fips!r} (county_confidence={row.get('county_confidence')!r})"
    )


def test_county_confidence_column_only_holds_allowed_values(places):
    allowed = {"zip_majority", "census_first_listed"}
    values = {r["county_confidence"] for r in places}
    assert values <= allowed, f"unexpected county_confidence values: {values - allowed}"
    assert values, "county_confidence column is empty"


def test_census_first_listed_share_is_below_documented_ceiling(places):
    """The zip-majority refinement can't resolve every place (small CDPs
    that never win the largest-ZIP-overlap tie-break for any ZIP fall back
    to the original Census first-listed county). This pins a ceiling on
    that fallback share so a future regression that breaks the ZIP<->place
    matching silently (e.g. a normalization change that stops matching
    almost everything) shows up as a failing test instead of quietly
    degrading data quality. Observed on the 2026-07-31 build: 28.8% of
    places (9,107 of 31,657) fell back -- ceiling set at 40% to give some
    headroom for Census vintage churn without masking a real regression."""
    total = len(places)
    fallback = sum(1 for r in places if r["county_confidence"] == "census_first_listed")
    share = fallback / total if total else 1.0
    assert share < 0.40, f"census_first_listed share {share:.1%} exceeds the 40% documented ceiling"


def test_zip_30301_absent_is_a_known_zcta_gap_not_silent_data_loss(zips):
    """Documents (rather than hides) the 30301 gap so a future re-run that
    somehow re-introduces it doesn't look like a regression, and so nobody
    "fixes" this test by fabricating a fake row for 30301."""
    assert not any(r["zip5"] == "30301" for r in zips)


# ---------------------------------------------------------------------
# Lat/lng plausibility (allowing AK/HI/territories)
# ---------------------------------------------------------------------

# Generous bounding box covering CONUS + AK + HI + PR/VI/GU/AS/MP.
LAT_RANGE = (-15.0, 72.0)  # American Samoa ~ -14, Alaska north slope ~ 71.5
LNG_RANGE = (-180.0, 180.0)  # Alaska crosses the antimeridian; Guam ~144E


def _assert_plausible_latlng(lat_str: str, lng_str: str, context: str) -> None:
    lat = float(lat_str)
    lng = float(lng_str)
    assert LAT_RANGE[0] <= lat <= LAT_RANGE[1], f"{context}: implausible lat {lat}"
    assert LNG_RANGE[0] <= lng <= LNG_RANGE[1], f"{context}: implausible lng {lng}"


def test_county_latlng_plausible(counties):
    for row in counties:
        _assert_plausible_latlng(row["lat"], row["lng"], f"county {row['county_fips']}")


def test_place_latlng_plausible(places):
    for row in places:
        _assert_plausible_latlng(row["lat"], row["lng"], f"place {row['place_key']}")


def test_zip_latlng_plausible(zips):
    for row in zips:
        _assert_plausible_latlng(row["lat"], row["lng"], f"zip {row['zip5']}")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
