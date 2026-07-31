"""Integrity checks for the committed CBSA (Core Based Statistical Area)
county-join layer.

Background: `scripts/build_cbsa_data.py` builds `data/geo/cbsa_by_county.tsv`
by downloading the Census Bureau / OMB "List 1" metro/micropolitan
delineation file and joining it onto the county_fips values already present
in the committed `data/geo/us_counties.tsv` (built separately by
`scripts/build_geo_data.py`) -- see `data/geo/README.md` for the source
citation and vintage. This suite validates the COMMITTED tsv output
directly -- it never re-downloads or re-runs the build -- so it stays fast,
deterministic, and offline. Mirrors the structure of
`tests/test_geo_data_integrity.py`.

Covers:
    1. File exists and is non-empty, with the expected header.
    2. Row-count sanity range (generous, not exact -- OMB updates
       delineations periodically and exact counts will drift).
    3. Zero-padding invariant: county_fips must stay a 5-char, all-digit
       STRING (see MEMORY.md "MPG id-width validator fanout" for a prior
       incident of this exact class of bug in this repo).
    4. cbsa_code is a 5-char, all-digit string.
    5. area_type only ever "Metropolitan Statistical Area" or
       "Micropolitan Statistical Area" -- never a fabricated value.
    6. Referential integrity: every county_fips in this file exists in
       us_counties.tsv (the join target), and at most one CBSA per county.
    7. Pinned known-good spot checks for major metros.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GEO_DIR = PROJECT_ROOT / "data" / "geo"

ALLOWED_AREA_TYPES = {"Metropolitan Statistical Area", "Micropolitan Statistical Area"}


def _read_tsv(name: str) -> List[Dict[str, str]]:
    path = GEO_DIR / name
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.fixture(scope="module")
def cbsa_rows() -> List[Dict[str, str]]:
    return _read_tsv("cbsa_by_county.tsv")


@pytest.fixture(scope="module")
def county_fips_set() -> set:
    return {r["county_fips"] for r in _read_tsv("us_counties.tsv")}


# ---------------------------------------------------------------------
# File exists, expected header
# ---------------------------------------------------------------------


def test_cbsa_tsv_exists():
    path = GEO_DIR / "cbsa_by_county.tsv"
    assert path.is_file(), f"missing {path}"
    assert path.stat().st_size > 0, f"{path} is empty"


def test_cbsa_tsv_has_expected_header():
    path = GEO_DIR / "cbsa_by_county.tsv"
    with path.open(encoding="utf-8", newline="") as f:
        header = next(csv.reader(f, delimiter="\t"))
    assert header == ["county_fips", "cbsa_code", "cbsa_title", "area_type"]


# ---------------------------------------------------------------------
# Row-count sanity range (generous, not exact)
# ---------------------------------------------------------------------


def test_cbsa_row_count_in_range(cbsa_rows):
    # Observed 2026-07-31 build: 1,915 rows (1,252 metro + 663 micro) from
    # the July 2023 OMB delineation. Generous range to tolerate a future
    # OMB delineation update without becoming a brittle exact pin.
    assert 1500 <= len(cbsa_rows) <= 2200, len(cbsa_rows)


# ---------------------------------------------------------------------
# Zero-padding / format invariants
# ---------------------------------------------------------------------


def test_every_cbsa_county_fips_is_5_digit_zero_padded_string(cbsa_rows):
    for row in cbsa_rows:
        fips = row["county_fips"]
        assert isinstance(fips, str)
        assert len(fips) == 5, f"county_fips {fips!r} is not 5 chars"
        assert fips.isdigit(), f"county_fips {fips!r} is not all-digit"


def test_every_cbsa_code_is_5_digit_string(cbsa_rows):
    for row in cbsa_rows:
        code = row["cbsa_code"]
        assert isinstance(code, str)
        assert len(code) == 5, f"cbsa_code {code!r} is not 5 chars"
        assert code.isdigit(), f"cbsa_code {code!r} is not all-digit"


def test_leading_zero_county_fips_present(cbsa_rows):
    """Regression guard: a naive int() coercion anywhere upstream would have
    silently dropped leading zeros (e.g. Connecticut '09xxx' counties)."""
    assert any(r["county_fips"].startswith("0") for r in cbsa_rows)


def test_area_type_only_holds_allowed_values(cbsa_rows):
    values = {r["area_type"] for r in cbsa_rows}
    assert values <= ALLOWED_AREA_TYPES, f"unexpected area_type values: {values - ALLOWED_AREA_TYPES}"
    assert values, "area_type column is empty"


def test_cbsa_title_is_never_blank(cbsa_rows):
    for row in cbsa_rows:
        assert row["cbsa_title"].strip(), f"blank cbsa_title for county_fips {row['county_fips']}"


# ---------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------


def test_every_cbsa_county_fips_exists_in_counties(cbsa_rows, county_fips_set):
    missing = {r["county_fips"] for r in cbsa_rows} - county_fips_set
    assert not missing, f"cbsa rows reference county_fips not in us_counties.tsv: {sorted(missing)[:10]}"


def test_at_most_one_cbsa_per_county(cbsa_rows):
    seen = [r["county_fips"] for r in cbsa_rows]
    assert len(seen) == len(set(seen)), "a county_fips appears more than once in cbsa_by_county.tsv"


def test_not_every_county_has_a_cbsa(cbsa_rows, county_fips_set):
    """Rural/unincorporated counties genuinely sit outside any CBSA -- this
    file must not claim 100% coverage (that would mean a fabricated row was
    added for a county with no real CBSA membership)."""
    covered = {r["county_fips"] for r in cbsa_rows}
    assert covered < county_fips_set, "every county has a CBSA -- suspiciously complete, check for fabrication"


# ---------------------------------------------------------------------
# Pinned known-good spot checks (matches
# tests/test_geo_data_integrity.py::MULTI_COUNTY_CITY_PINS's county_fips
# values, so a metro's CBSA and its county assignment are cross-checked
# against the same real-world city).
# ---------------------------------------------------------------------

MAJOR_METRO_CBSA_PINS = [
    # (county_fips, expected substring in cbsa_title, expected area_type)
    ("13121", "Atlanta", "Metropolitan Statistical Area"),          # Fulton County, GA
    ("48201", "Houston", "Metropolitan Statistical Area"),          # Harris County, TX
    ("48113", "Dallas", "Metropolitan Statistical Area"),           # Dallas County, TX
    ("29095", "Kansas City", "Metropolitan Statistical Area"),      # Jackson County, MO
    ("40109", "Oklahoma City", "Metropolitan Statistical Area"),    # Oklahoma County, OK
    ("39049", "Columbus", "Metropolitan Statistical Area"),         # Franklin County, OH
    ("17031", "Chicago", "Metropolitan Statistical Area"),          # Cook County, IL
    ("06037", "Los Angeles", "Metropolitan Statistical Area"),      # Los Angeles County, CA
    ("04013", "Phoenix", "Metropolitan Statistical Area"),          # Maricopa County, AZ
    ("53033", "Seattle", "Metropolitan Statistical Area"),          # King County, WA
]


def _cbsa_row_for(cbsa_rows: List[Dict[str, str]], county_fips: str) -> Dict[str, str]:
    matches = [r for r in cbsa_rows if r["county_fips"] == county_fips]
    assert matches, f"county_fips {county_fips} not found in cbsa_by_county.tsv"
    return matches[0]


@pytest.mark.parametrize("county_fips,expected_substring,expected_area_type", MAJOR_METRO_CBSA_PINS)
def test_major_metro_cbsa_pins(cbsa_rows, county_fips, expected_substring, expected_area_type):
    row = _cbsa_row_for(cbsa_rows, county_fips)
    assert expected_substring in row["cbsa_title"], (
        f"county_fips {county_fips}: cbsa_title {row['cbsa_title']!r} does not contain {expected_substring!r}"
    )
    assert row["area_type"] == expected_area_type


def test_a_known_micropolitan_county_is_labeled_micropolitan(cbsa_rows):
    """Regression guard against a build that mislabels every row as metro --
    Zapata County, TX (48505) is a real, small Micropolitan Statistical Area
    (Zapata, TX) in the source delineation file."""
    row = _cbsa_row_for(cbsa_rows, "48505")
    assert row["area_type"] == "Micropolitan Statistical Area"
    assert "Zapata" in row["cbsa_title"]


# ---------------------------------------------------------------------
# Hard requirement: never any mention of a commercial ratings vendor.
# ---------------------------------------------------------------------


def test_cbsa_data_never_mentions_a_commercial_ratings_vendor():
    path = GEO_DIR / "cbsa_by_county.tsv"
    text = path.read_text(encoding="utf-8").lower()
    assert "nielsen" not in text


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
