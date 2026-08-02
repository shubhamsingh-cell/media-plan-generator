"""Regression tests for the ZIP -> multi-county disclosure in
plan_location.py::_resolve_zip().

Background (the defect): scripts/build_geo_data.py::parse_zcta_county()
used to keep only the single county with the largest AREALAND_PART
overlap per ZCTA and silently discard every other county the ZIP
overlaps. 10,186 of 33,791 ZCTAs (~30%, max 6 -- e.g. ZIP 39573) span 2+
counties, yet _resolve_zip() returned status="resolved", confidence=1.0,
note="" for every ZIP hit -- asserting county certainty the data never
had, with zero user-visible disclosure.

The fix is purely additive: county_fips/status/confidence are UNCHANGED
for multi-county ZIPs (the ZIP itself really is resolved with certainty;
only the county attribution is a best guess). The new signal is
`county_count`, a populated `note`, and a dedicated `other_counties`
list -- see plan_location.py::LocationResolution and ::_resolve_zip.
"""

from __future__ import annotations

import plan_location as pl

# Wiggins, MS -- the example cited when the defect was found. Stone
# County (28131) has the largest land-area overlap; the ZCTA also
# genuinely overlaps five neighboring Mississippi counties that the old
# single-winner join silently dropped.
KNOWN_MULTI_COUNTY_ZIP = "39573"
KNOWN_MULTI_COUNTY_DOMINANT_FIPS = "28131"

# A ZCTA that maps to exactly one county, for the negative-case pin.
KNOWN_SINGLE_COUNTY_ZIP = "30303"


def test_multi_county_zip_discloses_county_count_note_and_other_counties():
    res = pl.resolve_location(KNOWN_MULTI_COUNTY_ZIP)

    # Additive design pin: the ZIP is still resolved with full confidence
    # -- multi-county disclosure must NEVER downgrade status/confidence.
    assert res.status == "resolved"
    assert res.confidence == 1.0
    assert res.kind == "zip"
    assert res.county_fips == KNOWN_MULTI_COUNTY_DOMINANT_FIPS

    # The new disclosure signal.
    assert res.county_count > 1
    assert res.note != ""
    assert res.other_counties

    # The dominant county must never also appear in other_counties.
    other_fips = {c["county_fips"] for c in res.other_counties}
    assert res.county_fips not in other_fips

    # other_counties count + dominant == county_count.
    assert len(res.other_counties) + 1 == res.county_count

    # Every other_counties entry carries the documented shape.
    for entry in res.other_counties:
        assert set(entry.keys()) == {"county_name", "county_fips", "state_usps"}
        assert entry["county_name"]
        assert entry["state_usps"]

    # Note names the dominant county and the count -- honest disclosure,
    # not a generic placeholder.
    assert str(res.county_count) in res.note
    assert KNOWN_MULTI_COUNTY_ZIP in res.note


def test_single_county_zip_has_no_multicounty_disclosure():
    res = pl.resolve_location(KNOWN_SINGLE_COUNTY_ZIP)

    assert res.status == "resolved"
    assert res.county_count == 1
    assert res.note == ""
    assert res.other_counties == []


def test_multicounty_disclosure_is_purely_additive_to_dict():
    """The public to_dict() contract gains the two new fields but never
    drops or renames the existing ones a multi-county ZIP already relied
    on (county_fips, status, confidence untouched -- see module
    docstring)."""
    res = pl.resolve_location(KNOWN_MULTI_COUNTY_ZIP)
    d = res.to_dict()
    assert d["status"] == "resolved"
    assert d["confidence"] == 1.0
    assert d["county_fips"] == KNOWN_MULTI_COUNTY_DOMINANT_FIPS
    assert d["county_count"] > 1
    assert d["other_counties"]


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
