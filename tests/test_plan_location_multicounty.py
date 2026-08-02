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
KNOWN_MULTI_COUNTY_DOMINANT_LABEL = "Stone County, MS"
# The remaining 5 counties as scripts/build_geo_data.py::parse_zcta_county()
# actually orders them -- by AREALAND_PART (land-area overlap) DESCENDING,
# straight from data/.geo_build_cache/tab20_zcta520_county20_natl.txt. This
# order is neither alphabetical nor county_fips-ascending (28109 sorts
# before 28047 here), so it can only be reproduced by genuinely preserving
# the land-area order -- a real pin, not a tautology.
KNOWN_MULTI_COUNTY_OTHER_LABELS_IN_AREA_ORDER = [
    "George County, MS",
    "Hancock County, MS",
    "Jackson County, MS",
    "Pearl River County, MS",
    "Harrison County, MS",
]

# A ZCTA that maps to exactly one county, for the negative-case pin.
KNOWN_SINGLE_COUNTY_ZIP = "30303"

# 02861 (Pawtucket, RI) -- one of the 137 multi-county ZIPs (per
# data/.geo_build_cache/tab20_zcta520_county20_natl.txt) whose dominant and
# overlapping counties sit in DIFFERENT states. Picked because it makes the
# ", {state_usps}" suffix on each *individual* other_counties entry
# load-bearing: on a same-state ZIP like 39573, a bug that stamped every
# entry with the DOMINANT county's state (instead of each entry's own
# state) would go undetected, since every county there happens to be in MS
# anyway. Here it wouldn't -- Bristol County is in MA, not RI.
CROSS_STATE_MULTI_COUNTY_ZIP = "02861"
CROSS_STATE_DOMINANT_LABEL = "Providence County, RI"
CROSS_STATE_OTHER_LABEL = "Bristol County, MA"

# 20110 (Manassas, VA) -- the dominant unit is a Virginia INDEPENDENT CITY,
# not a Census county proper. Pins the client-facing wording fix: a note
# must not call a set of units "counties" when one of them isn't one.
MIXED_UNIT_TYPE_ZIP = "20110"
MIXED_UNIT_TYPE_DOMINANT_LABEL = "Manassas city, VA"


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

    # The note's SEMANTICS are the deliverable, not just its non-emptiness:
    # it must actually name the correct dominant county...
    assert KNOWN_MULTI_COUNTY_DOMINANT_LABEL in res.note
    # ...AND every real overlapping county, in the documented land-area
    # order (see data/geo/README.md judgment call #4 and
    # scripts/build_geo_data.py::parse_zcta_county()) -- not a subset, not
    # reordered, not silently dropped.
    other_labels = [f"{c['county_name']}, {c['state_usps']}" for c in res.other_counties]
    assert other_labels == KNOWN_MULTI_COUNTY_OTHER_LABELS_IN_AREA_ORDER
    for label in KNOWN_MULTI_COUNTY_OTHER_LABELS_IN_AREA_ORDER:
        assert label in res.note, f"{label!r} missing from note: {res.note!r}"
    # No double-suffixed unit names ("Stone County County, MS").
    assert "County County" not in res.note


def test_cross_state_multi_county_zip_note_uses_each_countys_own_state():
    """02861 (Pawtucket, RI) spans Providence County, RI (dominant) and
    Bristol County, MA. Unlike the all-Mississippi 39573 fixture, a bug
    that stamped every other_counties entry with the DOMINANT county's
    state instead of each entry's own state would read
    "Bristol County, RI" here -- wrong -- so this makes the per-entry
    state_usps suffix genuinely load-bearing."""
    res = pl.resolve_location(CROSS_STATE_MULTI_COUNTY_ZIP)

    assert res.status == "resolved"
    assert res.confidence == 1.0
    assert res.county_count == 2
    assert len(res.other_counties) == 1

    assert CROSS_STATE_DOMINANT_LABEL in res.note
    assert CROSS_STATE_OTHER_LABEL in res.note
    assert "Bristol County, RI" not in res.note
    assert "County County" not in res.note

    other = res.other_counties[0]
    assert f"{other['county_name']}, {other['state_usps']}" == CROSS_STATE_OTHER_LABEL


def test_mixed_unit_type_note_says_counties_or_equivalents():
    """20110 (Manassas, VA) spans a Virginia INDEPENDENT CITY (not a
    Census county) plus a real county. Calling that pair "2 counties" is
    inaccurate client-facing copy; Census's own term of art for a mixed
    set is "counties or equivalents"."""
    res = pl.resolve_location(MIXED_UNIT_TYPE_ZIP)

    assert res.status == "resolved"
    assert res.county_count > 1
    assert MIXED_UNIT_TYPE_DOMINANT_LABEL in res.note
    assert f"spans {res.county_count} counties or equivalents." in res.note
    assert f"spans {res.county_count} counties." not in res.note  # not the bare "counties" wording


def test_all_counties_zip_note_keeps_plain_counties_wording():
    """The flip side of the mixed-unit-type case: when every named unit
    genuinely is a "* County", the note must keep the plain "counties"
    wording, not regress to always saying "counties or equivalents"."""
    res = pl.resolve_location(KNOWN_MULTI_COUNTY_ZIP)
    assert f"spans {res.county_count} counties." in res.note
    assert "counties or equivalents" not in res.note

    res2 = pl.resolve_location(CROSS_STATE_MULTI_COUNTY_ZIP)
    assert f"spans {res2.county_count} counties." in res2.note
    assert "counties or equivalents" not in res2.note


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
