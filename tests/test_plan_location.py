"""Tests for plan_location.py, the runtime US location resolver built on
top of the committed `data/geo/*.tsv` reference tables.

See plan_location.py's module docstring for the resolution-order rules,
the fuzzy-match cutoff rationale, and the ambiguity tiebreak policy this
suite pins.
"""

from __future__ import annotations

import concurrent.futures as cf
import re
import time

import pytest

import plan_location as pl

# The 32-entry legacy dict this module's fuzzy path (rule 9) replaces.
# Copied verbatim from app.py's `_LOCATION_CORRECTIONS`
# (S49 FIX / Issue 14, around app.py:15576-15607).
_LEGACY_LOCATION_CORRECTIONS = {
    "ithica": "Ithaca",
    "harrisburgh": "Harrisburg",
    "murfeesboro": "Murfreesboro",
    "pittsburg": "Pittsburgh",
    "albuquerque": "Albuquerque",
    "cincinatti": "Cincinnati",
    "colombus": "Columbus",
    "detriot": "Detroit",
    "houstan": "Houston",
    "philladelphia": "Philadelphia",
    "phoneix": "Phoenix",
    "sacremento": "Sacramento",
    "seatle": "Seattle",
    "tuscon": "Tucson",
    "milwakee": "Milwaukee",
    "minnapolis": "Minneapolis",
    "indianpolis": "Indianapolis",
    "nashvile": "Nashville",
    "lousville": "Louisville",
    "baltamore": "Baltimore",
    "richmand": "Richmond",
    "charlote": "Charlotte",
    "ralegh": "Raleigh",
    "memhpis": "Memphis",
    "knoxvile": "Knoxville",
    "cleavland": "Cleveland",
    "bufalo": "Buffalo",
    "rochestor": "Rochester",
    "siracuse": "Syracuse",
    "sprinfield": "Springfield",
    "talahassee": "Tallahassee",
    "jackonville": "Jacksonville",
}

# Two legacy keys are real, unrelated US place names on their own (not
# actual misspellings) -- see plan_location.py's module docstring. These
# resolve via the exact bare-city lookup (rule 5), not fuzzy correction
# (rule 9), which is the more honest behavior: "pittsburg" is a real place
# in 5 states, and "albuquerque" is already correctly spelled in the legacy
# dict (a no-op entry).
_EXACT_MATCH_NOT_FUZZY = {"pittsburg", "albuquerque"}


# ---------------------------------------------------------------------------
# Rule 1: ZIP / ZIP+4
# ---------------------------------------------------------------------------
def test_zip_exact():
    r = pl.resolve_location("30303")
    assert r.status == "resolved"
    assert r.kind == "zip"
    assert r.zip5 == "30303"
    assert r.state_usps == "GA"
    assert r.display_name == "Atlanta"


def test_zip_plus_four_truncates_to_zip5():
    r = pl.resolve_location("30303-1234")
    assert r.status == "resolved"
    assert r.zip5 == "30303"
    assert r.state_usps == "GA"


def test_zip_zero_padding_preserved_as_string():
    r = pl.resolve_location("02134")
    assert r.status == "resolved"
    assert r.zip5 == "02134"
    assert isinstance(r.zip5, str)
    assert r.state_usps == "MA"


def test_zip_30301_gap_is_graceful_and_helpful():
    """30301 is a real Atlanta PO-Box-only USPS zip absent from the ZCTA
    dataset (data/geo/README.md judgment call 5). Must not crash, must
    not silently pretend it resolved, and must be helpful."""
    r = pl.resolve_location("30301")
    assert r.status == "unresolved"
    assert r.kind == "zip"
    assert r.zip5 == "30301"
    assert "30301" in r.note
    # The note is read by clients in the wizard, so it states the consequence
    # for their plan and leaks no internal vocabulary or repo paths.
    assert "stays in your plan" in r.note
    for leak in ("ZCTA", "tabulation", "README", "data/geo", "resolver", "--"):
        assert leak.lower() not in r.note.lower(), f"internal detail {leak!r} leaked into client-facing note"
    # Helpful: suggests the nearby real ZCTA-backed Atlanta zip.
    assert r.alternatives, "expected a nearby-ZIP suggestion"
    assert r.alternatives[0]["zip5"] == "30303"
    assert r.alternatives[0]["state_usps"] == "GA"


def test_boise_is_not_silently_corrected_to_another_city():
    """Regression, found live: Census stores Boise as "Boise City", so the
    bare name matched no place and fell through to fuzzy, which "corrected"
    it to Bowie, AZ -- a confident wrong answer with a checkmark, on the very
    panel built to prevent those. Fuzzy must never see a real metro."""
    r = pl.resolve_location("Boise")
    assert r.status == "resolved", f"expected resolved, got {r.status} ({r.display_name})"
    assert r.state_usps == "ID"
    assert "boise" in (r.display_name or "").lower()


@pytest.mark.parametrize(
    "query,expect_state,expect_fragment",
    [
        ("Boise", "ID", "boise"),
        ("Honolulu", "HI", "honolulu"),
        ("Nashville", "TN", "nashville"),
        ("Lexington", "KY", "lexington"),
        ("Augusta", "GA", "augusta"),
        ("Macon", "GA", "macon"),
        ("Athens", "GA", "athens"),
        ("Butte", "MT", "butte"),
        ("Saint Louis", "MO", "louis"),
        ("St. Louis", "MO", "louis"),
        ("Carson City", "NV", "carson city"),
        ("Kansas City", "MO", "kansas city"),
        ("Saint Paul", "MN", "paul"),
    ],
)
def test_major_metros_resolve_to_the_right_state(query, expect_state, expect_fragment):
    """Census legal names ("Urban Honolulu", "Lexington-Fayette urban
    county") and Saint/St. spellings must not make a major market
    unreachable or land it in the wrong state."""
    r = pl.resolve_location(query)
    assert r.status in ("resolved", "ambiguous"), f"{query} -> {r.status}"
    assert r.state_usps == expect_state, f"{query} -> {r.display_name}, {r.state_usps}"
    assert expect_fragment in (r.display_name or "").lower()


def test_carson_city_display_name_keeps_its_own_city_word():
    """The legal-type stripper ran case-insensitively and reduced Nevada's
    capital "Carson City" to "Carson". Census writes the strippable type in
    lowercase ("Atlanta city"); a capitalised "City" is part of the name."""
    r = pl.resolve_location("Carson City, NV")
    assert r.display_name == "Carson City"


def test_no_note_leaks_internal_vocabulary_to_clients():
    """Every `note` is rendered verbatim in the client-facing wizard, so none
    may carry repo paths, module names, or developer jargon. A design review
    caught 'see data/geo/README.md' on screen in front of a paying client."""
    probes = [
        "30301", "00000", "99999", "London, UK", "Toronto, Canada", "Bangalore",
        "asdfghjkl", "Springfield", "Atlanta", "Cincinatti", "TX", "Remote", "",
    ]
    banned = ("readme", "data/geo", ".py", ".tsv", "zcta", "tabulation",
              "resolver", "out of scope", "not a failure", "--", "traceback")
    for probe in probes:
        note = (pl.resolve_location(probe).note or "").lower()
        for term in banned:
            assert term not in note, f"{probe!r} note leaked {term!r}: {note!r}"


# ---------------------------------------------------------------------------
# Rule 2/3: "City, ST" / "City, State Name" / "City, US(A)" / comma-less "City ST"
# ---------------------------------------------------------------------------
def test_city_comma_state_abbr():
    r = pl.resolve_location("Denver, CO")
    assert r.status == "resolved"
    assert r.kind == "city"
    assert r.display_name == "Denver"
    assert r.state_usps == "CO"


def test_city_comma_full_state_name():
    r = pl.resolve_location("Denver, Colorado")
    assert r.status == "resolved"
    assert r.state_usps == "CO"


def test_city_comma_less_state_abbr():
    r = pl.resolve_location("Denver CO")
    assert r.status == "resolved"
    assert r.state_usps == "CO"


def test_city_us_suffix_falls_back_to_unspecified_state_city():
    # No state specified -> rule 5 (bare-city) fires on "Denver" alone.
    # Denver is a real place name in 6 states (CO/IA/IN/MO/NC/PA), so this
    # is an ambiguous bare-city resolution, not a crash or a mis-parse of
    # the literal "Denver, US" string -- the curated tiebreak picks CO
    # (the well-known metro) as primary.
    for text in ("Denver, US", "Denver, USA", "Denver, United States"):
        r = pl.resolve_location(text)
        assert r.status == "ambiguous", f"{text!r} -> {r.status}"
        assert r.state_usps == "CO", f"{text!r} -> primary state {r.state_usps}"
        states = {a["state_usps"] for a in r.alternatives}
        assert "CO" in states and len(states) > 1

    # A genuinely US-unique city with the same suffix resolves cleanly.
    r = pl.resolve_location("Albuquerque, US")
    assert r.status == "resolved"
    assert r.state_usps == "NM"


def test_county_state_pattern():
    r = pl.resolve_location("Fulton County, GA")
    assert r.status == "resolved"
    assert r.kind == "county"
    assert r.county_name == "Fulton County"
    assert r.state_usps == "GA"
    assert r.county_fips


def test_city_with_recognized_state_but_no_match_is_honest_unresolved():
    r = pl.resolve_location("Zzzznotarealcity, TX")
    assert r.status == "unresolved"
    assert r.state_usps == "TX"


# ---------------------------------------------------------------------------
# Rule 4: bare state name or USPS abbreviation
# ---------------------------------------------------------------------------
def test_bare_state_abbr():
    r = pl.resolve_location("TX")
    assert r.status == "resolved"
    assert r.kind == "state"
    assert r.state_usps == "TX"
    assert r.state_name == "Texas"


def test_bare_state_full_name():
    r = pl.resolve_location("Texas")
    assert r.status == "resolved"
    assert r.kind == "state"
    assert r.state_usps == "TX"


def test_bare_state_territory():
    r = pl.resolve_location("Puerto Rico")
    assert r.status == "resolved"
    assert r.kind == "state"
    assert r.state_usps == "PR"


# ---------------------------------------------------------------------------
# Rule 5: bare city name, no state -- unique vs. ambiguous
# ---------------------------------------------------------------------------
def test_bare_city_unique():
    r = pl.resolve_location("Albuquerque")
    assert r.status == "resolved"
    assert r.state_usps == "NM"


def test_bare_city_ambiguous_springfield():
    r = pl.resolve_location("Springfield")
    assert r.status == "ambiguous"
    assert r.kind == "city"
    assert len(r.alternatives) > 1
    states = {a["state_usps"] for a in r.alternatives}
    assert len(states) > 1
    # Primary is one of the listed alternatives.
    assert r.state_usps in states
    assert r.note


def test_bare_city_ambiguous_portland():
    r = pl.resolve_location("Portland")
    assert r.status == "ambiguous"
    states = {a["state_usps"] for a in r.alternatives}
    assert "OR" in states
    assert "ME" in states
    # Curated well-known-metro tiebreak picks Portland, OR as primary.
    assert r.state_usps == "OR"


def test_ambiguity_tiebreak_is_deterministic():
    r1 = pl.resolve_location("Springfield")
    r2 = pl.resolve_location("Springfield")
    assert r1.state_usps == r2.state_usps
    assert r1.alternatives == r2.alternatives


# ---------------------------------------------------------------------------
# Rule 6/7: country / nationwide / remote keywords
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("text", ["United States", "US", "USA", "America"])
def test_country_keywords(text):
    r = pl.resolve_location(text)
    assert r.status == "resolved"
    assert r.kind == "country"


@pytest.mark.parametrize("text", ["Nationwide", "National", "All US"])
def test_nationwide_keywords(text):
    r = pl.resolve_location(text)
    assert r.status == "resolved"
    assert r.kind == "nationwide"


@pytest.mark.parametrize("text", ["Remote", "Work From Home", "WFH", "Anywhere", "remote"])
def test_remote_keywords_resolve_not_fail(text):
    r = pl.resolve_location(text)
    assert r.status == "resolved"
    assert r.kind == "remote"


# ---------------------------------------------------------------------------
# Rule 8: non-US -- must not crash, must not read as an error
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    ["London, UK", "Toronto, Canada", "Bangalore", "Sydney, Australia", "Dublin, Ireland"],
)
def test_non_us_is_unresolved_not_error(text):
    r = pl.resolve_location(text)
    assert r.status == "unresolved"
    assert r.kind == "unknown"
    # Should read as out-of-scope, not a failure -- must not say "error" in
    # the sense of something having gone wrong (an explicit reassurance
    # like "not a failure" is fine and expected).
    assert "went wrong" not in r.note.lower()
    assert "scope" in r.note.lower() or "us" in r.note.lower()


# ---------------------------------------------------------------------------
# Rule 9: fuzzy correction -- full legacy _LOCATION_CORRECTIONS parity
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("wrong,right", sorted(_LEGACY_LOCATION_CORRECTIONS.items()))
def test_legacy_misspelling_parity(wrong, right):
    r = pl.resolve_location(wrong)
    if wrong in _EXACT_MATCH_NOT_FUZZY:
        # These are real place names in their own right -- exact bare-city
        # match (rule 5) fires before fuzzy ever runs. "pittsburg" is
        # genuinely ambiguous across 5 states; "albuquerque" is unique.
        assert r.status in ("resolved", "ambiguous")
        assert r.display_name.lower() == wrong.lower()
    else:
        assert r.status == "corrected", f"{wrong!r} -> expected corrected, got {r.status}"
        assert r.display_name.lower() == right.lower(), f"{wrong!r} corrected to {r.display_name!r}, expected {right!r}"
        assert wrong.lower() in r.note.lower() or right.lower() in r.note.lower()


def test_fuzzy_cutoff_does_not_false_positive_on_garbage():
    for garbage in ["asdfghjkl", "xyzzyx123", "qqqqqqqqq", "zzzzz99999"]:
        r = pl.resolve_location(garbage)
        assert r.status == "unresolved", f"{garbage!r} unexpectedly matched: {r.to_dict()}"


# ---------------------------------------------------------------------------
# Rule 10: nothing matched
# ---------------------------------------------------------------------------
def test_completely_unresolvable():
    r = pl.resolve_location("qqzxjklw999notreal")
    assert r.status == "unresolved"
    assert r.kind == "unknown"


# ---------------------------------------------------------------------------
# Garbage / hostile inputs never raise
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad_input",
    ["asdfghjkl", "", None, 12345, "x" * 500, "   ", ",,,,", "!!!@@@###", 3.14, [], {}],
)
def test_garbage_never_raises(bad_input):
    r = pl.resolve_location(bad_input)  # type: ignore[arg-type]
    assert isinstance(r, pl.LocationResolution)
    assert r.status in ("resolved", "corrected", "ambiguous", "unresolved")


def test_resolve_locations_batch_never_raises_on_mixed_garbage():
    batch = ["30303", None, "", "asdfghjkl", "Atlanta", 12345, "London, UK"]
    results = pl.resolve_locations(batch)  # type: ignore[arg-type]
    assert len(results) == len(batch)
    assert all(isinstance(r, pl.LocationResolution) for r in results)


# ---------------------------------------------------------------------------
# DMA -- pluggable, off by default
# ---------------------------------------------------------------------------
def test_dma_absent_by_default():
    assert not (pl._GEO_DIR / "dma_by_county.tsv").exists(), (
        "dma_by_county.tsv exists -- this test suite assumes the DMA-absent "
        "path; if the file was added, this assertion (and the licensing "
        "note in plan_location.py) needs revisiting."
    )
    r = pl.resolve_location("30303")
    assert r.status == "resolved"
    assert r.dma_code is None
    assert r.dma_name is None
    assert r.dma_status == "unavailable"


def test_dma_never_labeled_as_nielsen_anywhere():
    import inspect

    source = inspect.getsource(pl)
    assert "nielsen" not in source.lower()


# ---------------------------------------------------------------------------
# CBSA -- pluggable like DMA, but ships with the repo (public-domain data,
# no licensing question) so it is "available" by default whenever the
# resolution carries a county_fips that sits inside a real CBSA.
# ---------------------------------------------------------------------------

CBSA_CITY_PINS = [
    # (query, expected_state, expected substring in cbsa_title)
    ("Atlanta, GA", "GA", "Atlanta"),
    ("Houston, TX", "TX", "Houston"),
    ("Chicago, IL", "IL", "Chicago"),
    ("Los Angeles, CA", "CA", "Los Angeles"),
    ("Phoenix, AZ", "AZ", "Phoenix"),
    ("Seattle, WA", "WA", "Seattle"),
    ("Denver, CO", "CO", "Denver"),
    ("Boston, MA", "MA", "Boston"),
    ("Miami, FL", "FL", "Miami"),
    ("Dallas, TX", "TX", "Dallas"),
]


@pytest.mark.parametrize("query,expected_state,expected_title_fragment", CBSA_CITY_PINS)
def test_major_metros_resolve_a_real_cbsa(query, expected_state, expected_title_fragment):
    r = pl.resolve_location(query)
    assert r.status == "resolved"
    assert r.state_usps == expected_state
    assert r.county_fips
    assert r.cbsa_status == "available", f"{query} -> cbsa_status={r.cbsa_status}"
    assert r.cbsa_code and r.cbsa_code.isdigit() and len(r.cbsa_code) == 5
    assert expected_title_fragment in (r.cbsa_title or "")


def test_cbsa_unavailable_when_no_county_fips():
    """Bare state / remote / nationwide resolutions carry no county_fips, so
    cbsa_code/cbsa_title must stay None -- never a fabricated CBSA."""
    for query in ("TX", "Remote", "Nationwide"):
        r = pl.resolve_location(query)
        assert not r.county_fips
        assert r.cbsa_code is None
        assert r.cbsa_title is None
        assert r.cbsa_status == "unavailable"


def test_cbsa_unavailable_for_a_county_genuinely_outside_any_cbsa():
    """Bullock County, AL (FIPS 01011) has a county_fips but sits outside
    any CBSA in the real July 2023 OMB delineation -- this must read as an
    honest "unavailable", not an error, and never a fabricated CBSA."""
    r = pl.resolve_location("Bullock County, AL")
    assert r.status == "resolved"
    assert r.county_fips == "01011"
    assert r.cbsa_code is None
    assert r.cbsa_title is None
    assert r.cbsa_status == "unavailable"


def test_cbsa_never_labeled_as_nielsen_anywhere():
    import inspect

    source = inspect.getsource(pl)
    assert "nielsen" not in source.lower()


# ---------------------------------------------------------------------------
# to_dict()
# ---------------------------------------------------------------------------
def test_to_dict_shape():
    r = pl.resolve_location("30303")
    d = r.to_dict()
    for key in (
        "input",
        "status",
        "kind",
        "display_name",
        "city",
        "county_name",
        "county_fips",
        "state_usps",
        "state_name",
        "zip5",
        "lat",
        "lng",
        "confidence",
        "matched_via",
        "note",
    ):
        assert key in d


def test_input_field_is_verbatim():
    r = pl.resolve_location("  30303  ")
    assert r.input == "  30303  "


# ---------------------------------------------------------------------------
# Performance: 1,000 warm resolutions well under a second
# ---------------------------------------------------------------------------
def test_performance_1000_resolutions_warm():
    samples = [
        "30303",
        "94103",
        "Atlanta",
        "Springfield, IL",
        "Denver, CO",
        "Remote",
        "TX",
        "Nationwide",
        "asdfghjkl",
        "Cincinatti",
        "London, UK",
        "02134",
        "30301",
        "Portland, ME",
        "Chicago",
        "Fulton County, GA",
    ]
    # Warm the lazy-load first; the perf budget is for warm calls only.
    pl.resolve_location("30303")

    t0 = time.perf_counter()
    for i in range(1000):
        pl.resolve_location(samples[i % len(samples)])
    elapsed = time.perf_counter() - t0
    assert elapsed < 1.0, f"1000 warm resolutions took {elapsed:.3f}s, expected well under 1s"


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------
def test_thread_safety_concurrent_resolution():
    inputs = ["30303", "Atlanta", "Cincinatti", "Remote", "TX", "London, UK", "Springfield", "30301"]

    def worker(_i: int) -> list[dict]:
        return [pl.resolve_location(x).to_dict() for x in inputs]

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(worker, range(8)))

    baseline = results[0]
    for r in results[1:]:
        assert r == baseline


def test_lazy_load_happens_once():
    # Loading is idempotent: forcing _ensure_loaded repeatedly must not
    # rebuild the indexes (identity check on a populated dict).
    pl._ensure_loaded()
    places_id_before = id(pl._places_by_key)
    size_before = len(pl._places_by_key)
    pl._ensure_loaded()
    assert id(pl._places_by_key) == places_id_before
    assert len(pl._places_by_key) == size_before
    assert size_before > 0
