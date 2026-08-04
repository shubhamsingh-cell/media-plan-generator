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
    caught 'see data/geo/README.md' on screen in front of a paying client.

    Includes the multi-county disclosure ZIPs (39573, 02861, 20110) --
    added 2026-08-02 alongside plan_location.py::_resolve_zip()'s new
    county_count/other_counties note, which this probe list originally
    missed entirely (it predates that note's construction path)."""
    probes = [
        "30301", "00000", "99999", "London, UK", "Toronto, Canada", "Bangalore",
        "asdfghjkl", "Springfield", "Atlanta", "Cincinatti", "TX", "Remote", "",
        "39573", "02861", "20110",
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
# Punctuated state-abbreviation tails ("Washington, D.C." / "Washington
# D.C."). Regression, found live by an independent adversarial reviewer
# during an unrelated fix: same failure class as Boise -> Bowie, AZ (a
# confident wrong answer instead of honest ambiguity or a correct
# resolution).
#
# DC is NOT missing from the data -- data/geo/us_states.tsv has
# "DC\tDistrict of Columbia" and "Washington, DC" (no periods) already
# resolved correctly before this fix. Root cause was two punctuation gaps:
#   1. The comma-tail check only matched a *literal* 2-letter USPS code
#      ("DC") against `_state_usps_set`, or the tail's `_norm_key`'d form
#      against `_states_by_name` -- but `_states_by_name` is keyed by the
#      normalized FULL state name ("district of columbia"), not by the
#      normalized USPS code ("dc"). "D.C." matched neither, so a real,
#      recognized state tail was treated as unrecognized and fell through
#      to fuzzy matching, which "corrected" it to Washington Park, AZ.
#   2. The comma-LESS path ("Washington D.C." as two whitespace tokens)
#      rejected the last token before tail-matching even ran: the filter
#      required exactly 2 alphabetic characters, and "D.C." is 4 characters
#      and not `.isalpha()` (periods aren't letters).
# ---------------------------------------------------------------------------
def test_washington_dc_with_periods_resolves_to_the_federal_district():
    r = pl.resolve_location("Washington, D.C.")
    assert r.status == "resolved", f"expected resolved, got {r.status} ({r.display_name}, {r.state_usps})"
    assert r.kind == "city"
    assert r.state_usps == "DC"
    assert "washington" in (r.display_name or "").lower()
    assert "washington park" not in (r.display_name or "").lower()


def test_washington_dc_no_periods_still_resolves_to_the_federal_district():
    """Guard: the already-correct exact-match path ("DC", no punctuation)
    must not regress when the punctuated path above is fixed."""
    r = pl.resolve_location("Washington, DC")
    assert r.status == "resolved"
    assert r.kind == "city"
    assert r.state_usps == "DC"
    assert r.matched_via == "place_city_state"


def test_washington_dc_no_comma_with_periods_resolves_to_the_federal_district():
    """Comma-less spelling: same "D.C." punctuation gap, but hit through the
    no-comma token-boundary path (root-cause point 2 above) instead of the
    comma-split path (point 1)."""
    r = pl.resolve_location("Washington D.C.")
    assert r.status == "resolved", f"expected resolved, got {r.status} ({r.display_name}, {r.state_usps})"
    assert r.kind == "city"
    assert r.state_usps == "DC"
    assert "washington park" not in (r.display_name or "").lower()


def test_bare_washington_stays_the_state_not_dc():
    """Guard: bare "Washington" (no tail at all, rule 4) must keep resolving
    to the state of Washington -- the fix above only touches the comma /
    comma-less tail-matching path (rule 2/3), which a single bare word never
    reaches."""
    r = pl.resolve_location("Washington")
    assert r.status == "resolved"
    assert r.kind == "state"
    assert r.state_usps == "WA"


# ---------------------------------------------------------------------------
# Accent folding (`_norm_key` must fold accents to their ASCII base letter
# BEFORE stripping non-alnum characters). Regression, memory precedent:
# an accented letter like "u" + COMBINING DIAERESIS ("ü") was just an
# unrecognized symbol to the old strip step and got deleted outright, so
# "Mayagüez" normalized to "mayagez" while plain-ASCII "Mayaguez" normalized
# to "mayaguez" -- two different keys for the same place name, neither of
# which matched the other's data.
# ---------------------------------------------------------------------------
def test_dona_ana_county_nm_resolves_both_spellings():
    """"Doña Ana County, NM" (accented) already matched by accident pre-fix
    -- the load-time key (built from the accented Census county_name) and
    the query key went through the same buggy `_norm_key`, so both dropped
    "ñ" the same way. The plain-ASCII spelling a user is far more likely to
    type, "Dona Ana County, NM", did NOT match pre-fix: dropping "ñ" from
    "doña ana" gives "doa ana", which is not "dona ana". Both spellings
    must resolve to the same county."""
    for query in ("Doña Ana County, NM", "Dona Ana County, NM"):
        r = pl.resolve_location(query)
        assert r.status == "resolved", f"{query!r} -> {r.status} ({r.note})"
        assert r.kind == "county"
        assert r.county_fips == "35013"
        assert r.state_usps == "NM"


@pytest.mark.parametrize(
    "accented,ascii_equivalent",
    [
        ("Cañon City, CO", "Canon City, CO"),
        ("La Cañada Flintridge, CA", "La Canada Flintridge, CA"),
        ("Mayagüez zona urbana, PR", "Mayaguez zona urbana, PR"),
    ],
)
def test_accent_folding_makes_ascii_spelling_match_accented_dataset_key(accented, ascii_equivalent):
    """The root-cause assertion: an accented query and its plain-ASCII
    respelling must normalize to the identical `_norm_key` output, so they
    always resolve to the same record together instead of the accented
    spelling working "by accident" (matching itself) while the ASCII
    spelling silently misses or -- worse -- gets fuzzy-corrected to an
    unrelated place (pre-fix, "Mayaguez zona urbana, PR" fuzzy-matched to
    "Jayuya zona urbana, PR", a confident wrong answer)."""
    assert pl._norm_key(accented) == pl._norm_key(ascii_equivalent)
    r_accented = pl.resolve_location(accented)
    r_ascii = pl.resolve_location(ascii_equivalent)
    assert r_accented.status == "resolved", f"{accented!r} -> {r_accented.status}"
    assert r_ascii.status == "resolved", f"{ascii_equivalent!r} -> {r_ascii.status}"
    assert r_accented.matched_via == "place_city_state", accented
    assert r_ascii.display_name == r_accented.display_name
    assert r_ascii.state_usps == r_accented.state_usps


def test_bare_mayaguez_pr_resolves_to_the_zona_urbana_place():
    """Was a DISTINCT, pre-existing gap (pinned by this test's earlier
    unresolved-on-purpose form, test_bare_mayaguez_pr_is_a_separate_still_
    open_gap_not_this_fix): the bare municipio name ("Mayagüez, PR" /
    "Mayaguez, PR", no "zona urbana") did not resolve in either spelling,
    even after accent-folding. Root cause: Puerto Rico's Census place
    record is named "Mayagüez zona urbana", and unlike Boise / Honolulu /
    Nashville (`_CENSUS_NAME_ALIASES`), there was no alias routing the bare
    municipio name to it. Fixed the same way _saint_variants folds
    "Saint X" <-> "St X" -- see `_pr_municipio_variants`, a systematic
    "<name> zona urbana" suffix try, not a per-name alias table entry
    (verified against all 78 PR municipios, not just Mayagüez -- see
    test_every_pr_municipio_resolves_to_its_zona_urbana_place below)."""
    for query in ("Mayagüez, PR", "Mayaguez, PR"):
        r = pl.resolve_location(query)
        assert r.status == "resolved", f"{query!r} -> {r.status} ({r.note})"
        assert r.kind == "city"
        assert r.matched_via == "place_city_state"
        assert r.display_name == "Mayagüez zona urbana"
        assert r.state_usps == "PR"


def test_every_pr_municipio_resolves_to_its_zona_urbana_place():
    """Exhaustive version of the spot-checks above (San Juan, Ponce,
    Bayamón, plus every other PR municipio): confirms `_pr_municipio_variants`
    generalizes safely across the full, verified 1:1 mapping (every one of
    PR's 78 municipios has exactly one "<name> zona urbana" Census place
    record, county_fips-matched to that municipio) instead of only covering
    the one name named in the bug report."""
    pl._ensure_loaded()
    pr_municipios = {
        fips: info["county_name"]
        for fips, info in pl._counties_by_fips.items()
        if info["state_usps"] == "PR"
    }
    assert len(pr_municipios) == 78, f"expected 78 PR municipios, found {len(pr_municipios)}"
    for fips, county_name in pr_municipios.items():
        bare_name = county_name[: -len("Municipio")].strip() if county_name.endswith("Municipio") else county_name
        r = pl.resolve_location(f"{bare_name}, PR")
        assert r.status == "resolved", f"{bare_name!r}, PR -> {r.status} ({r.note})"
        assert r.display_name == f"{bare_name} zona urbana", f"{bare_name!r} -> {r.display_name!r}"
        assert r.state_usps == "PR"


def test_every_pr_municipio_has_exactly_one_zona_urbana_place():
    """Data-integrity gate cited by `_pr_municipio_variants`'s docstring:
    verifies the exhaustive claim made there directly against the loaded
    geo tables instead of trusting the prose. All 78 PR county rows in
    us_counties.tsv are suffixed "Municipio"; every one has exactly one
    "<base name> zona urbana" place in us_places.tsv, county_fips-matched;
    base names are a perfect 1:1 (no duplicates on either side, nothing
    missing on either side)."""
    pl._ensure_loaded()
    pr_counties = {
        fips: info["county_name"] for fips, info in pl._counties_by_fips.items() if info["state_usps"] == "PR"
    }
    assert len(pr_counties) == 78, f"expected 78 PR municipios, found {len(pr_counties)}"

    non_suffixed = {fips: name for fips, name in pr_counties.items() if not name.endswith("Municipio")}
    assert not non_suffixed, f"PR county rows missing the 'Municipio' suffix: {non_suffixed}"

    county_base_by_fips = {fips: name[: -len("Municipio")].strip() for fips, name in pr_counties.items()}

    pr_zona_urbana_places = {
        key: place
        for key, place in pl._places_by_key.items()
        if (place.get("state_usps") or "").upper() == "PR" and (place.get("display_name") or "").endswith("zona urbana")
    }
    assert len(pr_zona_urbana_places) == 78, f"expected 78 PR 'zona urbana' places, found {len(pr_zona_urbana_places)}"

    place_base_to_keys: dict[str, list[str]] = {}
    for key, place in pr_zona_urbana_places.items():
        base = (place.get("display_name") or "")[: -len("zona urbana")].strip()
        place_base_to_keys.setdefault(base, []).append(key)

    dup_bases = {b: keys for b, keys in place_base_to_keys.items() if len(keys) > 1}
    assert not dup_bases, f"duplicate PR 'zona urbana' base names: {dup_bases}"

    county_bases = set(county_base_by_fips.values())
    place_bases = set(place_base_to_keys.keys())
    assert county_bases == place_bases, (
        f"county-only base names: {county_bases - place_bases}; "
        f"place-only base names: {place_bases - county_bases}"
    )

    mismatches = []
    for fips, base in county_base_by_fips.items():
        keys = place_base_to_keys.get(base, [])
        if len(keys) != 1:
            mismatches.append((fips, base, "not exactly one zona urbana place", keys))
            continue
        matched_fips = pr_zona_urbana_places[keys[0]].get("county_fips")
        if matched_fips != fips:
            mismatches.append((fips, base, "county_fips mismatch", matched_fips))
    assert not mismatches, f"county_fips mismatches: {mismatches}"


# Real, verified collision set (see `_pr_municipio_variants`'s docstring):
# comparing all 78 PR municipio base names against every other state's bare
# place names, through this module's own accent-folding `_norm_key`, finds
# exactly 7 collisions -- not 5. An earlier draft of this fix compared raw,
# un-folded strings and missed the two accented names (Rincón, Río Grande),
# which is exactly the case `_norm_key`'s accent-folding exists to catch.
_PR_COLLISION_NAMES: dict[str, set[str]] = {
    "Carolina": {"AL", "RI", "WV"},
    "Florida": {"MO", "NY", "OH"},
    "Rincón": {"GA", "NM"},
    "Río Grande": {"NJ", "OH"},
    "Salinas": {"CA"},
    "San Juan": {"TX"},
    "San Lorenzo": {"CA", "NM"},
}


def test_pr_municipio_names_that_collide_with_another_state_stay_ambiguous_not_silently_pr():
    """Seven PR municipio base names are ALSO real bare place names
    elsewhere (`_PR_COLLISION_NAMES`). `_pr_municipio_variants` adds PR as
    one more honest ambiguous alternative for a bare (no-state) query -- it
    never silently prefers PR, and it never drops the pre-existing non-PR
    state(s) from `alternatives`.

    This is a real, verified BEHAVIOR CHANGE for 6 of the 7 names, not a
    no-op some earlier test coverage implied by only exercising Carolina:
    pre-fix, none of these bare queries had PR in the picture at all.
    Salinas is the headline case -- it flips from status="resolved" (the
    only "Salinas" the old data knew, CA, uniquely) to status="ambiguous"
    with CA and PR both listed; asserted explicitly below, not just folded
    into the loop.

    "Florida" is the one name of the 7 that does NOT go ambiguous: it's
    also a US state name, and the bare-state rule (rule 4) runs before the
    bare-city rule (rule 5) `_pr_municipio_variants` feeds, so bare
    "Florida" keeps resolving as the state. That's a pre-existing rule-
    ordering fact this fix doesn't change -- the Florida/PR municipio
    collision is only reachable via "Florida, PR" (covered by
    test_every_pr_municipio_resolves_to_its_zona_urbana_place)."""
    for name, expected_other_states in _PR_COLLISION_NAMES.items():
        r = pl.resolve_location(name)
        if name == "Florida":
            assert r.status == "resolved" and r.kind == "state", (
                f"{name!r} -> status={r.status} kind={r.kind}; the bare-state rule should "
                "still shadow the bare-city PR collision for this one name"
            )
            continue
        assert r.status == "ambiguous", f"{name!r} -> {r.status} ({r.note})"
        states = {a["state_usps"] for a in r.alternatives}
        assert "PR" in states, f"{name!r} alternatives missing PR: {states}"
        assert expected_other_states <= states, (
            f"{name!r} alternatives dropped a pre-existing state: expected "
            f"{expected_other_states} <= {states}"
        )

    # Salinas explicitly: status flips from unique-resolved to
    # ambiguous-with-CA-and-PR-both-listed. Primary stays CA (see
    # _WELL_KNOWN_METRO_STATE) -- PR is a listed alternative, never silently
    # preferred.
    r = pl.resolve_location("Salinas")
    assert r.status == "ambiguous"
    assert r.state_usps == "CA"
    assert {a["state_usps"] for a in r.alternatives} == {"CA", "PR"}

    # San Juan explicitly: this is the name whose primary would silently
    # flip TX -> PR under the plain alphabetical fallback ("PR" < "TX" as a
    # bare string) without the explicit _WELL_KNOWN_METRO_STATE entry.
    r = pl.resolve_location("San Juan")
    assert r.status == "ambiguous"
    assert r.state_usps == "TX"
    assert {a["state_usps"] for a in r.alternatives} == {"TX", "PR"}


def test_norm_key_ascii_only_input_is_byte_identical_to_pre_fix_output():
    """Hard constraint: accent-folding must be a no-op for ASCII-only input
    (NFD-decomposing an ASCII string yields the same string; there are no
    combining marks to drop), so every ASCII key already in production
    stays byte-for-byte the same. Pinned against the exact pre-fix
    `_norm_key` body (lower + strip non-alnum + collapse whitespace) for a
    representative sweep -- not just the couple of probes exercised above."""
    pre_fix_non_alnum_space = re.compile(r"[^a-z0-9\s]")
    pre_fix_split_ws = re.compile(r"\s+")

    def pre_fix_norm_key(s: str) -> str:
        s = s.lower()
        s = pre_fix_non_alnum_space.sub("", s)
        s = pre_fix_split_ws.sub(" ", s).strip()
        return s

    probes = [
        "Denver, CO", "Fulton County, GA", "O'Fallon", "St. Louis",
        "Boise City", "  multi   space  ", "Zzzznotarealcity, TX", "30303",
        "New York", "Winston-Salem", "Coeur d'Alene", "St. Paul, MN",
        "Washington, D.C.", "USA", "united states", "Nashville-Davidson",
    ]
    for probe in probes:
        assert pl._norm_key(probe) == pre_fix_norm_key(probe), probe


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
