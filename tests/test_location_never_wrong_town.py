"""A real place must never be silently resolved to a DIFFERENT real town.

Found live (2026-10-16, origin/main 4f29917): ``resolve_location("Edison, NJ")``
returned Madison, NJ at 0.8 confidence -- status "corrected", so app.py rewrote
the plan's location to Madison. Root cause: Edison (a township, i.e. a county
subdivision rather than a Census *place*) was absent from ``us_places.tsv``, and
the state-scoped fuzzy rule accepted any ``difflib`` ratio >= 0.75 -- a
similarity score, not a typo test: ratio("edison", "madison") = 0.77 because of
the shared "dison". The same defect turned West Orange into East Orange and
left Woodbridge Township, Cherry Hill, Piscataway and Brick unresolved.

Two fixes are pinned here:
  * data: the 211 New Jersey townships with a unique name (Census 2024
    county-subdivision Gazetteer; see scripts/build_geo_nj_townships.py);
  * code: a fuzzy hit is taken only for a misspelling of the SAME place (same
    state, same first letter, one differing word within a small edit budget),
    never when the typed name is itself a real place elsewhere, never on a tie;
    and a real state with an unknown city is an honest "unresolved -- state-level
    data", not a guess.
"""

from __future__ import annotations

import csv
import re

import pytest

import plan_location as pl

NJ_COUNTY = {
    "Middlesex": "34023",
    "Camden": "34007",
    "Ocean": "34029",
    "Essex": "34013",
    "Sussex": "34037",
    "Hunterdon": "34019",
    "Somerset": "34035",
}


def _hide_place(monkeypatch, city_norm: str, state: str) -> None:
    """Make ``city_norm`` in ``state`` absent from the lookup tables, as it was
    before the township data was added -- so the code path for a real town the
    table lacks stays covered even after the data gap is closed."""
    pl._ensure_loaded()
    key = f"{city_norm}|{state.lower()}"
    assert key in pl._places_by_key, f"{key} is not in the table to hide"
    monkeypatch.setattr(pl, "_places_by_key", {k: v for k, v in pl._places_by_key.items() if k != key})
    by_city = dict(pl._places_by_city)
    rest = [e for e in by_city[city_norm] if e[1] != key]
    if rest:
        by_city[city_norm] = rest
    else:
        del by_city[city_norm]
        first = {k: [c for c in v if c != city_norm] for k, v in pl._city_norms_by_first_letter.items()}
        monkeypatch.setattr(pl, "_city_norms_by_first_letter", first)
    monkeypatch.setattr(pl, "_places_by_city", by_city)
    # raising=False: this index does not exist on the pre-fix code, which scoped
    # its fuzzy pool from _places_by_city -- already patched above.
    still_in_state = any(u == state for u, _ in rest)
    by_state = {k: list(v) for k, v in getattr(pl, "_city_norms_by_state", {}).items()}
    if by_state.get(state) and not still_in_state:
        by_state[state] = [c for c in by_state[state] if c != city_norm]
    monkeypatch.setattr(pl, "_city_norms_by_state", by_state, raising=False)


# ---------------------------------------------------------------------------
# The reported defect
# ---------------------------------------------------------------------------
def test_edison_nj_is_edison_not_madison():
    r = pl.resolve_location("Edison, NJ")
    assert r.display_name == "Edison", r.to_dict()
    assert r.status == "resolved"
    assert r.state_usps == "NJ"
    assert r.county_fips == NJ_COUNTY["Middlesex"]


def test_edison_without_comma_is_edison_too():
    r = pl.resolve_location("Edison NJ")
    assert (r.status, r.display_name, r.state_usps) == ("resolved", "Edison", "NJ")


@pytest.mark.parametrize(
    "text,county",
    [
        ("Woodbridge Township, NJ", NJ_COUNTY["Middlesex"]),
        ("Woodbridge, NJ", NJ_COUNTY["Middlesex"]),
        ("Cherry Hill, NJ", NJ_COUNTY["Camden"]),
        ("Cherry Hill Township, NJ", NJ_COUNTY["Camden"]),
        ("Piscataway, NJ", NJ_COUNTY["Middlesex"]),
        ("Piscataway Township, NJ", NJ_COUNTY["Middlesex"]),
        ("Brick, NJ", NJ_COUNTY["Ocean"]),
        ("Brick Township, NJ", NJ_COUNTY["Ocean"]),
        ("Brick Twp., NJ", NJ_COUNTY["Ocean"]),
        ("West Orange, NJ", NJ_COUNTY["Essex"]),
        ("Edison Township, NJ", NJ_COUNTY["Middlesex"]),
    ],
)
def test_nj_townships_resolve_to_themselves(text, county):
    r = pl.resolve_location(text)
    assert r.status in ("resolved", "corrected"), r.to_dict()
    assert r.state_usps == "NJ"
    assert r.county_fips == county, r.to_dict()
    base = text.split(",")[0].replace(" Township", "").replace(" Twp.", "")
    assert r.display_name.startswith(base), r.to_dict()
    assert r.cbsa_status == "available"


def test_townships_resolve_regardless_of_state_spelling():
    for text in ("Piscataway, New Jersey", "Piscataway, nj", "piscataway township, N.J."):
        r = pl.resolve_location(text)
        assert (r.status, r.display_name, r.state_usps) == ("resolved", "Piscataway", "NJ"), text


# ---------------------------------------------------------------------------
# Township != same-named borough
# ---------------------------------------------------------------------------
def test_township_is_not_the_same_named_borough():
    """Hampton borough (Hunterdon) and Hampton township (Sussex) are different
    municipalities in different counties; the plain name keeps meaning the
    borough, the explicit "Township" the township."""
    borough = pl.resolve_location("Hampton, NJ")
    township = pl.resolve_location("Hampton Township, NJ")
    assert borough.county_fips == NJ_COUNTY["Hunterdon"]
    assert township.county_fips == NJ_COUNTY["Sussex"]
    assert township.display_name == "Hampton Township"


@pytest.mark.parametrize("text", ["Washington Township, NJ", "Franklin Township, NJ", "Hamilton Township, NJ", "Monroe Twp, NJ"])
def test_township_name_shared_by_several_counties_is_not_guessed(text):
    """NJ has 5 Washington townships, 4 Franklin... Picking one (or returning
    the same-named borough) would be inventing a primary."""
    r = pl.resolve_location(text)
    assert r.status == "unresolved", r.to_dict()
    assert r.state_usps == "NJ"
    assert not r.display_name


# ---------------------------------------------------------------------------
# Typos still correct -- to the SAME place
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "typo,right",
    [
        ("Edsion, NJ", "Edison"),
        ("Piscatawy, NJ", "Piscataway"),
        ("Chery Hill, NJ", "Cherry Hill"),
        ("Piscatawy Township, NJ", "Piscataway"),
        ("Hobokin, NJ", "Hoboken"),
        ("Pateson, NJ", "Paterson"),
    ],
)
def test_typo_in_the_named_state_corrects_to_the_same_place(typo, right):
    r = pl.resolve_location(typo)
    assert r.status == "corrected", r.to_dict()
    assert r.display_name == right
    assert r.state_usps == "NJ"
    assert typo.split(",")[0] in r.note


# ---------------------------------------------------------------------------
# A real town the table lacks is never swapped for a similar-looking one
# ---------------------------------------------------------------------------
def test_edison_missing_from_the_table_is_not_corrected_to_madison(monkeypatch):
    """The original data gap, forced back open: even with no Edison row the
    answer is an honest unresolved, not Madison."""
    _hide_place(monkeypatch, "edison", "NJ")
    r = pl.resolve_location("Edison, NJ")
    assert r.status == "unresolved", r.to_dict()
    assert r.display_name == ""
    assert r.state_usps == "NJ"
    assert r.matched_via == "city_state_no_match"


def test_west_orange_missing_from_the_table_is_not_east_orange(monkeypatch):
    _hide_place(monkeypatch, "west orange", "NJ")
    r = pl.resolve_location("West Orange, NJ")
    assert r.status == "unresolved", r.to_dict()
    assert r.display_name == ""


@pytest.mark.parametrize(
    "text",
    ["Zzyzx Falls, NJ", "Qwertyville, NJ", "Atlantis, NJ", "Gotham, NJ", "Edisonville, NJ", "Madisonn Heights, NJ"],
)
def test_nonexistent_town_never_returns_a_different_real_town(text):
    r = pl.resolve_location(text)
    assert r.status == "unresolved", r.to_dict()
    assert r.display_name == ""
    assert r.county_fips == ""
    assert r.state_usps == "NJ"


def test_a_name_that_is_a_real_place_elsewhere_is_never_fuzzy_corrected_in_this_state():
    """Property over the whole table: every distinct place name that exists in
    the data but NOT in New Jersey is a real town our NJ list lacks (or a wrong
    state), never a misspelling -- so "<name>, NJ" must never be "corrected" into
    some other NJ town. Pre-fix this produced hundreds of silent wrong towns."""
    pl._ensure_loaded()
    nj_names = set(pl._city_norms_by_state["NJ"]) if hasattr(pl, "_city_norms_by_state") else None
    if nj_names is None:  # pre-fix code: derive the NJ list the old way
        nj_names = {c for c, entries in pl._places_by_city.items() if any(u == "NJ" for u, _ in entries)}
    offenders = []
    # Every 10th distinct name, in sorted order (deterministic): ~2,500 probes,
    # enough that the pre-fix code fails it by the dozen, and fast either side.
    for city_norm, entries in sorted(pl._places_by_city.items())[::10]:
        if city_norm in nj_names:
            continue
        display = pl._places_by_key[entries[0][1]]["display_name"]
        r = pl.resolve_location(f"{display}, NJ")
        if r.status == "corrected":
            offenders.append((display, r.display_name))
    assert not offenders, f"{len(offenders)} real places corrected to a different NJ town, e.g. {offenders[:8]}"


# ---------------------------------------------------------------------------
# The matcher itself
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "query,cand",
    [
        ("edison", "madison"),  # similarity 0.77, two edits, different first letter
        ("west orange", "east orange"),  # similarity 0.82, a 4-letter word swapped
        ("hamilton", "hamilton square"),  # different word count
        ("newark", "newton"),
    ],
)
def test_typo_distance_rejects_look_alike_towns_that_are_not_typos(query, cand):
    assert pl._typo_distance(query, cand) is None


def test_one_letter_apart_but_a_real_town_elsewhere_is_still_not_a_typo(monkeypatch):
    """Clinton/Clifton are one substitution apart -- inside the edit budget --
    so only the "real place in another state" guard keeps Clinton, NJ (hidden
    here) from becoming Clifton, NJ."""
    assert pl._typo_distance("clinton", "clifton") == 1
    _hide_place(monkeypatch, "clinton", "NJ")
    r = pl.resolve_location("Clinton, NJ")
    assert r.status == "unresolved", r.to_dict()
    assert r.display_name == ""


@pytest.mark.parametrize(
    "query,cand,distance",
    [
        ("edsion", "edison", 1),  # swapped letters
        ("sanfrancisco", "san francisco", 1),  # spacing only
        ("cincinatti", "cincinnati", 2),  # nine letters and up may take two edits
        ("san antonoi", "san antonio", 1),  # one word differs, the rest identical
        ("richmand", "richmond", 1),
    ],
)
def test_typo_distance_accepts_same_place_misspellings(query, cand, distance):
    assert pl._typo_distance(query, cand) == distance


def test_a_tie_between_two_real_towns_with_a_state_typed_picks_neither(monkeypatch):
    pl._ensure_loaded()
    monkeypatch.setitem(pl._city_norms_by_state, "ZZ", ["richland", "richmond"])
    assert pl._fuzzy_city_lookup("richmand", state_usps="ZZ") == []


# ---------------------------------------------------------------------------
# What the user and the downstream tables are told
# ---------------------------------------------------------------------------
def test_unresolved_city_note_says_state_level_and_names_no_other_town():
    r = pl.resolve_location("Zzyzx Falls, NJ")
    assert r.status == "unresolved"
    first_sentence = r.note.split(". ")[0]
    assert "Zzyzx Falls" in first_sentence
    assert "New Jersey" in first_sentence
    assert "state-level" in r.note
    assert "exactly as entered" in r.note
    assert "Madison" not in r.note and "Edison" not in r.note


def test_unresolved_city_is_priced_at_state_level_downstream():
    """The note promises state-level estimates; pin that this is what the
    market resolver actually does with such a verdict (the workbook then
    labels it "[NJ state-level estimate]")."""
    import geo_market

    r = pl.resolve_location("Zzyzx Falls, NJ")
    mkt = geo_market.resolve_market("Zzyzx Falls, NJ", r.to_dict())
    assert mkt.level == "state"
    assert mkt.state == "NJ"
    assert mkt.metro_key == ""


# ---------------------------------------------------------------------------
# The data
# ---------------------------------------------------------------------------
def _rows():
    with (pl._GEO_DIR / "us_places.tsv").open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def test_nj_township_rows_are_well_formed_and_do_not_displace_existing_places():
    rows = _rows()
    townships = [r for r in rows if r["state_usps"] == "NJ" and r["place_type"] == "township"]
    assert 200 <= len(townships) <= 223
    # The base table already carries ~195 same-key pairs (the README's "not
    # observed" is out of date); the township rows must not add to them.
    township_keys = [r["place_key"] for r in townships]
    other_keys = {r["place_key"] for r in rows if not (r["state_usps"] == "NJ" and r["place_type"] == "township")}
    assert len(township_keys) == len(set(township_keys)), "duplicate township place_key"
    assert not (set(township_keys) & other_keys), "a township row shares its key with another place"
    with (pl._GEO_DIR / "us_counties.tsv").open(encoding="utf-8", newline="") as f:
        fips = {r["county_fips"] for r in csv.DictReader(f, delimiter="\t")}
    for r in townships:
        file_norm = " ".join(re.sub(r"[^a-z0-9]+", " ", r["display_name"].lower()).split())
        assert r["place_key"] == f"{file_norm}|nj"
        assert r["county_fips"] in fips and r["county_fips"].startswith("34")
        assert r["county_confidence"] in ("zip_majority", "census_first_listed")
        float(r["lat"]), float(r["lng"])
    # The pre-existing same-name places are untouched.
    by_key = {r["place_key"]: r for r in rows}
    assert by_key["woodbridge|nj"]["place_type"] == "CDP"
    assert by_key["hampton|nj"]["place_type"] == "borough"
    assert by_key["madison|nj"]["place_type"] == "borough"


def test_ambiguous_township_names_are_deliberately_absent():
    keys = {r["place_key"] for r in _rows() if r["state_usps"] == "NJ"}
    for dup in ("fairfield", "hamilton", "lawrence", "mansfield", "monroe", "ocean"):
        assert f"{dup}|nj" not in keys and f"{dup} township|nj" not in keys, dup


# ---------------------------------------------------------------------------
# Bare ambiguous names: answered with one town only for a clear largest
# namesake (plan_location's DOMINANCE RULE); otherwise ambiguous, never rewritten
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("name", ["Portland", "Columbus", "Birmingham", "Springfield", "Madison", "Clinton"])
def test_exact_bare_ambiguous_name_is_never_resolved(name):
    """The curated namesake only heads the list; the verdict stays "ambiguous"
    (so app.py leaves the entry as typed). The workbook's "Assumed Portland,
    OR" is geo_market's labelled assumption, not a resolution made here."""
    r = pl.resolve_location(name)
    assert r.status == "ambiguous", r.to_dict()
    assert len(r.alternatives) > 1


@pytest.mark.parametrize(
    "typo,city,state",
    [("Portlnad", "Portland", "OR"), ("Colombus", "Columbus", "OH"), ("Cincinatti", "Cincinnati", "OH")],
)
def test_misspelled_bare_name_with_a_clear_largest_namesake_is_corrected_to_it(typo, city, state):
    r = pl.resolve_location(typo)
    assert (r.status, r.display_name, r.state_usps) == ("corrected", city, state), r.to_dict()
    assert len(r.alternatives) > 1, "the other states stay listed"


@pytest.mark.parametrize("typo,city", [("Sprinfield", "Springfield"), ("Madisen", "Madison"), ("Clintin", "Clinton")])
def test_misspelled_bare_name_with_no_clear_largest_namesake_is_ambiguous_not_corrected(typo, city):
    """Pre-fix these were "corrected" to the alphabetically first state
    (Springfield, AR) and app.py rewrote the plan's location to it."""
    r = pl.resolve_location(typo)
    assert r.status == "ambiguous", r.to_dict()
    assert r.matched_via == "fuzzy_city_no_dominant"
    assert r.display_name == city
    assert len(r.alternatives) > 1
    assert typo in r.note


def test_misspelled_nashville_reads_the_tennessee_metro_not_a_small_arkansas_town():
    r = pl.resolve_location("Nashvile")
    assert (r.status, r.state_usps) == ("corrected", "TN"), r.to_dict()
    assert r.display_name == pl.resolve_location("Nashville, TN").display_name


# ---------------------------------------------------------------------------
# "Remote" is a work mode, never a place
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "Remote", "remote", "Remote (US)", "Remote - US", "Remote, US", "Remote USA", "Fully Remote", "100% Remote",
        "Work From Home", "WFH", "Anywhere", "Remote/Hybrid", "Remote (Nationwide)", "Remote, United States",
        "Telecommute", "Virtual", "Remote - Anywhere in US",
    ],
)  # fmt: skip
def test_remote_in_any_wording_is_one_distinct_marker_not_a_place(text):
    r = pl.resolve_location(text)
    assert (r.status, r.kind, r.display_name, r.matched_via) == ("resolved", "remote", "Remote", "remote_keyword"), r.to_dict()
    assert not (r.state_usps or r.state_name or r.county_fips or r.city or r.zip5)
    assert r.lat is None and r.lng is None
    assert r.cbsa_status == "unavailable" and not r.alternatives


@pytest.mark.parametrize("text", ["Remote - Texas", "Austin", "Virtual Reality Way", "Home"])
def test_remote_phrase_does_not_swallow_a_place(text):
    assert pl.resolve_location(text).kind != "remote"


def test_remote_marker_is_not_counted_as_a_site_downstream():
    """A US plan with three real sites and a "Remote/Hybrid" entry must count
    three physical sites -- the workbook's per-city tables skip kind == "remote"
    via geo_market.Market.is_physical."""
    import geo_market

    data = {"locations": ["Austin, TX", "Remote/Hybrid", "Dallas, TX", "Edison, NJ"]}
    data["_location_resolution"] = [r.to_dict() for r in pl.resolve_locations(data["locations"])]
    physical = geo_market.physical_markets(data)
    assert [m.location for m in physical] == ["Austin, TX", "Dallas, TX", "Edison, NJ"]
