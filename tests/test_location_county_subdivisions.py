"""Townships, towns and plantations resolve in every state, not just New Jersey.

Background (2026-10-16): ``resolve_location("Edison, NJ")`` returned Madison
because a New Jersey township is a Census *county subdivision*, not a *place*,
and ``us_places.tsv`` only held places. The NJ pass added the 211 NJ townships
with a unique name. The same gap exists wherever the named municipality is a
subdivision -- PA, MI, OH, IN, IL, MN, MO, KS, NE, ND, SD (townships), NY, WI
and the six New England states (towns), Maine plantations -- so "Lower Merion
Township, PA", "Canton, MI", "West Chester, OH", "Glastonbury, CT" were all
unresolvable, and a typo-level fuzzy match was the only thing standing between
them and a wrong town.

``scripts/build_geo_county_subdivisions.py`` now adds 10,547 rows (the NJ rules,
every state). What is pinned here:

  * the new places resolve, as "Name, ST" and as "Name Township/Twp", with the
    right county; MI charter townships answer to "Charter Township" too;
  * every one of the 10,547 rows resolves back to itself (data and resolver
    agree), and so does its "<Name> Township" form;
  * a township whose plain name is another place in its state is stored as
    "<Name> Township" and never displaces that place; a town in that position
    is skipped; names several subdivisions of a state share are NOT rows and
    are never "corrected" into a look-alike;
  * the typo rule still holds for the new rows (a misspelling reads as the SAME
    place, a real place is never swapped for another);
  * a township elsewhere never changes a bare-name answer that already existed:
    "Chicago" is still Chicago, IL (not "pick one: Chicago, ND too").
"""

from __future__ import annotations

import csv
import functools
import re
import unicodedata

import pytest

import plan_location as pl

SUB = "census_subdivision"
STATES_WITH_SUBDIVISIONS = {
    "CT": 100, "IL": 1000, "IN": 250, "KS": 500, "MA": 150, "ME": 250, "MI": 700, "MN": 1300, "MO": 100, "ND": 1000,
    "NE": 150, "NH": 100, "NJ": 211, "NY": 400, "OH": 350, "PA": 600, "RI": 20, "SD": 450, "VT": 100, "WI": 400,
}  # fmt: skip  (state -> floor for the number of subdivision rows; the 2026-10-16 build is 10-30% above each)


@functools.lru_cache(maxsize=1)
def _places() -> tuple[dict[str, str], ...]:
    with (pl._GEO_DIR / "us_places.tsv").open(encoding="utf-8", newline="") as f:
        return tuple(csv.DictReader(f, delimiter="\t"))


def _subdivisions() -> list[dict[str, str]]:
    rows = [r for r in _places() if r["county_confidence"] == SUB]
    # Never vacuous: a file with no subdivision rows must fail every test built on them.
    assert len(rows) >= 10000, f"only {len(rows)} county-subdivision rows in us_places.tsv"
    return rows


@functools.lru_cache(maxsize=1)
def _ambiguous_rows() -> tuple[dict[str, str], ...]:
    with (pl._GEO_DIR / "us_subdivision_ambiguous.tsv").open(encoding="utf-8", newline="") as f:
        return tuple(csv.DictReader(f, delimiter="\t"))


def _norm(s: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s.lower()).split())


# ---------------------------------------------------------------------------
# The places now resolve
# ---------------------------------------------------------------------------
NEW_PLACES = [
    # Pennsylvania townships (the named municipality, absent from the place file)
    ("Lower Merion Township, PA", "Lower Merion", "PA", "42091"),
    ("Lower Merion Twp, PA", "Lower Merion", "PA", "42091"),
    ("Lower Merion, PA", "Lower Merion", "PA", "42091"),
    ("Bensalem, PA", "Bensalem", "PA", "42017"),
    ("Bensalem Township, PA", "Bensalem", "PA", "42017"),
    ("Bensalem Twp., PA", "Bensalem", "PA", "42017"),
    ("Upper Darby Township, PA", "Upper Darby", "PA", "42045"),
    # Michigan: a charter township is a township
    ("Canton, MI", "Canton", "MI", "26163"),
    ("Canton Township, MI", "Canton", "MI", "26163"),
    ("Canton Charter Township, MI", "Canton", "MI", "26163"),
    ("Waterford Township, MI", "Waterford", "MI", "26125"),
    # Ohio
    ("West Chester, OH", "West Chester", "OH", "39017"),
    ("West Chester Township, OH", "West Chester", "OH", "39017"),
    ("Anderson Township, OH", "Anderson", "OH", "39061"),
    # Minnesota: a plain-named township, and one whose name is also a city (full legal name)
    ("Castle Rock Township, MN", "Castle Rock", "MN", "27037"),
    ("Castle Rock, MN", "Castle Rock", "MN", "27037"),
    ("Oronoco Township, MN", "Oronoco Township", "MN", "27109"),
    # Illinois: Schaumburg village is a place; the township is a different municipality
    ("Schaumburg Township, IL", "Schaumburg Township", "IL", "17031"),
    ("Wheeling Township, IL", "Wheeling Township", "IL", "17031"),
    # Wisconsin and New York towns
    ("Grand Chute, WI", "Grand Chute", "WI", "55087"),
    ("Ramapo, NY", "Ramapo", "NY", "36087"),
    ("Clarkstown, NY", "Clarkstown", "NY", "36087"),
    ("Amherst, NY", "Amherst", "NY", "36029"),
    # New England towns
    ("Glastonbury, CT", "Glastonbury", "CT", "09110"),
    ("Wethersfield, CT", "Wethersfield", "CT", "09110"),
    ("Natick, MA", "Natick", "MA", "25017"),
    ("Weston, MA", "Weston", "MA", "25017"),
    ("Scarborough, ME", "Scarborough", "ME", "23005"),
    # a Maine plantation
    ("Grand Lake Stream, ME", "Grand Lake Stream", "ME", "23029"),
]  # fmt: skip


@pytest.mark.parametrize("text,display,state,county", NEW_PLACES)
def test_new_county_subdivision_resolves_to_itself(text, display, state, county):
    r = pl.resolve_location(text)
    assert r.status == "resolved", r.to_dict()
    assert (r.display_name, r.state_usps, r.county_fips) == (display, state, county), r.to_dict()
    assert r.kind == "city"
    assert r.lat is not None and r.lng is not None


def test_a_charter_township_answers_to_charter_township_too():
    plain = pl.resolve_location("Canton Township, MI")
    charter = pl.resolve_location("Canton Charter Township, MI")
    assert (charter.status, charter.display_name, charter.county_fips) == (plain.status, plain.display_name, plain.county_fips)
    assert pl._strip_township("canton charter township") == "canton"
    assert pl._strip_township("canton twp") == "canton"
    assert pl._strip_township("canton") == ""


def test_the_cbsa_layer_is_attached_to_a_subdivision_in_a_metro():
    """A township resolves to its county, and the county's CBSA comes with it."""
    r = pl.resolve_location("Lower Merion Township, PA")
    assert r.county_name and r.cbsa_status == "available"
    assert "Philadelphia" in (r.cbsa_title or "")


# ---------------------------------------------------------------------------
# Data and resolver agree, row by row
# ---------------------------------------------------------------------------
def test_every_subdivision_row_resolves_back_to_itself():
    """10,547 probes: "<display>, <ST>" returns that very row (state, county,
    display), so no row is shadowed by another place, unreachable through a
    punctuation quirk, or fuzzily redirected."""
    pl._ensure_loaded()
    bad = []
    for row in _subdivisions():
        r = pl.resolve_location(f"{row['display_name']}, {row['state_usps']}")
        if not (
            r.status == "resolved"
            and r.display_name == row["display_name"]
            and r.state_usps == row["state_usps"]
            and r.county_fips == row["county_fips"]
        ):
            bad.append((row["place_key"], r.status, r.display_name, r.county_fips))
    assert not bad, f"{len(bad)} subdivision rows do not resolve to themselves, e.g. {bad[:6]}"


def test_every_township_resolves_as_name_township_too():
    pl._ensure_loaded()
    bad = []
    for row in _subdivisions():
        if row["place_type"] != "township":
            continue
        base = re.sub(r" Township$", "", row["display_name"])
        r = pl.resolve_location(f"{base} Township, {row['state_usps']}")
        if not (r.status in ("resolved", "corrected") and r.county_fips == row["county_fips"] and r.state_usps == row["state_usps"]):
            bad.append((base, row["state_usps"], r.status, r.display_name, r.county_fips))
    assert not bad, f"{len(bad)} townships do not answer to '<Name> Township', e.g. {bad[:6]}"


# ---------------------------------------------------------------------------
# Row policy (the New Jersey rules, every state)
# ---------------------------------------------------------------------------
def test_subdivision_rows_are_well_formed_and_displace_nothing():
    rows = _places()
    sub = _subdivisions()
    with (pl._GEO_DIR / "us_counties.tsv").open(encoding="utf-8", newline="") as f:
        county_state = {r["county_fips"]: r["state_usps"] for r in csv.DictReader(f, delimiter="\t")}
    others = {r["place_key"] for r in rows if r["county_confidence"] != SUB}
    keys = [r["place_key"] for r in sub]
    assert len(keys) == len(set(keys)), "duplicate subdivision place_key"
    assert not (set(keys) & others), "a subdivision row shares its key with another place"
    for r in sub:
        assert r["place_key"] == f"{_norm(r['display_name'])}|{r['state_usps'].lower()}", r
        assert county_state.get(r["county_fips"]) == r["state_usps"], r
        assert r["place_type"] in {"township", "town", "plantation"}, r
        assert 17.0 < float(r["lat"]) < 72.0 and -180.0 < float(r["lng"]) < -66.0, r
    # a key held by a base row means the township took its full legal name
    for r in sub:
        if r["display_name"].endswith(" Township"):
            assert r["place_type"] == "township", r
            assert f"{_norm(re.sub(r' Township$', '', r['display_name']))}|{r['state_usps'].lower()}" in others, (
                "stored under its full legal name but the plain name is free", r)


@pytest.mark.parametrize("state,floor", sorted(STATES_WITH_SUBDIVISIONS.items()))
def test_every_subdivision_state_is_covered(state, floor):
    n = sum(1 for r in _subdivisions() if r["state_usps"] == state)
    assert n >= floor, (state, n)


def test_only_the_expected_states_carry_subdivision_rows():
    assert {r["state_usps"] for r in _subdivisions()} == set(STATES_WITH_SUBDIVISIONS)


def test_new_jersey_rows_are_unchanged_by_the_generalisation():
    nj = [r for r in _subdivisions() if r["state_usps"] == "NJ"]
    assert len(nj) == 211
    assert {r["place_type"] for r in nj} == {"township"}
    by_key = {r["place_key"]: r for r in nj}
    assert by_key["edison|nj"]["county_fips"] == "34023"
    assert by_key["hampton township|nj"]["county_fips"] == "34037"


def test_a_township_sharing_a_places_name_is_stored_as_name_township_and_the_place_is_untouched():
    by_key = {r["place_key"]: r for r in _places()}
    assert by_key["schaumburg|il"]["place_type"] == "village" and by_key["schaumburg|il"]["county_confidence"] != SUB
    assert by_key["schaumburg township|il"]["place_type"] == "township"
    assert by_key["plymouth|mi"]["place_type"] == "city"
    assert by_key["plymouth township|mi"]["county_confidence"] == SUB
    assert pl.resolve_location("Schaumburg, IL").display_name == "Schaumburg"
    assert pl.resolve_location("Plymouth, MI").display_name == "Plymouth"
    assert pl.resolve_location("Plymouth Township, MI").display_name == "Plymouth Township"


def test_a_town_sharing_a_places_name_is_skipped_not_duplicated():
    by_key = {r["place_key"]: r for r in _places()}
    assert by_key["hempstead|ny"]["place_type"] == "village"
    assert "hempstead town|ny" not in by_key
    assert by_key["glastonbury|ct"]["place_type"] == "town"  # towns ARE added when their name is free
    assert pl.resolve_location("Hempstead, NY").county_fips == "36059"


def test_names_a_state_shares_between_subdivisions_are_not_rows():
    keys = {r["place_key"] for r in _subdivisions()}
    assert _ambiguous_rows(), "the shared names are recorded"
    for key in ("washington township|pa", "clinton township|mi", "liberty township|oh", "conewago|pa", "thornbury|pa"):
        assert key not in keys, key
    for text in ("Washington Township, PA", "Clinton Township, MI", "Liberty Township, OH", "Springfield Township, PA"):
        r = pl.resolve_location(text)
        assert r.status == "unresolved" and not r.display_name, (text, r.to_dict())
        assert r.state_usps == text.rsplit(", ", 1)[1]


# ---------------------------------------------------------------------------
# A shared township name is real: never "corrected" into a look-alike
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "Conewago Township, PA",  # 3 PA counties; look-alike Conewango Township
        "Conewago, PA",
        "Thornbury Township, PA",  # Chester + Delaware; look-alike Thornburg borough
        "Thornbury, PA",
        "Huston Township, PA",  # Blair + Clearfield; look-alike Houston borough
        "Huston, PA",
        "Stoney Creek Township, IN",  # look-alike Stony Creek
        "Cloverland, WI",  # look-alike Cleveland
    ],
)  # fmt: skip
def test_a_shared_township_name_is_not_swapped_for_a_look_alike(text):
    r = pl.resolve_location(text)
    assert r.status == "unresolved", r.to_dict()
    assert r.display_name == "" and r.county_fips == ""
    assert r.state_usps == text.rsplit(", ", 1)[1]


def test_every_shared_name_is_unresolved_or_resolves_to_a_place_of_that_same_name():
    """Over all 1,314 shared names, typed plain and as "<Name> Township": the
    answer is honest unresolved, or a place that really has that name -- never
    a different town."""
    pl._ensure_loaded()
    bad = []
    for row in _ambiguous_rows():
        name_norm = pl._norm_key(row["display_name"])
        for text in (f"{row['display_name']}, {row['state_usps']}", f"{row['display_name']} Township, {row['state_usps']}"):
            r = pl.resolve_location(text)
            if r.status != "unresolved" and pl._norm_key(re.sub(r" Township$", "", r.display_name)) != name_norm:
                bad.append((text, r.status, r.display_name))
    assert not bad, f"{len(bad)} shared names resolved to a different town, e.g. {bad[:6]}"


def test_ambiguous_names_file_is_well_formed_and_disjoint_from_the_rows():
    rows = _ambiguous_rows()
    assert 1200 <= len(rows) <= 1500
    seen = set()
    row_keys = {r["place_key"] for r in _subdivisions()}
    for r in rows:
        k = (r["state_usps"], r["name_key"])
        assert k not in seen, k
        seen.add(k)
        assert int(r["subdivision_count"]) >= 2
        assert r["county_fips_list"] and all(len(c) == 5 and c.isdigit() for c in r["county_fips_list"].split("|"))
        assert f"{r['name_key']}|{r['state_usps'].lower()}" not in row_keys, "a shared name must not also be a row"
    assert ("PA", "conewago") in seen and ("PA", "washington") in seen and ("NJ", "hamilton") in seen


# ---------------------------------------------------------------------------
# The typo rule still holds for the new rows
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "typo,right,state",
    [
        ("Bensalm, PA", "Bensalem", "PA"),
        ("Lower Marion, PA", "Lower Merion", "PA"),
        ("Glastonbry, CT", "Glastonbury", "CT"),
        ("Wethersfeld, CT", "Wethersfield", "CT"),
        ("Natik, MA", "Natick", "MA"),
        ("Ramapoo, NY", "Ramapo", "NY"),
        ("Clarkstwn, NY", "Clarkstown", "NY"),
        ("Grand Chutte, WI", "Grand Chute", "WI"),
        ("West Chestr, OH", "West Chester", "OH"),
        ("Wateford Township, MI", "Waterford", "MI"),
    ],
)  # fmt: skip
def test_a_typo_of_a_new_place_corrects_to_the_same_place(typo, right, state):
    r = pl.resolve_location(typo)
    assert r.status == "corrected", r.to_dict()
    assert (r.display_name, r.state_usps) == (right, state), r.to_dict()


def test_a_county_subdivision_never_ties_with_a_real_place():
    """"Boton" is one edit from Boston (a city) and from Bolton (a town in the
    subdivision rows): that is not a toss-up. Before the town rows existed this
    was a plain correction to Boston."""
    r = pl.resolve_location("Boton, MA")
    assert (r.status, r.display_name, r.state_usps) == ("corrected", "Boston", "MA"), r.to_dict()
    assert pl._has_real_place("boston", "MA") and not pl._has_real_place("bolton", "MA")


def test_two_equally_close_subdivisions_still_tie_to_nothing(monkeypatch):
    """Findley and Findlay are both PA townships (neither a real place proper),
    equally close to "Findloy": we cannot tell which was meant, so none is taken."""
    pl._ensure_loaded()
    assert pl._typo_distance("findloy", "findley") == pl._typo_distance("findloy", "findlay") == 1
    assert not pl._has_real_place("findley", "PA") and not pl._has_real_place("findlay", "PA")
    monkeypatch.setitem(pl._city_norms_by_state, "PA", ["findley", "findlay"])
    assert pl._fuzzy_city_lookup("findloy", state_usps="PA") == []


def test_a_bare_misspelling_is_never_read_as_an_obscure_township():
    """The state-less typo pool holds names a real place carries. "Bensalm" with
    no state could be any of 50 states' townships; with PA typed it is Bensalem."""
    pl._ensure_loaded()
    assert pl._fuzzy_city_lookup("bensalm") == []
    assert pl._fuzzy_city_lookup("bensalm", state_usps="PA") == ["bensalem"]
    assert pl.resolve_location("Bensalm").status == "unresolved"
    assert pl._fuzzy_city_lookup("chicagoo") == ["chicago"]  # a real place's name still corrects
    assert "chicago" in pl._city_norms_by_first_letter["c"]
    assert "bensalem" not in pl._city_norms_by_first_letter["b"]
    assert "bensalem" in pl._city_norms_by_state["PA"]


def test_a_name_that_is_a_real_place_elsewhere_is_still_never_a_typo_in_the_new_states():
    """"Edison" exists in GA/OH/NJ; typed with PA it is a town PA's table lacks,
    not a misspelling of a PA town. Property over 1 in 7 names the state lacks,
    in five of the newly covered states."""
    pl._ensure_loaded()
    offenders = []
    for state in ("PA", "MI", "OH", "MN", "CT"):
        have = set(pl._city_norms_by_state[state])
        for city_norm, entries in sorted(pl._places_by_city.items())[::7]:
            if city_norm in have:
                continue
            display = pl._places_by_key[entries[0][1]]["display_name"]
            r = pl.resolve_location(f"{display}, {state}")
            if r.matched_via.startswith("fuzzy"):
                offenders.append((state, display, r.display_name))
    assert not offenders, f"{len(offenders)} real places fuzzy-corrected to a different town, e.g. {offenders[:6]}"


# ---------------------------------------------------------------------------
# A township elsewhere never changes an existing bare-name answer
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name,state",
    [("Chicago", "IL"), ("San Francisco", "CA"), ("Flint", "MI"), ("Terre Haute", "IN")],
)
def test_a_big_city_is_not_made_ambiguous_by_a_same_named_township(name, state):
    """Chicago also names a township in North Dakota, San Francisco one in
    Minnesota, Flint two (IL, ND). Each owns >= 3 ZIPs of its own; the township
    owns none."""
    assert any(r["state_usps"] != state and r["display_name"] == name and r["county_confidence"] == SUB for r in _places()), name
    r = pl.resolve_location(name)
    assert (r.status, r.display_name, r.state_usps) == ("resolved", name, state), r.to_dict()


def test_a_small_namesake_stays_honestly_ambiguous():
    """Cherry Hill, NJ (a township, 74k) and Cherry Hill, VA (a CDP) own 0 and 1
    ZIPs; the table has no population to say which is bigger, so it asks."""
    r = pl.resolve_location("Cherry Hill")
    assert r.status == "ambiguous", r.to_dict()
    assert {a["state_usps"] for a in r.alternatives} >= {"NJ", "VA"}


def test_a_name_only_a_township_carries_resolves_bare():
    for name, state in (("Bensalem", "PA"), ("Lower Merion", "PA"), ("Glastonbury", "CT")):
        r = pl.resolve_location(name)
        assert (r.status, r.display_name, r.state_usps) == ("resolved", name, state), r.to_dict()


def test_namesake_townships_join_an_already_ambiguous_list_without_heading_it():
    """Canton: places in GA/OH/MA/... were already ambiguous. The Michigan
    township is now one of the alternatives; the primary is unchanged."""
    r = pl.resolve_location("Canton")
    assert r.status == "ambiguous", r.to_dict()
    states = {a["state_usps"] for a in r.alternatives}
    assert "MI" in states and len(states) >= 8
    assert r.state_usps == "GA"
    edison = pl.resolve_location("Edison")
    assert edison.status == "ambiguous" and {"GA", "NJ", "OH"} <= {a["state_usps"] for a in edison.alternatives}


def test_a_subdivision_is_never_the_listed_primary_over_a_real_place():
    pl._ensure_loaded()
    bad = []
    for city_norm, entries in pl._places_by_city.items():
        kinds = {pl._is_subdivision(key) for _, key in entries}
        if kinds == {True, False}:
            _, key = pl._pick_ambiguous_primary(city_norm, list(entries))
            if pl._is_subdivision(key):
                bad.append(city_norm)
    assert not bad, bad[:10]


def test_zip_footprint_separates_a_city_from_a_township():
    pl._ensure_loaded()
    by_key = pl._places_by_key
    assert pl._zip_footprint(by_key["chicago|il"]) >= pl._CITY_ZIP_FLOOR
    assert pl._zip_footprint(by_key["san francisco|ca"]) >= pl._CITY_ZIP_FLOOR
    assert pl._zip_footprint(by_key["chicago|nd"]) == 0
    assert pl._zip_footprint(by_key["cherry hill|va"]) < pl._CITY_ZIP_FLOOR


# ---------------------------------------------------------------------------
# Load-time guard: the accent fold skip is a pure speed-up
# ---------------------------------------------------------------------------
def _norm_key_reference(s: str) -> str:
    """`_norm_key` as it was before the ASCII fast path."""
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return re.sub(r"\s+", " ", s).strip()


def test_norm_key_ascii_fast_path_matches_the_full_fold():
    samples = [r["display_name"] for r in _places()]
    samples += ["Mayagüez", "MAYAGUEZ", "Añasco", "Peñuelas", "St. Mary's", "O'Fallon", "Winston-Salem", "  Spaced   Out ", "Zürich", "Ça va", ""]
    for s in samples:
        assert pl._norm_key(s) == _norm_key_reference(s), s
