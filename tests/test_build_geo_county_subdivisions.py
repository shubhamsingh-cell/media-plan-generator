"""scripts/build_geo_county_subdivisions.py: the row policy, on a synthetic
Census county-subdivision file (no network).

The policy is the New Jersey pass's, applied to every state:
  * only active (FUNCSTAT A) townships / charter townships / towns / plantations;
  * a name used by 2+ of them in a state gets no row (recorded in the ambiguous
    list instead);
  * a township whose plain name is another place in its state is stored as
    "<Name> Township"; a town or plantation in that position is skipped;
  * the county is the GEOID's;
  * a second run changes nothing.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


@pytest.fixture(scope="module")
def bs():
    sys.path.insert(0, str(_SCRIPTS))
    try:
        spec = importlib.util.spec_from_file_location("build_geo_county_subdivisions", _SCRIPTS / "build_geo_county_subdivisions.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        sys.path.remove(str(_SCRIPTS))


def _cousub(bs, usps, name, geoid, funcstat="A", lat="40.000000", lng="-75.000000"):
    row = [""] * len(bs.COUSUB_HEADER)
    for col, val in (("USPS", usps), ("GEOID", geoid), ("NAME", name), ("FUNCSTAT", funcstat), ("INTPTLAT", lat), ("INTPTLONG", lng)):
        row[bs.COUSUB_HEADER.index(col)] = val
    return row


def _idx(bs):
    return {h: i for i, h in enumerate(bs.COUSUB_HEADER)}


def _base_row(key, name, usps, county="42091", ptype="city", conf="zip_majority"):
    return {
        "place_key": key,
        "display_name": name,
        "state_usps": usps,
        "county_fips": county,
        "lat": "40.0",
        "lng": "-75.0",
        "place_type": ptype,
        "county_confidence": conf,
    }


FIPS = {"42091", "42017", "26163", "34023", "09110", "23029", "36087"}


# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "census_name,expected",
    [
        ("Edison township", ("Edison", "township", " Township", "edison")),
        ("Canton charter township", ("Canton", "township", " Township", "canton")),
        ("St. Onge township", ("St. Onge", "township", " Township", "st onge")),
        ("Glastonbury town", ("Glastonbury", "town", "", "glastonbury")),
        ("Cary plantation", ("Cary", "plantation", "", "cary")),
        ("Hempstead village", None),
        ("Atlanta city", None),
        ("Charles Town", None),  # capitalised: part of a proper name, not the legal type
        ("Western Hills CCD", None),
        ("Washington borough", None),
        ("township", None),  # no name left
    ],
)
def test_classify_name(bs, census_name, expected):
    assert bs.classify_name(census_name) == expected


def test_only_active_subdivisions_of_the_supported_types_are_candidates(bs):
    rows = [
        _cousub(bs, "PA", "Lower Merion township", "4209143000"),
        _cousub(bs, "PA", "Ghost township", "4209100001", funcstat="N"),  # non-functioning
        _cousub(bs, "PA", "Fiction township", "4209100002", funcstat="F"),
        _cousub(bs, "PA", "Narberth borough", "4209150000"),  # a borough is a place already
        _cousub(bs, "NJ", "Edison township", "3402319000"),
    ]
    unique, ambiguous = bs.parse_subdivisions(rows, _idx(bs), FIPS)
    assert set(unique) == {("PA", "lower merion"), ("NJ", "edison")}
    assert not ambiguous
    assert unique[("PA", "lower merion")]["county_fips"] == "42091"


def test_a_name_two_subdivisions_share_in_a_state_is_not_a_candidate_but_is_recorded(bs):
    rows = [
        _cousub(bs, "PA", "Washington township", "4201779000"),
        _cousub(bs, "PA", "Washington township", "4209179001"),
        _cousub(bs, "PA", "Conewago town", "4209112345"),  # same name, different type, same state
        _cousub(bs, "PA", "Conewago township", "4201712346"),
        _cousub(bs, "NJ", "Washington township", "3402319001"),  # another state: unique there
    ]
    unique, ambiguous = bs.parse_subdivisions(rows, _idx(bs), FIPS)
    assert set(unique) == {("NJ", "washington")}
    assert set(ambiguous) == {("PA", "washington"), ("PA", "conewago")}
    out = {(r["state_usps"], r["name_key"]): r for r in bs.ambiguous_rows(ambiguous)}
    assert out[("PA", "washington")]["subdivision_count"] == "2"
    assert out[("PA", "washington")]["county_fips_list"] == "42017|42091"
    assert out[("PA", "conewago")]["display_name"] == "Conewago"


def test_a_geoid_whose_county_is_unknown_fails_loudly(bs):
    with pytest.raises(bs.bgd.BuildError):
        bs.parse_subdivisions([_cousub(bs, "PA", "Nowhere township", "9999943000")], _idx(bs), FIPS)
    with pytest.raises(bs.bgd.BuildError):
        bs.parse_subdivisions([_cousub(bs, "PA", "Short township", "42091")], _idx(bs), FIPS)


# ---------------------------------------------------------------------------
def _derive(bs, cousubs, base):
    unique, _ = bs.parse_subdivisions(cousubs, _idx(bs), FIPS)
    return bs.derive_rows(unique, base)


def test_plain_name_when_free_and_full_legal_name_when_a_township_collides(bs):
    cousubs = [
        _cousub(bs, "PA", "Bensalem township", "4201704976"),
        _cousub(bs, "IL", "Schaumburg township", "4209170000"),
    ]
    base = [_base_row("schaumburg|il", "Schaumburg", "IL", "42091", "village")]
    added, stats = _derive(bs, cousubs, base)
    by_key = {r["place_key"]: r for r in added}
    assert set(by_key) == {"bensalem|pa", "schaumburg township|il"}
    assert by_key["bensalem|pa"]["display_name"] == "Bensalem"
    assert by_key["schaumburg township|il"]["display_name"] == "Schaumburg Township"
    for r in added:
        assert r["place_type"] == "township" and r["county_confidence"] == "census_subdivision"
    assert by_key["bensalem|pa"]["county_fips"] == "42017"
    assert sum(stats["plain"].values()) == 1 and sum(stats["full_name"].values()) == 1
    assert base == [_base_row("schaumburg|il", "Schaumburg", "IL", "42091", "village")], "the base is never edited"


def test_a_town_or_plantation_that_collides_with_a_place_is_skipped(bs):
    cousubs = [
        _cousub(bs, "NY", "Hempstead town", "3608727000"),
        _cousub(bs, "ME", "Carroll plantation", "2302911111"),
        _cousub(bs, "CT", "Glastonbury town", "0911033000"),
    ]
    base = [
        _base_row("hempstead|ny", "Hempstead", "NY", "36087", "village"),
        _base_row("carroll|me", "Carroll", "ME", "23029", "CDP"),
    ]
    added, stats = _derive(bs, cousubs, base)
    assert [r["place_key"] for r in added] == ["glastonbury|ct"]
    assert added[0]["place_type"] == "town"
    assert sum(stats[bs.REASON_COLLISION].values()) == 2


def test_the_same_plain_name_in_two_states_is_two_rows(bs):
    cousubs = [_cousub(bs, "PA", "Lincoln township", "4209100003"), _cousub(bs, "MI", "Lincoln township", "2616300004")]
    added, _ = _derive(bs, cousubs, [])
    assert sorted(r["place_key"] for r in added) == ["lincoln|mi", "lincoln|pa"]


def test_a_taken_full_legal_name_is_counted_not_overwritten(bs):
    cousubs = [_cousub(bs, "PA", "Odd township", "4209100005")]
    base = [_base_row("odd|pa", "Odd", "PA"), _base_row("odd township|pa", "Odd Township", "PA")]
    added, stats = _derive(bs, cousubs, base)
    assert added == [] and sum(stats[bs.REASON_FULL_NAME_TAKEN].values()) == 1


def test_michigan_charter_township_is_stored_as_a_township(bs):
    added, _ = _derive(bs, [_cousub(bs, "MI", "Canton charter township", "2616311111")], [])
    assert (added[0]["place_key"], added[0]["display_name"], added[0]["place_type"]) == ("canton|mi", "Canton", "township")


# ---------------------------------------------------------------------------
def test_owned_rows_are_the_scripts_own_and_the_new_jersey_originals_only(bs):
    own = _base_row("a|pa", "A", "PA", ptype="township", conf="census_subdivision")
    legacy_nj = _base_row("b|nj", "B", "NJ", ptype="township", conf="census_first_listed")
    utah = _base_row("c metro|ut", "C metro", "UT", ptype="township", conf="zip_majority")  # a PLACE row whose suffix was "township"
    city = _base_row("d|pa", "D", "PA")
    base, owned = bs.split_owned([own, legacy_nj, utah, city])
    assert owned == [own, legacy_nj]
    assert base == [utah, city]


def test_a_second_run_is_a_no_op(bs, tmp_path, monkeypatch):
    cousub_rows = [
        _cousub(bs, "PA", "Bensalem township", "4201704976"),
        _cousub(bs, "PA", "Washington township", "4201779000"),
        _cousub(bs, "PA", "Washington township", "4209179001"),
        _cousub(bs, "NJ", "Edison township", "3402319000"),
        _cousub(bs, "IL", "Schaumburg township", "4209170000"),
        _cousub(bs, "NY", "Hempstead town", "3608727000"),
        _cousub(bs, "CT", "Glastonbury town", "0911033000"),
    ]
    raw = ("\t".join(bs.COUSUB_HEADER) + "\n" + "\n".join("\t".join(r) for r in cousub_rows) + "\n").encode()
    out = tmp_path / "geo"
    out.mkdir()
    with (out / "us_counties.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(["county_fips", "county_name", "state_usps", "lat", "lng"])
        for fips in sorted(FIPS):
            w.writerow([fips, "County " + fips, "XX", "40.0", "-75.0"])
    base = [
        _base_row("hempstead|ny", "Hempstead", "NY", "36087", "village"),
        _base_row("schaumburg|il", "Schaumburg", "IL", "42091", "village"),
        _base_row("zebra|pa", "Zebra", "PA", "42091"),
    ]
    with (out / "us_places.tsv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=bs.PLACES_FIELDS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(base)

    monkeypatch.setattr(bs.bgd, "OUTPUT_DIR", out)
    monkeypatch.setattr(bs.bgd, "fetch_zip_member", lambda url: raw)

    assert bs.main([]) == 0
    first = (out / "us_places.tsv").read_bytes()
    first_ambiguous = (out / bs.AMBIGUOUS_FILE).read_bytes()
    keys = [r["place_key"] for r in csv.DictReader((out / "us_places.tsv").open(encoding="utf-8"), delimiter="\t")]
    assert keys == sorted(keys, key=lambda k: (k.rsplit("|", 1)[1].upper(), k))
    assert set(keys) == {
        "bensalem|pa", "edison|nj", "glastonbury|ct", "hempstead|ny", "schaumburg township|il", "schaumburg|il", "zebra|pa",
    }  # fmt: skip

    assert bs.main([]) == 0
    assert (out / "us_places.tsv").read_bytes() == first, "second run changed us_places.tsv"
    assert (out / bs.AMBIGUOUS_FILE).read_bytes() == first_ambiguous
    amb = list(csv.DictReader((out / bs.AMBIGUOUS_FILE).open(encoding="utf-8"), delimiter="\t"))
    assert [(r["state_usps"], r["name_key"], r["subdivision_count"]) for r in amb] == [("PA", "washington", "2")]
