#!/usr/bin/env python3
"""Add county-subdivision municipalities (townships, towns, plantations) to
``data/geo/us_places.tsv`` for every US state.

Why this exists. The Census *place* gazetteer (what ``build_geo_data.py``
reads) lists incorporated places and CDPs. In roughly twenty states the
municipality people actually name is a *county subdivision*, not a place, so
it is simply absent from ``us_places.tsv``:

* townships -- NJ, PA, OH, MI (incl. charter townships), IN, IL, MN, MO, KS,
  NE, ND, SD, NH;
* towns -- NY, WI, and the New England states (CT, MA, ME, NH, RI, VT);
* Maine plantations.

Edison NJ, Cherry Hill NJ, Lower Merion PA, Bensalem PA, Canton MI, Glastonbury
CT ... A resolver that cannot find "Edison, NJ" has two bad options: say
nothing, or fuzzy-match it to a different real town (it picked Madison). The
New Jersey pass closed that for one state; this script is the same pass for
every state, from the same Census *county-subdivision* gazetteer, the same
public-domain source family (17 U.S.C. 105) and the same 2024 vintage as the
rest of ``data/geo/``:

    https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_cousubs_national.zip

Run it AFTER ``build_geo_data.py`` (which rewrites ``us_places.tsv`` from
scratch and therefore drops these rows):

    python3 scripts/build_geo_data.py
    python3 scripts/build_geo_county_subdivisions.py          # --no-cache to re-download

Idempotent: the rows this script owns are marked ``county_confidence ==
"census_subdivision"`` (plus the New-Jersey-only rows of this script's first
version, which were labelled ``census_first_listed``); they are dropped and
re-derived on every run, so a second run is a no-op.

Row policy (every choice is deliberate -- see ``data/geo/README.md``). It is
the exact rule the New Jersey pass used, applied to each state:

* Only subdivisions that are an ACTIVE, functioning government
  (``FUNCSTAT == "A"``) are considered. That drops the statistical-only and
  non-functioning "townships" of AR, NC, IA, ... and the fictitious ones.
* Only names that are UNIQUE within the state are added. Several townships
  with one name (the 20+ Washington townships of Pennsylvania) have no honest
  single primary, so none is added and the resolver answers with the honest
  state-level "not in our place list" instead of a guess. Those names are
  written to ``data/geo/us_subdivision_ambiguous.tsv`` (state, name, how many,
  which counties) so the resolver also knows they are REAL and never
  "corrects" one into a look-alike: "Conewago Township, PA" (three counties)
  must not become Conewango Township (a fourth, different one).
* The row keeps the ``display_name`` convention of the rest of the file (legal
  suffix stripped: "Edison"), keyed ``edison|nj``.
* If that key is already taken by another place -- a borough, a city, a CDP --
  the subdivision is a DIFFERENT municipality. A TOWNSHIP is then stored under
  its full legal name, key ``raritan township|nj``, display "Raritan Township"
  (the resolver reads "<Name> Township"/"Twp" as exactly that row). A TOWN or
  PLANTATION is skipped instead: the resolver has no "<Name> Town" form, and
  the plain name already resolves to a same-named place in the same state.
  Nothing existing is overwritten or merged.
* A Michigan *charter* township is stored as a township (``place_type ==
  "township"``): that is what "Canton Township, MI" means.
* ``county_fips`` is the first five digits of the subdivision GEOID: a county
  subdivision sits inside exactly one county, so this is exact, not a majority
  vote. ``county_confidence`` is "census_subdivision" -- a third value, exact
  like the GEOID it comes from. It also tells the resolver which rows are
  county subdivisions (a township elsewhere must never turn a bare "Chicago"
  into an ambiguity; see ``plan_location.py`` rule 5).
"""

from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_geo_data as bgd  # noqa: E402  (sibling script; reuses its fetch/cache/normalizers)

GAZ_COUSUB_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_cousubs_national.zip"
)
COUSUB_HEADER = [
    "USPS",
    "GEOID",
    "ANSICODE",
    "NAME",
    "FUNCSTAT",
    "ALAND",
    "AWATER",
    "ALAND_SQMI",
    "AWATER_SQMI",
    "INTPTLAT",
    "INTPTLONG",
]
PLACES_FIELDS = [
    "place_key",
    "display_name",
    "state_usps",
    "county_fips",
    "lat",
    "lng",
    "place_type",
    "county_confidence",
]

AMBIGUOUS_FILE = "us_subdivision_ambiguous.tsv"
AMBIGUOUS_FIELDS = ["state_usps", "name_key", "display_name", "subdivision_count", "county_fips_list"]

ACTIVE_FUNCSTAT = "A"
COUNTY_CONFIDENCE = "census_subdivision"
# Rows written by the New-Jersey-only first version of this script carried the
# base file's "census_first_listed"; they are recognised by state + type.
LEGACY_NJ_STATE, LEGACY_NJ_TYPE = "NJ", "township"

# Census NAME suffix (case-SENSITIVE, like build_geo_data: "Charles Town" the
# proper name is not the legal type) -> (stored place_type, suffix that makes
# the full legal name on a key collision, or "" to skip on collision).
# Order matters: " charter township" must be tried before " township".
SUBDIVISION_TYPES: Tuple[Tuple[str, str, str], ...] = (
    (" charter township", "township", " Township"),
    (" township", "township", " Township"),
    (" town", "town", ""),
    (" plantation", "plantation", ""),
)

# Dropped-row reasons, in the order the summary prints them.
REASON_AMBIGUOUS = "name used by 2+ subdivisions in the state"
REASON_COLLISION = "plain name is another place (town/plantation: skipped)"
REASON_FULL_NAME_TAKEN = "full legal name also taken"


def _read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def classify_name(census_name: str) -> Tuple[str, str, str, str] | None:
    """``(stripped_name, place_type, collision_suffix, normalised_key)`` for a
    Census cousub NAME of a type this script adds, else ``None``."""
    for suffix, place_type, collision_suffix in SUBDIVISION_TYPES:
        if census_name.endswith(suffix):
            stripped = census_name[: -len(suffix)].strip()
            key = bgd.normalize_place_key(stripped)
            if not key:
                return None
            return stripped, place_type, collision_suffix, key
    return None


def parse_subdivisions(
    rows: Iterable[Sequence[str]], idx: Dict[str, int], known_county_fips: set[str]
) -> Tuple[Dict[Tuple[str, str], Dict[str, str]], Dict[Tuple[str, str], List[Dict[str, str]]]]:
    """``({(state_usps, key): subdivision}, {(state_usps, key): [subdivisions]})``.

    Only active subdivisions of a type in ``SUBDIVISION_TYPES`` are grouped by
    ``(state, normalised stripped name)``; a group of exactly one becomes a
    candidate, a bigger group is returned apart (no honest primary, so no
    place row -- but the resolver is told the name is real, see
    ``AMBIGUOUS_FILE``)."""
    groups: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        if row[idx["FUNCSTAT"]] != ACTIVE_FUNCSTAT:
            continue
        info = classify_name(row[idx["NAME"]])
        if info is None:
            continue
        stripped, place_type, collision_suffix, key = info
        geoid = row[idx["GEOID"]]
        county_fips = geoid[:5]
        if len(geoid) != 10 or county_fips not in known_county_fips:
            raise bgd.BuildError(
                f"cousub {row[idx['NAME']]!r}: GEOID {geoid!r} / county {county_fips!r} not in us_counties.tsv"
            )
        groups[(row[idx["USPS"]], key)].append(
            {
                "name": stripped,
                "place_type": place_type,
                "collision_suffix": collision_suffix,
                "county_fips": county_fips,
                "lat": row[idx["INTPTLAT"]].strip(),
                "lng": row[idx["INTPTLONG"]].strip(),
            }
        )
    unique: Dict[Tuple[str, str], Dict[str, str]] = {}
    ambiguous: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for (usps, key), group in groups.items():
        if len(group) == 1:
            unique[(usps, key)] = group[0]
        else:
            ambiguous[(usps, key)] = group
    return unique, ambiguous


def ambiguous_rows(ambiguous: Dict[Tuple[str, str], List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """The rows of ``AMBIGUOUS_FILE``: one per skipped (state, name)."""
    out = []
    for (usps, key), group in sorted(ambiguous.items()):
        out.append(
            {
                "state_usps": usps,
                "name_key": key,
                "display_name": sorted({g["name"] for g in group})[0],
                "subdivision_count": str(len(group)),
                "county_fips_list": "|".join(sorted({g["county_fips"] for g in group})),
            }
        )
    return out


def _row(key: str, display: str, usps: str, sub: Dict[str, str]) -> Dict[str, str]:
    return {
        "place_key": key,
        "display_name": display,
        "state_usps": usps,
        "county_fips": sub["county_fips"],
        "lat": sub["lat"],
        "lng": sub["lng"],
        "place_type": sub["place_type"],
        "county_confidence": COUNTY_CONFIDENCE,
    }


def is_owned(row: Dict[str, str]) -> bool:
    """True for a row an earlier run of this script wrote."""
    if row["county_confidence"] == COUNTY_CONFIDENCE:
        return True
    return row["state_usps"] == LEGACY_NJ_STATE and row["place_type"] == LEGACY_NJ_TYPE


def split_owned(existing: List[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """``(base_rows, owned_rows)``: ``existing`` split into the rows this script
    wrote on an earlier run and the rest, so a re-run re-derives its own rows
    from scratch against an untouched base."""
    base = [r for r in existing if not is_owned(r)]
    owned = [r for r in existing if is_owned(r)]
    return base, owned


def derive_rows(
    unique: Dict[Tuple[str, str], Dict[str, str]], base: List[Dict[str, str]]
) -> Tuple[List[Dict[str, str]], Dict[str, Counter]]:
    """``(added_rows, {reason_or_form: Counter(state)})`` for ``unique`` against
    the ``base`` places. Deterministic: candidates are taken in (state, key)
    order."""
    taken = {r["place_key"] for r in base}
    added: List[Dict[str, str]] = []
    stats: Dict[str, Counter] = defaultdict(Counter)
    for (usps, key), sub in sorted(unique.items()):
        plain_key = f"{key}|{usps.lower()}"
        if plain_key not in taken:
            row = _row(plain_key, sub["name"], usps, sub)
            stats["plain"][usps] += 1
        elif sub["collision_suffix"]:
            display = sub["name"] + sub["collision_suffix"]
            full_key = f"{bgd.normalize_place_key(display)}|{usps.lower()}"
            if full_key in taken:
                stats[REASON_FULL_NAME_TAKEN][usps] += 1
                continue
            row = _row(full_key, display, usps, sub)
            stats["full_name"][usps] += 1
        else:
            stats[REASON_COLLISION][usps] += 1
            continue
        taken.add(row["place_key"])
        added.append(row)
    return added, stats


def _top(counter: Counter, n: int = 10) -> str:
    return ", ".join(f"{s} {c}" for s, c in counter.most_common(n))


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if "--no-cache" in args:
        cached = bgd._cache_path_for(GAZ_COUSUB_URL)
        if cached.exists():
            cached.unlink()

    places_path = bgd.OUTPUT_DIR / "us_places.tsv"
    known_fips = {c["county_fips"] for c in _read_tsv(bgd.OUTPUT_DIR / "us_counties.tsv")}
    existing = _read_tsv(places_path)

    raw = bgd.fetch_zip_member(GAZ_COUSUB_URL)
    header, rows = bgd._rows(raw, "\t")
    bgd._require_header(header, COUSUB_HEADER, "2024_Gaz_cousubs_national.txt")
    idx = {h: i for i, h in enumerate(COUSUB_HEADER)}

    unique, ambiguous = parse_subdivisions(rows, idx, known_fips)
    base, owned = split_owned(existing)
    added, stats = derive_rows(unique, base)

    merged = sorted(base + added, key=lambda r: (r["state_usps"], r["place_key"]))
    n = bgd.write_tsv(places_path, PLACES_FIELDS, merged)
    n_ambiguous = bgd.write_tsv(bgd.OUTPUT_DIR / AMBIGUOUS_FILE, AMBIGUOUS_FIELDS, ambiguous_rows(ambiguous))

    # "Added" = not in the file before this run (a re-run adds nothing; the
    # first run over the New-Jersey-only version re-derives its 211 rows).
    previously = {r["place_key"] for r in owned}
    net_new = [r for r in added if r["place_key"] not in previously]
    per_state = Counter(r["state_usps"] for r in added)
    new_per_state = Counter(r["state_usps"] for r in net_new)
    print(
        f"us_places.tsv: {len(base)} base rows + {len(added)} county subdivisions = {n} rows "
        f"({len(owned)} from an earlier run re-derived, {len(net_new)} new)"
    )
    print(f"  under the plain name: {sum(stats['plain'].values())}; under '<Name> Township': {sum(stats['full_name'].values())}")
    print(f"  skipped, {REASON_AMBIGUOUS}: {len(ambiguous)} (listed in {AMBIGUOUS_FILE}: {n_ambiguous} rows)")
    for reason in (REASON_COLLISION, REASON_FULL_NAME_TAKEN):
        print(f"  skipped, {reason}: {sum(stats[reason].values())}")
    print(f"  rows per state ({len(per_state)} states; total / new this run):")
    for usps, count in sorted(per_state.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"    {usps}: {count} / {new_per_state.get(usps, 0)}")
    print(f"  top 10 new: {_top(new_per_state)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
