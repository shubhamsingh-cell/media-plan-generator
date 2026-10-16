#!/usr/bin/env python3
"""Add New Jersey townships to ``data/geo/us_places.tsv``.

Why this exists. The Census *place* gazetteer (what ``build_geo_data.py``
reads) lists incorporated places and CDPs. A New Jersey township is a *county
subdivision*, not a place, unless a CDP happens to cover it -- so Edison,
Cherry Hill, Piscataway, Brick and ~170 more of the state's biggest
municipalities are simply absent from ``us_places.tsv``. A resolver that
cannot find "Edison, NJ" then has two bad options: say nothing, or fuzzy-match
it to a different real town (it picked Madison). This script closes the gap
from the Census *county-subdivision* gazetteer, the same public-domain
source family (17 U.S.C. 105) and the same 2024 vintage as the rest of
``data/geo/``:

    https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_cousubs_national.zip

Run it AFTER ``build_geo_data.py`` (which rewrites ``us_places.tsv`` from
scratch and therefore drops these rows):

    python3 scripts/build_geo_data.py
    python3 scripts/build_geo_nj_townships.py

Idempotent: rows this script owns (state NJ, ``place_type == "township"``) are
dropped and re-derived on every run, so a second run is a no-op.

Row policy (every choice is deliberate -- see ``data/geo/README.md``):

* Only townships whose name is UNIQUE within the state are added. Fairfield,
  Hamilton, Lawrence, Mansfield, Monroe, Ocean (and Franklin, Washington,
  Greenwich, Hopewell, Springfield, Union, which also share a name with a
  borough/CDP) each exist in two or more counties; picking one would be
  inventing a primary, so none is added and the resolver answers with the
  honest state-level "not in our place list" instead of a guess.
* The row keeps the ``display_name`` convention of the rest of the file
  (legal suffix stripped: "Edison"), keyed ``edison|nj``.
* If that key is already taken by another place -- a borough, a city, a CDP
  in another county (e.g. Raritan borough vs Raritan township, Hampton
  borough vs Hampton township) -- the township is a DIFFERENT place, so it is
  stored under its full legal name, key ``raritan township|nj``, display name
  "Raritan Township". Nothing existing is overwritten or merged.
* ``county_fips`` is the first five digits of the subdivision GEOID: a county
  subdivision sits inside exactly one county, so this is exact, not a
  majority vote. ``county_confidence`` stays "census_first_listed" because
  that column only admits the two documented values (and it is the
  conservative one).
"""

from __future__ import annotations

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List

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
TOWNSHIP_STATES = ("NJ",)
TOWNSHIP_SUFFIX = " township"
TOWNSHIP_PLACE_TYPE = "township"


def _read_tsv(path: Path) -> List[Dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def parse_townships(known_county_fips: set[str]) -> Dict[str, List[Dict[str, str]]]:
    """``{state_usps: [township rows with a unique name in that state]}``."""
    raw = bgd.fetch_zip_member(GAZ_COUSUB_URL)
    header, rows = bgd._rows(raw, "\t")
    bgd._require_header(header, COUSUB_HEADER, "2024_Gaz_cousubs_national.txt")
    idx = {h: i for i, h in enumerate(COUSUB_HEADER)}

    by_state_key: Dict[tuple[str, str], List[List[str]]] = defaultdict(list)
    for row in rows:
        usps = row[idx["USPS"]]
        name = row[idx["NAME"]]
        if usps not in TOWNSHIP_STATES or not name.endswith(TOWNSHIP_SUFFIX):
            continue
        if row[idx["FUNCSTAT"]] != "A":  # active, functioning government only
            continue
        stripped = name[: -len(TOWNSHIP_SUFFIX)].strip()
        by_state_key[(usps, bgd.normalize_place_key(stripped))].append(row)

    out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for (usps, _key), group in sorted(by_state_key.items()):
        if len(group) != 1:
            continue  # same name in 2+ counties: no honest single primary
        row = group[0]
        geoid = row[idx["GEOID"]]
        county_fips = geoid[:5]
        if len(geoid) != 10 or county_fips not in known_county_fips:
            raise bgd.BuildError(f"cousub {row[idx['NAME']]!r}: GEOID {geoid!r} / county {county_fips!r} not in us_counties.tsv")
        out[usps].append(
            {
                "name": row[idx["NAME"]][: -len(TOWNSHIP_SUFFIX)].strip(),
                "county_fips": county_fips,
                "lat": row[idx["INTPTLAT"]].strip(),
                "lng": row[idx["INTPTLONG"]].strip(),
            }
        )
    return out


def main() -> int:
    places_path = bgd.OUTPUT_DIR / "us_places.tsv"
    counties = _read_tsv(bgd.OUTPUT_DIR / "us_counties.tsv")
    known_fips = {c["county_fips"] for c in counties}

    existing = _read_tsv(places_path)
    # Drop the rows this script owns, so a re-run re-derives them from scratch.
    base = [
        r
        for r in existing
        if not (r["state_usps"] in TOWNSHIP_STATES and r["place_type"] == TOWNSHIP_PLACE_TYPE)
    ]
    taken = {r["place_key"] for r in base}

    townships = parse_townships(known_fips)
    added: List[Dict[str, str]] = []
    stripped_keys = 0
    full_name_keys = 0
    for usps, rows in townships.items():
        for t in rows:
            key = bgd.normalize_place_key(t["name"]) + "|" + usps.lower()
            display = t["name"]
            if key in taken:
                display = f"{t['name']} Township"
                key = bgd.normalize_place_key(display) + "|" + usps.lower()
                full_name_keys += 1
            else:
                stripped_keys += 1
            if key in taken:
                raise bgd.BuildError(f"township key {key!r} collides even with its full legal name")
            taken.add(key)
            added.append(
                {
                    "place_key": key,
                    "display_name": display,
                    "state_usps": usps,
                    "county_fips": t["county_fips"],
                    "lat": t["lat"],
                    "lng": t["lng"],
                    "place_type": TOWNSHIP_PLACE_TYPE,
                    "county_confidence": "census_first_listed",
                }
            )

    merged = sorted(base + added, key=lambda r: (r["state_usps"], r["place_key"]))
    n = bgd.write_tsv(places_path, PLACES_FIELDS, merged)
    per_state = Counter(r["state_usps"] for r in added)
    print(
        f"us_places.tsv: {len(base)} base rows + {len(added)} townships {dict(per_state)} "
        f"({stripped_keys} under the plain name, {full_name_keys} under '<Name> Township') = {n} rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
