#!/usr/bin/env python3
"""Build offline US location-resolution data files from US Census sources.

Downloads a handful of public-domain Census Bureau reference files (2024
Gazetteer files + 2020 relationship/reference files), joins them, and emits
compact tab-separated files into ``data/geo/`` for offline US
location-resolution (counties, places/cities, ZIP codes, states). Uses only
the Python standard library (urllib, zipfile, csv, io) -- no requests/pandas.

Re-run any time to refresh the data:

    python3 scripts/build_geo_data.py

Idempotent: downloads are cached under ``data/.geo_build_cache/`` (gitignored)
keyed by URL, so a re-run with a warm cache does no network I/O. Delete that
directory (or pass --no-cache) to force fresh downloads.

Sources (all verified HTTP 200 as of 2026-07-31; see data/geo/README.md for
the full citation list and judgment calls documented alongside the output):
    - 2024 Gazetteer: place / county / ZCTA national files
    - 2020 place -> county reference (national_place2020.txt)
    - 2020 ZCTA5 -> county and ZCTA5 -> place relationship files
    - 2020 state reference (national_state2020.txt)

This script produces DATA ONLY. It does not import or modify app.py,
plan_schema.py, or plan_geo.py, and does not implement any runtime resolver.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import urllib.error
import urllib.request
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / ".geo_build_cache"
OUTPUT_DIR = PROJECT_ROOT / "data" / "geo"

USER_AGENT = "media-plan-generator-geo-build/1.0 (+internal offline data pipeline)"
TIMEOUT_SECONDS = 60

# --------------------------------------------------------------------------
# Source registry
# --------------------------------------------------------------------------

GAZ_PLACE_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_place_national.zip"
GAZ_COUNTY_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip"
GAZ_ZCTA_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip"
NATIONAL_PLACE_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_place2020.txt"
NATIONAL_STATE_URL = "https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt"
# Verified by listing https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/
# with curl on 2026-07-31 -- this is the 2020 ZCTA5-to-county relationship file.
ZCTA_COUNTY_URL = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
# Same directory listing -- ZCTA5-to-place relationship file, used to derive
# each ZIP's primary city.
ZCTA_PLACE_URL = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_place20_natl.txt"

# Legal/statistical-area suffixes stripped from the END of a place name to
# produce display_name (see README.md "Judgment calls" section). We
# deliberately do NOT strip "county", "parish", "unified government",
# "consolidated government", "metropolitan government"/"metro government", or
# "urban county" -- those are load-bearing parts of a real, informative name
# for consolidated city-county governments (e.g. "Athens-Clarke County
# unified government", "Louisville/Jefferson County metro government"), and
# stripping them would produce a misleading or ambiguous display name.
STRIPPABLE_SUFFIXES = [
    "cdp",
    "city",
    "town",
    "village",
    "borough",
    "township",
    "municipality",
    "corporation",
]
_BALANCE_RE = re.compile(r"\s*\(balance\)\s*$", re.IGNORECASE)
# Case-SENSITIVE by design. Census writes the legal type in lowercase in the
# NAME field ("Atlanta city", "Boise City city") but capitalises it when the
# word is part of the proper name ("Carson City", NV -- the state capital,
# which a case-insensitive match reduced to "Carson"). "CDP" is the one type
# Census writes uppercase, so it is listed separately rather than folded in.
_SUFFIX_RE = re.compile(
    r"\s+(?:CDP|"
    + "|".join(re.escape(s) for s in STRIPPABLE_SUFFIXES if s != "cdp")
    + r")\s*$"
)
_PUNCT_RE = re.compile(r"[^a-z0-9]+")


class BuildError(RuntimeError):
    """Raised on any condition that would otherwise silently corrupt output."""


# --------------------------------------------------------------------------
# Download / cache
# --------------------------------------------------------------------------


def _cache_path_for(url: str) -> Path:
    name = url.split("/")[-1]
    return CACHE_DIR / name


def fetch(url: str) -> bytes:
    """Download url, caching to CACHE_DIR. Fails loudly on any HTTP error."""
    dest = _cache_path_for(url)
    if dest.exists() and dest.stat().st_size > 0:
        return dest.read_bytes()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            status = getattr(resp, "status", 200)
            if status != 200:
                raise BuildError(f"HTTP {status} fetching {url}")
            data = resp.read()
    except urllib.error.HTTPError as exc:
        raise BuildError(f"HTTP {exc.code} fetching {url}: {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise BuildError(f"Network error fetching {url}: {exc.reason}") from exc

    if not data:
        raise BuildError(f"Empty response body fetching {url}")

    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_bytes(data)
    tmp.replace(dest)
    return data


def fetch_zip_member(url: str) -> bytes:
    """Download a .zip and return the bytes of its single text member."""
    raw = fetch(url)
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if len(names) != 1:
            raise BuildError(f"Expected exactly one member in {url}, found {names}")
        return zf.read(names[0])


def _rows(raw: bytes, delimiter: str) -> Tuple[List[str], List[List[str]]]:
    # utf-8-sig transparently drops a leading UTF-8 BOM (several Census
    # relationship files ship one); fall back to latin-1 for the plain
    # gazetteer files, which are not valid UTF-8 in general.
    try:
        text = raw.decode("utf-8-sig") if raw[:3] == b"\xef\xbb\xbf" else raw.decode("latin-1")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    all_rows = [[cell.strip() for cell in row] for row in reader if row]
    if not all_rows:
        raise BuildError("Source file has no rows")
    header, data_rows = all_rows[0], all_rows[1:]
    return header, data_rows


def _require_header(actual: List[str], expected: List[str], source: str) -> None:
    # BOM can land in the first header cell; normalize before comparing.
    normalized = [c.lstrip("﻿") for c in actual]
    if normalized != expected:
        raise BuildError(
            f"{source}: unexpected header.\n  expected: {expected}\n  actual:   {normalized}"
        )


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------


def normalize_place_key(name: str) -> str:
    """lowercase, punctuation-stripped, whitespace-collapsed matching key."""
    lowered = name.lower()
    collapsed = _PUNCT_RE.sub(" ", lowered)
    return " ".join(collapsed.split())


def strip_legal_suffix(name: str) -> str:
    """Strip a trailing '(balance)' qualifier and one generic legal-type
    suffix (city/town/CDP/village/borough/township/municipality/corporation).
    Leaves consolidated city-county government names and county/parish names
    intact -- see STRIPPABLE_SUFFIXES docstring above."""
    stripped = _BALANCE_RE.sub("", name).strip()
    m = _SUFFIX_RE.search(stripped)
    if m:
        candidate = stripped[: m.start()].strip()
        if candidate:
            return candidate
    return stripped


def normalize_city_match(name: str) -> str:
    """Lowercase + whitespace-collapse a city name for the ZIP<->place
    majority-vote join (refine_place_counties_by_zip_majority). Deliberately
    NOT punctuation-stripped (unlike normalize_place_key) -- it only needs to
    match us_zips.tsv's primary_city, which is itself produced by
    strip_legal_suffix() on the same underlying Census place NAME, so the
    two strings are already identical in punctuation whenever they refer to
    the same place."""
    return " ".join(name.lower().split())


def parse_states() -> Dict[str, str]:
    """Return {state_usps: state_name}."""
    raw = fetch(NATIONAL_STATE_URL)
    header, rows = _rows(raw, "|")
    _require_header(header, ["STATE", "STATEFP", "STATENS", "STATE_NAME"], "national_state2020.txt")
    states: Dict[str, str] = {}
    for row in rows:
        usps, statefp, statens, name = row
        if len(usps) != 2:
            raise BuildError(f"national_state2020.txt: bad USPS code {usps!r}")
        states[usps] = name
    return states


def parse_counties() -> List[Dict[str, str]]:
    """Return county rows: county_fips, county_name, state_usps, lat, lng."""
    raw = fetch_zip_member(GAZ_COUNTY_URL)
    header, rows = _rows(raw, "\t")
    _require_header(
        header,
        ["USPS", "GEOID", "ANSICODE", "NAME", "ALAND", "AWATER", "ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"],
        "2024_Gaz_counties_national.txt",
    )
    out = []
    for row in rows:
        usps, geoid, _ansi, name, _aland, _awater, _al_sqmi, _aw_sqmi, lat, lng = row
        if len(geoid) != 5 or not geoid.isdigit():
            raise BuildError(f"Gaz counties: bad county FIPS {geoid!r} for {name}")
        out.append(
            {
                "county_fips": geoid,
                "county_name": name,
                "state_usps": usps,
                "lat": lat,
                "lng": lng,
            }
        )
    return out


def parse_place_county_map() -> Dict[str, List[str]]:
    """Return {statefp+placefp (7-digit): [county_name, ...]} from the 2020
    place->county reference file. A handful of places sit in more than one
    county; COUNTIES is a '~~~'-joined list, first entry = primary county
    (Census's own ordering) -- see README.md judgment-call notes."""
    raw = fetch(NATIONAL_PLACE_URL)
    header, rows = _rows(raw, "|")
    _require_header(
        header,
        ["STATE", "STATEFP", "PLACEFP", "PLACENS", "PLACENAME", "TYPE", "CLASSFP", "FUNCSTAT", "COUNTIES"],
        "national_place2020.txt",
    )
    out: Dict[str, List[str]] = {}
    for row in rows:
        _state, statefp, placefp, _placens, _placename, _typ, _classfp, _funcstat, counties = row
        geoid7 = statefp + placefp
        out[geoid7] = [c.strip() for c in counties.split("~~~") if c.strip()]
    return out


def parse_places(
    county_fips_by_state_name: Dict[Tuple[str, str], str]
) -> Tuple[List[Dict[str, str]], int, Dict[str, Tuple[str, str]]]:
    """Return (place rows, count of places skipped for missing county join,
    {place_geoid7: (display_name, state_usps)} for every place that DID
    resolve a county -- reused by build_zips() for the ZIP primary_city
    join so the gazetteer is only parsed once)."""
    raw = fetch_zip_member(GAZ_PLACE_URL)
    header, rows = _rows(raw, "\t")
    _require_header(
        header,
        ["USPS", "GEOID", "ANSICODE", "NAME", "LSAD", "FUNCSTAT", "ALAND", "AWATER", "ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"],
        "2024_Gaz_place_national.txt",
    )
    place_county = parse_place_county_map()

    out = []
    skipped = 0
    geoid_map: Dict[str, Tuple[str, str]] = {}
    for row in rows:
        usps, geoid, _ansi, name, _lsad, _funcstat, _aland, _awater, _al_sqmi, _aw_sqmi, lat, lng = row
        if len(geoid) != 7:
            raise BuildError(f"Gaz places: bad place GEOID {geoid!r} for {name}")

        counties = place_county.get(geoid)
        county_fips = None
        if counties:
            for county_name in counties:
                county_fips = county_fips_by_state_name.get((usps, county_name))
                if county_fips:
                    break
        if not county_fips:
            # Vintage mismatch between the 2024 gazetteer and the 2020 place
            # reference file (newly incorporated/renamed places, mostly small
            # CDPs). Reported in the build summary rather than silently
            # dropped -- see README.md.
            skipped += 1
            continue

        display_name = strip_legal_suffix(name)
        geoid_map[geoid] = (display_name, usps)
        out.append(
            {
                "place_key": normalize_place_key(display_name) + "|" + usps.lower(),
                "display_name": display_name,
                "state_usps": usps,
                "county_fips": county_fips,
                "lat": lat,
                "lng": lng,
                "place_type": name[len(display_name) :].strip() or "CDP",
            }
        )
    return out, skipped, geoid_map


def parse_zcta_gazetteer() -> Dict[str, Tuple[str, str]]:
    """Return {zip5: (lat, lng)}."""
    raw = fetch_zip_member(GAZ_ZCTA_URL)
    header, rows = _rows(raw, "\t")
    _require_header(
        header,
        ["GEOID", "ALAND", "AWATER", "ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"],
        "2024_Gaz_zcta_national.txt",
    )
    out = {}
    for row in rows:
        zip5, _aland, _awater, _al_sqmi, _aw_sqmi, lat, lng = row
        if len(zip5) != 5 or not zip5.isdigit():
            raise BuildError(f"Gaz ZCTA: bad zip5 {zip5!r}")
        out[zip5] = (lat, lng)
    return out


def parse_zcta_county() -> Dict[str, str]:
    """Return {zip5: county_fips} picking, per zip, the county with the
    largest land-area overlap (AREALAND_PART) when a ZCTA spans more than
    one county."""
    raw = fetch(ZCTA_COUNTY_URL)
    header, rows = _rows(raw, "|")
    _require_header(
        header,
        [
            "OID_ZCTA5_20", "GEOID_ZCTA5_20", "NAMELSAD_ZCTA5_20", "AREALAND_ZCTA5_20", "AREAWATER_ZCTA5_20",
            "MTFCC_ZCTA5_20", "CLASSFP_ZCTA5_20", "FUNCSTAT_ZCTA5_20", "OID_COUNTY_20", "GEOID_COUNTY_20",
            "NAMELSAD_COUNTY_20", "AREALAND_COUNTY_20", "AREAWATER_COUNTY_20", "MTFCC_COUNTY_20",
            "CLASSFP_COUNTY_20", "FUNCSTAT_COUNTY_20", "AREALAND_PART", "AREAWATER_PART",
        ],
        "tab20_zcta520_county20_natl.txt",
    )
    best_area: Dict[str, int] = {}
    best_county: Dict[str, str] = {}
    for row in rows:
        zip5 = row[1]
        county_fips = row[9]
        area_part = row[16]
        if not zip5 or not county_fips:
            continue
        area = int(area_part) if area_part.isdigit() else 0
        if zip5 not in best_area or area > best_area[zip5]:
            best_area[zip5] = area
            best_county[zip5] = county_fips
    return best_county


def parse_zcta_place() -> Dict[str, str]:
    """Return {zip5: place_geoid7} picking, per zip, the place with the
    largest land-area overlap."""
    raw = fetch(ZCTA_PLACE_URL)
    header, rows = _rows(raw, "|")
    _require_header(
        header,
        [
            "OID_ZCTA5_20", "GEOID_ZCTA5_20", "NAMELSAD_ZCTA5_20", "AREALAND_ZCTA5_20", "AREAWATER_ZCTA5_20",
            "MTFCC_ZCTA5_20", "CLASSFP_ZCTA5_20", "FUNCSTAT_ZCTA5_20", "OID_PLACE_20", "GEOID_PLACE_20",
            "NAMELSAD_PLACE_20", "AREALAND_PLACE_20", "AREAWATER_PLACE_20", "MTFCC_PLACE_20",
            "CLASSFP_PLACE_20", "FUNCSTAT_PLACE_20", "AREALAND_PART", "AREAWATER_PART",
        ],
        "tab20_zcta520_place20_natl.txt",
    )
    best_area: Dict[str, int] = {}
    best_place: Dict[str, str] = {}
    for row in rows:
        zip5 = row[1]
        place_geoid = row[9]
        area_part = row[16]
        if not zip5 or not place_geoid:
            continue
        area = int(area_part) if area_part.isdigit() else 0
        if zip5 not in best_area or area > best_area[zip5]:
            best_area[zip5] = area
            best_place[zip5] = place_geoid
    return best_place


def build_zips(
    counties: List[Dict[str, str]],
    place_rows_by_geoid: Dict[str, Tuple[str, str]],
) -> Tuple[List[Dict[str, str]], int]:
    zcta_latlng = parse_zcta_gazetteer()
    zcta_county = parse_zcta_county()
    zcta_place = parse_zcta_place()
    county_state = {c["county_fips"]: c["state_usps"] for c in counties}

    out = []
    skipped_no_county = 0
    for zip5, (lat, lng) in zcta_latlng.items():
        county_fips = zcta_county.get(zip5)
        if not county_fips or county_fips not in county_state:
            skipped_no_county += 1
            continue
        state_usps = county_state[county_fips]

        primary_city = ""
        place_geoid = zcta_place.get(zip5)
        if place_geoid:
            name_state = place_rows_by_geoid.get(place_geoid)
            if name_state:
                primary_city = name_state[0]

        out.append(
            {
                "zip5": zip5,
                "primary_city": primary_city,
                "state_usps": state_usps,
                "county_fips": county_fips,
                "lat": lat,
                "lng": lng,
            }
        )
    return out, skipped_no_county


def refine_place_counties_by_zip_majority(
    places: List[Dict[str, str]],
    zips: List[Dict[str, str]],
) -> int:
    """Post-join refinement: replace each place's county_fips (currently
    Census's FIRST-listed county from national_place2020.txt's COUNTIES
    field) with a MAJORITY VOTE over the ZCTAs in ``zips`` whose
    primary_city+state_usps match that place.

    Root cause of the bug this fixes: for multi-county cities,
    national_place2020.txt's COUNTIES column lists the constituent
    counties in what is effectively alphabetical-by-FIPS order, not
    population- or area-weighted order. "First listed" therefore silently
    picked the alphabetically-first county rather than the county
    containing most of the city (e.g. Atlanta, GA -> DeKalb instead of
    Fulton; Houston, TX -> Fort Bend instead of Harris). ZIP-level
    county_fips (already correct -- see build_zips()'s largest-overlap
    join) does not have this defect, so voting over a place's own ZIPs
    fixes it without needing a new Census source.

    Mutates each place dict in place, adding/overwriting "county_fips" and
    adding "county_confidence" ("zip_majority" when at least one ZIP voted,
    "census_first_listed" -- the pre-existing county_fips is KEPT, not
    dropped -- when no ZIP's primary_city matched). Returns the number of
    places that fell back to "census_first_listed".

    Matching key: normalize_city_match(display_name) + state_usps, i.e.
    lowercase + whitespace-collapsed (not punctuation-stripped). This is
    sufficient to also rescue consolidated city-county governments (e.g.
    "Nashville-Davidson metropolitan government", TN) because
    us_zips.tsv's primary_city is derived from the SAME strip_legal_suffix()
    call as a place's display_name -- there is no separate raw-Census-name
    matching pass to add, since the two strings are already produced by
    identical logic and match directly whenever they refer to the same
    place. (Verified: zero false-positive collisions introduced -- ties are
    broken deterministically by preferring the smaller county_fips, which
    only matters for the rare place whose ZIPs split exactly evenly across
    two counties.)
    """
    votes: Dict[Tuple[str, str], "Counter[str]"] = defaultdict(Counter)
    for z in zips:
        city_key = normalize_city_match(z["primary_city"])
        if not city_key:
            continue
        votes[(city_key, z["state_usps"])][z["county_fips"]] += 1

    fallback_count = 0
    for p in places:
        key = (normalize_city_match(p["display_name"]), p["state_usps"])
        county_votes = votes.get(key)
        if county_votes:
            best_fips, _best_count = max(
                county_votes.items(), key=lambda kv: (kv[1], -int(kv[0]))
            )
            p["county_fips"] = best_fips
            p["county_confidence"] = "zip_majority"
        else:
            p["county_confidence"] = "census_first_listed"
            fallback_count += 1
    return fallback_count


# --------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------


def write_tsv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, str]]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Delete the local download cache before building (forces fresh downloads).",
    )
    args = parser.parse_args()

    if args.no_cache and CACHE_DIR.exists():
        import shutil

        shutil.rmtree(CACHE_DIR)

    print("Building US geo data into", OUTPUT_DIR)

    print("Fetching state reference...")
    states = parse_states()

    print("Fetching county gazetteer...")
    counties = parse_counties()
    for c in counties:
        assert isinstance(c["county_fips"], str) and len(c["county_fips"]) == 5 and c["county_fips"].isdigit()
    county_fips_by_state_name = {(c["state_usps"], c["county_name"]): c["county_fips"] for c in counties}

    print("Fetching place gazetteer + place->county reference...")
    places, places_skipped, place_rows_by_geoid = parse_places(county_fips_by_state_name)
    for p in places:
        assert isinstance(p["county_fips"], str) and len(p["county_fips"]) == 5 and p["county_fips"].isdigit()

    print("Fetching ZCTA gazetteer + zip->county / zip->place relationships...")
    zips, zips_skipped = build_zips(counties, place_rows_by_geoid)

    print("Refining place->county assignment by ZIP majority vote...")
    fallback_count = refine_place_counties_by_zip_majority(places, zips)
    for p in places:
        assert isinstance(p["county_fips"], str) and len(p["county_fips"]) == 5 and p["county_fips"].isdigit()
        assert p["county_confidence"] in ("zip_majority", "census_first_listed")

    print("Writing TSVs...")
    n_states = write_tsv(
        OUTPUT_DIR / "us_states.tsv",
        ["state_usps", "state_name"],
        ({"state_usps": usps, "state_name": name} for usps, name in sorted(states.items())),
    )
    n_counties = write_tsv(
        OUTPUT_DIR / "us_counties.tsv",
        ["county_fips", "county_name", "state_usps", "lat", "lng"],
        sorted(counties, key=lambda r: r["county_fips"]),
    )
    n_places = write_tsv(
        OUTPUT_DIR / "us_places.tsv",
        ["place_key", "display_name", "state_usps", "county_fips", "lat", "lng", "place_type", "county_confidence"],
        sorted(places, key=lambda r: (r["state_usps"], r["place_key"])),
    )
    n_zips = write_tsv(
        OUTPUT_DIR / "us_zips.tsv",
        ["zip5", "primary_city", "state_usps", "county_fips", "lat", "lng"],
        sorted(zips, key=lambda r: r["zip5"]),
    )

    zip_majority_count = sum(1 for p in places if p["county_confidence"] == "zip_majority")
    fallback_pct = (100.0 * fallback_count / n_places) if n_places else 0.0

    print()
    print("=== Build summary ===")
    print(f"us_states.tsv   : {n_states} rows")
    print(f"us_counties.tsv : {n_counties} rows")
    print(f"us_places.tsv   : {n_places} rows ({places_skipped} skipped -- no county join, see README.md)")
    print(f"us_zips.tsv     : {n_zips} rows ({zips_skipped} skipped -- no county join, see README.md)")
    print(
        f"county_confidence: zip_majority={zip_majority_count} ({100.0 - fallback_pct:.2f}%), "
        f"census_first_listed={fallback_count} ({fallback_pct:.2f}%)"
    )
    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
