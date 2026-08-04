"""Runtime US location resolver, backed by the committed `data/geo/*.tsv`
reference tables (see `data/geo/README.md` for provenance/vintage/gaps).

Public API:
    resolve_location(raw: str) -> LocationResolution
    resolve_locations(raw_list: list[str]) -> list[LocationResolution]

This module REPLACES the 32-entry hardcoded ``_LOCATION_CORRECTIONS`` dict
in app.py (search "S49 FIX (Issue 14)") with a general fuzzy-match path
(rule 9 below) that covers every one of those 32 misspellings plus any
other typo close enough to a real US place name -- see the "Fuzzy
matching" section for the cutoff and how it was chosen.

Resolution order (first match wins; all matching is case/whitespace/
punctuation insensitive via ``_norm_key``):

  1. 5-digit ZIP, or ZIP+4 ("30303-1234" -> "30303") -> exact `us_zips`
     lookup. A well-formed but ABSENT zip5 (e.g. "30301" -- see
     `data/geo/README.md` judgment call 5, a real PO-Box-only USPS zip
     with no ZCTA) does not error: it returns status="unresolved" but
     with a helpful note and, when a same-prefix ZIP exists, a suggested
     nearby alternative.
  2. "City, ST" / "City, State Name" (comma) or comma-less "City ST" ->
     `us_places` lookup keyed on (place_key, state). Also tries a
     "County, ST" pattern against `us_counties` when the place lookup
     misses and the left-hand side ends in "county"/"parish"/"borough".
  3. "City, US" / "City, USA" / "City, United States" -> treated as a
     bare city with unspecified state; falls through to rule 5.
  4. Bare state name or 2-letter USPS abbreviation ("Texas", "TX",
     including the 5 non-state territories in `us_states.tsv`) ->
     kind="state".
  5. Bare city name, no state -> look up the place_key across ALL
     states.
       - Exactly one match -> resolved.
       - Multiple matches -> status="ambiguous". The dataset carries no
         population column, so "most likely" cannot be population-
         ranked from the data alone. Tiebreak (documented, deterministic):
         (a) if the city is in the curated `_WELL_KNOWN_METRO_STATE`
             table (~60 major US metros, hand-picked by real 2020-census
             population when the name collides across states -- e.g.
             Portland, OR (652k) over Portland, ME (68k)), that state is
             the primary;
         (b) otherwise, alphabetical-by-state-then-place_key. This is
             honest-but-weak (an alphabetically-first state is not
             "more likely"), so it is used only as a last-resort
             tiebreak, and `note` always says which rule fired.
       Status stays "ambiguous" either way -- the tiebreak only decides
       which candidate is listed as primary vs. in `alternatives`, never
       upgrades ambiguous to resolved.
  6. Country/nationwide keywords ("United States", "US", "USA",
     "Nationwide", "National", "All US") -> kind="country" or
     "nationwide", status="resolved" (this is a valid targeting answer).
  7. "Remote" / "Work From Home" / "WFH" / "Anywhere" -> kind="remote",
     status="resolved" (also a valid answer, not a failure).
  8. Non-US locations ("London, UK", "Toronto, Canada", "Bangalore") ->
     status="unresolved", kind="unknown", with a note that explicitly
     says this is out of US-only scope today, not an error. Country
     classification is delegated to `plan_geo._resolve_candidate` (the
     same US-state-table + currency-lookup + intl-token classifier
     `plan_geo.is_us_plan` uses) instead of a second hand-maintained
     country list -- see plan_geo.py's module docstring.
  9. Fuzzy correction (stdlib `difflib.get_close_matches`) against real
     place names -> status="corrected", noting the correction made.
 10. Nothing matched -> status="unresolved", kind="unknown".

Fuzzy matching (rule 9): cutoff=0.75 on `difflib.get_close_matches`,
candidates pre-filtered to same first letter and length within +/-2 of
the query (see `_fuzzy_city_lookup`) purely for speed -- every one of
the 32 legacy `_LOCATION_CORRECTIONS` misspellings preserves its first
letter, so this filter drops nothing the legacy dict covered while
cutting the candidate pool by ~15-30x. At 0.75, all garbage tested
("asdfghjkl", "xyzzyx123", "qqqqqqqqq", 500-char strings) produced zero
matches; "pittsburg" (one of the 32 legacy keys) is a real US place in
5 states (CA/IL/KS/OK/TX) and is deliberately caught by the exact bare-
city lookup (rule 5, ambiguous) BEFORE fuzzy ever runs -- rule 5 takes
precedence over rule 9 by construction, which is more honest than the
old hardcoded dict's blind "pittsburg" -> "Pittsburgh" overwrite (a real
place name is not a misspelling). See tests/test_plan_location.py for
the false-positive sweep this cutoff was tuned against.

DMA (Designated Market Area) is PLUGGABLE and OFF by default -- there is
an unresolved licensing question about DMA data. If
`data/geo/dma_by_county.tsv` (columns: county_fips, dma_code, dma_name)
exists, it is loaded and used to populate `dma_code`/`dma_name` on any
resolution carrying a county_fips, with `dma_source` set to whatever
free-text source string is baked into that file (never hardcoded here,
and never attributed to any commercial ratings vendor -- this is derived
data, not a licensed proprietary market-area feed). If the file is
absent (true today), `dma_code`/`dma_name` stay None and
`dma_status="unavailable"`; nothing else in this module is affected, and
no warning is logged for the expected/absent case.

CBSA (Core Based Statistical Area) is a genuine, free, public-domain
metro-level substitute for market grouping -- OMB's delineation of counties
into metro/micro statistical areas ("Atlanta-Sandy Springs-Roswell, GA",
etc.), published by the US Census Bureau (17 U.S.C. 105, public domain).
Like DMA, it is PLUGGABLE via an optional file: if
`data/geo/cbsa_by_county.tsv` (columns: county_fips, cbsa_code, cbsa_title,
area_type), built by `scripts/build_cbsa_data.py`, exists, it is loaded and
used to populate `cbsa_code`/`cbsa_title` on any resolution carrying a
county_fips, with `cbsa_status="available"`. Unlike DMA, this file ships
with the repo (built entirely from public data, no licensing question).
`cbsa_status` stays "unavailable" -- never a fabricated CBSA -- when the
file is absent, the resolution has no county_fips at all, or the county
genuinely sits outside any CBSA (true for many rural/unincorporated
counties; a real geographic fact, not a data gap).

Known limitation (rule 8 / rule 9 interaction): rule 8's non-US check
delegates to `plan_geo._resolve_candidate`, which in turn calls
`plan_currency.currency_for_country`. That function has a >=5-char
SUBSTRING fallback for country aliases (e.g. "india" inside a longer
word) -- real for country-in-phrase cases like "Indiana Jones Ave" style
free text, but it also fires on typo'd US city names that happen to
contain a country alias, e.g. "indianpolis"/"indianapolis" contain
"india". If rule 8 ran before rule 9, a typo this unlucky would get
mislabeled non-US and never reach the fuzzy-correction path meant to fix
exactly this kind of typo. So this module runs rule 9 (fuzzy, scoped to
real US place names) BEFORE rule 8 (non-US) whenever rule 5's exact
bare-city lookup misses -- a deliberate reordering from the numbered
list above, kept because the numbered order as literally read would
misresolve a real legacy-dict entry (Indianapolis). Only when fuzzy
finds nothing does the non-US classifier get consulted.
"""

from __future__ import annotations

import csv
import difflib
import logging
import re
import threading
import unicodedata
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import plan_geo

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent
_GEO_DIR = _PROJECT_ROOT / "data" / "geo"

_ZIP_RE = re.compile(r"^\s*(\d{5})(?:-\d{4})?\s*$")
_SPLIT_WS = re.compile(r"\s+")
_NON_ALNUM_SPACE = re.compile(r"[^a-z0-9\s]")

_TAIL_US_TOKENS = frozenset({"us", "usa", "united states", "united states of america"})
_COUNTRY_TOKENS = frozenset({"us", "usa", "united states", "united states of america", "america"})
_NATIONWIDE_TOKENS = frozenset({"nationwide", "national", "all us", "all-us"})
_REMOTE_TOKENS = frozenset(
    {"remote", "work from home", "wfh", "anywhere", "fully remote", "remote us", "remote usa", "100 remote"}
)
_COUNTY_SUFFIXES = (" county", " parish", " borough")

# Curated well-known-metro tiebreak: real 2020-census population ranking
# used ONLY to pick which state is listed as the primary candidate when a
# bare city name collides across states (rule 5). This does not change
# status -- collisions always stay "ambiguous". Deliberately does NOT
# include every collision (e.g. "springfield" is left to the alphabetical
# fallback) -- see module docstring and tests for both tiebreak paths.
_WELL_KNOWN_METRO_STATE: dict[str, str] = {
    "new york": "NY",
    "los angeles": "CA",
    "chicago": "IL",
    "houston": "TX",
    "phoenix": "AZ",
    "philadelphia": "PA",
    "san antonio": "TX",
    "san diego": "CA",
    "dallas": "TX",
    "austin": "TX",
    "san francisco": "CA",
    "seattle": "WA",
    "denver": "CO",
    "boston": "MA",
    "nashville": "TN",
    "portland": "OR",
    "oklahoma city": "OK",
    "las vegas": "NV",
    "detroit": "MI",
    "memphis": "TN",
    "louisville": "KY",
    "baltimore": "MD",
    "milwaukee": "WI",
    "albuquerque": "NM",
    "tucson": "AZ",
    "fresno": "CA",
    "sacramento": "CA",
    "atlanta": "GA",
    "kansas city": "MO",
    "colorado springs": "CO",
    "miami": "FL",
    "raleigh": "NC",
    "omaha": "NE",
    "cleveland": "OH",
    "tulsa": "OK",
    "minneapolis": "MN",
    "wichita": "KS",
    "arlington": "TX",
    "tampa": "FL",
    "new orleans": "LA",
    "honolulu": "HI",
    "aurora": "CO",
    "anaheim": "CA",
    "santa ana": "CA",
    "pittsburgh": "PA",
    "cincinnati": "OH",
    "indianapolis": "IN",
    "columbus": "OH",
    "charlotte": "NC",
    "jacksonville": "FL",
    "richmond": "VA",
    "buffalo": "NY",
    "rochester": "NY",
    "syracuse": "NY",
    "knoxville": "TN",
    "tallahassee": "FL",
    "murfreesboro": "TN",
    "harrisburg": "PA",
    "ithaca": "NY",
    # Metros reachable only via _CENSUS_NAME_ALIASES below; the tiebreak
    # needs them too, or the alias-added candidate loses to an alphabetically
    # earlier small town of the same name.
    "nashville": "TN",
    "lexington": "KY",
    "augusta": "GA",
    "macon": "GA",
    "athens": "GA",
    "butte": "MT",
    # Both spellings: the tiebreak keys off the normalized input, and
    # "Saint Louis" normalizes to "saint louis", not "st louis".
    "st louis": "MO",
    "saint louis": "MO",
    "carson city": "NV",
    "kansas city": "MO",
    "st paul": "MN",
    "saint paul": "MN",
    "st petersburg": "FL",
    "saint petersburg": "FL",
}

# Census records some major markets under a legal name nobody types. Without
# these, "Boise" found no place at all and fell through to fuzzy matching,
# which silently "corrected" it to Bowie, AZ -- a confident wrong answer on
# the very surface built to prevent them. Each entry ADDS the real place to
# the candidate set; it never removes the plain-name matches, so a genuine
# collision still resolves as ambiguous and asks the user.
_CENSUS_NAME_ALIASES: dict[str, tuple[str, ...]] = {
    "boise": ("boise city|id",),
    "honolulu": ("urban honolulu|hi",),
    "nashville": ("nashville davidson metropolitan government|tn",),
    "lexington": ("lexington fayette urban county|ky",),
    "augusta": ("augusta richmond county consolidated government|ga",),
    "macon": ("macon bibb county|ga",),
    "athens": ("athens clarke county unified government|ga",),
    "butte": ("butte silver bow|mt",),
    "louisville": ("louisville jefferson county metro government|ky",),
}

_FUZZY_CUTOFF = 0.75


@dataclass
class LocationResolution:
    """One resolved (or honestly-unresolved) location candidate."""

    input: str
    status: str = "unresolved"  # resolved | corrected | ambiguous | unresolved
    kind: str = "unknown"  # zip | city | county | state | country | nationwide | remote | unknown
    display_name: str = ""
    city: str = ""
    county_name: str = ""
    county_fips: str = ""
    state_usps: str = ""
    state_name: str = ""
    zip5: str = ""
    lat: float | None = None
    lng: float | None = None
    confidence: float = 0.0
    matched_via: str = ""
    note: str = ""
    alternatives: list[dict[str, str]] = field(default_factory=list)
    county_count: int = 0
    other_counties: list[dict[str, str]] = field(default_factory=list)
    dma_code: str | None = None
    dma_name: str | None = None
    dma_source: str | None = None
    dma_status: str = "unavailable"
    cbsa_code: str | None = None
    cbsa_title: str | None = None
    cbsa_status: str = "unavailable"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Lazy-loaded, lock-guarded reference data (server is multithreaded -- never
# re-read the TSVs per call).
# ---------------------------------------------------------------------------
_load_lock = threading.Lock()
_loaded = False

_states_by_name: dict[str, str] = {}
_states_by_usps: dict[str, str] = {}
_state_usps_set: frozenset[str] = frozenset()

_counties_by_fips: dict[str, dict[str, str]] = {}
_counties_by_name_state: dict[tuple[str, str], str] = {}  # (county_name_norm, state_usps) -> fips

_places_by_key: dict[str, dict[str, str]] = {}
_places_by_city: dict[str, list[tuple[str, str]]] = {}  # city_norm -> [(state_usps, place_key), ...]
_city_norms_by_first_letter: dict[str, list[str]] = {}

_zips_by_zip5: dict[str, dict[str, str]] = {}
_zips_by_prefix: dict[str, list[int]] = {}

_dma_by_fips: dict[str, tuple[str, str, str]] = {}  # fips -> (dma_code, dma_name, dma_source)
_dma_available = False

_cbsa_by_fips: dict[str, tuple[str, str, str]] = {}  # fips -> (cbsa_code, cbsa_title, area_type)
_cbsa_available = False


def _norm_key(s: str) -> str:
    """Lowercase, fold accents to their ASCII base letter, strip remaining
    punctuation, collapse whitespace -- matches the `place_key` convention
    documented in data/geo/README.md.

    Accent-folding (NFD decompose + drop combining marks) runs BEFORE the
    non-alnum strip below and is a no-op for ASCII input (nothing to
    decompose or drop), so ASCII-only keys are byte-identical to before.
    Without it, an accented letter like "u" + COMBINING DIAERESIS ("ü") is
    just an unrecognized symbol to the strip step and gets deleted outright
    -- "Mayagüez" normalized to "mayagez" while plain-ASCII "Mayaguez"
    normalized to "mayaguez", so the two spellings of the same place never
    matched each other (regression: tests/test_plan_location.py accent-fold
    tests; found live via "Mayagüez, PR" resolving under neither spelling)."""
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = _NON_ALNUM_SPACE.sub("", s)
    s = _SPLIT_WS.sub(" ", s).strip()
    return s


def _strip_trailing_us_tail(stripped: str) -> str:
    """Rule 3: "City, US"/"USA"/"United States" -> return just the city
    part so every downstream rule treats it as a bare, unspecified-state
    city (rule 5)."""
    if "," not in stripped:
        return stripped
    parts = [p.strip() for p in stripped.split(",") if p.strip()]
    if len(parts) == 2 and _norm_key(parts[1]) in _TAIL_US_TOKENS:
        return parts[0]
    return stripped


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _ensure_loaded() -> None:
    global _loaded, _dma_available, _cbsa_available
    if _loaded:
        return
    with _load_lock:
        if _loaded:  # double-checked locking
            return
        try:
            _load_states()
            _load_counties()
            _load_places()
            _load_zips()
            _load_dma()
            _load_cbsa()
        except Exception:
            logger.error("plan_location: failed to load geo reference data", exc_info=True)
            # Leave whatever partially loaded rather than crash the caller;
            # resolve_location() below tolerates empty indexes gracefully.
        _loaded = True


def _load_states() -> None:
    for row in _read_tsv(_GEO_DIR / "us_states.tsv"):
        usps = (row.get("state_usps") or "").strip().upper()
        name = (row.get("state_name") or "").strip()
        if not usps or not name:
            continue
        _states_by_usps[usps] = name
        _states_by_name[_norm_key(name)] = usps
    global _state_usps_set
    _state_usps_set = frozenset(_states_by_usps.keys())


def _load_counties() -> None:
    for row in _read_tsv(_GEO_DIR / "us_counties.tsv"):
        fips = (row.get("county_fips") or "").strip()
        if not fips:
            continue
        name = (row.get("county_name") or "").strip()
        usps = (row.get("state_usps") or "").strip().upper()
        _counties_by_fips[fips] = {
            "county_name": name,
            "state_usps": usps,
            "lat": row.get("lat") or "",
            "lng": row.get("lng") or "",
        }
        _counties_by_name_state[(_norm_key(name), usps)] = fips


def _load_places() -> None:
    for row in _read_tsv(_GEO_DIR / "us_places.tsv"):
        key = (row.get("place_key") or "").strip()
        if not key:
            continue
        _places_by_key[key] = row
        usps = (row.get("state_usps") or "").strip().upper()
        city_norm = _norm_key(row.get("display_name") or "")
        if not city_norm:
            continue
        _places_by_city.setdefault(city_norm, []).append((usps, key))
    for city_norm in _places_by_city:
        first = city_norm[0] if city_norm else ""
        _city_norms_by_first_letter.setdefault(first, []).append(city_norm)


def _load_zips() -> None:
    # _read_tsv (csv.DictReader) captures every column in us_zips.tsv into
    # `row`, including the county_count/all_county_fips columns build_zips()
    # added -- no extra plumbing needed here to carry them through; they're
    # read back out in _resolve_zip() below via record.get(...).
    for row in _read_tsv(_GEO_DIR / "us_zips.tsv"):
        zip5 = (row.get("zip5") or "").strip()
        if not zip5:
            continue
        _zips_by_zip5[zip5] = row
        prefix = zip5[:3]
        try:
            _zips_by_prefix.setdefault(prefix, []).append(int(zip5))
        except ValueError:
            continue
    for prefix in _zips_by_prefix:
        _zips_by_prefix[prefix].sort()


def _load_dma() -> None:
    global _dma_available
    dma_path = _GEO_DIR / "dma_by_county.tsv"
    if not dma_path.exists():
        _dma_available = False
        return
    try:
        for row in _read_tsv(dma_path):
            fips = (row.get("county_fips") or "").strip()
            if not fips:
                continue
            code = (row.get("dma_code") or "").strip()
            name = (row.get("dma_name") or "").strip()
            source = (row.get("dma_source") or "derived, not a licensed commercial market-area feed").strip()
            _dma_by_fips[fips] = (code, name, source)
        _dma_available = True
    except Exception:
        logger.error("plan_location: failed to load optional dma_by_county.tsv", exc_info=True)
        _dma_available = False


def _apply_dma(res: LocationResolution) -> None:
    if not _dma_available:
        res.dma_status = "unavailable"
        return
    if not res.county_fips:
        res.dma_status = "unavailable"
        return
    hit = _dma_by_fips.get(res.county_fips)
    if not hit:
        res.dma_status = "unavailable"
        return
    code, name, source = hit
    res.dma_code = code or None
    res.dma_name = name or None
    res.dma_source = source or None
    res.dma_status = "available"


def _load_cbsa() -> None:
    global _cbsa_available
    cbsa_path = _GEO_DIR / "cbsa_by_county.tsv"
    if not cbsa_path.exists():
        _cbsa_available = False
        return
    try:
        for row in _read_tsv(cbsa_path):
            fips = (row.get("county_fips") or "").strip()
            if not fips:
                continue
            code = (row.get("cbsa_code") or "").strip()
            title = (row.get("cbsa_title") or "").strip()
            area_type = (row.get("area_type") or "").strip()
            _cbsa_by_fips[fips] = (code, title, area_type)
        _cbsa_available = True
    except Exception:
        logger.error("plan_location: failed to load optional cbsa_by_county.tsv", exc_info=True)
        _cbsa_available = False


def _apply_cbsa(res: LocationResolution) -> None:
    if not _cbsa_available:
        res.cbsa_status = "unavailable"
        return
    if not res.county_fips:
        res.cbsa_status = "unavailable"
        return
    hit = _cbsa_by_fips.get(res.county_fips)
    if not hit:
        # A real, non-fabricated outcome for counties genuinely outside any
        # CBSA (rural/unincorporated areas) -- not an error.
        res.cbsa_status = "unavailable"
        return
    code, title, _area_type = hit
    res.cbsa_code = code or None
    res.cbsa_title = title or None
    res.cbsa_status = "available"


# ---------------------------------------------------------------------------
# Place-record helpers
# ---------------------------------------------------------------------------
def _fill_from_place(res: LocationResolution, place: dict[str, str]) -> None:
    res.display_name = place.get("display_name") or ""
    res.city = res.display_name
    res.state_usps = (place.get("state_usps") or "").upper()
    res.state_name = _states_by_usps.get(res.state_usps, "")
    res.county_fips = place.get("county_fips") or ""
    county = _counties_by_fips.get(res.county_fips)
    res.county_name = county.get("county_name", "") if county else ""
    res.lat = _to_float(place.get("lat"))
    res.lng = _to_float(place.get("lng"))
    res.kind = "city"


def _to_float(v: str | None) -> float | None:
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _alt_from_place(place: dict[str, str]) -> dict[str, str]:
    return {
        "display_name": place.get("display_name") or "",
        "state_usps": (place.get("state_usps") or "").upper(),
    }


def _pick_ambiguous_primary(city_norm: str, candidates: list[tuple[str, str]]) -> tuple[str, str]:
    """Deterministic tiebreak among same-name candidates in different
    states. See module docstring for the curated-metro-then-alphabetical
    policy. `candidates` is [(state_usps, place_key), ...]."""
    preferred_state = _WELL_KNOWN_METRO_STATE.get(city_norm)
    if preferred_state:
        for usps, key in candidates:
            if usps == preferred_state:
                return usps, key
    return sorted(candidates, key=lambda t: (t[0], t[1]))[0]


def _saint_variants(city_norm: str) -> list[str]:
    """"Saint Louis" and "St. Louis" are the same place to a user but not to
    a string index -- Census stores "St. Louis", which normalizes to
    "st louis"."""
    out = []
    if city_norm.startswith("saint "):
        out.append("st " + city_norm[6:])
    elif city_norm.startswith("st "):
        out.append("saint " + city_norm[3:])
    return out


def _pr_municipio_variants(city_norm: str) -> list[str]:
    """A bare Puerto Rico municipio name ("Mayagüez", "Carolina", "San
    Juan") is not itself a Census place record -- PR has no incorporated
    "cities" in the mainland sense. Census instead publishes one CDP per
    municipio for its urban core, always named "<municipio> zona urbana".
    Verified exhaustively against data/geo/*.tsv (not just spot-checked):
    all 78 PR municipios have exactly one such record, county_fips-matched
    to that municipio, with the base name identical to the municipio name
    once "zona urbana" / "Municipio" are stripped -- see
    tests/test_plan_location.py::test_every_pr_municipio_has_exactly_one_zona_urbana_place.
    Same shape as _saint_variants: a systematic suffix transformation, not
    a per-name alias table -- a plain dict miss for every non-PR name."""
    return [city_norm + " zona urbana"]


def _city_candidates(city_norm: str) -> list[tuple[str, str]]:
    """All (state, place_key) candidates for a normalized city name, folding
    in Saint/St. spellings, Puerto Rico's municipio/"zona urbana" naming,
    and the Census legal-name aliases. Runs before any fuzzy matching so a
    real metro is never "corrected" into a different city."""
    matches = list(_places_by_city.get(city_norm, []))
    for variant in _saint_variants(city_norm):
        for cand in _places_by_city.get(variant, []):
            if cand not in matches:
                matches.append(cand)
    for variant in _pr_municipio_variants(city_norm):
        for cand in _places_by_city.get(variant, []):
            if cand not in matches:
                matches.append(cand)
    for key in _CENSUS_NAME_ALIASES.get(city_norm, ()):
        place = _places_by_key.get(key)
        if not place:
            continue
        cand = ((place.get("state_usps") or "").upper(), key)
        if cand not in matches:
            matches.append(cand)
    return matches


def _resolve_bare_city(raw_input: str, city_text: str) -> LocationResolution:
    city_norm = _norm_key(city_text)
    matches = _city_candidates(city_norm)
    if not matches:
        return LocationResolution(input=raw_input, status="unresolved", kind="unknown")

    if len(matches) == 1:
        usps, key = matches[0]
        place = _places_by_key[key]
        res = LocationResolution(input=raw_input, status="resolved", confidence=0.95, matched_via="place_city_unique")
        _fill_from_place(res, place)
        res.note = f"resolved '{city_text.strip()}' as the only US place named '{res.display_name}' ({res.state_usps})."
        _apply_dma(res)
        _apply_cbsa(res)
        return res

    primary_usps, primary_key = _pick_ambiguous_primary(city_norm, matches)
    primary_place = _places_by_key[primary_key]
    res = LocationResolution(input=raw_input, status="ambiguous", confidence=0.5, matched_via="place_city_ambiguous")
    _fill_from_place(res, primary_place)
    res.alternatives = [_alt_from_place(_places_by_key[key]) for _, key in sorted(matches, key=lambda t: (t[0], t[1]))]
    res.note = (
        f"“{city_text.strip()}” matches {len(matches)} US cities. "
        "Pick the one you mean so the plan targets the right market."
    )
    _apply_dma(res)
    _apply_cbsa(res)
    return res


def _fuzzy_city_lookup(city_text: str, state_usps: str = "") -> list[str]:
    """Return up to 3 candidate city_norm strings close to `city_text`,
    optionally scoped to one state. Pre-filters by first letter + length
    for speed -- see module docstring for why this is safe for the 32
    legacy misspellings it must cover."""
    q = _norm_key(city_text)
    if not q:
        return []
    if state_usps:
        pool = [c for c in _places_by_city if any(u == state_usps for u, _ in _places_by_city[c])]
    else:
        pool = _city_norms_by_first_letter.get(q[0], [])
    filtered = [c for c in pool if abs(len(c) - len(q)) <= 2]
    return difflib.get_close_matches(q, filtered, n=3, cutoff=_FUZZY_CUTOFF)


# ---------------------------------------------------------------------------
# Main resolution
# ---------------------------------------------------------------------------
def resolve_location(raw: str) -> LocationResolution:
    """Resolve one free-text location string. Never raises -- any internal
    failure degrades to an "unresolved" result with a logged error."""
    try:
        return _resolve_location_inner(raw)
    except Exception:
        logger.error("plan_location.resolve_location failed for %r", raw, exc_info=True)
        safe_input = raw if isinstance(raw, str) else ("" if raw is None else str(raw))
        return LocationResolution(
            input=safe_input,
            status="unresolved",
            kind="unknown",
            note="internal error while resolving this location; treated as unresolved.",
        )


def _resolve_location_inner(raw: Any) -> LocationResolution:
    _ensure_loaded()

    if raw is None:
        return LocationResolution(input="", status="unresolved", kind="unknown", note="empty location.")
    raw_str = raw if isinstance(raw, str) else str(raw)
    stripped = raw_str.strip()
    if not stripped:
        return LocationResolution(input=raw_str, status="unresolved", kind="unknown", note="empty location.")

    # Rule 1: ZIP / ZIP+4.
    zip_match = _ZIP_RE.match(stripped)
    if zip_match:
        return _resolve_zip(raw_str, zip_match.group(1))

    # Rule 3: "City, US" / "City, USA" / "City, United States" -> strip the
    # US tail up front so every downstream rule (2/5/9/8/10) operates on
    # just the city part, not the literal "City, US" string.
    stripped = _strip_trailing_us_tail(stripped)

    # Rule 2: "City, ST" / "City, State Name" / comma-less "City ST".
    comma_result = _try_comma_or_trailing_state(raw_str, stripped)
    if comma_result is not None:
        return comma_result

    # Rule 4: bare state name or USPS abbreviation.
    norm_whole = _norm_key(stripped)
    state_res = _try_bare_state(raw_str, stripped, norm_whole)
    if state_res is not None:
        return state_res

    # Rule 6/7: country / nationwide / remote keywords (checked before the
    # bare-city fallback so short keyword tokens never get treated as an
    # attempted-but-failed city lookup).
    keyword_res = _try_keywords(raw_str, norm_whole)
    if keyword_res is not None:
        return keyword_res

    # Rule 5: bare city, no state.
    bare = _resolve_bare_city(raw_str, stripped)
    if bare.status in ("resolved", "ambiguous"):
        return bare

    # Rule 9 (checked before rule 8 -- see module docstring "Known
    # limitation" section): fuzzy correction against real US place names.
    # A confident fuzzy hit against real US places is stronger evidence of
    # a US-city typo than plan_geo's non-US classifier's substring
    # heuristic is evidence of an actual foreign country.
    fuzzy_res = _try_fuzzy(raw_str, stripped)
    if fuzzy_res is not None:
        return fuzzy_res

    # Rule 8: non-US, delegated to plan_geo's shared classifier.
    try:
        us_signal = plan_geo._resolve_candidate(stripped)  # noqa: SLF001 - deliberate reuse, see docstring
    except Exception:
        us_signal = None
    if us_signal is False:
        return LocationResolution(
            input=raw_str,
            status="unresolved",
            kind="unknown",
            matched_via="non_us_signal",
            note=(
                f"“{stripped}” is outside the US. It stays in your plan exactly as "
                "entered — Nova can only confirm US locations today."
            ),
        )

    # Rule 10: nothing matched.
    return LocationResolution(
        input=raw_str,
        status="unresolved",
        kind="unknown",
        note=(
            f"Nova couldn’t match “{stripped}” to a US ZIP, city, county, or state. "
            "It stays in your plan exactly as entered."
        ),
    )


def _resolve_zip(raw_str: str, zip5: str) -> LocationResolution:
    record = _zips_by_zip5.get(zip5)
    if record:
        res = LocationResolution(input=raw_str, status="resolved", kind="zip", confidence=1.0, matched_via="zip_exact")
        res.zip5 = zip5
        res.display_name = record.get("primary_city") or ""
        res.city = res.display_name
        res.state_usps = (record.get("state_usps") or "").upper()
        res.state_name = _states_by_usps.get(res.state_usps, "")
        res.county_fips = record.get("county_fips") or ""
        county = _counties_by_fips.get(res.county_fips)
        res.county_name = county.get("county_name", "") if county else ""
        res.lat = _to_float(record.get("lat"))
        res.lng = _to_float(record.get("lng"))

        # Additive multi-county disclosure -- see data/geo/README.md and
        # scripts/build_geo_data.py::parse_zcta_county(). ~30% of ZCTAs
        # genuinely span more than one county; the ZIP itself is still
        # resolved with certainty (status/confidence untouched), only the
        # county attribution carries uncertainty, so it gets its own
        # signal instead of muddying fields downstream code branches on.
        try:
            res.county_count = int(record.get("county_count") or "1")
        except ValueError:
            res.county_count = 1
        all_fips = [f for f in (record.get("all_county_fips") or "").split("|") if f]
        if res.county_count > 1 and len(all_fips) > 1:
            other_fips = all_fips[1:]
            other_counties = []
            for fips in other_fips:
                other_county = _counties_by_fips.get(fips)
                other_counties.append(
                    {
                        "county_name": other_county.get("county_name", "") if other_county else "",
                        "county_fips": fips,
                        "state_usps": other_county.get("state_usps", "") if other_county else "",
                    }
                )
            res.other_counties = other_counties
            # county_name already carries its own legal-type suffix
            # (Census NAME field, e.g. "Stone County", "Acadia Parish",
            # "Aleutians East Borough") -- do not append "County" again.
            dominant_label = (
                f"{res.county_name}, {res.state_usps}" if res.county_name else "the largest county"
            )
            other_names = [
                f"{c['county_name']}, {c['state_usps']}" if c["county_name"] else c["county_fips"]
                for c in other_counties
            ]
            # "county" is inaccurate client-facing copy for the 341 (of
            # 10,174) multi-county ZIPs whose named units aren't all
            # Census counties proper -- Louisiana Parishes, Puerto Rico
            # Municipios, Virginia independent cities, Alaska
            # Boroughs/Census Areas all live in county_fips/county_name too
            # (e.g. ZIP 20110: "Manassas city, VA" + "Prince William
            # County, VA"). Fall back to the Census term of art whenever any
            # named unit isn't a "* County".
            unit_names = [res.county_name] + [c["county_name"] for c in other_counties]
            unit_word = "counties" if all(n.endswith("County") for n in unit_names) else "counties or equivalents"
            res.note = (
                f"ZIP {zip5} spans {res.county_count} {unit_word}. Nova is using "
                f"{dominant_label} — it covers the largest share of the ZIP’s "
                f"land area. Also overlapping: {'; '.join(other_names)}."
            )
        _apply_dma(res)
        _apply_cbsa(res)
        return res

    # Well-formed ZIP absent from the ZCTA-derived dataset (see
    # data/geo/README.md judgment call 5 -- PO-Box-only USPS zips have no
    # ZCTA). Graceful, honest, and helpful: suggest the nearest resolvable
    # ZIP sharing the same 3-digit prefix, if any.
    res = LocationResolution(input=raw_str, status="unresolved", kind="zip", zip5=zip5, matched_via="zip_absent_from_zcta")
    prefix = zip5[:3]
    same_prefix = _zips_by_prefix.get(prefix, [])
    if same_prefix:
        try:
            target = int(zip5)
        except ValueError:
            target = None
        nearest = min(same_prefix, key=lambda z: abs(z - target)) if target is not None else same_prefix[0]
        nearest5 = f"{nearest:05d}"
        nearby = _zips_by_zip5.get(nearest5)
        if nearby:
            res.alternatives = [
                {
                    "zip5": nearest5,
                    "display_name": nearby.get("primary_city") or "",
                    "state_usps": (nearby.get("state_usps") or "").upper(),
                }
            ]
            # Matches the suggestion button's label exactly -- the two sit
            # three lines apart and differing precision reads as sloppiness.
            city_label = (
                f"{nearby.get('primary_city') or ''}, "
                f"{(nearby.get('state_usps') or '').upper()}"
            )
            res.note = (
                f"ZIP {zip5} has no mapped delivery area, so Nova can’t confirm it. "
                "It stays in your plan exactly as entered. "
                f"{nearest5} ({city_label}) is a nearby ZIP in the same postal area "
                "that Nova can confirm."
            )
            return res
    res.note = (
        f"ZIP {zip5} has no mapped delivery area, so Nova can’t confirm it. "
        "It stays in your plan exactly as entered."
    )
    return res


def _try_comma_or_trailing_state(raw_str: str, stripped: str) -> LocationResolution | None:
    city_text = ""
    tail_text = ""
    if "," in stripped:
        parts = [p.strip() for p in stripped.split(",") if p.strip()]
        if len(parts) >= 2:
            city_text = parts[0]
            tail_text = ",".join(parts[1:]).strip()
    else:
        tokens = _SPLIT_WS.split(stripped)
        if len(tokens) >= 2 and len(tokens[-1]) == 2 and tokens[-1].isalpha():
            city_text = " ".join(tokens[:-1])
            tail_text = tokens[-1]

    if not city_text or not tail_text:
        return None

    tail_norm = _norm_key(tail_text)
    if tail_norm in _TAIL_US_TOKENS:
        # Rule 3 is handled by the caller stripping the ", US"/"USA"/"United
        # States" tail before this function ever runs -- see
        # _resolve_location_inner. Reaching here with a US tail means the
        # caller passed the raw string through unchanged; treat as "no
        # recognizable state tail" so downstream rules (5, 9, 8, 10) can
        # still process the city part on their own terms.
        return None

    state_usps = ""
    if tail_text.strip().upper() in _state_usps_set:
        state_usps = tail_text.strip().upper()
    elif tail_norm in _states_by_name:
        state_usps = _states_by_name[tail_norm]

    if not state_usps:
        return None  # not a recognizable state tail; let downstream rules judge (e.g. "City, Country")

    city_norm = _norm_key(city_text)

    # Try county pattern first if the left side reads like a county name.
    if any(city_norm.endswith(suf.replace(" ", "")) or city_text.lower().strip().endswith(suf) for suf in _COUNTY_SUFFIXES):
        fips = _counties_by_name_state.get((city_norm, state_usps))
        if fips:
            county = _counties_by_fips[fips]
            res = LocationResolution(
                input=raw_str, status="resolved", kind="county", confidence=1.0, matched_via="county_state_exact"
            )
            res.county_fips = fips
            res.county_name = county["county_name"]
            res.state_usps = state_usps
            res.state_name = _states_by_usps.get(state_usps, "")
            res.display_name = f"{county['county_name']}, {state_usps}"
            res.lat = _to_float(county.get("lat"))
            res.lng = _to_float(county.get("lng"))
            _apply_dma(res)
            _apply_cbsa(res)
            return res

    place_key = f"{city_norm}|{state_usps.lower()}"
    place = _places_by_key.get(place_key)
    if not place:
        # Same Saint/St. + Census-legal-name folding as the bare-city path,
        # so "Boise, ID" and "Saint Louis, MO" resolve exactly like "Boise"
        # and "St. Louis" instead of falling through to fuzzy.
        for cand_state, cand_key in _city_candidates(city_norm):
            if cand_state == state_usps:
                place_key = cand_key
                place = _places_by_key.get(cand_key)
                break
    if place:
        res = LocationResolution(input=raw_str, status="resolved", confidence=1.0, matched_via="place_city_state")
        _fill_from_place(res, place)
        _apply_dma(res)
        _apply_cbsa(res)
        return res

    # Exact match missed -- try fuzzy, scoped to this state, before giving up
    # (rule 9, state-scoped variant).
    fuzzy_matches = _fuzzy_city_lookup(city_text, state_usps=state_usps)
    for candidate_norm in fuzzy_matches:
        candidate_key = f"{candidate_norm}|{state_usps.lower()}"
        candidate_place = _places_by_key.get(candidate_key)
        if candidate_place:
            res = LocationResolution(
                input=raw_str, status="corrected", confidence=0.8, matched_via="fuzzy_city_state"
            )
            _fill_from_place(res, candidate_place)
            res.note = (
                f"Read “{city_text.strip()}, {tail_text.strip()}” as "
                f"{res.display_name}, {res.state_usps}."
            )
            _apply_dma(res)
            _apply_cbsa(res)
            return res

    # Recognized a real state but not the city in it: honest unresolved,
    # not a silent fall-through to unrelated rules.
    return LocationResolution(
        input=raw_str,
        status="unresolved",
        kind="unknown",
        state_usps=state_usps,
        state_name=_states_by_usps.get(state_usps, ""),
        matched_via="city_state_no_match",
        note=f"'{city_text.strip()}' was not found in {_states_by_usps.get(state_usps, state_usps)} ({state_usps}).",
    )


def _try_bare_state(raw_str: str, stripped: str, norm_whole: str) -> LocationResolution | None:
    usps = ""
    if stripped.strip().upper() in _state_usps_set and len(stripped.strip()) == 2:
        usps = stripped.strip().upper()
    elif norm_whole in _states_by_name:
        usps = _states_by_name[norm_whole]
    if not usps:
        return None
    res = LocationResolution(input=raw_str, status="resolved", kind="state", confidence=1.0, matched_via="state_bare")
    res.state_usps = usps
    res.state_name = _states_by_usps.get(usps, "")
    res.display_name = res.state_name
    _apply_dma(res)
    _apply_cbsa(res)
    return res


def _try_keywords(raw_str: str, norm_whole: str) -> LocationResolution | None:
    if norm_whole in _REMOTE_TOKENS:
        res = LocationResolution(input=raw_str, status="resolved", kind="remote", confidence=1.0, matched_via="remote_keyword")
        res.display_name = "Remote"
        res.note = "interpreted as remote/work-from-home targeting, not a physical location."
        return res
    if norm_whole in _NATIONWIDE_TOKENS:
        res = LocationResolution(input=raw_str, status="resolved", kind="nationwide", confidence=1.0, matched_via="nationwide_keyword")
        res.display_name = "Nationwide (US)"
        return res
    if norm_whole in _COUNTRY_TOKENS:
        res = LocationResolution(input=raw_str, status="resolved", kind="country", confidence=1.0, matched_via="country_keyword")
        res.display_name = "United States"
        res.state_usps = ""
        return res
    return None


def _try_fuzzy(raw_str: str, stripped: str) -> LocationResolution | None:
    matches = _fuzzy_city_lookup(stripped)
    if not matches:
        return None
    best_norm = matches[0]
    candidates = _places_by_city.get(best_norm, [])
    if not candidates:
        return None
    if len(candidates) == 1:
        usps, key = candidates[0]
        place = _places_by_key[key]
        res = LocationResolution(input=raw_str, status="corrected", confidence=0.75, matched_via="fuzzy_city_unique")
        _fill_from_place(res, place)
        res.note = f"Read “{stripped}” as {res.display_name}, {res.state_usps}."
        _apply_dma(res)
        _apply_cbsa(res)
        return res
    primary_usps, primary_key = _pick_ambiguous_primary(best_norm, candidates)
    primary_place = _places_by_key[primary_key]
    res = LocationResolution(input=raw_str, status="corrected", confidence=0.55, matched_via="fuzzy_city_ambiguous")
    _fill_from_place(res, primary_place)
    res.alternatives = [_alt_from_place(_places_by_key[key]) for _, key in sorted(candidates, key=lambda t: (t[0], t[1]))]
    res.note = (
        f"Read “{stripped}” as {res.display_name}, which exists in several states. "
        f"Showing {res.state_usps} — pick another below if that isn’t the one."
    )
    _apply_dma(res)
    _apply_cbsa(res)
    return res


def resolve_locations(raw_list: list[str]) -> list[LocationResolution]:
    """Resolve a list of free-text locations, preserving order. Never
    raises -- a bad entry produces an unresolved result, it does not abort
    the batch."""
    if not raw_list:
        return []
    out: list[LocationResolution] = []
    for raw in raw_list:
        out.append(resolve_location(raw))
    return out
