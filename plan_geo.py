"""Single source of truth for "is this plan US-only?" geo resolution.

Backlog context: two real production briefs misfired because the old
``_is_us_only_campaign`` in ppt_generator.py (a) hard-returned ``False`` on
the FIRST unresolvable location candidate instead of continuing to check the
rest, and (b) had no concept of a bare US state name ("Massachusetts") or a
comma-less "City ST" pair ("Denver CO"). This module fixes both: it always
scans every candidate before deciding, and it adds a dedicated US state-name
table so common New-England-style location lists resolve correctly without
ever touching the currency table.

Usage:
    from plan_geo import is_us_plan, non_us_signals
    is_us_plan({"locations": ["Massachusetts", "Denver, CO"]})   # -> True
    is_us_plan({"locations": ["London, UK"]})                     # -> False
    non_us_signals({"locations": ["New York, NY", "London, UK"]}) # -> ["London, UK"]
"""

from __future__ import annotations

import re
from typing import Any

import plan_currency

# ---------------------------------------------------------------------------
# US state name / abbreviation table
# ---------------------------------------------------------------------------
# Bare state names ("Massachusetts", "Maine") and the standard 2-letter postal
# abbreviations. Resolving these does NOT require a currency lookup -- a state
# name or trailing state abbreviation is an unambiguous US signal on its own.
US_STATE_NAME_TO_ABBR: dict[str, str] = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
    "puerto rico": "PR",
}

US_STATE_ABBR: frozenset[str] = frozenset(US_STATE_NAME_TO_ABBR.values())

# ---------------------------------------------------------------------------
# International token / phrase fallback (ported from
# ppt_generator._is_us_only_campaign, lines ~2273-2455). Used only as a last
# resort when plan_currency and the state table both come up empty.
# ---------------------------------------------------------------------------
_INTL_TOKENS: frozenset[str] = frozenset(
    {
        "uk",
        "london",
        "europe",
        "apac",
        "emea",
        "asia",
        "india",
        "germany",
        "france",
        "japan",
        "china",
        "australia",
        "canada",
        "brazil",
        "mexico",
        "singapore",
        "zealand",
        "auckland",
        "wellington",
        "christchurch",
        "nz",
        "nzl",
        "ireland",
        "dublin",
        "netherlands",
        "amsterdam",
        "spain",
        "madrid",
        "italy",
        "sweden",
        "poland",
        "philippines",
        "manila",
        "malaysia",
        "indonesia",
        "vietnam",
        "thailand",
        "nigeria",
        "kenya",
        "uae",
        "dubai",
    }
)

_INTL_PHRASES: tuple[str, ...] = (
    "united kingdom",
    "hong kong",
    "new zealand",
    "south africa",
    "saudi",
)

_SPLIT_RE = re.compile(r"[,\s]+")


def _extract_str(raw: Any) -> str:
    """Turn a location candidate (str or dict) into a single lookup string.

    Dict entries are checked in order: country, location, then city+state
    (combined "City, ST") or whichever of city/state is present alone.
    """
    if isinstance(raw, dict):
        country = raw.get("country")
        if isinstance(country, str) and country.strip():
            return country.strip()
        location = raw.get("location")
        if isinstance(location, str) and location.strip():
            return location.strip()
        city = raw.get("city")
        state = raw.get("state")
        city = city.strip() if isinstance(city, str) else ""
        state = state.strip() if isinstance(state, str) else ""
        if city and state:
            return f"{city}, {state}"
        if city:
            return city
        if state:
            return state
        return ""
    if isinstance(raw, str):
        return raw.strip()
    return ""


def _us_state_signal(loc_str: str) -> bool | None:
    """True if ``loc_str`` is unambiguously a US state (bare name or a
    trailing 2-letter postal abbreviation), without any currency lookup."""
    s = loc_str.strip()
    if not s:
        return None
    low = s.lower()
    if low in US_STATE_NAME_TO_ABBR:
        return True
    tokens = [t for t in _SPLIT_RE.split(s) if t]
    if tokens:
        tail = tokens[-1]
        if len(tail) == 2 and tail.isalpha() and tail.upper() in US_STATE_ABBR:
            return True
    return None


def _resolve_candidate(loc_str: str) -> bool | None:
    """Resolve a single location string to True (US), False (non-US), or
    None (unresolvable -- caller must NOT treat this as a hard False)."""
    if not loc_str:
        return None

    if _us_state_signal(loc_str) is True:
        return True

    # Pass the FULL string to currency_for_country -- it has its own
    # "City, ST" state-abbreviation handling that pre-splitting would defeat.
    try:
        code = plan_currency.currency_for_country(loc_str)
    except Exception:  # noqa: BLE001 - resolution is best-effort
        code = None
    if code:
        return code == "USD"

    loc_lower = loc_str.lower()
    parts = {p for p in _SPLIT_RE.split(loc_lower) if p}
    if parts & _INTL_TOKENS or any(phrase in loc_lower for phrase in _INTL_PHRASES):
        return False

    return None


def _gather_candidates(data: dict) -> list[Any]:
    candidates: list[Any] = []
    country_field = data.get("country")
    if isinstance(country_field, str) and country_field.strip():
        candidates.append(country_field)
    locations = data.get("locations") or []
    if isinstance(locations, (list, tuple)):
        candidates.extend(locations)
    elif locations:
        candidates.append(locations)
    return candidates


def _evaluate(data: dict) -> tuple[bool, list[str]]:
    target_region = str(data.get("target_region") or "").lower().strip()
    if target_region == "us_only":
        return True, []
    if target_region in ("global", "emea", "apac", "custom"):
        return False, []

    candidates = _gather_candidates(data)
    if not candidates:
        return True, []

    signals: list[str] = []
    for raw in candidates:
        loc_str = _extract_str(raw)
        if not loc_str:
            continue
        result = _resolve_candidate(loc_str)
        # Unresolvable candidate => continue (never hard-return False on the
        # first miss -- that exact bug shipped).
        if result is False:
            signals.append(loc_str)

    return (len(signals) == 0), signals


def is_us_plan(data: dict) -> bool:
    """True if every location signal on ``data`` resolves to the US.

    Respects ``data['target_region']`` first ('us_only' -> True;
    'global'/'emea'/'apac'/'custom' -> False). Otherwise scans the ``country``
    field plus every entry in ``locations`` (str or dict). No candidates at
    all -> True (assume domestic).
    """
    is_us, _ = _evaluate(data)
    return is_us


def non_us_signals(data: dict) -> list[str]:
    """Return the location strings that drove ``is_us_plan`` to False, for
    honest workbook/deck messaging ("these locations pulled this plan
    international: ..."). Empty list when the plan is US-only."""
    _, signals = _evaluate(data)
    return signals
