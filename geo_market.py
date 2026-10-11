"""Resolve a plan location ONCE into the geographic level its figures describe.

Background (RTI / Hershey output audit, 2026-10-11). The same location string
was interpreted independently by research.py, gold_standard.py,
data_synthesizer.py, excel_v2.py and ppt_generator.py, and they disagreed:

* "Washington" was Washington State in one place, the DC metro in another, and
  a 1.25x salary multiplier in a third -- next to a separate "DC" row.
* "Jackson" was answered with Jacksonville's data (substring matching).
* A state ("NY", "Maryland", "PA") was answered with ONE metro's figures
  (Baltimore, NYC, Philadelphia...) in one path and a national default in the
  other.
* "Remote" / "Nationwide" printed a national placeholder as if it were a
  market, and were counted as "markets" on the deck.
* ``plan_location``'s verdict (``data["_location_resolution"]``: kind,
  ambiguity, CBSA title) was computed at the request boundary and never read.

``resolve_market`` is the single function that turns (location string,
optional plan_location result) into a ``Market``. Every consumer reads the
same object, so one location can never mean two places.

Decisions (documented, not silent):

* A bare ambiguous city resolves to the market this module's data actually has
  (the METRO_DATA metro of that name, or ``_AMBIGUOUS_BARE_DEFAULT``) and says
  so: ``Market.assumed`` is "Jacksonville, FL" and ``ambiguity_note`` explains.
* Bare "Washington" is assumed to be Washington, DC (the pre-existing intent of
  the metro table and the 1.25x salary multiplier), flagged as ambiguous with
  Washington State. "WA" / "Washington State" stay the state.
* DC is a city-state with no separate statewide labour market: "DC" and
  "District of Columbia" resolve to the Washington metro entry (level metro),
  not to a statewide row.
* Every other state entry (``kind == "state"``) is answered with STATEWIDE
  figures labelled statewide -- never one metro's.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import plan_geo

# Normalised spellings that are not a place at all.
_REMOTE_TOKENS = frozenset(
    {
        "remote",
        "remotework",
        "remoteus",
        "remoteusa",
        "remoteonly",
        "fullyremote",
        "workfromhome",
        "wfh",
        "virtual",
    }
)
_NATIONWIDE_TOKENS = frozenset(
    {
        "nationwide",
        "nationwideus",
        "nationwideusa",
        "national",
        "nationalus",
        "unitedstates",
        "usa",
        "us",
        "allus",
        "allunitedstates",
        "uswide",
    }
)

# Bare, ambiguous city names whose largest / dominant US namesake is not what
# plan_location's tie-break returns, or has no METRO_DATA entry. name -> (metro
# key or "", state). Applied ONLY when plan_location flagged the name ambiguous.
_AMBIGUOUS_BARE_DEFAULT: dict[str, tuple[str, str]] = {
    "jackson": ("jackson_ms", "MS"),
    "cleveland": ("", "OH"),
}


@dataclass
class Market:
    """What a plan location is, and at which geographic level its figures sit."""

    location: str
    kind: str  # remote | nationwide | state | city | unknown
    level: str  # none | metro | state | generic
    state: str = ""  # USPS code ("" when unknown / not a place)
    metro_key: str = ""  # research.METRO_DATA key when level == "metro"
    assumed: str = ""  # "Jacksonville, FL" when a bare ambiguous name was resolved
    ambiguity_note: str = ""
    cbsa_title: str = ""  # plan_location's MSA title for this site, when known
    site_key: str = ""  # identity of the physical site (dedupe / counting)

    @property
    def is_physical(self) -> bool:
        return self.kind not in ("remote", "nationwide")


def _norm(text: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())


def _title_city(text: str) -> str:
    return " ".join(w.capitalize() for w in str(text or "").split())


def _bare_state_code(loc: str) -> str:
    """USPS code when ``loc`` IS a state ("PA", "pa", "Pennsylvania"), else ""."""
    token = loc.strip().rstrip(".")
    if not token:
        return ""
    if len(token) == 2 and token.isalpha() and token.upper() in plan_geo.US_STATE_ABBR:
        return token.upper()
    return plan_geo.US_STATE_NAME_TO_ABBR.get(token.lower(), "")


def _explicit_state(loc: str) -> str:
    """USPS code from a trailing ", ST" / ", State Name", else ""."""
    if "," not in loc:
        return ""
    tail = loc.rsplit(",", 1)[1].strip().rstrip(".")
    tail = re.sub(r"\s*\d+.*$", "", tail)
    return _bare_state_code(tail)


def _ambiguity_count(res: dict) -> str:
    alts = res.get("alternatives")
    if isinstance(alts, list) and len(alts) >= 2:
        return f"{len(alts)} US places share this name"
    return "several US places share this name"


# Bare names that are BOTH a state and a major city: the metro reading is
# assumed (as every location table has always done) and the other reading is
# named in the note. "WA" / "NY" / "Washington State" stay the state.
_BARE_STATE_CITY_DEFAULT: dict[str, tuple[str, str, str]] = {
    "washington": (
        "washington_dc",
        "Washington, DC",
        "\u201cWashington\u201d can mean Washington, DC or Washington State; "
        "Washington, DC assumed.",
    ),
    "newyork": (
        "new_york",
        "New York, NY (NYC metro)",
        "\u201cNew York\u201d can mean New York City or New York State; "
        "New York, NY (NYC metro) assumed, New York State also possible.",
    ),
}


def _bare_state_city_default(loc: str, res: dict) -> tuple[str, str, str] | None:
    """(metro key, assumed label, note) when ``loc`` is the literal bare name of
    a state that is also a major city -- not "WA", "NY" or "Washington State"."""
    default = _BARE_STATE_CITY_DEFAULT.get(_norm(loc))
    if default is None:
        return None
    original = res.get("input") if res else loc
    return default if _norm(original) == _norm(loc) else None


def _metro_from_cbsa(cbsa_title: str) -> tuple[str, dict]:
    """The curated METRO_DATA entry for the MSA plan_location names for a site.

    A secondary city named in an MSA title ("Fort Worth", "Oakland", "Mesa") has
    no METRO_DATA key of its own, but its MSA does. Match on the MSA's principal
    (first) city, which survives OMB's title revisions ("San Francisco-Oakland-
    Fremont" vs "...-Berkeley"), and require the curated metro's state to be one
    of the CBSA's states so "Columbus, GA" never borrows Columbus, OH.
    """
    import research

    names, _, states = cbsa_title.rpartition(", ")
    if not names:
        return "", {}
    cbsa_states = {st.strip() for st in states.split("-")}
    principal = _norm(names.split("-")[0])
    for key, data in research.METRO_DATA.items():
        official = re.sub(r"\s+MSA\s*$", "", data.get("metro_name") or "")
        if (
            _norm(official.split("-")[0]) == principal
            and (data.get("state") or "") in cbsa_states
        ):
            return key, data
    return "", {}


def _metro_market(
    loc: str,
    metro_key: str,
    metro: dict,
    res: dict,
    *,
    kind: str,
    assumed: str = "",
    note: str = "",
    state: str = "",
) -> Market:
    # The site's state is its ADDRESS's; the metro (CBSA) only names the figures.
    state = state or str(metro.get("state") or "")
    if metro_key == "washington_dc":
        site_key = "city:washington,DC"
    else:
        city = res.get("city") or re.sub(r",.*$", "", loc).strip()
        site_key = f"city:{_norm(city)},{state}"
    return Market(
        location=loc,
        kind=kind,
        level="metro",
        state=state,
        metro_key=metro_key,
        assumed=assumed,
        ambiguity_note=note,
        cbsa_title=str(res.get("cbsa_title") or ""),
        site_key=site_key,
    )


def resolve_market(location_str: Any, resolution: dict | None = None) -> Market:
    """Resolve one US location. ``resolution`` is this location's
    ``plan_location`` result (``data["_location_resolution"][i]``), when known."""
    import research  # lazy: research imports nothing from here

    loc = str(location_str or "").strip()
    res: dict = resolution if isinstance(resolution, dict) else {}
    norm = _norm(loc)
    res_kind = str(res.get("kind") or "")

    # 1. Not a place.
    if res_kind in ("remote", "nationwide") or norm in _REMOTE_TOKENS or (
        norm in _NATIONWIDE_TOKENS
    ):
        kind = (
            res_kind
            if res_kind in ("remote", "nationwide")
            else ("remote" if norm in _REMOTE_TOKENS else "nationwide")
        )
        return Market(location=loc, kind=kind, level="none", site_key=kind)

    # 1b. A non-US market is not ours to resolve (never read "Georgia" the
    # country as a state, or any country as a US state/city).
    # Only a CONFIRMED verdict (resolved / corrected) skips this: plan_location
    # tags a bare "Toronto" or "London" as an ambiguous US city.
    if str(res.get("status") or "") not in ("resolved", "corrected") and not _bare_state_code(loc):
        country = research._detect_country(loc)
        if country and country != "United States":
            return Market(location=loc, kind="unknown", level="generic", site_key=f"intl:{norm}")

    # 2. Bare "Washington" / "New York": the metro, assumed and flagged.
    bare = _bare_state_city_default(loc, res)
    if bare is not None:
        key, assumed_label, bare_note = bare
        return _metro_market(
            loc,
            key,
            research.METRO_DATA.get(key) or {},
            res,
            kind="city",
            assumed=assumed_label,
            note=bare_note,
        )

    # 3. A state entry.
    state_code = ""
    if res_kind == "state":
        state_code = str(res.get("state_usps") or "")
    elif not res_kind:
        # No resolver verdict: a full state name that is ALSO a curated metro
        # ("New York") keeps meaning the city, as it always has here.
        mk, _m = research._find_metro(loc)
        if not mk:
            state_code = _bare_state_code(loc)
    if state_code:
        if state_code == "DC":
            metro = research.METRO_DATA.get("washington_dc") or {}
            return _metro_market(loc, "washington_dc", metro, res, kind="state")
        return Market(
            location=loc,
            kind="state",
            level="state",
            state=state_code,
            site_key=f"state:{state_code}",
        )

    # 4. A city (or something unresolvable).
    ambiguous = str(res.get("status") or "") == "ambiguous"
    resolved_state = (
        str(res.get("state_usps") or "")
        if str(res.get("status") or "") in ("resolved", "corrected")
        else ""
    )
    city_text = str(res.get("city") or "") or re.sub(r",.*$", "", loc).strip()
    query = f"{city_text}, {resolved_state}" if resolved_state else loc
    metro_key, metro = research._find_metro(query)
    if not metro and resolved_state:
        # Only a CONFIRMED site: an ambiguous name's cbsa_title is
        # plan_location's alphabetical tie-break pick, not evidence.
        metro_key, metro = _metro_from_cbsa(str(res.get("cbsa_title") or ""))
    state = resolved_state or _explicit_state(loc) or str((metro or {}).get("state") or "")

    if not metro and ambiguous:
        default_key, default_state = _AMBIGUOUS_BARE_DEFAULT.get(_norm(city_text), ("", ""))
        if default_key and default_key in research.METRO_DATA:
            metro_key, metro = default_key, research.METRO_DATA[default_key]
            state = default_state
        elif default_state:
            state = default_state

    assumed = ""
    note = ""
    if ambiguous and state:
        assumed = f"{_title_city(city_text)}, {state}"
        note = (
            f"“{_title_city(city_text)}” is ambiguous "
            f"({_ambiguity_count(res)}); {assumed} assumed."
        )
    elif ambiguous:
        note = (
            f"“{_title_city(city_text)}” is ambiguous "
            f"({_ambiguity_count(res)}); no market assumed, so no figures are shown."
        )

    if metro:
        return _metro_market(
            loc, metro_key, metro, res, kind="city", assumed=assumed, note=note, state=state
        )
    if state:
        return Market(
            location=loc,
            kind="city",
            level="state",
            state=state,
            assumed=assumed,
            ambiguity_note=note,
            cbsa_title=str(res.get("cbsa_title") or ""),
            site_key=f"city:{_norm(city_text)},{state}",
        )
    return Market(
        location=loc,
        kind="unknown",
        level="generic",
        ambiguity_note=note,
        site_key=f"city:{_norm(city_text)},",
    )


# ---------------------------------------------------------------------------
# Plan-level helpers (the sidecar is index-aligned with data["locations"]).
# ---------------------------------------------------------------------------


def resolution_map(data: dict | None) -> dict[str, dict]:
    """``{location string: plan_location result}`` for ``data["locations"]``.

    The sidecar is written index-aligned with the list it resolved, but
    ``data["locations"]`` is then rewritten to canonical display names, so the
    strings differ. Align by index when the lengths agree; otherwise fall back
    to matching the original input or display name.
    """
    out: dict[str, dict] = {}
    if not isinstance(data, dict):
        return out
    locs = data.get("locations") or []
    sidecar = data.get("_location_resolution") or []
    if not isinstance(locs, list) or not isinstance(sidecar, list):
        return out
    sidecar = [r for r in sidecar if isinstance(r, dict)]
    if len(sidecar) == len(locs):
        for loc, res in zip(locs, sidecar):
            if isinstance(loc, str) and loc not in out:
                out[loc] = res
        return out
    for loc in locs:
        if not isinstance(loc, str):
            continue
        for res in sidecar:
            if loc in (res.get("input"), res.get("display_name")):
                out.setdefault(loc, res)
                break
    return out


def resolution_list(data: dict | None, locations: list | None = None) -> list[dict | None]:
    """The plan_location result for each entry of ``locations`` (default
    ``data["locations"]``), matched by POSITION.

    Display names are not unique -- "NY" and "New York" are both displayed as
    "New York", "WA" and "Washington" as "Washington" -- so a name-keyed lookup
    hands the second entry the first one's verdict. When ``locations`` is the
    plan's own list the sidecar is index-aligned; otherwise fall back to the
    name map.
    """
    if not isinstance(data, dict):
        return [None] * len(locations or [])
    plan_locs = data.get("locations")
    locs = list(locations if locations is not None else plan_locs or [])
    sidecar = data.get("_location_resolution")
    if (
        isinstance(plan_locs, list)
        and isinstance(sidecar, list)
        and len(sidecar) == len(plan_locs)
        and locs == plan_locs
    ):
        return [r if isinstance(r, dict) else None for r in sidecar]
    by_name = resolution_map(data)
    return [by_name.get(loc) if isinstance(loc, str) else None for loc in locs]


def plan_markets(data: dict | None) -> list[Market]:
    """One ``Market`` per entry of ``data["locations"]``, in order."""
    if not isinstance(data, dict):
        return []
    locs = data.get("locations") or []
    if isinstance(locs, str):
        locs = [locs]
    resolutions = resolution_list(data, locs)
    return [
        resolve_market(loc, res)
        for loc, res in zip(locs, resolutions)
        if isinstance(loc, str) and loc.strip()
    ]


def physical_markets(data: dict | None) -> list[Market]:
    """Distinct physical sites: Remote / Nationwide are targeting modes, not
    places, and two spellings of one site ("DC" + "Washington") count once."""
    seen: set[str] = set()
    out: list[Market] = []
    for market in plan_markets(data):
        if not market.is_physical or market.site_key in seen:
            continue
        seen.add(market.site_key)
        out.append(market)
    return out


def has_nationwide(data: dict | None) -> bool:
    return any(m.kind == "nationwide" for m in plan_markets(data))
