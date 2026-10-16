"""Countries the repo's datasets name but ``research.COUNTRY_DATA`` has no row for.

Background (2026-10-17). ``research._detect_country`` only knew the 41 countries
in ``COUNTRY_DATA``.  Every other country -- Turkey, Peru, Romania, Nigeria,
Saudi Arabia, Taiwan, Bangladesh... (all six datasets below name them) -- came
back ``None``, which every caller reads as "a US location": a plan for Istanbul
got US salaries, US unemployment, a US competitor roster and US$ benchmarks.

This module is the one list of those countries.  It carries NO statistics: a
name, spellings, and the best-known cities (plain geography, so a bare
"Istanbul" is recognised).  Figures for a country come from the datasets that
cover it (``data/international_benchmarks_2026.json`` ...); a country with no
such data is shown as "no country data", never as another country's figures.

Stdlib only: ``plan_geo``, ``plan_currency`` and ``research`` all import it.

Coverage contract (``tests/test_unknown_country_not_us.py``): every country
named by ``international_benchmarks_2026.json``, ``global_supply.json``,
``intl_role_benchmarks_v1.json``, ``standardizer.COUNTRY_MAP`` and
``publisher_filter``'s country words is either a ``COUNTRY_DATA`` row or an
entry here.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Optional, Tuple

# canonical name (the benchmark dataset's own spelling) ->
#   (alternative spellings / codes, well-known cities)
# Cities that are also sizeable US places (Athens GA, Moscow ID, Alexandria VA)
# are left out of the lists: they are recognised in the "City, Country" form,
# which needs no city list.  The cities listed are read as the foreign city when
# written bare, the way a bare "London" or "Toronto" already is.
_EXTRA_COUNTRIES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]] = {
    "Turkey": (
        ("turkiye", "tr", "republic of turkey"),
        ("istanbul", "ankara", "izmir", "bursa", "antalya", "adana", "konya", "gaziantep"),
    ),
    "Saudi Arabia": (
        ("ksa", "kingdom of saudi arabia", "saudi"),
        ("riyadh", "jeddah", "dammam", "mecca", "medina", "khobar", "jubail", "neom"),
    ),
    "Nigeria": (
        ("ng",),
        ("lagos", "abuja", "kano", "ibadan", "port harcourt", "benin city", "kaduna"),
    ),
    "Peru": (
        (),
        ("lima", "arequipa", "trujillo", "chiclayo", "cusco", "piura"),
    ),
    "Romania": (
        ("ro",),
        ("bucharest", "cluj-napoca", "cluj napoca", "timisoara", "iasi", "brasov", "constanta"),
    ),
    "Taiwan": (
        ("tw", "republic of china taiwan"),
        ("taipei", "new taipei", "kaohsiung", "taichung", "tainan", "hsinchu", "taoyuan"),
    ),
    "Bangladesh": (
        ("bd",),
        ("dhaka", "chittagong", "chattogram", "khulna", "sylhet", "rajshahi", "gazipur"),
    ),
    "Egypt": (
        ("eg",),
        ("cairo", "giza", "shubra el kheima", "port said", "suez", "luxor", "mansoura"),
    ),
    "Pakistan": (
        ("pk",),
        ("karachi", "lahore", "islamabad", "faisalabad", "rawalpindi", "multan", "peshawar"),
    ),
    "Qatar": (
        ("qa",),
        ("doha", "al rayyan", "al wakrah"),
    ),
    "Greece": (
        ("hellas",),
        ("thessaloniki", "patras", "heraklion"),
    ),
    "Hungary": (
        (),
        ("budapest", "debrecen", "szeged", "miskolc"),
    ),
    "Russia": (
        ("russian federation", "ru"),
        ("st petersburg", "saint petersburg", "novosibirsk", "yekaterinburg", "kazan"),
    ),
    "Ukraine": (
        ("ua",),
        ("kyiv", "kiev", "kharkiv", "odesa", "odessa", "lviv", "dnipro"),
    ),
    "Sri Lanka": (
        ("lk",),
        ("colombo", "kandy", "galle", "negombo"),
    ),
    "Costa Rica": (
        (),
        ("alajuela", "heredia", "cartago"),
    ),
    "Morocco": (
        (),
        ("casablanca", "rabat", "marrakech", "tangier", "fes"),
    ),
}

# Sizeable US places: never a bare-city match even if a future edit adds one to
# the tables above.  (A bare COUNTRY name such as "Peru" is read as the country,
# the way "Mexico" and "India" are; "City, ST" / "City, State" is always the US
# place -- ``plan_geo.us_state_for_location`` runs first.)
_BARE_CITY_DENYLIST = frozenset({"athens", "moscow", "alexandria"})


def _fold(text: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", raw.lower().replace("’", "'")).strip()


def _build() -> Tuple[Dict[str, str], Dict[str, str]]:
    names: Dict[str, str] = {}
    cities: Dict[str, str] = {}
    for canonical, (aliases, city_list) in _EXTRA_COUNTRIES.items():
        names[_fold(canonical)] = canonical
        for alias in aliases:
            names[_fold(alias)] = canonical
        for city in city_list:
            key = _fold(city)
            if key not in _BARE_CITY_DENYLIST:
                cities[key] = canonical
    return names, cities


_NAMES, _CITIES = _build()

# Name spellings short enough to collide with a US state code, a province or an
# ordinary word may only appear as the trailing ", XX" / "(XX)" token.
_POSITIONAL_ONLY = frozenset(k for k in _NAMES if len(k) <= 3)


def extra_countries() -> Tuple[str, ...]:
    """Canonical names of every country in this registry."""
    return tuple(_EXTRA_COUNTRIES)


def canonical_name(text: Any) -> Optional[str]:
    """Registry country for an exact country name / spelling, else None."""
    return _NAMES.get(_fold(text))


def _is_us_form(folded: str) -> bool:
    """"Lima, OH" / "Peru, Indiana" / "Lima OH": a US place, never a country."""
    try:
        from plan_geo import US_STATE_ABBR, us_state_for_location
    except ImportError:  # pragma: no cover - plan_geo ships with the repo
        return False
    if us_state_for_location(folded):
        return True
    tokens = folded.split()
    return (
        len(tokens) >= 2
        and len(tokens[-1]) == 2
        and tokens[-1].upper() in US_STATE_ABBR
    )


def detect_extra(location: Any) -> Optional[str]:
    """Country (from this registry) a location string names, else None.

    * ``"Istanbul, Turkey"``, ``"Lima, Peru"``, ``"Riyadh, Saudi Arabia"``,
      ``"Dhaka, BD"``, ``"Istanbul (Turkey)"`` -> the country;
    * a bare well-known city (``"Istanbul"``, ``"Taipei"``) -> its country;
    * a US form (``"Lima, OH"``, ``"Peru, Indiana"``) -> None.
    """
    if not isinstance(location, str) or not location.strip():
        return None
    folded = _fold(location)
    if not folded or _is_us_form(folded):
        return None
    # the whole string is a country
    hit = _NAMES.get(folded)
    if hit:
        return hit
    # "City, Region, Country" / "City (Country)": the trailing token
    tail = folded.rsplit(",", 1)[-1].strip()
    paren = re.search(r"\(([^()]+)\)\s*$", folded)
    for token in (tail, paren.group(1).strip() if paren else ""):
        if token and token in _NAMES:
            return _NAMES[token]
    # a country NAME (not a 2-3 letter code) anywhere in the text
    for key, canonical in _NAMES.items():
        if key in _POSITIONAL_ONLY:
            continue
        if re.search(r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])", folded):
            return canonical
    # a bare well-known city: "Istanbul" or the head of "Istanbul, <anything>"
    head = folded.split(",", 1)[0].strip()
    return _CITIES.get(head)
