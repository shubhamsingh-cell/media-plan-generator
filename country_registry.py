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
        (
            "istanbul",
            "ankara",
            "izmir",
            "bursa",
            "antalya",
            "adana",
            "konya",
            "gaziantep",
        ),
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
        (
            "bucharest",
            "cluj-napoca",
            "cluj napoca",
            "timisoara",
            "iasi",
            "brasov",
            "constanta",
        ),
    ),
    "Taiwan": (
        ("tw", "republic of china taiwan"),
        (
            "taipei",
            "new taipei",
            "kaohsiung",
            "taichung",
            "tainan",
            "hsinchu",
            "taoyuan",
        ),
    ),
    "Bangladesh": (
        ("bd",),
        (
            "dhaka",
            "chittagong",
            "chattogram",
            "khulna",
            "sylhet",
            "rajshahi",
            "gazipur",
        ),
    ),
    "Egypt": (
        ("eg",),
        ("cairo", "giza", "shubra el kheima", "port said", "suez", "luxor", "mansoura"),
    ),
    "Pakistan": (
        ("pk",),
        (
            "karachi",
            "lahore",
            "islamabad",
            "faisalabad",
            "rawalpindi",
            "multan",
            "peshawar",
        ),
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

# Bare city names that are ALSO US places (audited against data/geo/us_places.tsv
# by tests/test_unknown_country_not_us.py): a bare "Odessa", "Medina" or "Lima"
# keeps the existing (US-leaning / ambiguous) reading; the "City, Country" form
# ("Odessa, Ukraine") still resolves abroad.  (A bare COUNTRY name such as
# "Peru" is read as the country, the way "Mexico" and "India" are; "City, ST" /
# "City, State" is always the US place -- ``plan_geo.us_state_for_location``.)
_BARE_CITY_DENYLIST = frozenset(
    {
        "athens",
        "moscow",
        "alexandria",
        "cairo",
        "cartago",
        "casablanca",
        "lima",
        "mecca",
        "medina",
        "odessa",
        "saint petersburg",
        "st petersburg",
        "suez",
        "tangier",
    }
)


# Well-known world cities written BARE -> the research.COUNTRY_DATA country they
# are in. Moved here from research._detect_country (2026-10-18) so plan_geo reads
# the same list instead of keeping a second opinion on "Munich". Entries may
# share a name with a US place ("Berlin", "Toronto"); research reads them as the
# city abroad (after its US-metro guard), while plan_geo only acts on a name the
# US gazetteer does not also hold (see plan_geo._bare_foreign_city). Keys are
# matched whole-word in research, so keep them lower-case.
WORLD_CITY_COUNTRY: Dict[str, str] = {
    "london": "United Kingdom",
    "manchester": "United Kingdom",
    "edinburgh": "United Kingdom",
    "birmingham uk": "United Kingdom",
    "berlin": "Germany",
    "munich": "Germany",
    "hamburg": "Germany",
    "frankfurt": "Germany",
    "paris": "France",
    "lyon": "France",
    "marseille": "France",
    "amsterdam": "Netherlands",
    "rotterdam": "Netherlands",
    "the hague": "Netherlands",
    "tokyo": "Japan",
    "osaka": "Japan",
    "nagoya": "Japan",
    "milan": "Italy",
    "milano": "Italy",
    "rome": "Italy",
    "turin": "Italy",
    "mumbai": "India",
    "bangalore": "India",
    "delhi": "India",
    "hyderabad": "India",
    "sydney": "Australia",
    "melbourne": "Australia",
    "brisbane": "Australia",
    "toronto": "Canada",
    "vancouver": "Canada",
    "montreal": "Canada",
    "dublin": "Ireland",
    "cork": "Ireland",
    "galway": "Ireland",
    "johannesburg": "South Africa",
    "cape town": "South Africa",
    "nairobi": "Kenya",
    "buenos aires": "Argentina",
    "são paulo": "Brazil",
    "rio de janeiro": "Brazil",
    "sao paulo": "Brazil",
    "zurich": "Switzerland",
    "geneva": "Switzerland",
    "basel": "Switzerland",
    "dubai": "UAE",
    "abu dhabi": "UAE",
    "madrid": "Spain",
    "barcelona": "Spain",
    "mexico city": "Mexico",
    "monterrey": "Mexico",
    "warsaw": "Poland",
    "krakow": "Poland",
    "copenhagen": "Denmark",
    "prague": "Czech Republic",
    "stockholm": "Sweden",
    "oslo": "Norway",
    "helsinki": "Finland",
    "lisbon": "Portugal",
    "brussels": "Belgium",
    "vienna": "Austria",
    "bogotá": "Colombia",
    "bogota": "Colombia",
    "santiago": "Chile",
    "manila": "Philippines",
    "kuala lumpur": "Malaysia",
    "auckland": "New Zealand",
    "wellington": "New Zealand",
    "seoul": "South Korea",
    "tel aviv": "Israel",
    "shanghai": "China",
    "beijing": "China",
    "shenzhen": "China",
    "jakarta": "Indonesia",
    "bangkok": "Thailand",
    "ho chi minh": "Vietnam",
    # The rest of Germany's ten largest cities (research listed 4 of the 10).
    "cologne": "Germany",
    "stuttgart": "Germany",
    "düsseldorf": "Germany",
    "dusseldorf": "Germany",
    "leipzig": "Germany",
    "dortmund": "Germany",
    "essen": "Germany",
}


def _fold(text: Any) -> str:
    raw = (
        unicodedata.normalize("NFKD", str(text or ""))
        .encode("ascii", "ignore")
        .decode()
    )
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
    """ "Lima, OH" / "Peru, Indiana" / "Lima OH" / "Peru Indiana" / "Greece New
    York": a US place, never a country."""
    try:
        from plan_geo import US_STATE_ABBR, US_STATE_NAME_TO_ABBR, us_state_for_location
    except ImportError:  # pragma: no cover - plan_geo ships with the repo
        return False
    if us_state_for_location(folded):
        return True
    text = re.sub(r"\s+\d{5}(?:-\d{4})?$", "", folded.rstrip(". "))  # trailing ZIP
    tokens = text.split()
    if len(tokens) < 2:
        return False
    if len(tokens[-1]) == 2 and tokens[-1].upper() in US_STATE_ABBR:
        return True
    # a trailing full state name without a comma, one or two words
    for width in (2, 1):
        if len(tokens) > width and " ".join(tokens[-width:]) in US_STATE_NAME_TO_ABBR:
            return True
    return False


def trailing_country(location: Any) -> Optional[str]:
    """Registry country named by the TRAILING token of a "City, Country" /
    "City (Country)" string, else None.  Checked before a US-metro guard so
    "San Jose, Costa Rica" is not read as San Jose, CA."""
    if not isinstance(location, str) or not location.strip():
        return None
    folded = _fold(location)
    if not folded or _is_us_form(folded):
        return None
    tail = folded.rsplit(",", 1)[-1].strip() if "," in folded else ""
    paren = re.search(r"\(([^()]+)\)\s*$", folded)
    for token in (tail, paren.group(1).strip() if paren else ""):
        if token and token in _NAMES:
            return _NAMES[token]
    return None


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


_WORLD_CITIES_FOLDED: Dict[str, str] = {
    _fold(city): country for city, country in WORLD_CITY_COUNTRY.items()
}


def world_city_country(location: Any) -> Optional[str]:
    """COUNTRY_DATA country of a location that is, as a whole, one BARE
    well-known world city ("Dortmund", "Munich", "Düsseldorf"), else None.

    Only the whole string counts: "Dortmund, Germany" and "Peru, Indiana" are
    other forms with their own readers, and a US form ("Berlin, NH") is never
    a world city here. A name that is also a US place is still returned --
    deciding what to do about the collision is the caller's job
    (``plan_geo._bare_foreign_city``).
    """
    if not isinstance(location, str) or not location.strip():
        return None
    folded = _fold(location)
    if not folded or "," in folded or _is_us_form(folded):
        return None
    return _WORLD_CITIES_FOLDED.get(folded)
