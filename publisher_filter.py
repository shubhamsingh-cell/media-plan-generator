"""Publisher exclusion and availability -- ONE matcher for every plan surface.

Two independent reasons keep a job board out of a plan, and both flow through
the same name matcher so a board can never be hidden on one sheet and quoted
on another:

* **Client exclusions** -- ``excluded_publishers`` on the request (the
  wizard's "Exclude job boards" tag input).  Disclosed on the Executive
  Summary and deck slide 2 as "Excluded at client request: ...".
* **Availability** -- ``data/publisher_availability.json``: boards that are
  not available through Joveo (or have exited a market).  Permanent, applied
  silently (no client-facing disclosure line).

Matching is deliberately forgiving about *spelling* and strict about
*identity*: accents are folded, case and punctuation ignored, ``www.`` and
TLDs stripped (``indeed.com`` == ``Indeed``), a trailing country name is
stripped (``Indeed France`` == ``Indeed``), generic trailing words are
ignored (``Indeed Sponsored Jobs`` == ``Indeed``), and a small sourced alias
table folds rebrands together (``RegionsJob`` == ``HelloWork``;
``OCC`` == ``OCC Mundial``).  Anything else must match the whole normalised
name -- "Job" never excludes "Jobcase".

Countries are the repo's canonical country NAMES ("France", "United Kingdom")
-- the strings ``research._detect_country`` returns; ``None`` means "no
country known at this call site".
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_AVAILABILITY_PATH = os.path.join(_BASE_DIR, "data", "publisher_availability.json")
_REPOSITORY_PATH = os.path.join(
    _BASE_DIR, "data", "joveo_global_supply_repository.json"
)
_NETWORK_PATH = os.path.join(_BASE_DIR, "data", "joveo_network_groups.json")
_SUPPLY_NOTES_PATH = os.path.join(_BASE_DIR, "data", "global_supply.json")

# Request-boundary caps (mirrored in wizard_inputs.INPUT_LIMITS).
MAX_EXCLUDED = 20
MAX_NAME_CHARS = 80

# Shown once per surface beside a "†" board.
RELATIONSHIP_MARK = "†"
RELATIONSHIP_NOTE = (
    f"{RELATIONSHIP_MARK} relationship unverified — confirm before quoting"
)

_COUNTRY_ALIASES: Dict[str, str] = {
    "us": "United States",
    "usa": "United States",
    "u.s.": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "gb": "United Kingdom",
    "great britain": "United Kingdom",
    "fr": "France",
    "de": "Germany",
    "mx": "Mexico",
    "in": "India",
    "au": "Australia",
    "sg": "Singapore",
    "vn": "Vietnam",
    "viet nam": "Vietnam",
    "uae": "United Arab Emirates",
    "ca": "Canada",
}

# Country words stripped from the END of a board name ("Indeed France").
_COUNTRY_WORDS: Tuple[str, ...] = (
    "united kingdom", "united states", "united arab emirates", "south africa",
    "south korea", "new zealand", "saudi arabia", "czech republic",
    "hong kong", "viet nam", "costa rica", "sri lanka",
    "argentina", "australia", "austria", "bangladesh", "belgium", "brazil",
    "canada", "chile", "china", "colombia", "denmark", "egypt", "finland",
    "france", "germany", "greece", "india", "indonesia", "ireland", "israel",
    "italy", "japan", "kenya", "malaysia", "mexico", "morocco", "netherlands",
    "nigeria", "norway", "pakistan", "peru", "philippines", "poland",
    "portugal", "romania", "singapore", "spain", "sweden", "switzerland",
    "taiwan", "thailand", "turkey", "uae", "ukraine", "uk", "usa", "us",
    "vietnam",
)  # fmt: skip
_COUNTRY_WORD_SET = frozenset(_COUNTRY_WORDS)

# Ordinary words that are also board names; never a prose-matching term alone.
_COMMON_WORDS = frozenset(
    {
        "talent", "jobs", "job", "career", "careers", "work", "works", "hire",
        "hiring", "recruit", "staff", "staffing", "find", "search", "apply",
        "hello", "match", "people", "market", "mobile", "social", "free",
        "direct", "global", "local", "national", "online", "network",
    }
)  # fmt: skip

# Words that are not a board name on their own: rejected at the request
# boundary (clear 400 / inline wizard message) and never a sweep pattern.
_STOPWORDS = frozenset(
    {
        "a", "an", "the", "it", "is", "of", "or", "and", "to", "in", "on", "at",
        "for", "by", "my", "no", "all", "any", "none", "other", "others", "n/a",
        "na", "jobs", "job", "career", "careers", "hire",
        "hiring", "board", "boards", "site", "sites", "online", "work", "works",
        "web", "website", "com", "net", "org", "www", "search", "find", "apply",
        "free", "new", "best", "top", "general", "global", "local", "national",
        "social", "network", "recruit", "recruiting", "recruitment", "staffing",
        "employment", "vacancies", "vacancy", "test", "example",
    }
)  # fmt: skip

# Boards whose name is also an ordinary English word: in prose they match only
# in their capitalised / upper-case brand form ("Seek", "SEEK"), never in
# "job seekers seek flexible work".
_ORDINARY_WORD_BOARDS = frozenset(
    {"seek", "reed", "monster", "dice", "indeed", "hired", "talent", "shine", "snag"}
)

# What may follow a brand's first word without making it a different board.
_BRAND_REST_WORDS = frozenset(
    {"sponsored", "jobs", "job", "ads", "advertising", "careers", "career",
     "hiring", "com", "europe", "asia", "apac", "emea", "latam", "global"}
)  # fmt: skip

# Multi-country brands whose national sites are written "<brand> <ISO-2>"
# ("Indeed DE", "Monster FR"): a trailing two-letter code is a country only
# after one of these (so "Dice NO" is never read as a Norwegian Dice).
_MULTI_COUNTRY_BRANDS = frozenset(
    {
        "indeed", "linkedin", "monster", "stepstone", "glassdoor", "jobrapido",
        "jooble", "adzuna", "jobijoba", "careerjet", "optioncarriere", "jobsdb",
        "jobstreet", "trovit", "neuvoo", "ziprecruiter", "talent",
    }
)  # fmt: skip

# Words that do not change which board it is.  "jobs"/"careers"/"com" are NOT
# here: "Total Jobs" is Totaljobs and "Snag A Job" is Snagajob -- the word is
# part of the name.  They are dropped only after a multi-country brand
# ("Indeed Jobs", "LinkedIn Careers").
_GENERIC_WORDS = frozenset(
    {
        "sponsored", "ads", "ad", "advertising", "inc", "ltd", "llc", "gmbh",
        "limited", "corp", "the", "network",
    }
)  # fmt: skip
_BRAND_SUFFIX_WORDS = frozenset({"jobs", "job", "careers", "career", "com"})

_TLDS: Tuple[str, ...] = tuple(
    sorted(
        {
            "com", "net", "org", "io", "eu", "co", "info", "biz", "jobs",
            "fr", "de", "uk", "in", "mx", "au", "sg", "vn", "ca", "es", "it",
            "nl", "br", "jp", "pl", "ro", "cz", "be", "at", "ch", "se", "no",
            "dk", "ie", "nz", "za", "ae", "sa", "tr", "kr", "tw", "ph", "my",
            "th", "id", "ar", "cl", "pe",
            "co.uk", "com.au", "com.sg", "com.vn", "com.br", "com.mx",
            "co.jp", "co.nz", "co.za", "co.kr", "com.tw", "com.ph", "com.my",
            "co.th", "co.id", "com.ar", "com.co", "co.in", "net.au", "org.uk",
        },
        key=len,
        reverse=True,
    )
)  # fmt: skip

_DOMAIN_RE = re.compile(
    r"^(?:https?://)?(?:www\.)?([a-z0-9][a-z0-9\-]*(?:\.[a-z0-9\-]+)+)(?:[/?#].*)?$"
)
_PAREN_RE = re.compile(r"\([^)]*\)|\[[^\]]*\]")
_SPLIT_RE = re.compile(r"\s+/\s+|\s*\|\s*")


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _fold(text: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(ch for ch in raw if not unicodedata.combining(ch)).lower().strip()


def canonical_country(country: Any) -> str:
    """Repo country name for a name/ISO code ("UK" -> "United Kingdom"); ''
    when blank.  Unknown strings pass through title-cased."""
    text = _fold(country)
    if not text:
        return ""
    if text in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[text]
    return " ".join(w.capitalize() for w in text.split())


def _strip_tld(host: str) -> str:
    for tld in _TLDS:
        suffix = "." + tld
        if host.endswith(suffix) and len(host) > len(suffix):
            return host[: -len(suffix)]
    return host


def _tokens_key(text: str) -> str:
    tokens = [t for t in re.split(r"[^a-z0-9]+", text) if t]
    # trailing country ("indeed france", "seek new zealand")
    while len(tokens) > 1:
        for width in (3, 2, 1):
            tail = " ".join(tokens[-width:])
            if len(tokens) > width and tail in _COUNTRY_WORD_SET:
                tokens = tokens[:-width]
                break
        else:
            break
    if (
        len(tokens) == 2
        and tokens[0] in _MULTI_COUNTRY_BRANDS
        and len(tokens[1]) == 2
        and tokens[1].isalpha()
    ):
        tokens = tokens[:1]
    if tokens and tokens[0] in _MULTI_COUNTRY_BRANDS:
        tokens = tokens[:1] + [t for t in tokens[1:] if t not in _BRAND_SUFFIX_WORDS]
    kept = [t for t in tokens if t not in _GENERIC_WORDS]
    return "".join(kept or tokens)


def _component_key(text: str) -> str:
    text = _PAREN_RE.sub(" ", text).strip()
    match = _DOMAIN_RE.match(text.replace(" ", "")) if " " not in text.strip() else None
    if match:
        return re.sub(r"[^a-z0-9]", "", _strip_tld(match.group(1)))
    return _tokens_key(text)


def _squash(name: Any) -> str:
    """Letters and digits only -- the exact-spelling key renames use (the
    country-stripping :func:`normalize` would make 'CareerBuilder Vietnam'
    collide with the US 'CareerBuilder')."""
    return re.sub(r"[^a-z0-9]", "", _fold(_PAREN_RE.sub(" ", str(name or ""))))


def normalize(name: Any) -> str:
    """The comparison key for one board name ('' when nothing is left)."""
    return _component_key(_fold(name))


def _parts(name: Any) -> List[str]:
    """The ' / ' separated parts of a compound board string ('Workopolis /
    Monster Canada'); one element for an ordinary name."""
    stripped = _PAREN_RE.sub(" ", _fold(name))
    parts = [p.strip() for p in _SPLIT_RE.split(stripped) if p and p.strip()]
    return parts if len(parts) > 1 else []


def _components(name: Any) -> List[str]:
    """Keys that identify ``name``: the whole string, plus each part of a
    compound ('A / B') so either board is reachable through the string."""
    text = _fold(name)
    keys: List[str] = []
    whole = _component_key(text)
    if whole:
        keys.append(whole)
    for part in _parts(name):
        key = _component_key(part)
        if key and key not in keys:
            keys.append(key)
    return keys


# ---------------------------------------------------------------------------
# Availability data
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_availability_cache: Optional[Dict[str, Any]] = None
_repo_index_cache: Optional[Dict[str, List[Tuple[frozenset, str]]]] = None
_repo_date_cache: str = ""
_supply_notes_cache: Optional[Dict[str, List[Tuple[frozenset, Dict[str, Any]]]]] = None
_repo_board_rows_cache: Optional[List[Tuple[str, frozenset, str, str]]] = None
_network_cache: Optional[List[Dict[str, Any]]] = None


def _load_availability() -> Dict[str, Any]:
    global _availability_cache
    with _lock:
        if _availability_cache is not None:
            return _availability_cache
        data: Dict[str, Any] = {}
        try:
            with open(_AVAILABILITY_PATH, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError) as exc:
            logger.error(
                "publisher_availability.json unreadable: %s", exc, exc_info=True
            )
        if not isinstance(data, dict):
            data = {}
        # alias key -> group id (every name in a group shares one key)
        group_of: Dict[str, str] = {}
        for group in data.get("alias_groups") or []:
            names = [n for n in (group.get("names") or []) if isinstance(n, str)]
            keys = [normalize(n) for n in names if normalize(n)]
            if not keys:
                continue
            for key in keys:
                group_of[key] = keys[0]
        entries: List[Dict[str, Any]] = []
        for entry in data.get("entries") or []:
            if not isinstance(entry, dict) or not entry.get("name"):
                continue
            all_names = [entry["name"]] + [
                a for a in (entry.get("aliases") or []) if isinstance(a, str)
            ]
            keys = {group_of.get(normalize(n), normalize(n)) for n in all_names}
            keys.discard("")
            entries.append(
                {
                    **entry,
                    "_keys": frozenset(keys),
                    "_countries": frozenset(
                        canonical_country(c) for c in (entry.get("countries") or [])
                    ),
                }
            )
        scopes: List[Dict[str, Any]] = []
        for scope in data.get("market_scope") or []:
            if not isinstance(scope, dict) or not scope.get("name"):
                continue
            all_names = [scope["name"]] + [
                a for a in (scope.get("aliases") or []) if isinstance(a, str)
            ]
            scopes.append(
                {
                    **scope,
                    "_keys": frozenset(
                        group_of.get(normalize(n), normalize(n)) for n in all_names
                    )
                    - {""},
                    "_countries": frozenset(
                        canonical_country(c) for c in (scope.get("countries") or [])
                    ),
                }
            )
        renames: List[Dict[str, Any]] = []
        for ren in data.get("renames") or []:
            if not isinstance(ren, dict) or not ren.get("new"):
                continue
            renames.append(
                {
                    **ren,
                    "_keys": frozenset(
                        _squash(o) for o in (ren.get("old") or []) if _squash(o)
                    ),
                    "_countries": frozenset(
                        canonical_country(c) for c in (ren.get("countries") or [])
                    ),
                }
            )
        folded: List[Dict[str, Any]] = []
        for fold in data.get("folded_boards") or []:
            if not isinstance(fold, dict) or not fold.get("name"):
                continue
            folded.append(
                {
                    **fold,
                    "_keys": frozenset({normalize(fold["name"])} - {""}),
                    "_countries": frozenset(
                        canonical_country(c) for c in (fold.get("countries") or [])
                    ),
                }
            )
        members_of: Dict[str, List[str]] = {}
        for member_key, group_key in group_of.items():
            members_of.setdefault(group_key, []).append(member_key)
        _availability_cache = {
            "group_of": group_of,
            "members_of": members_of,
            "entries": entries,
            "scopes": scopes,
            "renames": renames,
            "folded": folded,
        }
        return _availability_cache


def _keys_for(name: Any) -> List[str]:
    group_of = _load_availability()["group_of"]
    return [group_of.get(k, k) for k in _components(name)]


def names_match(a: Any, b: Any) -> bool:
    """True when two board names are the same board (alias-aware)."""
    keys_a = set(_keys_for(a))
    return bool(keys_a & set(_keys_for(b)))


def availability_for(name: Any, country: Any = None) -> Optional[Dict[str, Any]]:
    """The availability entry that bars ``name`` (in ``country``), else None.

    ``unavailable`` / ``no_relationship`` apply everywhere when no country is
    known (they describe a board that cannot be bought through Joveo);
    ``exited`` only ever applies in the countries the entry lists.
    """
    if _parts(name):
        return None
    keys = set(_keys_for(name))
    if not keys:
        return None
    want = canonical_country(country)
    for entry in _load_availability()["entries"]:
        if not keys & entry["_keys"]:
            continue
        countries = entry["_countries"]
        status = entry.get("status") or ""
        if want:
            if countries and want not in countries:
                continue
        elif status == "exited" and countries:
            continue
        return entry
    return None


def permanent_exclusion_names() -> List[str]:
    """Display names of every unavailable / no-relationship board (for the
    Nova tools' prompt text and tests)."""
    return [
        e["name"]
        for e in _load_availability()["entries"]
        if (e.get("status") or "") in ("unavailable", "no_relationship")
    ]


# ---------------------------------------------------------------------------
# Client exclusions
# ---------------------------------------------------------------------------


def is_generic_name(name: Any) -> bool:
    """True for a typed name that is not a board's own name: two characters
    or fewer, or made only of stopwords ("IT", "jobs", "the", "job board")."""
    text = _PAREN_RE.sub(" ", _fold(name)).strip()
    tokens = [t for t in re.split(r"[^a-z0-9]+", text) if t]
    if not tokens or len("".join(tokens)) <= 2:
        return True
    return all(t in _STOPWORDS for t in tokens)


def rejected_excluded(raw: Any) -> List[str]:
    """The typed names :func:`sanitize_excluded` refuses as generic."""
    if isinstance(raw, str):
        raw = re.split(r"[\n;,]", raw)
    if not isinstance(raw, (list, tuple)):
        return []
    out: List[str] = []
    for item in raw:
        if isinstance(item, str):
            text = re.sub(r"\s+", " ", re.sub(r"<[^>]*>", "", item)).strip()
            if text and is_generic_name(text):
                out.append(text)
    return out


def sanitize_excluded(raw: Any) -> List[str]:
    """Clean a request's ``excluded_publishers`` value into a list of unique
    display strings.  Accepts a list or a comma/newline separated string;
    drops blanks, generic words ("jobs", "IT"), over-long entries and
    duplicates (by matcher key); caps the count.  Never raises."""
    if isinstance(raw, str):
        raw = re.split(r"[\n;,]", raw)
    if not isinstance(raw, (list, tuple)):
        return []
    out: List[str] = []
    seen: set = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        text = re.sub(r"<[^>]*>", "", item)
        text = re.sub(r"\s+", " ", text).strip()
        if not text or len(text) > MAX_NAME_CHARS or is_generic_name(text):
            continue
        key = normalize(text)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= MAX_EXCLUDED:
            break
    return out


def user_excluded(data: Any) -> List[str]:
    """The client's exclusion list as typed (already request-sanitised)."""
    if not isinstance(data, dict):
        return []
    return sanitize_excluded(data.get("excluded_publishers"))


def _user_keys(data: Any) -> set:
    keys: set = set()
    for name in user_excluded(data):
        keys.update(_keys_for(name))
    return keys


def _user_group_ids(data: Any) -> set:
    """Ids of the board groups the client excluded by the group's own name."""
    out: set = set()
    for name in user_excluded(data):
        group = network_group_for_query(name)
        if group:
            out.add(group.get("id") or group["name"])
    return out


def is_excluded(name: Any, data: Any = None, country: Any = None) -> bool:
    """True when ``name`` must not appear on a plan surface: the client
    excluded it, or it is not available through Joveo (in ``country``)."""
    if not str(name or "").strip():
        return False
    parts = _parts(name)
    if parts:  # 'A / B' is excluded only when every part is
        return all(is_excluded(p, data, country) for p in parts)
    keys = set(_keys_for(name))
    # A product of an excluded brand ("Indeed Education" when the client
    # excluded Indeed) goes with it; the reverse never holds.
    brand = _brand_key(name)
    if (keys or brand) and (keys | ({brand} if brand else set())) & _user_keys(data):
        return True
    # Excluding a board GROUP by name ("The Stepstone Group") excludes every
    # member of it, country-scoped like the membership itself.
    excluded_groups = _user_group_ids(data)
    if excluded_groups:
        hit = network_member(name, country)
        if hit and hit["group_id"] in excluded_groups:
            return True
    return availability_for(name, country) is not None


def _brand_key(name: Any) -> Optional[str]:
    """First word of ``name`` when it is a multi-country brand
    ('Indeed Education' -> 'indeed'), else None."""
    first = re.split(r"[^a-z0-9]+", _PAREN_RE.sub(" ", _fold(name)).strip())[:1]
    return first[0] if first and first[0] in _MULTI_COUNTRY_BRANDS else None


def is_user_excluded(name: Any, data: Any = None) -> bool:
    parts = _parts(name)
    if parts:
        return all(is_user_excluded(p, data) for p in parts)
    keys = set(_keys_for(name))
    return bool(keys and keys & _user_keys(data))


def filter_names(
    names: Iterable[Any], data: Any = None, country: Any = None
) -> List[Any]:
    """``names`` minus excluded / unavailable boards, order preserved."""
    return [n for n in names if not is_excluded(n, data, country)]


def filter_items(
    items: Iterable[Any], data: Any = None, key: str = "name", country: Any = None
) -> List[Any]:
    """Like :func:`filter_names` for dicts (or objects keyed by ``key``);
    non-dict items are treated as the name itself."""
    out = []
    for item in items:
        name = item.get(key) if isinstance(item, dict) else item
        if not is_excluded(name, data, country):
            out.append(item)
    return out


def display_name(name: Any, country: Any = None) -> str:
    """``name`` after sourced renames ('CareerBuilder Vietnam' ->
    'CareerViet'); unchanged when no rename applies."""
    text = str(name or "")
    want = canonical_country(country)
    key = _squash(text)
    for ren in _load_availability()["renames"]:
        if (
            key
            and key in ren["_keys"]
            and (not want or not ren["_countries"] or want in ren["_countries"])
        ):
            return str(ren["new"])
    return text


def is_folded(name: Any, country: Any = None) -> bool:
    """True for a board that is only a portal of another board (Workopolis
    -> Indeed) and so must not be listed as an independent board."""
    if _parts(name):
        return False
    keys = set(_keys_for(name))
    want = canonical_country(country)
    for fold in _load_availability()["folded"]:
        if keys & fold["_keys"] and (
            not want or not fold["_countries"] or want in fold["_countries"]
        ):
            return True
    return False


def clean_names(
    names: Iterable[Any],
    data: Any = None,
    country: Any = None,
    dedupe: bool = True,
) -> List[str]:
    """The board list a surface may print: excluded / unavailable / folded
    boards removed (part-wise inside 'A / B' compounds), sourced renames
    applied, duplicates dropped (``dedupe=False`` keeps them, for a list whose
    entry count is itself meaningful), order preserved."""
    out: List[str] = []
    seen: set = set()
    for raw in names:
        text = str(raw or "").strip()
        if not text:
            continue
        parts = _parts(text)
        if parts:
            originals = [
                p.strip() for p in re.split(r"\s+/\s+|\s*\|\s*", text) if p.strip()
            ]
            kept = clean_names(originals, data, country, dedupe)
            text = " / ".join(kept)
        else:
            if is_excluded(text, data, country) or is_folded(text, country):
                continue
            text = display_name(text, country)
        key = normalize(text)
        if not text or (dedupe and key and key in seen):
            continue
        seen.add(key)
        out.append(text)
    return out


def clean_items(
    items: Iterable[Any], data: Any = None, key: str = "name", country: Any = None
) -> List[Any]:
    """:func:`clean_names` for dict rows: excluded / unavailable / folded
    rows are dropped and a renamed row is returned as a copy with the new
    ``key``; every other row (and every non-dict item) passes through, so a
    caller's positional logic ('first two platforms') sees the clean list."""
    out: List[Any] = []
    for item in items:
        if not isinstance(item, dict):
            out.extend(clean_names([item], data, country))
            continue
        name = str(item.get(key) or "")
        if not name.strip():
            out.append(item)
            continue
        cleaned = clean_names([name], data, country)
        if not cleaned:
            continue
        out.append(item if cleaned[0] == name else {**item, key: cleaned[0]})
    return out


_mention_cache: Dict[Tuple[str, ...], Any] = {}


def _mention_pattern(terms: Tuple[str, ...]) -> Any:
    """Case-insensitive pattern over the plain terms."""
    pat = _mention_cache.get(terms)
    if pat is None:
        alts = []
        for term in terms:
            if term.startswith("!"):
                # an ordinary-word brand still matches as a domain, in any case
                # ("indeed.com", "indeed.co.uk"): that spelling is unambiguous
                alts.append(re.escape(term[1:]) + r"(?:\.[a-z]{2,3})+")
                continue
            words = [w for w in re.split(r"[^a-z0-9]+", term) if w]
            if words:
                alts.append(r"[\W_]*".join(re.escape(w) for w in words))
        pat = (
            re.compile(r"(?<![a-z0-9])(?:" + "|".join(alts) + r")(?![a-z0-9])", re.I)
            if alts
            else None
        )
        _mention_cache[terms] = pat
    return pat


def _strict_pattern(terms: Tuple[str, ...]) -> Any:
    """Case-SENSITIVE pattern for boards that are ordinary words: only the
    capitalised or upper-case brand form matches ("Seek"/"SEEK", never "seek")."""
    key = ("!",) + terms
    pat = _mention_cache.get(key)
    if pat is None:
        alts = []
        for term in terms:
            if term.startswith("!"):
                word = re.escape(term[1:])
                alts.append(f"{word.capitalize()}|{word.upper()}")
        pat = (
            re.compile(r"(?<![A-Za-z0-9])(?:" + "|".join(alts) + r")(?![A-Za-z0-9])")
            if alts
            else None
        )
        _mention_cache[key] = pat
    return pat


_terms_cache: Dict[Tuple[Any, ...], Tuple[str, ...]] = {}


_known_cache: Optional[frozenset] = None


def _known_board_keys() -> frozenset:
    """Matcher keys of every board the repo's catalogs name: the supply
    repository, the benchmark dataset's platforms, the availability file and
    the multi-country brands.  Only these are ever used as free-text sweep
    patterns; an unrecognised typed name is honoured in structured lists
    (exact / normalised match) but never searched for in prose."""
    global _known_cache
    if _known_cache is None:
        keys = set(_repo_index())
        for entry in _dataset_countries().values():
            for plat in entry.get("platforms") or []:
                if isinstance(plat, dict) and plat.get("name"):
                    keys.add(normalize(plat["name"]))
        data = _load_availability()
        keys.update(data["group_of"].values())
        for ent in data["entries"] + data["scopes"]:
            keys.update(ent["_keys"])
        keys.update(_MULTI_COUNTRY_BRANDS)
        keys.update(_catalog_file_keys())
        keys.discard("")
        keys -= {normalize(w) for w in _STOPWORDS}
        _known_cache = frozenset(keys)
    return _known_cache


def _catalog_file_keys() -> set:
    """Keys of the board names in data/global_supply.json (country boards) and
    data/joveo_publishers.json (publisher lists)."""
    keys: set = set()
    base = os.path.join(_BASE_DIR, "data")
    try:
        with open(os.path.join(base, "global_supply.json"), encoding="utf-8") as fh:
            supply = json.load(fh)
        for entry in (supply.get("country_job_boards") or {}).values():
            for board in (entry or {}).get("boards") or []:
                if isinstance(board, dict) and board.get("name"):
                    keys.add(normalize(board["name"]))
    except (OSError, ValueError, AttributeError):
        pass
    try:
        with open(os.path.join(base, "joveo_publishers.json"), encoding="utf-8") as fh:
            pubs = json.load(fh)
        for group in ("by_category", "by_country"):
            for names in (pubs.get(group) or {}).values():
                for name in names if isinstance(names, list) else []:
                    if isinstance(name, str):
                        keys.add(normalize(name))
    except (OSError, ValueError, AttributeError):
        pass
    return keys


def is_known_board(name: Any) -> bool:
    """True when ``name`` resolves to a board in the repo's catalogs."""
    if is_generic_name(name):
        return False
    known = _known_board_keys()
    return any(k in known for k in _keys_for(name)) or (_brand_key(name) or "") in known


def _mention_terms(data: Any, countries: Sequence[str]) -> Tuple[str, ...]:
    """Lower-case surface forms of every board the plan must not name."""
    cache_key = (tuple(user_excluded(data)), tuple(sorted(str(c) for c in countries)))
    cached = _terms_cache.get(cache_key)
    if cached is not None:
        return cached
    terms = _compute_mention_terms(data, countries)
    if len(_terms_cache) > 256:
        _terms_cache.clear()
    _terms_cache[cache_key] = terms
    return terms


def _compute_mention_terms(data: Any, countries: Sequence[str]) -> Tuple[str, ...]:
    """A term is the WHOLE name the client typed (every word, spacing and
    punctuation tolerant), never a shortened form: "Total Jobs" is the phrase
    ``total jobs``, not the word ``total``.  Only a TLD ("indeed.com") or a
    trailing country on a multi-country brand ("Indeed France") is dropped."""
    terms: List[str] = []
    for name in user_excluded(data):
        group = network_group_for_query(name)
        if group:  # a whole group: each member's own name, never an alias word
            for member in group["_members"]:
                term = _PAREN_RE.sub(" ", _fold(member["name"])).strip()
                if term and term not in _COMMON_WORDS:
                    terms.append(term)
            continue
        if not is_known_board(name):
            continue  # structured lists only; never a prose pattern
        base = _PAREN_RE.sub(" ", _fold(name)).strip()
        match = _DOMAIN_RE.match(base.replace(" ", ""))
        bare = _strip_tld(match.group(1)) if match else base
        words = [w for w in re.split(r"[^a-z0-9]+", bare) if w]
        first = words[0] if words else ""
        rest = words[1:]
        if (
            first in _MULTI_COUNTRY_BRANDS
            and first not in _COMMON_WORDS
            and _brand_rest_is_suffix(rest)
        ):
            # "Indeed France" / "indeed.com" / "Indeed Jobs" are the brand;
            # "Indeed Hiring Lab" / "Indeed Flex" are different things and
            # are matched by their full name only
            bare = first
        elif first in _COMMON_WORDS:
            bare = base  # "talent.com": the form the client typed
        if bare in _ORDINARY_WORD_BOARDS:
            bare = "!" + bare  # brand form only
        terms.append(bare)
    wanted = {canonical_country(c) for c in countries if c}
    for entry in _load_availability()["entries"]:
        status = entry.get("status") or ""
        if wanted:
            # Market-scoped: a board gone from the UK is still a Dallas board.
            # Plan-wide prose is cleaned only when EVERY plan market is covered.
            if not entry["_countries"] or not wanted <= entry["_countries"]:
                continue
        elif status == "exited" and entry["_countries"]:
            continue
        for nm in [entry["name"]] + list(entry.get("aliases") or []):
            terms.append(_PAREN_RE.sub(" ", _fold(nm)).strip())
    # a platform that runs in none of the plan's markets (ZipRecruiter on a
    # France plan) is not a recommendation either
    if wanted:
        for scope in _load_availability()["scopes"]:
            if not any(platform_runs_in(scope["name"], c) for c in wanted):
                terms.append(_fold(scope["name"]))
                terms.extend(_fold(a) for a in (scope.get("aliases") or []))
    return tuple(sorted({t for t in terms if t}))


def _brand_rest_is_suffix(rest: List[str]) -> bool:
    """True when what follows a brand's first word is only a country / region
    name, a TLD or a jobs / careers / hiring suffix (or a 2-letter code)."""
    i = 0
    while i < len(rest):
        for width in (3, 2, 1):
            if " ".join(rest[i : i + width]) in _COUNTRY_WORD_SET and i + width <= len(
                rest
            ):
                i += width
                break
        else:
            if rest[i] in _BRAND_REST_WORDS or (
                len(rest[i]) == 2 and rest[i].isalpha()
            ):
                i += 1
            else:
                return False
    return True


def _plan_markets(
    data: Any, country: Any, countries: Optional[Sequence[str]]
) -> List[str]:
    if countries is not None:
        return list(countries)
    if country:
        return [canonical_country(country)]
    return plan_countries(data)


# --- provenance is never a recommendation ----------------------------------
# A board named as the publisher / data source of a figure ("(Indeed Mobile
# Report 2025)", "Publisher: Indeed", "LinkedIn Benchmarks", "108,871 LinkedIn
# job postings") is a citation.  Excluding a board removes it as a
# recommendation; it never strips sources, attributions or the data they back.
_REPORT_NOUNS = (
    r"reports?|studies|study|surveys?|research|lab|index|benchmarks?|dataset|"
    r"data|postings?|solutions|insights?|performance|economic|economics|"
    r"salary\s+guide|white\s*paper|analysis|statistics|barometer"
)
_FOLLOW_CITATION_RE = re.compile(
    r"^[ \t]*(?:[a-z0-9&'\-]+[ \t]+){0,2}?(?:" + _REPORT_NOUNS + r")\b"
)
_LEAD_CITATION_RE = re.compile(
    r"(?:publisher|source|sources|according\s+to|data\s+from|based\s+on|citing)"
    r"\W+(?:\d[\d,.\s]*)?$"
)
_CITATION_PAREN_RE = re.compile(r"\b20\d\d\b|\b(?:" + _REPORT_NOUNS + r")\b")


def _is_citation(folded: str, start: int, end: int) -> bool:
    """True when the mention at ``folded[start:end]`` is a citation."""
    if _FOLLOW_CITATION_RE.match(folded[end : end + 48]):
        return True
    if _LEAD_CITATION_RE.search(folded[max(0, start - 48) : start]):
        return True
    open_at = folded.rfind("(", 0, start)
    if open_at != -1 and folded.find(")", open_at, start) == -1:
        close_at = folded.find(")", end)
        if close_at != -1 and _CITATION_PAREN_RE.search(folded[open_at:close_at]):
            return True
    return False


# a mention of a brand takes its national suffix with it ("Indeed France")
_COUNTRY_ALT = "|".join(
    re.escape(w) for w in sorted(_COUNTRY_WORDS, key=len, reverse=True)
)
_SPAN_TAIL_RE = re.compile(
    r"(?:[ \t]+(?:sponsored|jobs?|ads|advertising))*(?:[ \t]+(?:"
    + _COUNTRY_ALT
    + r"|[a-z]{2}\b))?",
    re.I,
)
_LIST_AFTER_RE = re.compile(r"[ \t]*(?:,(?!\d)|/|&|\band\b|\+)[ \t]*", re.I)
_LIST_BEFORE_RE = re.compile(r"(?:(?<!\d),|/|&|\band\b|\+)[ \t]*$", re.I)


def _spans(
    text: str, data: Any, markets: Sequence[str]
) -> Tuple[str, List[Tuple[int, int]]]:
    """(text the spans index into, unprotected mention spans)."""
    terms = _mention_terms(data, markets)
    pat = _mention_pattern(terms) if terms else None
    strict = _strict_pattern(terms) if terms else None
    if pat is None and strict is None:
        return text, []
    folded = _fold(text)
    if len(folded) != len(text):  # ligature etc: index the raw text instead
        folded = text.lower()
    # the client's own name is never edited ("Seek Staffing Co")
    protected: List[Tuple[int, int]] = []
    client = _fold((data or {}).get("client_name")) if isinstance(data, dict) else ""
    if client and len(client) >= 3:
        for cm in re.finditer(re.escape(client), folded):
            protected.append((cm.start(), cm.end()))
    found = (
        [(m.start(), m.end(), m.group(0)) for m in pat.finditer(folded)] if pat else []
    )
    if strict is not None:
        found += [
            (m.start(), m.end(), m.group(0).lower()) for m in strict.finditer(text)
        ]
    spans: List[Tuple[int, int]] = []
    for start, end0, matched in sorted(found):
        if any(ps <= start and end0 <= pe for ps, pe in protected):
            continue
        if _is_citation(folded, start, end0):
            continue
        end = end0
        tail = _SPAN_TAIL_RE.match(folded, end)
        if tail and re.match(r"[a-z0-9]+$", matched.replace(" ", "")):
            # only widen over a brand's own suffixes, never past punctuation
            first = re.split(r"[^a-z0-9]+", matched)[0]
            if first in _MULTI_COUNTRY_BRANDS:
                end = tail.end()
        spans.append((start, end))
    return folded, spans


def mentions_excluded(
    text: Any,
    data: Any = None,
    country: Any = None,
    countries: Optional[Sequence[str]] = None,
) -> bool:
    """True when free ``text`` RECOMMENDS or profiles a board the client
    excluded, an unavailable board, or a platform that does not run in the
    plan's markets (whole-phrase, spacing/punctuation tolerant).  A board named
    as the source of a figure is a citation, not a mention."""
    if not isinstance(text, str) or not text:
        return False
    return bool(_spans(text, data, _plan_markets(data, country, countries))[1])


def _remove_listed(text: str, spans: List[Tuple[int, int]]) -> Optional[str]:
    """Remove each span together with one list separator ('Indeed, Reed' ->
    'Reed'; 'A / B' -> 'B'; '60,000 across Indeed and Reed' -> '60,000 across
    Reed').  A member must sit beside another capitalised member; None when a
    span is not a list member (the whole clause is then about the board)."""
    out = text
    for start, end in sorted(spans, reverse=True):
        after = _LIST_AFTER_RE.match(out, end)
        if after and out[after.end() : after.end() + 1][:1].isupper():
            out = out[:start] + out[after.end() :]
            continue
        before = _LIST_BEFORE_RE.search(out[:start])
        if before and out[: before.start()].rstrip()[-1:].isalnum():
            out = out[: before.start()].rstrip() + out[end:]
            continue
        return None
    return re.sub(r"[ \t]{2,}", " ", out).strip(" ,;")


def scrub_text(
    text: Any,
    data: Any = None,
    country: Any = None,
    countries: Optional[Sequence[str]] = None,
) -> str:
    """``text`` without what recommends an excluded / unavailable board: the
    member of a '/' compound or comma list that names it (the rest of the
    list stays), else the sentence / ';' clause about it.  Citations and
    sources are never touched.  Text that names none is returned unchanged."""
    if not isinstance(text, str):
        return text
    markets = _plan_markets(data, country, countries)
    folded, spans = _spans(text, data, markets)
    if not spans:
        return text
    if len(folded) == len(text):
        listed = _remove_listed(text, spans)
        if listed is not None and listed:
            # only when removing the members leaves no mention behind
            if not _spans(listed, data, markets)[1]:
                return listed
    kept: List[str] = []
    for part in re.split(r"(?<=[.;])\s+", text):
        _, part_spans = _spans(part, data, markets)
        if not part_spans:
            kept.append(part)
            continue
        listed_part = _remove_listed(part, part_spans) if len(part) > 0 else None
        if listed_part and not _spans(listed_part, data, markets)[1]:
            kept.append(listed_part)
        elif len(part) > 160:
            # smallest span: in a long sentence drop only the comma clauses
            # that name the board, not the whole sentence
            pieces = re.split(r"(?<!\d),\s+(?!\d)", part)
            survivors = [x for x in pieces if not _spans(x, data, markets)[1]]
            if survivors:
                tail = "." if part.rstrip().endswith(".") else ""
                kept.append(", ".join(x.rstrip(".") for x in survivors) + tail)
    return " ".join(kept).strip().rstrip(";").strip()


def sweep_text(
    text: Any, data: Any = None, countries: Optional[Sequence[str]] = None
) -> str:
    """Final-pass cleanup of one client-facing string (see :func:`scrub_text`):
    recommendations of an excluded board go, citations stay."""
    return scrub_text(text, data, countries=countries)


def has_exclusions(data: Any, countries: Optional[Sequence[str]] = None) -> bool:
    """True when anything could be swept for this plan (a client exclusion,
    an availability entry covering all its markets, or an out-of-market
    platform)."""
    markets = _plan_markets(data, None, countries)
    return bool(_mention_terms(data, markets))


def next_allowed(
    candidates: Sequence[Any], data: Any = None, country: Any = None
) -> Optional[Any]:
    """First candidate that is not excluded (the substitute for an excluded
    category representative), else None."""
    for cand in candidates:
        if not is_excluded(cand, data, country):
            return cand
    return None


def disclosure_line(data: Any) -> str:
    """'Excluded at client request: A, B' -- '' when the client excluded
    nothing.  Permanent availability exclusions are never disclosed."""
    names = user_excluded(data)
    if not names:
        return ""
    return "Excluded at client request: " + ", ".join(names)


# ---------------------------------------------------------------------------
# Market scope (a platform that does not run in a market)
# ---------------------------------------------------------------------------


def platform_runs_in(name: Any, country: Any) -> bool:
    """False when ``name`` is a platform with a declared market scope that
    does not include ``country``.  Unknown platforms and unknown countries
    return True (fail open)."""
    want = canonical_country(country)
    if not want:
        return True
    keys = set(_keys_for(name))
    for scope in _load_availability()["scopes"]:
        if keys & scope["_keys"]:
            exited = availability_for(name, want)
            if exited is not None:
                return False
            return want in scope["_countries"]
    return True


_INTL_NAME_ALIASES = {
    "turkiye": "Turkey",
    "t\u00fcrkiye": "Turkey",
    "viet nam": "Vietnam",
}


def _dataset_countries() -> Dict[str, Dict[str, Any]]:
    """Country display name -> entry of data/international_benchmarks_2026.json
    (38 markets, 8 of which -- Turkey, Romania, Peru, Nigeria, Saudi Arabia,
    Taiwan, Bangladesh, UAE -- have no research.COUNTRY_DATA row)."""
    try:
        import research

        countries = research._load_intl_benchmarks().get("countries", {})
    except (ImportError, AttributeError, TypeError):
        return {}
    return {
        str(v.get("name") or k): v for k, v in countries.items() if isinstance(v, dict)
    }


def detect_country(text: Any) -> Optional[str]:
    """Country a location string names, or None.  research._detect_country
    first (it knows the US metro/state pitfalls), then the benchmark dataset's
    own country names, so 'Istanbul, Turkey' is Turkey and not an unresolved
    place that downstream code reads as the US."""
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        import plan_geo

        if plan_geo.location_is_us(raw) is True:  # "New Mexico" is a state
            return None
    except (ImportError, AttributeError, TypeError, ValueError):
        pass
    try:
        import research

        found = research._detect_country(raw)
    except (ImportError, AttributeError, TypeError, ValueError):
        found = None
    if found:
        return str(found)
    folded = _fold(raw)
    tail = folded.rsplit(",", 1)[-1].strip()
    for alias, name in _INTL_NAME_ALIASES.items():
        if tail == _fold(alias):
            return name
    for name in _dataset_countries():
        if tail == _fold(name) or re.search(rf"\b{re.escape(_fold(name))}\b", folded):
            return name
    return None


def dataset_platform_names(country: Any) -> List[str]:
    """Platform names the benchmark dataset lists for ``country`` (rank
    order), [] when it has none -- the board source for markets that have no
    research.COUNTRY_DATA row (Turkey, Peru, Romania, ...)."""
    want = canonical_country(country)
    for name, entry in _dataset_countries().items():
        if canonical_country(name) == want:
            return [
                str(p["name"])
                for p in (entry.get("platforms") or [])
                if isinstance(p, dict) and p.get("name")
            ]
    return []


def plan_countries(data: Any) -> List[str]:
    """Canonical country names of the plan's markets ('United States' for US
    locations), in plan order, deduplicated.  Locations that resolve to no
    country ('Remote') contribute nothing."""
    if not isinstance(data, dict):
        return []
    try:
        import plan_geo

        detect = detect_country
        is_us = plan_geo.location_is_us
    except (ImportError, AttributeError):
        return []
    raw_locations = data.get("locations") or []
    if not isinstance(raw_locations, (list, tuple)):
        raw_locations = [raw_locations]
    countries: List[str] = []
    for raw in raw_locations:
        text = raw if isinstance(raw, str) else str((raw or {}).get("country") or "")
        if not text.strip():
            continue
        try:
            if is_us(raw) is True:
                country: Optional[str] = "United States"
            else:
                country = detect(text)
        except (TypeError, ValueError, AttributeError):
            continue
        if country and country not in countries:
            countries.append(country)
    return countries


def plan_has_us_market(data: Any) -> bool:
    """True when at least one of the plan's own locations is in the US (the
    deck's and workbook's shared gold_standard reading -- a London plan left
    on the wizard's default 'US Only' region is NOT a US-market plan)."""
    try:
        import gold_standard

        return bool(gold_standard.plan_market_presence(data)[0])
    except (ImportError, AttributeError, TypeError):
        return True


# ---------------------------------------------------------------------------
# Relationship status (Joveo supply repository)
# ---------------------------------------------------------------------------


def _repo_index() -> Dict[str, List[Tuple[frozenset, str]]]:
    global _repo_index_cache, _repo_date_cache
    with _lock:
        if _repo_index_cache is not None:
            return _repo_index_cache
        index: Dict[str, List[Tuple[frozenset, str]]] = {}
        try:
            with open(_REPOSITORY_PATH, encoding="utf-8") as fh:
                repo = json.load(fh)
            _repo_date_cache = str(repo.get("last_updated") or "")[:10]
            for region in (repo.get("by_region") or {}).values():
                for pub in region.get("publishers") or []:
                    key = normalize(pub.get("name"))
                    if not key:
                        continue
                    raw = str(pub.get("countries") or "")
                    if raw.strip().lower() in ("", "nan"):
                        countries: frozenset = frozenset()
                    else:
                        countries = frozenset(
                            canonical_country(c) for c in raw.split(",") if c.strip()
                        )
                    index.setdefault(key, []).append(
                        (countries, str(pub.get("status") or ""))
                    )
        except (OSError, ValueError, AttributeError) as exc:
            logger.error("supply repository unreadable: %s", exc, exc_info=True)
        _repo_index_cache = index
        return index


# Supply-record states for one board in one country.
SUPPLY_IN_NETWORK = "in_network"
SUPPLY_NOT_IN_NETWORK = "not_in_network"
SUPPLY_NO_RECORD = "no_record"
SUPPLY_UNAVAILABLE = "unavailable"
_IN_NETWORK_STATUS = "In Joveo Network"
UNAVAILABLE_STATUS = "Not available through Joveo"
EXITED_STATUS = "No longer operating in this market"

# The one-line key to the supply statuses, in plain words (workbook, help page).
SUPPLY_STATUS_KEY = (
    "Supply status key: In Joveo Network = confirmed as in the Joveo network. "
    "VONQ Only / Via Redwood = listed, but not in the Joveo network. "
    "Relationship unverified = the supply repository has no row for this "
    "board in this market. Not available through Joveo = never shown in a plan."
)

# Inside a parenthesis, "A / B" names two boards ("Recruit (Rikunabi / Indeed
# Japan)"); a single parenthetical ("(France Travail)") is only a note.
_PAREN_INNER_RE = re.compile(r"\(([^)]*)\)")

# The repository's own ``category`` values that are job-board listings.  Every
# other value (DSP, Social Media, Influencer Marketing, Programmatic Audio, AI
# tool, Data Partner, Staffing Partner / Agency, Community Hiring) is not a job
# board, and a row with no category ("nan") cannot be classified, so neither is
# counted.
_JOB_BOARD_CATEGORIES = frozenset(
    {
        "Universal",
        "University Job Board",
        "DEI",
        "Govt",
        "Health",
        "Tech",
        "Classifieds",
        "Free",
        "Veterinary",
    }
)

SELECTION_NOTE = "Boards shown are chosen by market share, not matched to the role."


def repository_date() -> str:
    """The supply repository's own ``last_updated`` date ('' when unreadable).
    This is when the full listing was last refreshed -- NOT when any single
    board's status was last confirmed."""
    _repo_index()
    return _repo_date_cache


def _compound_parts(name: Any) -> List[str]:
    """The boards a combined entry names: the ' / ' parts of "VDAB / Forem /
    Actiris", or the head and each piece of "Recruit (Rikunabi / Indeed
    Japan)".  [] for an ordinary single board (a lone parenthetical such as
    "(France Travail)" is a note, not a second board)."""
    text = str(name or "")
    slash = _parts(text)
    if slash:
        return slash
    for inner in _PAREN_INNER_RE.findall(text):
        pieces = [p.strip() for p in _SPLIT_RE.split(inner) if p and p.strip()]
        if len(pieces) >= 2:
            head = _PAREN_INNER_RE.sub(" ", text).strip()
            return ([head] if head else []) + pieces
    return []


def _record_keys(name: Any) -> List[str]:
    """Every repository key that identifies the single board ``name``: its own
    key(s) and every spelling in its alias group (a rename resolves to ONE
    record)."""
    members_of = _load_availability()["members_of"]
    expanded: List[str] = []
    for key in _keys_for(name):
        for candidate in [key] + members_of.get(key, []):
            if candidate not in expanded:
                expanded.append(candidate)
    return expanded


def _supply_notes_index() -> Dict[str, List[Tuple[frozenset, Dict[str, Any]]]]:
    """data/global_supply.json's per-country board notes, by canonical country:
    ``[(matcher keys, board entry)]``.  Those notes are web-research remarks
    (most carry the same "Confirmed part of Joveo network" sentence), not the
    supply repository's status; they are read only to surface a disagreement
    with it, never to override it."""
    global _supply_notes_cache
    group_of = _load_availability()["group_of"]  # before taking _lock
    with _lock:
        if _supply_notes_cache is not None:
            return _supply_notes_cache
        index: Dict[str, List[Tuple[frozenset, Dict[str, Any]]]] = {}
        try:
            with open(_SUPPLY_NOTES_PATH, encoding="utf-8") as fh:
                supply = json.load(fh)
            for country, entry in (supply.get("country_job_boards") or {}).items():
                want = canonical_country(country)
                for board in (entry or {}).get("boards") or []:
                    if not isinstance(board, dict) or not board.get("name"):
                        continue
                    keys = frozenset(
                        group_of.get(k, k) for k in _components(board["name"])
                    )
                    index.setdefault(want, []).append((keys, board))
        except (OSError, ValueError, AttributeError) as exc:
            logger.error("global_supply.json unreadable: %s", exc, exc_info=True)
        _supply_notes_cache = index
        return index


def _supply_note_entry(name: Any, country: str) -> Optional[Dict[str, Any]]:
    keys = set(_record_keys(name))
    if not keys:
        return None
    for member_keys, board in _supply_notes_index().get(country, ()):
        if keys & member_keys:
            return board
    return None


def board_supply_record(name: Any, country: Any = None) -> Dict[str, Any]:
    """What Joveo's own supply data says about ``name`` in ``country``.

    ``state`` is one of:

    * ``unavailable`` -- data/publisher_availability.json bars the board for
      the country (``status`` says so plainly, ``as_of`` is that file's date).
      Checked FIRST: nothing else in the data can make a barred board look
      available;
    * ``in_network`` -- a member of a board group Joveo has confirmed
      (data/joveo_network_groups.json), or the supply repository lists the
      board for the country (or as a global board) as "In Joveo Network";
    * ``not_in_network`` -- the repository lists it for the country, but not
      as "In Joveo Network" (``status`` carries the repository's own word,
      e.g. "VONQ Only");
    * ``no_record`` -- the repository has no row for this board in this
      country.  That is *unknown*, not "not available".

    A combined entry ("Recruit (Rikunabi / Indeed Japan)", "VDAB / Forem") is
    ``in_network`` only if EVERY board it names is; otherwise it is as
    unconfirmed as its weakest part.

    ``as_of`` is a date the data itself carries -- the barring entry's, the
    group's, or the repository's ``last_updated`` -- and is '' when there is
    no record.  Nothing here is estimated.  ``notes_confirmed`` is True when
    data/global_supply.json's notes call the board confirmed; it never changes
    ``state`` but is surfaced when it disagrees with the repository.  Names
    that are renames of each other (France Travail / Pole Emploi) resolve to
    the same record through the alias groups in
    data/publisher_availability.json.
    """
    text = str(name or "").strip()
    want = canonical_country(country)
    record: Dict[str, Any] = {
        "name": text,
        "country": want,
        "state": SUPPLY_NO_RECORD,
        "status": "",
        "as_of": "",
        "source": "",
        "parts": [],
        "notes_confirmed": False,
        "notes_as_of": "",
    }
    if not text:
        return record
    barred = availability_for(text, want)
    if barred:
        exited = barred.get("status") == "exited"
        record.update(
            state=SUPPLY_UNAVAILABLE,
            status=EXITED_STATUS if exited else UNAVAILABLE_STATUS,
            as_of=str(barred.get("as_of") or ""),
            source=str(barred.get("source") or "publisher availability"),
        )
        return record
    parts = _compound_parts(text)
    if parts:
        return _compound_record(record, parts, want)
    note = _supply_note_entry(text, want)
    if note:
        record["notes_confirmed"] = (
            "confirmed part of joveo network"
            in str(note.get("_verification_notes") or "").lower()
        )
        record["notes_as_of"] = str(note.get("_last_verified") or "")
    hit = network_member(text, want)
    if hit:
        record.update(
            state=SUPPLY_IN_NETWORK,
            status=str(hit["status"] or _IN_NETWORK_STATUS),
            as_of=str(hit["as_of"] or ""),
            source=str(hit["status_source"] or "network group"),
        )
        return record
    index = _repo_index()
    statuses: List[str] = []
    for key in _record_keys(text):
        for countries, status in index.get(key, ()):
            if not want or "Global" in countries or want in countries:
                statuses.append(status)
    if statuses:
        if _IN_NETWORK_STATUS in statuses:
            state, status = SUPPLY_IN_NETWORK, _IN_NETWORK_STATUS
        else:
            state = SUPPLY_NOT_IN_NETWORK
            status = " / ".join(sorted({s for s in statuses if s}))
        record.update(
            state=state,
            status=status,
            as_of=repository_date(),
            source="supply repository",
        )
    return record


def _compound_record(
    record: Dict[str, Any], parts: List[str], want: str
) -> Dict[str, Any]:
    """Fold the records of every board a combined entry names into one."""
    subs = [board_supply_record(part, want) for part in parts]
    record["parts"] = [{"name": s["name"], "state": s["state"]} for s in subs]
    states = {s["state"] for s in subs}
    if states == {SUPPLY_IN_NETWORK}:
        dates = [s["as_of"] for s in subs if s["as_of"]]
        record.update(
            state=SUPPLY_IN_NETWORK,
            status=_IN_NETWORK_STATUS,
            as_of=min(dates) if dates else "",  # the oldest: never overstate
            source="every board it names",
        )
    elif states == {SUPPLY_UNAVAILABLE}:
        record.update(state=SUPPLY_UNAVAILABLE, status=UNAVAILABLE_STATUS)
    elif SUPPLY_NO_RECORD in states:
        record.update(state=SUPPLY_NO_RECORD)
    else:
        listed = sorted(
            {
                s["status"]
                for s in subs
                if s["state"] != SUPPLY_IN_NETWORK and s["status"]
            }
        )
        record.update(
            state=SUPPLY_NOT_IN_NETWORK,
            status=" / ".join(listed),
            as_of=repository_date(),
            source="supply repository",
        )
    return record


def status_label(record: Dict[str, Any]) -> str:
    """The record's status with the date the data carries, e.g. 'In Joveo
    Network, as of 2026-10-17', 'VONQ Only, as of 2026-03-31' or 'Not
    available through Joveo, as of 2026-10-16'.  '' for a board with no record
    (it has neither a status nor a date to show).  The date always follows the
    status: a bare date on a VONQ-only board would read as 'available'."""
    rec = record or {}
    if rec.get("state") == SUPPLY_NO_RECORD:
        return ""
    status = str(rec.get("status") or "").strip()
    as_of = str(rec.get("as_of") or "").strip()
    if not status:
        return ""
    return f"{status}, as of {as_of}" if as_of else status


def unverified_note(record: Dict[str, Any]) -> str:
    """The plain-words reason a board is not confirmed, '' when it is.  When
    data/global_supply.json's notes call the board confirmed but the
    repository disagrees, both are said -- neither is silently preferred."""
    rec = record or {}
    state = rec.get("state")
    if state == SUPPLY_IN_NETWORK:
        return ""
    if state == SUPPLY_UNAVAILABLE:
        return status_label(rec)
    country = str(rec.get("country") or "") or "this country"
    if rec.get("parts"):
        base = f"not every board it names is confirmed for {country}"
    elif state == SUPPLY_NOT_IN_NETWORK:
        base = f"repository lists it as {rec.get('status') or 'not in network'}"
    else:
        base = f"repository has no row for {country}"
    said = "supply notes list it as confirmed; " if rec.get("notes_confirmed") else ""
    return f"relationship unverified for this country ({said}{base})"


def supply_status_phrase(record: Dict[str, Any]) -> str:
    """One plain clause for a board's supply record: 'VietnamWorks: In Joveo
    Network, as of 2026-03-31', 'APEC: VONQ Only / Via Redwood, as of
    2026-03-31 (relationship unverified)', 'OCC Mundial: Not available through
    Joveo, as of 2026-10-16', or the relationship-unverified reason for a board
    the repository has no row for.  Dates are the ones the data carries."""
    name = str((record or {}).get("name") or "")
    state = (record or {}).get("state")
    label = status_label(record)
    if state in (SUPPLY_IN_NETWORK, SUPPLY_UNAVAILABLE) and label:
        return f"{name}: {label}"
    if state == SUPPLY_NOT_IN_NETWORK and label:
        said = (
            "; supply notes list it as confirmed"
            if (record or {}).get("notes_confirmed")
            else ""
        )
        return f"{name}: {label} (relationship unverified{said})"
    return f"{name}: {unverified_note(record)}"


def relationship_unverified(name: Any, country: Any = None) -> bool:
    """True unless the supply data CONFIRMS the board in ``country``: the
    repository lists it as 'In Joveo Network', or it is a member of a board
    group Joveo has confirmed (data/joveo_network_groups.json).  A combined
    entry is confirmed only if every board it names is.

    That covers a board the repository lists for the country but not as
    in-network, a board the repository has no row for (an unrecorded
    relationship is unverified, not assumed) and a barred board.  Never
    invents a label the other way: an in-network record always wins -- except
    for a board data/publisher_availability.json bars."""
    if not str(name or "").strip():
        return False
    return board_supply_record(name, country)["state"] != SUPPLY_IN_NETWORK


# ---------------------------------------------------------------------------
# Board groups confirmed as in the Joveo network (data/joveo_network_groups.json)
# ---------------------------------------------------------------------------


def _load_network_groups() -> List[Dict[str, Any]]:
    global _network_cache
    avail = _load_availability()  # before taking _lock (it takes the same lock)
    group_of = avail["group_of"]
    with _lock:
        if _network_cache is not None:
            return _network_cache
        groups: List[Dict[str, Any]] = []
        try:
            with open(_NETWORK_PATH, encoding="utf-8") as fh:
                raw = json.load(fh)
            for grp in raw.get("groups") or []:
                if not isinstance(grp, dict) or not grp.get("name"):
                    continue
                members: List[Dict[str, Any]] = []
                for mem in grp.get("members") or []:
                    if not isinstance(mem, dict) or not mem.get("name"):
                        continue
                    names = [mem["name"]] + [
                        a for a in (mem.get("aliases") or []) if isinstance(a, str)
                    ]
                    keys: set = set()
                    for nm in names:
                        for part in [normalize(nm)] + [
                            normalize(p) for p in _parts(nm)
                        ]:
                            if part:
                                keys.add(group_of.get(part, part))
                    members.append(
                        {
                            **mem,
                            "_keys": frozenset(keys),
                            "_countries": frozenset(
                                canonical_country(c)
                                for c in (mem.get("countries") or [])
                            ),
                            "_lead_in": frozenset(
                                canonical_country(c) for c in (mem.get("lead_in") or [])
                            ),
                        }
                    )
                label_keys = {
                    _squash(n)
                    for n in [grp["name"]]
                    + [a for a in (grp.get("aliases") or []) if isinstance(a, str)]
                }
                groups.append(
                    {
                        **grp,
                        "_members": members,
                        "_labels": frozenset(label_keys - {""}),
                    }
                )
        except (OSError, ValueError, AttributeError) as exc:
            logger.error("joveo_network_groups.json unreadable: %s", exc, exc_info=True)
        _network_cache = groups
        return groups


def _network_hit(group: Dict[str, Any], member: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "group": group["name"],
        "group_id": group.get("id") or group["name"],
        "member": member["name"],
        "status": group.get("status") or "In Joveo Network",
        "as_of": group.get("as_of") or "",
        "status_source": group.get("status_source") or "",
        "member_source": member.get("source") or "",
        "countries": sorted(member["_countries"]),
    }


def network_member(name: Any, country: Any = None) -> Optional[Dict[str, Any]]:
    """The network-group record for ``name`` (in ``country``) when it is a
    member of a board group Joveo has confirmed as in its network, else None.

    A member is scoped to its countries (``Jobs.ie`` is Ireland's, never
    ``Jobs.ch``); with no country known a member whose key is an ordinary word
    ("Jobs.ie" -> "jobs") is matched only by its exact name."""
    text = str(name or "").strip()
    if not text:
        return None
    keys = set(_keys_for(text))
    if not keys:
        return None
    want = canonical_country(country)
    ordinary = {normalize(w) for w in _STOPWORDS}
    for group in _load_network_groups():
        for member in group["_members"]:
            hit = keys & member["_keys"]
            if not hit:
                continue
            if want:
                if member["_countries"] and want not in member["_countries"]:
                    continue
            elif hit <= ordinary and _fold(text) not in {
                _fold(n)
                for n in [member["name"]]
                + [a for a in (member.get("aliases") or []) if isinstance(a, str)]
            }:
                continue  # an ordinary-word key matches by exact name only
            return _network_hit(group, member)
    return None


def network_group_for_query(text: Any) -> Optional[Dict[str, Any]]:
    """The group a free-text query names as a whole ("Stepstone Group",
    "StepGroup"), else None."""
    key = _squash(text)
    if not key:
        return None
    for group in _load_network_groups():
        if key in group["_labels"]:
            return group
    return None


def network_member_names(group: Dict[str, Any], country: Any = None) -> List[str]:
    """Display names of a group's members (those operating in ``country``
    when one is given), in file order."""
    want = canonical_country(country)
    return [
        m["name"]
        for m in group["_members"]
        if not want or not m["_countries"] or want in m["_countries"]
    ]


def network_boards_for(country: Any) -> List[str]:
    """Every in-network group board that operates in ``country``."""
    out: List[str] = []
    for group in _load_network_groups():
        out.extend(network_member_names(group, country) if country else [])
    return out


def network_lead_boards(country: Any) -> List[str]:
    """General (non-niche) in-network group boards offered first in
    ``country``'s local-board list, in file order."""
    want = canonical_country(country)
    if not want:
        return []
    return [
        m["name"]
        for group in _load_network_groups()
        for m in group["_members"]
        if want in m["_lead_in"]
    ]


def network_search(term: Any, country: Any = None) -> List[str]:
    """Group-board names a free-text publisher search names: every member of a
    group queried by its own name, else the members (in ``country`` when one is
    given, anywhere when not) whose name or alias equals the term or, for a
    term of four letters or more, contains it."""
    text = _fold(term).strip()
    if len(text) < 3:
        return []
    group = network_group_for_query(text)
    if group:
        return network_member_names(group, country)
    want = canonical_country(country)
    found: List[str] = []
    for grp in _load_network_groups():
        for mem in grp["_members"]:
            if want and mem["_countries"] and want not in mem["_countries"]:
                continue
            labels = [_fold(mem["name"])] + [
                _fold(a) for a in (mem.get("aliases") or []) if isinstance(a, str)
            ]
            if any(text == lb or (len(text) >= 4 and text in lb) for lb in labels):
                found.append(mem["name"])
    return found


def network_note(hit: Optional[Dict[str, Any]]) -> str:
    """One neutral sentence saying a board's group is in the Joveo network."""
    if not hit:
        return ""
    return (
        f"{hit['member']} is part of {hit['group']}, whose job boards are in the "
        f"Joveo network ({hit['status_source'] or 'confirmed'}, as of {hit['as_of']})."
    )


def with_network_board(
    names: Sequence[Any], country: Any, data: Any = None, limit: int = 3
) -> List[str]:
    """The first ``limit`` boards of a market's list, with an in-network group
    board among them when the market has one.

    ``names`` is already cleaned (client exclusions / availability / renames
    applied).  When none of the first ``limit`` is a group member, the market's
    first lead group board that survives those same filters replaces the last
    slot (or fills a free one); a group board the list already holds beyond the
    cut is the one promoted.  A market whose list is empty gets its lead group
    boards.  The list never grows past ``limit`` and an excluded or
    unavailable board is never offered."""
    shown = [str(n) for n in names][:limit]
    want = canonical_country(country)
    if not want or limit <= 0:
        return shown
    if any(network_member(n, want) for n in shown):
        return shown
    candidates = [str(n) for n in list(names)[limit:] if network_member(n, want)]
    for lead in network_lead_boards(want):
        survivors = clean_names([lead], data, want)
        if survivors and not any(names_match(survivors[0], n) for n in shown):
            candidates.append(survivors[0])
    if not candidates:
        return shown
    if not names:  # no dataset boards at all: offer the market's lead boards
        return list(dict.fromkeys(candidates))[:limit]
    if len(shown) < limit:
        return shown + [candidates[0]]
    return shown[:-1] + [candidates[0]]


def annotate(names: Sequence[Any], country: Any = None) -> Tuple[List[str], bool]:
    """(names with a trailing dagger on unverified boards, any_marked)."""
    out: List[str] = []
    marked = False
    for name in names:
        text = str(name or "")
        if text and relationship_unverified(text, country):
            out.append(f"{text} {RELATIONSHIP_MARK}")
            marked = True
        else:
            out.append(text)
    return out, marked


# Boards whose benchmark market share is this close (percentage points) are
# peers: among peers, one with a confirmed in-network record is listed first.
EQUAL_RANK_SHARE_BAND = 5.0


def _dataset_shares(country: Any) -> Dict[str, float]:
    """Benchmark market share (%) by matcher key for the dataset's boards in
    ``country`` ({} when the dataset has none)."""
    want = canonical_country(country)
    for name, entry in _dataset_countries().items():
        if canonical_country(name) != want:
            continue
        shares: Dict[str, float] = {}
        for plat in entry.get("platforms") or []:
            if not isinstance(plat, dict) or not plat.get("name"):
                continue
            share = plat.get("market_share_pct")
            if isinstance(share, bool) or not isinstance(share, (int, float)):
                continue
            shares.setdefault(normalize(plat["name"]), float(share))
        return shares
    return {}


def supply_ordered(names: Sequence[Any], country: Any = None) -> List[str]:
    """``names`` with confirmed in-network boards ahead of their peers.

    Order stays the benchmark's market-share rank.  Only boards that are
    peers (benchmark share within :data:`EQUAL_RANK_SHARE_BAND` points of the
    first board of their run) are re-ordered among themselves, a board with a
    confirmed in-network record first.  Nothing is added or removed, so a
    market-leading board without a record stays exactly where the market puts
    it -- it is marked (by :func:`annotate`), never hidden."""
    listed = [str(n) for n in names]
    want = canonical_country(country)
    if not want or len(listed) < 2:
        return listed
    shares = _dataset_shares(want)
    runs: List[Dict[str, Any]] = []
    for idx, name in enumerate(listed):
        share = shares.get(normalize(name))
        run = runs[-1] if runs else None
        if (
            run is not None
            and share is not None
            and run["anchor"] is not None
            and abs(run["anchor"] - share) <= EQUAL_RANK_SHARE_BAND
        ):
            run["items"].append((idx, name))
        else:
            runs.append({"anchor": share, "items": [(idx, name)]})
    ordered: List[str] = []
    for run in runs:
        ranked = sorted(
            run["items"],
            key=lambda item: (
                (
                    0
                    if board_supply_record(item[1], want)["state"] == SUPPLY_IN_NETWORK
                    else 1
                ),
                item[0],
            ),
        )
        ordered.extend(name for _, name in ranked)
    return ordered


def local_boards_for(
    country: Any,
    data: Any = None,
    names: Optional[Sequence[Any]] = None,
    limit: int = 3,
) -> List[str]:
    """The local boards a plan names for ``country``: the benchmark dataset's
    boards (or ``names`` when the caller already holds that list) after client
    exclusions, availability, folded boards and renames, cut to ``limit`` with
    an in-network group board among them, in supply-aware order.  The ONE
    picker behind deck slide 5 and the workbook's Local Job Boards table."""
    want = canonical_country(country)
    source = list(names) if names is not None else dataset_platform_names(want)
    cleaned = clean_names(source, data, want)
    return supply_ordered(with_network_board(cleaned, want, data, limit), want)


def _repo_board_rows() -> List[Tuple[str, frozenset, str, str]]:
    """``[(board name, countries, status, category)]`` for every repository
    row that names at least one country -- the category is what lets a count
    tell a job board from an ad platform."""
    global _repo_board_rows_cache
    with _lock:
        if _repo_board_rows_cache is not None:
            return _repo_board_rows_cache
        rows: List[Tuple[str, frozenset, str, str]] = []
        try:
            with open(_REPOSITORY_PATH, encoding="utf-8") as fh:
                repo = json.load(fh)
            for region in (repo.get("by_region") or {}).values():
                for pub in region.get("publishers") or []:
                    name = str(pub.get("name") or "").strip()
                    raw = str(pub.get("countries") or "")
                    if not name or raw.strip().lower() in ("", "nan"):
                        continue
                    rows.append(
                        (
                            name,
                            frozenset(
                                canonical_country(c)
                                for c in raw.split(",")
                                if c.strip()
                            ),
                            str(pub.get("status") or ""),
                            str(pub.get("category") or ""),
                        )
                    )
        except (OSError, ValueError, AttributeError) as exc:
            logger.error("supply repository unreadable: %s", exc, exc_info=True)
        _repo_board_rows_cache = rows
        return rows


def _repo_network_boards(country: Any) -> Dict[str, str]:
    """``{folded repository name: matcher key}`` of the repository's job boards
    that are 'In Joveo Network' and NAME ``country`` (the country may be
    spelled 'UK', 'UAE', ... in the repository).  Global rows are not counted
    -- they are ad platforms, influencer tools and the few boards that run
    everywhere -- and neither is any row whose category is not a job-board
    category.  A name listed twice counts once."""
    want = canonical_country(country)
    if not want:
        return {}
    return {
        _fold(name): normalize(name)
        for name, countries, status, category in _repo_board_rows()
        if status == _IN_NETWORK_STATUS
        and category in _JOB_BOARD_CATEGORIES
        and want in countries
        and normalize(name)
    }


def supply_depth(country: Any, shown: Sequence[Any]) -> Optional[Dict[str, Any]]:
    """``{"country", "boards", "confirmed", "total"}`` for a market's shown list.

    * ``boards`` -- how many boards are shown;
    * ``confirmed`` -- how many of them the supply data confirms as in network,
      judged by the very record that decides each dagger (a global board such
      as Indeed counts: it runs everywhere);
    * ``total`` -- the number of repository rows that are in-network JOB
      BOARDS naming ``country`` (by the repository's own category, one per
      distinct name), or None when the repository names none (the clause is
      then left out, never guessed).

    None when there is no country or no board shown."""
    want = canonical_country(country)
    names = [str(n) for n in shown if str(n or "").strip()]
    if not want or not names:
        return None
    named = _repo_network_boards(want)
    return {
        "country": want,
        "boards": len(names),
        "confirmed": sum(
            1
            for name in names
            if board_supply_record(name, want)["state"] == SUPPLY_IN_NETWORK
        ),
        "total": len(named) or None,
    }


def supply_depth_line(groups: Sequence[Dict[str, Any]]) -> str:
    """One plain line under the local boards.  It says how many of the boards
    shown are confirmed in network (the same states that put a dagger on the
    rest), how many in-network job boards the repository names for the market
    in total (only where it can count them exactly), and that the boards are
    chosen by market share, not matched to the role.  '' when there are no
    markets."""
    if not groups:
        return ""
    depths = [g["depth"] for g in groups if isinstance(g.get("depth"), dict)]
    if not depths:
        return SELECTION_NOTE
    if len(depths) == 1:
        d = depths[0]
        noun = "board" if d["boards"] == 1 else "boards"
        verb = "is" if d["confirmed"] == 1 else "are"
        text = (
            f"Of the {d['boards']} {noun} shown, {d['confirmed']} {verb} "
            "confirmed in network (global boards included)."
        )
        if d["total"]:
            return (
                f"{text} The repository lists {d['total']} in-network job boards "
                f"for {d['country']} in total; boards shown are chosen by market "
                "share, not matched to the role."
            )
        return f"{text} {SELECTION_NOTE}"
    confirmed = "; ".join(
        f"{d['country']} {d['confirmed']} of {d['boards']}" for d in depths
    )
    text = f"Boards shown that are confirmed in network (global boards included): {confirmed}."
    totals = [d for d in depths if d["total"]]
    if totals:
        listed = "; ".join(f"{d['country']} {d['total']}" for d in totals)
        return (
            f"{text} In-network job boards the repository lists in total: {listed}. "
            f"{SELECTION_NOTE}"
        )
    return f"{text} {SELECTION_NOTE}"


def plan_market_boards(
    data: Any, max_markets: int = 3, max_boards: int = 3
) -> List[Dict[str, Any]]:
    """The plan's top local boards per non-US market, after client
    exclusions, availability, folded boards and renames:
    ``[{"country", "locations", "boards" (daggered unless the supply data
    confirms the board), "unverified", "records" (one supply record per
    board, with the date the data carries), "depth", "more"}]``.  ``more``
    counts further markets beyond ``max_markets`` (the caller says "+N more
    markets").  Empty for a US-only plan or when the dataset has no boards
    for a market."""
    if not isinstance(data, dict):
        return []
    raw_locations = data.get("locations") or []
    if not isinstance(raw_locations, (list, tuple)):
        raw_locations = [raw_locations]
    groups: Dict[str, Dict[str, Any]] = {}
    for raw in raw_locations:
        text = raw if isinstance(raw, str) else str((raw or {}).get("city") or "")
        if not text.strip():
            continue
        country = detect_country(text)
        if not country or country == "United States":
            continue
        country = canonical_country(country)
        names = local_boards_for(country, data, limit=max_boards)
        if not names:
            continue
        marked, flag = annotate(names, country)
        group = groups.setdefault(
            country,
            {
                "country": country,
                "locations": [],
                "boards": marked,
                "unverified": flag,
                "records": [board_supply_record(n, country) for n in names],
                "depth": supply_depth(country, names),
            },
        )
        group["locations"].append(text)
    ordered = list(groups.values())
    shown = ordered[:max_markets]
    for group in shown:
        group["more"] = 0
    if shown:
        shown[0]["more"] = len(ordered) - len(shown)
    return shown


def local_boards_selected(data: Any) -> bool:
    """False when the user turned off BOTH the Global and the Regional job-board
    channel categories: such a plan funds no job board, so neither the deck nor
    the workbook recommends local ones.  The Niche tick plays no part -- local
    boards are not niche boards.  The one rule for slide 5 and the workbook."""
    try:
        import gold_standard

        off = gold_standard.deselected_channel_categories(data)
    except (ImportError, AttributeError, TypeError):
        return True
    return not {"global_boards", "regional_boards"} <= set(off)


# ---------------------------------------------------------------------------
# Knowledge-base text: a barred board never appears as a recommendation
# ---------------------------------------------------------------------------

DROPPED = object()  # returned in place of a node that IS a barred board


def _country_of_key(key: Any) -> str:
    """The country a dict key / path segment names ('mexico', 'United_Kingdom',
    'japan', 'uk'), else ''.  Any country counts, not only the ones with a
    barred board: knowing a list is Japan's is what stops Japan's public
    'Hello Work' being read as the barred French HelloWork."""
    text = str(key or "").replace("_", " ").strip().lower()
    if not text:
        return ""
    if text in _COUNTRY_WORD_SET or text in _COUNTRY_ALIASES:
        return canonical_country(text)
    return ""


def country_in_path(path: Any) -> str:
    """The first country named by a search path such as
    'verticals.technology.by_country.mexico.top_platforms[0]', else ''."""
    for seg in re.split(r"[.\[\]]", str(path or "")):
        found = _country_of_key(seg)
        if found:
            return found
    return ""


def _barred_entry(name: Any, countries: Sequence[str]) -> Optional[Dict[str, Any]]:
    """The availability entry that bars the single board ``name`` in any of
    ``countries`` ('' = no country known).  Spellings like 'occ_mundial' are
    read as names; a name of three letters or fewer ('OCC') is only read as a
    board where a country is known, since on its own it could be anything."""
    text = str(name or "").strip()
    if not text or len(text) > 80:
        return None
    for form in (text, text.replace("_", " ")):
        for country in countries or [""]:
            if not country and len(normalize(form)) <= 3:
                continue
            entry = availability_for(form, country or None)
            if entry:
                return entry
    return None


def _barred_in_all(name: Any, countries: Sequence[str]) -> Optional[Dict[str, Any]]:
    """The bar on ``name`` when it holds in EVERY listed country: a profile of
    a board that runs in several markets (ZipRecruiter: US, Canada, UK) stays
    when only one of them is barred."""
    entries = [_barred_entry(name, [c]) for c in (countries or [""])]
    return entries[0] if entries and all(entries) else None


def _entry_countries(node: Dict[str, Any], ctx: str) -> List[str]:
    found: List[str] = []
    raw = node.get("countries", node.get("country"))
    items = raw if isinstance(raw, (list, tuple)) else str(raw or "").split(",")
    for item in items:
        text = str(item or "").strip()
        if text and text.lower() != "nan":
            found.append(canonical_country(text))
    return found or [ctx]


def _note_barred(
    removed: Optional[List[Dict[str, Any]]], entry: Dict[str, Any]
) -> None:
    if removed is None:
        return
    exited = entry.get("status") == "exited"
    status = EXITED_STATUS if exited else UNAVAILABLE_STATUS
    as_of = str(entry.get("as_of") or "")
    note = f"{status}, as of {as_of}" if as_of else status
    item = {"name": str(entry.get("name") or ""), "note": note}
    if item not in removed:
        removed.append(item)


def _strip_barred(node: Any, ctx: str, removed: Optional[List[Dict[str, Any]]]) -> Any:
    if isinstance(node, dict):
        label = node.get("name", node.get("platform"))
        if isinstance(label, str) and label.strip():
            entry = _barred_in_all(label, _entry_countries(node, ctx))
            if entry:
                _note_barred(removed, entry)
                return DROPPED
            parts = [
                p.strip() for p in re.split(r"\s+/\s+|\s*\|\s*", label) if p.strip()
            ]
            if len(parts) > 1:
                kept = []
                for part in parts:
                    barred = _barred_in_all(part, _entry_countries(node, ctx))
                    if barred:
                        _note_barred(removed, barred)
                    else:
                        kept.append(part)
                if not kept:
                    return DROPPED
                if len(kept) < len(parts):
                    field = "name" if "name" in node else "platform"
                    node = {**node, field: " / ".join(kept)}
        out: Dict[Any, Any] = {}
        changed = False
        for key, value in node.items():
            key_ctx = _country_of_key(key) or ctx
            entry = _barred_entry(key, [key_ctx]) if isinstance(key, str) else None
            if entry:
                _note_barred(removed, entry)
                changed = True
                continue
            new_value = _strip_barred(value, key_ctx, removed)
            if new_value is DROPPED:
                changed = True
                continue
            changed = changed or new_value is not value
            out[key] = new_value
        return out if changed else node
    if isinstance(node, list):
        kept_items: List[Any] = []
        changed = False
        for item in node:
            new_item = _strip_barred(item, ctx, removed)
            if new_item is DROPPED:
                changed = True
                continue
            changed = changed or new_item is not item
            kept_items.append(new_item)
        return kept_items if changed else node
    if isinstance(node, str):
        entry = _barred_entry(node, [ctx])
        if entry:
            _note_barred(removed, entry)
            return DROPPED
    return node


def without_barred_boards(
    node: Any, country: Any = None, removed: Optional[List[Dict[str, Any]]] = None
) -> Any:
    """``node`` (any JSON-like knowledge-base value) with every board the
    availability file bars -- in the country the data places it in -- taken
    out: list items, dict entries and profiles keyed by the board's name or id,
    and a barred part of a combined name.  A same-named board in another
    country is untouched.  Returns the node itself when nothing changed (never
    edits its input) and :data:`DROPPED` when ``node`` is itself a barred
    board.  ``removed`` collects ``{"name", "note"}`` for each board taken out,
    the note being the plain "Not available through Joveo, as of <date>"."""
    ctx = canonical_country(country) if country else ""
    return _strip_barred(node, ctx, removed)


def client_config() -> Dict[str, Any]:
    """The tables the wizard's JS port of :func:`normalize` needs (embedded in
    the page by wizard_inputs.page_config, never re-typed in JS) so the tag
    input can reject "Indeed France" after "indeed.com" as the same board."""
    return {
        "country_words": list(_COUNTRY_WORDS),
        "brands": sorted(_MULTI_COUNTRY_BRANDS),
        "brand_suffix": sorted(_BRAND_SUFFIX_WORDS),
        "generic": sorted(_GENERIC_WORDS),
        "tlds": list(_TLDS),
        "aliases": dict(_load_availability()["group_of"]),
        "stopwords": sorted(_STOPWORDS),
    }


def plan_market_board_gaps(data: Any) -> List[str]:
    """Non-US plan markets the benchmark dataset has no board data for
    (Spain, Italy, ...), in plan order -- the deck says so instead of silently
    leaving them off the market band."""
    if not isinstance(data, dict):
        return []
    raw_locations = data.get("locations") or []
    if not isinstance(raw_locations, (list, tuple)):
        raw_locations = [raw_locations]
    gaps: List[str] = []
    for raw in raw_locations:
        text = raw if isinstance(raw, str) else str((raw or {}).get("city") or "")
        country = detect_country(text) if text.strip() else None
        if not country or canonical_country(country) == "United States":
            continue
        country = canonical_country(country)
        if (
            not dataset_platform_names(country)
            # the market's group boards count only when the client has not
            # excluded them all (and none is unavailable)
            and not clean_names(network_lead_boards(country), data, country)
            and country not in gaps
        ):
            gaps.append(country)
    return gaps


def reset_caches() -> None:
    """Test hook: drop the lazily-loaded data."""
    global _availability_cache, _repo_index_cache, _known_cache, _network_cache
    global _repo_date_cache, _supply_notes_cache, _repo_board_rows_cache
    with _lock:
        _repo_date_cache = ""
        _supply_notes_cache = None
        _repo_board_rows_cache = None
        _network_cache = None
        _known_cache = None
        _availability_cache = None
        _repo_index_cache = None
        _terms_cache.clear()
        _mention_cache.clear()
