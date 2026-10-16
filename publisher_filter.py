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
        _availability_cache = {
            "group_of": group_of,
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
    global _repo_index_cache
    with _lock:
        if _repo_index_cache is not None:
            return _repo_index_cache
        index: Dict[str, List[Tuple[frozenset, str]]] = {}
        try:
            with open(_REPOSITORY_PATH, encoding="utf-8") as fh:
                repo = json.load(fh)
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


def relationship_unverified(name: Any, country: Any = None) -> bool:
    """True when the Joveo supply repository lists this board for the
    country but NOT as 'In Joveo Network'.  A board the repository does not
    list at all is unknown, not unverified -> False (never invent a label)."""
    want = canonical_country(country)
    index = _repo_index()
    found = False
    for key in _keys_for(name):
        for countries, status in index.get(key, ()):
            applies = not want or "Global" in countries or want in countries
            if not applies:
                continue
            found = True
            if status == "In Joveo Network":
                return False
    return found


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


def plan_market_boards(
    data: Any, max_markets: int = 3, max_boards: int = 3
) -> List[Dict[str, Any]]:
    """The plan's top local boards per non-US market, after client
    exclusions, availability, folded boards and renames:
    ``[{"country", "locations", "boards" (daggered when relationship is
    unverified), "unverified", "more"}]``.  ``more`` counts further markets
    beyond ``max_markets`` (the caller says "+N more markets").  Empty for a
    US-only plan or when the dataset has no boards for a market."""
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
        names = clean_names(dataset_platform_names(country), data, country)[:max_boards]
        if not names:
            continue
        marked, flag = annotate(names, country)
        group = groups.setdefault(
            country,
            {"country": country, "locations": [], "boards": marked, "unverified": flag},
        )
        group["locations"].append(text)
    ordered = list(groups.values())
    shown = ordered[:max_markets]
    for group in shown:
        group["more"] = 0
    if shown:
        shown[0]["more"] = len(ordered) - len(shown)
    return shown


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
        if not dataset_platform_names(country) and country not in gaps:
            gaps.append(country)
    return gaps


def reset_caches() -> None:
    """Test hook: drop the lazily-loaded data."""
    global _availability_cache, _repo_index_cache, _known_cache
    with _lock:
        _known_cache = None
        _availability_cache = None
        _repo_index_cache = None
        _terms_cache.clear()
        _mention_cache.clear()
