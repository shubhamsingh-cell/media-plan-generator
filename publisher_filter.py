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

# Words that do not change which board it is.
_GENERIC_WORDS = frozenset(
    {
        "sponsored", "jobs", "job", "ads", "ad", "advertising", "careers",
        "career", "com", "inc", "ltd", "llc", "gmbh", "limited", "corp",
        "the", "network",
    }
)  # fmt: skip

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

_DOMAIN_RE = re.compile(r"^(?:https?://)?(?:www\.)?([a-z0-9][a-z0-9\-]*(?:\.[a-z0-9\-]+)+)(?:[/?#].*)?$")
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
            logger.error("publisher_availability.json unreadable: %s", exc, exc_info=True)
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


def sanitize_excluded(raw: Any) -> List[str]:
    """Clean a request's ``excluded_publishers`` value into a list of unique
    display strings.  Accepts a list or a comma/newline separated string;
    drops blanks, over-long entries and duplicates (by matcher key); caps the
    count.  Never raises."""
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
        if not text or len(text) > MAX_NAME_CHARS:
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
        if key and key in ren["_keys"] and (
            not want or not ren["_countries"] or want in ren["_countries"]
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
    names: Iterable[Any], data: Any = None, country: Any = None
) -> List[str]:
    """The board list a surface may print: excluded / unavailable / folded
    boards removed (part-wise inside 'A / B' compounds), sourced renames
    applied, duplicates dropped, order preserved."""
    out: List[str] = []
    seen: set = set()
    for raw in names:
        text = str(raw or "").strip()
        if not text:
            continue
        parts = _parts(text)
        if parts:
            originals = [p.strip() for p in re.split(r"\s+/\s+|\s*\|\s*", text) if p.strip()]
            kept = clean_names(originals, data, country)
            text = " / ".join(kept)
        else:
            if is_excluded(text, data, country) or is_folded(text, country):
                continue
            text = display_name(text, country)
        key = normalize(text)
        if not text or (key and key in seen):
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
    pat = _mention_cache.get(terms)
    if pat is None:
        alts = []
        for term in terms:
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


def _trim_term(term: str) -> str:
    """'indeed france' -> 'indeed' (a trailing country/generic word never
    narrows a mention term)."""
    key = re.sub(r"[^a-z0-9 ]+", " ", term).split()
    while len(key) > 1 and key[-1] in (_COUNTRY_WORD_SET | _GENERIC_WORDS):
        key = key[:-1]
    return " ".join(key)


_terms_cache: Dict[Tuple[Any, ...], Tuple[str, ...]] = {}


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
    terms: List[str] = []
    for name in user_excluded(data):
        base = _PAREN_RE.sub(" ", _fold(name))
        match = _DOMAIN_RE.match(base.replace(" ", ""))
        terms.append(_strip_tld(match.group(1)) if match else base)
    wanted = {canonical_country(c) for c in countries if c}
    for entry in _load_availability()["entries"]:
        status = entry.get("status") or ""
        if wanted:
            if entry["_countries"] and not wanted & entry["_countries"]:
                continue
        elif status == "exited" and entry["_countries"]:
            continue
        terms.append(_fold(entry["name"]))
        terms.extend(_fold(a) for a in (entry.get("aliases") or []))
    # a platform that runs in none of the plan's markets (ZipRecruiter on a
    # France plan) is not a recommendation either
    if wanted:
        for scope in _load_availability()["scopes"]:
            if not any(platform_runs_in(scope["name"], c) for c in wanted):
                terms.append(_fold(scope["name"]))
                terms.extend(_fold(a) for a in (scope.get("aliases") or []))
    return tuple(sorted({t for t in (_trim_term(x) for x in terms) if t}))


def _plan_markets(data: Any, country: Any, countries: Optional[Sequence[str]]) -> List[str]:
    if countries is not None:
        return list(countries)
    if country:
        return [canonical_country(country)]
    return plan_countries(data)


def mentions_excluded(
    text: Any, data: Any = None, country: Any = None, countries: Optional[Sequence[str]] = None
) -> bool:
    """True when free ``text`` names a board the client excluded, a board
    that is unavailable, or a platform that does not run in the plan's
    markets (whole-word, spacing/punctuation tolerant).  Used to drop a
    knowledge-base sentence that recommends such a board; NOT run on
    report-publisher citations."""
    if not isinstance(text, str) or not text:
        return False
    terms = _mention_terms(data, _plan_markets(data, country, countries))
    if not terms:
        return False
    pat = _mention_pattern(terms)
    return bool(pat and pat.search(_fold(text)))


def scrub_text(
    text: Any, data: Any = None, country: Any = None, countries: Optional[Sequence[str]] = None
) -> str:
    """``text`` without the sentences / ';' clauses / comma-list items that
    name an excluded or unavailable board.  Text that names none is returned
    unchanged."""
    if not isinstance(text, str):
        return text
    markets = _plan_markets(data, country, countries)

    def hit(piece: str) -> bool:
        return mentions_excluded(piece, data, countries=markets)

    if not hit(text):
        return text
    kept: List[str] = []
    for part in re.split(r"(?<=[.;])\s+", text):
        if not hit(part):
            kept.append(part)
            continue
        body = part.rstrip(".;")
        items = [i.strip() for i in body.split(",")]
        if len(items) > 1 and all(len(i) <= 45 for i in items):
            survivors = [i for i in items if not hit(i)]
            if survivors:
                kept.append(", ".join(survivors))
    return " ".join(kept).strip().rstrip(";").strip()


def sweep_text(
    text: Any, data: Any = None, countries: Optional[Sequence[str]] = None
) -> str:
    """Final-pass cleanup of one client-facing string: a parenthetical that
    cites an excluded / unavailable board ('(Indeed Mobile Report 2025)') is
    removed, then any clause that still names one is scrubbed.  Unchanged
    when nothing matches."""
    if not isinstance(text, str) or not text:
        return text
    markets = _plan_markets(data, None, countries)
    if not mentions_excluded(text, data, countries=markets):
        return text
    out = re.sub(
        r"\s*\([^()]*\)",
        lambda m: "" if mentions_excluded(m.group(0), data, countries=markets) else m.group(0),
        text,
    )
    return scrub_text(out, data, countries=markets)


def has_exclusions(data: Any, countries: Optional[Sequence[str]] = None) -> bool:
    """True when anything could be swept for this plan (a client exclusion,
    an availability entry in its markets, or an out-of-market platform)."""
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


def plan_countries(data: Any) -> List[str]:
    """Canonical country names of the plan's markets ('United States' for US
    locations), in plan order, deduplicated.  Locations that resolve to no
    country ('Remote') contribute nothing."""
    if not isinstance(data, dict):
        return []
    try:
        import plan_geo
        import research  # lazy: heavy module, already loaded in the app

        detect = research._detect_country
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
            applies = (
                not want
                or "Global" in countries
                or want in countries
            )
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
    unverified), "unverified"}]``.  Empty for a US-only plan or when the
    research dataset has no boards for a market."""
    if not isinstance(data, dict):
        return []
    try:
        import research
    except ImportError:
        return []
    raw_locations = data.get("locations") or []
    if not isinstance(raw_locations, (list, tuple)):
        raw_locations = [raw_locations]
    groups: Dict[str, Dict[str, Any]] = {}
    for raw in raw_locations:
        text = raw if isinstance(raw, str) else str((raw or {}).get("city") or "")
        if not text.strip():
            continue
        try:
            info = research.get_location_info(text) or {}
        except (TypeError, ValueError, AttributeError, KeyError):
            continue
        country = info.get("country")
        if not country or country == "United States":
            continue
        names = clean_names(
            [
                p.get("name")
                for p in (info.get("intl_platforms") or [])
                if isinstance(p, dict) and p.get("name")
            ],
            data,
            country,
        )[:max_boards]
        if not names:
            continue
        marked, flag = annotate(names, country)
        group = groups.setdefault(
            country,
            {"country": country, "locations": [], "boards": marked, "unverified": flag},
        )
        group["locations"].append(text)
    return list(groups.values())[:max_markets]


def reset_caches() -> None:
    """Test hook: drop the lazily-loaded data."""
    global _availability_cache, _repo_index_cache
    with _lock:
        _availability_cache = None
        _repo_index_cache = None
        _terms_cache.clear()
        _mention_cache.clear()
