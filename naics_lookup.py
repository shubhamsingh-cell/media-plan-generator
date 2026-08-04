"""US NAICS 2022 industry lookup + search (S94, NAICS typeahead).

Loads ``data/naics_2022.json`` once at import (error-isolated per project
convention -- see load_channels_db() in app.py) and exposes:

- ``naics_lookup(code)``  -> single code record + resolved internal_key, or None
- ``naics_search(q, limit)`` -> ranked matches for the wizard's typeahead

Search matches on WORD BOUNDARIES (whole word > word prefix; a mid-word
substring is not a match), and bridges the recruiter's vocabulary to the
industry taxonomy through a curated occupation alias map -- NAICS titles
name industries, but the wizard asks users what they are hiring for, and
no NAICS title contains "nurse", "driver" or "cashier". See _ALIAS_GROUPS.

Resolution rule (per the data contract in data/naics_2022.json):
  1. Strip a ranged sector code (e.g. "31-33") to its first component ("31").
  2. Longest-prefix match against ``internal_key_map`` (checking progressively
     shorter prefixes of the numeric code).
  3. Fall back to ``default_internal_key`` if nothing matches.

Every one of the 2,125 codes is guaranteed to resolve to one of the 21/22
internal industry keys the benchmark stack keys off (shared_utils.INDUSTRY_LABEL_MAP)
-- this module never invents a new key and never returns an unresolved code.

Pure stdlib. No dependency on app.py (app.py imports this module, not the
other way around) so it stays free of circular-import risk.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_NAICS_PATH = os.path.join(_BASE_DIR, "data", "naics_2022.json")

_MAX_QUERY_LEN = 100

# Widest sector range expanded into its members (see _code_match_keys).
# The real dataset's widest is "31-33"; the cap only exists so a malformed
# regeneration can never blow up memory at import.
_MAX_RANGE_SPAN = 20

# Word tokenizer shared by the index builder and the query parser, so a
# title and a query are always split the same way.
_WORD_RE = re.compile(r"[a-z0-9]+")

# ── Token match strengths (higher = better) ───────────────────────────
# A query token matches a title word as a WHOLE WORD or as a WORD PREFIX.
# A mid-word substring is deliberately NOT a match at all: it is what made
# "rn" return "Corn Farming", "Furniture Retailers" and "International
# Affairs" (design panel 2026-07-31, mechanism lens).
_MATCH_NONE = 0
_MATCH_PREFIX = 1
_MATCH_WORD = 2

# ── Rank tiers (lower = better) ───────────────────────────────────────
_TIER_EXACT = 0  # exact code, or title identical to the query
_TIER_CODE_PREFIX = 1  # numeric query is a prefix of the code
_TIER_TITLE_PREFIX = 2  # title starts with the query, ending on a word boundary
_TIER_ALL_WORDS = 3  # every query token appears as a whole word in the title
_TIER_ALIAS = 4  # curated occupation -> industry alias (see below)
_TIER_ALL_PREFIXES = 5  # every query token is at least a word prefix

# ── Occupation -> industry aliases ────────────────────────────────────
# WHY THIS EXISTS (design panel 2026-07-31, mechanism lens VETO):
# NAICS is an *industry* taxonomy, but the wizard asks recruiters to type
# what they are hiring for -- step 2 ships an example chip reading "Nurse
# in LA". The corpus contains no occupation names at all: no NAICS title
# contains the word "nurse", or "driver", or "cashier". So the flagship
# recruitment queries could not match anything real, and fell through to
# accidental word-prefix collisions -- "nurse" prefixes "nursery", which
# is why q=nurse returned six horticulture codes and zero healthcare ones.
#
# Ranking alone cannot fix that (there is nothing correct to rank), and a
# morphological stemmer cannot either -- "CDL" -> trucking and "CNA" ->
# nursing care are semantic, not morphological. So this map bridges the
# two vocabularies explicitly: auditable, testable, and inert for any term
# it does not cover, which falls through to the ranking tiers unchanged.
#
# Rules for editing:
#   * Phrases are ORDERED -- the first one is the most on-point industry
#     and sorts first inside the alias tier.
#   * Every phrase must match a real NAICS title. test_naics_industry_select.py
#     asserts every key returns results, so a typo or an invented industry
#     fails the suite rather than silently shipping an empty typeahead.
#   * Only map an occupation when the industry genuinely employs it.
#     Deliberately NOT mapped: "welder" (the only welding title is
#     "Welding and Soldering Equipment Manufacturing" -- that industry
#     builds the equipment, it is not where welders are hired), and bare
#     "tech" (ambiguous between the technology sector and "pharmacy tech"
#     / "vet tech"; the compound forms are mapped instead, and bare "tech"
#     still word-prefix matches Technical/Technology titles as before).
_ALIAS_GROUPS: List[tuple] = [
    # ── Healthcare ────────────────────────────────────────────────────
    (
        ["nursing", "home health care", "hospitals"],
        [
            "nurse",
            "nurses",
            "nursing",
            "rn",
            "rns",
            "registered nurse",
            "registered nurses",
            "lpn",
            "lvn",
            "licensed practical nurse",
            "nurse practitioner",
            "nurse practitioners",
            "charge nurse",
            "staff nurse",
            "travel nurse",
            "travel nurses",
            "icu nurse",
            "er nurse",
        ],
    ),
    # CNAs work primarily in skilled-nursing facilities; home health aides
    # and caregivers primarily in home health. Same industries, different
    # order -- the alias order is what decides the top row, so these two
    # families stay separate rather than sharing one averaged ordering.
    (
        ["nursing", "residential care", "home health care", "assisted living"],
        [
            "cna",
            "cnas",
            "certified nursing assistant",
            "nursing assistant",
            "nursing assistants",
            "patient care technician",
        ],
    ),
    (
        ["home health care", "nursing", "residential care", "assisted living"],
        [
            "aide",
            "aides",
            "home health aide",
            "home health aides",
            "hha",
            "caregiver",
            "caregivers",
            "caretaker",
            "personal care aide",
            "direct support professional",
        ],
    ),
    (
        ["offices of physicians", "hospitals"],
        ["physician", "physicians", "doctor", "doctors", "surgeon", "surgeons"],
    ),
    (
        ["offices of dentists", "dental laboratories"],
        [
            "dentist",
            "dentists",
            "dental hygienist",
            "dental hygienists",
            "dental assistant",
            "dental assistants",
        ],
    ),
    (
        ["pharmacies", "pharmaceutical"],
        [
            "pharmacist",
            "pharmacists",
            "pharmacy technician",
            "pharmacy technicians",
            "pharmacy tech",
            "pharmacy techs",
        ],
    ),
    (
        ["therapists", "mental health", "hospitals"],
        [
            "therapist",
            "therapists",
            "physical therapist",
            "physical therapists",
            "occupational therapist",
            "occupational therapists",
            "speech therapist",
        ],
    ),
    (
        ["medical and diagnostic laboratories", "offices of physicians", "hospitals"],
        [
            "medical assistant",
            "medical assistants",
            "phlebotomist",
            "phlebotomists",
            "medical technologist",
            "lab tech",
            "lab technician",
            "radiology tech",
            "radiologic technologist",
        ],
    ),
    (["ambulance"], ["emt", "emts", "paramedic", "paramedics"]),
    (
        # NAICS 2022 renamed 6244 from "Child Day Care Services" to
        # "Child Care Services" -- the old phrase matched nothing and sent
        # q=daycare to "Elementary and Secondary Schools".
        ["child care services", "child and youth services"],
        [
            "daycare",
            "day care",
            "childcare",
            "child care",
            "preschool teacher",
            "daycare worker",
        ],
    ),
    # ── Transport & logistics ─────────────────────────────────────────
    (
        [
            "general freight trucking",
            "specialized freight",
            "couriers",
            "transit",
            "taxi",
        ],
        [
            "driver",
            "drivers",
            "truck driver",
            "truck drivers",
            "cdl",
            "cdl a",
            "cdl driver",
            "cdl drivers",
            "class a driver",
            "otr driver",
            "otr drivers",
            "delivery driver",
            "delivery drivers",
            "courier",
            "couriers",
            "bus driver",
            "bus drivers",
        ],
    ),
    (
        ["warehousing and storage", "couriers"],
        [
            "warehouse",
            "warehouse associate",
            "warehouse worker",
            "forklift",
            "forklift operator",
            "forklift operators",
            "picker",
            "packer",
            "picker packer",
            "material handler",
            "material handlers",
        ],
    ),
    (
        ["freight transportation arrangement", "general freight trucking"],
        ["dispatcher", "dispatchers", "logistics coordinator"],
    ),
    # ── Skilled trades ────────────────────────────────────────────────
    (
        ["electrical contractors"],
        ["electrician", "electricians", "electrical apprentice"],
    ),
    (
        ["plumbing, heating, and air-conditioning contractors"],
        [
            "plumber",
            "plumbers",
            "hvac",
            "hvac technician",
            "hvac technicians",
            "hvac tech",
            "pipefitter",
        ],
    ),
    (
        ["automotive repair", "automotive mechanical", "automobile dealers"],
        [
            "mechanic",
            "mechanics",
            "auto mechanic",
            "auto mechanics",
            "diesel mechanic",
            "diesel mechanics",
            "auto technician",
            "auto tech",
            "automotive technician",
        ],
    ),
    # ── Food service & hospitality ────────────────────────────────────
    (
        ["restaurants", "food service contractors", "drinking places", "caterers"],
        [
            "cook",
            "cooks",
            "line cook",
            "line cooks",
            "prep cook",
            "chef",
            "chefs",
            "server",
            "servers",
            "waiter",
            "waitress",
            "barista",
            "baristas",
            "dishwasher",
            "dishwashers",
            "busser",
            "food service worker",
        ],
    ),
    (
        ["hotels and motels", "traveler accommodation", "accommodation"],
        [
            "housekeeper",
            "housekeepers",
            "housekeeping",
            "front desk agent",
            "hotel staff",
        ],
    ),
    # ── Retail ────────────────────────────────────────────────────────
    (
        [
            "grocery and convenience retailers",
            "general merchandise retailers",
            "clothing and clothing accessories retailers",
        ],
        [
            "cashier",
            "cashiers",
            "retail associate",
            "retail associates",
            "sales associate",
            "sales associates",
            "store associate",
            "stocker",
            "stockers",
        ],
    ),
    # ── Everything else, high-frequency in recruitment ────────────────
    (
        ["elementary and secondary schools", "educational services"],
        [
            "teacher",
            "teachers",
            "substitute teacher",
            "substitute teachers",
            "tutor",
            "tutors",
            "paraprofessional",
        ],
    ),
    (
        ["security guards", "investigation and security"],
        [
            "security guard",
            "security guards",
            "security officer",
            "security officers",
        ],
    ),
    (
        ["janitorial", "services to buildings"],
        [
            "janitor",
            "janitors",
            "custodian",
            "custodians",
            "cleaner",
            "cleaners",
            "housekeeping aide",
        ],
    ),
    (
        ["accounting", "payroll"],
        [
            "accountant",
            "accountants",
            "bookkeeper",
            "bookkeepers",
            "payroll specialist",
            "staff accountant",
        ],
    ),
    (
        ["computer systems design", "software publishers"],
        [
            "software engineer",
            "software engineers",
            "software developer",
            "software developers",
            "developer",
            "developers",
            "programmer",
            "programmers",
            "data scientist",
            "devops engineer",
        ],
    ),
    (
        # NOT bare "construction": that word also appears in "Construction
        # Sand and Gravel Mining" and "Construction Machinery
        # Manufacturing", which do not employ construction laborers.
        [
            "residential building construction",
            "nonresidential building construction",
            "building finishing contractors",
            "building equipment contractors",
        ],
        [
            "laborer",
            "laborers",
            "construction worker",
            "construction workers",
            "carpenter",
            "carpenters",
            "roofer",
            "roofers",
        ],
    ),
]

# Populated at import time; stay empty (never raise) if the data file is
# missing or malformed so a bad/missing NAICS dataset never blocks plan
# generation or server startup.
_CODES: List[Dict[str, Any]] = []
_BY_CODE: Dict[str, Dict[str, Any]] = {}
_INTERNAL_KEY_MAP: Dict[str, str] = {}
_DEFAULT_INTERNAL_KEY: str = "general_entry_level"
_LOADED: bool = False

# Per-code search index, built once at import so the typeahead does not
# re-lowercase and re-tokenize 2,125 titles on every keystroke.
# Each row: (code, title, title_lower, words tuple, words set, level).
_SEARCH_ROWS: List[tuple] = []

# Code -> every literal a numeric query may match it by (see
# _code_match_keys). Kept beside _SEARCH_ROWS rather than inside a row so
# the row shape stays exactly what the alias-map tests unpack.
_CODE_KEYS: Dict[str, Tuple[str, ...]] = {}

# Normalized alias key -> ordered list of expansion phrases (token tuples).
_ALIAS_INDEX: Dict[str, List[tuple]] = {}


def _words(text: str) -> tuple:
    """Split text into lowercase alphanumeric word tokens."""
    return tuple(_WORD_RE.findall((text or "").lower()))


def _code_match_keys(code: str) -> Tuple[str, ...]:
    """Every literal a numeric query can legitimately match ``code`` by.

    A plain code stands only for itself. A ranged sector code ("31-33")
    also stands for each sector inside the inclusive range -- ("31-33",
    "31", "32", "33") -- because that is what the NAICS range means, and
    it is the expansion internal_key_map already spells out by hand
    (31/32/33 each map to one internal key). Without it a user typing
    "33" or "45" never sees the sector their code lives under.
    """
    if "-" not in code:
        return (code,)
    start, _, end = code.partition("-")
    if not (start.isdigit() and end.isdigit()) or len(start) != len(end):
        return (code, start) if start else (code,)
    lo, hi = int(start), int(end)
    if not 0 <= hi - lo <= _MAX_RANGE_SPAN:
        return (code, start, end)
    return (code,) + tuple(str(n).zfill(len(start)) for n in range(lo, hi + 1))


def _normalize_key(text: str) -> str:
    """Normalize an alias key / query for alias lookup ("CDL-A" -> "cdl a")."""
    return " ".join(_words(text))


def _build_alias_index() -> Dict[str, List[tuple]]:
    index: Dict[str, List[tuple]] = {}
    for phrases, terms in _ALIAS_GROUPS:
        expansions = [_words(p) for p in phrases]
        expansions = [e for e in expansions if e]
        for term in terms:
            key = _normalize_key(term)
            if key:
                index[key] = expansions
    return index


try:
    with open(_NAICS_PATH, "r", encoding="utf-8") as _f:
        _raw = json.load(_f)
    _CODES = _raw.get("codes") or []
    _INTERNAL_KEY_MAP = _raw.get("internal_key_map") or {}
    _DEFAULT_INTERNAL_KEY = _raw.get("default_internal_key") or "general_entry_level"
    _BY_CODE = {c["code"]: c for c in _CODES if isinstance(c, dict) and c.get("code")}
    _SEARCH_ROWS = []
    for _c in _CODES:
        if not isinstance(_c, dict):
            continue
        _title = _c.get("title") or ""
        _title_lower = _title.lower()
        _w = _words(_title_lower)
        _SEARCH_ROWS.append(
            (
                _c.get("code") or "",
                _title,
                _title_lower,
                _w,
                frozenset(_w),
                int(_c.get("level") or 0),
            )
        )
    _CODE_KEYS = {_code: _code_match_keys(_code) for _code in _BY_CODE}
    _ALIAS_INDEX = _build_alias_index()
    _LOADED = True
    logger.info(
        "Loaded %d NAICS 2022 codes (%s), %d occupation aliases",
        len(_CODES),
        _raw.get("version", "?"),
        len(_ALIAS_INDEX),
    )
except (FileNotFoundError, json.JSONDecodeError, OSError, KeyError, TypeError) as e:
    logger.error("Failed to load data/naics_2022.json: %s", e, exc_info=True)
    _CODES = []
    _BY_CODE = {}
    _INTERNAL_KEY_MAP = {}
    _DEFAULT_INTERNAL_KEY = "general_entry_level"
    _SEARCH_ROWS = []
    _CODE_KEYS = {}
    _ALIAS_INDEX = {}
    _LOADED = False


def _first_component(code: str) -> str:
    """Strip a ranged sector code ("31-33") down to its first component ("31")."""
    return code.split("-")[0] if "-" in code else code


def resolve_internal_key(code: str) -> str:
    """Resolve a NAICS code to one of the 21 internal industry keys.

    Longest-prefix match against internal_key_map; falls back to
    default_internal_key. Never raises, never returns an unmapped key.
    """
    if not code:
        return _DEFAULT_INTERNAL_KEY
    stripped = _first_component(str(code).strip())
    digits = re.sub(r"[^0-9]", "", stripped)
    if not digits:
        return _DEFAULT_INTERNAL_KEY
    for prefix_len in range(len(digits), 0, -1):
        prefix = digits[:prefix_len]
        if prefix in _INTERNAL_KEY_MAP:
            return _INTERNAL_KEY_MAP[prefix]
    return _DEFAULT_INTERNAL_KEY


def naics_lookup(code: str) -> Optional[Dict[str, Any]]:
    """Return {code, title, level, internal_key} for an exact NAICS code, or None."""
    if not code:
        return None
    record = _BY_CODE.get(str(code).strip())
    if not record:
        return None
    return {
        "code": record["code"],
        "title": record.get("title", ""),
        "level": record.get("level"),
        "internal_key": resolve_internal_key(record["code"]),
    }


def _token_strength(token: str, words: tuple, words_set: frozenset) -> int:
    """How well a query token matches a title: whole word > word prefix > none.

    A mid-word substring is NOT a match -- see _MATCH_NONE above.
    """
    if token in words_set:
        return _MATCH_WORD
    for w in words:
        if w.startswith(token):
            return _MATCH_PREFIX
    return _MATCH_NONE


def _phrase_strength(tokens: tuple, words: tuple, words_set: frozenset) -> int:
    """Weakest strength across all tokens; _MATCH_NONE if any token misses.

    Every token must match (AND semantics, as before), so the phrase is
    only as strong as its weakest token.
    """
    weakest = _MATCH_WORD
    for t in tokens:
        s = _token_strength(t, words, words_set)
        if s == _MATCH_NONE:
            return _MATCH_NONE
        if s < weakest:
            weakest = s
    return weakest


def _query_expansions(q_key: str, tokens: tuple) -> List[tuple]:
    """Ordered alias expansions for a query, most on-point phrase first.

    The whole query is looked up first so a specific multi-word alias
    ("truck driver" -> trucking) wins over the union of its parts
    ("truck" + "driver"). Only if the whole query is not an alias do the
    individual tokens get expanded.
    """
    if q_key in _ALIAS_INDEX:
        return _ALIAS_INDEX[q_key]
    expansions: List[tuple] = []
    seen: set = set()
    for t in tokens:
        for phrase in _ALIAS_INDEX.get(t, ()):
            if phrase not in seen:
                seen.add(phrase)
                expansions.append(phrase)
    return expansions


def _alias_order(
    expansions: List[tuple], words: tuple, words_set: frozenset
) -> Optional[int]:
    """Index of the first alias expansion this title matches, or None."""
    for i, phrase in enumerate(expansions):
        if _phrase_strength(phrase, words, words_set) != _MATCH_NONE:
            return i
    return None


def _starts_on_word_boundary(title_lower: str, q_lower: str) -> bool:
    """Title begins with the query AND the query ends on a word boundary.

    Without the boundary check, "nurse" counts as a title-prefix match for
    "Nursery and Tree Production" -- the exact defect that put six
    horticulture codes above every healthcare code for q=nurse.
    """
    if not title_lower.startswith(q_lower):
        return False
    tail = title_lower[len(q_lower) :]
    return not tail or not tail[0].isalnum()


def naics_search(q: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Ranked NAICS search for the wizard typeahead.

    Rank tiers (lower = better): 0 exact code or exact title, 1 code
    prefix, 2 title starts with the query on a word boundary, 3 every
    query token is a whole word in the title, 4 curated occupation alias
    (see _ALIAS_GROUPS), 5 every query token is a word prefix. Ties are
    broken by alias order, then deeper (6-digit) codes, then shorter
    titles.

    The alias tier sits ABOVE the word-prefix tier on purpose. A curated
    "nurse means nursing" is a deliberate statement of intent; "nurse
    happens to prefix nursery" is an accident of spelling, and ranking the
    accident higher is what made q=nurse return only horticulture codes.
    No intent heuristic is involved and nothing is suppressed -- the
    horticulture rows still appear for q=nurse, just below the nursing
    ones.

    Ranged sector codes (2026-08-02 fix): three sectors are stored with
    a hyphen -- "31-33" Manufacturing, "44-45" Retail Trade, "48-49"
    Transportation and Warehousing. A code query containing an internal
    hyphen is matched literally rather than as concatenated digits;
    collapsing "31-33" to "3133" used to hit the unrelated 4-digit
    Textile Mills code and miss the sector entirely, and "44-45"/"48-49"
    matched nothing at all. Any member of a range ("31", "32", "33") is
    an exact hit on the sector, so a 2-digit query surfaces its sector
    row first -- the contract "54" already had for un-ranged sectors,
    which the -level tie-break otherwise sorted last.

    Dedupe (design panel 2026-07-31, iteration 2, mechanism finding):
    NAICS titles frequently repeat verbatim across a parent code and its
    lone child (e.g. 92213 "Legal Counsel and Prosecution" duplicates
    922130's title; 54111 "Offices of Lawyers" duplicates 541110's) --
    the parent exists in the standard purely as an aggregation bucket
    with one child, so showing both rows in a typeahead is pure noise.
    When two matches in the same result set share an exact title, only
    the deepest (highest-level, e.g. 6-digit) row is kept. Rows with
    genuinely distinct titles are never affected.
    """
    q = (q or "").strip()[:_MAX_QUERY_LEN]
    if not q or not _SEARCH_ROWS:
        return []
    try:
        limit = max(1, min(int(limit), 50))
    except (TypeError, ValueError):
        limit = 20

    q_lower = q.lower()
    q_digits = re.sub(r"[^0-9]", "", q)
    tokens = _words(q_lower)
    q_key = " ".join(tokens)
    # Only treat the query as a code lookup when it is ENTIRELY numeric.
    # Testing `q_digits` alone made "top 40 retail" a prefix search for
    # every code starting "40".
    q_is_code = bool(q_digits) and not any(ch.isalpha() for ch in q_lower)
    # Literal form of a code query: "31 - 33" and a pasted en-dash
    # "31<U+2013>33" both normalize to "31-33"; leading/trailing hyphens
    # are noise, so only an *internal* hyphen marks a range query, which
    # is then matched literally instead of being concatenated into the
    # unrelated code "3133".
    #
    # An internal hyphen commits the query to this literal reading for
    # good -- there is no fallback to q_digits once one is present, even
    # mid-range ("31-3" matches only the "31-33" prefix, not "313"
    # Textile Mills). Blending the two readings back together on a
    # partial query is not actually free: digit-stripping the *complete*
    # query "31-33" also produces "3133", a real code -- so any rule
    # that re-admits q_digits once a hyphen is present either resurrects
    # this exact bug for the complete-range case, or needs to special-
    # case "complete" vs. "partial" range to avoid it. Both cost more
    # than the one keystroke of typeahead gap this trades away.
    q_code = re.sub(r"[\u2010-\u2015]", "-", re.sub(r"\s+", "", q)).strip("-")
    q_num = q_code if "-" in q_code else q_digits
    expansions = _query_expansions(q_key, tokens)

    scored: List[tuple] = []
    for code, title, title_lower, words, words_set, level in _SEARCH_ROWS:
        tier: Optional[int] = None
        alias_order = 0
        code_keys = _CODE_KEYS.get(code) or (code,)

        if q_is_code and q_num in code_keys:
            tier = _TIER_EXACT
        elif title_lower == q_lower:
            tier = _TIER_EXACT
        elif q_is_code and any(k.startswith(q_num) for k in code_keys):
            tier = _TIER_CODE_PREFIX
        elif tokens:
            strength = _phrase_strength(tokens, words, words_set)
            if strength == _MATCH_WORD:
                tier = (
                    _TIER_TITLE_PREFIX
                    if _starts_on_word_boundary(title_lower, q_lower)
                    else _TIER_ALL_WORDS
                )
            else:
                # A curated alias outranks a merely-prefix direct match.
                order = _alias_order(expansions, words, words_set)
                if order is not None:
                    tier = _TIER_ALIAS
                    alias_order = order
                elif strength == _MATCH_PREFIX:
                    tier = _TIER_ALL_PREFIXES

        if tier is not None:
            scored.append((tier, alias_order, -level, len(title), code, title, level))

    scored.sort(key=lambda item: item[:4])

    results: List[Dict[str, Any]] = []
    seen_titles: set = set()
    for _tier, _order, _neg_level, _title_len, code, title, level in scored:
        if len(results) >= limit:
            break
        # -level sorts deepest-first within a tier, and two codes sharing a
        # title always land in the same tier/alias_order, so the first
        # occurrence of a given title is already the deepest one -- skip
        # any later (shallower) duplicate rather than the reverse.
        if title in seen_titles:
            continue
        seen_titles.add(title)
        results.append(
            {
                "code": code,
                "title": title,
                "level": level,
                "internal_key": resolve_internal_key(code),
            }
        )
    return results


def is_loaded() -> bool:
    """Whether the NAICS dataset loaded successfully (used by health checks/tests)."""
    return _LOADED
