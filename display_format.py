"""Single presentation layer for client-facing numbers, labels, and names.

Consolidates formatting logic that was previously duplicated (and drifting)
across excel_v2.py, ppt_generator.py, and app.py -- the shipped bugs this
fixes include raw ``snake_case`` channel keys leaking into workbook cells,
"$150.0K" trailing-zero artifacts, and an "18 months" duration turning into
17 months after a round-trip through a rounded weeks value.
"""

from __future__ import annotations

import math
import re
import unicodedata
from typing import Any

# ---------------------------------------------------------------------------
# Channel display names
# ---------------------------------------------------------------------------
# Internal channel keys (from ppt_generator.INDUSTRY_ALLOC_PROFILES / CHANNEL_
# ALLOC and budget_engine.CHANNEL_QUALITY_SCORES / CHANNEL_NAME_TO_CATEGORY)
# mapped to client-facing display names.
CHANNEL_DISPLAY: dict[str, str] = {
    # Core plan channels (ppt_generator.CHANNEL_ALLOC / INDUSTRY_ALLOC_PROFILES)
    "niche_boards": "Niche / Industry Boards",
    "programmatic_dsp": "Programmatic (DSP)",
    "global_boards": "Global Job Boards",
    "social_media": "Social Media",
    "regional_boards": "Regional Job Boards",
    "employer_branding": "Employer Branding",
    "apac_regional": "APAC Regional",
    "emea_regional": "EMEA Regional",
    # Additional channel types referenced elsewhere in the codebase
    "direct_hiring": "Direct Hiring",
    "lead_generation": "Lead Generation",
    "referrals": "Referrals",
    "referral": "Referral Programs",
    "staffing_agency": "Staffing Agencies",
    "staffing": "Staffing Agencies",
    "search_engine": "Search / SEM",
    "search": "Search / SEM",
    "career_site": "Career Sites",
    "display_retargeting": "Display / Retargeting",
    "display": "Display / Banner",
    "events_jobfairs": "Recruitment Events & Job Fairs",
    "events": "Recruitment Events & Job Fairs",
    "internal_mobility": "Internal Mobility",
    "university": "University / Campus Recruiting",
    "job_board": "Job Boards",
    "programmatic": "Programmatic",
    "social": "Social Media",
    "email": "Email Marketing",
    "regional": "Regional & Local Boards",
}


# ---------------------------------------------------------------------------
# Acronym-preserving title case
# ---------------------------------------------------------------------------
# copy:both#5-family: plain ``str.title()`` clobbers domain acronyms embedded
# in snake_case KB keys/labels -- e.g. "cdl_drivers" -> "Cdl Drivers" instead
# of "CDL Drivers". This allowlist covers the acronyms actually seen in
# recruitment KB data (role/credential abbreviations); extend as new ones
# surface rather than special-casing individual strings at call sites.
ACRONYMS: set[str] = {
    "CDL",
    "RN",
    "LPN",
    "CNA",
    "HVAC",
    "CPA",
    "CPC",
    "CPH",
    "DSP",
    "ATS",
}


def smart_title(s: str) -> str:
    """Acronym-preserving title case for a snake_case or space-separated
    label. 'cdl_drivers' -> 'CDL Drivers' (not 'Cdl Drivers'); any word whose
    upper-cased form is in :data:`ACRONYMS` is rendered in full caps, every
    other word is capitalized normally."""
    if not s or not isinstance(s, str):
        return s or ""
    words = s.replace("_", " ").split()
    out = []
    for w in words:
        if w.upper() in ACRONYMS:
            out.append(w.upper())
        else:
            out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out)


def channel_label(key: str) -> str:
    """Client-facing channel name. Falls back to a title-cased version of the
    key so callers NEVER see a raw ``snake_case`` key on a workbook/deck."""
    if not key or not isinstance(key, str):
        return ""
    if key in CHANNEL_DISPLAY:
        return CHANNEL_DISPLAY[key]
    return key.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Internal-key humanizer (industry / channel / supply tier / data-source id)
# ---------------------------------------------------------------------------
# Same token shape bundle_qa's snake_case_leak rule flags (one-or-more
# "_word" segments, so multi-underscore keys match too).
SNAKE_TOKEN_RE = re.compile(r"\b[a-z0-9]+(?:_[a-z0-9]+)+\b")
_URL_OR_EMAIL_RE = re.compile(r"https?://|www\.|[\w.+-]+@[\w-]+\.[\w.-]+")

# Keys that reach client text but live in neither shared_utils'
# INDUSTRY_LABEL_MAP nor CHANNEL_DISPLAY above. Anything not listed still
# humanizes generically -- this map only upgrades the wording.
KEY_DISPLAY: dict[str, str] = {
    # wizard-only industry value (app.classify_industry sector name)
    "rideshare": "Rideshare & Gig Economy",
    # (gold_standard._SUPPLY_TIERS such as "critically_scarce" are ordinary
    # words: deliberately NOT listed, so prose gets "critically scarce" and
    # a label cell gets smart_title's "Critically Scarce".)
    # data/international_benchmarks_2026.json "source" id
    "international_benchmarks_2026": "International Benchmarks 2026",
}


def _industry_label_map() -> dict[str, str]:
    # Local import: shared_utils is the single source of truth for industry
    # labels and is itself a leaf module (no import cycle), but keeping the
    # import lazy leaves display_format importable in isolation.
    try:
        from shared_utils import INDUSTRY_LABEL_MAP
    except ImportError:  # pragma: no cover -- shared_utils ships with the app
        return {}
    return INDUSTRY_LABEL_MAP


def humanize_key(key: Any, prose: bool = False) -> str:
    """Client-facing wording for ANY internal key that can reach a deck or
    workbook: an industry key (``construction_real_estate`` -> "Construction
    & Real Estate"), a channel key, a supply tier, a data-source id.

    Curated labels win (INDUSTRY_LABEL_MAP, CHANNEL_DISPLAY, KEY_DISPLAY).
    Anything else is humanized generically so no raw snake_case key can ever
    reach client text: ``prose=False`` title-cases it for a label/cell
    (``smart_title``), ``prose=True`` keeps it lower-case for use inside a
    sentence ("a critically scarce talent-supply tier"). Known acronyms
    (``ACRONYMS``) are upper-cased either way.
    """
    if not isinstance(key, str) or not key.strip():
        return ""
    k = key.strip()
    lk = k.lower()
    label = (
        _industry_label_map().get(lk) or CHANNEL_DISPLAY.get(lk) or KEY_DISPLAY.get(lk)
    )
    if label:
        return label
    if not prose:
        return smart_title(k)
    words = [w for w in k.replace("_", " ").split() if w]
    return " ".join(w.upper() if w.upper() in ACRONYMS else w for w in words)


def humanize_snake_tokens(text: Any, prose: bool = True) -> str:
    """Replace every raw snake_case token inside free ``text`` with its
    ``humanize_key`` form. Text carrying a URL or e-mail address is returned
    unchanged (the same exemption bundle_qa's snake_case rule applies --
    underscores are legitimate there)."""
    if not isinstance(text, str) or not text:
        return text if isinstance(text, str) else ""
    if _URL_OR_EMAIL_RE.search(text):
        return text
    return SNAKE_TOKEN_RE.sub(lambda m: humanize_key(m.group(0), prose=prose), text)


# ---------------------------------------------------------------------------
# Client / company name casing
# ---------------------------------------------------------------------------
# Articles / prepositions / conjunctions that go lowercase inside a client
# name (unless they are the first word, where normal title case capitalises
# them: "The Hershey Company", "Bank of America", "Bank of the West"). '&' is
# included for documentation parity with the prose rule even though it never
# reaches the word classifier below (a bare '&' has no letters, so it is
# always preserved as-is regardless of this set).
_CLIENT_NAME_CONNECTIVES: frozenset[str] = frozenset(
    {
        "of",
        "and",
        "the",
        "de",
        "la",
        "du",
        "von",
        "van",
        "for",
        "in",
        "on",
        "at",
        "to",
        "by",
        "with",
        "or",
        "&",
    }
)

# Known brand/acronym tokens for CLIENT NAMES specifically (distinct from
# the role/credential ACRONYMS table above). Case-insensitive lookup wins
# over both the "capitalize a bare-lowercase word" rule and the "shouty
# ALL-CAPS input" rule, so 'kpmg' / 'KPMG' / 'Kpmg' all normalize to the one
# correct form instead of becoming 'Kpmg'.
_CLIENT_BRAND_CASING: dict[str, str] = {
    "ups": "UPS",
    "ibm": "IBM",
    "at&t": "AT&T",
    "hca": "HCA",
    "cvs": "CVS",
    "ge": "GE",
    "3m": "3M",
    "bp": "BP",
    "dhl": "DHL",
    "usps": "USPS",
    "xpo": "XPO",
    "bmw": "BMW",
    "ihg": "IHG",
    "kpmg": "KPMG",
    "ey": "EY",
    "pwc": "PwC",
    "jpmorgan": "JPMorgan",
    "fedex": "FedEx",
    # Spellings no rule can derive from an all-caps input.
    "ebay": "eBay",
    "geico": "GEICO",
    "iqvia": "IQVIA",
    "nvidia": "NVIDIA",
    "pepsico": "PepsiCo",
    "linkedin": "LinkedIn",
    "paypal": "PayPal",
    "doordash": "DoorDash",
    "jetblue": "JetBlue",
    "gamestop": "GameStop",
    "autozone": "AutoZone",
    "autonation": "AutoNation",
    "carmax": "CarMax",
    "dewalt": "DeWalt",
    "unitedhealth": "UnitedHealth",
    "blackrock": "BlackRock",
    "chick-fil-a": "Chick-fil-A",
}

# Abbreviations whose conventional casing differs from plain Title Case (or
# that an all-caps input would otherwise leave shouted). Keys are lowercase
# and looked up per letter-run, so "LTD." / "(plc)" / "SA/NV" all resolve and
# the punctuation the client typed is kept. Suffixes that Title Case already
# renders correctly (Inc, Corp, Co, Company, Corporation, Pty, Limited) are
# deliberately absent.
_CLIENT_NAME_ABBREVIATIONS: dict[str, str] = {
    # Corporate / legal suffixes.
    "ltd": "Ltd",
    "plc": "plc",
    "llc": "LLC",
    "llp": "LLP",
    "lp": "LP",
    "gmbh": "GmbH",
    "ag": "AG",
    "sa": "SA",
    "se": "SE",
    "nv": "NV",
    "bv": "BV",
    "ab": "AB",
    "kg": "KG",
    # Titles and place abbreviations that appear inside client names.
    "st": "St",
    "mt": "Mt",
    "ft": "Ft",
    "dr": "Dr",
    "jr": "Jr",
    "sr": "Sr",
    "ii": "II",
    "iii": "III",
    "iv": "IV",
    # Consonant-only abbreviations that are words, not acronyms.
    "intl": "Intl",
    "mfg": "Mfg",
    "mgmt": "Mgmt",
    "svcs": "Svcs",
}

# Acronym-shaped tokens an ALL-CAPS input keeps as typed. A token with no
# vowel and at most _ACRONYM_MAX_LEN letters (GM, HP, CVS, KFC, CDW) is kept
# by shape; this list covers the ones that contain a vowel and so look like
# words (ADP, SAP, NASA, IKEA, USAA, CBRE...). Only non-words belong here --
# anything that is also an English word ("ARM", "ACE", "GAP") would be
# mis-kept, so spell those via the brand table instead. Extend as real
# client names surface. An acronym missing from both falls back to Title
# Case; a client can always type it mixed-case to have it preserved as-is.
_CLIENT_ACRONYMS: frozenset[str] = frozenset(
    {
        "AAA",
        "ABB",
        "ABC",
        "ABM",
        "ADM",
        "ADP",
        "ADT",
        "AEP",
        "AES",
        "AIA",
        "AIG",
        "AMC",
        "AMD",
        "AMN",
        "ANZ",
        "AOL",
        "AXA",
        "BAE",
        "BDO",
        "BNY",
        "CBRE",
        "CNA",
        "CNH",
        "CSL",
        "DXC",
        "EA",
        "EMC",
        "EPAM",
        "ESPN",
        "FIS",
        "HBO",
        "HPE",
        "IKEA",
        "ING",
        "MCI",
        "MUFG",
        "NASA",
        "NBA",
        "NEC",
        "NY",
        "SAIC",
        "SAP",
        "SAS",
        "TIAA",
        "UBS",
        "UHS",
        "UK",
        "US",
        "USA",
        "USAA",
    }
)
_ACRONYM_MAX_LEN = 4

# "MACDONALD" -> "MacDonald" needs an explicit list: a blanket Mac- rule
# would mangle MACHINE, MACY'S, MACK, MACHO. (Mc- has no common English-word
# collisions at 5+ letters, so it is a rule, not a list.)
_MAC_SURNAMES: frozenset[str] = frozenset(
    {
        "macdonald",
        "macarthur",
        "macgregor",
        "mackenzie",
        "maclean",
        "macleod",
        "macmillan",
        "macpherson",
    }
)

# After an apostrophe these stay lowercase (McDonald's, Macy's, Don't); any
# other tail is a name part and is capitalised (O'Reilly, L'Oréal, D'Angelo).
_APOSTROPHE_TAILS: frozenset[str] = frozenset(
    {"s", "t", "d", "m", "n", "ll", "re", "ve"}
)

# A trailing ".com"-style label inside one word stays lowercase (Amazon.com).
_DOMAIN_LABELS: frozenset[str] = frozenset({"com", "net", "org", "io", "ai", "tv"})

# Longest name the casing rules are applied to. app.py rejects client_name
# above 200 characters; anything past this bound is not a name, so it is
# returned whitespace-collapsed and otherwise untouched instead of being
# re-cased (no silent truncation, no quadratic work on pasted junk).
_CLIENT_NAME_MAX_LEN = 1000

_ORDINAL_RUN = re.compile(r"\d+(?:st|nd|rd|th)", re.IGNORECASE)


def _has_vowel(text: str) -> bool:
    """True if ``text`` contains a vowel (Y counts, so SKY/GYM/DRY read as
    words; accents are ignored, so É is a vowel)."""
    decomposed = unicodedata.normalize("NFD", text.upper())
    return any(ch in "AEIOUY" for ch in decomposed)


def _is_acronym(run: str) -> bool:
    """An acronym-shaped letter run: on the known list, or short with no
    vowel (GM, HP, KFC). Everything else in a shouted name is a word."""
    if run.upper() in _CLIENT_ACRONYMS:
        return True
    return len(run) <= _ACRONYM_MAX_LEN and not _has_vowel(run)


def _title(run: str) -> str:
    """Capitalise the first letter, lowercase the rest ('4IMPRINT' ->
    '4Imprint'; accents and titlecase letters handled by str.capitalize)."""
    for i, ch in enumerate(run):
        if ch.isalpha():
            return run[:i] + run[i:].capitalize()
    return run


def _fix_run(run: str, shouting: bool) -> str:
    """Case one run of letters/digits (no punctuation inside)."""
    if run.isdigit():
        return run
    if run.lower() in _CLIENT_NAME_ABBREVIATIONS:
        return _CLIENT_NAME_ABBREVIATIONS[run.lower()]  # LTD -> Ltd, SA/NV
    if _ORDINAL_RUN.fullmatch(run):
        return run.lower()  # 21ST -> 21st
    if any(ch.isdigit() for ch in run):
        # 3M / B2B stay shouted; a longer mixed token is a word (4Imprint)
        return run.upper() if len(run) <= 4 else _title(run)
    if shouting and _is_acronym(run):
        return run
    if len(run) >= 5 and run.isalpha() and run[:2].upper() == "MC":
        return "Mc" + _title(run[2:])  # MCKESSON -> McKesson
    if run.lower() in _MAC_SURNAMES:
        return "Mac" + _title(run[3:])  # MACDONALD -> MacDonald
    return _title(run)


def _is_word_char(ch: str) -> bool:
    """Letter, digit, or a combining mark (which belongs to the letter it
    follows -- lowercasing 'İ' yields 'i' + a combining dot)."""
    return ch.isalnum() or unicodedata.category(ch).startswith("M")


def _split_runs(text: str) -> list[str]:
    """Split into [run, sep, run, sep, ..., run]: a run is a stretch of word
    characters (possibly empty between two separators), a sep is exactly one
    non-word character. Linear."""
    parts: list[str] = []
    current: list[str] = []
    for ch in text:
        if _is_word_char(ch):
            current.append(ch)
        else:
            parts.append("".join(current))
            parts.append(ch)
            current = []
    parts.append("".join(current))
    return parts


def _fix_core(core: str, shouting: bool) -> str:
    """Case one punctuation-stripped word, run by run. Hyphens, dots,
    ampersands and apostrophes inside the word are kept where they are; each
    letter/digit run between them is cased on its own, so '7-ELEVEN' ->
    '7-Eleven', "O'REILLY" -> "O'Reilly", 'J.P' -> 'J.P', 'M&T' -> 'M&T'."""
    parts = _split_runs(core)
    for i in range(0, len(parts), 2):
        run = parts[i]
        if not run:
            continue
        if i >= 2 and parts[i - 1] in ("'", "\u2019"):
            tail = run.lower()
            parts[i] = tail if tail in _APOSTROPHE_TAILS else _title(run)
        else:
            parts[i] = _fix_run(run, shouting)
    if (
        len(parts) >= 3
        and parts[-2] == "."
        and len(parts[0]) > 1
        and parts[-1].lower() in _DOMAIN_LABELS
    ):
        parts[-1] = parts[-1].lower()  # AMAZON.COM -> Amazon.com
    return "".join(parts)


def _strip_edge_punct(word: str) -> str:
    """``word`` without leading/trailing non-word characters."""
    start, end = 0, len(word)
    while start < end and not _is_word_char(word[start]):
        start += 1
    while end > start and not _is_word_char(word[end - 1]):
        end -= 1
    return word[start:end]


def _fix_word(word: str, is_first: bool, repair: bool, shouting: bool) -> str:
    """Case one space-delimited word of a client name.

    Edge punctuation (commas, dots, brackets) is peeled off, looked up or
    recased, and put back exactly as typed. ``repair=False`` leaves the word
    as typed unless it is a brand-table entry; ``shouting`` marks a word
    that came from an all-caps name (enables the acronym rule). The peel is
    a linear scan, not a regex, so a long punctuation-heavy token cannot go
    quadratic."""
    start, end = 0, len(word)
    while start < end and not _is_word_char(word[start]):
        start += 1
    while end > start and not _is_word_char(word[end - 1]):
        end -= 1
    core = word[start:end]
    if not core:
        return word  # '&', '-', '...': nothing to case
    key = core.lower()
    if key in _CLIENT_BRAND_CASING:
        fixed = _CLIENT_BRAND_CASING[key]
    elif not repair:
        return word
    elif not is_first and key in _CLIENT_NAME_CONNECTIVES:
        fixed = key
    else:
        fixed = _fix_core(core, shouting)
    return word[:start] + fixed + word[end:]


def client_display_name(raw: str | None) -> str:
    """Client-facing casing of a client name. The client's own spelling is
    authoritative wherever they typed it deliberately; only raw source data
    (all-caps or all-lowercase) is rewritten.

    Whitespace is always trimmed and collapsed; the text is NFC-normalised
    (a decomposed 'E' + accent composes to one letter). Empty, ``None`` and
    non-string input give ``""``. The function never raises, is idempotent
    (``f(f(x)) == f(x)``, which matters because app.py, ppt_generator,
    excel_v2 and bundle_qa each re-apply it), and does linear work; a name
    longer than :data:`_CLIENT_NAME_MAX_LEN` is returned collapsed but
    un-cased.

    Rules, in the order a word meets them:

    1. Mixed-case input is kept as typed -- 'FORD Motor Company', 'Walmart
       INC', 'McKesson Corp', 'eBay' and 'Bank Of America' come back
       unchanged. Only two repairs apply to a mixed-case name: a
       brand-table word (:data:`_CLIENT_BRAND_CASING`) is respelled in any
       casing ('Ups' -> 'UPS', 'Fedex' -> 'FedEx'), and a word typed in
       ALL LOWERCASE is treated as raw data and run through rule 2
       ('atria Senior living' -> 'Atria Senior Living'). If those repairs
       leave a name of nothing but capitals ('THE HERSHEY llc' -> 'THE
       HERSHEY LLC'), it is then cased as an all-caps name ('The Hershey
       LLC') so that applying the function twice changes nothing more.
    2. A word being repaired -- every word of an all-caps name, or a
       lowercase word of a mixed one -- is resolved by the first rule that
       fits:

       a. brand table: 'JPMORGAN' -> 'JPMorgan', 'AT&T' stays, '3M' stays.
       b. connective (:data:`_CLIENT_NAME_CONNECTIVES`: the/of/and/for...):
          lowercase, except as the first word ('The Home Depot', 'Bank of
          America').
       c. otherwise each letter/digit run inside the word is cased on its
          own, so hyphens, dots, '&', slashes and apostrophes survive and
          the punctuation the client typed is kept. A run is, in order:
          an abbreviation with a conventional casing
          (:data:`_CLIENT_NAME_ABBREVIATIONS`: 'LTD.' -> 'Ltd.', 'PLC' ->
          'plc', 'GMBH' -> 'GmbH', 'ST.' -> 'St.', 'SA/NV' -> 'SA/NV'); an
          ordinal ('21ST' -> '21st'); an acronym, which an all-caps name
          keeps as typed (on the :data:`_CLIENT_ACRONYMS` list, or at most
          4 letters with no vowel: 'ADP', 'NASA', 'GM', 'CVS', 'J.P.'); a
          Mc- prefix or listed Mac- surname with its inner capital
          ('MCKESSON' -> 'McKesson', 'MACDONALD' -> 'MacDonald', while
          'MACHINE' and 'MACY'S' stay ordinary words); otherwise Title
          Case ('MANPOWER' -> 'Manpower'). The tail after an apostrophe is
          capitalised unless it is a contraction ("O'REILLY" ->
          "O'Reilly", "L'ORÉAL" -> "L'Oréal", "MCDONALD'S" ->
          "McDonald's"), and a trailing '.com' stays lowercase
          ('AMAZON.COM' -> 'Amazon.com').

    0. A name that is a single all-caps token of 2-4 letters ('AWP', 'ACE',
       'BMW', 'A&W') is the client's own acronym and is kept as typed,
       vowels or not; only the brand table overrides it ('PWC' -> 'PwC').

    Known limit: inside a LONGER all-caps name, an unfamiliar vowel-bearing
    word of four letters or fewer that is not on the acronym list is
    Title-Cased ('AAON HOLDINGS' -> 'Aaon Holdings'); add it to the list,
    or type it mixed-case.
    """
    if not raw or not isinstance(raw, str):
        return ""
    collapsed = re.sub(r"\s+", " ", unicodedata.normalize("NFC", raw)).strip()
    if not collapsed or len(collapsed) > _CLIENT_NAME_MAX_LEN:
        return collapsed
    shouting = collapsed.isupper()
    if (
        shouting
        and " " not in collapsed
        and 2 <= sum(ch.isalpha() for ch in collapsed) <= _ACRONYM_MAX_LEN
        and _strip_edge_punct(collapsed).lower() not in _CLIENT_BRAND_CASING
    ):
        # A client name that is ONE all-caps token of 2-4 letters (AWP, ADP,
        # GM, UPS, ACE, BMW, A&W) is the client's own acronym: keep it as
        # typed, vowels or not. The vowel test below is for words INSIDE a
        # longer shouted name (BIG LOTS, HOME DEPOT); applied to a lone
        # token it turned the prod client "AWP" into "Awp" on 15 surfaces.
        # The brand table still wins (PWC -> PwC).
        return collapsed
    # Re-normalise: capitalising a letter can create a new composable pair
    # (the 'ß' + combining-accent case), and the output must be a fixed point.
    result = unicodedata.normalize(
        "NFC",
        " ".join(
            _fix_word(w, i == 0, shouting or w.islower(), shouting)
            for i, w in enumerate(collapsed.split(" "))
        ),
    )
    if not shouting and result.isupper():
        # Repairing the lowercase words of a mixed-case name can leave
        # nothing but capitals ('THE HERSHEY llc' -> 'THE HERSHEY LLC'). The
        # next call would then see an all-caps name and case it again, so
        # settle it now: f(f(x)) == f(x). `result` is upper-case, so this
        # recursion takes the all-caps branch and stops there.
        return client_display_name(result)
    return result


# ---------------------------------------------------------------------------
# Money / percent / count formatting
# ---------------------------------------------------------------------------
def fmt_money(x: float, compact: bool = False) -> str:
    """150000 -> '$150,000'; compact: '$150K'; 52500 compact -> '$52.5K'
    (trailing '.0' is always stripped -- never '$150.0K')."""
    try:
        val = float(x)
    except (TypeError, ValueError):
        return "$0"
    sign = "-" if val < 0 else ""
    val = abs(val)
    if compact:
        if val >= 1_000_000:
            scaled = val / 1_000_000
            body = f"{scaled:.1f}".rstrip("0").rstrip(".")
            return f"{sign}${body}M"
        if val >= 1_000:
            scaled = val / 1_000
            body = f"{scaled:.1f}".rstrip("0").rstrip(".")
            return f"{sign}${body}K"
        return f"{sign}${val:,.0f}"
    return f"{sign}${val:,.0f}"


def fmt_pct(x: float, decimals: int = 0, is_fraction: bool = False) -> str:
    """Format a percent. ``x`` is a 0-100 value by default; pass
    ``is_fraction=True`` when ``x`` is a 0-1 fraction instead. Never emits
    more than 2 decimal places regardless of ``decimals``."""
    try:
        val = float(x)
    except (TypeError, ValueError):
        return "0%"
    if is_fraction:
        val *= 100
    d = max(0, min(2, int(decimals)))
    return f"{val:.{d}f}%"


def _pluralize(word: str) -> str:
    if not word:
        return word
    lower = word.lower()
    if lower.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    if lower.endswith("y") and len(word) > 1 and lower[-2] not in "aeiou":
        return word[:-1] + "ies"
    return word + "s"


def fmt_count(n: int, noun: str) -> str:
    """'1 market', '6 markets' -- never emits '(s)'."""
    try:
        count = int(n)
    except (TypeError, ValueError):
        count = 0
    label = noun if count == 1 else _pluralize(noun)
    return f"{count} {label}"


def fmt_float(x: float, max_decimals: int = 2) -> str:
    """Format a float, stripping trailing zeros (and a trailing '.')."""
    try:
        val = float(x)
    except (TypeError, ValueError):
        return "0"
    d = max(0, int(max_decimals))
    s = f"{val:.{d}f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


# ---------------------------------------------------------------------------
# Monthly-to-total reconciliation (largest remainder method)
# ---------------------------------------------------------------------------
def reconcile_monthly_to_total(monthly: list[float], total: float) -> list[int]:
    """Round each monthly value to an int such that the ints sum EXACTLY to
    ``round(total)``, using the largest-remainder method (fewest possible
    per-bucket adjustments, ties broken by original list order)."""
    if not monthly:
        return []
    target = int(round(total))
    n = len(monthly)
    vals = [float(v) if isinstance(v, (int, float)) else 0.0 for v in monthly]
    floors = [math.floor(v) for v in vals]
    remainders = [v - f for v, f in zip(vals, floors)]
    result = list(floors)
    diff = target - sum(floors)

    if diff > 0:
        order = sorted(range(n), key=lambda i: (-remainders[i], i))
        idx = 0
        while diff > 0:
            result[order[idx % n]] += 1
            diff -= 1
            idx += 1
    elif diff < 0:
        order = sorted(range(n), key=lambda i: (remainders[i], i))
        idx = 0
        while diff < 0:
            result[order[idx % n]] -= 1
            diff += 1
            idx += 1

    return [int(v) for v in result]


# ---------------------------------------------------------------------------
# Duration <-> weeks (exact round-trip)
# ---------------------------------------------------------------------------
_WEEKS_RE = re.compile(r"~\s*(\d+)\s*week")
_NUM_RE = re.compile(r"[\d.]+")


def parse_duration_to_weeks(text: str | int | float) -> int:
    """'6 months' -> 26, '18 months'/'1.5 years' -> 78, '12 weeks' -> 12,
    a bare int/numeric-only string = weeks. Uses 52/12 weeks-per-month
    CONSISTENTLY with :func:`weeks_to_duration_label`.

    Labels produced by :func:`weeks_to_duration_label` embed the literal
    week count as "(~NN weeks)"; when present, that literal count is used
    directly so ``parse_duration_to_weeks(weeks_to_duration_label(w)) == w``
    for every ``w``, not just multiples of the 52/12 ratio.
    """
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        return max(0, round(float(text)))
    if not isinstance(text, str):
        return 0
    s = text.strip().lower()
    if not s:
        return 0

    literal = _WEEKS_RE.search(s)
    if literal:
        return max(0, int(literal.group(1)))

    match = _NUM_RE.search(s)
    if not match:
        return 0
    num = float(match.group())

    if "year" in s:
        weeks = num * 52.0
    elif "month" in s:
        weeks = num * 52.0 / 12.0
    elif "week" in s:
        weeks = num
    elif "day" in s:
        weeks = num / 7.0
    else:
        weeks = num  # bare numeric string -- treat as weeks

    return max(0, round(weeks))


def weeks_to_duration_label(weeks: int) -> str:
    """Exact inverse framing of :func:`parse_duration_to_weeks`:
    26 -> '6 months (~26 weeks)', 78 -> '18 months (~78 weeks)'.
    <=13 weeks stays expressed in weeks."""
    w = max(0, int(weeks or 0))
    if w <= 13:
        return fmt_count(w, "week")

    months = round(w * 12.0 / 52.0)
    if months <= 24:
        return f"{months} months (~{w} weeks)"

    years = months / 12.0
    if abs(years - round(years)) < 0.05:
        yr = int(round(years))
        yr_txt = f"{yr} year" + ("s" if yr != 1 else "")
    else:
        yr_txt = f"{years:.1f} years"
    return f"{yr_txt} (~{w} weeks)"


# ---------------------------------------------------------------------------
# Canonical campaign-duration resolution (single source of truth)
# ---------------------------------------------------------------------------
# Real shipped defect (Uber, brief campaign_duration="1-3 months"): the
# workbook's Executive Summary + 90-Day Forecast said "4 weeks" (a raw
# string re-parsed through parse_duration_to_weeks's generic 52/12 numeric
# rule, which reads "1-3 months" as just "1 month"), the deck's
# Implementation Timeline + the workbook's own Optimization Milestones said
# "Weeks 1-12" (app.py's phrase-ladder bucket for the SAME string), the
# deck's Next Steps slide said "1-3 months" (the raw brief string, never
# resolved at all), and the 90-Day Forecast window showed a fixed ~13-week
# date range -- four different derivations of one input. This is the S91
# drift bug class recurring: app.py's inline phrase ladder and this
# module's parse_duration_to_weeks were two independently maintained
# week-resolvers that could (and did) disagree, and a THIRD, separate
# duration-label formatter lived in app.py's own request handler
# (diverging from weeks_to_duration_label -- e.g. 80 weeks read back as
# "1.5 years (~18 months)" there but "18 months (~80 weeks)" here; the
# latter is the one every regression test in this repo actually asserts).
#
# Everything below is now the ONE place that maps a free-text
# campaign_duration to a week count (:func:`resolve_campaign_weeks`) and to
# the label shown anywhere in a bundle (:func:`resolve_campaign_duration_
# label`). app.py, excel_v2.py and ppt_generator.py all delegate here
# instead of re-deriving their own answer.

_UNBOUNDED_DURATION_LABEL = "Ongoing (no fixed end date)"
_UNBOUNDED_DURATION_EXACT = frozenset({"unbounded", "not specified", "tbd", "n/a"})

# Fixed marketing buckets the wizard's duration dropdown offers, each mapped
# to a deliberately "round" week count for phasing purposes -- NOT a strict
# 52/12 conversion of the phrase's own numbers (e.g. "6 months" -> 24 weeks
# here, a 4-weeks/month convention that matches every reference bundle and
# regression test in this repo, vs. parse_duration_to_weeks("6 months") ==
# 26, the general 52/12 free-text conversion used for values that AREN'T
# one of these dropdown options). Order matters: a phrase must be checked
# before a shorter phrase it could otherwise be mistaken for (e.g. "1-2
# year" before "2 year"). Literal single-month phrases ("1 month", "2
# month", "3 month") were deliberately removed from the "1-3 month" bucket
# so they no longer match here and instead fall through to the accurate
# 52/12 conversion in parse_duration_to_weeks below -- per Jesse Ofner's
# 2026-07-31 feedback that this bucket was silently expanding a requested
# "1 month" plan to ~84 days (12 weeks).
_DURATION_PHRASE_LADDER: tuple[tuple[tuple[str, ...], int], ...] = (
    (("2-5 year", "long-term", "long term"), 156),
    (("1-2 year", "2 year"), 80),
    (("6-12 month", "9 month", "12 month", "1 year"), 48),
    (("3-6 month", "4 month", "5 month", "6 month"), 24),
    (("1-3 month",), 12),
    (("ongoing",), 52),
)


def is_unbounded_duration(duration: Any) -> bool:
    """True for the wizard's "Ongoing" duration option (any case/whitespace
    variant or synonym), or an empty/not-specified/TBD placeholder -- an
    open-ended campaign with no fixed end date, as opposed to a fixed
    length like "6 months". Single source of truth for a check that used
    to be duplicated verbatim in excel_v2.py and ppt_generator.py (and
    would otherwise need to recognise both the raw wizard value "Ongoing"
    and the canonical label :data:`_UNBOUNDED_DURATION_LABEL` this module
    produces for it)."""
    s = str(duration or "").strip().lower()
    if not s or s in _UNBOUNDED_DURATION_EXACT:
        return True
    return "ongoing" in s


def resolve_campaign_weeks(duration_str: Any) -> int:
    """THE single source of truth mapping a free-text campaign_duration
    string to a week count.

    Merges what used to be two independently maintained ladders: app.py's
    inline phrase ladder for the wizard's fixed dropdown buckets, and this
    module's own :func:`parse_duration_to_weeks` for everything else. Every
    caller must resolve campaign_weeks through this one function so the
    same duration string can never produce two different week counts
    depending on which module happened to parse it.

    Resolution order: a fixed marketing-bucket phrase first (dropdown
    option -- includes "ongoing" -> 52, an annual-cycle approximation used
    for PHASING math only, never for the duration label -- see
    :func:`resolve_campaign_duration_label`), then
    :func:`parse_duration_to_weeks` for anything else (explicit "N
    weeks"/"N months"/"N years", bare numerics). Unparseable/unrecognized
    input defaults to 12 weeks, matching the legacy ladder's own default.
    """
    s = str(duration_str or "").strip().lower()
    for phrases, weeks in _DURATION_PHRASE_LADDER:
        if any(p in s for p in phrases):
            return weeks
    weeks = parse_duration_to_weeks(s)
    return weeks if weeks > 0 else 12


def resolve_campaign_duration_label(data: dict) -> str:
    """THE single source of truth for the campaign-duration STRING shown
    anywhere in a bundle (Executive Summary, 90-Day Forecast, deck Next
    Steps/Implementation Timeline...). Every surface must render THIS
    value rather than re-deriving its own wording from the raw brief
    string, so the bundle can never state its duration two different ways.

    Preference order:
      1. An explicitly "Ongoing"/"unbounded" raw duration always wins --
         regardless of any numeric campaign_weeks already computed for
         phasing purposes -- so an open-ended campaign never reads as a
         specific fixed length (the prod defect this guards against:
         "Ongoing" silently resolving to "1 year (~12 months)").
      2. ``data["campaign_duration_canonical"]`` when already resolved
         (set once, upstream -- e.g. by app.py from campaign_weeks).
      3. Derived from ``data["campaign_weeks"]`` when present.
      4. Derived by resolving the raw ``campaign_duration``/``timeline``
         string through :func:`resolve_campaign_weeks`.
      5. The raw string itself, or "Not specified".
    """
    raw = str(data.get("campaign_duration") or data.get("timeline") or "").strip()
    raw_lower = raw.lower()
    if raw_lower == "unbounded" or "ongoing" in raw_lower:
        return _UNBOUNDED_DURATION_LABEL

    canonical = data.get("campaign_duration_canonical")
    if isinstance(canonical, str) and canonical.strip():
        return canonical.strip()

    weeks = data.get("campaign_weeks")
    try:
        weeks_int = int(weeks) if weeks else 0
    except (TypeError, ValueError):
        weeks_int = 0
    if weeks_int > 0:
        return weeks_to_duration_label(weeks_int)

    if not raw_lower or raw_lower in ("not specified", "tbd", "n/a"):
        return "Not specified"

    derived = resolve_campaign_weeks(raw)
    if derived > 0:
        return weeks_to_duration_label(derived)
    return raw or "Not specified"


def scale_week_phases(total_weeks: int, num_phases: int) -> list[tuple[int, int]]:
    """Partition weeks 1..``total_weeks`` into ``num_phases`` contiguous,
    non-overlapping (start, end) week ranges whose FINAL phase always ends
    EXACTLY at ``total_weeks`` -- so a "Week N-M" phase/milestone table
    built from this can never contradict the campaign's own canonical
    duration (the real shipped defect this guards against: a fixed
    "Week 1-12" milestones table rendered unchanged regardless of whether
    the campaign was 4 weeks or 78).

    A campaign shorter than ``num_phases`` weeks legitimately compresses --
    several phases can share the same single week (mirrors how
    ppt_generator's own Implementation Timeline slide already collapses
    its phases for a <=12-week campaign, e.g. "Weeks 4-4") -- rather than
    ever spilling a phase past the campaign's real end.
    """
    total = max(1, int(total_weeks or 0))
    n = max(1, int(num_phases or 1))
    phases: list[tuple[int, int]] = []
    prev_end = 0
    for i in range(1, n + 1):
        raw_end = round(i * total / n)
        start = min(prev_end + 1, total)
        end = max(raw_end, start)
        end = min(end, total)
        phases.append((start, end))
        prev_end = end
    # Rounding can leave the last phase short of `total` (e.g. total=52,
    # n=6 rounds to .../44-52 already, but not every (total, n) pair does)
    # -- pin it exactly so the bundle's own phased timeline always agrees
    # with the canonical campaign_weeks to the week.
    last_start, _ = phases[-1]
    phases[-1] = (last_start, total)
    return phases


# ---------------------------------------------------------------------------
# Hire goal parsing + gap statement
# ---------------------------------------------------------------------------
def parse_hire_goal(hire_volume: Any) -> int:
    """Parse the client's stated hiring GOAL to a comparable integer (low
    end). Ported verbatim from excel_v2.py:1665-1692 (``_parse_hire_goal``).

    ``hire_volume`` arrives as a free-text field: a bare int, "5000 hires",
    "50-100 hires", "5,000+", or "Not specified"/"TBD"/"". For a range we take
    the LOW end. Returns 0 when no numeric goal can be parsed, which callers
    treat as "no goal stated".
    """
    if isinstance(hire_volume, (int, float)):
        return max(0, int(hire_volume))
    if not isinstance(hire_volume, str):
        return 0
    text = hire_volume.strip().lower()
    if not text or text in ("not specified", "tbd", "n/a", "none", "unknown"):
        return 0
    nums = re.findall(r"\d[\d,]*", text)
    if not nums:
        return 0
    try:
        return max(0, int(nums[0].replace(",", "")))
    except ValueError:
        return 0


def goal_gap(projected_hires: int, goal: int, cost_per_hire: float) -> dict | None:
    """Honest gap statement between the client's stated hiring goal and the
    plan's projected hires. ``None`` when there is no goal to compare against
    (``goal <= 0``) or the plan already meets/exceeds it."""
    try:
        goal_i = int(goal)
    except (TypeError, ValueError):
        return None
    try:
        projected_i = int(projected_hires)
    except (TypeError, ValueError):
        projected_i = 0
    if goal_i <= 0 or projected_i >= goal_i:
        return None
    try:
        cph = float(cost_per_hire)
    except (TypeError, ValueError):
        cph = 0.0

    pct_of_goal = (projected_i / goal_i) * 100 if goal_i else 0.0
    # cost_per_hire <= 0 means UNKNOWN (typically a plan projecting zero
    # hires, where budget / 0 has no value), not "free". Multiplying the gap
    # by 0 shipped "scaling path: ~$0 additional" on the deck -- an inverted
    # message on exactly the plans whose honesty matters most. Report None
    # and let each surface omit the figure.
    additional_budget = (goal_i - projected_i) * cph if cph > 0 else None
    return {
        "goal": goal_i,
        "projected": projected_i,
        "pct_of_goal": round(pct_of_goal, 1),
        "additional_budget": (
            round(additional_budget, 2) if additional_budget is not None else None
        ),
    }
