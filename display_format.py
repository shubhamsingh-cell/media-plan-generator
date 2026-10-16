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

import wizard_inputs

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
    # Camel-case spellings read off each company's own site (2026-10-16):
    # corporate.exxonmobil.com, adventhealth.com, conocophillips.com,
    # metlife.com, manpowergroup.com, davita.com, commonspirit.org,
    # ziprecruiter.com. An all-caps input cannot yield the inner capital.
    "exxonmobil": "ExxonMobil",
    "adventhealth": "AdventHealth",
    "conocophillips": "ConocoPhillips",
    "metlife": "MetLife",
    "manpowergroup": "ManpowerGroup",
    "davita": "DaVita",
    "commonspirit": "CommonSpirit",
    "ziprecruiter": "ZipRecruiter",
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

# Acronym-shaped tokens an ALL-CAPS input keeps as typed. A token of at most
# _ACRONYM_MAX_LEN letters is kept by shape when it has no vowel (GM, HP, CVS,
# KFC, CDW) or is not an English word (AWP, RTI -- see _SHORT_WORDS); this
# list is the explicit record of known acronyms, and the only way to keep one
# that is ALSO a word (SAP, ING). Only acronyms belong here -- anything that
# should read as a word ("ARM", "ACE", "GAP") must not be listed; spell the
# odd one via the brand table instead. Extend as real client names surface.
# A client can always type a name mixed-case to have it preserved as-is.
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
    """An acronym-shaped letter run in a shouted name: on the known list, or
    at most :data:`_ACRONYM_MAX_LEN` letters and either vowel-less (GM, HP,
    KFC) or not an English word (AWP, RTI, ADP -- the vowel does not make
    them words). A longer run, a word on :data:`_SHORT_WORDS` (NEW, LOTS,
    HOME) and a non-ASCII token we cannot judge (ETE) are words."""
    if run.upper() in _CLIENT_ACRONYMS:
        return True
    if len(run) > _ACRONYM_MAX_LEN:
        return False
    if not _has_vowel(run):
        return True
    return run.isascii() and run.isalpha() and run.lower() not in _SHORT_WORDS


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
          keeps as typed: on the :data:`_CLIENT_ACRONYMS` list, or at most
          4 letters and either vowel-less ('GM', 'CVS', 'J.P.') or not an
          English word ('AWP', 'RTI', 'ADP', 'NASA' -- the words are the
          2-4 letter entries of :data:`_SHORT_WORDS`, so 'NEW', 'LOTS',
          'HOME' and 'YORK' are Title-Cased and 'SAFETY' (longer than 4
          letters) always is: 'AWP SAFETY' -> 'AWP Safety', 'RTI
          INTERNATIONAL' -> 'RTI International'); a
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

    Known limit: the acronym test is "not a known word", so inside a LONGER
    all-caps name an uncommon word or name of four letters or fewer that is
    missing from :data:`_SHORT_WORDS` stays shouted ('TUMI INC' -> 'TUMI
    Inc'); add it to the word list (or the brand table for a special
    spelling), or type the name mixed-case. The reverse is also possible: an
    acronym that is also a word on the list ('ACE') reads as a word.
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


# A name that already carries its own possessive-looking ending: a straight or
# typographic apostrophe followed by 's' / 'S' ("McDonald's", "Kohl’s",
# "DENNY'S").
_ALREADY_POSSESSIVE_RE = re.compile(r"['’ʼ][sS]$")
_APOSTROPHES = "'’ʼ"


def possessive(name: Any) -> str:
    """The possessive form of a client / company name: 'Joveo' -> "Joveo's".

    Every ``f"{client}'s ..."`` in the deck and workbook used to bolt a bare
    ``'s`` onto whatever name arrived, which printed "McDonald's's",
    "Brightway Home Stores's", "Barclays's" and "Werner Enterprises's" on
    client-facing slides (2026-10-16 sweep: ~100 hits across 10 of 26
    bundles). One rule, applied in this order, on the name with surrounding
    whitespace removed:

    1. Empty, ``None`` or whitespace-only -> ``""`` (never a stray "'s").
    2. Already ends in an apostrophe + 's'/'S' ("McDonald's", "Macy's",
       "Kohl's", "DENNY'S", typographic apostrophe too) -> unchanged: the
       name is already in possessive form, and "McDonald's's" is the bug.
    3. Already ends in a bare apostrophe ("Stores'") -> unchanged.
    4. Ends in 's' or 'S' ("Stores", "Services", "Barclays", "UPS") -> add a
       bare apostrophe: "Stores'", "Barclays'", "UPS'". (Plural-looking and
       s-final singular names read the same way and cannot be told apart
       from the string alone, so both take the apostrophe-only form.)
    5. Anything else -> add "'s": "Joveo's", "3M's", "AT&T's".

    The text before the suffix is returned exactly as given (no recasing --
    :func:`client_display_name` owns casing), so the function is idempotent:
    ``possessive(possessive(x)) == possessive(x)``.
    """
    if name is None:
        return ""
    text = (name if isinstance(name, str) else str(name)).strip()
    if not text:
        return ""
    if _ALREADY_POSSESSIVE_RE.search(text) or text[-1] in _APOSTROPHES:
        return text
    if text[-1] in "sS":
        return text + "'"
    return text + "'s"


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

# Calendar-span durations. A quarter is 13 weeks, a half 26, a fiscal/full
# year 52 -- the same 52-week year the month conversion above uses. Verified
# 2026-10-16: without these, the generic "first number is a count of weeks"
# rule read "Q1-Q3" as 1 week, "Q4" as 4, "2 quarters" as 2 and "FY2026" as
# 2026, so a Q1-Q3 plan was announced as "$1.5M over 1 week".
_WEEKS_PER_QUARTER = 13
_WEEKS_PER_HALF = 26
_WEEKS_PER_YEAR = 52
_RANGE_JOIN = r"(?:-|\u2013|\u2014|to|through|thru)"
# "q1-q3", "q1 to q3", "q1 2026 - q1 2027" (year optional on either end)
_QUARTER_RANGE_RE = re.compile(
    rf"\bq([1-4])(?:\s*(20\d\d))?\s*{_RANGE_JOIN}\s*q([1-4])(?:\s*(20\d\d))?\b"
)
_QUARTER_RE = re.compile(r"\bq([1-4])\b")
_N_QUARTERS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*-?\s*quarters?\b")
_HALF_RANGE_RE = re.compile(rf"\bh([12])\s*{_RANGE_JOIN}\s*h([12])\b")
_HALF_RE = re.compile(r"\bh([12])\b")
_N_HALVES_RE = re.compile(r"(\d+(?:\.\d+)?)\s*-?\s*halves\b")
_HALF_YEAR_RE = re.compile(r"\bhalf[\s-]?year\b|\b(?:first|second|1st|2nd)\s+half\b")
_FULL_YEAR_RE = re.compile(
    r"\bfy(?:\s*(?:20\d\d|\d\d))?\b|\bfull[\s-]?year\b"
    r"|\bfiscal\s+year\b|\bcalendar\s+year\b"
)
# A stated length ("6 weeks in Q3", "Q3 (8 weeks)") beats the calendar anchor.
_STATED_LENGTH_RE = re.compile(
    r"(\d+(?:\.\d+)?)\s*-?\s*(days?|wks?|weeks?|mos?|months?|yrs?|years?)\b"
)


def _calendar_span_weeks(s: str) -> int:
    """Weeks for a quarter / half / fiscal-year expression in lower-cased
    ``s`` -- 'q1-q3' -> 39, 'q4' -> 13, '2 quarters' -> 26, 'h2' -> 26,
    'fy2026' / 'full year' -> 52 -- or 0 when ``s`` has none. A range is the
    inclusive span of its ends (a descending range wraps the year: 'q4-q2'
    is Q4, Q1, Q2; with both years stated the years decide); a list of
    tokens ('q1 & q2') spans from the earliest to the latest."""
    rng = _QUARTER_RANGE_RE.search(s)
    if rng:
        first, y1, last, y2 = (
            int(rng.group(1)),
            rng.group(2),
            int(rng.group(3)),
            rng.group(4),
        )
        quarters = (last - first) % 4 + 1
        if y1 and y2:
            stated = (int(y2) - int(y1)) * 4 + (last - first) + 1
            if stated >= 1:
                quarters = stated
        return quarters * _WEEKS_PER_QUARTER
    singles = [int(q) for q in _QUARTER_RE.findall(s)]
    if singles:
        return (max(singles) - min(singles) + 1) * _WEEKS_PER_QUARTER
    n_quarters = _N_QUARTERS_RE.search(s)
    if n_quarters:
        return round(float(n_quarters.group(1)) * _WEEKS_PER_QUARTER)

    half_rng = _HALF_RANGE_RE.search(s)
    if half_rng:
        first, last = int(half_rng.group(1)), int(half_rng.group(2))
        return ((last - first) % 2 + 1) * _WEEKS_PER_HALF
    halves = [int(h) for h in _HALF_RE.findall(s)]
    if halves:
        return (max(halves) - min(halves) + 1) * _WEEKS_PER_HALF
    n_halves = _N_HALVES_RE.search(s)
    if n_halves:
        return round(float(n_halves.group(1)) * _WEEKS_PER_HALF)
    if _HALF_YEAR_RE.search(s):
        return _WEEKS_PER_HALF

    if _FULL_YEAR_RE.search(s):
        return _WEEKS_PER_YEAR
    return 0


def parse_duration_to_weeks(text: str | int | float) -> int:
    """'6 months' -> 26, '18 months'/'1.5 years' -> 78, '12 weeks' -> 12,
    a bare int/numeric-only string = weeks. Uses 52/12 weeks-per-month
    CONSISTENTLY with :func:`weeks_to_duration_label`.

    Calendar spans parse as calendar spans, not as a count of weeks: a
    quarter is 13 weeks ('Q4' -> 13, 'Q1-Q3' -> 39, '2 quarters' -> 26), a
    half is 26 ('H1', 'H2', 'half year'), a fiscal or full year is 52
    ('FY', 'FY2026', 'full year'). A stated length wins over the calendar
    anchor ('6 weeks in Q3' -> 6).

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

    span = _calendar_span_weeks(s)
    if span:
        stated = _STATED_LENGTH_RE.search(s)
        if not stated:
            return span
        count, unit = float(stated.group(1)), stated.group(2)
        if unit.startswith("y"):
            return max(0, round(count * _WEEKS_PER_YEAR))
        if unit.startswith("m"):
            return max(0, round(count * _WEEKS_PER_YEAR / 12.0))
        if unit.startswith("d"):
            return max(0, round(count / 7.0))
        return max(0, round(count))  # weeks

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

    Resolution order:
      1. a literal "(~N weeks)" (a label this module produced) -> N, so a
         label always round-trips;
      2. a wizard duration option or an explicit "N weeks/months/years"
         -> the campaign's months (``wizard_inputs.known_campaign_months``,
         the SAME months the budget multiplier uses) at 52/12 weeks per
         month. "6-12 months" is 9 months -> 39 weeks, never the 48-week
         bucket that made a $10,000/month x 9 plan read "over 11 months
         (~48 weeks)" (wizard audit D-05); "24 months" is 104 weeks, not
         the "4 month" substring bucket (24 weeks);
      3. a legacy marketing-bucket phrase (API free text such as
         "long-term"), then :func:`parse_duration_to_weeks` for bare
         numerics. "ongoing" is 52 -- an annual-cycle approximation for
         PHASING math only, never the duration label (see
         :func:`resolve_campaign_duration_label`).
    Unparseable/unrecognized input defaults to 12 weeks, matching the legacy
    ladder's own default.
    """
    s = str(duration_str or "").strip().lower()
    literal = _WEEKS_RE.search(s)
    if literal:
        return max(0, int(literal.group(1)))
    months = wizard_inputs.known_campaign_months(s)
    if months > 0:
        return wizard_inputs.months_to_weeks(months)
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
    # A wizard range option plans at its midpoint months ("3-6 months" ->
    # 4.5): state THOSE months, not the weeks rounded back to 5.
    months = wizard_inputs.known_campaign_months(raw)
    if months > 0 and weeks_int in (0, wizard_inputs.months_to_weeks(months)):
        return duration_label_for_months(months)
    if weeks_int > 0:
        return weeks_to_duration_label(weeks_int)

    if not raw_lower or raw_lower in ("not specified", "tbd", "n/a"):
        return "Not specified"

    derived = resolve_campaign_weeks(raw)
    if derived > 0:
        return weeks_to_duration_label(derived)
    return raw or "Not specified"


def duration_label_for_months(months: float) -> str:
    """Label for a campaign of ``months`` (the months the budget used):
    whole months read as :func:`weeks_to_duration_label` of their weeks;
    a fractional midpoint states itself -- 4.5 -> "4.5 months (~20 weeks)"."""
    weeks = wizard_inputs.months_to_weeks(months)
    if float(months).is_integer() or weeks <= 13:
        return weeks_to_duration_label(weeks)
    return f"{months:g} months (~{weeks} weeks)"


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


def goal_break_even_phrase(budget: Any, goal: Any, money_fn) -> str:
    """THE break-even wording for a hiring goal, shared by the deck's slide-2
    goal line and comparison-slide band and the workbook's goal row (B17)
    and strategic summary: "goal met if hires cost ≤ ₹50K each", where the
    amount is budget ÷ goal formatted by the caller's ``money_fn`` (design-
    judge round 4, F1: three surfaces had three phrasings, one of them the
    plan's own cost per hire rather than the break-even). "" when either
    figure is missing."""
    try:
        budget_f, goal_i = float(budget), int(goal)
    except (TypeError, ValueError):
        return ""
    if budget_f <= 0 or goal_i <= 0:
        return ""
    return f"goal met if hires cost ≤ {money_fn(budget_f / goal_i)} each"


def goal_at_conservative_end(
    projected_hires: Any, goal: Any, hires_low: Any
) -> dict | None:
    """The hiring goal judged at the CONSERVATIVE end of the hires range
    (design-judge round 2, 2026-10-01, item 1): when the headline meets the
    stated goal but every hire costing the industry midpoint (``hires_low``)
    does not, the goal sentence must say so -- "at the midpoint cost the
    plan buys 434 hires against a goal of 500". ``None`` otherwise."""
    try:
        goal_i, projected_i, low_i = int(goal), int(projected_hires), int(hires_low)
    except (TypeError, ValueError):
        return None
    if goal_i <= 0 or projected_i < goal_i or not 0 <= low_i < goal_i:
        return None
    return {"goal": goal_i, "projected": projected_i, "hires_low": low_i}


def goal_gap(
    projected_hires: int,
    goal: int,
    cost_per_hire: float,
    budget: float | None = None,
) -> dict | None:
    """Honest gap statement between the client's stated hiring goal and the
    plan's projected hires. ``None`` when there is no goal to compare against
    (``goal <= 0``) or the plan already meets/exceeds it.

    ``additional_budget`` is what the goal costs at ``cost_per_hire`` minus
    what is already committed: ``goal x cost_per_hire - budget`` when
    ``budget`` is given (else ``(goal - projected) x cost_per_hire``, the
    same figure when the cost per hire is the plan's own budget / hires).
    It is ``None`` -- each surface then omits the figure -- when the cost
    per hire is unknown, or when the plan projects fewer than one hire: a
    top-up multiplied out from a cost the plan never achieves is arithmetic,
    not a plan (a $3,000 plan was told to "add $3,350" for its one hire --
    design-judge, 2026-10-01). ``cost_per_hire`` is echoed back (``None``
    when unknown) so a surface can state the indicative budget for one
    hire instead."""
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
    try:
        budget_f = float(budget) if budget is not None else 0.0
    except (TypeError, ValueError):
        budget_f = 0.0

    pct_of_goal = (projected_i / goal_i) * 100 if goal_i else 0.0
    # cost_per_hire <= 0 means UNKNOWN (typically a plan projecting zero
    # hires, where budget / 0 has no value), not "free". Multiplying the gap
    # by 0 shipped "scaling path: ~$0 additional" on the deck -- an inverted
    # message on exactly the plans whose honesty matters most. Report None
    # and let each surface omit the figure.
    additional_budget = None
    if cph > 0 and projected_i >= 1:
        if budget_f > 0:
            additional_budget = goal_i * cph - budget_f
            if additional_budget <= 0:
                additional_budget = None
        else:
            additional_budget = (goal_i - projected_i) * cph
    return {
        "goal": goal_i,
        "projected": projected_i,
        "pct_of_goal": round(pct_of_goal, 1),
        "additional_budget": (
            round(additional_budget, 2) if additional_budget is not None else None
        ),
        "cost_per_hire": round(cph, 2) if cph > 0 else None,
    }


# English words and common name tokens of 2-4 letters (about 6,561 entries),
# lowercase. Inside an ALL-CAPS name, a token of at most _ACRONYM_MAX_LEN
# letters that is NOT on this list is the client's own acronym and keeps its
# caps ("AWP SAFETY" -> "AWP Safety", "RTI INTERNATIONAL" -> "RTI
# International"); a token that IS on it is an ordinary word and is Title-
# Cased ("NEW YORK LIFE" -> "New York Life"). Built (2026-10-16) from the
# system English word list: every lowercase 3-4 letter entry with a vowel,
# the plural of every 3-letter entry ("lots", "jobs"), and given names of
# 3-4 letters ("sam", "jude"). Removed by hand: shorthand that is not a
# word (sec, val, ust, ...), strings that are real acronyms of employers
# (ama, uva, aya, ...), and the dictionary's non-English noise (3-letter
# entries like "tji" or "ssu"). Added by hand: legal-suffix words (inc,
# corp, co, pty), and brand or place tokens that are not dictionary words
# (york, macy, nike, zara). Two-letter entries are hand-picked: the system
# list's own 2-letter entries are obscure ("ab", "ae"). A real acronym that
# is also a word (SAP, ING) goes in _CLIENT_ACRONYMS, which is consulted
# first. The dictionary tail is obscure on purpose: its only cost is a rare
# word shadowing a rare acronym. A common word or name MISSING here stays
# shouted -- add it. Entries must be 2-4 ASCII letters, lowercase.
_SHORT_WORDS: frozenset[str] = frozenset(
    """
        aals aams abac abas abbs abed abet abey abir able ably abox abus abut aby abys
        acca ace acer aces ache achs achy acid acle acme acne acor acre act acta acts
        actu acyl adad adam adat adaw aday add adda adds ades adet adit admi ado ados
        adry ady adys adz adze adzs aeon aero aers aery aess afar affa affy aft afts
        aga agal agar agas age aged agee agen ager ages agha agio agla ago agog agon
        agos agre agua ague agy agys aha ahas ahem ahey aho ahos ahoy ahu ahum ahus
        aid aide aids aiel ail aile ails aim aims aint aion air aire airs airt airy
        ait aits ajar ajog aka akas ake akee akes akey akia akin ako akos akov akra
        aku akus ala alan alar alas alba albe albs alco ale alec alee alef alem alen
        ales alex alf alfa alfs alga alif alin alit alk alks alky all alls ally alma
        alme alms alns alod aloe alop alos alow alp alps also alt alto alts alum aly
        alys amah amar amas amba ambo ame amen ames ami amic amid amil amin amir amis
        amla amli amma ammo ammu amok amor amos amp amps amra amts amy amyl amys an
        ana anal anam anan anas anay anba and anda ands andy anes anew ango ani anil
        anis ankh ann anna anne anns anoa anon ansa ansu ant anta ante anti ants antu
        anus any anys apa apar apas ape aper apes apex apii apio apod apse apt apts
        aqua aquo ara arad arar aras arba arc arca arch arcs ardu are area ared ares
        argo aria arid aril ark arks arm arms army arna arne arni arns arow arse art
        arts arty aru arui arus arxs ary aryl arys as asak asci asda ase asem ases ash
        ashs ashy ask asks asok asop asor asp asps asse assi assn asss asta asts at
        atap ate atef ates atip atis atle atma atmo atom atop atry atta atwo aube auca
        audi auge augh auhs auk auks aula auld aulu aums aune aunt aura ausu aute auto
        ava aval avas ave aver aves avid avis avo avon avos avow awa awag awas awat
        away awds awe awee awes awfu awin awl awls awns awny awry axal axe axed axel
        axes axil axis axle axon ayah aye ayes ayin ayus azo azon azos azox baa baal
        baar baas baba babe babu baby bac bach back bacs bad bade bads baes baff baft
        bag baga bago bags bah baho bahs baht bail bain bait baka bake baku bal bald
        bale bali balk ball balm bals balu bam bams ban banc band bane bang bani bank
        bans bant bap baps bar bara barb bard bare bari bark barm barn bars bart baru
        bas base bash bask bass bast bat bate bath bats batt batz baud baul baun baw
        bawd bawl bawn baws bay baya bays baze be bea bead beak beal beam bean bear
        beat beau beck bed beds bee beef beek been beer bees beet beg bego begs behn
        bel bela beld belk bell bels belt bely bema ben bena bend bene beng beni benj
        benn beno bens bent ber bere berg berm bers bert bes besa bess best bet beta
        beth bets bevy bey beys bhat bhoy bhut bias bib bibi bibs bice bick bid bide
        bids bien bier biff big biga bigg bigs bija bike bikh bile bilk bill bilo bin
        bind bine bing binh bink bino bins bint biod bion bird biri birk birl birn
        birr bis biss bit bite biti bito bits bitt biwa biz bizs bizz blab blad blae
        blah blan blas blat blaw blay bleb bled blee bleo blet blip blob bloc blos
        blot blow blub blue blup blur boa boar boas boat bob boba bobo bobs boce bock
        bod bode bods body bog boga bogo bogs bogy boho boid boil bojo boke bola bold
        bole bolk boll bolo bolt bom bomb boms bon bond bone bong bonk bons bony boo
        boob bood boof book boom boon boor boos boot bop bops bora bord bore borg borh
        born boro bors bort bose bosh bosk bosn boss bot bota bote both bots bott boud
        bouk boun bout bouw bow bowk bowl bows box boxy boy boyd boys boza bozo bra
        brab brad brae brag bran bras brat braw bray bred bree brei bret brew brey
        brig brim brin brit brob brod brog broo bros brot brow brut bual bub buba bubo
        bubs buck bud buda buds buff bufo bug bugs buhl buhr bukh bulb bulk bull bult
        bum bump bums bun buna bund bung bunk buns bunt buoy bur burd bure burg buri
        burl burn buro burp burr burs burt bury bus bush busk buss bust busy but buts
        butt buy buys buzz by bye byee byes bygo byon byre byth caam cab caba cabs
        cack cad cade cadi cads cafe cafh cag cage cags caid cain cake caky cal calf
        calk call calm calp cals calx cam camb came camp cams can cane cank cans cant
        cany cap cape caph caps car card care cark carl carp cars cart case cash cask
        cass cast cat cate cats cauk caul caum caup cava cave cavy caw cawk caws cay
        cays caza cede cee cees ceil cel cell cels celt cent cep cepa cepe ceps cere
        cern cero cess cest ceti chaa chab chad chai chal cham chao chap char chas
        chat chaw chay chee chef ches chet chew chia chic chid chih chil chin chip
        chis chit chob chol chop chos chow chub chug chum chun chut cig cigs cine cion
        cipo cise cist cite cits city cive clad clag clam clan clap clat claw clay
        cled clee clef cleg clem clep clew clip clit clod clog clop clot clow cloy
        club clue clys co coak coal coat coax cob cobs coca cock coco cod coda code
        codo cods coed coes coff coft cog cogs coho coif coil coin coir coke coky col
        cola cold cole coli colk coll colp cols colt coly coma comb come con cond cone
        conk conn cons cony coo coof cook cool coom coon coop coos coot cop copa cope
        copr cops copy cor cora cord core corf cork corm corn corp cors cory cos cosh
        coss cost cosy cot cote coth coto cots coty coue coul coup cove cow cowl cows
        cowy cox coxa coxs coxy coy coyo coys coz coze cozs cozy crab crag cram cran
        crap craw crea cree crew crib cric crig crin cris croc crop cros crow croy
        crum crus crux cry crys cub cube cubi cubs cuck cud cuds cue cues cuff cuir
        cuke cull culm cult cum cump cums cup cups curb curd cure curl curn curs curt
        cush cusk cusp cuss cut cute cuts cuvy cuya cyan cyke cyma cyme cyp cyps cyst
        czar dab dabb dabs dace dad dada dade dado dads dae daer daes daff daft dag
        dags dah dahs dain dais dak daks dal dale dali dalk dals dalt dam dama dame
        damn damp dams dan dana dand dang dani dank dans dao daos dap daps dar dare
        darg dari dark darn dars dart dash dasi dass data date daub daud daut dauw
        dave davy daw dawn daws day days daze dazy dead deaf deal dean dear deb debi
        debs debt deck dedo dee deed deem deep deer dees deft defy degs degu del dele
        delf dell deme demy den dene dens dent deny depa dept dere derm dern desi desk
        dess deul deva dew dews dewy dey deys dhai dhak dhan dhas dhaw dhow dhus dial
        dian dib dibs dice dich dick did dids die dieb diem dier dies diet dig digs
        dika dike dill dilo dim dime dims din dine ding dink dins dint diol dion dip
        dips dird dire dirk dirl dirt disc dish disk diss dit dita dite dits diva dive
        dixy do doab doat dob dobe dobs doby doc dock docs dodd dodo dods doe doer
        does doff dog doge dogs dogy doit doke dola dole doli doll dolt dom dome domn
        doms domy don done dong donn dons dont doob dook dool doom doon door dop dopa
        dope dops dor dori dorm dorn dorp dors dory dos dosa dose doss dot dote dots
        doty douc doug doum doup dour dout dove dow dowd dowf dowl down dowp dows doxa
        doxy doze dozy drab drag dram drat draw dray dree dreg drew drib drip drop
        drow drub drug drum dry drys duad dual dub dubb dubs duck duct dud dude duds
        due duel duer dues duet duff dug dugs duim duit duke dull dult duly dum duma
        dumb dump dums dun dune dung dunk duns dunt duny duo duos dupe dups dura dure
        durn duro dush dusk dust duty dux duxs dyad dyce dye dyer dyes dyke dyne each
        eans ear earl earn ears ease east easy eat eats eave ebb ebbs eboe ebon ecad
        eche echo ecru eddo eddy edea edge edgy edhs edit eel eels eely eers eft efts
        egad egg eggs eggy egma ego egol egos eheu ejoo eke eker ekes ekka elbs elds
        elf elfs eli elk elks ell elle ells elm elms elmy elod els elsa else elss elt
        elts eme emes emil emir emit emma empt emu emus emyd enam end ends enol enow
        ens ense enss envy eoan eon eons epee epha epic epos era eral eras erds ere
        eres erg ergs eria eric erik erne eros err errs erss erth eruc esca esne espy
        ess esss eta etas etch etna etsy etua etui etym euge eva evan eve even ever
        eves evil evoe ewe ewer ewes ewry exam exes exit exon eyah eyas eye eyed eyen
        eyer eyes eyey eyne eyns eyot eyra eyre ezba face fack fact facy fad fade fads
        fady fae faes faff fag fage fags fail fain fair fake faky fall falx fam fame
        fams fan fana fand fang fans fant faon far fare farl farm faro fars fash fass
        fast fat fate fats faun favn fawn fay fays faze feak feal fear feat feck fed
        feds fee feed feel feer fees fei feif feil feis fell fels felt feme fen fend
        fens fent feod ferk fern feru fess fest fet fets feu feud feus few fews fey
        feys fez fezs fiar fiat fib fibs fice fids fie fies fife fifo fig figs fike
        file fill film filo fils fin find fine fink fins fip fir fire firk firm firn
        firs fisc fise fish fist fit fits five fix fixs fizz flag flak flam flan flap
        flat flaw flax flay flea fled flee flet flew flex fley flip flit flix flob
        floc floe flog flop flot flow flu flub flue flus flux fly flys foal foam fob
        fobs foci fod fods foe foes fog fogo fogs fogy foil fold fole folk fond fono
        fons font foo food fool foos foot fop fops for fora forb ford fore fork form
        fors fort fosh fot fots fou foud foul foun four fous fow fowk fowl fows fox
        foxs foxy foy foys fozy frab frae frap fras frat fray fred free fret frib frig
        frim frit friz fro froe frog from fros frot frow fry frys fub fubs fuci fud
        fuds fuel fuff fug fugs fugu fuji fulk full fum fume fums fumy fun fund funk
        funs funt fur furl furs fury fusc fuse fuss fust fut fute futs fuye fuze fuzz
        fyke fyrd gab gabi gabs gaby gad gade gads gaen gaet gaff gag gage gags gail
        gain gair gait gaj gajs gal gala gale gali gall galp gals galt gam gamb game
        gamp gams gamy gan gane gang gans gant gaol gap gapa gape gapo gaps gapy gar
        gara garb gare garn gars gary gas gash gasp gass gast gat gata gate gats gau
        gaub gaud gaum gaun gaup gaur gaus gaut gave gaw gawk gawm gawn gaws gay gays
        gaz gaze gazi gazs gazy geal gean gear geat geck ged geds gee geek gees geet
        gegg gein gel geld gell gels gelt gem gems gen gena gene gens gent genu ger
        gerb germ gers gest get geta gets geum gey geys gez gezs ghat ghee gib gibe
        gibs gid gids gie gied gien gies gifs gift gig gigi gigs gil gild gill gilo
        gilt gim gimp gims gin ging gink gins gio gios gip gips gird girl girn giro
        girr girt gish gist gith gits give gizz glad glam glar glee gleg glen glia
        glib glom glop glor glow gloy glub glue glug glum glut gnar gnat gnaw gnu gnus
        go goa goad goaf goal goas goat gob gobi gobo gobs goby god gode gods goel
        goer goes goff gog gogo gogs goi gois gol gola gold golf goli gols gon gone
        gong gons gony goo good goof gook gool goon goos gor gora gorb gore gors gory
        gos gosh goss got gote gots goup gout gove gowf gowk gowl gown goy goys grab
        grad gram gras grat gray greg grew grey grid grig grim grin grip gris grit
        grog gros grot grow grub grue grum grun guan guao guar gud gude guds gue gues
        gufa guff gugu guhr guib gul gula gule gulf gull gulp guls gum gump gums gun
        guna gunj gunk gunl guns gup gups gurk gurl gurr gurs gurt guru gush guss gust
        gut guts gutt guy guys guz guze guzs gwag gyle gym gyms gyn gyne gyns gyp gype
        gyps gyre gyri gyro gyte gyve haab haaf habu hack had hade hadj hads haec haem
        haet haff haft hag hagi hags hah hahs haik hail hain hair haje hak hake hako
        haks haku hal hala hale half hall halo hals halt ham hame hami hams han hand
        hank hans hant hao haos hap haps hapu hard hare hark harl harm harn harp harr
        hart harv hash hask hasp hat hate hath hats hatt hau haul haus have haw hawk
        hawm haws hay haya hays hayz haze hazy he head heaf heal heap hear heat hech
        heck heed heel heer heft hei heii hein heir heis hele hell helm help hem heme
        heml hemp hems hen hend hens hent hep heps her herb herd here herl hern hero
        hers hess hest hets hevi hew hewn hews hewt hex hexa hexs hey heys hia hias
        hic hick hics hid hide hids hie hies high hike hill hilt him himp hims hin
        hind hing hins hint hip hipe hips hire hiro his hish hisn hiss hit hits hive
        hizz hoar hoax hob hobo hobs hock hod hods hoe hoer hoes hog hogs hohn hoi
        hoin hois hoit hoju hold hole holl holm holt holy home homo homy hon hone hong
        honk hood hoof hook hoon hoop hoot hop hope hopi hops hora horn hory hose host
        hot hoti hots hour hove how howe howk howl hows hox hoxs hoy hoys hub hubb
        hubs huck hud huds hue hued huer hues huey huff hug huge hugh hugs huh huhs
        hui huia huke hula hulk hull hulu hum hume hump hums hung hunh hunk hunt hup
        hups hura hure hurf hurl hurr hurt huse hush husk huso huss hut huts huzz hyke
        hyle hymn hyne hyp hypo hyps iamb ian iaos iba ibas ibex ibid ibis ice iced
        ices icho ichs ichu icon icy icys ide idea ides idic idle idly idol idyl if
        ife ifes iffy ihi ihis iiwi ijma ikat ikey ikra ilex ilia ilk ilka ilks ill
        ills illy ilot ilya ima imam imbe imi imis immi imp impi imps impy imu imus in
        inbe inby inc inch inde indy info ings ink inks inky inly inn inns inro into
        iodo ion ions iota ipid ipil ira ire ires irid iris irk irks irok iron is isba
        isle ism isms ismy isos ists itch item itmo its itss iva ivas ivin ivy ivys
        iwa iwas iwis iyos izar izle izzy jab jabs jack jacu jade jady jag jags jail
        jake jako jam jama jamb jami jams jan jane jank jann jaob jap jape japs jar
        jara jarg jarl jars jass jati jato jauk jaun jaup jaw jaws jawy jay jays jazz
        jean jed jeds jeel jeep jeer jef jeff jehu jell jem jerk jerl jerm jert jess
        jest jet jete jets jhow jib jibe jibi jibs jiff jig jigs jill jilt jim jimp
        jin jing jink jinn jinx jiri jiti jiva jive job jobo jobs joch jock jocu joe
        joel joes joey jog jogs john join joke joky joll jolt jon jong joni joom joon
        jos jose josh joss jot jota jots joug jouk jow jowl jows joy joys juan juba
        jube juck jud jude judo juds judy jug jugs juha juju juke jump jun june junk
        junt jupe jure juri jury just jut jute juts jynx kago kagu kaha kahu kai kaid
        kaik kail kais kaj kaka kaki kale kalo kame kan kana kang kans kapa kapp kari
        karl karo kasa kasm kat kate kath kats kava kay kayo kays kazi kea keas keb
        kebs ked keds kee keek keel keen keep kees keet kef kefs keg kegs keld kele
        kelk kell kelp kelt kemb kemp ken kend kenn keno kens kent kep kepi keps kept
        kerf kern ket keta keto kets keup kex kexs kexy key keys khan khar khat khet
        khir khot khus kibe kiby kick kid kids kier kike kiki kiku kil kill kiln kilo
        kilp kils kilt kim kims kin kina kind king kink kino kins kip kipe kips kiri
        kirk kirn kish kiss kist kit kite kith kits kiva kivu kiwi kiyi klam klip klom
        klop kmet knab knag knap knar knee knet knew knez knit knob knop knot know
        knub knur knut koa koae koas kob kobi kobs kobu koda koel koff koft kohl koi
        koil kois koko koku kola kolo kon kona kons kop koph kopi kops kor kora kore
        kori kors kory kos koss koto kou kous kozo kral kran kras kris kuan kuba kudu
        kuei kuge kuku kula kulm kung kunk kurt kusa kwan kyah kyar kyat kyl kyle kylo
        kyls kyte kyu lab labs lace lack lacy lad lade lads lady laet lag lags lai
        laic laid lain lair lais lak lake laks laky lall lalo lam lama lamb lame lamp
        lams lan lana land lane lank lans lant lanx lap laps lar lard lari lark lars
        las lasa lash lask lass last lat lata late lath lats laud laun laur lava lave
        law lawk lawn laws lax laxs lay lays laze lazy lea lead leaf leak leal leam
        lean leap lear leas leat lech leck led lede leds lee leed leek leep leer lees
        leet left leg legs lehr lei leif leis lek leks len lend lene leno lens lent
        leo leon lepa lerp les less lest let lete lets leu leud leuk leus lev levo
        levs levy lew lewd lews lex ley leys liar lice lich lick lid lido lids lie
        lied lief lien lier lies lieu life lifo lift liin lija like lila lile lill
        lilt lily lim limb lime limn limo limp lims limu limy lin lina line ling link
        linn lino lins lint liny lion lip lipa lips lira lire lis lisa lish lisk lisp
        liss list lit lite lith lits litz live liz liza llyn loa load loaf loam loan
        loas lob lobe lobo lobs loca loch loci lock loco lod lode lods lofs loft log
        loge logo logs logy loin loir lois loka loke loll lone long loo lood loof look
        loom loon loop loos loot lop lope lops lora lord lore lori lorn loro lors lory
        lose losh loss lost lot lota lote lots lou loud louk loup lour lout love low
        lowa lowe lown lows lowy lox loxs loy loyd loys lube luc luce luck lucy ludo
        lue lues luff lug luge lugs lui luis luke lull lulu lum lump lums luna lune
        lung lunn lunt lupe lura lure lurg lurk lush lusk lust lut lute luts lux luxe
        luxs lyam lye lyes lyft lynn lynx lyra lyre lys lyse lyss maam mabi mac mace
        mack maco macs macy mad made mado mads mae maes mag mage magi mags mah maha
        maid mail maim main majo make maki mako mal mala male mali mall malm malo mals
        malt mamo man mana mand mane mang mani mank mann mano mans mant many mao maos
        map mapo maps mar mara marc mare mark marl marm maro mars mart maru mary mas
        masa mash mask mass mast masu mat mate math mats matt maty mau maud maul maun
        maus maux maw mawk mawp maws max may maya mayo mays maza maze mazy me mead
        meak meal mean meat med meed meek meet mein meio mel mela meld mele mell mels
        melt memo mems men mend meng mens menu meny mere merk merl mero mesa mese mesh
        meso mess met meta mete mets mew mewl mews mhos mian mias mib mibs mica mice
        mick mico mid mide mids miek mien miff mig migs mijl mike miki mil mila mild
        mile milk mill milo mils milt mim mima mime mimp mims min mina mind mine ming
        mink mino mint minx miny mir mird mire mirk miro mirs miry mise miss mist mite
        mitt mity mix mixs mixy moan moat mob mobs mock mode moe moff mog mogo mogs
        moha moho mohr moil moio moit mojo moke moki moko moky mola mold mole moll
        molt moly mome momo mon mona mone mong monk mono mons moo mood mool moon moop
        moor moos moot mop mope moph mops mor mora more morg morn moro mors mort moss
        most mot mote moth mots mott mou moud moul moup mous mout move mow mown mows
        mowt moxa moy moyo moys much muck mud muds muff mug muga mugg mugs muid muir
        mule mulk mull mult mum mump mums mun mund mung munj muns munt mura mure murk
        muse mush musk muss must muta mute muth mutt mux muxs muzz my myal myna myst
        myth myxa myxo naa naam naas nab nabk nabs nace nach nae nael naes nag naga
        nags naid naif naig naik nail naim nain naio nais nak nake nako naks name nams
        nan nana nane nans nant naos nap napa nape naps napu nard nark narr nars nary
        nash nasi nast nat nate natr nats naut nave navy naw naws nawt nay nays naze
        nea neal neap near neas neat neb nebs neck ned nee need neem neep neer nees
        neet nef nefs nei neif neil neis nema neo neon neos nep neps nese nesh ness
        nest net nete neth neti nets neve nevo new news newt next ngai nhan nib nibs
        nice nici nick nid nide nidi nids nife nig nigh nigs nike nil nils nim nimb
        nims nine ning niog nip nipa nips nit nito nits nix nixs nizy no noa noam noas
        nob nobs nock nod node nodi nods noel nog nogs noil noir noll nolo nome non
        none nons nook noon nope nor nora nori norm nors nose nosh nosy not note nots
        nou noun noup nous nova now nows nowt nowy noxa noy noys nub nubs nude nuke
        nul null nuls numb nun nuns nut nuts nye nyes oaf oafs oak oaks oaky oams oar
        oars oary oast oat oath oats oaty oban obe obes obex obey obi obis obit oboe
        obol ochs ocht ocks oda odal odas odd odds ode odel odes odic odor odso odum
        odyl oer oers oess of ofer off offs oft ofts ogam ogee ogle ogre ogum ohia ohm
        ohms oho ohos ohoy oiis oil oils oily oime oint oka okas okee oket oki okia
        okis okra olaf olam old olds ole oleg oleo olid olio olla olm olms olof olpe
        omao omar omen omer omit on ona onas onca once ondy one oner ones only onss
        onto onus onym onyx onza oofy ooid oons oont oord ooze oozy opah opal ope opel
        open opes opsy opt opts opus or ora orad oral oras orb orbs orby orc orcs ordu
        ore ores orf orfs orgy orle orlo orna orts ory orys ose oses osse otic otto
        ouch oufs ough our ours oust out outs ova oval ovas oven over ovey ovum owds
        owe owen ower owes owks owl owls owly own owns owse oxan oxea oxen oxer oxy
        oxyl oxys oyer ozan paal paar pac paca pace pack paco pacs pact pad pads paga
        page pah paha pahi paho pahs paik pail pain paip pair pais pal pale pali pall
        palm palp pals palt paly pam pams pan pand pane pang pank pans pant paon pap
        papa pape paps par para pard pare pari park parr pars part pash pasi pass past
        pat pata pate path pato pats patu paty pau paul paup paus paut pave pavy paw
        pawk pawl pawn paws pax paxs pay pays pea peag peai peak peal pean pear peas
        peat peba pech peck ped peda peds pee peed peek peel peen peep peer pees peet
        peg pega pegs peho pelf pell pelt pelu pen pend penk pens pent peon pep pepo
        peps per peri perk perm pern pers pert pesa peso pess pest pet pete peto petr
        pets pew pews pewy pfui phew phi phil phis phit phiz phoh phon phoo phos phot
        phus phut pia pial pian pias pic pica pice pick pico pics pict pie pied pien
        pier pies piet piff pig pigs pik pika pike piki piks piky pile pili pill pilm
        pily pim pimp pin pina pind pine ping pink pino pins pint piny pip pipa pipe
        pipi pips pipy pir pirl pirn pirr pirs pise pish pisk piss pist pit pita pith
        pits pity pix pixs pixy pize plak plan plap play plea pleb pled plew plex plim
        plod plop plot plow ploy plud plug plum plup plus ply plys pob pobs pock poco
        pod pods poe poem poes poet pogy poh poha pohs poi poil pois poke poky pol
        pole polk poll polo pols polt poly pom pome pomp poms pon pond pone pong pons
        pont pony pooa poof pooh pook pool poon poop poor poot pop pope pops pore pork
        porr port pory pose posh poss post posy pot pote pots pott pouf pour pout pow
        pows pox poxs poxy poy poys prad pram prat prau pray prey pria prig prim pro
        proa proo prop pros prow pruh prut pry prys psha psi psis pty pua puan puas
        pub pubs puce puck pud puds pudu puff pug pugh pugs puja puka puke puku puky
        pul pule puli pulk pull pulp puls pulu puly puma pump pun puna pung punk puns
        punt puny pup pupa pups pure purl purr purs pus push puss put puts putt puxy
        pya pyal pyas pyic pyin pyke pyla pyr pyre pyro pyrs pyx pyxs qere qeri qoph
        qua quab quad quag quan quar quas quat quaw quay quei quet quey quib quid quin
        quip quis quit quiz quo quod quop quos raad rab rabs race rach rack racy rad
        rada rads raff raft rag raga rage rags rah rahs raia raid raif rail rain rais
        raj raja rajs rake rakh raki raku rale ralf ram rame rami ramp rams ran rand
        rane rang rani rank rann rans rant rap rape raps rapt rare rase rash rasp rass
        rat rata rate rath rats rauk raul raun rave ravi raw raws rax raxs ray rays
        raze razz rea read reak real ream reap rear reas reb rebs reck red redd rede
        redo reds ree reed reef reek reel reem reen rees reet ref refs reft reh rehs
        reid reif reim rein reis reit rely rend renk rent renu rep repp reps resh rest
        rets rev reve revs rex rexs rhea rhes rho rhos ria rial rias rib ribe ribs ric
        rice rich rick rid ride rids rie riem rier ries rife riff rift rig rigs rik
        rikk rile rill rim rima rime rims rimu rimy rind rine ring rink rio rios riot
        rip ripa ripe rips rise risk risp rist rit rita rite riva rive rix rixs rixy
        road roam roan roar rob robe robs roc rock rocs rod rodd rode rods roe roed
        roer roes roey rog rogs roi roid roil rois roit roka roke roky role rolf roll
        romp ron rond rone rood roof rook rool room roon root rope ropp ropy rory rose
        ross rosy rot rota rote roto rots roub roud roue roun roup rout rove row rows
        rowy rox roxs roxy roy royt rub rubs ruby ruck rud rudd rude ruds rudy rue
        ruen ruer rues ruff rug ruga rugs ruin rukh rule rull rum rump rums run rune
        rung runs runt rupa ruru ruse rush rusk russ rust rut ruth ruts rux ruxs ryal
        rye ryen ryes ryme rynd rynt ryot rype saa saab saad sab sabe sabs sac sack
        saco sacs sad sade sadh sado sadr sads safe saft sag saga sage sago sags sagy
        sah sahh sahs sai saia said sail saim sain saip sair sais saj sajs sake saki
        sal sale salp sals salt sam same samh samp sams san sand sane sang sank sans
        saos sapa saps sar sard sare sari sark sars sart sasa sash sass sat sate sats
        sauf saul saum saur saut save saw sawn saws sawt sax saxs say saya says scab
        scad scam scan scap scar scat scaw scho scob scog scot scow scry scud scug
        scum scun scup scur scut scye scyt sea seah seak seal seam sean sear seas seat
        seax sech seck sect see seed seek seel seem seen seep seer sees sego seit sele
        self sell selt seme semi sen send sent seps sept sera sere serf sero sers sert
        set seta seth sets sett sew sewn sews sex sexs sext sexy sey seys shab shad
        shag shah shai sham shan shap shas shat shaw shay she shea shed shee shel sher
        shes shih shim shin ship shis shiv shod shoe shog shoo shop shoq shor shos
        shot shou show shug shul shun shut shy shys siak sial sib sibs sic sice sick
        sics sid side sidi sidy sie sier sies sife sift sigh sign sigs sika sike sil
        sile silk sill silo sils silt sima sime simp sin sina sind sine sing sinh sink
        sins siol sion sip sipe sips sir sire sirs sis sise sish sisi siss sist sit
        site sith sits siva six sixs size sizy sizz skag skal skat skaw skee skef skeg
        skel sken skeo skep sker skew ski skid skil skim skin skip skis skit skiv skoo
        skua skun sky skys slab slad slae slag slam slap slas slat slaw slay sled slee
        slew sley slid slim slip slit slob slod sloe slog slon sloo slop slot slow
        slub slud slue slug slum slur slut sly slys smas smee smew smit smog smug smur
        smut snab snag snap snaw sneb sned snee snew snib snig snip snob snod snog
        snop snot snow snub snug snum snup snur snys so soak soam soap soar sob sobs
        soce sock soco socs sod soda sods sody soe soes sofa soft sog sogs soho sohs
        soil soja sok soke soks sol sola sold sole soli solo sols soma some son sond
        song sonk sons sook sool soon soot sop sope soph sops sora sorb sore sori sorn
        sort sory sosh soso soss sot sots sou soud soul soum soup sour sov sovs sow
        sowl sown sows sowt soy soya soys spa spad spae spak spam span spar spas spat
        spay spec sped spet spew spex spig spin spit spiv spor spot spry spud spug
        spun spur sput spy spys sri sris ssus stab stag stam stan stap star staw stay
        steg stem sten step stet stew stey stib stid stim stir stoa stob stod stof
        stog stop stot stow stra stre stub stud stue stug stum stun stut sty stys sub
        subs such suck sud sudd suds sue suer sues suet sugh sugi suid suit suji suld
        sulk sull sum sump sums sun sune sung sunk sunn suns sunt sup supe sups sura
        surd sure surf surs susi susu suu suum suwe suz suzs swab swad swag swam swan
        swap swas swat sway swep swig swim swiz swob swom swot swow swum syce syd sye
        syed syes sync syne syre syrt syun taa taar taas tab tabs tabu tach tack taco
        tact tad tade tads tae tael taen taes taft tag tags taha tahr tai tail tain
        tais tait taj tajs take takt taky tal tala talc tald tale tali talk tall tals
        tam tame tamp tams tan tana tane tang tanh tank tans tao taos tap tapa tape
        taps tapu tar tara tare tari tarn taro tarp tarr tars tart tash task tass tasu
        tat tata tate tath tats tatu tau taum taun taur taus taut tave tavs taw tawa
        tawn taws tax taxi taxs taxy tay tays tche tchu tea tead teak teal team tean
        teap tear teas teat teca tech teck tecs ted teds tee teel teem teen teer tees
        teet teff teg tegs teil teju tele teli tell telt ten tend teng tens tent tera
        teri term tern terp test tete teth teuk tew tews text tez tezs thad than thar
        thas that thaw the theb thee them then theo thes thew they thig thin thio thir
        this thob thof thon thoo thos thou thow thro thud thug thus thy thys tiao tiar
        tib tibs tic tice tick tics tide tids tidy tie tied tien tier ties tiff tift
        tige tigs til tile till tils tilt tim time timo tin tind tine ting tink tins
        tint tiny tip tipe tips tire tirl tirr tit tite titi tits tivy tiza tjis to
        toa toad toas toat tobe toby tock toco tod todd tode tods tody toe toed toes
        toff toft tofu tog toga togs togt toho toi toil tois toit toke toko told tole
        toll tols tolt tolu tom tomb tome toms ton tone tong tonk tons tony too took
        tool toom toon toop toos toot top tope toph topi topo tops tor tora torc tore
        torn toro tors tort toru tory tosh toss tost tosy tot tote toto tots toty tou
        toug toup tour tous tout tow towd town tows towy tox toxa toxs toy toys toze
        trag trah tram tran trap tras tray tree tref trek tret trey trig trim trin
        trio trip tris trod trog tron trot trow troy trub true trug trun try tryp trys
        tryt tsar tsia tsun tua tuan tub tuba tube tubs tuck tues tufa tuff tuft tug
        tugs tui tuik tuis tuke tula tule tum tume tump tums tun tuna tund tune tung
        tunk tuno tuns tunu tuny tup tups tur turb turd turf turk turm turn turp turr
        turs tush tusk tut tute tuth tuts tutu tuwi tux tuxs tuza twae twal twas twat
        tway twee twig twin twit two twos tye tyee tyes tyg tygs tyke tymp tynd type
        typo typp typy tyre tyro tyt tyts uang uber ubi ubis udal udo udos ughs ugly
        uily uji ujis uke ukes ula ulas ule ules ulex ulla ulls ulmo ulna ulta ulu
        ulua ulus umbo ume umes ump umph umps umu umus unal unau unbe unca unci unco
        unde undo undy unie unio unit unto untz unze up upas updo upgo upla upo upon
        upos ura ural uran urao uras urde urds ure urea ures urfs urge uric urn urna
        urns urus urva usar use used usee user uses ushs usts uta utai utas utch utu
        utum utus uval uvas uvea uvic uvid uzan vade vady vag vage vags vail vain vair
        vale vali vall vamp van vane vang vans vara vare vari vary vas vasa vase vass
        vast vasu vat vats vau vaus veal vee veen veep veer vees vei veil vein veis
        vela vell velo vend vent vera verb verd veri vern vert very vest vet veta veto
        vets vex vexs vext via vial vias vic vice vick vie vier vies view viga vila
        vile vill vim vims vina vine vino vint viny viol vire virl visa vise viss vita
        viva vive vlei voar voe voes voet vog vogs void vole vols volt vota vote vow
        vows vug vugs vuln vum vums waag wab wabe wabs wace wack wad wade wadi wads
        wae waeg waer waes waff waft wag wage wags wah wahs waif waik wail wain wait
        waka wake wakf waky wale wali walk wall walt wame wamp wan wand wane wang wans
        want wany wap wapp waps war ward ware warf wark warl warm warn warp wars wart
        wary was wase wash wasp wass wast wat wath wats watt wauf waul waup waur wave
        wavy waw wawa waws wax waxs waxy way ways we weak weal weam wean wear web webs
        wed wede weds wee weed week weel ween weep wees weet weft weir weka weki weld
        welk well wels welt wem wems wen wend wene wens went wept wer were werf weri
        wers wert wes wese west wet weta wets weve wey weys wham whan whap whar whas
        what whau whee when whet whew whey whid whig whim whin whip whir whit whiz who
        whoa whom whoo whop whos whud whun whup whuz why whyo whys wice wick wid wide
        wids widu wife wig wigs wild wile wilk will wilt wily wim wime wimp wims win
        wind wine wing wink wins wint winy wipe wips wir wird wire wirl wirr wirs wiry
        wis wise wish wisp wiss wist wit wite with wits wive wiz wizs woad woak woan
        wob wobs wode wods woe woes woft wog wogs wok woke woks wold wolf womb won
        wone wong wons wont woo wood woof wool woom woon woos wop wops word wore work
        worm worn wort wot wote wots wouf wove wow wows wowt woy woys wran wrap wraw
        wren wrig writ wros wrox wry wrys wud wuds wudu wugg wulk wull wun wuns wup
        wups wur wurs wush wusp wuss wust wut wuts wuzu wyde wye wyes wyke wyle wyn
        wynd wyne wynn wyns wype wyss wyve xyla xyst yaba yabu yade yads yaff yagi
        yahs yair yaje yak yaks yalb yale yali yam yamp yams yang yank yans yap yapa
        yapp yaps yarb yard yare yark yarl yarm yarn yarr yars yass yate yati yats
        yaud yava yaw yawl yawn yawp yaws yawy yaya ycie yday yea yeah yean year yeas
        yeat yede yeds yeel yees yegg yeld yelk yell yelm yelp yelt yen yeni yens yeos
        yep yeps yerb yerd yere yerk yern yers yes yese yeso yess yest yet yeta yeth
        yets yeuk yew yews yexs yezs yigh yill yilt yin yins yip yips yird yirk yirm
        yirn yirr yiss yite yobi yock yodh yoes yoga yogh yogi yois yoke yoks yoky
        yolk yoms yon yond yons yont yook yoop yore york yors yote yots you youd youl
        youp your yous yowl yows yowt yoxs yuan yuca yuck yuft yuhs yule yurt yuss
        yutu zac zacs zad zads zag zags zain zak zaks zant zany zar zara zarf zarp
        zars zat zati zats zax zaxs zeal zebu zed zeds zee zeed zees zein zel zels
        zemi zen zenu zer zero zest zeta zig zigs zimb zinc zing zink zip zips zira
        zizz zoa zoas zobo zoea zogo zoic zoid zoll zone zoo zoom zoon zoos zuza zyga
        zyme
    """.split()
)
