"""Where a channel's budget actually goes, and which funded channels project no hires.

Two plan-honesty helpers shared by the workbook, the deck and Nova:

* ``regional_budget_split`` -- the "Regional & aggregator boards" budget used
  to sit under one unnamed row. It is split here across the job boards the
  supply data confirms as in the Joveo network for each plan country. Boards
  are chosen by a local-relevance SCORE, not by how many countries a board
  lists: a multi-country board that is strong in a market (market-share list,
  supply-team performance note, Joveo-built plans, role fit, programmatic
  feed, ...) is funded as a local board. The top boards are funded in
  proportion to their score; the next ones are listed as standby with no
  budget; boards bought by posting or subscription are listed apart as "post
  directly". Sub-amounts sum exactly to the channel budget.
* ``zero_hire_channels`` / ``zero_hire_summary`` -- under a plan that
  projects hires, a funded channel that projects none is labelled as an
  awareness / test budget instead of reading like a failed buy.

No figure here is invented: amounts are the plan's own channel budget, split
by the score; every board and priority is a supply-repository row, and every
score signal traces to a data file (market-share list, supply performance
notes, plan-evidence boards, global supply) or a repository field.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

import publisher_filter

logger = logging.getLogger(__name__)

AWARENESS_TEST_LABEL = "Awareness / test budget — no hires projected at this budget"
STANDBY_LABEL = "Standby: reallocate if a funded board under-delivers"
FUNDED_BOARD_COUNT = 5
STANDBY_BOARD_COUNT = 10

# Boards already bought under the plan's other job-board channel
# (excel_v2._CATEGORY_PLATFORM_CANDIDATES["job_board"]): splitting the
# regional budget across them would count the same spend twice.
_OTHER_CHANNEL_BOARDS: Tuple[str, ...] = ("Indeed", "LinkedIn", "Glassdoor")

_REGIONAL_WORDS: Tuple[str, ...] = ("regional", "local", "apac", "emea")


def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _alloc_dollars(ch: Any) -> float:
    if not isinstance(ch, dict):
        return 0.0
    return _num(ch.get("dollar_amount", ch.get("dollars") or 0))


def channel_display_name(key: Any, ch: Any) -> str:
    label = str((ch or {}).get("label") or "") if isinstance(ch, dict) else ""
    return label or str(key).replace("_", " ").title()


def is_regional_channel(key: Any, ch: Any = None) -> bool:
    text = (
        f"{key} {(ch or {}).get('label', '') if isinstance(ch, dict) else ''}".lower()
    )
    return any(word in text for word in _REGIONAL_WORDS)


# ---------------------------------------------------------------------------
# Zero-hire funded channels
# ---------------------------------------------------------------------------


def zero_hire_channels(
    budget_alloc: Any, hires_suppressed: bool = False
) -> List[Dict[str, Any]]:
    """Funded channels that project no hires, as ``[{"key", "name", "dollars"}]``.

    Empty when hires are suppressed for the plan (nothing is projected, so no
    channel is singled out) or when the plan projects no hires at all (then
    every channel reads zero and singling out a few would mislead)."""
    if hires_suppressed or not isinstance(budget_alloc, dict):
        return []
    allocs = budget_alloc.get("channel_allocations")
    if not isinstance(allocs, dict):
        return []
    funded = [(k, c) for k, c in allocs.items() if _alloc_dollars(c) > 0]
    if sum(int(_num(c.get("projected_hires"))) for _, c in funded) <= 0:
        return []
    return [
        {
            "key": str(k),
            "name": channel_display_name(k, c),
            "dollars": _alloc_dollars(c),
        }
        for k, c in funded
        if int(_num(c.get("projected_hires"))) <= 0
    ]


def zero_hire_summary(
    budget_alloc: Any,
    hires_suppressed: bool = False,
    money: Optional[Callable[[float], str]] = None,
) -> str:
    """ "<N>% of budget (<amount>) funds channels that project no hires at this
    budget: <names>." -- '' when no funded channel is in that position."""
    rows = zero_hire_channels(budget_alloc, hires_suppressed)
    if not rows:
        return ""
    allocs = (budget_alloc or {}).get("channel_allocations") or {}
    total = sum(_alloc_dollars(c) for c in allocs.values())
    spend = sum(r["dollars"] for r in rows)
    if total <= 0 or spend <= 0:
        return ""
    fmt = money or (lambda v: f"{v:,.0f}")
    pct = round(spend / total * 100)
    names = ", ".join(r["name"] for r in rows)
    return (
        f"{pct}% of budget ({fmt(spend)}) funds channels that project no hires "
        f"at this budget: {names}."
    )


# ---------------------------------------------------------------------------
# Regional & aggregator budget split
# ---------------------------------------------------------------------------


def _largest_remainder_cents(total_cents: int, weights: List[float]) -> List[int]:
    """Split ``total_cents`` by ``weights`` into whole cents that sum exactly."""
    wsum = sum(weights)
    if total_cents <= 0 or wsum <= 0 or not weights:
        return [0 for _ in weights]
    raw = [total_cents * w / wsum for w in weights]
    floors = [int(r) for r in raw]
    short = total_cents - sum(floors)
    order = sorted(range(len(raw)), key=lambda i: (floors[i] - raw[i], i))
    for i in order[:short]:
        floors[i] += 1
    return floors


_PERFORMANCE_NOTES_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "supply_performance_notes.json"
)


def _performance_notes(country: str) -> List[Dict[str, Any]]:
    """Supply-team notes naming ``country`` (data/supply_performance_notes.json),
    each with the names the board goes by. Empty when the file is missing or
    unreadable: a note only ever adds to a score, never makes a board eligible."""
    try:
        with open(_PERFORMANCE_NOTES_PATH, encoding="utf-8") as fh:
            entries = json.load(fh).get("entries") or []
    except (OSError, ValueError, AttributeError):
        logger.error("supply performance notes unreadable", exc_info=True)
        return []
    wanted = str(country or "").strip().casefold()
    out: List[Dict[str, Any]] = []
    for e in entries:
        if not isinstance(e, dict) or not e.get("name"):
            continue
        if wanted not in [str(c).strip().casefold() for c in e.get("countries") or []]:
            continue
        out.append(
            {
                "names": [e["name"]] + [str(a) for a in e.get("aliases") or []],
                "text": str(e.get("note") or "").strip(),
                "source": str(e.get("source") or "").replace(" feedback", "").strip(),
                "as_of": str(e.get("as_of") or "").strip(),
            }
        )
    return out


def _note_suffix(note: Dict[str, Any]) -> str:
    cite = ", ".join(x for x in (note["source"], note["as_of"]) if x)
    return f"; {note['text']}" + (f" ({cite})" if cite else "")


# ---------------------------------------------------------------------------
# Local-relevance score
# ---------------------------------------------------------------------------
#
# A board is not local because it lists few countries: a multi-country board
# can be the strongest option in one market. Every in-network candidate for a
# country is scored from the signals below and the top ones are funded in
# proportion to their score. The hard gate is not a signal: a board must be
# in network for the country (publisher_filter.board_supply_record, via
# in_network_job_boards -- never unavailable, onboarding, not in network or
# conflicted), not client-excluded, and not one the plan buys under another
# channel (Indeed, LinkedIn, Glassdoor).

SCORE_MARKET_SHARE_TOP = 40.0  # first on the country's market-share list
SCORE_MARKET_SHARE_FLOOR = 30.0  # later positions step down 2 points to here
SCORE_PERFORMANCE_NOTE = 35.0  # Joveo supply team reports it performing well
SCORE_PLAN_FUNDED = 20.0  # funded in a Joveo-built plan for the country
SCORE_PLAN_STANDBY = 10.0  # listed there as standby
SCORE_MARKET_NATIVE = 15.0  # country-code domain / name token / local list
SCORE_ROLE_FIT_MAX = 20.0
SCORE_PROGRAMMATIC = 10.0  # CPC or CPA pricing and an XML feed
SCORE_PRIORITY: Dict[str, float] = {"P0": 8.0, "P1": 5.0, "P2": 2.0, "P3": 0.0, "": 0.0}
POST_DIRECTLY_COUNT = 5
POST_DIRECTLY_LABEL = "Post directly (not bought programmatically)"

_PLAN_EVIDENCE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "plan_evidence_boards.json"
)
_GLOBAL_SUPPLY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "global_supply.json"
)

# Repository categories that serve one audience; such a board only scores role
# fit when the plan's roles are that audience, and with no fit it is never
# funded (standby at most).
_NICHE_CATEGORY_FAMILY: Dict[str, str] = {
    "University Job Board": "early_career",
    "Health": "healthcare",
    "Tech": "it",
    "Govt": "government",
    "DEI": "dei",
    "Veterinary": "veterinary",
}

# Occupation family -> words in a role title (or industry) that put the plan in
# it, and words in a repository ``job_categories`` label that serve it.
_FAMILY_ROLE_WORDS: Dict[str, Tuple[str, ...]] = {
    "production": (
        "maintenance",
        "technician",
        "production",
        "assembl",
        "manufactur",
        "operator",
        "machinist",
        "welder",
        "painter",
        "fabricat",
        "mechanic",
        "plant ",
        "industrial",
        "electrician",
        "quality inspector",
    ),
    "logistics": (
        "warehouse",
        "driver",
        "logistic",
        "delivery",
        "forklift",
        "shipping",
        "supply chain",
        "courier",
        "truck",
        "cdl",
        "fulfil",
        "picker",
        "packer",
        "dispatch",
    ),
    "healthcare": (
        "nurse",
        "nursing",
        "physician",
        "clinical",
        "medical",
        "caregiver",
        "therapist",
        "pharmac",
        "dental",
        "health",
        "patient",
        "paramedic",
        "radiolog",
        "surgeon",
        "doctor",
    ),
    "it": (
        "software",
        "developer",
        "devops",
        "cyber",
        "it support",
        "programmer",
        "cloud",
        "data engineer",
        "data scientist",
        "machine learning",
    ),
    "retail_hospitality": (
        "cashier",
        "retail",
        "store ",
        "sales associate",
        "server",
        "cook",
        "chef",
        "hotel",
        "restaurant",
        "barista",
        "hospitality",
        "waiter",
        "bartender",
        "housekeep",
        "customer service",
    ),
    "construction": (
        "construction",
        "carpent",
        "plumb",
        "electrician",
        "laborer",
        "labourer",
        "roofer",
        "mason",
        "hvac",
        "civil",
    ),
    "finance": (
        "accountant",
        "finance",
        "financial",
        "bank",
        "auditor",
        "insurance",
        "underwrit",
        "actuar",
    ),
    "education": ("teacher", "professor", "lecturer", "instructor", "tutor", "educat"),
    "government": (
        "government",
        "public sector",
        "federal",
        "civil servant",
        "military",
        "national guard",
        "soldier",
        "police",
        "firefight",
    ),
    "early_career": (
        "intern",
        "graduate",
        "trainee",
        "apprentice",
        "entry level",
        "entry-level",
        "student",
        "werkstudent",
    ),
    "veterinary": ("veterinar", "vet tech"),
    "dei": ("diversity", "inclusion", "veteran", "disabilit"),
}
_FAMILY_INDUSTRY_WORDS: Dict[str, Tuple[str, ...]] = {
    "production": (
        "manufactur",
        "automotive",
        "aerospace",
        "industrial",
        "chemical",
        "machinery",
        "mining",
        "oil and gas",
        "oil & gas",
        "energy",
        "utilit",
    ),
    "logistics": ("logistic", "transport", "supply chain", "shipping", "trucking"),
    "healthcare": ("health", "medical", "pharma", "nursing", "biotech", "life science"),
    "it": ("tech", "software", "telecom", "cyber", "saas", "information"),
    "retail_hospitality": (
        "retail",
        "hospitality",
        "restaurant",
        "food",
        "ecommerce",
        "e commerce",
        "consumer",
        "hotel",
        "travel",
    ),
    "construction": ("construct", "real estate", "trades"),
    "finance": ("financ", "bank", "insurance", "accounting"),
    "education": ("educat", "school", "universit"),
    "government": ("government", "public sector", "defen", "military"),
}
_FAMILY_BOARD_PATTERNS: Dict[str, "re.Pattern[str]"] = {
    fam: re.compile(pat)
    for fam, pat in {
        "production": r"manufactur|production|mechanical|industrial|automotive|skilled trades|electrical|chemical",
        "logistics": r"logistic|warehous|driving|transport|driver|delivery",
        "healthcare": r"health|nurs|medic|dental|pharma|clinical|allied",
        "it": r"\bit\b|software|technolog|cyber|telecom",
        "retail_hospitality": r"retail|hospitality|catering|tourism|customer service|e-commerce|fmcg",
        "construction": r"construction|architecture|skilled trades|trades",
        "finance": r"financ|accounting|banking|insurance",
        "education": r"education|teaching|childcare",
        "government": r"government|public|military",
        "early_career": r"apprentice|trainee|intern|graduate",
        "veterinary": r"veterin",
        "dei": r"divers",
    }.items()
}
_FAMILY_LABEL: Dict[str, str] = {
    "production": "production",
    "logistics": "logistics",
    "healthcare": "healthcare",
    "it": "IT",
    "retail_hospitality": "retail & hospitality",
    "construction": "construction",
    "finance": "finance",
    "education": "education",
    "government": "public-sector",
    "early_career": "early-career",
    "veterinary": "veterinary",
    "dei": "DEI",
}
# A board that lists only this many job categories, none of them the plan's,
# serves other occupations: it is treated like a niche board with no fit.
SPECIALIST_MAX_LABELS = 5
_BLUE_COLLAR_FAMILIES = frozenset(
    {"production", "logistics", "retail_hospitality", "construction"}
)

# Country -> (country-code domains, name tokens) that mark a board as native to
# it; a plan's own city names are added at scoring time.
_COUNTRY_NATIVE: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]] = {
    "Germany": (("de",), ("germany", "german", "deutschland", "deutsch")),
    "Austria": (("at",), ("austria", "osterreich", "oesterreich")),
    "Switzerland": (("ch",), ("switzerland", "swiss", "schweiz")),
    "United Kingdom": (("uk",), ("united kingdom", "britain", "british")),
    "France": (("fr",), ("france", "french", "francais")),
    "Italy": (("it",), ("italy", "italia", "italian")),
    "Spain": (("es",), ("spain", "espana", "spanish")),
    "Netherlands": (("nl",), ("netherlands", "dutch", "nederland")),
    "Belgium": (("be",), ("belgium", "belgie", "belgique")),
    "Poland": (("pl",), ("poland", "polska", "polish")),
    "Sweden": (("se",), ("sweden", "sverige", "swedish")),
    "Norway": (("no",), ("norway", "norge", "norwegian")),
    "Finland": (("fi",), ("finland", "suomi", "finnish")),
    "Denmark": (("dk",), ("denmark", "danmark", "danish")),
    "Ireland": (("ie",), ("ireland", "irish")),
    "Portugal": (("pt",), ("portugal", "portuguese")),
    "United States": (("us",), ("united states", "american", " usa")),
    "Canada": (("ca",), ("canada", "canadian")),
    "Mexico": (("mx",), ("mexico", "mexican")),
    "Brazil": (("br",), ("brazil", "brasil", "brazilian")),
    "Colombia": (("co",), ("colombia", "colombian")),
    "Argentina": (("ar",), ("argentina",)),
    "Chile": (("cl",), ("chile", "chilean")),
    "India": (("in",), ("india", "indian")),
    "Australia": (("au",), ("australia", "australian")),
    "New Zealand": (("nz",), ("new zealand", "kiwi")),
    "Japan": (("jp",), ("japan", "japanese")),
    "Singapore": (("sg",), ("singapore",)),
    "Philippines": (("ph",), ("philippines", "pinoy", "filipino")),
    "Malaysia": (("my",), ("malaysia", "malaysian")),
    "South Africa": (("za",), ("south africa", "south african")),
    "Egypt": (("eg",), ("egypt", "egyptian")),
    "Oman": (("om",), ("oman", "omani")),
    "United Arab Emirates": (("ae",), ("emirates", "uae", "dubai")),
    "Saudi Arabia": (("sa",), ("saudi", "ksa")),
    "Turkey": (("tr",), ("turkey", "turkiye", "turkish")),
}


def _fold_text(text: Any) -> str:
    return re.sub(r"[^a-z0-9&.\- ]+", " ", str(text or "").casefold())


def _plan_roles(data: Any) -> List[str]:
    if not isinstance(data, dict):
        return []
    raw = data.get("target_roles") or data.get("roles") or []
    if not isinstance(raw, (list, tuple)):
        raw = [raw]
    out: List[str] = []
    for r in raw:
        if isinstance(r, dict):
            r = r.get("title") or r.get("role") or r.get("name") or ""
        if str(r or "").strip():
            out.append(str(r))
    return out


def plan_role_families(data: Any) -> Tuple[str, ...]:
    """The occupation families a plan's roles and industry belong to
    (e.g. ``("production",)`` for maintenance technicians in automotive)."""
    if not isinstance(data, dict):
        return ()
    role_text = " " + " ; ".join(_fold_text(r) for r in _plan_roles(data)) + " "
    industry = _fold_text(str(data.get("industry") or "").replace("_", " "))
    found: List[str] = []
    for fam, words in _FAMILY_ROLE_WORDS.items():
        if any(w in role_text for w in words):
            found.append(fam)
    for fam, words in _FAMILY_INDUSTRY_WORDS.items():
        if fam not in found and any(w in industry for w in words):
            found.append(fam)
    return tuple(found)


def _is_niche(category: str) -> bool:
    return category in _NICHE_CATEGORY_FAMILY


def _role_fit(
    board: Dict[str, Any], families: Tuple[str, ...]
) -> Tuple[float, List[str], bool]:
    """``(0..SCORE_ROLE_FIT_MAX, matched family names, specialist_miss)`` for
    one board row; ``specialist_miss`` is True for a narrow board whose few
    job categories are none of the plan's."""
    category = board.get("category") or ""
    niche_family = _NICHE_CATEGORY_FAMILY.get(category)
    job_type = board.get("job_type") or ""
    blue_plan = any(f in _BLUE_COLLAR_FAMILIES for f in families)
    white_plan = bool(families) and not blue_plan
    type_bonus = 0.0
    if (job_type == "Blue Collar" and blue_plan) or (
        job_type == "White Collar" and white_plan
    ):
        type_bonus = 4.0
    elif job_type == "General":
        type_bonus = 2.0
    if niche_family is not None:
        if niche_family not in families:
            return 0.0, [], False
        return min(SCORE_ROLE_FIT_MAX, 16.0 + type_bonus), [niche_family], False
    if not families:
        return 10.0, [], False  # no roles known: a generalist board is neutral
    labels = [str(c).casefold() for c in board.get("job_categories") or ()]
    if not labels:
        return min(SCORE_ROLE_FIT_MAX, 8.0 + type_bonus), [], False  # not recorded
    matched = [
        f
        for f in families
        if any(_FAMILY_BOARD_PATTERNS[f].search(lab) for lab in labels)
    ]
    if not matched and len(labels) <= SPECIALIST_MAX_LABELS:
        return 0.0, [], True
    base = round(16.0 * len(matched) / len(families))
    return min(SCORE_ROLE_FIT_MAX, base + type_bonus), matched, False


def _is_manual(pricing: str, xml: str) -> bool:
    """A board bought by posting, subscription or slot, or one that takes no
    XML feed, cannot be given programmatic budget. A posting-priced board that
    does take a feed (xml == 'Yes') can be fed, so it is not manual."""
    if (xml or "").strip().casefold() == "no":
        return True
    tokens = {t.strip().casefold() for t in re.split(r"[+/,]", pricing or "")}
    return bool(tokens & {"posting", "subscription", "slot"}) and (
        (xml or "").strip().casefold() != "yes"
    )


def _is_programmatic(pricing: str, xml: str) -> bool:
    upper = (pricing or "").upper()
    return ("CPC" in upper or "CPA" in upper) and (
        xml or ""
    ).strip().casefold() == "yes"


def _load_json(path: str) -> Any:
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        logger.error("%s unreadable", path, exc_info=True)
        return {}


def _plan_evidence(country: str) -> Dict[str, List[str]]:
    """Boards named in Joveo-built plans for ``country``
    (data/plan_evidence_boards.json): ``{"funded": [...], "standby": [...]}``."""
    doc = _load_json(_PLAN_EVIDENCE_PATH)
    want = publisher_filter.canonical_country(country)
    for name, entry in ((doc or {}).get("countries") or {}).items():
        if publisher_filter.canonical_country(name) == want and isinstance(entry, dict):
            return {
                "funded": [str(n) for n in entry.get("funded") or []],
                "standby": [str(n) for n in entry.get("standby") or []],
            }
    return {"funded": [], "standby": []}


def _local_supply_names(country: str) -> List[str]:
    """Boards data/global_supply.json lists for ``country``."""
    doc = _load_json(_GLOBAL_SUPPLY_PATH)
    want = publisher_filter.canonical_country(country)
    for name, entry in ((doc or {}).get("country_job_boards") or {}).items():
        if publisher_filter.canonical_country(name) == want:
            return [
                str(b["name"])
                for b in (entry or {}).get("boards") or []
                if isinstance(b, dict) and b.get("name")
            ]
    return []


def _native_markers(data: Any, country: str) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    domains, tokens = _COUNTRY_NATIVE.get(
        publisher_filter.canonical_country(country), ((), ())
    )
    tokens = tuple(tokens) + (_fold_text(country).strip(),)
    cities: List[str] = []
    raw = (data or {}).get("locations") if isinstance(data, dict) else None
    for loc in raw if isinstance(raw, (list, tuple)) else [raw] if raw else []:
        text = loc if isinstance(loc, str) else str((loc or {}).get("city") or "")
        city = _fold_text(text.split(",")[0]).strip()
        if len(city) >= 4 and city != _fold_text(country).strip():
            cities.append(city)
    return tuple(domains), tokens + tuple(cities)


def _is_native(name: str, domains: Tuple[str, ...], tokens: Tuple[str, ...]) -> bool:
    text = " " + _fold_text(name) + " "
    if any(re.search(rf"\.{re.escape(d)}(?![a-z0-9])", text) for d in domains):
        return True
    return any(
        t and re.search(rf"(?<![a-z0-9]){re.escape(t.strip())}(?![a-z0-9])", text)
        for t in tokens
    )


# Public employment services / national job portals: the only government
# general boards exempt from the role-fit requirement. A government site that
# is not one (an EU researcher-mobility portal) is an ordinary niche board.
_EMPLOYMENT_SERVICE = re.compile(
    r"arbeitsagentur|bundesagentur|employment|jobcentre|\bdwp\b|find a job|"
    r"p[o\u00f4]le emploi|france travail|arbetsf[o\u00f6]rmedlingen|arbertformedligen|"
    r"platsbanken|usajobs|careeronestop|\bnav\b|arbeidsplassen|\bvdab\b|\bforem\b|"
    r"\bactiris\b|\bams\b|\buwv\b|werk\.nl|\bsepe\b|\banpal\b|job[- ]?room|"
    r"jobnet|job bank|jobsireland|job market finland|jobs\.gov\.hk|jobapplyni",
    re.IGNORECASE,
)


def _gov_why(board: Dict[str, Any], signals: Dict[str, Any]) -> str:
    """Why a government general board is listed: what it is and how it is bought."""
    names = f"{board['name']} {signals.get('ms_name') or ''}"
    kind = (
        "public employment service"
        if _EMPLOYMENT_SERVICE.search(names)
        else "government job board"
    )
    free = " \u00b7 free organic posting" if board["pricing"] == "Organic" else ""
    return kind + free


def _market_share_bonus(position: int) -> float:
    return max(SCORE_MARKET_SHARE_FLOOR, SCORE_MARKET_SHARE_TOP - 2.0 * position)


def _display_name(row_name: str, signals: Dict[str, Any]) -> str:
    """The name printed for a board: the market-share / note / plan label, but
    never a compound one ("VDAB / Forem / Actiris") -- that names several
    boards, only one of which is this row -- so a compound label shows the
    repository row's own name."""
    label = (
        signals.get("ms_name") or signals.get("note_name") or signals.get("plan_name")
    )
    if not label or "/" in label or "&" in label:
        return row_name
    return label


def _score_candidates(data: Any, country: str) -> List[Dict[str, Any]]:
    """Every in-network job board for ``country`` with its local-relevance
    score and the signals that produced it, best first (score, then fewer
    listed countries, then name). Client exclusions, boards bought under
    another channel and duplicate spellings are applied by :func:`_walk`."""
    boards = publisher_filter.in_network_job_boards(country)
    index: Dict[str, List[int]] = {}
    for i, b in enumerate(boards):
        for key in publisher_filter._keys_for(b["name"]):
            index.setdefault(key, []).append(i)

    def _hits(names: List[str]) -> List[Tuple[int, int, str]]:
        found: List[Tuple[int, int, str]] = []
        for pos, name in enumerate(names):
            for key in set(publisher_filter._keys_for(name)):
                found.extend((i, pos, name) for i in index.get(key, ()))
        return found

    try:
        market_share = publisher_filter.dataset_platform_names(country)
    except (AttributeError, TypeError, ValueError):
        logger.error("market-share boards unavailable for %s", country, exc_info=True)
        market_share = []
    sig: List[Dict[str, Any]] = [{} for _ in boards]
    for i, pos, name in _hits(market_share):
        if "ms_pos" not in sig[i] or pos < sig[i]["ms_pos"]:
            sig[i]["ms_pos"], sig[i]["ms_name"] = pos, name
    for note in _performance_notes(country):
        for i, _, name in _hits(note["names"]):
            sig[i].setdefault("note", note)
            sig[i].setdefault("note_name", name)
    evidence = _plan_evidence(country)
    for i, _, name in _hits(evidence["standby"]):
        sig[i]["plan"] = sig[i].get("plan") or "standby"
        sig[i].setdefault("plan_name", name)
    for i, _, name in _hits(evidence["funded"]):
        sig[i]["plan"] = "funded"
        sig[i]["plan_name"] = name
    local_list = {i for i, _, _ in _hits(_local_supply_names(country))}

    families = plan_role_families(data)
    domains, tokens = _native_markers(data, country)
    out: List[Dict[str, Any]] = []
    for i, b in enumerate(boards):
        s = sig[i]
        score = 0.0
        why: List[str] = []
        if "ms_pos" in s:
            score += _market_share_bonus(s["ms_pos"])
            why.append("market leader")
        if "note" in s:
            score += SCORE_PERFORMANCE_NOTE
            why.append("performs well (Joveo supply team)")
        if s.get("plan") == "funded":
            score += SCORE_PLAN_FUNDED
            why.append("in Joveo-built plans")
        elif s.get("plan") == "standby":
            score += SCORE_PLAN_STANDBY
            why.append("standby in Joveo-built plans")
        if i in local_list or _is_native(b["name"], domains, tokens):
            score += SCORE_MARKET_NATIVE
            why.append(f"local to {country}")
        fit, matched, specialist = _role_fit(b, families)
        score += fit
        if matched:
            why.append("fits " + "/".join(_FAMILY_LABEL[f] for f in matched) + " roles")
        manual = _is_manual(b["pricing"], b["xml"])
        if _is_programmatic(b["pricing"], b["xml"]):
            score += SCORE_PROGRAMMATIC
            why.append("programmatic")
        score += SCORE_PRIORITY[b["priority"]]
        niche = _is_niche(b["category"]) or specialist
        # a government general board is free organic posting and among the
        # largest boards in its country: exempt from the role-fit requirement
        # for the post-directly list (never for programmatic funding)
        gov_general = (
            b["category"] == "Govt"
            and b["job_type"] in ("General", "")
            and bool(_EMPLOYMENT_SERVICE.search(f"{b['name']} {s.get('ms_name') or ''}"))
        )
        if gov_general:
            why = [_gov_why(b, s)]
        has_signal = any(k in s for k in ("ms_pos", "note", "plan"))
        status = publisher_filter.status_label(b["record"])
        if "note" in s:
            status += _note_suffix(s["note"])
        out.append(
            {
                "name": _display_name(b["name"], s),
                "priority": b["priority"],
                "score": score,
                "why": " · ".join(why) or "in-network board",
                "status": status,
                "manual": manual,
                "niche": niche,
                "gov_general": gov_general,
                "role_fit": fit,
                # a niche board with no fit to the plan's roles is never funded
                "fundable": not (niche and fit <= 0),
                "relevant": has_signal,
                "n_countries": b["n_countries"],
                "row_name": b["name"],
            }
        )
    out.sort(key=lambda c: (-c["score"], c["n_countries"], c["name"].casefold()))
    return out


def _walk(
    cands: List[Dict[str, Any]],
    data: Any,
    country: str,
    wanted: Callable[[Dict[str, Any]], bool],
    limit: int,
) -> List[Dict[str, Any]]:
    """The first ``limit`` candidates passing ``wanted`` that are not bought
    under another channel, client-excluded or a second spelling of a board
    already taken."""
    chosen: List[Dict[str, Any]] = []
    taken: set = set()
    for c in cands:
        if len(chosen) >= limit:
            break
        if not wanted(c):
            continue
        keys = set(publisher_filter._keys_for(c["row_name"])) | set(
            publisher_filter._keys_for(c["name"])
        )
        if keys & taken:
            continue
        if any(
            publisher_filter.names_match(c["row_name"], o)
            or publisher_filter.names_match(c["name"], o)
            for o in _OTHER_CHANNEL_BOARDS
        ):
            continue
        if publisher_filter.is_excluded(
            c["row_name"], data, country
        ) or publisher_filter.is_excluded(c["name"], data, country):
            continue
        taken |= keys
        chosen.append(c)
    return chosen


def ranked_boards(data: Any, country: str, limit: int = 60) -> List[Dict[str, Any]]:
    """In-network, not-excluded, programmatically buyable boards for ``country``
    by local-relevance score, best first (up to ``limit``), each ``{"name",
    "priority", "score", "why", "status", "fundable", ...}``.

    Score = market-share list (+40 first, stepping down to +30) + supply-team
    performance note (+35) + funded (+20) or standby (+10) in a Joveo-built
    plan for the country + market-native (+15: country-code domain, country or
    plan-city name token, or data/global_supply.json's list) + role fit (0..20)
    + programmatic CPC/CPA with an XML feed (+10) + priority (P0 8, P1 5, P2 2).
    Ties: fewer listed countries, then name. ``fundable`` is False for a niche
    board (university, health, tech, DEI, government, veterinary) whose
    audience does not match the plan's roles; it is kept only when a relevance
    signal puts it in play, as standby. Boards bought by posting, subscription
    or slot are not here: see :func:`post_directly_boards`."""
    cands = _score_candidates(data, country)
    return _walk(
        cands,
        data,
        country,
        lambda c: not c["manual"] and (c["fundable"] or c["relevant"]),
        limit,
    )


def post_directly_boards(
    data: Any, country: str, limit: int = POST_DIRECTLY_COUNT
) -> List[Dict[str, Any]]:
    """The best in-network boards for ``country`` that are bought by posting,
    subscription or slot (or take no XML feed): worth using, but never given
    programmatic budget. Same score and gates as :func:`ranked_boards`."""
    cands = _score_candidates(data, country)
    # government general boards lead the list (stable: score order within each
    # group), and need no role fit
    cands.sort(key=lambda c: not c["gov_general"])
    return _walk(
        cands,
        data,
        country,
        lambda c: c["manual"] and (c["fundable"] or c["gov_general"]),
        limit,
    )


def funding_order(data: Any, country: str) -> List[Dict[str, Any]]:
    """The boards eligible for budget in funding order -- what the plan funds
    (first :data:`FUNDED_BOARD_COUNT`) and what Nova lists as best."""
    return [b for b in ranked_boards(data, country) if b["fundable"]]


def rank_names(names: List[str], country: str, data: Any = None) -> List[str]:
    """``names`` reordered by their place in :func:`funding_order` for
    ``country`` (best first); names the score does not place keep their
    original order after the ranked ones. Nova orders its supply listings with
    this, so chat and plan agree on which boards lead."""
    position: Dict[str, int] = {}
    for i, b in enumerate(funding_order(data, country)):
        for key in publisher_filter._keys_for(
            b["row_name"]
        ) + publisher_filter._keys_for(b["name"]):
            position.setdefault(key, i)

    def _place(item: Tuple[int, str]) -> Tuple[int, int]:
        keys = publisher_filter._keys_for(item[1])
        ranks = [position[k] for k in keys if k in position]
        return (min(ranks) if ranks else len(position) + 1, item[0])

    return [n for _, n in sorted(enumerate(names), key=_place)]


def _country_weights(data: Any, countries: List[str]) -> List[float]:
    """Each plan country's share of the plan's locations (one per location)."""
    counts = {c: 0 for c in countries}
    raw = (data or {}).get("locations") or []
    if not isinstance(raw, (list, tuple)):
        raw = [raw]
    for loc in raw:
        for c in publisher_filter.plan_countries({"locations": [loc]}):
            if c in counts:
                counts[c] += 1
    weights = [float(counts[c]) for c in countries]
    return weights if sum(weights) > 0 else [1.0 for _ in countries]


def regional_budget_split(data: Any, budget_alloc: Any) -> Optional[Dict[str, Any]]:
    """Split the plan's regional & aggregator channel budget over named boards.

    ``{"budget", "channels", "countries": [{"country", "amount", "boards":
    [{"name", "priority", "share", "amount", "status"}], "standby": [...],
    "note"}]}`` or None when the plan funds no such channel or names no
    country. ``share`` is a fraction of the country's amount; board amounts sum
    exactly to it (largest remainder, whole cents). With fewer than two
    in-network boards for a country, ``boards`` is empty and ``note`` says the
    budget is held as aggregator spend."""
    if not isinstance(budget_alloc, dict):
        return None
    allocs = budget_alloc.get("channel_allocations")
    if not isinstance(allocs, dict):
        return None
    regional = [
        (k, c)
        for k, c in allocs.items()
        if is_regional_channel(k, c) and _alloc_dollars(c) > 0
    ]
    countries = publisher_filter.plan_countries(data)
    if not regional or not countries:
        return None
    total_cents = sum(round(_alloc_dollars(c) * 100) for _, c in regional)

    # Per-country amounts: the channel's own per-country figures when it has
    # them, else the plan's country weights.
    per_country = [0.0 for _ in countries]
    weighted_cents = 0
    for _, ch in regional:
        own = ch.get("country_amounts") or ch.get("by_country")
        cents = round(_alloc_dollars(ch) * 100)
        if isinstance(own, dict) and own:
            known = {
                publisher_filter.canonical_country(k): _num(v) for k, v in own.items()
            }
            if all(c in known for c in countries) and sum(known.values()) > 0:
                for i, c in enumerate(countries):
                    per_country[i] += known[c] * 100
                continue
        weighted_cents += cents
    if weighted_cents:
        parts = _largest_remainder_cents(
            weighted_cents, _country_weights(data, countries)
        )
        per_country = [p + q for p, q in zip(per_country, parts)]
    country_cents = _largest_remainder_cents(
        total_cents, [max(p, 0.0) for p in per_country]
    )

    result_countries: List[Dict[str, Any]] = []
    for country, cents in zip(countries, country_cents):
        ranked = ranked_boards(data, country)
        fundable = [b for b in ranked if b["fundable"]]
        entry: Dict[str, Any] = {
            "country": country,
            "amount": cents / 100.0,
            "boards": [],
            "standby": [],
            "post_directly": [],
            "note": "",
        }
        if len(fundable) < 2:
            entry["note"] = (
                f"No in-network local boards on record for {country}; budget "
                "held as aggregator spend"
            )
            result_countries.append(entry)
            continue
        funded = fundable[:FUNDED_BOARD_COUNT]
        weights = [b["score"] for b in funded]
        amounts = _largest_remainder_cents(cents, weights)
        wsum = sum(weights)
        entry["boards"] = [
            {
                "name": b["name"],
                "priority": b["priority"],
                "score": b["score"],
                "share": b["score"] / wsum,
                "amount": a / 100.0,
                "status": b["status"],
                "why": b["why"],
            }
            for b, a in zip(funded, amounts)
        ]
        funded_ids = {id(b) for b in funded}
        entry["standby"] = [
            {
                "name": b["name"],
                "priority": b["priority"],
                "score": b["score"],
                "status": b["status"],
                "why": b["why"],
            }
            for b in [x for x in ranked if id(x) not in funded_ids][
                :STANDBY_BOARD_COUNT
            ]
        ]
        entry["post_directly"] = [
            {
                "name": b["name"],
                "priority": b["priority"],
                "score": b["score"],
                "status": b["status"],
                "why": (
                    b["why"] if b["gov_general"] else b["why"] + " \u00b7 post directly"
                ),
            }
            for b in post_directly_boards(data, country)
        ]
        result_countries.append(entry)
    return {
        "budget": total_cents / 100.0,
        "channels": [channel_display_name(k, c) for k, c in regional],
        "countries": result_countries,
    }


def funded_board_names(split: Optional[Dict[str, Any]]) -> List[str]:
    """Names of every funded board in a split, in country then funding order."""
    out: List[str] = []
    for entry in (split or {}).get("countries") or []:
        for b in entry.get("boards") or []:
            if b["name"] not in out:
                out.append(b["name"])
    return out
