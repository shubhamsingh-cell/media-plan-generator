#!/usr/bin/env python3
"""Hand a plan-shaped Nova answer to the full media-plan wizard.

Nova answers "plan a campaign" questions with a short overview. The wizard
at ``/media-plan`` builds the real deliverable and already reads a brief from
its query string (roles, locations, budget, hires, timeline, industry,
client -- the same parameters Quick Brief emits). This module turns whatever
the conversation already said into that URL so the user does not retype it.

Only parameters that actually parsed are emitted; nothing is guessed.
Lists are joined with "; " (the wizard splits sites on ";" but reads
"Jacksonville, Washington" as a single City, State site) and every value is
URL-encoded.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

from nova_memory import extract_industries, extract_locations, extract_roles

logger = logging.getLogger(__name__)

HANDOFF_LABEL = "Build the full media plan"
_WIZARD_PATH = "/media-plan"

# nova_memory.extract_industries() words -> wizard industry-card keys
# (templates/partials/index: ``.industry-card[data-industry]``). Words with no
# card are left out rather than approximated; "media", "marketing" and
# "advertising" are left out on purpose because they appear in "media plan" /
# "marketing budget" with no bearing on the client's industry.
_INDUSTRY_TO_WIZARD_KEY: Dict[str, str] = {
    "tech": "tech_engineering",
    "technology": "tech_engineering",
    "healthcare": "healthcare_medical",
    "finance": "finance_banking",
    "banking": "finance_banking",
    "retail": "retail_consumer",
    "ecommerce": "retail_consumer",
    "e-commerce": "retail_consumer",
    "education": "education",
    "hospitality": "hospitality_travel",
    "restaurant": "food_beverage",
    "food service": "food_beverage",
    "logistics": "logistics_supply_chain",
    "transportation": "logistics_supply_chain",
    "supply chain": "logistics_supply_chain",
    "pharma": "pharma_biotech",
    "pharmaceutical": "pharma_biotech",
    "biotech": "pharma_biotech",
    "construction": "construction_real_estate",
    "real estate": "construction_real_estate",
    "energy": "energy_utilities",
    "oil and gas": "energy_utilities",
    "renewable": "energy_utilities",
    "insurance": "insurance",
    "legal": "legal_services",
    "telecommunications": "telecommunications",
    "telecom": "telecommunications",
    "automotive": "automotive",
    "aerospace": "aerospace_defense",
    "defense": "aerospace_defense",
    "military": "military_recruitment",
}
_WIZARD_INDUSTRY_KEYS = frozenset(_INDUSTRY_TO_WIZARD_KEY.values()) | {
    "blue_collar_trades",
    "general_entry_level",
    "maritime_marine",
    "mental_health",
    "rideshare",
}

_US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO "
    "MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY "
    "DC".split()
)
_CITY_STATE_RE = re.compile(
    r"\b([A-Z][a-z]+(?:\s[A-Z][a-z]+){0,2}),\s*([A-Z]{2})\b"
)

_ACRONYMS = {
    "hr": "HR",
    "ux": "UX",
    "ui": "UI",
    "sre": "SRE",
    "ai": "AI",
    "cdl": "CDL",
    "devops": "DevOps",
}

# A markdown table whose header names a Channel column next to a money /
# share column is a channel-allocation table.
_CHANNEL_TABLE_RE = re.compile(
    r"^\s*\|[^\n]*\bchannel\b[^\n]*\|[^\n]*(?:budget|allocation|spend|%)[^\n]*\|",
    re.IGNORECASE | re.MULTILINE,
)

_BUDGET_DOLLAR_RE = re.compile(
    r"\$\s?(\d[\d,]*(?:\.\d+)?)\s*(million|mm|m|thousand|k)?\b", re.IGNORECASE
)
_BUDGET_WORD_RE = re.compile(
    r"\b(?:budget(?:\s+of|\s+is|\s*[:=])?\s*)(\d[\d,]*(?:\.\d+)?)\s*(million|mm|m|thousand|k)?\b",
    re.IGNORECASE,
)
_BUDGET_TRAIL_RE = re.compile(
    r"\b(\d[\d,]*(?:\.\d+)?)\s*(million|mm|m|thousand|k)\s+budget\b", re.IGNORECASE
)
_TIMELINE_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*[- ]?\s*(weeks?|wks?|months?|mos?|years?|yrs?)\b",
    re.IGNORECASE,
)
_HIRE_WORDS = (
    r"hires?|openings?|positions?|vacancies|employees?|people|candidates?|"
    r"associates?|workers?|staff|headcount"
)
_NUMBER_NOT_MONEY = r"(?<![\$\d,.])(\d[\d,]*)(?![\d,.]*\s*(?:[kKmM%]\b|million|thousand))"
_HIRE_VERB_RE = re.compile(
    r"\b(?:hire|hiring|fill|recruit(?:ing)?|need(?:s)?|staff(?:ing)?)\s+(?:about\s+|around\s+|up to\s+)?"
    + _NUMBER_NOT_MONEY
    + r"\b",
    re.IGNORECASE,
)
_HIRE_NOUN_RE = re.compile(
    _NUMBER_NOT_MONEY + r"\s+(?:[a-z-]+\s+){0,2}?(?:" + _HIRE_WORDS + r")\b",
    re.IGNORECASE,
)
_CLIENT_RE = re.compile(
    r"\bclient\s*(?:name)?\s*(?:is|:|=|called|named)\s+([A-Z][\w&.'-]*(?:\s+[A-Z][\w&.'-]*){0,3})"
)


def _strip_plurals(text: str) -> str:
    """'software engineers' -> 'software engineer' so title matching works."""
    return re.sub(r"\b([A-Za-z]{3,}?)s\b", r"\1", text)


def _scale(raw: str, unit: str) -> Optional[int]:
    try:
        value = float(raw.replace(",", ""))
    except ValueError:
        return None
    unit = (unit or "").lower()
    if unit in ("k", "thousand"):
        value *= 1_000
    elif unit in ("m", "mm", "million"):
        value *= 1_000_000
    amount = int(round(value))
    return amount if amount > 0 else None


def parse_budget(text: str) -> Optional[int]:
    """First budget in ``text`` as whole dollars, or None."""
    for rx in (_BUDGET_DOLLAR_RE, _BUDGET_TRAIL_RE, _BUDGET_WORD_RE):
        m = rx.search(text or "")
        if m:
            amount = _scale(m.group(1), m.group(2) or "")
            if amount:
                return amount
    return None


def parse_timeline(text: str) -> Optional[str]:
    """First duration as '<n> weeks|months|years' (wizard-readable), or None."""
    m = _TIMELINE_RE.search(text or "")
    if not m:
        return None
    n = m.group(1)
    unit = m.group(2).lower()
    word = "weeks" if unit.startswith("w") else "years" if unit.startswith("y") else "months"
    if n.endswith(".0"):
        n = n[:-2]
    return f"{n} {word}"


def _display_role(role: str) -> str:
    return " ".join(_ACRONYMS.get(w, w.capitalize()) for w in role.split())


def parse_roles(text: str) -> List[str]:
    """Known role titles in ``text`` (plural-tolerant), generic words dropped
    when a more specific title containing them was also found."""
    found: List[str] = []
    for variant in (text or "", _strip_plurals(text or "")):
        for role in extract_roles(variant):
            if role not in found:
                found.append(role)
    kept = [
        r
        for r in found
        if not any(r != other and re.search(rf"\b{re.escape(r)}\b", other) for other in found)
    ]
    return [_display_role(r) for r in kept]


def parse_locations(text: str) -> List[str]:
    """Locations in ``text``; 'City, ST' keeps its uppercase state and a bare
    name already covered by a longer entry is dropped."""
    cleaned: List[str] = []
    pairs = [
        f"{city}, {st}"
        for city, st in _CITY_STATE_RE.findall(text or "")
        if st in _US_STATES
    ]
    for loc in pairs + extract_locations(text or ""):
        loc = re.sub(r",\s*([A-Za-z]{2})\b", lambda m: ", " + m.group(1).upper(), loc)
        if loc not in cleaned:
            cleaned.append(loc)
    return [
        loc
        for loc in cleaned
        if not any(
            loc != other and re.search(rf"\b{re.escape(loc)}\b", other, re.IGNORECASE)
            for other in cleaned
        )
    ]


def parse_hires(text: str, roles: Iterable[str]) -> Optional[int]:
    """Head-count: '<n> hires/openings/...', 'hire <n>', or '<n> <role>'."""
    text = text or ""
    for rx in (_HIRE_VERB_RE, _HIRE_NOUN_RE):
        m = rx.search(text)
        if m:
            return _hires_value(m.group(1))
    singular = _strip_plurals(text)
    for role in roles:
        m = re.search(
            _NUMBER_NOT_MONEY + r"\s+(?:[a-z-]+\s+){0,1}?" + re.escape(role.lower()) + r"\b",
            singular.lower(),
        )
        if m:
            return _hires_value(m.group(1))
    return None


def _hires_value(raw: str) -> Optional[int]:
    try:
        n = int(raw.replace(",", ""))
    except ValueError:
        return None
    return n if 0 < n < 1_000_000 else None


def parse_industry(text: str) -> Optional[str]:
    """Wizard industry-card key for the first industry word that has a card."""
    for word in extract_industries(text or ""):
        key = _INDUSTRY_TO_WIZARD_KEY.get(word)
        if key:
            return key
    return None


def is_plan_shaped(query_type: str, response_text: str) -> bool:
    """A media-plan question, or an answer carrying a channel-allocation table."""
    if query_type == "media_plan":
        return True
    return bool(_CHANNEL_TABLE_RE.search(response_text or ""))


def _tool_json(tool_results: Dict[str, Any], name: str) -> Dict[str, Any]:
    raw = (tool_results or {}).get(name)
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _positive_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value) if value > 0 else None


def extract_brief(
    message: str,
    history: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    tool_results: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Parse the brief. The current message wins, then earlier user turns
    (newest first), then request context, then the budget tool's own inputs.
    Fields that never parsed are absent."""
    texts = [message or ""]
    for turn in reversed(list(history or [])[-8:]):
        if isinstance(turn, dict) and turn.get("role") == "user":
            content = turn.get("content") or turn.get("text") or ""
            if isinstance(content, str) and content:
                texts.append(content)
    ctx = context if isinstance(context, dict) else {}
    budget_tool = _tool_json(tool_results or {}, "query_budget_projection")

    brief: Dict[str, Any] = {}

    for text in texts:
        roles = parse_roles(text)
        if roles:
            brief["roles"] = roles
            break
    if "roles" not in brief and isinstance(ctx.get("roles"), list):
        ctx_roles = [str(r).strip() for r in ctx["roles"] if str(r).strip()]
        if ctx_roles:
            brief["roles"] = ctx_roles[:10]

    for text in texts:
        locations = parse_locations(text)
        if locations:
            brief["locations"] = locations
            break
    if "locations" not in brief and isinstance(ctx.get("locations"), list):
        ctx_locs = [str(x).strip() for x in ctx["locations"] if str(x).strip()]
        if ctx_locs:
            brief["locations"] = ctx_locs[:10]

    for text in texts:
        budget = parse_budget(text)
        if budget:
            brief["budget"] = budget
            break
    if "budget" not in brief:
        budget = _positive_int(ctx.get("campaign_budget")) or _positive_int(
            budget_tool.get("total_budget")
        )
        if budget:
            brief["budget"] = budget

    for text in texts:
        hires = parse_hires(text, [r.lower() for r in brief.get("roles") or []])
        if hires:
            brief["hires"] = hires
            break
    if "hires" not in brief:
        hires = _positive_int(budget_tool.get("target_hires"))
        if hires:
            brief["hires"] = hires

    for text in texts:
        timeline = parse_timeline(text)
        if timeline:
            brief["timeline"] = timeline
            break

    for text in texts:
        industry = parse_industry(text)
        if industry:
            brief["industry"] = industry
            break
    if "industry" not in brief:
        for candidate in (ctx.get("campaign_industry"), ctx.get("industry"), budget_tool.get("industry")):
            if isinstance(candidate, str) and candidate.strip() in _WIZARD_INDUSTRY_KEYS:
                brief["industry"] = candidate.strip()
                break
            if isinstance(candidate, str):
                key = _INDUSTRY_TO_WIZARD_KEY.get(candidate.strip().lower())
                if key:
                    brief["industry"] = key
                    break

    client = ""
    for candidate in (ctx.get("campaign_client"), ctx.get("client")):
        if isinstance(candidate, str) and candidate.strip():
            client = candidate.strip()
            break
    if not client:
        for text in texts:
            m = _CLIENT_RE.search(text)
            if m:
                client = m.group(1).strip()
                break
    if client:
        brief["client"] = client[:80]

    return brief


def build_handoff_url(brief: Dict[str, Any]) -> str:
    """``/media-plan?...`` carrying only the fields present in ``brief``."""
    pairs: List[str] = []

    def add(key: str, value: Any) -> None:
        pairs.append(f"{key}={quote(str(value), safe='')}")

    if brief.get("roles"):
        add("roles", "; ".join(brief["roles"]))
    if brief.get("locations"):
        add("locations", "; ".join(brief["locations"]))
    if brief.get("budget"):
        add("budget", brief["budget"])
    if brief.get("hires"):
        add("hires", brief["hires"])
    if brief.get("timeline"):
        add("timeline", brief["timeline"])
    if brief.get("industry"):
        add("industry", brief["industry"])
    if brief.get("client"):
        add("client", brief["client"])
    return _WIZARD_PATH + ("?" + "&".join(pairs) if pairs else "")


def build_media_plan_handoff(
    message: str,
    history: Optional[List[Dict[str, Any]]],
    context: Optional[Dict[str, Any]],
    result: Dict[str, Any],
    query_type: str,
    tool_results: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """The ``media_plan_handoff`` payload for a plan-shaped answer, else None."""
    if not isinstance(result, dict) or result.get("error"):
        return None
    response_text = result.get("response") or ""
    if not response_text.strip() or not is_plan_shaped(query_type, response_text):
        return None
    try:
        brief = extract_brief(message, history, context, tool_results)
    except Exception:
        logger.error("Media plan handoff brief parse failed", exc_info=True)
        brief = {}
    return {
        "url": build_handoff_url(brief),
        "label": HANDOFF_LABEL,
        "params": brief,
    }
