"""Inline Plan Copilot for Nova AI Suite.

Provides contextual AI nudges next to media plan form fields.
"Related titles widen the pool of qualified applicants."
The Grammarly model applied to media planning.

Honesty rule (wizard audit, trust lens): a nudge never states a percentage,
multiplier, dollar benchmark or day count unless it is read at runtime from a
data file that carries it. The only such figure is the metro cost multiplier
(``data/recruitment_benchmarks_2026_deep.json`` -> ``by_metro.*.metro_cpa_mult``,
cited in the copy as e.g. "blended C2ER and BLS public data": the sources that
metro's ``source_ids`` list). Everything else is qualitative advice. tests/test_plan_copilot_claims.py scans this file's
user-facing strings and fails on any other figure.
"""

import json
import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Role families and channel suggestions (editorial ordering, not published
# statistics -- the nudges built from them never print a score)
# ---------------------------------------------------------------------------

ROLE_FAMILIES: Dict[str, List[str]] = {
    "engineering": [
        "software engineer",
        "frontend developer",
        "backend developer",
        "full stack developer",
        "devops engineer",
        "sre",
        "data engineer",
        "machine learning engineer",
        "platform engineer",
        "qa engineer",
    ],
    "data_science": [
        "data scientist",
        "data analyst",
        "analytics engineer",
        "business intelligence",
        "ml researcher",
        "ai engineer",
    ],
    "product": [
        "product manager",
        "product owner",
        "product designer",
        "ux designer",
        "ui designer",
        "ux researcher",
    ],
    "sales": [
        "account executive",
        "sales development",
        "sdr",
        "bdr",
        "enterprise sales",
        "sales manager",
        "account manager",
    ],
    "marketing": [
        "marketing manager",
        "content marketer",
        "growth marketer",
        "digital marketing",
        "brand manager",
        "seo specialist",
    ],
    "executive": [
        "cto",
        "cfo",
        "coo",
        "vp engineering",
        "vp product",
        "director of engineering",
        "head of product",
        "c-suite",
    ],
    "healthcare": [
        "nurse",
        "physician",
        "pharmacist",
        "medical technician",
        "clinical researcher",
        "healthcare administrator",
    ],
    "operations": [
        "operations manager",
        "supply chain",
        "logistics",
        "warehouse manager",
        "procurement",
        "facilities manager",
    ],
}

# ---------------------------------------------------------------------------
# Metro cost multipliers -- read from the repo's benchmark data, never typed in
# ---------------------------------------------------------------------------
# The hard-coded table that used to live here ("Los Angeles cost index: 1.25x",
# "Remote could save 20-25%") matched no data file in the repo (the benchmark
# set has Los Angeles at 1.24x median cost per application, Dallas at 1.02x
# where the table said 0.93x, ...). The 40 US metros below come from
# data/recruitment_benchmarks_2026_deep.json -> by_metro.<metro>.metro_cpa_mult
# ("local multiplier vs national median"; C2ER cost of living + Appcast). There
# is no per-city source for non-US cities, so none is quoted for them.
BENCHMARK_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data",
    "recruitment_benchmarks_2026_deep.json",
)
# Each metro cites its own sources (by_metro.<metro>.source_ids): C2ER's
# cost-of-living index and BLS metro CPI for every metro, plus Appcast for some.
# The copy names exactly the ones the entry lists, as "blended ... public data" --
# the multiplier is not a Joveo survey.
_SOURCE_SHORT_NAMES = {
    "coli_c2er_2024": "C2ER",
    "bls_cpi_metro_2025": "BLS",
    "appcast_2025": "Appcast",
}


def _source_label(source_ids: List[str]) -> str:
    names = [_SOURCE_SHORT_NAMES[s] for s in source_ids if s in _SOURCE_SHORT_NAMES]
    if not names:
        return "public data"
    joined = names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]
    return f"blended {joined} public data"

_geo_lock = threading.Lock()
_geo_table: Optional[Dict[str, Dict[str, Any]]] = None


def _city_key(text: str) -> str:
    """Lower-case a place name to letters and single spaces ("St. Louis" ->
    "st louis")."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", "", str(text).lower())).strip()


def _load_geo_table() -> Dict[str, Dict[str, Any]]:
    """city key -> {"name", "state", "mult"} for every metro in by_metro.

    Keyed by the metro's principal city (first city of its MSA name). Returns
    {} when the data file is missing or unreadable (the nudges then say
    nothing about cost rather than guess).
    """
    table: Dict[str, Dict[str, Any]] = {}
    try:
        with open(BENCHMARK_FILE, encoding="utf-8") as fh:
            metros = (json.load(fh).get("by_metro")) or {}
    except (OSError, ValueError) as exc:
        logger.warning("plan_copilot: metro benchmarks unavailable: %s", exc)
        return table
    for key, entry in metros.items():
        if key.startswith("_") or not isinstance(entry, dict):
            continue
        mult = entry.get("metro_cpa_mult")
        if not isinstance(mult, (int, float)) or mult <= 0:
            continue
        state = key.rsplit("_", 1)[-1] if re.search(r"_[a-z]{2}$", key) else ""
        principal = re.split(r"[-,]", str(entry.get("msa") or ""))[0].strip()
        city = _city_key(principal or key.replace("_", " "))
        if not city:
            continue
        table[city] = {
            "name": principal.replace(".", "") or city.title(),
            "state": state,
            "mult": float(mult),
            "label": _source_label(list(entry.get("source_ids") or [])),
        }
    return table


def _geo_for(text: str) -> Optional[Dict[str, Any]]:
    """The by_metro entry for a location typed as "Los Angeles", "Los Angeles,
    CA" or "Greater Los Angeles area"; None for anything else (including
    non-US cities and "Remote": no source, so no figure)."""
    global _geo_table
    if not text:
        return None
    with _geo_lock:
        if _geo_table is None:
            _geo_table = _load_geo_table()
        table = _geo_table
    cleaned = re.sub(r"\b(greater|metro|metropolitan|area)\b", " ", str(text).lower())
    parts = [p.strip() for p in cleaned.split(",")]
    entry = table.get(_city_key(parts[0]))
    if not entry:
        return None
    # "Portland, ME" is not the Portland in the table (OR)
    if len(parts) > 1 and re.fullmatch(r"[a-z]{2}", parts[1]) and entry["state"]:
        if parts[1] != entry["state"]:
            return None
    return entry

# Relative ranking weights ONLY: they decide the ORDER channels are suggested
# in. They are not measured effectiveness rates and are never printed (they
# used to be shown as "LinkedIn (85%)" with "based on quality-of-hire
# benchmarks", which no data file in the repo backs).
CHANNEL_EFFECTIVENESS: Dict[str, Dict[str, float]] = {
    "engineering": {
        "LinkedIn": 0.85,
        "Indeed": 0.60,
        "Glassdoor": 0.55,
        "AngelList": 0.70,
        "HackerNews": 0.65,
        "Programmatic": 0.72,
    },
    "data_science": {
        "LinkedIn": 0.82,
        "Kaggle": 0.75,
        "Indeed": 0.58,
        "Glassdoor": 0.55,
        "AngelList": 0.65,
        "Programmatic": 0.70,
    },
    "product": {
        "LinkedIn": 0.88,
        "Indeed": 0.62,
        "Glassdoor": 0.60,
        "AngelList": 0.68,
        "Programmatic": 0.65,
    },
    "sales": {
        "LinkedIn": 0.90,
        "Indeed": 0.72,
        "Glassdoor": 0.65,
        "ZipRecruiter": 0.68,
        "Programmatic": 0.75,
    },
    "marketing": {
        "LinkedIn": 0.85,
        "Indeed": 0.65,
        "Glassdoor": 0.60,
        "AngelList": 0.62,
        "Programmatic": 0.70,
    },
    "executive": {
        "LinkedIn": 0.92,
        "Executive Networks": 0.85,
        "Glassdoor": 0.50,
        "Programmatic": 0.45,
    },
    "healthcare": {
        "Indeed": 0.80,
        "Health eCareers": 0.78,
        "LinkedIn": 0.65,
        "Programmatic": 0.72,
        "Glassdoor": 0.55,
    },
    "operations": {
        "Indeed": 0.78,
        "LinkedIn": 0.70,
        "ZipRecruiter": 0.72,
        "Programmatic": 0.68,
        "Glassdoor": 0.58,
    },
}

# Internal thresholds that choose which budget advice to show. They are NOT
# published benchmarks and are never printed (they used to appear in the copy
# as "Industry benchmark is $6,500/hire"); _nudges_budget only reads them.
BUDGET_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "engineering": {
        "min_per_hire": 3000,
        "optimal_per_hire": 6500,
        "max_per_hire": 15000,
    },
    "data_science": {
        "min_per_hire": 3500,
        "optimal_per_hire": 7000,
        "max_per_hire": 16000,
    },
    "product": {"min_per_hire": 2500, "optimal_per_hire": 5500, "max_per_hire": 12000},
    "sales": {"min_per_hire": 1500, "optimal_per_hire": 3500, "max_per_hire": 8000},
    "marketing": {
        "min_per_hire": 2000,
        "optimal_per_hire": 4500,
        "max_per_hire": 10000,
    },
    "executive": {
        "min_per_hire": 8000,
        "optimal_per_hire": 18000,
        "max_per_hire": 45000,
    },
    "healthcare": {
        "min_per_hire": 2000,
        "optimal_per_hire": 5000,
        "max_per_hire": 12000,
    },
    "operations": {
        "min_per_hire": 1200,
        "optimal_per_hire": 3000,
        "max_per_hire": 7000,
    },
}

# "optimal" is a recommendation (a wizard duration option), not a statistic.
# The "reason" lines are qualitative: they used to quote average days to fill
# ("42 days", "45+ day", "90+ day", "30-45 days") that no data file carried.
DURATION_RECOMMENDATIONS: Dict[str, Dict[str, str]] = {
    "engineering": {
        "optimal": "3-6 months",
        "reason": "Technical roles take longer to fill; longer campaigns improve quality of hire",
    },
    "data_science": {
        "optimal": "3-6 months",
        "reason": "Specialized talent pools require sustained outreach and a longer time to fill",
    },
    "product": {
        "optimal": "3-6 months",
        "reason": "Product roles benefit from a few months of brand awareness buildup",
    },
    "sales": {
        "optimal": "1-3 months",
        "reason": "Sales talent moves quickly; shorter, high-intensity campaigns convert better",
    },
    "marketing": {
        "optimal": "1-3 months",
        "reason": "Marketing candidates respond well to focused, shorter campaigns",
    },
    "executive": {
        "optimal": "6-12 months",
        "reason": "Executive search requires relationship building and usually the longest time to fill",
    },
    "healthcare": {
        "optimal": "1-3 months",
        "reason": "Healthcare hiring is urgent-driven; speed-to-post matters most",
    },
    "operations": {
        "optimal": "1-3 months",
        "reason": "Operations roles fill quickly with adequate sourcing",
    },
}

# Related titles for roles whose family list is a poor fit. The healthcare
# family is one flat keyword list (nurse, physician, pharmacist, ...), so a
# Registered Nurse was told to consider "Physician" and "Pharmacist".
RELATED_TITLE_OVERRIDES = (
    (
        "nurs",
        [
            "Licensed Practical Nurse",
            "Nurse Practitioner",
            "Nursing Assistant",
            "Medical Technician",
        ],
    ),
)

BROAD_TITLE_WARNINGS: List[str] = [
    "manager",
    "analyst",
    "coordinator",
    "specialist",
    "associate",
    "consultant",
    "developer",
    "engineer",
    "designer",
    "lead",
]


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def _classify_role_family(title: str) -> Optional[str]:
    """Classify a job title into a role family.

    Args:
        title: Job title string to classify

    Returns:
        Role family key or None if no match
    """
    title_lower = title.lower().strip()
    for family, keywords in ROLE_FAMILIES.items():
        for keyword in keywords:
            if keyword in title_lower:
                return family
    # Fallback heuristics
    if any(
        w in title_lower for w in ("develop", "program", "code", "software", "web dev")
    ):
        return "engineering"
    if any(w in title_lower for w in ("data", "analytics", "ml ", "ai ")):
        return "data_science"
    if any(w in title_lower for w in ("product", "ux", "ui")):
        return "product"
    if any(w in title_lower for w in ("sales", "account exec", "business develop")):
        return "sales"
    if any(w in title_lower for w in ("market", "brand", "content", "seo", "growth")):
        return "marketing"
    if any(
        w in title_lower for w in ("vp ", "director", "head of", "chief", "c-level")
    ):
        return "executive"
    return None


def _parse_budget_value(budget_str: str) -> Optional[float]:
    """Parse a budget string into a numeric value.

    Args:
        budget_str: Budget string like "$50,000 - $250,000" or "500K"

    Returns:
        Estimated numeric budget value, or None
    """
    if not budget_str:
        return None
    budget_str = budget_str.strip()

    # Handle range selects -- use midpoint
    range_match = re.search(r"\$?([\d,.]+)\s*[-\u2013]\s*\$?([\d,.]+)", budget_str)
    if range_match:
        try:
            low = float(range_match.group(1).replace(",", ""))
            high = float(range_match.group(2).replace(",", ""))
            return (low + high) / 2
        except ValueError:
            pass

    # Handle "Under $50,000"
    under_match = re.search(
        r"(?:under|<|less than)\s*\$?([\d,.]+)", budget_str, re.IGNORECASE
    )
    if under_match:
        try:
            return float(under_match.group(1).replace(",", "")) * 0.75
        except ValueError:
            pass

    # Handle "$3,000,000+" or "3M+"
    plus_match = re.search(r"\$?([\d,.]+)\+", budget_str)
    if plus_match:
        try:
            return float(plus_match.group(1).replace(",", "")) * 1.25
        except ValueError:
            pass

    # Handle shorthand: 500K, 12.5M, 2B
    short_match = re.search(r"\$?([\d.]+)\s*([KkMmBb])", budget_str)
    if short_match:
        try:
            num = float(short_match.group(1))
            multiplier_char = short_match.group(2).upper()
            multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
            return num * multipliers.get(multiplier_char, 1)
        except ValueError:
            pass

    # Plain number
    plain_match = re.search(r"\$?([\d,]+)", budget_str)
    if plain_match:
        try:
            return float(plain_match.group(1).replace(",", ""))
        except ValueError:
            pass

    return None


def _get_nudges_for_field(
    field: str, value: str, context: Dict[str, Any]
) -> List[Dict[str, str]]:
    """Generate nudges for a specific field based on value and context.

    Args:
        field: Form field name
        value: Current field value
        context: Other form field values

    Returns:
        List of nudge dicts
    """
    nudges: List[Dict[str, str]] = []

    try:
        if field == "job_title":
            nudges = _nudges_job_title(value, context)
        elif field == "budget":
            nudges = _nudges_budget(value, context)
        elif field == "location":
            nudges = _nudges_location(value, context)
        elif field == "channel":
            nudges = _nudges_channel(value, context)
        elif field == "duration":
            nudges = _nudges_duration(value, context)
    except Exception as e:
        logger.error("Error generating nudge for field=%s: %s", field, e, exc_info=True)

    return nudges


# ---------------------------------------------------------------------------
# Per-field nudge generators
# ---------------------------------------------------------------------------


def _nudges_job_title(value: str, context: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate nudges for the job_title field.

    Args:
        value: Current job title input
        context: Other form values

    Returns:
        List of nudge dicts
    """
    if not value or len(value) < 2:
        return []

    nudges: List[Dict[str, str]] = []
    value_lower = value.lower().strip()
    family = _classify_role_family(value)

    # Warn about overly broad titles
    if value_lower in BROAD_TITLE_WARNINGS:
        nudges.append(
            {
                "field": "job_title",
                "message": f'"{value}" is very broad and may attract unqualified applicants. Consider adding a specialty (e.g., "Marketing Manager" instead of "Manager").',
                "type": "warning",
                "confidence": 0.9,
                "source": "Nova Copilot -- title analysis",
            }
        )

    # Suggest similar role titles within the family
    if family:
        related = None
        for needle, titles in RELATED_TITLE_OVERRIDES:
            if needle in value_lower:
                related = [
                    t
                    for t in titles
                    if t.lower() not in value_lower and value_lower not in t.lower()
                ][:4]
                break
        if related is None:
            related = [
                kw.title()
                for kw in ROLE_FAMILIES[family]
                if kw != value_lower and kw not in value_lower and value_lower not in kw
            ][:4]
        if related:
            nudges.append(
                {
                    "field": "job_title",
                    "message": f"Related titles to consider: {', '.join(related)}. Covering related titles can widen your pool of qualified applicants.",
                    "type": "suggestion",
                    "confidence": 0.75,
                    "source": "Nova Copilot -- role family matching",
                }
            )

        # Channel hint based on role family
        top_channels = CHANNEL_EFFECTIVENESS.get(family, {})
        if top_channels:
            top_2 = sorted(top_channels.items(), key=lambda x: x[1], reverse=True)[:2]
            channel_names = " and ".join(ch[0] for ch in top_2)
            nudges.append(
                {
                    "field": "job_title",
                    "message": f"For {family.replace('_', ' ')} roles, {channel_names} are good channels to start with.",
                    "type": "info",
                    "confidence": 0.8,
                    "source": "Nova Copilot -- channel suggestions",
                }
            )

    return nudges


def _nudges_budget(value: str, context: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate nudges for the budget field.

    Args:
        value: Current budget value or range
        context: Other form values including roles

    Returns:
        List of nudge dicts
    """
    if not value or value == "__exact__":
        return []

    nudges: List[Dict[str, str]] = []
    budget = _parse_budget_value(value)
    if budget is None:
        return []

    # Determine role family from context
    roles = context.get("roles", [])
    if isinstance(roles, str):
        roles = [roles]

    family = None
    for role in roles:
        family = _classify_role_family(str(role))
        if family:
            break

    if family and family in BUDGET_BENCHMARKS:
        bench = BUDGET_BENCHMARKS[family]
        role_word = family.replace("_", " ")
        # Internal heuristic only: the thresholds decide WHICH advice to give;
        # none of them is printed (they are not published benchmarks).
        est_hires = max(1, round(budget / bench["optimal_per_hire"]))

        if budget < bench["min_per_hire"] * est_hires * 0.8:
            nudges.append(
                {
                    "field": "budget",
                    "message": f"This budget may be tight for {role_word} roles. Consider raising it, or narrowing the hiring volume or locations, for better reach.",
                    "type": "warning",
                    "confidence": 0.85,
                    "source": "Nova Copilot -- budget heuristic",
                }
            )
        elif budget > bench["max_per_hire"] * est_hires * 1.2:
            nudges.append(
                {
                    "field": "budget",
                    "message": f"This budget is generous for {role_word} roles. You could reallocate surplus to employer branding or tighten your cost-per-application targets.",
                    "type": "info",
                    "confidence": 0.7,
                    "source": "Nova Copilot -- budget heuristic",
                }
            )
        # No "your budget is well matched" branch: the wizard sends no hiring
        # volume here (est_hires is derived FROM the budget, so any budget
        # trivially fits it), so there is nothing true to say in the middle.

    # Location-adjusted budget insight (US metros with a benchmark only)
    locations = context.get("locations", [])
    if isinstance(locations, str):
        locations = [locations]
    for loc in locations[:1]:
        geo = _geo_for(str(loc))
        if geo and geo["mult"] > 1.2:
            adjusted = round(budget / geo["mult"])
            nudges.append(
                {
                    "field": "budget",
                    "message": f"In {geo['name']}, cost per application runs about {geo['mult']:.2f}x the national median ({geo['label']}), so your budget stretches like ~${adjusted:,} in a median market. A remote or hybrid option can widen the pool.",
                    "type": "suggestion",
                    "confidence": 0.75,
                    "source": f"Nova Copilot -- metro cost benchmark ({geo['label']})",
                }
            )
            break

    return nudges


def _nudges_location(value: str, context: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate nudges for the location field.

    Args:
        value: Current location input
        context: Other form values

    Returns:
        List of nudge dicts
    """
    if not value or len(value) < 2:
        return []

    nudges: List[Dict[str, str]] = []
    # US metros only: the benchmark set has no per-city figure for anywhere
    # else (or for "Remote"), so those get no cost nudge rather than a guess.
    geo = _geo_for(value)

    if geo:
        nudges.append(
            {
                "field": "location",
                "message": f"{geo['name']}: cost per application runs about {geo['mult']:.2f}x the national median ({geo['label']}).",
                "type": "info",
                "confidence": 0.85,
                "source": f"Nova Copilot -- metro cost benchmark ({geo['label']})",
            }
        )

        if geo["mult"] > 1.2:
            nudges.append(
                {
                    "field": "location",
                    "message": f"Adding a remote option alongside {value} can widen your talent pool and ease the local cost premium.",
                    "type": "suggestion",
                    "confidence": 0.7,
                    "source": "Nova Copilot -- remote alternative",
                }
            )

    # Role-specific location insight
    roles = context.get("roles", [])
    if isinstance(roles, str):
        roles = [roles]
    for role in roles[:1]:
        family = _classify_role_family(str(role))
        if family == "engineering" and geo and geo["mult"] > 1.2:
            nudges.append(
                {
                    "field": "location",
                    "message": f"Engineering talent in {value} is highly competitive, so plan specialist channels such as LinkedIn and GitHub alongside general job boards.",
                    "type": "info",
                    "confidence": 0.75,
                    "source": "Nova Copilot -- channel suggestions",
                }
            )
            break

    return nudges


def _nudges_channel(value: str, context: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate nudges for channel selection.

    Args:
        value: Selected channel name or comma-separated list
        context: Other form values

    Returns:
        List of nudge dicts
    """
    if not value:
        return []

    nudges: List[Dict[str, str]] = []

    # Determine role family
    roles = context.get("roles", [])
    if isinstance(roles, str):
        roles = [roles]

    family = None
    for role in roles:
        family = _classify_role_family(str(role))
        if family:
            break

    if not family:
        industry = context.get("industry") or ""
        if isinstance(industry, str) and industry.lower() in (
            "technology",
            "tech",
            "software",
        ):
            family = "engineering"

    if family and family in CHANNEL_EFFECTIVENESS:
        effectiveness = CHANNEL_EFFECTIVENESS[family]
        selected_channels = [ch.strip() for ch in value.split(",") if ch.strip()]

        # Recommend top channels not yet selected
        top_channels = sorted(effectiveness.items(), key=lambda x: x[1], reverse=True)
        missing_top = [
            (ch, score)
            for ch, score in top_channels
            if not any(
                ch.lower() in sel.lower() or sel.lower() in ch.lower()
                for sel in selected_channels
            )
        ][:2]

        if missing_top:
            # names only: the 0-1 scores in CHANNEL_EFFECTIVENESS are an
            # editorial ordering, not a published benchmark, so they are never
            # printed as percentages
            recs = ", ".join(ch for ch, _score in missing_top)
            nudges.append(
                {
                    "field": "channel",
                    "message": f"For {family.replace('_', ' ')} roles, consider adding: {recs}.",
                    "type": "suggestion",
                    "confidence": 0.8,
                    "source": "Nova Copilot -- channel suggestions",
                }
            )

        # Warn if only using generic channels
        selected_lower = [ch.lower() for ch in selected_channels]
        generic_only = all(
            ch in ("indeed", "glassdoor", "ziprecruiter") for ch in selected_lower
        )
        if generic_only and len(selected_lower) > 0:
            top_specialized = [
                ch
                for ch, _ in top_channels
                if ch.lower() not in ("indeed", "glassdoor", "ziprecruiter")
            ][:2]
            if top_specialized:
                nudges.append(
                    {
                        "field": "channel",
                        "message": f"Generic job boards alone may miss passive candidates. Adding {' or '.join(top_specialized)} can improve applicant quality.",
                        "type": "warning",
                        "confidence": 0.85,
                        "source": "Nova Copilot -- channel suggestions",
                    }
                )

    return nudges


def _nudges_duration(value: str, context: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate nudges for campaign duration selection.

    Args:
        value: Selected campaign duration
        context: Other form values

    Returns:
        List of nudge dicts
    """
    if not value:
        return []

    nudges: List[Dict[str, str]] = []

    # Determine role family
    roles = context.get("roles", [])
    if isinstance(roles, str):
        roles = [roles]

    family = None
    for role in roles:
        family = _classify_role_family(str(role))
        if family:
            break

    if family and family in DURATION_RECOMMENDATIONS:
        rec = DURATION_RECOMMENDATIONS[family]
        value_lower = value.lower().strip()
        optimal = rec["optimal"].lower()

        # Check if selected duration matches recommendation
        if optimal not in value_lower and value_lower not in optimal:
            nudges.append(
                {
                    "field": "duration",
                    "message": f"Recommended duration for {family.replace('_', ' ')} roles: {rec['optimal']}. {rec['reason']}.",
                    "type": "suggestion",
                    "confidence": 0.75,
                    "source": "Nova Copilot -- duration guidance",
                }
            )
        else:
            nudges.append(
                {
                    "field": "duration",
                    "message": f"Good choice. {rec['optimal']} aligns with best practices for {family.replace('_', ' ')} roles. {rec['reason']}.",
                    "type": "info",
                    "confidence": 0.85,
                    "source": "Nova Copilot -- duration guidance",
                }
            )

    # Warn about very short campaigns for specialized roles
    if value.lower() in ("1-3 months",) and family in ("executive", "data_science"):
        nudges.append(
            {
                "field": "duration",
                "message": f"Short campaigns for {family.replace('_', ' ')} roles risk low yield: passive candidates take time to engage, so a longer campaign usually works better.",
                "type": "warning",
                "confidence": 0.8,
                "source": "Nova Copilot -- duration guidance",
            }
        )

    # Warn about very long campaigns for high-turnover roles
    if "year" in value.lower() and family in ("sales", "operations"):
        nudges.append(
            {
                "field": "duration",
                "message": f"Long-term campaigns for {family.replace('_', ' ')} roles may see diminishing returns. Consider breaking into quarterly sprints for freshness.",
                "type": "suggestion",
                "confidence": 0.7,
                "source": "Nova Copilot -- duration guidance",
            }
        )

    return nudges


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_copilot_nudge(
    field: str, value: str, context: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, str]]:
    """Get a contextual nudge for a specific form field and value.

    Args:
        field: Form field name (job_title, budget, location, channel, duration)
        value: Current field value
        context: Optional dict with other form values for cross-field intelligence

    Returns:
        Dict with nudge data or None if no relevant nudge
    """
    context = context or {}
    nudges = _get_nudges_for_field(field, value, context)
    return nudges[0] if nudges else None


def get_all_nudges(form_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Get all relevant nudges for the current form state.

    Args:
        form_data: Complete form data dict

    Returns:
        List of nudge dicts with field, message, type, confidence, source
    """
    all_nudges: List[Dict[str, str]] = []
    for field, value in form_data.items():
        if value is None:
            continue
        nudge = get_copilot_nudge(field, str(value), form_data)
        if nudge:
            all_nudges.append(nudge)
    return all_nudges


def get_copilot_nudges_multi(
    field: str, value: str, context: Optional[Dict[str, Any]] = None
) -> List[Dict[str, str]]:
    """Get all contextual nudges for a specific form field (not just the first).

    Args:
        field: Form field name
        value: Current field value
        context: Optional dict with other form values

    Returns:
        List of nudge dicts
    """
    context = context or {}
    return _get_nudges_for_field(field, value, context)
