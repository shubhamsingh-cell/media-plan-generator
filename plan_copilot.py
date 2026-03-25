"""Inline Plan Copilot for Nova AI Suite.

Provides contextual AI nudges next to media plan form fields.
"Similar companies allocate 15% more to LinkedIn for this role."
The Grammarly model applied to media planning.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Benchmark data -- role families, geo cost indices, channel effectiveness
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

GEO_COST_INDEX: Dict[str, Dict[str, Any]] = {
    "san francisco": {"index": 1.45, "label": "Very High", "remote_saving": "30-40%"},
    "new york": {"index": 1.40, "label": "Very High", "remote_saving": "25-35%"},
    "seattle": {"index": 1.30, "label": "High", "remote_saving": "20-30%"},
    "boston": {"index": 1.28, "label": "High", "remote_saving": "20-30%"},
    "los angeles": {"index": 1.25, "label": "High", "remote_saving": "20-25%"},
    "austin": {"index": 1.10, "label": "Above Average", "remote_saving": "10-15%"},
    "denver": {"index": 1.08, "label": "Above Average", "remote_saving": "10-15%"},
    "chicago": {"index": 1.05, "label": "Average", "remote_saving": "10-15%"},
    "atlanta": {"index": 0.95, "label": "Below Average", "remote_saving": "5-10%"},
    "dallas": {"index": 0.93, "label": "Below Average", "remote_saving": "5-10%"},
    "phoenix": {"index": 0.90, "label": "Low", "remote_saving": "5-10%"},
    "london": {"index": 1.35, "label": "High", "remote_saving": "20-30%"},
    "berlin": {"index": 1.05, "label": "Average", "remote_saving": "10-15%"},
    "toronto": {"index": 1.15, "label": "Above Average", "remote_saving": "15-20%"},
    "sydney": {"index": 1.20, "label": "High", "remote_saving": "15-25%"},
    "singapore": {"index": 1.25, "label": "High", "remote_saving": "15-25%"},
    "bangalore": {"index": 0.55, "label": "Very Low", "remote_saving": "N/A"},
    "remote": {"index": 0.85, "label": "Low (avg)", "remote_saving": "N/A"},
}

CHANNEL_EFFECTIVENESS: Dict[str, Dict[str, float]] = {
    "engineering": {
        "LinkedIn": 0.85,
        "GitHub Jobs": 0.80,
        "Stack Overflow": 0.78,
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

DURATION_RECOMMENDATIONS: Dict[str, Dict[str, str]] = {
    "engineering": {
        "optimal": "3-6 months",
        "reason": "Technical roles average 42 days to fill; longer campaigns improve quality of hire",
    },
    "data_science": {
        "optimal": "3-6 months",
        "reason": "Specialized talent pools require sustained outreach; 45+ day avg time-to-fill",
    },
    "product": {
        "optimal": "3-6 months",
        "reason": "Product roles benefit from brand awareness buildup over 3+ months",
    },
    "sales": {
        "optimal": "1-3 months",
        "reason": "Sales talent moves quickly; shorter, high-intensity campaigns convert better",
    },
    "marketing": {
        "optimal": "1-3 months",
        "reason": "Marketing candidates respond well to focused 60-90 day campaigns",
    },
    "executive": {
        "optimal": "6-12 months",
        "reason": "Executive search requires relationship building; 90+ day avg time-to-fill",
    },
    "healthcare": {
        "optimal": "1-3 months",
        "reason": "Healthcare hiring is urgent-driven; speed-to-post matters most",
    },
    "operations": {
        "optimal": "1-3 months",
        "reason": "Operations roles fill within 30-45 days with adequate sourcing",
    },
}

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
        related = [
            kw.title()
            for kw in ROLE_FAMILIES[family]
            if kw != value_lower and kw not in value_lower and value_lower not in kw
        ][:4]
        if related:
            nudges.append(
                {
                    "field": "job_title",
                    "message": f"Related titles to consider: {', '.join(related)}. Multi-title campaigns typically see 23% more qualified applicants.",
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
                    "message": f"For {family.replace('_', ' ')} roles, {channel_names} typically deliver the highest quality candidates.",
                    "type": "info",
                    "confidence": 0.8,
                    "source": "Nova Copilot -- channel benchmarks",
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
        # Estimate number of hires (rough: budget / optimal cost per hire)
        est_hires = max(1, round(budget / bench["optimal_per_hire"]))

        if budget < bench["min_per_hire"] * est_hires * 0.8:
            nudges.append(
                {
                    "field": "budget",
                    "message": f"This budget may be tight for {family.replace('_', ' ')} roles. Industry benchmark is ${bench['optimal_per_hire']:,}/hire. Consider increasing by 20-30% for better reach.",
                    "type": "warning",
                    "confidence": 0.85,
                    "source": "Nova Copilot -- budget benchmarks",
                }
            )
        elif budget > bench["max_per_hire"] * est_hires * 1.2:
            nudges.append(
                {
                    "field": "budget",
                    "message": f"Budget is above typical range for {family.replace('_', ' ')} roles (${bench['max_per_hire']:,}/hire max benchmark). You could reallocate surplus to employer branding or reduce CPA targets.",
                    "type": "info",
                    "confidence": 0.7,
                    "source": "Nova Copilot -- budget benchmarks",
                }
            )
        else:
            per_hire = round(budget / est_hires)
            nudges.append(
                {
                    "field": "budget",
                    "message": f"~${per_hire:,}/hire for {family.replace('_', ' ')} roles is within the optimal range (${bench['min_per_hire']:,}-${bench['max_per_hire']:,}). Well positioned for competitive sourcing.",
                    "type": "info",
                    "confidence": 0.8,
                    "source": "Nova Copilot -- budget benchmarks",
                }
            )

    # Location-adjusted budget insight
    locations = context.get("locations", [])
    if isinstance(locations, str):
        locations = [locations]
    for loc in locations[:1]:
        loc_lower = str(loc).lower().strip()
        geo = GEO_COST_INDEX.get(loc_lower)
        if geo and geo["index"] > 1.2:
            adjusted = round(budget / geo["index"])
            nudges.append(
                {
                    "field": "budget",
                    "message": f"{loc} has a {geo['label'].lower()} cost index ({geo['index']}x). Your effective budget is ~${adjusted:,} in normalized terms. Remote hiring could save {geo['remote_saving']}.",
                    "type": "suggestion",
                    "confidence": 0.75,
                    "source": "Nova Copilot -- geo cost index",
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
    value_lower = value.lower().strip()
    geo = GEO_COST_INDEX.get(value_lower)

    if geo:
        nudges.append(
            {
                "field": "location",
                "message": f"{value} cost index: {geo['index']}x national average ({geo['label']}). Median CPA is ~{geo['index']}x the baseline.",
                "type": "info",
                "confidence": 0.85,
                "source": "Nova Copilot -- geo cost index",
            }
        )

        if geo["index"] > 1.2 and geo["remote_saving"] != "N/A":
            nudges.append(
                {
                    "field": "location",
                    "message": f"Adding a remote option alongside {value} could save {geo['remote_saving']} on CPA while expanding your talent pool by 3-5x.",
                    "type": "suggestion",
                    "confidence": 0.7,
                    "source": "Nova Copilot -- remote alternative analysis",
                }
            )
    else:
        # Partial matching for common cities
        partial_matches = [
            city
            for city in GEO_COST_INDEX
            if value_lower in city or city in value_lower
        ]
        if partial_matches:
            match = partial_matches[0]
            geo_match = GEO_COST_INDEX[match]
            nudges.append(
                {
                    "field": "location",
                    "message": f"{match.title()} cost index: {geo_match['index']}x national average ({geo_match['label']}).",
                    "type": "info",
                    "confidence": 0.65,
                    "source": "Nova Copilot -- geo cost index",
                }
            )

    # Role-specific location insight
    roles = context.get("roles", [])
    if isinstance(roles, str):
        roles = [roles]
    for role in roles[:1]:
        family = _classify_role_family(str(role))
        if family == "engineering" and geo and geo["index"] > 1.2:
            nudges.append(
                {
                    "field": "location",
                    "message": f"Engineering talent in {value} is highly competitive. Companies hiring here allocate 15-20% more to LinkedIn and GitHub than the national average.",
                    "type": "info",
                    "confidence": 0.75,
                    "source": "Nova Copilot -- market intelligence",
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
        industry = context.get("industry", "") or ""
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
            recs = ", ".join(f"{ch} ({int(score * 100)}%)" for ch, score in missing_top)
            nudges.append(
                {
                    "field": "channel",
                    "message": f"For {family.replace('_', ' ')} roles, consider adding: {recs}. Effectiveness score based on quality-of-hire benchmarks.",
                    "type": "suggestion",
                    "confidence": 0.8,
                    "source": "Nova Copilot -- channel effectiveness benchmarks",
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
                        "message": f"Generic job boards alone may miss passive candidates. Adding {' or '.join(top_specialized)} can improve applicant quality by 30-45%.",
                        "type": "warning",
                        "confidence": 0.85,
                        "source": "Nova Copilot -- channel mix analysis",
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
                    "source": "Nova Copilot -- duration benchmarks",
                }
            )
        else:
            nudges.append(
                {
                    "field": "duration",
                    "message": f"Good choice. {rec['optimal']} aligns with best practices for {family.replace('_', ' ')} roles. {rec['reason']}.",
                    "type": "info",
                    "confidence": 0.85,
                    "source": "Nova Copilot -- duration benchmarks",
                }
            )

    # Warn about very short campaigns for specialized roles
    if value.lower() in ("1-3 months",) and family in ("executive", "data_science"):
        nudges.append(
            {
                "field": "duration",
                "message": f"Short campaigns for {family.replace('_', ' ')} roles risk low yield. Only 15% of passive candidates engage in the first 30 days.",
                "type": "warning",
                "confidence": 0.8,
                "source": "Nova Copilot -- time-to-fill analysis",
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
                "source": "Nova Copilot -- campaign lifecycle analysis",
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
