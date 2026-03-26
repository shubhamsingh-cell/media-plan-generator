"""
quick_plan.py -- Role-Level Quick Plan Builder

Lightweight, instant channel recommendation engine for a single role + location
+ budget combo. Returns structured results for on-screen display without any
file download required.

Uses only cached/embedded data from budget_engine, trend_engine,
collar_intelligence, and research.py. No external API calls -- sub-100ms
response times.

Thread-safe, never crashes (all exceptions caught and degraded gracefully).
"""

from __future__ import annotations

import logging
import re
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Thread lock for shared mutable state ──
_LOCK = threading.Lock()

# ── Lazy imports (try/except matching codebase conventions) ──

try:
    import budget_engine as _budget_engine

    _HAS_BUDGET_ENGINE = True
except ImportError:
    _budget_engine = None  # type: ignore
    _HAS_BUDGET_ENGINE = False

try:
    import trend_engine as _trend_engine

    _HAS_TREND_ENGINE = True
except ImportError:
    _trend_engine = None  # type: ignore
    _HAS_TREND_ENGINE = False

try:
    import collar_intelligence as _collar_intel

    _HAS_COLLAR_INTEL = True
except ImportError:
    _collar_intel = None  # type: ignore
    _HAS_COLLAR_INTEL = False

try:
    import research as _research

    _HAS_RESEARCH = True
except ImportError:
    _research = None  # type: ignore
    _HAS_RESEARCH = False

try:
    from shared_utils import (
        INDUSTRY_LABEL_MAP,
        parse_budget,
        standardize_location,
    )
except ImportError:
    INDUSTRY_LABEL_MAP = {}

    def parse_budget(v, *, default=100_000.0):  # type: ignore
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    def standardize_location(s):  # type: ignore
        return s


# ═══════════════════════════════════════════════════════════════════════════════
# CHANNEL DEFINITIONS
# Maps quick-plan channels to trend_engine platforms and budget_engine categories.
# ═══════════════════════════════════════════════════════════════════════════════

QUICK_CHANNELS: Dict[str, Dict[str, Any]] = {
    "indeed": {
        "label": "Indeed",
        "icon": "briefcase",
        "platform": "indeed",
        "category": "job_board",
        "description": "World's largest job board. High volume, broad reach.",
    },
    "linkedin": {
        "label": "LinkedIn",
        "icon": "linkedin",
        "platform": "linkedin",
        "category": "niche_board",
        "description": "Professional network. Best for white-collar and senior roles.",
    },
    "google_search": {
        "label": "Google Search Ads",
        "icon": "search",
        "platform": "google_search",
        "category": "search",
        "description": "Capture active job seekers via paid search campaigns.",
    },
    "meta_facebook": {
        "label": "Meta (Facebook/Instagram)",
        "icon": "users",
        "platform": "meta_facebook",
        "category": "social",
        "description": "Social recruitment ads. Strong for blue/pink collar.",
    },
    "programmatic": {
        "label": "Programmatic & DSP",
        "icon": "zap",
        "platform": "programmatic",
        "category": "programmatic",
        "description": "Automated ad buying across job exchanges.",
    },
    "ziprecruiter": {
        "label": "ZipRecruiter",
        "icon": "target",
        "platform": "indeed",
        "category": "job_board",
        "description": "AI-matching job board with strong SMB presence.",
    },
    "glassdoor": {
        "label": "Glassdoor",
        "icon": "star",
        "platform": "indeed",
        "category": "niche_board",
        "description": "Employer brand + job listings. Research-driven candidates.",
    },
    "niche_boards": {
        "label": "Niche & Industry Boards",
        "icon": "layers",
        "platform": "indeed",
        "category": "niche_board",
        "description": "Specialized job boards for specific industries or roles.",
    },
}

# ── Collar-to-channel fit scores (0-100) ──
# How well each channel matches each collar type
_COLLAR_CHANNEL_FIT: Dict[str, Dict[str, int]] = {
    "blue_collar": {
        "indeed": 92,
        "linkedin": 18,
        "google_search": 65,
        "meta_facebook": 88,
        "programmatic": 90,
        "ziprecruiter": 85,
        "glassdoor": 25,
        "niche_boards": 60,
    },
    "white_collar": {
        "indeed": 72,
        "linkedin": 95,
        "google_search": 70,
        "meta_facebook": 45,
        "programmatic": 55,
        "ziprecruiter": 60,
        "glassdoor": 82,
        "niche_boards": 78,
    },
    "grey_collar": {
        "indeed": 80,
        "linkedin": 55,
        "google_search": 60,
        "meta_facebook": 65,
        "programmatic": 75,
        "ziprecruiter": 70,
        "glassdoor": 45,
        "niche_boards": 92,
    },
    "pink_collar": {
        "indeed": 85,
        "linkedin": 30,
        "google_search": 58,
        "meta_facebook": 82,
        "programmatic": 78,
        "ziprecruiter": 80,
        "glassdoor": 35,
        "niche_boards": 55,
    },
}

# ── Channel reasoning templates ──
_CHANNEL_REASONING: Dict[str, Dict[str, str]] = {
    "blue_collar": {
        "indeed": "Top channel for hourly/trade roles. High volume, mobile-friendly apply flow.",
        "linkedin": "Low fit for hourly roles. Most blue-collar candidates are not active on LinkedIn.",
        "google_search": 'Captures active job seekers searching for roles like "{role}".',
        "meta_facebook": "Excellent for reaching passive blue-collar candidates via social feeds.",
        "programmatic": "Automated distribution maximizes reach across job exchanges for volume hiring.",
        "ziprecruiter": "AI matching helps surface trade/hourly candidates. Strong in mid-markets.",
        "glassdoor": "Limited value -- blue-collar candidates rarely research employer brands here.",
        "niche_boards": "Industry-specific boards can deliver higher-quality trade candidates.",
    },
    "white_collar": {
        "indeed": "Broad reach for professional roles. Good baseline but less targeted.",
        "linkedin": "Premier channel for professional talent. InMail 3x response vs job boards.",
        "google_search": 'Captures intent-driven searches for "{role}" in {location}.',
        "meta_facebook": "Lower fit for professional roles but useful for employer branding.",
        "programmatic": "Broad reach but less targeted for specialized professional roles.",
        "ziprecruiter": "Decent reach but less preferred by mid-senior professionals.",
        "glassdoor": "Strong employer brand channel. Research-driven professionals check reviews.",
        "niche_boards": "Specialized boards (Dice, BuiltIn) deliver pre-qualified candidates.",
    },
    "grey_collar": {
        "indeed": "Major source for clinical/technical roles. Strong in healthcare hiring.",
        "linkedin": "Moderate fit. Senior clinical and technical roles benefit from LinkedIn.",
        "google_search": "Captures candidates searching for certified/licensed positions.",
        "meta_facebook": "Useful for reaching nurses and techs during off-hours on mobile.",
        "programmatic": "Good for volume clinical hiring across multiple facilities.",
        "ziprecruiter": "Helpful for mid-level clinical roles but niche boards outperform.",
        "glassdoor": "Limited impact for clinical roles vs specialized health boards.",
        "niche_boards": "Vivian, NurseFly, Health eCareers convert 2x better than general boards.",
    },
    "pink_collar": {
        "indeed": "Strong for admin, service, and care roles. High apply volume.",
        "linkedin": "Low fit for most pink-collar roles except senior admin positions.",
        "google_search": "Captures active seekers for customer service and admin roles.",
        "meta_facebook": "Excellent reach -- culture messaging outperforms compensation messaging.",
        "programmatic": "Good volume play for customer service and hospitality roles.",
        "ziprecruiter": "Quick-apply flow works well for admin and service candidates.",
        "glassdoor": "Minimal impact for entry-level service and care roles.",
        "niche_boards": "Care.com and hospitality boards deliver targeted candidates.",
    },
}

# ── Metro tier lookup for dynamic fit score adjustment ──
TIER_1_METROS: set[str] = {
    "new york",
    "san francisco",
    "los angeles",
    "chicago",
    "boston",
    "seattle",
    "washington dc",
    "washington d.c.",
}

TIER_2_METROS: set[str] = {
    "denver",
    "austin",
    "atlanta",
    "dallas",
    "phoenix",
    "portland",
    "minneapolis",
    "san diego",
    "raleigh",
    "nashville",
    "charlotte",
    "salt lake city",
}

# Industries with younger workforce demographics (social media boost)
_YOUNG_WORKFORCE_INDUSTRIES: set[str] = {
    "hospitality",
    "retail",
    "food_service",
    "entertainment",
    "quick_service_restaurant",
    "fast_food",
    "gig_economy",
    "startup",
    "gaming",
    "social_media",
}

# ── Dynamic Collar-to-Channel Fit Score Modifiers ──
# Lookup table: (collar_type, metro_tier, industry_group) -> per-channel modifier dict.
# Modifiers are additive adjustments (positive = boost, negative = penalty) applied
# on top of the base _COLLAR_CHANNEL_FIT scores before clamping to 0-100.
#
# Industry groups collapse granular industry keys into strategic categories.
_INDUSTRY_GROUP_MAP: Dict[str, str] = {
    "technology": "tech",
    "software": "tech",
    "saas": "tech",
    "fintech": "tech",
    "it_services": "tech",
    "cybersecurity": "tech",
    "healthcare": "healthcare",
    "hospital": "healthcare",
    "pharma": "healthcare",
    "nursing": "healthcare",
    "clinical": "healthcare",
    "logistics": "logistics",
    "transportation": "logistics",
    "warehousing": "logistics",
    "supply_chain": "logistics",
    "trucking": "logistics",
    "manufacturing": "manufacturing",
    "construction": "manufacturing",
    "industrial": "manufacturing",
    "retail": "retail",
    "hospitality": "retail",
    "food_service": "retail",
    "quick_service_restaurant": "retail",
    "fast_food": "retail",
    "restaurant": "retail",
    "finance": "finance",
    "banking": "finance",
    "insurance": "finance",
    "accounting": "finance",
    "government": "government",
    "defense": "government",
    "federal": "government",
    "education": "education",
    "higher_education": "education",
    "k12": "education",
}


def _resolve_industry_group(industry: str) -> str:
    """Map a granular industry key to a strategic industry group.

    Args:
        industry: Raw industry string from user input.

    Returns:
        Industry group key (e.g. "tech", "healthcare") or "general".
    """
    key = (industry or "").lower().replace(" ", "_").replace("-", "_")
    return _INDUSTRY_GROUP_MAP.get(key, "general")


# (collar_type, metro_tier, industry_group) -> channel modifier dict.
# Only non-zero modifiers are listed. Missing combos fall back to
# (collar, metro_tier, "general"), then (collar, "any", industry), then zero.
_FIT_SCORE_MODIFIERS: Dict[tuple[str, str, str], Dict[str, int]] = {
    # ── Blue collar ──
    ("blue_collar", "tier_1", "logistics"): {
        "programmatic": +5,
        "indeed": +3,
        "meta_facebook": +4,
        "google_search": +3,
    },
    ("blue_collar", "tier_1", "retail"): {
        "meta_facebook": +6,
        "programmatic": +4,
        "indeed": +3,
    },
    ("blue_collar", "tier_2", "manufacturing"): {
        "indeed": +5,
        "niche_boards": +4,
        "programmatic": +3,
    },
    ("blue_collar", "tier_2", "logistics"): {
        "programmatic": +4,
        "indeed": +4,
        "ziprecruiter": +3,
    },
    ("blue_collar", "tier_3", "manufacturing"): {
        "indeed": +6,
        "niche_boards": +5,
        "meta_facebook": +3,
    },
    ("blue_collar", "rural", "general"): {
        "indeed": +5,
        "meta_facebook": +5,
        "linkedin": -5,
    },
    # ── White collar ──
    ("white_collar", "tier_1", "tech"): {
        "linkedin": +5,
        "niche_boards": +6,
        "glassdoor": +4,
        "programmatic": -3,
    },
    ("white_collar", "tier_1", "finance"): {
        "linkedin": +4,
        "glassdoor": +5,
        "niche_boards": +3,
    },
    ("white_collar", "tier_2", "tech"): {
        "linkedin": +3,
        "niche_boards": +4,
        "indeed": +2,
    },
    ("white_collar", "tier_2", "healthcare"): {
        "niche_boards": +5,
        "indeed": +3,
        "linkedin": +2,
    },
    ("white_collar", "tier_3", "general"): {
        "indeed": +4,
        "linkedin": -2,
        "glassdoor": -2,
    },
    ("white_collar", "rural", "general"): {
        "indeed": +6,
        "linkedin": -5,
        "glassdoor": -4,
        "programmatic": +3,
    },
    # ── Grey collar ──
    ("grey_collar", "tier_1", "healthcare"): {
        "niche_boards": +6,
        "linkedin": +3,
        "glassdoor": +2,
    },
    ("grey_collar", "tier_2", "healthcare"): {
        "niche_boards": +5,
        "indeed": +3,
        "programmatic": +2,
    },
    ("grey_collar", "rural", "healthcare"): {
        "indeed": +5,
        "niche_boards": +4,
        "meta_facebook": +4,
        "linkedin": -3,
    },
    # ── Pink collar ──
    ("pink_collar", "tier_1", "retail"): {
        "meta_facebook": +5,
        "programmatic": +3,
        "indeed": +2,
    },
    ("pink_collar", "tier_2", "retail"): {
        "meta_facebook": +4,
        "indeed": +3,
        "ziprecruiter": +2,
    },
    ("pink_collar", "rural", "general"): {
        "indeed": +5,
        "meta_facebook": +4,
        "linkedin": -5,
    },
}


def _classify_metro_tier(location: str) -> str:
    """Classify a location into metro tier for fit score adjustment.

    Args:
        location: Free-text location string (e.g., "San Francisco, CA").

    Returns:
        One of "tier_1", "tier_2", "tier_3", or "rural".
    """
    loc_lower = (location or "").lower().strip()
    if not loc_lower:
        return "tier_3"

    for metro in TIER_1_METROS:
        if metro in loc_lower:
            return "tier_1"

    for metro in TIER_2_METROS:
        if metro in loc_lower:
            return "tier_2"

    # Check for common rural indicators
    rural_keywords = {"rural", "county", "township", "village", "unincorporated"}
    if any(kw in loc_lower for kw in rural_keywords):
        return "rural"

    return "tier_3"


def adjust_fit_scores(
    base_scores: Dict[str, int],
    location: str,
    industry: str,
    collar_type: str = "white_collar",
) -> Dict[str, int]:
    """Dynamically adjust collar-channel fit scores based on metro, industry, and collar.

    Uses a three-tier lookup strategy:
    1. Exact match: (collar_type, metro_tier, industry_group)
    2. Fallback: (collar_type, metro_tier, "general")
    3. Fallback: (collar_type, "any", industry_group) -- not yet populated

    On top of the lookup-table modifiers, legacy heuristic adjustments still apply:
    - LinkedIn: +3 in Tier 1 metros, -3 in rural areas
    - Indeed/job boards: +2 in Tier 2/3 areas (lower LinkedIn penetration)
    - Social media: +2 for younger workforce demographics
    - Glassdoor: +2 in Tier 1 metros (research-driven professionals)

    All scores are clamped to the 0-100 range.

    Args:
        base_scores: Dict of channel_key -> fit_score (0-100).
        location: Free-text location string.
        industry: Industry key or label.
        collar_type: Collar classification (blue_collar, white_collar, etc.).

    Returns:
        New dict of channel_key -> adjusted fit_score.
    """
    adjusted = dict(base_scores)
    metro_tier = _classify_metro_tier(location)
    industry_lower = (industry or "").lower().replace(" ", "_")
    industry_group = _resolve_industry_group(industry)

    try:
        # ── Lookup-table modifiers (collar, metro, industry) ──
        modifiers: Dict[str, int] = (
            _FIT_SCORE_MODIFIERS.get((collar_type, metro_tier, industry_group))
            or _FIT_SCORE_MODIFIERS.get((collar_type, metro_tier, "general"))
            or {}
        )
        for ch_key, delta in modifiers.items():
            if ch_key in adjusted:
                adjusted[ch_key] = adjusted[ch_key] + delta

        # ── Legacy heuristic adjustments (additive, stacks with lookup table) ──
        # LinkedIn adjustment: boost in Tier 1, reduce in rural
        if "linkedin" in adjusted:
            if metro_tier == "tier_1":
                adjusted["linkedin"] = adjusted["linkedin"] + 3
            elif metro_tier == "rural":
                adjusted["linkedin"] = adjusted["linkedin"] - 3

        # Indeed / job board boost in Tier 2/3/rural (lower LinkedIn penetration)
        if metro_tier in ("tier_2", "tier_3", "rural"):
            for ch in ("indeed", "ziprecruiter", "niche_boards"):
                if ch in adjusted:
                    adjusted[ch] = adjusted[ch] + 2

        # Social media boost for younger workforce demographics
        if industry_lower in _YOUNG_WORKFORCE_INDUSTRIES:
            if "meta_facebook" in adjusted:
                adjusted["meta_facebook"] = adjusted["meta_facebook"] + 2

        # Glassdoor boost in Tier 1 metros (research-driven market)
        if metro_tier == "tier_1":
            if "glassdoor" in adjusted:
                adjusted["glassdoor"] = adjusted["glassdoor"] + 2

        # Clamp all scores to 0-100
        adjusted = {k: max(0, min(100, v)) for k, v in adjusted.items()}

    except (ValueError, KeyError, TypeError) as exc:
        logger.warning(f"Fit score adjustment failed: {exc}")
        # Return base scores on any error
        return dict(base_scores)

    return adjusted


# ── Hiring difficulty descriptors ──
_DIFFICULTY_LEVELS = [
    (20, "Easy", "Ample candidate supply. Fast fills expected."),
    (40, "Moderate", "Balanced market. Standard effort needed."),
    (60, "Competitive", "Tight market. Strong EVP and competitive pay needed."),
    (80, "Hard", "Significant talent shortage. Premium sourcing required."),
    (
        100,
        "Very Hard",
        "Critical shortage. Sign-on bonuses and creative sourcing essential.",
    ),
]

# ── Month names for seasonal advice ──
_MONTH_NAMES = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ROLE INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════


def get_role_insights(role: str, industry: str = "") -> Dict[str, Any]:
    """Analyze a role: collar classification, salary range, hiring difficulty.

    Uses collar_intelligence for classification and research.py for salary data.
    All from embedded/cached data -- no API calls.

    Args:
        role: Job title (e.g., "Software Engineer", "Truck Driver")
        industry: Optional industry context for better classification

    Returns:
        {
            "role": str,
            "collar_type": str,
            "collar_label": str,
            "collar_confidence": float,
            "salary_range": str,
            "hiring_difficulty": int (0-100),
            "hiring_difficulty_label": str,
            "hiring_difficulty_description": str,
            "time_to_fill_days": int,
            "apply_rate": float,
            "mobile_apply_pct": float,
            "preferred_platforms": list,
            "key_insight": str,
        }
    """
    try:
        result: Dict[str, Any] = {"role": role}

        # ── Collar classification ──
        collar_type = "white_collar"
        collar_confidence = 0.5
        if _HAS_COLLAR_INTEL:
            try:
                classification = _collar_intel.classify_collar(role, industry)
                collar_type = classification.get("collar_type", "white_collar")
                collar_confidence = classification.get("confidence", 0.5)
            except Exception as e:
                logger.warning("Collar classification failed for '%s': %s", role, e)

        collar_labels = {
            "blue_collar": "Blue Collar",
            "white_collar": "White Collar",
            "grey_collar": "Grey Collar",
            "pink_collar": "Pink Collar",
        }
        result["collar_type"] = collar_type
        result["collar_label"] = collar_labels.get(collar_type, "White Collar")
        result["collar_confidence"] = round(collar_confidence, 2)

        # ── Salary range ──
        salary_range = "$45,000 - $80,000"
        if _HAS_RESEARCH:
            try:
                salary_range = _research.get_role_salary_range(role, location_coli=100)
            except Exception as e:
                logger.warning("Salary lookup failed for '%s': %s", role, e)
        result["salary_range"] = salary_range

        # ── Strategy metadata from COLLAR_STRATEGY ──
        strategy = {}
        if _HAS_COLLAR_INTEL and hasattr(_collar_intel, "COLLAR_STRATEGY"):
            strategy = _collar_intel.COLLAR_STRATEGY.get(collar_type, {})

        result["time_to_fill_days"] = strategy.get("time_to_fill_benchmark_days", 28)
        result["apply_rate"] = strategy.get("avg_apply_rate", 0.05)
        result["mobile_apply_pct"] = strategy.get("mobile_apply_pct", 0.55)
        result["preferred_platforms"] = strategy.get("preferred_platforms") or [][:5]
        result["key_insight"] = strategy.get("key_insight") or ""

        # ── Hiring difficulty (derived from time_to_fill, apply_rate, collar type) ──
        ttf = result["time_to_fill_days"]
        apply_rate = result["apply_rate"]
        # Higher TTF and lower apply rate = harder to hire
        difficulty = min(
            100, max(0, int((ttf / 50.0) * 50 + (1.0 - apply_rate / 0.10) * 50))
        )
        # Collar adjustments
        if collar_type == "grey_collar":
            difficulty = min(100, difficulty + 10)  # healthcare shortage
        elif collar_type == "blue_collar":
            difficulty = max(0, difficulty - 5)  # generally available

        result["hiring_difficulty"] = difficulty
        for threshold, label, desc in _DIFFICULTY_LEVELS:
            if difficulty <= threshold:
                result["hiring_difficulty_label"] = label
                result["hiring_difficulty_description"] = desc
                break
        else:
            result["hiring_difficulty_label"] = "Very Hard"
            result["hiring_difficulty_description"] = _DIFFICULTY_LEVELS[-1][2]

        return result

    except Exception as e:
        logger.error("get_role_insights failed for '%s': %s", role, e, exc_info=True)
        return {
            "role": role,
            "collar_type": "white_collar",
            "collar_label": "White Collar",
            "collar_confidence": 0.3,
            "salary_range": "$45,000 - $80,000",
            "hiring_difficulty": 50,
            "hiring_difficulty_label": "Moderate",
            "hiring_difficulty_description": "Balanced market. Standard effort needed.",
            "time_to_fill_days": 28,
            "apply_rate": 0.05,
            "mobile_apply_pct": 0.55,
            "preferred_platforms": ["Indeed", "LinkedIn"],
            "key_insight": "",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. LOCATION INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════


def get_location_insights(location: str) -> Dict[str, Any]:
    """Analyze a location: COLI, market tightness, metro info.

    Uses research.py get_location_info for embedded data. No API calls.

    Args:
        location: Location string (e.g., "San Francisco, CA", "London", "Remote")

    Returns:
        {
            "location": str,
            "display_location": str,
            "coli": int,
            "coli_label": str,
            "market_tightness": str (loose/balanced/tight/very_tight),
            "market_tightness_score": int (0-100),
            "metro_name": str,
            "population": str,
            "region": str,
            "is_international": bool,
            "is_remote": bool,
            "cpc_multiplier": float,
            "top_employers_industries": list,
        }
    """
    try:
        display_loc = standardize_location(location) if location else "National"
        is_remote = _is_remote_location(location)

        result: Dict[str, Any] = {
            "location": location,
            "display_location": display_loc,
            "is_remote": is_remote,
            "is_international": False,
        }

        if is_remote:
            result.update(
                {
                    "coli": 100,
                    "coli_label": "National Average",
                    "market_tightness": "balanced",
                    "market_tightness_score": 50,
                    "metro_name": "Remote / National",
                    "population": "N/A",
                    "region": "National",
                    "cpc_multiplier": 1.0,
                    "top_employers_industries": [],
                }
            )
            return result

        # ── Location data from research.py ──
        loc_info: Dict[str, Any] = {}
        if _HAS_RESEARCH:
            try:
                loc_info = _research.get_location_info(location)
            except Exception as e:
                logger.warning("Location lookup failed for '%s': %s", location, e)

        coli = loc_info.get("coli", 100)
        is_intl = loc_info.get("is_international", False)

        result["coli"] = int(coli)
        result["is_international"] = is_intl
        result["metro_name"] = loc_info.get("metro_name", display_loc)
        result["population"] = _format_population(loc_info.get("population") or 0)
        result["region"] = loc_info.get("region", loc_info.get("state") or "")
        result["top_employers_industries"] = loc_info.get(
            "major_employers", loc_info.get("top_industries") or []
        )
        if isinstance(result["top_employers_industries"], str):
            result["top_employers_industries"] = [
                s.strip()
                for s in result["top_employers_industries"].split(",")
                if s.strip()
            ]

        # ── COLI label ──
        if coli >= 140:
            result["coli_label"] = "Very High Cost"
        elif coli >= 115:
            result["coli_label"] = "High Cost"
        elif coli >= 95:
            result["coli_label"] = "Average Cost"
        elif coli >= 75:
            result["coli_label"] = "Below Average"
        else:
            result["coli_label"] = "Low Cost"

        # ── CPC multiplier from trend_engine ──
        cpc_mult = 1.0
        if _HAS_TREND_ENGINE:
            try:
                if hasattr(_trend_engine, "REGIONAL_CPC_MULTIPLIERS_US"):
                    for (
                        metro,
                        mult,
                    ) in _trend_engine.REGIONAL_CPC_MULTIPLIERS_US.items():
                        if _location_matches(location, metro):
                            cpc_mult = mult
                            break
                if (
                    cpc_mult == 1.0
                    and is_intl
                    and hasattr(_trend_engine, "REGIONAL_CPC_MULTIPLIERS_INTL")
                ):
                    country = loc_info.get("country") or ""
                    if (
                        country
                        and country in _trend_engine.REGIONAL_CPC_MULTIPLIERS_INTL
                    ):
                        cpc_mult = _trend_engine.REGIONAL_CPC_MULTIPLIERS_INTL[country]
            except Exception as e:
                logger.warning("CPC multiplier lookup failed: %s", e)
        result["cpc_multiplier"] = round(cpc_mult, 2)

        # ── Market tightness (derived from COLI + CPC multiplier) ──
        tightness_score = min(
            100, max(0, int((coli / 150.0) * 40 + (cpc_mult / 1.7) * 40 + 10))
        )
        result["market_tightness_score"] = tightness_score
        if tightness_score >= 75:
            result["market_tightness"] = "very_tight"
        elif tightness_score >= 55:
            result["market_tightness"] = "tight"
        elif tightness_score >= 35:
            result["market_tightness"] = "balanced"
        else:
            result["market_tightness"] = "loose"

        return result

    except Exception as e:
        logger.error(
            "get_location_insights failed for '%s': %s", location, e, exc_info=True
        )
        return {
            "location": location,
            "display_location": location,
            "coli": 100,
            "coli_label": "Average Cost",
            "market_tightness": "balanced",
            "market_tightness_score": 50,
            "metro_name": location,
            "population": "N/A",
            "region": "",
            "is_international": False,
            "is_remote": False,
            "cpc_multiplier": 1.0,
            "top_employers_industries": [],
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CHANNEL SCORING & RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def score_channels_for_context(
    role: str,
    location: str,
    industry: str,
    collar_type: str,
    budget: float = 100_000.0,
    cpc_multiplier: float = 1.0,
) -> List[Dict[str, Any]]:
    """Score each channel's fit for a specific role/location/industry combo.

    Returns a sorted list (best fit first) of channel recommendation dicts.

    Args:
        role: Job title
        location: Location string
        industry: Industry key or label
        collar_type: blue_collar, white_collar, grey_collar, pink_collar
        budget: Total budget in USD
        cpc_multiplier: Regional CPC adjustment factor

    Returns:
        List of {channel_key, channel_name, fit_score, allocation_pct, spend,
                 cpc, cpa, projected_clicks, projected_applies,
                 projected_hires, reasoning}
    """
    try:
        collar = collar_type if collar_type in _COLLAR_CHANNEL_FIT else "white_collar"
        base_fit_scores = _COLLAR_CHANNEL_FIT.get(
            collar, _COLLAR_CHANNEL_FIT["white_collar"]
        )

        # ── Dynamically adjust fit scores for location, industry, and collar ──
        fit_scores = adjust_fit_scores(base_fit_scores, location, industry, collar)

        # ── Get channel allocation percentages from collar strategy ──
        alloc_pcts = _get_collar_allocation(collar)

        # ── Map our channels to allocation categories ──
        channel_alloc_map = {
            "indeed": "global_job_boards",
            "linkedin": "linkedin",
            "google_search": "search",
            "meta_facebook": "social_media",
            "programmatic": "programmatic",
            "ziprecruiter": "global_job_boards",
            "glassdoor": "niche_boards",
            "niche_boards": "niche_boards",
        }

        # ── Compute raw allocation for each channel ──
        raw_allocs: Dict[str, float] = {}
        for ch_key, ch_info in QUICK_CHANNELS.items():
            alloc_cat = channel_alloc_map.get(ch_key, "programmatic")
            base_alloc = alloc_pcts.get(alloc_cat, 0.05)
            fit = fit_scores.get(ch_key, 50) / 100.0

            # Weighted allocation: base strategy * fit score
            raw_allocs[ch_key] = base_alloc * fit

        # Normalize allocations to sum to 1.0
        total_raw = sum(raw_allocs.values())
        if total_raw > 0:
            norm_allocs = {k: v / total_raw for k, v in raw_allocs.items()}
        else:
            n = len(QUICK_CHANNELS)
            norm_allocs = {k: 1.0 / n for k in QUICK_CHANNELS}

        # ── Get benchmarks from trend_engine ──
        benchmarks: Dict[str, Dict[str, Any]] = {}
        if _HAS_TREND_ENGINE:
            try:
                benchmarks = _trend_engine.get_all_platform_benchmarks(
                    industry=industry or "general_entry_level",
                    collar_type=collar,
                    location=location,
                )
            except Exception as e:
                logger.warning("Trend benchmarks failed: %s", e)

        # ── Base benchmarks fallback ──
        base_cpc_map = {
            "indeed": 0.85,
            "linkedin": 3.80,
            "google_search": 2.50,
            "meta_facebook": 1.20,
            "programmatic": 0.65,
            "ziprecruiter": 0.90,
            "glassdoor": 1.40,
            "niche_boards": 1.40,
        }
        base_apply_rates = {
            "indeed": 0.08,
            "linkedin": 0.04,
            "google_search": 0.05,
            "meta_facebook": 0.03,
            "programmatic": 0.06,
            "ziprecruiter": 0.07,
            "glassdoor": 0.05,
            "niche_boards": 0.10,
        }

        # ── Collar-based hire rate ──
        hire_rates = {
            "blue_collar": 0.06,
            "white_collar": 0.025,
            "grey_collar": 0.04,
            "pink_collar": 0.05,
        }
        hire_rate = hire_rates.get(collar, 0.03)

        # ── Build scored channel list ──
        channels: List[Dict[str, Any]] = []
        for ch_key, ch_info in QUICK_CHANNELS.items():
            alloc_pct = round(norm_allocs.get(ch_key, 0) * 100, 1)
            spend = round(budget * norm_allocs.get(ch_key, 0), 2)
            fit = fit_scores.get(ch_key, 50)

            # CPC: prefer trend_engine, fall back to base
            cpc = base_cpc_map.get(ch_key, 1.00)
            platform_key = ch_info.get("platform", "indeed")
            if platform_key in benchmarks:
                bench = benchmarks[platform_key]
                if isinstance(bench, dict):
                    cpc_data = bench.get("cpc", {})
                    if isinstance(cpc_data, dict) and "value" in cpc_data:
                        cpc = cpc_data["value"]
                    elif isinstance(cpc_data, (int, float)):
                        cpc = float(cpc_data)

            # Apply regional multiplier
            cpc = round(cpc * cpc_multiplier, 2)

            # Projections
            projected_clicks = int(spend / cpc) if cpc > 0 else 0
            apply_rate = base_apply_rates.get(ch_key, 0.05)
            projected_applies = int(projected_clicks * apply_rate)
            projected_hires = (
                max(1, int(projected_applies * hire_rate))
                if projected_applies > 0
                else 0
            )
            cpa = round(spend / projected_applies, 2) if projected_applies > 0 else 0

            # Reasoning
            reasoning_templates = _CHANNEL_REASONING.get(
                collar, _CHANNEL_REASONING["white_collar"]
            )
            reasoning = reasoning_templates.get(
                ch_key, ch_info.get("description") or ""
            )
            reasoning = reasoning.replace("{role}", role).replace(
                "{location}", location
            )

            # Quality score from budget_engine
            quality_info = {}
            if _HAS_BUDGET_ENGINE:
                try:
                    quality_info = _budget_engine.score_channel_quality(
                        ch_info.get("category", "job_board"),
                        collar_type=collar,
                        industry=industry,
                    )
                except Exception:
                    pass

            channels.append(
                {
                    "channel_key": ch_key,
                    "channel_name": ch_info["label"],
                    "icon": ch_info.get("icon", "briefcase"),
                    "fit_score": fit,
                    "allocation_pct": alloc_pct,
                    "spend": round(spend, 2),
                    "cpc": cpc,
                    "cpa": cpa,
                    "projected_clicks": projected_clicks,
                    "projected_applies": projected_applies,
                    "projected_hires": projected_hires,
                    "reasoning": reasoning,
                    "quality_score": quality_info.get("quality_score") or 0,
                    "retention_6mo": quality_info.get("retention_6mo_pct") or 0,
                }
            )

        # Sort by fit_score descending
        channels.sort(key=lambda c: c["fit_score"], reverse=True)
        return channels

    except Exception as e:
        logger.error("score_channels_for_context failed: %s", e, exc_info=True)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# 4. BUDGET ASSESSMENT
# ═══════════════════════════════════════════════════════════════════════════════


def _assess_budget(
    budget: float,
    collar_type: str,
    industry: str,
    channel_recommendations: List[Dict],
) -> Dict[str, Any]:
    """Assess whether the budget is sufficient for the role/location combo.

    Returns:
        {
            "status": "sufficient" | "tight" | "insufficient",
            "explanation": str,
            "budget_per_hire": float,
            "industry_avg_cph": list[float, float],
            "recommended_budget": float,
        }
    """
    try:
        total_projected_hires = sum(
            c.get("projected_hires") or 0 for c in channel_recommendations
        )
        total_projected_applies = sum(
            c.get("projected_applies") or 0 for c in channel_recommendations
        )

        if total_projected_hires <= 0:
            total_projected_hires = 1

        budget_per_hire = budget / total_projected_hires

        # Industry CPH ranges from budget_engine
        cph_ranges = {
            "healthcare_medical": (9000, 12000),
            "tech_engineering": (6000, 14000),
            "blue_collar_trades": (3500, 5600),
            "general_entry_level": (2000, 4700),
            "finance_banking": (5000, 12000),
            "retail_consumer": (2500, 5000),
            "logistics_supply_chain": (3000, 5500),
            "hospitality_travel": (2000, 4000),
            "construction_real_estate": (3500, 6000),
            "education": (3000, 6000),
        }
        if _HAS_BUDGET_ENGINE and hasattr(_budget_engine, "INDUSTRY_CPH_RANGES"):
            cph_ranges.update(_budget_engine.INDUSTRY_CPH_RANGES)

        industry_cph = cph_ranges.get(industry, (4000, 8000))
        avg_cph = (industry_cph[0] + industry_cph[1]) / 2

        # Collar-based minimum viable budgets per hire
        collar_minimums = {
            "blue_collar": 2500,
            "white_collar": 6000,
            "grey_collar": 5000,
            "pink_collar": 3000,
        }
        min_viable_cph = collar_minimums.get(collar_type, 4000)

        if budget_per_hire >= avg_cph:
            status = "sufficient"
            explanation = (
                f"Your budget of ${budget:,.0f} provides ${budget_per_hire:,.0f} per projected hire, "
                f"which meets or exceeds the industry average of ${industry_cph[0]:,}-${industry_cph[1]:,} per hire. "
                f"You have room for competitive bids and premium placements."
            )
        elif budget_per_hire >= min_viable_cph:
            status = "tight"
            explanation = (
                f"Your budget of ${budget:,.0f} provides ${budget_per_hire:,.0f} per projected hire. "
                f"This is below the industry average of ${industry_cph[0]:,}-${industry_cph[1]:,} but workable. "
                f"Focus on high-converting channels and optimize bids carefully."
            )
        else:
            status = "insufficient"
            recommended = int(total_projected_hires * avg_cph)
            explanation = (
                f"Your budget of ${budget:,.0f} provides only ${budget_per_hire:,.0f} per projected hire, "
                f"well below the minimum viable ${min_viable_cph:,} for {collar_type.replace('_', ' ')} roles. "
                f"Consider increasing to ${recommended:,} or reducing scope to fewer channels."
            )

        recommended_budget = int(total_projected_hires * avg_cph)

        return {
            "status": status,
            "explanation": explanation,
            "budget_per_hire": round(budget_per_hire, 2),
            "industry_avg_cph": list(industry_cph),
            "recommended_budget": recommended_budget,
        }

    except Exception as e:
        logger.error("Budget assessment failed: %s", e, exc_info=True)
        return {
            "status": "tight",
            "explanation": "Unable to fully assess budget. Review channel allocations carefully.",
            "budget_per_hire": budget,
            "industry_avg_cph": [4000, 8000],
            "recommended_budget": int(budget),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 5. SEASONAL ADVICE
# ═══════════════════════════════════════════════════════════════════════════════


def _get_seasonal_advice(collar_type: str) -> Dict[str, Any]:
    """Return seasonal hiring advice: best and worst months, current adjustment.

    Returns:
        {
            "current_month": str,
            "current_multiplier": float,
            "current_label": str,
            "best_months": list of {"month": str, "multiplier": float},
            "worst_months": list of {"month": str, "multiplier": float},
            "advice": str,
        }
    """
    try:
        current_month = datetime.now().month
        collar = (
            collar_type
            if collar_type in ("blue_collar", "white_collar", "grey_collar")
            else "mixed"
        )

        # Get seasonal data from trend_engine
        multipliers: Dict[int, float] = {}
        if _HAS_TREND_ENGINE and hasattr(_trend_engine, "SEASONAL_MULTIPLIERS"):
            multipliers = _trend_engine.SEASONAL_MULTIPLIERS.get(collar, {})

        if not multipliers:
            # Fallback
            multipliers = {m: 1.0 for m in range(1, 13)}

        current_mult = multipliers.get(current_month, 1.0)

        # Find best and worst months
        sorted_months = sorted(multipliers.items(), key=lambda x: x[1])
        worst_3 = sorted_months[:3]
        best_3 = sorted_months[-3:][::-1]

        best_months = [
            {"month": _MONTH_NAMES[m], "multiplier": round(v, 2)} for m, v in best_3
        ]
        worst_months = [
            {"month": _MONTH_NAMES[m], "multiplier": round(v, 2)} for m, v in worst_3
        ]

        # Current month assessment
        if current_mult >= 1.10:
            current_label = "Peak Season"
            advice = (
                f"{_MONTH_NAMES[current_month]} is a peak hiring period for {collar.replace('_', ' ')} roles "
                f"(demand multiplier: {current_mult:.2f}x). Expect higher CPCs but also more candidate activity. "
                f"Act fast -- competition is high."
            )
        elif current_mult >= 1.0:
            current_label = "Good Season"
            advice = (
                f"{_MONTH_NAMES[current_month]} is a solid hiring period "
                f"(demand multiplier: {current_mult:.2f}x). Balanced market conditions. "
                f"Good time to launch campaigns at standard bid levels."
            )
        elif current_mult >= 0.90:
            current_label = "Off-Peak"
            advice = (
                f"{_MONTH_NAMES[current_month]} is slightly below peak "
                f"(demand multiplier: {current_mult:.2f}x). Lower competition means "
                f"your budget goes further. Great time for cost-efficient hiring."
            )
        else:
            current_label = "Low Season"
            advice = (
                f"{_MONTH_NAMES[current_month]} is a low hiring period "
                f"(demand multiplier: {current_mult:.2f}x). Candidate volume drops but "
                f"CPCs are lowest. Consider scheduling campaigns for "
                f"{best_months[0]['month']} or {best_months[1]['month']} for maximum impact."
            )

        # Build human-readable adjustment explanation
        pct_change = round((current_mult - 1.0) * 100, 1)
        if abs(pct_change) < 1.0:
            adjustment_label = "No seasonal adjustment applied (baseline month)"
        elif pct_change > 0:
            adjustment_label = f"Adjusted +{pct_change}% for {_MONTH_NAMES[current_month]} {collar.replace('_', ' ')} hiring surge"
        else:
            adjustment_label = f"Adjusted {pct_change}% for {_MONTH_NAMES[current_month]} {collar.replace('_', ' ')} hiring lull"

        # Quarter label
        quarter = (current_month - 1) // 3 + 1
        quarter_label = f"Q{quarter}"

        return {
            "current_month": _MONTH_NAMES[current_month],
            "current_multiplier": round(current_mult, 2),
            "current_label": current_label,
            "adjustment_label": adjustment_label,
            "adjustment_pct": pct_change,
            "quarter": quarter_label,
            "best_months": best_months,
            "worst_months": worst_months,
            "advice": advice,
            "budget_impact_note": (
                f"Your effective CPC is {current_mult:.2f}x baseline due to {_MONTH_NAMES[current_month]} "
                f"seasonal demand. Plan budget accordingly."
                if abs(pct_change) >= 5.0
                else "Minimal seasonal impact on your budget this month."
            ),
        }

    except Exception as e:
        logger.error("Seasonal advice failed: %s", e, exc_info=True)
        return {
            "current_month": _MONTH_NAMES[datetime.now().month],
            "current_multiplier": 1.0,
            "current_label": "Normal",
            "adjustment_label": "No seasonal adjustment applied",
            "adjustment_pct": 0.0,
            "quarter": f"Q{(datetime.now().month - 1) // 3 + 1}",
            "best_months": [],
            "worst_months": [],
            "advice": "Seasonal data unavailable. Campaign timing should be based on your hiring urgency.",
            "budget_impact_note": "Seasonal impact unknown. Use baseline budget assumptions.",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 6. A/B TESTING RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _generate_ab_testing_recommendations(
    collar_type: str,
    channel_recommendations: List[Dict[str, Any]],
    role: str,
    location: str,
    budget: float,
) -> List[Dict[str, Any]]:
    """Generate A/B testing recommendations for the media plan.

    Produces actionable test hypotheses based on the collar type, top channels,
    role, and budget. Each recommendation includes the variable to test, a
    hypothesis, suggested duration, and sample size guidance.

    Args:
        collar_type: Collar classification for the role.
        channel_recommendations: Scored channel list from ``score_channels_for_context``.
        role: Job title being planned for.
        location: Target location.
        budget: Total plan budget in USD.

    Returns:
        List of A/B test recommendation dicts, each containing:
        - test_id, variable, hypothesis, channels, duration_days,
          min_sample_size, priority, expected_lift_pct.
    """
    tests: List[Dict[str, Any]] = []

    # Sort channels by spend descending to identify top spenders
    top_channels = sorted(
        channel_recommendations, key=lambda c: c.get("spend", 0), reverse=True
    )
    top_2 = top_channels[:2] if len(top_channels) >= 2 else top_channels

    # 1. Channel comparison test (always recommended if 2+ channels)
    if len(top_2) >= 2:
        ch_a = top_2[0].get("channel_name", "Channel A")
        ch_b = top_2[1].get("channel_name", "Channel B")
        tests.append(
            {
                "test_id": "channel_comparison",
                "variable": "Channel Performance",
                "hypothesis": (
                    f"Test {ch_a} vs {ch_b} for {role} roles in {location}. "
                    f"Allocate equal budget to each channel for a controlled comparison."
                ),
                "channels": [ch_a, ch_b],
                "duration_days": 14,
                "min_sample_size": 200,
                "priority": "high",
                "expected_lift_pct": 15,
                "budget_split": "50/50",
            }
        )

    # 2. Collar-specific messaging test
    messaging_tests: Dict[str, Dict[str, str]] = {
        "blue_collar": {
            "variable": "Ad Copy: Pay vs Benefits",
            "hypothesis": (
                f"Test pay-rate-first messaging vs benefits-first messaging for {role}. "
                f"Blue collar candidates typically respond better to upfront pay transparency."
            ),
            "variant_a": "Lead with hourly rate and schedule",
            "variant_b": "Lead with benefits package and perks",
        },
        "white_collar": {
            "variable": "Ad Copy: Growth vs Compensation",
            "hypothesis": (
                f"Test career-growth messaging vs total-compensation messaging for {role}. "
                f"Senior professionals often value growth narratives over salary numbers."
            ),
            "variant_a": "Lead with career growth and title progression",
            "variant_b": "Lead with total compensation and equity",
        },
        "grey_collar": {
            "variable": "Ad Copy: Sign-On Bonus vs Schedule Flexibility",
            "hypothesis": (
                f"Test sign-on bonus offers vs flexible scheduling for {role}. "
                f"Clinical roles are highly sensitive to both financial incentives and work-life balance."
            ),
            "variant_a": "Highlight sign-on bonus amount",
            "variant_b": "Highlight shift flexibility and self-scheduling",
        },
        "pink_collar": {
            "variable": "Ad Copy: Culture vs Stability",
            "hypothesis": (
                f"Test workplace-culture messaging vs job-stability messaging for {role}. "
                f"Service and admin candidates value both team environment and predictable schedules."
            ),
            "variant_a": "Lead with team culture and work environment",
            "variant_b": "Lead with job stability and benefits",
        },
    }
    msg_test = messaging_tests.get(collar_type, messaging_tests["white_collar"])
    tests.append(
        {
            "test_id": "messaging_variant",
            "variable": msg_test["variable"],
            "hypothesis": msg_test["hypothesis"],
            "channels": (
                [top_2[0].get("channel_name", "Primary Channel")]
                if top_2
                else ["Primary Channel"]
            ),
            "duration_days": 21,
            "min_sample_size": 300,
            "priority": "high",
            "expected_lift_pct": 20,
            "variant_a": msg_test.get("variant_a") or "",
            "variant_b": msg_test.get("variant_b") or "",
        }
    )

    # 3. Application flow test (if budget supports it)
    if budget >= 10_000:
        tests.append(
            {
                "test_id": "apply_flow",
                "variable": "Application Length",
                "hypothesis": (
                    f"Test short-form (name + phone + resume optional) vs standard application "
                    f"for {role}. Shorter forms typically increase apply rates 20-40% for "
                    f"{collar_type.replace('_', ' ')} roles but may reduce quality."
                ),
                "channels": ["All channels"],
                "duration_days": 14,
                "min_sample_size": 150,
                "priority": "medium",
                "expected_lift_pct": 25,
                "variant_a": "Short form (3 fields, no resume)",
                "variant_b": "Standard form (5+ fields, resume required)",
            }
        )

    # 4. Bid strategy test (for programmatic channels)
    programmatic_channels = [
        c
        for c in channel_recommendations
        if c.get("channel_key") in ("programmatic", "google_search")
    ]
    if programmatic_channels and budget >= 5_000:
        tests.append(
            {
                "test_id": "bid_strategy",
                "variable": "Bidding Strategy",
                "hypothesis": (
                    f"Test CPC-optimized bidding vs CPA-optimized bidding on programmatic channels. "
                    f"CPA bidding often delivers better ROI for established campaigns with conversion data."
                ),
                "channels": [
                    (c.get("channel_name") or "") for c in programmatic_channels
                ],
                "duration_days": 21,
                "min_sample_size": 500,
                "priority": "medium",
                "expected_lift_pct": 12,
                "variant_a": "CPC-optimized (maximize clicks)",
                "variant_b": "CPA-optimized (maximize applications)",
            }
        )

    # 5. Time-of-day targeting test (collar-specific)
    peak_hours = {
        "blue_collar": (
            "early morning (6-8 AM) and evening (5-9 PM)",
            "midday (11 AM-2 PM)",
        ),
        "white_collar": (
            "morning commute (7-9 AM) and late evening (8-10 PM)",
            "midday (12-1 PM)",
        ),
        "grey_collar": (
            "shift change windows (6-7 AM, 2-3 PM, 10-11 PM)",
            "standard business hours",
        ),
        "pink_collar": ("morning (8-10 AM) and evening (6-8 PM)", "midday"),
    }
    peak_a, peak_b = peak_hours.get(collar_type, peak_hours["white_collar"])
    tests.append(
        {
            "test_id": "dayparting",
            "variable": "Ad Scheduling (Dayparting)",
            "hypothesis": (
                f"Test ad delivery during peak engagement windows ({peak_a}) vs "
                f"even distribution throughout the day. Dayparting can reduce wasted spend by 10-20%."
            ),
            "channels": (
                [top_2[0].get("channel_name", "Primary Channel")]
                if top_2
                else ["Primary Channel"]
            ),
            "duration_days": 14,
            "min_sample_size": 250,
            "priority": "low",
            "expected_lift_pct": 10,
            "variant_a": f"Peak hours only: {peak_a}",
            "variant_b": "Even distribution (24-hour)",
        }
    )

    return tests


# ═══════════════════════════════════════════════════════════════════════════════
# 7. MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════


def generate_quick_plan(
    role: str,
    location: str,
    budget: Any,
    industry: str = "",
) -> Dict[str, Any]:
    """Generate an instant channel recommendation plan for a single role.

    Main function called by the /api/quick-plan endpoint. Returns a complete
    structured result for on-screen rendering.

    Args:
        role: Job title (e.g., "Software Engineer", "Nurse", "Truck Driver")
        location: Location string (e.g., "San Francisco, CA", "London", "Remote")
        budget: Budget value (string or number, parsed via shared_utils)
        industry: Optional industry key for context

    Returns:
        {
            "success": bool,
            "role_analysis": {...},
            "location_analysis": {...},
            "channel_recommendations": [...],
            "budget_assessment": {...},
            "seasonal_advice": {...},
            "total_projected": {...},
            "generated_at": str,
        }
    """
    try:
        # ── Input validation ──
        if not role or not role.strip():
            return {"success": False, "error": "Role is required."}
        if not location or not location.strip():
            return {"success": False, "error": "Location is required."}

        role = role.strip()
        location = location.strip()
        budget_val = parse_budget(budget, default=50_000.0)

        if budget_val <= 0:
            budget_val = 50_000.0

        # ── Role analysis ──
        role_analysis = get_role_insights(role, industry)
        collar_type = role_analysis.get("collar_type", "white_collar")

        # ── Location analysis ──
        location_analysis = get_location_insights(location)
        cpc_multiplier = location_analysis.get("cpc_multiplier", 1.0)
        coli = location_analysis.get("coli", 100)

        # Adjust salary range for location COLI
        if coli != 100 and _HAS_RESEARCH:
            try:
                adjusted_salary = _research.get_role_salary_range(
                    role, location_coli=coli
                )
                role_analysis["salary_range"] = adjusted_salary
                role_analysis["salary_location_adjusted"] = True
            except Exception:
                role_analysis["salary_location_adjusted"] = False
        else:
            role_analysis["salary_location_adjusted"] = False

        # ── Channel scoring ──
        channel_recommendations = score_channels_for_context(
            role=role,
            location=location,
            industry=industry,
            collar_type=collar_type,
            budget=budget_val,
            cpc_multiplier=cpc_multiplier,
        )

        # ── Budget assessment ──
        budget_assessment = _assess_budget(
            budget=budget_val,
            collar_type=collar_type,
            industry=industry or "general_entry_level",
            channel_recommendations=channel_recommendations,
        )

        # ── Seasonal advice ──
        seasonal_advice = _get_seasonal_advice(collar_type)

        # ── A/B Testing recommendations ──
        ab_testing = _generate_ab_testing_recommendations(
            collar_type=collar_type,
            channel_recommendations=channel_recommendations,
            role=role,
            location=location or "National",
            budget=budget_val,
        )

        # ── Aggregate projections ──
        total_clicks = sum(
            c.get("projected_clicks") or 0 for c in channel_recommendations
        )
        total_applies = sum(
            c.get("projected_applies") or 0 for c in channel_recommendations
        )
        total_hires = sum(
            c.get("projected_hires") or 0 for c in channel_recommendations
        )
        blended_cpa = round(budget_val / total_applies, 2) if total_applies > 0 else 0
        blended_cph = round(budget_val / total_hires, 2) if total_hires > 0 else 0

        total_projected = {
            "total_budget": round(budget_val, 2),
            "total_clicks": total_clicks,
            "total_applies": total_applies,
            "total_hires": total_hires,
            "blended_cpa": blended_cpa,
            "blended_cph": blended_cph,
        }

        return {
            "success": True,
            "role_analysis": role_analysis,
            "location_analysis": location_analysis,
            "channel_recommendations": channel_recommendations,
            "budget_assessment": budget_assessment,
            "seasonal_advice": seasonal_advice,
            "ab_testing_recommendations": ab_testing,
            "total_projected": total_projected,
            "generated_at": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error("generate_quick_plan failed: %s", e, exc_info=True)
        return {
            "success": False,
            "error": "Plan generation failed. Please try again.",
        }


# ═══════════════════════════════════════════════════════════════════════════════
# PRIVATE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _is_remote_location(location: str) -> bool:
    """Check if the location indicates remote work."""
    if not location:
        return False
    lower = location.strip().lower()
    return (
        lower
        in (
            "remote",
            "remote work",
            "work from home",
            "wfh",
            "hybrid",
            "anywhere",
            "global",
            "distributed",
        )
        or "remote" in lower.split(",")[0].strip().split()
    )


def _location_matches(query: str, metro: str) -> bool:
    """Fuzzy match a location query against a metro key."""
    q = query.lower().strip()
    m = metro.lower().strip()
    # Exact match
    if q == m:
        return True
    # City name match
    q_city = q.split(",")[0].strip()
    m_city = m.split(",")[0].strip()
    if q_city == m_city:
        return True
    # Substring
    if q_city in m_city or m_city in q_city:
        return True
    return False


def _format_population(pop: Any) -> str:
    """Format population number as human-readable string."""
    try:
        p = int(pop) if pop else 0
    except (TypeError, ValueError):
        return str(pop) if pop else "N/A"
    if p <= 0:
        return "N/A"
    if p >= 1_000_000:
        return f"{p / 1_000_000:.1f}M"
    if p >= 1_000:
        return f"{p / 1_000:.0f}K"
    return str(p)


def _get_collar_allocation(collar_type: str) -> Dict[str, float]:
    """Get channel allocation percentages for a collar type.

    Maps collar_intelligence COLLAR_STRATEGY channel_mix keys to our
    channel categories.
    """
    if _HAS_COLLAR_INTEL and hasattr(_collar_intel, "COLLAR_STRATEGY"):
        strategy = _collar_intel.COLLAR_STRATEGY.get(collar_type, {})
        channel_mix = strategy.get("channel_mix", {})
        if channel_mix:
            return channel_mix

    # Fallback allocations by collar type
    fallbacks = {
        "blue_collar": {
            "programmatic": 0.35,
            "global_job_boards": 0.30,
            "social_media": 0.20,
            "regional_local": 0.10,
            "niche_boards": 0.05,
        },
        "white_collar": {
            "linkedin": 0.30,
            "programmatic": 0.15,
            "niche_boards": 0.20,
            "employer_branding": 0.15,
            "social_media": 0.10,
            "search": 0.05,
            "global_job_boards": 0.05,
        },
        "grey_collar": {
            "niche_boards": 0.35,
            "programmatic": 0.25,
            "global_job_boards": 0.20,
            "social_media": 0.15,
            "regional_local": 0.05,
        },
        "pink_collar": {
            "global_job_boards": 0.30,
            "programmatic": 0.25,
            "social_media": 0.25,
            "regional_local": 0.15,
            "niche_boards": 0.05,
        },
    }
    return fallbacks.get(collar_type, fallbacks["white_collar"])
