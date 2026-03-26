"""
collar_intelligence.py -- Blue Collar vs White Collar Intelligence Engine

First-class collar-type classification and strategy differentiation for
recruitment advertising. Makes collar type a routing parameter at every
level: orchestrator queries, budget allocation, channel selection,
PPT output, and chat responses.

Classification uses:
    - SOC major groups (47-53 = blue collar, 11-29 = white collar)
    - O*NET Job Zones (1-2 = blue/entry, 3 = grey/skilled, 4-5 = white/professional)
    - standardizer.py role tiers (entry, skilled, professional, executive)
    - Keyword pattern matching for unclassified roles

Supports 4 collar types:
    - blue_collar:  Manual labor, trades, hourly, shift-based
    - white_collar: Professional, office-based, salaried
    - grey_collar:  Licensed/clinical but shift-based (nurses, technicians)
    - pink_collar:  Administrative, care, service (historically female-dominated)

Thread-safe, stdlib only (except optional standardizer import).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Optional imports ──
try:
    from standardizer import (
        get_role_tier as _std_get_role_tier,
        get_soc_code as _std_get_soc_code,
    )

    _HAS_STANDARDIZER = True
except ImportError:
    _HAS_STANDARDIZER = False


# ═══════════════════════════════════════════════════════════════════════════════
# 1. SOC-BASED COLLAR CLASSIFICATION
#    SOC major groups: https://www.bls.gov/soc/2018/major_groups.htm
# ═══════════════════════════════════════════════════════════════════════════════

# SOC major group -> collar type mapping
SOC_COLLAR_MAP: Dict[str, str] = {
    # White collar (professional, management, office)
    "11": "white_collar",  # Management
    "13": "white_collar",  # Business and Financial Operations
    "15": "white_collar",  # Computer and Mathematical
    "17": "white_collar",  # Architecture and Engineering
    "19": "white_collar",  # Life, Physical, and Social Science
    "23": "white_collar",  # Legal
    "25": "white_collar",  # Educational Instruction and Library
    "27": "white_collar",  # Arts, Design, Entertainment, Sports, Media
    # Grey collar (licensed/clinical, skilled hybrid)
    "21": "grey_collar",  # Community and Social Service
    "29": "grey_collar",  # Healthcare Practitioners and Technical
    # Pink collar (admin, care, service)
    "31": "pink_collar",  # Healthcare Support
    "39": "pink_collar",  # Personal Care and Service
    "43": "pink_collar",  # Office and Administrative Support
    # Blue collar (manual, trades, operations)
    "33": "blue_collar",  # Protective Service
    "35": "blue_collar",  # Food Preparation and Serving
    "37": "blue_collar",  # Building and Grounds Cleaning/Maintenance
    "41": "blue_collar",  # Sales (retail/field)
    "45": "blue_collar",  # Farming, Fishing, and Forestry
    "47": "blue_collar",  # Construction and Extraction
    "49": "blue_collar",  # Installation, Maintenance, and Repair
    "51": "blue_collar",  # Production
    "53": "blue_collar",  # Transportation and Material Moving
    "55": "blue_collar",  # Military Specific
}


# ═══════════════════════════════════════════════════════════════════════════════
# 2. KEYWORD-BASED CLASSIFICATION (fallback when SOC unavailable)
# ═══════════════════════════════════════════════════════════════════════════════

_BLUE_COLLAR_KEYWORDS = {
    "driver",
    "cdl",
    "trucker",
    "delivery",
    "courier",
    "warehouse",
    "forklift",
    "picker",
    "packer",
    "stocker",
    "dock",
    "loader",
    "mover",
    "shipping",
    "receiving",
    "material handler",
    "laborer",
    "construction",
    "carpenter",
    "electrician",
    "plumber",
    "hvac",
    "welder",
    "machinist",
    "painter",
    "roofer",
    "ironworker",
    "crane",
    "heavy equipment",
    "concrete",
    "mechanic",
    "technician",
    "installer",
    "maintenance",
    "janitor",
    "custodian",
    "landscaper",
    "pest control",
    "factory",
    "assembly",
    "production",
    "machine operator",
    "line worker",
    "manufacturing",
    "cook",
    "chef",
    "dishwasher",
    "server",
    "bartender",
    "barista",
    "housekeeper",
    "cleaner",
    "security guard",
    "farmer",
    "fisherman",
    "mining",
    "oil rig",
    "deckhand",
    "marine",
    "seaman",
    "boatswain",
    "rigger",
    "scaffolder",
    "bricklayer",
    "glazier",
    "tiler",
}

_WHITE_COLLAR_KEYWORDS = {
    "engineer",
    "developer",
    "programmer",
    "architect",
    "scientist",
    "analyst",
    "manager",
    "director",
    "vp",
    "vice president",
    "ceo",
    "cto",
    "cfo",
    "coo",
    "executive",
    "consultant",
    "attorney",
    "lawyer",
    "counsel",
    "accountant",
    "auditor",
    "controller",
    "actuary",
    "underwriter",
    "professor",
    "researcher",
    "physician",
    "surgeon",
    "specialist",
    "psychologist",
    "pharmacist",
    "product manager",
    "project manager",
    "program manager",
    "data scientist",
    "designer",
    "strategist",
    "planner",
    "broker",
    "trader",
    "portfolio",
    "compliance",
    "regulatory",
}

_GREY_COLLAR_KEYWORDS = {
    "nurse",
    "rn",
    "lpn",
    "cna",
    "medical assistant",
    "dental",
    "phlebotomist",
    "emt",
    "paramedic",
    "therapist",
    "counselor",
    "social worker",
    "respiratory",
    "radiology",
    "surgical tech",
    "pharmacy tech",
    "lab technician",
    "medical technologist",
    "occupational therapist",
    "physical therapist",
    "speech pathologist",
    "dietitian",
    "optician",
    "audiologist",
}

_PINK_COLLAR_KEYWORDS = {
    "receptionist",
    "secretary",
    "administrative",
    "admin assistant",
    "office manager",
    "clerk",
    "data entry",
    "bookkeeper",
    "customer service",
    "call center",
    "support specialist",
    "caregiver",
    "home health aide",
    "childcare",
    "nanny",
    "teacher aide",
    "teaching assistant",
    "library",
}


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CLASSIFICATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════


def classify_collar(
    role: str,
    industry: str = "",
    soc_code: str = "",
) -> Dict[str, Any]:
    """Classify a role into collar type with confidence and strategy metadata.

    Classification cascade:
        1. SOC code major group (if provided or resolved via standardizer)
        2. Standardizer role tier mapping
        3. Keyword pattern matching
        4. Industry-based fallback

    Args:
        role: Job title or role name
        industry: Optional industry context
        soc_code: Optional SOC code (e.g., "53-3032")

    Returns:
        {
            "collar_type": "blue_collar" | "white_collar" | "grey_collar" | "pink_collar",
            "confidence": 0.0-1.0,
            "sub_type": more specific classification,
            "method": how it was classified,
            "indicators": list of signals that led to classification,
            "channel_strategy": "volume" | "targeted" | "premium",
        }
    """
    if not role:
        return _default_result("white_collar", 0.20, "no_role_provided")

    role_lower = role.strip().lower()
    indicators = []

    # Method 1: SOC code (highest confidence)
    resolved_soc = soc_code
    if not resolved_soc and _HAS_STANDARDIZER:
        try:
            resolved_soc = _std_get_soc_code(role)
        except Exception:
            pass

    if resolved_soc:
        major_group = (
            resolved_soc.split("-")[0] if "-" in resolved_soc else resolved_soc[:2]
        )
        collar = SOC_COLLAR_MAP.get(major_group)
        if collar:
            indicators.append(f"SOC major group {major_group}")
            sub_type = _get_sub_type(role_lower, collar)
            return _build_result(collar, 0.92, "soc_code", sub_type, indicators)

    # Method 2: Standardizer role tier
    if _HAS_STANDARDIZER:
        try:
            tier = _std_get_role_tier(role)
            if tier:
                indicators.append(f"standardizer tier: {tier}")
                tier_collar = _TIER_TO_COLLAR.get(tier.lower())
                if tier_collar:
                    sub_type = _get_sub_type(role_lower, tier_collar)
                    return _build_result(
                        tier_collar, 0.85, "standardizer_tier", sub_type, indicators
                    )
        except Exception:
            pass

    # Method 3: Keyword matching
    collar, kw_confidence, matched_keywords = _keyword_classify(role_lower)
    if collar and kw_confidence >= 0.60:
        indicators.extend([f"keyword: {kw}" for kw in matched_keywords[:3]])
        sub_type = _get_sub_type(role_lower, collar)
        return _build_result(
            collar, kw_confidence, "keyword_match", sub_type, indicators
        )

    # Method 4: Industry-based fallback
    if industry:
        ind_lower = industry.strip().lower().replace(" ", "_").replace("-", "_")
        ind_collar = _INDUSTRY_DEFAULT_COLLAR.get(ind_lower)
        if ind_collar:
            indicators.append(f"industry default: {industry}")
            sub_type = _get_sub_type(role_lower, ind_collar)
            return _build_result(
                ind_collar, 0.45, "industry_fallback", sub_type, indicators
            )

    # Ultimate fallback
    return _default_result("white_collar", 0.25, "no_match")


# Tier -> collar mapping
_TIER_TO_COLLAR: Dict[str, str] = {
    "executive": "white_collar",
    "professional": "white_collar",
    "skilled": "blue_collar",  # skilled trades
    "entry": "blue_collar",  # entry-level hourly
    "clinical": "grey_collar",
}

# Industry default collar type (when no role info available)
_INDUSTRY_DEFAULT_COLLAR: Dict[str, str] = {
    "healthcare_medical": "grey_collar",
    "tech_engineering": "white_collar",
    "finance_banking": "white_collar",
    "retail_consumer": "blue_collar",
    "blue_collar_trades": "blue_collar",
    "general_entry_level": "blue_collar",
    "logistics_supply_chain": "blue_collar",
    "hospitality_travel": "blue_collar",
    "construction_real_estate": "blue_collar",
    "pharma_biotech": "white_collar",
    "aerospace_defense": "white_collar",
    "legal_services": "white_collar",
    "mental_health": "grey_collar",
    "insurance": "white_collar",
    "telecommunications": "white_collar",
    "automotive": "blue_collar",
    "food_beverage": "blue_collar",
    "energy_utilities": "blue_collar",
    "education": "white_collar",
    "media_entertainment": "white_collar",
    "maritime_marine": "blue_collar",
    "military_recruitment": "blue_collar",
}


def _keyword_classify(role_lower: str) -> Tuple[Optional[str], float, List[str]]:
    """Classify via keyword matching. Returns (collar, confidence, matched_keywords)."""
    matches: Dict[str, List[str]] = {
        "blue_collar": [],
        "white_collar": [],
        "grey_collar": [],
        "pink_collar": [],
    }

    for kw in _BLUE_COLLAR_KEYWORDS:
        if kw in role_lower:
            matches["blue_collar"].append(kw)
    for kw in _WHITE_COLLAR_KEYWORDS:
        if kw in role_lower:
            matches["white_collar"].append(kw)
    for kw in _GREY_COLLAR_KEYWORDS:
        if kw in role_lower:
            matches["grey_collar"].append(kw)
    for kw in _PINK_COLLAR_KEYWORDS:
        if kw in role_lower:
            matches["pink_collar"].append(kw)

    # Pick the collar with most keyword matches
    best_collar = max(matches, key=lambda c: len(matches[c]))
    count = len(matches[best_collar])

    if count == 0:
        return None, 0.0, []

    # Confidence scales with match count
    if count >= 3:
        conf = 0.88
    elif count >= 2:
        conf = 0.78
    else:
        conf = 0.65

    return best_collar, conf, matches[best_collar]


def _get_sub_type(role_lower: str, collar: str) -> str:
    """Determine more specific sub-type within a collar classification."""
    if collar == "blue_collar":
        if any(
            kw in role_lower
            for kw in ("driver", "cdl", "trucker", "delivery", "courier")
        ):
            return "transportation"
        if any(
            kw in role_lower
            for kw in ("warehouse", "forklift", "picker", "dock", "shipping")
        ):
            return "warehouse_logistics"
        if any(
            kw in role_lower
            for kw in ("construction", "carpenter", "electrician", "plumber", "welder")
        ):
            return "skilled_trades"
        if any(
            kw in role_lower
            for kw in ("factory", "assembly", "production", "machine", "manufacturing")
        ):
            return "manufacturing"
        if any(
            kw in role_lower
            for kw in ("cook", "chef", "server", "bartender", "barista")
        ):
            return "food_service"
        if any(kw in role_lower for kw in ("security", "guard", "loss prevention")):
            return "protective_service"
        if any(
            kw in role_lower
            for kw in ("mechanic", "technician", "installer", "maintenance")
        ):
            return "maintenance_repair"
        return "general_labor"

    if collar == "white_collar":
        if any(
            kw in role_lower
            for kw in ("engineer", "developer", "programmer", "architect", "devops")
        ):
            return "technology"
        if any(
            kw in role_lower
            for kw in ("executive", "ceo", "cto", "cfo", "vp", "director")
        ):
            return "executive"
        if any(kw in role_lower for kw in ("analyst", "consultant", "strategist")):
            return "business_professional"
        if any(
            kw in role_lower for kw in ("attorney", "lawyer", "counsel", "paralegal")
        ):
            return "legal"
        if any(
            kw in role_lower
            for kw in ("accountant", "auditor", "controller", "actuary")
        ):
            return "finance"
        if any(kw in role_lower for kw in ("manager", "project", "product", "program")):
            return "management"
        if any(kw in role_lower for kw in ("professor", "teacher", "researcher")):
            return "education"
        return "general_professional"

    if collar == "grey_collar":
        if any(kw in role_lower for kw in ("nurse", "rn", "lpn")):
            return "nursing"
        if any(kw in role_lower for kw in ("therapist", "counselor", "psychologist")):
            return "behavioral_health"
        if any(kw in role_lower for kw in ("emt", "paramedic")):
            return "emergency_medical"
        return "clinical_support"

    if collar == "pink_collar":
        if any(
            kw in role_lower for kw in ("receptionist", "secretary", "admin", "clerk")
        ):
            return "administrative"
        if any(
            kw in role_lower for kw in ("customer service", "call center", "support")
        ):
            return "customer_service"
        if any(
            kw in role_lower
            for kw in ("caregiver", "home health", "childcare", "nanny")
        ):
            return "care_service"
        return "general_service"

    return "unclassified"


def _build_result(
    collar: str,
    confidence: float,
    method: str,
    sub_type: str,
    indicators: List[str],
) -> Dict[str, Any]:
    """Build a standardized classification result."""
    strategy = _COLLAR_TO_STRATEGY.get(collar, "targeted")
    return {
        "collar_type": collar,
        "confidence": round(confidence, 2),
        "sub_type": sub_type,
        "method": method,
        "indicators": indicators,
        "channel_strategy": strategy,
    }


def _default_result(collar: str, confidence: float, method: str) -> Dict[str, Any]:
    return _build_result(collar, confidence, method, "unclassified", [])


_COLLAR_TO_STRATEGY: Dict[str, str] = {
    "blue_collar": "volume",  # High volume, low cost, mobile-first
    "white_collar": "targeted",  # Targeted, professional platforms, quality over quantity
    "grey_collar": "targeted",  # Niche boards, clinical networks
    "pink_collar": "volume",  # Volume-oriented but different channels than blue collar
}


# ═══════════════════════════════════════════════════════════════════════════════
# 4. COLLAR-SPECIFIC STRATEGY RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════════════════════════

COLLAR_STRATEGY: Dict[str, Dict[str, Any]] = {
    "blue_collar": {
        "channel_mix": {
            "programmatic": 0.40,
            "global_job_boards": 0.25,
            "social_media": 0.20,
            "regional_local": 0.15,
        },
        "preferred_platforms": [
            "Indeed",
            "Facebook",
            "ZipRecruiter",
            "Google Search Ads",
            "Snagajob",
            "Craigslist",
            "Talroo",
            "Jobcase",
        ],
        "messaging_tone": "Direct, benefits-focused, mobile-first. Lead with pay rate, schedule, and location.",
        "ad_format_priority": [
            "mobile_display",
            "social_feed",
            "sms",
            "push_notification",
        ],
        "application_complexity": "Minimal: name, phone, 1-click apply. No resume required.",
        "time_to_fill_benchmark_days": 14,
        "avg_cpa_range": [8, 25],
        "avg_cpc_range": [0.25, 1.20],
        "avg_cph_range": [2500, 5500],
        "peak_job_seeking_hours": ["6-8 AM", "5-9 PM", "Weekends"],
        "top_retention_factors": [
            "pay",
            "schedule_flexibility",
            "proximity",
            "benefits",
        ],
        "mobile_apply_pct": 0.78,
        "avg_apply_rate": 0.065,
        "key_insight": "Blue collar candidates search on mobile (78%), prefer short applications (<2 min), and prioritize pay transparency. Programmatic and Indeed dominate.",
    },
    "white_collar": {
        "channel_mix": {
            "linkedin": 0.30,
            "programmatic": 0.20,
            "niche_boards": 0.20,
            "employer_branding": 0.15,
            "social_media": 0.10,
            "search": 0.05,
        },
        "preferred_platforms": [
            "LinkedIn",
            "Indeed",
            "Glassdoor",
            "Dice",
            "BuiltIn",
            "AngelList",
        ],
        "messaging_tone": "Career-growth focused. Highlight culture, remote/hybrid options, DEI, and total compensation.",
        "ad_format_priority": [
            "linkedin_inmail",
            "search_ads",
            "display_retargeting",
            "email_campaigns",
        ],
        "application_complexity": "Standard: resume upload, optional cover letter. 5-10 min process acceptable.",
        "time_to_fill_benchmark_days": 38,
        "avg_cpa_range": [20, 75],
        "avg_cpc_range": [1.50, 5.00],
        "avg_cph_range": [6000, 22000],
        "peak_job_seeking_hours": ["7-9 AM", "12-1 PM", "8-10 PM"],
        "top_retention_factors": [
            "career_growth",
            "compensation",
            "remote_flexibility",
            "culture",
            "learning",
        ],
        "mobile_apply_pct": 0.45,
        "avg_apply_rate": 0.042,
        "key_insight": "White collar candidates research companies extensively (Glassdoor, LinkedIn). Employer brand matters more than CPC. LinkedIn InMail has 3x response rate vs job board applications.",
    },
    "grey_collar": {
        "channel_mix": {
            "niche_boards": 0.35,
            "programmatic": 0.25,
            "global_job_boards": 0.20,
            "social_media": 0.15,
            "regional_local": 0.05,
        },
        "preferred_platforms": [
            "Indeed",
            "Vivian Health",
            "NurseFly",
            "Health eCareers",
            "LinkedIn",
        ],
        "messaging_tone": "Credential-aware. Highlight licensure support, shift flexibility, sign-on bonuses, and continuing education.",
        "ad_format_priority": [
            "niche_job_boards",
            "social_feed",
            "search_ads",
            "email",
        ],
        "application_complexity": "Credential-focused: license verification, certifications required. 5-15 min process.",
        "time_to_fill_benchmark_days": 28,
        "avg_cpa_range": [15, 50],
        "avg_cpc_range": [0.80, 3.00],
        "avg_cph_range": [5000, 15000],
        "peak_job_seeking_hours": ["6-8 AM", "7-10 PM", "Weekends"],
        "top_retention_factors": [
            "schedule_flexibility",
            "pay",
            "sign_on_bonus",
            "patient_ratio",
            "burnout_support",
        ],
        "mobile_apply_pct": 0.62,
        "avg_apply_rate": 0.048,
        "key_insight": "Grey collar (nurses, therapists, techs) are in critical shortage. Sign-on bonuses ($5K-$20K) are standard. Niche boards (Vivian, NurseFly) convert 2x better than general boards.",
    },
    "pink_collar": {
        "channel_mix": {
            "global_job_boards": 0.30,
            "programmatic": 0.30,
            "social_media": 0.25,
            "regional_local": 0.15,
        },
        "preferred_platforms": [
            "Indeed",
            "Facebook",
            "Snagajob",
            "Care.com",
            "LinkedIn",
        ],
        "messaging_tone": "People-focused. Highlight work environment, team culture, growth opportunities, and benefits.",
        "ad_format_priority": ["social_feed", "mobile_display", "search_ads", "email"],
        "application_complexity": "Simple: resume optional, quick apply preferred. 3-5 min process.",
        "time_to_fill_benchmark_days": 18,
        "avg_cpa_range": [10, 30],
        "avg_cpc_range": [0.50, 1.80],
        "avg_cph_range": [3000, 7000],
        "peak_job_seeking_hours": ["7-9 AM", "12-2 PM", "6-9 PM"],
        "top_retention_factors": [
            "work_environment",
            "pay",
            "benefits",
            "schedule",
            "growth",
        ],
        "mobile_apply_pct": 0.68,
        "avg_apply_rate": 0.055,
        "key_insight": "Pink collar roles (admin, customer service, care) respond well to Facebook ads and local targeting. Culture messaging outperforms compensation messaging.",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 5. COLLAR HIRING PATTERNS BY INDUSTRY
# ═══════════════════════════════════════════════════════════════════════════════

COLLAR_HIRING_PATTERNS: Dict[str, Dict[str, Any]] = {
    "healthcare_medical": {
        "blue_collar_pct": 15,
        "white_collar_pct": 30,
        "grey_collar_pct": 45,
        "pink_collar_pct": 10,
        "primary_collar": "grey_collar",
        "blue_collar_roles": ["custodian", "food service", "maintenance"],
        "white_collar_roles": ["physician", "administrator", "analyst"],
        "grey_collar_roles": ["nurse", "therapist", "medical assistant", "EMT"],
        "pink_collar_roles": ["receptionist", "medical coder", "scheduler"],
        "blue_collar_cpa_range": [10, 22],
        "white_collar_cpa_range": [35, 85],
        "grey_collar_cpa_range": [18, 45],
        "blue_collar_time_to_fill": 14,
        "white_collar_time_to_fill": 45,
        "grey_collar_time_to_fill": 28,
    },
    "tech_engineering": {
        "blue_collar_pct": 5,
        "white_collar_pct": 85,
        "grey_collar_pct": 5,
        "pink_collar_pct": 5,
        "primary_collar": "white_collar",
        "blue_collar_roles": ["warehouse tech", "cable installer", "field tech"],
        "white_collar_roles": [
            "software engineer",
            "data scientist",
            "product manager",
            "designer",
        ],
        "grey_collar_roles": ["network technician", "hardware engineer"],
        "pink_collar_roles": ["admin assistant", "office manager"],
        "blue_collar_cpa_range": [12, 28],
        "white_collar_cpa_range": [25, 75],
        "grey_collar_cpa_range": [20, 45],
        "blue_collar_time_to_fill": 14,
        "white_collar_time_to_fill": 42,
        "grey_collar_time_to_fill": 28,
    },
    "finance_banking": {
        "blue_collar_pct": 5,
        "white_collar_pct": 80,
        "grey_collar_pct": 0,
        "pink_collar_pct": 15,
        "primary_collar": "white_collar",
        "blue_collar_roles": ["security guard", "courier", "mail room"],
        "white_collar_roles": ["analyst", "trader", "portfolio manager", "actuary"],
        "pink_collar_roles": ["teller", "customer service", "admin assistant"],
        "blue_collar_cpa_range": [10, 25],
        "white_collar_cpa_range": [30, 65],
        "pink_collar_cpa_range": [15, 35],
        "blue_collar_time_to_fill": 12,
        "white_collar_time_to_fill": 48,
        "pink_collar_time_to_fill": 18,
    },
    "retail_consumer": {
        "blue_collar_pct": 60,
        "white_collar_pct": 15,
        "grey_collar_pct": 0,
        "pink_collar_pct": 25,
        "primary_collar": "blue_collar",
        "blue_collar_roles": ["store associate", "stocker", "warehouse", "delivery"],
        "white_collar_roles": ["buyer", "marketing manager", "district manager"],
        "pink_collar_roles": ["cashier", "customer service", "receptionist"],
        "blue_collar_cpa_range": [6, 18],
        "white_collar_cpa_range": [25, 55],
        "pink_collar_cpa_range": [8, 22],
        "blue_collar_time_to_fill": 10,
        "white_collar_time_to_fill": 35,
        "pink_collar_time_to_fill": 12,
    },
    "blue_collar_trades": {
        "blue_collar_pct": 85,
        "white_collar_pct": 10,
        "grey_collar_pct": 0,
        "pink_collar_pct": 5,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "electrician",
            "plumber",
            "welder",
            "carpenter",
            "HVAC tech",
        ],
        "white_collar_roles": ["project manager", "estimator", "safety officer"],
        "pink_collar_roles": ["admin assistant", "dispatcher"],
        "blue_collar_cpa_range": [12, 35],
        "white_collar_cpa_range": [25, 55],
        "blue_collar_time_to_fill": 18,
        "white_collar_time_to_fill": 30,
    },
    "logistics_supply_chain": {
        "blue_collar_pct": 70,
        "white_collar_pct": 15,
        "grey_collar_pct": 0,
        "pink_collar_pct": 15,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "truck driver",
            "warehouse worker",
            "forklift operator",
            "dock worker",
        ],
        "white_collar_roles": [
            "logistics manager",
            "supply chain analyst",
            "operations director",
        ],
        "pink_collar_roles": ["dispatcher", "customer service", "data entry"],
        "blue_collar_cpa_range": [10, 28],
        "white_collar_cpa_range": [25, 52],
        "pink_collar_cpa_range": [12, 28],
        "blue_collar_time_to_fill": 12,
        "white_collar_time_to_fill": 35,
        "pink_collar_time_to_fill": 14,
    },
    "hospitality_travel": {
        "blue_collar_pct": 65,
        "white_collar_pct": 10,
        "grey_collar_pct": 0,
        "pink_collar_pct": 25,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "cook",
            "server",
            "bartender",
            "housekeeper",
            "dishwasher",
        ],
        "white_collar_roles": ["hotel manager", "revenue manager", "marketing"],
        "pink_collar_roles": ["front desk", "concierge", "reservations agent"],
        "blue_collar_cpa_range": [6, 20],
        "white_collar_cpa_range": [22, 48],
        "pink_collar_cpa_range": [8, 22],
        "blue_collar_time_to_fill": 8,
        "white_collar_time_to_fill": 30,
        "pink_collar_time_to_fill": 10,
    },
    "construction_real_estate": {
        "blue_collar_pct": 75,
        "white_collar_pct": 15,
        "grey_collar_pct": 0,
        "pink_collar_pct": 10,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "construction laborer",
            "heavy equipment operator",
            "scaffold erector",
        ],
        "white_collar_roles": ["architect", "civil engineer", "project manager"],
        "pink_collar_roles": ["admin", "coordinator", "permitting clerk"],
        "blue_collar_cpa_range": [12, 30],
        "white_collar_cpa_range": [28, 60],
        "blue_collar_time_to_fill": 14,
        "white_collar_time_to_fill": 38,
    },
    "pharma_biotech": {
        "blue_collar_pct": 15,
        "white_collar_pct": 65,
        "grey_collar_pct": 15,
        "pink_collar_pct": 5,
        "primary_collar": "white_collar",
        "blue_collar_roles": ["lab assistant", "packaging operator", "warehouse"],
        "white_collar_roles": [
            "research scientist",
            "clinical researcher",
            "regulatory affairs",
        ],
        "grey_collar_roles": [
            "lab technician",
            "quality control analyst",
            "pharmacy tech",
        ],
        "blue_collar_cpa_range": [12, 28],
        "white_collar_cpa_range": [40, 110],
        "grey_collar_cpa_range": [22, 55],
        "blue_collar_time_to_fill": 14,
        "white_collar_time_to_fill": 52,
        "grey_collar_time_to_fill": 30,
    },
    "aerospace_defense": {
        "blue_collar_pct": 40,
        "white_collar_pct": 50,
        "grey_collar_pct": 5,
        "pink_collar_pct": 5,
        "primary_collar": "white_collar",
        "blue_collar_roles": [
            "machinist",
            "assembly technician",
            "welder",
            "sheet metal worker",
        ],
        "white_collar_roles": [
            "aerospace engineer",
            "systems engineer",
            "program manager",
        ],
        "grey_collar_roles": ["avionics technician", "quality inspector"],
        "blue_collar_cpa_range": [15, 35],
        "white_collar_cpa_range": [35, 80],
        "blue_collar_time_to_fill": 18,
        "white_collar_time_to_fill": 55,
    },
    "energy_utilities": {
        "blue_collar_pct": 55,
        "white_collar_pct": 30,
        "grey_collar_pct": 10,
        "pink_collar_pct": 5,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "lineman",
            "pipeline worker",
            "plant operator",
            "field tech",
        ],
        "white_collar_roles": ["petroleum engineer", "geologist", "project manager"],
        "grey_collar_roles": ["environmental technician", "safety specialist"],
        "blue_collar_cpa_range": [14, 32],
        "white_collar_cpa_range": [30, 65],
        "blue_collar_time_to_fill": 18,
        "white_collar_time_to_fill": 42,
    },
    "education": {
        "blue_collar_pct": 15,
        "white_collar_pct": 60,
        "grey_collar_pct": 0,
        "pink_collar_pct": 25,
        "primary_collar": "white_collar",
        "blue_collar_roles": ["custodian", "groundskeeper", "cafeteria worker"],
        "white_collar_roles": ["teacher", "professor", "administrator", "counselor"],
        "pink_collar_roles": ["secretary", "library aide", "teaching assistant"],
        "blue_collar_cpa_range": [8, 20],
        "white_collar_cpa_range": [18, 45],
        "pink_collar_cpa_range": [10, 25],
        "blue_collar_time_to_fill": 10,
        "white_collar_time_to_fill": 32,
        "pink_collar_time_to_fill": 14,
    },
    "food_beverage": {
        "blue_collar_pct": 75,
        "white_collar_pct": 10,
        "grey_collar_pct": 0,
        "pink_collar_pct": 15,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "cook",
            "line cook",
            "dishwasher",
            "baker",
            "food production",
        ],
        "white_collar_roles": ["restaurant manager", "brand manager", "food scientist"],
        "pink_collar_roles": ["cashier", "host", "barista"],
        "blue_collar_cpa_range": [5, 18],
        "white_collar_cpa_range": [20, 45],
        "blue_collar_time_to_fill": 7,
        "white_collar_time_to_fill": 28,
    },
    "automotive": {
        "blue_collar_pct": 60,
        "white_collar_pct": 25,
        "grey_collar_pct": 10,
        "pink_collar_pct": 5,
        "primary_collar": "blue_collar",
        "blue_collar_roles": ["assembly worker", "auto mechanic", "painter", "welder"],
        "white_collar_roles": [
            "automotive engineer",
            "design engineer",
            "plant manager",
        ],
        "grey_collar_roles": ["quality inspector", "diagnostic technician"],
        "blue_collar_cpa_range": [10, 25],
        "white_collar_cpa_range": [25, 55],
        "blue_collar_time_to_fill": 14,
        "white_collar_time_to_fill": 38,
    },
    "maritime_marine": {
        "blue_collar_pct": 70,
        "white_collar_pct": 20,
        "grey_collar_pct": 5,
        "pink_collar_pct": 5,
        "primary_collar": "blue_collar",
        "blue_collar_roles": [
            "deckhand",
            "marine diesel mechanic",
            "rigger",
            "crane operator",
        ],
        "white_collar_roles": ["marine engineer", "naval architect", "port manager"],
        "grey_collar_roles": ["marine surveyor", "safety officer"],
        "blue_collar_cpa_range": [15, 35],
        "white_collar_cpa_range": [30, 60],
        "blue_collar_time_to_fill": 21,
        "white_collar_time_to_fill": 45,
    },
    "military_recruitment": {
        "blue_collar_pct": 50,
        "white_collar_pct": 30,
        "grey_collar_pct": 15,
        "pink_collar_pct": 5,
        "primary_collar": "blue_collar",
        "blue_collar_roles": ["infantry", "mechanic", "combat engineer", "logistics"],
        "white_collar_roles": ["officer", "intelligence analyst", "cyber operations"],
        "grey_collar_roles": ["medic", "corpsman", "medical officer"],
        "blue_collar_cpa_range": [25, 55],
        "white_collar_cpa_range": [40, 80],
        "blue_collar_time_to_fill": 30,
        "white_collar_time_to_fill": 60,
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# 6. BLENDED ALLOCATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════


def get_blended_allocation(
    roles: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute a weighted channel allocation blend based on collar mix.

    Args:
        roles: List of dicts, each with at minimum {"role": "...", "count": N}.
               Optional: "industry", "collar_type" (pre-classified).

    Returns:
        {
            "collar_breakdown": {"blue_collar": 0.60, "white_collar": 0.30, ...},
            "blended_channel_mix": {"programmatic": 0.32, "linkedin": 0.09, ...},
            "primary_collar": "blue_collar",
            "strategy_summary": "Volume-oriented with ...",
            "roles_classified": [...],
        }
    """
    if not roles:
        # Default to mixed
        return {
            "collar_breakdown": {
                "blue_collar": 0.25,
                "white_collar": 0.25,
                "grey_collar": 0.25,
                "pink_collar": 0.25,
            },
            "blended_channel_mix": COLLAR_STRATEGY["white_collar"]["channel_mix"],
            "primary_collar": "white_collar",
            "strategy_summary": "Balanced strategy (no role data provided).",
            "roles_classified": [],
        }

    # Classify each role and weight by count
    classified = []
    collar_counts: Dict[str, int] = {
        "blue_collar": 0,
        "white_collar": 0,
        "grey_collar": 0,
        "pink_collar": 0,
    }
    total_count = 0

    for r in roles:
        role_name = r.get("role") or ""
        count = max(1, r.get("count", 1))
        industry = r.get("industry") or ""
        pre_collar = r.get("collar_type") or ""

        if pre_collar and pre_collar in collar_counts:
            collar = pre_collar
            classification = {
                "collar_type": collar,
                "confidence": 1.0,
                "method": "pre_classified",
            }
        else:
            classification = classify_collar(role_name, industry)
            collar = classification["collar_type"]

        collar_counts[collar] += count
        total_count += count
        classified.append(
            {
                "role": role_name,
                "count": count,
                "collar_type": collar,
                "confidence": classification.get("confidence", 0.5),
            }
        )

    # Compute percentages
    collar_pcts: Dict[str, float] = {}
    for c, cnt in collar_counts.items():
        collar_pcts[c] = round(cnt / total_count, 3) if total_count > 0 else 0.0

    # Weighted blend of channel mixes
    blended_channels: Dict[str, float] = {}
    for collar, pct in collar_pcts.items():
        if pct > 0 and collar in COLLAR_STRATEGY:
            for channel, alloc in COLLAR_STRATEGY[collar]["channel_mix"].items():
                blended_channels[channel] = blended_channels.get(channel, 0) + (
                    alloc * pct
                )

    # Normalize to sum to 1.0
    total_alloc = sum(blended_channels.values())
    if total_alloc > 0:
        blended_channels = {
            k: round(v / total_alloc, 3) for k, v in blended_channels.items()
        }

    # Primary collar
    primary = max(collar_pcts, key=collar_pcts.get) if collar_pcts else "white_collar"
    primary_pct = collar_pcts.get(primary, 0)

    # Strategy summary
    if primary_pct >= 0.80:
        summary = f"Strongly {primary.replace('_', ' ')} focused ({primary_pct:.0%}). {COLLAR_STRATEGY.get(primary, {}).get('key_insight') or ''}"
    elif primary_pct >= 0.50:
        secondary = (
            sorted(collar_pcts, key=collar_pcts.get, reverse=True)[1]
            if len(collar_pcts) > 1
            else primary
        )
        summary = f"Primarily {primary.replace('_', ' ')} ({primary_pct:.0%}) with significant {secondary.replace('_', ' ')} ({collar_pcts.get(secondary, 0):.0%}) component. Blended strategy recommended."
    else:
        summary = "Diverse collar mix. Blended multi-channel strategy required for optimal reach across all segments."

    return {
        "collar_breakdown": collar_pcts,
        "blended_channel_mix": blended_channels,
        "primary_collar": primary,
        "strategy_summary": summary,
        "roles_classified": classified,
    }


def get_collar_comparison(
    collar_a: str = "blue_collar", collar_b: str = "white_collar"
) -> Dict[str, Any]:
    """Return a side-by-side comparison of two collar types.

    Useful for the PPT "Collar Strategy" slide and Nova chat comparisons.
    """
    a = COLLAR_STRATEGY.get(collar_a, COLLAR_STRATEGY["blue_collar"])
    b = COLLAR_STRATEGY.get(collar_b, COLLAR_STRATEGY["white_collar"])

    return {
        "collar_a": collar_a,
        "collar_b": collar_b,
        "comparison": {
            "cpc_range": {collar_a: a["avg_cpc_range"], collar_b: b["avg_cpc_range"]},
            "cpa_range": {collar_a: a["avg_cpa_range"], collar_b: b["avg_cpa_range"]},
            "cph_range": {collar_a: a["avg_cph_range"], collar_b: b["avg_cph_range"]},
            "time_to_fill": {
                collar_a: a["time_to_fill_benchmark_days"],
                collar_b: b["time_to_fill_benchmark_days"],
            },
            "apply_rate": {
                collar_a: a["avg_apply_rate"],
                collar_b: b["avg_apply_rate"],
            },
            "mobile_pct": {
                collar_a: a["mobile_apply_pct"],
                collar_b: b["mobile_apply_pct"],
            },
            "top_platforms": {
                collar_a: a["preferred_platforms"][:4],
                collar_b: b["preferred_platforms"][:4],
            },
            "channel_mix": {collar_a: a["channel_mix"], collar_b: b["channel_mix"]},
            "messaging": {collar_a: a["messaging_tone"], collar_b: b["messaging_tone"]},
            "retention": {
                collar_a: a["top_retention_factors"],
                collar_b: b["top_retention_factors"],
            },
        },
        "key_differences": [
            f"CPC: {collar_a.replace('_',' ')} is {round(a['avg_cpc_range'][1]/b['avg_cpc_range'][1]*100 - 100)}% {'cheaper' if a['avg_cpc_range'][1] < b['avg_cpc_range'][1] else 'more expensive'} than {collar_b.replace('_',' ')}",
            f"Time to fill: {a['time_to_fill_benchmark_days']} days vs {b['time_to_fill_benchmark_days']} days",
            f"Mobile apply: {a['mobile_apply_pct']:.0%} vs {b['mobile_apply_pct']:.0%}",
            f"Apply rate: {a['avg_apply_rate']:.1%} vs {b['avg_apply_rate']:.1%}",
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 7. ROLE SENIORITY DECOMPOSITION (Batch C - Micro1 lens)
#    Decomposes a role + count into seniority-level sub-allocations with
#    CPA multipliers.  Supports industry-specific adjustments.
# ═══════════════════════════════════════════════════════════════════════════════

# 7a. Default seniority distribution splits by role family
ROLE_SENIORITY_SPLITS: Dict[str, Dict[str, float]] = {
    "software_engineer": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "data_scientist": {"junior": 0.25, "mid": 0.40, "senior": 0.30, "lead": 0.05},
    "product_manager": {"junior": 0.20, "mid": 0.40, "senior": 0.30, "lead": 0.10},
    "designer": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "nurse": {"junior": 0.35, "mid": 0.40, "senior": 0.20, "lead": 0.05},
    "physician": {"junior": 0.15, "mid": 0.35, "senior": 0.35, "lead": 0.15},
    "pharmacist": {"junior": 0.25, "mid": 0.40, "senior": 0.30, "lead": 0.05},
    "therapist": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "warehouse_worker": {"junior": 0.50, "mid": 0.35, "senior": 0.12, "lead": 0.03},
    "driver": {"junior": 0.40, "mid": 0.40, "senior": 0.15, "lead": 0.05},
    "construction_worker": {"junior": 0.40, "mid": 0.35, "senior": 0.20, "lead": 0.05},
    "electrician": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "plumber": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "mechanic": {"junior": 0.35, "mid": 0.40, "senior": 0.20, "lead": 0.05},
    "retail_associate": {"junior": 0.55, "mid": 0.30, "senior": 0.12, "lead": 0.03},
    "cashier": {"junior": 0.60, "mid": 0.30, "senior": 0.08, "lead": 0.02},
    "restaurant_worker": {"junior": 0.50, "mid": 0.35, "senior": 0.12, "lead": 0.03},
    "hotel_staff": {"junior": 0.45, "mid": 0.35, "senior": 0.15, "lead": 0.05},
    "teacher": {"junior": 0.25, "mid": 0.40, "senior": 0.25, "lead": 0.10},
    "accountant": {"junior": 0.25, "mid": 0.40, "senior": 0.25, "lead": 0.10},
    "analyst": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "sales_rep": {"junior": 0.35, "mid": 0.40, "senior": 0.20, "lead": 0.05},
    "marketing_specialist": {"junior": 0.30, "mid": 0.40, "senior": 0.25, "lead": 0.05},
    "hr_specialist": {"junior": 0.25, "mid": 0.40, "senior": 0.25, "lead": 0.10},
    "executive": {"junior": 0.0, "mid": 0.0, "senior": 0.40, "lead": 0.60},
}

# 7b. CPA multipliers by seniority level
SENIORITY_CPA_MULTIPLIERS: Dict[str, float] = {
    "junior": 0.7,
    "mid": 1.0,
    "senior": 1.6,
    "lead": 2.2,
    "executive": 3.5,
}

# 7c. Industry-specific adjustment factors for seniority splits
#     Multiplied against the default split for a given seniority level,
#     then re-normalized so the distribution sums to 1.0.
INDUSTRY_SENIORITY_ADJUSTMENTS: Dict[str, Dict[str, float]] = {
    "healthcare_medical": {"senior": 1.3, "lead": 1.2},
    "tech_engineering": {"junior": 1.2, "mid": 1.1},
    "retail_consumer": {"junior": 1.3, "mid": 0.9},
    "finance_banking": {"senior": 1.2, "lead": 1.3},
    "logistics_supply_chain": {"junior": 1.2, "mid": 1.1},
    "hospitality_travel": {"junior": 1.3, "mid": 0.9},
}

# ── Keyword -> role family mapping for fuzzy matching ──
_ROLE_FAMILY_KEYWORDS: Dict[str, List[str]] = {
    "software_engineer": [
        "software",
        "developer",
        "programmer",
        "frontend",
        "backend",
        "fullstack",
        "full-stack",
        "full stack",
        "devops",
        "sre",
        "site reliability",
        "web developer",
        "mobile developer",
        "ios developer",
        "android developer",
    ],
    "data_scientist": [
        "data scientist",
        "machine learning",
        "ml engineer",
        "ai engineer",
        "deep learning",
        "nlp engineer",
        "data engineer",
        "data analyst",
    ],
    "product_manager": [
        "product manager",
        "product owner",
        "program manager",
        "project manager",
        "scrum master",
        "agile coach",
    ],
    "designer": [
        "designer",
        "ux",
        "ui",
        "graphic design",
        "visual design",
        "interaction design",
        "creative director",
    ],
    "nurse": [
        "nurse",
        "rn ",
        " rn",
        "lpn",
        "cna",
        "registered nurse",
        "licensed practical nurse",
        "nursing",
    ],
    "physician": [
        "physician",
        "doctor",
        "surgeon",
        "md ",
        " md",
        "medical director",
        "attending",
        "resident physician",
        "hospitalist",
    ],
    "pharmacist": [
        "pharmacist",
        "pharmacy manager",
        "clinical pharmacist",
        "pharmacy director",
    ],
    "therapist": [
        "therapist",
        "counselor",
        "psychologist",
        "social worker",
        "behavioral health",
        "mental health",
        "occupational therapist",
        "physical therapist",
        "speech pathologist",
        "speech therapist",
    ],
    "warehouse_worker": [
        "warehouse",
        "picker",
        "packer",
        "forklift",
        "dock worker",
        "shipping clerk",
        "receiving clerk",
        "material handler",
        "stocker",
        "loader",
    ],
    "driver": [
        "driver",
        "cdl",
        "trucker",
        "truck driver",
        "delivery",
        "courier",
        "chauffeur",
        "bus driver",
        "transit operator",
    ],
    "construction_worker": [
        "construction",
        "laborer",
        "concrete",
        "roofer",
        "ironworker",
        "scaffolder",
        "bricklayer",
        "glazier",
        "tiler",
        "framer",
        "heavy equipment operator",
    ],
    "electrician": [
        "electrician",
        "electrical apprentice",
        "journeyman electrician",
        "master electrician",
        "electrical technician",
    ],
    "plumber": [
        "plumber",
        "pipefitter",
        "steamfitter",
        "plumbing",
        "journeyman plumber",
    ],
    "mechanic": [
        "mechanic",
        "auto mechanic",
        "diesel mechanic",
        "aircraft mechanic",
        "maintenance technician",
        "hvac",
        "hvac technician",
    ],
    "retail_associate": [
        "retail associate",
        "store associate",
        "sales associate",
        "retail clerk",
        "store clerk",
        "merchandise",
    ],
    "cashier": [
        "cashier",
        "checkout",
        "point of sale",
        "pos clerk",
    ],
    "restaurant_worker": [
        "cook",
        "chef",
        "line cook",
        "sous chef",
        "dishwasher",
        "server",
        "waiter",
        "waitress",
        "bartender",
        "barista",
        "food prep",
        "kitchen",
    ],
    "hotel_staff": [
        "hotel",
        "front desk",
        "concierge",
        "housekeeper",
        "housekeeping",
        "bellhop",
        "valet",
        "reservations agent",
    ],
    "teacher": [
        "teacher",
        "instructor",
        "professor",
        "educator",
        "tutor",
        "teaching",
        "faculty",
    ],
    "accountant": [
        "accountant",
        "auditor",
        "bookkeeper",
        "controller",
        "cpa ",
        " cpa",
        "tax preparer",
        "tax analyst",
    ],
    "analyst": [
        "analyst",
        "business analyst",
        "financial analyst",
        "operations analyst",
        "research analyst",
        "intelligence analyst",
    ],
    "sales_rep": [
        "sales rep",
        "sales representative",
        "account executive",
        "business development",
        "bdr",
        "sdr",
        "account manager",
        "territory manager",
        "sales associate",
    ],
    "marketing_specialist": [
        "marketing",
        "content writer",
        "seo",
        "sem",
        "social media",
        "brand manager",
        "communications",
        "public relations",
        "copywriter",
        "digital marketing",
    ],
    "hr_specialist": [
        "hr ",
        " hr",
        "human resources",
        "recruiter",
        "talent acquisition",
        "people operations",
        "compensation",
        "benefits analyst",
        "hrbp",
        "hr generalist",
    ],
    "executive": [
        "executive",
        "ceo",
        "cto",
        "cfo",
        "coo",
        "cmo",
        "cio",
        "chief",
        "president",
        "vice president",
        "vp ",
        " vp",
        "svp",
        "evp",
        "managing director",
        "partner",
    ],
}


def _match_role_family(role: str) -> str:
    """Fuzzy-match a role string to a key in ROLE_SENIORITY_SPLITS.

    Uses keyword matching against _ROLE_FAMILY_KEYWORDS.  Returns the best
    matching role family key, or ``"analyst"`` as a safe middle-ground
    default when no keywords match.

    Thread-safe: operates on immutable module-level dicts only.
    """
    try:
        if not role:
            return "analyst"

        role_lower = role.strip().lower()

        best_family: str = "analyst"
        best_match_count: int = 0
        best_match_len: int = 0  # tiebreaker: longest keyword matched

        for family, keywords in _ROLE_FAMILY_KEYWORDS.items():
            match_count = 0
            longest = 0
            for kw in keywords:
                if kw in role_lower:
                    match_count += 1
                    if len(kw) > longest:
                        longest = len(kw)
            if match_count > best_match_count or (
                match_count == best_match_count and longest > best_match_len
            ):
                best_match_count = match_count
                best_match_len = longest
                best_family = family

        if best_match_count == 0:
            logger.debug(
                "No role-family keyword match for '%s'; defaulting to 'analyst'",
                role,
            )
            return "analyst"

        logger.debug(
            "Matched role '%s' -> family '%s' (matches=%d)",
            role,
            best_family,
            best_match_count,
        )
        return best_family

    except Exception:
        logger.exception("Error in _match_role_family for role='%s'", role)
        return "analyst"


def decompose_role(
    role: str,
    count: int,
    industry: str = "",
) -> List[Dict[str, Any]]:
    """Decompose a role into seniority-level sub-allocations.

    Breaks a hiring requisition into junior/mid/senior/lead segments using
    default splits per role family, optionally adjusted by industry.

    Args:
        role: Job title or role name (e.g. "Software Engineer").
        count: Total headcount to decompose.
        industry: Optional industry key (e.g. "tech_engineering") used to
                  shift the seniority distribution.

    Returns:
        List of dicts, each representing a seniority sub-segment::

            [{
                "title": "Junior Software Engineer",
                "count": 15,
                "seniority": "junior",
                "cpa_multiplier": 0.7,
                "collar_type": "white_collar",
                "pct_of_total": 0.30,
            }, ...]

    Thread-safe.  All exceptions caught and logged; returns a single-item
    fallback list on error.
    """
    try:
        if count <= 0:
            count = 1

        # Step 1: Match role to a family
        family = _match_role_family(role)

        # Step 2: Get base seniority splits
        base_splits = ROLE_SENIORITY_SPLITS.get(
            family, ROLE_SENIORITY_SPLITS["analyst"]
        )
        # Work on a mutable copy
        splits: Dict[str, float] = dict(base_splits)

        # Step 3: Apply industry adjustments (if applicable)
        ind_key = (
            industry.strip().lower().replace(" ", "_").replace("-", "_")
            if industry
            else ""
        )
        if ind_key and ind_key in INDUSTRY_SENIORITY_ADJUSTMENTS:
            adjustments = INDUSTRY_SENIORITY_ADJUSTMENTS[ind_key]
            for seniority, factor in adjustments.items():
                if seniority in splits:
                    splits[seniority] = splits[seniority] * factor
            # Re-normalize to sum = 1.0
            total = sum(splits.values())
            if total > 0:
                splits = {k: v / total for k, v in splits.items()}

        # Step 4: Compute counts per seniority level
        # Use largest-remainder method to ensure integer counts sum to `count`
        raw_counts: Dict[str, float] = {}
        for seniority, pct in splits.items():
            if pct > 0:
                raw_counts[seniority] = pct * count

        # Floor allocation + remainder distribution
        floored: Dict[str, int] = {s: int(rc) for s, rc in raw_counts.items()}
        remainders: Dict[str, float] = {
            s: rc - floored[s] for s, rc in raw_counts.items()
        }
        allocated = sum(floored.values())
        shortfall = count - allocated

        # Distribute shortfall to seniority levels with largest remainders
        for s in sorted(remainders, key=remainders.get, reverse=True):
            if shortfall <= 0:
                break
            floored[s] += 1
            shortfall -= 1

        # Step 5: Classify collar type (reuse existing classify_collar)
        collar_info = classify_collar(role, industry)
        collar_type = collar_info.get("collar_type", "white_collar")

        # Step 6: Build result list
        results: List[Dict[str, Any]] = []
        seniority_label_prefix = {
            "junior": "Junior",
            "mid": "Mid-Level",
            "senior": "Senior",
            "lead": "Lead",
        }
        for seniority, sub_count in floored.items():
            if sub_count <= 0:
                continue
            prefix = seniority_label_prefix.get(seniority, seniority.capitalize())
            title = f"{prefix} {role.strip()}" if role.strip() else prefix
            cpa_mult = SENIORITY_CPA_MULTIPLIERS.get(seniority, 1.0)
            pct = round(sub_count / count, 4) if count > 0 else 0.0

            results.append(
                {
                    "title": title,
                    "count": sub_count,
                    "seniority": seniority,
                    "cpa_multiplier": cpa_mult,
                    "collar_type": collar_type,
                    "pct_of_total": pct,
                }
            )

        # Sort by seniority order for consistent output
        _SENIORITY_ORDER = {
            "junior": 0,
            "mid": 1,
            "senior": 2,
            "lead": 3,
            "executive": 4,
        }
        results.sort(key=lambda d: _SENIORITY_ORDER.get(d["seniority"], 99))

        logger.debug(
            "Decomposed role='%s' count=%d into %d seniority segments (family=%s)",
            role,
            count,
            len(results),
            family,
        )
        return results

    except Exception:
        logger.exception(
            "Error in decompose_role(role='%s', count=%d, industry='%s')",
            role,
            count,
            industry,
        )
        # Fallback: return single-item list with the original role
        collar_info = classify_collar(role, industry)
        return [
            {
                "title": role.strip() if role.strip() else "Unknown Role",
                "count": max(count, 1),
                "seniority": "mid",
                "cpa_multiplier": 1.0,
                "collar_type": collar_info.get("collar_type", "white_collar"),
                "pct_of_total": 1.0,
            }
        ]


# ═══════════════════════════════════════════════════════════════════════════════
# 8. SKILLS-GAP ANALYSIS (Batch F - Micro1 lens)
#    Maps roles to required skills, scores skill scarcity, and recommends
#    hiring-channel adjustments based on talent supply gaps.
# ═══════════════════════════════════════════════════════════════════════════════

# 8a. Top skills per role family (sourced from O*NET knowledge/skill categories)
ROLE_SKILLS_MAP: Dict[str, List[str]] = {
    "software_engineer": [
        "Python",
        "JavaScript",
        "SQL",
        "Cloud (AWS/GCP/Azure)",
        "Git",
        "Agile/Scrum",
        "System Design",
    ],
    "data_scientist": [
        "Python",
        "Machine Learning",
        "SQL",
        "Statistics",
        "TensorFlow/PyTorch",
        "Data Visualization",
    ],
    "product_manager": [
        "Product Strategy",
        "Agile/Scrum",
        "Data Analysis",
        "User Research",
        "Roadmapping",
        "Stakeholder Management",
    ],
    "designer": [
        "Figma/Sketch",
        "User Research",
        "Prototyping",
        "Visual Design",
        "Design Systems",
        "Accessibility",
    ],
    "nurse": [
        "Patient Assessment",
        "Medication Administration",
        "Electronic Health Records",
        "BLS/ACLS",
        "Patient Education",
        "Clinical Documentation",
    ],
    "physician": [
        "Clinical Diagnosis",
        "Patient Assessment",
        "Electronic Health Records",
        "Medical Procedures",
        "Clinical Research",
        "Board Certification",
    ],
    "pharmacist": [
        "Medication Dispensing",
        "Drug Interaction Analysis",
        "Patient Counseling",
        "Pharmacy Management Systems",
        "Regulatory Compliance",
        "Compounding",
    ],
    "therapist": [
        "Therapeutic Techniques",
        "Patient Assessment",
        "Treatment Planning",
        "Clinical Documentation",
        "Crisis Intervention",
        "Cultural Competency",
    ],
    "warehouse_worker": [
        "Forklift Operation",
        "Inventory Management",
        "RF Scanner",
        "Safety Protocols",
        "Order Picking",
        "Physical Stamina",
    ],
    "driver": [
        "CDL License",
        "DOT Regulations",
        "Route Navigation",
        "Vehicle Inspection",
        "Hours of Service",
        "Defensive Driving",
    ],
    "construction_worker": [
        "Blueprint Reading",
        "Power Tools",
        "Safety Protocols",
        "Physical Stamina",
        "OSHA Compliance",
        "Material Handling",
        "Concrete Work",
    ],
    "electrician": [
        "NEC Code Knowledge",
        "Electrical Troubleshooting",
        "Blueprint Reading",
        "Safety Protocols",
        "Conduit Bending",
        "PLC Programming",
    ],
    "plumber": [
        "Pipe Fitting",
        "Blueprint Reading",
        "Plumbing Codes",
        "Welding",
        "Safety Protocols",
        "Drain Cleaning",
    ],
    "mechanic": [
        "Diagnostic Equipment",
        "Engine Repair",
        "Electrical Systems",
        "Brake Systems",
        "Preventive Maintenance",
        "ASE Certification",
    ],
    "retail_associate": [
        "Customer Service",
        "POS Systems",
        "Merchandising",
        "Inventory Management",
        "Cash Handling",
        "Product Knowledge",
    ],
    "cashier": [
        "Cash Handling",
        "POS Systems",
        "Customer Service",
        "Basic Math",
        "Attention to Detail",
    ],
    "restaurant_worker": [
        "Food Safety (ServSafe)",
        "Customer Service",
        "POS Systems",
        "Time Management",
        "Team Collaboration",
        "Physical Stamina",
    ],
    "hotel_staff": [
        "Guest Relations",
        "Reservation Systems",
        "Customer Service",
        "Multitasking",
        "Conflict Resolution",
        "Attention to Detail",
    ],
    "teacher": [
        "Curriculum Development",
        "Classroom Management",
        "Differentiated Instruction",
        "Assessment Design",
        "Educational Technology",
        "Student Engagement",
    ],
    "accountant": [
        "GAAP/IFRS",
        "Tax Preparation",
        "Financial Reporting",
        "Excel/Spreadsheets",
        "Audit Procedures",
        "ERP Systems (SAP/Oracle)",
    ],
    "analyst": [
        "Data Analysis",
        "Excel/Spreadsheets",
        "SQL",
        "Data Visualization",
        "Statistical Analysis",
        "Presentation Skills",
    ],
    "sales_rep": [
        "CRM Software",
        "Prospecting",
        "Negotiation",
        "Pipeline Management",
        "Presentation Skills",
        "Account Management",
    ],
    "marketing_specialist": [
        "Digital Marketing",
        "Content Strategy",
        "SEO/SEM",
        "Marketing Analytics",
        "Social Media Management",
        "Campaign Management",
    ],
    "hr_specialist": [
        "Talent Acquisition",
        "HRIS Systems",
        "Employment Law",
        "Compensation Analysis",
        "Employee Relations",
        "Performance Management",
    ],
    "executive": [
        "Strategic Planning",
        "Leadership",
        "P&L Management",
        "Board Relations",
        "Organizational Design",
        "Change Management",
        "M&A Experience",
    ],
}

# 8b. Skill scarcity indicators (0.0 = abundant, 1.0 = extremely scarce)
#     Reflects general US labor-market supply-demand tension.
SKILL_SCARCITY: Dict[str, float] = {
    # ── Technology skills ──
    "Python": 0.35,
    "JavaScript": 0.30,
    "SQL": 0.25,
    "Cloud (AWS/GCP/Azure)": 0.55,
    "Machine Learning": 0.70,
    "System Design": 0.65,
    "Kubernetes": 0.72,
    "Rust": 0.80,
    "Git": 0.15,
    "Agile/Scrum": 0.20,
    "TensorFlow/PyTorch": 0.68,
    "Data Visualization": 0.30,
    "Statistics": 0.40,
    "PLC Programming": 0.58,
    "Figma/Sketch": 0.32,
    "Prototyping": 0.35,
    "Visual Design": 0.30,
    "Design Systems": 0.45,
    "Accessibility": 0.50,
    "Educational Technology": 0.35,
    # ── Healthcare / clinical skills ──
    "BLS/ACLS": 0.40,
    "Patient Assessment": 0.45,
    "Electronic Health Records": 0.35,
    "Medication Administration": 0.42,
    "Patient Education": 0.30,
    "Clinical Documentation": 0.32,
    "Clinical Diagnosis": 0.60,
    "Medical Procedures": 0.58,
    "Clinical Research": 0.55,
    "Board Certification": 0.50,
    "Medication Dispensing": 0.35,
    "Drug Interaction Analysis": 0.48,
    "Patient Counseling": 0.38,
    "Pharmacy Management Systems": 0.42,
    "Compounding": 0.52,
    "Therapeutic Techniques": 0.45,
    "Treatment Planning": 0.40,
    "Crisis Intervention": 0.50,
    "Cultural Competency": 0.30,
    # ── Blue collar / trades skills ──
    "CDL License": 0.60,
    "Forklift Operation": 0.30,
    "Welding": 0.55,
    "DOT Regulations": 0.35,
    "Route Navigation": 0.20,
    "Vehicle Inspection": 0.25,
    "Hours of Service": 0.22,
    "Defensive Driving": 0.20,
    "RF Scanner": 0.15,
    "Inventory Management": 0.25,
    "Safety Protocols": 0.20,
    "Order Picking": 0.12,
    "Physical Stamina": 0.10,
    "Blueprint Reading": 0.42,
    "Power Tools": 0.20,
    "OSHA Compliance": 0.28,
    "Material Handling": 0.18,
    "Concrete Work": 0.45,
    "NEC Code Knowledge": 0.52,
    "Electrical Troubleshooting": 0.48,
    "Conduit Bending": 0.40,
    "Pipe Fitting": 0.45,
    "Plumbing Codes": 0.38,
    "Drain Cleaning": 0.22,
    "Diagnostic Equipment": 0.40,
    "Engine Repair": 0.42,
    "Electrical Systems": 0.38,
    "Brake Systems": 0.32,
    "Preventive Maintenance": 0.25,
    "ASE Certification": 0.45,
    # ── Business / general professional skills ──
    "Data Analysis": 0.30,
    "Excel/Spreadsheets": 0.15,
    "Presentation Skills": 0.18,
    "Statistical Analysis": 0.38,
    "Product Strategy": 0.48,
    "User Research": 0.40,
    "Roadmapping": 0.30,
    "Stakeholder Management": 0.28,
    "GAAP/IFRS": 0.42,
    "Tax Preparation": 0.35,
    "Financial Reporting": 0.38,
    "Audit Procedures": 0.40,
    "ERP Systems (SAP/Oracle)": 0.50,
    "CRM Software": 0.22,
    "Prospecting": 0.25,
    "Negotiation": 0.30,
    "Pipeline Management": 0.28,
    "Account Management": 0.25,
    "Digital Marketing": 0.32,
    "Content Strategy": 0.35,
    "SEO/SEM": 0.38,
    "Marketing Analytics": 0.42,
    "Social Media Management": 0.22,
    "Campaign Management": 0.30,
    "Talent Acquisition": 0.35,
    "HRIS Systems": 0.38,
    "Employment Law": 0.45,
    "Compensation Analysis": 0.42,
    "Employee Relations": 0.30,
    "Performance Management": 0.28,
    # ── Leadership / executive skills ──
    "Strategic Planning": 0.45,
    "Leadership": 0.30,
    "P&L Management": 0.55,
    "Board Relations": 0.62,
    "Organizational Design": 0.58,
    "Change Management": 0.48,
    "M&A Experience": 0.72,
    "Project Management": 0.25,
    "Regulatory Compliance": 0.40,
    # ── Service industry skills ──
    "Customer Service": 0.12,
    "POS Systems": 0.10,
    "Cash Handling": 0.08,
    "Merchandising": 0.20,
    "Product Knowledge": 0.15,
    "Basic Math": 0.05,
    "Attention to Detail": 0.10,
    "Food Safety (ServSafe)": 0.25,
    "Time Management": 0.10,
    "Team Collaboration": 0.10,
    "Guest Relations": 0.22,
    "Reservation Systems": 0.20,
    "Multitasking": 0.08,
    "Conflict Resolution": 0.25,
    # ── Education skills ──
    "Curriculum Development": 0.40,
    "Classroom Management": 0.35,
    "Differentiated Instruction": 0.42,
    "Assessment Design": 0.38,
    "Student Engagement": 0.30,
}


def analyze_skills_gap(
    role: str,
    location: str = "",
    industry: str = "",
) -> Dict[str, Any]:
    """Analyze the skills gap for a given role.

    Identifies required skills, partitions them by scarcity, computes
    an overall scarcity score, and derives a CPA hiring-difficulty
    adjustment multiplier.

    Args:
        role: Job title or role name (e.g. "Software Engineer").
        location: Optional location context (reserved for future
                  geo-specific scarcity overlays).
        industry: Optional industry context for role-family matching.

    Returns:
        Dict with keys::

            {
                "role_family": str,
                "required_skills": List[str],
                "scarce_skills": List[Dict[str, Any]],
                "abundant_skills": List[Dict[str, Any]],
                "overall_scarcity_score": float,
                "hiring_difficulty_adjustment": float,
                "recommendations": List[str],
            }

    Thread-safe.  All exceptions caught and logged; returns a safe
    fallback dict on error.
    """
    try:
        family = _match_role_family(role)

        # Look up required skills
        required_skills = ROLE_SKILLS_MAP.get(
            family, ROLE_SKILLS_MAP.get("analyst") or []
        )

        # Partition skills into scarce vs abundant
        scarce_skills: List[Dict[str, Any]] = []
        abundant_skills: List[Dict[str, Any]] = []
        scarcity_values: List[float] = []

        for skill in required_skills:
            scarcity = SKILL_SCARCITY.get(skill, 0.40)  # default 0.4 if unknown
            scarcity_values.append(scarcity)
            entry = {"skill": skill, "scarcity": round(scarcity, 2)}
            if scarcity > 0.5:
                scarce_skills.append(entry)
            else:
                abundant_skills.append(entry)

        # Sort by scarcity descending for convenience
        scarce_skills.sort(key=lambda d: d["scarcity"], reverse=True)
        abundant_skills.sort(key=lambda d: d["scarcity"], reverse=True)

        # Weighted average scarcity score
        overall_scarcity = (
            round(sum(scarcity_values) / len(scarcity_values), 3)
            if scarcity_values
            else 0.40
        )

        # Hiring difficulty adjustment: 1.0 + (scarcity * 0.4) => range 1.0-1.4
        hiring_difficulty = round(1.0 + (overall_scarcity * 0.4), 3)

        # Generate recommendations (2-3 contextual tips)
        recommendations: List[str] = []

        if scarce_skills:
            top_scarce_names = [s["skill"] for s in scarce_skills[:3]]
            recommendations.append(
                f"{', '.join(top_scarce_names)} "
                f"{'is' if len(top_scarce_names) == 1 else 'are'} scarce "
                f"(>0.5 scarcity) - consider premium channels and targeted sourcing."
            )

        if overall_scarcity >= 0.5:
            recommendations.append(
                "Overall skill scarcity is high. Budget for longer time-to-fill "
                "and higher CPA. Employer branding and referral programs can help."
            )
        elif overall_scarcity >= 0.35:
            recommendations.append(
                "Moderate skill scarcity. A blended approach of job boards and "
                "targeted outreach should yield results within standard timelines."
            )
        else:
            recommendations.append(
                "Required skills are generally abundant in the market. "
                "Volume-based channels (programmatic, job boards) should perform well."
            )

        if len(scarce_skills) >= 3:
            recommendations.append(
                "Multiple scarce skills detected. Consider hiring for potential "
                "and investing in upskilling/training programs."
            )

        logger.debug(
            "Skills gap for role='%s' (family=%s): scarcity=%.3f, difficulty=%.3f, "
            "scarce=%d, abundant=%d",
            role,
            family,
            overall_scarcity,
            hiring_difficulty,
            len(scarce_skills),
            len(abundant_skills),
        )

        return {
            "role_family": family,
            "required_skills": list(required_skills),
            "scarce_skills": scarce_skills,
            "abundant_skills": abundant_skills,
            "overall_scarcity_score": overall_scarcity,
            "hiring_difficulty_adjustment": hiring_difficulty,
            "recommendations": recommendations,
        }

    except Exception:
        logger.exception(
            "Error in analyze_skills_gap(role='%s', location='%s', industry='%s')",
            role,
            location,
            industry,
        )
        return {
            "role_family": "analyst",
            "required_skills": [],
            "scarce_skills": [],
            "abundant_skills": [],
            "overall_scarcity_score": 0.40,
            "hiring_difficulty_adjustment": 1.16,
            "recommendations": [
                "Unable to analyze skills gap; using default moderate-difficulty estimates."
            ],
        }
