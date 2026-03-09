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
        normalize_role as _std_normalize_role,
        get_role_tier as _std_get_role_tier,
        get_soc_code as _std_get_soc_code,
        CANONICAL_ROLES as _CANON_ROLES,
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
    "11": "white_collar",   # Management
    "13": "white_collar",   # Business and Financial Operations
    "15": "white_collar",   # Computer and Mathematical
    "17": "white_collar",   # Architecture and Engineering
    "19": "white_collar",   # Life, Physical, and Social Science
    "23": "white_collar",   # Legal
    "25": "white_collar",   # Educational Instruction and Library
    "27": "white_collar",   # Arts, Design, Entertainment, Sports, Media

    # Grey collar (licensed/clinical, skilled hybrid)
    "21": "grey_collar",    # Community and Social Service
    "29": "grey_collar",    # Healthcare Practitioners and Technical

    # Pink collar (admin, care, service)
    "31": "pink_collar",    # Healthcare Support
    "39": "pink_collar",    # Personal Care and Service
    "43": "pink_collar",    # Office and Administrative Support

    # Blue collar (manual, trades, operations)
    "33": "blue_collar",    # Protective Service
    "35": "blue_collar",    # Food Preparation and Serving
    "37": "blue_collar",    # Building and Grounds Cleaning/Maintenance
    "41": "blue_collar",    # Sales (retail/field)
    "45": "blue_collar",    # Farming, Fishing, and Forestry
    "47": "blue_collar",    # Construction and Extraction
    "49": "blue_collar",    # Installation, Maintenance, and Repair
    "51": "blue_collar",    # Production
    "53": "blue_collar",    # Transportation and Material Moving
    "55": "blue_collar",    # Military Specific
}


# ═══════════════════════════════════════════════════════════════════════════════
# 2. KEYWORD-BASED CLASSIFICATION (fallback when SOC unavailable)
# ═══════════════════════════════════════════════════════════════════════════════

_BLUE_COLLAR_KEYWORDS = {
    "driver", "cdl", "trucker", "delivery", "courier", "warehouse", "forklift",
    "picker", "packer", "stocker", "dock", "loader", "mover", "shipping",
    "receiving", "material handler", "laborer", "construction", "carpenter",
    "electrician", "plumber", "hvac", "welder", "machinist", "painter",
    "roofer", "ironworker", "crane", "heavy equipment", "concrete",
    "mechanic", "technician", "installer", "maintenance", "janitor",
    "custodian", "landscaper", "pest control", "factory", "assembly",
    "production", "machine operator", "line worker", "manufacturing",
    "cook", "chef", "dishwasher", "server", "bartender", "barista",
    "housekeeper", "cleaner", "security guard", "farmer", "fisherman",
    "mining", "oil rig", "deckhand", "marine", "seaman", "boatswain",
    "rigger", "scaffolder", "bricklayer", "glazier", "tiler",
}

_WHITE_COLLAR_KEYWORDS = {
    "engineer", "developer", "programmer", "architect", "scientist",
    "analyst", "manager", "director", "vp", "vice president", "ceo",
    "cto", "cfo", "coo", "executive", "consultant", "attorney", "lawyer",
    "counsel", "accountant", "auditor", "controller", "actuary",
    "underwriter", "professor", "researcher", "physician", "surgeon",
    "specialist", "psychologist", "pharmacist", "product manager",
    "project manager", "program manager", "data scientist", "designer",
    "strategist", "planner", "broker", "trader", "portfolio",
    "compliance", "regulatory",
}

_GREY_COLLAR_KEYWORDS = {
    "nurse", "rn", "lpn", "cna", "medical assistant", "dental",
    "phlebotomist", "emt", "paramedic", "therapist", "counselor",
    "social worker", "respiratory", "radiology", "surgical tech",
    "pharmacy tech", "lab technician", "medical technologist",
    "occupational therapist", "physical therapist", "speech pathologist",
    "dietitian", "optician", "audiologist",
}

_PINK_COLLAR_KEYWORDS = {
    "receptionist", "secretary", "administrative", "admin assistant",
    "office manager", "clerk", "data entry", "bookkeeper",
    "customer service", "call center", "support specialist",
    "caregiver", "home health aide", "childcare", "nanny",
    "teacher aide", "teaching assistant", "library",
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
        major_group = resolved_soc.split("-")[0] if "-" in resolved_soc else resolved_soc[:2]
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
                    return _build_result(tier_collar, 0.85, "standardizer_tier", sub_type, indicators)
        except Exception:
            pass

    # Method 3: Keyword matching
    collar, kw_confidence, matched_keywords = _keyword_classify(role_lower)
    if collar and kw_confidence >= 0.60:
        indicators.extend([f"keyword: {kw}" for kw in matched_keywords[:3]])
        sub_type = _get_sub_type(role_lower, collar)
        return _build_result(collar, kw_confidence, "keyword_match", sub_type, indicators)

    # Method 4: Industry-based fallback
    if industry:
        ind_lower = industry.strip().lower().replace(" ", "_").replace("-", "_")
        ind_collar = _INDUSTRY_DEFAULT_COLLAR.get(ind_lower)
        if ind_collar:
            indicators.append(f"industry default: {industry}")
            sub_type = _get_sub_type(role_lower, ind_collar)
            return _build_result(ind_collar, 0.45, "industry_fallback", sub_type, indicators)

    # Ultimate fallback
    return _default_result("white_collar", 0.25, "no_match")


# Tier -> collar mapping
_TIER_TO_COLLAR: Dict[str, str] = {
    "executive": "white_collar",
    "professional": "white_collar",
    "skilled": "blue_collar",  # skilled trades
    "entry": "blue_collar",    # entry-level hourly
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
        "blue_collar": [], "white_collar": [], "grey_collar": [], "pink_collar": [],
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
        if any(kw in role_lower for kw in ("driver", "cdl", "trucker", "delivery", "courier")):
            return "transportation"
        if any(kw in role_lower for kw in ("warehouse", "forklift", "picker", "dock", "shipping")):
            return "warehouse_logistics"
        if any(kw in role_lower for kw in ("construction", "carpenter", "electrician", "plumber", "welder")):
            return "skilled_trades"
        if any(kw in role_lower for kw in ("factory", "assembly", "production", "machine", "manufacturing")):
            return "manufacturing"
        if any(kw in role_lower for kw in ("cook", "chef", "server", "bartender", "barista")):
            return "food_service"
        if any(kw in role_lower for kw in ("security", "guard", "loss prevention")):
            return "protective_service"
        if any(kw in role_lower for kw in ("mechanic", "technician", "installer", "maintenance")):
            return "maintenance_repair"
        return "general_labor"

    if collar == "white_collar":
        if any(kw in role_lower for kw in ("engineer", "developer", "programmer", "architect", "devops")):
            return "technology"
        if any(kw in role_lower for kw in ("executive", "ceo", "cto", "cfo", "vp", "director")):
            return "executive"
        if any(kw in role_lower for kw in ("analyst", "consultant", "strategist")):
            return "business_professional"
        if any(kw in role_lower for kw in ("attorney", "lawyer", "counsel", "paralegal")):
            return "legal"
        if any(kw in role_lower for kw in ("accountant", "auditor", "controller", "actuary")):
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
        if any(kw in role_lower for kw in ("receptionist", "secretary", "admin", "clerk")):
            return "administrative"
        if any(kw in role_lower for kw in ("customer service", "call center", "support")):
            return "customer_service"
        if any(kw in role_lower for kw in ("caregiver", "home health", "childcare", "nanny")):
            return "care_service"
        return "general_service"

    return "unclassified"


def _build_result(
    collar: str, confidence: float, method: str,
    sub_type: str, indicators: List[str],
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
    "blue_collar": "volume",    # High volume, low cost, mobile-first
    "white_collar": "targeted", # Targeted, professional platforms, quality over quantity
    "grey_collar": "targeted",  # Niche boards, clinical networks
    "pink_collar": "volume",    # Volume-oriented but different channels than blue collar
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
        "preferred_platforms": ["Indeed", "Facebook", "Craigslist", "JobGet", "Instawork", "Snagajob"],
        "messaging_tone": "Direct, benefits-focused, mobile-first. Lead with pay rate, schedule, and location.",
        "ad_format_priority": ["mobile_display", "social_feed", "sms", "push_notification"],
        "application_complexity": "Minimal: name, phone, 1-click apply. No resume required.",
        "time_to_fill_benchmark_days": 14,
        "avg_cpa_range": [8, 25],
        "avg_cpc_range": [0.25, 1.20],
        "avg_cph_range": [2500, 5500],
        "peak_job_seeking_hours": ["6-8 AM", "5-9 PM", "Weekends"],
        "top_retention_factors": ["pay", "schedule_flexibility", "proximity", "benefits"],
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
        "preferred_platforms": ["LinkedIn", "Indeed", "Glassdoor", "Dice", "BuiltIn", "AngelList"],
        "messaging_tone": "Career-growth focused. Highlight culture, remote/hybrid options, DEI, and total compensation.",
        "ad_format_priority": ["linkedin_inmail", "search_ads", "display_retargeting", "email_campaigns"],
        "application_complexity": "Standard: resume upload, optional cover letter. 5-10 min process acceptable.",
        "time_to_fill_benchmark_days": 38,
        "avg_cpa_range": [20, 75],
        "avg_cpc_range": [1.50, 5.00],
        "avg_cph_range": [6000, 22000],
        "peak_job_seeking_hours": ["7-9 AM", "12-1 PM", "8-10 PM"],
        "top_retention_factors": ["career_growth", "compensation", "remote_flexibility", "culture", "learning"],
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
        "preferred_platforms": ["Indeed", "Vivian Health", "NurseFly", "Health eCareers", "LinkedIn"],
        "messaging_tone": "Credential-aware. Highlight licensure support, shift flexibility, sign-on bonuses, and continuing education.",
        "ad_format_priority": ["niche_job_boards", "social_feed", "search_ads", "email"],
        "application_complexity": "Credential-focused: license verification, certifications required. 5-15 min process.",
        "time_to_fill_benchmark_days": 28,
        "avg_cpa_range": [15, 50],
        "avg_cpc_range": [0.80, 3.00],
        "avg_cph_range": [5000, 15000],
        "peak_job_seeking_hours": ["6-8 AM", "7-10 PM", "Weekends"],
        "top_retention_factors": ["schedule_flexibility", "pay", "sign_on_bonus", "patient_ratio", "burnout_support"],
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
        "preferred_platforms": ["Indeed", "Facebook", "Snagajob", "Care.com", "LinkedIn"],
        "messaging_tone": "People-focused. Highlight work environment, team culture, growth opportunities, and benefits.",
        "ad_format_priority": ["social_feed", "mobile_display", "search_ads", "email"],
        "application_complexity": "Simple: resume optional, quick apply preferred. 3-5 min process.",
        "time_to_fill_benchmark_days": 18,
        "avg_cpa_range": [10, 30],
        "avg_cpc_range": [0.50, 1.80],
        "avg_cph_range": [3000, 7000],
        "peak_job_seeking_hours": ["7-9 AM", "12-2 PM", "6-9 PM"],
        "top_retention_factors": ["work_environment", "pay", "benefits", "schedule", "growth"],
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
        "white_collar_roles": ["software engineer", "data scientist", "product manager", "designer"],
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
        "blue_collar_roles": ["electrician", "plumber", "welder", "carpenter", "HVAC tech"],
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
        "blue_collar_roles": ["truck driver", "warehouse worker", "forklift operator", "dock worker"],
        "white_collar_roles": ["logistics manager", "supply chain analyst", "operations director"],
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
        "blue_collar_roles": ["cook", "server", "bartender", "housekeeper", "dishwasher"],
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
        "blue_collar_roles": ["construction laborer", "heavy equipment operator", "scaffold erector"],
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
        "white_collar_roles": ["research scientist", "clinical researcher", "regulatory affairs"],
        "grey_collar_roles": ["lab technician", "quality control analyst", "pharmacy tech"],
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
        "blue_collar_roles": ["machinist", "assembly technician", "welder", "sheet metal worker"],
        "white_collar_roles": ["aerospace engineer", "systems engineer", "program manager"],
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
        "blue_collar_roles": ["lineman", "pipeline worker", "plant operator", "field tech"],
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
        "blue_collar_roles": ["cook", "line cook", "dishwasher", "baker", "food production"],
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
        "white_collar_roles": ["automotive engineer", "design engineer", "plant manager"],
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
        "blue_collar_roles": ["deckhand", "marine diesel mechanic", "rigger", "crane operator"],
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
            "collar_breakdown": {"blue_collar": 0.25, "white_collar": 0.25,
                                 "grey_collar": 0.25, "pink_collar": 0.25},
            "blended_channel_mix": COLLAR_STRATEGY["white_collar"]["channel_mix"],
            "primary_collar": "white_collar",
            "strategy_summary": "Balanced strategy (no role data provided).",
            "roles_classified": [],
        }

    # Classify each role and weight by count
    classified = []
    collar_counts: Dict[str, int] = {
        "blue_collar": 0, "white_collar": 0, "grey_collar": 0, "pink_collar": 0,
    }
    total_count = 0

    for r in roles:
        role_name = r.get("role", "")
        count = max(1, r.get("count", 1))
        industry = r.get("industry", "")
        pre_collar = r.get("collar_type", "")

        if pre_collar and pre_collar in collar_counts:
            collar = pre_collar
            classification = {"collar_type": collar, "confidence": 1.0, "method": "pre_classified"}
        else:
            classification = classify_collar(role_name, industry)
            collar = classification["collar_type"]

        collar_counts[collar] += count
        total_count += count
        classified.append({
            "role": role_name,
            "count": count,
            "collar_type": collar,
            "confidence": classification.get("confidence", 0.5),
        })

    # Compute percentages
    collar_pcts: Dict[str, float] = {}
    for c, cnt in collar_counts.items():
        collar_pcts[c] = round(cnt / total_count, 3) if total_count > 0 else 0.0

    # Weighted blend of channel mixes
    blended_channels: Dict[str, float] = {}
    for collar, pct in collar_pcts.items():
        if pct > 0 and collar in COLLAR_STRATEGY:
            for channel, alloc in COLLAR_STRATEGY[collar]["channel_mix"].items():
                blended_channels[channel] = blended_channels.get(channel, 0) + (alloc * pct)

    # Normalize to sum to 1.0
    total_alloc = sum(blended_channels.values())
    if total_alloc > 0:
        blended_channels = {k: round(v / total_alloc, 3) for k, v in blended_channels.items()}

    # Primary collar
    primary = max(collar_pcts, key=collar_pcts.get) if collar_pcts else "white_collar"
    primary_pct = collar_pcts.get(primary, 0)

    # Strategy summary
    if primary_pct >= 0.80:
        summary = f"Strongly {primary.replace('_', ' ')} focused ({primary_pct:.0%}). {COLLAR_STRATEGY.get(primary, {}).get('key_insight', '')}"
    elif primary_pct >= 0.50:
        secondary = sorted(collar_pcts, key=collar_pcts.get, reverse=True)[1] if len(collar_pcts) > 1 else primary
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


def get_collar_comparison(collar_a: str = "blue_collar", collar_b: str = "white_collar") -> Dict[str, Any]:
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
            "time_to_fill": {collar_a: a["time_to_fill_benchmark_days"], collar_b: b["time_to_fill_benchmark_days"]},
            "apply_rate": {collar_a: a["avg_apply_rate"], collar_b: b["avg_apply_rate"]},
            "mobile_pct": {collar_a: a["mobile_apply_pct"], collar_b: b["mobile_apply_pct"]},
            "top_platforms": {collar_a: a["preferred_platforms"][:4], collar_b: b["preferred_platforms"][:4]},
            "channel_mix": {collar_a: a["channel_mix"], collar_b: b["channel_mix"]},
            "messaging": {collar_a: a["messaging_tone"], collar_b: b["messaging_tone"]},
            "retention": {collar_a: a["top_retention_factors"], collar_b: b["top_retention_factors"]},
        },
        "key_differences": [
            f"CPC: {collar_a.replace('_',' ')} is {round(a['avg_cpc_range'][1]/b['avg_cpc_range'][1]*100 - 100)}% {'cheaper' if a['avg_cpc_range'][1] < b['avg_cpc_range'][1] else 'more expensive'} than {collar_b.replace('_',' ')}",
            f"Time to fill: {a['time_to_fill_benchmark_days']} days vs {b['time_to_fill_benchmark_days']} days",
            f"Mobile apply: {a['mobile_apply_pct']:.0%} vs {b['mobile_apply_pct']:.0%}",
            f"Apply rate: {a['avg_apply_rate']:.1%} vs {b['avg_apply_rate']:.1%}",
        ],
    }
