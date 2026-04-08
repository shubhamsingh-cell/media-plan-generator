"""
Budget Allocation Engine -- converts percentage allocations to concrete dollar amounts.

Takes the user's total budget, role/location details, synthesized market data,
and produces per-channel spend recommendations with projected outcomes
(clicks, applications, hires).

v3 upgrades:
    - Dynamic CPC benchmarks from trend_engine.py (when available)
    - Collar-weighted allocation via collar_intelligence.py
    - Trend engine CPC overrides via synthesized_data["trend_benchmarks"]
    - Structured confidence on channel allocations

This module prefers self-contained operation (stdlib imports) but will
optionally import trend_engine and collar_intelligence for dynamic benchmarks.
The caller (app.py or data_orchestrator.py) passes in any enrichment
data it has already fetched.
"""

import logging
import datetime
import math
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Optional v3 imports (dynamic benchmarks) ──
try:
    import trend_engine as _trend_engine

    _HAS_TREND_ENGINE = True
except ImportError:
    _HAS_TREND_ENGINE = False

try:
    import collar_intelligence as _collar_intel

    _HAS_COLLAR_INTEL = True
except ImportError:
    _HAS_COLLAR_INTEL = False

# ── Canonical taxonomy standardizer ──
# Used to normalize industry keys before CPH lookups.
# Falls back gracefully if unavailable.
try:
    from standardizer import (
        normalize_industry as _std_normalize_industry,
        CANONICAL_INDUSTRIES as _CANON_INDUSTRIES,
    )

    _HAS_STANDARDIZER = True
except ImportError:
    _HAS_STANDARDIZER = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Role tier CPA multipliers (relative to base Professional = 1.0)
ROLE_TIER_MULTIPLIERS: Dict[str, float] = {
    "Executive": 3.5,
    "Executive / Leadership": 3.5,
    "Professional": 1.8,
    "Professional / White-Collar": 1.8,
    "Clinical": 2.2,
    "Clinical / Licensed": 2.2,
    "Trades": 1.0,
    "Skilled Trades / Technical": 1.0,
    "Hourly": 0.7,
    "Hourly / Entry-Level": 0.7,
    "Gig": 0.5,
    "Gig / Independent Contractor": 0.5,
    "Education": 1.0,
    "Education / Academic": 1.0,
}

# NOTE: Canonical benchmark source is trend_engine.py. These values are fallbacks only.
# See trend_engine.get_benchmark() for authoritative CPC/CPA/CPM data with
# seasonal, regional, and collar-type adjustments.
# Base benchmarks (USD) -- overridden by live data when available.
# Keys map to *ad-platform categories* used in compute_channel_dollar_amounts.
# last_updated: 2026-Q1 (review quarterly -- see trend_engine.py for live benchmarks)
BASE_BENCHMARKS: Dict[str, Any] = {
    "cpc": {
        "job_board": 0.85,
        "social": 1.20,
        "search": 2.50,
        "programmatic": 0.65,
        "display": 0.45,
        "niche_board": 1.40,
        "employer_branding": 0.90,
        "referral": 0.00,  # referral programmes have no click cost
        "events": 0.00,  # events are flat-fee, not CPC
        "staffing": 0.00,  # agencies bill per placement
        "email": 0.35,
        "career_site": 0.30,
        "regional": 0.75,
    },
    "apply_rate": {
        "job_board": 0.08,
        "social": 0.03,
        "search": 0.05,
        "programmatic": 0.06,
        "display": 0.02,
        "niche_board": 0.10,
        "employer_branding": 0.04,
        "referral": 0.25,
        "events": 0.15,
        "staffing": 0.20,
        "email": 0.04,
        "career_site": 0.12,
        "regional": 0.07,
    },
    "hire_rate": 0.02,  # applications -> hires (default baseline)
}

# NOTE: Canonical benchmark source is trend_engine.py. These tier-specific
# hire rates supplement the base hire_rate above. See trend_engine.py for
# authoritative conversion funnel benchmarks.
# C4 FIX: Role-tier-specific hire rates instead of universal 2%
HIRE_RATE_BY_TIER: Dict[str, float] = {
    "Hourly / Entry-Level": 0.06,  # high-volume, lower bar
    "Skilled Trades / Technical": 0.04,  # CDL, warehouse, construction
    "Clinical / Licensed": 0.03,  # nurses, therapists — credentialing bottleneck
    "Professional / White-Collar": 0.02,  # standard corporate roles
    "Executive / Leadership": 0.008,  # highly selective
    "Technology / Engineering": 0.015,  # competitive market
    "Sales / Revenue": 0.035,  # high turnover, faster hiring
    "default": 0.02,
}

# Map user-facing channel names (from _INDUSTRY_ALLOC in app.py) to internal
# ad-platform category keys used in BASE_BENCHMARKS.
CHANNEL_NAME_TO_CATEGORY: Dict[str, str] = {
    # Canonical names from _INDUSTRY_ALLOC
    "programmatic_dsp": "programmatic",
    "global_boards": "job_board",
    "niche_boards": "niche_board",
    "social_media": "social",
    "regional_boards": "regional",
    "employer_branding": "employer_branding",
    "apac_regional": "regional",
    "emea_regional": "regional",
    # Friendly display names
    "Programmatic & DSP": "programmatic",
    "Global Job Boards": "job_board",
    "Niche & Industry Boards": "niche_board",
    "Social Media Channels": "social",
    "Regional & Local Boards": "regional",
    "Employer Branding": "employer_branding",
    "APAC Regional": "regional",
    "EMEA Regional": "regional",
    # Extended channel names
    "Job Boards": "job_board",
    "Social Media": "social",
    "Programmatic": "programmatic",
    "Search/SEM": "search",
    "Display/Banner": "display",
    "Career Sites": "career_site",
    "Referral Programs": "referral",
    "Recruitment Events": "events",
    "Staffing Agencies": "staffing",
    "Email Marketing": "email",
}

# Industry-average cost-per-hire ranges (USD).
# Mirrors the table in app.py lines 1657-1680.
INDUSTRY_CPH_RANGES: Dict[str, Tuple[float, float]] = {
    "healthcare_medical": (9_000, 12_000),
    "tech_engineering": (6_000, 14_000),
    "blue_collar_trades": (3_500, 5_600),
    "general_entry_level": (2_000, 4_700),
    "finance_banking": (5_000, 12_000),
    "retail_consumer": (2_700, 4_000),
    "pharma_biotech": (8_000, 18_000),
    "hospitality_travel": (2_500, 4_000),
    "logistics_supply_chain": (4_500, 8_000),
    "energy_utilities": (5_000, 10_000),
    "automotive": (5_600, 9_000),
    "insurance": (5_000, 10_000),
    "aerospace_defense": (6_000, 14_000),
    "education": (3_500, 6_000),
    "legal_services": (5_000, 11_000),
    "mental_health": (4_000, 8_000),
    "maritime_marine": (4_500, 9_000),
}
_DEFAULT_CPH_RANGE: Tuple[float, float] = (4_000, 8_000)

# Minimum viable budget per opening (USD).  Below this the plan is
# essentially unfundable for most channels.
_MIN_BUDGET_PER_OPENING: float = 200.0

# Industry-specific realistic minimum cost-per-hire thresholds
# Based on recruitment industry benchmarks
_INDUSTRY_MIN_CPH = {
    "technology": 4000,
    "healthcare": 3500,
    "finance": 4500,
    "engineering": 5000,
    "executive": 8000,
    "legal": 5000,
    "pharmaceutical": 6000,
    "energy": 4000,
    "aerospace": 5500,
    "manufacturing": 2500,
    "construction": 2000,
    "retail": 1200,
    "hospitality": 800,
    "logistics": 1500,
    "education": 2000,
    "government": 2500,
    "nonprofit": 1800,
    "general": 2000,
}

# S49: Per-channel minimum CPH floors (USD).
# Prevents unrealistically low cost-per-hire projections for individual
# channels.  E.g. Programmatic DSP at $0.80 CPC with 2% hire rate can
# mathematically produce $515/hire, but real-world DSP hires cost $800+.
_CHANNEL_MIN_CPH: Dict[str, float] = {
    "programmatic": 800,
    "social": 600,
    "search": 700,
    "display": 750,
    "job_board": 500,
    "niche_board": 600,
    "regional": 400,
    "career_site": 300,
    "employer_branding": 400,
    "referral": 200,
    "events": 500,
    "staffing": 1000,
    "email": 300,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Division that never raises ZeroDivisionError."""
    if denominator == 0:
        return default
    return numerator / denominator


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _resolve_tier(role: Dict) -> str:
    """Extract a canonical tier string from a role dict."""
    tier = role.get("tier") or "" or role.get("role_tier") or ""
    if isinstance(tier, dict):
        tier = tier.get("tier", "Professional")
    if not tier:
        tier = "Professional"
    return str(tier).strip()


def _tier_multiplier(tier: str) -> float:
    """Return the CPA multiplier for a tier string, with fuzzy fallback."""
    if tier in ROLE_TIER_MULTIPLIERS:
        return ROLE_TIER_MULTIPLIERS[tier]
    tier_lower = tier.lower()
    for key, val in ROLE_TIER_MULTIPLIERS.items():
        if key.lower() in tier_lower or tier_lower in key.lower():
            return val
    return 1.8  # default to Professional


def _category_for_channel(channel_name: str) -> str:
    """Map a channel name to its ad-platform category key."""
    if channel_name in CHANNEL_NAME_TO_CATEGORY:
        return CHANNEL_NAME_TO_CATEGORY[channel_name]
    # Fuzzy match
    name_lower = channel_name.lower()
    for key, cat in CHANNEL_NAME_TO_CATEGORY.items():
        if key.lower() in name_lower or name_lower in key.lower():
            return cat
    return "job_board"  # safe default


def _extract_cpc_from_synthesized(
    category: str,
    synthesized_data: Optional[Dict],
) -> Optional[float]:
    """
    Try to pull a real CPC value from enrichment / synthesized data.

    First checks the synthesized ``ad_platform_analysis`` (fused output
    with keys like "google", "meta", "linkedin", etc.).
    Then falls back to raw enrichment keys (``google_ads_data``,
    ``meta_ads_data``, etc.) when available.
    Returns None when no live data is available so the caller can fall back.
    """
    if not synthesized_data:
        return None

    candidate_cpcs: List[float] = []

    # --- Path 1: Synthesized ad_platform_analysis (preferred) ---
    ad_analysis = synthesized_data.get("ad_platform_analysis")
    if isinstance(ad_analysis, dict):
        # Map budget category -> synthesized platform keys
        synth_platform_keys: Dict[str, List[str]] = {
            "search": ["google", "bing"],
            "display": ["google"],
            "social": ["meta", "linkedin", "tiktok"],
            "programmatic": ["google"],
            "job_board": ["google"],
            "niche_board": ["google"],
            "regional": ["google"],
            "email": [],
            "career_site": [],
            "referral": [],
            "events": [],
            "staffing": [],
            "employer_branding": ["linkedin"],
        }
        for plat_key in synth_platform_keys.get(category, []):
            plat = ad_analysis.get(plat_key) or {}
            # Synthesized entries have avg_cpc (float)
            cpc_val = plat.get("avg_cpc") or 0
            if isinstance(cpc_val, (int, float)) and cpc_val > 0:
                candidate_cpcs.append(float(cpc_val))

    # --- Path 2: Raw enrichment keys (fallback) ---
    raw_keys: Dict[str, List[str]] = {
        "search": ["google_ads_data", "bing_ads_data"],
        "display": ["google_ads_data"],
        "social": ["meta_ads_data", "linkedin_ads_data", "tiktok_ads_data"],
        "programmatic": ["google_ads_data"],
        "job_board": ["google_ads_data"],
        "niche_board": ["google_ads_data"],
        "regional": ["google_ads_data"],
        "email": [],
        "career_site": [],
        "referral": [],
        "events": [],
        "staffing": [],
        "employer_branding": ["linkedin_ads_data"],
    }

    if not candidate_cpcs:
        for data_key in raw_keys.get(category, []):
            platform_data = synthesized_data.get(data_key) or {}
            # Per-role keyword data (Google Ads shape)
            keywords = platform_data.get("keywords") or {}
            for _role, kw_data in keywords.items():
                cpc_val = kw_data.get("avg_cpc_usd") or 0
                if isinstance(cpc_val, (int, float)) and cpc_val > 0:
                    candidate_cpcs.append(float(cpc_val))
            # Platform-level summary (Meta / LinkedIn shape)
            for sub_key in ("facebook", "instagram", "linkedin", "tiktok"):
                sub = platform_data.get(sub_key) or {}
                cpc_val = sub.get("avg_cpc_usd") or 0
                if isinstance(cpc_val, (int, float)) and cpc_val > 0:
                    candidate_cpcs.append(float(cpc_val))
            # Top-level avg_cpc_usd
            top_cpc = platform_data.get("avg_cpc_usd") or 0
            if isinstance(top_cpc, (int, float)) and top_cpc > 0:
                candidate_cpcs.append(float(top_cpc))

    if candidate_cpcs:
        return round(sum(candidate_cpcs) / len(candidate_cpcs), 2)
    return None


def _extract_cpc_from_kb(
    category: str,
    knowledge_base: Optional[Dict],
) -> Optional[float]:
    """
    Pull a CPC value from the recruitment_industry_knowledge.json structure.

    The KB stores values as strings like "$0.85" or ranges like "$0.25-$1.50".
    We parse and average them.
    """
    if not knowledge_base:
        return None

    benchmarks = knowledge_base.get("benchmarks", {})
    cpc_section = benchmarks.get("cost_per_click", {})
    by_platform = cpc_section.get("by_platform", {})

    # Map category to KB platform keys
    platform_map: Dict[str, List[str]] = {
        "search": ["google_search_ads"],
        "display": ["google_display_ads"],
        "social": ["meta_facebook_ads", "linkedin"],
        "job_board": ["indeed", "ziprecruiter"],
        "niche_board": ["linkedin"],
        "programmatic": ["google_display_ads"],
        "regional": ["indeed"],
        "employer_branding": ["linkedin"],
        "email": [],
        "career_site": [],
        "referral": [],
        "events": [],
        "staffing": [],
    }

    candidate_cpcs: List[float] = []
    for pkey in platform_map.get(category, []):
        pdata = by_platform.get(pkey, {})
        # Look for fields containing "cpc"
        for field_name, field_val in pdata.items():
            if "cpc" not in field_name.lower():
                continue
            parsed = _parse_dollar_value(field_val)
            if parsed is not None and parsed > 0:
                candidate_cpcs.append(parsed)

    if candidate_cpcs:
        return round(sum(candidate_cpcs) / len(candidate_cpcs), 2)
    return None


def _get_trend_engine_cpc(
    category: str,
    industry: str = "",
    collar_type: str = "both",
    location: str = "",
    month: int = 0,
) -> Optional[Tuple[float, Dict[str, Any]]]:
    """
    Fetch CPC from trend_engine.py with full context awareness.

    Returns (cpc_value, metadata_dict) or None if trend engine unavailable.
    The metadata dict includes trend_direction, trend_pct_yoy, seasonal_factor,
    confidence_interval, and data_confidence for downstream structured confidence.
    """
    if not _HAS_TREND_ENGINE:
        return None

    # Map budget category -> trend_engine platform key(s)
    _CATEGORY_TO_PLATFORMS: Dict[str, List[str]] = {
        "search": ["google"],
        "display": ["google"],
        "social": ["meta_fb", "meta_ig"],
        "programmatic": ["programmatic"],
        "job_board": ["indeed"],
        "niche_board": ["linkedin"],
        "regional": ["indeed"],
        "employer_branding": ["linkedin"],
        "email": [],
        "career_site": [],
        "referral": [],
        "events": [],
        "staffing": [],
    }

    platform_keys = _CATEGORY_TO_PLATFORMS.get(category, [])
    if not platform_keys:
        return None

    import datetime

    current_month = (
        month if (month and 1 <= month <= 12) else datetime.datetime.now().month
    )

    cpcs: List[float] = []
    best_meta: Dict[str, Any] = {}

    for plat_key in platform_keys:
        try:
            result = _trend_engine.get_benchmark(
                platform=plat_key,
                industry=industry or "general",
                metric="cpc",
                collar_type=collar_type,
                location=location,
                month=current_month,
            )
            if result and isinstance(result, dict):
                val = result.get("value") or 0
                if isinstance(val, (int, float)) and val > 0:
                    cpcs.append(float(val))
                    if not best_meta:
                        best_meta = {
                            "trend_direction": result.get("trend_direction", "stable"),
                            "trend_pct_yoy": result.get("trend_pct_yoy", 0.0),
                            "seasonal_factor": result.get("seasonal_factor", 1.0),
                            "confidence_interval": result.get("confidence_interval")
                            or [],
                            "data_confidence": result.get("data_confidence", 0.7),
                            "sources": result.get("sources", ["trend_engine"]),
                        }
        except Exception as e:
            logger.debug(
                "trend_engine.get_benchmark failed for %s/%s: %s", plat_key, category, e
            )

    if cpcs:
        avg_cpc = round(sum(cpcs) / len(cpcs), 2)
        return (avg_cpc, best_meta)
    return None


def _get_collar_apply_rate_adjustment(category: str, collar_type: str) -> float:
    """
    Return an apply-rate multiplier based on collar type.

    Blue collar roles have higher apply rates on job boards / programmatic
    but lower on LinkedIn. White collar is the inverse.
    """
    # Collar-specific apply rate multipliers by channel category
    _COLLAR_APPLY_MULT: Dict[str, Dict[str, float]] = {
        "blue_collar": {
            "job_board": 1.4,  # blue collar applies heavily on Indeed etc.
            "programmatic": 1.3,
            "social": 1.2,  # Facebook effective for blue collar
            "search": 0.8,
            "niche_board": 0.7,  # LinkedIn less relevant
            "display": 1.1,
            "regional": 1.3,
            "employer_branding": 0.6,
            "email": 0.8,
            "career_site": 1.2,
        },
        "white_collar": {
            "job_board": 0.9,
            "programmatic": 0.8,
            "social": 0.9,  # LinkedIn-heavy, FB less
            "search": 1.2,
            "niche_board": 1.4,  # LinkedIn premium for white collar
            "display": 0.7,
            "regional": 0.7,
            "employer_branding": 1.3,
            "email": 1.1,
            "career_site": 1.1,
        },
        "grey_collar": {
            "job_board": 1.2,
            "programmatic": 1.1,
            "social": 1.0,
            "search": 1.0,
            "niche_board": 1.1,
            "display": 0.9,
            "regional": 1.1,
            "employer_branding": 0.9,
            "email": 1.0,
            "career_site": 1.1,
        },
    }
    collar_mults = _COLLAR_APPLY_MULT.get(collar_type, {})
    return collar_mults.get(category, 1.0)


def _classify_roles_collar(roles_data: Dict[str, Dict], industry: str = "") -> str:
    """
    Determine dominant collar type from role budgets.

    Uses collar_intelligence if available, else falls back to tier heuristics.
    Returns 'blue_collar', 'white_collar', 'grey_collar', or 'both'.
    """
    if not _HAS_COLLAR_INTEL:
        # Fallback: use tier to guess collar type
        blue_count = 0
        white_count = 0
        for rb in roles_data.values():
            tier = rb.get("tier", "Professional")
            hc = rb.get("headcount", rb.get("openings", 1))
            if tier in (
                "Hourly / Entry-Level",
                "Skilled Trades / Technical",
                "Hourly",
                "Trades",
                "Gig",
                "Gig / Independent Contractor",
            ):
                blue_count += hc
            else:
                white_count += hc
        total = blue_count + white_count
        if total == 0:
            return "both"
        if blue_count / total > 0.65:
            return "blue_collar"
        if white_count / total > 0.65:
            return "white_collar"
        return "both"

    # Use collar_intelligence for proper classification
    collar_counts: Dict[str, int] = {}
    for role_title, rb in roles_data.items():
        hc = rb.get("headcount", rb.get("openings", 1))
        try:
            result = _collar_intel.classify_collar(
                role=role_title,
                industry=industry,
                soc_code=rb.get("soc_code") or "",
            )
            ct = result.get("collar_type", "white_collar")
        except Exception:
            ct = "white_collar"
        collar_counts[ct] = collar_counts.get(ct, 0) + hc

    total = sum(collar_counts.values()) or 1
    # Determine dominant
    dominant = max(collar_counts, key=collar_counts.get) if collar_counts else "both"
    dominant_pct = collar_counts.get(dominant, 0) / total
    if dominant_pct < 0.60:
        return "both"
    return dominant


def _parse_dollar_value(val: Any) -> Optional[float]:
    """Parse values like '$2.69', '$0.25-$1.50', or plain numbers."""
    if isinstance(val, (int, float)):
        return float(val)
    if not isinstance(val, str):
        return None
    import re

    cleaned = val.replace("$", "").replace(",", "").strip()
    # Range: take midpoint
    if "-" in cleaned:
        parts = cleaned.split("-")
        nums = []
        for p in parts:
            p = p.strip().rstrip("+")
            try:
                nums.append(float(p))
            except ValueError:
                pass
        if nums:
            return sum(nums) / len(nums)
        return None
    # Single value (may have trailing +)
    cleaned = cleaned.rstrip("+")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _industry_avg_cph(industry: str) -> float:
    """Return the midpoint cost-per-hire for an industry.

    Uses the canonical standardizer's ``deep_bench_key`` to map incoming
    industry strings to ``INDUSTRY_CPH_RANGES`` keys, with direct-match
    fallback.
    """
    # 1. Try direct match first (fast path for canonical keys)
    if industry in INDUSTRY_CPH_RANGES:
        low, high = INDUSTRY_CPH_RANGES[industry]
        return (low + high) / 2.0

    # 2. Try via standardizer -> deep_bench_key, then aliases
    if _HAS_STANDARDIZER:
        canonical = _std_normalize_industry(industry)
        meta = _CANON_INDUSTRIES.get(canonical, {})
        deep_key = meta.get("deep_bench_key") or ""
        if deep_key and deep_key in INDUSTRY_CPH_RANGES:
            low, high = INDUSTRY_CPH_RANGES[deep_key]
            return (low + high) / 2.0
        # deep_bench_key might not match CPH keys exactly;
        # scan aliases for a match in INDUSTRY_CPH_RANGES
        for alias in meta.get("aliases") or []:
            if alias in INDUSTRY_CPH_RANGES:
                low, high = INDUSTRY_CPH_RANGES[alias]
                return (low + high) / 2.0

    # 3. Fallback to default range
    low, high = _DEFAULT_CPH_RANGE
    return (low + high) / 2.0


def estimate_cph_from_salary(annual_salary: float) -> float:
    """Estimate Cost-Per-Hire from annual salary using the 4.4% rule.

    Normal staffing companies charge 10-20% of salary. Programmatic
    recruitment advertising (Joveo's model) is significantly cheaper.
    4.4% is a conservative estimate for total recruitment advertising
    cost per hire as a percentage of the role's annual salary.

    This is the DEFINITIVE LAST-RESORT fallback when no industry CPH
    benchmark, no Appcast data, and no client-specific data is available.

    Args:
        annual_salary: The annual salary for the role in USD.

    Returns:
        Estimated CPH in USD.
    """
    if annual_salary <= 0:
        return 4500.0  # absolute fallback
    return round(annual_salary * 0.044, 2)


def _score_roi(
    cost_per_hire: float, industry_avg: float, projected_hires: int = -1
) -> int:
    """
    Score ROI on a 1-10 scale.

    10 = cost_per_hire is <=20% of the industry average (exceptional).
     1 = cost_per_hire is >=3x the industry average (terrible).

    S49 FIX (Issue 16): If projected_hires == 0, the channel produces no
    hires so ROI is capped at 2 regardless of cost_per_hire ratio.
    A channel spending money with zero projected hires is low-efficiency.

    S50 FIX 3: If CPH > 5x industry average, cap ROI at 3/10.
    A channel with $10K CPH against a $2K industry avg should NOT score 8/10.
    """
    if projected_hires == 0:
        return 1  # zero hires = worst ROI regardless of spend
    if industry_avg <= 0 or cost_per_hire <= 0:
        return 5  # unknown
    ratio = cost_per_hire / industry_avg

    # S50 FIX 3: Hard cap -- absurdly expensive channels cannot score well
    if ratio >= 5.0:
        return min(3, int(_clamp(round(10 - (ratio - 0.2) * (9 / 2.8)), 1, 3)))

    # Linear mapping: ratio 0.2 -> 10, ratio 3.0 -> 1
    score = 10 - (ratio - 0.2) * (9 / 2.8)
    return int(_clamp(round(score), 1, 10))


# ---------------------------------------------------------------------------
# Core public functions
# ---------------------------------------------------------------------------


def compute_location_cost_multipliers(
    locations: List[Dict],
    synthesized_data: Optional[Dict] = None,
) -> Dict[str, float]:
    """
    Compute cost-of-living multipliers for each location.

    Uses Teleport COLI data from synthesized location profiles when available.
    Base = 1.0 (US national average).
    Examples: SF/NYC ~ 1.4, Rural US ~ 0.7, India ~ 0.3.

    Args:
        locations: List of location dicts.  Each dict should have at least
                   one of ``city``, ``location``, or ``name`` as a key.
        synthesized_data: The full enrichment payload (may contain
                          ``teleport_data`` with per-city cost_of_living).

    Returns:
        Dict mapping ``location_key`` -> float multiplier.
        Always contains at least one entry; returns ``{"default": 1.0}``
        when no locations are supplied.
    """
    if not locations:
        logger.debug("No locations provided; returning default multiplier 1.0")
        return {"default": 1.0}

    multipliers: Dict[str, float] = {}
    # Try raw teleport_data first, then synthesized location_profiles
    teleport = (synthesized_data or {}).get("teleport_data", {})
    teleport_cities = teleport.get("cities", {}) if isinstance(teleport, dict) else {}
    # Also check synthesized location_profiles for COLI data
    loc_profiles = (synthesized_data or {}).get("location_profiles", {})
    if isinstance(loc_profiles, dict):
        for _lp_key, _lp_val in loc_profiles.items():
            if isinstance(_lp_val, dict) and "cost_of_living_index" in _lp_val:
                if _lp_key not in teleport_cities:
                    teleport_cities[_lp_key] = {
                        "cost_of_living": {
                            "cost_of_living_index": _lp_val["cost_of_living_index"]
                        },
                        "quality_scores": {
                            "Cost of Living": _lp_val.get("quality_of_life_score") or 0
                        },
                    }

    # Known fallback multipliers for major metro areas (relative to US avg)
    _FALLBACK_MULTIPLIERS: Dict[str, float] = {
        "san francisco": 1.45,
        "new york": 1.40,
        "manhattan": 1.45,
        "los angeles": 1.25,
        "boston": 1.30,
        "seattle": 1.25,
        "chicago": 1.10,
        "austin": 1.05,
        "denver": 1.10,
        "dallas": 0.95,
        "houston": 0.90,
        "atlanta": 0.95,
        "miami": 1.10,
        "phoenix": 0.90,
        "detroit": 0.85,
        "minneapolis": 0.95,
        "philadelphia": 1.05,
        "washington": 1.25,
        "portland": 1.10,
        "san diego": 1.20,
        "nashville": 0.95,
        "charlotte": 0.90,
        "london": 1.35,
        "munich": 1.20,
        "zurich": 1.55,
        "paris": 1.25,
        "amsterdam": 1.15,
        "dublin": 1.15,
        "singapore": 1.30,
        "tokyo": 1.25,
        "sydney": 1.20,
        "toronto": 1.10,
        "vancouver": 1.15,
        "bangalore": 0.30,
        "mumbai": 0.35,
        "delhi": 0.30,
        "hyderabad": 0.28,
        "manila": 0.30,
        "lagos": 0.25,
    }

    for loc in locations:
        loc_key = _location_key(loc)
        if not loc_key:
            continue

        # 1. Try Teleport enrichment data
        teleport_entry = _find_teleport_entry(loc_key, teleport_cities)
        if teleport_entry:
            col = teleport_entry.get("cost_of_living", {})
            # Teleport provides absolute dollar amounts for rent, groceries, etc.
            # We derive a relative multiplier from the "Cost of Living Index"
            # score (Teleport quality_scores -> "Cost of Living" is 1-10 where
            # 10 = very affordable, 1 = very expensive).
            qs = teleport_entry.get("quality_scores", {})
            col_score = qs.get("Cost of Living") or 0
            if col_score > 0:
                # Convert: score 10 -> multiplier ~0.6, score 1 -> multiplier ~1.5
                mult = round(1.55 - (col_score / 10.0) * 0.95, 2)
                multipliers[loc_key] = _clamp(mult, 0.2, 2.5)
                logger.debug(
                    "Location %s: Teleport COLI score %.1f -> multiplier %.2f",
                    loc_key,
                    col_score,
                    multipliers[loc_key],
                )
                continue

        # 2. Fallback: known city lookup
        city_lower = loc_key.split(",")[0].strip().lower()
        if city_lower in _FALLBACK_MULTIPLIERS:
            multipliers[loc_key] = _FALLBACK_MULTIPLIERS[city_lower]
            logger.debug(
                "Location %s: fallback multiplier %.2f", loc_key, multipliers[loc_key]
            )
            continue

        # 3. Country-level heuristic
        country = _guess_country(loc)
        if country:
            country_mult = _country_multiplier(country)
            multipliers[loc_key] = country_mult
            logger.debug(
                "Location %s: country-level multiplier %.2f", loc_key, country_mult
            )
            continue

        # 4. Default
        multipliers[loc_key] = 1.0

    if not multipliers:
        multipliers["default"] = 1.0

    return multipliers


def compute_role_weighted_spend(
    roles: List[Dict],
    total_budget: float,
    location_multipliers: Dict[str, float],
) -> Dict[str, Dict]:
    """
    Weight budget across roles by tier multiplier and opening count.

    Executive roles receive a 3.5x weight, Hourly receives 0.7x, etc.
    More openings for a role = proportionally more budget.

    Args:
        roles: List of role dicts.  Each should have ``title`` (str),
               optionally ``count``/``openings`` (int, default 1), and
               ``tier`` (str, default "Professional").
        total_budget: Total campaign budget in USD.
        location_multipliers: Output of ``compute_location_cost_multipliers``.

    Returns:
        Dict keyed by role title, each value a dict with:
        ``budget_share`` (0-1), ``dollar_amount``, ``tier``, ``multiplier``,
        ``openings``.
    """
    if not roles:
        logger.warning("No roles provided; assigning entire budget to default role")
        return {
            "General": {
                "budget_share": 1.0,
                "dollar_amount": round(total_budget, 2),
                "tier": "Professional",
                "multiplier": 1.8,
                "openings": 1,
            }
        }

    if total_budget <= 0:
        logger.warning(
            "Budget is zero or negative (%.2f); returning zero allocations",
            total_budget,
        )
        result: Dict[str, Dict] = {}
        for role in roles:
            title = role.get("title", "Unknown Role")
            result[title] = {
                "budget_share": 0.0,
                "dollar_amount": 0.0,
                "tier": _resolve_tier(role),
                "multiplier": _tier_multiplier(_resolve_tier(role)),
                "openings": max(
                    1, int(role.get("count", role.get("openings", 1)) or 1)
                ),
            }
        return result

    # Compute average location multiplier (used to scale costs globally)
    avg_loc_mult = (
        sum(location_multipliers.values()) / len(location_multipliers)
        if location_multipliers
        else 1.0
    )

    # Build weighted scores
    weighted_scores: List[Tuple[str, float, Dict]] = []
    total_weight = 0.0
    for role in roles:
        title = role.get("title", "Unknown Role")
        openings = max(1, int(role.get("count", role.get("openings", 1)) or 1))
        tier = _resolve_tier(role)
        mult = _tier_multiplier(tier)
        weight = mult * openings * avg_loc_mult
        weighted_scores.append((title, weight, role))
        total_weight += weight

    result = {}
    for title, weight, role_dict in weighted_scores:
        share = _safe_divide(weight, total_weight, 0.0)
        tier = _resolve_tier(role_dict)
        openings = max(
            1, int(role_dict.get("count", role_dict.get("openings", 1)) or 1)
        )
        result[title] = {
            "budget_share": round(share, 4),
            "dollar_amount": round(total_budget * share, 2),
            "tier": tier,
            "multiplier": _tier_multiplier(tier),
            "openings": openings,
            "headcount": openings,  # alias for hire_rate blending
        }

    logger.info(
        "Role-weighted spend computed for %d roles, total $%.2f",
        len(result),
        total_budget,
    )
    return result


# ---------------------------------------------------------------------------
# Dynamic Budget Allocation Scoring Engine
# ---------------------------------------------------------------------------

# Channel scoring weights by factor
_CHANNEL_BASE_SCORES: Dict[str, Dict[str, float]] = {
    # channel_key: {factor: base_score 0-100}
    # S50 FIX 4: Rebalanced programmatic vs niche scores.
    # Previous: programmatic blue_collar=85, entry=90 -> ~40% budget share.
    # Niche blue_collar=40, entry=30 -> ~8% budget share.
    # Fix: Programmatic still leads for blue-collar volume but with narrower
    # gap so niche boards get proportional allocation (~12-15% vs ~25-30%).
    "programmatic_dsp": {
        "blue_collar": 75,  # was 85: good for volume but not dominant
        "white_collar": 55,
        "grey_collar": 65,
        "tier_1": 65,
        "tier_2": 75,
        "tier_3": 85,
        "entry": 78,  # was 90: still strong for entry but not overwhelming
        "mid": 68,
        "senior": 40,
        "exec": 15,
        "small_budget": 75,
        "medium_budget": 80,
        "large_budget": 72,
        "high_competition": 75,
        "medium_competition": 70,
        "low_competition": 60,
    },
    "global_boards": {
        "blue_collar": 72,
        "white_collar": 75,
        "grey_collar": 73,
        "tier_1": 80,
        "tier_2": 72,
        "tier_3": 58,
        "entry": 78,
        "mid": 80,
        "senior": 65,
        "exec": 40,
        "small_budget": 72,
        "medium_budget": 76,
        "large_budget": 72,
        "high_competition": 72,
        "medium_competition": 73,
        "low_competition": 75,
    },
    "niche_boards": {
        "blue_collar": 50,  # was 40: niche boards (Indeed industry, CDL boards) matter
        "white_collar": 85,
        "grey_collar": 68,
        "tier_1": 80,
        "tier_2": 72,
        "tier_3": 55,
        "entry": 45,  # was 30: entry-level niche boards (Craigslist, local) still relevant
        "mid": 72,
        "senior": 90,
        "exec": 85,
        "small_budget": 90,
        "medium_budget": 80,
        "large_budget": 70,
        "high_competition": 90,
        "medium_competition": 75,
        "low_competition": 60,
    },
    "social_media": {
        "blue_collar": 55,
        "white_collar": 80,
        "grey_collar": 68,
        "tier_1": 85,
        "tier_2": 70,
        "tier_3": 50,
        "entry": 85,
        "mid": 75,
        "senior": 70,
        "exec": 60,
        "small_budget": 60,
        "medium_budget": 75,
        "large_budget": 85,
        "high_competition": 85,
        "medium_competition": 70,
        "low_competition": 55,
    },
    "regional_boards": {
        "blue_collar": 90,
        "white_collar": 40,
        "grey_collar": 65,
        "tier_1": 30,
        "tier_2": 70,
        "tier_3": 95,
        "entry": 85,
        "mid": 60,
        "senior": 30,
        "exec": 15,
        "small_budget": 85,
        "medium_budget": 70,
        "large_budget": 55,
        "high_competition": 60,
        "medium_competition": 70,
        "low_competition": 80,
    },
    "employer_branding": {
        "blue_collar": 25,
        "white_collar": 80,
        "grey_collar": 55,
        "tier_1": 85,
        "tier_2": 65,
        "tier_3": 35,
        "entry": 20,
        "mid": 50,
        "senior": 85,
        "exec": 95,
        "small_budget": 15,
        "medium_budget": 50,
        "large_budget": 85,
        "high_competition": 90,
        "medium_competition": 65,
        "low_competition": 35,
    },
    "apac_regional": {
        "blue_collar": 50,
        "white_collar": 55,
        "grey_collar": 52,
        "tier_1": 40,
        "tier_2": 50,
        "tier_3": 30,
        "entry": 45,
        "mid": 50,
        "senior": 40,
        "exec": 30,
        "small_budget": 20,
        "medium_budget": 35,
        "large_budget": 50,
        "high_competition": 40,
        "medium_competition": 35,
        "low_competition": 30,
    },
    "emea_regional": {
        "blue_collar": 45,
        "white_collar": 55,
        "grey_collar": 50,
        "tier_1": 40,
        "tier_2": 50,
        "tier_3": 30,
        "entry": 40,
        "mid": 50,
        "senior": 40,
        "exec": 30,
        "small_budget": 15,
        "medium_budget": 30,
        "large_budget": 50,
        "high_competition": 40,
        "medium_competition": 35,
        "low_competition": 30,
    },
}

# Factor weights (must sum to 1.0)
_FACTOR_WEIGHTS: Dict[str, float] = {
    "collar": 0.25,
    "metro": 0.20,
    "seniority": 0.25,
    "competition": 0.15,
    "budget_size": 0.15,
}

# Minimum allocation percentage per channel (prevent channels from zeroing out)
_MIN_ALLOC_PCT: float = 1.0
# Maximum allocation percentage per channel (prevent over-concentration)
_MAX_ALLOC_PCT: float = 50.0


def _classify_metro_tier(locations: List[str]) -> str:
    """Classify location list into metro tier for budget scoring.

    Tier 1: Major metros (NYC, SF, LA, Chicago, Boston, Seattle, DC, etc.)
    Tier 2: Mid-size cities (Denver, Austin, Nashville, Raleigh, etc.)
    Tier 3: Smaller markets and rural areas

    Args:
        locations: List of location strings from user input.

    Returns:
        One of 'tier_1', 'tier_2', or 'tier_3'.
    """
    if not locations:
        return "tier_2"  # default to mid-tier

    combined = " ".join(loc.lower() for loc in locations)

    tier_1_markers = [
        "new york",
        "nyc",
        "manhattan",
        "brooklyn",
        "san francisco",
        "sf bay",
        "los angeles",
        "la ",
        "chicago",
        "boston",
        "seattle",
        "washington dc",
        "dc metro",
        "miami",
        "dallas",
        "houston",
        "atlanta",
        "philadelphia",
        "denver",
        "phoenix",
        "san diego",
        "san jose",
        "silicon valley",
        "london",
        "paris",
        "tokyo",
        "singapore",
        "sydney",
        "toronto",
        "mumbai",
        "bangalore",
        "berlin",
        "amsterdam",
    ]
    tier_2_markers = [
        "austin",
        "nashville",
        "raleigh",
        "charlotte",
        "portland",
        "minneapolis",
        "salt lake",
        "tampa",
        "orlando",
        "pittsburgh",
        "indianapolis",
        "columbus",
        "kansas city",
        "st. louis",
        "cincinnati",
        "milwaukee",
        "sacramento",
        "richmond",
        "jacksonville",
        "memphis",
        "san antonio",
        "birmingham",
        "manchester",
        "dublin",
        "melbourne",
        "calgary",
        "vancouver",
        "munich",
        "barcelona",
    ]

    for marker in tier_1_markers:
        if marker in combined:
            return "tier_1"

    for marker in tier_2_markers:
        if marker in combined:
            return "tier_2"

    return "tier_3"


def _classify_seniority(roles: List[str], role_tiers: Optional[Dict] = None) -> str:
    """Classify dominant seniority level from role titles or tier data.

    Args:
        roles: List of role title strings.
        role_tiers: Optional dict of role -> tier info from classify_tier_fn.

    Returns:
        One of 'entry', 'mid', 'senior', or 'exec'.
    """
    if role_tiers:
        tier_counts: Dict[str, int] = {}
        for role, tier_info in role_tiers.items():
            tier = (
                tier_info.get("tier", "Professional")
                if isinstance(tier_info, dict)
                else str(tier_info)
            )
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        exec_tiers = {"Executive", "Executive / Leadership"}
        senior_tiers = {
            "Professional",
            "Professional / White-Collar",
            "Clinical",
            "Clinical / Licensed",
        }
        entry_tiers = {
            "Hourly",
            "Hourly / Entry-Level",
            "Gig",
            "Gig / Independent Contractor",
        }

        exec_count = sum(tier_counts.get(t, 0) for t in exec_tiers)
        senior_count = sum(tier_counts.get(t, 0) for t in senior_tiers)
        entry_count = sum(tier_counts.get(t, 0) for t in entry_tiers)
        total = max(exec_count + senior_count + entry_count, 1)

        if exec_count / total > 0.3:
            return "exec"
        if senior_count / total > 0.5:
            return "senior"
        if entry_count / total > 0.5:
            return "entry"
        return "mid"

    if not roles:
        return "mid"

    combined = " ".join(r.lower() for r in roles)
    exec_keywords = [
        "ceo",
        "cfo",
        "cto",
        "coo",
        "vp ",
        "vice president",
        "chief",
        "president",
        "director",
        "partner",
        "principal",
    ]
    senior_keywords = ["senior", "lead", "staff", "architect", "manager", "head of"]
    entry_keywords = [
        "intern",
        "junior",
        "entry",
        "associate",
        "assistant",
        "trainee",
        "apprentice",
        "hourly",
        "part-time",
    ]

    if any(kw in combined for kw in exec_keywords):
        return "exec"
    if any(kw in combined for kw in senior_keywords):
        return "senior"
    if any(kw in combined for kw in entry_keywords):
        return "entry"
    return "mid"


def _classify_competition_level(
    synthesized_data: Optional[Dict] = None,
    industry: str = "",
) -> str:
    """Classify competition level from enrichment data.

    Args:
        synthesized_data: Enrichment payload with market data.
        industry: Industry key for fallback classification.

    Returns:
        One of 'high_competition', 'medium_competition', or 'low_competition'.
    """
    if synthesized_data:
        # Check labor market data for competition signals
        labor = synthesized_data.get("labor_market", {})
        if isinstance(labor, dict):
            unemployment = labor.get("unemployment_rate")
            if isinstance(unemployment, (int, float)):
                if unemployment < 3.5:
                    return "high_competition"
                if unemployment > 5.5:
                    return "low_competition"
                return "medium_competition"

        # Check competition data
        competition = synthesized_data.get("competition_analysis", {})
        if isinstance(competition, dict):
            level = (competition.get("competition_level") or "").lower()
            if "high" in level:
                return "high_competition"
            if "low" in level:
                return "low_competition"

    # Industry-based fallback
    high_comp_industries = {
        "tech_engineering",
        "healthcare_medical",
        "pharma_biotech",
        "aerospace_defense",
        "finance_banking",
    }
    low_comp_industries = {
        "hospitality_travel",
        "retail_consumer",
        "general_entry_level",
    }
    if industry in high_comp_industries:
        return "high_competition"
    if industry in low_comp_industries:
        return "low_competition"
    return "medium_competition"


def _classify_budget_size(total_budget: float) -> str:
    """Classify budget into small/medium/large for scoring.

    Args:
        total_budget: Total campaign budget in USD.

    Returns:
        One of 'small_budget', 'medium_budget', or 'large_budget'.
    """
    if total_budget < 5000:
        return "small_budget"
    if total_budget > 50000:
        return "large_budget"
    return "medium_budget"


def compute_dynamic_allocation(
    collar_type: str = "",
    locations: Optional[List[str]] = None,
    roles: Optional[List[str]] = None,
    role_tiers: Optional[Dict] = None,
    total_budget: float = 0.0,
    industry: str = "",
    synthesized_data: Optional[Dict] = None,
    static_fallback: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    """Compute dynamic channel allocation percentages based on multi-factor scoring.

    Each channel receives a weighted score (0-100) based on:
    - Collar type (blue/white/grey) -- affects channel mix
    - Metro tier (Tier 1 NYC/SF vs Tier 2 vs Tier 3 rural) -- affects digital vs traditional
    - Role seniority (entry/mid/senior/exec) -- affects LinkedIn weight
    - Competition level (from enrichment data if available)
    - Budget size (small <$5K favors focused channels, large >$50K can diversify)

    Scores are normalized to percentages for allocation.
    Falls back to static profiles when dynamic data is insufficient.

    Args:
        collar_type: One of 'blue_collar', 'white_collar', 'grey_collar', 'both'.
        locations: List of location strings for metro tier classification.
        roles: List of role title strings.
        role_tiers: Optional dict of role -> tier info.
        total_budget: Total campaign budget in USD.
        industry: Industry classification key.
        synthesized_data: Enrichment payload for competition analysis.
        static_fallback: Static percentage profile to use if scoring fails.

    Returns:
        Dict mapping channel names to allocation percentages (sum to ~100).
    """
    try:
        # Classify factors
        collar_key = (
            collar_type.lower().replace("-", "_").replace(" ", "_")
            if collar_type
            else "white_collar"
        )
        if collar_key == "both":
            collar_key = "grey_collar"
        if collar_key not in ("blue_collar", "white_collar", "grey_collar"):
            collar_key = "white_collar"

        metro_tier = _classify_metro_tier(locations or [])
        seniority = _classify_seniority(roles or [], role_tiers)
        competition = _classify_competition_level(synthesized_data, industry)
        budget_size = _classify_budget_size(total_budget)

        logger.info(
            "Dynamic allocation factors: collar=%s, metro=%s, seniority=%s, "
            "competition=%s, budget=%s",
            collar_key,
            metro_tier,
            seniority,
            competition,
            budget_size,
        )

        # Score each channel
        channel_scores: Dict[str, float] = {}

        for channel, scores in _CHANNEL_BASE_SCORES.items():
            weighted_score = 0.0

            # Collar factor
            collar_score = scores.get(collar_key, 50.0)
            weighted_score += collar_score * _FACTOR_WEIGHTS["collar"]

            # Metro tier factor
            metro_score = scores.get(metro_tier, 50.0)
            weighted_score += metro_score * _FACTOR_WEIGHTS["metro"]

            # Seniority factor
            seniority_score = scores.get(seniority, 50.0)
            weighted_score += seniority_score * _FACTOR_WEIGHTS["seniority"]

            # Competition factor
            comp_score = scores.get(competition, 50.0)
            weighted_score += comp_score * _FACTOR_WEIGHTS["competition"]

            # Budget size factor
            budget_score = scores.get(budget_size, 50.0)
            weighted_score += budget_score * _FACTOR_WEIGHTS["budget_size"]

            channel_scores[channel] = round(weighted_score, 2)

        # Normalize scores to percentages
        total_score = sum(channel_scores.values())
        if total_score <= 0:
            logger.warning(
                "All channel scores are zero; falling back to static profiles"
            )
            return dict(static_fallback) if static_fallback else {}

        raw_pcts: Dict[str, float] = {}
        for channel, score in channel_scores.items():
            pct = (score / total_score) * 100.0
            # Apply min/max clamps
            pct = max(_MIN_ALLOC_PCT, min(_MAX_ALLOC_PCT, pct))
            raw_pcts[channel] = pct

        # Re-normalize after clamping to ensure sum is exactly 100
        pct_sum = sum(raw_pcts.values())
        allocation: Dict[str, float] = {}
        for channel, pct in raw_pcts.items():
            allocation[channel] = round((pct / pct_sum) * 100.0, 1)

        # Validate: ensure sum is close to 100 (adjust largest channel for rounding)
        alloc_sum = sum(allocation.values())
        if abs(alloc_sum - 100.0) > 0.5:
            largest_ch = max(allocation, key=allocation.get)
            allocation[largest_ch] = round(
                allocation[largest_ch] + (100.0 - alloc_sum), 1
            )

        logger.info(
            "Dynamic allocation computed: %s (total=%.1f%%)",
            {k: f"{v:.1f}%" for k, v in allocation.items()},
            sum(allocation.values()),
        )
        return allocation

    except Exception as exc:
        logger.error(
            "Dynamic allocation scoring failed, falling back to static: %s",
            exc,
            exc_info=True,
        )
        if static_fallback:
            return dict(static_fallback)
        return {}


def compute_channel_dollar_amounts(
    channel_percentages: Dict[str, float],
    role_budgets: Dict[str, Dict],
    synthesized_data: Optional[Dict] = None,
    knowledge_base: Optional[Dict] = None,
    industry: str = "",
    collar_type: str = "",
    location: str = "",
    month: int = 0,
) -> Dict[str, Dict]:
    """
    Convert channel percentages to dollar amounts with projected outcomes.

    For each channel:
        1. Dollar amount = total_role_budget * (pct / 100)
        2. CPC from synthesized data, trend_engine, KB benchmarks, or BASE_BENCHMARKS
        3. Projected clicks = dollars / CPC
        4. Projected applications = clicks * apply_rate (collar-adjusted)
        5. Projected hires = applications * hire_rate
        6. Effective CPA = dollars / applications
        7. Effective cost_per_hire = dollars / hires
        8. ROI score (1-10) against industry average

    v3 upgrades:
        - trend_engine CPC layer between synthesized and KB
        - Collar-aware apply rate adjustments
        - Trend metadata (direction, YoY%) on each channel
        - Structured confidence with sources list

    Args:
        channel_percentages: ``{channel_name: percentage}`` (0-100).
        role_budgets: Output of ``compute_role_weighted_spend``.
        synthesized_data: Enrichment payload (optional).
        knowledge_base: Loaded knowledge base JSON (optional).
        industry: Industry string for trend engine lookups (v3).
        collar_type: Collar type for apply rate adjustments (v3).
        location: Primary location for regional CPC adjustments (v3).

    Returns:
        Dict keyed by channel name, each value a dict with:
        ``dollars``, ``cpc``, ``projected_clicks``, ``projected_applications``,
        ``projected_hires``, ``cpa``, ``cost_per_hire``, ``roi_score``,
        ``confidence``, ``category``, and v3 fields: ``cpc_source``,
        ``trend_direction``, ``trend_pct_yoy``, ``apply_rate_collar_adjusted``.
    """
    total_budget = sum(rb.get("dollar_amount") or 0 for rb in role_budgets.values())
    if total_budget <= 0:
        logger.warning("Total role budget is zero; returning empty channel allocations")
        return {}

    # Normalise percentages to sum to 100
    pct_sum = sum(channel_percentages.values())
    if pct_sum <= 0:
        logger.warning("Channel percentages sum to zero; returning empty allocations")
        return {}
    norm_factor = 100.0 / pct_sum

    # C4 FIX: Compute blended hire rate from role tiers instead of flat 2%
    _tier_counts: Dict[str, int] = {}
    for rb in role_budgets.values():
        tier = rb.get("tier", "default")
        _tier_counts[tier] = _tier_counts.get(tier, 0) + rb.get("headcount", 1)
    _total_hc = max(sum(_tier_counts.values()), 1)
    hire_rate = sum(
        HIRE_RATE_BY_TIER.get(tier, HIRE_RATE_BY_TIER["default"]) * count / _total_hc
        for tier, count in _tier_counts.items()
    )
    logger.info("Blended hire_rate=%.4f from tiers: %s", hire_rate, _tier_counts)
    industry_avg_cph = (
        6_000.0  # fallback; caller can override via assess_budget_sufficiency
    )

    # v3: Determine collar type from roles if not explicitly provided
    effective_collar = collar_type
    if not effective_collar:
        effective_collar = _classify_roles_collar(role_budgets, industry)

    allocations: Dict[str, Dict] = {}
    for ch_name, raw_pct in channel_percentages.items():
        pct = raw_pct * norm_factor
        dollars = round(total_budget * pct / 100.0, 2)
        category = _category_for_channel(ch_name)

        # ── CPC Resolution Cascade (v3): synthesized > trend_engine > KB > static ──
        cpc = _extract_cpc_from_synthesized(category, synthesized_data)
        cpc_source = "synthesized"
        confidence = "high"
        trend_meta: Dict[str, Any] = {}

        if cpc is None:
            # v3: Try trend_engine with full context
            te_result = _get_trend_engine_cpc(
                category,
                industry=industry,
                collar_type=effective_collar,
                location=location,
                month=month,
            )
            if te_result is not None:
                cpc, trend_meta = te_result
                cpc_source = "trend_engine"
                confidence = "high"

        if cpc is None:
            cpc = _extract_cpc_from_kb(category, knowledge_base)
            if cpc is not None:
                cpc_source = "knowledge_base"
                confidence = "medium"

        if cpc is None:
            cpc = BASE_BENCHMARKS["cpc"].get(category, 0.85)
            cpc_source = "static_benchmark"
            confidence = "low"

        # ── Apply Rate with Collar Adjustment (v3) ──
        base_apply_rate = BASE_BENCHMARKS["apply_rate"].get(category, 0.05)
        collar_mult = 1.0
        if effective_collar and effective_collar != "both":
            collar_mult = _get_collar_apply_rate_adjustment(category, effective_collar)
        apply_rate_adj = round(base_apply_rate * collar_mult, 4)

        # For channels without a CPC model (referral, events, staffing),
        # we estimate outcomes differently.
        if cpc <= 0:
            # Flat-cost channels: estimate a synthetic CPA instead
            projected_clicks = 0
            projected_applications = max(
                1, int(dollars / 50.0)
            )  # ~$50/application heuristic
            projected_hires = max(
                0, int(projected_applications * hire_rate * 2)
            )  # higher quality
            cpa = _safe_divide(dollars, projected_applications, dollars)
            cost_per_hire = _safe_divide(dollars, max(projected_hires, 1), dollars)
        else:
            projected_clicks = max(0, int(dollars / cpc))
            projected_applications = max(0, int(projected_clicks * apply_rate_adj))
            projected_hires = max(0, int(projected_applications * hire_rate))
            cpa = _safe_divide(dollars, max(projected_applications, 1), dollars)
            cost_per_hire = _safe_divide(dollars, max(projected_hires, 1), dollars)

        # S49 FIX (Issue 9): Enforce per-channel minimum CPH floor.
        # Prevents unrealistically low hire projections (e.g. Programmatic DSP
        # at $515/hire when real-world floor is $800).  Cap projected_hires so
        # that cost_per_hire >= channel minimum.
        _ch_min_cph = _CHANNEL_MIN_CPH.get(category, 0)
        if _ch_min_cph > 0 and dollars > 0 and projected_hires > 0:
            max_hires_at_floor = int(dollars / _ch_min_cph)
            if projected_hires > max_hires_at_floor:
                projected_hires = max(max_hires_at_floor, 0)
                cost_per_hire = _safe_divide(dollars, max(projected_hires, 1), dollars)

        # S39/S46/S48: Platform-differentiated safety margins for CPH/CPA
        # Margins reflect data quality and variability per platform.
        # Platform-specific margins override category defaults.
        _PLATFORM_SAFETY_MARGINS = {
            # High data quality platforms -> lower margin
            "indeed": 1.20,
            "linkedin": 1.20,
            "ziprecruiter": 1.20,
            "glassdoor": 1.20,
            # Moderate variability
            "programmatic": 1.30,
            "google": 1.25,
            "microsoft": 1.25,
            # Niche boards -> less data
            "niche": 1.40,
            "careerbuilder": 1.40,
            "diversity": 1.40,
            # Social media -> high variability
            "meta": 1.45,
            "facebook": 1.45,
            "instagram": 1.45,
            "tiktok": 1.45,
            "snapchat": 1.45,
            "twitter": 1.45,
            # Craigslist -> variable
            "craigslist": 1.35,
        }
        # Category-level fallbacks when platform name not matched
        _CATEGORY_SAFETY_MARGINS = {
            "job_board": 1.20,  # Indeed/ZipRecruiter level
            "social": 1.45,  # Meta/TikTok level
            "programmatic": 1.30,
            "search": 1.25,
            "niche_board": 1.40,
            "display": 1.35,
            "employer_branding": 1.20,
            "regional": 1.35,
        }
        # Try platform-specific margin first, fall back to category
        _ch_lower = ch_name.lower() if ch_name else ""
        _margin = 1.0
        for _plat_key, _plat_margin in _PLATFORM_SAFETY_MARGINS.items():
            if _plat_key in _ch_lower:
                _margin = _plat_margin
                break
        if _margin == 1.0:
            _margin = _CATEGORY_SAFETY_MARGINS.get(category, 1.0)
        if _margin > 1.0:
            # S49 FIX: Adjust projections DOWN so CPA/CPH math is self-consistent.
            # Previously: cpa *= _margin (inflated CPA but left applications unchanged,
            # causing 20-45% discrepancy when users verify dollars / applications != CPA).
            # Now: reduce projected_applications and projected_hires by the margin factor
            # so that dollars / adjusted_applications naturally yields the safety-buffered CPA.
            projected_applications = max(1, int(projected_applications / _margin))
            projected_hires = max(0, int(projected_hires / _margin))
            # Recompute CPA and CPH from adjusted projections
            cpa = _safe_divide(dollars, max(projected_applications, 1), dollars)
            cost_per_hire = _safe_divide(dollars, max(projected_hires, 1), dollars)

        # S49 FIX (Issue 16): Pass projected_hires so zero-hire channels
        # get appropriately low ROI scores instead of inflated ones.
        roi = _score_roi(
            cost_per_hire, industry_avg_cph, projected_hires=projected_hires
        )

        # S49 FIX (Issue 16): Flag channels spending >$1000 with 0 hires
        _efficiency_flag = ""
        if projected_hires == 0 and dollars > 1000:
            _efficiency_flag = "Low Efficiency"
        elif projected_hires == 0 and dollars > 0:
            _efficiency_flag = "No Projected Hires"

        # ── S49/S50 FIX: Downgrade channel confidence from input data quality ──
        # The CPC-based confidence above only reflects the CPC data source,
        # NOT the quality of upstream inputs (salary, enrichment, etc.).
        # A "high" CPC source with 50% enrichment confidence should NOT
        # display as HIGH -- downgrade it.
        #
        # S50 fix: The S49 version had three bugs:
        #   1. It checked confidence_scores.overall (synthesizer average) first,
        #      which averages KB-fallback section scores (~0.6) and hides low
        #      raw enrichment confidence. Now we check enrichment_summary FIRST
        #      because that reflects actual API data quality.
        #   2. Thresholds were too low: < 0.5 missed exactly 0.5 (50%).
        #      Now: <= 0.5 -> low, <= 0.7 -> medium.
        #   3. Only downgraded "high" to "medium" at < 0.5. Now properly
        #      downgrades any confidence level when data quality is poor.
        _confidence_downgrade_reason = ""
        if synthesized_data and isinstance(synthesized_data, dict):
            # Path 1 (preferred): enrichment_summary.confidence_score from
            # api_enrichment -- this is the RAW API success ratio and the most
            # accurate signal of upstream data quality.
            _input_conf = (synthesized_data.get("enrichment_summary") or {}).get(
                "confidence_score"
            )
            # Path 2 (fallback): confidence_scores.overall from data_synthesizer
            # -- this averages section scores and can mask low enrichment quality.
            if _input_conf is None:
                _input_conf = (synthesized_data.get("confidence_scores") or {}).get(
                    "overall"
                )
            if isinstance(_input_conf, (int, float)):
                if _input_conf <= 0.5:
                    if confidence != "low":
                        _confidence_downgrade_reason = (
                            f"Input data confidence {_input_conf:.0%} <= 50%"
                        )
                    confidence = "low"
                elif _input_conf <= 0.7:
                    if confidence == "high":
                        _confidence_downgrade_reason = (
                            f"Input data confidence {_input_conf:.0%} <= 70%"
                        )
                        confidence = "medium"

        allocation_entry: Dict[str, Any] = {
            "dollar_amount": dollars,
            "percentage": round(pct, 1),
            "cpc": round(cpc, 2),
            "projected_clicks": projected_clicks,
            "projected_applications": projected_applications,
            "projected_hires": projected_hires,
            "cpa": round(cpa, 2),
            "cost_per_hire": round(cost_per_hire, 2),
            "roi_score": roi,
            "confidence": confidence or "low",
            "category": category,
            "efficiency_flag": _efficiency_flag,
            "safety_margin": round(_margin, 2),
            # v3 fields
            "cpc_source": cpc_source,
            "apply_rate": round(apply_rate_adj, 4),
            "apply_rate_collar_adjusted": collar_mult != 1.0,
        }
        if _confidence_downgrade_reason:
            allocation_entry["confidence_downgrade_reason"] = (
                _confidence_downgrade_reason
            )

        # v3: Attach trend metadata when available
        if trend_meta:
            allocation_entry["trend_direction"] = trend_meta.get(
                "trend_direction", "stable"
            )
            allocation_entry["trend_pct_yoy"] = round(
                trend_meta.get("trend_pct_yoy", 0.0), 1
            )
            allocation_entry["seasonal_factor"] = round(
                trend_meta.get("seasonal_factor", 1.0), 2
            )

        allocations[ch_name] = allocation_entry

    logger.info(
        "Channel dollar amounts computed for %d channels (collar=%s, trend_engine=%s)",
        len(allocations),
        effective_collar,
        "yes" if _HAS_TREND_ENGINE else "no",
    )
    return allocations


def rebalance_low_roi_channels(
    channel_allocations: Dict[str, Dict],
    total_budget: float,
    roi_floor: int = 2,
    alloc_cap_pct: float = 3.0,
    spend_threshold_pct: float = 5.0,
    recipient_roi_min: int = 6,
) -> Dict[str, Dict]:
    """Rebalance budget away from low-ROI channels to high-ROI channels.

    After ``compute_channel_dollar_amounts`` produces per-channel ROI scores,
    this post-processor identifies channels whose ROI is too low relative to
    their budget share and redistributes the freed dollars to the strongest
    performers.

    Rules:
        - A channel is a **donor** when its ``roi_score <= roi_floor`` AND its
          allocation exceeds ``spend_threshold_pct`` percent of total budget.
          Its allocation is capped at ``alloc_cap_pct`` percent.
        - Freed budget is redistributed proportionally (by ROI score) to
          channels whose ``roi_score >= recipient_roi_min``.
        - Projected metrics (clicks, applications, hires, CPA, CPH) are
          recomputed for every affected channel using its existing CPC and
          apply rate so the numbers stay internally consistent.

    Args:
        channel_allocations: Output of ``compute_channel_dollar_amounts``.
            **Modified in-place** and also returned for convenience.
        total_budget: Total campaign budget in USD.
        roi_floor: Maximum ROI score to qualify as a donor (inclusive).
        alloc_cap_pct: Target cap for donor channels (percent of total budget).
        spend_threshold_pct: Minimum allocation percent to trigger rebalancing.
        recipient_roi_min: Minimum ROI score to qualify as a recipient.

    Returns:
        The (mutated) ``channel_allocations`` dict with updated dollar amounts,
        percentages, and projected metrics for affected channels.
    """
    if not channel_allocations or total_budget <= 0:
        return channel_allocations

    cap_dollars = total_budget * (alloc_cap_pct / 100.0)
    threshold_dollars = total_budget * (spend_threshold_pct / 100.0)

    # Identify donors and recipients
    freed_pool = 0.0
    donors: Dict[str, float] = {}  # channel -> dollars freed
    recipients: List[str] = []

    for ch_name, ch_data in channel_allocations.items():
        roi = ch_data.get("roi_score", 5)
        dollars = ch_data.get("dollar_amount", 0)

        if roi <= roi_floor and dollars > threshold_dollars:
            freed = max(0.0, dollars - cap_dollars)
            if freed > 0:
                donors[ch_name] = freed
                freed_pool += freed

        elif roi >= recipient_roi_min and dollars > 0:
            recipients.append(ch_name)

    if freed_pool <= 0 or not recipients:
        return channel_allocations

    # Compute recipient weights (proportional to ROI score)
    recipient_roi_sum = sum(
        channel_allocations[r].get("roi_score", 5) for r in recipients
    )
    if recipient_roi_sum <= 0:
        return channel_allocations

    logger.info(
        "Low-ROI rebalance: $%.0f freed from %d donor(s) -> %d recipient(s)",
        freed_pool,
        len(donors),
        len(recipients),
    )

    # Apply reductions to donors
    for ch_name, freed in donors.items():
        ch = channel_allocations[ch_name]
        old_dollars = ch.get("dollar_amount", 0)
        new_dollars = round(old_dollars - freed, 2)
        _recompute_channel_metrics(ch, new_dollars, total_budget)
        logger.info(
            "  Donor %s: $%.0f -> $%.0f (ROI %d)",
            ch_name,
            old_dollars,
            new_dollars,
            ch.get("roi_score", 0),
        )

    # Distribute freed pool to recipients
    for ch_name in recipients:
        ch = channel_allocations[ch_name]
        roi = ch.get("roi_score", 5)
        share = roi / recipient_roi_sum
        bonus = freed_pool * share
        old_dollars = ch.get("dollar_amount", 0)
        new_dollars = round(old_dollars + bonus, 2)
        _recompute_channel_metrics(ch, new_dollars, total_budget)
        logger.info(
            "  Recipient %s: $%.0f -> $%.0f (+$%.0f, ROI %d)",
            ch_name,
            old_dollars,
            new_dollars,
            bonus,
            roi,
        )

    return channel_allocations


def _recompute_channel_metrics(
    ch: Dict[str, Any], new_dollars: float, total_budget: float
) -> None:
    """Recompute projected metrics for a channel after its dollar amount changes.

    Updates the channel dict **in-place** with recalculated clicks,
    applications, hires, CPA, CPH, and percentage.

    Uses the channel's existing CPC, apply_rate, and hire_rate assumptions
    so that the rebalanced numbers are consistent with the original model.
    """
    ch["dollar_amount"] = new_dollars
    ch["percentage"] = round(_safe_divide(new_dollars, total_budget, 0.0) * 100.0, 1)

    cpc = ch.get("cpc", 0.85)
    apply_rate = ch.get("apply_rate", 0.05)

    # Infer hire_rate from existing data when possible
    old_apps = ch.get("projected_applications", 0)
    old_hires = ch.get("projected_hires", 0)
    if old_apps > 0 and old_hires > 0:
        hire_rate = old_hires / old_apps
    else:
        hire_rate = 0.02  # default fallback

    if cpc > 0:
        clicks = max(0, int(new_dollars / cpc))
        apps = max(0, int(clicks * apply_rate))
        hires = max(0, int(apps * hire_rate))
    else:
        clicks = 0
        apps = max(1, int(new_dollars / 50.0))
        hires = max(0, int(apps * hire_rate * 2))

    # Enforce per-channel CPH floor (same logic as primary path)
    category = ch.get("category", "")
    _ch_min_cph = _CHANNEL_MIN_CPH.get(category, 0)
    if _ch_min_cph > 0 and new_dollars > 0 and hires > 0:
        max_hires_at_floor = int(new_dollars / _ch_min_cph)
        if hires > max_hires_at_floor:
            hires = max(max_hires_at_floor, 0)

    ch["projected_clicks"] = clicks
    ch["projected_applications"] = apps
    ch["projected_hires"] = hires
    ch["cpa"] = round(_safe_divide(new_dollars, max(apps, 1), new_dollars), 2)
    ch["cost_per_hire"] = round(
        _safe_divide(new_dollars, max(hires, 1), new_dollars), 2
    )


def assess_budget_sufficiency(
    total_budget: float,
    total_openings: int,
    industry: str,
    channel_allocations: Dict,
    knowledge_base: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Check whether the total budget is sufficient for the hiring goals.

    Compares projected cost-per-hire against industry benchmarks.
    Flags when budget per opening falls below minimum viable thresholds.

    Args:
        total_budget: Campaign budget in USD.
        total_openings: Number of positions to fill.
        industry: Industry classification string (e.g. "healthcare_medical").
        channel_allocations: Output of ``compute_channel_dollar_amounts``.
        knowledge_base: Loaded KB JSON (optional, for CPH benchmarks).

    Returns:
        Dict with ``sufficient``, ``budget_per_opening``,
        ``industry_avg_cost_per_hire``, ``gap_amount``, ``warnings``,
        ``recommendations``.
    """
    warnings: List[str] = []
    recommendations: List[str] = []

    total_openings = max(total_openings, 1)
    n_openings = total_openings  # alias for readability in feasibility block
    budget_per_opening = _safe_divide(total_budget, total_openings, 0.0)
    avg_cph = _industry_avg_cph(industry)

    # Try to refine avg_cph from KB
    if knowledge_base:
        kb_benchmarks = knowledge_base.get("benchmarks", {})
        cph_section = kb_benchmarks.get("cost_per_hire", {})
        shrm = cph_section.get("shrm_2026") or cph_section.get("shrm_2025", {})
        raw = shrm.get("average_cost_per_hire")
        parsed = _parse_dollar_value(raw)
        if parsed and parsed > 0:
            # Blend KB value with industry-specific range (KB is cross-industry)
            avg_cph = (avg_cph + parsed) / 2.0

    gap = max(0.0, (avg_cph * total_openings) - total_budget)
    sufficient = budget_per_opening >= avg_cph * 0.5  # at least 50% of avg CPH

    # Build projected totals from allocations
    total_proj_hires = sum(
        ch.get("projected_hires") or 0 for ch in channel_allocations.values()
    )

    # ── Budget Reality Check ──────────────────────────────────────
    # Map the raw industry key (e.g. "healthcare_medical") to a
    # simplified key for _INDUSTRY_MIN_CPH lookup.
    _ind_lower = industry.lower().replace("-", "_") if industry else "general"
    industry_key = "general"
    for _cph_key in _INDUSTRY_MIN_CPH:
        if _cph_key in _ind_lower:
            industry_key = _cph_key
            break

    industry_min_cph = _INDUSTRY_MIN_CPH.get(industry_key, _INDUSTRY_MIN_CPH["general"])
    min_viable_budget = industry_min_cph * n_openings
    budget_utilization = (
        (total_budget / min_viable_budget * 100) if min_viable_budget > 0 else 0
    )

    # Determine feasibility tier
    if budget_per_opening < industry_min_cph * 0.1:
        feasibility_tier = "impossible"
        feasibility_label = "UNREALISTIC"
        feasibility_msg = (
            f"A budget of ${total_budget:,.0f} for {n_openings} hires "
            f"translates to ${budget_per_opening:,.0f}/hire — far below the "
            f"{industry_key} industry minimum of ~${industry_min_cph:,.0f}/hire. "
            f"This budget could realistically support ~{max(1, int(total_budget / industry_min_cph))} hire(s). "
            f"Recommended minimum budget: ${min_viable_budget:,.0f}."
        )
    elif budget_per_opening < industry_min_cph * 0.3:
        feasibility_tier = "severely_underfunded"
        feasibility_label = "SEVERELY UNDERFUNDED"
        feasibility_msg = (
            f"At ${budget_per_opening:,.0f}/hire, this budget covers only "
            f"{budget_utilization:.0f}% of the minimum required. "
            f"Realistically achievable hires: ~{max(1, int(total_budget / industry_min_cph))}. "
            f"Recommended budget for {n_openings} hires: ${min_viable_budget:,.0f}."
        )
    elif budget_per_opening < industry_min_cph * 0.5:
        feasibility_tier = "underfunded"
        feasibility_label = "UNDERFUNDED"
        feasibility_msg = (
            f"Budget of ${budget_per_opening:,.0f}/hire is below the "
            f"industry average of ~${industry_min_cph:,.0f}/hire. "
            f"Consider reducing target to {max(1, int(total_budget / industry_min_cph))} hires "
            f"or increasing budget to ${min_viable_budget:,.0f}."
        )
    elif budget_per_opening < industry_min_cph:
        feasibility_tier = "tight"
        feasibility_label = "TIGHT BUT FEASIBLE"
        feasibility_msg = (
            f"Budget of ${budget_per_opening:,.0f}/hire is below the "
            f"industry average of ~${industry_min_cph:,.0f}/hire but achievable "
            f"with optimized channel selection and programmatic buying."
        )
    elif budget_per_opening < industry_min_cph * 1.5:
        feasibility_tier = "adequate"
        feasibility_label = "ADEQUATE"
        feasibility_msg = (
            f"Budget of ${budget_per_opening:,.0f}/hire is within the normal range "
            f"for {industry_key} hiring. Good foundation for a competitive campaign."
        )
    else:
        feasibility_tier = "generous"
        feasibility_label = "WELL-FUNDED"
        feasibility_msg = (
            f"Budget of ${budget_per_opening:,.0f}/hire exceeds the industry average. "
            f"Consider investing surplus in employer branding or premium placements."
        )

    # --- Warnings ---
    if budget_per_opening < _MIN_BUDGET_PER_OPENING:
        warnings.append(
            f"Budget of ${total_budget:,.0f} for {total_openings} openings "
            f"(${budget_per_opening:,.0f}/opening) is below the minimum viable "
            f"threshold of ${_MIN_BUDGET_PER_OPENING:,.0f}/opening. Most channels "
            f"cannot generate meaningful results at this level."
        )

    if not sufficient:
        recommended_budget = avg_cph * total_openings
        reduced_openings = max(1, int(total_budget / avg_cph))
        warnings.append(
            f"Budget of ${total_budget:,.0f} for {total_openings} openings "
            f"(${budget_per_opening:,.0f}/opening) is significantly below the "
            f"{_format_industry_name(industry)} industry average of "
            f"${avg_cph:,.0f}/hire. Consider reducing to {reduced_openings} "
            f"priority openings or increasing budget to ${recommended_budget:,.0f}."
        )

    if total_proj_hires < total_openings and total_proj_hires > 0:
        shortfall = total_openings - total_proj_hires
        warnings.append(
            f"Projected hires ({total_proj_hires}) fall short of the "
            f"{total_openings} target openings by {shortfall}. The campaign may "
            f"need a longer run time or supplemental sourcing strategies."
        )

    # --- Recommendations ---
    if gap > 0:
        recommendations.append(
            f"To fully fund all {total_openings} openings at industry-average "
            f"CPH, an additional ${gap:,.0f} would be needed (total "
            f"${avg_cph * total_openings:,.0f})."
        )

    if budget_per_opening < avg_cph and total_openings > 3:
        recommendations.append(
            "Consider a phased hiring approach: prioritise the most critical "
            "roles in Phase 1, then reinvest savings into subsequent phases."
        )

    # Check for channels with very low ROI
    low_roi_channels = [
        name
        for name, ch in channel_allocations.items()
        if ch.get("roi_score", 5) <= 3
        and ch.get("dollar_amount", ch.get("dollars") or 0) > 0
    ]
    if low_roi_channels:
        recommendations.append(
            f"Channels with low ROI scores ({', '.join(low_roi_channels)}) "
            f"may benefit from budget reallocation to higher-performing channels."
        )

    # S49 Issue 16: Flag channels with low efficiency (spending >$1000, 0 hires)
    low_eff_channels = [
        name
        for name, ch in channel_allocations.items()
        if ch.get("efficiency_flag") == "Low Efficiency"
    ]
    if low_eff_channels:
        recommendations.append(
            f"Low Efficiency alert: {', '.join(low_eff_channels)} "
            f"projected 0 hires despite >$1,000 spend. Consider reallocating "
            f"this budget to channels with measurable hiring outcomes."
        )

    if total_budget > avg_cph * total_openings * 1.5:
        recommendations.append(
            "Budget exceeds 1.5x the industry average per hire. Consider "
            "investing the surplus in employer branding, referral incentives, "
            "or talent pipeline development for long-term ROI."
        )

    result = {
        "sufficient": sufficient,
        "budget_per_opening": round(budget_per_opening, 2),
        "industry_avg_cost_per_hire": round(avg_cph, 2),
        "gap_amount": round(gap, 2),
        "total_projected_hires": total_proj_hires,
        "warnings": warnings,
        "recommendations": recommendations,
    }

    result["budget_reality_check"] = {
        "feasibility_tier": feasibility_tier,
        "feasibility_label": feasibility_label,
        "feasibility_message": feasibility_msg,
        "budget_per_hire": round(budget_per_opening, 2),
        "industry_avg_cph": industry_min_cph,
        "min_viable_budget": min_viable_budget,
        "realistic_hires": (
            max(1, int(total_budget / industry_min_cph))
            if industry_min_cph > 0
            else n_openings
        ),
        "budget_utilization_pct": round(budget_utilization, 1),
        "target_hires": n_openings,
    }

    return result


def optimize_allocation(
    channel_allocations: Dict,
    total_budget: float,
    optimization_goal: str = "hires",
    collar_type: str = "",
) -> Dict[str, Any]:
    """
    Suggest reallocation to optimise for the specified goal.

    Shifts budget from low-ROI channels to high-ROI channels.
    Never reduces any channel below 5% of its original allocation.

    Args:
        channel_allocations: Output of ``compute_channel_dollar_amounts``.
        total_budget: Total budget in USD.
        optimization_goal: One of ``"hires"``, ``"applications"``, ``"clicks"``.
        collar_type: Collar type hint for tier-aware hire/apply rates
            (e.g. ``"blue_collar"``, ``"white_collar"``).  Empty string
            falls back to the flat ``BASE_BENCHMARKS["hire_rate"]``.

    Returns:
        Dict with ``optimized_allocations``, ``improvement``, ``changes``.
    """
    if not channel_allocations:
        logger.warning("No channel allocations to optimise")
        return {
            "optimized_allocations": {},
            "improvement": {
                "metric": optimization_goal,
                "original": 0,
                "optimized": 0,
                "pct_change": 0.0,
            },
            "changes": [],
        }

    goal_key = {
        "hires": "projected_hires",
        "applications": "projected_applications",
        "clicks": "projected_clicks",
    }.get(optimization_goal, "projected_hires")

    # Compute efficiency: goal metric per dollar for each channel
    efficiencies: Dict[str, float] = {}
    for ch_name, ch_data in channel_allocations.items():
        dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
        metric_val = ch_data.get(goal_key, 0)
        efficiencies[ch_name] = _safe_divide(metric_val, dollars, 0.0)

    if not efficiencies or all(v == 0 for v in efficiencies.values()):
        return {
            "optimized_allocations": dict(channel_allocations),
            "improvement": {
                "metric": optimization_goal,
                "original": 0,
                "optimized": 0,
                "pct_change": 0.0,
            },
            "changes": [],
        }

    # Rank channels by efficiency (higher = better)
    ranked = sorted(efficiencies.items(), key=lambda x: x[1], reverse=True)
    median_eff = sorted(efficiencies.values())[len(efficiencies) // 2]

    # Identify donors (below-median efficiency) and recipients (above-median)
    donors: List[str] = []
    recipients: List[str] = []
    for ch_name, eff in ranked:
        if eff < median_eff * 0.7:
            donors.append(ch_name)
        elif eff > median_eff * 1.3:
            recipients.append(ch_name)

    if not donors or not recipients:
        # No clear winners/losers; return original
        original_total = sum(ch.get(goal_key, 0) for ch in channel_allocations.values())
        return {
            "optimized_allocations": dict(channel_allocations),
            "improvement": {
                "metric": optimization_goal,
                "original": original_total,
                "optimized": original_total,
                "pct_change": 0.0,
            },
            "changes": [],
        }

    # Compute transfer amounts (max 30% from each donor, min 5% floor)
    transfer_pool = 0.0
    donor_reductions: Dict[str, float] = {}
    for ch_name in donors:
        orig_dollars = channel_allocations[ch_name].get(
            "dollar_amount", channel_allocations[ch_name].get("dollars") or 0
        )
        max_reduction = orig_dollars * 0.30  # never take more than 30%
        floor = orig_dollars * 0.05  # keep at least 5%
        reduction = min(max_reduction, orig_dollars - floor)
        reduction = max(0, reduction)
        donor_reductions[ch_name] = reduction
        transfer_pool += reduction

    if transfer_pool <= 0:
        original_total = sum(ch.get(goal_key, 0) for ch in channel_allocations.values())
        return {
            "optimized_allocations": dict(channel_allocations),
            "improvement": {
                "metric": optimization_goal,
                "original": original_total,
                "optimized": original_total,
                "pct_change": 0.0,
            },
            "changes": [],
        }

    # Distribute pool to recipients proportional to their efficiency
    recipient_eff_sum = sum(efficiencies[r] for r in recipients)
    recipient_gains: Dict[str, float] = {}
    for ch_name in recipients:
        share = _safe_divide(efficiencies[ch_name], recipient_eff_sum, 0.0)
        recipient_gains[ch_name] = transfer_pool * share

    # Build optimised allocations and reproject outcomes
    optimized: Dict[str, Dict] = {}
    changes: List[Dict] = []
    original_metric_total = 0
    optimized_metric_total = 0

    # H7: Collar-to-tier mapping for hire rate lookup (defined once outside loop)
    _COLLAR_TO_TIER: Dict[str, str] = {
        "blue_collar": "Hourly / Entry-Level",
        "white_collar": "Professional / White-Collar",
        "grey_collar": "Skilled Trades / Technical",
    }

    for ch_name, ch_data in channel_allocations.items():
        orig_dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
        orig_pct = ch_data.get("percentage") or 0
        original_metric_total += ch_data.get(goal_key, 0)

        new_dollars = orig_dollars
        reason = ""

        if ch_name in donor_reductions and donor_reductions[ch_name] > 0:
            new_dollars = orig_dollars - donor_reductions[ch_name]
            reason = (
                f"Low {optimization_goal} efficiency "
                f"({efficiencies[ch_name]:.4f}/{optimization_goal}/dollar)"
            )
        elif ch_name in recipient_gains and recipient_gains[ch_name] > 0:
            new_dollars = orig_dollars + recipient_gains[ch_name]
            reason = (
                f"High {optimization_goal} efficiency "
                f"({efficiencies[ch_name]:.4f}/{optimization_goal}/dollar)"
            )

        new_pct = _safe_divide(new_dollars, total_budget, 0.0) * 100
        cpc = ch_data.get("cpc", 0.85)
        category = ch_data.get("category", "job_board")

        # H7 FIX: Use collar-adjusted apply rate when available,
        # otherwise fall back to base rate with collar adjustment.
        if ch_data.get("apply_rate") and ch_data.get("apply_rate_collar_adjusted"):
            apply_rate = ch_data["apply_rate"]
        else:
            apply_rate = BASE_BENCHMARKS["apply_rate"].get(category, 0.05)
            if collar_type and collar_type != "both":
                apply_rate *= _get_collar_apply_rate_adjustment(category, collar_type)

        # H7 FIX: Use collar-type-aware hire rate from HIRE_RATE_BY_TIER
        # instead of flat BASE_BENCHMARKS["hire_rate"].
        if collar_type and collar_type in _COLLAR_TO_TIER:
            tier_key = _COLLAR_TO_TIER[collar_type]
            hire_rate = HIRE_RATE_BY_TIER.get(tier_key, HIRE_RATE_BY_TIER["default"])
        else:
            hire_rate = BASE_BENCHMARKS["hire_rate"]

        if cpc > 0:
            new_clicks = max(0, int(new_dollars / cpc))
            new_apps = max(0, int(new_clicks * apply_rate))
            new_hires = max(0, int(new_apps * hire_rate))
        else:
            new_clicks = 0
            new_apps = max(1, int(new_dollars / 50.0))
            new_hires = max(0, int(new_apps * hire_rate * 2))

        # S49: Apply per-channel CPH floor in optimizer path (same as primary)
        _opt_min_cph = _CHANNEL_MIN_CPH.get(category, 0)
        if _opt_min_cph > 0 and new_dollars > 0 and new_hires > 0:
            _opt_max_hires = int(new_dollars / _opt_min_cph)
            if new_hires > _opt_max_hires:
                new_hires = max(_opt_max_hires, 0)

        optimized_metric_total += {
            "projected_hires": new_hires,
            "projected_applications": new_apps,
            "projected_clicks": new_clicks,
        }.get(goal_key, 0)

        opt_entry = dict(ch_data)
        opt_entry["dollars"] = round(new_dollars, 2)
        opt_entry["percentage"] = round(new_pct, 1)
        opt_entry["projected_clicks"] = new_clicks
        opt_entry["projected_applications"] = new_apps
        opt_entry["projected_hires"] = new_hires
        opt_entry["cpa"] = round(
            _safe_divide(new_dollars, max(new_apps, 1), new_dollars), 2
        )
        opt_entry["cost_per_hire"] = round(
            _safe_divide(new_dollars, max(new_hires, 1), new_dollars), 2
        )
        optimized[ch_name] = opt_entry

        if abs(new_dollars - orig_dollars) > 0.01:
            changes.append(
                {
                    "channel": ch_name,
                    "original_dollars": round(orig_dollars, 2),
                    "new_dollars": round(new_dollars, 2),
                    "original_pct": round(orig_pct, 1),
                    "new_pct": round(new_pct, 1),
                    "reason": reason,
                }
            )

    pct_change = (
        _safe_divide(
            optimized_metric_total - original_metric_total,
            max(original_metric_total, 1),
            0.0,
        )
        * 100
    )

    logger.info(
        "Optimisation for '%s': %d -> %d (%.1f%% improvement)",
        optimization_goal,
        original_metric_total,
        optimized_metric_total,
        pct_change,
    )

    return {
        "optimized_allocations": optimized,
        "improvement": {
            "metric": optimization_goal,
            "original": original_metric_total,
            "optimized": optimized_metric_total,
            "pct_change": round(pct_change, 1),
        },
        "changes": changes,
    }


# ---------------------------------------------------------------------------
# Master function
# ---------------------------------------------------------------------------


def calculate_budget_allocation(
    total_budget: float,
    roles: List[Dict],
    locations: List[Dict],
    industry: str,
    channel_percentages: Dict[str, float],
    synthesized_data: Optional[Dict] = None,
    knowledge_base: Optional[Dict] = None,
    collar_type: str = "",
    campaign_start_month: int = 0,
) -> Dict[str, Any]:
    """
    Master budget allocation function.

    Orchestrates location multipliers, role weighting, channel dollar
    calculations, sufficiency assessment, and optimisation suggestions
    into a single comprehensive result.

    v3: Accepts collar_type for trend-engine-aware CPC resolution and
    collar-specific apply rate adjustments.

    Args:
        total_budget: Total budget in USD (e.g. 50000).
        roles: List of role dicts with ``title``, ``count``, ``tier``.
        locations: List of location dicts with ``city``, ``state``, ``country``.
        industry: Industry classification string.
        channel_percentages: Dict of ``channel_name -> percentage`` (0-100).
        synthesized_data: Output from enrichment pipeline (optional).
        knowledge_base: Loaded knowledge base JSON (optional).
        collar_type: v3 collar type hint (auto-detected from roles if empty).

    Returns:
        Dict with keys:
        - ``channel_allocations``: per-channel spend and projections
        - ``role_allocations``: per-role budget split
        - ``location_adjustments``: cost multipliers by location
        - ``total_projected``: aggregate {clicks, applications, hires, cost_per_hire}
        - ``sufficiency``: budget sufficiency assessment
        - ``warnings``: list of budget insufficiency warnings
        - ``recommendations``: list of optimisation suggestions
        - ``optimized``: optional optimised allocation
    """
    logger.info(
        "calculate_budget_allocation: budget=$%.2f, roles=%d, locations=%d, "
        "industry=%s, channels=%d",
        total_budget,
        len(roles),
        len(locations),
        industry,
        len(channel_percentages),
    )

    # Guard: zero budget
    if total_budget <= 0:
        logger.warning("Zero or negative budget; returning empty allocation")
        return _empty_result(
            warnings=["Budget is zero or negative. No allocations can be made."]
        )

    # Guard: no channels
    if not channel_percentages:
        logger.warning("No channel percentages provided; returning empty allocation")
        return _empty_result(
            warnings=["No channel percentages were provided. Cannot allocate budget."]
        )

    # Step 1: Location cost multipliers
    location_multipliers = compute_location_cost_multipliers(
        locations, synthesized_data
    )

    # Step 1b: Apply geopolitical risk adjustments (if available)
    geo_context = (
        synthesized_data.get("geopolitical_context", {}) if synthesized_data else {}
    )
    geo_locations = geo_context.get("locations", {})
    if geo_locations:
        for loc_key, loc_mult in list(location_multipliers.items()):
            # Match location key (case-insensitive partial match)
            loc_lower = loc_key.lower()
            for geo_loc, geo_data in geo_locations.items():
                if geo_loc.lower() in loc_lower or loc_lower in geo_loc.lower():
                    adj_factor = geo_data.get("budget_adjustment_factor", 1.0)
                    # Cap geopolitical adjustment at 1.5x to avoid runaway costs
                    adj_factor = min(max(adj_factor, 0.8), 1.5)
                    if adj_factor != 1.0:
                        location_multipliers[loc_key] = loc_mult * adj_factor
                        logger.info(
                            "Geopolitical adjustment for %s: %.2fx (risk factor %.2f)",
                            loc_key,
                            location_multipliers[loc_key],
                            adj_factor,
                        )
                    break

    # Step 2: Role-weighted spend
    role_budgets = compute_role_weighted_spend(
        roles, total_budget, location_multipliers
    )

    # Step 3: Channel dollar amounts with projections (v3: trend + collar aware)
    # Extract primary location for regional CPC adjustments
    primary_location = ""
    if locations:
        loc0 = locations[0]
        if isinstance(loc0, dict):
            primary_location = loc0.get(
                "city", loc0.get("location", loc0.get("name") or "")
            )
        elif isinstance(loc0, str):
            primary_location = loc0

    channel_allocs = compute_channel_dollar_amounts(
        channel_percentages,
        role_budgets,
        synthesized_data,
        knowledge_base,
        industry=industry,
        collar_type=collar_type,
        location=primary_location,
        month=campaign_start_month,
    )

    # Step 3.5: Auto-rebalance low-ROI channels
    # Channels with ROI <= 2 and spending > 5% of budget get capped at 3%.
    # Freed budget is redistributed proportionally to channels with ROI >= 6.
    channel_allocs = rebalance_low_roi_channels(channel_allocs, total_budget)

    # Step 4: Aggregate projected totals
    total_clicks = sum(
        ch.get("projected_clicks") or 0 for ch in channel_allocs.values()
    )
    total_apps = sum(
        ch.get("projected_applications") or 0 for ch in channel_allocs.values()
    )
    total_hires = sum(ch.get("projected_hires") or 0 for ch in channel_allocs.values())
    avg_cost_per_hire = _safe_divide(total_budget, max(total_hires, 1), total_budget)

    # S39: Enforce benchmark CPH floor -- plan CPH should never be below
    # the industry benchmark minimum.  This prevents plans from showing
    # e.g. $246 CPH when industry benchmarks say $400-$800.
    # S48 FIX: When adjusting total_hires for CPH floor, also scale per-channel
    # projected_hires proportionally so that SUM(channel hires) == total_hires.
    # This eliminates the inconsistency where the header showed 10 hires but
    # the channel rows summed to 56.
    _benchmark_cph_floor = _industry_avg_cph(industry) * 0.5  # 50% of avg as floor
    if avg_cost_per_hire < _benchmark_cph_floor and total_hires > 0:
        # Adjust hires down so CPH meets benchmark floor
        avg_cost_per_hire = _benchmark_cph_floor
        new_total_hires = max(1, int(total_budget / _benchmark_cph_floor))
        # Scale per-channel hires proportionally to keep consistency
        scale_factor = new_total_hires / total_hires if total_hires > 0 else 0
        _remaining_hires = new_total_hires
        _channels_with_hires = [
            (name, ch)
            for name, ch in channel_allocs.items()
            if (ch.get("projected_hires") or 0) > 0
        ]
        for i, (ch_name, ch) in enumerate(_channels_with_hires):
            old_ch_hires = ch.get("projected_hires") or 0
            if i == len(_channels_with_hires) - 1:
                # Last channel gets remainder to avoid rounding drift
                new_ch_hires = _remaining_hires
            else:
                new_ch_hires = max(0, int(old_ch_hires * scale_factor))
                _remaining_hires -= new_ch_hires
            ch["projected_hires"] = new_ch_hires
            # Recalculate per-channel cost_per_hire to stay consistent
            ch_dollars = ch.get("dollar_amount") or 0
            ch["cost_per_hire"] = round(
                _safe_divide(ch_dollars, max(new_ch_hires, 1), ch_dollars), 2
            )
        total_hires = new_total_hires

    total_projected = {
        "clicks": total_clicks,
        "applications": total_apps,
        "hires": total_hires,
        "cost_per_hire": round(avg_cost_per_hire, 2),
        "cost_per_application": round(
            _safe_divide(total_budget, max(total_apps, 1), 0), 2
        ),
        "cost_per_click": round(_safe_divide(total_budget, max(total_clicks, 1), 0), 2),
    }

    # Step 5: Budget sufficiency assessment
    total_openings = sum(
        max(1, int(r.get("count", r.get("openings", 1)) or 1))
        for r in (roles or [{"count": 1}])
    )

    sufficiency = assess_budget_sufficiency(
        total_budget,
        total_openings,
        industry,
        channel_allocs,
        knowledge_base,
    )

    # Step 6: Optimisation suggestions
    optimized = optimize_allocation(
        channel_allocs, total_budget, "hires", collar_type=collar_type
    )

    # Consolidate warnings and recommendations
    all_warnings = list(sufficiency.get("warnings") or [])
    all_recommendations = list(sufficiency.get("recommendations") or [])
    if (optimized.get("improvement", {}).get("pct_change") or 0) > 5:
        all_recommendations.append(
            f"Optimised allocation could improve projected hires by "
            f"{optimized['improvement']['pct_change']:.0f}%. "
            f"See the 'optimized' section for details."
        )

    result = {
        "channel_allocations": channel_allocs,
        "role_allocations": role_budgets,
        "location_adjustments": location_multipliers,
        "total_projected": total_projected,
        "sufficiency": sufficiency,
        "warnings": all_warnings,
        "recommendations": all_recommendations,
        "optimized": optimized,
        "metadata": {
            "total_budget": total_budget,
            "industry": industry,
            "total_openings": total_openings,
            "industry_avg_cph": round(_industry_avg_cph(industry), 2),
            "channels_count": len(channel_allocs),
            "roles_count": len(role_budgets),
            "locations_count": len(location_multipliers),
            # v3 metadata
            "trend_engine_available": _HAS_TREND_ENGINE,
            "collar_intelligence_available": _HAS_COLLAR_INTEL,
            "collar_type_used": collar_type
            or _classify_roles_collar(role_budgets, industry),
            "primary_location": primary_location,
            "campaign_start_month": (
                campaign_start_month
                if campaign_start_month
                else datetime.datetime.now().month
            ),
        },
    }

    logger.info(
        "Budget allocation complete: $%.2f -> %d clicks, %d applications, "
        "%d projected hires (CPH $%.0f)",
        total_budget,
        total_clicks,
        total_apps,
        total_hires,
        avg_cost_per_hire,
    )

    return result


# ---------------------------------------------------------------------------
# Private helpers (location processing)
# ---------------------------------------------------------------------------


def _location_key(loc: Dict) -> str:
    """Build a stable string key from a location dict."""
    if isinstance(loc, str):
        return loc.strip()
    city = loc.get("city", loc.get("location", loc.get("name") or ""))
    state = loc.get("state", loc.get("region") or "")
    country = loc.get("country") or ""
    parts = [p.strip() for p in [city, state, country] if p and str(p).strip()]
    return ", ".join(parts) if parts else ""


def _find_teleport_entry(loc_key: str, teleport_cities: Dict) -> Optional[Dict]:
    """Find a Teleport city entry by fuzzy key matching."""
    if not teleport_cities:
        return None
    # Exact match
    if loc_key in teleport_cities:
        return teleport_cities[loc_key]
    # City-name match (first token before comma)
    city_lower = loc_key.split(",")[0].strip().lower()
    for tk, tv in teleport_cities.items():
        if tk.lower().startswith(city_lower) or city_lower in tk.lower():
            return tv
    return None


def _guess_country(loc: Any) -> str:
    """Try to extract a country from a location dict or string."""
    if isinstance(loc, str):
        parts = [p.strip() for p in loc.split(",")]
        return parts[-1] if len(parts) > 1 else ""
    if isinstance(loc, dict):
        return str(loc.get("country") or "").strip()
    return ""


def _country_multiplier(country: str) -> float:
    """Return a cost multiplier for a country relative to US baseline."""
    _COUNTRY_MULTIPLIERS: Dict[str, float] = {
        "united states": 1.0,
        "us": 1.0,
        "usa": 1.0,
        "united kingdom": 1.15,
        "uk": 1.15,
        "gb": 1.15,
        "canada": 1.05,
        "ca": 1.05,
        "australia": 1.10,
        "au": 1.10,
        "germany": 1.15,
        "de": 1.15,
        "france": 1.10,
        "fr": 1.10,
        "netherlands": 1.10,
        "nl": 1.10,
        "switzerland": 1.50,
        "ch": 1.50,
        "japan": 1.15,
        "jp": 1.15,
        "singapore": 1.25,
        "sg": 1.25,
        "india": 0.30,
        "in": 0.30,
        "philippines": 0.28,
        "ph": 0.28,
        "mexico": 0.40,
        "mx": 0.40,
        "brazil": 0.45,
        "br": 0.45,
        "china": 0.50,
        "cn": 0.50,
        "south korea": 0.85,
        "kr": 0.85,
        "poland": 0.55,
        "pl": 0.55,
        "romania": 0.40,
        "ro": 0.40,
        "ireland": 1.12,
        "ie": 1.12,
        "israel": 1.15,
        "il": 1.15,
        "uae": 1.10,
        "ae": 1.10,
        "saudi arabia": 0.90,
        "sa": 0.90,
        "nigeria": 0.20,
        "ng": 0.20,
        "kenya": 0.22,
        "ke": 0.22,
        "south africa": 0.35,
        "za": 0.35,
    }
    c_lower = country.lower().strip()
    return _COUNTRY_MULTIPLIERS.get(c_lower, 0.80)


def _format_industry_name(industry: str) -> str:
    """Convert 'healthcare_medical' -> 'Healthcare / Medical'."""
    if not industry:
        return "General"
    return " / ".join(w.capitalize() for w in industry.split("_"))


def _empty_result(warnings: Optional[List[str]] = None) -> Dict[str, Any]:
    """Return a structurally valid but empty result dict."""
    return {
        "channel_allocations": {},
        "role_allocations": {},
        "location_adjustments": {},
        "total_projected": {
            "clicks": 0,
            "applications": 0,
            "hires": 0,
            "cost_per_hire": 0.0,
            "cost_per_application": 0.0,
            "cost_per_click": 0.0,
        },
        "sufficiency": {
            "sufficient": False,
            "budget_per_opening": 0.0,
            "industry_avg_cost_per_hire": 0.0,
            "gap_amount": 0.0,
            "total_projected_hires": 0,
            "warnings": warnings or [],
            "recommendations": [],
        },
        "warnings": warnings or [],
        "recommendations": [],
        "optimized": {
            "optimized_allocations": {},
            "improvement": {
                "metric": "hires",
                "original": 0,
                "optimized": 0,
                "pct_change": 0.0,
            },
            "changes": [],
        },
        "metadata": {},
    }


# ===========================================================================
# Section 1: Channel Quality Scoring (Mercor lens)
# ===========================================================================
#
# Quality-of-hire scores per channel per collar type.  These capture three
# key dimensions:
#   quality      -- quality-of-hire index (0.0-1.0)
#   retention_6mo -- probability that a hire is still employed after 6 months
#   time_to_productive -- median days until the new hire is fully productive
#
# Every ad-platform category present in BASE_BENCHMARKS is represented here
# under a human-readable channel key.  A mapping from BASE_BENCHMARKS keys
# to quality-score keys is provided via _CATEGORY_TO_QUALITY_KEY.
# ===========================================================================

CHANNEL_QUALITY_SCORES: Dict[str, Dict[str, Dict[str, float]]] = {
    # channel -> collar_type -> {quality, retention_6mo, time_to_productive}
    "job_board": {
        "blue_collar": {
            "quality": 0.65,
            "retention_6mo": 0.58,
            "time_to_productive": 20,
        },
        "white_collar": {
            "quality": 0.70,
            "retention_6mo": 0.62,
            "time_to_productive": 35,
        },
    },
    "programmatic": {
        "blue_collar": {
            "quality": 0.60,
            "retention_6mo": 0.55,
            "time_to_productive": 18,
        },
        "white_collar": {
            "quality": 0.65,
            "retention_6mo": 0.58,
            "time_to_productive": 30,
        },
    },
    "social_media": {
        "blue_collar": {
            "quality": 0.55,
            "retention_6mo": 0.50,
            "time_to_productive": 22,
        },
        "white_collar": {
            "quality": 0.60,
            "retention_6mo": 0.55,
            "time_to_productive": 32,
        },
    },
    "search_engine": {
        "blue_collar": {
            "quality": 0.70,
            "retention_6mo": 0.62,
            "time_to_productive": 18,
        },
        "white_collar": {
            "quality": 0.75,
            "retention_6mo": 0.65,
            "time_to_productive": 28,
        },
    },
    "career_site": {
        "blue_collar": {
            "quality": 0.75,
            "retention_6mo": 0.68,
            "time_to_productive": 16,
        },
        "white_collar": {
            "quality": 0.80,
            "retention_6mo": 0.72,
            "time_to_productive": 25,
        },
    },
    "employer_branding": {
        "blue_collar": {
            "quality": 0.72,
            "retention_6mo": 0.65,
            "time_to_productive": 15,
        },
        "white_collar": {
            "quality": 0.78,
            "retention_6mo": 0.70,
            "time_to_productive": 22,
        },
    },
    "referral": {
        "blue_collar": {
            "quality": 0.88,
            "retention_6mo": 0.82,
            "time_to_productive": 12,
        },
        "white_collar": {
            "quality": 0.90,
            "retention_6mo": 0.85,
            "time_to_productive": 18,
        },
    },
    "staffing_agency": {
        "blue_collar": {
            "quality": 0.62,
            "retention_6mo": 0.52,
            "time_to_productive": 14,
        },
        "white_collar": {
            "quality": 0.68,
            "retention_6mo": 0.58,
            "time_to_productive": 25,
        },
    },
    "niche_board": {
        "blue_collar": {
            "quality": 0.72,
            "retention_6mo": 0.64,
            "time_to_productive": 15,
        },
        "white_collar": {
            "quality": 0.78,
            "retention_6mo": 0.70,
            "time_to_productive": 28,
        },
    },
    "university": {
        "blue_collar": {
            "quality": 0.58,
            "retention_6mo": 0.52,
            "time_to_productive": 30,
        },
        "white_collar": {
            "quality": 0.72,
            "retention_6mo": 0.65,
            "time_to_productive": 40,
        },
    },
    "display_retargeting": {
        "blue_collar": {
            "quality": 0.50,
            "retention_6mo": 0.48,
            "time_to_productive": 22,
        },
        "white_collar": {
            "quality": 0.55,
            "retention_6mo": 0.50,
            "time_to_productive": 35,
        },
    },
    "events_jobfairs": {
        "blue_collar": {
            "quality": 0.80,
            "retention_6mo": 0.72,
            "time_to_productive": 14,
        },
        "white_collar": {
            "quality": 0.75,
            "retention_6mo": 0.68,
            "time_to_productive": 30,
        },
    },
    "internal_mobility": {
        "blue_collar": {
            "quality": 0.92,
            "retention_6mo": 0.88,
            "time_to_productive": 8,
        },
        "white_collar": {
            "quality": 0.95,
            "retention_6mo": 0.90,
            "time_to_productive": 12,
        },
    },
}

# Map BASE_BENCHMARKS ad-platform category keys to CHANNEL_QUALITY_SCORES keys.
# Channels without a direct quality entry fall back to "job_board" defaults.
_CATEGORY_TO_QUALITY_KEY: Dict[str, str] = {
    "job_board": "job_board",
    "social": "social_media",
    "search": "search_engine",
    "programmatic": "programmatic",
    "display": "display_retargeting",
    "niche_board": "niche_board",
    "employer_branding": "employer_branding",
    "referral": "referral",
    "events": "events_jobfairs",
    "staffing": "staffing_agency",
    "email": "job_board",  # email campaigns similar quality profile
    "career_site": "career_site",
    "regional": "job_board",  # regional boards similar to generic job boards
}

# Industry-specific quality adjustments.  Each entry maps an industry
# keyword fragment to a dict of {channel_quality_key: quality_bonus}.
# Bonuses are *additive* to the base quality score, clamped to [0, 1].
_INDUSTRY_QUALITY_ADJUSTMENTS: Dict[str, Dict[str, float]] = {
    "healthcare": {"referral": 0.05, "niche_board": 0.04, "staffing_agency": 0.03},
    "tech": {"career_site": 0.04, "referral": 0.03, "niche_board": 0.05},
    "engineering": {"referral": 0.04, "niche_board": 0.05, "university": 0.03},
    "finance": {"referral": 0.04, "employer_branding": 0.03, "niche_board": 0.03},
    "retail": {"events_jobfairs": 0.04, "social_media": 0.03},
    "hospitality": {"social_media": 0.04, "events_jobfairs": 0.05},
    "manufacturing": {
        "events_jobfairs": 0.04,
        "referral": 0.03,
        "staffing_agency": 0.02,
    },
    "education": {"university": 0.06, "career_site": 0.03},
    "logistics": {"referral": 0.03, "staffing_agency": 0.03, "events_jobfairs": 0.02},
    "pharma": {"niche_board": 0.06, "referral": 0.04},
    "energy": {"niche_board": 0.04, "referral": 0.03},
    "government": {"career_site": 0.05, "job_board": 0.03},
}


def score_channel_quality(
    channel: str,
    collar_type: str = "white_collar",
    industry: str = "",
) -> Dict[str, Any]:
    """
    Score a channel's quality-of-hire potential.

    Returns a dict with the quality score, 6-month retention rate, estimated
    time-to-productive in days, a cost-per-quality-hire factor, and a
    human-readable explanation string.

    The ``cost_per_quality_hire_factor`` is inversely proportional to the
    quality score: lower quality means higher true cost per *quality* hire.

    Industry adjustments are applied when the ``industry`` string contains a
    recognised keyword (e.g. "healthcare", "tech").

    Args:
        channel: Channel key (one of the keys in CHANNEL_QUALITY_SCORES or
                 a BASE_BENCHMARKS category key or a user-facing channel name).
        collar_type: ``"blue_collar"`` or ``"white_collar"`` (default).
        industry: Optional industry string for industry-specific adjustments.

    Returns:
        Dict with keys:
        - ``quality_score``: float 0.0-1.0
        - ``retention_6mo``: float 0.0-1.0
        - ``time_to_productive``: int (days)
        - ``cost_per_quality_hire_factor``: float >= 1.0
        - ``explanation``: str
    """
    try:
        # Normalise collar type
        collar = collar_type.lower().strip() if collar_type else "white_collar"
        if collar not in ("blue_collar", "white_collar"):
            collar = "white_collar"

        # Resolve channel to a CHANNEL_QUALITY_SCORES key
        quality_key = _resolve_quality_key(channel)

        # Look up base scores
        channel_data = CHANNEL_QUALITY_SCORES.get(quality_key, {})
        collar_data = channel_data.get(collar)
        if collar_data is None:
            # Fall back to white_collar, then to a safe default
            collar_data = channel_data.get(
                "white_collar",
                {"quality": 0.60, "retention_6mo": 0.55, "time_to_productive": 30},
            )

        quality = collar_data["quality"]
        retention = collar_data["retention_6mo"]
        ttp = collar_data["time_to_productive"]

        # Apply industry adjustments
        industry_bonus = 0.0
        industry_note = ""
        if industry:
            industry_lower = industry.lower()
            for ind_key, adjustments in _INDUSTRY_QUALITY_ADJUSTMENTS.items():
                if ind_key in industry_lower:
                    bonus = adjustments.get(quality_key, 0.0)
                    if bonus > 0:
                        industry_bonus = bonus
                        industry_note = (
                            f" (+{bonus:.0%} industry bonus for "
                            f"{ind_key} via {quality_key})"
                        )
                    break

        adjusted_quality = _clamp(quality + industry_bonus, 0.0, 1.0)

        # Cost-per-quality-hire factor: lower quality -> higher true cost
        cpqh_factor = round(_safe_divide(1.0, adjusted_quality, 10.0), 2)

        # Build explanation
        explanation = (
            f"{quality_key} ({collar}): quality={adjusted_quality:.2f}, "
            f"6mo retention={retention:.0%}, "
            f"time-to-productive={ttp}d, "
            f"cost-per-quality-hire factor={cpqh_factor}x"
            f"{industry_note}"
        )

        return {
            "quality_score": round(adjusted_quality, 3),
            "retention_6mo": round(retention, 3),
            "time_to_productive": int(ttp),
            "cost_per_quality_hire_factor": cpqh_factor,
            "explanation": explanation,
        }

    except Exception as exc:
        logger.error("score_channel_quality failed for channel=%s: %s", channel, exc)
        return {
            "quality_score": 0.60,
            "retention_6mo": 0.55,
            "time_to_productive": 30,
            "cost_per_quality_hire_factor": 1.67,
            "explanation": f"Fallback defaults (error: {exc})",
        }


def _resolve_quality_key(channel: str) -> str:
    """
    Resolve a channel identifier to a CHANNEL_QUALITY_SCORES key.

    Accepts CHANNEL_QUALITY_SCORES keys directly, BASE_BENCHMARKS category
    keys (via ``_CATEGORY_TO_QUALITY_KEY``), or user-facing channel names
    (via ``CHANNEL_NAME_TO_CATEGORY`` then ``_CATEGORY_TO_QUALITY_KEY``).
    Falls back to ``"job_board"`` when no match is found.
    """
    if not channel:
        return "job_board"
    ch = channel.strip()

    # Direct match in CHANNEL_QUALITY_SCORES
    if ch in CHANNEL_QUALITY_SCORES:
        return ch

    # Match via BASE_BENCHMARKS category key
    if ch in _CATEGORY_TO_QUALITY_KEY:
        return _CATEGORY_TO_QUALITY_KEY[ch]

    # Match via user-facing channel name -> category -> quality key
    category = _category_for_channel(ch)
    return _CATEGORY_TO_QUALITY_KEY.get(category, "job_board")


# ===========================================================================
# Section 2: What-If Scenario Engine (Palantir lens)
# ===========================================================================
#
# These functions let the caller explore budget and channel-mix changes
# without re-running the full enrichment pipeline.  They operate on the
# result dict returned by ``calculate_budget_allocation()``.
# ===========================================================================


def simulate_budget_change(
    base_allocation: Dict[str, Any],
    delta_budget: float = 0.0,
    delta_pct: float = 0.0,
) -> Dict[str, Any]:
    """
    Simulate: "What if we increase/decrease budget by X?"

    Takes the result of ``calculate_budget_allocation()`` and projects
    how a budget change would affect clicks, applications, hires, CPA,
    and cost-per-hire across all channels.

    An economy-of-scale factor is applied: efficiency improves by 0.5%
    per 10% budget increase (diminishing returns), or degrades
    symmetrically for budget decreases.

    Args:
        base_allocation: Result dict from ``calculate_budget_allocation()``.
        delta_budget: Absolute budget change in USD (e.g. +20000 or -10000).
        delta_pct: Percentage budget change (e.g. 0.20 for +20%).
                   If both are provided, ``delta_budget`` takes precedence.

    Returns:
        Dict with ``original_budget``, ``new_budget``, ``change_pct``,
        ``impact`` (metrics comparison), ``channel_changes`` (per-channel
        dollar deltas), and ``recommendations`` (list of strings).
    """
    try:
        channel_allocs = base_allocation.get("channel_allocations", {})
        if not channel_allocs:
            logger.warning("simulate_budget_change: no channel_allocations in base")
            return _empty_scenario(
                "budget_change", "No channel allocations in base result"
            )

        # --- 1. Determine original budget ---
        original_budget = sum(
            ch.get("dollar_amount", ch.get("dollars") or 0)
            for ch in channel_allocs.values()
        )
        if original_budget <= 0:
            original_budget = (
                base_allocation.get("metadata", {}).get("total_budget") or 0
            )
        if original_budget <= 0:
            logger.warning("simulate_budget_change: original budget is zero")
            return _empty_scenario("budget_change", "Original budget is zero")

        # --- 2. Compute new budget ---
        if delta_budget != 0.0:
            new_budget = original_budget + delta_budget
        elif delta_pct != 0.0:
            new_budget = original_budget * (1.0 + delta_pct)
        else:
            # No change requested -- return identity
            new_budget = original_budget

        new_budget = max(new_budget, 0.0)
        change_pct = (
            _safe_divide(new_budget - original_budget, original_budget, 0.0) * 100.0
        )

        # --- 3. Economy-of-scale factor ---
        # +0.5% efficiency per +10% budget (diminishing via sqrt)
        if change_pct >= 0:
            scale_factor = 1.0 + 0.005 * math.sqrt(max(change_pct / 10.0, 0))
        else:
            # Budget decrease: efficiency drops (mirror of the improvement)
            scale_factor = 1.0 - 0.005 * math.sqrt(max(abs(change_pct) / 10.0, 0))
        scale_factor = _clamp(scale_factor, 0.80, 1.25)

        # --- 4. Scale channels proportionally and reproject ---
        budget_ratio = _safe_divide(new_budget, original_budget, 0.0)
        channel_changes: Dict[str, Dict[str, Any]] = {}

        total_orig_clicks = 0
        total_orig_apps = 0
        total_orig_hires = 0
        total_new_clicks = 0
        total_new_apps = 0
        total_new_hires = 0

        for ch_name, ch_data in channel_allocs.items():
            orig_dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
            new_dollars = round(orig_dollars * budget_ratio, 2)
            cpc = ch_data.get("cpc") or 0
            apply_rate = ch_data.get(
                "apply_rate",
                BASE_BENCHMARKS["apply_rate"].get(
                    ch_data.get("category", "job_board"), 0.05
                ),
            )
            hire_rate_val = BASE_BENCHMARKS.get("hire_rate", 0.02)

            # Original metrics
            orig_clicks = ch_data.get("projected_clicks") or 0
            orig_apps = ch_data.get("projected_applications") or 0
            orig_hires = ch_data.get("projected_hires") or 0

            total_orig_clicks += orig_clicks
            total_orig_apps += orig_apps
            total_orig_hires += orig_hires

            # New metrics with economy-of-scale
            if cpc > 0:
                new_clicks = max(0, int((new_dollars / cpc) * scale_factor))
                new_apps = max(0, int(new_clicks * apply_rate))
                new_hires = max(0, int(new_apps * hire_rate_val))
            else:
                new_clicks = 0
                new_apps = max(0, int((new_dollars / 50.0) * scale_factor))
                new_hires = max(0, int(new_apps * hire_rate_val * 2))

            total_new_clicks += new_clicks
            total_new_apps += new_apps
            total_new_hires += new_hires

            channel_changes[ch_name] = {
                "original_dollars": round(orig_dollars, 2),
                "new_dollars": new_dollars,
                "change": round(new_dollars - orig_dollars, 2),
            }

        # --- 5. Compute aggregate impact ---
        original_cpa = round(
            _safe_divide(original_budget, max(total_orig_apps, 1), 0.0), 2
        )
        new_cpa = round(_safe_divide(new_budget, max(total_new_apps, 1), 0.0), 2)
        original_cph = round(
            _safe_divide(original_budget, max(total_orig_hires, 1), 0.0), 2
        )
        new_cph = round(_safe_divide(new_budget, max(total_new_hires, 1), 0.0), 2)
        roi_delta_pct = round(
            _safe_divide(original_cph - new_cph, max(original_cph, 1), 0.0) * 100.0, 1
        )

        impact = {
            "additional_clicks": total_new_clicks - total_orig_clicks,
            "additional_applications": total_new_apps - total_orig_apps,
            "additional_hires": total_new_hires - total_orig_hires,
            "original_hires": total_orig_hires,
            "new_projected_hires": total_new_hires,
            "original_cpa": original_cpa,
            "new_cpa": new_cpa,
            "original_cph": original_cph,
            "new_cph": new_cph,
            "roi_delta_pct": roi_delta_pct,
        }

        # --- 6. Build recommendations ---
        recommendations: List[str] = []
        hire_delta = total_new_hires - total_orig_hires
        if hire_delta > 0:
            recommendations.append(
                f"{change_pct:+.0f}% budget change projects +{hire_delta} additional hires "
                f"({total_orig_hires} -> {total_new_hires})"
            )
        elif hire_delta < 0:
            recommendations.append(
                f"{change_pct:+.0f}% budget reduction projects {hire_delta} fewer hires "
                f"({total_orig_hires} -> {total_new_hires})"
            )
        else:
            recommendations.append(
                f"Budget change of {change_pct:+.0f}% has minimal impact on projected hires "
                f"({total_orig_hires})"
            )

        if new_cpa < original_cpa and change_pct > 0:
            cpa_improvement = (
                _safe_divide(original_cpa - new_cpa, max(original_cpa, 1), 0.0) * 100.0
            )
            recommendations.append(
                f"CPA improves by {cpa_improvement:.1f}% due to economies of scale"
            )
        elif new_cpa > original_cpa and change_pct < 0:
            cpa_degradation = (
                _safe_divide(new_cpa - original_cpa, max(original_cpa, 1), 0.0) * 100.0
            )
            recommendations.append(
                f"CPA degrades by {cpa_degradation:.1f}% due to loss of scale efficiencies"
            )

        if change_pct > 50:
            recommendations.append(
                "Budget increases above 50% see diminishing returns; consider "
                "phased investment with performance checkpoints"
            )
        if new_budget < 1000:
            recommendations.append(
                "New budget is very low; most channels will not have enough spend "
                "for meaningful reach"
            )

        logger.info(
            "simulate_budget_change: $%.0f -> $%.0f (%+.1f%%), hires %d -> %d",
            original_budget,
            new_budget,
            change_pct,
            total_orig_hires,
            total_new_hires,
        )

        return {
            "scenario": "budget_change",
            "original_budget": round(original_budget, 2),
            "new_budget": round(new_budget, 2),
            "change_pct": round(change_pct, 1),
            "impact": impact,
            "channel_changes": channel_changes,
            "recommendations": recommendations,
        }

    except Exception as exc:
        logger.error("simulate_budget_change failed: %s", exc)
        return _empty_scenario("budget_change", str(exc))


def simulate_channel_swap(
    base_allocation: Dict[str, Any],
    remove_channel: str = "",
    add_channel: str = "",
    rebalance: bool = True,
) -> Dict[str, Any]:
    """
    Simulate: "What if we replace channel X with channel Y?"

    Removes a channel's budget from the allocation and either assigns it
    to a new channel or redistributes it proportionally across the
    remaining channels.  Quality scores from ``CHANNEL_QUALITY_SCORES``
    are compared before and after to quantify the quality-of-hire impact.

    Args:
        base_allocation: Result dict from ``calculate_budget_allocation()``.
        remove_channel: Channel key or name to remove.  Empty string means
                        "don't remove anything" (pure addition).
        add_channel: Channel key or name to add.  Empty string means
                     "redistribute only" (pure removal).
        rebalance: When ``True`` and ``add_channel`` is empty, distribute
                   the freed budget proportionally across remaining
                   channels.  Ignored when ``add_channel`` is provided.

    Returns:
        Dict with ``removed``, ``added``, ``impact`` (quality/CPA/hire
        deltas), ``budget_redistribution``, and ``recommendations``.
    """
    try:
        channel_allocs = base_allocation.get("channel_allocations", {})
        if not channel_allocs:
            logger.warning("simulate_channel_swap: no channel_allocations in base")
            return _empty_scenario(
                "channel_swap", "No channel allocations in base result"
            )

        if not remove_channel and not add_channel:
            return _empty_scenario(
                "channel_swap", "No channel specified to add or remove"
            )

        # Determine collar type from metadata
        metadata = base_allocation.get("metadata", {})
        collar_type = metadata.get("collar_type_used", "white_collar")
        industry = metadata.get("industry") or ""
        if collar_type == "both":
            collar_type = "white_collar"

        total_budget = sum(
            ch.get("dollar_amount", ch.get("dollars") or 0)
            for ch in channel_allocs.values()
        )

        # --- 1. Compute original quality (weighted by dollar share) ---
        orig_weighted_quality = 0.0
        orig_total_hires = 0
        orig_total_apps = 0
        for ch_name, ch_data in channel_allocs.items():
            dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
            weight = _safe_divide(dollars, max(total_budget, 1), 0.0)
            q_info = score_channel_quality(ch_name, collar_type, industry)
            orig_weighted_quality += q_info["quality_score"] * weight
            orig_total_hires += ch_data.get("projected_hires") or 0
            orig_total_apps += ch_data.get("projected_applications") or 0

        original_cpa = round(
            _safe_divide(total_budget, max(orig_total_apps, 1), 0.0), 2
        )

        # --- 2. Identify freed budget from removed channel ---
        freed_budget = 0.0
        matched_remove_key = ""
        if remove_channel:
            # Try exact match first, then fuzzy
            for ch_name in channel_allocs:
                if (
                    ch_name == remove_channel
                    or ch_name.lower() == remove_channel.lower()
                ):
                    matched_remove_key = ch_name
                    break
            if not matched_remove_key:
                # Try category-based matching
                remove_cat = _category_for_channel(remove_channel)
                for ch_name, ch_data in channel_allocs.items():
                    if ch_data.get("category") or "" == remove_cat:
                        matched_remove_key = ch_name
                        break
            if matched_remove_key:
                freed_budget = channel_allocs[matched_remove_key].get(
                    "dollar_amount",
                    channel_allocs[matched_remove_key].get("dollars") or 0,
                )

        if remove_channel and not matched_remove_key:
            logger.warning(
                "simulate_channel_swap: channel '%s' not found in allocations",
                remove_channel,
            )
            return _empty_scenario(
                "channel_swap",
                f"Channel '{remove_channel}' not found in current allocations",
            )

        # --- 3. Build new allocation ---
        new_allocs: Dict[str, Dict[str, Any]] = {}
        remaining_budget = total_budget - freed_budget

        # Copy existing channels except the removed one
        for ch_name, ch_data in channel_allocs.items():
            if ch_name == matched_remove_key:
                continue
            new_allocs[ch_name] = dict(ch_data)

        # --- 4. Add new channel or rebalance ---
        if add_channel:
            # Assign freed budget to the new channel
            add_dollars = freed_budget if freed_budget > 0 else 0.0
            add_category = _category_for_channel(add_channel)
            add_cpc = BASE_BENCHMARKS["cpc"].get(add_category, 0.85)
            add_apply_rate = BASE_BENCHMARKS["apply_rate"].get(add_category, 0.05)
            hire_rate_val = BASE_BENCHMARKS.get("hire_rate", 0.02)

            if add_cpc > 0:
                add_clicks = max(0, int(add_dollars / add_cpc))
                add_apps = max(0, int(add_clicks * add_apply_rate))
                add_hires = max(0, int(add_apps * hire_rate_val))
            else:
                add_clicks = 0
                add_apps = max(0, int(add_dollars / 50.0))
                add_hires = max(0, int(add_apps * hire_rate_val * 2))

            new_allocs[add_channel] = {
                "dollar_amount": round(add_dollars, 2),
                "percentage": round(
                    _safe_divide(add_dollars, max(total_budget, 1), 0.0) * 100, 1
                ),
                "cpc": round(add_cpc, 2),
                "projected_clicks": add_clicks,
                "projected_applications": add_apps,
                "projected_hires": add_hires,
                "cpa": round(
                    _safe_divide(add_dollars, max(add_apps, 1), add_dollars), 2
                ),
                "cost_per_hire": round(
                    _safe_divide(add_dollars, max(add_hires, 1), add_dollars), 2
                ),
                "category": add_category,
            }

        elif rebalance and freed_budget > 0 and new_allocs:
            # Distribute freed budget proportionally across remaining channels
            remaining_total = sum(
                ch.get("dollar_amount", ch.get("dollars") or 0)
                for ch in new_allocs.values()
            )
            for ch_name, ch_data in new_allocs.items():
                ch_dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
                share = _safe_divide(ch_dollars, max(remaining_total, 1), 0.0)
                additional = freed_budget * share
                new_dollars = ch_dollars + additional
                ch_data["dollar_amount"] = round(new_dollars, 2)

                # Reproject outcomes
                cpc = ch_data.get("cpc", 0.85)
                apply_rate = ch_data.get(
                    "apply_rate",
                    BASE_BENCHMARKS["apply_rate"].get(
                        ch_data.get("category", "job_board"), 0.05
                    ),
                )
                hire_rate_val = BASE_BENCHMARKS.get("hire_rate", 0.02)

                if cpc > 0:
                    ch_data["projected_clicks"] = max(0, int(new_dollars / cpc))
                    ch_data["projected_applications"] = max(
                        0, int(ch_data["projected_clicks"] * apply_rate)
                    )
                    ch_data["projected_hires"] = max(
                        0, int(ch_data["projected_applications"] * hire_rate_val)
                    )
                else:
                    ch_data["projected_clicks"] = 0
                    ch_data["projected_applications"] = max(0, int(new_dollars / 50.0))
                    ch_data["projected_hires"] = max(
                        0, int(ch_data["projected_applications"] * hire_rate_val * 2)
                    )

        # --- 5. Compute new quality and metrics ---
        new_weighted_quality = 0.0
        new_total_hires = 0
        new_total_apps = 0
        budget_redistribution: Dict[str, float] = {}

        for ch_name, ch_data in new_allocs.items():
            dollars = ch_data.get("dollar_amount", ch_data.get("dollars") or 0)
            weight = _safe_divide(dollars, max(total_budget, 1), 0.0)
            q_info = score_channel_quality(ch_name, collar_type, industry)
            new_weighted_quality += q_info["quality_score"] * weight
            new_total_hires += ch_data.get("projected_hires") or 0
            new_total_apps += ch_data.get("projected_applications") or 0
            budget_redistribution[ch_name] = round(
                _safe_divide(dollars, max(total_budget, 1), 0.0) * 100, 1
            )

        new_cpa = round(_safe_divide(total_budget, max(new_total_apps, 1), 0.0), 2)
        quality_change = round(new_weighted_quality - orig_weighted_quality, 3)
        cpa_change_pct = round(
            _safe_divide(new_cpa - original_cpa, max(original_cpa, 1), 0.0) * 100, 1
        )
        hires_change = new_total_hires - orig_total_hires

        # --- 6. Build recommendations ---
        recommendations: List[str] = []
        if remove_channel and add_channel:
            recommendations.append(
                f"Swapping {remove_channel} for {add_channel} "
                f"{'improves' if quality_change > 0 else 'reduces'} "
                f"weighted quality by {abs(quality_change):.3f}"
            )
        elif remove_channel:
            recommendations.append(
                f"Removing {remove_channel} and redistributing ${freed_budget:,.0f} "
                f"across remaining channels"
            )
        elif add_channel:
            recommendations.append(f"Adding {add_channel} to the channel mix")

        if hires_change > 0:
            recommendations.append(
                f"Projected hires increase by {hires_change} "
                f"({orig_total_hires} -> {new_total_hires})"
            )
        elif hires_change < 0:
            recommendations.append(
                f"Projected hires decrease by {abs(hires_change)} "
                f"({orig_total_hires} -> {new_total_hires})"
            )

        if cpa_change_pct < -2:
            recommendations.append(
                f"CPA improves by {abs(cpa_change_pct):.1f}% "
                f"(${original_cpa:,.2f} -> ${new_cpa:,.2f})"
            )
        elif cpa_change_pct > 2:
            recommendations.append(
                f"CPA worsens by {cpa_change_pct:.1f}% "
                f"(${original_cpa:,.2f} -> ${new_cpa:,.2f})"
            )

        if quality_change > 0.05:
            recommendations.append(
                "The new channel mix significantly improves quality-of-hire; "
                "consider this swap for long-term retention gains"
            )
        elif quality_change < -0.05:
            recommendations.append(
                "The new channel mix may reduce quality-of-hire; weigh the "
                "cost savings against potential retention risk"
            )

        logger.info(
            "simulate_channel_swap: remove=%s, add=%s, quality_delta=%+.3f, "
            "hires_delta=%+d, cpa_delta=%+.1f%%",
            remove_channel,
            add_channel,
            quality_change,
            hires_change,
            cpa_change_pct,
        )

        return {
            "scenario": "channel_swap",
            "removed": remove_channel,
            "added": add_channel,
            "freed_budget": round(freed_budget, 2),
            "impact": {
                "quality_change": quality_change,
                "cpa_change_pct": cpa_change_pct,
                "projected_hires_change": hires_change,
                "original_hires": orig_total_hires,
                "new_projected_hires": new_total_hires,
                "original_cpa": original_cpa,
                "new_cpa": new_cpa,
                "original_weighted_quality": round(orig_weighted_quality, 3),
                "new_weighted_quality": round(new_weighted_quality, 3),
                "budget_redistribution": budget_redistribution,
            },
            "recommendations": recommendations,
        }

    except Exception as exc:
        logger.error("simulate_channel_swap failed: %s", exc)
        return _empty_scenario("channel_swap", str(exc))


def simulate_what_if(
    base_allocation: Dict[str, Any],
    scenario_description: str = "",
    delta_budget: float = 0.0,
    delta_pct: float = 0.0,
    add_channel: str = "",
    remove_channel: str = "",
) -> Dict[str, Any]:
    """
    Unified entry point for what-if scenarios.

    Routes to ``simulate_budget_change`` or ``simulate_channel_swap``
    (or both) based on which parameters are provided.  When both a budget
    change and a channel change are requested, the budget change is applied
    first, then the channel swap is simulated on the adjusted result.

    Args:
        base_allocation: Result dict from ``calculate_budget_allocation()``.
        scenario_description: Free-text description of the scenario
                              (logged for traceability, not parsed).
        delta_budget: Absolute budget change in USD.
        delta_pct: Percentage budget change (0.20 = +20%).
        add_channel: Channel to add to the mix.
        remove_channel: Channel to remove from the mix.

    Returns:
        Dict with ``scenario_description``, ``budget_impact`` (if budget
        change requested), ``channel_impact`` (if channel change requested),
        and a merged ``recommendations`` list.
    """
    try:
        logger.info(
            "simulate_what_if: desc='%s', delta_budget=%.0f, delta_pct=%.2f, "
            "add=%s, remove=%s",
            scenario_description,
            delta_budget,
            delta_pct,
            add_channel,
            remove_channel,
        )

        has_budget_change = delta_budget != 0.0 or delta_pct != 0.0
        has_channel_change = bool(add_channel) or bool(remove_channel)

        if not has_budget_change and not has_channel_change:
            return {
                "scenario_description": scenario_description or "No changes specified",
                "budget_impact": None,
                "channel_impact": None,
                "recommendations": ["No budget or channel changes were specified."],
            }

        budget_result = None
        channel_result = None
        all_recommendations: List[str] = []

        # --- Budget change ---
        if has_budget_change:
            budget_result = simulate_budget_change(
                base_allocation,
                delta_budget=delta_budget,
                delta_pct=delta_pct,
            )
            all_recommendations.extend(budget_result.get("recommendations") or [])

        # --- Channel swap ---
        if has_channel_change:
            # If we also had a budget change, build an intermediate
            # base_allocation with scaled channel dollars so the channel
            # swap operates on the adjusted budget.
            swap_base = base_allocation
            if budget_result and budget_result.get("channel_changes"):
                swap_base = _build_intermediate_allocation(
                    base_allocation, budget_result
                )

            channel_result = simulate_channel_swap(
                swap_base,
                remove_channel=remove_channel,
                add_channel=add_channel,
                rebalance=True,
            )
            all_recommendations.extend(channel_result.get("recommendations") or [])

        return {
            "scenario_description": scenario_description
            or _auto_describe(delta_budget, delta_pct, add_channel, remove_channel),
            "budget_impact": budget_result,
            "channel_impact": channel_result,
            "recommendations": all_recommendations,
        }

    except Exception as exc:
        logger.error("simulate_what_if failed: %s", exc)
        return {
            "scenario_description": scenario_description or "Error",
            "budget_impact": None,
            "channel_impact": None,
            "recommendations": [f"Simulation failed: {exc}"],
        }


# ---------------------------------------------------------------------------
# What-If private helpers
# ---------------------------------------------------------------------------


def _empty_scenario(scenario_type: str, reason: str = "") -> Dict[str, Any]:
    """Return a structurally valid but empty scenario result."""
    base: Dict[str, Any] = {
        "scenario": scenario_type,
        "recommendations": [reason] if reason else [],
    }
    if scenario_type == "budget_change":
        base.update(
            {
                "original_budget": 0.0,
                "new_budget": 0.0,
                "change_pct": 0.0,
                "impact": {
                    "additional_clicks": 0,
                    "additional_applications": 0,
                    "additional_hires": 0,
                    "original_hires": 0,
                    "new_projected_hires": 0,
                    "original_cpa": 0.0,
                    "new_cpa": 0.0,
                    "original_cph": 0.0,
                    "new_cph": 0.0,
                    "roi_delta_pct": 0.0,
                },
                "channel_changes": {},
            }
        )
    elif scenario_type == "channel_swap":
        base.update(
            {
                "removed": "",
                "added": "",
                "freed_budget": 0.0,
                "impact": {
                    "quality_change": 0.0,
                    "cpa_change_pct": 0.0,
                    "projected_hires_change": 0,
                    "original_hires": 0,
                    "new_projected_hires": 0,
                    "original_cpa": 0.0,
                    "new_cpa": 0.0,
                    "original_weighted_quality": 0.0,
                    "new_weighted_quality": 0.0,
                    "budget_redistribution": {},
                },
            }
        )
    return base


def _build_intermediate_allocation(
    base_allocation: Dict[str, Any],
    budget_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a synthetic base_allocation with channel dollars adjusted per
    ``budget_result["channel_changes"]``.

    This lets ``simulate_channel_swap`` operate on the budget-adjusted
    state when both a budget change and a channel swap are requested
    together in ``simulate_what_if``.
    """
    intermediate = {
        "channel_allocations": {},
        "metadata": dict(base_allocation.get("metadata", {})),
    }
    channel_changes = budget_result.get("channel_changes", {})
    orig_allocs = base_allocation.get("channel_allocations", {})

    for ch_name, ch_data in orig_allocs.items():
        new_ch = dict(ch_data)
        change_info = channel_changes.get(ch_name, {})
        if change_info:
            new_ch["dollar_amount"] = change_info.get(
                "new_dollars",
                ch_data.get("dollar_amount", ch_data.get("dollars") or 0),
            )
        intermediate["channel_allocations"][ch_name] = new_ch

    # Update metadata budget
    if budget_result.get("new_budget"):
        intermediate["metadata"]["total_budget"] = budget_result["new_budget"]

    return intermediate


def _auto_describe(
    delta_budget: float,
    delta_pct: float,
    add_channel: str,
    remove_channel: str,
) -> str:
    """Generate a human-readable scenario description from parameters."""
    parts: List[str] = []
    if delta_budget != 0:
        parts.append(
            f"{'Increase' if delta_budget > 0 else 'Decrease'} "
            f"budget by ${abs(delta_budget):,.0f}"
        )
    elif delta_pct != 0:
        parts.append(
            f"{'Increase' if delta_pct > 0 else 'Decrease'} "
            f"budget by {abs(delta_pct) * 100:.0f}%"
        )
    if remove_channel and add_channel:
        parts.append(f"swap {remove_channel} for {add_channel}")
    elif remove_channel:
        parts.append(f"remove {remove_channel}")
    elif add_channel:
        parts.append(f"add {add_channel}")
    return "; ".join(parts) if parts else "No-op scenario"
