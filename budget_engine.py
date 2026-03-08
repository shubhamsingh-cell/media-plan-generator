"""
Budget Allocation Engine -- converts percentage allocations to concrete dollar amounts.

Takes the user's total budget, role/location details, synthesized market data,
and produces per-channel spend recommendations with projected outcomes
(clicks, applications, hires).

This module is intentionally self-contained: it imports only stdlib so that
the rest of the pipeline can call it without circular-dependency risk.
The caller (app.py or data_orchestrator.py) passes in any enrichment
data it has already fetched.

Future enhancement: Incorporate orchestrator's EnrichmentContext confidence
scores to weight channel allocation reliability and flag low-confidence
projections in the output.
"""

import logging
import math
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

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

# Base benchmarks (USD) -- overridden by live data when available.
# Keys map to *ad-platform categories* used in compute_channel_dollar_amounts.
BASE_BENCHMARKS: Dict[str, Any] = {
    "cpc": {
        "job_board": 0.85,
        "social": 1.20,
        "search": 2.50,
        "programmatic": 0.65,
        "display": 0.45,
        "niche_board": 1.40,
        "employer_branding": 0.90,
        "referral": 0.00,       # referral programmes have no click cost
        "events": 0.00,         # events are flat-fee, not CPC
        "staffing": 0.00,       # agencies bill per placement
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

# C4 FIX: Role-tier-specific hire rates instead of universal 2%
HIRE_RATE_BY_TIER: Dict[str, float] = {
    "Hourly / Entry-Level": 0.06,          # high-volume, lower bar
    "Skilled Trades / Technical": 0.04,     # CDL, warehouse, construction
    "Clinical / Licensed": 0.03,            # nurses, therapists — credentialing bottleneck
    "Professional / White-Collar": 0.02,    # standard corporate roles
    "Executive / Leadership": 0.008,        # highly selective
    "Technology / Engineering": 0.015,      # competitive market
    "Sales / Revenue": 0.035,              # high turnover, faster hiring
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
    "technology": 4000, "healthcare": 3500, "finance": 4500,
    "engineering": 5000, "executive": 8000, "legal": 5000,
    "pharmaceutical": 6000, "energy": 4000, "aerospace": 5500,
    "manufacturing": 2500, "construction": 2000, "retail": 1200,
    "hospitality": 800, "logistics": 1500, "education": 2000,
    "government": 2500, "nonprofit": 1800, "general": 2000,
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
    tier = role.get("tier", "") or role.get("role_tier", "")
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
            cpc_val = plat.get("avg_cpc", 0)
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
                cpc_val = kw_data.get("avg_cpc_usd", 0)
                if isinstance(cpc_val, (int, float)) and cpc_val > 0:
                    candidate_cpcs.append(float(cpc_val))
            # Platform-level summary (Meta / LinkedIn shape)
            for sub_key in ("facebook", "instagram", "linkedin", "tiktok"):
                sub = platform_data.get(sub_key) or {}
                cpc_val = sub.get("avg_cpc_usd", 0)
                if isinstance(cpc_val, (int, float)) and cpc_val > 0:
                    candidate_cpcs.append(float(cpc_val))
            # Top-level avg_cpc_usd
            top_cpc = platform_data.get("avg_cpc_usd", 0)
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
        deep_key = meta.get("deep_bench_key", "")
        if deep_key and deep_key in INDUSTRY_CPH_RANGES:
            low, high = INDUSTRY_CPH_RANGES[deep_key]
            return (low + high) / 2.0
        # deep_bench_key might not match CPH keys exactly;
        # scan aliases for a match in INDUSTRY_CPH_RANGES
        for alias in meta.get("aliases", []):
            if alias in INDUSTRY_CPH_RANGES:
                low, high = INDUSTRY_CPH_RANGES[alias]
                return (low + high) / 2.0

    # 3. Fallback to default range
    low, high = _DEFAULT_CPH_RANGE
    return (low + high) / 2.0


def _score_roi(cost_per_hire: float, industry_avg: float) -> int:
    """
    Score ROI on a 1-10 scale.

    10 = cost_per_hire is <=20% of the industry average (exceptional).
     1 = cost_per_hire is >=3x the industry average (terrible).
    """
    if industry_avg <= 0 or cost_per_hire <= 0:
        return 5  # unknown
    ratio = cost_per_hire / industry_avg
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
                        "cost_of_living": {"cost_of_living_index": _lp_val["cost_of_living_index"]},
                        "quality_scores": {"Cost of Living": _lp_val.get("quality_of_life_score", 0)},
                    }

    # Known fallback multipliers for major metro areas (relative to US avg)
    _FALLBACK_MULTIPLIERS: Dict[str, float] = {
        "san francisco": 1.45, "new york": 1.40, "manhattan": 1.45,
        "los angeles": 1.25, "boston": 1.30, "seattle": 1.25,
        "chicago": 1.10, "austin": 1.05, "denver": 1.10,
        "dallas": 0.95, "houston": 0.90, "atlanta": 0.95,
        "miami": 1.10, "phoenix": 0.90, "detroit": 0.85,
        "minneapolis": 0.95, "philadelphia": 1.05,
        "washington": 1.25, "portland": 1.10, "san diego": 1.20,
        "nashville": 0.95, "charlotte": 0.90,
        "london": 1.35, "munich": 1.20, "zurich": 1.55,
        "paris": 1.25, "amsterdam": 1.15, "dublin": 1.15,
        "singapore": 1.30, "tokyo": 1.25, "sydney": 1.20,
        "toronto": 1.10, "vancouver": 1.15,
        "bangalore": 0.30, "mumbai": 0.35, "delhi": 0.30,
        "hyderabad": 0.28, "manila": 0.30, "lagos": 0.25,
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
            col_score = qs.get("Cost of Living", 0)
            if col_score > 0:
                # Convert: score 10 -> multiplier ~0.6, score 1 -> multiplier ~1.5
                mult = round(1.55 - (col_score / 10.0) * 0.95, 2)
                multipliers[loc_key] = _clamp(mult, 0.2, 2.5)
                logger.debug("Location %s: Teleport COLI score %.1f -> multiplier %.2f",
                             loc_key, col_score, multipliers[loc_key])
                continue

        # 2. Fallback: known city lookup
        city_lower = loc_key.split(",")[0].strip().lower()
        if city_lower in _FALLBACK_MULTIPLIERS:
            multipliers[loc_key] = _FALLBACK_MULTIPLIERS[city_lower]
            logger.debug("Location %s: fallback multiplier %.2f", loc_key, multipliers[loc_key])
            continue

        # 3. Country-level heuristic
        country = _guess_country(loc)
        if country:
            country_mult = _country_multiplier(country)
            multipliers[loc_key] = country_mult
            logger.debug("Location %s: country-level multiplier %.2f", loc_key, country_mult)
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
        logger.warning("Budget is zero or negative (%.2f); returning zero allocations", total_budget)
        result: Dict[str, Dict] = {}
        for role in roles:
            title = role.get("title", "Unknown Role")
            result[title] = {
                "budget_share": 0.0,
                "dollar_amount": 0.0,
                "tier": _resolve_tier(role),
                "multiplier": _tier_multiplier(_resolve_tier(role)),
                "openings": max(1, int(role.get("count", role.get("openings", 1)) or 1)),
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
        openings = max(1, int(role_dict.get("count", role_dict.get("openings", 1)) or 1))
        result[title] = {
            "budget_share": round(share, 4),
            "dollar_amount": round(total_budget * share, 2),
            "tier": tier,
            "multiplier": _tier_multiplier(tier),
            "openings": openings,
            "headcount": openings,  # alias for hire_rate blending
        }

    logger.info("Role-weighted spend computed for %d roles, total $%.2f", len(result), total_budget)
    return result


def compute_channel_dollar_amounts(
    channel_percentages: Dict[str, float],
    role_budgets: Dict[str, Dict],
    synthesized_data: Optional[Dict] = None,
    knowledge_base: Optional[Dict] = None,
) -> Dict[str, Dict]:
    """
    Convert channel percentages to dollar amounts with projected outcomes.

    For each channel:
        1. Dollar amount = total_role_budget * (pct / 100)
        2. CPC from synthesized ad-platform data, KB benchmarks, or BASE_BENCHMARKS
        3. Projected clicks = dollars / CPC
        4. Projected applications = clicks * apply_rate
        5. Projected hires = applications * hire_rate
        6. Effective CPA = dollars / applications
        7. Effective cost_per_hire = dollars / hires
        8. ROI score (1-10) against industry average

    Args:
        channel_percentages: ``{channel_name: percentage}`` (0-100).
        role_budgets: Output of ``compute_role_weighted_spend``.
        synthesized_data: Enrichment payload (optional).
        knowledge_base: Loaded knowledge base JSON (optional).

    Returns:
        Dict keyed by channel name, each value a dict with:
        ``dollars``, ``cpc``, ``projected_clicks``, ``projected_applications``,
        ``projected_hires``, ``cpa``, ``cost_per_hire``, ``roi_score``,
        ``confidence``, ``category``.
    """
    total_budget = sum(rb.get("dollar_amount", 0) for rb in role_budgets.values())
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
    industry_avg_cph = 6_000.0  # fallback; caller can override via assess_budget_sufficiency

    allocations: Dict[str, Dict] = {}
    for ch_name, raw_pct in channel_percentages.items():
        pct = raw_pct * norm_factor
        dollars = round(total_budget * pct / 100.0, 2)
        category = _category_for_channel(ch_name)

        # Resolve CPC: synthesized > KB > base benchmark
        cpc = _extract_cpc_from_synthesized(category, synthesized_data)
        confidence = "high" if cpc is not None else None

        if cpc is None:
            cpc = _extract_cpc_from_kb(category, knowledge_base)
            if cpc is not None:
                confidence = "medium"

        if cpc is None:
            cpc = BASE_BENCHMARKS["cpc"].get(category, 0.85)
            confidence = "low"

        apply_rate = BASE_BENCHMARKS["apply_rate"].get(category, 0.05)

        # For channels without a CPC model (referral, events, staffing),
        # we estimate outcomes differently.
        if cpc <= 0:
            # Flat-cost channels: estimate a synthetic CPA instead
            projected_clicks = 0
            projected_applications = max(1, int(dollars / 50.0))  # ~$50/application heuristic
            projected_hires = max(0, int(projected_applications * hire_rate * 2))  # higher quality
            cpa = _safe_divide(dollars, projected_applications, dollars)
            cost_per_hire = _safe_divide(dollars, max(projected_hires, 1), dollars)
        else:
            projected_clicks = max(0, int(dollars / cpc))
            projected_applications = max(0, int(projected_clicks * apply_rate))
            projected_hires = max(0, int(projected_applications * hire_rate))
            cpa = _safe_divide(dollars, max(projected_applications, 1), dollars)
            cost_per_hire = _safe_divide(dollars, max(projected_hires, 1), dollars)

        roi = _score_roi(cost_per_hire, industry_avg_cph)

        allocations[ch_name] = {
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
        }

    logger.info("Channel dollar amounts computed for %d channels", len(allocations))
    return allocations


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
        ch.get("projected_hires", 0) for ch in channel_allocations.values()
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
    budget_utilization = (total_budget / min_viable_budget * 100) if min_viable_budget > 0 else 0

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
        name for name, ch in channel_allocations.items()
        if ch.get("roi_score", 5) <= 3 and ch.get("dollar_amount", ch.get("dollars", 0)) > 0
    ]
    if low_roi_channels:
        recommendations.append(
            f"Channels with low ROI scores ({', '.join(low_roi_channels)}) "
            f"may benefit from budget reallocation to higher-performing channels."
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
        "realistic_hires": max(1, int(total_budget / industry_min_cph)) if industry_min_cph > 0 else n_openings,
        "budget_utilization_pct": round(budget_utilization, 1),
        "target_hires": n_openings,
    }

    return result


def optimize_allocation(
    channel_allocations: Dict,
    total_budget: float,
    optimization_goal: str = "hires",
) -> Dict[str, Any]:
    """
    Suggest reallocation to optimise for the specified goal.

    Shifts budget from low-ROI channels to high-ROI channels.
    Never reduces any channel below 5% of its original allocation.

    Args:
        channel_allocations: Output of ``compute_channel_dollar_amounts``.
        total_budget: Total budget in USD.
        optimization_goal: One of ``"hires"``, ``"applications"``, ``"clicks"``.

    Returns:
        Dict with ``optimized_allocations``, ``improvement``, ``changes``.
    """
    if not channel_allocations:
        logger.warning("No channel allocations to optimise")
        return {
            "optimized_allocations": {},
            "improvement": {"metric": optimization_goal, "original": 0, "optimized": 0, "pct_change": 0.0},
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
        dollars = ch_data.get("dollar_amount", ch_data.get("dollars", 0))
        metric_val = ch_data.get(goal_key, 0)
        efficiencies[ch_name] = _safe_divide(metric_val, dollars, 0.0)

    if not efficiencies or all(v == 0 for v in efficiencies.values()):
        return {
            "optimized_allocations": dict(channel_allocations),
            "improvement": {"metric": optimization_goal, "original": 0, "optimized": 0, "pct_change": 0.0},
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
        orig_dollars = channel_allocations[ch_name].get("dollar_amount", channel_allocations[ch_name].get("dollars", 0))
        max_reduction = orig_dollars * 0.30  # never take more than 30%
        floor = orig_dollars * 0.05          # keep at least 5%
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

    for ch_name, ch_data in channel_allocations.items():
        orig_dollars = ch_data.get("dollar_amount", ch_data.get("dollars", 0))
        orig_pct = ch_data.get("percentage", 0)
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
        apply_rate = BASE_BENCHMARKS["apply_rate"].get(category, 0.05)
        hire_rate = BASE_BENCHMARKS["hire_rate"]

        if cpc > 0:
            new_clicks = max(0, int(new_dollars / cpc))
            new_apps = max(0, int(new_clicks * apply_rate))
            new_hires = max(0, int(new_apps * hire_rate))
        else:
            new_clicks = 0
            new_apps = max(1, int(new_dollars / 50.0))
            new_hires = max(0, int(new_apps * hire_rate * 2))

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
        opt_entry["cpa"] = round(_safe_divide(new_dollars, max(new_apps, 1), new_dollars), 2)
        opt_entry["cost_per_hire"] = round(
            _safe_divide(new_dollars, max(new_hires, 1), new_dollars), 2
        )
        optimized[ch_name] = opt_entry

        if abs(new_dollars - orig_dollars) > 0.01:
            changes.append({
                "channel": ch_name,
                "original_dollars": round(orig_dollars, 2),
                "new_dollars": round(new_dollars, 2),
                "original_pct": round(orig_pct, 1),
                "new_pct": round(new_pct, 1),
                "reason": reason,
            })

    pct_change = _safe_divide(
        optimized_metric_total - original_metric_total,
        max(original_metric_total, 1),
        0.0,
    ) * 100

    logger.info(
        "Optimisation for '%s': %d -> %d (%.1f%% improvement)",
        optimization_goal, original_metric_total, optimized_metric_total, pct_change,
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
) -> Dict[str, Any]:
    """
    Master budget allocation function.

    Orchestrates location multipliers, role weighting, channel dollar
    calculations, sufficiency assessment, and optimisation suggestions
    into a single comprehensive result.

    Args:
        total_budget: Total budget in USD (e.g. 50000).
        roles: List of role dicts with ``title``, ``count``, ``tier``.
        locations: List of location dicts with ``city``, ``state``, ``country``.
        industry: Industry classification string.
        channel_percentages: Dict of ``channel_name -> percentage`` (0-100).
        synthesized_data: Output from enrichment pipeline (optional).
        knowledge_base: Loaded knowledge base JSON (optional).

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
        total_budget, len(roles), len(locations), industry,
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

    # Step 2: Role-weighted spend
    role_budgets = compute_role_weighted_spend(
        roles, total_budget, location_multipliers
    )

    # Step 3: Channel dollar amounts with projections
    channel_allocs = compute_channel_dollar_amounts(
        channel_percentages, role_budgets, synthesized_data, knowledge_base
    )

    # Step 4: Aggregate projected totals
    total_clicks = sum(ch.get("projected_clicks", 0) for ch in channel_allocs.values())
    total_apps = sum(ch.get("projected_applications", 0) for ch in channel_allocs.values())
    total_hires = sum(ch.get("projected_hires", 0) for ch in channel_allocs.values())
    avg_cost_per_hire = _safe_divide(total_budget, max(total_hires, 1), total_budget)

    total_projected = {
        "clicks": total_clicks,
        "applications": total_apps,
        "hires": total_hires,
        "cost_per_hire": round(avg_cost_per_hire, 2),
        "cost_per_application": round(_safe_divide(total_budget, max(total_apps, 1), 0), 2),
        "cost_per_click": round(_safe_divide(total_budget, max(total_clicks, 1), 0), 2),
    }

    # Step 5: Budget sufficiency assessment
    total_openings = sum(
        max(1, int(r.get("count", r.get("openings", 1)) or 1))
        for r in (roles or [{"count": 1}])
    )

    sufficiency = assess_budget_sufficiency(
        total_budget, total_openings, industry,
        channel_allocs, knowledge_base,
    )

    # Step 6: Optimisation suggestions
    optimized = optimize_allocation(channel_allocs, total_budget, "hires")

    # Consolidate warnings and recommendations
    all_warnings = list(sufficiency.get("warnings", []))
    all_recommendations = list(sufficiency.get("recommendations", []))
    if optimized.get("improvement", {}).get("pct_change", 0) > 5:
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
        },
    }

    logger.info(
        "Budget allocation complete: $%.2f -> %d clicks, %d applications, "
        "%d projected hires (CPH $%.0f)",
        total_budget, total_clicks, total_apps, total_hires, avg_cost_per_hire,
    )

    return result


# ---------------------------------------------------------------------------
# Private helpers (location processing)
# ---------------------------------------------------------------------------

def _location_key(loc: Dict) -> str:
    """Build a stable string key from a location dict."""
    if isinstance(loc, str):
        return loc.strip()
    city = loc.get("city", loc.get("location", loc.get("name", "")))
    state = loc.get("state", loc.get("region", ""))
    country = loc.get("country", "")
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
        return str(loc.get("country", "")).strip()
    return ""


def _country_multiplier(country: str) -> float:
    """Return a cost multiplier for a country relative to US baseline."""
    _COUNTRY_MULTIPLIERS: Dict[str, float] = {
        "united states": 1.0, "us": 1.0, "usa": 1.0,
        "united kingdom": 1.15, "uk": 1.15, "gb": 1.15,
        "canada": 1.05, "ca": 1.05,
        "australia": 1.10, "au": 1.10,
        "germany": 1.15, "de": 1.15,
        "france": 1.10, "fr": 1.10,
        "netherlands": 1.10, "nl": 1.10,
        "switzerland": 1.50, "ch": 1.50,
        "japan": 1.15, "jp": 1.15,
        "singapore": 1.25, "sg": 1.25,
        "india": 0.30, "in": 0.30,
        "philippines": 0.28, "ph": 0.28,
        "mexico": 0.40, "mx": 0.40,
        "brazil": 0.45, "br": 0.45,
        "china": 0.50, "cn": 0.50,
        "south korea": 0.85, "kr": 0.85,
        "poland": 0.55, "pl": 0.55,
        "romania": 0.40, "ro": 0.40,
        "ireland": 1.12, "ie": 1.12,
        "israel": 1.15, "il": 1.15,
        "uae": 1.10, "ae": 1.10,
        "saudi arabia": 0.90, "sa": 0.90,
        "nigeria": 0.20, "ng": 0.20,
        "kenya": 0.22, "ke": 0.22,
        "south africa": 0.35, "za": 0.35,
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
            "improvement": {"metric": "hires", "original": 0, "optimized": 0, "pct_change": 0.0},
            "changes": [],
        },
        "metadata": {},
    }
