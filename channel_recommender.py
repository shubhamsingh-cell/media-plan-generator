"""Channel Recommendations Engine -- actionable channel mix for recruitment campaigns.

Combines platform fit scores (20 industries x 10 channels), role-tier CPA multipliers,
budget constraints, and location factors into tiered recommendations with projected ROI.
"""

from __future__ import annotations

import json
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── International benchmarks (38 countries) -- loaded once at module level ──
_intl_benchmarks_cache: Optional[Dict] = None


def _load_intl_benchmarks() -> Dict:
    """Load international_benchmarks_2026.json once, cache in module global."""
    global _intl_benchmarks_cache
    if _intl_benchmarks_cache is not None:
        return _intl_benchmarks_cache
    _path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "data",
        "international_benchmarks_2026.json",
    )
    try:
        with open(_path, "r", encoding="utf-8") as fh:
            _intl_benchmarks_cache = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.error("Failed to load intl benchmarks: %s", exc, exc_info=True)
        _intl_benchmarks_cache = {}
    return _intl_benchmarks_cache


def _get_intl_local_platforms(locations: List[str]) -> List[Dict[str, Any]]:
    """Extract local platform recommendations from intl benchmarks for given locations.

    Returns a list of dicts with keys: name, country, cpc_usd, cpa_usd, apply_rate_pct, notes.
    """
    intl = _load_intl_benchmarks()
    countries = intl.get("countries", {})
    results: List[Dict[str, Any]] = []
    _seen: set = set()
    for loc in locations:
        loc_lower = (loc or "").lower().strip()
        for _ck, _cv in countries.items():
            _cname = (_cv.get("name") or "").lower()
            if _ck in loc_lower or _cname in loc_lower or loc_lower in _cname:
                for p in (_cv.get("platforms") or [])[:3]:
                    _pname = p.get("name", "")
                    if _pname and _pname not in _seen:
                        _seen.add(_pname)
                        _cpc = p.get("cpc_usd", {})
                        _cpa = p.get("cpa_usd", {})
                        results.append(
                            {
                                "name": _pname,
                                "country": _cv.get("name", _ck),
                                "cpc_usd": (
                                    _cpc.get("median", 0)
                                    if isinstance(_cpc, dict)
                                    else 0
                                ),
                                "cpa_usd": (
                                    _cpa.get("median", 0)
                                    if isinstance(_cpa, dict)
                                    else 0
                                ),
                                "apply_rate_pct": p.get("apply_rate_pct", 0),
                                "notes": p.get("notes", ""),
                                "type": p.get("type", "job_board"),
                            }
                        )
                break
    return results


# Imports from existing modules (fail-safe)

try:
    from ppt_generator import INDUSTRY_ALLOC_PROFILES, CHANNEL_ALLOC
except Exception:  # May fail due to transitive import errors (research.py on <3.10)
    INDUSTRY_ALLOC_PROFILES = {}
    CHANNEL_ALLOC = {}

try:
    from budget_engine import (
        BASE_BENCHMARKS,
        ROLE_TIER_MULTIPLIERS,
        HIRE_RATE_BY_TIER,
        CHANNEL_NAME_TO_CATEGORY,
    )
except Exception:
    BASE_BENCHMARKS = {"cpc": {}, "apply_rate": {}, "hire_rate": 0.02}
    ROLE_TIER_MULTIPLIERS = {}
    HIRE_RATE_BY_TIER = {"default": 0.02}
    CHANNEL_NAME_TO_CATEGORY = {}

try:
    from shared_utils import INDUSTRY_LABEL_MAP, parse_budget
except Exception:
    INDUSTRY_LABEL_MAP = {}

    def parse_budget(b: Any, *, default: float = 100_000.0) -> float:
        """Stub budget parser."""
        try:
            return float(b)
        except (TypeError, ValueError):
            return default


# Platform fit scores (compact).  Authoritative source: data_synthesizer.py.
# Order: Google, Meta, LinkedIn, TikTok, Bing, Snap, X, Programmatic, Indeed, Zip
_FIT_CHANNELS = [
    "Google Ads",
    "Meta (Facebook/Instagram)",
    "LinkedIn Ads",
    "TikTok Ads",
    "Microsoft/Bing Ads",
    "Snapchat Ads",
    "X (Twitter) Ads",
    "Programmatic Display (DSP)",
    "Indeed Sponsored Jobs",
    "ZipRecruiter Sponsored",
]
_FIT_RAW: Dict[str, Tuple[int, ...]] = {
    "tech_engineering": (8, 5, 9, 3, 6, 2, 7, 7, 7, 5),
    "healthcare_medical": (7, 6, 7, 3, 5, 2, 3, 8, 9, 8),
    "retail_consumer": (7, 9, 3, 8, 4, 7, 4, 8, 9, 7),
    "finance_banking": (8, 5, 9, 2, 7, 2, 5, 7, 7, 6),
    "blue_collar_trades": (7, 8, 3, 6, 4, 5, 3, 8, 9, 8),
    "hospitality_travel": (6, 8, 3, 8, 3, 7, 3, 7, 9, 8),
    "transportation_logistics": (7, 6, 4, 4, 5, 3, 3, 8, 9, 8),
    "manufacturing": (7, 7, 5, 4, 5, 3, 3, 8, 9, 8),
    "construction_real_estate": (7, 7, 4, 5, 4, 4, 3, 8, 9, 8),
    "government_public_sector": (6, 5, 8, 2, 6, 2, 4, 7, 8, 7),
    "education": (7, 6, 8, 3, 5, 2, 4, 7, 8, 6),
    "nonprofit": (8, 7, 7, 4, 5, 3, 5, 6, 8, 6),
    "energy_utilities": (7, 5, 7, 2, 6, 2, 4, 8, 8, 7),
    "professional_services": (8, 5, 9, 2, 7, 1, 5, 6, 7, 5),
    "media_entertainment": (7, 8, 6, 8, 4, 6, 7, 7, 6, 4),
    "insurance": (8, 5, 8, 2, 7, 2, 4, 7, 8, 7),
    "staffing_recruitment": (8, 7, 7, 5, 5, 4, 4, 9, 9, 9),
    "gig_economy": (6, 8, 2, 7, 3, 6, 4, 8, 8, 7),
    "food_beverage": (6, 8, 2, 8, 3, 7, 3, 7, 9, 8),
    "aerospace_defense": (7, 4, 9, 2, 6, 1, 5, 7, 7, 5),
}
_INDUSTRY_PLATFORM_FIT: Dict[str, Dict[str, int]] = {
    ind: dict(zip(_FIT_CHANNELS, scores)) for ind, scores in _FIT_RAW.items()
}

# Industry alias resolver (mirrors data_synthesizer logic)

# Grouped alias -> canonical key mapping (compressed)
_INDUSTRY_ALIASES: Dict[str, str] = {}
for _canon, _aliases in {
    "transportation_logistics": "transportation logistics trucking delivery warehousing freight shipping supply_chain logistics_supply_chain",
    "tech_engineering": "technology tech software it engineering saas cybersecurity fintech data_science ai machine_learning telecommunications",
    "healthcare_medical": "healthcare medical nursing pharma biotech pharmaceutical senior_living senior_care assisted_living home_health hospice mental_health behavioral_health clinical dental veterinary pharma_biotech healthcare_medical",
    "retail_consumer": "retail ecommerce consumer shopping",
    "finance_banking": "finance banking financial_services accounting investment wealth_management",
    "hospitality_travel": "hospitality travel hotel tourism hospitality_travel",
    "construction_real_estate": "construction real_estate property_management construction_real_estate",
    "government_public_sector": "government public_sector federal state municipal government_public_sector",
    "energy_utilities": "energy utilities oil_gas renewable solar wind energy_utilities",
    "professional_services": "consulting legal professional_services localization translation staffing_agency hr_services",
    "media_entertainment": "media entertainment gaming publishing advertising media_entertainment",
    "staffing_recruitment": "staffing recruitment staffing_recruitment talent_acquisition hr",
    "gig_economy": "gig gig_economy freelance on_demand",
    "food_beverage": "restaurant food_service food_beverage food beverage qsr fast_food",
    "aerospace_defense": "aerospace defense military aerospace_defense",
    "blue_collar_trades": "blue_collar trades skilled_trades",
    "manufacturing": "manufacturing automotive assembly production plant industrial",
    "nonprofit": "nonprofit non_profit ngo charity",
    "education": "education university college school academic higher_education k12",
    "insurance": "insurance underwriting claims",
    "general_entry_level": "general general_entry_level entry_level hourly",
}.items():
    for _a in _aliases.split():
        _INDUSTRY_ALIASES[_a] = _canon

# Keyword fragments for substring matching (order matters -- more specific first)
_INDUSTRY_KEYWORD_FRAGMENTS: List[Tuple[str, str]] = [
    ("senior living", "healthcare_medical"),
    ("senior care", "healthcare_medical"),
    ("assisted living", "healthcare_medical"),
    ("home health", "healthcare_medical"),
    ("behavioral health", "healthcare_medical"),
    ("mental health", "healthcare_medical"),
    ("health care", "healthcare_medical"),
    ("healthcare", "healthcare_medical"),
    ("medical", "healthcare_medical"),
    ("nursing", "healthcare_medical"),
    ("pharma", "healthcare_medical"),
    ("biotech", "healthcare_medical"),
    ("clinical", "healthcare_medical"),
    ("hospital", "healthcare_medical"),
    ("trucking", "transportation_logistics"),
    ("logistics", "transportation_logistics"),
    ("supply chain", "transportation_logistics"),
    ("transportation", "transportation_logistics"),
    ("warehousing", "transportation_logistics"),
    ("freight", "transportation_logistics"),
    ("shipping", "transportation_logistics"),
    ("delivery", "transportation_logistics"),
    ("localization", "professional_services"),
    ("translation", "professional_services"),
    ("consulting", "professional_services"),
    ("legal", "professional_services"),
    ("technology", "tech_engineering"),
    ("software", "tech_engineering"),
    ("engineering", "tech_engineering"),
    ("cybersecurity", "tech_engineering"),
    ("fintech", "tech_engineering"),
    ("saas", "tech_engineering"),
    ("telecom", "tech_engineering"),
    ("manufacturing", "manufacturing"),
    ("automotive", "manufacturing"),
    ("industrial", "manufacturing"),
    ("production", "manufacturing"),
    ("construction", "construction_real_estate"),
    ("real estate", "construction_real_estate"),
    ("property", "construction_real_estate"),
    ("hospitality", "hospitality_travel"),
    ("hotel", "hospitality_travel"),
    ("travel", "hospitality_travel"),
    ("tourism", "hospitality_travel"),
    ("restaurant", "food_beverage"),
    ("food service", "food_beverage"),
    ("food & beverage", "food_beverage"),
    ("qsr", "food_beverage"),
    ("fast food", "food_beverage"),
    ("finance", "finance_banking"),
    ("banking", "finance_banking"),
    ("accounting", "finance_banking"),
    ("investment", "finance_banking"),
    ("insurance", "insurance"),
    ("retail", "retail_consumer"),
    ("ecommerce", "retail_consumer"),
    ("consumer", "retail_consumer"),
    ("education", "education"),
    ("university", "education"),
    ("school", "education"),
    ("government", "government_public_sector"),
    ("public sector", "government_public_sector"),
    ("federal", "government_public_sector"),
    ("energy", "energy_utilities"),
    ("utilities", "energy_utilities"),
    ("oil", "energy_utilities"),
    ("renewable", "energy_utilities"),
    ("aerospace", "aerospace_defense"),
    ("defense", "aerospace_defense"),
    ("staffing", "staffing_recruitment"),
    ("recruitment", "staffing_recruitment"),
    ("nonprofit", "nonprofit"),
    ("non-profit", "nonprofit"),
    ("ngo", "nonprofit"),
    ("gig", "gig_economy"),
    ("freelance", "gig_economy"),
    ("media", "media_entertainment"),
    ("entertainment", "media_entertainment"),
    ("gaming", "media_entertainment"),
]


def _resolve_industry(raw: str, collar_type: str = "") -> str:
    """Normalize a raw industry string to a canonical fit-key.

    Resolution order:
    1. Direct match against _INDUSTRY_PLATFORM_FIT keys
    2. Exact alias lookup in _INDUSTRY_ALIASES
    3. Substring/keyword matching against _INDUSTRY_KEYWORD_FRAGMENTS
    4. Collar-type-based fallback (blue_collar -> blue_collar_trades, white -> professional_services)
    5. Final fallback: professional_services (never retail_consumer unless input says retail)
    """
    if not raw:
        # No industry at all -- use collar_type as primary signal
        collar_lower = (collar_type or "").lower().replace(" ", "_").replace("-", "_")
        if "blue" in collar_lower:
            return "blue_collar_trades"
        if "clinical" in collar_lower or "licensed" in collar_lower:
            return "healthcare_medical"
        return "professional_services"

    normalized = (
        raw.lower()
        .strip()
        .replace(" & ", "_")
        .replace(" and ", "_")
        .replace("-", "_")
        .replace(" ", "_")
        .replace("__", "_")
    )

    # 1. Direct match against platform fit keys
    if normalized in _INDUSTRY_PLATFORM_FIT:
        return normalized

    # 2. Exact alias lookup
    alias_match = _INDUSTRY_ALIASES.get(normalized)
    if alias_match:
        return alias_match

    # 3. Substring/keyword matching against the raw (lowered) input
    raw_lower = raw.lower().strip()
    for keyword, canon in _INDUSTRY_KEYWORD_FRAGMENTS:
        if keyword in raw_lower:
            return canon

    # 4. Also try normalized form against aliases with partial matching
    for alias_key, canon in _INDUSTRY_ALIASES.items():
        if alias_key in normalized or normalized in alias_key:
            return canon

    # 5. Collar-type-based fallback
    collar_lower = (collar_type or "").lower().replace(" ", "_").replace("-", "_")
    if "blue" in collar_lower or "hourly" in collar_lower or "entry" in collar_lower:
        return "blue_collar_trades"
    if "clinical" in collar_lower or "licensed" in collar_lower:
        return "healthcare_medical"
    if "executive" in collar_lower or "leadership" in collar_lower:
        return "professional_services"

    # 6. Final fallback -- general, NOT retail
    logger.warning(
        "Industry '%s' could not be resolved to a known category; "
        "falling back to 'professional_services'.",
        raw,
    )
    return "professional_services"


# Role-tier classifier (lightweight)

_EXECUTIVE_KEYWORDS = {
    "vp",
    "director",
    "chief",
    "cxo",
    "president",
    "svp",
    "evp",
    "head of",
    "partner",
}
_CLINICAL_KEYWORDS = {
    "nurse",
    "rn",
    "lpn",
    "physician",
    "therapist",
    "pharmacist",
    "doctor",
    "surgeon",
    "dentist",
}
_TRADES_KEYWORDS = {
    "technician",
    "mechanic",
    "electrician",
    "plumber",
    "welder",
    "cdl",
    "driver",
    "forklift",
    "hvac",
}
_GIG_KEYWORDS = {
    "gig",
    "freelance",
    "contractor",
    "courier",
    "shopper",
    "dasher",
    "delivery",
}


def _classify_role_tier(role: str, collar_type: str = "") -> str:
    """Classify a role title into a tier key matching ROLE_TIER_MULTIPLIERS.

    If *collar_type* is provided (e.g. from the plan data), it is used as
    the fallback instead of defaulting to "Professional / White-Collar".
    Role-title keyword matching still takes priority when keywords are found.
    """
    # Map collar_type strings from the plan to tier labels
    _COLLAR_TO_TIER: Dict[str, str] = {
        "blue_collar": "Hourly / Entry-Level",
        "blue collar": "Hourly / Entry-Level",
        "blue-collar": "Hourly / Entry-Level",
        "hourly": "Hourly / Entry-Level",
        "entry_level": "Hourly / Entry-Level",
        "entry level": "Hourly / Entry-Level",
        "white_collar": "Professional / White-Collar",
        "white collar": "Professional / White-Collar",
        "white-collar": "Professional / White-Collar",
        "professional": "Professional / White-Collar",
        "clinical": "Clinical / Licensed",
        "licensed": "Clinical / Licensed",
        "executive": "Executive / Leadership",
        "leadership": "Executive / Leadership",
        "trades": "Skilled Trades / Technical",
        "skilled_trades": "Skilled Trades / Technical",
        "gig": "Gig / Independent Contractor",
    }

    # Determine fallback from collar_type (if provided), else Professional
    collar_lower = (collar_type or "").lower().strip()
    fallback = _COLLAR_TO_TIER.get(collar_lower, "")
    if not fallback:
        # Try substring matching for compound collar_type values
        for ck, ct in _COLLAR_TO_TIER.items():
            if ck in collar_lower:
                fallback = ct
                break
    if not fallback:
        fallback = "Professional / White-Collar"

    if not role:
        return fallback

    r = role.lower()
    if any(kw in r for kw in _EXECUTIVE_KEYWORDS):
        return "Executive / Leadership"
    if any(kw in r for kw in _CLINICAL_KEYWORDS):
        return "Clinical / Licensed"
    if any(kw in r for kw in _GIG_KEYWORDS):
        return "Gig / Independent Contractor"
    if any(kw in r for kw in _TRADES_KEYWORDS):
        return "Skilled Trades / Technical"
    # Hourly/entry-level signals
    if any(
        kw in r
        for kw in (
            "cashier",
            "associate",
            "crew",
            "barista",
            "server",
            "warehouse",
            "picker",
            "packer",
            "cleaner",
            "janitor",
            "caregiver",
            "aide",
            "attendant",
            "housekeeper",
            "cook",
            "dishwasher",
            "laborer",
            "loader",
            "stocker",
        )
    ):
        return "Hourly / Entry-Level"
    # No keyword match -- use collar_type fallback instead of hardcoded white-collar
    return fallback


# Map platform names to the budget engine's display category names.
# These match the friendly names in CHANNEL_NAME_TO_CATEGORY from budget_engine.py
# so the Channel Recommendations sheet uses the same taxonomy as the main plan.
_PLATFORM_TO_CATEGORY_DISPLAY: Dict[str, str] = {
    "Google Ads": "Search/SEM",
    "Meta (Facebook/Instagram)": "Social Media Channels",
    "LinkedIn Ads": "Social Media Channels",
    "TikTok Ads": "Social Media Channels",
    "Microsoft/Bing Ads": "Search/SEM",
    "Snapchat Ads": "Social Media Channels",
    "X (Twitter) Ads": "Social Media Channels",
    "Programmatic Display (DSP)": "Programmatic & DSP",
    "Indeed Sponsored Jobs": "Global Job Boards",
    "ZipRecruiter Sponsored": "Global Job Boards",
}


# CPC benchmarks per named ad channel

# (CPC, apply_rate) per channel -- static defaults, overlaid by live data below
_CHANNEL_BENCH_STATIC: Dict[str, Tuple[float, float]] = {
    "Indeed Sponsored Jobs": (0.85, 0.10),
    "ZipRecruiter Sponsored": (0.95, 0.09),
    "LinkedIn Ads": (2.83, 0.04),
    "Google Ads": (2.50, 0.05),
    "Meta (Facebook/Instagram)": (1.20, 0.03),
    "Programmatic Display (DSP)": (0.65, 0.06),
    "Microsoft/Bing Ads": (1.80, 0.04),
    "TikTok Ads": (1.00, 0.02),
    "Snapchat Ads": (0.90, 0.02),
    "X (Twitter) Ads": (1.30, 0.03),
}


# ── Live benchmark overlay (channel_benchmarks_live.json) ──
# Maps live JSON slugs to channel_recommender platform names.
_LIVE_SLUG_TO_PLATFORM: Dict[str, str] = {
    "indeed": "Indeed Sponsored Jobs",
    "linkedin": "LinkedIn Ads",
    "ziprecruiter": "ZipRecruiter Sponsored",
    "glassdoor": "Google Ads",  # Glassdoor CPC proxies search/display
    "monster": "Indeed Sponsored Jobs",  # Monster CPCs close to Indeed
}


def _overlay_live_benchmarks() -> Dict[str, Tuple[float, float]]:
    """Return _CHANNEL_BENCH_STATIC overlaid with channel_benchmarks_live.json CPCs.

    Live data provides CPC ranges; we use the geometric mean of min/max
    as the typical CPC.  Apply rates are kept from static defaults since
    live data does not include them.
    """
    result = dict(_CHANNEL_BENCH_STATIC)
    live_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "data",
        "channel_benchmarks_live.json",
    )
    try:
        with open(live_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        for entry in raw.get("data", []):
            slug = (entry.get("channel") or "").lower().strip()
            platform = _LIVE_SLUG_TO_PLATFORM.get(slug)
            if not platform or platform not in result:
                continue
            meta = entry.get("metadata") or {}
            cpc_range = meta.get("cpc_range") or {}
            cpc_min = cpc_range.get("min")
            cpc_max = cpc_range.get("max")
            if cpc_min and cpc_max and cpc_min > 0 and cpc_max > 0:
                import math as _m

                live_cpc = round(_m.sqrt(cpc_min * cpc_max), 2)
                _, static_ar = result[platform]
                result[platform] = (live_cpc, static_ar)
                logger.info(
                    "channel_recommender: overlaid %s CPC %.2f from live data (was %.2f)",
                    platform,
                    live_cpc,
                    _CHANNEL_BENCH_STATIC[platform][0],
                )
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        logger.debug("channel_recommender: live benchmark overlay skipped: %s", exc)
    return result


# Build the effective benchmark table -- live overlay on static defaults
_CHANNEL_BENCH: Dict[str, Tuple[float, float]] = _overlay_live_benchmarks()
_CHANNEL_CPC = {k: v[0] for k, v in _CHANNEL_BENCH.items()}
_CHANNEL_APPLY_RATE = {k: v[1] for k, v in _CHANNEL_BENCH.items()}

# Channel rationale templates by channel
_CHANNEL_RATIONALE: Dict[str, str] = {
    "Indeed Sponsored Jobs": "Largest active job seeker audience; highest volume for {industry}.",
    "ZipRecruiter Sponsored": "AI-matching drives qualified applicants at low CPA.",
    "LinkedIn Ads": "Best for professional targeting with title/skills/company filters.",
    "Google Ads": "High-intent candidates actively searching for {industry} jobs.",
    "Meta (Facebook/Instagram)": "Massive passive-candidate reach; strong for hourly/local roles.",
    "Programmatic Display (DSP)": "Broad reach at low CPC across 10K+ sites for volume hiring.",
    "Microsoft/Bing Ads": "Lower competition CPC; strong for finance, government, B2B.",
    "TikTok Ads": "Gen-Z workforce reach; strong for retail, hospitality, gig roles.",
    "Snapchat Ads": "Effective for 18-24 demographic and entry-level positions.",
    "X (Twitter) Ads": "Niche reach for tech, media, and employer brand campaigns.",
}

# Core engine


def recommend_channels(
    industry: str,
    role: str = "",
    budget: Any = 100_000,
    locations: Optional[List[str]] = None,
    goals: Optional[List[str]] = None,
    collar_type: str = "",
    main_plan_total_hires: int = 0,
) -> Dict[str, Any]:
    """Produce tiered channel recommendations for a recruitment campaign.

    Parameters
    ----------
    industry : str
        Industry vertical string (free-form or canonical key).
    role : str
        Job title used for role-tier classification.
    budget : Any
        Total campaign budget (parsed via parse_budget).
    locations : list[str] | None
        Target locations for geo adjustments.
    goals : list[str] | None
        Campaign goals (e.g. volume, quality).
    collar_type : str
        Optional collar classification from plan data (e.g. "blue_collar",
        "white_collar").  Used as fallback for both industry resolution
        and role-tier classification when keywords are ambiguous.
    main_plan_total_hires : int
        Total projected hires from the main budget engine.  When provided
        (> 0), the recommender normalizes its per-channel hire projections
        so the sum matches the main plan.  This eliminates the discrepancy
        between the Channel Recommendations sheet and the Executive Summary
        (e.g. 1,317 vs 380 hires on the same budget).
    """
    # ── Resolve inputs ──
    ind_key = _resolve_industry(industry, collar_type=collar_type)
    ind_label = INDUSTRY_LABEL_MAP.get(ind_key) or industry.replace("_", " ").title()
    tier = _classify_role_tier(role, collar_type=collar_type)
    budget_val = parse_budget(budget, default=100_000.0)
    locs = locations or []
    campaign_goals = goals or []

    # ── Get fit scores for this industry ──
    fit_scores = dict(_INDUSTRY_PLATFORM_FIT.get(ind_key, {}))
    if not fit_scores:
        # ind_key didn't have a fit table (e.g. general_entry_level) -- use
        # professional_services as a balanced fallback, NOT retail_consumer
        fit_scores = dict(_INDUSTRY_PLATFORM_FIT.get("professional_services", {}))

    # ── Role-tier adjustments ──
    # Executive/professional roles: boost LinkedIn, lower TikTok/Snapchat
    # Hourly/entry: boost Indeed, Meta, TikTok; lower LinkedIn
    _tier_adjustments = {
        "Executive / Leadership": {
            "LinkedIn Ads": 2,
            "Indeed Sponsored Jobs": -1,
            "TikTok Ads": -2,
            "Snapchat Ads": -2,
        },
        "Professional / White-Collar": {"LinkedIn Ads": 1, "Google Ads": 1},
        "Clinical / Licensed": {
            "Indeed Sponsored Jobs": 1,
            "Google Ads": 1,
            "TikTok Ads": -2,
        },
        "Skilled Trades / Technical": {
            "Indeed Sponsored Jobs": 1,
            "Meta (Facebook/Instagram)": 1,
            "LinkedIn Ads": -2,
        },
        "Hourly / Entry-Level": {
            "Indeed Sponsored Jobs": 2,
            "Meta (Facebook/Instagram)": 2,
            "TikTok Ads": 2,
            "LinkedIn Ads": -3,
        },
        "Gig / Independent Contractor": {
            "Meta (Facebook/Instagram)": 2,
            "TikTok Ads": 2,
            "LinkedIn Ads": -4,
            "Snapchat Ads": 1,
        },
    }
    for channel, adj in _tier_adjustments.get(tier, {}).items():
        if channel in fit_scores:
            fit_scores[channel] = max(1, min(10, fit_scores[channel] + adj))

    # ── Location adjustments ──
    # If locations suggest local/blue-collar, boost local-reach channels
    _loc_lower = " ".join(locs).lower()
    is_local = any(
        kw in _loc_lower for kw in ("local", "city", "metro", "rural", "suburban")
    )
    if is_local:
        for ch in (
            "Meta (Facebook/Instagram)",
            "Indeed Sponsored Jobs",
            "Programmatic Display (DSP)",
        ):
            if ch in fit_scores:
                fit_scores[ch] = min(10, fit_scores[ch] + 1)

    # ── Budget constraint: small budgets concentrate on fewer channels ──
    max_channels = 10
    if budget_val <= 25_000:
        max_channels = 3
    elif budget_val <= 50_000:
        max_channels = 5
    elif budget_val <= 100_000:
        max_channels = 7

    # ── CPA multiplier for this tier ──
    cpa_mult = ROLE_TIER_MULTIPLIERS.get(tier, 1.0)
    hire_rate = HIRE_RATE_BY_TIER.get(tier) or HIRE_RATE_BY_TIER.get("default", 0.02)

    # ── Score and rank channels ──
    scored: List[Dict[str, Any]] = []
    total_fit = sum(fit_scores.values()) or 1

    for channel, fit in sorted(fit_scores.items(), key=lambda x: x[1], reverse=True):
        cpc = _CHANNEL_CPC.get(channel, 1.00) * cpa_mult
        apply_rate = _CHANNEL_APPLY_RATE.get(channel, 0.04)
        cpa = cpc / apply_rate if apply_rate > 0 else cpc * 25

        # Allocation % proportional to fit score
        raw_pct = (fit / total_fit) * 100
        spend = budget_val * (raw_pct / 100)
        clicks = int(spend / cpc) if cpc > 0 else 0
        applications = int(clicks * apply_rate)
        hires = max(1, int(applications * hire_rate)) if applications > 0 else 0

        # Confidence based on fit score
        if fit >= 8:
            confidence = "high"
        elif fit >= 5:
            confidence = "medium"
        else:
            confidence = "low"

        rationale = _CHANNEL_RATIONALE.get(
            channel, "Suitable for {industry} recruitment campaigns."
        ).format(industry=ind_label)

        scored.append(
            {
                "channel": _PLATFORM_TO_CATEGORY_DISPLAY.get(channel, "Other"),
                "platform": channel,
                "fit_score": fit,
                "allocation_pct": round(raw_pct, 1),
                "expected_cpc": round(cpc, 2),
                "expected_cpa": round(cpa, 2),
                "projected_spend": round(spend, 2),
                "projected_clicks": clicks,
                "projected_applications": applications,
                "projected_hires": hires,
                "confidence": confidence,
                "rationale": rationale,
            }
        )

    # ── Tier the channels ──
    # Limit to max_channels worth of "active" channels
    must_have: List[Dict[str, Any]] = []
    should_have: List[Dict[str, Any]] = []
    test_and_learn: List[Dict[str, Any]] = []
    skip: List[Dict[str, Any]] = []

    active_count = 0
    for ch in scored:
        fit = ch["fit_score"]
        if fit >= 7 and active_count < max_channels:
            ch["tier"] = "Must Have"
            must_have.append(ch)
            active_count += 1
        elif fit >= 5 and active_count < max_channels:
            ch["tier"] = "Should Have"
            should_have.append(ch)
            active_count += 1
        elif fit >= 3:
            ch["tier"] = "Test & Learn"
            test_and_learn.append(ch)
        else:
            ch["tier"] = "Skip"
            skip.append(ch)

    # ── Re-normalize allocation % across active channels only ──
    active = must_have + should_have
    if active:
        total_active_fit = sum(c["fit_score"] for c in active)
        for ch in active:
            ch["allocation_pct"] = round((ch["fit_score"] / total_active_fit) * 100, 1)
            ch["projected_spend"] = round(budget_val * ch["allocation_pct"] / 100, 2)
            cpc = ch["expected_cpc"]
            apply_rate_val = _CHANNEL_APPLY_RATE.get(ch["channel"], 0.04)
            ch["projected_clicks"] = int(ch["projected_spend"] / cpc) if cpc > 0 else 0
            ch["projected_applications"] = int(ch["projected_clicks"] * apply_rate_val)
            ch["projected_hires"] = (
                max(1, int(ch["projected_applications"] * hire_rate))
                if ch["projected_applications"] > 0
                else 0
            )

    # Zero out allocation for test/skip
    for ch in test_and_learn + skip:
        ch["allocation_pct"] = 0.0
        ch["projected_spend"] = 0.0

    # ── Normalize hire projections to match main budget engine ──
    # The channel recommender uses its own CPC/apply-rate/hire-rate constants
    # without the safety margins, CPH floors, and collar adjustments that the
    # main budget engine applies.  This causes a significant discrepancy
    # (e.g. 1,317 vs 380 hires on the same $2M budget).
    # When main_plan_total_hires is provided, scale all per-channel hires
    # proportionally so the Channel Recommendations sheet matches the
    # Executive Summary.
    _raw_total_hires = sum(c["projected_hires"] for c in active)
    if (
        main_plan_total_hires > 0
        and _raw_total_hires > 0
        and abs(_raw_total_hires - main_plan_total_hires) / _raw_total_hires > 0.20
    ):
        _scale = main_plan_total_hires / _raw_total_hires
        logger.info(
            "Channel recommender hire normalization: raw=%d, main_plan=%d, "
            "scale=%.3f (%.0f%% adjustment)",
            _raw_total_hires,
            main_plan_total_hires,
            _scale,
            (1 - _scale) * 100,
        )
        _remaining = main_plan_total_hires
        _channels_with_hires = [c for c in active if c["projected_hires"] > 0]
        for i, ch in enumerate(_channels_with_hires):
            if i == len(_channels_with_hires) - 1:
                # Last channel gets remainder to avoid rounding drift
                ch["projected_hires"] = max(0, _remaining)
            else:
                new_hires = max(0, int(ch["projected_hires"] * _scale))
                ch["projected_hires"] = new_hires
                _remaining -= new_hires
        # Also scale applications proportionally for CPA consistency
        _raw_total_apps = sum(c["projected_applications"] for c in active)
        if _raw_total_apps > 0 and _scale < 1.0:
            for ch in active:
                ch["projected_applications"] = max(
                    1, int(ch["projected_applications"] * _scale)
                )
            # Recalculate per-channel CPA from adjusted applications
            for ch in active:
                spend = ch.get("projected_spend", 0)
                apps = ch.get("projected_applications", 1)
                ch["expected_cpa"] = round(spend / max(apps, 1), 2)

    # ── Summary stats ──
    total_apps = sum(c["projected_applications"] for c in active)
    total_hires = sum(c["projected_hires"] for c in active)
    total_clicks = sum(c["projected_clicks"] for c in active)
    avg_cpa = round(budget_val / total_apps, 2) if total_apps > 0 else 0.0

    summary = (
        f"For {ind_label} hiring ({role or 'various roles'}), "
        f"we recommend {len(must_have)} must-have and {len(should_have)} should-have channels "
        f"across a ${budget_val:,.0f} budget. "
        f"Expected: ~{total_clicks:,} clicks, ~{total_apps:,} applications, "
        f"~{total_hires:,} hires at ${avg_cpa:,.2f} avg CPA."
    )

    # ── International local platform recommendations ──
    intl_local_platforms: List[Dict[str, Any]] = []
    if locs:
        try:
            intl_local_platforms = _get_intl_local_platforms(locs)
        except Exception as _intl_err:
            logger.error(
                "Failed to get intl local platforms: %s", _intl_err, exc_info=True
            )

    # ── Joveo publisher network enrichment ──
    joveo_publisher_info: Dict[str, Any] = {}
    try:
        joveo_publisher_info = _enrich_with_joveo_publishers(locs)
    except Exception as _jp_err:
        logger.debug("Joveo publisher enrichment skipped: %s", _jp_err)

    # ── Global supply enrichment for international locations ──
    global_supply_info: Dict[str, Any] = {}
    try:
        global_supply_info = _enrich_with_global_supply(locs)
    except Exception as _gs_err:
        logger.debug("Global supply enrichment skipped: %s", _gs_err)

    result: Dict[str, Any] = {
        "must_have": must_have,
        "should_have": should_have,
        "test_and_learn": test_and_learn,
        "skip": skip,
        "summary": summary,
        "metadata": {
            "industry": ind_key,
            "industry_label": ind_label,
            "role": role or "Various",
            "role_tier": tier,
            "budget": budget_val,
            "locations": locs,
            "goals": campaign_goals,
            "total_projected_clicks": total_clicks,
            "total_projected_applications": total_apps,
            "total_projected_hires": total_hires,
            "avg_cpa": avg_cpa,
        },
    }
    if intl_local_platforms:
        result["intl_local_platforms"] = intl_local_platforms
        result["summary"] += (
            f" Additionally, {len(intl_local_platforms)} local platform(s) recommended "
            f"for international markets: {', '.join(p['name'] for p in intl_local_platforms[:5])}."
        )
    if joveo_publisher_info:
        result["joveo_publisher_network"] = joveo_publisher_info
    if global_supply_info:
        result["global_supply_intelligence"] = global_supply_info
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# JOVEO PUBLISHER NETWORK ENRICHMENT (joveo_publishers.json)
# ═══════════════════════════════════════════════════════════════════════════════
_joveo_publishers_cache: Optional[Dict] = None


def _load_joveo_publishers() -> Dict:
    """Load joveo_publishers.json once, cache at module level."""
    global _joveo_publishers_cache
    if _joveo_publishers_cache is not None:
        return _joveo_publishers_cache
    _path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", "joveo_publishers.json"
    )
    try:
        with open(_path, "r", encoding="utf-8") as fh:
            _joveo_publishers_cache = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.debug("Failed to load joveo_publishers.json: %s", exc)
        _joveo_publishers_cache = {}
    return _joveo_publishers_cache


def _enrich_with_joveo_publishers(locations: List[str]) -> Dict[str, Any]:
    """Enrich channel recommendations with Joveo publisher network data.

    Returns publisher counts by category and relevant country-specific publishers.
    """
    pubs = _load_joveo_publishers()
    if not pubs:
        return {}

    result: Dict[str, Any] = {
        "total_active_publishers": pubs.get("total_active_publishers", 0),
    }

    by_category = pubs.get("by_category", {})
    if by_category:
        result["publishers_by_category"] = {
            cat: len(names) for cat, names in by_category.items()
        }

    by_country = pubs.get("by_country", {})
    if locations and by_country:
        matched_countries: Dict[str, List[str]] = {}
        for loc in locations:
            loc_lower = (loc or "").lower().strip()
            for country_name, country_pubs in by_country.items():
                if (
                    loc_lower in country_name.lower()
                    or country_name.lower() in loc_lower
                ):
                    matched_countries[country_name] = country_pubs[:10]  # top 10
                    break
        if matched_countries:
            result["location_publishers"] = {
                country: {
                    "publisher_count": len(pubs_list),
                    "sample_publishers": pubs_list,
                }
                for country, pubs_list in matched_countries.items()
            }

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL SUPPLY ENRICHMENT (global_supply.json)
# ═══════════════════════════════════════════════════════════════════════════════
_global_supply_cache: Optional[Dict] = None


def _load_global_supply() -> Dict:
    """Load global_supply.json once, cache at module level."""
    global _global_supply_cache
    if _global_supply_cache is not None:
        return _global_supply_cache
    _path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", "global_supply.json"
    )
    try:
        with open(_path, "r", encoding="utf-8") as fh:
            _global_supply_cache = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.debug("Failed to load global_supply.json: %s", exc)
        _global_supply_cache = {}
    return _global_supply_cache


def _enrich_with_global_supply(locations: List[str]) -> Dict[str, Any]:
    """Enrich channel recommendations with global supply intelligence.

    Returns country-specific board info, DEI boards, and billing models
    for locations that match the supply data.
    """
    supply = _load_global_supply()
    if not supply:
        return {}

    country_boards = supply.get("country_job_boards", {})
    dei_boards = supply.get("dei_boards_by_country", {})
    niche_boards = supply.get("niche_industry_boards", {})
    billing_models = supply.get("billing_models", {})

    result: Dict[str, Any] = {
        "available_countries": len(country_boards),
    }

    if not locations:
        return result

    matched: Dict[str, Dict[str, Any]] = {}
    for loc in locations:
        loc_lower = (loc or "").lower().strip()
        for country_name, country_data in country_boards.items():
            if loc_lower in country_name.lower() or country_name.lower() in loc_lower:
                boards = country_data.get("boards", [])
                entry: Dict[str, Any] = {
                    "total_boards": len(boards),
                    "tier_1_boards": [
                        b.get("name")
                        for b in boards
                        if (b.get("tier") or "").lower() == "tier 1"
                    ],
                    "monthly_spend": country_data.get("monthly_spend", "N/A"),
                    "key_metros": country_data.get("key_metros", []),
                }
                # DEI boards for the country
                country_dei = dei_boards.get(country_name, [])
                if country_dei:
                    entry["dei_boards"] = country_dei[:10]
                matched[country_name] = entry
                break

    if matched:
        result["country_intelligence"] = matched

    # Include billing model guidance
    if billing_models:
        result["billing_models"] = billing_models

    return result


def format_recommendation_text(rec: Dict[str, Any]) -> str:
    """Format recommendation dict into markdown text for chatbot display."""
    lines: List[str] = []
    meta = rec.get("metadata", {})
    lines.append(f"**Channel Recommendations: {meta.get('industry_label', '')}**")
    lines.append(f"Role: {meta.get('role', 'Various')} ({meta.get('role_tier', '')})")
    lines.append(f"Budget: ${meta.get('budget', 0):,.0f}")
    lines.append("")

    for tier_key, tier_label, emoji in [
        ("must_have", "MUST HAVE", "!!"),
        ("should_have", "SHOULD HAVE", "!"),
        ("test_and_learn", "TEST & LEARN", "?"),
    ]:
        channels = rec.get(tier_key, [])
        if not channels:
            continue
        lines.append(f"**{tier_label}** channels:")
        for ch in channels:
            alloc = ch.get("allocation_pct", 0)
            alloc_str = (
                f" -- {alloc:.0f}% of budget (${ch.get('projected_spend', 0):,.0f})"
                if alloc > 0
                else ""
            )
            plat = ch.get("platform", "")
            plat_suffix = f" [{plat}]" if plat else ""
            lines.append(f"  {ch['channel']}{plat_suffix}{alloc_str}")
            lines.append(
                f"    CPC: ${ch['expected_cpc']:.2f} | CPA: ${ch['expected_cpa']:.2f} | "
                f"Apps: ~{ch.get('projected_applications', 0):,} | "
                f"Confidence: {ch['confidence']}"
            )
            lines.append(f"    Why: {ch['rationale']}")
        lines.append("")

    skip_channels = rec.get("skip", [])
    if skip_channels:
        lines.append(f"**SKIP** (poor fit for this campaign):")
        for ch in skip_channels:
            plat = ch.get("platform", "")
            plat_tag = f" [{plat}]" if plat else ""
            lines.append(
                f"  {ch['channel']}{plat_tag} (fit score: {ch['fit_score']}/10)"
            )
        lines.append("")

    lines.append(rec.get("summary", ""))
    return "\n".join(lines)
