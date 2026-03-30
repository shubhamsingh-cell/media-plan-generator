"""
Data Synthesis Engine -- fuses 25 API sources into unified intelligence.

Takes raw enrichment data from api_enrichment.py and the recruitment knowledge
base, cross-references and validates data points, and produces synthesized
analysis with confidence scores.

DATA PRIORITY SYSTEM (see data_orchestrator.py for full documentation):
    Priority 1: Client-provided data (uploaded briefs/historical data)
    Priority 2: Live API data (real-time market signals from 25 APIs)
    Priority 3: KB benchmark data (Appcast 2026, Google Ads 2025, recruitment_benchmarks_deep)
    Priority 4: Embedded research.py fallback data

KB data sources consumed by synthesis functions:
    - google_ads_benchmarks:  kb["google_ads_benchmarks"] -> 8 categories, CPC/CTR stats
    - Appcast 2026 report:    kb["white_papers"]["reports"]["appcast_benchmark_2026"]["benchmarks"]
                              -> CPA/CPH/apply_rate by 24 occupations, full funnel, international
    - recruitment_benchmarks: kb["recruitment_benchmarks"]["industry_benchmarks"] -> 22 industries
    - platform_intelligence:  kb["platform_intelligence"]["platforms"] -> 91 platforms
    - workforce_trends:       kb["workforce_trends"] -> Gen-Z, remote work, DEI
    - white_papers:           kb["white_papers"]["reports"] -> 74 industry reports

The enriched dict (from ``api_enrichment.enrich_data``) may contain these keys:
    salary_data, bls_data, onet_data, industry_employment, sec_data,
    adzuna_data, google_trends_data, location_demographics, geonames_data,
    teleport_data, countries_data, imf_data, world_bank_data, clearbit_data,
    wikipedia_data, datausa_data, google_ads_keyword_data, census_data,
    google_ads_data, meta_ads_data, bing_ads_data, tiktok_ads_data,
    linkedin_ads_data, careeronestop_data, jooble_data, enrichment_summary

Note: The actual enriched dict uses slightly different names for some keys
(e.g. ``datausa_occupation``, ``datausa_location``, ``country_data``,
``search_trends``, ``imf_indicators``). This module handles both naming
conventions transparently.
"""

from __future__ import annotations

import logging
import math
import statistics
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Canonical taxonomy standardizer ──
# Used to normalize industry/role/platform keys before KB lookups.
# Falls back gracefully if unavailable.
try:
    from standardizer import (
        normalize_industry as _std_industry,
        normalize_platform as _std_platform,
        CANONICAL_INDUSTRIES as _CANON_IND,
    )

    _HAS_STANDARDIZER = True
except ImportError:
    _HAS_STANDARDIZER = False


# ---------------------------------------------------------------------------
# Source reliability weights (higher = more trustworthy)
# Government > Commercial > Aggregator
# ---------------------------------------------------------------------------

SOURCE_WEIGHTS: Dict[str, float] = {
    # Government / official (weight 1.0)
    "BLS OES": 1.0,
    "BLS": 1.0,
    "BLS-QCEW": 1.0,
    "US Census ACS": 1.0,
    "Census-ACS": 1.0,
    "O*NET": 0.95,
    "O*NET Live API": 0.95,
    "O*NET Curated Fallback": 0.80,
    "CareerOneStop API": 0.95,
    "CareerOneStop Benchmarks": 0.75,
    "IMF": 0.90,
    "WorldBank": 0.90,
    "FRED": 0.90,
    "SEC-EDGAR": 0.85,
    # Commercial (weight 0.7-0.85)
    "LinkedIn Marketing API": 0.85,
    "LinkedIn Ads Benchmarks": 0.70,
    "Google Ads API": 0.85,
    "Google Ads Benchmarks": 0.70,
    "Google Trends": 0.75,
    "Meta Marketing API": 0.80,
    "Meta Ads Benchmarks": 0.65,
    "Bing Ads API": 0.80,
    "Bing Ads Benchmarks": 0.65,
    "TikTok Marketing API": 0.75,
    "TikTok Ads Benchmarks": 0.60,
    "Clearbit": 0.70,
    "Clearbit-Auto": 0.70,
    "Wikipedia": 0.65,
    "Teleport": 0.70,
    "GeoNames": 0.75,
    "RESTCountries": 0.70,
    "DataUSA": 0.80,
    # Aggregator (weight 0.5-0.65)
    "Adzuna": 0.60,
    "Jooble API": 0.60,
    "Jooble Market Benchmarks": 0.50,
    "CurrencyRates": 0.70,
    # Knowledge base fallback
    "KB Benchmark": 0.40,
    "KB Fallback": 0.30,
}

# Mapping from NAICS / legacy industry keys to KB
# ``industry_specific_benchmarks`` section keys
_INDUSTRY_TO_KB_KEY: Dict[str, str] = {
    # Healthcare (C1 fix: pharma/biotech → healthcare, not manufacturing)
    "healthcare": "healthcare",
    "healthcare_medical": "healthcare",
    "mental_health": "healthcare",
    "pharma": "healthcare",
    "pharma_biotech": "healthcare",
    "life_sciences": "healthcare",
    "biotech": "healthcare",
    # Technology
    "technology": "technology",
    "tech_engineering": "technology",
    "telecom": "technology",
    "telecommunications": "technology",
    "saas": "technology",
    "fintech": "technology",
    # Retail & Hospitality
    "retail": "retail_hospitality",
    "retail_consumer": "retail_hospitality",
    "hospitality": "retail_hospitality",
    "hospitality_travel": "retail_hospitality",
    "food_beverage": "retail_hospitality",
    "restaurant": "retail_hospitality",
    # Construction
    "construction": "construction_infrastructure",
    "construction_real_estate": "construction_infrastructure",
    "real_estate": "construction_infrastructure",
    # Transportation & Logistics (C5 fix: blue_collar → transportation, not manufacturing)
    "transportation": "transportation_logistics",
    "logistics": "transportation_logistics",
    "logistics_supply_chain": "transportation_logistics",
    "maritime": "transportation_logistics",
    "maritime_marine": "transportation_logistics",
    "rideshare": "transportation_logistics",
    "blue_collar": "transportation_logistics",  # C5 FIX: was "manufacturing"
    "blue_collar_trades": "transportation_logistics",  # C5 FIX: was "manufacturing"
    # Manufacturing (C1 fix: NAICS 31-33 is broad manufacturing, not food-only)
    "manufacturing": "manufacturing",
    "automotive": "manufacturing",
    "aerospace": "manufacturing",  # kept — aerospace IS manufacturing
    "aerospace_defense": "manufacturing",  # defense manufacturing
    "industrial": "manufacturing",
    "semiconductor": "manufacturing",
    # Financial Services
    "finance": "financial_services",
    "finance_banking": "financial_services",
    "insurance": "financial_services",
    "accounting": "financial_services",
    # Government & Utilities
    "government": "government_utilities",
    "military_recruitment": "government_utilities",
    "energy": "government_utilities",
    "energy_utilities": "government_utilities",
    "public_sector": "government_utilities",
    # C5 FIX: education and professional services get proper mappings
    "education": "government_utilities",  # C5 FIX: was "technology"
    "professional_services": "technology",  # C5 FIX: was "financial_services"
    "legal_services": "financial_services",
    "nonprofit": "government_utilities",
    "general": "retail_hospitality",
    "general_entry_level": "retail_hospitality",
    "media_entertainment": "technology",
}

# ---------------------------------------------------------------------------
# Hardcoded fallback salary ranges by common role keywords.
# Single source of truth for salary fallbacks -- referenced by fuse_salary_intelligence().
# NOTE: For CPC/CPA/CPM benchmark data, canonical source is trend_engine.py.
# See trend_engine.get_benchmark() for authoritative ad platform benchmarks.
# ---------------------------------------------------------------------------
_ROLE_SALARY_FALLBACKS: Dict[str, Dict[str, int]] = {
    "software": {
        "median": 130000,
        "min": 90000,
        "p25": 110000,
        "p75": 155000,
        "max": 200000,
    },
    "engineer": {
        "median": 120000,
        "min": 80000,
        "p25": 100000,
        "p75": 145000,
        "max": 190000,
    },
    "data scientist": {
        "median": 135000,
        "min": 95000,
        "p25": 115000,
        "p75": 160000,
        "max": 210000,
    },
    "data": {
        "median": 120000,
        "min": 80000,
        "p25": 100000,
        "p75": 145000,
        "max": 185000,
    },
    "product manager": {
        "median": 140000,
        "min": 100000,
        "p25": 120000,
        "p75": 165000,
        "max": 220000,
    },
    "product": {
        "median": 130000,
        "min": 90000,
        "p25": 110000,
        "p75": 155000,
        "max": 200000,
    },
    "designer": {
        "median": 110000,
        "min": 70000,
        "p25": 90000,
        "p75": 135000,
        "max": 170000,
    },
    "ux": {"median": 115000, "min": 75000, "p25": 95000, "p75": 140000, "max": 175000},
    "devops": {
        "median": 135000,
        "min": 95000,
        "p25": 115000,
        "p75": 160000,
        "max": 200000,
    },
    "marketing": {
        "median": 85000,
        "min": 55000,
        "p25": 70000,
        "p75": 105000,
        "max": 140000,
    },
    "sales": {
        "median": 90000,
        "min": 50000,
        "p25": 70000,
        "p75": 115000,
        "max": 160000,
    },
    "hr": {"median": 75000, "min": 50000, "p25": 62000, "p75": 92000, "max": 120000},
    "analyst": {
        "median": 85000,
        "min": 55000,
        "p25": 70000,
        "p75": 105000,
        "max": 140000,
    },
    "manager": {
        "median": 105000,
        "min": 70000,
        "p25": 85000,
        "p75": 130000,
        "max": 170000,
    },
    "director": {
        "median": 155000,
        "min": 110000,
        "p25": 130000,
        "p75": 180000,
        "max": 250000,
    },
    "nurse": {"median": 82000, "min": 55000, "p25": 68000, "p75": 95000, "max": 120000},
    "driver": {"median": 52000, "min": 38000, "p25": 45000, "p75": 62000, "max": 78000},
    "warehouse": {
        "median": 42000,
        "min": 32000,
        "p25": 37000,
        "p75": 50000,
        "max": 60000,
    },
    "mechanic": {
        "median": 52000,
        "min": 36000,
        "p25": 44000,
        "p75": 62000,
        "max": 75000,
    },
    "electrician": {
        "median": 60000,
        "min": 42000,
        "p25": 50000,
        "p75": 72000,
        "max": 90000,
    },
    "accountant": {
        "median": 78000,
        "min": 52000,
        "p25": 65000,
        "p75": 95000,
        "max": 125000,
    },
    "teacher": {
        "median": 62000,
        "min": 42000,
        "p25": 52000,
        "p75": 75000,
        "max": 95000,
    },
    "construction": {
        "median": 55000,
        "min": 38000,
        "p25": 46000,
        "p75": 68000,
        "max": 85000,
    },
    "physician": {
        "median": 229000,
        "min": 180000,
        "p25": 200000,
        "p75": 280000,
        "max": 400000,
    },
    "pharmacist": {
        "median": 132000,
        "min": 100000,
        "p25": 115000,
        "p75": 150000,
        "max": 175000,
    },
    "therapist": {
        "median": 75000,
        "min": 48000,
        "p25": 60000,
        "p75": 88000,
        "max": 110000,
    },
    "physician assistant": {
        "median": 121000,
        "min": 90000,
        "p25": 105000,
        "p75": 140000,
        "max": 165000,
    },
    "medical": {
        "median": 95000,
        "min": 55000,
        "p25": 72000,
        "p75": 120000,
        "max": 180000,
    },
    "healthcare": {
        "median": 78000,
        "min": 45000,
        "p25": 58000,
        "p75": 95000,
        "max": 140000,
    },
    "dental": {
        "median": 85000,
        "min": 50000,
        "p25": 65000,
        "p75": 105000,
        "max": 160000,
    },
}

# ---------------------------------------------------------------------------
# Hardcoded fallback demand data by common role keywords.
# Single source of truth -- referenced by fuse_job_market_demand().
# ---------------------------------------------------------------------------
_ROLE_DEMAND_FALLBACKS: Dict[str, Dict[str, Any]] = {
    "software": {
        "job_postings": 150000,
        "search_interest": "High",
        "talent_pool": 2500000,
        "competition_index": 7.2,
        "trend": "Growing (+8% YoY)",
    },
    "engineer": {
        "job_postings": 200000,
        "search_interest": "High",
        "talent_pool": 3000000,
        "competition_index": 6.8,
        "trend": "Growing (+6% YoY)",
    },
    "data scientist": {
        "job_postings": 45000,
        "search_interest": "Very High",
        "talent_pool": 800000,
        "competition_index": 8.5,
        "trend": "Growing (+15% YoY)",
    },
    "data": {
        "job_postings": 120000,
        "search_interest": "High",
        "talent_pool": 2000000,
        "competition_index": 7.0,
        "trend": "Growing (+10% YoY)",
    },
    "product manager": {
        "job_postings": 60000,
        "search_interest": "High",
        "talent_pool": 900000,
        "competition_index": 7.5,
        "trend": "Growing (+5% YoY)",
    },
    "product": {
        "job_postings": 80000,
        "search_interest": "High",
        "talent_pool": 1200000,
        "competition_index": 6.5,
        "trend": "Growing (+5% YoY)",
    },
    "designer": {
        "job_postings": 55000,
        "search_interest": "Medium",
        "talent_pool": 1100000,
        "competition_index": 5.8,
        "trend": "Stable (+2% YoY)",
    },
    "ux": {
        "job_postings": 40000,
        "search_interest": "High",
        "talent_pool": 700000,
        "competition_index": 6.5,
        "trend": "Growing (+7% YoY)",
    },
    "devops": {
        "job_postings": 50000,
        "search_interest": "High",
        "talent_pool": 600000,
        "competition_index": 8.0,
        "trend": "Growing (+12% YoY)",
    },
    "marketing": {
        "job_postings": 100000,
        "search_interest": "Medium",
        "talent_pool": 2500000,
        "competition_index": 4.5,
        "trend": "Stable (+1% YoY)",
    },
    "sales": {
        "job_postings": 180000,
        "search_interest": "Medium",
        "talent_pool": 4000000,
        "competition_index": 4.0,
        "trend": "Stable (+1% YoY)",
    },
    "hr": {
        "job_postings": 60000,
        "search_interest": "Medium",
        "talent_pool": 1500000,
        "competition_index": 4.2,
        "trend": "Stable (+2% YoY)",
    },
    "analyst": {
        "job_postings": 90000,
        "search_interest": "High",
        "talent_pool": 1800000,
        "competition_index": 5.5,
        "trend": "Growing (+6% YoY)",
    },
    "manager": {
        "job_postings": 250000,
        "search_interest": "High",
        "talent_pool": 5000000,
        "competition_index": 5.0,
        "trend": "Stable (+2% YoY)",
    },
    "director": {
        "job_postings": 40000,
        "search_interest": "Medium",
        "talent_pool": 800000,
        "competition_index": 5.5,
        "trend": "Stable (+1% YoY)",
    },
    "nurse": {
        "job_postings": 200000,
        "search_interest": "Very High",
        "talent_pool": 4000000,
        "competition_index": 9.0,
        "trend": "Growing (+12% YoY)",
    },
    "driver": {
        "job_postings": 300000,
        "search_interest": "High",
        "talent_pool": 3500000,
        "competition_index": 8.5,
        "trend": "Growing (+10% YoY)",
    },
    "warehouse": {
        "job_postings": 250000,
        "search_interest": "High",
        "talent_pool": 3000000,
        "competition_index": 7.5,
        "trend": "Growing (+8% YoY)",
    },
    "mechanic": {
        "job_postings": 80000,
        "search_interest": "Medium",
        "talent_pool": 1200000,
        "competition_index": 6.0,
        "trend": "Stable (+3% YoY)",
    },
    "electrician": {
        "job_postings": 70000,
        "search_interest": "High",
        "talent_pool": 900000,
        "competition_index": 7.5,
        "trend": "Growing (+6% YoY)",
    },
    "accountant": {
        "job_postings": 85000,
        "search_interest": "Medium",
        "talent_pool": 1800000,
        "competition_index": 4.8,
        "trend": "Stable (+2% YoY)",
    },
    "teacher": {
        "job_postings": 120000,
        "search_interest": "Medium",
        "talent_pool": 3500000,
        "competition_index": 3.5,
        "trend": "Stable (+1% YoY)",
    },
    "construction": {
        "job_postings": 180000,
        "search_interest": "High",
        "talent_pool": 2500000,
        "competition_index": 7.0,
        "trend": "Growing (+5% YoY)",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert *value* to float, stripping currency symbols and commas."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = str(value).replace("$", "").replace(",", "").replace("%", "").strip()
        if not cleaned or cleaned == "N/A":
            return default
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    """Convert *value* to int safely."""
    try:
        return int(_safe_float(value, float(default)))
    except (ValueError, TypeError, OverflowError):
        return default


def _get_nested(d: dict, *keys: str, default: Any = None) -> Any:
    """Safely traverse nested dicts."""
    current = d
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _weight_for_source(source_label: str) -> float:
    """Return reliability weight for a source label, defaulting to 0.5."""
    if not source_label:
        return 0.5
    # Try exact match first, then substring matching
    if source_label in SOURCE_WEIGHTS:
        return SOURCE_WEIGHTS[source_label]
    source_lower = source_label.lower()
    for key, weight in SOURCE_WEIGHTS.items():
        if key.lower() in source_lower or source_lower in key.lower():
            return weight
    return 0.5


def _weighted_median(values: List[float], weights: List[float]) -> float:
    """Compute a weighted median from parallel value/weight lists.

    Sorts by value, accumulates weights, and finds the value at which
    cumulative weight crosses 50 % of total weight.
    """
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]

    pairs = sorted(zip(values, weights), key=lambda p: p[0])
    total_weight = sum(w for _, w in pairs)
    if total_weight <= 0:
        return statistics.median(values)

    cumulative = 0.0
    half = total_weight / 2.0
    for val, wt in pairs:
        cumulative += wt
        if cumulative >= half:
            return val
    return pairs[-1][0]


def _percentile(sorted_values: List[float], pct: float) -> float:
    """Compute *pct* percentile (0-100) from an already-sorted list."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100.0)
    f = int(math.floor(k))
    c = min(f + 1, len(sorted_values) - 1)
    d = k - f
    return sorted_values[f] + d * (sorted_values[c] - sorted_values[f])


def _flag_outliers(values: List[float], tolerance_std: float = 2.0) -> List[bool]:
    """Return a boolean mask where True marks outliers (> *tolerance_std* from mean)."""
    if len(values) < 3:
        return [False] * len(values)
    mean = statistics.mean(values)
    std = statistics.stdev(values)
    if std == 0:
        return [False] * len(values)
    return [abs(v - mean) > tolerance_std * std for v in values]


def _market_temperature(competition_index: float) -> str:
    """Classify market temperature from a competition index (postings / supply)."""
    if competition_index >= 3.0:
        return "hot"
    elif competition_index >= 1.5:
        return "warm"
    elif competition_index >= 0.5:
        return "cool"
    return "cold"


def _trend_direction(values: List[float]) -> str:
    """Determine trend direction from a time series (oldest first)."""
    if len(values) < 2:
        return "stable"
    first_half = (
        statistics.mean(values[: len(values) // 2]) if values[: len(values) // 2] else 0
    )
    second_half = (
        statistics.mean(values[len(values) // 2 :]) if values[len(values) // 2 :] else 0
    )
    if first_half == 0:
        return "stable"
    change_pct = (second_half - first_half) / abs(first_half)
    if change_pct > 0.05:
        return "growing"
    elif change_pct < -0.05:
        return "declining"
    return "stable"


def _kb_industry_benchmarks(kb: dict, industry: str) -> dict:
    """Look up industry-specific benchmarks from the knowledge base.

    Resolution order:
      1. Canonical standardizer lookup -> ``kb_key`` metadata field (preferred).
      2. Direct match in ``_INDUSTRY_TO_KB_KEY`` (hardcoded fallback).
      3. Fallback to empty dict.
    """
    kb_key = ""
    # 1. Standardizer first (canonical taxonomy is the source of truth)
    if _HAS_STANDARDIZER:
        canonical = _std_industry(industry)
        meta = _CANON_IND.get(canonical, {})
        kb_key = meta.get("kb_key") or ""
        if kb_key:
            logger.debug(
                "KB industry resolved via standardizer: %s -> %s -> kb_key=%s",
                industry,
                canonical,
                kb_key,
            )
    # 2. Fallback to hardcoded dict
    if not kb_key:
        kb_key = _INDUSTRY_TO_KB_KEY.get(industry) or ""
    if not kb_key:
        return {}
    return _get_nested(kb, "industry_specific_benchmarks", kb_key, default={})


def _kb_salary_trends(kb: dict, region: str = "united_states") -> dict:
    """Return salary trend data from the KB for a given region."""
    return _get_nested(kb, "salary_trends", region, default={})


def _kb_platform_benchmarks(kb: dict) -> dict:
    """Return the benchmarks section of the KB."""
    return _get_nested(kb, "benchmarks", default={})


def _kb_regional_data(kb: dict, region: str = "united_states") -> dict:
    """Return regional economic data from the KB."""
    return _get_nested(kb, "regional_data", region, default={})


def _kb_platform_deep(kb: dict, platform: str) -> dict:
    """Get deep platform intelligence for a specific platform.

    Uses the canonical standardizer when available to resolve aliases
    (e.g. ``"Facebook"`` -> ``"meta"``, ``"Bing"`` -> ``"microsoft_bing"``).
    """
    pi = kb.get("platform_intelligence", {})
    platforms = pi.get("platforms", {})
    # Try exact key first, then lowercase
    result = platforms.get(platform, platforms.get(platform.lower(), {}))
    if not result and _HAS_STANDARDIZER:
        canon = _std_platform(platform)
        result = platforms.get(canon, {})
    return result


def _kb_recruitment_benchmarks(kb: dict, industry: str) -> dict:
    """Get recruitment benchmarks (CPA/CPH/apply-rate) for an industry.

    The ``recruitment_benchmarks_deep.json`` file uses keys like
    ``healthcare_medical``, ``technology_engineering`` etc. -- these are
    the ``deep_bench_key`` values from CANONICAL_INDUSTRIES.  We resolve
    via standardizer first, with direct-match fallback.
    """
    rb = kb.get("recruitment_benchmarks", {})
    benchmarks = rb.get("industry_benchmarks", {})
    result = {}
    # 1. Standardizer first (deep_bench_key is the canonical mapping)
    if _HAS_STANDARDIZER:
        canonical = _std_industry(industry)
        meta = _CANON_IND.get(canonical, {})
        deep_key = meta.get("deep_bench_key") or ""
        if deep_key:
            result = benchmarks.get(deep_key, {})
    # 2. Fallback: try exact key, then common variants
    if not result:
        result = benchmarks.get(industry, {})
    if not result:
        result = benchmarks.get(industry.lower().replace(" ", "_"), {})
    return result


def _kb_regional_market(kb: dict, region: str, market: str = "") -> dict:
    """Get regional hiring intelligence for a specific region/market."""
    rh = kb.get("regional_hiring", {})
    regions = rh.get("regions", {})
    region_data = regions.get(region, {})
    if market:
        return region_data.get(market, {})
    return region_data


def _kb_employer_branding(kb: dict) -> dict:
    """Get employer branding strategy intelligence."""
    rs = kb.get("recruitment_strategy", {})
    return rs.get("employer_branding", {})


def _kb_workforce_trends(kb: dict, generation: str = "gen_z") -> dict:
    """Get workforce trends for a specific generation."""
    wt = kb.get("workforce_trends", {})
    gt = wt.get("generational_trends", {})
    return gt.get(generation, {})


def _kb_white_papers(kb: dict, report_key: str = "") -> dict:
    """Get white paper/report data. If report_key given, get specific report."""
    wp = kb.get("white_papers", {})
    reports = wp.get("reports", {})
    if report_key:
        return reports.get(report_key, {})
    return reports


def _kb_supply_ecosystem(kb: dict) -> dict:
    """Get supply ecosystem / programmatic advertising intelligence."""
    se = kb.get("supply_ecosystem", {})
    return se.get("programmatic_ecosystem", {})


def _kb_funnel_benchmarks(kb: dict) -> dict:
    """Get funnel conversion rate benchmarks across industries."""
    rb = kb.get("recruitment_benchmarks", {})
    return rb.get("funnel_conversion_rates", {})


def _kb_google_ads_benchmarks(kb: dict, category: str = "") -> dict:
    """Get Google Ads 2025 campaign performance data from Joveo's first-party data.

    Data priority: Priority 3 (KB benchmark data).
    Source: data/google_ads_2025_benchmarks.json -- 6,338 keywords, $454K spend,
    8 job categories with CPC/CTR stats and top-performing keywords.

    Args:
        kb: Knowledge base dict from load_knowledge_base().
        category: Optional category key (e.g. 'skilled_healthcare', 'general_recruitment',
                  'software_tech', 'logistics_supply_chain', etc.). If empty, returns all categories.

    Returns:
        Category dict with blended_cpc, blended_ctr, cpc_stats, top_performing_keywords,
        or full categories dict if no category specified.
    """
    gab = kb.get("google_ads_benchmarks", {})
    categories = gab.get("categories", {})
    if category:
        return categories.get(category, categories.get(category.lower(), {}))
    return categories


def _kb_appcast_2026_benchmarks(kb: dict) -> dict:
    """Extract structured Appcast 2026 benchmark data from white papers KB.

    Data priority: Priority 3 (KB benchmark data).
    Source: industry_white_papers.json -> reports -> appcast_benchmark_2026 -> benchmarks.
    Contains 200+ data points from 10th annual report:
      - cpa_by_occupation_2025 (24 occupations)
      - cph_by_occupation_2025 (24 occupations)
      - apply_rate_by_occupation_2025 (24 occupations)
      - cost_per_screen/interview/offer_by_occupation_2025
      - search_cpc_by_occupation_2025
      - social_cpc_by_occupation_2025
      - international_cpa_2025 (18 countries)
      - job_ad_insights_2025
      - full funnel median costs (CPC -> CPA -> Screen -> Interview -> Offer -> Hire)

    Returns:
        The benchmarks dict from the Appcast 2026 report, or empty dict.
    """
    wp = kb.get("white_papers", {})
    reports = wp.get("reports", {})
    appcast_2026 = reports.get("appcast_benchmark_2026", {})
    return appcast_2026.get("benchmarks", {})


def _kb_appcast_occupation_cpa(kb: dict, occupation: str = "") -> Optional[str]:
    """Get Appcast 2026 CPA for a specific occupation.

    Args:
        kb: Knowledge base dict.
        occupation: Occupation key (e.g. 'healthcare', 'technology', 'retail').

    Returns:
        CPA string (e.g. '$35.00') or None if not found.
    """
    benchmarks = _kb_appcast_2026_benchmarks(kb)
    cpa_data = benchmarks.get("cpa_by_occupation_2025", {})
    if occupation:
        return cpa_data.get(occupation, cpa_data.get(occupation.lower(), None))
    return None


def _kb_appcast_occupation_cph(kb: dict, occupation: str = "") -> Optional[str]:
    """Get Appcast 2026 CPH for a specific occupation.

    Returns:
        CPH string (e.g. '$2,795') or None if not found.
    """
    benchmarks = _kb_appcast_2026_benchmarks(kb)
    cph_data = benchmarks.get("cph_by_occupation_2025", {})
    if occupation:
        return cph_data.get(occupation, cph_data.get(occupation.lower(), None))
    return None


# ── Mapping from standardized industry keys to Appcast occupation keys ──
_INDUSTRY_TO_APPCAST_OCCUPATION: Dict[str, str] = {
    "healthcare": "healthcare",
    "healthcare_medical": "healthcare",
    "technology": "technology",
    "tech_engineering": "technology",
    "retail": "retail",
    "retail_consumer": "retail",
    "finance": "finance",
    "finance_banking": "finance",
    "insurance": "insurance",
    "construction": "construction_skilled_trades",
    "construction_real_estate": "construction_skilled_trades",
    "blue_collar_trades": "construction_skilled_trades",
    "logistics": "warehousing_logistics",
    "logistics_supply_chain": "warehousing_logistics",
    "transportation": "transportation",
    "manufacturing": "manufacturing",
    "hospitality": "hospitality",
    "hospitality_travel": "hospitality",
    "food_beverage": "food_service",
    "education": "education",
    "legal_services": "legal",
    "marketing": "marketing_advertising",
    "media_entertainment": "marketing_advertising",
    "general": "administration",
    "general_entry_level": "administration",
    "pharma_biotech": "science_engineering",
    "energy_utilities": "science_engineering",
    "professional_services": "consulting",
    "government_utilities": "administration",
}


# ── Mapping from Google Ads benchmark categories to industry keys ──
_INDUSTRY_TO_GOOGLE_ADS_CATEGORY: Dict[str, str] = {
    "healthcare": "skilled_healthcare",
    "healthcare_medical": "skilled_healthcare",
    "pharma_biotech": "skilled_healthcare",
    "technology": "software_tech",
    "tech_engineering": "software_tech",
    "retail": "retail_hospitality",
    "retail_consumer": "retail_hospitality",
    "hospitality": "retail_hospitality",
    "hospitality_travel": "retail_hospitality",
    "food_beverage": "retail_hospitality",
    "logistics": "logistics_supply_chain",
    "logistics_supply_chain": "logistics_supply_chain",
    "transportation": "logistics_supply_chain",
    "blue_collar_trades": "logistics_supply_chain",
    "manufacturing": "logistics_supply_chain",
    "construction": "logistics_supply_chain",
    "construction_real_estate": "logistics_supply_chain",
    "finance": "corporate_professional",
    "finance_banking": "corporate_professional",
    "insurance": "corporate_professional",
    "professional_services": "corporate_professional",
    "legal_services": "corporate_professional",
    "marketing": "corporate_professional",
    "media_entertainment": "corporate_professional",
    "general": "general_recruitment",
    "general_entry_level": "general_recruitment",
    "education": "education_public_service",
    "government_utilities": "education_public_service",
    "energy_utilities": "education_public_service",
}


def _parse_salary_range(range_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse a salary range string like '$80,000 - $120,000' or '80K-120K'."""
    if not range_str or range_str == "Not available":
        return None, None
    import re

    nums = re.findall(
        r"[\d,]+\.?\d*", str(range_str).replace("K", "000").replace("k", "000")
    )
    floats = []
    for n in nums:
        try:
            floats.append(float(n.replace(",", "")))
        except ValueError:
            continue
    if len(floats) >= 2:
        return min(floats), max(floats)
    elif len(floats) == 1:
        return floats[0], floats[0]
    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# CORE VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════


def validate_with_knowledge_base(
    data_point: float,
    kb_benchmark: float,
    tolerance: float = 0.30,
) -> Dict[str, Any]:
    """Cross-reference a live data point against a KB benchmark value.

    Args:
        data_point: The value from a live API source.
        kb_benchmark: The expected value from the knowledge base.
        tolerance: Maximum acceptable fractional deviation (0.30 = 30 %).

    Returns:
        Dict with ``validated`` (bool), ``deviation`` (float fraction),
        and optionally ``flag`` describing the nature of the discrepancy.
    """
    if kb_benchmark == 0 or data_point == 0:
        return {"validated": False, "deviation": 0.0, "flag": "insufficient_data"}

    deviation = abs(data_point - kb_benchmark) / abs(kb_benchmark)
    validated = deviation <= tolerance

    flag: Optional[str] = None
    if not validated:
        if data_point > kb_benchmark:
            flag = "above_benchmark"
        else:
            flag = "below_benchmark"

    return {
        "validated": validated,
        "deviation": round(deviation, 4),
        "flag": flag,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIDENCE SCORING
# ═══════════════════════════════════════════════════════════════════════════════


def compute_confidence_scores(synthesis_results: Dict[str, Any]) -> Dict[str, Any]:
    """Score every synthesis section on data trustworthiness.

    Note: This confidence system is used by the batch generation pipeline
    (Excel/PPT media plan creation). The real-time chatbot pipeline uses
    data_orchestrator.py's independent EnrichmentContext confidence scoring,
    which operates on a per-request basis with different thresholds.

    Scoring rubric:
        1.0 -- 3+ independent sources agree within 15 %
        0.8 -- 2 sources agree
        0.6 -- 1 live source + KB validation
        0.4 -- 1 live source only
        0.2 -- KB / benchmark fallback only
        0.0 -- no data available

    Returns:
        ``{ "per_section": {section_name: score}, "overall": float,
            "data_quality_grade": str }``
    """
    per_section: Dict[str, float] = {}

    for section_key, section_data in synthesis_results.items():
        if section_key in ("confidence_scores", "data_quality"):
            continue
        if not isinstance(section_data, dict):
            per_section[section_key] = 0.0
            continue
        per_section[section_key] = _score_section(section_data)

    # Only include sections that actually have data (score > 0) in the
    # overall average.  Sections with score == 0.0 represent skipped or
    # unavailable data sources and should NOT deflate the confidence
    # grade -- they are "not applicable", not "failed".
    active_scores = [s for s in per_section.values() if s > 0.0]
    overall = round(statistics.mean(active_scores), 2) if active_scores else 0.0

    # Letter grade
    if overall >= 0.85:
        grade = "A"
    elif overall >= 0.70:
        grade = "B"
    elif overall >= 0.55:
        grade = "C"
    elif overall >= 0.35:
        grade = "D"
    else:
        grade = "F"

    return {
        "per_section": per_section,
        "overall": overall,
        "data_quality_grade": grade,
    }


def _score_section(section: dict) -> float:
    """Compute a confidence score for a single synthesis section.

    Looks for ``_meta`` dicts embedded by fuse functions that record
    ``source_count`` and ``kb_validated``.
    """
    source_counts: List[int] = []
    kb_validated_flags: List[bool] = []

    def _walk(d: Any) -> None:
        if isinstance(d, dict):
            meta = d.get("_meta")
            if isinstance(meta, dict):
                sc = meta.get("source_count") or 0
                kv = meta.get("kb_validated", False)
                source_counts.append(sc)
                kb_validated_flags.append(kv)
            for v in d.values():
                _walk(v)
        elif isinstance(d, list):
            for item in d:
                _walk(item)

    _walk(section)

    if not source_counts:
        # No meta information -- check if section has any data at all.
        # S24: Boost fallback score based on data richness.  Sections with
        # many populated fields likely drew from multiple sources even if
        # _meta was not embedded by the fuse function.
        if _section_has_data(section):
            _populated = sum(
                1
                for k, v in section.items()
                if k != "_meta" and v is not None and v != "" and v != 0 and v != []
            )
            if _populated >= 8:
                return 0.8  # Rich data — likely 2+ sources
            elif _populated >= 4:
                return 0.6  # Moderate data — at least 1 source + KB
            return 0.5  # Some data — assume 1 source contributed
        return 0.0

    max_sources = max(source_counts) if source_counts else 0
    any_validated = any(kb_validated_flags)

    if max_sources >= 3:
        return 1.0
    elif max_sources == 2:
        return 0.8
    elif max_sources == 1 and any_validated:
        return 0.6
    elif max_sources == 1:
        return 0.4
    elif any_validated:
        return 0.2
    return 0.0


def _section_has_data(section: dict) -> bool:
    """Return True if a dict contains non-empty, non-meta data."""
    for key, value in section.items():
        if key.startswith("_"):
            continue
        if value and value != {} and value != []:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# FUSE: SALARY INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════


def fuse_salary_intelligence(
    enriched: dict, kb: dict, input_data: dict
) -> Dict[str, Any]:
    """Fuse salary data from BLS, CareerOneStop, DataUSA, O*NET, and Jooble.

    For each role:
      - Collects salary estimates from all available sources
      - Computes weighted median (weight by source reliability)
      - Cross-references against KB benchmarks
      - Flags outliers (> 2 std dev from median)
      - Produces: min, median, max, p25, p75, source_count, confidence
    """
    roles = input_data.get("roles") or [] or input_data.get("target_roles") or [] or []
    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",") if r.strip()]
    # Normalize dict-format roles (e.g. {"title": "...", "count": 5}) to strings
    roles = [r.get("title") or "" if isinstance(r, dict) else r for r in roles]
    roles = [r for r in roles if isinstance(r, str) and r.strip()]

    industry = input_data.get("industry") or ""
    result: Dict[str, Any] = {}

    # Source data accessors
    bls_salaries = enriched.get("salary_data", {})
    onet_data = enriched.get("onet_data", {})
    onet_occupations = (
        onet_data.get("occupations", {}) if isinstance(onet_data, dict) else {}
    )
    datausa_occ = enriched.get("datausa_occupation", enriched.get("datausa_data", {}))
    datausa_occupations = (
        datausa_occ.get("occupations", {}) if isinstance(datausa_occ, dict) else {}
    )
    cos_data = enriched.get("careeronestop_data", {})
    cos_occupations = (
        cos_data.get("occupations", {}) if isinstance(cos_data, dict) else {}
    )
    jooble_data = enriched.get("jooble_data", {})
    jooble_market = (
        jooble_data.get("job_market", {}) if isinstance(jooble_data, dict) else {}
    )

    # KB salary trends
    kb_salary = _kb_salary_trends(kb)
    kb_by_industry = _get_nested(kb_salary, "by_industry", default={})

    for role in roles:
        salary_points: List[Tuple[float, float, str]] = []  # (value, weight, source)

        # --- BLS OES data ---
        bls_entry = bls_salaries.get(role, {})
        if isinstance(bls_entry, dict):
            bls_source = bls_entry.get("source", "BLS OES")
            bls_weight = _weight_for_source(bls_source)
            for field in ("median", "mean"):
                val = _safe_float(bls_entry.get(field))
                if val > 0:
                    salary_points.append((val, bls_weight, bls_source))
                    break  # prefer median over mean

        # --- O*NET salary ---
        onet_entry = onet_occupations.get(role, {})
        if isinstance(onet_entry, dict):
            onet_source = onet_entry.get("source", "O*NET")
            onet_weight = _weight_for_source(onet_source)
            onet_salary = _safe_float(onet_entry.get("median_salary"))
            if onet_salary > 0:
                salary_points.append((onet_salary, onet_weight, onet_source))

        # --- DataUSA wage ---
        datausa_entry = datausa_occupations.get(role, {})
        if isinstance(datausa_entry, dict):
            datausa_wage = _safe_float(datausa_entry.get("average_wage"))
            if datausa_wage > 0:
                salary_points.append(
                    (datausa_wage, _weight_for_source("DataUSA"), "DataUSA")
                )

        # --- CareerOneStop salary ---
        cos_entry = cos_occupations.get(role, {})
        if isinstance(cos_entry, dict):
            cos_source = cos_data.get("source", "CareerOneStop Benchmarks")
            cos_salary_data = cos_entry.get("salary", {})
            if isinstance(cos_salary_data, dict):
                cos_median = _safe_float(cos_salary_data.get("median"))
                if cos_median > 0:
                    salary_points.append(
                        (cos_median, _weight_for_source(cos_source), cos_source)
                    )

        # --- Jooble salary range (parse midpoint) ---
        jooble_role_data = jooble_market.get(role, {})
        if isinstance(jooble_role_data, dict):
            # Jooble data is keyed by location
            for loc_key, loc_data in jooble_role_data.items():
                if isinstance(loc_data, dict):
                    jooble_salary = loc_data.get("salary_range") or ""
                    low, high = _parse_salary_range(str(jooble_salary))
                    if low and high:
                        midpoint = (low + high) / 2
                        jooble_src = jooble_data.get(
                            "source", "Jooble Market Benchmarks"
                        )
                        salary_points.append(
                            (midpoint, _weight_for_source(jooble_src), jooble_src)
                        )
                        break  # Use first location with salary data

        # --- Fallback: Use knowledge base benchmarks if no API data ---
        if not salary_points:
            kb_benchmarks = kb.get("benchmarks", {}) if isinstance(kb, dict) else {}
            # Try industry-specific salary from KB
            industry_salaries = kb_benchmarks.get(
                "salary_ranges", kb_benchmarks.get("compensation", {})
            )

            # Use module-level _ROLE_SALARY_FALLBACKS (single source of truth)
            role_lower = role.lower()
            for keyword, sal_data in _ROLE_SALARY_FALLBACKS.items():
                if keyword in role_lower:
                    salary_points.append(
                        (sal_data["median"], 0.3, "Industry Benchmark")
                    )
                    break
            else:
                # Generic professional fallback
                salary_points.append((85000, 0.2, "General Benchmark"))

        # If only fallback data, use the full fallback structure with percentiles
        if len(salary_points) == 1 and salary_points[0][2] in (
            "Industry Benchmark",
            "General Benchmark",
        ):
            # Use module-level _ROLE_SALARY_FALLBACKS (single source of truth)
            role_lower = role.lower()
            for keyword, sal_data in _ROLE_SALARY_FALLBACKS.items():
                if keyword in role_lower:
                    result[role] = {
                        "median": sal_data["median"],
                        "mean": sal_data["median"],
                        "min": sal_data["min"],
                        "max": sal_data["max"],
                        "p10": sal_data["min"],
                        "p25": sal_data["p25"],
                        "p75": sal_data["p75"],
                        "p90": sal_data["max"],
                        "sources": ["Industry Benchmark"],
                        "outlier_flags": [],
                        "kb_validation": {
                            "validated": False,
                            "deviation": 0.0,
                            "flag": "fallback_data",
                        },
                        "_meta": {"source_count": 1, "kb_validated": False},
                    }
                    break
            else:
                result[role] = {
                    "median": 85000,
                    "mean": 85000,
                    "min": 55000,
                    "max": 140000,
                    "p10": 55000,
                    "p25": 68000,
                    "p75": 105000,
                    "p90": 140000,
                    "sources": ["General Benchmark"],
                    "outlier_flags": [],
                    "kb_validation": {
                        "validated": False,
                        "deviation": 0.0,
                        "flag": "fallback_data",
                    },
                    "_meta": {"source_count": 1, "kb_validated": False},
                }
            continue

        # --- Synthesize ---
        if not salary_points:
            result[role] = _empty_salary_result(role)
            continue

        values = [sp[0] for sp in salary_points]
        weights = [sp[1] for sp in salary_points]
        sources = [sp[2] for sp in salary_points]
        source_count = len(salary_points)

        # Remove outliers before computing final values
        outlier_flags = _flag_outliers(values)
        clean_values = [
            v for v, is_outlier in zip(values, outlier_flags) if not is_outlier
        ]
        clean_weights = [
            w for w, is_outlier in zip(weights, outlier_flags) if not is_outlier
        ]
        flagged_sources = [
            s for s, is_outlier in zip(sources, outlier_flags) if is_outlier
        ]

        if not clean_values:
            clean_values = values
            clean_weights = weights

        w_median = _weighted_median(clean_values, clean_weights)
        sorted_vals = sorted(clean_values)

        # BLS-specific percentiles (more precise) or computed from available data
        p10 = _safe_float(bls_entry.get("p10")) if isinstance(bls_entry, dict) else 0.0
        p90 = _safe_float(bls_entry.get("p90")) if isinstance(bls_entry, dict) else 0.0
        if p10 <= 0:
            p10 = (
                _percentile(sorted_vals, 10)
                if len(sorted_vals) >= 3
                else round(w_median * 0.65, 0)
            )
        if p90 <= 0:
            p90 = (
                _percentile(sorted_vals, 90)
                if len(sorted_vals) >= 3
                else round(w_median * 1.45, 0)
            )

        p25 = (
            _percentile(sorted_vals, 25)
            if len(sorted_vals) >= 3
            else round(w_median * 0.82, 0)
        )
        p75 = (
            _percentile(sorted_vals, 75)
            if len(sorted_vals) >= 3
            else round(w_median * 1.18, 0)
        )

        # KB validation
        kb_validation = {"validated": False, "deviation": 0.0, "flag": None}
        kb_industry_key = ""
        if _HAS_STANDARDIZER:
            _canon = _std_industry(industry)
            _meta = _CANON_IND.get(_canon, {})
            kb_industry_key = _meta.get("kb_key") or ""
        if not kb_industry_key:
            kb_industry_key = _INDUSTRY_TO_KB_KEY.get(industry) or ""
        if kb_industry_key and kb_by_industry:
            industry_salary_data = kb_by_industry.get(kb_industry_key, {})
            if isinstance(industry_salary_data, dict):
                # Try to extract a comparable salary benchmark from KB
                kb_salary_growth = _safe_float(
                    industry_salary_data.get("salary_growth_moderation")
                )
                # KB doesn't store absolute salary -- use overall median as proxy
                overall_data = kb_salary.get("overall", {})
                if isinstance(overall_data, dict):
                    kb_median_str = overall_data.get("median_2025")
                    kb_median = _safe_float(kb_median_str)
                    if kb_median > 0:
                        kb_validation = validate_with_knowledge_base(
                            w_median,
                            kb_median,
                            tolerance=0.30,  # S27: tightened from 0.50
                        )

        result[role] = {
            "median": round(w_median),
            "mean": round(statistics.mean(clean_values)) if clean_values else 0,
            "min": round(min(sorted_vals)) if sorted_vals else 0,
            "max": round(max(sorted_vals)) if sorted_vals else 0,
            "p10": round(p10),
            "p25": round(p25),
            "p75": round(p75),
            "p90": round(p90),
            "sources": list(set(sources)),
            "outlier_flags": flagged_sources,
            "kb_validation": kb_validation,
            "_meta": {
                "source_count": source_count,
                "kb_validated": kb_validation.get("validated", False),
            },
        }
        if flagged_sources:
            logger.warning("Salary outliers detected for %s: %s", role, flagged_sources)

    # --- Enrich from recruitment benchmarks KB ---
    _rb = _kb_recruitment_benchmarks(kb, industry)
    if _rb:
        for _rk, _rv in result.items():
            if isinstance(_rv, dict):
                _rv["industry_cpa_benchmark"] = _rb.get("cpa", {}).get("median", None)
                _rv["industry_cph_benchmark"] = _rb.get("cph", {}).get("median", None)
                _rv["industry_time_to_fill"] = _rb.get("time_to_fill", {}).get(
                    "median", None
                )

    # --- Enrich from DataUSA location data (previously orphaned) ---
    _dusa_loc = enriched.get("datausa_location_data", {})
    if isinstance(_dusa_loc, dict) and _dusa_loc:
        for _rk, _rv in result.items():
            if isinstance(_rv, dict) and not _rv.get("location_demographics"):
                _rv["location_demographics"] = {
                    "population": _dusa_loc.get("population"),
                    "median_household_income": _dusa_loc.get("median_household_income"),
                    "poverty_rate": _dusa_loc.get("poverty_rate"),
                }

    logger.info("Salary intelligence fused for %d roles", len(result))
    return result


def _empty_salary_result(role: str) -> Dict[str, Any]:
    """Return an empty salary structure when no data is available."""
    return {
        "median": 0,
        "mean": 0,
        "min": 0,
        "max": 0,
        "p10": 0,
        "p25": 0,
        "p75": 0,
        "p90": 0,
        "sources": [],
        "outlier_flags": [],
        "kb_validation": {"validated": False, "deviation": 0.0, "flag": "no_data"},
        "_meta": {"source_count": 0, "kb_validated": False},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FUSE: JOB MARKET DEMAND
# ═══════════════════════════════════════════════════════════════════════════════


def fuse_job_market_demand(
    enriched: dict, kb: dict, input_data: dict
) -> Dict[str, Any]:
    """Fuse demand signals from Adzuna, Jooble, Google Ads, Google Trends, LinkedIn.

    For each role + location:
      - Job posting volume (Adzuna + Jooble)
      - Search interest (Google Trends + Google Ads keyword volume)
      - Professional supply (LinkedIn talent pool)
      - Competition index (postings / available talent)
      - Market temperature: hot / warm / cool / cold
      - Trend direction: growing / stable / declining
    """
    roles = input_data.get("roles") or [] or input_data.get("target_roles") or [] or []
    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",") if r.strip()]
    roles = [r.get("title") or "" if isinstance(r, dict) else r for r in roles]
    roles = [r for r in roles if isinstance(r, str) and r.strip()]
    locations = input_data.get("locations") or []
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",") if l.strip()]
    locations = [
        (
            ", ".join(
                filter(
                    None,
                    [l.get("city") or "", l.get("state") or "", l.get("country") or ""],
                )
            )
            if isinstance(l, dict)
            else l
        )
        for l in locations
    ]
    locations = [l for l in locations if isinstance(l, str) and l.strip()]

    industry = input_data.get("industry") or ""

    # Source data
    adzuna_data = enriched.get("job_market", enriched.get("adzuna_data", {}))
    jooble_data = enriched.get("jooble_data", {})
    jooble_market = (
        jooble_data.get("job_market", {}) if isinstance(jooble_data, dict) else {}
    )
    google_trends = enriched.get(
        "search_trends", enriched.get("google_trends_data", {})
    )
    google_ads = enriched.get(
        "google_ads_data", enriched.get("google_ads_keyword_data", {})
    )
    google_ads_keywords = (
        google_ads.get("keywords", {}) if isinstance(google_ads, dict) else {}
    )
    linkedin_data = enriched.get("linkedin_ads_data", {})
    linkedin_roles = (
        linkedin_data.get("roles", {}) if isinstance(linkedin_data, dict) else {}
    )

    # KB industry data
    kb_industry = _kb_industry_benchmarks(kb, industry)

    result: Dict[str, Any] = {}

    for role in roles:
        role_result: Dict[str, Any] = {}

        # --- Job posting volume ---
        posting_volumes: List[Tuple[int, str]] = []

        # Adzuna
        adzuna_role = adzuna_data.get(role, {}) if isinstance(adzuna_data, dict) else {}
        if isinstance(adzuna_role, dict):
            adzuna_count = _safe_int(adzuna_role.get("posting_count"))
            if adzuna_count > 0:
                posting_volumes.append((adzuna_count, "Adzuna"))

        # Jooble (aggregate across locations)
        jooble_role = jooble_market.get(role, {})
        if isinstance(jooble_role, dict):
            total_jooble = 0
            for loc_key, loc_data in jooble_role.items():
                if isinstance(loc_data, dict):
                    total_jooble += _safe_int(loc_data.get("total_job_postings"))
            if total_jooble > 0:
                posting_volumes.append(
                    (total_jooble, jooble_data.get("source", "Jooble"))
                )

        total_postings = sum(v for v, _ in posting_volumes)
        posting_source_count = len(posting_volumes)

        # --- Search interest ---
        search_volume = 0
        trend_values: List[float] = []

        # Google Ads keyword volume
        gads_role = google_ads_keywords.get(role, {})
        if isinstance(gads_role, dict):
            search_volume = _safe_int(gads_role.get("avg_monthly_searches"))

        # Google Trends
        if isinstance(google_trends, dict):
            trends_data = google_trends.get(
                "trends", google_trends.get("interest_over_time", {})
            )
            if isinstance(trends_data, dict):
                role_trend = trends_data.get(role, trends_data.get(f"{role} jobs", {}))
                if isinstance(role_trend, dict):
                    trend_values = [
                        _safe_float(v)
                        for v in role_trend.values()
                        if _safe_float(v) > 0
                    ]
                elif isinstance(role_trend, list):
                    trend_values = [
                        _safe_float(v) for v in role_trend if _safe_float(v) > 0
                    ]

        # --- Professional supply (LinkedIn) ---
        linkedin_role = linkedin_roles.get(role, {})
        audience_str = ""
        if isinstance(linkedin_role, dict):
            audience_str = str(linkedin_role.get("estimated_audience") or "")

        talent_pool = _parse_audience_number(audience_str)

        # --- Fallback demand data when APIs return nothing ---
        if total_postings == 0 and search_volume == 0 and talent_pool == 0:
            # Use module-level _ROLE_DEMAND_FALLBACKS (single source of truth)
            role_lower = role.lower()
            fallback_demand = None
            for keyword, fb_data in _ROLE_DEMAND_FALLBACKS.items():
                if keyword in role_lower:
                    fallback_demand = fb_data
                    break
            if fallback_demand is None:
                # Generic professional fallback
                fallback_demand = {
                    "job_postings": 75000,
                    "search_interest": "Medium",
                    "talent_pool": 1500000,
                    "competition_index": 5.0,
                    "trend": "Stable (+2% YoY)",
                }

            total_postings = fallback_demand["job_postings"]
            search_volume = total_postings // 10  # Estimate monthly search volume
            talent_pool = fallback_demand["talent_pool"]
            competition_index = (
                fallback_demand["competition_index"] / 100.0
            )  # Normalize
            posting_volumes = [(total_postings, "Industry Benchmark")]
            posting_source_count = 1
            trend_dir = fallback_demand["trend"]

        # --- Competition index ---
        _used_fallback = any(s == "Industry Benchmark" for _, s in posting_volumes)
        if not _used_fallback:
            competition_index = 0.0
            if talent_pool > 0 and total_postings > 0:
                competition_index = round(total_postings / talent_pool, 4)
            elif total_postings > 5000:
                competition_index = 2.5  # Estimated high
            elif total_postings > 1000:
                competition_index = 1.0
            trend_dir = _trend_direction(trend_values) if trend_values else "stable"

        temperature = _market_temperature(competition_index * 100)

        # Source counting
        source_count = posting_source_count
        if search_volume > 0:
            source_count += 1
        if trend_values:
            source_count += 1
        if talent_pool > 0:
            source_count += 1

        # KB validation
        kb_validated = False
        if kb_industry:
            # Check if industry has known hiring strength
            hiring_strength = kb_industry.get("hiring_strength") or ""
            if hiring_strength:
                kb_validated = True

        role_result = {
            "total_postings": total_postings,
            "posting_sources": [s for _, s in posting_volumes],
            "search_volume_monthly": search_volume,
            "trend_direction": trend_dir,
            "talent_pool_estimate": talent_pool,
            "competition_index": round(competition_index, 4),
            "market_temperature": temperature,
            "kb_industry_context": {
                "hiring_strength": kb_industry.get("hiring_strength", "N/A"),
                "demand_drivers": kb_industry.get("demand_drivers") or [],
                "outlook": kb_industry.get("outlook", "N/A"),
            },
            "_meta": {
                "source_count": source_count,
                "kb_validated": kb_validated,
            },
        }

        # Per-location breakdown if data available
        location_breakdown: Dict[str, Any] = {}
        for loc in locations:
            loc_posting = 0
            loc_sources: List[str] = []

            # Jooble per-location
            jooble_loc = (
                jooble_role.get(loc, {}) if isinstance(jooble_role, dict) else {}
            )
            if isinstance(jooble_loc, dict):
                jlp = _safe_int(jooble_loc.get("total_job_postings"))
                if jlp > 0:
                    loc_posting += jlp
                    loc_sources.append("Jooble")

            jooble_activity = ""
            if isinstance(jooble_loc, dict):
                jooble_activity = jooble_loc.get("market_activity") or ""

            location_breakdown[loc] = {
                "postings": loc_posting,
                "market_activity": jooble_activity
                or (
                    "High"
                    if loc_posting > 3000
                    else "Medium" if loc_posting > 500 else "Low"
                ),
                "sources": loc_sources,
            }

        if location_breakdown:
            role_result["by_location"] = location_breakdown

        result[role] = role_result

    # --- Enrich from workforce trends KB ---
    _wt = _kb_workforce_trends(kb)
    if _wt:
        for _rk, _rv in result.items():
            if isinstance(_rv, dict):
                _rv["workforce_trends"] = {
                    "gen_z_platform_preferences": _wt.get(
                        "job_search_behavior", {}
                    ).get("platform_usage", {}),
                    "remote_work_trends": _wt.get("workplace_expectations", {}).get(
                        "flexibility", {}
                    ),
                }

    # --- Enrich from Google Trends (previously orphaned) ---
    _gtrends = enriched.get("search_trends", enriched.get("google_trends_data", {}))
    if isinstance(_gtrends, dict) and _gtrends:
        for _rk, _rv in result.items():
            if isinstance(_rv, dict) and not _rv.get("search_trend"):
                _rv["search_trend"] = _gtrends

    # --- Enrich from FRED indicators (previously orphaned) ---
    _fred = enriched.get("fred_indicators", enriched.get("fred_data", {}))
    if isinstance(_fred, dict) and _fred:
        for _rk, _rv in result.items():
            if isinstance(_rv, dict):
                _rv["macro_economic"] = {
                    "unemployment_rate": _fred.get("unemployment_rate"),
                    "labor_force_participation": _fred.get("labor_force_participation"),
                    "job_openings_rate": _fred.get("job_openings_rate"),
                }

    logger.info("Job market demand fused for %d roles", len(result))
    return result


def _parse_audience_number(audience_str: str) -> int:
    """Parse an audience string like '2M-5M' or '150K-300K' into a midpoint integer."""
    if not audience_str:
        return 0
    import re

    audience_str = str(audience_str).upper()
    nums = re.findall(r"([\d.]+)\s*([MK]?)", audience_str)
    values: List[float] = []
    for num_str, suffix in nums:
        try:
            val = float(num_str)
            if suffix == "M":
                val *= 1_000_000
            elif suffix == "K":
                val *= 1_000
            values.append(val)
        except ValueError:
            continue
    if values:
        return int(statistics.mean(values))
    # Try plain integer
    try:
        return int(float(audience_str.replace(",", "")))
    except (ValueError, TypeError):
        return 0


# ═══════════════════════════════════════════════════════════════════════════════
# FUSE: LOCATION PROFILES
# ═══════════════════════════════════════════════════════════════════════════════


def fuse_location_profiles(
    enriched: dict, kb: dict, input_data: dict
) -> Dict[str, Any]:
    """Fuse location data from Census, GeoNames, Teleport, RESTCountries, IMF, WorldBank.

    For each location:
      - Population and workforce size
      - Cost of living index (Teleport)
      - GDP and economic indicators (IMF / World Bank for international)
      - Currency and exchange rates
      - Talent density (workforce / population)
      - Infrastructure quality score
    """
    locations = input_data.get("locations") or []
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",") if l.strip()]
    locations = [
        (
            ", ".join(
                filter(
                    None,
                    [l.get("city") or "", l.get("state") or "", l.get("country") or ""],
                )
            )
            if isinstance(l, dict)
            else l
        )
        for l in locations
    ]
    locations = [l for l in locations if isinstance(l, str) and l.strip()]

    # Source data
    census_data = enriched.get("location_demographics", enriched.get("census_data", {}))
    geonames_raw = enriched.get("geonames_data", {})
    geonames_locations = (
        geonames_raw.get("locations", {}) if isinstance(geonames_raw, dict) else {}
    )
    teleport_raw = enriched.get("teleport_data", {})
    teleport_cities = (
        teleport_raw.get("cities", {}) if isinstance(teleport_raw, dict) else {}
    )
    countries_data = enriched.get("country_data", enriched.get("countries_data", {}))
    imf_raw = enriched.get("imf_indicators", enriched.get("imf_data", {}))
    imf_countries = imf_raw.get("countries", {}) if isinstance(imf_raw, dict) else {}
    world_bank = enriched.get("global_indicators", enriched.get("world_bank_data", {}))
    currency_rates = enriched.get("currency_rates", {})
    datausa_loc = enriched.get("datausa_location", {})
    datausa_locations = (
        datausa_loc.get("locations", {}) if isinstance(datausa_loc, dict) else {}
    )

    # KB regional data
    kb_us = _kb_regional_data(kb, "united_states")
    kb_uk = _kb_regional_data(kb, "united_kingdom")
    kb_global = _kb_regional_data(kb, "global")

    result: Dict[str, Any] = {}

    for loc in locations:
        loc_profile: Dict[str, Any] = {"location": loc}
        source_count = 0
        kb_validated = False

        # --- Census / Demographics ---
        census_entry = census_data.get(loc, {}) if isinstance(census_data, dict) else {}
        if isinstance(census_entry, dict) and census_entry:
            population = _safe_int(census_entry.get("population"))
            median_income = _safe_int(census_entry.get("median_income"))
            if population > 0:
                loc_profile["population"] = population
                source_count += 1
            if median_income > 0:
                loc_profile["median_household_income"] = median_income
            loc_profile["state_name"] = census_entry.get("state_name") or ""
            loc_profile["geo_level"] = census_entry.get("geo_level") or ""
            loc_profile["demographics_source"] = census_entry.get("source", "Census")

        # --- DataUSA location data ---
        datausa_entry = datausa_locations.get(loc, {})
        if isinstance(datausa_entry, dict) and datausa_entry:
            dusa_pop = _safe_int(datausa_entry.get("population"))
            if dusa_pop > 0 and "population" not in loc_profile:
                loc_profile["population"] = dusa_pop
                source_count += 1
            dusa_income = _safe_int(datausa_entry.get("median_income"))
            if dusa_income > 0 and "median_household_income" not in loc_profile:
                loc_profile["median_household_income"] = dusa_income

        # --- GeoNames ---
        geo_entry = geonames_locations.get(loc, {})
        if isinstance(geo_entry, dict) and geo_entry:
            loc_profile["coordinates"] = {
                "latitude": _safe_float(geo_entry.get("latitude")),
                "longitude": _safe_float(geo_entry.get("longitude")),
            }
            loc_profile["timezone"] = geo_entry.get("timezone") or ""
            geo_pop = _safe_int(geo_entry.get("population"))
            if geo_pop > 0 and "population" not in loc_profile:
                loc_profile["population"] = geo_pop
            loc_profile["country_code"] = geo_entry.get("country_code") or ""
            source_count += 1

        # --- Teleport quality of life ---
        teleport_entry = teleport_cities.get(loc, {})
        if isinstance(teleport_entry, dict) and teleport_entry:
            city_score = _safe_float(teleport_entry.get("teleport_city_score"))
            if city_score > 0:
                loc_profile["quality_of_life_score"] = city_score

            quality_scores = teleport_entry.get("quality_scores", {})
            if isinstance(quality_scores, dict) and quality_scores:
                loc_profile["quality_breakdown"] = quality_scores

            cost_of_living = teleport_entry.get("cost_of_living", {})
            if isinstance(cost_of_living, dict) and cost_of_living:
                loc_profile["cost_of_living"] = cost_of_living

            # Teleport salary data for this location
            teleport_salaries = teleport_entry.get("salaries", {})
            if isinstance(teleport_salaries, dict) and teleport_salaries:
                loc_profile["teleport_salaries"] = teleport_salaries

            source_count += 1

        # --- REST Countries ---
        country_entry = {}
        if isinstance(countries_data, dict):
            # Countries data may be keyed by country name or code
            for ck, cv in countries_data.items():
                if isinstance(cv, dict) and (ck in loc or loc in ck):
                    country_entry = cv
                    break

        if isinstance(country_entry, dict) and country_entry:
            loc_profile["country_info"] = {
                "name": country_entry.get("name") or "",
                "population": _safe_int(country_entry.get("population")),
                "currencies": country_entry.get("currencies", {}),
                "languages": country_entry.get("languages", {}),
                "region": country_entry.get("region") or "",
                "subregion": country_entry.get("subregion") or "",
            }
            source_count += 1

        # --- IMF economic indicators ---
        # Match location to IMF country
        for imf_key, imf_entry in imf_countries.items():
            if not isinstance(imf_entry, dict):
                continue
            if imf_key.lower() in loc.lower() or loc.lower() in imf_key.lower():
                loc_profile["economic_indicators"] = {
                    "gdp_growth": imf_entry.get("gdp_growth"),
                    "inflation": imf_entry.get("inflation"),
                    "unemployment": imf_entry.get("unemployment"),
                    "source": "IMF",
                }
                source_count += 1
                break

        # --- World Bank ---
        if isinstance(world_bank, dict):
            for wb_key, wb_entry in world_bank.items():
                if isinstance(wb_entry, dict) and (
                    wb_key.lower() in loc.lower() or loc.lower() in wb_key.lower()
                ):
                    loc_profile["world_bank_indicators"] = {
                        "unemployment_rate": wb_entry.get("unemployment_rate"),
                        "gdp_growth": wb_entry.get("gdp_growth"),
                        "labor_force": wb_entry.get("labor_force"),
                        "source": "World Bank",
                    }
                    source_count += 1
                    break

        # --- Currency rates ---
        if isinstance(currency_rates, dict) and currency_rates:
            loc_profile["currency_rates_usd"] = dict(list(currency_rates.items())[:10])

        # --- Talent density ---
        population = loc_profile.get("population") or 0
        if population > 0:
            # Estimate workforce as ~48% of population (standard labor force participation)
            workforce_estimate = int(population * 0.48)
            loc_profile["workforce_estimate"] = workforce_estimate
            loc_profile["talent_density"] = round(workforce_estimate / population, 3)

        # --- Infrastructure quality (from Teleport scores) ---
        quality_breakdown = loc_profile.get("quality_breakdown", {})
        if isinstance(quality_breakdown, dict):
            infra_scores = []
            for key in ("Commute", "Internet Access", "Business Freedom", "Education"):
                score = _safe_float(quality_breakdown.get(key))
                if score > 0:
                    infra_scores.append(score)
            if infra_scores:
                loc_profile["infrastructure_score"] = round(
                    statistics.mean(infra_scores), 2
                )

        # --- KB validation ---
        loc_lower = loc.lower()
        if "us" in loc_lower or any(st in loc for st in ["CA", "NY", "TX", "FL", "IL"]):
            if kb_us:
                kb_validated = True
        elif "uk" in loc_lower or "london" in loc_lower:
            if kb_uk:
                kb_validated = True

        loc_profile["_meta"] = {
            "source_count": source_count,
            "kb_validated": kb_validated,
        }

        result[loc] = loc_profile

    # --- Enrich from regional hiring KB ---
    _rh_regions = kb.get("regional_hiring", {}).get("regions", {})
    if _rh_regions:
        for _lk, _lv in result.items():
            if isinstance(_lv, dict):
                # Try to match location to a region/market
                _lk_lower = _lk.lower().replace(" ", "_").replace(",", "")
                for _region_key, _region_data in _rh_regions.items():
                    if isinstance(_region_data, dict):
                        for _market_key, _market_data in _region_data.items():
                            if isinstance(_market_data, dict):
                                _mname = (_market_data.get("name") or "" or "").lower()
                                if _lk.lower() in _mname or _lk_lower in _market_key:
                                    _lv["regional_intelligence"] = {
                                        "region": _region_key,
                                        "market": _market_key,
                                        "top_job_boards": _market_data.get(
                                            "top_job_boards"
                                        )
                                        or [],
                                        "dominant_industries": _market_data.get(
                                            "dominant_industries"
                                        )
                                        or [],
                                        "talent_dynamics": _market_data.get(
                                            "talent_dynamics", {}
                                        ),
                                        "hiring_regulations": _market_data.get(
                                            "hiring_regulations", {}
                                        ),
                                        "cultural_norms": _market_data.get(
                                            "cultural_norms", {}
                                        ),
                                        "cpa_benchmark": _market_data.get(
                                            "cpa_benchmark", {}
                                        ),
                                    }
                                    break

    # --- Enrich from currency rates (previously orphaned) ---
    _curr = enriched.get("currency_rates", enriched.get("exchange_rates", {}))
    if isinstance(_curr, dict) and _curr:
        for _lk, _lv in result.items():
            if isinstance(_lv, dict) and not _lv.get("currency_data"):
                _lv["currency_data"] = _curr

    # --- Enrich from country data (previously orphaned) ---
    _cdata = enriched.get("country_data", {})
    if isinstance(_cdata, dict) and _cdata:
        for _lk, _lv in result.items():
            if isinstance(_lv, dict) and not _lv.get("country_profile"):
                _lv["country_profile"] = {
                    "gdp": _cdata.get("gdp"),
                    "population": _cdata.get("population"),
                    "gdp_per_capita": _cdata.get("gdp_per_capita"),
                }

    # --- Enrich from GeoNames (previously orphaned) ---
    _geo = enriched.get("geonames_data", {})
    if isinstance(_geo, dict) and _geo:
        for _lk, _lv in result.items():
            if isinstance(_lv, dict) and not _lv.get("geo_data"):
                _lv["geo_data"] = {
                    "timezone": _geo.get("timezone"),
                    "population": _geo.get("population"),
                    "latitude": _geo.get("latitude"),
                    "longitude": _geo.get("longitude"),
                }

    # --- Enrich from IMF indicators (previously orphaned) ---
    _imf = enriched.get("imf_indicators", enriched.get("imf_data", {}))
    if isinstance(_imf, dict) and _imf:
        for _lk, _lv in result.items():
            if isinstance(_lv, dict) and not _lv.get("imf_data"):
                _lv["imf_data"] = _imf

    logger.info("Location profiles fused for %d locations", len(result))
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# FUSE: AD PLATFORM ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════


def fuse_ad_platform_analysis(
    enriched: dict, kb: dict, input_data: dict
) -> Dict[str, Any]:
    """Fuse ad platform data from Google Ads, Meta Ads, Bing Ads, TikTok Ads, LinkedIn Ads.

    For each platform:
      - Estimated CPC, CPM, CPA
      - Audience reach estimate
      - Platform fit score for role type
      - Historical benchmark from KB
      - ROI projection
      - Recommended daily budget range
    """
    roles = input_data.get("roles") or [] or input_data.get("target_roles") or [] or []
    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",") if r.strip()]
    roles = [r.get("title") or "" if isinstance(r, dict) else r for r in roles]
    roles = [r for r in roles if isinstance(r, str) and r.strip()]

    budget = _safe_float(input_data.get("budget") or 0)
    industry = input_data.get("industry") or ""

    # Source data
    google_ads = enriched.get(
        "google_ads_data", enriched.get("google_ads_keyword_data", {})
    )
    meta_ads = enriched.get("meta_ads_data", {})
    bing_ads = enriched.get("bing_ads_data", {})
    tiktok_ads = enriched.get("tiktok_ads_data", {})
    linkedin_ads = enriched.get("linkedin_ads_data", {})

    # KB platform benchmarks
    kb_benchmarks = _kb_platform_benchmarks(kb)
    kb_cpc = _get_nested(kb_benchmarks, "cost_per_click", "by_platform", default={})
    kb_cpa = _get_nested(kb_benchmarks, "cost_per_application", default={})

    # Platform fit scores by role type
    PLATFORM_FIT = {
        "google": {
            "professional": 0.85,
            "technical": 0.80,
            "hourly": 0.75,
            "executive": 0.80,
            "default": 0.80,
        },
        "meta": {
            "professional": 0.60,
            "technical": 0.50,
            "hourly": 0.85,
            "executive": 0.40,
            "default": 0.65,
        },
        "bing": {
            "professional": 0.70,
            "technical": 0.65,
            "hourly": 0.55,
            "executive": 0.70,
            "default": 0.60,
        },
        "tiktok": {
            "professional": 0.35,
            "technical": 0.30,
            "hourly": 0.80,
            "executive": 0.15,
            "default": 0.45,
        },
        "linkedin": {
            "professional": 0.95,
            "technical": 0.85,
            "hourly": 0.30,
            "executive": 0.95,
            "default": 0.75,
        },
    }

    result: Dict[str, Any] = {}

    # Determine role type for platform fit scoring
    role_type = _classify_role_type(roles)

    # --- Google Ads ---
    result["google"] = _build_platform_entry(
        platform_name="Google Search & Display",
        enriched_data=google_ads,
        data_key="keywords",
        roles=roles,
        kb_cpc_entry=kb_cpc.get("google_search_ads", {}),
        kb_cpa_data=kb_cpa,
        fit_score=PLATFORM_FIT["google"].get(role_type, 0.80),
        budget=budget,
    )

    # --- Enrich Google Ads entry with Joveo first-party benchmark data ---
    # Data priority: Priority 3 (KB benchmark data) -- used to cross-validate
    # live API results or fill gaps when API data is unavailable.
    gads_category = _INDUSTRY_TO_GOOGLE_ADS_CATEGORY.get(industry) or ""
    gads_kb = _kb_google_ads_benchmarks(kb, gads_category) if gads_category else {}
    if gads_kb and isinstance(result.get("google"), dict):
        google_entry = result["google"]
        google_entry["joveo_google_ads_benchmarks"] = {
            "category": gads_kb.get("category_name", gads_category),
            "blended_cpc": gads_kb.get("blended_cpc"),
            "blended_ctr": gads_kb.get("blended_ctr"),
            "cpc_stats": gads_kb.get("cpc_stats", {}),
            "ctr_stats": gads_kb.get("ctr_stats", {}),
            "total_keywords_analyzed": gads_kb.get("total_keywords"),
            "total_spend_analyzed": gads_kb.get("total_spend"),
            "source": "Joveo Google Ads 2025 Campaign Data (first-party)",
            "data_priority": 3,
        }
        # Add top keywords for ad copy recommendations
        top_kw = gads_kb.get("top_performing_keywords") or []
        if top_kw:
            google_entry["joveo_top_keywords"] = [
                {
                    "keyword": kw.get("keyword"),
                    "cpc": kw.get("cpc"),
                    "ctr_pct": kw.get("ctr_pct"),
                }
                for kw in top_kw[:5]
            ]
        # Cross-validate: if live API CPC exists, compare to KB benchmark
        live_cpc = google_entry.get("avg_cpc") or 0
        kb_blended_cpc = gads_kb.get("blended_cpc") or 0
        if isinstance(live_cpc, (int, float)) and live_cpc > 0 and kb_blended_cpc:
            deviation = abs(live_cpc - kb_blended_cpc) / kb_blended_cpc
            google_entry["cpc_kb_cross_validation"] = {
                "live_cpc": live_cpc,
                "kb_benchmark_cpc": kb_blended_cpc,
                "deviation_pct": round(deviation * 100, 1),
                "within_tolerance": deviation <= 0.50,
                "note": (
                    "Live CPC within expected range"
                    if deviation <= 0.50
                    else "Live CPC deviates significantly from Joveo 2025 benchmarks"
                ),
            }

    # --- Enrich with Appcast 2026 search/social CPC data ---
    # Data priority: Priority 3 (KB benchmark data from Appcast 2026 report)
    appcast_bm = _kb_appcast_2026_benchmarks(kb)
    appcast_occupation = _INDUSTRY_TO_APPCAST_OCCUPATION.get(industry) or ""
    if appcast_bm and appcast_occupation:
        search_cpc_data = appcast_bm.get("search_cpc_by_occupation_2025", {})
        social_cpc_data = appcast_bm.get("social_cpc_by_occupation_2025", {})
        search_cpc = search_cpc_data.get(appcast_occupation)
        social_cpc = social_cpc_data.get(appcast_occupation)
        if search_cpc or social_cpc:
            if isinstance(result.get("google"), dict):
                result["google"]["appcast_search_cpc_benchmark"] = search_cpc
            if isinstance(result.get("meta"), dict):
                result["meta"]["appcast_social_cpc_benchmark"] = social_cpc

    # --- Meta (Facebook + Instagram) ---
    result["meta"] = _build_meta_platform_entry(
        enriched_data=meta_ads,
        roles=roles,
        kb_cpc_entry=kb_cpc.get("meta_facebook_ads", {}),
        fit_score=PLATFORM_FIT["meta"].get(role_type, 0.65),
        budget=budget,
    )

    # --- Bing Ads ---
    result["bing"] = _build_platform_entry(
        platform_name="Microsoft / Bing Ads",
        enriched_data=bing_ads,
        data_key="keywords",
        roles=roles,
        kb_cpc_entry=kb_cpc.get("microsoft_bing_ads", {}),
        kb_cpa_data=kb_cpa,
        fit_score=PLATFORM_FIT["bing"].get(role_type, 0.60),
        budget=budget,
    )

    # --- TikTok Ads ---
    result["tiktok"] = _build_platform_entry(
        platform_name="TikTok",
        enriched_data=tiktok_ads,
        data_key="roles",
        roles=roles,
        kb_cpc_entry={},
        kb_cpa_data=kb_cpa,
        fit_score=PLATFORM_FIT["tiktok"].get(role_type, 0.45),
        budget=budget,
    )

    # --- LinkedIn Ads ---
    result["linkedin"] = _build_platform_entry(
        platform_name="LinkedIn",
        enriched_data=linkedin_ads,
        data_key="roles",
        roles=roles,
        kb_cpc_entry=kb_cpc.get("linkedin", {}),
        kb_cpa_data=kb_cpa,
        fit_score=PLATFORM_FIT["linkedin"].get(role_type, 0.75),
        budget=budget,
    )

    # --- Fallback: Industry benchmark data for major ad platforms ---
    # NOTE: Canonical benchmark source is trend_engine.py. These values are fallbacks only.
    # See trend_engine.get_benchmark() for authoritative CPC/CPA/CPM data.
    # Check if all platforms returned empty/zero data
    _all_empty = all(
        isinstance(result.get(pk), dict)
        and result[pk].get("avg_cpc")
        or 0 == 0
        and result[pk].get("avg_cpm")
        or 0 == 0
        and result[pk].get("avg_cpa")
        or 0 == 0
        for pk in ("google", "meta", "bing", "tiktok", "linkedin")
        if pk in result
    )
    if _all_empty:
        _PLATFORM_BENCHMARKS = {
            "Google Ads": {
                "cpc": 2.69,
                "cpm": 3.12,
                "cpa": 48.96,
                "audience_reach": "5.6B+ monthly searches",
                "daily_budget_range": "$50 - $500",
                "best_for": "Active job seekers, high intent",
            },
            "Meta (Facebook/Instagram)": {
                "cpc": 1.72,
                "cpm": 7.19,
                "cpa": 18.68,
                "audience_reach": "3.0B+ monthly active users",
                "daily_budget_range": "$20 - $300",
                "best_for": "Passive candidates, employer branding",
            },
            "LinkedIn Ads": {
                "cpc": 5.26,
                "cpm": 6.59,
                "cpa": 56.08,
                "audience_reach": "1.0B+ professionals",
                "daily_budget_range": "$50 - $1,000",
                "best_for": "Professional/white-collar roles, B2B",
            },
            "TikTok Ads": {
                "cpc": 1.00,
                "cpm": 10.00,
                "cpa": 20.00,
                "audience_reach": "1.5B+ monthly active users",
                "daily_budget_range": "$20 - $200",
                "best_for": "Gen-Z talent, hourly/retail roles",
            },
            "Microsoft/Bing Ads": {
                "cpc": 1.54,
                "cpm": 2.00,
                "cpa": 41.44,
                "audience_reach": "1.0B+ monthly searches",
                "daily_budget_range": "$30 - $300",
                "best_for": "Professional candidates, desktop users",
            },
            "Snapchat Ads": {
                "cpc": 1.30,
                "cpm": 2.95,
                "cpa": 22.00,
                "audience_reach": "750M+ monthly active users",
                "daily_budget_range": "$20 - $150",
                "best_for": "Young hourly workforce, retail/hospitality",
            },
            "X (Twitter) Ads": {
                "cpc": 1.35,
                "cpm": 6.46,
                "cpa": 28.00,
                "audience_reach": "500M+ monthly active users",
                "daily_budget_range": "$30 - $200",
                "best_for": "Tech talent, thought leadership",
            },
            "Programmatic Display (DSP)": {
                "cpc": 0.63,
                "cpm": 2.80,
                "cpa": 15.00,
                "audience_reach": "Billions of impressions across open web",
                "daily_budget_range": "$100 - $2,000",
                "best_for": "Scale hiring, retargeting, geo-targeting",
            },
            "Roku/CTV Advertising": {
                "cpc": 0.00,
                "cpm": 25.00,
                "cpa": 45.00,
                "audience_reach": "80M+ US households",
                "daily_budget_range": "$200 - $5,000",
                "best_for": "Employer branding, mass-market roles",
            },
            "Spotify Audio Ads": {
                "cpc": 0.00,
                "cpm": 15.00,
                "cpa": 35.00,
                "audience_reach": "600M+ monthly active users",
                "daily_budget_range": "$50 - $500",
                "best_for": "Brand awareness, commuter audience",
            },
            "Reddit Ads": {
                "cpc": 0.75,
                "cpm": 3.50,
                "cpa": 25.00,
                "audience_reach": "1.7B+ monthly active users",
                "daily_budget_range": "$20 - $200",
                "best_for": "Tech/engineering communities, niche targeting",
            },
            "Indeed Sponsored Jobs": {
                "cpc": 0.50,
                "cpm": 0.00,
                "cpa": 22.00,
                "audience_reach": "350M+ monthly unique visitors",
                "daily_budget_range": "$30 - $500",
                "best_for": "Direct applicants, all industries",
            },
            "ZipRecruiter Sponsored": {
                "cpc": 1.50,
                "cpm": 0.00,
                "cpa": 28.00,
                "audience_reach": "25M+ monthly active job seekers",
                "daily_budget_range": "$16 - $300",
                "best_for": "SMB hiring, quick fills",
            },
        }

        # Industry-specific platform fit scores
        _INDUSTRY_PLATFORM_FIT = {
            "tech_engineering": {
                "LinkedIn Ads": 9,
                "Google Ads": 8,
                "Reddit Ads": 7,
                "Meta (Facebook/Instagram)": 6,
                "X (Twitter) Ads": 7,
                "Programmatic Display (DSP)": 8,
            },
            "healthcare_medical": {
                "Indeed Sponsored Jobs": 9,
                "Google Ads": 8,
                "Meta (Facebook/Instagram)": 7,
                "LinkedIn Ads": 6,
                "Programmatic Display (DSP)": 8,
            },
            "retail_consumer": {
                "Meta (Facebook/Instagram)": 9,
                "TikTok Ads": 8,
                "Snapchat Ads": 7,
                "Google Ads": 7,
                "Programmatic Display (DSP)": 8,
            },
            "finance_banking": {
                "LinkedIn Ads": 9,
                "Google Ads": 8,
                "Microsoft/Bing Ads": 7,
                "Meta (Facebook/Instagram)": 6,
                "Programmatic Display (DSP)": 7,
            },
            "blue_collar_trades": {
                "Indeed Sponsored Jobs": 9,
                "Meta (Facebook/Instagram)": 8,
                "Google Ads": 7,
                "TikTok Ads": 6,
                "Programmatic Display (DSP)": 8,
            },
            "hospitality_travel": {
                "Indeed Sponsored Jobs": 9,
                "Meta (Facebook/Instagram)": 8,
                "TikTok Ads": 8,
                "Snapchat Ads": 7,
                "Programmatic Display (DSP)": 7,
            },
        }
        industry_fit = _INDUSTRY_PLATFORM_FIT.get(industry, {})

        # Replace result with comprehensive benchmark-based platform data
        result = {}
        for pname, pdata in _PLATFORM_BENCHMARKS.items():
            fit_score = industry_fit.get(pname, 5)  # default fit = 5
            roi_proj = round(10 - (pdata["cpa"] / 10), 1) if pdata["cpa"] > 0 else 5.0
            roi_proj = max(1.0, min(10.0, roi_proj))
            platform_key = (
                pname.lower()
                .replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
                .replace("/", "_")
            )
            result[platform_key] = {
                "platform_name": pname,
                "source": "Industry Benchmark (2024-2025)",
                "avg_cpc": pdata["cpc"],
                "avg_cpm": pdata["cpm"],
                "avg_cpa": pdata["cpa"],
                "total_monthly_searches": 0,
                "estimated_reach": 0,
                "audience_reach": pdata["audience_reach"],
                "fit_score": fit_score / 10.0,  # Normalize to 0-1 scale
                "roi_projection": roi_proj,
                "roi_projection_applications": (
                    round(budget / pdata["cpa"], 0)
                    if pdata["cpa"] > 0 and budget > 0
                    else 0.0
                ),
                "daily_budget_range": pdata["daily_budget_range"],
                "best_for": pdata["best_for"],
                "cpc_kb_validation": {},
                "recommended_daily_budget": {
                    "min": round(pdata["cpc"] * 10, 2) if pdata["cpc"] > 0 else 20.0,
                    "max": round(pdata["cpc"] * 50, 2) if pdata["cpc"] > 0 else 100.0,
                },
                "platform_summary": {},
                "_meta": {"source_count": 1, "kb_validated": False, "fallback": True},
            }

    # --- Platform ranking ---
    rankings: List[Tuple[str, float]] = []
    for platform_key, platform_data in result.items():
        if isinstance(platform_data, dict) and not platform_key.startswith("_"):
            composite = (
                (platform_data.get("fit_score") or 0) * 0.4
                + (1.0 - min(platform_data.get("avg_cpc", 5.0) / 10.0, 1.0)) * 0.3
                + min((platform_data.get("estimated_reach") or 0) / 1_000_000, 1.0)
                * 0.3
            )
            rankings.append((platform_key, round(composite, 3)))

    rankings.sort(key=lambda x: x[1], reverse=True)
    result["_platform_ranking"] = [
        {"platform": p, "composite_score": s} for p, s in rankings
    ]

    # --- Enrich from platform intelligence KB (91 platforms) ---
    _pi = kb.get("platform_intelligence", {}).get("platforms", {})
    if _pi:
        for _pk, _pv in result.items():
            if isinstance(_pv, dict):
                _deep = _pi.get(_pk, _pi.get(_pk.lower(), {}))
                if _deep:
                    _pv["deep_intelligence"] = {
                        "monthly_visitors": _deep.get("monthly_visitors"),
                        "candidate_demographics": _deep.get(
                            "candidate_demographics", {}
                        ),
                        "best_for": _deep.get("best_for") or [],
                        "programmatic_compatible": _deep.get("programmatic_compatible"),
                        "apply_rate": _deep.get("apply_rate"),
                        "mobile_traffic_pct": _deep.get("mobile_traffic_pct"),
                        "dei_features": _deep.get("dei_features") or [],
                        "ai_features": _deep.get("ai_features") or [],
                        "ats_integrations": _deep.get("ats_integrations") or [],
                        "pros": _deep.get("pros") or [],
                        "cons": _deep.get("cons") or [],
                    }

    # --- Enrich from supply ecosystem KB ---
    _se = _kb_supply_ecosystem(kb)
    if _se:
        result["_programmatic_insights"] = {
            "bidding_models": _se.get("bidding_models", {}),
            "publisher_waterfall": _se.get("publisher_waterfall", {}),
            "quality_signals": _se.get("key_concepts", {}).get("quality_signals", {}),
        }

    # --- Enrich from recruitment benchmarks KB (funnel rates) ---
    _funnel = _kb_funnel_benchmarks(kb)
    if _funnel:
        result["_funnel_benchmarks"] = _funnel

    # --- Enrich from employer branding KB ---
    _eb = _kb_employer_branding(kb)
    if _eb:
        _channel_eff = _eb.get("channel_effectiveness", {})
        if _channel_eff:
            result["_employer_branding_effectiveness"] = _channel_eff

    logger.info("Ad platform analysis fused for %d platforms", len(result) - 1)
    return result


def _classify_role_type(roles: List[str]) -> str:
    """Classify roles into broad categories for platform fit scoring."""
    if not roles:
        return "default"

    role_text = " ".join(r.lower() for r in roles)

    executive_kw = [
        "director",
        "vp",
        "vice president",
        "chief",
        "cto",
        "cfo",
        "ceo",
        "coo",
        "cmo",
        "head of",
        "svp",
        "evp",
    ]
    technical_kw = [
        "engineer",
        "developer",
        "programmer",
        "data scientist",
        "devops",
        "architect",
        "analyst",
        "scientist",
    ]
    hourly_kw = [
        "driver",
        "warehouse",
        "retail",
        "cashier",
        "barista",
        "cook",
        "server",
        "housekeeper",
        "janitor",
        "laborer",
        "assembler",
        "technician",
        "aide",
        "assistant",
        "clerk",
    ]

    if any(kw in role_text for kw in executive_kw):
        return "executive"
    if any(kw in role_text for kw in technical_kw):
        return "technical"
    if any(kw in role_text for kw in hourly_kw):
        return "hourly"
    return "professional"


def _build_platform_entry(
    platform_name: str,
    enriched_data: dict,
    data_key: str,
    roles: List[str],
    kb_cpc_entry: dict,
    kb_cpa_data: dict,
    fit_score: float,
    budget: float,
) -> Dict[str, Any]:
    """Build a unified platform entry from enriched data and KB benchmarks."""
    if not isinstance(enriched_data, dict):
        enriched_data = {}

    source = enriched_data.get("source", f"{platform_name} Benchmarks")
    platform_summary = enriched_data.get("platform_summary", {})
    role_data = enriched_data.get(data_key, {})
    if not isinstance(role_data, dict):
        role_data = {}

    # Aggregate metrics across roles
    cpc_values: List[float] = []
    cpm_values: List[float] = []
    cpa_values: List[float] = []
    search_volumes: List[int] = []
    audience_values: List[int] = []

    for role in roles:
        entry = role_data.get(role, {})
        if not isinstance(entry, dict):
            continue

        cpc = _safe_float(entry.get("avg_cpc_usd", entry.get("avg_cpc")))
        if cpc > 0:
            cpc_values.append(cpc)

        cpm = _safe_float(entry.get("avg_cpm_usd", entry.get("avg_cpm")))
        if cpm > 0:
            cpm_values.append(cpm)

        cpa = _safe_float(entry.get("cost_per_application", entry.get("cpa")))
        if cpa > 0:
            cpa_values.append(cpa)

        sv = _safe_int(entry.get("avg_monthly_searches", entry.get("search_volume")))
        if sv > 0:
            search_volumes.append(sv)

        audience = _parse_audience_number(str(entry.get("estimated_audience") or ""))
        if audience > 0:
            audience_values.append(audience)

    avg_cpc = round(statistics.mean(cpc_values), 2) if cpc_values else 0.0
    avg_cpm = round(statistics.mean(cpm_values), 2) if cpm_values else 0.0
    avg_cpa = round(statistics.mean(cpa_values), 2) if cpa_values else 0.0
    total_search = sum(search_volumes)
    estimated_reach = sum(audience_values) if audience_values else 0

    # KB validation for CPC
    kb_cpc_val = 0.0
    if isinstance(kb_cpc_entry, dict):
        kb_cpc_str = (
            kb_cpc_entry.get("average_cpc")
            or kb_cpc_entry.get("average_cpc_range")
            or ""
            or kb_cpc_entry.get("job_ad_cpc_range")
            or ""
        )
        low, high = _parse_salary_range(str(kb_cpc_str))
        if low and high:
            kb_cpc_val = (low + high) / 2
        elif low:
            kb_cpc_val = low

    cpc_validation = {}
    if avg_cpc > 0 and kb_cpc_val > 0:
        cpc_validation = validate_with_knowledge_base(
            avg_cpc, kb_cpc_val, tolerance=0.40
        )

    # ROI projection
    roi_projection = 0.0
    if avg_cpa > 0 and budget > 0:
        estimated_applications = budget / avg_cpa
        roi_projection = round(estimated_applications, 0)

    # Recommended daily budget
    daily_budget_min = round(avg_cpc * 10, 2) if avg_cpc > 0 else 20.0
    daily_budget_max = round(avg_cpc * 50, 2) if avg_cpc > 0 else 100.0

    source_count = 1 if (cpc_values or cpm_values or cpa_values) else 0
    kb_validated_flag = bool(cpc_validation.get("validated"))

    return {
        "platform_name": platform_name,
        "source": source,
        "avg_cpc": avg_cpc,
        "avg_cpm": avg_cpm,
        "avg_cpa": avg_cpa,
        "total_monthly_searches": total_search,
        "estimated_reach": estimated_reach,
        "fit_score": fit_score,
        "cpc_kb_validation": cpc_validation,
        "roi_projection_applications": roi_projection,
        "recommended_daily_budget": {
            "min": daily_budget_min,
            "max": daily_budget_max,
        },
        "platform_summary": (
            platform_summary if isinstance(platform_summary, dict) else {}
        ),
        "_meta": {
            "source_count": source_count + (1 if kb_validated_flag else 0),
            "kb_validated": kb_validated_flag,
        },
    }


def _build_meta_platform_entry(
    enriched_data: dict,
    roles: List[str],
    kb_cpc_entry: dict,
    fit_score: float,
    budget: float,
) -> Dict[str, Any]:
    """Build a unified entry for Meta (Facebook + Instagram)."""
    if not isinstance(enriched_data, dict):
        enriched_data = {}

    source = enriched_data.get("source", "Meta Ads Benchmarks")
    platform_summary = enriched_data.get("platform_summary", {})
    role_data = enriched_data.get("roles", {})
    if not isinstance(role_data, dict):
        role_data = {}

    fb_cpc: List[float] = []
    fb_cpm: List[float] = []
    fb_cpa: List[float] = []
    ig_cpc: List[float] = []
    ig_cpm: List[float] = []
    ig_cpa: List[float] = []
    audience_values: List[int] = []

    for role in roles:
        entry = role_data.get(role, {})
        if not isinstance(entry, dict):
            continue

        # Facebook metrics
        fb = entry.get("facebook", entry)
        if isinstance(fb, dict):
            v = _safe_float(fb.get("avg_cpc_usd"))
            if v > 0:
                fb_cpc.append(v)
            v = _safe_float(fb.get("avg_cpm_usd"))
            if v > 0:
                fb_cpm.append(v)
            v = _safe_float(fb.get("cost_per_application"))
            if v > 0:
                fb_cpa.append(v)
            a = _parse_audience_number(str(fb.get("estimated_audience_size") or ""))
            if a > 0:
                audience_values.append(a)

        # Instagram metrics
        ig = entry.get("instagram", {})
        if isinstance(ig, dict):
            v = _safe_float(ig.get("avg_cpc_usd"))
            if v > 0:
                ig_cpc.append(v)
            v = _safe_float(ig.get("avg_cpm_usd"))
            if v > 0:
                ig_cpm.append(v)
            v = _safe_float(ig.get("cost_per_application"))
            if v > 0:
                ig_cpa.append(v)

    all_cpc = fb_cpc + ig_cpc
    all_cpm = fb_cpm + ig_cpm
    all_cpa = fb_cpa + ig_cpa

    avg_cpc = round(statistics.mean(all_cpc), 2) if all_cpc else 0.0
    avg_cpm = round(statistics.mean(all_cpm), 2) if all_cpm else 0.0
    avg_cpa = round(statistics.mean(all_cpa), 2) if all_cpa else 0.0
    estimated_reach = sum(audience_values) if audience_values else 0

    # KB CPC validation
    kb_cpc_val = 0.0
    if isinstance(kb_cpc_entry, dict):
        kb_str = (
            kb_cpc_entry.get("median_cpc_jan_2026")
            or kb_cpc_entry.get("median_cpc_peak_nov_2025")
            or ""
        )
        kb_cpc_val = _safe_float(kb_str)

    cpc_validation = {}
    if avg_cpc > 0 and kb_cpc_val > 0:
        cpc_validation = validate_with_knowledge_base(
            avg_cpc, kb_cpc_val, tolerance=0.40
        )

    roi_projection = round(budget / avg_cpa, 0) if avg_cpa > 0 and budget > 0 else 0.0
    daily_min = round(avg_cpc * 10, 2) if avg_cpc > 0 else 15.0
    daily_max = round(avg_cpc * 50, 2) if avg_cpc > 0 else 75.0

    source_count = 1 if all_cpc else 0
    kb_validated_flag = bool(cpc_validation.get("validated"))

    return {
        "platform_name": "Meta (Facebook + Instagram)",
        "source": source,
        "avg_cpc": avg_cpc,
        "avg_cpm": avg_cpm,
        "avg_cpa": avg_cpa,
        "facebook_avg_cpc": round(statistics.mean(fb_cpc), 2) if fb_cpc else 0.0,
        "instagram_avg_cpc": round(statistics.mean(ig_cpc), 2) if ig_cpc else 0.0,
        "estimated_reach": estimated_reach,
        "fit_score": fit_score,
        "cpc_kb_validation": cpc_validation,
        "roi_projection_applications": roi_projection,
        "recommended_daily_budget": {"min": daily_min, "max": daily_max},
        "platform_summary": (
            platform_summary if isinstance(platform_summary, dict) else {}
        ),
        "_meta": {
            "source_count": source_count + (1 if kb_validated_flag else 0),
            "kb_validated": kb_validated_flag,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FUSE: COMPETITIVE INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════


def fuse_competitive_intelligence(
    enriched: dict, kb: dict, input_data: dict
) -> Dict[str, Any]:
    """Fuse company / market data from Wikipedia, Clearbit, SEC filings.

    Produces:
      - Company profile (size, industry, financials)
      - Competitor landscape
      - Industry hiring trends
      - Market positioning insights
    """
    company_name = (
        input_data.get("company_name") or "" or input_data.get("client_name") or ""
    )
    industry = input_data.get("industry") or ""

    # Source data
    wiki_data = enriched.get("company_info", enriched.get("wikipedia_data", {}))
    clearbit_data = enriched.get("company_metadata", enriched.get("clearbit_data", {}))
    sec_data = enriched.get("sec_data", {})
    competitor_logos = enriched.get("competitor_logos", {})
    industry_employment = enriched.get("industry_employment", {})

    # KB industry context
    kb_industry = _kb_industry_benchmarks(kb, industry)
    kb_market_trends = _get_nested(kb, "market_trends", default={})

    result: Dict[str, Any] = {}
    source_count = 0
    kb_validated = False

    # --- Company profile ---
    company_profile: Dict[str, Any] = {"name": company_name}

    if isinstance(wiki_data, dict) and wiki_data:
        company_profile["description"] = wiki_data.get("description") or ""
        company_profile["summary"] = wiki_data.get("summary") or ""[:500]
        company_profile["wikipedia_url"] = wiki_data.get("url") or ""
        if wiki_data.get("description") or wiki_data.get("summary"):
            source_count += 1

    if isinstance(clearbit_data, dict) and clearbit_data:
        company_profile["domain"] = clearbit_data.get("domain") or ""
        company_profile["logo_url"] = clearbit_data.get("logo") or ""
        company_profile["clearbit_category"] = clearbit_data.get("category", {})
        company_profile["clearbit_tags"] = clearbit_data.get("tags") or []
        if clearbit_data.get("domain"):
            source_count += 1

    if isinstance(sec_data, dict) and sec_data:
        company_profile["sec_ticker"] = sec_data.get("ticker") or ""
        company_profile["sec_cik"] = sec_data.get("cik") or ""
        company_profile["sec_sic"] = sec_data.get("sic") or ""
        company_profile["sec_sic_description"] = sec_data.get("sic_description") or ""
        company_profile["is_public"] = bool(sec_data.get("ticker"))
        filings = sec_data.get("recent_filings") or []
        if isinstance(filings, list):
            company_profile["recent_filings_count"] = len(filings)
        if sec_data.get("ticker"):
            source_count += 1

    result["company_profile"] = company_profile

    # --- Competitor landscape ---
    competitors_info: Dict[str, Any] = {}
    if isinstance(competitor_logos, dict):
        for comp_name, logo_data in competitor_logos.items():
            comp_entry: Dict[str, Any] = {"name": comp_name}
            if isinstance(logo_data, dict):
                comp_entry["logo_url"] = logo_data.get(
                    "logo", logo_data.get("url") or ""
                )
                comp_entry["domain"] = logo_data.get("domain") or ""
            elif isinstance(logo_data, str):
                comp_entry["logo_url"] = logo_data
            competitors_info[comp_name] = comp_entry

    result["competitors"] = competitors_info

    # --- Industry hiring trends ---
    hiring_trends: Dict[str, Any] = {}

    if isinstance(industry_employment, dict) and industry_employment:
        hiring_trends["employment_count"] = industry_employment.get("employment")
        hiring_trends["average_weekly_wage"] = industry_employment.get(
            "avg_weekly_wage"
        )
        hiring_trends["establishment_count"] = industry_employment.get("establishments")
        hiring_trends["source"] = industry_employment.get("source", "BLS QCEW")
        source_count += 1

    if kb_industry:
        hiring_trends["kb_context"] = kb_industry
        kb_validated = True

    # Market trends from KB
    if kb_market_trends:
        ai_trends = kb_market_trends.get("ai_in_recruiting", {})
        market_state = kb_market_trends.get("great_stay_low_hire_low_fire", {})
        hiring_trends["market_context"] = {
            "ai_adoption": _get_nested(
                ai_trends,
                "adoption_rates",
                "organizations_using_ai_2025",
                default="N/A",
            ),
            "market_state": market_state.get("title", "N/A"),
            "key_indicators": market_state.get("key_indicators", {}),
        }

    result["hiring_trends"] = hiring_trends

    # --- Market positioning ---
    result["market_positioning"] = {
        "industry_sector": industry,
        "is_public_company": company_profile.get("is_public", False),
        "competitor_count": len(competitors_info),
        "has_sec_filings": bool(company_profile.get("sec_ticker")),
    }

    result["_meta"] = {
        "source_count": source_count,
        "kb_validated": kb_validated,
    }

    # --- Enrich from company_info API (Clearbit, previously orphaned) ---
    _cinfo = enriched.get("company_info", {})
    if isinstance(_cinfo, dict) and _cinfo:
        result["company_clearbit"] = {
            "domain": _cinfo.get("domain"),
            "industry": _cinfo.get("industry"),
            "employee_count": _cinfo.get("metrics", {}).get("employees"),
            "annual_revenue": _cinfo.get("metrics", {}).get("annualRevenue"),
            "tech_stack": _cinfo.get("tech") or [],
            "tags": _cinfo.get("tags") or [],
        }

    # --- Enrich from company_metadata API (Wikipedia, previously orphaned) ---
    _cmeta = enriched.get("company_metadata", {})
    if isinstance(_cmeta, dict) and _cmeta:
        result["company_wikipedia"] = {
            "description": _cmeta.get("extract", _cmeta.get("description") or ""),
            "founded": _cmeta.get("founded"),
            "headquarters": _cmeta.get("headquarters"),
            "url": _cmeta.get("url"),
        }

    # --- Enrich from SEC data (previously orphaned) ---
    _sec = enriched.get("sec_company_data", enriched.get("sec_data", {}))
    if isinstance(_sec, dict) and _sec:
        result["company_sec"] = {
            "cik": _sec.get("cik"),
            "filings": _sec.get("recent_filings") or [][:5],
            "sic_code": _sec.get("sic"),
            "fiscal_year_end": _sec.get("fiscal_year_end"),
        }

    # --- Enrich from competitor logos (previously orphaned) ---
    _logos = enriched.get("competitor_logos", {})
    if isinstance(_logos, dict) and _logos:
        result["competitor_logos"] = _logos

    logger.info("Competitive intelligence fused (%d sources)", source_count)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY ASSESSMENT
# ═══════════════════════════════════════════════════════════════════════════════


def _assess_data_quality(enriched: dict) -> Dict[str, Any]:
    """Produce a data quality report from the enrichment summary."""
    summary = enriched.get("enrichment_summary", {})
    if not isinstance(summary, dict):
        return {
            "apis_called": 0,
            "apis_succeeded": 0,
            "apis_failed": 0,
            "success_rate": 0.0,
            "total_time_seconds": 0.0,
            "quality_tier": "none",
        }

    called = len(summary.get("apis_called") or [])
    succeeded = len(summary.get("apis_succeeded") or [])
    failed = len(summary.get("apis_failed") or [])
    skipped = len(summary.get("apis_skipped") or [])
    elapsed = summary.get("total_time_seconds") or 0

    success_rate = (succeeded / called * 100) if called > 0 else 0.0

    if success_rate >= 80:
        quality_tier = "excellent"
    elif success_rate >= 60:
        quality_tier = "good"
    elif success_rate >= 40:
        quality_tier = "fair"
    elif success_rate > 0:
        quality_tier = "limited"
    else:
        quality_tier = "none"

    return {
        "apis_called": called,
        "apis_succeeded": succeeded,
        "apis_failed": failed,
        "apis_skipped": skipped,
        "success_rate": round(success_rate, 1),
        "total_time_seconds": elapsed,
        "quality_tier": quality_tier,
        "succeeded_list": summary.get("apis_succeeded") or [],
        "failed_list": summary.get("apis_failed") or [],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FUSE: WORKFORCE INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════


def fuse_workforce_insights(enriched: dict, kb: dict, industry: str) -> dict:
    """Fuse workforce trends + employer branding intelligence.

    Sources:
    - workforce_trends_intelligence.json (Gen-Z, remote work, DEI)
    - recruitment_strategy_intelligence.json (employer branding ROI)
    - industry_white_papers.json (research findings)
    """
    result = {}

    # Gen-Z trends
    gen_z = _kb_workforce_trends(kb, "gen_z")
    if gen_z:
        result["gen_z_insights"] = {
            "workforce_share": gen_z.get("workforce_share"),
            "job_search_platforms": gen_z.get("job_search_behavior", {}).get(
                "platform_usage", {}
            ),
            "mobile_vs_desktop": gen_z.get("job_search_behavior", {}).get(
                "mobile_vs_desktop", {}
            ),
            "social_media_habits": gen_z.get("job_search_behavior", {}).get(
                "social_media_habits", {}
            ),
            "workplace_expectations": {
                "flexibility": gen_z.get("workplace_expectations", {}).get(
                    "flexibility", {}
                ),
                "mental_health": gen_z.get("workplace_expectations", {}).get(
                    "mental_health", {}
                ),
                "dei": gen_z.get("workplace_expectations", {}).get(
                    "dei_expectations", {}
                ),
                "purpose": gen_z.get("workplace_expectations", {}).get(
                    "purpose_driven_work", {}
                ),
            },
            "salary_expectations": gen_z.get("salary_expectations", {}),
            "tenure": gen_z.get("tenure_and_job_hopping", {}),
        }

    # Employer branding
    eb = _kb_employer_branding(kb)
    if eb:
        result["employer_branding"] = {
            "roi_data": eb.get("roi_data", {}),
            "best_practices": eb.get("best_practices", {}),
            "channel_effectiveness": eb.get("channel_effectiveness", {}),
        }

    # White paper highlights for this industry
    reports = _kb_white_papers(kb)
    if reports:
        relevant_reports = []
        industry_lower = industry.lower() if industry else ""
        for rkey, rval in reports.items():
            if isinstance(rval, dict):
                title = (rval.get("title") or "" or "").lower()
                findings = rval.get("key_findings") or []
                # Include if industry-relevant or general recruitment
                if (
                    industry_lower in title
                    or "recruit" in title
                    or "hiring" in title
                    or "benchmark" in title
                ):
                    relevant_reports.append(
                        {
                            "key": rkey,
                            "title": rval.get("title") or "",
                            "publisher": rval.get("publisher") or "",
                            "year": rval.get("year"),
                            "finding_count": len(findings),
                            "top_findings": (
                                findings[:3] if isinstance(findings, list) else []
                            ),
                        }
                    )
        result["relevant_research"] = relevant_reports[:10]  # Top 10 relevant reports

    # ── Appcast 2026 Benchmark Report: structured occupation-level data ──
    # Data priority: Priority 3 (KB benchmark data)
    # These are the richest industry benchmarks available (302M clicks, 27.4M applies)
    appcast_bm = _kb_appcast_2026_benchmarks(kb)
    if appcast_bm:
        appcast_occupation = _INDUSTRY_TO_APPCAST_OCCUPATION.get(industry) or ""
        occupation_benchmarks = {}

        # Extract industry-specific Appcast metrics
        cpa_data = appcast_bm.get("cpa_by_occupation_2025", {})
        cph_data = appcast_bm.get("cph_by_occupation_2025", {})
        apply_rate_data = appcast_bm.get("apply_rate_by_occupation_2025", {})
        cost_per_screen = appcast_bm.get("cost_per_screen_by_occupation_2025", {})
        cost_per_interview = appcast_bm.get("cost_per_interview_by_occupation_2025", {})
        cost_per_offer = appcast_bm.get("cost_per_offer_by_occupation_2025", {})

        if appcast_occupation:
            occupation_benchmarks["occupation_key"] = appcast_occupation
            if cpa_data.get(appcast_occupation):
                occupation_benchmarks["cpa"] = cpa_data[appcast_occupation]
            if cph_data.get(appcast_occupation):
                occupation_benchmarks["cph"] = cph_data[appcast_occupation]
            if apply_rate_data.get(appcast_occupation):
                occupation_benchmarks["apply_rate"] = apply_rate_data[
                    appcast_occupation
                ]
            if cost_per_screen.get(appcast_occupation):
                occupation_benchmarks["cost_per_screen"] = cost_per_screen[
                    appcast_occupation
                ]
            if cost_per_interview.get(appcast_occupation):
                occupation_benchmarks["cost_per_interview"] = cost_per_interview[
                    appcast_occupation
                ]
            if cost_per_offer.get(appcast_occupation):
                occupation_benchmarks["cost_per_offer"] = cost_per_offer[
                    appcast_occupation
                ]

        # Full funnel median costs (industry-wide)
        full_funnel = {}
        for funnel_key in (
            "overall_median_cpc",
            "overall_median_cpa",
            "overall_median_cps",
            "overall_median_cpi",
            "overall_median_cpo",
            "overall_median_cph",
        ):
            val = appcast_bm.get(funnel_key)
            if val:
                full_funnel[funnel_key] = val

        # International CPA data
        intl_cpa = appcast_bm.get("international_cpa_2025", {})

        # Job ad optimization insights
        job_ad_insights = appcast_bm.get("job_ad_insights_2025", {})

        result["appcast_2026_benchmarks"] = {
            "source": "Appcast 10th Annual Recruitment Marketing Benchmark Report (2026)",
            "data_year": 2025,
            "data_analyzed": appcast_bm.get(
                "data_analyzed", "302M clicks, 27.4M applies"
            ),
            "data_priority": 3,
            "occupation_benchmarks": occupation_benchmarks,
            "full_funnel_costs": full_funnel,
            "international_cpa": intl_cpa,
            "job_ad_insights": job_ad_insights,
            "overall_apply_rate": appcast_bm.get("overall_apply_rate"),
            "mobile_click_share": appcast_bm.get("mobile_click_share"),
            "mobile_apply_share": appcast_bm.get("mobile_apply_share"),
        }

    # ── Google Ads 2025 Benchmark Data (Joveo first-party) ──
    # Data priority: Priority 3 (KB benchmark data)
    gads_category = _INDUSTRY_TO_GOOGLE_ADS_CATEGORY.get(industry) or ""
    gads_kb = _kb_google_ads_benchmarks(kb, gads_category) if gads_category else {}
    if gads_kb:
        result["google_ads_2025_benchmarks"] = {
            "source": "Joveo Google Ads 2025 Campaign Data (first-party)",
            "data_priority": 3,
            "category": gads_kb.get("category_name", gads_category),
            "blended_cpc": gads_kb.get("blended_cpc"),
            "blended_ctr": gads_kb.get("blended_ctr"),
            "cpc_stats": gads_kb.get("cpc_stats", {}),
            "total_keywords": gads_kb.get("total_keywords"),
            "total_spend": gads_kb.get("total_spend"),
            "top_keywords": [
                {
                    "keyword": kw.get("keyword"),
                    "cpc": kw.get("cpc"),
                    "ctr_pct": kw.get("ctr_pct"),
                }
                for kw in (gads_kb.get("top_performing_keywords") or [] or [])[:5]
            ],
        }

    # Supply partner trends
    sp_trends = kb.get("workforce_trends", {}).get("supply_partner_trends", {})
    if sp_trends:
        result["supply_partner_trends"] = sp_trends

    # Job type trends
    jt_trends = kb.get("workforce_trends", {}).get("job_type_trends", {})
    if jt_trends:
        result["job_type_trends"] = jt_trends

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MASTER ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════


def synthesize(
    enriched: dict, knowledge_base: dict, input_data: dict
) -> Dict[str, Any]:
    """Master orchestrator -- calls all fuse functions and returns unified synthesis.

    Args:
        enriched: The dict returned by ``api_enrichment.enrich_data()``,
            containing results from up to 25 API sources.
        knowledge_base: Parsed recruitment industry knowledge base dict
            (from ``load_knowledge_base()``).
        input_data: Original request containing ``roles``, ``locations``,
            ``industry``, ``budget``, ``company_name``, etc.

    Returns:
        Dict with keys:
            ``salary_intelligence`` -- fused salary data per role
            ``job_market_demand``   -- demand signals per role + location
            ``location_profiles``   -- per-location economic profiles
            ``ad_platform_analysis`` -- per-platform cost / reach analysis
            ``competitive_intelligence`` -- company / market context
            ``confidence_scores``   -- per-section and overall scoring
            ``data_quality``        -- API success / failure report
    """
    if not isinstance(enriched, dict):
        enriched = {}
    if not isinstance(knowledge_base, dict):
        knowledge_base = {}
    if not isinstance(input_data, dict):
        input_data = {}

    logger.info(
        "Starting data synthesis (roles=%s, locations=%s, industry=%s)",
        len(input_data.get("roles") or []),
        len(input_data.get("locations") or []),
        input_data.get("industry", "N/A"),
    )

    # Run all fuse functions
    synthesis: Dict[str, Any] = {}

    try:
        synthesis["salary_intelligence"] = fuse_salary_intelligence(
            enriched, knowledge_base, input_data
        )
    except Exception as exc:
        logger.error("fuse_salary_intelligence failed: %s", exc, exc_info=True)
        synthesis["salary_intelligence"] = {}

    try:
        synthesis["job_market_demand"] = fuse_job_market_demand(
            enriched, knowledge_base, input_data
        )
    except Exception as exc:
        logger.error("fuse_job_market_demand failed: %s", exc, exc_info=True)
        synthesis["job_market_demand"] = {}

    try:
        synthesis["location_profiles"] = fuse_location_profiles(
            enriched, knowledge_base, input_data
        )
    except Exception as exc:
        logger.error("fuse_location_profiles failed: %s", exc, exc_info=True)
        synthesis["location_profiles"] = {}

    try:
        synthesis["ad_platform_analysis"] = fuse_ad_platform_analysis(
            enriched, knowledge_base, input_data
        )
    except Exception as exc:
        logger.error("fuse_ad_platform_analysis failed: %s", exc, exc_info=True)
        synthesis["ad_platform_analysis"] = {}

    try:
        synthesis["competitive_intelligence"] = fuse_competitive_intelligence(
            enriched, knowledge_base, input_data
        )
    except Exception as exc:
        logger.error("fuse_competitive_intelligence failed: %s", exc, exc_info=True)
        synthesis["competitive_intelligence"] = {}

    try:
        _industry = input_data.get("industry") or ""
        synthesis["workforce_insights"] = fuse_workforce_insights(
            enriched, knowledge_base, _industry
        )
    except Exception as exc:
        logger.error("fuse_workforce_insights failed: %s", exc, exc_info=True)
        synthesis["workforce_insights"] = {}

    # Compute confidence scores across all sections
    try:
        synthesis["confidence_scores"] = compute_confidence_scores(synthesis)
    except Exception as exc:
        logger.error("compute_confidence_scores failed: %s", exc, exc_info=True)
        synthesis["confidence_scores"] = {
            "per_section": {},
            "overall": 0.0,
            "data_quality_grade": "F",
        }

    # Data quality assessment from enrichment metadata
    synthesis["data_quality"] = _assess_data_quality(enriched)

    # AI-powered narrative synthesis (optional, requires ANTHROPIC_API_KEY)
    # Always sets the key (empty dict if skipped) for consistent API shape.
    try:
        synthesis["ai_narratives"] = generate_ai_narratives(synthesis, input_data) or {}
    except Exception as exc:
        logger.warning("AI narrative generation skipped: %s", exc)
        synthesis["ai_narratives"] = {}

    logger.info(
        "Synthesis complete -- overall confidence=%.2f, quality_tier=%s, ai_narratives=%s",
        synthesis["confidence_scores"].get("overall", 0.0),
        synthesis["data_quality"].get("quality_tier", "unknown"),
        bool(synthesis.get("ai_narratives")),
    )

    return synthesis


# ═══════════════════════════════════════════════════════════════════════════════
# AI-POWERED NARRATIVE SYNTHESIS
# ═══════════════════════════════════════════════════════════════════════════════


def generate_ai_narratives(
    synthesis: Dict[str, Any],
    input_data: Dict[str, Any],
) -> Dict[str, str]:
    """Generate AI-powered narrative sections via llm_router (free-first cascade).

    Takes the structured synthesis data and generates human-readable
    narratives for:
    1. Executive Summary - high-level overview of the market landscape
    2. Strategic Recommendations - actionable channel/budget advice
    3. Competitive Insights - market positioning analysis
    4. Risk Assessment - potential challenges and mitigation strategies

    Routes through llm_router.call_llm() so free providers (Gemini, Groq,
    Cerebras) are tried before paid providers (GPT-4o, Claude).  Falls back
    gracefully to an empty dict if all providers fail or no API keys are set.

    Args:
        synthesis: The structured synthesis dict from ``synthesize()``.
        input_data: Original request with roles, locations, industry, budget.

    Returns:
        Dict with narrative keys: executive_summary, strategic_recommendations,
        competitive_insights, risk_assessment. Empty dict on failure.
    """
    # Build a compact data summary (avoid sending entire synthesis)
    data_summary = _build_narrative_context(synthesis, input_data)
    if not data_summary:
        return {}

    prompt = f"""You are a senior recruitment marketing strategist. Based on the following synthesized market data, generate concise, actionable narratives for a media plan document.

## INPUT DATA
{data_summary}

## INSTRUCTIONS
Generate exactly 4 sections. Each section must be 2-4 sentences. Be specific with numbers from the data. Do not invent statistics -- only reference data points from the input above. If data is missing for a topic, say "data not available" rather than guessing.

Format your response as JSON with these exact keys:
{{
  "executive_summary": "2-4 sentence overview of the hiring market for these roles/locations",
  "strategic_recommendations": "2-4 sentence actionable channel/budget advice based on the data",
  "competitive_insights": "2-4 sentence analysis of market competitiveness and positioning",
  "risk_assessment": "2-4 sentence assessment of hiring challenges and mitigation strategies"
}}

Respond with ONLY the JSON object, no markdown formatting or code blocks."""

    try:
        from llm_router import call_llm, TASK_NARRATIVE
        import json as _json

        # Scale max_tokens for complex multi-city/multi-role plans
        _roles_raw = input_data.get("target_roles") or input_data.get("roles") or []
        _locs_raw = input_data.get("locations") or []
        _n_roles = len(_roles_raw) if isinstance(_roles_raw, list) else 1
        _n_locs = len(_locs_raw) if isinstance(_locs_raw, list) else 1
        _narrative_max_tokens = 8192 if (_n_roles >= 3 or _n_locs >= 3) else 1024

        result = call_llm(
            messages=[{"role": "user", "content": prompt}],
            system_prompt="You are a senior recruitment marketing strategist. Return ONLY valid JSON.",
            max_tokens=_narrative_max_tokens,
            task_type=TASK_NARRATIVE,
        )

        text = (result or {}).get("text") or ""
        provider = (result or {}).get("provider", "unknown")
        if text:
            logger.info(
                "AI narratives generated via %s/%s",
                provider,
                (result or {}).get("model", "unknown"),
            )

        if not text:
            return {}

        # Parse JSON from response (handle potential markdown wrapping)
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        narratives = _json.loads(text)

        # Validate expected keys
        expected_keys = {
            "executive_summary",
            "strategic_recommendations",
            "competitive_insights",
            "risk_assessment",
        }
        if not isinstance(narratives, dict):
            return {}
        # Only keep expected keys
        return {
            k: v
            for k, v in narratives.items()
            if k in expected_keys and isinstance(v, str)
        }

    except Exception as exc:
        logger.warning("AI narrative generation failed: %s", exc)
        return {}


def _build_narrative_context(
    synthesis: Dict[str, Any],
    input_data: Dict[str, Any],
) -> str:
    """Build a compact text summary of synthesis data for the AI narrator.

    Extracts the most important data points from each synthesis section
    and formats them as a readable text block that fits within token limits.
    """
    parts = []

    # Input parameters
    roles = input_data.get("roles", input_data.get("target_roles") or [])
    if isinstance(roles, list):
        role_names = [
            r.get("title", str(r)) if isinstance(r, dict) else str(r) for r in roles[:5]
        ]
        parts.append(f"Roles: {', '.join(role_names)}")

    locations = input_data.get("locations") or []
    if isinstance(locations, list):
        loc_names = []
        for loc in locations[:5]:
            if isinstance(loc, dict):
                city = loc.get("city") or ""
                state = loc.get("state") or ""
                country = loc.get("country") or ""
                loc_names.append(f"{city}, {state}, {country}".strip(", "))
            else:
                loc_names.append(str(loc))
        parts.append(f"Locations: {', '.join(loc_names)}")

    industry = input_data.get("industry") or ""
    if industry:
        parts.append(f"Industry: {industry}")

    budget = input_data.get("budget") or 0
    if budget:
        parts.append(f"Budget: ${budget:,.0f}")

    company = input_data.get("company_name") or ""
    if company:
        parts.append(f"Company: {company}")

    # Salary intelligence
    salary_data = synthesis.get("salary_intelligence", {})
    if salary_data:
        for role_key, sal in salary_data.items():
            if isinstance(sal, dict) and sal.get("median"):
                sources_str = ", ".join(sal.get("sources") or [][:3])
                parts.append(
                    f"Salary ({role_key}): median ${sal['median']:,}, "
                    f"range ${sal.get('p25') or 0:,}-${sal.get('p75') or 0:,}, "
                    f"sources: {sources_str}"
                )

    # Job market demand
    demand = synthesis.get("job_market_demand", {})
    if isinstance(demand, dict):
        for role_key, demand_data in demand.items():
            if isinstance(demand_data, dict):
                temp = demand_data.get("market_temperature") or ""
                competition = demand_data.get("competition_index") or ""
                posting_vol = demand_data.get("posting_volume") or ""
                if temp or competition:
                    parts.append(
                        f"Demand ({role_key}): temperature={temp}, "
                        f"competition={competition}, postings={posting_vol}"
                    )

    # Confidence scores
    confidence = synthesis.get("confidence_scores", {})
    if confidence:
        parts.append(
            f"Data quality: overall={confidence.get('overall') or 0:.2f}, "
            f"grade={confidence.get('data_quality_grade', 'N/A')}"
        )

    # Data quality
    dq = synthesis.get("data_quality", {})
    if dq:
        parts.append(
            f"APIs: {dq.get('apis_succeeded') or 0}/{dq.get('apis_called') or 0} succeeded, "
            f"quality_tier={dq.get('quality_tier', 'unknown')}"
        )

    # Ad platform analysis (top platforms)
    ad_platforms = synthesis.get("ad_platform_analysis", {})
    if isinstance(ad_platforms, dict):
        top_platforms = []
        for plat_key, plat_data in list(ad_platforms.items())[:5]:
            if isinstance(plat_data, dict) and plat_data.get("recommended_cpc"):
                top_platforms.append(
                    f"{plat_key} (CPC: {plat_data['recommended_cpc']})"
                )
        if top_platforms:
            parts.append(f"Top platforms: {', '.join(top_platforms)}")

    # S29: Include vector search results in narrative context
    _vs_results = (
        input_data.get("_knowledge_base", {}).get("_vector_search_results") or []
    )
    if _vs_results:
        _vs_texts = [
            r.get("text", "")[:200] for r in _vs_results[:5] if isinstance(r, dict)
        ]
        if _vs_texts:
            parts.append(
                "Relevant KB insights:\n" + "\n".join(f"- {t}" for t in _vs_texts)
            )

    # S29: Include supply repository data
    _supply = (
        input_data.get("_supply_repository")
        or input_data.get("_knowledge_base", {}).get("_supabase_supply_repository")
        or []
    )
    if _supply and isinstance(_supply, list):
        _pub_names = [
            p.get("name", "")
            for p in _supply[:10]
            if isinstance(p, dict) and p.get("name")
        ]
        if _pub_names:
            parts.append(f"Supply publishers: {', '.join(_pub_names)}")

    # S29: Include market trends
    _trends = input_data.get("_knowledge_base", {}).get("_supabase_market_trends") or []
    if _trends and isinstance(_trends, list):
        _trend_summaries = [
            t.get("title", t.get("category", ""))
            for t in _trends[:5]
            if isinstance(t, dict)
        ]
        if _trend_summaries:
            parts.append(f"Market trends: {', '.join(filter(None, _trend_summaries))}")

    # S29: Include compliance context
    _compliance = (
        input_data.get("_knowledge_base", {}).get("_supabase_compliance_rules") or []
    )
    if _compliance and isinstance(_compliance, list):
        _rule_names = [
            r.get("rule_name", r.get("title", ""))
            for r in _compliance[:5]
            if isinstance(r, dict)
        ]
        if _rule_names:
            parts.append(f"Compliance rules: {', '.join(filter(None, _rule_names))}")

    return "\n".join(parts) if parts else ""
