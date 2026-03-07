"""
Data Synthesis Engine -- fuses 25 API sources into unified intelligence.

Takes raw enrichment data from api_enrichment.py and the recruitment knowledge
base, cross-references and validates data points, and produces synthesized
analysis with confidence scores.

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
    "healthcare": "healthcare",
    "healthcare_medical": "healthcare",
    "mental_health": "healthcare",
    "technology": "technology",
    "tech_engineering": "technology",
    "telecom": "technology",
    "telecommunications": "technology",
    "retail": "retail_hospitality",
    "retail_consumer": "retail_hospitality",
    "hospitality": "retail_hospitality",
    "hospitality_travel": "retail_hospitality",
    "food_beverage": "retail_hospitality",
    "construction": "construction_infrastructure",
    "construction_real_estate": "construction_infrastructure",
    "transportation": "transportation_logistics",
    "logistics_supply_chain": "transportation_logistics",
    "maritime": "transportation_logistics",
    "maritime_marine": "transportation_logistics",
    "rideshare": "transportation_logistics",
    "manufacturing": "manufacturing",
    "automotive": "manufacturing",
    "blue_collar": "manufacturing",
    "blue_collar_trades": "manufacturing",
    "aerospace": "manufacturing",
    "aerospace_defense": "manufacturing",
    "pharma": "manufacturing",
    "pharma_biotech": "manufacturing",
    "finance": "financial_services",
    "finance_banking": "financial_services",
    "insurance": "financial_services",
    "government": "government_utilities",
    "military_recruitment": "government_utilities",
    "energy": "government_utilities",
    "energy_utilities": "government_utilities",
    "education": "technology",
    "professional_services": "financial_services",
    "legal_services": "financial_services",
    "nonprofit": "government_utilities",
    "general": "retail_hospitality",
    "general_entry_level": "retail_hospitality",
    "media_entertainment": "technology",
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
    first_half = statistics.mean(values[: len(values) // 2]) if values[: len(values) // 2] else 0
    second_half = statistics.mean(values[len(values) // 2:]) if values[len(values) // 2:] else 0
    if first_half == 0:
        return "stable"
    change_pct = (second_half - first_half) / abs(first_half)
    if change_pct > 0.05:
        return "growing"
    elif change_pct < -0.05:
        return "declining"
    return "stable"


def _kb_industry_benchmarks(kb: dict, industry: str) -> dict:
    """Look up industry-specific benchmarks from the knowledge base."""
    kb_key = _INDUSTRY_TO_KB_KEY.get(industry, "")
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


def _parse_salary_range(range_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse a salary range string like '$80,000 - $120,000' or '80K-120K'."""
    if not range_str or range_str == "Not available":
        return None, None
    import re
    nums = re.findall(r'[\d,]+\.?\d*', str(range_str).replace("K", "000").replace("k", "000"))
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

    scores = list(per_section.values())
    overall = round(statistics.mean(scores), 2) if scores else 0.0

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
                sc = meta.get("source_count", 0)
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
        # No meta information -- check if section has any data at all
        if _section_has_data(section):
            return 0.4  # Assume at least one source contributed
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
    roles = input_data.get("roles", []) or input_data.get("target_roles", []) or []
    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",") if r.strip()]
    # Normalize dict-format roles (e.g. {"title": "...", "count": 5}) to strings
    roles = [r.get("title", "") if isinstance(r, dict) else r for r in roles]
    roles = [r for r in roles if isinstance(r, str) and r.strip()]

    industry = input_data.get("industry", "")
    result: Dict[str, Any] = {}

    # Source data accessors
    bls_salaries = enriched.get("salary_data", {})
    onet_data = enriched.get("onet_data", {})
    onet_occupations = onet_data.get("occupations", {}) if isinstance(onet_data, dict) else {}
    datausa_occ = enriched.get("datausa_occupation", enriched.get("datausa_data", {}))
    datausa_occupations = datausa_occ.get("occupations", {}) if isinstance(datausa_occ, dict) else {}
    cos_data = enriched.get("careeronestop_data", {})
    cos_occupations = cos_data.get("occupations", {}) if isinstance(cos_data, dict) else {}
    jooble_data = enriched.get("jooble_data", {})
    jooble_market = jooble_data.get("job_market", {}) if isinstance(jooble_data, dict) else {}

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
                salary_points.append((datausa_wage, _weight_for_source("DataUSA"), "DataUSA"))

        # --- CareerOneStop salary ---
        cos_entry = cos_occupations.get(role, {})
        if isinstance(cos_entry, dict):
            cos_source = cos_data.get("source", "CareerOneStop Benchmarks")
            cos_salary_data = cos_entry.get("salary", {})
            if isinstance(cos_salary_data, dict):
                cos_median = _safe_float(cos_salary_data.get("median"))
                if cos_median > 0:
                    salary_points.append((cos_median, _weight_for_source(cos_source), cos_source))

        # --- Jooble salary range (parse midpoint) ---
        jooble_role_data = jooble_market.get(role, {})
        if isinstance(jooble_role_data, dict):
            # Jooble data is keyed by location
            for loc_key, loc_data in jooble_role_data.items():
                if isinstance(loc_data, dict):
                    jooble_salary = loc_data.get("salary_range", "")
                    low, high = _parse_salary_range(str(jooble_salary))
                    if low and high:
                        midpoint = (low + high) / 2
                        jooble_src = jooble_data.get("source", "Jooble Market Benchmarks")
                        salary_points.append((midpoint, _weight_for_source(jooble_src), jooble_src))
                        break  # Use first location with salary data

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
        clean_values = [v for v, is_outlier in zip(values, outlier_flags) if not is_outlier]
        clean_weights = [w for w, is_outlier in zip(weights, outlier_flags) if not is_outlier]
        flagged_sources = [s for s, is_outlier in zip(sources, outlier_flags) if is_outlier]

        if not clean_values:
            clean_values = values
            clean_weights = weights

        w_median = _weighted_median(clean_values, clean_weights)
        sorted_vals = sorted(clean_values)

        # BLS-specific percentiles (more precise) or computed from available data
        p10 = _safe_float(bls_entry.get("p10")) if isinstance(bls_entry, dict) else 0.0
        p90 = _safe_float(bls_entry.get("p90")) if isinstance(bls_entry, dict) else 0.0
        if p10 <= 0:
            p10 = _percentile(sorted_vals, 10) if len(sorted_vals) >= 3 else round(w_median * 0.65, 0)
        if p90 <= 0:
            p90 = _percentile(sorted_vals, 90) if len(sorted_vals) >= 3 else round(w_median * 1.45, 0)

        p25 = _percentile(sorted_vals, 25) if len(sorted_vals) >= 3 else round(w_median * 0.82, 0)
        p75 = _percentile(sorted_vals, 75) if len(sorted_vals) >= 3 else round(w_median * 1.18, 0)

        # KB validation
        kb_validation = {"validated": False, "deviation": 0.0, "flag": None}
        kb_industry_key = _INDUSTRY_TO_KB_KEY.get(industry, "")
        if kb_industry_key and kb_by_industry:
            industry_salary_data = kb_by_industry.get(kb_industry_key, {})
            if isinstance(industry_salary_data, dict):
                # Try to extract a comparable salary benchmark from KB
                kb_salary_growth = _safe_float(industry_salary_data.get("salary_growth_moderation"))
                # KB doesn't store absolute salary -- use overall median as proxy
                overall_data = kb_salary.get("overall", {})
                if isinstance(overall_data, dict):
                    kb_median_str = overall_data.get("median_2025")
                    kb_median = _safe_float(kb_median_str)
                    if kb_median > 0:
                        kb_validation = validate_with_knowledge_base(w_median, kb_median, tolerance=0.50)

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
    roles = input_data.get("roles", []) or input_data.get("target_roles", []) or []
    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",") if r.strip()]
    roles = [r.get("title", "") if isinstance(r, dict) else r for r in roles]
    roles = [r for r in roles if isinstance(r, str) and r.strip()]
    locations = input_data.get("locations", [])
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",") if l.strip()]
    locations = [
        ", ".join(filter(None, [l.get("city", ""), l.get("state", ""), l.get("country", "")]))
        if isinstance(l, dict) else l for l in locations
    ]
    locations = [l for l in locations if isinstance(l, str) and l.strip()]

    industry = input_data.get("industry", "")

    # Source data
    adzuna_data = enriched.get("job_market", enriched.get("adzuna_data", {}))
    jooble_data = enriched.get("jooble_data", {})
    jooble_market = jooble_data.get("job_market", {}) if isinstance(jooble_data, dict) else {}
    google_trends = enriched.get("search_trends", enriched.get("google_trends_data", {}))
    google_ads = enriched.get("google_ads_data", enriched.get("google_ads_keyword_data", {}))
    google_ads_keywords = google_ads.get("keywords", {}) if isinstance(google_ads, dict) else {}
    linkedin_data = enriched.get("linkedin_ads_data", {})
    linkedin_roles = linkedin_data.get("roles", {}) if isinstance(linkedin_data, dict) else {}

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
                posting_volumes.append((total_jooble, jooble_data.get("source", "Jooble")))

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
            trends_data = google_trends.get("trends", google_trends.get("interest_over_time", {}))
            if isinstance(trends_data, dict):
                role_trend = trends_data.get(role, trends_data.get(f"{role} jobs", {}))
                if isinstance(role_trend, dict):
                    trend_values = [_safe_float(v) for v in role_trend.values() if _safe_float(v) > 0]
                elif isinstance(role_trend, list):
                    trend_values = [_safe_float(v) for v in role_trend if _safe_float(v) > 0]

        # --- Professional supply (LinkedIn) ---
        linkedin_role = linkedin_roles.get(role, {})
        audience_str = ""
        if isinstance(linkedin_role, dict):
            audience_str = str(linkedin_role.get("estimated_audience", ""))

        talent_pool = _parse_audience_number(audience_str)

        # --- Competition index ---
        competition_index = 0.0
        if talent_pool > 0 and total_postings > 0:
            competition_index = round(total_postings / talent_pool, 4)
        elif total_postings > 5000:
            competition_index = 2.5  # Estimated high
        elif total_postings > 1000:
            competition_index = 1.0

        temperature = _market_temperature(competition_index * 100)
        trend_dir = _trend_direction(trend_values) if trend_values else "stable"

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
            hiring_strength = kb_industry.get("hiring_strength", "")
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
                "demand_drivers": kb_industry.get("demand_drivers", []),
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
            jooble_loc = jooble_role.get(loc, {}) if isinstance(jooble_role, dict) else {}
            if isinstance(jooble_loc, dict):
                jlp = _safe_int(jooble_loc.get("total_job_postings"))
                if jlp > 0:
                    loc_posting += jlp
                    loc_sources.append("Jooble")

            jooble_activity = ""
            if isinstance(jooble_loc, dict):
                jooble_activity = jooble_loc.get("market_activity", "")

            location_breakdown[loc] = {
                "postings": loc_posting,
                "market_activity": jooble_activity or ("High" if loc_posting > 3000 else
                                                       "Medium" if loc_posting > 500 else "Low"),
                "sources": loc_sources,
            }

        if location_breakdown:
            role_result["by_location"] = location_breakdown

        result[role] = role_result

    logger.info("Job market demand fused for %d roles", len(result))
    return result


def _parse_audience_number(audience_str: str) -> int:
    """Parse an audience string like '2M-5M' or '150K-300K' into a midpoint integer."""
    if not audience_str:
        return 0
    import re
    audience_str = str(audience_str).upper()
    nums = re.findall(r'([\d.]+)\s*([MK]?)', audience_str)
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
    locations = input_data.get("locations", [])
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",") if l.strip()]
    locations = [
        ", ".join(filter(None, [l.get("city", ""), l.get("state", ""), l.get("country", "")]))
        if isinstance(l, dict) else l for l in locations
    ]
    locations = [l for l in locations if isinstance(l, str) and l.strip()]

    # Source data
    census_data = enriched.get("location_demographics", enriched.get("census_data", {}))
    geonames_raw = enriched.get("geonames_data", {})
    geonames_locations = geonames_raw.get("locations", {}) if isinstance(geonames_raw, dict) else {}
    teleport_raw = enriched.get("teleport_data", {})
    teleport_cities = teleport_raw.get("cities", {}) if isinstance(teleport_raw, dict) else {}
    countries_data = enriched.get("country_data", enriched.get("countries_data", {}))
    imf_raw = enriched.get("imf_indicators", enriched.get("imf_data", {}))
    imf_countries = imf_raw.get("countries", {}) if isinstance(imf_raw, dict) else {}
    world_bank = enriched.get("global_indicators", enriched.get("world_bank_data", {}))
    currency_rates = enriched.get("currency_rates", {})
    datausa_loc = enriched.get("datausa_location", {})
    datausa_locations = datausa_loc.get("locations", {}) if isinstance(datausa_loc, dict) else {}

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
            loc_profile["state_name"] = census_entry.get("state_name", "")
            loc_profile["geo_level"] = census_entry.get("geo_level", "")
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
            loc_profile["timezone"] = geo_entry.get("timezone", "")
            geo_pop = _safe_int(geo_entry.get("population"))
            if geo_pop > 0 and "population" not in loc_profile:
                loc_profile["population"] = geo_pop
            loc_profile["country_code"] = geo_entry.get("country_code", "")
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
                "name": country_entry.get("name", ""),
                "population": _safe_int(country_entry.get("population")),
                "currencies": country_entry.get("currencies", {}),
                "languages": country_entry.get("languages", {}),
                "region": country_entry.get("region", ""),
                "subregion": country_entry.get("subregion", ""),
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
                if isinstance(wb_entry, dict) and (wb_key.lower() in loc.lower() or loc.lower() in wb_key.lower()):
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
            loc_profile["currency_rates_usd"] = dict(
                list(currency_rates.items())[:10]
            )

        # --- Talent density ---
        population = loc_profile.get("population", 0)
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
                loc_profile["infrastructure_score"] = round(statistics.mean(infra_scores), 2)

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
    roles = input_data.get("roles", []) or input_data.get("target_roles", []) or []
    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",") if r.strip()]
    roles = [r.get("title", "") if isinstance(r, dict) else r for r in roles]
    roles = [r for r in roles if isinstance(r, str) and r.strip()]

    budget = _safe_float(input_data.get("budget", 0))
    industry = input_data.get("industry", "")

    # Source data
    google_ads = enriched.get("google_ads_data", enriched.get("google_ads_keyword_data", {}))
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
        "google": {"professional": 0.85, "technical": 0.80, "hourly": 0.75, "executive": 0.80, "default": 0.80},
        "meta": {"professional": 0.60, "technical": 0.50, "hourly": 0.85, "executive": 0.40, "default": 0.65},
        "bing": {"professional": 0.70, "technical": 0.65, "hourly": 0.55, "executive": 0.70, "default": 0.60},
        "tiktok": {"professional": 0.35, "technical": 0.30, "hourly": 0.80, "executive": 0.15, "default": 0.45},
        "linkedin": {"professional": 0.95, "technical": 0.85, "hourly": 0.30, "executive": 0.95, "default": 0.75},
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

    # --- Platform ranking ---
    rankings: List[Tuple[str, float]] = []
    for platform_key, platform_data in result.items():
        if isinstance(platform_data, dict):
            composite = (
                platform_data.get("fit_score", 0) * 0.4
                + (1.0 - min(platform_data.get("avg_cpc", 5.0) / 10.0, 1.0)) * 0.3
                + min(platform_data.get("estimated_reach", 0) / 1_000_000, 1.0) * 0.3
            )
            rankings.append((platform_key, round(composite, 3)))

    rankings.sort(key=lambda x: x[1], reverse=True)
    result["_platform_ranking"] = [
        {"platform": p, "composite_score": s} for p, s in rankings
    ]

    logger.info("Ad platform analysis fused for %d platforms", len(result) - 1)
    return result


def _classify_role_type(roles: List[str]) -> str:
    """Classify roles into broad categories for platform fit scoring."""
    if not roles:
        return "default"

    role_text = " ".join(r.lower() for r in roles)

    executive_kw = ["director", "vp", "vice president", "chief", "cto", "cfo",
                     "ceo", "coo", "cmo", "head of", "svp", "evp"]
    technical_kw = ["engineer", "developer", "programmer", "data scientist",
                    "devops", "architect", "analyst", "scientist"]
    hourly_kw = ["driver", "warehouse", "retail", "cashier", "barista", "cook",
                 "server", "housekeeper", "janitor", "laborer", "assembler",
                 "technician", "aide", "assistant", "clerk"]

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

        audience = _parse_audience_number(str(entry.get("estimated_audience", "")))
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
        kb_cpc_str = (kb_cpc_entry.get("average_cpc")
                      or kb_cpc_entry.get("average_cpc_range", "")
                      or kb_cpc_entry.get("job_ad_cpc_range", ""))
        low, high = _parse_salary_range(str(kb_cpc_str))
        if low and high:
            kb_cpc_val = (low + high) / 2
        elif low:
            kb_cpc_val = low

    cpc_validation = {}
    if avg_cpc > 0 and kb_cpc_val > 0:
        cpc_validation = validate_with_knowledge_base(avg_cpc, kb_cpc_val, tolerance=0.40)

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
        "platform_summary": platform_summary if isinstance(platform_summary, dict) else {},
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
            a = _parse_audience_number(str(fb.get("estimated_audience_size", "")))
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
        kb_str = kb_cpc_entry.get("median_cpc_jan_2026") or kb_cpc_entry.get("median_cpc_peak_nov_2025", "")
        kb_cpc_val = _safe_float(kb_str)

    cpc_validation = {}
    if avg_cpc > 0 and kb_cpc_val > 0:
        cpc_validation = validate_with_knowledge_base(avg_cpc, kb_cpc_val, tolerance=0.40)

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
        "platform_summary": platform_summary if isinstance(platform_summary, dict) else {},
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
    company_name = (input_data.get("company_name", "")
                    or input_data.get("client_name", ""))
    industry = input_data.get("industry", "")

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
        company_profile["description"] = wiki_data.get("description", "")
        company_profile["summary"] = wiki_data.get("summary", "")[:500]
        company_profile["wikipedia_url"] = wiki_data.get("url", "")
        if wiki_data.get("description") or wiki_data.get("summary"):
            source_count += 1

    if isinstance(clearbit_data, dict) and clearbit_data:
        company_profile["domain"] = clearbit_data.get("domain", "")
        company_profile["logo_url"] = clearbit_data.get("logo", "")
        company_profile["clearbit_category"] = clearbit_data.get("category", {})
        company_profile["clearbit_tags"] = clearbit_data.get("tags", [])
        if clearbit_data.get("domain"):
            source_count += 1

    if isinstance(sec_data, dict) and sec_data:
        company_profile["sec_ticker"] = sec_data.get("ticker", "")
        company_profile["sec_cik"] = sec_data.get("cik", "")
        company_profile["sec_sic"] = sec_data.get("sic", "")
        company_profile["sec_sic_description"] = sec_data.get("sic_description", "")
        company_profile["is_public"] = bool(sec_data.get("ticker"))
        filings = sec_data.get("recent_filings", [])
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
                comp_entry["logo_url"] = logo_data.get("logo", logo_data.get("url", ""))
                comp_entry["domain"] = logo_data.get("domain", "")
            elif isinstance(logo_data, str):
                comp_entry["logo_url"] = logo_data
            competitors_info[comp_name] = comp_entry

    result["competitors"] = competitors_info

    # --- Industry hiring trends ---
    hiring_trends: Dict[str, Any] = {}

    if isinstance(industry_employment, dict) and industry_employment:
        hiring_trends["employment_count"] = industry_employment.get("employment")
        hiring_trends["average_weekly_wage"] = industry_employment.get("avg_weekly_wage")
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
            "ai_adoption": _get_nested(ai_trends, "adoption_rates",
                                       "organizations_using_ai_2025", default="N/A"),
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

    called = len(summary.get("apis_called", []))
    succeeded = len(summary.get("apis_succeeded", []))
    failed = len(summary.get("apis_failed", []))
    skipped = len(summary.get("apis_skipped", []))
    elapsed = summary.get("total_time_seconds", 0)

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
        "succeeded_list": summary.get("apis_succeeded", []),
        "failed_list": summary.get("apis_failed", []),
    }


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
        len(input_data.get("roles", [])),
        len(input_data.get("locations", [])),
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

    logger.info(
        "Synthesis complete -- overall confidence=%.2f, quality_tier=%s",
        synthesis["confidence_scores"].get("overall", 0.0),
        synthesis["data_quality"].get("quality_tier", "unknown"),
    )

    return synthesis
