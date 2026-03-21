"""
market_intel_reports.py -- Comprehensive Recruitment Market Intelligence Reports

Generates data-rich market intelligence reports combining labor market data,
compensation benchmarks, channel performance metrics, CPC trends, seasonal
patterns, and actionable recommendations.

Outputs:
    - Structured Python dict (JSON-serializable)
    - Excel workbook (Sapphire Blue palette)
    - PowerPoint deck (Joveo branding)

All external module imports are lazy with fallback data so the module
degrades gracefully when dependencies are unavailable.
"""

from __future__ import annotations

import io
import logging
import math
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Lazy imports with fallback flags
# ─────────────────────────────────────────────────────────────────────────────

_trend_engine = None
_research = None
_api_enrichment = None
_collar_intelligence = None
_shared_utils = None

_INDUSTRY_LABEL_MAP: Dict[str, str] = {}


def _lazy_load():
    """Import optional modules once, caching results."""
    global _trend_engine, _research, _api_enrichment
    global _collar_intelligence, _shared_utils, _INDUSTRY_LABEL_MAP

    if _trend_engine is not None or _research is not None:
        return  # already attempted

    try:
        import trend_engine as _te
        _trend_engine = _te
    except Exception:
        _trend_engine = None

    try:
        import research as _r
        _research = _r
    except Exception:
        _research = None

    try:
        import api_enrichment as _ae
        _api_enrichment = _ae
    except Exception:
        _api_enrichment = None

    try:
        import collar_intelligence as _ci
        _collar_intelligence = _ci
    except Exception:
        _collar_intelligence = None

    try:
        import shared_utils as _su
        _shared_utils = _su
        _INDUSTRY_LABEL_MAP = getattr(_su, "INDUSTRY_LABEL_MAP", {})
    except Exception:
        _shared_utils = None


# ─────────────────────────────────────────────────────────────────────────────
# Fallback data (used when live modules are unavailable)
# ─────────────────────────────────────────────────────────────────────────────

_FALLBACK_INDUSTRY_LABELS: Dict[str, str] = {
    "healthcare_medical": "Healthcare & Medical",
    "blue_collar_trades": "Blue Collar / Skilled Trades",
    "tech_engineering": "Technology & Engineering",
    "general_entry_level": "General / Entry-Level",
    "finance_banking": "Finance & Banking",
    "retail_consumer": "Retail & Consumer",
    "logistics_supply_chain": "Logistics & Supply Chain",
    "hospitality_travel": "Hospitality & Travel",
    "construction_real_estate": "Construction & Real Estate",
    "education": "Education",
    "automotive": "Automotive & Manufacturing",
    "pharma_biotech": "Pharma & Biotech",
    "energy_utilities": "Energy & Utilities",
    "insurance": "Insurance",
    "food_beverage": "Food & Beverage",
    "aerospace_defense": "Aerospace & Defense",
    "legal_services": "Legal Services",
    "mental_health": "Mental Health & Behavioral",
    "media_entertainment": "Media & Entertainment",
    "telecommunications": "Telecommunications",
    "maritime_marine": "Maritime & Marine",
    "military_recruitment": "Military Recruitment",
}

_FALLBACK_CHANNEL_BENCHMARKS: Dict[str, Dict[str, float]] = {
    "google_search": {"cpc": 3.80, "cpa": 55.0, "ctr": 0.042},
    "indeed": {"cpc": 0.45, "cpa": 18.0, "ctr": 0.032},
    "linkedin": {"cpc": 5.50, "cpa": 85.0, "ctr": 0.028},
    "meta": {"cpc": 0.90, "cpa": 32.0, "ctr": 0.015},
    "programmatic": {"cpc": 0.65, "cpa": 22.0, "ctr": 0.038},
    "ziprecruiter": {"cpc": 0.55, "cpa": 20.0, "ctr": 0.035},
}

_FALLBACK_SEASONAL: Dict[int, float] = {
    1: 1.12, 2: 1.08, 3: 1.05, 4: 1.02, 5: 0.98, 6: 0.95,
    7: 0.92, 8: 0.94, 9: 1.06, 10: 1.03, 11: 0.96, 12: 0.89,
}

_FALLBACK_CPC_TREND: Dict[int, float] = {
    2022: 1.07, 2023: 0.85, 2024: 0.78, 2025: 0.82,
}

_FALLBACK_SALARY: Dict[str, Dict[str, int]] = {
    "general": {"p10": 35000, "p25": 45000, "median": 58000, "p75": 78000, "p90": 105000},
}

_FALLBACK_LABOR_MARKET: Dict[str, Any] = {
    "unemployment_rate": 3.9,
    "job_openings_rate": 5.5,
    "hires_rate": 3.6,
    "quits_rate": 2.3,
    "supply_demand_ratio": 0.7,
    "avg_time_to_fill_days": 44,
}


# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def _industry_label(industry_key: str) -> str:
    """Resolve an industry key to a human-readable label."""
    _lazy_load()
    label_map = _INDUSTRY_LABEL_MAP or _FALLBACK_INDUSTRY_LABELS
    return label_map.get(industry_key, industry_key.replace("_", " ").title())


def _detect_collar(role: str, industry: str) -> str:
    """Classify role into collar type."""
    _lazy_load()
    if _collar_intelligence:
        try:
            result = _collar_intelligence.classify_collar(role)
            return result.get("collar", "white_collar")
        except Exception:
            pass
    # Simple heuristic fallback
    blue_keywords = [
        "driver", "warehouse", "technician", "mechanic", "operator",
        "welder", "electrician", "plumber", "carpenter", "assembler",
        "laborer", "loader", "forklift", "janitor", "custodian",
        "maintenance", "hvac", "construction", "installer",
    ]
    role_lower = role.lower()
    for kw in blue_keywords:
        if kw in role_lower:
            return "blue_collar"
    if industry in ("blue_collar_trades", "construction_real_estate",
                    "logistics_supply_chain", "maritime_marine"):
        return "blue_collar"
    return "white_collar"


def _safe_get(func, *args, default=None, **kwargs):
    """Call func and return its result, or default on any error."""
    try:
        result = func(*args, **kwargs)
        return result if result is not None else default
    except Exception as exc:
        logger.debug("_safe_get caught %s: %s", type(exc).__name__, exc)
        return default


def _fmt_currency(value: float, decimals: int = 0) -> str:
    """Format a number as USD currency string."""
    if decimals == 0:
        return f"${value:,.0f}"
    return f"${value:,.{decimals}f}"


def _fmt_pct(value: float, decimals: int = 1) -> str:
    """Format a decimal as a percentage string."""
    return f"{value * 100:.{decimals}f}%" if value < 1 else f"{value:.{decimals}f}%"


def _pct_change(old: float, new: float) -> float:
    """Compute percentage change, safe for zero denominators."""
    if old == 0:
        return 0.0
    return (new - old) / abs(old)


def _trend_arrow(pct: float) -> str:
    """Return a text trend indicator."""
    if pct > 0.05:
        return "Rising"
    elif pct < -0.05:
        return "Declining"
    return "Stable"


def _current_year() -> int:
    return datetime.now().year


def _current_month() -> int:
    return datetime.now().month


# ─────────────────────────────────────────────────────────────────────────────
# Data collection functions
# ─────────────────────────────────────────────────────────────────────────────

def collect_labor_market_data(
    role: str,
    industry: str,
    location: str,
) -> Dict[str, Any]:
    """Collect labor market supply/demand and unemployment data.

    Uses research.py's get_labour_market_intelligence and
    api_enrichment.py's BLS/JOLTS functions where available.
    """
    _lazy_load()
    data: Dict[str, Any] = {
        "source": "fallback",
        "role": role,
        "industry": industry,
        "location": location,
    }

    # Try research module first
    if _research:
        lmi = _safe_get(
            _research.get_labour_market_intelligence,
            industry, [location],
            default={},
        )
        if lmi:
            data["source"] = "research"
            data["industry_metrics"] = lmi.get("industry_metrics", {})
            data["national_summary"] = lmi.get("national_summary", {})
            data["location_contexts"] = lmi.get("location_contexts", [])
            data["hiring_difficulty"] = lmi.get("hiring_difficulty", "")

    # Enrich with api_enrichment JOLTS/FRED if available
    if _api_enrichment:
        jolts = _safe_get(
            _api_enrichment.get_jolts_hiring_difficulty,
            industry,
            default={},
        )
        if jolts:
            data["jolts"] = jolts
            data["source"] = "api_enrichment+research" if data["source"] == "research" else "api_enrichment"

        tightness = _safe_get(
            _api_enrichment.get_labor_market_tightness,
            industry,
            default={},
        )
        if tightness:
            data["labor_market_tightness"] = tightness

    # Fallback
    if data["source"] == "fallback":
        data.update(_FALLBACK_LABOR_MARKET)

    # Always add a derived difficulty score (0-100)
    data["difficulty_score"] = _compute_difficulty_score(data)
    return data


def _compute_difficulty_score(labor_data: Dict[str, Any]) -> int:
    """Compute a 0-100 hiring difficulty score from labor market signals."""
    score = 50  # neutral baseline

    unemp = labor_data.get("unemployment_rate")
    if unemp is not None:
        if isinstance(unemp, str):
            try:
                unemp = float(unemp.replace("%", ""))
            except ValueError:
                unemp = None
    if unemp is not None:
        # Lower unemployment => harder to hire
        if unemp < 3.0:
            score += 20
        elif unemp < 4.0:
            score += 10
        elif unemp > 6.0:
            score -= 15

    ratio = labor_data.get("supply_demand_ratio")
    if ratio is not None:
        try:
            ratio = float(ratio)
        except (ValueError, TypeError):
            ratio = None
    if ratio is not None:
        if ratio < 0.5:
            score += 15
        elif ratio < 1.0:
            score += 5
        elif ratio > 2.0:
            score -= 10

    jolts = labor_data.get("jolts", {})
    if jolts.get("openings_per_unemployed", 0) > 1.5:
        score += 10

    return max(0, min(100, score))


def collect_compensation_data(
    role: str,
    industry: str,
    location: str,
) -> Dict[str, Any]:
    """Collect salary range data for the target role and location.

    Uses research.py's get_role_salary_range and api_enrichment's
    fetch_salary_data / fetch_careeronestop_data where available.
    """
    _lazy_load()
    data: Dict[str, Any] = {
        "source": "fallback",
        "role": role,
        "industry": industry,
        "location": location,
    }

    # Try research module
    if _research:
        salary_str = _safe_get(
            _research.get_role_salary_range, role, 100, default=""
        )
        if salary_str:
            data["salary_range_text"] = salary_str
            data["source"] = "research"

    # Try api_enrichment BLS salary
    if _api_enrichment:
        salary_data = _safe_get(
            _api_enrichment.fetch_salary_data, [role], default={}
        )
        if salary_data:
            data["bls_salary"] = salary_data
            data["source"] = "api_enrichment" if data["source"] == "fallback" else "research+api_enrichment"

        # CareerOneStop
        cos_data = _safe_get(
            _api_enrichment.fetch_careeronestop_data, [role], [location], default={}
        )
        if cos_data:
            data["careeronestop"] = cos_data

        # H1B benchmarks for tech/skilled roles
        h1b = _safe_get(
            _api_enrichment.fetch_h1b_wage_benchmarks, [role], default={}
        )
        if h1b:
            data["h1b_benchmarks"] = h1b

    # Build unified salary ranges
    data["salary_ranges"] = _build_salary_ranges(data)
    return data


def _build_salary_ranges(comp_data: Dict[str, Any]) -> Dict[str, Any]:
    """Unify salary data from multiple sources into a single range dict."""
    # Attempt to extract from BLS data
    bls = comp_data.get("bls_salary", {})
    role = comp_data.get("role", "")
    role_key = role.lower().replace(" ", "_")

    # BLS data is typically keyed by role name
    for key, val in bls.items():
        if isinstance(val, dict) and "median" in val:
            return {
                "p10": val.get("p10", val["median"] * 0.6),
                "p25": val.get("p25", val["median"] * 0.75),
                "median": val["median"],
                "p75": val.get("p75", val["median"] * 1.3),
                "p90": val.get("p90", val["median"] * 1.6),
                "source": "BLS OES",
            }

    # CareerOneStop extraction
    cos = comp_data.get("careeronestop", {})
    if cos:
        for key, val in cos.items():
            if isinstance(val, dict):
                salary_info = val.get("salary") or val.get("wages") or {}
                if isinstance(salary_info, dict) and salary_info.get("median"):
                    med = salary_info["median"]
                    return {
                        "p10": salary_info.get("p10", med * 0.6),
                        "p25": salary_info.get("p25", med * 0.75),
                        "median": med,
                        "p75": salary_info.get("p75", med * 1.3),
                        "p90": salary_info.get("p90", med * 1.6),
                        "source": "CareerOneStop",
                    }

    # Fallback
    fb = _FALLBACK_SALARY.get("general", {})
    return {**fb, "source": "estimate"}


def collect_channel_performance(
    industry: str,
    collar_type: str,
) -> Dict[str, Any]:
    """Collect CPC/CPA/CTR benchmarks across all advertising channels.

    Uses trend_engine's get_all_platform_benchmarks.
    """
    _lazy_load()
    data: Dict[str, Any] = {
        "source": "fallback",
        "industry": industry,
        "collar_type": collar_type,
        "channels": {},
    }

    if _trend_engine:
        benchmarks = _safe_get(
            _trend_engine.get_all_platform_benchmarks,
            industry, collar_type,
            default={},
        )
        if benchmarks:
            data["source"] = "trend_engine"
            for platform, metrics in benchmarks.items():
                data["channels"][platform] = {
                    "cpc": metrics.get("cpc", {}).get("value", 0),
                    "cpa": metrics.get("cpa", {}).get("value", 0) if "cpa" in metrics else None,
                    "ctr": metrics.get("ctr", {}).get("value", 0) if "ctr" in metrics else None,
                    "confidence": metrics.get("cpc", {}).get("confidence", "medium"),
                    "trend": metrics.get("cpc", {}).get("trend_direction", "stable"),
                }

    if not data["channels"]:
        data["source"] = "fallback"
        for platform, metrics in _FALLBACK_CHANNEL_BENCHMARKS.items():
            data["channels"][platform] = {
                "cpc": metrics["cpc"],
                "cpa": metrics["cpa"],
                "ctr": metrics.get("ctr"),
                "confidence": "low",
                "trend": "stable",
            }

    # Add rankings
    data["ranked_by_cpc"] = sorted(
        data["channels"].items(), key=lambda x: x[1].get("cpc", 999)
    )
    data["ranked_by_cpa"] = sorted(
        data["channels"].items(),
        key=lambda x: x[1].get("cpa") or 999,
    )
    return data


def collect_cpc_trends(
    industry: str,
) -> Dict[str, Any]:
    """Collect historical CPC trends across platforms.

    Uses trend_engine's get_trend for multi-year CPC data per platform.
    """
    _lazy_load()
    data: Dict[str, Any] = {
        "source": "fallback",
        "industry": industry,
        "platform_trends": {},
        "aggregate_trend": {},
    }

    platforms = ["google_search", "indeed", "linkedin", "meta", "programmatic"]

    if _trend_engine:
        any_success = False
        for platform in platforms:
            trend = _safe_get(
                _trend_engine.get_trend,
                platform, industry, "cpc", 4,
                default={},
            )
            if trend and trend.get("data"):
                any_success = True
                data["platform_trends"][platform] = {
                    "yearly_data": trend.get("data", {}),
                    "yoy_change": trend.get("yoy_change_pct", 0),
                    "trend_direction": trend.get("trend_direction", "stable"),
                }
        if any_success:
            data["source"] = "trend_engine"

    if not data["platform_trends"]:
        data["source"] = "fallback"
        for platform in platforms:
            base = _FALLBACK_CHANNEL_BENCHMARKS.get(platform, {}).get("cpc", 1.0)
            yearly = {}
            for yr, mult in _FALLBACK_CPC_TREND.items():
                yearly[yr] = round(base * mult, 2)
            data["platform_trends"][platform] = {
                "yearly_data": yearly,
                "yoy_change": -3.5,
                "trend_direction": "declining",
            }

    # Compute aggregate across platforms
    all_yoy = [
        v.get("yoy_change", 0) for v in data["platform_trends"].values()
        if v.get("yoy_change") is not None
    ]
    if all_yoy:
        avg_yoy = statistics.mean(all_yoy)
        data["aggregate_trend"] = {
            "avg_yoy_change": round(avg_yoy, 2),
            "direction": _trend_arrow(avg_yoy / 100),
        }

    return data


def collect_seasonal_patterns(
    industry: str,
) -> Dict[str, Any]:
    """Collect monthly hiring/CPC seasonal patterns.

    Uses trend_engine's get_seasonal_adjustment and
    research.py's get_seasonal_hiring_advice.
    """
    _lazy_load()
    data: Dict[str, Any] = {
        "source": "fallback",
        "industry": industry,
        "monthly_multipliers": {},
        "hiring_advice": {},
    }

    # Trend engine seasonal multipliers
    if _trend_engine:
        for month in range(1, 13):
            adj = _safe_get(
                _trend_engine.get_seasonal_adjustment,
                "mixed", month,
                default={},
            )
            if adj:
                data["monthly_multipliers"][month] = {
                    "multiplier": adj.get("multiplier", 1.0),
                    "label": adj.get("label", ""),
                }
                data["source"] = "trend_engine"

    # Research module seasonal advice
    if _research:
        advice = _safe_get(
            _research.get_seasonal_hiring_advice, industry, default={}
        )
        if advice:
            data["hiring_advice"] = advice
            if data["source"] == "fallback":
                data["source"] = "research"
            else:
                data["source"] += "+research"

    # Fallback
    if not data["monthly_multipliers"]:
        month_names = [
            "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ]
        for m in range(1, 13):
            data["monthly_multipliers"][m] = {
                "multiplier": _FALLBACK_SEASONAL[m],
                "label": month_names[m],
            }

    if not data["hiring_advice"]:
        data["hiring_advice"] = {
            "peak_months": ["Jan", "Sep"],
            "ramp_start": "Dec",
            "note": "Standard hiring follows Q1 budget releases and fall planning cycles.",
        }

    # Derive peak and trough months
    mults = data["monthly_multipliers"]
    if mults:
        sorted_months = sorted(mults.items(), key=lambda x: x[1].get("multiplier", 1.0))
        data["trough_months"] = [m for m, _ in sorted_months[:3]]
        data["peak_months"] = [m for m, _ in sorted_months[-3:]]

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Narrative generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_executive_summary(all_data: Dict[str, Any]) -> str:
    """Generate a narrative executive summary from collected intelligence data."""
    role = all_data.get("role", "the target role")
    industry_label = _industry_label(all_data.get("industry", ""))
    location = all_data.get("location", "")
    paras: List[str] = []

    paras.append(
        f"This market intelligence report analyzes the recruitment landscape for {role} "
        f"positions in the {industry_label} sector{f', focused on {location}' if location else ''}. "
        f"The analysis covers labor market dynamics, compensation benchmarks, channel performance, cost trends, and seasonal patterns."
    )

    labor = all_data.get("labor_market", {})
    d = labor.get("difficulty_score", 50)
    dt = "highly competitive" if d >= 70 else ("moderately competitive" if d >= 50 else "relatively accessible")
    extras = []
    if labor.get("unemployment_rate") is not None:
        extras.append(f"unemployment at {labor['unemployment_rate']}%")
    if labor.get("supply_demand_ratio") is not None:
        extras.append(f"supply-to-demand ratio of {labor['supply_demand_ratio']}")
    if labor.get("avg_time_to_fill_days") is not None:
        extras.append(f"avg time-to-fill of {labor['avg_time_to_fill_days']} days")
    paras.append(f"The labor market is {dt} (difficulty: {d}/100). " + (", ".join(extras) + "." if extras else ""))

    sal = all_data.get("compensation", {}).get("salary_ranges", {})
    if sal.get("median"):
        paras.append(
            f"Median salary: {_fmt_currency(sal['median'])} (25th-75th: {_fmt_currency(sal.get('p25',0))}"
            f" to {_fmt_currency(sal.get('p75',0))}). Source: {sal.get('source','estimate')}."
        )

    ranked = all_data.get("channel_performance", {}).get("ranked_by_cpc", [])
    if ranked:
        lo, hi = ranked[0], ranked[-1]
        paras.append(
            f"Lowest CPC: {lo[0].replace('_',' ').title()} at {_fmt_currency(lo[1]['cpc'],2)}. "
            f"Highest: {hi[0].replace('_',' ').title()} at {_fmt_currency(hi[1]['cpc'],2)}."
        )

    agg = all_data.get("cpc_trends", {}).get("aggregate_trend", {})
    if agg:
        paras.append(f"Aggregate CPC trend: {agg.get('direction','stable').lower()}, {agg.get('avg_yoy_change',0):+.1f}% YoY.")

    peak = all_data.get("seasonal_patterns", {}).get("hiring_advice", {}).get("peak_months", [])
    if peak:
        paras.append(f"Peak hiring months: {', '.join(str(m) for m in peak)}.")

    return "\n\n".join(paras)


# ─────────────────────────────────────────────────────────────────────────────
# Recommendations engine
# ─────────────────────────────────────────────────────────────────────────────

def generate_recommendations(all_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate actionable recommendations ranked by impact_score (1-10)."""
    recs: List[Dict[str, Any]] = []
    channels = all_data.get("channel_performance", {})
    labor = all_data.get("labor_market", {})
    comp = all_data.get("compensation", {})
    trends = all_data.get("cpc_trends", {})
    seasonal = all_data.get("seasonal_patterns", {})
    collar = all_data.get("collar_type", "white_collar")
    difficulty = labor.get("difficulty_score", 50)

    def _add(title, desc, impact, score, cat):
        recs.append({"title": title, "description": desc, "impact": impact, "impact_score": score, "category": cat})

    # Channel optimization
    ranked_cpc = channels.get("ranked_by_cpc", [])
    if ranked_cpc:
        c = ranked_cpc[0]
        _add(f"Prioritize {c[0].replace('_',' ').title()} for cost efficiency",
             f"At {_fmt_currency(c[1]['cpc'],2)} CPC, allocate 30-40% of budget here for maximum reach efficiency.",
             "high", 9, "channel")

    # Declining CPC opportunity
    for plat, info in trends.get("platform_trends", {}).items():
        if info.get("yoy_change", 0) < -5:
            _add(f"Increase {plat.replace('_',' ').title()} spend -- CPCs declining",
                 f"CPCs declined {abs(info['yoy_change']):.1f}% YoY, presenting a buying opportunity.", "high", 8, "channel")
            break

    # Seasonal timing
    trough = seasonal.get("trough_months", [])
    if trough:
        mn = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        _add("Launch campaigns during low-competition months",
             f"CPCs are lowest in {', '.join(mn.get(m,str(m)) for m in trough)}. Front-load budget to cut CPA 10-15%.",
             "medium", 7, "timing")

    # Compensation competitiveness
    sal = comp.get("salary_ranges", {})
    if difficulty >= 65 and sal.get("median"):
        _add("Highlight compensation to attract scarce talent",
             f"Difficulty {difficulty}/100. Feature salary (median: {_fmt_currency(sal['median'])}) in ads to boost apply rates 20-30%.",
             "high", 8, "compensation")

    # Collar-specific advice
    if collar == "blue_collar":
        _add("Use mobile-first application flows",
             "Blue-collar candidates apply 3x more via mobile. Use short-form, mobile-optimized pages with Indeed/programmatic.",
             "high", 8, "strategy")
    else:
        _add("Leverage LinkedIn and employer branding",
             "Invest in LinkedIn Sponsored Jobs and company page content alongside search-based recruitment ads.",
             "medium", 6, "strategy")

    # Diversification
    if ranked_cpc and len(ranked_cpc) >= 3:
        _add("Diversify across 3-4 channels", "Reduces risk from platform CPC spikes and broadens candidate reach.", "medium", 6, "budget")

    # Tight market
    if difficulty >= 75:
        _add("Target passive candidates",
             f"Difficulty {difficulty}/100. Use LinkedIn InMail, retargeting, and talent community nurture.", "high", 9, "strategy")

    recs.sort(key=lambda r: r.get("impact_score", 0), reverse=True)
    return recs


# ─────────────────────────────────────────────────────────────────────────────
# Excel report generation  (Sapphire Blue palette)
# ─────────────────────────────────────────────────────────────────────────────

_SAPPHIRE_DARK, _SAPPHIRE_BLUE, _SAPPHIRE_LIGHT = "0F172A", "2563EB", "DBEAFE"
_WHITE, _FONT_NAME = "FFFFFF", "Calibri"


def generate_intel_excel(report: Dict[str, Any]) -> bytes:
    """Generate Excel workbook. Sapphire palette, Calibri, data in col B."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    dark_fill = PatternFill("solid", fgColor=_SAPPHIRE_DARK)
    blue_fill = PatternFill("solid", fgColor=_SAPPHIRE_BLUE)
    light_fill = PatternFill("solid", fgColor=_SAPPHIRE_LIGHT)
    white_fill = PatternFill("solid", fgColor=_WHITE)
    hdr_font = Font(name=_FONT_NAME, size=11, bold=True, color=_WHITE)
    body_font = Font(name=_FONT_NAME, size=10, color="333333")
    thin_border = Border(bottom=Side(style="thin", color="CBD5E1"))
    c_align = Alignment(horizontal="center", vertical="center")
    l_align = Alignment(horizontal="left", vertical="center", wrap_text=True)

    def _hdr(ws, row, c1, c2):
        for c in range(c1, c2 + 1):
            cell = ws.cell(row=row, column=c)
            cell.fill, cell.font, cell.alignment = blue_fill, hdr_font, c_align

    def _drow(ws, row, c1, c2, alt=False):
        for c in range(c1, c2 + 1):
            cell = ws.cell(row=row, column=c)
            cell.fill = light_fill if alt else white_fill
            cell.font, cell.border, cell.alignment = body_font, thin_border, l_align

    def _sheet_title(ws, merge, title, col_range):
        ws.column_dimensions["A"].width = 3
        ws.sheet_properties.tabColor = _SAPPHIRE_BLUE
        ws.merge_cells(merge)
        ws.cell(row=2, column=2, value=title).font = Font(name=_FONT_NAME, size=14, bold=True, color=_WHITE)
        for c in col_range:
            ws.cell(row=2, column=c).fill = dark_fill

    role = report.get("role", "N/A")
    industry_label = _industry_label(report.get("industry", ""))
    location = report.get("location", "N/A")

    # Sheet 1: Overview
    ws = wb.active
    ws.title = "Overview"
    ws.column_dimensions["A"].width = 3
    ws.column_dimensions["B"].width = 30
    ws.column_dimensions["C"].width = 45
    ws.sheet_properties.tabColor = _SAPPHIRE_BLUE
    ws.merge_cells("B2:D2")
    t = ws.cell(row=2, column=2, value="Market Intelligence Report")
    t.font = Font(name=_FONT_NAME, size=16, bold=True, color=_WHITE)
    for c in range(2, 5):
        ws.cell(row=2, column=c).fill = dark_fill
    for i, (lbl, val) in enumerate([
        ("Role", role), ("Industry", industry_label), ("Location", location),
        ("Generated", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("Difficulty Score", f"{report.get('labor_market', {}).get('difficulty_score', 'N/A')}/100"),
    ]):
        ws.cell(row=4+i, column=2, value=lbl).font = Font(name=_FONT_NAME, size=11, bold=True, color=_SAPPHIRE_DARK)
        ws.cell(row=4+i, column=3, value=val).font = body_font
    summary = report.get("executive_summary", "")
    if summary:
        ws.merge_cells("B10:D10")
        ws.cell(row=10, column=2, value="Executive Summary").font = Font(name=_FONT_NAME, size=13, bold=True, color=_SAPPHIRE_DARK)
        ws.merge_cells("B11:D20")
        sc = ws.cell(row=11, column=2, value=summary)
        sc.font, sc.alignment = body_font, Alignment(wrap_text=True, vertical="top")

    # Sheet 2: Channel Performance
    ws2 = wb.create_sheet("Channel Performance")
    _sheet_title(ws2, "B2:G2", "Channel Performance Benchmarks", range(2, 8))
    for col, w in [("B", 22), ("C", 14), ("D", 14), ("E", 14), ("F", 16), ("G", 14)]:
        ws2.column_dimensions[col].width = w
    for i, h in enumerate(["Channel", "CPC", "CPA", "CTR", "Confidence", "Trend"]):
        ws2.cell(row=4, column=2+i, value=h)
    _hdr(ws2, 4, 2, 7)
    for idx, (name, m) in enumerate(report.get("channel_performance", {}).get("channels", {}).items()):
        r = 5 + idx
        vals = [name.replace("_", " ").title(), f"${m.get('cpc',0):.2f}",
                f"${m['cpa']:.2f}" if m.get("cpa") else "N/A",
                f"{m['ctr']*100:.1f}%" if m.get("ctr") else "N/A",
                m.get("confidence", "N/A"), m.get("trend", "N/A")]
        for i, v in enumerate(vals):
            ws2.cell(row=r, column=2+i, value=v)
        _drow(ws2, r, 2, 7, alt=(idx % 2 == 1))

    # Sheet 3: Compensation
    ws3 = wb.create_sheet("Compensation")
    _sheet_title(ws3, "B2:C2", "Salary Benchmarks", range(2, 4))
    ws3.column_dimensions["B"].width = 20
    ws3.column_dimensions["C"].width = 20
    for i, h in enumerate(["Percentile", "Annual Salary"]):
        ws3.cell(row=4, column=2+i, value=h)
    _hdr(ws3, 4, 2, 3)
    sal = report.get("compensation", {}).get("salary_ranges", {})
    for idx, (lbl, key) in enumerate([("10th", "p10"), ("25th", "p25"), ("Median", "median"), ("75th", "p75"), ("90th", "p90")]):
        r = 5 + idx
        v = sal.get(key, 0)
        ws3.cell(row=r, column=2, value=f"{lbl} Percentile")
        ws3.cell(row=r, column=3, value=_fmt_currency(v) if v else "N/A")
        _drow(ws3, r, 2, 3, alt=(idx % 2 == 1))

    # Sheet 4: CPC Trends
    ws4 = wb.create_sheet("CPC Trends")
    _sheet_title(ws4, "B2:G2", "Historical CPC Trends", range(2, 8))
    ws4.column_dimensions["B"].width = 22
    pt = report.get("cpc_trends", {}).get("platform_trends", {})
    all_years = sorted({yr for info in pt.values() for yr in info.get("yearly_data", {})})
    ws4.cell(row=4, column=2, value="Platform")
    for i, yr in enumerate(all_years):
        ws4.column_dimensions[get_column_letter(3+i)].width = 14
        ws4.cell(row=4, column=3+i, value=str(yr))
    lc = 3 + len(all_years)
    ws4.cell(row=4, column=lc, value="YoY %")
    _hdr(ws4, 4, 2, lc)
    for idx, (plat, info) in enumerate(pt.items()):
        r = 5 + idx
        ws4.cell(row=r, column=2, value=plat.replace("_", " ").title())
        yd = info.get("yearly_data", {})
        for i, yr in enumerate(all_years):
            v = yd.get(yr) or yd.get(str(yr))
            ws4.cell(row=r, column=3+i, value=f"${v:.2f}" if v else "N/A")
        ws4.cell(row=r, column=lc, value=f"{info.get('yoy_change',0):+.1f}%")
        _drow(ws4, r, 2, lc, alt=(idx % 2 == 1))

    # Sheet 5: Seasonal Patterns
    ws5 = wb.create_sheet("Seasonal Patterns")
    _sheet_title(ws5, "B2:D2", "Seasonal Hiring Patterns", range(2, 5))
    for col, w in [("B", 14), ("C", 18), ("D", 18)]:
        ws5.column_dimensions[col].width = w
    for i, h in enumerate(["Month", "CPC Multiplier", "Interpretation"]):
        ws5.cell(row=4, column=2+i, value=h)
    _hdr(ws5, 4, 2, 4)
    mn = ["","January","February","March","April","May","June","July","August","September","October","November","December"]
    mults = report.get("seasonal_patterns", {}).get("monthly_multipliers", {})
    for idx in range(12):
        r, m = 5 + idx, idx + 1
        info = mults.get(m, mults.get(str(m), {}))
        mult = info.get("multiplier", 1.0) if isinstance(info, dict) else 1.0
        interp = "Above average" if mult > 1.05 else ("Below average (opportunity)" if mult < 0.95 else "Average")
        ws5.cell(row=r, column=2, value=mn[m])
        ws5.cell(row=r, column=3, value=f"{mult:.2f}x")
        ws5.cell(row=r, column=4, value=interp)
        _drow(ws5, r, 2, 4, alt=(idx % 2 == 1))

    # Sheet 6: Recommendations
    ws6 = wb.create_sheet("Recommendations")
    _sheet_title(ws6, "B2:F2", "Recommendations", range(2, 7))
    for col, w in [("B", 8), ("C", 38), ("D", 60), ("E", 12), ("F", 14)]:
        ws6.column_dimensions[col].width = w
    for i, h in enumerate(["#", "Recommendation", "Details", "Impact", "Category"]):
        ws6.cell(row=4, column=2+i, value=h)
    _hdr(ws6, 4, 2, 6)
    for idx, rec in enumerate(report.get("recommendations", [])):
        r = 5 + idx
        for i, v in enumerate([idx+1, rec.get("title",""), rec.get("description",""), rec.get("impact","").upper(), rec.get("category","")]):
            ws6.cell(row=r, column=2+i, value=v)
        _drow(ws6, r, 2, 6, alt=(idx % 2 == 1))

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# PowerPoint report generation  (Joveo branding)
# ─────────────────────────────────────────────────────────────────────────────

def generate_intel_ppt(report: Dict[str, Any]) -> bytes:
    """Generate PowerPoint deck. Joveo branding: Port Gore, Blue Violet, Downy teal."""
    from pptx import Presentation  # type: ignore
    from pptx.util import Inches, Pt  # type: ignore
    from pptx.dml.color import RGBColor  # type: ignore
    from pptx.enum.text import PP_ALIGN  # type: ignore
    from pptx.enum.shapes import MSO_SHAPE  # type: ignore

    prs = Presentation()
    prs.slide_width, prs.slide_height = Inches(13.333), Inches(7.5)
    PG, BV, DT = RGBColor(0x20,0x20,0x58), RGBColor(0x5A,0x54,0xBD), RGBColor(0x6B,0xB3,0xCD)
    WH, LG, DK = RGBColor(0xFF,0xFF,0xFF), RGBColor(0xF0,0xF0,0xF8), RGBColor(0x1E,0x1E,0x2E)
    role, loc = report.get("role","N/A"), report.get("location","N/A")
    ind_label = _industry_label(report.get("industry",""))

    def _bg(sl, c):
        sl.background.fill.solid(); sl.background.fill.fore_color.rgb = c
    def _tb(sl, l, t, w, h, txt, sz=12, b=False, c=DK, a=PP_ALIGN.LEFT):
        tb = sl.shapes.add_textbox(Inches(l),Inches(t),Inches(w),Inches(h))
        tb.text_frame.word_wrap = True
        p = tb.text_frame.paragraphs[0]
        p.text, p.font.size, p.font.bold, p.font.color.rgb, p.alignment = txt, Pt(sz), b, c, a
        return tb
    def _rect(sl, l, t, w, h, fc):
        s = sl.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(l),Inches(t),Inches(w),Inches(h))
        s.fill.solid(); s.fill.fore_color.rgb = fc; s.line.fill.background()
        return s
    def _slide_hdr(sl, title):
        _bg(sl, WH); _rect(sl, 0,0,13.333,1.1, PG)
        _tb(sl, 0.8,0.2,11,0.7, title, sz=24, b=True, c=WH)

    # Slide 1: Title
    s1 = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(s1, PG); _rect(s1, 0,2.5,13.333,0.08, DT)
    _tb(s1, 1,1.5,11,1.2, "MARKET INTELLIGENCE REPORT", sz=32, b=True, c=WH, a=PP_ALIGN.CENTER)
    _tb(s1, 1,3.0,11,0.8, f"{role}  |  {ind_label}  |  {loc}", sz=18, c=DT, a=PP_ALIGN.CENTER)
    _tb(s1, 1,5.5,11,0.5, f"Generated {datetime.now().strftime('%B %d, %Y')}  |  Powered by Joveo", sz=12, c=WH, a=PP_ALIGN.CENTER)

    # Slide 2: Executive Summary
    s2 = prs.slides.add_slide(prs.slide_layouts[6])
    _slide_hdr(s2, "Executive Summary")
    summary = report.get("executive_summary", "No summary available.")
    _tb(s2, 0.8,1.5,11.5,5.0, summary[:1200], sz=13, c=DK)
    ds = report.get("labor_market",{}).get("difficulty_score",50)
    _rect(s2, 10.5,1.5,2.2,1.2, BV)
    _tb(s2, 10.6,1.55,2.0,0.4, "HIRING DIFFICULTY", sz=9, b=True, c=WH, a=PP_ALIGN.CENTER)
    _tb(s2, 10.6,1.95,2.0,0.7, f"{ds}/100", sz=28, b=True, c=WH, a=PP_ALIGN.CENTER)

    # Slide 3: Channel Performance (card layout)
    s3 = prs.slides.add_slide(prs.slide_layouts[6])
    _slide_hdr(s3, "Channel Performance Benchmarks")
    chs = report.get("channel_performance",{}).get("channels",{})
    for i,(nm,m) in enumerate(list(chs.items())[:6]):
        x = 0.8 + i * 2.05
        _rect(s3, x,1.5,1.85,4.5, LG)
        _tb(s3, x+0.1,1.6,1.65,0.5, nm.replace("_"," ").title(), sz=11, b=True, c=PG, a=PP_ALIGN.CENTER)
        _rect(s3, x+0.15,2.3,1.55,0.9, BV)
        _tb(s3, x+0.2,2.35,1.45,0.3, "CPC", sz=9, c=WH, a=PP_ALIGN.CENTER)
        _tb(s3, x+0.2,2.65,1.45,0.5, f"${m.get('cpc',0):.2f}", sz=20, b=True, c=WH, a=PP_ALIGN.CENTER)
        cpa = m.get("cpa")
        _tb(s3, x+0.1,3.5,1.65,0.3, "CPA", sz=9, b=True, c=BV, a=PP_ALIGN.CENTER)
        _tb(s3, x+0.1,3.8,1.65,0.4, f"${cpa:.2f}" if cpa else "N/A", sz=16, b=True, c=DK, a=PP_ALIGN.CENTER)
        _tb(s3, x+0.1,4.5,1.65,0.4, f"Trend: {m.get('trend','stable').title()}", sz=10, c=PG, a=PP_ALIGN.CENTER)

    # Slide 4: Compensation (bar chart)
    s4 = prs.slides.add_slide(prs.slide_layouts[6])
    _slide_hdr(s4, "Compensation Benchmarks")
    sal = report.get("compensation",{}).get("salary_ranges",{})
    pcts = [("10th",sal.get("p10",0)),("25th",sal.get("p25",0)),("Median",sal.get("median",0)),("75th",sal.get("p75",0)),("90th",sal.get("p90",0))]
    mx = max((v for _,v in pcts), default=1) or 1
    for i,(lbl,v) in enumerate(pcts):
        y = 2.0 + i*1.0
        _tb(s4, 1,y,2,0.5, lbl, sz=12, b=True, c=DK)
        bw = max(0.5, (v/mx)*8.0)
        _rect(s4, 3.2,y+0.05,bw,0.4, DT if lbl=="Median" else BV)
        _tb(s4, 3.3+bw,y,2,0.5, _fmt_currency(v) if v else "N/A", sz=12, b=True, c=DK)

    # Slide 5: CPC Trends
    s5 = prs.slides.add_slide(prs.slide_layouts[6])
    _slide_hdr(s5, "CPC Trend Analysis")
    pt = report.get("cpc_trends",{}).get("platform_trends",{})
    for i,(plat,info) in enumerate(list(pt.items())[:5]):
        y = 1.5 + i*1.0
        _tb(s5, 1,y,2.5,0.5, plat.replace("_"," ").title(), sz=11, b=True, c=DK)
        yd = info.get("yearly_data",{})
        _tb(s5, 3.8,y,7,0.5, "  |  ".join(f"{yr}: ${v:.2f}" for yr,v in sorted(yd.items()) if v), sz=10, c=DK)
        yoy = info.get("yoy_change",0)
        _tb(s5, 11.5,y,1.5,0.5, f"{yoy:+.1f}%", sz=12, b=True, c=DT if yoy<0 else RGBColor(0xDC,0x26,0x26))
    agg = report.get("cpc_trends",{}).get("aggregate_trend",{})
    if agg:
        _rect(s5, 1,6.5,11.3,0.6, LG)
        _tb(s5, 1.2,6.5,10,0.5, f"Aggregate: {agg.get('avg_yoy_change',0):+.1f}% YoY  |  Direction: {agg.get('direction','stable')}", sz=12, b=True, c=PG)

    # Slide 6: Seasonal Patterns (bar chart)
    s6 = prs.slides.add_slide(prs.slide_layouts[6])
    _slide_hdr(s6, "Seasonal Hiring Patterns")
    mults = report.get("seasonal_patterns",{}).get("monthly_multipliers",{})
    ma = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    mm = max((info.get("multiplier",1.0) if isinstance(info,dict) else 1.0) for info in mults.values()) if mults else 1.2
    for i in range(12):
        info = mults.get(i+1, mults.get(str(i+1), {}))
        mult = info.get("multiplier",1.0) if isinstance(info,dict) else 1.0
        x, bh = 0.8+i*1.02, max(0.2,(mult/mm)*3.5)
        clr = DT if mult<0.97 else (BV if mult>1.03 else RGBColor(0x94,0xA3,0xB8))
        _rect(s6, x,5.5-bh,0.85,bh, clr)
        _tb(s6, x,5.6,0.85,0.3, ma[i], sz=9, b=True, c=DK, a=PP_ALIGN.CENTER)
        _tb(s6, x,5.2-bh,0.85,0.3, f"{mult:.2f}x", sz=8, c=DK, a=PP_ALIGN.CENTER)
    note = report.get("seasonal_patterns",{}).get("hiring_advice",{}).get("note","")
    if note:
        _tb(s6, 0.8,6.3,11.5,0.8, note, sz=10, c=PG)

    # Slide 7: Recommendations
    s7 = prs.slides.add_slide(prs.slide_layouts[6])
    _slide_hdr(s7, "Key Recommendations")
    for i, rec in enumerate(report.get("recommendations",[])[:6]):
        y, imp = 1.4+i*0.95, rec.get("impact","medium")
        bc = BV if imp=="high" else (DT if imp=="medium" else RGBColor(0x94,0xA3,0xB8))
        _rect(s7, 0.8,y,0.15,0.7, bc)
        _tb(s7, 1.1,y,4,0.35, rec.get("title",""), sz=11, b=True, c=DK)
        _tb(s7, 1.1,y+0.35,10.5,0.35, rec.get("description",""), sz=9, c=RGBColor(0x64,0x74,0x8B))
        _tb(s7, 11.8,y+0.05,1,0.3, imp.upper(), sz=9, b=True, c=bc, a=PP_ALIGN.RIGHT)

    # Slide 8: Closing
    s8 = prs.slides.add_slide(prs.slide_layouts[6])
    _bg(s8, PG); _rect(s8, 0,3.5,13.333,0.06, DT)
    _tb(s8, 1,2.5,11,0.8, "Thank You", sz=36, b=True, c=WH, a=PP_ALIGN.CENTER)
    _tb(s8, 1,4.0,11,0.6, "Powered by Joveo  |  Intelligent Recruitment Advertising", sz=14, c=DT, a=PP_ALIGN.CENTER)

    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def generate_market_intel_report(
    role: str,
    industry: str,
    location: str,
    options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generate a comprehensive market intelligence report.

    Orchestrates parallel data collection via ThreadPoolExecutor, then
    generates narrative summary, recommendations, and optional Excel/PPT
    output files.

    Parameters
    ----------
    role : str
        Target job role (e.g., "Registered Nurse", "Software Engineer").
    industry : str
        Industry key (e.g., "healthcare_medical", "tech_engineering").
    location : str
        Target location (e.g., "New York, NY", "London, UK").
    options : dict, optional
        Control flags:
            - include_excel: bool (default True)
            - include_ppt: bool (default True)
            - max_workers: int (default 4)

    Returns
    -------
    dict
        Complete report with keys: metadata, labor_market, compensation,
        channel_performance, cpc_trends, seasonal_patterns,
        executive_summary, recommendations, excel_bytes, ppt_bytes.
    """
    _lazy_load()
    options = options or {}
    include_excel = options.get("include_excel", True)
    include_ppt = options.get("include_ppt", True)
    max_workers = options.get("max_workers", 4)

    collar_type = _detect_collar(role, industry)

    report: Dict[str, Any] = {
        "metadata": {
            "role": role,
            "industry": industry,
            "industry_label": _industry_label(industry),
            "location": location,
            "collar_type": collar_type,
            "generated_at": datetime.now().isoformat(),
            "data_sources": [],
        },
        "role": role,
        "industry": industry,
        "location": location,
        "collar_type": collar_type,
    }

    # Parallel data collection
    futures: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures["labor_market"] = executor.submit(
            collect_labor_market_data, role, industry, location
        )
        futures["compensation"] = executor.submit(
            collect_compensation_data, role, industry, location
        )
        futures["channel_performance"] = executor.submit(
            collect_channel_performance, industry, collar_type
        )
        futures["cpc_trends"] = executor.submit(
            collect_cpc_trends, industry
        )
        futures["seasonal_patterns"] = executor.submit(
            collect_seasonal_patterns, industry
        )

        for key, future in futures.items():
            try:
                report[key] = future.result(timeout=30)
                source = report[key].get("source", "unknown")
                report["metadata"]["data_sources"].append(
                    {"section": key, "source": source}
                )
            except Exception as exc:
                logger.error("Data collection failed for %s: %s", key, exc)
                report[key] = {"source": "error", "error": str(exc)}
                report["metadata"]["data_sources"].append(
                    {"section": key, "source": "error"}
                )

    # Generate narrative and recommendations (sequential, depends on data)
    report["executive_summary"] = generate_executive_summary(report)
    report["recommendations"] = generate_recommendations(report)

    # Generate output files
    if include_excel:
        try:
            report["excel_bytes"] = generate_intel_excel(report)
        except Exception as exc:
            logger.error("Excel generation failed: %s", exc)
            report["excel_bytes"] = None
            report["excel_error"] = str(exc)

    if include_ppt:
        try:
            report["ppt_bytes"] = generate_intel_ppt(report)
        except Exception as exc:
            logger.error("PPT generation failed: %s", exc)
            report["ppt_bytes"] = None
            report["ppt_error"] = str(exc)

    return report


# ─────────────────────────────────────────────────────────────────────────────
# CLI demo / quick test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)

    role = sys.argv[1] if len(sys.argv) > 1 else "Registered Nurse"
    industry = sys.argv[2] if len(sys.argv) > 2 else "healthcare_medical"
    location = sys.argv[3] if len(sys.argv) > 3 else "New York, NY"

    print(f"Generating market intel report for: {role} / {industry} / {location}")
    result = generate_market_intel_report(
        role, industry, location,
        options={"include_excel": False, "include_ppt": False},
    )

    # Print JSON-safe version (exclude bytes)
    output = {k: v for k, v in result.items() if not k.endswith("_bytes")}
    print(json.dumps(output, indent=2, default=str))
