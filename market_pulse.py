"""
market_pulse.py -- Automated Weekly Market Pulse Report System

Generates weekly hiring market intelligence reports with CPC trends,
industry spotlights, platform comparisons, seasonal insights, and
market demand data. Reports are delivered via email (Resend) and
viewable on a web page.

Data sources (all internal, zero required external calls):
    - trend_engine.py: CPC/CPA/CPM benchmarks across 6 platforms x 22 industries
    - trend_engine.py: SEASONAL_MULTIPLIERS for seasonal analysis
    - research.py: COUNTRY_DATA, METRO_DATA, salary ranges
    - api_enrichment.py: Optional live JOLTS/FRED economic data
    - collar_intelligence.py: Collar-type classification

Thread-safe, stdlib-only (no pip dependencies).
"""

from __future__ import annotations

import html
import json
import logging
import os
import threading
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

_RESEND_ENDPOINT = "https://api.resend.com/emails"
_SEND_TIMEOUT = 15

# Base URL for "View full report" links in emails
_BASE_URL = os.environ.get(
    "PULSE_BASE_URL",
    "https://media-plan-generator.onrender.com",
).rstrip("/")

# ═══════════════════════════════════════════════════════════════════════════════
# PLATFORM & INDUSTRY DISPLAY LABELS
# ═══════════════════════════════════════════════════════════════════════════════

PLATFORM_LABELS: Dict[str, str] = {
    "google_search": "Google Search",
    "meta_facebook": "Meta / Facebook",
    "meta_instagram": "Meta / Instagram",
    "linkedin": "LinkedIn",
    "indeed": "Indeed",
    "programmatic": "Programmatic",
}

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE REPORT STORAGE
# ═══════════════════════════════════════════════════════════════════════════════

_lock = threading.Lock()
_report_history: List[Dict[str, Any]] = []  # Most recent first
_MAX_HISTORY = 52  # Keep ~1 year of weekly reports

# Scheduler state
_scheduler_lock = threading.Lock()
_scheduler_timer: Optional[threading.Timer] = None
_scheduler_running = False
_scheduler_last_run: Optional[str] = None
_scheduler_next_run: Optional[str] = None
_scheduler_last_summary: Optional[str] = None
_scheduler_interval_hours = 168
_scheduler_recipients: List[str] = []


# ═══════════════════════════════════════════════════════════════════════════════
# LAZY IMPORTS -- avoid circular dependencies, load only when needed
# ═══════════════════════════════════════════════════════════════════════════════


def _lazy_trend_engine():
    """Lazy import trend_engine to avoid circular imports."""
    try:
        import trend_engine

        return trend_engine
    except ImportError:
        logger.warning("market_pulse: trend_engine not available")
        return None


def _lazy_research():
    """Lazy import research module."""
    try:
        import research

        return research
    except ImportError:
        logger.warning("market_pulse: research module not available")
        return None


def _lazy_api_enrichment():
    """Lazy import api_enrichment for optional live data."""
    try:
        import api_enrichment

        return api_enrichment
    except ImportError:
        logger.warning("market_pulse: api_enrichment not available")
        return None


def _lazy_shared_utils():
    """Lazy import shared_utils."""
    try:
        import shared_utils

        return shared_utils
    except ImportError:
        return None


def _lazy_collar_intelligence():
    """Lazy import collar_intelligence."""
    try:
        import collar_intelligence

        return collar_intelligence
    except ImportError:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _safe(value: Any) -> str:
    """HTML-escape any value."""
    if value is None:
        return ""
    return html.escape(str(value))


def _fmt_currency(value: Any) -> str:
    """Format a numeric value as currency."""
    try:
        num = float(value)
        if num >= 1_000_000:
            return f"${num / 1_000_000:,.1f}M"
        if num >= 1_000:
            return f"${num:,.0f}"
        return f"${num:,.2f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_pct(value: Any, decimals: int = 1) -> str:
    """Format as percentage with sign."""
    try:
        num = float(value)
        sign = "+" if num > 0 else ""
        return f"{sign}{num:.{decimals}f}%"
    except (TypeError, ValueError):
        return str(value)


def _trend_arrow(pct_change: float) -> str:
    """Return a unicode trend arrow based on direction."""
    if pct_change > 2.0:
        return "\u2191"  # up arrow
    elif pct_change < -2.0:
        return "\u2193"  # down arrow
    else:
        return "\u2192"  # right arrow (flat)


def _trend_word(pct_change: float) -> str:
    """Return a human-readable trend description."""
    if pct_change > 5.0:
        return "Rising sharply"
    elif pct_change > 2.0:
        return "Rising"
    elif pct_change > -2.0:
        return "Stable"
    elif pct_change > -5.0:
        return "Declining"
    else:
        return "Declining sharply"


def _get_current_date() -> datetime:
    """Return current date in UTC."""
    return datetime.now(timezone.utc)


def _industry_label(key: str) -> str:
    """Convert industry key to display label."""
    utils = _lazy_shared_utils()
    if utils and hasattr(utils, "INDUSTRY_LABEL_MAP"):
        return utils.INDUSTRY_LABEL_MAP.get(key, key.replace("_", " ").title())
    return key.replace("_", " ").title()


# ═══════════════════════════════════════════════════════════════════════════════
# 1. DATA COLLECTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def collect_cpc_trends() -> Dict[str, Any]:
    """Pull CPC/CPA trends across all 6 platforms x 22 industries.

    Compares current year vs last year, calculates trend direction
    and percentage change. Returns structured data for report rendering.
    """
    te = _lazy_trend_engine()
    if not te:
        return {"available": False, "error": "trend_engine not loaded"}

    now = _get_current_date()
    current_year = min(now.year, 2025)
    prior_year = current_year - 1
    current_month = now.month

    platform_summaries = {}
    industry_cpc_changes = {}

    for plat in te.PLATFORMS:
        plat_label = PLATFORM_LABELS.get(plat, plat)
        plat_data = {
            "label": plat_label,
            "industries": {},
            "avg_cpc_current": 0.0,
            "avg_cpc_prior": 0.0,
            "avg_cpa_current": 0.0,
            "avg_cpa_prior": 0.0,
        }

        cpc_sum_current = 0.0
        cpc_sum_prior = 0.0
        cpa_sum_current = 0.0
        cpa_sum_prior = 0.0
        count = 0

        for ind in te.INDUSTRIES:
            try:
                current = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpc",
                    month=current_month,
                    year=current_year,
                )
                prior = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpc",
                    year=prior_year,
                )
                cpa_current = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpa",
                    month=current_month,
                    year=current_year,
                )
                cpa_prior = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpa",
                    year=prior_year,
                )

                cpc_now = current.get("value") or 0
                cpc_prev = prior.get("value") or 0
                cpa_now = cpa_current.get("value") or 0
                cpa_prev = cpa_prior.get("value") or 0

                pct_change = 0.0
                if cpc_prev and cpc_prev > 0:
                    pct_change = ((cpc_now - cpc_prev) / cpc_prev) * 100

                plat_data["industries"][ind] = {
                    "label": _industry_label(ind),
                    "cpc_current": round(cpc_now, 2),
                    "cpc_prior": round(cpc_prev, 2),
                    "cpc_change_pct": round(pct_change, 1),
                    "cpa_current": round(cpa_now, 2),
                    "cpa_prior": round(cpa_prev, 2),
                    "trend": _trend_word(pct_change),
                    "arrow": _trend_arrow(pct_change),
                }

                cpc_sum_current += cpc_now
                cpc_sum_prior += cpc_prev
                cpa_sum_current += cpa_now
                cpa_sum_prior += cpa_prev
                count += 1

                # Track industry-level aggregation across platforms
                if ind not in industry_cpc_changes:
                    industry_cpc_changes[ind] = {
                        "total_change": 0.0,
                        "count": 0,
                        "cpc_current": 0.0,
                        "cpc_prior": 0.0,
                    }
                industry_cpc_changes[ind]["total_change"] += pct_change
                industry_cpc_changes[ind]["count"] += 1
                industry_cpc_changes[ind]["cpc_current"] += cpc_now
                industry_cpc_changes[ind]["cpc_prior"] += cpc_prev

            except Exception as exc:
                logger.debug("market_pulse: CPC trend error %s/%s: %s", plat, ind, exc)
                continue

        if count > 0:
            plat_data["avg_cpc_current"] = round(cpc_sum_current / count, 2)
            plat_data["avg_cpc_prior"] = round(cpc_sum_prior / count, 2)
            plat_data["avg_cpa_current"] = round(cpa_sum_current / count, 2)
            plat_data["avg_cpa_prior"] = round(cpa_sum_prior / count, 2)
            if plat_data["avg_cpc_prior"] > 0:
                plat_data["avg_cpc_change_pct"] = round(
                    (
                        (plat_data["avg_cpc_current"] - plat_data["avg_cpc_prior"])
                        / plat_data["avg_cpc_prior"]
                    )
                    * 100,
                    1,
                )
            else:
                plat_data["avg_cpc_change_pct"] = 0.0
        else:
            plat_data["avg_cpc_change_pct"] = 0.0

        platform_summaries[plat] = plat_data

    # Compute average CPC change per industry across all platforms
    industry_avg_changes = {}
    for ind, data in industry_cpc_changes.items():
        if data["count"] > 0:
            avg_change = data["total_change"] / data["count"]
            avg_cpc = data["cpc_current"] / data["count"]
            industry_avg_changes[ind] = {
                "label": _industry_label(ind),
                "avg_cpc_change_pct": round(avg_change, 1),
                "avg_cpc": round(avg_cpc, 2),
                "trend": _trend_word(avg_change),
                "arrow": _trend_arrow(avg_change),
            }

    return {
        "available": True,
        "period": f"{now.strftime('%B %Y')}",
        "comparison": f"vs {(now.replace(year=now.year - 1)).strftime('%B %Y')}",
        "platforms": platform_summaries,
        "industry_averages": industry_avg_changes,
    }


def collect_market_demand() -> Dict[str, Any]:
    """Pull JOLTS data and market tightness indicators.

    Attempts live BLS JOLTS data via api_enrichment, falls back
    to research.py embedded data if unavailable.
    """
    result = {
        "available": False,
        "job_openings": None,
        "hires": None,
        "quits": None,
        "layoffs": None,
        "tightness_index": None,
        "tightness_label": None,
        "unemployment_rate": None,
        "source": "embedded",
    }

    # Attempt live JOLTS data
    api = _lazy_api_enrichment()
    if api and hasattr(api, "fetch_bls_jolts"):
        try:
            jo_data = api.fetch_bls_jolts(
                industry_code="000000", data_element="JO", years=2
            )
            hi_data = api.fetch_bls_jolts(
                industry_code="000000", data_element="HI", years=2
            )
            qu_data = api.fetch_bls_jolts(
                industry_code="000000", data_element="QU", years=2
            )
            ld_data = api.fetch_bls_jolts(
                industry_code="000000", data_element="LD", years=2
            )

            if jo_data and jo_data.get("latest_value"):
                result["job_openings"] = jo_data.get("latest_value")
                result["job_openings_trend"] = jo_data.get("trend", "stable")
                result["source"] = "BLS JOLTS (live)"
                result["available"] = True

            if hi_data and hi_data.get("latest_value"):
                result["hires"] = hi_data.get("latest_value")

            if qu_data and qu_data.get("latest_value"):
                result["quits"] = qu_data.get("latest_value")

            if ld_data and ld_data.get("latest_value"):
                result["layoffs"] = ld_data.get("latest_value")

        except Exception as exc:
            logger.debug("market_pulse: JOLTS fetch error: %s", exc)

    # Attempt FRED data for unemployment
    if api and hasattr(api, "fetch_fred_indicators"):
        try:
            fred = api.fetch_fred_indicators()
            if fred and fred.get("unemployment_rate"):
                unemp = fred.get("unemployment_rate")
                if isinstance(unemp, dict):
                    result["unemployment_rate"] = f"{unemp.get('value', 'N/A')}%"
                else:
                    result["unemployment_rate"] = unemp or "N/A"
                if not result["available"]:
                    result["source"] = "FRED (live)"
                    result["available"] = True
        except Exception as exc:
            logger.debug("market_pulse: FRED fetch error: %s", exc)

    # Fallback to research.py embedded data for any missing fields
    jolts_fallbacks: Dict[str, str] = {
        "job_openings": "8.1M",
        "hires": "5.6M",
        "quits": "3.4M",
        "layoffs": "1.6M",
    }
    if not result["available"]:
        res = _lazy_research()
        if res and hasattr(res, "COUNTRY_DATA"):
            us_data = res.COUNTRY_DATA.get("United States", {})
            if us_data.get("unemployment"):
                result["unemployment_rate"] = us_data["unemployment"]
                result["available"] = True
                result["source"] = "embedded (research.py)"

        # Fill all JOLTS fields from curated estimates
        for field, fallback_val in jolts_fallbacks.items():
            result[field] = fallback_val
        result["available"] = True
    else:
        # BLS partially succeeded -- fill any individual fields still None
        for field, fallback_val in jolts_fallbacks.items():
            if result.get(field) is None:
                result[field] = fallback_val

    # Calculate tightness index
    try:
        openings_val = result.get("job_openings")
        if isinstance(openings_val, str):
            openings_val = float(
                openings_val.replace("M", "").replace("m", "").replace(",", "")
            )
        hires_val = result.get("hires")
        if isinstance(hires_val, str):
            hires_val = float(
                hires_val.replace("M", "").replace("m", "").replace(",", "")
            )

        if openings_val and hires_val and hires_val > 0:
            tightness = openings_val / hires_val
            result["tightness_index"] = round(tightness, 2)
            if tightness > 1.8:
                result["tightness_label"] = (
                    "Very Tight (employer's market is challenging)"
                )
            elif tightness > 1.4:
                result["tightness_label"] = "Tight (competitive hiring landscape)"
            elif tightness > 1.0:
                result["tightness_label"] = "Moderate (balanced market)"
            else:
                result["tightness_label"] = "Loose (favorable for employers)"
    except Exception:
        result["tightness_index"] = None
        result["tightness_label"] = "Data unavailable"

    return result


def collect_industry_spotlight() -> Dict[str, Any]:
    """Pick top 5 industries by CPC change magnitude.

    Shows CPC trend, direction, and actionable recommendation
    for each spotlighted industry.
    """
    te = _lazy_trend_engine()
    if not te:
        return {"available": False, "industries": []}

    now = _get_current_date()
    current_year = min(now.year, 2025)
    prior_year = current_year - 1

    industry_scores = []

    for ind in te.INDUSTRIES:
        try:
            # Aggregate CPC change across platforms
            total_change = 0.0
            total_cpc = 0.0
            total_cpa = 0.0
            platform_count = 0

            for plat in te.PLATFORMS:
                current = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpc",
                    year=current_year,
                )
                prior = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpc",
                    year=prior_year,
                )
                cpa_now = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpa",
                    year=current_year,
                )

                cpc_now = current.get("value") or 0
                cpc_prev = prior.get("value") or 0

                if cpc_prev > 0:
                    change = ((cpc_now - cpc_prev) / cpc_prev) * 100
                    total_change += change
                    total_cpc += cpc_now
                    total_cpa += cpa_now.get("value") or 0
                    platform_count += 1

            if platform_count == 0:
                continue

            avg_change = total_change / platform_count
            avg_cpc = total_cpc / platform_count
            avg_cpa = total_cpa / platform_count

            # Generate recommendation based on trend
            if avg_change > 5:
                rec = (
                    f"CPCs rising {avg_change:.0f}% YoY. Consider shifting budget to "
                    f"programmatic or lower-cost platforms. Optimize ad copy for higher CTR."
                )
            elif avg_change > 0:
                rec = (
                    f"Moderate CPC increase of {avg_change:.0f}% YoY. Maintain current "
                    f"strategy but monitor for further escalation."
                )
            elif avg_change > -3:
                rec = (
                    f"CPCs stable. Good time to expand reach or test new channels "
                    f"while costs remain predictable."
                )
            else:
                rec = (
                    f"CPCs declining {abs(avg_change):.0f}% YoY. Opportunity to capture "
                    f"more volume at lower cost. Increase investment now."
                )

            industry_scores.append(
                {
                    "key": ind,
                    "label": _industry_label(ind),
                    "avg_cpc": round(avg_cpc, 2),
                    "avg_cpa": round(avg_cpa, 2),
                    "avg_change_pct": round(avg_change, 1),
                    "trend": _trend_word(avg_change),
                    "arrow": _trend_arrow(avg_change),
                    "recommendation": rec,
                    "activity_score": abs(avg_change),  # For sorting by magnitude
                }
            )

        except Exception as exc:
            logger.debug("market_pulse: industry spotlight error %s: %s", ind, exc)
            continue

    # Sort by activity score (most volatile first) and take top 5
    industry_scores.sort(key=lambda x: x["activity_score"], reverse=True)
    top_5 = industry_scores[:5]

    return {
        "available": True,
        "industries": top_5,
        "total_industries_analyzed": len(industry_scores),
    }


def collect_platform_shifts() -> Dict[str, Any]:
    """Compare platform performance changes across all industries.

    Ranks platforms by value (cheapest CPC, best CPA improvement).
    """
    te = _lazy_trend_engine()
    if not te:
        return {"available": False, "platforms": []}

    now = _get_current_date()
    current_year = min(now.year, 2025)
    prior_year = current_year - 1
    current_month = now.month

    platform_perf = []

    for plat in te.PLATFORMS:
        try:
            cpc_total = 0.0
            cpa_total = 0.0
            ctr_total = 0.0
            cvr_total = 0.0
            cpc_prev_total = 0.0
            count = 0

            for ind in te.INDUSTRIES:
                bm_cpc = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpc",
                    month=current_month,
                    year=current_year,
                )
                bm_cpa = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpa",
                    month=current_month,
                    year=current_year,
                )
                bm_ctr = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="ctr",
                    year=current_year,
                )
                bm_cvr = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cvr",
                    year=current_year,
                )
                bm_cpc_prev = te.get_benchmark(
                    platform=plat,
                    industry=ind,
                    metric="cpc",
                    year=prior_year,
                )

                cpc_total += bm_cpc.get("value") or 0
                cpa_total += bm_cpa.get("value") or 0
                ctr_total += bm_ctr.get("value") or 0
                cvr_total += bm_cvr.get("value") or 0
                cpc_prev_total += bm_cpc_prev.get("value") or 0
                count += 1

            if count == 0:
                continue

            avg_cpc = cpc_total / count
            avg_cpa = cpa_total / count
            avg_ctr = ctr_total / count
            avg_cvr = cvr_total / count
            avg_cpc_prev = cpc_prev_total / count

            cpc_change = 0.0
            if avg_cpc_prev > 0:
                cpc_change = ((avg_cpc - avg_cpc_prev) / avg_cpc_prev) * 100

            # Value score: lower CPC + improving CPC = better value
            # Negative change is good (getting cheaper)
            value_score = -cpc_change + (avg_ctr * 1000)  # Weight CTR

            platform_perf.append(
                {
                    "key": plat,
                    "label": PLATFORM_LABELS.get(plat, plat),
                    "avg_cpc": round(avg_cpc, 2),
                    "avg_cpa": round(avg_cpa, 2),
                    "avg_ctr": round(avg_ctr * 100, 2),  # As percentage
                    "avg_cvr": round(avg_cvr * 100, 2),  # As percentage
                    "cpc_change_pct": round(cpc_change, 1),
                    "trend": _trend_word(cpc_change),
                    "arrow": _trend_arrow(cpc_change),
                    "value_score": round(value_score, 1),
                }
            )

        except Exception as exc:
            logger.debug("market_pulse: platform shift error %s: %s", plat, exc)
            continue

    # Sort by value score descending (best value first)
    platform_perf.sort(key=lambda x: x["value_score"], reverse=True)

    # Assign rank
    for i, p in enumerate(platform_perf):
        p["rank"] = i + 1

    return {
        "available": True,
        "platforms": platform_perf,
    }


def collect_seasonal_insights() -> Dict[str, Any]:
    """Analyze what is happening seasonally this month and predict next month.

    Uses trend_engine SEASONAL_MULTIPLIERS for collar-type differentiation.
    """
    te = _lazy_trend_engine()
    if not te:
        return {"available": False}

    now = _get_current_date()
    current_month = now.month
    next_month = (current_month % 12) + 1
    next_month_name = datetime(2025, next_month, 1).strftime("%B")
    current_month_name = now.strftime("%B")

    insights = {}

    for collar in ["white_collar", "blue_collar", "grey_collar", "mixed"]:
        mults = te.SEASONAL_MULTIPLIERS.get(collar, {})
        current_mult = mults.get(current_month, 1.0)
        next_mult = mults.get(next_month, 1.0)

        # Interpret multiplier
        if current_mult > 1.10:
            intensity = "Peak hiring activity"
        elif current_mult > 1.02:
            intensity = "Above-average hiring"
        elif current_mult > 0.95:
            intensity = "Normal hiring levels"
        elif current_mult > 0.88:
            intensity = "Below-average hiring"
        else:
            intensity = "Low hiring activity"

        # Predict next month direction
        direction_change = next_mult - current_mult
        if direction_change > 0.05:
            forecast = f"Expect hiring to increase in {next_month_name}"
        elif direction_change < -0.05:
            forecast = f"Expect hiring to decrease in {next_month_name}"
        else:
            forecast = f"Hiring expected to remain steady into {next_month_name}"

        collar_label = collar.replace("_", " ").title()
        insights[collar] = {
            "label": collar_label,
            "current_multiplier": current_mult,
            "next_multiplier": next_mult,
            "intensity": intensity,
            "forecast": forecast,
            "cpc_impact": (
                f"CPCs are at {current_mult:.0%} of baseline"
                if current_mult != 1.0
                else "CPCs at baseline levels"
            ),
        }

    # Build a narrative summary for the overall market
    mixed = insights.get("mixed", {})
    wc = insights.get("white_collar", {})
    bc = insights.get("blue_collar", {})

    narrative_parts = []
    narrative_parts.append(
        f"{current_month_name} seasonal pattern: {mixed.get('intensity', 'Normal')}."
    )
    if wc.get("current_multiplier", 1.0) != bc.get("current_multiplier", 1.0):
        wc_mult = wc.get("current_multiplier", 1.0)
        bc_mult = bc.get("current_multiplier", 1.0)
        if wc_mult > bc_mult:
            narrative_parts.append(
                "White-collar hiring is outpacing blue-collar this month."
            )
        elif bc_mult > wc_mult:
            narrative_parts.append(
                "Blue-collar/hourly hiring is outpacing white-collar this month."
            )
    narrative_parts.append(mixed.get("forecast") or "")

    return {
        "available": True,
        "current_month": current_month_name,
        "next_month": next_month_name,
        "collar_insights": insights,
        "narrative": " ".join(narrative_parts),
    }


def collect_salary_trends() -> Dict[str, Any]:
    """Top roles by salary data from research.py embedded data.

    Shows salary ranges for common recruitment roles across industries.
    """
    res = _lazy_research()
    if not res:
        return {"available": False, "roles": []}

    # Representative roles across collar types
    sample_roles = [
        ("Registered Nurse", "healthcare_medical"),
        ("Software Engineer", "tech_engineering"),
        ("Warehouse Associate", "logistics_supply_chain"),
        ("Financial Analyst", "finance_banking"),
        ("Retail Sales Associate", "retail_consumer"),
        ("Electrician", "blue_collar_trades"),
        ("Restaurant Manager", "hospitality_travel"),
        ("Truck Driver", "logistics_supply_chain"),
        ("Data Scientist", "tech_engineering"),
        ("Pharmacist", "pharma_biotech"),
    ]

    role_data = []
    for role_name, industry in sample_roles:
        try:
            if hasattr(res, "_get_role_salary_range"):
                salary = res._get_role_salary_range(role_name, location_coli=100)
            elif hasattr(res, "get_role_salary_range"):
                salary = res.get_role_salary_range(role_name, location_coli=100)
            else:
                salary = "N/A"

            role_data.append(
                {
                    "role": role_name,
                    "industry": _industry_label(industry),
                    "salary_range": salary if salary else "N/A",
                }
            )
        except Exception:
            role_data.append(
                {
                    "role": role_name,
                    "industry": _industry_label(industry),
                    "salary_range": "N/A",
                }
            )

    return {
        "available": True,
        "roles": role_data,
        "note": "Salary ranges represent national median estimates, adjusted for cost of living index.",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════


def _generate_key_takeaways(
    cpc_trends: Dict,
    market_demand: Dict,
    industry_spotlight: Dict,
    platform_shifts: Dict,
    seasonal_insights: Dict,
) -> List[str]:
    """Auto-generate top 3-5 key takeaways from collected data."""
    takeaways = []

    # 1. Overall CPC direction
    if cpc_trends.get("available"):
        platforms = cpc_trends.get("platforms", {})
        rising = 0
        falling = 0
        for _plat, pdata in platforms.items():
            change = pdata.get("avg_cpc_change_pct") or 0
            if change > 2:
                rising += 1
            elif change < -2:
                falling += 1
        if rising > falling:
            takeaways.append(
                f"Recruitment CPCs are trending upward across {rising} of {len(platforms)} "
                f"platforms -- budget owners should review allocation efficiency."
            )
        elif falling > rising:
            takeaways.append(
                f"CPCs are declining on {falling} of {len(platforms)} platforms -- "
                f"an opportunity to increase volume at lower cost per click."
            )
        else:
            takeaways.append(
                "Recruitment CPCs remain broadly stable across platforms this period."
            )

    # 2. Market tightness
    if market_demand.get("available"):
        tightness = market_demand.get("tightness_label") or ""
        if "Tight" in tightness or "Very Tight" in tightness:
            takeaways.append(
                f"The labor market remains tight (openings/hires ratio: "
                f"{market_demand.get('tightness_index', 'N/A')}) -- "
                f"employers must compete aggressively for talent."
            )
        elif "Loose" in tightness:
            takeaways.append(
                "Labor market conditions are easing -- employers have more "
                "leverage in talent acquisition negotiations."
            )

    # 3. Hottest industry
    if industry_spotlight.get("available"):
        industries = industry_spotlight.get("industries") or []
        if industries:
            top = industries[0]
            direction = "rising" if top["avg_change_pct"] > 0 else "declining"
            takeaways.append(
                f"{top['label']} shows the most CPC movement ({_fmt_pct(top['avg_change_pct'])} YoY), "
                f"with costs {direction}. {top['recommendation'][:80]}..."
            )

    # 4. Best value platform
    if platform_shifts.get("available"):
        platforms = platform_shifts.get("platforms") or []
        if platforms:
            best = platforms[0]
            takeaways.append(
                f"{best['label']} offers the best value this period with avg CPC "
                f"of {_fmt_currency(best['avg_cpc'])} ({_fmt_pct(best['cpc_change_pct'])} YoY)."
            )

    # 5. Seasonal outlook
    if seasonal_insights.get("available"):
        narrative = seasonal_insights.get("narrative") or ""
        if narrative:
            takeaways.append(narrative)

    return takeaways[:5]


def generate_pulse_report(week_date: Optional[str] = None) -> Dict[str, Any]:
    """Orchestrator: collect all data sources and build structured report.

    Args:
        week_date: Optional ISO date string (YYYY-MM-DD) for the report.
                   Defaults to current date.

    Returns:
        Structured report dict with all sections.
    """
    now = _get_current_date()
    if week_date:
        try:
            report_date = datetime.fromisoformat(week_date.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            report_date = now
    else:
        report_date = now

    report_id = report_date.strftime("%Y%m%d_%H%M%S")
    period_start = report_date - timedelta(days=7)

    logger.info(
        "market_pulse: generating report for %s", report_date.strftime("%Y-%m-%d")
    )

    # Collect all data sections with error isolation
    cpc_trends = {}
    market_demand = {}
    industry_spotlight = {}
    platform_shifts = {}
    seasonal_insights = {}
    salary_trends = {}

    try:
        cpc_trends = collect_cpc_trends()
    except Exception as exc:
        logger.error("market_pulse: cpc_trends collection failed: %s", exc)
        cpc_trends = {"available": False, "error": str(exc)}

    try:
        market_demand = collect_market_demand()
    except Exception as exc:
        logger.error("market_pulse: market_demand collection failed: %s", exc)
        market_demand = {"available": False, "error": str(exc)}

    try:
        industry_spotlight = collect_industry_spotlight()
    except Exception as exc:
        logger.error("market_pulse: industry_spotlight collection failed: %s", exc)
        industry_spotlight = {"available": False, "error": str(exc)}

    try:
        platform_shifts = collect_platform_shifts()
    except Exception as exc:
        logger.error("market_pulse: platform_shifts collection failed: %s", exc)
        platform_shifts = {"available": False, "error": str(exc)}

    try:
        seasonal_insights = collect_seasonal_insights()
    except Exception as exc:
        logger.error("market_pulse: seasonal_insights collection failed: %s", exc)
        seasonal_insights = {"available": False, "error": str(exc)}

    try:
        salary_trends = collect_salary_trends()
    except Exception as exc:
        logger.error("market_pulse: salary_trends collection failed: %s", exc)
        salary_trends = {"available": False, "error": str(exc)}

    # Generate takeaways (with error isolation)
    try:
        key_takeaways = _generate_key_takeaways(
            cpc_trends,
            market_demand,
            industry_spotlight,
            platform_shifts,
            seasonal_insights,
        )
    except Exception as exc:
        logger.error("market_pulse: key_takeaways generation failed: %s", exc)
        key_takeaways = ["Report generated but takeaway summary unavailable."]

    report = {
        "report_id": report_id,
        "report_date": report_date.strftime("%Y-%m-%d"),
        "report_date_display": report_date.strftime("%B %d, %Y"),
        "period": f"{period_start.strftime('%b %d')} - {report_date.strftime('%b %d, %Y')}",
        "generated_at": now.isoformat(),
        "cpc_trends": cpc_trends,
        "market_demand": market_demand,
        "industry_spotlight": industry_spotlight,
        "platform_shifts": platform_shifts,
        "seasonal_insights": seasonal_insights,
        "salary_trends": salary_trends,
        "key_takeaways": key_takeaways,
        "data_vintage": None,
    }

    # Include trend engine data vintage if available
    te = _lazy_trend_engine()
    if te and hasattr(te, "BENCHMARK_VINTAGE"):
        report["data_vintage"] = te.BENCHMARK_VINTAGE

    # Store in history
    with _lock:
        _report_history.insert(
            0,
            {
                "report_id": report_id,
                "report_date": report["report_date"],
                "period": report["period"],
                "generated_at": report["generated_at"],
                "key_takeaways": key_takeaways[:3],
                "full_report": report,
            },
        )
        # Trim history
        while len(_report_history) > _MAX_HISTORY:
            _report_history.pop()

    logger.info("market_pulse: report %s generated successfully", report_id)
    return report


# ═══════════════════════════════════════════════════════════════════════════════
# 3. HTML REPORT GENERATION -- Full Standalone Report
# ═══════════════════════════════════════════════════════════════════════════════


def generate_pulse_html(report_data: Dict[str, Any]) -> str:
    """Generate a beautiful standalone HTML report for print/web viewing.

    Reuses brand colors and print-optimized patterns from pdf_generator.py.
    """
    # Brand colors
    PORT_GORE = "#202058"
    BLUE_VIOLET = "#5A54BD"
    DOWNY_TEAL = "#6BB3CD"
    TAPESTRY_PINK = "#B5669C"
    RAW_SIENNA = "#CE9047"
    TEXT_DARK = "#1a1a2e"
    TEXT_MUTED = "#555566"
    BORDER_LIGHT = "#d0d0e0"
    BG_ZEBRA = "#f4f4f9"

    report_date = _safe(report_data.get("report_date_display") or "")
    period = _safe(report_data.get("period") or "")
    vintage = _safe(report_data.get("data_vintage") or "")

    # --- Build CPC Dashboard section ---
    cpc_section = ""
    cpc_trends = report_data.get("cpc_trends", {})
    if cpc_trends.get("available"):
        platform_bars = []
        platforms = cpc_trends.get("platforms", {})
        max_cpc = max(
            (p.get("avg_cpc_current") or 0 for p in platforms.values()), default=1
        )
        if max_cpc == 0:
            max_cpc = 1

        bar_colors = [
            BLUE_VIOLET,
            DOWNY_TEAL,
            TAPESTRY_PINK,
            RAW_SIENNA,
            PORT_GORE,
            "#7C6BC4",
        ]
        for i, (plat_key, plat_data) in enumerate(platforms.items()):
            pct_width = min(
                100, ((plat_data.get("avg_cpc_current") or 0) / max_cpc) * 100
            )
            color = bar_colors[i % len(bar_colors)]
            change = plat_data.get("avg_cpc_change_pct") or 0
            arrow = _trend_arrow(change)
            change_color = (
                "#d32f2f" if change > 0 else "#2e7d32" if change < 0 else TEXT_MUTED
            )

            platform_bars.append(
                f"""
            <div style="margin-bottom:12px;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                    <span style="font-weight:600;font-size:13px;color:{TEXT_DARK};">{_safe(plat_data.get('label', plat_key))}</span>
                    <span style="font-size:13px;">
                        <strong>${plat_data.get('avg_cpc_current') or 0:.2f}</strong>
                        <span style="color:{change_color};margin-left:6px;">{arrow} {change:+.1f}%</span>
                    </span>
                </div>
                <div style="background:#e8e8f0;border-radius:4px;height:22px;overflow:hidden;">
                    <div style="background:{color};width:{pct_width:.0f}%;height:100%;border-radius:4px;transition:width 0.3s;"></div>
                </div>
            </div>"""
            )

        cpc_section = f"""
        <div style="page-break-inside:avoid;margin-bottom:28px;">
            <h2 style="color:{PORT_GORE};font-size:20px;border-bottom:2px solid {BLUE_VIOLET};padding-bottom:8px;margin-bottom:16px;">
                CPC Trend Dashboard
            </h2>
            <p style="color:{TEXT_MUTED};font-size:13px;margin-bottom:16px;">
                Average CPC across all 22 industries &middot; {_safe(cpc_trends.get('comparison') or '')}
            </p>
            {''.join(platform_bars)}
        </div>"""

    # --- Build Industry Spotlight section ---
    industry_section = ""
    ind_data = report_data.get("industry_spotlight", {})
    if ind_data.get("available"):
        cards = []
        for ind in ind_data.get("industries") or []:
            change = ind.get("avg_change_pct") or 0
            change_color = (
                "#d32f2f" if change > 0 else "#2e7d32" if change < 0 else TEXT_MUTED
            )
            cards.append(
                f"""
            <div style="border:1px solid {BORDER_LIGHT};border-radius:8px;padding:16px;margin-bottom:12px;background:{BG_ZEBRA};page-break-inside:avoid;">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                    <h3 style="margin:0;font-size:15px;color:{PORT_GORE};">{_safe(ind.get('label') or '')}</h3>
                    <span style="color:{change_color};font-weight:700;font-size:15px;">
                        {ind.get('arrow') or ''} {_fmt_pct(change)}
                    </span>
                </div>
                <div style="display:flex;gap:20px;font-size:13px;color:{TEXT_MUTED};margin-bottom:8px;">
                    <span>Avg CPC: <strong style="color:{TEXT_DARK};">${ind.get('avg_cpc') or 0:.2f}</strong></span>
                    <span>Avg CPA: <strong style="color:{TEXT_DARK};">${ind.get('avg_cpa') or 0:.2f}</strong></span>
                    <span>Trend: <strong>{_safe(ind.get('trend') or '')}</strong></span>
                </div>
                <p style="font-size:12px;color:{TEXT_DARK};margin:0;line-height:1.5;">
                    {_safe(ind.get('recommendation') or '')}
                </p>
            </div>"""
            )

        industry_section = f"""
        <div style="page-break-inside:avoid;margin-bottom:28px;">
            <h2 style="color:{PORT_GORE};font-size:20px;border-bottom:2px solid {TAPESTRY_PINK};padding-bottom:8px;margin-bottom:16px;">
                Industry Spotlight (Top 5 by Activity)
            </h2>
            {''.join(cards)}
        </div>"""

    # --- Platform Comparison Table ---
    platform_table = ""
    plat_shifts = report_data.get("platform_shifts", {})
    if plat_shifts.get("available"):
        rows = []
        for p in plat_shifts.get("platforms") or []:
            change = p.get("cpc_change_pct") or 0
            change_color = (
                "#d32f2f" if change > 0 else "#2e7d32" if change < 0 else TEXT_MUTED
            )
            bg = BG_ZEBRA if p.get("rank") or 0 % 2 == 0 else "#ffffff"
            rows.append(
                f"""
            <tr style="background:{bg};">
                <td style="padding:10px 12px;font-weight:600;color:{TEXT_DARK};">#{p.get('rank') or ''}</td>
                <td style="padding:10px 12px;color:{TEXT_DARK};">{_safe(p.get('label') or '')}</td>
                <td style="padding:10px 12px;text-align:right;">${p.get('avg_cpc') or 0:.2f}</td>
                <td style="padding:10px 12px;text-align:right;">${p.get('avg_cpa') or 0:.2f}</td>
                <td style="padding:10px 12px;text-align:right;">{p.get('avg_ctr') or 0:.2f}%</td>
                <td style="padding:10px 12px;text-align:right;">{p.get('avg_cvr') or 0:.2f}%</td>
                <td style="padding:10px 12px;text-align:right;color:{change_color};font-weight:600;">{p.get('arrow') or ''} {change:+.1f}%</td>
            </tr>"""
            )

        platform_table = f"""
        <div style="page-break-inside:avoid;margin-bottom:28px;">
            <h2 style="color:{PORT_GORE};font-size:20px;border-bottom:2px solid {DOWNY_TEAL};padding-bottom:8px;margin-bottom:16px;">
                Platform Comparison (Ranked by Value)
            </h2>
            <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <thead>
                    <tr style="background:{PORT_GORE};color:white;">
                        <th style="padding:10px 12px;text-align:left;">Rank</th>
                        <th style="padding:10px 12px;text-align:left;">Platform</th>
                        <th style="padding:10px 12px;text-align:right;">Avg CPC</th>
                        <th style="padding:10px 12px;text-align:right;">Avg CPA</th>
                        <th style="padding:10px 12px;text-align:right;">Avg CTR</th>
                        <th style="padding:10px 12px;text-align:right;">Avg CVR</th>
                        <th style="padding:10px 12px;text-align:right;">CPC YoY</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows)}
                </tbody>
            </table>
        </div>"""

    # --- Seasonal Outlook ---
    seasonal_section = ""
    seasonal = report_data.get("seasonal_insights", {})
    if seasonal.get("available"):
        collar_rows = []
        for collar_key in ["white_collar", "blue_collar", "grey_collar", "mixed"]:
            c = seasonal.get("collar_insights", {}).get(collar_key, {})
            if c:
                collar_rows.append(
                    f"""
                <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid {BORDER_LIGHT};">
                    <span style="font-weight:600;color:{TEXT_DARK};width:120px;">{_safe(c.get('label') or '')}</span>
                    <span style="color:{TEXT_MUTED};font-size:13px;">{_safe(c.get('intensity') or '')}</span>
                    <span style="font-size:13px;color:{TEXT_DARK};">{_safe(c.get('cpc_impact') or '')}</span>
                    <span style="font-size:12px;color:{BLUE_VIOLET};">{_safe(c.get('forecast') or '')}</span>
                </div>"""
                )

        seasonal_section = f"""
        <div style="page-break-inside:avoid;margin-bottom:28px;">
            <h2 style="color:{PORT_GORE};font-size:20px;border-bottom:2px solid {RAW_SIENNA};padding-bottom:8px;margin-bottom:16px;">
                Seasonal Outlook: {_safe(seasonal.get('current_month') or '')} &rarr; {_safe(seasonal.get('next_month') or '')}
            </h2>
            <div style="background:{BG_ZEBRA};border-radius:8px;padding:16px;margin-bottom:12px;">
                <p style="font-size:14px;color:{TEXT_DARK};line-height:1.6;margin:0;">
                    {_safe(seasonal.get('narrative') or '')}
                </p>
            </div>
            {''.join(collar_rows)}
        </div>"""

    # --- Market Demand ---
    demand_section = ""
    md = report_data.get("market_demand", {})
    if md.get("available"):
        demand_section = f"""
        <div style="page-break-inside:avoid;margin-bottom:28px;">
            <h2 style="color:{PORT_GORE};font-size:20px;border-bottom:2px solid {PORT_GORE};padding-bottom:8px;margin-bottom:16px;">
                Labor Market Demand
            </h2>
            <div style="display:flex;flex-wrap:wrap;gap:16px;">
                <div style="flex:1;min-width:140px;background:{BG_ZEBRA};border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:24px;font-weight:700;color:{BLUE_VIOLET};">{_safe(md.get('job_openings', 'N/A'))}</div>
                    <div style="font-size:12px;color:{TEXT_MUTED};margin-top:4px;">Job Openings</div>
                </div>
                <div style="flex:1;min-width:140px;background:{BG_ZEBRA};border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:24px;font-weight:700;color:{DOWNY_TEAL};">{_safe(md.get('hires', 'N/A'))}</div>
                    <div style="font-size:12px;color:{TEXT_MUTED};margin-top:4px;">Monthly Hires</div>
                </div>
                <div style="flex:1;min-width:140px;background:{BG_ZEBRA};border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:24px;font-weight:700;color:{RAW_SIENNA};">{_safe(md.get('quits', 'N/A'))}</div>
                    <div style="font-size:12px;color:{TEXT_MUTED};margin-top:4px;">Voluntary Quits</div>
                </div>
                <div style="flex:1;min-width:140px;background:{BG_ZEBRA};border-radius:8px;padding:16px;text-align:center;">
                    <div style="font-size:24px;font-weight:700;color:{TAPESTRY_PINK};">{_safe(md.get('unemployment_rate', 'N/A'))}</div>
                    <div style="font-size:12px;color:{TEXT_MUTED};margin-top:4px;">Unemployment Rate</div>
                </div>
            </div>
            <div style="margin-top:16px;padding:12px 16px;background:{BG_ZEBRA};border-radius:8px;border-left:4px solid {BLUE_VIOLET};">
                <strong style="color:{TEXT_DARK};">Market Tightness:</strong>
                <span style="color:{TEXT_MUTED};margin-left:8px;">{_safe(md.get('tightness_label', 'N/A'))}</span>
                <span style="color:{BLUE_VIOLET};margin-left:8px;font-weight:700;">
                    (Ratio: {_safe(md.get('tightness_index', 'N/A'))})
                </span>
            </div>
            <p style="font-size:11px;color:{TEXT_MUTED};margin-top:8px;">Source: {_safe(md.get('source') or '')}</p>
        </div>"""

    # --- Key Takeaways ---
    takeaways = report_data.get("key_takeaways") or []
    takeaway_items = "\n".join(
        f'<li style="margin-bottom:8px;line-height:1.6;color:{TEXT_DARK};">{_safe(t)}</li>'
        for t in takeaways
    )

    # --- Executive Summary ---
    exec_summary = f"""
    <div style="page-break-inside:avoid;margin-bottom:28px;">
        <h2 style="color:{PORT_GORE};font-size:20px;border-bottom:2px solid {BLUE_VIOLET};padding-bottom:8px;margin-bottom:16px;">
            Executive Summary
        </h2>
        <ul style="padding-left:20px;font-size:14px;">
            {takeaway_items}
        </ul>
    </div>"""

    # --- Assemble Full Report ---
    report_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Market Intelligence Pulse - {report_date} | Nova AI Suite</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        * {{ margin:0; padding:0; box-sizing:border-box; }}
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            color: {TEXT_DARK};
            background: #ffffff;
            line-height: 1.6;
        }}
        .container {{
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 32px;
        }}
        @media print {{
            body {{ background: white; }}
            .container {{ padding: 20px; max-width: 100%; }}
            .no-print {{ display: none !important; }}
        }}
        @media (max-width: 640px) {{
            .container {{ padding: 20px 16px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div style="text-align:center;margin-bottom:36px;padding-bottom:24px;border-bottom:3px solid {PORT_GORE};">
            <div style="font-size:12px;text-transform:uppercase;letter-spacing:3px;color:{BLUE_VIOLET};font-weight:700;margin-bottom:8px;">Nova AI Suite</div>
            <h1 style="font-size:32px;color:{PORT_GORE};margin-bottom:8px;font-weight:800;">Market Intelligence Pulse</h1>
            <p style="font-size:15px;color:{TEXT_MUTED};">
                {period} &middot; Generated {report_date}
            </p>
            {f'<p style="font-size:11px;color:{TEXT_MUTED};margin-top:4px;">Data vintage: {vintage}</p>' if vintage else ''}
        </div>

        {exec_summary}
        {demand_section}
        {cpc_section}
        {industry_section}
        {platform_table}
        {seasonal_section}

        <!-- Footer -->
        <div style="text-align:center;margin-top:40px;padding-top:24px;border-top:2px solid {BORDER_LIGHT};">
            <p style="font-size:12px;color:{TEXT_MUTED};">
                Powered by <strong style="color:{BLUE_VIOLET};">Nova AI Suite</strong> &middot;
                Data from 6 ad platforms across 22 industries &middot;
                <a href="https://media-plan-generator.onrender.com" style="color:{BLUE_VIOLET};text-decoration:none;">Nova AI Suite</a>
            </p>
            <p style="font-size:11px;color:{TEXT_MUTED};margin-top:4px;">
                This report is generated from proprietary benchmark data and public economic indicators.
                Past performance does not guarantee future results.
            </p>
        </div>
    </div>
</body>
</html>"""

    return report_html


# ═══════════════════════════════════════════════════════════════════════════════
# 4. EMAIL-SAFE HTML REPORT
# ═══════════════════════════════════════════════════════════════════════════════


def generate_pulse_email_html(report_data: Dict[str, Any]) -> str:
    """Generate email-safe HTML (tables for layout, no flexbox/grid).

    Shorter highlight version with link to full report.
    """
    report_date = _safe(report_data.get("report_date_display") or "")
    period = _safe(report_data.get("period") or "")
    report_id = report_data.get("report_id") or ""
    full_report_url = f"{_BASE_URL}/market-pulse"

    # Key takeaways
    takeaways = report_data.get("key_takeaways") or []
    takeaway_rows = ""
    for t in takeaways:
        takeaway_rows += f"""
        <tr>
            <td style="padding:8px 12px;font-size:14px;line-height:1.6;color:#1a1a2e;border-bottom:1px solid #e8e8f0;">
                &#8226; {_safe(t)}
            </td>
        </tr>"""

    # Platform CPC summary table
    platform_rows = ""
    cpc_trends = report_data.get("cpc_trends", {})
    if cpc_trends.get("available"):
        for plat_key, plat_data in cpc_trends.get("platforms", {}).items():
            change = plat_data.get("avg_cpc_change_pct") or 0
            change_color = (
                "#d32f2f" if change > 0 else "#2e7d32" if change < 0 else "#555566"
            )
            platform_rows += f"""
            <tr>
                <td style="padding:8px 12px;font-size:13px;color:#1a1a2e;border-bottom:1px solid #e8e8f0;">
                    {_safe(plat_data.get('label', plat_key))}
                </td>
                <td style="padding:8px 12px;font-size:13px;text-align:right;color:#1a1a2e;border-bottom:1px solid #e8e8f0;">
                    ${plat_data.get('avg_cpc_current') or 0:.2f}
                </td>
                <td style="padding:8px 12px;font-size:13px;text-align:right;color:{change_color};font-weight:600;border-bottom:1px solid #e8e8f0;">
                    {change:+.1f}%
                </td>
            </tr>"""

    # Top 3 industries
    industry_rows = ""
    ind_data = report_data.get("industry_spotlight", {})
    if ind_data.get("available"):
        for ind in ind_data.get("industries") or [][:3]:
            change = ind.get("avg_change_pct") or 0
            change_color = (
                "#d32f2f" if change > 0 else "#2e7d32" if change < 0 else "#555566"
            )
            industry_rows += f"""
            <tr>
                <td style="padding:8px 12px;font-size:13px;color:#1a1a2e;border-bottom:1px solid #e8e8f0;">
                    {_safe(ind.get('label') or '')}
                </td>
                <td style="padding:8px 12px;font-size:13px;text-align:right;color:#1a1a2e;border-bottom:1px solid #e8e8f0;">
                    ${ind.get('avg_cpc') or 0:.2f}
                </td>
                <td style="padding:8px 12px;font-size:13px;text-align:right;color:{change_color};font-weight:600;border-bottom:1px solid #e8e8f0;">
                    {change:+.1f}%
                </td>
            </tr>"""

    email_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Market Intelligence Pulse - {report_date}</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f4f9;font-family:Arial,Helvetica,sans-serif;">
    <!-- Wrapper -->
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#f4f4f9;">
        <tr>
            <td align="center" style="padding:24px 16px;">
                <!-- Main content -->
                <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="background:#ffffff;border-radius:8px;overflow:hidden;max-width:600px;">

                    <!-- Header -->
                    <tr>
                        <td style="background:#202058;padding:28px 24px;text-align:center;">
                            <p style="margin:0 0 4px;font-size:11px;text-transform:uppercase;letter-spacing:3px;color:#818CF8;font-weight:700;">Nova AI Suite</p>
                            <h1 style="margin:0;font-size:24px;color:#ffffff;font-weight:800;">Market Intelligence Pulse</h1>
                            <p style="margin:8px 0 0;font-size:13px;color:rgba(255,255,255,0.7);">{period}</p>
                        </td>
                    </tr>

                    <!-- Key Takeaways -->
                    <tr>
                        <td style="padding:24px;">
                            <h2 style="margin:0 0 12px;font-size:18px;color:#202058;border-bottom:2px solid #5A54BD;padding-bottom:8px;">
                                Key Takeaways
                            </h2>
                            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                                {takeaway_rows}
                            </table>
                        </td>
                    </tr>

                    <!-- Platform CPC Summary -->
                    {f'''<tr>
                        <td style="padding:0 24px 24px;">
                            <h2 style="margin:0 0 12px;font-size:18px;color:#202058;border-bottom:2px solid #6BB3CD;padding-bottom:8px;">
                                Platform CPC Snapshot
                            </h2>
                            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #e8e8f0;border-radius:4px;">
                                <tr style="background:#202058;">
                                    <th style="padding:10px 12px;text-align:left;font-size:12px;color:white;font-weight:600;">Platform</th>
                                    <th style="padding:10px 12px;text-align:right;font-size:12px;color:white;font-weight:600;">Avg CPC</th>
                                    <th style="padding:10px 12px;text-align:right;font-size:12px;color:white;font-weight:600;">YoY Change</th>
                                </tr>
                                {platform_rows}
                            </table>
                        </td>
                    </tr>''' if platform_rows else ''}

                    <!-- Top Industries -->
                    {f'''<tr>
                        <td style="padding:0 24px 24px;">
                            <h2 style="margin:0 0 12px;font-size:18px;color:#202058;border-bottom:2px solid #B5669C;padding-bottom:8px;">
                                Top 3 Industry Movers
                            </h2>
                            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border:1px solid #e8e8f0;border-radius:4px;">
                                <tr style="background:#202058;">
                                    <th style="padding:10px 12px;text-align:left;font-size:12px;color:white;font-weight:600;">Industry</th>
                                    <th style="padding:10px 12px;text-align:right;font-size:12px;color:white;font-weight:600;">Avg CPC</th>
                                    <th style="padding:10px 12px;text-align:right;font-size:12px;color:white;font-weight:600;">YoY Change</th>
                                </tr>
                                {industry_rows}
                            </table>
                        </td>
                    </tr>''' if industry_rows else ''}

                    <!-- CTA -->
                    <tr>
                        <td style="padding:0 24px 32px;text-align:center;">
                            <a href="{full_report_url}" style="display:inline-block;background:#5A54BD;color:#ffffff;padding:12px 32px;border-radius:6px;text-decoration:none;font-size:14px;font-weight:600;">
                                View Full Report
                            </a>
                        </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                        <td style="background:#f4f4f9;padding:20px 24px;text-align:center;border-top:1px solid #e8e8f0;">
                            <p style="margin:0;font-size:11px;color:#555566;">
                                Powered by <strong style="color:#5A54BD;">Nova AI Suite</strong> |
                                Data from 6 ad platforms across 22 industries
                            </p>
                            <p style="margin:6px 0 0;font-size:10px;color:#888899;">
                                You are receiving this because you subscribed to Nova AI Suite Market Pulse reports.
                                <a href="{_BASE_URL}/market-pulse" style="color:#5A54BD;">Manage preferences</a>
                            </p>
                        </td>
                    </tr>

                </table>
            </td>
        </tr>
    </table>
</body>
</html>"""

    return email_html


# ═══════════════════════════════════════════════════════════════════════════════
# 5. EMAIL SENDING (via Resend API)
# ═══════════════════════════════════════════════════════════════════════════════


def send_pulse_report(
    report_data: Dict[str, Any],
    recipients: List[str],
) -> Dict[str, Any]:
    """Send pulse report via Resend API.

    Follows the pattern established in email_alerts.py:
    - Uses RESEND_API_KEY env var for auth
    - Bearer token authentication
    - urllib.request for stdlib-only HTTP
    - Graceful failure (never raises)

    Args:
        report_data: The structured report dict from generate_pulse_report().
        recipients: List of email addresses to send to.

    Returns:
        Dict with: sent (bool), recipients_sent (list), errors (list)
    """
    api_key = (os.environ.get("RESEND_API_KEY") or "").strip()
    from_email = os.environ.get("RESEND_FROM_EMAIL", "onboarding@resend.dev").strip()

    result = {
        "sent": False,
        "recipients_sent": [],
        "errors": [],
    }

    if not api_key:
        result["errors"].append("RESEND_API_KEY not configured")
        logger.warning("market_pulse: cannot send email -- RESEND_API_KEY not set")
        return result

    if not recipients:
        result["errors"].append("No recipients specified")
        return result

    # Generate email HTML
    email_html = generate_pulse_email_html(report_data)

    report_date = report_data.get("report_date_display", "Weekly")
    subject = f"Market Intelligence Pulse - {report_date} | Nova AI Suite"

    for recipient in recipients:
        recipient = recipient.strip()
        if not recipient or "@" not in recipient:
            result["errors"].append(f"Invalid email: {recipient}")
            continue

        try:
            payload = {
                "from": from_email,
                "to": [recipient],
                "subject": subject,
                "html": email_html,
            }

            body = json.dumps(payload).encode("utf-8")

            req = urllib.request.Request(
                _RESEND_ENDPOINT,
                data=body,
                method="POST",
            )
            req.add_header("Authorization", f"Bearer {api_key}")
            req.add_header("Content-Type", "application/json")
            req.add_header("Accept", "application/json")
            req.add_header(
                "User-Agent",
                "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)",
            )

            with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))
                email_id = resp_data.get("id") or ""
                logger.info(
                    "market_pulse: sent report to %s (id=%s)", recipient, email_id
                )
                result["recipients_sent"].append(recipient)

        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")[:200]
            except Exception:
                pass
            err_msg = f"HTTP {exc.code} sending to {recipient}: {error_body}"
            result["errors"].append(err_msg)
            logger.warning("market_pulse: %s", err_msg)

        except Exception as exc:
            err_msg = f"Error sending to {recipient}: {str(exc)}"
            result["errors"].append(err_msg)
            logger.warning("market_pulse: %s", err_msg)

    result["sent"] = len(result["recipients_sent"]) > 0
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 6. SCHEDULER (Background Daemon Thread)
# ═══════════════════════════════════════════════════════════════════════════════


def _scheduler_tick():
    """Execute one scheduler cycle: generate + send report."""
    global _scheduler_last_run, _scheduler_next_run, _scheduler_last_summary

    try:
        logger.info("market_pulse: scheduler tick -- generating report")
        report = generate_pulse_report()

        # Update state
        with _scheduler_lock:
            _scheduler_last_run = datetime.now(timezone.utc).isoformat()
            takeaways = report.get("key_takeaways") or []
            _scheduler_last_summary = takeaways[0] if takeaways else "Report generated"

        # Send if recipients configured
        recipients = []
        with _scheduler_lock:
            recipients = list(_scheduler_recipients)

        if recipients:
            send_result = send_pulse_report(report, recipients)
            logger.info(
                "market_pulse: scheduler sent to %d/%d recipients",
                len(send_result.get("recipients_sent") or []),
                len(recipients),
            )

    except Exception as exc:
        logger.error("market_pulse: scheduler tick failed: %s", exc)
        with _scheduler_lock:
            _scheduler_last_summary = f"Error: {str(exc)[:100]}"

    # Schedule next tick
    with _scheduler_lock:
        if _scheduler_running:
            interval_seconds = _scheduler_interval_hours * 3600
            _scheduler_next_run = (
                datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
            ).isoformat()
            # Cancel existing timer before creating new one to prevent leaks
            global _scheduler_timer
            if _scheduler_timer is not None:
                _scheduler_timer.cancel()
            timer = threading.Timer(interval_seconds, _scheduler_tick)
            timer.daemon = True
            timer.start()
            _scheduler_timer = timer


def start_pulse_scheduler(
    interval_hours: int = 168,
    recipients: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Start the background pulse report scheduler.

    Args:
        interval_hours: Hours between reports (default 168 = weekly).
        recipients: List of email addresses to send reports to.

    Returns:
        Dict with scheduler status.
    """
    global _scheduler_running, _scheduler_interval_hours
    global _scheduler_recipients, _scheduler_timer, _scheduler_next_run

    with _scheduler_lock:
        if _scheduler_running:
            return {
                "status": "already_running",
                "interval_hours": _scheduler_interval_hours,
                "recipients": list(_scheduler_recipients),
                "next_run": _scheduler_next_run,
            }

        _scheduler_running = True
        _scheduler_interval_hours = interval_hours
        _scheduler_recipients = list(recipients or [])
        _scheduler_next_run = (
            datetime.now(timezone.utc) + timedelta(hours=interval_hours)
        ).isoformat()

        # Cancel existing timer before creating new one to prevent leaks
        if _scheduler_timer is not None:
            _scheduler_timer.cancel()
        # Start first tick after a short delay (don't block startup)
        timer = threading.Timer(5.0, _scheduler_tick)
        timer.daemon = True
        timer.start()
        _scheduler_timer = timer

    logger.info(
        "market_pulse: scheduler started (interval=%dh, recipients=%d)",
        interval_hours,
        len(recipients or []),
    )

    return {
        "status": "started",
        "interval_hours": interval_hours,
        "recipients": list(recipients or []),
        "next_run": _scheduler_next_run,
    }


def stop_pulse_scheduler() -> Dict[str, Any]:
    """Stop the background pulse report scheduler."""
    global _scheduler_running, _scheduler_timer

    with _scheduler_lock:
        _scheduler_running = False
        if _scheduler_timer:
            _scheduler_timer.cancel()
            _scheduler_timer = None

    logger.info("market_pulse: scheduler stopped")
    return {"status": "stopped"}


def get_scheduler_status() -> Dict[str, Any]:
    """Return current scheduler state."""
    with _scheduler_lock:
        return {
            "running": _scheduler_running,
            "interval_hours": _scheduler_interval_hours,
            "last_run": _scheduler_last_run,
            "next_run": _scheduler_next_run if _scheduler_running else None,
            "last_report_summary": _scheduler_last_summary,
            "recipients": list(_scheduler_recipients),
            "reports_in_history": len(_report_history),
        }


def update_scheduler_config(
    interval_hours: Optional[int] = None,
    recipients: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Update scheduler configuration without restarting.

    Args:
        interval_hours: New interval (applied on next tick).
        recipients: New recipient list.

    Returns:
        Updated scheduler status.
    """
    global _scheduler_interval_hours, _scheduler_recipients

    with _scheduler_lock:
        if interval_hours is not None:
            _scheduler_interval_hours = interval_hours
        if recipients is not None:
            _scheduler_recipients = list(recipients)

    return get_scheduler_status()


# ═══════════════════════════════════════════════════════════════════════════════
# 7. PUBLIC API HELPERS (for app.py route handlers)
# ═══════════════════════════════════════════════════════════════════════════════


def get_latest_report() -> Optional[Dict[str, Any]]:
    """Return the most recent report from history, or None."""
    with _lock:
        if _report_history:
            return _report_history[0].get("full_report")
    return None


def get_report_history() -> List[Dict[str, Any]]:
    """Return list of past reports (metadata only, no full HTML)."""
    with _lock:
        return [
            {
                "report_id": r.get("report_id"),
                "report_date": r.get("report_date"),
                "period": r.get("period"),
                "generated_at": r.get("generated_at"),
                "key_takeaways": r.get("key_takeaways") or [],
            }
            for r in _report_history
        ]


def get_report_by_id(report_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a specific report by ID."""
    with _lock:
        for r in _report_history:
            if r.get("report_id") == report_id:
                return r.get("full_report")
    return None


def handle_pulse_api(
    path: str, method: str = "GET", body: Optional[Dict] = None
) -> Tuple[int, Dict[str, Any]]:
    """Unified API handler for /api/pulse/* routes.

    Called from app.py's MediaPlanHandler.

    Routes:
        GET  /api/pulse/latest        -- Get latest report data
        GET  /api/pulse/history       -- List past reports
        GET  /api/pulse/report/{id}   -- Get specific report
        GET  /api/pulse/status        -- Scheduler status
        POST /api/pulse/generate      -- Generate new report
        POST /api/pulse/send          -- Send report to recipients
        POST /api/pulse/scheduler     -- Start/stop/update scheduler

    Returns:
        (status_code, response_dict)
    """
    body = body or {}

    try:
        # GET /api/pulse/latest
        if path == "/api/pulse/latest" and method == "GET":
            report = get_latest_report()
            if report:
                return 200, {"ok": True, "report": report}
            # Generate one if none exists
            report = generate_pulse_report()
            return 200, {"ok": True, "report": report}

        # GET /api/pulse/latest/html
        if path == "/api/pulse/latest/html" and method == "GET":
            report = get_latest_report()
            if not report:
                report = generate_pulse_report()
            html_report = generate_pulse_html(report)
            return 200, {"ok": True, "html": html_report}

        # GET /api/pulse/history
        if path == "/api/pulse/history" and method == "GET":
            history = get_report_history()
            return 200, {"ok": True, "history": history}

        # GET /api/pulse/report/{id}
        if path.startswith("/api/pulse/report/") and method == "GET":
            report_id = path.split("/api/pulse/report/")[-1]
            report = get_report_by_id(report_id)
            if report:
                return 200, {"ok": True, "report": report}
            return 404, {"ok": False, "error": "Report not found"}

        # GET /api/pulse/status
        if path == "/api/pulse/status" and method == "GET":
            status = get_scheduler_status()
            return 200, {"ok": True, **status}

        # POST /api/pulse/generate
        if path == "/api/pulse/generate" and method == "POST":
            week_date = body.get("week_date")
            report = generate_pulse_report(week_date=week_date)
            return 200, {"ok": True, "report": report}

        # POST /api/pulse/send
        if path == "/api/pulse/send" and method == "POST":
            recipients = body.get("recipients") or []
            if isinstance(recipients, str):
                recipients = [r.strip() for r in recipients.split(",") if r.strip()]
            if not recipients:
                return 400, {"ok": False, "error": "No recipients specified"}

            # Get or generate report
            report = get_latest_report()
            if not report:
                report = generate_pulse_report()

            send_result = send_pulse_report(report, recipients)
            return 200, {"ok": True, **send_result}

        # POST /api/pulse/scheduler
        if path == "/api/pulse/scheduler" and method == "POST":
            action = body.get("action", "status")

            if action == "start":
                interval = body.get("interval_hours", 168)
                recipients = body.get("recipients") or []
                if isinstance(recipients, str):
                    recipients = [r.strip() for r in recipients.split(",") if r.strip()]
                result = start_pulse_scheduler(
                    interval_hours=interval, recipients=recipients
                )
                return 200, {"ok": True, **result}

            elif action == "stop":
                result = stop_pulse_scheduler()
                return 200, {"ok": True, **result}

            elif action == "update":
                interval = body.get("interval_hours")
                recipients = body.get("recipients")
                if isinstance(recipients, str):
                    recipients = [r.strip() for r in recipients.split(",") if r.strip()]
                result = update_scheduler_config(
                    interval_hours=interval, recipients=recipients
                )
                return 200, {"ok": True, **result}

            else:
                status = get_scheduler_status()
                return 200, {"ok": True, **status}

        return 404, {"ok": False, "error": f"Unknown pulse endpoint: {path}"}

    except Exception as exc:
        logger.error("market_pulse: API error on %s: %s", path, exc)
        return 500, {"ok": False, "error": str(exc)}
