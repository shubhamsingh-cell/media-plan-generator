"""Unified benchmark registry for recruitment advertising metrics.

Single source of truth for CPC, CPA, conversion rates, and cost-per-hire
benchmarks across all Nova AI Suite products.

Resolves the 6-file benchmark conflict where competitive_intel.py,
performance_tracker.py, market_intel_reports.py, audit_tool.py,
data_synthesizer.py, and data_orchestrator.py each had their own
hardcoded CPC/CPA values (e.g., Indeed CPC ranged from $0.45 to $0.85).

Live market data from Firecrawl scrapes (data/live_market_data.json) is
loaded at startup and overlaid on top of static benchmarks when available.

Usage:
    from benchmark_registry import get_channel_benchmark, get_all_benchmarks
    bench = get_channel_benchmark("indeed", industry="technology")
    # => {"cpc": 1.62, "cpa": 25.0, "cpc_adjusted": 2.27, ...}
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# LIVE MARKET DATA LOADER
# ═══════════════════════════════════════════════════════════════════════════════

_LIVE_DATA_PATH: Path = Path(__file__).parent / "data" / "live_market_data.json"
_live_market_data: dict[str, Any] = {}
_live_data_loaded: bool = False


def _load_live_data() -> dict[str, Any]:
    """Load live market data from Firecrawl scrape results.

    Reads data/live_market_data.json once and caches in module-level dict.
    Returns empty dict on any failure (file missing, corrupt JSON, etc.).
    """
    global _live_market_data, _live_data_loaded
    if _live_data_loaded:
        return _live_market_data
    _live_data_loaded = True
    try:
        if _LIVE_DATA_PATH.exists():
            with open(_LIVE_DATA_PATH, "r", encoding="utf-8") as f:
                _live_market_data = json.load(f)
                logger.info(
                    "Loaded live market data from %s (%d top-level keys)",
                    _LIVE_DATA_PATH.name,
                    len(_live_market_data),
                )
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load live market data: %s", e, exc_info=True)
        _live_market_data = {}
    return _live_market_data


# ═══════════════════════════════════════════════════════════════════════════════
# CONSOLIDATED BENCHMARKS -- SINGLE SOURCE OF TRUTH
# ═══════════════════════════════════════════════════════════════════════════════
#
# Values reconciled from 6 files + updated with 2025-2026 Firecrawl data.
# Q1 2026 refresh: Updated Google Ads benchmarks using WordStream/LOCALiQ 2025
#   report (Apr 2024 - Mar 2025 data, published 2025) and Appcast 2026 Recruitment
#   Marketing Benchmark Report. Note: general commercial CPC avg is $5.26 per
#   WordStream, but recruitment-specific CPC is lower (~$2.90 avg).
# Conflict resolution methodology:
#   - Indeed CPC: $1.62 (July-2026 refresh; see the per-entry comment below.
#     Prior $0.50 "typical" of 2026-03-26 retired by the cited July-2026 research.)
#   - Google Ads CPC/CPA: $5.81 / $67.36 (2026-10-01 refresh; LocaliQ 2026
#     Search Advertising Benchmarks, Career & Employment -- see the entry).
#   - Meta/Facebook CPC/CPA: $0.73 / $12.30 (2026-10-01 refresh; LocaliQ 2026
#     Facebook Advertising Benchmarks, Career & Employment -- see the entry).
#   - All other values: cross-referenced with live_market_data.json where available
# Coherence rule (2026-10-01, audit F §3.7): an entry's stated cpa should
#   equal cpc / apply_rate within ~15% unless its comment says why not;
#   tests/test_benchmark_constants_refresh.py enforces it for the entries
#   fixed on that date.
# Sources: WordStream 2025 Google Ads Benchmarks, Appcast 2026 Benchmark Report,
#   Joveo Google Ads 2025 first-party data. Updated 2026-03-26; Indeed/LinkedIn
#   CPCs refreshed 2026-07-16 to match the cited July-2026 research shipped to
#   data/recruitment_benchmarks_comprehensive_2026.json (cpc_by_platform),
#   data/channel_benchmarks_seed.json, and data/live_market_data.json job_boards.

CHANNEL_BENCHMARKS: dict[str, dict[str, Any]] = {
    # Indeed CPC $1.62 = geometric mean of the cited $0.97-$2.71 typical US-role
    # band (Pin.com citing ThePricer 2026/ShiftNow 2025/Job Board Doctor May 2025;
    # Indeed publishes no official figure). Same derivation as
    # live_market_data.json avg_cpc_typical and the KB cpc_by_platform entry
    # refreshed 2026-07-16. Updated: 2026-07-16.
    "indeed": {
        "cpc": 1.62,
        "cpa": 25.0,
        "apply_rate": 0.08,
        "ctr": 0.040,
        "cpm": 5.00,
        "quality_score": 7.5,
        "monthly_reach": 250_000_000,
        "pricing_model": "CPC + subscription",
        "category": "major_job_board",
    },
    # LinkedIn CPA: $30-$90 for Sponsored Jobs (US avg ~$45). Full CPA factors
    # in apply rates (3-5%). Source: Postiv.ai, SpeedWork Social, Recruitics
    # (2025-2026 data). CPA updated: 2026-04-07.
    # LinkedIn CPC $2.60 = geometric mean of the cited $1.50-$4.50 Promoted Jobs
    # band (Pin.com; LinkedIn's own FAQs confirm the auction model but publish
    # no figure). Prior 5.26 blended sponsored-content CPC ($5-$12) into what
    # should be a job-ads figure -- retired by the July-2026 research (see KB
    # cpc_by_platform refreshed_2026_07_16 note). CPC updated: 2026-07-16.
    # 2026-10-01 (audit F §3.7): stated CPA was 45.0 against an implied
    # cpc / apply_rate of 2.60 / 0.035 = 74.29 (-39%). CPA is now that
    # derived value -- still inside the cited $30-$90 CPA band above, while
    # the alternative (apply_rate = 2.60 / 45 = 5.8%) would leave the cited
    # 3-5% apply-rate band.
    "linkedin": {
        "cpc": 2.60,
        "cpa": 74.29,
        "cpa_min": 30.0,
        "cpa_max": 90.0,
        "apply_rate": 0.035,
        "ctr": 0.008,
        "cpm": 35.00,
        "quality_score": 8.5,
        "monthly_reach": 1_000_000_000,
        "pricing_model": "CPC + subscription",
        "category": "professional_network",
    },
    "ziprecruiter": {
        "cpc": 1.50,
        "cpa": 35.0,
        "apply_rate": 0.06,
        "ctr": 0.035,
        "cpm": 8.00,
        "quality_score": 7.0,
        "monthly_reach": 30_000_000,
        "pricing_model": "subscription + CPC",
        "category": "major_job_board",
    },
    "glassdoor": {
        "cpc": 1.20,
        "cpa": 40.0,
        "apply_rate": 0.05,
        "ctr": 0.030,
        "cpm": 7.00,
        "quality_score": 7.8,
        "monthly_reach": 55_000_000,
        "pricing_model": "subscription + CPC",
        "category": "employer_brand",
    },
    # Google Ads -- 2026-10-01 refresh. Source: LocaliQ "Search Advertising
    # Benchmarks for Every Industry [2026 Data]", Career & Employment row
    # (https://localiq.com/blog/search-advertising-benchmarks/, last updated
    # 2026-06-01, fetched 2026-10-01): avg CPC $5.81, avg cost per lead
    # $67.36, CTR 5.88%, CVR 3.05%. Prior 2.90 / 48.0: the 48.0 CPA matched
    # WordStream's Aug 2017-Jan 2018 Employment Services figure.
    # NOT CPC/apply_rate-coherent, deliberately: the source's own triple is
    # internally inconsistent (5.81 / 3.05% = ~$190 per lead, not $67.36; the
    # page prints the identical $67.36 for Health & Fitness), so neither a
    # derived CPA nor a derived apply rate would be a sourced number. The
    # printed CPC and CPL are used as published; apply_rate stays 0.04.
    "google_ads": {
        "cpc": 5.81,
        "cpa": 67.36,
        "apply_rate": 0.04,
        "ctr": 0.045,
        "cpm": 11.00,
        "quality_score": 6.5,
        "monthly_reach": 8_500_000_000,
        "pricing_model": "CPC/CPM",
        "category": "search_engine",
    },
    # Alias: many files use "google_search" instead of "google_ads"
    "google_search": {
        "cpc": 5.81,
        "cpa": 67.36,
        "apply_rate": 0.04,
        "ctr": 0.045,
        "cpm": 11.00,
        "quality_score": 6.5,
        "monthly_reach": 8_500_000_000,
        "pricing_model": "CPC/CPM",
        "category": "search_engine",
    },
    # Meta/Facebook -- 2026-10-01 refresh. Source: LocaliQ "Facebook
    # Advertising Benchmarks" 2026, Career & Employment row, leads objective
    # (https://localiq.com/blog/facebook-advertising-benchmarks/, last
    # updated 2026-09-23, fetched 2026-10-01): CPC $0.73, cost per lead
    # $12.30, CVR 5.38%. apply_rate = that 5.38% click-to-lead rate, so the
    # entry is coherent: 0.73 / 0.0538 = 13.57 vs 12.30 (-9%). Prior
    # 1.86 / 32.0 / 0.025 (WordStream 2025) implied a $74.40 CPA.
    "meta_facebook": {
        "cpc": 0.73,
        "cpa": 12.30,
        "apply_rate": 0.0538,
        "ctr": 0.013,
        "cpm": 8.20,
        "quality_score": 5.5,
        "monthly_reach": 3_000_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    # Alias: some files use just "meta" -- same 2026-10-01 LocaliQ values.
    "meta": {
        "cpc": 0.73,
        "cpa": 12.30,
        "apply_rate": 0.0538,
        "ctr": 0.013,
        "cpm": 8.20,
        "quality_score": 5.5,
        "monthly_reach": 3_000_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    "meta_instagram": {
        "cpc": 1.50,
        "cpa": 35.0,
        "apply_rate": 0.02,
        "ctr": 0.010,
        "cpm": 8.00,
        "quality_score": 5.0,
        "monthly_reach": 2_000_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    "instagram": {
        "cpc": 1.50,
        "cpa": 35.0,
        "apply_rate": 0.02,
        "ctr": 0.010,
        "cpm": 8.00,
        "quality_score": 5.0,
        "monthly_reach": 2_000_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    "monster": {
        "cpc": 1.00,
        "cpa": 45.0,
        "apply_rate": 0.04,
        "ctr": 0.025,
        "cpm": 6.00,
        "quality_score": 6.0,
        "monthly_reach": 8_000_000,
        "pricing_model": "subscription",
        "category": "major_job_board",
    },
    # 2026-10-01 (audit F §3.7): no cited CPA exists for CareerBuilder
    # (its employer pricing now 301-redirects to Monster+; see
    # data/channel_benchmarks_seed.json). Stated 50.0 vs implied
    # 0.80 / 0.035 = 22.86 (+119%); CPA is now the derived figure.
    "careerbuilder": {
        "cpc": 0.80,
        "cpa": 22.86,
        "apply_rate": 0.035,
        "ctr": 0.022,
        "cpm": 5.50,
        "quality_score": 5.5,
        "monthly_reach": 6_000_000,
        "pricing_model": "subscription",
        "category": "major_job_board",
    },
    # 2026-10-01 (audit F §3.7): stated CPA 22.0 vs implied 0.63 / 0.07 =
    # 9.00 (+144%). CPA is now the derived 9.00, inside the Recruitonomics
    # $7-16 per-application sector band cited in
    # data/channel_benchmarks_seed.json (Indeed entry notes).
    "programmatic": {
        "cpc": 0.63,
        "cpa": 9.00,
        "apply_rate": 0.07,
        "ctr": 0.025,
        "cpm": 4.50,
        "quality_score": 7.0,
        "monthly_reach": 500_000_000,
        "pricing_model": "CPC/CPA",
        "category": "programmatic",
    },
    "tiktok": {
        "cpc": 1.00,
        "cpa": 28.0,
        "apply_rate": 0.015,
        "ctr": 0.012,
        "cpm": 6.00,
        "quality_score": 4.5,
        "monthly_reach": 1_500_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    "twitter_x": {
        "cpc": 2.00,
        "cpa": 55.0,
        "apply_rate": 0.02,
        "ctr": 0.010,
        "cpm": 9.00,
        "quality_score": 5.0,
        "monthly_reach": 600_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    # Craigslist -- Source: CG Automation 98,665 posts across 397 US locations
    # (Dec 2025-Mar 2026). CL uses flat post pricing ($3-$75 per post depending
    # on market and category; many gig posts $0-$5). CPC/CPA computed from
    # avg_media_cost=$0.21, avg_clicks=8.6, avg_applies=5.1 per post.
    # General impression-to-apply rates: professional 12%, hourly/gig 18%,
    # blue collar/trades 25%. Click-to-apply ~59% (CG Apex client-specific).
    # Updated 2026-04-08 from craigslist_performance_benchmarks.json.
    "craigslist": {
        "cpc": 0.024,
        "cpa": 0.041,
        "cpa_min": 0.02,
        "cpa_max": 5.00,
        "apply_rate": 0.18,
        "ctr": 0.102,
        "cpm": 2.50,
        "quality_score": 6.0,
        "monthly_reach": 50_000_000,
        "pricing_model": "flat_post_fee",
        "category": "classified_board",
        "data_source_detail": "CG Automation 98K posts (Dec 2025-Mar 2026)",
        "avg_post_cost": 0.21,
        "avg_impressions_per_post": 84,
        "avg_clicks_per_post": 8.6,
        "avg_applies_per_post": 5.1,
        "click_to_apply_rate": 0.59,
        "top_categories": [
            "labor gigs",
            "domestic gigs",
            "creative gigs",
            "event gigs",
            "talent gigs",
        ],
    },
}


# Industry multipliers for CPC adjustment
INDUSTRY_MULTIPLIERS: dict[str, float] = {
    "technology": 1.4,
    "tech_engineering": 1.4,
    "healthcare": 1.6,
    "healthcare_medical": 1.6,
    "finance": 1.3,
    "finance_banking": 1.3,
    "retail": 0.7,
    "retail_consumer": 0.65,
    "manufacturing": 0.8,
    "hospitality": 0.6,
    "hospitality_travel": 0.7,
    "education": 0.75,
    "government": 0.85,
    "construction": 0.9,
    "construction_real_estate": 0.85,
    "logistics": 0.7,
    "logistics_supply_chain": 0.8,
    "legal_services": 1.5,
    "aerospace_defense": 1.3,
    "pharma_biotech": 1.35,
    "blue_collar_trades": 0.7,
    "general_entry_level": 0.6,
    "food_beverage": 0.6,
    "overall": 1.0,
}


# Cost per hire by industry (live market data first; see get_cost_per_hire).
# "overall" = SHRM 2025 Benchmarking nonexecutive AVERAGE cost per hire,
# $5,475 (SHRM press release 2025-10-15,
# https://www.shrm.org/about/press-room/shrm-releases-2025-benchmarking-reports--how-does-your-organizat,
# fetched 2026-10-01). The prior 4750 was labelled "SHRM 2026" but was the
# average of an older $4,700 SHRM figure and a secondary $4,800; SHRM's 2026
# benchmarking pages publish no dollar cost per hire.
COST_PER_HIRE: dict[str, float] = {
    "technology": 6200.0,
    "healthcare": 9000.0,
    "finance": 5500.0,
    "retail": 2700.0,
    "manufacturing": 3800.0,
    "hospitality": 2200.0,
    "education": 3200.0,
    "cybersecurity": 10000.0,
    "data_science": 10000.0,
    "engineering": 6200.0,
    "overall": 5475.0,
}


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def get_channel_benchmark(
    channel: str,
    industry: str = "overall",
) -> dict[str, Any]:
    """Get CPC/CPA benchmarks for a channel, adjusted by industry.

    Checks live Firecrawl data first (data/live_market_data.json), then
    falls back to static CHANNEL_BENCHMARKS. Applies industry multiplier
    to produce cpc_adjusted and cpa_adjusted values.

    Args:
        channel: Platform name (e.g., "indeed", "google_search", "meta_facebook").
                 Spaces and hyphens are normalized to underscores.
        industry: Industry key for multiplier adjustment. Defaults to "overall" (1.0x).

    Returns:
        Dict with keys: cpc, cpa, cpc_adjusted, cpa_adjusted, apply_rate,
        ctr, cpm, quality_score, monthly_reach, pricing_model, category,
        data_source, industry.
    """
    live = _load_live_data()
    live_boards: dict[str, Any] = live.get("job_boards") or {}

    # Normalize channel name
    channel_key = channel.lower().replace(" ", "_").replace("-", "_")

    base = CHANNEL_BENCHMARKS.get(channel_key)
    if base is None:
        base = CHANNEL_BENCHMARKS.get("programmatic", {})
    multiplier = INDUSTRY_MULTIPLIERS.get(industry.lower(), 1.0)

    # Overlay live data if available
    live_channel: dict[str, Any] = live_boards.get(channel_key) or {}
    # Live data uses "avg_cpc_typical" for main boards, "avg_cpc" for industries
    live_cpc = live_channel.get("avg_cpc_typical") or live_channel.get("avg_cpc")

    result: dict[str, Any] = {**base}
    if live_cpc and isinstance(live_cpc, (int, float)) and live_cpc > 0:
        result["cpc"] = float(live_cpc)
        result["data_source"] = "live_firecrawl"
    else:
        result["data_source"] = "benchmark"

    result["cpc_adjusted"] = round(result["cpc"] * multiplier, 2)
    cpa_base = result.get("cpa") or 30.0
    result["cpa_adjusted"] = round(cpa_base * multiplier, 2)
    result["industry"] = industry

    return result


def get_all_benchmarks(
    industry: str = "overall",
) -> dict[str, dict[str, Any]]:
    """Get all channel benchmarks adjusted for an industry.

    Args:
        industry: Industry key for multiplier adjustment.

    Returns:
        Dict keyed by channel name, each value a benchmark dict from
        get_channel_benchmark.
    """
    return {ch: get_channel_benchmark(ch, industry) for ch in CHANNEL_BENCHMARKS}


def get_cost_per_hire(industry: str = "overall") -> float:
    """Get average cost per hire for an industry.

    Checks live Firecrawl data first, then falls back to static COST_PER_HIRE.

    Args:
        industry: Industry key (e.g., "technology", "healthcare").

    Returns:
        Cost per hire in USD as a float.
    """
    live = _load_live_data()
    live_benchmarks: dict[str, Any] = live.get("industry_benchmarks") or {}
    live_industry: dict[str, Any] = live_benchmarks.get(industry.lower()) or {}
    live_cph = live_industry.get("avg_cost_per_hire")

    if live_cph and isinstance(live_cph, (int, float)) and live_cph > 0:
        return float(live_cph)
    return COST_PER_HIRE.get(industry.lower(), COST_PER_HIRE["overall"])


def get_industry_apply_rate(industry: str) -> float | None:
    """Industry apply rate (fraction, e.g. 0.032) from
    data/live_market_data.json ``industry_benchmarks[industry].apply_rate_pct``
    (keys: technology, healthcare, retail, finance, manufacturing,
    hospitality, ...), or ``None`` when absent. Never raises."""
    live = _load_live_data()
    entry = (live.get("industry_benchmarks") or {}).get(str(industry or "").lower())
    if not isinstance(entry, dict):
        return None
    pct = entry.get("apply_rate_pct")
    if isinstance(pct, (int, float)) and not isinstance(pct, bool) and 0 < pct < 100:
        return float(pct) / 100.0
    return None


def get_benchmark_value(
    channel: str,
    metric: str,
    industry: str = "overall",
) -> float:
    """Get a single benchmark metric value for a channel.

    Convenience function used by audit_tool and performance_tracker
    as a drop-in replacement for their _fallback_benchmark functions.

    Args:
        channel: Platform name.
        metric: One of "cpc", "cpa", "ctr", "cpm".
        industry: Industry key for multiplier (only applied to cpc/cpa).

    Returns:
        The benchmark value as a float. Returns 1.0 if metric not found.
    """
    bench = get_channel_benchmark(channel, industry)
    if metric in ("cpc",):
        return bench.get("cpc_adjusted", bench.get("cpc", 1.0))
    if metric in ("cpa",):
        return bench.get("cpa_adjusted", bench.get("cpa", 30.0))
    return bench.get(metric, 1.0)
