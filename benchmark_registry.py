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
    # => {"cpc": 0.50, "cpa": 25.0, "cpc_adjusted": 0.70, ...}
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

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
# Conflict resolution methodology:
#   - Indeed CPC: $0.50 (live Firecrawl data confirms $0.10-$5.00 range, typical $0.50)
#   - Google Ads CPC: $2.69 (data_synthesizer median, closest to Appcast benchmark)
#   - Meta/Facebook CPC: $1.72 (data_synthesizer median, reconciled with live data)
#   - All other values: cross-referenced with live_market_data.json where available

CHANNEL_BENCHMARKS: dict[str, dict[str, Any]] = {
    "indeed": {
        "cpc": 0.50,
        "cpa": 25.0,
        "apply_rate": 0.08,
        "ctr": 0.040,
        "cpm": 5.00,
        "quality_score": 7.5,
        "monthly_reach": 250_000_000,
        "pricing_model": "CPC + subscription",
        "category": "major_job_board",
    },
    "linkedin": {
        "cpc": 5.26,
        "cpa": 75.0,
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
    "google_ads": {
        "cpc": 2.69,
        "cpa": 45.0,
        "apply_rate": 0.04,
        "ctr": 0.042,
        "cpm": 10.00,
        "quality_score": 6.5,
        "monthly_reach": 8_500_000_000,
        "pricing_model": "CPC/CPM",
        "category": "search_engine",
    },
    # Alias: many files use "google_search" instead of "google_ads"
    "google_search": {
        "cpc": 2.69,
        "cpa": 45.0,
        "apply_rate": 0.04,
        "ctr": 0.042,
        "cpm": 10.00,
        "quality_score": 6.5,
        "monthly_reach": 8_500_000_000,
        "pricing_model": "CPC/CPM",
        "category": "search_engine",
    },
    "meta_facebook": {
        "cpc": 1.72,
        "cpa": 30.0,
        "apply_rate": 0.025,
        "ctr": 0.012,
        "cpm": 7.50,
        "quality_score": 5.5,
        "monthly_reach": 3_000_000_000,
        "pricing_model": "CPC/CPM",
        "category": "social_media",
    },
    # Alias: some files use just "meta"
    "meta": {
        "cpc": 1.72,
        "cpa": 30.0,
        "apply_rate": 0.025,
        "ctr": 0.012,
        "cpm": 7.50,
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
    "careerbuilder": {
        "cpc": 0.80,
        "cpa": 50.0,
        "apply_rate": 0.035,
        "ctr": 0.022,
        "cpm": 5.50,
        "quality_score": 5.5,
        "monthly_reach": 6_000_000,
        "pricing_model": "subscription",
        "category": "major_job_board",
    },
    "programmatic": {
        "cpc": 0.63,
        "cpa": 22.0,
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


# Cost per hire by industry (from live market data + SHRM 2026)
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
    "overall": 4750.0,
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
