"""
ROI Projector -- Confidence-interval recruitment ROI projections.

Given a channel, budget, industry, role, and locations, produces pessimistic /
expected / optimistic hire forecasts with confidence scoring.  Designed for
Cindy's ask: "If you spend $10K on Indeed, expect 30-40 hires (85% confidence)."

Also provides multi-channel comparison with optimal budget allocation.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_benchmarks_data: Optional[Dict] = None
_intl_benchmarks_data: Optional[Dict] = None


def _load_intl_benchmarks() -> Dict:
    """Load international benchmarks JSON once, cache in module global."""
    global _intl_benchmarks_data
    if _intl_benchmarks_data is not None:
        return _intl_benchmarks_data
    _path = os.path.join(
        os.path.dirname(__file__),
        "data",
        "international_benchmarks_2026.json",
    )
    try:
        with open(_path, "r", encoding="utf-8") as fh:
            _intl_benchmarks_data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.error("Failed to load intl benchmarks: %s", exc, exc_info=True)
        _intl_benchmarks_data = {}
    return _intl_benchmarks_data


def _get_intl_cpc_for_location(location: str) -> Optional[Tuple[float, float]]:
    """Return (median_cpc_usd, median_apply_rate) from intl benchmarks for a location.

    Returns None if no match found.
    """
    intl = _load_intl_benchmarks()
    countries = intl.get("countries", {})
    loc_lower = (location or "").lower().strip()
    for _ck, _cv in countries.items():
        _cname = (_cv.get("name") or "").lower()
        if _ck in loc_lower or _cname in loc_lower or loc_lower in _cname:
            platforms = _cv.get("platforms", [])
            if not platforms:
                return None
            # Weighted average CPC/apply_rate from top platforms
            _cpcs = []
            _ars = []
            for p in platforms[:5]:
                cpc_usd = p.get("cpc_usd", {})
                if isinstance(cpc_usd, dict) and cpc_usd.get("median"):
                    _cpcs.append(cpc_usd["median"])
                ar = p.get("apply_rate_pct", 0)
                if ar > 0:
                    _ars.append(ar / 100.0)
            if _cpcs:
                avg_cpc = sum(_cpcs) / len(_cpcs)
                avg_ar = sum(_ars) / len(_ars) if _ars else 0.05
                return (round(avg_cpc, 2), round(avg_ar, 3))
            return None
    return None


def _load_benchmarks() -> Dict:
    """Load recruitment benchmarks JSON once, cache in module global."""
    global _benchmarks_data
    if _benchmarks_data is not None:
        return _benchmarks_data
    _path = os.path.join(
        os.path.dirname(__file__),
        "data",
        "recruitment_benchmarks_comprehensive_2026.json",
    )
    try:
        with open(_path, "r", encoding="utf-8") as fh:
            _benchmarks_data = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.error("Failed to load recruitment benchmarks: %s", exc, exc_info=True)
        _benchmarks_data = {}
    return _benchmarks_data


def _get_metro_coli(location: str) -> float:
    """Return cost-of-living index for a location (100 = US average)."""
    try:
        from research import METRO_DATA
    except (ImportError, TypeError):
        return 100.0
    loc_lower = location.lower().strip().replace(",", "").split()[0] if location else ""
    for key, data in METRO_DATA.items():
        if loc_lower and (loc_lower in key.lower() or key.lower() in loc_lower):
            return float(data.get("coli", 100))
    return 100.0


# ---------------------------------------------------------------------------
# Channel benchmarks: CPC (USD) and click-to-apply rate (decimal)
# ---------------------------------------------------------------------------
# fmt: off
_CHANNELS: Dict[str, Tuple[float, float]] = {
    # key: (cpc_usd, click_to_apply_rate)
    "indeed": (0.92, 0.050), "linkedin": (5.26, 0.030),
    "ziprecruiter": (1.50, 0.070), "glassdoor": (5.00, 0.040),
    "careerbuilder": (2.00, 0.035), "craigslist": (0.50, 0.120),
    "facebook": (1.11, 0.040), "meta": (1.11, 0.040),
    "google": (3.20, 0.045), "programmatic": (0.65, 0.052),
    "job boards": (0.92, 0.060), "social media": (1.50, 0.035),
    "niche boards": (1.40, 0.100), "regional boards": (0.75, 0.055),
}

# Industry benchmarks: (apps_per_hire, cpa_lo, cpa_hi, cph_lo, cph_hi, ttf_lo, ttf_hi)
_INDUSTRIES: Dict[str, Tuple[int, float, float, int, int, int, int]] = {
    "healthcare":       (47,  15, 45,  5000, 12000, 40, 55),
    "technology":       (191, 12, 35,  6000, 14000, 35, 50),
    "finance":          (150, 15, 40,  5000, 12000, 30, 45),
    "retail":           (120,  5, 15,  2700,  4000, 15, 30),
    "hospitality":      (120,  5, 20,  2500,  4000, 10, 25),
    "logistics":        (80,  10, 30,  3500,  8000, 20, 35),
    "gig":              (40,   3, 12,   800,  2500,  5, 15),
    "blue_collar":      (60,   3, 12,  3500,  5600, 15, 30),
    "pharma":           (160, 15, 40,  8000, 18000, 45, 60),
    "energy":           (140, 12, 30,  5000, 10000, 30, 50),
    "education":        (130,  8, 22,  3500,  6000, 25, 40),
    "insurance":        (150, 15, 40,  5000, 10000, 30, 45),
    "automotive":       (100, 10, 25,  5600,  9000, 25, 40),
    "aerospace":        (170, 12, 35,  6000, 14000, 35, 55),
    "legal":            (160, 15, 40,  5000, 11000, 30, 50),
    "marketing":        (140, 10, 25,  4000,  7000, 20, 35),
    "general":          (180, 10, 30,  4000,  8000, 25, 42),
}
# Aliases for common variations
_INDUSTRY_ALIASES: Dict[str, str] = {
    "healthcare_medical": "healthcare", "tech_engineering": "technology",
    "tech": "technology", "finance_banking": "finance",
    "retail_consumer": "retail", "hospitality_travel": "hospitality",
    "logistics_supply_chain": "logistics", "light_industrial": "gig",
    "blue_collar_trades": "blue_collar", "pharma_biotech": "pharma",
    "energy_utilities": "energy", "aerospace_defense": "aerospace",
    "legal_services": "legal",
}
# fmt: on


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d else default


def _norm(s: str) -> str:
    return (
        (s or "").strip().lower().replace(" ", "_").replace("/", "_").replace("-", "_")
    )


def _get_channel(ch: str) -> Tuple[float, float]:
    """Return (cpc, apply_rate) for a channel."""
    k = _norm(ch)
    if k in _CHANNELS:
        return _CHANNELS[k]
    for key, val in _CHANNELS.items():
        if k in key or key in k:
            return val
    return (1.00, 0.05)


def _get_industry(ind: str) -> Tuple[int, float, float, int, int, int, int]:
    """Return industry benchmark tuple."""
    k = _norm(ind)
    resolved = _INDUSTRY_ALIASES.get(k, k)
    if resolved in _INDUSTRIES:
        return _INDUSTRIES[resolved]
    for key, val in _INDUSTRIES.items():
        if k in key or key in k:
            return val
    return _INDUSTRIES["general"]


def _location_mult(locations: Optional[List[str]]) -> float:
    if not locations:
        return 1.0
    colis = [_get_metro_coli(loc) for loc in locations]
    return (sum(colis) / len(colis)) / 100.0 if colis else 1.0


def _confidence(
    ch: str, ind: str, role: Optional[str], locs: Optional[List[str]]
) -> Tuple[float, str, str]:
    """Compute confidence (score, label, basis_text)."""
    score = 0.50
    k_ch, k_ind = _norm(ch), _norm(ind)
    resolved_ind = _INDUSTRY_ALIASES.get(k_ind, k_ind)
    if k_ch in _CHANNELS:
        score += 0.15
    if resolved_ind in _INDUSTRIES:
        score += 0.15
    if resolved_ind in _INDUSTRIES and resolved_ind != "general":
        score += 0.10
    if locs and any(_get_metro_coli(l) != 100.0 for l in locs):
        score += 0.05
    if role:
        try:
            from budget_engine import ROLE_TIER_MULTIPLIERS

            if any(role.lower() in k.lower() for k in ROLE_TIER_MULTIPLIERS):
                score += 0.05
        except ImportError:
            pass
    score = min(score, 0.95)
    bm = _load_benchmarks()
    src_count = bm.get("_metadata", {}).get("sources_analyzed", 28) if bm else 28
    aph = _get_industry(ind)[0]
    parts = []
    if resolved_ind in _INDUSTRIES and resolved_ind != "general":
        parts.append(f"industry-specific funnel data ({aph} apps/hire)")
    if k_ch in _CHANNELS:
        parts.append("platform CPC benchmarks")
    parts.append(f"{src_count} industry sources")
    label = "High" if score >= 0.80 else ("Medium" if score >= 0.60 else "Low")
    return (round(score, 2), label, f"Based on {', '.join(parts)}")


# ---------------------------------------------------------------------------
# Core projection
# ---------------------------------------------------------------------------


def project_roi(
    channel: str,
    budget: float,
    industry: str = "general",
    role: Optional[str] = None,
    locations: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Project ROI for a single channel/budget/industry combination."""
    if budget <= 0:
        return {"error": "Budget must be positive", "channel": channel}

    cpc, apply_rate = _get_channel(channel)
    aph, cpa_lo, cpa_hi, cph_lo, cph_hi, ttf_lo, ttf_hi = _get_industry(industry)

    # ── International location override: use intl benchmark CPC/apply rate ──
    _has_intl_data = False
    if locations:
        _intl_cpcs: List[float] = []
        _intl_ars: List[float] = []
        for loc in locations:
            intl_data = _get_intl_cpc_for_location(loc)
            if intl_data:
                _intl_cpcs.append(intl_data[0])
                _intl_ars.append(intl_data[1])
        if _intl_cpcs:
            _has_intl_data = True
            # Blend: if mixed US/intl locations, average both
            _us_count = len(locations) - len(_intl_cpcs)
            if _us_count > 0:
                # Blend US CPC with intl CPC
                _us_cpc = cpc
                _intl_avg_cpc = sum(_intl_cpcs) / len(_intl_cpcs)
                cpc = (_us_cpc * _us_count + _intl_avg_cpc * len(_intl_cpcs)) / len(
                    locations
                )
                _intl_avg_ar = sum(_intl_ars) / len(_intl_ars)
                apply_rate = (
                    apply_rate * _us_count + _intl_avg_ar * len(_intl_ars)
                ) / len(locations)
            else:
                # All international
                cpc = sum(_intl_cpcs) / len(_intl_cpcs)
                apply_rate = (
                    sum(_intl_ars) / len(_intl_ars) if _intl_ars else apply_rate
                )

    loc_m = _location_mult(locations)
    cpc_adj = cpc * loc_m
    cpa_lo_a, cpa_hi_a = cpa_lo * loc_m, cpa_hi * loc_m

    clicks = _safe_div(budget, cpc_adj)
    apps = clicks * apply_rate
    hires = _safe_div(apps, aph)

    var = 0.25 if _norm(channel) in ("indeed", "linkedin", "ziprecruiter") else 0.30
    h_pes, h_opt = max(0, hires * (1 - var)), hires * (1 + var)

    cpa_exp = max(cpa_lo_a, min(cpa_hi_a, _safe_div(budget, apps, cpa_hi_a)))
    cpa_pes, cpa_opt = cpa_exp * (1 + var), max(0.01, cpa_exp * (1 - var))

    cph_exp = max(cph_lo * 0.5, _safe_div(budget, hires, cph_hi))
    cph_pes = _safe_div(budget, max(h_pes, 0.1), cph_hi)
    cph_opt = _safe_div(budget, max(h_opt, 0.1), cph_lo)

    avg_cph_mkt = (cph_lo + cph_hi) / 2
    roi_ratio = round(_safe_div(avg_cph_mkt, cph_exp, 1.0), 1)
    brk_days = max(
        7, min(90, round(_safe_div((ttf_lo + ttf_hi) / 2, max(hires, 0.1), 30)))
    )

    conf_score, conf_label, conf_basis = _confidence(channel, industry, role, locations)
    # Slightly reduce confidence for international projections (less data certainty)
    if _has_intl_data:
        conf_score = max(0.40, conf_score - 0.05)
        conf_label = (
            "High"
            if conf_score >= 0.80
            else ("Medium" if conf_score >= 0.60 else "Low")
        )
        conf_basis += " | international benchmarks (38 countries)"

    # Recommendation
    h_int = max(1, round(hires))
    strength = (
        "Strong investment"
        if roi_ratio >= 2
        else ("Reasonable investment" if roi_ratio >= 1 else "Below-average return")
    )
    if cph_exp < avg_cph_mkt * 0.8:
        eff = f"CPH is {round((1 - cph_exp / avg_cph_mkt) * 100)}% below market average"
    elif cph_exp > avg_cph_mkt * 1.2:
        eff = f"CPH is {round((cph_exp / avg_cph_mkt - 1) * 100)}% above market average -- consider diversifying channels"
    else:
        eff = "CPH is in line with market benchmarks"
    rec = (
        f"{strength}. {channel} is projected to deliver ~{h_int} hires "
        f"at ${budget:,.0f} for {industry.replace('_', ' ').title()}. {eff}. Confidence: {conf_label}."
    )

    return {
        "channel": channel,
        "budget": budget,
        "industry": industry,
        "role": role,
        "locations": locations or [],
        "projections": {
            "pessimistic": {
                "hires": max(1, round(h_pes)),
                "cpa": round(cpa_pes, 2),
                "time_to_fill": ttf_hi,
            },
            "expected": {
                "hires": max(1, round(hires)),
                "cpa": round(cpa_exp, 2),
                "time_to_fill": round((ttf_lo + ttf_hi) / 2),
            },
            "optimistic": {
                "hires": max(1, round(h_opt)),
                "cpa": round(cpa_opt, 2),
                "time_to_fill": ttf_lo,
            },
        },
        "confidence": {"level": conf_score, "label": conf_label, "basis": conf_basis},
        "roi_metrics": {
            "cost_per_hire": {
                "low": round(cph_opt),
                "expected": round(cph_exp),
                "high": round(cph_pes),
            },
            "roi_ratio": roi_ratio,
            "breakeven_days": brk_days,
        },
        "funnel": {
            "clicks": round(clicks),
            "applications": round(apps),
            "apps_per_hire_benchmark": aph,
            "cpc_used": round(cpc_adj, 2),
            "apply_rate_used": round(apply_rate, 3),
            "location_cost_multiplier": round(loc_m, 2),
        },
        "recommendation": rec,
        "source": "Nova ROI Projector (28 industry sources, 302M+ data points)",
    }


# ---------------------------------------------------------------------------
# Multi-channel comparison / optimal allocation
# ---------------------------------------------------------------------------


def compare_channel_roi(
    channels: Optional[List[str]] = None,
    total_budget: float = 10_000,
    industry: str = "general",
    role: Optional[str] = None,
    locations: Optional[List[str]] = None,
    optimize_for: str = "hires",
) -> Dict[str, Any]:
    """Compare ROI across channels and suggest optimal budget allocation."""
    if total_budget <= 0:
        return {"error": "Budget must be positive"}
    if not channels:
        channels = ["Indeed", "LinkedIn", "Programmatic", "Facebook", "Google"]

    eq_bud = total_budget / len(channels)
    ch_proj = {
        ch: project_roi(ch, eq_bud, industry, role, locations) for ch in channels
    }

    if optimize_for == "cost":
        ranked = sorted(
            channels,
            key=lambda c: ch_proj[c]
            .get("roi_metrics", {})
            .get("cost_per_hire", {})
            .get("expected", 99999),
        )
    else:
        ranked = sorted(
            channels,
            key=lambda c: ch_proj[c]
            .get("projections", {})
            .get("expected", {})
            .get("hires", 0),
            reverse=True,
        )

    eff_scores: Dict[str, float] = {}
    for ch in channels:
        p = ch_proj[ch]
        h = p.get("projections", {}).get("expected", {}).get("hires", 1)
        cph = p.get("roi_metrics", {}).get("cost_per_hire", {}).get("expected", 10000)
        eff_scores[ch] = max(
            _safe_div(1.0, cph) if optimize_for == "cost" else _safe_div(h, eq_bud),
            0.0001,
        )

    total_eff = sum(eff_scores.values())
    alloc: Dict[str, Dict[str, Any]] = {}
    tot_h_opt = tot_h_eq = 0
    min_ch = min(500, total_budget / len(channels) * 0.5)

    for ch in channels:
        w = _safe_div(eff_scores[ch], total_eff, 1.0 / len(channels))
        ab = max(min_ch, round(total_budget * w, 2))
        op = project_roi(ch, ab, industry, role, locations)
        oh = op.get("projections", {}).get("expected", {}).get("hires", 0)
        eh = ch_proj[ch].get("projections", {}).get("expected", {}).get("hires", 0)
        alloc[ch] = {
            "budget": round(ab),
            "pct_of_total": round(w * 100, 1),
            "projected_hires": oh,
            "cph": op.get("roi_metrics", {})
            .get("cost_per_hire", {})
            .get("expected", 0),
            "cpa": op.get("projections", {}).get("expected", {}).get("cpa", 0),
        }
        tot_h_opt += oh
        tot_h_eq += eh

    # Normalize to total_budget
    a_sum = sum(a["budget"] for a in alloc.values())
    if a_sum > 0 and abs(a_sum - total_budget) > 1:
        s = total_budget / a_sum
        for d in alloc.values():
            d["budget"] = round(d["budget"] * s)

    imp = round(_safe_div(tot_h_opt - tot_h_eq, max(tot_h_eq, 1)) * 100, 1)
    return {
        "total_budget": total_budget,
        "industry": industry,
        "optimize_for": optimize_for,
        "channel_count": len(channels),
        "equal_split_projections": {
            ch: {
                "budget": round(eq_bud),
                "hires": ch_proj[ch]
                .get("projections", {})
                .get("expected", {})
                .get("hires", 0),
                "cph": ch_proj[ch]
                .get("roi_metrics", {})
                .get("cost_per_hire", {})
                .get("expected", 0),
                "cpa": ch_proj[ch]
                .get("projections", {})
                .get("expected", {})
                .get("cpa", 0),
                "confidence": ch_proj[ch].get("confidence", {}).get("label", "Medium"),
            }
            for ch in channels
        },
        "optimal_allocation": alloc,
        "ranking": ranked,
        "summary": {
            "total_hires_equal_split": tot_h_eq,
            "total_hires_optimal": tot_h_opt,
            "improvement_pct": imp,
            "best_channel": ranked[0] if ranked else None,
            "recommendation": (
                f"Optimized allocation projects {tot_h_opt} hires vs "
                f"{tot_h_eq} with equal split ({'+' if imp > 0 else ''}{imp}%). "
                f"Top channel: {ranked[0] if ranked else 'N/A'}."
            ),
        },
        "source": "Nova ROI Projector (28 industry sources, 302M+ data points)",
    }
