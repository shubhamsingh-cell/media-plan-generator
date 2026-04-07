#!/usr/bin/env python3
"""Campaign Optimization Engine (S46)

Rules-based + data-driven engine that recommends optimal channel allocations
for specific role/location/budget/industry combinations.

Uses: Joveo first-party data (66M+ views, 11M+ clicks), SlotOps benchmarks
(108K jobs), seasonal curves, geo-cost indices, and collar-type fit scores.

API: optimize_campaign(role, location, industry, budget, duration, goals) -> dict
"""

import datetime
import logging
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_feature_store = None
_collar_intel = None
_init_lock = threading.Lock()


def _get_feature_store() -> Any:
    """Lazy-load the FeatureStore singleton (thread-safe)."""
    global _feature_store
    if _feature_store is not None:
        return _feature_store
    with _init_lock:
        if _feature_store is None:
            try:
                from feature_store import get_feature_store

                _feature_store = get_feature_store()
                _feature_store.initialize()
            except Exception as exc:
                logger.warning("FeatureStore unavailable: %s", exc)
    return _feature_store


def _classify_collar(role: str, industry: str) -> Dict[str, Any]:
    """Classify role into collar type via collar_intelligence or fallback."""
    global _collar_intel
    try:
        if _collar_intel is None:
            with _init_lock:
                if _collar_intel is None:
                    import collar_intelligence as ci

                    _collar_intel = ci
        return _collar_intel.classify_collar(role, industry)
    except (ImportError, AttributeError, TypeError) as exc:
        logger.debug("collar_intelligence unavailable: %s", exc)
    return _collar_fallback(role)


# -- Channel metadata: label, ad-category, base CPC (USD), apply rate --
_CH = {
    "indeed": ("Indeed", "job_board", 1.20, 0.08),
    "linkedin": ("LinkedIn", "niche_board", 6.50, 0.10),
    "google_search": ("Google Search Ads", "search", 2.50, 0.05),
    "meta_facebook": ("Meta (Facebook/Instagram)", "social", 1.80, 0.03),
    "programmatic": ("Programmatic & DSP", "programmatic", 0.85, 0.06),
    "ziprecruiter": ("ZipRecruiter", "job_board", 1.50, 0.08),
    "glassdoor": ("Glassdoor", "niche_board", 2.80, 0.10),
    "niche_boards": ("Niche & Industry Boards", "niche_board", 2.00, 0.10),
}

# Platform safety margins for CPH/CPA projections (from budget_engine.py)
_MARGINS = {
    "programmatic": 1.35,
    "social": 1.20,
    "job_board": 1.10,
    "search": 1.15,
    "niche_board": 1.10,
    "employer_branding": 1.10,
}

# Collar-to-channel fit scores (from quick_plan.py)
_FIT = {
    "blue_collar": {
        "indeed": 92,
        "linkedin": 18,
        "google_search": 65,
        "meta_facebook": 88,
        "programmatic": 90,
        "ziprecruiter": 85,
        "glassdoor": 25,
        "niche_boards": 60,
    },
    "white_collar": {
        "indeed": 72,
        "linkedin": 95,
        "google_search": 70,
        "meta_facebook": 45,
        "programmatic": 55,
        "ziprecruiter": 60,
        "glassdoor": 82,
        "niche_boards": 78,
    },
    "grey_collar": {
        "indeed": 80,
        "linkedin": 55,
        "google_search": 60,
        "meta_facebook": 65,
        "programmatic": 75,
        "ziprecruiter": 70,
        "glassdoor": 45,
        "niche_boards": 92,
    },
    "pink_collar": {
        "indeed": 85,
        "linkedin": 30,
        "google_search": 58,
        "meta_facebook": 82,
        "programmatic": 78,
        "ziprecruiter": 80,
        "glassdoor": 35,
        "niche_boards": 55,
    },
}

# Hire rate by collar (from budget_engine.py HIRE_RATE_BY_TIER)
_HIRE_RATE = {
    "blue_collar": 0.06,
    "white_collar": 0.02,
    "grey_collar": 0.03,
    "pink_collar": 0.04,
    "default": 0.02,
}

_COLLAR_LABELS = {
    "blue_collar": "Blue Collar",
    "white_collar": "White Collar",
    "grey_collar": "Grey Collar",
    "pink_collar": "Pink Collar",
}

# Industry key normalization
_IND_ALIAS: Dict[str, str] = {
    "technology": "tech_engineering",
    "tech": "tech_engineering",
    "software": "tech_engineering",
    "healthcare": "healthcare_medical",
    "medical": "healthcare_medical",
    "nursing": "healthcare_medical",
    "retail": "retail_consumer",
    "consumer": "retail_consumer",
    "ecommerce": "retail_consumer",
    "logistics": "logistics_transportation",
    "transportation": "logistics_transportation",
    "finance": "finance_banking",
    "banking": "finance_banking",
    "fintech": "finance_banking",
    "hospitality": "hospitality_food",
    "food": "hospitality_food",
    "restaurant": "hospitality_food",
}


def _collar_fallback(role: str) -> Dict[str, Any]:
    """Keyword-based collar classification when collar_intelligence unavailable."""
    r = (role or "").lower()
    _map = [
        (
            "blue_collar",
            [
                "driver",
                "warehouse",
                "mechanic",
                "technician",
                "welder",
                "plumber",
                "electrician",
                "construction",
                "laborer",
                "forklift",
            ],
        ),
        (
            "grey_collar",
            [
                "nurse",
                "therapist",
                "physician",
                "clinical",
                "radiol",
                "pharmacy",
                "dental",
                "medical",
                "surgeon",
                "paramedic",
            ],
        ),
        (
            "pink_collar",
            [
                "customer service",
                "admin",
                "receptionist",
                "cashier",
                "retail",
                "caregiver",
                "housekeeper",
                "server",
                "barista",
            ],
        ),
    ]
    for collar, keywords in _map:
        for kw in keywords:
            if kw in r:
                return {"collar_type": collar, "confidence": 0.7}
    return {"collar_type": "white_collar", "confidence": 0.5}


def _normalize_industry(industry: str) -> str:
    """Normalize industry to canonical key."""
    raw = (industry or "").lower().strip()
    if raw in _IND_ALIAS:
        return _IND_ALIAS[raw]
    try:
        from feature_store import INDUSTRY_SEASONAL_CPA

        for key in INDUSTRY_SEASONAL_CPA:
            if key == raw or key.replace("_", " ") in raw or raw in key:
                return key
    except ImportError:
        pass
    return raw


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    """Safe division returning default when divisor is zero."""
    return a / b if b else default


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------


def optimize_campaign(
    role: str,
    location: str,
    industry: str,
    budget: float,
    duration_months: int = 1,
    goals: Optional[List[str]] = None,
    constraints: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Generate an optimized campaign recommendation.

    Returns dict with recommended_allocation, total_projected, scenarios,
    recommendations, warnings, and confidence metadata.
    """
    try:
        return _optimize_impl(
            role,
            location,
            industry,
            budget,
            duration_months,
            goals or [],
            constraints or {},
        )
    except Exception as exc:
        logger.error("Campaign optimizer failed: %s", exc, exc_info=True)
        return {
            "error": f"Optimization failed: {exc}",
            "recommended_allocation": {},
            "total_projected": {},
            "confidence_overall": "LOW",
        }


def _optimize_impl(
    role: str,
    location: str,
    industry: str,
    budget: float,
    duration_months: int,
    goals: List[str],
    constraints: Dict[str, Any],
) -> Dict[str, Any]:
    """Internal implementation of campaign optimization."""
    # Input validation
    budget = max(budget, 0)
    if budget < 100:
        return {
            "error": "Budget too small (minimum $100)",
            "recommended_allocation": {},
            "total_projected": {},
            "confidence_overall": "LOW",
        }
    duration_months = max(1, min(duration_months, 24))

    now = datetime.datetime.now()
    month = now.month
    data_sources: List[str] = []
    optimizations: List[str] = []
    warnings: List[str] = []

    # 1. Classify role family
    fs = _get_feature_store()
    role_family = fs.get_role_family(role) if fs else "general"
    if fs:
        data_sources.append("feature_store")

    # 2. Collar type
    collar_info = _classify_collar(role, industry)
    collar_type = collar_info.get("collar_type") or "white_collar"
    collar_conf = collar_info.get("confidence", 0.5)
    if collar_type not in _FIT:
        collar_type = "white_collar"
    data_sources.append("collar_intelligence")

    # 3. Geo-cost index
    geo_index = fs.get_geo_cost_index(location) if fs else 1.0
    if fs:
        optimizations.append("geo_cost_index")

    # 4. Seasonal + industry CPA
    industry_key = _normalize_industry(industry)
    if fs:
        seasonal = fs.get_seasonal_factor(month)
        ind_cpa = fs.get_industry_seasonal_cpa(industry_key, month)
        optimizations.append("seasonal_adjustment")
        if industry_key:
            optimizations.append("industry_seasonal_cpa")
    else:
        seasonal = ind_cpa = 1.0

    # 5. Channel fit scores
    fit_scores = _FIT.get(collar_type, _FIT["white_collar"])
    optimizations.append("collar_fit")

    # 6. Constraints + budget concentration
    excluded = set(constraints.get("exclude") or [])
    max_ch = constraints.get("max_channels", 8)
    eligible = sorted(
        [(c, s) for c, s in fit_scores.items() if c not in excluded and c in _CH],
        key=lambda x: x[1],
        reverse=True,
    )
    mo_budget = budget / max(duration_months, 1)
    if mo_budget < 5_000:
        max_ch = min(max_ch, 2)
        optimizations.append("budget_concentration_micro")
        warnings.append(
            f"Budget of ${mo_budget:,.0f}/mo is very small. "
            "Concentrating on top 2 channels for maximum impact."
        )
    elif mo_budget < 15_000:
        max_ch = min(max_ch, 3)
        optimizations.append("budget_concentration_small")
    elif mo_budget < 50_000:
        max_ch = min(max_ch, 5)
        optimizations.append("budget_concentration_medium")
    eligible = eligible[:max_ch]

    # 7. Allocate budget proportionally to fit scores
    total_fit = sum(s for _, s in eligible) or 1
    hire_rate = _HIRE_RATE.get(collar_type, 0.02)
    alloc: Dict[str, Dict[str, Any]] = {}
    tots = {"clicks": 0, "applies": 0, "hires": 0, "spend": 0.0}

    for ch_key, fit in eligible:
        label, cat, base_cpc, ar = _CH[ch_key]
        pct = round(fit / total_fit * 100, 1)
        ch_bud = round(budget * fit / total_fit, 2)
        adj_cpc = round(base_cpc * geo_index * ind_cpa, 2) or 0.01
        clicks = int(_safe_div(ch_bud, adj_cpc))
        applies = int(clicks * ar)
        hires = max(0, int(applies * hire_rate))
        margin = _MARGINS.get(cat, 1.0)

        alloc[ch_key] = {
            "label": label,
            "pct": pct,
            "budget": ch_bud,
            "cpc": adj_cpc,
            "apply_rate": ar,
            "projected_clicks": clicks,
            "projected_applies": applies,
            "projected_hires": hires,
            "cpa": round(_safe_div(ch_bud, max(applies, 1)) * margin, 2),
            "cph": round(_safe_div(ch_bud, max(hires, 1)) * margin, 2),
            "safety_margin": margin,
            "fit_score": fit,
            "confidence": _ch_conf(fit, collar_conf, ch_bud),
        }
        tots["clicks"] += clicks
        tots["applies"] += applies
        tots["hires"] += hires
        tots["spend"] += ch_bud

    optimizations.append("safety_margins")

    # 8. Totals + scenarios
    proj = {
        "clicks": tots["clicks"],
        "applies": tots["applies"],
        "hires": tots["hires"],
        "avg_cpa": round(_safe_div(budget, max(tots["applies"], 1)), 2),
        "avg_cph": round(_safe_div(budget, max(tots["hires"], 1)), 2),
        "budget": round(budget, 2),
        "duration_months": duration_months,
    }
    scenarios = {
        "conservative": _scale(proj, 0.80),
        "moderate": proj.copy(),
        "aggressive": _scale(proj, 1.20),
    }

    # 9. Recommendations + warnings
    recs = _recs(alloc, collar_type, role, mo_budget, goals)
    warnings.extend(_warns(collar_type, month, industry_key, geo_index, budget, alloc))

    return {
        "recommended_allocation": alloc,
        "total_projected": proj,
        "optimizations_applied": optimizations,
        "scenarios": scenarios,
        "recommendations": recs,
        "warnings": warnings,
        "confidence_overall": _overall_conf(
            collar_conf, len(alloc), budget, len(data_sources)
        ),
        "data_sources_used": data_sources,
        "metadata": {
            "role": role,
            "role_family": role_family,
            "collar_type": collar_type,
            "collar_label": _COLLAR_LABELS.get(collar_type, "White Collar"),
            "location": location,
            "geo_cost_index": geo_index,
            "industry": industry_key or industry,
            "seasonal_factor": seasonal,
            "industry_cpa_multiplier": ind_cpa,
            "month": month,
            "channels_evaluated": len(fit_scores),
            "channels_selected": len(alloc),
            "hire_rate_used": hire_rate,
            "generated_at": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scale(base: Dict[str, Any], factor: float) -> Dict[str, Any]:
    """Scale projection metrics by a factor for scenario modeling."""
    s = base.copy()
    for k in ("clicks", "applies", "hires"):
        s[k] = max(0, int(base.get(k, 0) * factor))
    s["avg_cpa"] = round(_safe_div(base.get("budget", 0), max(s["applies"], 1)), 2)
    s["avg_cph"] = round(_safe_div(base.get("budget", 0), max(s["hires"], 1)), 2)
    return s


def _ch_conf(fit: int, collar_conf: float, ch_budget: float) -> str:
    """Channel-level confidence."""
    if fit >= 80 and collar_conf >= 0.7 and ch_budget >= 2_000:
        return "HIGH"
    if fit >= 50 and collar_conf >= 0.5 and ch_budget >= 500:
        return "MEDIUM"
    return "LOW"


def _overall_conf(collar_conf: float, n_ch: int, budget: float, n_src: int) -> str:
    """Overall optimization confidence."""
    score = (
        min(collar_conf, 1.0) * 30
        + min(n_ch / 5, 1.0) * 20
        + min(budget / 50_000, 1.0) * 25
        + min(n_src / 3, 1.0) * 25
    )
    if score >= 70:
        return "HIGH"
    return "MEDIUM" if score >= 45 else "LOW"


def _recs(
    alloc: Dict, collar: str, role: str, mo_budget: float, goals: List[str]
) -> List[str]:
    """Generate text recommendations explaining channel choices."""
    recs: List[str] = []
    if alloc:
        top_k, top_v = max(alloc.items(), key=lambda x: x[1]["pct"])
        cl = (_COLLAR_LABELS.get(collar) or "").lower()
        recs.append(
            f"Lead with {top_v['label']} ({top_v['pct']}% of budget) -- "
            f"highest fit score ({top_v['fit_score']}/100) for {cl} roles like \"{role}\"."
        )
    vol = [k for k, v in alloc.items() if v.get("cpc", 0) < 2.0]
    qual = [k for k, v in alloc.items() if v.get("cpc", 0) >= 4.0]
    if vol and qual:
        recs.append(
            f"Mix of volume channels ({', '.join(alloc[c]['label'] for c in vol)}) "
            f"and quality channels ({', '.join(alloc[c]['label'] for c in qual)}) "
            "provides balanced reach and candidate quality."
        )
    if mo_budget < 10_000:
        recs.append(
            "With a lean budget, concentrate spend on proven channels "
            "and avoid spreading too thin across platforms."
        )
    elif mo_budget >= 100_000:
        recs.append(
            "Strong budget allows multi-channel diversification. "
            "Consider A/B testing creative across top 3 channels."
        )
    gs = set(g.lower() for g in goals)
    if gs & {"speed", "urgency"}:
        recs.append(
            "For speed-to-hire, front-load budget in week 1 with "
            "programmatic and job boards for immediate reach."
        )
    if "quality" in gs:
        recs.append(
            "For candidate quality, allocate more to LinkedIn and niche boards "
            "where apply-to-hire conversion rates are higher."
        )
    if gs & {"diversity", "dei"}:
        recs.append(
            "For diversity goals, include niche DEI boards and social media "
            "channels with inclusive targeting options."
        )
    return recs


def _warns(
    collar: str, month: int, industry: str, geo: float, budget: float, alloc: Dict
) -> List[str]:
    """Generate risk warnings based on context."""
    w: List[str] = []
    if month in (1, 2):
        w.append(
            "Q1 hiring surge -- CPC rates typically 10-20% above baseline. "
            "Budget may yield fewer clicks than summer months."
        )
    elif month in (11, 12):
        w.append(
            "Holiday season -- candidate response rates drop 15-25%. "
            "Consider extending campaign into January."
        )
    if collar == "blue_collar" and "linkedin" in alloc:
        w.append(
            "LinkedIn has low fit for blue-collar roles (18/100). "
            "Consider reallocating LinkedIn spend to Indeed or Meta."
        )
    if geo >= 1.3:
        w.append(
            f"High-cost market (geo index {geo}x). "
            "CPC and CPA will be significantly above national averages."
        )
    if industry == "retail_consumer" and month in (8, 9, 10):
        w.append(
            "Retail holiday hiring season -- CPA spikes 25-40% Aug-Oct. "
            "Lock in budgets and campaigns early."
        )
    if industry == "healthcare_medical" and month in (6, 7):
        w.append(
            "Summer travel nurse season -- healthcare CPA peaks in June/July. "
            "Niche health boards (Vivian, NurseFly) may offer better ROI."
        )
    if budget < 5_000 and len(alloc) > 2:
        w.append(
            "Budget may be too thin across selected channels. "
            "Consider reducing to 1-2 channels for meaningful impact."
        )
    return w


# ---------------------------------------------------------------------------
# Chatbot tool wrapper
# ---------------------------------------------------------------------------


def optimize_campaign_tool(params: dict) -> dict:
    """Wrapper for Nova chatbot tool dispatch."""
    return optimize_campaign(
        role=params.get("role") or params.get("job_title") or "",
        location=params.get("location") or "",
        industry=params.get("industry") or "",
        budget=float(params.get("budget") or 10_000),
        duration_months=int(params.get("duration_months") or 1),
        goals=params.get("goals") or [],
        constraints=params.get("constraints") or {},
    )
