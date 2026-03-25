"""Outcome Guarantee Engine -- pay-per-outcome pricing model.

Shifts from SaaS subscriptions to pay-per-qualified-applicant pricing.
Provides conversion funnel modeling, outcome estimation, ROI comparison,
and pricing tiers by role family, seniority, and location.

Thread-safe: all mutable state is protected by locks.
"""

import logging
import threading
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# PRICING TIERS BY ROLE FAMILY (USD per qualified applicant)
# ═══════════════════════════════════════════════════════════════════════════════

OUTCOME_PRICING_TIERS: dict[str, dict[str, Any]] = {
    "engineering": {
        "min_price": 150,
        "max_price": 300,
        "base_price": 200,
        "description": "Software engineers, DevOps, data engineers, QA",
    },
    "healthcare": {
        "min_price": 75,
        "max_price": 150,
        "base_price": 100,
        "description": "Nurses, physicians, allied health, clinical staff",
    },
    "executive": {
        "min_price": 500,
        "max_price": 1000,
        "base_price": 700,
        "description": "C-suite, VP, SVP, managing directors",
    },
    "sales": {
        "min_price": 100,
        "max_price": 200,
        "base_price": 140,
        "description": "Account executives, BDRs, sales managers",
    },
    "operations": {
        "min_price": 50,
        "max_price": 100,
        "base_price": 70,
        "description": "Warehouse, logistics, supply chain, facilities",
    },
    "marketing": {
        "min_price": 125,
        "max_price": 250,
        "base_price": 175,
        "description": "Digital marketing, content, brand, growth",
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# CONVERSION FUNNEL RATES
# ═══════════════════════════════════════════════════════════════════════════════

# Impressions -> Clicks (CTR by channel)
CHANNEL_CTR: dict[str, float] = {
    "linkedin": 0.005,
    "indeed": 0.012,
    "google": 0.025,
    "ziprecruiter": 0.010,
    "glassdoor": 0.008,
    "careerbuilder": 0.009,
    "monster": 0.007,
    "google_for_jobs": 0.020,
    "programmatic": 0.015,
    "facebook": 0.011,
    "instagram": 0.006,
    "twitter": 0.004,
}

# Clicks -> Applications (apply rate by role family)
ROLE_APPLY_RATE: dict[str, float] = {
    "engineering": 0.03,
    "healthcare": 0.08,
    "executive": 0.02,
    "sales": 0.05,
    "operations": 0.10,
    "marketing": 0.04,
}

# Applications -> Qualified (qualification rate by seniority)
SENIORITY_QUAL_RATE: dict[str, float] = {
    "entry": 0.40,
    "mid": 0.30,
    "senior": 0.20,
    "executive": 0.15,
}

# Location cost multipliers
LOCATION_MULTIPLIERS: dict[str, float] = {
    "san_francisco": 1.5,
    "new_york": 1.4,
    "los_angeles": 1.3,
    "seattle": 1.3,
    "boston": 1.25,
    "austin": 1.1,
    "denver": 1.1,
    "chicago": 1.15,
    "miami": 1.1,
    "london": 1.35,
    "berlin": 1.0,
    "toronto": 1.1,
    "sydney": 1.2,
    "singapore": 1.3,
    "bangalore": 0.5,
    "remote": 1.0,
    "us_default": 1.0,
    "eu_default": 0.9,
    "apac_default": 0.7,
    "latam_default": 0.5,
}

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE STATS TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

_stats_lock = threading.Lock()
_engine_stats: dict[str, Any] = {
    "total_estimates": 0,
    "total_price_lookups": 0,
    "total_comparisons": 0,
    "last_estimate_at": None,
    "started_at": time.time(),
}


def _increment_stat(key: str) -> None:
    """Thread-safe stat increment."""
    with _stats_lock:
        _engine_stats[key] = _engine_stats.get(key, 0) + 1
        if key == "total_estimates":
            _engine_stats["last_estimate_at"] = time.time()


# ═══════════════════════════════════════════════════════════════════════════════
# CORE FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def _normalize_role_family(role_family: str) -> str:
    """Normalize role family string to a known key.

    Handles common aliases and variations.
    """
    raw = (role_family or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases: dict[str, str] = {
        "engineer": "engineering",
        "software": "engineering",
        "developer": "engineering",
        "devops": "engineering",
        "data": "engineering",
        "tech": "engineering",
        "nurse": "healthcare",
        "medical": "healthcare",
        "clinical": "healthcare",
        "physician": "healthcare",
        "exec": "executive",
        "c_suite": "executive",
        "vp": "executive",
        "director": "executive",
        "leadership": "executive",
        "account_executive": "sales",
        "bdr": "sales",
        "sdr": "sales",
        "business_development": "sales",
        "ops": "operations",
        "warehouse": "operations",
        "logistics": "operations",
        "supply_chain": "operations",
        "facilities": "operations",
        "digital_marketing": "marketing",
        "content": "marketing",
        "brand": "marketing",
        "growth": "marketing",
    }
    return aliases.get(raw, raw) if raw not in OUTCOME_PRICING_TIERS else raw


def _normalize_seniority(seniority: str) -> str:
    """Normalize seniority string to a known key."""
    raw = (seniority or "mid").strip().lower().replace("-", "_").replace(" ", "_")
    aliases: dict[str, str] = {
        "junior": "entry",
        "intern": "entry",
        "associate": "entry",
        "level_1": "entry",
        "level_2": "mid",
        "intermediate": "mid",
        "staff": "senior",
        "principal": "senior",
        "lead": "senior",
        "manager": "mid",
        "vp": "executive",
        "c_suite": "executive",
        "c_level": "executive",
        "svp": "executive",
        "director": "senior",
    }
    return aliases.get(raw, raw) if raw not in SENIORITY_QUAL_RATE else raw


def _resolve_location_multiplier(location: str) -> float:
    """Resolve a location string to its cost multiplier."""
    loc = (location or "").strip().lower().replace(",", "").replace(" ", "_")

    # Direct match
    if loc in LOCATION_MULTIPLIERS:
        return LOCATION_MULTIPLIERS[loc]

    # Partial matching
    for key, mult in LOCATION_MULTIPLIERS.items():
        if key in loc or loc in key:
            return mult

    # Region detection
    apac_markers = (
        "india",
        "asia",
        "singapore",
        "australia",
        "japan",
        "korea",
        "china",
        "bangalore",
        "mumbai",
        "delhi",
    )
    eu_markers = (
        "uk",
        "united_kingdom",
        "germany",
        "france",
        "europe",
        "london",
        "berlin",
        "paris",
        "amsterdam",
    )
    latam_markers = (
        "brazil",
        "mexico",
        "latin",
        "latam",
        "colombia",
        "argentina",
        "chile",
    )

    if any(m in loc for m in apac_markers):
        return LOCATION_MULTIPLIERS["apac_default"]
    if any(m in loc for m in eu_markers):
        return LOCATION_MULTIPLIERS["eu_default"]
    if any(m in loc for m in latam_markers):
        return LOCATION_MULTIPLIERS["latam_default"]

    return LOCATION_MULTIPLIERS["us_default"]


def calculate_outcome_price(
    role_family: str,
    location: str = "",
    seniority: str = "mid",
) -> dict[str, Any]:
    """Calculate the outcome-based price per qualified applicant.

    Args:
        role_family: Role category (e.g., 'engineering', 'healthcare').
        location: Geographic location for cost adjustment.
        seniority: Seniority level (entry, mid, senior, executive).

    Returns:
        Dict with price_per_qualified, price_range, tier details, and multipliers.
    """
    _increment_stat("total_price_lookups")

    normalized_family = _normalize_role_family(role_family)
    normalized_seniority = _normalize_seniority(seniority)
    location_mult = _resolve_location_multiplier(location)

    tier = OUTCOME_PRICING_TIERS.get(normalized_family)
    if tier is None:
        # Fallback to operations (lowest tier) with a warning
        tier = OUTCOME_PRICING_TIERS["operations"]
        normalized_family = "operations"
        logger.warning(
            f"Unknown role family '{role_family}', falling back to operations tier"
        )

    # Seniority multiplier: entry=0.8, mid=1.0, senior=1.3, executive=1.6
    seniority_multipliers: dict[str, float] = {
        "entry": 0.8,
        "mid": 1.0,
        "senior": 1.3,
        "executive": 1.6,
    }
    seniority_mult = seniority_multipliers.get(normalized_seniority, 1.0)

    base_price = tier["base_price"]
    adjusted_price = round(base_price * location_mult * seniority_mult, 2)

    # Clamp to tier min/max (adjusted for location)
    adjusted_min = round(tier["min_price"] * location_mult, 2)
    adjusted_max = round(tier["max_price"] * location_mult * seniority_mult, 2)
    adjusted_price = max(adjusted_min, min(adjusted_max, adjusted_price))

    return {
        "role_family": normalized_family,
        "seniority": normalized_seniority,
        "location": location or "US (default)",
        "price_per_qualified_applicant": adjusted_price,
        "price_range": {
            "min": adjusted_min,
            "max": adjusted_max,
        },
        "tier": {
            "name": normalized_family,
            "description": tier["description"],
            "base_price": tier["base_price"],
        },
        "multipliers": {
            "location": location_mult,
            "seniority": seniority_mult,
        },
        "currency": "USD",
    }


def estimate_outcomes(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Estimate guaranteed outcomes from a media plan.

    Models the full conversion funnel: impressions -> clicks -> applications -> qualified.

    Args:
        plan_data: Dict with keys like budget, role_family, channels, seniority, location.
            Required: budget (float), role_family (str).
            Optional: channels (list[str]), seniority (str), location (str),
                      impressions (int), cpc (float).

    Returns:
        Dict with estimated_applications, estimated_qualified, confidence_interval,
        guaranteed_minimum, outcome_price, total_outcome_cost, savings_vs_traditional,
        and full funnel breakdown.
    """
    _increment_stat("total_estimates")

    budget = float(plan_data.get("budget") or 0)
    role_family = plan_data.get("role_family") or plan_data.get("role") or "operations"
    seniority = plan_data.get("seniority") or "mid"
    location = plan_data.get("location") or ""
    channels = plan_data.get("channels") or ["indeed", "linkedin", "google"]
    provided_impressions = plan_data.get("impressions")
    provided_cpc = plan_data.get("cpc")

    normalized_family = _normalize_role_family(role_family)
    normalized_seniority = _normalize_seniority(seniority)

    if budget <= 0:
        return {
            "error": "Budget must be greater than 0",
            "estimated_applications": 0,
            "estimated_qualified": 0,
        }

    # Step 1: Estimate impressions from budget (if not provided)
    # Assume average CPM of $5-15 depending on channel mix
    if provided_impressions:
        total_impressions = int(provided_impressions)
    else:
        avg_cpm = 8.0  # $8 per 1000 impressions (blended average)
        total_impressions = int((budget / avg_cpm) * 1000)

    # Step 2: Impressions -> Clicks (weighted CTR across channels)
    channel_details: list[dict[str, Any]] = []
    total_clicks = 0
    channel_count = max(1, len(channels))
    impressions_per_channel = total_impressions // channel_count

    for channel in channels:
        ch_key = channel.lower().replace(" ", "_").replace("-", "_")
        ctr = CHANNEL_CTR.get(ch_key, 0.008)  # default 0.8% CTR
        ch_clicks = int(impressions_per_channel * ctr)
        total_clicks += ch_clicks
        channel_details.append(
            {
                "channel": channel,
                "impressions": impressions_per_channel,
                "ctr": ctr,
                "clicks": ch_clicks,
            }
        )

    # Override with CPC-based click estimate if CPC provided
    if provided_cpc and float(provided_cpc) > 0:
        cpc_based_clicks = int(budget / float(provided_cpc))
        if cpc_based_clicks > total_clicks:
            total_clicks = cpc_based_clicks

    # Step 3: Clicks -> Applications (role-family apply rate)
    apply_rate = ROLE_APPLY_RATE.get(normalized_family, 0.05)
    total_applications = int(total_clicks * apply_rate)

    # Step 4: Applications -> Qualified (seniority qualification rate)
    qual_rate = SENIORITY_QUAL_RATE.get(normalized_seniority, 0.30)
    total_qualified = int(total_applications * qual_rate)

    # Step 5: Confidence interval (based on sample size and conversion variance)
    # Use a simplified +/- 25% for the confidence interval
    ci_low = max(0, int(total_qualified * 0.75))
    ci_high = int(total_qualified * 1.30)

    # Guaranteed minimum: conservative estimate at 60% of expected
    guaranteed_minimum = (
        max(1, int(total_qualified * 0.60)) if total_qualified > 0 else 0
    )

    # Step 6: Pricing
    pricing = calculate_outcome_price(role_family, location, seniority)
    outcome_price = pricing["price_per_qualified_applicant"]
    total_outcome_cost = round(outcome_price * guaranteed_minimum, 2)

    # Step 7: Savings vs traditional (flat media spend)
    # Traditional: you spend the full budget regardless of outcome
    savings_amount = round(budget - total_outcome_cost, 2)
    savings_pct = round((savings_amount / max(1, budget)) * 100, 2)

    return {
        "estimated_impressions": total_impressions,
        "estimated_clicks": total_clicks,
        "estimated_applications": total_applications,
        "estimated_qualified": total_qualified,
        "confidence_interval": {"low": ci_low, "high": ci_high},
        "guaranteed_minimum": guaranteed_minimum,
        "outcome_price": outcome_price,
        "total_outcome_cost": total_outcome_cost,
        "traditional_spend": budget,
        "savings_vs_traditional": {
            "amount": savings_amount,
            "percentage": savings_pct,
            "favorable": savings_amount > 0,
        },
        "funnel": {
            "impressions_to_clicks": {
                "rate": round(total_clicks / max(1, total_impressions), 4),
                "input": total_impressions,
                "output": total_clicks,
            },
            "clicks_to_applications": {
                "rate": apply_rate,
                "input": total_clicks,
                "output": total_applications,
            },
            "applications_to_qualified": {
                "rate": qual_rate,
                "input": total_applications,
                "output": total_qualified,
            },
        },
        "channel_breakdown": channel_details,
        "pricing_details": pricing,
        "plan_inputs": {
            "budget": budget,
            "role_family": normalized_family,
            "seniority": normalized_seniority,
            "location": location or "US (default)",
            "channels": channels,
        },
    }


def compare_pricing_models(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Compare flat media spend vs outcome-based pricing.

    Args:
        plan_data: Same as estimate_outcomes input.

    Returns:
        Dict with side-by-side comparison of flat, outcome, and hybrid models.
    """
    _increment_stat("total_comparisons")

    budget = float(plan_data.get("budget") or 0)
    outcomes = estimate_outcomes(plan_data)

    if outcomes.get("error"):
        return outcomes

    outcome_price = outcomes["outcome_price"]
    guaranteed_min = outcomes["guaranteed_minimum"]
    estimated_qualified = outcomes["estimated_qualified"]

    # Model 1: Flat spend (traditional)
    flat_model: dict[str, Any] = {
        "name": "Flat Media Spend",
        "description": "Fixed budget regardless of outcomes",
        "total_cost": budget,
        "cost_per_qualified": round(budget / max(1, estimated_qualified), 2),
        "risk_bearer": "advertiser",
        "guaranteed_outcomes": 0,
        "estimated_outcomes": estimated_qualified,
        "payment_trigger": "impressions/clicks",
    }

    # Model 2: Pure outcome-based
    outcome_cost = round(outcome_price * estimated_qualified, 2)
    outcome_model: dict[str, Any] = {
        "name": "Pay-Per-Outcome",
        "description": "Pay only for qualified applicants delivered",
        "total_cost": outcome_cost,
        "cost_per_qualified": outcome_price,
        "risk_bearer": "platform",
        "guaranteed_outcomes": guaranteed_min,
        "estimated_outcomes": estimated_qualified,
        "payment_trigger": "qualified applicant",
    }

    # Model 3: Hybrid (base fee + outcome bonus)
    base_fee = round(budget * 0.3, 2)  # 30% flat base
    per_outcome_hybrid = round(outcome_price * 0.6, 2)  # 60% of full outcome price
    hybrid_cost = round(base_fee + (per_outcome_hybrid * estimated_qualified), 2)
    hybrid_model: dict[str, Any] = {
        "name": "Hybrid (Base + Outcome)",
        "description": "30% flat base fee + reduced per-outcome rate",
        "total_cost": hybrid_cost,
        "cost_per_qualified": round(hybrid_cost / max(1, estimated_qualified), 2),
        "risk_bearer": "shared",
        "guaranteed_outcomes": int(guaranteed_min * 0.8),
        "estimated_outcomes": estimated_qualified,
        "payment_trigger": "base fee + qualified applicant",
        "base_fee": base_fee,
        "per_outcome_rate": per_outcome_hybrid,
    }

    # Determine recommendation
    models = [flat_model, outcome_model, hybrid_model]
    cheapest = min(models, key=lambda m: m["total_cost"])
    recommendation = cheapest["name"]

    # Nuanced recommendation: if budget is high and outcomes are few, outcome is better
    if estimated_qualified < 10:
        recommendation = "Pay-Per-Outcome"
        recommendation_reason = (
            "Low expected volume makes per-outcome pricing most cost-effective"
        )
    elif estimated_qualified > 100:
        recommendation = "Hybrid (Base + Outcome)"
        recommendation_reason = "High volume benefits from a blended rate"
    else:
        recommendation_reason = f"{cheapest['name']} offers the lowest total cost at ${cheapest['total_cost']:,.2f}"

    return {
        "models": {
            "flat": flat_model,
            "outcome": outcome_model,
            "hybrid": hybrid_model,
        },
        "recommendation": {
            "model": recommendation,
            "reason": recommendation_reason,
        },
        "estimated_qualified_applicants": estimated_qualified,
        "guaranteed_minimum": guaranteed_min,
        "plan_inputs": outcomes.get("plan_inputs", {}),
    }


def get_outcome_stats() -> dict[str, Any]:
    """Return engine stats for /api/health.

    Thread-safe read of accumulated statistics.
    """
    with _stats_lock:
        uptime = round(time.time() - _engine_stats["started_at"], 2)
        return {
            "status": "ok",
            "total_estimates": _engine_stats["total_estimates"],
            "total_price_lookups": _engine_stats["total_price_lookups"],
            "total_comparisons": _engine_stats["total_comparisons"],
            "last_estimate_at": _engine_stats["last_estimate_at"],
            "uptime_seconds": uptime,
            "role_families": list(OUTCOME_PRICING_TIERS.keys()),
            "channels_tracked": len(CHANNEL_CTR),
        }


def get_all_pricing_tiers() -> dict[str, Any]:
    """Return all pricing tiers for reference/display.

    Returns:
        Dict mapping role family names to their pricing details.
    """
    result: dict[str, Any] = {}
    for family, tier in OUTCOME_PRICING_TIERS.items():
        result[family] = {
            "min_price": tier["min_price"],
            "max_price": tier["max_price"],
            "base_price": tier["base_price"],
            "description": tier["description"],
            "currency": "USD",
            "unit": "per qualified applicant",
        }
    return result
