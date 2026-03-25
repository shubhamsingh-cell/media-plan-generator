"""Plan Outcome Prediction Model -- ML-lite weighted scoring model.

Predicts plan success probability and expected outcomes based on historical
recruitment advertising data patterns.  Pure Python, no external ML deps.

Usage:
    from prediction_model import predict_outcomes, grade_plan, compare_plans

    prediction = predict_outcomes(plan_data)
    grade = grade_plan(plan_data)
    comparison = compare_plans(plan_a, plan_b)
"""

from __future__ import annotations

import datetime
import logging
import math
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Thread-safe counters
# ═══════════════════════════════════════════════════════════════════════════════

_lock = threading.Lock()
_prediction_count: int = 0
_grade_count: int = 0
_compare_count: int = 0
_total_latency_ms: float = 0.0
_last_prediction_at: str | None = None

# ═══════════════════════════════════════════════════════════════════════════════
# Reference data -- recruitment advertising baselines
# ═══════════════════════════════════════════════════════════════════════════════

# Role family difficulty multipliers (higher = harder to fill)
_ROLE_DIFFICULTY: dict[str, float] = {
    "engineering": 1.4,
    "technology": 1.35,
    "data_science": 1.5,
    "product": 1.3,
    "design": 1.2,
    "healthcare": 1.3,
    "nursing": 1.35,
    "skilled_trades": 1.25,
    "logistics": 1.1,
    "warehouse": 0.9,
    "retail": 0.85,
    "food_service": 0.8,
    "customer_service": 0.9,
    "sales": 1.0,
    "marketing": 1.05,
    "finance": 1.15,
    "hr": 1.0,
    "legal": 1.25,
    "executive": 1.6,
    "administrative": 0.85,
    "education": 1.1,
    "general": 1.0,
}

# Location tier cost multipliers
_LOCATION_TIER: dict[str, float] = {
    "tier_1": 1.4,  # SF, NYC, Seattle, Boston, LA
    "tier_2": 1.15,  # Austin, Denver, Chicago, Atlanta, DC
    "tier_3": 1.0,  # Standard metro areas
    "tier_4": 0.85,  # Smaller cities / rural
    "remote": 0.95,
}

_TIER_1_CITIES = frozenset(
    {
        "san francisco",
        "new york",
        "seattle",
        "boston",
        "los angeles",
        "nyc",
        "sf",
        "la",
        "manhattan",
        "brooklyn",
        "silicon valley",
        "palo alto",
        "mountain view",
        "sunnyvale",
        "cupertino",
    }
)
_TIER_2_CITIES = frozenset(
    {
        "austin",
        "denver",
        "chicago",
        "atlanta",
        "washington",
        "dc",
        "miami",
        "dallas",
        "houston",
        "portland",
        "philadelphia",
        "san diego",
        "raleigh",
        "nashville",
        "minneapolis",
        "charlotte",
    }
)

# Seasonal hiring multipliers by month (1-12)
_SEASONAL_FACTORS: dict[int, float] = {
    1: 1.15,  # Jan -- new year hiring surge
    2: 1.10,
    3: 1.05,
    4: 1.00,
    5: 0.95,
    6: 0.90,  # Jun -- summer slowdown
    7: 0.85,  # Jul -- lowest
    8: 0.90,
    9: 1.10,  # Sep -- fall hiring pickup
    10: 1.05,
    11: 0.95,
    12: 0.80,  # Dec -- holiday slowdown
}

# Channel effectiveness scores by role family (0-10)
_CHANNEL_EFFECTIVENESS: dict[str, dict[str, float]] = {
    "indeed": {
        "general": 8.0,
        "warehouse": 9.0,
        "retail": 8.5,
        "logistics": 8.5,
        "customer_service": 8.0,
        "healthcare": 7.5,
        "engineering": 5.0,
        "technology": 5.0,
        "executive": 3.0,
        "food_service": 8.5,
        "administrative": 8.0,
        "sales": 7.5,
        "skilled_trades": 8.0,
    },
    "linkedin": {
        "general": 6.0,
        "engineering": 9.0,
        "technology": 9.0,
        "product": 9.0,
        "design": 8.5,
        "executive": 9.5,
        "finance": 8.5,
        "marketing": 8.5,
        "sales": 8.0,
        "hr": 8.0,
        "legal": 8.0,
        "data_science": 9.0,
        "warehouse": 2.0,
        "retail": 3.0,
        "food_service": 2.0,
    },
    "ziprecruiter": {
        "general": 7.0,
        "warehouse": 7.5,
        "retail": 7.0,
        "logistics": 7.5,
        "customer_service": 7.5,
        "healthcare": 6.5,
        "engineering": 5.5,
        "administrative": 7.5,
        "sales": 7.0,
        "skilled_trades": 7.0,
    },
    "google_ads": {
        "general": 6.0,
        "engineering": 7.0,
        "technology": 7.0,
        "healthcare": 7.5,
        "nursing": 8.0,
        "skilled_trades": 6.5,
        "executive": 5.0,
        "retail": 5.5,
    },
    "meta_facebook": {
        "general": 5.5,
        "warehouse": 7.0,
        "retail": 7.5,
        "food_service": 8.0,
        "customer_service": 6.5,
        "nursing": 6.5,
        "healthcare": 6.0,
        "engineering": 4.0,
        "executive": 2.5,
    },
    "programmatic": {
        "general": 7.5,
        "warehouse": 8.0,
        "retail": 7.5,
        "logistics": 8.0,
        "healthcare": 7.0,
        "nursing": 7.5,
        "engineering": 6.5,
        "skilled_trades": 7.5,
        "customer_service": 7.0,
    },
    "glassdoor": {
        "general": 6.5,
        "engineering": 7.5,
        "technology": 7.5,
        "product": 7.0,
        "design": 7.0,
        "finance": 7.0,
        "marketing": 7.0,
        "sales": 6.5,
    },
}

# Ideal budget ranges per hire by role family
_BUDGET_PER_HIRE: dict[str, tuple[float, float]] = {
    "engineering": (3000, 8000),
    "technology": (2500, 7000),
    "data_science": (3500, 9000),
    "executive": (5000, 15000),
    "healthcare": (2000, 5000),
    "nursing": (1500, 4000),
    "retail": (500, 1500),
    "warehouse": (400, 1200),
    "food_service": (300, 1000),
    "customer_service": (600, 1800),
    "sales": (1000, 3000),
    "marketing": (1500, 4000),
    "finance": (2000, 5000),
    "administrative": (500, 1500),
    "general": (1000, 3000),
}


# ═══════════════════════════════════════════════════════════════════════════════
# Feature extraction
# ═══════════════════════════════════════════════════════════════════════════════


def _classify_role(role_str: str) -> str:
    """Map a free-text role string to a role family key."""
    if not role_str:
        return "general"
    lower = role_str.lower()
    mappings: list[tuple[list[str], str]] = [
        (
            [
                "engineer",
                "developer",
                "swe",
                "devops",
                "sre",
                "backend",
                "frontend",
                "fullstack",
            ],
            "engineering",
        ),
        (["data scien", "machine learn", "ml ", "ai ", "deep learn"], "data_science"),
        (["product manager", "product owner", "product lead"], "product"),
        (["design", "ux", "ui ", "creative director"], "design"),
        (
            [
                "executive",
                "ceo",
                "cfo",
                "cto",
                "coo",
                "vp ",
                "vice president",
                "c-suite",
            ],
            "executive",
        ),
        (["nurse", "nursing", "rn ", "lpn", "cna"], "nursing"),
        (
            ["doctor", "physician", "medical", "healthcare", "pharma", "clinical"],
            "healthcare",
        ),
        (["warehouse", "picker", "packer", "forklift"], "warehouse"),
        (["truck", "driver", "logistics", "supply chain", "fleet"], "logistics"),
        (["retail", "store", "cashier", "merchandis"], "retail"),
        (["food", "restaurant", "chef", "cook", "barista", "server"], "food_service"),
        (
            ["customer service", "support", "call center", "help desk"],
            "customer_service",
        ),
        (["sales", "account executive", "bdr", "sdr", "business develop"], "sales"),
        (["marketing", "content", "seo", "growth", "brand manager"], "marketing"),
        (["finance", "accounting", "analyst", "controller", "auditor"], "finance"),
        (["hr ", "human resource", "recruiter", "talent acqui", "people ops"], "hr"),
        (["legal", "attorney", "counsel", "paralegal", "compliance"], "legal"),
        (
            ["admin", "assistant", "receptionist", "office manager", "secretary"],
            "administrative",
        ),
        (["teacher", "professor", "instructor", "education", "tutor"], "education"),
        (
            [
                "electrician",
                "plumber",
                "hvac",
                "mechanic",
                "technician",
                "welder",
                "carpenter",
            ],
            "skilled_trades",
        ),
        (["tech", "software", "it ", "information tech", "cyber"], "technology"),
    ]
    for keywords, family in mappings:
        if any(kw in lower for kw in keywords):
            return family
    return "general"


def _classify_location_tier(locations: list[str]) -> str:
    """Determine location tier from a list of location strings."""
    if not locations:
        return "tier_3"
    combined = " ".join(locations).lower()
    if "remote" in combined:
        return "remote"
    for city in _TIER_1_CITIES:
        if city in combined:
            return "tier_1"
    for city in _TIER_2_CITIES:
        if city in combined:
            return "tier_2"
    return "tier_3"


def _shannon_entropy(allocations: list[float]) -> float:
    """Calculate Shannon entropy of budget allocations (channel diversity).

    Returns a value between 0 (single channel) and 1 (perfectly even split).
    Normalized by log(n) for comparability across different channel counts.
    """
    if not allocations:
        return 0.0
    total = sum(allocations)
    if total <= 0:
        return 0.0
    n = len(allocations)
    if n <= 1:
        return 0.0
    probabilities = [a / total for a in allocations if a > 0]
    if len(probabilities) <= 1:
        return 0.0
    raw_entropy = -sum(p * math.log2(p) for p in probabilities)
    max_entropy = math.log2(len(probabilities))
    return round(raw_entropy / max_entropy, 4) if max_entropy > 0 else 0.0


def _extract_features(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Extract prediction features from plan data.

    Handles both the full plan response format (with 'channels' list) and
    the raw input format (with 'budget', 'roles', etc.).
    """
    # -- Budget and channels --
    channels = plan_data.get("channels") or []
    budget_summary = plan_data.get("budget_summary") or {}
    total_budget = float(
        budget_summary.get("total")
        or plan_data.get("total_budget")
        or plan_data.get("budget")
        or 0
    )
    num_channels = (
        len(channels) if channels else int(plan_data.get("num_channels") or 1)
    )
    channel_budgets = [float(ch.get("budget") or 0) for ch in channels]
    budget_per_channel = total_budget / max(num_channels, 1)

    # -- Role classification --
    metadata = plan_data.get("metadata") or {}
    roles = (
        metadata.get("roles")
        or plan_data.get("roles")
        or plan_data.get("target_roles")
        or []
    )
    if isinstance(roles, str):
        roles = [roles]
    primary_role = (
        roles[0] if roles else plan_data.get("job_title") or plan_data.get("role") or ""
    )
    role_family = _classify_role(str(primary_role))

    # -- Location tier --
    locations = metadata.get("locations") or plan_data.get("locations") or []
    if isinstance(locations, str):
        locations = [locations]
    location_tier = _classify_location_tier(locations)

    # -- Seasonal factor --
    month = datetime.datetime.now().month
    seasonal_factor = _SEASONAL_FACTORS.get(month, 1.0)

    # -- Channel diversity (Shannon entropy) --
    diversity_score = _shannon_entropy(channel_budgets) if channel_budgets else 0.0

    # -- Budget alignment score --
    ideal_range = _BUDGET_PER_HIRE.get(role_family, (1000, 3000))
    num_hires = max(
        int(plan_data.get("num_hires") or plan_data.get("positions") or 1), 1
    )
    budget_per_hire = total_budget / num_hires
    if ideal_range[0] <= budget_per_hire <= ideal_range[1]:
        budget_alignment = 1.0
    elif budget_per_hire < ideal_range[0]:
        budget_alignment = max(0.2, budget_per_hire / ideal_range[0])
    else:
        budget_alignment = max(0.3, ideal_range[1] / budget_per_hire)

    # -- Channel effectiveness score --
    channel_effectiveness = _compute_channel_effectiveness(channels, role_family)

    return {
        "total_budget": total_budget,
        "num_channels": num_channels,
        "budget_per_channel": round(budget_per_channel, 2),
        "role_family": role_family,
        "location_tier": location_tier,
        "seasonal_factor": seasonal_factor,
        "channel_diversity_score": diversity_score,
        "budget_alignment_score": round(budget_alignment, 4),
        "channel_effectiveness_score": round(channel_effectiveness, 4),
        "num_hires": num_hires,
        "budget_per_hire": round(budget_per_hire, 2),
        "role_difficulty": _ROLE_DIFFICULTY.get(role_family, 1.0),
        "location_multiplier": _LOCATION_TIER.get(location_tier, 1.0),
    }


def _compute_channel_effectiveness(
    channels: list[dict[str, Any]], role_family: str
) -> float:
    """Compute weighted average channel effectiveness for the given role family."""
    if not channels:
        return 0.5  # neutral default
    total_weight = 0.0
    weighted_score = 0.0
    for ch in channels:
        ch_name = (ch.get("name") or "").lower().replace(" ", "_")
        budget = float(ch.get("budget") or 0)
        if budget <= 0:
            continue
        # Look up effectiveness -- check exact name, then try partial matches
        eff_data = _CHANNEL_EFFECTIVENESS.get(ch_name)
        if eff_data is None:
            # Try partial match
            for key, data in _CHANNEL_EFFECTIVENESS.items():
                if key in ch_name or ch_name in key:
                    eff_data = data
                    break
        score = 5.0  # neutral default
        if eff_data is not None:
            score = eff_data.get(role_family, eff_data.get("general", 5.0))
        weighted_score += score * budget
        total_weight += budget
    if total_weight <= 0:
        return 0.5
    return (weighted_score / total_weight) / 10.0  # normalize to 0-1


# ═══════════════════════════════════════════════════════════════════════════════
# Scoring engine -- weighted scoring model
# ═══════════════════════════════════════════════════════════════════════════════

# Feature weights (sum to 1.0) -- tuned from recruitment advertising patterns
_FEATURE_WEIGHTS: dict[str, float] = {
    "budget_alignment": 0.25,
    "channel_effectiveness": 0.25,
    "channel_diversity": 0.15,
    "seasonal_advantage": 0.10,
    "location_advantage": 0.10,
    "budget_sufficiency": 0.15,
}


def _score_features(features: dict[str, Any]) -> dict[str, float]:
    """Score each feature 0-100 and return individual scores."""
    scores: dict[str, float] = {}

    # Budget alignment: already 0-1 from extraction
    scores["budget_alignment"] = features["budget_alignment_score"] * 100

    # Channel effectiveness: already 0-1 from extraction
    scores["channel_effectiveness"] = features["channel_effectiveness_score"] * 100

    # Channel diversity: entropy 0-1, penalize single channel heavily
    diversity = features["channel_diversity_score"]
    num_ch = features["num_channels"]
    if num_ch <= 1:
        scores["channel_diversity"] = 20.0
    elif num_ch == 2:
        scores["channel_diversity"] = 40.0 + diversity * 30.0
    else:
        scores["channel_diversity"] = 50.0 + diversity * 50.0

    # Seasonal advantage: 0.8-1.15 mapped to 0-100
    sf = features["seasonal_factor"]
    scores["seasonal_advantage"] = min(100.0, max(0.0, (sf - 0.75) / 0.45 * 100))

    # Location advantage: lower cost = higher score
    loc_mult = features["location_multiplier"]
    scores["location_advantage"] = min(100.0, max(0.0, (1.5 - loc_mult) / 0.7 * 100))

    # Budget sufficiency: is there enough budget per hire?
    bph = features["budget_per_hire"]
    ideal_low = _BUDGET_PER_HIRE.get(features["role_family"], (1000, 3000))[0]
    if bph >= ideal_low * 1.5:
        scores["budget_sufficiency"] = 95.0
    elif bph >= ideal_low:
        scores["budget_sufficiency"] = (
            70.0 + (bph - ideal_low) / (ideal_low * 0.5) * 25.0
        )
    elif bph >= ideal_low * 0.5:
        scores["budget_sufficiency"] = (
            30.0 + (bph - ideal_low * 0.5) / (ideal_low * 0.5) * 40.0
        )
    else:
        scores["budget_sufficiency"] = max(5.0, bph / (ideal_low * 0.5) * 30.0)

    # Clamp all scores
    for key in scores:
        scores[key] = round(min(100.0, max(0.0, scores[key])), 1)

    return scores


def _compute_overall_score(scores: dict[str, float]) -> float:
    """Compute weighted overall score from individual feature scores."""
    total = sum(
        scores.get(feature, 50.0) * weight
        for feature, weight in _FEATURE_WEIGHTS.items()
    )
    return round(total, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# Outcome projection formulas
# ═══════════════════════════════════════════════════════════════════════════════


def _project_outcomes(features: dict[str, Any], overall_score: float) -> dict[str, Any]:
    """Project hiring outcomes based on features and overall score."""
    total_budget = features["total_budget"]
    num_hires = features["num_hires"]
    role_family = features["role_family"]
    role_diff = features["role_difficulty"]
    loc_mult = features["location_multiplier"]
    seasonal = features["seasonal_factor"]

    # Success probability: sigmoid-shaped mapping of overall_score
    # score 50 -> ~0.5, score 80 -> ~0.85, score 30 -> ~0.25
    raw_prob = 1.0 / (1.0 + math.exp(-0.08 * (overall_score - 50)))
    success_probability = round(min(0.95, max(0.05, raw_prob)), 3)

    # Predicted applications: based on budget, CPA assumptions, role difficulty
    # Base: budget / avg_CPA * application_multiplier
    base_cpa = _get_avg_cpa(role_family)
    adj_cpa = base_cpa * loc_mult / seasonal
    predicted_applications = int(total_budget / max(adj_cpa, 1.0) * 2.5)

    # Predicted qualified: apply qualification rate based on channel mix quality
    qual_rate = 0.15 + (features["channel_effectiveness_score"] * 0.25)
    predicted_qualified = int(predicted_applications * qual_rate)

    # Predicted time to fill: base days adjusted for difficulty, location, season
    base_days = 30.0
    predicted_ttf = round(
        base_days * role_diff * (1.0 + (loc_mult - 1.0) * 0.5) / seasonal, 0
    )
    predicted_ttf = max(7, min(120, int(predicted_ttf)))

    # Predicted cost per hire
    predicted_cph = round(total_budget / max(num_hires, 1), 2)

    # Confidence score: higher when we have more data points and features align
    confidence_base = 0.6
    if features["num_channels"] >= 3:
        confidence_base += 0.1
    if features["budget_alignment_score"] > 0.7:
        confidence_base += 0.1
    if features["channel_diversity_score"] > 0.5:
        confidence_base += 0.05
    if total_budget > 1000:
        confidence_base += 0.05
    confidence_score = round(min(0.95, confidence_base), 3)

    return {
        "success_probability": success_probability,
        "predicted_applications": predicted_applications,
        "predicted_qualified": predicted_qualified,
        "predicted_time_to_fill_days": predicted_ttf,
        "predicted_cost_per_hire": predicted_cph,
        "confidence_score": confidence_score,
    }


def _get_avg_cpa(role_family: str) -> float:
    """Get average CPA for a role family from benchmark data."""
    try:
        from benchmark_registry import CHANNEL_BENCHMARKS

        cpas = [
            v.get("cpa", 30.0)
            for v in CHANNEL_BENCHMARKS.values()
            if isinstance(v, dict) and "cpa" in v
        ]
        base_cpa = sum(cpas) / len(cpas) if cpas else 30.0
    except ImportError:
        base_cpa = 30.0

    # Adjust for role difficulty
    difficulty = _ROLE_DIFFICULTY.get(role_family, 1.0)
    return base_cpa * difficulty


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════


def predict_outcomes(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Predict outcomes for a media plan.

    Args:
        plan_data: Plan data dict with channels, budget, roles, locations, etc.

    Returns:
        Dict with predictions, features, scores, and metadata.
    """
    global _prediction_count, _total_latency_ms, _last_prediction_at
    start = time.monotonic()

    try:
        features = _extract_features(plan_data)
        scores = _score_features(features)
        overall = _compute_overall_score(scores)
        outcomes = _project_outcomes(features, overall)

        result = {
            "predictions": outcomes,
            "overall_score": overall,
            "feature_scores": scores,
            "features": features,
            "metadata": {
                "model_version": "1.0.0",
                "model_type": "weighted_scoring",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
        }
    except (ValueError, TypeError, KeyError, ZeroDivisionError) as e:
        logger.error("Prediction failed: %s", e, exc_info=True)
        result = {
            "predictions": {
                "success_probability": 0.5,
                "predicted_applications": 0,
                "predicted_qualified": 0,
                "predicted_time_to_fill_days": 30,
                "predicted_cost_per_hire": 0.0,
                "confidence_score": 0.1,
            },
            "overall_score": 50.0,
            "feature_scores": {},
            "features": {},
            "error": f"Prediction partially failed: {e}",
            "metadata": {
                "model_version": "1.0.0",
                "model_type": "weighted_scoring",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
        }

    elapsed = (time.monotonic() - start) * 1000
    with _lock:
        _prediction_count += 1
        _total_latency_ms += elapsed
        _last_prediction_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    result["metadata"]["latency_ms"] = round(elapsed, 2)
    return result


def grade_plan(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Grade a media plan A-F with strengths, weaknesses, and suggestions.

    Args:
        plan_data: Plan data dict.

    Returns:
        Dict with grade, letter, score, strengths, weaknesses, suggestions.
    """
    global _grade_count
    start = time.monotonic()

    prediction = predict_outcomes(plan_data)
    overall = prediction.get("overall_score", 50.0)
    scores = prediction.get("feature_scores", {})
    features = prediction.get("features", {})

    # Letter grade
    if overall >= 85:
        letter = "A"
    elif overall >= 75:
        letter = "B"
    elif overall >= 60:
        letter = "C"
    elif overall >= 45:
        letter = "D"
    else:
        letter = "F"

    # Strengths (scores >= 70)
    strengths: list[str] = []
    for feature, score in scores.items():
        if score >= 70:
            strengths.append(_feature_strength_text(feature, score, features))

    # Weaknesses (scores < 50)
    weaknesses: list[str] = []
    for feature, score in scores.items():
        if score < 50:
            weaknesses.append(_feature_weakness_text(feature, score, features))

    # Improvement suggestions
    suggestions: list[str] = _generate_suggestions(scores, features)

    elapsed = (time.monotonic() - start) * 1000
    with _lock:
        _grade_count += 1

    return {
        "grade": letter,
        "overall_score": overall,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "suggestions": suggestions,
        "predictions": prediction.get("predictions", {}),
        "feature_scores": scores,
        "metadata": {
            "model_version": "1.0.0",
            "latency_ms": round(elapsed, 2),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
    }


def compare_plans(plan_a: dict[str, Any], plan_b: dict[str, Any]) -> dict[str, Any]:
    """Compare two plans and determine which is predicted to perform better.

    Args:
        plan_a: First plan data.
        plan_b: Second plan data.

    Returns:
        Dict with winner, scores, per-feature comparison, and reasoning.
    """
    global _compare_count
    start = time.monotonic()

    pred_a = predict_outcomes(plan_a)
    pred_b = predict_outcomes(plan_b)

    score_a = pred_a.get("overall_score", 0)
    score_b = pred_b.get("overall_score", 0)
    scores_a = pred_a.get("feature_scores", {})
    scores_b = pred_b.get("feature_scores", {})

    # Per-feature comparison
    feature_comparison: dict[str, dict[str, Any]] = {}
    a_wins = 0
    b_wins = 0
    for feature in set(list(scores_a.keys()) + list(scores_b.keys())):
        sa = scores_a.get(feature, 50.0)
        sb = scores_b.get(feature, 50.0)
        diff = round(sa - sb, 1)
        winner_label = "plan_a" if diff > 0 else ("plan_b" if diff < 0 else "tie")
        feature_comparison[feature] = {
            "plan_a": sa,
            "plan_b": sb,
            "difference": diff,
            "winner": winner_label,
        }
        if diff > 2:
            a_wins += 1
        elif diff < -2:
            b_wins += 1

    # Overall winner
    if abs(score_a - score_b) < 2:
        winner = "tie"
        reasoning = "Both plans score within 2 points of each other -- performance should be similar."
    elif score_a > score_b:
        winner = "plan_a"
        reasoning = f"Plan A scores {score_a:.1f} vs Plan B {score_b:.1f}, winning {a_wins} of {len(feature_comparison)} feature categories."
    else:
        winner = "plan_b"
        reasoning = f"Plan B scores {score_b:.1f} vs Plan A {score_a:.1f}, winning {b_wins} of {len(feature_comparison)} feature categories."

    elapsed = (time.monotonic() - start) * 1000
    with _lock:
        _compare_count += 1

    return {
        "winner": winner,
        "reasoning": reasoning,
        "plan_a": {
            "overall_score": score_a,
            "predictions": pred_a.get("predictions", {}),
            "feature_scores": scores_a,
        },
        "plan_b": {
            "overall_score": score_b,
            "predictions": pred_b.get("predictions", {}),
            "feature_scores": scores_b,
        },
        "feature_comparison": feature_comparison,
        "metadata": {
            "model_version": "1.0.0",
            "latency_ms": round(elapsed, 2),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
    }


def get_prediction_stats() -> dict[str, Any]:
    """Return prediction model stats for /api/health.

    Returns:
        Dict with prediction counts, avg latency, and model info.
    """
    with _lock:
        avg_latency = (
            round(_total_latency_ms / _prediction_count, 2)
            if _prediction_count > 0
            else 0.0
        )
        return {
            "status": "ok",
            "model_version": "1.0.0",
            "model_type": "weighted_scoring",
            "total_predictions": _prediction_count,
            "total_grades": _grade_count,
            "total_comparisons": _compare_count,
            "avg_latency_ms": avg_latency,
            "last_prediction_at": _last_prediction_at,
            "feature_weights": dict(_FEATURE_WEIGHTS),
            "supported_role_families": sorted(_ROLE_DIFFICULTY.keys()),
            "num_channel_profiles": len(_CHANNEL_EFFECTIVENESS),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Helper functions for grading
# ═══════════════════════════════════════════════════════════════════════════════


def _feature_strength_text(feature: str, score: float, features: dict[str, Any]) -> str:
    """Generate human-readable strength text for a high-scoring feature."""
    texts: dict[str, str] = {
        "budget_alignment": f"Budget is well-aligned with industry benchmarks for {features.get('role_family', 'this role')} hiring (score: {score:.0f}/100)",
        "channel_effectiveness": f"Channel mix is well-suited for {features.get('role_family', 'this role')} recruitment (score: {score:.0f}/100)",
        "channel_diversity": f"Good channel diversification reduces single-source risk (score: {score:.0f}/100)",
        "seasonal_advantage": f"Current timing is favorable for hiring activity (score: {score:.0f}/100)",
        "location_advantage": f"Target locations offer competitive hiring costs (score: {score:.0f}/100)",
        "budget_sufficiency": f"Budget per hire is sufficient for competitive sourcing (score: {score:.0f}/100)",
    }
    return texts.get(
        feature,
        f"{feature.replace('_', ' ').title()} is strong (score: {score:.0f}/100)",
    )


def _feature_weakness_text(feature: str, score: float, features: dict[str, Any]) -> str:
    """Generate human-readable weakness text for a low-scoring feature."""
    texts: dict[str, str] = {
        "budget_alignment": f"Budget may be misaligned with typical costs for {features.get('role_family', 'this role')} hiring (score: {score:.0f}/100)",
        "channel_effectiveness": f"Channel selection could be better optimized for {features.get('role_family', 'this role')} candidates (score: {score:.0f}/100)",
        "channel_diversity": f"Limited channel diversification increases risk of poor results (score: {score:.0f}/100)",
        "seasonal_advantage": f"Current season may slow hiring velocity (score: {score:.0f}/100)",
        "location_advantage": f"Target locations have above-average hiring costs (score: {score:.0f}/100)",
        "budget_sufficiency": f"Budget per hire may be insufficient for competitive sourcing (score: {score:.0f}/100)",
    }
    return texts.get(
        feature,
        f"{feature.replace('_', ' ').title()} needs improvement (score: {score:.0f}/100)",
    )


def _generate_suggestions(
    scores: dict[str, float], features: dict[str, Any]
) -> list[str]:
    """Generate actionable improvement suggestions based on scores and features."""
    suggestions: list[str] = []

    # Budget suggestions
    if scores.get("budget_sufficiency", 100) < 50:
        ideal = _BUDGET_PER_HIRE.get(
            features.get("role_family", "general"), (1000, 3000)
        )
        num_hires = features.get("num_hires", 1)
        suggested_min = ideal[0] * num_hires
        suggestions.append(
            f"Consider increasing budget to at least ${suggested_min:,.0f} "
            f"(${ideal[0]:,.0f}/hire) for {features.get('role_family', 'this role')} roles."
        )

    if scores.get("budget_alignment", 100) < 50:
        suggestions.append(
            "Review budget allocation against industry benchmarks. "
            "Current spend may be too high or too low relative to market rates."
        )

    # Channel suggestions
    if scores.get("channel_diversity", 100) < 50:
        current = features.get("num_channels", 1)
        suggestions.append(
            f"Add more channels (currently {current}). "
            "Recommended: 3-5 channels for balanced reach and risk mitigation."
        )

    if scores.get("channel_effectiveness", 100) < 50:
        role = features.get("role_family", "general")
        best_channels = _get_best_channels_for_role(role)
        if best_channels:
            suggestions.append(
                f"For {role} roles, consider prioritizing: {', '.join(best_channels[:3])}."
            )

    # Seasonal suggestions
    if scores.get("seasonal_advantage", 100) < 40:
        month = datetime.datetime.now().month
        suggestions.append(
            f"Month {month} is traditionally slower for hiring. "
            "Consider boosting ad spend 15-20% to compensate for lower candidate activity."
        )

    # Location suggestions
    if scores.get("location_advantage", 100) < 40:
        suggestions.append(
            "High-cost location markets may benefit from remote/hybrid positioning "
            "or expanded geographic targeting to reach more candidates cost-effectively."
        )

    if not suggestions:
        suggestions.append(
            "Plan looks well-optimized. Monitor performance weekly and reallocate "
            "budget from underperforming channels after 2-3 weeks of data."
        )

    return suggestions


def _get_best_channels_for_role(role_family: str) -> list[str]:
    """Get top channels for a role family, sorted by effectiveness."""
    channel_scores: list[tuple[str, float]] = []
    for ch_name, eff_data in _CHANNEL_EFFECTIVENESS.items():
        score = eff_data.get(role_family, eff_data.get("general", 5.0))
        channel_scores.append((ch_name.replace("_", " ").title(), score))
    channel_scores.sort(key=lambda x: x[1], reverse=True)
    return [name for name, _ in channel_scores[:5]]
