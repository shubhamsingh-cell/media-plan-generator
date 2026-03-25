"""Closed-Loop Outcome Pipeline -- connects plan recommendations to actual hiring outcomes.

Creates a feedback loop that tracks actual outcomes against predictions and
uses observed data to improve future plan accuracy via exponential moving
average adjustments to conversion rates.

Outcome stages:
    PlanCreated -> CampaignLaunched -> ApplicationsReceived ->
    InterviewsScheduled -> OffersExtended -> HiresMade

Thread-safe: all mutable state is protected by locks.
"""

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from enum import IntEnum
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# OUTCOME STAGES
# ═══════════════════════════════════════════════════════════════════════════════


class OutcomeStage(IntEnum):
    """Ordered pipeline stages -- numeric values encode funnel position."""

    PLAN_CREATED = 0
    CAMPAIGN_LAUNCHED = 1
    APPLICATIONS_RECEIVED = 2
    INTERVIEWS_SCHEDULED = 3
    OFFERS_EXTENDED = 4
    HIRES_MADE = 5


STAGE_NAMES: dict[int, str] = {
    OutcomeStage.PLAN_CREATED: "plan_created",
    OutcomeStage.CAMPAIGN_LAUNCHED: "campaign_launched",
    OutcomeStage.APPLICATIONS_RECEIVED: "applications_received",
    OutcomeStage.INTERVIEWS_SCHEDULED: "interviews_scheduled",
    OutcomeStage.OFFERS_EXTENDED: "offers_extended",
    OutcomeStage.HIRES_MADE: "hires_made",
}

STAGE_FROM_NAME: dict[str, OutcomeStage] = {v: k for k, v in STAGE_NAMES.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# BASELINE CONVERSION RATES (sourced from outcome_engine.py)
# ═══════════════════════════════════════════════════════════════════════════════

# Stage-to-stage baseline conversion rates by role family.
# Keys: (from_stage_name, to_stage_name) -> rate
BASELINE_CONVERSIONS: dict[str, dict[str, float]] = {
    "engineering": {
        "campaign_launched_to_applications": 0.03,
        "applications_to_interviews": 0.25,
        "interviews_to_offers": 0.30,
        "offers_to_hires": 0.70,
    },
    "healthcare": {
        "campaign_launched_to_applications": 0.08,
        "applications_to_interviews": 0.30,
        "interviews_to_offers": 0.35,
        "offers_to_hires": 0.80,
    },
    "executive": {
        "campaign_launched_to_applications": 0.02,
        "applications_to_interviews": 0.40,
        "interviews_to_offers": 0.25,
        "offers_to_hires": 0.65,
    },
    "sales": {
        "campaign_launched_to_applications": 0.05,
        "applications_to_interviews": 0.28,
        "interviews_to_offers": 0.32,
        "offers_to_hires": 0.75,
    },
    "operations": {
        "campaign_launched_to_applications": 0.10,
        "applications_to_interviews": 0.35,
        "interviews_to_offers": 0.40,
        "offers_to_hires": 0.85,
    },
    "marketing": {
        "campaign_launched_to_applications": 0.04,
        "applications_to_interviews": 0.27,
        "interviews_to_offers": 0.30,
        "offers_to_hires": 0.72,
    },
}

DEFAULT_ROLE_FAMILY = "operations"

# EMA smoothing factor: 0 = ignore new data, 1 = fully trust new data
EMA_ALPHA = 0.15


# ═══════════════════════════════════════════════════════════════════════════════
# SUPABASE PERSISTENCE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_SUPABASE_URL = os.environ.get("SUPABASE_URL") or ""
_SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
_OUTCOMES_TABLE = "outcome_events"


def _supabase_rest(
    table: str,
    method: str = "GET",
    payload: Optional[Any] = None,
    params: str = "",
) -> Optional[Any]:
    """Make a REST call to the Supabase PostgREST API.

    Args:
        table: Table name.
        method: HTTP method (GET, POST, PATCH).
        payload: JSON body for POST/PATCH.
        params: Query string (e.g., '?plan_id=eq.abc&order=created_at.asc').

    Returns:
        Parsed JSON response or None on failure.
    """
    if not _SUPABASE_URL or not _SUPABASE_KEY:
        return None
    url = f"{_SUPABASE_URL.rstrip('/')}/rest/v1/{table}{params}"
    headers = {
        "apikey": _SUPABASE_KEY,
        "Authorization": f"Bearer {_SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    body = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        logger.error(
            "Supabase REST %s %s failed: %s", method, table, exc, exc_info=True
        )
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE IN-MEMORY STATE
# ═══════════════════════════════════════════════════════════════════════════════

_lock = threading.Lock()

# plan_id -> list of outcome events
_outcomes: dict[str, list[dict[str, Any]]] = {}

# plan_id -> { "role_family": str, "predicted": dict, "created_at": float }
_plan_meta: dict[str, dict[str, Any]] = {}

# Learned conversion rates per role family (start from baselines)
_learned_rates: dict[str, dict[str, float]] = {
    role: dict(rates) for role, rates in BASELINE_CONVERSIONS.items()
}

# Stats tracker
_stats: dict[str, Any] = {
    "total_events_recorded": 0,
    "total_accuracy_checks": 0,
    "total_model_updates": 0,
    "plans_tracked": 0,
    "started_at": time.time(),
}


# ═══════════════════════════════════════════════════════════════════════════════
# OUTCOME TRACKING
# ═══════════════════════════════════════════════════════════════════════════════


def record_outcome(
    plan_id: str,
    stage: str,
    data: Optional[dict[str, Any]] = None,
    role_family: str = "",
    predicted: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Record an actual outcome at a specific pipeline stage.

    Args:
        plan_id: Unique identifier for the media plan.
        stage: Stage name (e.g., 'applications_received', 'hires_made').
        data: Outcome data (e.g., {"count": 45, "source": "indeed"}).
        role_family: Role category for this plan (used for model learning).
        predicted: Original predicted values for accuracy comparison.

    Returns:
        Dict with recorded event details and current funnel snapshot.
    """
    stage_lower = (stage or "").strip().lower().replace(" ", "_")
    if stage_lower not in STAGE_FROM_NAME:
        return {
            "error": f"Unknown stage '{stage}'. Valid: {list(STAGE_FROM_NAME.keys())}",
            "recorded": False,
        }

    event = {
        "plan_id": plan_id,
        "stage": stage_lower,
        "stage_order": STAGE_FROM_NAME[stage_lower].value,
        "data": data or {},
        "recorded_at": time.time(),
        "recorded_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    with _lock:
        if plan_id not in _outcomes:
            _outcomes[plan_id] = []
            _stats["plans_tracked"] += 1

        _outcomes[plan_id].append(event)
        _stats["total_events_recorded"] += 1

        # Store plan metadata on first event
        if plan_id not in _plan_meta:
            _plan_meta[plan_id] = {
                "role_family": (role_family or DEFAULT_ROLE_FAMILY).lower(),
                "predicted": predicted or {},
                "created_at": time.time(),
            }
        elif role_family:
            _plan_meta[plan_id]["role_family"] = role_family.lower()
        if predicted:
            _plan_meta[plan_id]["predicted"] = predicted

    # Persist to Supabase in background
    _persist_outcome_async(event)

    return {
        "recorded": True,
        "event": event,
        "funnel": get_plan_outcomes(plan_id),
    }


def _persist_outcome_async(event: dict[str, Any]) -> None:
    """Persist an outcome event to Supabase in a background thread."""

    def _persist() -> None:
        try:
            _supabase_rest(_OUTCOMES_TABLE, method="POST", payload=event)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            logger.error(f"Failed to persist outcome event: {exc}", exc_info=True)

    thread = threading.Thread(target=_persist, daemon=True, name="outcome-persist")
    thread.start()


def get_plan_outcomes(plan_id: str) -> dict[str, Any]:
    """Get the full outcome funnel for a specific plan.

    Args:
        plan_id: Unique identifier for the media plan.

    Returns:
        Dict with stage-by-stage outcomes, timestamps, and funnel summary.
    """
    with _lock:
        events = list(_outcomes.get(plan_id, []))
        meta = dict(_plan_meta.get(plan_id, {}))

    if not events:
        return {
            "plan_id": plan_id,
            "stages": {},
            "events_count": 0,
            "funnel_complete": False,
        }

    # Aggregate by stage
    stages: dict[str, dict[str, Any]] = {}
    for event in events:
        stage_name = event["stage"]
        if stage_name not in stages:
            stages[stage_name] = {
                "stage": stage_name,
                "events": [],
                "latest_data": {},
                "total_count": 0,
                "first_recorded": event["recorded_at_iso"],
                "last_recorded": event["recorded_at_iso"],
            }
        stages[stage_name]["events"].append(event)
        stages[stage_name]["latest_data"] = event.get("data", {})
        stages[stage_name]["last_recorded"] = event["recorded_at_iso"]
        # Accumulate counts if present
        count = event.get("data", {}).get("count", 0)
        if isinstance(count, (int, float)):
            stages[stage_name]["total_count"] += count

    # Check funnel completeness
    completed_stages = set(stages.keys())
    all_stages = set(STAGE_FROM_NAME.keys())
    funnel_complete = all_stages.issubset(completed_stages)

    # Compute stage-to-stage conversion rates from actuals
    actual_conversions: dict[str, float] = {}
    stage_order = sorted(
        stages.items(),
        key=lambda x: STAGE_FROM_NAME.get(x[0], OutcomeStage.PLAN_CREATED).value,
    )
    for i in range(1, len(stage_order)):
        prev_name, prev_data = stage_order[i - 1]
        curr_name, curr_data = stage_order[i]
        prev_count = prev_data["total_count"]
        curr_count = curr_data["total_count"]
        if prev_count > 0:
            rate = curr_count / prev_count
            actual_conversions[f"{prev_name}_to_{curr_name}"] = round(rate, 4)

    return {
        "plan_id": plan_id,
        "role_family": meta.get("role_family", ""),
        "stages": stages,
        "actual_conversions": actual_conversions,
        "events_count": len(events),
        "funnel_complete": funnel_complete,
        "predicted": meta.get("predicted", {}),
    }


def get_outcome_trends(
    role_family: str = "",
    time_range_days: int = 90,
) -> dict[str, Any]:
    """Get aggregated outcome trends across plans for a role family.

    Args:
        role_family: Filter by role family (empty = all).
        time_range_days: How far back to look in days.

    Returns:
        Dict with aggregated conversion rates, plan count, and trends.
    """
    cutoff = time.time() - (time_range_days * 86400)
    role_filter = (role_family or "").strip().lower()

    with _lock:
        matching_plans: list[str] = []
        for pid, meta in _plan_meta.items():
            if meta.get("created_at", 0) < cutoff:
                continue
            if role_filter and meta.get("role_family", "") != role_filter:
                continue
            matching_plans.append(pid)

    # Aggregate conversion rates across matching plans
    rate_accumulators: dict[str, list[float]] = {}
    total_by_stage: dict[str, int] = {}

    for pid in matching_plans:
        funnel = get_plan_outcomes(pid)
        for key, rate in funnel.get("actual_conversions", {}).items():
            if key not in rate_accumulators:
                rate_accumulators[key] = []
            rate_accumulators[key].append(rate)
        for stage_name, stage_data in funnel.get("stages", {}).items():
            total_by_stage[stage_name] = total_by_stage.get(
                stage_name, 0
            ) + stage_data.get("total_count", 0)

    avg_conversions: dict[str, float] = {}
    for key, rates in rate_accumulators.items():
        if rates:
            avg_conversions[key] = round(sum(rates) / len(rates), 4)

    return {
        "role_family": role_filter or "all",
        "time_range_days": time_range_days,
        "plans_analyzed": len(matching_plans),
        "average_conversions": avg_conversions,
        "total_by_stage": total_by_stage,
        "baseline_conversions": BASELINE_CONVERSIONS.get(
            role_filter, BASELINE_CONVERSIONS.get(DEFAULT_ROLE_FAMILY, {})
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FEEDBACK LOOP
# ═══════════════════════════════════════════════════════════════════════════════


def compute_accuracy_score(plan_id: str) -> dict[str, Any]:
    """Compute how close predictions were to actual outcomes for a plan.

    Uses Mean Absolute Percentage Error (MAPE) for each stage where both
    predicted and actual values exist.

    Args:
        plan_id: Unique identifier for the media plan.

    Returns:
        Dict with overall_accuracy (0-1), per-stage accuracy, and deltas.
    """
    with _lock:
        _stats["total_accuracy_checks"] += 1
        meta = dict(_plan_meta.get(plan_id, {}))

    funnel = get_plan_outcomes(plan_id)
    predicted = meta.get("predicted", {})

    if not predicted:
        return {
            "plan_id": plan_id,
            "overall_accuracy": None,
            "message": "No predictions recorded for this plan",
            "stage_accuracy": {},
        }

    stage_accuracy: dict[str, dict[str, Any]] = {}
    errors: list[float] = []

    for stage_name, stage_data in funnel.get("stages", {}).items():
        actual_count = stage_data.get("total_count", 0)
        predicted_count = predicted.get(
            stage_name, predicted.get(f"predicted_{stage_name}")
        )

        if predicted_count is None or not isinstance(predicted_count, (int, float)):
            continue

        predicted_count = float(predicted_count)
        if predicted_count == 0 and actual_count == 0:
            accuracy = 1.0
            pct_error = 0.0
        elif predicted_count == 0:
            accuracy = 0.0
            pct_error = 1.0
        else:
            pct_error = abs(actual_count - predicted_count) / max(predicted_count, 1)
            accuracy = max(0.0, 1.0 - pct_error)

        errors.append(pct_error)
        stage_accuracy[stage_name] = {
            "predicted": predicted_count,
            "actual": actual_count,
            "delta": round(actual_count - predicted_count, 2),
            "pct_error": round(pct_error, 4),
            "accuracy": round(accuracy, 4),
        }

    overall_accuracy = round(1.0 - (sum(errors) / len(errors)), 4) if errors else None

    return {
        "plan_id": plan_id,
        "role_family": meta.get("role_family", ""),
        "overall_accuracy": overall_accuracy,
        "stage_accuracy": stage_accuracy,
        "stages_compared": len(stage_accuracy),
    }


def generate_improvement_suggestions(plan_id: str) -> list[str]:
    """Generate AI-driven suggestions based on the delta between predictions and actuals.

    Args:
        plan_id: Unique identifier for the media plan.

    Returns:
        List of actionable improvement suggestions.
    """
    accuracy = compute_accuracy_score(plan_id)
    stage_accuracy = accuracy.get("stage_accuracy", {})
    suggestions: list[str] = []

    if not stage_accuracy:
        return ["Record actual outcomes to receive improvement suggestions."]

    for stage_name, metrics in stage_accuracy.items():
        delta = metrics.get("delta", 0)
        pct_error = metrics.get("pct_error", 0)
        predicted = metrics.get("predicted", 0)
        actual = metrics.get("actual", 0)

        if pct_error < 0.1:
            continue  # Prediction was accurate enough

        if stage_name == "applications_received":
            if delta < 0:
                suggestions.append(
                    f"Applications were {abs(delta):.0f} below prediction "
                    f"({actual:.0f} vs {predicted:.0f}). Consider increasing ad spend, "
                    f"broadening job board selection, or improving job ad copy."
                )
            else:
                suggestions.append(
                    f"Applications exceeded prediction by {delta:.0f} "
                    f"({actual:.0f} vs {predicted:.0f}). Great channel performance -- "
                    f"consider reallocating budget from underperforming channels."
                )

        elif stage_name == "interviews_scheduled":
            if delta < 0:
                suggestions.append(
                    f"Interview conversion is {pct_error:.0%} below target. "
                    f"Review screening criteria -- they may be too strict. "
                    f"Consider adding phone screens to increase pipeline."
                )
            else:
                suggestions.append(
                    f"Interview scheduling exceeded expectations by {delta:.0f}. "
                    f"Screening process is efficient -- document and replicate."
                )

        elif stage_name == "offers_extended":
            if delta < 0:
                suggestions.append(
                    f"Offer rate is {pct_error:.0%} below prediction. "
                    f"Evaluate interviewer alignment on role requirements. "
                    f"Consider structured interview scorecards."
                )
            else:
                suggestions.append(
                    f"Offers exceeded prediction by {delta:.0f}. "
                    f"Strong candidate quality -- optimize sourcing channels."
                )

        elif stage_name == "hires_made":
            if delta < 0:
                suggestions.append(
                    f"Hires are {abs(delta):.0f} below target ({actual:.0f} vs "
                    f"{predicted:.0f}). Review offer competitiveness -- benchmark "
                    f"salary, benefits, and candidate experience against market."
                )
            else:
                suggestions.append(
                    f"Hiring exceeded target by {delta:.0f}. Pipeline is healthy. "
                    f"Consider reducing spend on high-cost channels while maintaining volume."
                )

    overall = accuracy.get("overall_accuracy")
    if overall is not None and overall < 0.7:
        suggestions.append(
            f"Overall prediction accuracy is {overall:.0%}. The model will "
            f"auto-adjust conversion rates using these results. Future plans "
            f"for this role family should be more accurate."
        )

    if not suggestions:
        suggestions.append(
            "Predictions closely match actuals. No major adjustments needed."
        )

    return suggestions


def update_prediction_model(
    outcomes: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Adjust conversion rates based on observed outcomes using exponential moving average.

    When called without arguments, processes all tracked plans. Otherwise
    processes the provided outcome events.

    Args:
        outcomes: Optional list of outcome dicts with plan_id, stage, data fields.

    Returns:
        Dict with updated rates, plans processed, and adjustment details.
    """
    with _lock:
        _stats["total_model_updates"] += 1
        plan_ids = list(_outcomes.keys()) if outcomes is None else []

    # If specific outcomes provided, extract plan IDs
    if outcomes:
        seen_pids: set[str] = set()
        for o in outcomes:
            pid = o.get("plan_id", "")
            if pid:
                seen_pids.add(pid)
        plan_ids = list(seen_pids)

    adjustments: list[dict[str, Any]] = []
    plans_processed = 0

    for pid in plan_ids:
        funnel = get_plan_outcomes(pid)
        role_family = funnel.get("role_family", DEFAULT_ROLE_FAMILY)
        actual_conversions = funnel.get("actual_conversions", {})

        if not actual_conversions:
            continue

        plans_processed += 1

        with _lock:
            if role_family not in _learned_rates:
                _learned_rates[role_family] = dict(
                    BASELINE_CONVERSIONS.get(
                        role_family,
                        BASELINE_CONVERSIONS.get(DEFAULT_ROLE_FAMILY, {}),
                    )
                )

            # Map actual conversion keys to learned rate keys
            rate_mapping = {
                "campaign_launched_to_applications_received": "campaign_launched_to_applications",
                "applications_received_to_interviews_scheduled": "applications_to_interviews",
                "interviews_scheduled_to_offers_extended": "interviews_to_offers",
                "offers_extended_to_hires_made": "offers_to_hires",
            }

            for actual_key, learned_key in rate_mapping.items():
                if actual_key not in actual_conversions:
                    continue
                actual_rate = actual_conversions[actual_key]
                old_rate = _learned_rates[role_family].get(learned_key)
                if old_rate is None:
                    continue

                # Exponential moving average update
                new_rate = round(
                    EMA_ALPHA * actual_rate + (1 - EMA_ALPHA) * old_rate, 6
                )
                _learned_rates[role_family][learned_key] = new_rate

                adjustments.append(
                    {
                        "role_family": role_family,
                        "rate_key": learned_key,
                        "old_rate": old_rate,
                        "actual_rate": round(actual_rate, 6),
                        "new_rate": new_rate,
                        "plan_id": pid,
                    }
                )

    with _lock:
        current_rates = {role: dict(rates) for role, rates in _learned_rates.items()}

    return {
        "plans_processed": plans_processed,
        "adjustments": adjustments,
        "current_learned_rates": current_rates,
        "ema_alpha": EMA_ALPHA,
    }


def get_learned_rates(role_family: str = "") -> dict[str, dict[str, float]]:
    """Get current learned conversion rates.

    Args:
        role_family: Filter by role family (empty = all).

    Returns:
        Dict of role_family -> conversion rates.
    """
    role_filter = (role_family or "").strip().lower()
    with _lock:
        if role_filter:
            rates = _learned_rates.get(role_filter)
            if rates:
                return {role_filter: dict(rates)}
            return {}
        return {role: dict(rates) for role, rates in _learned_rates.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# PREDICTION ACCURACY REPORT
# ═══════════════════════════════════════════════════════════════════════════════


def get_accuracy_report(
    role_family: str = "",
    time_range_days: int = 90,
) -> dict[str, Any]:
    """Generate a comprehensive prediction accuracy report across plans.

    Args:
        role_family: Filter by role family (empty = all).
        time_range_days: How far back to look in days.

    Returns:
        Dict with per-plan accuracy, aggregate stats, and learned rates.
    """
    cutoff = time.time() - (time_range_days * 86400)
    role_filter = (role_family or "").strip().lower()

    with _lock:
        matching_plans: list[str] = []
        for pid, meta in _plan_meta.items():
            if meta.get("created_at", 0) < cutoff:
                continue
            if role_filter and meta.get("role_family", "") != role_filter:
                continue
            matching_plans.append(pid)

    plan_accuracies: list[dict[str, Any]] = []
    accuracy_scores: list[float] = []

    for pid in matching_plans:
        acc = compute_accuracy_score(pid)
        plan_accuracies.append(acc)
        overall = acc.get("overall_accuracy")
        if overall is not None:
            accuracy_scores.append(overall)

    avg_accuracy = (
        round(sum(accuracy_scores) / len(accuracy_scores), 4)
        if accuracy_scores
        else None
    )

    return {
        "role_family": role_filter or "all",
        "time_range_days": time_range_days,
        "plans_analyzed": len(matching_plans),
        "plans_with_accuracy": len(accuracy_scores),
        "average_accuracy": avg_accuracy,
        "plan_details": plan_accuracies,
        "learned_rates": get_learned_rates(role_filter),
        "baseline_rates": BASELINE_CONVERSIONS.get(
            role_filter, BASELINE_CONVERSIONS if not role_filter else {}
        ),
        "improvement_vs_baseline": _compute_improvement_vs_baseline(role_filter),
    }


def _compute_improvement_vs_baseline(role_family: str = "") -> dict[str, Any]:
    """Compare learned rates against baselines to show model improvement.

    Args:
        role_family: Role family to compare (empty = aggregate all).

    Returns:
        Dict showing rate changes from baseline.
    """
    improvements: dict[str, dict[str, Any]] = {}
    with _lock:
        families = [role_family] if role_family else list(_learned_rates.keys())

    for family in families:
        baseline = BASELINE_CONVERSIONS.get(family, {})
        with _lock:
            learned = dict(_learned_rates.get(family, {}))

        family_improvements: dict[str, Any] = {}
        for key in baseline:
            base_val = baseline[key]
            learned_val = learned.get(key, base_val)
            if base_val > 0:
                change_pct = round(((learned_val - base_val) / base_val) * 100, 2)
            else:
                change_pct = 0.0
            family_improvements[key] = {
                "baseline": base_val,
                "learned": learned_val,
                "change_pct": change_pct,
            }
        improvements[family] = family_improvements

    return improvements


# ═══════════════════════════════════════════════════════════════════════════════
# HEALTH / STATS
# ═══════════════════════════════════════════════════════════════════════════════


def get_pipeline_stats() -> dict[str, Any]:
    """Get pipeline stats for /api/health integration.

    Returns:
        Dict with status, event counts, plan counts, uptime, and model info.
    """
    with _lock:
        stats_copy = dict(_stats)
        plans_count = len(_outcomes)
        learned_families = len(_learned_rates)

    uptime = time.time() - stats_copy.get("started_at", time.time())

    return {
        "status": "ok",
        "total_events_recorded": stats_copy.get("total_events_recorded", 0),
        "total_accuracy_checks": stats_copy.get("total_accuracy_checks", 0),
        "total_model_updates": stats_copy.get("total_model_updates", 0),
        "plans_tracked": plans_count,
        "learned_rate_families": learned_families,
        "ema_alpha": EMA_ALPHA,
        "uptime_seconds": round(uptime, 1),
        "stages": list(STAGE_NAMES.values()),
    }
