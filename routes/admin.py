"""Admin GET route handlers.

Extracted from app.py to reduce its size.  Every public function here
accepts ``handler`` (a ``MediaPlanHandler`` instance) and ``path`` (the
parsed URL path string).  Returns ``True`` if the route was handled.
"""

import datetime
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_admin_routes(handler, path: str, parsed: Any) -> bool:
    """Dispatch admin GET routes.  Returns True if handled."""
    _fn = _ADMIN_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _handle_admin_usage(handler, path: str, parsed: Any) -> None:
    """/api/admin/usage -- per-key usage dashboard (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    from app import _api_keys_lock, _api_keys_store, API_KEY_TIERS

    now = time.time()
    usage_data = {}
    with _api_keys_lock:
        for key, entry in _api_keys_store.items():
            masked = key[:8] + "..." if len(key) > 8 else key
            tier_name = entry.get("tier", "free")
            tier_limits = API_KEY_TIERS.get(tier_name, API_KEY_TIERS["free"])
            minute_usage = len(
                [t for t in entry.get("usage_minute") or [] if now - t < 60]
            )
            day_usage = len(
                [t for t in entry.get("usage_day") or [] if now - t < 86400]
            )
            usage_data[masked] = {
                "tier": tier_name,
                "label": entry.get("label") or "",
                "revoked": entry.get("revoked", False),
                "created": entry.get("created") or "",
                "requests_this_minute": minute_usage,
                "requests_today": day_usage,
                "limit_rpm": tier_limits["rpm"],
                "limit_rpd": tier_limits["rpd"],
            }
    handler._send_json(
        {
            "keys": usage_data,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    )


def _handle_admin_stats(handler, path: str, parsed: Any) -> None:
    """/api/admin/stats -- admin statistics endpoint (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    from app import load_request_log

    log_entries = load_request_log()
    total_plans = len(log_entries)
    gen_times = [
        e["generation_time_seconds"]
        for e in log_entries
        if isinstance(e.get("generation_time_seconds"), (int, float))
        and e["generation_time_seconds"] > 0
    ]
    avg_generation_time = (
        round(sum(gen_times) / len(gen_times), 2) if gen_times else 0.0
    )
    total_budget = 0.0
    for e in log_entries:
        raw_budget = e.get("budget") or 0
        if isinstance(raw_budget, (int, float)):
            total_budget += float(raw_budget)
        elif isinstance(raw_budget, str):
            try:
                total_budget += float(
                    raw_budget.replace(",", "").replace("$", "").strip()
                )
            except (ValueError, AttributeError):
                pass
    plans_by_industry: dict[str, int] = {}
    for e in log_entries:
        ind = e.get("industry", "Unknown") or "Unknown"
        plans_by_industry[ind] = plans_by_industry.get(ind, 0) + 1
    plans_by_day_map: dict[str, int] = {}
    for e in log_entries:
        ts = e.get("timestamp") or ""
        if ts:
            day = ts[:10]
            plans_by_day_map[day] = plans_by_day_map.get(day, 0) + 1
    plans_by_day = sorted(
        [{"date": d, "count": c} for d, c in plans_by_day_map.items()],
        key=lambda x: x["date"],
    )
    recent_plans = []
    for e in log_entries[-10:]:
        recent_plans.append(
            {
                "client_name": e.get("client_name", "Unknown"),
                "industry": e.get("industry", "Unknown"),
                "budget": e.get("budget") or 0,
                "timestamp": e.get("timestamp") or "",
            }
        )
    handler._send_json(
        {
            "total_plans": total_plans,
            "avg_generation_time": avg_generation_time,
            "total_budget_managed": round(total_budget, 2),
            "plans_by_industry": plans_by_industry,
            "plans_by_day": plans_by_day,
            "recent_plans": recent_plans,
        }
    )


def _handle_admin_posthog_stats(handler, path: str, parsed: Any) -> None:
    """/api/admin/posthog/stats -- PostHog analytics admin endpoint (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    from app import _posthog_available

    _ph_stats: dict[str, Any] = {}
    if _posthog_available:
        try:
            from app import _ph_get_stats

            _ph_stats["posthog_integration"] = _ph_get_stats()
        except Exception as _phe:
            _ph_stats["posthog_integration"] = {"error": str(_phe)}
    else:
        _ph_stats["posthog_integration"] = {"enabled": False}
    try:
        from posthog_tracker import get_posthog_stats as _ph_tracker_stats

        _ph_stats["posthog_tracker"] = _ph_tracker_stats()
    except ImportError:
        _ph_stats["posthog_tracker"] = {"enabled": False}
    except Exception as _phe2:
        _ph_stats["posthog_tracker"] = {"error": str(_phe2)}
    _ph_stats["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    handler._send_json(_ph_stats)


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_ADMIN_ROUTE_MAP: dict[str, Any] = {
    "/api/admin/usage": _handle_admin_usage,
    "/api/admin/stats": _handle_admin_stats,
    "/api/admin/posthog/stats": _handle_admin_posthog_stats,
}
