"""Admin GET route handlers.

Extracted from app.py to reduce its size.  Every public function here
accepts ``handler`` (a ``MediaPlanHandler`` instance) and ``path`` (the
parsed URL path string).  Returns ``True`` if the route was handled.
"""

import datetime
import hashlib
import json
import sys
import logging
import os
import threading
import time
from typing import Any

try:
    import resource as _resource
except ImportError:
    _resource = None  # Windows compatibility

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
    """/api/admin/usage -- per-key usage dashboard + system metrics (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _api_keys_lock = getattr(_app, "_api_keys_lock", None)
    _api_keys_store = getattr(_app, "_api_keys_store", None)
    API_KEY_TIERS = getattr(_app, "API_KEY_TIERS", None)

    now = time.time()
    usage_data = {}
    if _api_keys_lock and _api_keys_store and API_KEY_TIERS:
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

    # System resource metrics for admin dashboard
    memory_mb = 0.0
    try:
        if _resource is not None:
            mem_usage = _resource.getrusage(_resource.RUSAGE_SELF)
            if sys.platform == "darwin":
                # macOS reports ru_maxrss in bytes
                memory_mb = round(mem_usage.ru_maxrss / (1024 * 1024), 1)
            else:
                # Linux reports ru_maxrss in KB
                memory_mb = round(mem_usage.ru_maxrss / 1024, 1)
    except Exception:
        pass

    handler._send_json(
        {
            "keys": usage_data,
            "memory_mb": memory_mb,
            "threads": threading.active_count(),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    )


def _handle_admin_stats(handler, path: str, parsed: Any) -> None:
    """/api/admin/stats -- admin statistics endpoint (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    load_request_log = getattr(_app, "load_request_log", None)

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

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _posthog_available = getattr(_app, "_posthog_available", None)

    _ph_stats: dict[str, Any] = {}
    if _posthog_available:
        try:
            _app = sys.modules.get("__main__") or sys.modules.get("app")
            _ph_get_stats = getattr(_app, "_ph_get_stats", None)

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


def _handle_admin_plans(handler, path: str, parsed: Any) -> None:
    """/api/admin/plans -- list all generated media plans (admin-protected)."""
    if not handler._check_admin_auth():
        handler._send_json({"error": "Unauthorized"}, status_code=401)
        return

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    load_request_log = getattr(_app, "load_request_log", None)

    plans: list[dict[str, Any]] = []
    if load_request_log:
        try:
            log_entries = load_request_log()
            for e in log_entries:
                raw_budget = e.get("budget") or 0
                budget_val = 0.0
                if isinstance(raw_budget, (int, float)):
                    budget_val = float(raw_budget)
                elif isinstance(raw_budget, str):
                    try:
                        budget_val = float(
                            raw_budget.replace(",", "").replace("$", "").strip()
                        )
                    except (ValueError, AttributeError):
                        pass

                plans.append(
                    {
                        "client_name": e.get("client_name") or "Unknown",
                        "email": e.get("email") or e.get("generated_by") or "",
                        "timestamp": e.get("timestamp") or "",
                        "industry": e.get("industry") or "Unknown",
                        "budget": budget_val,
                        "channels": e.get("num_channels") or e.get("channels") or 0,
                        "status": "complete",
                        "generation_time": e.get("generation_time_seconds") or 0,
                    }
                )
        except Exception as exc:
            logger.error(
                "Failed to load request log for admin plans: %s", exc, exc_info=True
            )

    handler._send_json(
        {
            "plans": plans,
            "total": len(plans),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    )


def _handle_admin_llm_status(handler, path: str, parsed: Any) -> None:
    """/api/admin/llm-status -- LLM router status with provider health (admin-protected)."""
    if not handler._check_admin_auth():
        handler._send_json({"error": "Unauthorized"}, status_code=401)
        return

    result: dict[str, Any] = {
        "providers": {},
        "routing": {},
        "cost_tracking": {},
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    try:
        from llm_router import get_router_status, get_provider_health

        status = get_router_status()
        health = get_provider_health()

        # Merge health data into provider info
        providers = status.get("providers") or {}
        for pid, info in providers.items():
            h = health.get(pid) or {}
            info["health_score"] = h.get("health_score", 0.0)
            info["available"] = h.get("available", False)
            info["circuit_open"] = h.get("circuit_open", False)
            info["uptime_pct"] = h.get("uptime_pct", 0.0)

        result["providers"] = providers
        result["routing"] = status.get("routing") or {}
        result["cost_tracking"] = status.get("cost_tracking") or {}
        result["task_types"] = status.get("task_types") or []
        result["cache_hits"] = status.get("cache_hits", 0)
        result["cache_misses"] = status.get("cache_misses", 0)
    except ImportError:
        logger.warning("llm_router module not available for admin dashboard")
        result["error"] = "llm_router module not available"
    except Exception as exc:
        logger.error("Failed to get LLM router status: %s", exc, exc_info=True)
        result["error"] = str(exc)

    handler._send_json(result)


def _handle_admin_sessions(handler, path: str, parsed: Any) -> None:
    """/api/admin/sessions -- recent visitor sessions from rate limiter data (admin-protected)."""
    if not handler._check_admin_auth():
        handler._send_json({"error": "Unauthorized"}, status_code=401)
        return

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    sessions: list[dict[str, Any]] = []
    now = time.time()

    # Extract session data from rate limiter instances
    rate_limiters = {
        "generate": getattr(_app, "_rl_generate", None),
        "chat": getattr(_app, "_rl_chat", None),
        "llm_heavy": getattr(_app, "_rl_llm_heavy", None),
        "portal": getattr(_app, "_rl_portal", None),
        "general": getattr(_app, "_rl_general", None),
        "copilot": getattr(_app, "_rl_copilot", None),
    }

    # Aggregate IP data across all rate limiters
    ip_data: dict[str, dict[str, Any]] = {}
    for rl_name, rl in rate_limiters.items():
        if rl is None:
            continue
        try:
            with rl._lock:
                for ip, timestamps in rl._requests.items():
                    if not timestamps:
                        continue
                    # Hash IP for privacy
                    ip_hash = hashlib.sha256(ip.encode()).hexdigest()[:12]
                    if ip_hash not in ip_data:
                        ip_data[ip_hash] = {
                            "ip_hash": ip_hash,
                            "request_count": 0,
                            "last_seen_ts": 0.0,
                            "rate_limited": False,
                            "limiters": [],
                        }
                    recent = [t for t in timestamps if now - t < 86400]
                    ip_data[ip_hash]["request_count"] += len(recent)
                    if recent:
                        last_ts = max(recent)
                        if last_ts > ip_data[ip_hash]["last_seen_ts"]:
                            ip_data[ip_hash]["last_seen_ts"] = last_ts
                    ip_data[ip_hash]["limiters"].append(rl_name)
        except Exception as exc:
            logger.error(
                "Error reading rate limiter %s: %s", rl_name, exc, exc_info=True
            )

    # Convert to list and add formatted timestamps
    for entry in ip_data.values():
        ts = entry.pop("last_seen_ts", 0.0)
        if ts > 0:
            entry["last_seen"] = datetime.datetime.fromtimestamp(
                ts, tz=datetime.timezone.utc
            ).isoformat()
        else:
            entry["last_seen"] = ""
        sessions.append(entry)

    # Sort by most recent activity
    sessions.sort(key=lambda s: s.get("last_seen") or "", reverse=True)

    handler._send_json(
        {
            "sessions": sessions,
            "total_ips": len(sessions),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    )


def _handle_admin_audit_log(handler, path: str, parsed: Any) -> None:
    """/api/admin/audit-log -- recent audit events (admin-protected)."""
    if not handler._check_admin_auth():
        handler._send_json({"error": "Unauthorized"}, status_code=401)
        return

    events: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}

    try:
        from audit_logger import get_recent_events, get_audit_summary

        events = get_recent_events(limit=100)
        summary = get_audit_summary()
    except ImportError:
        logger.warning("audit_logger module not available for admin dashboard")
    except Exception as exc:
        logger.error("Failed to get audit log: %s", exc, exc_info=True)

    handler._send_json(
        {
            "events": events,
            "summary": summary,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    )


def _handle_admin_slow_endpoints(handler: Any, path: str, parsed: Any) -> None:
    """/api/admin/slow-endpoints -- top 10 slowest endpoints (admin-protected).

    Returns the endpoints that exceeded the 1-second threshold, sorted by
    duration descending. Useful for identifying performance bottlenecks.
    """
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    get_slow = getattr(_app, "get_slow_endpoints", None)

    if get_slow is None:
        handler._send_json(
            {"error": "Slow endpoint profiling not available"}, status_code=501
        )
        return

    slow_list = get_slow()

    # Also include Supabase connection metrics if available
    supabase_metrics: dict[str, Any] = {}
    try:
        from supabase_client import get_connection_metrics

        supabase_metrics = get_connection_metrics()
    except ImportError:
        pass

    # Background task executor metrics
    bg_metrics: dict[str, Any] = {}
    get_bg = getattr(_app, "get_background_task_metrics", None)
    if get_bg is not None:
        bg_metrics = get_bg()

    handler._send_json(
        {
            "slow_endpoints": slow_list,
            "threshold_ms": 1000,
            "count": len(slow_list),
            "supabase_connection_metrics": supabase_metrics,
            "background_task_metrics": bg_metrics,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    )


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_ADMIN_ROUTE_MAP: dict[str, Any] = {
    "/api/admin/usage": _handle_admin_usage,
    "/api/admin/stats": _handle_admin_stats,
    "/api/admin/posthog/stats": _handle_admin_posthog_stats,
    "/api/admin/plans": _handle_admin_plans,
    "/api/admin/llm-status": _handle_admin_llm_status,
    "/api/admin/sessions": _handle_admin_sessions,
    "/api/admin/audit-log": _handle_admin_audit_log,
    "/api/admin/slow-endpoints": _handle_admin_slow_endpoints,
}
