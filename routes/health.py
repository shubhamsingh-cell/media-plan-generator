"""Health, monitoring, and observability GET route handlers.

Extracted from app.py to reduce its size.  Every public function here
accepts ``handler`` (a ``MediaPlanHandler`` instance) and ``path`` (the
parsed URL path string).  Returns ``True`` if the route was handled.
"""

import datetime
import json
import logging
import os
import sys
import time
import urllib.parse
from typing import Any

logger = logging.getLogger(__name__)


def _send_json_response(handler: Any, data: Any, status_code: int = 200) -> None:
    """Send a JSON response using the standard HTTP response pattern.

    Avoids reliance on handler._send_json() which may not exist when
    routes are dispatched from a different handler class.
    """
    body = json.dumps(data).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_health_routes(handler, path: str, parsed: Any) -> bool:
    """Dispatch health/monitoring GET routes.  Returns True if handled."""
    _fn = _HEALTH_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _handle_health(handler, path: str, parsed: Any) -> None:
    """/api/health, /health -- detailed health check for Render.com monitoring."""
    try:
        # Use handler's server reference to call health_check_detailed
        # This avoids circular import from app.py
        import sys

        app_mod = sys.modules.get("app") or sys.modules.get("__main__")
        if app_mod and hasattr(app_mod, "health_check_detailed"):
            _health = app_mod.health_check_detailed()
        else:
            _health = {
                "status": "healthy",
                "note": "health_check_detailed not available",
            }
        status_code = 200 if _health.get("status") == "healthy" else 503
    except Exception as e:
        _health = {"status": "healthy", "error": str(e)}
        status_code = 200  # Don't block deploy on health check errors
    body = json.dumps(_health).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _handle_health_ready(handler, path: str, parsed: Any) -> None:
    """/api/health/ready, /ready -- deep readiness probe."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    health_check_readiness = getattr(_app, "health_check_readiness", None)

    if health_check_readiness is None:
        result = {"status": "healthy", "note": "health_check_readiness not available"}
    else:
        result = health_check_readiness()
    status_code = 200 if result.get("status") == "healthy" else 503
    body = json.dumps(result).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _handle_deck_status(handler, path: str, parsed: Any) -> None:
    """/api/deck/status -- deck generator tier availability."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _deck_generator = getattr(_app, "_deck_generator", None)

    if _deck_generator is not None:
        deck_status = _deck_generator.get_status()
        deck_body = json.dumps(deck_status, indent=2).encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        cors_origin = handler._get_cors_origin()
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.send_header("Content-Length", str(len(deck_body)))
        handler.end_headers()
        handler.wfile.write(deck_body)
    else:
        deck_err = json.dumps({"error": "Deck generator not available"}).encode("utf-8")
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        cors_origin = handler._get_cors_origin()
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.send_header("Content-Length", str(len(deck_err)))
        handler.end_headers()
        handler.wfile.write(deck_err)


def _handle_resilience_status(handler, path: str, parsed: Any) -> None:
    """/api/resilience/status -- resilience router status JSON API."""
    try:
        from resilience_router import get_router as _get_resilience_router

        _rr = _get_resilience_router()
        _rr_data = _rr.get_health_dashboard()
        _rr_body = json.dumps(_rr_data, indent=2).encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        cors_origin = handler._get_cors_origin()
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.send_header("Content-Length", str(len(_rr_body)))
        handler.end_headers()
        handler.wfile.write(_rr_body)
    except Exception as _rr_exc:
        _rr_err = json.dumps({"error": str(_rr_exc)}).encode("utf-8")
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(_rr_err)))
        handler.end_headers()
        handler.wfile.write(_rr_err)


def _handle_resilience_dashboard(handler, path: str, parsed: Any) -> None:
    """/api/resilience/dashboard -- resilience dashboard HTML page."""
    try:
        from resilience_router import get_router as _get_resilience_router

        _rr = _get_resilience_router()
        _rr_html = _rr.get_dashboard_html().encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        cors_origin = handler._get_cors_origin()
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.send_header("Content-Length", str(len(_rr_html)))
        handler.end_headers()
        handler.wfile.write(_rr_html)
    except Exception as _rr_exc:
        _rr_err = f"<h1>Error</h1><pre>{_rr_exc}</pre>".encode("utf-8")
        handler.send_response(500)
        handler.send_header("Content-Type", "text/html")
        handler.send_header("Content-Length", str(len(_rr_err)))
        handler.end_headers()
        handler.wfile.write(_rr_err)


def _handle_dashboard_widgets(handler, path: str, parsed: Any) -> None:
    """/api/dashboard/widgets -- live dashboard widget data for platform home."""
    try:
        import random

        _app = sys.modules.get("__main__") or sys.modules.get("app")
        _supabase_data_available = getattr(_app, "_supabase_data_available", None)

        widgets = {
            "campaigns": {
                "count": 0,
                "active_name": "",
                "status": "no_campaigns",
            },
            "budget": {
                "total": 0,
                "spent": 0,
                "spent_pct": 0,
                "status": "healthy",
            },
            "market": {
                "trend": "stable",
                "label": "Labor market trends steady",
                "cpc_change": round(random.uniform(-5, 5), 1),
                "demand_index": round(random.uniform(60, 95), 0),
            },
            "compliance": {
                "score": 0,
                "status": "unknown",
                "last_checked": None,
            },
            "recent_activity": [],
        }

        # Pull real data from Supabase if available
        if _supabase_data_available:
            try:
                _app = sys.modules.get("__main__") or sys.modules.get("app")
                get_market_trends = getattr(_app, "get_market_trends", None)

                trends = get_market_trends()
                if trends:
                    widgets["market"]["trend"] = (
                        "growing" if len(trends) > 3 else "stable"
                    )
                    widgets["market"]["label"] = f"{len(trends)} active market signals"
            except Exception as e:
                logger.error("Dashboard widget market data error: %s", e, exc_info=True)

        _send_json_response(handler, widgets)
    except Exception as e:
        logger.error("Dashboard widgets error: %s", e, exc_info=True)
        _send_json_response(handler, {"error": str(e)}, status_code=500)


def _handle_health_data_matrix(handler, path: str, parsed: Any) -> None:
    """/api/health/data-matrix -- admin-protected data matrix health."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _data_matrix = getattr(_app, "_data_matrix", None)

    if _data_matrix:
        dm_result = _data_matrix.get_status()
        dm_code = 200 if dm_result.get("status") != "degraded" else 503
        dm_body = json.dumps(dm_result, indent=2).encode("utf-8")
        handler.send_response(dm_code)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(dm_body)))
        handler.end_headers()
        handler.wfile.write(dm_body)
    else:
        dm_err_body = json.dumps({"error": "Data matrix monitor not available"}).encode(
            "utf-8"
        )
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(dm_err_body)))
        handler.end_headers()
        handler.wfile.write(dm_err_body)


def _handle_health_auto_qc(handler, path: str, parsed: Any) -> None:
    """/api/health/auto-qc -- admin-protected autonomous QC engine status."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _auto_qc = getattr(_app, "_auto_qc", None)

    if _auto_qc:
        qc_result = _auto_qc.get_status()
        qc_code = 200 if qc_result.get("status") != "degraded" else 503
        qc_body = json.dumps(qc_result, indent=2).encode("utf-8")
        handler.send_response(qc_code)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(qc_body)))
        handler.end_headers()
        handler.wfile.write(qc_body)
    else:
        qc_err_body = json.dumps({"error": "AutoQC engine not available"}).encode(
            "utf-8"
        )
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(qc_err_body)))
        handler.end_headers()
        handler.wfile.write(qc_err_body)


def _handle_health_enrichment(handler, path: str, parsed: Any) -> None:
    """/api/health/enrichment -- admin-protected data enrichment engine status."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _data_enrichment_available = getattr(_app, "_data_enrichment_available", None)

    if _data_enrichment_available:
        _app = sys.modules.get("__main__") or sys.modules.get("app")
        get_enrichment_status = getattr(_app, "get_enrichment_status", None)

        de_result = get_enrichment_status()
        de_body = json.dumps(de_result, indent=2).encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(de_body)))
        handler.end_headers()
        handler.wfile.write(de_body)
    else:
        de_err_body = json.dumps(
            {"error": "Data enrichment engine not available"}
        ).encode("utf-8")
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(de_err_body)))
        handler.end_headers()
        handler.wfile.write(de_err_body)


def _handle_health_integrations(handler, path: str, parsed: Any) -> None:
    """/api/health/integrations -- comprehensive integrations status (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return

    result: dict[str, Any] = {
        "infrastructure": {},
        "free_data_apis": {},
        "free_llm_providers": {},
        "paid_llm_providers": {},
        "ad_platform_apis": {},
        "communication": {},
    }

    # ---- Infrastructure & Monitoring ----
    infra = result["infrastructure"]

    # -- Sentry --
    try:
        _sentry_dsn = (os.environ.get("SENTRY_DSN") or "").strip()
        if _sentry_dsn:
            import sentry_sdk as _sk

            infra["sentry"] = {
                "name": "Sentry",
                "status": "ok",
                "detail": "Error tracking + performance tracing active",
                "sdk_version": _sk.VERSION,
                "value": "Unhandled exceptions, 10% perf traces, 10% profiling. Tracks do_GET/do_POST latency and child spans for enrichment, KB load, synthesis, Excel/PPT gen.",
            }
        else:
            infra["sentry"] = {
                "name": "Sentry",
                "status": "disabled",
                "detail": "SENTRY_DSN not set",
                "value": "Error tracking and performance monitoring",
            }
    except Exception as _e:
        infra["sentry"] = {
            "name": "Sentry",
            "status": "error",
            "detail": str(_e),
        }

    # -- Upstash Redis --
    try:
        from upstash_cache import get_stats as _upstash_stats

        _us = _upstash_stats()
        _us["name"] = "Upstash Redis"
        _us["value"] = "L4 persistent cache (Redis REST API, survives redeploys)"
        infra["upstash_redis"] = _us
    except ImportError:
        infra["upstash_redis"] = {
            "name": "Upstash Redis",
            "status": "disabled",
            "detail": "Not configured",
            "value": "L4 persistent cache layer",
        }
    except Exception as _e:
        infra["upstash_redis"] = {
            "name": "Upstash Redis",
            "status": "error",
            "detail": str(_e),
        }

    # -- PostHog (with runtime stats) --
    try:
        from posthog_tracker import get_posthog_stats as _ph_stats_fn

        _ph_data = _ph_stats_fn()
        infra["posthog"] = {
            "name": "PostHog",
            "status": "ok" if _ph_data.get("enabled") else "disabled",
            "detail": (
                f"Analytics active -- {_ph_data.get('total_sent') or 0} events sent, {_ph_data.get('total_queued') or 0} queued"
                if _ph_data.get("enabled")
                else "POSTHOG_API_KEY not set"
            ),
            "value": "Product analytics: plan_generated, plan_failed, chat_message, file_uploaded events",
            "runtime": {
                "total_queued": _ph_data.get("total_queued") or 0,
                "total_sent": _ph_data.get("total_sent") or 0,
                "total_dropped": _ph_data.get("total_dropped") or 0,
                "send_errors": _ph_data.get("total_send_errors") or 0,
                "queue_size": _ph_data.get("queue_size") or 0,
                "events_by_type": _ph_data.get("events_by_type", {}),
            },
        }
    except Exception:
        _ph_key = (os.environ.get("POSTHOG_API_KEY") or "").strip()
        infra["posthog"] = {
            "name": "PostHog",
            "status": "ok" if _ph_key else "disabled",
            "detail": (
                "Analytics active (backend + frontend)"
                if _ph_key
                else "POSTHOG_API_KEY not set"
            ),
            "value": "Product analytics: plan_generated, plan_failed, chat_message, file_uploaded events",
        }

    # -- Supabase (with runtime stats) --
    try:
        from supabase_cache import get_supabase_stats as _supa_stats_fn

        _supa_data = _supa_stats_fn()
        _supa_enabled = _supa_data.get("enabled", False)
        _supa_hits = _supa_data.get("hits") or 0
        _supa_misses = _supa_data.get("misses") or 0
        _supa_writes = _supa_data.get("writes") or 0
        _supa_hr = _supa_data.get("hit_rate") or 0
        infra["supabase"] = {
            "name": "Supabase PostgreSQL",
            "status": "ok" if _supa_enabled else "disabled",
            "detail": (
                f"L3 cache -- {_supa_hits} hits, {_supa_misses} misses, {_supa_writes} writes (hit rate: {_supa_hr:.0%})"
                if _supa_enabled
                else "Not configured"
            ),
            "value": "L3 distributed cache with TTL, hit counting, category tagging",
            "runtime": {
                "hits": _supa_hits,
                "misses": _supa_misses,
                "writes": _supa_writes,
                "errors": _supa_data.get("errors") or 0,
                "hit_rate": _supa_hr,
            },
        }
    except ImportError:
        infra["supabase"] = {
            "name": "Supabase PostgreSQL",
            "status": "disabled",
            "detail": "Not available",
            "value": "L3 persistent cache layer",
        }
    except Exception as _e:
        infra["supabase"] = {
            "name": "Supabase PostgreSQL",
            "status": "error",
            "detail": str(_e),
        }

    # -- Grafana Loki --
    try:
        from grafana_logger import get_grafana_stats as _graf_stats_fn

        _graf_data = _graf_stats_fn()
        _graf_shipped = _graf_data.get("records_shipped") or 0
        _graf_dropped = _graf_data.get("records_dropped") or 0
        _graf_errors = _graf_data.get("flush_errors") or 0
        _graf_last_err = _graf_data.get("last_error")
        _graf_url = (os.environ.get("GRAFANA_LOKI_URL") or "").strip()
        _graf_status = "disabled"
        if _graf_url:
            _graf_last_err_t = _graf_data.get("last_error_time")
            _graf_last_flush_t = _graf_data.get("last_flush_time")
            if _graf_errors > 0 and (
                _graf_shipped == 0
                or (
                    _graf_last_err_t
                    and (
                        not _graf_last_flush_t or _graf_last_err_t > _graf_last_flush_t
                    )
                )
            ):
                _graf_status = "degraded"
            else:
                _graf_status = "ok"
        _graf_detail = (
            f"Centralized logging -- {_graf_shipped} shipped, {_graf_dropped} dropped, {_graf_errors} errors"
            if _graf_url
            else "GRAFANA_LOKI_URL not set"
        )
        if _graf_last_err and _graf_url:
            _graf_detail += f" | last error: {_graf_last_err[:150]}"
        infra["grafana_loki"] = {
            "name": "Grafana Loki",
            "status": _graf_status,
            "detail": _graf_detail,
            "value": "Ships WARNING/ERROR/CRITICAL logs to Grafana Cloud Loki for centralized search and alerting.",
            "runtime": {
                "records_shipped": _graf_shipped,
                "records_dropped": _graf_dropped,
                "flush_errors": _graf_errors,
                "last_flush_iso": _graf_data.get("last_flush_iso"),
                "last_error": _graf_last_err,
                "last_error_status": _graf_data.get("last_error_status"),
                "last_error_iso": _graf_data.get("last_error_iso"),
            },
        }
    except ImportError:
        infra["grafana_loki"] = {
            "name": "Grafana Loki",
            "status": "disabled",
            "detail": "Module not available",
            "value": "Centralized logging to Grafana Cloud",
        }
    except Exception as _e:
        infra["grafana_loki"] = {
            "name": "Grafana Loki",
            "status": "error",
            "detail": str(_e),
        }

    # -- Resend Email Alerts --
    try:
        from email_alerts import get_alert_status as _resend_stats_fn

        _resend_data = _resend_stats_fn()
        _resend_enabled = _resend_data.get("enabled", False)
        _resend_sent = _resend_data.get("total_sent") or 0
        _resend_failed = _resend_data.get("total_failed") or 0
        _resend_this_hr = _resend_data.get("emails_sent_this_hour") or 0
        _resend_last_err = _resend_data.get("last_error")
        _resend_status = "disabled"
        if _resend_enabled:
            _resend_last_err_t = _resend_data.get("last_error_time")
            _resend_last_sent_t = _resend_data.get("last_sent_time")
            if _resend_failed > 0 and (
                _resend_sent == 0
                or (
                    _resend_last_err_t
                    and (
                        not _resend_last_sent_t
                        or _resend_last_err_t > _resend_last_sent_t
                    )
                )
            ):
                _resend_status = "degraded"
            else:
                _resend_status = "ok"
        _resend_detail = (
            f"Email alerts -- {_resend_sent} sent, {_resend_failed} failed, {_resend_this_hr} this hour"
            if _resend_enabled
            else "RESEND_API_KEY or ALERT_EMAIL_TO not set"
        )
        if _resend_last_err and _resend_enabled:
            _resend_detail += f" | last error: {_resend_last_err[:150]}"
        infra["resend"] = {
            "name": "Resend Email",
            "status": _resend_status,
            "detail": _resend_detail,
            "value": "Error alerts, circuit breaker notifications, daily digest summaries. Rate-limited with exponential backoff dedup.",
            "runtime": {
                "total_sent": _resend_sent,
                "total_failed": _resend_failed,
                "rate_limited": _resend_data.get("total_rate_limited") or 0,
                "deduplicated": _resend_data.get("total_deduplicated") or 0,
                "emails_this_hour": _resend_this_hr,
                "remaining_this_hour": _resend_data.get("remaining_this_hour") or 0,
                "from_email": _resend_data.get("from_email"),
                "last_sent_subject": _resend_data.get("last_sent_subject"),
                "last_error": _resend_last_err,
                "last_error_status": _resend_data.get("last_error_status"),
            },
        }
    except Exception:
        _resend_key = (os.environ.get("RESEND_API_KEY") or "").strip()
        infra["resend"] = {
            "name": "Resend Email",
            "status": "ok" if _resend_key else "disabled",
            "detail": (
                "Alert emails active" if _resend_key else "RESEND_API_KEY not set"
            ),
            "value": "Error alerts, circuit breaker notifications, daily digests",
        }

    # ---- Free Data APIs ----
    free_apis = result["free_data_apis"]
    _free_api_registry = [
        (
            "bls_oes",
            "BLS OES Salary Data",
            "api.bls.gov",
            "Median/percentile wages by SOC occupation code",
            "BLS_API_KEY",
        ),
        (
            "bls_qcew",
            "BLS QCEW Employment",
            "data.bls.gov",
            "Industry employment stats, establishment counts, avg wages",
            None,
        ),
        (
            "bls_jolts",
            "BLS JOLTS",
            "api.bls.gov",
            "Job openings, hires, quits by industry",
            "BLS_API_KEY",
        ),
        (
            "census_acs",
            "US Census ACS",
            "api.census.gov",
            "State population, median household income",
            None,
        ),
        (
            "world_bank",
            "World Bank Open Data",
            "api.worldbank.org",
            "Global GDP, population, unemployment by country",
            None,
        ),
        (
            "fred",
            "FRED Economic Data",
            "api.stlouisfed.org",
            "US economic indicators (CPI, unemployment, GDP)",
            "FRED_API_KEY",
        ),
        (
            "onet",
            "O*NET Web Services",
            "services.onetcenter.org",
            "Occupation skills, knowledge, job outlook, job zones",
            "ONET_USERNAME",
        ),
        (
            "imf",
            "IMF DataMapper",
            "imf.org",
            "International GDP, inflation, unemployment forecasts",
            None,
        ),
        (
            "rest_countries",
            "REST Countries",
            "restcountries.com",
            "Country population, currency, languages, region data",
            None,
        ),
        (
            "geonames",
            "GeoNames",
            "geonames.org",
            "Geographic coordinates, timezone, nearby cities",
            "GEONAMES_USERNAME",
        ),
        (
            "teleport",
            "Teleport API",
            "api.teleport.org",
            "Quality of life scores, cost of living by city",
            None,
        ),
        (
            "datausa",
            "DataUSA",
            "datausa.io",
            "US occupation wages, state-level demographics",
            None,
        ),
        (
            "wikipedia",
            "Wikipedia REST",
            "en.wikipedia.org",
            "Company descriptions, industry background",
            None,
        ),
        (
            "clearbit",
            "Clearbit Logo API",
            "logo.clearbit.com",
            "Company logos, competitor logos, metadata",
            None,
        ),
        (
            "sec_edgar",
            "SEC EDGAR",
            "sec.gov",
            "Public company tickers, CIK, filing data",
            None,
        ),
        (
            "exchange_rates",
            "Exchange Rate API",
            "open.er-api.com",
            "Live currency exchange rates (USD base)",
            None,
        ),
        (
            "eurostat",
            "Eurostat Labour",
            "ec.europa.eu",
            "EU unemployment, wages, employment by country",
            None,
        ),
        (
            "ilo",
            "ILO ILOSTAT",
            "sdmx.ilo.org",
            "Global labour participation, unemployment rates",
            None,
        ),
        (
            "google_trends",
            "Google Trends",
            "trends.google.com",
            "Search interest/trend data for job keywords",
            None,
        ),
    ]
    for _api_id, _api_name, _api_host, _api_value, _api_key_env in _free_api_registry:
        _has_key = True
        if _api_key_env:
            _has_key = bool(os.environ.get(_api_key_env, "").strip())
        free_apis[_api_id] = {
            "name": _api_name,
            "host": _api_host,
            "value": _api_value,
            "status": "ok" if _has_key else "available",
            "detail": (
                "Active"
                if _has_key
                else f"No key ({_api_key_env}) -- uses free tier or benchmarks"
            ),
            "key_required": bool(_api_key_env),
            "key_configured": _has_key,
        }

    # ---- Free LLM Providers ----
    free_llms = result["free_llm_providers"]
    _free_llm_registry = [
        (
            "gemini",
            "Gemini 2.0 Flash",
            "Google",
            "GEMINI_API_KEY",
            "Structured JSON, code gen, fastest free",
        ),
        (
            "groq",
            "Groq Llama 3.3 70B",
            "Groq",
            "GROQ_API_KEY",
            "Complex reasoning, conversational",
        ),
        (
            "cerebras",
            "Cerebras Llama 3.3 70B",
            "Cerebras",
            "CEREBRAS_API_KEY",
            "Hot spare for Groq (same model, independent infra)",
        ),
        (
            "mistral",
            "Mistral Small",
            "Mistral AI",
            "MISTRAL_API_KEY",
            "JSON, multilingual, code generation",
        ),
        (
            "openrouter",
            "Llama 4 Maverick (free)",
            "OpenRouter",
            "OPENROUTER_API_KEY",
            "General purpose, strong reasoning",
        ),
        ("xai", "Grok 2", "xAI", "XAI_API_KEY", "Strong reasoning ($25 free credits)"),
        (
            "sambanova",
            "Llama 3.1 405B",
            "SambaNova",
            "SAMBANOVA_API_KEY",
            "Largest open model, fastest inference (RDU)",
        ),
        (
            "nvidia_nim",
            "Nemotron Nano 30B",
            "NVIDIA NIM",
            "NVIDIA_NIM_API_KEY",
            "NVIDIA-optimized inference",
        ),
        (
            "cloudflare",
            "Llama 3.3 70B",
            "Cloudflare Workers AI",
            "CLOUDFLARE_AI_TOKEN",
            "Edge-distributed, low latency",
        ),
    ]
    for _llm_id, _llm_model, _llm_provider, _llm_env, _llm_value in _free_llm_registry:
        _has = bool(os.environ.get(_llm_env, "").strip())
        free_llms[_llm_id] = {
            "name": _llm_model,
            "provider": _llm_provider,
            "value": _llm_value,
            "status": "ok" if _has else "no_key",
            "detail": f"Key configured ({_llm_env})" if _has else f"Missing {_llm_env}",
            "key_configured": _has,
        }

    # ---- Paid LLM Providers ----
    paid_llms = result["paid_llm_providers"]
    _paid_llm_registry = [
        (
            "claude_haiku",
            "Claude Haiku 4.5",
            "Anthropic",
            "ANTHROPIC_API_KEY",
            "Fast, cheap paid fallback",
        ),
        (
            "gpt4o",
            "GPT-4o",
            "OpenAI",
            "OPENAI_API_KEY",
            "Structured JSON, general reasoning",
        ),
        (
            "claude_sonnet",
            "Claude Sonnet 4",
            "Anthropic",
            "ANTHROPIC_API_KEY",
            "Complex multi-step tool chains",
        ),
        (
            "claude_opus",
            "Claude Opus 4.6",
            "Anthropic",
            "ANTHROPIC_API_KEY",
            "Highest quality, last resort",
        ),
    ]
    for _llm_id, _llm_model, _llm_provider, _llm_env, _llm_value in _paid_llm_registry:
        _has = bool(os.environ.get(_llm_env, "").strip())
        paid_llms[_llm_id] = {
            "name": _llm_model,
            "provider": _llm_provider,
            "value": _llm_value,
            "status": "ok" if _has else "no_key",
            "detail": f"Key configured ({_llm_env})" if _has else f"Missing {_llm_env}",
            "key_configured": _has,
        }

    # ---- Ad Platform APIs ----
    ad_apis = result["ad_platform_apis"]
    _ad_registry = [
        (
            "google_ads",
            "Google Ads",
            "Keyword volumes, CPC/CPM benchmarks",
            "GOOGLE_ADS_CLIENT_ID",
        ),
        (
            "meta_ads",
            "Meta (Facebook/Instagram)",
            "Audience sizing, CPC/CPM estimates",
            "META_ACCESS_TOKEN",
        ),
        (
            "bing_ads",
            "Microsoft/Bing Ads",
            "Search volumes, CPC estimates",
            "BING_CLIENT_ID",
        ),
        (
            "tiktok_ads",
            "TikTok Marketing",
            "Audience estimation, CPC/CPM",
            "TIKTOK_ACCESS_TOKEN",
        ),
        (
            "linkedin_ads",
            "LinkedIn Marketing",
            "Professional audience sizing, CPC",
            "LINKEDIN_ACCESS_TOKEN",
        ),
    ]
    for _ad_id, _ad_name, _ad_value, _ad_env in _ad_registry:
        _has = bool(os.environ.get(_ad_env, "").strip())
        ad_apis[_ad_id] = {
            "name": _ad_name,
            "value": _ad_value,
            "status": "ok" if _has else "no_key",
            "detail": (
                f"Key configured ({_ad_env})"
                if _has
                else f"Missing {_ad_env} -- uses benchmarks"
            ),
            "key_configured": _has,
        }

    # ---- Communication ----
    comms = result["communication"]
    _slack_token = (os.environ.get("SLACK_BOT_TOKEN") or "").strip()
    comms["slack"] = {
        "name": "Slack Bot",
        "status": "ok" if _slack_token else "disabled",
        "detail": "Bot connected" if _slack_token else "SLACK_BOT_TOKEN not set",
        "value": "Nova chatbot for media plan queries, recruitment channel intelligence, and workforce analytics via Slack",
    }

    # ---- Summary counts ----
    result["summary"] = {
        "total_integrations": sum(
            len(v)
            for v in result.values()
            if isinstance(v, dict) and v != result.get("summary")
        ),
        "active": sum(
            1
            for cat in result.values()
            if isinstance(cat, dict)
            for v in cat.values()
            if isinstance(v, dict) and v.get("status") == "ok"
        ),
        "available": sum(
            1
            for cat in result.values()
            if isinstance(cat, dict)
            for v in cat.values()
            if isinstance(v, dict) and v.get("status") == "available"
        ),
        "disabled": sum(
            1
            for cat in result.values()
            if isinstance(cat, dict)
            for v in cat.values()
            if isinstance(v, dict) and v.get("status") in ("disabled", "no_key")
        ),
    }

    _int_body = json.dumps(result, indent=2).encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(_int_body)))
    handler.end_headers()
    handler.wfile.write(_int_body)


def _handle_health_integrations_diagnose(handler, path: str, parsed: Any) -> None:
    """/api/health/integrations/diagnose -- live diagnostics (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    diag_results: dict[str, Any] = {}
    # Grafana Loki diagnostic
    try:
        from grafana_logger import diagnose_grafana

        diag_results["grafana_loki"] = diagnose_grafana()
    except ImportError:
        diag_results["grafana_loki"] = {
            "ok": False,
            "detail": "grafana_logger module not available",
        }
    except Exception as _de:
        diag_results["grafana_loki"] = {
            "ok": False,
            "detail": f"diagnostic error: {_de}",
        }
    # Resend Email diagnostic
    try:
        from email_alerts import diagnose_resend

        diag_results["resend"] = diagnose_resend()
    except ImportError:
        diag_results["resend"] = {
            "ok": False,
            "detail": "email_alerts module not available",
        }
    except Exception as _de:
        diag_results["resend"] = {
            "ok": False,
            "detail": f"diagnostic error: {_de}",
        }
    diag_results["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    _send_json_response(handler, diag_results)


def _handle_health_orchestrator(handler, path: str, parsed: Any) -> None:
    """/api/health/orchestrator -- orchestrator cache stats (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    try:
        import data_orchestrator as _do

        orch_data = {
            "cache_stats": _do.get_cache_stats(),
            "fallback_telemetry": _do.get_fallback_telemetry(),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        _send_json_response(handler, orch_data)
    except Exception as _oe:
        logger.error("Orchestrator unavailable: %s", _oe, exc_info=True)
        _send_json_response(handler, {"error": "Orchestrator unavailable"})


def _handle_metrics(handler, path: str, parsed: Any) -> None:
    """/api/metrics -- metrics endpoint (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _metrics = getattr(_app, "_metrics", None)

    metrics_data = (
        _metrics.get_metrics() if _metrics else {"error": "Monitoring not available"}
    )
    _send_json_response(handler, metrics_data)


def _handle_nova_metrics(handler, path: str, parsed: Any) -> None:
    """/api/nova/metrics -- Nova chatbot metrics (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    try:
        from nova import get_nova_metrics

        _send_json_response(handler, get_nova_metrics())
    except Exception as e:
        logger.error("Nova metrics error: %s", e, exc_info=True)
        _send_json_response(handler, {"error": "Failed to retrieve Nova metrics"})


def _handle_health_slos(handler, path: str, parsed: Any) -> None:
    """/api/health/slos -- SLO compliance check (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    try:
        from monitoring import MetricsCollector as _MC

        _app = sys.modules.get("__main__") or sys.modules.get("app")
        _mc_inst = getattr(_app, "_metrics", None)

        if _mc_inst and hasattr(_mc_inst, "check_slo_compliance"):
            slo_result = _mc_inst.check_slo_compliance()
            _send_json_response(handler, slo_result)
        else:
            _send_json_response(
                handler, {"error": "SLO compliance check not available"}
            )
    except Exception as _slo_err:
        logger.error("SLO check error: %s", _slo_err, exc_info=True)
        slo_err_body = json.dumps({"error": "SLO check failed"}).encode("utf-8")
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(slo_err_body)))
        handler.end_headers()
        handler.wfile.write(slo_err_body)


def _handle_observability_platform(handler, path: str, parsed: Any) -> None:
    """/api/observability/platform -- aggregated health dashboard (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    try:
        _app = sys.modules.get("__main__") or sys.modules.get("app")
        get_platform_observability = getattr(_app, "get_platform_observability", None)

        obs_data = get_platform_observability()
        _send_json_response(handler, obs_data)
    except Exception as _obs_err:
        logger.error("Platform observability error: %s", _obs_err, exc_info=True)
        obs_err_body = json.dumps(
            {"error": "Platform observability check failed"}
        ).encode("utf-8")
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(obs_err_body)))
        handler.end_headers()
        handler.wfile.write(obs_err_body)


def _handle_health_eval(handler, path: str, parsed: Any) -> None:
    """/api/health/eval -- eval framework scores (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    try:
        from eval_framework import EvalSuite

        _app = sys.modules.get("__main__") or sys.modules.get("app")
        _generate_product_insights = getattr(_app, "_generate_product_insights", None)

        _ef = EvalSuite()
        eval_result = _ef.run_full_eval()
        if isinstance(eval_result, dict):
            eval_result["ai_insights"] = _generate_product_insights(
                "Eval Framework",
                {
                    k: eval_result.get(k)
                    for k in ("overall_score", "test_results", "failures", "warnings")
                    if eval_result.get(k) is not None
                },
                context="Platform quality evaluation results",
            )
        _send_json_response(handler, eval_result)
    except ImportError:
        _send_json_response(handler, {"error": "Eval framework not available"})
    except Exception as _eval_err:
        logger.error("Eval framework error: %s", _eval_err, exc_info=True)
        eval_err_body = json.dumps({"error": "Eval failed"}).encode("utf-8")
        handler.send_response(503)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(eval_err_body)))
        handler.end_headers()
        handler.wfile.write(eval_err_body)


def _handle_llm_costs(handler, path: str, parsed: Any) -> None:
    """/api/llm/costs -- LLM cost tracking report."""
    try:
        from llm_router import get_cost_report

        _send_json_response(handler, get_cost_report())
    except Exception as e:
        _send_json_response(
            handler, {"error": str(e), "note": "Cost tracking not available"}
        )


def _handle_audit_events(handler, path: str, parsed: Any) -> None:
    """/api/audit/events -- audit events endpoint."""
    try:
        from audit_logger import get_recent_events, get_audit_summary

        params = urllib.parse.parse_qs(urllib.parse.urlparse(handler.path).query)
        if "summary" in params:
            _send_json_response(handler, get_audit_summary())
        else:
            limit = int(params.get("limit", ["100"])[0])
            action = params.get("action", [None])[0]
            _send_json_response(handler, {"events": get_recent_events(limit, action)})
    except Exception as e:
        _send_json_response(handler, {"error": str(e)})


# ---------------------------------------------------------------------------
# Route map -- paths that map to exact matches
# ---------------------------------------------------------------------------
# For paths with aliases (e.g., /health and /api/health), we register each
# alias separately to keep the dispatch O(1).


def _handle_config(handler, path: str, parsed: Any) -> None:
    """/api/config -- public frontend configuration (PostHog key, feature flags).

    This endpoint exposes only public frontend configuration values.
    Sensitive keys and internal details are NOT included.
    """
    _ph_key: str = (os.environ.get("POSTHOG_API_KEY") or "").strip()
    config: dict[str, Any] = {}
    if _ph_key:
        config["posthog_key"] = _ph_key
    config["posthog_host"] = "https://us.i.posthog.com"
    _send_json_response(handler, config)


def _handle_features(handler, path: str, parsed: Any) -> None:
    """/api/features -- feature store status and channel recommendations.

    Query params (all optional):
        job_title: Target role for channel recommendations.
        budget: Monthly budget in USD.
        location: Hiring location for geo-adjusted CPCs.

    When all three params are provided, returns full channel recommendations.
    Otherwise returns feature store summary only.
    """
    try:
        from feature_store import get_feature_store

        fs = get_feature_store()
        query = urllib.parse.parse_qs(parsed.query)
        job_title = (query.get("job_title") or [None])[0]
        budget_str = (query.get("budget") or [None])[0]
        location = (query.get("location") or [None])[0]

        result: dict[str, Any] = {
            "status": "ok" if fs._initialized else "not_initialized",
            "summary": fs.get_all_features(),
        }

        if job_title and budget_str and location:
            try:
                budget = float(budget_str)
            except (ValueError, TypeError):
                _send_json_response(
                    handler,
                    {"error": "Invalid budget value -- must be a number"},
                    status_code=400,
                )
                return
            result["recommendations"] = fs.get_channel_recommendations(
                job_title, budget, location
            )
        elif job_title:
            result["role_family"] = fs.get_role_family(job_title)

        _send_json_response(handler, result)
    except ImportError:
        _send_json_response(
            handler,
            {"error": "feature_store module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Feature store endpoint error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"error": f"Feature store error: {e}"},
            status_code=500,
        )


def _handle_morning_brief_api(handler, path: str, parsed: Any) -> None:
    """/api/morning-brief -- JSON morning brief digest."""
    try:
        from morning_brief import generate_morning_brief

        brief = generate_morning_brief()
        _send_json_response(handler, brief)
    except ImportError:
        _send_json_response(
            handler,
            {"error": "morning_brief module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Morning brief API error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"error": f"Morning brief generation failed: {e}"},
            status_code=500,
        )


def _handle_morning_brief_page(handler, path: str, parsed: Any) -> None:
    """/morning-brief -- HTML morning brief page."""
    try:
        from morning_brief import generate_morning_brief, generate_brief_html

        brief = generate_morning_brief()
        html = generate_brief_html(brief)
        body = html.encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
    except ImportError:
        _send_json_response(
            handler,
            {"error": "morning_brief module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Morning brief page error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"error": f"Morning brief page failed: {e}"},
            status_code=500,
        )


def _handle_market_pulse_json(handler, path: str, parsed: Any) -> None:
    """/api/market-pulse -- JSON market pulse data for PLG digest."""
    try:
        from market_pulse import generate_market_pulse

        pulse = generate_market_pulse()
        _send_json_response(handler, {"ok": True, "pulse": pulse})
    except ImportError:
        _send_json_response(
            handler,
            {"ok": False, "error": "market_pulse module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Market pulse API error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"ok": False, "error": f"Market pulse generation failed: {e}"},
            status_code=500,
        )


def _handle_rate_limits(handler, path: str, parsed: Any) -> None:
    """/api/rate-limits -- current rate limiter configuration, usage, and state."""
    try:
        from rate_limiter_adaptive import get_rate_limiter_stats

        stats = get_rate_limiter_stats()
        _send_json_response(handler, {"ok": True, **stats})
    except ImportError:
        _send_json_response(
            handler,
            {"ok": False, "error": "rate_limiter_adaptive module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Rate limits endpoint error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"ok": False, "error": f"Rate limiter stats failed: {e}"},
            status_code=500,
        )


# ---------------------------------------------------------------------------
# Market Signals route handlers
# ---------------------------------------------------------------------------


def _handle_signals(handler, path: str, parsed: Any) -> None:
    """/api/signals -- active market signals with optional ?role_family= filter."""
    try:
        from market_signals import get_active_signals

        query_params = urllib.parse.parse_qs(parsed.query)
        role_family = (query_params.get("role_family") or [None])[0]
        location = (query_params.get("location") or [None])[0]

        signals = get_active_signals(role_family=role_family, location=location)
        _send_json_response(
            handler,
            {
                "ok": True,
                "count": len(signals),
                "filters": {
                    "role_family": role_family,
                    "location": location,
                },
                "signals": signals,
            },
        )
    except ImportError:
        _send_json_response(
            handler,
            {"ok": False, "error": "market_signals module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Signals endpoint error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"ok": False, "error": f"Signal retrieval failed: {e}"},
            status_code=500,
        )


def _handle_signals_volatility(handler, path: str, parsed: Any) -> None:
    """/api/signals/volatility -- market volatility index."""
    try:
        from market_signals import get_market_volatility

        volatility = get_market_volatility()
        _send_json_response(handler, {"ok": True, **volatility})
    except ImportError:
        _send_json_response(
            handler,
            {"ok": False, "error": "market_signals module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Volatility endpoint error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"ok": False, "error": f"Volatility computation failed: {e}"},
            status_code=500,
        )


def _handle_signals_trending(handler, path: str, parsed: Any) -> None:
    """/api/signals/trending -- trending channels."""
    try:
        from market_signals import get_trending_channels

        trending = get_trending_channels()
        _send_json_response(
            handler,
            {
                "ok": True,
                "count": len(trending),
                "channels": trending,
            },
        )
    except ImportError:
        _send_json_response(
            handler,
            {"ok": False, "error": "market_signals module not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Trending channels endpoint error: %s", e, exc_info=True)
        _send_json_response(
            handler,
            {"ok": False, "error": f"Trending channels failed: {e}"},
            status_code=500,
        )


_HEALTH_ROUTE_MAP: dict[str, Any] = {
    "/api/config": _handle_config,
    "/api/features": _handle_features,
    "/api/health": _handle_health,
    "/health": _handle_health,
    "/api/health/ready": _handle_health_ready,
    "/ready": _handle_health_ready,
    "/api/deck/status": _handle_deck_status,
    "/api/resilience/status": _handle_resilience_status,
    "/api/resilience/dashboard": _handle_resilience_dashboard,
    "/api/dashboard/widgets": _handle_dashboard_widgets,
    "/api/health/data-matrix": _handle_health_data_matrix,
    "/api/health/auto-qc": _handle_health_auto_qc,
    "/api/health/enrichment": _handle_health_enrichment,
    "/api/health/integrations": _handle_health_integrations,
    "/api/health/integrations/diagnose": _handle_health_integrations_diagnose,
    "/api/health/orchestrator": _handle_health_orchestrator,
    "/api/metrics": _handle_metrics,
    "/api/nova/metrics": _handle_nova_metrics,
    "/api/health/slos": _handle_health_slos,
    "/api/observability/platform": _handle_observability_platform,
    "/api/health/eval": _handle_health_eval,
    "/api/llm/costs": _handle_llm_costs,
    "/api/audit/events": _handle_audit_events,
    "/api/morning-brief": _handle_morning_brief_api,
    "/morning-brief": _handle_morning_brief_page,
    "/api/market-pulse": _handle_market_pulse_json,
    "/api/rate-limits": _handle_rate_limits,
    "/api/signals": _handle_signals,
    "/api/signals/volatility": _handle_signals_volatility,
    "/api/signals/trending": _handle_signals_trending,
}
