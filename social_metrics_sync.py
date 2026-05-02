"""Live social ads metrics sync: Meta Ads + Google Ads -> Supabase.

Replaces the Supermetrics -> Google Sheets pipeline with a direct
background sync. Pulls daily per-campaign performance from Meta Marketing
API and Google Ads API, normalizes to a unified schema, and upserts into
the `social_campaign_metrics` table on Supabase. Nova chatbot tools then
query this table to answer questions about live campaign performance.

Required env vars
-----------------
ENABLE_SOCIAL_METRICS_SYNC=1   -- gate the background scheduler
SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY  -- already set
META_ACCESS_TOKEN, META_AD_ACCOUNT_ID    -- already set
GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CUSTOMER_ID  -- already set
GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET
GOOGLE_ADS_LOGIN_CUSTOMER_ID  -- optional MCC manager ID (digits only)
SOCIAL_METRICS_LOOKBACK_DAYS  -- optional; default 7
SOCIAL_METRICS_SYNC_INTERVAL_HOURS  -- optional; default 6
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_META_BASE = "https://graph.facebook.com/v19.0"
_GADS_BASE = "https://googleads.googleapis.com/v16"
_GADS_TOKEN_URL = "https://oauth2.googleapis.com/token"
_TABLE_METRICS = "social_campaign_metrics"
_TABLE_SYNC_LOG = "social_metrics_sync_log"

_DEFAULT_LOOKBACK_DAYS = 7
_DEFAULT_INTERVAL_HOURS = 6
_HTTP_TIMEOUT = 30
_ssl_ctx = ssl.create_default_context()
_lock = threading.Lock()


# ── env helpers ────────────────────────────────────────────────────────────
def _is_enabled() -> bool:
    """Background sync is opt-in to avoid surprising cost spikes."""
    val = (os.environ.get("ENABLE_SOCIAL_METRICS_SYNC") or "").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _lookback_days() -> int:
    raw = os.environ.get("SOCIAL_METRICS_LOOKBACK_DAYS") or ""
    try:
        n = int(raw.strip()) if raw.strip() else _DEFAULT_LOOKBACK_DAYS
        return max(1, min(n, 90))  # bound to [1, 90]
    except (TypeError, ValueError):
        return _DEFAULT_LOOKBACK_DAYS


def _interval_seconds() -> int:
    raw = os.environ.get("SOCIAL_METRICS_SYNC_INTERVAL_HOURS") or ""
    try:
        h = float(raw.strip()) if raw.strip() else _DEFAULT_INTERVAL_HOURS
        return int(max(1.0, min(h, 24.0)) * 3600)
    except (TypeError, ValueError):
        return _DEFAULT_INTERVAL_HOURS * 3600


def _date_range(days: int) -> Tuple[str, str]:
    """Return (start_date, end_date) in YYYY-MM-DD. End is yesterday (UTC)."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


# ── Meta Ads pull ──────────────────────────────────────────────────────────
def _meta_creds() -> Tuple[Optional[str], Optional[str]]:
    token = (os.environ.get("META_ACCESS_TOKEN") or "").strip() or None
    raw = (os.environ.get("META_AD_ACCOUNT_ID") or "").strip()
    if not raw:
        return token, None
    account = raw if raw.startswith("act_") else f"act_{raw}"
    return token, account


def _safe_float(val: Any) -> float:
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(val: Any) -> int:
    try:
        return int(float(val)) if val is not None else 0
    except (TypeError, ValueError):
        return 0


def _meta_conversions(actions: Any) -> float:
    """Sum Meta `actions` entries that look like conversions or apps.

    Meta returns a list of {action_type, value}; we count any
    submit_application, lead, complete_registration, or onsite_conversion
    as a conversion since this is recruitment-focused.
    """
    if not isinstance(actions, list):
        return 0.0
    targets = {
        "submit_application_total",
        "submit_application",
        "lead",
        "leadgen_grouped",
        "complete_registration",
        "onsite_conversion.lead_grouped",
        "offsite_conversion.fb_pixel_lead",
        "offsite_conversion.fb_pixel_complete_registration",
    }
    total = 0.0
    for a in actions:
        if not isinstance(a, dict):
            continue
        if (a.get("action_type") or "") in targets:
            total += _safe_float(a.get("value"))
    return total


def pull_meta_insights(days: int) -> List[Dict[str, Any]]:
    """Pull daily per-campaign insights from Meta Marketing API.

    Returns a list of unified-schema rows, or [] if unconfigured / on error.
    """
    token, account = _meta_creds()
    if not token or not account:
        logger.info(
            "Meta sync skipped: META_ACCESS_TOKEN or META_AD_ACCOUNT_ID not set"
        )
        return []

    start, end = _date_range(days)
    params = {
        "access_token": token,
        "level": "campaign",
        "time_increment": "1",
        "time_range": json.dumps({"since": start, "until": end}),
        "fields": (
            "campaign_id,campaign_name,objective,spend,impressions,clicks,"
            "ctr,cpc,cpm,cpp,actions,date_start,account_currency"
        ),
        "limit": "500",
    }
    url = f"{_META_BASE}/{account}/insights?{urllib.parse.urlencode(params)}"

    rows: List[Dict[str, Any]] = []
    page = 0
    while url and page < 20:  # safety: max 20 paginations (~10k rows)
        page += 1
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(
                req, context=_ssl_ctx, timeout=_HTTP_TIMEOUT
            ) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            err_body = ""
            try:
                err_body = exc.read().decode("utf-8")
            except OSError:
                pass
            logger.error("Meta insights HTTP %d: %s", exc.code, err_body, exc_info=True)
            break
        except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
            logger.error("Meta insights request failed: %s", exc, exc_info=True)
            break

        if "error" in body:
            logger.error("Meta API error: %s", body["error"])
            break

        for r in body.get("data") or []:
            spend = _safe_float(r.get("spend"))
            impressions = _safe_int(r.get("impressions"))
            clicks = _safe_int(r.get("clicks"))
            conversions = _meta_conversions(r.get("actions"))
            row_date = r.get("date_start") or ""
            cpa = round(spend / conversions, 4) if conversions > 0 else None
            rows.append(
                {
                    "platform": "meta",
                    "account_id": account,
                    "campaign_id": r.get("campaign_id") or "",
                    "campaign_name": r.get("campaign_name") or "",
                    "objective": r.get("objective") or "",
                    "date": row_date,
                    "spend": round(spend, 4),
                    "impressions": impressions,
                    "clicks": clicks,
                    "conversions": round(conversions, 4),
                    "ctr": round(_safe_float(r.get("ctr")), 4),
                    "cpc": round(_safe_float(r.get("cpc")), 4),
                    "cpm": round(_safe_float(r.get("cpm")), 4),
                    "cpa": cpa,
                    "currency": r.get("account_currency") or "USD",
                    "raw": r,
                }
            )

        # Follow pagination
        paging = (body.get("paging") or {}).get("next")
        url = paging or ""

    logger.info("Meta sync: pulled %d daily campaign rows", len(rows))
    return rows


# ── Google Ads pull ────────────────────────────────────────────────────────
def _gads_creds() -> Optional[Dict[str, str]]:
    """Return all Google Ads creds, or None if any are missing."""
    creds = {
        "developer_token": (os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip(),
        "refresh_token": (os.environ.get("GOOGLE_ADS_REFRESH_TOKEN") or "").strip(),
        "client_id": (os.environ.get("GOOGLE_ADS_CLIENT_ID") or "").strip(),
        "client_secret": (os.environ.get("GOOGLE_ADS_CLIENT_SECRET") or "").strip(),
        "customer_id": (os.environ.get("GOOGLE_ADS_CUSTOMER_ID") or "")
        .strip()
        .replace("-", ""),
        "login_customer_id": (os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "")
        .strip()
        .replace("-", ""),
    }
    if not all(
        [
            creds["developer_token"],
            creds["refresh_token"],
            creds["client_id"],
            creds["client_secret"],
            creds["customer_id"],
        ]
    ):
        return None
    return creds


def _gads_access_token(creds: Dict[str, str]) -> Optional[str]:
    """Refresh a Google Ads OAuth2 access token from the refresh token."""
    payload = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "refresh_token": creds["refresh_token"],
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        _GADS_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            req, context=_ssl_ctx, timeout=_HTTP_TIMEOUT
        ) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return data.get("access_token")
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Google Ads OAuth2 refresh failed: %s", exc, exc_info=True)
        return None


def pull_google_ads_insights(days: int) -> List[Dict[str, Any]]:
    """Pull daily per-campaign metrics via the Google Ads searchStream endpoint."""
    creds = _gads_creds()
    if not creds:
        logger.info(
            "Google Ads sync skipped: missing one of GOOGLE_ADS_DEVELOPER_TOKEN, "
            "REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET, CUSTOMER_ID"
        )
        return []

    access_token = _gads_access_token(creds)
    if not access_token:
        return []

    start, end = _date_range(days)
    gaql = (
        "SELECT "
        "campaign.id, campaign.name, campaign.advertising_channel_type, "
        "segments.date, "
        "metrics.cost_micros, metrics.impressions, metrics.clicks, "
        "metrics.ctr, metrics.average_cpc, metrics.average_cpm, "
        "metrics.conversions, metrics.cost_per_conversion "
        "FROM campaign "
        f"WHERE segments.date BETWEEN '{start}' AND '{end}'"
    )
    url = f"{_GADS_BASE}/customers/{creds['customer_id']}/googleAds:searchStream"
    body = json.dumps({"query": gaql}).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": creds["developer_token"],
        "Content-Type": "application/json",
    }
    if creds["login_customer_id"]:
        headers["login-customer-id"] = creds["login_customer_id"]

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(
            req, context=_ssl_ctx, timeout=_HTTP_TIMEOUT
        ) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8")
        except OSError:
            pass
        logger.error(
            "Google Ads searchStream HTTP %d: %s", exc.code, err_body, exc_info=True
        )
        return []
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Google Ads searchStream failed: %s", exc, exc_info=True)
        return []

    # searchStream returns either a single dict {results:[...]} or a list of
    # such batch dicts depending on response framing. Handle both.
    batches = payload if isinstance(payload, list) else [payload]
    rows: List[Dict[str, Any]] = []
    for batch in batches:
        for r in (batch or {}).get("results") or []:
            campaign = r.get("campaign") or {}
            segments = r.get("segments") or {}
            metrics = r.get("metrics") or {}
            cost_micros = _safe_float(metrics.get("costMicros"))
            spend = round(cost_micros / 1_000_000.0, 4)
            impressions = _safe_int(metrics.get("impressions"))
            clicks = _safe_int(metrics.get("clicks"))
            conversions = _safe_float(metrics.get("conversions"))
            cpa = round(spend / conversions, 4) if conversions > 0 else None
            cpc_micros = _safe_float(metrics.get("averageCpc"))
            cpm_micros = _safe_float(metrics.get("averageCpm"))
            rows.append(
                {
                    "platform": "google_ads",
                    "account_id": creds["customer_id"],
                    "campaign_id": str(campaign.get("id") or ""),
                    "campaign_name": campaign.get("name") or "",
                    "objective": campaign.get("advertisingChannelType") or "",
                    "date": segments.get("date") or "",
                    "spend": spend,
                    "impressions": impressions,
                    "clicks": clicks,
                    "conversions": round(conversions, 4),
                    "ctr": round(_safe_float(metrics.get("ctr")) * 100.0, 4),
                    "cpc": round(cpc_micros / 1_000_000.0, 4),
                    "cpm": round(cpm_micros / 1_000_000.0, 4),
                    "cpa": cpa,
                    "currency": "USD",
                    "raw": r,
                }
            )

    logger.info("Google Ads sync: pulled %d daily campaign rows", len(rows))
    return rows


# ── Supabase upsert ────────────────────────────────────────────────────────
def _upsert_metrics(rows: List[Dict[str, Any]]) -> int:
    """Upsert rows into social_campaign_metrics. Returns count upserted."""
    if not rows:
        return 0
    try:
        from supabase_client import get_client
    except ImportError:
        logger.warning("supabase_client not importable; skipping upsert")
        return 0

    client = get_client()
    if client is None:
        logger.warning("Supabase client unavailable; skipping upsert")
        return 0

    # Drop empty-date rows defensively (rare; would violate NOT NULL)
    rows = [r for r in rows if r.get("date") and r.get("campaign_id")]
    if not rows:
        return 0

    # Supabase Python client batches well at 500/req. Conflict target matches
    # our UNIQUE constraint so this is a true upsert.
    BATCH = 500
    upserted = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        try:
            (
                client.table(_TABLE_METRICS)
                .upsert(chunk, on_conflict="platform,account_id,campaign_id,date")
                .execute()
            )
            upserted += len(chunk)
        except Exception as exc:
            logger.error(
                "Supabase upsert failed (chunk %d-%d): %s",
                i,
                i + len(chunk),
                exc,
                exc_info=True,
            )
    return upserted


def _log_sync(
    platform: str,
    rows_upserted: int,
    status: str,
    duration_ms: int,
    error: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Best-effort write to social_metrics_sync_log. Never raises."""
    try:
        from supabase_client import get_client

        client = get_client()
        if client is None:
            return
        client.table(_TABLE_SYNC_LOG).insert(
            {
                "platform": platform,
                "finished_at": datetime.utcnow().isoformat(),
                "status": status,
                "rows_upserted": rows_upserted,
                "duration_ms": duration_ms,
                "error": (error or "")[:2000] if error else None,
                "metadata": metadata or {},
            }
        ).execute()
    except Exception as exc:
        logger.warning("sync log write failed: %s", exc)


# ── orchestrator ───────────────────────────────────────────────────────────
def run_sync(days: Optional[int] = None) -> Dict[str, Any]:
    """Pull Meta + Google Ads insights and upsert to Supabase. Idempotent.

    Returns a summary dict with per-platform counts and overall status.
    Safe to call manually or from the background scheduler.
    """
    days = days if days is not None else _lookback_days()
    summary: Dict[str, Any] = {"days": days, "platforms": {}}

    for platform, puller in (
        ("meta", pull_meta_insights),
        ("google_ads", pull_google_ads_insights),
    ):
        t0 = time.time()
        rows: List[Dict[str, Any]] = []
        status = "ok"
        error: Optional[str] = None
        try:
            rows = puller(days)
        except Exception as exc:
            status = "error"
            error = str(exc)
            logger.error("%s pull failed: %s", platform, exc, exc_info=True)
        upserted = 0
        if rows and status == "ok":
            try:
                upserted = _upsert_metrics(rows)
                if upserted < len(rows):
                    status = "partial"
            except Exception as exc:
                status = "error"
                error = str(exc)
                logger.error("%s upsert failed: %s", platform, exc, exc_info=True)
        elif not rows and status == "ok":
            status = "skipped"  # unconfigured or no data
        duration_ms = int((time.time() - t0) * 1000)
        _log_sync(platform, upserted, status, duration_ms, error)
        summary["platforms"][platform] = {
            "status": status,
            "rows_pulled": len(rows),
            "rows_upserted": upserted,
            "duration_ms": duration_ms,
            "error": error,
        }

    summary["finished_at"] = datetime.utcnow().isoformat()
    return summary


# ── background scheduler ───────────────────────────────────────────────────
_scheduler_started = False


def _scheduled_run() -> None:
    """Self-rescheduling timer: run sync, then schedule the next run."""
    interval = _interval_seconds()
    try:
        result = run_sync()
        logger.info(
            "social_metrics_sync run complete: %s",
            json.dumps({k: v.get("status") for k, v in result["platforms"].items()}),
        )
    except Exception as exc:
        logger.error("social_metrics_sync run failed: %s", exc, exc_info=True)
    finally:
        t = threading.Timer(interval, _scheduled_run)
        t.daemon = True
        t.name = "social-metrics-sync"
        t.start()


def start_background_sync() -> bool:
    """Start the background sync scheduler if ENABLE_SOCIAL_METRICS_SYNC=1.

    Uses a jittered first-run delay (in [60s, interval)) to avoid timer
    reconvergence with other 6-12h schedulers in the same worker.
    Returns True if started, False if disabled or already running.
    """
    global _scheduler_started
    if _scheduler_started:
        return False
    if not _is_enabled():
        logger.info(
            "social_metrics_sync: DISABLED (set ENABLE_SOCIAL_METRICS_SYNC=1 to enable)"
        )
        return False

    with _lock:
        if _scheduler_started:
            return False
        _scheduler_started = True

    interval = _interval_seconds()
    first_delay = 60.0 + secrets.SystemRandom().uniform(0.0, float(interval))
    t = threading.Timer(first_delay, _scheduled_run)
    t.daemon = True
    t.name = "social-metrics-sync"
    t.start()
    logger.info(
        "social_metrics_sync scheduler started (first run in %.1fh, jittered "
        "from %.1fh base, lookback=%d days)",
        first_delay / 3600.0,
        interval / 3600.0,
        _lookback_days(),
    )
    return True


# ── chatbot helpers (used by Nova tools) ───────────────────────────────────
def query_performance(
    platform: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    campaign_filter: Optional[str] = None,
    top_n: int = 5,
) -> Dict[str, Any]:
    """Aggregate performance for `platform` over a date range. Returns totals
    + top-N campaigns by spend. Used by Nova's query_meta_performance and
    query_google_ads_performance tools.
    """
    if platform not in {"meta", "google_ads"}:
        return {"error": f"unknown platform: {platform}"}

    try:
        from supabase_client import get_client
    except ImportError:
        return {"error": "supabase_client not available"}
    client = get_client()
    if client is None:
        return {"error": "Supabase not configured"}

    if not end_date:
        end_date = (date.today() - timedelta(days=1)).isoformat()
    if not start_date:
        # Default to 7 days ending at end_date
        try:
            ed = date.fromisoformat(end_date)
        except ValueError:
            ed = date.today() - timedelta(days=1)
        start_date = (ed - timedelta(days=6)).isoformat()

    try:
        q = (
            client.table(_TABLE_METRICS)
            .select("*")
            .eq("platform", platform)
            .gte("date", start_date)
            .lte("date", end_date)
        )
        if campaign_filter:
            # ilike on campaign_name for fuzzy filtering
            q = q.ilike("campaign_name", f"%{campaign_filter}%")
        resp = q.execute()
        rows = resp.data or []
    except Exception as exc:
        logger.error("query_performance failed: %s", exc, exc_info=True)
        return {"error": f"query failed: {exc}"}

    if not rows:
        return {
            "platform": platform,
            "start_date": start_date,
            "end_date": end_date,
            "rows": 0,
            "message": "No data found. Check that ENABLE_SOCIAL_METRICS_SYNC=1 "
            "and the sync has run at least once.",
            "totals": {},
            "top_campaigns": [],
        }

    # Totals
    spend = sum(_safe_float(r.get("spend")) for r in rows)
    impressions = sum(_safe_int(r.get("impressions")) for r in rows)
    clicks = sum(_safe_int(r.get("clicks")) for r in rows)
    conversions = sum(_safe_float(r.get("conversions")) for r in rows)
    ctr = (clicks / impressions * 100.0) if impressions > 0 else 0.0
    cpc = (spend / clicks) if clicks > 0 else 0.0
    cpm = (spend / impressions * 1000.0) if impressions > 0 else 0.0
    cpa = (spend / conversions) if conversions > 0 else None

    # Top N campaigns by spend
    by_camp: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        cid = r.get("campaign_id") or ""
        agg = by_camp.setdefault(
            cid,
            {
                "campaign_id": cid,
                "campaign_name": r.get("campaign_name") or "",
                "spend": 0.0,
                "impressions": 0,
                "clicks": 0,
                "conversions": 0.0,
            },
        )
        agg["spend"] += _safe_float(r.get("spend"))
        agg["impressions"] += _safe_int(r.get("impressions"))
        agg["clicks"] += _safe_int(r.get("clicks"))
        agg["conversions"] += _safe_float(r.get("conversions"))
    top = sorted(by_camp.values(), key=lambda x: x["spend"], reverse=True)[
        : max(1, top_n)
    ]
    for c in top:
        c["spend"] = round(c["spend"], 2)
        c["conversions"] = round(c["conversions"], 2)
        c["ctr_pct"] = (
            round(c["clicks"] / c["impressions"] * 100.0, 3)
            if c["impressions"] > 0
            else 0.0
        )
        c["cpc"] = round(c["spend"] / c["clicks"], 4) if c["clicks"] > 0 else 0.0
        c["cpa"] = (
            round(c["spend"] / c["conversions"], 4) if c["conversions"] > 0 else None
        )

    return {
        "platform": platform,
        "start_date": start_date,
        "end_date": end_date,
        "rows": len(rows),
        "campaign_filter": campaign_filter or "",
        "totals": {
            "spend": round(spend, 2),
            "impressions": impressions,
            "clicks": clicks,
            "conversions": round(conversions, 2),
            "ctr_pct": round(ctr, 3),
            "cpc": round(cpc, 4),
            "cpm": round(cpm, 4),
            "cpa": round(cpa, 4) if cpa is not None else None,
            "currency": rows[0].get("currency") or "USD",
        },
        "top_campaigns": top,
        "data_source": "social_campaign_metrics (live sync from Meta/Google Ads APIs)",
    }


def query_daily_rows(
    platform: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    campaign_filter: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Return raw daily campaign rows from social_campaign_metrics.

    Used by the /api/campaign-intel/metrics endpoint to serve CSV exports
    (drop-in replacement for the Google Sheet that Campaign Intelligence
    currently fetches manually).
    """
    if platform not in {"meta", "google_ads"}:
        return []

    try:
        from supabase_client import get_client
    except ImportError:
        return []
    client = get_client()
    if client is None:
        return []

    if not end_date:
        end_date = (date.today() - timedelta(days=1)).isoformat()
    if not start_date:
        try:
            ed = date.fromisoformat(end_date)
        except ValueError:
            ed = date.today() - timedelta(days=1)
        start_date = (ed - timedelta(days=29)).isoformat()  # default 30 days for CSV

    try:
        q = (
            client.table(_TABLE_METRICS)
            .select(
                "platform,account_id,campaign_id,campaign_name,objective,date,"
                "spend,impressions,clicks,conversions,ctr,cpc,cpa,cpm,currency"
            )
            .eq("platform", platform)
            .gte("date", start_date)
            .lte("date", end_date)
            .order("date", desc=True)
            .limit(max(1, min(limit, 50000)))
        )
        if campaign_filter:
            q = q.ilike("campaign_name", f"%{campaign_filter}%")
        resp = q.execute()
        return resp.data or []
    except Exception as exc:
        logger.error("query_daily_rows failed: %s", exc, exc_info=True)
        return []


def get_status() -> Dict[str, Any]:
    """Health check for the social metrics sync module."""
    meta_token, meta_account = _meta_creds()
    gads = _gads_creds()
    return {
        "module": "social_metrics_sync",
        "enabled": _is_enabled(),
        "scheduler_running": _scheduler_started,
        "lookback_days": _lookback_days(),
        "interval_hours": _interval_seconds() / 3600,
        "meta": {
            "configured": bool(meta_token and meta_account),
            "account_id": meta_account if meta_account else None,
        },
        "google_ads": {
            "configured": gads is not None,
            "customer_id": gads["customer_id"] if gads else None,
            "has_login_customer": bool(gads and gads.get("login_customer_id")),
        },
    }
