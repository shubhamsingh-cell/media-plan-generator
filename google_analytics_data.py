"""Google Analytics 4 Data API integration for Nova AI Suite.

GA4 reporting: traffic overview, top pages, traffic sources, geo breakdown,
realtime users, and chatbot tool handler. Uses GA4 Data API v1 REST endpoints
(stdlib urllib only). Auth via GOOGLE_SLIDES_CREDENTIALS_B64 env var.

Env: GOOGLE_SLIDES_CREDENTIALS_B64 (shared), GA4_PROPERTY_ID (default property).
"""

from __future__ import annotations

import base64, json, logging, os, ssl, threading, time
import urllib.error, urllib.parse, urllib.request
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_GA4_BASE = "https://analyticsdata.googleapis.com/v1beta"
_TOKEN_URI = "https://oauth2.googleapis.com/token"
_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
_token_lock = threading.Lock()
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0.0}
_cache_lock = threading.Lock()
_result_cache: Dict[str, Any] = {}
_CACHE_TTL = 300  # 5 minutes

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _cache_key(fn: str, *a: Any) -> str:
    return f"{fn}:{json.dumps(a, sort_keys=True, default=str)}"


def _cache_get(key: str) -> Optional[Any]:
    with _cache_lock:
        e = _result_cache.get(key)
        if e and e["exp"] > time.time():
            return e["val"]
        if e:
            del _result_cache[key]
    return None


def _cache_set(key: str, val: Any) -> None:
    with _cache_lock:
        _result_cache[key] = {"val": val, "exp": time.time() + _CACHE_TTL}


# ---------------------------------------------------------------------------
# Credential + token helpers (mirrors google_ads_analytics.py pattern)
# ---------------------------------------------------------------------------


def _load_credentials() -> Optional[Dict[str, str]]:
    """Load service-account JSON from GOOGLE_SLIDES_CREDENTIALS_B64."""
    b64 = os.environ.get("GOOGLE_SLIDES_CREDENTIALS_B64") or ""
    if not b64:
        return None
    try:
        creds = json.loads(base64.b64decode(b64))
        for f in ("client_email", "private_key", "token_uri"):
            if f not in creds:
                logger.error("Service account JSON missing field: %s", f)
                return None
        return creds
    except (ValueError, json.JSONDecodeError) as exc:
        logger.error("Failed to load B64 credentials: %s", exc, exc_info=True)
        return None


def _build_jwt(creds: Dict[str, str]) -> str:
    """Build a signed RS256 JWT for OAuth2 token exchange."""
    import subprocess, tempfile

    now = int(time.time())

    def _b64url(d: bytes) -> str:
        return base64.urlsafe_b64encode(d).rstrip(b"=").decode("ascii")

    hdr = _b64url(
        json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode()
    )
    clm = _b64url(
        json.dumps(
            {
                "iss": creds["client_email"],
                "scope": " ".join(_SCOPES),
                "aud": creds.get("token_uri") or _TOKEN_URI,
                "iat": now,
                "exp": now + 3600,
            },
            separators=(",", ":"),
        ).encode()
    )
    si = f"{hdr}.{clm}".encode("ascii")
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        key = serialization.load_pem_private_key(
            creds["private_key"].encode(), password=None
        )
        sig = key.sign(si, padding.PKCS1v15(), hashes.SHA256())
        return f"{hdr}.{clm}.{_b64url(sig)}"
    except ImportError:
        pass
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
            kf.write(creds["private_key"])
            key_path = kf.name
        try:
            proc = subprocess.run(
                ["openssl", "dgst", "-sha256", "-sign", key_path],
                input=si,
                capture_output=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                return f"{hdr}.{clm}.{_b64url(proc.stdout)}"
        finally:
            try:
                os.unlink(key_path)
            except OSError:
                pass
    except FileNotFoundError:
        pass
    raise RuntimeError(
        "Cannot sign JWT: install 'cryptography' or ensure 'openssl' on PATH"
    )


def _get_access_token() -> Optional[str]:
    """Obtain a Google OAuth2 access token. Thread-safe with caching."""
    with _token_lock:
        now = time.time()
        if _token_cache["token"] and _token_cache["expires_at"] > now + 300:
            return _token_cache["token"]
        creds = _load_credentials()
        if not creds:
            return None
        try:
            jwt_tok = _build_jwt(creds)
        except RuntimeError as exc:
            logger.error("JWT signing failed: %s", exc, exc_info=True)
            return None
        payload = urllib.parse.urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:2.0-jwt-bearer",
                "assertion": jwt_tok,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            creds.get("token_uri") or _TOKEN_URI,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
                td = json.loads(resp.read().decode("utf-8"))
            _token_cache["token"] = td["access_token"]
            _token_cache["expires_at"] = now + td.get("expires_in", 3600)
            return _token_cache["token"]
        except (urllib.error.URLError, json.JSONDecodeError, KeyError) as exc:
            logger.error("OAuth2 token exchange failed: %s", exc, exc_info=True)
            return None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _api_call(url: str, body: Optional[dict] = None) -> Optional[dict]:
    """Make an authenticated request to a Google API endpoint."""
    token = _get_access_token()
    if not token:
        return None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    method = "POST" if body else "GET"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("GA4 API %s %s returned %d: %s", method, url, exc.code, err)
        return None
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        logger.error("GA4 API request failed: %s", exc, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


def _parse_report(resp: dict) -> List[Dict[str, str]]:
    """Parse a GA4 runReport response into a list of row dicts."""
    rows: List[Dict[str, str]] = []
    d_hdrs = [h.get("name") or "" for h in (resp.get("dimensionHeaders") or [])]
    m_hdrs = [h.get("name") or "" for h in (resp.get("metricHeaders") or [])]
    for row in resp.get("rows") or []:
        entry: Dict[str, str] = {}
        for i, dv in enumerate(row.get("dimensionValues") or []):
            if i < len(d_hdrs):
                entry[d_hdrs[i]] = dv.get("value") or ""
        for i, mv in enumerate(row.get("metricValues") or []):
            if i < len(m_hdrs):
                entry[m_hdrs[i]] = mv.get("value") or ""
        rows.append(entry)
    return rows


def _prop(property_id: str) -> str:
    """Resolve property ID from argument or GA4_PROPERTY_ID env var."""
    return (property_id or os.environ.get("GA4_PROPERTY_ID") or "").strip()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_ga4_report(
    property_id: str,
    dimensions: List[str],
    metrics: List[str],
    date_range: Tuple[str, str] = ("30daysAgo", "today"),
    limit: int = 100,
) -> Dict[str, Any]:
    """Run a GA4 report with arbitrary dimensions and metrics.

    Args:
        property_id: GA4 property ID (numeric). Falls back to GA4_PROPERTY_ID env.
        dimensions: Dimension names (e.g. ["country", "pagePath"]).
        metrics: Metric names (e.g. ["activeUsers", "sessions"]).
        date_range: (start_date, end_date) in GA4 format.
        limit: Max rows to return.

    Returns:
        Dict with 'rows', 'row_count', and metadata.
    """
    p = _prop(property_id)
    if not p:
        return {"error": "No GA4 property ID configured", "rows": [], "row_count": 0}
    ck = _cache_key("report", p, dimensions, metrics, date_range, limit)
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    body: Dict[str, Any] = {
        "dateRanges": [{"startDate": date_range[0], "endDate": date_range[1]}],
        "metrics": [{"name": m} for m in metrics],
        "limit": str(limit),
    }
    if dimensions:
        body["dimensions"] = [{"name": d} for d in dimensions]
    resp = _api_call(f"{_GA4_BASE}/properties/{p}:runReport", body)
    if not resp:
        return {"error": "GA4 API call failed", "rows": [], "row_count": 0}
    rows = _parse_report(resp)
    result = {
        "property_id": p,
        "date_range": {"start": date_range[0], "end": date_range[1]},
        "dimensions": dimensions,
        "metrics": metrics,
        "rows": rows,
        "row_count": len(rows),
        "total_rows": resp.get("rowCount") or len(rows),
    }
    _cache_set(ck, result)
    return result


def get_traffic_overview(property_id: str = "", days: int = 30) -> Dict[str, Any]:
    """Get traffic summary: totals, top pages, top countries, top sources.

    Args:
        property_id: GA4 property ID. Falls back to GA4_PROPERTY_ID env.
        days: Lookback window in days.
    """
    p = _prop(property_id)
    if not p:
        return {"error": "No GA4 property ID configured"}
    ck = _cache_key("overview", p, days)
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    start = f"{days}daysAgo"
    totals_resp = _api_call(
        f"{_GA4_BASE}/properties/{p}:runReport",
        {
            "dateRanges": [{"startDate": start, "endDate": "today"}],
            "metrics": [
                {"name": "activeUsers"},
                {"name": "sessions"},
                {"name": "screenPageViews"},
                {"name": "bounceRate"},
                {"name": "averageSessionDuration"},
                {"name": "newUsers"},
                {"name": "engagementRate"},
            ],
        },
    )
    totals: Dict[str, str] = {}
    if totals_resp:
        parsed = _parse_report(totals_resp)
        if parsed:
            totals = parsed[0]
    pages = get_top_pages(p, days=days, limit=10)
    geo = get_geo_breakdown(p, days=days, limit=10)
    sources = get_traffic_sources(p, days=days, limit=10)
    result = {
        "property_id": p,
        "period_days": days,
        "totals": {
            "active_users": totals.get("activeUsers") or "0",
            "sessions": totals.get("sessions") or "0",
            "pageviews": totals.get("screenPageViews") or "0",
            "bounce_rate": totals.get("bounceRate") or "0",
            "avg_session_duration_sec": totals.get("averageSessionDuration") or "0",
            "new_users": totals.get("newUsers") or "0",
            "engagement_rate": totals.get("engagementRate") or "0",
        },
        "top_pages": pages.get("rows") or [],
        "top_countries": geo.get("rows") or [],
        "top_sources": sources.get("rows") or [],
    }
    _cache_set(ck, result)
    return result


def get_top_pages(
    property_id: str = "", days: int = 30, limit: int = 20
) -> Dict[str, Any]:
    """Get top pages by pageviews."""
    return get_ga4_report(
        property_id=property_id,
        dimensions=["pagePath", "pageTitle"],
        metrics=["screenPageViews", "activeUsers", "averageSessionDuration"],
        date_range=(f"{days}daysAgo", "today"),
        limit=limit,
    )


def get_traffic_sources(
    property_id: str = "", days: int = 30, limit: int = 20
) -> Dict[str, Any]:
    """Get traffic sources breakdown (source/medium)."""
    return get_ga4_report(
        property_id=property_id,
        dimensions=["sessionSource", "sessionMedium"],
        metrics=["sessions", "activeUsers", "bounceRate", "engagementRate"],
        date_range=(f"{days}daysAgo", "today"),
        limit=limit,
    )


def get_geo_breakdown(
    property_id: str = "", days: int = 30, limit: int = 20
) -> Dict[str, Any]:
    """Get geographic breakdown by country and city."""
    return get_ga4_report(
        property_id=property_id,
        dimensions=["country", "city"],
        metrics=["activeUsers", "sessions", "screenPageViews"],
        date_range=(f"{days}daysAgo", "today"),
        limit=limit,
    )


def get_realtime_users(property_id: str = "") -> Dict[str, Any]:
    """Get current active users via the GA4 Realtime API."""
    p = _prop(property_id)
    if not p:
        return {"error": "No GA4 property ID configured", "active_users": 0}
    ck = _cache_key("realtime", p)
    cached = _cache_get(ck)
    if cached is not None:
        return cached
    resp = _api_call(
        f"{_GA4_BASE}/properties/{p}:runRealtimeReport",
        {
            "metrics": [{"name": "activeUsers"}],
            "dimensions": [{"name": "country"}],
        },
    )
    if not resp:
        return {
            "property_id": p,
            "active_users": 0,
            "error": "Realtime API call failed",
            "by_country": [],
        }
    rows = _parse_report(resp)
    total = sum(int(r.get("activeUsers") or "0") for r in rows)
    result = {"property_id": p, "active_users": total, "by_country": rows[:10]}
    _cache_set(ck, result)
    return result


# ---------------------------------------------------------------------------
# Chatbot tool handler
# ---------------------------------------------------------------------------


def handle_ga4_query(body: Dict[str, Any]) -> Dict[str, Any]:
    """POST handler for chatbot GA4 queries. Routes natural-language queries
    to the appropriate GA4 function based on keyword matching.

    Args:
        body: Dict with 'query' string and optional 'property_id', 'days', 'limit'.
    """
    query = (body.get("query") or "").lower().strip()
    prop = body.get("property_id") or ""
    days = int(body.get("days") or 30)
    limit = int(body.get("limit") or 20)
    try:
        if any(
            k in query
            for k in ["realtime", "real-time", "right now", "live", "current"]
        ):
            return get_realtime_users(prop)
        if any(k in query for k in ["overview", "summary", "dashboard", "traffic"]):
            return get_traffic_overview(prop, days=days)
        if any(
            k in query for k in ["top page", "popular page", "page view", "pageview"]
        ):
            return get_top_pages(prop, days=days, limit=limit)
        if any(
            k in query for k in ["source", "medium", "referr", "channel", "acquisition"]
        ):
            return get_traffic_sources(prop, days=days, limit=limit)
        if any(k in query for k in ["country", "city", "geo", "location", "region"]):
            return get_geo_breakdown(prop, days=days, limit=limit)
        return get_traffic_overview(prop, days=days)
    except Exception as exc:
        logger.error("GA4 chatbot query failed: %s", exc, exc_info=True)
        return {"error": f"GA4 query failed: {exc}", "query": query}


# ---------------------------------------------------------------------------
# Status check
# ---------------------------------------------------------------------------


def get_status() -> Dict[str, Any]:
    """Health check for the GA4 Data API module."""
    creds = _load_credentials()
    ga4_prop = _prop("")
    return {
        "module": "google_analytics_data",
        "configured": creds is not None,
        "service_account": (creds.get("client_email") or "unknown") if creds else None,
        "ga4_property_configured": bool(ga4_prop),
        "ga4_property_id": ga4_prop or None,
        "features": {
            "ga4_reports": creds is not None,
            "traffic_overview": creds is not None,
            "top_pages": creds is not None,
            "traffic_sources": creds is not None,
            "geo_breakdown": creds is not None,
            "realtime_users": creds is not None,
            "chatbot_handler": True,
        },
        "cache_ttl_seconds": _CACHE_TTL,
    }
