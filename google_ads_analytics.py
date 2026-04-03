"""Google Ads + Analytics benchmark integration for media plans.

Falls back to curated industry benchmarks when API credentials are unavailable.
Env: GOOGLE_SLIDES_CREDENTIALS_B64 (shared), GA4_PROPERTY_ID.
"""

from __future__ import annotations
import base64, json, logging, os, ssl, threading, time
import urllib.error, urllib.parse, urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
_ANALYTICS_BASE = "https://analyticsdata.googleapis.com/v1beta"
_TOKEN_URI = "https://oauth2.googleapis.com/token"
_SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]
_token_lock = threading.Lock()
_token_cache: Dict[str, Any] = {"token": None, "expires_at": 0.0}

# ---------------------------------------------------------------------------
# Curated fallback benchmarks (recruitment, 2024-2025)
# Format: (cpc_l, cpc_a, cpc_h, cpa_l, cpa_a, cpa_h,
#           ctr_l, ctr_a, ctr_h, cvr_l, cvr_a, cvr_h, source)
# ---------------------------------------------------------------------------
_BM_KEYS = ("cpc", "cpa", "ctr", "conversion_rate")


def _bm(t: tuple) -> Dict[str, Any]:
    """Unpack a compact benchmark tuple into a structured dict."""
    return {
        "cpc": {"low": t[0], "avg": t[1], "high": t[2]},
        "cpa": {"low": t[3], "avg": t[4], "high": t[5]},
        "ctr": {"low": t[6], "avg": t[7], "high": t[8]},
        "conversion_rate": {"low": t[9], "avg": t[10], "high": t[11]},
        "source": t[12],
    }


_SRC = "Appcast/Recruitics 2024"
_RB_RAW = {
    "healthcare": (0.85, 1.45, 2.80, 18, 32, 55, 2.8, 4.2, 6.5, 3.5, 5.8, 9.2, _SRC),
    "technology": (1.20, 2.15, 4.50, 25, 45, 85, 2.2, 3.5, 5.8, 2.8, 4.5, 7.5, _SRC),
    "retail": (0.55, 0.95, 1.80, 12, 22, 40, 3.5, 5.2, 8.0, 4.0, 6.5, 10.5, _SRC),
    "finance": (1.50, 2.65, 5.20, 28, 50, 95, 2.0, 3.2, 5.0, 2.5, 4.2, 6.8, _SRC),
    "manufacturing": (0.70, 1.20, 2.30, 15, 28, 48, 3.0, 4.8, 7.2, 3.8, 5.5, 8.8, _SRC),
    "logistics": (0.60, 1.10, 2.10, 14, 25, 42, 3.2, 5.0, 7.8, 4.2, 6.0, 9.5, _SRC),
    "hospitality": (0.45, 0.80, 1.50, 10, 18, 32, 4.0, 5.8, 8.5, 4.5, 7.0, 11.0, _SRC),
    "education": (0.90, 1.55, 3.00, 20, 35, 60, 2.5, 3.8, 6.0, 3.2, 5.0, 8.0, _SRC),
    "default": (
        0.80,
        1.50,
        3.20,
        18,
        35,
        65,
        2.5,
        4.0,
        6.5,
        3.0,
        5.0,
        8.5,
        "Industry average 2024",
    ),
}

_JOB_MULT: Dict[str, float] = {
    "engineering": 1.40,
    "software": 1.45,
    "data_science": 1.50,
    "executive": 1.60,
    "management": 1.25,
    "sales": 0.90,
    "customer_service": 0.75,
    "entry_level": 0.65,
    "skilled_trades": 0.85,
    "nursing": 1.10,
    "physician": 1.55,
    "driver": 0.70,
    "warehouse": 0.60,
    "administrative": 0.80,
    "default": 1.00,
}

# YouTube: (cpv_l, cpv_a, cpv_h, vr_l, vr_a, vr_h,
#            ctr_l, ctr_a, ctr_h, wp_l, wp_a, wp_h, cpa_l, cpa_a, cpa_h, src)
_YT_SRC = "Rally Recruitment Marketing 2024"
_YT_RAW = {
    "healthcare": (
        0.03,
        0.06,
        0.12,
        18,
        28,
        42,
        0.4,
        0.8,
        1.5,
        25,
        40,
        60,
        35,
        65,
        120,
        _YT_SRC,
    ),
    "technology": (
        0.04,
        0.08,
        0.15,
        15,
        25,
        38,
        0.3,
        0.7,
        1.2,
        22,
        35,
        55,
        50,
        90,
        160,
        _YT_SRC,
    ),
    "default": (
        0.03,
        0.07,
        0.14,
        16,
        26,
        40,
        0.35,
        0.75,
        1.3,
        23,
        38,
        58,
        40,
        75,
        140,
        "Industry average 2024",
    ),
}


def _yt_bm(t: tuple) -> Dict[str, Any]:
    return {
        "cpv": {"low": t[0], "avg": t[1], "high": t[2]},
        "view_rate": {"low": t[3], "avg": t[4], "high": t[5]},
        "ctr": {"low": t[6], "avg": t[7], "high": t[8]},
        "avg_watch_pct": {"low": t[9], "avg": t[10], "high": t[11]},
        "cpa": {"low": t[12], "avg": t[13], "high": t[14]},
        "source": t[15],
    }


def _expand_aliases(mapping: Dict[str, List[str]]) -> Dict[str, str]:
    return {alias: canon for canon, aliases in mapping.items() for alias in aliases}


_IND_ALIASES = _expand_aliases(
    {
        "technology": ["tech", "it", "software"],
        "healthcare": ["health", "medical", "pharma"],
        "finance": ["banking", "financial", "insurance"],
        "logistics": ["transport", "transportation", "supply_chain", "trucking"],
        "hospitality": ["hotel", "restaurant", "food"],
        "manufacturing": ["warehouse", "industrial"],
        "education": ["school", "university", "academic"],
        "retail": ["ecommerce", "e_commerce", "store"],
    }
)
_JOB_ALIASES = _expand_aliases(
    {
        "software": ["developer", "programmer", "swe"],
        "engineering": ["devops", "qa"],
        "data_science": ["ml", "ai", "analyst"],
        "executive": ["c_suite", "director", "vp"],
        "management": ["manager", "supervisor", "lead"],
        "customer_service": ["support", "call_center"],
        "entry_level": ["intern", "junior", "associate"],
        "nursing": ["rn", "lpn", "nurse"],
        "physician": ["doctor", "md", "surgeon"],
        "driver": ["cdl", "trucker", "delivery"],
        "warehouse": ["picker", "packer", "fulfillment"],
        "administrative": ["admin", "clerk"],
        "skilled_trades": ["mechanic", "electrician", "plumber", "welder"],
    }
)


def _norm(val: str, aliases: dict) -> str:
    key = (val or "").strip().lower().replace(" ", "_").replace("-", "_")
    return aliases.get(key, key)


# ---------------------------------------------------------------------------
# Credential + token helpers (mirrors sheets_export.py)
# ---------------------------------------------------------------------------
def _load_credentials() -> Optional[Dict[str, str]]:
    """Load service-account JSON from GOOGLE_SLIDES_CREDENTIALS_B64."""
    b64_creds = os.environ.get("GOOGLE_SLIDES_CREDENTIALS_B64") or ""
    if not b64_creds:
        return None
    try:
        creds = json.loads(base64.b64decode(b64_creds))
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

    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    header_b64 = _b64url(
        json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode()
    )
    claims_b64 = _b64url(
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
    signing_input = f"{header_b64}.{claims_b64}".encode("ascii")

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        key = serialization.load_pem_private_key(
            creds["private_key"].encode(), password=None
        )
        sig = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        return f"{header_b64}.{claims_b64}.{_b64url(sig)}"
    except ImportError:
        pass

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
            kf.write(creds["private_key"])
            key_path = kf.name
        try:
            proc = subprocess.run(
                ["openssl", "dgst", "-sha256", "-sign", key_path],
                input=signing_input,
                capture_output=True,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout:
                return f"{header_b64}.{claims_b64}.{_b64url(proc.stdout)}"
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
            jwt_token = _build_jwt(creds)
        except RuntimeError as exc:
            logger.error("JWT signing failed: %s", exc, exc_info=True)
            return None
        payload = urllib.parse.urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:2.0-jwt-bearer",
                "assertion": jwt_token,
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


def _api_request(url: str, body: Optional[dict] = None) -> Optional[dict]:
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
        logger.error("Google API %s %s returned %d: %s", method, url, exc.code, err)
        return None
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        logger.error("Google API request failed: %s", exc, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def get_google_ads_benchmarks(industry: str, job_category: str = "") -> Dict[str, Any]:
    """Get CPC/CPA/CTR benchmarks for a given industry and job category.

    Returns dict with cpc, cpa, ctr, conversion_rate (each low/avg/high).
    Job category applies a multiplier to cost metrics (CPC/CPA).
    """
    ind_key = _norm(industry, _IND_ALIASES)
    raw = _RB_RAW.get(ind_key, _RB_RAW["default"])
    bm = _bm(raw)

    cat_key = _norm(job_category, _JOB_ALIASES)
    mult = _JOB_MULT.get(cat_key, _JOB_MULT["default"])

    def _apply(m: Dict[str, float]) -> Dict[str, float]:
        return {k: round(v * mult, 2) for k, v in m.items()}

    return {
        "industry": ind_key if ind_key in _RB_RAW else "default",
        "job_category": cat_key if cat_key in _JOB_MULT else "default",
        "multiplier_applied": round(mult, 2),
        "cpc": _apply(bm["cpc"]),
        "cpa": _apply(bm["cpa"]),
        "ctr": bm["ctr"],
        "conversion_rate": bm["conversion_rate"],
        "source": bm["source"],
        "data_type": "curated_benchmarks",
    }


def get_analytics_data(
    property_id: str = "",
    metrics: Optional[List[str]] = None,
    date_range: str = "last_30_days",
) -> Dict[str, Any]:
    """Pull GA4 data for campaign performance. Falls back to GA4_PROPERTY_ID env var."""
    prop_id = (property_id or os.environ.get("GA4_PROPERTY_ID") or "").strip()
    if not prop_id:
        return {
            "configured": False,
            "message": "GA4_PROPERTY_ID not set. Configure to pull live analytics.",
            "data": [],
        }

    if metrics is None:
        metrics = [
            "sessions",
            "totalUsers",
            "conversions",
            "engagementRate",
            "bounceRate",
        ]

    range_map = {
        "last_7_days": {"startDate": "7daysAgo", "endDate": "today"},
        "last_30_days": {"startDate": "30daysAgo", "endDate": "today"},
        "last_90_days": {"startDate": "90daysAgo", "endDate": "today"},
    }
    if date_range in range_map:
        dr = range_map[date_range]
    elif ":" in date_range:
        parts = date_range.split(":")
        dr = {"startDate": parts[0], "endDate": parts[1] if len(parts) > 1 else "today"}
    else:
        dr = {"startDate": "30daysAgo", "endDate": "today"}

    resp = _api_request(
        f"{_ANALYTICS_BASE}/properties/{prop_id}:runReport",
        {
            "dateRanges": [dr],
            "metrics": [{"name": m} for m in metrics],
            "dimensions": [{"name": "date"}],
        },
    )
    if not resp:
        return {
            "configured": True,
            "message": "GA4 API call failed. Check service account permissions.",
            "data": [],
        }

    rows: List[Dict[str, str]] = []
    m_hdrs = [h.get("name") or "" for h in (resp.get("metricHeaders") or [])]
    d_hdrs = [h.get("name") or "" for h in (resp.get("dimensionHeaders") or [])]
    for row in resp.get("rows") or []:
        entry = {
            d_hdrs[i]: (dv.get("value") or "")
            for i, dv in enumerate(row.get("dimensionValues") or [])
            if i < len(d_hdrs)
        }
        entry.update(
            {
                m_hdrs[i]: (mv.get("value") or "")
                for i, mv in enumerate(row.get("metricValues") or [])
                if i < len(m_hdrs)
            }
        )
        rows.append(entry)

    return {
        "configured": True,
        "property_id": prop_id,
        "date_range": date_range,
        "metrics_requested": metrics,
        "row_count": len(rows),
        "data": rows,
    }


def get_youtube_recruitment_benchmarks(industry: str = "") -> Dict[str, Any]:
    """Get YouTube ad benchmarks for recruitment video advertising."""
    ind_key = _norm(industry, _IND_ALIASES)
    raw = _YT_RAW.get(ind_key, _YT_RAW["default"])
    bm = _yt_bm(raw)
    return {
        "industry": ind_key if ind_key in _YT_RAW else "default",
        "platform": "youtube",
        "ad_formats": ["TrueView In-Stream", "Bumper Ads", "Discovery Ads"],
        **{k: bm[k] for k in ("cpv", "view_rate", "ctr", "avg_watch_pct", "cpa")},
        "source": bm["source"],
        "data_type": "curated_benchmarks",
        "recommendations": {
            "optimal_video_length": "30-60 seconds for recruitment",
            "best_targeting": "Custom intent + job title keywords",
            "creative_tips": "Show real employees, workplace culture, day-in-life",
        },
    }


def get_status() -> Dict[str, Any]:
    """Health check for Google Ads/Analytics benchmark module."""
    creds = _load_credentials()
    ga4_prop = (os.environ.get("GA4_PROPERTY_ID") or "").strip()
    return {
        "module": "google_ads_analytics",
        "configured": creds is not None,
        "service_account": (creds.get("client_email") or "unknown") if creds else None,
        "ga4_property_configured": bool(ga4_prop),
        "features": {
            "google_ads_benchmarks": True,
            "analytics_data": creds is not None and bool(ga4_prop),
            "youtube_benchmarks": True,
        },
        "benchmark_coverage": {
            "industries": len(_RB_RAW) - 1,
            "job_categories": len(_JOB_MULT) - 1,
            "youtube_industries": len(_YT_RAW) - 1,
        },
    }
