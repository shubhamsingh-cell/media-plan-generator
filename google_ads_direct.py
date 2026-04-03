"""Google Ads Keyword Planner + Search Ads 360 live CPC/CPA integration.

Falls back to curated recruitment benchmarks when API credentials are unavailable.
Env: GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_DEVELOPER_TOKEN, SA360_CUSTOMER_ID,
     GOOGLE_SLIDES_CREDENTIALS_B64 (service account for auth).
"""

from __future__ import annotations
import base64, json, logging, os, ssl, threading, time
import urllib.error, urllib.parse, urllib.request
from typing import Any, Dict, Optional
from google_ads_analytics import _load_credentials

logger = logging.getLogger(__name__)
_ADS = "https://googleads.googleapis.com/v17"
_SA360 = "https://searchads360.googleapis.com/v0"
_TOKEN_URI = "https://oauth2.googleapis.com/token"
_S_ADS = ["https://www.googleapis.com/auth/adwords"]
_S_SA = ["https://www.googleapis.com/auth/doubleclicksearch"]
_lock = threading.Lock()
_tcache: Dict[str, Any] = {}

# ---- Benchmarks (recruitment CPC by job family, location, industry) ------
_CPC_BM: Dict[str, tuple[float, float, float]] = {
    "registered nurse": (0.95, 1.65, 3.20),
    "software engineer": (1.40, 2.50, 5.00),
    "truck driver": (0.55, 1.00, 1.90),
    "warehouse associate": (0.40, 0.75, 1.40),
    "retail associate": (0.45, 0.85, 1.60),
    "customer service": (0.50, 0.90, 1.70),
    "sales representative": (0.70, 1.25, 2.40),
    "data analyst": (1.10, 2.00, 3.80),
    "project manager": (1.00, 1.80, 3.50),
    "accountant": (0.90, 1.55, 3.00),
    "teacher": (0.65, 1.15, 2.20),
    "electrician": (0.70, 1.20, 2.30),
    "marketing manager": (1.05, 1.85, 3.60),
    "medical assistant": (0.75, 1.30, 2.50),
    "default": (0.80, 1.50, 3.20),
}
_LOC_M: Dict[str, float] = {
    "new york": 1.45,
    "san francisco": 1.55,
    "los angeles": 1.35,
    "chicago": 1.20,
    "boston": 1.30,
    "seattle": 1.40,
    "austin": 1.15,
    "denver": 1.15,
    "miami": 1.10,
    "dallas": 1.10,
    "atlanta": 1.05,
    "phoenix": 1.00,
    "houston": 1.05,
    "philadelphia": 1.15,
    "remote": 1.25,
    "rural": 0.70,
    "suburban": 0.85,
}
_IND_CPC: Dict[str, tuple[float, float, float]] = {
    "healthcare": (0.85, 1.45, 2.80),
    "technology": (1.20, 2.15, 4.50),
    "retail": (0.55, 0.95, 1.80),
    "finance": (1.50, 2.65, 5.20),
    "manufacturing": (0.70, 1.20, 2.30),
    "logistics": (0.60, 1.10, 2.10),
    "hospitality": (0.45, 0.80, 1.50),
    "education": (0.90, 1.55, 3.00),
    "default": (0.80, 1.50, 3.20),
}
_COMP = {0: "UNSPECIFIED", 2: "LOW", 3: "MEDIUM", 4: "HIGH"}
_SEASON = {
    1: 1.15,
    2: 1.10,
    3: 1.05,
    4: 1.00,
    5: 0.95,
    6: 0.90,
    7: 0.85,
    8: 0.90,
    9: 1.10,
    10: 1.05,
    11: 0.95,
    12: 0.80,
}


# ---- Auth helpers --------------------------------------------------------
def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _sign_jwt(creds: Dict[str, str], scopes: list[str]) -> str:
    """Build a signed RS256 JWT for the given scopes."""
    import subprocess, tempfile

    now = int(time.time())
    hdr = _b64url(
        json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")).encode()
    )
    pay = _b64url(
        json.dumps(
            {
                "iss": creds["client_email"],
                "scope": " ".join(scopes),
                "aud": creds.get("token_uri") or _TOKEN_URI,
                "iat": now,
                "exp": now + 3600,
            },
            separators=(",", ":"),
        ).encode()
    )
    msg = f"{hdr}.{pay}".encode("ascii")
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        key = serialization.load_pem_private_key(
            creds["private_key"].encode(), password=None
        )
        return (
            f"{hdr}.{pay}.{_b64url(key.sign(msg, padding.PKCS1v15(), hashes.SHA256()))}"
        )
    except ImportError:
        pass
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as kf:
        kf.write(creds["private_key"])
        kp = kf.name
    try:
        p = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", kp],
            input=msg,
            capture_output=True,
            timeout=10,
        )
        if p.returncode == 0 and p.stdout:
            return f"{hdr}.{pay}.{_b64url(p.stdout)}"
    finally:
        try:
            os.unlink(kp)
        except OSError:
            pass
    raise RuntimeError(
        "Cannot sign JWT -- install cryptography or ensure openssl on PATH"
    )


def _get_token(scopes: list[str]) -> Optional[str]:
    """Cached, thread-safe OAuth2 token for given scopes."""
    key = scopes[0]
    with _lock:
        e = _tcache.get(key) or {}
        if e.get("t") and e.get("x", 0) > time.time() + 300:
            return e["t"]
        creds = _load_credentials()
        if not creds:
            return None
        try:
            jwt = _sign_jwt(creds, scopes)
        except RuntimeError as exc:
            logger.error("JWT sign failed: %s", exc, exc_info=True)
            return None
        body = urllib.parse.urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": jwt,
            }
        ).encode()
        req = urllib.request.Request(
            creds.get("token_uri") or _TOKEN_URI,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(
                req, context=ssl.create_default_context(), timeout=15
            ) as r:
                td = json.loads(r.read())
            _tcache[key] = {
                "t": td["access_token"],
                "x": time.time() + td.get("expires_in", 3600),
            }
            return td["access_token"]
        except (urllib.error.URLError, json.JSONDecodeError, KeyError) as exc:
            logger.error("Token exchange failed: %s", exc, exc_info=True)
            return None


def _ads_req(url: str, body: dict) -> Optional[dict]:
    """POST to Google Ads REST API with auth + developer token."""
    token = _get_token(_S_ADS)
    dev = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or ""
    if not token or not dev:
        return None
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "developer-token": dev,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            req, context=ssl.create_default_context(), timeout=20
        ) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode()
        except Exception:
            pass
        logger.error("Ads API %s %d: %s", url, exc.code, err)
        return None
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        logger.error("Ads API failed: %s", exc, exc_info=True)
        return None


def _n(v: str) -> str:
    return (v or "").strip().lower().replace("-", " ").replace("_", " ")


def _lm(loc: str) -> float:
    s = _n(loc)
    for k, m in _LOC_M.items():
        if k in s:
            return m
    return 1.0


# ---- 1. get_keyword_ideas ------------------------------------------------
def get_keyword_ideas(
    keywords: list[str],
    location_ids: list[str] | None = None,
    language_id: str = "1000",
) -> list[dict]:
    """Get keyword ideas with CPC estimates from Google Ads Keyword Planner."""
    cid = (os.environ.get("GOOGLE_ADS_CUSTOMER_ID") or "").replace("-", "")
    if not cid:
        logger.warning("GOOGLE_ADS_CUSTOMER_ID not set")
        return []
    geo = [f"geoTargetConstants/{g}" for g in (location_ids or ["2840"])]
    resp = _ads_req(
        f"{_ADS}/customers/{cid}:generateKeywordIdeas",
        {
            "keywordSeed": {"keywords": keywords},
            "language": f"languageConstants/{language_id}",
            "geoTargetConstants": geo,
            "keywordPlanNetwork": "GOOGLE_SEARCH",
        },
    )
    if not resp:
        return []
    out: list[dict] = []
    for r in resp.get("results") or []:
        m = r.get("keywordIdeaMetrics") or {}
        lo = m.get("lowTopOfPageBidMicros") or 0
        hi = m.get("highTopOfPageBidMicros") or 0
        out.append(
            {
                "keyword": r.get("text") or "",
                "avg_monthly_searches": m.get("avgMonthlySearches") or 0,
                "competition": _COMP.get(m.get("competition") or 0, "UNSPECIFIED"),
                "low_bid_micros": lo,
                "high_bid_micros": hi,
                "cpc_estimate": round((lo + hi) / 2_000_000, 2),
            }
        )
    return out


# ---- 2. get_recruitment_cpc ----------------------------------------------
def get_recruitment_cpc(job_title: str, location: str = "") -> dict:
    """Get live CPC for a recruitment keyword, falling back to benchmarks."""
    kw = f"{job_title} jobs" if "job" not in _n(job_title) else job_title
    ideas = get_keyword_ideas([kw])
    if ideas:
        b = ideas[0]
        lm = _lm(location)
        return {
            "keyword": b["keyword"],
            "cpc_low": round(b["low_bid_micros"] / 1e6 * lm, 2),
            "cpc_avg": round(b["cpc_estimate"] * lm, 2),
            "cpc_high": round(b["high_bid_micros"] / 1e6 * lm, 2),
            "monthly_searches": b["avg_monthly_searches"],
            "competition_level": b["competition"],
            "source": "google_ads_keyword_planner",
            "location_multiplier": lm,
        }
    return get_estimated_cpc(job_title, "", location)


# ---- 3. get_seasonal_trends ----------------------------------------------
def get_seasonal_trends(keyword: str, months: int = 12) -> list[dict]:
    """Monthly search volume trends. Live API first, then curated seasonality."""
    cid = (os.environ.get("GOOGLE_ADS_CUSTOMER_ID") or "").replace("-", "")
    if cid:
        resp = _ads_req(
            f"{_ADS}/customers/{cid}:generateKeywordHistoricalMetrics",
            {"keywords": [keyword], "keywordPlanNetwork": "GOOGLE_SEARCH"},
        )
        if resp:
            trends: list[dict] = []
            for r in resp.get("results") or []:
                for mv in (r.get("keywordMetrics") or {}).get(
                    "monthlySearchVolumes"
                ) or []:
                    trends.append(
                        {
                            "year": mv.get("year") or 0,
                            "month": mv.get("month") or 0,
                            "monthly_searches": mv.get("monthlySearches") or 0,
                        }
                    )
            if trends:
                return trends[-months:]
    # Fallback: curated seasonality
    bm = _CPC_BM.get(_n(keyword), _CPC_BM["default"])
    now = time.localtime()
    cm, cy = now.tm_mon, now.tm_year
    out = []
    for i in range(months):
        off = months - 1 - i
        m = ((cm - 1 - off) % 12) + 1
        y = cy - ((off + (12 - cm)) // 12)
        s = _SEASON.get(m, 1.0)
        out.append(
            {
                "year": y,
                "month": m,
                "monthly_searches": int(1000 * s),
                "cpc_estimate": round(bm[1] * s, 2),
                "source": "curated_seasonality",
            }
        )
    return out


# ---- 4. get_search_engine_comparison -------------------------------------
def get_search_engine_comparison(keywords: list[str]) -> dict:
    """Compare CPC across Google/Bing via SA360. Falls back to curated data."""
    sa_id = (os.environ.get("SA360_CUSTOMER_ID") or "").strip()
    token = _get_token(_S_SA) if sa_id else None
    if not sa_id or not token:
        return _est_engine(keywords)
    kw_c = " OR ".join(f"metrics.keyword = '{k}'" for k in keywords[:20])
    body = {
        "query": f"SELECT metrics.average_cpc, metrics.ctr, segments.product_channel, "
        f"segments.keyword.text FROM keyword WHERE {kw_c} DURING LAST_30_DAYS"
    }
    req = urllib.request.Request(
        f"{_SA360}/customers/{sa_id}/searchAds360:search",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            req, context=ssl.create_default_context(), timeout=20
        ) as r:
            result = json.loads(r.read())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        logger.error("SA360 failed: %s", exc, exc_info=True)
        return _est_engine(keywords)
    gc, gt, bc, bt = [], [], [], []
    for row in result.get("results") or []:
        met = row.get("metrics") or {}
        ch = ((row.get("segments") or {}).get("productChannel") or "").lower()
        cpc = (met.get("averageCpc") or 0) / 1e6
        ctr = (met.get("ctr") or 0) * 100
        (bc if "bing" in ch or "microsoft" in ch else gc).append(cpc)
        (bt if "bing" in ch or "microsoft" in ch else gt).append(ctr)
    _a = lambda l: round(sum(l) / len(l), 2) if l else 0.0
    g, b, gctr, bctr = _a(gc), _a(bc), _a(gt), _a(bt)
    rec = (
        (
            "Bing"
            if b and g and b < g * 0.8
            else (
                "Google"
                if gctr > bctr * 1.2 and gctr
                else "Split budget 60/40 Google/Bing"
            )
        )
        if b and g
        else "Insufficient data"
    )
    return {
        "google": {"cpc": g, "ctr": gctr},
        "bing": {"cpc": b, "ctr": bctr},
        "recommendation": rec,
        "keywords_analyzed": len(keywords),
        "source": "search_ads_360",
    }


def _est_engine(keywords: list[str]) -> dict:
    """Curated cross-engine comparison fallback."""
    avg = round(
        sum(_CPC_BM.get(_n(k), _CPC_BM["default"])[1] for k in keywords)
        / max(len(keywords), 1),
        2,
    )
    return {
        "google": {"cpc": avg, "ctr": 3.8},
        "bing": {"cpc": round(avg * 0.75, 2), "ctr": 2.9},
        "recommendation": "Split budget 60/40 Google/Bing for recruitment",
        "keywords_analyzed": len(keywords),
        "source": "curated_benchmarks",
    }


# ---- 5. get_estimated_cpc ------------------------------------------------
def get_estimated_cpc(job_title: str, industry: str = "", location: str = "") -> dict:
    """Curated CPC estimates blending job-title, industry, and location data."""
    jt, ind, lm = _n(job_title), _n(industry), _lm(location)
    jb = _CPC_BM["default"]
    for k, v in _CPC_BM.items():
        if k != "default" and k in jt:
            jb = v
            break
    ib = _IND_CPC["default"]
    for k, v in _IND_CPC.items():
        if k != "default" and k in ind:
            ib = v
            break
    if ind:
        lo = round((jb[0] * 0.6 + ib[0] * 0.4) * lm, 2)
        av = round((jb[1] * 0.6 + ib[1] * 0.4) * lm, 2)
        hi = round((jb[2] * 0.6 + ib[2] * 0.4) * lm, 2)
    else:
        lo, av, hi = round(jb[0] * lm, 2), round(jb[1] * lm, 2), round(jb[2] * lm, 2)
    return {
        "keyword": f"{job_title} jobs",
        "cpc_low": lo,
        "cpc_avg": av,
        "cpc_high": hi,
        "monthly_searches": 0,
        "competition_level": "MEDIUM",
        "source": "curated_benchmarks",
        "location_multiplier": lm,
    }


# ---- 6. get_status -------------------------------------------------------
def get_status() -> dict:
    """Health check showing which APIs are configured."""
    creds = _load_credentials()
    a_id = bool((os.environ.get("GOOGLE_ADS_CUSTOMER_ID") or "").strip())
    d_tok = bool((os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip())
    s_id = bool((os.environ.get("SA360_CUSTOMER_ID") or "").strip())
    ads_ok = creds is not None and a_id and d_tok
    return {
        "module": "google_ads_direct",
        "google_ads_api": {
            "configured": ads_ok,
            "customer_id_set": a_id,
            "developer_token_set": d_tok,
            "service_account": bool(creds),
        },
        "search_ads_360": {
            "configured": creds is not None and s_id,
            "customer_id_set": s_id,
        },
        "fallback_benchmarks": {
            "job_titles": len(_CPC_BM) - 1,
            "industries": len(_IND_CPC) - 1,
            "locations": len(_LOC_M),
        },
        "features": {
            "keyword_ideas": ads_ok,
            "recruitment_cpc": True,
            "seasonal_trends": True,
            "engine_comparison": True,
            "estimated_cpc": True,
        },
    }
