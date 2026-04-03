"""Meta Marketing API integration for recruitment advertising benchmarks.

Provides audience sizing, cost estimates, interest targeting, and creative
recommendations for recruitment campaigns on Facebook/Instagram.  Auth:
META_ACCESS_TOKEN env var (long-lived token from Facebook Business Manager).
Optionally META_AD_ACCOUNT_ID for account-specific reach estimates.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_BASE_URL = "https://graph.facebook.com/v19.0"
_lock = threading.Lock()
_ssl_ctx = ssl.create_default_context()


# -- Curated recruitment benchmarks: {cpc, cpa, ctr, cpm} ranges ----------
def _bm(
    cpc: tuple, cpa: tuple, ctr: tuple, cpm: tuple, fb: int, ig: int, fmts: list
) -> dict:
    return {
        "cpc": {"low": cpc[0], "avg": cpc[1], "high": cpc[2]},
        "cpa": {"low": cpa[0], "avg": cpa[1], "high": cpa[2]},
        "ctr": {"low": ctr[0], "avg": ctr[1], "high": ctr[2]},
        "cpm": {"low": cpm[0], "avg": cpm[1], "high": cpm[2]},
        "platform_split": {"facebook_pct": fb, "instagram_pct": ig},
        "best_ad_formats": fmts,
    }


_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "healthcare": _bm(
        (1.20, 2.00, 2.80),
        (35, 60, 85),
        (0.8, 1.15, 1.5),
        (8, 14, 22),
        65,
        35,
        ["single_image", "carousel", "lead_form"],
    ),
    "technology": _bm(
        (1.50, 2.50, 3.50),
        (45, 82.5, 120),
        (0.6, 0.9, 1.2),
        (10, 18, 28),
        55,
        45,
        ["single_image", "video", "carousel"],
    ),
    "retail": _bm(
        (0.40, 0.80, 1.20),
        (8, 16.5, 25),
        (1.2, 1.85, 2.5),
        (4, 7, 12),
        60,
        40,
        ["carousel", "single_image", "stories"],
    ),
    "trucking": _bm(
        (0.80, 1.40, 2.00),
        (20, 37.5, 55),
        (1.0, 1.4, 1.8),
        (6, 10, 16),
        75,
        25,
        ["single_image", "video", "lead_form"],
    ),
    "transportation": _bm(
        (0.80, 1.40, 2.00),
        (20, 37.5, 55),
        (1.0, 1.4, 1.8),
        (6, 10, 16),
        75,
        25,
        ["single_image", "video", "lead_form"],
    ),
    "manufacturing": _bm(
        (0.60, 1.05, 1.50),
        (15, 30, 45),
        (1.0, 1.5, 2.0),
        (5, 9, 14),
        70,
        30,
        ["single_image", "video", "lead_form"],
    ),
    "hospitality": _bm(
        (0.35, 0.68, 1.00),
        (8, 15, 22),
        (1.5, 2.25, 3.0),
        (3.5, 6, 10),
        55,
        45,
        ["carousel", "stories", "video"],
    ),
    "finance": _bm(
        (1.80, 3.15, 4.50),
        (55, 102.5, 150),
        (0.5, 0.75, 1.0),
        (12, 20, 32),
        60,
        40,
        ["single_image", "lead_form", "video"],
    ),
}
_DEFAULT_BM = _bm(
    (0.80, 1.65, 2.50),
    (25, 50, 75),
    (0.8, 1.15, 1.5),
    (6, 11, 18),
    62,
    38,
    ["single_image", "carousel", "video"],
)

# -- Curated interest categories for targeting fallback --------------------
_CURATED_INTERESTS: Dict[str, List[Dict[str, Any]]] = {
    "nursing": [
        {
            "id": "6003349442530",
            "name": "Nursing",
            "audience_size": 48_000_000,
            "path": ["Interests", "Healthcare"],
            "description": "People interested in nursing careers",
        },
        {
            "id": "6003476182657",
            "name": "Registered nurse",
            "audience_size": 12_000_000,
            "path": ["Interests", "Healthcare", "Nursing"],
            "description": "Licensed registered nurses",
        },
    ],
    "software": [
        {
            "id": "6003139266461",
            "name": "Software engineering",
            "audience_size": 35_000_000,
            "path": ["Interests", "Technology"],
            "description": "People interested in software development",
        },
        {
            "id": "6003402953689",
            "name": "Computer programming",
            "audience_size": 28_000_000,
            "path": ["Interests", "Technology"],
            "description": "People interested in programming",
        },
    ],
    "truck driver": [
        {
            "id": "6003263791660",
            "name": "Truck driver",
            "audience_size": 8_500_000,
            "path": ["Interests", "Transportation"],
            "description": "People interested in truck driving careers",
        },
        {
            "id": "6003017068817",
            "name": "Commercial driver's license",
            "audience_size": 4_200_000,
            "path": ["Interests", "Transportation"],
            "description": "CDL holders and seekers",
        },
    ],
    "default": [
        {
            "id": "6003330604564",
            "name": "Job hunting",
            "audience_size": 95_000_000,
            "path": ["Interests", "Business"],
            "description": "People actively looking for jobs",
        },
        {
            "id": "6003476330915",
            "name": "Employment",
            "audience_size": 120_000_000,
            "path": ["Interests", "Business"],
            "description": "People interested in employment topics",
        },
    ],
}


# -- Auth helpers ----------------------------------------------------------
def _get_access_token() -> Optional[str]:
    """Return the Meta access token from environment, or None."""
    return os.environ.get("META_ACCESS_TOKEN") or None


def _get_ad_account_id() -> Optional[str]:
    """Return the Meta ad account ID from environment, or None."""
    raw = os.environ.get("META_AD_ACCOUNT_ID") or ""
    if not raw:
        return None
    return raw if raw.startswith("act_") else f"act_{raw}"


# -- Internal request helper -----------------------------------------------
def _meta_request(
    endpoint: str, params: Optional[Dict[str, str]] = None, method: str = "GET"
) -> Optional[dict]:
    """Make an authenticated request to the Meta Graph API."""
    token = _get_access_token()
    if not token:
        logger.debug("Meta API: no access token configured")
        return None
    params = dict(params or {})
    params["access_token"] = token
    url = f"{_BASE_URL}{endpoint}"
    if method == "GET":
        url = f"{url}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url)
    else:
        data = urllib.parse.urlencode(params).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method)
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if "error" in body:
            err = body["error"]
            logger.error(
                "Meta API error %s: %s",
                err.get("code") or "unknown",
                err.get("message") or "unknown",
            )
            return None
        return body
    except urllib.error.HTTPError as exc:
        err_body = ""
        try:
            err_body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("Meta API HTTP %d: %s", exc.code, err_body, exc_info=True)
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Meta API request failed: %s", exc, exc_info=True)
        return None


# -- Public API ------------------------------------------------------------


def get_audience_estimate(
    targeting: dict, optimization_goal: str = "LINK_CLICKS"
) -> dict:
    """Estimate audience size for a recruitment targeting spec.

    Uses /act_{id}/reachestimate when META_AD_ACCOUNT_ID is set;
    otherwise returns curated estimates based on targeting structure.
    """
    ad_account = _get_ad_account_id()
    if ad_account and _get_access_token():
        data = _meta_request(
            f"/{ad_account}/reachestimate",
            {
                "targeting_spec": json.dumps(targeting),
                "optimization_goal": optimization_goal,
            },
        )
        if data and "data" in data:
            est = data["data"]
            return {
                "audience_size_lower": est.get("users_lower_bound") or 0,
                "audience_size_upper": est.get("users_upper_bound") or 0,
                "daily_outcomes_estimate": est.get("estimate_dau") or 0,
                "source": "meta_api",
            }
    # Curated fallback
    geo_count = len((targeting.get("geo_locations") or {}).get("countries") or ["US"])
    interest_count = max(len(targeting.get("interests") or []), 1)
    base = 500_000 * geo_count * interest_count
    return {
        "audience_size_lower": int(base * 0.7),
        "audience_size_upper": int(base * 1.3),
        "daily_outcomes_estimate": int(base * 0.002),
        "source": "curated_estimate",
    }


def get_recruitment_benchmarks(
    industry: str, job_category: str, location: str = "US"
) -> dict:
    """Return CPC/CPA/CTR/CPM benchmarks for recruitment ads on Meta.

    Falls back to curated industry data. Returns dict with cpc, cpa, ctr,
    cpm ranges, platform_split, and best_ad_formats.
    """
    key = (industry or "").lower().strip()
    result = dict(_BENCHMARKS.get(key, _DEFAULT_BM))
    result["industry"] = key or "default"
    result["job_category"] = job_category or ""
    result["location"] = location or "US"
    result["source"] = "curated"
    return result


def get_interest_targeting(job_title: str, industry: str = "") -> List[Dict[str, Any]]:
    """Search for targeting interests related to a job title or industry.

    Uses /search?type=adinterest when META_ACCESS_TOKEN is set;
    otherwise returns curated interest categories.
    """
    query = (job_title or "").strip()
    if not query:
        return []
    if _get_access_token():
        data = _meta_request(
            "/search", {"type": "adinterest", "q": query, "limit": "10"}
        )
        if data and "data" in data:
            return [
                {
                    "id": item.get("id") or "",
                    "name": item.get("name") or "",
                    "audience_size": item.get("audience_size") or 0,
                    "path": item.get("path") or [],
                    "description": item.get("description") or "",
                }
                for item in (data.get("data") or [])
            ]
    # Curated fallback: keyword match
    q = query.lower()
    for keyword, interests in _CURATED_INTERESTS.items():
        if keyword in q or q in keyword:
            return interests
    return _CURATED_INTERESTS["default"]


def get_ad_creative_recommendations(industry: str, job_category: str) -> dict:
    """Return best practices for recruitment ad creatives on Meta.

    Returns dict with recommended_formats, image_specs, video_specs,
    copy_tips, cta_options, placement_recommendations.
    """
    key = (industry or "").lower().strip()
    blue = key in (
        "trucking",
        "transportation",
        "manufacturing",
        "retail",
        "hospitality",
    )
    third = (
        {
            "placement": "facebook_marketplace",
            "priority": 3,
            "note": "Strong for blue-collar",
        }
        if blue
        else {"placement": "instagram_stories", "priority": 3}
    )
    return {
        "recommended_formats": [
            {
                "format": "single_image",
                "priority": 1,
                "note": "Highest CTR for recruitment",
            },
            {
                "format": "video",
                "priority": 2,
                "note": "15-30s employee testimonials perform best",
            },
            {
                "format": "carousel",
                "priority": 3,
                "note": "Show workplace, team, benefits",
            },
            {
                "format": "lead_form",
                "priority": 4,
                "note": "Instant Apply reduces drop-off",
            },
        ],
        "image_specs": {
            "feed": {"width": 1200, "height": 628, "ratio": "1.91:1"},
            "square": {"width": 1080, "height": 1080, "ratio": "1:1"},
            "stories": {"width": 1080, "height": 1920, "ratio": "9:16"},
            "max_text_pct": 20,
        },
        "video_specs": {
            "min_length_s": 6,
            "max_length_s": 60,
            "recommended_length_s": 15 if blue else 30,
            "ratio": "1:1",
            "captions_required": True,
        },
        "copy_tips": [
            "Lead with salary/pay range in the first line",
            "Mention specific benefits (healthcare, 401k, PTO)",
            "Include location or 'remote' prominently",
            "Use action verbs: Apply, Join, Start, Earn",
            "Keep primary text under 125 characters for mobile",
            "Add urgency: 'Hiring now', 'Immediate openings'",
        ],
        "cta_options": [
            {"cta": "APPLY_NOW", "recommended": True},
            {"cta": "LEARN_MORE", "recommended": True},
            {"cta": "SIGN_UP", "recommended": False},
            {"cta": "CONTACT_US", "recommended": False},
        ],
        "placement_recommendations": [
            {"placement": "facebook_feed", "priority": 1},
            {"placement": "instagram_feed", "priority": 2},
            third,
            {"placement": "audience_network", "priority": 4},
        ],
        "industry": key or "default",
        "job_category": job_category or "",
    }


def estimate_campaign_cost(
    budget: float,
    industry: str,
    job_category: str,
    location: str = "US",
    duration_days: int = 30,
) -> dict:
    """Estimate campaign outcomes for a given budget using benchmarks.

    Returns dict with estimated_clicks, estimated_applies, estimated_cpc,
    estimated_cpa, estimated_reach, estimated_impressions, confidence_level.
    """
    if budget <= 0:
        return {"error": "budget must be positive"}
    if duration_days <= 0:
        return {"error": "duration_days must be positive"}
    bm = get_recruitment_benchmarks(industry, job_category, location)
    avg_cpc, avg_cpa = bm["cpc"]["avg"], bm["cpa"]["avg"]
    avg_ctr, avg_cpm = bm["ctr"]["avg"] / 100.0, bm["cpm"]["avg"]
    impressions = int((budget / avg_cpm) * 1000)
    clicks = int(impressions * avg_ctr)
    applies = int(budget / avg_cpa) if avg_cpa > 0 else 0
    reach = int(impressions * 0.65)
    daily = budget / duration_days
    confidence = "low" if daily < 10 else ("medium" if daily < 50 else "high")
    return {
        "estimated_clicks": max(clicks, 1),
        "estimated_applies": max(applies, 1),
        "estimated_cpc": round(avg_cpc, 2),
        "estimated_cpa": round(avg_cpa, 2),
        "estimated_reach": reach,
        "estimated_impressions": impressions,
        "daily_budget": round(daily, 2),
        "duration_days": duration_days,
        "confidence_level": confidence,
        "industry": bm.get("industry") or "",
        "source": "benchmark_estimate",
    }


def get_status() -> dict:
    """Health check for the Meta Ads integration."""
    has_token = bool(_get_access_token())
    has_account = bool(_get_ad_account_id())
    return {
        "configured": has_token,
        "meta_access_token": has_token,
        "meta_ad_account_id": has_account,
        "endpoints": {
            "audience_estimate": True,
            "recruitment_benchmarks": True,
            "interest_targeting": True,
            "ad_creative_recommendations": True,
            "campaign_cost_estimate": True,
        },
        "api_enabled": has_token,
        "note": "All endpoints work with curated fallback data when API is not configured",
    }
