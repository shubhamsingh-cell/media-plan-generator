"""Google Custom Search + PageSpeed Insights integration.

Two APIs in one module:
  - Custom Search JSON API: recruitment-scoped web search
  - PageSpeed Insights API: career page performance audits

Auth: GOOGLE_SEARCH_API_KEY or GOOGLE_MAPS_API_KEY env var.
Custom Search also requires GOOGLE_CSE_ID (Programmable Search Engine ID).
Stdlib only.  Thread-safe.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_CUSTOM_SEARCH_URL = "https://www.googleapis.com/customsearch/v1"
_PAGESPEED_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
_lock = threading.Lock()
_ssl_ctx = ssl.create_default_context()
_BATCH_DELAY_S = 1.0  # 1 s between PageSpeed requests

_RECRUITMENT_SITES = (
    "linkedin.com",
    "indeed.com",
    "glassdoor.com",
    "monster.com",
    "ziprecruiter.com",
    "dice.com",
    "careerbuilder.com",
    "simplyhired.com",
)


def _get_api_key() -> Optional[str]:
    """Return a Google API key from environment, or None."""
    return (
        os.environ.get("GOOGLE_SEARCH_API_KEY")
        or os.environ.get("GOOGLE_MAPS_API_KEY")
        or None
    )


def _get_cse_id() -> Optional[str]:
    """Return the Programmable Search Engine ID from environment, or None."""
    return os.environ.get("GOOGLE_CSE_ID") or None


# ---------------------------------------------------------------------------
# Internal request helper
# ---------------------------------------------------------------------------
def _api_get(url: str, params: Dict[str, str], timeout: int = 20) -> Optional[dict]:
    """Authenticated GET to a Google API endpoint. Returns parsed JSON or None."""
    api_key = _get_api_key()
    if not api_key:
        logger.error(
            "Google API: no key configured (set GOOGLE_SEARCH_API_KEY or GOOGLE_MAPS_API_KEY)"
        )
        return None
    params["key"] = api_key
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url)
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("Google API HTTP %d: %s", exc.code, body, exc_info=True)
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Google API request failed: %s", exc, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Custom Search: internal
# ---------------------------------------------------------------------------
def _custom_search(
    query: str, num_results: int = 10, site_restrict: Optional[str] = None
) -> list[dict]:
    """Run a Custom Search query. Returns [{title, link, snippet}]."""
    cse_id = _get_cse_id()
    if not cse_id:
        logger.error("Google Custom Search: GOOGLE_CSE_ID not configured")
        return []
    results: list[dict] = []
    fetched = 0
    while fetched < num_results:
        page_size = min(10, num_results - fetched)
        params: Dict[str, str] = {
            "cx": cse_id,
            "q": query,
            "num": str(page_size),
            "start": str(fetched + 1),
        }
        if site_restrict:
            params["siteSearch"] = site_restrict
            params["siteSearchFilter"] = "i"
        data = _api_get(_CUSTOM_SEARCH_URL, params)
        if not data:
            break
        items = data.get("items") or []
        if not items:
            break
        for item in items:
            results.append(
                {
                    "title": item.get("title") or "",
                    "link": item.get("link") or "",
                    "snippet": item.get("snippet") or "",
                }
            )
        fetched += len(items)
        if fetched < num_results and items:
            time.sleep(0.2)
    return results[:num_results]


# ---------------------------------------------------------------------------
# Custom Search: public functions
# ---------------------------------------------------------------------------
def search_recruitment(query: str, num_results: int = 10) -> list[dict]:
    """Search scoped to major recruitment sites.

    Args:
        query: Search terms (e.g. "software engineer remote").
        num_results: Max results to return (default 10, API max 100).
    Returns:
        List of dicts with keys: title, link, snippet.
    """
    site_query = " OR ".join(f"site:{s}" for s in _RECRUITMENT_SITES)
    return _custom_search(f"{query} ({site_query})", num_results=min(num_results, 100))


def search_career_pages(company_name: str) -> list[dict]:
    """Find career / jobs pages for a given company.

    Args:
        company_name: Company name (e.g. "Google", "Stripe").
    Returns:
        List of dicts with keys: title, link, snippet.
    """
    query = f'"{company_name}" careers jobs "join us" OR "open positions" OR "we are hiring"'
    return _custom_search(query, num_results=10)


def search_competitor_jobs(job_title: str, location: str) -> list[dict]:
    """Find competitor job postings for a given role and location.

    Args:
        job_title: Role title (e.g. "Data Scientist").
        location: Geographic area (e.g. "New York" or "Remote").
    Returns:
        List of dicts with keys: title, link, snippet.
    """
    site_query = " OR ".join(f"site:{s}" for s in _RECRUITMENT_SITES)
    return _custom_search(
        f'"{job_title}" "{location}" job posting apply ({site_query})', num_results=10
    )


# ---------------------------------------------------------------------------
# PageSpeed Insights: internal
# ---------------------------------------------------------------------------
def _parse_pagespeed(data: dict) -> dict:
    """Extract scores and Core Web Vitals from a PageSpeed API response."""
    lhr = data.get("lighthouseResult") or {}
    categories = lhr.get("categories") or {}
    audits = lhr.get("audits") or {}

    def _score(key: str) -> Optional[float]:
        raw = (categories.get(key) or {}).get("score")
        return round(raw * 100, 1) if raw is not None else None

    cwv_keys = {
        "lcp": "largest-contentful-paint",
        "fid": "max-potential-fid",
        "cls": "cumulative-layout-shift",
    }
    core_web_vitals = {
        k: (audits.get(v) or {}).get("displayValue") or None
        for k, v in cwv_keys.items()
    }
    recommendations: list[str] = []
    for audit_data in audits.values():
        if not isinstance(audit_data, dict):
            continue
        score = audit_data.get("score")
        if score is not None and score < 0.9:
            title = audit_data.get("title") or ""
            if title:
                display = audit_data.get("displayValue") or ""
                recommendations.append(f"{title} ({display})" if display else title)
    return {
        "url": data.get("id") or "",
        "performance_score": _score("performance"),
        "accessibility_score": _score("accessibility"),
        "seo_score": _score("seo"),
        "best_practices_score": _score("best-practices"),
        "core_web_vitals": core_web_vitals,
        "recommendations": recommendations[:15],
    }


# ---------------------------------------------------------------------------
# PageSpeed Insights: public functions
# ---------------------------------------------------------------------------
def audit_career_page(url: str, strategy: str = "mobile") -> dict:
    """Audit a career page for performance, accessibility, SEO, and best practices.

    Args:
        url: Full URL to audit.
        strategy: "mobile" (default) or "desktop".
    Returns:
        Dict with performance_score, accessibility_score, seo_score,
        best_practices_score, core_web_vitals, and recommendations.
    """
    if strategy not in ("mobile", "desktop"):
        strategy = "mobile"
    api_key = _get_api_key()
    if not api_key:
        logger.error("PageSpeed: no API key configured")
        return {"error": "No API key configured", "url": url}
    query_parts = [
        ("url", url),
        ("strategy", strategy),
        ("category", "PERFORMANCE"),
        ("category", "ACCESSIBILITY"),
        ("category", "SEO"),
        ("category", "BEST_PRACTICES"),
        ("key", api_key),
    ]
    full_url = f"{_PAGESPEED_URL}?{urllib.parse.urlencode(query_parts)}"
    req = urllib.request.Request(full_url)
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        return _parse_pagespeed(data)
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("PageSpeed HTTP %d for %s: %s", exc.code, url, body, exc_info=True)
        return {"error": f"HTTP {exc.code}", "url": url}
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("PageSpeed request failed for %s: %s", url, exc, exc_info=True)
        return {"error": str(exc), "url": url}


def batch_audit_pages(urls: list[str], strategy: str = "mobile") -> list[dict]:
    """Audit multiple URLs with rate limiting between requests.

    Args:
        urls: List of URLs to audit.
        strategy: "mobile" (default) or "desktop".
    Returns:
        List of audit result dicts (same shape as audit_career_page).
    """
    results: list[dict] = []
    for i, url in enumerate(urls):
        with _lock:
            result = audit_career_page(url, strategy=strategy)
        results.append(result)
        if i < len(urls) - 1:
            time.sleep(_BATCH_DELAY_S)
    return results


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def get_status() -> dict:
    """Health check for both Google APIs.

    Returns:
        Dict with api_key_configured, cse_id_configured, and quota info.
    """
    api_key = _get_api_key()
    cse_id = _get_cse_id()
    status: Dict[str, Any] = {
        "api_key_configured": api_key is not None,
        "cse_id_configured": cse_id is not None,
        "custom_search": {
            "available": api_key is not None and cse_id is not None,
            "daily_quota_free": 100,
            "notes": "100 queries/day free; $5 per 1000 after",
        },
        "pagespeed_insights": {
            "available": api_key is not None,
            "daily_quota_free": 25000,
            "notes": "25K queries/day free; no billing required",
        },
    }
    if api_key:
        try:
            test_params = [
                ("url", "https://example.com"),
                ("strategy", "mobile"),
                ("category", "PERFORMANCE"),
                ("key", api_key),
            ]
            test_url = f"{_PAGESPEED_URL}?{urllib.parse.urlencode(test_params)}"
            req = urllib.request.Request(test_url, method="GET")
            with urllib.request.urlopen(req, context=_ssl_ctx, timeout=10) as resp:
                status["pagespeed_insights"]["reachable"] = resp.status == 200
        except Exception as exc:
            logger.warning("PageSpeed connectivity check failed: %s", exc)
            status["pagespeed_insights"]["reachable"] = False
    return status
