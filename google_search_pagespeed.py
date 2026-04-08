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

# Browser-like User-Agent for direct fetch fallback (Chrome 124 on macOS)
_CHROME_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# HTTP status codes that indicate bot-blocking / access denial
_ACCESS_BLOCKED_CODES = {403, 401, 406, 429, 451}

# Industry-standard career page best practices (benchmark-based fallback)
_CAREER_PAGE_BEST_PRACTICES: dict[str, Any] = {
    "performance": [
        "Aim for LCP (Largest Contentful Paint) under 2.5 seconds",
        "Minimize JavaScript bundle size -- career pages should load fast on mobile",
        "Use lazy loading for images and non-critical resources",
        "Enable text compression (gzip/brotli) for HTML, CSS, and JS",
        "Optimize hero images and company photos (WebP format, <200KB)",
    ],
    "accessibility": [
        "Ensure all job listing links have descriptive text (not 'click here')",
        "Provide sufficient color contrast (WCAG AA: 4.5:1 for text)",
        "Add alt text to all company images and team photos",
        "Ensure the job search and filter controls are keyboard-navigable",
        "Use semantic HTML headings (h1 for page title, h2 for sections)",
        "Include skip-to-content links for screen reader users",
    ],
    "seo": [
        "Use unique, descriptive title tags (e.g., 'Careers at [Company] | Open Roles')",
        "Add structured data (JobPosting schema) to each listing",
        "Ensure career pages are crawlable (not blocked by robots.txt)",
        "Use clean, descriptive URLs (e.g., /careers/engineering not /jobs?id=123)",
        "Include an XML sitemap for all job listing pages",
    ],
    "mobile": [
        "Ensure the Apply button is easily tappable (min 48x48px touch target)",
        "Use responsive design -- 60%+ of job seekers browse on mobile",
        "Avoid horizontal scrolling on job descriptions",
        "Keep application forms short on mobile (3-5 fields max for initial apply)",
    ],
    "general": [
        "Feature employee testimonials and company culture content",
        "Display clear employer value proposition above the fold",
        "Include benefits, salary ranges, and location details in listings",
        "Provide easy-to-find search/filter for open positions",
        "Add social proof (awards, ratings, Glassdoor score)",
    ],
}


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
# Fallback helpers for bot-blocked career pages
# ---------------------------------------------------------------------------


def _is_access_blocked_error(error_str: str) -> bool:
    """Check if a PageSpeed/fetch error indicates bot-blocking (403, etc.)."""
    error_lower = error_str.lower()
    # PageSpeed API returns errors when the target site blocks Lighthouse
    for code in _ACCESS_BLOCKED_CODES:
        if f"http {code}" in error_lower or f"{code}" in error_lower:
            return True
    for keyword in (
        "forbidden",
        "blocked",
        "access denied",
        "not allowed",
        "bot detection",
        "captcha",
        "challenge",
    ):
        if keyword in error_lower:
            return True
    return False


def _fetch_career_page_direct(url: str, timeout: int = 12) -> Optional[dict]:
    """Try fetching the career page directly with browser-like headers.

    Returns a dict with basic page info if successful, None on failure.
    """
    headers = {
        "User-Agent": _CHROME_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", errors="replace")
            status_code = resp.status
        if status_code != 200 or not content:
            return None
        content_lower = content.lower()
        # Extract basic page signals
        has_apply_button = any(
            kw in content_lower
            for kw in (
                "apply now",
                "apply for this",
                "submit application",
                "apply today",
                "apply here",
                "start application",
            )
        )
        has_search = any(
            kw in content_lower
            for kw in (
                "search jobs",
                "search positions",
                "find jobs",
                "job search",
                "search openings",
                "search roles",
            )
        )
        has_structured_data = (
            "jobposting" in content_lower or "schema.org" in content_lower
        )
        has_mobile_viewport = 'name="viewport"' in content_lower
        page_size_kb = round(len(content) / 1024, 1)
        return {
            "fetched": True,
            "method": "direct_fetch",
            "page_size_kb": page_size_kb,
            "has_apply_button": has_apply_button,
            "has_job_search": has_search,
            "has_structured_data": has_structured_data,
            "has_mobile_viewport": has_mobile_viewport,
        }
    except urllib.error.HTTPError as exc:
        logger.info("Direct fetch HTTP %d for %s", exc.code, url)
        return None
    except (urllib.error.URLError, OSError) as exc:
        logger.info("Direct fetch failed for %s: %s", url, exc)
        return None


def _fetch_via_jina(url: str, timeout: int = 12) -> Optional[dict]:
    """Try fetching the career page via Jina Reader API as a fallback.

    Returns a dict with basic page info if successful, None on failure.
    """
    jina_key = os.environ.get("JINA_API_KEY") or ""
    jina_url = f"https://r.jina.ai/{url}"
    headers: dict[str, str] = {
        "Accept": "text/markdown",
        "User-Agent": "NovaAISuite/1.0",
    }
    if jina_key:
        headers["Authorization"] = f"Bearer {jina_key}"

    req = urllib.request.Request(jina_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=timeout) as resp:
            content = resp.read().decode("utf-8", errors="replace")
        if not content or len(content.strip()) < 50:
            return None
        content_lower = content.lower()
        has_apply_button = any(
            kw in content_lower
            for kw in (
                "apply now",
                "apply for this",
                "submit application",
                "apply today",
                "apply here",
                "start application",
            )
        )
        has_search = any(
            kw in content_lower
            for kw in (
                "search jobs",
                "search positions",
                "find jobs",
                "job search",
                "search openings",
                "search roles",
            )
        )
        return {
            "fetched": True,
            "method": "jina_reader",
            "page_size_kb": round(len(content) / 1024, 1),
            "has_apply_button": has_apply_button,
            "has_job_search": has_search,
            "has_structured_data": "jobposting" in content_lower,
            "has_mobile_viewport": True,  # Cannot determine from markdown
            "content_preview": content[:500],
        }
    except urllib.error.HTTPError as exc:
        logger.info("Jina Reader HTTP %d for %s", exc.code, url)
        return None
    except (urllib.error.URLError, OSError) as exc:
        logger.info("Jina Reader failed for %s: %s", url, exc)
        return None


def _benchmark_based_audit(
    url: str, strategy: str, page_signals: Optional[dict] = None
) -> dict:
    """Return a benchmark-based audit when the page blocks automated access.

    Provides industry-standard career page best practices and any signals
    we were able to extract from partial fetches.
    """
    # Infer company name from URL
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.hostname or ""
        # Strip common prefixes: careers., jobs., www.
        company_hint = domain
        for prefix in ("careers.", "jobs.", "www.", "apply.", "recruiting."):
            if company_hint.startswith(prefix):
                company_hint = company_hint[len(prefix) :]
        # Take first part of domain
        company_hint = company_hint.split(".")[0].capitalize()
    except Exception:
        company_hint = "the company"

    recommendations: list[str] = []
    if page_signals:
        if not page_signals.get("has_apply_button"):
            recommendations.append(
                "No clear 'Apply Now' call-to-action detected -- ensure it is prominent and above the fold"
            )
        if not page_signals.get("has_job_search"):
            recommendations.append(
                "No job search/filter functionality detected -- candidates should be able to search by keyword, location, or department"
            )
        if not page_signals.get("has_structured_data"):
            recommendations.append(
                "No JobPosting structured data detected -- add Schema.org/JobPosting markup for better Google for Jobs visibility"
            )
        if not page_signals.get("has_mobile_viewport"):
            recommendations.append(
                "No mobile viewport meta tag detected -- critical for mobile job seekers (60%+ of traffic)"
            )
        page_size = page_signals.get("page_size_kb", 0)
        if page_size > 2000:
            recommendations.append(
                f"Page size is {page_size}KB -- aim for under 1500KB for fast mobile loading"
            )

    # Add general best practices
    recommendations.extend(_CAREER_PAGE_BEST_PRACTICES["general"][:3])
    if strategy == "mobile":
        recommendations.extend(_CAREER_PAGE_BEST_PRACTICES["mobile"][:2])
    recommendations.extend(_CAREER_PAGE_BEST_PRACTICES["performance"][:2])
    recommendations.extend(_CAREER_PAGE_BEST_PRACTICES["accessibility"][:2])
    recommendations.extend(_CAREER_PAGE_BEST_PRACTICES["seo"][:2])

    result: dict[str, Any] = {
        "url": url,
        "access_blocked": True,
        "audit_method": "benchmark_based",
        "note": (
            f"{company_hint}'s career page blocks automated auditing tools (HTTP 403). "
            "This is common for large enterprises using bot-detection (e.g., Cloudflare, Akamai). "
            "Below are industry-standard recommendations based on career page best practices."
        ),
        "performance_score": None,
        "accessibility_score": None,
        "seo_score": None,
        "best_practices_score": None,
        "core_web_vitals": {"lcp": None, "fid": None, "cls": None},
        "recommendations": recommendations[:15],
        "best_practices_reference": {
            "performance": _CAREER_PAGE_BEST_PRACTICES["performance"],
            "accessibility": _CAREER_PAGE_BEST_PRACTICES["accessibility"],
            "seo": _CAREER_PAGE_BEST_PRACTICES["seo"],
            "mobile": _CAREER_PAGE_BEST_PRACTICES["mobile"],
        },
    }
    if page_signals:
        result["page_signals"] = {
            k: v
            for k, v in page_signals.items()
            if k not in ("fetched", "content_preview")
        }
    return result


# ---------------------------------------------------------------------------
# PageSpeed Insights: public functions
# ---------------------------------------------------------------------------
def audit_career_page(url: str, strategy: str = "mobile") -> dict:
    """Audit a career page for performance, accessibility, SEO, and best practices.

    Uses a multi-strategy fallback chain:
      1. Google PageSpeed Insights API (full Lighthouse audit)
      2. Direct fetch with browser-like headers (basic page signals)
      3. Jina Reader API (content extraction through bot-blocked pages)
      4. Benchmark-based audit (industry best practices when all else fails)

    Args:
        url: Full URL to audit.
        strategy: "mobile" (default) or "desktop".
    Returns:
        Dict with performance_score, accessibility_score, seo_score,
        best_practices_score, core_web_vitals, and recommendations.
        When the page blocks automated access, returns benchmark-based
        recommendations with access_blocked=True.
    """
    if strategy not in ("mobile", "desktop"):
        strategy = "mobile"

    # ── Strategy 1: Google PageSpeed Insights (happy path) ──────────
    api_key = _get_api_key()
    pagespeed_error = ""
    if api_key:
        query_parts = [
            ("url", url),
            ("strategy", strategy),
            ("category", "PERFORMANCE"),
            ("category", "ACCESSIBILITY"),
            ("category", "SEO"),
            ("category", "BEST_PRACTICES"),
            ("key", api_key),
        ]
        pagespeed_url = f"{_PAGESPEED_URL}?{urllib.parse.urlencode(query_parts)}"
        req = urllib.request.Request(pagespeed_url)
        try:
            with urllib.request.urlopen(req, context=_ssl_ctx, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            result = _parse_pagespeed(data)
            # Check if PageSpeed itself reported an error from the target site
            if not result.get("error"):
                result["audit_method"] = "pagespeed_insights"
                return result
            pagespeed_error = result.get("error", "")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8")
            except Exception:
                pass
            pagespeed_error = f"HTTP {exc.code}: {body[:200]}"
            logger.warning(
                "PageSpeed HTTP %d for %s (will try fallbacks)", exc.code, url
            )
        except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
            pagespeed_error = str(exc)
            logger.warning("PageSpeed failed for %s: %s (will try fallbacks)", url, exc)
    else:
        pagespeed_error = "No API key configured"
        logger.warning("PageSpeed: no API key configured, trying fallbacks for %s", url)

    # If the error is NOT access-related, return it directly (genuine API issue)
    if pagespeed_error and not _is_access_blocked_error(pagespeed_error):
        # Still try direct fetch for basic signals before giving up
        page_signals = _fetch_career_page_direct(url)
        if page_signals:
            return {
                "url": url,
                "audit_method": "direct_fetch",
                "note": f"PageSpeed unavailable ({pagespeed_error}). Basic page analysis from direct fetch.",
                "performance_score": None,
                "accessibility_score": None,
                "seo_score": None,
                "best_practices_score": None,
                "core_web_vitals": {"lcp": None, "fid": None, "cls": None},
                "page_signals": {
                    k: v
                    for k, v in page_signals.items()
                    if k not in ("fetched", "content_preview")
                },
                "recommendations": _CAREER_PAGE_BEST_PRACTICES["general"][:5],
            }
        return {"error": pagespeed_error, "url": url}

    # ── Access blocked path: try fallback strategies ────────────────
    logger.info("Career page %s appears to block bots, trying fallback chain", url)

    # ── Strategy 2: Direct fetch with browser-like headers ──────────
    page_signals = _fetch_career_page_direct(url)
    if page_signals:
        logger.info("Direct fetch succeeded for %s", url)
        # We got basic signals but no Lighthouse scores
        return _benchmark_based_audit(url, strategy, page_signals=page_signals)

    # ── Strategy 3: Jina Reader API ─────────────────────────────────
    jina_signals = _fetch_via_jina(url)
    if jina_signals:
        logger.info("Jina Reader succeeded for %s", url)
        return _benchmark_based_audit(url, strategy, page_signals=jina_signals)

    # ── Strategy 4: Pure benchmark-based audit ──────────────────────
    logger.info("All fetch strategies failed for %s, returning benchmark audit", url)
    return _benchmark_based_audit(url, strategy, page_signals=None)


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
