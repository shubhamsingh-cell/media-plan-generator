"""
firecrawl_enrichment.py -- Firecrawl REST API integration for live web data enrichment.

Provides real-time market data to Nova AI Suite products via Firecrawl's
scrape, map, and search APIs. Three main use cases:

    1. scrape_job_board_pricing()  -- Current CPC/CPA from job board pricing pages
    2. analyze_competitor_careers() -- Hiring intelligence from company career pages
    3. fetch_recruitment_news()     -- Latest recruitment industry news & trends

All external API calls:
    - Use only urllib.request (stdlib, no third-party dependencies)
    - Have a 15-second timeout per call
    - Are cached on disk with configurable TTL
    - Fail gracefully with fallback data (never crash the calling product)
    - Log errors with exc_info=True per project rules

Cache storage: data/firecrawl_cache/ (disk-based JSON, MD5-keyed)

Usage:
    from firecrawl_enrichment import scrape_job_board_pricing
    pricing = scrape_job_board_pricing("indeed")
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import ssl
import time
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

FIRECRAWL_API_KEY: str = os.environ.get("FIRECRAWL_API_KEY") or ""
FIRECRAWL_BASE_URL: str = "https://api.firecrawl.dev/v1"
CACHE_DIR: Path = Path(__file__).resolve().parent / "data" / "firecrawl_cache"
REQUEST_TIMEOUT: int = 15  # seconds

# TTL constants (seconds)
TTL_JOB_BOARD: int = 86400  # 24 hours
TTL_CAREERS: int = 21600  # 6 hours
TTL_NEWS: int = 43200  # 12 hours
TTL_JOB_VOLUME: int = 43200  # 12 hours
TTL_JOB_DENSITY: int = 86400  # 24 hours
TTL_COMPLIANCE: int = 604800  # 168 hours (weekly)
TTL_SALARY: int = 604800  # 168 hours (weekly)
TTL_AD_SPECS: int = 2592000  # 720 hours (monthly)

# Ensure cache directory exists at import time
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
except OSError as exc:
    logger.warning(f"Could not create firecrawl cache dir: {exc}")


# ═══════════════════════════════════════════════════════════════════════════════
# DISK CACHE (JSON-based, TTL-aware)
# ═══════════════════════════════════════════════════════════════════════════════


def _cache_key(func_name: str, *args: Any) -> str:
    """Generate an MD5 cache key from function name and arguments.

    Returns a hex digest string suitable for use as a filename.
    """
    raw = f"{func_name}:{json.dumps(args, sort_keys=True, default=str)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _cache_get(key: str, ttl: int) -> Optional[Any]:
    """Retrieve cached data if it exists and has not expired.

    Args:
        key: MD5 hex digest cache key.
        ttl: Maximum age in seconds before the entry is considered stale.

    Returns:
        Cached data if valid, or None if missing/expired/corrupt.
    """
    cache_file = CACHE_DIR / f"{key}.json"
    if not cache_file.exists():
        return None
    try:
        with open(cache_file, "r", encoding="utf-8") as fh:
            entry = json.load(fh)
        cached_at = entry.get("cached_at") or 0
        if time.time() - cached_at < ttl:
            return entry.get("data")
        # Expired -- remove stale file
        cache_file.unlink(missing_ok=True)
    except (json.JSONDecodeError, OSError, KeyError) as exc:
        logger.debug(f"Cache read failed for {key}: {exc}")
        try:
            cache_file.unlink(missing_ok=True)
        except OSError:
            pass
    return None


def _cache_set(key: str, data: Any, ttl: int) -> None:
    """Write data to disk cache with timestamp and TTL metadata.

    Args:
        key: MD5 hex digest cache key.
        data: JSON-serializable data to cache.
        ttl: TTL in seconds (stored for reference, checked on read).
    """
    cache_file = CACHE_DIR / f"{key}.json"
    entry = {
        "cached_at": time.time(),
        "ttl": ttl,
        "data": data,
    }
    try:
        with open(cache_file, "w", encoding="utf-8") as fh:
            json.dump(entry, fh, ensure_ascii=False)
    except OSError as exc:
        logger.warning(f"Failed to write firecrawl cache {cache_file}: {exc}")


def _cleanup_cache(max_files: int = 500) -> None:
    """Remove oldest cache files if count exceeds max_files."""
    try:
        files = sorted(CACHE_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime)
        if len(files) > max_files:
            for f in files[: len(files) - max_files]:
                try:
                    f.unlink()
                except OSError:
                    pass
    except OSError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# FIRECRAWL REST API HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _build_ssl_context() -> ssl.SSLContext:
    """Build a permissive SSL context for urllib requests."""
    ctx = ssl.create_default_context()
    return ctx


def _firecrawl_request(
    endpoint: str,
    payload: dict[str, Any],
    method: str = "POST",
) -> Optional[dict[str, Any]]:
    """Make an authenticated request to the Firecrawl REST API.

    Args:
        endpoint: API path after base URL (e.g., "/scrape", "/map", "/search").
        payload: JSON body to send.
        method: HTTP method (default POST).

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    if not FIRECRAWL_API_KEY:
        logger.warning("FIRECRAWL_API_KEY not set -- skipping Firecrawl request")
        return None

    url = f"{FIRECRAWL_BASE_URL}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
    }
    body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, headers=headers, method=method)

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except HTTPError as exc:
        logger.error(
            f"Firecrawl API HTTP error {exc.code} for {endpoint}: {exc.reason}",
            exc_info=True,
        )
    except URLError as exc:
        logger.error(
            f"Firecrawl API URL error for {endpoint}: {exc.reason}",
            exc_info=True,
        )
    except json.JSONDecodeError as exc:
        logger.error(
            f"Firecrawl API JSON decode error for {endpoint}: {exc}",
            exc_info=True,
        )
    except OSError as exc:
        logger.error(
            f"Firecrawl API OS error for {endpoint}: {exc}",
            exc_info=True,
        )
    return None


def _extract_text_from_markdown(markdown: str) -> str:
    """Strip markdown formatting to get plain text for analysis."""
    if not markdown:
        return ""
    # Remove markdown links, images, headers, bold, italic
    text = re.sub(r"!\[.*?\]\(.*?\)", "", markdown)
    text = re.sub(r"\[([^\]]+)\]\(.*?\)", r"\1", text)
    text = re.sub(r"#{1,6}\s*", "", text)
    text = re.sub(r"\*{1,2}(.*?)\*{1,2}", r"\1", text)
    text = re.sub(r"_{1,2}(.*?)_{1,2}", r"\1", text)
    return text.strip()


# ═══════════════════════════════════════════════════════════════════════════════
# JOB BOARD PRICING URLs
# ═══════════════════════════════════════════════════════════════════════════════

_JOB_BOARD_URLS: dict[str, str] = {
    "indeed": "https://www.indeed.com/hire/pricing",
    "linkedin": "https://business.linkedin.com/talent-solutions/pricing",
    "ziprecruiter": "https://www.ziprecruiter.com/pricing",
    "glassdoor": "https://www.glassdoor.com/employers/pricing",
    "monster": "https://hiring.monster.com/pricing/",
    "careerbuilder": "https://www.careerbuilder.com/solutions/pricing",
}

# Hardcoded fallback benchmarks (used when API unavailable)
_FALLBACK_BENCHMARKS: dict[str, dict[str, Any]] = {
    "indeed": {
        "board_name": "Indeed",
        "cpc_range": {"min": 0.15, "max": 5.00, "currency": "USD"},
        "cpa_estimate": {"min": 8.00, "max": 35.00, "currency": "USD"},
        "posting_cost": {"free_option": True, "sponsored_min": 5.00},
        "model": "CPC (Sponsored Jobs)",
        "source": "fallback_benchmarks",
    },
    "linkedin": {
        "board_name": "LinkedIn",
        "cpc_range": {"min": 2.00, "max": 8.00, "currency": "USD"},
        "cpa_estimate": {"min": 30.00, "max": 90.00, "currency": "USD"},
        "posting_cost": {"free_option": True, "promoted_min": 10.00},
        "model": "CPC (Promoted Jobs)",
        "source": "fallback_benchmarks",
    },
    "ziprecruiter": {
        "board_name": "ZipRecruiter",
        "cpc_range": {"min": 0.50, "max": 5.50, "currency": "USD"},
        "cpa_estimate": {"min": 12.00, "max": 45.00, "currency": "USD"},
        "posting_cost": {"free_option": False, "standard_monthly": 299.00},
        "model": "Performance-based (CPC + CPA)",
        "source": "fallback_benchmarks",
    },
    "glassdoor": {
        "board_name": "Glassdoor",
        "cpc_range": {"min": 1.00, "max": 6.00, "currency": "USD"},
        "cpa_estimate": {"min": 15.00, "max": 50.00, "currency": "USD"},
        "posting_cost": {"free_option": False, "sponsored_min": 249.00},
        "model": "CPC (Sponsored Listings)",
        "source": "fallback_benchmarks",
    },
    "monster": {
        "board_name": "Monster",
        "cpc_range": {"min": 0.30, "max": 4.00, "currency": "USD"},
        "cpa_estimate": {"min": 10.00, "max": 40.00, "currency": "USD"},
        "posting_cost": {"free_option": False, "single_posting": 279.00},
        "model": "CPC + Subscription",
        "source": "fallback_benchmarks",
    },
    "careerbuilder": {
        "board_name": "CareerBuilder",
        "cpc_range": {"min": 0.40, "max": 5.00, "currency": "USD"},
        "cpa_estimate": {"min": 12.00, "max": 45.00, "currency": "USD"},
        "posting_cost": {"free_option": False, "single_posting": 375.00},
        "model": "CPC + Subscription",
        "source": "fallback_benchmarks",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: scrape_job_board_pricing
# ═══════════════════════════════════════════════════════════════════════════════


def scrape_job_board_pricing(board_name: str) -> dict[str, Any]:
    """Scrape current pricing/CPC data from a job board's pricing page.

    Uses Firecrawl's scrape API with JSON extraction to pull structured
    pricing information. Falls back to hardcoded benchmarks on failure.

    Args:
        board_name: Lowercase board identifier. One of: indeed, linkedin,
                    ziprecruiter, glassdoor, monster, careerbuilder.

    Returns:
        Dict with keys: board_name, cpc_range, cpa_estimate, posting_cost,
        model, source, last_updated.
    """
    board_key = board_name.lower().strip()
    fallback = _FALLBACK_BENCHMARKS.get(board_key) or {
        "board_name": board_name,
        "cpc_range": {"min": 0.50, "max": 5.00, "currency": "USD"},
        "cpa_estimate": {"min": 10.00, "max": 50.00, "currency": "USD"},
        "posting_cost": {"free_option": False},
        "model": "Unknown",
        "source": "fallback_benchmarks",
    }

    # Check cache first
    ckey = _cache_key("scrape_job_board_pricing", board_key)
    cached = _cache_get(ckey, TTL_JOB_BOARD)
    if cached is not None:
        logger.debug(f"Firecrawl job board cache hit: {board_key}")
        return cached

    url = _JOB_BOARD_URLS.get(board_key)
    if not url:
        logger.warning(f"Unknown job board: {board_key}, using fallback")
        fallback["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return fallback

    # Use Firecrawl scrape with JSON extraction
    payload: dict[str, Any] = {
        "url": url,
        "formats": ["json"],
        "jsonOptions": {
            "prompt": (
                "Extract job posting pricing information including: "
                "cost-per-click (CPC) range with min and max values, "
                "cost-per-application (CPA) estimate range, "
                "base job posting cost or subscription price, "
                "pricing model type (CPC, CPA, subscription, flat-rate), "
                "any free tier or trial options."
            ),
            "schema": {
                "type": "object",
                "properties": {
                    "cpc_min": {"type": "number", "description": "Minimum CPC in USD"},
                    "cpc_max": {"type": "number", "description": "Maximum CPC in USD"},
                    "cpa_min": {"type": "number", "description": "Minimum CPA in USD"},
                    "cpa_max": {"type": "number", "description": "Maximum CPA in USD"},
                    "posting_cost": {
                        "type": "number",
                        "description": "Base posting cost in USD",
                    },
                    "free_option": {
                        "type": "boolean",
                        "description": "Whether a free posting option exists",
                    },
                    "pricing_model": {
                        "type": "string",
                        "description": "Pricing model type",
                    },
                },
            },
        },
        "onlyMainContent": True,
    }

    response = _firecrawl_request("/scrape", payload)
    if not response or not response.get("success"):
        logger.warning(
            f"Firecrawl scrape failed for {board_key}, using fallback benchmarks"
        )
        fallback["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return fallback

    # Parse the extracted JSON data
    extracted = response.get("data", {}).get("json") or {}

    result: dict[str, Any] = {
        "board_name": _FALLBACK_BENCHMARKS.get(board_key, {}).get("board_name")
        or board_name.title(),
        "cpc_range": {
            "min": extracted.get("cpc_min")
            or fallback.get("cpc_range", {}).get("min", 0.50),
            "max": extracted.get("cpc_max")
            or fallback.get("cpc_range", {}).get("max", 5.00),
            "currency": "USD",
        },
        "cpa_estimate": {
            "min": extracted.get("cpa_min")
            or fallback.get("cpa_estimate", {}).get("min", 10.00),
            "max": extracted.get("cpa_max")
            or fallback.get("cpa_estimate", {}).get("max", 50.00),
            "currency": "USD",
        },
        "posting_cost": {
            "free_option": (
                extracted.get("free_option")
                if extracted.get("free_option") is not None
                else fallback.get("posting_cost", {}).get("free_option", False)
            ),
            "base_cost": extracted.get("posting_cost"),
        },
        "model": extracted.get("pricing_model") or fallback.get("model", "Unknown"),
        "source": "firecrawl_live",
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    _cache_set(ckey, result, TTL_JOB_BOARD)
    logger.info(f"Firecrawl scraped pricing for {board_key} successfully")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: analyze_competitor_careers
# ═══════════════════════════════════════════════════════════════════════════════


def analyze_competitor_careers(company_domain: str) -> dict[str, Any]:
    """Scrape a company's careers page to extract hiring intelligence.

    First attempts to discover career page URLs via Firecrawl map, then
    scrapes the most relevant page for structured hiring data.

    Args:
        company_domain: Company domain (e.g., "google.com"). Do not include
                       protocol prefix.

    Returns:
        Dict with keys: company, domain, total_openings, departments,
        locations, boards_detected, career_urls, source, last_updated.
    """
    domain = company_domain.lower().strip().rstrip("/")
    # Strip protocol if accidentally included
    if domain.startswith("http://"):
        domain = domain[7:]
    if domain.startswith("https://"):
        domain = domain[8:]

    empty_result: dict[str, Any] = {
        "company": domain.split(".")[0].title(),
        "domain": domain,
        "total_openings": 0,
        "departments": [],
        "locations": [],
        "boards_detected": [],
        "career_urls": [],
        "source": "unavailable",
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    # Check cache
    ckey = _cache_key("analyze_competitor_careers", domain)
    cached = _cache_get(ckey, TTL_CAREERS)
    if cached is not None:
        logger.debug(f"Firecrawl careers cache hit: {domain}")
        return cached

    # Step 1: Use Firecrawl map to discover career page URLs
    career_urls: list[str] = []
    map_payload: dict[str, Any] = {
        "url": f"https://{domain}",
        "search": "careers jobs openings",
        "limit": 20,
    }
    map_response = _firecrawl_request("/map", map_payload)
    if map_response and map_response.get("success"):
        raw_links = map_response.get("links") or map_response.get("urls") or []
        career_keywords = ("career", "job", "opening", "hiring", "work-with-us")
        for link in raw_links:
            link_lower = (link or "").lower()
            if any(kw in link_lower for kw in career_keywords):
                career_urls.append(link)
        logger.debug(f"Firecrawl map found {len(career_urls)} career URLs for {domain}")

    # Step 2: Build candidate URLs (discovered + common patterns)
    candidate_urls = career_urls[:3]  # Top 3 from map
    for suffix in ("/careers", "/jobs", "/careers/search"):
        candidate = f"https://{domain}{suffix}"
        if candidate not in candidate_urls:
            candidate_urls.append(candidate)

    if not candidate_urls:
        logger.warning(f"No career URLs found for {domain}")
        return empty_result

    # Step 3: Scrape the best career page with JSON extraction
    scrape_url = candidate_urls[0]
    scrape_payload: dict[str, Any] = {
        "url": scrape_url,
        "formats": ["json", "markdown"],
        "jsonOptions": {
            "prompt": (
                "Extract hiring information from this careers/jobs page: "
                "total number of open positions, departments or teams hiring, "
                "office locations mentioned, any job boards or ATS platforms "
                "referenced (e.g., Greenhouse, Lever, Workday, Taleo)."
            ),
            "schema": {
                "type": "object",
                "properties": {
                    "total_openings": {
                        "type": "integer",
                        "description": "Total number of open job positions",
                    },
                    "departments": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of departments or teams hiring",
                    },
                    "locations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Office locations mentioned",
                    },
                    "ats_platform": {
                        "type": "string",
                        "description": "ATS or job board platform detected",
                    },
                },
            },
        },
        "onlyMainContent": True,
    }

    scrape_response = _firecrawl_request("/scrape", scrape_payload)

    if not scrape_response or not scrape_response.get("success"):
        # Try fallback URL if first failed
        if len(candidate_urls) > 1:
            scrape_payload["url"] = candidate_urls[1]
            scrape_response = _firecrawl_request("/scrape", scrape_payload)

    if not scrape_response or not scrape_response.get("success"):
        logger.warning(f"Firecrawl scrape failed for {domain} careers")
        empty_result["career_urls"] = career_urls[:5]
        return empty_result

    # Parse extracted data
    resp_data = scrape_response.get("data") or {}
    extracted = resp_data.get("json") or {}
    markdown_content = resp_data.get("markdown") or ""

    # Detect job boards from markdown content
    boards_detected: list[str] = []
    board_patterns = {
        "greenhouse": "Greenhouse",
        "lever.co": "Lever",
        "workday": "Workday",
        "taleo": "Taleo",
        "icims": "iCIMS",
        "smartrecruiters": "SmartRecruiters",
        "jobvite": "Jobvite",
        "bamboohr": "BambooHR",
        "ashbyhq": "Ashby",
    }
    content_lower = markdown_content.lower()
    for pattern, board_label in board_patterns.items():
        if pattern in content_lower:
            boards_detected.append(board_label)

    ats = extracted.get("ats_platform") or ""
    if ats and ats not in boards_detected:
        boards_detected.append(ats)

    result: dict[str, Any] = {
        "company": domain.split(".")[0].title(),
        "domain": domain,
        "total_openings": extracted.get("total_openings") or 0,
        "departments": extracted.get("departments") or [],
        "locations": extracted.get("locations") or [],
        "boards_detected": boards_detected,
        "career_urls": career_urls[:5],
        "source": "firecrawl_live",
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    _cache_set(ckey, result, TTL_CAREERS)
    logger.info(
        f"Firecrawl analyzed careers for {domain}: "
        f"{result['total_openings']} openings, "
        f"{len(result['departments'])} departments"
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: fetch_recruitment_news
# ═══════════════════════════════════════════════════════════════════════════════


def fetch_recruitment_news(
    topic: str = "recruitment advertising",
) -> list[dict[str, Any]]:
    """Search for recent recruitment industry news and trends.

    Uses Firecrawl search to find articles, then scrapes top results
    for structured summaries.

    Args:
        topic: Search topic string (default: "recruitment advertising").

    Returns:
        List of dicts, each with: title, summary, source, url, date.
        Returns empty list on failure.
    """
    safe_topic = (topic or "recruitment advertising").strip()

    # Check cache
    ckey = _cache_key("fetch_recruitment_news", safe_topic)
    cached = _cache_get(ckey, TTL_NEWS)
    if cached is not None:
        logger.debug(f"Firecrawl news cache hit: {safe_topic}")
        return cached

    # Step 1: Search for recent articles
    search_payload: dict[str, Any] = {
        "query": f"{safe_topic} latest news trends 2026",
        "limit": 5,
        "scrapeOptions": {
            "formats": ["json"],
            "jsonOptions": {
                "prompt": (
                    "Extract the article title, a 1-2 sentence summary, "
                    "the publication/source name, and the publication date."
                ),
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "Article headline",
                        },
                        "summary": {
                            "type": "string",
                            "description": "1-2 sentence summary",
                        },
                        "source_name": {
                            "type": "string",
                            "description": "Publication or website name",
                        },
                        "date": {
                            "type": "string",
                            "description": "Publication date",
                        },
                    },
                },
            },
            "onlyMainContent": True,
        },
    }

    search_response = _firecrawl_request("/search", search_payload)
    if not search_response or not search_response.get("success"):
        logger.warning(f"Firecrawl search failed for topic: {safe_topic}")
        return []

    results_raw = search_response.get("data") or []
    articles: list[dict[str, Any]] = []

    for item in results_raw[:5]:
        extracted = item.get("json") or {}
        url = item.get("url") or item.get("metadata", {}).get("sourceURL") or ""
        title = (
            extracted.get("title")
            or item.get("metadata", {}).get("title")
            or "Untitled"
        )
        summary = extracted.get("summary") or ""
        source_name = (
            extracted.get("source_name")
            or item.get("metadata", {}).get("source")
            or _domain_from_url(url)
        )
        date = (
            extracted.get("date") or item.get("metadata", {}).get("publishedDate") or ""
        )

        articles.append(
            {
                "title": title,
                "summary": summary,
                "source": source_name,
                "url": url,
                "date": date,
            }
        )

    _cache_set(ckey, articles, TTL_NEWS)
    logger.info(f"Firecrawl fetched {len(articles)} news articles for: {safe_topic}")
    return articles


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: scrape_job_posting_volume (HireSignal)
# ═══════════════════════════════════════════════════════════════════════════════


def scrape_job_posting_volume(role: str, location: str = "") -> dict[str, Any]:
    """Scrape Indeed/LinkedIn for current job posting count by role and location.

    Uses Firecrawl search to find job listing pages and extract approximate
    counts of open positions for demand-signal analysis.

    Args:
        role: Job title or role to search for (e.g., "Software Engineer").
        location: Optional location filter (e.g., "New York, NY").

    Returns:
        Dict with keys: role, location, estimated_openings, sources, trend,
        source, last_updated.
    """
    safe_role = (role or "").strip()
    safe_location = (location or "").strip()
    if not safe_role:
        return {
            "role": "",
            "location": safe_location,
            "estimated_openings": 0,
            "sources": [],
            "trend": "unknown",
            "source": "error",
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    ckey = _cache_key("scrape_job_posting_volume", safe_role, safe_location)
    cached = _cache_get(ckey, TTL_JOB_VOLUME)
    if cached is not None:
        logger.debug(f"Firecrawl job volume cache hit: {safe_role} {safe_location}")
        return cached

    location_clause = f" {safe_location}" if safe_location else ""
    query = (
        f'"{safe_role}" jobs{location_clause} site:indeed.com OR site:linkedin.com/jobs'
    )

    search_payload: dict[str, Any] = {
        "query": query,
        "limit": 5,
    }

    fallback: dict[str, Any] = {
        "role": safe_role,
        "location": safe_location,
        "estimated_openings": 0,
        "sources": [],
        "trend": "unknown",
        "source": "unavailable",
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        response = _firecrawl_request("/search", search_payload)
        if not response or not response.get("success"):
            logger.warning(f"Firecrawl job volume search failed for: {safe_role}")
            return fallback

        results = response.get("data") or []
        sources: list[str] = []
        total_estimate = 0

        for item in results[:5]:
            url = item.get("url") or item.get("metadata", {}).get("sourceURL") or ""
            title = item.get("metadata", {}).get("title") or ""
            description = item.get("metadata", {}).get("description") or ""
            snippet = f"{title} {description}"

            if url:
                sources.append(url)

            # Extract numbers from result snippets (e.g., "3,456 jobs found")
            numbers = re.findall(
                r"([\d,]+)\s*(?:jobs?|positions?|openings?|results?)",
                snippet,
                re.IGNORECASE,
            )
            for num_str in numbers:
                try:
                    parsed_num = int(num_str.replace(",", ""))
                    if parsed_num > total_estimate:
                        total_estimate = parsed_num
                except ValueError:
                    pass

        # Determine trend based on volume
        if total_estimate > 10000:
            trend = "high_demand"
        elif total_estimate > 1000:
            trend = "moderate_demand"
        elif total_estimate > 0:
            trend = "low_demand"
        else:
            trend = "unknown"

        result: dict[str, Any] = {
            "role": safe_role,
            "location": safe_location,
            "estimated_openings": total_estimate,
            "sources": sources[:5],
            "trend": trend,
            "source": "firecrawl_live",
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        _cache_set(ckey, result, TTL_JOB_VOLUME)
        logger.info(f"Firecrawl job volume for {safe_role}: {total_estimate} openings")
        return result

    except (ValueError, TypeError, KeyError) as exc:
        logger.error(
            f"Error parsing job volume data for {safe_role}: {exc}", exc_info=True
        )
        return fallback


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: scrape_job_density_by_location (Talent Heatmap)
# ═══════════════════════════════════════════════════════════════════════════════


def scrape_job_density_by_location(role: str, locations: list[str]) -> dict[str, Any]:
    """Scrape job posting density across multiple locations for a role.

    Queries Firecrawl search for each location to estimate relative posting
    volume, enabling geographic demand comparison for talent heatmaps.

    Args:
        role: Job title or role to search for.
        locations: List of city/region names to compare.

    Returns:
        Dict with keys: role, locations (dict of city->count), source,
        last_updated.
    """
    safe_role = (role or "").strip()
    safe_locations = [loc.strip() for loc in (locations or []) if loc and loc.strip()]

    if not safe_role or not safe_locations:
        return {
            "role": safe_role,
            "locations": {},
            "source": "error",
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    ckey = _cache_key("scrape_job_density_by_location", safe_role, safe_locations)
    cached = _cache_get(ckey, TTL_JOB_DENSITY)
    if cached is not None:
        logger.debug(f"Firecrawl job density cache hit: {safe_role}")
        return cached

    location_counts: dict[str, int] = {}

    for loc in safe_locations:
        query = f'"{safe_role}" jobs in {loc} site:indeed.com'
        search_payload: dict[str, Any] = {
            "query": query,
            "limit": 3,
        }

        try:
            response = _firecrawl_request("/search", search_payload)
            if not response or not response.get("success"):
                location_counts[loc] = 0
                continue

            results = response.get("data") or []
            best_count = 0

            for item in results[:3]:
                title = item.get("metadata", {}).get("title") or ""
                description = item.get("metadata", {}).get("description") or ""
                snippet = f"{title} {description}"

                numbers = re.findall(
                    r"([\d,]+)\s*(?:jobs?|positions?|openings?|results?)",
                    snippet,
                    re.IGNORECASE,
                )
                for num_str in numbers:
                    try:
                        parsed_num = int(num_str.replace(",", ""))
                        if parsed_num > best_count:
                            best_count = parsed_num
                    except ValueError:
                        pass

            location_counts[loc] = best_count

        except (ValueError, TypeError, KeyError) as exc:
            logger.error(f"Error scraping job density for {loc}: {exc}", exc_info=True)
            location_counts[loc] = 0

    result: dict[str, Any] = {
        "role": safe_role,
        "locations": location_counts,
        "source": (
            "firecrawl_live"
            if any(v > 0 for v in location_counts.values())
            else "unavailable"
        ),
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    _cache_set(ckey, result, TTL_JOB_DENSITY)
    logger.info(
        f"Firecrawl job density for {safe_role}: {len(location_counts)} locations"
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: scrape_compliance_updates (ComplianceGuard)
# ═══════════════════════════════════════════════════════════════════════════════


def scrape_compliance_updates() -> list[dict[str, Any]]:
    """Scrape latest pay transparency and employment law updates.

    Searches for recent legislative changes affecting recruitment advertising,
    focusing on pay transparency laws, salary disclosure requirements, and
    employment compliance regulations.

    Returns:
        List of dicts, each with: title, summary, jurisdiction,
        effective_date, url. Returns empty list on failure.
    """
    ckey = _cache_key("scrape_compliance_updates")
    cached = _cache_get(ckey, TTL_COMPLIANCE)
    if cached is not None:
        logger.debug("Firecrawl compliance updates cache hit")
        return cached

    search_payload: dict[str, Any] = {
        "query": '"pay transparency law 2025 2026" OR "salary disclosure requirement new"',
        "limit": 5,
        "scrapeOptions": {
            "formats": ["json"],
            "jsonOptions": {
                "prompt": (
                    "Extract the law or regulation title, a brief 1-2 sentence summary, "
                    "the jurisdiction (state, country, or city), "
                    "the effective date if mentioned, and the page URL."
                ),
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "Law or regulation title",
                        },
                        "summary": {
                            "type": "string",
                            "description": "1-2 sentence summary",
                        },
                        "jurisdiction": {
                            "type": "string",
                            "description": "State, country, or city",
                        },
                        "effective_date": {
                            "type": "string",
                            "description": "Effective date if mentioned",
                        },
                    },
                },
            },
            "onlyMainContent": True,
        },
    }

    try:
        response = _firecrawl_request("/search", search_payload)
        if not response or not response.get("success"):
            logger.warning("Firecrawl compliance updates search failed")
            return []

        results_raw = response.get("data") or []
        updates: list[dict[str, Any]] = []

        for item in results_raw[:5]:
            extracted = item.get("json") or {}
            url = item.get("url") or item.get("metadata", {}).get("sourceURL") or ""
            title = (
                extracted.get("title")
                or item.get("metadata", {}).get("title")
                or "Untitled"
            )
            summary = extracted.get("summary") or ""
            jurisdiction = extracted.get("jurisdiction") or ""
            effective_date = extracted.get("effective_date") or ""

            updates.append(
                {
                    "title": title,
                    "summary": summary,
                    "jurisdiction": jurisdiction,
                    "effective_date": effective_date,
                    "url": url,
                }
            )

        _cache_set(ckey, updates, TTL_COMPLIANCE)
        logger.info(f"Firecrawl fetched {len(updates)} compliance updates")
        return updates

    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"Error parsing compliance updates: {exc}", exc_info=True)
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: scrape_salary_data (PayScale Sync)
# ═══════════════════════════════════════════════════════════════════════════════


def scrape_salary_data(role: str, location: str = "") -> dict[str, Any]:
    """Scrape salary benchmarks from Glassdoor/Levels.fyi.

    Uses Firecrawl search to find salary data pages and extract compensation
    ranges from search result snippets and scraped content.

    Args:
        role: Job title to look up salary data for.
        location: Optional location for regional salary data.

    Returns:
        Dict with keys: role, location, salary_range (min/median/max),
        sources, source, last_updated.
    """
    safe_role = (role or "").strip()
    safe_location = (location or "").strip()

    fallback: dict[str, Any] = {
        "role": safe_role,
        "location": safe_location,
        "salary_range": {"min": 0, "median": 0, "max": 0},
        "sources": [],
        "source": "unavailable",
        "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    if not safe_role:
        fallback["source"] = "error"
        return fallback

    ckey = _cache_key("scrape_salary_data", safe_role, safe_location)
    cached = _cache_get(ckey, TTL_SALARY)
    if cached is not None:
        logger.debug(f"Firecrawl salary cache hit: {safe_role} {safe_location}")
        return cached

    location_clause = f" {safe_location}" if safe_location else ""
    query = (
        f'"{safe_role}" salary{location_clause} site:glassdoor.com OR site:levels.fyi'
    )

    search_payload: dict[str, Any] = {
        "query": query,
        "limit": 5,
    }

    try:
        response = _firecrawl_request("/search", search_payload)
        if not response or not response.get("success"):
            logger.warning(f"Firecrawl salary search failed for: {safe_role}")
            return fallback

        results = response.get("data") or []
        sources: list[str] = []
        salary_values: list[int] = []

        for item in results[:5]:
            url = item.get("url") or item.get("metadata", {}).get("sourceURL") or ""
            title = item.get("metadata", {}).get("title") or ""
            description = item.get("metadata", {}).get("description") or ""
            snippet = f"{title} {description}"

            if url:
                sources.append(url)

            # Extract dollar amounts (e.g., "$85,000", "$120K", "$150,000/yr")
            dollar_matches = re.findall(
                r"\$\s?([\d,]+)(?:\s*[Kk])?\s*(?:/\s*(?:yr|year|annually))?",
                snippet,
            )
            for match in dollar_matches:
                try:
                    val = int(match.replace(",", ""))
                    # Normalize K values
                    if val < 1000:
                        val *= 1000
                    if 20000 <= val <= 500000:
                        salary_values.append(val)
                except ValueError:
                    pass

        if salary_values:
            salary_values.sort()
            sal_min = salary_values[0]
            sal_max = salary_values[-1]
            sal_median = salary_values[len(salary_values) // 2]
        else:
            sal_min = sal_median = sal_max = 0

        result: dict[str, Any] = {
            "role": safe_role,
            "location": safe_location,
            "salary_range": {"min": sal_min, "median": sal_median, "max": sal_max},
            "sources": sources[:5],
            "source": "firecrawl_live" if salary_values else "unavailable",
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        _cache_set(ckey, result, TTL_SALARY)
        logger.info(
            f"Firecrawl salary for {safe_role}: "
            f"${sal_min:,}-${sal_max:,} ({len(salary_values)} data points)"
        )
        return result

    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"Error parsing salary data for {safe_role}: {exc}", exc_info=True)
        return fallback


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API: scrape_platform_ad_specs (Social Plan)
# ═══════════════════════════════════════════════════════════════════════════════

_AD_SPEC_URLS: dict[str, str] = {
    "facebook": "https://www.facebook.com/business/ads-guide",
    "linkedin": "https://business.linkedin.com/marketing-solutions/ads/ad-specifications",
    "tiktok": "https://ads.tiktok.com/help/article/tiktok-ad-specs",
}

_FALLBACK_AD_SPECS: dict[str, dict[str, Any]] = {
    "facebook": {
        "platform": "Facebook",
        "formats": ["Image", "Video", "Carousel", "Stories"],
        "image_specs": {
            "recommended_size": "1080x1080",
            "aspect_ratio": "1:1",
            "max_file_size_mb": 30,
        },
        "video_specs": {
            "max_duration_sec": 240,
            "recommended_resolution": "1080x1080",
            "max_file_size_mb": 4000,
        },
        "text_limits": {"primary_text": 125, "headline": 40, "description": 30},
        "source": "fallback_specs",
    },
    "linkedin": {
        "platform": "LinkedIn",
        "formats": ["Single Image", "Video", "Carousel", "Message Ad"],
        "image_specs": {
            "recommended_size": "1200x627",
            "aspect_ratio": "1.91:1",
            "max_file_size_mb": 5,
        },
        "video_specs": {
            "max_duration_sec": 600,
            "recommended_resolution": "1920x1080",
            "max_file_size_mb": 200,
        },
        "text_limits": {"intro_text": 600, "headline": 70, "description": 100},
        "source": "fallback_specs",
    },
    "tiktok": {
        "platform": "TikTok",
        "formats": ["In-Feed", "TopView", "Branded Hashtag", "Spark Ads"],
        "image_specs": {
            "recommended_size": "1080x1920",
            "aspect_ratio": "9:16",
            "max_file_size_mb": 10,
        },
        "video_specs": {
            "max_duration_sec": 60,
            "recommended_resolution": "1080x1920",
            "max_file_size_mb": 500,
        },
        "text_limits": {"ad_text": 100, "display_name": 40},
        "source": "fallback_specs",
    },
}


def scrape_platform_ad_specs(platform: str) -> dict[str, Any]:
    """Scrape current ad specifications for social media platforms.

    Uses Firecrawl scrape with JSON extraction to pull structured ad spec
    data from official platform documentation. Falls back to hardcoded
    specs on failure.

    Args:
        platform: Platform identifier ("facebook", "linkedin", "tiktok").

    Returns:
        Dict with keys: platform, formats, image_specs, video_specs,
        text_limits, source, last_updated.
    """
    platform_key = (platform or "").lower().strip()
    fallback = _FALLBACK_AD_SPECS.get(platform_key) or {
        "platform": platform_key.title(),
        "formats": [],
        "image_specs": {},
        "video_specs": {},
        "text_limits": {},
        "source": "fallback_specs",
    }

    ckey = _cache_key("scrape_platform_ad_specs", platform_key)
    cached = _cache_get(ckey, TTL_AD_SPECS)
    if cached is not None:
        logger.debug(f"Firecrawl ad specs cache hit: {platform_key}")
        return cached

    url = _AD_SPEC_URLS.get(platform_key)
    if not url:
        logger.warning(f"Unknown ad platform: {platform_key}, using fallback specs")
        fallback["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return fallback

    scrape_payload: dict[str, Any] = {
        "url": url,
        "formats": ["json"],
        "jsonOptions": {
            "prompt": (
                "Extract advertising specifications including: "
                "available ad formats (image, video, carousel, stories, etc.), "
                "image size requirements (recommended dimensions, aspect ratios, max file size), "
                "video specifications (max duration, resolution, max file size), "
                "text/character limits for headlines, descriptions, and primary text."
            ),
            "schema": {
                "type": "object",
                "properties": {
                    "formats": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Available ad format types",
                    },
                    "image_width": {
                        "type": "integer",
                        "description": "Recommended image width in pixels",
                    },
                    "image_height": {
                        "type": "integer",
                        "description": "Recommended image height in pixels",
                    },
                    "image_aspect_ratio": {
                        "type": "string",
                        "description": "Recommended aspect ratio",
                    },
                    "image_max_size_mb": {
                        "type": "number",
                        "description": "Max image file size in MB",
                    },
                    "video_max_duration_sec": {
                        "type": "integer",
                        "description": "Max video duration in seconds",
                    },
                    "video_resolution": {
                        "type": "string",
                        "description": "Recommended video resolution",
                    },
                    "video_max_size_mb": {
                        "type": "number",
                        "description": "Max video file size in MB",
                    },
                    "headline_limit": {
                        "type": "integer",
                        "description": "Max headline characters",
                    },
                    "description_limit": {
                        "type": "integer",
                        "description": "Max description characters",
                    },
                    "primary_text_limit": {
                        "type": "integer",
                        "description": "Max primary text characters",
                    },
                },
            },
        },
        "onlyMainContent": True,
    }

    try:
        response = _firecrawl_request("/scrape", scrape_payload)
        if not response or not response.get("success"):
            logger.warning(
                f"Firecrawl ad specs scrape failed for {platform_key}, using fallback"
            )
            fallback["last_updated"] = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            )
            return fallback

        extracted = response.get("data", {}).get("json") or {}
        fb = fallback  # shorthand

        img_w = extracted.get("image_width") or 0
        img_h = extracted.get("image_height") or 0
        recommended_size = (
            f"{img_w}x{img_h}"
            if img_w and img_h
            else fb.get("image_specs", {}).get("recommended_size") or ""
        )

        result: dict[str, Any] = {
            "platform": fb.get("platform") or platform_key.title(),
            "formats": extracted.get("formats") or fb.get("formats") or [],
            "image_specs": {
                "recommended_size": recommended_size,
                "aspect_ratio": extracted.get("image_aspect_ratio")
                or fb.get("image_specs", {}).get("aspect_ratio")
                or "",
                "max_file_size_mb": extracted.get("image_max_size_mb")
                or fb.get("image_specs", {}).get("max_file_size_mb")
                or 0,
            },
            "video_specs": {
                "max_duration_sec": extracted.get("video_max_duration_sec")
                or fb.get("video_specs", {}).get("max_duration_sec")
                or 0,
                "recommended_resolution": extracted.get("video_resolution")
                or fb.get("video_specs", {}).get("recommended_resolution")
                or "",
                "max_file_size_mb": extracted.get("video_max_size_mb")
                or fb.get("video_specs", {}).get("max_file_size_mb")
                or 0,
            },
            "text_limits": {
                "headline": extracted.get("headline_limit")
                or fb.get("text_limits", {}).get("headline")
                or 0,
                "description": extracted.get("description_limit")
                or fb.get("text_limits", {}).get("description")
                or 0,
                "primary_text": extracted.get("primary_text_limit")
                or fb.get("text_limits", {}).get("primary_text")
                or 0,
            },
            "source": "firecrawl_live",
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        _cache_set(ckey, result, TTL_AD_SPECS)
        logger.info(f"Firecrawl scraped ad specs for {platform_key} successfully")
        return result

    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"Error parsing ad specs for {platform_key}: {exc}", exc_info=True)
        fallback["last_updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return fallback


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY: Status & Cache Management
# ═══════════════════════════════════════════════════════════════════════════════


def get_firecrawl_status() -> dict[str, Any]:
    """Return the configuration status of the Firecrawl integration.

    Returns:
        Dict with: configured (bool), has_api_key (bool), base_url, cache_dir,
        cache_files_count.
    """
    cache_count = 0
    try:
        cache_count = len(list(CACHE_DIR.glob("*.json")))
    except OSError:
        pass

    return {
        "configured": bool(FIRECRAWL_API_KEY),
        "has_api_key": bool(FIRECRAWL_API_KEY),
        "base_url": FIRECRAWL_BASE_URL,
        "cache_dir": str(CACHE_DIR),
        "cache_files_count": cache_count,
    }


def clear_firecrawl_cache() -> dict[str, Any]:
    """Remove all cached Firecrawl data files.

    Returns:
        Dict with: cleared (int), errors (int).
    """
    cleared = 0
    errors = 0
    try:
        for f in CACHE_DIR.glob("*.json"):
            try:
                f.unlink()
                cleared += 1
            except OSError:
                errors += 1
    except OSError as exc:
        logger.error(f"Cache clear error: {exc}", exc_info=True)
    return {"cleared": cleared, "errors": errors}


def _domain_from_url(url: str) -> str:
    """Extract a readable domain name from a URL."""
    if not url:
        return "Unknown"
    try:
        # Simple extraction without urllib.parse to keep it lightweight
        domain = url.split("://")[-1].split("/")[0]
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except (IndexError, AttributeError):
        return "Unknown"


# Run cache cleanup on import (non-blocking)
_cleanup_cache()
