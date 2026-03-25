#!/usr/bin/env python3
"""Tavily AI Search integration for Nova AI Suite with multi-tier fallback.

Fallback tiers:
    Tier 1: Tavily AI Search (primary, AI-optimized results)
    Tier 2: Jina AI Search (free, no API key needed)
    Tier 3: DuckDuckGo HTML search (no API key, parse results from HTML)
    Tier 4: Return empty results with a note (let LLM answer from training data)

Uses stdlib urllib (no SDK needed).

API: POST https://api.tavily.com/search
Env var: TAVILY_API_KEY (sign up free at https://app.tavily.com -- 1,000 credits/month)

All functions:
    - Return None on failure (never raise)
    - Cache results for 30 minutes
    - Log errors with exc_info=True
    - Use type hints on all signatures
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
_TAVILY_API_URL = "https://api.tavily.com/search"
_TAVILY_TIMEOUT = 8  # seconds (shortened to prevent tool timeout)
_TAVILY_API_KEY: str | None = None

# Rate limiting for Tavily (1000 credits/month ~ 33/day ~ 2/hour conservative)
_TAVILY_RPM_LIMIT = 15  # max requests per minute
_tavily_request_times: list[float] = []
_tavily_rate_lock = threading.Lock()

# Rate limiting for Jina (fallback)
_JINA_RPM_LIMIT = 10
_jina_request_times: list[float] = []
_jina_rate_lock = threading.Lock()

# Rate limiting for DuckDuckGo (be respectful)
_DDG_RPM_LIMIT = 5
_ddg_request_times: list[float] = []
_ddg_rate_lock = threading.Lock()


def _is_rate_limited(
    request_times: list[float],
    lock: threading.Lock,
    rpm_limit: int,
) -> bool:
    """Check if a service is rate limited using sliding window.

    Args:
        request_times: Mutable list of request timestamps.
        lock: Threading lock for the list.
        rpm_limit: Max requests per minute.

    Returns:
        True if rate limited, False if OK to proceed.
    """
    now = time.monotonic()
    with lock:
        request_times[:] = [t for t in request_times if now - t < 60]
        return len(request_times) >= rpm_limit


def _record_request(request_times: list[float], lock: threading.Lock) -> None:
    """Record a request timestamp for rate limiting.

    Args:
        request_times: Mutable list of request timestamps.
        lock: Threading lock for the list.
    """
    with lock:
        request_times.append(time.monotonic())


def _get_api_key() -> str | None:
    """Load Tavily API key from environment (cached after first load)."""
    global _TAVILY_API_KEY
    if _TAVILY_API_KEY is None:
        _TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY") or ""
    return _TAVILY_API_KEY if _TAVILY_API_KEY else None


# ── In-memory cache with TTL ─────────────────────────────────────────────────
_cache: dict[str, tuple[Any, float]] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 1800.0  # 30 minutes


def _cache_key(prefix: str, *parts: str) -> str:
    """Generate a deterministic cache key from string parts."""
    raw = f"{prefix}:{'|'.join(str(p) for p in parts)}"
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()


def _cache_get(key: str) -> Any | None:
    """Return cached value if not expired, else None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        value, ts = entry
        if time.time() - ts > _CACHE_TTL:
            del _cache[key]
            return None
        return value


def _cache_set(key: str, value: Any) -> None:
    """Store value in cache with current timestamp."""
    with _cache_lock:
        _cache[key] = (value, time.time())


# ── Tier 1: Tavily API ──────────────────────────────────────────────────────


def _tavily_request(
    query: str,
    max_results: int = 5,
    search_depth: str = "basic",
    include_answer: bool = False,
    topic: str = "general",
) -> dict | None:
    """Make a raw Tavily API search request.

    Args:
        query: Search query string.
        max_results: Number of results (1-20).
        search_depth: "basic" (faster) or "advanced" (deeper).
        include_answer: Whether to include AI-generated answer.
        topic: "general" or "news".

    Returns:
        Raw API response dict, or None on failure.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.warning(
            "TAVILY_API_KEY not set. Sign up at https://app.tavily.com "
            "and set TAVILY_API_KEY environment variable."
        )
        return None

    if _is_rate_limited(_tavily_request_times, _tavily_rate_lock, _TAVILY_RPM_LIMIT):
        logger.warning("Tavily rate limited (%d req/min), skipping", _TAVILY_RPM_LIMIT)
        return None

    payload = {
        "api_key": api_key,
        "query": query,
        "max_results": min(max_results, 20),
        "search_depth": search_depth,
        "include_answer": include_answer,
        "topic": topic,
    }

    try:
        _record_request(_tavily_request_times, _tavily_rate_lock)
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            _TAVILY_API_URL,
            data=data,
            headers={"Content-Type": "application/json", "Connection": "keep-alive"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=_TAVILY_TIMEOUT) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        logger.error(
            "Tavily API HTTP error %d for query=%s: %s",
            e.code,
            query[:50],
            e.reason,
            exc_info=True,
        )
        return None
    except urllib.error.URLError as e:
        logger.error(
            "Tavily API URL error for query=%s: %s",
            query[:50],
            e.reason,
            exc_info=True,
        )
        return None
    except (json.JSONDecodeError, OSError, ValueError, TypeError) as e:
        logger.error("Tavily API error for query=%s: %s", query[:50], e, exc_info=True)
        return None


# ── Tier 2: Jina AI Search (free, no API key) ───────────────────────────────

_JINA_SEARCH_URL = "https://s.jina.ai/"


def _jina_search(query: str, max_results: int = 5) -> list[dict] | None:
    """Search using Jina AI Search API (free tier, no key required).

    Args:
        query: Search query string.
        max_results: Max results to return.

    Returns:
        List of result dicts or None on failure.
    """
    if _is_rate_limited(_jina_request_times, _jina_rate_lock, _JINA_RPM_LIMIT):
        logger.warning("Jina AI rate limited, skipping")
        return None

    try:
        _record_request(_jina_request_times, _jina_rate_lock)
        encoded_query = urllib.parse.quote(query)
        url = f"{_JINA_SEARCH_URL}{encoded_query}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "X-Return-Format": "text",
                "Connection": "keep-alive",
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)

        results: list[dict] = []
        items = data.get("data") or []
        for item in items[:max_results]:
            results.append(
                {
                    "title": item.get("title") or "",
                    "url": item.get("url") or "",
                    "content": (item.get("description") or item.get("content") or "")[
                        :500
                    ],
                    "score": 0.5,  # No relevance score from Jina
                }
            )

        if results:
            logger.info(
                "Jina AI search returned %d results for query=%s",
                len(results),
                query[:50],
            )
        return results if results else None

    except urllib.error.HTTPError as e:
        logger.error(
            "Jina AI HTTP error %d for query=%s", e.code, query[:50], exc_info=True
        )
        return None
    except urllib.error.URLError as e:
        logger.error(
            "Jina AI URL error for query=%s: %s", query[:50], e.reason, exc_info=True
        )
        return None
    except (json.JSONDecodeError, OSError, ValueError, TypeError) as e:
        logger.error("Jina AI error for query=%s: %s", query[:50], e, exc_info=True)
        return None


# ── Tier 3: DuckDuckGo HTML Search (no API key) ─────────────────────────────

_DDG_HTML_URL = "https://html.duckduckgo.com/html/"


def _ddg_search(query: str, max_results: int = 5) -> list[dict] | None:
    """Search using DuckDuckGo HTML endpoint (no API key needed).

    Parses search results from the HTML response. This is a last-resort
    fallback when both Tavily and Jina are unavailable.

    Args:
        query: Search query string.
        max_results: Max results to return.

    Returns:
        List of result dicts or None on failure.
    """
    if _is_rate_limited(_ddg_request_times, _ddg_rate_lock, _DDG_RPM_LIMIT):
        logger.warning("DuckDuckGo rate limited, skipping")
        return None

    try:
        _record_request(_ddg_request_times, _ddg_rate_lock)
        form_data = urllib.parse.urlencode({"q": query}).encode("utf-8")
        req = urllib.request.Request(
            _DDG_HTML_URL,
            data=form_data,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; NovaAISuite/1.0)",
                "Content-Type": "application/x-www-form-urlencoded",
                "Connection": "keep-alive",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="replace")

        # Parse results from HTML using regex (no lxml/bs4 needed)
        results: list[dict] = []

        # DuckDuckGo HTML results are in <a class="result__a" href="...">title</a>
        # followed by <a class="result__snippet">snippet</a>
        # Pattern: find result blocks
        result_blocks = re.findall(
            r'<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>(.*?)</a>'
            r'.*?<a[^>]*class="result__snippet"[^>]*>(.*?)</a>',
            body,
            re.DOTALL,
        )

        for url_raw, title_raw, snippet_raw in result_blocks[:max_results]:
            # Clean HTML entities and tags
            title = re.sub(r"<[^>]+>", "", html.unescape(title_raw)).strip()
            snippet = re.sub(r"<[^>]+>", "", html.unescape(snippet_raw)).strip()
            url = html.unescape(url_raw).strip()

            # DuckDuckGo wraps URLs in a redirect; extract actual URL
            if "uddg=" in url:
                parsed = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
                actual_urls = parsed.get("uddg", [])
                url = actual_urls[0] if actual_urls else url

            if title and url:
                results.append(
                    {
                        "title": title,
                        "url": url,
                        "content": snippet[:500],
                        "score": 0.3,  # Low confidence for HTML-parsed results
                    }
                )

        if results:
            logger.info(
                "DuckDuckGo search returned %d results for query=%s",
                len(results),
                query[:50],
            )
        return results if results else None

    except urllib.error.HTTPError as e:
        logger.error(
            "DuckDuckGo HTTP error %d for query=%s", e.code, query[:50], exc_info=True
        )
        return None
    except urllib.error.URLError as e:
        logger.error(
            "DuckDuckGo URL error for query=%s: %s", query[:50], e.reason, exc_info=True
        )
        return None
    except (OSError, ValueError, TypeError) as e:
        logger.error("DuckDuckGo error for query=%s: %s", query[:50], e, exc_info=True)
        return None


# ── Multi-tier search with fallback ──────────────────────────────────────────


def _search_with_fallback(
    query: str,
    max_results: int = 5,
    search_depth: str = "basic",
    topic: str = "general",
) -> list[dict] | None:
    """Execute search with automatic fallback across tiers.

    Tier 1: Tavily -> Tier 2: Jina AI -> Tier 3: DuckDuckGo -> Tier 4: None

    Args:
        query: Search query.
        max_results: Number of results.
        search_depth: Tavily search depth.
        topic: Tavily topic filter.

    Returns:
        List of result dicts, or None if all tiers fail.
    """
    # Tier 1: Tavily
    raw = _tavily_request(
        query=query,
        max_results=max_results,
        search_depth=search_depth,
        topic=topic,
    )
    if raw is not None:
        results: list[dict] = []
        for item in raw.get("results") or []:
            results.append(
                {
                    "title": item.get("title") or "",
                    "url": item.get("url") or "",
                    "content": item.get("content") or "",
                    "score": item.get("score") or 0.0,
                }
            )
        if results:
            return results

    # Tier 2: Jina AI
    logger.info("Tavily unavailable for query=%s, trying Jina AI", query[:50])
    jina_results = _jina_search(query, max_results=max_results)
    if jina_results:
        return jina_results

    # Tier 3: DuckDuckGo
    logger.info("Jina AI unavailable for query=%s, trying DuckDuckGo", query[:50])
    ddg_results = _ddg_search(query, max_results=max_results)
    if ddg_results:
        return ddg_results

    # Tier 4: All search providers failed
    logger.warning(
        "All search tiers failed for query=%s (Tavily, Jina, DuckDuckGo)",
        query[:50],
    )
    return None


# ── Public API ───────────────────────────────────────────────────────────────


def search(
    query: str,
    max_results: int = 5,
    search_depth: str = "basic",
) -> list[dict] | None:
    """Search the web using multi-tier fallback (Tavily -> Jina -> DDG).

    Args:
        query: Search query string.
        max_results: Number of results (1-20).
        search_depth: "basic" (faster) or "advanced" (deeper).

    Returns:
        List of dicts with keys: title, url, content, score.
        Returns None on failure.
    """
    ck = _cache_key("search", query, str(max_results), search_depth)
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    results = _search_with_fallback(
        query=query,
        max_results=max_results,
        search_depth=search_depth,
    )

    if results:
        _cache_set(ck, results)
        logger.info(
            "Web search returned %d results for query=%s", len(results), query[:50]
        )

    return results


def search_recruitment_news(topic: str) -> list[dict] | None:
    """Search for recruitment industry news on a topic.

    Pre-configured with recruitment-specific query enhancement and
    uses the 'news' topic for recency-biased results.

    Args:
        topic: News topic (e.g. "remote work trends", "AI recruiting").

    Returns:
        List of dicts with keys: title, url, content, score.
        Returns None on failure.
    """
    enhanced_query = f"recruitment hiring {topic} latest news 2026"

    ck = _cache_key("recruitment_news", topic)
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    results = _search_with_fallback(
        query=enhanced_query,
        max_results=10,
        search_depth="advanced",
        topic="news",
    )

    if results:
        _cache_set(ck, results)
        logger.info(
            "Recruitment news search returned %d results for topic=%s",
            len(results),
            topic[:50],
        )

    return results


def research_company(company_name: str) -> dict | None:
    """Deep search about a company's hiring activity, culture, and reviews.

    Performs multiple targeted searches and aggregates the results.

    Args:
        company_name: Company name (e.g. "Google", "Deloitte").

    Returns:
        Dict with keys: company, hiring_info, culture_info, reviews,
        recent_news. Returns None on failure.
    """
    ck = _cache_key("company_research", company_name)
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    # Run targeted searches
    hiring_results = search(
        f"{company_name} hiring jobs open positions 2026",
        max_results=5,
        search_depth="advanced",
    )
    culture_results = search(
        f"{company_name} company culture employee reviews glassdoor",
        max_results=5,
        search_depth="advanced",
    )
    news_results = search(
        f"{company_name} company news layoffs growth recent",
        max_results=5,
        search_depth="basic",
    )

    if hiring_results is None and culture_results is None and news_results is None:
        return None

    result = {
        "company": company_name,
        "hiring_info": hiring_results or [],
        "culture_info": culture_results or [],
        "recent_news": news_results or [],
        "total_sources": (
            len(hiring_results or [])
            + len(culture_results or [])
            + len(news_results or [])
        ),
    }

    _cache_set(ck, result)
    logger.info(
        "Company research for %s: %d total sources",
        company_name,
        result["total_sources"],
    )
    return result


# ── Status ───────────────────────────────────────────────────────────────────


def get_status() -> dict:
    """Return status dict for health/diagnostics endpoints."""
    has_key = bool(_get_api_key())
    return {
        "tavily_configured": has_key,
        "cache_entries": len(_cache),
        "api_url": _TAVILY_API_URL,
        "fallback_tiers": ["tavily", "jina_ai", "duckduckgo"],
    }
