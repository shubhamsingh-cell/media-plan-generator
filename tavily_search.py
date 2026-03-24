#!/usr/bin/env python3
"""Tavily AI Search integration for Nova AI Suite.

Uses stdlib urllib (no SDK needed) to call the Tavily Search API.
Provides AI-optimized web search with clean, structured results designed
for LLM consumption.

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
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
_TAVILY_API_URL = "https://api.tavily.com/search"
_TAVILY_TIMEOUT = 15  # seconds
_TAVILY_API_KEY: str | None = None


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


# ── Core API call ────────────────────────────────────────────────────────────


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

    payload = {
        "api_key": api_key,
        "query": query,
        "max_results": min(max_results, 20),
        "search_depth": search_depth,
        "include_answer": include_answer,
        "topic": topic,
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            _TAVILY_API_URL,
            data=data,
            headers={"Content-Type": "application/json"},
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


# ── Public API ───────────────────────────────────────────────────────────────


def search(
    query: str,
    max_results: int = 5,
    search_depth: str = "basic",
) -> list[dict] | None:
    """Search the web using Tavily AI search.

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

    raw = _tavily_request(
        query=query,
        max_results=max_results,
        search_depth=search_depth,
    )
    if raw is None:
        return None

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

    _cache_set(ck, results)
    logger.info(
        "Tavily search returned %d results for query=%s", len(results), query[:50]
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

    raw = _tavily_request(
        query=enhanced_query,
        max_results=10,
        search_depth="advanced",
        topic="news",
    )
    if raw is None:
        return None

    results: list[dict] = []
    for item in raw.get("results") or []:
        results.append(
            {
                "title": item.get("title") or "",
                "url": item.get("url") or "",
                "content": item.get("content") or "",
                "score": item.get("score") or 0.0,
                "published_date": item.get("published_date") or "",
            }
        )

    _cache_set(ck, results)
    logger.info(
        "Tavily recruitment news returned %d results for topic=%s",
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
        "Tavily company research for %s: %d total sources",
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
    }
