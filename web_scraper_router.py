"""
web_scraper_router.py -- Multi-tier web scraping fallback system (v3).

Provides a unified interface for web scraping and search with automatic
fallback across 6 tiers.  When one tier fails (402 credit exhausted,
429 rate limited, or network error), the router transparently falls
through to the next available tier.

Tier 1:   Firecrawl      -- Full-featured scrape/search/map (API key required, paid)
Tier 1.5: Apify          -- Website Content Crawler actor (APIFY_API_TOKEN, cheerio)
Tier 2:   Jina AI Reader -- Free markdown reader (GET https://r.jina.ai/{url}, no key)
Tier 3:   Tavily Extract -- URL content extraction (TAVILY_API_KEY, 1K credits/month)
Tier 4:   LLM-assisted   -- stdlib fetch raw HTML -> LLM router extracts structured content
Tier 5:   Cache fallback  -- Google Cache + Internet Archive Wayback Machine
Tier 6:   stdlib urllib   -- Raw HTML fetch + HTMLParser text extraction (always works)

All external API calls:
    - Use only stdlib (urllib.request, json, os) -- no third-party dependencies
    - Have per-tier circuit breakers (5 failures -> 60s cooldown)
    - In-memory LRU cache (200 entries) + optional Upstash Redis L2 cache
    - Content quality scoring (rejects login walls, bot blocks, cookie pages)
    - Content freshness tracking (hash-based change detection for compliance)
    - Per-tier cost/usage tracking with estimated spend
    - Track request counts and success rates
    - Are thread-safe (locks on shared state)
    - Log which tier was used with timing
    - Return normalized output

Usage:
    from web_scraper_router import scrape_url, search_web, get_scraper_status
    result = scrape_url("https://example.com")
    results = search_web("recruitment advertising trends")
"""

from __future__ import annotations

import hashlib
import html.parser
import json
import logging
import os
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from typing import Any, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

FIRECRAWL_API_KEY: str = os.environ.get("FIRECRAWL_API_KEY") or ""
FIRECRAWL_BASE_URL: str = "https://api.firecrawl.dev/v1"
APIFY_API_TOKEN: str = os.environ.get("APIFY_API_TOKEN") or ""
JINA_API_KEY: str = os.environ.get("JINA_API_KEY") or ""
TAVILY_API_KEY: str = os.environ.get("TAVILY_API_KEY") or ""

REQUEST_TIMEOUT: int = (
    8  # seconds per external request (tightened for tool timeout safety)
)
LLM_SCRAPE_TIMEOUT: int = 30  # LLM calls can take longer
CACHE_SCRAPE_TIMEOUT: int = 10  # cache/archive lookups should be fast

# Circuit breaker tuning (matches llm_router.py pattern)
CB_FAILURE_THRESHOLD: int = 5  # failures before tripping
CB_COOLDOWN_SECONDS: int = 60  # seconds to disable after trip


# =============================================================================
# LRU IN-MEMORY CACHE (L1) + OPTIONAL UPSTASH REDIS (L2)
# =============================================================================

_LRU_MAX_SIZE: int = 200
_LRU_TTL: float = 1800.0  # 30 minutes

_lru_cache: OrderedDict[str, tuple[Any, float]] = OrderedDict()
_lru_lock = threading.Lock()

# Optional L2 Redis cache
try:
    from upstash_cache import cache_get as _redis_get, cache_set as _redis_set

    _redis_available = True
except ImportError:
    _redis_get = _redis_set = None  # type: ignore[assignment]
    _redis_available = False
    logger.info(
        "upstash_cache not available; L2 Redis cache disabled for web_scraper_router"
    )


def _cache_key(url_or_query: str, operation: str = "scrape") -> str:
    """Generate a deterministic cache key.

    Args:
        url_or_query: The URL or search query.
        operation: 'scrape' or 'search'.

    Returns:
        MD5 hex digest prefixed with operation type.
    """
    raw = f"wsr:{operation}:{url_or_query}"
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()


def _cache_get(key: str) -> Any | None:
    """Read from L1 (memory LRU) then L2 (Redis) cache.

    Args:
        key: Cache key string.

    Returns:
        Cached value or None if miss/expired.
    """
    # L1: in-memory LRU
    with _lru_lock:
        entry = _lru_cache.get(key)
        if entry is not None:
            value, ts = entry
            if time.time() - ts <= _LRU_TTL:
                _lru_cache.move_to_end(key)
                return value
            else:
                del _lru_cache[key]

    # L2: Upstash Redis
    if _redis_available and _redis_get is not None:
        try:
            val = _redis_get(f"wsr:{key}")
            if val is not None:
                # Promote to L1
                _cache_put(key, val, skip_redis=True)
                return val
        except Exception as exc:
            logger.error(f"Redis cache_get error for {key}: {exc}", exc_info=True)

    return None


def _cache_put(key: str, value: Any, skip_redis: bool = False) -> None:
    """Write to L1 (memory LRU) and optionally L2 (Redis).

    Args:
        key: Cache key string.
        value: Value to cache.
        skip_redis: If True, only write to L1 (used for L2->L1 promotion).
    """
    with _lru_lock:
        _lru_cache[key] = (value, time.time())
        _lru_cache.move_to_end(key)
        # Evict oldest if over capacity
        while len(_lru_cache) > _LRU_MAX_SIZE:
            _lru_cache.popitem(last=False)

    if not skip_redis and _redis_available and _redis_set is not None:
        try:
            _redis_set(f"wsr:{key}", value, ttl_seconds=3600, category="web_scraper")
        except Exception as exc:
            logger.error(f"Redis cache_set error for {key}: {exc}", exc_info=True)


# =============================================================================
# CIRCUIT BREAKER (thread-safe, per-tier, 5 failures -> 60s cooldown)
# =============================================================================


class CircuitBreaker:
    """Thread-safe circuit breaker for a single scraping tier.

    Trips after CB_FAILURE_THRESHOLD consecutive failures, disabling the tier
    for CB_COOLDOWN_SECONDS.  Matches the llm_router.py pattern.
    """

    def __init__(
        self,
        name: str,
        cooldown: int = CB_COOLDOWN_SECONDS,
        threshold: int = CB_FAILURE_THRESHOLD,
    ) -> None:
        """Initialize circuit breaker for a named tier.

        Args:
            name: Human-readable tier name (e.g., 'firecrawl').
            cooldown: Seconds to disable after tripping.
            threshold: Consecutive failures before tripping.
        """
        self.name = name
        self.cooldown = cooldown
        self.threshold = threshold
        self._consecutive_failures: int = 0
        self._disabled_until: float = 0.0
        self._total_requests: int = 0
        self._successful_requests: int = 0
        self._failed_requests: int = 0
        self._last_error: str = ""
        self._last_error_time: float = 0.0
        self._lock = threading.Lock()

    @property
    def is_available(self) -> bool:
        """Check if this tier is currently available (not tripped)."""
        with self._lock:
            if self._disabled_until <= 0:
                return True
            if time.time() >= self._disabled_until:
                self._disabled_until = 0.0
                self._consecutive_failures = 0
                return True
            return False

    @property
    def remaining_cooldown(self) -> int:
        """Seconds remaining in cooldown, or 0 if available."""
        with self._lock:
            if self._disabled_until <= 0:
                return 0
            remaining = self._disabled_until - time.time()
            return max(0, int(remaining))

    def trip(self, reason: str = "") -> None:
        """Force-trip the circuit breaker (e.g., on 402/429).

        Args:
            reason: Human-readable reason for the trip.
        """
        with self._lock:
            self._disabled_until = time.time() + self.cooldown
            self._last_error = reason
            self._last_error_time = time.time()
            self._total_requests += 1
            self._failed_requests += 1
            self._consecutive_failures = self.threshold  # mark as fully tripped
        logger.warning(
            f"Circuit breaker TRIPPED for {self.name}: {reason}. "
            f"Disabled for {self.cooldown}s."
        )

    def record_success(self) -> None:
        """Record a successful request and reset consecutive failure count."""
        with self._lock:
            self._total_requests += 1
            self._successful_requests += 1
            self._consecutive_failures = 0

    def record_failure(self, reason: str = "") -> None:
        """Record a failed request. Trips breaker after threshold consecutive failures.

        Args:
            reason: Description of the failure.
        """
        with self._lock:
            self._total_requests += 1
            self._failed_requests += 1
            self._last_error = reason
            self._last_error_time = time.time()
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.threshold:
                self._disabled_until = time.time() + self.cooldown
                logger.warning(
                    f"Circuit breaker auto-tripped for {self.name} after "
                    f"{self.threshold} consecutive failures: {reason}. "
                    f"Disabled for {self.cooldown}s."
                )

    def get_stats(self) -> dict[str, Any]:
        """Return monitoring stats for this circuit breaker."""
        with self._lock:
            success_rate = 0.0
            if self._total_requests > 0:
                success_rate = round(
                    self._successful_requests / self._total_requests * 100, 1
                )
            now = time.time()
            if self._disabled_until <= 0 or now >= self._disabled_until:
                available = True
                cooldown_remaining = 0
            else:
                available = False
                cooldown_remaining = max(0, int(self._disabled_until - now))
            return {
                "name": self.name,
                "available": available,
                "remaining_cooldown_seconds": cooldown_remaining,
                "total_requests": self._total_requests,
                "successful_requests": self._successful_requests,
                "failed_requests": self._failed_requests,
                "success_rate_pct": success_rate,
                "consecutive_failures": self._consecutive_failures,
                "last_error": self._last_error,
                "last_error_time": (
                    time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ",
                        time.gmtime(self._last_error_time),
                    )
                    if self._last_error_time > 0
                    else ""
                ),
            }

    def reset(self) -> None:
        """Reset the circuit breaker (for testing/admin)."""
        with self._lock:
            self._disabled_until = 0.0
            self._consecutive_failures = 0
            self._total_requests = 0
            self._successful_requests = 0
            self._failed_requests = 0
            self._last_error = ""
            self._last_error_time = 0.0


# Per-tier circuit breakers (module-level singletons)
_cb_firecrawl = CircuitBreaker("firecrawl")
_cb_apify = CircuitBreaker("apify")
_cb_jina = CircuitBreaker("jina")
_cb_tavily = CircuitBreaker("tavily")
_cb_llm_assist = CircuitBreaker("llm_assisted")
_cb_cache_fallback = CircuitBreaker("cache_fallback")
_cb_urllib = CircuitBreaker("urllib_direct")


# =============================================================================
# CONTENT QUALITY SCORING (detects login walls, bot blocks, empty pages)
# =============================================================================


def _score_content_quality(content: str, url: str = "") -> float:
    """Score scraped content quality 0.0-1.0.

    Detects: login walls, cookie consent pages, bot detection, empty pages.

    Args:
        content: The scraped text content.
        url: The source URL (for context, currently unused).

    Returns:
        Quality score between 0.0 (garbage) and 1.0 (good content).
    """
    if not content:
        return 0.0

    score = 1.0
    lower = content.lower()
    content_len = len(content.strip())

    # Binary/non-text content detection (null bytes, high ratio of control chars)
    _sample = content[:1000]
    if "\x00" in _sample:
        return 0.05  # Null bytes = binary data
    _control_chars = sum(1 for c in _sample if ord(c) < 32 and c not in "\n\r\t")
    if _control_chars > len(_sample) * 0.1:
        return 0.05  # High control char ratio = binary data

    # Too short = likely blocked or empty
    if content_len < 50:
        return 0.1
    if content_len < 200:
        score *= 0.4

    # Login wall detection
    login_signals = [
        "sign in",
        "log in",
        "create account",
        "access denied",
        "403 forbidden",
        "please verify",
        "captcha",
        "are you a robot",
        "cloudflare",
    ]
    login_matches = sum(1 for s in login_signals if s in lower)
    if login_matches >= 2:
        score *= 0.2  # Likely a login wall

    # Cookie consent page (mostly cookie text, not real content)
    cookie_signals = [
        "cookie policy",
        "we use cookies",
        "cookie consent",
        "accept cookies",
        "privacy policy",
        "gdpr",
    ]
    cookie_matches = sum(1 for s in cookie_signals if s in lower)
    if cookie_matches >= 2 and content_len < 1000:
        score *= 0.3

    # Bot detection
    bot_signals = [
        "enable javascript",
        "browser not supported",
        "please enable",
        "ray id",
        "checking your browser",
    ]
    if any(s in lower for s in bot_signals):
        score *= 0.2

    return round(min(score, 1.0), 2)


# =============================================================================
# CONTENT FRESHNESS TRACKING (hash-based change detection)
# =============================================================================


class ContentFreshnessTracker:
    """Tracks content freshness by storing hashes of scraped content.

    Thread-safe. Stores SHA-256 prefix hashes to detect when page content
    changes between scrapes. Useful for compliance monitoring.
    """

    def __init__(self) -> None:
        """Initialize with empty hash store."""
        self._hashes: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def record(self, url: str, content: str) -> dict[str, Any]:
        """Record content and detect changes.

        Args:
            url: The URL that was scraped.
            content: The scraped content text.

        Returns:
            Freshness metadata dict with keys: freshness, change_count,
            and optionally days_since_change.
        """
        content_hash = hashlib.sha256(content.encode()[:10000]).hexdigest()[:16]
        now = time.time()

        with self._lock:
            if url in self._hashes:
                existing = self._hashes[url]
                changed = existing["hash"] != content_hash
                if changed:
                    existing["hash"] = content_hash
                    existing["change_count"] += 1
                    existing["last_changed"] = now
                existing["last_seen"] = now
                return {
                    "freshness": "changed" if changed else "unchanged",
                    "change_count": existing["change_count"],
                    "days_since_change": round(
                        (now - existing.get("last_changed", now)) / 86400, 1
                    ),
                }
            else:
                self._hashes[url] = {
                    "hash": content_hash,
                    "first_seen": now,
                    "last_seen": now,
                    "last_changed": now,
                    "change_count": 0,
                }
                return {"freshness": "new", "change_count": 0}

    def get_stats(self) -> dict[str, Any]:
        """Return summary stats for tracked URLs.

        Returns:
            Dict with tracked_urls count and recently_changed count.
        """
        with self._lock:
            return {
                "tracked_urls": len(self._hashes),
                "recently_changed": sum(
                    1
                    for v in self._hashes.values()
                    if time.time() - v.get("last_changed", 0) < 86400
                ),
            }


_freshness_tracker = ContentFreshnessTracker()


# =============================================================================
# PER-TIER COST / USAGE TRACKING
# =============================================================================


class TierUsageTracker:
    """Tracks usage and estimated cost per scraping tier.

    Thread-safe. Records call counts, success/failure rates, and approximate
    cost per tier based on known pricing.
    """

    # Approximate per-call costs in USD
    TIER_COSTS: dict[str, float] = {
        "firecrawl": 0.001,
        "apify": 0.002,
        "jina": 0.0005,
        "tavily": 0.001,
        "llm_assisted": 0.01,
        "cache_fallback": 0.0,
        "stdlib": 0.0,
    }

    def __init__(self) -> None:
        """Initialize with empty usage counters."""
        self._usage: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def record(self, tier_name: str, success: bool) -> None:
        """Record a tier usage event.

        Args:
            tier_name: Name of the tier (e.g. 'firecrawl', 'jina').
            success: Whether the call succeeded.
        """
        with self._lock:
            if tier_name not in self._usage:
                self._usage[tier_name] = {
                    "calls": 0,
                    "successes": 0,
                    "failures": 0,
                    "est_cost_usd": 0.0,
                }
            entry = self._usage[tier_name]
            entry["calls"] += 1
            if success:
                entry["successes"] += 1
            else:
                entry["failures"] += 1
            entry["est_cost_usd"] += self.TIER_COSTS.get(tier_name, 0)

    def get_report(self) -> dict[str, Any]:
        """Return a usage report across all tiers.

        Returns:
            Dict with total_est_cost_usd and by_tier breakdown.
        """
        with self._lock:
            total_cost = sum(v["est_cost_usd"] for v in self._usage.values())
            return {
                "total_est_cost_usd": round(total_cost, 4),
                "by_tier": {k: dict(v) for k, v in self._usage.items()},
            }


_tier_usage = TierUsageTracker()


# =============================================================================
# SSL CONTEXT
# =============================================================================


def _build_ssl_context() -> ssl.SSLContext:
    """Build SSL context for urllib requests."""
    return ssl.create_default_context()


# =============================================================================
# NORMALIZED RESULT HELPERS
# =============================================================================


def _scrape_result(
    content: str,
    url: str,
    provider: str,
    title: str = "",
    metadata: Optional[dict[str, Any]] = None,
    latency_ms: float = 0.0,
    error: str = "",
) -> dict[str, Any]:
    """Build a normalized scrape result dict.

    Args:
        content: Extracted text/markdown content.
        url: The URL that was scraped.
        provider: Name of the provider tier that succeeded.
        title: Page title if available.
        metadata: Any additional metadata from the provider.
        latency_ms: Time taken in milliseconds.
        error: Error message if scrape failed (e.g., security validation).

    Returns:
        Normalized result dict.
    """
    result = {
        "content": content or "",
        "url": url,
        "provider": provider,
        "title": title or "",
        "metadata": metadata or {},
        "latency_ms": round(latency_ms, 1),
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if error:
        result["error"] = error
    return result


def _search_result(
    title: str,
    url: str,
    snippet: str,
    provider: str,
) -> dict[str, Any]:
    """Build a normalized search result dict.

    Args:
        title: Result title.
        url: Result URL.
        snippet: Content snippet/description.
        provider: Name of the provider tier.

    Returns:
        Normalized result dict.
    """
    return {
        "title": title or "",
        "url": url or "",
        "snippet": snippet or "",
        "provider": provider,
    }


# =============================================================================
# TIER 1: FIRECRAWL (paid, full-featured scrape/search/map)
# =============================================================================


def _firecrawl_scrape(url: str) -> Optional[dict[str, Any]]:
    """Scrape a URL using Firecrawl's /scrape endpoint.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not FIRECRAWL_API_KEY:
        return None
    if not _cb_firecrawl.is_available:
        return None

    t0 = time.monotonic()
    payload = {
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
    }
    req = urllib.request.Request(
        f"{FIRECRAWL_BASE_URL}/scrape",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if not data.get("success"):
            _cb_firecrawl.record_failure("API returned success=false")
            return None

        resp_data = data.get("data") or {}
        content = resp_data.get("markdown") or ""
        title = resp_data.get("metadata", {}).get("title") or ""
        _cb_firecrawl.record_success()
        elapsed = (time.monotonic() - t0) * 1000
        return _scrape_result(content, url, "firecrawl", title, latency_ms=elapsed)

    except urllib.error.HTTPError as exc:
        if exc.code in (402, 429):
            _cb_firecrawl.trip(f"HTTP {exc.code}: {exc.reason}")
            logger.warning(
                f"Firecrawl scrape HTTP {exc.code} for {url}: {exc.reason} (falling through to backup tiers)",
            )
        else:
            _cb_firecrawl.record_failure(f"HTTP {exc.code}: {exc.reason}")
            logger.error(
                f"Firecrawl scrape HTTP {exc.code} for {url}: {exc.reason}",
                exc_info=True,
            )
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        _cb_firecrawl.record_failure(str(exc))
        logger.error(f"Firecrawl scrape error for {url}: {exc}", exc_info=True)
    return None


def _firecrawl_search(
    query: str, num_results: int = 5
) -> Optional[list[dict[str, Any]]]:
    """Search using Firecrawl's /search endpoint.

    Args:
        query: Search query string.
        num_results: Max number of results to return.

    Returns:
        List of normalized search results or None on failure.
    """
    if not FIRECRAWL_API_KEY:
        return None
    if not _cb_firecrawl.is_available:
        return None

    payload = {
        "query": query,
        "limit": num_results,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
    }
    req = urllib.request.Request(
        f"{FIRECRAWL_BASE_URL}/search",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if not data.get("success"):
            _cb_firecrawl.record_failure("Search returned success=false")
            return None

        raw_results = data.get("data") or []
        results: list[dict[str, Any]] = []
        for item in raw_results[:num_results]:
            meta = item.get("metadata") or {}
            results.append(
                _search_result(
                    title=meta.get("title") or "",
                    url=item.get("url") or meta.get("sourceURL") or "",
                    snippet=meta.get("description") or item.get("markdown", "")[:300],
                    provider="firecrawl",
                )
            )
        _cb_firecrawl.record_success()
        return results

    except urllib.error.HTTPError as exc:
        if exc.code in (402, 429):
            _cb_firecrawl.trip(f"HTTP {exc.code}: {exc.reason}")
            logger.warning(
                f"Firecrawl search HTTP {exc.code}: {exc.reason} (falling through to backup tiers)",
            )
        else:
            _cb_firecrawl.record_failure(f"HTTP {exc.code}: {exc.reason}")
            logger.error(
                f"Firecrawl search HTTP {exc.code}: {exc.reason}", exc_info=True
            )
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        _cb_firecrawl.record_failure(str(exc))
        logger.error(f"Firecrawl search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 1.5: APIFY WEBSITE CONTENT CRAWLER (API key required)
# =============================================================================

_APIFY_SCRAPE_URL: str = (
    "https://api.apify.com/v2/acts/apify~website-content-crawler/"
    "run-sync-get-dataset-items"
)
_APIFY_TIMEOUT: int = 30  # Apify sync runs can take a while


def _apify_scrape(url: str) -> Optional[dict[str, Any]]:
    """Scrape a URL using Apify's Website Content Crawler actor (sync).

    Uses the run-sync-get-dataset-items endpoint which runs the actor and
    returns the dataset items in a single call. Only activated if
    APIFY_API_TOKEN env var is set.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result dict, or None on failure.
    """
    if not APIFY_API_TOKEN:
        return None

    if not _cb_apify.is_available:
        logger.debug("Apify circuit breaker is open, skipping")
        return None

    t0 = time.monotonic()
    try:
        payload = json.dumps(
            {
                "startUrls": [{"url": url}],
                "maxCrawlPages": 1,
                "crawlerType": "cheerio",
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            _APIFY_SCRAPE_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {APIFY_API_TOKEN}",
            },
            method="POST",
        )

        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=_APIFY_TIMEOUT, context=ctx) as resp:
            body = resp.read().decode("utf-8")
            items = json.loads(body)

        elapsed = (time.monotonic() - t0) * 1000

        if not items or not isinstance(items, list):
            _cb_apify.record_failure("Empty response from Apify")
            return None

        # Apify returns array of items; take the first
        item = items[0]
        # The actor returns content in 'markdown' or 'text' fields
        content = item.get("markdown") or item.get("text") or item.get("body") or ""
        title = item.get("title") or item.get("metadata", {}).get("title") or ""

        if not content.strip():
            _cb_apify.record_failure("Apify returned empty content")
            return None

        _cb_apify.record_success()
        return _scrape_result(
            content=content,
            url=url,
            provider="apify",
            title=title,
            metadata={"actor": "website-content-crawler", "crawler_type": "cheerio"},
            latency_ms=elapsed,
        )

    except urllib.error.HTTPError as exc:
        elapsed = (time.monotonic() - t0) * 1000
        if exc.code in (402, 429):
            _cb_apify.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_apify.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.warning(
            f"Apify scrape failed for {url}: HTTP {exc.code} ({elapsed:.0f}ms)"
        )
        return None
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        _cb_apify.record_failure(str(exc))
        logger.error(f"Apify scrape error for {url}: {exc}", exc_info=True)
        return None


# =============================================================================
# TIER 2: JINA AI READER (free, no API key for basic)
# =============================================================================


def _jina_scrape(url: str) -> Optional[dict[str, Any]]:
    """Scrape a URL using Jina AI's reader API.

    Jina Reader converts any URL to clean markdown by prefixing with
    https://r.jina.ai/. No API key required for basic use.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not _cb_jina.is_available:
        return None

    t0 = time.monotonic()
    jina_url = f"https://r.jina.ai/{url}"
    headers: dict[str, str] = {
        "Accept": "text/markdown",
        "User-Agent": "NovaAISuite/1.0",
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    req = urllib.request.Request(jina_url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            content = resp.read().decode("utf-8", errors="replace")

        if not content or len(content.strip()) < 50:
            _cb_jina.record_failure("Empty or too-short response")
            return None

        title = ""
        title_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if title_match:
            title = title_match.group(1).strip()

        _cb_jina.record_success()
        elapsed = (time.monotonic() - t0) * 1000
        return _scrape_result(content, url, "jina", title, latency_ms=elapsed)

    except urllib.error.HTTPError as exc:
        if exc.code in (402, 429):
            _cb_jina.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_jina.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(
            f"Jina scrape HTTP {exc.code} for {url}: {exc.reason}", exc_info=True
        )
    except (urllib.error.URLError, OSError) as exc:
        _cb_jina.record_failure(str(exc))
        logger.error(f"Jina scrape error for {url}: {exc}", exc_info=True)
    return None


def _jina_search(query: str, num_results: int = 5) -> Optional[list[dict[str, Any]]]:
    """Search using Jina AI's search API.

    Args:
        query: Search query string.
        num_results: Max number of results.

    Returns:
        List of normalized search results or None on failure.
    """
    if not _cb_jina.is_available:
        return None

    encoded_query = urllib.parse.quote(query, safe="")
    jina_url = f"https://s.jina.ai/{encoded_query}"
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "NovaAISuite/1.0",
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    req = urllib.request.Request(jina_url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")

        results: list[dict[str, Any]] = []
        try:
            data = json.loads(raw)
            items = data.get("data") or data.get("results") or []
            if isinstance(items, list):
                for item in items[:num_results]:
                    results.append(
                        _search_result(
                            title=item.get("title") or "",
                            url=item.get("url") or "",
                            snippet=item.get("description")
                            or item.get("content", "")[:300],
                            provider="jina",
                        )
                    )
        except json.JSONDecodeError:
            sections = re.split(r"\n##?\s+", raw)
            for section in sections[:num_results]:
                lines = section.strip().split("\n")
                title = lines[0] if lines else ""
                snippet = " ".join(lines[1:3]) if len(lines) > 1 else ""
                url_match = re.search(r"\[.*?\]\((https?://[^\)]+)\)", section)
                result_url = url_match.group(1) if url_match else ""
                if title.strip():
                    results.append(
                        _search_result(
                            title=title.strip(),
                            url=result_url,
                            snippet=snippet.strip()[:300],
                            provider="jina",
                        )
                    )

        if results:
            _cb_jina.record_success()
            return results

        _cb_jina.record_failure("No results parsed from Jina search")
        return None

    except urllib.error.HTTPError as exc:
        if exc.code in (402, 429):
            _cb_jina.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_jina.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Jina search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (urllib.error.URLError, OSError) as exc:
        _cb_jina.record_failure(str(exc))
        logger.error(f"Jina search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 3: TAVILY EXTRACT (URL content extraction, 1K credits/month)
# =============================================================================


def _tavily_scrape(url: str) -> Optional[dict[str, Any]]:
    """Use Tavily extract endpoint to scrape a URL.

    Tavily's /extract endpoint pulls content from a specific URL.
    Distinct from their /search endpoint -- this is for known URLs.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not TAVILY_API_KEY:
        return None
    if not _cb_tavily.is_available:
        return None

    t0 = time.monotonic()
    payload = {
        "urls": [url],
        "api_key": TAVILY_API_KEY,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(
        "https://api.tavily.com/extract",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        results = data.get("results") or []
        if results:
            item = results[0]
            content = item.get("raw_content") or item.get("content") or ""
            if content and len(content.strip()) > 50:
                _cb_tavily.record_success()
                elapsed = (time.monotonic() - t0) * 1000
                return _scrape_result(content, url, "tavily", latency_ms=elapsed)

        _cb_tavily.record_failure("No content from Tavily extract")
        return None

    except urllib.error.HTTPError as exc:
        if exc.code in (402, 429):
            _cb_tavily.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_tavily.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(
            f"Tavily scrape HTTP {exc.code} for {url}: {exc.reason}", exc_info=True
        )
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        _cb_tavily.record_failure(str(exc))
        logger.error(f"Tavily scrape error for {url}: {exc}", exc_info=True)
    return None


def _tavily_search(query: str, num_results: int = 5) -> Optional[list[dict[str, Any]]]:
    """Search using Tavily's search API.

    Args:
        query: Search query string.
        num_results: Max number of results.

    Returns:
        List of normalized search results or None on failure.
    """
    if not TAVILY_API_KEY:
        return None
    if not _cb_tavily.is_available:
        return None

    payload = {
        "query": query,
        "api_key": TAVILY_API_KEY,
        "max_results": num_results,
        "include_answer": False,
        "search_depth": "basic",
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(
        "https://api.tavily.com/search",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        raw_results = data.get("results") or []
        results: list[dict[str, Any]] = []
        for item in raw_results[:num_results]:
            results.append(
                _search_result(
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    snippet=item.get("content") or "",
                    provider="tavily",
                )
            )

        if results:
            _cb_tavily.record_success()
            return results

        _cb_tavily.record_failure("No results from Tavily")
        return None

    except urllib.error.HTTPError as exc:
        if exc.code in (402, 429):
            _cb_tavily.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_tavily.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Tavily search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        _cb_tavily.record_failure(str(exc))
        logger.error(f"Tavily search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 4: LLM-ASSISTED SCRAPING (stdlib fetch + LLM content extraction)
# =============================================================================
#
# Fetch raw HTML with stdlib urllib, truncate to ~12K chars to stay within
# free-tier token limits, then send it to a cheap/free LLM via the llm_router
# to intelligently extract and summarize the page content.
#
# This is surprisingly powerful: the LLM can understand context, ignore
# boilerplate, and extract structured data even from messy HTML.
# =============================================================================

# Preferred cheap/free LLMs for HTML extraction (tried in order)
_LLM_SCRAPE_PROVIDERS: list[str] = ["gemini", "groq", "cerebras"]

# Max HTML chars to send to the LLM (keeps token count ~3-4K)
_LLM_HTML_TRUNCATE: int = 12000


def _fetch_raw_html(url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[str]:
    """Fetch raw HTML from a URL using stdlib urllib.

    Args:
        url: The URL to fetch.
        timeout: Request timeout in seconds.

    Returns:
        Raw HTML string or None on failure.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            content_type = resp.headers.get("Content-Type") or ""
            raw = resp.read()

            # Handle gzip
            if resp.headers.get("Content-Encoding") == "gzip":
                import gzip as gzip_module

                raw = gzip_module.decompress(raw)

            # Determine encoding
            charset = "utf-8"
            if "charset=" in content_type:
                charset = content_type.split("charset=")[-1].split(";")[0].strip()

            return raw.decode(charset, errors="replace")

    except (urllib.error.HTTPError, urllib.error.URLError, OSError) as exc:
        logger.error(f"Raw HTML fetch failed for {url}: {exc}", exc_info=True)
        return None


def _strip_html_boilerplate(html_text: str) -> str:
    """Strip script, style, and nav elements to reduce token count before LLM.

    Args:
        html_text: Raw HTML string.

    Returns:
        Cleaned HTML with boilerplate removed.
    """
    # Remove script, style, noscript, svg, nav, footer, header (boilerplate)
    for tag in ("script", "style", "noscript", "svg", "nav", "footer", "header"):
        html_text = re.sub(
            rf"<{tag}[^>]*>.*?</{tag}>",
            "",
            html_text,
            flags=re.DOTALL | re.IGNORECASE,
        )
    # Remove HTML comments
    html_text = re.sub(r"<!--.*?-->", "", html_text, flags=re.DOTALL)
    # Collapse whitespace
    html_text = re.sub(r"\s+", " ", html_text)
    return html_text.strip()


def _llm_assisted_scrape(url: str, topic_hint: str = "") -> Optional[dict[str, Any]]:
    """Scrape a URL by fetching raw HTML and using an LLM to extract content.

    Fetches raw HTML with stdlib, strips boilerplate, truncates to ~12K chars,
    then sends it to a cheap/free LLM (Gemini, Groq, or Cerebras) to
    intelligently extract the main content and key facts.

    Args:
        url: The URL to scrape.
        topic_hint: Optional hint about what the page is about, for better extraction.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not _cb_llm_assist.is_available:
        return None

    t0 = time.monotonic()

    # Step 1: Fetch raw HTML
    raw_html = _fetch_raw_html(url)
    if not raw_html or len(raw_html.strip()) < 100:
        _cb_llm_assist.record_failure("Failed to fetch raw HTML or too short")
        return None

    # Step 2: Strip boilerplate and truncate
    cleaned = _strip_html_boilerplate(raw_html)
    if len(cleaned) > _LLM_HTML_TRUNCATE:
        cleaned = cleaned[:_LLM_HTML_TRUNCATE] + "\n\n[...truncated...]"

    # Step 3: Build extraction prompt
    topic_context = f" about {topic_hint}" if topic_hint else ""
    system_prompt = (
        "You are a web content extraction specialist. Extract the main content "
        "from HTML pages accurately and concisely. Return clean, readable text "
        "with key facts preserved. Ignore navigation, ads, and boilerplate."
    )
    user_prompt = (
        f"Extract the main content, key facts, and any structured data from "
        f"this HTML page{topic_context}. Return clean, readable text.\n\n"
        f"URL: {url}\n\n"
        f"HTML:\n{cleaned}"
    )

    # Step 4: Call LLM router with cheap providers
    try:
        from llm_router import call_llm

        llm_result = call_llm(
            messages=[{"role": "user", "content": user_prompt}],
            system_prompt=system_prompt,
            max_tokens=2048,
            task_type="structured",
            preferred_providers=_LLM_SCRAPE_PROVIDERS,
        )

        extracted = llm_result.get("text") or ""
        if extracted and len(extracted.strip()) > 50:
            _cb_llm_assist.record_success()
            elapsed = (time.monotonic() - t0) * 1000
            llm_provider = llm_result.get("provider") or "unknown"
            return _scrape_result(
                content=extracted,
                url=url,
                provider=f"llm_assisted:{llm_provider}",
                metadata={
                    "llm_provider": llm_provider,
                    "llm_model": llm_result.get("model") or "",
                    "llm_latency_ms": llm_result.get("latency_ms") or 0,
                    "html_chars_sent": len(cleaned),
                },
                latency_ms=elapsed,
            )

        _cb_llm_assist.record_failure("LLM returned empty or too-short extraction")
        return None

    except ImportError:
        _cb_llm_assist.record_failure("llm_router not available")
        logger.error(
            "LLM-assisted scraping failed: llm_router import error", exc_info=True
        )
        return None
    except Exception as exc:
        _cb_llm_assist.record_failure(str(exc))
        logger.error(f"LLM-assisted scraping failed for {url}: {exc}", exc_info=True)
        return None


# =============================================================================
# TIER 5: GOOGLE CACHE / WEB ARCHIVE FALLBACK
# =============================================================================
#
# When the original URL is down, blocked, or returns errors, try fetching
# cached/archived versions. Two sources:
#   1. Google Webcache: https://webcache.googleusercontent.com/search?q=cache:{url}
#   2. Internet Archive Wayback Machine: https://web.archive.org/web/2024/{url}
#
# These are fetched with stdlib urllib and parsed with the same HTML extractor
# used by Tier 6.
# =============================================================================


def _cache_fallback_scrape(url: str) -> Optional[dict[str, Any]]:
    """Try scraping a URL from Google Cache or Internet Archive.

    Attempts Google Cache first, then Wayback Machine.  Uses the stdlib
    HTML parser to extract text from the cached HTML.

    Args:
        url: The original URL to look up in caches.

    Returns:
        Normalized scrape result or None if no cached version found.
    """
    if not _cb_cache_fallback.is_available:
        return None

    t0 = time.monotonic()

    # Source 1: Google Webcache
    google_cache_url = (
        f"https://webcache.googleusercontent.com/search"
        f"?q=cache:{urllib.parse.quote(url, safe='')}"
    )
    result = _fetch_and_parse_html(google_cache_url, CACHE_SCRAPE_TIMEOUT)
    if result:
        content, title = result
        _cb_cache_fallback.record_success()
        elapsed = (time.monotonic() - t0) * 1000
        return _scrape_result(
            content=content,
            url=url,
            provider="google_cache",
            title=title,
            metadata={"cache_url": google_cache_url},
            latency_ms=elapsed,
        )

    # Source 2: Internet Archive Wayback Machine
    archive_url = f"https://web.archive.org/web/2024/{url}"
    result = _fetch_and_parse_html(archive_url, CACHE_SCRAPE_TIMEOUT)
    if result:
        content, title = result
        _cb_cache_fallback.record_success()
        elapsed = (time.monotonic() - t0) * 1000
        return _scrape_result(
            content=content,
            url=url,
            provider="web_archive",
            title=title,
            metadata={"archive_url": archive_url},
            latency_ms=elapsed,
        )

    _cb_cache_fallback.record_failure(f"No cached version found for {url}")
    return None


def _fetch_and_parse_html(
    fetch_url: str, timeout: int = REQUEST_TIMEOUT
) -> Optional[tuple[str, str]]:
    """Fetch a URL and parse HTML to extract text content.

    Args:
        fetch_url: The URL to fetch.
        timeout: Request timeout in seconds.

    Returns:
        Tuple of (content, title) or None on failure.
    """
    raw_html = _fetch_raw_html(fetch_url, timeout=timeout)
    if not raw_html or len(raw_html.strip()) < 100:
        return None

    parser = _HTMLTextExtractor()
    try:
        parser.feed(raw_html)
    except Exception:
        pass

    if parser.texts:
        content = "\n\n".join(parser.texts)
        if len(content.strip()) > 50:
            return content, parser.title

    return None


# =============================================================================
# TIER 6: STDLIB URLLIB RAW FETCH + HTML PARSER (always works)
# =============================================================================


class _HTMLTextExtractor(html.parser.HTMLParser):
    """Simple HTML parser that extracts visible text from semantic tags.

    Skips script and style content. Collects text into a list of strings.
    """

    _VISIBLE_TAGS = frozenset(
        {"p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "span", "div", "a"}
    )
    _SKIP_TAGS = frozenset({"script", "style", "noscript", "svg", "path"})

    def __init__(self) -> None:
        """Initialize the parser with empty state."""
        super().__init__()
        self.texts: list[str] = []
        self.title: str = ""
        self._in_visible = False
        self._in_skip = False
        self._in_title = False
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        """Track tag entry for visible content extraction."""
        tag_lower = tag.lower()
        if tag_lower in self._SKIP_TAGS:
            self._in_skip = True
        elif tag_lower == "title":
            self._in_title = True
        elif tag_lower in self._VISIBLE_TAGS:
            self._in_visible = True

    def handle_endtag(self, tag: str) -> None:
        """Track tag exit and flush accumulated text."""
        tag_lower = tag.lower()
        if tag_lower in self._SKIP_TAGS:
            self._in_skip = False
        elif tag_lower == "title":
            self._in_title = False
            self.title = " ".join(self._current_text).strip()
            self._current_text = []
        elif tag_lower in self._VISIBLE_TAGS:
            if self._current_text:
                text = " ".join(self._current_text).strip()
                if text:
                    self.texts.append(text)
                self._current_text = []
            self._in_visible = False

    def handle_data(self, data: str) -> None:
        """Accumulate text data from visible tags."""
        if self._in_skip:
            return
        if self._in_title:
            self._current_text.append(data.strip())
        elif self._in_visible:
            stripped = data.strip()
            if stripped:
                self._current_text.append(stripped)


def _urllib_scrape(url: str) -> Optional[dict[str, Any]]:
    """Scrape a URL directly using stdlib urllib and parse HTML for text.

    This is the fallback-of-last-resort. No API key needed, but quality
    is lower than dedicated scraping APIs or LLM-assisted extraction.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not _cb_urllib.is_available:
        return None

    t0 = time.monotonic()
    raw_html = _fetch_raw_html(url)
    if not raw_html:
        _cb_urllib.record_failure("Failed to fetch HTML")
        return None

    # Parse HTML to extract text
    parser = _HTMLTextExtractor()
    try:
        parser.feed(raw_html)
    except Exception:
        pass

    if parser.texts:
        content = "\n\n".join(parser.texts)
        _cb_urllib.record_success()
        elapsed = (time.monotonic() - t0) * 1000
        return _scrape_result(
            content, url, "urllib_direct", parser.title, latency_ms=elapsed
        )

    # Fallback: regex extraction if parser produced nothing
    cleaned = re.sub(
        r"<(script|style)[^>]*>.*?</\1>",
        "",
        raw_html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    text_parts = re.findall(
        r"<(?:p|h[1-6]|li|td)[^>]*>(.*?)</(?:p|h[1-6]|li|td)>",
        cleaned,
        re.DOTALL | re.IGNORECASE,
    )
    if text_parts:
        content = "\n\n".join(
            re.sub(r"<[^>]+>", "", part).strip() for part in text_parts if part.strip()
        )
        if content.strip():
            _cb_urllib.record_success()
            elapsed = (time.monotonic() - t0) * 1000
            return _scrape_result(content, url, "urllib_direct", latency_ms=elapsed)

    _cb_urllib.record_failure("No text extracted from HTML")
    return None


# =============================================================================
# PUBLIC API: scrape_url
# =============================================================================


def _validate_url_security(url: str) -> tuple[bool, str]:
    """Validate URL to prevent SSRF attacks.

    Blocks:
    - Dangerous schemes (only allow http/https)
    - Private/internal IP ranges (127.*, 192.168.*, 10.*, 172.16.*, 169.254.*)
    - Reserved hostnames (localhost, 0.0.0.0, ::1)
    - AWS metadata endpoint (169.254.169.254)

    Args:
        url: The URL to validate.

    Returns:
        Tuple of (is_valid: bool, error_message: str). If valid, error_message is empty.
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception as e:
        return False, f"Invalid URL format: {e}"

    # Check scheme: only allow http and https
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        return False, f"Dangerous scheme '{scheme}' not allowed (only http/https)"

    hostname = parsed.hostname or ""
    if not hostname:
        return False, "URL missing hostname"

    hostname_lower = hostname.lower()

    # Blocked reserved hostnames
    blocked_hostnames = {"localhost", "0.0.0.0", "::1", "127.0.0.1"}
    if hostname_lower in blocked_hostnames:
        return False, f"Reserved hostname '{hostname}' blocked"

    # Block private IP ranges
    private_patterns = [
        r"^127\.",  # 127.0.0.0/8
        r"^192\.168\.",  # 192.168.0.0/16
        r"^10\.",  # 10.0.0.0/8
        r"^172\.(1[6-9]|2[0-9]|3[0-1])\.",  # 172.16.0.0/12
        r"^169\.254\.",  # 169.254.0.0/16 (link-local)
    ]

    for pattern in private_patterns:
        if re.match(pattern, hostname):
            return False, f"Private IP range '{hostname}' blocked (SSRF protection)"

    # Block AWS metadata endpoint specifically
    if hostname_lower == "169.254.169.254":
        return False, "AWS metadata endpoint blocked (SSRF protection)"

    return True, ""


def scrape_url(
    url: str,
    topic_hint: str = "",
    use_cache: bool = True,
) -> dict[str, Any]:
    """Scrape a URL using the best available provider with automatic fallback.

    Tries each tier in order:
        1.   Firecrawl (paid, highest quality)
        1.5  Apify Website Content Crawler (API key, cheerio-based)
        2.   Jina AI Reader (free, good markdown)
        3.   Tavily Extract (API key, good content extraction)
        4.   LLM-assisted (free LLM + stdlib fetch, context-aware extraction)
        5.   Google Cache / Web Archive (cached versions of the page)
        6.   stdlib urllib + HTMLParser (always works, basic text)

    Falls through on any failure. Returns empty result only if ALL tiers fail.

    Args:
        url: The URL to scrape.
        topic_hint: Optional hint about page content (improves LLM extraction).
        use_cache: Whether to check/populate the LRU + Redis cache.

    Returns:
        Normalized result dict with keys: content, url, provider, title,
        metadata, latency_ms, scraped_at. On total failure, content will
        be empty and provider will be 'none'.
    """
    if not url or not url.strip():
        return _scrape_result("", "", "none")

    url = url.strip()

    # SECURITY: Validate URL to prevent SSRF attacks (P0 vulnerability fix)
    is_valid, error_msg = _validate_url_security(url)
    if not is_valid:
        logger.warning(
            f"scrape_url: URL security validation failed: {error_msg} for {url}"
        )
        return _scrape_result("", url, "none", error=error_msg)

    # Check cache first
    if use_cache:
        ck = _cache_key(url, "scrape")
        cached = _cache_get(ck)
        if cached is not None:
            logger.info(f"scrape_url: cache HIT for {url}")
            cached["provider"] = f"cache:{cached.get('provider', 'unknown')}"
            return cached

    t0_total = time.monotonic()

    # Quality threshold: below this score, fall through to next tier
    _QUALITY_THRESHOLD = 0.3

    def _accept_result(
        result: Optional[dict[str, Any]], tier_name: str, tier_label: str
    ) -> Optional[dict[str, Any]]:
        """Check quality, record usage/freshness, and accept or reject a result."""
        if not result or not result.get("content"):
            _tier_usage.record(tier_name, success=False)
            return None

        content = result["content"]
        quality = _score_content_quality(content, url)
        result["metadata"] = result.get("metadata") or {}
        result["metadata"]["content_quality_score"] = quality

        if quality < _QUALITY_THRESHOLD:
            _tier_usage.record(tier_name, success=False)
            logger.info(
                f"scrape_url: {tier_label} content quality too low "
                f"({quality:.2f} < {_QUALITY_THRESHOLD}) for {url}, falling through"
            )
            return None

        # Quality OK -- record success and freshness
        _tier_usage.record(tier_name, success=True)
        freshness = _freshness_tracker.record(url, content)
        result["metadata"]["freshness"] = freshness

        logger.info(
            f"scrape_url: {tier_label} succeeded for {url} "
            f"in {result.get('latency_ms', 0):.0f}ms "
            f"(quality={quality:.2f}, freshness={freshness.get('freshness', 'new')})"
        )
        if use_cache:
            _cache_put(_cache_key(url, "scrape"), result)
        return result

    # -- Tier 1: Firecrawl --
    accepted = _accept_result(_firecrawl_scrape(url), "firecrawl", "Tier 1 (Firecrawl)")
    if accepted:
        return accepted

    # -- Tier 1.5: Apify Website Content Crawler --
    accepted = _accept_result(_apify_scrape(url), "apify", "Tier 1.5 (Apify)")
    if accepted:
        return accepted

    # -- Tier 2: Jina AI Reader --
    accepted = _accept_result(_jina_scrape(url), "jina", "Tier 2 (Jina)")
    if accepted:
        return accepted

    # -- Tier 3: Tavily Extract --
    accepted = _accept_result(_tavily_scrape(url), "tavily", "Tier 3 (Tavily)")
    if accepted:
        return accepted

    # -- Tier 4: LLM-assisted scraping --
    accepted = _accept_result(
        _llm_assisted_scrape(url, topic_hint=topic_hint),
        "llm_assisted",
        "Tier 4 (LLM-assisted)",
    )
    if accepted:
        return accepted

    # -- Tier 5: Google Cache / Web Archive --
    accepted = _accept_result(
        _cache_fallback_scrape(url), "cache_fallback", "Tier 5 (Cache)"
    )
    if accepted:
        return accepted

    # -- Tier 6: stdlib urllib + HTMLParser --
    # Last resort: accept even low-quality results rather than returning nothing
    result = _urllib_scrape(url)
    if result and result.get("content"):
        content = result["content"]
        quality = _score_content_quality(content, url)
        result["metadata"] = result.get("metadata") or {}
        result["metadata"]["content_quality_score"] = quality
        freshness = _freshness_tracker.record(url, content)
        result["metadata"]["freshness"] = freshness
        _tier_usage.record("stdlib", success=True)
        logger.info(
            f"scrape_url: Tier 6 (urllib) succeeded for {url} "
            f"in {result.get('latency_ms', 0):.0f}ms "
            f"(quality={quality:.2f}, last-resort)"
        )
        if use_cache:
            _cache_put(_cache_key(url, "scrape"), result)
        return result

    _tier_usage.record("stdlib", success=False)
    total_elapsed = (time.monotonic() - t0_total) * 1000
    logger.warning(
        f"scrape_url: ALL 7 tiers failed for {url} after {total_elapsed:.0f}ms"
    )
    return _scrape_result("", url, "none", latency_ms=total_elapsed)


# =============================================================================
# PUBLIC API: search_web
# =============================================================================


def search_web(
    query: str,
    num_results: int = 5,
    use_cache: bool = True,
) -> list[dict[str, Any]]:
    """Search the web using the best available provider with automatic fallback.

    Tries each tier in order: Firecrawl -> Jina -> Tavily.
    Falls through on any failure. Returns empty list only if ALL tiers fail.

    Args:
        query: Search query string.
        num_results: Maximum number of results to return (default 5).
        use_cache: Whether to check/populate the LRU + Redis cache.

    Returns:
        List of normalized search result dicts, each with keys: title, url,
        snippet, provider. Empty list on total failure.
    """
    if not query or not query.strip():
        return []

    query = query.strip()

    # Check cache first
    if use_cache:
        ck = _cache_key(f"{query}:{num_results}", "search")
        cached = _cache_get(ck)
        if cached is not None:
            logger.info(f"search_web: cache HIT for query={query[:50]}")
            return cached

    # Tier 1: Firecrawl
    results = _firecrawl_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 1 (Firecrawl) returned {len(results)} results")
        if use_cache:
            _cache_put(_cache_key(f"{query}:{num_results}", "search"), results)
        return results

    # Tier 2: Jina Search
    results = _jina_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 2 (Jina) returned {len(results)} results")
        if use_cache:
            _cache_put(_cache_key(f"{query}:{num_results}", "search"), results)
        return results

    # Tier 3: Tavily Search
    results = _tavily_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 3 (Tavily) returned {len(results)} results")
        if use_cache:
            _cache_put(_cache_key(f"{query}:{num_results}", "search"), results)
        return results

    logger.warning(f"search_web: ALL tiers failed for query: {query[:80]}")
    return []


# =============================================================================
# PUBLIC API: check_content_changed (compliance monitoring)
# =============================================================================


def check_content_changed(url: str) -> dict[str, Any]:
    """Check if a URL's content has changed since last scrape.

    Useful for compliance monitoring -- scrapes the URL and compares
    its content hash against the previously stored hash.

    Args:
        url: The URL to check for content changes.

    Returns:
        Dict with url, changed (bool or None on error), freshness metadata,
        and a content_preview (first 500 chars).
    """
    result = scrape_url(url)
    if result and result.get("content"):
        freshness = _freshness_tracker.record(url, result["content"])
        return {
            "url": url,
            "changed": freshness.get("freshness") == "changed",
            "freshness": freshness,
            "content_preview": result["content"][:500],
        }
    return {"url": url, "changed": None, "error": "Could not scrape URL"}


# =============================================================================
# PUBLIC API: get_tier_usage_report
# =============================================================================


def get_tier_usage_report() -> dict[str, Any]:
    """Return per-tier usage and estimated cost report.

    Returns:
        Dict with total_est_cost_usd and per-tier breakdown of calls,
        successes, failures, and estimated cost.
    """
    return _tier_usage.get_report()


# =============================================================================
# PUBLIC API: get_freshness_stats
# =============================================================================


def get_freshness_stats() -> dict[str, Any]:
    """Return content freshness tracking statistics.

    Returns:
        Dict with tracked_urls count and recently_changed count.
    """
    return _freshness_tracker.get_stats()


# =============================================================================
# PUBLIC API: get_scraper_status
# =============================================================================


def get_scraper_status() -> dict[str, Any]:
    """Return the health and configuration status of all scraping tiers.

    Returns:
        Dict with per-tier status including availability, circuit breaker
        state, request counts, success rates, and cache stats.
    """
    # Check LLM router availability
    llm_available = False
    try:
        from llm_router import call_llm  # noqa: F401

        llm_available = True
    except ImportError:
        pass

    tiers = [
        {
            "tier": 1,
            "provider": "firecrawl",
            "has_api_key": bool(FIRECRAWL_API_KEY),
            "capabilities": ["scrape", "search", "map"],
            "free_tier": "500 credits/month",
            **_cb_firecrawl.get_stats(),
        },
        {
            "tier": 1.5,
            "provider": "apify",
            "has_api_key": bool(APIFY_API_TOKEN),
            "capabilities": ["scrape"],
            "free_tier": "$5 free credit/month",
            "note": "Website Content Crawler actor (cheerio-based, fast)",
            **_cb_apify.get_stats(),
        },
        {
            "tier": 2,
            "provider": "jina",
            "has_api_key": True,  # No key needed for basic
            "capabilities": ["scrape", "search"],
            "free_tier": "Unlimited basic (rate-limited)",
            **_cb_jina.get_stats(),
        },
        {
            "tier": 3,
            "provider": "tavily",
            "has_api_key": bool(TAVILY_API_KEY),
            "capabilities": ["scrape", "search"],
            "free_tier": "1,000 credits/month",
            **_cb_tavily.get_stats(),
        },
        {
            "tier": 4,
            "provider": "llm_assisted",
            "has_api_key": llm_available,
            "capabilities": ["scrape"],
            "free_tier": "Free (uses Gemini/Groq/Cerebras via llm_router)",
            "note": "Fetches HTML with stdlib, sends to LLM for extraction",
            **_cb_llm_assist.get_stats(),
        },
        {
            "tier": 5,
            "provider": "cache_fallback",
            "has_api_key": True,  # Always available
            "capabilities": ["scrape"],
            "free_tier": "Unlimited (Google Cache + Web Archive)",
            "note": "Falls back to cached/archived versions of pages",
            **_cb_cache_fallback.get_stats(),
        },
        {
            "tier": 6,
            "provider": "urllib_direct",
            "has_api_key": True,  # Always available
            "capabilities": ["scrape"],
            "free_tier": "Unlimited (no API needed)",
            **_cb_urllib.get_stats(),
        },
    ]

    available_count = sum(1 for t in tiers if t.get("available", False))
    configured_count = sum(1 for t in tiers if t.get("has_api_key", False))

    # Cache stats
    with _lru_lock:
        cache_size = len(_lru_cache)

    return {
        "total_tiers": len(tiers),
        "available_tiers": available_count,
        "configured_tiers": configured_count,
        "cache": {
            "l1_entries": cache_size,
            "l1_max_size": _LRU_MAX_SIZE,
            "l1_ttl_seconds": int(_LRU_TTL),
            "l2_redis_available": _redis_available,
        },
        "tier_usage": _tier_usage.get_report(),
        "freshness": _freshness_tracker.get_stats(),
        "tiers": tiers,
    }


# =============================================================================
# PUBLIC API: reset_circuit_breakers (admin/testing)
# =============================================================================


def reset_circuit_breakers() -> dict[str, str]:
    """Reset all circuit breakers to their initial state.

    Returns:
        Confirmation dict.
    """
    for cb in (
        _cb_firecrawl,
        _cb_apify,
        _cb_jina,
        _cb_tavily,
        _cb_llm_assist,
        _cb_cache_fallback,
        _cb_urllib,
    ):
        cb.reset()
    return {"status": "all_circuit_breakers_reset"}


# =============================================================================
# PUBLIC API: clear_cache (admin/testing)
# =============================================================================


def clear_cache() -> dict[str, Any]:
    """Clear the in-memory LRU cache.

    Returns:
        Dict with number of entries cleared.
    """
    with _lru_lock:
        count = len(_lru_cache)
        _lru_cache.clear()
    return {"status": "cache_cleared", "entries_removed": count}
