"""
web_scraper_router.py -- Multi-tier web scraping fallback system.

Provides a unified interface for web scraping and search with automatic
fallback across 6 providers. When one tier fails (402 credit exhausted,
429 rate limited, or network error), the router transparently falls
through to the next available tier.

Tier 1: Firecrawl     -- Full-featured scrape/search/map (API key required)
Tier 2: Jina AI Reader -- Free markdown reader (no API key for basic)
Tier 3: Tavily Search  -- 1,000 free searches/month (API key required)
Tier 4: Serper         -- 2,500 free searches/month (API key required)
Tier 5: Brave Search   -- 2,000 free searches/month (API key required)
Tier 6: Direct urllib   -- Always available, no API key needed

All external API calls:
    - Use only urllib.request (stdlib, no third-party dependencies)
    - Have per-tier circuit breakers (1hr cooldown on 402/429)
    - Track request counts and success rates
    - Are thread-safe
    - Log which tier was used
    - Return normalized output

Usage:
    from web_scraper_router import scrape_url, search_web, get_scraper_status
    result = scrape_url("https://example.com")
    results = search_web("recruitment advertising trends")
"""

from __future__ import annotations

import html.parser
import json
import logging
import os
import re
import ssl
import threading
import time
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

FIRECRAWL_API_KEY: str = os.environ.get("FIRECRAWL_API_KEY") or ""
FIRECRAWL_BASE_URL: str = "https://api.firecrawl.dev/v1"
JINA_API_KEY: str = os.environ.get("JINA_API_KEY") or ""
TAVILY_API_KEY: str = os.environ.get("TAVILY_API_KEY") or ""
SERPER_API_KEY: str = os.environ.get("SERPER_API_KEY") or ""
BRAVE_API_KEY: str = os.environ.get("BRAVE_API_KEY") or ""

REQUEST_TIMEOUT: int = 15  # seconds per request
CIRCUIT_BREAKER_COOLDOWN: int = 3600  # 1 hour cooldown after 402/429


# =============================================================================
# CIRCUIT BREAKER (thread-safe, per-tier)
# =============================================================================


class CircuitBreaker:
    """Thread-safe circuit breaker for a single scraping tier.

    Disables a tier for a configurable cooldown period after receiving
    a 402 (credits exhausted) or 429 (rate limited) response. Tracks
    request counts and success rates for monitoring.
    """

    def __init__(self, name: str, cooldown: int = CIRCUIT_BREAKER_COOLDOWN) -> None:
        """Initialize circuit breaker for a named tier.

        Args:
            name: Human-readable tier name (e.g., "firecrawl").
            cooldown: Seconds to disable after a trip.
        """
        self.name = name
        self.cooldown = cooldown
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
                # Cooldown expired, re-enable
                self._disabled_until = 0.0
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
        """Trip the circuit breaker, disabling the tier for the cooldown period.

        Args:
            reason: Human-readable reason for the trip (e.g., "402 credits exhausted").
        """
        with self._lock:
            self._disabled_until = time.time() + self.cooldown
            self._last_error = reason
            self._last_error_time = time.time()
            self._failed_requests += 1
        logger.warning(
            f"Circuit breaker tripped for {self.name}: {reason}. "
            f"Disabled for {self.cooldown}s."
        )

    def record_success(self) -> None:
        """Record a successful request."""
        with self._lock:
            self._total_requests += 1
            self._successful_requests += 1

    def record_failure(self, reason: str = "") -> None:
        """Record a failed request (does NOT trip the breaker).

        Args:
            reason: Description of the failure.
        """
        with self._lock:
            self._total_requests += 1
            self._failed_requests += 1
            self._last_error = reason
            self._last_error_time = time.time()

    def get_stats(self) -> dict[str, Any]:
        """Return monitoring stats for this circuit breaker."""
        with self._lock:
            success_rate = 0.0
            if self._total_requests > 0:
                success_rate = round(
                    self._successful_requests / self._total_requests * 100, 1
                )
            return {
                "name": self.name,
                "available": self.is_available,
                "remaining_cooldown_seconds": self.remaining_cooldown,
                "total_requests": self._total_requests,
                "successful_requests": self._successful_requests,
                "failed_requests": self._failed_requests,
                "success_rate_pct": success_rate,
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
            self._total_requests = 0
            self._successful_requests = 0
            self._failed_requests = 0
            self._last_error = ""
            self._last_error_time = 0.0


# Per-tier circuit breakers (module-level singletons)
_cb_firecrawl = CircuitBreaker("firecrawl")
_cb_jina = CircuitBreaker("jina")
_cb_tavily = CircuitBreaker("tavily")
_cb_serper = CircuitBreaker("serper")
_cb_brave = CircuitBreaker("brave")
_cb_urllib = CircuitBreaker("urllib_direct", cooldown=300)  # 5-min cooldown


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
) -> dict[str, Any]:
    """Build a normalized scrape result dict.

    Args:
        content: Extracted text/markdown content.
        url: The URL that was scraped.
        provider: Name of the provider tier that succeeded.
        title: Page title if available.
        metadata: Any additional metadata from the provider.

    Returns:
        Normalized result dict with content, url, provider, title, metadata.
    """
    return {
        "content": content or "",
        "url": url,
        "provider": provider,
        "title": title or "",
        "metadata": metadata or {},
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


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
# TIER 1: FIRECRAWL
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
    req = Request(
        f"{FIRECRAWL_BASE_URL}/scrape",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if not data.get("success"):
            _cb_firecrawl.record_failure("API returned success=false")
            return None

        resp_data = data.get("data") or {}
        content = resp_data.get("markdown") or ""
        title = resp_data.get("metadata", {}).get("title") or ""
        _cb_firecrawl.record_success()
        return _scrape_result(content, url, "firecrawl", title)

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_firecrawl.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_firecrawl.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(
            f"Firecrawl scrape HTTP {exc.code} for {url}: {exc.reason}",
            exc_info=True,
        )
    except (URLError, json.JSONDecodeError, OSError) as exc:
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
    req = Request(
        f"{FIRECRAWL_BASE_URL}/search",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
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

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_firecrawl.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_firecrawl.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Firecrawl search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (URLError, json.JSONDecodeError, OSError) as exc:
        _cb_firecrawl.record_failure(str(exc))
        logger.error(f"Firecrawl search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 2: JINA AI READER (free, no API key needed for basic)
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

    jina_url = f"https://r.jina.ai/{url}"
    headers: dict[str, str] = {
        "Accept": "text/markdown",
        "User-Agent": "NovaAISuite/1.0",
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    req = Request(jina_url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            content = resp.read().decode("utf-8", errors="replace")

        if not content or len(content.strip()) < 50:
            _cb_jina.record_failure("Empty or too-short response")
            return None

        # Extract title from first markdown heading if present
        title = ""
        title_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        if title_match:
            title = title_match.group(1).strip()

        _cb_jina.record_success()
        return _scrape_result(content, url, "jina", title)

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_jina.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_jina.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(
            f"Jina scrape HTTP {exc.code} for {url}: {exc.reason}", exc_info=True
        )
    except (URLError, OSError) as exc:
        _cb_jina.record_failure(str(exc))
        logger.error(f"Jina scrape error for {url}: {exc}", exc_info=True)
    return None


def _jina_search(query: str, num_results: int = 5) -> Optional[list[dict[str, Any]]]:
    """Search using Jina AI's search API.

    Jina Search returns markdown results by querying https://s.jina.ai/{query}.

    Args:
        query: Search query string.
        num_results: Max number of results.

    Returns:
        List of normalized search results or None on failure.
    """
    if not _cb_jina.is_available:
        return None

    import urllib.parse

    encoded_query = urllib.parse.quote(query, safe="")
    jina_url = f"https://s.jina.ai/{encoded_query}"
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": "NovaAISuite/1.0",
    }
    if JINA_API_KEY:
        headers["Authorization"] = f"Bearer {JINA_API_KEY}"

    req = Request(jina_url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")

        # Jina search can return JSON or markdown depending on Accept header
        results: list[dict[str, Any]] = []
        try:
            data = json.loads(raw)
            # JSON response format
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
            # Markdown response -- parse sections as results
            sections = re.split(r"\n##?\s+", raw)
            for section in sections[:num_results]:
                lines = section.strip().split("\n")
                title = lines[0] if lines else ""
                snippet = " ".join(lines[1:3]) if len(lines) > 1 else ""
                # Try to extract URL from markdown links
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

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_jina.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_jina.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Jina search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (URLError, OSError) as exc:
        _cb_jina.record_failure(str(exc))
        logger.error(f"Jina search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 3: TAVILY SEARCH (1,000 free searches/month)
# =============================================================================


def _tavily_search(query: str, num_results: int = 5) -> Optional[list[dict[str, Any]]]:
    """Search using Tavily's search API.

    Tavily provides high-quality search results with content snippets.
    Requires TAVILY_API_KEY env var.

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
    req = Request(
        "https://api.tavily.com/search",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
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

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_tavily.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_tavily.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Tavily search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (URLError, json.JSONDecodeError, OSError) as exc:
        _cb_tavily.record_failure(str(exc))
        logger.error(f"Tavily search error: {exc}", exc_info=True)
    return None


def _tavily_scrape(url: str) -> Optional[dict[str, Any]]:
    """Use Tavily extract to scrape a URL.

    Tavily's extract endpoint can pull content from a URL.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not TAVILY_API_KEY:
        return None
    if not _cb_tavily.is_available:
        return None

    payload = {
        "urls": [url],
        "api_key": TAVILY_API_KEY,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = Request(
        "https://api.tavily.com/extract",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        results = data.get("results") or []
        if results:
            item = results[0]
            content = item.get("raw_content") or item.get("content") or ""
            if content and len(content.strip()) > 50:
                _cb_tavily.record_success()
                return _scrape_result(content, url, "tavily")

        _cb_tavily.record_failure("No content from Tavily extract")
        return None

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_tavily.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_tavily.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(
            f"Tavily scrape HTTP {exc.code} for {url}: {exc.reason}", exc_info=True
        )
    except (URLError, json.JSONDecodeError, OSError) as exc:
        _cb_tavily.record_failure(str(exc))
        logger.error(f"Tavily scrape error for {url}: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 4: SERPER (2,500 free searches/month)
# =============================================================================


def _serper_search(query: str, num_results: int = 5) -> Optional[list[dict[str, Any]]]:
    """Search using Serper's Google Search API.

    Serper provides Google search results via a simple API.
    Requires SERPER_API_KEY env var.

    Args:
        query: Search query string.
        num_results: Max number of results.

    Returns:
        List of normalized search results or None on failure.
    """
    if not SERPER_API_KEY:
        return None
    if not _cb_serper.is_available:
        return None

    payload = {
        "q": query,
        "num": num_results,
    }
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": SERPER_API_KEY,
    }
    req = Request(
        "https://google.serper.dev/search",
        data=body,
        headers=headers,
        method="POST",
    )

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        organic = data.get("organic") or []
        results: list[dict[str, Any]] = []
        for item in organic[:num_results]:
            results.append(
                _search_result(
                    title=item.get("title") or "",
                    url=item.get("link") or "",
                    snippet=item.get("snippet") or "",
                    provider="serper",
                )
            )

        if results:
            _cb_serper.record_success()
            return results

        _cb_serper.record_failure("No organic results from Serper")
        return None

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_serper.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_serper.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Serper search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (URLError, json.JSONDecodeError, OSError) as exc:
        _cb_serper.record_failure(str(exc))
        logger.error(f"Serper search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 5: BRAVE SEARCH (2,000 free searches/month)
# =============================================================================


def _brave_search(query: str, num_results: int = 5) -> Optional[list[dict[str, Any]]]:
    """Search using Brave Search API.

    Brave provides independent web search results.
    Requires BRAVE_API_KEY env var.

    Args:
        query: Search query string.
        num_results: Max number of results.

    Returns:
        List of normalized search results or None on failure.
    """
    if not BRAVE_API_KEY:
        return None
    if not _cb_brave.is_available:
        return None

    import urllib.parse

    encoded_query = urllib.parse.quote(query, safe="")
    brave_url = (
        f"https://api.search.brave.com/res/v1/web/search"
        f"?q={encoded_query}&count={num_results}"
    )
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }
    req = Request(brave_url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            raw = resp.read()
            # Handle gzip encoding
            if resp.headers.get("Content-Encoding") == "gzip":
                import gzip as gzip_module

                raw = gzip_module.decompress(raw)
            data = json.loads(raw.decode("utf-8"))

        web_results = data.get("web", {}).get("results") or []
        results: list[dict[str, Any]] = []
        for item in web_results[:num_results]:
            results.append(
                _search_result(
                    title=item.get("title") or "",
                    url=item.get("url") or "",
                    snippet=item.get("description") or "",
                    provider="brave",
                )
            )

        if results:
            _cb_brave.record_success()
            return results

        _cb_brave.record_failure("No web results from Brave")
        return None

    except HTTPError as exc:
        if exc.code in (402, 429):
            _cb_brave.trip(f"HTTP {exc.code}: {exc.reason}")
        else:
            _cb_brave.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(f"Brave search HTTP {exc.code}: {exc.reason}", exc_info=True)
    except (URLError, json.JSONDecodeError, OSError) as exc:
        _cb_brave.record_failure(str(exc))
        logger.error(f"Brave search error: {exc}", exc_info=True)
    return None


# =============================================================================
# TIER 6: DIRECT URLLIB (always available, no API key)
# =============================================================================


class _HTMLTextExtractor(html.parser.HTMLParser):
    """Simple HTML parser that extracts visible text from p, h1-h6, li, td tags.

    Skips script and style content. Collects text into a list of strings.
    """

    _VISIBLE_TAGS = frozenset(
        {"p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td", "th", "span", "div", "a"}
    )
    _SKIP_TAGS = frozenset({"script", "style", "noscript", "svg", "path"})

    def __init__(self) -> None:
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
    """Scrape a URL directly using urllib and parse HTML for text.

    This is the fallback-of-last-resort. No API key needed, but
    quality is lower than dedicated scraping APIs.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized scrape result or None on failure.
    """
    if not _cb_urllib.is_available:
        return None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    req = Request(url, headers=headers, method="GET")

    try:
        ctx = _build_ssl_context()
        with urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
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

            html_text = raw.decode(charset, errors="replace")

        # Parse HTML to extract text
        parser = _HTMLTextExtractor()
        try:
            parser.feed(html_text)
        except Exception:
            # HTMLParser can raise on malformed HTML; fall back to regex
            pass

        if parser.texts:
            content = "\n\n".join(parser.texts)
            _cb_urllib.record_success()
            return _scrape_result(content, url, "urllib_direct", parser.title)

        # Fallback: regex extraction if parser failed
        # Remove script/style blocks
        cleaned = re.sub(
            r"<(script|style)[^>]*>.*?</\1>",
            "",
            html_text,
            flags=re.DOTALL | re.IGNORECASE,
        )
        # Extract text from remaining tags
        text_parts = re.findall(
            r"<(?:p|h[1-6]|li|td)[^>]*>(.*?)</(?:p|h[1-6]|li|td)>",
            cleaned,
            re.DOTALL | re.IGNORECASE,
        )
        if text_parts:
            # Strip any remaining HTML tags
            content = "\n\n".join(
                re.sub(r"<[^>]+>", "", part).strip()
                for part in text_parts
                if part.strip()
            )
            if content.strip():
                _cb_urllib.record_success()
                return _scrape_result(content, url, "urllib_direct")

        _cb_urllib.record_failure("No text extracted from HTML")
        return None

    except HTTPError as exc:
        if exc.code == 429:
            _cb_urllib.trip(f"HTTP 429: {exc.reason}")
        else:
            _cb_urllib.record_failure(f"HTTP {exc.code}: {exc.reason}")
        logger.error(
            f"urllib scrape HTTP {exc.code} for {url}: {exc.reason}", exc_info=True
        )
    except (URLError, OSError) as exc:
        _cb_urllib.record_failure(str(exc))
        logger.error(f"urllib scrape error for {url}: {exc}", exc_info=True)
    return None


# =============================================================================
# PUBLIC API: scrape_url
# =============================================================================


def scrape_url(url: str) -> dict[str, Any]:
    """Scrape a URL using the best available provider with automatic fallback.

    Tries each tier in order: Firecrawl -> Jina -> Tavily -> urllib.
    Falls through on any failure. Returns empty result only if ALL tiers fail.

    Args:
        url: The URL to scrape.

    Returns:
        Normalized result dict with keys: content, url, provider, title,
        metadata, scraped_at. On total failure, content will be empty and
        provider will be "none".
    """
    if not url or not url.strip():
        return _scrape_result("", "", "none")

    url = url.strip()

    # Tier 1: Firecrawl
    result = _firecrawl_scrape(url)
    if result:
        logger.info(f"scrape_url: Tier 1 (Firecrawl) succeeded for {url}")
        return result

    # Tier 2: Jina AI Reader
    result = _jina_scrape(url)
    if result:
        logger.info(f"scrape_url: Tier 2 (Jina) succeeded for {url}")
        return result

    # Tier 3: Tavily Extract
    result = _tavily_scrape(url)
    if result:
        logger.info(f"scrape_url: Tier 3 (Tavily) succeeded for {url}")
        return result

    # Tier 6: Direct urllib (tiers 4-5 are search-only, skip for scrape)
    result = _urllib_scrape(url)
    if result:
        logger.info(f"scrape_url: Tier 6 (urllib) succeeded for {url}")
        return result

    logger.warning(f"scrape_url: ALL tiers failed for {url}")
    return _scrape_result("", url, "none")


# =============================================================================
# PUBLIC API: search_web
# =============================================================================


def search_web(query: str, num_results: int = 5) -> list[dict[str, Any]]:
    """Search the web using the best available provider with automatic fallback.

    Tries each tier in order: Firecrawl -> Jina -> Tavily -> Serper -> Brave.
    Falls through on any failure. Returns empty list only if ALL tiers fail.

    Args:
        query: Search query string.
        num_results: Maximum number of results to return (default 5).

    Returns:
        List of normalized search result dicts, each with keys: title, url,
        snippet, provider. Empty list on total failure.
    """
    if not query or not query.strip():
        return []

    query = query.strip()

    # Tier 1: Firecrawl
    results = _firecrawl_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 1 (Firecrawl) returned {len(results)} results")
        return results

    # Tier 2: Jina Search
    results = _jina_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 2 (Jina) returned {len(results)} results")
        return results

    # Tier 3: Tavily
    results = _tavily_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 3 (Tavily) returned {len(results)} results")
        return results

    # Tier 4: Serper
    results = _serper_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 4 (Serper) returned {len(results)} results")
        return results

    # Tier 5: Brave
    results = _brave_search(query, num_results)
    if results:
        logger.info(f"search_web: Tier 5 (Brave) returned {len(results)} results")
        return results

    logger.warning(f"search_web: ALL tiers failed for query: {query}")
    return []


# =============================================================================
# PUBLIC API: get_scraper_status
# =============================================================================


def get_scraper_status() -> dict[str, Any]:
    """Return the health and configuration status of all scraping tiers.

    Returns:
        Dict with per-tier status including availability, circuit breaker
        state, request counts, and success rates.
    """
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
            "tier": 2,
            "provider": "jina",
            "has_api_key": bool(JINA_API_KEY),
            "capabilities": ["scrape", "search"],
            "free_tier": "Unlimited basic (rate-limited)",
            **_cb_jina.get_stats(),
        },
        {
            "tier": 3,
            "provider": "tavily",
            "has_api_key": bool(TAVILY_API_KEY),
            "capabilities": ["scrape", "search"],
            "free_tier": "1,000 searches/month",
            **_cb_tavily.get_stats(),
        },
        {
            "tier": 4,
            "provider": "serper",
            "has_api_key": bool(SERPER_API_KEY),
            "capabilities": ["search"],
            "free_tier": "2,500 searches/month",
            **_cb_serper.get_stats(),
        },
        {
            "tier": 5,
            "provider": "brave",
            "has_api_key": bool(BRAVE_API_KEY),
            "capabilities": ["search"],
            "free_tier": "2,000 searches/month",
            **_cb_brave.get_stats(),
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

    # Count available tiers
    available_count = sum(1 for t in tiers if t.get("available", False))
    configured_count = sum(1 for t in tiers if t.get("has_api_key", False))

    return {
        "total_tiers": len(tiers),
        "available_tiers": available_count,
        "configured_tiers": configured_count,
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
    for cb in (_cb_firecrawl, _cb_jina, _cb_tavily, _cb_serper, _cb_brave, _cb_urllib):
        cb.reset()
    return {"status": "all_circuit_breakers_reset"}
