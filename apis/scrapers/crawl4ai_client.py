"""crawl4ai_client.py -- Crawl4AI synchronous wrapper for Joveo Nova.

Crawl4AI (https://github.com/unclecode/crawl4ai, 61K+ GitHub stars) is the
open-source alternative to Firecrawl. It is fully self-hostable, async,
includes a BM25 relevance filter, and produces clean markdown.

This module exposes a *synchronous* interface that fits Joveo's existing
scraper protocol (mirrors ``web_scraper_router.scrape_url`` -- see top-level
``web_scraper_router.py``). Joveo code is largely synchronous (stdlib
``urllib.request`` + threaded ``HTTPServer``), so we hide all asyncio plumbing
behind ``asyncio.run`` executed in a dedicated worker thread.

Design constraints honoured here:
    1. **Graceful degradation** -- if ``crawl4ai`` is not installed or the
       ``CRAWL4AI_ENABLED`` feature flag is off, return a structured error dict
       instead of raising. Callers never see ``ImportError``.
    2. **Thread safety** -- every call gets its own asyncio event loop in a
       dedicated thread (via ``concurrent.futures.ThreadPoolExecutor``). Safe
       to invoke from gunicorn worker threads or gevent greenlets without
       deadlocking on a shared loop.
    3. **Resource cleanup** -- ``AsyncWebCrawler`` is always exited via
       ``async with`` (or explicit ``aclose()`` in the manual fallback) so
       Playwright browsers close even on exception.
    4. **Drop-in compatibility** -- the result dict carries both the
       *user-spec* keys (``markdown``, ``html``, ``title``, ``links``,
       ``media``, ``metadata``, ``source``, ``elapsed_ms``, ``error``) and the
       canonical Joveo keys (``content``, ``provider``, ``latency_ms``,
       ``scraped_at``) so existing consumers of ``scrape_url`` keep working
       when this is wired in as an additional tier.

Public API:
    scrape_with_crawl4ai(url, *, timeout=30, ...) -> dict
    scrape_many(urls, *, concurrency=5, **kwargs) -> list[dict]

The module never raises during import even if ``crawl4ai`` is missing -- the
absence is detected lazily inside the wrapper.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import threading
import time
from typing import Any, Iterable, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Feature flag -- callers can roll this out gradually. Anything other than the
# truthy strings below disables the wrapper at runtime, matching Render-style
# env-var conventions used elsewhere in the project (see ``app.py``).
_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled"})


def _is_enabled() -> bool:
    """Read the ``CRAWL4AI_ENABLED`` flag at call time (not import time)."""
    return os.environ.get("CRAWL4AI_ENABLED", "").strip().lower() in _TRUE_VALUES


# Default exclusion list -- noise-bearing tags Crawl4AI itself recommends
# pruning before BM25 / markdown extraction.
_DEFAULT_EXCLUDED_TAGS: tuple[str, ...] = ("nav", "footer", "header", "aside")

# Max concurrency for ``scrape_many``. Hard cap to protect Playwright's
# memory-hungry browser pool on small Render instances.
_DEFAULT_CONCURRENCY: int = 5
_MAX_CONCURRENCY: int = 15


# =============================================================================
# OPTIONAL DEPENDENCY DETECTION (NEVER raise at import time)
# =============================================================================

try:  # pragma: no cover - import guard exercised by tests
    import crawl4ai  # type: ignore[import-not-found]

    _CRAWL4AI_AVAILABLE = True
    _CRAWL4AI_IMPORT_ERROR: Optional[str] = None
except Exception as _exc:  # noqa: BLE001 -- want to swallow ANY import-time error
    crawl4ai = None  # type: ignore[assignment]
    _CRAWL4AI_AVAILABLE = False
    _CRAWL4AI_IMPORT_ERROR = f"{type(_exc).__name__}: {_exc}"
    logger.info(
        "crawl4ai not importable (%s); apis.scrapers.crawl4ai_client will "
        "return error dicts until the package is installed.",
        _CRAWL4AI_IMPORT_ERROR,
    )


# =============================================================================
# RESULT NORMALIZATION
# =============================================================================


def _empty_result(
    url: str,
    *,
    error: str,
    elapsed_ms: int = 0,
) -> dict[str, Any]:
    """Build a normalized error result.

    Returns a dict that satisfies BOTH the user-facing schema requested in the
    Crawl4AI spec and the canonical ``web_scraper_router._scrape_result``
    schema, so the wrapper is drop-in compatible with existing consumers.
    """
    return {
        # Crawl4AI-spec keys
        "url": url,
        "markdown": "",
        "html": None,
        "title": None,
        "links": [],
        "media": [],
        "metadata": {},
        "source": "crawl4ai",
        "elapsed_ms": int(elapsed_ms),
        "error": error,
        # Joveo router-compatible keys
        "content": "",
        "provider": "crawl4ai",
        "latency_ms": float(elapsed_ms),
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _success_result(
    url: str,
    *,
    markdown: str,
    html: Optional[str],
    title: Optional[str],
    links: List[dict[str, Any]],
    media: List[dict[str, Any]],
    metadata: dict[str, Any],
    elapsed_ms: int,
) -> dict[str, Any]:
    """Build a normalized success result with both schemas populated."""
    return {
        "url": url,
        "markdown": markdown or "",
        "html": html,
        "title": title,
        "links": links or [],
        "media": media or [],
        "metadata": metadata or {},
        "source": "crawl4ai",
        "elapsed_ms": int(elapsed_ms),
        "error": None,
        "content": markdown or "",
        "provider": "crawl4ai",
        "latency_ms": float(elapsed_ms),
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _coerce_list_of_dicts(value: Any) -> List[dict[str, Any]]:
    """Best-effort coerce Crawl4AI's link/media payload to ``list[dict]``.

    Different Crawl4AI versions return either ``list[dict]``, ``dict[str, list]``
    (e.g., ``{"internal": [...], "external": [...]}``), or pydantic objects
    with ``.dict()``/``.model_dump()``. We normalize aggressively so callers
    get the same shape regardless of upstream version drift.
    """
    if value is None:
        return []
    if isinstance(value, dict):
        flattened: List[dict[str, Any]] = []
        for bucket in value.values():
            if isinstance(bucket, list):
                flattened.extend(_coerce_list_of_dicts(bucket))
        return flattened
    if not isinstance(value, list):
        return []
    out: List[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            out.append(item)
            continue
        # pydantic v2 or v1 style
        for attr in ("model_dump", "dict"):
            method = getattr(item, attr, None)
            if callable(method):
                try:
                    coerced = method()
                except Exception:  # noqa: BLE001
                    coerced = None
                if isinstance(coerced, dict):
                    out.append(coerced)
                    break
        else:
            # Last resort -- string-coerce so we never drop the item silently.
            out.append({"value": str(item)})
    return out


# =============================================================================
# ASYNC CORE -- one event loop per call, isolated in a worker thread
# =============================================================================


async def _async_scrape(
    url: str,
    *,
    timeout: int,
    bm25_query: Optional[str],
    css_selector: Optional[str],
    extract_strategy: str,
    word_count_threshold: int,
    excluded_tags: List[str],
    js_enabled: bool,
    headless: bool,
) -> dict[str, Any]:
    """Run a single Crawl4AI fetch inside an active event loop.

    Uses Crawl4AI's ``AsyncWebCrawler`` as a context manager so the underlying
    Playwright browser is always closed, even when ``arun`` raises.
    """
    if crawl4ai is None:  # pragma: no cover - guarded by caller
        raise RuntimeError("crawl4ai unavailable")

    # Lazily import the symbols we need; AsyncWebCrawler / CrawlerRunConfig
    # have moved between sub-modules across versions, so try a couple of
    # paths before giving up.
    AsyncWebCrawler = getattr(crawl4ai, "AsyncWebCrawler", None)
    if AsyncWebCrawler is None:
        from crawl4ai.async_webcrawler import (  # type: ignore[import-not-found]
            AsyncWebCrawler,
        )

    # Build optional run-config; absent in older versions, so we tolerate it.
    run_kwargs: dict[str, Any] = {
        "url": url,
        "word_count_threshold": word_count_threshold,
        "excluded_tags": excluded_tags,
        "bypass_cache": False,
    }
    if css_selector:
        run_kwargs["css_selector"] = css_selector
    if bm25_query and extract_strategy in {"default", "llm"}:
        # Hand BM25 to Crawl4AI's content filter when supported; older versions
        # accept the kwarg, newer versions take a strategy object. Best-effort.
        run_kwargs["bm25_query"] = bm25_query

    browser_kwargs: dict[str, Any] = {
        "headless": headless,
        "verbose": False,
    }
    # ``js_enabled=False`` is honoured by passing ``--disable-javascript`` via
    # browser args when the kwarg isn't natively supported.
    if not js_enabled:
        browser_kwargs["browser_type"] = "chromium"
        browser_kwargs.setdefault("extra_args", []).append("--disable-javascript")

    try:
        async with AsyncWebCrawler(**browser_kwargs) as crawler:
            result = await asyncio.wait_for(crawler.arun(**run_kwargs), timeout=timeout)
    except TypeError:
        # Older API -- AsyncWebCrawler() takes no kwargs; retry minimal.
        async with AsyncWebCrawler() as crawler:
            result = await asyncio.wait_for(crawler.arun(url=url), timeout=timeout)

    return _coerce_crawl4ai_result(result, url=url)


def _coerce_crawl4ai_result(result: Any, *, url: str) -> dict[str, Any]:
    """Map Crawl4AI's ``CrawlResult`` object to our normalized schema."""
    if result is None:
        return {
            "markdown": "",
            "html": None,
            "title": None,
            "links": [],
            "media": [],
            "metadata": {"reason": "empty_result"},
        }

    # Crawl4AI's CrawlResult exposes attribute access; fall back to dict.
    def _get(name: str, default: Any = None) -> Any:
        if hasattr(result, name):
            return getattr(result, name)
        if isinstance(result, dict):
            return result.get(name, default)
        return default

    markdown_obj = _get("markdown") or _get("markdown_v2") or ""
    # Newer versions wrap markdown in a ``MarkdownGenerationResult`` object
    # exposing ``raw_markdown`` / ``fit_markdown``. Prefer the BM25-filtered
    # ``fit_markdown`` when present, else raw.
    if hasattr(markdown_obj, "fit_markdown") and getattr(
        markdown_obj, "fit_markdown", None
    ):
        markdown = str(markdown_obj.fit_markdown)
    elif hasattr(markdown_obj, "raw_markdown"):
        markdown = str(getattr(markdown_obj, "raw_markdown", "") or "")
    else:
        markdown = str(markdown_obj or "")

    metadata = _get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {"raw": str(metadata)}

    title = _get("title") or metadata.get("title")
    if title is not None:
        title = str(title)

    return {
        "markdown": markdown,
        "html": _get("html") if _get("html") else None,
        "title": title,
        "links": _coerce_list_of_dicts(_get("links")),
        "media": _coerce_list_of_dicts(_get("media")),
        "metadata": metadata,
    }


# =============================================================================
# SYNC EXECUTION -- bridge async core into Joveo's threaded synchronous code
# =============================================================================


def _run_in_dedicated_loop(coro_factory, *, timeout: int) -> Any:
    """Execute ``coro_factory()`` in a fresh event loop on a worker thread.

    Using a dedicated thread + ``asyncio.new_event_loop`` per call is what
    makes this safe under gunicorn (whose workers may already have a running
    loop in gevent mode). It avoids the "cannot run nested event loops" error
    and prevents shared-loop deadlocks.

    The outer ``timeout`` here is a *belt-and-braces* guard around the inner
    ``asyncio.wait_for`` -- if the worker thread is wedged for any reason,
    the calling thread still returns control to the gunicorn worker.
    """

    def _runner() -> Any:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro_factory())
        finally:
            try:
                # Cancel anything still pending so the loop can shut down.
                pending = asyncio.all_tasks(loop=loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True)
                    )
            except Exception:  # noqa: BLE001
                logger.debug("crawl4ai loop drain failed", exc_info=True)
            finally:
                asyncio.set_event_loop(None)
                loop.close()

    # ``ThreadPoolExecutor`` per call -- cheap, isolates failures, avoids
    # stockpiling threads. Outer timeout is timeout + 5s grace for shutdown.
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="crawl4ai"
    ) as pool:
        future = pool.submit(_runner)
        try:
            return future.result(timeout=timeout + 5)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise asyncio.TimeoutError(
                f"crawl4ai outer timeout after {timeout + 5}s"
            ) from exc


# =============================================================================
# PUBLIC API
# =============================================================================


def scrape_with_crawl4ai(
    url: str,
    *,
    timeout: int = 30,
    bm25_query: Optional[str] = None,
    css_selector: Optional[str] = None,
    extract_strategy: str = "default",
    word_count_threshold: int = 10,
    excluded_tags: Optional[List[str]] = None,
    js_enabled: bool = True,
    headless: bool = True,
) -> dict[str, Any]:
    """Scrape ``url`` via Crawl4AI and return a normalized result dict.

    Args:
        url: Page to fetch. Empty / whitespace returns an error result.
        timeout: Per-request wall-clock timeout in seconds (default 30).
        bm25_query: Optional BM25 relevance query passed to Crawl4AI's
            content filter. Improves signal/noise on long pages.
        css_selector: Restrict extraction to nodes matching this selector.
        extract_strategy: ``"default"`` | ``"llm"`` | ``"css"``. ``"llm"`` only
            takes effect if Crawl4AI's LLM strategy is configured upstream.
        word_count_threshold: Skip blocks with fewer words than this.
        excluded_tags: Tags to drop before extraction. Defaults to
            ``("nav", "footer", "header", "aside")``.
        js_enabled: When ``False``, Playwright runs with JavaScript disabled.
        headless: Run Playwright headless (default ``True``).

    Returns:
        Normalized dict carrying both Crawl4AI-spec keys and Joveo
        ``web_scraper_router``-compatible keys::

            {
                "url": str,
                "markdown": str,         # cleaned markdown
                "html": str | None,
                "title": str | None,
                "links": list[dict],
                "media": list[dict],
                "metadata": dict,
                "source": "crawl4ai",
                "elapsed_ms": int,
                "error": str | None,
                # Joveo-router-compatible aliases:
                "content": str,
                "provider": "crawl4ai",
                "latency_ms": float,
                "scraped_at": str,       # UTC ISO-8601
            }

        On any failure (disabled flag, missing package, timeout, network
        error) the dict has ``error`` populated and ``markdown``/``content``
        empty -- never raises.
    """
    if not url or not url.strip():
        return _empty_result("", error="empty url")

    url = url.strip()

    if not _is_enabled():
        return _empty_result(url, error="crawl4ai disabled")

    if not _CRAWL4AI_AVAILABLE:
        detail = _CRAWL4AI_IMPORT_ERROR or "ImportError"
        return _empty_result(
            url,
            error=(
                "crawl4ai not installed; pip install crawl4ai "
                f"(import error: {detail})"
            ),
        )

    if extract_strategy not in {"default", "llm", "css"}:
        return _empty_result(
            url,
            error=f"invalid extract_strategy: {extract_strategy!r}",
        )

    tags = list(excluded_tags) if excluded_tags else list(_DEFAULT_EXCLUDED_TAGS)
    safe_timeout = max(1, int(timeout))

    t0 = time.monotonic()
    try:
        payload = _run_in_dedicated_loop(
            lambda: _async_scrape(
                url,
                timeout=safe_timeout,
                bm25_query=bm25_query,
                css_selector=css_selector,
                extract_strategy=extract_strategy,
                word_count_threshold=word_count_threshold,
                excluded_tags=tags,
                js_enabled=js_enabled,
                headless=headless,
            ),
            timeout=safe_timeout,
        )
    except asyncio.TimeoutError as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.warning("crawl4ai timeout for %s after %sms: %s", url, elapsed, exc)
        return _empty_result(
            url, error=f"timeout after {safe_timeout}s", elapsed_ms=elapsed
        )
    except ImportError as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.error("crawl4ai sub-module import failed: %s", exc, exc_info=True)
        return _empty_result(
            url,
            error=f"crawl4ai sub-module import failed: {exc}",
            elapsed_ms=elapsed,
        )
    except Exception as exc:  # noqa: BLE001 -- top-level wrapper
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.error("crawl4ai scrape failed for %s: %s", url, exc, exc_info=True)
        return _empty_result(
            url,
            error=f"{type(exc).__name__}: {exc}",
            elapsed_ms=elapsed,
        )

    elapsed = int((time.monotonic() - t0) * 1000)
    return _success_result(
        url,
        markdown=payload.get("markdown", "") or "",
        html=payload.get("html"),
        title=payload.get("title"),
        links=payload.get("links") or [],
        media=payload.get("media") or [],
        metadata=payload.get("metadata") or {},
        elapsed_ms=elapsed,
    )


def scrape_many(
    urls: Iterable[str],
    *,
    concurrency: int = _DEFAULT_CONCURRENCY,
    **kwargs: Any,
) -> List[dict[str, Any]]:
    """Batch-scrape ``urls`` with bounded thread concurrency.

    Each URL is dispatched to its own ``scrape_with_crawl4ai`` call, which
    in turn runs in its own event loop on its own worker thread. This means
    concurrency here is enforced at the *thread* level via
    ``ThreadPoolExecutor``; the underlying browser pool is the bottleneck.

    Args:
        urls: Iterable of URLs to scrape.
        concurrency: Max concurrent scrapes (clamped to ``1..15``).
        **kwargs: Forwarded verbatim to ``scrape_with_crawl4ai``.

    Returns:
        List of result dicts in the same order as ``urls``. Each entry is a
        successful or error dict -- the function never raises.
    """
    url_list = [u for u in urls if isinstance(u, str)]
    if not url_list:
        return []

    pool_size = max(1, min(_MAX_CONCURRENCY, int(concurrency)))
    results: List[Optional[dict[str, Any]]] = [None] * len(url_list)
    lock = threading.Lock()

    def _worker(index: int, target: str) -> None:
        try:
            res = scrape_with_crawl4ai(target, **kwargs)
        except Exception as exc:  # noqa: BLE001 -- defensive backstop
            logger.error(
                "scrape_many worker failed for %s: %s", target, exc, exc_info=True
            )
            res = _empty_result(target, error=f"{type(exc).__name__}: {exc}")
        with lock:
            results[index] = res

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=pool_size, thread_name_prefix="crawl4ai-batch"
    ) as pool:
        futures = [pool.submit(_worker, i, u) for i, u in enumerate(url_list)]
        for fut in concurrent.futures.as_completed(futures):
            # ``_worker`` itself swallows exceptions; this loop just drains.
            try:
                fut.result()
            except Exception:  # noqa: BLE001 - paranoia
                logger.debug("scrape_many future raised", exc_info=True)

    # Replace any leftover None (shouldn't happen) with error dicts.
    final: List[dict[str, Any]] = []
    for idx, item in enumerate(results):
        if item is None:
            final.append(_empty_result(url_list[idx], error="worker did not run"))
        else:
            final.append(item)
    return final


__all__ = ["scrape_many", "scrape_with_crawl4ai"]
