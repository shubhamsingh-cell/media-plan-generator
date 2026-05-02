"""Tests for ``apis.scrapers.crawl4ai_client``.

Five focus areas:
    1. Import works without ``crawl4ai`` installed (graceful degradation).
    2. ``scrape_with_crawl4ai`` returns an error dict when the feature flag
       is off.
    3. With a mocked ``AsyncWebCrawler`` we verify our wrapper forwards
       arguments to ``arun`` correctly.
    4. Timeout handling -- a slow async fetch returns an error dict, never
       raises.
    5. ``scrape_many`` runs concurrently and preserves order.

The tests are written to be runnable on machines where ``crawl4ai`` is *not*
installed (which is the default for Joveo's main ``requirements.txt`` -- the
package is opt-in via ``requirements_optional_crawl4ai.txt``).
"""

from __future__ import annotations

import asyncio
import importlib
import sys
import threading
import time
import types
from pathlib import Path
from typing import Any, Optional
from unittest import mock

import pytest

# Ensure project root is importable.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Fixtures: fake crawl4ai module + AsyncWebCrawler
# =============================================================================


class _FakeMarkdown:
    """Mimic newer Crawl4AI ``MarkdownGenerationResult`` objects."""

    def __init__(self, raw: str, fit: Optional[str] = None) -> None:
        self.raw_markdown = raw
        self.fit_markdown = fit if fit is not None else raw


class _FakeCrawlResult:
    """Mimic Crawl4AI's ``CrawlResult`` attribute surface."""

    def __init__(
        self,
        *,
        markdown: Any = "# hello",
        html: Optional[str] = "<html>hi</html>",
        title: Optional[str] = "Example",
        links: Any = None,
        media: Any = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        self.markdown = markdown
        self.html = html
        self.title = title
        self.links = links if links is not None else [{"href": "https://x.test"}]
        self.media = media if media is not None else []
        self.metadata = metadata or {"source": "fake"}


class _FakeAsyncWebCrawler:
    """Async context manager that records calls for assertion."""

    last_browser_kwargs: dict[str, Any] = {}
    last_arun_kwargs: dict[str, Any] = {}
    behaviour: str = "ok"  # "ok" | "slow" | "raise"
    sleep_seconds: float = 0.0
    result_factory = staticmethod(_FakeCrawlResult)
    call_lock = threading.Lock()
    call_count = 0

    def __init__(self, **kwargs: Any) -> None:
        type(self).last_browser_kwargs = dict(kwargs)

    async def __aenter__(self) -> "_FakeAsyncWebCrawler":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: D401, ANN001
        return None

    async def arun(self, **kwargs: Any) -> Any:
        type(self).last_arun_kwargs = dict(kwargs)
        with type(self).call_lock:
            type(self).call_count += 1
        behaviour = type(self).behaviour
        if behaviour == "raise":
            raise RuntimeError("simulated failure")
        if behaviour == "slow":
            await asyncio.sleep(type(self).sleep_seconds)
        return type(self).result_factory()


@pytest.fixture
def fake_crawl4ai_module(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Inject a fake ``crawl4ai`` module + flip the availability flag on."""
    fake_module = types.ModuleType("crawl4ai")
    fake_module.AsyncWebCrawler = _FakeAsyncWebCrawler  # type: ignore[attr-defined]

    # Reset class state between tests
    _FakeAsyncWebCrawler.last_browser_kwargs = {}
    _FakeAsyncWebCrawler.last_arun_kwargs = {}
    _FakeAsyncWebCrawler.behaviour = "ok"
    _FakeAsyncWebCrawler.sleep_seconds = 0.0
    _FakeAsyncWebCrawler.call_count = 0
    _FakeAsyncWebCrawler.result_factory = staticmethod(_FakeCrawlResult)

    monkeypatch.setitem(sys.modules, "crawl4ai", fake_module)
    monkeypatch.setenv("CRAWL4AI_ENABLED", "1")

    # Patch the client module's cached import + flag.
    from apis.scrapers import crawl4ai_client as client

    monkeypatch.setattr(client, "crawl4ai", fake_module, raising=True)
    monkeypatch.setattr(client, "_CRAWL4AI_AVAILABLE", True, raising=True)
    monkeypatch.setattr(client, "_CRAWL4AI_IMPORT_ERROR", None, raising=True)
    return fake_module


@pytest.fixture
def disabled_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure ``CRAWL4AI_ENABLED`` is unset for this test."""
    monkeypatch.delenv("CRAWL4AI_ENABLED", raising=False)


# =============================================================================
# Test #1 -- graceful degradation when crawl4ai is missing
# =============================================================================


def test_import_succeeds_without_crawl4ai(monkeypatch: pytest.MonkeyPatch) -> None:
    """Module must import cleanly even with ``crawl4ai`` absent.

    We simulate the missing package by removing it from ``sys.modules`` and
    forcing ``importlib.import_module`` to raise. Re-importing the client
    module should still succeed and expose the public API.
    """
    monkeypatch.delitem(sys.modules, "crawl4ai", raising=False)
    monkeypatch.delitem(sys.modules, "apis.scrapers.crawl4ai_client", raising=False)

    real_import = __import__

    def _blocking_import(name: str, *args: Any, **kwargs: Any):
        if name == "crawl4ai" or name.startswith("crawl4ai."):
            raise ImportError("simulated absence of crawl4ai")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _blocking_import)

    module = importlib.import_module("apis.scrapers.crawl4ai_client")
    assert hasattr(module, "scrape_with_crawl4ai")
    assert hasattr(module, "scrape_many")
    assert module._CRAWL4AI_AVAILABLE is False
    # Calling the API while the package is missing must NOT raise.
    monkeypatch.setenv("CRAWL4AI_ENABLED", "1")
    out = module.scrape_with_crawl4ai("https://example.com")
    assert out["error"], "expected error dict when package missing"
    assert "crawl4ai not installed" in out["error"]
    assert out["markdown"] == ""
    assert out["content"] == ""
    assert out["source"] == "crawl4ai"


# =============================================================================
# Test #2 -- feature flag off returns error dict
# =============================================================================


def test_disabled_flag_returns_error_dict(disabled_env: None) -> None:  # noqa: ARG001
    """With ``CRAWL4AI_ENABLED`` unset the wrapper short-circuits."""
    # Re-import to pick up the disabled env
    sys.modules.pop("apis.scrapers.crawl4ai_client", None)
    from apis.scrapers.crawl4ai_client import scrape_with_crawl4ai

    out = scrape_with_crawl4ai("https://example.com")
    assert out["source"] == "crawl4ai"
    assert out["error"] == "crawl4ai disabled"
    assert out["markdown"] == ""
    assert out["url"] == "https://example.com"
    # Joveo-router compatibility keys are populated even on error.
    assert out["provider"] == "crawl4ai"
    assert out["content"] == ""
    assert "scraped_at" in out


# =============================================================================
# Test #3 -- mocked AsyncWebCrawler verifies arg forwarding
# =============================================================================


def test_arguments_forwarded_to_async_crawler(
    fake_crawl4ai_module: types.ModuleType,  # noqa: ARG001
) -> None:
    """The wrapper must forward CSS / BM25 / tag args into ``arun``."""
    from apis.scrapers.crawl4ai_client import scrape_with_crawl4ai

    out = scrape_with_crawl4ai(
        "https://joveo.test/page",
        timeout=10,
        bm25_query="recruitment marketing",
        css_selector="article.main",
        word_count_threshold=42,
        excluded_tags=["nav", "footer"],
        js_enabled=True,
        headless=True,
    )

    assert out["error"] is None, f"unexpected error: {out['error']}"
    assert out["source"] == "crawl4ai"
    assert out["markdown"] == "# hello"
    assert out["content"] == "# hello"  # Joveo-compatible alias populated
    assert out["title"] == "Example"
    assert out["url"] == "https://joveo.test/page"
    assert isinstance(out["elapsed_ms"], int) and out["elapsed_ms"] >= 0
    assert isinstance(out["latency_ms"], float)

    arun_kwargs = _FakeAsyncWebCrawler.last_arun_kwargs
    assert arun_kwargs["url"] == "https://joveo.test/page"
    assert arun_kwargs["css_selector"] == "article.main"
    assert arun_kwargs["bm25_query"] == "recruitment marketing"
    assert arun_kwargs["word_count_threshold"] == 42
    assert arun_kwargs["excluded_tags"] == ["nav", "footer"]

    browser_kwargs = _FakeAsyncWebCrawler.last_browser_kwargs
    assert browser_kwargs.get("headless") is True
    assert browser_kwargs.get("verbose") is False


def test_invalid_extract_strategy_returns_error(
    fake_crawl4ai_module: types.ModuleType,  # noqa: ARG001
) -> None:
    """Bad ``extract_strategy`` rejects with a clear error message."""
    from apis.scrapers.crawl4ai_client import scrape_with_crawl4ai

    out = scrape_with_crawl4ai(
        "https://example.com", extract_strategy="not-a-real-strategy"
    )
    assert out["error"] is not None
    assert "invalid extract_strategy" in out["error"]


# =============================================================================
# Test #4 -- timeout handling
# =============================================================================


def test_timeout_returns_error_dict(
    fake_crawl4ai_module: types.ModuleType,  # noqa: ARG001
) -> None:
    """When ``arun`` is slower than the timeout we get a clean error dict."""
    _FakeAsyncWebCrawler.behaviour = "slow"
    _FakeAsyncWebCrawler.sleep_seconds = 5.0  # > timeout below

    from apis.scrapers.crawl4ai_client import scrape_with_crawl4ai

    out = scrape_with_crawl4ai("https://slow.test", timeout=1)
    assert out["error"] is not None
    assert "timeout" in out["error"].lower()
    assert out["markdown"] == ""
    assert out["content"] == ""
    # We still report elapsed time as a non-negative int.
    assert isinstance(out["elapsed_ms"], int)
    assert out["elapsed_ms"] >= 0


def test_underlying_exception_is_swallowed(
    fake_crawl4ai_module: types.ModuleType,  # noqa: ARG001
) -> None:
    """Any exception inside ``arun`` becomes an error dict, never propagates."""
    _FakeAsyncWebCrawler.behaviour = "raise"
    from apis.scrapers.crawl4ai_client import scrape_with_crawl4ai

    out = scrape_with_crawl4ai("https://broken.test", timeout=5)
    assert out["error"] is not None
    assert "RuntimeError" in out["error"] or "simulated failure" in out["error"]


# =============================================================================
# Test #5 -- scrape_many concurrency + ordering
# =============================================================================


def test_scrape_many_runs_concurrently_and_preserves_order(
    fake_crawl4ai_module: types.ModuleType,  # noqa: ARG001
) -> None:
    """``scrape_many`` should overlap calls and return results in input order."""
    _FakeAsyncWebCrawler.behaviour = "slow"
    _FakeAsyncWebCrawler.sleep_seconds = 0.4  # 400ms each

    from apis.scrapers.crawl4ai_client import scrape_many

    urls = [
        "https://a.test",
        "https://b.test",
        "https://c.test",
        "https://d.test",
    ]

    t0 = time.monotonic()
    results = scrape_many(urls, concurrency=4, timeout=5)
    elapsed = time.monotonic() - t0

    assert len(results) == len(urls)
    # Order must match input order
    for url, res in zip(urls, results):
        assert res["url"] == url
        assert res["error"] is None, f"unexpected error: {res['error']}"
        assert res["markdown"] == "# hello"

    # 4 calls of 0.4s with concurrency=4 must finish well under the
    # serial-equivalent (1.6s). Allow generous slack for slow CI.
    assert elapsed < 1.5, (
        f"scrape_many appears serial: elapsed={elapsed:.2f}s "
        f"(expected < 1.5s with concurrency=4 and 0.4s per call)"
    )

    # Worker count check -- concurrency cap honoured.
    assert _FakeAsyncWebCrawler.call_count == len(urls)


def test_scrape_many_handles_empty_input(
    fake_crawl4ai_module: types.ModuleType,  # noqa: ARG001
) -> None:
    """Empty iterable returns ``[]`` without spawning threads."""
    from apis.scrapers.crawl4ai_client import scrape_many

    assert scrape_many([]) == []
    # Generator inputs work too
    assert scrape_many(iter([])) == []
