"""Tests for web_scraper_router.py -- multi-tier web scraping fallback system.

Tests circuit breaker behavior, tier fallback logic, normalized outputs,
and the public API surface. Uses unittest.mock to avoid real HTTP calls.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Ensure the project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import web_scraper_router as router
from web_scraper_router import (
    CircuitBreaker,
    scrape_url,
    search_web,
    get_scraper_status,
    reset_circuit_breakers,
    _scrape_result,
    _search_result,
    _HTMLTextExtractor,
)


# =============================================================================
# CircuitBreaker tests
# =============================================================================


class TestCircuitBreaker:
    """Tests for the CircuitBreaker class."""

    def test_initial_state_available(self) -> None:
        """New circuit breaker should be available."""
        cb = CircuitBreaker("test_tier")
        assert cb.is_available is True
        assert cb.remaining_cooldown == 0

    def test_trip_disables_breaker(self) -> None:
        """Tripping the breaker should make it unavailable."""
        cb = CircuitBreaker("test_tier", cooldown=60)
        cb.trip("test trip")
        assert cb.is_available is False
        assert cb.remaining_cooldown > 0
        assert cb.remaining_cooldown <= 60

    def test_cooldown_expiry(self) -> None:
        """Breaker should re-enable after cooldown expires."""
        cb = CircuitBreaker("test_tier", cooldown=1)
        cb.trip("test trip")
        assert cb.is_available is False
        time.sleep(1.1)
        assert cb.is_available is True

    def test_record_success(self) -> None:
        """record_success should increment counters."""
        cb = CircuitBreaker("test_tier")
        cb.record_success()
        cb.record_success()
        stats = cb.get_stats()
        assert stats["total_requests"] == 2
        assert stats["successful_requests"] == 2
        assert stats["failed_requests"] == 0
        assert stats["success_rate_pct"] == 100.0

    def test_record_failure(self) -> None:
        """record_failure should increment failure counter without tripping."""
        cb = CircuitBreaker("test_tier")
        cb.record_success()
        cb.record_failure("some error")
        stats = cb.get_stats()
        assert stats["total_requests"] == 2
        assert stats["successful_requests"] == 1
        assert stats["failed_requests"] == 1
        assert stats["success_rate_pct"] == 50.0
        # Should NOT be tripped
        assert cb.is_available is True

    def test_reset(self) -> None:
        """reset should clear all state."""
        cb = CircuitBreaker("test_tier", cooldown=3600)
        cb.record_success()
        cb.record_failure("error")
        cb.trip("forced")
        cb.reset()
        stats = cb.get_stats()
        assert stats["total_requests"] == 0
        assert stats["successful_requests"] == 0
        assert stats["failed_requests"] == 0
        assert cb.is_available is True

    def test_get_stats_structure(self) -> None:
        """get_stats should return all expected keys."""
        cb = CircuitBreaker("test_tier")
        stats = cb.get_stats()
        expected_keys = {
            "name",
            "available",
            "remaining_cooldown_seconds",
            "total_requests",
            "successful_requests",
            "failed_requests",
            "success_rate_pct",
            "last_error",
            "last_error_time",
        }
        assert set(stats.keys()) == expected_keys
        assert stats["name"] == "test_tier"

    def test_thread_safety(self) -> None:
        """Circuit breaker operations should be thread-safe."""
        import threading

        cb = CircuitBreaker("threaded_test")
        errors: list[str] = []

        def hammer(count: int) -> None:
            try:
                for _ in range(count):
                    cb.record_success()
                    cb.record_failure("err")
                    _ = cb.is_available
                    _ = cb.get_stats()
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=hammer, args=(50,)) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        stats = cb.get_stats()
        assert (
            stats["total_requests"] == 1000
        )  # 10 threads * 50 * 2 (success + failure)


# =============================================================================
# Normalized result helpers
# =============================================================================


class TestNormalizedResults:
    """Tests for _scrape_result and _search_result helpers."""

    def test_scrape_result_fields(self) -> None:
        """_scrape_result should return all expected keys."""
        result = _scrape_result(
            "content here", "https://example.com", "test_provider", "Title"
        )
        assert result["content"] == "content here"
        assert result["url"] == "https://example.com"
        assert result["provider"] == "test_provider"
        assert result["title"] == "Title"
        assert "scraped_at" in result
        assert isinstance(result["metadata"], dict)

    def test_scrape_result_defaults(self) -> None:
        """_scrape_result should handle empty/None values."""
        result = _scrape_result("", "", "none")
        assert result["content"] == ""
        assert result["title"] == ""
        assert result["metadata"] == {}

    def test_search_result_fields(self) -> None:
        """_search_result should return all expected keys."""
        result = _search_result(
            "Title", "https://example.com", "snippet text", "test_provider"
        )
        assert result["title"] == "Title"
        assert result["url"] == "https://example.com"
        assert result["snippet"] == "snippet text"
        assert result["provider"] == "test_provider"


# =============================================================================
# HTML Text Extractor
# =============================================================================


class TestHTMLTextExtractor:
    """Tests for the stdlib HTML parser used in tier 6."""

    def test_extracts_paragraph_text(self) -> None:
        """Should extract text from <p> tags."""
        parser = _HTMLTextExtractor()
        parser.feed("<html><body><p>Hello world</p></body></html>")
        assert "Hello world" in parser.texts

    def test_extracts_headings(self) -> None:
        """Should extract text from heading tags."""
        parser = _HTMLTextExtractor()
        parser.feed("<h1>Main Title</h1><h2>Subtitle</h2>")
        assert "Main Title" in parser.texts
        assert "Subtitle" in parser.texts

    def test_skips_script_and_style(self) -> None:
        """Should skip content inside <script> and <style> tags."""
        parser = _HTMLTextExtractor()
        parser.feed(
            "<p>Visible</p><script>var x = 1;</script>"
            "<style>.hidden{}</style><p>Also visible</p>"
        )
        assert "Visible" in parser.texts
        assert "Also visible" in parser.texts
        assert not any("var x" in t for t in parser.texts)

    def test_extracts_title(self) -> None:
        """Should extract the page title."""
        parser = _HTMLTextExtractor()
        parser.feed(
            "<html><head><title>Page Title</title></head><body><p>Text</p></body></html>"
        )
        assert parser.title == "Page Title"

    def test_handles_nested_tags(self) -> None:
        """Should handle nested visible tags."""
        parser = _HTMLTextExtractor()
        parser.feed("<p>Outer <span>inner</span> text</p>")
        # The span content should be captured (span is in VISIBLE_TAGS)
        combined = " ".join(parser.texts)
        assert "inner" in combined

    def test_empty_html(self) -> None:
        """Should handle empty HTML gracefully."""
        parser = _HTMLTextExtractor()
        parser.feed("")
        assert parser.texts == []
        assert parser.title == ""


# =============================================================================
# Tier function tests (mocked HTTP)
# =============================================================================


class TestFirecrawlTier:
    """Tests for Firecrawl tier functions."""

    def setup_method(self) -> None:
        """Reset circuit breaker before each test."""
        router._cb_firecrawl.reset()

    @patch.object(router, "FIRECRAWL_API_KEY", "")
    def test_no_api_key_returns_none(self) -> None:
        """Should return None if no API key is configured."""
        result = router._firecrawl_scrape("https://example.com")
        assert result is None

    @patch.object(router, "FIRECRAWL_API_KEY", "test-key")
    @patch("web_scraper_router.urlopen")
    def test_successful_scrape(self, mock_urlopen: MagicMock) -> None:
        """Should return normalized result on successful scrape."""
        response_data = {
            "success": True,
            "data": {
                "markdown": "# Test Content\nHello world",
                "metadata": {"title": "Test Page"},
            },
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = router._firecrawl_scrape("https://example.com")
        assert result is not None
        assert result["provider"] == "firecrawl"
        assert "Hello world" in result["content"]

    @patch.object(router, "FIRECRAWL_API_KEY", "test-key")
    @patch("web_scraper_router.urlopen")
    def test_402_trips_circuit_breaker(self, mock_urlopen: MagicMock) -> None:
        """402 error should trip the circuit breaker."""
        from urllib.error import HTTPError

        mock_urlopen.side_effect = HTTPError(
            "https://api.firecrawl.dev/v1/scrape", 402, "Payment Required", {}, None
        )
        result = router._firecrawl_scrape("https://example.com")
        assert result is None
        assert router._cb_firecrawl.is_available is False


class TestJinaTier:
    """Tests for Jina AI Reader tier."""

    def setup_method(self) -> None:
        """Reset circuit breaker before each test."""
        router._cb_jina.reset()

    @patch("web_scraper_router.urlopen")
    def test_successful_scrape(self, mock_urlopen: MagicMock) -> None:
        """Should return normalized result on Jina scrape."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = (
            "# Page Title\n\nThis is the extracted content from the page."
        ).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = router._jina_scrape("https://example.com")
        assert result is not None
        assert result["provider"] == "jina"
        assert "extracted content" in result["content"]
        assert result["title"] == "Page Title"

    @patch("web_scraper_router.urlopen")
    def test_empty_response_returns_none(self, mock_urlopen: MagicMock) -> None:
        """Short responses should be rejected."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"short"
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = router._jina_scrape("https://example.com")
        assert result is None


class TestTavilyTier:
    """Tests for Tavily tier."""

    def setup_method(self) -> None:
        """Reset circuit breaker before each test."""
        router._cb_tavily.reset()

    @patch.object(router, "TAVILY_API_KEY", "")
    def test_no_api_key_returns_none(self) -> None:
        """Should return None without API key."""
        result = router._tavily_search("test query")
        assert result is None

    @patch.object(router, "TAVILY_API_KEY", "test-key")
    @patch("web_scraper_router.urlopen")
    def test_successful_search(self, mock_urlopen: MagicMock) -> None:
        """Should return normalized search results."""
        response_data = {
            "results": [
                {
                    "title": "Result 1",
                    "url": "https://example.com/1",
                    "content": "First result snippet",
                },
                {
                    "title": "Result 2",
                    "url": "https://example.com/2",
                    "content": "Second result snippet",
                },
            ]
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        results = router._tavily_search("test query", 5)
        assert results is not None
        assert len(results) == 2
        assert results[0]["provider"] == "tavily"
        assert results[0]["title"] == "Result 1"


class TestSerperTier:
    """Tests for Serper tier."""

    def setup_method(self) -> None:
        """Reset circuit breaker."""
        router._cb_serper.reset()

    @patch.object(router, "SERPER_API_KEY", "")
    def test_no_api_key_returns_none(self) -> None:
        """Should return None without API key."""
        result = router._serper_search("test query")
        assert result is None

    @patch.object(router, "SERPER_API_KEY", "test-key")
    @patch("web_scraper_router.urlopen")
    def test_successful_search(self, mock_urlopen: MagicMock) -> None:
        """Should parse Serper organic results."""
        response_data = {
            "organic": [
                {
                    "title": "Google Result",
                    "link": "https://example.com/google",
                    "snippet": "A Google search result",
                },
            ]
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        results = router._serper_search("test query", 5)
        assert results is not None
        assert len(results) == 1
        assert results[0]["provider"] == "serper"
        assert results[0]["url"] == "https://example.com/google"


class TestBraveTier:
    """Tests for Brave Search tier."""

    def setup_method(self) -> None:
        """Reset circuit breaker."""
        router._cb_brave.reset()

    @patch.object(router, "BRAVE_API_KEY", "")
    def test_no_api_key_returns_none(self) -> None:
        """Should return None without API key."""
        result = router._brave_search("test query")
        assert result is None

    @patch.object(router, "BRAVE_API_KEY", "test-key")
    @patch("web_scraper_router.urlopen")
    def test_successful_search(self, mock_urlopen: MagicMock) -> None:
        """Should parse Brave web results."""
        response_data = {
            "web": {
                "results": [
                    {
                        "title": "Brave Result",
                        "url": "https://brave.com/result",
                        "description": "A Brave search result",
                    },
                ]
            }
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(response_data).encode("utf-8")
        mock_resp.headers = MagicMock()
        mock_resp.headers.get.return_value = None  # No gzip
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        results = router._brave_search("test query", 5)
        assert results is not None
        assert len(results) == 1
        assert results[0]["provider"] == "brave"


class TestUrllibTier:
    """Tests for direct urllib tier."""

    def setup_method(self) -> None:
        """Reset circuit breaker."""
        router._cb_urllib.reset()

    @patch("web_scraper_router.urlopen")
    def test_successful_html_scrape(self, mock_urlopen: MagicMock) -> None:
        """Should extract text from HTML."""
        html_content = """
        <html>
        <head><title>Test Page</title></head>
        <body>
            <h1>Main Heading</h1>
            <p>This is a paragraph with useful content about the topic.</p>
            <p>Another paragraph with more details.</p>
            <script>var x = 1;</script>
        </body>
        </html>
        """
        mock_resp = MagicMock()
        mock_resp.read.return_value = html_content.encode("utf-8")
        mock_resp.headers = MagicMock()
        mock_resp.headers.get.side_effect = lambda key, default=None: {
            "Content-Type": "text/html; charset=utf-8",
            "Content-Encoding": None,
        }.get(key, default)
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = router._urllib_scrape("https://example.com")
        assert result is not None
        assert result["provider"] == "urllib_direct"
        assert "Main Heading" in result["content"]
        assert "useful content" in result["content"]


# =============================================================================
# Public API: scrape_url fallback tests
# =============================================================================


class TestScrapeUrlFallback:
    """Tests for the scrape_url() public API with tier fallback."""

    def setup_method(self) -> None:
        """Reset all circuit breakers."""
        reset_circuit_breakers()

    def test_empty_url_returns_none_provider(self) -> None:
        """Empty URL should return provider='none'."""
        result = scrape_url("")
        assert result["provider"] == "none"
        assert result["content"] == ""

    @patch("web_scraper_router._firecrawl_scrape")
    def test_tier1_success_short_circuits(self, mock_fc: MagicMock) -> None:
        """When tier 1 succeeds, lower tiers should not be called."""
        mock_fc.return_value = _scrape_result(
            "Firecrawl content", "https://example.com", "firecrawl"
        )

        with patch("web_scraper_router._jina_scrape") as mock_jina:
            result = scrape_url("https://example.com")
            assert result["provider"] == "firecrawl"
            mock_jina.assert_not_called()

    @patch("web_scraper_router._firecrawl_scrape", return_value=None)
    @patch("web_scraper_router._jina_scrape")
    def test_falls_through_to_tier2(
        self, mock_jina: MagicMock, mock_fc: MagicMock
    ) -> None:
        """When tier 1 fails, should try tier 2."""
        mock_jina.return_value = _scrape_result(
            "Jina content", "https://example.com", "jina"
        )

        result = scrape_url("https://example.com")
        assert result["provider"] == "jina"

    @patch("web_scraper_router._firecrawl_scrape", return_value=None)
    @patch("web_scraper_router._jina_scrape", return_value=None)
    @patch("web_scraper_router._tavily_scrape", return_value=None)
    @patch("web_scraper_router._urllib_scrape", return_value=None)
    def test_all_tiers_fail(
        self,
        mock_urllib: MagicMock,
        mock_tavily: MagicMock,
        mock_jina: MagicMock,
        mock_fc: MagicMock,
    ) -> None:
        """When all tiers fail, should return provider='none'."""
        result = scrape_url("https://example.com")
        assert result["provider"] == "none"
        assert result["content"] == ""


# =============================================================================
# Public API: search_web fallback tests
# =============================================================================


class TestSearchWebFallback:
    """Tests for the search_web() public API with tier fallback."""

    def setup_method(self) -> None:
        """Reset all circuit breakers."""
        reset_circuit_breakers()

    def test_empty_query_returns_empty(self) -> None:
        """Empty query should return empty list."""
        results = search_web("")
        assert results == []

    @patch("web_scraper_router._firecrawl_search")
    def test_tier1_success(self, mock_fc: MagicMock) -> None:
        """When tier 1 returns results, should use them."""
        mock_fc.return_value = [
            _search_result("Result 1", "https://example.com", "snippet", "firecrawl")
        ]
        results = search_web("test query")
        assert len(results) == 1
        assert results[0]["provider"] == "firecrawl"

    @patch("web_scraper_router._firecrawl_search", return_value=None)
    @patch("web_scraper_router._jina_search", return_value=None)
    @patch("web_scraper_router._tavily_search")
    def test_falls_through_to_tier3(
        self, mock_tavily: MagicMock, mock_jina: MagicMock, mock_fc: MagicMock
    ) -> None:
        """When tiers 1-2 fail, should try tier 3."""
        mock_tavily.return_value = [
            _search_result(
                "Tavily Result", "https://example.com", "tavily snippet", "tavily"
            )
        ]
        results = search_web("test query")
        assert len(results) == 1
        assert results[0]["provider"] == "tavily"

    @patch("web_scraper_router._firecrawl_search", return_value=None)
    @patch("web_scraper_router._jina_search", return_value=None)
    @patch("web_scraper_router._tavily_search", return_value=None)
    @patch("web_scraper_router._serper_search", return_value=None)
    @patch("web_scraper_router._brave_search", return_value=None)
    def test_all_tiers_fail(
        self,
        mock_brave: MagicMock,
        mock_serper: MagicMock,
        mock_tavily: MagicMock,
        mock_jina: MagicMock,
        mock_fc: MagicMock,
    ) -> None:
        """When all tiers fail, should return empty list."""
        results = search_web("test query")
        assert results == []


# =============================================================================
# Status endpoint
# =============================================================================


class TestGetScraperStatus:
    """Tests for get_scraper_status()."""

    def setup_method(self) -> None:
        """Reset all circuit breakers."""
        reset_circuit_breakers()

    def test_status_structure(self) -> None:
        """Status should have expected top-level keys."""
        status = get_scraper_status()
        assert "total_tiers" in status
        assert status["total_tiers"] == 6
        assert "available_tiers" in status
        assert "configured_tiers" in status
        assert "tiers" in status
        assert len(status["tiers"]) == 6

    def test_tier_structure(self) -> None:
        """Each tier should have expected keys."""
        status = get_scraper_status()
        for tier in status["tiers"]:
            assert "tier" in tier
            assert "provider" in tier
            assert "has_api_key" in tier
            assert "capabilities" in tier
            assert "free_tier" in tier
            assert "available" in tier

    def test_tier_order(self) -> None:
        """Tiers should be in priority order."""
        status = get_scraper_status()
        providers = [t["provider"] for t in status["tiers"]]
        assert providers == [
            "firecrawl",
            "jina",
            "tavily",
            "serper",
            "brave",
            "urllib_direct",
        ]

    def test_urllib_always_available(self) -> None:
        """urllib_direct tier should always report has_api_key=True."""
        status = get_scraper_status()
        urllib_tier = status["tiers"][-1]
        assert urllib_tier["provider"] == "urllib_direct"
        assert urllib_tier["has_api_key"] is True


# =============================================================================
# Reset circuit breakers
# =============================================================================


class TestResetCircuitBreakers:
    """Tests for reset_circuit_breakers()."""

    def test_reset_all(self) -> None:
        """Should reset all breakers to initial state."""
        router._cb_firecrawl.trip("test")
        router._cb_jina.trip("test")
        result = reset_circuit_breakers()
        assert result["status"] == "all_circuit_breakers_reset"
        assert router._cb_firecrawl.is_available is True
        assert router._cb_jina.is_available is True


# =============================================================================
# Integration: firecrawl_enrichment uses router
# =============================================================================


class TestFirecrawlEnrichmentIntegration:
    """Test that firecrawl_enrichment.py properly imports the router."""

    def test_router_imported(self) -> None:
        """firecrawl_enrichment should have _router_available flag."""
        import firecrawl_enrichment

        assert hasattr(firecrawl_enrichment, "_router_available")

    def test_firecrawl_status_includes_router(self) -> None:
        """get_firecrawl_status should include router_available key."""
        import firecrawl_enrichment

        status = firecrawl_enrichment.get_firecrawl_status()
        assert "router_available" in status
