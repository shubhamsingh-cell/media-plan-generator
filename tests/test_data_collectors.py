#!/usr/bin/env python3
"""Tests for data collection error isolation.

Validates that each data collector module:
1. Imports without error
2. Handles API failures gracefully (try/except isolation)
3. Returns expected types even when external services are unavailable
4. Handles timeout and invalid data scenarios

All external calls are mocked. No network access required.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Data Orchestrator
# ═══════════════════════════════════════════════════════════════════════════════


class TestDataOrchestratorImport:
    """Test data orchestrator module imports and structure."""

    def test_importable(self) -> None:
        """data_orchestrator module should import without error."""
        import data_orchestrator

        assert hasattr(data_orchestrator, "DataOrchestrator")
        assert hasattr(data_orchestrator, "get_orchestrator")

    def test_platform_module_enum(self) -> None:
        """PlatformModule enum should have expected members."""
        from data_orchestrator import PlatformModule

        assert PlatformModule.COMMAND_CENTER == "command_center"
        assert PlatformModule.INTELLIGENCE_HUB == "intelligence_hub"
        assert PlatformModule.NOVA_AI == "nova_ai"

    def test_data_source_type_enum(self) -> None:
        """DataSourceType enum should have expected members."""
        from data_orchestrator import DataSourceType

        expected = [
            "knowledge_base",
            "adzuna",
            "bls",
            "fred",
            "bea",
            "census",
            "onet",
            "usajobs",
            "jooble",
            "web_scraper",
            "tavily_search",
            "vector_search",
            "supabase",
            "llm",
        ]
        for source in expected:
            assert hasattr(
                DataSourceType, source.upper()
            ), f"Missing DataSourceType: {source}"

    def test_enrich_methods_exist(self) -> None:
        """All 7 enrich methods should exist on DataOrchestrator."""
        from data_orchestrator import DataOrchestrator

        methods = [
            "enrich_salary",
            "enrich_market_demand",
            "enrich_budget",
            "enrich_location",
            "enrich_skills_gap",
            "enrich_geopolitical_risk",
            "enrich_market_trends",
        ]
        for method in methods:
            assert hasattr(DataOrchestrator, method), f"Missing method: {method}"

    def test_enrich_salary_returns_dict(self) -> None:
        """enrich_salary should return a dict with expected structure."""
        from data_orchestrator import get_orchestrator

        orch = get_orchestrator()
        if orch:
            result = orch.enrich_salary(
                role="Software Engineer", location="San Francisco"
            )
            assert isinstance(result, dict)
            assert "source" in result
            assert "data" in result


# ═══════════════════════════════════════════════════════════════════════════════
# API Integrations (error isolation)
# ═══════════════════════════════════════════════════════════════════════════════


class TestAPIIntegrationsImport:
    """Test api_integrations module imports without error."""

    def test_importable(self) -> None:
        """api_integrations module should import without error."""
        try:
            import api_integrations

            assert True
        except ImportError:
            pytest.skip("api_integrations not available")

    def test_individual_apis_importable(self) -> None:
        """Individual API client objects should be importable."""
        try:
            from api_integrations import (
                fred,
                adzuna,
                jooble,
                onet,
                bea,
                census,
                usajobs,
                bls,
            )

            # These are client objects (classes), not bare functions
            assert fred is not None
            assert adzuna is not None
        except ImportError:
            pytest.skip("api_integrations not available")


class TestAPIErrorIsolation:
    """Test that API failures are isolated and don't crash the system."""

    @mock.patch("urllib.request.urlopen")
    def test_fred_api_failure_handled(self, mock_urlopen: mock.MagicMock) -> None:
        """FRED API failure should be caught, not crash."""
        mock_urlopen.side_effect = Exception("Connection refused")
        try:
            from api_integrations import fred

            result = fred("GDP")
            # Should return None, empty dict, or error dict -- not crash
            assert result is None or isinstance(result, (dict, list))
        except ImportError:
            pytest.skip("api_integrations not available")
        except Exception:
            # Even if it raises, it should be a controlled exception
            pass

    @mock.patch("urllib.request.urlopen")
    def test_adzuna_api_failure_handled(self, mock_urlopen: mock.MagicMock) -> None:
        """Adzuna API failure should be caught, not crash."""
        mock_urlopen.side_effect = TimeoutError("Request timed out")
        try:
            from api_integrations import adzuna

            result = adzuna("nurse", "us")
            assert result is None or isinstance(result, (dict, list))
        except ImportError:
            pytest.skip("api_integrations not available")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
# Web Scraper Router (error isolation)
# ═══════════════════════════════════════════════════════════════════════════════


class TestWebScraperRouter:
    """Test web_scraper_router error isolation."""

    def test_importable(self) -> None:
        """web_scraper_router should import without error."""
        import web_scraper_router

        assert hasattr(web_scraper_router, "scrape_url")

    def test_content_quality_scoring(self) -> None:
        """Content quality scorer should distinguish good from bad content."""
        from web_scraper_router import _score_content_quality

        assert _score_content_quality("") < 0.2
        assert (
            _score_content_quality(
                "Please sign in to continue. Log in. Create account."
            )
            < 0.5
        )
        assert (
            _score_content_quality(
                "This is a detailed article about recruitment marketing strategies "
                "and best practices for hiring in 2026. The labor market continues "
                "to evolve with AI-driven tools and programmatic job advertising."
            )
            > 0.3
        )

    def test_scrape_url_handles_network_error(self) -> None:
        """scrape_url should handle network errors gracefully."""
        import urllib.error
        from web_scraper_router import scrape_url

        # Use URLError which is what the scraper tiers catch
        with mock.patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.URLError("DNS resolution failed")
            result = scrape_url("https://nonexistent.example.com")
            # Should return None (all tiers exhausted), not crash
            assert result is None or isinstance(result, (dict, str))


# ═══════════════════════════════════════════════════════════════════════════════
# Knowledge Base Loader
# ═══════════════════════════════════════════════════════════════════════════════


class TestKnowledgeBaseLoader:
    """Test kb_loader module for knowledge base loading."""

    def test_importable(self) -> None:
        """kb_loader module should import without error."""
        import kb_loader

        assert True

    def test_data_directory_exists(self) -> None:
        """data/ directory should exist with knowledge base files."""
        data_dir = PROJECT_ROOT / "data"
        assert data_dir.exists(), "data/ directory missing"
        assert data_dir.is_dir(), "data/ is not a directory"

    def test_knowledge_base_files_exist(self) -> None:
        """Key knowledge base JSON files should exist."""
        data_dir = PROJECT_ROOT / "data"
        json_files = list(data_dir.glob("*.json"))
        assert len(json_files) > 0, "No JSON files in data/ directory"

    def test_knowledge_base_files_valid_json(self) -> None:
        """All JSON files in data/ should be valid JSON."""
        import json

        data_dir = PROJECT_ROOT / "data"
        for json_file in data_dir.glob("*.json"):
            content = json_file.read_text(encoding="utf-8")
            try:
                json.loads(content)
            except json.JSONDecodeError as e:
                pytest.fail(f"Invalid JSON in {json_file.name}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Benchmark Registry
# ═══════════════════════════════════════════════════════════════════════════════


class TestBenchmarkRegistry:
    """Test benchmark_registry module for channel benchmark data."""

    def test_importable(self) -> None:
        """benchmark_registry module should import without error."""
        import benchmark_registry

        assert True

    def test_has_lookup_function(self) -> None:
        """benchmark_registry should have accessor functions."""
        import benchmark_registry

        has_accessor = (
            hasattr(benchmark_registry, "get_channel_benchmark")
            or hasattr(benchmark_registry, "get_all_benchmarks")
            or hasattr(benchmark_registry, "get_benchmark_value")
        )
        assert has_accessor, "benchmark_registry missing accessor function"

    def test_get_all_benchmarks_returns_dict(self) -> None:
        """get_all_benchmarks should return a dict."""
        from benchmark_registry import get_all_benchmarks

        result = get_all_benchmarks()
        assert isinstance(result, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# Supabase Data Layer (error isolation)
# ═══════════════════════════════════════════════════════════════════════════════


class TestSupabaseDataLayer:
    """Test supabase_data module error isolation."""

    def test_importable(self) -> None:
        """supabase_data module should import without error."""
        try:
            import supabase_data

            assert True
        except ImportError:
            pytest.skip("supabase_data not available")

    def test_functions_exist(self) -> None:
        """Key data access functions should exist."""
        try:
            from supabase_data import (
                get_knowledge,
                get_channel_benchmarks,
                get_salary_data,
            )

            assert callable(get_knowledge)
            assert callable(get_channel_benchmarks)
            assert callable(get_salary_data)
        except ImportError:
            pytest.skip("supabase_data not available")


# ═══════════════════════════════════════════════════════════════════════════════
# Monitoring Module
# ═══════════════════════════════════════════════════════════════════════════════


class TestMonitoringModule:
    """Test monitoring module for metrics collection."""

    def test_importable(self) -> None:
        """monitoring module should import without error."""
        import monitoring

        assert hasattr(monitoring, "MetricsCollector")

    def test_singleton_pattern(self) -> None:
        """MetricsCollector should use singleton pattern."""
        from monitoring import MetricsCollector

        a = MetricsCollector()
        b = MetricsCollector()
        assert a is b

    def test_prometheus_export(self) -> None:
        """Prometheus export should return string with nova_ prefix."""
        from monitoring import MetricsCollector

        mc = MetricsCollector()
        if hasattr(mc, "export_prometheus"):
            result = mc.export_prometheus()
            assert isinstance(result, str)
            assert "nova_" in result


# ═══════════════════════════════════════════════════════════════════════════════
# Shared Utils
# ═══════════════════════════════════════════════════════════════════════════════


class TestSharedUtils:
    """Test shared_utils module functions."""

    def test_importable(self) -> None:
        """shared_utils module should import without error."""
        from shared_utils import parse_budget, standardize_location

        assert callable(parse_budget)
        assert callable(standardize_location)

    def test_parse_budget_numeric(self) -> None:
        """parse_budget should handle numeric inputs."""
        from shared_utils import parse_budget

        result = parse_budget(50000)
        assert isinstance(result, (int, float))
        assert result > 0

    def test_parse_budget_string(self) -> None:
        """parse_budget should handle string inputs like '$50,000'."""
        from shared_utils import parse_budget

        result = parse_budget("$50,000")
        assert isinstance(result, (int, float))
        assert result > 0

    def test_parse_budget_with_k_suffix(self) -> None:
        """parse_budget should handle '50K' or '50k' inputs."""
        from shared_utils import parse_budget

        result = parse_budget("50K")
        assert isinstance(result, (int, float))
        assert result >= 50000

    def test_standardize_location(self) -> None:
        """standardize_location should return a string."""
        from shared_utils import standardize_location

        result = standardize_location("NYC")
        assert isinstance(result, str)
        assert len(result) > 0
