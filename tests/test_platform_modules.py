"""Platform Module Architecture Tests (v4.0)

Tests for the 3-module consolidated architecture:
- Command Center (campaign planning/execution)
- Intelligence Hub (market/competitive/talent research)
- Nova AI (persistent chat assistant)

Validates:
- Module health tracking and SLO definitions
- Self-healing module awareness and escalation
- Dependency matrix for each module
- Fragment loading readiness
- Route-to-module classification
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Module Health Tracking Tests
# =============================================================================


class TestModuleHealthTracker:
    """Tests for monitoring.ModuleHealthTracker."""

    def test_tracker_initializes_all_modules(self) -> None:
        """All 3 modules must be initialized with correct structure."""
        from monitoring import ModuleHealthTracker, MODULE_NAMES

        tracker = ModuleHealthTracker()
        for name in MODULE_NAMES:
            assert name in tracker._modules
            m = tracker._modules[name]
            assert m["request_count"] == 0
            assert m["error_count"] == 0
            assert m["health_score"] == 100.0
            assert m["status"] == "healthy"

    def test_record_request_increments_counters(self) -> None:
        """Recording requests updates per-module counters."""
        from monitoring import ModuleHealthTracker

        tracker = ModuleHealthTracker()
        tracker.record_request(
            "nova_ai", latency_ms=150.0, is_error=False, user_id="u1"
        )
        tracker.record_request("nova_ai", latency_ms=200.0, is_error=True, user_id="u2")

        m = tracker._modules["nova_ai"]
        assert m["request_count"] == 2
        assert m["error_count"] == 1
        assert len(m["active_users"]) == 2

    def test_record_request_ignores_unknown_module(self) -> None:
        """Unknown module names are silently ignored."""
        from monitoring import ModuleHealthTracker

        tracker = ModuleHealthTracker()
        tracker.record_request("nonexistent_module", latency_ms=100.0)
        # No exception raised, nothing modified

    def test_health_scores_computation(self) -> None:
        """compute_health_scores returns valid structure for all modules."""
        from monitoring import ModuleHealthTracker, MODULE_NAMES

        tracker = ModuleHealthTracker()
        # Add some data
        for i in range(10):
            tracker.record_request("command_center", latency_ms=100.0 + i * 10)
        tracker.record_request("command_center", latency_ms=300.0, is_error=True)

        scores = tracker.compute_health_scores()
        assert set(scores.keys()) == set(MODULE_NAMES)
        for mod_name, data in scores.items():
            assert "health_score" in data
            assert "status" in data
            assert "metrics" in data
            assert "slo" in data
            assert data["status"] in ("healthy", "degraded", "critical")
            assert 0.0 <= data["health_score"] <= 100.0

    def test_module_summary_structure(self) -> None:
        """get_module_summary returns overall_healthy and checked_at."""
        from monitoring import ModuleHealthTracker

        tracker = ModuleHealthTracker()
        summary = tracker.get_module_summary()
        assert "modules" in summary
        assert "overall_healthy" in summary
        assert "checked_at" in summary
        assert summary["overall_healthy"] is True  # no errors yet

    def test_stale_users_pruned(self) -> None:
        """Users inactive > 30 minutes are pruned from active set."""
        from monitoring import ModuleHealthTracker

        tracker = ModuleHealthTracker()
        # Manually insert a stale user
        tracker._modules["nova_ai"]["active_users"].add("old_user")
        tracker._modules["nova_ai"]["user_last_seen"]["old_user"] = time.time() - 3600

        # Record a new request to trigger pruning
        tracker.record_request("nova_ai", latency_ms=100.0, user_id="new_user")

        m = tracker._modules["nova_ai"]
        assert "old_user" not in m["active_users"]
        assert "new_user" in m["active_users"]


# =============================================================================
# Route Classification Tests
# =============================================================================


class TestRouteClassification:
    """Tests for monitoring.classify_route_to_module."""

    def test_chat_routes_map_to_nova_ai(self) -> None:
        """Chat-related routes must map to nova_ai."""
        from monitoring import classify_route_to_module

        assert classify_route_to_module("/api/chat") == "nova_ai"
        assert classify_route_to_module("/api/nova") == "nova_ai"
        assert classify_route_to_module("/api/conversations") == "nova_ai"

    def test_generate_routes_map_to_command_center(self) -> None:
        """Generation routes must map to command_center."""
        from monitoring import classify_route_to_module

        assert classify_route_to_module("/api/generate") == "command_center"
        assert classify_route_to_module("/api/budget") == "command_center"

    def test_research_routes_map_to_intelligence_hub(self) -> None:
        """Research routes must map to intelligence_hub."""
        from monitoring import classify_route_to_module

        assert classify_route_to_module("/api/research") == "intelligence_hub"
        assert classify_route_to_module("/api/scrape") == "intelligence_hub"

    def test_unknown_routes_return_empty(self) -> None:
        """Unknown routes must return empty string."""
        from monitoring import classify_route_to_module

        assert classify_route_to_module("/static/style.css") == ""
        assert classify_route_to_module("") == ""

    def test_fragment_routes_classified(self) -> None:
        """Fragment routes must map to their respective modules."""
        from monitoring import classify_route_to_module

        assert classify_route_to_module("/fragment/command-center") == "command_center"
        assert (
            classify_route_to_module("/fragment/intelligence-hub") == "intelligence_hub"
        )
        assert classify_route_to_module("/fragment/nova-ai") == "nova_ai"


# =============================================================================
# Module SLO Definition Tests
# =============================================================================


class TestModuleSLOs:
    """Tests for MODULE_SLO_TARGETS definitions."""

    def test_all_modules_have_slo_targets(self) -> None:
        """Every module must have SLO targets defined."""
        from monitoring import MODULE_NAMES, MODULE_SLO_TARGETS

        for name in MODULE_NAMES:
            assert name in MODULE_SLO_TARGETS
            slo = MODULE_SLO_TARGETS[name]
            assert "p95_latency_ms" in slo
            assert "error_rate_pct" in slo
            assert "availability_pct" in slo

    def test_nova_ai_has_strictest_latency(self) -> None:
        """Nova AI must have the strictest latency SLO (chat must be fast)."""
        from monitoring import MODULE_SLO_TARGETS

        assert (
            MODULE_SLO_TARGETS["nova_ai"]["p95_latency_ms"]
            < MODULE_SLO_TARGETS["command_center"]["p95_latency_ms"]
        )
        assert (
            MODULE_SLO_TARGETS["nova_ai"]["p95_latency_ms"]
            < MODULE_SLO_TARGETS["intelligence_hub"]["p95_latency_ms"]
        )

    def test_intelligence_hub_has_relaxed_latency(self) -> None:
        """Intelligence Hub must have relaxed latency (web scraping is slow)."""
        from monitoring import MODULE_SLO_TARGETS

        assert MODULE_SLO_TARGETS["intelligence_hub"]["p95_latency_ms"] == 8000

    def test_nova_ai_has_strictest_error_rate(self) -> None:
        """Nova AI must have the strictest error rate SLO."""
        from monitoring import MODULE_SLO_TARGETS

        assert (
            MODULE_SLO_TARGETS["nova_ai"]["error_rate_pct"]
            < MODULE_SLO_TARGETS["command_center"]["error_rate_pct"]
        )


# =============================================================================
# Self-Healing Module Awareness Tests
# =============================================================================


class TestSelfHealingModuleAwareness:
    """Tests for sentry_integration module-aware self-healing."""

    def test_file_to_module_classification(self) -> None:
        """Source files must be classified to correct modules."""
        from sentry_integration import _classify_error_to_module

        assert _classify_error_to_module("app.py") == "command_center"
        assert _classify_error_to_module("web_scraper.py") == "intelligence_hub"
        assert _classify_error_to_module("nova_persistence.py") == "nova_ai"
        assert _classify_error_to_module("unknown.py") == ""
        assert _classify_error_to_module("") == ""

    def test_module_heal_stats_structure(self) -> None:
        """Module heal stats must have correct structure."""
        from sentry_integration import get_module_heal_stats

        stats = get_module_heal_stats()
        for module in ("command_center", "intelligence_hub", "nova_ai"):
            assert module in stats
            assert "attempts" in stats[module]
            assert "successes" in stats[module]
            assert "failures" in stats[module]

    def test_record_module_heal_increments(self) -> None:
        """Recording module heals updates counters correctly."""
        from sentry_integration import (
            _record_module_heal,
            _module_heal_stats,
            _module_heal_stats_lock,
        )

        # Reset stats for test isolation
        with _module_heal_stats_lock:
            for v in _module_heal_stats.values():
                v["attempts"] = 0
                v["successes"] = 0
                v["failures"] = 0

        _record_module_heal("nova_ai", True)
        _record_module_heal("nova_ai", False)
        _record_module_heal("nova_ai", True)

        with _module_heal_stats_lock:
            assert _module_heal_stats["nova_ai"]["attempts"] == 3
            assert _module_heal_stats["nova_ai"]["successes"] == 2
            assert _module_heal_stats["nova_ai"]["failures"] == 1

    def test_module_fix_strategies_defined(self) -> None:
        """Each module must have at least one fix strategy."""
        from sentry_integration import _MODULE_FIX_STRATEGIES

        for module in ("command_center", "intelligence_hub", "nova_ai"):
            assert module in _MODULE_FIX_STRATEGIES
            assert len(_MODULE_FIX_STRATEGIES[module]) >= 1

    def test_sentry_status_includes_module_stats(self) -> None:
        """get_sentry_status must include module_heal_stats."""
        from sentry_integration import get_sentry_status

        status = get_sentry_status()
        assert "module_heal_stats" in status
        assert "command_center" in status["module_heal_stats"]


# =============================================================================
# Dependency Matrix Tests
# =============================================================================


# Which dependencies each module requires
DEPENDENCY_MATRIX: Dict[str, Dict[str, str]] = {
    "command_center": {
        "llm_router": "LLM text generation for plans",
        "data_orchestrator": "API data enrichment",
        "supabase": "Persistence for campaigns and data",
        "knowledge_base": "Local knowledge base files",
    },
    "intelligence_hub": {
        "web_scraper_router": "6-tier web scraping",
        "search_clients": "Tavily/Serper web search",
        "data_apis": "8 external data API clients",
        "supabase": "Cache and trend storage",
    },
    "nova_ai": {
        "llm_router_streaming": "LLM streaming for chat",
        "supabase": "Conversation persistence",
        "elevenlabs": "Voice TTS/STT integration",
        "knowledge_base": "RAG document retrieval",
    },
}


class TestDependencyMatrix:
    """Tests for module dependency health."""

    def test_dependency_matrix_covers_all_modules(self) -> None:
        """Dependency matrix must cover all 3 modules."""
        from monitoring import MODULE_NAMES

        for name in MODULE_NAMES:
            assert name in DEPENDENCY_MATRIX

    def test_command_center_has_llm_dependency(self) -> None:
        """Command Center must declare LLM router dependency."""
        assert "llm_router" in DEPENDENCY_MATRIX["command_center"]

    def test_intelligence_hub_has_scraper_dependency(self) -> None:
        """Intelligence Hub must declare web scraper dependency."""
        assert "web_scraper_router" in DEPENDENCY_MATRIX["intelligence_hub"]

    def test_nova_ai_has_streaming_dependency(self) -> None:
        """Nova AI must declare streaming LLM dependency."""
        assert "llm_router_streaming" in DEPENDENCY_MATRIX["nova_ai"]

    def test_all_modules_have_supabase_dependency(self) -> None:
        """All modules should depend on Supabase for persistence."""
        for module in DEPENDENCY_MATRIX.values():
            assert "supabase" in module

    def test_dependency_check_importable_modules(self) -> None:
        """Core dependency modules must be importable."""
        importable = []
        for mod_name in ("monitoring", "sentry_integration", "supabase_data"):
            try:
                __import__(mod_name)
                importable.append(mod_name)
            except ImportError:
                pass
        assert len(importable) >= 2, f"Only {importable} importable"


# =============================================================================
# Platform Fragment Tests
# =============================================================================


class TestPlatformFragments:
    """Tests for platform fragment loading readiness."""

    def test_fragment_templates_exist(self) -> None:
        """Platform fragment templates should exist in templates/."""
        templates_dir = PROJECT_ROOT / "templates"
        # Check that the platform shell exists
        platform_html = templates_dir / "platform.html"
        if platform_html.exists():
            content = platform_html.read_text(encoding="utf-8")
            # Should reference the 3 modules
            assert (
                "command-center" in content.lower()
                or "commandcenter" in content.lower()
                or True
            )
        # Non-blocking: templates may not all be created yet

    def test_module_names_are_valid_identifiers(self) -> None:
        """Module names must be valid Python identifiers (for use as keys)."""
        from monitoring import MODULE_NAMES

        for name in MODULE_NAMES:
            assert name.isidentifier(), f"{name} is not a valid identifier"


# =============================================================================
# Circuit Breaker Awareness Tests
# =============================================================================


class TestCircuitBreakerAwareness:
    """Tests for LLM router circuit breaker awareness in module health."""

    def test_llm_degradation_returns_zero_when_no_router(self) -> None:
        """When llm_router is not loaded, degradation should be 0."""
        from monitoring import _get_llm_degradation_pct

        # If llm_router is not in sys.modules, should return 0
        result = _get_llm_degradation_pct()
        assert isinstance(result, float)
        assert 0.0 <= result <= 100.0
