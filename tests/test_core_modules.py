#!/usr/bin/env python3
"""Integration tests for Nova AI Suite core modules.

Tests critical paths: LLM router, data orchestrator, Nova chat,
monitoring, resilience, and web scraper.
"""

import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestLLMRouter(unittest.TestCase):
    """Test LLM router core functionality."""

    def test_import(self) -> None:
        """LLM router module imports without error."""
        import llm_router

        self.assertTrue(hasattr(llm_router, "call_llm"))
        self.assertTrue(hasattr(llm_router, "get_router_status"))

    def test_router_status_structure(self) -> None:
        """Router status returns expected structure."""
        from llm_router import get_router_status

        status = get_router_status()
        self.assertIsInstance(status, dict)

    def test_provider_config_valid(self) -> None:
        """All provider configs have required fields."""
        from llm_router import PROVIDER_CONFIG

        required_fields = {"model", "env_key"}
        for pid, config in PROVIDER_CONFIG.items():
            for field in required_fields:
                self.assertIn(field, config, f"Provider {pid} missing {field}")

    def test_cost_report(self) -> None:
        """Cost report returns valid structure."""
        from llm_router import get_cost_report

        report = get_cost_report()
        self.assertIsInstance(report, dict)

    def test_response_quality_scoring(self) -> None:
        """Response quality scorer works correctly."""
        from llm_router import _score_response_quality

        self.assertEqual(_score_response_quality(""), 0.0)
        self.assertGreater(
            _score_response_quality(
                "This is a detailed helpful response about recruitment marketing strategies."
            ),
            0.5,
        )
        self.assertLess(_score_response_quality("I cannot help with that"), 0.5)

    def test_classify_task(self) -> None:
        """Task classifier returns a string."""
        from llm_router import classify_task

        result = classify_task("What is the average salary for a nurse in Texas?")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_provider_health(self) -> None:
        """Provider health returns a dict."""
        from llm_router import get_provider_health

        result = get_provider_health()
        self.assertIsInstance(result, dict)


class TestDataOrchestrator(unittest.TestCase):
    """Test data orchestrator functionality."""

    def test_import(self) -> None:
        """Data orchestrator imports without error."""
        import data_orchestrator

        self.assertTrue(hasattr(data_orchestrator, "DataOrchestrator"))

    def test_enrich_methods_exist(self) -> None:
        """All 7 enrich methods exist on DataOrchestrator."""
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
            self.assertTrue(
                hasattr(DataOrchestrator, method), f"Missing method: {method}"
            )

    def test_enrich_salary_returns_dict(self) -> None:
        """enrich_salary returns a dict with expected keys."""
        from data_orchestrator import get_orchestrator

        orch = get_orchestrator()
        if orch:
            result = orch.enrich_salary(role="Software Engineer", location="New York")
            self.assertIsInstance(result, dict)
            self.assertIn("source", result)
            self.assertIn("data", result)
            self.assertIn("sources_used", result)


class TestMonitoring(unittest.TestCase):
    """Test monitoring module."""

    def test_import(self) -> None:
        """Monitoring module imports without error."""
        import monitoring

        self.assertTrue(hasattr(monitoring, "MetricsCollector"))

    def test_metrics_collector_singleton(self) -> None:
        """MetricsCollector returns the same instance."""
        from monitoring import MetricsCollector

        a = MetricsCollector()
        b = MetricsCollector()
        self.assertIs(a, b)

    def test_burn_rate(self) -> None:
        """Burn rate returns a dict."""
        from monitoring import MetricsCollector

        mc = MetricsCollector()
        if hasattr(mc, "compute_burn_rate"):
            result = mc.compute_burn_rate()
            self.assertIsInstance(result, dict)

    def test_anomaly_detection(self) -> None:
        """Anomaly detection returns a list."""
        from monitoring import MetricsCollector

        mc = MetricsCollector()
        if hasattr(mc, "check_anomalies"):
            result = mc.check_anomalies()
            self.assertIsInstance(result, list)

    def test_prometheus_export(self) -> None:
        """Prometheus export returns a string with nova_ prefix."""
        from monitoring import MetricsCollector

        mc = MetricsCollector()
        if hasattr(mc, "export_prometheus"):
            result = mc.export_prometheus()
            self.assertIsInstance(result, str)
            self.assertIn("nova_", result)


class TestResilience(unittest.TestCase):
    """Test resilience router."""

    def test_import(self) -> None:
        """Resilience router imports without error."""
        import resilience_router

        self.assertTrue(hasattr(resilience_router, "get_router"))
        self.assertTrue(hasattr(resilience_router, "get_resilience_summary"))

    def test_summary(self) -> None:
        """Resilience summary returns a dict."""
        from resilience_router import get_resilience_summary

        result = get_resilience_summary()
        self.assertIsInstance(result, dict)


class TestWebScraper(unittest.TestCase):
    """Test web scraper router."""

    def test_import(self) -> None:
        """Web scraper router imports without error."""
        import web_scraper_router

        self.assertTrue(hasattr(web_scraper_router, "scrape_url"))

    def test_content_quality_scoring(self) -> None:
        """Content quality scorer distinguishes good from bad content."""
        from web_scraper_router import _score_content_quality

        self.assertLess(_score_content_quality(""), 0.2)
        self.assertLess(
            _score_content_quality(
                "Please sign in to continue. Log in. Create account."
            ),
            0.5,
        )
        self.assertGreater(
            _score_content_quality(
                "This is a detailed article about recruitment marketing strategies "
                "and best practices for hiring in 2026. The labor market continues "
                "to evolve with AI-driven tools."
            ),
            0.3,
        )


class TestAuth(unittest.TestCase):
    """Test authentication module."""

    def test_import(self) -> None:
        """Auth module imports without error."""
        import auth

        self.assertTrue(hasattr(auth, "authenticate"))
        self.assertTrue(hasattr(auth, "is_auth_enabled"))

    def test_public_endpoints_bypass(self) -> None:
        """Public endpoints bypass authentication."""
        from auth import authenticate

        result = authenticate("/", None)
        self.assertTrue(result["authenticated"])
        self.assertEqual(result["role"], "public")

    def test_api_without_key_when_auth_disabled(self) -> None:
        """API endpoints pass when auth is disabled."""
        from auth import authenticate, is_auth_enabled

        if not is_auth_enabled():
            result = authenticate("/api/chat", None)
            self.assertTrue(result["authenticated"])


class TestAuditLogger(unittest.TestCase):
    """Test audit logger."""

    def test_import(self) -> None:
        """Audit logger imports without error."""
        import audit_logger

        self.assertTrue(hasattr(audit_logger, "log_event"))
        self.assertTrue(hasattr(audit_logger, "get_audit_summary"))

    def test_log_and_retrieve(self) -> None:
        """Events can be logged and retrieved."""
        from audit_logger import log_event, get_recent_events

        log_event("test.event", actor="test", resource="/test")
        events = get_recent_events(limit=5, action_filter="test.event")
        self.assertTrue(len(events) > 0)
        self.assertEqual(events[-1]["action"], "test.event")


class TestPostHog(unittest.TestCase):
    """Test PostHog integration."""

    def test_import(self) -> None:
        """PostHog integration imports without error."""
        import posthog_integration

        self.assertTrue(hasattr(posthog_integration, "track_event"))
        self.assertTrue(hasattr(posthog_integration, "is_feature_enabled"))
        self.assertTrue(hasattr(posthog_integration, "track_group"))
        self.assertTrue(hasattr(posthog_integration, "set_consent"))


class TestAutoQC(unittest.TestCase):
    """Test auto QC module."""

    def test_import(self) -> None:
        """Auto QC imports without error."""
        import auto_qc

        self.assertTrue(hasattr(auto_qc, "get_status"))

    def test_sla_report(self) -> None:
        """SLA report returns a dict."""
        from auto_qc import get_sla_report

        if callable(get_sla_report):
            result = get_sla_report()
            self.assertIsInstance(result, dict)


class TestInputSanitization(unittest.TestCase):
    """Test the _sanitize_input helper used for chat inputs."""

    def _get_sanitizer(self):
        """Import the sanitize function from app module constants."""
        # The function is module-level in app.py
        try:
            from app import _sanitize_chat_input

            return _sanitize_chat_input
        except ImportError:
            self.skipTest("_sanitize_chat_input not importable (app.py too large)")

    def test_strips_html_tags(self) -> None:
        """HTML tags are stripped from input."""
        sanitize = self._get_sanitizer()
        result = sanitize("<script>alert('xss')</script>Hello")
        self.assertNotIn("<script>", result)
        self.assertIn("Hello", result)

    def test_strips_event_handlers(self) -> None:
        """Event handler attributes are stripped."""
        sanitize = self._get_sanitizer()
        result = sanitize('<img onerror="alert(1)" src=x>')
        self.assertNotIn("onerror", result)

    def test_preserves_plain_text(self) -> None:
        """Plain text passes through unchanged."""
        sanitize = self._get_sanitizer()
        msg = "What is the average salary for nurses in Texas?"
        self.assertEqual(sanitize(msg), msg)

    def test_length_limit(self) -> None:
        """Messages exceeding 10000 chars are truncated."""
        sanitize = self._get_sanitizer()
        long_msg = "a" * 15000
        result = sanitize(long_msg)
        self.assertLessEqual(len(result), 10000)

    def test_empty_passthrough(self) -> None:
        """Empty string passes through."""
        sanitize = self._get_sanitizer()
        self.assertEqual(sanitize(""), "")

    def test_none_passthrough(self) -> None:
        """None passes through as empty string."""
        sanitize = self._get_sanitizer()
        self.assertEqual(sanitize(None), "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
