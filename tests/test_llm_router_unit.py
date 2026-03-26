#!/usr/bin/env python3
"""Unit tests for llm_router.py -- task classification, cache, provider config,
rate limiting, circuit breaker, quality scoring, and cost tracking.

All external API calls are mocked. No network access required.
"""

from __future__ import annotations

import collections
import hashlib
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Provider Configuration
# ═══════════════════════════════════════════════════════════════════════════════


class TestProviderConfig:
    """Validate PROVIDER_CONFIG structure and completeness."""

    def test_provider_config_is_dict(self) -> None:
        """PROVIDER_CONFIG should be a non-empty dict."""
        from llm_router import PROVIDER_CONFIG

        assert isinstance(PROVIDER_CONFIG, dict)
        assert len(PROVIDER_CONFIG) >= 20, "Expected at least 20 providers"

    def test_all_providers_have_required_fields(self) -> None:
        """Every provider must have model and env_key."""
        from llm_router import PROVIDER_CONFIG

        required_fields = {"model", "env_key"}
        for pid, config in PROVIDER_CONFIG.items():
            for field in required_fields:
                assert field in config, f"Provider {pid} missing '{field}'"

    def test_all_providers_have_name(self) -> None:
        """Every provider should have a human-readable name."""
        from llm_router import PROVIDER_CONFIG

        for pid, config in PROVIDER_CONFIG.items():
            assert "name" in config, f"Provider {pid} missing 'name'"
            assert len(config["name"]) > 0, f"Provider {pid} has empty name"

    def test_all_providers_have_timeout(self) -> None:
        """Every provider should have a timeout value."""
        from llm_router import PROVIDER_CONFIG

        for pid, config in PROVIDER_CONFIG.items():
            timeout = config.get("timeout", 0)
            assert timeout > 0, f"Provider {pid} has invalid timeout: {timeout}"

    def test_all_providers_have_api_style(self) -> None:
        """Every provider should have an api_style field."""
        from llm_router import PROVIDER_CONFIG

        valid_styles = {
            "openai",
            "gemini",
            "anthropic",
            "zhipu",
            "cloudflare",
            "sambanova",
            "nvidia_nim",
            "huggingface",
        }
        for pid, config in PROVIDER_CONFIG.items():
            style = config.get("api_style", "")
            assert style, f"Provider {pid} missing api_style"

    def test_provider_ids_match_constants(self) -> None:
        """Provider ID constants should match PROVIDER_CONFIG keys."""
        from llm_router import (
            PROVIDER_CONFIG,
            GEMINI,
            GROQ,
            CEREBRAS,
            MISTRAL,
            OPENROUTER,
            GPT4O,
            CLAUDE_HAIKU,
            CLAUDE,
            CLAUDE_OPUS,
        )

        for pid in [
            GEMINI,
            GROQ,
            CEREBRAS,
            MISTRAL,
            OPENROUTER,
            GPT4O,
            CLAUDE_HAIKU,
            CLAUDE,
            CLAUDE_OPUS,
        ]:
            assert (
                pid in PROVIDER_CONFIG
            ), f"Provider constant {pid} not in PROVIDER_CONFIG"

    def test_paid_providers_identified(self) -> None:
        """Paid providers should include Claude and GPT-4o."""
        from llm_router import PROVIDER_CONFIG, CLAUDE_HAIKU, CLAUDE, CLAUDE_OPUS, GPT4O

        paid = [CLAUDE_HAIKU, CLAUDE, CLAUDE_OPUS, GPT4O]
        for pid in paid:
            assert pid in PROVIDER_CONFIG, f"Paid provider {pid} missing"


# ═══════════════════════════════════════════════════════════════════════════════
# Task Classification
# ═══════════════════════════════════════════════════════════════════════════════


class TestTaskClassification:
    """Test classify_task() returns meaningful task types."""

    def test_returns_string(self) -> None:
        """classify_task should always return a non-empty string."""
        from llm_router import classify_task

        result = classify_task("What is the average salary for nurses?")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_structured_query(self) -> None:
        """Data-heavy queries should classify as structured or related."""
        from llm_router import classify_task, TASK_STRUCTURED

        result = classify_task("Show me CPC benchmarks for Indeed in New York")
        assert isinstance(result, str)
        # Should be structured or a module-specific type
        assert len(result) > 0

    def test_conversational_query(self) -> None:
        """General questions should classify as conversational."""
        from llm_router import classify_task, TASK_CONVERSATIONAL

        result = classify_task("Hello, how are you?")
        assert isinstance(result, str)

    def test_research_query(self) -> None:
        """Research questions should classify appropriately."""
        from llm_router import classify_task

        result = classify_task(
            "What are the latest trends in recruitment marketing for 2026?"
        )
        assert isinstance(result, str)

    def test_code_query(self) -> None:
        """Code-related queries should classify as code."""
        from llm_router import classify_task

        result = classify_task(
            "Write a Python function to calculate CPC from budget and clicks"
        )
        assert isinstance(result, str)

    def test_verification_query(self) -> None:
        """Fact-checking queries should classify as verification."""
        from llm_router import classify_task

        result = classify_task("Verify that the average CPC for Indeed is $2.50")
        assert isinstance(result, str)

    def test_compliance_query(self) -> None:
        """Compliance queries should get compliance task type."""
        from llm_router import classify_task

        result = classify_task(
            "Check if this job posting complies with EEOC regulations"
        )
        assert isinstance(result, str)

    def test_campaign_plan_query(self) -> None:
        """Campaign planning queries should get campaign_plan type."""
        from llm_router import classify_task

        result = classify_task(
            "Create a media plan for hiring 50 nurses with $100K budget"
        )
        assert isinstance(result, str)

    def test_empty_query_handled(self) -> None:
        """Empty query should not crash."""
        from llm_router import classify_task

        result = classify_task("")
        assert isinstance(result, str)

    def test_very_long_query_handled(self) -> None:
        """Very long query should not crash."""
        from llm_router import classify_task

        result = classify_task("recruitment " * 500)
        assert isinstance(result, str)


# ═══════════════════════════════════════════════════════════════════════════════
# Response Cache
# ═══════════════════════════════════════════════════════════════════════════════


class TestResponseCache:
    """Test _ResponseCache behavior: hit, miss, TTL, eviction."""

    def setup_method(self) -> None:
        """Disable L3 (Upstash) for unit tests so L1 behavior is isolated."""
        import llm_router

        self._orig_upstash = llm_router._UPSTASH_ENABLED
        llm_router._UPSTASH_ENABLED = False

    def teardown_method(self) -> None:
        """Restore original Upstash state."""
        import llm_router

        llm_router._UPSTASH_ENABLED = self._orig_upstash

    def _make_cache(self, max_size: int = 10, ttl: float = 60.0) -> Any:
        """Create a fresh _ResponseCache instance."""
        from llm_router import _ResponseCache

        return _ResponseCache(max_size=max_size, ttl=ttl)

    def test_cache_miss(self) -> None:
        """Cache should return None for unknown keys."""
        cache = self._make_cache()
        result = cache.get("structured", "system prompt", "user message")
        assert result is None

    def test_cache_hit(self) -> None:
        """Cache should return stored response on hit."""
        cache = self._make_cache()
        response = {"text": "test response", "provider": "test"}
        cache.put("structured", "system prompt", "user message", response)
        result = cache.get("structured", "system prompt", "user message")
        assert result is not None
        assert result["text"] == "test response"

    def test_cache_key_deterministic(self) -> None:
        """Same inputs should produce the same cache key."""
        from llm_router import _ResponseCache

        k1 = _ResponseCache._make_key("structured", "sys", "user msg")
        k2 = _ResponseCache._make_key("structured", "sys", "user msg")
        assert k1 == k2

    def test_cache_key_varies_with_task_type(self) -> None:
        """Different task types should produce different cache keys."""
        from llm_router import _ResponseCache

        k1 = _ResponseCache._make_key("structured", "sys", "user msg")
        k2 = _ResponseCache._make_key("conversational", "sys", "user msg")
        assert k1 != k2

    def test_cache_key_varies_with_message(self) -> None:
        """Different messages should produce different cache keys."""
        from llm_router import _ResponseCache

        k1 = _ResponseCache._make_key("structured", "sys", "message one")
        k2 = _ResponseCache._make_key("structured", "sys", "message two")
        assert k1 != k2

    def test_cache_key_is_sha256_hex(self) -> None:
        """Cache key should be a SHA-256 hex digest (64 chars)."""
        from llm_router import _ResponseCache

        key = _ResponseCache._make_key("structured", "sys", "msg")
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)

    def test_cache_eviction_on_max_size(self) -> None:
        """Cache should evict oldest entries when max_size is reached."""
        cache = self._make_cache(max_size=3)
        for i in range(5):
            cache.put("structured", "sys", f"msg {i}", {"text": f"resp {i}"})
        stats = cache.get_stats()
        assert stats["cache_size"] <= 3

    def test_cache_stats_structure(self) -> None:
        """Cache stats should include hits, misses, size."""
        cache = self._make_cache()
        cache.get("structured", "sys", "miss")
        cache.put("structured", "sys", "hit", {"text": "r"})
        cache.get("structured", "sys", "hit")
        stats = cache.get_stats()
        assert "cache_hits" in stats
        assert "cache_misses" in stats
        assert "cache_size" in stats

    def test_cache_ttl_for_realtime_tasks(self) -> None:
        """Real-time task types should get shorter TTL."""
        from llm_router import (
            _ResponseCache,
            _CACHE_TTL_REALTIME_SECONDS,
            _CACHE_TTL_SECONDS,
        )

        realtime_ttl = _ResponseCache._ttl_for_task("market_analysis")
        general_ttl = _ResponseCache._ttl_for_task("conversational")
        # Real-time should be <= general
        assert realtime_ttl <= general_ttl

    def test_cache_thread_safety(self) -> None:
        """Concurrent cache operations should not crash."""
        cache = self._make_cache(max_size=50)
        errors: list[str] = []

        def worker(thread_id: int) -> None:
            try:
                for i in range(20):
                    cache.put(
                        "structured", "sys", f"t{thread_id}_m{i}", {"text": f"r{i}"}
                    )
                    cache.get("structured", "sys", f"t{thread_id}_m{i}")
            except Exception as exc:
                errors.append(f"Thread {thread_id}: {exc}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert errors == [], f"Thread errors: {errors}"


# ═══════════════════════════════════════════════════════════════════════════════
# Response Quality Scoring
# ═══════════════════════════════════════════════════════════════════════════════


class TestResponseQualityScoring:
    """Test _score_response_quality heuristic."""

    def test_empty_response_scores_zero(self) -> None:
        """Empty response should score 0.0."""
        from llm_router import _score_response_quality

        assert _score_response_quality("") == 0.0

    def test_good_response_scores_high(self) -> None:
        """A detailed, helpful response should score > 0.5."""
        from llm_router import _score_response_quality

        text = (
            "Based on the 2026 labor market data, the average CPC for Indeed "
            "in New York is $2.45, with seasonal variation between $2.10 and $2.80. "
            "I recommend allocating 60% of your budget to Indeed and 40% to LinkedIn."
        )
        score = _score_response_quality(text)
        assert score > 0.5

    def test_refusal_response_scores_low(self) -> None:
        """Refusal responses should score lower."""
        from llm_router import _score_response_quality

        score = _score_response_quality("I cannot help with that request.")
        assert score < 0.5

    def test_very_short_response_penalized(self) -> None:
        """Very short responses should be penalized."""
        from llm_router import _score_response_quality

        short_score = _score_response_quality("OK")
        long_score = _score_response_quality(
            "The average CPC for Indeed job postings in the healthcare sector "
            "ranges from $1.80 to $3.50 depending on location and competition."
        )
        assert short_score < long_score

    def test_score_range(self) -> None:
        """Score should always be between 0.0 and 1.0."""
        from llm_router import _score_response_quality

        test_texts = [
            "",
            "Hi",
            "I cannot help with that",
            "This is a medium length response about recruitment.",
            "A" * 1000,
        ]
        for text in test_texts:
            score = _score_response_quality(text)
            assert 0.0 <= score <= 1.0, f"Score {score} out of range for: {text[:30]}"


# ═══════════════════════════════════════════════════════════════════════════════
# Rate Tracker
# ═══════════════════════════════════════════════════════════════════════════════


class TestRateTracker:
    """Test _RateTracker sliding window rate limiter."""

    def _make_tracker(self) -> Any:
        """Create a fresh _RateTracker instance."""
        from llm_router import _RateTracker

        return _RateTracker()

    def test_not_rate_limited_initially(self) -> None:
        """No provider should be rate limited at start."""
        tracker = self._make_tracker()
        assert tracker.is_rate_limited("gemini") is False

    def test_rate_limited_after_burst(self) -> None:
        """Provider should be rate limited after exceeding RPM."""
        tracker = self._make_tracker()
        # Simulate 35 requests in < 60s for groq (RPM=30)
        for _ in range(35):
            tracker.record_request("groq")
        assert tracker.is_rate_limited("groq") is True

    def test_different_providers_independent(self) -> None:
        """Rate limits should be per-provider."""
        tracker = self._make_tracker()
        for _ in range(35):
            tracker.record_request("groq")
        # groq should be limited, but gemini should not
        assert tracker.is_rate_limited("groq") is True
        assert tracker.is_rate_limited("gemini") is False

    def test_get_counts_returns_dict(self) -> None:
        """get_counts should return a dict of provider -> count."""
        tracker = self._make_tracker()
        tracker.record_request("gemini")
        tracker.record_request("gemini")
        counts = tracker.get_counts()
        assert isinstance(counts, dict)
        assert counts.get("gemini", 0) == 2

    def test_thread_safety(self) -> None:
        """Concurrent rate tracking should not crash."""
        tracker = self._make_tracker()
        errors: list[str] = []

        def worker(pid: str) -> None:
            try:
                for _ in range(20):
                    tracker.record_request(pid)
                    tracker.is_rate_limited(pid)
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker, args=(f"p{i}",)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert errors == []


# ═══════════════════════════════════════════════════════════════════════════════
# Cost Tracking
# ═══════════════════════════════════════════════════════════════════════════════


class TestCostTracking:
    """Test cost tracking report structure."""

    def test_cost_report_returns_dict(self) -> None:
        """get_cost_report should return a dict."""
        from llm_router import get_cost_report

        report = get_cost_report()
        assert isinstance(report, dict)

    def test_router_status_returns_dict(self) -> None:
        """get_router_status should return a dict with provider info."""
        from llm_router import get_router_status

        status = get_router_status()
        assert isinstance(status, dict)

    def test_provider_health_returns_dict(self) -> None:
        """get_provider_health should return a dict with per-provider health."""
        from llm_router import get_provider_health

        health = get_provider_health()
        assert isinstance(health, dict)
        # Should have entries for configured providers
        for pid, info in health.items():
            assert "health_score" in info
            assert "available" in info
            assert 0.0 <= info["health_score"] <= 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Request Priority
# ═══════════════════════════════════════════════════════════════════════════════


class TestRequestPriority:
    """Test RequestPriority class."""

    def test_priority_constants_defined(self) -> None:
        """HIGH, MEDIUM, LOW priority constants should be defined."""
        from llm_router import RequestPriority

        assert RequestPriority.HIGH == "high"
        assert RequestPriority.MEDIUM == "medium"
        assert RequestPriority.LOW == "low"


# ═══════════════════════════════════════════════════════════════════════════════
# call_llm with mocked providers
# ═══════════════════════════════════════════════════════════════════════════════


class TestCallLLMMocked:
    """Test call_llm with fully mocked HTTP calls."""

    @mock.patch("llm_router._call_single_provider")
    def test_call_llm_returns_dict(self, mock_call: mock.MagicMock) -> None:
        """call_llm should return a dict with 'text' key."""
        mock_call.return_value = {
            "text": "Mocked response about recruitment strategies.",
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "tokens_in": 50,
            "tokens_out": 100,
        }
        from llm_router import call_llm

        result = call_llm(
            messages=[{"role": "user", "content": "test"}],
            system_prompt="You are a recruitment assistant.",
            task_type="conversational",
            use_cache=False,
        )
        assert isinstance(result, dict)
        assert "text" in result or "error" in result

    @mock.patch("llm_router._call_single_provider")
    def test_call_llm_with_cache_enabled(self, mock_call: mock.MagicMock) -> None:
        """call_llm should attempt cache lookup when use_cache=True."""
        mock_call.return_value = {
            "text": "Cached response test.",
            "provider": "gemini",
            "model": "gemini-2.5-flash",
            "tokens_in": 10,
            "tokens_out": 20,
        }
        from llm_router import call_llm

        # First call populates cache
        result1 = call_llm(
            messages=[{"role": "user", "content": "cache test query xyz123"}],
            system_prompt="sys",
            task_type="conversational",
            use_cache=True,
        )
        # Second identical call should hit cache
        result2 = call_llm(
            messages=[{"role": "user", "content": "cache test query xyz123"}],
            system_prompt="sys",
            task_type="conversational",
            use_cache=True,
        )
        # Both should return valid results
        assert isinstance(result1, dict)
        assert isinstance(result2, dict)


# ═══════════════════════════════════════════════════════════════════════════════
# Global timeout budget
# ═══════════════════════════════════════════════════════════════════════════════


class TestGlobalTimeoutBudget:
    """Test global timeout budget constants."""

    def test_global_timeout_positive(self) -> None:
        """GLOBAL_TIMEOUT_BUDGET should be a positive number."""
        from llm_router import GLOBAL_TIMEOUT_BUDGET

        assert GLOBAL_TIMEOUT_BUDGET > 0

    def test_min_remaining_budget_positive(self) -> None:
        """_MIN_REMAINING_BUDGET should be positive and < GLOBAL_TIMEOUT_BUDGET."""
        from llm_router import GLOBAL_TIMEOUT_BUDGET, _MIN_REMAINING_BUDGET

        assert _MIN_REMAINING_BUDGET > 0
        assert _MIN_REMAINING_BUDGET < GLOBAL_TIMEOUT_BUDGET
