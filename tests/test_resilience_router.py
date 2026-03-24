#!/usr/bin/env python3
"""Tests for resilience_router.py -- circuit breakers, fallback chains, and all tiers."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from resilience_router import (
    CircuitBreaker,
    FileCache,
    LocalErrorLogger,
    LocalEventLogger,
    LocalJSONDB,
    LocalStructuredLogger,
    MemoryCache,
    MemoryCounter,
    ResilienceRouter,
    ServiceTier,
    get_router,
    reset_router,
)


# =============================================================================
# CircuitBreaker tests
# =============================================================================


class TestCircuitBreaker(unittest.TestCase):
    """Test circuit breaker state transitions."""

    def test_initial_state_closed(self) -> None:
        cb = CircuitBreaker(max_failures=3, cooldown_seconds=60)
        self.assertFalse(cb.is_open())

    def test_opens_after_max_failures(self) -> None:
        cb = CircuitBreaker(max_failures=2, cooldown_seconds=3600)
        cb.record_failure("err1")
        self.assertFalse(cb.is_open())
        cb.record_failure("err2")
        self.assertTrue(cb.is_open())

    def test_success_resets_failures(self) -> None:
        cb = CircuitBreaker(max_failures=3, cooldown_seconds=3600)
        cb.record_failure("err1")
        cb.record_failure("err2")
        cb.record_success()
        self.assertFalse(cb.is_open())
        self.assertEqual(cb.failures, 0)

    def test_cooldown_closes_circuit(self) -> None:
        cb = CircuitBreaker(max_failures=1, cooldown_seconds=1)
        cb.record_failure("err")
        self.assertTrue(cb.is_open())
        time.sleep(1.1)
        self.assertFalse(cb.is_open())

    def test_manual_reset(self) -> None:
        cb = CircuitBreaker(max_failures=1, cooldown_seconds=3600)
        cb.record_failure("err")
        self.assertTrue(cb.is_open())
        cb.reset()
        self.assertFalse(cb.is_open())

    def test_snapshot_serializable(self) -> None:
        cb = CircuitBreaker(max_failures=3, cooldown_seconds=60)
        cb.record_failure("test error")
        snap = cb.snapshot()
        # Must be JSON serializable
        json.dumps(snap)
        self.assertEqual(snap["failures"], 1)
        self.assertEqual(snap["last_failure_reason"], "test error")

    def test_thread_safety(self) -> None:
        cb = CircuitBreaker(max_failures=100, cooldown_seconds=3600)
        errors: list[Exception] = []

        def record_ops() -> None:
            try:
                for _ in range(50):
                    cb.record_failure("err")
                    cb.record_success()
                    cb.is_open()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=record_ops) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        self.assertEqual(len(errors), 0, f"Thread errors: {errors}")


# =============================================================================
# ServiceTier tests
# =============================================================================


class TestServiceTier(unittest.TestCase):
    """Test ServiceTier availability and status logic."""

    def test_configured_and_healthy(self) -> None:
        tier = ServiceTier("Test", "test", 1, is_configured=True)
        self.assertTrue(tier.is_available())
        self.assertEqual(tier.status_label(), "OK")

    def test_not_configured(self) -> None:
        tier = ServiceTier("Test", "test", 1, is_configured=False)
        self.assertFalse(tier.is_available())
        self.assertEqual(tier.status_label(), "NOT CONFIGURED")

    def test_circuit_open(self) -> None:
        tier = ServiceTier("Test", "test", 1, is_configured=True, max_failures=1)
        tier.circuit_breaker.record_failure("err")
        self.assertFalse(tier.is_available())
        self.assertEqual(tier.status_label(), "CIRCUIT OPEN")

    def test_snapshot(self) -> None:
        tier = ServiceTier("Redis", "upstash", 1, is_configured=True)
        snap = tier.snapshot()
        self.assertEqual(snap["name"], "Redis")
        self.assertEqual(snap["provider"], "upstash")
        self.assertEqual(snap["priority"], 1)
        self.assertTrue(snap["is_configured"])
        json.dumps(snap)  # Must be JSON serializable


# =============================================================================
# MemoryCache tests
# =============================================================================


class TestMemoryCache(unittest.TestCase):
    """Test in-memory cache with TTL."""

    def test_set_and_get(self) -> None:
        cache = MemoryCache()
        cache.set("key1", {"data": 42}, ttl_seconds=60)
        self.assertEqual(cache.get("key1"), {"data": 42})

    def test_miss_returns_none(self) -> None:
        cache = MemoryCache()
        self.assertIsNone(cache.get("nonexistent"))

    def test_ttl_expiry(self) -> None:
        cache = MemoryCache()
        cache.set("key1", "value", ttl_seconds=1)
        self.assertEqual(cache.get("key1"), "value")
        time.sleep(1.1)
        self.assertIsNone(cache.get("key1"))

    def test_delete(self) -> None:
        cache = MemoryCache()
        cache.set("key1", "value")
        cache.delete("key1")
        self.assertIsNone(cache.get("key1"))

    def test_clear(self) -> None:
        cache = MemoryCache()
        cache.set("a", 1)
        cache.set("b", 2)
        count = cache.clear()
        self.assertEqual(count, 2)
        self.assertEqual(cache.size(), 0)

    def test_max_entries_eviction(self) -> None:
        cache = MemoryCache(max_entries=3)
        for i in range(5):
            cache.set(f"key{i}", i, ttl_seconds=3600)
        self.assertLessEqual(cache.size(), 3)


# =============================================================================
# FileCache tests
# =============================================================================


class TestFileCache(unittest.TestCase):
    """Test file-based cache with TTL."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.cache = FileCache(Path(self._tmpdir))

    def test_set_and_get(self) -> None:
        self.cache.set("fc_key", {"val": 123}, ttl_seconds=60)
        result = self.cache.get("fc_key")
        self.assertEqual(result, {"val": 123})

    def test_miss(self) -> None:
        self.assertIsNone(self.cache.get("nokey"))

    def test_ttl_expiry(self) -> None:
        self.cache.set("expire_key", "data", ttl_seconds=1)
        time.sleep(1.1)
        self.assertIsNone(self.cache.get("expire_key"))

    def test_delete(self) -> None:
        self.cache.set("del_key", "data")
        self.cache.delete("del_key")
        self.assertIsNone(self.cache.get("del_key"))


# =============================================================================
# LocalJSONDB tests
# =============================================================================


class TestLocalJSONDB(unittest.TestCase):
    """Test local JSON database fallback."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.db = LocalJSONDB(Path(self._tmpdir))

    def test_write_and_read(self) -> None:
        rows = [{"id": 1, "name": "test"}]
        self.assertTrue(self.db.write_table("test_table", rows))
        result = self.db.read_table("test_table")
        self.assertEqual(result, rows)

    def test_read_nonexistent(self) -> None:
        result = self.db.read_table("missing")
        self.assertEqual(result, [])

    def test_append_row(self) -> None:
        self.db.write_table("append_test", [{"id": 1}])
        self.db.append_row("append_test", {"id": 2})
        result = self.db.read_table("append_test")
        self.assertEqual(len(result), 2)


# =============================================================================
# LocalEventLogger tests
# =============================================================================


class TestLocalEventLogger(unittest.TestCase):
    """Test local analytics event logging."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.logger = LocalEventLogger(Path(self._tmpdir))

    def test_log_event(self) -> None:
        result = self.logger.log_event("test_event", {"key": "value"})
        self.assertTrue(result)

    def test_log_file_created(self) -> None:
        self.logger.log_event("test", {})
        files = list(Path(self._tmpdir).glob("events_*.jsonl"))
        self.assertEqual(len(files), 1)


# =============================================================================
# LocalErrorLogger tests
# =============================================================================


class TestLocalErrorLogger(unittest.TestCase):
    """Test local error logging."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.logger = LocalErrorLogger(Path(self._tmpdir))

    def test_log_error(self) -> None:
        try:
            raise ValueError("test error")
        except ValueError as exc:
            result = self.logger.log_error(exc, {"module": "test"})
            self.assertTrue(result)

    def test_error_file_created(self) -> None:
        try:
            raise RuntimeError("boom")
        except RuntimeError as exc:
            self.logger.log_error(exc)
        files = list(Path(self._tmpdir).glob("errors_*.jsonl"))
        self.assertEqual(len(files), 1)


# =============================================================================
# LocalStructuredLogger tests
# =============================================================================


class TestLocalStructuredLogger(unittest.TestCase):
    """Test local structured log writer."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.slogger = LocalStructuredLogger(Path(self._tmpdir))

    def test_log(self) -> None:
        result = self.slogger.log("info", "test message", module="test")
        self.assertTrue(result)

    def test_log_file_content(self) -> None:
        self.slogger.log("error", "something failed", code=500)
        files = list(Path(self._tmpdir).glob("structured_*.jsonl"))
        self.assertEqual(len(files), 1)
        line = files[0].read_text().strip()
        data = json.loads(line)
        self.assertEqual(data["level"], "error")
        self.assertEqual(data["code"], 500)


# =============================================================================
# MemoryCounter tests
# =============================================================================


class TestMemoryCounter(unittest.TestCase):
    """Test in-memory analytics counter."""

    def test_increment(self) -> None:
        mc = MemoryCounter()
        mc.increment("page_view")
        mc.increment("page_view")
        mc.increment("click")
        counts = mc.get_counts()
        self.assertEqual(counts["page_view"], 2)
        self.assertEqual(counts["click"], 1)

    def test_snapshot(self) -> None:
        mc = MemoryCounter()
        mc.increment("ev1")
        snap = mc.snapshot()
        self.assertEqual(snap["total"], 1)
        self.assertIn("uptime_seconds", snap)


# =============================================================================
# ResilienceRouter integration tests
# =============================================================================


class TestResilienceRouter(unittest.TestCase):
    """Test the master resilience router with mocked backends."""

    def setUp(self) -> None:
        reset_router()

    def test_cache_memory_fallback(self) -> None:
        """When Upstash and Supabase are down, memory cache works."""
        router = ResilienceRouter()
        # Disable tier 1 and 2 by opening circuit breakers
        for tier in router.get_tiers("caching")[:2]:
            for _ in range(tier.circuit_breaker.max_failures):
                tier.circuit_breaker.record_failure("simulated failure")

        # Memory cache (tier 3) should still work
        router.cache_set("test_key", {"val": 42})
        result = router.cache_get("test_key")
        self.assertEqual(result, {"val": 42})

    def test_cache_file_fallback(self) -> None:
        """When memory and primary caches are down, file cache works."""
        router = ResilienceRouter()
        # Disable tiers 1-3
        for tier in router.get_tiers("caching")[:3]:
            for _ in range(tier.circuit_breaker.max_failures):
                tier.circuit_breaker.record_failure("simulated failure")

        router.cache_set("file_test", "file_value")
        result = router.cache_get("file_test")
        self.assertEqual(result, "file_value")

    def test_analytics_memory_counter_fallback(self) -> None:
        """When PostHog and local file fail, memory counter works."""
        router = ResilienceRouter()
        # Disable tier 1 and 2
        for tier in router.get_tiers("analytics")[:2]:
            for _ in range(tier.circuit_breaker.max_failures):
                tier.circuit_breaker.record_failure("down")

        result = router.track_event("test_event", {"prop": "val"})
        self.assertTrue(result)
        counts = router.memory_counter.get_counts()
        self.assertEqual(counts.get("test_event"), 1)

    def test_error_reporting_stderr_fallback(self) -> None:
        """When all error tiers fail, stderr fallback works."""
        router = ResilienceRouter()
        # Disable all tiers except stderr (last one)
        error_tiers = router.get_tiers("errors")
        for tier in error_tiers[:-1]:
            for _ in range(tier.circuit_breaker.max_failures):
                tier.circuit_breaker.record_failure("down")

        try:
            raise RuntimeError("test error")
        except RuntimeError as exc:
            result = router.report_error(exc, {"test": True})
            self.assertTrue(result)

    def test_logging_stderr_fallback(self) -> None:
        """When Grafana and local file fail, stderr works."""
        router = ResilienceRouter()
        for tier in router.get_tiers("logging")[:-1]:
            for _ in range(tier.circuit_breaker.max_failures):
                tier.circuit_breaker.record_failure("down")

        result = router.log_structured("info", "test log message", source="test")
        self.assertTrue(result)

    def test_email_stderr_fallback(self) -> None:
        """When Resend/SMTP/Slack fail, stderr fallback works."""
        router = ResilienceRouter()
        for tier in router.get_tiers("email")[:-1]:
            for _ in range(tier.circuit_breaker.max_failures):
                tier.circuit_breaker.record_failure("down")

        result = router.send_email(
            to="test@test.com", subject="Test", body="body", severity="info"
        )
        self.assertTrue(result)

    def test_db_local_json_fallback(self) -> None:
        """When Supabase is down, local JSON fallback works."""
        router = ResilienceRouter()
        # Disable Supabase tier
        supabase_tier = router.get_tiers("database")[0]
        for _ in range(supabase_tier.circuit_breaker.max_failures):
            supabase_tier.circuit_breaker.record_failure("down")

        # Write to local JSON
        success = router.db_write("test_table", {"id": 1, "value": "test"})
        self.assertTrue(success)

        # Read back
        rows = router.db_query("test_table")
        self.assertTrue(len(rows) > 0)

    def test_health_dashboard(self) -> None:
        """Health dashboard returns valid JSON-serializable data."""
        router = ResilienceRouter()
        dashboard = router.get_health_dashboard()

        # Must be JSON serializable
        json.dumps(dashboard)

        self.assertIn("services", dashboard)
        self.assertIn("summary", dashboard)
        self.assertIn("health_score", dashboard["summary"])
        self.assertEqual(len(dashboard["services"]), 6)

    def test_priority_matrix(self) -> None:
        """Priority matrix returns a non-empty string."""
        router = ResilienceRouter()
        matrix = router.get_priority_matrix()
        self.assertIsInstance(matrix, str)
        self.assertIn("Caching", matrix)
        self.assertIn("Database", matrix)
        self.assertIn("Email", matrix)

    def test_dashboard_html(self) -> None:
        """Dashboard HTML renders without errors."""
        router = ResilienceRouter()
        html = router.get_dashboard_html()
        self.assertIn("<!DOCTYPE html>", html)
        self.assertIn("Resilience Dashboard", html)
        self.assertIn("Health Score", html)

    def test_singleton(self) -> None:
        """get_router() returns the same instance."""
        reset_router()
        r1 = get_router()
        r2 = get_router()
        self.assertIs(r1, r2)

    def test_singleton_thread_safety(self) -> None:
        """get_router() is safe to call from multiple threads."""
        reset_router()
        routers: list[ResilienceRouter] = []
        errors: list[Exception] = []

        def get() -> None:
            try:
                routers.append(get_router())
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=get) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(len(errors), 0)
        # All threads should get the same instance
        for r in routers:
            self.assertIs(r, routers[0])

    def test_never_crashes_on_total_failure(self) -> None:
        """Router must never raise even if all tiers fail."""
        router = ResilienceRouter()
        # Open all circuit breakers for all services
        for service_tiers in router._tiers.values():
            for tier in service_tiers:
                for _ in range(tier.circuit_breaker.max_failures + 1):
                    tier.circuit_breaker.record_failure("total failure")

        # All operations should return gracefully, not raise
        self.assertIsNone(router.cache_get("any"))
        self.assertFalse(router.cache_set("any", "val"))
        self.assertEqual(router.db_query("any"), [])
        self.assertFalse(router.db_write("any", {}))
        # track_event / report_error / log_structured with all circuits open
        # Memory tiers have max_failures=999, so they won't trip. That's by design.
        # The point is: no exceptions raised.


if __name__ == "__main__":
    unittest.main()
