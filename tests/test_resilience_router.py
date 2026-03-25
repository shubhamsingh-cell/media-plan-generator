#!/usr/bin/env python3
"""Tests for resilience_router.py -- circuit breakers, fallback chains, unified API.

60+ tests covering:
    - CircuitBreaker state transitions (open/close/half-open/reset/thread-safety)
    - ServiceTier availability, cost_label, rate_limit_info, snapshots
    - MemoryCache TTL, eviction, clear
    - FileCache set/get/delete
    - LocalJSONDB write/read/append
    - LocalEventLogger / LocalErrorLogger / LocalStructuredLogger
    - MemoryCounter increments and snapshot
    - ResilienceRouter: 8 categories, tier fallback, unified execute() API
    - get_status() / reset() / get_health_dashboard()
    - HTML-to-PDF deck generation fallback
    - Auto-discovery of tiers based on env vars
    - Singleton thread safety
    - Total failure graceful degradation
"""

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


class TestCircuitBreaker(unittest.TestCase):

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
        # _get_cooldown_with_jitter applies exponential backoff, so
        # disabled_until may be > now + 1s.  Override it directly to
        # test the cooldown-recovery path without sleeping too long.
        cb.disabled_until = time.time() + 0.3
        time.sleep(0.5)
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
        json.dumps(snap)
        self.assertEqual(snap["failures"], 1)
        self.assertEqual(snap["last_failure_reason"], "test error")

    def test_total_counters(self) -> None:
        cb = CircuitBreaker(max_failures=10, cooldown_seconds=60)
        cb.record_success()
        cb.record_success()
        cb.record_failure("err")
        self.assertEqual(cb.total_successes, 2)
        self.assertEqual(cb.total_failures, 1)

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

    def test_cooldown_timing_long(self) -> None:
        cb = CircuitBreaker(max_failures=1, cooldown_seconds=9999)
        cb.record_failure("err")
        self.assertTrue(cb.is_open())
        self.assertGreater(cb.disabled_until, time.time())

    def test_half_open_zero_cooldown(self) -> None:
        cb = CircuitBreaker(max_failures=1, cooldown_seconds=0)
        cb.record_failure("err")
        self.assertFalse(cb.is_open())


class TestServiceTier(unittest.TestCase):

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

    def test_snapshot_includes_all_fields(self) -> None:
        tier = ServiceTier("Redis", "upstash", 1, is_configured=True)
        snap = tier.snapshot()
        self.assertEqual(snap["name"], "Redis")
        self.assertIn("cost_label", snap)
        self.assertIn("rate_limit_info", snap)
        json.dumps(snap)

    def test_cost_label_and_rate_limit(self) -> None:
        tier = ServiceTier(
            "T", "t", 1, True, cost_label="paid", rate_limit_info="100/day"
        )
        self.assertEqual(tier.cost_label, "paid")
        self.assertEqual(tier.rate_limit_info, "100/day")
        snap = tier.snapshot()
        self.assertEqual(snap["cost_label"], "paid")

    def test_default_cost_label(self) -> None:
        tier = ServiceTier("T", "t", 1, True)
        self.assertEqual(tier.cost_label, "free")
        self.assertEqual(tier.rate_limit_info, "unlimited")


class TestMemoryCache(unittest.TestCase):

    def test_set_and_get(self) -> None:
        c = MemoryCache()
        c.set("k", {"d": 42}, ttl_seconds=60)
        self.assertEqual(c.get("k"), {"d": 42})

    def test_miss(self) -> None:
        self.assertIsNone(MemoryCache().get("x"))

    def test_ttl_expiry(self) -> None:
        c = MemoryCache()
        c.set("k", "v", ttl_seconds=1)
        time.sleep(1.1)
        self.assertIsNone(c.get("k"))

    def test_delete(self) -> None:
        c = MemoryCache()
        c.set("k", "v")
        c.delete("k")
        self.assertIsNone(c.get("k"))

    def test_clear(self) -> None:
        c = MemoryCache()
        c.set("a", 1)
        c.set("b", 2)
        self.assertEqual(c.clear(), 2)
        self.assertEqual(c.size(), 0)

    def test_max_entries_eviction(self) -> None:
        c = MemoryCache(max_entries=3)
        for i in range(5):
            c.set(f"k{i}", i, ttl_seconds=3600)
        self.assertLessEqual(c.size(), 3)


class TestFileCache(unittest.TestCase):

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.cache = FileCache(Path(self._tmpdir))

    def test_set_and_get(self) -> None:
        self.cache.set("k", {"v": 1}, ttl_seconds=60)
        self.assertEqual(self.cache.get("k"), {"v": 1})

    def test_miss(self) -> None:
        self.assertIsNone(self.cache.get("x"))

    def test_ttl_expiry(self) -> None:
        self.cache.set("k", "d", ttl_seconds=1)
        time.sleep(1.1)
        self.assertIsNone(self.cache.get("k"))

    def test_delete(self) -> None:
        self.cache.set("k", "d")
        self.cache.delete("k")
        self.assertIsNone(self.cache.get("k"))


class TestLocalJSONDB(unittest.TestCase):

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self.db = LocalJSONDB(Path(self._tmpdir))

    def test_write_and_read(self) -> None:
        rows = [{"id": 1, "name": "test"}]
        self.assertTrue(self.db.write_table("t", rows))
        self.assertEqual(self.db.read_table("t"), rows)

    def test_read_nonexistent(self) -> None:
        self.assertEqual(self.db.read_table("missing"), [])

    def test_append_row(self) -> None:
        self.db.write_table("t", [{"id": 1}])
        self.db.append_row("t", {"id": 2})
        self.assertEqual(len(self.db.read_table("t")), 2)


class TestLocalEventLogger(unittest.TestCase):

    def test_log_event(self) -> None:
        lg = LocalEventLogger(Path(tempfile.mkdtemp()))
        self.assertTrue(lg.log_event("ev", {"k": "v"}))

    def test_log_file_created(self) -> None:
        d = Path(tempfile.mkdtemp())
        lg = LocalEventLogger(d)
        lg.log_event("ev", {})
        self.assertEqual(len(list(d.glob("events_*.jsonl"))), 1)


class TestLocalErrorLogger(unittest.TestCase):

    def test_log_error(self) -> None:
        lg = LocalErrorLogger(Path(tempfile.mkdtemp()))
        try:
            raise ValueError("err")
        except ValueError as exc:
            self.assertTrue(lg.log_error(exc, {"m": "t"}))

    def test_error_file_created(self) -> None:
        d = Path(tempfile.mkdtemp())
        lg = LocalErrorLogger(d)
        try:
            raise RuntimeError("boom")
        except RuntimeError as exc:
            lg.log_error(exc)
        self.assertEqual(len(list(d.glob("errors_*.jsonl"))), 1)


class TestLocalStructuredLogger(unittest.TestCase):

    def test_log(self) -> None:
        lg = LocalStructuredLogger(Path(tempfile.mkdtemp()))
        self.assertTrue(lg.log("info", "test", module="t"))

    def test_log_file_content(self) -> None:
        d = Path(tempfile.mkdtemp())
        lg = LocalStructuredLogger(d)
        lg.log("error", "fail", code=500)
        files = list(d.glob("structured_*.jsonl"))
        data = json.loads(files[0].read_text().strip())
        self.assertEqual(data["level"], "error")
        self.assertEqual(data["code"], 500)


class TestMemoryCounter(unittest.TestCase):

    def test_increment(self) -> None:
        mc = MemoryCounter()
        mc.increment("pv")
        mc.increment("pv")
        mc.increment("cl")
        self.assertEqual(mc.get_counts()["pv"], 2)

    def test_snapshot(self) -> None:
        mc = MemoryCounter()
        mc.increment("e")
        snap = mc.snapshot()
        self.assertEqual(snap["total"], 1)
        self.assertIn("uptime_seconds", snap)


class TestResilienceRouter(unittest.TestCase):

    def setUp(self) -> None:
        reset_router()

    def test_all_eight_categories(self) -> None:
        router = ResilienceRouter()
        expected = {
            "caching",
            "database",
            "email",
            "analytics",
            "errors",
            "logging",
            "web_scraping",
            "deck_generation",
        }
        self.assertEqual(set(router._tiers.keys()), expected)

    def test_each_category_has_at_least_two_tiers(self) -> None:
        router = ResilienceRouter()
        for cat, tiers in router._tiers.items():
            self.assertGreaterEqual(len(tiers), 2, f"'{cat}' < 2 tiers")

    def test_tiers_sorted_by_priority(self) -> None:
        router = ResilienceRouter()
        for cat, tiers in router._tiers.items():
            p = [t.priority for t in tiers]
            self.assertEqual(p, sorted(p), f"'{cat}' not sorted")

    def test_last_tier_always_configured(self) -> None:
        router = ResilienceRouter()
        for cat, tiers in router._tiers.items():
            self.assertTrue(
                tiers[-1].is_configured, f"'{cat}' last tier not configured"
            )

    def test_cache_memory_fallback(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("caching")[:2]:
            for _ in range(t.circuit_breaker.max_failures):
                t.circuit_breaker.record_failure("sim")
        router.cache_set("tk", {"v": 42})
        self.assertEqual(router.cache_get("tk"), {"v": 42})

    def test_cache_file_fallback(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("caching")[:3]:
            for _ in range(t.circuit_breaker.max_failures):
                t.circuit_breaker.record_failure("sim")
        router.cache_set("fk", "fv")
        self.assertEqual(router.cache_get("fk"), "fv")

    def test_cache_delete(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("caching")[:2]:
            for _ in range(t.circuit_breaker.max_failures):
                t.circuit_breaker.record_failure("d")
        router.cache_set("dk", "dv")
        self.assertTrue(router.cache_delete("dk"))

    def test_analytics_memory_counter_fallback(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("analytics"):
            if t.provider != "memory_counter":
                for _ in range(t.circuit_breaker.max_failures):
                    t.circuit_breaker.record_failure("d")
        self.assertTrue(router.track_event("te", {"p": "v"}))
        self.assertEqual(router.memory_counter.get_counts().get("te"), 1)

    def test_error_stderr_fallback(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("errors"):
            if t.provider != "stderr":
                for _ in range(t.circuit_breaker.max_failures):
                    t.circuit_breaker.record_failure("d")
        try:
            raise RuntimeError("te")
        except RuntimeError as exc:
            self.assertTrue(router.report_error(exc, {"t": True}))

    def test_logging_stderr_fallback(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("logging")[:-1]:
            for _ in range(t.circuit_breaker.max_failures):
                t.circuit_breaker.record_failure("d")
        self.assertTrue(router.log_structured("info", "tm", source="t"))

    def test_email_local_log_fallback(self) -> None:
        router = ResilienceRouter()
        for t in router.get_tiers("email"):
            if t.provider not in ("local_log", "sentry_breadcrumb"):
                for _ in range(t.circuit_breaker.max_failures):
                    t.circuit_breaker.record_failure("d")
        self.assertTrue(router.send_email("a@b.c", "S", "B", "info"))

    def test_db_local_json_fallback(self) -> None:
        router = ResilienceRouter()
        st = router.get_tiers("database")[0]
        for _ in range(st.circuit_breaker.max_failures):
            st.circuit_breaker.record_failure("d")
        self.assertTrue(router.db_write("tt", {"id": 1}))
        self.assertTrue(len(router.db_query("tt")) > 0)

    def test_never_crashes_on_total_failure(self) -> None:
        router = ResilienceRouter()
        for tiers in router._tiers.values():
            for t in tiers:
                for _ in range(t.circuit_breaker.max_failures + 1):
                    t.circuit_breaker.record_failure("total")
        self.assertIsNone(router.cache_get("x"))
        self.assertFalse(router.cache_set("x", "v"))
        self.assertEqual(router.db_query("x"), [])
        self.assertFalse(router.db_write("x", {}))


class TestUnifiedExecuteAPI(unittest.TestCase):

    def setUp(self) -> None:
        reset_router()
        self.router = ResilienceRouter()
        for cat in self.router._tiers:
            for t in self.router._tiers[cat]:
                if t.provider in (
                    "memory",
                    "file",
                    "memory_kb",
                    "local_json",
                    "local_log",
                    "memory_counter",
                    "local_file",
                    "stderr",
                    "html_pdf",
                    "pptx",
                    "urllib",
                    "sentry_breadcrumb",
                ):
                    continue
                for _ in range(t.circuit_breaker.max_failures):
                    t.circuit_breaker.record_failure("trip")

    def test_execute_cache_set_get(self) -> None:
        self.assertTrue(self.router.execute("caching", "set", key="ek", data="ev"))
        self.assertEqual(self.router.execute("caching", "get", key="ek"), "ev")

    def test_execute_cache_delete(self) -> None:
        self.router.execute("caching", "set", key="dk", data="dv")
        self.assertTrue(self.router.execute("caching", "delete", key="dk"))

    def test_execute_unknown_category(self) -> None:
        with self.assertRaises(ValueError):
            self.router.execute("bogus", "get")

    def test_execute_unknown_operation(self) -> None:
        with self.assertRaises(ValueError):
            self.router.execute("caching", "fly")

    def test_execute_analytics(self) -> None:
        self.assertTrue(self.router.execute("analytics", "track", event="et"))

    def test_execute_logging(self) -> None:
        self.assertTrue(
            self.router.execute("logging", "log", level="info", message="tm")
        )

    def test_execute_deck_html_fallback(self) -> None:
        for t in self.router._tiers["deck_generation"]:
            if t.provider != "html_pdf":
                for _ in range(t.circuit_breaker.max_failures):
                    t.circuit_breaker.record_failure("trip")
        result = self.router.execute(
            "deck_generation",
            "generate",
            data={"title": "TD", "channels": [{"name": "Indeed", "spend": 5000}]},
        )
        self.assertIsNotNone(result)
        self.assertIn(b"TD", result)
        self.assertIn(b"Indeed", result)

    def test_execute_database_write(self) -> None:
        self.assertTrue(
            self.router.execute("database", "write", table="et", row={"id": 1})
        )

    def test_execute_error_report(self) -> None:
        self.assertTrue(
            self.router.execute(
                "errors",
                "report",
                error=RuntimeError("ee"),
                context={"t": True},
            )
        )

    def test_execute_email_send(self) -> None:
        self.assertTrue(
            self.router.execute(
                "email",
                "send",
                to="a@b.c",
                subject="S",
                body="B",
            )
        )


class TestStatusAndReset(unittest.TestCase):

    def setUp(self) -> None:
        reset_router()

    def test_get_status_has_eight_services(self) -> None:
        self.assertEqual(
            ResilienceRouter().get_status()["summary"]["total_services"], 8
        )

    def test_health_score_range(self) -> None:
        d = ResilienceRouter().get_health_dashboard()
        self.assertGreaterEqual(d["summary"]["health_score"], 0)
        self.assertLessEqual(d["summary"]["health_score"], 100)

    def test_reset_single_category(self) -> None:
        r = ResilienceRouter()
        t = r._tiers["caching"][0]
        for _ in range(t.circuit_breaker.max_failures):
            t.circuit_breaker.record_failure("x")
        r.reset("caching")
        self.assertFalse(t.circuit_breaker.is_open())

    def test_reset_all(self) -> None:
        r = ResilienceRouter()
        for c in ("caching", "email", "errors"):
            t = r._tiers[c][0]
            for _ in range(t.circuit_breaker.max_failures):
                t.circuit_breaker.record_failure("x")
        r.reset()
        for c in ("caching", "email", "errors"):
            self.assertFalse(r._tiers[c][0].circuit_breaker.is_open())

    def test_dashboard_degraded_detection(self) -> None:
        r = ResilienceRouter()
        p = r._tiers["caching"][0]
        for _ in range(p.circuit_breaker.max_failures):
            p.circuit_breaker.record_failure("x")
        d = r.get_health_dashboard()
        if p.is_configured:
            self.assertIn(d["services"]["caching"]["status"], ("degraded", "healthy"))

    def test_priority_matrix_text(self) -> None:
        m = ResilienceRouter().get_priority_matrix()
        self.assertIsInstance(m, str)
        self.assertGreater(len(m), 100)

    def test_dashboard_html(self) -> None:
        h = ResilienceRouter().get_dashboard_html()
        self.assertIn("<!DOCTYPE html>", h)
        self.assertIn("Resilience Dashboard", h)

    def test_dashboard_json_serializable(self) -> None:
        json.dumps(ResilienceRouter().get_health_dashboard())


class TestSingleton(unittest.TestCase):

    def test_same_instance(self) -> None:
        reset_router()
        self.assertIs(get_router(), get_router())

    def test_reset_new_instance(self) -> None:
        r1 = get_router()
        reset_router()
        self.assertIsNot(r1, get_router())

    def test_thread_safety(self) -> None:
        reset_router()
        routers: list[ResilienceRouter] = []

        def get() -> None:
            routers.append(get_router())

        threads = [threading.Thread(target=get) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        for r in routers:
            self.assertIs(r, routers[0])


class TestHTMLPDFFallback(unittest.TestCase):

    def test_basic_html(self) -> None:
        r = ResilienceRouter()._generate_html_pdf({"title": "My Plan"})
        self.assertIn(b"<h1>My Plan</h1>", r)
        self.assertIn(b"Nova AI Suite", r)

    def test_html_with_channels(self) -> None:
        r = ResilienceRouter()._generate_html_pdf(
            {
                "title": "T",
                "channels": [{"name": "Indeed", "spend": 5000}],
            }
        )
        self.assertIn(b"Indeed", r)
        self.assertIn(b"<table>", r)

    def test_html_empty_data(self) -> None:
        self.assertIn(b"Media Plan", ResilienceRouter()._generate_html_pdf({}))


class TestAutoDiscovery(unittest.TestCase):

    def test_memory_and_file_always_available(self) -> None:
        r = ResilienceRouter()
        p = {t.provider: t.is_configured for t in r._tiers["caching"]}
        self.assertTrue(p.get("memory"))
        self.assertTrue(p.get("file"))

    def test_urllib_always_available(self) -> None:
        r = ResilienceRouter()
        t = next((t for t in r._tiers["web_scraping"] if t.provider == "urllib"), None)
        self.assertIsNotNone(t)
        self.assertTrue(t.is_configured)

    def test_pptx_and_html_pdf_always_available(self) -> None:
        r = ResilienceRouter()
        for p in ("pptx", "html_pdf"):
            t = next((t for t in r._tiers["deck_generation"] if t.provider == p), None)
            self.assertIsNotNone(t, f"{p} missing")
            self.assertTrue(t.is_configured)

    @patch.dict(os.environ, {"FIRECRAWL_API_KEY": "fc-test123"})
    def test_firecrawl_discovered(self) -> None:
        r = ResilienceRouter()
        t = next(
            (t for t in r._tiers["web_scraping"] if t.provider == "firecrawl"), None
        )
        self.assertTrue(t.is_configured)

    @patch.dict(os.environ, {}, clear=True)
    def test_cloud_tiers_unconfigured_without_env(self) -> None:
        r = ResilienceRouter()
        t = next((t for t in r._tiers["database"] if t.provider == "supabase"), None)
        self.assertFalse(t.is_configured)

    def test_web_scraping_six_tiers(self) -> None:
        self.assertEqual(len(ResilienceRouter()._tiers["web_scraping"]), 6)

    def test_deck_generation_three_tiers(self) -> None:
        self.assertEqual(len(ResilienceRouter()._tiers["deck_generation"]), 3)


class TestThreadSafety(unittest.TestCase):

    def test_concurrent_cache_ops(self) -> None:
        r = ResilienceRouter()
        for t in r.get_tiers("caching")[:2]:
            for _ in range(t.circuit_breaker.max_failures):
                t.circuit_breaker.record_failure("trip")
        errors: list[str] = []
        barrier = threading.Barrier(5)

        def worker(i: int) -> None:
            barrier.wait()
            try:
                r.cache_set(f"t{i}", f"v{i}")
                if r.cache_get(f"t{i}") != f"v{i}":
                    errors.append(f"thread {i} mismatch")
            except Exception as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertEqual(errors, [])

    def test_concurrent_execute(self) -> None:
        r = ResilienceRouter()
        for t in r.get_tiers("analytics"):
            if t.provider != "memory_counter":
                for _ in range(t.circuit_breaker.max_failures):
                    t.circuit_breaker.record_failure("trip")
        results: list[bool] = []
        barrier = threading.Barrier(5)

        def worker(i: int) -> None:
            barrier.wait()
            results.append(bool(r.execute("analytics", "track", event=f"c{i}")))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertTrue(all(results))


class TestAccessors(unittest.TestCase):

    def test_memory_cache(self) -> None:
        self.assertIsInstance(ResilienceRouter().memory_cache, MemoryCache)

    def test_memory_counter(self) -> None:
        self.assertIsInstance(ResilienceRouter().memory_counter, MemoryCounter)

    def test_local_db(self) -> None:
        self.assertIsInstance(ResilienceRouter().local_db, LocalJSONDB)

    def test_get_tiers(self) -> None:
        tiers = ResilienceRouter().get_tiers("caching")
        self.assertTrue(all(isinstance(t, ServiceTier) for t in tiers))

    def test_get_tiers_unknown(self) -> None:
        self.assertEqual(ResilienceRouter().get_tiers("x"), [])


if __name__ == "__main__":
    unittest.main()
