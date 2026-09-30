"""Regression tests for the cache-cleanup sweep's TTL handling.

Bug fixed: ``app._cache_cleanup_loop`` judged shared plans and async plan
results with

    if now - v.get("created_at") or 0 > _SHARED_PLANS_TTL
    if now - v.get("created")    or 0 > _PLAN_RESULTS_TTL_SECONDS

Python parses ``A - B or 0 > C`` as ``(A - B) or (0 > C)``. ``A - B`` is a
nonzero number (truthy) for every real entry, so EVERY entry was judged stale
and deleted on EVERY sweep: shared-plan links (TTL 24h) and async plan results
(TTL 24h) vanished within one sweep interval (``_CACHE_CLEANUP_INTERVAL`` =
300s) instead of living for their TTL. The intended expression is
``(now - (v.get("created_at") or 0)) > TTL``.

These tests drive the REAL ``_cache_cleanup_loop`` for exactly one sweep (by
intercepting its ``time.sleep`` in the sweep thread only), so they certify the
production code path, not a re-implementation of the predicate.

Missing-timestamp policy (decided + documented here): an entry whose
``created_at`` / ``created`` is missing, ``None`` or ``0`` is treated as STALE
(``now - 0`` is ~1.7e9 seconds, far past any TTL). It is swept on the next
cycle rather than kept forever.
"""

from __future__ import annotations

from typing import Any, Iterator

import pytest

import app
from tests.sweep_driver import run_one_sweep


@pytest.fixture
def isolated_stores() -> Iterator[None]:
    """Empty the two TTL stores for the test and restore them afterwards."""
    with app._shared_plans_lock:
        saved_shared = dict(app._shared_plans)
        app._shared_plans.clear()
    with app._plan_results_lock:
        saved_results = dict(app._plan_results_store)
        app._plan_results_store.clear()
    try:
        yield
    finally:
        with app._shared_plans_lock:
            app._shared_plans.clear()
            app._shared_plans.update(saved_shared)
        with app._plan_results_lock:
            app._plan_results_store.clear()
            app._plan_results_store.update(saved_results)


def _now() -> float:
    return app.time.time()


class TestSharedPlansSweep:
    def test_fresh_entry_survives_sweep(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A just-created share link must still exist after a sweep."""
        app._shared_plans["fresh01"] = {
            "plan_data": {"x": 1},
            "client": "Acme",
            "created_at": _now() - 60,
        }
        run_one_sweep(monkeypatch)
        assert "fresh01" in app._shared_plans

    def test_entry_just_inside_ttl_survives(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app._shared_plans["edge01"] = {
            "plan_data": {"x": 1},
            "created_at": _now() - (app._SHARED_PLANS_TTL - 600),
        }
        run_one_sweep(monkeypatch)
        assert "edge01" in app._shared_plans

    def test_entry_older_than_ttl_is_removed(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app._shared_plans["old001"] = {
            "plan_data": {"x": 1},
            "created_at": _now() - (app._SHARED_PLANS_TTL + 600),
        }
        run_one_sweep(monkeypatch)
        assert "old001" not in app._shared_plans

    @pytest.mark.parametrize("entry", [{}, {"created_at": None}, {"created_at": 0}])
    def test_missing_created_at_is_treated_as_stale(
        self,
        entry: dict,
        isolated_stores: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app._shared_plans["nots01"] = {"plan_data": {"x": 1}, **entry}
        run_one_sweep(monkeypatch)
        assert "nots01" not in app._shared_plans

    def test_mixed_entries_only_stale_ones_go(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = _now()
        app._shared_plans["keep-a"] = {"created_at": now - 10}
        app._shared_plans["keep-b"] = {"created_at": now - 3600}
        app._shared_plans["drop-a"] = {"created_at": now - app._SHARED_PLANS_TTL - 1}
        app._shared_plans["drop-b"] = {}
        run_one_sweep(monkeypatch)
        assert set(app._shared_plans) == {"keep-a", "keep-b"}

    def test_size_cap_evicts_oldest_fresh_entries(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The _SHARED_PLANS_MAX cap branch was unreachable while the TTL bug
        emptied the store every sweep; with the fix it is live, so prove it
        evicts the OLDEST entries and keeps the newest."""
        monkeypatch.setattr(app, "_SHARED_PLANS_MAX", 3)
        now = _now()
        for i, age in enumerate([50, 40, 30, 20, 10]):
            app._shared_plans[f"cap{i}"] = {"created_at": now - age}
        run_one_sweep(monkeypatch)
        assert set(app._shared_plans) == {"cap2", "cap3", "cap4"}


class TestPlanResultsSweep:
    def test_fresh_entry_survives_sweep(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An async plan result must still be retrievable after a sweep."""
        app._plan_results_store["a" * 32] = {
            "data": {"channels": []},
            "created": _now() - 60,
        }
        run_one_sweep(monkeypatch)
        assert "a" * 32 in app._plan_results_store

    def test_entry_with_sheets_url_survives_sweep(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The async Sheets export writes ``sheets_url`` into the entry later;
        a fresh entry carrying it must survive too."""
        app._plan_results_store["b" * 32] = {
            "data": {},
            "created": _now() - 120,
            "sheets_url": "https://example.invalid/sheet",
        }
        run_one_sweep(monkeypatch)
        assert "b" * 32 in app._plan_results_store

    def test_entry_older_than_ttl_is_removed(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app._plan_results_store["c" * 32] = {
            "data": {},
            "created": _now() - (app._PLAN_RESULTS_TTL_SECONDS + 600),
        }
        run_one_sweep(monkeypatch)
        assert "c" * 32 not in app._plan_results_store

    @pytest.mark.parametrize("entry", [{}, {"created": None}, {"created": 0}])
    def test_missing_created_is_treated_as_stale(
        self,
        entry: dict,
        isolated_stores: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app._plan_results_store["d" * 32] = {"data": {}, **entry}
        run_one_sweep(monkeypatch)
        assert "d" * 32 not in app._plan_results_store

    def test_mixed_entries_only_stale_ones_go(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = _now()
        app._plan_results_store["keep-a"] = {"data": {}, "created": now - 5}
        app._plan_results_store["keep-b"] = {"data": {}, "created": now - 7200}
        app._plan_results_store["drop-a"] = {
            "data": {},
            "created": now - app._PLAN_RESULTS_TTL_SECONDS - 1,
        }
        app._plan_results_store["drop-b"] = {"data": {}}
        run_one_sweep(monkeypatch)
        assert set(app._plan_results_store) == {"keep-a", "keep-b"}


class TestSweepCadence:
    def test_loop_sleeps_the_documented_interval(
        self, isolated_stores: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sleeps = run_one_sweep(monkeypatch)
        assert sleeps[0] == app._CACHE_CLEANUP_INTERVAL == 300.0

    def test_ttls_outlive_the_sweep_interval(self) -> None:
        """Sanity: if a TTL were shorter than the sweep interval the store
        would hold expired entries between sweeps; both are far longer."""
        assert app._SHARED_PLANS_TTL > app._CACHE_CLEANUP_INTERVAL
        assert app._PLAN_RESULTS_TTL_SECONDS > app._CACHE_CLEANUP_INTERVAL


class TestIsExpiredHelper:
    def test_fresh_is_not_expired(self) -> None:
        assert app._is_expired(1000.0, now=1100.0, ttl=500.0) is False

    def test_boundary_is_not_expired(self) -> None:
        # Strictly greater-than: an entry exactly ttl old is still alive.
        assert app._is_expired(1000.0, now=1500.0, ttl=500.0) is False

    def test_past_ttl_is_expired(self) -> None:
        assert app._is_expired(1000.0, now=1500.1, ttl=500.0) is True

    @pytest.mark.parametrize("created", [None, 0, 0.0])
    def test_missing_timestamp_is_expired(self, created: Any) -> None:
        assert app._is_expired(created, now=1_700_000_000.0, ttl=86400.0) is True
