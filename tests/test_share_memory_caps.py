"""Memory bounds for the in-memory plan stores, now that entries live 24 hours.

Background: the cache sweep used to delete every shared plan / plan result each
5 minutes (see test_cache_cleanup_ttl.py), which accidentally bounded memory.
With the TTL fixed, ``POST /api/plan/share`` (unauthenticated, 1 MB body cap)
would otherwise let one client park ``_SHARED_PLANS_MAX`` (1000) plans of up to
~7 MiB resident each in every worker. Bounds under test:

  * per plan: ``_SHARED_PLAN_MAX_BYTES`` (256 KiB of JSON) -> 413 above it,
    and the free-text ``client`` label is truncated so it cannot smuggle bytes;
  * whole store: ``_SHARED_PLANS_MAX_EST_BYTES`` (estimated resident bytes) and
    ``_SHARED_PLANS_MAX`` (count), both oldest-first, enforced at insert time
    (so a burst cannot outrun the 5-minute sweep) and again by the sweep;
  * ``_plan_results_store``: ``_PLAN_RESULTS_MAX`` (200) oldest-first, at insert
    and in the sweep.

The route tests go over real HTTP against an in-process ``app.ThreadedHTTPServer``.
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from typing import Any, Iterator

import pytest

import app
from tests.sweep_driver import run_one_sweep


@pytest.fixture(scope="module")
def share_server() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-share-caps-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("share_server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


@pytest.fixture(autouse=True)
def _no_rate_limit_and_clean_stores(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep the shared per-IP limiter out of these tests; isolate both stores."""
    monkeypatch.setattr(app._rl_general, "is_allowed", lambda *a, **k: True)
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


def _csrf_token(port: int) -> str:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request("GET", "/api/csrf-token")
    resp = conn.getresponse()
    token = json.loads(resp.read())["token"]
    conn.close()
    return token


def _share(port: int, plan_data: Any, client: str = "Acme") -> tuple[int, dict]:
    token = _csrf_token(port)
    body = json.dumps({"plan_data": plan_data, "client": client}).encode("utf-8")
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request(
        "POST",
        "/api/plan/share",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Cookie": f"csrf_token={token}",
            "X-CSRF-Token": token,
        },
    )
    resp = conn.getresponse()
    payload = json.loads(resp.read() or b"{}")
    conn.close()
    return resp.status, payload


def _plan_of(n_bytes: int) -> dict[str, str]:
    return {"blob": "x" * n_bytes}


class TestSharePerPlanCap:
    def test_oversized_plan_is_rejected_with_413_and_not_stored(
        self, share_server: int
    ) -> None:
        status, payload = _share(share_server, _plan_of(300 * 1024))
        assert status == 413
        assert payload["success"] is False
        assert "too large" in payload["error"]
        assert "256 KiB" in payload["error"]
        assert app._shared_plans == {}

    def test_plan_just_over_the_cap_is_rejected(self, share_server: int) -> None:
        limit = app._SHARED_PLAN_MAX_BYTES
        status, _ = _share(share_server, _plan_of(limit))  # + JSON framing > limit
        assert status == 413

    def test_realistic_plan_is_accepted_and_sized(self, share_server: int) -> None:
        status, payload = _share(share_server, _plan_of(130 * 1024))
        assert status == 200
        entry = app._shared_plans[payload["share_id"]]
        # est_bytes is the serialized size scaled by the resident-memory factor
        assert entry["est_bytes"] >= 130 * 1024 * app._SHARED_PLAN_MEM_FACTOR

    def test_client_label_is_truncated(self, share_server: int) -> None:
        status, payload = _share(share_server, {"a": 1}, client="C" * 100_000)
        assert status == 200
        assert len(app._shared_plans[payload["share_id"]]["client"]) == 200


class TestShareStoreBounds:
    def test_count_cap_evicts_oldest_at_insert(
        self, share_server: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(app, "_SHARED_PLANS_MAX", 3)
        ids = []
        for i in range(5):
            status, payload = _share(share_server, {"n": i + 1})
            assert status == 200
            ids.append(payload["share_id"])
        assert set(app._shared_plans) == set(ids[-3:])

    def test_total_size_budget_evicts_oldest_at_insert(
        self, share_server: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Each ~50 KB plan is ~400 KB estimated; a 1,000,000-byte budget fits 2.
        monkeypatch.setattr(app, "_SHARED_PLANS_MAX_EST_BYTES", 1_000_000)
        ids = []
        for _ in range(4):
            status, payload = _share(share_server, _plan_of(50_000))
            assert status == 200
            ids.append(payload["share_id"])
        assert set(app._shared_plans) == set(ids[-2:])
        total = sum(e["est_bytes"] for e in app._shared_plans.values())
        assert total <= 1_000_000

    def test_sweep_also_enforces_the_size_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(app, "_SHARED_PLANS_MAX_EST_BYTES", 1_000)
        now = app.time.time()
        for i, age in enumerate([50, 40, 30]):
            app._shared_plans[f"s{i}"] = {"created_at": now - age, "est_bytes": 400}
        run_one_sweep(monkeypatch)
        assert set(app._shared_plans) == {"s1", "s2"}  # 800 <= 1000 < 1200

    def test_estimate_total_never_exceeds_budget_for_max_size_plans(self) -> None:
        """Worst case by construction: the per-plan cap times the memory factor
        is far below the store budget, so the budget always has room for the
        entry being inserted."""
        worst = app._SHARED_PLAN_MAX_BYTES * app._SHARED_PLAN_MEM_FACTOR
        assert worst * 4 < app._SHARED_PLANS_MAX_EST_BYTES


class TestPlanResultsCap:
    def test_store_plan_result_evicts_oldest_beyond_cap(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(app, "_PLAN_RESULTS_MAX", 3)
        ids = [f"{i:032x}" for i in range(5)]
        for pid in ids:
            app._store_plan_result(pid, {})
            time.sleep(0.002)  # distinct created timestamps
        assert set(app._plan_results_store) == set(ids[-3:])

    def test_sweep_also_enforces_the_cap(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(app, "_PLAN_RESULTS_MAX", 3)
        now = app.time.time()
        for i, age in enumerate([50, 40, 30, 20, 10]):
            app._plan_results_store[f"r{i}"] = {"data": {}, "created": now - age}
        run_one_sweep(monkeypatch)
        assert set(app._plan_results_store) == {"r2", "r3", "r4"}

    def test_default_cap_is_two_hundred(self) -> None:
        assert app._PLAN_RESULTS_MAX == 200
