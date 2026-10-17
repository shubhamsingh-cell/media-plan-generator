"""The enrichment deadline actually ends the work (2026-10-17).

Before: app.py's /api/generate wrapped api_enrichment.enrich_data() in a
20 s outer wait. That wait only ever stopped the CALLER: enrich_data() kept
running its own 50 s pool on 5 threads while holding one of the three
enrichment slots (_enrichment_semaphore), queued sources still got a thread
and ran their full socket timeouts, and the 17 role-based sources were
gated OFF (ENRICH_ROLE_SOURCES) because nothing bounded them.

Now the outer budget travels as a thread-local monotonic deadline
(api_enrichment.set_deadline): app.py's two enrichment wrappers set it on
every task thread; enrich_data() ends its semaphore and pool waits at it,
carries it into its workers, records what it cut off under
``apis_deadline``, and returns by itself; _safe_call() refuses to start a
source once it has passed; every HTTP helper caps its socket timeout and
retry back-off to the time left. Every role-based source also has a
proactive limiter entry.

Every source is stubbed (no network); the real orchestrator and the real
app.py wrappers run.
"""

from __future__ import annotations

import concurrent.futures
import sys
import threading
import time
from pathlib import Path
from typing import Any, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import app as app_module  # noqa: E402
from tests.test_enrich_geopolitical_budget import (  # noqa: E402
    HERSHEY_REQUEST,
    LOCATION_SOURCE_FETCHERS,
    isolated_breakers,  # noqa: F401 -- pytest fixture
    stub_sources,
)

ALL_FETCHERS: List[str] = [
    n
    for n in dir(api_enrichment)
    if (n.startswith("fetch_") or n == "_fetch_h1b_salary_intelligence")
    and callable(getattr(api_enrichment, n))
]

# roles + locations + industry + client + competitors: the widest dispatch.
ROLE_REQUEST = {
    "client_name": "Acme Health",
    "industry": "healthcare_medical",
    "roles": ["Registered Nurse", "Medical Assistant"],
    "locations": ["Denver, CO"],
    "competitors": ["Beta Health"],
}
WIZARD_REQUEST = {
    **{k: v for k, v in ROLE_REQUEST.items() if k != "roles"},
    "target_roles": ROLE_REQUEST["roles"],
}


@pytest.fixture(autouse=True)
def _no_geopolitical_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")


@pytest.fixture(autouse=True)
def _clean_thread_deadline():
    api_enrichment.set_deadline(None)
    yield
    api_enrichment.set_deadline(None)


class _Blocker:
    """A source that blocks until released; counts how many actually started
    and what deadline each worker thread saw."""

    def __init__(self) -> None:
        self.gate = threading.Event()
        self.started = 0
        self.lock = threading.Lock()
        self.remaining_seen: List[Any] = []

    def __call__(self, *args: Any, **kwargs: Any) -> dict:
        with self.lock:
            self.started += 1
            self.remaining_seen.append(api_enrichment.deadline_remaining())
        self.gate.wait(timeout=15)
        return {"late": True}


@pytest.fixture
def blocker():
    b = _Blocker()
    yield b
    b.gate.set()  # release the straggler threads at teardown


def _data(label: str):
    return lambda *args, **kwargs: {"value": label}


# ─── enrich_data() itself ────────────────────────────────────────────────────


def test_sources_not_started_by_the_deadline_are_skipped_not_run(
    monkeypatch, isolated_breakers, blocker
):
    """Every source blocks. With a 0.4 s deadline the pool (5 workers) starts
    at most 5 of them, the rest never get a thread, and enrich_data()
    returns on its own well inside a second with every source accounted
    for as cut off."""
    stub_sources(monkeypatch, ALL_FETCHERS, {n: blocker for n in ALL_FETCHERS})

    t0 = time.monotonic()
    result = api_enrichment.enrich_data(
        dict(ROLE_REQUEST), deadline=time.monotonic() + 0.4
    )
    elapsed = time.monotonic() - t0

    summary = result["enrichment_summary"]
    called = summary["apis_called"]
    assert len(called) >= 15, called
    assert elapsed < 1.0, elapsed
    assert blocker.started <= 5
    assert set(summary["apis_deadline"]) == set(called)
    assert set(summary["apis_failed"]) == set(called)
    statuses = [summary["api_details"][a]["status"] for a in called]
    assert statuses.count("skipped_deadline") >= len(called) - 5
    assert all(s in ("deadline", "skipped_deadline") for s in statuses)
    assert summary["apis_pending"] == []
    assert summary["complete"] is True
    assert summary["confidence_score"] == 0.0
    # The worker threads carried the deadline (and saw time left when they
    # started -- that is why they were allowed to start).
    assert blocker.remaining_seen and all(
        r is not None and 0 < r <= 0.4 for r in blocker.remaining_seen
    ), blocker.remaining_seen


def test_sources_finished_before_the_deadline_are_kept(
    monkeypatch, isolated_breakers, blocker
):
    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {
            **{n: blocker for n in LOCATION_SOURCE_FETCHERS},
            "fetch_location_demographics": _data("census"),
            "fetch_geonames_data": _data("geonames"),
        },
    )

    result = api_enrichment.enrich_data(
        dict(HERSHEY_REQUEST), deadline=time.monotonic() + 0.6
    )

    summary = result["enrichment_summary"]
    assert set(summary["apis_succeeded"]) == {"Census-ACS", "GeoNames"}
    assert result["location_demographics"] == {"value": "census"}
    cut = set(summary["apis_called"]) - {"Census-ACS", "GeoNames"}
    assert set(summary["apis_deadline"]) == cut
    assert cut <= set(summary["apis_failed"])
    # Cut-off sources applied and returned nothing: still in the denominator.
    assert summary["confidence_score"] == round(2 / len(summary["apis_called"]), 3)


def test_without_a_deadline_the_hard_timeout_alone_applies(
    monkeypatch, isolated_breakers
):
    """No deadline anywhere: the pool waits for its sources as before."""
    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {"fetch_location_demographics": lambda *a, **k: (time.sleep(0.3), {"v": 1})[1]},
    )
    result = api_enrichment.enrich_data(dict(HERSHEY_REQUEST))
    summary = result["enrichment_summary"]
    assert "Census-ACS" in summary["apis_succeeded"]
    assert summary["apis_deadline"] == []


def test_semaphore_wait_ends_at_the_deadline(monkeypatch, isolated_breakers):
    """All three enrichment slots busy: the wait for one stops at the
    deadline (it used to be a flat 20 s), nothing is dispatched, and the
    summary says so."""
    stub_sources(monkeypatch, LOCATION_SOURCE_FETCHERS)
    sem = api_enrichment._enrichment_semaphore
    held = 0
    try:
        while sem.acquire(blocking=False):
            held += 1
        assert held == 3
        t0 = time.monotonic()
        result = api_enrichment.enrich_data(
            dict(HERSHEY_REQUEST), deadline=time.monotonic() + 0.3
        )
        elapsed = time.monotonic() - t0
    finally:
        for _ in range(held):
            sem.release()

    assert elapsed < 0.8, elapsed
    summary = result["enrichment_summary"]
    assert summary["apis_succeeded"] == []
    assert summary["geopolitical_status"] == "not_started"
    assert summary["complete"] is True


def test_geopolitical_collect_never_waits_past_the_deadline():
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    gate = threading.Event()
    try:
        handle = {
            "budget": 8.0,
            "started": time.time(),
            "future": pool.submit(gate.wait, 15),
            "executor": pool,
            "status": "pending",
        }
        t0 = time.monotonic()
        ctx, status = api_enrichment._collect_geopolitical_context(
            handle, deadline=time.monotonic() + 0.2
        )
        elapsed = time.monotonic() - t0
    finally:
        gate.set()
        pool.shutdown(wait=False)
    assert elapsed < 0.7, elapsed
    assert (ctx, status) == ({}, "timeout")


def test_bounded_wait_math():
    now = time.monotonic()
    assert api_enrichment._bounded_wait(50, None) == 50.0
    assert 9.4 < api_enrichment._bounded_wait(50, now + 10, grace=0.5) <= 9.5
    assert api_enrichment._bounded_wait(50, now - 1) == 0.0
    assert api_enrichment._bounded_wait(0.2, now + 10) == 0.2


# ─── _safe_call / HTTP helpers ───────────────────────────────────────────────


def test_safe_call_refuses_to_start_past_the_deadline(isolated_breakers):
    api_enrichment.set_deadline(time.monotonic() - 1.0)
    called: list = []
    result, status, meta = api_enrichment._safe_call(
        lambda: called.append(1) or {"x": 1}, "Teleport"
    )
    assert (result, status) == (None, "skipped_deadline")
    assert called == []
    assert meta["source"] == "deadline"


class _Resp:
    def __init__(self, status: int = 200) -> None:
        self.status = status

    def read(self) -> bytes:
        return b'{"ok": 1}'

    def getheader(self, name: str):
        return None


def _fake_pool(monkeypatch, status: int = 200):
    calls: List[Any] = []

    def _fake(url, **kwargs):
        calls.append(kwargs["timeout"])
        return _Resp(status)

    monkeypatch.setattr(api_enrichment, "_pooled_request", _fake)
    monkeypatch.setattr(api_enrichment, "_HAS_POOL", True)
    return calls


@pytest.mark.parametrize(
    "call, expected",
    [
        (lambda: api_enrichment._http_get_json("https://x.test/a"), {"ok": 1}),
        (
            lambda: api_enrichment._http_post_json("https://x.test/a", {"q": 1}),
            {"ok": 1},
        ),
        (lambda: api_enrichment._http_get_text("https://x.test/a"), '{"ok": 1}'),
    ],
    ids=["get_json", "post_json", "get_text"],
)
def test_http_helpers_cap_the_socket_timeout_to_the_deadline(
    monkeypatch, call, expected
):
    calls = _fake_pool(monkeypatch)

    assert call() == expected
    assert calls[-1] == api_enrichment.API_TIMEOUT  # no deadline: as before

    api_enrichment.set_deadline(time.monotonic() + 0.3)
    assert call() == expected
    assert 0 < calls[-1] <= 0.3, calls[-1]

    api_enrichment.set_deadline(time.monotonic() - 1.0)
    before = len(calls)
    assert call() is None  # no socket opened at all
    assert len(calls) == before


def test_urllib_fallback_caps_the_timeout_too(monkeypatch):
    seen: List[Any] = []

    class _Ctx:
        def __enter__(self):
            return _Resp()

        def __exit__(self, *exc):
            return False

    def _fake_urlopen(req, timeout=None, context=None):
        seen.append(timeout)
        return _Ctx()

    monkeypatch.setattr(api_enrichment, "_HAS_POOL", False)
    monkeypatch.setattr(api_enrichment.urllib.request, "urlopen", _fake_urlopen)
    api_enrichment.set_deadline(time.monotonic() + 0.3)
    assert api_enrichment._http_get_json("https://x.test/a") == {"ok": 1}
    assert 0 < seen[-1] <= 0.3


def test_retry_backoff_never_sleeps_past_the_deadline():
    t0 = time.monotonic()
    api_enrichment._backoff_sleep(0.02)
    assert time.monotonic() - t0 >= 0.02  # no deadline: the real wait

    api_enrichment.set_deadline(time.monotonic() + 0.05)
    t0 = time.monotonic()
    api_enrichment._backoff_sleep(1.5)
    assert time.monotonic() - t0 < 0.3


def test_retry_loop_gives_up_when_the_deadline_passes_mid_retry(monkeypatch):
    """A 429 every time: the back-off is capped at the time left and the next
    attempt refuses to open a socket, so the helper returns None at the
    deadline instead of running its full 0.5 s + 1.5 s retry ladder."""
    calls = _fake_pool(monkeypatch, status=429)
    api_enrichment.set_deadline(time.monotonic() + 0.3)
    t0 = time.monotonic()
    assert api_enrichment._http_get_json("https://x.test/a") is None
    assert time.monotonic() - t0 < 0.8
    assert len(calls) <= 2


# ─── every role source behind a limiter ─────────────────────────────────────


def test_every_role_source_has_a_limiter_entry():
    labels = api_enrichment.role_sources_for(True)
    assert len(labels) == 17
    for label in labels:
        entry = api_enrichment._API_RATE_LIMITS[label]
        assert entry["max_calls"] >= 1, label
        assert entry["window_seconds"] >= 60, label


def test_each_role_source_is_refused_after_its_cap(monkeypatch):
    monkeypatch.setattr(api_enrichment, "_circuit_breaker_check", lambda label: False)
    monkeypatch.setattr(
        api_enrichment, "_circuit_breaker_record_success", lambda label: None
    )
    monkeypatch.setattr(api_enrichment, "_rate_limiter_state", {})
    monkeypatch.setattr(
        api_enrichment,
        "_API_RATE_LIMITS",
        {
            label: {"max_calls": 2, "window_seconds": 3600}
            for label in api_enrichment.role_sources_for(True)
        },
    )
    for label in api_enrichment.role_sources_for(True):
        if label in api_enrichment._REQUEST_METERED:
            # Adzuna is counted per HTTP request by adzuna_quota, not per
            # dispatch here; its caps are covered in test_role_source_allowlist.py
            continue
        if label in api_enrichment._PER_POST_METERED:
            # BLS is charged per POST at the request site (_rate_limit_acquire),
            # not per dispatch; covered in test_roles_audit_fixes.py
            continue
        statuses = [
            api_enrichment._safe_call(lambda: {"v": 1}, label)[1] for _ in range(3)
        ]
        assert statuses == ["ok", "ok", "rate_limited"], (label, statuses)
    assert set(api_enrichment._REQUEST_METERED) == {"Adzuna"}


# ─── the real app.py wrappers ────────────────────────────────────────────────


def test_sync_pool_sets_the_deadline_on_every_task_thread():
    seen: dict = {}

    def _probe() -> "tuple[str, dict]":
        """probe"""
        seen["remaining"] = api_enrichment.deadline_remaining()
        seen["thread"] = threading.current_thread().name
        return ("probe", {"ok": True})

    enriched, market_ctx = app_module._run_parallel_enrichment_tasks([_probe], 2.0, {})

    assert market_ctx == {"probe": {"ok": True}}
    assert enriched == {}
    assert seen["remaining"] is not None and 0 < seen["remaining"] <= 2.0
    assert seen["thread"].startswith("gen-enrich")
    assert api_enrichment.get_deadline() is None  # the caller's thread is untouched


def test_async_wrapper_with_role_sources_on_returns_inside_the_budget(
    monkeypatch, isolated_breakers, blocker
):
    """ENRICH_ROLE_SOURCES=1 and every source hangs: the 17 role sources are
    dispatched, enrich_data() still returns by itself inside the outer
    budget (the wrapper's timeout does not fire), and the cut-off sources are
    listed. This is the bound the opt-in used to lack."""
    stub_sources(monkeypatch, ALL_FETCHERS, {n: blocker for n in ALL_FETCHERS})
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "1")

    t0 = time.monotonic()
    enriched, timed_out = app_module._run_enrich_data_with_partial_fallback(
        api_enrichment.enrich_data, dict(WIZARD_REQUEST), "rid-deadline", 0.6
    )
    elapsed = time.monotonic() - t0

    assert elapsed < 1.2, elapsed
    assert timed_out is False
    summary = enriched["enrichment_summary"]
    assert set(api_enrichment.role_sources_for(True)) <= set(summary["apis_called"])
    assert blocker.started <= 5
    assert summary["apis_deadline"]
    assert summary["complete"] is True


def test_outer_timeout_still_guards_a_callable_that_ignores_the_deadline():
    """The safety net: a callable that never looks at the deadline is cut by
    the outer wait, and whatever it had written live is kept."""

    def _ignores_deadline(data, partial_result=None, **kwargs):
        partial_result.update(
            {"location_demographics": {"Hershey, PA": {"population": 14257}}}
        )
        time.sleep(0.8)
        return {"late": True}

    enriched, timed_out = app_module._run_enrich_data_with_partial_fallback(
        _ignores_deadline, {}, None, 0.2
    )
    assert timed_out is True
    assert enriched == {"location_demographics": {"Hershey, PA": {"population": 14257}}}


def test_sync_pool_adopts_the_partial_when_enrich_data_ignores_the_deadline():
    partial: dict = {}

    def _call_enrich_data() -> "tuple[str, dict]":
        partial.update({"location_demographics": {"Hershey, PA": {"population": 1}}})
        time.sleep(0.8)
        return ("api_enrichment", {"late": True})

    enriched, market_ctx = app_module._run_parallel_enrichment_tasks(
        [_call_enrich_data], 0.2, partial
    )
    assert enriched == {"location_demographics": {"Hershey, PA": {"population": 1}}}
    assert market_ctx == {}
