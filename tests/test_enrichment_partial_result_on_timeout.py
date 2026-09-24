"""Regression tests for the Hershey Company "0% location confidence" bug.

REPORTED BUG (Slack, real client plan): every role/location in a generated
plan showed 0% location confidence. Traced (not fixed) by a prior session to
``data_synthesizer.synthesize()``'s ``confidence_scores.per_section
.location_profiles`` correctly reporting 0.0 whenever ``fuse_location_profiles``
sees zero sources for a location -- the scorer itself is not buggy.

ROOT CAUSE (confirmed by reading the code, not by production timing data --
this repo's sandbox has no access to Render logs): ``app.py``'s
``/api/generate`` handler (both the synchronous path, ~app.py:18973, and the
async job path, ~app.py:16573) wraps the single ``api_enrichment.enrich_data()``
call in an OUTER per-request timeout (20s combined across 12 concurrent
enrichment tasks in the sync path; 20s alone in the async path). But
``enrich_data()`` itself fans out into its own ~15-35 sub-API tasks over only
5 worker threads with a 50s internal hard timeout (`_ENRICHMENT_HARD_TIMEOUT`,
api_enrichment.py:15062) -- realistic for a request with roles + locations +
industry + client_name + competitors all populated (a Hershey-shaped request
easily queues 20+ sub-tasks). enrich_data() is NOT the buggy component: it
already returns whatever it has finished on ITS OWN internal timeout
(api_enrichment.py:15146-15161, `except TimeoutError: ... return enriched`).
The bug is that the OUTER caller's shorter timeout fires first, so
``future.result()`` for the api_enrichment task is never read, and the
already-completed partial work inside enrich_data() (e.g. Census demographics
for a small town like Hershey, PA, which -- unlike major metros -- has no
hardcoded METRO_DATA fallback) is thrown away wholesale: ``enriched`` stays
``{}``, ``fuse_location_profiles`` sees zero sources for every location, and
every plan's location confidence reads exactly 0%.

THE FIX: ``enrich_data()`` gained an optional ``partial_result`` dict it
mutates live (same object, from its worker thread) instead of only ever
returning a private local dict. Both app.py call sites pass in a dict they
already hold a reference to, and on an outer TimeoutError now fall back to a
snapshot of that dict (``app._partial_enrichment_snapshot``) instead of
unconditionally discarding everything. This mirrors the pattern the other
11 concurrent enrichment tasks in the sync path already use (each contributes
its result to the shared context as soon as ITS future resolves, regardless
of whether sibling tasks are still pending).

Sections below:
    1. Real reproduction -- data_synthesizer.synthesize() (unmocked) proves
       enriched=={} yields exactly 0.0 location-confidence for a real,
       non-metro location, and that a plausible partial `enriched` (what
       enrich_data() would have gathered by the time a mid-flight timeout
       hits) yields a nonzero score instead.
    2. api_enrichment.enrich_data(partial_result=...) -- proves the dict
       passed in is mutated live, in place, from the worker thread, before
       the function itself returns -- the actual new capability the fix
       depends on.
    3. app._partial_enrichment_snapshot -- the small helper both app.py call
       sites use to safely adopt that partial dict after an outer timeout.
    4. End-to-end pattern test -- reproduces the exact app.py control flow
       (ThreadPoolExecutor + an outer timeout shorter than the callee's own
       budget) using the real api_enrichment.enrich_data() and the real
       app._partial_enrichment_snapshot helper, and proves that with the fix
       applied, an outer timeout keeps the fast sub-task's real result
       instead of discarding it.
"""

from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import app as app_module  # noqa: E402
import data_synthesizer  # noqa: E402

_HERSHEY_INPUT = {
    "roles": ["Production Associate"],
    "locations": ["Hershey, PA"],
    "industry": "manufacturing",
}


# ─── 1. Real reproduction: the client-visible symptom ────────────────────────


def test_empty_enriched_yields_zero_location_confidence_for_real_client_location():
    """Reproduces the reported bug with the real (unmocked) synthesis code.

    "Hershey, PA" -- the actual client's own plan location -- is deliberately
    used instead of a major metro: fuse_location_profiles has a hardcoded
    METRO_DATA fallback for big cities (verified separately: Columbus, OH
    scores 0.4 even with enriched=={}), which would mask this exact bug.
    Small/mid-size towns like the real client's have no such fallback, so an
    all-or-nothing discard of enrich_data()'s work is the only thing standing
    between them and real Census/GeoNames data.
    """
    result = data_synthesizer.synthesize({}, {}, _HERSHEY_INPUT)

    location_confidence = result["confidence_scores"]["per_section"][
        "location_profiles"
    ]
    assert location_confidence == 0.0
    profile = result["location_profiles"]["Hershey, PA"]
    assert profile["_meta"]["source_count"] == 0


def test_partial_enriched_yields_nonzero_location_confidence():
    """The fix's payoff: whatever enrich_data() finished before a timeout
    (e.g. Census demographics, one of its faster sub-tasks) is enough on its
    own to move location confidence off 0% for the same location/input.
    """
    partial_enriched = {
        "location_demographics": {
            "Hershey, PA": {
                "population": 14257,
                "median_income": 71000,
                "source": "Census-ACS",
            }
        },
    }

    result = data_synthesizer.synthesize(partial_enriched, {}, _HERSHEY_INPUT)

    location_confidence = result["confidence_scores"]["per_section"][
        "location_profiles"
    ]
    assert location_confidence > 0.0
    profile = result["location_profiles"]["Hershey, PA"]
    assert profile["population"] == 14257
    assert profile["_meta"]["source_count"] == 1


# ─── 2. api_enrichment.enrich_data(partial_result=...) live mutation ────────


@pytest.fixture
def _no_circuit_breaker_or_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate these tests from circuit-breaker/rate-limiter state any other
    test module in the same pytest process may have left behind for the
    real API labels ("Census-ACS", "WorldBank") used below.
    """
    monkeypatch.setattr(api_enrichment, "_circuit_breaker_check", lambda label: False)
    monkeypatch.setattr(api_enrichment, "_rate_limit_check", lambda label: False)
    monkeypatch.setattr(
        api_enrichment, "_circuit_breaker_record_success", lambda label: None
    )
    monkeypatch.setattr(api_enrichment, "_rate_limit_record", lambda label: None)


def test_enrich_data_partial_result_is_mutated_in_place(
    monkeypatch: pytest.MonkeyPatch, _no_circuit_breaker_or_rate_limit
) -> None:
    """The dict passed as `partial_result` IS the object enrich_data() fills
    (not a copy handed back only at the end) -- the core new capability.
    """
    monkeypatch.setattr(
        api_enrichment,
        "fetch_location_demographics",
        lambda locations: {"Hershey, PA": {"population": 14257}},
    )
    monkeypatch.setattr(api_enrichment, "fetch_global_indicators", lambda locations: {})

    shared: dict = {}
    result = api_enrichment.enrich_data(
        {"locations": ["Hershey, PA"]}, partial_result=shared
    )

    assert shared is result
    assert shared["location_demographics"] == {"Hershey, PA": {"population": 14257}}


def test_enrich_data_partial_result_visible_before_function_returns(
    monkeypatch: pytest.MonkeyPatch, _no_circuit_breaker_or_rate_limit
) -> None:
    """The exact mechanism the app.py fix relies on: a fast sub-task's real
    result is already sitting in the shared dict while a slower sibling
    sub-task is still running -- i.e. long before enrich_data() itself
    returns. An outer caller with a shorter timeout than enrich_data()'s own
    budget can therefore still read it.
    """
    slow_task_may_return = threading.Event()
    slow_task_started = threading.Event()

    def _fast_fetch(locations):
        return {"Hershey, PA": {"population": 14257, "source": "Census-ACS"}}

    def _slow_fetch(locations):
        slow_task_started.set()
        slow_task_may_return.wait(timeout=10)
        return {}

    monkeypatch.setattr(api_enrichment, "fetch_location_demographics", _fast_fetch)
    monkeypatch.setattr(api_enrichment, "fetch_global_indicators", _slow_fetch)

    shared: dict = {}
    driver_pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = driver_pool.submit(
            api_enrichment.enrich_data,
            {"locations": ["Hershey, PA"]},
            partial_result=shared,
        )

        # Wait for the slow sub-task to actually start (deterministic
        # handshake, no sleep-and-hope), then give the fast sub-task a brief
        # moment to complete and write into `shared` -- it has nothing to
        # wait on, so this is generous, not a race.
        assert slow_task_started.wait(timeout=10)
        deadline = time.time() + 5
        while not shared.get("location_demographics") and time.time() < deadline:
            time.sleep(0.01)

        # enrich_data() has NOT returned yet (blocked on the slow sub-task),
        # but its partial progress is already visible in `shared`.
        assert not future.done()
        assert shared["location_demographics"] == {
            "Hershey, PA": {"population": 14257, "source": "Census-ACS"}
        }

        # Let enrich_data() finish for real and drain the background thread
        # before the test ends.
        slow_task_may_return.set()
        final_result = future.result(timeout=10)
        assert final_result is shared
    finally:
        driver_pool.shutdown(wait=True)


# ─── 3. app._partial_enrichment_snapshot ─────────────────────────────────────


def test_partial_enrichment_snapshot_empty_dict_yields_empty_dict():
    assert app_module._partial_enrichment_snapshot({}) == {}


def test_partial_enrichment_snapshot_returns_independent_copy():
    """A copy, not the same object -- so a straggler enrich_data() thread
    still writing into the original after the outer timeout can't keep
    mutating what the caller already moved on to use downstream.

    The real code only ever does top-level key REPLACEMENT on this dict
    (``enriched[result_key] = result``, api_enrichment.py's per-task
    as_completed loop -- never an in-place mutation of an already-published
    result), so a shallow copy is the right amount of independence: a
    straggler task publishing a late result for the same key must not
    retroactively change the snapshot already handed to the caller.
    """
    partial = {"location_demographics": {"Hershey, PA": {"population": 14257}}}
    snapshot = app_module._partial_enrichment_snapshot(partial)

    assert snapshot == partial
    assert snapshot is not partial

    partial["location_demographics"] = {"Hershey, PA": {"population": 999999}}
    assert snapshot["location_demographics"]["Hershey, PA"]["population"] == 14257


# ─── 4. End-to-end pattern: outer timeout shorter than enrich_data()'s own ──


def test_outer_timeout_keeps_partial_enrichment_instead_of_discarding_it(
    monkeypatch: pytest.MonkeyPatch, _no_circuit_breaker_or_rate_limit
) -> None:
    """Reproduces app.py's actual control flow: a single enrich_data() future
    submitted to a pool, read with `future.result(timeout=...)` where the
    outer timeout is shorter than what enrich_data() needs (mirroring the
    real 20s-outer-vs-50s-inner mismatch, just compressed to run in
    milliseconds). Fails (asserts enriched stays {}) against the pre-fix
    pattern; passes against the fix.
    """
    slow_task_may_return = threading.Event()
    slow_task_started = threading.Event()

    monkeypatch.setattr(
        api_enrichment,
        "fetch_location_demographics",
        lambda locations: {
            "Hershey, PA": {"population": 14257, "source": "Census-ACS"}
        },
    )

    def _slow_global_indicators(locations):
        slow_task_started.set()
        slow_task_may_return.wait(timeout=10)
        return {}

    monkeypatch.setattr(
        api_enrichment, "fetch_global_indicators", _slow_global_indicators
    )

    _enrich_data_partial: dict = {}
    enriched: dict = {}
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-enrich")
    try:
        future = pool.submit(
            api_enrichment.enrich_data,
            {"locations": ["Hershey, PA"]},
            partial_result=_enrich_data_partial,
        )
        assert slow_task_started.wait(timeout=10)
        # Give the fast sub-task a moment to land in the shared dict, same
        # as the app.py sync-path test above.
        deadline = time.time() + 5
        while (
            not _enrich_data_partial.get("location_demographics")
            and time.time() < deadline
        ):
            time.sleep(0.01)

        with pytest.raises(FutureTimeoutError):
            future.result(timeout=0.05)  # outer budget << enrich_data()'s own

        # THE FIX under test:
        if not enriched:
            enriched = app_module._partial_enrichment_snapshot(_enrich_data_partial)

        assert enriched, (
            "outer timeout discarded 100% of enrich_data()'s completed work "
            "-- the exact all-or-nothing bug this test guards against"
        )
        assert enriched["location_demographics"] == {
            "Hershey, PA": {"population": 14257, "source": "Census-ACS"}
        }

        # And that non-empty enriched dict is enough to move location
        # confidence off 0% -- the client-visible end of the bug.
        synth = data_synthesizer.synthesize(enriched, {}, _HERSHEY_INPUT)
        assert synth["confidence_scores"]["per_section"]["location_profiles"] > 0.0
    finally:
        slow_task_may_return.set()
        future.result(timeout=10)
        pool.shutdown(wait=True)
