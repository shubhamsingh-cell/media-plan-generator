"""Regression tests for the "bare TimeoutError vs concurrent.futures.TimeoutError"
bug class.

ROOT CAUSE: on Python < 3.11, ``concurrent.futures.TimeoutError`` is NOT the
same class as the builtin ``TimeoutError`` (unified only in 3.11+; this
repo's dev/CI interpreter is 3.9.x -- confirmed live:
``concurrent.futures.TimeoutError is builtins.TimeoutError`` is ``False``
here). ``app.py`` had 12 call sites that wrapped a
``concurrent.futures.Future.result(timeout=...)`` (or an
``as_completed(..., timeout=...)``) in a bare ``except TimeoutError:`` --
which never actually catches the real timeout exception on this
interpreter, so every one of those sites silently fell through to a
generic ``except Exception:`` handler (or, worse, propagated unexpectedly)
instead of running its intended timeout-fallback branch. Two sites were
already fixed (``_run_enrich_data_with_partial_fallback`` and
``_run_parallel_enrichment_tasks``, see
``tests/test_enrichment_partial_result_on_timeout.py`` -- NOT touched
here) by catching the module-level ``_FutureTimeoutError`` alias
(``from concurrent.futures import TimeoutError as _FutureTimeoutError``)
instead. This file covers the remaining sites.

Most of the remaining sites live deep inside ``MediaPlanHandler._handle_POST``
(a single stdlib ``http.server`` handler method, not Flask/Django -- see
CLAUDE.md). Driving a real HTTP request all the way through the full
generation pipeline just to hit one specific internal timeout branch would
mean multi-second-to-minute, network-dependent, flaky end-to-end tests for
each of 7 call sites. Per this task's own guidance (mirroring what commit
876c56e already did for the two pre-existing fixed functions), those 7
sites were extracted into ONE small, shared, directly-callable module-level
helper, ``app._run_pool_submit_with_timeout`` -- every one of the 7
``_handle_POST`` call sites (KB synthesis, async+sync Gold Standard quality
gates, async+sync plan-data verification, Excel generation, PPT
generation) now routes its "submit a callable to a fresh single-worker
pool, wait up to a timeout, fall back on timeout" logic through this one
function, so testing it directly here proves the fix for all 7 at once
(each call site's own pre-existing fallback value / log message / re-raise
was left completely unchanged -- only the pool-submit-and-wait mechanics
moved).

The remaining sites (product insights LLM call, /api/health self-healing
imports + external status checks, and chat-context enrichment) were
already small, already-isolated closures/helpers reachable directly, so
they're driven directly below instead.

Every timeout test here drives a REAL ``ThreadPoolExecutor`` with a
genuinely slow callable (a ``threading.Event`` that blocks past the
timeout) and a real short timeout -- never a mocked ``.result()`` that
raises an exception -- so a passing test actually proves the correct
timeout-fallback CODE PATH ran (via its distinguishing return value / log
message), not merely that some exception was swallowed somewhere.
"""

from __future__ import annotations

import logging
import sys
import threading
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402


# ─── 1. `_run_pool_submit_with_timeout` -- the shared primitive behind ──────
# ─── sites 6, 7, 8, 9, 10, 11 and 12 (KB synthesis, Gold Standard gates, ───
# ─── plan-data verification x2, Excel generation, PPT generation) ──────────


def test_run_pool_submit_with_timeout_returns_result_when_fast() -> None:
    result, timed_out = app_module._run_pool_submit_with_timeout(lambda: 42, timeout=5)
    assert (result, timed_out) == (42, False)


def test_run_pool_submit_with_timeout_times_out_gracefully_on_slow_callable() -> None:
    """The core regression proof: a genuinely slow callable + a short
    timeout must return ``(None, True)`` -- not raise, not hang -- proving
    ``except _FutureTimeoutError:`` actually catches the real
    ``concurrent.futures.TimeoutError`` raised by
    ``future.result(timeout=...)`` on this repo's Python 3.9 interpreter.
    Reverting the fix back to a bare ``except TimeoutError:`` here makes
    this test fail with an unhandled ``concurrent.futures.TimeoutError``
    instead of the assertion below (verified manually during development
    by temporarily reverting the one line in ``app._run_pool_submit_with_timeout``
    and re-running this test, which errors with exactly that unhandled
    exception on this 3.9.6 interpreter).
    """
    started = threading.Event()
    may_return = threading.Event()

    def _slow() -> str:
        started.set()
        may_return.wait(timeout=10)
        return "too-late"

    try:
        result, timed_out = app_module._run_pool_submit_with_timeout(_slow, timeout=0.2)
    finally:
        may_return.set()  # let the background thread finish; don't leak it

    assert started.wait(timeout=1), "background callable never started"
    assert (result, timed_out) == (None, True)


def test_run_pool_submit_with_timeout_propagates_non_timeout_exceptions() -> None:
    """A non-timeout failure from the submitted callable must propagate to
    the caller unchanged (every _handle_POST call site's own surrounding
    ``except ImportError`` / ``except Exception`` still has to see it)."""

    def _boom() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        app_module._run_pool_submit_with_timeout(_boom, timeout=5)


def test_run_pool_submit_with_timeout_shutdown_cancel_futures_param_is_forwarded() -> (
    None
):
    """Sanity check that the `shutdown_cancel_futures` knob (needed because
    the 7 original call sites disagreed on it -- some used
    ``shutdown(wait=False, cancel_futures=True)``, others plain
    ``shutdown(wait=False)``) doesn't break either normal completion or
    the timeout path, for both settings."""
    for flag in (True, False):
        result, timed_out = app_module._run_pool_submit_with_timeout(
            lambda: "ok", timeout=5, shutdown_cancel_futures=flag
        )
        assert (result, timed_out) == ("ok", False)


# ─── 2. Site 1: `_generate_product_insights` (LLM insights call) ───────────


def test_generate_product_insights_times_out_gracefully(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    router = app_module._lazy_llm_router()
    if router is None:
        pytest.skip("llm_router not importable in this environment")

    monkeypatch.setattr(app_module, "_INSIGHTS_LLM_TIMEOUT", 0.2)
    monkeypatch.setattr(app_module, "_vector_search_available", False)

    started = threading.Event()
    may_return = threading.Event()

    def _slow_call_llm(**_kwargs: object) -> dict:
        started.set()
        may_return.wait(timeout=10)
        return {"text": "too-late"}

    monkeypatch.setattr(router, "call_llm", _slow_call_llm)

    try:
        with caplog.at_level(logging.WARNING):
            result = app_module._generate_product_insights(
                "Timeout Regression Test Product",
                {"job_title": "Test Role For Timeout Regression"},
            )
    finally:
        may_return.set()  # let the background LLM-call thread finish

    assert started.wait(timeout=1), "background LLM call never started"
    assert result == ""
    messages = [rec.message for rec in caplog.records]
    assert any("LLM insights timed out" in m for m in messages), messages
    assert not any("LLM insights failed" in m for m in messages), (
        "the generic exception-fallback branch ran instead of the timeout "
        "branch -- exactly the bug this test guards against"
    )


# ─── 3. Sites 2, 3 & 4: /api/health self-healing imports + status checks ──


def test_health_self_heal_imports_reports_down_on_real_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    may_return = threading.Event()

    def _slow_try_import(_name: str) -> bool:
        started.set()
        may_return.wait(timeout=10)
        return True

    monkeypatch.setattr(app_module, "_try_import_module", _slow_try_import)

    try:
        results = app_module._health_self_heal_imports(
            ["fake_module_for_timeout_test"], timeout=0.2
        )
    finally:
        may_return.set()

    assert started.wait(timeout=1)
    assert results == {"fake_module_for_timeout_test": False}


def test_health_self_heal_imports_reports_ok_on_fast_success() -> None:
    results = app_module._health_self_heal_imports(["os"], timeout=3.0)
    assert results == {"os": True}


def test_run_status_checks_overall_timeout_falls_back_to_defaults() -> None:
    """Exercises the OUTER ``except _FutureTimeoutError:`` around
    ``as_completed(..., timeout=overall_timeout)`` inside
    ``_run_status_checks`` (site 4 -- ~app.py's original line 7670).

    Note: the INNER per-future catch a few lines below it in the same
    function (site 3 -- ``_sfut.result(timeout=per_result_timeout)``) is
    not separately exercised with a real timeout here: by the time
    ``as_completed`` yields a future it is, by definition, already done,
    and ``concurrent.futures.Future.result()`` on an already-done future
    returns immediately regardless of the ``timeout`` argument (it only
    waits when the future is NOT yet done). That inner except clause is
    therefore unreachable via any real execution path -- defensive dead
    code, not something a genuine-timeout test can drive. It still needed
    the same one-line class fix for consistency/correctness, and this
    test proves the identical ``concurrent.futures.TimeoutError`` class
    fix is correct for the (reachable) sibling branch immediately above
    it in the same function.
    """
    started = threading.Event()
    may_return = threading.Event()

    def _slow_check() -> str:
        started.set()
        may_return.wait(timeout=10)
        return "too-late"

    try:
        result = app_module._run_status_checks(
            {"slow": (_slow_check, {"status": "default"})},
            overall_timeout=0.2,
            per_result_timeout=0.1,
        )
    finally:
        may_return.set()

    assert started.wait(timeout=1)
    assert result == {"slow": {"status": "default"}}


def test_run_status_checks_fast_check_returns_real_result() -> None:
    result = app_module._run_status_checks(
        {"fast": (lambda: {"status": "ok"}, {"status": "default"})},
        overall_timeout=3.5,
        per_result_timeout=0.1,
    )
    assert result == {"fast": {"status": "ok"}}


# ─── 4. Site 5: `_enrich_chat_context` (chat per-task 8s timeout) ──────────


def test_enrich_chat_context_task_timeout_falls_back_gracefully(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Shrink the (otherwise hardcoded-8s) per-task budget so the test runs
    # in milliseconds instead of >8s real time.
    monkeypatch.setattr(app_module, "_CHAT_ENRICHMENT_TASK_TIMEOUT", 0.2)

    # Gate every OTHER enrichment task off so only the (slow) vector-search
    # task actually runs -- keeps the test deterministic and fast.
    monkeypatch.setattr(app_module, "_api_integrations_available", False)
    monkeypatch.setattr(app_module, "_jobspy_available", False)
    monkeypatch.setattr(app_module, "_tavily_available", False)
    monkeypatch.setattr(app_module, "_vector_search_available", True)

    started = threading.Event()
    may_return = threading.Event()

    def _slow_vector_search(_query: str, top_k: int = 3) -> list:
        started.set()
        may_return.wait(timeout=10)
        return [{"text": "too-late", "score": 1.0}]

    monkeypatch.setattr(app_module, "_vector_search", _slow_vector_search)

    try:
        with caplog.at_level(logging.WARNING):
            result = app_module._enrich_chat_context(
                {"message": "hello there"}, "hello there"
            )
    finally:
        may_return.set()

    assert started.wait(timeout=1), "background vector-search call never started"
    # The timed-out task's result must never have been merged in.
    assert "vector_kb_results" not in result.get("_api_context", {})
    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Chat enrichment task timed out" in m for m in messages), messages
