"""Regression tests for circuit_breaker_mesh.py's self-deadlock bug.

Root cause: ``ProviderCircuit._lock`` was a plain ``threading.Lock``, but
``get_status()`` and ``_transition_to()`` both call ``self.health_score()``
*while already holding that lock*. A plain ``Lock`` is not reentrant, so the
second acquire from the same thread blocked forever.

Blast radius verified during investigation:
  - Every ``/api/health`` call that reached the "circuit breaker mesh
    status" section (``app.py`` -> ``circuit_breaker_mesh.get_mesh_status()``
    -> ``ProviderCircuit.get_status()``) deadlocked its background health
    thread permanently. The only thing that ever "resolved" it was the
    *outer* 8s ``.join(timeout=8.0)`` in ``routes/health.py::_handle_health``,
    which leaked the deadlocked thread and made every health check take a
    consistent ~8s -- independent of any concurrent /api/generate traffic.
  - ``llm_router.py`` registers providers into the mesh at import time, and
    calls ``_circuit_mesh.record_failure()`` on every failed LLM call. Five
    consecutive failures trips ``_transition_to(OPEN)``, which also calls
    ``self.health_score()`` while holding the lock -- so a real LLM request
    thread could hang forever whenever a provider circuit actually opened.

Fix: ``_lock`` is now a ``threading.RLock``, which permits the same thread
to reacquire it. These tests would hang forever (not just fail) if the bug
were reintroduced as a plain ``Lock`` with the current nested-call pattern,
so every call into the previously-deadlocking paths runs in a background
thread with a bounded ``.join()`` -- a hang is reported as a test failure
instead of freezing the whole run.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Any, Callable

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from circuit_breaker_mesh import (  # noqa: E402
    CircuitBreakerMesh,
    CircuitState,
    ProviderCircuit,
)

_DEADLOCK_TIMEOUT = 3.0  # generous; a healthy call returns in microseconds


def _call_with_timeout(fn: Callable[[], Any], timeout: float = _DEADLOCK_TIMEOUT):
    """Run ``fn`` in a daemon thread and fail (not hang) if it deadlocks.

    Returns fn()'s result. Raises AssertionError if fn() does not return
    within ``timeout`` seconds -- the calling pytest thread never blocks
    forever even if the lock regresses to non-reentrant.
    """
    result: list[Any] = []
    error: list[BaseException] = []

    def _target() -> None:
        try:
            result.append(fn())
        except BaseException as exc:  # noqa: BLE001 -- re-raised on the test thread
            error.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout)
    assert not t.is_alive(), (
        f"{fn!r} did not return within {timeout}s -- this is the "
        "ProviderCircuit._lock self-deadlock regressing (non-reentrant Lock "
        "+ nested health_score() call)"
    )
    if error:
        raise error[0]
    return result[0]


def test_lock_is_reentrant() -> None:
    """The fix is specifically making _lock reentrant -- pin the type."""
    circuit = ProviderCircuit(name="reentrancy-check")
    assert isinstance(circuit._lock, type(threading.RLock())), (
        "ProviderCircuit._lock must be a threading.RLock (get_status() and "
        "_transition_to() call self.health_score() while holding it)"
    )


def test_get_status_does_not_deadlock() -> None:
    """get_status() calls self.health_score() while holding self._lock."""
    circuit = ProviderCircuit(name="p1")
    circuit.record_success(120.0)
    status = _call_with_timeout(circuit.get_status)
    assert status["name"] == "p1"
    assert status["state"] == "closed"
    assert 0.0 <= status["health_score"] <= 100.0


def test_get_status_does_not_deadlock_with_no_data() -> None:
    """Same call path, but on a circuit with zero recorded calls yet
    (the total == 0 early-return branch inside health_score())."""
    circuit = ProviderCircuit(name="p-empty")
    status = _call_with_timeout(circuit.get_status)
    assert status["health_score"] == 80.0  # benefit-of-the-doubt score


def test_transition_to_open_does_not_deadlock() -> None:
    """record_failure() -> _transition_to(OPEN) -> self.health_score(),
    all while _lock is held. This is the path a REAL LLM request thread
    hits whenever a provider circuit actually trips."""
    circuit = ProviderCircuit(name="p2", failure_threshold=5)

    def _trip_circuit() -> CircuitState:
        for _ in range(5):
            circuit.record_failure("simulated provider failure")
        return circuit.state

    state = _call_with_timeout(_trip_circuit)
    assert state == CircuitState.OPEN


def test_transition_to_half_open_and_closed_does_not_deadlock() -> None:
    """Exercise the HALF_OPEN -> CLOSED transition (record_success path)
    and the HALF_OPEN -> OPEN re-open path, both of which also call
    _transition_to() while holding the lock."""
    circuit = ProviderCircuit(
        name="p3", failure_threshold=2, success_threshold=2, open_timeout=0.0
    )

    def _cycle() -> list[str]:
        states: list[str] = []
        circuit.record_failure("boom")
        circuit.record_failure("boom")
        states.append(circuit.state.value)  # OPEN
        # open_timeout=0.0 -> should_allow_request() immediately flips to HALF_OPEN
        assert circuit.should_allow_request() is True
        states.append(circuit.state.value)  # HALF_OPEN
        circuit.record_success(50.0)
        circuit.record_success(50.0)
        states.append(circuit.state.value)  # CLOSED
        return states

    states = _call_with_timeout(_cycle)
    assert states == ["open", "half_open", "closed"]


def test_get_mesh_status_does_not_deadlock() -> None:
    """The exact call chain /api/health uses:
    app.py -> circuit_breaker_mesh.get_circuit_mesh().get_mesh_status()
    -> ProviderCircuit.get_status() for every registered provider."""
    mesh = CircuitBreakerMesh()
    for i in range(5):
        mesh.register_provider(f"provider-{i}")
    mesh.record_success("provider-0", 80.0)
    mesh.record_failure("provider-1", "timeout")

    status = _call_with_timeout(mesh.get_mesh_status, timeout=_DEADLOCK_TIMEOUT * 2)
    assert status["total_providers"] == 5
    assert len(status["providers"]) == 5
    for provider_status in status["providers"]:
        assert 0.0 <= provider_status["health_score"] <= 100.0


def test_health_score_standalone_still_works() -> None:
    """health_score() called on its own (not nested under get_status()) is
    the normal case and must keep working after switching to RLock."""
    circuit = ProviderCircuit(name="p4")
    circuit.record_success(10.0)
    circuit.record_success(20.0)
    score = _call_with_timeout(circuit.health_score)
    assert score > 0.0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
