"""Drive a real ``app`` background-cleanup loop through exactly one sweep.

Shared by the cleanup regression tests. ``app._cache_cleanup_loop`` and
``app._cleanup_generation_jobs`` are ``while True: time.sleep(N); sweep`` loops
running in daemon threads; to test the REAL sweep (not a re-implementation) we
run the loop in a private thread and intercept its ``time.sleep``.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

import pytest

import app


class _StopSweep(BaseException):
    """Raised inside the sweep thread to end the loop after one sweep.

    Derives from BaseException on purpose: the loop's own ``except Exception``
    handlers must not swallow it.
    """


def run_one_sweep(
    monkeypatch: pytest.MonkeyPatch,
    loop: Callable[[], None] | None = None,
) -> list[float]:
    """Run a real ``app`` cleanup loop (default ``_cache_cleanup_loop``) once.

    ``app.time`` is swapped for a proxy whose ``sleep`` is intercepted ONLY in
    the dedicated sweep thread (other threads keep the real sleep). The first
    sleep returns immediately (so the sweep runs); the second raises
    ``_StopSweep``. Returns the durations the loop asked to sleep.
    """
    real_time = app.time
    sleeps: list[float] = []
    holder: dict[str, threading.Thread] = {}

    class _TimeProxy:
        def __getattr__(self, name: str) -> Any:
            return getattr(real_time, name)

        def sleep(self, seconds: float) -> None:
            if threading.current_thread() is not holder.get("t"):
                real_time.sleep(seconds)
                return
            sleeps.append(seconds)
            if len(sleeps) > 1:
                raise _StopSweep()

    def _target() -> None:
        try:
            (loop or app._cache_cleanup_loop)()
        except _StopSweep:
            pass

    thread = threading.Thread(target=_target, name="test-one-sweep", daemon=True)
    holder["t"] = thread
    monkeypatch.setattr(app, "time", _TimeProxy())
    thread.start()
    thread.join(timeout=30)
    assert not thread.is_alive(), "cleanup loop did not stop after one sweep"
    return sleeps
