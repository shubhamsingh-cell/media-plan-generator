"""Timing helpers that keep elapsed-time assertions honest on a loaded machine.

Why this exists
---------------
``scripts/ship_from_worktree.sh`` runs the whole suite and aborts the ship on
any failure, with no retry.  Several sessions run suites at the same time, so
load averages of 40-130 on 15 cores are routine, and a CPU-bound section that
takes 1.7 s on an idle box took 37 s of wall time in a real ship run
(``test_offline_run_is_fast``).  A budget written against an idle machine
therefore fails for correct code, and the failure says nothing about the change
being shipped.

The rule these helpers encode, by what the test is protecting:

* **CPU-bound "must be fast"** (a regex that must stay linear, a scan that must
  not go quadratic): measure *CPU time*, not wall time.  CPU time is what the
  algorithm costs; it does not grow when other processes compete for the cores
  (:class:`CpuTimer`).  A genuine regression still shows up, because the
  regressions these tests guard are 10x-1000x, not 20 %.
* **Waiting / IO "must not block"** (returns before a hung dependency does,
  runs calls concurrently): CPU time cannot see waiting, so keep wall time but
  scale the idle budget by how loaded the box is right now
  (:func:`wall_budget`), and never let the scaled budget reach the time the bug
  would take (``regression_s``), so the assertion still fails on the bug.
  Where there is also a structural twin (a call count, an "unresolved" list, a
  leaked-thread check) the clock is only a backstop.
* **Concurrency / ordering**: do not infer it from a stopwatch at all.  Record
  the property directly (peak number of calls in flight, which side started
  first).  See ``FakeNet.peak_inflight`` and ``_FakeAsyncWebCrawler``.
* **Mocked clocks** (``time.time`` / ``time.sleep``): ``mock.patch.object(
  time_module, "sleep")`` patches the *module*, so every other thread in the
  pytest process (leaked background loops from other tests are common) lands in
  the mock.  :func:`owner_thread_clock` confines the fake clock and the sleep
  recorder to the calling thread.
"""

from __future__ import annotations

import contextlib
import os
import threading
import time
from types import ModuleType
from typing import Any, Generator, List, Optional
from unittest import mock

# A runaway factor would turn every budget into "anything passes"; cap it.
_MAX_LOAD_FACTOR = 25.0
# CPU seconds burned per calibration sample, and samples taken.
_SPIN_CPU_S = 0.02
_SPIN_SAMPLES = 2


# ---------------------------------------------------------------------------
# CPU-time measurement (CPU-bound "must be fast" tests)
# ---------------------------------------------------------------------------


class CpuTimer:
    """Context manager measuring the CPU time the *calling thread* consumed.

    ``cpu`` is ``time.thread_time()`` deltas: it excludes time spent descheduled
    or sleeping and ignores other threads, so a background loop leaked by
    another test cannot inflate it.  Use it only for work done on the calling
    thread; for work fanned out to worker threads use :class:`ProcessCpuTimer`.
    ``wall`` is kept for failure messages.
    """

    _cpu_clock = staticmethod(time.thread_time)

    def __init__(self) -> None:
        self.cpu: float = 0.0
        self.wall: float = 0.0
        self._c0 = 0.0
        self._w0 = 0.0

    def __enter__(self) -> "CpuTimer":
        self._w0 = time.monotonic()
        self._c0 = self._cpu_clock()
        return self

    def __exit__(self, *_exc_info: Any) -> None:
        self.cpu = self._cpu_clock() - self._c0
        self.wall = time.monotonic() - self._w0

    def describe(self) -> str:
        return f"cpu={self.cpu:.3f}s wall={self.wall:.3f}s load_factor={load_factor():.1f}"


class ProcessCpuTimer(CpuTimer):
    """Like :class:`CpuTimer` but for every thread of the process.

    Needed when the measured work runs on another thread of this process (e.g.
    an in-process HTTP server handling the request).  Any thread burning CPU in
    the same window is counted, so keep the window to the request itself.
    """

    _cpu_clock = staticmethod(time.process_time)


# ---------------------------------------------------------------------------
# Load-scaled wall-clock budgets (waiting / IO tests)
# ---------------------------------------------------------------------------


def _spin_ratio() -> float:
    """wall/cpu of a short CPU burst: how much slower than idle this thread runs.

    1.0 on an idle box.  With N runnable threads per core a spinning thread
    gets about 1/N of a core, so the ratio approximates N.
    """
    ratios: List[float] = []
    for _ in range(_SPIN_SAMPLES):
        w0 = time.perf_counter()
        c0 = time.thread_time()
        while time.thread_time() - c0 < _SPIN_CPU_S:
            pass
        cpu = time.thread_time() - c0
        wall = time.perf_counter() - w0
        ratios.append(wall / cpu if cpu > 0 else 1.0)
    return max(ratios)


def _loadavg_ratio() -> float:
    """1-minute load average per core (a lagging, smoother second opinion)."""
    try:
        return os.getloadavg()[0] / (os.cpu_count() or 1)
    except (OSError, AttributeError):
        return 1.0


def load_factor() -> float:
    """How many times slower than idle wall-clock time currently runs here.

    ``max(spin calibration, loadavg per core)``, clamped to ``[1, 25]``.  The
    spin sees contention *now*; the load average catches a burst the short spin
    happened to slip through.  Always at least 1.0, so a budget scaled by it is
    never stricter than the idle budget.
    """
    return max(1.0, min(_MAX_LOAD_FACTOR, max(_spin_ratio(), _loadavg_ratio())))


def wall_budget(
    idle_budget_s: float,
    *,
    regression_s: Optional[float] = None,
    factor: Optional[float] = None,
) -> float:
    """Wall-clock budget for a section that takes well under it on an idle box.

    Args:
        idle_budget_s: The budget on an idle machine (the original constant).
        regression_s: Wall time the bug being guarded would take (a hang
            duration, the serial total, a sleep that must be skipped).  The
            scaled budget is held to 85 % of it so the assertion still fails on
            the bug; it is never taken below ``idle_budget_s``.
        factor: Override the measured :func:`load_factor` (tests of this module).

    Returns:
        Seconds.  ``idle_budget_s * load_factor``, clamped as described.
    """
    scale = load_factor() if factor is None else max(1.0, factor)
    budget = idle_budget_s * scale
    if regression_s is not None:
        budget = min(budget, regression_s * 0.85)
    return max(budget, idle_budget_s)


def assert_wall_below(
    elapsed_s: float,
    idle_budget_s: float,
    what: str,
    *,
    regression_s: Optional[float] = None,
) -> None:
    """Assert ``elapsed_s`` is within the load-scaled wall budget.

    The failure message carries the idle budget, the scaled budget and the
    measured load factor, so a red run can be told apart from a real
    regression (a real one blows through even the scaled budget).
    """
    factor = load_factor()
    budget = wall_budget(idle_budget_s, regression_s=regression_s, factor=factor)
    assert elapsed_s < budget, (
        f"{what}: took {elapsed_s:.2f}s, budget {budget:.2f}s "
        f"(idle budget {idle_budget_s:.2f}s x load factor {factor:.1f}"
        + (f", held below the {regression_s:.1f}s the bug would take" if regression_s else "")
        + ")"
    )


# ---------------------------------------------------------------------------
# Thread-confined fake clock (mocked time.time / time.sleep)
# ---------------------------------------------------------------------------


class OwnerThreadClock:
    """Fake ``time.time`` / ``time.sleep`` visible to one thread only.

    ``now`` is what ``time.time()`` returns on the owning thread (set it between
    steps); ``slept`` records the owning thread's ``time.sleep`` arguments
    *without sleeping*.  Every other thread keeps the real clock and the real
    sleep, so a leaked background loop can neither pollute ``slept`` nor spin
    hot on an instant fake sleep.
    """

    def __init__(self, now: float) -> None:
        self.now: float = now
        self.slept: List[float] = []
        self._owner = threading.get_ident()

    @property
    def owner(self) -> int:
        return self._owner


@contextlib.contextmanager
def owner_thread_clock(
    time_module: ModuleType, now: float
) -> Generator[OwnerThreadClock, None, None]:
    """Patch ``time_module.time`` and ``.sleep`` for the calling thread only.

    Args:
        time_module: Normally ``vs.time`` -- which *is* the global ``time``
            module, so the patch is process-wide unless made thread-aware.
        now: Initial fake wall-clock reading for the owning thread.
    """
    real_time = time_module.time
    real_sleep = time_module.sleep
    clock = OwnerThreadClock(now)

    def _time() -> float:
        return clock.now if threading.get_ident() == clock.owner else real_time()

    def _sleep(seconds: float) -> None:
        if threading.get_ident() == clock.owner:
            clock.slept.append(seconds)
        else:
            real_sleep(seconds)

    with mock.patch.object(time_module, "time", _time), mock.patch.object(
        time_module, "sleep", _sleep
    ):
        yield clock
