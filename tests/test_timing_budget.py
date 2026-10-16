"""Tests for tests/timing_budget.py -- the helpers that keep timing asserts
honest on a loaded machine.  None of these assert on a wall-clock threshold."""

from __future__ import annotations

import threading
import time

import pytest

from tests import timing_budget as tb


def test_cpu_timer_does_not_count_time_spent_waiting() -> None:
    with tb.CpuTimer() as t:
        time.sleep(0.2)
    # Sleeping costs ~no CPU however long the scheduler made the sleep take.
    assert t.wall >= 0.19
    assert t.cpu < 0.1, t.describe()


def test_cpu_timer_counts_the_calling_threads_own_work() -> None:
    with tb.CpuTimer() as t:
        end = time.thread_time() + 0.05
        while time.thread_time() < end:
            pass
    assert t.cpu >= 0.05


def test_cpu_timer_ignores_other_threads_burning_cpu() -> None:
    stop = threading.Event()

    def _burn() -> None:
        while not stop.is_set():
            pass

    burner = threading.Thread(target=_burn, daemon=True)
    burner.start()
    try:
        with tb.CpuTimer() as t:
            time.sleep(0.15)
        with tb.ProcessCpuTimer() as p:
            time.sleep(0.15)
    finally:
        stop.set()
        burner.join(timeout=5)
    assert t.cpu < 0.1, "thread_time must not include the burner thread"
    assert p.cpu > t.cpu, "process_time must include it"


def test_load_factor_is_at_least_one_and_capped() -> None:
    factor = tb.load_factor()
    assert 1.0 <= factor <= tb._MAX_LOAD_FACTOR


def test_wall_budget_scales_with_load_but_never_below_idle() -> None:
    assert tb.wall_budget(2.0, factor=1.0) == 2.0
    assert tb.wall_budget(2.0, factor=0.2) == 2.0  # a sub-1 factor cannot tighten it
    assert tb.wall_budget(2.0, factor=5.0) == 10.0


def test_wall_budget_is_held_below_the_time_the_bug_would_take() -> None:
    # An unbounded scale would let a 6 s hang pass a "returns early" assertion.
    assert tb.wall_budget(2.0, regression_s=6.0, factor=25.0) == pytest.approx(5.1)
    # ...but the clamp never drops below the idle budget.
    assert tb.wall_budget(2.0, regression_s=1.0, factor=25.0) == 2.0


def test_assert_wall_below_passes_inside_and_fails_outside_the_budget() -> None:
    tb.assert_wall_below(0.1, 1.0, "fast section")
    with pytest.raises(AssertionError, match="slow section: took 500.00s"):
        tb.assert_wall_below(500.0, 1.0, "slow section")


def test_owner_thread_clock_fakes_time_and_records_sleeps_for_the_owner() -> None:
    real_before = time.time()
    with tb.owner_thread_clock(time, 1000.0) as clock:
        assert time.time() == 1000.0
        clock.now = 1001.5
        assert time.time() == 1001.5
        started = time.monotonic()
        time.sleep(30)  # recorded, not slept
        tb.assert_wall_below(
            time.monotonic() - started, 5.0, "a faked sleep must not block", regression_s=30.0
        )
        assert clock.slept == [30]
    assert time.time() >= real_before  # restored


def test_owner_thread_clock_leaves_other_threads_on_the_real_clock() -> None:
    other_saw: dict = {}
    other_slept = threading.Event()

    def _other() -> None:
        other_saw["time"] = time.time()
        time.sleep(0.05)  # must really sleep, and must not be recorded
        other_slept.set()

    with tb.owner_thread_clock(time, 1000.0) as clock:
        t = threading.Thread(target=_other, daemon=True)
        t.start()
        assert other_slept.wait(timeout=30), "other thread's sleep never returned"
        t.join(timeout=30)
        assert clock.slept == []
    assert other_saw["time"] > 1_000_000.0  # a real epoch time, not the fake 1000.0
