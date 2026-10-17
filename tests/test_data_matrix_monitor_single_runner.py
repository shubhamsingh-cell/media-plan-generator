"""DataMatrixMonitor must run its periodic check on ONE worker, quietly.

Render log burst 2026-10-16 04:17-04:18Z: ~4,400 ``budget_engine`` lines in two
minutes and four ``DataMatrixMonitor: check #2`` lines. Under gunicorn
``--preload`` + gevent the monitor's background thread is not one-per-instance
(see wsgi.py's note on the vector-index thread), so every copy ran the full
4x9 probe, and each probe call logged its budget_engine INFO/WARNING lines.
"""

from __future__ import annotations

import logging
from unittest import mock

import pytest

import data_matrix_monitor as dmm


class _StopLoop(Exception):
    pass


@pytest.fixture()
def slot_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("NOVA_SLOT_DIR", str(tmp_path))
    return tmp_path


def _drive_one_cycle(monitor: dmm.DataMatrixMonitor) -> None:
    """Run the real background loop for exactly one cycle (jitter sleep skipped)."""
    sleeps = iter([None])

    def fake_sleep(_seconds: float) -> None:
        try:
            next(sleeps)
        except StopIteration:
            raise _StopLoop

    with mock.patch.object(dmm.time, "sleep", fake_sleep), mock.patch.object(
        monitor, "_check_memory_pressure"
    ), mock.patch.object(monitor, "_check_config_drift"):
        with pytest.raises(_StopLoop):
            monitor._background_loop()


def test_only_one_worker_runs_the_check_when_copies_wake_together(slot_dir):
    a, b = dmm.DataMatrixMonitor(), dmm.DataMatrixMonitor()
    runs = []

    def a_check():
        runs.append("a")
        _drive_one_cycle(b)  # worker B wakes while A is mid-check
        return {}

    def b_check():
        runs.append("b")
        return {}

    with mock.patch.object(a, "run_check", a_check), mock.patch.object(
        b, "run_check", b_check
    ):
        _drive_one_cycle(a)
    assert runs == ["a"]


def test_a_worker_waking_just_after_the_check_does_not_repeat_it(slot_dir):
    a, b = dmm.DataMatrixMonitor(), dmm.DataMatrixMonitor()
    runs = []
    with mock.patch.object(
        a, "run_check", lambda: runs.append("a") or {}
    ), mock.patch.object(b, "run_check", lambda: runs.append("b") or {}):
        _drive_one_cycle(a)
        _drive_one_cycle(b)
    assert runs == ["a"]


def test_the_check_runs_again_once_the_interval_has_passed(slot_dir):
    a, b = dmm.DataMatrixMonitor(), dmm.DataMatrixMonitor()
    runs = []
    with mock.patch.object(
        a, "run_check", lambda: runs.append("a") or {}
    ), mock.patch.object(b, "run_check", lambda: runs.append("b") or {}):
        _drive_one_cycle(a)
        with mock.patch.object(dmm, "_CHECK_INTERVAL", 0):
            _drive_one_cycle(b)
    assert runs == ["a", "b"]


def test_a_run_logs_one_info_summary_and_demotes_the_probe_lines(caplog):
    be = logging.getLogger("budget_engine")

    def noisy_probe(self, product, layer):
        be.info("Channel dollar amounts computed for %d channels", 6)
        be.warning("Zero or negative budget; returning empty allocation")
        return {"status": "ok"}

    monitor = dmm.DataMatrixMonitor()
    with caplog.at_level(logging.DEBUG), mock.patch.object(
        dmm.DataMatrixMonitor, "_probe_layer", noisy_probe
    ):
        monitor.run_check()
    loud = [r for r in caplog.records if r.levelno >= logging.INFO]
    summaries = [
        r for r in loud if r.getMessage().startswith("DataMatrixMonitor: check #")
    ]
    assert len(summaries) == 1, [r.getMessage() for r in loud]
    assert [r for r in loud if r.name == "budget_engine"] == []
    # the per-item lines are still there, at DEBUG
    assert any(
        r.name == "budget_engine" and r.levelno == logging.DEBUG for r in caplog.records
    )


def test_budget_engine_logging_is_untouched_outside_a_check(caplog):
    with caplog.at_level(logging.INFO):
        logging.getLogger("budget_engine").warning("real request warning")
    assert [r.levelno for r in caplog.records if r.name == "budget_engine"] == [
        logging.WARNING
    ]


def test_a_budget_engine_error_inside_a_check_is_not_hidden(caplog):
    def failing_probe(self, product, layer):
        logging.getLogger("budget_engine").error("boom")
        return {"status": "ok"}

    with caplog.at_level(logging.DEBUG), mock.patch.object(
        dmm.DataMatrixMonitor, "_probe_layer", failing_probe
    ):
        dmm.DataMatrixMonitor().run_check()
    assert any(
        r.name == "budget_engine" and r.levelno == logging.ERROR for r in caplog.records
    )


def test_a_worker_that_skipped_the_cycle_serves_the_winners_result(slot_dir):
    winner, other = dmm.DataMatrixMonitor(), dmm.DataMatrixMonitor()
    with mock.patch.object(
        dmm.DataMatrixMonitor, "_probe_layer", lambda *a: {"status": "ok"}
    ):
        dmm._publish_result(winner.run_check())
    status = other.get_status()
    assert status.get("status") in {"healthy", "degraded"}
    assert status["check_number"] == 1
