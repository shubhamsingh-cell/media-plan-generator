"""Regression tests for the AutoQC cross-process leader lock (app.py).

Pre-fix, app.py's module-level "Autonomous QC engine" block called
auto_qc.start_background() unconditionally. Under gunicorn's --workers 2
+ gevent + --preload, threads started in that block do not simply die at
fork the way a plain OS thread would (see wsgi.py's note on the vector
-index thread "reproduced directly: it re-runs to completion independently
in the master AND in EVERY forked worker") -- so the 60s background
monitor (auto_qc.py's _CHECK_INTERVAL) started once per worker instead of
once per instance, multiplying its 5-endpoint check cycle (including the
heavy "/" and "/api/channels") by the worker count on every pass.

_acquire_auto_qc_leader_lock() fixes this with the same flock-on-shared
-tmpdir primitive already proven correct in this exact deployment by
_CrossProcessSlots (/api/generate's concurrency cap) and
vector_search._voyage_reserve_slot (the Voyage RPM limiter): every
process races to flock() one shared lock file; exactly one wins and
starts the monitor.

These tests exercise that primitive directly, same style as
tests/test_multiprocess_serving.py's cross-process slot tests: calling it
twice in-process, each call opening its own fresh file descriptor on the
same path, faithfully reproduces "two workers racing" because flock()
contends per open-file-description, not per process.
"""

from __future__ import annotations

import tempfile

import pytest

import app as app_module


@pytest.fixture()
def isolated_slot_dir(monkeypatch: pytest.MonkeyPatch) -> str:
    """Point NOVA_SLOT_DIR at a fresh, empty tmpdir for one test.

    app._acquire_auto_qc_leader_lock() re-reads NOVA_SLOT_DIR on every
    call (not cached at import time), so monkeypatching the env var is
    enough -- no module reload required. A fresh directory per test means
    a lock a previous test acquired (and, by design, never releases)
    can't leak into this one.
    """
    d = tempfile.mkdtemp(prefix="nova_autoqc_leader_test_")
    monkeypatch.setenv("NOVA_SLOT_DIR", d)
    return d


def test_first_caller_becomes_leader(isolated_slot_dir: str) -> None:
    assert app_module._acquire_auto_qc_leader_lock() is True


def test_second_caller_loses_the_race(isolated_slot_dir: str) -> None:
    """The bug this fix closes: pre-fix, nothing stopped a second worker
    from also starting the monitor. Post-fix, only the first caller may
    hold the lock at a time."""
    assert app_module._acquire_auto_qc_leader_lock() is True
    assert app_module._acquire_auto_qc_leader_lock() is False


def test_third_caller_also_loses_while_leader_holds(isolated_slot_dir: str) -> None:
    """Not just a two-way race -- a third (or Nth) worker must also lose
    while the lock is held, matching render.yaml's --workers 2 (plus the
    --preload master's own pre-fork pass)."""
    assert app_module._acquire_auto_qc_leader_lock() is True
    assert app_module._acquire_auto_qc_leader_lock() is False
    assert app_module._acquire_auto_qc_leader_lock() is False


def test_lock_is_released_if_the_leader_process_exits(
    isolated_slot_dir: str,
) -> None:
    """The kernel auto-releases flock() on fd close/process exit, so a
    restarted or crashed leader's slot must become available again --
    otherwise a leader crash would permanently blind AutoQC on this
    instance."""
    assert app_module._acquire_auto_qc_leader_lock() is True
    # Simulate the leader process exiting: close every fd it was holding
    # open (this is exactly what OS process teardown does, and what the
    # real function deliberately never does on its own -- see its
    # docstring).
    for fd in app_module._auto_qc_leader_fds:
        fd.close()
    app_module._auto_qc_leader_fds.clear()

    assert app_module._acquire_auto_qc_leader_lock() is True
