"""The hub Prediction Engine grid must not flood the boot logs.

Every production boot prices 560 scenarios through ``budget_engine`` at
deferred startup (``proof_claims.hub_pe_grid()``); the engine logs a dozen INFO
lines per call, so each boot wrote 6.7k-9.2k ``budget_engine`` lines to the
Render log (pushing real signal out of the 30-day window).  The grid build now
runs inside ``engine_log_quiet.quiet_engine_logs()`` -- the same helper the
DataMatrixMonitor probe uses -- which demotes the engine's INFO/WARNING lines
to DEBUG on the building thread only, with ERROR still passing.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import textwrap
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# app.py replaces the root logging handlers on import, which would silently
# unhook pytest's caplog if it were imported inside a test; import it up front
# (it also registers the real estimator with proof_claims).
import app  # noqa: E402,F401
import proof_claims  # noqa: E402

ENGINE = "budget_engine"


def _fresh_grid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the next hub_pe_grid() call to build, and restore the cache after."""
    monkeypatch.setattr(proof_claims, "_grid", None)
    monkeypatch.setattr(proof_claims, "_grid_month", None)


def _with_estimator(
    monkeypatch: pytest.MonkeyPatch, estimator: Callable[[dict], Dict[str, Any]]
) -> Dict[str, Any]:
    _fresh_grid(monkeypatch)
    monkeypatch.setattr(proof_claims, "_engine", lambda: estimator)
    grid = proof_claims.hub_pe_grid()
    assert grid is not None
    return grid


def _engine_records(caplog: pytest.LogCaptureFixture) -> List[logging.LogRecord]:
    return [r for r in caplog.records if r.name == ENGINE]


def test_building_the_real_grid_emits_no_engine_info_lines(monkeypatch, caplog) -> None:
    _fresh_grid(monkeypatch)
    with caplog.at_level(logging.INFO):
        grid = proof_claims.hub_pe_grid()
    assert grid is not None and len(grid["scenarios"]) > 100

    loud = [r for r in _engine_records(caplog) if r.levelno >= logging.INFO]
    assert loud == [], f"{len(loud)} budget_engine lines at INFO+ during the grid build"
    summaries = [
        r
        for r in caplog.records
        if r.name == "proof_claims" and "hub prediction grid priced" in r.getMessage()
    ]
    assert len(summaries) == 1
    assert f"priced {len(grid['scenarios'])} scenarios" in summaries[0].getMessage()


def test_the_demoted_lines_are_still_there_at_debug(monkeypatch, caplog) -> None:
    be = logging.getLogger(ENGINE)

    def noisy(brief: dict) -> Dict[str, Any]:
        be.info("Channel dollar amounts computed for %d channels", 6)
        be.warning("Zero or negative budget; returning empty allocation")
        return {"est_hires": 3}

    with caplog.at_level(logging.DEBUG):
        _with_estimator(monkeypatch, noisy)
    levels = {r.levelno for r in _engine_records(caplog)}
    assert levels == {logging.DEBUG}


def test_an_engine_error_during_the_grid_build_is_not_hidden(
    monkeypatch, caplog
) -> None:
    be = logging.getLogger(ENGINE)

    def failing(brief: dict) -> Dict[str, Any]:
        be.info("chatter")
        be.error("engine boom")
        return {"est_hires": 3}

    with caplog.at_level(logging.INFO):
        _with_estimator(monkeypatch, failing)
    records = _engine_records(caplog)
    assert records and {r.getMessage() for r in records} == {"engine boom"}
    assert {r.levelno for r in records} == {logging.ERROR}


def test_another_threads_engine_info_line_is_not_demoted(monkeypatch, caplog) -> None:
    be = logging.getLogger(ENGINE)
    first_call: List[bool] = []

    def overlapping(brief: dict) -> Dict[str, Any]:
        if not first_call:
            first_call.append(True)
            # a request thread logs while the grid build is mid-flight
            worker = threading.Thread(target=lambda: be.info("request thread line"))
            worker.start()
            worker.join()
            be.info("grid builder line")
        return {"est_hires": 3}

    with caplog.at_level(logging.INFO):
        _with_estimator(monkeypatch, overlapping)
    loud = [
        r.getMessage() for r in _engine_records(caplog) if r.levelno >= logging.INFO
    ]
    assert loud == ["request thread line"]


def test_the_quiet_context_nests_and_restores(caplog) -> None:
    from engine_log_quiet import quiet_engine_logs

    be = logging.getLogger(ENGINE)
    with caplog.at_level(logging.INFO):
        with quiet_engine_logs():
            with quiet_engine_logs():
                be.info("inner")
            be.info("outer, after the inner block")
        be.info("after")
    assert [r.getMessage() for r in _engine_records(caplog)] == ["after"]


def test_a_sibling_greenlet_is_not_demoted_under_gevent() -> None:
    """Production runs gevent workers: the flag must be per-greenlet, not per-OS-thread."""
    pytest.importorskip("gevent")
    code = textwrap.dedent(
        """
        from gevent import monkey
        monkey.patch_all()
        import json, logging
        import gevent
        from engine_log_quiet import quiet_engine_logs

        seen = []

        class Grab(logging.Handler):
            def emit(self, record):
                seen.append([record.levelname, record.getMessage()])

        logging.getLogger().addHandler(Grab())
        logging.getLogger().setLevel(logging.INFO)
        be = logging.getLogger("budget_engine")

        def builder():
            with quiet_engine_logs():
                be.info("builder before yield")
                gevent.sleep(0.05)
                be.info("builder after yield")

        def request():
            gevent.sleep(0.01)
            be.info("request greenlet line")

        gevent.joinall([gevent.spawn(builder), gevent.spawn(request)])
        print(json.dumps(seen))
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.strip().splitlines()[-1]) == [
        ["INFO", "request greenlet line"]
    ]


def test_the_monitor_and_the_grid_share_one_demotion_helper() -> None:
    import data_matrix_monitor
    import engine_log_quiet

    assert data_matrix_monitor.quiet_engine_logs is engine_log_quiet.quiet_engine_logs
    assert proof_claims.quiet_engine_logs is engine_log_quiet.quiet_engine_logs
    # one filter on the engine logger, however often the installer runs
    engine_log_quiet._install()
    engine_log_quiet._install()
    ours = [
        f
        for f in logging.getLogger(ENGINE).filters
        if type(f).__name__ == "_DemoteEngineLogs"
    ]
    assert len(ours) == 1
