"""budget_engine.default_campaign_month is the single wall-clock fallback for the
campaign month, and tests/conftest.py pins it to October (the month the approved
expected numbers were captured in) unless a test is marked ``real_clock``."""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
from pathlib import Path

import pytest

import budget_engine


def test_conftest_pins_the_fallback_to_october() -> None:
    assert budget_engine.default_campaign_month() == 10
    assert budget_engine.default_campaign_month(0) == 10


def test_explicit_month_still_wins_under_the_pin() -> None:
    assert budget_engine.default_campaign_month(3) == 3
    assert budget_engine.default_campaign_month(12) == 12
    assert budget_engine.default_campaign_month(13) == 10  # invalid -> fallback


@pytest.mark.real_clock
@pytest.mark.parametrize("today_month", [1, 6, 11])
def test_real_fallback_reads_the_clock(
    monkeypatch: pytest.MonkeyPatch, today_month: int
) -> None:
    class _FakeDT(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, today_month, 15)

    monkeypatch.setattr(datetime, "datetime", _FakeDT)
    assert budget_engine.default_campaign_month() == today_month
    assert budget_engine.default_campaign_month(4) == 4
    assert budget_engine.default_campaign_month(13) == today_month


def test_trend_engine_seasonal_default_follows_the_pinned_helper() -> None:
    import trend_engine

    assert trend_engine.get_seasonal_adjustment("blue_collar")["month"] == 10


_ROOT = Path(__file__).resolve().parent.parent
_PROBE = "import budget_engine; print(budget_engine.default_campaign_month())"


def _probe(env: dict[str, str]) -> str:
    return (
        subprocess.run(
            [sys.executable, "-c", _PROBE],
            cwd=_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        .stdout.strip()
        .splitlines()[-1]
    )


def test_spawned_subprocess_inherits_the_october_pin() -> None:
    """Any server/CLI a test spawns inherits os.environ, so it agrees with the
    in-process computation (no October-vs-today split)."""
    assert _probe(dict(os.environ)) == "10"


def test_env_override_is_read_at_call_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", "7")
    assert budget_engine.default_campaign_month() == 7
    assert budget_engine.default_campaign_month(2) == 2  # explicit month wins


@pytest.mark.real_clock
@pytest.mark.parametrize("bad", ["13", "0", "x", "", "-3", "1.5"])
def test_invalid_env_override_is_ignored(
    monkeypatch: pytest.MonkeyPatch, bad: str
) -> None:
    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", bad)
    assert budget_engine.default_campaign_month() == datetime.datetime.now().month
    assert _probe(dict(os.environ)) == str(datetime.datetime.now().month)


@pytest.mark.real_clock
def test_real_clock_marker_removes_the_pin() -> None:
    assert "MPG_DEFAULT_CAMPAIGN_MONTH" not in os.environ
