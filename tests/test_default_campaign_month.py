"""budget_engine.default_campaign_month is the single wall-clock fallback for the
campaign month, and tests/conftest.py pins it to October (the month the approved
expected numbers were captured in) unless a test is marked ``real_clock``."""

from __future__ import annotations

import datetime

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
