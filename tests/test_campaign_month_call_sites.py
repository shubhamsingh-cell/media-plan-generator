"""Campaign-month fallbacks go through budget_engine.default_campaign_month.

Sites that read the wall-clock month for "the campaign month" went stale when
the calendar rolled over (the month time-bomb class, SESSION_HANDOFF hazards).
The pin is the MPG_DEFAULT_CAMPAIGN_MONTH env var, read at call time. These
tests move the pin to a month other than conftest's October and assert each
touched path follows it, and a grep-style test keeps the raw reads out.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

# Files whose "campaign month" fallbacks were routed through the helper.
_FILES = ("excel_v2.py", "gold_standard.py", "nova.py", "ppt_generator.py", "quick_plan.py")
_RAW_MONTH_READ = re.compile(r"(?:datetime|date|_dt|_dt_risk)\.(?:now|today)\(\)\.month")


@pytest.mark.parametrize("name", _FILES)
def test_no_raw_wall_clock_month_reads(name: str) -> None:
    source = (ROOT / name).read_text(encoding="utf-8")
    hits = [
        i
        for i, line in enumerate(source.splitlines(), 1)
        if _RAW_MONTH_READ.search(line) and not line.lstrip().startswith("#")
    ]
    assert not hits, (
        f"{name} reads the wall-clock month at line(s) {hits}; use "
        "budget_engine.default_campaign_month() via the module's _default_month()"
    )


@pytest.mark.parametrize("module", ["excel_v2", "gold_standard", "nova", "ppt_generator", "quick_plan"])
def test_module_month_shim_follows_the_pin(module: str, monkeypatch) -> None:
    mod = __import__(module)
    assert mod._default_month() == 10  # conftest pin
    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", "3")
    assert mod._default_month() == 3


def test_quick_plan_seasonal_advice_honours_the_pin(monkeypatch) -> None:
    import quick_plan

    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", "3")
    assert quick_plan._get_seasonal_advice("white_collar")["current_month"] == "March"


def test_activation_calendar_defaults_to_the_pinned_month(monkeypatch) -> None:
    import gold_standard

    data = {"industry": "technology", "roles": ["Software Engineer"]}
    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", "3")
    march = gold_standard.build_activation_calendar(dict(data))
    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", "9")
    sept = gold_standard.build_activation_calendar(dict(data))
    assert march != sept
    explicit = gold_standard.build_activation_calendar({**data, "campaign_start_month": 3})
    monkeypatch.setenv("MPG_DEFAULT_CAMPAIGN_MONTH", "3")
    assert gold_standard.build_activation_calendar(dict(data)) == explicit
