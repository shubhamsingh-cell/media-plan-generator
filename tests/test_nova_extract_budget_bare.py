"""A bare budget figure is read, not asked back.

Sweep 2026-11-03: "Build a media plan for 10 software engineers in Berlin with
a 50000 budget" answered "How much is the total budget?".
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402


@pytest.mark.parametrize(
    "text,value",
    [
        ("build a media plan for 10 software engineers in berlin with a 50000 budget", 50000),
        ("budget of 50,000 for nurses", 50000),
        ("our budget is 75000", 75000),
        ("$40,000 for 20 nurses", 40000),
        ("50k budget", 50000),
    ],
)
def test_reads_budget(text: str, value: float) -> None:
    assert nova._extract_budget(text) == value


@pytest.mark.parametrize("text", ["hire 10 nurses in 2026", "10 software engineers"])
def test_does_not_mistake_counts_or_years(text: str) -> None:
    assert nova._extract_budget(text) == 0.0


# Review 2026-11-03: the bare-budget patterns read years as budgets.
@pytest.mark.parametrize(
    "text",
    [
        "plan our 2026 budget for 40 nurses in berlin",
        "q1 2027 budget plan",
        "budget 2026 for 10 hires",
        "the 1999 budget was smaller",
    ],
)
def test_a_year_is_not_a_budget(text: str) -> None:
    assert nova._extract_budget(text) == 0.0


@pytest.mark.parametrize(
    "text,value",
    [("a 2,026 budget", 2026), ("budget of 2026k", 2026000), ("$2026 budget", 2026)],
)
def test_year_shaped_amount_with_money_cues_still_reads(text: str, value: float) -> None:
    assert nova._extract_budget(text) == value
