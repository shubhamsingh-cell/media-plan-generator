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
