"""Nova's seasonal chat context must survive every seasonal_hiring_trends
entry, including sectors with no stated multipliers.

2026-10-11: the new food_manufacturing entry (months only, no
peak/low_multiplier) made nova.py index _sp['peak_multiplier'] and raise
KeyError on every chat turn mentioning budget/campaign/month/timing during
its peak or low months (October included) -- Nova chat was down.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("month", range(1, 13))
def test_seasonal_prompt_builds_in_every_month(month, monkeypatch):
    import nova

    real_dt = datetime.datetime

    class _FixedDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, month, 15, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(datetime, "datetime", _FixedDT)
    prompt = nova.Nova().get_system_prompt(
        "When is the best month to run a seasonal hiring campaign for our budget?"
    )
    assert isinstance(prompt, str) and prompt


def test_every_seasonal_pattern_is_renderable_without_multipliers():
    data = json.loads((ROOT / "data" / "seasonal_hiring_trends.json").read_text())
    pats = data["seasonal_patterns"]
    assert "food_manufacturing" in pats
    # The multiplier keys are optional; months are not.
    for name, sp in pats.items():
        assert isinstance(sp.get("peak_months"), list), name
        assert isinstance(sp.get("low_months"), list), name
