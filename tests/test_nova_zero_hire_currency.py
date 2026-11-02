"""The zero-hire channel note is written in the plan's currency.

nova._query_budget_projection formatted the "N% of budget (X) funds channels
that project no hires" note with a literal "$" whatever ``currency`` the
projection reported, so a plan priced in euros read in dollars.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import channel_funding  # noqa: E402
import nova  # noqa: E402

_ALLOC = {
    "currency": "EUR",
    "total_projected": {"hires": 4},
    "channel_allocations": {
        "programmatic": {"dollar_amount": 9000, "projected_hires": 4},
        "employer_branding": {"dollar_amount": 1000, "projected_hires": 0},
    },
}


def test_formatter_follows_projection_currency() -> None:
    assert nova._plan_money_formatter({"currency": "EUR"})(1000) == "€1,000"
    assert nova._plan_money_formatter({"currency": "USD"})(1000) == "$1,000"
    assert nova._plan_money_formatter({})(1000) == "$1,000"


def test_zero_hire_note_in_plan_currency() -> None:
    note = channel_funding.zero_hire_summary(_ALLOC, False, nova._plan_money_formatter(_ALLOC))
    assert note, "fixture must produce a zero-hire note"
    assert "€1,000" in note and "$" not in note


def test_budget_tool_does_not_hardcode_dollar_for_the_note() -> None:
    src = Path(nova.__file__).read_text(encoding="utf-8")
    assert 'zero_hire_summary(\n                    result, False, lambda v: f"${v:,.0f}"' not in src
