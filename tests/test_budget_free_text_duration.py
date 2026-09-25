"""Regression: free-text campaign durations scale a per-period budget by the
REAL number of months (52/12 math), not by display_format's 4-week-month
substring buckets.

Adversarial review of 51f50b2: _budget_duration_months() sent every
non-dropdown duration through display_format.resolve_campaign_weeks, whose
phrase ladder matches by SUBSTRING against marketing bands -- so
"24 months" hit the "4 month" bucket (24 weeks -> x5.5), "12 months" /
"1 year" / "9 months" hit the 48-week bucket (x11.08) and "6 months" the
24-week bucket (x5.54). The wizard preview must agree for any free text.
"""

from __future__ import annotations

import pytest

import app
from tests.test_budget_period_preview_parity import (
    _preview_campaign_total,
    _server_campaign_budget,
    needs_node,
)

# (duration, expected months)
_CASES = [
    ("24 months", 24.0),
    ("12 months", 12.0),
    ("1 year", 12.0),
    ("9 months", 9.0),
    ("6 months", 6.0),
    ("18 months", 18.0),
    ("2 years", 24.0),
    ("52 weeks", 12.0),
    ("26 weeks", 6.0),
    ("4-8 months", 6.0),  # range -> midpoint, same as the dropdown ranges
    ("6-12 months", 9.0),  # exact dropdown value still wins (same answer)
    ("2 weeks", 0.5),  # exact dropdown value keeps its map entry
]


@pytest.mark.parametrize("duration,months", _CASES)
def test_monthly_multiplier_uses_real_months(duration, months):
    assert app._budget_period_multiplier("monthly", duration) == pytest.approx(
        max(months, 1.0)
    )


@pytest.mark.parametrize(
    "duration,expected",
    [
        ("24 months", 240000),
        ("12 months", 120000),
        ("1 year", 120000),
        ("9 months", 90000),
        ("6 months", 60000),
    ],
)
def test_server_campaign_total_for_free_text(duration, expected):
    assert round(_server_campaign_budget("$10,000", "monthly", duration)) == expected


@needs_node
@pytest.mark.parametrize("duration", [c[0] for c in _CASES] + ["16 weeks", "3 years"])
@pytest.mark.parametrize("period", ["monthly", "quarterly", "annual"])
def test_preview_agrees_with_server_for_free_text(duration, period):
    preview = round(_preview_campaign_total(10000.0, period, duration))
    server = round(_server_campaign_budget("$10,000", period, duration))
    assert preview == server, f"{duration!r}/{period}: preview {preview} vs server {server}"


def test_unparseable_duration_still_defaults_to_6_months():
    assert app._budget_period_multiplier("monthly", "asap") == 6.0
    assert app._budget_period_multiplier("monthly", "") == 6.0
