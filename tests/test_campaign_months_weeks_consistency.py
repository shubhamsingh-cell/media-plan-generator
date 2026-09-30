"""Campaign duration: one months value for the budget, the weeks and the
label (wizard audit D-05, 2026-10-01).

Pre-fix the budget multiplier used the wizard's midpoint months
(BUDGET_DURATION_MONTHS: "6-12 months" -> 9) while campaign_weeks came from
display_format's 4-week-month SUBSTRING buckets ("6-12 month" -> 48 weeks),
so a $10,000/month x 9 = $90,000 plan's workbook said "with a $90,000 budget
over 11 months (~48 weeks)". Same split for 3-6 (4.5 vs 5.5 months), 1-3
(2 vs 2.8), 2-5 years (42 vs 36); and exact durations typed via the API
hit the buckets too ("24 months" -> 24 weeks, "12 months" -> 48 weeks).
There was also no 6-month option (Brendan: "$10,000/month for 6 months").
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import app
import display_format
import wizard_inputs
from tests.wizard_js_harness import NODE, inputs_functions_js, run_node

_CONTENT = (
    Path(__file__).resolve().parent.parent
    / "templates" / "partials" / "index" / "body_content.html"
).read_text(encoding="utf-8")


def _options() -> list[str]:
    start = _CONTENT.index('id="campaignDuration"')
    end = _CONTENT.index("</select>", start)
    return [v for v in re.findall(r'<option value="([^"]*)"', _CONTENT[start:end]) if v]


OPTIONS = _options()


def test_standard_exact_durations_are_offered():
    for months in (1, 2, 3, 4, 6, 9, 12, 18, 24):
        label = "1 month" if months == 1 else f"{months} months"
        assert label in OPTIONS, label
    assert set(OPTIONS) <= set(wizard_inputs.CAMPAIGN_DURATION_MONTHS)


@pytest.mark.parametrize("option", OPTIONS)
def test_weeks_come_from_the_budget_months(option):
    months = app._budget_duration_months(option)
    data = {"campaign_duration": option}
    weeks = app._resolve_campaign_weeks(data)
    assert weeks == data["campaign_weeks"] == wizard_inputs.months_to_weeks(months)


@pytest.mark.parametrize(
    "option,label",
    [
        ("6-12 months", "9 months (~39 weeks)"),
        ("3-6 months", "4.5 months (~20 weeks)"),
        ("1-3 months", "9 weeks"),
        ("1-2 years", "18 months (~78 weeks)"),
        ("2-5 years (Long-term)", "3.5 years (~182 weeks)"),
        ("6 months", "6 months (~26 weeks)"),
        ("4 months", "4 months (~17 weeks)"),
        ("9 months", "9 months (~39 weeks)"),
        ("12 months", "12 months (~52 weeks)"),
        ("24 months", "24 months (~104 weeks)"),
        ("3 months", "13 weeks"),
        ("2 weeks", "2 weeks"),
    ],
)
def test_duration_label_states_the_months_the_budget_used(option, label):
    data = {"campaign_duration": option}
    app._resolve_campaign_weeks(data)
    assert display_format.resolve_campaign_duration_label(data) == label


def test_ongoing_label_is_unchanged():
    data = {"campaign_duration": "Ongoing"}
    assert app._resolve_campaign_weeks(data) == 52
    assert display_format.resolve_campaign_duration_label(data).startswith("Ongoing")


def test_brendan_monthly_budget_over_a_range_reads_the_same_months_everywhere():
    """$10,000/month x "6-12 months": the budget is x9, and the weeks/label
    the workbook and deck print are 9 months -- not 11 (~48 weeks)."""
    from tests.test_budget_period_preview_parity import _server_campaign_budget

    assert _server_campaign_budget("$10,000", "monthly", "6-12 months") == 90_000
    data = {"campaign_duration": "6-12 months"}
    weeks = app._resolve_campaign_weeks(data)
    label = display_format.resolve_campaign_duration_label(data)
    assert weeks == 39 and label.startswith("9 months")
    assert "11 months" not in label and "48 weeks" not in label


def test_six_month_option_budgets_six_months():
    from tests.test_budget_period_preview_parity import _server_campaign_budget

    assert _server_campaign_budget("$10,000", "monthly", "6 months") == 60_000


@pytest.mark.parametrize(
    "free_text,weeks",
    [("24 months", 104), ("12 months", 52), ("1 year", 52), ("9 months", 39),
     ("6 months", 26), ("16 weeks", 16), ("90 days", 13)],
)
def test_explicit_free_text_durations_are_not_bucketed(free_text, weeks):
    assert display_format.resolve_campaign_weeks(free_text) == weeks


@pytest.mark.parametrize("weeks", range(1, 300))
def test_labels_still_round_trip(weeks):
    label = display_format.weeks_to_duration_label(weeks)
    assert display_format.resolve_campaign_weeks(label) == weeks


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_review_step_states_the_planned_months():
    script = "\n".join(
        [
            inputs_functions_js(),
            "process.stdout.write(JSON.stringify([",
            "  novaDurationReadingText('6-12 months'),",
            "  novaDurationReadingText('3-6 months'),",
            "  novaDurationReadingText('6 months'),",
            "  novaDurationReadingText('Ongoing'),",
            "  novaMonthsToWeeks(novaCampaignMonths('6-12 months')),",
            "]));",
        ]
    )
    got = run_node(script)
    assert got[0] == "6-12 months — planned as 9 months (~39 weeks)"
    assert got[1] == "3-6 months — planned as 4.5 months (~20 weeks)"
    assert got[2] == "6 months (~26 weeks)"
    assert got[3] == "Ongoing (budgeted as 12 months)"
    assert got[4] == 39
