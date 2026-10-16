"""Quarter / half / fiscal-year durations parse as calendar spans.

Verified 2026-10-16 (coordinator probe) before the fix: parse_duration_to_weeks
read the first digit of a calendar token as a number of WEEKS, so
"Q1-Q3" -> 1, "Q4" -> 4, "2 quarters" -> 2, "H2" -> 2 and "FY2026" -> 2026,
while "FY" and "full year" -> 0. A campaign_duration of "Q1-Q3" therefore read
"1 week" on every surface (Slack: "$1.5M over 1 week"; workbook timeline:
"Weeks 3-1").

Calendar convention (matches the 52-week year the rest of display_format
uses): a quarter is 13 weeks, a half is 26, a fiscal/full year is 52. Every
expected number below is typed by hand.
"""

from __future__ import annotations

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import display_format as fmt  # noqa: E402
import wizard_inputs  # noqa: E402

# (text, weeks)
QUARTER_CASES = [
    # the verified defects
    ("Q1-Q3", 39),
    ("Q4", 13),
    ("2 quarters", 26),
    ("H2", 26),
    ("H1", 26),
    ("FY", 52),
    ("full year", 52),
    # quarters: single, ranges, spellings
    ("Q1", 13),
    ("q3", 13),
    ("Q1-Q4", 52),
    ("Q2-Q3", 26),
    ("Q1 - Q3", 39),
    ("Q1–Q3", 39),  # en dash
    ("Q1 to Q3", 39),
    ("Q1 through Q2", 26),
    ("Q4-Q2", 39),  # wraps the year: Q4, Q1, Q2
    ("Q1 2026 - Q1 2027", 65),  # five quarters when the years are stated
    ("Q2 2026 - Q1 2027", 52),
    ("Q3 2026", 13),
    ("Q1 & Q2", 26),
    ("1 quarter", 13),
    ("3 quarters", 39),
    ("4 quarters", 52),
    ("1.5 quarters", 20),
    # halves
    ("H1 2026", 26),
    ("H1-H2", 52),
    ("half year", 26),
    ("half-year", 26),
    ("first half", 26),
    ("second half of 2026", 26),
    ("2 halves", 52),
    # fiscal / full year
    ("FY2026", 52),
    ("FY26", 52),
    ("FY 2026", 52),
    ("fy", 52),
    ("full-year", 52),
    ("Full Year", 52),
    ("fiscal year", 52),
    ("calendar year", 52),
    # an explicit duration beats a calendar anchor
    ("6 weeks in Q3", 6),
    ("Q3 (8 weeks)", 8),
    ("Q1-Q3 (9 months)", 39),
    ("12 weeks starting H2", 12),
    ("1 year from Q2", 52),
    # a literal "(~N weeks)" label still wins
    ("9 months (~39 weeks)", 39),
]

# Everything that parsed before must parse identically.
UNCHANGED_CASES = [
    ("6 months", 26),
    ("18 months", 78),
    ("1.5 years", 78),
    ("12 weeks", 12),
    ("26", 26),
    ("26 weeks", 26),
    ("10 days", 1),
    ("1 year", 52),
    ("", 0),
    ("tbd", 0),
    ("   ", 0),
    ("H1B visa sponsorship", 1),  # not a half: the H1 is glued to a letter
    ("seq1", 1),  # not a quarter: Q1 is glued to a letter
    ("hq2", 2),
    ("something with no numbers", 0),
]


class TestParseDurationCalendarTokens:
    @pytest.mark.parametrize(
        "text,weeks", QUARTER_CASES, ids=[c[0] for c in QUARTER_CASES]
    )
    def test_calendar_spans(self, text, weeks):
        assert fmt.parse_duration_to_weeks(text) == weeks

    @pytest.mark.parametrize(
        "text,weeks", UNCHANGED_CASES, ids=[c[0] or "<empty>" for c in UNCHANGED_CASES]
    )
    def test_existing_behaviour_is_unchanged(self, text, weeks):
        assert fmt.parse_duration_to_weeks(text) == weeks

    def test_numeric_inputs_still_mean_weeks(self):
        assert fmt.parse_duration_to_weeks(12) == 12
        assert fmt.parse_duration_to_weeks(12.4) == 12
        assert fmt.parse_duration_to_weeks(None) == 0  # type: ignore[arg-type]

    def test_label_round_trip_still_exact_for_every_week_count(self):
        for weeks in range(0, 160):
            assert (
                fmt.parse_duration_to_weeks(fmt.weeks_to_duration_label(weeks)) == weeks
            )


class TestResolveAndLabelAgree:
    """resolve_campaign_weeks / resolve_campaign_duration_label feed every
    surface; they must say the same thing as the parser."""

    @pytest.mark.parametrize(
        "text,weeks",
        [
            ("Q1-Q3", 39),
            ("Q4", 13),
            ("2 quarters", 26),
            ("H2", 26),
            ("FY", 52),
            ("full year", 52),
            ("FY2026", 52),
        ],
    )
    def test_resolve_campaign_weeks(self, text, weeks):
        assert fmt.resolve_campaign_weeks(text) == weeks

    @pytest.mark.parametrize(
        "text,label",
        [
            ("Q1-Q3", "9 months (~39 weeks)"),
            ("Q4", "13 weeks"),
            ("2 quarters", "6 months (~26 weeks)"),
            ("H2", "6 months (~26 weeks)"),
            ("H1", "6 months (~26 weeks)"),
            ("FY", "12 months (~52 weeks)"),
            ("full year", "12 months (~52 weeks)"),
        ],
    )
    def test_duration_label(self, text, label):
        assert fmt.resolve_campaign_duration_label({"campaign_duration": text}) == label

    def test_label_reads_back_to_the_same_weeks(self):
        for text in ("Q1-Q3", "Q4", "2 quarters", "H2", "FY", "full year"):
            label = fmt.resolve_campaign_duration_label({"campaign_duration": text})
            assert fmt.parse_duration_to_weeks(label) == fmt.resolve_campaign_weeks(
                text
            )

    def test_wizard_dropdown_options_and_ladder_unchanged(self):
        assert fmt.resolve_campaign_weeks("6 months") == 26
        assert fmt.resolve_campaign_weeks("1-3 months") == 9
        assert fmt.resolve_campaign_weeks("6-12 months") == 39
        assert fmt.resolve_campaign_weeks("long-term") == 156
        assert fmt.resolve_campaign_weeks("ongoing") == 52
        assert fmt.resolve_campaign_weeks("garbage") == 12
        assert fmt.resolve_campaign_weeks("") == 12

    def test_budget_months_follow_the_calendar_span(self):
        # wizard_inputs.campaign_months falls back to resolve_campaign_weeks
        # for digit-bearing free text, so the budget multiplier now sees nine
        # months for Q1-Q3 instead of one week's worth.
        months = wizard_inputs.campaign_months("Q1-Q3", fmt.resolve_campaign_weeks)
        assert months == pytest.approx(9.0)
        months = wizard_inputs.campaign_months("2 quarters", fmt.resolve_campaign_weeks)
        assert months == pytest.approx(6.0)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
