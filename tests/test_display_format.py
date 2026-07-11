"""Tests for display_format -- the single presentation layer for money,
percent, count, channel-label, client-name, and duration formatting."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from display_format import (  # noqa: E402
    CHANNEL_DISPLAY,
    channel_label,
    client_display_name,
    fmt_count,
    fmt_float,
    fmt_money,
    fmt_pct,
    goal_gap,
    parse_duration_to_weeks,
    parse_hire_goal,
    reconcile_monthly_to_total,
    weeks_to_duration_label,
)


class TestChannelLabel:
    @pytest.mark.parametrize(
        "key,expected",
        [
            ("niche_boards", "Niche / Industry Boards"),
            ("programmatic_dsp", "Programmatic (DSP)"),
            ("global_boards", "Global Job Boards"),
            ("social_media", "Social Media"),
            ("regional_boards", "Regional Job Boards"),
            ("employer_branding", "Employer Branding"),
        ],
    )
    def test_known_keys(self, key, expected):
        assert channel_label(key) == expected

    def test_fallback_never_contains_underscore(self):
        label = channel_label("some_totally_unmapped_key")
        assert "_" not in label
        assert label == "Some Totally Unmapped Key"

    def test_empty_and_none_safe(self):
        assert channel_label("") == ""
        assert channel_label(None) == ""

    def test_all_channel_display_values_are_underscore_free(self):
        for value in CHANNEL_DISPLAY.values():
            assert "_" not in value


class TestClientDisplayName:
    def test_lowercase_words_capitalized(self):
        assert client_display_name("atria Senior living") == "Atria Senior Living"

    def test_all_caps_string_is_title_cased(self):
        assert client_display_name("MANPOWER - AMERIGAS") == "Manpower - Amerigas"

    def test_internal_capitals_preserved(self):
        assert client_display_name("eBay") == "eBay"
        assert client_display_name("McKinsey") == "McKinsey"
        assert client_display_name("visit eBay careers") == "Visit eBay Careers"

    def test_acronym_preserved_when_not_all_caps(self):
        assert client_display_name("AMC theatres hiring") == "AMC Theatres Hiring"

    def test_collapses_whitespace(self):
        assert client_display_name("atria   senior   living") == "Atria Senior Living"

    def test_empty_and_none(self):
        assert client_display_name("") == ""
        assert client_display_name(None) == ""


class TestFmtMoney:
    def test_plain(self):
        assert fmt_money(150000) == "$150,000"

    def test_compact_round(self):
        assert fmt_money(150000, compact=True) == "$150K"

    def test_compact_strips_trailing_zero(self):
        assert fmt_money(52500, compact=True) == "$52.5K"
        assert "$150.0K" not in fmt_money(150000, compact=True)

    def test_compact_millions(self):
        assert fmt_money(1200000, compact=True) == "$1.2M"

    def test_compact_under_1000_unaffected(self):
        assert fmt_money(500, compact=True) == "$500"

    def test_negative(self):
        assert fmt_money(-1000) == "-$1,000"

    def test_non_numeric_safe(self):
        assert fmt_money(None) == "$0"
        assert fmt_money("garbage") == "$0"


class TestFmtPct:
    def test_default_is_0_to_100_scale(self):
        assert fmt_pct(42) == "42%"

    def test_is_fraction_scales_up(self):
        assert fmt_pct(0.42, is_fraction=True) == "42%"

    def test_decimals(self):
        assert fmt_pct(42.567, decimals=1) == "42.6%"

    def test_never_exceeds_2_decimals(self):
        assert fmt_pct(42.56789, decimals=5) == "42.57%"

    def test_non_numeric_safe(self):
        assert fmt_pct(None) == "0%"


class TestFmtCount:
    def test_singular(self):
        assert fmt_count(1, "market") == "1 market"

    def test_plural(self):
        assert fmt_count(6, "market") == "6 markets"

    def test_zero_is_plural(self):
        assert fmt_count(0, "market") == "0 markets"

    def test_never_emits_parenthetical_s(self):
        for n in (0, 1, 2, 5):
            assert "(s)" not in fmt_count(n, "market")


class TestFmtFloat:
    def test_strips_trailing_zeros(self):
        assert fmt_float(3.0) == "3"

    def test_respects_max_decimals(self):
        assert fmt_float(3.14159, 2) == "3.14"

    def test_partial_trailing_zero_strip(self):
        assert fmt_float(3.50, 2) == "3.5"


class TestReconcileMonthlyToTotal:
    def test_sums_exactly_to_rounded_total(self):
        monthly = [100.4, 100.3, 100.3]
        result = reconcile_monthly_to_total(monthly, 301)
        assert sum(result) == 301
        assert len(result) == 3

    def test_handles_underflow_total(self):
        monthly = [33.33, 33.33, 33.33]
        result = reconcile_monthly_to_total(monthly, 100)
        assert sum(result) == 100

    def test_empty_list(self):
        assert reconcile_monthly_to_total([], 100) == []

    def test_all_zero_monthly_still_reconciles(self):
        result = reconcile_monthly_to_total([0, 0, 0, 0], 10)
        assert sum(result) == 10
        assert len(result) == 4

    def test_large_diff_relative_to_bucket_count(self):
        # diff far exceeds n -- must still land exactly on target via
        # wraparound distribution, not silently truncate.
        result = reconcile_monthly_to_total([1.0, 1.0], 50)
        assert sum(result) == 50
        assert len(result) == 2


class TestParseDurationToWeeks:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("6 months", 26),
            ("18 months", 78),
            ("1.5 years", 78),
            ("12 weeks", 12),
            (12, 12),
            ("12", 12),
        ],
    )
    def test_examples(self, text, expected):
        assert parse_duration_to_weeks(text) == expected

    def test_empty_and_none_safe(self):
        assert parse_duration_to_weeks("") == 0
        assert parse_duration_to_weeks(None) == 0
        assert parse_duration_to_weeks("not specified") == 0


class TestWeeksToDurationLabel:
    def test_examples(self):
        assert weeks_to_duration_label(26) == "6 months (~26 weeks)"
        assert weeks_to_duration_label(78) == "18 months (~78 weeks)"

    def test_short_duration_stays_in_weeks(self):
        assert weeks_to_duration_label(12) == "12 weeks"
        assert weeks_to_duration_label(13) == "13 weeks"

    def test_14_weeks_switches_to_months(self):
        assert "months" in weeks_to_duration_label(14)


class TestDurationRoundTrip:
    @pytest.mark.parametrize("weeks", [1, 6, 12, 13, 14, 26, 52, 78, 104])
    def test_round_trips_exactly(self, weeks):
        label = weeks_to_duration_label(weeks)
        assert parse_duration_to_weeks(label) == weeks

    def test_18_months_shipped_bug_repro(self):
        # The shipped bug: 18 months -> weeks via *4 then /4.33 turned 18
        # into 17 on the way back. Assert the full loop is exact.
        weeks = parse_duration_to_weeks("18 months")
        assert weeks == 78
        label = weeks_to_duration_label(weeks)
        assert label == "18 months (~78 weeks)"
        assert parse_duration_to_weeks(label) == 78


class TestParseHireGoal:
    def test_bare_int(self):
        assert parse_hire_goal(5000) == 5000

    def test_range_takes_low_end(self):
        assert parse_hire_goal("50-100 hires") == 50

    def test_plus_suffix(self):
        assert parse_hire_goal("5,000+") == 5000

    def test_not_specified_variants(self):
        assert parse_hire_goal("Not specified") == 0
        assert parse_hire_goal("TBD") == 0
        assert parse_hire_goal("") == 0
        assert parse_hire_goal(None) == 0

    def test_simple_number_string(self):
        assert parse_hire_goal("200 hires") == 200


class TestGoalGap:
    def test_none_when_goal_not_positive(self):
        assert goal_gap(100, 0, 5000) is None
        assert goal_gap(100, -5, 5000) is None

    def test_none_when_projected_meets_or_exceeds_goal(self):
        assert goal_gap(100, 100, 5000) is None
        assert goal_gap(150, 100, 5000) is None

    def test_gap_dict_shape(self):
        result = goal_gap(60, 100, 5000)
        assert result is not None
        assert result["goal"] == 100
        assert result["projected"] == 60
        assert result["pct_of_goal"] == 60.0
        assert result["additional_budget"] == 40 * 5000
