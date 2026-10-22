"""Review fixes (2026-10-21): the benchmark-gap wording and fallback, item-level
market tags on Workforce Trends, channel display names, the platform-span gap, the
core-section draft stamp, the employer heading, supervisor scoring, the client
never listed as a large employer, and the supervisor SOC rule's context words."""

import copy
import json

import pytest

import excel_v2
import gold_standard
import plan_validator
import standardizer

from tests.test_plan_integrity import (
    _deck_slide_texts,
    _german_plan,
    _workbook_texts,
)


# ---------------------------------------------------------------------------
# 1 + 3: wrapped legend fallback by status; channel display names
# ---------------------------------------------------------------------------


def _channel_below_plan():
    """Blended CPA inside the German industry range, three funded channels under
    it, keyed the way the allocation engine keys them."""
    data = _german_plan(cpa_eur=14.0, confidence=0.8)
    alloc = data["_budget_allocation"]
    alloc["channel_allocations"] = {
        key: {"dollar_amount": 1000, "cpa": 1.0}
        for key in ("programmatic_dsp", "global_boards", "emea_regional")
    }
    return data


class TestChannelBelowFallback:
    def test_fallback_never_says_the_blended_cpa_is_below_when_only_a_channel_is(self):
        gap = plan_validator.cpa_benchmark_gap(_channel_below_plan())
        assert gap["status"] == "channel_below"
        fallback = plan_validator.benchmark_gap_fallback(gap)
        assert fallback.startswith("One channel's planned CPA is below")
        assert "Planned CPA is below" not in fallback

    def test_blended_gap_fallback_says_the_blended_cpa(self):
        gap = plan_validator.cpa_benchmark_gap(_german_plan(cpa_eur=5.0))
        assert gap["status"] == "below"
        assert plan_validator.benchmark_gap_fallback(gap).startswith(
            "Planned CPA is below"
        )

    def test_no_fallback_when_nothing_is_below(self):
        assert plan_validator.benchmark_gap_fallback(None) == ""

    def test_deck_legend_for_a_channel_only_gap_does_not_claim_the_blended_cpa(self):
        slides = _deck_slide_texts(_channel_below_plan())
        joined = " ".join(" ".join(s) for s in slides)
        assert "channel's planned CPA is below" in joined
        assert "Planned CPA is below" not in joined


class TestChannelNamesInTheGapMessage:
    def test_channels_are_named_the_way_the_workbook_rows_name_them(self):
        gap = plan_validator.cpa_benchmark_gap(_channel_below_plan())
        msg = plan_validator.benchmark_gap_message(gap)
        assert "Programmatic (DSP)" in msg
        assert "Global Job Boards" in msg
        assert "Programmatic Dsp" not in msg and "Emea Regional" not in msg

    def test_a_channels_own_label_wins(self):
        data = _channel_below_plan()
        data["_budget_allocation"]["channel_allocations"]["programmatic_dsp"][
            "label"
        ] = "Programmatic (Germany)"
        msg = plan_validator.benchmark_gap_message(
            plan_validator.cpa_benchmark_gap(data)
        )
        assert "Programmatic (Germany)" in msg


# ---------------------------------------------------------------------------
# 4: platform ranges are a span, never a "market" or "industry" benchmark
# ---------------------------------------------------------------------------


def _platform_range_plan(cpa_eur):
    data = _german_plan(cpa_eur=cpa_eur, confidence=0.8)
    data["industry"] = "automotive"  # no industry-level German range
    data["_intl_benchmarks"] = {
        "countries": {
            "germany": {
                "name": "Germany",
                "platforms": [
                    {"name": "StepStone", "cpa_usd": {"min": 10.9, "max": 54.5}},
                    {"name": "Indeed Germany", "cpa_usd": {"min": 5.45, "max": 38.15}},
                ],
            }
        }
    }
    return data


class TestPlatformSpanGap:
    def test_a_cpa_inside_one_platforms_range_does_not_fire(self):
        # EUR 6.50 = US$7.38: under StepStone's low (10.9) but inside Indeed
        # Germany's printed US$5-38. Only "below every printed range" fires.
        gap = plan_validator.cpa_benchmark_gap(_platform_range_plan(6.5))
        assert gap["basis"] == "platforms"
        assert gap["low_usd"] == pytest.approx(5.45)
        assert gap["high_usd"] == pytest.approx(54.5)
        assert gap["status"] != "below"

    def test_below_the_lowest_low_fires_and_names_the_lowest_board(self):
        gap = plan_validator.cpa_benchmark_gap(_platform_range_plan(4.0))
        assert gap["status"] == "below"
        msg = plan_validator.benchmark_gap_message(gap)
        assert "below every platform CPA range printed for Germany" in msg
        assert "(lowest: Indeed Germany US$5.45)" in msg
        assert "market's benchmark" not in msg and "industry benchmark" not in msg

    def test_short_form_does_not_call_the_span_a_benchmark(self):
        gap = plan_validator.cpa_benchmark_gap(_platform_range_plan(4.0))
        short = plan_validator.benchmark_gap_message(gap, short=True)
        assert "every platform CPA range printed for Germany" in short
        assert "benchmark" not in short
        assert "benchmark" not in plan_validator.benchmark_gap_fallback(gap)

    def test_industry_range_keeps_the_industry_wording(self):
        gap = plan_validator.cpa_benchmark_gap(_german_plan(cpa_eur=5.0))
        assert gap["basis"] == "industry"
        assert "industry benchmark" in plan_validator.benchmark_gap_message(gap)

    def test_workbook_and_deck_agree_for_a_platform_span_plan(self):
        data = _platform_range_plan(4.0)
        wording = "below every platform CPA range printed for Germany"
        texts = _workbook_texts(data)
        assert any(wording in t for t in texts["Executive Summary"])
        joined = " ".join(" ".join(s) for s in _deck_slide_texts(data))
        assert "market's benchmark" not in joined


# ---------------------------------------------------------------------------
# 5: a core section at 0% stamps the plan an internal draft
# ---------------------------------------------------------------------------


def _with_sections(data, **sections):
    data = copy.deepcopy(data)
    data["_synthesized"]["confidence_scores"]["per_section"] = dict(sections)
    return data


class TestCoreSectionDraftStamp:
    def test_zero_salary_stamps_even_when_overall_is_high(self):
        data = _with_sections(
            _german_plan(confidence=0.8),
            salary_intelligence=0.0,
            job_market_demand=0.6,
            location_profiles=0.8,
        )
        assert plan_validator.internal_draft_stamp(data) == (
            "Internal draft: salary data missing (0%). "
            "Review before sharing with a client."
        )

    def test_zero_demand_is_named_too(self):
        data = _with_sections(
            _german_plan(confidence=0.8), job_market_demand=0.0, location_profiles=1.0
        )
        assert "job market demand data missing (0%)" in (
            plan_validator.internal_draft_stamp(data)
        )

    def test_no_zero_core_section_no_stamp(self):
        data = _with_sections(
            _german_plan(confidence=0.8),
            salary_intelligence=0.4,
            job_market_demand=0.2,
            location_profiles=1.0,
            competitive_intelligence=0.0,  # not a core section
        )
        assert plan_validator.internal_draft_stamp(data) == ""

    def test_an_unscored_section_is_not_a_zero_section(self):
        data = _with_sections(_german_plan(confidence=0.8), ad_platform_analysis=0.4)
        assert plan_validator.internal_draft_stamp(data) == ""

    def test_low_overall_confidence_keeps_its_own_wording(self):
        data = _with_sections(_german_plan(confidence=0.47), salary_intelligence=0.0)
        assert plan_validator.internal_draft_stamp(data) == (
            "Internal draft: data confidence 47%. "
            "Review before sharing with a client."
        )

    def test_validator_reports_it(self):
        data = _with_sections(_german_plan(confidence=0.8), salary_intelligence=0.0)
        summary = plan_validator.validate_plan(data)
        assert any(
            f["check"] == "low_data_confidence"
            and "salary data missing" in f["message"]
            for f in summary["findings"]
        )


# ---------------------------------------------------------------------------
# 2: item-level market tags on the "Global" Workforce Trends blocks
# ---------------------------------------------------------------------------


def _real_workforce_blocks():
    trends = json.load(open("data/workforce_trends_intelligence.json"))
    strategy = json.load(open("data/recruitment_strategy_intelligence.json"))
    eb = strategy["employer_branding"]
    return {
        "employer_branding": {
            k: eb[k] for k in ("roi_data", "best_practices", "channel_effectiveness")
        },
        "supply_partner_trends": trends["supply_partner_trends"],
        "job_type_trends": trends["job_type_trends"],
    }


def _market_intelligence_blob(data):
    data = copy.deepcopy(data)
    data.setdefault("_synthesized", {})["workforce_insights"] = _real_workforce_blocks()
    return " ".join(_workbook_texts(data)["Market Intelligence"])


# Strings the Germany workbook printed under the "(Global)" Workforce Trends blocks
# although they state or come from a US source.
US_STRINGS_IN_GLOBAL_BLOCKS = (
    "74% of Gen Z use TikTok",
    "76% of Gen Z rely on Instagram",
    "federal government RTO",
    "Truist",
    "FlexJobs",
    "Gen Z Participation",
    "Wonolo",
    "Census Data",
    "Proposition 22",
)


class TestWorkforceItemTags:
    def test_germany_workbook_drops_us_sourced_items_from_global_blocks(self):
        blob = _market_intelligence_blob(_german_plan(confidence=0.8))
        for text in US_STRINGS_IN_GLOBAL_BLOCKS:
            assert text not in blob, text

    def test_germany_workbook_keeps_the_items_tagged_global(self):
        blob = _market_intelligence_blob(_german_plan(confidence=0.8))
        assert "Supply Partner Trends (Global)" in blob
        assert "Click Fraud And Ad Fraud" in blob
        assert "Upwork Stats" in blob

    def test_us_plan_is_unchanged(self):
        data = _german_plan(confidence=0.8)
        data["locations"] = ["Phoenix, AZ, United States"]
        data["country"] = "United States"
        blob = _market_intelligence_blob(data)
        assert "Truist" in blob and "FlexJobs" in blob

    def test_germany_workbook_drops_us_case_study_from_social_recruiting(self):
        blob = _market_intelligence_blob(_german_plan(confidence=0.8))
        assert "Chipotle" not in blob
        assert "Case Studies" not in blob
        # the global platform facts of the same block stay
        assert "Platform Stats: 1 billion+ monthly active users" in blob
        assert "'For You' feed" in blob

    def test_us_plan_keeps_the_social_recruiting_case_study(self):
        data = _german_plan(confidence=0.8)
        data["locations"] = ["Phoenix, AZ, United States"]
        data["country"] = "United States"
        blob = _market_intelligence_blob(data)
        assert "Chipotle" in blob and "Platform Stats" in blob

    def test_an_untagged_item_is_dropped(self):
        row = {"current_state": {"us_remote_rate": "22.8% of workers"}}
        assert (
            excel_v2._workforce_global_row("job_type_trends", "remote_work", row) == {}
        )

    def test_a_tagged_item_that_names_a_us_source_is_still_dropped(self):
        got = excel_v2._workforce_global_row(
            "job_type_trends",
            "remote_work",
            {"employer_plans": {"note": "Reported by the U.S. Census Bureau"}},
        )
        assert got == {}

    def test_a_block_left_with_no_item_is_hidden(self):
        data = copy.deepcopy(_german_plan(confidence=0.8))
        data.setdefault("_synthesized", {})["workforce_insights"] = {
            "job_type_trends": {
                "remote_work": {
                    "return_to_office_mandates": {"examples": "Truist, TikTok"}
                }
            }
        }
        blob = " ".join(_workbook_texts(data)["Market Intelligence"])
        assert "Truist" not in blob
        assert "Job Type Trends (Global)" not in blob


# ---------------------------------------------------------------------------
# 6 + 8: employer heading; the client is never listed
# ---------------------------------------------------------------------------


class TestLargestEmployers:
    def test_heading_says_largest_employers_in_the_country(self):
        out = gold_standard.large_employers_for("united kingdom", "retail")
        assert out["label"] == "Largest employers in United Kingdom (Retail)"
        assert out["note"] == gold_standard.LARGE_EMPLOYERS_NOTE

    def test_the_client_itself_is_never_listed(self):
        names = [
            e["name"]
            for e in gold_standard.large_employers_for(
                "united kingdom", "retail", client_name="Tesco"
            )["employers"]
        ]
        assert "Tesco" not in names and "Asda" in names

    def test_client_match_survives_case_possessive_and_suffix(self):
        names = [
            e["name"]
            for e in gold_standard.large_employers_for(
                "united kingdom", "retail", client_name="SAINSBURYS plc"
            )["employers"]
        ]
        assert "Sainsbury's" not in names

    def test_other_clients_leave_the_list_alone(self):
        everyone = gold_standard.large_employers_for("united kingdom", "retail")
        other = gold_standard.large_employers_for(
            "united kingdom", "retail", client_name="Example Freight Co"
        )
        assert other["employers"] == everyone["employers"]


# ---------------------------------------------------------------------------
# 7: supervisor titles score above the frontline profile they only head-noun match
# ---------------------------------------------------------------------------


def _scores(titles, locations=("Dallas, TX",)):
    rows = gold_standard.classify_difficulty(
        {"target_roles": [{"title": t} for t in titles], "locations": list(locations)}
    )
    return {r["role_title"]: r for r in rows}


class TestSupervisorScore:
    def test_warehouse_supervisor_scores_above_associate_and_not_below_order_picker(
        self,
    ):
        got = _scores(["Warehouse Associate", "Order Picker", "Warehouse Supervisor"])
        sup = got["Warehouse Supervisor"]["complexity_score"]
        assert sup > got["Warehouse Associate"]["complexity_score"]
        assert sup >= got["Order Picker"]["complexity_score"]

    def test_fulfillment_and_warehouse_supervisor_score_the_same(self):
        for loc in (("Dallas, TX",), ("Manchester, United Kingdom",)):
            got = _scores(["Fulfillment Supervisor", "Warehouse Supervisor"], loc)
            assert (
                got["Fulfillment Supervisor"]["complexity_score"]
                == got["Warehouse Supervisor"]["complexity_score"]
            )

    def test_the_reason_is_stated_in_the_description(self):
        row = _scores(["Warehouse Supervisor"])["Warehouse Supervisor"]
        assert "supervisor tier" in row["description"]

    def test_team_lead_warehouse_follows_the_same_rule(self):
        got = _scores(["Team Lead Warehouse", "Warehouse Associate"])
        assert (
            got["Team Lead Warehouse"]["complexity_score"]
            > got["Warehouse Associate"]["complexity_score"]
        )


# ---------------------------------------------------------------------------
# 9: the supervisor SOC rule needs a production / maintenance context word
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "title",
    [
        "Help Line Supervisor",
        "Fire Line Supervisor",
        "Press Office Supervisor",
        "Line Cook Supervisor",
        "Landscape Maintenance Supervisor",
    ],
)
def test_supervisor_without_a_plant_context_gets_no_first_line_supervisor_soc(title):
    assert standardizer.get_soc_code(title) not in ("51-1011", "49-1011")
    assert standardizer.normalize_role(title) not in (
        "production_supervisor",
        "maintenance_supervisor",
    )


@pytest.mark.parametrize(
    "title,soc",
    [
        ("Assembly Line Supervisor", "51-1011"),
        ("Paint Shop Supervisor", "51-1011"),
        ("Press Shop Supervisor", "51-1011"),
        ("Plant Maintenance Supervisor", "49-1011"),
        ("Equipment Repair Supervisor", "49-1011"),
        ("Maintenance Supervisor", "49-1011"),
    ],
)
def test_supervisor_with_a_plant_context_keeps_its_soc(title, soc):
    assert standardizer.get_soc_code(title) == soc
