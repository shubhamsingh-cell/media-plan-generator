"""Regression: one industry-average cost-per-hire per plan, and a local
market's own benchmark for a local-currency plan (audit 2026-10-01, F §3.2 /
§3.5 / §4.1).

Defects pinned here (each reproduced against f99beef before the fix):

* One healthcare build carried $10,500 (metadata.industry_avg_cph), $7,650
  (sufficiency.industry_avg_cost_per_hire -- a blend with a KB SHRM figure)
  and $3,500 (budget_reality_check.industry_avg_cph -- a separate "minimum"
  table whose messages still said "industry average").
* budget_reality_check graded a plan pricing hires at its own 0.5x-average
  floor "WELL-FUNDED ... exceeds the industry average".
* An India RN plan divided ₹20M by $10,500 / 0.012 = ₹875,000 (FX-translated
  US constant): 45 hires at ₹444,444/hire, ~10x the repo's own India
  healthcare cost per hire (₹45,000, intl_role_benchmarks_v1.json).
"""

from __future__ import annotations

import pytest

import budget_engine as be
import intl_benchmark_lookup as ibl
from kb_loader import load_knowledge_base

_CHANNELS = {
    "programmatic_dsp": 35,
    "global_boards": 20,
    "niche_boards": 15,
    "social_media": 12,
    "regional_boards": 8,
    "employer_branding": 5,
    "apac_regional": 3,
    "emea_regional": 2,
}


def _plan(industry, roles, location, budget, currency, symbol, loc_raw=None):
    return be.calculate_budget_allocation(
        total_budget=budget,
        roles=[{"title": r, "count": 1, "tier": "Professional"} for r in roles],
        locations=[location],
        industry=industry,
        channel_percentages=dict(_CHANNELS),
        synthesized_data={},
        knowledge_base=load_knowledge_base(),
        plan_currency=currency,
        locations_raw=loc_raw,
        budget_text=f"{symbol}{budget:,.0f}",
    )


class TestOneNumberPerPlan:
    def test_every_industry_average_key_agrees_with_a_real_kb(self):
        res = _plan(
            "healthcare_medical",
            ["Registered Nurse"],
            {"city": "Dallas", "state": "TX", "country": "United States"},
            250_000.0,
            "USD",
            "$",
        )
        meta = res["metadata"]
        suff = res["sufficiency"]
        values = {
            "metadata.industry_avg_cph": meta["industry_avg_cph"],
            "sufficiency.industry_avg_cost_per_hire": suff[
                "industry_avg_cost_per_hire"
            ],
            "budget_reality_check.industry_avg_cph": suff["budget_reality_check"][
                "industry_avg_cph"
            ],
        }
        assert set(values.values()) == {10_500.0}, values
        assert meta["industry_cph"]["value"] == 10_500.0
        assert meta["industry_cph"]["basis"] == "us_benchmark"

    def test_us_headline_hires_unchanged(self):
        """The resolver returns the same US midpoint the floor always used,
        so US headline hires do not move."""
        res = _plan(
            "healthcare_medical",
            ["Registered Nurse"],
            {"city": "Dallas", "state": "TX", "country": "United States"},
            250_000.0,
            "USD",
            "$",
        )
        tp = res["total_projected"]
        assert res["metadata"]["cph_benchmark_floor"] == 5_250.0
        assert tp["cost_per_hire"] >= 5_250.0
        assert tp["hires"] == int(250_000 / 5_250)


class TestRealityCheckUsesTheSameAverage:
    def test_plan_at_its_own_floor_is_not_well_funded(self):
        """47 target hires for $250K = $5,319/hire: half the $10,500 average
        (the plan's own floor). Pre-fix this graded against a $3,500
        "minimum" and printed WELL-FUNDED ... exceeds the industry average."""
        suff = be.assess_budget_sufficiency(
            250_000.0, 47, "healthcare_medical", {}, knowledge_base=None
        )
        rc = suff["budget_reality_check"]
        assert rc["industry_avg_cph"] == 10_500.0
        assert rc["feasibility_tier"] == "tight", rc
        assert "10,500" in rc["feasibility_message"]
        assert "exceeds the industry average" not in rc["feasibility_message"]

    def test_message_names_the_average_it_compares_against(self):
        suff = be.assess_budget_sufficiency(
            250_000.0, 1, "healthcare_medical", {}, knowledge_base=None
        )
        rc = suff["budget_reality_check"]
        assert rc["feasibility_tier"] == "generous"
        assert "$10,500" in rc["feasibility_message"], rc["feasibility_message"]


class TestLocalMarketCostPerHire:
    def test_india_rn_uses_the_india_benchmark_not_an_fx_translated_us_one(self):
        res = _plan(
            "healthcare_medical",
            ["Registered Nurse"],
            {"city": "Bangalore", "state": "", "country": "India"},
            20_000_000.0,
            "INR",
            "₹",
            loc_raw=["Bangalore, India"],
        )
        meta = res["metadata"]
        # The plan's own CPH sits inside the KB's ₹20K-80K band, not ~₹444K.
        assert 20_000 <= res["total_projected"]["cost_per_hire"] <= 80_000, res[
            "total_projected"
        ]
        assert meta["industry_avg_cph"] == 45_000.0
        assert res["sufficiency"]["industry_avg_cost_per_hire"] == 45_000.0
        cph = meta["industry_cph"]
        assert cph["basis"] == "local_kb", cph
        assert cph["value"] == 45_000.0  # KB India general_hire median, ₹
        assert cph["fx"]["usd_per_local"] > 0

    def test_brazil_retail_uses_brazil_benchmark(self):
        res = _plan(
            "retail_consumer",
            ["Retail Sales Associate"],
            {"city": "Sao Paulo", "state": "", "country": "Brazil"},
            500_000.0,
            "BRL",
            "R$",
            loc_raw=["Sao Paulo, Brazil"],
        )
        cph = res["metadata"]["industry_cph"]
        assert cph["basis"] == "local_kb", cph
        assert cph["value"] == 1_800.0  # KB Brazil hospitality median, R$
        assert cph["currency"] == "BRL"

    def test_no_local_benchmark_suppresses_the_claim(self):
        """general_entry_level has no intl vertical: the FX-translated US
        figure may still drive the floor, but is never presented."""
        res = _plan(
            "general_entry_level",
            ["Customer Service Representative"],
            {"city": "Tokyo", "state": "", "country": "Japan"},
            30_000_000.0,
            "JPY",
            "¥",
            loc_raw=["Tokyo, Japan"],
        )
        cph = res["metadata"]["industry_cph"]
        assert cph["claim_suppressed"] is True
        assert cph["value"] is None and cph["basis"] == "us_benchmark_fx_no_local"
        suff = res["sufficiency"]
        assert suff["industry_avg_cost_per_hire"] is None
        assert suff["benchmark_available"] is False
        assert suff["budget_reality_check"]["feasibility_tier"] == "not_assessed"
        # math still has a (yen-scale) floor so hires stay bounded
        assert res["metadata"]["cph_benchmark_floor"] > 100_000

    def test_channel_floors_follow_the_local_average(self):
        basis = {"basis": "local", "usd_per_local": 0.012, "local_cph_scale": 0.05}
        plain = {"basis": "local", "usd_per_local": 0.012}
        assert be._channel_min_cph("programmatic", plain) == pytest.approx(800 / 0.012)
        assert be._channel_min_cph("programmatic", basis) == pytest.approx(
            800 / 0.012 * 0.05
        )
        assert be._channel_min_cph("programmatic", None) == 800


class TestLocalCphLookup:
    def test_native_currency_median(self):
        got = ibl.get_local_cph_benchmark("healthcare_nursing", "India", "INR", 0.012)
        assert got["value"] == 45_000.0 and got["method"] == "local_median"
        assert got["low"] == 20_000.0 and got["high"] == 80_000.0

    def test_fee_and_platform_rows_are_not_a_cost_per_hire(self):
        # UAE healthcare holds only an agency-fee % and a monthly platform cost
        assert ibl.get_local_cph_benchmark("healthcare_nursing", "UAE", "AED", 0.27) is None

    def test_median_across_rows_when_no_general_row(self):
        got = ibl.get_local_cph_benchmark("healthcare_nursing", "UK", "GBP", 1.3)
        assert got["value"] == pytest.approx((9_500 + 3_500) / 2)

    def test_usd_rows_converted_with_the_plans_rate(self):
        got = ibl.get_local_cph_benchmark("healthcare_nursing", "India", "EUR", 1.1)
        assert got["method"] == "usd_median_fx"
        assert got["value"] == pytest.approx((535.5 / 1.1 + 535.0 / 1.1) / 2, rel=1e-3)
