"""Regression: the CPH-floor constant must never wear the plan's name.

Measured 2026-09-08: a healthcare plan at total_budget=2,000,000 (London,
GBP) and another at 150,000 (Dallas, USD) BOTH reported
``total_projected.cost_per_hire == 5250.0`` -- a round figure invariant to a
13x budget change and a currency change. It was ``0.5 * industry_avg_cph``
(the S39 benchmark floor) leaking out under the plan-projection key, while
every slide/sheet that derives budget / hires itself (slide 2 hero, slide 6
takeaway, plan_validator, excel_v2) printed 5,263.16 = 2,000,000 / 380.

Contract now:
  1. budget_engine: ``total_projected.cost_per_hire`` == round(total_budget
     / total_projected.hires, 2) even when the floor capped hires; the floor
     itself is exposed as ``metadata.cph_benchmark_floor`` /
     ``metadata.cph_floor_applied`` and the benchmark as
     ``metadata.industry_avg_cph``.
  2. ppt_generator Layer 0 (_get_benchmarks): the "Est. Cost-per-Hire (this
     plan)" band is built around _compute_blended_cph -- the SAME figure
     slides 2 and 6 print -- so one plan renders ONE cost-per-hire.
"""

from __future__ import annotations

import re

import pytest

import budget_engine
import ppt_generator as ppt


def _healthcare_alloc(total_budget: float, location: str):
    return budget_engine.calculate_budget_allocation(
        total_budget=total_budget,
        roles=[{"title": "Registered Nurse", "count": 40, "tier": "Hourly"}],
        locations=[location],
        industry="healthcare_medical",
        channel_percentages={
            "programmatic_dsp": 30,
            "global_boards": 25,
            "niche_boards": 20,
            "social_media": 5,
            "regional_boards": 15,
            "employer_branding": 5,
        },
        synthesized_data=None,
        knowledge_base={},
    )


class TestEngineReportsTrueRatio:
    @pytest.mark.parametrize(
        "total_budget,location",
        [
            (2_000_000.0, "London, UK"),
            (150_000.0, "Dallas, TX"),
            (300_000.0, "New York, NY"),
        ],
    )
    def test_cost_per_hire_is_budget_over_hires_when_floor_fires(
        self, total_budget, location
    ):
        result = _healthcare_alloc(total_budget, location)
        tp = result["total_projected"]
        meta = result["metadata"]
        assert meta["cph_floor_applied"] is True, meta
        floor = meta["cph_benchmark_floor"]
        assert floor == pytest.approx(meta["industry_avg_cph"] * 0.5)
        hires = tp["hires"]
        assert hires == int(total_budget / floor)
        # The plan's own figure, not the floor constant.
        assert tp["cost_per_hire"] == pytest.approx(
            round(total_budget / hires, 2), abs=0.005
        )
        # Floor semantics preserved whenever the budget affords >= 1
        # floor-priced hire (all three budgets here do).
        assert total_budget >= floor
        assert tp["cost_per_hire"] >= floor - 1e-6
        # Channel hires still foot to the total (S48 invariant).
        assert (
            sum(
                int(ch.get("projected_hires") or 0)
                for ch in result["channel_allocations"].values()
            )
            == hires
        )

    def test_sub_floor_budget_reports_whole_budget_not_floor(self):
        """Boundary: a budget below one floor-priced hire clamps hires to 1.
        The honest cost-per-hire is then the whole budget (below the floor),
        never the floor constant the pre-fix engine emitted (5250)."""
        # Two openings across two channels: raw projected hires > 0 so the
        # floor fires, but 5,000 < the 5,250 floor.
        result = budget_engine.calculate_budget_allocation(
            total_budget=5_000.0,
            roles=[{"title": "Registered Nurse", "count": 2, "tier": "Hourly"}],
            locations=["Dallas, TX"],
            industry="healthcare_medical",
            channel_percentages={"programmatic_dsp": 50, "global_boards": 50},
            synthesized_data=None,
            knowledge_base={},
        )
        tp = result["total_projected"]
        meta = result["metadata"]
        assert meta["cph_floor_applied"] is True
        assert 5_000.0 < meta["cph_benchmark_floor"]
        assert tp["hires"] == 1
        assert tp["cost_per_hire"] == pytest.approx(5_000.0, abs=0.005)

    def test_constant_no_longer_invariant_to_budget(self):
        """The tell from the incident: 13x budget change must move the
        reported CPH unless budget / floor happens to divide exactly."""
        big = _healthcare_alloc(2_000_000.0, "London, UK")["total_projected"]
        assert big["cost_per_hire"] == pytest.approx(2_000_000.0 / 380, abs=0.005)
        assert big["cost_per_hire"] != pytest.approx(5250.0, abs=0.005)


class TestLayer0UsesBlendedCph:
    def _data(self):
        # total_projected.cost_per_hire deliberately DISAGREES with
        # budget / hires (the pre-fix engine shape) so the test proves
        # Layer 0 no longer trusts the raw key.
        return {
            "client_name": "Atria Senior Living",
            "industry": "healthcare_medical",
            "budget": "$300,000",
            "locations": ["New York, NY"],
            "_budget_allocation": {
                "metadata": {
                    "total_budget": 300000.0,
                    "industry": "healthcare_medical",
                },
                "total_projected": {"hires": 57, "cost_per_hire": 5250.0},
                "channel_allocations": {
                    "programmatic_dsp": {"projected_hires": 30},
                    "global_boards": {"projected_hires": 27},
                },
            },
        }

    def test_layer0_band_brackets_slide2_slide6_figure(self):
        data = self._data()
        ppt._set_active_currency(data)
        bm = ppt._get_benchmarks("healthcare_medical", data)
        assert bm.get("cph_is_usd_benchmark") is False, bm
        blended, hires = ppt._compute_blended_cph(data["_budget_allocation"])
        assert (blended, hires) == (5263.16, 57)
        nums = [
            float(n.replace(",", ""))
            for n in re.findall(r"[\d,]+(?:\.\d+)?", bm["cph"])
        ]
        assert len(nums) == 2, bm["cph"]
        assert nums[0] == pytest.approx(blended * 0.8, abs=1.0), bm["cph"]
        assert nums[1] == pytest.approx(blended * 1.2, abs=1.0), bm["cph"]
        # And NOT the stale raw key: 5250 * 0.8 = 4200 / 5250 * 1.2 = 6300.
        assert nums[0] != pytest.approx(4200.0, abs=0.5)
        assert nums[1] != pytest.approx(6300.0, abs=0.5)
