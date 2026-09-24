"""Regression: calculate_budget_allocation's total-plan-level industry-CPH
benchmark must never wear a USD figure on a non-USD plan.

Found 2026-09-24 investigating a related report about the per-channel
``_CHANNEL_MIN_CPH`` floor (a separate, not-yet-landed-here conversion --
see budget_engine.py's own `_CHANNEL_MIN_CPH` usages, still raw USD in this
tree as of this commit): ``_industry_avg_cph(industry)`` returns a
USD constant (e.g. $10,500 for healthcare), but three places in
``calculate_budget_allocation`` compared or reported it alongside plan-native
figures without converting it first --

  1. ``_benchmark_cph_floor`` (0.5x the benchmark) gates ``avg_cost_per_hire``
     (``total_budget / total_hires``, plan-native): on a JPY plan the raw-USD
     floor was ~149x too small to ever fire (unrealistic hire counts went
     uncapped); on a GBP plan it was too aggressive (over-capped hires).
  2. ``_industry_avg_cph_val`` fed every channel's ``roi_score`` via
     ``_redistribute_hires_by_conversion`` -> ``_score_roi``
     (``cost_per_hire / industry_avg``): on a JPY plan the ratio blew past
     the >=5.0 hard-cap for every channel, collapsing ALL roi_scores to 1/10
     regardless of relative channel performance -- exactly the kind of
     ROI-blind reallocation a real shipped GBP plan hit before (see S92/Fix 2
     docstrings), just triggered by JPY-scale instead of a stale
     provisional score.
  3. ``metadata.industry_avg_cph`` (the reported benchmark) silently
     disagreed with ``sufficiency.industry_avg_cost_per_hire`` -- the SAME
     conceptual benchmark, already correctly converted since the F4 fix --
     on any non-USD plan.

Fix: convert once via the shared ``_usd_const_to_plan_basis`` helper (the
same one ``assess_budget_sufficiency``'s ``_usd_const`` now delegates to)
and reuse that single converted value for all three call sites.
"""

from __future__ import annotations

import pytest

import budget_engine as be

_ROLES = [{"title": "Registered Nurse", "count": 40, "tier": "Hourly"}]
_CHANNELS = {
    "programmatic_dsp": 30,
    "global_boards": 25,
    "niche_boards": 20,
    "social_media": 5,
    "regional_boards": 15,
    "employer_branding": 5,
}


def _alloc(total_budget, location, plan_currency, symbol):
    # knowledge_base={} deliberately -- a real KB blends in a second, KB-
    # sourced figure (e.g. SHRM's average_cost_per_hire) INTO
    # sufficiency.industry_avg_cost_per_hire only (see assess_budget_
    # sufficiency's KB-blend block), which makes it legitimately diverge
    # from metadata.industry_avg_cph even pre-fix, for a reason unrelated
    # to currency. Passing {} isolates the currency-agreement invariant
    # this file tests from that separate, pre-existing KB-blend behavior.
    return be.calculate_budget_allocation(
        total_budget=total_budget,
        roles=_ROLES,
        locations=[location],
        industry="healthcare_medical",
        channel_percentages=dict(_CHANNELS),
        synthesized_data=None,
        knowledge_base={},
        plan_currency=plan_currency,
        budget_text=f"{symbol}{total_budget:,.0f}",
    )


class TestUsdConstToPlanBasisHelper:
    def test_no_rate_returns_value_unconverted(self):
        assert be._usd_const_to_plan_basis(10_500.0, None) == 10_500.0

    def test_non_positive_rate_returns_value_unconverted(self):
        assert be._usd_const_to_plan_basis(10_500.0, 0) == 10_500.0
        assert be._usd_const_to_plan_basis(10_500.0, -1.0) == 10_500.0

    def test_positive_rate_divides(self):
        # usd_per_local=0.0067 means 1 local unit == $0.0067 (JPY-scale).
        assert be._usd_const_to_plan_basis(10_500.0, 0.0067) == pytest.approx(
            10_500.0 / 0.0067
        )


class TestIndustryAvgCphAgreesAcrossOutputs:
    """metadata.industry_avg_cph and sufficiency.industry_avg_cost_per_hire
    must agree on CURRENCY (this file's invariant) whenever nothing else
    makes them diverge. With knowledge_base={} (see _alloc) there is no
    KB-blend to introduce a legitimate non-currency difference, so under
    that condition they must match exactly, in whichever currency the plan
    is actually priced in. A real KB can still make them differ by design
    (assess_budget_sufficiency blends a KB figure into its own value only)
    -- that's a separate, pre-existing behavior this test doesn't cover."""

    @pytest.mark.parametrize(
        "total_budget,location,plan_currency,symbol",
        [
            (30_000_000.0, "Tokyo, Japan", "JPY", "¥"),
            (200_000.0, "London, UK", "GBP", "£"),
            (150_000.0, "Dallas, TX", "USD", "$"),
        ],
    )
    def test_metadata_matches_sufficiency(
        self, total_budget, location, plan_currency, symbol
    ):
        result = _alloc(total_budget, location, plan_currency, symbol)
        meta = result["metadata"]
        suff = result["sufficiency"]
        assert meta["industry_avg_cph"] == pytest.approx(
            suff["industry_avg_cost_per_hire"]
        ), (meta["industry_avg_cph"], suff["industry_avg_cost_per_hire"])
        # And the floor is always exactly half of that SAME agreed figure.
        assert meta["cph_benchmark_floor"] == pytest.approx(
            meta["industry_avg_cph"] * 0.5
        )

    def test_usd_plan_unchanged_raw_constant(self):
        """No conversion path taken for a USD plan -- byte-identical to
        pre-fix behavior."""
        result = _alloc(150_000.0, "Dallas, TX", "USD", "$")
        assert result["metadata"]["industry_avg_cph"] == pytest.approx(10_500.0)


class TestJpyFloorNoLongerInert:
    """Pre-fix: a raw-USD floor (~$5,250) against JPY-scale cost_per_hire
    (hundreds of thousands of yen) could never fire. A JPY plan sized so the
    naive (unconverted) hire count implies a CPH far below the true,
    converted floor must now trigger the floor."""

    def test_floor_applies_and_is_yen_scaled(self):
        result = _alloc(30_000_000.0, "Tokyo, Japan", "JPY", "¥")
        meta = result["metadata"]
        # The floor must be in yen-thousands, not a bare ~5,250.
        assert meta["cph_benchmark_floor"] > 100_000, meta["cph_benchmark_floor"]
        assert meta["cph_floor_applied"] is True
        tp = result["total_projected"]
        assert tp["cost_per_hire"] >= meta["cph_benchmark_floor"] - 1e-6


class TestRoiScoreNotCollapsedByCurrencyScale:
    """Pre-fix: comparing plan-native (JPY) cost_per_hire against a raw-USD
    industry_avg pushed every channel's ratio past the _score_roi hard cap
    (ratio >= 5.0), so every funded channel scored 1/10 regardless of its
    actual relative efficiency -- indistinguishable from a real
    across-the-board disaster, and useless input to the ROI-driven
    reallocation (rebalance_low_roi_channels)."""

    def test_jpy_plan_roi_scores_are_differentiated(self):
        result = _alloc(30_000_000.0, "Tokyo, Japan", "JPY", "¥")
        scores = {
            name: ch.get("roi_score")
            for name, ch in result["channel_allocations"].items()
            if (ch.get("dollar_amount") or 0) > 0
        }
        assert scores, "expected at least one funded channel"
        # Not every channel pinned to the worst score -- some genuine spread.
        assert len(set(scores.values())) > 1, scores
        assert max(scores.values()) > 3, scores
