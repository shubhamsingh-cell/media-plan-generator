"""Regression tests for Fix 3: USD-scale noise-filter constants converted
into the plan's local currency, same pattern as Fix 1 (see
test_intl_locale_cpc_calibration.py / test_client_visible_numbers.py's
``TestLocalCurrencyBasisIsOneUnit``).

Bug fixed: Fix 1 converted ``BASE_BENCHMARKS['cpc']`` fallback values into
the plan's local currency via ``intl_cpc_basis['usd_per_local']`` before
comparing/using them, for a plan whose ``intl_cpc_basis['basis'] == "local"``
(single non-US market whose own currency matches the plan's currency, e.g.
a JPY plan for Japan or a GBP plan for the UK -- see
``intl_benchmark_lookup.get_locale_cpc_basis``). Two more hardcoded
USD-scale constants were left un-converted and compared directly against
plan-native dollar/hire figures:

    1. ``_CHANNEL_MIN_CPH`` (a per-channel-category USD cost-per-hire floor,
       e.g. programmatic=$800) -- compared directly against ``dollars``/
       ``new_dollars`` at three call sites: ``compute_channel_dollar_amounts``,
       ``_recompute_channel_metrics`` (used by ``rebalance_low_roi_channels``),
       and ``optimize_allocation``. For a JPY plan (dollar amounts ~149x the
       nominal USD figure) the floor essentially never triggers, letting a
       plan report thousands of unrealistically cheap hires. For a GBP plan
       (dollar amounts SMALLER than the USD-equivalent) the floor
       over-triggers, capping hires below what the real-world floor
       actually implies.
    2. The "Low Efficiency" / "No Projected Hires" flag threshold
       (``dollars > 1000``) in ``compute_channel_dollar_amounts`` -- the same
       unit mismatch mislabels channels: trivial JPY spends get flagged
       "Low Efficiency" instead of "No Projected Hires".

Fix: ``_usd_const_to_intl_basis`` reuses the exact ``usd_per_local`` gate
Fix 1 established (``basis == "local"`` and a valid positive rate) to
convert each constant before comparison. A US plan, a multi-market/
USD-blend plan, or a missing rate leaves the constant byte-identical to
before this fix -- no exchange rate is ever invented.

Runs under pytest, or standalone: ``python3 tests/test_channel_min_cph_currency.py``.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine as be  # noqa: E402

_JPY_BASIS = {
    "basis": "local",
    "usd_per_local": 0.0067,  # matches data/international_benchmarks_2026.json:japan
    "currency": "JPY",
    "source": "intl_local:japan",
}
_GBP_BASIS = {
    "basis": "local",
    "usd_per_local": 1.27,  # matches data/international_benchmarks_2026.json:uk
    "currency": "GBP",
    "source": "intl_local:uk",
}
_USD_BLEND_BASIS = {
    "basis": "usd_blend",
    "matched_countries": ["uk", "australia"],
    "source": "intl_usd_blend:uk,australia",
}


# ---------------------------------------------------------------------------
# 1. Unit tests for the conversion helper itself
# ---------------------------------------------------------------------------
class TestUsdConstToPlanBasis:
    def test_local_basis_converts_by_usd_per_local(self):
        assert be._usd_const_to_intl_basis(800, _JPY_BASIS) == 800 / 0.0067
        assert be._usd_const_to_intl_basis(800, _GBP_BASIS) == 800 / 1.27

    def test_none_basis_is_a_no_op(self):
        """The overwhelming common case (a US plan, or any caller that never
        heard of Fix 1/Fix 3): must reproduce the exact prior constant."""
        assert be._usd_const_to_intl_basis(800, None) == 800
        assert be._usd_const_to_intl_basis(1000, None) == 1000

    def test_usd_blend_basis_is_a_no_op(self):
        """A multi-market/blended plan has no single conversion rate to
        apply (same reason Fix 1's own CPC conversion skips it) -- the
        constant is left exactly as before."""
        assert be._usd_const_to_intl_basis(800, _USD_BLEND_BASIS) == 800

    def test_missing_rate_is_a_no_op(self):
        broken_basis = {"basis": "local"}  # no usd_per_local key
        assert be._usd_const_to_intl_basis(800, broken_basis) == 800


# ---------------------------------------------------------------------------
# 2. compute_channel_dollar_amounts -- the CPH floor (call site 1)
# ---------------------------------------------------------------------------
class TestChannelMinCphFloorIsLocalCurrency:
    def test_jpy_plan_floor_engages_instead_of_never_triggering(self):
        """Before Fix 3: a JPY plan's dollar amounts are ~149x the nominal
        USD figure, so the raw $800 floor (read as "800 yen") essentially
        never binds -- a programmatic channel could report hundreds of
        hires at an absurd ~JPY7,000/hire. After Fix 3: the floor converts
        to ~JPY119,403 (800 / 0.0067) and correctly caps hires."""
        role_budgets = {"role1": {"dollar_amount": 2_500_000.0}}
        basis = dict(_JPY_BASIS, categories={"programmatic": 5.0})
        allocs = be.compute_channel_dollar_amounts(
            {"programmatic_dsp": 100.0},
            role_budgets,
            None,
            None,
            industry="logistics_supply_chain",
            collar_type="blue_collar",
            intl_cpc_basis=basis,
        )
        ch = allocs["programmatic_dsp"]
        correct_floor_local = 800 / 0.0067
        assert ch["cost_per_hire"] >= correct_floor_local, (
            f"cost_per_hire {ch['cost_per_hire']} fell below the correctly "
            f"converted JPY floor {correct_floor_local:.0f} -- the raw USD "
            "floor was compared against JPY-scale dollars again"
        )
        # The un-converted floor (800 treated as yen) would have permitted
        # far more hires than the plan can realistically support.
        naive_max_hires = int(ch["dollar_amount"] / 800)
        assert ch["projected_hires"] < naive_max_hires

    def test_gbp_plan_floor_does_not_over_suppress(self):
        """Before Fix 3: a GBP plan's dollar amounts are smaller than the
        USD-equivalent, so comparing against the raw (unconverted) $800
        floor is stricter than the real-world floor (GBP630 = 800 / 1.27)
        and caps hires more aggressively than intended.

        Fixture: natural (pre-floor) hires = 140 on a GBP100,000 channel.
        The un-converted floor (GBP800) caps this to int(100000/800) = 125
        -- an unnecessary cap, since 140 hires already clears the REAL,
        correctly-converted floor of int(100000/629.92) = 158. A test that
        only asserts ``cost_per_hire >= 800/1.27`` would pass on both the
        buggy and fixed code (the stricter buggy floor also satisfies that
        inequality) -- this one asserts the actual hire count differs."""

        def make_ch():
            return {
                "cpc": 5.0,
                "apply_rate": 0.06,
                "category": "programmatic",
                "projected_applications": 1200,
                "projected_hires": 140,  # implies hire_rate 140/1200
            }

        ch_buggy = make_ch()
        be._recompute_channel_metrics(
            ch_buggy, 100_000.0, 200_000.0, intl_cpc_basis=None
        )
        ch_fixed = make_ch()
        be._recompute_channel_metrics(
            ch_fixed, 100_000.0, 200_000.0, intl_cpc_basis=_GBP_BASIS
        )
        assert ch_buggy["projected_hires"] == 125, (
            "sanity: the un-converted floor should over-suppress to "
            f"int(100000/800)=125, got {ch_buggy['projected_hires']}"
        )
        assert ch_fixed["projected_hires"] == 140, (
            "the correctly-converted floor (GBP629.92) should not suppress "
            f"the natural 140 hires, got {ch_fixed['projected_hires']}"
        )
        assert ch_fixed["projected_hires"] > ch_buggy["projected_hires"]

    def test_us_plan_floor_unaffected(self):
        """No intl_cpc_basis (the default, and every existing US caller):
        byte-identical floor behavior to before Fix 3."""
        role_budgets = {"role1": {"dollar_amount": 90_000.0}}
        allocs = be.compute_channel_dollar_amounts(
            {"programmatic_dsp": 100.0},
            role_budgets,
            None,
            None,
            industry="logistics_supply_chain",
            collar_type="blue_collar",
        )
        ch = allocs["programmatic_dsp"]
        assert ch["cost_per_hire"] >= be._CHANNEL_MIN_CPH["programmatic"] * 0.999


# ---------------------------------------------------------------------------
# 3. compute_channel_dollar_amounts -- the efficiency-flag threshold
# ---------------------------------------------------------------------------
class TestEfficiencyFlagThresholdIsLocalCurrency:
    def test_trivial_jpy_spend_is_not_mislabeled_low_efficiency(self):
        """A JPY50,000 (~$335) spend with 0 hires is a trivial real-world
        amount -- it must be labeled 'No Projected Hires', not 'Low
        Efficiency', which the raw (un-converted) $1000-read-as-yen
        threshold would have wrongly assigned to almost any nonzero JPY
        spend."""
        role_budgets = {"role1": {"dollar_amount": 50_000.0}}
        basis = dict(_JPY_BASIS, categories={"job_board": 100_000.0})
        allocs = be.compute_channel_dollar_amounts(
            {"global_boards": 100.0},
            role_budgets,
            None,
            None,
            industry="logistics_supply_chain",
            collar_type="blue_collar",
            intl_cpc_basis=basis,
        )
        ch = allocs["global_boards"]
        assert ch["projected_hires"] == 0
        assert ch["dollar_amount"] > 1000, "fixture must exceed the raw threshold"
        assert ch["efficiency_flag"] == "No Projected Hires", ch["efficiency_flag"]

    def test_us_plan_threshold_unaffected(self):
        """A US plan (no intl_cpc_basis) keeps the raw, un-converted $1000
        threshold -- a trivial $1 zero-hire spend is 'No Projected Hires',
        not 'Low Efficiency', exactly as before Fix 3."""
        role_budgets = {"role1": {"dollar_amount": 1.0}}
        allocs = be.compute_channel_dollar_amounts(
            {"global_boards": 100.0},
            role_budgets,
            None,
            None,
            industry="logistics_supply_chain",
            collar_type="blue_collar",
        )
        ch = allocs["global_boards"]
        assert ch["projected_hires"] == 0
        assert ch["efficiency_flag"] == "No Projected Hires"


# ---------------------------------------------------------------------------
# 4. _recompute_channel_metrics (call site 2, used by rebalance_low_roi_channels)
# ---------------------------------------------------------------------------
class TestRecomputeChannelMetricsFloorConversion:
    @staticmethod
    def _channel():
        return {
            "cpc": 5.0,
            "apply_rate": 0.06,
            "category": "programmatic",
            "projected_applications": 36000,
            "projected_hires": 720,
        }

    def test_jpy_basis_caps_hires_that_none_basis_leaves_uncapped(self):
        ch_none = self._channel()
        be._recompute_channel_metrics(
            ch_none, 3_000_000.0, 5_000_000.0, intl_cpc_basis=None
        )
        ch_jpy = self._channel()
        be._recompute_channel_metrics(
            ch_jpy, 3_000_000.0, 5_000_000.0, intl_cpc_basis=_JPY_BASIS
        )
        assert ch_none["projected_hires"] == 720, (
            "sanity: without a basis the raw $800-as-yen floor should not "
            "engage at this scale"
        )
        assert ch_jpy["projected_hires"] < ch_none["projected_hires"]
        correct_floor_local = 800 / 0.0067
        assert ch_jpy["cost_per_hire"] >= correct_floor_local


# ---------------------------------------------------------------------------
# 5. optimize_allocation (call site 3)
# ---------------------------------------------------------------------------
class TestOptimizeAllocationFloorConversion:
    @staticmethod
    def _channel_allocations():
        return {
            "programmatic_dsp": {  # very efficient -> recipient
                "dollar_amount": 3_000_000.0,
                "percentage": 50.0,
                "cpc": 5.0,
                "category": "programmatic",
                "apply_rate": 0.06,
                "apply_rate_collar_adjusted": True,
                "projected_hires": 720,
                "cost_per_hire": 4166.7,
            },
            "global_boards": {  # mid -- anchors the median
                "dollar_amount": 2_000_000.0,
                "percentage": 33.0,
                "cpc": 120.0,
                "category": "job_board",
                "apply_rate": 0.08,
                "apply_rate_collar_adjusted": True,
                "projected_hires": 60,
                "cost_per_hire": 33333.3,
            },
            "social_media": {  # very poor -> donor
                "dollar_amount": 1_000_000.0,
                "percentage": 17.0,
                "cpc": 250.0,
                "category": "social",
                "apply_rate": 0.03,
                "apply_rate_collar_adjusted": True,
                "projected_hires": 2,
                "cost_per_hire": 500_000.0,
            },
        }

    def test_jpy_basis_reduces_hires_none_basis_leaves_unrealistic(self):
        res_none = be.optimize_allocation(
            copy.deepcopy(self._channel_allocations()),
            6_000_000.0,
            "hires",
            collar_type="blue_collar",
            intl_cpc_basis=None,
        )
        res_jpy = be.optimize_allocation(
            copy.deepcopy(self._channel_allocations()),
            6_000_000.0,
            "hires",
            collar_type="blue_collar",
            intl_cpc_basis=_JPY_BASIS,
        )
        prog_none = res_none["optimized_allocations"]["programmatic_dsp"]
        prog_jpy = res_jpy["optimized_allocations"]["programmatic_dsp"]
        # Without a basis the raw $800-as-yen floor barely constrains this
        # channel's naturally cheap-looking CPC; with the correct JPY basis
        # it is capped to something realistic.
        assert prog_jpy["projected_hires"] < prog_none["projected_hires"]
        correct_floor_local = 800 / 0.0067
        assert prog_jpy["cost_per_hire"] >= correct_floor_local

    def test_no_basis_reproduces_prior_behavior(self):
        """intl_cpc_basis defaults to None -- every pre-existing caller of
        optimize_allocation (before Fix 3 added the parameter) gets exactly
        the same result as passing None explicitly."""
        res_default = be.optimize_allocation(
            copy.deepcopy(self._channel_allocations()),
            6_000_000.0,
            "hires",
            collar_type="blue_collar",
        )
        res_explicit_none = be.optimize_allocation(
            copy.deepcopy(self._channel_allocations()),
            6_000_000.0,
            "hires",
            collar_type="blue_collar",
            intl_cpc_basis=None,
        )
        assert (
            res_default["optimized_allocations"]
            == res_explicit_none["optimized_allocations"]
        )


# ---------------------------------------------------------------------------
# 6. The flat-cost ~$50/application heuristic (referral/events/staffing --
#    channels with no CPC model). Same USD/local mismatch, confirmed during
#    adversarial review of the fix above: present at the same three call
#    sites (compute_channel_dollar_amounts, _recompute_channel_metrics,
#    optimize_allocation) plus _dedupe_shared_fallback_cpcs's own collision
#    fallback -- all four now convert via _usd_const_to_intl_basis too.
# ---------------------------------------------------------------------------
class TestFlatCostPerApplicationIsLocalCurrency:
    def test_jpy_referral_channel_applications_are_not_inflated(self):
        """Before: ~$50/application read as '50 yen' on a JPY plan inflated
        applications (and hires) by ~149x -- a referral channel could
        report 100,000 applications from a JPY5M budget instead of ~670."""
        role_budgets = {"role1": {"dollar_amount": 5_000_000.0}}
        allocs_none = be.compute_channel_dollar_amounts(
            {"Referral Programs": 100.0},
            role_budgets,
            None,
            None,
            industry="logistics_supply_chain",
            collar_type="blue_collar",
            intl_cpc_basis=None,
        )
        allocs_jpy = be.compute_channel_dollar_amounts(
            {"Referral Programs": 100.0},
            role_budgets,
            None,
            None,
            industry="logistics_supply_chain",
            collar_type="blue_collar",
            intl_cpc_basis=_JPY_BASIS,
        )
        apps_none = allocs_none["Referral Programs"]["projected_applications"]
        apps_jpy = allocs_jpy["Referral Programs"]["projected_applications"]
        assert apps_none == 100_000, "sanity: no-basis path unchanged"
        assert apps_jpy < apps_none / 50, (
            f"JPY applications ({apps_jpy}) should collapse by ~149x from "
            f"the no-basis figure ({apps_none}), not scale with it"
        )

    def test_dedupe_shared_fallback_flat_cost_is_local_currency(self):
        """_dedupe_shared_fallback_cpcs's own cpc<=0 flat-cost branch (hit
        when its collision-resolved static-benchmark CPC is 0, e.g.
        referral/events) must also convert the ~$50/application constant."""

        def make_allocs():
            return {
                "referral": {
                    "cpc": 5.0,
                    "cpc_source": "kb",
                    "category": "referral",
                    "dollar_amount": 3_000_000.0,
                    "apply_rate": 0.25,
                    "projected_applications": 100,
                    "projected_hires": 10,
                },
                "events": {
                    "cpc": 5.0,
                    "cpc_source": "kb",
                    "category": "events",
                    "dollar_amount": 2_000_000.0,
                    "apply_rate": 0.15,
                    "projected_applications": 50,
                    "projected_hires": 5,
                },
            }

        allocs_none = make_allocs()
        be._dedupe_shared_fallback_cpcs(allocs_none, intl_cpc_basis=None)
        allocs_jpy = make_allocs()
        be._dedupe_shared_fallback_cpcs(allocs_jpy, intl_cpc_basis=_JPY_BASIS)
        # Both categories' static-benchmark CPC (referral, events) is 0 in
        # BASE_BENCHMARKS, so both land on the flat-cost branch.
        assert allocs_none["referral"]["cpc"] == 0.0
        assert (
            allocs_jpy["referral"]["projected_applications"]
            < allocs_none["referral"]["projected_applications"] / 50
        )
        assert (
            allocs_jpy["events"]["projected_applications"]
            < allocs_none["events"]["projected_applications"] / 50
        )

    def test_no_basis_reproduces_prior_dedupe_behavior(self):
        """intl_cpc_basis defaults to None -- the pre-existing lone caller
        (compute_channel_dollar_amounts) passing no basis at all reproduces
        this function's exact prior signature and behavior."""
        allocs_default = {
            "referral": {
                "cpc": 5.0,
                "cpc_source": "kb",
                "category": "referral",
                "dollar_amount": 3_000_000.0,
                "apply_rate": 0.25,
                "projected_applications": 100,
                "projected_hires": 10,
            },
            "events": {
                "cpc": 5.0,
                "cpc_source": "kb",
                "category": "events",
                "dollar_amount": 2_000_000.0,
                "apply_rate": 0.15,
                "projected_applications": 50,
                "projected_hires": 5,
            },
        }
        allocs_explicit_none = copy.deepcopy(allocs_default)
        be._dedupe_shared_fallback_cpcs(allocs_default)
        be._dedupe_shared_fallback_cpcs(allocs_explicit_none, intl_cpc_basis=None)
        assert allocs_default == allocs_explicit_none


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
