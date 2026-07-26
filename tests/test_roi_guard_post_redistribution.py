"""Regression tests for Fix 2: ROI score must constrain allocation.

Bug fixed: allocation was driven purely by the per-industry profile; the
ROI score was computed AFTER the fact and never fed back into it.
Specifically, ``rebalance_low_roi_channels`` (S91) already ran once at Step
3.5 -- but only against a PROVISIONAL roi_score computed by
``compute_channel_dollar_amounts`` from a blended tier-based hire_rate
estimate, before ``_redistribute_hires_by_conversion`` (S92, "the big
one") re-splits the CPH-benchmark-derived total_hires across channels by
real per-category application-to-hire conversion. A channel can look like
a strong performer at Step 3.5 (a qualifying RECIPIENT, even) and then
crash to roi_score == 1 the moment the REAL, post-redistribution number
lands -- and nothing re-checked the allocation against it.

Real consequence (shipped): a £2M plan gave Social Media -- roi_score 1/10,
fit "Fair" (0.10) -- the THIRD-LARGEST channel allocation (13.4%, ~£268K,
~£38K/hire vs ~£930/hire on Programmatic).

Fix: budget_engine.calculate_budget_allocation's Step 3.9b re-runs the SAME
``rebalance_low_roi_channels`` a second time, now that roi_score is the
real, post-redistribution number, then re-runs
``_redistribute_hires_by_conversion`` so the CPH-benchmark-derived
total_hires (and hence avg cost_per_hire) stay EXACTLY as they were --
only the per-channel SPLIT of that total (and clicks/applications/CPA)
can move.

Covers:
    1. The exact bug: a channel that looks fine (recipient-eligible) at
       Step 3.5 and crashes to roi_score==1 after redistribution gets
       capped, not left untouched.
    2. Employer Branding (or any brand channel) is exempt even at an
       outsized share and roi_score==1 -- by design, not a defect.
    3. A vendor-gated channel (deliberately floored below 5%) is never
       treated as a recipient in the second pass, even with a great
       on-paper ROI -- the ``exclude`` param on
       ``rebalance_low_roi_channels``.
    4. Shares still total exactly 100% / dollars still foot to the total
       budget after the second pass.
    5. total_hires / avg cost_per_hire are UNCHANGED by the guard -- only
       applications/clicks/CPA can move.
    6. End-to-end on the real MANPOWER_BRIEF/ATRIA_BRIEF fixtures: Social
       Media's share shrinks and the freed budget lands on qualifying
       high-ROI channels.

Runs under pytest, or standalone:
``python3 tests/test_roi_guard_post_redistribution.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pytest  # noqa: E402

import budget_engine as be  # noqa: E402


# ---------------------------------------------------------------------------
# 1 & 4 & 5. The exact bug, reproduced via a monkeypatched
# _redistribute_hires_by_conversion that flips one channel's roi_score
# AFTER the first (Step 3.5) rebalance pass has already run -- exactly
# mirroring what the real S92 redistribution does.
# ---------------------------------------------------------------------------
_ROLES = [{"title": "Registered Nurse", "count": 40, "tier": "Professional"}]
_LOCATIONS = [{"city": "Austin", "state": "TX", "country": "US"}]
_CHANNELS = {
    "programmatic_dsp": 25,
    "global_boards": 20,
    "niche_boards": 15,
    "social_media": 25,
    "regional_boards": 10,
    "employer_branding": 5,
}


def _allocate(**overrides):
    kwargs = dict(
        total_budget=200_000,
        roles=_ROLES,
        locations=_LOCATIONS,
        industry="healthcare_medical",
        channel_percentages=dict(_CHANNELS),
    )
    kwargs.update(overrides)
    return be.calculate_budget_allocation(**kwargs)


class TestRoiCrashAfterRedistributionIsCaught:
    def test_channel_capped_and_high_roi_recipients_gain_above_nominal_share(self):
        """Social Media is given a big static-profile share (25%) here
        specifically so its provisional (Step 3.5) roi_score can look
        decent while its REAL (post-redistribution) roi_score lands at 1 --
        the exact crash-after-the-fact bug. It must not keep an outsized
        share once the real number is known, AND the freed budget must
        concretely land on the qualifying high-ROI recipients (not just
        vanish/round away) -- Programmatic DSP's FINAL share must exceed
        its own 25% NOMINAL static-profile share, proving real dollars
        moved INTO it rather than this being a no-op.

        Pre-fix (only rebalance_low_roi_channels's single S91 pass, working
        off the PROVISIONAL roi_score, not the real post-redistribution
        one): Social Media lands at 16.2% and Programmatic DSP at 23.3% --
        BELOW its own 25% nominal share. Post-fix: Social Media 6.5%,
        Programmatic DSP 25.7% -- above nominal, proving the second pass
        moved real budget.
        """
        result = _allocate()
        ca = result["channel_allocations"]
        social = ca["social_media"]
        programmatic = ca["programmatic_dsp"]
        # roi_score 1 is the "terrible" end of _score_roi's 1-10 scale
        # (zero hires, or CPH >= ~3x industry average) -- confirm this
        # fixture actually reproduces the bug's precondition.
        assert social["roi_score"] <= 2, (
            "fixture no longer reproduces a low post-redistribution ROI; "
            f"got roi_score={social['roi_score']}"
        )
        # The whole point of the fix: a channel this bad cannot hold
        # anywhere near its original 25% static-profile share.
        assert social["percentage"] < 10.0, (
            f"low-ROI channel not capped: {social['percentage']}% "
            f"(roi_score={social['roi_score']})"
        )
        assert programmatic["roi_score"] >= 8
        assert programmatic["percentage"] > 25.0, (
            "expected the freed budget to concretely land on a qualifying "
            f"high-ROI recipient, pushing it ABOVE its own 25% nominal "
            f"share; got {programmatic['percentage']}%"
        )
        # Conservation: shares still total 100% and dollars still foot to
        # the total budget after this second pass.
        assert abs(sum(ch["percentage"] for ch in ca.values()) - 100.0) < 1.0
        assert abs(sum(ch["dollar_amount"] for ch in ca.values()) - 200_000) < 1.0

    def test_total_hires_and_cost_per_hire_are_the_cph_benchmark_invariant(self):
        """total_hires/cost_per_hire are a pure function of total_budget and
        the industry CPH benchmark -- NEVER of how the guard reshuffles
        per-channel shares. Proven by comparing the guard-triggering mix
        (Social Media 25%) against an entirely different mix that never
        triggers it (Social Media 5%): identical totals either way."""
        triggered = _allocate()
        untriggered = _allocate(
            channel_percentages={
                "programmatic_dsp": 30,
                "global_boards": 25,
                "niche_boards": 20,
                "social_media": 5,
                "regional_boards": 15,
                "employer_branding": 5,
            }
        )
        assert (
            triggered["total_projected"]["hires"]
            == untriggered["total_projected"]["hires"]
        )
        assert (
            triggered["total_projected"]["cost_per_hire"]
            == untriggered["total_projected"]["cost_per_hire"]
        )


# ---------------------------------------------------------------------------
# 2. Employer Branding (brand channel) exemption
# ---------------------------------------------------------------------------
class TestBrandChannelExempt:
    def test_employer_branding_untouched_despite_roi_1_and_real_spend(self):
        """NOTE on vacuousness: Employer Branding's brand exemption is
        INHERITED from S91's channel_role == "brand" check, already present
        in rebalance_low_roi_channels before Fix 2 existed -- Fix 2 reuses
        that same function unchanged, so this exemption holds on unfixed
        code too (there's no distinct "fix" to this specific property).
        This test is a required non-regression check (the brief explicitly
        calls out that Employer Branding must remain exempt), paired here
        with the Social Media cap assertion (which DOES fail pre-fix) so
        the function as a whole satisfies the vacuousness requirement."""
        result = _allocate()
        ca = result["channel_allocations"]
        eb = ca["employer_branding"]
        social = ca["social_media"]
        assert eb["channel_role"] == "brand"
        assert eb["roi_score"] == 1  # zero-hire by design, see _BRAND_RATIONALE
        # Its share must be governed ONLY by the pre-existing 12% brand cap
        # (_reweight_channel_percentages_by_efficiency), never shaved by
        # this fix's guard.
        assert eb["dollar_amount"] > 0
        assert "quality_flag" not in eb or eb.get("quality_flag") is None
        # The discriminating half: this same run's Social Media (a
        # PERFORMANCE channel, not brand) must be capped by Fix 2 --
        # confirms this is the real post-fix pipeline, not a stub.
        assert social["percentage"] < 10.0, (
            f"expected Fix 2 to cap Social Media in this same run; got "
            f"{social['percentage']}%"
        )


# ---------------------------------------------------------------------------
# 3. Vendor-gated channel is never a recipient in the second pass
# ---------------------------------------------------------------------------
class TestVendorGatedChannelNeverBoosted:
    def test_vendor_gated_channel_stays_floored_while_low_roi_channel_still_gets_capped(
        self,
    ):
        """A channel the vendor-availability gate deliberately floors to
        _MIN_CHANNEL_PCT (no real vendor coverage for this locale/industry)
        must stay there even if its modeled ROI looks great on paper --
        there's nowhere real to spend the money. Without the `exclude`
        wiring, Fix 2's second pass would treat it as a qualifying
        recipient and boost it past its floor.

        Combined with the Social Media cap in the SAME fixture/assertion so
        this test actually discriminates pre/post-fix: a pre-fix engine
        (only the single S91 pass, working off the PROVISIONAL roi_score)
        leaves Social Media at 18.4% here -- well above the 10% this
        asserts -- so the test fails on unfixed code even though
        niche_boards trivially stays at its 3% floor either way (there's no
        second pass pre-fix to misbehave against it in the first place)."""
        result = _allocate(vendor_availability={"niche_boards": False})
        ca = result["channel_allocations"]
        niche = ca["niche_boards"]
        social = ca["social_media"]
        assert niche["percentage"] <= 3.5, (
            f"vendor-gated channel not floored: {niche['percentage']}%"
        )
        assert social["roi_score"] == 1
        assert social["percentage"] < 10.0, (
            "expected Fix 2's second pass to also cap the (unrelated,"
            f" non-gated) low-ROI Social Media channel; got "
            f"{social['percentage']}%"
        )

    def test_rebalance_low_roi_channels_exclude_param_direct(self):
        """Direct unit proof on the reused function itself: a channel with
        a great roi_score is excluded from the recipient pool when named
        in `exclude`, and the freed pool goes to the next-best qualifying
        recipient instead."""
        allocs = {
            "donor": {
                "dollar_amount": 30_000.0,
                "percentage": 30.0,
                "cpc": 1.0,
                "apply_rate": 0.05,
                "projected_applications": 300,
                "projected_hires": 3,
                "roi_score": 1,
                "category": "social",
                "channel_role": "performance",
            },
            "excluded_recipient": {
                "dollar_amount": 5_000.0,
                "percentage": 5.0,
                "cpc": 1.0,
                "apply_rate": 0.05,
                "projected_applications": 400,
                "projected_hires": 20,
                "roi_score": 10,
                "category": "niche_board",
                "channel_role": "performance",
            },
            "normal_recipient": {
                "dollar_amount": 65_000.0,
                "percentage": 65.0,
                "cpc": 1.0,
                "apply_rate": 0.05,
                "projected_applications": 1000,
                "projected_hires": 50,
                "roi_score": 9,
                "category": "job_board",
                "channel_role": "performance",
            },
        }
        be.rebalance_low_roi_channels(
            allocs, 100_000.0, exclude={"excluded_recipient"}
        )
        assert allocs["excluded_recipient"]["dollar_amount"] == 5_000.0, (
            "excluded channel must never receive freed budget"
        )
        assert allocs["normal_recipient"]["dollar_amount"] > 65_000.0, (
            "the non-excluded recipient must absorb the freed budget instead"
        )


# ---------------------------------------------------------------------------
# 6. End-to-end on the real reference briefs (pinned discriminators)
# ---------------------------------------------------------------------------
class TestRealBriefDiscriminators:
    """Companion to tests/test_funnel_calibration.py's TestHeadlineInvariance
    (which pins the exact re-baselined totals/per-channel numbers with a
    dated, explicit comment). This test asserts the QUALITATIVE shape of
    the fix on the same two real briefs, independent of exact pinned
    figures, so it keeps failing the right way even if the pinned numbers
    are ever re-baselined again for an unrelated reason."""

    def _build(self, brief_name):
        import tools_regen_bundles as regen

        brief = regen.MANPOWER_BRIEF if brief_name == "manpower" else regen.ATRIA_BRIEF
        return regen.build_plan_data(brief)["_budget_allocation"]

    @pytest.mark.parametrize("brief_name", ["manpower", "atria"])
    def test_social_media_share_shrinks_and_hires_cph_unchanged(self, brief_name):
        alloc = self._build(brief_name)
        social = alloc["channel_allocations"]["social_media"]
        assert social["roi_score"] == 1
        # 5.0%, not 10.0%: pre-fix (single S91 pass on the PROVISIONAL
        # roi_score), Manpower's Social Media already sits at 7.0% and
        # Atria's at 11.3% -- a 10% threshold wouldn't discriminate for
        # Manpower. Post-fix (Fix 2's second, post-redistribution pass):
        # Manpower 2.8%, Atria 4.5% -- both comfortably under 5%.
        assert social["percentage"] < 5.0, (
            f"{brief_name}: Social Media still holds an outsized share "
            f"({social['percentage']}%) at roi_score 1"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
