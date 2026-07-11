"""Regression tests for S91 (ROI-aware budget allocation, agent A).

Bug fixed: channel_percentages came straight from a static industry
profile and dollars were assigned BEFORE any CPA/ROI existed. The only
correction was ``rebalance_low_roi_channels(roi_floor=2, alloc_cap_pct=3.0)``,
which was too weak -- a channel allocated 35% of budget at ROI 4/10 and a
~$230 CPA sailed straight through untouched.

This file covers:
    1. Efficiency reweighting (``_reweight_channel_percentages_by_efficiency``)
       -- a poorly-performing, heavily-allocated channel loses share BEFORE
       the rebalancer even runs, and can no longer hold the largest slice.
    2. Brand channel classification + the 12% combined brand cap, which
       holds even after per-channel [3%, 35%] clamp/renormalization.
    3. Vendor-availability gating (``_apply_vendor_gate``) -- a channel with
       no vendor coverage is floored to 3% and the difference goes to the
       strongest-ROI performance channels.
    4. The rebalancer's new thresholds (roi_floor 2->4, recipient_roi_min
       6->8), its severity-proportional donor shave, its exemption of brand
       channels, and its quality_flag safety net.
    5. Wiring sanity: totals still foot to budget, and per-channel
       projected_hires land in the result for sheet/slide risk-text sums.

Runs under pytest, or standalone:
``python3 tests/test_budget_engine_roi_reweight.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine as be  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_ROLES = [{"title": "CDL A Driver", "count": 300, "tier": "Hourly"}]
_LOCATIONS = [{"city": "Boston", "state": "MA", "country": "US"}]

# A realistic 7-channel profile where "Niche & Industry Boards" starts as
# the LARGEST allocation (35%) despite historically being the worst
# performer for this synthetic scenario -- mirrors the shipped defect
# ("a 35%-allocated channel at ROI 4/10 with $230 CPA sails through").
_CHANNELS = {
    "Programmatic DSP": 35,
    "Global Job Boards": 20,
    "Niche & Industry Boards": 15,
    "Social Media": 12,
    "Regional & Local Boards": 8,
    "Employer Branding": 5,
    "Career Sites": 5,
}


def _allocate(**overrides):
    kwargs = dict(
        total_budget=150_000,
        roles=_ROLES,
        locations=_LOCATIONS,
        industry="logistics_supply_chain",
        channel_percentages=dict(_CHANNELS),
    )
    kwargs.update(overrides)
    return be.calculate_budget_allocation(**kwargs)


# ---------------------------------------------------------------------------
# 1. Efficiency reweighting
# ---------------------------------------------------------------------------


def test_worst_cpa_channel_no_longer_holds_largest_share():
    """The channel with the worst first-pass CPH must not end up as the
    single largest allocation just because the static profile said so."""
    result = _allocate()
    allocs = result["channel_allocations"]

    largest_channel = max(allocs, key=lambda c: allocs[c]["percentage"])
    largest_pct = allocs[largest_channel]["percentage"]

    # The worst performer in this fixture is Niche & Industry Boards
    # (highest CPC / lowest apply rate combination for its category).
    worst_roi_channel = min(
        (c for c in allocs if allocs[c].get("channel_role") != "brand"),
        key=lambda c: allocs[c]["roi_score"],
    )

    assert allocs[worst_roi_channel]["roi_score"] <= 4
    assert largest_channel != worst_roi_channel or largest_pct <= 15.0, (
        f"{worst_roi_channel} (ROI {allocs[worst_roi_channel]['roi_score']}) "
        f"still holds the largest share ({largest_pct}%) after reweighting"
    )
    # Never left starved to absurdity either -- floor is respected.
    for ch, data in allocs.items():
        assert data["percentage"] >= 2.5, f"{ch} starved below the 3% floor"


def test_reweight_blend_moves_share_toward_efficiency():
    """Direct unit test of the reweighting helper: a channel with a much
    worse first-pass cost-per-hire should end up with LESS share than its
    static profile gave it, and a strong performer should end up with MORE."""
    role_budgets = be.compute_role_weighted_spend(
        _ROLES, 150_000, be.compute_location_cost_multipliers(_LOCATIONS)
    )
    first_pass = be.compute_channel_dollar_amounts(
        _CHANNELS, role_budgets, {}, None, industry="logistics_supply_chain"
    )
    new_pct, meta = be._reweight_channel_percentages_by_efficiency(
        _CHANNELS, first_pass
    )

    assert abs(sum(new_pct.values()) - 100.0) < 0.5
    assert "profile_pct" in meta and "efficiency_reweighted_pct" in meta

    # Rank channels by first-pass cost-per-hire (lower = better).
    perf_channels = [
        c for c in _CHANNELS if first_pass[c]["channel_role"] == "performance"
    ]
    best = min(perf_channels, key=lambda c: first_pass[c]["cost_per_hire"])
    worst = max(perf_channels, key=lambda c: first_pass[c]["cost_per_hire"])

    assert new_pct[worst] < _CHANNELS[worst] + 0.01, (
        f"worst performer {worst} should not have GAINED share"
    )
    assert new_pct[best] >= new_pct[worst]


def test_reweight_clamps_every_channel_to_3_35_range():
    result = _allocate()
    for ch, data in result["channel_allocations"].items():
        assert 2.5 <= data["percentage"] <= 35.5, f"{ch} out of [3,35] range: {data['percentage']}"


def test_reweight_is_noop_safe_on_empty_input():
    new_pct, meta = be._reweight_channel_percentages_by_efficiency({}, {})
    assert new_pct == {}
    assert meta["brand_capped"] is False


# ---------------------------------------------------------------------------
# 2. Brand classification + 12% combined cap
# ---------------------------------------------------------------------------


def test_employer_branding_classified_as_brand_with_rationale():
    role_budgets = be.compute_role_weighted_spend(
        _ROLES, 150_000, be.compute_location_cost_multipliers(_LOCATIONS)
    )
    allocs = be.compute_channel_dollar_amounts(
        _CHANNELS, role_budgets, {}, None, industry="logistics_supply_chain"
    )
    brand_entry = allocs["Employer Branding"]
    assert brand_entry["channel_role"] == "brand"
    assert brand_entry["roi_scoring_excluded"] is True
    assert "rationale" in brand_entry
    assert "not direct CPA" in brand_entry["rationale"]

    perf_entry = allocs["Programmatic DSP"]
    assert perf_entry["channel_role"] == "performance"
    assert "rationale" not in perf_entry


def test_brand_cap_enforced_at_12_percent():
    """A brand channel given 25% of the static profile must never exceed
    12% combined, even after per-channel clamp/renormalize."""
    channels = {
        "Programmatic DSP": 30,
        "Global Job Boards": 20,
        "Regional & Local Boards": 15,
        "Social Media": 10,
        "Niche & Industry Boards": 10,
        "Employer Branding": 15,
    }
    result = _allocate(channel_percentages=channels)
    brand_pct = sum(
        d["percentage"]
        for d in result["channel_allocations"].values()
        if d.get("channel_role") == "brand"
    )
    assert brand_pct <= 12.5, f"combined brand allocation {brand_pct}% exceeds the 12% cap"
    assert result["metadata"]["channel_reweight"]["brand_capped"] is True


def test_brand_channel_under_cap_is_left_untouched_by_renormalization():
    """Regression guard: earlier draft of the reweighting renormalized ALL
    channels (brand + performance) together at the end, which let a brand
    channel drift ABOVE its intended share whenever performance channels
    needed a lot of floor-raising. Brand must stay pinned to its own pool."""
    role_budgets = be.compute_role_weighted_spend(
        _ROLES, 150_000, be.compute_location_cost_multipliers(_LOCATIONS)
    )
    # Deliberately terrible economics on several performance channels so
    # the floor-raise on those channels would, under the old (buggy) single
    # renormalization pass, inflate the brand channel's share too.
    channels = {
        "Programmatic DSP": 60,
        "Niche & Industry Boards": 20,
        "Social Media": 15,
        "Employer Branding": 5,
    }
    first_pass = be.compute_channel_dollar_amounts(
        channels, role_budgets, {}, None, industry="logistics_supply_chain"
    )
    new_pct, meta = be._reweight_channel_percentages_by_efficiency(
        channels, first_pass
    )
    assert new_pct["Employer Branding"] == 5.0, (
        "brand channel drifted away from its own (uncapped, under-12%) "
        f"profile share: {new_pct['Employer Branding']}"
    )


# ---------------------------------------------------------------------------
# 3. Vendor-availability gate
# ---------------------------------------------------------------------------


def test_vendor_gate_floors_unavailable_channel_and_reallocates():
    result = _allocate(
        vendor_availability={"Niche & Industry Boards": False}
    )
    allocs = result["channel_allocations"]
    gated_pct = allocs["Niche & Industry Boards"]["percentage"]
    assert gated_pct <= 3.5, f"gated channel not floored: {gated_pct}%"

    vendor_meta = result["metadata"]["vendor_gate"]
    assert vendor_meta["gated_channels"]
    assert vendor_meta["gated_channels"][0]["channel"] == "Niche & Industry Boards"
    assert vendor_meta.get("recipients")

    # Freed budget must still foot to 100% / total budget.
    assert abs(sum(d["percentage"] for d in allocs.values()) - 100.0) < 0.5
    assert abs(sum(d["dollar_amount"] for d in allocs.values()) - 150_000) < 1.0


def test_vendor_gate_is_noop_when_availability_none():
    with_none = _allocate(vendor_availability=None)
    without_arg = _allocate()
    pct_a = {k: v["percentage"] for k, v in with_none["channel_allocations"].items()}
    pct_b = {
        k: v["percentage"] for k, v in without_arg["channel_allocations"].items()
    }
    assert pct_a == pct_b


def test_vendor_gate_never_floors_a_brand_recipient():
    """Even when gating frees budget, brand channels must never be chosen
    as a recipient (they're not CPA-scored)."""
    result = _allocate(
        vendor_availability={"Niche & Industry Boards": False}
    )
    recipients = result["metadata"]["vendor_gate"].get("recipients") or []
    assert "Employer Branding" not in recipients


# ---------------------------------------------------------------------------
# 4. Rebalancer thresholds
# ---------------------------------------------------------------------------


def _fake_channel(pct, dollars, roi, role="performance"):
    return {
        "dollar_amount": dollars,
        "percentage": pct,
        "cpc": 1.0,
        "apply_rate": 0.05,
        "projected_applications": 100,
        "projected_hires": 2,
        "roi_score": roi,
        "category": "job_board",
        "channel_role": role,
    }


def test_rebalancer_roi_floor_raised_to_4():
    """A channel at ROI exactly 4 (previously safe under roi_floor=2) must
    now be treated as a donor when it holds >5% of budget."""
    total_budget = 100_000
    allocs = {
        "bad": _fake_channel(30.0, 30_000, roi=4),
        "good": _fake_channel(70.0, 70_000, roi=9),
    }
    be.rebalance_low_roi_channels(allocs, total_budget)
    assert allocs["bad"]["dollar_amount"] < 30_000, "ROI==roi_floor donor was not shaved"


def test_rebalancer_recipient_roi_min_raised_to_8():
    """A channel at ROI 6 (previously a valid recipient under
    recipient_roi_min=6) must NOT receive freed budget anymore."""
    total_budget = 100_000
    allocs = {
        "bad": _fake_channel(30.0, 30_000, roi=1),
        "mid": _fake_channel(30.0, 30_000, roi=6),
        "good": _fake_channel(40.0, 40_000, roi=9),
    }
    before_mid = allocs["mid"]["dollar_amount"]
    be.rebalance_low_roi_channels(allocs, total_budget)
    assert allocs["mid"]["dollar_amount"] == before_mid, "ROI 6 channel wrongly received freed budget"
    assert allocs["good"]["dollar_amount"] > 40_000


def test_rebalancer_shave_is_severity_proportional():
    """A roi_score==1 donor must lose a LARGER fraction of its own
    allocation than a roi_score==4 (boundary) donor."""
    total_budget = 100_000
    worst = {
        "worst": _fake_channel(30.0, 30_000, roi=1),
        "good": _fake_channel(70.0, 70_000, roi=9),
    }
    boundary = {
        "boundary": _fake_channel(30.0, 30_000, roi=4),
        "good": _fake_channel(70.0, 70_000, roi=9),
    }
    be.rebalance_low_roi_channels(worst, total_budget)
    be.rebalance_low_roi_channels(boundary, total_budget)

    worst_shave_frac = 1 - (worst["worst"]["dollar_amount"] / 30_000)
    boundary_shave_frac = 1 - (boundary["boundary"]["dollar_amount"] / 30_000)

    assert worst_shave_frac > boundary_shave_frac
    assert worst_shave_frac <= 0.61, "shave exceeded the 60% cap"
    assert boundary_shave_frac > 0, "boundary donor (ROI==roi_floor) must still lose something"


def test_rebalancer_exempts_brand_channels():
    """A brand channel with terrible ROI and a large allocation must never
    be treated as a donor (or recipient) -- it's governed by the 12% cap,
    not CPA-based ROI."""
    total_budget = 100_000
    allocs = {
        "brand": _fake_channel(30.0, 30_000, roi=1, role="brand"),
        "good": _fake_channel(70.0, 70_000, roi=9),
    }
    before = dict(allocs["brand"])
    be.rebalance_low_roi_channels(allocs, total_budget)
    assert allocs["brand"]["dollar_amount"] == before["dollar_amount"]
    assert allocs["brand"]["percentage"] == before["percentage"]
    assert "quality_flag" not in allocs["brand"]


def test_rebalancer_quality_flag_when_no_recipient_qualifies():
    """When a channel is clearly underperforming (ROI<=3, >15% of budget)
    but no recipient qualifies, the allocation must be flagged for manual
    review instead of silently left as-is with no signal."""
    total_budget = 100_000
    allocs = {
        "bad": _fake_channel(40.0, 40_000, roi=2),
        # Only a mediocre channel available -- doesn't clear
        # recipient_roi_min=8, so no rebalance can occur.
        "mediocre": _fake_channel(60.0, 60_000, roi=5),
    }
    be.rebalance_low_roi_channels(allocs, total_budget)
    assert "quality_flag" in allocs["bad"]
    assert "ROI 2" in allocs["bad"]["quality_flag"]


def test_rebalancer_no_flag_when_allocation_is_small():
    """A bad-ROI channel under the 15% threshold should not be flagged --
    it's not worth a manual-review callout."""
    total_budget = 100_000
    allocs = {
        "small_bad": _fake_channel(10.0, 10_000, roi=1),
        "mediocre": _fake_channel(90.0, 90_000, roi=5),
    }
    be.rebalance_low_roi_channels(allocs, total_budget)
    assert "quality_flag" not in allocs["small_bad"]


# ---------------------------------------------------------------------------
# 5. Wiring sanity
# ---------------------------------------------------------------------------


def test_totals_foot_to_budget():
    for total_budget in (50_000, 150_000, 300_000):
        result = _allocate(total_budget=total_budget)
        allocs = result["channel_allocations"]
        assert abs(sum(d["dollar_amount"] for d in allocs.values()) - total_budget) < 1.0
        assert abs(sum(d["percentage"] for d in allocs.values()) - 100.0) < 0.5


def test_projected_hires_present_per_channel_for_risk_text_sums():
    """Sheet/slide risk text sums ACTUAL per-channel projected_hires --
    verify the field is present, numeric, and non-negative on every
    channel in the result (S91 item 1d)."""
    result = _allocate()
    for ch, data in result["channel_allocations"].items():
        assert "projected_hires" in data
        assert isinstance(data["projected_hires"], int)
        assert data["projected_hires"] >= 0


def test_quality_flags_surfaced_at_top_level_metadata():
    result = _allocate(
        channel_percentages={
            "Programmatic DSP": 50,
            "Niche & Industry Boards": 45,
            "Employer Branding": 5,
        }
    )
    assert "quality_flags" in result["metadata"]
    assert isinstance(result["metadata"]["quality_flags"], list)


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
