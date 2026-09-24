"""Regression tests: ``efficiency_flag`` must never go stale after a later
budget_engine step rewrites a channel's ``projected_hires``/``dollar_amount``.

Coordinator-reported defect (2026-09-24), observed on a real jp_jpy bundle:
``_redistribute_hires_by_conversion`` (S92 -- see
test_budget_engine_hire_redistribution.py) mutates ``projected_hires`` /
``cost_per_hire`` / ``roi_score`` in place AFTER ``efficiency_flag`` was
already set by ``compute_channel_dollar_amounts``, and never recomputes it.
So a channel that had 0 hires at the FIRST pass (before redistribution)
keeps its "Low Efficiency" flag even after redistribution hands it a large,
real hire count -- niche_boards ended with efficiency_flag="Low Efficiency"
while its live projected_hires was 514, which put "Low Efficiency alert:
Niche / Industry Boards projected 0 hires" into the client workbook right
next to a channel visibly projecting hundreds of hires.

Root cause: ``efficiency_flag`` is a derived field (S49 Issue 16:
``hires == 0 and dollars > 1000`` -> "Low Efficiency"; ``hires == 0 and
dollars > 0`` -> "No Projected Hires"; else ``""``) computed ONCE in
``compute_channel_dollar_amounts`` and never refreshed by any of the SEVERAL
downstream steps that legitimately change hires/dollars afterward:
``_dedupe_shared_fallback_cpcs``, ``_redistribute_hires_by_conversion``,
``_recompute_channel_metrics`` (vendor-gate / reweight paths), the live-CPA
calibration blend, and the total-hires rescale step inside
``calculate_budget_allocation``.

Fix: a single ``_compute_efficiency_flag(dollars, hires)`` helper, called at
every one of those mutation sites (and at the original call site, so there
is exactly one implementation of the rule).

VACUOUSNESS: every test below was run against a pre-fix throwaway worktree
(``git worktree add --detach <base commit>``, no changes) BEFORE
``_compute_efficiency_flag`` existed, and failed there -- see the task
report for the observed pre-fix failures.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine as be  # noqa: E402


def _fake_channel(
    dollars,
    apps,
    hires,
    roi,
    category="job_board",
    role="performance",
    cpc_source="static_benchmark",
    efficiency_flag="",
):
    return {
        "dollar_amount": dollars,
        "percentage": 0.0,
        "cpc": 1.0,
        "cpc_source": cpc_source,
        "apply_rate": 0.05,
        "projected_applications": apps,
        "projected_hires": hires,
        "roi_score": roi,
        "category": category,
        "channel_role": role,
        "efficiency_flag": efficiency_flag,
    }


# ---------------------------------------------------------------------------
# 1. _compute_efficiency_flag itself -- the S49 Issue 16 rule, isolated.
# ---------------------------------------------------------------------------


def test_compute_efficiency_flag_low_efficiency_above_threshold():
    assert be._compute_efficiency_flag(dollars=5000, hires=0) == "Low Efficiency"


def test_compute_efficiency_flag_no_projected_hires_below_threshold():
    assert be._compute_efficiency_flag(dollars=500, hires=0) == "No Projected Hires"


def test_compute_efficiency_flag_clear_once_hires_nonzero():
    assert be._compute_efficiency_flag(dollars=5000, hires=1) == ""


def test_compute_efficiency_flag_clear_when_no_spend():
    assert be._compute_efficiency_flag(dollars=0, hires=0) == ""


# ---------------------------------------------------------------------------
# 2. _redistribute_hires_by_conversion -- the exact coordinator repro:
#    a channel starts flagged "Low Efficiency" (0 hires, real spend) but has
#    real application volume, so redistribution hands it a large non-zero
#    hire share. The stale flag must be cleared.
# ---------------------------------------------------------------------------


def test_redistribute_clears_stale_low_efficiency_flag_when_hires_land():
    allocs = {
        # Mirrors the real jp_jpy defect: 0 hires at first pass (before
        # redistribution), real spend, real apps -- flagged "Low
        # Efficiency" -- but its apps x conversion-midpoint weight is by
        # far the largest, so redistribution gives it the bulk of the
        # plan's total hires.
        "Niche / Industry Boards": _fake_channel(
            1_500_000,
            9000,
            0,
            1,
            category="niche_board",
            efficiency_flag="Low Efficiency",
        ),
        "Programmatic (DSP)": _fake_channel(
            13_908_000, 900, 48, 6, category="programmatic"
        ),
    }
    # Sanity on the fixture itself: pre-redistribution state reproduces the
    # reported defect shape (0 hires, flagged, real spend).
    assert allocs["Niche / Industry Boards"]["projected_hires"] == 0
    assert allocs["Niche / Industry Boards"]["efficiency_flag"] == "Low Efficiency"

    be._redistribute_hires_by_conversion(allocs, total_hires=550, industry_avg_cph=6000)

    niche = allocs["Niche / Industry Boards"]
    assert niche["projected_hires"] > 0, (
        "fixture didn't reproduce the defect precondition -- niche board "
        "must land a real hire share for this test to mean anything"
    )
    assert niche["efficiency_flag"] == "", (
        f"efficiency_flag is stale: projected_hires={niche['projected_hires']} "
        f"but efficiency_flag={niche['efficiency_flag']!r} still claims 0 hires "
        "-- this is the exact client-workbook defect "
        "('Low Efficiency alert: ... projected 0 hires' next to a channel "
        "visibly projecting hundreds of hires)"
    )


def test_redistribute_keeps_flag_cleared_for_brand_channels_with_spend():
    # Brand channels are pinned to 0 hires BY DESIGN (not a defect) -- they
    # correctly keep "Low Efficiency" (or "No Projected Hires"), since they
    # really do have 0 hires after redistribution too.
    allocs = {
        "Employer Branding": _fake_channel(
            2_500_000, 500, 0, 1, category="employer_branding", role="brand"
        ),
        "Programmatic (DSP)": _fake_channel(
            13_908_000, 900, 48, 6, category="programmatic"
        ),
    }
    be._redistribute_hires_by_conversion(allocs, total_hires=100, industry_avg_cph=6000)
    brand = allocs["Employer Branding"]
    assert brand["projected_hires"] == 0
    assert brand["efficiency_flag"] == "Low Efficiency"


def test_redistribute_zero_total_hires_branch_sets_flag_consistently():
    allocs = {
        "A": _fake_channel(5000, 100, 20, 5, efficiency_flag=""),
        "B": _fake_channel(200, 10, 1, 5, efficiency_flag=""),
    }
    be._redistribute_hires_by_conversion(allocs, total_hires=0, industry_avg_cph=6000)
    assert allocs["A"]["projected_hires"] == 0
    assert allocs["A"]["efficiency_flag"] == "Low Efficiency"  # $5000 > $1000
    assert allocs["B"]["projected_hires"] == 0
    assert allocs["B"]["efficiency_flag"] == "No Projected Hires"  # $200 <= $1000


# ---------------------------------------------------------------------------
# 3. _dedupe_shared_fallback_cpcs -- also rewrites hires/dollars in place.
# ---------------------------------------------------------------------------


def test_dedupe_shared_fallback_cpcs_recomputes_efficiency_flag():
    allocs = {
        "Niche / Industry Boards": _fake_channel(
            18_000,
            1500,
            0,
            8,
            category="niche_board",
            cpc_source="knowledge_base",
            efficiency_flag="Low Efficiency",
        ),
        "Employer Branding": _fake_channel(
            8_000,
            50,
            0,
            1,
            category="employer_branding",
            role="brand",
            cpc_source="knowledge_base",
            efficiency_flag="Low Efficiency",
        ),
    }
    allocs["Niche / Industry Boards"]["cpc"] = 11.47
    allocs["Employer Branding"]["cpc"] = 11.47

    be._dedupe_shared_fallback_cpcs(allocs)

    niche = allocs["Niche / Industry Boards"]
    # niche_board's category static benchmark (1.40) is cheap enough
    # relative to its $18,000 budget to project a non-zero hire count --
    # efficiency_flag must track that, not the stale pre-dedupe value.
    expected = be._compute_efficiency_flag(
        niche["dollar_amount"], niche["projected_hires"]
    )
    assert niche["efficiency_flag"] == expected


# ---------------------------------------------------------------------------
# 4. End-to-end via calculate_budget_allocation on a jp_jpy-shaped plan --
#    no channel's efficiency_flag may contradict its own final hires/spend.
# ---------------------------------------------------------------------------


def test_end_to_end_jp_jpy_no_channel_has_stale_efficiency_flag():
    roles = [
        {"title": "Warehouse Associate", "count": 80, "tier": "Hourly"},
        {"title": "Delivery Driver", "count": 40, "tier": "Hourly"},
    ]
    locations = [
        {"city": "Tokyo", "state": "", "country": "Japan"},
        {"city": "Osaka", "state": "", "country": "Japan"},
    ]
    channel_pcts = {
        "programmatic_dsp": 35,
        "global_boards": 20,
        "niche_boards": 15,
        "social_media": 12,
        "regional_boards": 8,
        "employer_branding": 5,
        "apac_regional": 3,
        "emea_regional": 2,
    }
    result = be.calculate_budget_allocation(
        total_budget=50_000_000,
        roles=roles,
        locations=locations,
        industry="logistics_supply_chain",
        channel_percentages=channel_pcts,
        collar_type="",
        campaign_start_month=10,
        locations_raw=["Tokyo, Japan", "Osaka, Japan"],
    )
    mismatches = []
    for name, ch in result["channel_allocations"].items():
        expected = be._compute_efficiency_flag(
            ch.get("dollar_amount"), ch.get("projected_hires")
        )
        actual = ch.get("efficiency_flag")
        if actual != expected:
            mismatches.append((name, ch.get("projected_hires"), actual, expected))
    assert not mismatches, (
        f"stale efficiency_flag(s) after full pipeline: {mismatches!r}"
    )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
