"""Regression tests for S92 (hire-distribution coherence, agent EN).

Bug fixed: the plan's TOTAL hire count was CPH-benchmark-derived and
conservative (kept as-is by this fix), but the PER-CHANNEL split of that
total came from a blended, tier-based hire_rate that ignores channel type
entirely -- so niche/social/EB channels routinely modeled at 0 hires while
holding 8-24% of budget, even though the plan's own printed conversion
table (``_HIRE_CONVERSION_RATES`` / excel_v2._ROI_CONVERSION_RATES) ranks
niche/specialty conversion HIGHEST (10-15%) of any channel type. The
'Fit'/vetted-tier tables also used to derive from two unrelated scoring
paths, so the same channel could rank #1 in one table and worst in the
other on the same worksheet.

This file covers:
    1. ``_redistribute_hires_by_conversion`` -- total hires preserved
       exactly (largest-remainder rounding), split proportional to
       apps x conversion-rate midpoint, brand channels pinned to 0.
    2. ``_finalize_channel_ranking`` -- fit_score/vetted_tier derive from
       the single final roi_score ranking; brand channels get their own tier.
    3. ``_dedupe_shared_fallback_cpcs`` -- a shared-fallback CPC collision
       across different channel categories is split into distinct,
       category-plausible values and confidence downgraded to 'estimated'.
    4. End-to-end via ``calculate_budget_allocation`` on brief-shaped
       fixtures mirroring the real manpower/atria bundles: a niche board
       with meaningful applications no longer lands on 0 hires, and the
       top-dollar performance channel is never the worst roi_score.

Runs under pytest, or standalone:
``python3 tests/test_budget_engine_hire_redistribution.py``.
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


def _fake_channel(
    dollars,
    apps,
    hires,
    roi,
    category="job_board",
    role="performance",
    cpc_source="static_benchmark",
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
    }


# Brief-shaped fixture mirroring the real manpower brief's channel profile
# (blue-collar CDL driver hiring, 6 channels, $150K budget).
_MANPOWER_ROLES = [{"title": "CDL A Driver", "count": 300, "tier": "Hourly"}]
_MANPOWER_LOCATIONS = [{"city": "Denver", "state": "CO", "country": "US"}]
_MANPOWER_CHANNELS = {
    "Programmatic (DSP)": 28,
    "Regional Job Boards": 27,
    "Global Job Boards": 25,
    "Niche / Industry Boards": 12,
    "Employer Branding": 5,
    "Social Media": 3,
}

# Brief-shaped fixture mirroring the real atria brief (senior-living roles,
# 6 channels, $300K budget).
_ATRIA_ROLES = [{"title": "Registered Nurse", "count": 40, "tier": "Clinical"}]
_ATRIA_LOCATIONS = [{"city": "New York", "state": "NY", "country": "US"}]
_ATRIA_CHANNELS = {
    "Programmatic (DSP)": 29,
    "Global Job Boards": 28,
    "Regional Job Boards": 19,
    "Niche / Industry Boards": 9,
    "Employer Branding": 8,
    "Social Media": 7,
}


def _allocate_manpower(**overrides):
    kwargs = dict(
        total_budget=150_000,
        roles=_MANPOWER_ROLES,
        locations=_MANPOWER_LOCATIONS,
        industry="logistics_supply_chain",
        channel_percentages=dict(_MANPOWER_CHANNELS),
    )
    kwargs.update(overrides)
    return be.calculate_budget_allocation(**kwargs)


def _allocate_atria(**overrides):
    kwargs = dict(
        total_budget=300_000,
        roles=_ATRIA_ROLES,
        locations=_ATRIA_LOCATIONS,
        industry="healthcare_medical",
        channel_percentages=dict(_ATRIA_CHANNELS),
    )
    kwargs.update(overrides)
    return be.calculate_budget_allocation(**kwargs)


# ---------------------------------------------------------------------------
# 1. _redistribute_hires_by_conversion
# ---------------------------------------------------------------------------


def test_total_hires_preserved_exactly_via_largest_remainder():
    """The total hire count is CPH-benchmark-derived and must be kept
    EXACTLY -- redistribution only changes the per-channel split."""
    allocs = {
        "a": _fake_channel(50_000, 4000, 0, 5, category="programmatic"),
        "b": _fake_channel(30_000, 1200, 0, 5, category="niche_board"),
        "c": _fake_channel(20_000, 300, 0, 5, category="social"),
        "brand": _fake_channel(
            10_000, 50, 0, 1, category="employer_branding", role="brand"
        ),
    }
    for total in (0, 1, 7, 48, 57, 101, 999):
        allocs_copy = {k: dict(v) for k, v in allocs.items()}
        be._redistribute_hires_by_conversion(
            allocs_copy, total, industry_avg_cph=4000.0
        )
        assert (
            sum(c["projected_hires"] for c in allocs_copy.values()) == total
        ), f"total={total} did not round-trip exactly"


def test_niche_board_with_meaningful_apps_gets_nonzero_hires():
    """THE BIG ONE: a niche board with real application volume must no
    longer be modeled at 0 hires just because a blended hire_rate ignored
    its (highest-of-all) conversion rate."""
    allocs = {
        "Programmatic (DSP)": _fake_channel(
            60_000, 9000, 0, 10, category="programmatic"
        ),
        "Regional Job Boards": _fake_channel(40_000, 7000, 0, 10, category="regional"),
        "Niche / Industry Boards": _fake_channel(
            18_000, 1500, 0, 2, category="niche_board"
        ),
        "Social Media": _fake_channel(4_000, 40, 0, 1, category="social"),
        "Employer Branding": _fake_channel(
            8_000, 50, 0, 1, category="employer_branding", role="brand"
        ),
    }
    be._redistribute_hires_by_conversion(
        allocs, total_hires=48, industry_avg_cph=4000.0
    )
    assert (
        allocs["Niche / Industry Boards"]["projected_hires"] > 0
    ), "Niche board with 1500 applications still modeled at 0 hires"


def test_brand_channels_always_zero_hires_after_redistribution():
    """Brand channels are reach/awareness spend by design -- they must
    never receive hires from the redistribution, regardless of dollars."""
    allocs = {
        "perf": _fake_channel(50_000, 5000, 0, 8, category="programmatic"),
        "brand": _fake_channel(
            50_000, 5000, 0, 5, category="employer_branding", role="brand"
        ),
    }
    be._redistribute_hires_by_conversion(
        allocs, total_hires=30, industry_avg_cph=4000.0
    )
    assert allocs["brand"]["projected_hires"] == 0
    assert allocs["perf"]["projected_hires"] == 30


def test_hire_split_ranks_by_conversion_table_not_flat_rate():
    """Two channels with identical application volume but different
    channel-type conversion rates must NOT get identical hire counts --
    the split must follow ``_HIRE_CONVERSION_RATES``, not a flat rate."""
    allocs = {
        # niche_board midpoint 0.125 vs social midpoint 0.045 -- same apps.
        "Niche": _fake_channel(20_000, 1000, 0, 5, category="niche_board"),
        "Social": _fake_channel(20_000, 1000, 0, 5, category="social"),
    }
    be._redistribute_hires_by_conversion(
        allocs, total_hires=50, industry_avg_cph=4000.0
    )
    assert allocs["Niche"]["projected_hires"] > allocs["Social"]["projected_hires"], (
        "Equal-apps niche and social channels split hires identically -- "
        "conversion-rate table isn't actually driving the split"
    )


def test_conversion_rate_table_matches_excel_v2_printed_table():
    """budget_engine's hire-distribution table must stay byte-identical to
    the table excel_v2 prints on the ROI Projections sheet -- otherwise the
    printed conversion table and the modeled hire split disagree again."""
    import excel_v2

    assert be._HIRE_CONVERSION_RATES == excel_v2._ROI_CONVERSION_RATES


# ---------------------------------------------------------------------------
# 2. _finalize_channel_ranking
# ---------------------------------------------------------------------------


def test_fit_score_is_monotonic_in_roi_score():
    allocs = {
        "best": _fake_channel(10_000, 500, 20, 10, category="programmatic"),
        "mid": _fake_channel(10_000, 500, 10, 6, category="regional"),
        "worst": _fake_channel(10_000, 500, 1, 2, category="social"),
    }
    be._finalize_channel_ranking(allocs)
    assert (
        allocs["best"]["fit_score"]
        > allocs["mid"]["fit_score"]
        > allocs["worst"]["fit_score"]
    )
    assert allocs["best"]["fit_rank"] == 1


def test_vetted_tier_and_fit_never_contradict_roi_ranking():
    """Regression for data:manpower#4 / data:atria#1: a channel that scores
    BEST on roi_score must never end up in a WORSE vetted_tier than a
    channel with a lower roi_score (the exact inversion that was shipped)."""
    allocs = {
        "high_roi": _fake_channel(10_000, 500, 20, 10, category="programmatic"),
        "low_roi": _fake_channel(10_000, 500, 1, 2, category="niche_board"),
    }
    be._finalize_channel_ranking(allocs)
    assert allocs["high_roi"]["fit_score"] >= allocs["low_roi"]["fit_score"]
    assert allocs["high_roi"]["fit_rank"] < allocs["low_roi"]["fit_rank"]


def test_brand_channels_get_their_own_labeled_tier():
    allocs = {
        "perf": _fake_channel(10_000, 500, 5, 5, category="regional"),
        "brand": _fake_channel(
            10_000, 50, 0, 1, category="employer_branding", role="brand"
        ),
    }
    be._finalize_channel_ranking(allocs)
    assert allocs["brand"]["vetted_tier"] == "Brand & Awareness"
    assert allocs["brand"]["fit_rank"] is None
    assert allocs["perf"]["vetted_tier"] != "Brand & Awareness"


# ---------------------------------------------------------------------------
# 3. _dedupe_shared_fallback_cpcs
# ---------------------------------------------------------------------------


def test_shared_fallback_cpc_is_differentiated_and_downgraded():
    """Regression for strategy:manpower#5: two unrelated channel categories
    both landing on the identical $11.47 CPC (from a shared upstream
    fallback) must be split into distinct, category-plausible values and
    marked confidence='estimated'."""
    allocs = {
        "Niche / Industry Boards": _fake_channel(
            18_000, 1500, 6, 8, category="niche_board", cpc_source="knowledge_base"
        ),
        "Employer Branding": _fake_channel(
            8_000,
            50,
            0,
            1,
            category="employer_branding",
            role="brand",
            cpc_source="knowledge_base",
        ),
    }
    allocs["Niche / Industry Boards"]["cpc"] = 11.47
    allocs["Employer Branding"]["cpc"] = 11.47
    for ch in allocs.values():
        ch["confidence"] = "high"

    be._dedupe_shared_fallback_cpcs(allocs)

    niche_cpc = allocs["Niche / Industry Boards"]["cpc"]
    brand_cpc = allocs["Employer Branding"]["cpc"]
    assert niche_cpc != brand_cpc, "shared fallback CPC not differentiated"
    assert allocs["Niche / Industry Boards"]["confidence"] == "estimated"
    assert allocs["Employer Branding"]["confidence"] == "estimated"


def test_dedupe_leaves_genuinely_shared_category_cpc_alone():
    """Two channels of the SAME category legitimately sharing a CPC (e.g.
    two regional boards) is not a defect -- must not be touched."""
    allocs = {
        "Regional A": _fake_channel(
            10_000, 500, 5, 5, category="regional", cpc_source="live_benchmark"
        ),
        "Regional B": _fake_channel(
            10_000, 500, 5, 5, category="regional", cpc_source="live_benchmark"
        ),
    }
    allocs["Regional A"]["cpc"] = 0.75
    allocs["Regional B"]["cpc"] = 0.75
    for ch in allocs.values():
        ch["confidence"] = "high"

    be._dedupe_shared_fallback_cpcs(allocs)

    assert allocs["Regional A"]["cpc"] == 0.75
    assert allocs["Regional B"]["cpc"] == 0.75
    assert allocs["Regional A"]["confidence"] == "high"


def test_dedupe_leaves_static_benchmark_source_alone():
    """The static benchmark table is already category-differentiated --
    two channels landing on it independently isn't a collision."""
    allocs = {
        "a": _fake_channel(
            10_000, 500, 5, 5, category="job_board", cpc_source="static_benchmark"
        ),
        "b": _fake_channel(
            10_000, 500, 5, 5, category="programmatic", cpc_source="static_benchmark"
        ),
    }
    allocs["a"]["cpc"] = 0.85
    allocs["b"]["cpc"] = 0.85
    for ch in allocs.values():
        ch["confidence"] = "low"

    be._dedupe_shared_fallback_cpcs(allocs)

    assert allocs["a"]["cpc"] == 0.85
    assert allocs["b"]["cpc"] == 0.85


# ---------------------------------------------------------------------------
# 4. End-to-end on brief-shaped fixtures (manpower / atria)
# ---------------------------------------------------------------------------


def test_manpower_niche_board_no_longer_zero_hires_end_to_end():
    result = _allocate_manpower()
    allocs = result["channel_allocations"]
    niche = allocs.get("Niche / Industry Boards")
    assert niche is not None
    assert (
        niche["projected_hires"] > 0
    ), "Niche board still projects 0 hires after the full allocation pipeline"


def test_atria_niche_board_no_longer_zero_hires_end_to_end():
    result = _allocate_atria()
    allocs = result["channel_allocations"]
    niche = allocs.get("Niche / Industry Boards")
    assert niche is not None
    assert niche["projected_hires"] > 0


def test_total_hires_unchanged_by_redistribution_end_to_end():
    """The CPH-benchmark-derived total must be identical whether or not
    the per-channel conversion-based redistribution ran -- only the split
    changes, never the total."""
    for allocate in (_allocate_manpower, _allocate_atria):
        result = allocate()
        channel_sum = sum(
            c.get("projected_hires") or 0
            for c in result["channel_allocations"].values()
        )
        assert channel_sum == result["total_projected"]["hires"]


def test_top_allocated_performance_channel_never_worst_roi():
    """Residual check for strategy:manpower#6: the largest-$ performance
    channel must never be the single worst-roi_score channel."""
    for allocate in (_allocate_manpower, _allocate_atria):
        result = allocate()
        allocs = result["channel_allocations"]
        performance = {
            name: ch for name, ch in allocs.items() if ch.get("channel_role") != "brand"
        }
        top_name = max(
            performance, key=lambda n: performance[n].get("dollar_amount") or 0
        )
        top_roi = performance[top_name].get("roi_score") or 0
        worst_roi = min(ch.get("roi_score") or 0 for ch in performance.values())
        assert top_roi > worst_roi, (
            f"{top_name} holds the largest allocation but ties the worst "
            f"roi_score ({worst_roi}) among performance channels"
        )


def test_metadata_channel_ranking_present_and_ordered_by_roi():
    for allocate in (_allocate_manpower, _allocate_atria):
        result = allocate()
        ranking = result["metadata"]["channel_ranking"]
        assert ranking, "channel_ranking metadata missing"
        ranked = [r for r in ranking if r["rank"] is not None]
        roi_scores = [r["roi_score"] for r in ranked]
        assert roi_scores == sorted(
            roi_scores, reverse=True
        ), "channel_ranking metadata is not sorted by roi_score descending"


def test_fit_score_and_vetted_tier_set_on_every_allocation_channel():
    for allocate in (_allocate_manpower, _allocate_atria):
        result = allocate()
        for name, ch in result["channel_allocations"].items():
            assert "fit_score" in ch, f"{name} missing fit_score"
            assert "vetted_tier" in ch, f"{name} missing vetted_tier"
            assert 0.0 <= ch["fit_score"] <= 1.0


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
