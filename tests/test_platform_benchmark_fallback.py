"""Unified-layer serving invariants for data_synthesizer's _PLATFORM_BENCHMARKS.

fuse_ad_platform_analysis's 13-platform table is the deliberate canonical
source for ad_platform_analysis (2026-07-17 regime decision -- see the block
comment in data_synthesizer.py and tests/test_ad_platform_benchmark_regime.py).
Since 2026-07-21 the CPCs of the platforms the benchmark registry covers on
the same job-ads basis are OVERLAID from
``benchmark_registry.get_channel_benchmark()`` at serve time, so registry
refreshes and the data/live_market_data.json live overlay propagate into
every plan automatically instead of requiring hand-edits to the table.

These tests pin that serving invariant by CALLING both real paths (the
public fuse function and the registry getter) and, to stay non-vacuous when
the values happen to match anyway, by injecting a sentinel CPC into the
registry and proving it propagates into the served section.

LinkedIn stays pinned to the job-ads/Promoted-Jobs basis (cited 2.60); the
retired 5.26 blended sponsored-content CPC into a job-ads figure and must
not reappear.

Runs under pytest, or standalone:
``python3 tests/test_platform_benchmark_fallback.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import benchmark_registry  # noqa: E402
import data_synthesizer  # noqa: E402

# Table platform name -> (result key after platform_key normalization,
#                         registry key serving its CPC at serve time)
# Mirrors data_synthesizer's _REGISTRY_CPC_KEYS. "X (Twitter) Ads" is
# deliberately absent: its 1.35 (table) and 2.00 (registry) are different
# uncited vintages -- neither may overwrite the other.
_LIKE_FOR_LIKE = {
    "Google Ads": ("google_ads", "google_ads"),
    "Meta (Facebook/Instagram)": ("meta_facebook_instagram", "meta_facebook"),
    "LinkedIn Ads": ("linkedin_ads", "linkedin"),
    "TikTok Ads": ("tiktok_ads", "tiktok"),
    "Programmatic Display (DSP)": ("programmatic_display_dsp", "programmatic"),
    "Indeed Sponsored Jobs": ("indeed_sponsored_jobs", "indeed"),
    "ZipRecruiter Sponsored": ("ziprecruiter_sponsored", "ziprecruiter"),
}


def _served_result() -> dict:
    return data_synthesizer.fuse_ad_platform_analysis({}, {}, {})


def test_served_cpcs_match_registry_getter():
    """Every mapped platform's served CPC equals the live-overlaid getter."""
    result = _served_result()
    for pname, (result_key, registry_key) in _LIKE_FOR_LIKE.items():
        entry = result.get(result_key)
        assert isinstance(entry, dict), f"missing served entry {result_key}"
        getter_cpc = benchmark_registry.get_channel_benchmark(registry_key)["cpc"]
        assert entry["avg_cpc"] == getter_cpc, (
            f"{pname} served CPC {entry['avg_cpc']} != "
            f"get_channel_benchmark({registry_key!r}) CPC {getter_cpc}; "
            "the serve-time overlay in fuse_ad_platform_analysis is broken "
            "or the mapping drifted"
        )


def test_registry_refresh_propagates_into_served_section():
    """Non-vacuous proof: a registry CPC change must reach the served plan.

    Uses tiktok because it has no data/live_market_data.json entry, so the
    getter serves the static value and the sentinel is guaranteed to win.
    """
    original = benchmark_registry.CHANNEL_BENCHMARKS["tiktok"]["cpc"]
    sentinel = 9.87
    try:
        benchmark_registry.CHANNEL_BENCHMARKS["tiktok"]["cpc"] = sentinel
        result = _served_result()
        assert result["tiktok_ads"]["avg_cpc"] == sentinel, (
            "registry CPC change did not propagate into ad_platform_analysis; "
            "the serve-time overlay is disconnected"
        )
    finally:
        benchmark_registry.CHANNEL_BENCHMARKS["tiktok"]["cpc"] = original
    # And the restore must propagate back too.
    assert _served_result()["tiktok_ads"]["avg_cpc"] == original


def test_linkedin_uses_job_ads_basis_not_sponsored_content():
    """5.26 (sponsored-content blend) must not reappear on the LinkedIn entry."""
    result = _served_result()
    linkedin = result["linkedin_ads"]
    assert linkedin["avg_cpc"] == 2.60
    assert linkedin["avg_cpc"] != 5.26


def test_entries_are_labeled_and_flagged():
    result = _served_result()
    for result_key, _ in _LIKE_FOR_LIKE.values():
        entry = result[result_key]
        assert entry["source"] == "Industry Benchmark (2024-2026)"
        assert entry["_meta"]["fallback"] is True


if __name__ == "__main__":
    test_served_cpcs_match_registry_getter()
    test_registry_refresh_propagates_into_served_section()
    test_linkedin_uses_job_ads_basis_not_sponsored_content()
    test_entries_are_labeled_and_flagged()
    print("all platform-benchmark serving-invariant tests passed")
