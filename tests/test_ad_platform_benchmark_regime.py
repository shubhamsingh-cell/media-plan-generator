"""Regression test locking in the 2026-07-17 ad-platform-benchmark regime decision.

fuse_ad_platform_analysis's _all_empty guard was a tautology from 2d89bacf
(2026-03-23) onward, so the 13-platform static _PLATFORM_BENCHMARKS table
silently became the always-on source for ad_platform_analysis, discarding a
per-role enriched path (5 platforms via _build_platform_entry /
_build_meta_platform_entry). The static table was made the deliberate,
unconditional canonical source (see the block comment above
_PLATFORM_BENCHMARKS in data_synthesizer.py) rather than restoring the
enriched path -- the S93 calibration and the 2026-07-16/17
CPC-reconciliation work both anchor on these 13 platforms.

This test pins that decision: feeding fuse_ad_platform_analysis rich,
non-empty per-platform API data still returns the same 13-platform static
table, proving the function no longer consults `enriched` at all. If a
future change reintroduces a live per-platform path, this should fail and
force a conscious update (along with test_platform_benchmark_fallback.py's
CPC pins).

Runs under pytest, or standalone:
``python3 tests/test_ad_platform_benchmark_regime.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import data_synthesizer  # noqa: E402

_EXPECTED_PLATFORM_KEYS = {
    "google_ads",
    "meta_facebook_instagram",
    "linkedin_ads",
    "microsoft_bing_ads",
    "tiktok_ads",
    "snapchat_ads",
    "x_twitter_ads",
    "programmatic_display_dsp",
    "roku_ctv_advertising",
    "spotify_audio_ads",
    "reddit_ads",
    "indeed_sponsored_jobs",
    "ziprecruiter_sponsored",
}

# Simulates what the now-deleted enriched-path builders would have consumed:
# live, non-zero CPC data for all five platforms the old guard was meant to
# gate on.
_RICH_ENRICHED_INPUT = {
    "google_ads_data": {
        "keywords": {
            "Software Engineer": {"avg_cpc_usd": 4.75, "avg_monthly_searches": 5000}
        },
    },
    "meta_ads_data": {
        "facebook": {"avg_cpc_usd": 2.10},
        "instagram": {"avg_cpc_usd": 1.95},
    },
    "bing_ads_data": {
        "keywords": {"Software Engineer": {"avg_cpc_usd": 3.20}},
    },
    "tiktok_ads_data": {
        "roles": {"Software Engineer": {"avg_cpc_usd": 2.50}},
    },
    "linkedin_ads_data": {
        "roles": {"Software Engineer": {"avg_cpc_usd": 6.80}},
    },
}


def _canonical_result() -> dict:
    return data_synthesizer.fuse_ad_platform_analysis({}, {}, {})


def _rich_result() -> dict:
    return data_synthesizer.fuse_ad_platform_analysis(_RICH_ENRICHED_INPUT, {}, {})


def test_static_table_is_the_only_regime_regardless_of_enriched_input():
    """Rich per-platform live data must not change the returned CPCs."""
    empty_result = _canonical_result()
    rich_result = _rich_result()

    for pk in ("google_ads", "meta_facebook_instagram", "linkedin_ads"):
        assert empty_result[pk]["avg_cpc"] == rich_result[pk]["avg_cpc"], (
            f"{pk} CPC changed with rich enriched input -- a live per-platform "
            "path may have been reintroduced without updating this pin"
        )
        # None of the live CPC values fed in above should leak through --
        # confirms `enriched` is fully unconsulted.
        assert rich_result[pk]["avg_cpc"] not in (4.75, 2.10, 1.95, 3.20, 2.50, 6.80)


def test_result_always_has_exactly_the_13_static_platforms():
    for result in (_canonical_result(), _rich_result()):
        platform_keys = {
            k for k in result if not k.startswith("_") and isinstance(result[k], dict)
        }
        assert platform_keys == _EXPECTED_PLATFORM_KEYS


def test_enriched_path_builders_were_deliberately_removed():
    """_build_platform_entry / _build_meta_platform_entry are dead code and
    were deleted, not merely made unreachable -- guards against silent
    resurrection of half-wired enrichment logic."""
    assert not hasattr(data_synthesizer, "_build_platform_entry")
    assert not hasattr(data_synthesizer, "_build_meta_platform_entry")


if __name__ == "__main__":
    test_static_table_is_the_only_regime_regardless_of_enriched_input()
    test_result_always_has_exactly_the_13_static_platforms()
    test_enriched_path_builders_were_deliberately_removed()
    print("all ad-platform-benchmark regime tests passed")
