"""Like-for-like pins for data_synthesizer's _PLATFORM_BENCHMARKS fallback.

fuse_ad_platform_analysis carries a hardcoded 13-platform benchmark table
(intended as an emergency fallback; currently the always-on source because
2d89bacf's mass `.get(x, 0) == 0` -> `.get(x) or 0 == 0` conversion turned
the _all_empty guard into a tautology -- see the WARNING comment above the
guard in data_synthesizer.py).

These tests keep the fallback's Google/Meta/LinkedIn CPCs equal to
benchmark_registry.CHANNEL_BENCHMARKS -- the repo's cited, refreshed source
(LinkedIn 2.60 job-ads/Promoted-Jobs basis per the July-2026 research; the
retired 5.26 blended sponsored-content CPC into a job-ads figure). If the
registry refreshes again, these fail and point straight at the dict to update.

The assertions exercise the fallback through the public function with fully
empty inputs, which triggers the fallback under both the current (broken,
always-on) guard and the originally intended all-empty semantics -- so they
survive a future guard fix.

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

# Fallback dict name -> (result key after platform_key normalization,
#                        CHANNEL_BENCHMARKS key with the cited CPC)
_LIKE_FOR_LIKE = {
    "Google Ads": ("google_ads", "google_ads"),
    "Meta (Facebook/Instagram)": ("meta_facebook_instagram", "meta_facebook"),
    "LinkedIn Ads": ("linkedin_ads", "linkedin"),
}


def _fallback_result() -> dict:
    return data_synthesizer.fuse_ad_platform_analysis({}, {}, {})


def test_fallback_cpcs_match_benchmark_registry():
    result = _fallback_result()
    for pname, (result_key, registry_key) in _LIKE_FOR_LIKE.items():
        entry = result.get(result_key)
        assert isinstance(entry, dict), f"missing fallback entry {result_key}"
        registry_cpc = benchmark_registry.CHANNEL_BENCHMARKS[registry_key]["cpc"]
        assert entry["avg_cpc"] == registry_cpc, (
            f"{pname} fallback CPC {entry['avg_cpc']} drifted from "
            f"CHANNEL_BENCHMARKS[{registry_key!r}] CPC {registry_cpc}; "
            "update data_synthesizer._PLATFORM_BENCHMARKS with the cited figure"
        )


def test_fallback_linkedin_uses_job_ads_basis_not_sponsored_content():
    """5.26 (sponsored-content blend) must not reappear on the LinkedIn entry."""
    result = _fallback_result()
    linkedin = result["linkedin_ads"]
    assert linkedin["avg_cpc"] == 2.60
    assert linkedin["avg_cpc"] != 5.26


def test_fallback_entries_are_labeled_and_flagged():
    result = _fallback_result()
    for result_key, _ in _LIKE_FOR_LIKE.values():
        entry = result[result_key]
        assert entry["source"] == "Industry Benchmark (2024-2026)"
        assert entry["_meta"]["fallback"] is True


if __name__ == "__main__":
    test_fallback_cpcs_match_benchmark_registry()
    test_fallback_linkedin_uses_job_ads_basis_not_sponsored_content()
    test_fallback_entries_are_labeled_and_flagged()
    print("all platform-benchmark fallback tests passed")
