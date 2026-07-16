"""Tests for app._compute_cpc_alerts (S46 real-time CPC/CPA drift monitor).

Background: ``_monitor_cpc_changes`` (a daemon thread started at import time)
compared ``data/channel_benchmarks_live.json`` against
``data/recruitment_benchmarks_comprehensive_2026.json`` every 6h and
published divergence alerts to the (currently unconsumed) ``/api/cpc-alerts``
endpoint. The comparison logic had TWO bugs from the day it was written,
making it a silent no-op:

  1. ``channel_benchmarks_live.json`` parses to
     ``{"data": [...], "_refreshed_at": ..., "_provenance": ...}`` -- NOT a
     flat ``{platform: {...}}`` mapping. The old code did
     ``for platform_key, platform_data in live_data.items(): ...`` directly
     on the raw dict, which yields the three top-level keys
     ("data"/"_refreshed_at"/"_provenance") as "platforms" -- none of which
     are per-platform dicts, so ``isinstance(platform_data, dict)`` is False
     for all three and the loop body never runs. Zero platforms compared,
     always.
  2. The KB lookup read a nonexistent ``platform_benchmarks`` top-level key.
     The KB's real path is
     ``A_cpa_cph_benchmarks_by_channel.cpc_by_platform.data``. The old code's
     ``.get("platform_benchmarks", {})`` silently returned ``{}``, so
     ``kb_cpc`` was always empty too.

Both bugs independently zero out the comparison; together the monitor never
alerted on anything, ever, regardless of how far live and KB data diverged.

The logic was extracted to the pure, testable ``app._compute_cpc_alerts``
function (no I/O, no globals) so these bugs can't regress silently again.

RED-FIRST PROOF (see the commit that adds this file for the actual command
output): at the pre-fix commit (336480d), ``app.py`` has no
``_compute_cpc_alerts`` function at all -- the comparison logic was inline
inside the ``_monitor_cpc_changes`` daemon loop, which cannot be unit-tested
in isolation (it sleeps, loops forever, and only reads real files off disk).
Running this exact test file against that commit fails every test with
``AttributeError: module 'app' has no attribute '_compute_cpc_alerts'``.
Separately, a 3-line snippet reproducing the OLD inline parsing against the
real file shapes demonstrates it yields zero platforms / an empty KB dict --
i.e. the monitor was provably a no-op, not just untested.

Runs under pytest, or standalone: ``python3 tests/test_cpc_monitor.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402


def _live_fixture() -> dict:
    """Real-shaped data/channel_benchmarks_live.json fixture."""
    return {
        "data": [
            {
                "channel": "indeed",
                "industry": "overall",
                "metadata": {
                    "cpc_range": {"min": 0.97, "max": 2.71, "currency": "USD"}
                },
            },
            {
                "channel": "linkedin",
                "industry": "overall",
                "metadata": {
                    "cpc_range": {"min": 1.50, "max": 4.50, "currency": "USD"}
                },
            },
            {
                # Subscription-only board: no cpc_range. Must be SKIPPED,
                # never synthesized into a $0 CPC.
                "channel": "ziprecruiter",
                "industry": "overall",
                "metadata": {"model": "Flat monthly subscription per job slot"},
            },
        ],
        "_refreshed_at": 1784186961.39,
        "_provenance": "test fixture: web-researched snapshot",
    }


def _kb_fixture() -> dict:
    """Real-shaped data/recruitment_benchmarks_comprehensive_2026.json fixture
    (only the branch _compute_cpc_alerts reads). Entries carry ONLY
    cpc_median_usd -- this exercises the median-fallback path; the real file's
    refreshed entries carry min/max bands and take the band-midpoint path
    (see the _kb_band_fixture tests below)."""
    return {
        "A_cpa_cph_benchmarks_by_channel": {
            "section_title": "CPA/CPH benchmarks by channel",
            "cpc_by_platform": {
                "data": [
                    {"platform": "Indeed", "cpc_median_usd": 0.92},
                    {"platform": "LinkedIn", "cpc_median_usd": 5.26},
                    # No cpc_median_usd/cpc_max_usd -- subscription product,
                    # correctly excluded from kb_cpc.
                    {"platform": "ZipRecruiter", "monthly_min_usd": 299},
                ]
            },
        }
    }


# ── divergence detection ─────────────────────────────────────────────────
def test_alerts_fire_for_greater_than_15pct_divergence():
    alerts = app._compute_cpc_alerts(
        _live_fixture(), _kb_fixture(), now="2026-07-16T00:00:00Z"
    )
    by_platform = {a["platform"]: a for a in alerts}

    assert "indeed" in by_platform, "expected an alert for indeed (+~100% vs KB)"
    assert "linkedin" in by_platform, "expected an alert for linkedin (-~43% vs KB)"
    # ziprecruiter has no cpc_range -> never compared, never alerted.
    assert "ziprecruiter" not in by_platform

    indeed = by_platform["indeed"]
    assert indeed["kb_value"] == 0.92
    assert indeed["live_value"] == 1.84  # (0.97 + 2.71) / 2
    assert indeed["direction"] == "up"
    assert indeed["change_pct"] > 15
    assert indeed["severity"] == "high"  # >= 30% threshold

    linkedin = by_platform["linkedin"]
    assert linkedin["kb_value"] == 5.26
    assert linkedin["live_value"] == 3.0  # (1.50 + 4.50) / 2
    assert linkedin["direction"] == "down"
    assert abs(linkedin["change_pct"]) > 15
    assert linkedin["severity"] == "high"


def test_alert_baseline_label_names_kb_vintage_honestly():
    """Each alert must make the KB baseline's static vintage obvious to any
    future consumer of /api/cpc-alerts (currently unconsumed)."""
    alerts = app._compute_cpc_alerts(
        _live_fixture(), _kb_fixture(), now="2026-07-16T00:00:00Z"
    )
    assert alerts, "fixture is designed to produce alerts"
    for a in alerts:
        assert "KB median" in a["baseline_label"]
        assert "static 2026 KB file" in a["baseline_label"]
        assert "KB median" in a["message"]
        assert a["timestamp"] == "2026-07-16T00:00:00Z"


def test_below_threshold_divergence_does_not_alert():
    live = {
        "data": [
            {
                "channel": "indeed",
                "metadata": {"cpc_range": {"min": 0.90, "max": 0.94}},  # mid 0.92 == KB
            },
        ]
    }
    alerts = app._compute_cpc_alerts(live, _kb_fixture(), now="2026-07-16T00:00:00Z")
    assert alerts == []


def test_board_without_cpc_range_is_skipped_not_treated_as_zero():
    live = {"data": [{"channel": "ziprecruiter", "metadata": {}}]}
    alerts = app._compute_cpc_alerts(live, _kb_fixture(), now="2026-07-16T00:00:00Z")
    assert alerts == []


def test_null_cpc_range_values_do_not_raise_typeerror():
    """A present-but-null cpc_range (min/max: null) must be skipped cleanly,
    not raise -- this is the shape that a naive `.get(key, 0.0)` fallback
    mishandles (default only fires on an ABSENT key, not a present null)."""
    live = {
        "data": [
            {
                "channel": "indeed",
                "metadata": {"cpc_range": {"min": None, "max": None}},
            },
        ]
    }
    alerts = app._compute_cpc_alerts(live, _kb_fixture(), now="2026-07-16T00:00:00Z")
    assert alerts == []


def test_empty_kb_data_yields_no_alerts_no_exception():
    alerts = app._compute_cpc_alerts(_live_fixture(), {}, now="2026-07-16T00:00:00Z")
    assert alerts == []


# ── like-for-like band comparison (2026-07-16 KB reconciliation) ─────────
def _kb_band_fixture() -> dict:
    """KB entry shaped like the reconciled real file: a cited min/max band
    plus a geometric-mean cpc_median_usd that deliberately differs from the
    band midpoint (Indeed: midpoint 1.84, geomean median 1.62)."""
    return {
        "A_cpa_cph_benchmarks_by_channel": {
            "cpc_by_platform": {
                "data": [
                    {
                        "platform": "Indeed",
                        "cpc_min_usd": 0.97,
                        "cpc_max_usd": 2.71,
                        "cpc_median_usd": 1.62,
                    },
                ]
            }
        }
    }


def test_kb_band_entry_compares_midpoint_to_midpoint_not_median():
    """When live and KB describe the SAME cited band, no alert may fire.
    This is the reconciled LinkedIn shape: live midpoint 3.00 vs the
    geometric-mean median 2.60 is +15.4% -- above the 15% threshold, so a
    median-first comparison would alert forever on identical bands. The
    band-midpoint path reports exactly 0%. This test FAILS if the monitor
    reverts to median-first for band-carrying entries."""
    kb = {
        "A_cpa_cph_benchmarks_by_channel": {
            "cpc_by_platform": {
                "data": [
                    {
                        "platform": "LinkedIn",
                        "cpc_min_usd": 1.50,
                        "cpc_max_usd": 4.50,
                        "cpc_median_usd": 2.60,
                    },
                ]
            }
        }
    }
    live = {
        "data": [
            {
                "channel": "linkedin",
                "metadata": {"cpc_range": {"min": 1.50, "max": 4.50}},
            },
        ]
    }
    alerts = app._compute_cpc_alerts(live, kb, now="2026-07-16T00:00:00Z")
    assert alerts == []


def test_kb_band_real_drift_still_alerts_with_band_label():
    live = {
        "data": [
            {
                "channel": "indeed",
                "metadata": {"cpc_range": {"min": 3.50, "max": 4.50}},
            },
        ]
    }
    alerts = app._compute_cpc_alerts(
        live, _kb_band_fixture(), now="2026-07-16T00:00:00Z"
    )
    assert len(alerts) == 1
    a = alerts[0]
    assert a["kb_value"] == 1.84  # (0.97 + 2.71) / 2 -- midpoint, NOT the 1.62 median
    assert a["live_value"] == 4.0
    assert a["direction"] == "up"
    assert a["severity"] == "high"
    assert "KB band midpoint" in a["baseline_label"]
    assert "static 2026 KB file" in a["baseline_label"]
    assert "KB band midpoint" in a["message"]


def test_reconciled_repo_data_files_produce_no_artifact_alerts():
    """Commit-time invariant: the repo's real KB cpc_by_platform and the real
    channel_benchmarks_seed.json (the live file's source of truth) must agree
    within the 15% alert threshold. If a future session refreshes the seed
    without reconciling the KB (or vice versa), this fails -- refresh both
    from the same cited research, or consciously re-baseline this test.
    Assumption: the seed is a faithful proxy for the runtime
    channel_benchmarks_live.json the monitor actually reads (data_seeds
    copies seed -> live); if a live refresh ever sources CPCs from anything
    other than the seed, the monitor can fire in prod without this
    commit-time check noticing."""
    import json

    root = Path(__file__).resolve().parent.parent
    with open(root / "data" / "channel_benchmarks_seed.json", encoding="utf-8") as f:
        seed = json.load(f)
    with open(
        root / "data" / "recruitment_benchmarks_comprehensive_2026.json",
        encoding="utf-8",
    ) as f:
        kb = json.load(f)
    alerts = app._compute_cpc_alerts(seed, kb, now="2026-07-16T00:00:00Z")
    assert (
        alerts == []
    ), "seed vs KB artifact alerts -- reconcile the data files: " + repr(alerts)


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
