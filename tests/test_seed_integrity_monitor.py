"""Unit tests for ``data_matrix_monitor._check_seed_integrity`` -- the
data-matrix health check for the three seeded runtime data files.

Background: ``data_seeds.py`` (since 38ac22a/a0a9912) copies tracked
``*_seed.json`` snapshots onto three gitignored live filenames the first
time each is absent at app import -- channel_benchmarks_live.json,
job_posting_volumes.json, google_trends.json -- because prod has no runtime
writer for them (refresh daemon disabled, S50; data_enrichment env-gated off
by default). These files are now critical plan inputs (CPC live_benchmark
tier) with no self-heal path: data_enrichment.run_cycle cannot rewrite them.

The Opus review of a0a9912 flagged that data_matrix_monitor.py doesn't watch
channel_benchmarks_live.json at all, and that the module's existing
``_check_cache_staleness`` has the wrong semantics for seeded files anyway
(mtime-freshness thresholds + self-heal via data_enrichment.run_cycle --
enrichment can't rewrite these files, and mtime staleness would false-alarm
on a long-lived instance that seeded once at startup and never again).
``_check_seed_integrity`` closes that gap with presence + parse + non-empty
checks only -- no freshness/mtime logic, no self-heal hook.

Test groups (mirrors tests/test_data_seeds.py's tmp_data_dir pattern):
    (a) all three seeded live files intact -> ok
    (b) one file absent -> fires (status error)
    (c) truncated/invalid JSON -> fires
    (d) empty payload (both the channel_benchmarks "data" list variant and
        the generic top-level-dict variant) -> fires
    (e) data_seeds unimportable -> warning, not a crash
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import data_matrix_monitor  # noqa: E402
from data_matrix_monitor import DataMatrixMonitor  # noqa: E402

CHANNEL_BENCH_LIVE = "channel_benchmarks_live.json"
JOB_POSTING_LIVE = "job_posting_volumes.json"
GOOGLE_TRENDS_LIVE = "google_trends.json"


def _write_valid_seed_files(data_dir: Path) -> None:
    """Write minimal-but-shape-accurate versions of all three live files
    (shapes taken from `git show` of the tracked *_seed.json snapshots)."""
    (data_dir / CHANNEL_BENCH_LIVE).write_text(
        json.dumps(
            {
                "data": [{"channel": "indeed", "industry": "overall", "cpc": 1.5}],
                "_refreshed_at": "2026-07-16T00:00:00+00:00",
                "_provenance": "seed",
            }
        ),
        encoding="utf-8",
    )
    (data_dir / JOB_POSTING_LIVE).write_text(
        json.dumps(
            {
                "scraped_at": "2026-06-02T19:15:10+00:00",
                "volumes": {
                    "Software Engineer": {
                        "role": "Software Engineer",
                        "estimated_openings": 145000,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (data_dir / GOOGLE_TRENDS_LIVE).write_text(
        json.dumps(
            {
                "data": {
                    "roles": {
                        "Software Engineer": {
                            "current_interest": 43,
                            "trend_direction": "declining",
                        }
                    }
                },
                "_refreshed_at": 1752000000.0,
                "_refreshed_iso": "2026-07-16T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture()
def tmp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point data_matrix_monitor at an isolated tmp dir so tests never touch
    the real data/ directory."""
    monkeypatch.setattr(data_matrix_monitor, "DATA_DIR", tmp_path)
    return tmp_path


@pytest.fixture()
def monitor() -> DataMatrixMonitor:
    return DataMatrixMonitor()


# ── (a) all three intact -> ok ───────────────────────────────────────────────


def test_all_three_intact_is_ok(tmp_data_dir: Path, monitor: DataMatrixMonitor) -> None:
    _write_valid_seed_files(tmp_data_dir)

    result = monitor._check_seed_integrity()

    assert result["status"] == "ok"
    assert result["failures"] == []
    assert result["healed"] is False
    assert result["name"] == "seed_integrity"


# ── (b) one file absent -> fires ─────────────────────────────────────────────


def test_one_file_absent_fires(tmp_data_dir: Path, monitor: DataMatrixMonitor) -> None:
    _write_valid_seed_files(tmp_data_dir)
    (tmp_data_dir / JOB_POSTING_LIVE).unlink()

    result = monitor._check_seed_integrity()

    assert result["status"] == "error"
    assert any(
        "missing" in f and JOB_POSTING_LIVE in f for f in result["failures"]
    ), result["failures"]


# ── (c) truncated/invalid JSON -> fires ──────────────────────────────────────


def test_truncated_json_fires(tmp_data_dir: Path, monitor: DataMatrixMonitor) -> None:
    _write_valid_seed_files(tmp_data_dir)
    # Simulates an interrupted write: valid opening bytes, no closing braces.
    (tmp_data_dir / CHANNEL_BENCH_LIVE).write_text(
        '{"data": [{"channel": "in', encoding="utf-8"
    )

    result = monitor._check_seed_integrity()

    assert result["status"] == "error"
    assert any(
        "invalid JSON" in f and CHANNEL_BENCH_LIVE in f for f in result["failures"]
    ), result["failures"]


# ── (d) empty payload -> fires ───────────────────────────────────────────────


def test_empty_data_list_fires(tmp_data_dir: Path, monitor: DataMatrixMonitor) -> None:
    """channel_benchmarks_live.json branch: top-level dict present but its
    "data" list is empty -- the shape-specific check must catch this even
    though the dict itself is non-empty (has _refreshed_at/_provenance)."""
    _write_valid_seed_files(tmp_data_dir)
    (tmp_data_dir / CHANNEL_BENCH_LIVE).write_text(
        json.dumps({"data": [], "_refreshed_at": "x", "_provenance": "seed"}),
        encoding="utf-8",
    )

    result = monitor._check_seed_integrity()

    assert result["status"] == "error"
    assert any(
        "empty payload" in f and CHANNEL_BENCH_LIVE in f for f in result["failures"]
    ), result["failures"]


def test_empty_top_level_dict_fires(
    tmp_data_dir: Path, monitor: DataMatrixMonitor
) -> None:
    """job_posting_volumes.json / google_trends.json branch: generic
    non-empty-top-level-container check (shape-tolerant, no nested-key
    knowledge required)."""
    _write_valid_seed_files(tmp_data_dir)
    (tmp_data_dir / JOB_POSTING_LIVE).write_text("{}", encoding="utf-8")

    result = monitor._check_seed_integrity()

    assert result["status"] == "error"
    assert any(
        "empty payload" in f and JOB_POSTING_LIVE in f for f in result["failures"]
    ), result["failures"]


# ── (e) data_seeds unimportable -> warning, not a crash ──────────────────────


def test_data_seeds_unimportable_warns_not_crashes(
    tmp_data_dir: Path, monitor: DataMatrixMonitor, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_valid_seed_files(tmp_data_dir)
    # sys.modules[name] = None is the documented way to force the next
    # `import`/`from ... import` of that name to raise ImportError.
    monkeypatch.setitem(sys.modules, "data_seeds", None)

    result = monitor._check_seed_integrity()  # must not raise

    assert result["status"] == "warning"
    assert "data_seeds" in result["detail"]
    assert result["healed"] is False


# ── Wiring: registered in the extended-health aggregation ───────────────────


def test_seed_integrity_registered_in_extended_health(
    tmp_data_dir: Path, monitor: DataMatrixMonitor
) -> None:
    _write_valid_seed_files(tmp_data_dir)

    extended = monitor._probe_extended_health()

    assert "seed_integrity" in extended
    assert extended["seed_integrity"]["status"] == "ok"


def test_filenames_derived_from_data_seeds_seed_pairs(
    tmp_data_dir: Path, monitor: DataMatrixMonitor
) -> None:
    """The check must not hardcode a second filename list -- it derives live
    filenames from data_seeds._SEED_PAIRS (single source of truth)."""
    from data_seeds import _SEED_PAIRS

    _write_valid_seed_files(tmp_data_dir)
    live_names = {live for _seed, live in _SEED_PAIRS}
    assert live_names == {CHANNEL_BENCH_LIVE, JOB_POSTING_LIVE, GOOGLE_TRENDS_LIVE}

    result = monitor._check_seed_integrity()
    assert result["status"] == "ok"
