"""Unit tests for ``data_seeds`` -- runtime data file seeding.

Background: commits b74be8d and cd30a3d untracked+gitignored
channel_benchmarks_live.json, job_posting_volumes.json, and
google_trends.json. Prod's wsgi startup path has the data-refresh pipeline
disabled ("[DISABLED S50]") and data_enrichment is env-gated off by default,
so these files have no runtime writer on Render -- the tracked *_seed.json
copies are the only delivery path. ``data_seeds.seed_runtime_data_files()``
copies each seed onto its live filename the first time the live file is
absent, and must never clobber a live file that already exists.

Test groups:
    1. Seeding -- live file absent + seed present -> copied, byte-identical.
    2. Never-overwrite -- live file already present -> left untouched.
    3. Tracked seed content -- the three real seed files under data/ parse as
       JSON and channel_benchmarks_seed.json has the expected indeed/linkedin
       entries.
    4. Missing seed -- no crash, and the live filename is not reported as
       seeded.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import data_seeds  # noqa: E402


@pytest.fixture()
def tmp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point data_seeds at an isolated tmp dir so tests never touch data/."""
    monkeypatch.setattr(data_seeds, "_DATA_DIR", tmp_path)
    return tmp_path


def test_seeds_when_live_file_absent(tmp_data_dir: Path) -> None:
    seed_path = tmp_data_dir / "channel_benchmarks_seed.json"
    live_path = tmp_data_dir / "channel_benchmarks_live.json"
    seed_content = '{"data": [{"channel": "indeed"}]}'
    seed_path.write_text(seed_content, encoding="utf-8")

    seeded = data_seeds.seed_runtime_data_files()

    assert "channel_benchmarks_live.json" in seeded
    assert live_path.exists()
    assert live_path.read_text(encoding="utf-8") == seed_content


def test_never_overwrites_existing_live_file(tmp_data_dir: Path) -> None:
    seed_path = tmp_data_dir / "job_posting_volumes_seed.json"
    live_path = tmp_data_dir / "job_posting_volumes.json"
    seed_path.write_text('{"data": "seed-content"}', encoding="utf-8")
    sentinel = '{"data": "already-here-do-not-touch"}'
    live_path.write_text(sentinel, encoding="utf-8")

    seeded = data_seeds.seed_runtime_data_files()

    assert "job_posting_volumes.json" not in seeded
    assert live_path.read_text(encoding="utf-8") == sentinel


def test_missing_seed_file_does_not_crash(tmp_data_dir: Path) -> None:
    # No seed files written at all -- tmp_data_dir is empty.
    seeded = data_seeds.seed_runtime_data_files()

    assert seeded == []
    assert not (tmp_data_dir / "google_trends.json").exists()


@pytest.mark.parametrize(
    "seed_name",
    [
        "channel_benchmarks_seed.json",
        "job_posting_volumes_seed.json",
        "google_trends_seed.json",
    ],
)
def test_tracked_seed_file_parses_as_json(seed_name: str) -> None:
    path = PROJECT_ROOT / "data" / seed_name
    assert path.exists(), f"tracked seed file missing: {path}"
    with open(path, "r", encoding="utf-8") as f:
        json.load(f)  # must not raise


def test_channel_benchmarks_seed_has_indeed_and_linkedin() -> None:
    path = PROJECT_ROOT / "data" / "channel_benchmarks_seed.json"
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    assert isinstance(raw.get("data"), list)
    channels = {(entry.get("channel") or "").lower() for entry in raw["data"]}
    assert "indeed" in channels
    assert "linkedin" in channels
