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
    5. Atomic copy -- an interrupted copy must never leave a truncated live
       file behind (skip-if-exists would never repair it on a later deploy).
"""

from __future__ import annotations

import json
import shutil
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


def test_interrupted_copy_leaves_no_truncated_live_file(
    tmp_data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash mid-copy (OOM kill, deploy restart) must never leave a
    truncated live file behind: seeding is skip-if-exists, so a truncated
    live file would silently never be repaired by any later deploy of that
    instance -- dropping the live tier forever, not just for one run.

    Simulates the interruption by making the byte-copy loop write a partial
    chunk and then raise, the same shape of failure a real SIGKILL produces
    mid-copy. The old ``shutil.copyfile(seed_path, live_path)`` implementation
    opened ``live_path`` directly for writing, so an interruption left a
    truncated file at the live filename. The current implementation copies
    into a temp file in the same directory and only ``os.replace()``s it
    into place on success, so the live filename must never exist (and no
    stray temp file may be left behind) when the copy is interrupted.
    """
    seed_path = tmp_data_dir / "channel_benchmarks_seed.json"
    live_path = tmp_data_dir / "channel_benchmarks_live.json"
    seed_path.write_bytes(b'{"data": [{"channel": "indeed"}]}' * 200)

    def _flaky_copyfileobj(fsrc, fdst, length=shutil.COPY_BUFSIZE):
        fdst.write(fsrc.read(16))  # a partial chunk reaches disk...
        raise OSError("simulated interruption mid-copy")  # ...then we die

    monkeypatch.setattr(data_seeds.shutil, "copyfileobj", _flaky_copyfileobj)

    seeded = data_seeds.seed_runtime_data_files()

    assert seeded == []
    assert not live_path.exists(), (
        "an interrupted copy left a truncated live file behind -- "
        "skip-if-exists will never repair it on a later deploy"
    )
    leftovers = [p for p in tmp_data_dir.iterdir() if p.name != seed_path.name]
    assert leftovers == [], f"stray temp file(s) left behind after failure: {leftovers}"
