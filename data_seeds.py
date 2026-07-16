"""
Runtime data file seeding -- restores gitignored data/ files that prod never
regenerates on its own.

Background: commits b74be8d and cd30a3d untracked+gitignored three files that
budget_engine.py, market_signals.py, channel_recommender.py, market_pulse.py,
and app.py's CPC monitor all read from disk: channel_benchmarks_live.json,
job_posting_volumes.json, and google_trends.json. On Render, wsgi.py's
deferred startup has the data-refresh pipeline disabled ("[DISABLED S50]")
and data_enrichment is env-gated off by default (ENABLE_DATA_ENRICHMENT unset),
so nothing on prod ever writes these files -- git was their only delivery path.
Since b74be8d, every prod deploy has started with these files absent.

This module copies last-tracked snapshots (checked in as *_seed.json, which
are NOT gitignored) onto the live filenames the first time the app starts
with no live file present. It never overwrites a live file that already
exists (e.g. one written by a local enrichment run), and it never raises --
a missing or unreadable seed should degrade to "no seed applied", not a
startup crash.
"""

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).parent / "data"

# (seed filename, live filename) pairs. The live filenames are gitignored
# (data/.gitignore) and have no runtime writer in production; the seed
# filenames are tracked copies of the last-known-good content.
_SEED_PAIRS: List[Tuple[str, str]] = [
    ("channel_benchmarks_seed.json", "channel_benchmarks_live.json"),
    ("job_posting_volumes_seed.json", "job_posting_volumes.json"),
    ("google_trends_seed.json", "google_trends.json"),
]


def _atomic_copy(src: Path, dst: Path) -> None:
    """Copy ``src`` onto ``dst`` atomically.

    ``shutil.copyfile`` writes directly into ``dst``; a process kill (OOM,
    deploy restart) mid-copy leaves a truncated ``dst`` on disk. Because
    ``seed_runtime_data_files`` is skip-if-exists, that truncated file is
    never repaired by a later deploy -- it silently drops the live tier
    forever on that instance. Instead, write into a temp file in the same
    directory (same filesystem, so the replace below is atomic) and swap it
    into place with ``os.replace``, which is atomic on POSIX: readers only
    ever see either no ``dst`` or a fully-written one, never a partial one.
    Raises ``OSError`` on any failure (temp-file creation, copy, or
    replace), matching what ``shutil.copyfile`` would have raised, so the
    caller's existing ``except OSError`` handling is unchanged.
    """
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{dst.name}.", suffix=".tmp", dir=str(dst.parent)
    )
    try:
        with os.fdopen(fd, "wb") as tmp_f, open(src, "rb") as src_f:
            shutil.copyfileobj(src_f, tmp_f)
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, dst)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def seed_runtime_data_files() -> List[str]:
    """Copy tracked seed files onto their live filenames when the live file
    is absent.

    Returns the list of live filenames actually seeded this call. Never
    overwrites an existing live file and never raises -- any per-file
    failure is logged and skipped.
    """
    seeded: List[str] = []
    for seed_name, live_name in _SEED_PAIRS:
        seed_path = _DATA_DIR / seed_name
        live_path = _DATA_DIR / live_name
        if live_path.exists():
            continue
        if not seed_path.exists():
            continue
        try:
            _atomic_copy(seed_path, live_path)
        except OSError as exc:
            logger.warning(
                "data_seeds: failed to seed %s from %s: %s", live_name, seed_name, exc
            )
            continue
        logger.info("data_seeds: seeded %s from %s", live_name, seed_name)
        seeded.append(live_name)
    return seeded
