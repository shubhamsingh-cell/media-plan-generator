"""Pruning old KB backups must tolerate a file removed mid-prune.

Production warned "Failed to prune backup kb_backup_*.zip: [Errno 2] No such
file or directory": another worker/run deleted the file between the directory
listing and ``unlink``. A vanished file is already pruned, not a failure.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import backup_kb  # noqa: E402


def _make_backups(tmp_path: Path, count: int) -> list[Path]:
    files = []
    for i in range(count):
        f = tmp_path / f"kb_backup_2026010{i}_000000.zip"
        f.write_bytes(b"x")
        os.utime(f, (1_700_000_000 + i * 100, 1_700_000_000 + i * 100))
        files.append(f)
    return files


def test_unlink_race_is_debug_not_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    files = _make_backups(tmp_path, backup_kb._MAX_BACKUPS + 2)
    oldest = files[0]
    real_unlink = Path.unlink

    def racing_unlink(self: Path, *a, **kw):  # another worker wins the race
        if self == oldest:
            real_unlink(self)
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real_unlink(self, *a, **kw)

    monkeypatch.setattr(Path, "unlink", racing_unlink)
    with caplog.at_level(logging.DEBUG, logger=backup_kb.logger.name):
        backup_kb._prune_old_backups(tmp_path)  # must not raise

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
    remaining = sorted(p.name for p in tmp_path.glob("kb_backup_*.zip"))
    assert remaining == sorted(f.name for f in files[2:])  # the 2 oldest are gone
    assert any(
        r.levelno == logging.DEBUG and oldest.name in r.getMessage()
        for r in caplog.records
    )


def test_prune_continues_past_the_vanished_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = _make_backups(tmp_path, backup_kb._MAX_BACKUPS + 3)
    first = files[0]
    real_unlink = Path.unlink

    def racing_unlink(self: Path, *a, **kw):
        if self == first:
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real_unlink(self, *a, **kw)

    monkeypatch.setattr(Path, "unlink", racing_unlink)
    backup_kb._prune_old_backups(tmp_path)
    # files[1], files[2] were still pruned after the race on files[0]
    assert not files[1].exists() and not files[2].exists()


def test_stat_race_during_listing_is_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    files = _make_backups(tmp_path, backup_kb._MAX_BACKUPS + 1)
    gone = files[3]
    real_stat = Path.stat

    def racing_stat(self: Path, *a, **kw):
        if self == gone:
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real_stat(self, *a, **kw)

    monkeypatch.setattr(Path, "stat", racing_stat)
    with caplog.at_level(logging.DEBUG, logger=backup_kb.logger.name):
        backup_kb._prune_old_backups(tmp_path)  # must not raise
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_other_oserror_still_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _make_backups(tmp_path, backup_kb._MAX_BACKUPS + 1)

    def denied(self: Path, *a, **kw):
        raise PermissionError(13, "Permission denied", str(self))

    monkeypatch.setattr(Path, "unlink", denied)
    with caplog.at_level(logging.DEBUG, logger=backup_kb.logger.name):
        backup_kb._prune_old_backups(tmp_path)
    assert any(r.levelno == logging.WARNING for r in caplog.records)
