"""Concurrent KB backups must not collide.

The scheduler's jitter is computed once in the gunicorn master and the timer is
re-run in every forked worker, so several workers can fire the same backup in
the same second. They used to open the same ``kb_backup_<second>.zip`` for write
(and a failing one unlinked its siblings' archive).
"""

from __future__ import annotations

import os
import sys
import threading
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import backup_kb  # noqa: E402

pytestmark = pytest.mark.skipif(
    backup_kb.fcntl is None, reason="flock needs a POSIX platform"
)


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "channels_db.json").write_text('{"a": 1}')
    return d


def test_second_backup_while_one_runs_is_skipped_and_leaves_the_archive(
    data_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inside = threading.Event()
    release = threading.Event()
    real_write = zipfile.ZipFile.write

    def slow_write(self, *a, **kw):  # first backup stalls mid-archive
        inside.set()
        release.wait(timeout=10)
        return real_write(self, *a, **kw)

    monkeypatch.setattr(zipfile.ZipFile, "write", slow_write)
    results: dict[str, object] = {}

    def first() -> None:
        results["first"] = backup_kb.backup_knowledge_base(str(data_dir))

    t = threading.Thread(target=first)
    t.start()
    assert inside.wait(timeout=5)
    try:
        with pytest.raises(backup_kb.BackupInProgressError):
            backup_kb.backup_knowledge_base(str(data_dir))
    finally:
        release.set()
        t.join(timeout=10)

    archive = Path(results["first"])  # type: ignore[arg-type]
    assert archive.exists(), "the loser must not delete the winner's archive"
    with zipfile.ZipFile(archive) as zf:
        assert "channels_db.json" in zf.namelist()
    assert len(list((data_dir / "backups").glob("kb_backup_*.zip"))) == 1


def test_lock_is_released_after_a_run_and_after_a_failure(
    data_dir: Path,
) -> None:
    backup_kb.backup_knowledge_base(str(data_dir))
    backup_kb.backup_knowledge_base(str(data_dir))  # no stale lock

    empty = data_dir.parent / "empty"
    empty.mkdir()
    with pytest.raises(RuntimeError):
        backup_kb.backup_knowledge_base(str(empty))
    (empty / "x.json").write_text("{}")
    backup_kb.backup_knowledge_base(str(empty))  # failure released the lock


def test_archive_name_carries_the_pid(data_dir: Path) -> None:
    out = Path(backup_kb.backup_knowledge_base(str(data_dir)))
    assert out.name.endswith(f"_{os.getpid()}.zip")
    assert out.name.startswith("kb_backup_")


def test_lock_file_is_not_archived(data_dir: Path) -> None:
    out = Path(backup_kb.backup_knowledge_base(str(data_dir)))
    with zipfile.ZipFile(out) as zf:
        assert not any(n.endswith(".lock") for n in zf.namelist())
