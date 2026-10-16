"""scripts/ship_from_worktree.sh ships must queue machine-wide, not run at once.

2026-10-16: six sessions shipping in parallel drove the Mac to load average
77-141 with ~277 pytest processes, and one ship failed its suite gate on two
wall-clock timing tests that pass alone in 14 s. The rebase -> suite ->
stability -> push section now runs under an exclusive flock; a second ship
waits (logging who holds it) and gives up with exit 7 after
SHIP_LOCK_TIMEOUT_S.

These drive the real script through its ``--lock-only`` dry-run mode. Every
test points SHIP_LOCK_FILE at a private path, so none can touch the real
/tmp/mpg-ship.lock that a concurrent ship may be holding.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterator, Optional

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "ship_from_worktree.sh"


@pytest.fixture()
def lock_env(tmp_path: Path) -> Dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "SHIP_LOCK_FILE": str(tmp_path / "ship.lock"),
            "SHIP_LOCK_WAIT_LOG_S": "1",
            "TMPDIR": str(tmp_path),
        }
    )
    env.pop("SHIP_LOCK_TIMEOUT_S", None)
    return env


def _run(
    env: Dict[str, str], *args: str, timeout_s: Optional[int] = None
) -> subprocess.CompletedProcess:
    env = dict(env)
    if timeout_s is not None:
        env["SHIP_LOCK_TIMEOUT_S"] = str(timeout_s)
    return subprocess.run(
        ["bash", str(SCRIPT), "--lock-only", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


class Holder:
    """A ship that has taken the lock and is sitting on it."""

    def __init__(self, env: Dict[str, str], hold_s: int, log: Path) -> None:
        self._log = log
        self._fh = log.open("w")
        self.proc = subprocess.Popen(
            ["bash", str(SCRIPT), "--lock-only", str(hold_s)],
            cwd=ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=self._fh,
        )
        deadline = time.time() + 20
        while "ship lock acquired" not in log.read_text():
            assert (
                time.time() < deadline
            ), f"holder never got the lock: {log.read_text()}"
            assert self.proc.poll() is None, log.read_text()
            time.sleep(0.1)

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self._fh.close()


@pytest.fixture()
def holder_factory(tmp_path: Path) -> Iterator:
    made = []

    def make(env: Dict[str, str], hold_s: int) -> Holder:
        h = Holder(env, hold_s, tmp_path / f"holder{len(made)}.log")
        made.append(h)
        return h

    yield make
    for h in made:
        h.stop()


def test_uncontended_ship_takes_and_releases_the_lock(lock_env: Dict[str, str]) -> None:
    result = _run(lock_env)
    assert result.returncode == 0, result.stderr
    assert "ship lock acquired" in result.stderr
    assert "ship lock released" in result.stderr
    assert "waiting for another ship" not in result.stderr


def test_second_ship_names_the_holder_and_gives_up_with_exit_7(
    lock_env: Dict[str, str], holder_factory
) -> None:
    holder = holder_factory(lock_env, 30)
    result = _run(lock_env, timeout_s=2)
    assert result.returncode == 7, result.stderr
    assert f"waiting for another ship (held by pid {holder.proc.pid} since" in (
        result.stderr
    )
    assert "gave up after 2s" in result.stderr
    assert "ship lock acquired" not in result.stderr


def test_queued_ship_proceeds_once_the_holder_finishes(
    lock_env: Dict[str, str], holder_factory
) -> None:
    holder_factory(lock_env, 3)
    result = _run(lock_env, timeout_s=30)
    assert result.returncode == 0, result.stderr
    assert "waiting for another ship" in result.stderr
    assert "ship lock acquired" in result.stderr


def test_waiter_repeats_the_waiting_line(
    lock_env: Dict[str, str], holder_factory
) -> None:
    holder_factory(lock_env, 30)
    result = _run(lock_env, timeout_s=4)
    assert result.stderr.count("waiting for another ship") >= 2, result.stderr


def test_lock_is_freed_when_the_holder_is_sigkilled(
    lock_env: Dict[str, str], holder_factory
) -> None:
    """No trap runs on SIGKILL; the kernel flock plus the helper's parent-pid
    watch must still free the lock, or one crashed ship wedges the queue."""
    holder = holder_factory(lock_env, 60)
    holder.proc.kill()
    holder.proc.wait()
    started = time.time()
    result = _run(lock_env, timeout_s=15)
    assert result.returncode == 0, result.stderr
    assert time.time() - started < 10


def test_unusable_lock_path_warns_but_never_blocks_a_ship(
    lock_env: Dict[str, str], tmp_path: Path
) -> None:
    env = dict(lock_env)
    env["SHIP_LOCK_FILE"] = str(tmp_path / "no_such_dir" / "ship.lock")
    result = _run(env, timeout_s=5)
    assert result.returncode == 0, result.stderr
    assert "WITHOUT the machine-wide lock" in result.stderr


def test_default_lock_path_is_fixed_not_tmpdir_relative() -> None:
    """$TMPDIR differs between sessions; a TMPDIR-relative path would give each
    session its own lock and queue nobody."""
    text = SCRIPT.read_text()
    assert 'SHIP_LOCK_FILE="${SHIP_LOCK_FILE:-/tmp/mpg-ship.lock}"' in text


def test_lock_wraps_the_ship_loop_and_is_freed_right_after_the_push() -> None:
    text = SCRIPT.read_text()
    acquire = text.index('ship_lock_acquire "$current_branch"')
    loop = text.index("attempt=1\nwhile")
    push = text.index("git push origin HEAD:main")
    release = text.index("    ship_lock_release\n", push)
    deploy_poll = text.index("Best-effort deploy verification")
    ci_watch = text.index('ci_watch_for_sha "$local_head"')
    assert acquire < loop < push < release < deploy_poll < ci_watch


def test_header_documents_exit_code_7_and_the_timeout() -> None:
    header = SCRIPT.read_text().split("set -euo pipefail")[0]
    assert "7 gave up waiting" in header
    assert "SHIP_LOCK_TIMEOUT_S" in header
    assert "--lock-only" in header
