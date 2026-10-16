"""scripts/ship_from_worktree.sh must notice GitHub CI, not just the local suite.

"Nova AI Suite CI" sat red on main for two weeks (2026-10-01 .. 2026-10-15,
a Linux-only argv limit) while every ship run stayed green, because the script
only ran the suite locally. The script now warns about a red main up front and
watches the pushed commit's CI run, exiting 6 if it goes red.

These drive the real script through its ``--ci-check-only`` dry-run mode with
a fake ``gh`` on PATH, so no network and no push is involved. All poll
intervals are overridden to ~1s.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "ship_from_worktree.sh"

SHA = "a" * 40
RUN_URL = "https://github.com/example/repo/actions/runs/101"
MAIN_URL = "https://github.com/example/repo/actions/runs/99"

# Behaviour comes from files in $FAKE_GH_DIR, so one script serves every test.
FAKE_GH = r"""#!/usr/bin/env bash
echo "$*" >> "$FAKE_GH_DIR/calls.log"
if [ -f "$FAKE_GH_DIR/gh_fails" ]; then
    exit 1
fi
case "$1 $2" in
    "run list")
        if [[ "$*" == *"--commit"* ]]; then
            n=$(cat "$FAKE_GH_DIR/commit_calls" 2>/dev/null || echo 0)
            n=$((n + 1))
            echo "$n" > "$FAKE_GH_DIR/commit_calls"
            if [ "$n" -le "$(cat "$FAKE_GH_DIR/appear_after" 2>/dev/null || echo 0)" ]; then
                echo "[]"
            else
                cat "$FAKE_GH_DIR/runs_for_commit.json"
            fi
        else
            cat "$FAKE_GH_DIR/runs_on_main.json"
        fi
        ;;
    "run watch")
        if [ -f "$FAKE_GH_DIR/watch_hangs" ]; then
            exec sleep 60
        fi
        exit "$(cat "$FAKE_GH_DIR/watch_rc" 2>/dev/null || echo 0)"
        ;;
    "run view")
        if [[ "$*" == *"jobs"* ]]; then
            cat "$FAKE_GH_DIR/jobs.json"
        else
            cat "$FAKE_GH_DIR/run_state.json"
        fi
        ;;
esac
"""

GREEN_MAIN = [
    {
        "status": "completed",
        "conclusion": "success",
        "headSha": SHA,
        "url": MAIN_URL,
    }
]
RED_MAIN = [
    {
        "status": "completed",
        "conclusion": "failure",
        "headSha": "b" * 40,
        "url": MAIN_URL,
    }
]
ONE_RUN = [{"databaseId": 101, "url": RUN_URL}]


class Harness:
    """A fake-``gh`` sandbox plus a runner for the real script."""

    def __init__(self, root: Path) -> None:
        self.dir = root / "fake_gh"
        self.dir.mkdir()
        gh = self.dir / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
        self.put("runs_on_main.json", GREEN_MAIN)
        self.put("runs_for_commit.json", ONE_RUN)
        self.put("run_state.json", {"status": "completed", "conclusion": "success"})
        self.put("jobs.json", {"jobs": []})

    def put(self, name: str, payload: Any) -> None:
        text = payload if isinstance(payload, str) else json.dumps(payload)
        (self.dir / name).write_text(text)

    def calls(self) -> List[str]:
        log = self.dir / "calls.log"
        return log.read_text().splitlines() if log.exists() else []

    def run(
        self,
        *args: str,
        extra_env: Optional[Dict[str, str]] = None,
        timeout: int = 60,
    ) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env.update(
            {
                "PATH": f"{self.dir}{os.pathsep}{env.get('PATH', '')}",
                "FAKE_GH_DIR": str(self.dir),
                "SHIP_CI_APPEAR_POLL_INTERVAL": "1",
                "SHIP_CI_APPEAR_POLL_MAX": "5",
                "SHIP_CI_WATCH_INTERVAL": "1",
                "SHIP_CI_WATCH_MAX": "20",
            }
        )
        env.pop("SHIP_SKIP_CI_WATCH", None)
        env.update(extra_env or {})
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


@pytest.fixture()
def gh(tmp_path: Path) -> Harness:
    return Harness(tmp_path)


def test_script_parses_as_bash() -> None:
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT)], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr


def test_green_ci_prints_url_and_exits_zero(gh: Harness) -> None:
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 0, result.stderr
    assert f"CI green: {RUN_URL}" in result.stderr
    assert "WARNING" not in result.stderr
    assert any(c.startswith("run watch 101") for c in gh.calls())


def test_watch_uses_exit_status_and_requested_interval(gh: Harness) -> None:
    gh.run("--ci-check-only", SHA)
    watch = [c for c in gh.calls() if c.startswith("run watch")]
    assert watch, gh.calls()
    assert "--exit-status" in watch[0]
    assert "--interval 1" in watch[0]


def test_red_ci_exits_6_and_names_only_the_failing_jobs(gh: Harness) -> None:
    gh.put("run_state.json", {"status": "completed", "conclusion": "failure"})
    gh.put("watch_rc", "1")
    gh.put(
        "jobs.json",
        {
            "jobs": [
                {"name": "Python Lint & Compile", "conclusion": "success"},
                {"name": "Pytest Suite", "conclusion": "failure"},
                {"name": "PR Size Check", "conclusion": "skipped"},
            ]
        },
    )
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 6, result.stderr
    assert "CI FAILED" in result.stderr
    assert RUN_URL in result.stderr
    assert "Pytest Suite (failure)" in result.stderr
    assert "Python Lint & Compile" not in result.stderr
    assert "PR Size Check" not in result.stderr


def test_a_cancelled_run_is_not_green(gh: Harness) -> None:
    gh.put("run_state.json", {"status": "completed", "conclusion": "cancelled"})
    gh.put("watch_rc", "1")
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 6, result.stderr


def test_watch_error_on_a_green_run_is_not_reported_red(gh: Harness) -> None:
    """`gh run watch` exits non-zero on its own network errors; the run's
    recorded conclusion decides, so a flaky watch cannot fake a red CI."""
    gh.put("watch_rc", "1")
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 0, result.stderr
    assert "CI green" in result.stderr


def test_red_main_is_a_loud_warning_that_does_not_block(gh: Harness) -> None:
    gh.put("runs_on_main.json", RED_MAIN)
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 0, result.stderr
    assert "WARNING: GitHub CI on main is NOT GREEN" in result.stderr
    assert "concluded 'failure'" in result.stderr
    assert MAIN_URL in result.stderr


def test_in_progress_run_does_not_hide_a_red_completed_one(gh: Harness) -> None:
    newer = {"status": "in_progress", "conclusion": "", "headSha": SHA, "url": "u2"}
    gh.put("runs_on_main.json", [newer] + RED_MAIN)
    result = gh.run("--ci-check-only", SHA)
    assert "NOT GREEN" in result.stderr
    assert "still in progress: u2" in result.stderr


def test_green_main_is_reported_without_a_warning(gh: Harness) -> None:
    result = gh.run("--ci-check-only", SHA)
    assert "GitHub CI on main is green" in result.stderr
    assert "NOT GREEN" not in result.stderr


def test_run_that_appears_late_is_still_found(gh: Harness) -> None:
    gh.put("appear_after", "2")
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 0, result.stderr
    assert "CI green" in result.stderr
    assert int((gh.dir / "commit_calls").read_text()) == 3


def test_run_that_never_appears_warns_but_does_not_fail(gh: Harness) -> None:
    gh.put("appear_after", "999")
    result = gh.run("--ci-check-only", SHA, extra_env={"SHIP_CI_APPEAR_POLL_MAX": "2"})
    assert result.returncode == 0, result.stderr
    assert "no GitHub CI run" in result.stderr
    assert "CI NOT verified" in result.stderr
    assert not any(c.startswith("run watch") for c in gh.calls())


def test_skip_flag_skips_the_watch_and_says_so(gh: Harness) -> None:
    result = gh.run("--ci-check-only", SHA, extra_env={"SHIP_SKIP_CI_WATCH": "1"})
    assert result.returncode == 0, result.stderr
    assert "SKIPPED" in result.stderr
    assert "NOT checked" in result.stderr
    assert not any(c.startswith("run watch") for c in gh.calls())
    assert not any("--commit" in c for c in gh.calls())


def test_watch_is_bounded_and_a_timeout_is_a_warning(gh: Harness) -> None:
    gh.put("watch_hangs", "1")
    gh.put("run_state.json", {"status": "in_progress", "conclusion": None})
    result = gh.run("--ci-check-only", SHA, extra_env={"SHIP_CI_WATCH_MAX": "2"})
    assert result.returncode == 0, result.stderr
    assert "stopped watching" in result.stderr
    assert "CI NOT verified" in result.stderr


def test_unusable_gh_warns_and_never_fails_the_ship(gh: Harness) -> None:
    gh.put("gh_fails", "1")
    result = gh.run("--ci-check-only", SHA)
    assert result.returncode == 0, result.stderr
    assert "could not query GitHub CI status" in result.stderr
    assert "NOT watched" in result.stderr


def test_unknown_flag_is_refused_instead_of_shipping(gh: Harness) -> None:
    """A typo'd dry-run flag must not fall through to a real ship."""
    result = gh.run("--ci-check-onyl")
    assert result.returncode == 1
    assert "unknown argument" in result.stderr
    assert gh.calls() == []


def test_header_documents_exit_code_6_and_the_skip_flag() -> None:
    header = SCRIPT.read_text().split("set -euo pipefail")[0]
    assert "6 pushed, but GitHub CI" in header
    assert "SHIP_SKIP_CI_WATCH=1" in header
    assert "--ci-check-only" in header
