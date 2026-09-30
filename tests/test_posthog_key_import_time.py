"""Import-time PostHog key resolution, exercised in a fresh interpreter.

The capture key is read from the environment when ``posthog_integration`` /
``posthog_tracker`` are imported, so these tests spawn a clean subprocess with a
controlled environment. Companion to tests/test_posthog_key_guard_and_backoff.py
(prod telemetry 2026-10-01: a ``phx_`` personal key was configured as the
capture key -> ~286 ERROR lines/hour and 100% of analytics dropped).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _run_fresh(code: str, env: dict[str, str]) -> str:
    full_env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("POSTHOG_API_KEY", "POSTHOG_PROJECT_API_KEY")
    }
    full_env.update(env)
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(PROJECT_ROOT),
        env=full_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    return out.stdout + out.stderr


def test_env_var_with_phx_key_disables_both_posthog_modules() -> None:
    out = _run_fresh(
        "import logging, sys, threading\n"
        "logging.basicConfig(level=logging.INFO, stream=sys.stdout,"
        " format='%(levelname)s|%(message)s')\n"
        "import posthog_integration as ph, posthog_tracker as pt\n"
        "for i in range(30):\n"
        "    ph.track_event('u', 'plan.generated',"
        " {'plan_type': 1, 'budget': 1, 'channels': []})\n"
        "print('ENABLED', ph.get_stats()['enabled'], pt._ENABLED)\n"
        "print('THREADS', sorted(t.name for t in threading.enumerate()))\n",
        {"POSTHOG_API_KEY": "phx_FAKEKEYFORPROBE123"},
    )
    assert "ENABLED False False" in out, out
    assert "posthog-flush" not in out.split("THREADS", 1)[1], out
    warnings = [ln for ln in out.splitlines() if ln.startswith("WARNING|")]
    assert len(warnings) == 1, warnings
    assert "POSTHOG_API_KEY" in warnings[0] and "phc_" in warnings[0]
    assert "phx_FAKEKEY" not in out, "the key must never be logged"


def test_project_key_env_var_takes_precedence_over_personal_key() -> None:
    out = _run_fresh(
        "import posthog_integration as ph, posthog_tracker as pt\n"
        "print('ENABLED', ph.get_stats()['enabled'], pt._ENABLED)\n",
        {
            "POSTHOG_API_KEY": "phx_FAKEKEYFORPROBE123",
            "POSTHOG_PROJECT_API_KEY": "phc_realprojectkey000",
        },
    )
    assert "ENABLED True True" in out, out


def test_enabled_log_lines_never_print_key_material() -> None:
    out = _run_fresh(
        "import logging, sys\n"
        "logging.basicConfig(level=logging.INFO, stream=sys.stdout)\n"
        "import posthog_integration as ph, posthog_tracker as pt\n"
        "ph.get_stats()\n",
        {"POSTHOG_PROJECT_API_KEY": "phc_SECRETPROJECTKEY0123456789"},
    )
    assert "SECRETPROJECTKEY" not in out, out
    assert "phc_SECR" not in out, out
