"""Shared fixtures for Nova AI Suite test suite.

The server uses http.server.HTTPServer which blocks on serve_forever(),
so tests focus on static analysis, template validation, data integrity,
and security checks rather than live HTTP testing.
"""

import errno
import os
import socket
import sys
import tempfile
from pathlib import Path
from typing import Generator

import pytest

# Keep the AutoQC background monitor out of the test suite. app.py starts it
# at import time (several test modules import app during collection); on a
# slow or CPU-loaded run the thread outlives its 90s startup grace, runs a
# real check cycle, and mutates auto_qc module state while tests are still
# asserting on get_status(). Must be set before the first `import app`.
# Hard assignment (not setdefault): an inherited NOVA_DISABLE_AUTO_QC=0 from
# an outer shell must not silently re-enable the monitor mid-suite.
os.environ["NOVA_DISABLE_AUTO_QC"] = "1"

# Ensure the project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Cross-RUN isolation for the flock-backed /api/generate slot pool.
# app.py's _CrossProcessSlots resolves its slot directory when the module
# is imported: $NOVA_SLOT_DIR if set, else a machine-shared tmpdir keyed
# only by PORT/TEST_PORT ("nova_gen_slots_dev" for plain pytest runs,
# "nova_gen_slots_59999" for every ship_from_worktree.sh gate run).
# Sharing across processes is the production design -- gunicorn workers
# must contend for the same slots -- but between two unrelated pytest
# runs on one machine it means suite A's real /api/generate holds a flock
# that suite B's live server then sees: B gets spurious 429s and its
# slots-released barriers time out (2026-07-16: 4-9 failures per run in
# test_generate_concurrency.py whenever two suites overlapped, 0 when
# quiet -- e.g. a ship gate racing a concurrent session's suite). Point
# each pytest run at its own private pool before any test module imports
# app; within-run cross-process semantics are unchanged, and tests that
# need a bespoke dir (test_multiprocess_serving.py) still override at a
# finer granularity. Left uncleaned like app.py's own slot dirs: a few
# tiny lock/mirror files the OS tmpdir reaper handles.
if "NOVA_SLOT_DIR" not in os.environ:
    os.environ["NOVA_SLOT_DIR"] = tempfile.mkdtemp(prefix="nova_gen_slots_pytest_")

# ── External-network guard ──────────────────────────────────────────────────
# The suite's contract is "all external calls are mocked", but nothing used
# to enforce it: one unmocked call reaching a slow upstream deterministically
# hung the ship gate for 30+ minutes (2026-07-17, fixed in 4cac3116 after the
# test-pollution root cause was traced). This guard closes the CLASS: any
# non-loopback TCP connect during pytest fails in milliseconds with an
# unmistakable error instead of hanging. Loopback stays open (several tests
# run a real local HTTP server), AF_UNIX stays open, `live`-marked tests are
# exempted per-test by the _allow_net_for_live_tests fixture below (so keyed
# environments still run their real probes; keyless ones keep auto-skipping
# on absent keys), and whole intentionally-live runs can opt out with
# NOVA_TESTS_ALLOW_NET=1. Installed at conftest import -- before
# any test module imports app -- so app.py's background threads are covered
# too. The error subclasses OSError so production code's graceful
# network-failure fallbacks (urllib catches OSError) degrade cleanly rather
# than crash; DNS resolution is not intercepted (OS resolvers time out on
# their own -- the hang class lived in connect/read, which this blocks).

_REAL_SOCKET_CONNECT = socket.socket.connect
_REAL_SOCKET_CONNECT_EX = socket.socket.connect_ex


class ExternalNetworkBlocked(ConnectionRefusedError):
    """Raised when a test tries to reach a non-loopback address."""


def _address_is_loopback(address) -> bool:  # noqa: ANN001
    """True for AF_UNIX paths and loopback host tuples."""
    if isinstance(address, (str, bytes)):  # AF_UNIX socket path
        return True
    host = str(address[0]).strip("[]").lower() if address else ""
    return (
        host in ("localhost", "::1", "")
        or host.startswith("127.")
        or host.startswith("::ffff:127.")
    )


def _guarded_connect(self, address):  # noqa: ANN001
    if _address_is_loopback(address) or os.environ.get("NOVA_TESTS_ALLOW_NET") == "1":
        return _REAL_SOCKET_CONNECT(self, address)
    raise ExternalNetworkBlocked(
        f"tests/conftest.py network guard: blocked external connect to "
        f"{address!r}. Unit tests must mock external calls (see module "
        f"docstrings); set NOVA_TESTS_ALLOW_NET=1 only for intentional "
        f"live-network runs."
    )


def _guarded_connect_ex(self, address):  # noqa: ANN001
    if _address_is_loopback(address) or os.environ.get("NOVA_TESTS_ALLOW_NET") == "1":
        return _REAL_SOCKET_CONNECT_EX(self, address)
    return errno.ECONNREFUSED  # refuse without raising (connect_ex contract)


socket.socket.connect = _guarded_connect  # type: ignore[method-assign]
socket.socket.connect_ex = _guarded_connect_ex  # type: ignore[method-assign]


@pytest.fixture(autouse=True)
def _allow_net_for_live_tests(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exempt `live`-marked tests from the external-network guard.

    Their whole point is a real probe, and they gate themselves on key
    presence -- without this, a keyed environment would fail them against
    the guard instead of probing. monkeypatch restores the env after each
    test, so the exemption never leaks into neighboring tests.
    """
    if request.node.get_closest_marker("live"):
        monkeypatch.setenv("NOVA_TESTS_ALLOW_NET", "1")


TEMPLATES_DIR = PROJECT_ROOT / "templates"
DATA_DIR = PROJECT_ROOT / "data"


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def templates_dir() -> Path:
    """Return the templates directory."""
    return TEMPLATES_DIR


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Return the data directory."""
    return DATA_DIR


@pytest.fixture(scope="session")
def template_files() -> list[Path]:
    """Return all HTML template files."""
    return sorted(TEMPLATES_DIR.glob("*.html"))


@pytest.fixture(scope="session")
def python_files() -> list[Path]:
    """Return all .py files in the project root (non-recursive)."""
    return sorted(PROJECT_ROOT.glob("*.py"))


@pytest.fixture(scope="session")
def app_source() -> str:
    """Read and return app.py + routes/pages.py source code (cached for the session).

    Routes were decomposed from app.py into routes/pages.py, so tests that
    check for route strings need both files concatenated.
    """
    source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
    pages_path = PROJECT_ROOT / "routes" / "pages.py"
    if pages_path.exists():
        source += "\n" + pages_path.read_text(encoding="utf-8")
    return source


def pytest_configure(config) -> None:  # noqa: ANN001
    """Register custom markers used by individual test modules."""
    config.addinivalue_line(
        "markers",
        "live: live-network smoke tests; auto-skipped when API keys are absent",
    )
