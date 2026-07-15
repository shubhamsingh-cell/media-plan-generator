"""Tests for the multi-process serving fixes: a cross-process /api/generate
concurrency cap (``_CrossProcessSlots``) and cross-worker job-status
mirroring (``_mirror_job`` + the GET /api/jobs/<id> mirror-read path).

Render runs gunicorn ``--preload --worker-class gevent --workers N``
(render.yaml is authoritative), single instance, all workers sharing one
filesystem but NOT one process. Two things were broken as a result:

1. A plain ``threading.BoundedSemaphore`` only bounds concurrency within
   one forked worker, so the real /api/generate cap was silently
   N x _MAX_CONCURRENT_GENERATE. ``_CrossProcessSlots`` fixes this with
   flock() on shared lock files in a tmpdir -- true across process
   boundaries, and the kernel auto-releases the flock if a worker dies.

2. ``_generation_jobs`` is a per-process dict, so a GET /api/jobs/<id>
   poll landing on a worker that didn't run the job 404'd until the S47
   Supabase fallback had bytes (only true once fully complete).
   ``_mirror_job`` writes a JSON snapshot to the same tmpdir so any
   worker can answer in-progress polls.

Tests (a)/(b) exercise the real live HTTP handler (per the task's
"integration-lite" allowance, same pattern as test_generate_concurrency.py)
so the mirror-read path in do_GET is proven end-to-end, not just the
helper function in isolation. Tests (c)/(d) construct fresh
``_CrossProcessSlots`` instances directly, per the module's own note that
env vars like NOVA_SLOT_DIR are read at instantiation time -- fighting
import order to test that is pointless when a fresh instance does the job.
"""

from __future__ import annotations

import http.client
import json
import secrets
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Iterator

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    """Start the real ThreadedHTTPServer on an ephemeral port (same
    pattern as tests/test_generate_concurrency.py's fixture)."""
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-http-server-mp"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("live_server did not start accepting connections in time")
    _http_get(port, "/api/health")  # absorb one-time warmup cost
    yield port
    server.shutdown()
    server.server_close()


@pytest.fixture()
def isolated_slot_dir(tmp_path, monkeypatch) -> str:
    """Redirect the module-global ``_generate_slots._slot_dir`` to a fresh
    per-test tmp dir. Both ``_mirror_job`` and the GET /api/jobs/<id>
    mirror-read path resolve this attribute at call time (not at import
    time), so this one monkeypatch is enough to isolate a test from any
    mirror/lock files left behind by other test modules sharing the real
    tmpdir path."""
    slot_dir = tmp_path / "mirror_slots"
    slot_dir.mkdir()
    monkeypatch.setattr(app_module._generate_slots, "_slot_dir", str(slot_dir))
    return str(slot_dir)


def _http_get(
    port: int, path: str, timeout: float = 5.0, headers: dict | None = None
) -> tuple[int, dict, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path, headers=headers or {})
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, dict(resp.getheaders()), body
    finally:
        conn.close()


def _make_processing_job(session_token: str) -> str:
    """Insert a job entry directly into app_module._generation_jobs,
    mirror it, then remove the dict entry -- simulating a poll that lands
    on a gunicorn worker OTHER than the one running the job."""
    job_id = uuid.uuid4().hex[:12]
    with app_module._generation_jobs_lock:
        app_module._generation_jobs[job_id] = {
            "status": "processing",
            "progress_pct": 40,
            "status_message": "Synthesizing knowledge base...",
            "created": time.time(),
            "result_bytes": None,
            "result_content_type": None,
            "result_filename": None,
            "error": None,
            "_session_token": session_token,
        }
    app_module._mirror_job(job_id)
    with app_module._generation_jobs_lock:
        app_module._generation_jobs.pop(job_id, None)
    return job_id


# ---------------------------------------------------------------------------
# (a) REGRESSION: cross-worker poll must see the mirror, not 404
# ---------------------------------------------------------------------------


def test_poll_reads_mirror_when_dict_entry_absent(
    live_server: int, isolated_slot_dir: str
) -> None:
    """The literal bug: /api/jobs/<id> landing on a worker that never had
    this job in its in-memory dict must still report live progress (via
    the mirror file), not a false 404."""
    port = live_server
    session_token = secrets.token_hex(16)
    job_id = _make_processing_job(session_token)

    status, _headers, body = _http_get(
        port,
        f"/api/jobs/{job_id}",
        headers={
            "Accept": "application/json",
            "Cookie": f"nova_session={session_token}",
        },
    )

    assert status == 200, f"expected 200 from the mirror path, got {status}: {body}"
    payload = json.loads(body)
    assert payload["status"] == "processing"
    assert payload["progress_pct"] == 40
    assert payload["source"] == "mirror"


# ---------------------------------------------------------------------------
# (b) IDOR: wrong session cookie must be rejected, not leak another user's job
# ---------------------------------------------------------------------------


def test_poll_mirror_rejects_mismatched_session_cookie(
    live_server: int, isolated_slot_dir: str
) -> None:
    port = live_server
    session_token = secrets.token_hex(16)
    job_id = _make_processing_job(session_token)

    status, _headers, body = _http_get(
        port,
        f"/api/jobs/{job_id}",
        headers={
            "Accept": "application/json",
            "Cookie": f"nova_session={secrets.token_hex(16)}",  # wrong token
        },
    )

    assert status == 403, f"expected 403 for a mismatched session, got {status}: {body}"
    payload = json.loads(body)
    assert payload["code"] == "FORBIDDEN"


# ---------------------------------------------------------------------------
# (c) Cross-process: the cap is enforced by the kernel, not by this process
# ---------------------------------------------------------------------------


def test_cross_process_slots_blocked_then_auto_released_on_process_death(
    tmp_path, monkeypatch
) -> None:
    """A separate OS process holding both slot flocks must block this
    process's acquire(); once that process dies (even ungracefully), the
    kernel auto-releases its flocks and the slot becomes acquirable again
    -- proving the cap is process-crash-safe, not just thread-safe."""
    slot_dir = tmp_path / "cross_proc_slots"
    slot_dir.mkdir()
    monkeypatch.setenv("NOVA_SLOT_DIR", str(slot_dir))

    holder_script = (
        "import fcntl, os, time\n"
        f"slot_dir = {str(slot_dir)!r}\n"
        "fds = []\n"
        "for i in range(2):\n"
        "    fd = open(os.path.join(slot_dir, f'generate_slot_{i}.lock'), 'a+b')\n"
        "    fcntl.flock(fd, fcntl.LOCK_EX)\n"
        "    fds.append(fd)\n"
        "print('LOCKED', flush=True)\n"
        "time.sleep(3)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", holder_script],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        ready_line = proc.stdout.readline()
        assert ready_line.strip() == "LOCKED", (
            f"subprocess failed to acquire both slot locks: {ready_line!r}"
        )

        slots = app_module._CrossProcessSlots(2)
        assert slots.acquire(blocking=False) is False, (
            "parent acquired a slot while an external process held both flocks"
        )

        proc.wait(timeout=10)

        assert slots.acquire(blocking=False) is True, (
            "parent could not acquire after the external process died -- "
            "kernel auto-release of its flocks is not working"
        )
        slots.release()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# (d) Over-release contract: preserved from BoundedSemaphore
# ---------------------------------------------------------------------------


def test_release_on_empty_stack_raises_value_error(tmp_path) -> None:
    """The handoff comment at app.py's async-generate release site depends
    on over-release raising, same as threading.BoundedSemaphore -- a
    silent no-op here would let a double-release bug through unnoticed."""
    slots = app_module._CrossProcessSlots(2)
    slots._slot_dir = str(tmp_path)
    with pytest.raises(ValueError):
        slots.release()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
