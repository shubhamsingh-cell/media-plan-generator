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

import base64
import hashlib
import http.client
import json
import re
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
# (b2) Once a job COMPLETES its id is a bearer token: the Slack notification
# posts /api/jobs/<id> to a channel, so the people clicking it never hold the
# generating session's cookie. 403ing them made every Slack download button
# dead for the full 24h job-expiry window. In-flight jobs stay session-locked
# (test (b) above) -- only the finished artifact is shareable.
# ---------------------------------------------------------------------------


def _make_completed_job(session_token: str, keep_in_dict: bool = False) -> str:
    job_id = uuid.uuid4().hex[:12]
    with app_module._generation_jobs_lock:
        app_module._generation_jobs[job_id] = {
            "status": "completed",
            "progress_pct": 100,
            "status_message": "Complete",
            "created": time.time(),
            "result_bytes": b"PK\x03\x04fake-zip-payload",
            "result_content_type": "application/zip",
            "result_filename": "Innovetive_PetCare_Media_Plan.zip",
            "error": None,
            "_session_token": session_token,
        }
    app_module._mirror_job(job_id)
    if not keep_in_dict:
        with app_module._generation_jobs_lock:
            app_module._generation_jobs.pop(job_id, None)
    return job_id


def test_completed_mirror_serves_mismatched_session(
    live_server: int, isolated_slot_dir: str
) -> None:
    """Cross-worker path: a teammate polling a COMPLETED job from Slack has a
    different cookie and must still be told the plan is ready."""
    port = live_server
    job_id = _make_completed_job(secrets.token_hex(16))

    status, _headers, body = _http_get(
        port,
        f"/api/jobs/{job_id}",
        headers={
            "Accept": "application/json",
            "Cookie": f"nova_session={secrets.token_hex(16)}",  # wrong token
        },
    )

    assert status == 200, f"expected 200 for a completed job, got {status}: {body}"
    payload = json.loads(body)
    assert payload["status"] == "completed"
    assert payload["filename"] == "Innovetive_PetCare_Media_Plan.zip"


def test_completed_in_memory_job_downloads_for_mismatched_session(
    live_server: int, isolated_slot_dir: str
) -> None:
    """Same-worker path: the job is still in this process's dict, so the poll
    never reaches the mirror. It must serve the bytes, not 403."""
    port = live_server
    job_id = _make_completed_job(secrets.token_hex(16), keep_in_dict=True)
    try:
        status, headers, body = _http_get(
            port,
            f"/api/jobs/{job_id}",
            headers={"Cookie": f"nova_session={secrets.token_hex(16)}"},
        )

        assert status == 200, f"expected 200 download, got {status}: {body}"
        assert body == b"PK\x03\x04fake-zip-payload"
        assert "attachment" in (headers.get("Content-Disposition") or "")
    finally:
        with app_module._generation_jobs_lock:
            app_module._generation_jobs.pop(job_id, None)


# ---------------------------------------------------------------------------
# (b3) The S47 Supabase copy is the ONLY store that outlives a worker restart
# and the in-memory 5-minute byte cleanup, so it backstops the download. Two
# things must hold: it takes over when the in-memory bytes are gone (otherwise
# the second person to click a Slack link gets "already downloaded"), and it
# honours the same 24h lifetime as every other path -- nova_generated_plans
# has no TTL of its own, so an unbounded read makes the link permanent.
# ---------------------------------------------------------------------------


class _FakeSupabaseTable:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self._job_id: str | None = None
        self._cutoff: str | None = None

    def select(self, *_args: object) -> _FakeSupabaseTable:
        return self

    def eq(self, _field: str, value: str) -> _FakeSupabaseTable:
        self._job_id = value
        return self

    def gte(self, _field: str, value: str) -> _FakeSupabaseTable:
        self._cutoff = value
        return self

    def execute(self) -> object:
        matched = [r for r in self._rows if r["job_id"] == self._job_id]
        if self._cutoff is not None:
            matched = [r for r in matched if r["created_at"] >= self._cutoff]
        return type("_Result", (), {"data": matched})()


def _install_fake_supabase(monkeypatch, rows: list[dict]) -> None:
    import supabase_client

    monkeypatch.setattr(
        supabase_client,
        "get_client",
        lambda: type("_Client", (), {"table": lambda _self, _n: _FakeSupabaseTable(rows)})(),
    )


def _supabase_row(job_id: str, age_seconds: float) -> dict:
    return {
        "job_id": job_id,
        "zip_data": base64.b64encode(b"PK\x03\x04supabase-copy").decode(),
        "filename": "Innovetive_PetCare_Media_Plan.zip",
        "created_at": time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - age_seconds)
        ),
    }


def test_supabase_serves_when_in_memory_bytes_already_cleared(
    live_server: int, isolated_slot_dir: str, monkeypatch
) -> None:
    """S37 clears result_bytes 5 min after the first download. Without the
    Supabase fallback the next click gets 410 'already downloaded'."""
    port = live_server
    job_id = _make_completed_job(secrets.token_hex(16), keep_in_dict=True)
    with app_module._generation_jobs_lock:
        app_module._generation_jobs[job_id]["result_bytes"] = b""
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=60)])
    try:
        status, _headers, body = _http_get(port, f"/api/jobs/{job_id}")
        assert status == 200, f"expected Supabase fallback 200, got {status}: {body}"
        assert body == b"PK\x03\x04supabase-copy"
    finally:
        with app_module._generation_jobs_lock:
            app_module._generation_jobs.pop(job_id, None)


def test_supabase_refuses_row_older_than_job_expiry(
    live_server: int, isolated_slot_dir: str, monkeypatch
) -> None:
    """A completed job_id is a bearer token, so an unbounded Supabase read
    would make that token valid forever to anyone who ever saw the link."""
    port = live_server
    job_id = uuid.uuid4().hex[:12]  # not in this process's dict, no mirror file
    stale_age = app_module._GENERATION_JOB_EXPIRY_SECONDS + 3600
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=stale_age)])

    status, _headers, body = _http_get(port, f"/api/jobs/{job_id}")
    assert status == 404, f"expected 404 for an expired plan, got {status}: {body}"
    assert b"supabase-copy" not in body


def test_supabase_serves_row_inside_job_expiry(
    live_server: int, isolated_slot_dir: str, monkeypatch
) -> None:
    """Guard against the expiry bound above being so tight it rejects
    everything -- a fresh row on the same path must still serve."""
    port = live_server
    job_id = uuid.uuid4().hex[:12]
    fresh_age = app_module._GENERATION_JOB_EXPIRY_SECONDS - 3600
    _install_fake_supabase(monkeypatch, [_supabase_row(job_id, age_seconds=fresh_age)])

    status, _headers, body = _http_get(port, f"/api/jobs/{job_id}")
    assert status == 200, f"expected 200 for a fresh plan, got {status}: {body}"
    assert body == b"PK\x03\x04supabase-copy"


# ---------------------------------------------------------------------------
# (b4) A completed job_id is a bearer token, so its ENTROPY is the only thing
# standing between a stranger and someone's media plan. Rate limiting cannot
# be that control: the office shares one NAT egress IP and the wizard polls
# /api/jobs/<id> every 2s (30/min, exactly _RATE_LIMIT_MAX), so an IP bucket
# would throttle real users first. These pin the id width instead.
# ---------------------------------------------------------------------------


def test_plan_results_id_is_full_128_bit_and_route_accepts_it(
    live_server: int, isolated_slot_dir: str
) -> None:
    """GET /api/plan-results/<id> returns the whole plan JSON with NO auth
    check (unlike /api/campaign-intel/metrics beside it), so that id is a
    bearer token too and needs the same width as job_id -- and the route has
    to admit it. The width of the id the app mints is pinned behaviourally in
    tests/test_generate_concurrency.py; this covers the route half."""
    port = live_server
    plan_id = uuid.uuid4().hex
    app_module._store_plan_result(plan_id, {"client_name": "Acme", "roles": []})
    try:
        status, _headers, body = _http_get(port, f"/api/plan-results/{plan_id}")
        assert status == 200, f"32-char plan_id must be accepted, got {status}: {body}"
        assert json.loads(body)["plan_id"] == plan_id
    finally:
        with app_module._plan_results_lock:
            app_module._plan_results_store.pop(plan_id, None)


def test_every_hex_id_validator_accepts_the_current_id_width() -> None:
    """job_id and _plan_id are minted in ONE place each but validated in
    several, across app.py and routes/. When they were widened to 128 bits,
    three validators still hard-coded the old 12-char cap -- /api/jobs/<id>/
    qa-ack (caught by its own test) and the PDF-export and plan-view routes
    (caught by NOTHING; the whole suite stayed green while both 400'd on every
    new plan). This is the invariant that would have caught all three: any
    validator for a lowercase-hex id must admit the width we actually mint.
    """
    # Whole raw-string pattern literals mentioning a lowercase-hex class --
    # NOT the individual {n} fragments, which would flag a correct
    # ({12}|{32}) alternation for the half that legitimately doesn't match.
    literal_re = re.compile(r'r"((?:[^"\\]|\\.)*\[a-f0-9\](?:[^"\\]|\\.)*)"')
    live_id = uuid.uuid4().hex  # 32 chars, as minted today
    offenders: list[str] = []

    for py in sorted(PROJECT_ROOT.glob("*.py")) + sorted(
        PROJECT_ROOT.glob("routes/*.py")
    ):
        for lineno, line in enumerate(py.read_text().splitlines(), 1):
            for pattern in literal_re.findall(line):
                try:
                    match = re.match(pattern, live_id)
                except re.error:
                    continue  # not a standalone compilable validator
                if not match or match.end() != len(live_id):
                    offenders.append(
                        f"{py.relative_to(PROJECT_ROOT)}:{lineno}: {pattern!r} "
                        f"rejects a live {len(live_id)}-char id"
                    )

    assert not offenders, "hex-id validators out of step with minted ids:\n" + "\n".join(
        offenders
    )


def test_job_route_accepts_32_char_ids_and_rejects_malformed(
    live_server: int, isolated_slot_dir: str
) -> None:
    """The route regex has to admit the new 32-char ids (and the 12-char ones
    still in flight during a rolling deploy) without admitting junk."""
    port = live_server

    status, _h, _b = _http_get(port, f"/api/jobs/{uuid.uuid4().hex}")
    assert status == 404, f"32-char id should reach lookup (404), got {status}"

    status, _h, _b = _http_get(port, f"/api/jobs/{uuid.uuid4().hex[:12]}")
    assert status == 404, f"legacy 12-char id should still reach lookup, got {status}"

    for bad in ("g" * 12, "a" * 33, "../etc/passwd", "abc-def"):
        status, _h, _b = _http_get(port, f"/api/jobs/{bad}")
        assert status in (
            400,
            404,
        ), f"malformed id {bad!r} must not reach the job store, got {status}"


# ---------------------------------------------------------------------------
# (a2) SECURITY: raw session token must never touch disk, only its sha256.
# The mirror file is default-0644 and can live up to 24h -- verified to
# FAIL against pre-fix app.py (commit 5ba77bf7, which snapshotted the raw
# "_session_token" field): see the task report for the failing run.
# ---------------------------------------------------------------------------


def test_mirror_file_never_contains_raw_session_token(
    live_server: int, isolated_slot_dir: str
) -> None:
    session_token = secrets.token_hex(16)
    job_id = _make_processing_job(session_token)

    mirror_path = Path(isolated_slot_dir) / f"job_{job_id}.json"
    raw_bytes = mirror_path.read_bytes()

    assert (
        session_token.encode("utf-8") not in raw_bytes
    ), "raw session token substring found in mirror file on disk"

    mirror_data = json.loads(raw_bytes)
    expected_sha = hashlib.sha256(session_token.encode("utf-8")).hexdigest()
    assert mirror_data.get("_session_token_sha256") == expected_sha
    assert "_session_token" not in mirror_data


# ---------------------------------------------------------------------------
# (a3) LEGACY COMPAT: a mirror file written by the previous build (raw
# "_session_token", no sha256 key) must still enforce ownership rather than
# silently skipping the check during a rolling deploy.
# ---------------------------------------------------------------------------


def _write_legacy_mirror(slot_dir: str, job_id: str, session_token: str) -> None:
    """Hand-write a mirror JSON file in the OLD raw-token format, simulating
    a straggler file left on disk by the pre-hardening build."""
    mirror_path = Path(slot_dir) / f"job_{job_id}.json"
    mirror_path.write_text(
        json.dumps(
            {
                "status": "processing",
                "progress_pct": 55,
                "status_message": "Synthesizing knowledge base...",
                "created": time.time(),
                "error": None,
                "result_filename": None,
                "result_content_type": None,
                "_session_token": session_token,
            }
        )
    )


def test_poll_mirror_legacy_raw_token_format_correct_cookie(
    live_server: int, isolated_slot_dir: str
) -> None:
    port = live_server
    session_token = secrets.token_hex(16)
    job_id = uuid.uuid4().hex[:12]
    _write_legacy_mirror(isolated_slot_dir, job_id, session_token)

    status, _headers, body = _http_get(
        port,
        f"/api/jobs/{job_id}",
        headers={
            "Accept": "application/json",
            "Cookie": f"nova_session={session_token}",
        },
    )
    assert (
        status == 200
    ), f"expected 200 for correct cookie on legacy mirror, got {status}: {body}"
    payload = json.loads(body)
    assert payload["status"] == "processing"


def test_poll_mirror_legacy_raw_token_format_wrong_cookie(
    live_server: int, isolated_slot_dir: str
) -> None:
    port = live_server
    session_token = secrets.token_hex(16)
    job_id = uuid.uuid4().hex[:12]
    _write_legacy_mirror(isolated_slot_dir, job_id, session_token)

    status, _headers, body = _http_get(
        port,
        f"/api/jobs/{job_id}",
        headers={
            "Accept": "application/json",
            "Cookie": f"nova_session={secrets.token_hex(16)}",  # wrong token
        },
    )
    assert (
        status == 403
    ), f"expected 403 for wrong cookie on legacy mirror, got {status}: {body}"
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
        assert (
            ready_line.strip() == "LOCKED"
        ), f"subprocess failed to acquire both slot locks: {ready_line!r}"

        slots = app_module._CrossProcessSlots(2)
        assert (
            slots.acquire(blocking=False) is False
        ), "parent acquired a slot while an external process held both flocks"

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
