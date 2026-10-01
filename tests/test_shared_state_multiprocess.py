"""Cross-worker and restart-survival tests over REAL server processes.

Production: ``gunicorn --workers 4 --preload`` on one Render instance
(numInstances 1, no persistent disk; the 2026-10-01 read of the Render
service API). Share links, plan results, job status/downloads and the QA
override-ack all lived in per-process dicts, so any request landing on a
worker other than the one that wrote the state 404'd -- ``qa-ack`` returned
404 on 3 of 5 real prod jobs -- and every deploy (37 in 7 days) wiped them.

Each worker here is a separate OS process (tests/shared_state_worker.py)
running the real ``app`` handler on its own port and sharing one state
directory, like gunicorn workers sharing an instance's disk. The restart
test additionally points two processes with DIFFERENT (empty) directories at
one loopback PostgREST fake (tests/fake_postgrest.py), i.e. a new instance
after a deploy that can only recover state from the durable Supabase layer.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

import pytest

from tests.fake_postgrest import FakePostgrest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKER = PROJECT_ROOT / "tests" / "shared_state_worker.py"

_SESSION = secrets.token_hex(16)  # the generating session (nova_session cookie)
_ZIP = b"PK\x03\x04" + b"cross-worker-zip-payload" * 64
_BUNDLE_QA = {
    "qa_status": "critical",
    "critical_count": 1,
    "codes": ["snake_case_leak"],
    "findings": [{"severity": "critical", "code": "snake_case_leak"}],
}


class Worker:
    """One real app process: HTTP on ``port`` plus a stdin/stdout command pipe."""

    def __init__(self, state_dir: str, log_dir: str, name: str, extra_env: dict) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("SUPABASE_")}
        env.pop("NOVA_STATE_SUPABASE_URL", None)
        env.pop("NOVA_STATE_SUPABASE_KEY", None)
        env.update({"NOVA_SLOT_DIR": state_dir, "PYTHONUNBUFFERED": "1"})
        env.update(extra_env)
        self.name = name
        self.log_path = os.path.join(log_dir, f"{name}.log")
        self._log = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [sys.executable, str(WORKER)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log,
            env=env,
            cwd=str(PROJECT_ROOT),
            text=True,
            bufsize=1,
        )
        self.port = 0

    def wait_ready(self, timeout: float = 45.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                break
            if line.startswith("READY "):
                self.port = int(line.split()[1])
                return
        raise AssertionError(f"worker {self.name} never became ready; see {self.log_path}")

    def cmd(self, **payload: Any) -> dict:
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        assert line, f"worker {self.name} died; see {self.log_path}"
        return json.loads(line)

    def http(
        self, method: str, path: str, body: Any = None, headers: dict | None = None
    ) -> tuple[int, dict, bytes]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        try:
            raw = json.dumps(body).encode("utf-8") if body is not None else None
            hdrs = {"Content-Type": "application/json"} if raw is not None else {}
            hdrs.update(headers or {})
            conn.request(method, path, body=raw, headers=hdrs)
            resp = conn.getresponse()
            return resp.status, dict(resp.getheaders()), resp.read()
        finally:
            conn.close()

    def stop(self) -> None:
        try:
            if self.proc.poll() is None:
                self.proc.stdin.close()
                self.proc.terminate()
                self.proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            self.proc.kill()
            self.proc.wait(timeout=5)
        finally:
            self._log.close()


def _start(specs: list[tuple[str, str, dict]], log_dir: str) -> list[Worker]:
    workers = [Worker(d, log_dir, n, env) for n, d, env in specs]
    try:
        for w in workers:
            w.wait_ready()
    except AssertionError:
        for w in workers:
            w.stop()
        raise
    return workers


def _csrf_token() -> str:
    """Fresh double-submit token per request: the token embeds its expiry, so
    one minted at import goes stale if the wall clock jumps (host sleep)."""
    return f"{secrets.token_hex(16)}.{int(time.time()) + 3600}"


def _post_headers(session: str = _SESSION) -> dict:
    csrf = _csrf_token()
    return {
        "Cookie": f"csrf_token={csrf}; nova_session={session}",
        "X-CSRF-Token": csrf,
        "Origin": "http://localhost",
    }


@pytest.fixture(scope="module")
def log_dir() -> Iterator[str]:
    with tempfile.TemporaryDirectory(prefix="nova_state_logs_") as d:
        yield d


@pytest.fixture(scope="module")
def cluster(log_dir: str) -> Iterator[tuple[Worker, Worker, Worker]]:
    """Three workers sharing ONE instance directory, no durable layer: every
    cross-worker read below must be served by the shared file layer."""
    with tempfile.TemporaryDirectory(prefix="nova_state_instance_") as state_dir:
        workers = _start([(n, state_dir, {}) for n in ("A", "B", "C")], log_dir)
        try:
            yield workers[0], workers[1], workers[2]
        finally:
            for w in workers:
                w.stop()


def _share(worker: Worker, plan: dict, client: str) -> tuple[int, dict]:
    status, _h, raw = worker.http(
        "POST", "/api/plan/share", {"plan_data": plan, "client": client}, _post_headers()
    )
    return status, json.loads(raw or b"{}")


# ---------------------------------------------------------------------------
# Share links
# ---------------------------------------------------------------------------


def test_share_created_on_one_worker_is_viewable_on_every_worker(cluster) -> None:
    a, b, c = cluster
    status, payload = _share(a, {"summary": {"industry": "Logistics"}}, "Crossworker Freight")
    assert status == 200, payload
    share_id = payload["share_id"]
    for worker in (a, b, c):
        status, _h, body = worker.http("GET", f"/plan/shared/{share_id}")
        assert status == 200, f"worker {worker.name}: GET share -> {status}"
        assert b"Crossworker Freight" in body


def test_share_feedback_is_accepted_on_a_worker_that_did_not_create_it(cluster) -> None:
    a, _b, c = cluster
    status, payload = _share(a, {"summary": {"industry": "Retail"}}, "Feedback Co")
    assert status == 200, payload
    status, _h, raw = c.http(
        "POST",
        "/api/plan/feedback",
        {"share_id": payload["share_id"], "name": "Rev", "comment": "Looks right"},
        _post_headers(),
    )
    assert status == 200, raw
    assert json.loads(raw)["ok"] is True


def test_new_share_ids_are_unguessable(cluster) -> None:
    a, b, _c = cluster
    status, payload = _share(a, {"summary": {"industry": "Energy"}}, "Entropy Co")
    assert status == 200, payload
    assert re.fullmatch(r"[A-Za-z0-9_-]{22}", payload["share_id"]), payload["share_id"]
    # A malformed / path-like id never reaches any store and stays a 404.
    for bad in ("..%2F..%2Fetc%2Fpasswd", "x" * 200, "abc.def"):
        status, _h, _b = b.http("GET", f"/plan/shared/{bad}")
        assert status == 404, (bad, status)


def test_unknown_share_is_still_404_everywhere(cluster) -> None:
    _a, b, c = cluster
    for worker in (b, c):
        status, _h, _b = worker.http("GET", f"/plan/shared/{secrets.token_urlsafe(16)}")
        assert status == 404


# ---------------------------------------------------------------------------
# Plan results (on-screen dashboard, /plan/<id>, sheets-url)
# ---------------------------------------------------------------------------


def test_plan_result_written_by_one_worker_is_served_by_the_others(cluster) -> None:
    a, b, c = cluster
    plan_id = uuid.uuid4().hex
    assert a.cmd(op="store_plan_result", plan_id=plan_id, data={"client_name": "PR Co"})["ok"]
    status, _h, raw = b.http("GET", f"/api/plan-results/{plan_id}")
    assert status == 200, raw
    assert json.loads(raw)["plan_id"] == plan_id
    status, _h, _body = c.http("GET", f"/plan/{plan_id}")
    assert status == 200
    status, _h, raw = c.http("GET", f"/api/plan/sheets-url?plan_id={plan_id}")
    assert status == 200, raw
    assert json.loads(raw)["ready"] is False
    # The generating worker attaches the Sheet URL later; C already cached the
    # result without it and must still report it.
    assert a.cmd(op="set_sheets_url", plan_id=plan_id, url="https://sheets.example/x")["ok"]
    status, _h, raw = c.http("GET", f"/api/plan/sheets-url?plan_id={plan_id}")
    assert status == 200, raw
    assert json.loads(raw) == {
        "plan_id": plan_id,
        "sheets_url": "https://sheets.example/x",
        "ready": True,
    }


# ---------------------------------------------------------------------------
# Async generation jobs: status, download, QA override-ack
# ---------------------------------------------------------------------------


def _completed_job(worker: Worker, session: str) -> str:
    job_id = uuid.uuid4().hex
    reply = worker.cmd(
        op="make_job",
        job_id=job_id,
        session=session,
        status="completed",
        bundle_qa=_BUNDLE_QA,
        result_b64=base64.b64encode(_ZIP).decode(),
        filename="Crossworker_Media_Plan.zip",
    )
    assert reply["ok"], reply
    return job_id


def test_completed_job_poll_and_download_work_on_other_workers(cluster) -> None:
    a, b, c = cluster
    job_id = _completed_job(a, _SESSION)
    status, _h, raw = b.http(
        "GET", f"/api/jobs/{job_id}", headers={"Accept": "application/json"}
    )
    assert status == 200, raw
    payload = json.loads(raw)
    assert payload["status"] == "completed"
    assert payload["qa_status"] == "critical"
    assert payload["filename"] == "Crossworker_Media_Plan.zip"

    status, headers, body = c.http("GET", f"/api/jobs/{job_id}")
    assert status == 200, body[:200]
    assert body == _ZIP
    assert "Crossworker_Media_Plan.zip" in (headers.get("Content-Disposition") or "")


def test_qa_ack_works_on_a_worker_that_did_not_run_the_job(cluster) -> None:
    a, b, c = cluster
    job_id = _completed_job(a, _SESSION)
    status, _h, raw = b.http(
        "POST", f"/api/jobs/{job_id}/qa-ack", {"acknowledged_by": "ops@joveo.com"}, _post_headers()
    )
    assert status == 200, raw
    assert json.loads(raw)["ok"] is True
    # The acknowledgement is state every worker can see, not a local dict write.
    record = c.cmd(op="job_record", job_id=job_id)["record"] or {}
    assert record.get("_qa_acknowledged_by") == "ops@joveo.com"


def test_qa_ack_on_another_worker_still_rejects_a_different_session(cluster) -> None:
    a, b, _c = cluster
    job_id = _completed_job(a, _SESSION)
    other = f"{secrets.token_hex(16)}.{int(time.time()) + 3600}"
    headers = {
        "Cookie": f"csrf_token={other}",
        "X-CSRF-Token": other,
        "Origin": "http://localhost",
    }
    status, _h, raw = b.http(
        "POST", f"/api/jobs/{job_id}/qa-ack", {"acknowledged_by": "x@y.z"}, headers
    )
    assert status == 403, raw


def test_in_flight_job_progress_is_visible_and_session_locked(cluster) -> None:
    a, b, c = cluster
    job_id = uuid.uuid4().hex
    assert a.cmd(op="make_job", job_id=job_id, session=_SESSION, status="processing")["ok"]
    status, _h, raw = b.http(
        "GET",
        f"/api/jobs/{job_id}",
        headers={"Accept": "application/json", "Cookie": f"nova_session={_SESSION}"},
    )
    assert status == 200, raw
    assert json.loads(raw)["progress_pct"] == 40
    status, _h, _raw = c.http(
        "GET",
        f"/api/jobs/{job_id}",
        headers={"Accept": "application/json", "Cookie": "nova_session=someone-else"},
    )
    assert status == 403


# ---------------------------------------------------------------------------
# Restart / deploy survival through the durable (Supabase cache table) layer
# ---------------------------------------------------------------------------


def test_state_survives_a_restart_onto_an_empty_disk(log_dir: str) -> None:
    fake = FakePostgrest()
    durable_env = {
        "NOVA_STATE_SUPABASE_URL": fake.url,
        "NOVA_STATE_SUPABASE_KEY": "test-service-key",
    }
    try:
        with tempfile.TemporaryDirectory(prefix="nova_state_old_") as old_dir:
            (writer,) = _start([("W", old_dir, durable_env)], log_dir)
            try:
                status, payload = _share(writer, {"summary": {"industry": "Media"}}, "Durable Media Co")
                assert status == 200, payload
                share_id = payload["share_id"]
                plan_id = uuid.uuid4().hex
                assert writer.cmd(
                    op="store_plan_result", plan_id=plan_id, data={"client_name": "Durable"}
                )["ok"]
                job_id = _completed_job(writer, _SESSION)
                assert writer.cmd(op="flush", timeout=10)["flushed"] is True
            finally:
                writer.stop()

        # Nothing the old process wrote may be readable in the clear by anyone
        # holding the (public) anon key: no ids, no client names in the rows.
        dump = json.dumps(fake.rows)
        for secret in (share_id, plan_id, job_id, "Durable Media Co"):
            assert secret not in dump, f"{secret!r} stored in the clear"

        with tempfile.TemporaryDirectory(prefix="nova_state_new_") as new_dir:
            (reader,) = _start([("R", new_dir, durable_env)], log_dir)
            try:
                status, _h, body = reader.http("GET", f"/plan/shared/{share_id}")
                assert status == 200, status
                assert b"Durable Media Co" in body
                status, _h, raw = reader.http("GET", f"/api/plan-results/{plan_id}")
                assert status == 200, raw
                status, _h, raw = reader.http(
                    "GET", f"/api/jobs/{job_id}", headers={"Accept": "application/json"}
                )
                assert status == 200, raw
                assert json.loads(raw)["qa_status"] == "critical"
                status, _h, raw = reader.http(
                    "POST",
                    f"/api/jobs/{job_id}/qa-ack",
                    {"acknowledged_by": "ops@joveo.com"},
                    _post_headers(),
                )
                assert status == 200, raw
            finally:
                reader.stop()
    finally:
        fake.close()
