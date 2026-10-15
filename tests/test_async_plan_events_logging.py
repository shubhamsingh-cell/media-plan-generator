"""The async generation path must write the same durable plan_events row the
sync /api/generate path writes -- exactly once per job.

Context: only the sync handler ever called ``app.log_request()``. Every plan
generated through the wizard goes through the async worker
(``_run_async_generate``, X-Async: true), so the Supabase ``plan_events``
table stopped at 25 rows (last 2026-08-04) while ~85 real plans were
generated, and failed async plans were recorded nowhere durable. Usage
reporting had to fall back to the Slack notifier and the
``nova_generated_plans`` ZIP table.

This file pins:

  1. ``log_request`` dedupe -- one terminal row (success/error) per request
     dict, so the async worker's success + failure branches, or the sync and
     async paths sharing one ``data`` dict, can never double-count a job.
  2. ``plan_id`` -- the async job id wins, so plan_events joins to
     ``nova_generated_plans.job_id``.
  3. End-to-end against a real ``app.ThreadedHTTPServer`` (same pattern as
     tests/test_bundle_qa_gate.py): an async success and an async failure
     each produce exactly ONE plan_events insert for that job.

The Supabase client is a local fake -- nothing touches the network.

Runs under pytest, or standalone:
``python3 -m pytest tests/test_async_plan_events_logging.py``.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402


# ---------------------------------------------------------------------------
# Fake Supabase client
# ---------------------------------------------------------------------------
class _FakeQuery:
    """Chainable stand-in for a supabase-py query builder: records inserts,
    answers every other builder call (select/eq/upsert/order/...) with
    itself, and ``execute()`` returns an empty result."""

    def __init__(self, client: "_FakeSupabase", table: str) -> None:
        self._client = client
        self._table = table

    def insert(self, row, *args, **kwargs):
        self._client.record(self._table, row)
        return self

    def execute(self):
        return SimpleNamespace(data=[], count=0)

    def __getattr__(self, name):
        return lambda *args, **kwargs: self


class _FakeSupabase:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.inserts: list = []

    def record(self, table: str, row: dict) -> None:
        with self._lock:
            self.inserts.append((table, dict(row)))

    def table(self, name: str) -> _FakeQuery:
        return _FakeQuery(self, name)

    def __getattr__(self, name):
        return lambda *args, **kwargs: _FakeQuery(self, name)

    def plan_events(self, plan_id: str) -> list:
        with self._lock:
            return [
                row
                for table, row in self.inserts
                if table == "plan_events" and row.get("plan_id") == plan_id
            ]


@pytest.fixture
def fake_supabase() -> Iterator[_FakeSupabase]:
    fake = _FakeSupabase()
    with mock.patch("supabase_client.get_client", return_value=fake):
        yield fake


@pytest.fixture(autouse=True)
def _isolated_request_log(tmp_path, monkeypatch):
    """log_request also appends to data/request_log.json -- point it at a
    temp file so this suite never touches the checkout's copy."""
    log_file = tmp_path / "request_log.json"
    monkeypatch.setattr(app_module, "REQUEST_LOG_FILE", str(log_file))
    monkeypatch.setattr(app_module, "REQUEST_LOG_LOCK", str(log_file) + ".lock")


# ---------------------------------------------------------------------------
# A. log_request -- unit
# ---------------------------------------------------------------------------
def test_second_terminal_event_for_same_request_is_not_logged(fake_supabase):
    data = {"client_name": "Dedupe Co", "requester_email": "qa@joveo.com"}
    first = app_module.log_request(data, "success", file_size=10, plan_id="job-a")
    second = app_module.log_request(data, "error", error_msg="late", plan_id="job-a")
    assert first is not None
    assert second is None
    rows = fake_supabase.plan_events("job-a")
    assert [r["event_type"] for r in rows] == ["plan_generated"]


def test_plan_id_kwarg_wins_over_request_id_fallback(fake_supabase):
    data = {"client_name": "Join Co", "_request_id": "req-123"}
    app_module.log_request(data, "success", plan_id="job-b")
    assert len(fake_supabase.plan_events("job-b")) == 1
    assert fake_supabase.plan_events("req-123") == []


def test_supabase_insert_failure_never_raises(monkeypatch):
    broken = mock.MagicMock()
    broken.table.return_value.insert.return_value.execute.side_effect = RuntimeError(
        "supabase down"
    )
    with mock.patch("supabase_client.get_client", return_value=broken):
        entry = app_module.log_request({"client_name": "Down Co"}, "error")
    assert entry is not None
    assert entry["status"] == "error"


# ---------------------------------------------------------------------------
# B. End-to-end against a real live server
# ---------------------------------------------------------------------------
_GEN_PAYLOAD = {
    "client_name": "Plan Events Test Co",
    "requester_name": "QA Bot",
    "requester_email": "qa@joveo.com",
    "target_roles": ["Warehouse Associate"],
    "locations": ["Columbus, OH"],
    "budget": 20000,
    "campaign_duration": "4 weeks",
}
_ORIGIN_HEADERS = {"Content-Type": "application/json", "Origin": "http://localhost"}
_CSRF = f"plan-events-test-token.{int(time.time()) + 3600}"
_COOKIE_HEADERS = {"Cookie": f"csrf_token={_CSRF}"}


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    """Real app.ThreadedHTTPServer on an ephemeral port -- duplicated from
    tests/test_bundle_qa_gate.py to keep this file self-contained."""
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="plan-events-test-http-server"
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
    yield port
    server.shutdown()
    server.server_close()


def _http(port: int, method: str, path: str, body=None, headers=None, timeout=30.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        conn.request(method, path, body=data, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, dict(resp.getheaders()), raw
    finally:
        conn.close()


def _wait_for_a_free_generate_slot(timeout: float = 15.0) -> None:
    """Slot release trails the previous job's response -- see the same
    helper in tests/test_bundle_qa_gate.py."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if app_module._generate_slots.acquire(blocking=False):
            app_module._generate_slots.release()
            return
        time.sleep(0.1)


def _reset_generate_rate_limit() -> None:
    """/api/generate's IP limiter is process-global across test files."""
    with app_module._rl_generate._lock:
        app_module._rl_generate._requests.pop("127.0.0.1", None)


def _submit_async(port: int, tag: str) -> str:
    payload = dict(_GEN_PAYLOAD)
    payload["client_name"] = f"{_GEN_PAYLOAD['client_name']} ({tag})"
    headers = dict(_ORIGIN_HEADERS)
    headers.update(_COOKIE_HEADERS)
    headers["X-Async"] = "true"
    _reset_generate_rate_limit()
    _wait_for_a_free_generate_slot()
    status, _, raw = _http(port, "POST", "/api/generate", body=payload, headers=headers)
    assert status == 200, f"submit failed: {status} {raw[:400]!r}"
    return json.loads(raw)["job_id"]


def _poll_until_done(port: int, job_id: str, timeout: float = 60.0) -> dict:
    deadline = time.time() + timeout
    headers = dict(_COOKIE_HEADERS)
    headers["Accept"] = "application/json"
    last = None
    while time.time() < deadline:
        status, _, raw = _http(port, "GET", f"/api/jobs/{job_id}", headers=headers)
        assert status == 200, f"poll failed: {status} {raw[:400]!r}"
        last = json.loads(raw)
        if last["status"] in ("completed", "failed"):
            return last
        time.sleep(0.1)
    pytest.fail(f"job {job_id} did not finish within {timeout}s: {last}")


def _join_worker(job_id: str, timeout: float = 60.0) -> None:
    """The plan_events row is written AFTER the job flips to completed (so
    the Supabase round-trip never delays the user) -- join the worker
    thread so the count below is final, not a race."""
    for t in threading.enumerate():
        if t.name == f"async-gen-{job_id}":
            t.join(timeout)
            assert not t.is_alive(), f"worker for {job_id} still running"


def test_async_success_writes_exactly_one_plan_generated_row(
    live_server, fake_supabase
):
    job_id = _submit_async(live_server, "success")
    result = _poll_until_done(live_server, job_id)
    _join_worker(job_id)
    assert result["status"] == "completed", result
    rows = fake_supabase.plan_events(job_id)
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["event_type"] == "plan_generated"
    # The pipeline title-cases client names ("(success)" -> "(Success)").
    assert row["client_name"].lower() == "plan events test co (success)"
    assert row["user_email"] == "qa@joveo.com"
    assert row["file_size_bytes"] > 1000
    assert row["error_message"] is None
    assert row["metadata"]["doc_filename"]


def test_async_failure_writes_exactly_one_plan_failed_row(live_server, fake_supabase):
    # _normalize_dict_roles is the async worker's first step; the X-Async
    # branch returns before the sync handler's own call to it, so this fails
    # the job without touching request validation.
    with mock.patch.object(
        app_module,
        "_normalize_dict_roles",
        side_effect=RuntimeError("injected async failure"),
    ):
        job_id = _submit_async(live_server, "failure")
        result = _poll_until_done(live_server, job_id)
        _join_worker(job_id)
    assert result["status"] == "failed", result
    rows = fake_supabase.plan_events(job_id)
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["event_type"] == "plan_failed"
    assert "injected async failure" in (row["error_message"] or "")
    assert row["client_name"].lower() == "plan events test co (failure)"
