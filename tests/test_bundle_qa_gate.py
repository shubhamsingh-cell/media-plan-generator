"""Generation-time QA gate: bundle_qa criticals must gate the DOWNLOAD,
never generation itself (app.py wiring around bundle_qa.summarize_findings).

Context: bundle_qa.run_bundle_qa has run on every generated bundle since
S93/S94, but was "isolated: never blocks or fails generation, only observes
and records" -- findings went to the server log and a summary onto the job
record, and nothing else ever read it. A real client bundle shipped on
2026-07-23 carrying 5 critical findings nobody saw: the signal existed, in
the log, and no human read it. This file covers the fix:

  1. ``bundle_qa.summarize_findings`` -- the pure reducer that turns a
     findings list into {qa_status, critical_count, warn_count, codes,
     findings}, used by every caller below instead of each one re-deriving
     counts itself.
  2. ``app._build_bundle_qa_status_header`` -- the sync /api/generate
     path's X-Bundle-QA-Status response header, built in the exact same
     spirit/convention as the pre-existing X-Narrative-Status header (see
     tests/test_narrative_status_header.py).
  3. ``app._bundle_qa_response_fields`` -- the shape promoted onto the
     async job-poll (/api/jobs/<id>) JSON response.
  4. End-to-end, against a real ``app.ThreadedHTTPServer`` (same
     "integration-lite" pattern as tests/test_generate_concurrency.py):
       - a clean bundle downloads with no friction;
       - a bundle with criticals surfaces them in both the async poll
         response and the sync response header, AND -- this is the part
         that matters most -- is STILL fully produced and retrievable
         (the false-positive guard: a QA finding must never fail
         generation, only make it impossible to miss);
       - a bundle_qa crash degrades to today's behaviour (qa_status
         "clean", download unaffected) rather than locking anything;
       - POST /api/jobs/<id>/qa-ack records an operator override via the
         same audit_logger.log_event pattern already used for
         bundle_qa.critical_findings.

Runs under pytest, or standalone: ``python3 tests/test_bundle_qa_gate.py``.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Iterator
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402
import audit_logger  # noqa: E402
import bundle_qa  # noqa: E402

# ---------------------------------------------------------------------------
# A. bundle_qa.summarize_findings -- pure unit tests
# ---------------------------------------------------------------------------
def test_summarize_findings_empty_list_is_clean():
    summary = bundle_qa.summarize_findings([])
    assert summary["qa_status"] == "clean"
    assert summary["critical_count"] == 0
    assert summary["warn_count"] == 0
    assert summary["codes"] == []
    assert summary["findings"] == []


def test_summarize_findings_none_input_is_clean():
    """Must never raise -- a caller with no findings list (module not
    loaded, or a crash upstream) should get a safe default, not a crash."""
    summary = bundle_qa.summarize_findings(None)
    assert summary["qa_status"] == "clean"
    assert summary["critical_count"] == 0


def test_summarize_findings_warn_only_is_warnings_not_critical():
    findings = [
        {"severity": "warn", "code": "w1", "message": "m", "location": "l"},
        {"severity": "warn", "code": "w2", "message": "m", "location": "l"},
    ]
    summary = bundle_qa.summarize_findings(findings)
    assert summary["qa_status"] == "warnings"
    assert summary["critical_count"] == 0
    assert summary["warn_count"] == 2
    assert summary["codes"] == []  # codes are critical-only


def test_summarize_findings_any_critical_is_critical():
    findings = [
        {"severity": "warn", "code": "w1", "message": "m", "location": "l"},
        {"severity": "critical", "code": "c1", "message": "m", "location": "l"},
        {"severity": "critical", "code": "c2", "message": "m", "location": "l"},
    ]
    summary = bundle_qa.summarize_findings(findings)
    assert summary["qa_status"] == "critical"
    assert summary["critical_count"] == 2
    assert summary["warn_count"] == 1
    assert summary["codes"] == ["c1", "c2"]


def test_summarize_findings_caps_findings_at_25():
    findings = [
        {"severity": "critical", "code": f"c{i}", "message": "m", "location": "l"}
        for i in range(40)
    ]
    summary = bundle_qa.summarize_findings(findings)
    assert summary["critical_count"] == 40  # counts reflect the TRUE total
    assert len(summary["findings"]) == 25  # only the response body is capped


# ---------------------------------------------------------------------------
# B. app._build_bundle_qa_status_header / app._bundle_qa_response_fields
# ---------------------------------------------------------------------------
def test_status_header_none_input_is_clean_json():
    assert app_module._build_bundle_qa_status_header(None) == '{"qa_status":"clean"}'
    assert app_module._build_bundle_qa_status_header({}) == '{"qa_status":"clean"}'


def test_status_header_carries_critical_verdict():
    summary = bundle_qa.summarize_findings(
        [{"severity": "critical", "code": "x", "message": "m", "location": "l"}]
    )
    header = app_module._build_bundle_qa_status_header(summary)
    assert "\n" not in header and "\r" not in header
    parsed = json.loads(header)
    assert parsed["qa_status"] == "critical"
    assert parsed["critical_count"] == 1
    assert parsed["codes"] == ["x"]


def test_status_header_is_ascii_safe_and_capped():
    summary = {
        "qa_status": "critical",
        "critical_count": 1,
        "warn_count": 0,
        "codes": [f"code_{i}" for i in range(50)],
    }
    header = app_module._build_bundle_qa_status_header(summary)
    header.encode("ascii")  # must not raise
    assert len(header) <= 800


def test_response_fields_default_to_clean_when_summary_missing():
    fields = app_module._bundle_qa_response_fields(None)
    assert fields == {
        "qa_status": "clean",
        "qa_critical_count": 0,
        "qa_codes": [],
        "qa_findings": [],
    }


def test_response_fields_pass_through_a_real_summary():
    summary = bundle_qa.summarize_findings(
        [{"severity": "critical", "code": "x", "message": "m", "location": "l"}]
    )
    fields = app_module._bundle_qa_response_fields(summary)
    assert fields["qa_status"] == "critical"
    assert fields["qa_critical_count"] == 1
    assert fields["qa_codes"] == ["x"]
    assert fields["qa_findings"][0]["code"] == "x"


# ---------------------------------------------------------------------------
# C. End-to-end against a real live server
# ---------------------------------------------------------------------------
_GEN_PAYLOAD = {
    "client_name": "QA Gate Test Co",
    "requester_name": "QA Bot",
    "requester_email": "qa@joveo.com",
    "target_roles": ["Warehouse Associate"],
    "locations": ["Columbus, OH"],
    "budget": 20000,
    "campaign_duration": "4 weeks",
}
_ORIGIN_HEADERS = {"Content-Type": "application/json", "Origin": "http://localhost"}
_CSRF = f"qa-gate-test-token.{int(time.time()) + 3600}"
_COOKIE_HEADERS = {"Cookie": f"csrf_token={_CSRF}"}


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    """Start a real app.ThreadedHTTPServer on an ephemeral port -- same
    pattern as tests/test_generate_concurrency.py's fixture of the same
    name (duplicated, not imported, to keep this file self-contained)."""
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="qa-gate-test-http-server"
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
    # This module runs several REAL /api/generate calls (async + sync).
    # Slot release is NOT synchronous with the client seeing a completed/
    # 200 response -- do_POST's `finally` runs trailing post-response
    # work (metrics, Slack notify, Supabase writes) after the response is
    # already sent, and _generate_slots is a module-level pool shared by
    # every test file in this pytest run (test_generate_concurrency.py's
    # own docstring covers why). Waiting here for all slots to be free
    # before tearing down keeps this file from starving whichever test
    # module's concurrency-cap tests happen to run right after it.
    _qa_gate_deadline = time.time() + 30.0
    _qa_gate_acquired = 0
    try:
        while (
            _qa_gate_acquired < app_module._MAX_CONCURRENT_GENERATE
            and time.time() < _qa_gate_deadline
        ):
            if app_module._generate_slots.acquire(blocking=False):
                _qa_gate_acquired += 1
            else:
                time.sleep(0.05)
    finally:
        for _ in range(_qa_gate_acquired):
            app_module._generate_slots.release()
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
    """Slot release trails the client's response (do_POST's `finally`
    does metrics/Slack/Supabase work after the bytes are already sent),
    so a real call right after a prior one's "completed" poll can still
    hit a transient 429 if _MAX_CONCURRENT_GENERATE (default 2) is
    briefly fully held. Bounded, non-destructive poll before each real
    call in this module."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if app_module._generate_slots.acquire(blocking=False):
            app_module._generate_slots.release()
            return
        time.sleep(0.1)


def _reset_generate_rate_limit() -> None:
    """/api/generate's _rl_generate limiter (10 real requests/60s, keyed
    by client IP) is process-global and shared with every other test
    file's real /api/generate calls in the same pytest run (see
    test_generate_concurrency.py's test_async_generate_completes_with_dict_shaped_roles
    docstring for the exact same issue). This file alone makes 7 real
    calls; clearing this test run's own IP bucket before each one keeps
    the assertions about THIS gate, not about pytest's collection order
    or how many other suites shared the last 60 seconds."""
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


def _poll_until_done(port: int, job_id: str, timeout: float = 30.0) -> dict:
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
    pytest.fail(f"job {job_id} did not complete within {timeout}s: {last}")


def _download(port: int, job_id: str):
    return _http(port, "GET", f"/api/jobs/{job_id}", headers=dict(_COOKIE_HEADERS))


def test_clean_bundle_downloads_with_no_friction(live_server):
    port = live_server
    with mock.patch("bundle_qa.run_bundle_qa", return_value=[]):
        job_id = _submit_async(port, "clean")
        result = _poll_until_done(port, job_id)
    assert result["status"] == "completed"
    assert result["qa_status"] == "clean"
    assert result["qa_critical_count"] == 0
    assert result["qa_codes"] == []
    status, _, raw = _download(port, job_id)
    assert status == 200
    assert len(raw) > 1000


def test_critical_bundle_surfaces_findings_and_still_downloads(live_server):
    """The core requirement: criticals are unmissable in the response, but
    the bundle is still fully produced and retrievable -- generation
    itself is NEVER gated, only the operator's awareness is."""
    port = live_server
    fake_finding = {
        "severity": "critical",
        "code": "test_injected_critical",
        "message": "Injected critical for gate test",
        "location": "test!row1",
    }
    with mock.patch("bundle_qa.run_bundle_qa", return_value=[fake_finding]):
        job_id = _submit_async(port, "critical")
        result = _poll_until_done(port, job_id)
    assert result["status"] == "completed"
    assert result["qa_status"] == "critical"
    assert result["qa_critical_count"] == 1
    assert "test_injected_critical" in result["qa_codes"]
    assert any(
        f.get("code") == "test_injected_critical" for f in result["qa_findings"]
    )
    # FALSE-POSITIVE GUARD: still fully produced and retrievable.
    status, _, raw = _download(port, job_id)
    assert status == 200
    assert len(raw) > 1000


def test_bundle_qa_crash_degrades_gracefully(live_server):
    """A broken linter must degrade to today's behaviour (qa_status
    "clean", i.e. unreported/unknown -- never a locked download)."""
    port = live_server
    with mock.patch("bundle_qa.run_bundle_qa", side_effect=RuntimeError("boom")):
        job_id = _submit_async(port, "crash")
        result = _poll_until_done(port, job_id)
    assert result["status"] == "completed"
    assert result["qa_status"] == "clean"
    status, _, raw = _download(port, job_id)
    assert status == 200
    assert len(raw) > 1000


def test_sync_path_exposes_bundle_qa_status_header(live_server):
    port = live_server
    fake_finding = {
        "severity": "critical",
        "code": "sync_test_critical",
        "message": "m",
        "location": "l",
    }
    payload = dict(_GEN_PAYLOAD)
    payload["client_name"] = "QA Gate Sync Test Co"
    _reset_generate_rate_limit()
    _wait_for_a_free_generate_slot()
    with mock.patch("bundle_qa.run_bundle_qa", return_value=[fake_finding]):
        status, hdrs, raw = _http(
            port, "POST", "/api/generate", body=payload, headers=_ORIGIN_HEADERS
        )
    assert status == 200
    assert len(raw) > 1000  # the bundle is still fully produced
    qa_header = hdrs.get("X-Bundle-QA-Status") or hdrs.get("x-bundle-qa-status")
    assert qa_header, f"X-Bundle-QA-Status header missing; got {sorted(hdrs)}"
    parsed = json.loads(qa_header)
    assert parsed["qa_status"] == "critical"
    assert parsed["critical_count"] == 1
    assert "sync_test_critical" in parsed["codes"]


def test_sync_path_clean_bundle_header_is_clean(live_server):
    port = live_server
    payload = dict(_GEN_PAYLOAD)
    payload["client_name"] = "QA Gate Sync Clean Co"
    _reset_generate_rate_limit()
    _wait_for_a_free_generate_slot()
    with mock.patch("bundle_qa.run_bundle_qa", return_value=[]):
        status, hdrs, raw = _http(
            port, "POST", "/api/generate", body=payload, headers=_ORIGIN_HEADERS
        )
    assert status == 200
    assert len(raw) > 1000
    qa_header = hdrs.get("X-Bundle-QA-Status") or hdrs.get("x-bundle-qa-status")
    assert qa_header
    assert json.loads(qa_header)["qa_status"] == "clean"


def test_override_acknowledgement_writes_audit_event(live_server):
    port = live_server
    fake_finding = {
        "severity": "critical",
        "code": "ack_test_critical",
        "message": "m",
        "location": "l",
    }
    with mock.patch("bundle_qa.run_bundle_qa", return_value=[fake_finding]):
        job_id = _submit_async(port, "ack")
        _poll_until_done(port, job_id)

    before = audit_logger.get_recent_events(
        limit=5000, action_filter="bundle_qa.override_acknowledged"
    )

    headers = dict(_ORIGIN_HEADERS)
    headers.update(_COOKIE_HEADERS)
    headers["X-CSRF-Token"] = _CSRF
    status, _, raw = _http(
        port,
        "POST",
        f"/api/jobs/{job_id}/qa-ack",
        body={"acknowledged_by": "qa-test@joveo.com"},
        headers=headers,
    )
    assert status == 200, raw
    resp = json.loads(raw)
    assert resp["ok"] is True

    after = audit_logger.get_recent_events(
        limit=5000, action_filter="bundle_qa.override_acknowledged"
    )
    assert len(after) == len(before) + 1
    ev = after[-1]
    assert ev["resource"] == job_id
    assert ev["actor"] == "qa-test@joveo.com"
    assert ev["details"]["critical_count"] == 1
    assert "ack_test_critical" in ev["details"]["codes"]


def test_override_acknowledgement_rejects_wrong_session(live_server):
    """IDOR guard: a caller presenting a DIFFERENT session's cookie must
    not be able to acknowledge someone else's job."""
    port = live_server
    fake_finding = {
        "severity": "critical",
        "code": "ack_idor_test",
        "message": "m",
        "location": "l",
    }
    with mock.patch("bundle_qa.run_bundle_qa", return_value=[fake_finding]):
        job_id = _submit_async(port, "ack-idor")
        _poll_until_done(port, job_id)

    wrong_csrf = f"someone-elses-token.{int(time.time()) + 3600}"
    headers = dict(_ORIGIN_HEADERS)
    headers["Cookie"] = f"csrf_token={wrong_csrf}"
    headers["X-CSRF-Token"] = wrong_csrf
    status, _, raw = _http(
        port,
        "POST",
        f"/api/jobs/{job_id}/qa-ack",
        body={"acknowledged_by": "attacker@joveo.com"},
        headers=headers,
    )
    assert status == 403, raw


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
