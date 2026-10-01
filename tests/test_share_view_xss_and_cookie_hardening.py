"""Stored-XSS and session-cookie hardening for the share / plan / job routes.

1. ``POST /api/plan/share`` is unauthenticated (the CSRF double-submit token
   is one the caller mints for itself), so every field of ``plan_data`` and the
   client label is attacker-controlled -- and ``GET /plan/shared/<id>`` used to
   interpolate several of them into HTML unescaped (``channels`` as a
   non-list became the channel count verbatim; non-numeric spend / allocation
   / CPC / CPA fell back to ``str(value)``). ``GET /plan/<id>`` likewise
   printed ``allocation_pct`` and the summary counts raw. Every test here
   renders through the real handler and parses the HTML: the only markup
   allowed is the template's own.

2. ``hmac.compare_digest(str, str)`` raises TypeError for non-ASCII strings,
   and http.server decodes headers as latin-1, so a cookie byte >= 0x80 made
   the job poll and qa-ack 500 instead of 403.
"""

from __future__ import annotations

import http.client
import json
import secrets
import socket
import threading
import time
import uuid
from html import escape as html_escape
from html.parser import HTMLParser
from typing import Any, Iterator

import pytest

import app

_SCRIPT = "<script>alert(1)</script>"
_IMG = '"><img src=x onerror=alert(1)>'
_ATTR = "' onmouseover='alert(1)"


class _Markup(HTMLParser):
    """Collect every element and every on* attribute in a page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: list[str] = []
        self.handlers: list[tuple[str, str]] = []
        self.script_bodies: list[str] = []
        self._in_script = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Any]]) -> None:
        self.tags.append(tag)
        self.handlers += [(tag, name) for name, _v in attrs if name.lower().startswith("on")]
        self._in_script = tag == "script"

    def handle_endtag(self, tag: str) -> None:
        if tag == "script":
            self._in_script = False

    def handle_data(self, data: str) -> None:
        if self._in_script:
            self.script_bodies.append(data)


def _assert_no_injected_markup(page: str, allowed_handlers: set[tuple[str, str]]) -> None:
    for raw in (_SCRIPT, "<img", "onerror=alert(1)>", "' onmouseover='"):
        assert raw not in page, f"unescaped payload {raw!r} in page"
    markup = _Markup()
    markup.feed(page)
    assert "img" not in markup.tags
    assert set(markup.handlers) <= allowed_handlers, markup.handlers
    assert all("alert(1)" not in body for body in markup.script_bodies)


@pytest.fixture(scope="module")
def server() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    srv = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="test-xss-server").start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    yield port
    srv.shutdown()
    srv.server_close()


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(app._rl_general, "is_allowed", lambda *a, **k: True)


def _request(
    port: int, method: str, path: str, body: Any = None, headers: dict | None = None
) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        raw = json.dumps(body).encode("utf-8") if body is not None else None
        hdrs = {"Content-Type": "application/json"} if raw is not None else {}
        hdrs.update(headers or {})
        conn.request(method, path, body=raw, headers=hdrs)
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", "replace")
    finally:
        conn.close()


def _csrf_headers(extra_cookie: str = "") -> dict:
    token = f"{secrets.token_hex(16)}.{int(time.time()) + 3600}"
    cookie = f"csrf_token={token}" + (f"; {extra_cookie}" if extra_cookie else "")
    return {"Cookie": cookie, "X-CSRF-Token": token, "Origin": "http://localhost"}


def _hostile_channel(payload: str) -> dict:
    return {
        "name": payload,
        "channel": payload,
        "spend": payload,
        "budget": payload,
        "allocation_pct": payload,
        "cpc": payload,
        "cost_per_click": payload,
        "cpa": payload,
        "cost_per_apply": payload,
    }


def _hostile_summary(payload: str, channels: Any) -> dict:
    return {
        "channels": channels,
        "recommended_channels": channels,
        "total_budget": payload,
        "budget_range": payload,
        "industry": payload,
        "est_applications": payload,
        "estimated_applications": payload,
        "est_hires": payload,
        "estimated_hires": payload,
    }


@pytest.mark.parametrize("payload", [_SCRIPT, _IMG, _ATTR], ids=["script", "img", "attr"])
@pytest.mark.parametrize("channels_shape", ["string", "list", "dict"])
def test_shared_plan_view_escapes_every_field(server: int, payload: str, channels_shape: str) -> None:
    channels: Any = {
        "string": payload,
        "list": [_hostile_channel(payload), _hostile_channel(payload)],
        "dict": {"x": payload},
    }[channels_shape]
    plan = {
        "summary": _hostile_summary(payload, channels),
        "channels": channels,
        "budget_range": payload,
        "industry": payload,
        "industry_label": payload,
        "hire_volume": payload,
    }
    status, raw = _request(
        server, "POST", "/api/plan/share", {"plan_data": plan, "client": payload}, _csrf_headers()
    )
    assert status == 200, raw
    share_id = json.loads(raw)["share_id"]

    status, page = _request(server, "GET", f"/plan/shared/{share_id}")
    assert status == 200, page[:300]
    _assert_no_injected_markup(page, allowed_handlers={("button", "onclick")})


@pytest.mark.parametrize("payload", [_SCRIPT, _IMG, _ATTR], ids=["script", "img", "attr"])
def test_shared_plan_feedback_is_escaped_on_render(server: int, payload: str) -> None:
    status, raw = _request(
        server,
        "POST",
        "/api/plan/share",
        {"plan_data": {"summary": {"industry": "Retail"}}, "client": "Co"},
        _csrf_headers(),
    )
    assert status == 200, raw
    share_id = json.loads(raw)["share_id"]
    status, raw = _request(
        server,
        "POST",
        "/api/plan/feedback",
        {"share_id": share_id, "name": payload, "comment": payload},
        _csrf_headers(),
    )
    assert status == 200, raw
    status, page = _request(server, "GET", f"/plan/shared/{share_id}")
    assert status == 200
    assert html_escape(payload) in page  # the comment is shown, escaped
    _assert_no_injected_markup(page, allowed_handlers={("button", "onclick")})


@pytest.mark.parametrize("payload", [_SCRIPT, _IMG, _ATTR], ids=["script", "img", "attr"])
def test_shared_plan_view_survives_a_non_object_summary(server: int, payload: str) -> None:
    plan = {"summary": payload, "plan_summary": [payload], "channels": payload}
    status, raw = _request(
        server, "POST", "/api/plan/share", {"plan_data": plan, "client": payload}, _csrf_headers()
    )
    assert status == 200, raw
    status, page = _request(server, "GET", f"/plan/shared/{json.loads(raw)['share_id']}")
    assert status == 200, page[:300]
    _assert_no_injected_markup(page, allowed_handlers={("button", "onclick")})


@pytest.mark.parametrize("payload", [_SCRIPT, _IMG, _ATTR], ids=["script", "img", "attr"])
def test_plan_direct_view_escapes_every_field(server: int, payload: str) -> None:
    plan_id = uuid.uuid4().hex
    hostile = {
        "summary": {
            "channels": [
                {
                    "name": payload,
                    "budget": payload,
                    "allocation_pct": payload,
                    "cpc_range": payload,
                    "cpa_range": payload,
                }
            ],
            "total_channels": payload,
            "est_applications": payload,
            "est_hires": payload,
        },
        "metadata": {
            "client_name": payload,
            "industry_label": payload,
            "total_budget": payload,
            "generated_at": payload,
        },
    }
    with app._plan_results_lock:
        app._plan_results_store[plan_id] = {"data": hostile, "created": time.time()}
    try:
        status, page = _request(server, "GET", f"/plan/{plan_id}")
        assert status == 200, page[:300]
        _assert_no_injected_markup(page, allowed_handlers=set())
    finally:
        with app._plan_results_lock:
            app._plan_results_store.pop(plan_id, None)


# ---------------------------------------------------------------------------
# Non-ASCII session cookies: 403, never a 500
# ---------------------------------------------------------------------------


def _in_flight_job(session: str) -> str:
    job_id = uuid.uuid4().hex
    with app._generation_jobs_lock:
        app._generation_jobs[job_id] = {
            "status": "processing",
            "progress_pct": 10,
            "status_message": "x",
            "created": time.time(),
            "result_bytes": None,
            "error": None,
            "_session_token": session,
        }
    return job_id


def test_job_poll_with_non_ascii_session_cookie_is_403_not_500(server: int) -> None:
    job_id = _in_flight_job(secrets.token_hex(16))
    try:
        status, body = _request(
            server,
            "GET",
            f"/api/jobs/{job_id}",
            headers={"Accept": "application/json", "Cookie": "nova_session=café"},
        )
        assert status == 403, body
    finally:
        with app._generation_jobs_lock:
            app._generation_jobs.pop(job_id, None)


def test_qa_ack_with_non_ascii_session_cookie_is_403_not_500(server: int) -> None:
    job_id = _in_flight_job(secrets.token_hex(16))
    try:
        status, body = _request(
            server,
            "POST",
            f"/api/jobs/{job_id}/qa-ack",
            {"acknowledged_by": "x@joveo.com"},
            _csrf_headers(extra_cookie="nova_session=café"),
        )
        assert status == 403, body
    finally:
        with app._generation_jobs_lock:
            app._generation_jobs.pop(job_id, None)


def test_matching_non_ascii_session_cookie_still_owns_its_job(server: int) -> None:
    """The fix must compare, not just refuse: an identical non-ASCII token
    (latin-1 decoded on both sides) still matches."""
    job_id = _in_flight_job("café")
    try:
        status, body = _request(
            server,
            "GET",
            f"/api/jobs/{job_id}",
            headers={"Accept": "application/json", "Cookie": "nova_session=café"},
        )
        assert status == 200, body
    finally:
        with app._generation_jobs_lock:
            app._generation_jobs.pop(job_id, None)
