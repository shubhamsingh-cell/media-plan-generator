"""Stored XSS on /scorecard/<id> (POST /api/plan/scorecard -> GET /scorecard/<id>).

POST /api/plan/scorecard takes an arbitrary plan from any caller (the CSRF
token is one the caller mints). scorecard_generator printed the plan's
currency as the money symbol unescaped -- plan_currency.symbol_for_code
returns ``code.upper() + " "`` for an unknown code, and an upper-cased
``<IMG SRC=X ONERROR="&#97;...">`` is still a live handler -- and the stored
HTML was served verbatim on every later view (including from Supabase).

Every test goes through the real POST + GET handler and parses the page: the
scorecard template has no scripts and no event handlers of its own, so any
<script>, any on* attribute, and any <img>/<svg>-with-handler is injected.
"""

from __future__ import annotations

import http.client
import json
import secrets
import socket
import threading
import time
from html.parser import HTMLParser
from typing import Any, Iterator

import pytest

import app

_VERIFIER = '<IMG SRC=X ONERROR="&#97;&#108;&#101;&#114;&#116;&#40;&#49;&#41;">'
_PAYLOADS = {
    "script": "<script>alert(1)</script>",
    "img": '"><img src=x onerror=alert(1)>',
    "verifier-uppercase-entities": _VERIFIER,
    "attr": "' onmouseover='alert(1)",
    "svg": "<svg/onload=alert(1)>",
}


class _Markup(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: list[str] = []
        self.handlers: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Any]]) -> None:
        self.tags.append(tag)
        self.handlers += [(tag, n) for n, _v in attrs if n.lower().startswith("on")]


def _assert_clean(page: str) -> None:
    markup = _Markup()
    markup.feed(page)
    assert "script" not in markup.tags, "injected <script>"
    assert "img" not in markup.tags, "injected <img>"
    assert not markup.handlers, f"injected event handlers: {markup.handlers}"
    lowered = page.lower()
    for raw in ("<script>alert", "<img", "<svg/onload", "' onmouseover='"):
        assert raw not in lowered, f"unescaped payload {raw!r}"


def _hostile_plan(p: str) -> dict:
    return {
        "currency": p,
        "currency_code": p,
        "client_name": p,
        "job_title": p,
        "title": p,
        "target_roles": [{"title": p}],
        "roles": [p],
        "locations": [p],
        "location": p,
        "industry_label": p,
        "industry": p,
        "budget": 420000,  # numeric: forces the currency symbol into the output
        "summary": {
            "industry": p,
            "total_budget": p,
            "channels": [
                {"name": p, "label": p, "percentage": p, "budget": p, "dollar_amount": p},
                {"name": f"{p}_with_underscore", "percentage": 40, "dollar_amount": 1500},
                p,
            ],
        },
        "_budget_allocation": {
            "metadata": {"total_budget": 250000},
            "channel_allocations": {
                p: {"dollar_amount": 1000, "percentage": 50},
                f"{p}_snake": {"dollar_amount": p, "percentage": p},
                "linkedin": {"dollar_amount": 5000, "percentage": 50},
            },
        },
    }


@pytest.fixture(scope="module")
def server() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    srv = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="test-scorecard-xss").start()
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
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(app._rl_general, "is_allowed", lambda *a, **k: True)
    monkeypatch.setattr(app._generate_slots, "_slot_dir", str(tmp_path))
    monkeypatch.setattr(app, "_supabase_rest", lambda *a, **k: None)
    with app._scorecards_lock:
        app._scorecards.clear()


def _request(port: int, method: str, path: str, body: Any = None) -> tuple[int, str]:
    token = f"{secrets.token_hex(16)}.{int(time.time()) + 3600}"
    headers = {"Cookie": f"csrf_token={token}", "X-CSRF-Token": token, "Origin": "http://localhost"}
    raw = None
    if body is not None:
        raw = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        conn.request(method, path, body=raw, headers=headers)
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", "replace")
    finally:
        conn.close()


def _create(port: int, plan: dict) -> str:
    status, raw = _request(port, "POST", "/api/plan/scorecard", {"plan_data": plan})
    assert status == 200, raw[:300]
    return json.loads(raw)["share_id"]


@pytest.mark.parametrize("payload", list(_PAYLOADS.values()), ids=list(_PAYLOADS))
def test_scorecard_escapes_every_field(server: int, payload: str) -> None:
    share_id = _create(server, _hostile_plan(payload))
    status, page = _request(server, "GET", f"/scorecard/{share_id}")
    assert status == 200, page[:300]
    _assert_clean(page)


def test_verifier_currency_payload_falls_back_to_a_known_symbol(server: int) -> None:
    share_id = _create(
        server, {"currency": _VERIFIER, "budget": 420000, "roles": ["Nurse"], "locations": ["Austin"]}
    )
    status, page = _request(server, "GET", f"/scorecard/{share_id}")
    assert status == 200
    _assert_clean(page)
    assert "$420,000" in page


def test_known_currencies_still_render_their_symbol(server: int) -> None:
    for code, symbol in (("GBP", "£"), ("INR", "₹"), ("AED", "AED "), ("BDT", "BDT ")):
        share_id = _create(server, {"currency": code, "budget": 420000, "roles": ["Nurse"]})
        status, page = _request(server, "GET", f"/scorecard/{share_id}")
        assert status == 200
        assert f"{symbol}420,000" in page, code


def test_stored_supabase_html_is_never_served_verbatim(
    server: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A row written before the fix carries the attacker's rendered HTML: the
    fallback must re-render from the stored plan, not echo the stored page."""
    share_id = "ab12cd34ef56"
    poisoned = {
        "html": f"<html><body>{_PAYLOADS['script']}</body></html>",
        "plan_data": json.dumps(_hostile_plan(_PAYLOADS["img"])),
        "created_at": "2026-09-30T10:00:00Z",
    }
    monkeypatch.setattr(app, "_supabase_rest", lambda *a, **k: [poisoned])
    status, page = _request(server, "GET", f"/scorecard/{share_id}")
    assert status == 200, page[:300]
    _assert_clean(page)
    assert "Channel Allocation" in page  # a real re-render, not a blank page


@pytest.mark.parametrize("bad", ["x" * 13, "../../etc", "ABCDEF123456", "abc&select=*"])
def test_malformed_scorecard_ids_never_reach_a_store(
    server: int, bad: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[Any] = []
    monkeypatch.setattr(app, "_supabase_rest", lambda *a, **k: calls.append((a, k)))
    status, _page = _request(server, "GET", f"/scorecard/{bad}")
    assert status == 404
    assert calls == []
