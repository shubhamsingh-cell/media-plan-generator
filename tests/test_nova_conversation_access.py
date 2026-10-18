"""Stored Nova conversations are readable only by their owner or an admin.

* GET /api/chat/history and POST /api/nova/summarize require the
  conversation's session token in the X-Nova-Session-Token header (403
  otherwise).
* GET /api/chat/conversations and GET /api/nova/conversations/search list
  conversations across users, so they are admin only (401 otherwise).
"""

from __future__ import annotations

import http.client
import json
import urllib.parse

import pytest

import app as app_module
import nova
from tests import nova_conversation_store as conv_store
from tests.live_server import live_port  # noqa: F401 (fixture)

CONV = "nova-" + "d" * 32
TOKEN = "3" * 64
STORED = [{"role": "user", "content": "stored message"}]
ADMIN_KEY = "test-admin-key-for-conversation-access"


@pytest.fixture()
def stored(monkeypatch):
    store = conv_store.install(monkeypatch)
    store.add(CONV, TOKEN, STORED)
    monkeypatch.setattr(
        app_module,
        "_load_conversation_history",
        lambda cid: list((store.row(cid) or {}).get("messages") or []),
    )
    monkeypatch.setattr(
        nova,
        "summarize_conversation",
        lambda conversation_id, messages=None: {
            "conversation_id": conversation_id,
            "summary": "summary of stored conversation",
        },
    )
    monkeypatch.setattr(app_module, "ADMIN_API_KEY", ADMIN_KEY)
    monkeypatch.setattr(
        app_module, "_list_conversations", lambda limit: [{"conversation_id": CONV}]
    )
    monkeypatch.setattr(
        app_module,
        "_supabase_rest",
        lambda *a, **k: [{"id": CONV, "title": "match", "messages": STORED}],
    )
    return store


def _request(port: int, method: str, path: str, headers=None, body=None):
    conv_store.clear_rate_limits()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        hdrs = {"Content-Type": "application/json"}
        hdrs.update(headers or {})
        conn.request(method, path, body=payload, headers=hdrs)
        resp = conn.getresponse()
        raw = resp.read()
    finally:
        conn.close()
    try:
        return resp.status, json.loads(raw)
    except ValueError:
        return resp.status, raw


def _history(port: int, token=None):
    headers = {"X-Nova-Session-Token": token} if token is not None else {}
    qs = urllib.parse.urlencode({"conversation_id": CONV})
    return _request(port, "GET", f"/api/chat/history?{qs}", headers=headers)


def _summarize(port: int, token=None):
    # A Bearer header is how API clients skip the browser CSRF check.
    headers = {"Authorization": "Bearer api-client"}
    if token is not None:
        headers["X-Nova-Session-Token"] = token
    return _request(
        port, "POST", "/api/nova/summarize", headers, {"conversation_id": CONV}
    )


@pytest.mark.parametrize(
    "token", [None, "", "4" * 64], ids=["absent", "empty", "wrong"]
)
def test_history_requires_the_conversation_token(stored, live_port, token):
    status, body = _history(live_port, token)
    assert status == 403
    assert body.get("code") == "FORBIDDEN"
    assert "stored message" not in json.dumps(body)


def test_history_with_the_conversation_token(stored, live_port):
    status, body = _history(live_port, TOKEN)
    assert status == 200
    assert body["messages"] == STORED


def test_history_denied_for_a_conversation_without_a_stored_token(stored, live_port):
    stored.row(CONV)["session_token"] = ""
    status, _body = _history(live_port, TOKEN)
    assert status == 403


@pytest.mark.parametrize(
    "token", [None, "", "4" * 64], ids=["absent", "empty", "wrong"]
)
def test_summarize_requires_the_conversation_token(stored, live_port, token):
    status, body = _summarize(live_port, token)
    assert status == 403
    assert body.get("code") == "FORBIDDEN"
    assert "summary" not in body


def test_summarize_with_the_conversation_token(stored, live_port):
    status, body = _summarize(live_port, TOKEN)
    assert status == 200
    assert body["summary"] == "summary of stored conversation"


LISTING_PATHS = ["/api/chat/conversations", "/api/nova/conversations/search?q=match"]


@pytest.mark.parametrize("path", LISTING_PATHS)
@pytest.mark.parametrize(
    "headers",
    [{}, {"X-Admin-Key": "wrong"}, {"X-Nova-Session-Token": TOKEN}],
    ids=["none", "wrong-admin-key", "session-token"],
)
def test_conversation_listing_requires_admin(stored, live_port, path, headers):
    status, body = _request(live_port, "GET", path, headers=headers)
    assert status == 401
    text = json.dumps(body) if isinstance(body, dict) else body.decode("utf-8")
    assert CONV not in text


@pytest.mark.parametrize("path", LISTING_PATHS)
def test_conversation_listing_for_admin(stored, live_port, path):
    status, body = _request(live_port, "GET", path, {"X-Admin-Key": ADMIN_KEY})
    assert status == 200
    assert body["conversations"][0]["conversation_id"] == CONV
