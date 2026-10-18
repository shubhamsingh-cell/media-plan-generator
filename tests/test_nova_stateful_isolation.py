"""Answers shaped by one user's history/context must never be shared.

Both response caches, the request coalescer and the planner hint were keyed
by the message alone, so an answer built from user A's prior turns (or client
context) could be served to user B sending the same message.
"""

from __future__ import annotations

import copy
import threading
from unittest.mock import patch

import pytest

import nova
import nova_cache
import request_coalescing

MSG = (
    "Explain how a hiring funnel for forklift operators in Reno Nevada "
    "should be structured across paid channels"
)
SECRET = "Acme Hospital, Dallas"
HIST = [
    {"role": "user", "content": f"We are hiring nurses for {SECRET}"},
    {"role": "assistant", "content": "Noted."},
]


@pytest.fixture()
def isolated(tmp_path):
    with patch.object(nova, "_response_cache", {}), patch.object(
        nova, "RESPONSE_CACHE_FILE", tmp_path / "resp.json"
    ), patch.object(nova, "_upstash_enabled", False), patch.object(
        nova_cache, "_memory_cache", {}
    ), patch.object(nova_cache, "_supabase_enabled", False), patch.object(
        nova_cache, "_upstash_enabled", False
    ), patch.object(nova, "_intelligent_cache_available", True):
        yield


def _drive(requests):
    """Run requests through the real handle_chat_request -> Nova.chat path with a
    model stub that echoes whatever history/context it was handed."""
    calls = []

    def fake(self, user_message, conversation_history=None, enrichment_context=None, *a, **k):
        calls.append(1)
        seen = " ".join(m.get("content", "") for m in (conversation_history or []))
        ctx = str(enrichment_context or "")
        return {
            "response": "Recommended channels with 30% programmatic. " + seen + " " + ctx + " " * 5 + "x" * 40,
            "sources": ["KB"],
            "confidence": 0.9,
            "tools_used": ["query_channels"],
            "llm_provider": "stub",
        }

    out = []
    with patch.object(nova.Nova, "_chat_with_free_llm_tools", fake), patch.object(
        nova.Nova, "_chat_with_llm_router", lambda *a, **k: None
    ), patch.object(nova.Nova, "_chat_with_claude", fake):
        for req in requests:
            out.append(nova.handle_chat_request(copy.deepcopy(req)))
    return calls, out


def test_probe_history_answer_is_not_served_to_a_stateless_request(isolated):
    calls, (a, b) = _drive([{"message": MSG, "history": HIST}, {"message": MSG}])
    assert SECRET in a["response"]  # the stub really personalised A's answer
    assert SECRET not in b["response"]
    assert len(calls) == 2, "B must not be served from a cache A wrote"


def test_probe_history_answer_is_not_served_to_another_history_user(isolated):
    other = [{"role": "user", "content": "unrelated"}, {"role": "assistant", "content": "ok"}]
    calls, (a, b) = _drive(
        [{"message": MSG, "history": HIST}, {"message": MSG, "history": other}]
    )
    assert SECRET not in b["response"]
    assert len(calls) == 2


def test_stateless_answer_is_not_served_to_a_request_with_history(isolated):
    """A request with history is never read from the cache either."""
    calls, (a, b) = _drive([{"message": MSG}, {"message": MSG, "history": HIST}])
    assert len(calls) == 2
    assert SECRET in b["response"]  # B got an answer built from ITS history


def test_client_context_answer_is_not_shared(isolated):
    ctx = {"campaign_client": SECRET}
    calls, (a, b) = _drive([{"message": MSG, "context": ctx}, {"message": MSG}])
    assert SECRET in a["response"] and SECRET not in b["response"]
    assert len(calls) == 2


def test_stateless_first_turns_are_still_cached(isolated):
    calls, (a, b) = _drive([{"message": MSG}, {"message": MSG}])
    assert len(calls) == 1 and b["response"][:60] == a["response"][:60]


def test_nova_cache_skips_any_prior_history():
    skip, reason = nova_cache._should_skip_caching(MSG, HIST[:1])
    assert skip and reason == "followup"
    assert nova_cache._should_skip_caching(MSG, [])[0] is False


# ---------------------------------------------------------------------------
# L-B: request coalescing
# ---------------------------------------------------------------------------
class _FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, k):
        return self.kv.get(k)

    def set_nx(self, k, v, ttl):
        if k in self.kv:
            return False
        self.kv[k] = v
        return True

    def set_ex(self, k, v, ttl):
        self.kv[k] = v

    def delete(self, k):
        self.kv.pop(k, None)


@pytest.fixture()
def coalescer():
    r = _FakeRedis()
    with patch.object(request_coalescing, "_ENABLED", True), patch.object(
        request_coalescing, "_redis_get", r.get
    ), patch.object(request_coalescing, "_redis_set_nx", r.set_nx), patch.object(
        request_coalescing, "_redis_set_ex", r.set_ex
    ), patch.object(request_coalescing, "_redis_del", r.delete):
        yield request_coalescing.RequestCoalescer(), r


def test_coalescer_hash_depends_on_the_scope_marker(coalescer):
    c, r = coalescer
    _, h_plain, _ = c.check_or_register(MSG)
    _, h_scoped, _ = c.check_or_register(MSG, scope="stateless")
    assert h_plain != h_scoped


def test_only_stateless_requests_may_coalesce():
    def scope(state):
        nova._set_turn_tool_capture(state)
        try:
            return nova._coalesce_scope()
        finally:
            nova._set_turn_tool_capture(None)

    assert scope({"conversation_id": "", "stateless": True})
    assert scope({"conversation_id": "", "stateless": False}) is None  # history/context
    assert scope({"conversation_id": "conv-abcdefghijklmnop", "stateless": True}) is None
    assert scope({"conversation_id": "", "stateless": True, "personalized": True}) is None
    assert nova._coalesce_scope() is None  # no turn state -> never coalesce


class _RecordingCoalescer:
    def __init__(self):
        self.completed, self.released = [], []

    def complete(self, h, r):
        self.completed.append((h, r))

    def release(self, h):
        self.released.append(h)


def test_a_personalised_result_is_never_stored_for_followers():
    rec = _RecordingCoalescer()
    nova._set_turn_tool_capture({"conversation_id": "", "stateless": True, "personalized": True})
    try:
        nova._complete_coalesced(rec, "h1", {"response": SECRET})
    finally:
        nova._set_turn_tool_capture(None)
    assert rec.completed == [] and rec.released == ["h1"]


def test_a_stateless_result_is_stored():
    rec = _RecordingCoalescer()
    nova._set_turn_tool_capture({"conversation_id": "", "stateless": True})
    try:
        nova._complete_coalesced(rec, "h2", {"response": "generic"})
    finally:
        nova._set_turn_tool_capture(None)
    assert rec.completed == [("h2", {"response": "generic"})] and rec.released == []


def test_released_slot_lets_followers_proceed_without_a_result(coalescer):
    c, r = coalescer
    leader, h, _ = c.check_or_register(MSG, scope="stateless")
    assert leader
    got = {}

    def follower():
        got["r"] = c.check_or_register(MSG, scope="stateless")

    t = threading.Thread(target=follower)
    t.start()
    c.release(h)
    t.join(timeout=5)
    assert not t.is_alive(), "follower must not spin until the window expires"
    is_leader, _, result = got["r"]
    assert result is None
    assert not any(k.endswith(h) and "res:" in k for k in r.kv)


def test_chat_path_uses_the_scoped_coalescer():
    src = open(nova.__file__, encoding="utf-8").read()
    import re

    assert re.search(r"check_or_register\(\s*user_message, scope=", src)
    assert "_coalescer.complete(_qhash, _result)" not in src


# ---------------------------------------------------------------------------
# L-C: planner hint is request scoped, intent cache never holds history-derived intents
# ---------------------------------------------------------------------------
def test_planner_hint_is_request_scoped_across_interleaved_threads():
    barrier = threading.Barrier(2)
    seen = {}

    def worker(name):
        nova._set_turn_tool_capture({"conversation_id": "", "tools": {}})
        nova._set_intent_hint({"role": name})
        barrier.wait(timeout=5)  # both have stored their hint before either reads
        seen[name] = (nova._get_intent_hint() or {}).get("role")
        nova._set_turn_tool_capture(None)

    ts = [threading.Thread(target=worker, args=(n,)) for n in ("nurse", "driver")]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=10)
    assert seen == {"nurse": "nurse", "driver": "driver"}


def test_no_shared_instance_hint_remains():
    src = open(nova.__file__, encoding="utf-8").read()
    assert "self._intent_hint" not in src
    assert 'getattr(self, "_intent_hint"' not in src


def test_planner_never_caches_or_reads_history_derived_intents():
    n = nova.Nova.__new__(nova.Nova)
    n._intent_cache = nova.OrderedDict() if hasattr(nova, "OrderedDict") else __import__("collections").OrderedDict()
    n._intent_cache_lock = threading.Lock()
    summaries = []

    def fake_llm(message, history_summary=""):
        summaries.append(history_summary)
        return {"intent": ["media_plan"], "role": "nurse", "reason": history_summary or "none"}

    msg = "compare cpa for nurses in two cities"
    with patch.object(nova, "_t3_flag", lambda *_: True), patch.object(
        nova, "_is_planner_eligible_query", lambda *_: True
    ), patch.object(nova, "_call_planner_llm", fake_llm):
        a = n._run_planner_step(msg, HIST)
        b = n._run_planner_step(msg, None)
        c = n._run_planner_step(msg, None)
    assert SECRET in a["reason"]
    assert SECRET not in b["reason"], "B was served an intent derived from A's history"
    assert c == b and len(summaries) == 2  # stateless intents are still cached


# ── Chat turns bind to a stored conversation only with its session token ──
#
# A conversation's id keys its server-side memory, tool state and stored
# transcript. A chat turn that names an existing conversation without that
# conversation's session token (or with a wrong one) must run in a new
# conversation instead, and the response must say which one.

import base64  # noqa: E402
import http.client  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import socket  # noqa: E402
import struct  # noqa: E402
import time  # noqa: E402

import app as app_module  # noqa: E402
import nova_persistence  # noqa: E402
from tests import nova_conversation_store as conv_store  # noqa: E402
from tests.live_server import live_port  # noqa: E402,F401 (fixture)

CONV_B = "nova-" + "b" * 32
TOKEN_B = "1" * 64
REMEMBERED = "remembered-fact-for-conversation-b"
B_TRANSCRIPT = [{"role": "user", "content": "earlier turn"}]


@pytest.fixture()
def chat_env(monkeypatch):
    """Conversation B exists with a token and a remembered fact; the model,
    enrichment and follow-ups are stubbed; persistence writes are recorded."""
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, TOKEN_B, B_TRANSCRIPT)
    conv_store.seed_memory(monkeypatch, CONV_B, REMEMBERED)
    bound: list = []
    saved: list = []

    def fake_stream(data, cancel_event=None):
        cid = data.get("conversation_id") or ""
        bound.append(cid)
        text = "reply " + conv_store.memory_block(cid)
        yield {"token": text}
        yield {"done": True, "full_response": text}

    def fake_chat(data, outer_deadline=None, partial_accumulator=None):
        cid = data.get("conversation_id") or ""
        bound.append(cid)
        return {
            "response": "reply " + conv_store.memory_block(cid),
            "sources": [],
            "confidence": 0.5,
            "tools_used": [],
        }

    def record_save(conversation_id, *args, **kwargs):
        saved.append(conversation_id)

    monkeypatch.setattr(nova, "handle_chat_request_stream", fake_stream)
    monkeypatch.setattr(nova, "handle_chat_request", fake_chat)
    monkeypatch.setattr(app_module, "_enrich_chat_context", lambda *a, **k: None)
    monkeypatch.setattr(app_module, "_generate_followup_questions", lambda *a, **k: [])
    monkeypatch.setattr(app_module, "_save_conversation_turn", record_save)
    return {"store": store, "bound": bound, "saved": saved}


def _wait_for(items: list, timeout: float = 3.0) -> list:
    deadline = time.time() + timeout
    while not items and time.time() < deadline:
        time.sleep(0.02)
    return items


def _sse_done(port: int, payload: dict) -> dict:
    conv_store.clear_rate_limits()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        conn.request(
            "POST",
            "/api/chat/stream",
            body=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Origin": "http://localhost"},
        )
        resp = conn.getresponse()
        assert resp.status == 200, resp.read()[:300]
        while True:
            line = resp.readline()
            assert line, "stream ended without a done event"
            if not line.startswith(b"data: "):
                continue
            event = json.loads(line[6:])
            if event.get("done"):
                return event
    finally:
        conn.close()


def _post_chat(port: int, payload: dict) -> dict:
    conv_store.clear_rate_limits()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        conn.request(
            "POST",
            "/api/chat",
            body=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Origin": "http://localhost"},
        )
        resp = conn.getresponse()
        body = json.loads(resp.read())
        assert resp.status == 200, body
        return body
    finally:
        conn.close()


def _ws_done(port: int, payload: dict) -> dict:
    conv_store.clear_rate_limits()
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    sock = socket.create_connection(("127.0.0.1", port), timeout=20)
    try:
        sock.sendall(
            (
                "GET /ws/chat HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n"
                "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                f"Sec-WebSocket-Key: {key}\r\nOrigin: http://localhost:5000\r\n\r\n"
            ).encode("ascii")
        )
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = sock.recv(1024)
            assert chunk, "server closed during handshake"
            buf += chunk
        assert buf.split()[1] == b"101"
        raw = json.dumps(dict(payload, type="chat")).encode("utf-8")
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(raw))
        header = bytes([0x81, 0x80 | 126]) + struct.pack(">H", len(raw))
        sock.sendall(header + mask + masked)

        def recv_exact(n: int) -> bytes:
            data = b""
            while len(data) < n:
                chunk = sock.recv(n - len(data))
                if not chunk:
                    raise EOFError
                data += chunk
            return data

        while True:
            head = recv_exact(2)
            length = head[1] & 0x7F
            if length == 126:
                length = struct.unpack(">H", recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", recv_exact(8))[0]
            frame = json.loads(recv_exact(length))
            if frame.get("done"):
                return frame
    finally:
        sock.close()


def _turn(transport: str, port: int, token) -> dict:
    payload = {"message": "hello there", "conversation_id": CONV_B, "history": []}
    if token is not None:
        payload["session_token"] = token
    if transport == "sse":
        return _sse_done(port, payload)
    if transport == "ws":
        return _ws_done(port, payload)
    return _post_chat(port, payload)


def _reply_text(transport: str, done: dict) -> str:
    return done.get("response") if transport == "post" else done.get("full_response")


TRANSPORTS = ["sse", "ws", "post"]


@pytest.mark.parametrize("transport", TRANSPORTS)
@pytest.mark.parametrize(
    "token", ["", None, "2" * 64], ids=["empty", "absent", "wrong"]
)
def test_turn_without_the_conversation_token_runs_in_a_new_conversation(
    chat_env, live_port, transport, token
):
    done = _turn(transport, live_port, token)
    bound = chat_env["bound"]
    assert bound and bound[-1] != CONV_B, "turn must not bind to conversation B"
    assert REMEMBERED not in (_reply_text(transport, done) or "")
    assert done.get("conversation_id") == bound[-1]
    new_row = chat_env["store"].row(bound[-1])
    assert new_row is not None
    assert done.get("session_token") == new_row["session_token"]
    assert done.get("session_token") not in ("", TOKEN_B)
    saved = _wait_for(chat_env["saved"])
    assert saved and CONV_B not in saved, "B's transcript must not be appended to"
    assert chat_env["store"].row(CONV_B)["messages"] == B_TRANSCRIPT


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_turn_with_the_conversation_token_keeps_its_conversation(
    chat_env, live_port, transport
):
    done = _turn(transport, live_port, TOKEN_B)
    assert chat_env["bound"][-1] == CONV_B
    assert REMEMBERED in _reply_text(transport, done)
    assert done.get("conversation_id") == CONV_B
    assert done.get("session_token") == TOKEN_B


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_first_turn_of_a_new_conversation_keeps_its_id_and_gets_a_token(
    chat_env, live_port, transport
):
    new_id = "nova-" + "c" * 32
    payload = {"message": "hello there", "conversation_id": new_id, "history": []}
    payload["session_token"] = TOKEN_B  # a token from another conversation
    if transport == "sse":
        done = _sse_done(live_port, payload)
    elif transport == "ws":
        done = _ws_done(live_port, payload)
    else:
        done = _post_chat(live_port, payload)
    assert chat_env["bound"][-1] == new_id
    assert done.get("conversation_id") == new_id
    row = chat_env["store"].row(new_id)
    assert row is not None and done.get("session_token") == row["session_token"]
    assert row["session_token"] != TOKEN_B


def test_legacy_conversation_without_a_stored_token_is_not_resumed(monkeypatch):
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, "", B_TRANSCRIPT)
    cid, token = nova_persistence.resolve_chat_conversation(CONV_B, "")
    assert cid != CONV_B and cid.startswith("nova-")
    assert token and store.row(cid)["session_token"] == token


def test_lookup_error_starts_a_new_conversation(monkeypatch):
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, TOKEN_B, B_TRANSCRIPT)
    store.fail_reads = True
    cid, _token = nova_persistence.resolve_chat_conversation(CONV_B, TOKEN_B)
    assert cid != CONV_B


# ── verify_conversation_token: read paths deny whatever cannot be verified ──


def test_verify_token_accepts_only_the_stored_token(monkeypatch):
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, TOKEN_B, B_TRANSCRIPT)
    assert nova_persistence.verify_conversation_token(CONV_B, TOKEN_B) is True
    assert nova_persistence.verify_conversation_token(CONV_B, "2" * 64) is False
    assert nova_persistence.verify_conversation_token(CONV_B, "") is False


def test_verify_token_denies_a_conversation_without_a_stored_token(monkeypatch):
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, "", B_TRANSCRIPT)
    assert nova_persistence.verify_conversation_token(CONV_B, "2" * 64) is False


def test_verify_token_denies_when_the_lookup_fails(monkeypatch):
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, TOKEN_B, B_TRANSCRIPT)
    store.fail_reads = True
    assert nova_persistence.verify_conversation_token(CONV_B, TOKEN_B) is False


def test_verify_token_denies_without_persistence(monkeypatch):
    monkeypatch.setattr(nova_persistence, "_get_supabase", lambda: None)
    assert nova_persistence.verify_conversation_token(CONV_B, TOKEN_B) is False


def test_verify_token_denies_an_unknown_conversation(monkeypatch):
    conv_store.install(monkeypatch)
    assert nova_persistence.verify_conversation_token(CONV_B, TOKEN_B) is False


# ── Fail safe when the conversation store cannot answer ──


def _sse_turns(port: int, conversation_id: str, turns: int) -> list:
    """Run turns the way a client does: adopt the id and token each reply returns."""
    cid, token, ids = conversation_id, "", []
    for _ in range(turns):
        done = _sse_done(
            port,
            {
                "message": "hello there",
                "conversation_id": cid,
                "session_token": token,
                "history": [],
            },
        )
        cid, token = done["conversation_id"], done.get("session_token") or ""
        ids.append(cid)
    return ids


def test_a_failed_schema_check_is_retried_not_remembered(
    chat_env, live_port, monkeypatch
):
    """A transient error on the first schema check must not pin the old-schema
    path (every turn a new conversation) for the life of the process."""
    monkeypatch.setattr(nova_persistence, "_schema_is_document_model", None)
    monkeypatch.setattr(nova_persistence, "_schema_recheck_after", 0.0, raising=False)
    monkeypatch.setattr(nova_persistence, "_SCHEMA_RECHECK_SECONDS", 0.0, raising=False)
    chat_env["store"].fail_next_reads = 1  # the first schema check fails
    ids = _sse_turns(live_port, "nova-" + "f" * 32, 3)
    assert len(set(ids)) == 1, ids
    assert nova_persistence._schema_is_document_model is True


def test_a_missing_column_answer_is_a_definitive_old_schema():
    missing = RuntimeError(
        "{'code': 'PGRST204', 'message': \"Could not find the 'messages' column\"}"
    )
    assert nova_persistence._is_missing_column_error(missing) is True
    assert nova_persistence._is_missing_column_error(TimeoutError("timed out")) is False
    assert nova_persistence._is_missing_column_error(OSError("network down")) is False


@pytest.mark.parametrize("token", ["", TOKEN_B], ids=["no-token", "stored-token"])
def test_persistence_down_never_resumes_a_requested_conversation(
    chat_env, live_port, monkeypatch, token
):
    """Nothing can be verified without the store, so the turn gets a new
    conversation and none of the requested conversation's state."""
    monkeypatch.setattr(nova_persistence, "_get_supabase", lambda: None)
    done = _sse_done(
        live_port,
        {
            "message": "hello there",
            "conversation_id": CONV_B,
            "session_token": token,
            "history": [],
        },
    )
    assert chat_env["bound"][-1] != CONV_B
    assert done["conversation_id"] == chat_env["bound"][-1]
    assert done["conversation_id"].startswith("nova-")
    assert REMEMBERED not in (done.get("full_response") or "")
    saved = _wait_for(chat_env["saved"])
    assert CONV_B not in saved


def test_lookup_error_log_does_not_carry_the_conversation_id(monkeypatch, caplog):
    store = conv_store.install(monkeypatch)
    store.add(CONV_B, TOKEN_B, B_TRANSCRIPT)
    store.fail_reads = True
    with caplog.at_level("ERROR", logger="nova_persistence"):
        nova_persistence.resolve_chat_conversation(CONV_B, TOKEN_B)
        nova_persistence.verify_conversation_token(CONV_B, TOKEN_B)
    assert caplog.records
    assert all(CONV_B not in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("transport", ["sse", "ws"])
def test_cancelled_reply_carries_the_conversation_it_ran_in(
    chat_env, live_port, monkeypatch, transport
):
    """A turn moved to a new conversation and then stopped still tells the
    client which conversation (and token) it ran in."""
    monkeypatch.setattr(
        app_module,
        "_enrich_chat_context",
        lambda data, *_a, **_k: app_module._cancel_stream(data["conversation_id"]),
    )
    done = _turn(transport, live_port, "2" * 64)
    assert done.get("cancelled") is True
    new_id = done.get("conversation_id")
    assert new_id and new_id != CONV_B
    row = chat_env["store"].row(new_id)
    assert row is not None and done.get("session_token") == row["session_token"]
