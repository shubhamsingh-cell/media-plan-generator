"""Conversation ids are the only key to a conversation's memory.

They were minted with Math.random (~31 bits) on every surface and accepted as
any string by the server. Now: 128-bit ids from crypto, the server treats a
malformed/short id as "no id" (no memory read or written), and raw ids never
appear in log lines.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from unittest.mock import patch

import pytest

import nova
import nova_memory
from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

GOOD = "nova-0f8fad5b-d9cb-469f-a165-70867728950e"


# ---------------------------------------------------------------------------
# Server validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw",
    ["", None, "short", "abc123", "x" * 15, "has space in it 1234567", "semi;colon-1234567890",
     "../../etc/passwd-aaaa", "ünïcode-aaaaaaaaaaaaaaaa", "a" * 129, 12345678901234567890, ["a" * 20]],
)
def test_bad_conversation_ids_are_treated_as_id_less(raw):
    assert nova_memory.valid_conversation_id(raw) == ""
    assert nova_memory.get_conversation_memory(raw) is None


@pytest.mark.parametrize("raw", [GOOD, "conv-1700000000000-abc123", "A" * 16, "a_b-C" * 4, "x" * 128])
def test_well_formed_ids_are_kept(raw):
    assert nova_memory.valid_conversation_id(raw) == raw


def test_log_tag_never_contains_the_raw_id():
    tag = nova_memory.conversation_log_tag(GOOD)
    assert GOOD not in tag and GOOD[:8] not in tag
    assert re.fullmatch(r"conv#[0-9a-f]{8}", tag)
    assert nova_memory.conversation_log_tag("") == "conv#none"


class _StubIQ:
    seen_session = "unset"

    def chat(self, user_message, session_id=None, **kwargs):
        _StubIQ.seen_session = session_id
        return {"response": "ok " * 30, "sources": [], "confidence": 0.9, "tools_used": []}


def _handle(conv_id):
    patches = [
        patch.object(nova, "_get_iq", lambda: _StubIQ()),
        patch.object(nova_memory.NovaMemory, "load", lambda self: None),
        patch.object(nova_memory.NovaMemory, "_persist_entry", lambda s, e: None),
        patch.object(nova_memory, "_conversation_memories", OrderedDict()),
    ]
    for p in patches:
        p.start()
    try:
        result = nova.handle_chat_request(
            {"message": "I work at Acme Health, always", "conversation_id": conv_id}
        )
        return result, dict(nova_memory._conversation_memories)
    finally:
        for p in reversed(patches):
            p.stop()


def test_short_id_neither_saves_memory_nor_reaches_the_engine_as_a_session():
    result, memories = _handle("short1")
    assert memories == {}
    assert _StubIQ.seen_session is None
    assert "user_profile" not in result


def test_long_valid_id_is_used():
    result, memories = _handle(GOOD)
    assert len(memories) == 1
    assert _StubIQ.seen_session == GOOD


def test_no_raw_id_in_log_output(caplog):
    import logging

    with caplog.at_level(logging.DEBUG):
        _handle(GOOD)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert GOOD not in text
    assert GOOD[:12] not in text


def test_source_log_sites_use_the_hashed_tag():
    nova_src = (ROOT / "nova.py").read_text(encoding="utf-8")
    assert "_ab_session[:12]" not in nova_src
    assert "conversation_id[:8]" not in nova_src
    assert "for {conversation_id} " not in nova_src
    app_src = (ROOT / "app.py").read_text(encoding="utf-8")
    assert '"SSE client disconnected for conversation %s", conversation_id' not in app_src
    assert "Session token mismatch for conversation %s\",\n                            conversation_id," not in app_src


# ---------------------------------------------------------------------------
# Client minting (real shipped functions under node)
# ---------------------------------------------------------------------------
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

_WIDGET = ROOT / "static" / "nova-chat.js"
_DRAWER = ROOT / "static" / "js" / "platform.js"
_NOVA = ROOT / "static" / "js" / "nova.js"

_SERVER_RE = r"^[A-Za-z0-9_-]{16,128}$"


def _mint_script(src: str, secure_name: str, minter: str, prefix: str) -> str:
    return (
        r"""
var store = {};
var localStorage = { getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); } };
var CONFIG = { sessionKey: "nova_session" }, CONV_KEY = "nova_drawer_conv";
var mathCalls = 0; var _r = Math.random; Math.random = function () { mathCalls++; return _r(); };
"""
        + js_block(src, f"function {secure_name}(")
        + "\n"
        + js_block(src, f"function {minter}(")
        + r"""
var a = MINT(), b = MINT();
var viaUuid = {a: a, b: b, math: mathCalls};
// getRandomValues-only environment
var realCrypto = globalThis.crypto;
Object.defineProperty(globalThis, "crypto", { configurable: true, value: {
  getRandomValues: function (arr) { for (var i = 0; i < arr.length; i++) arr[i] = (i * 37 + 11) & 255; return arr; } } });
var c = MINT(); var m2 = mathCalls;
console.log(JSON.stringify({viaUuid: viaUuid, c: c, mathAfter: m2}));
""".replace("MINT", minter)
    )


@needs_node
@pytest.mark.parametrize(
    "path,secure,minter",
    [(_WIDGET, "secureRandomId", "mintSessionId"), (_DRAWER, "_secureRandomId", "_mintConversationId")],
)
def test_widget_and_drawer_ids_come_from_crypto(path, secure, minter):
    out = run_node(_mint_script(path.read_text(encoding="utf-8"), secure, minter, ""))
    for sid in (out["viaUuid"]["a"], out["viaUuid"]["b"], out["c"]):
        assert re.match(_SERVER_RE, sid), sid
    assert out["viaUuid"]["a"] != out["viaUuid"]["b"]
    assert out["viaUuid"]["math"] == 0  # Math.random is not involved
    assert out["mathAfter"] == 0
    # randomUUID is 36 chars incl. hyphens; the fallback is 32 hex chars (128 bits)
    assert re.search(r"[0-9a-f]{32}$", out["c"])


@needs_node
def test_nova_page_ids_come_from_crypto():
    src = _NOVA.read_text(encoding="utf-8")
    out = run_node(
        r"""
var mathCalls = 0; var _r = Math.random; Math.random = function () { mathCalls++; return _r(); };
"""
        + js_block(src, "function secureRandomId(")
        + js_block(src, "function generateId(")
        + r"""
var ids = []; for (var i = 0; i < 20; i++) ids.push(generateId());
console.log(JSON.stringify({ids: ids, math: mathCalls}));
"""
    )
    assert len(set(out["ids"])) == 20 and out["math"] == 0
    assert all(re.match(_SERVER_RE, i) for i in out["ids"])


@needs_node
def test_widget_replaces_a_legacy_math_random_session_id():
    src = _WIDGET.read_text(encoding="utf-8")
    out = run_node(
        r"""
var store = {nova_session: "nova-1700000000000-k3j9x2"};
var localStorage = { getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); } };
var CONFIG = { sessionKey: "nova_session" };
"""
        + js_block(src, "function secureRandomId(")
        + js_block(src, "function mintSessionId(")
        + js_block(src, "function getSessionId(")
        + r"""
var first = getSessionId(), again = getSessionId();
console.log(JSON.stringify({first: first, same: first === again, stored: store.nova_session === first}));
"""
    )
    assert re.match(r"^nova-[0-9a-f-]{36}$", out["first"]) and out["same"] and out["stored"]
