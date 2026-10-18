"""The chat widget, run whole under node, keeps one conversation per tab.

The widget (also the hub's chat: hub.js loads nova-chat.js) must send the
session token it was given for its conversation, follow the conversation id
the server answers with, and keep tokens per conversation so two open tabs
never overwrite each other. These tests drive the real send path and the real
done handlers (WebSocket, SSE, and the plain POST fallback); see
tests/widget_runtime_harness.py.
"""

from __future__ import annotations

import pytest

from tests.widget_runtime_harness import run_widget
from tests.wizard_js_harness import NODE

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")

# Mirrors nova_persistence.resolve_chat_conversation: an unseen id is created
# with a new token; a known id is kept only with its token, else a new
# conversation (same prefix) is started.
SERVER_JS = r"""
var serverTokens = {}, serverMinted = 0;
function server(body) {
  var cid = body.conversation_id, tok = body.session_token || "";
  if (!(cid in serverTokens)) { serverTokens[cid] = "tok-" + cid.slice(-6); }
  else if (serverTokens[cid] !== tok) {
    serverMinted++;
    cid = "nova-00000000-0000-4000-8000-00000000000" + serverMinted;
    serverTokens[cid] = "tok-new-" + serverMinted;
  }
  return {conversation_id: cid, session_token: serverTokens[cid]};
}
"""

TRANSPORTS = ["ws", "sse", "post"]


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_widget_keeps_one_conversation_across_turns(transport):
    out = run_widget(
        SERVER_JS,
        r"""
var chat = makeTab("A"); await settle();
for (var i = 0; i < 3; i++) { chat.send("question " + i); await settle(); }
console.log(JSON.stringify({bodies: bodies, minted: serverMinted}));
""",
        transport,
    )
    assert "error" not in out, out
    sent = out["bodies"]
    assert [b["via"] for b in sent] == [transport] * 3
    first = sent[0]["conversation_id"]
    assert sent[0]["session_token"] == ""
    assert [b["conversation_id"] for b in sent] == [first] * 3
    assert sent[1]["session_token"] == sent[2]["session_token"] == "tok-" + first[-6:]
    assert out["minted"] == 0


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_widget_follows_a_new_conversation_and_new_chat_starts_over(transport):
    out = run_widget(
        SERVER_JS,
        r"""
var chat = makeTab("A"); await settle();
chat.send("one"); await settle();
var cur = store.nova_session;
store["nova_session_token:" + cur] = "stale";   // token no longer valid
chat.send("two"); await settle();               // server starts a new conversation
chat.send("three"); await settle();
var beforeNewChat = store.nova_session;
chat.clearHistory();                            // New chat
chat.send("four"); await settle();
console.log(JSON.stringify({bodies: bodies, minted: serverMinted, beforeNewChat: beforeNewChat,
  oldToken: store["nova_session_token:" + beforeNewChat] || null}));
""",
        transport,
    )
    assert "error" not in out, out
    sent = out["bodies"]
    first = sent[0]["conversation_id"]
    assert sent[1]["conversation_id"] == first and sent[1]["session_token"] == "stale"
    moved = "nova-00000000-0000-4000-8000-000000000001"
    assert sent[2]["conversation_id"] == moved
    assert sent[2]["session_token"] == "tok-new-1"
    assert out["minted"] == 1
    assert out["beforeNewChat"] == moved and out["oldToken"] is None
    assert sent[3]["conversation_id"] not in (first, moved)
    assert sent[3]["session_token"] == ""


@pytest.mark.parametrize("transport", ["ws", "sse"])
def test_two_tabs_keep_their_own_conversations_after_new_chat(transport):
    """Two tabs open on the same conversation; New chat in one of them. Each
    tab must keep a single conversation afterwards (the old shared token slot
    made both tabs start a new conversation on every turn)."""
    out = run_widget(
        SERVER_JS,
        r"""
var a = makeTab("A"), b = makeTab("B"); await settle();
var log = [];
async function turn(chat, name) {
  var n = bodies.length;
  chat.send("q"); await settle();
  var sent = bodies[n];
  log.push({tab: name, id: sent.conversation_id, token: sent.session_token});
}
await turn(a, "A"); await turn(b, "B");
a.clearHistory();                                   // New chat in tab A only
for (var i = 0; i < 3; i++) { await turn(a, "A"); await turn(b, "B"); }
console.log(JSON.stringify({log: log, minted: serverMinted}));
""",
        transport,
    )
    assert "error" not in out, out
    log = out["log"]
    a_ids = [e["id"] for e in log if e["tab"] == "A"]
    b_ids = [e["id"] for e in log if e["tab"] == "B"]
    # tab A: one conversation before New chat, one after, never a fork
    assert len(set(a_ids[1:])) == 1 and a_ids[1] != a_ids[0]
    # tab B: at most one move (New chat in A dropped the shared conversation's
    # token), then a single conversation for the rest
    assert len(set(b_ids[2:])) == 1
    assert out["minted"] <= 1


def test_widget_moves_the_shared_token_of_an_older_build_once():
    out = run_widget(
        SERVER_JS,
        r"""
var id = "nova-" + "a".repeat(32);
store.nova_session = id;
server({conversation_id: id, session_token: ""});      // the server knows it
store.nova_session_token = serverTokens[id];            // older build's shared slot
var chat = makeTab("A"); await settle();
chat.send("q"); await settle();
console.log(JSON.stringify({sent: bodies[0], legacy: store.nova_session_token || null,
  perId: store["nova_session_token:" + id] || null, minted: serverMinted}));
""",
        "sse",
    )
    assert "error" not in out, out
    assert out["sent"]["conversation_id"] == "nova-" + "a" * 32
    assert out["sent"]["session_token"] == out["perId"] != ""
    assert out["legacy"] is None
    assert out["minted"] == 0
