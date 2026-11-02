"""The chat widget under NOVA_CHAT_GATE, run whole under node.

Under ``signed`` the widget must mint a widget token before its first message
(no race with the page load), send it on every transport (header for HTTP,
``widget_token`` field over the socket), follow the fresh token each reply
returns (including after the server moves the turn to a new conversation),
and recover once from a 401. Under ``off`` it asks the mint once and never
sends a token. See tests/widget_runtime_harness.py.
"""

from __future__ import annotations

import pytest

from tests.widget_runtime_harness import run_widget
from tests.wizard_js_harness import NODE

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")

TRANSPORTS = ["ws", "sse", "post"]

# Signed server: tokens look like the real format (4 dot parts, unix expiry in
# part 3) so the widget's expiry parsing is exercised.
SIGNED_SERVER = r"""
var issued = 0, rejectNext = 0;
function tok(cid, tag) { issued++;
  return "v1." + cid.slice(-6) + "." + (Math.floor(Date.now() / 1000) + 900) + "." + tag + issued; }
function serverMint(body) { return {gate: "signed", conversation_id: body.conversation_id,
  widget_token: tok(body.conversation_id, "m"), expires_in: 900}; }
var serverTokens = {}, moved = 0;
function server(body) {
  if (rejectNext > 0) { rejectNext--; return {status: 401}; }
  var cid = body.conversation_id;
  if (!body.widget_token || body.widget_token.split(".")[1] !== cid.slice(-6)) return {status: 401};
  if (!(cid in serverTokens)) serverTokens[cid] = "tok-" + cid.slice(-6);
  else if (serverTokens[cid] !== (body.session_token || "")) {
    moved++; cid = "nova-00000000-0000-4000-8000-00000000000" + moved;
    serverTokens[cid] = "tok-new-" + moved;
  }
  return {conversation_id: cid, session_token: serverTokens[cid], widget_token: tok(cid, "r")};
}
"""

OFF_SERVER = r"""
function serverMint(body) { return {gate: "off"}; }
var serverTokens = {};
function server(body) {
  var cid = body.conversation_id;
  if (!(cid in serverTokens)) serverTokens[cid] = "tok-" + cid.slice(-6);
  return {conversation_id: cid, session_token: serverTokens[cid]};
}
"""


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_signed_first_message_waits_for_a_minted_token(transport):
    out = run_widget(
        SIGNED_SERVER,
        r"""
var chat = makeTab("A");
chat.send("first message right after load"); await settle();
chat.send("second"); await settle();
chat.send("third"); await settle();
console.log(JSON.stringify({bodies: bodies, mints: mints}));
""",
        transport,
    )
    assert "error" not in out, out
    sent = out["bodies"]
    assert [b["via"] for b in sent] == [transport] * 3, sent
    cid = sent[0]["conversation_id"]
    assert out["mints"] == [cid]  # one mint, before the first send
    assert sent[0]["widget_token"].split(".")[3].startswith("m")
    # Later turns ride on the token each reply returned: no more mints.
    assert all(b["widget_token"].split(".")[3].startswith("r") for b in sent[1:])
    assert all(b["widget_token"].split(".")[1] == cid[-6:] for b in sent)


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_signed_follows_token_for_a_moved_conversation(transport):
    out = run_widget(
        SIGNED_SERVER,
        r"""
var chat = makeTab("A");
chat.send("q1"); await settle();
// Lose the stored session token: the server moves the next turn to a new
// conversation and returns a widget token bound to it.
Object.keys(store).forEach(function (k) {
  if (k.indexOf("nova_session_token:") === 0) delete store[k]; });
chat.send("q2"); await settle();
chat.send("q3"); await settle();
console.log(JSON.stringify({bodies: bodies, mints: mints, moved: moved}));
""",
        transport,
    )
    assert "error" not in out, out
    sent = out["bodies"]
    assert out["moved"] == 1
    assert len(sent) == 3
    new_cid = sent[2]["conversation_id"]
    assert new_cid != sent[0]["conversation_id"]
    assert sent[2]["widget_token"].split(".")[1] == new_cid[-6:]
    assert len(out["mints"]) == 1


# The gate answers the stream request itself with 401, so recovery lives on
# the WebSocket and SSE paths (the harness's "post" transport fakes a 500 on
# the stream, which a gated server never sends).
@pytest.mark.parametrize("transport", ["ws", "sse"])
def test_signed_401_re_mints_and_resends_once(transport):
    out = run_widget(
        SIGNED_SERVER,
        r"""
var chat = makeTab("A");
chat.send("warm up"); await settle();
rejectNext = 1;              // e.g. the server restarted with a new key
chat.send("after restart"); await settle();
var afterOne = bodies.length, mintsAfterOne = mints.length;
rejectNext = 99;             // a server that keeps refusing
chat.send("keeps failing"); await settle();
console.log(JSON.stringify({bodies: bodies, mints: mints, afterOne: afterOne,
  mintsAfterOne: mintsAfterOne}));
""",
        transport,
    )
    assert "error" not in out, out
    # warm-up (1) + rejected (1) + resent after a fresh mint (1)
    assert out["afterOne"] == 3, out["bodies"]
    assert out["mintsAfterOne"] == 2
    texts_after = out["bodies"][3:]
    # A refusing server gets exactly one silent retry (plus the SSE path's own
    # plain-POST fallback), never a loop.
    assert 2 <= len(texts_after) <= 4, texts_after


@pytest.mark.parametrize("transport", TRANSPORTS)
def test_off_never_sends_a_token_and_mints_once(transport):
    out = run_widget(
        OFF_SERVER,
        r"""
var chat = makeTab("A");
for (var i = 0; i < 3; i++) { chat.send("q" + i); await settle(); }
console.log(JSON.stringify({bodies: bodies, mints: mints}));
""",
        transport,
    )
    assert "error" not in out, out
    assert len(out["bodies"]) == 3
    assert all(b["widget_token"] == "" for b in out["bodies"])
    assert len(out["mints"]) == 1
