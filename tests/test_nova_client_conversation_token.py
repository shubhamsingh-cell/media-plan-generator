"""Chat clients send the conversation's session token and follow the server.

The server binds a chat turn to a stored conversation only when the request
carries that conversation's session token; otherwise it answers with a new
conversation id and token. Each client must send the token it was given and
switch to the conversation id the server answered with, or every later turn
would start over. The real client functions are lifted and run under node.
"""

from __future__ import annotations

import pytest

from tests.test_nova_stream_reset import _run as run_nova_page
from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")

_STORAGE_STUB = r"""
var store = {};
var localStorage = { getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); }, removeItem: function (k) { delete store[k]; } };
"""

SERVER_ID_SUFFIX = "0123abcd-0000-4000-8000-0123456789ab"

WIDGET_TOKEN_FUNCTIONS = (
    "function sessionTokenKeyFor(",
    "function readSessionToken(",
    "function writeSessionToken(",
    "function removeSessionToken(",
    "function adoptServerSession(",
)


def widget_token_js(src: str) -> str:
    return "\n".join(js_block(src, name) for name in WIDGET_TOKEN_FUNCTIONS) + "\n"


def test_nova_page_sends_the_token_and_follows_the_server_conversation():
    out = run_nova_page(
        f"""
sendMessage(); await tick();
streams[0].push('data: {{"done": true, "full_response": "hi", "conversation_id": "conv-{SERVER_ID_SUFFIX}", "session_token": "tok-1"}}\\n');
streams[0].end(); await tick(); await tick();
chatInput.value = "second question"; sendMessage(); await tick();
console.log(JSON.stringify({{first: bodies[0], second: bodies[1], localId: getActiveConv().id}}));
"""
    )
    assert "error" not in out, out
    assert out["first"]["conversation_id"] == out["localId"]
    assert out["first"]["session_token"] == ""
    assert out["second"]["conversation_id"] == f"conv-{SERVER_ID_SUFFIX}"
    assert out["second"]["session_token"] == "tok-1"


def _widget(body: str):
    src = (ROOT / "static" / "nova-chat.js").read_text(encoding="utf-8")
    script = (
        _STORAGE_STUB
        + 'var CONFIG = { sessionKey: "nova_session_id", sessionTokenKey: "nova_session_token" };\n'
        + 'var state = { sessionId: "nova-'
        + "a" * 32
        + '" };\n'
        + widget_token_js(src)
        + body
    )
    return run_node(script)


def test_widget_adopts_the_server_conversation_and_token():
    out = _widget(
        f"""
var sent = state.sessionId;
adoptServerSession({{conversation_id: "nova-{SERVER_ID_SUFFIX}", session_token: "tok-2"}}, sent);
console.log(JSON.stringify({{id: state.sessionId, storedId: store.nova_session_id,
  token: store["nova_session_token:" + state.sessionId], oldToken: store["nova_session_token:" + sent] || null}}));
"""
    )
    assert out["id"] == f"nova-{SERVER_ID_SUFFIX}"
    assert out["storedId"] == out["id"]
    assert out["token"] == "tok-2"
    assert out["oldToken"] is None


def test_widget_ignores_a_reply_from_before_a_new_chat():
    out = _widget(
        f"""
var sent = state.sessionId;
state.sessionId = "nova-" + "e".repeat(32);  // visitor started a new chat
adoptServerSession({{conversation_id: "nova-{SERVER_ID_SUFFIX}", session_token: "tok-3"}}, sent);
console.log(JSON.stringify({{id: state.sessionId,
  tokens: Object.keys(store).filter(function (k) {{ return k.indexOf("nova_session_token") === 0; }})}}));
"""
    )
    assert out["id"] == "nova-" + "e" * 32
    assert out["tokens"] == []


def _drawer(body: str):
    src = (ROOT / "static" / "js" / "platform.js").read_text(encoding="utf-8")
    script = (
        _STORAGE_STUB
        + 'var CONV_KEY = "nova_drawer_conv", CONV_TOKEN_KEY = "nova_drawer_conv_token";\n'
        + 'var _convId = "";\n'
        + "\n".join(
            js_block(src, name)
            for name in (
                "function _secureRandomId()",
                "function _mintConversationId()",
                "function _conversationId()",
                "function _tokenKey(id)",
                "function _conversationToken()",
                "function _forgetConversationToken(id)",
                "function _adoptServerConversation(data, sentId)",
            )
        )
        + "\n"
        + body
    )
    return run_node(script)


def test_drawer_sends_the_token_and_follows_the_server_conversation():
    out = _drawer(
        f"""
var first = _conversationId(), firstToken = _conversationToken();
_adoptServerConversation({{conversation_id: "drawer-{SERVER_ID_SUFFIX}", session_token: "tok-4"}}, first);
var second = _conversationId(), secondToken = _conversationToken();
_convId = _mintConversationId();  // new chat
console.log(JSON.stringify({{first: first, firstToken: firstToken, second: second,
  secondToken: secondToken, afterNewChatToken: _conversationToken(), afterNewChatId: _convId}}));
"""
    )
    assert out["firstToken"] == ""
    assert out["second"] == f"drawer-{SERVER_ID_SUFFIX}" != out["first"]
    assert out["secondToken"] == "tok-4"
    assert out["afterNewChatToken"] == ""
    assert out["afterNewChatId"] not in (out["first"], out["second"])


# ── One conversation across turns, per surface; New chat starts over ──
#
# Server stand-in: a turn that presents the conversation's token (or names a
# conversation the server has not seen) keeps its id; any other turn is moved
# to a new conversation -- the rule nova_persistence.resolve_chat_conversation
# applies. Each surface's real send/adopt code runs under node.

_SERVER_JS = r"""
var serverTokens = {}, serverMinted = 0;
function serverAnswer(body) {
  var cid = body.conversation_id, tok = body.session_token || "";
  if (!(cid in serverTokens)) { serverTokens[cid] = "tok-" + cid.slice(-4); }
  else if (serverTokens[cid] !== tok) {
    serverMinted++;
    var prefix = cid.split("-")[0] + "-";
    cid = prefix + "00000000-0000-4000-8000-00000000000" + serverMinted;
    serverTokens[cid] = "tok-new-" + serverMinted;
  }
  return {conversation_id: cid, session_token: serverTokens[cid]};
}
"""


def test_nova_page_keeps_one_conversation_and_new_chat_starts_over():
    out = run_nova_page(
        _SERVER_JS
        + r"""
var ids = [];
async function turn(i, text) {
  if (text) chatInput.value = text;
  sendMessage(); await tick();
  var ans = serverAnswer(bodies[i]);
  ids.push({sent: bodies[i].conversation_id, token: bodies[i].session_token});
  streams[i].push('data: ' + JSON.stringify({done: true, full_response: "ok", conversation_id: ans.conversation_id, session_token: ans.session_token}) + '\n');
  streams[i].end(); await tick(); await tick();
}
await turn(0); await turn(1, "two"); await turn(2, "three");
getActiveConv().sessionToken = "stale";       // e.g. token from another tab
await turn(3, "four"); await turn(4, "five");
createConversation();                          // New chat
await turn(5, "six");
console.log(JSON.stringify({ids: ids, minted: serverMinted}));
"""
    )
    assert "error" not in out, out
    ids = out["ids"]
    first = ids[0]["sent"]
    assert ids[0]["token"] == ""
    assert [t["sent"] for t in ids[1:3]] == [first, first]
    assert ids[1]["token"] == ids[2]["token"] != ""
    # a stale token moves the conversation once, then it stays put
    assert ids[3]["sent"] == first and ids[3]["token"] == "stale"
    assert ids[4]["sent"] != first and ids[4]["token"].startswith("tok-new-")
    assert out["minted"] == 1
    # New chat: a different id and no token
    assert ids[5]["sent"] not in (first, ids[4]["sent"]) and ids[5]["token"] == ""


def test_widget_keeps_one_conversation_and_new_chat_starts_over():
    """The widget (also the hub's chat: hub.js loads nova-chat.js). The real
    payload lines of sendMessage, adoptServerSession and clearConversation."""
    src = (ROOT / "static" / "nova-chat.js").read_text(encoding="utf-8")
    send = src[
        src.index("    // API call\n") : src.index("    // Include session context")
    ]
    out = run_node(
        _STORAGE_STUB
        + _SERVER_JS
        + r"""
var CONFIG = { storageKey: "nova_chat_history", sessionKey: "nova_session",
               sessionTokenKey: "nova_session_token", maxHistoryStorage: 50 };
var state = { messages: [], isLoading: false };
var messagesDiv = { innerHTML: "" }, sendBtn = { disabled: false }, input = { focus: function () {} };
var window = { location: { pathname: "/" } };
var document = { getElementById: function (id) {
  return id === "nova-messages" ? messagesDiv : id === "nova-send-btn" ? sendBtn
       : id === "nova-input" ? input : null; } };
function showWelcome() {} function hideStopButton() {} function hideTyping() {}
var _activeAbortCtrl = null;
"""
        + js_block(src, "function secureRandomId()")
        + js_block(src, "function mintSessionId()")
        + js_block(src, "function getSessionId()")
        + widget_token_js(src)
        + js_block(src, "function clearConversation(")
        + "\nfunction buildPayload(text, history) {\n"
        + send
        + "  return payload;\n}\n"
        + r"""
state.sessionId = getSessionId();
var sent = [];
function turn(text) {
  var payload = buildPayload(text, []);
  sent.push({id: payload.conversation_id, token: payload.session_token});
  adoptServerSession(serverAnswer(payload), payload.conversation_id);
}
turn("one"); turn("two"); turn("three");
store["nova_session_token:" + state.sessionId] = "stale";
turn("four"); turn("five");
var beforeNewChat = state.sessionId;
clearConversation("new_chat_button");
var afterNewChat = {token: store["nova_session_token:" + beforeNewChat] || null};
turn("six");
console.log(JSON.stringify({sent: sent, minted: serverMinted, afterNewChat: afterNewChat}));
"""
    )
    sent = out["sent"]
    first = sent[0]["id"]
    assert sent[0]["token"] == ""
    assert [s["id"] for s in sent[1:4]] == [first] * 3
    assert sent[1]["token"] == sent[2]["token"] != ""
    assert sent[3]["token"] == "stale"
    assert sent[4]["id"] != first and sent[4]["token"].startswith("tok-new-")
    assert out["minted"] == 1
    assert out["afterNewChat"]["token"] is None
    assert sent[5]["id"] not in (first, sent[4]["id"]) and sent[5]["token"] == ""


def test_drawer_keeps_one_conversation_and_new_chat_starts_over():
    out = _drawer(
        _SERVER_JS
        + r"""
var sent = [];
function turn() {
  var body = {conversation_id: _conversationId(), session_token: _conversationToken()};
  sent.push({id: body.conversation_id, token: body.session_token});
  _adoptServerConversation(serverAnswer(body), body.conversation_id);
}
turn(); turn(); turn();
store["nova_drawer_conv_token:" + _convId] = "stale";
turn(); turn();
_convId = _mintConversationId();  // newChat()
turn();
console.log(JSON.stringify({sent: sent, minted: serverMinted}));
"""
    )
    sent = out["sent"]
    first = sent[0]["id"]
    assert sent[0]["token"] == ""
    assert [s["id"] for s in sent[1:4]] == [first] * 3
    assert sent[1]["token"] == sent[2]["token"] != ""
    assert sent[4]["id"] != first and sent[4]["token"].startswith("tok-new-")
    assert out["minted"] == 1
    assert sent[5]["id"] not in (first, sent[4]["id"]) and sent[5]["token"] == ""


def test_drawer_send_bodies_carry_the_token():
    """Both /api/chat bodies in platform.js (first try and CSRF retry) send it."""
    src = (ROOT / "static" / "js" / "platform.js").read_text(encoding="utf-8")
    assert src.count("session_token: _conversationToken(),") == 2
    assert src.count("_adoptServerConversation(data, conversationId);") == 1


def test_nova_page_title_sync_uses_the_conversation_the_server_bound():
    src = (ROOT / "static" / "js" / "nova.js").read_text(encoding="utf-8")
    out = run_node(
        r"""
var synced = [];
function _syncTitleToSupabase(id, title) { synced.push(id); }
function generateSmartTitle(t) { return "Nurses in Texas"; }
function isGreetingOnly() { return false; }
function saveConversations() {} function renderSidebar() {}
"""
        + js_block(src, "function updateConvTitle(conv)")
        + r"""
updateConvTitle({id: "conv-local", serverId: "conv-server", title: "New Chat",
  messages: [{role: "user", content: "nurses in texas"}]});
updateConvTitle({id: "conv-only-local", title: "New Chat",
  messages: [{role: "user", content: "nurses in texas"}]});
console.log(JSON.stringify({synced: synced}));
"""
    )
    assert out["synced"] == ["conv-server", "conv-only-local"]


def _two_drawer_tabs(body: str):
    """Two drawer instances (tabs) sharing one localStorage."""
    src = (ROOT / "static" / "js" / "platform.js").read_text(encoding="utf-8")
    lifted = "\n".join(
        js_block(src, name)
        for name in (
            "function _secureRandomId()",
            "function _mintConversationId()",
            "function _conversationId()",
            "function _tokenKey(id)",
            "function _conversationToken()",
            "function _forgetConversationToken(id)",
            "function _adoptServerConversation(data, sentId)",
        )
    )
    script = (
        _STORAGE_STUB
        + 'var CONV_KEY = "nova_drawer_conv", CONV_TOKEN_KEY = "nova_drawer_conv_token";\n'
        + "function makeDrawer() {\n  var _convId = '', _generation = 0, _chatHistory = [];\n"
        + "  var CHAT_KEY = 'nova_chat_v1', document = { getElementById: function () { return null; } };\n"
        + "  var NovaContext = { clearCampaign: function () {}, flush: function () {} };\n"
        + "  function _renderMessages() {}\n"
        + lifted
        + "\n  var api = {\n"
        + js_block(src, "    newChat() {")
        + """,
    body: function () { return {conversation_id: _conversationId(), session_token: _conversationToken()}; },
    adopt: function (data, sent) { _adoptServerConversation(data, sent); },
  };
  return api;
}
"""
        + body
    )
    return run_node(script)


def test_two_drawer_tabs_keep_their_own_conversations_after_new_chat():
    out = _two_drawer_tabs(
        _SERVER_JS.replace('cid.split("-")[0] + "-"', '"drawer-"')
        + r"""
var a = makeDrawer(), b = makeDrawer(), log = [];
function turn(tab, name) {
  var body = tab.body();
  log.push({tab: name, id: body.conversation_id});
  tab.adopt(serverAnswer(body), body.conversation_id);
}
turn(a, "A"); turn(b, "B");
a.newChat();
for (var i = 0; i < 3; i++) { turn(a, "A"); turn(b, "B"); }
console.log(JSON.stringify({log: log, minted: serverMinted}));
"""
    )
    a_ids = [e["id"] for e in out["log"] if e["tab"] == "A"]
    b_ids = [e["id"] for e in out["log"] if e["tab"] == "B"]
    assert a_ids[0] == b_ids[0]
    assert len(set(a_ids[1:])) == 1 and a_ids[1] != a_ids[0]
    assert len(set(b_ids[2:])) == 1
    assert out["minted"] <= 1
