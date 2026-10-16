"""/nova: New chat / Clear during a stream must not save the old reply.

The reader's catch kept the partial text and ran _finalizeStream, which pushed
it into the conversation object the stream was started from. Clear reused that
same object under a new id, so the abandoned reply was saved in the NEW
conversation and sent as history on the next turn. The real sendMessage /
clearActiveConversation are lifted out of static/js/nova.js and run under node
with a controllable SSE stream.
"""

from __future__ import annotations

import pytest

from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

NOVA_JS = ROOT / "static" / "js" / "nova.js"

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")

_STUB = r"""
var store = {};
var localStorage = { getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); }, removeItem: function (k) { delete store[k]; } };
function El() { this.innerHTML = ""; this.value = ""; this.style = {}; this.classList = { add: function () {}, remove: function () {} };
  this.id = ""; this.className = ""; }
El.prototype.appendChild = function (c) { this.appended = (this.appended || 0) + 1; return c; };
El.prototype.remove = function () { this.removed = true; };
El.prototype.setAttribute = function () {};
var domMap = {};
var document = {
  getElementById: function (id) { return domMap[id] || null; },
  createElement: function () { return new El(); },
};
var window = { __csrfToken: "t", __csrfTokenFetchedAt: Date.now() };
var MAX_HISTORY = 20, MAX_CHARS = 4000, STREAM_URL = "/api/chat/stream";
var chatInput = new El(), sendBtn = new El(), stopBtn = new El(), chatContainer = new El(), fileInput = new El();
var state = { conversations: {}, activeConvId: null, isLoading: false, isStreaming: false,
  abortController: null, pendingFile: null, queryCount: 0 };
var appended = [], bodies = [], renders = 0, toasts = [];
function flushSaveConversations() {} function saveConversations() {} function renderSidebar() {}
function renderChat() { renders++; } function scrollToBottom() {} function scrollToBottomIfNear() {}
function updateCharCount() {} function updateSendBtn() {} function renderFileChip() {}
function isGreetingOnly() { return false; } function updateConvTitle() {}
function showThinking() {} function hideThinking() {} function clearToolStatus() {} function showToolStatus() {}
function renderMarkdown(t) { return t; } function _novaAvatarSVG() { return ""; }
function appendMessageDOM(m) { appended.push(m); } function autoPlayTTS() {} function updateTokenIndicator() {}
function renderFollowups() {} function showToast(m) { toasts.push(m); }
function __refreshCsrfToken() { return Promise.resolve(); }
function _getOrCreateWS(cb) { cb(null); }
// a controllable SSE stream: push(text) delivers a chunk; abort rejects the read
var streams = [];
function fetch(url, opts) {
  bodies.push(JSON.parse(opts.body));
  var queue = [], waiters = [];
  var s = { push: function (txt) {
      var v = new TextEncoder().encode(txt);
      if (waiters.length) waiters.shift().resolve({done: false, value: v}); else queue.push(v); },
    end: function () { if (waiters.length) waiters.shift().resolve({done: true}); else queue.push(null); } };
  streams.push(s);
  opts.signal.addEventListener("abort", function () {
    var e = new Error("aborted"); e.name = "AbortError";
    waiters.splice(0).forEach(function (w) { w.reject(e); });
  });
  return Promise.resolve({ ok: true, body: { getReader: function () { return {
    read: function () {
      if (opts.signal.aborted) { var e = new Error("aborted"); e.name = "AbortError"; return Promise.reject(e); }
      if (queue.length) { var v = queue.shift(); return Promise.resolve(v === null ? {done: true} : {done: false, value: v}); }
      return new Promise(function (resolve, reject) { waiters.push({resolve: resolve, reject: reject}); });
    } }; } } });
}
var tick = function () { return new Promise(function (r) { setTimeout(r, 15); }); };
"""


def _lifted() -> str:
    src = NOVA_JS.read_text(encoding="utf-8")
    return "\n".join(
        js_block(src, name)
        for name in (
            "function generateId()",
            "function getActiveConv()",
            "function createConversation()",
            "function resetLoadingState()",
            "function cancelInFlightReply()",
            "function clearActiveConversation()",
            "function sendMessage()",
        )
    )


def _run(body: str):
    return run_node(
        _STUB
        + _lifted()
        + "\n(async function () {\n"
        + """
createConversation();
var oldConv = getActiveConv();
chatInput.value = "first question";
"""
        + body
        # the 120s hard timeout / 90s reader timeout would keep node alive
        + "\nprocess.exit(0);\n})().catch(function (e) {"
        " console.log(JSON.stringify({error: String(e && e.stack)})); process.exit(0); });"
    )


@pytest.mark.parametrize("reset", ["clearActiveConversation()", "cancelInFlightReply(); createConversation()"])
def test_reset_mid_stream_leaves_the_new_conversation_empty(reset):
    out = _run(
        f"""
sendMessage(); await tick();
streams[0].push('data: {{"token": "PARTIAL OLD REPLY"}}\\n'); await tick();
{reset};
await tick(); await tick();
var cur = getActiveConv();
chatInput.value = "second question"; sendMessage(); await tick();
console.log(JSON.stringify({{
  newMessages: cur.messages, sameObject: cur === oldConv, nConvs: Object.keys(state.conversations).length,
  assistantRendered: appended.filter(function (m) {{ return m.role === "assistant"; }}).length,
  secondHistory: bodies[1] && bodies[1].history, secondId: bodies[1] && bodies[1].conversation_id,
  firstId: bodies[0].conversation_id, curId: cur.id, loading: state.isLoading,
}}));
"""
    )
    assert "error" not in out, out
    assert out["sameObject"] is False
    assert out["assistantRendered"] == 0
    # only the new user message (just pushed by the second send) is in the new conversation
    assert [m["content"] for m in out["newMessages"]] == ["second question"]
    assert out["secondHistory"] == [{"role": "user", "content": "second question"}]
    assert out["secondId"] == out["curId"] and out["curId"] != out["firstId"]


def test_normal_stream_still_saves_the_reply():
    out = _run(
        """
sendMessage(); await tick();
streams[0].push('data: {"token": "Hello there"}\\n');
streams[0].push('data: {"done": true, "full_response": "Hello there", "confidence": 0.9}\\n');
streams[0].end(); await tick(); await tick();
console.log(JSON.stringify({msgs: getActiveConv().messages.map(function (m) { return m.role + ":" + m.content; }),
  loading: state.isLoading}));
"""
    )
    assert out["msgs"] == ["user:first question", "assistant:Hello there"]
    assert out["loading"] is False


def test_late_completion_after_the_active_conversation_changed_is_dropped():
    """Even when nothing cancelled the stream, a reply that completes after the
    active conversation changed must not be saved or rendered anywhere."""
    out = _run(
        """
sendMessage(); await tick();
streams[0].push('data: {"token": "LATE OLD REPLY"}\\n'); await tick();
createConversation();                       // active conversation changes, stream untouched
var cur = getActiveConv();
streams[0].push('data: {"done": true, "full_response": "LATE OLD REPLY"}\\n');
streams[0].end(); await tick(); await tick();
console.log(JSON.stringify({newMessages: cur.messages, oldMessages: oldConv.messages.map(function (m) { return m.role; }),
  assistantRendered: appended.filter(function (m) { return m.role === "assistant"; }).length}));
"""
    )
    assert out["newMessages"] == []
    assert out["assistantRendered"] == 0
    assert out["oldMessages"] == ["user"]
