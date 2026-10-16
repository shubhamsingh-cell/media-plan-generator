"""Every Nova chat surface has a visible "New chat" that really starts over.

Before: the floating widget only cleared via a hidden Ctrl/Cmd+Shift+L (and
kept its conversation id, so server-side memory carried over); the platform
drawer had no reset at all (history in ``nova_chat_v1`` and the campaign in
``nova_context_v2`` lived on); on /nova "Clear chat" kept the conversation id
and "New Chat" sat in a sidebar that is off-screen at <=1024px.

The REAL handlers are lifted out of the shipped JS and run under node against
stub DOM/storage, so these fail on the pre-fix files.
"""

from __future__ import annotations

import re

import pytest

from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

WIDGET_JS = ROOT / "static" / "nova-chat.js"
PLATFORM_JS = ROOT / "static" / "js" / "platform.js"
NOVA_JS = ROOT / "static" / "js" / "nova.js"
NOVA_HTML = ROOT / "templates" / "nova.html"
PLATFORM_HTML = ROOT / "templates" / "platform.html"

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

_STORAGE = r"""
var store = {};
var localStorage = {
  getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); },
  removeItem: function (k) { delete store[k]; },
};
"""


# ---------------------------------------------------------------------------
# Floating widget (static/nova-chat.js)
# ---------------------------------------------------------------------------
def _widget_js() -> str:
    src = WIDGET_JS.read_text(encoding="utf-8")
    parts = [
        js_block(src, "function secureRandomId()"),
        js_block(src, "function mintSessionId()"),
        js_block(src, "function getSessionId()"),
        js_block(src, "function saveHistory("),
        js_block(src, "function clearConversation("),
    ]
    return "\n".join(parts)


_WIDGET_STUB = (
    _STORAGE
    + r"""
var CONFIG = { storageKey: "nova_chat_history", sessionKey: "nova_session",
               maxHistoryStorage: 50 };
var state = { messages: [{role: "user", content: "hi"}], sessionId: "nova-OLD",
              isLoading: true };
var welcomeShown = 0, hid = { stop: 0, typing: 0 }, focused = 0;
var messagesDiv = { innerHTML: "<p>old</p>" };
var sendBtn = { disabled: true };
var input = { focus: function () { focused++; } };
var window = { location: { pathname: "/x" } };
var document = { getElementById: function (id) {
  return id === "nova-messages" ? messagesDiv : id === "nova-send-btn" ? sendBtn
       : id === "nova-input" ? input : null; } };
function showWelcome() { welcomeShown++; }
function hideStopButton() { hid.stop++; }
function hideTyping() { hid.typing++; }
var _activeAbortCtrl = null;
"""
)


@needs_node
def test_widget_new_chat_rotates_id_and_clears_storage():
    out = run_node(
        _WIDGET_STUB
        + _widget_js()
        + r"""
store.nova_chat_history = JSON.stringify([{role: "user", content: "hi"}]);
store.nova_session = "nova-OLD";
var aborted = [];
_activeAbortCtrl = { abort: function () { aborted.push(1); } };
var ctrl = _activeAbortCtrl;
state._pendingBranch = {branched_from: 1};
clearConversation("new_chat_button");
console.log(JSON.stringify({
  sid: state.sessionId, storedSid: store.nova_session,
  history: store.nova_chat_history === undefined ? null : store.nova_chat_history,
  messages: state.messages, loading: state.isLoading, sendDisabled: sendBtn.disabled,
  innerHTML: messagesDiv.innerHTML, welcome: welcomeShown, aborted: aborted.length,
  reset: ctrl._isReset === true, cancel: ctrl._isUserCancel === true,
  ctrlCleared: _activeAbortCtrl === null, branch: "_pendingBranch" in state,
  stopHidden: hid.stop, typingHidden: hid.typing, focused: focused,
}));
"""
    )
    assert out["sid"] != "nova-OLD" and out["sid"].startswith("nova-")
    assert out["storedSid"] == out["sid"]  # persisted: next page load keeps the new id
    assert out["history"] is None  # persisted history key removed
    assert out["messages"] == [] and out["innerHTML"] == ""
    assert out["welcome"] == 1
    assert out["aborted"] == 1 and out["reset"] and out["cancel"] and out["ctrlCleared"]
    assert out["loading"] is False and out["sendDisabled"] is False
    assert out["branch"] is False
    assert out["stopHidden"] == 1 and out["typingHidden"] == 1 and out["focused"] == 1


@needs_node
def test_widget_two_new_chats_never_reuse_an_id():
    out = run_node(
        _WIDGET_STUB
        + _widget_js()
        + r"""
var ids = {};
for (var i = 0; i < 50; i++) { clearConversation(); ids[state.sessionId] = 1; }
console.log(JSON.stringify({n: Object.keys(ids).length}));
"""
    )
    assert out["n"] == 50


def test_widget_header_has_labelled_new_chat_button_and_handler():
    src = WIDGET_JS.read_text(encoding="utf-8")
    assert re.search(r'<button type="button" class="nova-newchat-btn" aria-label="New chat"', src)
    assert '.querySelector(".nova-newchat-btn")' in src
    assert 'clearConversation("new_chat_button")' in src
    # reduced motion: the button's transition is switched off
    reduced = src[src.index("prefers-reduced-motion: reduce) {\" +") :][:900]
    assert ".nova-newchat-btn" in reduced
    # an abort caused by New chat must not post "Response was stopped."
    assert "if (abortCtrl._isReset) return;" in src


# ---------------------------------------------------------------------------
# Platform drawer (static/js/platform.js + templates/platform.html)
# ---------------------------------------------------------------------------
def _drawer_js() -> str:
    src = PLATFORM_JS.read_text(encoding="utf-8")
    return (
        js_block(src, "const NovaContext = (() =>")
        + ")();\n"
        + "const MODULES = [];\n"
        + js_block(src, "function _escHtml(")
        + "\n"
        + js_block(src, "const NovaDrawer = (() =>")
        + ")();\n"
    )


_DRAWER_STUB = (
    _STORAGE
    + r"""
function El() { this.innerHTML = ""; this.value = ""; this.scrollTop = 0; this.scrollHeight = 0;
  this.classList = { add: function () {}, remove: function () {} };
  this.focus = function () {}; }
El.prototype.appendChild = function () {}; El.prototype.remove = function () {};
var els = {};
var window = { addEventListener: function () {}, innerWidth: 1400 };
var document = {
  getElementById: function (id) { return els[id] || (els[id] = new El()); },
  querySelector: function () { return null; },
  createElement: function () {
    var e = new El();
    Object.defineProperty(e, "textContent", { set: function (v) { e.innerHTML = v; } });
    return e;
  },
};
function _updateCampaignUI() {}
var NovaRouter = { navigate: function () {} };
var fetches = [];
function fetch(url, opts) {
  fetches.push({url: url, body: opts && opts.body ? JSON.parse(opts.body) : null});
  return new Promise(function () {});  // never resolves: reply stays in flight
}
"""
)


@needs_node
def test_drawer_new_chat_rotates_id_clears_history_and_campaign_context():
    out = run_node(
        _DRAWER_STUB
        + _drawer_js()
        + r"""
store.nova_chat_v1 = JSON.stringify([{role: "user", text: "old question"}]);
NovaContext.setCampaign({name: "Acme Health Q4", budget: 50000});
NovaContext.flush();
var before = JSON.parse(store.nova_context_v2).campaign;
els["nova-input"] = Object.assign(new El(), {value: "hello there"});
NovaDrawer.send();
var firstId = fetches[0].body.conversation_id;
NovaDrawer.newChat();
var welcome = els["nova-messages"].innerHTML.indexOf("Hey, I'm Nova") >= 0;
var after = JSON.parse(store.nova_context_v2).campaign;
els["nova-input"].value = "second question";
NovaDrawer.send();
console.log(JSON.stringify({
  before: before, after: after, firstId: firstId,
  secondId: fetches[1].body.conversation_id, storedId: store.nova_drawer_conv,
  history: "nova_chat_v1" in store ? store.nova_chat_v1 : null,
  rendered: welcome,
  liveCampaign: NovaContext.campaign.name,
}));
"""
    )
    assert out["before"]["name"] == "Acme Health Q4"
    assert out["after"]["name"] == "" and out["after"]["budget"] == 0
    assert out["liveCampaign"] == ""
    assert out["firstId"].startswith("drawer-")
    assert out["secondId"].startswith("drawer-") and out["secondId"] != out["firstId"]
    assert out["storedId"] == out["secondId"]
    assert out["history"] is None  # old transcript is gone (the new turn is not saved yet)
    assert out["rendered"]


@needs_node
def test_drawer_reply_arriving_after_new_chat_is_dropped():
    out = run_node(
        _DRAWER_STUB.replace(
            "return new Promise(function () {});  // never resolves: reply stays in flight",
            "return Promise.resolve({ok: true, json: function () {"
            " return Promise.resolve({response: 'LATE ANSWER'}); }});",
        )
        + _drawer_js()
        + r"""
(async function () {
  els["nova-input"] = Object.assign(new El(), {value: "question"});
  NovaDrawer.send();
  NovaDrawer.newChat();           // user starts over before the reply lands
  await new Promise(function (r) { setTimeout(r, 20); });
  console.log(JSON.stringify({
    saved: store.nova_chat_v1 || null,
    shown: els["nova-messages"].innerHTML.indexOf("LATE ANSWER") >= 0,
  }));
})();
"""
    )
    assert out["saved"] is None
    assert out["shown"] is False


def test_platform_drawer_header_has_labelled_new_chat_button():
    html = PLATFORM_HTML.read_text(encoding="utf-8")
    m = re.search(r"<button[^>]*id=\"nova-drawer-new\"[^>]*>", html, re.S)
    assert m, "drawer header lacks the New chat button"
    tag = m.group(0)
    assert 'aria-label="New chat"' in tag and 'type="button"' in tag
    assert "NovaDrawer.newChat()" in tag
    header = html[html.index('class="nova-drawer-header"') :]
    assert header.index("nova-drawer-new") < header.index("nova-drawer-close")
    css = (ROOT / "static" / "css" / "platform.css").read_text(encoding="utf-8")
    assert ".nova-drawer-new:focus-visible" in css
    assert re.search(
        r"prefers-reduced-motion: reduce\) \{\s*\.nova-drawer-new \{\s*transition: none", css
    )


# ---------------------------------------------------------------------------
# /nova full page (static/js/nova.js + templates/nova.html)
# ---------------------------------------------------------------------------
@needs_node
def test_nova_page_clear_chat_mints_a_new_conversation_id():
    src = NOVA_JS.read_text(encoding="utf-8")
    script = (
        _STORAGE
        + r"""
var state = { conversations: {}, activeConvId: null, isLoading: false };
var flushed = 0, sidebars = 0, chats = 0, cancelled = 0;
function flushSaveConversations() { flushed++; }
function renderSidebar() { sidebars++; }
function renderChat() { chats++; }
function resetLoadingState() { cancelled++; state.isLoading = false; }
var document = { getElementById: function () { return null; } };
"""
        + js_block(src, "function secureRandomId()")
        + js_block(src, "function generateId()")
        + js_block(src, "function getActiveConv()")
        + js_block(src, "function cancelInFlightReply()")
        + js_block(src, "function clearActiveConversation()")
        + r"""
state.conversations["conv-OLD"] = {id: "conv-OLD", title: "Budget talk",
  messages: [{role: "user", content: "x"}], createdAt: 1, updatedAt: 1};
state.activeConvId = "conv-OLD";
state.isLoading = true;
clearActiveConversation();
var ids = Object.keys(state.conversations);
var c = state.conversations[state.activeConvId];
console.log(JSON.stringify({ids: ids, active: state.activeConvId, msgs: c.messages,
  title: c.title, idField: c.id, flushed: flushed, cancelled: cancelled,
  userCancelled: state._userCancelled === true}));
"""
    )
    out = run_node(script)
    assert out["ids"] == [out["active"]] and out["active"] != "conv-OLD"
    assert out["idField"] == out["active"]
    assert out["msgs"] == [] and out["title"] == "New Chat"
    assert out["flushed"] == 1 and out["cancelled"] == 1 and out["userCancelled"]


def test_nova_page_new_chat_button_lives_in_the_always_visible_topbar():
    html = NOVA_HTML.read_text(encoding="utf-8")
    m = re.search(r"<button[^>]*id=\"topbar-new-chat-btn\"[^>]*>", html, re.S)
    assert m, "topbar lacks a New chat button"
    assert 'aria-label="New chat"' in m.group(0) and 'type="button"' in m.group(0)
    # Not inside the (collapsed at <=1024px) sidebar: it follows </aside>.
    assert html.index("topbar-new-chat-btn") > html.index("</aside>")
    assert html.index('class="topbar"') < html.index("topbar-new-chat-btn")
    js = NOVA_JS.read_text(encoding="utf-8")
    assert 'getElementById("topbar-new-chat-btn")' in js
