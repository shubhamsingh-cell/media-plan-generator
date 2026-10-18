"""Nova chat UX: a chat that can be closed, started over and retried.

Internal feedback (2026-10): the "Meet Nova" intro covered most of /nova with
no way to close it and pushed the real chat off-screen; there was no obvious
way to start over; the chat "often" stopped working; and the answer felt like
an overview rather than a plan.

What these tests pin, against the shipped files:

* /nova: the intro opens compact (title row only), the example is opt-in,
  the whole block can be hidden, it steps aside once a chat starts, and
  New chat has a visible label at >= 768px.
* Floating widget: on a phone the panel is a bounded bottom sheet (<= 85% of
  the height, safe-area aware) rather than a full-bleed takeover; Close and
  New chat carry text, not just an icon; Escape closes; focus returns to the
  launcher; the open state survives a page change.
* Widget reliability: errors say what to do (never a raw "HTTP 403"), error
  text is never injected as HTML, Retry resends without touching the input
  (a draft typed after the failure survives) and without duplicating the
  user's bubble, and "New chat" drops the stored session token, which belongs
  to the OLD conversation id and makes the server answer 403 to the first
  message of the new one.

The real functions are lifted out of the shipped JS and run under node against
stub DOM/storage, so these fail on the pre-fix files.
"""

from __future__ import annotations

import re

import pytest

from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

WIDGET_JS = ROOT / "static" / "nova-chat.js"
NOVA_HTML = ROOT / "templates" / "nova.html"

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

_STORAGE = r"""
var store = {};
var localStorage = {
  getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); },
  removeItem: function (k) { delete store[k]; },
};
"""


def _widget_src() -> str:
    return WIDGET_JS.read_text(encoding="utf-8")


def _lift_or_noop(src: str, signature: str, noop: str) -> str:
    """Lift ``signature`` from the shipped JS, or fall back to ``noop`` when the
    shipped file does not define it. Lets a behavioural test fail on its ASSERTION
    against code that predates the helper, instead of dying on a missing lift."""
    return js_block(src, signature) if signature in src else noop


def _html() -> str:
    return NOVA_HTML.read_text(encoding="utf-8")


def _persona_style() -> str:
    html = _html()
    start = html.index("<!-- Meet Nova: persona hero")
    return html[start : html.index("</style>", start)]


# ---------------------------------------------------------------------------
# Widget: error copy, retry, draft, stale session token
# ---------------------------------------------------------------------------
@needs_node
def test_widget_error_copy_says_what_to_do_not_a_raw_status():
    src = _widget_src()
    out = run_node(
        js_block(src, "function friendlyChatError(")
        + r"""
var cases = ["HTTP 401", "HTTP 403", "HTTP 429", "HTTP 502", "HTTP 404",
  "WebSocket closed", "WebSocket error", "Failed to fetch", "",
  "Response was stopped.", "I'm having trouble connecting. Please try again."];
console.log(JSON.stringify(cases.map(function (c) { return friendlyChatError(c); })));
"""
    )
    c401, c403, c429, c502, c404, wsc, wse, fetch_, empty, stopped, trouble = out
    assert "sign in" in c401.lower() and "Retry" in c401
    assert "expired" in c403.lower() and "Retry" in c403
    assert "busy" in c429.lower() and "Retry" in c429
    assert "temporarily unavailable" in c502.lower()
    assert "New chat" in c404
    for dropped in (wsc, wse, fetch_, empty):
        assert dropped == "The connection to Nova dropped. Press Retry."
    # Text already written for a person passes through unchanged.
    assert stopped == "Response was stopped."
    assert trouble == "I'm having trouble connecting. Please try again."
    for text in out:
        assert not re.search(r"\bHTTP\s*\d{3}\b", text), text


_DOM_STUB = r"""
function El(tag) {
  this.tag = tag; this.children = []; this.style = {}; this.attrs = {};
  this.listeners = {}; this.removed = false; this._text = ""; this.className = "";
  this.innerHTMLWrites = [];
}
El.prototype.appendChild = function (c) { this.children.push(c); return c; };
El.prototype.setAttribute = function (k, v) { this.attrs[k] = v; };
El.prototype.addEventListener = function (t, fn) { this.listeners[t] = fn; };
El.prototype.remove = function () { this.removed = true; };
El.prototype.scrollTo = function () {};
Object.defineProperty(El.prototype, "textContent", {
  get: function () { return this._text; }, set: function (v) { this._text = String(v); } });
Object.defineProperty(El.prototype, "innerHTML", {
  get: function () { return ""; }, set: function (v) { this.innerHTMLWrites.push(v); } });
var messagesDiv = new El("div");
var inputEl = new El("textarea"); inputEl.value = "a new draft the visitor is typing";
var document = {
  getElementById: function (id) {
    return id === "nova-messages" ? messagesDiv : id === "nova-input" ? inputEl : null; },
  createElement: function (t) { return new El(t); },
};
var sent = [];
function sendMessage(text, opts) { sent.push({text: text, opts: opts}); }
"""


@needs_node
def test_widget_retry_button_resends_without_touching_the_draft_or_injecting_html():
    src = _widget_src()
    out = run_node(
        _DOM_STUB
        + js_block(src, "function friendlyChatError(")
        + js_block(src, "function showStreamError(")
        + r"""
showStreamError("<img src=x onerror=alert(1)>", "original question");
var errEl = messagesDiv.children[0];
var textEl = errEl.children[0], retry = errEl.children[1];
var before = inputEl.value;
retry.listeners.click();
showStreamError("Response was stopped.", "q2", true);
var aborted = messagesDiv.children[1];
console.log(JSON.stringify({
  role: errEl.attrs.role, errClass: errEl.className,
  shown: textEl.textContent, htmlWrites: errEl.innerHTMLWrites.length + textEl.innerHTMLWrites.length,
  retryText: retry.textContent, retryClass: retry.className, retryType: retry.type,
  retryAria: retry.attrs["aria-label"], sent: sent, removed: errEl.removed,
  draftBefore: before, draftAfter: inputEl.value,
  abortedButtons: aborted.children.length,
}));
"""
    )
    assert out["role"] == "alert"
    # The raw string is data, not markup: textContent only, never innerHTML.
    assert out["shown"] == "<img src=x onerror=alert(1)>"
    assert out["htmlWrites"] == 0
    assert out["retryText"] == "Retry" and out["retryClass"] == "nova-retry-btn"
    assert out["retryType"] == "button" and "Retry" in out["retryAria"]
    # One click resends the ORIGINAL text as a retry, and the new draft stays put.
    assert out["sent"] == [{"text": "original question", "opts": {"isRetry": True}}]
    assert out["removed"] is True
    assert (
        out["draftBefore"] == out["draftAfter"] == "a new draft the visitor is typing"
    )
    assert out["abortedButtons"] == 1  # a user stop offers no Retry


@needs_node
def test_widget_send_text_and_retry_do_not_duplicate_the_user_bubble():
    src = _widget_src()
    out = run_node(
        js_block(src, "function resolveSendText(")
        + js_block(src, "function shouldAppendUserMessage(")
        + js_block(src, "function isWsTransportError(")
        + r"""
var input = {value: "  typed text  "};
var evt = {type: "click"};  // the send button hands its Event to sendMessage
var msgs = [{role: "assistant", content: "hi"}, {role: "user", content: "plan please"}];
console.log(JSON.stringify({
  fromInput: resolveSendText(undefined, input),
  fromEvent: resolveSendText(evt, input),
  fromString: resolveSendText("  resend me ", input),
  noInput: resolveSendText(undefined, null),
  freshSend: shouldAppendUserMessage(msgs, "plan please", false),
  retrySame: shouldAppendUserMessage(msgs, "plan please", true),
  retryOther: shouldAppendUserMessage(msgs, "something else", true),
  retryEmpty: shouldAppendUserMessage([], "x", true),
  retryAfterAnswer: shouldAppendUserMessage(
    msgs.concat([{role: "assistant", content: "ok"}]), "plan please", true),
  wsClosed: isWsTransportError("WebSocket closed"),
  wsError: isWsTransportError("WebSocket error"),
  wsTimeout: isWsTransportError("WebSocket timeout"),
  serverText: isWsTransportError("Rate limit exceeded"),
}));
"""
    )
    assert out["fromInput"] == "typed text"
    assert out["fromEvent"] == "typed text"  # an Event is not retry text
    assert out["fromString"] == "resend me"
    assert out["noInput"] == ""
    assert out["freshSend"] is True
    assert out["retrySame"] is False  # already in the transcript
    assert out["retryOther"] is True and out["retryEmpty"] is True
    assert out["retryAfterAnswer"] is True  # it was answered; this is a new ask
    # Only an instant transport failure falls back silently; a 90 s timeout and
    # server-reported errors are shown to the visitor.
    assert out["wsClosed"] and out["wsError"]
    assert not out["wsTimeout"] and not out["serverText"]


def test_widget_send_message_keeps_the_input_on_retry_and_retries_once_per_cause():
    src = _widget_src()
    head = src[src.index("function sendMessage(overrideText, opts)") :]
    head = head[: head.index("// Get CSRF token")]
    assert 'var fromInput = typeof overrideText !== "string";' in head
    assert "shouldAppendUserMessage(state.messages, text, isRetry)" in head
    # the input is cleared only when the text came from the input box
    assert re.search(r"if \(fromInput\) \{\s*input\.value = \"\";", head)
    assert "updateCharCounter(fromInput ? 0 : input.value.length)" in src
    # WS dropped before any token -> pin to SSE and resend once, silently
    assert (
        "transportRetried: true" in src
        and "_widgetWsFailCount = _WIDGET_WS_MAX_FAILS;" in src
    )
    # 403 -> clear the stale token, refresh CSRF, resend once
    assert re.search(r"/\^HTTP 403/\.test\(err\.message", src)
    assert "authRetried: true" in src and "clearStoredSessionToken();" in src


@needs_node
def test_widget_new_chat_drops_the_stored_session_token():
    """The token is bound to the conversation row it was issued for. Kept across
    New chat it is presented with the NEW id, the server answers 403 "invalid
    session token", and every Retry repeats it."""
    src = _widget_src()
    out = run_node(
        _STORAGE
        + r"""
var CONFIG = { storageKey: "nova_chat_history", sessionKey: "nova_session",
               sessionTokenKey: "nova_session_token", maxHistoryStorage: 50 };
var state = { messages: [{role: "user", content: "hi"}], sessionId: "nova-OLD", isLoading: false };
var messagesDiv = { innerHTML: "<p>old</p>" };
var sendBtn = { disabled: false };
var input = { focus: function () {} };
var window = { location: { pathname: "/x" } };
var document = { getElementById: function (id) {
  return id === "nova-messages" ? messagesDiv : id === "nova-send-btn" ? sendBtn
       : id === "nova-input" ? input : null; } };
function showWelcome() {}
function hideStopButton() {}
function hideTyping() {}
var _activeAbortCtrl = null;
"""
        + js_block(src, "function secureRandomId()")
        + js_block(src, "function mintSessionId()")
        + js_block(src, "function clearConversation(")
        + r"""
store.nova_session_token = "TOKEN-FOR-THE-OLD-CONVERSATION";
store.nova_chat_history = "[]";
clearConversation("new_chat_button");
console.log(JSON.stringify({
  token: "nova_session_token" in store ? store.nova_session_token : null,
  history: "nova_chat_history" in store ? store.nova_chat_history : null,
  sid: state.sessionId,
}));
"""
    )
    assert out["token"] is None
    assert out["history"] is None
    assert out["sid"] != "nova-OLD"


# ---------------------------------------------------------------------------
# Widget: open/close, focus return, persistence
# ---------------------------------------------------------------------------
@needs_node
def test_widget_open_state_helpers_survive_blocked_storage_and_gate_restore():
    src = _widget_src()
    out = run_node(
        r"""
var OPEN_STATE_KEY = "nova_widget_open";
var ss = {};
var sessionStorage = {
  getItem: function (k) { return k in ss ? ss[k] : null; },
  setItem: function (k, v) { ss[k] = String(v); },
};
"""
        + js_block(src, "function persistOpenState(")
        + js_block(src, "function readOpenState(")
        + js_block(src, "function shouldRestoreOpen(")
        + r"""
persistOpenState(true); var openedRead = readOpenState();
persistOpenState(false); var closedRead = readOpenState();
var raw = ss.nova_widget_open;
sessionStorage = { getItem: function () { throw new Error("blocked"); },
                   setItem: function () { throw new Error("blocked"); } };
var blocked = {};
try { persistOpenState(true); blocked.write = "ok"; } catch (e) { blocked.write = "threw"; }
try { blocked.read = readOpenState(); } catch (e) { blocked.read = "threw"; }
console.log(JSON.stringify({
  openedRead: openedRead, closedRead: closedRead, raw: raw, blocked: blocked,
  restoreWide: shouldRestoreOpen(true, 1200, 640, 3),
  restorePhone: shouldRestoreOpen(true, 375, 640, 3),
  restoreEmpty: shouldRestoreOpen(true, 1200, 640, 0),
  restoreWasClosed: shouldRestoreOpen(false, 1200, 640, 3),
}));
"""
    )
    assert (
        out["openedRead"] is True and out["closedRead"] is False and out["raw"] == "0"
    )
    assert out["blocked"] == {"write": "ok", "read": False}
    assert out["restoreWide"] is True
    # never over a phone's page by itself, never an empty panel, never once closed
    assert out["restorePhone"] is False
    assert out["restoreEmpty"] is False
    assert out["restoreWasClosed"] is False


_TOGGLE_STUB = r"""
var OPEN_STATE_KEY = "nova_widget_open";
var ss = {};
var sessionStorage = { getItem: function (k) { return k in ss ? ss[k] : null; },
                       setItem: function (k, v) { ss[k] = String(v); } };
var CONFIG = { mobileBreakpoint: 640 };
var ICONS = { close: "x" };
function El() { this.cls = {}; this.attrs = {}; this.focused = 0; this.children = [];
  this.style = {}; this.title = "";
  var self = this;
  this.classList = {
    add: function () { for (var i = 0; i < arguments.length; i++) self.cls[arguments[i]] = 1; },
    remove: function () { for (var i = 0; i < arguments.length; i++) delete self.cls[arguments[i]]; },
    contains: function (c) { return !!self.cls[c]; } }; }
El.prototype.setAttribute = function (k, v) { this.attrs[k] = v; };
El.prototype.appendChild = function (c) { this.children.push(c); return c; };
El.prototype.focus = function () { this.focused++; };
El.prototype.remove = function () {};
El.prototype.contains = function (el) { return !!el && el.inside === this; };
var panel = new El(), btn = new El(), orb = new El();
var inputEl = new El(); var posthogEvents = [];
var state = { isOpen: false, chatPanel: panel, floatingBtn: btn, orbCanvas: orb,
              orbAnimId: null, orbDrawFn: null, chatOpenedAt: 0, messagesSentCount: 0 };
var window = { innerWidth: 1200, posthog: { capture: function (n) { posthogEvents.push(n); } },
               location: { pathname: "/" } };
var activeEl = null;
var document = { hidden: false, body: { tag: "body" },
  get activeElement() { return activeEl; },
  getElementById: function (id) { return id === "nova-input" ? inputEl : null; },
  createElement: function () { return new El(); } };
var _widgetWsConn = null;
var WebSocket = { OPEN: 1 };
function _widgetGetOrCreateWS() {}
function cancelAnimationFrame() {} function requestAnimationFrame() { return 1; }
"""


@needs_node
def test_widget_close_returns_focus_to_the_launcher_and_persists_the_state():
    src = _widget_src()
    out = run_node(
        _TOGGLE_STUB
        + _lift_or_noop(
            src, "function persistOpenState(", "function persistOpenState() {}"
        )
        + js_block(src, "function togglePanel(")
        + r"""
var r = {};
togglePanel();                       // open from the launcher
r.openStored = ss.nova_widget_open || null; r.openCls = Object.keys(panel.cls).sort();
activeEl = { inside: panel };        // focus is inside the panel (typing)
togglePanel(true);                   // Close button / Escape
r.closeStored = ss.nova_widget_open || null; r.closedCls = Object.keys(panel.cls).sort();
r.focusAfterClose = btn.focused; r.btnAria = btn.attrs["aria-label"];

btn.focused = 0; var trackedBefore = posthogEvents.length;
togglePanel(false, true);            // silent restore (page change)
r.restoredOpen = state.isOpen; r.focusOnRestore = btn.focused;
r.postedOnRestore = posthogEvents.length - trackedBefore;
activeEl = { inside: "somewhere else" };     // visitor is in a wizard field
btn.focused = 0; togglePanel(true);
r.focusKeptElsewhere = btn.focused;
console.log(JSON.stringify(r));
"""
    )
    assert out["openStored"] == "1" and out["closeStored"] == "0"
    assert out["openCls"] == ["nova-visible"] and out["closedCls"] == ["nova-hidden"]
    assert out["focusAfterClose"] == 1  # keyboard users land on the launcher
    assert out["btnAria"] == "Open Nova Chat"
    assert out["restoredOpen"] is True
    assert out["focusOnRestore"] == 0  # restoring must not pull focus
    assert out["postedOnRestore"] == 0  # a restore is not a visitor "opened chat" event
    assert out["focusKeptElsewhere"] == 0  # never steal focus from a page field


def test_widget_escape_closes_menus_first_then_the_panel_and_buttons_say_what_they_do():
    src = _widget_src()
    esc = src[src.index('if (e.key === "Escape" && state.isOpen)') :][:900]
    assert esc.index(".nova-settings-open, .nova-export-open") < esc.index(
        "togglePanel(true)"
    )
    # Close and New chat carry text, with an accessible name and a tooltip.
    assert re.search(
        r'<button type="button" class="nova-close-btn" aria-label="Close chat" title="Close chat \(Esc\)[^"]*">',
        src,
    )
    assert "'<span class=\"nova-btn-label\">Close</span>'" in src
    assert "'<span class=\"nova-btn-label\">New chat</span>'" in src
    assert re.search(
        r'class="nova-newchat-btn" aria-label="New chat" title="New chat', src
    )
    # The New chat text shows from 768px; below it the icon keeps its aria-label.
    assert ".nova-newchat-btn .nova-btn-label { display: none; }" in src
    assert (
        "@media (min-width: 768px) { .nova-newchat-btn .nova-btn-label { display: inline; } }"
        in src
    )


def test_widget_phone_panel_is_a_bounded_bottom_sheet_not_a_takeover():
    src = _widget_src()
    i = src.index('"@media (max-width: " +')
    block = src[i : src.index("// ── Streaming cursor ──", i)]
    assert "100vh" not in block and "100vw" not in block  # no full-bleed panel
    assert "top: auto" in block
    assert "env(safe-area-inset-bottom)" in block
    heights = re.findall(r"(?<![-\w])height: calc\((\d+)(?:d)?vh", block)
    assert heights and all(int(h) <= 85 for h in heights), heights
    assert "85dvh" in block  # iOS: the dynamic viewport, not the address-bar-blind one
    # a phone-sized touch target on the dismissal control
    assert re.search(r"\.nova-close-btn \{ min-height: 44px; \}", block)
    # desktop panel is capped too
    assert "max-height: min(calc(100vh - 120px), 85vh)" in src


def test_widget_new_controls_respect_reduced_motion_and_the_11px_floor():
    src = _widget_src()
    reduced = src[src.index('"@media (prefers-reduced-motion: reduce) {" +') :][:700]
    for sel in (
        ".nova-retry-btn",
        ".nova-close-btn",
        ".nova-newchat-btn",
        "#nova-panel",
    ):
        assert sel in reduced, sel
    for sel in (".nova-close-btn", ".nova-newchat-btn", ".nova-retry-btn"):
        rule = re.search(re.escape(sel) + r" \{[^}]*font-size: (\d+(?:\.\d+)?)px", src)
        assert rule and float(rule.group(1)) >= 11, sel
    assert (
        ".nova-close-btn:focus-visible" in src
        and ".nova-retry-btn:focus-visible" in src
    )


# ---------------------------------------------------------------------------
# /nova: the intro never owns the page
# ---------------------------------------------------------------------------
@needs_node
def test_nova_page_intro_state_helpers():
    html = _html()
    out = run_node(
        js_block(html, "function novaPersonaNormalize(")
        + js_block(html, "function novaPersonaConversationStarted(")
        + js_block(html, "function novaPersonaToggleLabel(")
        + r"""
var withMsg = { querySelector: function (s) { return s === "#chat-container .message" ? {} : null; } };
var noMsg = { querySelector: function () { return null; } };
console.log(JSON.stringify({
  unset: novaPersonaNormalize(null), junk: novaPersonaNormalize("loud"),
  expanded: novaPersonaNormalize("expanded"), hidden: novaPersonaNormalize("hidden"),
  convo: novaPersonaConversationStarted(withMsg), empty: novaPersonaConversationStarted(noMsg),
  openLabel: novaPersonaToggleLabel("expanded"), closedLabel: novaPersonaToggleLabel("compact"),
}));
"""
    )
    # First visit and any unknown stored value open COMPACT, never expanded.
    assert out["unset"] == "compact" and out["junk"] == "compact"
    assert out["expanded"] == "expanded" and out["hidden"] == "hidden"
    assert out["convo"] is True and out["empty"] is False
    assert out["openLabel"] == "Hide example" and out["closedLabel"] == "See an example"


def test_nova_page_intro_is_compact_collapsible_dismissible_and_bounded():
    html = _html()
    style = _persona_style()
    assert re.search(r'id="nova-persona"[^>]*data-state="compact"', html, re.S)
    # compact hides the example; hidden hides the hero; a started chat hides it all
    assert re.search(r'\[data-state="compact"\] \.np-demo,', style)
    assert re.search(r'\[data-state="hidden"\] \.np-hero,', style)
    assert re.search(r'\[data-convo="true"\] \{\s*display: none;', style)
    # expanded is bounded and scrolls inside itself instead of owning the page
    expanded = re.search(r'\[data-state="expanded"\] \{([^}]*)\}', style)
    assert expanded and "max-height: 55dvh" in expanded.group(1)
    assert "overflow-y: auto" in expanded.group(1)
    # labelled controls (text, not just an icon) with disclosure semantics
    toggle = re.search(r'<button[^>]*id="nova-persona-toggle"[^>]*>', html, re.S).group(
        0
    )
    assert (
        'aria-expanded="false"' in toggle
        and 'aria-controls="nova-persona-demo"' in toggle
    )
    assert "See an example" in html and "Hide intro" in html and "Show intro" in html
    # #chat-container is parsed AFTER the intro script: the observer that hides the
    # intro when a chat starts must wait for the document, or it never attaches.
    assert 'document.addEventListener("DOMContentLoaded", watchConversation)' in html
    assert re.search(
        r"function watchConversation\(\) \{.*?new MutationObserver\(syncConversation\)",
        html,
        re.S,
    )
    # the choice is remembered, with storage failures swallowed
    assert 'var STORE_KEY = "nova_persona_state";' in html
    assert re.search(
        r"try \{\s*localStorage\.setItem\(STORE_KEY, state\);\s*\} catch", html
    )
    # animation honours reduced motion; no text under 11px in the intro styles
    assert re.search(
        r"prefers-reduced-motion: reduce\) \{\s*\.np-bubble \{[^}]*\}\s*\.np-btn,",
        style,
    )
    sizes = [float(v) for v in re.findall(r"font-size:\s*(\d+(?:\.\d+)?)px", style)]
    assert sizes and min(sizes) >= 11, sorted(set(sizes))[:3]
    # 44px touch targets on a phone
    assert re.search(
        r"max-width: 640px\) \{.*?\.np-btn \{\s*min-height: 44px;", style, re.S
    )


def test_nova_page_new_chat_has_a_visible_label_from_768px():
    html = _html()
    btn = re.search(
        r'<button[^>]*id="topbar-new-chat-btn"[^>]*>.*?</button>', html, re.S
    ).group(0)
    assert 'aria-label="New chat"' in btn and 'title="New chat' in btn
    assert '<span class="topbar-btn-label">New chat</span>' in btn
    style = _persona_style()
    assert re.search(
        r"#topbar-new-chat-btn \.topbar-btn-label \{\s*display: none;", style
    )
    assert re.search(
        r"@media \(min-width: 768px\) \{\s*#topbar-new-chat-btn \{[^}]*width: auto;"
        r".*?\.topbar-btn-label \{\s*display: inline;",
        style,
        re.S,
    )
