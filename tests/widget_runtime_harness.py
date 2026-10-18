"""Run the whole chat widget (static/nova-chat.js) under node, in one or more
simulated browser tabs that share one localStorage.

The file is evaluated unmodified inside a scope where each browser global it
touches resolves either to a stand-in defined here (localStorage, fetch,
WebSocket, location, document.getElementById) or to an inert object that
accepts any property access or call. A test drives the widget's real send
path and its real WebSocket / SSE / non-streaming done handlers through the
public ``NovaChat`` API.

The test supplies ``server(body)`` (JavaScript) answering each chat request
with ``{conversation_id, session_token}``. ``bodies`` records every chat
request as ``{tab, via, conversation_id, session_token}``. ``TRANSPORT`` is
"ws", "sse" or "post" (the stream fails and the widget falls back to a plain
POST /api/chat).
"""

from __future__ import annotations

from tests.wizard_js_harness import ROOT, run_node

WIDGET_JS = ROOT / "static" / "nova-chat.js"

_PRELUDE = r"""
var store = {};
var localStorage = { getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); }, removeItem: function (k) { delete store[k]; } };
var bodies = [];
var tick = function () { return new Promise(function (r) { setTimeout(r, 5); }); };
async function settle() { for (var i = 0; i < 20; i++) await tick(); }

function makeAny(name, elements) {
  var props = {};
  var self = new Proxy(function () {}, {
    get: function (t, k) {
      if (Object.prototype.hasOwnProperty.call(props, k)) return props[k];
      if (k === Symbol.toPrimitive) return function (hint) { return hint === "number" ? 0 : ""; };
      if (k === "then" || typeof k === "symbol") return undefined;
      if (k === "length") return 0;
      props[k] = makeAny(name + "." + String(k), elements);
      return props[k];
    },
    set: function (t, k, v) {
      props[k] = v;
      if (k === "id" && typeof v === "string") elements[v] = self;  // getElementById
      return true;
    },
    apply: function () { return makeAny(name + "()", elements); },
    construct: function () { return makeAny("new " + name, elements); },
  });
  return self;
}

function record(tab, via, body) {
  bodies.push({tab: tab, via: via, conversation_id: body.conversation_id,
               session_token: body.session_token});
  return server(body);
}

function sseResponse(events) {
  var chunks = events.map(function (e) {
    return new TextEncoder().encode("data: " + JSON.stringify(e) + "\n\n"); });
  return Promise.resolve({ ok: true, status: 200, body: { getReader: function () { return {
    read: function () {
      return Promise.resolve(chunks.length ? {done: false, value: chunks.shift()} : {done: true});
    } }; } } });
}

function makeFetch(tab) {
  return function fetch(url, opts) {
    if (url === "/api/chat/stream") {
      if (TRANSPORT === "post") return Promise.resolve({ ok: false, status: 500 });
      var ans = record(tab, "sse", JSON.parse(opts.body));
      return sseResponse([{token: "reply"}, {done: true, full_response: "reply",
        conversation_id: ans.conversation_id, session_token: ans.session_token}]);
    }
    if (url === "/api/chat") {
      var a = record(tab, "post", JSON.parse(opts.body));
      return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve(
        {response: "reply", conversation_id: a.conversation_id, session_token: a.session_token}); } });
    }
    return Promise.resolve({ ok: true, status: 200, json: function () { return Promise.resolve({}); } });
  };
}

function makeWebSocket(tab) {
  function FakeWebSocket() {
    var ws = this;
    ws.readyState = 0;
    setTimeout(function () { ws.readyState = 1; if (ws.onopen) ws.onopen(); }, 0);
  }
  FakeWebSocket.CONNECTING = 0; FakeWebSocket.OPEN = 1; FakeWebSocket.CLOSING = 2; FakeWebSocket.CLOSED = 3;
  FakeWebSocket.prototype.send = function (raw) {
    var ws = this, msg = JSON.parse(raw);
    if (msg.type !== "chat") return;
    var ans = record(tab, "ws", msg);
    setTimeout(function () {
      ws.onmessage({data: JSON.stringify({token: "reply", done: false})});
      ws.onmessage({data: JSON.stringify({token: "", done: true, full_response: "reply",
        conversation_id: ans.conversation_id, session_token: ans.session_token})});
    }, 0);
  };
  FakeWebSocket.prototype.close = function () {};
  return FakeWebSocket;
}

function makeTab(tab) {
  var elements = {};
  var document = makeAny("document", elements);
  document.getElementById = function (id) { return id in elements ? elements[id] : null; };
  document.querySelector = function () { return null; };
  document.querySelectorAll = function () { return []; };
  document.readyState = "complete";
  var location = { protocol: "http:", host: "localhost", pathname: "/", href: "http://localhost/" };
  var window = makeAny("window", elements);
  window.location = location;
  window.localStorage = localStorage;
  window.posthog = undefined;
  window.__csrfToken = "t";
  window.__csrfTokenFetchedAt = Date.now();
  window.__NOVA_PLATFORM = undefined;
  var env = { localStorage: localStorage, document: document, window: window, location: location,
    fetch: makeFetch(tab), WebSocket: TRANSPORT === "ws" ? makeWebSocket(tab) : undefined };
  window.WebSocket = env.WebSocket;
  var scope = new Proxy(env, {
    has: function (t, k) { return k in t || !(k in globalThis); },
    get: function (t, k) {
      if (k === Symbol.unscopables) return undefined;
      if (k in t) return t[k];
      return makeAny(String(k), elements);
    },
  });
  WIDGET_SOURCE(scope);
  window.NovaChat.init({containerId: null});
  return window.NovaChat;
}
"""


def run_widget(server_js: str, steps_js: str, transport: str) -> dict:
    """Run ``steps_js`` (async body; ``makeTab``, ``settle``, ``bodies`` and
    ``store`` are in scope) after defining ``server`` and return its JSON."""
    src = WIDGET_JS.read_text(encoding="utf-8")
    script = (
        f"var TRANSPORT = {transport!r};\n"
        + "function WIDGET_SOURCE(scope) {\nwith (scope) {\n"
        + src
        + "\n}\n}\n"
        + server_js
        + _PRELUDE
        + "(async function () {\n"
        + steps_js
        + "\nprocess.exit(0);\n})().catch(function (e) {"
        " console.log(JSON.stringify({error: String(e && e.stack)})); process.exit(0); });"
    )
    return run_node(script)
