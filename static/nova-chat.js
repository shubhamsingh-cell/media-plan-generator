/**
 * Nova AI Suite Chat Widget
 *
 * Self-contained chat interface for Nova AI Suite recruitment marketing intelligence.
 * Drop-in widget that can be embedded in any page.
 *
 * Usage:
 *   <script src="/static/nova-chat.js"></script>
 *   <div id="nova-chat"></div>
 *   <script>NovaChat.init({ containerId: 'nova-chat' });</script>
 *
 * Or use as floating widget (no container needed):
 *   <script src="/static/nova-chat.js"></script>
 *   <script>NovaChat.init();</script>
 */
(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------
  var CONFIG = {
    apiUrl: "/api/chat",
    streamUrl: "/api/chat/stream",
    wsUrl:
      (location.protocol === "https:" ? "wss://" : "ws://") +
      location.host +
      "/ws/chat",
    feedbackUrl: "/api/chat/feedback",
    stopUrl: "/api/chat/stop",
    useStreaming: true,
    useWebSocket: true,
    primaryColor: "#6BB3CD",
    primaryDark: "#0f0f1a",
    primaryLight: "#8bc7db",
    accentColor: "#6BB3CD",
    accentLight: "#8bc7db",
    accentPurple: "#5A54BD",
    textColor: "#e4e4e7",
    textLight: "#888",
    bgColor: "#0f0f1a",
    bgLight: "rgba(26,26,46,0.95)",
    borderColor: "rgba(107,179,205,0.1)",
    errorColor: "#F87171",
    successColor: "#34D399",
    maxHistoryStorage: 50,
    storageKey: "nova_chat_history",
    sessionKey: "nova_session",
    sessionTokenKey: "nova_session_token",
    widgetWidth: "400px",
    widgetHeight: "580px",
    mobileBreakpoint: 640,
    theme: "dark",
  };

  var SUGGESTED_QUESTIONS = [
    "Create a media plan for 50 software engineers in Austin",
    "What salary should I offer for a Data Scientist in NYC?",
    "Compare Indeed vs LinkedIn for engineering recruitment",
    "Recommend a $100K budget allocation for 20 nursing hires in Texas",
  ];

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  // PostHog Funnel: opened -> message_sent -> response_received -> rated -> action_taken
  var state = {
    isOpen: false,
    isLoading: false,
    messages: [],
    sessionId: null,
    container: null,
    chatPanel: null,
    floatingBtn: null,
    initialized: false,
    orbAnimId: null,
    orbDrawFn: null,
    chatOpenedAt: 0,
    messagesSentCount: 0,
  };

  // ---------------------------------------------------------------------------
  // WebSocket Transport (preferred over SSE for lower latency)
  // ---------------------------------------------------------------------------
  var _widgetWsConn = null;
  var _widgetWsFailCount = 0;
  var _WIDGET_WS_MAX_FAILS = 3;

  function _widgetGetOrCreateWS(onReady) {
    if (
      !window.WebSocket ||
      !CONFIG.useWebSocket ||
      _widgetWsFailCount >= _WIDGET_WS_MAX_FAILS
    ) {
      onReady(null);
      return;
    }
    if (_widgetWsConn && _widgetWsConn.readyState === WebSocket.OPEN) {
      onReady(_widgetWsConn);
      return;
    }
    if (_widgetWsConn && _widgetWsConn.readyState === WebSocket.CONNECTING) {
      var _wc = 0;
      var _wi = setInterval(function () {
        _wc++;
        if (_widgetWsConn && _widgetWsConn.readyState === WebSocket.OPEN) {
          clearInterval(_wi);
          onReady(_widgetWsConn);
        } else if (
          _wc > 30 ||
          !_widgetWsConn ||
          _widgetWsConn.readyState > WebSocket.OPEN
        ) {
          clearInterval(_wi);
          onReady(null);
        }
      }, 100);
      return;
    }
    try {
      _widgetWsConn = new WebSocket(CONFIG.wsUrl);
      _widgetWsConn.onopen = function () {
        _widgetWsFailCount = 0;
        onReady(_widgetWsConn);
      };
      _widgetWsConn.onerror = function () {
        console.warn("[Nova] WebSocket unavailable, using SSE fallback");
        _widgetWsFailCount++;
        onReady(null);
      };
      _widgetWsConn.onclose = function () {
        _widgetWsConn = null;
      };
      _widgetWsConn._novaManaged = true;
    } catch (e) {
      _widgetWsFailCount++;
      onReady(null);
    }
  }

  function _widgetStreamViaWS(ws, payload, callbacks) {
    var fullText = "";
    var metadata = {};
    var _done = false;
    var _wsTo = null;

    function resetTo() {
      if (_wsTo) clearTimeout(_wsTo);
      _wsTo = setTimeout(function () {
        if (!_done) {
          _done = true;
          callbacks.onError("WebSocket timeout");
        }
      }, 90000);
    }
    resetTo();

    ws.onmessage = function (event) {
      if (_done) return;
      resetTo();
      try {
        var evt = JSON.parse(event.data);
        if (evt.keepalive || evt.type === "pong") return;
        if (evt.error && evt.done) {
          _done = true;
          if (_wsTo) clearTimeout(_wsTo);
          callbacks.onError(evt.error);
          return;
        }
        if (evt.done) {
          _done = true;
          if (_wsTo) clearTimeout(_wsTo);
          metadata = evt;
          callbacks.onComplete(metadata, fullText);
          return;
        }
        // S18: Tool status events (real-time progress)
        if (evt.type === "tool_start" || evt.type === "tool_complete") {
          if (callbacks.onToolStatus) {
            callbacks.onToolStatus(evt.type, evt.tool, evt.label);
          }
          return;
        }
        if (evt.status) {
          callbacks.onStatus(evt.status);
          return;
        }
        if (evt.token) {
          fullText += evt.token;
          callbacks.onToken(evt.token, fullText);
        }
      } catch (e) {
        /* console.warn("[Nova Widget WS] Parse error:", e); */
      }
    };
    ws.onerror = function () {
      if (!_done) {
        _done = true;
        if (_wsTo) clearTimeout(_wsTo);
        callbacks.onError("WebSocket error");
      }
    };
    ws.onclose = function () {
      if (!_done) {
        _done = true;
        if (_wsTo) clearTimeout(_wsTo);
        if (fullText) {
          callbacks.onComplete({}, fullText);
        } else {
          callbacks.onError("WebSocket closed");
        }
      }
      _widgetWsConn = null;
    };
    ws.send(
      JSON.stringify({
        type: "chat",
        message: payload.message,
        conversation_id: payload.conversation_id,
        history: payload.history,
        session_token: payload.session_token || "",
        context: payload.context || null,
      }),
    );
    return function cancelWS() {
      if (!_done) {
        _done = true;
        if (_wsTo) clearTimeout(_wsTo);
        try {
          ws.send(
            JSON.stringify({
              type: "stop",
              conversation_id: payload.conversation_id,
            }),
          );
        } catch (_) {}
      }
    };
  }

  // ---------------------------------------------------------------------------
  // Styles (injected once)
  // ---------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById("nova-styles")) return;

    var css =
      `
/* ==========================================================================
   Nova AI Chat Widget Styles
   Sections: Layout, Header, Messages, Input, Suggestions, Actions,
   Animations, Responsive, Print, Reduced Motion
   ========================================================================== */

/* -- Layout & Container -------------------------------------------------- */
#nova-float-btn {
  position: fixed; bottom: 24px; right: 24px; z-index: 99999;
  width: 68px; height: 68px; border-radius: 50%;
  background: #0f0f1a;
  color: #fff; border: 1.5px solid rgba(107,179,205,0.3); cursor: pointer;
  box-shadow: 0 4px 24px rgba(90,84,189,0.25), 0 0 12px rgba(107,179,205,0.15);
  display: flex; align-items: center; justify-content: center;
  transition: transform 0.3s cubic-bezier(0.34, 1.56, 0.64, 1), box-shadow 0.3s ease;
  font-size: 0; padding: 0; overflow: hidden;
}
#nova-float-btn:hover {
  transform: scale(1.05);
  box-shadow: 0 8px 36px rgba(90,84,189,0.35), 0 0 20px rgba(107,179,205,0.25), 0 0 0 1px rgba(107,179,205,0.3);
  border-color: rgba(107,179,205,0.4);
}
#nova-float-btn:active { transform: scale(0.98); box-shadow: inset 0 2px 8px rgba(0,0,0,0.3); transition: transform 0.1s ease, box-shadow 0.1s ease; }
#nova-float-btn svg { width: 26px; height: 26px; }
#nova-float-btn canvas { display: block; }
#nova-float-btn.nova-btn-close { background: linear-gradient(135deg, #1a1a2e 0%, #5A54BD 100%); border-color: rgba(107,179,205,0.2); }
#nova-float-btn.nova-btn-close:hover { box-shadow: 0 8px 36px rgba(107,179,205,0.3), 0 0 60px rgba(90,84,189,0.1); }

#nova-panel {
  position: fixed; bottom: 96px; right: 24px; z-index: 99998;
  width: 420px; height: 600px; max-height: calc(100vh - 120px);
  background: rgba(15,15,26,0.97);
  backdrop-filter: blur(24px) saturate(1.4); -webkit-backdrop-filter: blur(24px) saturate(1.4);
  border-radius: 16px; box-shadow: 0 16px 48px rgba(0,0,0,0.6);
  display: flex; flex-direction: column; overflow: hidden;
  transition: opacity 0.35s cubic-bezier(0.4, 0, 0.2, 1), transform 0.35s cubic-bezier(0.4, 0, 0.2, 1);
  font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  border: 1px solid rgba(107,179,205,0.1);
}
#nova-panel.nova-hidden { opacity: 0; transform: translateY(20px) scale(0.92); pointer-events: none; }
#nova-panel.nova-visible { opacity: 1; transform: translateY(0) scale(1); pointer-events: auto; }` +
      // ── Header ──
      ".nova-header {" +
      "  background: rgba(15,15,26,0.95);" +
      "  color: #fff; padding: 16px 20px;" +
      "  display: flex; align-items: center; justify-content: space-between;" +
      "  flex-shrink: 0; position: relative; overflow: hidden;" +
      "}" +
      ".nova-header::before { display: none; }" +
      ".nova-header::after { display: none; }" +
      ".nova-header-left {" +
      "  display: flex; align-items: center; gap: 10px; position: relative;" +
      "}" +
      ".nova-header-icon {" +
      "  width: 36px; height: 36px; border-radius: 10px;" +
      "  background: linear-gradient(135deg, rgba(90,84,189,0.2), rgba(107,179,205,0.2));" +
      "  backdrop-filter: blur(8px);" +
      "  border: 1px solid rgba(107,179,205,0.15);" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  font-size: 18px;" +
      "  box-shadow: 0 0 16px rgba(107,179,205,0.15);" +
      "}" +
      ".nova-header-icon svg { filter: drop-shadow(0 0 4px rgba(107,179,205,0.4)); }" +
      ".nova-header-title {" +
      "  font-size: 16px; font-weight: 700; letter-spacing: 1.5px;" +
      "  color: #ededed;" +
      "}" +
      ".nova-header-subtitle {" +
      "  font-size: 11px; opacity: 0.7; margin-top: 1px; letter-spacing: 0.5px; color: #8899aa;" +
      "}" +
      ".nova-close-btn {" +
      "  background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);" +
      "  color: rgba(255,255,255,0.7); cursor: pointer;" +
      "  padding: 5px; border-radius: 8px; line-height: 1;" +
      "  transition: all 0.2s; position: relative;" +
      "}" +
      ".nova-close-btn:hover { background: rgba(255,255,255,0.1); color: #fff; border-color: rgba(255,255,255,0.2); }" +
      ".nova-close-btn svg { width: 18px; height: 18px; }" +
      ".nova-export-btn:hover { background: rgba(255,255,255,0.1) !important; color: #fff !important; border-color: rgba(255,255,255,0.2) !important; }" +
      // ── Messages area ──
      ".nova-messages {" +
      "  flex: 1; overflow-y: auto; padding: 16px;" +
      "  display: flex; flex-direction: column; gap: 14px;" +
      "  background: transparent;" +
      "}" +
      ".nova-messages::-webkit-scrollbar { width: 3px; }" +
      ".nova-messages::-webkit-scrollbar-track { background: transparent; }" +
      ".nova-messages::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 2px; }" +
      ".nova-messages::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.2); }" +
      // ── Message row (avatar + bubble) ──
      ".nova-msg-row {" +
      "  display: flex; gap: 8px; max-width: 92%;" +
      "  animation: nova-msgIn 0.4s cubic-bezier(0.16, 1, 0.3, 1);" +
      "}" +
      ".nova-msg-row-user { align-self: flex-end; flex-direction: row-reverse; }" +
      ".nova-msg-row-assistant { align-self: flex-start; }" +
      ".nova-msg-avatar {" +
      "  width: 24px; height: 24px; border-radius: 50%; flex-shrink: 0;" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  font-size: 11px; font-weight: 700; color: #fff; margin-top: 18px;" +
      "}" +
      ".nova-msg-avatar-user { background: #5A54BD; }" +
      ".nova-msg-avatar-assistant { background: linear-gradient(135deg, #6BB3CD, #4a9db5); }" +
      ".nova-msg-sender {" +
      "  font-size: 10px; font-weight: 600; margin-bottom: 2px; letter-spacing: 0.3px;" +
      "}" +
      ".nova-msg-sender-user { color: #5A54BD; text-align: right; }" +
      ".nova-msg-sender-assistant { color: #6BB3CD; }" +
      ".nova-msg-timestamp {" +
      "  font-size: 9px; color: #555; margin-top: 3px; letter-spacing: 0.2px;" +
      "}" +
      ".nova-msg-timestamp-user { text-align: right; }" +
      // ── Message bubbles ──
      ".nova-msg {" +
      "  max-width: 100%; padding: 14px 18px; border-radius: 16px;" +
      "  font-size: 14px; line-height: 1.7; word-wrap: break-word;" +
      "}" +
      "@keyframes nova-msgIn {" +
      "  0% { opacity: 0; transform: translateY(16px) scale(0.96); }" +
      "  60% { opacity: 1; transform: translateY(-3px) scale(1.01); }" +
      "  100% { opacity: 1; transform: translateY(0) scale(1); }" +
      "}" +
      ".nova-msg-user {" +
      "  align-self: flex-end;" +
      "  background: rgba(90,84,189,0.15);" +
      "  color: #e4e4e7;" +
      "  border: 1px solid rgba(90,84,189,0.2);" +
      "  border-bottom-right-radius: 4px;" +
      "  box-shadow: 0 2px 12px rgba(0,0,0,0.08);" +
      "}" +
      ".nova-msg-assistant {" +
      "  align-self: flex-start;" +
      "  background: rgba(26,26,46,0.8);" +
      "  color: #e4e4e7;" +
      "  border: 1px solid rgba(107,179,205,0.08);" +
      "  border-bottom-left-radius: 4px;" +
      "  box-shadow: 0 2px 8px rgba(0,0,0,0.15);" +
      "}" +
      // ── Markdown in messages ──
      ".nova-msg-assistant h3 {" +
      "  font-size: 13px; font-weight: 600; margin: 8px 0 4px 0;" +
      "  color: #fff;" +
      "}" +
      ".nova-msg-assistant h3:first-child { margin-top: 0; }" +
      ".nova-msg-assistant strong { font-weight: 600; color: #ededed; }" +
      ".nova-msg-assistant em { font-style: italic; color: #6b7c8d; }" +
      ".nova-msg-assistant ul, .nova-msg-assistant ol { margin: 4px 0; padding-left: 18px; }" +
      ".nova-msg-assistant ol { list-style-type: decimal; }" +
      ".nova-msg-assistant li { margin-bottom: 3px; }" +
      ".nova-msg-assistant li::marker { color: rgba(107,179,205,0.6); }" +
      ".nova-msg-assistant table {" +
      "  border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 11px;" +
      "}" +
      ".nova-msg-assistant th, .nova-msg-assistant td {" +
      "  border: 1px solid rgba(255,255,255,0.06); padding: 5px 8px; text-align: left;" +
      "}" +
      ".nova-msg-assistant th {" +
      "  background: rgba(255,255,255,0.04); font-weight: 600; color: #a1a1a1;" +
      "}" +
      ".nova-msg-assistant code {" +
      "  background: rgba(107,179,205,0.1); padding: 1px 5px; border-radius: 4px;" +
      '  font-family: "SF Mono", Monaco, Menlo, monospace; font-size: 11.5px; color: #6BB3CD;' +
      "}" +
      ".nova-msg-assistant pre {" +
      "  background: #0d0d18; border: 1px solid rgba(107,179,205,0.1); border-radius: 8px;" +
      "  padding: 12px; margin: 8px 0; overflow-x: auto;" +
      "}" +
      ".nova-msg-assistant pre code {" +
      "  background: none; padding: 0; border-radius: 0; color: #e4e4e7; line-height: 1.6;" +
      "}" +
      ".nova-msg-assistant p { margin: 4px 0; }" +
      ".nova-msg-assistant a { color: #6BB3CD; text-decoration: underline; }" +
      // ── Meta info (sources, confidence) ──
      ".nova-msg-meta {" +
      "  margin-top: 8px; padding-top: 6px;" +
      "  border-top: 1px solid rgba(255,255,255,0.06);" +
      "  display: flex; flex-wrap: wrap; gap: 4px; align-items: center;" +
      "}" +
      ".nova-badge {" +
      "  font-size: 9px; padding: 2px 7px; border-radius: 10px;" +
      "  background: rgba(107,179,205,0.1); color: #6BB3CD;" +
      "  border: 1px solid rgba(107,179,205,0.15);" +
      "  font-weight: 500; white-space: nowrap; letter-spacing: 0.3px;" +
      "}" +
      ".nova-confidence {" +
      "  font-size: 9px; padding: 2px 7px; border-radius: 10px;" +
      "  font-weight: 600; white-space: nowrap; cursor: pointer;" +
      "  position: relative; letter-spacing: 0.3px;" +
      "}" +
      ".nova-confidence-high { background: rgba(52,211,153,0.1); color: #34D399; border: 1px solid rgba(52,211,153,0.2); }" +
      ".nova-confidence-medium { background: rgba(255,170,0,0.1); color: #ffaa00; border: 1px solid rgba(255,170,0,0.2); }" +
      ".nova-confidence-low { background: rgba(248,113,113,0.1); color: #F87171; border: 1px solid rgba(248,113,113,0.2); }" +
      ".nova-confidence-tooltip {" +
      "  display: none; position: absolute; bottom: 100%; left: 50%;" +
      "  transform: translateX(-50%); margin-bottom: 6px;" +
      "  background: rgba(0,0,0,0.95); color: #d4d4d4; padding: 10px 14px;" +
      "  border: 1px solid rgba(255,255,255,0.08);" +
      "  border-radius: 10px; font-size: 11px; line-height: 1.5;" +
      "  min-width: 260px; max-width: 340px; z-index: 999;" +
      "  box-shadow: 0 8px 24px rgba(0,0,0,0.4); font-weight: 400;" +
      "  white-space: normal; text-align: left;" +
      "  backdrop-filter: blur(16px);" +
      "}" +
      ".nova-confidence:hover .nova-confidence-tooltip { display: block; }" +
      ".nova-tooltip-title { font-weight: 600; margin-bottom: 4px; font-size: 12px; color: #6BB3CD; }" +
      ".nova-tooltip-row { display: flex; justify-content: space-between; padding: 1px 0; }" +
      ".nova-tooltip-divider { border-top: 1px solid rgba(255,255,255,0.06); margin: 4px 0; }" +
      ".nova-tooltip-note { font-size: 9px; opacity: 0.5; margin-top: 4px; }" +
      // ── Typing indicator ──
      ".nova-typing {" +
      "  align-self: flex-start; display: flex; align-items: center; gap: 6px;" +
      "  padding: 12px 16px;" +
      "  background: rgba(26,26,46,0.8);" +
      "  border: 1px solid rgba(107,179,205,0.08);" +
      "  border-radius: 16px; border-bottom-left-radius: 4px;" +
      "}" +
      ".nova-typing-dot {" +
      "  width: 6px; height: 6px; border-radius: 50%;" +
      "  background: #6BB3CD;" +
      "  animation: nova-bounce 1.4s infinite;" +
      "  box-shadow: 0 0 6px rgba(107,179,205,0.4);" +
      "}" +
      ".nova-typing-dot:nth-child(2) { animation-delay: 0.2s; }" +
      ".nova-typing-dot:nth-child(3) { animation-delay: 0.4s; }" +
      "@keyframes nova-bounce {" +
      "  0%, 80%, 100% { transform: translateY(0) scale(1); opacity: 0.3; }" +
      "  40% { transform: translateY(-8px) scale(1.3); opacity: 1; }" +
      "}" +
      // ── Welcome state ──
      ".nova-welcome {" +
      "  display: flex; flex-direction: column; align-items: center;" +
      "  justify-content: center; padding: 32px 20px 16px; text-align: center;" +
      "}" +
      ".nova-welcome-orb {" +
      "  width: 40px; height: 40px; border-radius: 50%;" +
      "  background: linear-gradient(135deg, #5A54BD, #6BB3CD);" +
      "  margin-bottom: 16px; box-shadow: 0 0 24px rgba(107,179,205,0.3), 0 0 48px rgba(90,84,189,0.15);" +
      "  animation: nova-orb-pulse 3s ease-in-out infinite;" +
      "}" +
      "@keyframes nova-orb-pulse {" +
      "  0%, 100% { transform: scale(1); box-shadow: 0 0 24px rgba(107,179,205,0.3), 0 0 48px rgba(90,84,189,0.15); }" +
      "  50% { transform: scale(1.08); box-shadow: 0 0 32px rgba(107,179,205,0.45), 0 0 64px rgba(90,84,189,0.25); }" +
      "}" +
      ".nova-welcome-title {" +
      "  font-size: 18px; font-weight: 700; color: #ededed; margin-bottom: 6px;" +
      "}" +
      ".nova-welcome-subtitle {" +
      "  font-size: 12px; color: #888; margin-bottom: 20px; line-height: 1.5;" +
      "}" +
      // ── Suggested questions (2x2 grid) ──
      ".nova-suggestions {" +
      "  display: grid; grid-template-columns: 1fr 1fr; gap: 8px;" +
      "  padding: 0 20px 16px; width: 100%; box-sizing: border-box;" +
      "}" +
      ".nova-suggestion-btn {" +
      "  background: rgba(26,26,46,0.6); border: 1px solid rgba(107,179,205,0.12);" +
      "  padding: 12px 14px; border-radius: 12px; cursor: pointer;" +
      "  font-size: 11px; color: #a1a1a1; text-align: left;" +
      "  font-family: inherit; line-height: 1.45;" +
      "  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);" +
      "}" +
      ".nova-suggestion-btn:hover {" +
      "  border-color: rgba(107,179,205,0.35);" +
      "  background: rgba(107,179,205,0.06);" +
      "  color: #e4e4e7;" +
      "  transform: scale(1.05);" +
      "  box-shadow: 0 4px 12px rgba(0,0,0,0.15);" +
      "}" +
      ".nova-suggestion-btn:active {" +
      "  transform: scale(0.98);" +
      "  box-shadow: inset 0 2px 8px rgba(0,0,0,0.3);" +
      "  transition: transform 0.1s ease, box-shadow 0.1s ease;" +
      "}" +
      ".nova-suggestion-btn:focus-visible {" +
      "  outline: 2px solid #6BB3CD; outline-offset: 2px;" +
      "}" +
      // ── Input area ──
      ".nova-input-wrap {" +
      "  padding: 14px 16px 4px; border-top: 1px solid rgba(107,179,205,0.1);" +
      "  background: rgba(15,15,26,0.9); flex-shrink: 0;" +
      "}" +
      ".nova-input-area {" +
      "  display: flex; gap: 10px; align-items: flex-end;" +
      "}" +
      ".nova-char-counter {" +
      "  font-size: 9px; color: #555; text-align: right; padding: 2px 4px 4px 0;" +
      "  font-variant-numeric: tabular-nums; letter-spacing: 0.3px;" +
      "}" +
      ".nova-char-counter-warn { color: #ffaa00; }" +
      ".nova-char-counter-over { color: #F87171; }" +
      // ── Stop button ──
      ".nova-stop-btn {" +
      "  width: 40px; height: 40px; border-radius: 12px;" +
      "  background: rgba(248,113,113,0.15); color: #F87171;" +
      "  border: 1px solid rgba(248,113,113,0.25); cursor: pointer;" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  flex-shrink: 0; transition: all 0.2s;" +
      "}" +
      ".nova-stop-btn:hover { background: rgba(248,113,113,0.25); border-color: rgba(248,113,113,0.4); }" +
      ".nova-stop-btn svg { width: 16px; height: 16px; }" +
      ".nova-input {" +
      "  flex: 1; border: 1px solid rgba(107,179,205,0.1);" +
      "  border-radius: 12px; padding: 10px 14px;" +
      "  font-size: 13px; font-family: inherit;" +
      "  resize: none; outline: none; min-height: 20px; max-height: 100px;" +
      "  line-height: 1.5; background: rgba(20,20,37,0.8);" +
      "  color: #e4e4e7;" +
      "  transition: border-color 0.2s, background 0.2s, box-shadow 0.2s;" +
      "}" +
      ".nova-input:focus {" +
      "  border-color: rgba(107,179,205,0.4); background: rgba(107,179,205,0.04);" +
      "  box-shadow: 0 0 0 3px rgba(107,179,205,0.1), 0 0 20px rgba(107,179,205,0.06);" +
      "}" +
      ".nova-input::placeholder { color: #555; }" +
      ".nova-send-btn {" +
      "  width: 40px; height: 40px; border-radius: 12px;" +
      "  background: #6BB3CD;" +
      "  color: #fff;" +
      "  border: none; cursor: pointer;" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  flex-shrink: 0;" +
      "  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);" +
      "  box-shadow: 0 2px 12px rgba(0,0,0,0.2);" +
      "}" +
      ".nova-send-btn:hover {" +
      "  transform: scale(1.05);" +
      "  box-shadow: 0 4px 20px rgba(0,0,0,0.3);" +
      "}" +
      ".nova-send-btn:active { transform: scale(0.98); box-shadow: inset 0 2px 8px rgba(0,0,0,0.3); transition: transform 0.1s ease, box-shadow 0.1s ease; }" +
      ".nova-send-btn:disabled { opacity: 0.4; cursor: not-allowed; transform: none; box-shadow: none; }" +
      ".nova-send-btn svg { width: 18px; height: 18px; }" +
      // ── Footer ──
      ".nova-footer {" +
      "  text-align: center; padding: 6px 12px; font-size: 9px;" +
      "  color: #555; background: transparent;" +
      "  flex-shrink: 0;" +
      "  border-top: 1px solid rgba(107,179,205,0.06);" +
      "  letter-spacing: 1px; text-transform: uppercase;" +
      "}" +
      // ── Code syntax highlighting ──
      ".nova-code-header {" +
      "  display: flex; align-items: center; justify-content: space-between;" +
      "  padding: 6px 12px; background: rgba(107,179,205,0.06);" +
      "  font-size: 10px; color: #888;" +
      "}" +
      ".nova-code-copy-btn {" +
      "  background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.08);" +
      "  color: #888; cursor: pointer; padding: 2px 8px; border-radius: 4px;" +
      "  font-size: 10px; font-family: inherit; transition: all 0.15s;" +
      "}" +
      ".nova-code-copy-btn:hover { color: #6BB3CD; border-color: rgba(107,179,205,0.3); }" +
      ".nova-hl-kw { color: #818cf8; }" +
      ".nova-hl-str { color: #34D399; }" +
      ".nova-hl-num { color: #fb923c; }" +
      ".nova-hl-cmt { color: #555; font-style: italic; }" +
      ".nova-hl-fn { color: #c084fc; }" +
      ".nova-hl-type { color: #22d3ee; }" +
      // ── Mobile full-screen ──
      "@media (max-width: " +
      CONFIG.mobileBreakpoint +
      "px) {" +
      "  #nova-panel {" +
      "    width: 100vw; height: 100vh;" +
      "    max-height: none; bottom: 0; right: 0; top: 0; left: 0;" +
      "    border-radius: 0;" +
      "  }" +
      "  #nova-float-btn { bottom: 12px; right: 12px; }" +
      "  #nova-float-btn.nova-mobile-hidden { display: none !important; }" +
      "  .nova-suggestions { grid-template-columns: 1fr; }" +
      "}" +
      // ── Streaming cursor ──
      ".nova-streaming-cursor {" +
      "  display: inline-block; width: 2px; height: 1em; background: #6BB3CD;" +
      "  margin-left: 2px; vertical-align: text-bottom;" +
      "  animation: nova-cursor-blink 0.7s steps(2) infinite;" +
      "}" +
      "@keyframes nova-cursor-blink { 0% { opacity: 1; } 50% { opacity: 0; } 100% { opacity: 1; } }" +
      // ── Status/progress indicator ──
      ".nova-status-indicator {" +
      "  font-size: 11px; color: #888; padding: 4px 12px; text-align: center;" +
      "  animation: nova-status-pulse 2s ease-in-out infinite;" +
      "}" +
      "@keyframes nova-status-pulse {" +
      "  0%, 100% { opacity: 0.6; }" +
      "  50% { opacity: 1; }" +
      "}" +
      // ── S18: Tool status pills ──
      ".nova-tool-status-container {" +
      "  display: flex; flex-direction: column; gap: 3px;" +
      "  padding: 4px 12px; margin-bottom: 2px;" +
      "}" +
      ".nova-tool-pill {" +
      "  display: inline-flex; align-items: center; gap: 6px;" +
      "  padding: 3px 10px; border-radius: 999px; font-size: 11px;" +
      "  width: fit-content; transition: opacity 0.3s ease;" +
      "}" +
      ".nova-tool-pill-active {" +
      "  background: rgba(90,84,189,0.12); color: #7b75d4;" +
      "  border: 1px solid rgba(90,84,189,0.2);" +
      "}" +
      ".nova-tool-pill-done {" +
      "  background: rgba(52,211,153,0.08); color: #34d399;" +
      "  border: 1px solid rgba(52,211,153,0.15);" +
      "}" +
      ".nova-tool-spinner {" +
      "  display: inline-block; width: 10px; height: 10px;" +
      "  border: 1.5px solid #5a54bd; border-top-color: transparent;" +
      "  border-radius: 50%; animation: nova-tool-spin 0.7s linear infinite;" +
      "  flex-shrink: 0;" +
      "}" +
      ".nova-tool-check {" +
      "  display: inline-flex; align-items: center; justify-content: center;" +
      "  width: 12px; height: 12px; font-size: 9px; flex-shrink: 0; color: #34d399;" +
      "}" +
      ".nova-tool-label {" +
      "  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 200px;" +
      "}" +
      "@keyframes nova-tool-spin { to { transform: rotate(360deg); } }" +
      // ── Edit icon on user messages ──
      ".nova-msg-row-user .nova-edit-btn {" +
      "  opacity: 0; position: absolute; top: 4px; left: -28px;" +
      "  background: rgba(90,84,189,0.15); border: 1px solid rgba(90,84,189,0.2);" +
      "  color: #888; cursor: pointer; padding: 3px; border-radius: 6px;" +
      "  line-height: 1; transition: all 0.15s; display: flex; align-items: center; justify-content: center;" +
      "}" +
      ".nova-msg-row-user:hover .nova-edit-btn { opacity: 1; }" +
      ".nova-msg-row-user .nova-edit-btn:hover { color: #5A54BD; border-color: rgba(90,84,189,0.4); }" +
      // ── Edit textarea ──
      ".nova-edit-textarea {" +
      "  width: 100%; border: 1px solid rgba(90,84,189,0.3);" +
      "  border-radius: 8px; padding: 8px 10px;" +
      "  font-size: 13px; font-family: inherit;" +
      "  resize: none; outline: none; min-height: 40px; max-height: 120px;" +
      "  line-height: 1.5; background: rgba(20,20,37,0.9);" +
      "  color: #e4e4e7; margin-top: 4px;" +
      "}" +
      ".nova-edit-actions {" +
      "  display: flex; gap: 6px; margin-top: 6px; justify-content: flex-end;" +
      "}" +
      ".nova-edit-save-btn {" +
      "  padding: 4px 12px; border-radius: 6px; font-size: 11px; font-family: inherit;" +
      "  background: rgba(90,84,189,0.2); color: #8b85e0; border: 1px solid rgba(90,84,189,0.3);" +
      "  cursor: pointer; transition: all 0.15s;" +
      "}" +
      ".nova-edit-save-btn:hover { background: rgba(90,84,189,0.3); color: #a5a0f0; }" +
      ".nova-edit-cancel-btn {" +
      "  padding: 4px 12px; border-radius: 6px; font-size: 11px; font-family: inherit;" +
      "  background: rgba(255,255,255,0.05); color: #888; border: 1px solid rgba(255,255,255,0.1);" +
      "  cursor: pointer; transition: all 0.15s;" +
      "}" +
      ".nova-edit-cancel-btn:hover { color: #aaa; border-color: rgba(255,255,255,0.2); }" +
      // ── Regenerate button ──
      ".nova-regen-btn {" +
      "  display: inline-flex; align-items: center; gap: 4px; margin-top: 6px; margin-left: 6px;" +
      "  padding: 3px 8px; border-radius: 6px; font-size: 10px; color: #666;" +
      "  background: rgba(107,179,205,0.06); border: 1px solid rgba(107,179,205,0.1);" +
      "  cursor: pointer; transition: all 0.15s; font-family: inherit;" +
      "}" +
      ".nova-regen-btn:hover { color: #6BB3CD; border-color: rgba(107,179,205,0.25); }" +
      // ── Theme toggle button ──
      ".nova-theme-btn {" +
      "  background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);" +
      "  color: rgba(255,255,255,0.7); cursor: pointer;" +
      "  padding: 5px; border-radius: 8px; line-height: 1;" +
      "  transition: all 0.2s;" +
      "}" +
      ".nova-theme-btn:hover { background: rgba(255,255,255,0.1); color: #fff; border-color: rgba(255,255,255,0.2); }" +
      ".nova-theme-btn svg { width: 16px; height: 16px; }" +
      // ── Light theme overrides ──
      "#nova-panel.nova-light {" +
      "  background: rgba(245,245,247,0.97);" +
      "  border-color: rgba(0,0,0,0.08);" +
      "}" +
      "#nova-panel.nova-light .nova-header {" +
      "  background: rgba(245,245,247,0.95);" +
      "  color: #1a1a2e;" +
      "}" +
      "#nova-panel.nova-light .nova-header-title { color: #1a1a2e; }" +
      "#nova-panel.nova-light .nova-header-subtitle { color: #666; }" +
      "#nova-panel.nova-light .nova-close-btn," +
      "#nova-panel.nova-light .nova-theme-btn," +
      "#nova-panel.nova-light .nova-export-btn {" +
      "  background: rgba(0,0,0,0.04) !important; border-color: rgba(0,0,0,0.1) !important;" +
      "  color: #555 !important;" +
      "}" +
      "#nova-panel.nova-light .nova-close-btn:hover," +
      "#nova-panel.nova-light .nova-theme-btn:hover," +
      "#nova-panel.nova-light .nova-export-btn:hover {" +
      "  background: rgba(0,0,0,0.08) !important; color: #1a1a2e !important;" +
      "}" +
      "#nova-panel.nova-light .nova-messages { color: #1a1a2e; }" +
      "#nova-panel.nova-light .nova-msg-assistant {" +
      "  background: #ffffff; color: #1a1a2e;" +
      "  border-color: rgba(0,0,0,0.06);" +
      "}" +
      "#nova-panel.nova-light .nova-msg-sender-assistant { color: #5a54bd; }" +
      "#nova-panel.nova-light .nova-msg-sender-user { color: #5a54bd; }" +
      "#nova-panel.nova-light .nova-welcome-title { color: #1a1a2e; }" +
      "#nova-panel.nova-light .nova-welcome-subtitle { color: #555; }" +
      "#nova-panel.nova-light .nova-suggestion {" +
      "  background: rgba(90,84,189,0.06); color: #1a1a2e;" +
      "  border-color: rgba(90,84,189,0.15);" +
      "}" +
      "#nova-panel.nova-light .nova-suggestion:hover {" +
      "  background: rgba(90,84,189,0.12); border-color: rgba(90,84,189,0.25);" +
      "}" +
      "#nova-panel.nova-light .nova-input-area {" +
      "  background: rgba(245,245,247,0.95); border-color: rgba(0,0,0,0.06);" +
      "}" +
      "#nova-panel.nova-light .nova-input {" +
      "  background: #ffffff; color: #1a1a2e; border-color: rgba(0,0,0,0.1);" +
      "}" +
      "#nova-panel.nova-light .nova-input::placeholder { color: #999; }" +
      "#nova-panel.nova-light .nova-char-count { color: #999; }" +
      "#nova-panel.nova-light .nova-footer { color: #999; }" +
      "#nova-panel.nova-light .nova-msg strong { color: #111; }" +
      "#nova-panel.nova-light code { background: rgba(0,0,0,0.05); color: #1a1a2e; }" +
      "#nova-panel.nova-light table th { background: rgba(0,0,0,0.04); }" +
      "#nova-panel.nova-light table td { border-color: rgba(0,0,0,0.06); }" +
      // ── Settings panel ──
      ".nova-settings-btn:hover { background: rgba(255,255,255,0.1) !important; color: #fff !important; border-color: rgba(255,255,255,0.2) !important; }" +
      ".nova-settings-panel { position: absolute; top: 56px; right: 8px; z-index: 10; background: rgba(15,15,26,0.98); border: 1px solid rgba(107,179,205,0.15); border-radius: 12px; padding: 14px 16px; min-width: 220px; box-shadow: 0 8px 32px rgba(0,0,0,0.5); backdrop-filter: blur(16px); display: none; }" +
      ".nova-settings-panel.nova-settings-open { display: block; }" +
      ".nova-settings-panel label { display: block; font-size: 11px; color: #888; margin-bottom: 4px; letter-spacing: 0.3px; }" +
      ".nova-settings-panel input[type='text'] { width: 100%; box-sizing: border-box; padding: 6px 10px; background: rgba(20,20,37,0.9); border: 1px solid rgba(107,179,205,0.15); border-radius: 8px; color: #e4e4e7; font-size: 13px; font-family: inherit; outline: none; }" +
      ".nova-settings-panel input[type='text']:focus { border-color: rgba(107,179,205,0.4); }" +
      ".nova-settings-save-btn { margin-top: 8px; width: 100%; padding: 6px; border-radius: 8px; background: rgba(90,84,189,0.2); color: #8b85e0; border: 1px solid rgba(90,84,189,0.3); font-size: 11px; font-family: inherit; cursor: pointer; transition: all 0.15s; }" +
      ".nova-settings-save-btn:hover { background: rgba(90,84,189,0.35); color: #a5a0f0; }" +
      // ── Export dropdown ──
      ".nova-export-dropdown { position: absolute; top: 56px; right: 44px; z-index: 10; background: rgba(15,15,26,0.98); border: 1px solid rgba(107,179,205,0.15); border-radius: 10px; min-width: 180px; overflow: hidden; box-shadow: 0 8px 32px rgba(0,0,0,0.5); backdrop-filter: blur(16px); display: none; }" +
      ".nova-export-dropdown.nova-export-open { display: block; }" +
      ".nova-export-option { display: flex; align-items: center; gap: 8px; padding: 10px 14px; font-size: 12px; color: #a1a1a1; cursor: pointer; transition: all 0.15s; border: none; background: none; width: 100%; text-align: left; font-family: inherit; }" +
      ".nova-export-option:hover { background: rgba(107,179,205,0.08); color: #e4e4e7; }" +
      ".nova-export-option svg { width: 14px; height: 14px; flex-shrink: 0; }" +
      // ── Avatar tooltip ──
      ".nova-msg-avatar[data-tooltip]:hover::after { content: attr(data-tooltip); position: absolute; bottom: -22px; left: 50%; transform: translateX(-50%); background: rgba(0,0,0,0.9); color: #e4e4e7; padding: 2px 8px; border-radius: 4px; font-size: 9px; font-weight: 400; white-space: nowrap; pointer-events: none; z-index: 10; }" +
      // ── Branch indicator ──
      ".nova-branch-indicator { display: inline-flex; align-items: center; gap: 4px; font-size: 9px; color: #5A54BD; margin-bottom: 2px; letter-spacing: 0.3px; opacity: 0.8; }" +
      ".nova-branch-indicator svg { width: 10px; height: 10px; }" +
      // ── Print ──
      "@media print { #nova-float-btn, .nova-input-wrap, .nova-footer, .nova-close-btn, .nova-export-btn, .nova-settings-btn, .nova-suggestion-btn, .nova-suggestions, .nova-welcome, .nova-copy-btn, .nova-tts-btn, .nova-rate-btn, .nova-regen-btn, .nova-edit-btn, .nova-stop-btn, .nova-settings-panel, .nova-export-dropdown { display: none !important; } #nova-panel { position: static !important; width: 100% !important; height: auto !important; max-height: none !important; background: #fff !important; color: #000 !important; border: none !important; box-shadow: none !important; border-radius: 0 !important; backdrop-filter: none !important; } .nova-header { background: #fff !important; color: #000 !important; border-bottom: 2px solid #202058; } .nova-header-title { color: #202058 !important; } .nova-messages { overflow: visible !important; background: #fff !important; } .nova-msg { color: #000 !important; background: transparent !important; border: 1px solid #ddd !important; } .nova-msg-user { background: #f0f0f5 !important; } .nova-msg-assistant { background: #fff !important; } .nova-msg-sender { color: #333 !important; } .nova-msg-avatar { background: #ccc !important; } }" +
      // ── Reduced motion ──
      "@media (prefers-reduced-motion: reduce) {" +
      "  .nova-msg-row, .nova-msg-user, .nova-msg-assistant { animation: none !important; }" +
      "  .nova-msg-row { animation: none !important; }" +
      "  .nova-typing-dot { animation: none !important; opacity: 0.5; }" +
      "  #nova-float-btn, .nova-send-btn, .nova-suggestion-btn, .nova-close-btn, .nova-regen-btn, .nova-edit-btn, .nova-theme-btn { transition: none !important; }" +
      "  #nova-float-btn:hover, .nova-send-btn:hover, .nova-suggestion-btn:hover { transform: none !important; }" +
      "  #nova-float-btn:active, .nova-send-btn:active, .nova-suggestion-btn:active { transform: none !important; }" +
      "  .nova-streaming-cursor { animation: none !important; opacity: 0.7; }" +
      "  .nova-welcome-orb { animation: none !important; }" +
      "  .nova-status-indicator { animation: none !important; }" +
      "  .nova-tool-spinner { animation: none !important; opacity: 0.7; }" +
      "}";

    var styleEl = document.createElement("style");
    styleEl.id = "nova-styles";
    styleEl.textContent = css;
    document.head.appendChild(styleEl);
  }

  // ---------------------------------------------------------------------------
  // SVG Icons
  // ---------------------------------------------------------------------------
  var ICONS = {
    chat:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>' +
      "</svg>",
    close:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>' +
      "</svg>",
    send:
      '<svg viewBox="0 0 24 24" fill="currentColor">' +
      '<path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>' +
      "</svg>",
    iq:
      '<svg viewBox="0 0 24 24" fill="currentColor">' +
      '<path d="M12 2L14.5 8.5L21 11L14.5 13.5L12 20L9.5 13.5L3 11L9.5 8.5L12 2Z" opacity="0.9"/>' +
      '<path d="M19 2L20 5L23 6L20 7L19 10L18 7L15 6L18 5L19 2Z" opacity="0.6"/>' +
      "</svg>",
    export:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>' +
      '<polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>' +
      "</svg>",
    stop:
      '<svg viewBox="0 0 24 24" fill="currentColor">' +
      '<rect x="6" y="6" width="12" height="12" rx="2"/>' +
      "</svg>",
  };

  // ---------------------------------------------------------------------------
  // Markdown renderer (lightweight, no dependencies)
  // ---------------------------------------------------------------------------
  function renderMarkdown(text) {
    if (!text) return "";
    var html = escapeHtml(text);

    // Code blocks (fenced with ```)
    html = html.replace(
      /```(\w+)?\n([\s\S]*?)```/g,
      function (match, lang, code) {
        var langLabel = lang ? lang : "code";
        var highlighted = highlightSyntax(code.trim(), langLabel);
        var codeId = "nova-code-" + Math.random().toString(36).substr(2, 8);
        return (
          '<div style="margin:8px 0;border-radius:8px;overflow:hidden;border:1px solid rgba(107,179,205,0.1);">' +
          '<div class="nova-code-header">' +
          "<span>" +
          langLabel +
          "</span>" +
          '<button class="nova-code-copy-btn" data-code-id="' +
          codeId +
          '" onclick="(function(b){var c=document.getElementById(\'' +
          codeId +
          "');if(c){navigator.clipboard.writeText(c.textContent).then(function(){b.textContent='Copied!';setTimeout(function(){b.textContent='Copy'},1500);if(window.posthog&&typeof window.posthog.capture==='function'){try{window.posthog.capture('nova_chat_code_copied',{source:'widget',page:window.location.pathname});window.posthog.capture('nova_chat_action_taken',{action_type:'code_copy',page:window.location.pathname})}catch(_e){}}})}})(this)\">Copy</button>" +
          "</div>" +
          '<pre style="margin:0;padding:12px;background:#0d0d18;overflow-x:auto;"><code id="' +
          codeId +
          '" style="background:none;padding:0;color:#e4e4e7;font-size:11.5px;font-family:\'SF Mono\',Monaco,Menlo,monospace;line-height:1.6;">' +
          highlighted +
          "</code></pre></div>"
        );
      },
    );

    // Tables (process before other inline formatting)
    html = html.replace(
      /^(\|.+\|)\n(\|[-:\| ]+\|)\n((?:\|.+\|\n?)*)/gm,
      function (match, header, sep, body) {
        var headerCells = header.split("|").filter(function (c) {
          return c.trim() !== "";
        });
        var rows = body.trim().split("\n");
        var table = "<table><thead><tr>";
        headerCells.forEach(function (c) {
          table += "<th>" + c.trim() + "</th>";
        });
        table += "</tr></thead><tbody>";
        rows.forEach(function (row) {
          var cells = row.split("|").filter(function (c) {
            return c.trim() !== "";
          });
          table += "<tr>";
          cells.forEach(function (c) {
            table += "<td>" + c.trim() + "</td>";
          });
          table += "</tr>";
        });
        table += "</tbody></table>";
        return table;
      },
    );

    // Headers
    html = html.replace(/^### (.+)$/gm, "<h3>$1</h3>");
    html = html.replace(/^## (.+)$/gm, "<h3>$1</h3>");
    html = html.replace(/^# (.+)$/gm, "<h3>$1</h3>");

    // Bold and italic
    html = html.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/\*(.+?)\*/g, "<em>$1</em>");

    // Inline code
    html = html.replace(/`([^`]+)`/g, "<code>$1</code>");

    // Images: ![alt text](url) -- must come before link regex
    // Only allow https:// and data:image/ protocols for security
    html = html.replace(
      /!\[([^\]]*)\]\(([^)]+)\)/g,
      function (match, alt, url) {
        var trimmedUrl = url.trim();
        var lowerUrl = trimmedUrl.toLowerCase();
        if (
          lowerUrl.indexOf("https://") !== 0 &&
          lowerUrl.indexOf("data:image/") !== 0
        ) {
          return escapeHtml(alt || "image"); // Strip unsafe image, keep alt text
        }
        return (
          '<img src="' +
          trimmedUrl +
          '" alt="' +
          escapeHtml(alt) +
          '" style="max-width:100%;border-radius:8px;margin:8px 0;" loading="lazy" ' +
          "onerror=\"this.style.display='none'\" />"
        );
      },
    );

    // Links with URL protocol validation (W-08: block javascript:/data:/vbscript:)
    html = html.replace(
      /\[([^\]]+)\]\(([^)]+)\)/g,
      function (match, label, url) {
        var trimmedUrl = url.trim().toLowerCase();
        if (
          trimmedUrl.indexOf("javascript:") === 0 ||
          trimmedUrl.indexOf("data:") === 0 ||
          trimmedUrl.indexOf("vbscript:") === 0
        ) {
          return label; // Strip dangerous link, keep text only
        }
        return (
          '<a href="' +
          url.trim() +
          '" target="_blank" rel="noopener noreferrer">' +
          label +
          "</a>"
        );
      },
    );

    // Ordered lists (must come before unordered to avoid conflicts)
    html = html.replace(
      /^(\d+)\.\s+(.+)$/gm,
      '<li class="ol-item" value="$1">$2</li>',
    );
    html = html.replace(
      /((?:<li class="ol-item"[^>]*>.+<\/li>\n?)+)/g,
      "<ol>$1</ol>",
    );

    // Unordered lists
    html = html.replace(/^[-*]\s+(.+)$/gm, "<li>$1</li>");
    html = html.replace(/((?:<li>(?:(?!class=).)+<\/li>\n?)+)/g, "<ul>$1</ul>");

    // Inline citations [1], [2], etc.
    html = html.replace(
      /\[(\d+)\]/g,
      '<span style="display:inline-flex;align-items:center;justify-content:center;min-width:16px;height:16px;padding:0 4px;border-radius:10px;background:rgba(107,179,205,0.1);color:#6BB3CD;font-size:9px;font-weight:600;vertical-align:super;margin:0 1px;">$1</span>',
    );

    // Line breaks (double newline = paragraph, single = br)
    html = html.replace(/\n\n/g, "</p><p>");
    html = html.replace(/\n/g, "<br/>");

    // Wrap in paragraph if not already wrapped
    if (
      html.indexOf("<h3>") !== 0 &&
      html.indexOf("<ul>") !== 0 &&
      html.indexOf("<ol>") !== 0 &&
      html.indexOf("<table>") !== 0 &&
      html.indexOf("<div") !== 0
    ) {
      html = "<p>" + html + "</p>";
    }

    // Clean up empty paragraphs
    html = html.replace(/<p>\s*<\/p>/g, "");
    html = html.replace(/<p>\s*<br\/>\s*<\/p>/g, "");

    return html;
  }

  function escapeHtml(str) {
    if (str == null) return "";
    var div = document.createElement("div");
    div.appendChild(document.createTextNode(String(str)));
    return div.innerHTML;
  }

  // ── S18: Tool Status Indicator (real-time tool progress) ──
  var _widgetActiveTools = {};

  function showWidgetToolStatus(type, toolName, label) {
    var messagesEl = state.chatPanel
      ? state.chatPanel.querySelector(".nova-messages")
      : null;
    if (!messagesEl) return;

    var container = document.getElementById("nova-widget-tool-status");
    if (!container) {
      container = document.createElement("div");
      container.id = "nova-widget-tool-status";
      container.className = "nova-tool-status-container";
      messagesEl.appendChild(container);
    }

    if (type === "tool_start") {
      _widgetActiveTools[toolName] = true;
      var pillId = "nova-wt-" + toolName.replace(/[^a-zA-Z0-9]/g, "_");
      var pill = document.getElementById(pillId);
      if (!pill) {
        pill = document.createElement("div");
        pill.id = pillId;
        pill.className = "nova-tool-pill nova-tool-pill-active";
        pill.innerHTML =
          '<span class="nova-tool-spinner"></span>' +
          '<span class="nova-tool-label">' +
          escapeHtml(label) +
          "</span>";
        container.appendChild(pill);
      }
      messagesEl.scrollTo({ top: messagesEl.scrollHeight, behavior: "smooth" });
    } else if (type === "tool_complete") {
      delete _widgetActiveTools[toolName];
      var pillId2 = "nova-wt-" + toolName.replace(/[^a-zA-Z0-9]/g, "_");
      var pill2 = document.getElementById(pillId2);
      if (pill2) {
        pill2.className = "nova-tool-pill nova-tool-pill-done";
        pill2.innerHTML =
          '<span class="nova-tool-check">&#10003;</span>' +
          '<span class="nova-tool-label">' +
          escapeHtml(label) +
          "</span>";
        setTimeout(function () {
          if (pill2 && pill2.parentNode) {
            pill2.style.opacity = "0";
            setTimeout(function () {
              if (pill2 && pill2.parentNode) pill2.remove();
            }, 300);
          }
        }, 1500);
      }
    }
  }

  function clearWidgetToolStatus() {
    _widgetActiveTools = {};
    var container = document.getElementById("nova-widget-tool-status");
    if (container) container.remove();
  }

  function highlightSyntax(code, lang) {
    // Simple regex-based syntax highlighting for JS/Python/SQL patterns
    var l = (lang || "").toLowerCase();

    // Comments: // or # style (single line)
    code = code.replace(
      /(\/\/.*$|#.*$)/gm,
      '<span class="nova-hl-cmt">$1</span>',
    );
    // Multi-line comments /* */
    code = code.replace(
      /(\/\*[\s\S]*?\*\/)/g,
      '<span class="nova-hl-cmt">$1</span>',
    );

    // Strings (double and single quoted)
    code = code.replace(
      /(&quot;(?:[^&]|&(?!quot;))*?&quot;|&#39;(?:[^&]|&(?!#39;))*?&#39;|"[^"]*"|'[^']*')/g,
      '<span class="nova-hl-str">$1</span>',
    );

    // Numbers
    code = code.replace(
      /\b(\d+\.?\d*)\b/g,
      '<span class="nova-hl-num">$1</span>',
    );

    // Keywords (JS + Python + SQL common set)
    var kwPattern =
      /\b(function|const|let|var|return|if|else|for|while|class|import|from|export|default|async|await|try|catch|finally|throw|new|this|typeof|instanceof|def|self|print|lambda|yield|with|as|raise|except|pass|True|False|None|SELECT|FROM|WHERE|INSERT|UPDATE|DELETE|JOIN|LEFT|RIGHT|INNER|OUTER|GROUP|ORDER|BY|HAVING|LIMIT|CREATE|TABLE|ALTER|DROP|AND|OR|NOT|IN|IS|NULL|LIKE|BETWEEN|UNION|SET|VALUES|INTO|ON|AS|DISTINCT|COUNT|SUM|AVG|MAX|MIN|CASE|WHEN|THEN|ELSE|END)\b/g;
    code = code.replace(kwPattern, '<span class="nova-hl-kw">$1</span>');

    // Function calls: word followed by (
    code = code.replace(
      /\b([a-zA-Z_]\w*)\s*\(/g,
      '<span class="nova-hl-fn">$1</span>(',
    );

    return code;
  }

  // ---------------------------------------------------------------------------
  // Session / History persistence
  // ---------------------------------------------------------------------------
  function loadHistory() {
    try {
      var stored = localStorage.getItem(CONFIG.storageKey);
      if (!stored) return [];
      var msgs = JSON.parse(stored);
      // Remove empty assistant messages from timed-out sessions
      return msgs.filter(function (m) {
        return !(m.role === "assistant" && !m.content);
      });
    } catch (e) {
      return [];
    }
  }

  function saveHistory(messages) {
    try {
      var trimmed = messages.slice(-CONFIG.maxHistoryStorage);
      localStorage.setItem(CONFIG.storageKey, JSON.stringify(trimmed));
    } catch (e) {
      /* localStorage write failed */
    }
  }

  /**
   * Remove stale error messages and conversations older than 24 hours.
   * Error patterns: timeout, network errors, "something went wrong", etc.
   * This prevents the chat from showing previous session failures on load.
   */
  function _pruneStaleMessages(messages) {
    if (!messages || !messages.length) return [];
    var STALE_MS = 24 * 60 * 60 * 1000; // 24 hours
    var now = Date.now();
    var errorPatterns = [
      /timed?\s*out/i,
      /error/i,
      /something went wrong/i,
      /failed to/i,
      /network\s*(error|issue)/i,
      /unavailable/i,
      /try again/i,
      /could not connect/i,
      /rate limit/i,
      /500|502|503|504/,
    ];

    // If the last message is older than 24h, clear everything
    var lastMsg = messages[messages.length - 1];
    if (lastMsg && lastMsg.timestamp && now - lastMsg.timestamp > STALE_MS) {
      return [];
    }

    // Remove trailing error messages (most recent errors that would show on load)
    var cleaned = messages.slice();
    while (cleaned.length > 0) {
      var last = cleaned[cleaned.length - 1];
      if (last.role !== "assistant") break;
      var content = (last.content || "").toLowerCase();
      var isError = errorPatterns.some(function (pat) {
        return pat.test(content);
      });
      if (isError) {
        cleaned.pop();
        // Also remove the user message that triggered it
        if (cleaned.length > 0 && cleaned[cleaned.length - 1].role === "user") {
          cleaned.pop();
        }
      } else {
        break;
      }
    }
    return cleaned;
  }

  function getSessionId() {
    try {
      var sid = localStorage.getItem(CONFIG.sessionKey);
      if (!sid) {
        sid =
          "nova-" + Date.now() + "-" + Math.random().toString(36).substr(2, 6);
        localStorage.setItem(CONFIG.sessionKey, sid);
      }
      return sid;
    } catch (e) {
      return "nova-" + Date.now();
    }
  }

  // ---------------------------------------------------------------------------
  // User initial helper (Issue 1: avatars)
  // ---------------------------------------------------------------------------
  function _getUserInitial() {
    try {
      var name = localStorage.getItem("nova_user_name");
      if (name && name.trim()) return name.trim().charAt(0).toUpperCase();
    } catch (e) {
      /* localStorage read failed */
    }
    return "Y";
  }

  function _getUserDisplayName() {
    try {
      var name = localStorage.getItem("nova_user_name");
      if (name && name.trim()) return name.trim();
    } catch (e) {
      /* localStorage read failed */
    }
    return "You";
  }

  // ---------------------------------------------------------------------------
  // Toast notification helper for export feedback
  // ---------------------------------------------------------------------------
  function _showNovaToast(msg) {
    var t = document.createElement("div");
    t.textContent = msg;
    t.style.cssText =
      "position:fixed;bottom:80px;left:50%;transform:translateX(-50%);background:rgba(107,179,205,0.95);color:#fff;padding:8px 20px;border-radius:8px;font-size:13px;z-index:100000;opacity:0;transition:opacity 0.3s;pointer-events:none;";
    document.body.appendChild(t);
    requestAnimationFrame(function () {
      t.style.opacity = "1";
    });
    setTimeout(function () {
      t.style.opacity = "0";
      setTimeout(function () {
        t.remove();
      }, 300);
    }, 2000);
  }

  // Export handlers (Issue 2: export dropdown)
  // ---------------------------------------------------------------------------
  function handleExportOption(exportType) {
    var history = collectConversationHistory();
    if (!history.length) {
      appendMessage(
        {
          role: "assistant",
          content: "No conversation to export yet. Start chatting first!",
        },
        false,
      );
      return;
    }
    if (exportType === "text") {
      var textContent = "";
      history.forEach(function (m) {
        var label = m.role === "user" ? _getUserDisplayName() : "Nova";
        textContent += label + ": " + m.content + "\n\n";
      });
      navigator.clipboard.writeText(textContent).then(function () {
        _showNovaToast("Copied as text!");
      });
    } else if (exportType === "markdown") {
      var mdContent = "# Nova AI Conversation\n\n";
      history.forEach(function (m) {
        var label =
          m.role === "user"
            ? "**" + _getUserDisplayName() + ":**"
            : "**Nova:**";
        mdContent += label + " " + m.content + "\n\n---\n\n";
      });
      navigator.clipboard.writeText(mdContent).then(function () {
        _showNovaToast("Copied as Markdown!");
      });
    } else if (exportType === "pdf") {
      // Open the panel fullscreen for print
      if (state.chatPanel) {
        state.chatPanel.classList.add("nova-visible");
        state.chatPanel.classList.remove("nova-hidden");
      }
      window.print();
      _showNovaToast("Download started");
    } else if (exportType === "html") {
      exportConversation(); // existing HTML export via server
      _showNovaToast("Export started");
    }
  }

  // ---------------------------------------------------------------------------
  // DOM construction
  // ---------------------------------------------------------------------------
  function buildWidget(containerId) {
    // Guard: never build twice (safety net setTimeout may fire after normal init)
    if (document.getElementById("nova-float-btn")) return;

    injectStyles();

    // Floating button with mini 3D orb
    var btn = document.createElement("button");
    btn.id = "nova-float-btn";
    btn.title = "Open Nova Chat";
    btn.setAttribute("aria-label", "Open Nova Chat");
    btn.addEventListener("click", togglePanel);

    // Create mini orb canvas (2x resolution for retina)
    var orbCanvas = document.createElement("canvas");
    orbCanvas.width = 136;
    orbCanvas.height = 136;
    orbCanvas.style.width = "68px";
    orbCanvas.style.height = "68px";
    btn.appendChild(orbCanvas);
    document.body.appendChild(btn);
    state.floatingBtn = btn;
    state.orbCanvas = orbCanvas;

    // Premium AI orb animation -- luminous indigo/cyan energy sphere
    (function () {
      var ctx = orbCanvas.getContext("2d");
      var S = 136,
        C = S / 2,
        R = 38;
      var t = 0;

      // Sphere particles -- denser, brighter
      var dots = [];
      for (var ring = 0; ring < 10; ring++) {
        var phi = (Math.PI * (ring + 0.5)) / 10;
        var count = Math.floor(20 * Math.sin(phi));
        if (count < 4) count = 4;
        for (var d = 0; d < count; d++) {
          dots.push({
            phi: phi,
            theta: (2 * Math.PI * d) / count + (ring % 2) * 0.2,
            size: 0.7 + Math.random() * 1.0,
            bright: 0.4 + Math.random() * 0.6,
            phase: Math.random() * Math.PI * 2,
            ring: ring,
            // Each particle gets a hue blend: 0=indigo, 1=cyan
            hueBlend: Math.random(),
          });
        }
      }

      // Orbital ring particles (2 rings for more depth)
      var ringParts = [];
      for (var i = 0; i < 40; i++) {
        var isOuter = i > 25;
        ringParts.push({
          angle: (2 * Math.PI * i) / (isOuter ? 15 : 25) + Math.random() * 0.2,
          radius: isOuter
            ? R * 1.6 + (Math.random() - 0.5) * 6
            : R * 1.3 + (Math.random() - 0.5) * 8,
          speed: isOuter
            ? 0.2 + Math.random() * 0.08
            : 0.12 + Math.random() * 0.1,
          tilt: isOuter ? 0.35 : 0.2,
          size: 0.5 + Math.random() * 0.7,
          bright: 0.25 + Math.random() * 0.5,
          isCyan: Math.random() > 0.6,
        });
      }

      // Energy pulses -- ripple waves across the sphere
      var pulses = [];
      function spawnPulse() {
        pulses.push({
          phi: Math.random() * Math.PI,
          theta: Math.random() * Math.PI * 2,
          radius: 0,
          speed: 0.5 + Math.random() * 0.5,
          life: 1,
        });
      }
      function dist(p1, t1, p2, t2) {
        return Math.acos(
          Math.min(
            1,
            Math.max(
              -1,
              Math.sin(p1) * Math.sin(p2) * Math.cos(t1 - t2) +
                Math.cos(p1) * Math.cos(p2),
            ),
          ),
        );
      }

      function draw() {
        ctx.clearRect(0, 0, S, S);
        var rotY = t * 0.2,
          rotX = 0.4 + Math.sin(t * 0.3) * 0.05;

        // Multi-layer core glow (indigo center fading to cyan halo)
        var g1 = ctx.createRadialGradient(C, C, 0, C, C, R * 0.6);
        g1.addColorStop(0, "rgba(129,140,248,0.25)");
        g1.addColorStop(0.5, "rgba(99,102,241,0.12)");
        g1.addColorStop(1, "rgba(99,102,241,0)");
        ctx.fillStyle = g1;
        ctx.fillRect(0, 0, S, S);

        var g2 = ctx.createRadialGradient(C, C, R * 0.3, C, C, R * 1.8);
        g2.addColorStop(0, "rgba(99,102,241,0.06)");
        g2.addColorStop(0.4, "rgba(34,211,238,0.04)");
        g2.addColorStop(1, "rgba(0,0,0,0)");
        ctx.fillStyle = g2;
        ctx.fillRect(0, 0, S, S);

        // Breathing outer aura
        var breathe = 0.5 + 0.5 * Math.sin(t * 0.8);
        var g3 = ctx.createRadialGradient(C, C, R * 1.0, C, C, R * 2.2);
        g3.addColorStop(
          0,
          "rgba(99,102,241," + (0.04 * breathe).toFixed(3) + ")",
        );
        g3.addColorStop(1, "rgba(0,0,0,0)");
        ctx.fillStyle = g3;
        ctx.fillRect(0, 0, S, S);

        var list = [];
        var cosY = Math.cos(rotY),
          sinY = Math.sin(rotY);
        var cosX = Math.cos(rotX),
          sinX = Math.sin(rotX);

        for (var i = 0; i < dots.length; i++) {
          var dot = dots[i];
          var th = dot.theta + t * (0.08 + dot.ring * 0.005);
          var x = R * Math.sin(dot.phi) * Math.cos(th);
          var y = R * Math.cos(dot.phi);
          var z = R * Math.sin(dot.phi) * Math.sin(th);
          var x2 = x * cosY - z * sinY,
            z2 = x * sinY + z * cosY;
          var y2 = y * cosX - z2 * sinX,
            z3 = y * sinX + z2 * cosX;
          var sc = 300 / (300 + z3);

          // Pulse interaction
          var pulseBr = 0;
          for (var pi = 0; pi < pulses.length; pi++) {
            var p = pulses[pi];
            var dd = Math.abs(dist(dot.phi, th, p.phi, p.theta) - p.radius);
            if (dd < 0.18)
              pulseBr = Math.max(pulseBr, (1 - dd / 0.18) * p.life);
          }

          var al = (0.3 + dot.bright * 0.55 + pulseBr * 0.8) * (0.5 + sc * 0.5);
          al = Math.min(1, al + Math.sin(t * 1.2 + dot.phase) * 0.06);
          var sz = (dot.size + pulseBr * 1.8) * sc;

          // Dual-color system: indigo base, cyan highlights on pulses
          var blend = dot.hueBlend;
          if (pulseBr > 0.1) blend = Math.min(1, blend + pulseBr * 0.6);
          var cr = Math.round(99 * (1 - blend * 0.65) + 34 * blend * 0.65);
          var cg = Math.round(102 * (1 - blend) + 211 * blend);
          var cb = Math.round(241 * (1 - blend * 0.3) + 238 * blend * 0.3);

          list.push({
            z: z3,
            fn: (function (px, py, s, a, r, gg, b, glow) {
              return function () {
                // Glow halo for bright particles
                if (a > 0.35 || glow > 0.1) {
                  ctx.beginPath();
                  ctx.arc(px, py, s * 3.5, 0, Math.PI * 2);
                  ctx.fillStyle =
                    "rgba(" +
                    r +
                    "," +
                    gg +
                    "," +
                    b +
                    "," +
                    (a * 0.08 + glow * 0.15).toFixed(3) +
                    ")";
                  ctx.fill();
                }
                // Core dot
                ctx.beginPath();
                ctx.arc(px, py, s, 0, Math.PI * 2);
                ctx.fillStyle =
                  "rgba(" + r + "," + gg + "," + b + "," + a.toFixed(3) + ")";
                ctx.fill();
                // Hot center for very bright particles
                if (a > 0.6) {
                  ctx.beginPath();
                  ctx.arc(px, py, s * 0.5, 0, Math.PI * 2);
                  ctx.fillStyle =
                    "rgba(255,255,255," + (a * 0.3).toFixed(3) + ")";
                  ctx.fill();
                }
              };
            })(C + x2 * sc, C + y2 * sc, sz, al, cr, cg, cb, pulseBr),
          });
        }

        // Dual orbital rings
        for (var i = 0; i < ringParts.length; i++) {
          var rp = ringParts[i];
          var a = rp.angle + t * rp.speed;
          var rx = C + Math.cos(a) * rp.radius;
          var ry = C + Math.sin(a) * rp.radius * rp.tilt;
          var rz = Math.sin(a) * rp.radius;
          var al = rp.bright * (0.5 + 0.5 * Math.sin(t * 1.8 + i * 0.3));
          var clr = rp.isCyan ? "34,211,238" : "129,140,248";
          list.push({
            z: rz,
            fn: (function (x, y, s, a, c) {
              return function () {
                ctx.beginPath();
                ctx.arc(x, y, s * 2.5, 0, Math.PI * 2);
                ctx.fillStyle = "rgba(" + c + "," + (a * 0.1).toFixed(3) + ")";
                ctx.fill();
                ctx.beginPath();
                ctx.arc(x, y, s, 0, Math.PI * 2);
                ctx.fillStyle = "rgba(" + c + "," + a.toFixed(3) + ")";
                ctx.fill();
              };
            })(rx, ry, rp.size, al, clr),
          });
        }

        list.sort(function (a, b) {
          return a.z - b.z;
        });
        for (var i = 0; i < list.length; i++) list[i].fn();

        // Spawn pulses more frequently for livelier feel
        if (Math.floor(t * 60) % 35 === 0) spawnPulse();
        for (var i = pulses.length - 1; i >= 0; i--) {
          pulses[i].radius += pulses[i].speed * 0.016;
          pulses[i].life -= 0.008;
          if (pulses[i].life <= 0) pulses.splice(i, 1);
        }

        t += 0.016;
        state.orbAnimId = requestAnimationFrame(draw);
      }
      // W-09: Pause animation when tab hidden
      document.addEventListener("visibilitychange", function () {
        if (document.hidden) {
          cancelAnimationFrame(state.orbAnimId);
          state.orbAnimId = null;
        } else if (!state.isOpen) {
          // Only resume orb animation if panel is closed (orb visible)
          state.orbAnimId = requestAnimationFrame(draw);
        }
      });
      state.orbDrawFn = draw;
      draw();
    })();

    // Chat panel
    var panel = document.createElement("div");
    panel.id = "nova-panel";
    panel.className = "nova-hidden";
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-label", "Nova Chat");

    // Header
    var header = document.createElement("div");
    header.className = "nova-header";
    header.innerHTML =
      "" +
      '<div class="nova-header-left">' +
      '  <div class="nova-header-icon">' +
      ICONS.iq +
      "</div>" +
      "  <div>" +
      '    <div class="nova-header-title">Nova</div>' +
      '    <div class="nova-header-subtitle">Your Recruitment Intelligence, Illuminated</div>' +
      "  </div>" +
      "</div>" +
      '<div class="nova-chat-header-auth" style="display:none;align-items:center;margin-right:4px;"></div>' +
      '<div style="display:flex;align-items:center;gap:6px;">' +
      '<button class="nova-settings-btn" aria-label="Settings" title="Settings" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);color:rgba(255,255,255,0.7);cursor:pointer;padding:5px;border-radius:8px;line-height:1;transition:all 0.2s;">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px;"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>' +
      "</button>" +
      '<button class="nova-export-btn" aria-label="Export conversation" title="Export conversation" style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);color:rgba(255,255,255,0.7);cursor:pointer;padding:5px;border-radius:8px;line-height:1;transition:all 0.2s;">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
      'stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px;">' +
      '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>' +
      '<polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' +
      "</button>" +
      '<button class="nova-theme-btn" aria-label="Toggle theme" title="Toggle dark/light mode">' +
      '<svg class="nova-theme-icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
      'stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px;">' +
      '<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>' +
      '<svg class="nova-theme-icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
      'stroke-linecap="round" stroke-linejoin="round" style="display:none;width:16px;height:16px;">' +
      '<circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/>' +
      '<line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>' +
      '<line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/>' +
      '<line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/>' +
      '<line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>' +
      "</button>" +
      '<button class="nova-close-btn" aria-label="Close chat">' +
      ICONS.close +
      "</button>" +
      "</div>" +
      '<div class="nova-settings-panel" id="nova-settings-panel">' +
      '  <label for="nova-name-input">Your Name</label>' +
      '  <input type="text" id="nova-name-input" placeholder="Enter your name" maxlength="40" />' +
      '  <button class="nova-settings-save-btn" id="nova-settings-save">Save</button>' +
      "</div>" +
      '<div class="nova-export-dropdown" id="nova-export-dropdown">' +
      '  <button class="nova-export-option" data-export="text">Copy as Text</button>' +
      '  <button class="nova-export-option" data-export="markdown">Copy as Markdown</button>' +
      '  <button class="nova-export-option" data-export="pdf">Download as PDF</button>' +
      '  <button class="nova-export-option" data-export="html">Export as HTML</button>' +
      "</div>";

    // Settings gear
    header
      .querySelector(".nova-settings-btn")
      .addEventListener("click", function (e) {
        e.stopPropagation();
        var sp = document.getElementById("nova-settings-panel");
        var ed = document.getElementById("nova-export-dropdown");
        if (ed) ed.classList.remove("nova-export-open");
        if (sp) {
          sp.classList.toggle("nova-settings-open");
          if (sp.classList.contains("nova-settings-open")) {
            var ni = document.getElementById("nova-name-input");
            if (ni) {
              try {
                ni.value = localStorage.getItem("nova_user_name") || "";
              } catch (e) {
                /* localStorage read failed */
              }
              ni.focus();
            }
          }
        }
      });
    header
      .querySelector("#nova-settings-save")
      .addEventListener("click", function () {
        var ni = document.getElementById("nova-name-input");
        var name = ni ? ni.value.trim() : "";
        try {
          localStorage.setItem("nova_user_name", name);
        } catch (e) {
          /* localStorage write failed */
        }
        var sp = document.getElementById("nova-settings-panel");
        if (sp) sp.classList.remove("nova-settings-open");
        document
          .querySelectorAll(".nova-msg-avatar-user")
          .forEach(function (a) {
            a.textContent = _getUserInitial();
            a.setAttribute("data-tooltip", name || "You");
          });
      });
    header
      .querySelector("#nova-name-input")
      .addEventListener("keydown", function (e) {
        if (e.key === "Enter") {
          e.preventDefault();
          header.querySelector("#nova-settings-save").click();
        }
      });

    // Export dropdown
    header
      .querySelector(".nova-export-btn")
      .addEventListener("click", function (e) {
        e.stopPropagation();
        var ed = document.getElementById("nova-export-dropdown");
        var sp = document.getElementById("nova-settings-panel");
        if (sp) sp.classList.remove("nova-settings-open");
        if (ed) ed.classList.toggle("nova-export-open");
      });
    header.querySelectorAll(".nova-export-option").forEach(function (opt) {
      opt.addEventListener("click", function () {
        var dd = document.getElementById("nova-export-dropdown");
        if (dd) dd.classList.remove("nova-export-open");
        handleExportOption(this.getAttribute("data-export"));
      });
    });
    // Close dropdowns on panel click
    panel.addEventListener("click", function (e) {
      if (
        !e.target.closest(".nova-settings-panel") &&
        !e.target.closest(".nova-settings-btn") &&
        !e.target.closest(".nova-export-dropdown") &&
        !e.target.closest(".nova-export-btn")
      ) {
        var sp = document.getElementById("nova-settings-panel");
        var ed = document.getElementById("nova-export-dropdown");
        if (sp) sp.classList.remove("nova-settings-open");
        if (ed) ed.classList.remove("nova-export-open");
      }
    });
    // Theme toggle: dark/light mode
    var _themeBtn = header.querySelector(".nova-theme-btn");
    if (_themeBtn) {
      // Apply saved theme on build
      var _savedTheme = null;
      try {
        _savedTheme = localStorage.getItem("nova_theme");
      } catch (e) {
        /* localStorage read failed */
      }
      if (_savedTheme === "light") {
        panel.classList.add("nova-light");
        var _moonIcon = _themeBtn.querySelector(".nova-theme-icon-moon");
        var _sunIcon = _themeBtn.querySelector(".nova-theme-icon-sun");
        if (_moonIcon) _moonIcon.style.display = "none";
        if (_sunIcon) _sunIcon.style.display = "";
      }
      _themeBtn.addEventListener("click", function () {
        var isLight = panel.classList.toggle("nova-light");
        var moonIcon = _themeBtn.querySelector(".nova-theme-icon-moon");
        var sunIcon = _themeBtn.querySelector(".nova-theme-icon-sun");
        if (isLight) {
          try {
            localStorage.setItem("nova_theme", "light");
          } catch (e) {
            /* localStorage write failed */
          }
          if (moonIcon) moonIcon.style.display = "none";
          if (sunIcon) sunIcon.style.display = "";
        } else {
          try {
            localStorage.setItem("nova_theme", "dark");
          } catch (e) {
            /* localStorage write failed */
          }
          if (moonIcon) moonIcon.style.display = "";
          if (sunIcon) sunIcon.style.display = "none";
        }
      });
    }
    header
      .querySelector(".nova-close-btn")
      .addEventListener("click", function (e) {
        e.stopPropagation();
        e.preventDefault();
        // Force close (never toggle) -- S30 fix v2
        togglePanel(true);
      });
    panel.appendChild(header);

    // Messages container
    var messagesDiv = document.createElement("div");
    messagesDiv.className = "nova-messages";
    messagesDiv.id = "nova-messages";
    messagesDiv.setAttribute("role", "log");
    messagesDiv.setAttribute("aria-live", "polite");
    messagesDiv.setAttribute("aria-label", "Chat messages");
    // PostHog: delegated listener for link clicks inside Nova responses
    messagesDiv.addEventListener("click", function (e) {
      var link = e.target.closest("a[href]");
      if (
        link &&
        window.posthog &&
        typeof window.posthog.capture === "function"
      ) {
        window.posthog.capture("nova_chat_action_taken", {
          action_type: "link_click",
          page: window.location.pathname,
        });
      }
    });
    panel.appendChild(messagesDiv);

    // Input area wrapper (contains input row + char counter)
    var inputWrap = document.createElement("div");
    inputWrap.className = "nova-input-wrap";

    var inputArea = document.createElement("div");
    inputArea.className = "nova-input-area";
    inputArea.id = "nova-input-area";

    var textarea = document.createElement("textarea");
    textarea.className = "nova-input";
    textarea.id = "nova-input";
    textarea.placeholder = "Ask about recruitment marketing...";
    textarea.rows = 1;
    textarea.setAttribute("aria-label", "Chat message input");
    textarea.setAttribute("maxlength", "4000");
    textarea.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        if (state.isLoading) return; // Block input during active stream
        sendMessage();
      }
    });
    textarea.addEventListener("input", function () {
      // Auto-resize
      this.style.height = "auto";
      this.style.height = Math.min(this.scrollHeight, 100) + "px";
      // Update character counter
      updateCharCounter(this.value.length);
    });
    inputArea.appendChild(textarea);

    var sendBtn = document.createElement("button");
    sendBtn.className = "nova-send-btn";
    sendBtn.id = "nova-send-btn";
    sendBtn.innerHTML = ICONS.send;
    sendBtn.title = "Send message";
    sendBtn.setAttribute("aria-label", "Send message");
    sendBtn.addEventListener("click", sendMessage);
    inputArea.appendChild(sendBtn);
    inputWrap.appendChild(inputArea);

    // Character counter
    var charCounter = document.createElement("div");
    charCounter.className = "nova-char-counter";
    charCounter.id = "nova-char-counter";
    charCounter.textContent = "0 / 4000";
    inputWrap.appendChild(charCounter);

    panel.appendChild(inputWrap);

    // Footer
    var footer = document.createElement("div");
    footer.className = "nova-footer";
    footer.textContent = "Created by Shubham Singh Chandel";
    panel.appendChild(footer);

    // Add to container or body
    if (containerId) {
      var containerEl = document.getElementById(containerId);
      if (containerEl) {
        containerEl.appendChild(panel);
        state.container = containerEl;
      } else {
        document.body.appendChild(panel);
      }
    } else {
      document.body.appendChild(panel);
    }

    state.chatPanel = panel;

    // Load history
    state.messages = loadHistory();
    state.sessionId = getSessionId();

    // Clean stale error messages and old conversations (> 24h)
    state.messages = _pruneStaleMessages(state.messages);
    saveHistory(state.messages);

    // Render existing messages or show welcome
    if (state.messages.length > 0) {
      renderAllMessages();
    } else {
      showWelcome();
    }
  }

  // ---------------------------------------------------------------------------
  // Escape key handler (accessibility)
  // ---------------------------------------------------------------------------
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && state.isOpen) {
      togglePanel(true);
    }
    // Cmd+Shift+L (Mac) or Ctrl+Shift+L (Win/Linux): Clear conversation
    if (
      e.key === "L" &&
      e.shiftKey &&
      (e.metaKey || e.ctrlKey) &&
      state.isOpen
    ) {
      e.preventDefault();
      clearConversation();
    }
  });

  // ---------------------------------------------------------------------------
  // Panel toggle
  // ---------------------------------------------------------------------------
  function togglePanel(forceClose) {
    // Debounce FAB toggle (300ms cooldown) to prevent double-click open→close
    if (forceClose !== true) {
      if (window._novaFabDebounce) return;
      window._novaFabDebounce = true;
      setTimeout(function () {
        window._novaFabDebounce = false;
      }, 300);
    }
    // If forceClose is true, always close regardless of current state
    if (forceClose === true) {
      state.isOpen = false;
    } else {
      state.isOpen = !state.isOpen;
    }
    var panel = state.chatPanel;
    var btn = state.floatingBtn;
    var orbCanvas = state.orbCanvas;
    if (state.isOpen) {
      panel.classList.remove("nova-hidden");
      panel.classList.add("nova-visible");
      // PostHog: track panel open + record session start time
      state.chatOpenedAt = Date.now();
      state.messagesSentCount = 0;
      if (window.posthog && typeof window.posthog.capture === "function") {
        window.posthog.capture("nova_chat_opened", {
          source: "widget",
          page: window.location.pathname,
        });
      }
      // W-09: Pause orb animation when panel is open (canvas not visible)
      if (state.orbAnimId) {
        cancelAnimationFrame(state.orbAnimId);
        state.orbAnimId = null;
      }
      // Hide orb, show close icon
      if (orbCanvas) orbCanvas.style.display = "none";
      btn.classList.add("nova-btn-close");
      // Remove any existing close icon before adding new one (prevents duplicates)
      var existingClose = document.getElementById("nova-close-icon");
      if (existingClose) existingClose.remove();
      // Insert close SVG without destroying canvas
      var closeSpan = document.createElement("span");
      closeSpan.id = "nova-close-icon";
      closeSpan.innerHTML = ICONS.close;
      closeSpan.style.display = "flex";
      closeSpan.style.alignItems = "center";
      closeSpan.style.justifyContent = "center";
      btn.appendChild(closeSpan);
      btn.title = "Close Nova Chat";
      btn.setAttribute("aria-label", "Close Nova Chat");
      // Hide float button on mobile when panel is open
      if (window.innerWidth <= CONFIG.mobileBreakpoint) {
        btn.classList.add("nova-mobile-hidden");
      }
      // Focus input
      setTimeout(function () {
        var input = document.getElementById("nova-input");
        if (input) input.focus();
      }, 300);
    } else {
      // PostHog: track session duration on close
      if (
        window.posthog &&
        typeof window.posthog.capture === "function" &&
        state.chatOpenedAt > 0
      ) {
        try {
          window.posthog.capture("nova_chat_session_duration", {
            duration_ms: Date.now() - state.chatOpenedAt,
            messages_sent: state.messagesSentCount,
            page: window.location.pathname,
          });
        } catch (_e) {
          /* posthog tracking failure must never block close */
        }
      }
      state.chatOpenedAt = 0;
      // Close WebSocket connection when panel is closed
      if (_widgetWsConn && _widgetWsConn.readyState === WebSocket.OPEN) {
        _widgetWsConn.close();
      }
      panel.classList.remove("nova-visible");
      panel.classList.add("nova-hidden");
      // Remove close icon, restore orb
      var closeIcon = document.getElementById("nova-close-icon");
      if (closeIcon) closeIcon.remove();
      btn.classList.remove("nova-btn-close");
      if (orbCanvas) orbCanvas.style.display = "block";
      // W-09: Resume orb animation when panel closes (canvas visible again)
      if (state.orbDrawFn && !state.orbAnimId && !document.hidden) {
        state.orbAnimId = requestAnimationFrame(state.orbDrawFn);
      }
      btn.title = "Open Nova Chat";
      btn.setAttribute("aria-label", "Open Nova Chat");
      // Restore float button on mobile
      btn.classList.remove("nova-mobile-hidden");
    }
  }

  // ---------------------------------------------------------------------------
  // Welcome + suggestions
  // ---------------------------------------------------------------------------
  function showWelcome() {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;

    // Centered welcome layout
    var welcomeDiv = document.createElement("div");
    welcomeDiv.className = "nova-welcome";
    welcomeDiv.id = "nova-welcome-state";

    var orb = document.createElement("div");
    orb.className = "nova-welcome-orb";
    welcomeDiv.appendChild(orb);

    var titleEl = document.createElement("div");
    titleEl.className = "nova-welcome-title";
    titleEl.textContent = "Hi, I'm Nova";
    welcomeDiv.appendChild(titleEl);

    var subtitleEl = document.createElement("div");
    subtitleEl.className = "nova-welcome-subtitle";
    subtitleEl.textContent =
      "Your recruitment marketing intelligence assistant. Ask me anything about media planning, salaries, job boards, and market data.";
    welcomeDiv.appendChild(subtitleEl);

    messagesDiv.appendChild(welcomeDiv);

    // 2x2 suggestion cards
    var sugDiv = document.createElement("div");
    sugDiv.className = "nova-suggestions";
    sugDiv.id = "nova-suggestions";

    SUGGESTED_QUESTIONS.forEach(function (q) {
      var btn = document.createElement("button");
      btn.className = "nova-suggestion-btn";
      btn.textContent = q;
      btn.addEventListener("click", function () {
        // PostHog: track suggestion chip click
        if (window.posthog && typeof window.posthog.capture === "function") {
          window.posthog.capture("nova_chat_suggestion_clicked", {
            source: "widget",
            page: window.location.pathname,
            suggestion_text: q,
          });
        }
        var input = document.getElementById("nova-input");
        if (input) input.value = q;
        sendMessage();
      });
      sugDiv.appendChild(btn);
    });

    messagesDiv.appendChild(sugDiv);
  }

  // ---------------------------------------------------------------------------
  // Message rendering
  // ---------------------------------------------------------------------------
  function appendMessage(msg, persist) {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;

    // Remove welcome state and suggestions on first user message
    var sugEl = document.getElementById("nova-suggestions");
    if (sugEl && msg.role === "user") {
      sugEl.remove();
    }
    var welcomeEl = document.getElementById("nova-welcome-state");
    if (welcomeEl && msg.role === "user") {
      welcomeEl.remove();
    }

    // Build message row: avatar + bubble column
    var rowEl = document.createElement("div");
    rowEl.className = "nova-msg-row nova-msg-row-" + msg.role;

    // Avatar
    var avatarEl = document.createElement("div");
    avatarEl.className = "nova-msg-avatar nova-msg-avatar-" + msg.role;
    avatarEl.textContent = msg.role === "user" ? _getUserInitial() : "N";
    avatarEl.setAttribute("aria-hidden", "true");
    avatarEl.setAttribute(
      "data-tooltip",
      msg.role === "user" ? _getUserDisplayName() : "Nova",
    );
    rowEl.appendChild(avatarEl);

    // Bubble column (sender + bubble + timestamp)
    var colEl = document.createElement("div");
    colEl.style.cssText =
      "display:flex;flex-direction:column;min-width:0;flex:1;";

    // Branch indicator (Issue 5)
    if (msg.branched_from !== undefined && msg.branched_from !== null) {
      var branchEl = document.createElement("div");
      branchEl.className = "nova-branch-indicator";
      branchEl.innerHTML =
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg> Edited from original';
      colEl.appendChild(branchEl);
    }

    // Sender name
    var senderEl = document.createElement("div");
    senderEl.className = "nova-msg-sender nova-msg-sender-" + msg.role;
    senderEl.textContent = msg.role === "user" ? _getUserDisplayName() : "Nova";
    colEl.appendChild(senderEl);

    var msgEl = document.createElement("div");
    msgEl.className = "nova-msg nova-msg-" + msg.role;

    if (msg.role === "assistant") {
      msgEl.innerHTML = renderMarkdown(msg.content);

      // Add action buttons (copy, TTS, feedback, regenerate) via shared helper
      addActionButtonsToElement(msgEl, msg.content);

      // Meta: sources + confidence via shared helper
      addMetaToElement(
        msgEl,
        msg.sources || [],
        msg.confidence,
        msg.confidence_breakdown,
      );

      // "Why this answer?" transparency panel (collapsed by default).
      // Rendered for both live responses and restored history messages so
      // users can inspect provenance even after a page reload.
      addTransparencyToElement(msgEl, msg);

      // Confidence Pulse: animate stat values with pulsing rings
      applyConfidencePulse(msgEl, msg.confidence);
    } else {
      // User message: set text content and add edit icon
      msgEl.textContent = msg.content;
      msgEl.style.position = "relative";

      var editBtn = document.createElement("button");
      editBtn.className = "nova-edit-btn";
      editBtn.setAttribute("aria-label", "Edit message");
      editBtn.innerHTML =
        '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>';
      // Calculate the correct index for this message in state.messages
      // If persisting, the message will be pushed after this block, so index = current length
      // If rendering from history (persist===false), use the _renderIndex counter
      var editMsgIdx =
        persist !== false
          ? state.messages.length
          : typeof msg._renderIndex === "number"
            ? msg._renderIndex
            : state.messages.length - 1;
      (function (btn, row, idx) {
        btn.addEventListener("click", function () {
          editAndResend(row, idx);
        });
      })(editBtn, rowEl, editMsgIdx);
      msgEl.appendChild(editBtn);
    }

    colEl.appendChild(msgEl);

    // Timestamp
    var now = new Date();
    var tsEl = document.createElement("div");
    tsEl.className = "nova-msg-timestamp nova-msg-timestamp-" + msg.role;
    tsEl.textContent =
      String(now.getHours()).padStart(2, "0") +
      ":" +
      String(now.getMinutes()).padStart(2, "0");
    colEl.appendChild(tsEl);

    rowEl.appendChild(colEl);
    messagesDiv.appendChild(rowEl);
    messagesDiv.scrollTo({ top: messagesDiv.scrollHeight, behavior: "smooth" });

    if (persist !== false) {
      state.messages.push(msg);
      saveHistory(state.messages);
    }
  }

  function renderAllMessages() {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;
    messagesDiv.innerHTML = "";

    state.messages.forEach(function (msg, idx) {
      // Attach render index so appendMessage can use it for edit button
      msg._renderIndex = idx;
      appendMessage(msg, false);
      delete msg._renderIndex;
    });
  }

  var _typingStartTime = 0;
  var _typingElapsedInterval = null;

  function showTyping() {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;

    _typingStartTime = Date.now();

    var typing = document.createElement("div");
    typing.className = "nova-typing";
    typing.id = "nova-typing";
    typing.innerHTML =
      "" +
      '<div class="nova-typing-dot"></div>' +
      '<div class="nova-typing-dot"></div>' +
      '<div class="nova-typing-dot"></div>' +
      '<span id="nova-typing-elapsed" style="font-size:10px;color:#555;margin-left:6px;font-variant-numeric:tabular-nums;min-width:20px;"></span>';
    messagesDiv.appendChild(typing);
    messagesDiv.scrollTo({ top: messagesDiv.scrollHeight, behavior: "smooth" });

    // Show elapsed time after 2s
    _typingElapsedInterval = setInterval(function () {
      var elapsed = Math.round((Date.now() - _typingStartTime) / 1000);
      var el = document.getElementById("nova-typing-elapsed");
      if (el && elapsed >= 2) {
        el.textContent = elapsed + "s";
      }
    }, 1000);
  }

  function hideTyping() {
    if (_typingElapsedInterval) {
      clearInterval(_typingElapsedInterval);
      _typingElapsedInterval = null;
    }
    var el = document.getElementById("nova-typing");
    if (el) el.remove();
  }

  function updateCharCounter(len) {
    var el = document.getElementById("nova-char-counter");
    if (!el) return;
    el.textContent = len + " / 4000";
    el.className = "nova-char-counter";
    if (len > 4000) {
      el.classList.add("nova-char-counter-over");
    } else if (len > 3500) {
      el.classList.add("nova-char-counter-warn");
    }
  }

  // Active AbortController for streaming (exposed for stop button)
  var _activeAbortCtrl = null;

  function showStopButton() {
    var inputArea = document.getElementById("nova-input-area");
    if (!inputArea) return;
    // Remove existing stop button if any
    var existing = document.getElementById("nova-stop-btn");
    if (existing) existing.remove();

    var stopBtn = document.createElement("button");
    stopBtn.className = "nova-stop-btn";
    stopBtn.id = "nova-stop-btn";
    stopBtn.innerHTML = ICONS.stop;
    stopBtn.title = "Stop response";
    stopBtn.setAttribute("aria-label", "Stop response");
    stopBtn.addEventListener("click", function () {
      if (_activeAbortCtrl) {
        _activeAbortCtrl._isUserCancel = true;
        _activeAbortCtrl.abort();
        _activeAbortCtrl = null;
      }
      hideStopButton();
    });
    inputArea.appendChild(stopBtn);
  }

  function hideStopButton() {
    var el = document.getElementById("nova-stop-btn");
    if (el) el.remove();
  }

  // ---------------------------------------------------------------------------
  // Export conversation
  // ---------------------------------------------------------------------------
  function collectConversationHistory() {
    var history = [];
    state.messages.forEach(function (m) {
      if (m.role === "user" || m.role === "assistant") {
        var entry = { role: m.role, content: m.content };
        if (m.timestamp) entry.timestamp = m.timestamp;
        if (m.sources) entry.sources = m.sources;
        if (m.confidence) entry.confidence = m.confidence;
        history.push(entry);
      }
    });
    return history;
  }

  function exportConversation() {
    var history = collectConversationHistory();
    if (!history.length) {
      appendMessage(
        {
          role: "assistant",
          content: "No conversation to export yet. Start chatting first!",
        },
        false,
      );
      return;
    }

    var payload = {
      conversation_history: history,
      metadata: {
        session_id: state.sessionId || "",
        export_date: new Date().toISOString(),
      },
    };

    fetch("/api/nova/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.blob();
      })
      .then(function (blob) {
        var url = URL.createObjectURL(blob);
        var a = document.createElement("a");
        a.href = url;
        a.download = "nova-conversation-export.html";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      })
      .catch(function (err) {
        console.error("Nova export error:", err);
        appendMessage(
          {
            role: "assistant",
            content: "Failed to export conversation. Please try again.",
            sources: [],
            confidence: 0,
          },
          false,
        );
      });
  }

  // ---------------------------------------------------------------------------
  // Show streaming error with optional retry button
  // ---------------------------------------------------------------------------
  function showStreamError(errorMsg, retryText, isAbort) {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;
    var errEl = document.createElement("div");
    errEl.className = "nova-msg nova-msg-assistant";
    errEl.style.borderColor = "rgba(248,113,113,0.2)";
    errEl.innerHTML = '<span style="color:#F87171;">' + errorMsg + "</span>";
    if (!isAbort) {
      var retryBtn = document.createElement("button");
      retryBtn.textContent = "Retry";
      retryBtn.style.cssText =
        "display:inline-block;margin-top:8px;padding:4px 12px;border-radius:6px;font-size:11px;color:#6BB3CD;background:rgba(107,179,205,0.1);border:1px solid rgba(107,179,205,0.2);cursor:pointer;font-family:inherit;transition:all 0.15s;";
      retryBtn.addEventListener("mouseenter", function () {
        this.style.background = "rgba(107,179,205,0.2)";
      });
      retryBtn.addEventListener("mouseleave", function () {
        this.style.background = "rgba(107,179,205,0.1)";
      });
      (function (btn, txt) {
        btn.addEventListener("click", function () {
          errEl.remove();
          var inp = document.getElementById("nova-input");
          if (inp) inp.value = txt;
          sendMessage();
        });
      })(retryBtn, retryText);
      errEl.appendChild(retryBtn);
    }
    messagesDiv.appendChild(errEl);
    messagesDiv.scrollTo({ top: messagesDiv.scrollHeight, behavior: "smooth" });
  }

  // ---------------------------------------------------------------------------
  // Helper: add action buttons (copy, TTS, feedback, regenerate) to an element
  // ---------------------------------------------------------------------------
  function addActionButtonsToElement(msgEl, content) {
    // Copy button
    var copyBtn = document.createElement("button");
    copyBtn.className = "nova-copy-btn";
    copyBtn.innerHTML =
      '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg> Copy';
    copyBtn.style.cssText =
      "display:inline-flex;align-items:center;gap:4px;margin-top:6px;padding:3px 8px;border-radius:6px;font-size:10px;color:#666;background:rgba(107,179,205,0.06);border:1px solid rgba(107,179,205,0.1);cursor:pointer;transition:all 0.15s;font-family:inherit;";
    copyBtn.addEventListener("mouseenter", function () {
      this.style.color = "#6BB3CD";
      this.style.borderColor = "rgba(107,179,205,0.25)";
    });
    copyBtn.addEventListener("mouseleave", function () {
      this.style.color = "#666";
      this.style.borderColor = "rgba(107,179,205,0.1)";
    });
    (function (btn, c) {
      btn.addEventListener("click", function () {
        navigator.clipboard.writeText(c).then(function () {
          btn.innerHTML =
            '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg> Copied';
          btn.style.color = "#34D399";
          setTimeout(function () {
            btn.innerHTML =
              '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg> Copy';
            btn.style.color = "#666";
          }, 1500);
        });
      });
    })(copyBtn, content);
    msgEl.appendChild(copyBtn);

    // TTS button
    var ttsBtn = document.createElement("button");
    ttsBtn.className = "nova-tts-btn";
    ttsBtn.setAttribute("aria-label", "Listen to response");
    ttsBtn.innerHTML =
      '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg> Listen';
    ttsBtn.style.cssText =
      "display:inline-flex;align-items:center;gap:4px;margin-top:6px;margin-left:6px;padding:3px 8px;border-radius:6px;font-size:10px;color:#666;background:rgba(107,179,205,0.06);border:1px solid rgba(107,179,205,0.1);cursor:pointer;transition:all 0.15s;font-family:inherit;";
    ttsBtn.addEventListener("mouseenter", function () {
      this.style.color = "#6BB3CD";
      this.style.borderColor = "rgba(107,179,205,0.25)";
    });
    ttsBtn.addEventListener("mouseleave", function () {
      if (!this.dataset.playing) {
        this.style.color = "#666";
        this.style.borderColor = "rgba(107,179,205,0.1)";
      }
    });
    (function (btn, c) {
      var audio = null;
      btn.addEventListener("click", function () {
        if (audio && !audio.paused) {
          audio.pause();
          audio = null;
          btn.innerHTML =
            '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg> Listen';
          btn.style.color = "#666";
          delete btn.dataset.playing;
          return;
        }
        btn.innerHTML =
          '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="10" y1="15" x2="10" y2="9"/><line x1="14" y1="15" x2="14" y2="9"/></svg> Loading...';
        btn.style.color = "#6BB3CD";
        btn.dataset.playing = "1";
        var csrfToken = "";
        var csrfMeta = document.querySelector('meta[name="csrf-token"]');
        if (csrfMeta) csrfToken = csrfMeta.getAttribute("content") || "";
        if (!csrfToken) {
          var cookies = document.cookie.split(";");
          for (var ci = 0; ci < cookies.length; ci++) {
            var ck = cookies[ci].trim();
            if (ck.indexOf("csrf_token=") === 0) {
              csrfToken = ck.substring("csrf_token=".length);
              break;
            }
          }
        }
        fetch("/api/tts", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-CSRF-Token": csrfToken,
          },
          credentials: "same-origin",
          body: JSON.stringify({ text: c }),
        })
          .then(function (resp) {
            if (!resp.ok) throw new Error("TTS failed: " + resp.status);
            return resp.blob();
          })
          .then(function (blob) {
            var url = URL.createObjectURL(blob);
            btn._ttsBlobUrl = url;
            audio = new Audio(url);
            audio.play();
            btn.innerHTML =
              '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg> Playing';
            audio.addEventListener("ended", function () {
              btn.innerHTML =
                '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg> Listen';
              btn.style.color = "#666";
              delete btn.dataset.playing;
              URL.revokeObjectURL(url);
              btn._ttsBlobUrl = null;
            });
          })
          .catch(function () {
            // Revoke blob URL on error to prevent memory leak
            if (btn._ttsBlobUrl) {
              URL.revokeObjectURL(btn._ttsBlobUrl);
              btn._ttsBlobUrl = null;
            }
            btn.innerHTML =
              '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg> Listen';
            btn.style.color = "#F87171";
            delete btn.dataset.playing;
            setTimeout(function () {
              btn.style.color = "#666";
            }, 2000);
          });
      });
    })(ttsBtn, content);
    msgEl.appendChild(ttsBtn);

    // Thumbs up / thumbs down
    var msgIdx = state.messages.length;
    var ratingBtnStyle =
      "display:inline-flex;align-items:center;gap:2px;margin-top:6px;margin-left:6px;padding:3px 8px;border-radius:6px;font-size:10px;color:#666;background:rgba(107,179,205,0.06);border:1px solid rgba(107,179,205,0.1);cursor:pointer;transition:all 0.15s;font-family:inherit;";
    var thumbUpBtn = document.createElement("button");
    thumbUpBtn.className = "nova-rate-btn";
    thumbUpBtn.setAttribute("aria-label", "Rate positive");
    thumbUpBtn.innerHTML =
      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3H14z"/><path d="M7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3"/></svg>';
    thumbUpBtn.style.cssText = ratingBtnStyle;
    var thumbDownBtn = document.createElement("button");
    thumbDownBtn.className = "nova-rate-btn";
    thumbDownBtn.setAttribute("aria-label", "Rate negative");
    thumbDownBtn.innerHTML =
      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 15v4a3 3 0 0 0 3 3l4-9V2H5.72a2 2 0 0 0-2 1.7l-1.38 9a2 2 0 0 0 2 2.3H10z"/><path d="M17 2h3a2 2 0 0 1 2 2v7a2 2 0 0 1-2 2h-3"/></svg>';
    thumbDownBtn.style.cssText = ratingBtnStyle;
    (function (upBtn, downBtn, idx) {
      function handleRating(rating) {
        if (window.posthog && typeof window.posthog.capture === "function") {
          window.posthog.capture("nova_chat_response_rated", {
            rating: rating,
            message_index: idx,
            page: window.location.pathname,
          });
        }
        if (rating === "positive") {
          upBtn.style.color = "#34D399";
          upBtn.style.borderColor = "rgba(52,211,153,0.3)";
          downBtn.style.color = "#666";
          downBtn.style.borderColor = "rgba(107,179,205,0.1)";
        } else {
          downBtn.style.color = "#F87171";
          downBtn.style.borderColor = "rgba(248,113,113,0.3)";
          upBtn.style.color = "#666";
          upBtn.style.borderColor = "rgba(107,179,205,0.1)";
        }
        upBtn.disabled = true;
        downBtn.disabled = true;
      }
      upBtn.addEventListener("click", function () {
        handleRating("positive");
      });
      downBtn.addEventListener("click", function () {
        handleRating("negative");
      });
    })(thumbUpBtn, thumbDownBtn, msgIdx);
    msgEl.appendChild(thumbUpBtn);
    msgEl.appendChild(thumbDownBtn);

    // Regenerate button
    var regenBtn = document.createElement("button");
    regenBtn.className = "nova-regen-btn";
    regenBtn.setAttribute("aria-label", "Regenerate response");
    regenBtn.innerHTML =
      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg> Regenerate';
    (function (btn) {
      btn.addEventListener("click", function () {
        regenerateLastResponse();
      });
    })(regenBtn);
    msgEl.appendChild(regenBtn);
  }

  // ---------------------------------------------------------------------------
  // Helper: add meta (sources, confidence) to an element
  // ---------------------------------------------------------------------------
  // ── Confidence Pulse: wrap dollar/percent stats with pulsing rings ──
  function applyConfidencePulse(containerEl, confidence) {
    if (!containerEl || typeof confidence !== "number" || confidence <= 0)
      return;
    // Walk text nodes looking for $X,XXX or XX% patterns inside <strong>, <td>, <code>
    var targets = containerEl.querySelectorAll("strong, td, code, b");
    var statPattern = /(\$[\d,.]+[KMBkmb]?|[\d,.]+%)/;
    targets.forEach(function (el) {
      if (
        statPattern.test(el.textContent) &&
        !el.classList.contains("conf-pulse")
      ) {
        el.classList.add("conf-pulse");
        var level =
          confidence >= 0.8 ? "high" : confidence >= 0.5 ? "medium" : "low";
        var label =
          confidence >= 0.8 ? "High" : confidence >= 0.5 ? "Medium" : "Low";
        el.classList.add("conf-" + level);
        el.setAttribute("data-confidence", label);
        el.setAttribute(
          "title",
          label + " confidence (" + Math.round(confidence * 100) + "%)",
        );
      }
    });
  }

  function addMetaToElement(msgEl, sources, confidence, breakdown) {
    if (
      (sources && sources.length > 0) ||
      (typeof confidence === "number" && confidence > 0)
    ) {
      var metaDiv = document.createElement("div");
      metaDiv.className = "nova-msg-meta";

      (sources || []).forEach(function (src) {
        var badge = document.createElement("span");
        badge.className = "nova-badge";
        badge.textContent = src;
        metaDiv.appendChild(badge);
      });

      if (typeof confidence === "number" && confidence > 0) {
        var confBadge = document.createElement("span");
        var pct = Math.round(confidence * 100);
        var confClass = pct >= 75 ? "high" : pct >= 50 ? "medium" : "low";
        confBadge.className = "nova-confidence nova-confidence-" + confClass;
        var bd = breakdown;
        if (bd && bd.grade) {
          var gradeText = "Grade " + bd.grade;
          var srcCount = bd.sources_count || 0;
          var freshness = bd.data_freshness || "curated";
          var verif = bd.verification || "unverified";
          var verifLabel =
            verif === "verified"
              ? "Verified"
              : verif === "issues_found"
                ? "Issues flagged"
                : "Unverified";
          confBadge.textContent =
            gradeText +
            " \u2022 " +
            srcCount +
            " " +
            freshness +
            " source" +
            (srcCount !== 1 ? "s" : "");
          var tooltip = document.createElement("div");
          tooltip.className = "nova-confidence-tooltip";
          tooltip.innerHTML =
            '<div class="nova-tooltip-title">Confidence Breakdown</div>' +
            '<div class="nova-tooltip-row"><span>Overall Score</span><span>' +
            pct +
            "%</span></div>" +
            '<div class="nova-tooltip-row"><span>Grade</span><span>' +
            bd.grade +
            "</span></div>" +
            '<div class="nova-tooltip-row"><span>Data Sources</span><span>' +
            srcCount +
            " (" +
            freshness +
            ")</span></div>" +
            '<div class="nova-tooltip-row"><span>Grounding</span><span>' +
            Math.round((bd.grounding_score || 0) * 100) +
            "%</span></div>" +
            '<div class="nova-tooltip-row"><span>Verification</span><span>' +
            verifLabel +
            "</span></div>" +
            '<div class="nova-tooltip-divider"></div>' +
            '<div class="nova-tooltip-note">Confidence is a quality signal, not a filter. Lower scores widen estimate ranges but do not suppress data.</div>';
          confBadge.appendChild(tooltip);
        } else {
          confBadge.textContent = pct + "% confidence";
        }
        metaDiv.appendChild(confBadge);
      }

      msgEl.appendChild(metaDiv);
    }
  }

  // ---------------------------------------------------------------------------
  // "Why this answer?" TRANSPARENCY PANEL
  //
  // Collapsible footer under each Nova response showing latency, LLM chain,
  // tools fired, confidence, sources, and KB files consulted. Accessible via
  // aria-expanded + role="region". Styles injected once.
  // ---------------------------------------------------------------------------
  var _widgetWhyPanelIdCounter = 0;

  function _widgetEnsureTransparencyStyles() {
    if (document.getElementById("nova-widget-transparency-styles")) return;
    var style = document.createElement("style");
    style.id = "nova-widget-transparency-styles";
    style.textContent = [
      ".nova-why-wrap { margin-top: 8px; font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }",
      ".nova-why-toggle {",
      "  background: none; border: 1px solid rgba(255,255,255,0.10);",
      "  border-radius: 999px; color: #a1a1aa;",
      "  font-size: 11px; font-weight: 500; padding: 3px 10px; cursor: pointer;",
      "  transition: color 140ms ease, border-color 140ms ease, background 140ms ease;",
      "  letter-spacing: 0.2px; display: inline-flex; align-items: center; gap: 6px;",
      "}",
      ".nova-why-toggle:hover { color: #6BB3CD; border-color: rgba(107,179,205,0.35); background: rgba(107,179,205,0.06); }",
      ".nova-why-toggle:focus-visible { outline: 2px solid rgba(107,179,205,0.6); outline-offset: 2px; }",
      '.nova-why-toggle::after { content: "\\25B8"; font-size: 9px; opacity: 0.7; transition: transform 160ms ease; }',
      '.nova-why-toggle[aria-expanded="true"]::after { transform: rotate(90deg); opacity: 1; }',
      ".nova-why-panel {",
      "  margin-top: 6px; padding: 10px 12px;",
      "  background: linear-gradient(180deg, rgba(32,32,88,0.30) 0%, rgba(15,15,26,0.55) 100%);",
      "  border: 1px solid rgba(90,84,189,0.22); border-left: 3px solid #5A54BD;",
      "  border-radius: 10px; font-size: 11.5px; line-height: 1.55; color: #c0c0cc;",
      "}",
      ".nova-why-row { display: flex; gap: 10px; padding: 3px 0; align-items: flex-start; }",
      ".nova-why-row + .nova-why-row { border-top: 1px solid rgba(255,255,255,0.04); padding-top: 5px; margin-top: 3px; }",
      ".nova-why-label { flex: 0 0 82px; color: #6BB3CD; font-weight: 600; font-size: 10.5px; letter-spacing: 0.3px; text-transform: uppercase; }",
      ".nova-why-value { flex: 1 1 auto; min-width: 0; word-break: break-word; }",
      ".nova-why-chip { display: inline-block; padding: 1px 7px; margin: 1px 4px 1px 0; background: rgba(107,179,205,0.10); color: #a9d3e0; border: 1px solid rgba(107,179,205,0.18); border-radius: 999px; font-size: 10px; font-weight: 500; font-family: 'SF Mono', Menlo, ui-monospace, monospace; }",
      ".nova-why-fastpath { display: inline-block; padding: 1px 8px; background: rgba(52,211,153,0.12); color: #34D399; border: 1px solid rgba(52,211,153,0.25); border-radius: 999px; font-size: 10px; font-weight: 600; letter-spacing: 0.3px; }",
      ".nova-why-empty { opacity: 0.55; font-style: italic; }",
      "@media (prefers-reduced-motion: reduce) { .nova-why-toggle, .nova-why-toggle::after { transition: none; } }",
      "",
    ].join("\n");
    document.head.appendChild(style);
  }

  function _widgetFormatLatency(ms) {
    if (ms == null || isNaN(ms)) return null;
    var n = Number(ms);
    if (n < 1000) return Math.round(n) + "ms";
    return (n / 1000).toFixed(n < 10000 ? 2 : 1) + "s";
  }

  function _widgetFormatToolsUsed(tools) {
    if (!tools || !tools.length) return null;
    var counts = {};
    tools.forEach(function (t) {
      var k = String(t || "").trim();
      if (!k) return;
      counts[k] = (counts[k] || 0) + 1;
    });
    var keys = Object.keys(counts);
    if (!keys.length) return null;
    return keys.map(function (k) {
      return counts[k] > 1 ? k + " \u00d7" + counts[k] : k;
    });
  }

  function _widgetHasAnyTransparencyData(msg) {
    if (!msg) return false;
    if (msg.timing_ms != null) return true;
    if (msg.fast_path) return true;
    if (msg.llm_provider || msg.llm_model) return true;
    if (msg.tools_used && msg.tools_used.length) return true;
    if (msg.kb_files_queried && msg.kb_files_queried.length) return true;
    if (msg.sources && msg.sources.length) return true;
    if (typeof msg.confidence === "number" && msg.confidence > 0) return true;
    return false;
  }

  function _widgetBuildWhyPanel(msg) {
    _widgetEnsureTransparencyStyles();

    var wrap = document.createElement("div");
    wrap.className = "nova-why-wrap";

    _widgetWhyPanelIdCounter++;
    var panelId = "nova-widget-why-panel-" + _widgetWhyPanelIdCounter;

    var toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "nova-why-toggle";
    toggle.setAttribute("aria-expanded", "false");
    toggle.setAttribute("aria-controls", panelId);
    toggle.textContent = "Why this answer?";

    var panel = document.createElement("div");
    panel.className = "nova-why-panel";
    panel.id = panelId;
    panel.setAttribute("role", "region");
    panel.setAttribute("aria-label", "Why this answer");
    panel.setAttribute("hidden", "hidden");

    // Latency
    var latencyStr = _widgetFormatLatency(msg.timing_ms);
    var rowLatency = document.createElement("div");
    rowLatency.className = "nova-why-row";
    rowLatency.innerHTML =
      '<div class="nova-why-label">Latency</div>' +
      '<div class="nova-why-value">' +
      (latencyStr
        ? escapeHtml(latencyStr) + " total"
        : '<span class="nova-why-empty">not recorded</span>') +
      "</div>";
    panel.appendChild(rowLatency);

    // LLM
    var rowLlm = document.createElement("div");
    rowLlm.className = "nova-why-row";
    var llmHtml =
      '<div class="nova-why-label">LLM</div><div class="nova-why-value">';
    if (msg.fast_path) {
      llmHtml +=
        '<span class="nova-why-fastpath">Deterministic fast path</span>' +
        ' <span class="nova-why-chip" style="margin-left:6px;">' +
        escapeHtml(String(msg.fast_path)) +
        "</span>";
    } else if (msg.llm_provider || msg.llm_model) {
      var llmName =
        (msg.llm_provider || "") + (msg.llm_model ? " / " + msg.llm_model : "");
      llmHtml +=
        '<span class="nova-why-chip">' + escapeHtml(llmName) + "</span>";
    } else {
      llmHtml += '<span class="nova-why-empty">provider not reported</span>';
    }
    llmHtml += "</div>";
    rowLlm.innerHTML = llmHtml;
    panel.appendChild(rowLlm);

    // Tools
    var toolsList = _widgetFormatToolsUsed(msg.tools_used);
    var rowTools = document.createElement("div");
    rowTools.className = "nova-why-row";
    if (toolsList && toolsList.length) {
      rowTools.innerHTML =
        '<div class="nova-why-label">Tools</div>' +
        '<div class="nova-why-value"><div>' +
        escapeHtml(String(toolsList.length)) +
        " tool" +
        (toolsList.length === 1 ? "" : "s") +
        ' fired</div><div style="margin-top:4px;">' +
        toolsList
          .map(function (t) {
            return '<span class="nova-why-chip">' + escapeHtml(t) + "</span>";
          })
          .join("") +
        "</div></div>";
    } else {
      rowTools.innerHTML =
        '<div class="nova-why-label">Tools</div>' +
        '<div class="nova-why-value"><span class="nova-why-empty">no tools called</span></div>';
    }
    panel.appendChild(rowTools);

    // Confidence
    var conf = msg.confidence;
    if (typeof conf === "number" && conf > 0) {
      var rowConf = document.createElement("div");
      rowConf.className = "nova-why-row";
      var pct = Math.round(conf * 100);
      rowConf.innerHTML =
        '<div class="nova-why-label">Confidence</div>' +
        '<div class="nova-why-value">' +
        escapeHtml(pct + "%") +
        (msg.quality_score
          ? ' <span class="nova-why-chip">quality ' +
            escapeHtml(String(msg.quality_score)) +
            "/100</span>"
          : "") +
        "</div>";
      panel.appendChild(rowConf);
    }

    // Sources
    var sources = msg.sources || [];
    if (sources.length) {
      var rowSrc = document.createElement("div");
      rowSrc.className = "nova-why-row";
      rowSrc.innerHTML =
        '<div class="nova-why-label">Sources</div>' +
        '<div class="nova-why-value">' +
        sources
          .map(function (s) {
            return '<span class="nova-why-chip">' + escapeHtml(s) + "</span>";
          })
          .join("") +
        "</div>";
      panel.appendChild(rowSrc);
    }

    // KB files
    var kbFiles = msg.kb_files_queried || [];
    if (kbFiles.length) {
      var rowKb = document.createElement("div");
      rowKb.className = "nova-why-row";
      rowKb.innerHTML =
        '<div class="nova-why-label">KB files</div>' +
        '<div class="nova-why-value">' +
        kbFiles
          .map(function (f) {
            return '<span class="nova-why-chip">' + escapeHtml(f) + "</span>";
          })
          .join("") +
        "</div>";
      panel.appendChild(rowKb);
    }

    toggle.addEventListener("click", function () {
      var expanded = toggle.getAttribute("aria-expanded") === "true";
      toggle.setAttribute("aria-expanded", String(!expanded));
      if (expanded) {
        panel.setAttribute("hidden", "hidden");
      } else {
        panel.removeAttribute("hidden");
      }
    });

    wrap.appendChild(toggle);
    wrap.appendChild(panel);
    return wrap;
  }

  function addTransparencyToElement(msgEl, msg) {
    if (!msgEl || !msg) return;
    if (!_widgetHasAnyTransparencyData(msg)) return;
    try {
      msgEl.appendChild(_widgetBuildWhyPanel(msg));
    } catch (_e) {
      /* non-blocking */
    }
  }

  // ---------------------------------------------------------------------------
  // Regenerate last assistant response
  // ---------------------------------------------------------------------------
  function regenerateLastResponse() {
    if (state.isLoading) return;
    // Find the last user message
    var lastUserMsg = null;
    for (var i = state.messages.length - 1; i >= 0; i--) {
      if (state.messages[i].role === "user") {
        lastUserMsg = state.messages[i].content;
        break;
      }
    }
    if (!lastUserMsg) return;

    // Remove the last assistant message from state
    if (
      state.messages.length > 0 &&
      state.messages[state.messages.length - 1].role === "assistant"
    ) {
      state.messages.pop();
      saveHistory(state.messages);
    }

    // Remove the last assistant message row from DOM
    var messagesDiv = document.getElementById("nova-messages");
    if (messagesDiv) {
      var rows = messagesDiv.querySelectorAll(".nova-msg-row-assistant");
      if (rows.length > 0) {
        rows[rows.length - 1].remove();
      }
    }

    // Re-send the user message
    var input = document.getElementById("nova-input");
    if (input) input.value = lastUserMsg;
    sendMessage();
  }

  // ---------------------------------------------------------------------------
  // Edit and re-send a user message
  // ---------------------------------------------------------------------------
  function editAndResend(msgRowEl, msgIndex) {
    if (state.isLoading) return;
    var originalContent = state.messages[msgIndex].content;
    var colEl = msgRowEl.querySelector("div[style]");
    var bubbleEl = colEl ? colEl.querySelector(".nova-msg-user") : null;
    if (!bubbleEl) return;

    // Replace bubble content with editable textarea
    var originalHtml = bubbleEl.innerHTML;
    bubbleEl.innerHTML = "";

    var editArea = document.createElement("textarea");
    editArea.className = "nova-edit-textarea";
    editArea.value = originalContent;
    editArea.rows = 2;
    bubbleEl.appendChild(editArea);

    var actionsDiv = document.createElement("div");
    actionsDiv.className = "nova-edit-actions";

    var cancelBtn = document.createElement("button");
    cancelBtn.className = "nova-edit-cancel-btn";
    cancelBtn.textContent = "Cancel";
    cancelBtn.addEventListener("click", function () {
      bubbleEl.innerHTML = originalHtml;
    });

    var saveBtn = document.createElement("button");
    saveBtn.className = "nova-edit-save-btn";
    saveBtn.textContent = "Save & Send";
    saveBtn.addEventListener("click", function () {
      var newText = editArea.value.trim();
      if (!newText) return;

      // Remove all messages after this index from state and DOM
      var messagesDiv = document.getElementById("nova-messages");
      var allRows = messagesDiv
        ? messagesDiv.querySelectorAll(".nova-msg-row")
        : [];
      // Find which DOM row corresponds to msgIndex
      var rowIdx = 0;
      var targetDomIdx = -1;
      for (var r = 0; r < allRows.length; r++) {
        if (allRows[r] === msgRowEl) {
          targetDomIdx = r;
          break;
        }
      }
      // Remove subsequent DOM rows
      if (targetDomIdx >= 0) {
        for (var r = allRows.length - 1; r > targetDomIdx; r--) {
          allRows[r].remove();
        }
      }
      // Remove the current row too (it will be re-created by appendMessage)
      msgRowEl.remove();

      // Truncate state messages
      state.messages = state.messages.slice(0, msgIndex);
      saveHistory(state.messages);

      // Mark next message as branched (Issue 5)
      state._pendingBranch = { branched_from: msgIndex };

      // Send the edited message
      var input = document.getElementById("nova-input");
      if (input) input.value = newText;
      sendMessage();
    });

    // Enter key to save
    editArea.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        saveBtn.click();
      }
    });

    actionsDiv.appendChild(cancelBtn);
    actionsDiv.appendChild(saveBtn);
    bubbleEl.appendChild(actionsDiv);

    // Auto-resize and focus
    editArea.style.height = "auto";
    editArea.style.height = Math.min(editArea.scrollHeight, 120) + "px";
    editArea.focus();
    editArea.setSelectionRange(editArea.value.length, editArea.value.length);
  }

  // ---------------------------------------------------------------------------
  // Clear conversation (internal)
  // ---------------------------------------------------------------------------
  function clearConversation() {
    state.messages = [];
    saveHistory([]);
    var messagesDiv = document.getElementById("nova-messages");
    if (messagesDiv) {
      messagesDiv.innerHTML = "";
      showWelcome();
    }
    if (window.posthog && typeof window.posthog.capture === "function") {
      window.posthog.capture("nova_chat_history_cleared", {
        source: "keyboard_shortcut",
        page: window.location.pathname,
      });
    }
  }

  // Send message
  // ---------------------------------------------------------------------------
  function sendMessage() {
    if (state.isLoading) return;

    var input = document.getElementById("nova-input");
    if (!input) return;

    var text = input.value.trim();
    if (!text) return;

    // W-04: Client-side message length cap (4000 chars)
    if (text.length > 4000) {
      appendMessage(
        {
          role: "assistant",
          content:
            "Your message exceeds the 4,000-character limit. Please shorten it and try again.",
          sources: [],
          confidence: 0,
        },
        true,
      );
      return;
    }

    // PostHog: track message sent
    state.messagesSentCount++;
    if (window.posthog && typeof window.posthog.capture === "function") {
      try {
        window.posthog.capture("nova_chat_message_sent", {
          source: "widget",
          page: window.location.pathname,
          message_length: text.length,
        });
      } catch (_e) {
        /* PostHog not ready */
      }
    }

    // W-02: Set loading state BEFORE appending message to prevent race condition
    state.isLoading = true;
    var sendBtn = document.getElementById("nova-send-btn");
    if (sendBtn) sendBtn.disabled = true;

    // Add user message (with branch metadata if from edit)
    var userMsg = { role: "user", content: text };
    if (state._pendingBranch) {
      userMsg.branched_from = state._pendingBranch.branched_from;
      delete state._pendingBranch;
    }
    appendMessage(userMsg);
    input.value = "";
    input.style.height = "auto";

    showTyping();

    // Build history for API
    var history = [];
    state.messages.forEach(function (m) {
      if (m.role === "user" || m.role === "assistant") {
        history.push({ role: m.role, content: m.content });
      }
    });

    // API call
    var sessionToken = "";
    try {
      sessionToken = localStorage.getItem(CONFIG.sessionTokenKey) || "";
    } catch (e) {
      /* localStorage read failed */
    }
    var payload = {
      message: text,
      conversation_id: state.sessionId,
      session_token: sessionToken,
      history: history.slice(-20),
    };
    // Include session context if set via setContext() public API
    if (CONFIG._sessionContext && typeof CONFIG._sessionContext === "object") {
      payload.context = CONFIG._sessionContext;
    }

    // Abort any in-progress stream before starting a new one
    if (_activeAbortCtrl) {
      try {
        _activeAbortCtrl.abort();
      } catch (_) {}
      _activeAbortCtrl = null;
    }

    // AbortController with 35-second timeout for chat requests (matches 30s server budget + margin)
    // Timeout resets on every SSE event (keepalive, status, token)
    var abortCtrl = new AbortController();
    _activeAbortCtrl = abortCtrl;
    var _streamTimeoutMs = 120000; // S21: was 35s, increased to match gunicorn 120s timeout
    var _isUserCancel = false;
    var fetchTimeout = setTimeout(function () {
      abortCtrl.abort();
    }, _streamTimeoutMs);
    function _resetStreamTimeout() {
      clearTimeout(fetchTimeout);
      fetchTimeout = setTimeout(function () {
        abortCtrl.abort();
      }, _streamTimeoutMs);
    }

    // Reset char counter
    updateCharCounter(0);

    // Get CSRF token (fetched on widget init)
    var csrfToken = window.__csrfToken || "";
    var headers = { "Content-Type": "application/json" };
    if (csrfToken) headers["X-CSRF-Token"] = csrfToken;

    if (CONFIG.useStreaming) {
      showStopButton();

      // ── Try WebSocket first, fall back to SSE ──
      _widgetGetOrCreateWS(function (_wsForWidget) {
        if (_wsForWidget) {
          // ── WebSocket streaming mode ──
          hideTyping();
          // Create streaming message element
          var wsStreamRow = document.createElement("div");
          wsStreamRow.className = "nova-msg-row nova-msg-row-assistant";
          wsStreamRow.id = "nova-stream-row";
          var wsAvatar = document.createElement("div");
          wsAvatar.className = "nova-msg-avatar nova-msg-avatar-assistant";
          wsAvatar.textContent = "N";
          wsAvatar.setAttribute("aria-hidden", "true");
          wsStreamRow.appendChild(wsAvatar);
          var wsCol = document.createElement("div");
          wsCol.style.cssText =
            "display:flex;flex-direction:column;min-width:0;flex:1;";
          var wsSender = document.createElement("div");
          wsSender.className = "nova-msg-sender nova-msg-sender-assistant";
          wsSender.textContent = "Nova";
          wsCol.appendChild(wsSender);
          var wsStreamEl = document.createElement("div");
          wsStreamEl.className = "nova-msg nova-msg-assistant";
          wsStreamEl.id = "nova-stream-msg";
          wsStreamEl.innerHTML = '<span class="nova-streaming-cursor"></span>';
          wsCol.appendChild(wsStreamEl);
          wsStreamRow.appendChild(wsCol);
          var wsMessagesEl = state.chatPanel
            ? state.chatPanel.querySelector(".nova-messages")
            : null;
          if (wsMessagesEl) {
            wsMessagesEl.appendChild(wsStreamRow);
            wsMessagesEl.scrollTo({
              top: wsMessagesEl.scrollHeight,
              behavior: "smooth",
            });
          }

          var wsCancelFn = _widgetStreamViaWS(_wsForWidget, payload, {
            onToken: function (token, fullText) {
              wsStreamEl.innerHTML = renderMarkdown(fullText);
              var curEl = document.createElement("span");
              curEl.className = "nova-streaming-cursor";
              wsStreamEl.appendChild(curEl);
              if (wsMessagesEl)
                wsMessagesEl.scrollTo({
                  top: wsMessagesEl.scrollHeight,
                  behavior: "smooth",
                });
            },
            onStatus: function (statusText) {
              var statusEl = document.getElementById("nova-status-indicator");
              if (!statusEl) {
                statusEl = document.createElement("div");
                statusEl.className = "nova-status-indicator";
                statusEl.id = "nova-status-indicator";
                if (wsMessagesEl) wsMessagesEl.appendChild(statusEl);
              }
              statusEl.textContent = escapeHtml(statusText);
            },
            onToolStatus: function (type, tool, label) {
              showWidgetToolStatus(type, tool, label);
            },
            onComplete: function (metadata, fullText) {
              clearWidgetToolStatus();
              clearTimeout(fetchTimeout);
              // PostHog tracking
              if (
                window.posthog &&
                typeof window.posthog.capture === "function"
              ) {
                window.posthog.capture("nova_chat_response_received", {
                  source: "widget",
                  page: window.location.pathname,
                  response_length: (metadata.full_response || fullText || "")
                    .length,
                  provider: metadata.provider || "",
                  confidence: metadata.confidence || 0,
                  streaming: true,
                  transport: "websocket",
                });
              }
              var finalContent = metadata.full_response || fullText;
              // Client-side safety net: never render a blank response in the widget
              if (!finalContent || !finalContent.trim()) {
                finalContent =
                  "I'm having trouble with that request. Please try rephrasing — for example:\n\n" +
                  '- *"What are the best job boards for nurses in Texas?"*\n' +
                  '- *"Create a media plan for 20 software engineers in Austin"*\n' +
                  '- *"What salary should I offer for a Data Scientist in NYC?"*\n\n' +
                  "I specialize in recruitment marketing — channel recommendations, media plans, salary benchmarks, and hiring costs.";
              }
              var finalSources = metadata.sources || [];
              var finalConfidence = metadata.confidence || 0;
              var finalMsgId = metadata.message_id || "";
              // Transparency panel payload -- captured so re-renders (history
              // rehydration) can still display the "Why this answer?" footer.
              var finalToolsUsed = metadata.tools_used || [];
              var finalLlmProvider = metadata.llm_provider || "";
              var finalLlmModel = metadata.llm_model || "";
              var finalFastPath = metadata.fast_path || "";
              var finalQualityScore = metadata.quality_score || 0;
              var finalKbFiles = metadata.kb_files_queried || [];
              var finalTimingMs =
                metadata.timing_ms != null ? metadata.timing_ms : null;
              if (metadata.session_token) {
                try {
                  localStorage.setItem(
                    CONFIG.sessionTokenKey,
                    metadata.session_token,
                  );
                } catch (e) {}
              }
              var cursors = wsStreamEl.querySelectorAll(
                ".nova-streaming-cursor",
              );
              cursors.forEach(function (c) {
                c.remove();
              });
              var statusCleanup = document.getElementById(
                "nova-status-indicator",
              );
              if (statusCleanup) statusCleanup.remove();
              wsStreamEl.innerHTML = renderMarkdown(finalContent);
              addActionButtonsToElement(wsStreamEl, finalContent);
              addMetaToElement(wsStreamEl, finalSources, finalConfidence, null);
              // Attach live transparency panel to the streaming element so the
              // user sees it immediately without waiting for a history reload.
              addTransparencyToElement(wsStreamEl, {
                sources: finalSources,
                confidence: finalConfidence,
                tools_used: finalToolsUsed,
                llm_provider: finalLlmProvider,
                llm_model: finalLlmModel,
                fast_path: finalFastPath,
                quality_score: finalQualityScore,
                kb_files_queried: finalKbFiles,
                timing_ms: finalTimingMs,
              });
              applyConfidencePulse(wsStreamEl, finalConfidence);
              var wsColEl = wsStreamEl.parentNode;
              if (wsColEl) {
                var now = new Date();
                var tsEl = document.createElement("div");
                tsEl.className =
                  "nova-msg-timestamp nova-msg-timestamp-assistant";
                tsEl.textContent =
                  String(now.getHours()).padStart(2, "0") +
                  ":" +
                  String(now.getMinutes()).padStart(2, "0");
                wsColEl.appendChild(tsEl);
              }
              var wsRowCleanup = document.getElementById("nova-stream-row");
              if (wsRowCleanup) wsRowCleanup.removeAttribute("id");
              wsStreamEl.removeAttribute("id");
              state.messages.push({
                role: "assistant",
                content: finalContent,
                sources: finalSources,
                confidence: finalConfidence,
                message_id: finalMsgId,
                tools_used: finalToolsUsed,
                llm_provider: finalLlmProvider,
                llm_model: finalLlmModel,
                fast_path: finalFastPath,
                quality_score: finalQualityScore,
                kb_files_queried: finalKbFiles,
                timing_ms: finalTimingMs,
              });
              saveHistory(state.messages);
              state.isLoading = false;
              _activeAbortCtrl = null;
              hideStopButton();
              if (sendBtn) sendBtn.disabled = false;
              if (input) input.focus();
            },
            onError: function (errMsg) {
              clearTimeout(fetchTimeout);
              _widgetWsFailCount++;
              var wsRowCleanup = document.getElementById("nova-stream-row");
              if (wsRowCleanup) wsRowCleanup.remove();
              showStreamError(
                errMsg || "Connection error. Please try again.",
                text,
              );
              state.isLoading = false;
              _activeAbortCtrl = null;
              hideStopButton();
              if (sendBtn) sendBtn.disabled = false;
              if (input) input.focus();
            },
          });

          _activeAbortCtrl = {
            abort: function () {
              wsCancelFn();
            },
          };
          return; // Skip SSE path
        }

        // ── SSE Streaming fallback ──
        fetch(CONFIG.streamUrl, {
          method: "POST",
          headers: headers,
          body: JSON.stringify(payload),
          signal: abortCtrl.signal,
        })
          .then(function (res) {
            if (!res.ok) throw new Error("HTTP " + res.status);
            hideTyping();
            // Create streaming message element with avatar row and blinking cursor
            var streamRow = document.createElement("div");
            streamRow.className = "nova-msg-row nova-msg-row-assistant";
            streamRow.id = "nova-stream-row";
            var streamAvatar = document.createElement("div");
            streamAvatar.className =
              "nova-msg-avatar nova-msg-avatar-assistant";
            streamAvatar.textContent = "N";
            streamAvatar.setAttribute("aria-hidden", "true");
            streamAvatar.setAttribute("data-tooltip", "Nova");
            streamRow.appendChild(streamAvatar);
            var streamCol = document.createElement("div");
            streamCol.style.cssText =
              "display:flex;flex-direction:column;min-width:0;flex:1;";
            var streamSender = document.createElement("div");
            streamSender.className =
              "nova-msg-sender nova-msg-sender-assistant";
            streamSender.textContent = "Nova";
            streamCol.appendChild(streamSender);
            var streamEl = document.createElement("div");
            streamEl.className = "nova-msg nova-msg-assistant";
            streamEl.id = "nova-stream-msg";
            streamEl.innerHTML = '<span class="nova-streaming-cursor"></span>';
            streamCol.appendChild(streamEl);
            streamRow.appendChild(streamCol);
            var messagesEl = state.chatPanel
              ? state.chatPanel.querySelector(".nova-messages")
              : null;
            if (messagesEl) {
              messagesEl.appendChild(streamRow);
              messagesEl.scrollTo({
                top: messagesEl.scrollHeight,
                behavior: "smooth",
              });
            }
            var reader = res.body.getReader();
            var decoder = new TextDecoder();
            var buffer = "";
            var fullText = "";
            var metadata = {};
            var streamDone = false;
            var _statusRemoved = false;
            var _chunkIterations = 0;
            var _streamStartTime = Date.now();
            function processChunk() {
              if (++_chunkIterations > 50000) {
                _removeTypingIndicator();
                state.isLoading = false;
                return;
              }
              if (Date.now() - _streamStartTime > 120000) {
                abortCtrl.abort();
                _removeTypingIndicator();
                state.isLoading = false;
                return;
              }
              return reader.read().then(function (result) {
                if (result.done) return "stream_complete";
                buffer += decoder.decode(result.value, { stream: true });
                var lines = buffer.split("\n");
                buffer = lines.pop() || "";
                lines.forEach(function (line) {
                  if (line.indexOf("data: ") !== 0) return;
                  // Reset timeout on every SSE event (keepalive, status, token)
                  _resetStreamTimeout();
                  try {
                    var evt = JSON.parse(line.substring(6));
                    // Skip keepalive heartbeats (just reset timeout above)
                    if (evt.keepalive) return;
                    if (evt.done) {
                      metadata = evt;
                      streamDone = true;
                      clearWidgetToolStatus();
                      return;
                    }
                    // S18: Tool status events
                    if (
                      evt.type === "tool_start" ||
                      evt.type === "tool_complete"
                    ) {
                      showWidgetToolStatus(evt.type, evt.tool, evt.label);
                      return;
                    }
                    // Handle status/progress events
                    if (evt.status) {
                      var statusEl = document.getElementById(
                        "nova-status-indicator",
                      );
                      if (!statusEl) {
                        statusEl = document.createElement("div");
                        statusEl.className = "nova-status-indicator";
                        statusEl.id = "nova-status-indicator";
                        // Insert after typing indicator or at end of messages
                        var typingEl = document.getElementById("nova-typing");
                        if (typingEl && typingEl.parentNode) {
                          typingEl.parentNode.insertBefore(
                            statusEl,
                            typingEl.nextSibling,
                          );
                        } else if (messagesEl) {
                          messagesEl.appendChild(statusEl);
                        }
                      }
                      statusEl.textContent = escapeHtml(evt.status);
                      if (messagesEl)
                        messagesEl.scrollTo({
                          top: messagesEl.scrollHeight,
                          behavior: "smooth",
                        });
                    }
                    if (evt.token) {
                      // Remove status indicator once real tokens arrive (guarded)
                      if (!_statusRemoved) {
                        var activeStatus = document.getElementById(
                          "nova-status-indicator",
                        );
                        if (activeStatus) {
                          activeStatus.remove();
                          _statusRemoved = true;
                        }
                        clearWidgetToolStatus();
                      }
                      fullText += evt.token;
                      streamEl.innerHTML = renderMarkdown(fullText);
                      // Re-append blinking cursor at end of streamed content
                      var curEl = document.createElement("span");
                      curEl.className = "nova-streaming-cursor";
                      streamEl.appendChild(curEl);
                      if (messagesEl)
                        messagesEl.scrollTo({
                          top: messagesEl.scrollHeight,
                          behavior: "smooth",
                        });
                    }
                  } catch (e) {
                    console.warn("SSE parse error:", e);
                  }
                });
                // If server sent the done event, stop recursing
                if (streamDone) return "stream_complete";
                return processChunk();
              });
            }
            return processChunk().then(function () {
              clearTimeout(fetchTimeout);
              // PostHog: track response received
              if (
                window.posthog &&
                typeof window.posthog.capture === "function"
              ) {
                window.posthog.capture("nova_chat_response_received", {
                  source: "widget",
                  page: window.location.pathname,
                  response_length: (metadata.full_response || fullText || "")
                    .length,
                  provider: metadata.provider || "",
                  confidence: metadata.confidence || 0,
                  streaming: true,
                });
                // PostHog: track each tool used during this response
                var toolsUsed = metadata.tools_used || [];
                toolsUsed.forEach(function (toolName) {
                  window.posthog.capture("nova_chat_tool_used", {
                    tool_name: toolName,
                    page: window.location.pathname,
                  });
                });
              }

              // Fix: Update streaming element IN PLACE instead of remove+recreate (avoids flash)
              var finalContent = metadata.full_response || fullText;
              // Client-side safety net: never render a blank response in the widget
              if (!finalContent || !finalContent.trim()) {
                finalContent =
                  "I'm having trouble with that request. Please try rephrasing — for example:\n\n" +
                  '- *"What are the best job boards for nurses in Texas?"*\n' +
                  '- *"Create a media plan for 20 software engineers in Austin"*\n' +
                  '- *"What salary should I offer for a Data Scientist in NYC?"*\n\n' +
                  "I specialize in recruitment marketing — channel recommendations, media plans, salary benchmarks, and hiring costs.";
              }
              var finalSources = metadata.sources || [];
              var finalConfidence = metadata.confidence || 0;
              var finalBreakdown = null;
              var finalMsgId = metadata.message_id || "";
              // Transparency panel payload
              var finalToolsUsed = metadata.tools_used || [];
              var finalLlmProvider = metadata.llm_provider || "";
              var finalLlmModel = metadata.llm_model || "";
              var finalFastPath = metadata.fast_path || "";
              var finalQualityScore = metadata.quality_score || 0;
              var finalKbFiles = metadata.kb_files_queried || [];
              var finalTimingMs =
                metadata.timing_ms != null ? metadata.timing_ms : null;

              // Store session token if returned by server (Issue 4)
              if (metadata.session_token) {
                try {
                  localStorage.setItem(
                    CONFIG.sessionTokenKey,
                    metadata.session_token,
                  );
                } catch (e) {
                  /* localStorage write failed */
                }
              }

              // Remove blinking cursor
              var cursors = streamEl.querySelectorAll(".nova-streaming-cursor");
              cursors.forEach(function (c) {
                c.remove();
              });

              // Re-render final markdown content into existing element
              streamEl.innerHTML = renderMarkdown(finalContent);

              // Remove the status indicator if any (guarded)
              if (!_statusRemoved) {
                var statusEl = document.getElementById("nova-status-indicator");
                if (statusEl) {
                  statusEl.remove();
                  _statusRemoved = true;
                }
              }

              // Add action buttons to existing stream element
              addActionButtonsToElement(streamEl, finalContent);

              // Add source badges and confidence to existing stream element
              addMetaToElement(
                streamEl,
                finalSources,
                finalConfidence,
                finalBreakdown,
              );

              // "Why this answer?" transparency panel (collapsed by default)
              addTransparencyToElement(streamEl, {
                sources: finalSources,
                confidence: finalConfidence,
                tools_used: finalToolsUsed,
                llm_provider: finalLlmProvider,
                llm_model: finalLlmModel,
                fast_path: finalFastPath,
                quality_score: finalQualityScore,
                kb_files_queried: finalKbFiles,
                timing_ms: finalTimingMs,
              });

              // Confidence Pulse: animate stat values with pulsing rings
              applyConfidencePulse(streamEl, finalConfidence);

              // Add timestamp to the stream column
              var streamColEl = streamEl.parentNode;
              if (streamColEl) {
                var now = new Date();
                var tsEl = document.createElement("div");
                tsEl.className =
                  "nova-msg-timestamp nova-msg-timestamp-assistant";
                tsEl.textContent =
                  String(now.getHours()).padStart(2, "0") +
                  ":" +
                  String(now.getMinutes()).padStart(2, "0");
                streamColEl.appendChild(tsEl);
              }

              // Remove the temporary IDs so they don't conflict
              var streamRowEl = document.getElementById("nova-stream-row");
              if (streamRowEl) streamRowEl.removeAttribute("id");
              streamEl.removeAttribute("id");

              // Persist message to state + storage (includes transparency
              // fields so re-renders after reload keep the panel accurate)
              var msgObj = {
                role: "assistant",
                content: finalContent,
                sources: finalSources,
                confidence: finalConfidence,
                confidence_breakdown: finalBreakdown,
                message_id: finalMsgId,
                tools_used: finalToolsUsed,
                llm_provider: finalLlmProvider,
                llm_model: finalLlmModel,
                fast_path: finalFastPath,
                quality_score: finalQualityScore,
                kb_files_queried: finalKbFiles,
                timing_ms: finalTimingMs,
              };
              state.messages.push(msgObj);
              saveHistory(state.messages);
            });
          })
          .catch(function (err) {
            clearTimeout(fetchTimeout);
            hideTyping();
            // PostHog: track chat error
            if (
              window.posthog &&
              typeof window.posthog.capture === "function"
            ) {
              window.posthog.capture("nova_chat_error", {
                source: "widget",
                page: window.location.pathname,
                error_type: err.name || "Unknown",
                error_message: (err.message || "").substring(0, 200),
                streaming: true,
              });
            }
            var streamRowCleanup = document.getElementById("nova-stream-row");
            if (streamRowCleanup) streamRowCleanup.remove();
            var streamEl = document.getElementById("nova-stream-msg");
            if (streamEl) streamEl.remove();

            // Fallback: if streaming failed (not abort), try non-streaming endpoint
            if (err.name !== "AbortError") {
              console.warn(
                "Streaming failed, falling back to non-streaming endpoint:",
                err.message,
              );
              return fetch(CONFIG.apiUrl, {
                method: "POST",
                headers: headers,
                body: JSON.stringify(payload),
              })
                .then(function (res) {
                  if (!res.ok) throw new Error("HTTP " + res.status);
                  return res.json();
                })
                .then(function (data) {
                  appendMessage({
                    role: "assistant",
                    content: data.response || "No response received.",
                    sources: data.sources || [],
                    confidence: data.confidence || 0,
                    confidence_breakdown: data.confidence_breakdown || null,
                    tools_used: data.tools_used || [],
                    llm_provider: data.llm_provider || "",
                    llm_model: data.llm_model || "",
                    fast_path: data.fast_path || "",
                    quality_score: data.quality_score || 0,
                    kb_files_queried: data.kb_files_queried || [],
                    timing_ms: data.timing_ms != null ? data.timing_ms : null,
                  });
                })
                .catch(function (fallbackErr) {
                  // Fallback also failed -- show error with retry
                  showStreamError(
                    fallbackErr.message ||
                      "Connection error. Please try again.",
                    text,
                  );
                })
                .finally(function () {
                  state.isLoading = false;
                  if (sendBtn) sendBtn.disabled = false;
                  _removeTypingIndicator();
                });
            }

            // AbortError -- distinguish user cancel from timeout
            if (abortCtrl._isUserCancel) {
              showStreamError("Response was stopped.", text, true);
            } else {
              showStreamError(
                "I'm having trouble connecting. Please try again.",
                text,
              );
            }
          })
          .finally(function () {
            state.isLoading = false;
            _activeAbortCtrl = null;
            hideStopButton();
            if (sendBtn) sendBtn.disabled = false;
            if (input) input.focus();
            // Defensive cleanup: remove any stray streaming cursors that
            // may persist after errors or edge-case race conditions
            var strayCursors = document.querySelectorAll(
              ".nova-streaming-cursor",
            );
            strayCursors.forEach(function (el) {
              el.remove();
            });
            // Also remove any orphaned stream message elements
            var orphanStreamRow = document.getElementById("nova-stream-row");
            if (orphanStreamRow) orphanStreamRow.remove();
            var orphanStream = document.getElementById("nova-stream-msg");
            if (orphanStream) orphanStream.remove();
          });
      }); // end _widgetGetOrCreateWS callback
    } else {
      // ── Non-streaming fallback ──
      fetch(CONFIG.apiUrl, {
        method: "POST",
        headers: headers,
        body: JSON.stringify(payload),
        signal: abortCtrl.signal,
      })
        .then(function (res) {
          clearTimeout(fetchTimeout);
          if (!res.ok) throw new Error("HTTP " + res.status);
          return res.json();
        })
        .then(function (data) {
          hideTyping();
          // Unwrap API envelope: _send_json wraps as {success, data, error}
          var result = data && data.data ? data.data : data;
          // PostHog: track non-streaming response received
          if (window.posthog && typeof window.posthog.capture === "function") {
            window.posthog.capture("nova_chat_response_received", {
              source: "widget",
              page: window.location.pathname,
              response_length: (result.response || "").length,
              provider: result.provider || "",
              confidence: result.confidence || 0,
              streaming: false,
            });
            // PostHog: track each tool used during this response
            var nsToolsUsed = result.tools_used || [];
            nsToolsUsed.forEach(function (toolName) {
              window.posthog.capture("nova_chat_tool_used", {
                tool_name: toolName,
                page: window.location.pathname,
              });
            });
          }
          appendMessage({
            role: "assistant",
            content: result.response || "No response received.",
            sources: result.sources || [],
            confidence: result.confidence || 0,
            confidence_breakdown: result.confidence_breakdown || null,
            tools_used: result.tools_used || [],
            llm_provider: result.llm_provider || "",
            llm_model: result.llm_model || "",
            fast_path: result.fast_path || "",
            quality_score: result.quality_score || 0,
            kb_files_queried: result.kb_files_queried || [],
            timing_ms: result.timing_ms != null ? result.timing_ms : null,
          });
        })
        .catch(function (err) {
          clearTimeout(fetchTimeout);
          hideTyping();
          // PostHog: track non-streaming error
          if (window.posthog && typeof window.posthog.capture === "function") {
            window.posthog.capture("nova_chat_error", {
              source: "widget",
              page: window.location.pathname,
              error_type: err.name || "Unknown",
              error_message: (err.message || "").substring(0, 200),
              streaming: false,
            });
          }
          var errorMsg =
            err.name === "AbortError"
              ? "The request timed out. Please try a shorter question or try again later."
              : "Sorry, I encountered an error connecting to the server. Please try again.";
          appendMessage({
            role: "assistant",
            content: errorMsg,
            sources: [],
            confidence: 0,
          });
        })
        .finally(function () {
          state.isLoading = false;
          if (sendBtn) sendBtn.disabled = false;
          if (input) input.focus();
        });
    }
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------
  window.NovaChat = {
    /**
     * Initialize the Nova chat widget.
     * @param {Object} options
     * @param {string} [options.containerId] - DOM element ID to mount the panel in
     * @param {string} [options.apiUrl] - Custom API endpoint URL
     * @param {string} [options.primaryColor] - Custom primary brand color
     */
    init: function (options) {
      if (state.initialized) return;
      options = options || {};

      if (options.apiUrl) CONFIG.apiUrl = options.apiUrl;
      if (options.primaryColor) CONFIG.primaryColor = options.primaryColor;

      // Fetch CSRF token on init (needed for double-submit cookie pattern)
      function _fetchCsrfToken() {
        fetch("/api/csrf-token", { credentials: "same-origin" })
          .then(function (r) {
            return r.json();
          })
          .then(function (d) {
            window.__csrfToken = d.token || "";
            window.__csrfTokenFetchedAt = Date.now();
          })
          .catch(function () {
            /* CSRF fetch failed -- requests may fail */
          });
      }
      if (!window.__csrfToken) {
        _fetchCsrfToken();
      }
      // S47: Auto-refresh CSRF token before 4-hour expiry (check every 10 min)
      if (!window.__csrfRefreshInterval) {
        window.__csrfRefreshInterval = setInterval(
          function () {
            var elapsed = Date.now() - (window.__csrfTokenFetchedAt || 0);
            if (
              !window.__csrfTokenFetchedAt ||
              elapsed > (4 * 60 - 10) * 60 * 1000
            ) {
              _fetchCsrfToken();
            }
          },
          10 * 60 * 1000,
        );
      }

      // S25: Skip widget init when running inside the platform shell.
      // The platform already has its own Nova AI drawer/tab. Loading the
      // floating widget inside a fragment creates a position:fixed element
      // that overlaps the main content area.
      if (document.querySelector(".platform-shell") || window.__NOVA_PLATFORM) {
        return;
      }

      // Build widget when DOM is ready.  Covers three cases:
      // 1. Script in <head> (readyState === "loading"): wait for DOMContentLoaded
      // 2. Script at end of <body> (readyState "interactive"/"complete"): build now
      // 3. Fallback: if neither path fires within 500ms, force build via setTimeout
      var _built = false;
      function _safeBuild() {
        if (_built) return;
        _built = true;
        buildWidget(options.containerId);
      }

      if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", _safeBuild);
      } else {
        _safeBuild();
      }

      // Safety net -- guarantee the widget appears even if DOMContentLoaded
      // never fires (edge case with some browser extensions / service workers)
      setTimeout(_safeBuild, 500);

      state.initialized = true;
    },

    /**
     * Programmatically open the chat panel.
     */
    open: function () {
      if (!state.isOpen && state.chatPanel) togglePanel();
    },

    /**
     * Programmatically close the chat panel.
     */
    close: function () {
      // Force close regardless of state.isOpen -- S30 fix v2
      togglePanel(true);
    },

    /**
     * Send a message programmatically.
     * @param {string} message
     */
    send: function (message) {
      if (!message) return;
      var input = document.getElementById("nova-input");
      if (input) {
        input.value = message;
        sendMessage();
      }
    },

    /**
     * Provide session context (e.g., from an active media plan).
     * @param {Object} context - { roles, locations, industry, budget, enriched, synthesized }
     */
    setContext: function (context) {
      if (context && typeof context === "object") {
        CONFIG._sessionContext = context;
      }
    },

    /**
     * Track voice input usage (call from voice feature when implemented).
     */
    trackVoiceUsed: function () {
      if (window.posthog && typeof window.posthog.capture === "function") {
        window.posthog.capture("nova_chat_voice_used", {
          source: "widget",
          page: window.location.pathname,
        });
      }
    },

    /**
     * Track file upload (call from file upload feature when implemented).
     * @param {string} fileType - MIME type or extension
     * @param {number} fileSize - Size in bytes
     */
    trackFileUploaded: function (fileType, fileSize) {
      if (window.posthog && typeof window.posthog.capture === "function") {
        window.posthog.capture("nova_chat_file_uploaded", {
          source: "widget",
          page: window.location.pathname,
          file_type: fileType || "unknown",
          file_size: fileSize || 0,
        });
      }
    },

    /**
     * Clear conversation history.
     */
    clearHistory: function () {
      clearConversation();
    },
  };
})();
