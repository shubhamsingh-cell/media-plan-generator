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
    primaryColor: "#6BB3CD",
    primaryDark: "#0f0f1a",
    primaryLight: "#8bc7db",
    accentColor: "#6BB3CD",
    accentLight: "#8bc7db",
    accentPurple: "#5A54BD",
    textColor: "#d4d4d8",
    textLight: "#888",
    bgColor: "#0f0f1a",
    bgLight: "rgba(26,26,46,0.95)",
    borderColor: "rgba(107,179,205,0.1)",
    errorColor: "#F87171",
    successColor: "#34D399",
    maxHistoryStorage: 50,
    storageKey: "nova_chat_history",
    sessionKey: "nova_session",
    widgetWidth: "400px",
    widgetHeight: "580px",
    mobileBreakpoint: 640,
  };

  var SUGGESTED_QUESTIONS = [
    "What publishers work best for nursing roles?",
    "Compare CPC benchmarks across Google, LinkedIn, and Indeed for healthcare",
    "How difficult is it to hire software engineers right now?",
    "Recommend a $50K budget allocation for 10 engineering hires",
  ];

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  var state = {
    isOpen: false,
    isLoading: false,
    messages: [],
    sessionId: null,
    container: null,
    chatPanel: null,
    floatingBtn: null,
    initialized: false,
  };

  // ---------------------------------------------------------------------------
  // Styles (injected once)
  // ---------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById("nova-styles")) return;

    var css =
      "" +
      "#nova-float-btn {" +
      "  position: fixed; bottom: 24px; right: 24px; z-index: 99999;" +
      "  width: 68px; height: 68px; border-radius: 50%;" +
      "  background: #0f0f1a;" +
      "  color: #fff; border: 1px solid rgba(107,179,205,0.15); cursor: pointer;" +
      "  box-shadow: 0 4px 20px rgba(0,0,0,0.4);" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  transition: transform 0.3s cubic-bezier(0.34, 1.56, 0.64, 1), box-shadow 0.3s ease;" +
      "  font-size: 0; padding: 0; overflow: hidden;" +
      "}" +
      "#nova-float-btn:hover {" +
      "  transform: translateY(-3px) scale(1.1);" +
      "  box-shadow: 0 8px 30px rgba(0,0,0,0.5), 0 0 0 1px rgba(107,179,205,0.25);" +
      "  border-color: rgba(107,179,205,0.25);" +
      "}" +
      "#nova-float-btn:active {" +
      "  transform: scale(0.95);" +
      "}" +
      "#nova-float-btn svg { width: 26px; height: 26px; }" +
      "#nova-float-btn canvas { display: block; }" +
      "#nova-float-btn.nova-btn-close { background: linear-gradient(135deg, #1a1a2e 0%, #5A54BD 100%); border-color: rgba(107,179,205,0.2); }" +
      "#nova-float-btn.nova-btn-close:hover { box-shadow: 0 8px 36px rgba(107,179,205,0.3), 0 0 60px rgba(90,84,189,0.1); }" +
      // ── Dark Glassmorphism Panel ──
      "#nova-panel {" +
      "  position: fixed; bottom: 96px; right: 24px; z-index: 99998;" +
      "  width: 420px; height: 600px;" +
      "  max-height: calc(100vh - 120px);" +
      "  background: rgba(15,15,26,0.97);" +
      "  backdrop-filter: blur(24px) saturate(1.4);" +
      "  -webkit-backdrop-filter: blur(24px) saturate(1.4);" +
      "  border-radius: 16px;" +
      "  box-shadow: 0 16px 48px rgba(0,0,0,0.6);" +
      "  display: flex; flex-direction: column;" +
      "  overflow: hidden;" +
      "  transition: opacity 0.35s cubic-bezier(0.4, 0, 0.2, 1), transform 0.35s cubic-bezier(0.4, 0, 0.2, 1);" +
      '  font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;' +
      "  border: 1px solid rgba(107,179,205,0.1);" +
      "}" +
      "#nova-panel.nova-hidden {" +
      "  opacity: 0; transform: translateY(20px) scale(0.92); pointer-events: none;" +
      "}" +
      "#nova-panel.nova-visible {" +
      "  opacity: 1; transform: translateY(0) scale(1); pointer-events: auto;" +
      "}" +
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
      "  font-size: 10px; opacity: 0.6; margin-top: 1px; letter-spacing: 0.5px; color: #8899aa;" +
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
      // ── Message bubbles ──
      ".nova-msg {" +
      "  max-width: 88%; padding: 12px 16px; border-radius: 16px;" +
      "  font-size: 13px; line-height: 1.65; word-wrap: break-word;" +
      "  animation: nova-msgIn 0.4s cubic-bezier(0.16, 1, 0.3, 1);" +
      "}" +
      "@keyframes nova-msgIn {" +
      "  from { opacity: 0; transform: translateY(12px) scale(0.97); }" +
      "  to { opacity: 1; transform: translateY(0) scale(1); }" +
      "}" +
      ".nova-msg-user {" +
      "  align-self: flex-end;" +
      "  background: rgba(90,84,189,0.15);" +
      "  color: #d4d4d8;" +
      "  border: 1px solid rgba(90,84,189,0.2);" +
      "  border-bottom-right-radius: 4px;" +
      "  box-shadow: 0 2px 12px rgba(0,0,0,0.08);" +
      "}" +
      ".nova-msg-assistant {" +
      "  align-self: flex-start;" +
      "  background: rgba(26,26,46,0.8);" +
      "  color: #d4d4d8;" +
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
      "  0%, 60%, 100% { transform: translateY(0); opacity: 0.3; }" +
      "  30% { transform: translateY(-8px); opacity: 1; }" +
      "}" +
      // ── Suggested questions ──
      ".nova-suggestions {" +
      "  padding: 12px 16px 8px; display: flex; flex-direction: column; gap: 6px;" +
      "}" +
      ".nova-suggestions-title {" +
      "  font-size: 9px; color: #555; font-weight: 600;" +
      "  text-transform: uppercase; letter-spacing: 2px; margin-bottom: 4px;" +
      "}" +
      ".nova-suggestion-btn {" +
      "  background: transparent; border: 1px solid rgba(107,179,205,0.1);" +
      "  padding: 9px 14px; border-radius: 12px; cursor: pointer;" +
      "  font-size: 12px; color: #a1a1a1; text-align: left;" +
      "  font-family: inherit;" +
      "  transition: opacity 0.25s cubic-bezier(0.4, 0, 0.2, 1), border-color 0.25s ease; line-height: 1.4;" +
      "}" +
      ".nova-suggestion-btn:hover {" +
      "  border-color: rgba(107,179,205,0.25);" +
      "  opacity: 1;" +
      "  color: #d4d4d8;" +
      "}" +
      // ── Input area ──
      ".nova-input-area {" +
      "  padding: 14px 16px; border-top: 1px solid rgba(107,179,205,0.1);" +
      "  display: flex; gap: 10px; align-items: flex-end;" +
      "  background: rgba(15,15,26,0.9); flex-shrink: 0;" +
      "}" +
      ".nova-input {" +
      "  flex: 1; border: 1px solid rgba(107,179,205,0.1);" +
      "  border-radius: 12px; padding: 10px 14px;" +
      "  font-size: 13px; font-family: inherit;" +
      "  resize: none; outline: none; min-height: 20px; max-height: 100px;" +
      "  line-height: 1.5; background: rgba(20,20,37,0.8);" +
      "  color: #d4d4d8;" +
      "  transition: border-color 0.2s, background 0.2s, box-shadow 0.2s;" +
      "}" +
      ".nova-input:focus {" +
      "  border-color: rgba(107,179,205,0.3); background: rgba(107,179,205,0.04);" +
      "  box-shadow: 0 0 0 3px rgba(107,179,205,0.08);" +
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
      "  transform: scale(1.08);" +
      "  box-shadow: 0 4px 20px rgba(0,0,0,0.3);" +
      "}" +
      ".nova-send-btn:active { transform: scale(0.95); }" +
      ".nova-send-btn:disabled { opacity: 0.25; cursor: not-allowed; transform: none; box-shadow: none; }" +
      ".nova-send-btn svg { width: 18px; height: 18px; }" +
      // ── Footer ──
      ".nova-footer {" +
      "  text-align: center; padding: 6px 12px; font-size: 9px;" +
      "  color: #555; background: transparent;" +
      "  flex-shrink: 0;" +
      "  border-top: 1px solid rgba(107,179,205,0.06);" +
      "  letter-spacing: 1px; text-transform: uppercase;" +
      "}" +
      // ── Mobile responsive ──
      "@media (max-width: " +
      CONFIG.mobileBreakpoint +
      "px) {" +
      "  #nova-panel {" +
      "    width: calc(100vw - 16px); height: calc(100vh - 80px);" +
      "    max-height: none; bottom: 76px; right: 8px;" +
      "    border-radius: 16px;" +
      "  }" +
      "  #nova-float-btn { bottom: 12px; right: 12px; }" +
      "}" +
      // ── Reduced motion ──
      "@media (prefers-reduced-motion: reduce) {" +
      "  .nova-msg-user, .nova-msg-assistant { animation: none !important; }" +
      "  .nova-typing-dot { animation: none !important; opacity: 0.5; }" +
      "  #nova-float-btn, .nova-send-btn { transition: none !important; }" +
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
  };

  // ---------------------------------------------------------------------------
  // Markdown renderer (lightweight, no dependencies)
  // ---------------------------------------------------------------------------
  function renderMarkdown(text) {
    if (!text) return "";
    var html = escapeHtml(text);

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

    // Unordered lists
    html = html.replace(/^- (.+)$/gm, "<li>$1</li>");
    html = html.replace(/((?:<li>.+<\/li>\n?)+)/g, "<ul>$1</ul>");

    // Line breaks (double newline = paragraph, single = br)
    html = html.replace(/\n\n/g, "</p><p>");
    html = html.replace(/\n/g, "<br/>");

    // Wrap in paragraph if not already wrapped
    if (
      html.indexOf("<h3>") !== 0 &&
      html.indexOf("<ul>") !== 0 &&
      html.indexOf("<table>") !== 0
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

  // ---------------------------------------------------------------------------
  // Session / History persistence
  // ---------------------------------------------------------------------------
  function loadHistory() {
    try {
      var stored = sessionStorage.getItem(CONFIG.storageKey);
      return stored ? JSON.parse(stored) : [];
    } catch (e) {
      return [];
    }
  }

  function saveHistory(messages) {
    try {
      var trimmed = messages.slice(-CONFIG.maxHistoryStorage);
      sessionStorage.setItem(CONFIG.storageKey, JSON.stringify(trimmed));
    } catch (e) {
      /* ignore storage errors */
    }
  }

  function getSessionId() {
    try {
      var sid = sessionStorage.getItem(CONFIG.sessionKey);
      if (!sid) {
        sid =
          "nova-" + Date.now() + "-" + Math.random().toString(36).substr(2, 6);
        sessionStorage.setItem(CONFIG.sessionKey, sid);
      }
      return sid;
    } catch (e) {
      return "nova-" + Date.now();
    }
  }

  // ---------------------------------------------------------------------------
  // DOM construction
  // ---------------------------------------------------------------------------
  function buildWidget(containerId) {
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

      var animId;
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
        animId = requestAnimationFrame(draw);
      }
      document.addEventListener("visibilitychange", function () {
        if (document.hidden) cancelAnimationFrame(animId);
        else animId = requestAnimationFrame(draw);
      });
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
      '<div style="display:flex;align-items:center;gap:6px;">' +
      '<button class="nova-export-btn" aria-label="Export conversation" title="Export conversation" style="' +
      "background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);" +
      "color:rgba(255,255,255,0.7);cursor:pointer;padding:5px;border-radius:8px;line-height:1;" +
      'transition:all 0.2s;">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
      'stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px;">' +
      '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>' +
      '<polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>' +
      "</button>" +
      '<button class="nova-close-btn" aria-label="Close chat">' +
      ICONS.close +
      "</button>" +
      "</div>";
    header
      .querySelector(".nova-export-btn")
      .addEventListener("click", exportConversation);
    header
      .querySelector(".nova-close-btn")
      .addEventListener("click", togglePanel);
    panel.appendChild(header);

    // Messages container
    var messagesDiv = document.createElement("div");
    messagesDiv.className = "nova-messages";
    messagesDiv.id = "nova-messages";
    panel.appendChild(messagesDiv);

    // Input area
    var inputArea = document.createElement("div");
    inputArea.className = "nova-input-area";

    var textarea = document.createElement("textarea");
    textarea.className = "nova-input";
    textarea.id = "nova-input";
    textarea.placeholder = "Ask about recruitment marketing...";
    textarea.rows = 1;
    textarea.setAttribute("aria-label", "Chat message input");
    textarea.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });
    textarea.addEventListener("input", function () {
      // Auto-resize
      this.style.height = "auto";
      this.style.height = Math.min(this.scrollHeight, 100) + "px";
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
    panel.appendChild(inputArea);

    // Footer
    var footer = document.createElement("div");
    footer.className = "nova-footer";
    footer.textContent = "Powered by Nova \u00b7 Nova AI Suite";
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

    // Render existing messages or show welcome
    if (state.messages.length > 0) {
      renderAllMessages();
    } else {
      showWelcome();
    }
  }

  // ---------------------------------------------------------------------------
  // Panel toggle
  // ---------------------------------------------------------------------------
  function togglePanel() {
    state.isOpen = !state.isOpen;
    var panel = state.chatPanel;
    var btn = state.floatingBtn;
    var orbCanvas = state.orbCanvas;
    if (state.isOpen) {
      panel.classList.remove("nova-hidden");
      panel.classList.add("nova-visible");
      // Hide orb, show close icon
      if (orbCanvas) orbCanvas.style.display = "none";
      btn.classList.add("nova-btn-close");
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
      // Focus input
      setTimeout(function () {
        var input = document.getElementById("nova-input");
        if (input) input.focus();
      }, 300);
    } else {
      panel.classList.remove("nova-visible");
      panel.classList.add("nova-hidden");
      // Remove close icon, restore orb
      var closeIcon = document.getElementById("nova-close-icon");
      if (closeIcon) closeIcon.remove();
      btn.classList.remove("nova-btn-close");
      if (orbCanvas) orbCanvas.style.display = "block";
      btn.title = "Open Nova Chat";
      btn.setAttribute("aria-label", "Open Nova Chat");
    }
  }

  // ---------------------------------------------------------------------------
  // Welcome + suggestions
  // ---------------------------------------------------------------------------
  function showWelcome() {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;

    // Welcome message
    var welcomeMsg = {
      role: "assistant",
      content:
        "Hello! I'm **Nova**, your recruitment marketing intelligence assistant. " +
        "I have access to data from **10,238+ Supply Partners**, job boards across **70+ countries**, " +
        "and comprehensive industry benchmarks and salary data.\n\nHow can I help you today?",
      sources: [],
      confidence: 1.0,
    };
    appendMessage(welcomeMsg, false);

    // Suggestions
    var sugDiv = document.createElement("div");
    sugDiv.className = "nova-suggestions";
    sugDiv.id = "nova-suggestions";

    var title = document.createElement("div");
    title.className = "nova-suggestions-title";
    title.textContent = "Suggested questions";
    sugDiv.appendChild(title);

    SUGGESTED_QUESTIONS.forEach(function (q) {
      var btn = document.createElement("button");
      btn.className = "nova-suggestion-btn";
      btn.textContent = q;
      btn.addEventListener("click", function () {
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

    // Remove suggestions on first user message
    var sugEl = document.getElementById("nova-suggestions");
    if (sugEl && msg.role === "user") {
      sugEl.remove();
    }

    var msgEl = document.createElement("div");
    msgEl.className = "nova-msg nova-msg-" + msg.role;

    if (msg.role === "assistant") {
      msgEl.innerHTML = renderMarkdown(msg.content);

      // Meta: sources + confidence
      var sources = msg.sources || [];
      var confidence = msg.confidence;
      if (
        sources.length > 0 ||
        (typeof confidence === "number" && confidence > 0)
      ) {
        var metaDiv = document.createElement("div");
        metaDiv.className = "nova-msg-meta";

        sources.forEach(function (src) {
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

          // Use structured breakdown if available
          var bd = msg.confidence_breakdown;
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

            // Build tooltip
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
              '<div class="nova-tooltip-note">Confidence is a quality signal, not a filter. ' +
              "Lower scores widen estimate ranges but do not suppress data.</div>";
            confBadge.appendChild(tooltip);
          } else {
            confBadge.textContent = pct + "% confidence";
          }
          metaDiv.appendChild(confBadge);
        }

        msgEl.appendChild(metaDiv);
      }
    } else {
      msgEl.textContent = msg.content;
    }

    messagesDiv.appendChild(msgEl);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;

    if (persist !== false) {
      state.messages.push(msg);
      saveHistory(state.messages);
    }
  }

  function renderAllMessages() {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;
    messagesDiv.innerHTML = "";

    state.messages.forEach(function (msg) {
      appendMessage(msg, false);
    });
  }

  function showTyping() {
    var messagesDiv = document.getElementById("nova-messages");
    if (!messagesDiv) return;

    var typing = document.createElement("div");
    typing.className = "nova-typing";
    typing.id = "nova-typing";
    typing.innerHTML =
      "" +
      '<div class="nova-typing-dot"></div>' +
      '<div class="nova-typing-dot"></div>' +
      '<div class="nova-typing-dot"></div>';
    messagesDiv.appendChild(typing);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
  }

  function hideTyping() {
    var el = document.getElementById("nova-typing");
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
  // Send message
  // ---------------------------------------------------------------------------
  function sendMessage() {
    if (state.isLoading) return;

    var input = document.getElementById("nova-input");
    if (!input) return;

    var text = input.value.trim();
    if (!text) return;

    // Add user message
    appendMessage({ role: "user", content: text });
    input.value = "";
    input.style.height = "auto";

    // Disable send button
    var sendBtn = document.getElementById("nova-send-btn");
    if (sendBtn) sendBtn.disabled = true;
    state.isLoading = true;

    showTyping();

    // Build history for API
    var history = [];
    state.messages.forEach(function (m) {
      if (m.role === "user" || m.role === "assistant") {
        history.push({ role: m.role, content: m.content });
      }
    });

    // API call
    var payload = {
      message: text,
      conversation_id: state.sessionId,
      history: history.slice(-20),
    };
    // Include session context if set via setContext() public API
    if (CONFIG._sessionContext && typeof CONFIG._sessionContext === "object") {
      payload.context = CONFIG._sessionContext;
    }

    // AbortController with 60-second timeout for chat requests
    var abortCtrl = new AbortController();
    var fetchTimeout = setTimeout(function () {
      abortCtrl.abort();
    }, 60000);

    fetch(CONFIG.apiUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
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
        // Note: v2 orchestrator metadata (data_confidence, data_freshness,
        // sources_used) is available in the response payload but not yet
        // displayed in the UI. Future enhancement: show freshness badges
        // and per-source confidence breakdowns.
        appendMessage({
          role: "assistant",
          content: data.response || "No response received.",
          sources: data.sources || [],
          confidence: data.confidence || 0,
          confidence_breakdown: data.confidence_breakdown || null,
        });
      })
      .catch(function (err) {
        clearTimeout(fetchTimeout);
        hideTyping();
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
        console.error("Nova chat error:", err);
      })
      .finally(function () {
        state.isLoading = false;
        if (sendBtn) sendBtn.disabled = false;
        if (input) input.focus();
      });
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

      // Wait for DOM ready
      if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
          buildWidget(options.containerId);
        });
      } else {
        buildWidget(options.containerId);
      }

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
      if (state.isOpen && state.chatPanel) togglePanel();
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
     * Clear conversation history.
     */
    clearHistory: function () {
      state.messages = [];
      saveHistory([]);
      var messagesDiv = document.getElementById("nova-messages");
      if (messagesDiv) {
        messagesDiv.innerHTML = "";
        showWelcome();
      }
    },
  };
})();
