/**
 * ApplyFlow by Nova AI Suite -- Conversational Apply Widget
 *
 * Self-contained, embeddable candidate-facing chat widget that replaces
 * traditional job application forms with an AI-powered conversation.
 *
 * Usage:
 *   <script src="/static/applyflow-widget.js"></script>
 *   <script>
 *     ApplyFlow.init({
 *       role: "Software Engineer",
 *       company: "Acme Corp",
 *       location: "San Francisco, CA",
 *       requirements: ["3+ years Python", "REST APIs"],
 *       screeningQuestions: ["Do you have a CS degree?"],
 *       theme: "dark"   // "dark" (default) or "light"
 *     });
 *   </script>
 */
(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------
  var CONFIG = {
    apiUrl: "/api/applyflow/chat",
    primaryColor: "#6366F1",
    primaryGradient:
      "linear-gradient(135deg, #4F46E5 0%, #6366F1 50%, #818CF8 100%)",
    bgColor: "#000000",
    bgPanel: "rgba(15,23,42,0.94)",
    bgMessage: "rgba(20,30,50,0.6)",
    bgUserMessage: "rgba(99,102,241,0.15)",
    textColor: "#E2E8F0",
    textLight: "rgba(226,232,240,0.7)",
    textDim: "rgba(226,232,240,0.4)",
    borderColor: "rgba(255,255,255,0.06)",
    borderAccent: "rgba(99,102,241,0.2)",
    successColor: "#34D399",
    widgetWidth: "420px",
    widgetHeight: "600px",
    mobileBreakpoint: 640,
    storageKeyPrefix: "applyflow_",
    maxChips: 5,
  };

  var STAGE_ICONS = {
    greeting: "\u{1F44B}",
    qualification: "\u{1F4CB}",
    experience: "\u{1F4BC}",
    skills: "\u{2699}\uFE0F",
    contact: "\u{1F4E7}",
    confirmation: "\u{2705}",
    complete: "\u{1F389}",
  };

  var STAGE_LABELS = {
    greeting: "Welcome",
    qualification: "Screening",
    experience: "Experience",
    skills: "Skills",
    contact: "Contact",
    confirmation: "Review",
    complete: "Done",
  };

  var STAGES_ORDERED = [
    "greeting",
    "qualification",
    "experience",
    "skills",
    "contact",
    "confirmation",
    "complete",
  ];

  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  var state = {
    isOpen: false,
    isLoading: false,
    messages: [],
    sessionId: null,
    jobConfig: null,
    currentStage: "greeting",
    progressPct: 0,
    chips: [],
    isComplete: false,
    initialized: false,
    container: null,
    panel: null,
    floatingBtn: null,
    userConfig: {},
  };

  // ---------------------------------------------------------------------------
  // Styles
  // ---------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById("applyflow-styles")) return;

    var css =
      "" +
      // ── Floating Button ──
      "#applyflow-btn {" +
      "  position: fixed; bottom: 24px; right: 24px; z-index: 99999;" +
      "  width: 68px; height: 68px; border-radius: 50%;" +
      "  background: " +
      CONFIG.primaryGradient +
      ";" +
      "  color: #fff; border: none; cursor: pointer;" +
      "  box-shadow: 0 4px 24px rgba(99,102,241,0.4), 0 0 0 0 rgba(99,102,241,0.3);" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  transition: transform 0.3s cubic-bezier(0.34,1.56,0.64,1), box-shadow 0.3s ease;" +
      "  font-size: 0; padding: 0; overflow: hidden;" +
      "  animation: applyflow-pulse 2.5s ease-in-out infinite;" +
      "}" +
      "@keyframes applyflow-pulse {" +
      "  0%, 100% { box-shadow: 0 4px 24px rgba(99,102,241,0.4), 0 0 0 0 rgba(99,102,241,0.3); }" +
      "  50% { box-shadow: 0 4px 24px rgba(99,102,241,0.4), 0 0 0 10px rgba(99,102,241,0); }" +
      "}" +
      "#applyflow-btn:hover {" +
      "  transform: translateY(-3px) scale(1.1);" +
      "  box-shadow: 0 8px 36px rgba(99,102,241,0.5), 0 0 40px rgba(99,102,241,0.15);" +
      "  animation: none;" +
      "}" +
      "#applyflow-btn:active { transform: scale(0.95); }" +
      "#applyflow-btn svg { width: 26px; height: 26px; }" +
      "#applyflow-btn .applyflow-btn-label {" +
      "  font-size: 11px; font-weight: 700; letter-spacing: 0.5px;" +
      "  position: absolute; bottom: -22px; left: 50%; transform: translateX(-50%);" +
      "  white-space: nowrap; color: #6366F1; text-shadow: 0 0 8px rgba(99,102,241,0.3);" +
      "  opacity: 0; transition: opacity 0.3s;" +
      "}" +
      "#applyflow-btn:hover .applyflow-btn-label { opacity: 1; }" +
      "#applyflow-btn.applyflow-btn-close {" +
      "  background: linear-gradient(135deg, #1E1B4B 0%, #6366F1 100%);" +
      "  animation: none;" +
      "}" +
      // ── Chat Panel ──
      "#applyflow-panel {" +
      "  position: fixed; bottom: 96px; right: 24px; z-index: 99998;" +
      "  width: " +
      CONFIG.widgetWidth +
      "; height: " +
      CONFIG.widgetHeight +
      ";" +
      "  max-height: calc(100vh - 120px);" +
      "  background: " +
      CONFIG.bgPanel +
      ";" +
      "  backdrop-filter: blur(24px) saturate(1.4);" +
      "  -webkit-backdrop-filter: blur(24px) saturate(1.4);" +
      "  border-radius: 20px;" +
      "  box-shadow: 0 20px 80px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.06) inset;" +
      "  display: flex; flex-direction: column; overflow: hidden;" +
      "  transition: opacity 0.35s cubic-bezier(0.4,0,0.2,1), transform 0.35s cubic-bezier(0.4,0,0.2,1);" +
      '  font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;' +
      "  border: 1px solid rgba(255,255,255,0.06);" +
      "}" +
      "#applyflow-panel.af-hidden {" +
      "  opacity: 0; transform: translateY(20px) scale(0.92); pointer-events: none;" +
      "}" +
      "#applyflow-panel.af-visible {" +
      "  opacity: 1; transform: translateY(0) scale(1); pointer-events: auto;" +
      "}" +
      // ── Header ──
      ".af-header {" +
      "  background: linear-gradient(135deg, rgba(8,14,28,0.98) 0%, rgba(30,27,75,0.95) 100%);" +
      "  padding: 16px 20px 12px; flex-shrink: 0; position: relative;" +
      "  border-bottom: 1px solid rgba(99,102,241,0.1);" +
      "}" +
      ".af-header-top {" +
      "  display: flex; align-items: center; justify-content: space-between;" +
      "}" +
      ".af-header-info { display: flex; align-items: center; gap: 10px; }" +
      ".af-header-icon {" +
      "  width: 36px; height: 36px; border-radius: 10px;" +
      "  background: rgba(99,102,241,0.12);" +
      "  border: 1px solid rgba(99,102,241,0.2);" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  font-size: 16px;" +
      "}" +
      ".af-header-text { }" +
      ".af-header-role {" +
      "  font-size: 14px; font-weight: 700; color: #E2E8F0;" +
      "  line-height: 1.2; max-width: 260px; overflow: hidden;" +
      "  text-overflow: ellipsis; white-space: nowrap;" +
      "}" +
      ".af-header-company {" +
      "  font-size: 11px; color: rgba(226,232,240,0.5); margin-top: 2px;" +
      "}" +
      ".af-close-btn {" +
      "  background: rgba(255,255,255,0.05); border: 1px solid rgba(255,255,255,0.1);" +
      "  color: rgba(255,255,255,0.6); cursor: pointer;" +
      "  padding: 5px; border-radius: 8px; line-height: 1;" +
      "  transition: all 0.2s;" +
      "}" +
      ".af-close-btn:hover { background: rgba(255,255,255,0.1); color: #fff; }" +
      ".af-close-btn svg { width: 16px; height: 16px; }" +
      // ── Progress Bar ──
      ".af-progress-wrap {" +
      "  margin-top: 12px; display: flex; align-items: center; gap: 4px;" +
      "}" +
      ".af-progress-step {" +
      "  flex: 1; height: 3px; border-radius: 2px;" +
      "  background: rgba(255,255,255,0.06);" +
      "  transition: background 0.5s ease, box-shadow 0.5s ease;" +
      "  position: relative;" +
      "}" +
      ".af-progress-step.af-step-done {" +
      "  background: #6366F1;" +
      "  box-shadow: 0 0 8px rgba(99,102,241,0.3);" +
      "}" +
      ".af-progress-step.af-step-current {" +
      "  background: linear-gradient(90deg, #6366F1 0%, rgba(99,102,241,0.3) 100%);" +
      "  box-shadow: 0 0 6px rgba(99,102,241,0.2);" +
      "}" +
      ".af-progress-label {" +
      "  font-size: 9px; color: rgba(226,232,240,0.4); letter-spacing: 1px;" +
      "  text-transform: uppercase; margin-top: 6px; text-align: center;" +
      "}" +
      ".af-powered {" +
      "  font-size: 8px; color: rgba(226,232,240,0.25); letter-spacing: 0.5px;" +
      "  text-align: right; margin-top: 4px;" +
      "}" +
      // ── Messages ──
      ".af-messages {" +
      "  flex: 1; overflow-y: auto; padding: 16px;" +
      "  display: flex; flex-direction: column; gap: 12px;" +
      "}" +
      ".af-messages::-webkit-scrollbar { width: 3px; }" +
      ".af-messages::-webkit-scrollbar-track { background: transparent; }" +
      ".af-messages::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 2px; }" +
      // ── Message Bubbles ──
      ".af-msg {" +
      "  max-width: 85%; padding: 12px 16px; border-radius: 16px;" +
      "  font-size: 13px; line-height: 1.65; word-wrap: break-word;" +
      "  animation: af-msgIn 0.4s cubic-bezier(0.16,1,0.3,1);" +
      "}" +
      "@keyframes af-msgIn {" +
      "  from { opacity: 0; transform: translateY(12px) scale(0.97); }" +
      "  to { opacity: 1; transform: translateY(0) scale(1); }" +
      "}" +
      ".af-msg-user {" +
      "  align-self: flex-end;" +
      "  background: " +
      CONFIG.bgUserMessage +
      ";" +
      "  color: #e0f0ff;" +
      "  border: 1px solid rgba(99,102,241,0.15);" +
      "  border-bottom-right-radius: 4px;" +
      "}" +
      ".af-msg-assistant {" +
      "  align-self: flex-start;" +
      "  background: " +
      CONFIG.bgMessage +
      ";" +
      "  color: #c8d6e5;" +
      "  border: 1px solid rgba(255,255,255,0.04);" +
      "  border-bottom-left-radius: 4px;" +
      "}" +
      // Markdown in assistant messages
      ".af-msg-assistant strong { font-weight: 600; color: #e0f0ff; }" +
      ".af-msg-assistant em { font-style: italic; color: #8899aa; }" +
      ".af-msg-assistant p { margin: 4px 0; }" +
      ".af-msg-assistant ul, .af-msg-assistant ol { margin: 4px 0; padding-left: 18px; }" +
      ".af-msg-assistant li { margin-bottom: 3px; }" +
      ".af-msg-assistant li::marker { color: rgba(99,102,241,0.5); }" +
      // ── Typing Indicator ──
      ".af-typing {" +
      "  align-self: flex-start; display: flex; align-items: center; gap: 5px;" +
      "  padding: 12px 16px;" +
      "  background: " +
      CONFIG.bgMessage +
      ";" +
      "  border: 1px solid rgba(255,255,255,0.04);" +
      "  border-radius: 16px; border-bottom-left-radius: 4px;" +
      "  animation: af-msgIn 0.3s ease;" +
      "}" +
      ".af-typing-dot {" +
      "  width: 6px; height: 6px; border-radius: 50%;" +
      "  background: #6366F1;" +
      "  animation: af-bounce 1.4s infinite;" +
      "}" +
      ".af-typing-dot:nth-child(2) { animation-delay: 0.2s; }" +
      ".af-typing-dot:nth-child(3) { animation-delay: 0.4s; }" +
      "@keyframes af-bounce {" +
      "  0%,60%,100% { transform: translateY(0); opacity: 0.3; }" +
      "  30% { transform: translateY(-8px); opacity: 1; }" +
      "}" +
      // ── Quick Reply Chips ──
      ".af-chips {" +
      "  padding: 8px 16px 4px; display: flex; flex-wrap: wrap; gap: 6px;" +
      "  animation: af-msgIn 0.4s ease 0.1s both;" +
      "}" +
      ".af-chip {" +
      "  background: rgba(99,102,241,0.08);" +
      "  border: 1px solid rgba(99,102,241,0.15);" +
      "  padding: 7px 14px; border-radius: 20px; cursor: pointer;" +
      "  font-size: 12px; color: #818CF8; font-family: inherit;" +
      "  transition: all 0.2s; white-space: nowrap;" +
      "}" +
      ".af-chip:hover {" +
      "  background: rgba(99,102,241,0.15);" +
      "  border-color: rgba(99,102,241,0.3);" +
      "  color: #A5B4FC;" +
      "  transform: translateY(-1px);" +
      "}" +
      ".af-chip:active { transform: scale(0.96); }" +
      // ── Input Area ──
      ".af-input-area {" +
      "  padding: 12px 16px; border-top: 1px solid rgba(255,255,255,0.06);" +
      "  display: flex; gap: 10px; align-items: flex-end;" +
      "  background: rgba(8,14,28,0.7); flex-shrink: 0;" +
      "}" +
      ".af-input {" +
      "  flex: 1; border: 1px solid rgba(255,255,255,0.08);" +
      "  border-radius: 12px; padding: 10px 14px;" +
      "  font-size: 13px; font-family: inherit;" +
      "  resize: none; outline: none; min-height: 20px; max-height: 80px;" +
      "  line-height: 1.5; background: rgba(255,255,255,0.03);" +
      "  color: #c8d6e5;" +
      "  transition: border-color 0.2s, background 0.2s, box-shadow 0.2s;" +
      "}" +
      ".af-input:focus {" +
      "  border-color: rgba(99,102,241,0.4); background: rgba(99,102,241,0.04);" +
      "  box-shadow: 0 0 0 3px rgba(99,102,241,0.06);" +
      "}" +
      ".af-input::placeholder { color: rgba(130,160,190,0.5); }" +
      ".af-input:disabled { opacity: 0.4; cursor: not-allowed; }" +
      ".af-send-btn {" +
      "  width: 40px; height: 40px; border-radius: 12px;" +
      "  background: #6366F1; color: #fff; border: none; cursor: pointer;" +
      "  display: flex; align-items: center; justify-content: center;" +
      "  flex-shrink: 0; transition: all 0.25s cubic-bezier(0.4,0,0.2,1);" +
      "  box-shadow: 0 2px 12px rgba(0,0,0,0.2);" +
      "}" +
      ".af-send-btn:hover { transform: scale(1.08); box-shadow: 0 4px 20px rgba(0,0,0,0.3); }" +
      ".af-send-btn:active { transform: scale(0.95); }" +
      ".af-send-btn:disabled { opacity: 0.25; cursor: not-allowed; transform: none; box-shadow: none; }" +
      ".af-send-btn svg { width: 18px; height: 18px; }" +
      // ── Completion Banner ──
      ".af-complete-banner {" +
      "  padding: 20px; text-align: center;" +
      "  background: linear-gradient(135deg, rgba(52,211,153,0.08) 0%, rgba(99,102,241,0.08) 100%);" +
      "  border-top: 1px solid rgba(52,211,153,0.15);" +
      "  animation: af-msgIn 0.5s ease;" +
      "}" +
      ".af-complete-icon { font-size: 32px; margin-bottom: 8px; }" +
      ".af-complete-text { font-size: 13px; color: #34D399; font-weight: 600; }" +
      // ── Mobile ──
      "@media (max-width: " +
      CONFIG.mobileBreakpoint +
      "px) {" +
      "  #applyflow-panel {" +
      "    width: 100vw; height: 100vh; max-height: 100vh;" +
      "    bottom: 0; right: 0; border-radius: 0;" +
      "  }" +
      "  #applyflow-btn { bottom: 16px; right: 16px; width: 60px; height: 60px; }" +
      "}" +
      // ── Reduced Motion ──
      "@media (prefers-reduced-motion: reduce) {" +
      "  .af-msg, .af-typing, .af-chips { animation: none !important; }" +
      "  .af-typing-dot { animation: none !important; opacity: 0.5; }" +
      "  #applyflow-btn { animation: none !important; transition: none !important; }" +
      "  .af-send-btn { transition: none !important; }" +
      "  .af-progress-step { transition: none !important; }" +
      "}";

    var styleEl = document.createElement("style");
    styleEl.id = "applyflow-styles";
    styleEl.textContent = css;
    document.head.appendChild(styleEl);
  }

  // ---------------------------------------------------------------------------
  // SVG Icons
  // ---------------------------------------------------------------------------
  var ICONS = {
    briefcase:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<rect x="2" y="7" width="20" height="14" rx="2" ry="2"/>' +
      '<path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>' +
      "</svg>",
    close:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
      '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>' +
      "</svg>",
    send:
      '<svg viewBox="0 0 24 24" fill="currentColor">' +
      '<path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/>' +
      "</svg>",
    check:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">' +
      '<polyline points="20 6 9 17 4 12"/>' +
      "</svg>",
  };

  // ---------------------------------------------------------------------------
  // Markdown renderer (lightweight)
  // ---------------------------------------------------------------------------
  function escapeHtml(text) {
    var div = document.createElement("div");
    div.appendChild(document.createTextNode(text));
    return div.innerHTML;
  }

  function renderMarkdown(text) {
    if (!text) return "";
    var html = escapeHtml(text);
    // Bold: **text**
    html = html.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
    // Italic: *text*
    html = html.replace(/\*(.+?)\*/g, "<em>$1</em>");
    // Unordered list items: - text
    html = html.replace(/^- (.+)$/gm, "<li>$1</li>");
    html = html.replace(/(<li>.*<\/li>\n?)+/g, "<ul>$&</ul>");
    // Paragraphs: double newlines
    html = html.replace(/\n\n/g, "</p><p>");
    // Single newlines
    html = html.replace(/\n/g, "<br>");
    // Wrap in paragraph
    if (!html.startsWith("<")) html = "<p>" + html + "</p>";
    return html;
  }

  // ---------------------------------------------------------------------------
  // DOM Construction
  // ---------------------------------------------------------------------------
  function buildWidget() {
    // ── Floating Button ──
    var btn = document.createElement("button");
    btn.id = "applyflow-btn";
    btn.setAttribute("aria-label", "Apply Now - Open application chat");
    btn.setAttribute("role", "button");
    btn.setAttribute("tabindex", "0");
    btn.innerHTML =
      ICONS.briefcase + '<span class="applyflow-btn-label">Apply Now</span>';
    btn.addEventListener("click", togglePanel);
    document.body.appendChild(btn);
    state.floatingBtn = btn;

    // ── Chat Panel ──
    var panel = document.createElement("div");
    panel.id = "applyflow-panel";
    panel.className = "af-hidden";
    panel.setAttribute("role", "dialog");
    panel.setAttribute("aria-label", "Job Application Chat");
    panel.setAttribute("aria-modal", "true");

    var role = state.userConfig.role || "Position";
    var company = state.userConfig.company || "Company";

    panel.innerHTML =
      "" +
      // Header
      '<div class="af-header">' +
      '  <div class="af-header-top">' +
      '    <div class="af-header-info">' +
      '      <div class="af-header-icon">' +
      ICONS.briefcase +
      "</div>" +
      '      <div class="af-header-text">' +
      '        <div class="af-header-role" id="af-role" title="' +
      escapeHtml(role) +
      '">' +
      escapeHtml(role) +
      "</div>" +
      '        <div class="af-header-company" id="af-company">' +
      escapeHtml(company) +
      "</div>" +
      "      </div>" +
      "    </div>" +
      '    <button class="af-close-btn" aria-label="Close application chat" tabindex="0">' +
      ICONS.close +
      "</button>" +
      "  </div>" +
      '  <div class="af-progress-wrap" id="af-progress"></div>' +
      '  <div class="af-progress-label" id="af-stage-label">Welcome</div>' +
      '  <div class="af-powered">Created by Shubham Singh Chandel</div>' +
      "</div>" +
      // Messages
      '<div class="af-messages" id="af-messages" role="log" aria-live="polite" aria-label="Chat messages"></div>' +
      // Chips
      '<div class="af-chips" id="af-chips" role="group" aria-label="Quick reply options"></div>' +
      // Input
      '<div class="af-input-area">' +
      '  <textarea class="af-input" id="af-input" placeholder="Type your response..." ' +
      '    rows="1" aria-label="Your message" tabindex="0"></textarea>' +
      '  <button class="af-send-btn" id="af-send" aria-label="Send message" tabindex="0">' +
      ICONS.send +
      "</button>" +
      "</div>";

    document.body.appendChild(panel);
    state.panel = panel;

    // ── Event Listeners ──
    panel.querySelector(".af-close-btn").addEventListener("click", togglePanel);
    document.getElementById("af-send").addEventListener("click", sendMessage);
    var input = document.getElementById("af-input");
    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });
    // Auto-resize textarea
    input.addEventListener("input", function () {
      this.style.height = "auto";
      this.style.height = Math.min(this.scrollHeight, 80) + "px";
    });

    // Build progress bar
    updateProgressBar();

    // Keyboard: Escape to close
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && state.isOpen) {
        togglePanel();
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Progress Bar
  // ---------------------------------------------------------------------------
  function updateProgressBar() {
    var container = document.getElementById("af-progress");
    if (!container) return;

    var currentIdx = STAGES_ORDERED.indexOf(state.currentStage);
    if (currentIdx === -1) currentIdx = 0;

    var html = "";
    for (var i = 0; i < STAGES_ORDERED.length; i++) {
      var cls = "af-progress-step";
      if (i < currentIdx) cls += " af-step-done";
      else if (i === currentIdx) cls += " af-step-current";
      html +=
        '<div class="' +
        cls +
        '" title="' +
        STAGE_LABELS[STAGES_ORDERED[i]] +
        '"></div>';
    }
    container.innerHTML = html;

    // Update label
    var label = document.getElementById("af-stage-label");
    if (label) {
      label.textContent =
        STAGE_LABELS[state.currentStage] || state.currentStage;
    }
  }

  // ---------------------------------------------------------------------------
  // Message Rendering
  // ---------------------------------------------------------------------------
  function addMessage(role, content) {
    state.messages.push({ role: role, content: content, time: Date.now() });
    saveSession();

    var container = document.getElementById("af-messages");
    if (!container) return;

    var div = document.createElement("div");
    div.className = "af-msg af-msg-" + role;

    if (role === "assistant") {
      div.innerHTML = renderMarkdown(content);
    } else {
      div.textContent = content;
    }

    container.appendChild(div);
    scrollToBottom();
  }

  function showTyping() {
    var container = document.getElementById("af-messages");
    if (!container) return;

    // Remove existing
    var existing = container.querySelector(".af-typing");
    if (existing) existing.remove();

    var typing = document.createElement("div");
    typing.className = "af-typing";
    typing.setAttribute("aria-label", "Assistant is typing");
    typing.innerHTML =
      '<div class="af-typing-dot"></div><div class="af-typing-dot"></div><div class="af-typing-dot"></div>';
    container.appendChild(typing);
    scrollToBottom();
  }

  function hideTyping() {
    var container = document.getElementById("af-messages");
    if (!container) return;
    var typing = container.querySelector(".af-typing");
    if (typing) typing.remove();
  }

  function updateChips(chips) {
    var container = document.getElementById("af-chips");
    if (!container) return;

    state.chips = chips || [];
    container.innerHTML = "";

    if (!chips || chips.length === 0) return;

    var displayed = chips.slice(0, CONFIG.maxChips);
    for (var i = 0; i < displayed.length; i++) {
      var btn = document.createElement("button");
      btn.className = "af-chip";
      btn.textContent = displayed[i];
      btn.setAttribute("tabindex", "0");
      btn.setAttribute("aria-label", "Quick reply: " + displayed[i]);
      (function (text) {
        btn.addEventListener("click", function () {
          var input = document.getElementById("af-input");
          if (input) input.value = text;
          sendMessage();
        });
      })(displayed[i]);
      container.appendChild(btn);
    }
  }

  function scrollToBottom() {
    var container = document.getElementById("af-messages");
    if (container) {
      setTimeout(function () {
        container.scrollTop = container.scrollHeight;
      }, 50);
    }
  }

  function setInputEnabled(enabled) {
    var input = document.getElementById("af-input");
    var send = document.getElementById("af-send");
    if (input) input.disabled = !enabled;
    if (send) send.disabled = !enabled;
    state.isLoading = !enabled;
  }

  // ---------------------------------------------------------------------------
  // Panel Toggle
  // ---------------------------------------------------------------------------
  function togglePanel() {
    state.isOpen = !state.isOpen;
    var panel = state.panel;
    var btn = state.floatingBtn;

    if (state.isOpen) {
      panel.className = panel.className.replace("af-hidden", "").trim();
      // Force reflow before adding visible class
      void panel.offsetWidth;
      panel.className += " af-visible";
      btn.className += " applyflow-btn-close";
      btn.innerHTML = ICONS.close;
      btn.setAttribute("aria-label", "Close application chat");

      // Initialize session if not already done
      if (!state.sessionId) {
        initSession();
      }

      // Focus input
      setTimeout(function () {
        var input = document.getElementById("af-input");
        if (input && !state.isComplete) input.focus();
      }, 400);
    } else {
      panel.className = panel.className.replace("af-visible", "").trim();
      panel.className += " af-hidden";
      btn.className = btn.className.replace("applyflow-btn-close", "").trim();
      btn.innerHTML =
        ICONS.briefcase + '<span class="applyflow-btn-label">Apply Now</span>';
      btn.setAttribute("aria-label", "Apply Now - Open application chat");
    }
  }

  // ---------------------------------------------------------------------------
  // Session Management
  // ---------------------------------------------------------------------------
  function getStorageKey() {
    var jobId = state.userConfig.jobId || state.userConfig.role || "default";
    return CONFIG.storageKeyPrefix + jobId.replace(/\s+/g, "_").toLowerCase();
  }

  function saveSession() {
    try {
      var data = {
        sessionId: state.sessionId,
        messages: state.messages.slice(-50),
        currentStage: state.currentStage,
        progressPct: state.progressPct,
        isComplete: state.isComplete,
        savedAt: Date.now(),
      };
      localStorage.setItem(getStorageKey(), JSON.stringify(data));
    } catch (e) {
      // localStorage unavailable or full -- ignore
    }
  }

  function loadSession() {
    try {
      var raw = localStorage.getItem(getStorageKey());
      if (!raw) return false;
      var data = JSON.parse(raw);
      // Check if session is less than 1 hour old
      if (data.savedAt && Date.now() - data.savedAt > 3600000) {
        localStorage.removeItem(getStorageKey());
        return false;
      }
      if (data.sessionId) {
        state.sessionId = data.sessionId;
        state.currentStage = data.currentStage || "greeting";
        state.progressPct = data.progressPct || 0;
        state.isComplete = data.isComplete || false;

        // Restore messages
        var container = document.getElementById("af-messages");
        if (container && data.messages) {
          state.messages = data.messages;
          for (var i = 0; i < data.messages.length; i++) {
            var msg = data.messages[i];
            var div = document.createElement("div");
            div.className = "af-msg af-msg-" + msg.role;
            if (msg.role === "assistant") {
              div.innerHTML = renderMarkdown(msg.content);
            } else {
              div.textContent = msg.content;
            }
            // No animation for restored messages
            div.style.animation = "none";
            container.appendChild(div);
          }
          scrollToBottom();
        }
        updateProgressBar();

        if (state.isComplete) {
          setInputEnabled(false);
        }
        return true;
      }
    } catch (e) {
      // Corrupted data -- ignore
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  // API Communication
  // ---------------------------------------------------------------------------
  function buildJobConfig() {
    var cfg = {
      role: state.userConfig.role || "Open Position",
      company: state.userConfig.company || "Company",
      location: state.userConfig.location || "",
      industry: state.userConfig.industry || "",
    };
    if (state.userConfig.requirements) {
      cfg.requirements = state.userConfig.requirements;
    }
    if (state.userConfig.screeningQuestions) {
      cfg.screening_questions = state.userConfig.screeningQuestions;
    }
    if (state.userConfig.jobId) {
      cfg.job_id = state.userConfig.jobId;
    }
    return cfg;
  }

  function initSession() {
    // Try to restore from localStorage first
    if (loadSession()) return;

    setInputEnabled(false);
    showTyping();

    var payload = JSON.stringify({
      action: "init",
      job_config: buildJobConfig(),
    });

    apiCall(
      payload,
      function (data) {
        hideTyping();
        setInputEnabled(true);

        if (data.session_id) {
          state.sessionId = data.session_id;
        }
        if (data.response) {
          state.currentStage = data.stage || "greeting";
          addMessage("assistant", data.response);
          updateChips(data.chips || []);
          updateProgressBar();
        }
      },
      function (err) {
        hideTyping();
        setInputEnabled(true);
        addMessage(
          "assistant",
          "Welcome! I am here to help you apply. Please tell me a bit about yourself to get started.",
        );
      },
    );
  }

  function sendMessage() {
    var input = document.getElementById("af-input");
    if (!input) return;
    var text = input.value.trim();
    if (!text || state.isLoading || state.isComplete) return;

    // Add user message
    addMessage("user", text);
    input.value = "";
    input.style.height = "auto";
    updateChips([]); // Clear chips

    setInputEnabled(false);
    showTyping();

    var payload = JSON.stringify({
      action: "chat",
      session_id: state.sessionId,
      message: text,
      job_config: buildJobConfig(),
    });

    apiCall(
      payload,
      function (data) {
        hideTyping();

        if (data.session_id) {
          state.sessionId = data.session_id;
        }

        if (data.stage) {
          state.currentStage = data.stage;
        }
        if (typeof data.progress_pct === "number") {
          state.progressPct = data.progress_pct;
        }

        updateProgressBar();

        if (data.response) {
          addMessage("assistant", data.response);
        }

        if (data.is_complete) {
          state.isComplete = true;
          setInputEnabled(false);
          showCompleteBanner();
          saveSession();
        } else {
          setInputEnabled(true);
          updateChips(data.chips || []);
          // Focus input
          var inputEl = document.getElementById("af-input");
          if (inputEl) inputEl.focus();
        }
      },
      function (err) {
        hideTyping();
        setInputEnabled(true);
        addMessage(
          "assistant",
          "I am sorry, there was a connection issue. Could you please try again?",
        );
      },
    );
  }

  function showCompleteBanner() {
    var messages = document.getElementById("af-messages");
    if (!messages) return;
    var banner = document.createElement("div");
    banner.className = "af-complete-banner";
    banner.innerHTML =
      '<div class="af-complete-icon">' +
      ICONS.check +
      "</div>" +
      '<div class="af-complete-text">Application Submitted Successfully</div>';
    messages.appendChild(banner);
    scrollToBottom();
  }

  function apiCall(payload, onSuccess, onError) {
    var xhr = new XMLHttpRequest();
    xhr.open("POST", CONFIG.apiUrl, true);
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.timeout = 30000;

    xhr.onload = function () {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          var data = JSON.parse(xhr.responseText);
          onSuccess(data);
        } catch (e) {
          onError("Invalid response");
        }
      } else {
        onError("HTTP " + xhr.status);
      }
    };

    xhr.onerror = function () {
      onError("Network error");
    };
    xhr.ontimeout = function () {
      onError("Request timed out");
    };

    xhr.send(payload);
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------
  window.ApplyFlow = {
    /**
     * Initialize the ApplyFlow widget.
     *
     * @param {Object} config
     * @param {string} config.role - Job role title
     * @param {string} config.company - Company name
     * @param {string} config.location - Job location
     * @param {string} [config.jobId] - Unique job identifier
     * @param {string[]} [config.requirements] - List of requirements
     * @param {string[]} [config.screeningQuestions] - Custom screening questions (max 5)
     * @param {string} [config.industry] - Industry key
     * @param {string} [config.apiUrl] - Override API endpoint
     * @param {boolean} [config.autoOpen] - Auto-open the widget
     */
    init: function (config) {
      if (state.initialized) {
        console.warn("ApplyFlow: already initialized");
        return;
      }

      config = config || {};
      state.userConfig = config;

      // Override API URL if provided
      if (config.apiUrl) {
        CONFIG.apiUrl = config.apiUrl;
      }

      // Inject styles and build widget
      injectStyles();
      buildWidget();
      state.initialized = true;

      // Auto-open if requested
      if (config.autoOpen) {
        setTimeout(function () {
          if (!state.isOpen) togglePanel();
        }, 500);
      }
    },

    /**
     * Open the widget programmatically.
     */
    open: function () {
      if (!state.isOpen && state.initialized) {
        togglePanel();
      }
    },

    /**
     * Close the widget programmatically.
     */
    close: function () {
      if (state.isOpen && state.initialized) {
        togglePanel();
      }
    },

    /**
     * Reset the session (start over).
     */
    reset: function () {
      try {
        localStorage.removeItem(getStorageKey());
      } catch (e) {
        /* ignore */
      }
      state.sessionId = null;
      state.messages = [];
      state.currentStage = "greeting";
      state.progressPct = 0;
      state.isComplete = false;
      state.chips = [];

      var messages = document.getElementById("af-messages");
      if (messages) messages.innerHTML = "";
      var chips = document.getElementById("af-chips");
      if (chips) chips.innerHTML = "";

      setInputEnabled(true);
      updateProgressBar();

      if (state.isOpen) {
        initSession();
      }
    },

    /**
     * Destroy the widget (remove from DOM).
     */
    destroy: function () {
      if (state.floatingBtn) state.floatingBtn.remove();
      if (state.panel) state.panel.remove();
      var styles = document.getElementById("applyflow-styles");
      if (styles) styles.remove();
      state.initialized = false;
      state.isOpen = false;
      state.sessionId = null;
    },
  };
})();
