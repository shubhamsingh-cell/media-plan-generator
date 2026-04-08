/**
 * Nova AI Embed Widget v1.0.0
 * Lightweight, self-contained JavaScript widget for ATS vendors
 * to embed Nova-powered media planning directly in their platforms.
 *
 * Usage:
 *   <script src="https://media-plan-generator.onrender.com/embed/nova.js"
 *           data-nova-key="YOUR_API_KEY"
 *           data-nova-theme="dark"
 *           data-nova-position="bottom-right"></script>
 *
 * Configuration (data attributes on the script tag):
 *   data-nova-key       API key for authentication (optional for public endpoints)
 *   data-nova-theme     "dark" (default) or "light"
 *   data-nova-position  "bottom-right" (default) or "bottom-left"
 *   data-nova-base-url  Override the Nova API base URL (auto-detected from script src)
 *
 * postMessage API:
 *   window.postMessage({ type: 'nova-embed-open' }, '*')    -- open widget
 *   window.postMessage({ type: 'nova-embed-close' }, '*')   -- close widget
 *   window.postMessage({ type: 'nova-embed-prefill', data: { jobTitle, budget, location } }, '*')
 *
 * Emits:
 *   { type: 'nova-embed-ready' }
 *   { type: 'nova-embed-plan-generated', data: { ... } }
 *   { type: 'nova-embed-error', error: '...' }
 */
(function () {
  "use strict";

  // ── Prevent double initialization ──────────────────────────────────────────
  if (window.__novaEmbedLoaded) return;
  window.__novaEmbedLoaded = true;

  // ── Brand constants ────────────────────────────────────────────────────────
  var PORT_GORE = "#202058";
  var BLUE_VIOLET = "#5A54BD";
  var DOWNY_TEAL = "#6BB3CD";
  var WHITE = "#FFFFFF";
  var LIGHT_BG = "#F8F9FB";
  var LIGHT_CARD = "#FFFFFF";
  var LIGHT_TEXT = "#1a1a2e";
  var LIGHT_TEXT_SEC = "#555";
  var LIGHT_BORDER = "#e0e0e6";
  var DARK_BG = "#12122a";
  var DARK_CARD = "#1a1a3e";
  var DARK_TEXT = "#e0e0e8";
  var DARK_TEXT_SEC = "#9a9ab0";
  var DARK_BORDER = "#2a2a50";

  // ── Read config from script tag ────────────────────────────────────────────
  var scriptTag =
    document.currentScript ||
    (function () {
      var scripts = document.getElementsByTagName("script");
      for (var i = scripts.length - 1; i >= 0; i--) {
        if (scripts[i].src && scripts[i].src.indexOf("nova-embed") !== -1) {
          return scripts[i];
        }
      }
      return null;
    })();

  var config = {
    apiKey: (scriptTag && scriptTag.getAttribute("data-nova-key")) || "",
    theme: (scriptTag && scriptTag.getAttribute("data-nova-theme")) || "dark",
    position:
      (scriptTag && scriptTag.getAttribute("data-nova-position")) ||
      "bottom-right",
    baseUrl: (scriptTag && scriptTag.getAttribute("data-nova-base-url")) || "",
  };

  // Auto-detect base URL from script src
  if (!config.baseUrl && scriptTag && scriptTag.src) {
    try {
      var url = new URL(scriptTag.src);
      config.baseUrl = url.origin;
    } catch (e) {
      config.baseUrl = "";
    }
  }

  // ── Theme helpers ──────────────────────────────────────────────────────────
  function isDark() {
    return config.theme === "dark";
  }

  function themeColor(darkVal, lightVal) {
    return isDark() ? darkVal : lightVal;
  }

  // ── Embed stats tracker ────────────────────────────────────────────────────
  var _statsReported = false;
  function reportLoad() {
    if (_statsReported || !config.baseUrl) return;
    _statsReported = true;
    try {
      var img = new Image();
      img.src =
        config.baseUrl +
        "/api/embed/stats?event=load&host=" +
        encodeURIComponent(window.location.hostname) +
        "&t=" +
        Date.now();
    } catch (e) {
      /* silent */
    }
  }

  // ── Create shadow container for style isolation ────────────────────────────
  var hostEl = document.createElement("div");
  hostEl.id = "nova-embed-host";
  hostEl.style.cssText =
    "all:initial;position:fixed;z-index:2147483647;font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;";
  document.body.appendChild(hostEl);

  // ── Inject styles ──────────────────────────────────────────────────────────
  var styleEl = document.createElement("style");
  styleEl.textContent = buildStyles();
  hostEl.appendChild(styleEl);

  // ── Build the widget DOM ───────────────────────────────────────────────────
  var isOpen = false;
  var isLoading = false;

  // Floating action button
  var fab = document.createElement("button");
  fab.className = "nova-fab";
  fab.setAttribute("aria-label", "Open Nova AI Media Planner");
  fab.innerHTML =
    '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>';
  hostEl.appendChild(fab);

  // Widget panel
  var panel = document.createElement("div");
  panel.className = "nova-panel";
  panel.setAttribute("role", "dialog");
  panel.setAttribute("aria-label", "Nova AI Media Planner");
  panel.innerHTML = buildPanelHTML();
  hostEl.appendChild(panel);

  // ── Wire up events ─────────────────────────────────────────────────────────
  fab.addEventListener("click", toggleWidget);

  var closeBtn = panel.querySelector(".nova-panel-close");
  if (closeBtn) closeBtn.addEventListener("click", closeWidget);

  var form = panel.querySelector(".nova-form");
  if (form) form.addEventListener("submit", handleSubmit);

  // ── postMessage API ────────────────────────────────────────────────────────
  window.addEventListener("message", function (ev) {
    if (!ev.data || typeof ev.data.type !== "string") return;
    switch (ev.data.type) {
      case "nova-embed-open":
        openWidget();
        break;
      case "nova-embed-close":
        closeWidget();
        break;
      case "nova-embed-prefill":
        if (ev.data.data) prefillForm(ev.data.data);
        break;
    }
  });

  // Report load event and signal readiness
  reportLoad();
  window.postMessage({ type: "nova-embed-ready" }, "*");

  // ═════════════════════════════════════════════════════════════════════════════
  // Widget control functions
  // ═════════════════════════════════════════════════════════════════════════════

  function toggleWidget() {
    isOpen ? closeWidget() : openWidget();
  }

  function openWidget() {
    isOpen = true;
    panel.classList.add("nova-panel-open");
    fab.classList.add("nova-fab-hidden");
    var firstInput = panel.querySelector("input");
    if (firstInput) firstInput.focus();
  }

  function closeWidget() {
    isOpen = false;
    panel.classList.remove("nova-panel-open");
    fab.classList.remove("nova-fab-hidden");
    fab.focus();
  }

  function prefillForm(data) {
    var jobInput = panel.querySelector("#nova-job-title");
    var budgetInput = panel.querySelector("#nova-budget");
    var locationInput = panel.querySelector("#nova-location");
    if (data.jobTitle && jobInput) jobInput.value = data.jobTitle;
    if (data.budget && budgetInput) budgetInput.value = data.budget;
    if (data.location && locationInput) locationInput.value = data.location;
  }

  // ═════════════════════════════════════════════════════════════════════════════
  // API integration
  // ═════════════════════════════════════════════════════════════════════════════

  function handleSubmit(ev) {
    ev.preventDefault();
    if (isLoading) return;

    var jobTitle = panel.querySelector("#nova-job-title").value.trim();
    var budget = panel.querySelector("#nova-budget").value.trim();
    var location = panel.querySelector("#nova-location").value.trim();

    if (!jobTitle) {
      showError("Please enter a job title.");
      return;
    }

    isLoading = true;
    showLoading();

    var prompt = "Create a media plan for hiring a " + jobTitle;
    if (location) prompt += " in " + location;
    if (budget) prompt += " with a budget of $" + budget;
    prompt +=
      ". Show channel recommendations with budget allocation percentages.";

    var payload = JSON.stringify({ message: prompt });
    var apiUrl = config.baseUrl + "/api/chat";

    var headers = {
      "Content-Type": "application/json",
    };
    if (config.apiKey) {
      headers["Authorization"] = "Bearer " + config.apiKey;
    }

    fetch(apiUrl, {
      method: "POST",
      headers: headers,
      body: payload,
    })
      .then(function (resp) {
        if (!resp.ok) throw new Error("API error: " + resp.status);
        return resp.json();
      })
      .then(function (data) {
        isLoading = false;
        var response = data.response || data.message || data.reply || "";
        showResults(response, jobTitle, budget, location);
        window.postMessage(
          {
            type: "nova-embed-plan-generated",
            data: {
              jobTitle: jobTitle,
              budget: budget,
              location: location,
              response: response,
            },
          },
          "*",
        );
      })
      .catch(function (err) {
        isLoading = false;
        showError("Failed to generate plan. Please try again.");
        window.postMessage(
          {
            type: "nova-embed-error",
            error: err.message,
          },
          "*",
        );
      });
  }

  // ═════════════════════════════════════════════════════════════════════════════
  // UI rendering helpers
  // ═════════════════════════════════════════════════════════════════════════════

  function showLoading() {
    var resultsEl = panel.querySelector(".nova-results");
    resultsEl.innerHTML =
      '<div class="nova-loading">' +
      '<div class="nova-spinner"></div>' +
      "<p>Generating your media plan...</p>" +
      "</div>";
    resultsEl.style.display = "block";
    panel.querySelector(".nova-form").style.display = "none";
  }

  function showError(msg) {
    var resultsEl = panel.querySelector(".nova-results");
    resultsEl.innerHTML =
      '<div class="nova-error">' +
      "<p>" +
      escapeHTML(msg) +
      "</p>" +
      '<button class="nova-btn nova-btn-secondary nova-back-btn">Try Again</button>' +
      "</div>";
    resultsEl.style.display = "block";
    panel.querySelector(".nova-form").style.display = "none";
    resultsEl
      .querySelector(".nova-back-btn")
      .addEventListener("click", showForm);
  }

  function showResults(responseText, jobTitle, budget, location) {
    var resultsEl = panel.querySelector(".nova-results");
    var channelsHTML = parseChannelRecommendations(responseText);
    var fullPlanUrl = config.baseUrl + "/platform/plan";

    resultsEl.innerHTML =
      '<div class="nova-result-header">' +
      "<h3>Media Plan: " +
      escapeHTML(jobTitle) +
      "</h3>" +
      (location
        ? '<span class="nova-tag">' + escapeHTML(location) + "</span>"
        : "") +
      (budget
        ? '<span class="nova-tag">$' + escapeHTML(budget) + "</span>"
        : "") +
      "</div>" +
      '<div class="nova-channels">' +
      channelsHTML +
      "</div>" +
      '<div class="nova-response-text">' +
      formatMarkdown(responseText) +
      "</div>" +
      '<div class="nova-result-actions">' +
      '<a href="' +
      escapeHTML(fullPlanUrl) +
      '" target="_blank" rel="noopener" class="nova-btn nova-btn-primary">View Full Plan</a>' +
      '<button class="nova-btn nova-btn-secondary nova-back-btn">New Plan</button>' +
      "</div>";
    resultsEl.style.display = "block";
    panel.querySelector(".nova-form").style.display = "none";

    // Wire up "New Plan" button
    var backBtn = resultsEl.querySelector(".nova-back-btn");
    if (backBtn) backBtn.addEventListener("click", showForm);
  }

  function showForm() {
    var resultsEl = panel.querySelector(".nova-results");
    resultsEl.style.display = "none";
    resultsEl.innerHTML = "";
    panel.querySelector(".nova-form").style.display = "block";
  }

  function parseChannelRecommendations(text) {
    // Try to extract channel/percentage pairs from the response
    var channels = [];
    var patterns = [
      /(?:^|\n)\s*[-*]\s*\*?\*?([^:*\n]+?)\*?\*?\s*[:]\s*(\d+)%/gm,
      /([A-Za-z\s]+?)\s*[-:]\s*(\d+)%/g,
    ];
    var seen = {};

    for (var p = 0; p < patterns.length && channels.length < 8; p++) {
      var match;
      patterns[p].lastIndex = 0;
      while ((match = patterns[p].exec(text)) !== null && channels.length < 8) {
        var name = match[1].trim();
        var pct = parseInt(match[2], 10);
        if (
          pct > 0 &&
          pct <= 100 &&
          name.length > 1 &&
          name.length < 40 &&
          !seen[name.toLowerCase()]
        ) {
          seen[name.toLowerCase()] = true;
          channels.push({ name: name, pct: pct });
        }
      }
      if (channels.length > 0) break;
    }

    if (channels.length === 0) return "";

    var barColors = [
      BLUE_VIOLET,
      DOWNY_TEAL,
      "#7B68EE",
      "#48B0A0",
      "#9370DB",
      "#5AAFCF",
      "#6A5ACD",
      "#4DB8A4",
    ];
    var html = "";
    for (var i = 0; i < channels.length; i++) {
      var color = barColors[i % barColors.length];
      html +=
        '<div class="nova-channel-row">' +
        '<div class="nova-channel-label">' +
        escapeHTML(channels[i].name) +
        "</div>" +
        '<div class="nova-channel-bar-wrap">' +
        '<div class="nova-channel-bar" style="width:' +
        channels[i].pct +
        "%;background:" +
        color +
        '"></div>' +
        "</div>" +
        '<div class="nova-channel-pct">' +
        channels[i].pct +
        "%</div>" +
        "</div>";
    }
    return html;
  }

  function formatMarkdown(text) {
    // Minimal markdown: bold, line breaks, bullet points
    var safe = escapeHTML(text);
    safe = safe.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
    safe = safe.replace(/\n\s*[-*]\s+/g, "<br>&bull; ");
    safe = safe.replace(/\n/g, "<br>");
    // Truncate long responses for the widget view
    if (safe.length > 1200) {
      safe = safe.substring(0, 1200) + "&hellip;";
    }
    return "<p>" + safe + "</p>";
  }

  function escapeHTML(str) {
    var div = document.createElement("div");
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
  }

  // ═════════════════════════════════════════════════════════════════════════════
  // Panel HTML
  // ═════════════════════════════════════════════════════════════════════════════

  function buildPanelHTML() {
    return (
      '<div class="nova-panel-header">' +
      '<div class="nova-panel-title">' +
      '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="' +
      DOWNY_TEAL +
      '" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>' +
      "<span>Nova AI Media Planner</span>" +
      "</div>" +
      '<button class="nova-panel-close" aria-label="Close">&times;</button>' +
      "</div>" +
      '<div class="nova-panel-body">' +
      '<form class="nova-form">' +
      '<div class="nova-field">' +
      '<label for="nova-job-title">Job Title <span class="nova-required">*</span></label>' +
      '<input type="text" id="nova-job-title" placeholder="e.g., Senior Software Engineer" required autocomplete="off" />' +
      "</div>" +
      '<div class="nova-field">' +
      '<label for="nova-budget">Monthly Budget ($)</label>' +
      '<input type="text" id="nova-budget" placeholder="e.g., 5000" autocomplete="off" />' +
      "</div>" +
      '<div class="nova-field">' +
      '<label for="nova-location">Location</label>' +
      '<input type="text" id="nova-location" placeholder="e.g., San Francisco, CA" autocomplete="off" />' +
      "</div>" +
      '<button type="submit" class="nova-btn nova-btn-primary nova-submit-btn">Generate Plan</button>' +
      "</form>" +
      '<div class="nova-results" style="display:none"></div>' +
      "</div>" +
      '<div class="nova-panel-footer">' +
      "Created by Shubham Singh Chandel</div><!-- " +
      (config.baseUrl || "https://media-plan-generator.onrender.com") +
      '" target="_blank" rel="noopener">Nova AI Suite</a>' +
      "</div>"
    );
  }

  // ═════════════════════════════════════════════════════════════════════════════
  // Styles (fully self-contained, scoped to #nova-embed-host)
  // ═════════════════════════════════════════════════════════════════════════════

  function buildStyles() {
    var pos = config.position === "bottom-left" ? "left" : "right";
    var oppositePos = pos === "right" ? "left" : "right";

    return (
      "" +
      // ── Reset within host ──
      "#nova-embed-host * { box-sizing:border-box; margin:0; padding:0; font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif; }" +
      // ── FAB ──
      ".nova-fab {" +
      "  position:fixed;" +
      pos +
      ":24px;bottom:24px;" +
      "  width:56px;height:56px;border-radius:50%;border:none;cursor:pointer;" +
      "  background:linear-gradient(135deg," +
      BLUE_VIOLET +
      "," +
      DOWNY_TEAL +
      ");" +
      "  color:" +
      WHITE +
      ";" +
      "  box-shadow:0 4px 20px rgba(90,84,189,0.4);" +
      "  display:flex;align-items:center;justify-content:center;" +
      "  transition:all 0.3s cubic-bezier(0.34,1.56,0.64,1);" +
      "  z-index:2147483647;" +
      "}" +
      ".nova-fab:hover { transform:scale(1.1); box-shadow:0 6px 28px rgba(90,84,189,0.55); }" +
      ".nova-fab-hidden { transform:scale(0); opacity:0; pointer-events:none; }" +
      // ── Panel ──
      ".nova-panel {" +
      "  position:fixed;" +
      pos +
      ":24px;bottom:24px;" +
      oppositePos +
      ":auto;" +
      "  width:380px;max-width:calc(100vw - 48px);" +
      "  max-height:calc(100vh - 48px);" +
      "  background:" +
      themeColor(DARK_BG, LIGHT_BG) +
      ";" +
      "  border:1px solid " +
      themeColor(DARK_BORDER, LIGHT_BORDER) +
      ";" +
      "  border-radius:16px;" +
      "  box-shadow:0 8px 40px rgba(0,0,0," +
      (isDark() ? "0.5" : "0.15") +
      ");" +
      "  display:flex;flex-direction:column;overflow:hidden;" +
      "  transform:scale(0.8) translateY(20px);opacity:0;pointer-events:none;" +
      "  transition:all 0.3s cubic-bezier(0.34,1.56,0.64,1);" +
      "  transform-origin:bottom " +
      pos +
      ";" +
      "  z-index:2147483647;" +
      "}" +
      ".nova-panel-open { transform:scale(1) translateY(0);opacity:1;pointer-events:auto; }" +
      // ── Panel header ──
      ".nova-panel-header {" +
      "  display:flex;align-items:center;justify-content:space-between;" +
      "  padding:14px 16px;" +
      "  background:" +
      themeColor(
        "linear-gradient(135deg," + PORT_GORE + "," + DARK_CARD + ")",
        "linear-gradient(135deg,#f0f0f8,#fff)",
      ) +
      ";" +
      "  border-bottom:1px solid " +
      themeColor(DARK_BORDER, LIGHT_BORDER) +
      ";" +
      "}" +
      ".nova-panel-title { display:flex;align-items:center;gap:8px;font-size:14px;font-weight:600;color:" +
      themeColor(WHITE, LIGHT_TEXT) +
      "; }" +
      ".nova-panel-close {" +
      "  background:none;border:none;cursor:pointer;font-size:22px;line-height:1;" +
      "  color:" +
      themeColor(DARK_TEXT_SEC, LIGHT_TEXT_SEC) +
      ";padding:0 2px;" +
      "  transition:color 0.15s;" +
      "}" +
      ".nova-panel-close:hover { color:" +
      themeColor(WHITE, LIGHT_TEXT) +
      "; }" +
      // ── Panel body ──
      ".nova-panel-body {" +
      "  flex:1;overflow-y:auto;padding:20px 16px;" +
      "  color:" +
      themeColor(DARK_TEXT, LIGHT_TEXT) +
      ";" +
      "}" +
      // ── Form ──
      ".nova-field { margin-bottom:16px; }" +
      ".nova-field label {" +
      "  display:block;font-size:12px;font-weight:600;margin-bottom:6px;" +
      "  color:" +
      themeColor(DARK_TEXT_SEC, LIGHT_TEXT_SEC) +
      ";" +
      "  text-transform:uppercase;letter-spacing:0.5px;" +
      "}" +
      ".nova-required { color:#ef4444; }" +
      ".nova-field input {" +
      "  width:100%;padding:10px 12px;border-radius:8px;font-size:14px;" +
      "  border:1px solid " +
      themeColor(DARK_BORDER, LIGHT_BORDER) +
      ";" +
      "  background:" +
      themeColor(DARK_CARD, LIGHT_CARD) +
      ";" +
      "  color:" +
      themeColor(DARK_TEXT, LIGHT_TEXT) +
      ";" +
      "  outline:none;transition:border-color 0.2s;" +
      "}" +
      ".nova-field input:focus { border-color:" +
      BLUE_VIOLET +
      "; }" +
      ".nova-field input::placeholder { color:" +
      themeColor("#555570", "#999") +
      "; }" +
      // ── Buttons ──
      ".nova-btn {" +
      "  display:inline-flex;align-items:center;justify-content:center;" +
      "  padding:10px 18px;border-radius:8px;font-size:13px;font-weight:600;" +
      "  cursor:pointer;border:none;transition:all 0.2s;text-decoration:none;" +
      "}" +
      ".nova-btn-primary {" +
      "  background:linear-gradient(135deg," +
      BLUE_VIOLET +
      "," +
      DOWNY_TEAL +
      ");" +
      "  color:" +
      WHITE +
      ";width:100%;" +
      "}" +
      ".nova-btn-primary:hover { opacity:0.9;transform:translateY(-1px); }" +
      ".nova-btn-secondary {" +
      "  background:" +
      themeColor(DARK_CARD, "#eee") +
      ";" +
      "  color:" +
      themeColor(DARK_TEXT, LIGHT_TEXT) +
      ";" +
      "  border:1px solid " +
      themeColor(DARK_BORDER, LIGHT_BORDER) +
      ";" +
      "}" +
      ".nova-btn-secondary:hover { border-color:" +
      BLUE_VIOLET +
      "; }" +
      // ── Results ──
      ".nova-result-header { margin-bottom:16px; }" +
      ".nova-result-header h3 { font-size:16px;font-weight:700;margin-bottom:8px;color:" +
      themeColor(WHITE, LIGHT_TEXT) +
      "; }" +
      ".nova-tag {" +
      "  display:inline-block;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:500;" +
      "  margin-right:6px;margin-bottom:4px;" +
      "  background:" +
      themeColor("rgba(90,84,189,0.2)", "rgba(90,84,189,0.1)") +
      ";" +
      "  color:" +
      themeColor("#b0a8ff", BLUE_VIOLET) +
      ";" +
      "}" +
      // ── Channel bars ──
      ".nova-channels { margin-bottom:16px; }" +
      ".nova-channel-row { display:flex;align-items:center;gap:8px;margin-bottom:8px; }" +
      ".nova-channel-label { font-size:12px;width:110px;flex-shrink:0;color:" +
      themeColor(DARK_TEXT_SEC, LIGHT_TEXT_SEC) +
      ";white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }" +
      ".nova-channel-bar-wrap { flex:1;height:8px;background:" +
      themeColor("rgba(255,255,255,0.06)", "rgba(0,0,0,0.06)") +
      ";border-radius:4px;overflow:hidden; }" +
      ".nova-channel-bar { height:100%;border-radius:4px;transition:width 0.6s ease; }" +
      ".nova-channel-pct { font-size:12px;font-weight:600;width:36px;text-align:right;color:" +
      themeColor(DARK_TEXT, LIGHT_TEXT) +
      "; }" +
      // ── Response text ──
      ".nova-response-text {" +
      "  font-size:13px;line-height:1.6;max-height:200px;overflow-y:auto;" +
      "  margin-bottom:16px;padding:12px;border-radius:8px;" +
      "  background:" +
      themeColor(DARK_CARD, LIGHT_CARD) +
      ";" +
      "  border:1px solid " +
      themeColor(DARK_BORDER, LIGHT_BORDER) +
      ";" +
      "  color:" +
      themeColor(DARK_TEXT_SEC, LIGHT_TEXT_SEC) +
      ";" +
      "}" +
      ".nova-response-text strong { color:" +
      themeColor(WHITE, LIGHT_TEXT) +
      "; }" +
      // ── Result actions ──
      ".nova-result-actions { display:flex;gap:10px; }" +
      ".nova-result-actions .nova-btn { flex:1; }" +
      // ── Loading ──
      ".nova-loading { text-align:center;padding:40px 0; }" +
      ".nova-loading p { margin-top:16px;font-size:13px;color:" +
      themeColor(DARK_TEXT_SEC, LIGHT_TEXT_SEC) +
      "; }" +
      ".nova-spinner {" +
      "  width:32px;height:32px;margin:0 auto;border:3px solid " +
      themeColor("rgba(255,255,255,0.1)", "rgba(0,0,0,0.1)") +
      ";" +
      "  border-top-color:" +
      BLUE_VIOLET +
      ";border-radius:50%;" +
      "  animation:nova-spin 0.8s linear infinite;" +
      "}" +
      "@keyframes nova-spin { to { transform:rotate(360deg); } }" +
      // ── Error ──
      ".nova-error { text-align:center;padding:30px 0; }" +
      ".nova-error p { font-size:13px;color:#ef4444;margin-bottom:16px; }" +
      // ── Footer ──
      ".nova-panel-footer {" +
      "  padding:10px 16px;text-align:center;font-size:11px;" +
      "  color:" +
      themeColor(DARK_TEXT_SEC, LIGHT_TEXT_SEC) +
      ";" +
      "  border-top:1px solid " +
      themeColor(DARK_BORDER, LIGHT_BORDER) +
      ";" +
      "}" +
      ".nova-panel-footer a { color:" +
      DOWNY_TEAL +
      ";text-decoration:none; }" +
      ".nova-panel-footer a:hover { text-decoration:underline; }" +
      // ── Responsive ──
      "@media (max-width:440px) {" +
      "  .nova-panel { width:calc(100vw - 16px);" +
      pos +
      ":8px;bottom:8px;border-radius:12px; }" +
      "  .nova-fab { " +
      pos +
      ":16px;bottom:16px;width:48px;height:48px; }" +
      "}" +
      // ── Reduced motion ──
      "@media (prefers-reduced-motion:reduce) {" +
      "  .nova-fab,.nova-panel,.nova-channel-bar { transition:none !important; }" +
      "  .nova-spinner { animation:none; }" +
      "}"
    );
  }
})();
