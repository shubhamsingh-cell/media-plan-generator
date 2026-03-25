/** Nova Conversational Canvas -- split-pane chat + plan editor. NovaCanvas.init({containerId,planData}) */
(function () {
  "use strict";

  var C = {
    PORT_GORE: "#202058",
    BLUE_VIOLET: "#5A54BD",
    DOWNY_TEAL: "#6BB3CD",
    BG_DARK: "#0f0f1a",
    BG_CARD: "rgba(26,26,46,0.92)",
    BG_SURFACE: "rgba(32,32,88,0.25)",
    TEXT: "#d4d4d8",
    TEXT_DIM: "#888",
    BORDER: "rgba(107,179,205,0.15)",
    SUCCESS: "#34D399",
    WARNING: "#F59E0B",
    ERROR: "#F87171",
  };

  var state = {
    container: null,
    chatPanel: null,
    canvasPanel: null,
    planData: null,
    canvasState: null,
    chatMessages: [],
    dragTarget: null,
    dragStartX: 0,
    dragStartPct: 0,
    initialized: false,
    csrfToken: "",
  };

  function injectStyles() {
    if (document.getElementById("nova-canvas-css")) return;
    var s = document.createElement("style");
    s.id = "nova-canvas-css";
    var b = C.BORDER,
      t = C.DOWNY_TEAL,
      v = C.BLUE_VIOLET,
      d = C.TEXT_DIM,
      x = C.TEXT,
      bg = C.BG_DARK,
      sf = C.BG_SURFACE,
      cd = C.BG_CARD;
    s.textContent =
      ".nc-root{display:flex;height:100%;min-height:500px;font-family:'Inter',system-ui,sans-serif;color:" +
      x +
      ";background:" +
      bg +
      ";border-radius:12px;overflow:hidden;border:1px solid " +
      b +
      "}" +
      ".nc-chat{width:38%;min-width:280px;display:flex;flex-direction:column;border-right:1px solid " +
      b +
      ";background:" +
      bg +
      "}" +
      ".nc-canvas{flex:1;display:flex;flex-direction:column;background:linear-gradient(135deg," +
      bg +
      " 0%,rgba(32,32,88,.15) 100%);overflow-y:auto}" +
      ".nc-chat-header,.nc-canvas-header{padding:14px 18px;font-size:13px;font-weight:600;letter-spacing:.3px;text-transform:uppercase;border-bottom:1px solid " +
      b +
      ";color:" +
      t +
      ";display:flex;align-items:center;gap:8px}" +
      ".nc-chat-header svg,.nc-canvas-header svg{width:16px;height:16px;opacity:.7}" +
      ".nc-msgs{flex:1;overflow-y:auto;padding:14px;display:flex;flex-direction:column;gap:10px}" +
      ".nc-msg{padding:10px 14px;border-radius:10px;font-size:13px;line-height:1.55;max-width:88%;word-wrap:break-word;animation:nc-fadeIn .25s ease}" +
      ".nc-msg-user{align-self:flex-end;background:" +
      v +
      ";color:#fff;border-bottom-right-radius:3px}" +
      ".nc-msg-ai{align-self:flex-start;background:" +
      sf +
      ";border:1px solid " +
      b +
      ";border-bottom-left-radius:3px}" +
      ".nc-msg-system{align-self:center;color:" +
      d +
      ";font-size:11px;font-style:italic;padding:4px 8px}" +
      ".nc-input-wrap{padding:12px;border-top:1px solid " +
      b +
      ";display:flex;gap:8px}" +
      ".nc-input{flex:1;background:rgba(255,255,255,.04);border:1px solid " +
      b +
      ";border-radius:8px;padding:10px 14px;color:" +
      x +
      ";font-size:13px;outline:none;resize:none;font-family:inherit;min-height:20px;max-height:100px}" +
      ".nc-input:focus{border-color:" +
      t +
      "}" +
      ".nc-send-btn{background:" +
      t +
      ";border:none;border-radius:8px;padding:0 14px;cursor:pointer;color:#fff;font-weight:600;font-size:13px;transition:opacity .15s}" +
      ".nc-send-btn:hover{opacity:.85}.nc-send-btn:disabled{opacity:.4;cursor:not-allowed}" +
      ".nc-budget-bar{margin:16px 18px 8px;background:" +
      sf +
      ";border-radius:10px;padding:14px 18px;border:1px solid " +
      b +
      "}" +
      ".nc-budget-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}" +
      ".nc-budget-label{font-size:11px;color:" +
      d +
      ";text-transform:uppercase;letter-spacing:.5px}" +
      ".nc-budget-val{font-size:18px;font-weight:700;color:" +
      t +
      "}" +
      ".nc-budget-track{height:6px;background:rgba(255,255,255,.06);border-radius:3px;overflow:hidden}" +
      ".nc-budget-fill{height:100%;border-radius:3px;transition:width .4s cubic-bezier(.4,0,.2,1);background:linear-gradient(90deg," +
      t +
      "," +
      v +
      ")}" +
      ".nc-budget-remaining{font-size:11px;color:" +
      d +
      ";margin-top:6px;text-align:right}" +
      ".nc-channels{padding:8px 18px 18px;display:flex;flex-direction:column;gap:10px}" +
      ".nc-card{background:" +
      cd +
      ";border:1px solid " +
      b +
      ";border-radius:10px;padding:14px 16px;transition:border-color .2s,box-shadow .2s}" +
      ".nc-card:hover{border-color:rgba(107,179,205,.35)}" +
      ".nc-card.nc-highlight{border-color:" +
      t +
      ";box-shadow:0 0 20px rgba(107,179,205,.15)}" +
      ".nc-card-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}" +
      ".nc-card-name{font-size:14px;font-weight:600;display:flex;align-items:center;gap:8px}" +
      ".nc-card-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}" +
      ".nc-card-spend{font-size:13px;font-weight:600;color:" +
      t +
      "}" +
      ".nc-card-meta{display:flex;gap:16px;font-size:11px;color:" +
      d +
      ";margin-bottom:10px}" +
      ".nc-slider-wrap{position:relative;height:28px;display:flex;align-items:center}" +
      ".nc-slider-track{position:absolute;left:0;right:0;height:4px;background:rgba(255,255,255,.06);border-radius:2px}" +
      ".nc-slider-fill{position:absolute;left:0;height:4px;border-radius:2px;transition:width .2s}" +
      ".nc-slider-thumb{position:absolute;width:18px;height:18px;border-radius:50%;background:#fff;border:2px solid " +
      t +
      ";cursor:grab;top:50%;transform:translateY(-50%);z-index:2;transition:box-shadow .15s}" +
      ".nc-slider-thumb:hover{box-shadow:0 0 0 6px rgba(107,179,205,.2)}" +
      ".nc-slider-thumb:active{cursor:grabbing;box-shadow:0 0 0 8px rgba(107,179,205,.25)}" +
      ".nc-slider-pct{position:absolute;right:0;font-size:12px;font-weight:600;color:" +
      x +
      ";min-width:42px;text-align:right}" +
      ".nc-suggestions{padding:0 18px 18px}" +
      ".nc-sug{display:flex;align-items:flex-start;gap:10px;padding:10px 14px;background:rgba(107,179,205,.06);border:1px solid rgba(107,179,205,.12);border-radius:8px;margin-bottom:8px;font-size:12px;line-height:1.5;cursor:pointer;transition:background .15s}" +
      ".nc-sug:hover{background:rgba(107,179,205,.12);border-color:rgba(107,179,205,.25)}" +
      ".nc-sug-icon{font-size:14px;flex-shrink:0;margin-top:1px}" +
      ".nc-sug-text{flex:1;color:" +
      x +
      "}" +
      ".nc-sug-apply{font-size:11px;color:" +
      t +
      ";font-weight:600;white-space:nowrap;padding:3px 10px;border:1px solid " +
      t +
      ";border-radius:5px;background:transparent;cursor:pointer;transition:background .15s}" +
      ".nc-sug-apply:hover{background:rgba(107,179,205,.15)}" +
      "@keyframes nc-fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}" +
      "@keyframes nc-pulse{0%,100%{opacity:.6}50%{opacity:1}}" +
      ".nc-typing{padding:10px 14px;color:" +
      d +
      ";font-size:12px;animation:nc-pulse 1.2s infinite}" +
      "@media(max-width:768px){.nc-root{flex-direction:column;height:auto}.nc-chat{width:100%;border-right:none;border-bottom:1px solid " +
      b +
      ";max-height:45vh}.nc-canvas{min-height:55vh}}" +
      "@media(prefers-reduced-motion:reduce){.nc-msg,.nc-card,.nc-slider-fill,.nc-budget-fill{animation:none!important;transition:none!important}}";
    document.head.appendChild(s);
  }
  function fmt(n) {
    if (n >= 1000000) return "$" + (n / 1000000).toFixed(1) + "M";
    if (n >= 1000) return "$" + (n / 1000).toFixed(1) + "K";
    return "$" + n.toFixed(0);
  }
  function buildUI(container) {
    container.innerHTML = "";
    var root = document.createElement("div");
    root.className = "nc-root";
    var chat = document.createElement("div");
    chat.className = "nc-chat";
    chat.innerHTML =
      '<div class="nc-chat-header"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15a2 2 0 01-2 2H7l-4 4V5a2 2 0 012-2h14a2 2 0 012 2z"/></svg>Nova Chat</div>' +
      '<div class="nc-msgs" id="nc-msgs"></div>' +
      '<div class="nc-input-wrap"><textarea class="nc-input" id="nc-input" placeholder="Ask about your plan..." rows="1"></textarea><button class="nc-send-btn" id="nc-send">Send</button></div>';
    root.appendChild(chat);
    state.chatPanel = chat;
    var canvas = document.createElement("div");
    canvas.className = "nc-canvas";
    canvas.innerHTML =
      '<div class="nc-canvas-header"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="9" y1="3" x2="9" y2="21"/></svg>Plan Canvas</div>' +
      '<div id="nc-budget"></div>' +
      '<div id="nc-channels" class="nc-channels"></div>' +
      '<div id="nc-suggestions" class="nc-suggestions"></div>';
    root.appendChild(canvas);
    state.canvasPanel = canvas;

    container.appendChild(root);
    var input = document.getElementById("nc-input");
    var sendBtn = document.getElementById("nc-send");
    if (input && sendBtn) {
      sendBtn.addEventListener("click", function () {
        sendMessage();
      });
      input.addEventListener("keydown", function (e) {
        if (e.key === "Enter" && !e.shiftKey) {
          e.preventDefault();
          sendMessage();
        }
      });
      input.addEventListener("input", function () {
        this.style.height = "auto";
        this.style.height = Math.min(this.scrollHeight, 100) + "px";
      });
    }
  }
  function renderBudget() {
    var cs = state.canvasState;
    if (!cs) return;
    var el = document.getElementById("nc-budget");
    if (!el) return;
    var pct =
      cs.total_budget > 0
        ? Math.min(100, (cs.allocated / cs.total_budget) * 100)
        : 0;
    el.innerHTML =
      '<div class="nc-budget-bar">' +
      '<div class="nc-budget-row">' +
      '<span class="nc-budget-label">Total Budget</span>' +
      '<span class="nc-budget-val">' +
      fmt(cs.total_budget) +
      "</span>" +
      "</div>" +
      '<div class="nc-budget-track"><div class="nc-budget-fill" style="width:' +
      pct.toFixed(1) +
      '%"></div></div>' +
      '<div class="nc-budget-remaining">Allocated: ' +
      fmt(cs.allocated) +
      " / Remaining: " +
      fmt(cs.remaining) +
      "</div>" +
      "</div>";
  }
  function renderChannels() {
    var cs = state.canvasState;
    if (!cs) return;
    var wrap = document.getElementById("nc-channels");
    if (!wrap) return;
    wrap.innerHTML = "";

    cs.channels.forEach(function (ch) {
      var card = document.createElement("div");
      card.className = "nc-card";
      card.id = "nc-card-" + ch.id;
      card.setAttribute("data-channel-id", ch.id);

      var metaParts = [];
      if (ch.cpc != null)
        metaParts.push("CPC: $" + parseFloat(ch.cpc).toFixed(2));
      if (ch.cpa != null)
        metaParts.push("CPA: $" + parseFloat(ch.cpa).toFixed(2));

      card.innerHTML =
        '<div class="nc-card-head">' +
        '<span class="nc-card-name"><span class="nc-card-dot" style="background:' +
        ch.color +
        '"></span>' +
        escHtml(ch.name) +
        "</span>" +
        '<span class="nc-card-spend">' +
        fmt(ch.spend) +
        "</span>" +
        "</div>" +
        (metaParts.length
          ? '<div class="nc-card-meta">' +
            metaParts.join('<span style="opacity:.3">|</span>') +
            "</div>"
          : "") +
        '<div class="nc-slider-wrap">' +
        '<div class="nc-slider-track"></div>' +
        '<div class="nc-slider-fill" style="width:' +
        ch.percentage +
        "%;background:" +
        ch.color +
        '"></div>' +
        '<div class="nc-slider-thumb" style="left:calc(' +
        ch.percentage +
        '% - 9px)" data-ch="' +
        ch.id +
        '"></div>' +
        '<span class="nc-slider-pct">' +
        ch.percentage.toFixed(1) +
        "%</span>" +
        "</div>";

      wrap.appendChild(card);
    });
    wrap.querySelectorAll(".nc-slider-thumb").forEach(function (thumb) {
      thumb.addEventListener("mousedown", startDrag);
      thumb.addEventListener("touchstart", startDrag, { passive: false });
    });
  }
  function renderSuggestions() {
    var cs = state.canvasState;
    if (!cs || !cs.suggestions || !cs.suggestions.length) return;
    var wrap = document.getElementById("nc-suggestions");
    if (!wrap) return;
    wrap.innerHTML = "";

    cs.suggestions.forEach(function (sug) {
      var el = document.createElement("div");
      el.className = "nc-sug";
      var icon =
        sug.type === "warning"
          ? "\u26A0"
          : sug.type === "add"
            ? "+"
            : "\uD83D\uDCA1";
      el.innerHTML =
        '<span class="nc-sug-icon">' +
        icon +
        "</span>" +
        '<span class="nc-sug-text">' +
        escHtml(sug.text) +
        "</span>" +
        '<button class="nc-sug-apply">Apply</button>';

      el.querySelector(".nc-sug-apply").addEventListener("click", function (e) {
        e.stopPropagation();
        applySuggestion(sug);
      });
      wrap.appendChild(el);
    });
  }
  function startDrag(e) {
    e.preventDefault();
    var thumb = e.currentTarget;
    var chId = thumb.getAttribute("data-ch");
    var track = thumb.parentElement;
    var rect = track.getBoundingClientRect();

    state.dragTarget = chId;
    state.dragStartX = e.touches ? e.touches[0].clientX : e.clientX;
    state.dragTrackRect = rect;

    var ch = findChannel(chId);
    state.dragStartPct = ch ? ch.percentage : 0;

    document.addEventListener("mousemove", onDrag);
    document.addEventListener("mouseup", endDrag);
    document.addEventListener("touchmove", onDrag, { passive: false });
    document.addEventListener("touchend", endDrag);
  }

  function onDrag(e) {
    if (!state.dragTarget) return;
    e.preventDefault();
    var clientX = e.touches ? e.touches[0].clientX : e.clientX;
    var rect = state.dragTrackRect;
    var trackWidth = rect.width - 52;
    var relX = clientX - rect.left;
    var pct = Math.max(0, Math.min(100, (relX / trackWidth) * 100));
    var ch = findChannel(state.dragTarget);
    if (ch) {
      var card = document.getElementById("nc-card-" + ch.id);
      if (card) {
        var fill = card.querySelector(".nc-slider-fill");
        var thumb = card.querySelector(".nc-slider-thumb");
        var label = card.querySelector(".nc-slider-pct");
        if (fill) fill.style.width = pct.toFixed(1) + "%";
        if (thumb) thumb.style.left = "calc(" + pct.toFixed(1) + "% - 9px)";
        if (label) label.textContent = pct.toFixed(1) + "%";
      }
    }
  }

  function endDrag(e) {
    document.removeEventListener("mousemove", onDrag);
    document.removeEventListener("mouseup", endDrag);
    document.removeEventListener("touchmove", onDrag);
    document.removeEventListener("touchend", endDrag);

    if (!state.dragTarget) return;
    var chId = state.dragTarget;
    state.dragTarget = null;
    var card = document.getElementById("nc-card-" + chId);
    var fill = card ? card.querySelector(".nc-slider-fill") : null;
    var newPct = fill ? parseFloat(fill.style.width) : state.dragStartPct;

    if (Math.abs(newPct - state.dragStartPct) < 0.5) return; // No significant change

    var ch = findChannel(chId);
    var chName = ch ? ch.name : chId;

    applyEdit({
      type: "reallocate",
      channel_id: chId,
      percentage: Math.round(newPct * 10) / 10,
    });

    addChatMessage(
      "system",
      "Budget changed: " +
        chName +
        " " +
        state.dragStartPct.toFixed(1) +
        "% \u2192 " +
        newPct.toFixed(1) +
        "%",
    );
  }
  function applyEdit(edit) {
    var cs = state.canvasState;
    if (!cs) return;
    applyEditLocally(edit);
    var body = JSON.stringify({ plan_id: cs.plan_id, edit: edit });
    fetch("/api/canvas/edit", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": state.csrfToken,
      },
      body: body,
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (data.error) {
          addChatMessage("ai", "Edit failed: " + data.error);
          return;
        }
        state.canvasState = data;
        renderAll();
      })
      .catch(function (err) {
        addChatMessage("ai", "Could not save edit: " + err.message);
      });
  }
  function applyEditLocally(edit) {
    var cs = state.canvasState;
    if (!cs) return;

    if (edit.type === "reallocate") {
      var target = findChannel(edit.channel_id);
      if (!target) return;
      var oldPct = target.percentage;
      var newPct = edit.percentage;
      var delta = newPct - oldPct;
      var others = cs.channels.filter(function (c) {
        return c.id !== edit.channel_id;
      });
      var otherTotal = others.reduce(function (s, c) {
        return s + c.percentage;
      }, 0);

      if (otherTotal > 0 && Math.abs(delta) > 0.01) {
        others.forEach(function (c) {
          c.percentage = Math.max(
            0,
            +(c.percentage - delta * (c.percentage / otherTotal)).toFixed(1),
          );
          c.spend = +((cs.total_budget * c.percentage) / 100).toFixed(2);
        });
      }
      target.percentage = +newPct.toFixed(1);
      target.spend = +((cs.total_budget * target.percentage) / 100).toFixed(2);
    }

    cs.allocated = cs.channels.reduce(function (s, c) {
      return s + c.spend;
    }, 0);
    cs.remaining = Math.max(0, cs.total_budget - cs.allocated);
    renderAll();
  }
  function applySuggestion(sug) {
    if (sug.type === "add") {
      var name =
        sug.text.indexOf("LinkedIn") >= 0
          ? "LinkedIn"
          : sug.text.indexOf("programmatic") >= 0
            ? "Programmatic"
            : "New Channel";
      applyEdit({
        type: "add_channel",
        name: name,
        percentage: sug.suggested_pct,
      });
      addChatMessage(
        "system",
        "Added " + name + " at " + sug.suggested_pct + "%",
      );
    } else if (sug.channel_id) {
      applyEdit({
        type: "reallocate",
        channel_id: sug.channel_id,
        percentage: sug.suggested_pct,
      });
      var ch = findChannel(sug.channel_id);
      addChatMessage(
        "system",
        "Applied suggestion: " +
          (ch ? ch.name : "") +
          " \u2192 " +
          sug.suggested_pct +
          "%",
      );
    }
  }
  function sendMessage() {
    var input = document.getElementById("nc-input");
    if (!input) return;
    var text = input.value.trim();
    if (!text) return;
    input.value = "";
    input.style.height = "auto";

    addChatMessage("user", text);
    processChat(text);
  }

  function addChatMessage(role, text) {
    state.chatMessages.push({ role: role, text: text });
    var msgs = document.getElementById("nc-msgs");
    if (!msgs) return;
    var cls =
      role === "user"
        ? "nc-msg-user"
        : role === "ai"
          ? "nc-msg-ai"
          : "nc-msg-system";
    var el = document.createElement("div");
    el.className = "nc-msg " + cls;
    el.textContent = text;
    msgs.appendChild(el);
    msgs.scrollTop = msgs.scrollHeight;
  }

  function processChat(text) {
    var lower = text.toLowerCase();
    var cs = state.canvasState;
    if (!cs) {
      addChatMessage("ai", "No plan loaded. Initialize with plan data first.");
      return;
    }
    var match = lower.match(
      /(increase|decrease|set|boost|reduce|raise|lower)\s+(.+?)(?:\s+(?:to|by)\s+)(\d+)\s*%/,
    );
    if (match) {
      var action = match[1];
      var chName = match[2].trim();
      var val = parseInt(match[3], 10);
      var ch = cs.channels.find(function (c) {
        return c.name.toLowerCase().includes(chName);
      });

      if (ch) {
        var newPct = val;
        if (action === "increase" || action === "boost" || action === "raise") {
          newPct = ch.percentage + val;
        } else if (
          action === "decrease" ||
          action === "reduce" ||
          action === "lower"
        ) {
          newPct = Math.max(0, ch.percentage - val);
        }

        highlightCard(ch.id);
        applyEdit({
          type: "reallocate",
          channel_id: ch.id,
          percentage: newPct,
        });
        addChatMessage(
          "ai",
          "Updated " +
            ch.name +
            " to " +
            newPct.toFixed(1) +
            "%. The canvas reflects the change.",
        );
        return;
      }
    }
    if (lower.startsWith("add ")) {
      var name = text.substring(4).trim();
      applyEdit({ type: "add_channel", name: name, percentage: 10 });
      addChatMessage(
        "ai",
        "Added " + name + " with 10% allocation. Drag the slider to adjust.",
      );
      return;
    }
    if (lower.startsWith("remove ")) {
      var rName = text.substring(7).trim();
      var rCh = cs.channels.find(function (c) {
        return c.name.toLowerCase().includes(rName.toLowerCase());
      });
      if (rCh) {
        applyEdit({ type: "remove_channel", channel_id: rCh.id });
        addChatMessage(
          "ai",
          "Removed " +
            rCh.name +
            ". Budget redistributed among remaining channels.",
        );
      } else {
        addChatMessage(
          "ai",
          'Could not find a channel matching "' + rName + '".',
        );
      }
      return;
    }
    var budgetMatch = lower.match(/(?:set\s+)?budget\s+(?:to\s+)?\$?([\d,]+)/);
    if (budgetMatch) {
      var budget = parseInt(budgetMatch[1].replace(/,/g, ""), 10);
      applyEdit({ type: "set_budget", budget: budget });
      addChatMessage(
        "ai",
        "Budget updated to " +
          fmt(budget) +
          ". All channel spends recalculated.",
      );
      return;
    }
    addTypingIndicator();
    fetch("/api/chat", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": state.csrfToken,
      },
      body: JSON.stringify({ message: text, context: "canvas" }),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        removeTypingIndicator();
        addChatMessage("ai", data.response || data.message || "No response.");
      })
      .catch(function () {
        removeTypingIndicator();
        addChatMessage("ai", "Sorry, I couldn't reach Nova. Try again.");
      });
  }

  function addTypingIndicator() {
    var msgs = document.getElementById("nc-msgs");
    if (!msgs) return;
    var el = document.createElement("div");
    el.className = "nc-typing";
    el.id = "nc-typing";
    el.textContent = "Nova is thinking...";
    msgs.appendChild(el);
    msgs.scrollTop = msgs.scrollHeight;
  }

  function removeTypingIndicator() {
    var el = document.getElementById("nc-typing");
    if (el) el.remove();
  }
  function highlightCard(chId) {
    var card = document.getElementById("nc-card-" + chId);
    if (!card) return;
    card.classList.add("nc-highlight");
    card.scrollIntoView({ behavior: "smooth", block: "nearest" });
    setTimeout(function () {
      card.classList.remove("nc-highlight");
    }, 2000);
  }
  function findChannel(chId) {
    if (!state.canvasState) return null;
    return (
      state.canvasState.channels.find(function (c) {
        return c.id === chId;
      }) || null
    );
  }

  function escHtml(s) {
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function renderAll() {
    renderBudget();
    renderChannels();
    renderSuggestions();
  }
  function fetchCsrf() {
    fetch("/api/csrf-token")
      .then(function (r) {
        return r.json();
      })
      .then(function (d) {
        state.csrfToken = d.token || "";
      })
      .catch(function () {});
  }
  function init(opts) {
    if (state.initialized) return;
    opts = opts || {};
    injectStyles();

    var container = document.getElementById(opts.containerId || "canvas-root");
    if (!container) {
      console.error("[NovaCanvas] Container not found:", opts.containerId);
      return;
    }
    state.container = container;
    buildUI(container);
    fetchCsrf();

    if (opts.planData) {
      loadPlan(opts.planData);
    } else {
      addChatMessage(
        "ai",
        "Welcome to the Conversational Canvas. Load a plan to get started, or describe what you need.",
      );
    }

    state.initialized = true;
  }

  function loadPlan(planData) {
    state.planData = planData;
    fetch(
      "/api/canvas/state/" +
        encodeURIComponent(planData.plan_id || planData.id || "new"),
      {
        method: "GET",
      },
    )
      .then(function (r) {
        if (r.ok) return r.json();
        return parseLocally(planData);
      })
      .then(function (cs) {
        if (cs && !cs.error) {
          state.canvasState = cs;
        } else {
          state.canvasState = parseLocally(planData);
        }
        renderAll();
        addChatMessage(
          "ai",
          "Plan loaded with " +
            (state.canvasState.channels || []).length +
            " channels and " +
            fmt(state.canvasState.total_budget) +
            " total budget. Drag sliders to reallocate, or tell me what to change.",
        );
      })
      .catch(function () {
        state.canvasState = parseLocally(planData);
        renderAll();
        addChatMessage("ai", "Plan loaded locally. Backend sync unavailable.");
      });
  }
  function parseLocally(pd) {
    var channels =
      pd.channels || pd.channel_allocations || pd.recommendations || [];
    var total = 0;
    var parsed = [];
    var colors = [
      C.DOWNY_TEAL,
      C.BLUE_VIOLET,
      "#34D399",
      "#F59E0B",
      "#F87171",
      "#A78BFA",
      "#60A5FA",
      "#FBBF24",
    ];

    if (Array.isArray(channels)) {
      channels.forEach(function (ch, i) {
        var spend = parseFloat(ch.spend || ch.budget || ch.allocation || 0);
        total += spend;
        parsed.push({
          id: "ch_" + i,
          name: ch.channel || ch.name || ch.platform || "Channel " + (i + 1),
          spend: spend,
          color: colors[i % colors.length],
          cpc: ch.cpc || null,
          cpa: ch.cpa || null,
          percentage: 0,
        });
      });
    }

    if (!total) total = parseFloat(pd.total_budget || pd.budget || 0);
    parsed.forEach(function (ch) {
      ch.percentage = total > 0 ? +((ch.spend / total) * 100).toFixed(1) : 0;
    });

    return {
      plan_id: pd.plan_id || pd.id || "local_" + Date.now(),
      total_budget: total,
      allocated: parsed.reduce(function (s, c) {
        return s + c.spend;
      }, 0),
      remaining: 0,
      channels: parsed,
      suggestions: [],
      version: 1,
    };
  }
  window.NovaCanvas = {
    init: init,
    loadPlan: loadPlan,
    getState: function () {
      return state.canvasState;
    },
  };
})();
