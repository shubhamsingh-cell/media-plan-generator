(function () {
  "use strict";

  // ========================================================================
  // CONSTANTS & STATE
  // ========================================================================
  var API_URL = "/api/chat";
  var STREAM_URL = "/api/chat/stream";
  var WS_URL =
    (location.protocol === "https:" ? "wss://" : "ws://") +
    location.host +
    "/ws/chat";
  var FEEDBACK_URL = "/api/chat/feedback";
  var STOP_URL = "/api/chat/stop";
  var MAX_CHARS = 4000;
  var MAX_FILE_SIZE = 5 * 1024 * 1024; // 5MB
  var STORAGE_KEY = "nova_conversations";
  var ACTIVE_KEY = "nova_active_conv";
  var THEME_KEY = "nova_theme";
  var MAX_HISTORY = 20;
  var MAX_STORED_MESSAGES = 100;

  var state = {
    conversations: {},
    activeConvId: null,
    isLoading: false,
    isStreaming: false,
    abortController: null,
    _loadStart: 0,
    queryCount: 0,
    sidebarOpen: true,
    pendingFile: null, // {name, size, type, data(base64)}
  };

  // ========================================================================
  // DOM REFERENCES
  // ========================================================================
  var chatContainer = document.getElementById("chat-container");
  var chatArea = document.getElementById("chat-area");
  var chatInput = document.getElementById("chat-input");
  var sendBtn = document.getElementById("send-btn");
  var stopBtn = document.getElementById("stop-btn");
  var clearChatBtn = document.getElementById("clear-chat-btn");
  var newChatBtn = document.getElementById("new-chat-btn");
  var sidebarToggle = document.getElementById("sidebar-toggle");
  var sidebar = document.getElementById("sidebar");
  var sidebarOverlay = document.getElementById("sidebar-overlay");
  var convList = document.getElementById("conv-list");
  var charCount = document.getElementById("char-count");
  var toastContainer = document.getElementById("toast-container");
  var themeToggleBtn = document.getElementById("theme-toggle-btn");
  var convSearchInput = document.getElementById("conv-search-input");
  var shareBtn = document.getElementById("share-btn");
  var exportBtn = document.getElementById("export-btn");
  var shortcutsBtn = document.getElementById("shortcuts-btn");
  var attachBtn = document.getElementById("attach-btn");
  var fileInput = document.getElementById("file-input");
  var fileChips = document.getElementById("file-chips");

  // ========================================================================
  // UTILITIES
  // ========================================================================
  function generateId() {
    return "conv-" + Date.now() + "-" + Math.random().toString(36).substr(2, 6);
  }

  function formatTime(ts) {
    var d = new Date(ts);
    var now = new Date();
    var diff = now - d;
    if (diff < 60000) return "Just now";
    if (diff < 3600000) return Math.floor(diff / 60000) + "m ago";
    if (diff < 86400000) return Math.floor(diff / 3600000) + "h ago";
    if (diff < 604800000) return Math.floor(diff / 86400000) + "d ago";
    return d.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
    });
  }

  function formatTimestamp(ts) {
    return new Date(ts).toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    });
  }

  function getGreeting() {
    var h = new Date().getHours();
    return h < 12 ? "morning" : h < 17 ? "afternoon" : "evening";
  }

  function escapeHtml(str) {
    if (!str) return "";
    var el = document.createElement("div");
    el.appendChild(document.createTextNode(String(str)));
    return el.innerHTML;
  }

  // ========================================================================
  // TOAST NOTIFICATIONS
  // ========================================================================
  function showToast(message, type) {
    type = type || "success";
    var toast = document.createElement("div");
    toast.className = "toast " + type;
    toast.textContent = message;
    toastContainer.appendChild(toast);
    setTimeout(function () {
      toast.style.animation = "toast-out 0.3s ease forwards";
      setTimeout(function () {
        toast.remove();
      }, 300);
    }, 2500);
  }

  // ========================================================================
  // TEXT-TO-SPEECH (ElevenLabs)
  // ========================================================================
  var ttsState = {
    autoEnabled: false,
    currentAudio: null,
    currentBtn: null,
  };

  // SVG icon templates for TTS button states
  var TTS_ICON_SPEAKER =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg>';
  var TTS_ICON_STOP =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="6" width="12" height="12" rx="2"/></svg>';
  var TTS_ICON_SPINNER = '<div class="tts-spinner"></div>';

  function stopCurrentTTS() {
    if (ttsState.currentAudio) {
      ttsState.currentAudio.pause();
      ttsState.currentAudio.src = "";
      ttsState.currentAudio = null;
    }
    if (ttsState.currentBtn) {
      ttsState.currentBtn.innerHTML = TTS_ICON_SPEAKER;
      ttsState.currentBtn.classList.remove("tts-loading", "tts-playing");
      ttsState.currentBtn.title = "Read aloud";
      ttsState.currentBtn = null;
    }
  }

  function playTTS(text, btnEl) {
    // If this button is already playing, stop it
    if (ttsState.currentBtn === btnEl && ttsState.currentAudio) {
      stopCurrentTTS();
      return;
    }
    // Stop any other ongoing TTS
    stopCurrentTTS();

    // Strip markdown/HTML for cleaner TTS input
    var cleanText = text
      .replace(/```[\s\S]*?```/g, " code block omitted ")
      .replace(/`[^`]+`/g, "")
      .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
      .replace(/[*_~#>|]+/g, "")
      .replace(/\n{2,}/g, ". ")
      .replace(/\n/g, " ")
      .trim();

    if (!cleanText) {
      showToast("No text to read", "error");
      return;
    }

    // Truncate to 5000 chars for ElevenLabs limit
    if (cleanText.length > 5000) {
      cleanText = cleanText.substring(0, 5000);
    }

    // Set loading state
    btnEl.innerHTML = TTS_ICON_SPINNER;
    btnEl.classList.add("tts-loading");
    btnEl.classList.remove("tts-playing");
    btnEl.title = "Generating audio...";
    ttsState.currentBtn = btnEl;

    fetch("/api/elevenlabs/tts", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": window.__csrfToken,
      },
      body: JSON.stringify({ text: cleanText }),
    })
      .then(function (resp) {
        if (!resp.ok) {
          throw new Error("TTS request failed: HTTP " + resp.status);
        }
        return resp.blob();
      })
      .then(function (blob) {
        // Check if user stopped during loading
        if (ttsState.currentBtn !== btnEl) return;

        var audioUrl = URL.createObjectURL(blob);
        var audio = new Audio(audioUrl);
        ttsState.currentAudio = audio;

        // Switch to playing state
        btnEl.innerHTML = TTS_ICON_STOP;
        btnEl.classList.remove("tts-loading");
        btnEl.classList.add("tts-playing");
        btnEl.title = "Stop playback";

        audio.addEventListener("ended", function () {
          URL.revokeObjectURL(audioUrl);
          if (ttsState.currentBtn === btnEl) {
            btnEl.innerHTML = TTS_ICON_SPEAKER;
            btnEl.classList.remove("tts-playing");
            btnEl.title = "Read aloud";
            ttsState.currentAudio = null;
            ttsState.currentBtn = null;
          }
        });

        audio.addEventListener("error", function () {
          URL.revokeObjectURL(audioUrl);
          if (ttsState.currentBtn === btnEl) {
            btnEl.innerHTML = TTS_ICON_SPEAKER;
            btnEl.classList.remove("tts-loading", "tts-playing");
            btnEl.title = "Read aloud";
            ttsState.currentAudio = null;
            ttsState.currentBtn = null;
          }
          showToast("Audio playback error", "error");
        });

        audio.play().catch(function (playErr) {
          URL.revokeObjectURL(audioUrl);
          stopCurrentTTS();
          showToast("Could not play audio", "error");
        });
      })
      .catch(function (err) {
        if (ttsState.currentBtn === btnEl) {
          btnEl.innerHTML = TTS_ICON_SPEAKER;
          btnEl.classList.remove("tts-loading", "tts-playing");
          btnEl.title = "Read aloud";
          ttsState.currentBtn = null;
        }
        showToast("Text-to-speech unavailable", "error");
      });
  }

  // Auto-TTS: play latest Nova message automatically when enabled
  function autoPlayTTS(messageText) {
    if (!ttsState.autoEnabled) return;
    // Find the last TTS button in the chat
    var allTtsBtns = chatContainer.querySelectorAll(".tts-btn");
    if (allTtsBtns.length === 0) return;
    var lastBtn = allTtsBtns[allTtsBtns.length - 1];
    playTTS(messageText, lastBtn);
  }

  // Wire up auto-TTS toggle button
  (function () {
    var autoTtsBtn = document.getElementById("auto-tts-btn");
    if (!autoTtsBtn) return;
    autoTtsBtn.addEventListener("click", function () {
      ttsState.autoEnabled = !ttsState.autoEnabled;
      autoTtsBtn.classList.toggle("active", ttsState.autoEnabled);
      autoTtsBtn.title = ttsState.autoEnabled
        ? "Auto-read responses (on)"
        : "Auto-read responses (off)";
      if (!ttsState.autoEnabled) {
        stopCurrentTTS();
      }
      showToast(
        ttsState.autoEnabled ? "Auto-read enabled" : "Auto-read disabled",
        "success",
      );
    });
  })();

  // ========================================================================
  // SYNTAX HIGHLIGHTER (CSS-only classes, no external libs)
  // ========================================================================
  function highlightSyntax(code, lang) {
    if (!code) return code;
    var l = (lang || "").toLowerCase();
    var h = code;
    // Comments (must go first to avoid interference)
    if (l === "python" || l === "py") {
      h = h.replace(/(#[^\n]*)/g, '<span class="syn-cmt">$1</span>');
    } else if (
      l === "javascript" ||
      l === "js" ||
      l === "typescript" ||
      l === "ts"
    ) {
      h = h.replace(/(\/\/[^\n]*)/g, '<span class="syn-cmt">$1</span>');
    } else if (l === "json") {
      // JSON has no comments
    } else {
      // Generic: // and # comments
      h = h.replace(/(\/\/[^\n]*|#[^\n]*)/g, '<span class="syn-cmt">$1</span>');
    }
    // Strings (double and single quoted)
    h = h.replace(
      /(&quot;(?:[^&]|&(?!quot;))*?&quot;|&#39;(?:[^&]|&(?!#39;))*?&#39;)/g,
      '<span class="syn-str">$1</span>',
    );
    // Also handle "actual" strings if not HTML-escaped
    h = h.replace(/("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/g, function (m) {
      if (m.indexOf("class=") > -1) return m; // skip existing HTML
      return '<span class="syn-str">' + m + "</span>";
    });
    // Numbers
    h = h.replace(/\b(\d+\.?\d*)\b/g, '<span class="syn-num">$1</span>');
    // Booleans/null
    h = h.replace(
      /\b(true|false|null|None|True|False|undefined|nil)\b/g,
      '<span class="syn-bool">$1</span>',
    );
    // Keywords per language
    var kws = [];
    if (l === "python" || l === "py") {
      kws = [
        "def",
        "class",
        "if",
        "elif",
        "else",
        "for",
        "while",
        "return",
        "import",
        "from",
        "as",
        "with",
        "try",
        "except",
        "finally",
        "raise",
        "yield",
        "async",
        "await",
        "lambda",
        "pass",
        "break",
        "continue",
        "and",
        "or",
        "not",
        "in",
        "is",
      ];
    } else if (
      l === "javascript" ||
      l === "js" ||
      l === "typescript" ||
      l === "ts"
    ) {
      kws = [
        "function",
        "const",
        "let",
        "var",
        "if",
        "else",
        "for",
        "while",
        "return",
        "import",
        "export",
        "from",
        "class",
        "new",
        "this",
        "async",
        "await",
        "try",
        "catch",
        "finally",
        "throw",
        "typeof",
        "instanceof",
        "switch",
        "case",
        "default",
        "break",
        "continue",
        "yield",
        "of",
        "in",
      ];
    } else if (l === "sql") {
      kws = [
        "SELECT",
        "FROM",
        "WHERE",
        "INSERT",
        "INTO",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "TABLE",
        "ALTER",
        "DROP",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "ON",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "AS",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "UNION",
        "INDEX",
        "PRIMARY",
        "KEY",
        "FOREIGN",
        "REFERENCES",
        "VALUES",
        "DISTINCT",
        "COUNT",
        "SUM",
        "AVG",
        "MAX",
        "MIN",
      ];
    } else if (l === "bash" || l === "sh" || l === "shell") {
      kws = [
        "if",
        "then",
        "else",
        "elif",
        "fi",
        "for",
        "while",
        "do",
        "done",
        "case",
        "esac",
        "function",
        "return",
        "echo",
        "exit",
        "export",
        "source",
        "cd",
        "ls",
        "grep",
        "awk",
        "sed",
        "cat",
        "mkdir",
        "rm",
        "cp",
        "mv",
      ];
    } else {
      kws = [
        "function",
        "class",
        "if",
        "else",
        "for",
        "while",
        "return",
        "import",
        "from",
        "const",
        "let",
        "var",
        "def",
        "try",
        "catch",
        "except",
        "finally",
        "throw",
        "new",
        "this",
        "async",
        "await",
      ];
    }
    if (kws.length > 0) {
      var kwPat = new RegExp("\\b(" + kws.join("|") + ")\\b", "g");
      h = h.replace(kwPat, function (m) {
        // Don't highlight inside already-tagged spans
        return '<span class="syn-kw">' + m + "</span>";
      });
    }
    // Function calls
    h = h.replace(/\b([a-zA-Z_]\w*)\s*\(/g, '<span class="syn-fn">$1</span>(');
    return h;
  }

  // ========================================================================
  // MARKDOWN RENDERER (vanilla JS, no deps) -- with syntax highlighting
  // ========================================================================
  function renderMarkdown(text) {
    if (!text) return "";
    var h = escapeHtml(text);

    // Code blocks with language + syntax highlighting
    h = h.replace(/```(\w+)?\n([\s\S]*?)```/g, function (match, lang, code) {
      var langLabel = lang ? escapeHtml(lang) : "code";
      var highlighted = highlightSyntax(code.trim(), lang);
      return (
        '<div class="code-block-wrapper"><div class="code-block-header"><span>' +
        langLabel +
        '</span><button class="code-copy-btn" onclick="window.__novaCopyCode(this)" aria-label="Copy code">Copy</button></div>' +
        "<pre><code>" +
        highlighted +
        "</code></pre></div>"
      );
    });

    // Tables
    h = h.replace(
      /^(\|.+\|)\n(\|[-:\| ]+\|)\n((?:\|.+\|\n?)*)/gm,
      function (m, header, sep, body) {
        var hc = header.split("|").filter(function (c) {
          return c.trim() !== "";
        });
        var rows = body.trim().split("\n");
        var t = "<table><thead><tr>";
        hc.forEach(function (c) {
          t += "<th>" + c.trim() + "</th>";
        });
        t += "</tr></thead><tbody>";
        rows.forEach(function (r) {
          var cells = r.split("|").filter(function (c) {
            return c.trim() !== "";
          });
          t += "<tr>";
          cells.forEach(function (c) {
            t += "<td>" + c.trim() + "</td>";
          });
          t += "</tr>";
        });
        return t + "</tbody></table>";
      },
    );

    // Headers
    h = h.replace(/^### (.+)$/gm, "<h3>$1</h3>");
    h = h.replace(/^## (.+)$/gm, "<h3>$1</h3>");
    h = h.replace(/^# (.+)$/gm, "<h3>$1</h3>");

    // Bold and italic
    h = h.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
    h = h.replace(/\*(.+?)\*/g, "<em>$1</em>");

    // Inline code (but not inside pre/code blocks)
    h = h.replace(/`([^`]+)`/g, "<code>$1</code>");

    // Ordered lists
    h = h.replace(
      /^(\d+)\.\s+(.+)$/gm,
      '<li class="ol-item" value="$1">$2</li>',
    );
    h = h.replace(
      /((?:<li class="ol-item"[^>]*>.+<\/li>\n?)+)/g,
      "<ol>$1</ol>",
    );

    // Unordered lists
    h = h.replace(/^[-*]\s+(.+)$/gm, "<li>$1</li>");
    h = h.replace(/((?:<li>(?:(?!class=).)+<\/li>\n?)+)/g, "<ul>$1</ul>");

    // Links
    h = h.replace(
      /\[([^\]]+)\]\((https?:\/\/[^\)]+)\)/g,
      '<a href="$2" target="_blank" rel="noopener">$1</a>',
    );

    // Inline citations [1], [2], etc.
    h = h.replace(
      /\[(\d+)\]/g,
      '<span class="citation" data-cite="$1" tabindex="0" role="button" aria-label="Citation $1">$1</span>',
    );

    // Paragraphs
    h = h.replace(/\n\n/g, "</p><p>");
    h = h.replace(/\n/g, "<br/>");
    if (
      h.indexOf("<h3>") !== 0 &&
      h.indexOf("<ul>") !== 0 &&
      h.indexOf("<ol>") !== 0 &&
      h.indexOf("<table>") !== 0 &&
      h.indexOf("<div") !== 0
    ) {
      h = "<p>" + h + "</p>";
    }
    h = h.replace(/<p>\s*<\/p>/g, "");
    h = h.replace(/<p>\s*<br\/>\s*<\/p>/g, "");

    return h;
  }

  // Copy code block handler
  window.__novaCopyCode = function (btn) {
    var pre = btn.closest(".code-block-wrapper").querySelector("pre code");
    if (!pre) return;
    navigator.clipboard.writeText(pre.textContent).then(function () {
      btn.textContent = "Copied!";
      setTimeout(function () {
        btn.textContent = "Copy";
      }, 1500);
    });
  };

  // ========================================================================
  // CONVERSATION PERSISTENCE (localStorage)
  // ========================================================================
  function loadConversations() {
    try {
      var stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        var loaded = JSON.parse(stored);
        // Clean up conversations corrupted by timed-out sessions:
        // Remove empty assistant messages (content is null/undefined/"")
        Object.keys(loaded).forEach(function (id) {
          var conv = loaded[id];
          if (conv && Array.isArray(conv.messages)) {
            conv.messages = conv.messages.filter(function (m) {
              return !(m.role === "assistant" && !m.content);
            });
          }
        });
        state.conversations = loaded;
      }
      state.activeConvId = localStorage.getItem(ACTIVE_KEY) || null;
    } catch (e) {
      state.conversations = {};
    }
  }

  function safeSaveToStorage(key, value) {
    try {
      localStorage.setItem(key, value);
      return true;
    } catch (e) {
      if (e && e.name === "QuotaExceededError") {
        // Prune oldest conversations beyond the last 20
        var ids = Object.keys(state.conversations);
        if (ids.length > MAX_HISTORY) {
          ids.sort(function (a, b) {
            var tA =
              state.conversations[a].updatedAt ||
              state.conversations[a].createdAt ||
              0;
            var tB =
              state.conversations[b].updatedAt ||
              state.conversations[b].createdAt ||
              0;
            return tA - tB;
          });
          var toRemove = ids.slice(0, ids.length - MAX_HISTORY);
          toRemove.forEach(function (id) {
            delete state.conversations[id];
          });
        }
        // Retry with pruned data
        try {
          if (key === STORAGE_KEY) {
            value = JSON.stringify(buildSavePayload());
          }
          localStorage.setItem(key, value);
          showToast("Storage full \u2014 older conversations removed", "error");
          return true;
        } catch (retryErr) {
          showToast("Storage full \u2014 older conversations removed", "error");
          return false;
        }
      }
      return false;
    }
  }

  function buildSavePayload() {
    var toSave = {};
    Object.keys(state.conversations).forEach(function (id) {
      var conv = state.conversations[id];
      toSave[id] = {
        id: conv.id,
        title: conv.title,
        messages: conv.messages.slice(-MAX_STORED_MESSAGES),
        createdAt: conv.createdAt,
        updatedAt: conv.updatedAt,
      };
    });
    return toSave;
  }

  // Debounced save to prevent race conditions from concurrent writes.
  // Multiple async paths (streaming completion, auto-title, sidebar
  // render) can trigger saveConversations() within the same tick;
  // debouncing coalesces them into one localStorage write.
  var _saveTimer = null;
  var _SAVE_DEBOUNCE_MS = 200;

  function _doSaveConversations() {
    var payload = JSON.stringify(buildSavePayload());
    safeSaveToStorage(STORAGE_KEY, payload);
    safeSaveToStorage(ACTIVE_KEY, state.activeConvId || "");
  }

  function saveConversations() {
    if (_saveTimer) clearTimeout(_saveTimer);
    _saveTimer = setTimeout(function () {
      _saveTimer = null;
      _doSaveConversations();
    }, _SAVE_DEBOUNCE_MS);
  }

  // Immediate flush -- use for destructive or navigational actions
  // (delete, switch, clear, beforeunload) where data must persist now.
  function flushSaveConversations() {
    if (_saveTimer) {
      clearTimeout(_saveTimer);
      _saveTimer = null;
    }
    _doSaveConversations();
  }

  function getActiveConv() {
    if (!state.activeConvId || !state.conversations[state.activeConvId]) {
      return null;
    }
    return state.conversations[state.activeConvId];
  }

  function createConversation() {
    var id = generateId();
    state.conversations[id] = {
      id: id,
      title: "New Chat",
      messages: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    state.activeConvId = id;
    saveConversations();
    renderSidebar();
    renderChat();
    return id;
  }

  function switchConversation(id) {
    if (!state.conversations[id]) return;
    state.activeConvId = id;
    flushSaveConversations();
    renderSidebar();
    renderChat();
    recalcTokensForConv(state.conversations[id]);
  }

  function deleteConversation(id) {
    delete state.conversations[id];
    if (state.activeConvId === id) {
      var keys = Object.keys(state.conversations);
      state.activeConvId = keys.length > 0 ? keys[keys.length - 1] : null;
    }
    flushSaveConversations();
    renderSidebar();
    renderChat();
  }

  // Auto-title from first user message
  function updateConvTitle(conv) {
    if (!conv || conv.title !== "New Chat") return;
    var firstUser = conv.messages.find(function (m) {
      return m.role === "user";
    });
    if (firstUser) {
      var title = firstUser.content.substring(0, 50);
      if (firstUser.content.length > 50) title += "...";
      conv.title = title;
      saveConversations();
      renderSidebar();
    }
  }

  // ========================================================================
  // SIDEBAR RENDERING
  // ========================================================================
  function renderSidebar() {
    convList.innerHTML = "";
    var searchTerm = (convSearchInput ? convSearchInput.value : "")
      .trim()
      .toLowerCase();
    var keys = Object.keys(state.conversations).sort(function (a, b) {
      return (
        (state.conversations[b].updatedAt || 0) -
        (state.conversations[a].updatedAt || 0)
      );
    });
    // Filter by search term -- searches title AND all message content
    if (searchTerm) {
      keys = keys.filter(function (id) {
        var conv = state.conversations[id];
        if ((conv.title || "").toLowerCase().indexOf(searchTerm) !== -1)
          return true;
        // Search across ALL messages (user + assistant)
        var msgs = conv.messages || [];
        for (var mi = 0; mi < msgs.length; mi++) {
          var msgContent = (msgs[mi].content || "").toLowerCase();
          if (msgContent.indexOf(searchTerm) !== -1) return true;
        }
        return false;
      });
    }
    if (keys.length === 0) {
      var empty = document.createElement("div");
      empty.style.cssText =
        "padding:20px 12px;text-align:center;font-size:13px;color:var(--text-muted);";
      empty.textContent = searchTerm
        ? "No matching conversations"
        : "No conversations yet";
      convList.appendChild(empty);
      return;
    }
    keys.forEach(function (id) {
      var conv = state.conversations[id];
      var item = document.createElement("div");
      item.className =
        "conv-item" + (id === state.activeConvId ? " active" : "");
      item.setAttribute("role", "listitem");
      item.setAttribute("tabindex", "0");
      // Highlight title when search matches
      var displayTitle = escapeHtml(conv.title);
      var matchSnippet = "";
      if (searchTerm) {
        // Highlight matching text in title
        var titleLower = (conv.title || "").toLowerCase();
        var idx = titleLower.indexOf(searchTerm);
        if (idx !== -1) {
          var before = escapeHtml(conv.title.substring(0, idx));
          var match = escapeHtml(
            conv.title.substring(idx, idx + searchTerm.length),
          );
          var after = escapeHtml(conv.title.substring(idx + searchTerm.length));
          displayTitle =
            before +
            '<mark style="background:rgba(107,179,205,0.3);color:inherit;border-radius:2px;padding:0 1px">' +
            match +
            "</mark>" +
            after;
        }
        // Show matching message snippet if title didn't match
        if (idx === -1) {
          var msgs = conv.messages || [];
          for (var si = 0; si < msgs.length; si++) {
            var mc = (msgs[si].content || "").toLowerCase();
            var sIdx = mc.indexOf(searchTerm);
            if (sIdx !== -1) {
              var snippetStart = Math.max(0, sIdx - 20);
              var raw = msgs[si].content.substring(
                snippetStart,
                sIdx + searchTerm.length + 30,
              );
              matchSnippet =
                (snippetStart > 0 ? "..." : "") +
                escapeHtml(raw.trim()) +
                "...";
              break;
            }
          }
        }
      }

      item.innerHTML =
        '<svg class="conv-item-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>' +
        '<div class="conv-item-text">' +
        '<div class="conv-item-title">' +
        displayTitle +
        "</div>" +
        (matchSnippet
          ? '<div class="conv-item-snippet" style="font-size:11px;color:var(--text-muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px">' +
            matchSnippet +
            "</div>"
          : "") +
        '<div class="conv-item-date">' +
        formatTime(conv.updatedAt || conv.createdAt) +
        "</div>" +
        "</div>" +
        '<button class="conv-item-delete" data-id="' +
        id +
        '" aria-label="Delete conversation" title="Delete"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>';

      item.addEventListener("click", function (e) {
        if (e.target.closest(".conv-item-delete")) return;
        switchConversation(id);
        closeSidebarMobile();
      });
      item
        .querySelector(".conv-item-delete")
        .addEventListener("click", function (e) {
          e.stopPropagation();
          deleteConversation(id);
        });
      item.addEventListener("keydown", function (e) {
        if (e.key === "Enter") {
          switchConversation(id);
          closeSidebarMobile();
        }
      });
      convList.appendChild(item);
    });
  }

  // ========================================================================
  // CHAT RENDERING
  // ========================================================================
  function renderChat() {
    chatContainer.innerHTML = "";
    var conv = getActiveConv();
    if (!conv || conv.messages.length === 0) {
      renderWelcome();
      return;
    }
    conv.messages.forEach(function (msg) {
      appendMessageDOM(msg, false);
    });
    scrollToBottom();
  }

  function renderWelcome() {
    var welcome = document.createElement("div");
    welcome.className = "welcome-screen";
    welcome.id = "welcome-screen";
    welcome.innerHTML =
      '<div class="welcome-orb-container"><div class="welcome-orb" aria-hidden="true"></div></div>' +
      '<div class="welcome-title">Good ' +
      getGreeting() +
      ". How can I help?</div>" +
      '<div class="welcome-subtitle">Senior recruitment marketing analyst with access to real BLS, FRED, Adzuna, and O*NET data, 10,238+ publishers, 200+ occupation benchmarks, and 91 platform profiles.</div>' +
      '<div class="suggestions-grid">' +
      '<button class="suggestion-card" data-query="Create a media plan for 50 software engineers in Austin, TX with a $200K budget">' +
      '<span class="suggestion-card-icon" aria-hidden="true">&#x1F4CB;</span>Create a media plan for 50 software engineers in Austin' +
      "</button>" +
      '<button class="suggestion-card" data-query="What\'s the competitive landscape for Indeed vs LinkedIn for engineering roles?">' +
      '<span class="suggestion-card-icon" aria-hidden="true">&#x2696;</span>Indeed vs LinkedIn competitive landscape' +
      "</button>" +
      '<button class="suggestion-card" data-query="Audit my job posting for compliance issues in California">' +
      '<span class="suggestion-card-icon" aria-hidden="true">&#x1F6E1;</span>Audit a job posting for compliance' +
      "</button>" +
      '<button class="suggestion-card" data-query="What salary should I offer for a Data Scientist in New York City?">' +
      '<span class="suggestion-card-icon" aria-hidden="true">&#x1F4B0;</span>Data Scientist salary in NYC' +
      "</button>" +
      "</div>";
    chatContainer.appendChild(welcome);

    // Suggestion click handlers
    welcome.querySelectorAll(".suggestion-card").forEach(function (btn) {
      btn.addEventListener("click", function () {
        chatInput.value = btn.getAttribute("data-query");
        sendMessage();
      });
    });
  }

  function appendMessageDOM(msg, animate) {
    // Remove welcome screen if present
    var ws = document.getElementById("welcome-screen");
    if (ws) ws.remove();

    var wrapper = document.createElement("div");
    wrapper.className = "message";
    if (animate === false) {
      wrapper.style.animation = "none";
      wrapper.style.opacity = "1";
    } else {
      // Safety net: force opacity if CSS animation fails to fire
      setTimeout(function () {
        wrapper.style.opacity = "1";
      }, 350);
    }

    var isUser = msg.role === "user";

    // Avatar
    var avatar = document.createElement("div");
    avatar.className = "message-avatar " + (isUser ? "user" : "nova");
    avatar.textContent = isUser ? "U" : "N";
    avatar.setAttribute("aria-hidden", "true");

    // Body
    var body = document.createElement("div");
    body.className = "message-body";

    // Sender name
    var sender = document.createElement("div");
    sender.className = "message-sender " + (isUser ? "user" : "nova");
    sender.textContent = isUser ? "You" : "Nova";

    // Content
    var content = document.createElement("div");
    content.className = "message-content";
    if (isUser) {
      content.textContent = msg.content;
    } else {
      content.innerHTML = renderMarkdown(msg.content);
    }

    body.appendChild(sender);
    body.appendChild(content);

    // Source badges and confidence (assistant only)
    if (!isUser) {
      var sources = msg.sources || [];
      var confidence = msg.confidence;
      if (
        sources.length > 0 ||
        (typeof confidence === "number" && confidence > 0)
      ) {
        var meta = document.createElement("div");
        meta.className = "message-meta";
        sources.forEach(function (src) {
          var b = document.createElement("span");
          b.className = "meta-badge";
          b.textContent = src;
          meta.appendChild(b);
        });
        if (typeof confidence === "number" && confidence > 0) {
          var pct = Math.round(confidence * 100);
          var cls = pct >= 75 ? "high" : pct >= 50 ? "medium" : "low";
          var cb = document.createElement("span");
          cb.className = "conf-badge conf-" + cls;
          var bd = msg.confidence_breakdown;
          if (bd && bd.grade) {
            cb.textContent = "Grade " + bd.grade + " \u2022 " + pct + "%";
          } else {
            cb.textContent = pct + "% confidence";
          }
          meta.appendChild(cb);
        }
        body.appendChild(meta);
      }

      // Action buttons (assistant only)
      var actions = document.createElement("div");
      actions.className = "message-actions";
      actions.innerHTML =
        '<button class="msg-action-btn copy-btn" aria-label="Copy message"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>Copy</button>' +
        '<button class="msg-action-btn thumbs-up-btn" aria-label="Thumbs up"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3zM7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3"/></svg></button>' +
        '<button class="msg-action-btn thumbs-down-btn" aria-label="Thumbs down"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 15v4a3 3 0 0 0 3 3l4-9V2H5.72a2 2 0 0 0-2 1.7l-1.38 9a2 2 0 0 0 2 2.3zm7-13h2.67A2.31 2.31 0 0 1 22 4v7a2.31 2.31 0 0 1-2.33 2H17"/></svg></button>';

      // Wire copy button
      actions.querySelector(".copy-btn").addEventListener("click", function () {
        navigator.clipboard.writeText(msg.content).then(function () {
          showToast("Copied to clipboard", "success");
        });
      });

      // Wire thumbs up -- call feedback API
      actions
        .querySelector(".thumbs-up-btn")
        .addEventListener("click", function () {
          var btn = this;
          btn.classList.toggle("active");
          actions.querySelector(".thumbs-down-btn").classList.remove("active");
          // Remove any existing feedback popup
          var existingPopup = body.querySelector(".feedback-popup");
          if (existingPopup) existingPopup.remove();
          if (btn.classList.contains("active")) {
            sendFeedback(msg.message_id || "", "up", conv ? conv.id : "");
          }
        });
      // Wire thumbs down -- call feedback API + show text popup
      actions
        .querySelector(".thumbs-down-btn")
        .addEventListener("click", function () {
          var btn = this;
          btn.classList.toggle("active");
          actions.querySelector(".thumbs-up-btn").classList.remove("active");
          // Remove any existing feedback popup
          var existingPopup = body.querySelector(".feedback-popup");
          if (existingPopup) existingPopup.remove();
          if (btn.classList.contains("active")) {
            sendFeedback(msg.message_id || "", "down", conv ? conv.id : "");
            // Show text feedback popup
            var popup = document.createElement("div");
            popup.className = "feedback-popup";
            popup.innerHTML =
              '<textarea placeholder="What went wrong? (optional)" aria-label="Feedback details"></textarea>' +
              '<div class="feedback-popup-actions">' +
              '<button class="feedback-popup-btn secondary" data-action="cancel">Cancel</button>' +
              '<button class="feedback-popup-btn primary" data-action="submit">Submit</button>' +
              "</div>";
            body.appendChild(popup);
            popup.querySelector("textarea").focus();
            popup
              .querySelector('[data-action="cancel"]')
              .addEventListener("click", function () {
                popup.remove();
              });
            popup
              .querySelector('[data-action="submit"]')
              .addEventListener("click", function () {
                var feedbackText = popup.querySelector("textarea").value.trim();
                if (feedbackText) {
                  sendFeedback(
                    msg.message_id || "",
                    "down",
                    conv ? conv.id : "",
                    feedbackText,
                  );
                }
                popup.remove();
                showToast("Thanks for your feedback", "success");
              });
          }
        });

      body.appendChild(actions);

      // TTS (text-to-speech) button row
      var ttsRow = document.createElement("div");
      ttsRow.className = "message-tts-row";
      var ttsBtn = document.createElement("button");
      ttsBtn.className = "tts-btn";
      ttsBtn.setAttribute("aria-label", "Read aloud");
      ttsBtn.title = "Read aloud";
      ttsBtn.innerHTML =
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 0 1 0 7.07"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/></svg>';
      ttsBtn.addEventListener("click", function () {
        playTTS(msg.content, ttsBtn);
      });
      ttsRow.appendChild(ttsBtn);
      body.appendChild(ttsRow);
    }

    // Timestamp
    if (msg.timestamp) {
      var ts = document.createElement("div");
      ts.className = "message-time";
      ts.textContent = formatTimestamp(msg.timestamp);
      body.appendChild(ts);
    }

    wrapper.appendChild(avatar);
    wrapper.appendChild(body);
    chatContainer.appendChild(wrapper);
  }

  function scrollToBottom() {
    requestAnimationFrame(function () {
      chatArea.scrollTop = chatArea.scrollHeight;
    });
  }

  // ========================================================================
  // THINKING INDICATOR
  // ========================================================================
  var thinkingTexts = [
    "Analyzing your question...",
    "Searching 10,238+ publishers...",
    "Querying salary & labor data...",
    "Cross-referencing benchmarks...",
    "Building data-driven response...",
  ];

  function showThinking() {
    var existing = document.getElementById("thinking-wrapper");
    if (existing) existing.remove();

    var wrapper = document.createElement("div");
    wrapper.className = "thinking-wrapper";
    wrapper.id = "thinking-wrapper";
    wrapper.innerHTML =
      '<div class="message-avatar nova" aria-hidden="true">N</div>' +
      '<div class="thinking-bubble">' +
      '<div class="thinking-dots-pill" aria-label="Nova is thinking">' +
      '<div class="thinking-dot"></div>' +
      '<div class="thinking-dot"></div>' +
      '<div class="thinking-dot"></div>' +
      "</div>" +
      '<div class="thinking-text"><span id="thinking-text">' +
      thinkingTexts[0] +
      '</span><span class="thinking-elapsed" id="thinking-elapsed"></span></div>' +
      '<div class="skeleton-line"></div>' +
      '<div class="skeleton-line"></div>' +
      '<div class="skeleton-line"></div>' +
      "</div>";
    chatContainer.appendChild(wrapper);
    scrollToBottom();

    // Cycle thinking text
    var idx = 0;
    wrapper._thinkInterval = setInterval(function () {
      idx = (idx + 1) % thinkingTexts.length;
      var el = document.getElementById("thinking-text");
      if (el) el.textContent = thinkingTexts[idx];
    }, 2500);

    // Elapsed timer (show after 2s to avoid flicker on fast responses)
    var startTime = Date.now();
    wrapper._elapsedInterval = setInterval(function () {
      var elapsed = Math.round((Date.now() - startTime) / 1000);
      var el = document.getElementById("thinking-elapsed");
      if (el && elapsed >= 2) {
        el.textContent = elapsed + "s";
      }
    }, 1000);
  }

  function hideThinking() {
    var el = document.getElementById("thinking-wrapper");
    if (el) {
      if (el._thinkInterval) clearInterval(el._thinkInterval);
      if (el._elapsedInterval) clearInterval(el._elapsedInterval);
      el.remove();
    }
  }

  // ========================================================================
  // TOOL STATUS INDICATOR (S18)
  // ========================================================================
  var _activeTools = {};

  function showToolStatus(type, toolName, label) {
    var container = document.getElementById("tool-status-container");
    if (!container) {
      container = document.createElement("div");
      container.id = "tool-status-container";
      container.className = "nova-tool-status-container";
      // Insert before thinking wrapper or at end of chat
      var thinkingEl = document.getElementById("thinking-wrapper");
      if (thinkingEl) {
        thinkingEl.parentNode.insertBefore(container, thinkingEl);
      } else {
        chatContainer.appendChild(container);
      }
    }

    if (type === "tool_start") {
      _activeTools[toolName] = true;
      // Create or update the tool status pill
      var pillId = "tool-pill-" + toolName.replace(/[^a-zA-Z0-9]/g, "_");
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
      scrollToBottom();
    } else if (type === "tool_complete") {
      delete _activeTools[toolName];
      var pillId2 = "tool-pill-" + toolName.replace(/[^a-zA-Z0-9]/g, "_");
      var pill2 = document.getElementById(pillId2);
      if (pill2) {
        pill2.className = "nova-tool-pill nova-tool-pill-done";
        pill2.innerHTML =
          '<span class="nova-tool-check">&#10003;</span>' +
          '<span class="nova-tool-label">' +
          escapeHtml(label) +
          "</span>";
        // Fade out completed pills after 1.5s
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

  function clearToolStatus() {
    _activeTools = {};
    var container = document.getElementById("tool-status-container");
    if (container) container.remove();
  }

  // ========================================================================
  // FEEDBACK API
  // ========================================================================
  function sendFeedback(messageId, rating, conversationId, textFeedback) {
    if (!messageId && !conversationId) return;
    var payload = {
      message_id: messageId || "msg-" + Date.now(),
      rating: rating,
      conversation_id: conversationId || "",
    };
    if (textFeedback) payload.feedback_text = textFeedback;
    try {
      fetch(FEEDBACK_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": window.__csrfToken,
        },
        body: JSON.stringify(payload),
      }).catch(function () {});
    } catch (e) {}
  }

  // ========================================================================
  // FILE UPLOAD HANDLING
  // ========================================================================
  attachBtn.addEventListener("click", function () {
    fileInput.click();
  });

  fileInput.addEventListener("change", function () {
    var file = fileInput.files[0];
    if (!file) return;
    if (file.size > MAX_FILE_SIZE) {
      showToast("File too large (max 5MB)", "error");
      fileInput.value = "";
      return;
    }
    var validTypes = [".pdf", ".csv", ".xlsx", ".txt", ".json", ".docx"];
    var ext = "." + file.name.split(".").pop().toLowerCase();
    if (validTypes.indexOf(ext) === -1) {
      showToast(
        "Unsupported file type. Use: PDF, CSV, XLSX, TXT, JSON, DOCX",
        "error",
      );
      fileInput.value = "";
      return;
    }
    var reader = new FileReader();
    reader.onload = function (e) {
      state.pendingFile = {
        name: file.name,
        size: file.size,
        type: file.type || ext,
        data: e.target.result.split(",")[1] || "", // base64 part
      };
      renderFileChip();
    };
    reader.readAsDataURL(file);
  });

  function renderFileChip() {
    fileChips.innerHTML = "";
    if (!state.pendingFile) return;
    var sizeStr =
      state.pendingFile.size < 1024
        ? state.pendingFile.size + " B"
        : state.pendingFile.size < 1048576
          ? (state.pendingFile.size / 1024).toFixed(1) + " KB"
          : (state.pendingFile.size / 1048576).toFixed(1) + " MB";
    var chip = document.createElement("div");
    chip.className = "file-chip";
    chip.innerHTML =
      escapeHtml(state.pendingFile.name) +
      ' <span style="opacity:0.6">(' +
      sizeStr +
      ")</span>" +
      '<button class="file-chip-remove" title="Remove file" aria-label="Remove file"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>';
    chip
      .querySelector(".file-chip-remove")
      .addEventListener("click", function () {
        state.pendingFile = null;
        fileInput.value = "";
        renderFileChip();
      });
    fileChips.appendChild(chip);
  }

  // ========================================================================
  // SEND MESSAGE (uses streaming via SSE)
  // ========================================================================
  // Centralized UI reset -- single source of truth for unlocking the chat
  function resetLoadingState() {
    try {
      var wasLoading = state.isLoading;
      state.isLoading = false;
      state.isStreaming = false;
      state._loadStart = 0;
      if (state.abortController) {
        try {
          state.abortController.abort();
        } catch (_) {}
      }
      state.abortController = null;
      sendBtn.style.display = "";
      sendBtn.disabled = !chatInput.value.trim();
      stopBtn.classList.remove("visible");
      hideThinking();
      clearToolStatus();
      chatInput.focus();
      if (wasLoading) {
        /* S23: removed debug console.log */
      }
    } catch (e) {
      // Nuclear option: if DOM ops fail, at least clear the state flags
      state.isLoading = false;
      state.isStreaming = false;
      state._loadStart = 0;
      state.abortController = null;
      console.error("[Nova] resetLoadingState error:", e);
    }
  }

  // ========================================================================
  // WEBSOCKET TRANSPORT (preferred over SSE)
  // ========================================================================
  var _wsConn = null;
  var _wsReady = false;
  var _wsReconnectTimer = null;
  var _wsFailCount = 0;
  var _WS_MAX_FAILS = 3; // Fall back to SSE after N consecutive WS failures

  function _getOrCreateWS(onReady) {
    // If WebSocket is not supported or we've failed too many times, skip
    if (!window.WebSocket || _wsFailCount >= _WS_MAX_FAILS) {
      onReady(null);
      return;
    }
    // If already connected and ready, reuse
    if (_wsConn && _wsConn.readyState === WebSocket.OPEN) {
      onReady(_wsConn);
      return;
    }
    // If connecting, wait
    if (_wsConn && _wsConn.readyState === WebSocket.CONNECTING) {
      var _waitCount = 0;
      var _waitInterval = setInterval(function () {
        _waitCount++;
        if (_wsConn && _wsConn.readyState === WebSocket.OPEN) {
          clearInterval(_waitInterval);
          onReady(_wsConn);
        } else if (
          _waitCount > 30 ||
          !_wsConn ||
          _wsConn.readyState > WebSocket.OPEN
        ) {
          clearInterval(_waitInterval);
          onReady(null);
        }
      }, 100);
      return;
    }
    // Create new connection
    try {
      _wsConn = new WebSocket(WS_URL);
      _wsConn.onopen = function () {
        _wsReady = true;
        _wsFailCount = 0;
        onReady(_wsConn);
      };
      _wsConn.onerror = function () {
        _wsFailCount++;
        _wsReady = false;
        onReady(null);
      };
      _wsConn.onclose = function () {
        _wsReady = false;
        _wsConn = null;
      };
    } catch (e) {
      _wsFailCount++;
      onReady(null);
    }
  }

  function _streamViaWebSocket(ws, payload, callbacks) {
    var fullText = "";
    var metadata = {};
    var _done = false;
    var _wsTimeout = null;

    function resetWsTimeout() {
      if (_wsTimeout) clearTimeout(_wsTimeout);
      _wsTimeout = setTimeout(function () {
        // No data for 25s -- treat as failure
        if (!_done) {
          _done = true;
          callbacks.onError("WebSocket timeout -- no data received");
        }
      }, 25000);
    }

    resetWsTimeout();

    ws.onmessage = function (event) {
      if (_done) return;
      resetWsTimeout();
      try {
        var evt = JSON.parse(event.data);
        // Skip keepalive heartbeats
        if (evt.keepalive) return;
        // Pong responses
        if (evt.type === "pong") return;
        // Error
        if (evt.error && evt.done) {
          _done = true;
          if (_wsTimeout) clearTimeout(_wsTimeout);
          callbacks.onError(evt.error);
          return;
        }
        // Done/final event
        if (evt.done) {
          _done = true;
          if (_wsTimeout) clearTimeout(_wsTimeout);
          metadata = evt;
          callbacks.onComplete(metadata, fullText);
          return;
        }
        // S18: Tool status events (real-time tool progress)
        if (evt.type === "tool_start" || evt.type === "tool_complete") {
          if (callbacks.onToolStatus) {
            callbacks.onToolStatus(evt.type, evt.tool, evt.label);
          }
          return;
        }
        // Status event
        if (evt.status) {
          callbacks.onStatus(evt.status);
          return;
        }
        // Token event
        if (evt.token) {
          fullText += evt.token;
          callbacks.onToken(evt.token, fullText);
        }
      } catch (e) {
        console.warn("[Nova WS] Parse error:", e);
      }
    };

    ws.onerror = function () {
      if (!_done) {
        _done = true;
        if (_wsTimeout) clearTimeout(_wsTimeout);
        callbacks.onError("WebSocket error");
      }
    };

    ws.onclose = function () {
      if (!_done) {
        _done = true;
        if (_wsTimeout) clearTimeout(_wsTimeout);
        if (fullText) {
          // Partial response -- still deliver it
          callbacks.onComplete({}, fullText);
        } else {
          callbacks.onError("WebSocket closed unexpectedly");
        }
      }
      _wsConn = null;
      _wsReady = false;
    };

    // Send the chat message
    ws.send(
      JSON.stringify({
        type: "chat",
        message: payload.message,
        conversation_id: payload.conversation_id,
        history: payload.history,
        session_token: payload.session_token || "",
        file_attachment: payload.file_attachment || null,
      }),
    );

    // Return a cancel function
    return function cancelWS() {
      if (!_done) {
        _done = true;
        if (_wsTimeout) clearTimeout(_wsTimeout);
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

  function sendMessage() {
    var text = chatInput.value.trim();
    // Handle /clear command
    if (text === "/clear") {
      chatInput.value = "";
      updateSendBtn();
      var conv = getActiveConv();
      if (conv) {
        conv.messages = [];
        conv.title = "New Chat";
        conv.updatedAt = Date.now();
        flushSaveConversations();
        renderSidebar();
        renderChat();
      }
      return;
    }
    if (!text || state.isLoading) return;
    if (text.length > MAX_CHARS) {
      showToast("Message is too long (max " + MAX_CHARS + " chars)", "error");
      return;
    }

    if (!getActiveConv()) createConversation();
    var conv = getActiveConv();

    // Add user message
    var userMsg = { role: "user", content: text, timestamp: Date.now() };
    if (state.pendingFile) {
      userMsg.file_name = state.pendingFile.name;
    }
    conv.messages.push(userMsg);
    conv.updatedAt = Date.now();
    updateConvTitle(conv);
    saveConversations();
    renderSidebar();
    appendMessageDOM(userMsg, true);
    scrollToBottom();

    chatInput.value = "";
    chatInput.style.height = "auto";
    updateCharCount();
    updateSendBtn();

    state.isLoading = true;
    state.isStreaming = true;
    state._loadStart = Date.now();
    state._userCancelled = false;
    sendBtn.disabled = true;
    sendBtn.style.display = "none";
    stopBtn.classList.add("visible");
    showThinking();

    // Build history
    var history = [];
    conv.messages.forEach(function (m) {
      if (m.role === "user" || m.role === "assistant") {
        history.push({ role: m.role, content: m.content });
      }
    });

    var payload = {
      message: text,
      conversation_id: conv.id,
      history: history.slice(-MAX_HISTORY),
    };
    // Attach file if present
    if (state.pendingFile) {
      payload.file_attachment = {
        name: state.pendingFile.name,
        type: state.pendingFile.type,
        data: state.pendingFile.data,
      };
      state.pendingFile = null;
      fileInput.value = "";
      renderFileChip();
    }

    // ── Create streaming message DOM (shared by WS and SSE) ──
    function _createStreamingDOM() {
      var welcomeScreen = document.getElementById("welcome-screen");
      if (welcomeScreen) welcomeScreen.remove();
      var wrapper = document.createElement("div");
      wrapper.className = "message";
      wrapper.id = "streaming-msg";
      var avatar = document.createElement("div");
      avatar.className = "message-avatar nova";
      avatar.textContent = "N";
      avatar.setAttribute("aria-hidden", "true");
      var bodyEl = document.createElement("div");
      bodyEl.className = "message-body";
      var sender = document.createElement("div");
      sender.className = "message-sender nova";
      sender.textContent = "Nova";
      var content = document.createElement("div");
      content.className = "message-content";
      content.id = "streaming-content";
      var cursorEl = document.createElement("span");
      cursorEl.className = "streaming-cursor";
      cursorEl.id = "streaming-cursor";
      content.appendChild(cursorEl);
      bodyEl.appendChild(sender);
      bodyEl.appendChild(content);
      wrapper.appendChild(avatar);
      wrapper.appendChild(bodyEl);
      chatContainer.appendChild(wrapper);
      scrollToBottom();
      return content;
    }

    // ── Finalize stream response (shared by WS and SSE) ──
    function _finalizeStream(metadata, fullText) {
      var cur = document.getElementById("streaming-cursor");
      if (cur) cur.remove();
      state.queryCount++;
      var msgId = metadata.message_id || "msg-" + Date.now();

      var responseContent = metadata.full_response || fullText;
      if (!responseContent || !responseContent.trim()) {
        responseContent =
          "I wasn't able to generate a response for that question. I'm best at *recruitment marketing* topics -- try asking about publishers, benchmarks, budgets, or salary data!";
      }
      var assistantMsg = {
        role: "assistant",
        content: responseContent,
        sources: metadata.sources || [],
        confidence: metadata.confidence || 0,
        tools_used: metadata.tools_used || [],
        message_id: msgId,
        suggested_followups: metadata.suggested_followups || [],
        timestamp: Date.now(),
        token_usage: metadata.token_usage || null,
      };
      conv.messages.push(assistantMsg);
      conv.updatedAt = Date.now();
      saveConversations();

      if (metadata.token_usage) {
        updateTokenIndicator(metadata.token_usage);
      }

      var streamEl = document.getElementById("streaming-msg");
      if (streamEl) streamEl.remove();
      appendMessageDOM(assistantMsg, true);

      autoPlayTTS(assistantMsg.content);

      if (
        assistantMsg.suggested_followups &&
        assistantMsg.suggested_followups.length > 0
      ) {
        renderFollowups(assistantMsg.suggested_followups);
      }

      scrollToBottom();
      resetLoadingState();
    }

    // ── Show error banner (shared) ──
    function _showStreamError(errorMsg) {
      hideThinking();
      var streamEl = document.getElementById("streaming-msg");
      if (streamEl) streamEl.remove();

      var errBanner = document.createElement("div");
      errBanner.className = "error-banner";
      errBanner.innerHTML =
        "<span>" +
        escapeHtml(errorMsg) +
        "</span>" +
        '<button class="retry-btn-inline">Retry</button>';
      chatContainer.appendChild(errBanner);
      scrollToBottom();
      errBanner
        .querySelector(".retry-btn-inline")
        .addEventListener("click", function () {
          errBanner.remove();
          chatInput.value = text;
          sendMessage();
        });
      resetLoadingState();
    }

    // Hard timeout: forcibly reset after 120s no matter what
    // S21: was 60s, increased to match gunicorn --timeout 120
    var hardTimeout = setTimeout(function () {
      resetLoadingState();
      var streamEl = document.getElementById("streaming-msg");
      if (streamEl) streamEl.remove();
    }, 120000);

    // ── Try WebSocket first, fall back to SSE ──
    _getOrCreateWS(function (wsConn) {
      if (wsConn) {
        // ── WebSocket transport ──
        var content = _createStreamingDOM();
        hideThinking();

        var cancelFn = _streamViaWebSocket(wsConn, payload, {
          onToken: function (token, fullText) {
            try {
              content.innerHTML = renderMarkdown(fullText);
            } catch (_) {
              content.textContent = fullText;
            }
            var cur =
              document.getElementById("streaming-cursor") ||
              document.createElement("span");
            cur.className = "streaming-cursor";
            cur.id = "streaming-cursor";
            content.appendChild(cur);
            scrollToBottom();
          },
          onStatus: function (status) {
            // Could show status indicator -- for now just log
          },
          onToolStatus: function (type, tool, label) {
            showToolStatus(type, tool, label);
          },
          onComplete: function (metadata, fullText) {
            clearToolStatus();
            clearTimeout(hardTimeout);
            _finalizeStream(metadata, fullText);
          },
          onError: function (errMsg) {
            clearTimeout(hardTimeout);
            // On WS error, increment fail count
            _wsFailCount++;
            _showStreamError(errMsg || "Connection error. Please try again.");
          },
        });

        // Store cancel function for stop button
        state.abortController = {
          abort: function () {
            cancelFn();
          },
        };
      } else {
        // ── SSE fallback transport ──
        var ac = new AbortController();
        state.abortController = ac;

        var readerTimeout = null;
        function resetReaderTimeout() {
          if (readerTimeout) clearTimeout(readerTimeout);
          // S24: Was 25s — far too short for complex queries (media plans,
          // multi-tool iterations take 40-70s). Increased to 90s to match
          // server-side 75s chat timeout + margin.  Resets on each SSE event.
          readerTimeout = setTimeout(function () {
            if (ac && !ac.signal.aborted) {
              try {
                ac.abort();
              } catch (_) {}
            }
          }, 90000);
        }

        fetch(STREAM_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-CSRF-Token": window.__csrfToken,
          },
          body: JSON.stringify(payload),
          signal: ac.signal,
        })
          .then(function (r) {
            if (!r.ok) throw new Error("HTTP " + r.status);
            hideThinking();

            var content = _createStreamingDOM();
            var reader = r.body.getReader();
            var decoder = new TextDecoder();
            var buffer = "";
            var fullText = "";
            var metadata = {};

            resetReaderTimeout();

            function processChunk() {
              return reader.read().then(function (result) {
                if (result.done) return;
                resetReaderTimeout();
                buffer += decoder.decode(result.value, { stream: true });
                var lines = buffer.split("\n");
                buffer = lines.pop() || "";
                lines.forEach(function (line) {
                  if (line.indexOf("data: ") !== 0) return;
                  var jsonStr = line.substring(6);
                  try {
                    var evt = JSON.parse(jsonStr);
                    if (evt.keepalive) return;
                    if (evt.done) {
                      metadata = evt;
                      clearToolStatus();
                      return;
                    }
                    // S18: Tool status events
                    if (
                      evt.type === "tool_start" ||
                      evt.type === "tool_complete"
                    ) {
                      showToolStatus(evt.type, evt.tool, evt.label);
                      return;
                    }
                    if (evt.token) {
                      // Clear tool status on first real token
                      clearToolStatus();
                      fullText += evt.token;
                      try {
                        content.innerHTML = renderMarkdown(fullText);
                      } catch (_) {
                        content.textContent = fullText;
                      }
                      var cur =
                        document.getElementById("streaming-cursor") ||
                        document.createElement("span");
                      cur.className = "streaming-cursor";
                      cur.id = "streaming-cursor";
                      content.appendChild(cur);
                      scrollToBottom();
                    }
                  } catch (e) {}
                });
                return processChunk();
              });
            }

            return processChunk()
              .catch(function (readErr) {
                if (fullText) return;
                throw readErr;
              })
              .then(function () {
                if (readerTimeout) clearTimeout(readerTimeout);
                _finalizeStream(metadata, fullText);
              });
          })
          .catch(function (err) {
            if (readerTimeout) clearTimeout(readerTimeout);
            var errorMsg;
            if (err.name === "AbortError") {
              errorMsg = state._userCancelled
                ? "Response was stopped."
                : "Request timed out. Please try again.";
            } else if (err.message && err.message.indexOf("429") > -1) {
              errorMsg = "Nova is busy. Please wait a moment and try again.";
            } else if (err.message && err.message.indexOf("403") > -1) {
              errorMsg = "Session expired. Please refresh the page.";
            } else {
              errorMsg = "Connection error. Please try again.";
            }
            _showStreamError(errorMsg);
          })
          .finally(function () {
            clearTimeout(hardTimeout);
            if (readerTimeout) clearTimeout(readerTimeout);
          });
      }
    });
  }

  // ========================================================================
  // STOP GENERATION
  // ========================================================================
  stopBtn.addEventListener("click", function () {
    state._userCancelled = true;
    resetLoadingState();
    // Remove streaming message if present
    var streamEl = document.getElementById("streaming-msg");
    if (streamEl) streamEl.remove();
    var conv = getActiveConv();
    if (conv) {
      fetch(STOP_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": window.__csrfToken,
        },
        body: JSON.stringify({ conversation_id: conv.id }),
      }).catch(function () {});
    }
  });

  // ========================================================================
  // SUGGESTED FOLLOW-UPS
  // ========================================================================
  function renderFollowups(questions) {
    if (!questions || questions.length === 0) return;
    var container = document.createElement("div");
    container.className = "followup-pills";
    questions.forEach(function (q) {
      var pill = document.createElement("button");
      pill.className = "followup-pill";
      pill.textContent = q;
      pill.addEventListener("click", function () {
        container.remove();
        chatInput.value = q;
        sendMessage();
      });
      container.appendChild(pill);
    });
    chatContainer.appendChild(container);
    scrollToBottom();
  }

  // ========================================================================
  // INPUT HANDLERS
  // ========================================================================
  function updateSendBtn() {
    sendBtn.disabled = !chatInput.value.trim() || state.isLoading;
  }

  function updateCharCount() {
    var len = chatInput.value.length;
    charCount.textContent = len + " / " + MAX_CHARS;
    charCount.classList.toggle("visible", len > 100);
    if (len > MAX_CHARS) {
      charCount.style.color = "var(--accent-red)";
    } else {
      charCount.style.color = "";
    }
  }

  chatInput.addEventListener("input", function () {
    this.style.height = "auto";
    this.style.height = Math.min(this.scrollHeight, 200) + "px";
    updateSendBtn();
    updateCharCount();
  });

  // Centralized "user wants to send" handler -- forces loading reset
  // when the chat appears stuck, with a generous tolerance (60s).
  // S23 fix: 3s threshold caused duplicate sends on every complex query.
  // Complex queries take 15-55s for tool execution. Only reset if truly stuck.
  function forceResetIfStuck() {
    if (!state.isLoading) return;
    // Orphaned loading flag (no timestamp) -- reset immediately
    if (!state._loadStart || state._loadStart === 0) {
      resetLoadingState();
      var s1 = document.getElementById("streaming-msg");
      if (s1) s1.remove();
      return;
    }
    var elapsed = Date.now() - state._loadStart;
    // Loading for more than 60s -- likely genuinely stuck, allow reset
    if (elapsed > 60000) {
      resetLoadingState();
      var s2 = document.getElementById("streaming-msg");
      if (s2) s2.remove();
      return;
    }
    // Loading for <60s -- genuinely in-flight, show feedback
    showToast("Nova is still responding... please wait a moment", "info");
  }

  chatInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      forceResetIfStuck();
      sendMessage();
    }
  });

  sendBtn.addEventListener("click", function () {
    forceResetIfStuck();
    sendMessage();
  });

  // ========================================================================
  // VOICE INPUT (Web Speech API)
  // ========================================================================
  var _recognition = null;
  var voiceBtn = document.getElementById("voice-btn");

  function toggleVoiceInput() {
    if (
      !("webkitSpeechRecognition" in window) &&
      !("SpeechRecognition" in window)
    ) {
      showToast("Voice input not supported in this browser", "error");
      return;
    }
    if (_recognition) {
      _recognition.stop();
      _recognition = null;
      voiceBtn.classList.remove("recording");
      return;
    }
    var SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    _recognition = new SR();
    _recognition.continuous = false;
    _recognition.interimResults = true;
    _recognition.lang = "en-US";
    voiceBtn.classList.add("recording");

    _recognition.onresult = function (e) {
      var transcript = Array.from(e.results)
        .map(function (r) {
          return r[0].transcript;
        })
        .join("");
      chatInput.value = transcript;
      updateSendBtn();
      updateCharCount();
    };

    _recognition.onend = function () {
      voiceBtn.classList.remove("recording");
      _recognition = null;
      // Auto-send if we got text
      if (chatInput.value.trim()) {
        sendMessage();
      }
    };

    _recognition.onerror = function (e) {
      voiceBtn.classList.remove("recording");
      _recognition = null;
      if (e.error === "not-allowed") {
        showToast(
          "Microphone access denied. Check browser permissions.",
          "error",
        );
      } else if (e.error !== "aborted") {
        showToast("Voice input error: " + e.error, "error");
      }
    };

    try {
      _recognition.start();
      showToast("Listening... speak now", "info");
    } catch (err) {
      voiceBtn.classList.remove("recording");
      _recognition = null;
      showToast("Could not start voice input", "error");
    }
  }

  if (voiceBtn) {
    voiceBtn.addEventListener("click", toggleVoiceInput);
  }

  // ========================================================================
  // GLOBAL SAFETY: auto-reset stuck isLoading state every 2s
  // S23 fix: Check for tool status pills before killing stream.
  // During tool execution (15-55s), streamContent is empty but tool pills
  // are actively rendering. Only kill if NO activity at all for 90s,
  // or absolute hard limit of 120s.
  // ========================================================================
  setInterval(function () {
    if (!state.isLoading) return;
    // If _loadStart is 0 or missing, isLoading is orphaned -- reset immediately
    if (!state._loadStart || state._loadStart === 0) {
      resetLoadingState();
      return;
    }
    var elapsed = Date.now() - state._loadStart;
    var streamEl = document.getElementById("streaming-msg");
    var streamContent = document.getElementById("streaming-content");
    var hasVisibleStream =
      streamEl && streamContent && streamContent.textContent.length > 0;
    // S23: Check for tool status pills (rendered as siblings of streaming-msg, not children)
    var hasToolPills = document.querySelector(
      ".nova-tool-pill-active, #tool-status-container, .nova-tool-status-container",
    );
    var hasAnyActivity = hasVisibleStream || hasToolPills;
    if (elapsed > 120000) {
      // Absolute hard limit 120s -- always reset
      resetLoadingState();
      if (streamEl) streamEl.remove();
    } else if (elapsed > 90000 && !hasAnyActivity) {
      // 90s with zero activity (no stream text AND no tool pills) -- stuck
      resetLoadingState();
      if (streamEl) streamEl.remove();
    }
    // Otherwise: let the stream continue, tools are working
  }, 2000);

  // ========================================================================
  // CLEAR / NEW CHAT
  // ========================================================================
  clearChatBtn.addEventListener("click", function () {
    var conv = getActiveConv();
    if (conv) {
      conv.messages = [];
      conv.title = "New Chat";
      conv.updatedAt = Date.now();
      flushSaveConversations();
      renderSidebar();
      renderChat();
    }
  });

  newChatBtn.addEventListener("click", function () {
    createConversation();
    closeSidebarMobile();
    chatInput.focus();
  });

  // ========================================================================
  // SIDEBAR TOGGLE
  // ========================================================================
  function closeSidebarMobile() {
    if (window.innerWidth <= 1024) {
      sidebar.classList.add("collapsed");
      sidebarOverlay.classList.remove("visible");
      setTimeout(function () {
        sidebarOverlay.style.display = "none";
      }, 300);
    }
  }

  sidebarToggle.addEventListener("click", function () {
    var isCollapsed = sidebar.classList.contains("collapsed");
    if (isCollapsed) {
      sidebar.classList.remove("collapsed");
      if (window.innerWidth <= 1024) {
        sidebarOverlay.style.display = "block";
        requestAnimationFrame(function () {
          sidebarOverlay.classList.add("visible");
        });
      }
    } else {
      sidebar.classList.add("collapsed");
      sidebarOverlay.classList.remove("visible");
      setTimeout(function () {
        sidebarOverlay.style.display = "none";
      }, 300);
    }
  });

  sidebarOverlay.addEventListener("click", closeSidebarMobile);

  // ========================================================================
  // THEME TOGGLE (dark/light)
  // ========================================================================
  function initTheme() {
    var saved = localStorage.getItem(THEME_KEY);
    if (saved === "light") {
      document.documentElement.setAttribute("data-theme", "light");
      document.getElementById("theme-icon-moon").style.display = "none";
      document.getElementById("theme-icon-sun").style.display = "";
    }
  }
  initTheme();

  themeToggleBtn.addEventListener("click", function () {
    var current = document.documentElement.getAttribute("data-theme");
    if (current === "light") {
      document.documentElement.removeAttribute("data-theme");
      safeSaveToStorage(THEME_KEY, "dark");
      document.getElementById("theme-icon-moon").style.display = "";
      document.getElementById("theme-icon-sun").style.display = "none";
    } else {
      document.documentElement.setAttribute("data-theme", "light");
      safeSaveToStorage(THEME_KEY, "light");
      document.getElementById("theme-icon-moon").style.display = "none";
      document.getElementById("theme-icon-sun").style.display = "";
    }
  });

  // ========================================================================
  // SHARE CONVERSATION
  // ========================================================================
  shareBtn.addEventListener("click", function () {
    var conv = getActiveConv();
    if (!conv || conv.messages.length === 0) {
      showToast("No conversation to share", "error");
      return;
    }
    // Save to Supabase and generate share link
    var shareData = {
      conversation_id: conv.id,
      title: conv.title,
      messages: conv.messages.map(function (m) {
        return {
          role: m.role,
          content: m.content,
          timestamp: m.timestamp,
        };
      }),
    };
    fetch("/api/chat/share", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": window.__csrfToken,
      },
      body: JSON.stringify(shareData),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        var shareUrl =
          data.share_url || window.location.origin + "/nova/shared/" + conv.id;
        showShareModal(shareUrl);
      })
      .catch(function () {
        // Fallback: generate client-side share URL
        var shareUrl = window.location.origin + "/nova/shared/" + conv.id;
        showShareModal(shareUrl);
      });
  });

  function showShareModal(url) {
    var overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML =
      '<div class="modal-content">' +
      '<div class="modal-title">Share Conversation</div>' +
      '<p style="font-size:13px;color:var(--text-secondary);margin-bottom:12px;">Anyone with this link can view a read-only copy of this conversation.</p>' +
      '<div class="share-link-container">' +
      '<span class="share-link-url">' +
      escapeHtml(url) +
      "</span>" +
      '<button class="share-link-copy">Copy</button>' +
      "</div>" +
      '<button class="modal-close">Close</button>' +
      "</div>";
    document.body.appendChild(overlay);
    overlay
      .querySelector(".share-link-copy")
      .addEventListener("click", function () {
        navigator.clipboard.writeText(url).then(function () {
          showToast("Link copied to clipboard", "success");
        });
      });
    overlay
      .querySelector(".modal-close")
      .addEventListener("click", function () {
        overlay.remove();
      });
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) overlay.remove();
    });
  }

  // ========================================================================
  // EXPORT CONVERSATION
  // ========================================================================
  exportBtn.addEventListener("click", function () {
    var conv = getActiveConv();
    if (!conv || conv.messages.length === 0) {
      showToast("No conversation to export", "error");
      return;
    }
    showExportModal(conv);
  });

  function showExportModal(conv) {
    var overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML =
      '<div class="modal-content">' +
      '<div class="modal-title">Export Conversation</div>' +
      '<div class="modal-option" data-action="copy">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>' +
      "<span>Copy All to Clipboard</span></div>" +
      '<div class="modal-option" data-action="txt">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>' +
      "<span>Download as TXT</span></div>" +
      '<div class="modal-option" data-action="json">' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/></svg>' +
      "<span>Download as JSON</span></div>" +
      '<button class="modal-close">Cancel</button>' +
      "</div>";
    document.body.appendChild(overlay);

    overlay
      .querySelector('[data-action="copy"]')
      .addEventListener("click", function () {
        var text = formatConvAsText(conv);
        navigator.clipboard.writeText(text).then(function () {
          showToast("Conversation copied to clipboard", "success");
          overlay.remove();
        });
      });
    overlay
      .querySelector('[data-action="txt"]')
      .addEventListener("click", function () {
        var text = formatConvAsText(conv);
        downloadFile(conv.title + ".txt", text, "text/plain");
        overlay.remove();
      });
    overlay
      .querySelector('[data-action="json"]')
      .addEventListener("click", function () {
        var data = conv.messages.map(function (m) {
          return {
            role: m.role,
            content: m.content,
            timestamp: m.timestamp ? new Date(m.timestamp).toISOString() : "",
          };
        });
        downloadFile(
          conv.title + ".json",
          JSON.stringify(data, null, 2),
          "application/json",
        );
        overlay.remove();
      });
    overlay
      .querySelector(".modal-close")
      .addEventListener("click", function () {
        overlay.remove();
      });
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) overlay.remove();
    });
  }

  function formatConvAsText(conv) {
    return conv.messages
      .map(function (m) {
        var role = m.role === "user" ? "User" : "Nova";
        return role + ": " + m.content;
      })
      .join("\n\n");
  }

  function downloadFile(filename, content, mimeType) {
    var blob = new Blob([content], { type: mimeType });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ========================================================================
  // KEYBOARD SHORTCUTS
  // ========================================================================
  shortcutsBtn.addEventListener("click", showShortcutsOverlay);

  function showShortcutsOverlay() {
    var overlay = document.createElement("div");
    overlay.className = "shortcuts-overlay";
    overlay.innerHTML =
      '<div class="shortcuts-panel">' +
      '<div class="shortcuts-title">Keyboard Shortcuts</div>' +
      '<div class="shortcut-row"><span>Focus chat input</span><span class="shortcut-key"><kbd>Ctrl</kbd>+<kbd>K</kbd></span></div>' +
      '<div class="shortcut-row"><span>Copy last response</span><span class="shortcut-key"><kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>C</kbd></span></div>' +
      '<div class="shortcut-row"><span>Send message</span><span class="shortcut-key"><kbd>Enter</kbd></span></div>' +
      '<div class="shortcut-row"><span>New line</span><span class="shortcut-key"><kbd>Shift</kbd>+<kbd>Enter</kbd></span></div>' +
      '<div class="shortcut-row"><span>Close sidebar (mobile)</span><span class="shortcut-key"><kbd>Esc</kbd></span></div>' +
      '<div class="shortcut-row"><span>Clear chat</span><span class="shortcut-key">Type <kbd>/clear</kbd></span></div>' +
      '<div class="shortcut-row"><span>Toggle theme</span><span class="shortcut-key"><kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>T</kbd></span></div>' +
      '<button class="modal-close">Close</button>' +
      "</div>";
    document.body.appendChild(overlay);
    overlay
      .querySelector(".modal-close")
      .addEventListener("click", function () {
        overlay.remove();
      });
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) overlay.remove();
    });
  }

  // Flush any pending debounced save before the page unloads
  window.addEventListener("beforeunload", function () {
    flushSaveConversations();
  });

  document.addEventListener("keydown", function (e) {
    var isMac = navigator.platform.indexOf("Mac") > -1;
    var mod = isMac ? e.metaKey : e.ctrlKey;
    // Ctrl/Cmd+K: Focus input
    if (mod && e.key === "k") {
      e.preventDefault();
      chatInput.focus();
    }
    // Ctrl/Cmd+Shift+C: Copy last response
    if (mod && e.shiftKey && e.key === "C") {
      e.preventDefault();
      var conv = getActiveConv();
      if (conv) {
        for (var i = conv.messages.length - 1; i >= 0; i--) {
          if (conv.messages[i].role === "assistant") {
            navigator.clipboard
              .writeText(conv.messages[i].content)
              .then(function () {
                showToast("Last response copied", "success");
              });
            break;
          }
        }
      }
    }
    // Ctrl/Cmd+Shift+T: Toggle theme
    if (mod && e.shiftKey && e.key === "T") {
      e.preventDefault();
      themeToggleBtn.click();
    }
    // Escape: close sidebar on mobile, or close any modal
    if (e.key === "Escape") {
      var modal = document.querySelector(".modal-overlay, .shortcuts-overlay");
      if (modal) {
        modal.remove();
        return;
      }
      closeSidebarMobile();
    }
  });

  // ========================================================================
  // ORB ACCENT ANIMATION (decorative mini orb)
  // ========================================================================
  (function () {
    var canvas = document.getElementById("orb-canvas");
    if (!canvas) return;
    var ctx = canvas.getContext("2d");
    var W = 240,
      H = 240;
    var CX = W / 2,
      CY = H / 2;
    var orbR = 40;
    var t = 0;
    var dots = [];
    for (var i = 0; i < 60; i++) {
      var phi = Math.acos(1 - 2 * Math.random());
      var theta = Math.random() * Math.PI * 2;
      dots.push({
        phi: phi,
        theta: theta,
        size: 0.8 + Math.random() * 0.8,
      });
    }
    function draw() {
      ctx.clearRect(0, 0, W, H);
      var rotY = t * 0.3;

      // Core glow
      var g = ctx.createRadialGradient(CX, CY, 0, CX, CY, orbR * 2);
      g.addColorStop(0, "rgba(90,84,189,0.15)");
      g.addColorStop(0.5, "rgba(107,179,205,0.06)");
      g.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = g;
      ctx.fillRect(0, 0, W, H);

      dots.forEach(function (d) {
        var x = orbR * Math.sin(d.phi) * Math.cos(d.theta + rotY);
        var z = orbR * Math.sin(d.phi) * Math.sin(d.theta + rotY);
        var y = orbR * Math.cos(d.phi);
        var scale = 300 / (300 + z);
        var alpha = (0.3 + scale * 0.4) * (0.5 + 0.5 * Math.sin(t * 2 + d.phi));
        ctx.beginPath();
        ctx.arc(CX + x * scale, CY + y * scale, d.size * scale, 0, Math.PI * 2);
        ctx.fillStyle = "rgba(107,179,205," + alpha.toFixed(3) + ")";
        ctx.fill();
      });

      t += 0.016;
      requestAnimationFrame(draw);
    }
    draw();
  })();

  // ========================================================================
  // CONVERSATION SEARCH (sidebar filter)
  // ========================================================================
  if (convSearchInput) {
    var _searchDebounce = null;
    convSearchInput.addEventListener("input", function () {
      clearTimeout(_searchDebounce);
      _searchDebounce = setTimeout(function () {
        renderSidebar();
      }, 150);
    });
  }

  // ========================================================================
  // TOKEN USAGE INDICATOR (context window tracking)
  // ========================================================================
  function createTokenIndicator() {
    var charCountEl = document.getElementById("char-count");
    if (!charCountEl) return null;
    var indicator = document.getElementById("token-indicator");
    if (indicator) return indicator;
    indicator = document.createElement("div");
    indicator.id = "token-indicator";
    indicator.style.cssText =
      "font-size:11px;color:var(--text-muted);display:inline-flex;" +
      "align-items:center;gap:4px;margin-left:8px;opacity:0;transition:opacity 0.3s;";
    indicator.title = "Approximate token usage for this conversation";
    indicator.innerHTML =
      '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="flex-shrink:0">' +
      '<path d="M20.24 12.24a6 6 0 0 0-8.49-8.49L5 10.5V19h8.5z"/>' +
      '<line x1="16" y1="8" x2="2" y2="22"/><line x1="17.5" y1="15" x2="9" y2="15"/>' +
      '</svg><span class="token-text">0 tokens</span>';
    charCountEl.parentNode.insertBefore(indicator, charCountEl.nextSibling);
    return indicator;
  }

  function updateTokenIndicator(tokenUsage) {
    if (!tokenUsage) return;
    var indicator = createTokenIndicator();
    if (!indicator) return;
    var total = tokenUsage.total_tokens || 0;
    var textEl = indicator.querySelector(".token-text");
    if (textEl) {
      var display =
        total >= 1000 ? (total / 1000).toFixed(1) + "K" : String(total);
      textEl.textContent = display + " tokens";
    }
    indicator.style.opacity = total > 0 ? "1" : "0";
  }

  // Recalculate token count when switching conversations
  function recalcTokensForConv(conv) {
    if (!conv || !conv.messages || conv.messages.length === 0) {
      var ind = document.getElementById("token-indicator");
      if (ind) ind.style.opacity = "0";
      return;
    }
    // Find the last assistant message with token_usage
    for (var i = conv.messages.length - 1; i >= 0; i--) {
      if (conv.messages[i].token_usage) {
        updateTokenIndicator(conv.messages[i].token_usage);
        return;
      }
    }
    // Fallback: estimate from all messages client-side
    var total = 0;
    conv.messages.forEach(function (m) {
      var words = (m.content || "").split(/\s+/).length;
      total += Math.round(words * 1.3);
    });
    updateTokenIndicator({ total_tokens: total });
  }

  // ========================================================================
  // STALE ERROR CLEANUP
  // ========================================================================
  /**
   * Remove stale error messages from conversations on load.
   * Prevents showing previous session timeout/network errors.
   * Also clears conversations older than 24 hours.
   */
  function _pruneStaleErrors() {
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
    var changed = false;

    Object.keys(state.conversations).forEach(function (id) {
      var conv = state.conversations[id];
      // Remove entire conversation if older than 24h and has errors
      var updatedAt = conv.updatedAt || conv.createdAt || 0;
      if (updatedAt && now - updatedAt > STALE_MS) {
        // Keep the conversation but clear trailing errors
      }

      // Remove trailing error messages
      if (!conv.messages || !conv.messages.length) return;
      while (conv.messages.length > 0) {
        var last = conv.messages[conv.messages.length - 1];
        if (last.role !== "assistant") break;
        var content = (last.content || "").toLowerCase();
        var isError = errorPatterns.some(function (pat) {
          return pat.test(content);
        });
        if (isError) {
          conv.messages.pop();
          changed = true;
          // Also remove the triggering user message
          if (
            conv.messages.length > 0 &&
            conv.messages[conv.messages.length - 1].role === "user"
          ) {
            conv.messages.pop();
          }
        } else {
          break;
        }
      }
    });

    if (changed) {
      flushSaveConversations();
    }
  }

  // ========================================================================
  // INIT
  // ========================================================================
  loadConversations();
  _pruneStaleErrors();

  // Default: sidebar open on desktop, collapsed on mobile
  if (window.innerWidth <= 1024) {
    sidebar.classList.add("collapsed");
  }

  // If no conversations exist, or no active one, create fresh
  if (Object.keys(state.conversations).length === 0 || !state.activeConvId) {
    if (Object.keys(state.conversations).length > 0) {
      var keys = Object.keys(state.conversations).sort(function (a, b) {
        return (
          (state.conversations[b].updatedAt || 0) -
          (state.conversations[a].updatedAt || 0)
        );
      });
      state.activeConvId = keys[0];
    }
  }

  renderSidebar();
  renderChat();
  chatInput.focus();
})();
