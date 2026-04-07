/**
 * Nova Toast Notification System
 * Shared across all Nova AI Suite pages.
 * Usage: window.NovaToast.show("Message", "error"|"success"|"warning"|"info")
 */
(function () {
  "use strict";

  // Inject container if not present
  function _ensureContainer() {
    var el = document.getElementById("nova-toast-container");
    if (el) return el;
    el = document.createElement("div");
    el.id = "nova-toast-container";
    el.setAttribute("aria-live", "polite");
    el.setAttribute("aria-atomic", "false");
    el.style.cssText =
      "position:fixed;top:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;pointer-events:none;max-width:380px;width:100%;";
    document.body.appendChild(el);
    return el;
  }

  // Inject styles once
  var _stylesInjected = false;
  function _injectStyles() {
    if (_stylesInjected) return;
    _stylesInjected = true;
    var style = document.createElement("style");
    style.textContent =
      "#nova-toast-container .nova-toast{pointer-events:auto;display:flex;align-items:flex-start;gap:10px;padding:12px 16px;border-radius:10px;font-size:13px;line-height:1.4;font-family:Inter,-apple-system,BlinkMacSystemFont,sans-serif;color:#e4e4e7;background:rgba(17,17,17,0.95);backdrop-filter:blur(16px) saturate(1.3);border:1px solid rgba(255,255,255,0.08);box-shadow:0 8px 30px rgba(0,0,0,0.5);animation:nova-toast-in 0.3s cubic-bezier(0.16,1,0.3,1);}" +
      "#nova-toast-container .nova-toast.toast-out{animation:nova-toast-out 0.25s ease forwards;}" +
      "#nova-toast-container .nova-toast.error{border-color:rgba(239,68,68,0.3);}" +
      "#nova-toast-container .nova-toast.success{border-color:rgba(34,197,94,0.3);}" +
      "#nova-toast-container .nova-toast.warning{border-color:rgba(245,158,11,0.3);}" +
      "#nova-toast-container .nova-toast.info{border-color:rgba(90,84,189,0.3);}" +
      "#nova-toast-container .nova-toast-icon{flex-shrink:0;width:18px;height:18px;margin-top:1px;}" +
      "#nova-toast-container .nova-toast-msg{flex:1;}" +
      "#nova-toast-container .nova-toast-close{flex-shrink:0;background:none;border:none;color:rgba(255,255,255,0.4);cursor:pointer;padding:2px;font-size:16px;line-height:1;transition:color 0.15s;min-width:24px;min-height:24px;display:flex;align-items:center;justify-content:center;}" +
      "#nova-toast-container .nova-toast-close:hover{color:#fff;}" +
      "@keyframes nova-toast-in{from{opacity:0;transform:translateX(20px) scale(0.95);}to{opacity:1;transform:translateX(0) scale(1);}}" +
      "@keyframes nova-toast-out{from{opacity:1;transform:translateX(0) scale(1);}to{opacity:0;transform:translateX(20px) scale(0.95);}}";
    document.head.appendChild(style);
  }

  var ICONS = {
    error:
      '<svg viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
    success:
      '<svg viewBox="0 0 24 24" fill="none" stroke="#22c55e" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
    warning:
      '<svg viewBox="0 0 24 24" fill="none" stroke="#f59e0b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    info: '<svg viewBox="0 0 24 24" fill="none" stroke="#5a54bd" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
  };

  function show(message, type, duration) {
    _injectStyles();
    var container = _ensureContainer();
    type = type || "info";
    duration = duration || 4000;

    var toast = document.createElement("div");
    toast.className = "nova-toast " + type;
    toast.setAttribute("role", "alert");

    var iconHtml = ICONS[type] || ICONS.info;
    toast.innerHTML =
      '<span class="nova-toast-icon">' +
      iconHtml +
      "</span>" +
      '<span class="nova-toast-msg">' +
      _escapeHtml(message) +
      "</span>" +
      '<button class="nova-toast-close" aria-label="Dismiss">&times;</button>';

    var closeBtn = toast.querySelector(".nova-toast-close");
    closeBtn.addEventListener("click", function () {
      _dismiss(toast);
    });

    container.appendChild(toast);

    // Cap at 5 visible toasts
    var toasts = container.querySelectorAll(".nova-toast:not(.toast-out)");
    if (toasts.length > 5) {
      _dismiss(toasts[0]);
    }

    var timer = setTimeout(function () {
      _dismiss(toast);
    }, duration);

    toast._timer = timer;
  }

  function _dismiss(toast) {
    if (toast._dismissed) return;
    toast._dismissed = true;
    clearTimeout(toast._timer);
    toast.classList.add("toast-out");
    setTimeout(function () {
      if (toast.parentNode) toast.parentNode.removeChild(toast);
    }, 250);
  }

  function _escapeHtml(str) {
    var el = document.createElement("span");
    el.appendChild(document.createTextNode(str));
    return el.innerHTML;
  }

  // Global fetch error interceptor
  var _originalFetch = window.fetch;
  window.fetch = function () {
    return _originalFetch
      .apply(this, arguments)
      .then(function (response) {
        if (!response.ok && response.status >= 500) {
          show("Server error. Please try again.", "error");
        }
        return response;
      })
      .catch(function (err) {
        if (err.name !== "AbortError") {
          show("Connection lost. Check your network and retry.", "error");
        }
        throw err;
      });
  };

  window.NovaToast = { show: show };
})();
