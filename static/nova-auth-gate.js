/**
 * Nova Auth Gate -- Sign in ONCE, never again
 * Shows login overlay only if no cached session exists.
 * After first sign-in, session is stored in localStorage permanently.
 * Supports guest mode (read-only) for demos and prospects.
 */
(function () {
  "use strict";

  var STORAGE_KEY = "nova_auth_user";
  var GUEST_KEY = "nova_guest_mode";

  function isGuestMode() {
    try {
      return sessionStorage.getItem(GUEST_KEY) === "true";
    } catch (_) {
      return false;
    }
  }

  function enterGuestMode() {
    try {
      sessionStorage.setItem(GUEST_KEY, "true");
    } catch (_) {}
    removeGate();
    showGuestBanner();
  }

  function exitGuestMode() {
    try {
      sessionStorage.removeItem(GUEST_KEY);
    } catch (_) {}
    removeGuestBanner();
  }

  function showGuestBanner() {
    if (document.getElementById("nova-guest-banner")) return;
    var banner = document.createElement("div");
    banner.id = "nova-guest-banner";
    banner.style.cssText =
      "position:fixed;top:0;left:0;right:0;z-index:999998;background:linear-gradient(90deg,#202058,#5A54BD);" +
      "color:rgba(255,255,255,0.85);font-size:13px;font-family:Inter,system-ui,sans-serif;" +
      "text-align:center;padding:8px 16px;display:flex;align-items:center;justify-content:center;gap:8px;";
    banner.innerHTML =
      "<span>Viewing as guest \u2014 some features are restricted.</span>" +
      '<a id="nova-guest-signin-link" href="#" style="color:#7dd3fc;text-decoration:underline;font-weight:600;font-size:13px;">Sign in for full access</a>';
    document.body.appendChild(banner);
    // Push page content down so banner doesn't overlap
    document.body.style.paddingTop =
      (parseInt(getComputedStyle(document.body).paddingTop) || 0) +
      banner.offsetHeight +
      "px";
    document
      .getElementById("nova-guest-signin-link")
      .addEventListener("click", function (e) {
        e.preventDefault();
        exitGuestMode();
        document.body.style.paddingTop = "";
        createGate();
      });
  }

  function removeGuestBanner() {
    var banner = document.getElementById("nova-guest-banner");
    if (banner) {
      document.body.style.paddingTop = "";
      banner.remove();
    }
  }

  function isAuthenticated() {
    // Check 0: Guest mode active (read-only access)
    if (isGuestMode()) return true;

    // Check 1: NovaAuth already initialized and logged in
    if (typeof NovaAuth !== "undefined" && NovaAuth.isLoggedIn()) return true;

    // Check 2: localStorage has cached user (persists across refreshes)
    // S48: Validate email is a real @joveo.com address, not just any truthy string
    try {
      var cached = localStorage.getItem(STORAGE_KEY);
      if (cached) {
        var user = JSON.parse(cached);
        if (user && user.email && typeof user.email === "string") {
          var emailLower = user.email.toLowerCase().trim();
          // Must be a valid email format AND from @joveo.com domain
          if (
            /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(emailLower) &&
            emailLower.endsWith("@joveo.com")
          ) {
            return true;
          }
          // Invalid or non-joveo email in cache -- purge it
          localStorage.removeItem(STORAGE_KEY);
        }
      }
    } catch (_) {}

    // Check 3: Supabase session token in localStorage (set by Supabase JS)
    try {
      for (var i = 0; i < localStorage.length; i++) {
        var key = localStorage.key(i);
        if (
          key &&
          key.indexOf("supabase") !== -1 &&
          key.indexOf("auth") !== -1
        ) {
          var val = localStorage.getItem(key);
          if (val && val.indexOf("access_token") !== -1) return true;
        }
      }
    } catch (_) {}

    // Check 3.5: Detect OAuth error in URL hash or query params
    // Google OAuth / Supabase returns error info when auth fails
    // (e.g., user not in GCP testing user list, consent screen errors)
    try {
      var _hashStr = window.location.hash
        ? window.location.hash.substring(1)
        : "";
      var _searchStr = window.location.search
        ? window.location.search.substring(1)
        : "";
      var _combinedParams = _hashStr + "&" + _searchStr;
      if (_combinedParams.indexOf("error=") !== -1) {
        var _errParams = {};
        _combinedParams.split("&").forEach(function (p) {
          var kv = p.split("=");
          if (kv[0])
            _errParams[kv[0]] = decodeURIComponent(kv[1] || "").replace(
              /\+/g,
              " ",
            );
        });
        var _oauthError = _errParams["error"] || "";
        var _oauthDesc = _errParams["error_description"] || "";
        if (_oauthError) {
          // Clean URL
          history.replaceState(null, "", window.location.pathname);
          // Store error for display in gate UI
          window._novaOAuthError = {
            error: _oauthError,
            description: _oauthDesc,
          };
          console.error("[NovaAuth] OAuth error:", _oauthError, _oauthDesc);
        }
      }
    } catch (_) {}

    // Check 4: Just came back from OAuth redirect (access_token in URL hash)
    if (
      window.location.hash &&
      window.location.hash.indexOf("access_token") !== -1
    ) {
      try {
        var hashParams = {};
        window.location.hash
          .substring(1)
          .split("&")
          .forEach(function (p) {
            var kv = p.split("=");
            hashParams[kv[0]] = decodeURIComponent(kv[1] || "");
          });
        if (hashParams.access_token) {
          // S46: Extract actual email from JWT instead of storing "authenticated"
          var _gateEmail = "";
          try {
            var _jwtParts = hashParams.access_token.split(".");
            if (_jwtParts.length >= 2) {
              // Base64url decode the payload
              var _b64 = _jwtParts[1].replace(/-/g, "+").replace(/_/g, "/");
              while (_b64.length % 4) _b64 += "=";
              var _payload = JSON.parse(atob(_b64));
              _gateEmail = (_payload.email || "").toLowerCase().trim();
            }
          } catch (_decodeErr) {
            // JWT decode failed -- do not cache
          }
          if (_gateEmail && _gateEmail.endsWith("@joveo.com")) {
            localStorage.setItem(
              STORAGE_KEY,
              JSON.stringify({
                email: _gateEmail,
                token: hashParams.access_token,
                logged_in_at: new Date().toISOString(),
              }),
            );
            history.replaceState(null, "", window.location.pathname);
            return true;
          }
          // Non-joveo email or decode failure -- do not grant access
          history.replaceState(null, "", window.location.pathname);
          return false;
        }
      } catch (_) {}
    }

    // Check 5: Admin key in URL
    try {
      var urlParams = new URLSearchParams(window.location.search);
      var adminKey = urlParams.get("admin_key");
      if (adminKey) {
        fetch("/api/admin/status", { headers: { "X-Admin-Key": adminKey } })
          .then(function (r) {
            return r.json();
          })
          .then(function (d) {
            if (d && d.authenticated) {
              localStorage.setItem(
                STORAGE_KEY,
                JSON.stringify({ email: "admin@joveo.com", role: "admin" }),
              );
              urlParams.delete("admin_key");
              var clean = window.location.pathname;
              if (urlParams.toString()) clean += "?" + urlParams.toString();
              history.replaceState(null, "", clean);
              removeGate();
            }
          })
          .catch(function () {});
        return true; // Don't show gate while validating
      }
    } catch (_) {}

    return false;
  }

  function createGate() {
    if (isAuthenticated()) return;

    var overlay = document.createElement("div");
    overlay.id = "nova-auth-gate";
    overlay.style.cssText =
      "position:fixed;top:0;left:0;right:0;bottom:0;z-index:999999;" +
      "background:linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);" +
      "display:flex;align-items:center;justify-content:center;flex-direction:column;" +
      "overflow-y:auto;-webkit-overflow-scrolling:touch;padding:16px;box-sizing:border-box;";

    overlay.innerHTML =
      '<div style="text-align:center;max-width:420px;padding:40px 24px;margin:auto;">' +
      '  <div style="width:80px;height:80px;margin:0 auto 24px;border-radius:20px;background:linear-gradient(135deg,#5A54BD,#7B6FDE);display:flex;align-items:center;justify-content:center;box-shadow:0 8px 32px rgba(90,84,189,0.4);">' +
      '    <span style="font-size:40px;font-weight:800;color:white;font-family:Inter,system-ui,sans-serif;line-height:1;">N</span>' +
      "  </div>" +
      '  <h1 style="color:#e4e4e7;font-size:28px;font-weight:700;margin:0 0 8px;font-family:Inter,sans-serif;">Welcome to Nova AI</h1>' +
      '  <p style="color:rgba(255,255,255,0.5);font-size:15px;margin:0 0 32px;font-family:Inter,sans-serif;line-height:1.5;">AI-powered recruitment intelligence platform.<br>Sign in to access the suite.</p>' +
      '  <div id="nova-gate-error" style="display:none;margin:0 0 20px;padding:12px 16px;border-radius:8px;background:rgba(255,80,80,0.12);border:1px solid rgba(255,80,80,0.25);text-align:left;">' +
      '    <div style="color:#ff6b6b;font-size:13px;font-weight:600;margin-bottom:4px;font-family:Inter,sans-serif;">Sign-in failed</div>' +
      '    <div id="nova-gate-error-msg" style="color:rgba(255,255,255,0.6);font-size:12px;line-height:1.4;font-family:Inter,sans-serif;"></div>' +
      "  </div>" +
      '  <button id="nova-gate-login-btn" style="' +
      "    background:white;color:#1a1a2e;border:none;padding:14px 32px;border-radius:12px;" +
      "    font-size:15px;font-weight:600;cursor:pointer;display:inline-flex;align-items:center;" +
      '    gap:10px;font-family:Inter,sans-serif;transition:all 0.2s;box-shadow:0 4px 12px rgba(0,0,0,0.3);width:100%;justify-content:center;">' +
      '    <svg viewBox="0 0 24 24" style="width:20px;height:20px;flex-shrink:0;">' +
      '      <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z" fill="#4285F4"/>' +
      '      <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>' +
      '      <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>' +
      '      <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>' +
      "    </svg>" +
      "    Sign in with Google" +
      "  </button>" +
      '  <div style="margin-top:16px;">' +
      '    <a id="nova-gate-guest-btn" href="#" style="' +
      "      color:rgba(255,255,255,0.4);font-size:13px;font-family:Inter,sans-serif;" +
      '      text-decoration:none;transition:color 0.2s;">' +
      "      Preview without signing in" +
      "    </a>" +
      "  </div>" +
      '  <p style="color:rgba(255,255,255,0.15);font-size:11px;margin-top:24px;font-family:Inter,sans-serif;">Nova AI Suite</p>' +
      "</div>";

    document.body.appendChild(overlay);

    // Show OAuth error if detected (e.g., GCP testing mode, access_denied)
    if (window._novaOAuthError) {
      var errBox = document.getElementById("nova-gate-error");
      var errMsg = document.getElementById("nova-gate-error-msg");
      if (errBox && errMsg) {
        var errCode = window._novaOAuthError.error || "unknown_error";
        var errDesc = window._novaOAuthError.description || "";
        var friendlyMsg = errDesc;
        // Map common OAuth errors to user-friendly messages
        if (errCode === "access_denied") {
          friendlyMsg =
            "Access was denied. Your Google account may not be authorized. " +
            "Please contact the Nova admin to be added as an authorized user.";
        } else if (
          errCode === "server_error" ||
          errCode === "temporarily_unavailable"
        ) {
          friendlyMsg =
            "Google authentication is temporarily unavailable. Please try again in a few minutes.";
        } else if (errCode === "invalid_request") {
          friendlyMsg =
            "Authentication request was invalid. Please clear your browser cache and try again.";
        } else if (!friendlyMsg) {
          friendlyMsg =
            "An error occurred during sign-in (code: " +
            errCode +
            "). Please try again or contact admin.";
        }
        errMsg.textContent = friendlyMsg;
        errBox.style.display = "block";
      }
      delete window._novaOAuthError;
    }

    var btn = document.getElementById("nova-gate-login-btn");
    if (btn) {
      btn.onmouseover = function () {
        this.style.transform = "translateY(-2px)";
        this.style.boxShadow = "0 6px 20px rgba(0,0,0,0.4)";
      };
      btn.onmouseout = function () {
        this.style.transform = "none";
        this.style.boxShadow = "0 4px 12px rgba(0,0,0,0.3)";
      };
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        btn.innerHTML = "Connecting...";
        btn.disabled = true;
        btn.style.opacity = "0.7";
        fetch("/api/config")
          .then(function (r) {
            return r.json();
          })
          .then(function (cfg) {
            if (!cfg.auth_enabled || !cfg.supabase_url) {
              alert("Authentication is not configured.");
              btn.innerHTML = "Sign in with Google";
              btn.disabled = false;
              btn.style.opacity = "1";
              return;
            }
            var redirectTo = encodeURIComponent(
              window.location.origin + window.location.pathname,
            );
            window.location.href =
              cfg.supabase_url +
              "/auth/v1/authorize?provider=google&redirect_to=" +
              redirectTo;
          })
          .catch(function () {
            alert("Failed to connect. Please try again.");
            btn.innerHTML = "Sign in with Google";
            btn.disabled = false;
            btn.style.opacity = "1";
          });
      });
    }

    // Guest mode button
    var guestBtn = document.getElementById("nova-gate-guest-btn");
    if (guestBtn) {
      guestBtn.onmouseover = function () {
        this.style.color = "rgba(255,255,255,0.7)";
      };
      guestBtn.onmouseout = function () {
        this.style.color = "rgba(255,255,255,0.4)";
      };
      guestBtn.addEventListener("click", function (e) {
        e.preventDefault();
        enterGuestMode();
      });
    }

    // Escape key dismisses gate (enters guest mode)
    overlay._escHandler = function (e) {
      if (e.key === "Escape") {
        enterGuestMode();
      }
    };
    document.addEventListener("keydown", overlay._escHandler);
  }

  function removeGate() {
    var gate = document.getElementById("nova-auth-gate");
    if (gate) {
      // Clean up Escape key listener
      if (gate._escHandler) {
        document.removeEventListener("keydown", gate._escHandler);
      }
      gate.style.transition = "opacity 0.3s ease";
      gate.style.opacity = "0";
      setTimeout(function () {
        gate.remove();
      }, 300);
    }
  }

  // Listen for successful login from NovaAuth
  document.addEventListener("nova:auth:login", function () {
    removeGate();
  });

  // Initialize: show gate or guest banner
  function init() {
    if (isGuestMode()) {
      // Already in guest mode from this session -- show banner, skip gate
      showGuestBanner();
    } else {
      createGate();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
