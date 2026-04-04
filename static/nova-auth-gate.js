/**
 * Nova Auth Gate -- Hard login requirement
 * Blocks page content until Google sign-in completes.
 * Add this script AFTER nova-auth.js on protected pages.
 */
(function () {
  "use strict";

  function createGate() {
    // Don't gate if already logged in
    if (typeof NovaAuth !== "undefined" && NovaAuth.isLoggedIn()) return;

    // Check if we just came back from OAuth (access_token in URL hash)
    if (
      window.location.hash &&
      window.location.hash.indexOf("access_token") !== -1
    ) {
      // Auth succeeded -- save minimal user info and skip gate
      try {
        // Parse the hash params
        var hashParams = {};
        window.location.hash
          .substring(1)
          .split("&")
          .forEach(function (p) {
            var kv = p.split("=");
            hashParams[kv[0]] = decodeURIComponent(kv[1] || "");
          });
        if (hashParams.access_token) {
          // Store token so gate knows user is authenticated
          localStorage.setItem(
            "nova_auth_user",
            JSON.stringify({
              email: "authenticated",
              token: hashParams.access_token,
            }),
          );
          // Clean up URL hash
          history.replaceState(null, "", window.location.pathname);
          return; // Skip gate
        }
      } catch (_) {}
    }

    // Admin key bypass: ?admin_key=<key> skips OAuth entirely
    try {
      var urlParams = new URLSearchParams(window.location.search);
      var adminKey = urlParams.get("admin_key");
      if (adminKey) {
        // Validate against server
        fetch("/api/admin/status", {
          headers: { "X-Admin-Key": adminKey },
        })
          .then(function (r) {
            return r.json();
          })
          .then(function (d) {
            if (d && d.authenticated) {
              localStorage.setItem(
                "nova_auth_user",
                JSON.stringify({ email: "admin@joveo.com", role: "admin" }),
              );
              // Clean URL
              urlParams.delete("admin_key");
              var clean = window.location.pathname;
              if (urlParams.toString()) clean += "?" + urlParams.toString();
              history.replaceState(null, "", clean);
              // Remove gate if it was already created
              var gate = document.getElementById("nova-auth-gate");
              if (gate) gate.remove();
            }
          })
          .catch(function () {});
        return; // Don't show gate while validating
      }
    } catch (_) {}

    // Check localStorage for cached user
    try {
      var cached = localStorage.getItem("nova_auth_user");
      if (cached && JSON.parse(cached).email) return;
    } catch (_) {}

    // Create full-screen overlay
    var overlay = document.createElement("div");
    overlay.id = "nova-auth-gate";
    overlay.style.cssText =
      "position:fixed;top:0;left:0;right:0;bottom:0;z-index:999999;" +
      "background:linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);" +
      "display:flex;align-items:center;justify-content:center;flex-direction:column;";

    overlay.innerHTML =
      '<div style="text-align:center;max-width:420px;padding:40px;">' +
      '  <div style="width:80px;height:80px;margin:0 auto 24px;border-radius:20px;background:linear-gradient(135deg,#5A54BD,#7B6FDE);display:flex;align-items:center;justify-content:center;box-shadow:0 8px 32px rgba(90,84,189,0.4);">' +
      '    <span style="font-size:40px;font-weight:800;color:white;font-family:Inter,system-ui,sans-serif;line-height:1;">N</span>' +
      "  </div>" +
      '  <h1 style="color:#e4e4e7;font-size:28px;font-weight:700;margin:0 0 8px;font-family:Inter,sans-serif;">Welcome to Nova AI</h1>' +
      '  <p style="color:rgba(255,255,255,0.5);font-size:15px;margin:0 0 32px;font-family:Inter,sans-serif;line-height:1.5;">AI-powered recruitment intelligence platform.<br>Sign in to access the suite.</p>' +
      '  <button id="nova-gate-login-btn" style="' +
      "    background:white;color:#1a1a2e;border:none;padding:14px 32px;border-radius:12px;" +
      "    font-size:15px;font-weight:600;cursor:pointer;display:inline-flex;align-items:center;" +
      '    gap:10px;font-family:Inter,sans-serif;transition:all 0.2s;box-shadow:0 4px 12px rgba(0,0,0,0.3);">' +
      '    <svg viewBox="0 0 24 24" style="width:20px;height:20px;">' +
      '      <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z" fill="#4285F4"/>' +
      '      <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>' +
      '      <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>' +
      '      <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>' +
      "    </svg>" +
      "    Sign in with Google" +
      "  </button>" +
      '  <p style="color:rgba(255,255,255,0.2);font-size:11px;margin-top:24px;font-family:Inter,sans-serif;">Created by Shubham Singh Chandel</p>' +
      "</div>";

    document.body.appendChild(overlay);

    // Add hover effect
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
    }

    // BULLETPROOF click handler: fetches config and redirects directly
    // No dependency on Supabase JS, NovaAuth, or any library
    if (btn) {
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

            // Direct redirect to Supabase OAuth - NO JS library needed
            var redirectTo = encodeURIComponent(
              window.location.origin + window.location.pathname,
            );
            var authUrl =
              cfg.supabase_url +
              "/auth/v1/authorize" +
              "?provider=google" +
              "&redirect_to=" +
              redirectTo;

            // This WILL work - it's just a URL redirect
            window.location.href = authUrl;
          })
          .catch(function (err) {
            console.error("[NovaAuth] Sign-in error:", err);
            alert("Failed to connect. Please try again.");
            btn.innerHTML = "Sign in with Google";
            btn.disabled = false;
            btn.style.opacity = "1";
          });
      });
    }
  }

  function removeGate() {
    var gate = document.getElementById("nova-auth-gate");
    if (gate) {
      gate.style.transition = "opacity 0.3s ease";
      gate.style.opacity = "0";
      setTimeout(function () {
        gate.remove();
      }, 300);
    }
  }

  // Listen for successful login
  document.addEventListener("nova:auth:login", function () {
    removeGate();
  });

  // Create gate on DOM ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", createGate);
  } else {
    createGate();
  }
})();
