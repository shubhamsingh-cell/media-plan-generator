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
      '  <div style="width:64px;height:64px;margin:0 auto 24px;background:linear-gradient(135deg,#5A54BD,#6BB3CD);border-radius:16px;display:flex;align-items:center;justify-content:center;">' +
      '    <span style="color:white;font-size:28px;font-weight:800;font-family:Inter,sans-serif;">N</span>' +
      "  </div>" +
      '  <h1 style="color:#e4e4e7;font-size:28px;font-weight:700;margin:0 0 8px;font-family:Inter,sans-serif;">Welcome to Nova AI</h1>' +
      '  <p style="color:rgba(255,255,255,0.5);font-size:15px;margin:0 0 32px;font-family:Inter,sans-serif;line-height:1.5;">AI-powered recruitment intelligence platform.<br>Sign in to access the suite.</p>' +
      '  <button id="nova-gate-login-btn" data-nova-auth-login style="' +
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
      '  <p style="color:rgba(255,255,255,0.3);font-size:12px;margin-top:24px;font-family:Inter,sans-serif;">Powered by Joveo</p>' +
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

    // Direct click handler -- handles case where NovaAuth.init() hasn't completed
    if (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();

        // Try NovaAuth first
        if (
          typeof NovaAuth !== "undefined" &&
          NovaAuth.isInitialized &&
          NovaAuth.isInitialized()
        ) {
          NovaAuth.signInWithGoogle();
          return;
        }

        // Fallback: init directly from /api/config
        btn.textContent = "Connecting...";
        btn.disabled = true;

        fetch("/api/config")
          .then(function (r) {
            return r.json();
          })
          .then(function (cfg) {
            if (!cfg.auth_enabled || !cfg.supabase_url) {
              btn.textContent = "Sign in with Google";
              btn.disabled = false;
              alert("Authentication is not configured. Please contact admin.");
              return;
            }

            // Initialize NovaAuth if available
            if (typeof NovaAuth !== "undefined") {
              NovaAuth.init({
                supabaseUrl: cfg.supabase_url,
                supabaseAnonKey: cfg.supabase_anon_key,
                allowedDomains: [],
              });
              // Small delay for init to complete
              setTimeout(function () {
                NovaAuth.signInWithGoogle();
              }, 200);
              return;
            }

            // Last resort: create Supabase client directly
            if (typeof window.supabase !== "undefined") {
              var sb = window.supabase.createClient(
                cfg.supabase_url,
                cfg.supabase_anon_key,
              );
              sb.auth.signInWithOAuth({
                provider: "google",
                options: {
                  redirectTo: window.location.origin + window.location.pathname,
                },
              });
            } else {
              alert(
                "Authentication library failed to load. Please refresh the page.",
              );
              btn.textContent = "Sign in with Google";
              btn.disabled = false;
            }
          })
          .catch(function () {
            alert(
              "Failed to connect. Please check your internet and try again.",
            );
            btn.textContent = "Sign in with Google";
            btn.disabled = false;
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
