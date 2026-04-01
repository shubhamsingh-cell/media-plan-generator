/**
 * Nova AI Suite -- Google Auth Module (Supabase)
 *
 * Standalone auth module. Does NOT modify any existing functionality.
 * Products work 100% without login. Auth is optional and additive.
 *
 * Usage:
 *   <script src="/static/nova-auth.js"></script>
 *   <script>NovaAuth.init();</script>
 */
(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------
  var AUTH_CONFIG = {
    supabaseUrl: "", // Injected from server via data attribute
    supabaseAnonKey: "", // Injected from server via data attribute
    allowedDomains: ["joveo.com"], // Restrict to Joveo emails only
    storageKey: "nova_auth_user",
    sessionKey: "nova_auth_session",
    onLoginCallbacks: [],
    onLogoutCallbacks: [],
  };

  var _supabase = null;
  var _currentUser = null;
  var _initialized = false;

  // ---------------------------------------------------------------------------
  // Supabase Client (lazy init)
  // ---------------------------------------------------------------------------
  function _getSupabase() {
    if (_supabase) return _supabase;
    if (
      !AUTH_CONFIG.supabaseUrl ||
      !AUTH_CONFIG.supabaseAnonKey ||
      typeof window.supabase === "undefined"
    ) {
      return null;
    }
    try {
      _supabase = window.supabase.createClient(
        AUTH_CONFIG.supabaseUrl,
        AUTH_CONFIG.supabaseAnonKey,
        {
          auth: {
            autoRefreshToken: true,
            persistSession: true,
            detectSessionInUrl: true,
          },
        },
      );
      return _supabase;
    } catch (err) {
      console.warn("[NovaAuth] Failed to initialize Supabase client:", err);
      return null;
    }
  }

  // ---------------------------------------------------------------------------
  // Domain validation
  // ---------------------------------------------------------------------------
  function _isAllowedDomain(email) {
    if (
      !AUTH_CONFIG.allowedDomains ||
      AUTH_CONFIG.allowedDomains.length === 0
    ) {
      return true; // No restriction
    }
    if (!email) return false;
    var domain = email.split("@")[1];
    return AUTH_CONFIG.allowedDomains.indexOf(domain) !== -1;
  }

  // ---------------------------------------------------------------------------
  // User state management
  // ---------------------------------------------------------------------------
  function _setUser(user) {
    _currentUser = user;
    if (user) {
      try {
        localStorage.setItem(AUTH_CONFIG.storageKey, JSON.stringify(user));
      } catch (_) {}
    } else {
      try {
        localStorage.removeItem(AUTH_CONFIG.storageKey);
      } catch (_) {}
    }
    _notifyListeners(user ? "login" : "logout", user);
    _updateUI(user);
  }

  function _loadCachedUser() {
    try {
      var cached = localStorage.getItem(AUTH_CONFIG.storageKey);
      if (cached) {
        _currentUser = JSON.parse(cached);
        return _currentUser;
      }
    } catch (_) {}
    return null;
  }

  // ---------------------------------------------------------------------------
  // Event system
  // ---------------------------------------------------------------------------
  function _notifyListeners(event, user) {
    var callbacks =
      event === "login"
        ? AUTH_CONFIG.onLoginCallbacks
        : AUTH_CONFIG.onLogoutCallbacks;
    for (var i = 0; i < callbacks.length; i++) {
      try {
        callbacks[i](user);
      } catch (_) {}
    }
    // Dispatch custom DOM event for any listener
    try {
      document.dispatchEvent(
        new CustomEvent("nova:auth:" + event, { detail: { user: user } }),
      );
    } catch (_) {}
  }

  // ---------------------------------------------------------------------------
  // UI Updates (non-destructive -- only touches auth elements)
  // ---------------------------------------------------------------------------
  function _updateUI(user) {
    // Login button
    var loginBtn = document.getElementById("nova-auth-login-btn");
    var userInfo = document.getElementById("nova-auth-user-info");
    var userAvatar = document.getElementById("nova-auth-avatar");
    var userName = document.getElementById("nova-auth-name");
    var logoutBtn = document.getElementById("nova-auth-logout-btn");

    if (user) {
      if (loginBtn) loginBtn.style.display = "none";
      if (userInfo) userInfo.style.display = "flex";
      if (userAvatar) {
        userAvatar.src =
          user.avatar_url || user.user_metadata?.avatar_url || "";
        userAvatar.alt = user.name || user.email || "User";
        userAvatar.style.display = userAvatar.src ? "block" : "none";
      }
      if (userName) {
        userName.textContent =
          user.name ||
          user.user_metadata?.full_name ||
          user.email?.split("@")[0] ||
          "User";
      }
      if (logoutBtn) logoutBtn.style.display = "inline-flex";
    } else {
      if (loginBtn) loginBtn.style.display = "inline-flex";
      if (userInfo) userInfo.style.display = "none";
      if (logoutBtn) logoutBtn.style.display = "none";
    }

    // Update chat widget header if it exists
    _updateChatWidgetAuth(user);
  }

  function _updateChatWidgetAuth(user) {
    var chatHeader = document.querySelector(".nova-chat-header-auth");
    if (!chatHeader) return;

    if (user) {
      var name =
        user.name ||
        user.user_metadata?.full_name ||
        user.email?.split("@")[0] ||
        "";
      chatHeader.innerHTML =
        '<img class="nova-chat-auth-avatar" src="' +
        (user.avatar_url || user.user_metadata?.avatar_url || "") +
        '" alt="" style="width:24px;height:24px;border-radius:50%;margin-right:6px;">' +
        '<span style="font-size:12px;opacity:0.8;">' +
        _escapeHtml(name) +
        "</span>";
      chatHeader.style.display = "flex";
    } else {
      chatHeader.innerHTML = "";
      chatHeader.style.display = "none";
    }
  }

  function _escapeHtml(str) {
    var div = document.createElement("div");
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
  }

  // ---------------------------------------------------------------------------
  // Auth Actions
  // ---------------------------------------------------------------------------
  function signInWithGoogle() {
    var sb = _getSupabase();
    if (!sb) {
      console.warn("[NovaAuth] Supabase not initialized");
      _showAuthToast("Authentication not configured", "error");
      return Promise.reject(new Error("Supabase not initialized"));
    }

    return sb.auth
      .signInWithOAuth({
        provider: "google",
        options: {
          redirectTo: window.location.origin + window.location.pathname,
          queryParams: {
            access_type: "offline",
            prompt: "consent",
          },
        },
      })
      .then(function (result) {
        if (result.error) {
          console.error("[NovaAuth] Google sign-in error:", result.error);
          _showAuthToast("Sign-in failed: " + result.error.message, "error");
          return result;
        }
        return result;
      })
      .catch(function (err) {
        console.error("[NovaAuth] Google sign-in error:", err);
        _showAuthToast("Sign-in failed", "error");
        throw err;
      });
  }

  function signOut() {
    var sb = _getSupabase();
    if (sb) {
      sb.auth
        .signOut()
        .then(function () {
          _setUser(null);
          _showAuthToast("Signed out successfully", "success");
        })
        .catch(function (err) {
          console.warn("[NovaAuth] Sign-out error:", err);
          // Force local cleanup even if Supabase call fails
          _setUser(null);
        });
    } else {
      _setUser(null);
    }
  }

  // ---------------------------------------------------------------------------
  // Toast notification (uses existing Nova toast if available)
  // ---------------------------------------------------------------------------
  function _showAuthToast(message, type) {
    // Try to use existing Nova toast system
    if (typeof window.showToast === "function") {
      window.showToast(message, type || "info");
      return;
    }
    // Fallback: simple console log
    console.log("[NovaAuth]", message);
  }

  // ---------------------------------------------------------------------------
  // Session handling (listens for Supabase auth state changes)
  // ---------------------------------------------------------------------------
  function _setupAuthListener() {
    var sb = _getSupabase();
    if (!sb) return;

    sb.auth.onAuthStateChange(function (event, session) {
      if (event === "SIGNED_IN" && session && session.user) {
        var user = session.user;
        var email = user.email || "";

        // Domain check
        if (!_isAllowedDomain(email)) {
          _showAuthToast(
            "Access restricted to authorized domains only.",
            "error",
          );
          sb.auth.signOut();
          _setUser(null);
          return;
        }

        var userData = {
          id: user.id,
          email: email,
          name: user.user_metadata?.full_name || "",
          avatar_url: user.user_metadata?.avatar_url || "",
          user_metadata: user.user_metadata || {},
          provider: "google",
          logged_in_at: new Date().toISOString(),
        };

        _setUser(userData);
        _showAuthToast(
          "Welcome, " + (userData.name || userData.email) + "!",
          "success",
        );

        // Notify backend (fire-and-forget, non-blocking)
        _notifyBackend(userData, session.access_token);
      } else if (event === "SIGNED_OUT") {
        _setUser(null);
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Backend notification (optional -- non-blocking)
  // ---------------------------------------------------------------------------
  function _notifyBackend(user, accessToken) {
    try {
      fetch("/api/auth/session", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: "Bearer " + (accessToken || ""),
        },
        body: JSON.stringify({
          user_id: user.id,
          email: user.email,
          name: user.name,
        }),
      }).catch(function () {
        // Non-blocking: backend notification is best-effort
      });
    } catch (_) {}
  }

  // ---------------------------------------------------------------------------
  // Init
  // ---------------------------------------------------------------------------
  function init(options) {
    if (_initialized) return;
    _initialized = true;

    // Read config from data attributes on script tag or options
    var scriptTag = document.querySelector('script[src*="nova-auth"]');

    AUTH_CONFIG.supabaseUrl =
      (options && options.supabaseUrl) ||
      (scriptTag && scriptTag.dataset.supabaseUrl) ||
      document.documentElement.dataset.supabaseUrl ||
      "";

    AUTH_CONFIG.supabaseAnonKey =
      (options && options.supabaseAnonKey) ||
      (scriptTag && scriptTag.dataset.supabaseAnonKey) ||
      document.documentElement.dataset.supabaseAnonKey ||
      "";

    if (options && options.allowedDomains) {
      AUTH_CONFIG.allowedDomains = options.allowedDomains;
    }

    // Load cached user for instant UI
    var cachedUser = _loadCachedUser();
    if (cachedUser) {
      _updateUI(cachedUser);
    }

    // Initialize Supabase auth listener
    _setupAuthListener();

    // Check existing session
    var sb = _getSupabase();
    if (sb) {
      sb.auth.getSession().then(function (result) {
        if (result.data && result.data.session && result.data.session.user) {
          var user = result.data.session.user;
          if (_isAllowedDomain(user.email || "")) {
            var userData = {
              id: user.id,
              email: user.email || "",
              name: user.user_metadata?.full_name || "",
              avatar_url: user.user_metadata?.avatar_url || "",
              user_metadata: user.user_metadata || {},
              provider: "google",
              logged_in_at: new Date().toISOString(),
            };
            _setUser(userData);
          }
        }
      });
    }

    // Bind click handlers
    _bindUIHandlers();
  }

  function _bindUIHandlers() {
    // Login button
    document.addEventListener("click", function (e) {
      if (
        e.target.closest("#nova-auth-login-btn") ||
        e.target.closest("[data-nova-auth-login]")
      ) {
        e.preventDefault();
        signInWithGoogle();
      }
      if (
        e.target.closest("#nova-auth-logout-btn") ||
        e.target.closest("[data-nova-auth-logout]")
      ) {
        e.preventDefault();
        signOut();
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------
  window.NovaAuth = {
    init: init,
    signIn: signInWithGoogle,
    signInWithGoogle: signInWithGoogle,
    signOut: signOut,
    getUser: function () {
      return _currentUser;
    },
    isLoggedIn: function () {
      return !!_currentUser;
    },
    isInitialized: function () {
      return _initialized;
    },
    onLogin: function (cb) {
      AUTH_CONFIG.onLoginCallbacks.push(cb);
    },
    onLogout: function (cb) {
      AUTH_CONFIG.onLogoutCallbacks.push(cb);
    },
  };
})();
