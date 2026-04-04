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

    // Floating user badge
    if (user) {
      _renderUserBadge();
    } else {
      _removeUserBadge();
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
  // Floating User Badge (top-right corner, all gated pages)
  // ---------------------------------------------------------------------------
  var _badgeEl = null;
  var _dropdownOpen = false;

  function _renderUserBadge() {
    if (!_currentUser) return;
    // Skip floating badge on pages that have their own nav with auth (hub, platform)
    var hasNav =
      document.querySelector(".nav") ||
      document.querySelector(".top-nav") ||
      document.querySelector('[class*="nav-bar"]');
    var isHub =
      window.location.pathname === "/" || window.location.pathname === "/hub";
    if (hasNav || isHub) return;
    _removeUserBadge();

    var container = document.createElement("div");
    container.id = "nova-auth-badge";
    container.style.cssText =
      "position:fixed;top:16px;right:16px;z-index:99998;font-family:Inter,system-ui,sans-serif;";

    // Avatar circle
    var avatar = document.createElement("div");
    avatar.id = "nova-auth-badge-avatar";
    avatar.style.cssText =
      "width:40px;height:40px;border-radius:50%;cursor:pointer;" +
      "background:rgba(30,30,50,0.9);border:1px solid rgba(255,255,255,0.1);" +
      "display:flex;align-items:center;justify-content:center;" +
      "overflow:hidden;transition:box-shadow 0.2s ease;";

    if (_currentUser.avatar_url || _currentUser.avatar) {
      var img = document.createElement("img");
      img.src = _currentUser.avatar_url || _currentUser.avatar || "";
      img.alt = _currentUser.name || _currentUser.email || "User";
      img.style.cssText =
        "width:100%;height:100%;object-fit:cover;border-radius:50%;";
      img.onerror = function () {
        img.style.display = "none";
        _showInitial(avatar);
      };
      avatar.appendChild(img);
    } else {
      _showInitial(avatar);
    }

    // Tooltip (name on hover)
    avatar.title = _currentUser.name || _currentUser.email || "";

    // Dropdown panel
    var dropdown = document.createElement("div");
    dropdown.id = "nova-auth-badge-dropdown";
    dropdown.style.cssText =
      "position:absolute;top:48px;right:0;min-width:220px;" +
      "background:rgba(30,30,50,0.95);border:1px solid rgba(255,255,255,0.1);" +
      "border-radius:12px;padding:12px 16px;display:none;" +
      "box-shadow:0 8px 32px rgba(0,0,0,0.4);backdrop-filter:blur(12px);";

    // User info row
    var infoRow = document.createElement("div");
    infoRow.style.cssText =
      "margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid rgba(255,255,255,0.08);";

    var nameEl = document.createElement("div");
    nameEl.style.cssText =
      "color:#fff;font-size:14px;font-weight:600;line-height:1.3;";
    nameEl.textContent =
      _currentUser.name || _currentUser.email?.split("@")[0] || "User";
    infoRow.appendChild(nameEl);

    var emailEl = document.createElement("div");
    emailEl.style.cssText =
      "color:rgba(255,255,255,0.5);font-size:12px;margin-top:2px;";
    emailEl.textContent = _currentUser.email || "";
    infoRow.appendChild(emailEl);

    dropdown.appendChild(infoRow);

    // Sign out button
    var signOutBtn = document.createElement("button");
    signOutBtn.textContent = "Sign out";
    signOutBtn.style.cssText =
      "width:100%;padding:8px 12px;border:none;border-radius:8px;cursor:pointer;" +
      "background:rgba(255,255,255,0.06);color:rgba(255,255,255,0.8);" +
      "font-size:13px;font-family:Inter,system-ui,sans-serif;text-align:left;" +
      "transition:background 0.15s ease;";
    signOutBtn.onmouseenter = function () {
      signOutBtn.style.background = "rgba(255,80,80,0.15)";
      signOutBtn.style.color = "#ff6b6b";
    };
    signOutBtn.onmouseleave = function () {
      signOutBtn.style.background = "rgba(255,255,255,0.06)";
      signOutBtn.style.color = "rgba(255,255,255,0.8)";
    };
    signOutBtn.onclick = function (e) {
      e.stopPropagation();
      signOut();
    };
    dropdown.appendChild(signOutBtn);

    container.appendChild(avatar);
    container.appendChild(dropdown);

    // Toggle dropdown on avatar click
    avatar.onclick = function (e) {
      e.stopPropagation();
      _dropdownOpen = !_dropdownOpen;
      dropdown.style.display = _dropdownOpen ? "block" : "none";
      avatar.style.boxShadow = _dropdownOpen
        ? "0 0 0 2px rgba(90,84,189,0.5)"
        : "none";
    };

    // Hover glow
    avatar.onmouseenter = function () {
      if (!_dropdownOpen)
        avatar.style.boxShadow = "0 0 0 2px rgba(90,84,189,0.3)";
    };
    avatar.onmouseleave = function () {
      if (!_dropdownOpen) avatar.style.boxShadow = "none";
    };

    // Close dropdown when clicking elsewhere
    document.addEventListener("click", _closeBadgeDropdown);

    document.body.appendChild(container);
    _badgeEl = container;
  }

  function _showInitial(parent) {
    var initial = document.createElement("span");
    initial.style.cssText =
      "color:#fff;font-size:16px;font-weight:600;text-transform:uppercase;" +
      "user-select:none;";
    var letter =
      (_currentUser.name && _currentUser.name.charAt(0)) ||
      (_currentUser.email && _currentUser.email.charAt(0)) ||
      "?";
    initial.textContent = letter;
    parent.appendChild(initial);
  }

  function _removeUserBadge() {
    if (_badgeEl) {
      _badgeEl.remove();
      _badgeEl = null;
    }
    _dropdownOpen = false;
    document.removeEventListener("click", _closeBadgeDropdown);
  }

  function _closeBadgeDropdown() {
    if (_dropdownOpen) {
      _dropdownOpen = false;
      var dd = document.getElementById("nova-auth-badge-dropdown");
      if (dd) dd.style.display = "none";
      var av = document.getElementById("nova-auth-badge-avatar");
      if (av) av.style.boxShadow = "none";
    }
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
    var _cleanup = function () {
      _setUser(null);
      try {
        localStorage.removeItem(AUTH_CONFIG.sessionKey);
      } catch (_) {}
      window.location.reload();
    };
    if (sb) {
      sb.auth
        .signOut()
        .then(function () {
          _cleanup();
        })
        .catch(function (err) {
          console.warn("[NovaAuth] Sign-out error:", err);
          _cleanup();
        });
    } else {
      _cleanup();
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
