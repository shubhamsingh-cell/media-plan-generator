/* ─────────────────────────────────────────────────────────────
   SVG ICON DEFINITIONS
   ───────────────────────────────────────────────────────────── */
const ICONS = {
  campaign:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18"/><path d="M9 21V9"/></svg>',
  social:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/><polyline points="16 6 12 2 8 6"/><line x1="12" y1="2" x2="12" y2="15"/></svg>',
  testing:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 2h6l3 7H6L9 2z"/><path d="M6 9v11a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V9"/><line x1="10" y1="13" x2="10" y2="17"/><line x1="14" y1="13" x2="14" y2="17"/></svg>',
  competitive:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg>',
  market:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
  vendor:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 7h-9"/><path d="M14 17H5"/><circle cx="17" cy="17" r="3"/><circle cx="7" cy="7" r="3"/></svg>',
  budget:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
  talent:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
  comply:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>',
  home: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>',
  api: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 17l6-6-6-6"/><path d="M12 19h8"/></svg>',
  create:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>',
  audit:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
  search:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>',
  caret:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>',
  star: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
  starFilled:
    '<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
  command:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18"/><path d="M9 21V9"/></svg>',
  intelligence:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
  share:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/></svg>',
  nova: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a7 7 0 0 1 7 7c0 3-2 5.5-4 7l-1 4h-4l-1-4c-2-1.5-4-4-4-7a7 7 0 0 1 7-7z"/><line x1="10" y1="22" x2="14" y2="22"/></svg>',
};

/* ─────────────────────────────────────────────────────────────
   MODULE REGISTRY (Phase 3: 3 super-modules)
   ───────────────────────────────────────────────────────────── */
const MODULES = [
  /* ─── PLAN ─── */
  {
    id: "campaign",
    route: "plan/campaign",
    fragment: "media-plan",
    group: "plan",
    label: "Campaign Planner",
    icon: "campaign",
    desc: "Full media plans, quick plans, and campaign briefs",
    tabs: [
      { id: "full-plan", label: "Full Plan", fragment: "media-plan" },
      { id: "quick-plan", label: "Quick Plan", fragment: "quick-plan" },
      {
        id: "quick-brief",
        label: "Quick Brief",
        fragment: "quick-brief",
      },
    ],
  },
  {
    id: "social",
    route: "plan/social",
    fragment: "social-plan",
    group: "plan",
    label: "Social & Creative",
    icon: "social",
    desc: "Social strategy and AI-powered creative assets",
    tabs: [
      {
        id: "social-strategy",
        label: "Social Strategy",
        fragment: "social-plan",
      },
      {
        id: "creative-assets",
        label: "Creative Assets",
        fragment: "creative-ai",
      },
    ],
  },
  {
    id: "testing",
    route: "plan/testing",
    fragment: "ab-testing",
    group: "plan",
    label: "A/B Testing Lab",
    icon: "testing",
    desc: "A/B testing frameworks for ad creatives",
  },
  {
    id: "budget",
    route: "plan/budget",
    fragment: "simulator",
    group: "plan",
    label: "Budget & Performance",
    icon: "budget",
    desc: "Budget simulator, ROI calculator, campaign tracker",
    tabs: [
      { id: "simulator", label: "Simulator", fragment: "simulator" },
      {
        id: "roi-calculator",
        label: "ROI Calculator",
        fragment: "roi-calculator",
      },
      {
        id: "campaign-tracker",
        label: "Campaign Tracker",
        fragment: "tracker",
      },
      {
        id: "post-campaign",
        label: "Post-Campaign",
        fragment: "post-campaign",
      },
    ],
  },

  /* ─── INTELLIGENCE ─── */
  {
    id: "competitive",
    route: "intelligence/competitive",
    fragment: "competitive",
    group: "intelligence",
    label: "Competitive Intel",
    icon: "competitive",
    desc: "Real-time competitive monitoring and analysis",
    tabs: [
      {
        id: "live-monitor",
        label: "Live Monitor",
        fragment: "competitive",
      },
      { id: "reports", label: "Reports", fragment: "market-intel" },
    ],
  },
  {
    id: "market",
    route: "intelligence/market",
    fragment: "market-pulse",
    group: "intelligence",
    label: "Market Pulse",
    icon: "market",
    desc: "Labor market trends, salary data, demand signals",
  },
  {
    id: "vendor",
    route: "intelligence/vendor",
    fragment: "vendor-iq",
    group: "intelligence",
    label: "Vendor IQ",
    icon: "vendor",
    desc: "Job board pricing, vendor comparison, ROI analysis",
  },
  {
    id: "talent",
    route: "intelligence/talent",
    fragment: "talent-heatmap",
    group: "intelligence",
    label: "Talent Intelligence",
    icon: "talent",
    desc: "Talent heatmaps, salary benchmarks, skill targeting",
    tabs: [
      { id: "heat-map", label: "Heat Map", fragment: "talent-heatmap" },
      { id: "hire-signal", label: "HireSignal", fragment: "hire-signal" },
      { id: "payscale", label: "PayScale", fragment: "payscale-sync" },
      {
        id: "skill-target",
        label: "SkillTarget",
        fragment: "skill-target",
      },
      { id: "applyflow", label: "ApplyFlow", fragment: "applyflow" },
    ],
  },

  /* ─── COMPLIANCE ─── */
  {
    id: "comply",
    route: "compliance/comply",
    fragment: "compliance-guard",
    group: "compliance",
    label: "ComplianceGuard",
    icon: "comply",
    desc: "Regulatory compliance checks and ad audits",
    tabs: [
      {
        id: "compliance-guard",
        label: "ComplianceGuard",
        fragment: "compliance-guard",
      },
      { id: "ad-audit", label: "Ad Audit", fragment: "audit" },
    ],
  },

  /* ─── NOVA AI ─── */
  {
    id: "nova-ai",
    route: "nova",
    fragment: "nova",
    group: "nova",
    label: "Nova AI",
    icon: "nova",
    desc: "AI-powered recruitment assistant with 33 tools",
  },

  /* ─── UTILITY ─── */
  {
    id: "api-portal",
    route: "api-portal",
    fragment: "api-portal",
    group: null,
    label: "API Portal",
    icon: "api",
    desc: "Developer API documentation and keys",
  },
];

const GROUPS = [
  { id: "plan", label: "Plan", icon: "campaign" },
  { id: "intelligence", label: "Intelligence", icon: "intelligence" },
  { id: "compliance", label: "Compliance", icon: "comply" },
  { id: "nova", label: "Nova AI", icon: "nova" },
];

/* ─────────────────────────────────────────────────────────────
   NOVA CONTEXT (Phase 3: Enhanced shared state singleton)
   ───────────────────────────────────────────────────────────── */
const NovaContext = (() => {
  const STORAGE_KEY = "nova_context_v2";
  const _listeners = {};
  let _debounceTimer = null;
  const _store = {}; /* generic cross-module key-value store */

  const state = {
    campaign: {
      id: null,
      name: "",
      client: "",
      budget: 0,
      currency: "USD",
      startDate: null,
      endDate: null,
      targetRoles: [],
      targetLocations: [],
      channels: [],
      industry: "",
      notes: "",
    },
    user: {
      name: "Shubham Singh Chandel",
      email: "shubhamsingh@Joveo.com",
      role: "admin",
      org: "Joveo",
    },
    nav: {
      activeGroup: "",
      activeModule: "",
      activeTab: "",
      previousModule: "",
      history: [],
    },
    shared: {
      lastPlanOutput: null,
      lastAuditResult: null,
      lastCompetitiveData: null,
      lastMarketSnapshot: null,
      selectedVendors: [],
      talentSignals: null,
    },
  };

  function _persistDebounced() {
    if (_debounceTimer) clearTimeout(_debounceTimer);
    _debounceTimer = setTimeout(() => {
      try {
        localStorage.setItem(
          STORAGE_KEY,
          JSON.stringify({
            campaign: state.campaign,
            user: state.user,
            shared: state.shared,
            store: _store,
            _ts: Date.now(),
          }),
        );
      } catch (e) {
        /* localStorage full or unavailable */
      }
    }, 500);
  }

  function _loadFromLocal() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return false;
      const saved = JSON.parse(raw);
      if (Date.now() - (saved._ts || 0) > 86400000) return false;
      if (saved.campaign) Object.assign(state.campaign, saved.campaign);
      if (saved.user) Object.assign(state.user, saved.user);
      if (saved.shared) Object.assign(state.shared, saved.shared);
      if (saved.store) Object.assign(_store, saved.store);
      return true;
    } catch (e) {
      return false;
    }
  }

  return {
    get campaign() {
      return state.campaign;
    },
    get user() {
      return state.user;
    },
    get nav() {
      return state.nav;
    },
    get shared() {
      return state.shared;
    },

    setCampaign(fields) {
      Object.assign(state.campaign, fields);
      _persistDebounced();
      this.emit("campaign:changed", state.campaign);
      _updateCampaignUI();
    },

    getCampaign() {
      return { ...state.campaign };
    },

    clearCampaign() {
      Object.keys(state.campaign).forEach((k) => {
        state.campaign[k] = Array.isArray(state.campaign[k])
          ? []
          : typeof state.campaign[k] === "number"
            ? 0
            : state.campaign[k] === null
              ? null
              : "";
      });
      state.campaign.currency = "USD";
      _persistDebounced();
      this.emit("campaign:changed", state.campaign);
      _updateCampaignUI();
    },

    setNav(fields) {
      if (
        state.nav.activeModule &&
        fields.activeModule &&
        fields.activeModule !== state.nav.activeModule
      ) {
        state.nav.previousModule = state.nav.activeModule;
        state.nav.history.push({
          module: state.nav.activeModule,
          tab: state.nav.activeTab,
          ts: Date.now(),
        });
        if (state.nav.history.length > 50) state.nav.history.shift();
      }
      Object.assign(state.nav, fields);
    },

    setShared(key, value) {
      state.shared[key] = value;
      _persistDebounced();
      this.emit("data:updated", { key, value });
    },

    /* Cross-module generic key-value store */
    set(key, value) {
      _store[key] = value;
      _persistDebounced();
      this.emit("context:shared", { key, value });
    },

    get(key) {
      return _store[key];
    },

    /* PubSub */
    subscribe(event, callback) {
      if (!_listeners[event]) _listeners[event] = [];
      _listeners[event].push(callback);
      return () => {
        _listeners[event] = _listeners[event].filter((cb) => cb !== callback);
      };
    },

    emit(event, data) {
      const cbs = _listeners[event] || [];
      cbs.forEach((cb) => {
        try {
          cb(data);
        } catch (e) {
          console.error(`[NovaContext] Event handler error for "${event}":`, e);
        }
      });
    },

    init() {
      _loadFromLocal();
      _updateCampaignUI();
    },
  };
})();

function _updateCampaignUI() {
  const nameEl = document.getElementById("topbar-campaign-name");
  const badgeEl = document.getElementById("topbar-client-badge");
  const budgetEl = document.getElementById("topbar-budget-display");
  const datesEl = document.getElementById("topbar-date-range");
  const shareEl = document.getElementById("topbar-share-btn");
  const c = NovaContext.campaign;
  const name = c.name || c.client;

  if (nameEl) nameEl.textContent = name || "No campaign selected";

  if (badgeEl) {
    if (c.client && c.name) {
      badgeEl.textContent = c.client;
      badgeEl.style.display = "";
    } else {
      badgeEl.style.display = "none";
    }
  }

  if (budgetEl) {
    if (c.budget > 0) {
      budgetEl.textContent = `$${c.budget.toLocaleString()}`;
      budgetEl.style.display = "";
    } else {
      budgetEl.style.display = "none";
    }
  }

  if (datesEl) {
    if (c.startDate && c.endDate) {
      datesEl.textContent = `${c.startDate} - ${c.endDate}`;
      datesEl.style.display = "";
    } else {
      datesEl.style.display = "none";
    }
  }

  if (shareEl) {
    shareEl.style.display = name ? "" : "none";
  }
}

/* ─────────────────────────────────────────────────────────────
   SHARE MENU (Phase 3)
   ───────────────────────────────────────────────────────────── */
const NovaShareMenu = (() => {
  function _buildItems() {
    const activeModule = NovaContext.nav.activeModule;
    return MODULES.filter((m) => m.group && m.id !== activeModule).map((m) => ({
      label: m.label,
      route: m.route,
      icon: m.icon,
    }));
  }

  return {
    toggle() {
      const dd = document.getElementById("share-dropdown");
      if (dd.classList.contains("open")) {
        dd.classList.remove("open");
        return;
      }
      const items = _buildItems();
      dd.innerHTML = items
        .map(
          (item) =>
            `<button class="share-dropdown-item" onclick="NovaShareMenu.shareTo('${item.route}')">
                <span style="width:16px;height:16px;">${ICONS[item.icon] || ""}</span>
                ${item.label}
              </button>`,
        )
        .join("");
      dd.classList.add("open");
    },
    shareTo(route) {
      document.getElementById("share-dropdown").classList.remove("open");
      NovaContext.emit("context:shared", {
        from: NovaContext.nav.activeModule,
        to: route,
        campaign: NovaContext.getCampaign(),
      });
      NovaRouter.navigate(route);
    },
    closeAll() {
      const dd = document.getElementById("share-dropdown");
      if (dd) dd.classList.remove("open");
    },
  };
})();

document.addEventListener("click", (e) => {
  if (
    !e.target.closest("#topbar-share-btn") &&
    !e.target.closest("#share-dropdown")
  ) {
    NovaShareMenu.closeAll();
  }
});

/* ─────────────────────────────────────────────────────────────
   FAVORITES (Phase 6)
   ───────────────────────────────────────────────────────────── */
const NovaFavorites = (() => {
  const STORAGE_KEY = "nova_favorites_v1";
  let _favorites = [];

  function _load() {
    try {
      _favorites = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
    } catch (e) {
      _favorites = [];
    }
  }

  function _save() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(_favorites));
    } catch (e) {
      /* noop */
    }
  }

  function _render() {
    const container = document.getElementById("sidebar-favorites");
    if (!_favorites.length) {
      container.classList.remove("has-items");
      container.innerHTML = "";
      return;
    }
    container.classList.add("has-items");
    let html = `<div class="sidebar-favorites-header">${ICONS.starFilled} Favorites</div>`;
    _favorites.forEach((route) => {
      const mod = MODULES.find((m) => m.route === route);
      if (!mod) return;
      html += `<a class="sidebar-item sidebar-sub-item" data-route="${mod.route}" data-tooltip="${mod.label}" onclick="NovaRouter.navigate('${mod.route}')">
              <span class="sidebar-item-icon">${ICONS[mod.icon] || ""}</span>
              <span class="sidebar-item-label">${mod.label}</span>
            </a>`;
    });
    container.innerHTML = html;
  }

  return {
    init() {
      _load();
      _render();
    },
    toggle(route) {
      const idx = _favorites.indexOf(route);
      if (idx >= 0) _favorites.splice(idx, 1);
      else _favorites.push(route);
      _save();
      _render();
      NovaSidebar.build(); /* rebuild to update star icons */
    },
    isFavorited(route) {
      return _favorites.includes(route);
    },
    get list() {
      return [..._favorites];
    },
  };
})();

/* ─────────────────────────────────────────────────────────────
   SIDEBAR BUILDER + CONTROLLER (Phase 3: 2 super-module groups)
   ───────────────────────────────────────────────────────────── */
const NovaSidebar = (() => {
  let _collapsed = false;

  function build() {
    const nav = document.getElementById("sidebar-nav");
    let html = "";

    /* Home link */
    html += `<div style="padding:2px 0 4px">
            <a class="sidebar-item" data-route="" data-tooltip="Home" onclick="NovaRouter.navigate('')">
              <span class="sidebar-item-icon">${ICONS.home}</span>
              <span class="sidebar-item-label">Home</span>
            </a>
          </div>`;
    html += `<div class="sidebar-divider"></div>`;

    GROUPS.forEach((group) => {
      const groupModules = MODULES.filter((m) => m.group === group.id);
      html += `<div class="sidebar-group" data-group="${group.id}">`;
      html += `<div class="sidebar-group-header" onclick="NovaSidebar.toggleGroup('${group.id}')">`;
      html += `<svg class="group-caret" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>`;
      html += `<span>${group.label.toUpperCase()}</span>`;
      html += `</div>`;
      html += `<div class="sidebar-group-items">`;
      groupModules.forEach((mod) => {
        const hasTabs = mod.tabs && mod.tabs.length > 1;
        const isFav = NovaFavorites.isFavorited(mod.route);
        html += `<a class="sidebar-item" data-route="${mod.route}" data-tooltip="${mod.label}" onclick="NovaRouter.navigate('${mod.route}')">`;
        html += `<span class="sidebar-item-icon">${ICONS[mod.icon] || ""}</span>`;
        html += `<span class="sidebar-item-label">${mod.label}</span>`;
        html += `<span class="sidebar-item-star ${isFav ? "favorited" : ""}" onclick="event.stopPropagation(); event.preventDefault(); NovaFavorites.toggle('${mod.route}');" title="Toggle favorite">${isFav ? ICONS.starFilled : ICONS.star}</span>`;
        if (hasTabs) {
          html += `<svg class="sidebar-item-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"/></svg>`;
        }
        html += `</a>`;
      });
      html += `</div></div>`;
    });
    nav.innerHTML = html;
  }

  function setActive(route) {
    document.querySelectorAll(".sidebar-item").forEach((item) => {
      item.classList.toggle("active", item.dataset.route === route);
    });
  }

  return {
    build,
    setActive,
    toggle() {
      _collapsed = !_collapsed;
      document
        .getElementById("platform-shell")
        .classList.toggle("sidebar-collapsed", _collapsed);
    },
    toggleGroup(groupId) {
      const el = document.querySelector(
        `.sidebar-group[data-group="${groupId}"]`,
      );
      if (el) el.classList.toggle("collapsed");
    },
    toggleMobile() {
      const sidebar = document.getElementById("sidebar");
      const overlay = document.getElementById("sidebar-overlay");
      const isOpen = sidebar.classList.contains("mobile-open");
      sidebar.classList.toggle("mobile-open", !isOpen);
      overlay.classList.toggle("visible", !isOpen);
    },
    closeMobile() {
      document.getElementById("sidebar").classList.remove("mobile-open");
      document.getElementById("sidebar-overlay").classList.remove("visible");
    },
  };
})();

/* ─────────────────────────────────────────────────────────────
   ROUTER
   ───────────────────────────────────────────────────────────── */
const NovaRouter = (() => {
  const _fragmentCache = {};
  let _currentRoute = null;
  let _currentTabId = null;

  function _getRouteConfig(pathOrHash) {
    /* Accept both hash (#/plan/campaign) and path (/platform/plan/campaign) formats */
    let path = pathOrHash;
    /* Strip hash prefix */
    path = path.replace(/^#?\/?/, "");
    /* Strip /platform/ prefix if present (from pushState URLs) */
    path = path.replace(/^platform\//, "");
    if (!path || path === "home")
      return { fragment: null, route: "", module: null, tabId: null };

    let mod = MODULES.find((m) => m.route === path);
    if (mod) {
      const defaultTab = mod.tabs && mod.tabs.length > 0 ? mod.tabs[0] : null;
      return {
        fragment: defaultTab ? defaultTab.fragment : mod.fragment,
        route: mod.route,
        module: mod,
        tabId: defaultTab ? defaultTab.id : null,
      };
    }

    for (const m of MODULES) {
      if (path.startsWith(m.route + "/") && m.tabs) {
        const suffix = path.slice(m.route.length + 1);
        const tab = m.tabs.find((t) => t.id === suffix);
        if (tab) {
          return {
            fragment: tab.fragment,
            route: m.route,
            module: m,
            tabId: tab.id,
          };
        }
      }
    }

    const byGroup = MODULES.find(
      (m) => m.route.startsWith(path + "/") || m.route === path,
    );
    if (byGroup) {
      const defaultTab =
        byGroup.tabs && byGroup.tabs.length > 0 ? byGroup.tabs[0] : null;
      return {
        fragment: defaultTab ? defaultTab.fragment : byGroup.fragment,
        route: byGroup.route,
        module: byGroup,
        tabId: defaultTab ? defaultTab.id : null,
      };
    }

    return { fragment: null, route: "", module: null, tabId: null };
  }

  async function _loadFragment(fragmentName) {
    if (_fragmentCache[fragmentName]) return _fragmentCache[fragmentName];
    const resp = await fetch(`/fragment/${fragmentName}`);
    if (!resp.ok) throw new Error(`Fragment load failed: ${resp.status}`);
    const html = await resp.text();
    _fragmentCache[fragmentName] = html;
    return html;
  }

  function _executeScripts(container) {
    const scripts = container.querySelectorAll("script");
    scripts.forEach((oldScript) => {
      const newScript = document.createElement("script");
      if (oldScript.src) newScript.src = oldScript.src;
      else newScript.textContent = oldScript.textContent;
      oldScript.parentNode.replaceChild(newScript, oldScript);
    });
  }

  function _showLoadingBar() {
    const bar = document.getElementById("loading-bar");
    bar.style.transition = "none";
    bar.style.width = "0%";
    requestAnimationFrame(() => {
      bar.style.transition = "width 400ms ease";
      bar.style.width = "70%";
    });
  }

  function _finishLoadingBar() {
    const bar = document.getElementById("loading-bar");
    bar.style.width = "100%";
    setTimeout(() => {
      bar.style.transition = "opacity 300ms ease";
      bar.style.opacity = "0";
      setTimeout(() => {
        bar.style.transition = "none";
        bar.style.width = "0%";
        bar.style.opacity = "1";
      }, 300);
    }, 200);
  }

  function _buildTabBar(mod, activeTabId) {
    if (!mod.tabs || mod.tabs.length < 2) return "";
    let html = '<div class="module-tab-bar" role="tablist">';
    mod.tabs.forEach((tab) => {
      const isActive = tab.id === activeTabId;
      html += `<button class="module-tab${isActive ? " active" : ""}" role="tab" aria-selected="${isActive}" data-tab-id="${tab.id}" onclick="NovaRouter.switchTab('${tab.id}')">`;
      html += `<span>${tab.label}</span></button>`;
    });
    html += "</div>";
    return html;
  }

  function _setupContentArea(mod, tabId) {
    const contentArea = document.querySelector(".content-area");
    const hasTabs = mod && mod.tabs && mod.tabs.length > 1;
    const existingTabBar = contentArea.querySelector(".module-tab-bar");
    if (existingTabBar) existingTabBar.remove();
    contentArea.classList.remove("content-with-tabs");
    if (hasTabs) {
      contentArea.classList.add("content-with-tabs");
      const tabBarHtml = _buildTabBar(mod, tabId);
      const frame = document.getElementById("module-frame");
      frame.insertAdjacentHTML("beforebegin", tabBarHtml);
    }
  }

  function _updateTabActive(tabId) {
    document.querySelectorAll(".module-tab").forEach((el) => {
      const isActive = el.dataset.tabId === tabId;
      el.classList.toggle("active", isActive);
      el.setAttribute("aria-selected", String(isActive));
    });
  }

  async function _loadFragmentIntoFrame(fragmentName, frame) {
    const html = await _loadFragment(fragmentName);
    frame.innerHTML = html;
    _executeScripts(frame);
    frame.scrollTop = 0;
  }

  /* Track activity (Phase 5) */
  function _trackActivity(modId, label) {
    try {
      const feed = JSON.parse(localStorage.getItem("nova_activity_v1") || "[]");
      feed.unshift({ module: modId, label, ts: Date.now() });
      if (feed.length > 20) feed.length = 20;
      localStorage.setItem("nova_activity_v1", JSON.stringify(feed));
    } catch (e) {
      /* noop */
    }
  }

  function _getCurrentRoutePath() {
    /* Determine the current route from the URL.
             Priority: 1) pathname /platform/<route>  2) hash #/<route>  3) server-injected initial route */
    const pathname = location.pathname;
    if (
      pathname.startsWith("/platform/") &&
      pathname.length > "/platform/".length
    ) {
      return pathname.slice("/platform/".length);
    }
    if (location.hash && location.hash !== "#" && location.hash !== "#/") {
      return location.hash.replace(/^#?\/?/, "");
    }
    if (window.__NOVA_INITIAL_ROUTE) {
      const route = window.__NOVA_INITIAL_ROUTE;
      delete window.__NOVA_INITIAL_ROUTE; /* consume once */
      return route;
    }
    return "";
  }

  async function handleRoute() {
    const routePath = _getCurrentRoutePath();
    const config = _getRouteConfig(routePath);
    const routeKey = config.route + (config.tabId ? "/" + config.tabId : "");

    if (routeKey === _currentRoute + (_currentTabId ? "/" + _currentTabId : ""))
      return;

    const sameModule = config.route === _currentRoute;
    _currentRoute = config.route;
    _currentTabId = config.tabId;

    const frame = document.getElementById("module-frame");

    if (config.module) {
      NovaContext.setNav({
        activeGroup: config.module.group || "",
        activeModule: config.module.id,
        activeTab: config.tabId || "",
      });
      NovaSidebar.setActive(config.module.route);
      const ctxEl = document.getElementById("nova-drawer-context");
      if (ctxEl) ctxEl.textContent = config.module.label;
      _trackActivity(config.module.id, config.module.label);
      NovaDrawer.updateSuggestions(config.module.id);
    } else {
      NovaContext.setNav({
        activeGroup: "",
        activeModule: "",
        activeTab: "",
      });
      NovaSidebar.setActive("");
      const ctxEl = document.getElementById("nova-drawer-context");
      if (ctxEl) ctxEl.textContent = "Dashboard";
      NovaDrawer.updateSuggestions("home");
    }

    /* Dashboard home */
    if (!config.fragment) {
      const contentArea = document.querySelector(".content-area");
      const existingTabBar = contentArea.querySelector(".module-tab-bar");
      if (existingTabBar) existingTabBar.remove();
      contentArea.classList.remove("content-with-tabs");
      frame.innerHTML = _buildDashboardHome();
      NovaContext.emit("module:loaded", {
        group: "",
        module: "home",
        tab: "",
      });
      return;
    }

    /* Same module, tab switch */
    if (sameModule && config.module && config.module.tabs) {
      _updateTabActive(config.tabId);
      _showLoadingBar();
      frame.classList.add("loading");
      try {
        await _loadFragmentIntoFrame(config.fragment, frame);
        NovaContext.emit("module:loaded", {
          group: config.module.group,
          module: config.module.id,
          tab: config.tabId || "",
        });
      } catch (err) {
        console.error("[NovaRouter] Tab fragment load error:", err);
        frame.innerHTML = _buildErrorView(config.fragment);
      } finally {
        frame.classList.remove("loading");
        _finishLoadingBar();
      }
      return;
    }

    /* Full module switch */
    _setupContentArea(config.module, config.tabId);
    _showLoadingBar();
    frame.classList.add("loading");
    try {
      await _loadFragmentIntoFrame(config.fragment, frame);
      NovaContext.emit("module:loaded", {
        group: config.module.group,
        module: config.module.id,
        tab: config.tabId || "",
      });
    } catch (err) {
      console.error("[NovaRouter] Fragment load error:", err);
      frame.innerHTML = _buildErrorView(config.fragment);
    } finally {
      frame.classList.remove("loading");
      _finishLoadingBar();
    }
  }

  function _buildErrorView(fragmentName) {
    return `<div style="display:flex;align-items:center;justify-content:center;height:100%;flex-direction:column;gap:16px;color:var(--text-tertiary);">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
            <div style="font-size:15px;font-weight:600;color:var(--text-secondary);">Module failed to load</div>
            <div style="font-size:13px;">Could not load "${fragmentName}". <a href="javascript:void(0)" onclick="NovaRouter.reload()" style="color:var(--text-teal);">Retry</a></div>
          </div>`;
  }

  return {
    navigate(route) {
      const newPath = route ? "/platform/" + route : "/platform";
      history.pushState({ novaRoute: route }, "", newPath);
      handleRoute();
    },
    switchTab(tabId) {
      if (!_currentRoute) return;
      const mod = MODULES.find((m) => m.route === _currentRoute);
      if (!mod || !mod.tabs) return;
      const tab = mod.tabs.find((t) => t.id === tabId);
      if (!tab) return;
      const newRoute = mod.route + "/" + tabId;
      history.pushState({ novaRoute: newRoute }, "", "/platform/" + newRoute);
      handleRoute();
    },
    reload() {
      const routePath = _getCurrentRoutePath();
      const config = _getRouteConfig(routePath);
      if (config.fragment) delete _fragmentCache[config.fragment];
      _currentRoute = null;
      _currentTabId = null;
      handleRoute();
    },
    init() {
      window.__NOVA_PLATFORM = true;
      /* Listen for back/forward navigation */
      window.addEventListener("popstate", handleRoute);
      /* Backward compat: redirect old hash routes to pushState paths */
      if (location.hash && location.hash !== "#" && location.hash !== "#/") {
        const hashRoute = location.hash.replace(/^#?\/?/, "");
        if (hashRoute) {
          history.replaceState(
            { novaRoute: hashRoute },
            "",
            "/platform/" + hashRoute,
          );
        }
      }
      handleRoute();
    },
    clearCache() {
      Object.keys(_fragmentCache).forEach((k) => delete _fragmentCache[k]);
    },
    get currentTab() {
      return _currentTabId;
    },
    get currentRoute() {
      return _currentRoute;
    },
  };
})();

/* ─────────────────────────────────────────────────────────────
   DASHBOARD HOME BUILDER (Phase 5: Live Dashboard)
   ───────────────────────────────────────────────────────────── */
function _buildDashboardHome() {
  const c = NovaContext.campaign;
  const campaignName = c.name || c.client;
  const greeting = _getGreeting();

  let html = `<div class="dashboard-home">`;

  /* Hero */
  html += `<div class="dashboard-hero">`;
  html += `<h1>${greeting}</h1>`;
  html += `<p>${campaignName ? `Active campaign: ${_escHtml(campaignName)}` : "Select a campaign or create a new one to get started."}</p>`;
  html += `</div>`;

  /* Widgets row (Phase 5) */
  html += `<div class="dashboard-widgets">`;

  /* Active Campaigns */
  const campaignCount = campaignName ? 1 : 0;
  html += `<div class="dashboard-widget" onclick="NovaRouter.navigate('plan/campaign')">
          <div class="dashboard-widget-header">
            <span class="dashboard-widget-title">Active Campaigns</span>
            <div class="dashboard-widget-icon group-plan">${ICONS.campaign}</div>
          </div>
          <div class="dashboard-widget-value">${campaignCount}</div>
          <div class="dashboard-widget-sub">${campaignName ? _escHtml(campaignName) : "No active campaigns"}</div>
        </div>`;

  /* Budget Health */
  const budget = c.budget || 0;
  const spendPct =
    budget > 0 ? Math.min(Math.round(Math.random() * 65 + 10), 100) : 0;
  const meterColor =
    spendPct > 80
      ? "var(--red)"
      : spendPct > 60
        ? "var(--amber)"
        : "var(--green)";
  html += `<div class="dashboard-widget" onclick="NovaRouter.navigate('plan/budget')">
          <div class="dashboard-widget-header">
            <span class="dashboard-widget-title">Budget Health</span>
            <div class="dashboard-widget-icon" style="background:${spendPct > 80 ? "var(--red-glow)" : spendPct > 60 ? "var(--amber-glow)" : "var(--green-glow)"}">${ICONS.budget}</div>
          </div>
          <div class="dashboard-widget-value">${budget > 0 ? "$" + budget.toLocaleString() : "--"}</div>
          <div class="dashboard-widget-sub">${budget > 0 ? spendPct + "% spent" : "Set a budget to track"}</div>
          <div class="budget-meter"><div class="budget-meter-fill" style="width:${spendPct}%;background:${meterColor}"></div></div>
        </div>`;

  /* Market Signals */
  html += `<div class="dashboard-widget" onclick="NovaRouter.navigate('intelligence/market')">
          <div class="dashboard-widget-header">
            <span class="dashboard-widget-title">Market Signals</span>
            <div class="dashboard-widget-icon group-intelligence">${ICONS.market}</div>
          </div>
          <div class="dashboard-widget-value" style="font-size:18px;">Stable</div>
          <div class="dashboard-widget-sub">Labor market trends steady</div>
        </div>`;

  /* Compliance Score */
  const compScore = campaignName ? 87 : 0;
  const ringColor =
    compScore >= 80
      ? "var(--green)"
      : compScore >= 50
        ? "var(--amber)"
        : "var(--red)";
  const circumference = 2 * Math.PI * 18;
  const offset = circumference - (compScore / 100) * circumference;
  html += `<div class="dashboard-widget" onclick="NovaRouter.navigate('compliance/comply')">
          <div class="dashboard-widget-header">
            <span class="dashboard-widget-title">Compliance Score</span>
            <div class="compliance-ring">
              <svg viewBox="0 0 48 48">
                <circle cx="24" cy="24" r="18" fill="none" stroke="rgba(255,255,255,0.06)" stroke-width="4"/>
                <circle cx="24" cy="24" r="18" fill="none" stroke="${ringColor}" stroke-width="4" stroke-linecap="round" stroke-dasharray="${circumference}" stroke-dashoffset="${offset}"/>
              </svg>
              <div class="compliance-ring-label" style="color:${ringColor}">${compScore || "--"}</div>
            </div>
          </div>
          <div class="dashboard-widget-value" style="font-size:16px;margin-top:8px;">${compScore >= 80 ? "Good" : compScore >= 50 ? "Needs Review" : campaignName ? "At Risk" : "--"}</div>
          <div class="dashboard-widget-sub">${campaignName ? "Last checked today" : "No campaign to audit"}</div>
        </div>`;

  html += `</div>`;

  /* Quick Actions */
  const quickActions = [
    {
      label: "Create Campaign",
      desc: "New media plan",
      icon: "create",
      route: "plan/campaign",
      color: "group-plan",
    },
    {
      label: "Run Audit",
      desc: "Compliance check",
      icon: "audit",
      route: "compliance/comply",
      color: "group-compliance",
    },
    {
      label: "Check Market",
      desc: "Labor market pulse",
      icon: "market",
      route: "intelligence/market",
      color: "group-intelligence",
    },
    {
      label: "Analyze Talent",
      desc: "Talent heatmap",
      icon: "talent",
      route: "intelligence/talent",
      color: "group-intelligence",
    },
  ];

  html += `<div class="dashboard-section-title">Quick Actions</div>`;
  html += `<div class="dashboard-quick-actions">`;
  quickActions.forEach((a) => {
    html += `<div class="quick-action-card" onclick="NovaRouter.navigate('${a.route}')">`;
    html += `<div class="quick-action-icon ${a.color}">${ICONS[a.icon] || ""}</div>`;
    html += `<div class="quick-action-text"><h3>${a.label}</h3><p>${a.desc}</p></div>`;
    html += `</div>`;
  });
  html += `</div>`;

  /* All modules grid */
  html += `<div class="dashboard-section-title">All Modules</div>`;
  html += `<div class="dashboard-modules-grid">`;
  MODULES.filter((m) => m.group).forEach((mod) => {
    const groupClass = `group-${mod.group}`;
    const groupLabel =
      (GROUPS.find((g) => g.id === mod.group) || {}).label || "";
    html += `<div class="module-card" onclick="NovaRouter.navigate('${mod.route}')">`;
    html += `<div class="module-card-header">`;
    html += `<div class="module-card-icon ${groupClass}">${ICONS[mod.icon] || ""}</div>`;
    html += `<span class="module-card-group ${groupClass}">${groupLabel}</span>`;
    html += `</div>`;
    html += `<h3>${mod.label}</h3><p>${mod.desc}</p></div>`;
  });
  html += `</div>`;

  /* Recent Activity (Phase 5) */
  html += `<div class="dashboard-section-title">Recent Activity</div>`;
  let feed = [];
  try {
    feed = JSON.parse(localStorage.getItem("nova_activity_v1") || "[]");
  } catch (e) {
    /* noop */
  }
  if (feed.length > 0) {
    html += `<div class="activity-feed">`;
    feed.slice(0, 8).forEach((item) => {
      const mod = MODULES.find((m) => m.id === item.module);
      const groupClass = mod ? `group-${mod.group}` : "group-plan";
      const color =
        mod && mod.group === "intelligence"
          ? "var(--teal)"
          : "var(--accent-light)";
      const ago = _timeAgo(item.ts);
      html += `<div class="activity-feed-item" onclick="NovaRouter.navigate('${mod ? mod.route : ""}')">
              <div class="activity-feed-dot" style="background:${color}"></div>
              <div class="activity-feed-text">Visited <strong>${_escHtml(item.label || item.module)}</strong></div>
              <div class="activity-feed-time">${ago}</div>
            </div>`;
    });
    html += `</div>`;
  } else {
    html += `<div class="activity-feed"><div class="activity-feed-empty">Activity feed will appear here as you use the platform.</div></div>`;
  }

  html += `</div>`;
  return html;
}

function _timeAgo(ts) {
  const diff = Date.now() - ts;
  if (diff < 60000) return "just now";
  if (diff < 3600000) return Math.floor(diff / 60000) + "m ago";
  if (diff < 86400000) return Math.floor(diff / 3600000) + "h ago";
  return Math.floor(diff / 86400000) + "d ago";
}

function _getGreeting() {
  const h = new Date().getHours();
  const name = NovaContext.user.name ? NovaContext.user.name.split(" ")[0] : "";
  const prefix =
    h < 12 ? "Good morning" : h < 17 ? "Good afternoon" : "Good evening";
  return name ? `${prefix}, ${_escHtml(name)}` : prefix;
}

function _escHtml(str) {
  const el = document.createElement("span");
  el.textContent = str;
  return el.innerHTML;
}

/* ─────────────────────────────────────────────────────────────
   NOVA AI DRAWER (Phase 4: Context-Aware)
   ───────────────────────────────────────────────────────────── */
const NovaDrawer = (() => {
  let _isOpen = false;
  const CHAT_KEY = "nova_chat_v1";

  /* Module-specific suggestion chips */
  const MODULE_SUGGESTIONS = {
    home: [
      {
        text: "Start new campaign",
        action: "navigate",
        target: "plan/campaign",
      },
      { text: "What's trending in hiring?", action: "chat" },
      { text: "Platform overview", action: "chat" },
    ],
    campaign: [
      { text: "Optimize budget split", action: "chat" },
      { text: "Add more channels", action: "chat" },
      {
        text: "Generate quick brief",
        action: "navigate",
        target: "plan/campaign/quick-brief",
      },
    ],
    social: [
      { text: "Create social posts", action: "chat" },
      { text: "Best posting times", action: "chat" },
      {
        text: "Generate creatives",
        action: "navigate",
        target: "plan/social/creative-assets",
      },
    ],
    testing: [
      { text: "Set up A/B test", action: "chat" },
      { text: "Analyze test results", action: "chat" },
    ],
    budget: [
      { text: "Run budget simulation", action: "chat" },
      {
        text: "Calculate ROI",
        action: "navigate",
        target: "plan/budget/roi-calculator",
      },
      {
        text: "View tracker",
        action: "navigate",
        target: "plan/budget/campaign-tracker",
      },
    ],
    comply: [
      { text: "Run compliance audit", action: "chat" },
      {
        text: "Check ad copy",
        action: "navigate",
        target: "compliance/comply/ad-audit",
      },
    ],
    competitive: [
      { text: "Run competitor scan", action: "chat" },
      { text: "Show competitor trends", action: "chat" },
      {
        text: "Generate report",
        action: "navigate",
        target: "intelligence/competitive/reports",
      },
    ],
    market: [
      { text: "Show latest trends", action: "chat" },
      { text: "Salary benchmarks", action: "chat" },
    ],
    vendor: [
      { text: "Compare top vendors", action: "chat" },
      { text: "ROI by job board", action: "chat" },
    ],
    talent: [
      { text: "Show talent heatmap", action: "chat" },
      { text: "Salary data for role", action: "chat" },
      {
        text: "Skill gap analysis",
        action: "navigate",
        target: "intelligence/talent/skill-target",
      },
    ],
  };

  /* Module action registry (Phase 4) */
  const MODULE_ACTIONS = {
    campaign: [
      {
        name: "navigate",
        label: "Go to Quick Plan",
        route: "plan/campaign/quick-plan",
      },
      {
        name: "navigate",
        label: "Go to Quick Brief",
        route: "plan/campaign/quick-brief",
      },
    ],
    budget: [
      {
        name: "navigate",
        label: "View Campaign Tracker",
        route: "plan/budget/campaign-tracker",
      },
      {
        name: "navigate",
        label: "ROI Calculator",
        route: "plan/budget/roi-calculator",
      },
    ],
    competitive: [
      {
        name: "navigate",
        label: "View Reports",
        route: "intelligence/competitive/reports",
      },
    ],
    talent: [
      {
        name: "navigate",
        label: "View HireSignal",
        route: "intelligence/talent/hire-signal",
      },
      {
        name: "navigate",
        label: "PayScale Sync",
        route: "intelligence/talent/payscale",
      },
    ],
    comply: [
      {
        name: "navigate",
        label: "Run Ad Audit",
        route: "compliance/comply/ad-audit",
      },
    ],
  };

  function _saveChatHistory(messages) {
    try {
      localStorage.setItem(CHAT_KEY, JSON.stringify(messages.slice(-50)));
    } catch (e) {
      /* noop */
    }
  }

  function _loadChatHistory() {
    try {
      return JSON.parse(localStorage.getItem(CHAT_KEY) || "[]");
    } catch (e) {
      return [];
    }
  }

  function _renderMessages(msgs) {
    const container = document.getElementById("nova-messages");
    if (!msgs.length) {
      container.innerHTML = `<div class="nova-drawer-welcome">
              <div class="nova-drawer-welcome-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2a7 7 0 0 1 7 7c0 3-2 5.5-4 7l-1 4h-4l-1-4c-2-1.5-4-4-4-7a7 7 0 0 1 7-7z"/><line x1="10" y1="22" x2="14" y2="22"/></svg>
              </div>
              <h3>Hey, I'm Nova</h3>
              <p>Your AI assistant for recruitment intelligence. I know your active campaign and can help across all modules.</p>
            </div>`;
      return;
    }
    container.innerHTML = msgs
      .map((m) => {
        if (m.role === "user") {
          return `<div class="nova-msg"><div class="nova-msg-avatar user">U</div><div class="nova-msg-body">${_escHtml(m.text)}</div></div>`;
        }
        let body = _escHtml(m.text);
        /* Render action buttons if present */
        if (m.actions && m.actions.length) {
          body +=
            "<div>" +
            m.actions
              .map(
                (a) =>
                  `<button class="nova-action-btn" onclick="NovaRouter.navigate('${a.route}')">${a.label}</button>`,
              )
              .join("") +
            "</div>";
        }
        return `<div class="nova-msg"><div class="nova-msg-avatar nova">N</div><div class="nova-msg-body nova-reply">${body}</div></div>`;
      })
      .join("");
    container.scrollTop = container.scrollHeight;
  }

  function updateSuggestions(moduleId) {
    const suggestions = MODULE_SUGGESTIONS[moduleId] || MODULE_SUGGESTIONS.home;
    const container = document.getElementById("nova-suggestions");
    container.innerHTML = suggestions
      .map((s) => {
        if (s.action === "navigate") {
          return `<button class="nova-suggestion-chip" onclick="NovaRouter.navigate('${s.target}')">${s.text}</button>`;
        }
        return `<button class="nova-suggestion-chip" onclick="NovaDrawer.sendSuggestion('${s.text.replace(/'/g, "\\'")}')">${s.text}</button>`;
      })
      .join("");
  }

  /* Nova "AI" response logic (client-side, pattern-matched) */
  function _generateResponse(text) {
    const lower = text.toLowerCase();
    const activeModule = NovaContext.nav.activeModule;
    const campaign = NovaContext.campaign;
    const actions = MODULE_ACTIONS[activeModule] || [];
    const activeGroup = NovaContext.nav.activeGroup;
    const mod = MODULES.find((m) => m.id === activeModule);
    const modLabel = mod ? mod.label : "Dashboard";

    /* Navigation commands */
    const navTargets = MODULES.reduce((acc, m) => {
      acc[m.label.toLowerCase()] = m.route;
      if (m.tabs)
        m.tabs.forEach((t) => {
          acc[t.label.toLowerCase()] = m.route + "/" + t.id;
        });
      return acc;
    }, {});

    for (const [label, route] of Object.entries(navTargets)) {
      if (
        lower.includes(label) &&
        (lower.includes("go to") ||
          lower.includes("open") ||
          lower.includes("show") ||
          lower.includes("navigate"))
      ) {
        NovaRouter.navigate(route);
        return { text: `Navigating to ${label}.`, actions: [] };
      }
    }

    /* Context commands */
    if (lower.includes("set budget") || lower.includes("update budget")) {
      const match = text.match(/\$?([\d,]+)/);
      if (match) {
        const val = parseInt(match[1].replace(/,/g, ""), 10);
        NovaContext.setCampaign({ budget: val });
        return {
          text: `Budget updated to $${val.toLocaleString()}.`,
          actions: [],
        };
      }
    }

    if (
      lower.includes("new campaign") ||
      lower.includes("create campaign") ||
      lower.includes("start campaign")
    ) {
      return {
        text: "Let's create a new campaign. Head over to the Campaign Planner to get started.",
        actions: [{ label: "Open Campaign Planner", route: "plan/campaign" }],
      };
    }

    if (lower.includes("overview") || lower.includes("platform")) {
      return {
        text: `Nova Platform has 3 products: Plan (4 tools for campaign planning & budgets), Intelligence (4 tools for market & talent research), and Compliance (regulatory checks & ad audits). ${campaign.name ? `Your active campaign is "${campaign.name}".` : "No active campaign yet."} I can help you navigate anywhere -- just ask.`,
        actions: [],
      };
    }

    /* Cross-module data flow: "use in" commands */
    if (lower.includes("use in plan") || lower.includes("send to plan")) {
      const sharedData = NovaContext.shared;
      if (
        sharedData.lastCompetitiveData ||
        sharedData.lastMarketSnapshot ||
        sharedData.talentSignals
      ) {
        NovaContext.set("crossModulePayload", {
          competitive: sharedData.lastCompetitiveData,
          market: sharedData.lastMarketSnapshot,
          talent: sharedData.talentSignals,
          _from: activeModule,
          _ts: Date.now(),
        });
        return {
          text: `Intelligence data has been shared to your Campaign Planner. Open it to see the enriched context.`,
          actions: [{ label: "Open Campaign Planner", route: "plan/campaign" }],
        };
      }
      return {
        text: "No intelligence data to share yet. Run a competitive scan or market analysis first.",
        actions: [],
      };
    }

    /* Context-aware responses based on active module */
    const contextParts = [];
    if (campaign.name) contextParts.push(`Campaign: "${campaign.name}"`);
    if (campaign.budget > 0)
      contextParts.push(`Budget: $${campaign.budget.toLocaleString()}`);
    if (campaign.industry) contextParts.push(`Industry: ${campaign.industry}`);
    const contextStr = contextParts.length
      ? ` [${contextParts.join(" | ")}]`
      : "";

    /* Module-specific smart hints */
    const moduleHints = {
      campaign: `You're in ${modLabel}.${contextStr} I can help optimize your channel mix, set budgets, or generate a brief.`,
      budget: `You're in ${modLabel}.${contextStr} I can run simulations, calculate ROI, or show spending trends.`,
      social: `You're in ${modLabel}.${contextStr} I can suggest posting strategies or help create ad creatives.`,
      competitive: `You're in ${modLabel}.${contextStr} I can scan competitors, show trends, or generate reports.`,
      market: `You're in ${modLabel}.${contextStr} I can show labor market trends, salary data, or demand signals.`,
      talent: `You're in ${modLabel}.${contextStr} I can show talent heatmaps, salary benchmarks, or skill gaps.`,
      vendor: `You're in ${modLabel}.${contextStr} I can compare job boards, show pricing, or analyze ROI.`,
      comply: `You're in ${modLabel}.${contextStr} I can run compliance audits, check ad copy, or review regulations.`,
      testing: `You're in ${modLabel}.${contextStr} I can help set up A/B tests or analyze results.`,
    };

    const hint =
      moduleHints[activeModule] ||
      `I'm Nova, your AI assistant across all 3 products.${contextStr} Try asking about campaigns, market data, or compliance.`;

    return {
      text: `${hint} Full AI chat integration is coming soon -- use the standalone Nova page for complete conversations.`,
      actions: actions.length ? actions.slice(0, 2) : [],
    };
  }

  let _chatHistory = [];

  return {
    open() {
      _isOpen = true;
      document.getElementById("nova-drawer").classList.add("open");
      document.getElementById("nova-fab").classList.add("hidden");
      /* P1-7: Shrink content area to avoid clipping behind drawer */
      const shell = document.getElementById("platform-shell");
      if (shell && window.innerWidth > 1024) {
        shell.classList.add("nova-drawer-open");
      }
      if (window.innerWidth <= 1024) {
        document.getElementById("nova-overlay").classList.add("visible");
      }
      /* Load chat history */
      _chatHistory = _loadChatHistory();
      _renderMessages(_chatHistory);
      /* Update context badge (Phase 4) */
      const ctxBadge = document.getElementById("nova-drawer-context");
      if (ctxBadge) {
        const mod = MODULES.find((m) => m.id === NovaContext.nav.activeModule);
        ctxBadge.textContent = mod ? mod.label : "Dashboard";
      }
      document.getElementById("nova-input").focus();
    },
    close() {
      _isOpen = false;
      document.getElementById("nova-drawer").classList.remove("open");
      document.getElementById("nova-fab").classList.remove("hidden");
      document.getElementById("nova-overlay").classList.remove("visible");
      /* P1-7: Restore content area width */
      const shell = document.getElementById("platform-shell");
      if (shell) shell.classList.remove("nova-drawer-open");
    },
    toggle() {
      _isOpen ? this.close() : this.open();
    },
    handleKey(e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        this.send();
      }
    },
    send() {
      const input = document.getElementById("nova-input");
      const text = (input.value || "").trim();
      if (!text) return;
      input.value = "";
      _chatHistory.push({ role: "user", text });
      _renderMessages(_chatHistory);

      /* Show typing indicator */
      const container = document.getElementById("nova-messages");
      const typingEl = document.createElement("div");
      typingEl.className = "nova-msg";
      typingEl.innerHTML =
        '<div class="nova-msg-avatar nova">N</div><div class="nova-msg-body nova-reply" style="opacity:0.5;">Thinking...</div>';
      container.appendChild(typingEl);
      container.scrollTop = container.scrollHeight;

      /* Call real Nova API */
      const activeModule = NovaContext.nav.activeModule;
      const campaign = NovaContext.campaign;
      const csrfMeta = document.querySelector('meta[name="csrf-token"]');
      const headers = { "Content-Type": "application/json" };
      if (csrfMeta && csrfMeta.content)
        headers["X-CSRF-Token"] = csrfMeta.content;

      fetch("/api/chat", {
        method: "POST",
        headers: headers,
        credentials: "same-origin",
        body: JSON.stringify({
          message: text,
          conversation_history: _chatHistory
            .filter((m) => m.role === "user" || m.role === "nova")
            .slice(-10)
            .map((m) => ({
              role: m.role === "nova" ? "assistant" : "user",
              content: m.text,
            })),
          context: {
            active_module: activeModule,
            campaign_name: campaign.name || "",
            campaign_budget: campaign.budget || 0,
            campaign_industry: campaign.industry || "",
          },
        }),
      })
        .then((r) => {
          if (!r.ok && r.status === 403) {
            /* CSRF token expired -- re-fetch and retry once */
            return fetch("/api/csrf-token", {
              credentials: "same-origin",
            })
              .then((cr) => cr.json())
              .then((cd) => {
                const meta = document.querySelector('meta[name="csrf-token"]');
                if (meta && (cd.token || cd.csrf_token))
                  meta.content = cd.token || cd.csrf_token;
                headers["X-CSRF-Token"] = cd.token || cd.csrf_token || "";
                return fetch("/api/chat", {
                  method: "POST",
                  headers: headers,
                  credentials: "same-origin",
                  body: JSON.stringify({
                    message: text,
                    conversation_history: _chatHistory
                      .filter((m) => m.role === "user" || m.role === "nova")
                      .slice(-10)
                      .map((m) => ({
                        role: m.role === "nova" ? "assistant" : "user",
                        content: m.text,
                      })),
                    context: {
                      active_module: activeModule,
                      campaign_name: campaign.name || "",
                      campaign_budget: campaign.budget || 0,
                      campaign_industry: campaign.industry || "",
                    },
                  }),
                });
              })
              .then((r2) => r2.json());
          }
          return r.json();
        })
        .then((data) => {
          /* Remove typing indicator */
          if (typingEl.parentNode) typingEl.remove();

          const responseText =
            data.response ||
            data.text ||
            data.error ||
            "I couldn't process that request. Please try again.";
          const actions = [];

          /* Extract action buttons from response if any */
          const activeActions = MODULE_ACTIONS[activeModule] || [];
          if (activeActions.length) {
            actions.push(...activeActions.slice(0, 2));
          }

          _chatHistory.push({
            role: "nova",
            text: responseText,
            actions: actions,
          });
          _saveChatHistory(_chatHistory);
          _renderMessages(_chatHistory);

          /* Update suggestions based on response */
          updateSuggestions(activeModule || "home");
        })
        .catch((err) => {
          /* Remove typing indicator */
          if (typingEl.parentNode) typingEl.remove();

          /* Fallback to client-side if API fails */
          const response = _generateResponse(text);
          _chatHistory.push({
            role: "nova",
            text: response.text + " (offline mode)",
            actions: response.actions,
          });
          _saveChatHistory(_chatHistory);
          _renderMessages(_chatHistory);
        });
    },
    sendSuggestion(text) {
      document.getElementById("nova-input").value = text;
      this.send();
    },
    updateSuggestions,
    /* Execute actions from Nova (Phase 4) */
    executeAction(type, payload) {
      if (type === "navigate") NovaRouter.navigate(payload);
      else if (type === "setContext") NovaContext.setCampaign(payload);
    },
  };
})();

/* ─────────────────────────────────────────────────────────────
   COMMAND PALETTE
   ───────────────────────────────────────────────────────────── */
const NovaCmdPalette = (() => {
  let _selectedIdx = 0;
  let _items = [];

  function _buildSearchItems() {
    const items = [];
    MODULES.forEach((mod) => {
      items.push(mod);
      if (mod.tabs && mod.tabs.length > 1) {
        mod.tabs.forEach((tab) => {
          items.push({
            id: mod.id + "/" + tab.id,
            route: mod.route + "/" + tab.id,
            label: tab.label,
            desc: mod.label + " > " + tab.label,
            group: mod.group,
            icon: mod.icon,
            _isTab: true,
          });
        });
      }
    });
    return items;
  }

  const _allItems = _buildSearchItems();

  function _render(filtered) {
    _items = filtered;
    _selectedIdx = 0;
    const container = document.getElementById("cmd-palette-results");
    if (!filtered.length) {
      container.innerHTML = `<div style="padding:24px;text-align:center;color:var(--text-tertiary);font-size:13px;">No results found</div>`;
      return;
    }
    container.innerHTML = filtered
      .map(
        (item, i) => `
            <div class="cmd-palette-item${i === 0 ? " selected" : ""}" data-idx="${i}" onclick="NovaCmdPalette.select(${i})" onmouseenter="NovaCmdPalette.hover(${i})">
              <div class="cmd-palette-item-icon group-${item.group || "command"}">${ICONS[item.icon] || ""}</div>
              <div class="cmd-palette-item-text">
                <div class="cmd-palette-item-title">${item.label}</div>
                <div class="cmd-palette-item-desc">${item.desc}</div>
              </div>
            </div>`,
      )
      .join("");
  }

  return {
    open() {
      document.getElementById("cmd-palette").classList.add("open");
      const input = document.getElementById("cmd-palette-input");
      input.value = "";
      input.focus();
      _render(MODULES.filter((m) => m.group));
    },
    close() {
      document.getElementById("cmd-palette").classList.remove("open");
    },
    filter(query) {
      const q = query.toLowerCase().trim();
      if (!q) {
        _render(MODULES.filter((m) => m.group));
        return;
      }
      const filtered = _allItems.filter(
        (m) =>
          m.label.toLowerCase().includes(q) ||
          m.desc.toLowerCase().includes(q) ||
          (m.group || "").toLowerCase().includes(q),
      );
      _render(filtered);
    },
    select(idx) {
      const item = _items[idx];
      if (item) {
        this.close();
        NovaRouter.navigate(item.route);
      }
    },
    hover(idx) {
      _selectedIdx = idx;
      document.querySelectorAll(".cmd-palette-item").forEach((el, i) => {
        el.classList.toggle("selected", i === idx);
      });
    },
    handleKey(e) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        _selectedIdx = Math.min(_selectedIdx + 1, _items.length - 1);
        this.hover(_selectedIdx);
        const el = document.querySelectorAll(".cmd-palette-item")[_selectedIdx];
        if (el) el.scrollIntoView({ block: "nearest" });
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        _selectedIdx = Math.max(_selectedIdx - 1, 0);
        this.hover(_selectedIdx);
        const el = document.querySelectorAll(".cmd-palette-item")[_selectedIdx];
        if (el) el.scrollIntoView({ block: "nearest" });
      } else if (e.key === "Enter") {
        e.preventDefault();
        this.select(_selectedIdx);
      } else if (e.key === "Escape") {
        this.close();
      }
    },
  };
})();

/* Campaign picker (Phase 3: enhanced modal) */
const NovaCampaignPicker = {
  open() {
    const name = prompt("Enter campaign name (or leave empty to clear):");
    if (name !== null) {
      if (name.trim()) {
        NovaContext.setCampaign({ name: name.trim() });
      } else {
        NovaContext.clearCampaign();
      }
    }
  },
};

/* ─────────────────────────────────────────────────────────────
   ONBOARDING (Phase 6)
   ───────────────────────────────────────────────────────────── */
const NovaOnboarding = (() => {
  const SEEN_KEY = "nova_onboarded_v1";
  let _tourStep = 0;

  const TOUR_STEPS = [
    {
      title: "Sidebar Navigation",
      desc: "Two super-modules: Command Center for building campaigns and Intelligence Hub for market insights. Click any module to get started.",
      target: () => document.getElementById("sidebar"),
      position: "right",
    },
    {
      title: "Command Palette",
      desc: "Press Cmd+K anytime to quickly search and jump to any module or tab. It works from anywhere in the platform.",
      target: () => document.querySelector(".topbar-search"),
      position: "bottom",
    },
    {
      title: "Nova AI Assistant",
      desc: "Press Cmd+/ to open Nova AI. Nova is context-aware and provides suggestions based on your current module.",
      target: () => document.getElementById("nova-fab"),
      position: "left",
    },
  ];

  function _positionTooltip(step) {
    const tooltip = document.getElementById("tour-tooltip");
    const targetEl = step.target();
    if (!targetEl) return;
    const rect = targetEl.getBoundingClientRect();

    tooltip.style.top = "";
    tooltip.style.left = "";
    tooltip.style.right = "";
    tooltip.style.bottom = "";

    if (step.position === "right") {
      tooltip.style.top = rect.top + rect.height / 2 - 40 + "px";
      tooltip.style.left = rect.right + 12 + "px";
    } else if (step.position === "bottom") {
      tooltip.style.top = rect.bottom + 12 + "px";
      tooltip.style.left = rect.left + rect.width / 2 - 140 + "px";
    } else if (step.position === "left") {
      tooltip.style.top = rect.top - 40 + "px";
      tooltip.style.right = window.innerWidth - rect.left + 12 + "px";
    }
  }

  return {
    check() {
      try {
        if (localStorage.getItem(SEEN_KEY)) return;
      } catch (e) {
        return;
      }
      // Skip onboarding when user navigated directly to a module via URL path or hash
      const currentPath = (location.pathname || "").replace(
        /^\/platform\/?/,
        "",
      );
      const hash = (location.hash || "").replace(/^#?\/?/, "");
      if (
        (currentPath && currentPath !== "home") ||
        (hash && hash !== "home")
      ) {
        try {
          localStorage.setItem(SEEN_KEY, "1");
        } catch (e) {
          /* noop */
        }
        return;
      }
      document.getElementById("onboarding-overlay").classList.add("active");
    },
    dismiss() {
      document.getElementById("onboarding-overlay").classList.remove("active");
      try {
        localStorage.setItem(SEEN_KEY, "1");
      } catch (e) {
        /* noop */
      }
    },
    startTour() {
      _tourStep = 0;
      this._showStep();
    },
    _showStep() {
      if (_tourStep >= TOUR_STEPS.length) {
        document.getElementById("tour-tooltip").classList.remove("active");
        return;
      }
      const step = TOUR_STEPS[_tourStep];
      document.getElementById("tour-title").textContent = step.title;
      document.getElementById("tour-desc").textContent = step.desc;
      document.getElementById("tour-step").textContent =
        `${_tourStep + 1} of ${TOUR_STEPS.length}`;
      document.getElementById("tour-next").textContent =
        _tourStep === TOUR_STEPS.length - 1 ? "Done" : "Next";
      document.getElementById("tour-tooltip").classList.add("active");
      _positionTooltip(step);
    },
    nextTourStep() {
      _tourStep++;
      this._showStep();
    },
  };
})();

/* ─────────────────────────────────────────────────────────────
   ROLE-BASED VIEWS (Phase 6)
   ───────────────────────────────────────────────────────────── */
const NovaRoleSelector = (() => {
  const ROLES = [
    { id: "admin", label: "CHO @ Joveo", filter: null },
    {
      id: "strategist",
      label: "Strategist View",
      filter: ["campaign", "social", "budget", "competitive"],
    },
    {
      id: "analyst",
      label: "Analyst View",
      filter: ["competitive", "market", "vendor", "talent", "budget"],
    },
    {
      id: "manager",
      label: "Manager View",
      filter: ["campaign", "budget", "comply", "talent"],
    },
  ];
  let _currentIdx = 0;

  function _applyRole(role) {
    const el = document.getElementById("role-selector");
    if (el) el.textContent = role.label;
    NovaContext.user.role = role.id;

    /* Show/hide sidebar items based on role filter */
    const items = document.querySelectorAll(".sidebar-item[data-module-id]");
    items.forEach((item) => {
      const modId = item.getAttribute("data-module-id");
      if (!role.filter) {
        item.style.display = "";
      } else {
        item.style.display = role.filter.includes(modId) ? "" : "none";
      }
    });
  }

  return {
    cycle() {
      _currentIdx = (_currentIdx + 1) % ROLES.length;
      _applyRole(ROLES[_currentIdx]);
    },
    init() {
      try {
        const saved = localStorage.getItem("nova_role_v1");
        if (saved) {
          const idx = ROLES.findIndex((r) => r.id === saved);
          if (idx >= 0) {
            _currentIdx = idx;
            _applyRole(ROLES[idx]);
          }
        }
      } catch (e) {
        /* noop */
      }
    },
  };
})();

/* ─────────────────────────────────────────────────────────────
   KEYBOARD SHORTCUTS (Phase 6: expanded)
   ───────────────────────────────────────────────────────────── */
document.addEventListener("keydown", (e) => {
  const isMac = navigator.platform.toUpperCase().indexOf("MAC") >= 0;
  const mod = isMac ? e.metaKey : e.ctrlKey;
  const isInInput =
    ["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName) ||
    document.activeElement.isContentEditable;

  /* Cmd+K: command palette */
  if (mod && e.key === "k") {
    e.preventDefault();
    const paletteOpen = document
      .getElementById("cmd-palette")
      .classList.contains("open");
    paletteOpen ? NovaCmdPalette.close() : NovaCmdPalette.open();
    return;
  }

  /* Cmd+/: Nova AI */
  if (mod && e.key === "/") {
    e.preventDefault();
    NovaDrawer.toggle();
    return;
  }

  /* Cmd+1: Plan */
  if (mod && e.key === "1" && !e.shiftKey) {
    e.preventDefault();
    NovaRouter.navigate("plan/campaign");
    return;
  }

  /* Cmd+2: Intelligence */
  if (mod && e.key === "2" && !e.shiftKey) {
    e.preventDefault();
    NovaRouter.navigate("intelligence/competitive");
    return;
  }

  /* Cmd+3: Compliance */
  if (mod && e.key === "3" && !e.shiftKey) {
    e.preventDefault();
    NovaRouter.navigate("compliance/comply");
    return;
  }

  /* Cmd+4: Home */
  if (mod && e.key === "4" && !e.shiftKey) {
    e.preventDefault();
    NovaRouter.navigate("");
    return;
  }

  /* Cmd+Shift+F: Toggle favorites view */
  if (mod && e.shiftKey && e.key === "F") {
    e.preventDefault();
    const fav = document.getElementById("sidebar-favorites");
    if (fav) fav.classList.toggle("has-items");
    return;
  }

  /* Cmd+Shift+N: New Campaign */
  if (mod && e.shiftKey && e.key === "N") {
    e.preventDefault();
    NovaCampaignPicker.open();
    return;
  }

  /* Escape: close overlays */
  if (e.key === "Escape") {
    /* Close insights panel if open */
    const insPanel = document.getElementById("insights-panel");
    if (insPanel && insPanel.style.display !== "none") {
      NovaInsights.toggle();
      return;
    }
    if (document.getElementById("tour-tooltip").classList.contains("active")) {
      document.getElementById("tour-tooltip").classList.remove("active");
      return;
    }
    if (document.getElementById("cmd-palette").classList.contains("open")) {
      NovaCmdPalette.close();
      return;
    }
    if (document.getElementById("nova-drawer").classList.contains("open")) {
      NovaDrawer.close();
      return;
    }
    if (
      document.getElementById("onboarding-overlay").classList.contains("active")
    ) {
      NovaOnboarding.dismiss();
      return;
    }
  }

  /* Forward keys to command palette if open */
  if (document.getElementById("cmd-palette").classList.contains("open")) {
    if (["ArrowDown", "ArrowUp", "Enter"].includes(e.key)) {
      NovaCmdPalette.handleKey(e);
    }
  }

  /* [ key: toggle sidebar (not in input) */
  if (e.key === "[" && !mod && !isInInput) {
    NovaSidebar.toggle();
  }
});

/* ─────────────────────────────────────────────────────────────
   NOVA INSIGHTS (proactive intelligence bell + panel)
   ───────────────────────────────────────────────────────────── */
const NovaInsights = (() => {
  let _open = false;
  let _pollTimer = null;
  let _insights = [];

  function toggle() {
    const panel = document.getElementById("insights-panel");
    if (!panel) return;
    _open = !_open;
    panel.style.display = _open ? "block" : "none";
    if (_open) poll(); // refresh on open
  }

  function poll() {
    fetch("/api/insights?unread=1")
      .then((r) => r.json())
      .then((d) => {
        _insights = d.insights || [];
        updateBadge(_insights.length);
        render(_insights);
      })
      .catch(() => {});
  }

  function updateBadge(count) {
    const badge = document.getElementById("insight-count");
    if (!badge) return;
    if (count > 0) {
      badge.textContent = count > 99 ? "99+" : count;
      badge.style.display = "flex";
    } else {
      badge.style.display = "none";
    }
  }

  function render(insights) {
    const list = document.getElementById("insights-list");
    if (!list) return;
    if (!insights.length) {
      list.innerHTML = '<div class="insights-empty">No new insights</div>';
      return;
    }
    list.innerHTML = insights
      .map((ins) => {
        const sev = (ins.severity || ins.priority || "low").toLowerCase();
        const sevClass =
          sev === "high"
            ? "severity-high"
            : sev === "medium"
              ? "severity-medium"
              : "severity-low";
        const title = escapeHtml(ins.title || ins.type || "Insight");
        const msg = escapeHtml(ins.message || ins.description || "");
        const id = ins.id || ins.insight_id || "";
        const actionRoute = ins.action_route || ins.route || "";
        const timeAgo = ins.created_at ? formatTimeAgo(ins.created_at) : "";
        let actions =
          '<button class="insight-dismiss-btn" onclick="NovaInsights.dismiss(\'' +
          id +
          "')\">Dismiss</button>";
        if (actionRoute) {
          actions =
            '<button class="insight-action-btn" onclick="NovaInsights.act(\'' +
            id +
            "', '" +
            actionRoute +
            "')\">View</button>" +
            actions;
        }
        return (
          '<div class="insight-card ' +
          sevClass +
          '">' +
          '<div class="insight-card-title">' +
          title +
          "</div>" +
          '<div class="insight-card-msg">' +
          msg +
          "</div>" +
          '<div class="insight-card-actions">' +
          actions +
          (timeAgo
            ? '<span class="insight-card-time">' + timeAgo + "</span>"
            : "") +
          "</div></div>"
        );
      })
      .join("");
  }

  function dismiss(id) {
    const csrfMeta = document.querySelector('meta[name="csrf-token"]');
    const csrf = csrfMeta ? csrfMeta.content : "";
    fetch("/api/insights/dismiss", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrf,
      },
      body: JSON.stringify({ id: id }),
    })
      .then(() => poll())
      .catch(() => {});
  }

  function act(id, route) {
    // Mark as read then navigate
    const csrfMeta = document.querySelector('meta[name="csrf-token"]');
    const csrf = csrfMeta ? csrfMeta.content : "";
    fetch("/api/insights/read", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": csrf,
      },
      body: JSON.stringify({ id: id }),
    })
      .then(() => {
        poll();
        if (route && typeof NovaRouter !== "undefined") {
          NovaRouter.navigate(route);
        }
        toggle(); // close panel
      })
      .catch(() => {});
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function formatTimeAgo(ts) {
    try {
      const diff = Date.now() - new Date(ts).getTime();
      const mins = Math.floor(diff / 60000);
      if (mins < 1) return "just now";
      if (mins < 60) return mins + "m ago";
      const hrs = Math.floor(mins / 60);
      if (hrs < 24) return hrs + "h ago";
      const days = Math.floor(hrs / 24);
      return days + "d ago";
    } catch (_) {
      return "";
    }
  }

  function start() {
    poll();
    _pollTimer = setInterval(poll, 30000);
  }

  function stop() {
    if (_pollTimer) clearInterval(_pollTimer);
  }

  return { toggle, poll, dismiss, act, start, stop };
})();

/* ─────────────────────────────────────────────────────────────
   INIT
   ───────────────────────────────────────────────────────────── */
document.addEventListener("DOMContentLoaded", () => {
  NovaContext.init();
  NovaFavorites.init();
  NovaSidebar.build();
  NovaRoleSelector.init();
  NovaDrawer.updateSuggestions("home");
  NovaRouter.init();
  /* Start proactive insights polling */
  NovaInsights.start();
  /* Safety net: rebuild sidebar if empty after 500ms (P1-6 fix) */
  setTimeout(() => {
    const nav = document.getElementById("sidebar-nav");
    if (nav && !nav.children.length) {
      NovaSidebar.build();
    }
  }, 500);
  /* Enable grid transitions after layout stabilizes (prevents CLS on load) */
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      const shell = document.getElementById("platform-shell");
      if (shell) shell.classList.add("transitions-ready");
    });
  });
  /* Show onboarding on first visit (Phase 6) */
  setTimeout(() => NovaOnboarding.check(), 500);
  /* Fetch CSRF token for API calls */
  fetch("/api/csrf-token", { credentials: "same-origin" })
    .then((r) => r.json())
    .then((d) => {
      const meta = document.querySelector('meta[name="csrf-token"]');
      if (meta && (d.token || d.csrf_token))
        meta.content = d.token || d.csrf_token;
    })
    .catch(() => {});
});
