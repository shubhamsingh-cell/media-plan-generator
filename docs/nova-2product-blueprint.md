# Nova AI Suite -- 2-Product Consolidation Blueprint

**Version:** 2.0
**Date:** 2026-03-24
**Author:** Shubham Singh Chandel
**Status:** Design (no code yet)
**Supersedes:** platform-consolidation-blueprint.md (6-toolkit / 17-tool model)

---

## Executive Summary

Consolidate the entire Nova AI Suite from 24 separate product pages into exactly
2 products:

| # | Product | What it is |
|---|---------|------------|
| 1 | **Nova Platform** | Single-page dashboard/workspace. All 24 products become feature modules inside one persistent shell. Left sidebar navigation, shared campaign context, unified data layer. |
| 2 | **Nova AI** | Persistent chatbot overlay/sidebar available on every page. Can do everything the platform does via natural language. Context-aware, cross-module, action-capable. |

**Before:** 24 standalone HTML pages, each with its own `<head>`, CSS, JS, layout.
Users click "Back to Hub" to switch tools. No shared state. No cross-tool workflow.

**After:** One shell page. Modules load inside a content frame. Sidebar persists.
Campaign context flows everywhere. Nova AI can orchestrate any module.

**Design inspirations:**
- Semrush May 2025 Toolkit redesign (objective-centric grouping, unified sidebar)
- HubSpot hub model (shared CRM context across Marketing/Sales/Service hubs)
- Linear (single-page app, instant sidebar navigation, keyboard-first)
- Vercel dashboard (clean shell, project-scoped context, minimal chrome)
- Notion (sidebar + page model, slash commands, blocks as composable units)

---

## 1. Architecture Decision: How to Build an SPA on stdlib

### Constraints
- Python stdlib HTTP server (no Flask/Django)
- No React/Vue/Svelte build step
- 29 existing HTML templates with inline CSS/JS (each 500-3000 lines)
- Must work without Node.js toolchain on the deploy target

### Options Evaluated

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **A. Vanilla JS shell + fetch() for HTML fragments** | Full control, no dependencies, works with stdlib server, gradual migration | Must build router/state from scratch | **SELECTED** |
| B. iframe-based module loading | Zero template changes needed, instant win | No shared CSS, no shared state, feels janky, accessibility nightmare | Rejected |
| C. HTMX (library-based partial replacement) | Elegant partial swaps, hx-boost for links | Adds 14KB dependency, requires server to detect HX-Request header, every template needs hx- attributes | Rejected (good Phase 2 upgrade) |
| D. Full client-side framework (Lit, Alpine) | Reactive binding, component model | Adds build complexity, fights stdlib server model | Rejected |

### Selected: Approach A -- Vanilla JS Shell + fetch()

```
                    ARCHITECTURE OVERVIEW
 ================================================================

  Browser loads ONE page:  /platform  (platform-shell.html)

  +----------------------------------------------------------+
  |  platform-shell.html                                      |
  |  - <head> with shared CSS, fonts, design tokens           |
  |  - Sidebar nav (always visible)                           |
  |  - Top context bar (campaign/client)                      |
  |  - <div id="module-frame"> (content swap target)          |
  |  - Nova AI drawer (right side)                            |
  |  - Router JS (hash-based)                                 |
  |  - Shared context JS (NovaContext)                        |
  +----------------------------------------------------------+

  When user clicks sidebar item:
  1. Router updates hash:  #/plan/media
  2. fetch('/fragment/media-plan') -> server returns HTML fragment
  3. document.getElementById('module-frame').innerHTML = fragment
  4. Module's inline <script> executes via eval or reinsertion
  5. Module reads shared context from NovaContext

  Server-side:
  - New route: /fragment/<module-name>  returns ONLY the <body>
    inner content (no <html>, <head>, <style> duplication)
  - Existing routes (/media-plan, /tracker, etc.) still work
    for backward compatibility during migration
```

### Why Hash-Based Routing (not History API)

| Factor | Hash (#/plan/media) | History API (/plan/media) |
|--------|---------------------|---------------------------|
| Server config | Zero -- server only sees /platform | Requires catch-all route for every path |
| stdlib server compat | Perfect -- one route serves the shell | Complex -- must detect SPA routes vs API routes |
| Deep linking | Works -- hash is client-side only | Works but needs server fallback |
| Back/Forward | Native hashchange event | Requires popstate + manual state |
| SEO | Not needed (authenticated app) | Better for public pages |

**Decision:** Hash routing for the platform. Keep path-based routes alive for
backward compatibility and direct-link sharing during migration.

### Fragment Endpoint Design

The server gains ONE new route pattern:

```
GET /fragment/<module-name>

Returns: HTML fragment (the <body> inner content only, no <html>/<head>)
Content-Type: text/html; charset=utf-8
```

Each existing template is split into:
1. **Shared CSS** -- extracted to platform-shell.html (design tokens, reset, typography)
2. **Module CSS** -- stays inside fragment as <style scoped> or prefixed classes
3. **Module HTML** -- the actual UI (forms, tables, charts)
4. **Module JS** -- inline <script> at the bottom of the fragment

The server checks: if the request is for /fragment/media-plan, read
templates/media-plan.html, strip <html>/<head>/<body> tags, return the inner
content. If the request is for /media-plan directly (legacy), serve the full
standalone page as before.

---

## 2. Module Organization: 24 Products -> 10 Modules in 5 Groups

### Merge Strategy

Some products are thin wrappers or subsets of others. The merge rules:
- If product B is a simplified mode of product A, absorb B as a tab/mode in A
- If products share >70% of the same API calls, merge them
- If products serve the same user intent at different scales, unify with a scope toggle

### Final Module Map

```
GROUP              MODULE                    ABSORBS / MERGES
-----              ------                    -----------------
PLAN               Campaign Planner          Media Plan + Quick Plan + Quick Brief
                   Social & Creative         Social Plan + Creative AI
                   Testing Lab               A/B Testing

INTELLIGENCE       Competitive Intel         Competitive + Market Intel Reports
                   Market Pulse              Market Pulse (standalone)
                   Vendor IQ                 Vendor IQ (standalone)

OPTIMIZE           Budget & Performance      Simulator + ROI Calculator + Tracker
                                              + Post-Campaign Analysis

TALENT             Talent Intelligence       Hire Signal + Talent Heatmap
                                              + PayScale Sync + SkillTarget
                                              + ApplyFlow

COMPLY             Compliance Center         Compliance Guard + Audit
```

### Final Count: 10 modules in 5 groups

| # | Group | Module | Source Products | Tabs/Modes Inside |
|---|-------|--------|-----------------|-------------------|
| 1 | Plan | Campaign Planner | media-plan, quick-plan, quick-brief | Full Plan / Quick Plan / Brief |
| 2 | Plan | Social & Creative | social-plan, creative-ai | Social Strategy / Creative Assets |
| 3 | Plan | Testing Lab | ab-testing | (standalone) |
| 4 | Intel | Competitive Intel | competitive, market-intel | Live Monitor / Reports |
| 5 | Intel | Market Pulse | market-pulse | (standalone) |
| 6 | Intel | Vendor IQ | vendor-iq | (standalone) |
| 7 | Optimize | Budget & Performance | simulator, roi-calculator, tracker, post-campaign | Simulator / ROI / Tracker / Post-Campaign |
| 8 | Talent | Talent Intelligence | hire-signal, talent-heatmap, payscale-sync, skill-target, applyflow-demo | Signals / Heat Map / PayScale / SkillTarget / ApplyFlow |
| 9 | Comply | Compliance Center | compliance-guard, audit | Guard / Ad Audit |
| 10 | -- | API Portal | api-portal | (bottom of sidebar, utility) |

### Products Removed from Module List (become platform-level features)
- **/hub** -- Becomes the Dashboard home (not a module, it IS the landing state)
- **/nova** -- Becomes Nova AI overlay (Product 2, not a sidebar module)
- **/dashboard** -- Merges into the Dashboard home
- **/observability** -- Becomes Settings > System Health (admin only)
- **/pricing, /privacy, /terms** -- Static pages, served outside the shell

---

## 3. Sidebar Navigation Hierarchy

```
+-------------------------------------------+
|  [N] NOVA PLATFORM          [?] [S] [A]  |  <- Top bar: logo, help, settings, avatar
+-------------------------------------------+
|                                           |
|  SEARCH MODULES...          (Cmd+K)      |  <- Global search / command palette
|                                           |
|  ---- PLAN ----                           |
|  [icon] Campaign Planner          >       |
|  [icon] Social & Creative         >       |
|  [icon] Testing Lab                       |
|                                           |
|  ---- INTELLIGENCE ----                   |
|  [icon] Competitive Intel         >       |
|  [icon] Market Pulse                      |
|  [icon] Vendor IQ                         |
|                                           |
|  ---- OPTIMIZE ----                       |
|  [icon] Budget & Performance      >       |
|                                           |
|  ---- TALENT ----                         |
|  [icon] Talent Intelligence       >       |
|                                           |
|  ---- COMPLIANCE ----                     |
|  [icon] Compliance Center         >       |
|                                           |
|  ---                                      |
|  [icon] API Portal                        |
|  [icon] Settings                          |
|  [icon] System Health  (admin only)       |
|                                           |
|  ---                                      |
|  [avatar] Shubham Singh Chandel           |
|  CHO @ Joveo                              |
+-------------------------------------------+
```

### Sidebar Behavior

- **Collapsed state:** Icons only, 64px wide. Tooltip on hover shows label.
- **Expanded state:** Icons + labels + group headers, 260px wide.
- **Toggle:** Click the hamburger / press `[` to collapse/expand.
- **Active indicator:** Left accent border (4px, BLUE_VIOLET #5A54BD).
- **Group sections:** Collapsible with a small caret. All expanded by default.
  Collapsed groups show a dot-count badge if any module has notifications.
- **">" indicator:** Module has sub-tabs. Clicking opens directly to last-used tab.
  Long-press or right-click shows sub-tab flyout menu.
- **Keyboard:** Arrow keys navigate. Enter opens. Tab moves focus predictably.

### Command Palette (Cmd+K / Ctrl+K)

Inspired by Linear/Vercel. A search overlay that:
1. Searches module names ("campaign", "roi")
2. Searches recent items ("Acme Q3 plan")
3. Exposes actions ("Create new plan", "Run audit", "Export tracker data")
4. Launches Nova AI with a pre-filled query if no module matches

---

## 4. Platform Shell Layout (ASCII Wireframe)

```
+------------------------------------------------------------------+
| [=] NOVA PLATFORM        Campaign: Acme Corp Q3 2026    [N] [?]  |
+--------+---------------------------------------------------------+
|        |                                                          |
| SIDE   |   MODULE CONTENT FRAME                                   |
| BAR    |                                                          |
|        |   +--------------------------------------------------+   |
| 260px  |   | Tab Bar:  [Full Plan] [Quick Plan] [Brief]       |   |
| or     |   +--------------------------------------------------+   |
| 64px   |   |                                                  |   |
|        |   |   (Module HTML loads here via fetch)              |   |
|        |   |                                                  |   |
|        |   |   The Campaign Planner module content             |   |
|        |   |   renders in this frame. It has access to         |   |
|        |   |   NovaContext.campaign for shared state.          |   |
|        |   |                                                  |   |
|        |   |                                                  |   |
|        |   |                                                  |   |
|        |   |                                                  |   |
|        |   +--------------------------------------------------+   |
|        |                                                          |
+--------+----------------------------------------------+-----------+
                                                        | NOVA AI   |
                                                        | DRAWER    |
                                                        | (384px)   |
                                                        |           |
                                                        | [context] |
                                                        | [chat]    |
                                                        | [input]   |
                                                        +-----------+
```

### Responsive Breakpoints

| Breakpoint | Sidebar | Nova AI | Content |
|------------|---------|---------|---------|
| Desktop (>1280px) | Expanded (260px) | Drawer (384px) or hidden | Remaining width |
| Laptop (1024-1280px) | Collapsed (64px) | Drawer overlays content | Full width |
| Tablet (768-1024px) | Off-canvas (swipe) | Full-screen overlay | Full width |
| Mobile (<768px) | Bottom tab bar (5 groups) | Full-screen overlay | Full width |

---

## 5. Shared Context Layer (NovaContext)

### What Context Is Shared

```javascript
// NovaContext -- global singleton, lives in platform-shell.html

const NovaContext = {
  // ── Campaign Context (the "project" that ties everything together) ──
  campaign: {
    id: null,                    // UUID, persisted to Supabase
    name: '',                    // "Acme Corp Q3 2026 Engineering Hiring"
    client: '',                  // "Acme Corp"
    budget: 0,                   // Total budget in USD
    currency: 'USD',
    startDate: null,             // ISO date
    endDate: null,
    targetRoles: [],             // ["Software Engineer", "DevOps"]
    targetLocations: [],         // ["San Francisco, CA", "Austin, TX"]
    channels: [],                // ["LinkedIn", "Indeed", "Google Ads"]
    industry: '',                // "Technology"
    notes: '',
  },

  // ── User/Session Context ──
  user: {
    name: '',
    email: '',
    role: '',                    // "admin" | "planner" | "viewer"
    org: '',
  },

  // ── Navigation Context ──
  nav: {
    activeGroup: '',             // "plan"
    activeModule: '',            // "campaign-planner"
    activeTab: '',               // "full-plan"
    previousModule: '',          // For "back" behavior
    history: [],                 // Stack of visited modules
  },

  // ── Cross-Module Data Bus ──
  shared: {
    lastPlanOutput: null,        // Output from Campaign Planner -> ROI can read
    lastAuditResult: null,       // Output from Audit -> Compliance can read
    lastCompetitiveData: null,   // Output from Competitive Intel
    lastMarketSnapshot: null,    // Output from Market Pulse
    selectedVendors: [],         // VendorIQ selections -> feed into Planner
    talentSignals: null,         // HireSignal data -> feed into Talent module
  },

  // ── Methods ──
  setCampaign(fields) { ... },   // Merge fields, persist, emit event
  getCampaign() { ... },
  clearCampaign() { ... },
  subscribe(event, callback) { ... },  // PubSub for cross-module reactivity
  emit(event, data) { ... },
  persistToLocal() { ... },      // Save to localStorage
  persistToSupabase() { ... },   // Save to Supabase (debounced)
  loadFromLocal() { ... },
  loadFromSupabase(campaignId) { ... },
};
```

### Persistence Strategy

```
                  PERSISTENCE LAYERS
 ====================================================

  Layer 1: In-Memory (NovaContext object)
           - Instant access, no latency
           - Lost on page refresh

  Layer 2: localStorage
           - Persists across refreshes
           - Sync'd from in-memory on every change (debounced 500ms)
           - Key: nova_context_v1
           - Max size: ~5MB (sufficient)

  Layer 3: Supabase (table: campaign_contexts)
           - Persists across devices/sessions
           - Sync'd from in-memory on save action or auto-save (debounced 5s)
           - Loaded on login / campaign switch
           - Columns: id, user_email, campaign_data (JSONB), updated_at

  Load order on platform init:
  1. Check localStorage for nova_context_v1
  2. If found and fresh (<24h), hydrate NovaContext from it
  3. If stale or missing, fetch from Supabase
  4. If nothing in Supabase, show "Create Campaign" wizard
```

### PubSub Event System

Modules communicate via events, not direct references:

```
EVENT NAME                  EMITTER              LISTENERS
----------                  -------              ---------
campaign:updated            Any module           All modules (re-read context bar)
plan:generated              Campaign Planner     ROI Calculator, Tracker, Nova AI
audit:completed             Compliance Center    Campaign Planner (warnings badge)
competitive:updated         Competitive Intel    Campaign Planner, Nova AI
vendor:selected             Vendor IQ            Campaign Planner (vendor list)
talent:signals              Talent Intelligence  Campaign Planner, Market Pulse
budget:alert                Budget & Perf        All modules (top bar warning)
module:loaded               Router               Nova AI (knows active module)
module:unloaded             Router               Previous module (cleanup)
```

---

## 6. Nova AI Integration (Product 2)

### Architecture

Nova AI is NOT a module inside the platform. It is a persistent overlay that
exists at the shell level, outside the module content frame.

```
  NOVA AI COMPONENT TREE (inside platform-shell.html)
  ====================================================

  <div id="nova-ai-drawer">          // Right-side drawer, 384px
    <div id="nova-ai-header">        // "Nova AI" title + close + context badge
      <span class="context-badge">   // Shows: "Viewing: Campaign Planner"
    </div>
    <div id="nova-ai-messages">      // Chat history (scrollable)
    </div>
    <div id="nova-ai-suggestions">   // Context-aware quick actions
      // Example: "Generate media plan for Acme Corp"
      // Example: "What's the ROI if I increase budget by 20%?"
    </div>
    <div id="nova-ai-input">         // Input bar + send button + voice
      <textarea id="nova-prompt">
      <button id="nova-send">
      <button id="nova-voice">       // ElevenLabs STT
    </div>
  </div>

  <button id="nova-ai-fab">          // Floating action button when drawer closed
    // Bottom-right, always visible
    // Keyboard shortcut: Cmd+J / Ctrl+J
  </button>
```

### Context Awareness

Nova AI automatically knows:

1. **Active module** -- via `NovaContext.nav.activeModule`
   - "I see you're in Campaign Planner. Want me to generate a plan?"
2. **Campaign context** -- via `NovaContext.campaign`
   - "For Acme Corp's $50K budget targeting Software Engineers..."
3. **Recent outputs** -- via `NovaContext.shared`
   - "Based on the competitive analysis you just ran..."
4. **Module data** -- via DOM inspection of `#module-frame`
   - Can read form values, table data, chart state from the active module

### Action Capabilities

Nova AI can trigger module actions programmatically:

```
USER: "Create a media plan for Acme Corp Q3, $50K budget, targeting
       Senior Engineers in SF and Austin, across LinkedIn and Indeed"

NOVA AI ACTIONS:
  1. NovaContext.setCampaign({
       client: "Acme Corp", budget: 50000,
       targetRoles: ["Senior Engineer"],
       targetLocations: ["San Francisco, CA", "Austin, TX"],
       channels: ["LinkedIn", "Indeed"]
     })
  2. Navigate to #/plan/campaign-planner (if not already there)
  3. Fill form fields from context
  4. Trigger the "Generate Plan" API call
  5. Stream results back in the chat AND in the module frame
```

### Cross-Module Orchestration

Nova AI can chain workflows across modules:

```
USER: "Full campaign setup for Acme Corp"

NOVA AI ORCHESTRATES:
  Step 1: Set campaign context (client, budget, roles, locations)
  Step 2: Run Competitive Intel scan -> store results
  Step 3: Run Market Pulse for salary/demand data -> store results
  Step 4: Generate Campaign Plan (using intel + market data)
  Step 5: Run ROI projection on the generated plan
  Step 6: Run Compliance check on the plan
  Step 7: Summarize everything in chat with links to each module
```

### Suggested Actions Engine

The suggestion chips below the chat update based on context:

| Current Module | Suggested Actions |
|---------------|-------------------|
| Campaign Planner | "Generate plan" / "Add channel" / "Check compliance" |
| Competitive Intel | "Compare with last month" / "Export report" / "Use in plan" |
| Budget & Performance | "What-if +20% budget" / "Show ROI breakdown" / "Alert if overspend" |
| Talent Intelligence | "Top locations for role" / "Salary benchmark" / "Find candidates" |
| Dashboard (home) | "Start new campaign" / "Resume last campaign" / "What's trending" |
| Any module | "Explain this data" / "Export to sheets" / "Share with team" |

---

## 7. URL Routing Design

### Route Schema

```
Hash pattern:   #/<group>/<module>/<tab>

Examples:
  #/                                    -> Dashboard home
  #/plan/campaign-planner               -> Campaign Planner (default tab)
  #/plan/campaign-planner/quick         -> Campaign Planner, Quick Plan tab
  #/plan/campaign-planner/brief         -> Campaign Planner, Brief tab
  #/plan/social-creative                -> Social & Creative
  #/plan/social-creative/assets         -> Social & Creative, Creative Assets tab
  #/plan/testing-lab                    -> A/B Testing Lab
  #/intel/competitive                   -> Competitive Intel (default: Live)
  #/intel/competitive/reports           -> Competitive Intel, Reports tab
  #/intel/market-pulse                  -> Market Pulse
  #/intel/vendor-iq                     -> Vendor IQ
  #/optimize/budget-performance         -> Budget & Performance (default: Simulator)
  #/optimize/budget-performance/roi     -> ROI Calculator tab
  #/optimize/budget-performance/tracker -> Tracker tab
  #/optimize/budget-performance/post    -> Post-Campaign tab
  #/talent/talent-intel                 -> Talent Intelligence
  #/talent/talent-intel/heatmap         -> Heat Map tab
  #/talent/talent-intel/payscale        -> PayScale tab
  #/talent/talent-intel/skilltarget     -> SkillTarget tab
  #/talent/talent-intel/applyflow       -> ApplyFlow tab
  #/comply/compliance-center            -> Compliance Center
  #/comply/compliance-center/audit      -> Ad Audit tab
  #/settings                            -> Settings
  #/api-portal                          -> API Portal
```

### Router Implementation (Vanilla JS)

```
  ROUTER PSEUDOCODE
  =================

  const ROUTES = {
    '':                          { fragment: 'dashboard',           group: null },
    'plan/campaign-planner':     { fragment: 'media-plan',          group: 'plan',
                                   tabs: ['full','quick','brief'] },
    'plan/social-creative':      { fragment: 'social-plan',         group: 'plan',
                                   tabs: ['strategy','assets'] },
    'plan/testing-lab':          { fragment: 'ab-testing',          group: 'plan' },
    'intel/competitive':         { fragment: 'competitive',         group: 'intel',
                                   tabs: ['live','reports'] },
    // ... etc for all 10 modules
  };

  window.addEventListener('hashchange', handleRoute);

  function handleRoute() {
    const hash = location.hash.slice(2) || '';  // Remove #/
    const [group, module, tab] = hash.split('/');
    const route = ROUTES[`${group}/${module}`] || ROUTES[group] || ROUTES[''];

    // 1. Update sidebar active state
    setSidebarActive(group, module);

    // 2. Update context bar
    NovaContext.nav = { activeGroup: group, activeModule: module, activeTab: tab };
    NovaContext.emit('module:loaded', { group, module, tab });

    // 3. Fetch and swap content
    const url = `/fragment/${route.fragment}`;
    fetch(url)
      .then(r => r.text())
      .then(html => {
        document.getElementById('module-frame').innerHTML = html;
        // Execute inline scripts
        executeInlineScripts(document.getElementById('module-frame'));
        // If module has tabs, show the right one
        if (tab && route.tabs) activateTab(tab);
        // Scroll to top
        document.getElementById('module-frame').scrollTop = 0;
      });

    // 4. Push to navigation history
    NovaContext.nav.history.push(hash);
  }
```

### Deep Linking & Sharing

Users can share links like:
`https://media-plan-generator.onrender.com/platform#/plan/campaign-planner/quick`

This loads the shell, then the router reads the hash and navigates to the
Campaign Planner with the Quick Plan tab active.

### Backward Compatibility

During migration, BOTH systems work:
- `/media-plan` -> serves full standalone media-plan.html (legacy)
- `/platform#/plan/campaign-planner` -> serves inside shell (new)
- `/fragment/media-plan` -> serves the HTML fragment (shell consumption)

Legacy routes get a banner: "This page has moved to Nova Platform. [Open in Platform]"

---

## 8. Dashboard Home (replaces /hub)

### Layout

```
+------------------------------------------------------------------+
| SIDEBAR |                    DASHBOARD                            |
|         |                                                         |
|         |  Good morning, Shubham                      [+ New Campaign]
|         |                                                         |
|         |  +------------------+  +-----------------+  +----------+
|         |  | ACTIVE CAMPAIGNS |  | BUDGET HEALTH   |  | MARKET   |
|         |  | Acme Q3: 3 plans |  | $42K / $50K     |  | SIGNALS  |
|         |  | Beta Q2: 1 plan  |  | ████████░░ 84%  |  | +3.2%    |
|         |  | Gamma: draft     |  | On track        |  | demand   |
|         |  +------------------+  +-----------------+  +----------+
|         |                                                         |
|         |  +------------------+  +-----------------+  +----------+
|         |  | COMPLIANCE SCORE |  | QUICK ACTIONS   |  | TALENT   |
|         |  | 94/100           |  | > Create Plan   |  | HEAT     |
|         |  | 2 warnings       |  | > Run Audit     |  | 12 hot   |
|         |  | Last: 2h ago     |  | > Check ROI     |  | markets  |
|         |  +------------------+  | > Ask Nova AI   |  +----------+
|         |                        +-----------------+              |
|         |                                                         |
|         |  RECENT ACTIVITY                                        |
|         |  -------------------------------------------------------+
|         |  [icon] Media Plan generated for Acme Corp    2h ago    |
|         |  [icon] Competitive scan completed             5h ago    |
|         |  [icon] Budget alert: Gamma Q3 at 90%         1d ago    |
|         |  [icon] Compliance audit passed                1d ago    |
|         |                                                         |
+------------------------------------------------------------------+
```

### Dashboard Widgets (6 cards)

| Widget | Data Source | Updates |
|--------|------------|---------|
| Active Campaigns | Supabase campaign_contexts | Real-time |
| Budget Health | /api/metrics (simulator data) | Every 5 min |
| Market Signals | /api/market-pulse/news | Every 15 min |
| Compliance Score | /api/audit/analyze (last result) | On demand |
| Quick Actions | Static + context-aware | Instant |
| Talent Heat | /api/talent-heatmap data | Every 30 min |

### New User Experience

If no campaigns exist:
```
+------------------------------------------------------------------+
|                                                                    |
|           Welcome to Nova Platform                                 |
|                                                                    |
|           Let's set up your first campaign.                       |
|                                                                    |
|           [  Client name:  _______________  ]                     |
|           [  Budget:       _______________  ]                     |
|           [  Target roles: _______________  ]                     |
|           [  Locations:    _______________  ]                     |
|                                                                    |
|           [ Start with AI ]    [ Manual Setup ]                   |
|                                                                    |
|           "Start with AI" opens Nova AI to build the              |
|           campaign context via conversation.                      |
|                                                                    |
+------------------------------------------------------------------+
```

---

## 9. Migration Strategy: Current State -> 2-Product Model

### Guiding Principles
- **Zero downtime.** Production never breaks.
- **Incremental.** Each phase is independently deployable and testable.
- **Backward compatible.** Old URLs keep working until explicitly sunset.
- **Feature flagged.** A query param `?shell=1` or cookie enables the new shell for testing before it becomes the default.

### Phase Overview

```
Phase 1 ──> Phase 2 ──> Phase 3 ──> Phase 4 ──> Phase 5 ──> Phase 6
Shell +     Fragment    Shared      Nova AI     Dashboard   Module
Router      Endpoints   Context     Overlay     Widgets     Merging
(1 week)    (2 weeks)   (1 week)    (2 weeks)   (1 week)    (2 weeks)
                                                             ~~~~~~~~
                                                             Total: ~9 weeks
```

---

### Phase 1: Shell + Router Foundation (Week 1)

**Goal:** Create platform-shell.html with sidebar, router, and empty content frame.
No existing templates are modified.

**Tasks:**
1. Create `templates/platform-shell.html`
   - HTML shell with sidebar nav, top bar, `#module-frame` div
   - Inline CSS: design tokens, sidebar styles, layout grid
   - Inline JS: hash router, sidebar toggle, keyboard shortcuts
2. Add route in `app.py`:
   - `GET /platform` -> serves platform-shell.html
   - `GET /platform/` -> same
3. Add fragment route handler in `app.py`:
   - `GET /fragment/<name>` -> reads `templates/<name>.html`, strips outer tags, returns inner content
   - Helper function: `_extract_fragment(full_html) -> str` that strips `<!doctype>`, `<html>`, `<head>`, `<body>` wrappers
4. Sidebar renders all 10 modules (links to hash routes)
5. Router fetches fragments and swaps into `#module-frame`
6. Feature flag: `/platform` is opt-in, `/hub` remains default

**Deliverables:**
- `/platform` shows shell with working sidebar navigation
- Clicking any sidebar item loads the existing template as a fragment
- All 24 products accessible (some grouped under one sidebar item, tabs TBD)
- Legacy routes (`/media-plan`, `/tracker`, etc.) still work unchanged

**Risk:** Inline `<script>` in fragments may not execute when injected via innerHTML.
**Mitigation:** The `executeInlineScripts()` function finds all `<script>` tags in
the swapped fragment, creates new `<script>` elements with the same content, and
appends them to the DOM. This is a well-known pattern for vanilla JS SPAs.

---

### Phase 2: Fragment Endpoints + Template Splitting (Weeks 2-3)

**Goal:** Each template can serve both as a standalone page AND as a fragment.
This is the heaviest migration work.

**Tasks per template (repeat for all 24):**
1. Identify shared CSS (design tokens, reset, typography) vs. module-specific CSS
2. Prefix all module CSS classes with a namespace (e.g., `.mp-` for media-plan)
   to prevent collisions when multiple fragments share the shell CSS
3. Extract the `<body>` inner content into a "fragment-ready" block
4. Server-side: `_extract_fragment()` strips the outer HTML wrapper
5. Test: fragment loads correctly inside shell AND standalone page still works

**Template modification pattern:**
```html
<!-- BEFORE: templates/tracker.html -->
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Campaign Tracker</title>
  <style>
    /* 200 lines of CSS including reset, tokens, tracker-specific */
  </style>
</head>
<body>
  <div class="container">
    <!-- tracker UI -->
  </div>
  <script>
    // tracker JS
  </script>
</body>
</html>

<!-- AFTER: templates/tracker.html -->
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Campaign Tracker</title>
  <style>
    /* ONLY tracker-specific CSS, prefixed with .trk- */
    /* Shared tokens/reset come from shell when loaded as fragment */
  </style>
</head>
<body>
  <!-- FRAGMENT START (server extracts from here) -->
  <div class="trk-container" data-nova-module="tracker">
    <!-- tracker UI, all classes prefixed with trk- -->
  </div>
  <script>
    // tracker JS, wrapped in IIFE to avoid global pollution
    (function() {
      'use strict';
      const ctx = typeof NovaContext !== 'undefined' ? NovaContext : null;
      // ... tracker logic, reads ctx.campaign if available
    })();
  </script>
  <!-- FRAGMENT END -->
</body>
</html>
```

**Server-side extraction:**
```python
def _extract_fragment(html: str) -> str:
    """Strip <html>, <head>, <body> wrappers. Return inner content only."""
    # Find content between <body> and </body>
    # Keep <style> blocks that are inside <body> (module-specific CSS)
    # Keep <script> blocks
    # Return the fragment
```

**Order of template migration** (by dependency, simplest first):
1. roi-calculator (small, standalone)
2. ab-testing (small, standalone)
3. quick-brief (small, merges into campaign-planner later)
4. quick-plan (small, merges into campaign-planner later)
5. audit (medium, merges into compliance-center later)
6. skill-target, payscale-sync, talent-heatmap (medium, similar structure)
7. applyflow-demo, hire-signal (medium)
8. vendor-iq, market-pulse, market-intel (medium)
9. competitive, social-plan, creative-ai (larger)
10. tracker, simulator, post-campaign (larger)
11. compliance-guard (larger)
12. media-plan / index (largest, most complex -- last)

---

### Phase 3: Shared Context Layer (Week 4)

**Goal:** Implement NovaContext and the PubSub event system. Wire the campaign
context bar in the shell. Modules begin reading shared context.

**Tasks:**
1. Implement `NovaContext` object in platform-shell.html
2. Implement PubSub system (subscribe/emit)
3. Add campaign context bar to shell top bar
   - Dropdown to switch campaigns or create new
   - Shows: client name, budget, date range
4. localStorage persistence (auto-save on change, debounced)
5. Supabase persistence (new table: `campaign_contexts`)
6. Update 3 pilot modules to read/write context:
   - Campaign Planner: writes campaign fields on plan generation
   - Budget & Performance: reads campaign.budget for defaults
   - Competitive Intel: reads campaign.client for competitor search
7. "Use in Plan" buttons: Competitive Intel and Market Pulse get a button
   that writes their output to `NovaContext.shared.lastCompetitiveData`

**Supabase schema addition:**
```sql
CREATE TABLE IF NOT EXISTS campaign_contexts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_email TEXT NOT NULL,
  campaign_name TEXT NOT NULL,
  campaign_data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_campaign_ctx_user ON campaign_contexts(user_email);
```

---

### Phase 4: Nova AI Overlay (Weeks 5-6)

**Goal:** Move Nova chatbot from standalone /nova page to a persistent drawer
in the platform shell. Add context awareness and action capabilities.

**Tasks:**
1. Create Nova AI drawer component in platform-shell.html
   - Right-side slide-in drawer, 384px wide
   - Toggle with FAB button (bottom-right) or Cmd+J
2. Port nova.html chat UI into the drawer
   - Chat history, message bubbles, input bar, send button
   - Streaming response support (SSE via /api/chat/stream)
3. Context injection: on every message, include:
   - Active module name
   - Campaign context summary
   - Last shared data (if relevant to the query)
4. Suggested actions engine:
   - Context-aware chips that update when module changes
   - Subscribe to `module:loaded` event
5. Action execution: Nova AI can:
   - Navigate to modules: `NovaRouter.navigate('#/plan/campaign-planner')`
   - Fill form fields: `document.querySelector('[name=budget]').value = 50000`
   - Trigger API calls: `fetch('/api/generate', { body: planPayload })`
   - Write to context: `NovaContext.setCampaign({ budget: 50000 })`
6. Voice input via ElevenLabs STT (already integrated)
7. Remove /nova standalone page from sidebar (redirect to shell)

**Migration of nova.html:**
- nova.html remains as a standalone fallback (legacy URL)
- The drawer version extracts only the chat UI portion
- All API calls (/api/chat, /api/chat/stream, /api/nova/chat) remain unchanged
- The drawer JS adds context headers to API requests

---

### Phase 5: Dashboard Widgets (Week 7)

**Goal:** Replace the hub landing page catalog with a live dashboard inside
the platform shell.

**Tasks:**
1. Create dashboard fragment (templates/dashboard-home.html)
   - 6 widget cards (see Section 8)
   - CSS grid layout, responsive
2. Each widget fetches its data from existing API endpoints:
   - Active Campaigns: Supabase campaign_contexts query
   - Budget Health: /api/metrics
   - Market Signals: /api/market-pulse/news
   - Compliance Score: /api/audit status endpoint
   - Quick Actions: static + context-aware
   - Talent Heat: /api/talent-heatmap endpoint
3. Auto-refresh widgets on a schedule (5/15/30 min intervals)
4. Quick Actions link to module hash routes
5. Recent Activity feed from a new /api/activity endpoint
   (logs module usage + API calls with timestamps)
6. New User wizard (if no campaigns exist)
7. Make #/ (empty hash) load the dashboard by default

---

### Phase 6: Module Merging (Weeks 8-9)

**Goal:** Reduce 24 separate templates to 10 unified modules by merging
related products into tabbed interfaces.

**Merges to execute:**

| New Module | Products to Merge | Merge Pattern |
|-----------|-------------------|---------------|
| Campaign Planner | media-plan + quick-plan + quick-brief | Tab bar: Full / Quick / Brief. Shared form fields, different output complexity. |
| Social & Creative | social-plan + creative-ai | Tab bar: Strategy / Assets. Strategy feeds into Assets. |
| Competitive Intel | competitive + market-intel | Tab bar: Live Monitor / Reports. Reports uses competitive data. |
| Budget & Performance | simulator + roi-calculator + tracker + post-campaign | Tab bar: Simulator / ROI / Tracker / Post-Campaign. All share budget context. |
| Talent Intelligence | hire-signal + talent-heatmap + payscale-sync + skill-target + applyflow-demo | Tab bar: Signals / Heat Map / PayScale / SkillTarget / ApplyFlow. All share role/location context. |
| Compliance Center | compliance-guard + audit | Tab bar: Guard / Ad Audit. Guard is proactive, Audit is reactive. |

**Merge pattern for each:**
1. Create a new fragment template (e.g., `templates/module-campaign-planner.html`)
2. Add a tab bar component at the top
3. Each tab contains the content from its source template
4. Shared form fields (client, budget, roles) read from NovaContext
5. Tab state persists in the URL hash: `#/plan/campaign-planner/quick`
6. Old fragment endpoints redirect to the merged module with the right tab

**Tab bar component (reusable across all merged modules):**
```html
<div class="nova-tabs" data-module="campaign-planner">
  <button class="nova-tab active" data-tab="full">Full Plan</button>
  <button class="nova-tab" data-tab="quick">Quick Plan</button>
  <button class="nova-tab" data-tab="brief">Brief</button>
</div>
<div class="nova-tab-content" id="tab-full"> ... </div>
<div class="nova-tab-content" id="tab-quick" style="display:none"> ... </div>
<div class="nova-tab-content" id="tab-brief" style="display:none"> ... </div>
```

---

## 10. Design System Alignment

### Shared Design Tokens (extracted from existing templates)

Current state: hub.html uses `--accent: #6366f1` while nova.html uses
`--accent-purple: #5a54bd`. The platform shell unifies all tokens.

```css
/* platform-shell.html -- canonical design tokens */
:root {
  /* Brand */
  --nova-port-gore:     #202058;
  --nova-blue-violet:   #5A54BD;
  --nova-downy-teal:    #6BB3CD;

  /* Surfaces */
  --nova-bg-primary:    #0a0a0f;
  --nova-bg-secondary:  #111118;
  --nova-bg-card:       rgba(17, 17, 24, 0.85);
  --nova-bg-elevated:   #1a1a24;

  /* Text */
  --nova-text-primary:  #e4e4e7;
  --nova-text-secondary:#a1a1aa;
  --nova-text-muted:    #52525b;

  /* Borders */
  --nova-border:        rgba(255, 255, 255, 0.08);
  --nova-border-hover:  rgba(255, 255, 255, 0.16);

  /* Accent */
  --nova-accent:        var(--nova-blue-violet);
  --nova-accent-glow:   rgba(90, 84, 189, 0.15);

  /* Status */
  --nova-success:       #22c55e;
  --nova-warning:       #f59e0b;
  --nova-error:         #ef4444;
  --nova-info:          var(--nova-downy-teal);

  /* Radii */
  --nova-radius-sm:     6px;
  --nova-radius-md:     10px;
  --nova-radius-lg:     14px;

  /* Spacing */
  --nova-space-1:       4px;
  --nova-space-2:       8px;
  --nova-space-3:       12px;
  --nova-space-4:       16px;
  --nova-space-5:       24px;
  --nova-space-6:       32px;
  --nova-space-7:       48px;
  --nova-space-8:       64px;

  /* Typography */
  --nova-font:          'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  --nova-font-mono:     'JetBrains Mono', 'Fira Code', monospace;

  /* Animation */
  --nova-ease:          cubic-bezier(0.16, 1, 0.3, 1);
  --nova-duration:      200ms;

  /* Layout */
  --nova-sidebar-w:     260px;
  --nova-sidebar-w-collapsed: 64px;
  --nova-topbar-h:      56px;
  --nova-ai-drawer-w:   384px;
}
```

### CSS Architecture Rule

Modules MUST NOT override shared tokens. They MAY define module-scoped custom
properties for internal use:

```css
/* Inside module-campaign-planner.html fragment */
.cp-container {
  --cp-form-gap: var(--nova-space-5);
  --cp-card-bg: var(--nova-bg-card);
}
```

---

## 11. Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Cmd+K / Ctrl+K | Open command palette |
| Cmd+J / Ctrl+J | Toggle Nova AI drawer |
| [ | Toggle sidebar collapse |
| Cmd+1..5 / Ctrl+1..5 | Jump to group (Plan, Intel, Optimize, Talent, Comply) |
| Esc | Close overlay / drawer / palette |
| Cmd+Shift+P | Switch campaign |
| ? | Show keyboard shortcuts overlay |

---

## 12. Performance Budget

| Metric | Target | Strategy |
|--------|--------|----------|
| Shell load (LCP) | <1.5s | Single HTML file, inline critical CSS, no external JS bundles |
| Module swap (fragment load) | <300ms | fetch() with cache headers, preload adjacent modules |
| Context read | <1ms | In-memory JS object, no async |
| Context persist (local) | <5ms | Debounced localStorage write |
| Context persist (Supabase) | <500ms | Debounced async POST, non-blocking |
| Total shell weight | <150KB | Inline CSS + JS, no framework overhead |
| Fragment weight (avg) | <80KB | Module CSS + HTML + JS, no duplicated tokens |

### Preloading Strategy

When a user hovers over a sidebar item for >200ms, prefetch that module's fragment:
```javascript
sidebarItem.addEventListener('mouseenter', () => {
  prefetchTimer = setTimeout(() => {
    fetch(`/fragment/${moduleName}`);  // Browser caches the response
  }, 200);
});
```

---

## 13. Testing Strategy

| Layer | What to Test | Tool |
|-------|-------------|------|
| Unit | Router logic, NovaContext methods, PubSub | In-browser console tests |
| Fragment | Each fragment renders correctly in shell AND standalone | Manual + screenshot comparison |
| Integration | Module reads/writes context correctly | Automated fetch() tests in test-runner.html |
| Cross-module | Data flows from Intel -> Planner via shared context | End-to-end scenario test |
| Nova AI | Chatbot receives context, executes actions | Manual conversation tests |
| Navigation | All hash routes resolve, back/forward works, deep links work | Automated hashchange tests |
| Performance | LCP, fragment swap time, memory leaks | Lighthouse + manual profiling |
| Accessibility | Keyboard nav, screen reader, focus management | axe-core + manual |

---

## 14. Rollout Plan

```
WEEK 1:  Phase 1 -- Shell + Router
         -> Deploy behind /platform URL
         -> Internal testing, dogfooding
         -> Legacy /hub and all product URLs unchanged

WEEK 2-3: Phase 2 -- Fragment Endpoints
         -> Migrate templates one by one
         -> Each template PR: fragment works in shell + standalone works
         -> Feature flag: cookie nova_shell=1 redirects /hub to /platform

WEEK 4:  Phase 3 -- Shared Context
         -> NovaContext live, 3 pilot modules wired
         -> Campaign context bar visible in shell
         -> Supabase table created

WEEK 5-6: Phase 4 -- Nova AI Overlay
         -> Chat drawer working in shell
         -> Context-aware suggestions
         -> Action execution (navigate, fill, trigger)
         -> /nova page shows "Use in Platform" banner

WEEK 7:  Phase 5 -- Dashboard Widgets
         -> Dashboard replaces blank #/ state
         -> 6 live widgets
         -> New user wizard

WEEK 8-9: Phase 6 -- Module Merging
         -> 24 templates -> 10 merged modules
         -> Tab navigation within modules
         -> Old fragment endpoints redirect to merged modules

WEEK 10: Cutover
         -> /hub redirects to /platform (with 1-week "go back" option)
         -> /nova redirects to /platform with AI drawer open
         -> All legacy product URLs show "moved" banner + redirect
         -> Announce on landing page

WEEK 11+: Polish
         -> Remove legacy routes (after analytics confirm zero traffic)
         -> Role-based sidebar visibility
         -> Favorites / pinned modules
         -> Module search in sidebar
         -> Notification badges on modules
         -> Dark/light theme toggle
```

---

## 15. Open Questions / Decisions Needed

| # | Question | Options | Recommendation |
|---|----------|---------|----------------|
| 1 | Should fragments cache in browser? | Cache with ETag / No cache | Cache with short max-age (60s) + ETag for instant revalidation |
| 2 | How to handle CSS collisions during migration? | BEM namespacing / CSS Modules (build step) / Inline scoped styles | BEM namespacing with module prefix (no build step needed) |
| 3 | Should the command palette replace the current hub search? | Yes / Keep both | Yes -- Cmd+K is strictly better |
| 4 | Mobile: bottom tab bar or hamburger sidebar? | Bottom tabs (5 groups) / Hamburger | Bottom tabs -- more discoverable, matches native app patterns |
| 5 | Should modules lazy-load or all prefetch? | Lazy / Prefetch adjacent / Prefetch all | Lazy + prefetch on hover (best balance of speed vs bandwidth) |
| 6 | Nova AI drawer width on desktop? | 320px / 384px / 420px | 384px -- enough for code blocks, not too wide |
| 7 | Should legacy URLs redirect immediately or show a migration banner? | Redirect / Banner + manual click | Banner for 2 weeks, then auto-redirect |
| 8 | HTMX adoption in Phase 2+? | Pure fetch / Add HTMX for partial swaps | Defer to post-launch. Fetch is sufficient, HTMX is a nice upgrade later. |

---

## 16. Success Metrics

| Metric | Before (24 pages) | Target (2 products) |
|--------|-------------------|---------------------|
| Time to switch tools | 2-4s (full page load) | <300ms (fragment swap) |
| Steps to create plan with intel data | 8+ clicks across 3 pages | 2-3 clicks (context flows) |
| Tools accessible from current view | 1 (must go back to hub) | All 10 (sidebar) |
| AI context in chatbot | Zero (standalone page) | Full (campaign + module + history) |
| Shared data between tools | None (copy-paste) | Automatic (PubSub events) |
| Onboarding time for new user | ~15 min (find tools in hub) | ~5 min (guided wizard + sidebar) |
| User engagement (modules/session) | 1.8 avg | Target: 4+ avg |

---

## 17. File Map (New + Modified Files)

```
NEW FILES:
  templates/platform-shell.html        -- The single shell page (Product 1)
  templates/dashboard-home.html        -- Dashboard fragment for #/
  templates/module-campaign-planner.html   -- Merged: media-plan + quick-plan + brief
  templates/module-social-creative.html    -- Merged: social-plan + creative-ai
  templates/module-competitive-intel.html  -- Merged: competitive + market-intel
  templates/module-budget-perf.html        -- Merged: simulator + roi + tracker + post
  templates/module-talent-intel.html       -- Merged: 5 talent products
  templates/module-compliance.html         -- Merged: compliance-guard + audit

MODIFIED FILES:
  app.py                               -- New routes: /platform, /fragment/*
  templates/*.html (all 24)            -- CSS namespace prefixes, IIFE wrapping
  static/nova-chat-widget.js           -- Drawer mode support (if applicable)

UNCHANGED (served outside shell):
  templates/pricing.html
  templates/privacy.html
  templates/terms.html

DEPRECATED (redirect to shell after cutover):
  templates/hub.html                   -- Replaced by platform-shell + dashboard
  templates/nova.html                  -- Replaced by Nova AI drawer
  templates/dashboard.html             -- Replaced by dashboard-home.html
  templates/observability.html         -- Moved to Settings > System Health
```

---

## Appendix A: Comparison with Previous Blueprint

| Dimension | Previous (v1) | This (v2) |
|-----------|---------------|-----------|
| Products | 6 toolkits, 17 tools | 2 products (Platform + AI) |
| Navigation model | Toolkit picker -> tool list | Flat sidebar with grouped modules |
| Content delivery | Full page loads | Fragment swap in shell |
| Shared state | "Planned" but unspecified | NovaContext with PubSub + persistence |
| Nova AI | "Persistent sidebar" (aspirational) | Fully specified drawer with action API |
| Module count | 17 (light merging) | 10 (aggressive merging) |
| Migration plan | 6 vague phases | 6 phases with specific tasks and template order |
| Design system | Unspecified | Unified token set with namespace rules |
| Routing | Unspecified | Hash-based with 3-level schema |
| Dashboard | 6 widgets (names only) | 6 widgets with data sources and refresh rates |
| Timeline | "1-2 weeks" per phase (vague) | 9-week total with weekly breakdown |

---

## Appendix B: Full Route Table

```
HASH ROUTE                             FRAGMENT ENDPOINT        LEGACY URL
----------                             -----------------        ----------
#/                                     /fragment/dashboard-home /hub
#/plan/campaign-planner                /fragment/media-plan      /media-plan
#/plan/campaign-planner/quick          /fragment/media-plan      /quick-plan
#/plan/campaign-planner/brief          /fragment/media-plan      /quick-brief
#/plan/social-creative                 /fragment/social-plan     /social-plan
#/plan/social-creative/assets          /fragment/creative-ai     /creative-ai
#/plan/testing-lab                     /fragment/ab-testing      /ab-testing
#/intel/competitive                    /fragment/competitive     /competitive
#/intel/competitive/reports            /fragment/market-intel    /market-intel
#/intel/market-pulse                   /fragment/market-pulse    /market-pulse
#/intel/vendor-iq                      /fragment/vendor-iq       /vendor-iq
#/optimize/budget-performance          /fragment/simulator       /simulator
#/optimize/budget-performance/roi      /fragment/roi-calculator  /roi-calculator
#/optimize/budget-performance/tracker  /fragment/tracker         /tracker
#/optimize/budget-performance/post     /fragment/post-campaign   /post-campaign
#/talent/talent-intel                  /fragment/hire-signal     /hire-signal
#/talent/talent-intel/heatmap          /fragment/talent-heatmap  /talent-heatmap
#/talent/talent-intel/payscale         /fragment/payscale-sync   /payscale-sync
#/talent/talent-intel/skilltarget      /fragment/skill-target    /skill-target
#/talent/talent-intel/applyflow        /fragment/applyflow-demo  /applyflow
#/comply/compliance-center             /fragment/compliance-guard /compliance-guard
#/comply/compliance-center/audit       /fragment/audit           /audit
#/api-portal                           /fragment/api-portal      /api-portal
#/settings                             /fragment/settings        (new)
```

Note: During Phase 6 (module merging), the separate fragment endpoints for
merged products will redirect to the unified module fragment. For example,
`/fragment/quick-plan` will return the Campaign Planner fragment with the
Quick tab pre-activated.

---

*End of blueprint. Implementation begins with Phase 1: Shell + Router.*
