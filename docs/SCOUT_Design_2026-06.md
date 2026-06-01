# SCOUT — Nova UI/UX Design Audit (2026-06)

**Date:** 2026-06-02 · **Scope:** Nova chat (`/nova`), Hub (`/`), Media Plan Generator (`/`→`index.html`)
**Constraint:** Audit + recommend only. No code changed. "Without breaking anything" is paramount → recommendations favor additive CSS/JS.
**Browser tooling:** No Chrome/Playwright MCP available to this agent (only Read/Write/Edit/Bash). Evidence = live `curl` (HTTP headers, transfer sizes, HTTP 200 verified) + source inspection of `nova.html`, `nova.css`, `nova.js`. Visual screenshots not captured; all UI findings are traced to specific file + line.

---

## Executive summary

Nova is a genuinely strong, mature chat surface — most "2026 best practice" boxes are already ticked: token-by-token streaming (WebSocket primary, SSE fallback), skeleton + cycling "thinking" copy, per-tool progress pills, suggested follow-up chips, markdown tables, syntax-highlighted code blocks with copy, auto-charts over tables, sources panel, light/dark themes, TTS, PDF/TXT/JSON export, keyboard shortcuts (Cmd+K/Shift+C/Shift+T/Esc), CSRF auto-refresh, graceful retry banners. This is well above typical internal-tool quality.

The gaps are **polish and correctness at the edges**, not architecture. The three issues worth fixing first are all low-risk and additive:

1. **Inline `[1]` citations are a false affordance** — styled as clickable pills with `role=button`, `tabindex=0`, and a `.citation-tooltip` hover style, but **no JS ever populates the tooltip or wires a click**. Screen-reader users hear "button," keyboard users land on a dead element, mouse users hover and nothing appears. (`nova.js:625`, `nova.css:957-976`)
2. **Decorative orb canvas ignores `prefers-reduced-motion`** — a 60-dot rAF loop runs continuously; it only pauses on tab-hide, never on the motion preference, despite CLAUDE.md mandating reduced-motion support. (`nova.js:3725-3791`)
3. **Static assets are served `no-cache, must-revalidate`** — `nova.js` (138 KB raw / 25 KB brotli) and `nova.css` force a revalidation round-trip on every visit. A one-line cache header (or `?v=` immutable strategy) removes a repeat-visit latency tax. (live headers below)

None of the three touches core chat layout or message flow.

---

## 1. Chat UX (flagship — highest priority)

### 1.1 Streaming — STRONG, no change needed
**Current:** True token-by-token streaming. WebSocket primary (`WS_URL` `/ws/chat`, `nova.js:9-12`, `_streamViaWebSocket` at 2488) with automatic SSE fallback (`fetch(STREAM_URL)` at 2905, `reader.getReader()` + `TextDecoder` loop at 2919-2974). A blinking `streaming-cursor` is appended/removed per chunk (2962-2967). WS failure tracking with cooldown + periodic reconnect (`_wsFailCount`, `_startWsRetryCooldown` 2367). `reader.read()` has a 90 s reader-timeout watchdog (2902, `resetReaderTimeout`).
**Gap vs best practice:** Markdown is **re-rendered from scratch on every token** — `content.innerHTML = renderMarkdown(fullText)` (2958). For long answers with tables/code this is O(n²) string work + full reparse each chunk, which can cause jank and layout shift mid-stream. Most 2026 chat UIs append deltas or debounce the markdown pass.
**Recommended upgrade:** Debounce the markdown render during streaming (e.g. re-render at most every ~60-80 ms via `requestAnimationFrame` throttle), rendering raw text in between. Final pass on `done` stays exact.
**Effort:** S · **Risk:** Low (purely the render cadence; output identical). Additive throttle around line 2958.

### 1.2 Loading / thinking states — STRONG
**Current:** `showThinking()` (2100) renders avatar + 3 skeleton lines + a cycling label ("Analyzing…", "Searching 10,238+ publishers…", "Querying salary & labor data…", 2092-2098) on a 2.5 s rotation, plus an elapsed-seconds counter that appears after 2 s to avoid flicker (2137-2143). Per-tool pills with spinner→checkmark and fade-out (`showToolStatus` 2160-2214). This is best-in-class.
**Gap:** Minor — the skeleton lines are fixed-width; a subtle shimmer keyframe would read as more "alive." Cosmetic only.
**Recommended:** Optional shimmer on `.skeleton-line` (gated behind reduced-motion). **Effort:** S · **Risk:** Low.

### 1.3 Message rendering — GOOD, two correctness gaps
**Current:** Vanilla markdown renderer (`renderMarkdown` 545) handles code blocks (with per-language syntax highlighting + copy button), tables, headers, bold/italic, lists, links, paragraphs. Auto-charts (Chart.js, lazy-loaded) render donut/bar above chartable tables (640-1012) with brand palette and reduced-motion respected (842). Tables get horizontal scroll on mobile (need to verify `.message-content table { overflow-x }`).
**Gaps:**
- **(a) Citations non-functional** — see §3.1 / top-3. The renderer emits `<span class="citation" role="button" tabindex="0">` (625) but the styled `.citation-tooltip` (957) is never created in JS and no click handler scrolls to a source. False affordance.
- **(b) Regex markdown is fragile on edge cases** — nested lists, tables inside list items, or `*` inside code can mis-parse. Acceptable for an internal tool; flagged for awareness, not urgent.
**Recommended:**
- (a) On finalize, give each `.citation[data-cite=N]` a real tooltip populated from `msg.sources[N-1]` and a click that scrolls to / highlights the matching entry in the existing Sources panel. If sources can't be matched, **render `[1]` as plain text** (drop `role=button`/`tabindex`) so it stops lying to AT. (Wire inside `appendMessageDOM` after content insert.)
**Effort:** M · **Risk:** Low-Med (additive DOM wiring on already-rendered nodes; the "render as plain text" fallback is trivially safe).

### 1.4 Empty state / first-run — STRONG
**Current:** `renderWelcome()` (1507) shows an animated orb, a time-aware randomized greeting ("Good morning. How can I help?"), a data-capability subtitle, and a 2×2 grid of 4 suggestion cards with icons (1535-1543). A separate inline chip row also exists in `nova.html:504-529`. Welcome hides when first message arrives via MutationObserver.
**Gap:** Two parallel suggestion systems (welcome `.suggestion-card` grid in JS + `.suggestion-chip` row in HTML) is mild redundancy/inconsistency. No true first-run onboarding (no "seen before" flag), but the welcome screen serves that role well enough.
**Recommended:** Pick one suggestion surface (the welcome grid is richer) to avoid double-maintenance. **Effort:** S · **Risk:** Low.

### 1.5 Error states — STRONG
**Current:** `_showStreamError` (2790) renders an inline error banner with a working **Retry** button (2804). Error copy is well-differentiated: AbortError vs 429 ("Nova is busy…") vs 403 (auto-refreshes CSRF and retries once, *then* shows "Session expired") vs generic (2986-3013). The empty-response fallback (2729-2745, "S51 fix") explicitly stops blaming the user and offers concrete faster follow-ups. This is thoughtful, above-average error UX.
**Gap:** None material. **Risk:** —

### 1.6 Mobile / responsive — GOOD
**Current:** Breakpoints at 1024 px and 768 px (`nova.css:1547, 1565`), plus 480 px (2292). Sidebar collapses with overlay + toggle (`closeSidebarMobile`, `sidebar-overlay`). Input lives in a bottom `.input-wrapper` (thumb-reachable). Suggestions grid collapses (1572). `min-height: 44px` exists on at least one control (468).
**Gaps to verify on device (no browser MCP):**
- Touch targets: topbar icon buttons and `.send-btn` should be ≥44×44 px. Several buttons are 40 px (`height: 40px`, line 147). Below the WCAG 2.2 / iOS-HIG 44 px floor.
- Auto-chart `max-height:260px` donuts with right-side legends can crowd on narrow screens.
**Recommended:** Bump interactive controls to 44 px min on `≤768px`. **Effort:** S · **Risk:** Low (additive media-query rule).

### 1.7 Micro-interactions — GOOD
**Current:** `scrollToBottom()` uses smooth behavior (2085). Streaming cursor blink. Tool pills animate. Send button disables when empty/loading (3076).
**Gaps:**
- **Auto-scroll is unconditional** — `scrollToBottom()` fires on every token (2968). If a user scrolls **up** to read earlier text mid-stream, they're yanked back down. 2026 best practice: only auto-scroll if the user is already near the bottom ("sticky scroll"), and show a "↓ Jump to latest" pill otherwise.
- No message-entrance animation (new messages just appear). Minor.
**Recommended:** Add an `isNearBottom()` guard (within ~80 px of bottom) around the streaming `scrollToBottom()` and a floating "jump to latest" button when detached. **Effort:** S-M · **Risk:** Low (guards an existing call; does not change layout).

---

## 2. Accessibility (WCAG 2.2)

### 2.1 Color contrast — ONE failure, rest OK
**Tokens (dark):** `--text-primary:#e4e4e7`, `--text-secondary:#a1a1aa`, `--text-muted:#71717a` on `--bg #0d0d1a` (`nova.css:13-15`, `nova.html:42`).
- primary `#e4e4e7` on `#0d0d1a` ≈ **15:1** — passes AAA.
- secondary `#a1a1aa` ≈ **7.5:1** — passes AA/AAA.
- **muted `#71717a` ≈ 3.9:1 — FAILS WCAG AA (4.5:1) for normal text.** Used for timestamps, char-count, token indicator, "No conversations" helper, snippet text (e.g. `nova.html:636`, sidebar 1388/1458). These are small/secondary but still body-size text.
**Recommended:** Nudge `--text-muted` to ~`#8b8b94` (≈4.6:1) — still clearly "muted," now AA-compliant. **Effort:** S · **Risk:** Low (one token; cascades cleanly).

### 2.2 Keyboard navigation — GOOD
Enter-to-send / Shift+Enter newline (3122-3124). Global Cmd+K (focus input), Cmd+Shift+C (copy last), Cmd+Shift+T (theme), Esc (close modal/sidebar) (3684-3723). Conversation items are `tabindex=0` with Enter handlers (1481). Overflow menu closes on Esc and returns focus to trigger (`nova.html:944-950`). Solid.
**Gap:** The dead `.citation` (tabindex=0) puts a non-functional stop in tab order — fixed by §1.3a.

### 2.3 Screen reader — GOOD foundation, one streaming nuance
**Current:** `chat-area` is `role="log" aria-live="polite" aria-label="Chat messages"` (`nova.html:495-501`) — correct for a transcript. Sidebar `role=navigation`; conv list `role=list`. Avatars `aria-hidden`. Thinking indicator has `aria-label="Nova is thinking"` (2108). Charts get descriptive `aria-label` (884-894).
**Gap:** With `aria-live="polite"` on the *whole* log and markdown re-rendered every token (2958), some screen readers may re-announce large chunks or spam updates during streaming. Best practice: stream into a node and let only the final text be announced, or set the streaming node `aria-busy="true"` until `done`.
**Recommended:** Set `aria-busy="true"` on the streaming message during the token loop, clear on finalize (2722). **Effort:** S · **Risk:** Low (additive attribute).

### 2.4 Focus management — GOOD
Esc restores focus to overflow trigger. Modals (share/export) are click-outside + Esc dismissible. 
**Gap:** Share/Export/Shortcuts modals are not focus-*trapped* (Tab can escape behind the overlay), and focus isn't moved into the modal on open. Minor for an internal tool.
**Recommended:** On modal open, focus first actionable element; trap Tab within `.modal-content`. **Effort:** M · **Risk:** Low.

### 2.5 prefers-reduced-motion — PARTIAL (the notable miss)
**Current:** Honored broadly — inline `<style>` in `nova.html:45-52` zeroes animation/transition durations globally; `nova.css` has 4 reduced-motion blocks (728, 759, 2149, 2249); charts check the media query (842).
**Gap:** **The orb canvas bypasses CSS entirely** (`nova.js:3725-3791`). It's a JS rAF loop; the global CSS duration override can't stop a canvas redraw. It only pauses on `document.hidden` (3780). A motion-sensitive user gets a perpetually animating particle sphere.
**Recommended:** At orb init, check `window.matchMedia("(prefers-reduced-motion: reduce)").matches` (the pattern already used at `nova.js:843`); if reduced, draw **one static frame** and skip the rAF loop. **Effort:** S · **Risk:** Low (additive guard; orb is decorative `aria-hidden`).

### 2.6 Currency symbols (₹ £ € ৳) — verify
S79b reportedly addressed these. The chart numeric parser strips `$ € £ ¥ %` (`nova.js:763`) but **not `₹` (INR) or `৳` (BDT)** — so an Indian-rupee table cell like `₹45,000` won't parse to a number for auto-charts (it'd plot as 0). Rendering in text is fine; only the chart-axis parsing is affected.
**Recommended:** Add `₹৳₩₪` to the strip regex at line 763. **Effort:** S · **Risk:** Low.

---

## 3. Performance

### 3.1 Caching — clear repeat-visit win (top-3)
**Live evidence (curl, 2026-06-02):**
```
GET /static/js/nova.js   → content-type: application/javascript
                            cache-control: public, no-cache, must-revalidate
                            content-encoding: br · transferred 25,155 bytes (raw 138 KB)
GET /static/css/nova.css → cache-control: public, no-cache, must-revalidate
                            transferred 7,760 bytes (raw 50 KB)
GET /nova                → HTTP 200 · 35 KB · content-encoding: br · 0.45 s
```
Brotli is working well (138 KB → 25 KB). But `no-cache, must-revalidate` means **every page load re-validates each asset** (a conditional round-trip even on a 304). Assets are already cache-busted via `?v=s49` query strings (`nova.html:672-673`), so they could safely be `immutable, max-age=31536000`.
**Recommended:** For hashed/versioned static assets, serve `Cache-Control: public, max-age=31536000, immutable`. Removes the per-visit revalidation latency. **Effort:** S (one server header branch in `app.py`) · **Risk:** Low *provided* `?v=` is bumped on every deploy (it is, per `?v=s49`). Flag: if any asset is ever served without a version param, scope the immutable header to versioned requests only.

### 3.2 Bundle size — acceptable, optional split
`nova.js` is 138 KB raw (25 KB brotli) — one monolith covering chat, markdown, syntax highlighting, auto-charts, TTS, share/export, PDF, theme, orb, shortcuts. 25 KB on the wire is fine. Chart.js and jsPDF are already lazy/deferred (`_loadChartJs` 669; jsPDF `defer` `nova.html:882`). No urgent action.
**Optional:** Code-split rarely-used features (PDF export, orb) behind dynamic import to cut first-parse JS. **Effort:** M · **Risk:** Low-Med (touches load flow) — **defer; not worth the risk now.**

### 3.3 Layout shift (CLS) during streaming
Re-rendering full markdown every token (2958) can reflow tables/code blocks repeatedly mid-stream → visible CLS and the auto-scroll yank (§1.7). The §1.1 debounce + §1.7 sticky-scroll guard together largely resolve this. **Effort:** S-M · **Risk:** Low.

### 3.4 Font loading
`nova.html:19` uses `display=swap` (matches hub.html:77) — correct, no FOIT. `preconnect` to fonts.gstatic with crossorigin present (16-17). nova.css is `preload`+async-swapped (24-33) with `<noscript>` fallback, and critical colors are inlined (36-53) to prevent FOUC. This is already best practice.

---

## 4. 2026 design trends worth adopting (brand-respecting, additive)

Recommend **evolution, not rebrand** — PORT_GORE/BLUE_VIOLET/DOWNY_TEAL, Inter + Space Grotesk, glassmorphism, aurora all stay.

- **Functional inline citations + source cards** *(highest-value trend already half-built)* — finish §1.3a so `[1]` reveals a source card on hover/focus and jumps to the Sources panel. This is the single biggest "feels like a 2026 research assistant" upgrade and the scaffolding (sources panel, `data-cite`, tooltip CSS) already exists. **Effort:** M · **Risk:** Low.
- **Sticky-scroll + "jump to latest" pill** (§1.7) — now standard in ChatGPT/Claude/Perplexity UIs. **Effort:** S-M.
- **Suggested follow-ups already shipped** (`renderFollowups` 3053) — good. Consider persisting them under the last answer instead of removing on click, so users can pick a different branch. **Effort:** S · **Risk:** Low.
- **Free tooling for iteration (no code impact):** the project already has Magic MCP (21st.dev component patterns) and a design-token file (`tokens.css`). For visual-diff regression without a paid Chromatic/Percy seat, Playwright's built-in `toHaveScreenshot()` snapshot testing is free and already installed (`playwright` binary present) — a lightweight way to lock the chat UI against regressions. **Effort:** M (test setup) · **Risk:** none (test-only).
- **Container queries** for the auto-chart/table block (currently media-query driven) so charts adapt to the message column width rather than viewport. **Effort:** M · **Risk:** Low. Nice-to-have, not urgent.

---

## 5. Cross-product consistency

**Coherent overall.** Hub and Nova share fonts (Inter + Space Grotesk, `hub.html:77` ≈ `nova.html:19`), brand colors, dark theme, glassmorphism (`backdrop-filter: blur` hub:2861), aurora layers (hub:172-174), and the same "N" gradient logo. Hub nav links to `/nova` (hub.html:214). `back-to-suite.js` + sidebar "Back to Suite" (nova.html:267) tie the surfaces together. `tokens.css` + `buttons.css` are shared across both.

**Inconsistencies (minor):**
- **`index.html` is an 846-byte stub/redirect**, not a styled media-plan-generator page — the real generator UI lives elsewhere (dashboard/quick-plan templates). Worth confirming the intended entry; if `index.html` is a redirect, fine, but it means "Media plan generator" as a standalone branded surface isn't really at `/`.
- **Font weight sets differ:** nova.html loads Inter `400;500;600;700;800` (19); hub loads `500;600;700` (77). Nova pulls an extra 400 & 800 weight. Harmless, but consolidating the requested weight axis would marginally trim font payload and guarantee identical type rendering.
- **Two suggestion-prompt systems** in Nova (§1.4).
- Token color `--text-muted` failing contrast (§2.1) likely propagates to other templates that import `tokens.css` — fixing it once helps suite-wide.

---

## Prioritized upgrade plan

### P0 — broken / embarrassing (fix first; all additive, zero layout risk)
| # | Finding | File:line | Effort | Risk |
|---|---------|-----------|--------|------|
| P0-1 | Inline `[1]` citations are a false affordance (role=button + tabindex, empty tooltip, no click). Either wire to source cards **or** demote to plain text. | `nova.js:625`, `nova.css:957-976` | M | Low |
| P0-2 | Orb canvas ignores `prefers-reduced-motion` (violates CLAUDE.md mandate). Add matchMedia guard → static frame. | `nova.js:3725-3791` | S | Low |
| P0-3 | `--text-muted #71717a` fails WCAG AA (≈3.9:1). Nudge to ~`#8b8b94`. | `nova.css:15` | S | Low |

### P1 — clear wins
| # | Finding | File:line | Effort | Risk |
|---|---------|-----------|--------|------|
| P1-1 | Static assets `no-cache` → serve versioned assets `immutable, max-age=1y` (kills repeat-visit revalidation). | `app.py` static handler | S | Low |
| P1-2 | Sticky-scroll: only auto-scroll when near bottom + "jump to latest" pill (stop yanking users reading mid-stream). | `nova.js:2968`, 2085 | S-M | Low |
| P1-3 | Debounce markdown re-render during streaming (kills mid-stream CLS/jank from full reparse per token). | `nova.js:2958` | S | Low |
| P1-4 | Touch targets <44px on mobile → bump to 44px on ≤768px. | `nova.css:147` + media query | S | Low |
| P1-5 | `aria-busy` on streaming node so SR doesn't spam-announce every token. | `nova.js:2690-2722` | S | Low |

### P2 — polish
| # | Finding | Effort | Risk |
|---|---------|--------|------|
| P2-1 | Add `₹৳₩₪` to chart numeric-strip regex (`nova.js:763`). | S | Low |
| P2-2 | Focus-trap + initial focus in share/export/shortcuts modals. | M | Low |
| P2-3 | Consolidate the two suggestion-prompt systems into one. | S | Low |
| P2-4 | Skeleton shimmer (reduced-motion gated). | S | Low |
| P2-5 | Consolidate font weight axes between hub & nova; confirm `index.html` entry intent. | S | Low |
| P2-6 | Persist follow-up chips after click (allow branching). | S | Low |
| P2-7 | Free Playwright `toHaveScreenshot()` visual-regression harness (test-only, locks the UI). | M | None |

### The 3 highest-impact, lowest-risk wins
1. **P0-2 — orb reduced-motion guard** (S, Low). A documented accessibility mandate currently violated; the exact matchMedia pattern is already used elsewhere in the same file (`nova.js:843`). Trivial, safe, correct.
2. **P0-1 → demote/wire citations** (start with the *demote-to-plain-text* half: S, Low). Instantly removes a screen-reader/keyboard trap and a dead mouse affordance. Wiring real source cards (M) is the bigger feature, but the safe AA-compliance win is one regex tweak away.
3. **P1-1 — immutable cache headers for versioned assets** (S, Low). Pure repeat-visit speed with no UI change; assets already carry `?v=` busting so correctness is preserved.

All three are additive, touch no message layout or streaming logic, and directly serve the user's "upgrade without breaking the working product" goal.

---

## Files referenced
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/templates/nova.html`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/static/css/nova.css`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/static/js/nova.js`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/templates/hub.html`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/templates/index.html`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/static/css/tokens.css`, `buttons.css` (shared design system)
- `app.py` (static-asset cache headers — P1-1)
