# Nova Plan Studio — Session Handoff (2026-06-14)

Frontend redesign of `/media-plan` (the Media Plan Generator form) + a full
PM/design/architecture audit of MPG + Nova. Written for Shubham to resume and
deploy after being away.

---

## TL;DR

- **The Plan Studio redesign is LIVE in production.** `/media-plan` is now a
  two-pane cockpit (dark inputs + a light, brand-true live plan preview that
  recomputes as you type), extended across all 5 steps + the results/output
  screen, with a dialed-up deep-indigo + brand-aurora background and a
  light "deck-assembling" generation moment.
- **This session added more work that is committed but NOT pushed** (you were
  away; production deploys need your explicit OK). It sits on branch
  **`plan-studio-frontend`** (HEAD `22a7ec9`), 4 commits ahead of the redesign
  baseline. Safe to review, then push.
- **A 5-specialist product audit ran** (PM/flow, data/inputs, Nova widget,
  signup, performance/content). Prioritized roadmap below.

---

## Production state (what users see now)

`origin/main` = **`ae0b59a`** =
  - `86d022f` — my Plan Studio redesign + dialed-up background (LIVE), plus
  - `ae0b59a` — the concurrent session's S90 monitoring fix (grace-gate the
    global error-rate alert).

So production already has the **full redesign + dialed-up background**. Verified
live earlier this session (curl + the deploy poll).

---

## Committed but NOT pushed — branch `plan-studio-frontend` (base `86d022f`)

| Commit | What |
|---|---|
| `488cb33` | Results dashboard polish — brand-accent title bar + hero Total-Budget tile |
| `e352c22` | **Perf:** gzip + `Cache-Control` on `/media-plan` (**518KB → 105KB**, verified via curl). **Signup:** honest, outcome-led value prop ("Build a data-backed media plan in minutes" + the real-Joveo-benchmarks moat + "Your Joveo planning workspace"), `@joveo.com` helper line, fixed dishonest guest banner, actionable access-denied message. `?v=s50→s51` cache-buster. |
| `92eab61` | **Flow:** Step 1's optional sections (Competitive Intelligence + Historical Data) collapsed into a native `<details>` "Add context (optional)" accordion — required fields rise to the top; all fields stay in the DOM. |
| `22a7ec9` | **Flow:** preset chips enriched into complete scenarios (now also set duration / hire volume / experience → richer live preview). |

**To deploy when you're back:**
```
git checkout plan-studio-frontend
git fetch origin && git rebase origin/main      # rebase onto ae0b59a; conflict-free expected
                                                 # (they touched monitoring; I touched templates/ static/ routes/pages.py)
git push origin plan-studio-frontend:main        # needs your explicit OK (auto-deploys to Render)
```

---

## Reviewing locally (preview-loop gotchas)

- `python3 app.py 5099` → `http://localhost:5099/media-plan`.
- **Templates are cached at startup → restart the server after any
  template/inline-CSS edit.**
- **NEW:** the page now sends `Cache-Control: private, max-age=300`, so the
  browser caches it for 5 min. To see template edits in the preview, **hard-
  reload or hit `/media-plan?cb=<random>`** (this is correct production
  behavior; it only affects the dev preview loop).
- Dismiss the auth gate with "Preview without signing in".

---

## The audit roadmap (Top-12, by impact-per-effort)

Status legend: ✅ done-local (on `plan-studio-frontend`) · 🔵 LIVE · ⬜ pending

| # | Initiative | Area | Status | Notes / evidence |
|---|---|---|---|---|
| 1 | Gzip + cache `/media-plan` (518KB→105KB) | Perf | ✅ | `routes/pages.py:_serve_template` |
| 2 | Pass `client_website` → `enrich_company()` | Data | ⬜ | one-liner `app.py:16377`; `api_enrichment.py:2877` already accepts it. **Backend (shared file) — do with the concurrent session.** |
| 3 | Remove duplicate APAC/EMEA channel toggles | Flow | ⬜ | region drives them (`body_app_js.html:1399`); needs care so `_syncRegionChannels` + payload still work. |
| 4 | Cut/wire dead inputs | Data | ⬜ | `target_demographic` genuinely unused → cut or wire to PPT audience slide. **`experience_level` is NOT dead** — it now powers the live-preview seniority model (`body_preview_js.html`); wire it server-side, don't delete. |
| 5 | Bridge form context → Nova widget (`setContext`) | Widget | ⬜ | `setContext()` exists `nova-chat.js`, never called; chat reads only `context.role`. Frontend half safe; server half on `app.py`. |
| 6 | Render server `follow_ups` as clickable chips | Widget | ⬜ | server returns them (`nova.py:3106`); UI flattens to text. **Couldn't verify locally (needs LLM).** |
| 7 | Signup honest value-prop | Signup | ✅ | `static/nova-auth-gate.js` |
| 8 | **Verify Supabase env on Render** | Data | ⬜ **(your call)** | If `SUPABASE_URL`/`ANON_KEY` unset, the entire "Joveo measured" feature is dark in prod and #11 is moot. Decided: auth is **internal-only @joveo**. |
| 9 | Collapse Step 1 (partial) + merge budget/hire controls | Flow | 🟡 partial | accordion ✅; the budget(range+period+exact)→single-control and hire-volume→exact-first merges still pending — they change approved inputs, so **show a before/after first**. |
| 10 | Currency-aware generator | Perf | ⬜ | hardcoded USD (`excel_v2.py:239`) despite region selectors → non-US plans show "$". **L effort, backend, needs testing.** |
| 11 | Promote `cg_benchmarks` real outcomes → budget calibration | Data | ⬜ | today display-only (`budget_engine.py:2744`). Flag-gated, sample-weighted. Gated by #8. |
| 12 | `/api/copilot/extract` — paste brief → auto-fill 6 fields | Flow | ⬜ | the keystone bet; copilot today returns tips not extraction (`app.py:1171`). Needs a backend route + LLM. |

## The 3 differentiating bets (each unlocks existing infra)
1. **Brief-first auto-extract flow** (#12) — "tell us about the role" instead of "fill out this form."
2. **Context-aware Nova co-pilot** (#5/#6 + action cards that edit the plan).
3. **Real-outcome calibration** (#11) — Joveo's first-party `cg_benchmarks` (~6,175 rows) is the moat; today displayed, not used.

## My cross-checks on the audit (it over-claimed in 2 spots)
- `experience_level` is **used** now (live-preview seniority) — don't delete; wire server-side.
- Several "quick wins" touch `app.py` / `nova.py` / `budget_engine.py` / `excel_v2.py` — the **shared backend files a concurrent session also edits** (it just shipped `ae0b59a` + a `nova-alerting-s90` memory). Coordinate / rebase before pushing.

---

## Decisions captured this session
- **Auth = internal-only (@joveo.com).** Signup work = honesty/polish (done), not a prospect funnel.
- Background dialed up (vivid indigo + aurora) — approved + shipped.

## Open questions for you
- Is `SUPABASE_URL`/`ANON_KEY` set on Render? (gates #8/#11)
- OK to do the budget/hire-control merges (#9) — want the before/after first?
- Push `plan-studio-frontend` to main now, or review the diffs first?
