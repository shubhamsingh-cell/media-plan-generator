# Session Handoff — S89 "World-Class Upgrade" Program

**Last updated:** 2026-06-13
**Purpose:** Resume this multi-turn upgrade in a fresh session with zero loss.
**Read order on resume:** this file → `docs/ARCHITECTURE_UPGRADE_2026.md` → memory
(`mpg-nova-upgrade-backlog.md`) → the session task list (#1–#16, mirrored in §5).

---

## 1. DO THIS FIRST on resume

The build fleet (`wf_2fe3d455-f1c`) is **DONE, integrated, verified, and live** —
all 9 units shipped (commits `98340e3`, `402aaf1`, `f3ad719`), 2,106 tests pass,
deliverables render, an adversarial pre-deploy review ran and its one real find
(PPTX currency global → thread-local race) was fixed + verified. **Nothing to
reconcile.** Working tree was clean at handoff. Just pick up the remaining queue
in §5.

> Always `git checkout -- data/audit_log.jsonl data/benchmark_drift_results.json
> data/job_posting_volumes.json` before committing — tests/renders write to them.

> ⚠️ **This working tree is shared with a concurrent session.** `main` moves and
> files get committed *under you*. Before every `git add`: re-run `git status`,
> stage **only your own files by explicit path** (never `git add -A`), and
> `git fetch` first. Direct pushes to `main` may be blocked by the permission
> classifier — see §2 for the 2026-06-13 unpushed commits awaiting authorization.

---

## 2. Current state
- **HEAD ≈ `cb3d3ea`** (+ this handoff refresh). `origin/main` last confirmed at
  `33661e4`. Live: https://media-plan-generator.onrender.com/ (`main` auto-deploys).
  Pushes to `main` are gated by the permission classifier (each batch needs
  explicit user OK). **Awaiting push at handoff:** `86dd040`, `cb3d3ea`
  (frontend) + this refresh — run `git log origin/main..HEAD`.
- Whole S89 program is shipped EXCEPT the items in §5 (mostly user-creds, the
  deliberately-deferred true-streaming work, or #8 which is blocked on a product
  decision per §6).

## 3. Shipped (history below; latest turn `f94c19c` → `0474f38`, 2026-06-13)
**PROD BUGFIX `0474f38`:** every `/api/admin/*` HTTP endpoint returned 503
"Auth module unavailable" in production (pre-existing). Root cause: `wsgi.py`'s
deferred-startup mirror called `auth.init()` but never set
`app._auth_module_loaded=True` (app.py's `__main__` path does), so the admin
gate stayed fail-closed under gunicorn. Fixed to flip the flag **only when an
admin key is configured** (`is_auth_enabled()`), else stay 503 (never expose
admin unauthenticated). After deploy: admin endpoints are 401-without-key /
work-with-key **iff `NOVA_ADMIN_KEY` is set in Render**; if not set they stay
503 — set `NOVA_ADMIN_KEY` to use them (incl. the #9 parity endpoint).

**This turn (CI hardening + primitive adoption + #8 investigation):**
- `f94c19c` fix(enrichment): stop scheduling 7 dead Firecrawl sources that fired
  false CRITICAL "multiple sources failed" alerts (firecrawl module deleted S72).
- `a58e857` refactor(enrichment): delete the 7 now-unreachable `_enrich_*` methods
  *(by the concurrent session)*.
- `4559149` **eval-gate in CI** + `evals/baseline_scores.json` (overall 93.75%,
  128 cases) — offline/deterministic `EvalSuite` gate; build reds on floor/3pp
  regression. **LIVE on origin.**
- `a2b6ad0` **pytest job in CI** — runs all 2,106 tests (offline, ~11s) on every
  push; CI previously ran zero tests. *(unpushed — see §2)*
- `ffe94cd` **call_llm_json adoption #1**: `data_synthesizer.generate_ai_narratives`
  now uses the structured-JSON primitive (schema + 1 retry) instead of one-shot
  `json.loads`; all fallbacks verified.
- `8443ee1` **#8 investigation + re-scope**: live schema inspection proved a
  `cg_daily_raw`→`cg_benchmarks` re-aggregation would corrupt the keystone; see
  `docs/BENCHMARK_REFRESH_FINDINGS_2026-06.md`. #8 now blocked on a product
  decision (§6).
- `a6c0fe3` / `185923d` / `553af96` **call_llm_json adoption — all 8 app.py
  sites** (`_verify_plan_data`, `_copilot_suggest_brief`, `_analyze_compliance`,
  `_generate_post_campaign_summary`, `_audit_complianceguard`,
  `_generate_creative_ads`, `_generate_ab_test_with_claude`) + 16 mocked tests.
  Full suite 2,127 passed; eval gate green. **Adoption complete.**
- **frontend brand pass — all surfaces, LIVE** (after user feedback that design
  hadn't improved; user then asked to take it across "all surfaces"):
  - `86dd040` hero: washed-gray white→50% headline (Space Grotesk; Poppins
    wasn't loaded) → bright #f6f5ff Poppins + purple→teal→magenta gradient accent
    + brand-color glows.
  - `cb3d3ea` + `21adaae` form: card lift (indigo→purple→teal top-accent stripe,
    indigo tint, brand mesh), Poppins headings, brand-tinted inputs + preset chips.
  - `420815c` landing: ALL headings (h1-h4/.section-title/.card-title/product
    titles) → Poppins via --font-heading (hub.css is landing-only; dark
    dashboards keep Space Grotesk by design); brighter .section-label eyebrows.
  - Nova chat widget: **already on-brand** (Poppins + full brand palette) — left
    as-is by design. Verified by rendering each page in the live preview.
  - `1e836ce` **generative hero demo**: replaced the orbit-ring hero artifact
    with a Linear-inspired product panel that animates Nova *building* a plan —
    counts the $50,000 budget up, fills 5 brand-gradient channel bars
    ($18,500→$4,000), counts outcome metrics (2,247 apps · 38 hires · $1,316/hire),
    flips "Analyzing 10,341 partners…" → "Plan ready". JS in `hub.js`
    (IntersectionObserver + load fallback, run-once guard, snaps to final on
    ready, full reduced-motion path); CSS `.genplan*` in `hub.css`. (Mobbin was
    login-gated, so I studied Linear's hero live in the user's Chrome instead.)
  See memory `frontend-design-bar.md`. Local dev preview: `.claude/launch.json`
  (gitignored) runs `python3 app.py 5099`; **RESTART the server after editing
  templates/inline CSS** (app caches templates at startup) + bump the `?v=` on
  linked CSS.
- **#9 map done** (workflow `wf_f9192b9b-43d`): per-accessor specs + parity
  design saved raw at `docs/_s89_9_parity_map_raw.json` — see §5 #9.

**Earlier session (commits `f216464` → `7bb9a80`):** Excel numeric/totals/freeze ·
scorecard/PDF P0 image-404 fixes (OG card + logo) · PPTX brand chart palette · KB
enrichment + gap-fill · Nova chat-UI fixes · MPG frontend brand/a11y · Opus 4.8
bump · architecture design doc · **keystone accessor** `get_real_outcomes()` ·
**L3.1** `llm_router.call_llm_json()`. All verified + live.

## 4. Decisions locked (user-delegated)
- **#11 embeddings:** GEMINI (free, key in stack) — not paid Voyage. Needs reindex.
- **#8 live refresh:** ⚠️ RE-SCOPED 2026-06-13 — a `cg_daily_raw`→`cg_benchmarks`
  re-aggregation **cannot be built in MPG** (it would corrupt the keystone:
  `cg_benchmarks` has client_name + revenue cols `avg_nr/avg_gr/avg_profit_pct`
  + `avg_multiplier` that don't exist in `cg_daily_raw` and aren't a clean
  formula; the transform is owned by the external `cg_*` uploader). See
  `docs/BENCHMARK_REFRESH_FINDINGS_2026-06.md`. Path A (preferred): MPG cron
  *triggers* the owning system's refresh; needs that entrypoint from the user.
- **#9 source of truth:** Supabase canonical (benchmarks/supply/salary/compliance);
  JSON KB = curated/editorial + offline fallback. Migrate via dual-read parity.
- **#10 MCP server:** internal-only, API-key gated (`NOVA_MCP_API_KEY`).
- **#14 agentic generation:** design doc + feature-flag rollout after eval gate.
- **#15 external MCPs:** deferred until our own MCP ships.

## 5. Task board (session tasks #1–#16)
- ✅ DONE + LIVE: #1 MPG frontend · #2 PPTX currency+fonts · #3 PDF/scorecard P1 ·
  #4 KB gap-fill · #5 structured-output primitive · #6 typed schema · #7 excel
  provenance · #10 MCP server · #13 eval gate (**now also runs in CI** + pytest
  suite in CI) · #14 agentic DESIGN · #16 keystone
- 🟡 #11 Gemini embeddings — cutover EXECUTED then ROLLED BACK 2026-07-21/22: prod GEMINI_API_KEY 403-blocks BatchEmbedContents (Google-console API restriction). **Needs you:** unblock embed methods on the key, then say the word — re-flip is a one-line code-default change (see docs/EMBEDDING_CUTOVER_RUNBOOK.md). All machinery live: gemini-embedding-2 target, blue/green collections, self-migrating deploys, public verify via /api/deploy/ready embedding block. The prior instruction
  here (reindex script + set env in Render dashboard) is **superseded and was
  DANGEROUS as written**: it targeted `text-embedding-004`, which Google shut
  down 2026-01-14, and — before the collection-scoping fix below — a
  `EMBEDDING_PROVIDER=gemini` reindex run would have dropped the live Voyage
  serving collection. New state: `vector_search.py` now targets
  `gemini-embedding-2` (current GA model) with **model-scoped Qdrant
  collections** (`_active_collection()` — Voyage keeps `nova_knowledge`,
  Gemini gets its own `nova_knowledge__gemini-embedding-2_768`), and startup
  indexing self-migrates the new space on deploy. The flip is now a
  **one-word `render.yaml` change** (`EMBEDDING_PROVIDER: voyage` → `gemini`)
  — no Qdrant shell/dashboard work needed. See
  `docs/EMBEDDING_CUTOVER_RUNBOOK.md` for the full how/verify/rollback.
- ⏳ REMAINING build work (no user needed; do next session):
  - **#12 true Nova streaming** — deferred (riskiest); do solo behind a flag with
    fallback to the current simulated path.
  - **#8 live-refresh cron** —🛑 BLOCKED/re-scoped after live schema inspection;
    NOT a blind re-aggregation (corrupts keystone). See
    `docs/BENCHMARK_REFRESH_FINDINGS_2026-06.md`. QStash infra
    (`/api/cron/run` + `CRON_SECRET`, S88) already exists; the missing piece is
    the *owned, correct* refresh action — needs a product decision from the user.
  - **#9 source-of-truth migration** — ✅ **parity-audit BUILT** (`ffdc024`):
    `supabase_parity.py` (`run_parity_audit()`, pure `diff_rows()`) +
    `scripts/parity_audit.py` (CLI) + ADMIN-gated `GET /api/admin/parity`
    + `tests/test_supabase_parity.py` (11 cases). **Prod-usage:** the HTTP
    endpoint needs `NOVA_ADMIN_KEY` set (see the wsgi auth fix below); the CLI
    `python3 scripts/parity_audit.py` works without it. READ-ONLY, additive —
    does NOT change serving (which is already Supabase-first). Flags `salary_data`/
    `compliance_rules`/`vendor_profiles` as `supabase_only` (empty JSON fallback
    = single point of failure); coverage-only for `knowledge_base`/
    `supply_repository`; excludes `cg_benchmarks`. **Remaining #9 = OPERATIONAL
    (needs prod):** hit `/api/admin/parity` with `ADMIN_API_KEY` against the live
    Supabase, then seed curated JSON fallbacks for the supabase_only domains.
  - **#15 external data MCPs** — deferred (opportunistic).
- **Residual follow-ups:**
  - ✅ eval-gate + pytest now run in CI (`4559149`, `a2b6ad0`); baseline committed.
  - ✅ **`call_llm_json` adoption COMPLETE** — `data_synthesizer.generate_ai_narratives`
    (`ffe94cd`) + **all 8 app.py LLM sites** (`a6c0fe3`, `185923d`, `553af96`):
    `_verify_plan_data`, `_copilot_suggest_brief`, `_analyze_compliance`,
    `_generate_post_campaign_summary`, `_audit_complianceguard`,
    `_generate_creative_ads`, `_generate_ab_test_with_claude` (router path).
    Each has tests (`tests/test_verify_plan_data.py`,
    `tests/test_app_llm_json_sites.py`, 16 cases) mocking `call_llm_json`;
    fallbacks (narrative-via-`raw`, template, `[]`/`skipped`) preserved. No
    hand-rolled `call_llm`+`json.loads` structured sites remain.
  - ⏳ adopt `plan_schema` at pipeline boundaries; MPG async-null-data dashboard
    bug + generation-progress UX; h1b KB refresh; (later) agentic pipeline behind
    the flag per `docs/AGENTIC_GENERATION_DESIGN.md`.
    **2026-07-25 re-check against origin/main (~6 weeks of shipped work since
    this list was written) -- re-verified against the code, not re-derived
    from memory:**
    - ⏳ **`plan_schema` adoption — still OPEN.** `plan_schema.py` (the typed
      Layer-1 pipeline contract) exists and is exercised by
      `tests/test_plan_schema.py`, but `grep -rln "import plan_schema\|from
      plan_schema"` across the repo returns only that one test file --
      `app.py`, `excel_v2.py`, `budget_engine.py`, and the rest of the
      `enrich -> synthesize -> budget -> generators` pipeline still pass
      plain dicts with no `plan_schema` boundary adoption. No status change.
    - ✅ **MPG async-null-data dashboard bug + generation-progress UX —
      CLOSED**, in `5ba77bf` (2026-07-15, *after* this doc's 2026-06-13
      timestamp): `_generation_jobs` was a per-process dict, so a
      `GET /api/jobs/<id>` poll landing on a gunicorn worker that didn't run
      the job returned a false "not found"/null-ish result mid-generation
      (exactly this bug) until the job fully completed and the Supabase
      fallback had bytes. Fixed with `_mirror_job()`, which snapshots
      whitelisted job fields (incl. `progress_pct`/`status_message`) to a
      shared slot-dir file on every progress transition, checked by
      `GET /api/jobs/<id>` before the Supabase fallback. Regression-tested
      in `tests/test_multiprocess_serving.py` (the commit message records
      it 404s on pre-fix base `259fa64f` even with the mirror file present
      on disk). The generation-progress UX itself was already live and
      wired end-to-end (`templates/partials/index/body_app_js.html`: a
      `setInterval` poll loop reads `pd.progress_pct` /
      `pd.status_message` from the job-status response and drives a real
      `#pageProgress` progress bar) -- the cross-worker fix is what makes
      that UX report real state instead of intermittently stalling/erroring
      depending on which worker served the poll.
    - ⏳ **h1b KB refresh — still OPEN, and more overdue than in June.**
      `data/h1b_salary_intelligence.json`'s own `_meta.last_updated` reads
      `"2025-Q1"`. `git log --follow -- data/h1b_salary_intelligence.json`
      shows exactly one commit, `8d006bf` (2026-03-26, file creation) --
      no refresh has ever landed, and no refresh script/mechanism exists
      anywhere in the repo (`find . -iname "*h1b*refresh*"` empty). The
      data is live in production: `h1b_data.py` is imported by
      `api_enrichment.py`, `data_synthesizer.py`, and `nova.py`. No status
      change; flagging as higher priority since the vintage gap has grown.

## 6. What's needed from the user
1. ~~Qdrant write credentials → run the Gemini-embeddings reindex~~ —
   **superseded**: #11 now self-migrates via startup indexing on deploy, no
   manual reindex or Qdrant creds needed. See `docs/EMBEDDING_CUTOVER_RUNBOOK.md`.
2. **#8 ownership decision** (NEW 2026-06-13) → which system owns the
   `cg_*` upload→`cg_benchmarks` transform (incl. its revenue/multiplier model +
   client mapping) and its refresh entrypoint. MPG can only *trigger* it, not
   recompute it. See `docs/BENCHMARK_REFRESH_FINDINGS_2026-06.md`.

Everything else proceeds without the user.

## 7. Architecture keystone (the big finding — see design doc)
Products use **none** of the `cg_*` Supabase warehouse: `cg_daily_raw` (520,771
rows daily perf) + `cg_benchmarks` (6,175 rows real Joveo cost/applies by
title/location/day; 2 clients, 72 titles, 412 locations, updated 2026-05-12).
Accessor `supabase_data.get_real_outcomes(title, location)` is built+tested; the
fleet wires it into the budget engine (additive) + a Nova tool. Active Supabase
project: `trpynqjatlhatxpzrvgt`. Inspect via the Supabase MCP.

## 8. Conventions / verification
- Brand: import hexes from `joveo_brand_2026.py`; placeholders `—`; light deck
  theme for deliverables+Nova, dark BY DESIGN for dashboards.
- Render harness: `python3 scripts/render_sample_outputs.py` (real budget engine →
  PPTX/Excel/scorecard); OG card: `python3 scripts/generate_og_card.py`.
- Local deps installed this session: pytest, python-pptx, openpyxl, matplotlib.
- Commit only when work is verified; branch is `main` (auto-deploys).
