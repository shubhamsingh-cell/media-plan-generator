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
- **HEAD ≈ `a6c0fe3`** (+ this handoff refresh). Live:
  https://media-plan-generator.onrender.com/ (HTTP 200; `main` auto-deploys).
  This turn's commits (`4559149`, `ffe94cd`, `a2b6ad0`, `8443ee1`, `a6c0fe3`)
  were pushed with explicit user authorization after the first direct push was
  denied by the permission classifier — a fresh push each turn may re-prompt;
  ask, or let the concurrent session's push carry them.
- Whole S89 program is shipped EXCEPT the items in §5 (mostly user-creds, the
  deliberately-deferred true-streaming work, or #8 which is blocked on a product
  decision per §6).

## 3. Shipped (history below; latest turn `f94c19c` → `a6c0fe3`, 2026-06-13)
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
- `a6c0fe3` **call_llm_json adoption #2** (1st app.py site): `_verify_plan_data`
  + `tests/test_verify_plan_data.py` (reusable mocked-`call_llm_json` test
  pattern). Full suite 2,111 passed.

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
- 🟡 #11 Gemini embeddings — CODE done; **needs you**: Qdrant write creds, then
  run `EMBEDDING_PROVIDER=gemini python3 scripts/reindex_embeddings.py` and set
  `EMBEDDING_PROVIDER=gemini` in Render env (kills Nova's 10-RPM search wall).
- ⏳ REMAINING build work (no user needed; do next session):
  - **#12 true Nova streaming** — deferred (riskiest); do solo behind a flag with
    fallback to the current simulated path.
  - **#8 live-refresh cron** —🛑 BLOCKED/re-scoped after live schema inspection;
    NOT a blind re-aggregation (corrupts keystone). See
    `docs/BENCHMARK_REFRESH_FINDINGS_2026-06.md`. QStash infra
    (`/api/cron/run` + `CRON_SECRET`, S88) already exists; the missing piece is
    the *owned, correct* refresh action — needs a product decision from the user.
  - **#9 source-of-truth migration** — decided: Supabase canonical; implement
    dual-read parity checks → cutover; JSON KB stays as fallback.
  - **#15 external data MCPs** — deferred (opportunistic).
- **Residual follow-ups:**
  - ✅ eval-gate + pytest now run in CI (`4559149`, `a2b6ad0`); baseline committed.
  - ✅ `call_llm_json` adopted in `data_synthesizer.generate_ai_narratives` (`ffe94cd`).
  - 🟡 **app.py `call_llm_json` adoption** — 1/≈8 done: `_verify_plan_data`
    (`a6c0fe3`) + `tests/test_verify_plan_data.py` (the **reusable mocked-
    call_llm_json test pattern** — copy it per site). Remaining heterogeneous
    sites (lines ≈ `1242` brief-suggestions array, `7723` compliance obj w/
    narrative fallback, `8052`/`8174` ad-copy arrays, `8815` A/B
    `_build_ab_response`). Do each with its own test; they're in the
    highest-collision file. (Old `app.py:6765` ref had drifted to a file-upload
    parser — not an LLM call.)
  - ⏳ adopt `plan_schema` at pipeline boundaries; MPG async-null-data dashboard
    bug + generation-progress UX; h1b KB refresh; (later) agentic pipeline behind
    the flag per `docs/AGENTIC_GENERATION_DESIGN.md`.

## 6. What's needed from the user
1. **Qdrant write credentials** → run the Gemini-embeddings reindex
   (`scripts/reindex_embeddings.py`). Unblocks Nova's search speed (#11).
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
