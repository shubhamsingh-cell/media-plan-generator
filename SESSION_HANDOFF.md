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

---

## 2. Current state
- **HEAD = `f3ad719`**, clean, synced with `origin/main`. Live:
  https://media-plan-generator.onrender.com/ (HTTP 200; `main` auto-deploys).
- Whole S89 program is shipped EXCEPT the items in §5 (mostly user-creds or the
  deliberately-deferred true-streaming work).

## 3. Shipped this session (commits `f216464` → `7bb9a80`)
Excel numeric/totals/freeze overhaul · scorecard/PDF P0 image-404 fixes (real OG
card + logo) · PPTX brand chart palette · KB enrichment (4 files, cited 2026) ·
Nova chat-UI fixes (badge contrast, markdown-math, table scroll) · MPG frontend
brand colors + safe errors + field a11y · KB gap-fill (Reddit/Snap/YT/Pinterest,
healthcare pay, vintage hygiene) · Opus 4.8 bump (earlier) · architecture design
doc · **keystone accessor** `supabase_data.get_real_outcomes()` · **L3.1**
`llm_router.call_llm_json()`. All verified + live.

## 4. Decisions locked (user-delegated)
- **#11 embeddings:** GEMINI (free, key in stack) — not paid Voyage. Needs reindex.
- **#8 live refresh:** re-aggregate `cg_daily_raw` on a QStash cron (no ext. keys).
- **#9 source of truth:** Supabase canonical (benchmarks/supply/salary/compliance);
  JSON KB = curated/editorial + offline fallback. Migrate via dual-read parity.
- **#10 MCP server:** internal-only, API-key gated (`NOVA_MCP_API_KEY`).
- **#14 agentic generation:** design doc + feature-flag rollout after eval gate.
- **#15 external MCPs:** deferred until our own MCP ships.

## 5. Task board (session tasks #1–#16)
- ✅ DONE + LIVE: #1 MPG frontend · #2 PPTX currency+fonts · #3 PDF/scorecard P1 ·
  #4 KB gap-fill · #5 structured-output primitive · #6 typed schema · #7 excel
  provenance · #10 MCP server · #13 eval gate · #14 agentic DESIGN · #16 keystone
- 🟡 #11 Gemini embeddings — CODE done; **needs you**: Qdrant write creds, then
  run `EMBEDDING_PROVIDER=gemini python3 scripts/reindex_embeddings.py` and set
  `EMBEDDING_PROVIDER=gemini` in Render env (kills Nova's 10-RPM search wall).
- ⏳ REMAINING build work (no user needed; do next session):
  - **#12 true Nova streaming** — deferred (riskiest); do solo behind a flag with
    fallback to the current simulated path.
  - **#8 live-refresh cron** — decided: QStash endpoint re-aggregating
    `cg_daily_raw`→`cg_benchmarks` on a schedule (no external keys). Build it.
  - **#9 source-of-truth migration** — decided: Supabase canonical; implement
    dual-read parity checks → cutover; JSON KB stays as fallback.
  - **#15 external data MCPs** — deferred (opportunistic).
- **Residual follow-ups:** adopt `call_llm_json` at the hand-rolled json.loads
  sites (`data_synthesizer.py:4775`, `app.py:6765`); adopt `plan_schema` at
  pipeline boundaries; MPG async-null-data dashboard bug + generation-progress UX;
  h1b KB refresh; add the eval-gate job to `.github/workflows/ci.yml` + commit
  `evals/baseline_scores.json`; (later) implement the agentic pipeline behind the
  flag per `docs/AGENTIC_GENERATION_DESIGN.md`.

## 6. THE ONE thing needed from the user
**Qdrant write credentials** → run the Gemini-embeddings reindex
(`scripts/reindex_embeddings.py` once the fleet lands it). That unblocks Nova's
search speed (#11). Everything else proceeds without the user.

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
