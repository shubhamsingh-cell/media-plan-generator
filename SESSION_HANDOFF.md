# Session Handoff — S89 "World-Class Upgrade" Program

**Last updated:** 2026-06-13
**Purpose:** Resume this multi-turn upgrade in a fresh session with zero loss.
**Read order on resume:** this file → `docs/ARCHITECTURE_UPGRADE_2026.md` → memory
(`mpg-nova-upgrade-backlog.md`) → the session task list (#1–#16, mirrored in §5).

---

## 1. DO THIS FIRST on resume (critical)

A background **build fleet** was running when this session ended. State to reconcile:

- **Run ID:** `wf_2fe3d455-f1c`  · **Task ID:** `wxdtykmrp`
- **Script:** `.../workflows/scripts/conclude-everything-wf_2fe3d455-f1c.js`
- **What it builds (9 agents, disjoint files):** pptx currency+fonts, pdf/scorecard
  P1, excel provenance, `plan_schema.py`, `mcp_server.py`, gemini embeddings +
  reindex, keystone budget wiring, nova keystone tool, eval gate + agentic design doc.

**Resume procedure:**
1. `git status --short` — see which fleet files landed (at handoff only
   `budget_engine.py` was written; fleet was mid-flight).
2. Check the fleet result: `TaskOutput(wxdtykmrp)` if same app session, else read
   `/private/tmp/.../tasks/wxdtykmrp.output`. If the workflow didn't finish, re-run:
   `Workflow({scriptPath: <above>, resumeFromRunId: "wf_2fe3d455-f1c"})` — completed
   agents return cached; only unfinished ones re-run.
3. **Integrate + verify (do NOT skip — 9 agents touched critical live files):**
   - `python3 -m py_compile` every changed `.py`.
   - `python3 -m pytest tests/ -q` (expect prior baseline ~636 + new unit tests).
   - `python3 scripts/render_sample_outputs.py` — PPTX/XLSX/scorecard must render;
     then `rm -rf tmp_render`.
   - Run an **adversarial pre-deploy review** on the combined diff (pattern proven
     this session — see commit history; a 3-agent correctness/safety/render review).
4. **Commit each unit cleanly** (one commit per work-unit, `S89 ...` prefix, with
   the `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>` trailer).
5. `git push origin main` (Render auto-deploys; verify HTTP 200 at the live URL).
6. Reconcile the task board (§5): mark the fleet's units completed.

> Always `git checkout -- data/audit_log.jsonl` before committing — tests write to it.

---

## 2. Current state
- **HEAD = `47c2a3a`**, synced with `origin/main`. Live: https://media-plan-generator.onrender.com/ (HTTP 200, auto-deploy on push from `main`, `autoDeploy: true`).
- **Uncommitted at handoff:** `budget_engine.py` (fleet, keystone wiring — verify before committing).
- Note: `e28c657` + `47c2a3a` (CI/Render best-effort) were added outside my work — already on main.

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
- ✅ #1 MPG frontend · ✅ #4 KB gap-fill · ✅ #5 structured-output primitive
- 🔵 (in fleet) #2 PPTX currency+fonts · #3 PDF/scorecard P1 · #6 typed schema ·
  #7 excel provenance · #10 MCP server · #11 gemini embeddings · #13 eval gate ·
  #14 agentic design · #16 keystone wiring (budget + nova tool)
- ⏳ pending after fleet: #12 true Nova streaming (deferred — riskiest, do solo
  behind a flag) · #8 live-refresh cron (build after #16 lands) · #9 source-of-truth
  migration (dual-read then cutover) · #15 external MCPs
- **Residual follow-ups noted in tasks:** adopt `call_llm_json` at the hand-rolled
  json.loads sites (data_synthesizer.py:4775, app.py:6765); h1b KB refresh; MPG
  async-null-data dashboard bug + generation-progress UX.

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
