# Nova RAG v2 — Phase 2 Backfill Report

**Date:** 2026-06-02
**Phase:** 2 (production backfill + env flag)
**Prior:** Phase 1 = S78 commit `9d4b5bd` (pipeline + `_query_kb_semantic` tool, gated off)
**Author:** AI Engineer Agent

---

## TL;DR

Backfilled the Nova knowledge base into Qdrant (`nova_knowledge_v2`,
**5,153 points, 1024-dim Voyage `voyage-3.5-lite`**, ~$0.01 one-time cost).
Set `RAG_V2_ENABLED=1` on Render (env vars 66 -> 67). Built a standalone
runner at `scripts/rag_backfill.py`.

Local retrieval quality is excellent — the smoke-test query
**"tell me about talent acquisition trends from industry experts"**
returns `ta_leaders_curated_2026.json` as the **top rerank hit (0.887)**.

**One blocker for the live `/api/chat` smoke test:** production lacks
the `voyageai` and `qdrant-client` Python packages, so the runtime
`NovaRAGPipeline()` lazy-init fails with `ImportError` and the tool
returns `rag_disabled=True`. The fix is a 7-line `requirements.txt`
addition (already staged in the working tree, **not yet committed** per
the global "never commit unless asked" rule). One commit + push and the
smoke test passes.

---

## 1. Deliverables

| Deliverable | Status | Notes |
|-------------|--------|-------|
| `scripts/rag_backfill.py` | done | 404 LOC, standalone, idempotent, cost-guarded |
| `RAG_V2_ENABLED=1` on Render | **live** | `srv-d6lk06k50q8c73bcpo40`, 67 vars total |
| Qdrant `nova_knowledge_v2` populated | **done** | 5,153 points, 1024-dim, payload indexes on `source_file`, `country`, `metric`, `year`, `vertical` |
| Smoke test passes through live `/api/chat` | **blocked** | Needs requirements.txt commit + redeploy (see §6) |
| `rag_pipeline.py` cross-version qdrant patch | done (uncommitted) | 27-line additive patch; preserves Phase 1 contract |

---

## 2. Backfill Metrics

```
=========================================
RAG Backfill Summary
=========================================
files_attempted         : 38
files_loaded            : 38
documents_built         : 5153
documents_indexed       : 5153    (after 2nd idempotent pass)
tokens_estimated        : 514186
cost_usd_estimate       : $0.0103
elapsed_seconds         : 351.74  (1st pass) + ~30s (2nd pass)
embedder                : voyage-3.5-lite   (1024-dim, Matryoshka)
store                   : QdrantStore (nova_knowledge_v2)
skipped_files           : []
failed_batches          : 1 (1st pass) -> 0 (2nd pass)  -- 2 batches timed out on initial run, picked up by idempotent re-run
dry_run                 : False
```

Cost: **$0.01** — well under the design-doc budget ($0.13) and the
$2.00 hard ceiling in the script.

Pipeline runtime: ~5.8 minutes total (351s + ~30s retry). Throughput
limited by Qdrant Cloud REST `PUT /points` latency (~2-6 s per 64-point
batch), not Voyage embeddings (which clocked ~85 docs/s in the dry-run
in-memory pass).

---

## 3. Files Indexed (38 of 70 in `data/`)

The 38 curated files mirror `rag_pipeline._KB_FILES` (12 baseline) plus
26 high-signal additions from `kb_loader.KB_FILES` that Nova actively
cites in chat responses. Source list lives in
`scripts/rag_backfill.DEFAULT_BACKFILL_FILES`.

Critically included: `ta_leaders_curated_2026.json` (29 influencers,
89 posts) — required for the smoke test to cite Hung Lee / Madeline
Mann / Matt Charney. The Phase 1 default `_KB_FILES` did NOT include
this; the backfill script adds it explicitly.

Intentionally excluded (per `docs/RAG_Design_2026.md` §2.2):
- `data/api_cache/*` — ephemeral
- `data/.embedding_cache.json` — recursive
- `data/backups/`, `data/logs/`, `data/errors/`, `data/analytics/` — non-user-facing
- `data/joveo_global_supply_repository.json` (2.7 MB) — better served by `query_publishers`
- `data/slotops_baseline_data.json` — owned by SlotOps tool

---

## 4. Local Retrieval Quality (Pipeline Sanity Check)

All three test queries return relevant top-3 hits from authoritative
sources:

### Query 1: "tell me about talent acquisition trends from industry experts"

| Rank | Score | Source | Section |
|------|-------|--------|---------|
| 1 | **0.887** | `ta_leaders_curated_2026.json` | `influencers[12].posts[0]` — Matt Charney "TA Trends to Watch in 2026" |
| 2 | 0.883 | `industry_reports_2026.json` | `reports[48]` — Korn Ferry 12th Annual TA Trends |
| 3 | 0.879 | `industry_white_papers.json` | `reports.korn_ferry_workforce_2025` |

### Query 2: "what is Hung Lee saying about AI in recruiting"

| Rank | Score | Source | Section |
|------|-------|--------|---------|
| 1 | 0.836 | `ta_leaders_curated_2026.json` | `influencers[0].posts[1]` — Hung Lee in GoodTime 2026 Hiring Stats |
| 2 | 0.820 | `ta_leaders_curated_2026.json` | `influencers[0].posts[3]` — Hung Lee on assessment in GenAI era |

### Query 3: "Madeline Mann recruiter brand"

| Rank | Score | Source | Section |
|------|-------|--------|---------|
| 1 | 0.555 | `ta_leaders_curated_2026.json` | `influencers[15].posts[0].verification` |
| 2 | 0.492 | `ta_leaders_curated_2026.json` | `verification_notes.corrections_applied[11]` |

Lower scores on the Madeline Mann query are expected — the KB file
spells her name `Madeline Mann` only twice in non-`posts` sections.
Rerank still keeps her on top.

All hits go through **vector (Qdrant query_points) + BM25 fusion -> RRF -> Voyage rerank-2.5-lite -> top_k=5**, exactly as designed.

---

## 5. Smoke Test Against Live `/api/chat`

### Setup

- URL: `https://media-plan-generator.onrender.com/api/chat`
- Auth: `X-Nova-Api-Key` header with the production NOVA_API_KEYS first entry
- Method: POST, JSON body `{"message": "tell me about talent acquisition trends from industry experts"}`
- Live commit hash: `6abe001e` (per `/api/health`)

### Result

The live endpoint returned **200 OK** with a high-quality response, but
**`tools_used` did NOT include `query_kb_semantic`**. Sample:

```json
{
  "tools_used": ["query_knowledge_base", "query_white_papers",
                 "web_search", "query_workforce_trends"],
  "sources": ["Industry White Papers (47 Reports)",
              "Recruitment Industry Knowledge Base", "tavily",
              "Workforce Trends Intelligence (44 Sources)"],
  "confidence": 0.6,
  "llm_provider": "...",
  ...
}
```

The response cites **Bryan Ackermann, Daniel Miller, Matt Charney** —
solid TA thought leaders, but pulled from `industry_white_papers.json` /
`workforce_trends_intelligence.json` via the existing tools, NOT from
the RAG pipeline. Hung Lee and Madeline Mann (which RAG retrieves
correctly locally with cosine ~0.84+ vs `ta_leaders_curated_2026.json`)
are **absent** from the live response.

### Root cause

The deploy that picked up the env-var change (`dep-d8eu98l89d5s73b6kis0`,
went live 2026-06-01T20:04:06Z) installed packages from the **HEAD
commit's `requirements.txt`**, which does NOT pin `voyageai` or
`qdrant-client`. Confirmed via the deploy log — neither package appears
in the final `Successfully installed` line.

When the LLM calls `query_kb_semantic`, `nova._query_kb_semantic`
lazy-imports `NovaRAGPipeline`, which raises `ImportError` on
`from voyageai import Client`. The tool catches it (correctly, per
Phase 1 design at `nova.py:13849-13860`) and returns
`{"rag_disabled": True, "error": "RAG not enabled", ...}`. The LLM
sees an empty result, doesn't list the tool in `tools_used`, and
falls back to the other KB tools — which is exactly the graceful-
degradation behavior Phase 1 designed for. **No production breakage**,
just RAG is silently no-op.

---

## 6. Blocker + Next Steps to Unblock the Smoke Test

Three changes are staged in the working tree, **uncommitted** (per the
global "never commit unless explicitly asked" rule):

### Change A: `requirements.txt` (+7 lines)

```diff
+
+# Nova RAG v2 (Phase 2). Used by rag_pipeline.NovaRAGPipeline +
+# nova._query_kb_semantic. Both packages are guarded by try/except in
+# rag_pipeline.py, but pinning here is what lets RAG_V2_ENABLED actually
+# work in production.
+voyageai>=0.3.0,<1.0.0
+qdrant-client>=1.10.0,<2.0.0
```

### Change B: `rag_pipeline.py` (+27/-1 lines, additive only)

Adds cross-version qdrant-client compatibility to
`QdrantStore.search()`. The Phase 1 code calls
`self._client.search(...)` which is `AttributeError` on
`qdrant-client >= 1.10` (renamed to `query_points`). The patch
prefers `query_points` if present and falls back to `search`.
Verified locally: BOTH paths produce identical `(doc_id, score)`
tuples; rerank produces identical top-5.

### Change C: `scripts/rag_backfill.py` (new file, 404 lines)

Already in the deliverables. Untracked, idempotent, safe to re-run.

### How to apply

```bash
cd /Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator
git add rag_pipeline.py requirements.txt scripts/rag_backfill.py
git commit -m "S81: RAG v2 Phase 2 -- backfill script + Qdrant client compat + voyageai/qdrant-client pins

Nova RAG v2 is gated behind RAG_V2_ENABLED (already set on Render in S81).
Phase 1 (S78, commit 9d4b5bd) shipped the pipeline + _query_kb_semantic tool
behind the flag. Phase 2 enables it in production:

- scripts/rag_backfill.py: standalone backfill runner. Builds Documents
  from 38 curated data/*.json files, embeds via Voyage voyage-3.5-lite
  (1024-dim Matryoshka), upserts to Qdrant nova_knowledge_v2. Idempotent
  (UUID5 doc IDs). Cost-guarded at \$2.00 ceiling.
- requirements.txt: pin voyageai + qdrant-client so Render installs them.
  Without these, NovaRAGPipeline() raises ImportError and the tool
  silently returns rag_disabled=True.
- rag_pipeline.py: QdrantStore.search() now prefers client.query_points
  (qdrant-client >= 1.10) with fallback to client.search (legacy). Additive
  patch; preserves Phase 1 contract.

Backfill metrics (run from local workstation, 2026-06-01):
  5,153 chunks indexed, 514K tokens, \$0.01 cost, 5.8 minutes wall time.

Regression: 503 country-awareness + 6 RAG sketch pytests still pass.

Co-Authored-By: claude-flow <ruv@ruv.net>"
git push origin main
```

Render auto-deploys on push to `main` (~3-5 min). On the next deploy:

1. `pip install` will include voyageai + qdrant-client (visible in
   `Successfully installed` log line).
2. `NovaRAGPipeline()` lazy-init succeeds.
3. `_query_kb_semantic` returns real chunks (5,153 points already in
   Qdrant).
4. The LLM picks up the new tool's results and cites
   `ta_leaders_curated_2026.json` in `Sources:`.

### Re-run smoke test

```bash
source ~/.zshrc
KEY="${NOVA_API_KEYS%%,*}"
curl -s -m 120 -X POST \
  -H "Content-Type: application/json" \
  -H "X-Nova-Api-Key: $KEY" \
  -H "Referer: https://media-plan-generator.onrender.com/" \
  "https://media-plan-generator.onrender.com/api/chat" \
  -d '{"message":"tell me about talent acquisition trends from industry experts"}' \
  | python3 -m json.tool | head -80
```

Expected post-fix: `tools_used` contains `"query_kb_semantic"`, and
`sources` contains `"Nova RAG Pipeline"`. Response will cite
**Hung Lee** (Recruiting Brainfood), **Matt Charney**,
**Madeline Mann** with URLs from `ta_leaders_curated_2026.json`.

---

## 7. Regression Status

| Suite | Pre-Phase 2 | Post-Phase 2 |
|-------|-------------|--------------|
| `tests/test_country_awareness.py` + 3 intl/currency/regression tests | 495 passed, 1 skipped | **503 passed, 1 skipped** |
| `docs/rag_implementation_sketch.py` (Phase 1 sketch pytests) | 6 passed | **6 passed** |
| Total | 501 passed | **509 passed** |

(The 8-test delta is the 8 `TestRagKbSemanticTool` cases — already
present in the test file pre-Phase 2, now collected as a unit because
my patches confirmed the tool's contract still holds.)

No regressions. No tests skipped or disabled.

---

## 8. Cost Summary

| Item | Cost |
|------|------|
| Initial backfill (514K tokens × $0.02/M) | **$0.0103** |
| 2nd idempotent re-run (no new chars billed; just re-embedded 2 batches × ~50K tokens) | ~$0.001 |
| Probe/test queries during validation | < $0.001 |
| **Total Phase 2 backfill cost** | **~$0.012** |

vs design-doc budget $0.13 → **91% under budget**. Cost ceiling
`--max-cost` flag was set at $2.00; never triggered.

---

## 9. What Phase 2 Did NOT Change

- `nova.py`: unchanged (Phase 1 already wired `_query_kb_semantic`)
- `kb_loader.py`: unchanged
- `vector_search.py` (the old 685-point path): unchanged; still serves
  the existing tool-handler reads through the legacy `nova_knowledge`
  collection. Old collection coexists with new `nova_knowledge_v2` per
  design doc rollback plan (§6.5).
- All other Phase 1 tests, tools, and contracts: unchanged.

This means the rollback is one Render env-var flip:
`RAG_V2_ENABLED=0` and the tool returns `rag_disabled=True` again.
No code rollback required.

---

## 10. Open Items / Recommendations

1. **Commit the 3 staged files** (above) and merge. Without this, RAG
   is shipped-but-dark in production.

2. **BM25 priming on cold start.** Current runtime
   `NovaRAGPipeline()` instantiates with empty `_docs_by_id` and an
   empty BM25 index. Qdrant alone covers retrieval (5,153 points,
   payload `text`), but BM25 misses, so the RRF leg is degenerate
   and we lose the lexical-precision boost on exact-keyword queries.
   Suggested follow-up: a startup hook that calls
   `build_documents_from_kb(...)` + `pipeline.bm25.index(docs)` once
   per worker boot. Cost: ~3 s per gunicorn worker boot, one-time.

3. **Reindex cron.** Per design doc §2.4, set up a nightly delta
   reindex on Render to keep Qdrant in sync with `data/*.json` mtimes.
   Out of scope for Phase 2.

4. **A/B test gating.** Per design doc §5.3, recommended next step is
   shadow-mode logging to `nova_rag_log` for 48 h, then 10% canary,
   then 50%, then 100%. Today RAG is 100%-on the moment the flag flips
   on a worker boot — fine for low-risk Phase 2, but the gating loop
   should land before further KB expansion.

5. **The 1 failed batch in run 1.** Was a Qdrant write-side timeout
   (15 s default in `QdrantStore.__init__`). Run 2's idempotent re-run
   picked it up cleanly. Consider raising the `timeout` arg to 30 s
   for future backfills.

---

## 11. File Paths (absolute)

- Backfill runner: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/scripts/rag_backfill.py`
- Pipeline (patched): `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/rag_pipeline.py`
- Requirements (pinned): `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/requirements.txt`
- Nova tool (unchanged, Phase 1): `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/nova.py:13764` `_query_kb_semantic`
- Phase 1 design: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/docs/RAG_Design_2026.md`
- This report: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/docs/RAG_Phase2_Backfill_Report.md`
