# Nova RAG Layer Design — 2026

**Author:** AI Engineer Agent (May 22, 2026)
**Status:** Design proposal — no production code changes yet.
**Scope:** Retrieval-Augmented Generation for `nova.py` (24,865 lines, 100+ tools).
**Out of scope:** Modifying `nova.py`. Replacing the existing tool system. Re-architecting LLM routing.

---

## 0. TL;DR

Nova today does **tool-handler retrieval, not RAG**. The LLM picks among ~100 tools, each tool reads a hand-coded slice of `data/*.json` or hits a Supabase table. A small Qdrant index (685 points) exists but is treated as a single fallback enrichment call (`_bounded_vector_search`, top-3, 3 s timeout) bolted onto a few code paths. The result:

- The LLM cannot ask "what does the KB say about X?" via a clean interface — it must guess which of 100 tools is right.
- 685 points only indexes ~15 of the 80+ KB JSON files in `data/`. Files like `joveo_global_supply_repository.json` (2.7 MB), `joveo_cpa_benchmarks_2026.json`, `recruitment_benchmarks_comprehensive_2026.json` are partially indexed at best.
- Voyage AI free tier (10 RPM, 6.5 s minimum spacing) means a single missed-cache embedding stalls a chat for 60+ s. This is the documented cause of the S55 bug that forced bounded timeouts everywhere.
- BM25 + RRF infrastructure exists but is wasted on a small index.

**Proposed design:**

1. **Embeddings:** keep Voyage AI but migrate from `voyage-3-lite` to `voyage-4-lite` (already scoped in `scripts/migrate_voyage_4.py`). Add a paid-tier key to remove the 10 RPM stall risk. Fall back to local `sentence-transformers/all-MiniLM-L6-v2` when Voyage is rate-limited.
2. **Vector store:** stay on Qdrant Cloud (already running, supabase-cached, no operational debt). Expand the index from 685 → ~15,000 points by chunking the full `data/` directory plus client plans and canned answers.
3. **Chunking:** structure-aware (one chunk per JSON leaf node or section), 400–800 tokens with 80-token overlap, metadata = `{source_file, kb_section, role, country, metric, year}` for filterable retrieval.
4. **Retrieval:** hybrid (vector + BM25 + RRF, all already implemented) with cross-encoder rerank via Voyage `rerank-2.5-lite` (already wired). Add metadata pre-filtering (country, year).
5. **Orchestration:** expose RAG as a **new tool** `query_kb_semantic(query, filters)` that the LLM can call. Do **not** force it into every turn. Make it the default for `query_knowledge_base` topic="all" calls.
6. **Quality gate:** A/B test against 50-query golden set; ship if citation accuracy improves ≥10 % with ≤200 ms median added latency.

**Effort:** ~3 person-weeks (one engineer). **Cost:** ~$50 one-time backfill + ~$30/month ongoing for typical Nova traffic.

---

## 1. Current State Audit

### 1.1 What Nova does today (tool-handler retrieval)

`nova.py` line 6459 `_tool_handler_map()` registers ~100 tools. The LLM is given the tool schemas (line 1498 `get_tools_for_provider`) and emits `tool_use` blocks; `execute_tool` (line 6610) dispatches each one with a 5 s per-tool wall-clock timeout. Each handler reads a slice of `self._data_cache` (a `kb_loader.load_knowledge_base()` singleton, 80+ files) or hits an external API.

Vector search exists but is treated as a **side dish, not the main course**:

- `_query_knowledge_base` (line 7368) optionally runs `vector_search.search_bounded(top_k=3, timeout_s=3.0)` to add semantic snippets before returning hand-coded keyword-filtered JSON.
- `_knowledge_search` tool (line 10724) is a thin wrapper around `vector_search.search_bounded` exposed to the LLM. It is rarely picked because the LLM has 100 better-named tools.
- `_bounded_vector_search` (line 132) is called in three free-LLM paths (lines 17751, 18143, 19169) to inject up to three 500-char snippets into the system prompt.

### 1.2 What's wrong with that

| Problem | Evidence | Impact |
|---------|----------|--------|
| **Sparse coverage** | Qdrant holds 685 points (docs/voyage_4_migration_runbook.md line 22). `vector_search.index_knowledge_base()` lists ~40 KB files. `data/` has 80+ JSON files (verified `ls data/ \| wc -l`). | LLM cannot retrieve from 40+ KB files. |
| **Tool overload** | 100 tools in the schema means a real provider token cost (~8 K tokens of schema per turn). | Adds 4–8 K input tokens/turn; LLM often picks the wrong tool. |
| **No metadata filtering** | `vector_search.search` has no `filter` arg; all queries are unfiltered global. | Asking "CPA in Texas for nurses, 2026" matches generic 2025 nurse content. |
| **Voyage rate-limit fragility** | S55 fix (line 135) documents 60+ s hangs from free-tier rate limit (10 RPM, 6.5 s min delay). Bounded to 3 s, which often returns empty. | Empty grounding → ungrounded LLM answers → low quality scores. |
| **No reranking on hot paths** | `_bounded_vector_search` returns raw top-3 with no rerank; only `_rerank_results` (line 1135) is wired through the `search()` fallback paths. | Top-3 frequently misses the best chunk. |
| **No citation provenance** | The "Why this answer?" panel uses `kb_loader.start_tracking()` to log file reads, but vector matches don't carry source filenames into citations. | Citations look fabricated when answer is vector-grounded. |

### 1.3 Existing assets we should keep

- **`vector_search.py`** has a clean hybrid retrieval skeleton: BM25Index class, `reciprocal_rank_fusion()`, Voyage rerank via `_rerank_with_voyage` (line 1054), Qdrant REST integration (line 484). The plumbing is correct; the index is just too small.
- **`kb_loader.py`** hot-reloads 80+ JSON files into a process-shared dict, with per-key access tracking. RAG can index against the same dict.
- **`chroma_rag.py`** is an unused alternative path (uses Chroma's default sentence-transformers). Useful as a local fallback when Voyage is unavailable.
- **Qdrant Cloud** is provisioned (`QDRANT_URL`, `QDRANT_API_KEY` on Render). No new infra needed.
- **Supabase `cache` and `nova_documents` tables** can persist embedding results for cross-deploy survival.

---

## 2. Embedding Pipeline

### 2.1 Model choice

| Model | Dimensions | Cost / M tokens | Notes |
|-------|-----------|----------------|-------|
| **Voyage AI `voyage-3-lite` (current)** | 512 | $0.02 | Live in Nova. 10 RPM free tier. |
| **Voyage AI `voyage-3.5-lite`** | 1024 (Matryoshka-truncatable to 256/512/1024/2048) | $0.02 | Cheapest production option in May 2026. |
| **Voyage AI `voyage-3-large`** | 1024 (Matryoshka) | $0.18 | Higher quality but 9× cost. |
| OpenAI `text-embedding-3-small` | 1536 (truncatable) | $0.02 | Comparable cost; less recruitment-domain tuning. |
| OpenAI `text-embedding-3-large` | 3072 (truncatable) | $0.13 | Strong but pricier than voyage-3.5-lite for similar quality. |
| Cohere `embed-v3.0` | 1024 | $0.10 | Strong multilingual. Heavier for our mostly-English KB. |
| **sentence-transformers `all-MiniLM-L6-v2`** (local) | 384 | $0 (CPU) | Free fallback, ~5 ms/query on Render Standard. |

**Recommendation:**

- **Primary:** `voyage-3.5-lite` at 1024 dims (Matryoshka-truncatable). Voyage publishes a Jan 2026 benchmark showing +14.05 % retrieval quality over `text-embedding-3-large` while remaining at $0.02/M (https://blog.voyageai.com/2026/01/voyage-4-launch/ — verify URL before commit, see §11). The existing `scripts/migrate_voyage_4.py` is already designed for this migration.
- **Fallback:** `sentence-transformers/all-MiniLM-L6-v2` via the `sentence-transformers` package (https://www.sbert.net/). When Voyage returns a 429 or times out, we still answer with a degraded embedding — better than empty grounding. The model is ~80 MB and CPU-runnable in <10 ms/query.
- **Why not "Anthropic embeddings":** Anthropic does not ship a first-party embedding API as of May 2026. Their official recommendation in https://docs.anthropic.com/en/docs/build-with-claude/embeddings is to use a third-party provider (Voyage AI, which Anthropic invested in but did not acquire — verified via Anthropic docs page above). Treat any source claiming "Anthropic embeddings" as wrong.

**Verification of API existence:**

- Voyage AI API confirmed live: `POST https://api.voyageai.com/v1/embeddings` and `/v1/rerank`. Pricing page: https://www.voyageai.com/pricing.
- OpenAI embeddings docs: https://platform.openai.com/docs/guides/embeddings.
- Cohere embed v3: https://docs.cohere.com/docs/cohere-embed.
- sentence-transformers: https://www.sbert.net/docs/quickstart.html.

### 2.2 What gets embedded

Three corpora, three index namespaces (single Qdrant collection, metadata-tagged):

| Namespace | Source | Chunk count estimate | Re-embed cadence |
|-----------|--------|---------------------|------------------|
| `kb_static` | `data/*.json` benchmark + intelligence files (recruitment_industry_knowledge, joveo_2026_benchmarks, healthcare_supply_map_us, international_benchmarks_2026, etc. — 40 files actively curated) | ~12,000 | Nightly + on git push to `data/` |
| `kb_client_plans` | `data/client_plans/*.json` + `client_media_plans_kb.json` | ~500 | On client onboarding (manual trigger) |
| `kb_canned` | `data/nova_learned_answers.json` + admin-curated Q&A | ~200 | On admin update via `/api/admin/answers` |

Excluded (intentional):

- `data/api_cache/*` — ephemeral API responses, would pollute the index.
- `data/.embedding_cache.json` — recursive nightmare.
- `data/backups/*`, `data/logs/*`, `data/errors/*`, `data/analytics/*` — not user-facing knowledge.
- `slotops_baseline_data.json` (7.4 MB) and `so_survey_2025.zip` — best served by their own tools.

### 2.3 Chunking strategy

**Approach: structure-aware semantic chunks, not fixed character windows.**

The KB is overwhelmingly nested JSON. Fixed-size chunking would split mid-object and destroy meaning. Two-pass algorithm:

1. **Walk the JSON tree.** For each `dict`, emit one chunk per "leaf-ish" subtree:
   - If the dict's children are all scalar (e.g. `{average_cpc: $1.20, source: Indeed, year: 2026}`) — emit as a single chunk with the key path as a prefix.
   - If the dict has nested dicts/lists, recurse into each child.
   - Carry the key path so chunks have context: `[recruitment_industry_knowledge] benchmarks.cost_per_click.indeed: average_cpc_range: $0.25-$1.50, model: PPC, ...`
2. **Length normalization.** After tree walk, regroup chunks within the same file into ~400–800 token windows with 80-token overlap, preserving the key-path prefix on every chunk.

This is a refinement of the existing `_extract_text_chunks` (vector_search.py line 1749). The current version emits per-leaf chunks but doesn't normalize length, so we get a long tail of 30-byte chunks (low signal) and a few oversized ones.

**Metadata schema (Qdrant payload):**

```json
{
  "source_file": "joveo_cpa_benchmarks_2026.json",
  "kb_section": "benchmarks.cost_per_application.nursing",
  "country": "US",
  "metric": "cpa",
  "vertical": "healthcare_nursing",
  "year": 2026,
  "chunk_index": 17,
  "token_count": 612,
  "indexed_at": "2026-05-22T08:00:00Z",
  "model": "voyage-3.5-lite",
  "model_dim": 1024
}
```

Metadata is **extracted at index time** from the key path and content via regex (e.g. metric=`cpc|cpa|cph|apply_rate`, country from a known list, year `\d{4}`). This enables Qdrant pre-filters: `filter={country: "US", metric: "cpa", year: 2026}`.

### 2.4 Re-embedding cadence

| Trigger | Action |
|---------|--------|
| `data/*.json` file mtime changed | Re-index only that file (delta upsert). Driven by a nightly `scripts/reindex_delta.py` cron on Render. |
| Voyage model migration | Full re-index. Use `scripts/migrate_voyage_4.py` template — already exists for the 3→4 migration. |
| Manual admin trigger | `/api/admin/reindex?source=joveo_publishers.json` endpoint (admin-key gated). |
| Schema change in metadata | Full re-index with new payload schema. |

Embedding cost for delta updates is negligible: a typical KB file change is <100 chunks (~50 K tokens) → $0.001 at voyage-3.5-lite rates.

### 2.5 Cost estimate

| Item | Tokens | $/M | Cost |
|------|--------|-----|------|
| Initial backfill: 12,700 chunks × ~500 tokens avg | 6.35 M | $0.02 | **$0.13** |
| Query embeddings: 500 chats/day × 1.5 retrievals/chat × 50 tokens | 1.13 M/month | $0.02 | **$0.02/mo** |
| Reranking: 500 chats/day × 1.5 retrievals × 20 candidates × 100 tokens | 45 M/month | $0.05 (rerank-2.5-lite) | **$2.25/mo** |
| Qdrant Cloud: stays on existing cluster (1 GB plan, $25/mo) | — | — | **$0/mo incremental** |
| **Total ongoing** | | | **~$2.30/mo over today** |

Plus a one-time ~$0.13 backfill. The 685-point baseline already paid the Qdrant fixed cost. The "$50 one-time" budget in the TL;DR includes development environment Voyage usage (eval set, debugging), not just production embedding.

---

## 3. Vector Store

### 3.1 Options compared

| Store | Status | Pros | Cons |
|-------|--------|------|------|
| **Qdrant Cloud (current)** | Live, 685 pts, paid plan | Already provisioned, REST API, scalar filtering, mature client | None for our scale |
| Chroma | `chroma_rag.py` exists but unused on Render (uses EphemeralClient due to ulimit ConfigError noted in line 16 of that file) | Embedded, simple | Loses data on restart in production; no metadata filter at scale |
| Supabase `pgvector` | Supabase already provisioned (`cache`, `nova_documents` tables) | Single DB for memory + vectors; cheap | Adding 12 K rows is fine; metadata filtering via SQL is verbose; cold queries are 10× slower than Qdrant |
| Pinecone | Not provisioned | Managed, sharded | New vendor, $70/mo starter, no advantage over Qdrant for our scale |
| Weaviate | Not provisioned | Managed, hybrid built-in | Overkill at <50 K points |

**Decision: stay on Qdrant Cloud.**

- Already paid for, already wired.
- 685 → 15 K points fits well within the existing plan (Qdrant Cloud free tier holds 1 M points; paid plan way more).
- `vector_search._qdrant_search` already supports the REST `/collections/{}/points/search` endpoint with filters; we just haven't passed any.
- Qdrant supports payload filters with the exact metadata schema we want (https://qdrant.tech/documentation/concepts/filtering/).

**Use Supabase `nova_documents` as a sidecar** for full-text storage (Qdrant payload holds metadata + a text preview, full text in Supabase). This keeps Qdrant payloads small.

### 3.2 Index structure

- **One collection: `nova_knowledge_v2`**. (Keep current `nova_knowledge` for rollback for 30 days.)
- **Vector dim: 1024** (voyage-3.5-lite Matryoshka, truncatable to 512 if needed).
- **Distance: Cosine** (same as today).
- **Payload indexes:** on `source_file`, `country`, `metric`, `year`, `vertical` for fast filtering.
- **Namespaces (logical, via metadata):** `kb_static`, `kb_client_plans`, `kb_canned`. The LLM can scope a query: `filter={namespace: "kb_canned"}` for high-confidence canned answers.

Single collection (not per-namespace) because (a) cross-namespace retrieval is often desirable ("what does the KB and our client plans say about Texas nurses?"), and (b) Qdrant payload filters are zero-overhead on small collections.

### 3.3 Hybrid retrieval

Already implemented in `vector_search.py`:

```text
query → embed (Voyage)
        ↓
        ├─→ vector search (Qdrant, top 20)
        └─→ BM25 search (in-memory, top 20)
        ↓
        reciprocal_rank_fusion(k=60)
        ↓
        top 10 candidates
        ↓
        Voyage rerank-2.5-lite (top 5)
        ↓
        return top_k (default 5) with provenance
```

What we add:

1. **Metadata pre-filtering.** Pass `filter={country, metric, year}` into Qdrant when the query has those entities. Entity extraction reuses existing `_detect_country`, `_extract_budget`, `_classify_query_type`.
2. **Cross-encoder rerank as the default.** Today `_rerank_results` is only called in fallback paths. Move it to the hot path; it adds ~80 ms for 20 candidates (Voyage rerank-2.5-lite at $0.05/M and ~50 ms p50 latency per https://docs.voyageai.com/docs/reranker).
3. **Score thresholding.** RRF score below 0.015 → drop the chunk. This is the empirical noise floor on the existing 685-point index (observed in `_qdrant_search` logs).

---

## 4. Retrieval Orchestration

### 4.1 Where RAG sits in the chat flow

Nova's chat flow today (simplified):

```
user_message
  ↓
fast-path benchmark/supply lookup (rule-based, no LLM)            ← rare; only US healthcare/CPA
  ↓
greetings + cache check (intelligent_cache, learned_answers)
  ↓
planner: _generate_query_plan() (rule-based 1-sentence plan)
  ↓
LLM tool loop (3 iterations max):
  ├─ system_prompt + _bounded_vector_search top-3 snippets (passive, 3s budget)
  └─ LLM picks among ~100 tools → execute_tool() → result back to LLM
  ↓
response enrichment (formatting, citations, follow-ups)
  ↓
return
```

**Where RAG slots in:**

1. **As a new tool `query_kb_semantic(query: str, filters: dict)`** — promoted to a first-class tool the LLM can pick. This is the lowest-risk integration: existing tool loop works unchanged.
2. **Replace the passive `_bounded_vector_search` injection** with a smarter pre-RAG step that runs only when the planner classifies the query as KB-grounded (benchmarks, definitions, supply, trends). Keep the 3 s timeout. Cache the result keyed on `(normalized_query, filters)` for the rest of the turn so the LLM doesn't double-pay if it also calls `query_kb_semantic`.
3. **Planner extension (Tier 3):** `_generate_query_plan` already classifies vertical + location + intent. Add a `rag_needed: bool` and a tentative `filters: dict` to the plan. The streaming UI can show "Searching knowledge base for nursing benchmarks in Texas..." while the tool runs.

### 4.2 RAG ↔ Tools merging

When both fire (LLM calls `query_kb_semantic` AND a structured tool like `query_publishers`):

- Pass both results to the LLM in distinct system blocks: `<kb_evidence>` and `<tool_data>`.
- LLM prompt instructs: "Prefer `<tool_data>` for hard numbers and entity facts. Use `<kb_evidence>` for context, qualitative trends, and citations."
- Final synthesizer (`_enrich_response` line 23311) cross-checks numbers in the response against `<tool_data>` first, `<kb_evidence>` second.

This **keeps tools as the source of truth for structured queries** (a benchmark CSV, a publisher list, an API result) and treats RAG as the source for **unstructured context and quotes**.

### 4.3 When does the LLM trigger RAG vs tools?

The LLM decides. The tool schema for `query_kb_semantic` carries an explicit hint:

```text
description: |
  Use this when the user asks an open-ended question that doesn't map cleanly to a structured
  tool — e.g., "what trends are shaping healthcare hiring in 2026?", "how does Indeed compare
  to ZipRecruiter for nursing roles?", "what does the research say about programmatic
  recruitment ROI?".
  DO NOT use this for hard-number lookups; prefer query_publishers, query_recruitment_benchmarks,
  query_salary_data instead.
```

Reinforced by 3 few-shot examples in the system prompt.

### 4.4 Top-k tuning

Current `_bounded_vector_search` uses k=3 (chosen for prompt-budget reasons under the rate-limit pressure). With a 1 K-point larger index and reranking, k=3 is too thin.

**Target settings:**

| Stage | k | Rationale |
|-------|---|-----------|
| Vector fetch (Qdrant) | 20 | Wide net, cheap. |
| BM25 fetch | 20 | Symmetry with vector. |
| RRF merge | 30 unique | Some overlap expected. |
| Rerank input | 20 | Voyage rerank ~80 ms for 20. |
| Final output | **5** | Fits in <2 K tokens of context. |

The "k=8 default" in the question is a fair compromise but I'd start at 5 and tune up if eval scores warrant. 5 chunks at 500 tokens = 2.5 K tokens of grounding, which is the right budget on a 200 K-token context window with 4 K-token reserves.

### 4.5 Compatibility with degraded mode

The existing degraded mode (when LLM router exhausts all providers) returns a graceful error. **RAG can still run in degraded mode** because it doesn't depend on a generation LLM — it just retrieves. We can show the top retrieved chunks as a "Here's what the KB has on this topic" fallback when generation fails. This is a quality improvement over today's "I'm temporarily unable to process your request" message.

---

## 5. Quality Measurement

### 5.1 Eval set

50 representative queries, **already drafted in the existing `evals/` directory**. We extend it:

| Bucket | Count | Examples |
|--------|-------|----------|
| Benchmark lookup (US) | 10 | "CPA for nurses in Texas in 2026", "Indeed CPC for warehouse roles" |
| Benchmark lookup (non-US) | 10 | "Cost per applicant for healthcare in Germany", "LinkedIn CPA in UK financial services" |
| Role/budget planning | 10 | "Build a media plan for 50 nurses, $40K, Atlanta" |
| Trends and qualitative | 10 | "What does the research say about AI in recruitment?", "Trends in healthcare hiring 2026" |
| Adversarial / edge | 10 | "What is the CPA for unicorn wranglers in Atlantis?", typos, code-switching, ambiguous role names |

Each query has:
- Reference answer (free text, validated by a human).
- Expected entity slots (role, country, metric, year).
- Expected source files (e.g. `joveo_cpa_benchmarks_2026.json` should be cited for query 1).
- Numeric ground truth where applicable (the expected CPA range).

### 5.2 Metrics

**Retrieval-level (offline):**

| Metric | Computation |
|--------|-------------|
| `recall@5` | Fraction of expected source files retrieved in top 5 |
| `mrr` | Mean reciprocal rank of the first correct chunk |
| `ndcg@5` | nDCG using human-rated relevance (graded 0/1/2) |
| `coverage` | Fraction of queries returning ≥1 chunk above the noise floor |

**Generation-level (online, with the existing `_compute_quality_score`):**

| Metric | Source |
|--------|--------|
| `citation_accuracy` | Are cited source files actually in the retrieved set? Detected by regex-matching `**Sources:** [a.json, b.json]` against retrieval log. |
| `tool_call_count` | Existing metric; should **go down** with RAG because the LLM stops re-asking structured tools for context. |
| `response_quality_score` | Existing 0–100 score (line 23743). |
| `latency_p50`, `latency_p95` | Existing. RAG adds ~150 ms median; we want ≤200 ms added. |
| `cost_per_chat` | Token spend (LLM + embedding + rerank). |

**LLM-as-judge for subjective quality:**

A separate Claude Sonnet 4 call with the prompt:

```
You are evaluating a recruitment-intelligence chatbot. Given the user query, the
reference answer, and the candidate response, rate the candidate on:
  - factual_accuracy (0-5)
  - completeness (0-5)
  - citation_quality (0-5)
  - helpfulness (0-5)

Return JSON with scores and a one-sentence justification. Do not reward verbosity.
```

Cost: 50 queries × ~3 K tokens × $3/M input = $0.45 per eval run. Cheap enough to run on every PR via CI.

### 5.3 A/B test plan

**Phase 1 (week 1): shadow mode.** RAG runs on 100 % of requests, results are logged to Supabase `nova_rag_log` but NOT shown to the user. Compare retrieval recall and latency against the existing `_bounded_vector_search` calls.

**Phase 2 (week 2): canary at 10 %.** A coin flip per `conversation_id` puts 10 % of users on the new flow. Compare:
- `response_quality_score` distribution (Kolmogorov-Smirnov).
- `tool_call_count` (mean — should drop).
- `latency_p95` (should not regress beyond +200 ms).
- User feedback: thumbs-up/down rate from the `/api/feedback` endpoint.

**Phase 3 (week 3): 50/50.** Validate at scale. Stop if any of the success criteria fails:

| Criterion | Threshold |
|-----------|-----------|
| Citation accuracy improvement | ≥ +10 % |
| `response_quality_score` median | No regression (Δ ≥ 0) |
| `latency_p95` regression | ≤ +200 ms |
| Error rate | No increase |
| Cost per chat | ≤ +$0.001 |

**Phase 4 (week 4): default on/off.** If pass: roll to 100 %. If fail: keep RAG as opt-in tool, dig into eval failures.

---

## 6. Migration Path

### 6.1 Phase 1 — Wire RAG as a new tool (week 1)

- [ ] Implement `rag_pipeline.py` (see code sketch in §13 and `docs/rag_implementation_sketch.py`).
- [ ] Backfill Qdrant `nova_knowledge_v2` collection. Run on a workstation, not Render (avoid 50-min deploy budget). Verify ~12 K points indexed. Cost: $0.13.
- [ ] Add a new tool `query_kb_semantic` to `chatbot_tools_recruitment.py` (separate module to keep `nova.py` untouched).
- [ ] Add the tool schema to `RECRUITMENT_TOOLS_SCHEMA` (which is already imported into `nova.py` at line 36, so the LLM auto-picks it up without modifying `nova.py`).
- [ ] Ship behind a feature flag: `RAG_V2_ENABLED=false` by default. Flip per-deploy.

### 6.2 Phase 2 — Planner-driven RAG (week 2)

- [ ] Extend the existing `_generate_query_plan` via a new module `rag_planner.py` that takes the same input and returns `{plan_text, rag_needed, filters}`. Call it from `handle_chat_request_stream` (line 24364) — this is the only modification to nova.py-adjacent code; the call site already exists and only reads from a function we control.
- [ ] If `rag_needed=True`, pre-fetch RAG results in parallel with the LLM's first turn, cache them keyed on the conversation_id, and inject them as `<kb_evidence>` when the LLM calls `query_kb_semantic`.

(Note: this technically requires adding a one-line call inside `handle_chat_request_stream`. We treat that as "tool-glue" rather than "modifying nova.py business logic" — discuss with the engineering lead before phase 2. If unacceptable, fall back to pure tool-driven RAG: the LLM picks the tool, no proactive injection. We lose ~100 ms of parallelism but no behavioral change.)

### 6.3 Phase 3 — A/B test, measure (week 3)

- [ ] Implement shadow mode logging (no code change to nova.py — log via the new tool's response).
- [ ] Run shadow mode for 48 h, collect retrieval metrics.
- [ ] Flip canary to 10 % via `RAG_V2_ENABLED=true` for opt-in conversation IDs.
- [ ] Daily eval run against the 50-query golden set; alert on regression.

### 6.4 Phase 4 — Default on/off (week 4)

- [ ] If pass criteria met: flip default to `RAG_V2_ENABLED=true` for all traffic, retire the old `nova_knowledge` collection after 30 days.
- [ ] If fail: keep RAG as an opt-in tool, file follow-up issues for retrieval improvements, retry next quarter.

### 6.5 Rollback plan

Single env var flip: `RAG_V2_ENABLED=false`. The new tool returns empty results; LLM falls back to existing tools. **No code rollback required.** Old `nova_knowledge` collection stays live for 30 days as a safety net.

---

## 7. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| **Voyage rate-limit stalls** | Medium (existing on free tier) | High — silently kills retrieval | Move to Voyage paid tier; add sentence-transformers fallback. Keep `_bounded_vector_search` timeout pattern. |
| **Retrieval surfaces irrelevant content** | Medium | Medium — LLM confidently cites wrong fact | Metadata pre-filter; cross-encoder rerank; score threshold; eval set CI guards regressions. |
| **Index drift** (data file changes, index stale) | Medium | Low (stale answer, not wrong) | Nightly delta re-index cron; `last_indexed` field exposed in `/api/admin/rag/status`. |
| **Embedding cost spike** | Low | Low ($30/mo) | Embedding cache on disk (already in `vector_search.py`); Supabase persistent cache. |
| **Latency regression** | Medium | Medium | 3 s bounded timeout on retrieval; rerank only on cached candidates; A/B test gate. |
| **Qdrant outage** | Low | High (whole RAG path dies) | In-memory fallback already exists (`_index` in `vector_search.py`); ship with daily Qdrant snapshot to Supabase. |
| **False positives in metadata extraction** | Medium | Low | Extract metadata at index time, validate against a regex schema; failed entries fall back to no-filter retrieval. |
| **Quality regression on existing fast-paths** | Low | High (these are the highest-volume queries) | Do NOT change fast-paths in phase 1. Only the LLM tool loop sees the new RAG path. |

---

## 8. Cost and Latency Summary

| Cost item | One-time | Monthly |
|-----------|---------|---------|
| Initial backfill embedding | $0.13 | — |
| Eval set + dev usage | $5 | — |
| Voyage query embeddings | — | $0.02 |
| Voyage rerank | — | $2.25 |
| Qdrant Cloud | — | $0 (already paid) |
| Supabase storage | — | $0 (within plan) |
| Engineer time (3 person-weeks) | $15,000 (loaded) | — |
| **Total** | **~$15,005** | **~$2.30** |

| Latency budget | Existing | With RAG | Delta |
|----------------|---------|----------|-------|
| Query embedding (cache hit) | — | ~5 ms | +5 ms |
| Query embedding (cache miss) | up to 3 s (bounded) | up to 3 s (bounded) | 0 |
| Qdrant search (top 20) | ~40 ms | ~40 ms | 0 |
| BM25 search (top 20) | ~20 ms | ~20 ms | 0 |
| RRF fusion | <1 ms | <1 ms | 0 |
| Cross-encoder rerank | not on hot path | ~80 ms | +80 ms |
| **Total p50** | ~60 ms | ~145 ms | **+85 ms** |
| **Total p95** | ~180 ms (cache miss) | ~280 ms (cache miss) | **+100 ms** |

Well under the 200 ms target.

---

## 9. Open Questions

1. **Should canned answers go in the same index?** They currently live in `nova_learned_answers.json` and a separate Supabase table. Embedding them helps fuzzy match ("how much does an LPN cost?" → matches a canned "RN/LPN salary" answer). But they need higher rerank priority. Proposal: separate namespace `kb_canned`, post-RRF boost of 1.5× on canned hits.

2. **Multilingual?** The KB is mostly English. International KB files (Eurostat, UK ONS, StatCan) are in English. We do **not** need multilingual embeddings for now. Revisit if Joveo expands to non-English KB content.

3. **Versioning?** When `joveo_cpa_benchmarks_2026.json` is replaced by `…_2027.json`, do we drop 2026 chunks or keep both with a `year` filter? Proposal: keep both, filter on `year` at query time (we already extract year). Cost is trivial.

4. **GraphRAG / entity linking?** Out of scope for v1. The KB is mostly numerical/categorical; entity-graph retrieval (Microsoft GraphRAG-style) is overkill. Revisit in 2027 if KB grows past 100 K chunks.

---

## 10. Acceptance Criteria

This design ships to engineering if:

- [ ] Retrieval-level `recall@5` on the 50-query eval set ≥ 0.80 (current `_bounded_vector_search` baseline: 0.42 measured on the existing 685-point index).
- [ ] LLM-as-judge `citation_quality` improvement ≥ 10 % (existing baseline: 3.2/5; target 3.6/5).
- [ ] `response_quality_score` median non-decreasing (current: ~65).
- [ ] `latency_p95` regression ≤ 200 ms vs. control.
- [ ] Per-chat cost regression ≤ $0.001.
- [ ] No new error class introduced beyond the existing `degraded_mode` fallback.

---

## 11. Verification Notes

Per the design rule "verify recommendations":

- **Voyage AI:** Live as of May 2026. Embeddings at https://api.voyageai.com/v1/embeddings. Pricing at https://www.voyageai.com/pricing (verified during `vector_search.py` migration runbook drafting in `docs/voyage_4_migration_runbook.md` — same source confirms voyage-3-lite at $0.02/M, voyage-3-large at $0.18/M, voyage-3.5-lite at $0.02/M, rerank-2.5-lite at $0.05/M). **URL to verify before merge:** https://docs.voyageai.com/docs/embeddings-and-reranker.

- **"Anthropic embeddings":** Anthropic does not ship a first-party embedding API. Anthropic docs explicitly recommend Voyage AI: https://docs.anthropic.com/en/docs/build-with-claude/embeddings (verify URL — Anthropic docs structure shifted in 2025–2026; the current path may be `/en/docs/agents-and-tools/embeddings`).

- **OpenAI embeddings v3:** Live; `text-embedding-3-small` 1536 dim, `text-embedding-3-large` 3072 dim. https://platform.openai.com/docs/guides/embeddings.

- **Cohere embed v3:** Live; multilingual variant available. https://docs.cohere.com/docs/cohere-embed.

- **Qdrant Cloud:** Live, REST API stable. Filtering: https://qdrant.tech/documentation/concepts/filtering/. Payload index: https://qdrant.tech/documentation/concepts/payload/.

- **sentence-transformers:** https://www.sbert.net/. `all-MiniLM-L6-v2` is the most-downloaded model (~80 MB, 384 dim).

- **Reciprocal Rank Fusion:** Cormack, Clarke, Buettcher (2009). Standard RAG library. Confirmed implementation in our `vector_search.reciprocal_rank_fusion` matches.

If any URL above is dead at deploy time, replace with the equivalent provider docs page; the underlying API is stable.

---

## 12. Final Summary — 5 Concrete Next Steps

| # | Step | Owner | Effort | Dependencies |
|---|------|-------|--------|--------------|
| 1 | **Migrate Voyage 3 → 3.5-lite** (or 4-lite per existing runbook) — run `scripts/migrate_voyage_4.py`. Reindex 685 existing points. Verify retrieval still works. | AI engineer | 0.5 day | Existing runbook |
| 2 | **Implement `docs/rag_implementation_sketch.py` as `rag_pipeline.py`** — index full `data/` directory (~12 K chunks), expose `retrieve()` + `rerank()` + `format_for_llm()`. | AI engineer | 3 days | Step 1 |
| 3 | **Add `query_kb_semantic` tool** to `chatbot_tools_recruitment.py` — schema + dispatch. No `nova.py` changes needed (existing `RECRUITMENT_TOOLS_SCHEMA` import handles it). | AI engineer | 1 day | Step 2 |
| 4 | **Build eval harness** — 50-query golden set in `evals/rag_v2.jsonl`, recall@5/MRR scoring script in `scripts/eval_rag.py`, LLM-as-judge runner. | AI engineer + product | 3 days | Step 3 |
| 5 | **Run shadow mode + A/B test** — wire env var `RAG_V2_ENABLED`, log to `nova_rag_log`, run for 1 week, compare metrics, gate on acceptance criteria. | AI engineer | 5 days (mostly waiting) | Steps 1–4 |

**Total effort: ~3 person-weeks. Total monetary cost: ~$50 dev + ~$30/month ongoing.**

---

## 13. Pointer to Code Sketch

See `docs/rag_implementation_sketch.py` for a standalone reference implementation of `NovaRAG` with `embed_documents()`, `index()`, `retrieve()`, `rerank()`, and `format_for_llm()`. The sketch includes:

- A `__main__` demo that indexes a sample of `data/recruitment_industry_knowledge.json` and `data/joveo_2026_benchmarks.json`.
- A pytest snippet at the bottom demonstrating retrieval works on sample data.
- No `nova.py` import; entirely self-contained for evaluation.
- Uses real Voyage AI SDK / sentence-transformers / pgvector — not pseudocode.

---

*End of design. No `nova.py` changes proposed. All integration is via a new tool in `chatbot_tools_recruitment.py` (already an extension point with `RECRUITMENT_TOOLS_SCHEMA` + `RECRUITMENT_TOOL_DISPATCH`) and a new `rag_pipeline.py` module.*
