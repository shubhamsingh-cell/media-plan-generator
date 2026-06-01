# RAG REST Conversion Report

**Date:** 2026-06-02
**File changed:** `rag_pipeline.py` (only)
**Goal:** Make Nova's RAG pipeline run WITHOUT the `voyageai` and `qdrant-client`
pip deps (removed in commit `39a12dc` because voyageai's stable wheels cap at
Python < 3.13 and Render builds on 3.13), by replacing the embedder + vector
store + reranker internals with pure-stdlib REST calls.

---

## 1. What changed

All changes are confined to `rag_pipeline.py`. The hybrid pipeline
(BM25 + RRF + rerank), the chunking/metadata code, the dataclasses, and the
public `retrieve()` / `index()` / `format_for_llm()` signatures are unchanged.

### 1.1 New stdlib HTTP layer
- `_http_post_json(url, payload, headers, ...)` — POST JSON via `urllib` + `json`
  + `ssl`. One retry on **429 and 5xx** with exponential backoff (honours
  `Retry-After`). Non-retryable 4xx (e.g. 401) fail fast. Network errors
  (timeout/DNS/reset) retried the same way.
- `_http_json(url, headers, method=..., payload=...)` — GET/PUT/POST variant
  used by the Qdrant store (collection check, retrieve, upsert, count). 404 is
  surfaced as a control-flow signal (logged at debug, not error).
- `_retry_delay(...)` — backoff helper preferring the server `Retry-After`.
- One shared `ssl.create_default_context()` reused across calls.

### 1.2 Embedder: `VoyageRESTEmbedder` (new PRIMARY)
- POSTs to `https://api.voyageai.com/v1/embeddings` with
  `Authorization: Bearer $VOYAGE_API_KEY` and body
  `{"input": [...], "model": "voyage-3.5-lite", "input_type": "document"|"query"}`.
- `input_type="document"` for indexing (`embed_batch`), `"query"` for retrieval
  (`embed_query`) — Voyage uses asymmetric encoders.
- Parses `data[].embedding`; sorts rows by `index` defensively; validates count.
- Batches `<= 128` inputs per call (`_VOYAGE_BATCH_LIMIT`); truncates each input
  to 32K chars.
- The SDK class `VoyageEmbedder` is retained as an optional fallback.

### 1.3 Vector store: `QdrantRESTStore` (new PRIMARY)
- Query: `POST {QDRANT_URL}/collections/nova_knowledge_v2/points/query` with
  header `api-key: $QDRANT_API_KEY`, body
  `{"query": [vector], "limit": k, "with_payload": ..., "filter": {...}}`. Parses
  `result.points[].{id, score, payload}`.
- **Endpoint fallback:** if `/points/query` returns 404/501 (older Qdrant), it
  latches to the legacy `/points/search` (`{"vector": ...}`, parses `result[]`)
  so subsequent calls skip the failed probe. `/points/query` is current for
  Qdrant 1.10+ (the Cloud collection here is on 1.10+, confirmed live).
- Filter builder emits Qdrant REST JSON:
  `{"must": [{"key": k, "match": {"value": v}}]}` (or `{"any": [...]}` for lists).
- `get` (POST `/points` with `ids`), `count` (GET collection →
  `result.points_count`), and `upsert`/`_ensure_collection` (PUT `/points`,
  PUT collection + payload indexes) all over REST.
- The SDK class `QdrantStore` is retained as an optional fallback.

### 1.4 N+1 fix: `search_payload()`
Added an **optional** `search_payload()` to the vector-store base + the REST
store. It returns `(doc_id, score, Document)` in a **single** `/points/query`
call using `with_payload=true`. `retrieve()` now prefers it and primes a
per-query doc cache, eliminating the previous N+1 `store.get()` per candidate
(up to 20 sequential REST round-trips per query) on the production hot path
(where the in-process `_docs_by_id` mirror is empty because the corpus lives
only in Qdrant). `search()` and the public API are unchanged; backends that
don't support it return `None` and `retrieve()` degrades to `search` + `get`.

### 1.5 Reranker: REST (new PRIMARY)
- `_Reranker` now POSTs to `https://api.voyageai.com/v1/rerank` with
  `{"query": ..., "documents": [...], "model": "rerank-2.5-lite", "top_k": k}`
  and parses `data[].{index, relevance_score}`. No SDK import.
- Best-effort: any failure (missing key, REST error, malformed response) falls
  back to the RRF ordering, so retrieval never breaks.

### 1.6 Factory selection (`_make_embedder` / `_make_store`)
Auto-detection order is now **REST first**, SDK second, then local/in-memory:
- Embedder: `VoyageRESTEmbedder` → `VoyageEmbedder` (SDK) → `LocalEmbedder` →
  `HashEmbedder`.
- Store: `QdrantRESTStore` → `QdrantStore` (SDK) → `InMemoryStore`.
New `prefer` keys: `"voyage_rest"`, `"qdrant_rest"` (the historical `"voyage"` /
`"qdrant"` keep their SDK meaning for any callers/tests).

### 1.7 Docs / exports
- Module docstring updated to describe the REST transport and endpoints.
- `requirements.txt` already documents the no-SDK stance (unchanged by this task).
- `__all__` adds `VoyageRESTEmbedder`, `QdrantRESTStore`.

---

## 2. Before / after

| Aspect | Before | After |
|---|---|---|
| Embedder primary | `voyageai` SDK (`pip install voyageai`) | stdlib REST POST to `/v1/embeddings` |
| Vector store primary | `qdrant-client` SDK | stdlib REST POST to `/points/query` |
| Reranker | `voyageai` SDK `.rerank()` | stdlib REST POST to `/v1/rerank` |
| Imports without SDKs | crashes / degrades to hash + in-memory (no real retrieval) | imports clean; full Voyage + Qdrant retrieval over REST |
| Prod (`RAG_V2_ENABLED`) result | `rag_disabled` (SDKs absent on Py 3.13) | real cited passages from 5,153-point collection |
| Candidate materialization | N+1 `store.get()` per hit | single `search_payload` call (`with_payload`) |
| New pip deps | — | **none** (urllib/json/ssl/time only) |

---

## 3. Live retrieval proof

Run with real env vars (`VOYAGE_API_KEY`, `QDRANT_URL`, `QDRANT_API_KEY`):

```
pipeline embedder: VoyageRESTEmbedder
pipeline store   : QdrantRESTStore
rerank enabled   : True
store count      : 5153
======================================================================
query: "talent acquisition trends from industry experts"  (top_k=5)
total hits returned: 5
1. src=ta_leaders_curated_2026.json   section=influencers[12].posts[0]  score=0.8750  method=rerank
2. src=recruitment_industry_knowledge.json  section=sources[55]         score=0.8633  method=rerank
3. src=external_benchmarks_2025.json  section=reports.gartner_ta_trends_2025_2026  score=0.8516  method=rerank
4. src=ta_leaders_curated_2026.json   section=influencers[1].posts[3]   score=0.8359  method=rerank
5. src=industry_reports_2026.json     section=reports[21]               score=0.7578  method=rerank
======================================================================
hits from ta_leaders_curated_2026.json: 2
```

**Top hit from `ta_leaders_curated_2026.json`** (the backfilled TA-leaders source):

```
source : ta_leaders_curated_2026.json
section: influencers[12].posts[0]
score  : 0.875  (method: rerank)
text   :
  [ta_leaders_curated_2026.json] influencers[12].posts[0]
  title: Talent Acquisition Trends to Watch in 2026
  url: https://mattcharney.com/2025/12/30/talent-acquisition-trends-to-watch-in-2026
  thesis: Six trends: (1) Hiring becomes surgical not strategic; (2) Talent
          shortage is really risk avoidance; (3) RPA rebranded as Agentic AI;
          (4) Talent attraction shifts to talent rediscovery; (5) Skills-based
          hiring won't happen; (6) TA leaves the HR group chat.
  key_quote: Somewhere along the line, the people who build HR tech stopped
             believing in recruiters.
```

This confirms the full path — Voyage embed (REST) → Qdrant query (REST) →
RRF → Voyage rerank (REST) — returns real cited passages, with `>= 1` hit
(in fact 2) from `ta_leaders_curated_2026.json`.

---

## 4. Verification status

| Check | Result |
|---|---|
| Import WITHOUT voyageai/qdrant_client stubs (`anthropic`/`openai`/`supabase`/`redis`/`sentence_transformers`/`posthog` stubbed) | PASS — `rag_pipeline imports OK (no SDK deps)` |
| Factory selects REST classes with real env vars | PASS — `VoyageRESTEmbedder` + `QdrantRESTStore` |
| `docs/rag_implementation_sketch.py` (6 sketch tests) | PASS — 6 passed |
| `tests/test_country_awareness.py` | PASS — 70 passed |
| Live `retrieve()` against real Voyage + Qdrant | PASS — 5 hits, 2 from `ta_leaders_curated_2026.json`, top score 0.875 |
| Live metadata filter (`source_file=ta_leaders_curated_2026.json`) | PASS — 5 hits, no leak |
| Graceful degradation, keys missing | PASS — hash + in-memory, `retrieve` returns `[]`, no crash |
| Graceful degradation, bad Voyage key | PASS — returns `[]`, no crash |
| Graceful degradation, bad Qdrant key | PASS — returns `[]`, no crash |
| Retry: 429 → 1 retry → success | PASS (2 attempts, honoured `Retry-After`) |
| Retry: persistent 500 → raise after 1 retry | PASS (2 attempts) |
| Non-retryable 401 → fail fast | PASS (1 attempt) |
| Legacy `/points/search` fallback latch on 404 | PASS (latches, no re-probe) |
| `py_compile`, no bare excepts, type hints | PASS |

**Environment note:** local Python is 3.9.6 (Render is 3.13). The module uses
`from __future__ import annotations`, so the `X | None` hints are strings at
runtime and import on both. The only runtime use of `Optional[...]` is via the
imported `typing.Optional`.

---

## 5. Constraints honoured
- Stdlib only on the primary path (`urllib`, `json`, `ssl`, `time`). No new pip deps.
- Type hints throughout; `logger.error(..., exc_info=True)` on REST failures;
  no bare `except`.
- `retrieve()` / `index()` / `format_for_llm()` public signatures unchanged.
- Only `rag_pipeline.py` edited (plus this report).
