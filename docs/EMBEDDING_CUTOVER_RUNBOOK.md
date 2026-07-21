# Nova Embedding Cutover Runbook: Voyage → Gemini (Layer-3 #11)

**Status:** code ready on `main`. The cutover itself is a one-line
`render.yaml` change, not yet flipped. This doc is the how/why/verify/rollback
for whoever flips it.

## (a) Why

Nova's vector search has been running on Voyage AI (`voyage-3-lite`,
512-dim) with a self-imposed `_VOYAGE_RPM_LIMIT = 10` requests/minute
(`vector_search.py`) — a conservative estimate, not a documented account
ceiling, but it is the wall search throughput hits today.

Separately, the two obvious "free, already in the stack" alternatives are
both dead:

- **`text-embedding-004`** — shut down 2026-01-14 (ai.google.dev changelog:
  *"January 14, 2026: The text-embedding-004 model has been shut down."*).
- **`gemini-embedding-001`** — documented shutdown date 2026-07-14
  (deprecations table: `gemini-embedding-001 | July 14, 2025 | July 14, 2026 |
  gemini-embedding-2`).

The current GA model is **`gemini-embedding-2`** (GA 2026-04-22): 768-dim
recommended via `outputDimensionality` ("near-peak quality, 1/4 the storage"
of its native 3072-dim), auto-normalized, 8192-token input, $0.20/1M tokens
standard pricing. `vector_search.py` now targets this model by default
(`GEMINI_EMBED_MODEL` env-overridable), so the SESSION_HANDOFF #11 plan of
reindexing against `text-embedding-004` is superseded — that model doesn't
exist anymore, and the old instructions would have run the reindex script
against a dead endpoint (and, before the collection-scoping fix below,
against the live-serving Voyage collection).

## (b) How the cutover works

1. Flip the value in `render.yaml`:

   ```yaml
   - key: EMBEDDING_PROVIDER
     value: gemini
   ```

2. Ship the commit through the normal deploy path
   (`scripts/ship_from_worktree.sh` per `CLAUDE.md` §11 — never push to `main`
   from a worktree by hand).

3. On the new instance's startup, deferred indexing runs automatically:
   `app.py` startup → `index_knowledge_base()` → `build_index()` →
   `embed_batch()` (now provider-routed to Gemini) → upsert into
   `vector_search._active_collection()`, which for the Gemini provider is
   `nova_knowledge__gemini-embedding-2_768` — a **new, empty** collection the
   very first time, not the Voyage one.
   - Corpus: 6,226 chunks / ~273K tokens ≈ 63 batches of 100
     (`_GEMINI_MAX_BATCH = 100`).
   - No manual `scripts/reindex_embeddings.py` run or Qdrant shell/dashboard
     work is required — the deploy *is* the migration.
   - While that first batch of indexing is in flight, search serves from
     BM25 (keyword) only; vector results come online once the collection is
     populated. This is the existing graceful-degradation path
     (`embed_batch` returning `None` on any failure already falls back to
     BM25/TF-IDF), not new behavior introduced by this cutover.
   - The on-disk embedding cache (`data/.embedding_cache.json`, or
     `/data/persistent/.embedding_cache.json` on Render) is keyed by
     `f"{get_active_embedding_model()}:{text}"`
     (`vector_search._text_cache_key`), so it's already model-namespaced —
     Gemini and Voyage entries can't collide. Once warm, later deploys reuse
     cached vectors instead of re-embedding, so the ~63-batch cost is paid
     once, not on every deploy.

## (c) Verification

1. **Deploy landed:** `GET /api/health` — check `version`/deploy timestamp
   flipped to confirm the new instance is serving.
2. **Provider flip took effect:** the same `/api/health` response embeds the
   vector-search status under the `vector_search` key
   (`app.py::_build_health_response` → `routes/health.py::_handle_health` →
   `vector_search.get_status()`, `vector_search.py` ~line 2862). Confirm:
   ```json
   "vector_search": {
     "embedding_provider": "gemini",
     "embedding_model": "gemini-embedding-2",
     "embedding_dim": 768,
     ...
   }
   ```
   (`get_status()`'s `model`/`embedding_model` both come from
   `get_active_embedding_model()`, `embedding_dim` from `_active_vector_dim()`
   — both provider-routed, so this is a live read of what's actually serving,
   not a config echo.)
3. **Live query:** run one real chat/search query against Nova (chat widget
   or `/api/nova/chat`) and confirm a relevant, non-BM25-only-looking answer
   comes back — i.e. the new collection has vectors in it, not just an empty
   index falling back to keyword search.

## (d) Rollback

Revert the `render.yaml` commit (`EMBEDDING_PROVIDER` back to `voyage`) and
redeploy. The Voyage collection (`nova_knowledge`) is never written to or
deleted by the Gemini path — `_active_collection()` only ever touches the
Gemini-scoped collection while `EMBEDDING_PROVIDER=gemini` — so rollback is
instant and lossless: Voyage resumes serving from the same collection it
never stopped owning.

## (e) Model succession playbook

When Google ships a successor to `gemini-embedding-2`:

1. Set `GEMINI_EMBED_MODEL=<new model>` (env var, e.g. in `render.yaml` or
   the Render dashboard).
2. Deploy.
3. `_active_collection()` derives its name from `_GEMINI_EMBED_MODEL` (and
   dimension), so the new model automatically gets its own fresh collection
   name — it can never collide with or overwrite the previous Gemini
   collection. Startup indexing fills it exactly as in (b) above.
4. Rollback is unsetting `GEMINI_EMBED_MODEL` (or setting it back), same as
   the provider-level rollback in (d).

This is config, not surgery — no code change is required for a same-shape
model succession (the batchEmbedContents contract, `outputDimensionality`
field, and response-shape validation all stay the same).

## (f) Known facts & levers

- **Voyage rate limit:** the in-app `_VOYAGE_RPM_LIMIT = 10` is a
  conservative estimate, not a documented ceiling. Voyage's own docs
  (docs.voyageai.com/docs/rate-limits) list Tier 1 (payment method added) at
  2000 RPM / 16M TPM for `voyage-3.5-lite`, with `rerank-2.5-lite` on its own
  separate 2000 RPM / 4M TPM bucket, plus 200M free tokens for embeddings and
  a separate 200M for rerank-2.5-lite. **Check the Voyage dashboard to
  confirm the actual account tier** before raising `_VOYAGE_RPM_LIMIT` — the
  10 RPM constant is a lever, not a hard vendor limit, but 2000 RPM is
  unverified for this specific account.
- **Gemini interactive rate limit:** not published for the free/interactive
  tier used here; the AI Studio dashboard shows the account's real quota.
  Failure mode is graceful: `_embed_batch_gemini` retries 429s with backoff
  (`_GEMINI_MAX_RETRIES = 3`, exponential from `_GEMINI_BASE_BACKOFF = 2.0s`)
  and falls back to BM25/TF-IDF on exhaustion — never a hard failure.
- **`taskType` (RETRIEVAL_QUERY / RETRIEVAL_DOCUMENT):** Gemini's embedding
  API supports a `taskType` field for asymmetric query/document embeddings,
  which is a real quality lever we are not yet using — the current
  implementation embeds both queries and documents the same way. Adopting it
  is future work requiring both the embed-cache key and the collection name
  to be re-scoped (a query embedded with `RETRIEVAL_QUERY` is a different
  vector space than the same text embedded plain), so it is not a drop-in
  change.
- **`scripts/migrate_voyage_4.py`** is a legacy one-off migration script with
  its own hardcoded `QDRANT_COLLECTION = "nova_knowledge"` constant. It is
  **not** in the live embedding-provider path and is unaffected by (and
  unaware of) `_active_collection()` / the provider switch — do not confuse
  it with `scripts/reindex_embeddings.py`, which is the current, provider-aware
  tool.
