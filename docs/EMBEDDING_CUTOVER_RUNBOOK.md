# Nova Embedding Cutover Runbook: Voyage → Gemini (Layer-3 #11)

**Status (2026-07-22):** cutover EXECUTED and ROLLED BACK pending one
Google-console action. The flip reached prod (provider/model/collection all
verified live via the deploy/ready embedding block) but startup indexing
failed with, verbatim: `gemini HTTP 403 PERMISSION_DENIED -- "Requests to
this API generativelanguage.googleapis.com method google.ai.generativelanguage.
v1beta.GenerativeService.BatchEmbedContents are blocked."` The prod
GEMINI_API_KEY has API restrictions allowing chat but blocking embeddings.
UNBLOCK: Google AI Studio / Cloud Console -> Credentials -> that key -> API
restrictions -> allow the Generative Language API embed methods (or remove
method restrictions). RE-FLIP: change get_embedding_provider()'s default
return back to EMBEDDING_PROVIDER_GEMINI (one line, plus its two default
tests) and ship; the deploy self-migrates and /api/deploy/ready shows
indexed_documents ~6.2K on success.

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

## (b) How the cutover works (CORRECTED 2026-07-21)

**Proven live:** Render does NOT apply this service's `render.yaml` envVars
(the service is dashboard-managed; the file's env block is documentation).
A render.yaml-only flip (`EMBEDDING_PROVIDER: gemini`, commit shipped and
deploy-verified) changed nothing -- caught by the `/api/deploy/ready`
`embedding` block still reporting voyage on the fresh boot.

The flip therefore lives in CODE: `vector_search.get_embedding_provider()`
defaults to **gemini** when `EMBEDDING_PROVIDER` is unset (cutover commit,
2026-07-21). The env var remains an override in BOTH directions:

* **Flip/rollback by commit:** change the code default and ship (full gate).
* **Emergency rollback, no deploy:** set `EMBEDDING_PROVIDER=voyage` in the
  Render dashboard (env change restarts the service; the Voyage collection
  `nova_knowledge` is untouched by the gemini path, so it serves instantly).

On the first gemini boot the app self-migrates: deferred-startup indexing
embeds the KB (~6,226 chunks / ~273K tokens / ~63 batched requests) via
gemini-embedding-2 into the fresh model-scoped collection
`nova_knowledge__gemini-embedding-2_768`. BM25/TF-IDF serve during the
window; the disk cache (model-namespaced) makes every later deploy's
re-index near-instant.

## (c) Verification

1. **Deploy landed:** `GET /api/health` — check `version`/deploy timestamp
   flipped to confirm the new instance is serving.
2. **Provider flip took effect:** the detailed `/api/health` payload that
   embeds `vector_search.get_status()` is **admin-gated** (Bug #9: public
   callers get the minimal status/version body only), so the public
   instrument is the readiness gate: `GET /api/deploy/ready` carries an
   `embedding` block (zero-I/O module-state reads, added 2026-07-21):
   ```json
   "embedding": {
     "provider": "gemini",
     "model": "gemini-embedding-2",
     "dim": 768,
     "collection": "nova_knowledge__gemini-embedding-2_768",
     "indexed_documents": 6226
   }
   ```
   `provider`/`model`/`dim`/`collection` confirm the flip instantly;
   `indexed_documents` climbing from 0 to roughly the corpus size (~6.2K
   chunks) is the proof that startup indexing actually filled the new
   space -- poll it for a few minutes after the deploy goes ready. (With
   `ADMIN_API_KEY`, the full `/api/health` shows the same via
   `vector_search.get_status()`, `vector_search.py` ~line 2862.)
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
