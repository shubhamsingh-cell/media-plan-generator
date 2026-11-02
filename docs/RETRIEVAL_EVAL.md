# Retrieval eval (recall@k / MRR) for the Nova knowledge base

`evals/retrieval_eval.py` is the gate for any retrieval change: embedding model
swap, reranker, chunking, hybrid weights, collection migration. It replays 79
golden questions against the same chunks the production indexer builds and
reports recall@1/3/5/10 and MRR@10 per retrieval tier.

## What it measures

| Tier | What runs | Needs |
|---|---|---|
| `bm25` | `vector_search.BM25Index` (the production class) | nothing |
| `tfidf` | `vector_search._tfidf_search` (the production fallback) | nothing |
| `vector` | dense cosine over chunk embeddings | an embedder (key) |
| `hybrid_rrf` | vector + BM25 fused with RRF k=60, as `search()` does | an embedder (key) |

The two offline tiers are the FALLBACK tiers; production serves `vector` from
Qdrant (`search_mode: vector`). The offline numbers therefore gate the fallback
path and the chunk text; the dense tiers gate what users actually get and only
run when an embedder is supplied. Without one they print an explicit
`SKIPPED: no key ...` row, never a silent omission.

* A document is one chunk of `data/*.json` as `vector_search.index_knowledge_base`
  builds it (`tests/test_retrieval_eval.py` pins parity with the real indexer).
* Golden rows reference a STABLE id, `file.json#key.path`, not the positional
  `file.json:N` id (that counter shifts when any KB file changes).
* `expected_ids` are alternative valid answers; `recall@k` is the percentage of
  queries with at least one expected id in the top k. `mrr` is MRR@10 x 100. All
  metrics are percentage points, so the 3-point regression rule is uniform.

## Commands

```bash
# Offline (what CI runs): BM25 + TF-IDF, prints the table + worst queries
python3 evals/retrieval_eval.py

# Gate: exit 1 when any tier is more than 3 points below the committed baseline
python3 evals/retrieval_eval.py --check

# After a verified-good change, refresh the committed baseline
python3 evals/retrieval_eval.py --update-baseline

# Do all golden ids still resolve against the current KB files?
python3 evals/retrieval_eval.py --audit-golden
```

### Owner run with a real Voyage key (dense + hybrid tiers)

```bash
# Prod model. ~6.2k chunk embeddings at 32/request: ~195 requests. At the repo's
# 10 RPM assumption that is ~20 minutes the first time; vectors are cached under
# evals/.cache/ (git-ignored) so every later run only embeds new text.
VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage --voyage-rpm 10

# First keyed run: record the dense tiers in the baseline (then commit it)
VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage --update-baseline

# Gate afterwards (offline CI without the key just skips the dense tiers)
VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage --check

# Plan step 6 A/B: same golden set, different model; nothing else changes
VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage \
    --voyage-model voyage-3-large --report /tmp/voyage3large.json
```

`--voyage-rpm` should match the account's real limit: per Voyage/MongoDB docs
(retrieved 2026-10-01) an account WITHOUT a payment method is limited to 3 RPM
and 10K TPM, and one with a payment method (Tier 1) gets 2,000 RPM for
voyage-4-lite. At 10K TPM a 32-chunk batch can exceed the per-minute token cap,
so use `--voyage-batch 8` on an unpaid key. An Atlas-issued key needs
`VOYAGE_BASE_URL=https://ai.mongodb.com/v1`.
To plug in another embedder, pass any object with `name` and
`embed(texts, input_type)` to `run_eval(embedder=...)`.

The eval never touches `data/.embedding_cache.json` or Qdrant.

## Golden set

`evals/retrieval_golden.json`: `{"rows": [{"id", "domain", "query", "expected_ids",
"notes"}]}`. Domains: `cpc_cpa`, `salary`, `channel`, `seasonality`,
`recruitment_marketing`, `platform_performance`, `regional_supply`.

To add a row, find the chunk(s) that answer the question, put their stable ids in
`expected_ids` (include every chunk that genuinely answers it; the KB repeats
facts across files), and run `--audit-golden` to confirm each id resolves and
reads right.

Known limits, stated plainly:

* Expected ids were pooled from a manual KB search plus the top results of BM25
  and TF-IDF. A dense tier can surface valid chunks nobody pooled, so dense
  recall here is a conservative lower bound until the golden set is re-pooled
  with the dense tier's top results.
* Queries are deliberately a mix of lexically close and paraphrased wording, but
  79 rows is a regression gate, not a statistically tight benchmark; compare
  tiers by direction and by the domain breakdown, not by single points.
* Rows whose ids go stale after a KB edit are reported (`stale_expected_ids`) and
  excluded from scoring; a row with no resolvable id fails the test suite.

## Chunker information loss (found while building the golden set)

`vector_search._extract_text_chunks` only keeps string values longer than 20
characters. Across the 37 indexed KB files, 73.5% of leaf values (21.8% numbers,
47.2% short strings, the rest booleans/skipped keys) never reach the embedding
or BM25 text; only 26.5% (long strings) do. Concretely, numeric benchmark tables
(e.g. `salary_benchmarks_detailed_2026.json` roles, `joveo_2026_benchmarks.json`
`median_cpa_by_occupation_2025`) and platform names stored as short strings are
absent from the indexed text, so those answers cannot be retrieved by any tier.
The golden set therefore targets chunks whose indexed text carries the answer.
Fixing this changes every chunk id and requires a re-embed (a new blue/green
collection), so it is a decision, not part of this change; this eval is the tool
that will measure whether it helps.

## Nova retrieval eval (`evals/nova_retrieval_eval.py`)

A second, client-neutral question set (`evals/nova_retrieval_queries.json`, 72
rows: CPC/CPA by country and industry, job boards by country, report facts,
plan figures, 10 out-of-scope) scored on recall@5, recall@10 and MRR. It reuses
this harness's corpus and stable ids. Out-of-scope rows are never in recall;
their mean top-1 score is reported beside the in-scope mean.

| Tier | What runs | Needs |
|---|---|---|
| `local_bm25` / `local_tfidf` | the in-process keyword tiers | nothing |
| `qdrant_vector` | the LIVE collection prod searches (one GET through the dim guard, then `points/search`; read-only) | `QDRANT_URL`, `QDRANT_API_KEY`, `VOYAGE_API_KEY` |
| `qdrant_hybrid` | Qdrant + local BM25 fused with RRF k=60 | same |

`--check` exits 1 when any tier's recall@10 is more than 3 points below
`evals/nova_retrieval_baseline.json` or a label stops resolving. The Qdrant tiers
are not in the committed baseline yet (no keys where it was recorded).

Caveat on `qdrant_hybrid`: it fuses by STABLE id, so it measures hybrid quality
as if Qdrant and the local BM25 index agreed on every chunk. Production fuses by
the positional `file.json:N` id behind a drift guard (`_separate_drifted_ids`):
an id whose local text differs from Qdrant's is ranked separately, not fused. An
independent review measured 401 of 6,234 ids drifted on 2026-11-03, so this tier
can overstate prod hybrid quality until the collection is re-indexed from the
current KB. `retrieval.keyword_indexes.hybrid_id_text_mismatches` on
`/api/deploy/ready` counts the drift prod actually hits.

CI: job `nova-retrieval-eval` (offline) and `nova-retrieval-eval-live`
(push to main, non-blocking, skipped without secrets). Neither blocks a merge or
a deploy today: main has no branch protection and Render `autoDeploy` ships main.

### Owner-only actions

* Require the `nova-retrieval-eval` check on main (GitHub branch protection);
  until then it is a signal, not a gate.
* Add the `QDRANT_URL`, `QDRANT_API_KEY` and `VOYAGE_API_KEY` repository secrets
  if the live job should run, then record the Qdrant tiers once:
  `python3 evals/nova_retrieval_eval.py --update-baseline` with the keys set.

### Model-id note (2026-11-03 audit)

The OpenRouter DeepSeek fallback moved from `deepseek/deepseek-r1` to
`deepseek/deepseek-v4-flash` because the bare `deepseek/deepseek-r1` slug is not
in OpenRouter's public catalog. The dated `deepseek/deepseek-r1-0528` is still
listed there; it was not chosen because the repo pins DeepSeek to the V4 pair.
(The commit message for that change says R1 "left the catalog"; this is the
precise statement.)

## 2026-11-03: short-string chunker gap (analysis only, no reindex)

Scope: analysis of what `vector_search._extract_text_chunks` drops and what
relaxing it would cost. Nothing was changed: no chunker edit, no reindex, no
Voyage or Qdrant call. All counts below come from a throwaway script that
re-implements the walk, then was checked against the real code (see Method).

### Verified facts (read from code and data)

* The walk covers the 42 filenames in `vector_search._KB_INDEX_FILES`, shared by
  `_collect_kb_documents()` (index and BM25/TF-IDF) and
  `scripts/reindex_embeddings.py`. Current output: **6,228 chunks, 1,096,780
  chars**.
* Chunk formation, `_extract_text_chunks`:
  * A dict becomes ONE chunk: `[file] key.path` plus a `key: value` line for each
    direct string value longer than 20 chars. Nested dicts and lists recurse and
    form their own chunks. A dict whose direct string values are all 20 chars or
    fewer emits no chunk at all.
  * A string that is a direct list item (or the root) becomes its own chunk
    `[file] path[i]: value`, again only if longer than 20 chars.
  * Keys starting with `_` and the keys `metadata`, `last_updated`, `version`
    are skipped. This is a SECOND filter, independent of length: every
    `_verification_notes` and `_last_verified` field is unindexed today and would
    stay unindexed after a length-only fix.
  * `_collect_kb_documents` then drops chunks under 20 chars after strip (the
    `[file] path` prefix makes that moot in practice) and truncates to 2,000
    chars. Ids are positional (`file.json:N`), so any chunk added shifts every
    later id in the run.
* Short string leaves dropped by the length rule (reachable, non-underscore
  keys): **16,620** across the 42 files.
* `global_supply.json` `country_job_boards`: 34 countries, 253 board entries,
  each `{name, billing, category, tier, _last_verified, _verification_notes}`.
  `name`, `billing`, `category`, `tier` are nearly all 20 chars or fewer, so a
  board dict emits no chunk. Today the key yields **19 chunks (1,984 chars)**,
  one `name:` line each for the few boards whose name is over 20 chars. About
  234 of 253 boards are absent from every tier. 1,175 short leaves dropped
  under this key (includes `key_metros` list items and `monthly_spend`).

### Method

A throwaway script kept outside the repo (not committed). It
copies the walker with a configurable minimum length, runs it over the same file
list, and attributes chunks to `file :: top-level key`. Parity check against the
real `vs._collect_kb_documents()` (imported with sockets blocked, no keys set):
real 6,228 chunks / 1,096,780 chars, copy 6,228 / 1,096,780. Exact match.

Variants (`len >= N` on the string value):

| Variant | Rule | Total chunks | Extra chunks | Extra chars | Extra tokens (chars/4) |
|---|---|---|---|---|---|
| baseline | dict values and list items `> 20` | 6,228 | 0 | 0 | 0 |
| **B (recommended)** | dict values `>= 3`, list-item strings stay `> 20` | 8,008 | +1,780 | +331,200 | ~82,800 |
| A | every string `>= 3` | 15,218 | +8,990 | +987,130 | ~246,800 |
| C | every string `>= 1` | 15,700 | +9,472 | +1,025,929 | ~256,500 |

Why B: variant A and C also turn each short list item (e.g. a metro name in
`key_metros`) into its own one-line chunk with a long path prefix. That adds
about 7,200 near-empty chunks, which dilutes dense retrieval and BM25. B keeps
the dict-level grouping (one chunk per board, per region, per role) and fixes
the actual loss. It is a recommendation to evaluate, not a measured win: run the
eval below before choosing.

### Most affected keys (extra chunks vs baseline)

Variant B, top keys:

| File :: key | Extra chunks | Extra chars | Short leaves dropped today |
|---|---|---|---|
| `global_supply.json :: country_job_boards` | +267 (19 -> 286) | +32,699 | 1,175 |
| `regional_hiring_intelligence.json :: regions` | +569 | +84,554 | 2,418 |
| `salary_benchmarks_detailed_2026.json :: roles` | +131 | +10,353 | 153 |
| `industry_white_papers.json :: reports` | +124 | +30,515 | 817 |
| `linkedin_performance_benchmarks.json :: linkedin_timezone_peaks` | +73 | +13,841 | 301 |
| `top_employers_by_city_2026.json :: metros` | +52 | +5,158 | 140 |
| `google_ads_2025_benchmarks.json :: categories` | +43 | +4,871 | 43 |
| `recruitment_benchmarks_deep.json :: industry_benchmarks` | +41 | +12,422 | 556 |
| `external_benchmarks_2025.json :: reports` | +39 | +5,597 | 104 |
| `global_supply.json :: dei_boards_by_country` | +37 | +4,181 | 133 |
| `global_supply.json :: niche_industry_boards` | +29 | +3,600 | 83 |

`country_job_boards` after B: 286 chunks averaging 121 chars (~30 tokens), e.g.
`[global_supply.json] country_job_boards.<Country>.boards[i]` then
`name / billing / category / tier` lines. The country is in the path, so
"job boards in <country>" queries get a lexical and semantic anchor.
`global_supply.json` total under B: +343 chunks.

Variant A, top keys (shows where the list-item micro-chunks land):
`regional_hiring_intelligence :: regions` +1,249, `client_media_plans_kb ::
plans` +1,076, `joveo_publishers :: by_country` +1,051, `platform_intelligence_deep
:: platforms` +652, `h1b_salary_intelligence :: roles` +609, `channels_db ::
traditional_channels` +475, `channels_db :: job_categories` +454,
`global_supply :: country_job_boards` +413. Under A, 3 files change by zero,
under B 8 files change by zero.

Note the relaxation also edits EXISTING chunks (a dict that already had a long
string now also carries its short fields). Under B, 4,342 of 8,008 chunk texts
are byte-identical to today's; the other 3,666 are new or modified.

### Voyage token cost (estimate)

* Model: `_VOYAGE_MODEL` default `voyage-4-lite`, 1024-dim, provider default
  `voyage` (`vector_search.get_embedding_provider`, `EMBEDDING_PROVIDER` unset).
* Tokens are estimated at chars/4 (no Voyage tokenizer installed). The repo's own
  runbook uses the same ratio (~273K tokens for the current 6.2k chunks). Short
  name and number text tokenizes denser than prose, so the chars/3 column is a
  plausible upper bound.
* Pricing, first-party: https://docs.voyageai.com/docs/pricing (retrieved
  2026-11-03): `voyage-4-lite` $0.02 per 1M tokens, first 200M tokens free per
  account. Whether this account's grant is already partly used is not visible
  from here.

| Scenario | Chunks embedded | Tokens chars/4 | Tokens chars/3 | List price if past free grant |
|---|---|---|---|---|
| Today's full corpus (reference) | 6,228 | ~274K | ~366K | $0.0055 |
| B, full reindex of new collection | 8,008 | ~357K | ~476K | $0.0071 to $0.0095 |
| B, delta only (3,666 new/modified texts) | 3,666 | ~181K | ~242K | $0.0036 to $0.0048 |
| A, full reindex | 15,218 | ~521K | ~695K | $0.0104 to $0.0139 |
| A, delta only (10,876 texts) | 10,876 | ~345K | ~460K | $0.0069 to $0.0092 |

All rows sit inside the 200M free grant (largest is 0.35% of it), so the
expected cash cost is $0 and the list price is a ceiling. The practical cost is
time, not money: at the code's `_VOYAGE_RPM_LIMIT = 10` and batch 32, 8,008 chunks
is ~250 requests, roughly 25 minutes (assumes the 10 RPM figure is the binding
limit; the account may allow more).

"Delta only" is a floor, not what the current tooling does. The local
`data/.embedding_cache.json` holds 4,864 entries and none match the
voyage-4-lite document-typed cache key for today's chunks (0 of 6,228), so it
cannot supply the unchanged 4,342 texts, and no repo code copies vectors from the
old Qdrant collection. Plan on the full-reindex row. The eval run below embeds
the corpus once more into `evals/.cache/`, so budget roughly 2x the full-reindex
row in total (still well under 1M tokens).

### Blue/green path the repo supports (and the gap)

Verified in code:

* `vector_search._active_collection()` derives the collection name only from
  provider, model and dim: `nova_knowledge__voyage-4-lite_1024` today
  (`nova_knowledge` is pinned to legacy `voyage-3-lite`). There is NO env var for
  a collection name, no suffix or version hook, and no Qdrant alias code
  (grep of `*.py` and `docs/` for `QDRANT_COLLECTION`, `alias` found none).
  Blue/green here means "different model gets a different collection".
* `scripts/reindex_embeddings.py` rebuilds the ACTIVE collection and by default
  runs `DELETE` then `PUT` on it. Run as-is with a relaxed chunker it would drop
  the live collection. `--no-recreate` is also unsafe for this change: ids are
  `md5("file.json:N")`, and N shifts under the new chunker, so new points would
  overwrite unrelated old chunks in the live collection.
* Production does not self-index. `wsgi.py` gates the in-process build behind
  `NOVA_BUILD_LOCAL_INDEX=1` (off); collections are built offline with the
  reindex script and the app attaches read-only (`warm_retrieval` in the master
  pre-fork, then lazy `_qdrant_attach`). The runbook's "self-migrates on first
  boot" text predates this gating.
* `/api/deploy/ready` (routes/health.py) reports `embedding.collection`,
  `embedding.qdrant_point_count` (live count, the number to poll),
  `embedding.dim_guard`, `embedding.last_embed_error`, `last_qdrant_error`, and
  `retrieval.{qdrant_attached, search_mode, bm25_built, keyword_indexes.bm25_documents,
  keyword_indexes.hybrid_id_text_mismatches, can_retrieve}`.
* The retrieval eval imports the production `vs._extract_text_chunks` and embeds
  in memory; it never queries Qdrant. So it validates the CHUNKER change, not a
  built collection.
* Rollback precedent: flip `VOYAGE_MODEL` or `EMBEDDING_PROVIDER` in the Render
  dashboard (env change restarts the service); the old collection is untouched
  by the new one (runbook sections d and g).

Assumptions (not confirmed in code, need a decision before running):

1. A small code change is required to name a second collection (for example an
   optional `NOVA_QDRANT_COLLECTION_SUFFIX` appended in `_active_collection()`,
   read by both the app and the reindex script). Without it there is no
   non-destructive way to build the new collection with the same model.
   Switching `VOYAGE_MODEL` just to get a new name would mix two changes in one
   A/B and is not recommended.
2. Shipping the relaxed chunker ahead of the flip makes local BM25/TF-IDF ids
   (built from code at boot) differ from the live collection's ids.
   `_separate_drifted_ids` renames mismatches to `bm25-only::...` so hybrid does
   not mis-fuse, but hybrid quality degrades and
   `hybrid_id_text_mismatches` will climb until the flip. Ship the chunker change
   and the collection flip in the same window, or keep the old chunker behind the
   suffix env until the new collection is ready. Not tested here.
3. Qdrant write access, `VOYAGE_API_KEY`, and the 10 RPM assumption must be
   checked by the operator.

### Operator steps

1. Branch with the relaxed chunker (variant B, or A) plus the collection-suffix
   hook from assumption 1. Do not merge yet.
2. Offline gate, no keys needed, compares tiers against the committed baseline:
   `python3 evals/retrieval_eval.py --check` (BM25 and TF-IDF). Golden ids are
   stable `file.json#key.path` ids, so they survive the id shift; run
   `python3 evals/retrieval_eval.py --audit-golden` to confirm they still resolve.
3. Dense gate with a real key (embeds the new corpus into `evals/.cache/`, never
   touches Qdrant):
   `VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage --voyage-rpm 10 --check`
   (`--voyage-batch 8` on an unpaid key). Compare recall@k and MRR per tier and
   per domain against the current baseline; require no tier more than 3 points
   below. Add golden rows for the newly indexable facts first (for example a
   job-board-by-country question), otherwise the eval cannot show the gain.
   Only after a verified-good result: `--update-baseline`.
4. Build the green collection with the new suffix set, from an operator machine:
   `NOVA_QDRANT_COLLECTION_SUFFIX=<tag> QDRANT_URL=... QDRANT_API_KEY=... VOYAGE_API_KEY=... python3 scripts/reindex_embeddings.py`
   (the suffix variable is the assumed hook from assumption 1; the script then
   recreates only the suffixed collection). Optional smoke first: add `--limit 200`.
5. Verify green before serving: Qdrant point count equals the chunk count the
   script logged (about 8,008 for B), vector size 1024, cosine.
6. Flip: set the suffix env in the Render dashboard on the service and deploy the
   chunker branch together (assumption 2). Poll `GET /api/deploy/ready` until
   `embedding.collection` shows the suffixed name, `embedding.qdrant_point_count`
   is the expected count, `embedding.dim_guard` is null, `last_embed_error` and
   `last_qdrant_error` are null, `retrieval.search_mode` is `vector`, and
   `keyword_indexes.hybrid_id_text_mismatches` stays 0. Then run 3 to 5 real
   queries (for example a job-boards-by-country question) end to end.
7. Rollback: unset the suffix env in the dashboard (restarts onto the old
   collection, which was never written) and redeploy or revert the chunker
   commit so BM25 ids match the old collection again. Keep the old collection
   for at least one release cycle before deleting it (deleting is a separate,
   confirmed step).

### Open decisions

* Variant B vs A: B is smaller and avoids micro-chunks, but it is untested on
  dense recall here. The eval in step 3 decides.
* `_verification_notes` (underscore-prefixed) holds the richest board text
  (ownership changes, status) and is skipped by a separate rule. Indexing it is a
  different, larger change and is not costed above.
* Whether to add the suffix hook or a proper Qdrant alias swap (not implemented
  anywhere in the repo today).
