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
