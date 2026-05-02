# Voyage 4 Migration Runbook

**Script:** `scripts/migrate_voyage_4.py` (~919 lines, stdlib only).
**From:** `voyage-3-lite` (current production embedding model, 512-dim).
**To:** `voyage-4-lite` (default) or `voyage-4-large`.
**Scope:** 685 points in Qdrant collection `nova_knowledge`.
**Estimated cost:** ~$0.07 (3.4M tokens × $0.02/M for `voyage-4-lite`).
**Estimated wall time:** ~2.5 minutes for embeddings (free-tier rate-limited at 10 RPM, 6.5s/batch × 22 batches).

---

## Why migrate

| Quality lift | Notes |
|---|---|
| +14.05% over OpenAI v3-large | Voyage-published Jan 2026 benchmark |
| ~6× cheaper queries | Voyage 4 lite + large share an embedding space — index once with large, query with lite |
| Matryoshka truncation | Pick output dim (256 / 512 / 1024 / 2048) at request time without re-indexing |

## Why deferred (not landed in S50)

`voyage-3-lite` and `voyage-4-lite` live in **different embedding spaces**. They are not compatible. Mixing them in a single Qdrant collection is invalid — query similarity scores will be meaningless. All 685 points must be re-embedded together as a single migration, with a backup created up-front so rollback is possible.

The migration script handles this: backup → re-embed in batches → upsert into the same Qdrant collection with the same point IDs (the migration is idempotent — re-running it just re-embeds harmlessly).

---

## Pre-flight checks

### Required env vars

```bash
echo $VOYAGE_API_KEY    # must be set; signs Voyage embedding requests
echo $QDRANT_URL        # full Qdrant cluster URL, no trailing slash
echo $QDRANT_API_KEY    # api-key header for Qdrant
```

If any are blank locally:

```bash
source ~/.zshrc          # if configured in shell rc
```

The script's `_validate_env()` exits with code 1 and a clear message if any are missing.

### Qdrant collection sanity

The script's `_qdrant_collection_info()` runs a `GET /collections/nova_knowledge` and prints:

```
Qdrant collection 'nova_knowledge': points=685  dim=512  distance=Cosine
```

Confirm `points=685` (or whatever your current count is) and `dim=512`. **If `dim != 512`, the migration will refuse to run.** The script enforces `current_dim == output_dim` because changing dimension would require recreating the Qdrant collection.

### Voyage 4 valid output dimensions

Pick from `{256, 512, 1024, 2048}`. The default `512` matches the current Qdrant schema and is the safe choice. Picking a different dim requires recreating the collection, which is out of scope for this migration.

---

## Step 1: Dry run with backup

The dry run logs every action without changing Qdrant. The `--backup` flag forces a backup file to be written so you can inspect contents before committing.

```bash
cd /Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator
python3 scripts/migrate_voyage_4.py --dry-run --backup
```

**Expected log tail:**

```
Voyage 4 Migration -- mode=DRY RUN
  target_model=voyage-4-lite  output_dim=512
======================================================================
Qdrant collection 'nova_knowledge': points=685  dim=512  distance=Cosine
Scrolling all points for backup...
Scrolled page 1 (200 points; total=200)
Scrolled page 2 (200 points; total=400)
Scrolled page 3 (200 points; total=600)
Scrolled page 4 (85 points; total=685)
Backup written: data/qdrant_backup_pre_voyage4_2026-05-02T14-30-00Z.json (685 points)
Embedding 5 sample texts to verify model + output_dim...
Sample test OK: returned 5 embeddings of dim 512.
Re-embedding 685 points in 22 batches (size=32, ~6.5s/batch rate-limited)...
[DRY RUN] Batch 1/22: would embed 32 texts and upsert with same IDs
[DRY RUN] Batch 2/22: would embed 32 texts and upsert with same IDs
... (22 batches total)
[DRY RUN] Would run recall@5 sanity check after migration.
======================================================================
MIGRATION SUMMARY (DRY RUN)
  Target model:        voyage-4-lite
  Output dim:          512
  Points migrated:     685 / 685
  Failed batches:      0
  Elapsed:             0.5s
  Tokens (est):        ~3400000
  Cost (est):          $0.0680 USD ($0.0200/M tokens)
======================================================================
Next steps:
  1. Inspect any backup file under data/.
  2. When ready, re-run with --execute to perform the migration.
```

**Confirm** before proceeding:
- `points=685` matches expectation
- A backup file exists at `data/qdrant_backup_pre_voyage4_<timestamp>.json` (size proportional to point count)
- `Failed batches: 0` (a dry run with failures means the live run will also fail — investigate first)
- Cost estimate is in the expected range (~$0.07 for `voyage-4-lite`, ~$0.40 for `voyage-4-large`)

---

## Step 2: Execute

`--execute` overrides `--dry-run`. A backup is **always** written when `--execute` is set; you cannot skip it.

```bash
python3 scripts/migrate_voyage_4.py --execute
```

**Expected log tail (~2.5 min):**

```
Voyage 4 Migration -- mode=EXECUTE
  target_model=voyage-4-lite  output_dim=512
======================================================================
Qdrant collection 'nova_knowledge': points=685  dim=512  distance=Cosine
Scrolling all points for backup...
Backup written: data/qdrant_backup_pre_voyage4_2026-05-02T14-30-00Z.json (685 points)
Embedding 5 sample texts to verify model + output_dim...
Sample test OK: returned 5 embeddings of dim 512.
Re-embedding 685 points in 22 batches (size=32, ~6.5s/batch rate-limited)...
Batch 1/22: upserted 32 points (running total=32)
Batch 2/22: upserted 32 points (running total=64)
... (each batch ~6.5s apart due to free-tier rate limit)
Batch 22/22: upserted 13 points (running total=685)
Running recall@5 sanity check with 3 queries...
Top-5 results per query (new model):
  Q: recruitment marketing benchmarks 2026
    1. id=... score=0.87xx preview=...
    2. id=... score=0.85xx preview=...
    ...
  Q: Indeed cost per click programmatic
    ...
  Q: LinkedIn applicant rate Easy Apply versus ATS
    ...
======================================================================
MIGRATION SUMMARY (EXECUTE)
  Target model:        voyage-4-lite
  Output dim:          512
  Points migrated:     685 / 685
  Failed batches:      0
  Elapsed:             148.3s (2.5 min)
  Tokens (est):        ~3400000
  Cost (est):          $0.0680 USD ($0.0200/M tokens)
======================================================================
Next steps:
  1. Update vector_search.py: _VOYAGE_MODEL = "voyage-4-lite"
  2. Add output_dimension=512 to Voyage embed payload.
  3. Run nova-test golden eval and verify recall.
  4. Deploy. If issues arise, --rollback restores from backup.
```

**Critical:** Migration exits with code:
- `0` if all batches succeeded
- `1` if a fatal pre-flight failure occurred (env, Qdrant unreachable, sample embed dim mismatch)
- `2` if some batches failed — Qdrant is in a partially-migrated state. Re-run `--execute` (idempotent: already-migrated points are re-embedded harmlessly).

If `Failed batches > 0`: do **not** flip `VOYAGE_MODEL` until you re-run and reach 0 failures.

---

## Step 3: Verify recall@5

The script runs three sanity-check queries automatically at the end of `--execute` (and prints results). The queries are hardcoded in `migrate_voyage_4.py`:

```
"recruitment marketing benchmarks 2026"
"Indeed cost per click programmatic"
"LinkedIn applicant rate Easy Apply versus ATS"
```

**First run:** the script writes the new top-5 IDs to `data/recall_baseline_voyage3.json` as the new baseline (because no baseline existed previously). This is a one-time bootstrap.

**Subsequent runs:** the script reads the baseline and reports overlap per query:

```
Comparing against baseline data/recall_baseline_voyage3.json ...
  Q: recruitment marketing benchmarks 2026 -- recall@5 overlap with baseline: 4/5
  Q: Indeed cost per click programmatic -- recall@5 overlap with baseline: 5/5
  Q: LinkedIn applicant rate Easy Apply versus ATS -- recall@5 overlap with baseline: 4/5
```

**Interpretation:**

| Overlap (avg across 3 queries) | Verdict |
|---|---|
| 5/5 across all 3 | Migration recovered identical top-5 — proceed with cutover |
| 4/5 across all 3 | Acceptable — different model, expected minor reordering |
| ≥3/5 across all 3 | Marginal — run `nova-test` golden eval before flipping `VOYAGE_MODEL` |
| <3/5 on any query | Stop. Investigate. Possible causes: text payload missing on points (script logs `Point id=... has no payload.text`), Voyage 4 model outage, output_dim mismatch |

To establish a **fresh** baseline against the new index after cutover, delete `data/recall_baseline_voyage3.json` and re-run the script — but only do so **after** confirming the overlap above is healthy on the existing baseline.

---

## Step 4: Cutover

The script intentionally does **not** modify `vector_search.py`. After Qdrant holds Voyage 4 vectors, you flip the runtime model string. Two equivalent paths:

### Option A: env-var-only cutover (recommended for first deploy)

Add to Render env (no redeploy needed; service restart picks it up):

```bash
VOYAGE_MODEL=voyage-4-lite
```

Then in `vector_search.py:73`, the existing module-level constant `_VOYAGE_MODEL = "voyage-3-lite"` would need to be wrapped in `os.environ.get("VOYAGE_MODEL") or "voyage-3-lite"` for this to take effect — confirm whether the env-var hook is wired before relying on this path.

### Option B: source-code cutover

Edit `vector_search.py:73`:

```python
# Before:
_VOYAGE_MODEL = "voyage-3-lite"
# After:
_VOYAGE_MODEL = "voyage-4-lite"
```

If `voyage-4-lite` requires `output_dimension=512` in the embed payload (Voyage 4 supports Matryoshka truncation), update the embed call to include this parameter. Confirm against current Voyage docs at migration time.

Commit, push, wait for Render auto-deploy. Verify via `/api/health` admin endpoint that the new model is live.

---

## Step 5: Monitor

Run the chatbot eval suite immediately after cutover:

```bash
# Golden eval (validates retrieval quality end-to-end)
python3 tests/test_nova_eval.py

# Or if registered as a slash command in your environment
/nova-test
```

**Expected:**
- Retrieval relevance scores within 5% of pre-migration baseline
- No new "no relevant context found" errors in logs
- p99 chat latency unchanged (Voyage 4 lite is comparable in latency to v3 lite)

**Sentry / log monitoring** for the first 1–2 hours:
- Watch for any `voyage-4-lite` 404 or 429 spikes
- Watch the embedding cache hit rate — first hour will be cold (cache is keyed by hash of the text, but the cache contains old voyage-3 vectors). Cache will repopulate naturally as new queries come in.

---

## Rollback

The script supports a `--rollback` mode that restores Qdrant from any backup file. Time-to-rollback: ~30 seconds (Qdrant upserts are not Voyage-rate-limited).

```bash
# Dry run first (always)
python3 scripts/migrate_voyage_4.py --rollback \
    --rollback-from data/qdrant_backup_pre_voyage4_2026-05-02T14-30-00Z.json

# Then execute
python3 scripts/migrate_voyage_4.py --rollback \
    --rollback-from data/qdrant_backup_pre_voyage4_2026-05-02T14-30-00Z.json \
    --execute
```

**Expected rollback log tail:**

```
Voyage 4 Migration ROLLBACK -- mode=EXECUTE
  backup_file=data/qdrant_backup_pre_voyage4_2026-05-02T14-30-00Z.json
======================================================================
Loaded backup: data/qdrant_backup_pre_voyage4_<TS>.json (685 points, source=voyage-3-lite)
Rollback batch 1/7: restored 100 points (total=100)
Rollback batch 2/7: restored 100 points (total=200)
... (7 batches at 100/batch)
Rollback batch 7/7: restored 85 points (total=685)
======================================================================
ROLLBACK SUMMARY (EXECUTE)
  Points restored:     685 / 685
  Failed batches:      0
  Elapsed:             27.4s
======================================================================
Rollback complete. Revert vector_search.py to voyage-3-lite as well.
```

**After rollback:**
1. Revert `vector_search.py:73` to `_VOYAGE_MODEL = "voyage-3-lite"` (or unset `VOYAGE_MODEL` env var if you used the env-var path).
2. Redeploy.
3. Verify `/api/health` reports `voyage-3-lite` as the active embedding model.
4. Re-run `tests/test_nova_eval.py` to confirm relevance scores recovered.

---

## Script reference

```
$ python3 scripts/migrate_voyage_4.py --help
usage: migrate_voyage_4.py [-h] [--dry-run] [--execute] [--backup] [--rollback]
                            [--rollback-from ROLLBACK_FROM]
                            [--target-model {voyage-4-large,voyage-4-lite}]
                            [--output-dim OUTPUT_DIM]
                            [--baseline-path BASELINE_PATH]

Migrate Qdrant 'nova_knowledge' index from voyage-3-lite to voyage-4-{lite,large}.
Safe by default (dry-run). Use --execute to actually mutate. Use --rollback to
restore from a backup.
```

| Flag | Default | Notes |
|---|---|---|
| `--dry-run` | true | DEFAULT. Logs every action without writing. |
| `--execute` | false | Performs the mutation. Always backs up first. |
| `--backup` | false | Force-write a backup even on dry run. Always on with `--execute`. |
| `--rollback` | false | Switches mode to rollback (requires `--rollback-from`). |
| `--rollback-from PATH` | "" | Required with `--rollback`. Path to a backup JSON. |
| `--target-model NAME` | `voyage-4-lite` | Choices: `voyage-4-lite` ($0.02/M), `voyage-4-large` ($0.12/M). |
| `--output-dim N` | 512 | Matryoshka truncation. Must match Qdrant collection vector size. Valid: `{256, 512, 1024, 2048}`. |
| `--baseline-path PATH` | `data/recall_baseline_voyage3.json` | Where to read/write the recall@5 baseline. |

---

## Failure modes and recovery

| Failure | Symptom | Recovery |
|---|---|---|
| Voyage 429 mid-batch | `Voyage API HTTPError 429 ... rate limit` in logs | Script raises `RuntimeError`, increments `failed_batches`, continues. Re-run `--execute` after cooldown. Migration is idempotent. |
| Qdrant connection drop | `Qdrant API error ... ` in logs, batch upsert returns False | Increments `failed_batches`. Re-run `--execute`. |
| Sample embed dim mismatch | `Sample embedding dim mismatch: expected 512, got 1024. Aborting.` | Pass `--output-dim 1024` (or whatever Voyage returned), or recreate the Qdrant collection with the new dim. |
| Empty payload text | `Point id=... has no payload.text or payload.content; skipping` | These points are silently skipped. Inspect Qdrant payloads — should not happen with the current `kb_loader` indexing path. |
| Backup file missing on rollback | `Backup file not found: <path>` | Pass an absolute path or check `data/qdrant_backup_pre_voyage4_*.json` glob to find an alternative. |

---

## Voyage 4 pricing reference

From `migrate_voyage_4.py:60`:

```python
VOYAGE_4_PRICING = {
    "voyage-4-lite": 0.02,    # USD per 1M tokens
    "voyage-4-large": 0.12,
}
```

For 685 points × ~5K tokens each (typical Nova KB chunk size):
- `voyage-4-lite`: ~3.4M tokens × $0.02/M = ~$0.07
- `voyage-4-large`: ~3.4M tokens × $0.12/M = ~$0.41

`voyage-4-large` is recommended only if you plan to leverage the shared embedding space (index with large, query with lite at ~6× cheaper query cost). For a one-shot migration where Nova's query embeddings will continue to use lite, picking `voyage-4-lite` for both index and query is the cheaper, simpler path.
