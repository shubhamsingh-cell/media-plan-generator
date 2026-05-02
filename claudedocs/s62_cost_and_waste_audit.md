# S62 Cost & Waste Audit — Nightly OOMs + $50/mo Render Bill

**Date**: 2026-04-24
**Scope**: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/`
**Symptoms**: 11 "exceeded memory limit" emails in 27 days, $18.60 bandwidth (124 GB), $19.46 service cost.
**Mode**: Read-only. No source changes, no commits.

---

## TL;DR — The Single Biggest-Saving Fix

**Disable inline injection of `/static/nova-chat.js` (176 KB) on every HTML response and serve it from the same `/static/js/*.js` path that already has ETag + 1y immutable caching.**

- `app.py:20603` defines `_NOVA_WIDGET_SNIPPET = b'<script src="/static/nova-chat.js?v=3.5.3"></script>'`, but this path is NOT served by the long-cache static branch (`app.py:10728` only caches `.js`/`.css` files under `/static/js/` and `/static/css/`). `nova-chat.js` lives at `/static/nova-chat.js` and is served through the template path with `Cache-Control: no-cache, no-store, must-revalidate` (app.py:20803, 20810).
- Every HTML pageview re-downloads the full 176 KB widget plus 80–190 KB of HTML.
- Combined with the 124 GB/mo cost, a single round-trip of `nova-chat.js` per visit accounts for ~40–50 % of egress.
- **Fix**: (a) move `nova-chat.js` under `/static/js/nova-chat.js` (or add its directory to the long-cache allowlist), (b) remove `no-cache, no-store, must-revalidate` from HTML template responses and replace with `private, max-age=300, must-revalidate` — HTML is small but ships on every click-through today.

Expected savings: **~60–70 GB/mo egress ($9–$11/mo)**. Zero functional risk.

---

## Part 1 — Nightly OOMs: Top 3 Suspects (ranked)

The symptom is not actually a scheduled nightly batch job (all the 24 h daemons -- `precompute.py`, `data_refresh.py`, `nova_proactive.py`, `feature_store.py` -- are already disabled in `wsgi.py:157-197` under S50). The "nightly" pattern is a slow-leak pattern that crosses Render's ceiling after ~12–24 h of quiet uptime (no `--max-requests` recycle because traffic is low). Evidence below is ranked by confidence.

### #1 (HIGH) — Unbounded `_embedding_cache` in `vector_search.py`

**Evidence**:
- `vector_search.py:95` `_embedding_cache: dict[str, list[float]] = {}` — module-level dict, no max size, no LRU.
- `vector_search.py:668-674` every un-cached embedding is unconditionally stored; no eviction.
- Grep for `_embedding_cache.pop|evict|prune|maxsize|LRU`: **zero hits** except for `len()` in the status endpoint.
- On-disk mirror is already 30.9 MB (`data/.embedding_cache.json`, mtime Apr 3). That 30.9 MB is loaded into RAM at startup via `_load_embedding_cache()` (line 151-183) and then every new chat query, KB chunk, and tool invocation appends more entries.
- Every write also spawns a fresh daemon thread (line 679) to re-serialize the whole dict to disk — N threads per minute under load, plus the dict cost.
- Two gunicorn workers × ~30 MB baseline + organic growth explains the multi-day ramp to OOM.

**Fix**: add an `OrderedDict` with max ~5000 entries, move-to-end on access, pop oldest on insert. Persist to disk once every 5 min instead of on every write. Expected savings per worker: **40–80 MB steady state**, eliminates thread spam.

### #2 (MEDIUM) — In-memory vector `_index` + `_tfidf_index` held at full fidelity

**Evidence**:
- `vector_search.py:429` `_index: list[dict] = []` stores `{"id", "text", "embedding", "metadata"}` — full text plus full 1024-dim Voyage embedding plus metadata per chunk.
- `vector_search.py:1633` parallel `_tfidf_index` holds per-chunk term-frequency dicts.
- `wsgi.py:137-142` explicitly warns at runtime: `if _rss_vi_mb > 400: MEMORY PRESSURE: peak RSS exceeds 400 MB threshold after vector index build.`
- KB reload in `kb_loader.py:709-710` calls `live_kb.clear(); live_kb.update(kb_updated)` every 5 min — which forces GC churn on the ~4 MB merged dict, but the vector `_index` is never pruned or rebuilt with the new data, so stale chunks accumulate over long uptime if any pathway re-indexes.

**Fix**: (a) don't store raw `text` in `_index` — keep only `id` + metadata, fetch text from KB on display. (b) quantize embeddings to int8 (4× shrink, negligible recall loss). Expected savings: **60-100 MB per worker**.

### #3 (LOW-MEDIUM) — Thread-spawn per event (`plan_events.py`, `nova_memory.py`, `slack_plan_notifier.py`, `vector_search` cache saves)

**Evidence**:
- `plan_events.py:481-486`: every plan event spawns a new `threading.Thread` for Supabase persistence.
- `nova_memory.py:139, 162, 175`: every memory write spawns a new thread.
- `vector_search.py:679`: every embedding cache write spawns a new thread.
- `nova_cache.py:635, 649`: every Supabase write spawns a new thread.
- Under gevent monkey-patching (`wsgi.py:30-32`) these are greenlets, so the thread cost is small, but each one captures closure state (the entry dict) and keeps it alive until Supabase ACKs (10–30 s under load). A few hundred in-flight greenlets × KB-sized payloads is measurable.

**Fix**: replace per-event threads with a single-consumer `queue.Queue` + one dedicated writer greenlet per module (4–6 workers total instead of hundreds). Expected savings: **10–30 MB** and lower tail latency.

---

## Part 2 — Bandwidth: Top 5 Consumers (124 GB / mo = ~4 GB/day)

### #1 — HTML templates forced `no-cache, no-store, must-revalidate` (app.py:20803, 20810, 20842)

- Templates range 68 KB–192 KB: `vendor-iq.html 192 KB`, `slotops.html 188 KB`, `dashboard.html 140 KB`, `hub.html 136 KB`.
- Zero browser caching. Every in-session navigation re-downloads the full shell, even though HTML changes only on deploys.
- **Fix**: `Cache-Control: private, max-age=300` + `ETag`. Savings **~20–30 GB/mo**.

### #2 — `nova-chat.js` (176 KB) injected on every page served through the no-cache path

- `app.py:20603` inlines a `<script src="/static/nova-chat.js?v=3.5.3">` tag into every HTML response where the widget isn't already present.
- `/static/nova-chat.js` is served from a path that is **not** covered by the `/static/js/*.js` long-cache branch (app.py:10728) — the `.js` allowlist there applies to files under `/static/js/`, but `nova-chat.js` is at `/static/nova-chat.js` (one level up).
- Result: 176 KB re-downloaded on every first pageview.
- **Fix**: move the file to `/static/js/nova-chat.js` (or extend the caching branch to match the root-level file). The ETag path already exists — it just isn't reached. Savings **~30-50 GB/mo** (biggest single win).

### #3 — SSE / streaming chat endpoints with verbose payloads

- `/api/chat/stream` and `/api/generate` return large bodies — the user's note cited 65 KB per healthcare-map chat response and 122 KB ZIP per plan generate. At only 600 plan-generations + 2000 chat calls per day that is already ~450 MB/day.
- No per-route bandwidth metric in the code to tell which endpoint is the outlier.
- **Fix**: enable gzip (confirmed present for templates at app.py:20796; verify it's also applied to JSON SSE — line-search showed inconsistent use). Measure via Render's response-size logs. Savings **~10-20 GB/mo** if JSON responses aren't already gzipped.

### #4 — `/api/docs/openapi.json` (1 h cache) + full OpenAPI spec being re-fetched

- `app.py:10762-10769` serves the full spec (`_OPENAPI_SPEC`) with `public, max-age=3600`. Fine for the spec but the spec is rebuilt on every request (`json.dumps(_OPENAPI_SPEC, indent=2)` rather than serving a pre-serialized bytes cache).
- Not a bandwidth hit directly; more a CPU hit. Skip unless profiling shows it.

### #5 — Uptime-probe + health-check traffic

- `/api/health`, `/api/health/ready`, `/api/health/ping`, `/api/health/data-matrix`, `/api/health/auto-qc`, `/api/deploy/ready` — six liveness/readiness endpoints.
- Render itself probes `/health` roughly every 5 s. That's ~518 K pings/mo. Even at 300 bytes each that's ~150 MB, but some of these (`/api/health/ready`) return multi-KB diagnostic JSON.
- **Fix**: point Render's healthcheck at the lightest endpoint (`/api/health/ping` is already optimized for this) and return `Content-Length: 2` `ok`. Savings **~5-15 GB/mo**.

**Can we downgrade to Starter ($7 instead of $19.46)?** Not today — Starter is 512 MB. Even after the embedding-cache fix you will be at ~350-400 MB per worker on 2 workers. Drop to **1 worker + gevent** (plenty for current traffic; gevent gives hundreds of greenlets of concurrency in a single process) and Starter is feasible. Savings **$12.46/mo**.

---

## Part 3 — Dead Code / Bloat (Top 10)

Rank is by disk size × confidence it's unused. "Refs" is the number of `(^|[^a-z_])(from|import) <module>\b` matches across production code (excluding `tests/` and `archive/`).

| # | File | Size | Refs in prod | Delete-safe | Evidence |
|---|---|---|---|---|---|
| 1 | `chroma_rag.py` | 20 K | **0** | YES | Only appears as a `_chroma_rag_available = False` flag in app.py:266 and status checks. Module never imported. |
| 2 | `competitive_intel.py` | 108 K | 5 | NO (has refs) | Needs verification of each ref — may be all lazy + unreachable. |
| 3 | `data_synthesizer.py` | 192 K | 5 | NO (has refs) | Same as above — biggest file worth checking. |
| 4 | `applyflow.py` | 64 K | 1 (lazy, line 19591) | LIKELY YES | Only one lazy import inside a POST handler; the route (applyflow-demo) exists but whole widget is unlaunched per hub. |
| 5 | `calendar_sync.py` | 16 K | 2 | LIKELY YES | Already flagged "DISABLED S50" in wsgi.py:222-228. Comment says "not used by core products." |
| 6 | `feature_store.py` | 20 K | 8 | CONDITIONAL | Already disabled in wsgi.py:190-197 but refs remain in app.py. Needs trace; removal likely safe. |
| 7 | `creative_quality_score.py` | 12 K | 3 | LIKELY YES | Scan refs; if only from optional scoring endpoints. |
| 8 | `canvas_engine.py` | 16 K | 6 | NO (routes/canvas.py active) | Keep. |
| 9 | `ats_widget.py` | 4 K | 3 | CHECK | Small, but may be dead. |
| 10 | `archive/` directory | 528 K | 0 (import guard) | YES | Whole directory is archival. Ships into the slug and loads into Python's module path. Delete from deploy. |

Additional delete-safe bloat:
- `data/backups/` = **17 MB** of `kb_backup_*.zip` (7 files from March-April). Ship these out of the slug (ignore list / `.renderignore`). Zero savings on RAM but cuts deploy size.
- `data/generated_docs/` = **3.7 MB** / 63 test ZIPs (e.g., `20260307_161117_Client.zip`, `TestCorp6.zip`). Obvious test fixtures in production build.
- `data/.embedding_cache.json` = **30.9 MB** — should be on the persistent disk (`/data/persistent/`) not in the slug. Currently `vector_search.py:89-93` falls back to `data/.embedding_cache.json` when `/data/persistent` is missing, and that fallback IS what ships.
- `data/slotops_baseline_data.json` = **7 MB** — valid product data but loaded into process memory if used by the KB loader; verify if it needs to be in-memory or can be lazy-loaded.
- `data/auto_qc_results.json` = **588 K** growing file.

---

## The Three Sharpest Findings (3 lines each)

**1. Memory leak root cause is `_embedding_cache` in `vector_search.py:95`.**
It's an unbounded module-level dict with no eviction, currently 30.9 MB on disk and growing every chat query; reloaded fully into RAM at worker startup and persisted on every write via a fresh daemon thread. Add an LRU cap of 5 K entries + batched flush; this single change should eliminate the nightly OOMs. Evidence: `vector_search.py:95, 668-674, 679`; disk file `data/.embedding_cache.json` = 30.9 MB; `wsgi.py:137-142` already warns about 400 MB RSS threshold.

**2. The 124 GB/mo bandwidth is driven by `nova-chat.js` being served through the no-cache HTML path, not the long-cache `/static/js/` branch.**
`app.py:20603` injects `<script src="/static/nova-chat.js">` into every HTML response, but the file lives at the slug root (not `/static/js/nova-chat.js`), so the caching allowlist at `app.py:10728` skips it and each 176 KB request goes to origin. Moving the file to `/static/js/` (or extending the allowlist) should save 30-50 GB/mo. Evidence: `app.py:20603` and `app.py:10728`.

**3. HTML templates are served with `Cache-Control: no-cache, no-store, must-revalidate` even though they change only on deploy.**
Every navigation re-downloads 68-192 KB HTML. `app.py:20803, 20810`. Changing to `private, max-age=300` plus ETag saves 20-30 GB/mo; zero functional risk because each response already computes a gzip + Content-Length and the backend only mutates templates on container restart.

---

## One Change That Saves the Most Money This Month

**Move `static/nova-chat.js` under `static/js/nova-chat.js`** (one `git mv` + update the script tag at `app.py:20603` from `/static/nova-chat.js` to `/static/js/nova-chat.js`). This flips the widget from "no-cache re-download on every pageview" to "1-year immutable cache with ETag revalidation" (app.py:10728-10731 already implements that for `/static/js/`). Expected bandwidth drop: **30-50 GB/mo → $5-8/mo saved**. Zero deploy risk; one-file change. After this lands, fix the `_embedding_cache` leak (#1 above) to stop the nightly OOMs, then drop to 1 worker on Render Starter for another $12/mo.

**Total realistic monthly savings from all three fixes: $18-22/mo (~40% of the current bill), plus the OOM emails stop.**
