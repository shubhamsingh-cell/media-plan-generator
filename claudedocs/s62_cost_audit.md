# S62 Cost-Reduction + Dead-Code Audit — Nova AI Suite

**Date**: 2026-04-24
**Scope**: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/`
**User symptom**: "Render is costing a lot" + "I see this happening at night close to daily" (nightly spike)
**Mode**: Read-only. No files modified, no commits.
**Complementary report**: `s62_cost_and_waste_audit.md` (memory + bandwidth focus). This report focuses on the **nightly spike** + cost ranking.

---

## TL;DR

**Nightly spike (single-sentence hypothesis)**: `data_enrichment.py` runs every hour and triggers a cascade of **14 external-API tasks** (Firecrawl, BLS, FRED, Adzuna, Census, 5 job boards, 10 metros). With a `_INITIAL_DELAY = 300s` and `ENRICHMENT_INTERVAL = 3600s`, whenever the worker starts during daytime local, the 12h-stale sources (`live_market_data`, `market_trends`, `job_posting_volume`, `job_density`) all come due together ~12h later — right in the middle of the night — and the cycle fires 6+ Firecrawl/Apify scrapes + LLM salary enrichment in a single pass. That convergence is the spike.

**Top 3 cuts**:
1. Disable `data_enrichment.start()` (app.py:5759). Saves ~$8-15/mo (Firecrawl calls + LLM tokens + RAM) with zero product impact because Firecrawl is already at **0/500 credits dead** and every other call is redundantly cached.
2. Evict `_embedding_cache` LRU + stop per-write daemon threads in `vector_search.py:95, 668-679`. Saves ~$12/mo by letting you downgrade from 2 workers → 1 worker (Standard → Starter feasible).
3. Delete or `.renderignore` the `data/backups/` 17MB + `data/generated_docs/` 3.7MB + `data/.embedding_cache.json` 30MB slug bloat. Saves ~$2/mo egress + deploy time.

---

## A. Likely nightly-spike culprits

### Background loops still running (post-S50 disables)

| Loop | File:Line | Interval | Sleep starts at | Nightly-pattern? |
|---|---|---|---|---|
| **data_enrichment (14 tasks)** | `data_enrichment.py:55, 2084, 2110` | **3600s (1h)**, first run @ startup+300s | bootstrap | **YES — 12h-stale sources converge ~12h after startup** |
| data_matrix_monitor (47 health probes) | `data_matrix_monitor.py:43, 424` | 12h, first run @ startup+300s | bootstrap+5min | **YES — runs exactly twice/day, second run = 12h after first** |
| auto_qc (5-endpoint probe) | `auto_qc.py:27` | **60s**, not 12h (misnamed in startup log) | bootstrap+90s | no (low cost, 5 probes/min) |
| monitoring alert bridge | `monitoring.py` | 60s | bootstrap | no (local in-proc checks) |
| supabase_cache cleanup | `supabase_cache.py:853, 865` | **6h** | bootstrap | maybe — hits every 6h = 4×/day, one run always at night |
| KB backup | `app.py:5790, 5815` | **24h** | bootstrap+86400s | **YES — fires exactly 24h after startup (daily at same wall-clock time)** |
| Qdrant keep-alive ping | `app.py:5826, 5855` | **12h**, first run @ startup+60s | bootstrap | **YES — same 12h convergence pattern** |
| rate limiter cleanup | `app.py:6162` | 5min | bootstrap | no |
| async job cleanup | `app.py:4845` | 5min | bootstrap | no |
| kb hot-reload | `kb_loader.py:847` | 5min (mtime check) | bootstrap | no (file-mtime driven) |
| nova_cache prewarm | `nova_cache.py:903` | one-shot @ startup | bootstrap | no |
| llm_router L3 queue worker | `llm_router.py:392, 492` | event-driven, not timed | n/a | no |
| posthog flush | `posthog_tracker.py:44` | 10s | bootstrap | no (small payloads) |
| cpc monitor | `app.py:4982` | continuous | bootstrap | unknown — check `_monitor_cpc_changes` |
| chat-thread cleanup sweep | `nova.py:186, 220` | 60s | bootstrap | no |
| nova_slack token refresh | `nova_slack.py:43, 514` | 30min | bootstrap | no |
| nova_cache L2 persist | `nova_cache.py` (daemon on each write) | event-driven | n/a | no |

### The math that explains "daily at night"

- If Render Standard worker restarts at **10:00 local** (common -- after morning deploys), the following loops converge at **22:00 local** = night:
  - **Data matrix monitor** 12h: runs at 10:05 + 22:05 → heavy (47 URL probes + hash checks + Supabase writes)
  - **Qdrant keep-alive** 12h: runs at 10:01 + 22:01 → lightweight but adds to pile
  - **data_enrichment** hourly: the 12h-stale sources (`live_market_data`, `market_trends`, `job_posting_volume`, `job_density`) all become stale around 22:05 and the full cascade fires on the 22:05 cycle (= ~10 Firecrawl API calls + LLM salary enrichment + Adzuna + BLS in a single pass).
  - **Supabase cleanup** 6h: runs at 10:00 + 16:00 + 22:00 + 04:00 → one of the 4 runs always lands at night.

This is a **reconvergence pattern** — multiple N-hour timers that started together come due together forever. Whether the spike is at 22:00, 02:00, or 04:00 depends on the worker's last restart time. When the user reports "close to daily at night" that's the 12h convergence producing the tall bar roughly every 24h (since it also runs during the day, but the day one is masked by user traffic; the night one stands out as a flat baseline + spike).

**Confirmation test (do NOT need code changes, just logs):** search Render logs for `Data enrichment cycle starting...` and `DataMatrixMonitor: running check`. They should line up with the spike timestamps.

### The specific data_enrichment cascade

`data_enrichment.py:1964-1981` runs **14 tasks in sequence per cycle**:

```python
live_market_data       # 12h stale -> Firecrawl /scrape calls
firecrawl_news         # 6h stale  -> Firecrawl /search calls
bls_salary             # 7d stale  -> BLS API
fred_economic          # 7d stale  -> FRED API
adzuna_jobs            # 7d stale  -> Adzuna API
census_demographics    # 30d stale -> Census API
compliance_updates     # 7d stale  -> Firecrawl
firecrawl_salary       # 7d stale  -> Firecrawl
job_posting_volume     # 12h stale -> LinkUp/Revelio/Firecrawl
job_density            # 12h stale -> Firecrawl
platform_ad_specs      # 7d stale  -> Firecrawl
competitor_analysis    # 7d stale  -> Firecrawl career-page scrape
benchmark_drift_check  # 90d stale -> internal diff
```

**Cost math on a convergence night**: 10 Firecrawl calls + 3 LLM calls for salary synthesis + 10 Adzuna calls + Supabase writes. With Firecrawl at **0 credits** (per user note + `firecrawl_enrichment.py:64`), those 10 calls all 402-error and cycle through the cooldown. But the Adzuna/BLS/FRED/Census calls still consume quota and CPU. A single convergence cycle can be 30-60 seconds of continuous API burst + Supabase writes per worker × 2 workers.

---

## B. Top 10 Savings Ranked by $/month

| # | Item | Evidence | $/mo saved | Effort | Risk |
|---|---|---|---|---|---|
| **1** | **Disable `data_enrichment.start()`** | `app.py:5759`. Runs 14 tasks hourly. Firecrawl=0 credits (dead), BLS/FRED/Adzuna/Census are L3-cached redundantly in `supabase_cache` (24h TTL), so the enrichment run is mostly redundant cache-fill. | **$8-15** (Firecrawl subscription if re-enabled + Adzuna rate-limit headroom + LLM salary tokens + RAM held for 14 source states) | tiny (1-line comment) | low (Nova chatbot still falls back to Tavily / cached data) |
| **2** | **Evict `_embedding_cache` + batch flush** | `vector_search.py:95` unbounded dict, 30MB on disk (`data/.embedding_cache.json`) loaded into RAM; `line 679` spawns fresh daemon thread **per write**. See S62 memory audit. | **$12** (enables drop to 1 worker → Starter plan $7, from $19.46) | small (20 LOC OrderedDict LRU) | low (cache re-warms in 5 min) |
| **3** | **Kill KB-backup timer (or move to cron)** | `app.py:5790, 5815` runs every 24h in-process, zips all KB JSON to `data/backups/`. 17MB dir already committed to slug. Never pruned. | **$2** (slug size + deploy bandwidth + RAM holding zip during backup) | tiny (remove `_kb_backup_timer.start()`; manual `git commit` of KB already preserves history) | none |
| **4** | **Disable data_matrix_monitor or stretch to 24h** | `data_matrix_monitor.py:43` runs 47 probes every 12h. Useful in dev; in prod, Render healthchecks + auto_qc already cover it. | **$2-3** (47× URL probes × 2/day × 2 workers = 188 probes/day + Supabase writes) | tiny (stretch `_CHECK_INTERVAL = 24*3600` or disable in wsgi) | low (auto_qc catches endpoint failures anyway) |
| **5** | **`.renderignore` `data/backups/`, `data/generated_docs/`, `archive/`** | 17MB + 3.7MB + 528K of slug bloat. Egress on every deploy; also part of the 124 GB/mo bandwidth. | **$2-3** egress/deploy | tiny (add 3 lines) | none |
| **6** | **Move `data/.embedding_cache.json` (30MB) to persistent disk** | `vector_search.py:89-93` already has fallback path `/data/persistent/`; current deploy ships the 30MB JSON in the slug. | **$1-2** | tiny (set `RENDER_DISK_PATH=/data/persistent`) | none |
| **7** | **Drop Supabase cleanup to 24h (from 6h)** | `app.py:5782` `interval_hours=6` = 4 runs/day × Supabase query cost. Cleanup is idempotent. | **$0.50-1** | tiny (change `6` to `24`) | none |
| **8** | **Firecrawl dead-code cleanup** | Firecrawl at 0 credits. `firecrawl_enrichment.py` (60KB) and all 7 call sites in `data_enrichment.py` short-circuit via `_firecrawl_disabled_until`. If not re-funding, delete the module; if re-funding, audit the 6h stale setting — that's too frequent. | **$0** (module already disabled at runtime) — saves CPU cycles on cooldown checks + slug size | small (delete module + imports) | low (already no-op) |
| **9** | **Gzip JSON responses on `/api/chat/stream` and `/api/generate`** | S62 bandwidth audit flagged inconsistent gzip. | **$3-5** (egress) | small | none |
| **10** | **Cache LLM responses more aggressively (task-level)** | `llm_router.py:380-395` in-memory LRU of 200 + Upstash L3. But `_CACHE_TTL_REALTIME_SECONDS` is 5min for chatbot-ish tasks — many chat queries are repeat questions that could share 1h+ cache. | **$3-8** (Anthropic Haiku tokens — primary provider) | small (extend TTL taxonomy) | low (response drift on real-time data still covered by 5min bucket) |

**Sub-total realistic**: **$30-45/mo** (over a $40-50 Render bill).

---

## C. Dead Code Inventory

"Referenced at runtime" = the code path actually executes on a user request (chat or generate). "Imported" = just an `import` or `from X import Y` that may be dead-branch.

| file:line | symbol | imported_in | called_at_runtime? | safe_to_delete? |
|---|---|---|---|---|
| `resilience_router.py:2278` | `resilient_fetch()` | `tests/test_resilience_router.py:35` only. `app.py:5225` and `routes/health.py:221, 246` only import `get_router` / `get_resilience_summary` — never `resilient_fetch`. | **NO** — zero prod call sites | YES (delete the function, keep the router class) |
| `chroma_rag.py` (entire module, 544 LOC, 20KB) | all | Never imported. `app.py:266` has `_chroma_rag_available = False` constant but the module itself is never loaded. | NO | **YES** — delete entire file |
| `auto_feedback_trainer.py` | (does not exist) | `claudedocs/s56_self_healing_check.md:27` confirms "DOES NOT EXIST". User's memory of building one is incorrect. | N/A | N/A (nothing to delete) |
| `anomaly_detector.py:295-324` | `get_anomaly_detector`, `record_metric`, `check_anomaly`, `get_baselines` | `nova.py:10646-10659` (1 chatbot tool), `app.py:9965, 12121`, `routes/health.py:1490-1515` | YES (sparse — 4 call sites) | NO — keep but review if worth the code for 1 chatbot tool; 300 LOC for anomaly detection that fires on 1 of 4 latency metrics (`METRIC_REQUEST_LATENCY`) is fine |
| `feature_store.py` (581 LOC, 20KB) | all | `app.py:5422, 5759`-style lazy imports + disabled in `wsgi.py:190-197` comment. `excel_v2.py:4019` and `campaign_optimizer.py:35, 205` do use it for `INDUSTRY_SEASONAL_CPA`. | YES (sparse, 3 prod call sites after S50 disables) | NO — used by excel_v2/campaign_optimizer; extract just the `INDUSTRY_SEASONAL_CPA` constant and delete the rest |
| `nova_proactive.py` (306 LOC) | `start_proactive_engine`, `get_insights`, `get_unread_insights`, `mark_insight_read`, `dismiss_insight`, `get_proactive_stats` | `app.py:11637, 11651, 20294, 20308, 21065` + `wsgi.py:181` commented-out | Start_proactive_engine is disabled in wsgi; the 5 handler endpoints still exist. Verify the endpoints are reachable from UI. | CONDITIONAL — if UI doesn't surface "proactive insights" panel, delete all endpoints + module |
| `data_refresh.py` (493 LOC) | `start_data_refresh`, `get_refresh_status` | `app.py:11835, 21045` + `wsgi.py:163` commented-out | start disabled, `get_refresh_status` is a health endpoint | LIKELY YES — disabled + only the status endpoint remains |
| `applyflow.py` (1728 LOC, 64KB) | `handle_applyflow_request` | `app.py:19650` (one POST handler, `/api/applyflow`) | Check if UI calls `/api/applyflow` in any template | LIKELY YES — one endpoint, hub says unlaunched per S62 memory audit |
| `creative_quality_score.py` (295 LOC) | `score_creative_quality` | `nova.py:11888`, `app.py:14079, 16067` | YES (3 call sites) | NO — keep, it's live |
| `ats_widget.py` (111 LOC) | `get_widget_stats`, `generate_embed_code` | `app.py:5672, 11854`, `nova.py:10617` | YES (3 call sites) | NO — keep |
| `calendar_sync.py` | all | disabled in `wsgi.py:222-228` | NO at runtime | YES |
| `competitive_intel.py` (2813 LOC, 108K) | `run_full_analysis`, `generate_competitive_excel`, `generate_competitive_ppt`, `assess_competitive_threats` | `routes/competitive.py:164, 192, 224`, `nova.py:12434` | YES (active /api/competitive/*) | NO — keep |
| `data_synthesizer.py` (4948 LOC, 192K) | `synthesize`, `get_deep_benchmarks`, `_validate_location_plausibility` | `app.py:2850`, `nova.py:10596`, `budget_engine.py:1927` (comment) | YES (active synth path in /api/generate) | NO — keep but 4948 LOC is massive, ripe for `sc:cleanup` pass |

### nova.py imports never invoked

Grep for imports in top 50 lines of nova.py vs. call-site search showed no obvious top-level dead imports beyond those already flagged above. The module is 23,886 lines — a separate dedicated pass would be warranted.

### Files loaded into slug but never referenced

- `data/backups/*.zip` — 7 files, 17MB. Created by in-process backup loop, never read.
- `data/generated_docs/*.zip` — 63 test fixtures, 3.7MB. Obvious test pollution in production build.
- `archive/excel_legacy.py` — in .renderignore candidate.
- `uploads/` directory — user upload staging, should not ship in slug.

---

## D. Env Var Cleanup

Env vars declared in Render (64 total per memory) vs. call sites found in `*.py`:

### Zero call-site (DELETE from Render)

Based on grep of `os.environ.get("X")` patterns:

| Env var | Call sites in prod `.py` | Recommendation |
|---|---|---|
| `RENDER_API_KEY` | 0 (only CLI / scripts use it) | keep (it's infra, not app) |
| `APIFY_API_TOKEN` | 1 (`web_scraper_router.py:60`) — actively used as Tier 1.5 scraper | keep |
| `FIRECRAWL_API_KEY` | 6 sites in `firecrawl_enrichment.py` | **consider removing** if not re-funding (saves secret-store slot; app degrades gracefully) |
| `ELEVENLABS_API_KEY` | 22 sites in `elevenlabs_integration.py` | only if TTS feature ships — confirm with user |
| `GOOGLE_ADS_CUSTOMER_ID`, `GOOGLE_ADS_DEVELOPER_TOKEN`, `GOOGLE_ADS_MCC_NAME` | ~10 sites in `google_ads_*` | keep if MCC intel is active in chatbot |
| `META_ACCESS_TOKEN` | ~3 sites in `meta_ads_integration.py` | keep if Meta chatbot tool is live |
| `NVIDIA_API_KEY`, `SAMBANOVA_API_KEY`, `SILICONFLOW_API_KEY`, `ZHIPU_API_KEY`, `TOGETHER_API_KEY`, `HUGGINGFACE_API_KEY`, 7× `OPENROUTER_*` | fallback LLM providers in `llm_router.py` | **audit** — if Haiku primary + Gemini fallback works, most of these 12+ free-tier fallbacks are never hit. Delete the bottom 8 of the 23-provider chain. |

### Env vars referenced but rarely

- `SENTRY_DSN` — 29 sites; active, keep.
- `POSTHOG_API_KEY` — 9 sites; active, keep.
- `RESEND_API_KEY` — 15 sites; active (email alerts), keep.
- `UPSTASH_REDIS_REST_URL` / `_TOKEN` — 14 sites; L3 cache, keep.
- `JINA_API_KEY` — 10M token budget not yet used; keep.
- `CLOUDFLARE_API_KEY` — used for Cloudflare LLM; keep only if Cloudflare Workers AI fallback is hit.
- `GEONAMES_USERNAME` — 4 sites geocoding fallback; keep.
- `GOOGLE_MAPS_API_KEY` — 2 sites; active on GeoViz, keep.
- `GOOGLE_SLIDES_CREDENTIALS_B64` — 1 site; active, keep.

### Recommended action on env vars

Run locally: `for v in $(env | cut -d= -f1); do grep -rq "os.environ.get..$v" --include="*.py" . || echo "UNUSED: $v"; done`. This flags Render-side env vars with no Python reference — typical cleanup yield is 5-10 dead keys.

---

## E. One Concrete Action Plan

### Cut first (Week 1 — saves ~$20-25/mo, risk low)

1. **Disable data_enrichment** (1 line in `app.py:5759`: comment out `start_enrichment()`). Immediately stops the hourly 14-task cascade. Firecrawl is already dead; cached data in Supabase L3 covers the rest for 24h+.
2. **Add `.renderignore`** at repo root:
   ```
   data/backups/
   data/generated_docs/
   data/.embedding_cache.json
   archive/
   uploads/
   node_modules/
   ```
   Drops ~50MB+ from the slug. Cuts deploy bandwidth.
3. **Disable in-process KB backup timer** (`app.py:5815-5819`). If backups matter, run `scripts/backup_kb.py` from a GitHub Action cron.
4. **Stretch data_matrix_monitor to 24h** (`data_matrix_monitor.py:43` change `12 * 3600` → `24 * 3600`). Removes one convergence point.

Expected: **nightly spike disappears** (the 12h convergence is broken), RAM drops 20-40MB per worker, egress drops 30-50 GB/mo.

### Cut second (Week 2 — saves ~$12/mo)

5. Add LRU cap + batch flush to `_embedding_cache` (see S62 memory audit for code pattern).
6. Downgrade Render Standard 2-worker → Starter 1-worker with gevent. Savings ~$12.46/mo.

### Cut third (Week 3 — polish)

7. Delete `chroma_rag.py` (544 LOC, zero refs).
8. Audit and shrink the 23-provider LLM fallback chain.
9. Extend LLM cache TTL from 5min → 1h for non-real-time task types.
10. Delete `applyflow.py` if hub page confirms it's unlaunched.

**Total realistic monthly savings: $35-50/mo (~70-100% of the current bill — should take Render to the Starter floor of $7).** Plus OOM emails stop + nightly spike flattens.

---

## Validation Hooks (no code changes needed)

- Grep Render logs for `Data enrichment cycle starting`, `DataMatrixMonitor: running check`, `Qdrant keep-alive ping`, `Scheduled KB backup succeeded`. Timestamps should line up with the nightly spike on the graph.
- Count `Firecrawl disabled until` log lines — if hundreds/day, confirms the dead-credit cascade hypothesis.
- Check `/api/health/enrichment` response: count of `skipped` sources >> `refreshed` confirms the cache-fill is redundant.

---

## Files & lines referenced (all absolute paths)

- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data_enrichment.py` lines 55, 1964-1981, 2075-2113
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data_matrix_monitor.py` lines 43-46, 424
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/auto_qc.py` lines 27-31, 236-282
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/app.py` lines 446, 4843, 5723-5787, 5790-5821, 5826-5861, 6162
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/wsgi.py` lines 100-105, 137-142, 157-237
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/vector_search.py` lines 89-95, 668-679
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/supabase_cache.py` lines 853-876
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/resilience_router.py` line 2278
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/chroma_rag.py` (entire file)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/firecrawl_enrichment.py` line 64
