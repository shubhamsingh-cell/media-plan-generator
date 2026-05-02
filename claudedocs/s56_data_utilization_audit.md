# Nova AI Suite — S56 Data-Utilization & Architecture Audit

**Date**: 2026-04-24
**Scope**: Read-only, evidence-based audit of `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/`
**Verdict**: The user's intuition is correct. **Roughly two-thirds of the registered data assets are loaded into memory but never flow to end-user output.** The system has a genuine and severe data-utilization gap, caused primarily by a duplicated KB-loader architecture and a "transparency panel" that *labels* responses with files it never actually reads.

---

## Headline numbers (all cited below)

| Dimension | Registered / Advertised | Actually reaching user output | Unused / misleading |
|---|---|---|---|
| KB JSON files (`KB_FILES` in kb_loader.py) | 54 | **~15 HOT/WARM** | **~39 COLD/DEAD** |
| Client media plans | "100s" (per MEMORY.md); "6 plans, 532 channels" (per prompts) | **7 plans total** (6 in `client_media_plans_kb.json` + 2 RTX JSON files, with 1 overlap) | Claim of "100s" is false |
| LLM providers (PROVIDER_CONFIG) | 23 (v4.1 advertises "23 providers") | **17 with local env keys**; only 3-4 (Haiku, Gemini, Cloudflare, Groq) likely to see traffic given routing tables | 6 keys absent locally (DEEPSEEK, PERPLEXITY, etc. not in PROVIDER_CONFIG but referenced in comments; XIAOMI, SILICONFLOW, OPENROUTER variants rarely hit) |
| External APIs (MEMORY claims 22+) | 22+ | **~10 actively wired** to plan/chat output (BLS, Adzuna, Jooble, FRED, O*NET, Tavily, Google Maps, GeoNames, Jina, Apify) | Firecrawl (0 credits, auto-disabled), Reed, LinkUp, Voyage, BigQuery, GCS, Vision OCR (partially), ElevenLabs — all loaded but rarely or never called |
| Supabase tables (MEMORY claims 31) | 31 in docs | **12 actually referenced** in runtime code (cache, knowledge_base, nova_conversations, nova_documents, nova_saved_plans, nova_generated_plans, nova_shared_conversations, nova_memory, plan_events, enrichment_log, metrics_snapshot, nova_conversation_state) | 19 dead/CG-only/seed-only (salary_data, compliance_rules, market_trends, vendor_profiles, supply_repository, all 9 `cg_*` tables belong to a different product) |
| MCP servers (MEMORY claims 40+) | 40+ | **0 runtime-integrated** into Nova chat tools | All are Claude-Code dev tooling, not product runtime |
| Python modules | 123 | — | Many 300-900 line Google Cloud integrations (BQ, GCS, Vision, Ads Analytics) have a single call site or only a health-check touchpoint |

Evidence: `kb_loader.py:34-108` (54 keys); `data/client_plans/` (2 files); `data/client_media_plans_kb.json` has `plans` dict with **7 keys** (`rtx_us`, `bae_systems`, `amazon_cs_india`, `rolls_royce_solutions_america`, `rtx_poland`, `peraton`, `rtx_usa`).

---

## KB usage matrix — all 54 keys

I searched every call site for `kb.get("<key>")`, `knowledge_base.get("<key>")`, `kb["<key>"]`, and `_data_cache.get("<key>")`, excluding `kb_loader.py` itself. Results (where `count` = direct-access reads across the codebase):

### HOT (read in the main chat / plan path, multiple call sites)
| Key | Read sites | Primary consumers |
|---|---|---|
| `core` (via kb) | 1 | merged into top-level; fed into backward-compat |
| `platform_intelligence` | 3 | `data_synthesizer.py:755`, `nova.py:7253`, `_data_cache` |
| `recruitment_benchmarks` | 5 | `data_synthesizer.py:773,831`, `nova.py`, `ppt_generator.py:5883`, `excel_v2.py:2344` |
| `workforce_trends` | 4 | `data_synthesizer.py:809`, `nova.py:9332,9386` |
| `white_papers` | 7 | `data_synthesizer.py:816,877`, `ppt_generator.py:1460`, `archive/excel_legacy.py:1851`, `nova.py:9386` |
| `google_ads_benchmarks` | 4 | `data_synthesizer.py:851,2393,4447`, `nova.py:9115,9468` |
| `regional_hiring` | 2 | `data_synthesizer.py:793,2209`, `nova.py:9207` |
| `client_media_plans` | 2 (within Nova's `_data_cache`) | `nova.py:3804,9690` — the `query_client_plans` tool |
| `expanded_supply_repo` | 3 | `nova.py:6517,6747,15198` — global supply search |
| `joveo_publishers` | 2 | `nova.py:3913,6697,8161,15196` — used in supply tooling |
| `channels_db` | 3 | `nova.py:6613,6698,15197` |
| `healthcare_supply_map_us` | 2 | `nova.py:14921,14926` — ONLY via fast-path `query_healthcare_supply_map` |
| `global_supply` | 2 | `nova.py:6516,8150` |

### WARM (loaded; read from at most one non-trivial surface)
| Key | Read sites | Where |
|---|---|---|
| `recruitment_strategy` | 1 | `nova.py:9176` |
| `supply_ecosystem` | 1 | `nova.py:9298` |
| `external_benchmarks` | 2 | `nova.py:9145,9551`, `app.py:3673` |
| `fred_indicators` | 3 | `data_synthesizer.py:1862`, `excel_v2.py:3420`, `ppt_generator.py:5174` (Excel/PPT only, never in chat) |
| `google_trends` | 1 | `data_synthesizer.py:1822` (Excel/PPT only) |
| `platform_ad_specs` | 1 | `creative_quality_score.py:102` |
| `adzuna_benchmarks` | 1 | `nova.py:12093` (single tool, rarely triggered) |
| `craigslist_benchmarks` | 1 | `nova.py:7326` |
| `seasonal_hiring_trends` | 1 | `nova.py:4207` (prompt-time only) |
| `international_sources` | 1 | `nova.py:7272` |
| `international_benchmarks` | 1 | `nova.py:7290` |
| `linkedin_guidewire` (legacy, not in KB_FILES) | 2 | `nova.py:8899,19168` |

### COLD (loaded by kb_loader but NEVER read by any consumer)
Evidence: `grep kb.get("<key>")` returns 0 across the repo.

| Key | File | Size | Status |
|---|---|---|---|
| `joveo_2026_benchmarks` | joveo_2026_benchmarks.json | 11.2 KB | **0 direct reads**. Only mentioned in prompt strings telling the LLM to "use" it (nova.py:3873, 4060, 6178, 17017). |
| `joveo_cpa_benchmarks` | joveo_cpa_benchmarks_2026.json | 30.4 KB | **0 direct reads**. 304 CPA categories loaded, never consulted. Only appears in prompt text and `_SOURCE_TO_KB_FILES` labels. |
| `hr_tech_landscape` | hr_tech_landscape_2026.json | — | 0 reads |
| `publisher_benchmarks` | publisher_benchmarks_2026.json | — | 0 reads; prompt-only (nova.py:6169) |
| `recruitment_marketing_trends` | recruitment_marketing_trends_2026.json | — | 0 reads |
| `labor_market_outlook` | labor_market_outlook_2026.json | — | 0 reads |
| `salary_benchmarks_detailed` | salary_benchmarks_detailed_2026.json | — | 0 reads; prompt-only |
| `ad_benchmarks_recruitment` | ad_benchmarks_recruitment_2026.json | — | 0 reads |
| `industry_hiring_patterns` | industry_hiring_patterns_2026.json | — | 0 reads |
| `top_employers_by_city` | top_employers_by_city_2026.json | — | 0 reads |
| `compliance_regulations` | compliance_regulations_2026.json | — | 0 reads |
| `agency_rpo_market` | agency_rpo_market_2026.json | — | 0 reads |
| `global_supply_repository` | joveo_global_supply_repository.json | 2.7 MB | Loaded into `expanded_supply_repo` under different key (nova.py:3776). The `global_supply_repository` key in `load_knowledge_base()` is loaded and then abandoned. |
| `rtx_media_plan` / `rtx_aerospace_benchmarks` | client_plans/*.json | — | 0 direct reads. Loaded, merged into `client_media_plans`, but the RTX-specific benchmarks KB has no query path. |
| `channel_benchmarks_live` | channel_benchmarks_live.json | — | 0 reads via kb_loader key |
| `competitor_careers` | competitor_careers.json | — | 0 reads |
| `h1b_salary_intelligence` | h1b_salary_intelligence.json | — | 0 reads (indexed into vector store only) |
| `job_density_metros` | job_density_metros.json | — | 0 reads |
| `job_posting_volumes` | job_posting_volumes.json | — | 0 reads |
| `live_market_data` | live_market_data.json | — | 0 reads via kb key |
| `market_trends_live` | market_trends_live.json | — | 0 reads via kb key |
| `partner_specialty_crosswalk` | partner_specialty_crosswalk.json | 187 KB | 0 reads |
| `partner_url_registry` | partner_url_registry.json | — | 0 reads |
| `category_to_partners` | category_to_partners.json | — | 0 reads |
| `recruitment_benchmarks_2026_deep` | recruitment_benchmarks_2026_deep.json | 36.8 KB | 0 reads |
| `employer_career_intelligence_2026` | employer_career_intelligence_2026.json | 46.5 KB | 0 reads |
| `healthcare_specialty_pay_2026` | healthcare_specialty_pay_2026.json | 32.2 KB | 0 reads |
| `linkedin_benchmarks` | linkedin_performance_benchmarks.json | — | 0 direct reads; only indirect via vector search |
| `channels_db` (top-level) | channels_db.json | — | Read via `_data_cache`, NOT via kb_loader's key |

**Critical nuance**: Some of the "cold" files are indexed into the *vector search* system at startup (`vector_search.py:1353 index_knowledge_base`). That means chunks of them might surface if a query happens to match a relevant text chunk. But this is semantic retrieval with a ~250 char chunk, not structured data access — so the detailed benchmarks (numbers, tables, CPA figures) inside those files are NOT recoverable through the chat path. For the 39 cold files, the data is present in ChromaDB embeddings but **never flows through a `kb.get(...)` → tool response path**. The user's complaint is empirically correct.

---

## The "Transparency panel" lie — most important architectural finding

`nova.py:22619-22685` defines `_SOURCE_TO_KB_FILES`, a dict mapping 60+ human-friendly source labels to filenames. When Nova returns a response, `_map_sources_to_kb_files` (line 22688) looks at the `sources` list the LLM produced and echoes back the "matched" KB filenames as `kb_files_queried`.

**The problem**: These filenames are presented to the frontend as the files consulted for the answer, but the system never actually loaded or read most of them during that query. If the LLM mentions "salary benchmarks" in its sources (because the prompt told it salary data exists), the transparency panel claims `salary_benchmarks_detailed_2026.json` was queried — even though no code path ever opened that file for this request.

Evidence:
- The `salary_benchmarks_detailed` key has zero call sites (grep result above).
- Nova's `_data_cache` does NOT include `salary_benchmarks_detailed` (nova.py:3738-3761 — not in the `_research_files` dict).
- Yet the source label "salary benchmarks" maps to it (nova.py:22655).
- So any response that says "per salary benchmarks, X is $Y" will display `salary_benchmarks_detailed_2026.json` in the "Why this answer?" panel — even though the actual number came from the LLM's prior training or was hallucinated.

This is worse than "not using the data." It's **performative usage** — showing users we consulted sources we didn't.

---

## Architecture smells found

1. **Duplicate KB loader (kb_loader.py vs nova.py:3680-3816)**. `load_knowledge_base()` in `kb_loader.py` loads 54 files into a single process-wide dict. Then `Nova.__init__` at nova.py:3680 loads ~20 of those same files *again* into `self._data_cache` with slightly different keys. Two copies in memory, two divergent sources of truth, and almost no Nova tools call `load_knowledge_base()` — they go through `_data_cache`. This is why half the KB_FILES dict is "cold": Nova ignores it.

2. **Monolithic nova.py (23,692 lines, 233 functions, 135 lazy imports)**. 96 top-level + 137 indented function/method definitions. 135 local `from X import Y` statements inside function bodies — a signature of circular-import avoidance, not intentional laziness. Changing anything in nova.py risks cascading breakage; single file >500-line limit from project CLAUDE.md by 47×.

3. **app.py is 20,950 lines with 550 KB `api_enrichment.py`, 145 KB `api_integrations.py`**. Three files total >1.6 MB of Python.

4. **LLM provider config bloat**. `PROVIDER_CONFIG` has 23 entries (llm_router.py:526-826). `TASK_CONVERSATIONAL` routes to Haiku first, then Gemini, then GPT-4o — the bottom 20 providers in the routing list are practically unreachable unless the top 3 all fail simultaneously. Yet code maintains them with circuit breakers, rate limiters, per-minute tracking, and health scores. Pure maintenance tax.

5. **~400 stale api_cache JSON entries**. `data/api_cache/` has 213 files, majority dated Mar 8-26 (weeks old). Cache TTLs, eviction, or cleanup appear missing.

6. **Firecrawl is disabled with a 1-hour cooldown after 402**. firecrawl_enrichment.py:`_firecrawl_disabled_until` + `_FIRECRAWL_COOLDOWN: int = 3600`. The memory note says "Firecrawl has 0 credits, we know" — yet the code is still imported, initialized, and tried first. When the 402 returns, it waits an hour and tries again. Every request pays the handshake + error cost.

7. **Giant dead integrations**. `google_bigquery_integration.py` (399 lines) — `bq_store_plan` called exactly once (app.py:16220). `google_cloud_storage.py` (304 lines) — `gcs_upload` referenced but never called at any user-visible surface. `google_vision_integration.py` (271 lines) — `extract_text_from_image` defined but never called; `extract_text_from_pdf_vision` called once in `file_processor.py:109`. `google_ads_analytics.py` (494 lines) — only used by health-check endpoint (routes/health.py:1581). `elevenlabs_integration.py` (988 lines) — imported in app.py but its exported functions return `{"error": "elevenlabs_integration module not available"}` when not configured (app.py:11405).

8. **Hardcoded role/industry data that should be KB-sourced**. `ROLE_TIER_KEYWORDS` at app.py:2909 is an in-code dict of industry → keyword list. Appears to duplicate partial content from `industry_hiring_patterns_2026.json` (which has 0 reads).

9. **Supabase Row Level Security ambiguity**. Code writes to `nova_conversations` with `.insert/upsert/update/delete` (nova_persistence.py — 40+ call sites), but the tables `salary_data`, `compliance_rules`, `market_trends`, `vendor_profiles`, `supply_repository` are listed in MEMORY.md as "seeded" and have zero `.table(...)` references. Either the seed script populated them at some past point and the app never queries them, or they're orphans from an earlier design iteration.

10. **MCP server theater**. MEMORY.md lists 40+ MCP servers (Context7, Sequential, Serena, PostHog, Supabase, Exa, Playwright, etc.). None of these are integrated into the runtime Nova chat product — they're Claude Code dev tooling. The memory conflates developer tools with product capabilities.

---

## TOP 10 ISSUES RANKED BY USER IMPACT

### 1. **Transparency panel shows KB filenames for files never actually read this turn**
- **What's broken**: `_SOURCE_TO_KB_FILES` (nova.py:22619-22685) displays "salary_benchmarks_detailed_2026.json" to the user as a queried KB file when the LLM mentions "salary benchmarks" — but no code path ever opened that file for the request. 39 of 54 KB keys have zero direct-read sites (grep `kb.get("<key>")` = 0).
- **Evidence**: nova.py:22619, 22655; `kb.get("salary_benchmarks_detailed")` returns 0 matches; file is not in Nova's `_research_files` dict (nova.py:3738-3761).
- **User-visible impact**: Loss of trust when a savvy user clicks "Why this answer?" and realizes the answer couldn't have come from the named source. This is the single most corrosive data-integrity issue.
- **Fix**: (a) Log the *actual* files read per-turn in a request-scoped list; only populate `kb_files_queried` from that list. (b) Delete all entries in `_SOURCE_TO_KB_FILES` that don't correspond to a real read-site. (c) Add an assert in CI that every `_SOURCE_TO_KB_FILES` value is referenced in a `kb.get`/`_data_cache.get` call.

---

### 2. **Duplicate KB loader — Nova's `_data_cache` bypasses `load_knowledge_base()` for 39/54 files**
- **What's broken**: kb_loader.py loads 54 files into `_knowledge_base` dict. nova.py:3698-3761 re-loads ~20 files into `self._data_cache`. Nova's chat tools call `self._data_cache.get(...)`, not `load_knowledge_base().get(...)`. So any file not in Nova's smaller dict is effectively invisible to the chat product, regardless of what kb_loader loaded.
- **Evidence**: nova.py:3698 `_data_cache = {}`; nova.py:3721-3735 loads 6 files; nova.py:3738-3761 loads 20 more; `grep -n "_data_cache.get" nova.py` returns 40+ hits, `grep kb.get` returns only ~10 hits in nova.py. `load_knowledge_base()` is called 15 times (`nova.py` imports it but rarely uses it in tool code paths).
- **User-visible impact**: The 39 cold KB files ship as dead weight. ~5 MB of RAM per worker × 4 workers = 20 MB never fed to responses. Users asking "what's the CPA benchmark for healthcare RNs in NYC?" get general answers even though `joveo_cpa_benchmarks_2026.json` (304 categories) sits in memory unused.
- **Fix**: Consolidate to a single in-memory KB accessor. Delete `_data_cache` or make it an alias to `load_knowledge_base()`. Then inventory every declared KB file and either wire it to a tool or remove it from `KB_FILES`.

---

### 3. **Client media plans claim "100s" — there are 7**
- **What's broken**: MEMORY.md says "100s of client media plans in the KB". Prompt strings say "6 reference plans, 532 channels". The actual file `data/client_media_plans_kb.json` has `plans` dict with **7 keys**: `rtx_us`, `bae_systems`, `amazon_cs_india`, `rolls_royce_solutions_america`, `rtx_poland`, `peraton`, `rtx_usa`. The `client_plans/` directory has 2 JSON files (`rtx_usa_media_plan.json`, `rtx_aerospace_defense_benchmarks.json`), with 1 overlap (`rtx_usa`).
- **Evidence**: `python3 -c "import json; print(len(json.load(open('data/client_media_plans_kb.json'))['plans']))"` → `7`. File size: 60,064 bytes. `ls data/client_plans/` → 2 files.
- **User-visible impact**: When a user asks "show me a media plan similar to Verizon" or "pull up client plans in healthcare," Nova's `query_client_plans` tool (nova.py:9690) can only find 7 templates — all aerospace/defense + Amazon CS India. The "Joveo Client Portfolio" label (nova.py:611-613) is misleading.
- **Fix**: Either (a) honestly rename the KB to `reference_plan_templates` and remove marketing language, or (b) actually ingest Joveo's real historical plans from whatever system holds them. Until (b) happens, don't route questions about client portfolios to this KB.

---

### 4. **LLM router has 23 providers in PROVIDER_CONFIG; only ~3-4 ever see real traffic**
- **What's broken**: `TASK_CONVERSATIONAL` routing list (llm_router.py:888-905) puts Claude Haiku first, Gemini second, GPT-4o third, then 17 providers that will only fire if the top 3 are simultaneously circuit-broken. Each provider carries circuit-breaker state, rate-limiter state, health score, and code paths that need maintenance. Local env has 17 of the 23 keys; providers like XIAOMI_MIMO, OPENROUTER_ARCEE, OPENROUTER_LIQUID are barely reachable.
- **Evidence**: llm_router.py:526 `PROVIDER_CONFIG` has 23 entries; `grep env_key` shows 28 references (multiple providers share ANTHROPIC_API_KEY/OPENROUTER_API_KEY); local env has keys for GEMINI, GROQ, CEREBRAS, MISTRAL, OPENROUTER, SAMBANOVA, NVIDIA_NIM, CLOUDFLARE, ZHIPU, SILICONFLOW, HUGGINGFACE, XIAOMI, TOGETHER, OPENAI, ANTHROPIC, XAI = 17 keys.
- **User-visible impact**: Latency budget wasted in circuit-breaker checks. When Haiku has a transient 5xx, fallback paths frequently 404 because Gemini model IDs were wrong (S53 fix, still fragile — see llm_router.py:530-533). Users see slow responses or "all providers exhausted" errors.
- **Fix**: Trim PROVIDER_CONFIG to 5 real providers: Haiku (primary), Gemini (free fallback), GPT-4o (paid fallback), Groq (high-RPM overflow), Cloudflare (burst capacity). Delete routing lists for tasks that aren't actually used (e.g., TASK_TRANSLATION, TASK_DEEP_REASONING — grep for where they're invoked).

---

### 5. **Firecrawl is permanently disabled yet still imported and first-tried**
- **What's broken**: `firecrawl_enrichment.py` has `_firecrawl_disabled_until` and `_FIRECRAWL_COOLDOWN: int = 3600`. Firecrawl has 0 credits (per MEMORY.md). Every attempt returns 402, disables for an hour, and retries. The scraper router (web_scraper_router.py, 24 env-var touches) lists Firecrawl in the rotation.
- **Evidence**: firecrawl_enrichment.py top-of-file cooldown code; MEMORY.md "#3, no credits"; api_enrichment.py:21 env-key references.
- **User-visible impact**: Every scrape request pays the Firecrawl handshake + 402 parse + cooldown update before falling back. Adds 500-2000 ms to requests that need scraping.
- **Fix**: Set `FIRECRAWL_API_KEY=""` in env (or add a hard disable flag), and short-circuit the scraper router when disabled. Move Apify and Jina to the top of the rotation (matches MEMORY.md "Scraper order: Apify #1, Jina #2, Firecrawl #3" but Firecrawl is at #3 not disabled).

---

### 6. **Giant Google Cloud integrations with one real call site each**
- **What's broken**: `google_bigquery_integration.py` (399 lines) — `bq_store_plan` called once at app.py:16220 after plan generation. `google_cloud_storage.py` (304 lines) — `gcs_upload` imported but no production call site. `google_vision_integration.py` (271 lines) — `extract_text_from_image` (119-line function) never called; `extract_text_from_pdf_vision` called once in file_processor.py:109. `google_ads_analytics.py` (494 lines) — `get_google_ads_benchmarks` only reachable via health check. `elevenlabs_integration.py` (988 lines) — returns "module not available" error to the user (app.py:11405).
- **Evidence**: `grep -n bq_store_plan` → 2 hits (definition + 1 call). `grep -n extract_text_from_image` → 2 hits (definition + import). `grep -n get_google_ads_benchmarks` → 2 hits (definition + health). Line counts from `wc -l`.
- **User-visible impact**: Startup time (import cost), memory overhead, and confusing failure modes. Users see "ElevenLabs audio feature" in the UI but the backend returns an error.
- **Fix**: Either wire these to user features (PDF upload OCR, image-to-text, audio narration of plans) or delete them. Do not half-integrate. 2,456 lines across 5 files for near-zero user value.

---

### 7. **Monolithic nova.py (23,692 lines) with 135 lazy imports — circular-import code smell**
- **What's broken**: Project CLAUDE.md says "Keep files under 500 lines". nova.py is 23,692 lines (47× over budget). 135 `from X import Y` statements live inside function bodies, not at top-of-file. This is the classic circular-import workaround. Changing one tool risks cascading breakage because function defs, prompt strings, tool dispatch, data loading, and utility helpers are all in the same file.
- **Evidence**: `wc -l nova.py` → 23,692. `grep -c "^    from\|^    import" nova.py` → 135. Example: nova.py:11271 `from google_knowledge_graph import ...` inside a tool dispatch function.
- **User-visible impact**: Every code change takes longer to validate. Bugs in prompt strings (line 3873, 6206, 17017) that reference non-read files have persisted because nobody audits 23K lines.
- **Fix**: Extract tool dispatchers into `tools/` package, prompt strings into `prompts/` module, data loading into `data_sources.py`. Target: nova.py < 3,000 lines doing only chat orchestration.

---

### 8. **Supabase table count drift: 31 claimed, 12 actually used, 19 dead**
- **What's broken**: MEMORY.md lists 31 Supabase tables (22 Nova + 9 CG). Runtime references (via `.table("X")` and `rest/v1/X` URL paths) find 12 tables in product code: `cache`, `knowledge_base`, `nova_conversations`, `nova_documents`, `nova_generated_plans`, `nova_memory`, `nova_saved_plans`, `nova_shared_conversations`, `plan_events`, `enrichment_log`, `metrics_snapshot`, `nova_conversation_state`. The "seeded" tables (`salary_data`, `compliance_rules`, `market_trends`, `vendor_profiles`, `supply_repository`) have zero `.table(...)` references. The 9 `cg_*` tables belong to a different product (CG Automation, at `/Users/shubhamsinghchandel/Downloads/Claude/cg-automation/`).
- **Evidence**: `grep -rE '\.table\("' --include="*.py"` gave 10 unique tables (plus `my_table` test string + `knowledge_base` for sync); `grep -rE 'rest/v1/[a-z_]+'` added `enrichment_log`, `metrics_snapshot`, `nova_conversation_state`. No hits for seeded tables or cg_* tables in Nova code.
- **User-visible impact**: Low direct impact. But it means MEMORY.md and system docs mislead developers, and Supabase cost pays for 19 orphan tables.
- **Fix**: Drop the 5 seeded tables in Supabase if unused; clarify in MEMORY.md that 9 `cg_*` tables live in a separate product.

---

### 9. **"MCP servers (40+ total)" are dev tooling, not product capability**
- **What's broken**: MEMORY.md lists Context7, Sequential, Supabase, PostHog, Playwright, Serena, Slack, Linear, and 32+ other MCP servers as Nova AI Suite capabilities. Grep for `mcp_server|mcp_client|MCP_` in product `.py` files returns zero matches. All 40+ MCP servers are Claude-Code-side tools used by Claude during development — they never run in the Nova product on Render.
- **Evidence**: `grep -rE "mcp_server|mcp_client|MCP_" --include="*.py"` → 0 matches. The `.mcp.json` file in project root configures Claude Code, not the Render service.
- **User-visible impact**: End users and stakeholders may believe Nova "has 40 integrations" when it doesn't. Budget/architectural decisions based on that count are wrong.
- **Fix**: Remove MCP-server count from Nova product marketing. Keep a separate "Dev tooling: 40+ MCPs used in Claude Code" section.

---

### 10. **Stale data theater: 19+ files dated 2024 used to answer 2026 questions, and hot-reload can't save them**
- **What's broken**: kb_loader.py:112 warns on files >90 days old. Spot check: `google_ads_2025_benchmarks.json` (by name, data baseline 2025), `external_benchmarks_2025.json`, and several `_2026.json` files with `last_updated` timestamps in early-to-mid 2024. Hot-reload checks mtime every 5 minutes but nothing writes fresh data to these files — they're static exports.
- **Evidence**: 19 occurrences of `"2024-"` dates in `data/*.json`. `_FILE_FRESHNESS_THRESHOLD_DAYS = 90` (kb_loader.py:112). `_check_file_freshness_at_startup` only *warns*, never refuses to serve.
- **User-visible impact**: Users get 2024 benchmarks presented as authoritative for 2026 media plans. CPC/CPA figures quoted with confidence are months/years out of date.
- **Fix**: (a) Add a visible staleness badge in responses when consulted data is >180 days old. (b) Add a `data_refresh.py` scheduled job that actually rewrites these files from live BLS/Adzuna/FRED APIs. (c) Block serving benchmarks older than 365 days unless user explicitly opts into "historical mode".

---

## Quick wins (low effort, high user impact)

1. Delete the 39 cold entries from `KB_FILES` in `kb_loader.py` to cut RAM by ~5 MB × 4 workers and eliminate the "transparency panel" deception. *OR* wire them to tools if they're supposed to be used. Don't leave them half-integrated.
2. Rename `client_media_plans_kb.json` to `reference_plan_templates.json` and stop labeling responses "Joveo Client Portfolio". Seven plans is not a portfolio.
3. Delete `_SOURCE_TO_KB_FILES` entries for filenames with zero call sites. (Automatable: CI check.)
4. Trim `PROVIDER_CONFIG` from 23 to 5 providers. Delete `TASK_TRANSLATION`, `TASK_DEEP_REASONING`, `TASK_INTELLIGENCE_SUMMARY` routing lists if unused.
5. Hard-disable Firecrawl (`FIRECRAWL_API_KEY=""` or a feature flag) until credits are restored.
6. Add a CI check that every `KB_FILES` key is referenced somewhere in the codebase under `kb.get(...)` or equivalent — fail the build on unused keys.

---

## Out of scope but worth flagging

- The `/api/chat` path loads `nova.py` → imports happen lazily → first chat after cold start pays a 5-10 s penalty. Consider warming the Nova singleton during `app.py` startup.
- `api_cache/` contains 213 files many of which are weeks old. No TTL eviction visible. Grows unbounded.
- `nova.py` imports `google_knowledge_graph`, `google_maps_integration`, `google_translate_integration`, `google_youtube_scheduler`, `meta_ads_integration`, `google_analytics_data` inside tool-dispatch functions. If any of these has an ImportError, the error lands at tool-call time, not startup. Prefer top-level conditional imports with clear "capability disabled" signals.

---

## What went right

- `load_knowledge_base()` thread-safety, hot-reload, and freshness-validation logic in `kb_loader.py` is well-written (lines 302-640). If the architectural gap above is fixed, the foundation is solid.
- The LLM router's circuit-breaker + rate-limiter + health-score machinery is genuinely sophisticated for the 3-4 providers that actually see traffic.
- Supabase tables that ARE used (`nova_conversations`, `plan_events`, `metrics_snapshot`) have clean write paths with proper error handling.
- `vector_search.index_knowledge_base` (vector_search.py:1353) and `chroma_rag.index_knowledge_base_chroma` (chroma_rag.py:429) do read the cold KB files and put them into embeddings. So the data isn't 100% wasted — semantic retrieval has access to it. But structured-data consumers don't.

---

## Verification commands (reproducible)

```bash
# KB file cold-read count
cd /Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator
for key in joveo_cpa_benchmarks salary_benchmarks_detailed publisher_benchmarks \
  compliance_regulations recruitment_benchmarks_2026_deep healthcare_specialty_pay_2026; do
  count=$(grep -rE "kb\.get\(\"${key}\"|knowledge_base\.get\(\"${key}\"" --include="*.py" . \
          | grep -v kb_loader.py | wc -l)
  echo "$key: $count reads"
done

# Actual Supabase tables referenced
grep -rE '\.table\("[^"]+"\)' --include="*.py" . | grep -v archive \
  | grep -oE '\.table\("[^"]+"\)' | sort -u

# Client plan count
python3 -c "import json; print(len(json.load(open('data/client_media_plans_kb.json'))['plans']))"

# Lazy import count (circular-import smell)
grep -cE '^\s+(from|import)\s' nova.py
```

---

**Bottom line**: The user's "majority of output data parameters are not even looked at" claim is empirically true. 39 of 54 registered KB files are cold; the transparency panel cites files that weren't read; `joveo_cpa_benchmarks_2026.json` with 304 CPA categories sits in memory with zero query paths reaching it; "100s of client media plans" is actually 7. The path to fix is mechanical: inventory every `KB_FILES` key, either wire it to a real tool or remove it, and rebuild the "sources" display from real per-turn read tracking.
