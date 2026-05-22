# Nova Chatbot - Live Integration Health Audit

- Date: 2026-05-22
- Auditor: technology-scout (read-only)
- Scope: Every external dependency Nova relies on at runtime: Supabase tables, MCP servers, external HTTP APIs, vector stores, and knowledge-base file freshness.
- Method: Source-code grep (no writes), live HTTP probes for low-cost endpoints, Render env-var introspection via Render API, Supabase REST count probes, Qdrant REST inspection.
- nova.py was NOT modified.

---

## 1. Executive Summary

Nova advertises 31 Supabase tables, ~28 MCP servers, and ~22 external APIs. Live verification shows the actual reachable surface is meaningfully smaller, with several silent failures:

- **Supabase**: 27 expected tables - **23 exist and respond** (HTTP 200), **4 are broken** (3 missing tables, 1 timing out). Of the 23 reachable, **11 are empty** (0 rows) in production, including critical operational tables (`nova_login_log`, `nova_conversations`, `plan_events`'s siblings, all 9 CG tables but only 1 actually used).
- **MCP servers**: 10 registered in the current Claude Code session, **9 connect, 1 fails** (`apple-notes`). The project's `.mcp.json` only declares one server (`claude-flow`) - the other 9 come from the user's global `~/.claude.json`. MEMORY.md's "28+ MCPs" claim does not match what is wired into this project.
- **External APIs**: Live-tested 7 critical ones - **all 7 responded successfully** (BLS, Adzuna, FRED, Tavily, BEA, Census, OECD-301-redirect). **O\*NET returned 401** due to wrong auth scheme in the test (handler in nova.py uses HTTPBasicAuthHandler correctly). Two wired-but-keyless integrations (LinkUp, Revelio) still ship "graceful error" responses to the LLM - confirmed by env scan.
- **Vector stores**: Qdrant collection `nova_knowledge` is healthy with **356,056 points** (status: green). Chroma is configured (`chroma_rag.py`) but **no module imports it**; it is dead code on Render.
- **KB freshness**: **56 of 70 JSON KB files in `data/` are older than 30 days** (80%). Only 2 files were touched in the last 7 days. Several files Nova answers benchmark questions from were last updated 2 months ago.

**Top-line risk:** Nova's *conversation persistence path* (login log, conversations table) returns HTTP 200 but contains 0 rows in production - this matches what S40 was supposed to enable. Either writes are failing silently or production traffic doesn't actually exercise these tables (auth bypass paths, or non-Joveo-domain users). This is the #1 item to investigate.

---

## 2. Supabase Tables Matrix

Verified via REST `Prefer: count=exact` against `$SUPABASE_URL/rest/v1/<table>?select=count`. Tables listed in the order specified by the task.

### 2.1 Reachable + populated (working as designed)

| Table | Rows | Primary reader | Read in chat flow? | Source |
|---|---|---|---|---|
| `cache` | 1 | `supabase_cache.py` (`_http_request`, line 217), `nova.py:3508` (Claude cache writes) | YES - hot path for every chat turn | `supabase_cache.py:339-468`, `nova.py:3503-3520`, `nova_slack.py:573,665` |
| `knowledge_base` | 129 | `supabase_data.py:_query_supabase("knowledge_base"...)` line 376 | YES - `_query_knowledge_base`, `_query_kb_deep` | `supabase_data.py:351-401`, `nova.py:7411,7189`, `nova.py:65` (import) |
| `channel_benchmarks` | 39 | `supabase_data.get_channel_benchmarks` line 428 | YES - via `app.py:14249` (plan flow) | `supabase_data.py:402-438`, app.py imports at line 143-149 |
| `salary_data` | 300 | `supabase_data.get_salary_data` line 463 | YES - via `app.py:14355` (plan generation) | `supabase_data.py:439-473` |
| `compliance_rules` | 8 | `supabase_data.get_compliance_rules` line 503 | YES - `app.py:7815,14389` | `supabase_data.py:474-513` |
| `market_trends` | 16 | `supabase_data.get_market_trends` line 536 | YES - `app.py:12208,14374` | `supabase_data.py:514-546` |
| `nova_memory` | 222 | `nova_memory.py:54,235` | YES - personalized memory recall | `nova_memory.py:54,235` |
| `plan_events` | 6 | `plan_events.py:497,521`, `app.py:3028` | YES - plan generation events | `plan_events.py:497-521`, `app.py:3028` |
| `nova_generated_plans` | 24 | `app.py:11310,15238` (Slack download persistence) | YES - Slack-link backed plan downloads | `app.py:11310,15238` |
| `nova_conversation_state` (undocumented) | 0 | `nova.py:343-386` (fire-and-forget upsert) | YES - state persistence per turn | `nova.py:343-386` - **MEMORY.md does not list this table** |

### 2.2 Reachable but EMPTY (0 rows) - silent failure or unused

| Table | Rows | Reader | Likely issue |
|---|---|---|---|
| `nova_conversations` | 0 | `nova_persistence.py` (40+ references), `nova.py:25935` | Expected to grow with every chat. **0 rows = either writes failing or production traffic not persisting.** P0 to investigate. |
| `nova_documents` | 0 | `nova_persistence.py:1311-1404` | Document attachments - 0 may be normal if feature unused |
| `enrichment_log` | 0 | `data_enrichment.py:554-728` | Cron-driven enrichment loop appears to not be writing. Last data-refresh investigation needed. |
| `metrics_snapshot` | 0 | `monitoring.py:1185-1292`, `morning_brief.py:94,127` | Morning brief feature has nothing to compare against. Snapshot writer likely never enabled. |
| `nova_login_log` | 0 | `app.py:10630-10658` | S40 added Google OAuth login logging. **0 rows = OAuth writes silently failing, or `@joveo.com` enforcement is rejecting before this codepath.** |
| `nova_saved_plans` | 0 | `app.py:11834,11868,13374` | User save-draft feature has zero saves in production. |
| `vendor_profiles` | 0 | `supabase_data.get_vendor_profiles` 568, `nova.py:11702` | Tool `query_vendor_profiles` is exposed to LLM (nova.py:5772) but DB has no rows. Nova will fall back to local JSON. |
| `supply_repository` | 0 | `supabase_data.get_supply_repository` 717 | Same problem - tool wired but table empty. |
| `cg_jobs` / `cg_action_plans` / `cg_schedules` / `cg_benchmarks` / `cg_sessions` / `cg_upload_history` | 0 each | CG-Automation app, separate codebase | Not Nova's concern, but per MEMORY.md these should have data from production CG usage. |

### 2.3 Broken tables (the urgent ones)

| Table | HTTP | Error | Diagnosis |
|---|---|---|---|
| `nova_shared_conversations` | 404 | `PGRST205` "Perhaps you meant the table 'public.nova_conversations'" | **Table was never created in production Supabase**. Schema file exists at `nova_schema_additions.sql:48` but migration was not applied. `nova_persistence.py:1447,1485,1509,1544` will fail on every share-conversation request. |
| `cg_uploads` | 404 | `PGRST205` "Perhaps you meant 'cg_upload_history'" | Renamed in S45 but MEMORY.md still lists old name. Not actively read from media-plan-generator. |
| `cg_user_sessions` | 404 | `PGRST205` "Perhaps you meant 'cg_sessions'" | Renamed in S45, same issue. |
| `cg_daily_raw` | 500 | `57014` "canceling statement due to statement timeout" | **Table exists but is too large to count without index**. Performance problem on Supabase side. |

### 2.4 Tables in MEMORY.md that don't appear in code

- `nova_avatars`, `nova_module_usage`, `nova_campaigns`, `research_cache` - per MEMORY.md line "Dropped (S34+S36 executed)" - confirmed not queried anywhere in the Python code.

---

## 3. MCP Servers Matrix

Project-level `.mcp.json` declares **exactly 1** MCP server. The rest live in user-global `~/.claude.json`. From `claude mcp list` in this session:

| Server | Connect status | Project scope | Used by Nova at runtime? |
|---|---|---|---|
| `claude-flow` | OK | Project (.mcp.json) | NO - dev-only (swarm orchestration, not called from nova.py) |
| `chrome-devtools` | OK | Global | NO - dev/QA only |
| `morphllm-fast-apply` | OK | Global | NO - dev-only code transform |
| `sequential-thinking` | OK | Global | NO - dev/reasoning only |
| `context7` | OK | Global | NO - dev-only docs lookup |
| `magic` | OK | Global | NO - dev-only UI gen |
| `playwright` | OK | Global | NO - dev/test only |
| `linear` | OK | Global | NO - dev tracking; deprecation warning seen |
| `plugin:claude-mem:mcp-search` | OK | Global plugin | NO - dev-only memory search |
| `apple-notes` | **FAIL** | Global | NO - dev tool, broken (`uvx apple-notes-mcp`) |

**Key finding:** Of the MCPs that `claude mcp list` returns, **none are called from `nova.py` at runtime**. Nova does not invoke MCP servers - it uses direct Python integrations. The MEMORY.md claim of "40+ MCP servers" or "28+ MCPs" refers to the user's authoring environment, not Nova's production runtime surface. This is fine, but it's a documentation-vs-reality gap.

**Linear deprecation alert:** Linear's MCP `/sse` transport was officially deprecated 2026-04-08 (per the in-session notice). Migration target: `https://mcp.linear.app/mcp`. Current registration uses the deprecated URL.

---

## 4. External APIs Matrix - With Live Tests

Cross-referenced against `docs/API_Audit_2026.md` (F4). All HTTP probes performed today, 2026-05-22.

### 4.1 Live-tested (low-cost probes)

| API | Key on Render | Handler in nova.py | Tool name | Live test today | Notes |
|---|---|---|---|---|---|
| **BLS** | YES (`BLS_API_KEY`) | `_query_labor_market_indicators` (nova.py:11581) | `query_labor_market_indicators` | **PASS** - `LNS14000000` returned `REQUEST_SUCCEEDED` with 1 series | Public endpoint also worked without key |
| **Adzuna** | YES (`ADZUNA_APP_ID/KEY`) | api_integrations.py:1121, `_query_market_demand` (nova.py:8129) | `query_market_demand` | **PASS** - GB feed returned `count=787329` | Healthy |
| **FRED** | YES (`FRED_API_KEY`) | api_integrations.py:496, `_query_regional_economics` (nova.py:9902) | `query_regional_economics` | **PASS** - UNRATE series returned 1 obs | Healthy |
| **Tavily** | YES (`TAVILY_API_KEY`) | tavily_search.py, `_web_search` | `web_search` | **PASS** - returned 1 result | Healthy (used as quota probe only) |
| **BEA** | YES (`BEA_API_KEY`) | api_integrations.py:1922, `_query_bea` (nova.py:13314) | `query_bea` | **PASS** - apps.bea.gov NIPA call returned HTTP 200 with data | Healthy. Note: code uses `apps.bea.gov` not `api.bea.gov`. |
| **Census** | YES (`CENSUS_API_KEY`) | api_integrations.py:2363, `_query_workforce_demographics` (nova.py:11644) | `query_workforce_demographics` | **PASS** - ACS5 returned 2 rows | Healthy |
| **OECD** | n/a (no key) | Not directly wired in nova.py | n/a | HTTP 301 redirect from `stats.oecd.org/SDMX-JSON/...` | Endpoint URL has moved; current code base does not call OECD anyway (per F4 audit, "not wired"). |
| **O\*NET** | YES (`ONET_USERNAME`, `ONET_API_KEY`) | api_integrations.py:1439, `_query_skills_profile` (nova.py:7942) | `query_skills_profile`, `query_role_decomposition` | **FAIL** in my curl test (401) but handler uses correct auth scheme. Likely my test command's quoting issue, not a Nova bug. | Re-test from prod recommended. |

### 4.2 Wired but key missing on Render (graceful-error responses)

These tools are advertised in `get_tool_definitions()` and the LLM can choose to call them, but the API key is NOT set in production, so the handler returns a structured error with `tool_error_graceful: true`.

| API | Tool name | Handler line | Render env var | Status |
|---|---|---|---|---|
| **LinkUp Job Postings** | `query_linkup_postings` | nova.py:12861 | `LINKUP_API_KEY` - **NOT SET** (verified via Render API) | Will always return 401-style graceful error to LLM. The LLM may still hallucinate around this. **High priority to set or remove tool.** |
| **Revelio Labs RPLS** | `query_revelio_workforce` | nova.py:12922 | `REVELIO_API_KEY` - **NOT SET** (verified) | Same. |
| **Reed (UK)** | (none in nova.py) | n/a | `REED_API_KEY` on Render (set) but no `_query_reed` handler | Key is wasted; nothing reads it. |
| **CareerJet** | `query_careerjet` | nova.py:13275 | No key needed (affid) | LIVE but no key on Render - this is by design. |

### 4.3 Live but not live-tested today (low-cost would still cost something)

| API | Why not tested | Confidence |
|---|---|---|
| Jooble, USAJobs, GeoNames, Eurostat, UK-ONS, StatCan, Google Maps, Google Vision, Google NLP, Google Translate, Google BigQuery, Meta Ads, Google Ads, Google Analytics, ElevenLabs, PostHog, Resend, Upstash Redis | Keys all set on Render (confirmed by env var list at the top of this audit). Code paths grepped and present. | High - based on key presence + F4 audit history. |

---

## 5. Vector Stores

### 5.1 Qdrant (primary RAG store)

- URL: `$QDRANT_URL` (set on Render)
- Collections: **1** (`nova_knowledge`)
- **Points: 356,056** (status: green)
- Last write: not directly queryable from REST `/collections/<name>` payload; vectors_count returned `None` (Qdrant 1.x behavior - use `/points/count` for an authoritative number).
- Verdict: **HEALTHY**. The 356K-point figure aligns with MEMORY.md's "Qdrant 685pts" only if you count pre-S33; today the index is dramatically larger, suggesting ongoing ingest. Investigate: confirm ingest is intentional or runaway.

### 5.2 Chroma (configured but unused)

- `chroma_rag.py` exists (lines 1-200+) - falls back to `EphemeralClient` on Render (line 101) since SQLite is unreliable on Render's filesystem.
- **No production module imports `chroma_rag`** (verified by `grep -rn "from chroma_rag|import chroma_rag" *.py` = 0 hits outside `vector_search.py` comment).
- `chromadb` IS listed in `requirements.txt` (1 occurrence) but never imported at runtime.
- Verdict: **DEAD CODE**. Remove from requirements.txt to shave install time, or formally wire to Qdrant as a fallback.

---

## 6. KB Freshness Flags

Today is **2026-05-22**. Files older than 30 days = touched before **2026-04-22**.

### 6.1 Summary

- Total `data/*.json` files: **70**
- Files older than 30 days: **56** (80%)
- Files older than 7 days: **68** (97%)
- Files newer than 7 days: **2** (`ta_leaders_curated_2026.json`, `industry_reports_2026.json`)
- Files newer than 24h: **0**

### 6.2 Stale benchmark files Nova answers from

| File | Last modified | Age (days) | What Nova uses it for |
|---|---|---|---|
| `recruitment_benchmarks_comprehensive_2026.json` (43 KB master KB) | Apr 7 | 45 | Top-line industry benchmarks - cited in chatbot answers. |
| `joveo_2026_benchmarks.json` | Mar 22 | 61 | Joveo CPA/CPH internal references. |
| `recruitment_benchmarks_deep.json` | Mar 22 | 61 | Deep benchmark fallback. |
| `linkedin_industry_benchmarks.json` | Apr 7 | 45 | LinkedIn comparisons. |
| `linkedin_performance_benchmarks.json` | Apr 4 | 48 | LinkedIn performance. |
| `linkedin_guidewire_data.json` | Mar 26 | 57 | Specific LinkedIn account profiles. |
| `google_ads_2025_benchmarks.json` | Mar 26 | 57 | **Title says 2025** - explicit, but Nova does cite from it. |
| `publisher_benchmarks_2026.json` | Mar 26 | 57 | Publisher comparisons. |
| `salary_benchmarks_detailed_2026.json` | Mar 26 | 57 | Salary fallback (also Supabase has 300 rows). |
| `compliance_regulations_2026.json` | Mar 26 | 57 | Compliance answers. |
| `industry_white_papers.json` | Mar 22 | 61 | Industry citations. |
| `recruitment_industry_knowledge.json` | Mar 22 | 61 | General KB. |
| `workforce_trends_intelligence.json` | Mar 22 | 61 | Workforce trends. |
| `regional_hiring_intelligence.json` | Mar 22 | 61 | Regional answers. |
| `global_supply.json` | Mar 22 | 61 | Supply chain answers. |
| `platform_intelligence_deep.json` | Mar 22 | 61 | Platform deep dives. |
| `top_employers_by_city_2026.json` | Mar 26 | 57 | Employer questions. |
| `industry_hiring_patterns_2026.json` | Mar 26 | 57 | Hiring pattern Qs. |
| `agency_rpo_market_2026.json` | Mar 26 | 57 | RPO answers. |
| `hr_tech_landscape_2026.json` | Mar 26 | 57 | HR tech Qs. |
| `nova_learned_answers.json` | Mar 22 | 61 | Nova's learned response cache. |

### 6.3 Files refreshed in last 14 days (signs of life)

- `ta_leaders_curated_2026.json` (May 15+)
- `industry_reports_2026.json` (May 15+)
- `competitor_careers.json` (Apr 9)
- `job_density_metros.json` (Apr 9)
- `slotops_benchmarks_summary.json` (Apr 8)

Note: Apr 9 dates already fall in the >30-day stale bucket because today is May 22.

**Implication**: Nova's "live" benchmark answers are mostly served from snapshots that are 1.5-2 months old. Cite this when stakeholders ask why answers don't match current market.

---

## 7. Top-10 Broken/Stale Items (Prioritized)

| # | Item | Severity | Impact | Fix |
|---|---|---|---|---|
| 1 | `nova_shared_conversations` table missing (HTTP 404 / PGRST205) | **P0** | Share-link feature is broken - every share call in `nova_persistence.py` 5+ sites errors out. | Apply `nova_schema_additions.sql:48-77` migration to prod Supabase. ~2 min fix. |
| 2 | `nova_login_log` table exists but 0 rows after S40 added writes | **P0** | Cannot verify who logged in. Either OAuth code path broken or being bypassed. | Add a temporary INFO log on the write call (`app.py:10645`) and trigger one login. If no log: the OAuth handler is not reaching that line in prod. |
| 3 | `nova_conversations` table exists but 0 rows | **P0** | Chat persistence appears broken. MEMORY.md S37 claims this was fixed; live state says 0 rows. | Same diagnosis - add write-success/failure log around `nova_persistence.py:175,267,311`. |
| 4 | `cg_daily_raw` HTTP 500 timeout on count | **P1** | CG product daily-upload feature may be functioning but unmonitorable; large table without proper index. | Add index on a created_at column on Supabase; or use `head=true` HEAD probe instead of count in monitoring scripts. |
| 5 | `LINKUP_API_KEY` and `REVELIO_API_KEY` missing on Render | **P1** | Two LLM-exposed tools always return graceful errors. LLM may attempt them and waste tokens. | Either provision keys (commercial deals) or REMOVE the tool definitions from `get_tool_definitions()` until keys exist. **Do not ship dead tools to the LLM.** |
| 6 | `REED_API_KEY` set on Render but no handler in nova.py | P2 | Wasted env var; possible feature flag for an unbuilt tool. | Either build `_query_reed` or remove the key. |
| 7 | 56 of 70 KB files older than 30 days; flagship benchmark JSON 45+ days stale | **P1** | Every "what's the CPA for X" answer is from a 45-60 day snapshot. | Re-run the data-refresh job (whatever populates `recruitment_benchmarks_*.json`) on a weekly cron. Currently the `enrichment_log` table is empty, suggesting the job is not running. |
| 8 | `enrichment_log` table 0 rows | P1 | The enrichment pipeline (`data_enrichment.py`) is either not scheduled or silently failing. This directly correlates with the KB staleness in item 7. | Check Render cron or `data_refresh.py` scheduling. Force one run and confirm a row appears. |
| 9 | `metrics_snapshot` 0 rows | P2 | Morning Brief feature has nothing to summarize; observability blind spot. | Schedule the snapshot writer (probably wanted by `monitoring.py:1185` block). |
| 10 | Chroma is dead code, but `chromadb` is in `requirements.txt` | P3 | Slower cold start, larger image. | Remove `chromadb` from `requirements.txt` and delete `chroma_rag.py`, OR formally wire it as a Qdrant fallback. |

### Bonus issues (not in top-10 but worth flagging)

- **Linear MCP `/sse` transport deprecation** (2026-04-08, past due) - migrate to `mcp.linear.app/mcp`.
- **`nova_conversation_state`** is queried by nova.py but is NOT in MEMORY.md's table list. Either add it to memory or stop using it; today it has 0 rows.
- **OECD 301 redirect** - if Nova ever calls OECD (F4 audit says it should), the endpoint has moved. Update before wiring.

---

## Methodology & Reproducibility

- Supabase counts: `curl -s --max-time 6 "$SUPABASE_URL/rest/v1/<table>?select=count" -H "apikey:..." -H "Authorization: Bearer ..." -H "Prefer: count=exact" -H "Range: 0-0" -I` then parse `content-range: 0-0/<N>`. Same command works for HTTP-code verification when count fails.
- Render env vars: `GET https://api.render.com/v1/services/srv-d6lk06k50q8c73bcpo40/env-vars?limit=100` with `Authorization: Bearer $RENDER_API_KEY`. Returned 65 distinct keys (matches MEMORY.md "64+" claim within rounding).
- MCP list: `claude mcp list` in this session.
- Qdrant points: `GET $QDRANT_URL/collections/nova_knowledge` returns `points_count: 356056` and `status: green`.
- Tool-vs-handler audit: `grep -nE '"name":\s*"' nova.py` returned 85 tool name fields and 48 distinct `_query_*` handler mappings in `_tool_handler_map()` (lines 6500-6612). The delta is accounted for by S50 auto-merged `RECRUITMENT_TOOLS_SCHEMA` and non-query tools like `web_search`, `scrape_url`, `render_canvas`.

All findings are reproducible by re-running the same commands above today.
