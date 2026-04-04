# Nova AI Suite -- Technical Architecture

*Last updated: 2026-04-02*
*Author: Shubham Singh Chandel, Chief of Strategic Initiatives, Joveo*
*Audience: Engineering leads, technical stakeholders, infrastructure reviewers*

---

## By The Numbers

| Metric | Value |
|--------|-------|
| **AI Tools** | 57 across 20+ domains (salary, demand, channels, publishers, benchmarks, skills, H-1B, federal jobs, economics, demographics, remote jobs, labor market, predictions, competitive intel, scorecards, copilot, morning brief, canvas, ATS, CG automation) |
| **Live APIs** | 22+ (BLS, O*NET, Adzuna, Jooble, FRED+JOLTS, CareerOneStop, USAJobs, BEA, Census, RemoteOK, H-1B/LCA, Google Trends, CareerJet, Eurostat, UK ONS, StatCan, Tavily, GeoNames) |
| **Vector Search** | Hybrid: Voyage AI embeddings + BM25 keyword + Reciprocal Rank Fusion |
| **LLM Providers** | 23-provider fallback matrix with per-provider circuit breakers |
| **Cache Layers** | 3: L1 in-memory (200 entries) + L3 Upstash Redis + Supabase persistent |
| **Tool Execution** | Parallel: ThreadPoolExecutor 5 workers, 15s per-tool timeout |
| **Quality Score** | 9/10 with response templates, post-processing, citation injection |
| **Knowledge Base** | 25+ files, 5,415 vector-indexed chunks, ~0.9 MB in memory |
| **Supabase Tables** | 22 (17 Nova + 5 CG) with Row-Level Security |
| **Qdrant Vectors** | 685 points, 512-dim Voyage AI voyage-3-lite |
| **MCP Servers** | 28+ (Context7, Sequential Thinking, GitHub, Playwright, Render, Serena, Sentry, Supabase, PostHog, Resend, and more) |
| **Monitoring** | Sentry (80-90% noise reduction) + Resend email alerts + AutoQC (60s) + Data Matrix Monitor (12h) |
| **Data Refresh** | Weekly auto-refresh: BLS, Adzuna, FRED, Google Trends |
| **Pre-compute** | 1,000 city-role combos (50 cities x 20 roles) + 100 comparison combos |
| **Eval Score** | 8/9 PASS (greeting 0.67s, salary 1.07s, comparison 22s, media plan 81s) |

---

## Resilience and Fallback Architecture

Every layer has built-in redundancy:

| Layer | Primary | Fallback | Last Resort |
|-------|---------|----------|-------------|
| **LLM** | Claude Haiku 4.5 | Gemini 2.5 Flash | GPT-4o, then 20+ free providers |
| **Data Cache** | L1 in-memory | Upstash Redis | Supabase persistent |
| **Search** | Vector (Voyage AI) | BM25 keyword | Reciprocal Rank Fusion blend |
| **Web Scraping** | Apify | Jina AI | Firecrawl |
| **Monitoring** | AutoQC (60s) | Data Matrix Monitor (12h) | Sentry + Resend alerts |
| **Health** | /api/health (always 200) | /api/health/ping (instant) | HEAD / support |
| **Deploy** | Background vector indexing | 300s enrichment delay | 90s AutoQC grace period |
| **LLM Routing** | Complexity scoring | Circuit breaker mesh | Provider health scoring |

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Knowledge Base and Data Layer](#2-knowledge-base-and-data-layer)
3. [LLM Router v4.1](#3-llm-router-v41)
4. [Tool Execution Engine](#4-tool-execution-engine)
5. [Data Sources](#5-data-sources)
6. [Data Enrichment Pipeline](#6-data-enrichment-pipeline)
7. [Quality Assurance Stack](#7-quality-assurance-stack)
8. [Reliability and Monitoring](#8-reliability-and-monitoring)
9. [Infrastructure and Deploy](#9-infrastructure-and-deploy)
10. [MCP Servers](#10-mcp-servers)
11. [Security](#11-security)
12. [CG Automation Architecture](#12-cg-automation-architecture)
13. [Key Engineering Decisions](#13-key-engineering-decisions)

---

## 1. Architecture Overview

Nova AI Suite is a recruitment intelligence platform built on Python's standard library HTTP server -- no Flask, Django, or any web framework. The application is a single `BaseHTTPRequestHandler` subclass (`MediaPlanHandler`) in `app.py` (18,359 lines), wrapped in a WSGI adapter (`wsgi.py`, 526 lines) for production serving.

### Runtime Stack

| Component | Technology | Configuration |
|-----------|-----------|---------------|
| Application server | gunicorn | 4 workers, gevent worker class, 1000 connections per worker |
| Concurrency model | gevent greenlets | `monkey.patch_all()` at import time in `wsgi.py` |
| Timeout | 120s | Covers long-running SSE chat streams |
| Preloading | `--preload` | Shares knowledge base across forked workers (copy-on-write) |
| Streaming | WSGI pipe adapter | Handler writes to pipe write-end; WSGI iterator reads from read-end |
| WebSocket | Pure Python RFC 6455 | `/ws/chat` endpoint, SSE fallback for clients without WS support |

### Procfile

```
web: gunicorn --bind 0.0.0.0:$PORT --worker-class gevent --workers 4 --worker-connections 1000 --timeout 120 --preload --access-logfile - --error-logfile - wsgi:app
```

### WSGI Architecture

The WSGI adapter (`wsgi.py`) bridges the stdlib handler with gunicorn. Key design:

- **SSE/streaming support**: A pipe-based architecture where the handler runs in a background thread, writing to the pipe's write end. The WSGI response iterator yields chunks from the read end, enabling true streaming through gunicorn.
- **FakeSocket**: A minimal socket stand-in satisfies `BaseHTTPRequestHandler`'s constructor, which expects a real socket. `rfile` and `wfile` are overridden before each request.
- **Multipart uploads**: `self.rfile` is set to a `BytesIO` containing the full request body, so `cgi.FieldStorage` works unchanged.

### Templates

Templates are served inline from the `templates/` directory. There are 35 HTML files plus a `partials/` subdirectory. Entry points:

- `hub.html` -- Product suite landing page
- `index.html` -- Media plan generator
- `nova.html` -- Nova AI chatbot
- `platform.html` -- Unified platform with sidebar navigation, Cmd+K search
- `health-dashboard.html` -- Operational health monitoring

### Static Assets

Static files are served from `static/` with cache-busting:

- **JS/CSS**: `no-cache` + `must-revalidate` with ETag validation (changed from 1-year immutable in S35 after discovering browsers held stale `platform.js` indefinitely)
- **Images**: `max-age=31536000, immutable` (content-addressable, safe to cache forever)
- **Minification**: `static_minifier.py` runs at import time, minifying CSS/JS for zero-latency serving (28% size reduction)

### Source Code Organization

106 Python modules in the project root, separated by concern:

| Module | Lines | Purpose |
|--------|-------|---------|
| `app.py` | 18,359 | Main server, routes, all API endpoints |
| `data_enrichment.py` | 2,171 | Background data freshness engine |
| `vector_search.py` | 1,699 | Hybrid search (Voyage AI + BM25 + RRF) |
| `data_matrix_monitor.py` | 1,580 | Background data availability checker |
| `wsgi.py` | 526 | WSGI adapter with streaming support |
| `circuit_breaker_mesh.py` | 454 | Per-provider failure isolation |
| `auto_qc.py` | 347 | Background health monitor |

---

## 2. Knowledge Base and Data Layer

### Knowledge Base

25+ JSON files loaded at startup from the `data/` directory (68 total files and subdirectories in `data/`). The KB is pre-warmed during `_run_deferred_startup()` in `wsgi.py` and shared across all gunicorn workers via `--preload` (copy-on-write memory savings).

Contents include:

- `channels_db.json` -- Job board and recruitment channel database
- `joveo_cpa_benchmarks_2026.json` -- 304 categories from JAX platform (Oct 2025 - Feb 2026)
- `joveo_global_supply_repository.json` -- 7,053 publishers worldwide
- `client_media_plans_kb.json` -- Client plan templates and historical data
- `h1b_salary_intelligence.json` -- H-1B/LCA visa salary data
- `labor_market_outlook_2026.json` -- Labor market forecasts
- `hr_tech_landscape_2026.json` -- HR tech vendor landscape
- `industry_hiring_patterns_2026.json` -- Hiring patterns by industry
- `ad_benchmarks_recruitment_2026.json` -- Recruitment advertising benchmarks
- `compliance_regulations_2026.json` -- Employment compliance regulations
- `international_sources.json` -- International data source mappings
- `global_supply.json` -- Global publisher supply data
- `google_ads_2025_benchmarks.json` -- Google Ads recruitment benchmarks

KB stats at startup: 24 keys, approximately 0.9 MB in memory.

### Supabase (PostgreSQL)

22 tables total (17 Nova + 5 CG Automation), all with Row Level Security (RLS):

**Nova tables (active read/write)**:
- `cache` -- Query result cache (persistent L3)
- `enrichment_log` -- Data enrichment audit trail
- `nova_conversations` -- Chat history
- `nova_documents` -- Uploaded documents
- `nova_shared_conversations` -- Shareable chat links
- `metrics_snapshot` -- Performance metrics
- `nova_memory` -- User personalization state

**Nova tables (active read-only)**:
- `knowledge_base` -- Structured KB entries
- `channel_benchmarks` -- Channel performance benchmarks

**Nova tables (seeded)**:
- `salary_data` (48 rows), `compliance_rules` (8), `market_trends` (8), `vendor_profiles` (8), `supply_repository` (20)

**CG Automation tables (S33)**:
- `cg_jobs`, `cg_action_plans`, `cg_schedules`, `cg_uploads`, `cg_user_sessions`

**Dropped (S34)**: `nova_avatars`, `nova_module_usage`, `nova_campaigns`, `research_cache` -- confirmed dead code.

### Vector Search (Qdrant + Voyage AI)

Implemented in `vector_search.py` (1,699 lines). Hybrid search combining semantic vector similarity with exact keyword matching.

**Embedding model**: Voyage AI `voyage-3-lite` (512-dimensional vectors via HTTP API, no pip dependencies).

**Storage tiers** (in priority order):
1. **Qdrant Cloud** -- Persistent, shared across deploys. Collection `nova_knowledge`, 1024-dim cosine similarity. 685 points indexed.
2. **In-memory dict** -- Fast ephemeral fallback, per-process.
3. **BM25 index** -- Pure Python, always built alongside vector index.
4. **TF-IDF index** -- Warm standby if both vector and BM25 fail.

**Search strategy**: Reciprocal Rank Fusion (RRF, k=60) combining vector similarity scores with BM25 keyword scores. The hybrid approach handles both semantic queries ("what are the best channels for healthcare hiring") and exact-match queries ("Indeed CPC benchmark").

**Indexing**: Runs in a background daemon thread during startup. Indexes 5,415 chunks from all KB files. Takes 3-5 minutes. Previously ran synchronously in `_run_deferred_startup()`, blocking gunicorn from binding to `$PORT` and causing Render deploy timeouts -- fixed in S35 (`b3042d8`).

### Cache Architecture (3 layers)

| Layer | Backend | Capacity | TTL | Purpose |
|-------|---------|----------|-----|---------|
| L1 | In-memory dict | 200 entries | Session lifetime | Hot query cache, zero-latency |
| L2 | Upstash Redis | Unlimited | Configurable per key | Cross-request cache, shared state |
| L3 | Supabase `cache` table | Unlimited | 24h default | Persistent, survives deploys |

Cache lookup order: L1 -> L2 -> L3 -> compute. Results written back to all missed layers.

### Pre-compute Pipeline

Salary and demand data pre-computed for 50 cities x 20 roles (1,000 combinations) during startup. Stored in L1 cache. Runs with a 600s delay after startup to avoid competing with vector indexing and enrichment.

---

## 3. LLM Router v4.1

23 LLM providers with intelligent routing based on query complexity, cost, and provider health.

### Routing Strategy

```
Query arrives
  |
  v
Complexity classifier (word count, data intent keywords)
  |
  +-- Simple (greetings, <5 words) --> Free tier
  |
  +-- Data intent (5+ words, contains salary/market/benchmark keywords) --> Paid tier
  |
  v
Provider selection (within tier)
  |
  +-- Check circuit breaker state (CLOSED/HALF_OPEN/OPEN)
  +-- Check health score (0-100, weighted: 60% success rate, 20% latency, 20% recency)
  +-- Select highest-health available provider
  |
  v
Execute with timeout --> Success: record success, update health
                     --> Failure: record failure, try next in chain
```

### Provider Tiers

**Paid tier** (quality-first for data queries):

| Priority | Provider | Use Case |
|----------|----------|----------|
| #1 (primary) | Claude Haiku 4.5 | All data queries -- quality-first routing |
| #2 (fallback) | Gemini 2.5 Flash | Free, good quality |
| #3 (fallback) | GPT-4o | Premium fallback |
| Synthesis | Claude Haiku | All narrative generation (plan gen, summaries) |
| Heavy reasoning | Claude Sonnet / Opus | Complex multi-step analysis |

**Free tier** (simple queries, greetings):

Groq, Cerebras, Zhipu, Mistral, SambaNova, SiliconFlow, Nvidia NIM, Cloudflare, Together, HuggingFace, 7 OpenRouter models.

### Circuit Breaker Mesh

Implemented in `circuit_breaker_mesh.py` (454 lines). Each of the 24 LLM providers has an independent circuit breaker with three states:

- **CLOSED** (healthy) -- Requests flow normally.
- **HALF_OPEN** (testing) -- Allow limited requests after cooldown. Promoted to CLOSED on success, reverted to OPEN on failure.
- **OPEN** (tripped) -- All requests blocked until cooldown expires.

Health score formula:
```
score = (success_rate * 0.60) + (latency_percentile * 0.20) + (recency_score * 0.20)
```

Exponential backoff on repeated failures. Rolling window of 50 most recent latency measurements per provider. Thread-safe via per-provider locks.

---

## 4. Tool Execution Engine

57 tools spanning salary data, market demand, channel benchmarks, publisher intelligence, skills analysis, H-1B visa data, federal jobs, economics, demographics, remote jobs, labor market predictions, competitive intel, scorecards, copilot features, morning briefs, canvas, ATS integrations, and more.

### Execution Model

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Executor | `ThreadPoolExecutor` | 5 concurrent workers |
| Per-tool timeout | 15s | Prevent single tool from blocking pipeline |
| Max iterations (Claude) | 5 | Balance depth vs. latency |
| Max iterations (free LLMs) | 4 | Free models less reliable at iteration |
| Tool calling | Mandatory | Minimum 3 tools for complex queries |
| Reject + retry | Enabled | Responses without tool calls are rejected and retried |

### Comparison Query Optimization (S35)

City-vs-city comparison queries (e.g., "NY vs Chicago nurses") were timing out at >90 seconds. Optimizations applied:

- **Batched tool calls**: Multiple tools called in parallel per iteration instead of serially.
- **Reduced max iterations**: 3 (down from 5) for comparison queries specifically.
- **Loop cap**: 35s hard timeout on the tool-calling loop.
- **Early exit**: If both cities have sufficient data, exit without exhausting iterations.

Result: 22s average (down from >90s timeout).

### Dynamic Tool Loop Budget

The tool execution loop has a budget system that tracks elapsed time and remaining iterations. Early exit triggers when:

1. Confidence score exceeds 0.90 and minimum tool calls are met.
2. Loop cap timer expires (35s for comparisons, uncapped otherwise).
3. All requested data dimensions have been populated.

---

## 5. Data Sources

### Government APIs (7)

| API | Data | Refresh |
|-----|------|---------|
| BLS (Bureau of Labor Statistics) | Wage data, employment statistics | Weekly |
| O*NET v2.0 | Occupation data, skills, education requirements | On-demand |
| FRED v2 + JOLTS | Economic indicators, job openings/separations | Weekly |
| USAJobs | Federal job postings | On-demand |
| BEA (Bureau of Economic Analysis) | GDP, regional economic data | On-demand |
| Census Bureau | Demographics, population, commute data | On-demand |
| H-1B/LCA | Visa salary data, employer filings | On-demand |

### Job Market APIs (5)

| API | Coverage | Notes |
|-----|----------|-------|
| Adzuna | US, UK, EU markets | Weekly refresh |
| Jooble | Global aggregator | On-demand |
| RemoteOK | Remote-first positions | On-demand |
| CareerJet | 60+ countries | Added S30, broadest international coverage |
| CareerOneStop v2 | US career resources | On-demand |

### International APIs (3)

| API | Coverage |
|-----|----------|
| Eurostat | EU labor statistics |
| UK ONS | UK employment data |
| StatCan | Canadian labor market |

### Research and Enrichment APIs (4)

| API | Purpose |
|-----|---------|
| Google Trends | Search interest trends, weekly refresh |
| Tavily | Web research, real-time information |
| GeoNames | Geographic data, city/metro resolution |
| Firecrawl | Web scraping (demoted to #3, no credits) |

### Web Scraper Priority Order

1. **Apify** -- Primary scraper, most reliable.
2. **Jina** -- Secondary, good for content extraction.
3. **Firecrawl** -- Tertiary, demoted from #1 after credits exhausted.

### Automatic Refresh Schedule

BLS, Adzuna, FRED, and Google Trends data auto-refresh every 7 days via the data refresh pipeline (`data_refresh.py`). Triggered by the background daemon started in `wsgi.py` deferred startup.

---

## 6. Data Enrichment Pipeline

Implemented in `data_enrichment.py` (2,171 lines). The third pillar of self-maintaining infrastructure alongside `auto_qc.py` and `data_matrix_monitor.py`.

### Three Pillars of Self-Maintenance

| Pillar | Module | Question It Answers | Lines |
|--------|--------|---------------------|-------|
| Code health | `auto_qc.py` | "Is the code working?" | 347 |
| Data access | `data_matrix_monitor.py` | "Can products access data?" | 1,580 |
| Data freshness | `data_enrichment.py` | "Is the data fresh?" | 2,171 |

### Enrichment Sources (13)

Each source has its own enrichment function, freshness threshold, and circuit breaker:

| Interval | Sources | Rationale |
|----------|---------|-----------|
| 6 hours | Recruitment news, live market data | Time-sensitive |
| 12 hours | Job board pricing, market trends | Slower-moving |
| 7 days | Salary data, compliance updates | Stable data |

### Data Flow

1. Hourly tick checks each source against its per-source freshness interval.
2. Stale sources trigger their enrichment function.
3. Results written to both local JSON files and Supabase (upsert with `on_conflict` merge).
4. LLM generates summaries for applicable data types (news, salary, compliance).
5. State persisted to Supabase `enrichment_log` table + local `enrichment_state.json` fallback.
6. Status exposed via `GET /api/health/enrichment` (admin-protected).

### Circuit Breakers (Per-Table)

After a 401 Unauthorized on a specific Supabase table, only upserts to that table are blocked for 5 minutes (not all tables for 1 hour). This prevents one bad table from killing all 13 enrichment sources.

### Startup Behavior

- **300s initial delay** after server start, allowing services to warm up before the first enrichment cycle.
- **Background daemon thread**: Non-blocking, does not interfere with request handling.
- **Enrichment state survives deploys** via Supabase persistence (local JSON is a fallback).

### S35 Fix: PostgREST `on_conflict` Encoding

Enrichment was producing 400 errors because PostgREST requires `on_conflict` column names to be URL-encoded with commas as `%2C` rather than literal commas. Fixed in commit `7477c3c`.

---

## 7. Quality Assurance Stack

### AutoQC (Background Health Monitor)

`auto_qc.py` (347 lines) runs every 60 seconds after a 90-second startup grace period. Validates:

- API endpoint availability (health, chat, generate).
- Data pipeline connectivity (Supabase, Redis, vector search).
- Tool execution (sample tool calls).
- Response quality (confidence score thresholds).

Results written to `data/auto_qc_results.json` and `data/auto_qc_dynamic_tests.json`.

### Response Quality Pipeline

**Few-shot examples**: 3 ideal responses embedded in all system prompts, demonstrating expected tool usage, citation format, and response structure.

**Response templates**: 8 query types with specialized formatting:

| Template | Trigger | Format |
|----------|---------|--------|
| `salary` | Salary/compensation queries | Table with percentiles, source citations |
| `media_plan` | Plan generation requests | Structured plan with channels, budgets, timelines |
| `comparison` | City-vs-city, role-vs-role | Side-by-side comparison tables |
| `competitive` | Competitive intelligence | Competitor cards with positioning |
| `channels` | Channel recommendations | Ranked channel list with rationale |
| `compliance` | Compliance queries | Regulation summary with risk flags |
| `morning_brief` | Daily briefing requests | Executive summary format |
| `general` | Everything else | Conversational with citations |

**Post-processing pipeline** (applied to every response):

1. Auto-format numbers (currency, percentages, large numbers).
2. Inject citations from tool call results.
3. Append contextual follow-up question suggestions.
4. Compute quality score (0-100) based on tool usage, data density, and response completeness.

### User Personalization

Tracks user roles, locations, and industries across conversations. After 3+ queries from the same user, responses are personalized:

- Location-specific defaults (salary data defaults to user's metro).
- Industry-specific benchmarks surfaced proactively.
- Role-relevant tools prioritized in the execution pipeline.

Personalization state stored in the `nova_memory` Supabase table.

### A/B Testing

10% of traffic is routed through a provider quality comparison pipeline. Measures:

- Response quality score (0-100).
- Tool utilization rate.
- Latency.
- User satisfaction signals (follow-up queries, session length).

Used to validate routing changes before promoting to 100% traffic.

---

## 8. Reliability and Monitoring

### Sentry Integration

80-90% noise reduction via a custom `before_send` filter that suppresses 7 categories of non-actionable errors:

- Expected timeouts on long-running queries.
- Rate limit responses from free LLM providers.
- Network transients on external API calls.
- Gevent greenlet lifecycle events (`GreenletExit`).
- `FuturesTimeoutError` `UnboundLocalError` on CPython 3.14.3 (18 references fixed in S19).

Environment tag: `"production"`. Source maps enabled for JS errors.

### Alert Manager

Email alerts via Resend for critical health drops. Triggers:

- AutoQC score below threshold.
- Data matrix monitor detects stale critical data.
- Circuit breaker mesh has >50% of providers in OPEN state.

### SLO Monitoring

| Endpoint | P50 | P99 | Budget |
|----------|-----|-----|--------|
| `/api/chat` | 15s | 65s | Includes LLM round-trips |
| `/api/generate` | 20s | 45s | Plan generation pipeline |
| `/api/health/ping` | <10ms | <50ms | Instant, no dependencies |
| `/api/health` | 3s | 9s | Parallel dependency checks |

### Data Matrix Monitor

`data_matrix_monitor.py` (1,580 lines) runs 12-hour background checks on all data sources. Self-healing: when stale data is detected, the monitor triggers the enrichment pipeline for that specific source.

### Health Endpoint Design

The health endpoint (`/api/health`) always returns HTTP 200 with a JSON body containing per-subsystem status. This is a deliberate design choice -- Render's health check mechanism requires 200 to keep the service running. Actual health status is encoded in the response body, not the status code.

Previous behavior (returning 503 during enrichment bursts) caused Render to consider the service unhealthy and block deploys. Fixed in S35 (`065b0db`).

---

## 9. Infrastructure and Deploy

### Render.com Configuration

| Setting | Value |
|---------|-------|
| Tier | Standard (paid) |
| Service ID | `srv-d6lk06k50q8c73bcpo40` |
| URL | https://media-plan-generator.onrender.com/ |
| Custom domain | `nova.joveo.com` (CNAME pending IT DNS configuration) |
| Auto-deploy | From `main` branch (unreliable -- manual API trigger recommended) |
| Environment variables | 45 total |

### Startup Sequence

The deferred startup in `wsgi.py` runs the following in order:

1. **Knowledge base pre-warm** -- Load all 25+ JSON files into memory.
2. **Vector search indexing** -- Background daemon thread, 5,415 chunks, 3-5 minutes. Non-blocking.
3. **Data refresh pipeline** -- Start weekly auto-refresh timers.
4. **Proactive health checker** -- Start Sentry-integrated health monitoring.
5. **Proactive intelligence engine** -- Start trend detection and alerting.
6. **Feature store initialization** -- Load feature flags and experiments.
7. **API key authentication** -- Initialize auth module.
8. **Module preloading** -- Preload `vector_search`, `slack_alerts`, `calendar_sync` to avoid lazy-import latency in health checks.
9. **Deploy warmup flag** -- Set `_DEPLOY_WARMUP_COMPLETE = True`.

### Startup Grace Periods

| Component | Delay | Rationale |
|-----------|-------|-----------|
| Enrichment pipeline | 300s | Let core services warm up first |
| AutoQC | 90s | Wait for startup to complete |
| Pre-compute (50x20) | 600s | Avoid competing with vector indexing |

### The Port-Binding Fix (S35)

Root cause of repeated deploy failures: `_vector_index_kb()` ran synchronously during deferred startup, taking 5 minutes to index 5,415 chunks. This blocked gunicorn from binding to `$PORT`. Render's port scan timed out after ~3 minutes, marking the deploy as failed.

Fix (`b3042d8`): Vector indexing moved to a background daemon thread. gunicorn binds to `$PORT` immediately. Deploy promoted to live in under 3 minutes.

### Static Asset Cache-Busting (S35)

JS/CSS files were served with `max-age=31536000, immutable` (1-year cache). After deploying `platform.js` changes, users saw stale JavaScript indefinitely. Fixed by switching to `no-cache, must-revalidate` with ETag validation. Images retain 1-year immutable caching since they are content-addressable.

---

## 10. MCP Servers

28+ MCP (Model Context Protocol) servers are configured for development tooling. These power the Claude Code development environment, not the production application.

### Active MCP Servers

| Server | Purpose |
|--------|---------|
| Context7 | Library documentation lookup |
| Sequential Thinking | Structured multi-step reasoning |
| GitHub | Repository operations |
| Playwright | Browser automation and E2E testing |
| Render | Deployment management |
| Serena | Semantic code understanding, session persistence |
| Sentry | Error monitoring integration |
| DeepWiki | Documentation research |
| Supabase | Database management |
| PostHog | Product analytics |
| Resend | Email sending (alerts) |
| Chroma | Vector DB operations |
| Excalidraw | Architecture diagrams |
| RAG-Memory | Retrieval-augmented memory |
| Ruflo (claude-flow) | Multi-agent orchestration |
| Todoist | Task management |
| Firecrawl | Web scraping |

### Missing / Not Yet Configured

Apify, Qdrant, Google Drive, Notion.

---

## 11. Security

### Authentication

**Google OAuth**: Joveo SSO with domain restriction (`@joveo.com` only). Implemented in `nova-auth.js` and `nova-auth-gate.js`. Users outside the Joveo domain cannot access the platform.

**Admin key bypass**: `NOVA_ADMIN_KEY` environment variable allows API access for testing and automation without OAuth. Only `/api/admin/*` endpoints require authentication -- all other APIs are public.

### Network Security

- **CORS**: Explicit allowed origins. Previously `allow_origins=["*"]` (wildcard), tightened in S35 to explicit domains.
- **CSP headers**: Content Security Policy headers on all HTML responses.
- **HTTPS enforcement**: All production traffic over TLS via Render's edge.

### Input Validation

- **System boundaries**: All user input validated at API entry points.
- **File path sanitization**: Directory traversal prevention on template and static file serving.
- **Webhook URL SSRF validation** (S35): HTTPS required for all webhook URLs. Private IP ranges (10.x, 172.16-31.x, 192.168.x, 127.x, ::1) blocked via `socket.getaddrinfo()` resolution before making outbound requests.

### Secrets Management

- All API keys stored as Render environment variables (45 total).
- No secrets in source code or git history.
- `.env` files excluded from version control.

---

## 12. CG Automation Architecture

CG Automation is a separate microservice for Craigslist posting optimization. It shares Supabase infrastructure with Nova but runs as an independent deployment.

### Stack

| Component | Technology |
|-----------|-----------|
| Backend | FastAPI |
| Frontend | React + Tailwind CSS |
| Python | 3.11.9 (pinned via `.python-version` + `runtime.txt`) |
| Deployment | Render.com, service ID `srv-d76h116a2pns73eo4d6g` |
| URL | https://cg-automation.onrender.com |

### Engine Pipeline

9-step optimization pipeline in `engine.py` (900 lines). Processes 98,665 rows of Apex campaign data (5,707 posts across 397 locations) in 8 seconds.

### LLM Router (CG-specific)

8 providers with cost-optimized routing (CG queries are simpler than Nova):

| Priority | Provider |
|----------|----------|
| 1 | Gemini 2.5 Flash |
| 2 | Groq |
| 3 | Cerebras |
| 4 | SambaNova |
| 5 | Mistral |
| 6 | Together |
| 7 | OpenRouter |
| 8 | Claude (fallback) |

### Frontend

React application with 4 primary views:

1. **Dashboard** -- Campaign overview and KPIs.
2. **Action Plan** -- AI-generated posting recommendations.
3. **Intelligence** -- Market intelligence and trends.
4. **Reports** -- Performance reports and analytics.

### Data Layer

5 Supabase tables (`cg_jobs`, `cg_action_plans`, `cg_schedules`, `cg_uploads`, `cg_user_sessions`).

Schedules are persisted to Supabase and reloaded on startup via a FastAPI lifespan handler (fixed in S35 -- previously lost on Render restarts).

### Security (S35 Hardening)

- **CORS**: Explicit origins (`cg-automation.onrender.com`, `localhost:5173`, `localhost:3000`) instead of wildcard.
- **File upload limit**: 50 MB maximum, enforced with HTTP 413 before pandas parsing begins.
- **Webhook SSRF validation**: HTTPS required, private IP ranges blocked.
- **Admin key**: `CG_ADMIN_KEY` environment variable for protected endpoints.

### Python Version Pin

CG Automation was originally running on Python 3.14, which caused Pydantic V1 compatibility warnings. Pinned to 3.11.9 via `.python-version` and `runtime.txt` in S34.

---

## 13. Key Engineering Decisions

### Why No Web Framework

The entire application is built on Python's `http.server.BaseHTTPRequestHandler`. This was a deliberate choice:

- **Zero dependencies** for the core HTTP layer -- no framework version conflicts.
- **Full control** over request handling, streaming, and WebSocket negotiation.
- **WSGI compatibility** via a thin adapter, allowing gunicorn for production without changing the handler.
- **Trade-off**: `app.py` is 18,359 lines. A framework would provide routing, middleware, and request parsing out of the box. The current approach requires manual implementation of these concerns.

### Why Hybrid Search Over Pure Vector

Pure vector search misses exact-match queries. If a user asks for "Indeed CPC benchmark", the BM25 component catches the exact keyword match even if the vector similarity score is low. Reciprocal Rank Fusion combines both signals without requiring manual weight tuning.

### Why gevent Over Threading

gevent's cooperative multitasking handles thousands of concurrent connections with minimal overhead. The `monkey.patch_all()` approach transparently replaces stdlib threading, socket, and select modules, so existing code (locks, threads, urllib) works without modification. This is critical for SSE streaming where many connections are held open simultaneously.

### Why Background Daemon Threads

Non-blocking startup is essential on Render, which has a port-binding timeout. Every long-running initialization (vector indexing, enrichment, pre-compute) runs in a daemon thread. The server binds to `$PORT` immediately and serves requests while background initialization continues.

### Why Per-Table Circuit Breakers (Enrichment)

The original design had a single circuit breaker for all Supabase operations. When one table had an auth issue, all 13 enrichment sources were blocked for 1 hour. Per-table circuit breakers with 5-minute cooldowns limit blast radius to the affected table only.

### Why Health Always Returns 200

Render's health check mechanism interprets non-200 responses as service failure and prevents deploys. Encoding actual health status in the response body (not the status code) allows Render to consider the service healthy while still exposing degraded subsystem status to monitoring dashboards.

### Why Mandatory Tool Calling

Early versions of Nova would sometimes generate answers from the LLM's training data without consulting any data sources, producing plausible but outdated or incorrect responses. Mandatory tool calling (minimum 3 tools for complex queries) ensures every response is grounded in real-time data. Responses without tool calls are rejected and retried.

---

*This document reflects the system state as of commit `b3042d8` (Nova, S35) and `69a143b` (CG, S35). For session-by-session change history, see the memory files in `.claude/projects/` session progress files.*
