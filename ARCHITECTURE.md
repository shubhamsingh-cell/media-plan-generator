# Media Plan Generator — Full System Architecture

Last updated: 2026-03-08

## System Overview

The Media Plan Generator is an AI-powered recruitment advertising platform that generates comprehensive media plans by combining:
- **25+ live external API integrations** (labor market, salary, demographics, ad platforms)
- **Joveo's proprietary supply repository** (10,238+ publishers, 40+ countries, 200+ niche boards)
- **Deep research knowledge base** (~1MB structured JSON from 100+ industry sources)
- **AI-powered chatbot (Nova)** with 18 specialized data tools
- **Budget allocation engine** with industry-specific optimization
- **PowerPoint/Excel generator** with data-driven visualizations

```
                  ┌─────────────────────────────────────────────────┐
                  │              USER INTERFACES                     │
                  │  Web UI  |  Nova Chat  |  Slack Bot  |  API      │
                  └─────┬──────────┬──────────┬───────────┬─────────┘
                        │          │          │           │
                  ┌─────v──────────v──────────v───────────v─────────┐
                  │              app.py (HTTP Server)                │
                  │  /api/generate  |  /api/chat  |  /api/slack     │
                  └─────┬──────────────────┬────────────────────────┘
                        │                  │
          ┌─────────────v───┐    ┌─────────v──────────┐
          │ Media Plan      │    │ Nova Chatbot        │
          │ Generation      │    │ (nova.py)           │
          │ Pipeline        │    │ 18 data tools       │
          └──┬──┬──┬──┬─────┘    └──────┬──────────────┘
             │  │  │  │                 │
    ┌────────┘  │  │  └────────┐        │
    v           v  v           v        v
┌────────┐ ┌────────┐ ┌────────┐ ┌──────────────────────┐
│research│ │budget_ │ │ppt_    │ │ data_orchestrator.py  │
│.py     │ │engine  │ │generat.│ │ (UNIFIED DATA LAYER)  │
│        │ │.py     │ │.py     │ │ Cascades: research -> │
└───┬────┘ └───┬────┘ └───┬────┘ │ API -> KB -> fallback │
    │          │          │      └──────────┬───────────┘
    └──────────┴──────────┴──────┬──────────┘
                                 │
              ┌──────────────────v──────────────────┐
              │         DATA LAYER                   │
              │  ┌──────────┐  ┌──────────────────┐  │
              │  │data_     │  │ standardizer.py   │  │
              │  │synthesiz.│  │ (canonical maps)  │  │
              │  └──────────┘  └──────────────────┘  │
              │  ┌─────────────┐  ┌───────────────┐  │
              │  │ 25 External │  │ 14 JSON Data  │  │
              │  │ APIs        │  │ Files (~1MB)  │  │
              │  │ (api_enrich)│  │               │  │
              │  └──────┬──────┘  └───────────────┘  │
              │         │                            │
              │  ┌──────v──────┐                     │
              │  │ api_cache/  │                     │
              │  │ (81 files)  │                     │
              │  │ 24hr TTL    │                     │
              │  └─────────────┘                     │
              └──────────────────────────────────────┘
```

---

## 1. External API Integrations (25 APIs)

**File**: `api_enrichment.py` (~1,200 lines)
**Caching**: Two-tier (in-memory + disk at `data/api_cache/`), 24-hour TTL
**Concurrency**: ThreadPoolExecutor with max 15 workers
**Circuit breaker**: 3 consecutive failures triggers 5-minute backoff
**Timeout**: 8 seconds per API call

### Labor Market & Salary APIs

| # | API | Function | Data Provided | Auth |
|---|-----|----------|--------------|------|
| 1 | BLS OES (Bureau of Labor Statistics) | `fetch_salary_data()` | Salary benchmarks by SOC code and location | Free |
| 2 | BLS QCEW | `fetch_industry_employment()` | Industry employment & wage statistics | Free |
| 3 | O*NET Web Services | `fetch_onet_occupation_data()` | Occupation skills, knowledge, outlook | Free credentials |
| 4 | CareerOneStop (DOL) | `fetch_careeronestop_data()` | DOL salary, outlook, certifications | API key |
| 5 | DataUSA (2 endpoints) | `fetch_datausa_occupation_stats()` / `fetch_datausa_location_data()` | US occupation wages, state demographics | Free |

### Demographics & Economics APIs

| # | API | Function | Data Provided | Auth |
|---|-----|----------|--------------|------|
| 6 | US Census ACS | `fetch_location_demographics()` | Population, income, education by location | Free |
| 7 | World Bank Open Data | `fetch_global_indicators()` | Global economic indicators by country | Free |
| 8 | FRED (Federal Reserve) | `fetch_fred_indicators()` | US unemployment, inflation, labor force | API key (free) |
| 9 | IMF DataMapper | `fetch_imf_indicators()` | International GDP, inflation, unemployment | Free |
| 10 | REST Countries v3.1 | `fetch_country_data()` | Country population, currency, languages | Free |
| 11 | GeoNames | `fetch_geonames_data()` | Geographic data, coordinates, timezone | Free username |
| 12 | Teleport API | `fetch_teleport_city_data()` | Quality of life scores, cost of living | Free |

### Company Intelligence APIs

| # | API | Function | Data Provided | Auth |
|---|-----|----------|--------------|------|
| 13 | Wikipedia REST | `fetch_company_info()` | Company descriptions and background | Free |
| 14 | Clearbit Logo | `fetch_company_logo()` | Company logos and branding assets | Free tier |
| 15 | Clearbit Autocomplete | `fetch_company_metadata()` | Company metadata, domain lookup | Free tier |
| 16 | Google Favicons | (fallback in `fetch_company_logo()`) | Favicon fallback for logos | Free |
| 17 | SEC EDGAR | `fetch_sec_company_data()` | Public company ticker/CIK/filing data | Free |

### Job Market APIs

| # | API | Function | Data Provided | Auth |
|---|-----|----------|--------------|------|
| 18 | Adzuna | `fetch_job_market()` | Job postings and salary data | API key |
| 19 | Jooble | `fetch_jooble_data()` | International job market (69 countries) | API key |
| 20 | Google Trends | `fetch_search_trends()` | Search interest and demand signals | pytrends |
| 21 | Currency Rates | `fetch_currency_rates()` | Live exchange rates (30+ currencies) | Free |

### Advertising Platform APIs

| # | API | Function | Data Provided | Auth |
|---|-----|----------|--------------|------|
| 22 | Google Ads | `fetch_google_ads_data()` | Keyword volumes, CPC/CPM benchmarks | OAuth2 |
| 23 | Meta Marketing | `fetch_meta_ads_data()` | Facebook/Instagram audience sizing, CPC | Token |
| 24 | Microsoft/Bing Ads | `fetch_bing_ads_data()` | Search volumes, CPC estimates | OAuth2 |
| 25 | TikTok Marketing | `fetch_tiktok_ads_data()` | Audience estimation, CPC/CPM | Token |
| 26 | LinkedIn Marketing | `fetch_linkedin_ads_data()` | Professional audience sizing, CPC | Token |

**Failover**: All ad platform APIs have hardcoded benchmark fallbacks when keys are unavailable.

### How APIs Flow Into the System

```
User submits: Company, Roles, Locations, Budget
                    │
         ┌──────────v──────────┐
         │  api_enrichment.py  │
         │  ThreadPoolExecutor │
         │  (15 workers)       │
         └──────────┬──────────┘
                    │  Parallel calls to:
    ┌───────┬───────┼───────┬───────┬───────┐
    v       v       v       v       v       v
  BLS    Census   FRED   Google  Company  O*NET
  Salary  Demo    Econ   Trends  Info     Skills
    │       │       │       │       │       │
    └───────┴───────┴───────┴───────┴───────┘
                    │
         ┌──────────v──────────┐
         │  data_synthesizer   │
         │  Source weights     │
         │  (0.3-1.0 scale)   │
         │  Weighted median    │
         │  Cross-validation   │
         └──────────┬──────────┘
                    │
    ┌───────────────┼───────────────┐
    v               v               v
  budget_engine   research.py    ppt_generator
  (allocation)    (benchmarks)   (output)
```

---

## 2. Joveo Supply Repository

### A. Publisher Network — `data/joveo_publishers.json` (94KB)
- **1,238 active publishers** across 15+ categories
- Categories: AI tools, Classifieds, Community Hiring, DEI (50+), DSP, Data Partners, Free boards (40+), Global boards, Healthcare, Niche industry, Regional boards
- Fields per publisher: name, billing model (CPC/CPA/CPH/slot), category, tier (1/2/Niche), verification notes, last verified date
- Used by: Nova tools (`query_publishers`), budget engine, PPT generator

### B. Global Supply — `data/joveo_global_supply.json` (105KB)
- **40+ countries** with full board inventories
- Per-country data: job boards (name, billing model, category, tier), verification notes with 2026 status, monthly spend estimates, key metros, top employers
- US alone has 50+ Tier 1-2 boards (Indeed, LinkedIn, ZipRecruiter, etc.)
- Used by: Nova tools (`query_global_supply`, `query_location_profile`), research.py

### C. Channel Database — `data/channels_db.json` (57KB)
- **200+ boards** organized by reach type
- Regional/local: 32 boards | Global: 35+ boards | Niche by industry: 22 categories
- Industries covered: Healthcare, Tech, Blue-collar, Maritime, Military, Legal, Finance, etc.
- Non-traditional channels: Events, referral programs, employer branding
- Used by: Nova tools (`query_channels`), budget engine

---

## 3. Deep Research Knowledge Base

All sourced from 100+ industry reports, surveys, and platforms. Loaded at startup into memory.

### A. Recruitment Benchmarks — `data/recruitment_benchmarks_deep.json` (62KB)
- **22 industries** with CPA, CPC, CPH, apply rates, time-to-fill, funnel data, YoY trends
- **21 data sources**: Appcast, SHRM, iCIMS, Indeed, LinkedIn, Glassdoor, etc.
- Data period: 2024-2026
- Example: Healthcare — CPA $35-$58, CPC $0.50-$1.44, CPH $9K-$12K

### B. Industry Knowledge — `data/recruitment_industry_knowledge.json` (99KB)
- **42 data sources** integrated
- Per-platform metrics: Indeed ($0.25-$1.50 CPC), LinkedIn ($1.50-$4.50 CPC), ZipRecruiter ($299/mo), Google Ads ($5.26 avg CPC), Meta ($0.86 CPC)
- Appcast dataset: 379M job ad clicks, 30M+ applies, 1,300+ employers

### C. Platform Intelligence — `data/platform_intelligence_deep.json` (82KB)
- **91-platform database** with per-platform: CPC, CPA, apply rates, visitors, mobile %, demographics, DEI/AI features, pros/cons, industry benchmarks

### D. Regional Hiring Intelligence — `data/regional_hiring_intelligence.json` (141KB)
- **16 sources** (BLS, LinkedIn, Indeed, Appcast, SHRM, etc.)
- **25+ US metros**: population, top 10 boards (with Joveo availability), dominant industries with share % and growth, average salaries (entry/mid/senior), talent dynamics, best channels with CPC, CPA benchmarks, hiring regulations, cultural norms
- International: UK, Germany, France, Australia, India, etc.

### E. Workforce Trends — `data/workforce_trends_intelligence.json` (71KB)
- **44 sources** (BLS, LinkedIn, Deloitte, McKinsey, etc.)
- Gen Z insights: 27% of workforce by 2026, 22.1% use TikTok for job search, 1.1yr avg tenure
- Workplace: 72% left job for lack of flexibility, 61% would leave for better mental health benefits
- DEI: 48% cite DEI initiatives as application driver

### F. Supply Ecosystem — `data/supply_ecosystem_intelligence.json` (88KB)
- **24 sources** on programmatic recruitment mechanics
- 8-step workflow: job ingestion -> classification -> publisher selection -> bid optimization -> distribution -> monitoring -> optimization -> pacing
- Bidding models: CPC, CPA, CPQA, CPM, slot-based, RTB
- Joveo differentiation: CPQA (Cost Per Qualified Application)

### G. Strategy Intelligence — `data/recruitment_strategy_intelligence.json` (70KB)
- Strategic hiring insights from 20+ sources
- Channel effectiveness analysis, market positioning

### H. Industry White Papers — `data/industry_white_papers.json` (74KB)
- **47 industry reports** from Appcast, Radancy, Recruitics, PandoLogic, Joveo
- Report metadata: keys, URLs, data scopes, release dates

### I. research.py Embedded Data (253KB)
- **40+ countries**: COLI, median salaries, unemployment %, top boards, top industries
- **100+ US metro areas**: population, job market details, talent dynamics
- **265+ SOC code mappings** (job title -> standard occupation code)
- **30+ NAICS code mappings** (industry -> standard industry code)
- **22 industry benchmark sets** with full CPA/CPH/apply rate data

---

## 4. Data Synthesizer — How Sources Combine

**File**: `data_synthesizer.py` (113KB)

The synthesizer fuses data from all sources using weighted intelligence:

| Function | Sources Combined | Method |
|----------|-----------------|--------|
| `fuse_salary_intelligence()` | BLS + O*NET + DataUSA + CareerOneStop + benchmarks | Source reliability weights (0.3-1.0), weighted median |
| `fuse_job_market_demand()` | Adzuna + Jooble + Google Trends + Indeed data | Cross-reference validation, confidence scoring |
| `fuse_geographic_context()` | Census + Teleport + GeoNames + regional intel | Hierarchical fallback (API -> research -> defaults) |
| `synthesize_all()` | All above | Unified fusion with conflict resolution |

**Source reliability weights**:
- BLS/Census/FRED: 1.0 (government data, highest trust)
- LinkedIn/Indeed: 0.8 (large-scale platform data)
- O*NET/DataUSA: 0.7 (curated occupation data)
- Adzuna/Jooble: 0.5 (aggregator estimates)
- Google Trends: 0.3 (directional signals only)

---

## 5. Standardizer — Single Source of Truth

**File**: `standardizer.py` (48KB)

Canonical taxonomy consumed by all modules:

| Taxonomy | Count | Examples |
|----------|-------|---------|
| Industries | 17 canonical | Healthcare, Tech, Finance, Retail, Manufacturing |
| Locations | 50+ mappings | Countries, US states, regions |
| US Regions | 5 | Northeast, Southeast, Midwest, West, South |
| Platforms | 30+ aliases | "LI" -> "LinkedIn", "FB" -> "Facebook" |
| Metrics | Normalized | CPC, CPA, CPH, CPM, CPQA |
| SOC codes | 265+ | Job title -> Standard Occupation Code |
| NAICS codes | 30+ | Industry -> Standard Industry Code |

---

## 5b. Data Orchestrator — Unified Data Access Layer

**File**: `data_orchestrator.py` (NEW)

Single entry point that cascades through all data sources in cost/speed order:

```
Query -> research.py (free, instant) -> API call (cached 24h) -> KB fallback
```

| Function | Cascades Through | Used By |
|----------|-----------------|---------|
| `enrich_salary(role, location)` | research.py COLI + BLS API + curated ranges | Nova salary tool |
| `enrich_location(location)` | research.py (40+ countries, 100+ metros) + Census/World Bank | Nova location tool |
| `enrich_market_demand(role, loc, ind)` | research.py labor intel + Adzuna/Jooble API | Nova market tool |
| `enrich_competitive(company, ind)` | research.py + SEC/Wikipedia API | Nova competitive queries |
| `enrich_budget(budget, roles, locs)` | Budget engine with cached enrichment data | Nova budget tool |
| `enrich_platform_audiences(industry)` | research.py platform audience data | Nova ad platform tool |
| `normalize(industry, location, role)` | standardizer.py canonical taxonomy | All tools, input processing |

**Thread-safe**: Lazy imports with double-checked locking, shared cache with locks.
**Cache**: 24-hour TTL, max 500 entries, auto-eviction.
**Fault-tolerant**: Every function has try/except, always returns usable data.

**Data flow matrix (after orchestrator integration)**:

```
                    | Excel/PPT | Nova Chat | Slack Bot | PPT      |
--------------------|-----------|-----------|-----------|----------|
A. JSON Files       |    YES    |    YES    |    YES    |    YES   |
B. 25 APIs          |    YES    |    YES*   |    YES*   |  PARTIAL |
C. research.py      |    YES    |    YES*   |    YES*   |    YES   |
D. data_synthesizer |    YES    |    NO**   |    NO**   |    YES   |
E. budget_engine    |    YES    |    YES*   |    YES*   |    YES   |
F. standardizer     |    YES    |    YES*   |    YES*   |    NO    |
G. Claude API       |    NO     |    YES    |    YES    |    NO    |

* = via data_orchestrator.py
** = too heavy for real-time chatbot; synthesizer is for batch generation
```

---

## 6. Nova Chatbot — 18 Data Tools

**File**: `nova.py` (~3,500 lines)

Nova gives Claude access to all data sources through 18 specialized tools:

### Joveo Data Tools
| Tool | Data Source | What It Returns |
|------|-----------|-----------------|
| `query_publishers` | joveo_publishers.json | Publisher search across 10,238+ supply partners |
| `query_global_supply` | global_supply.json | Country-level board inventory, spend data, DEI boards |
| `query_channels` | channels_db.json | Channel recommendations by reach type and industry |
| `query_location_profile` | global_supply + research | Location spend, metros, publisher availability |

### Benchmark & Intelligence Tools
| Tool | Data Source | What It Returns |
|------|-----------|-----------------|
| `query_knowledge_base` | All research JSONs | Core recruitment KB search (benchmarks, trends, platforms) |
| `query_recruitment_benchmarks` | benchmarks_deep.json | Industry-specific CPA/CPC/CPH for 22 industries |
| `query_platform_deep` | platform_intelligence_deep.json | 91-platform database with full metrics |
| `query_regional_market` | regional_hiring_intelligence.json | US regional + global market hiring intel (16 sources) |
| `query_workforce_trends` | workforce_trends_intelligence.json | Gen-Z, remote work, DEI trends (44 sources) |
| `query_supply_ecosystem` | supply_ecosystem_intelligence.json | Programmatic mechanics, bidding models (24 sources) |
| `query_white_papers` | industry_white_papers.json | 47 industry report search |
| `query_employer_branding` | strategy_intelligence.json | Employer branding intel (34 sources) |
| `query_linkedin_guidewire` | linkedin_guidewire_data.json | LinkedIn Hiring Value Review case study |

### Computation Tools
| Tool | Data Source | What It Returns |
|------|-----------|-----------------|
| `query_salary_data` | research.py embedded + BLS | Salary ranges by role/location with tier classification |
| `query_market_demand` | research.py + API data | Applicant ratios, source-of-hire, hiring strength |
| `query_budget_projection` | budget_engine.py | Budget allocation across 6 channels with projections |
| `query_ad_platform` | platform_intelligence + research | Platform recommendations by role type |
| `suggest_smart_defaults` | All sources | Auto-detect budget/channel split from roles/locations |

### Decision Flow

```
User asks: "What's the CPC for nursing roles in Texas?"
                    │
         1. Check learned answers (Jaccard >= 0.35)
                    │ miss
         2. Check response cache (normalized key)
                    │ miss
         3. Claude API selects tools:
              ├── query_recruitment_benchmarks(industry="healthcare")
              ├── query_regional_market(region="south", state="TX")
              └── query_salary_data(role="nursing", location="Texas")
                    │
         4. Claude synthesizes tool results into response
                    │
         5. Cache response (if confidence >= 0.6)
```

---

## 7. Budget Engine

**File**: `budget_engine.py` (55KB)

### Inputs
- Role tiers: 6 levels (Executive 3.5x multiplier down to Gig 0.5x)
- Industry CPH ranges: 16 industries ($2K-$18K)
- Channel base benchmarks: CPC, apply rate, hire rate per channel
- Hire rates by tier: Hourly 6%, Executive 0.8%

### Channels Modeled (8)
Programmatic DSP, Global Job Boards, Niche Boards, Social Media, Regional Boards, Employer Branding, APAC Regional, EMEA Regional

### Output
Per-channel: dollar allocation, projected clicks, projected applications, projected hires

---

## 8. PPT/Excel Generator

**File**: `ppt_generator.py` (165KB)

### Data Sources Feeding Output
- Industry benchmarks (22 industries with CPA/CPC/CPH)
- Channel allocations from budget engine (8 channels with %)
- API enrichment data (salary, demographics, company info)
- Research data (market analysis, competitive landscape)
- Hiring complications per industry

### Output Structure (7 slides)
1. Executive Summary | 2. Market Analysis | 3. Channel Allocation (pie chart) | 4. Budget Projections | 5. Regional Performance | 6. Competitive Benchmarks | 7. Call-to-Action

---

## 9. Key Files Summary

| File | Size | Purpose |
|------|------|---------|
| `app.py` | ~8,300 lines | HTTP server: all routes, rate limiting, CORS, admin auth |
| `nova.py` | ~3,500 lines | AI chatbot: 18 tools, learned answers, caching, Claude API |
| `nova_slack.py` | ~900 lines | Slack bot: event handling, token rotation, mrkdwn formatting |
| `api_enrichment.py` | ~1,200 lines | 25 external API integrations with two-tier caching |
| `research.py` | ~4,000 lines | Embedded knowledge base: 40+ countries, 100+ metros, 265+ SOC codes |
| `data_synthesizer.py` | ~2,000 lines | Multi-source data fusion with reliability weighting |
| `budget_engine.py` | ~800 lines | Budget allocation with industry/tier optimization |
| `ppt_generator.py` | ~3,000 lines | PowerPoint/Excel output with data visualizations |
| `standardizer.py` | ~700 lines | Canonical taxonomy: industries, locations, SOC/NAICS codes |
| `data_orchestrator.py` | ~500 lines | Unified data access: cascades research->API->KB for all consumers |
| `joveo_iq.py` | ~2,000 lines | Joveo IQ module |
| `monitoring.py` | ~300 lines | Health checks, request metrics |

---

## 10. Data Files Summary

| File | Size | Records | Sources |
|------|------|---------|---------|
| `joveo_publishers.json` | 94KB | 1,238 publishers | Joveo supply network |
| `joveo_global_supply.json` | 105KB | 40+ countries | Joveo global operations |
| `channels_db.json` | 57KB | 200+ boards | Joveo channel database |
| `recruitment_benchmarks_deep.json` | 62KB | 22 industries | 21 industry sources |
| `recruitment_industry_knowledge.json` | 99KB | 42 sources | Appcast, SHRM, iCIMS, etc. |
| `platform_intelligence_deep.json` | 82KB | 91 platforms | Platform-specific research |
| `regional_hiring_intelligence.json` | 141KB | 25+ metros | 16 regional sources |
| `workforce_trends_intelligence.json` | 71KB | 44 sources | BLS, LinkedIn, Deloitte, McKinsey |
| `supply_ecosystem_intelligence.json` | 88KB | 24 sources | Programmatic ecosystem data |
| `recruitment_strategy_intelligence.json` | 70KB | 20+ sources | Strategic hiring insights |
| `industry_white_papers.json` | 74KB | 47 reports | Appcast, Radancy, Recruitics, etc. |
| `linkedin_guidewire_data.json` | 27KB | 1 case study | LinkedIn Hiring Value Review |
| `nova_learned_answers.json` | 6KB | 12 Q&A pairs | Pre-computed chatbot answers |
| `nova_response_cache.json` | Variable | ~200 entries | 7-day TTL response cache |

**Total knowledge base**: ~1.0MB structured JSON + 253KB embedded in research.py

---

## 11. API Endpoints

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/api/generate` | POST | Rate limited | Media plan generation (Excel/PPT) |
| `/api/chat` | POST | Rate limited (per-IP + global) | Nova chatbot |
| `/api/nova/chat` | POST | Rate limited | Alias for /api/chat |
| `/api/nova/metrics` | GET | `Authorization: Bearer` or `?key=` | Nova performance metrics |
| `/api/health` | GET | None | Liveness probe |
| `/api/health/ready` | GET | None | Readiness probe |
| `/api/metrics` | GET | Admin | Server-wide metrics |
| `/api/slack/events` | POST | Slack signature | Slack event webhook |

---

## 12. Metrics & Observability

Nova tracks per-request metrics via `_NovaMetrics` singleton:

- **Response mode counters**: learned_answers, cache_hits, claude_api, rule_based
- **Token usage**: input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens
- **Latency**: Per-request ms (rolling window of last 200)
- **Estimated cost**: Calculated from Haiku 4.5 pricing ($1/$5 per M tokens)
- **Cache hit rate**: (learned + cache) / total * 100

Access via `GET /api/nova/metrics` (requires `Authorization: Bearer <key>` header or `?key=<key>` query param).

---

## 13. Token Budget per Request

| Component | Tokens | Notes |
|-----------|--------|-------|
| System prompt | ~1,300 | Compressed, cached after first request |
| Tool definitions | ~2,500 | 18 tools, compressed, cached |
| History (6 turns) | ~800-1,600 | Depends on conversation length |
| User message | ~20-100 | |
| **Total input** | **~4,600-5,500** | First request; ~600-1,700 after prompt caching |
| **Output** | **1,024-4,096** | Adaptive based on query complexity |

---

## 14. Testing

```bash
# Against local server
./tests/test_nova_chat.sh

# Against production
./tests/test_nova_chat.sh https://media-plan-generator.onrender.com

# With metrics endpoint testing
ADMIN_API_KEY=your_key ./tests/test_nova_chat.sh https://media-plan-generator.onrender.com
```

Tests cover: response structure, learned answers, cache behavior, ask-before-answering logic, complex queries, empty input handling, and metrics endpoint.
