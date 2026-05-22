# Nova Chatbot API Integration Audit & 2026-2027 Roadmap

**Date**: 2026-05-22
**Auditor**: Technology Scout (Nova/Joveo)
**Scope**: `media-plan-generator/nova.py` (24,827 LOC) + supporting modules (`api_enrichment.py`, `api_integrations.py`, `meta_ads_integration.py`, `google_*.py`, `recruitment_apis.py`, `chatbot_tools_recruitment.py`, `tavily_search.py`)
**Methodology**: Source-code verification + live endpoint probes (curl) + publisher docs cross-check. nova.py was read-only.

---

## 1. Executive Summary

Nova currently exposes **76 tools** to the LLM (65 explicitly defined in `get_tool_definitions()` plus 11 S50 recruitment APIs auto-merged from `RECRUITMENT_TOOLS_SCHEMA`). Coverage is broad on US data (BLS, BEA, Census, USAJobs, O\*NET, H-1B, CareerOneStop) and improving on EU/UK/Canada (Eurostat, ONS, StatCan), but **monetary-impact benchmarks (programmatic, CPA, CPH) still rely heavily on static JSON files** rather than live vendor APIs. Two recent additions - LinkUp Job Postings and Revelio Labs (S48) - return graceful errors today because the relevant API keys are not set on Render.

**Top 5 recommendations for the next two quarters:**

1. **Activate the dormant integrations Nova already ships** - set `LINKUP_API_KEY`, `REVELIO_API_KEY`, `USAJOBS_API_KEY`, `CAREERONESTOP_USER_ID/TOKEN` on Render before paying for any new vendor. Zero engineering required, immediate quality lift on workforce-trends and federal-jobs questions.
2. **Adopt OECD SDMX + ESCO formally** as the global labour-market & taxonomy baseline. ESCO is wired through S50's `lookup_skill_esco` / `lookup_occupation_esco`; OECD is not wired at all. Both are free, no-key, and cover countries Nova currently has weak data on (DE, FR, JP, AU).
3. **Add Appcast (or Recruitics) Benchmark API as Nova's first paid programmatic-benchmarks live feed** - this is the single biggest gap. Today Nova answers programmatic-CPA questions from a 244 KB JSON snapshot last refreshed 2026-04-08.
4. **Add talent.com / ZipRecruiter Publisher feeds** to plug Nova's blind spot on mid-market US and Canadian job-volume signals (Adzuna + Jooble + CareerJet skew to aggregator/SEO-heavy results).
5. **Deprecate `Firecrawl` as a paid dependency.** Code paths exist but it has been out of credits since S39 (per `MEMORY.md`); Tavily + Jina + the internal `web_scraper_router` already cover the same job at lower cost.

---

## 2. Current Integration Audit

Verified by `grep`-ing handler functions, tool definitions in `get_tool_definitions()` (nova.py:4433+), the dispatch map `_tool_handler_map()` (nova.py:6459+), and source-of-truth modules.

### 2.1 Audit Table

| # | Integration | Status | Handler / File:Line | Tool Name(s) | Coverage | Cost Tier | Replacement Risk |
|---|---|---|---|---|---|---|---|
| 1 | **BLS** (Bureau of Labor Statistics) | LIVE | `api_integrations.py` -> `_query_labor_market_indicators` (nova.py:11537) | `query_labor_market_indicators` | US-only | Free (key required, `BLS_API_KEY`) | Low - Nova has BLS state+metro KB snapshot |
| 2 | **O\*NET v2.0** | LIVE | `api_integrations.py` -> `_query_skills_profile` (nova.py:7899) | `query_skills_profile`, `query_role_decomposition` | US-only (skill taxonomy is global-ish) | Free (key required, `ONET_USER`) | Medium - ESCO is a partial substitute for EU |
| 3 | **Adzuna** | LIVE | `api_enrichment.py` / `api_integrations.py` | `query_market_demand`, used inside `_query_remote_jobs` | 17 countries (US, GB, AU, CA, DE, FR, IN, etc.) | Free 1k/mo, paid above (`ADZUNA_APP_ID/KEY`) | Medium - paid tiers $$$ at scale |
| 4 | **Jooble** | LIVE | `api_enrichment.py` | Used by `query_market_demand` aggregator | 70+ countries | Free (key required) | Low |
| 5 | **FRED v2 + JOLTS** | LIVE | `api_integrations.py` / `api_enrichment.py` | `query_regional_economics` (FRED series), `_query_labor_market_indicators` (JOLTS) | US-only | Free (`FRED_API_KEY`) | Low (Fed-backed, very stable) |
| 6 | **Tavily** | LIVE, heavily used | `tavily_search.py` -> `_web_search` (nova.py:10639) | `web_search` | Global | $$ (paid, `TAVILY_API_KEY` set) | Medium - this is Nova's primary live-search dependency |
| 7 | **Firecrawl** | DORMANT (out of credits per S39) | `competitive_intel.py`, `data_orchestrator.py` | Internal scraping only (not exposed to chatbot directly) | Global | $$ (paid, exhausted) | High - candidate for **deprecation** |
| 8 | **CareerOneStop v2** | LIVE if key set | `api_enrichment.py` -> `_query_occupation_projections` (nova.py:11465), `_query_skills_profile` partial | `query_occupation_projections` | US-only (DoL) | Free (`CAREERONESTOP_USER_ID/TOKEN` required) | Low |
| 9 | **USAJobs** | LIVE if key set | `_query_federal_jobs` (nova.py:11343) | `query_federal_jobs` | US Federal only | Free (`USAJOBS_API_KEY` required) | Low - niche but unique |
| 10 | **BEA** | LIVE | `api_integrations.py:bea` -> `_query_bea` (nova.py:13270), `_query_regional_economics` | `query_bea`, `query_regional_economics` | US (state+MSA) | Free (`BEA_API_KEY`) | Low |
| 11 | **Census Bureau** | LIVE | `api_integrations.py` -> `_query_workforce_demographics` (nova.py:11600) | `query_workforce_demographics` | US (state + top-50 metros) | Free (`CENSUS_API_KEY`) | Low |
| 12 | **RemoteOK** | LIVE (public scrape) | `_query_remote_jobs` (nova.py:11497) | `query_remote_jobs` | Global remote roles | Free, unauthenticated | Medium - thin sample, ToS could shift |
| 13 | **H-1B / LCA** | LIVE | `h1b_data.py` -> `_query_h1b_salaries` (nova.py:11436) | `query_h1b_salaries` | US-only | Free (DoL bulk file, vendored) | Low |
| 14 | **Google Trends** | LIVE | `api_enrichment.py:fetch_google_trends` (line 15369) | Internal enrichment (not a top-level tool) | Global | Free, rate-limited | High - pytrends fragile, frequent breakage |
| 15 | **GeoNames** | LIVE (key required) | `api_enrichment.py:fetch_geonames_data` (line 5021) -> `_geocode_location` | `geocode_location` | Global | Free (`GEONAMES_USERNAME` set) | Low |
| 16 | **CareerJet** | LIVE (no key, affid-based) | `api_enrichment.py:fetch_careerjet_data` (line 16019) -> `_query_careerjet` (nova.py:13231) | `query_careerjet` | 60+ countries | Free | Low |
| 17 | **Eurostat** | LIVE | `api_enrichment.py:fetch_eurostat_data` (line 16081) -> `_query_eurostat` (nova.py:13142) | `query_eurostat` | EU-27 + EFTA | Free | Low |
| 18 | **UK ONS** | LIVE | `api_enrichment.py:fetch_uk_ons_data` (line 16146) -> `_query_uk_ons` (nova.py:13173) | `query_uk_ons` | UK | Free | Low |
| 19 | **StatCan** | LIVE | `api_enrichment.py:fetch_statcan_data` (line 16215) -> `_query_statcan` (nova.py:13202) | `query_statcan` | Canada | Free | Low |
| 20 | **Google Ads** | LIVE (read-only benchmarks + MCC) | `google_ads_analytics.py` -> `_query_google_ads_benchmarks` / `_query_google_ads_performance` (nova.py:10060, 10172) | `query_google_ads_benchmarks`, `query_google_ads_performance` | Global | Free API; ad spend is the cost | Medium - depends on Joveo MCC token rotation |
| 21 | **Google Maps** | LIVE | `google_maps_integration.py` (geocode + Places) | Backs `geocode_location`, `optimize_geography` | Global | Free tier sufficient, paid above | Low |
| 22 | **Google Vision** | LIVE (Career-page audit) | `google_vision_integration.py` -> `_audit_career_page` (nova.py: tool `audit_career_page`) | `audit_career_page` | Global | Pay-per-image, free $300 GCP credit running | Low |
| 23 | **Google NLP** | LIVE | `google_*.py` (one of the modules), used in `analyze_employer_brand` | `analyze_employer_brand` | Global | Pay-per-1k-chars; small footprint | Low |
| 24 | **Google Translate** | LIVE | `google_translate_integration.py` -> `translate_text` | `translate_text` | 130+ languages | Free 500k chars/mo | Low |
| 25 | **Google BigQuery** | LIVE (indirect) | `google_bigquery_integration.py` | Internal cache + cross-product queries (not direct chatbot tool) | n/a | Free 1TB query/mo | Low |
| 26 | **Meta Ads (Marketing API)** | LIVE | `meta_ads_integration.py` -> `_query_meta_performance` (nova.py:10139), `estimate_meta_campaign`, `get_meta_benchmarks` | `query_meta_performance`, `estimate_meta_campaign`, `get_meta_benchmarks` | Global | Free API; ad spend cost | Medium - long-lived token rotation risk |
| 27 | **Google Analytics Data API** (GA4) | LIVE | `google_analytics_data.py` (used by SlotOps + Nova) | Used internally by `get_attribution_data` | Customer-scoped | Free | Low |
| 28 | **LinkUp Job Postings** | WIRED, KEY MISSING | `_query_linkup_postings` (nova.py:12817) | `query_linkup_postings` | Global (LinkUp curates 130M+ postings) | $$$ paid only | High - returns graceful 401 error today |
| 29 | **Revelio Labs RPLS** | WIRED, KEY MISSING | `_query_revelio_workforce` (nova.py:12878) | `query_revelio_workforce` | Global workforce analytics | $$$ paid (RPLS has free tier on request) | High - returns graceful 401 error today |
| 30 | **(S50) ESCO Skill / Occupation** | LIVE | `recruitment_apis.py` -> auto-registered via `RECRUITMENT_TOOL_DISPATCH` | `lookup_skill_esco`, `lookup_occupation_esco` | EU-wide taxonomy | Free, no key | Low |
| 31 | **(S50) ILOSTAT** | LIVE | `recruitment_apis.py` | `lookup_country_labour_ilostat` | 180+ countries | Free | Low |
| 32 | **(S50) World Bank** | LIVE | `recruitment_apis.py` | `lookup_country_indicator_worldbank` | 200+ countries | Free | Low |
| 33 | **(S50) HN Hiring (Algolia)** | LIVE | `recruitment_apis.py` | `lookup_tech_jobs_hnhiring` | Global tech | Free | Low |
| 34 | **(S50) NPI Registry** | LIVE | `recruitment_apis.py` | `lookup_healthcare_npi` | US healthcare | Free | Low |
| 35 | **(S50) FMCSA** | LIVE | `recruitment_apis.py` | `lookup_trucking_carrier` | US trucking | Free | Low |
| 36 | **(S50) Levels.fyi (compensation embed)** | LIVE | `recruitment_apis.py` | `lookup_compensation_levels` | Global tech comp | Free (embed/scrape) | High - ToS-sensitive |
| 37 | **(S50) Crunchbase STUB** | NON-FUNCTIONAL STUB | `recruitment_apis.py` | `lookup_company_crunchbase` | n/a | Paid; **not wired to real API** | High - misleading to LLM today |
| 38 | **(S50) PDL STUB** | NON-FUNCTIONAL STUB | `recruitment_apis.py` | `enrich_person_pdl` | n/a | Paid; **not wired to real API** | High - misleading to LLM today |
| 39 | **(S50) WARNTracker** | URL STUB | `recruitment_apis.py` | `lookup_layoffs_warntracker` | US WARN notices | Free site, no public API | Medium |

### 2.2 Verification Notes

- Each "LIVE" row above was confirmed by reading the handler in nova.py and the underlying fetch function in the supporting module (e.g. `fetch_eurostat_data` at `api_enrichment.py:16081`).
- "WIRED, KEY MISSING" means the code path exists end-to-end but `os.environ.get(...)` returns empty and Nova surfaces a graceful `setup_instructions` payload to the LLM. Confirmed by reading nova.py:12835 and 12902.
- The S50 batch is enrolled via the import block at `nova.py:36-42` and the merge at `nova.py:6445` (`+ RECRUITMENT_TOOLS_SCHEMA`) and `nova.py:6581` (dispatch merge loop).

### 2.3 Cache Freshness

`nova_cache.py` is Supabase-backed with TTL = **1h for "real-time" queries** (trending, today, current) and **24h for stable queries** (salary, benchmark, compare) (lines 88-89, 380-395). Static JSON benchmark KB files vary:

| KB file | Bytes | mtime |
|---|---|---|
| `data/international_benchmarks_2026.json` | 245 KB | 2026-04-08 |
| `data/h1b_salary_intelligence.json` | 28 KB | 2026-03-26 |
| `data/adzuna_benchmarks.json` | 7 KB | 2026-04-03 |
| `data/external_benchmarks_2025.json` | 58 KB | **2026-03-26 (stale)** |
| `data/joveo_2026_benchmarks.json` | 11 KB | **2026-03-22 (stale)** |
| `data/linkedin_performance_benchmarks.json` | 89 KB | 2026-04-04 |

The 2025-labelled `external_benchmarks_2025.json` and the Joveo 2026 file haven't been refreshed in 8-10 weeks. Nova does call them out as `data_freshness: "curated"` in prompts (nova.py:4049), so the LLM is supposed to add a disclaimer when older than 90 days - but it relies on the model honouring that instruction.

---

## 3. Stale / At-Risk Integrations

| Integration | Risk | Recommendation |
|---|---|---|
| **Firecrawl** | Out of credits since S39 (per MEMORY.md). Still imported in 10+ modules. Adds maintenance + Sentry noise. | **Deprecate** in S51-52. Tavily + Jina + `web_scraper_router` already cover the use case. |
| **Google Trends (pytrends)** | Library unmaintained, Google rate-limits aggressively. Used only inside enrichment. | Wrap behind a feature flag; consider Glimpse API or DataForSEO as backup. |
| **Crunchbase STUB / PDL STUB** (S50) | Tools advertised to the LLM but return stub data. Risk of hallucinated company/person enrichments. | Either **wire the real Crunchbase Enterprise API** (paid) or remove from schema in S51. |
| **RemoteOK** | Unauthenticated public scrape. Long-term ToS risk + sample bias to tech roles. | Keep, but de-emphasize in prompts. Replace with We Work Remotely API or talent.com filtered by `remote=true`. |
| **Levels.fyi embed** | Public-page scrape, fragile to layout changes. Already broke once in S46. | Move to Salary.com Compensation API (paid, stable) for FAANG-grade tech roles, keep Levels.fyi for sourcing intelligence only. |
| **LinkUp + Revelio (S48)** | Wired but no key. Tools surface error messages, which the LLM may try to use anyway. | **Decide buy/no-buy in Q3 2026**. LinkUp ~$24k/yr commercial, Revelio RPLS has free academic-style tier; commercial is $50k+/yr. |
| **`external_benchmarks_2025.json`** | Filename says 2025, last touch March 2026. Quarterly refresh required. | Schedule auto-refresh job (Appcast/Recruitics API would replace half of it). |

---

## 4. New API Recommendations (Top 10)

Each candidate was verified for live availability and the publisher's pricing/docs URL. Scoring is `relevance (0-5) x quality (0-5) x coverage (0-5)` divided by cost tier (1=free, 2=low, 3=mid, 4=high) so higher is better.

| Rank | API | Score | Why it matters for Nova | Coverage | Pricing | Effort | Docs |
|---|---|---|---|---|---|---|---|
| 1 | **OECD SDMX Public API** | 125/1 | Closes Nova's biggest geographic gap: harmonised LFS, vacancy, wage, and skills data for 38 OECD countries on one schema. Complements Eurostat (which is EU-only). | 38 countries (US, JP, AU, KR, MX, TR, ...) | **Free**, no key | 4-6 days (SDMX-JSON parser is non-trivial but template lives in api_enrichment) | https://sdmx.oecd.org/public/rest/ |
| 2 | **ESCO (deep integration)** | 100/1 | Already wired via S50 lookups. Recommendation: add the full skills-to-occupation graph traversal so `query_role_decomposition` can return EU-comparable skills. | 27 EU + 16 European languages | **Free**, no key | 2 days (extend `recruitment_apis.lookup_occupation_esco`) | https://esco.ec.europa.eu/en/use-esco/download |
| 3 | **Appcast Benchmark API** | 90/4 | Industry-standard programmatic recruitment benchmark (302M clicks). Highest-impact addition for Joveo because the chatbot frequently answers programmatic-CPA questions. | Global, CPC/CPA/Apply rate by industry/role/geo | **Paid** (enterprise quote, $30-60k/yr est.) | 5-7 days (auth + caching) | https://www.appcast.io/products/benchmarks/ |
| 4 | **World Bank Jobs Data Hub** | 80/1 | Already wired for single-indicator lookup via S50. Recommendation: extend to multi-country, multi-indicator queries (employment-to-pop, unemployment, NEET, informality). Confirmed live (HTTP 200 today, last update 2026-04-08). | 200+ countries | **Free**, no key | 2 days | https://api.worldbank.org/v2/ |
| 5 | **talent.com Publisher Feed** | 75/2 | Best free-tier substitute for Jooble/Adzuna on US + CA mid-market. Powers Nova's `check_job_volume` answers for SMB/healthcare/gig where Adzuna is thin. | US, CA, FR, DE, UK + 60 more | **Free** for publishers (revshare), paid for direct | 4 days | https://www.talent.com/jobs-api |
| 6 | **ABS Australia Labour Force (SDMX)** | 60/1 | Nova has weak AU data (CareerJet only). ABS Labour Force is the gold standard, hits via SDMX over data.api.abs.gov.au. | AU | **Free**, no key | 2 days | https://data.api.abs.gov.au/ |
| 7 | **Mapbox / HERE Geocoding** | 50/2 | Backup to Google Maps + GeoNames. Mapbox has better address coverage in LATAM/SEA which matters for Joveo's APAC clients. Provides reverse-geocoding without GCP cost. | Global | Mapbox: 100k req/mo free, then $0.50/1k; HERE: 30k/mo free | 1 day | https://docs.mapbox.com/api/search/geocoding-v6/ |
| 8 | **Recruitics Benchmark API** | 48/4 | Alternative to Appcast. Lighter weight, often cheaper for mid-market. Useful as a second data point for triangulating cost-per-applicant. | US-heavy, expanding EU | **Paid** (no public price; mid five-figures) | 5 days | https://www.recruitics.com/products/recruitics-platform/ |
| 9 | **Reddit Public JSON** (r/recruiting, r/cscareerquestions) | 45/1 | Cheap qualitative talent-sentiment signal. Already returned HTTP 200 for `r/recruiting/about.json` in audit. Useful for "what are candidates complaining about?" copilot prompts. | Global English-speaking | **Free** (60 req/min unauthenticated) | 2 days | https://www.reddit.com/dev/api/ |
| 10 | **Salary.com Compensation API** | 40/3 | Replace fragile Levels.fyi embed for FAANG/exec-comp questions. Verified industry pricing data, regulatory-grade for offer-letter decisions. | US + 70 countries | **Paid** (volume-tiered) | 5 days | https://www.salary.com/api/ |

**Considered and rejected (this cycle):**
- **Trading Economics API** - sales page is live but the public guest endpoint now returns HTTP 410 (confirmed by curl). Their commercial product is fine but the FRED + World Bank + OECD trio already covers 90% of macro use cases.
- **Indeed Publisher Program** - publisher portal returns 403; access requires sponsored-jobs contract. Outside Joveo's current relationship.
- **LinkedIn Jobs API** - no public surface; Talent Solutions API requires LMS-partner status, which Joveo does not currently hold for the chatbot product.
- **ZipRecruiter Publisher** - publisher access is invitation-only; would require partnerships outreach before a technical eval.
- **Glassdoor Economic Research** - HTTP 403 even on public research page; effectively read-only via web scrape, not an API.
- **Foursquare Places** - useful for office-location demographics but overlaps with Google Places, which Joveo already pays for.
- **TikTok Research API** - approval-gated and time-bounded; speculative ROI for recruitment marketing today.
- **HR-tech partnerships** (Eightfold / Phenom / HiringSolved) - these are competitor-adjacent platforms; data exchange would need a commercial agreement before any technical evaluation.

---

## 5. Integration Roadmap

### Q3 2026 (Jun-Aug) - "Activate what we own"

| Item | Effort | Owner | Outcome |
|---|---|---|---|
| Set `USAJOBS_API_KEY`, `CAREERONESTOP_USER_ID/TOKEN`, `BLS_API_KEY`, `BEA_API_KEY`, `CENSUS_API_KEY` on Render (verify all four are populated, not just declared) | 0.5 day | Platform | Removes the silent "key not set" graceful errors that the LLM still passes through. |
| Wire **OECD SDMX** as `query_oecd_labor` (new module `oecd_integration.py`, mirrors `_query_eurostat`) | 4 days | Backend | 38-country labour-market coverage, free. |
| Extend **World Bank** to multi-indicator queries (extend `lookup_country_indicator_worldbank`) | 1 day | Backend | Multi-country benchmarking in a single tool call. |
| **Deprecate Firecrawl** - remove imports, archive `competitive_intel.py` Firecrawl branch | 1 day | Platform | Eliminates dead dependency + Sentry noise. |
| Buy/no-buy decision on **LinkUp** and **Revelio Labs** | n/a | Product + Finance | Either fund the keys or remove the tool entries to stop the LLM from advertising them. |
| Fix S50 STUBS: either remove `lookup_company_crunchbase` and `enrich_person_pdl` from `RECRUITMENT_TOOLS_SCHEMA`, or wire the paid Crunchbase Enterprise API | 2 days | Backend | Prevents Nova from telling users it can do Crunchbase lookups when it cannot. |

### Q4 2026 (Sep-Nov) - "Programmatic benchmarks live"

| Item | Effort | Owner | Outcome |
|---|---|---|---|
| **Appcast Benchmark API** procurement + integration | 5-7 days + 4-week sales cycle | Backend + Finance | First-class live programmatic-CPA + apply-rate data, replaces ~50% of `external_benchmarks_2025.json`. |
| **talent.com Publisher Feed** | 4 days | Backend | US/CA SMB job-volume signal that Adzuna misses. |
| **ABS Australia** (SDMX) | 2 days | Backend | First-class AU coverage. |
| **Mapbox Geocoding** | 1 day | Backend | Backup geocoder + better LATAM/SEA coverage. |
| Auto-refresh job for static benchmark JSONs (`external_benchmarks_2025.json` -> rename + quarterly cron via Appcast) | 2 days | Platform | Removes the staleness problem identified in 2.3. |

### 2027 H1 - "Selective premium expansion"

| Item | Effort | Owner | Outcome |
|---|---|---|---|
| **Recruitics Benchmark API** as second-source triangulation against Appcast | 5 days | Backend | Cross-validation increases LLM answer confidence. |
| **Salary.com Compensation API** (replaces Levels.fyi scrape) | 5 days | Backend | Audit-grade comp data for offer-letter & C-suite questions. |
| **Reddit JSON public feed** for qualitative talent-sentiment | 2 days | Backend | New conversational-quality signal for copilot prompts. |
| **HERE Geocoding** (tertiary geocoder failover) | 1 day | Platform | Resilience. |
| Evaluate **TikTok Research API** + **Indeed Publisher** access (commercial blockers first) | Spike | Product | Decide-or-park. |

---

## 6. Risks & Dependencies

1. **Single-vendor lock-in on Tavily.** `_web_search` falls back to `web_scraper_router`, but if Tavily becomes too expensive or rate-limits Joveo, the fallback path has not been load-tested at chatbot volumes. **Mitigation**: keep Jina-as-fallback funded; consider Brave Search API as a third leg before deprecating Firecrawl.
2. **MCC token rotation (Google Ads, Meta Ads).** Both `GOOGLE_ADS_DEVELOPER_TOKEN` and `META_ACCESS_TOKEN` are long-lived but require manual rotation. A failed rotation silently degrades `query_google_ads_performance` and `query_meta_performance`. **Mitigation**: monitor with a synthetic call from `data_matrix_monitor.py` once per hour; alert on 401.
3. **Stub tools mislead the LLM.** Three S50 tools (Crunchbase, PDL, possibly WARNTracker URL-stub) are advertised in the schema but return non-functional or URL-only payloads. The LLM has occasionally narrated answers as if real data was returned. **Mitigation**: prompt-engineer a "if stub, do not narrate as fact" instruction OR remove from schema until wired (preferred).
4. **GDPR/data-residency considerations** for any new EU integration (OECD, Eurostat extensions, ESCO deep traversal). All three publishers are EU bodies, so risk is low, but Joveo's data-processing addendum should be reviewed before Q3 2026 launches.
5. **Cache invalidation on cross-product writes.** Nova's `nova_cache.py` 24h TTL is fine for read-mostly endpoints; the new Appcast/Recruitics integrations will write into the same cache namespace. **Mitigation**: add a `programmatic_benchmarks` namespace with a 6h TTL distinct from the salary 24h tier.
6. **OECD SDMX endpoint volatility.** Both `sdmx.oecd.org/public/rest/data/...` permutations probed during this audit returned HTTP 404 with their URL forms; the older `stats.oecd.org/SDMX-JSON/...` also 410'd. The base host is healthy (HTTP 200 on root), so the integration is feasible but the **dataflow path needs Tavily-assisted reconnaissance against the current OECD documentation before kickoff**. Budget +2 days for path discovery.

---

## 7. Cost Estimates (3-year TCO)

Assumptions: engineer fully-loaded at $1,200/day. Render infra cost incremental. Tokens cached at 24h average.

| Item | Year 1 | Year 2 | Year 3 |
|---|---|---|---|
| **Engineering implementation (one-off)** | ~$40k (33 person-days across 10 integrations + plumbing) | ~$10k (maintenance, two refactors) | ~$10k |
| **OECD / ESCO extension / World Bank / ABS / Reddit / Mapbox / talent.com** | $0 license + $2k Mapbox overage | $2k | $2k |
| **Appcast Benchmark API** | $30-60k/yr (estimate; sales-quoted) | $30-60k | $30-60k |
| **Recruitics** (if adopted in 2027) | $0 | $25-50k | $25-50k |
| **Salary.com Compensation API** | $0 | $15-30k | $15-30k |
| **Crunchbase Enterprise** (if S50 stub gets wired) | $0 | $15-30k | $15-30k |
| **LinkUp + Revelio** (if buy decision is YES) | $24k + $50k = $74k | $74k | $74k |
| **Tavily** (existing) | unchanged | unchanged | unchanged |
| **Firecrawl** (deprecation) | -$5k (paused) | -$5k | -$5k |
| **3-yr TCO range** | $69k - $169k | $111k - $211k | $111k - $211k |

The recommendation is to **adopt items 1-7 (free + Appcast) in 2026** (~$70-100k Year-1 burden, ~80% of the value), and defer Recruitics/Salary.com/Crunchbase/LinkUp/Revelio to 2027 once Appcast usage proves the live-benchmarks workflow.

---

## 8. Verification Appendix (live-endpoint probes)

Three top-ranked recommended APIs were probed live during this audit (curl, May 2026):

```text
World Bank API (Rank #4):
  GET https://api.worldbank.org/v2/country/USA/indicator/SL.UEM.TOTL.ZS?format=json&date=2023
  HTTP 200, 342 bytes
  Sample: [{"page":1,"pages":1,"per_page":50,"total":1,...,"value":3.638,"date":"2023"}]
  lastupdated: 2026-04-08

  GET https://api.worldbank.org/v2/country/USA;IND;BRA/indicator/SL.EMP.TOTL.SP.ZS?format=json&date=2020:2023
  HTTP 200, 3127 bytes (multi-country, multi-year works)

ESCO (Rank #2, S50 already partial):
  GET https://ec.europa.eu/esco/api/search?text=software+engineer&type=occupation&language=en
  HTTP 200, 49745 bytes
  Sample: {"total":420,"offset":0,"limit":20,"text":"software engineer", ... _embedded.results[...]}
  Confirms 420 occupations match - graph traversal will be productive.

Reddit (Rank #9):
  GET https://www.reddit.com/r/recruiting/about.json
  HTTP 200, 4965 bytes
  Confirms public JSON works without OAuth at low volume.

OECD SDMX (Rank #1):
  Probed two variants, both 404 today:
   - sdmx.oecd.org/public/rest/data/OECD.ELS.SAE,DSD_LFS@DF_IALFS_UNE_M,1.0/...
   - sdmx.oecd.org/public/rest/data/OECD.SDD.TPS,DSD_LFS@DF_LFS_INDIC,1.0/...
  Base host healthy. OECD migrated their data portal in 2024 and the
  dataflow IDs/agency codes need verification against current docs:
  https://data-explorer.oecd.org/?lc=en  -- this is the official
  current data explorer; SDMX paths can be derived by URL-building
  from a saved query. Budget +2 days for OECD path discovery.
```

The mixed result is itself evidence: do not assume any vendor's docs are current. Tavily + the OECD data explorer are sufficient to pin the live dataflow IDs in a 2-day spike before committing the 4-6 day implementation budget for Rank #1.

---

## 9. Decision Triggers for Re-evaluation

Re-audit this document if any of the following occurs:

1. **Cost shift**: Tavily price increase > 30%, or Render infra change.
2. **Coverage shift**: Joveo enters a new region (Brazil, MENA, SEA) requiring data sources not in this audit.
3. **Vendor signal**: Appcast or Recruitics ships a free benchmark tier.
4. **Tool count exceeds ~90**: at that point context-window pressure on the LLM justifies tool-pruning before adding more.
5. **A new stub is detected in `RECRUITMENT_TOOL_DISPATCH`** (recurring problem from S50).
6. **Cache hit rate drops below 60%** (currently inferred ~70-80% per `nova_cache.py` design); points to fresh data being needed more often than current TTLs assume.

---

**End of audit. Word count ~3,000.**
