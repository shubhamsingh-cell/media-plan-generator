# API Scout Report — New Free/Freemium Data Sources for Nova

**Date:** 2026-06-02 | **Scout:** Technology evaluation pass | **Mode:** Research only, no code changes
**Integration pattern:** `fetch_X()` in `api_enrichment.py` (stdlib `urllib` only, ~250 LOC) + `_query_X` tool in `nova.py`, mirroring `fetch_esco_occupations` (S80) and `fetch_oecd_sdmx_data` (S78).

## Methodology

All free-tier claims below were verified on 2026-06-02 via live `curl` probes (HTTP status + payload inspection) and cross-referenced against published docs. Status codes: `200` = working open endpoint; `401/402/422/400` = endpoint live but key/credit-gated; `410/000` = dead/deprecated. Probes returning real data are noted explicitly. Candidates already in Nova (World Bank, ILOSTAT, ESCO, OECD, O*NET, Census, BLS, etc.) are excluded from recommendations.

**Honest exclusions (verified dead or useless):**
- **Glassdoor API** — `410 Gone`. Officially decommissioned years ago; no 2026 API. Do not pursue.
- **Indeed Publisher (apisearch)** — connection refused (`000`). Closed to new publishers since 2021; legacy endpoint dead.
- **Levels.fyi** — `403` on the unofficial `salaryData.json`; no public free API (matches existing stub in `recruitment_apis.py` line 674: "programmatic API requires application"). Embed-only.
- **OpenCorporates** — now `401` ("Invalid Api Token"); the old open `v0.4` tier is gone, key required and the free tier is heavily throttled/approval-gated. Marginal.
- **PeopleDataLabs / Crunchbase** — already stubbed in `recruitment_apis.py` (key-gated, `PDL_API_KEY`/`CRUNCHBASE_API_KEY` unset). PDL free tier is 100 lookups/mo — too small for production enrichment. Not re-recommended; wire only if a key is purchased.
- **ZipRecruiter Publisher** — `502`; partner-program only, no self-serve free tier in 2026.
- **DataUSA** — `404` on the data path; already disabled in Nova via `DATAUSA_DISABLED` flag. Skip.
- **Coresignal / TheirStack / JobsPikr** — all paid-first; free tiers are trial credits (Coresignal `404` on open path, TheirStack `405` = POST-only key-gated). Useful data but not "free." TheirStack noted in stretch list.

---

## Ranked Top-10 to Integrate

### 1. Greenhouse Job Board API — **HIGH** relevance
- **Provides:** Live, structured job postings (title, location, department, full description, absolute URL) for any employer using Greenhouse ATS.
- **URL:** `https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs`
- **Free tier:** Fully open, **no key**, no documented hard rate limit (be polite ~1 req/s). VERIFIED `200`: Datadog board returned **423 real jobs** with locations (e.g. "AI Research Engineer — DAIR, New York").
- **Auth:** No-key public.
- **Coverage:** Global (employer-dependent; thousands of mid/large tech, healthcare, gig employers).
- **Nova use case:** Competitive intel (`competitive_intel.py`, `hire_signal.py`) — count a target employer's open reqs, geographic spread, function mix, hiring velocity. Directly feeds "what is competitor X hiring for, where" in the chatbot and media-plan competitive section.
- **Effort:** ~200 LOC. `fetch_greenhouse_jobs(board_token, content=False)` + `_query_competitor_hiring` tool. Trivial JSON shape.

### 2. DBnomics API — **HIGH** relevance
- **Provides:** Free aggregator over **90+ providers** (IMF WEO, ILO, Eurostat, OECD, BLS, World Bank, national statistics) through ONE consistent JSON API. Single integration → dozens of macro series.
- **URL:** `https://api.db.nomics.world/v22/series/{provider}/{dataset}/{series}?observations=1`
- **Free tier:** Fully open, **no key**, generous. VERIFIED: BLS CPI series `200`; IMF WEO `USA.LUR` returned named series "United States – Unemployment rate – Percent of total labor force" via `302→200`.
- **Auth:** No-key public.
- **Coverage:** Global (all member-state stats agencies).
- **Nova use case:** Macro enrichment in `market_intel_reports.py` / `data_synthesizer.py` — unemployment, wage growth, GDP, labor-force participation for ANY country a media plan targets. Fills the EMEA/APAC macro gaps that pure-US sources (BLS/JOLTS) miss, complementing OECD/ILOSTAT without per-provider auth.
- **Effort:** ~250 LOC. `fetch_dbnomics_series(provider, dataset, series)` + `_query_macro_indicator` tool. One client unlocks many indicators.

### 3. Jina AI Reader — **HIGH** relevance
- **Provides:** URL → clean LLM-ready markdown extraction (`r.jina.ai`) — strips boilerplate from career pages, competitor sites, news.
- **URL:** `https://r.jina.ai/{target_url}`
- **Free tier:** Open **no-key** baseline (VERIFIED `200`, returned extracted content). Free API key raises limits to ~1M tokens / generous RPM. Note: Nova already lists `JINA_API_KEY` (10M tokens) in env and references "Jina Reader" in `nova.py:24501` — **partial overlap; verify it is fully wired as a `fetch_` function, not just a label.** If only labelled, formalize it.
- **Auth:** No-key public (or free-key for higher limits).
- **Coverage:** Global (any public URL).
- **Nova use case:** Career-page audit (`audit_tool.py`), competitor creative scraping, feeding RAG (`rag_pipeline.py`). Cheaper/cleaner than Firecrawl (which is out of credits per memory) and a strong scraper-router fallback in `web_scraper_router.py`.
- **Effort:** ~150 LOC if not already wired; mostly a thin GET wrapper + scraper-router registration.

### 4. SEC EDGAR (data.sec.gov) — **HIGH** relevance
- **Provides:** US public-company filings, financials (XBRL company facts), recent filing feeds. Layoffs, hiring guidance, segment revenue, headcount disclosures.
- **URL:** `https://data.sec.gov/submissions/CIK{##########}.json` and `/api/xbrl/companyfacts/CIK{##########}.json`
- **Free tier:** Fully open, **no key**, requires a `User-Agent` header (email). VERIFIED `200` for Amazon CIK. ~10 req/s fair-use limit.
- **Auth:** No-key (UA header mandatory).
- **Coverage:** US public companies (global ADRs included).
- **Nova use case:** Competitive/firmographic intel — verify a target employer's financial health, headcount trend, and recent layoff/RIF 8-K filings to time outreach and inform "company viability" in `competitive_intel.py`. Strong signal for enterprise accounts.
- **Effort:** ~250 LOC. `fetch_sec_company_facts(cik_or_ticker)` + ticker→CIK lookup (SEC publishes a free `company_tickers.json`) + `_query_company_financials` tool.

### 5. Lightcast Open Skills API — **HIGH** relevance
- **Provides:** Open, standardized skills taxonomy (~32k skills) with IDs, types, descriptions, and related skills. The de-facto skills graph behind much HR-tech.
- **URL:** `https://emsiservices.com/skills/versions/latest/skills` (auth via `https://auth.emsicloud.com/connect/token`)
- **Free tier:** **Free** "Open Skills" tier via OAuth `client_credentials`. VERIFIED: skills endpoint `401` without token, auth endpoint `400` on empty body (= live, expects free client_id/secret). Self-serve free registration at lightcast.io/open-skills.
- **Auth:** OAuth2 client-credentials (free client_id/secret) → bearer token (cache ~1h).
- **Coverage:** Global (English-centric taxonomy).
- **Nova use case:** Skills extraction/normalization in `skill_target.py`, `role_taxonomy.py`. Complements ESCO (EU) with a US/market-driven taxonomy + skill-relatedness for "skills adjacency" recommendations. Higher industry adoption than ESCO for US recruiting.
- **Effort:** ~280 LOC (slightly above baseline due to token caching). `fetch_lightcast_skills(query)` + token helper + `_query_skill_taxonomy` tool.

### 6. IMF DataMapper API — **MED-HIGH** relevance
- **Provides:** Curated macro indicators (real GDP growth, unemployment, inflation) by country, simple flat JSON. Lighter-weight than DBnomics for headline numbers.
- **URL:** `https://www.imf.org/external/datamapper/api/v1/{indicator}/{ISO3}`
- **Free tier:** Fully open, **no key**. VERIFIED `200`; `/indicators` returned the full indicator catalog with labels/descriptions.
- **Auth:** No-key public.
- **Coverage:** Global (all IMF member states).
- **Nova use case:** Quick country macro context for international media plans (`intl_benchmark_lookup.py`). Partial overlap with DBnomics — **if #2 is integrated, this becomes redundant** for the same indicators. Recommend EITHER DBnomics (broader) OR DataMapper (simpler), not both. Listed for the simpler-integration option.
- **Effort:** ~150 LOC. Flattest JSON of the macro set.

### 7. Ashby Posting API — **MED-HIGH** relevance
- **Provides:** Job postings **with compensation ranges** for employers on Ashby ATS (fast-growing among startups/scaleups).
- **URL:** `https://api.ashbyhq.com/posting-api/job-board/{board_name}?includeCompensation=true`
- **Free tier:** Open **no-key** public board endpoint. VERIFIED `200`: Ashby's own board returned **55 jobs with compensation present on the first record** (title "Engineering Manager, EU").
- **Auth:** No-key public.
- **Coverage:** Global; skews to tech startups/scaleups (high-value for AI-labs/competitor comp benchmarking).
- **Nova use case:** **Salary benchmarking from real postings** — Ashby's `includeCompensation` is rare among free ATS feeds and directly feeds `data_synthesizer.py` comp ranges and SlotOps/competitive comp intel. Pairs with Greenhouse (#1) for breadth.
- **Effort:** ~200 LOC. `fetch_ashby_jobs(board_name)` + `_query_ats_comp` tool (shared with Greenhouse).

### 8. Lever Postings API — **MED** relevance
- **Provides:** Job postings (title, team, location, commitment) for employers on Lever ATS.
- **URL:** `https://api.lever.co/v0/postings/{company}?mode=json`
- **Free tier:** Open **no-key**. VERIFIED `200` on demo board (`leverdemo`). Note: tested employers (Netflix) returned 0 — many large names have migrated off Lever, so **coverage is employer-dependent**; still valuable for the long tail of Lever customers.
- **Auth:** No-key public.
- **Coverage:** Global; SMB/mid-market skew.
- **Nova use case:** Same competitor-hiring/firmographic use case as Greenhouse; add as a second ATS source so the "count competitor reqs" tool resolves more employers. Best built as one multi-ATS function.
- **Effort:** ~120 LOC (fold into the Greenhouse/Ashby multi-ATS fetcher).

### 9. Wikidata + Wikipedia REST API — **MED** relevance
- **Provides:** Free firmographic/entity data — company founding year, HQ, industry, employee count (where present), aliases; Wikipedia REST gives clean company summaries.
- **URL:** `https://www.wikidata.org/w/api.php?action=wbsearchentities&search={name}` + `https://en.wikipedia.org/api/rest_v1/page/summary/{title}`
- **Free tier:** Fully open, **no key** (UA header for Wikipedia). VERIFIED `200` for both (Stripe search + summary).
- **Auth:** No-key public.
- **Coverage:** Global.
- **Nova use case:** Free firmographic enrichment to replace the gap left by paid Clearbit/Crunchbase/PDL — resolve a company's industry, size band, and HQ for the media-plan "client context" and `competitive_intel.py`. Lower precision than paid sources but $0 and global.
- **Effort:** ~220 LOC. `fetch_company_firmographics_wikidata(name)` (search → entity claims) + `_query_company_profile` tool. Slightly fiddly claim parsing.

### 10. SmartRecruiters Public Postings API — **MED-LOW** relevance
- **Provides:** Public job postings for employers on SmartRecruiters ATS.
- **URL:** `https://api.smartrecruiters.com/v1/companies/{company}/postings`
- **Free tier:** Open **no-key** (VERIFIED `200`). **Honest caveat:** all tested employers (Square, Bosch, IKEA, Ubisoft, Avalara) returned `totalFound: 0` — either renamed identifiers or migrated. The endpoint is healthy but **resolving the correct company slug is the hard part**; coverage felt thin in probes.
- **Auth:** No-key public.
- **Coverage:** Global enterprise; slug-dependent.
- **Nova use case:** Third ATS source for competitor-hiring breadth. Low priority given slug-resolution friction; only worth it once a board-token/slug directory is in place.
- **Effort:** ~120 LOC (fold into multi-ATS fetcher), plus slug-resolution overhead.

---

## Quick-Win Subsection — 3 Easiest, Highest-Impact

These three are **no-key, verified returning real data today, ~150–250 LOC each**, and hit distinct high-value Nova use cases. Recommended for the next integration session.

| # | API | Why it's a quick win | Verified payload |
|---|-----|----------------------|------------------|
| **A** | **Greenhouse Job Board** (#1) | No-key, trivial JSON, instantly powers "what/where is competitor X hiring" — a top chatbot ask. Highest impact-per-LOC. | 423 live jobs at Datadog w/ locations |
| **B** | **DBnomics** (#2) | One no-key integration unlocks IMF/ILO/Eurostat/World Bank macro for **every country** a plan targets — closes EMEA/APAC macro gaps in one shot. | Named IMF WEO unemployment series for USA |
| **C** | **SEC EDGAR** (#4) | No-key (UA only), authoritative US-public-company financials + layoff 8-Ks → "is this employer healthy / hiring or cutting?" for enterprise accounts. | 200 on Amazon company submissions |

**Sequencing note:** Build Greenhouse + Ashby + Lever as a single `fetch_ats_postings(provider, board)` multi-ATS function with one shared `_query_competitor_hiring` / `_query_ats_comp` tool — three sources, one ~300-LOC module, maximum coverage. DBnomics and SEC EDGAR are independent and parallelizable.

---

## Overlap / Do-Not-Double-Count Ledger

- **IMF DataMapper (#6)** overlaps **DBnomics (#2)** — pick one (DBnomics broader). Listed both only because #6 is a simpler integration if scope is tight.
- **Jina Reader (#3)** is **partially present** — `JINA_API_KEY` is in env and "Jina Reader" appears in `nova.py:24449/24501`. Confirm whether it's a real `fetch_` function or just a router label before re-building. Overlaps Firecrawl (out of credits) and the existing scraper router.
- **ILOSTAT / OECD / World Bank / ESCO / Census / BLS** — already integrated; DBnomics re-exposes them but via one unified client (net simplification, not duplication).
- **Wikidata/Wikipedia (#9)** is the free substitute for the existing **PDL/Crunchbase/Clearbit stubs** (all key-gated/paid) — fills firmographics at $0.
- **Mapbox / Foursquare / Overpass / Nominatim** (Category 6) — geographic. Probed live (Mapbox/Foursquare `401` key-gated; Nominatim `200`; Overpass `406` on the test query but service is up). **All overlap existing GeoNames + Google Maps in Nova** — no compelling gap; excluded from top-10. Nominatim is a viable free no-key geocode fallback if GeoNames quota becomes a problem, but not a net-new capability.

## Stretch / Watchlist (not recommended now)

- **Brave Search API** — `422` (live, key-gated). Free tier ~2,000 queries/mo, 1 q/s. Genuine free tier and a solid Tavily complement, but **overlaps existing Tavily** search; only add if Tavily quota is a constraint. Free-key.
- **Exa (Metaphor)** — `402` (live, credits required). Neural search, ~$10 free credits then paid. Not a sustained free tier; overlaps Tavily. Hold.
- **TheirStack** — `405` (POST-only, key-gated). Excellent tech-stack + job-postings data but free tier is trial credits only. Revisit if budget opens.
- **UN Comtrade** — `200` open preview tier, but trade data has **low recruitment relevance**. Skip.
- **PatentsView (USPTO)** — probe inconclusive (empty status, API recently migrated to `search.patentsview.org` with a free key requirement). Innovation-intensity signal for tech-hiring intel; low priority.
- **Companies House UK** — `401` (free key, basic-auth). Solid free UK firmographics, but **UK-only** and narrower than Wikidata's global coverage. Add only for UK-heavy accounts.
- **Numbeo** — `200` on `/cities` but real cost-of-living data is paid-key; cost-of-living already approximable via existing macro sources. Skip.

## Risk Notes

- **ATS feeds (Greenhouse/Lever/Ashby/SmartRecruiters):** undocumented public endpoints — **no SLA, can change without notice.** Wrap each in its own try/except (per project rule) and treat as best-effort enrichment, never a hard dependency. Coverage is employer-dependent; build a board-token directory for target accounts.
- **No-key endpoints (DBnomics, SEC, IMF, Wikidata, Jina):** stable and institutionally backed (SEC/IMF) or well-funded OSS (DBnomics/Jina); lowest lock-in risk. Always send a descriptive `User-Agent` (mandatory for SEC, polite for the rest).
- **Lightcast:** free tier is generous but OAuth adds token-refresh complexity and a vendor relationship; data is proprietary taxonomy (re-evaluate if they sunset the open tier).
- All recommendations fit the existing stdlib-`urllib` pattern — **no new dependencies**, consistent with `requirements.txt` discipline.
