# Client Plans, Supply Repository & Benchmark Usage Audit

**Date:** 2026-05-22
**Scope:** Read-only audit of `nova.py` (25,989 lines) + `kb_loader.py` (892 lines) + `vector_search.py` (~1,750 lines).
**Verdict:** **Partial reachability. ~55% of the brain layer is dead or buried.**

---

## 1. Executive Summary

The user's concern is correct. Out of **49 KB sections registered in `kb_loader.KB_FILES`** plus 1 critical file that was never registered, only **18 sections (≈37%)** have a direct `self._data_cache.get(...)` reader bound to a tool that can be triggered by realistic queries. The rest are in three failure buckets:

| Bucket | Count | Description |
|--------|-------|-------------|
| **A. Reachable & live** | 18 | A tool reads the key and at least one realistic query routes to that tool. |
| **B. Reachable only via vector RAG** | 11 | No direct reader, but `vector_search.py` indexes the file. `knowledge_search` may surface it — quality depends on embedding match. |
| **C. Buried — reader exists, but no realistic trigger** | 6 | Reader present, but the system prompt / tool descriptions do not steer queries at it. |
| **D. Stranded — loaded but no reader anywhere** | 15+ | File is `json.load`-ed into RAM, costs memory, and no code path surfaces it. |
| **E. Critical bug — broken reader** | 1 | `_query_kb_deep` (the catch-all for 32 datasets) reads `self._kb`, which is **never assigned** (line 7233). It silently returns empty for every dataset. |
| **F. Never loaded** | 2 | `industry_reports_2026.json` (110KB, created today) and `recruitment_benchmarks_comprehensive_2026.json` (43KB) — registered nowhere in `KB_FILES`. |

### Headline numbers
- **49** KB sections registered in `kb_loader.KB_FILES` (`kb_loader.py:211-290`)
- **~3.5MB** of KB JSON loaded into memory at startup
- **18** tool handlers actually read `self._data_cache.get(<key>)` (counted in §3)
- **3 of the user's "must-be-used" files have zero readers**: `joveo_2026_benchmarks`, `joveo_cpa_benchmarks_2026`, `recruitment_benchmarks_comprehensive_2026` (last one isn't even loaded as a section)
- The "Why this answer?" transparency panel was redesigned in S56 (`kb_loader.py:32-205`) to track real reads — **but if 31 of 49 files are never read, the panel will never show them.** The redesign exposed the wiring gap rather than fixing it.

---

## 2. Architecture Recap (3 lines)

1. **Load:** `kb_loader.load_knowledge_base()` reads every file in `KB_FILES` into a `TrackedDict` and returns the singleton (`kb_loader.py:518-634`).
2. **Share:** `Nova._load_data_sources()` aliases `self._data_cache = kb` (`nova.py:3914`). Same object — hot-reload propagates.
3. **Surface:** Tool handlers do `self._data_cache.get("<cache_key>", {})` and return scoped slices. The LLM picks tools from `_tool_handler_map()` (`nova.py:6503-6614`).

**Key invariant:** A KB file is only reachable via chat if (a) registered in `KB_FILES`, AND (b) some `_query_*` handler reads its cache key, AND (c) some tool definition in `get_tool_definitions()` exposes that handler, AND (d) realistic user phrasing triggers that tool. Break any link and the data is dark.

---

## 3. Per-File Trace Table

Legend: ✅ reachable / ⚠ buried (reader exists but rarely triggered) / 🟦 RAG only / ❌ stranded / 💥 reader broken / 🚫 not loaded

| # | File | Cache Key | Reader in nova.py | Tool | Status | Realistic Trigger |
|---|------|-----------|-------------------|------|--------|-------------------|
| 1 | `client_media_plans_kb.json` | `client_media_plans` | `_query_client_plans` (`nova.py:10400`) | `query_client_plans` (`nova.py:4940`) | ✅ | "past media plans for healthcare clients" |
| 2 | `client_plans/rtx_usa_media_plan.json` | `rtx_media_plan` + merged into `client_media_plans.plans` | `_merge_client_plans_into_shared_kb` (`nova.py:3931-3974`) → readable via `_query_client_plans` lookup of `plan_key` | `query_client_plans` | ✅ | "show me RTX media plan" |
| 3 | `client_plans/rtx_aerospace_defense_benchmarks.json` | `rtx_aerospace_benchmarks` + merged | same | `query_client_plans` | ✅ | same |
| 4 | `joveo_global_supply_repository.json` | `global_supply_repository` (alias `expanded_supply_repo`) | `_query_global_supply` (`nova.py:6820`), `_query_publishers` fallback (`nova.py:7050`), publishers panel (`nova.py:16184`) | `query_global_supply`, `query_publishers` | ✅ | "what publishers do we have for tech in India" |
| 5 | `joveo_publishers.json` | `joveo_publishers` | `_query_publishers` (`nova.py:7000`), `_query_ad_platform` (`nova.py:8660`), prompt builder (`nova.py:4072`), publishers panel (`nova.py:16182`) | `query_publishers` | ✅ | "list publishers for India tech" |
| 6 | `joveo_2026_benchmarks.json` | `joveo_2026_benchmarks` | **NONE** — only listed as enum value in `_query_kb_deep` (`nova.py:7221`) and mentioned in system prompt strings (`nova.py:4082, 4257`). | `query_kb_deep` (broken) | 💥 | "what's Joveo's 2026 healthcare CPA benchmark" — system prompt instructs LLM to use `joveo_cpa_benchmarks` (`nova.py:4032`) but no tool reads it. |
| 7 | `joveo_cpa_benchmarks_2026.json` | `joveo_cpa_benchmarks` | **NONE** — same as #6. Only referenced in system prompt and `_query_kb_deep` enum. | `query_kb_deep` (broken) | 💥 | "joveo CPA for nursing in Texas" — LLM is told to use this but cannot reach it. |
| 8 | `category_to_partners.json` | `category_to_partners` | **NONE** — registered in `KB_FILES` (`kb_loader.py:275`) but **zero** `_data_cache.get("category_to_partners")` callers. | none | ❌ | "what diversity partners do we have" |
| 9 | `channels_db.json` | `channels_db` | `_query_channels` (`nova.py:6916`), `_query_publishers` fallback (`nova.py:7001`), publishers panel (`nova.py:16183`) | `query_channels`, `query_publishers` | ✅ | "what channels are available" |
| 10 | `channel_benchmarks_live.json` | `channel_benchmarks_live` | **NONE** | none (RAG-indexed in `vector_search.py:1664`) | 🟦 | "live channel CPC" — only via fuzzy embedding match |
| 11 | `healthcare_supply_map_us.json` | `healthcare_supply_map_us` | **NONE** | none, not RAG-indexed | ❌ | "US healthcare supply partners" — 194KB of curated mapping with no readers. |
| 12 | `recruitment_benchmarks_comprehensive_2026.json` | **NOT IN `KB_FILES`** — file is 43KB on disk but `kb_loader` never opens it. Read by `app.py:5145`, `roi_projector.py:84`, `vector_search.py:1683` directly via `open()`. | none in chat path | RAG-only via `_knowledge_search` | 🟦 | "what's our 2026 CPA benchmark for nursing" — chatbot only sees this through vector search of 28-source compilation, with no direct accessor. |
| 13 | `ad_benchmarks_recruitment_2026.json` | `ad_benchmarks_recruitment` | **NONE** | none (vector-indexed) | 🟦 | "recruitment ad benchmarks" |
| 14 | `google_ads_2025_benchmarks.json` | `google_ads_benchmarks` | `_query_google_ads_benchmarks` (`nova.py:10112`) | `query_google_ads_benchmarks` | ✅ | "Google Ads CPC for healthcare" |
| 15 | `external_benchmarks_2025.json` | `external_benchmarks` | `_query_external_benchmarks` (`nova.py:10261`) | `query_external_benchmarks` | ✅ | "external industry CPA benchmark" |
| 16 | `labor_market_outlook_2026.json` | `labor_market_outlook` | **NONE** in direct read; `_query_kb_deep` enum lists it (`nova.py:7216`) but the tool is broken. | (`query_kb_deep` broken) | 💥 | "2026 labor market outlook" |
| 17 | `industry_hiring_patterns_2026.json` | `industry_hiring_patterns` | **NONE**; `_query_kb_deep` enum lists it (broken). | (broken) | 💥 | "2026 hiring patterns by industry" |
| 18 | `healthcare_specialty_pay_2026.json` | `healthcare_specialty_pay_2026` | **NONE** | none, not vector-indexed | ❌ | "RN pay by specialty 2026" |
| 19 | `h1b_salary_intelligence.json` | `h1b_salary_intelligence` | **NONE** in `_data_cache.get`; vector-indexed (`vector_search.py:1681`). `_query_h1b_salaries` (`nova.py:11480`) uses a different LCA API path, not this file. | RAG only | 🟦 | "H-1B salary for software engineer" |
| 20 | `industry_reports_2026.json` (110KB, today) | **NOT IN `KB_FILES`, NOT IN `vector_search`** | **ZERO readers in any `.py` file** | none | 🚫 | (impossible to surface) |
| 21 | `recruitment_industry_knowledge.json` | `core` (+ flattened to top-level via `_rebuild_backward_compat`) → `knowledge_base` alias | `_query_knowledge_base` (`nova.py:7450`), `_suggest_smart_defaults`, `_query_market_demand` (`nova.py:8196`), 8+ readers | `query_knowledge_base` | ✅ | "what's industry CPH for tech" |
| 22 | `platform_intelligence_deep.json` | `platform_intelligence` | `_query_platform_deep` (`nova.py:9558`), `_query_channels` (`nova.py:6955`) | `query_platform_deep`, `query_channels` | ✅ | "deep platform analysis for Indeed" |
| 23 | `recruitment_benchmarks_deep.json` | `recruitment_benchmarks` | `_query_recruitment_benchmarks` (`nova.py:9667`) | `query_recruitment_benchmarks` | ✅ | "recruitment benchmark for retail" |
| 24 | `recruitment_strategy_intelligence.json` | `recruitment_strategy` | `_query_employer_branding` (`nova.py:9820`) | `query_employer_branding` | ⚠ | "employer brand strategy" — narrow trigger |
| 25 | `regional_hiring_intelligence.json` | `regional_hiring` | `_query_regional_market` (`nova.py:9851`) | `query_regional_market` | ✅ | "hiring market in Phoenix" |
| 26 | `supply_ecosystem_intelligence.json` | `supply_ecosystem` | `_query_supply_ecosystem` (`nova.py:9942`) | `query_supply_ecosystem` | ⚠ | rarely triggered — vague tool name |
| 27 | `workforce_trends_intelligence.json` | `workforce_trends` | `_query_workforce_trends` (`nova.py:9976`) | `query_workforce_trends` | ✅ | "workforce trends in healthcare" |
| 28 | `industry_white_papers.json` | `white_papers` | `_query_white_papers` (`nova.py:10030`), `_query_platform_deep` (`nova.py:9704`) | `query_white_papers` | ⚠ | almost never triggered by LLM tool-pick |
| 29 | `international_sources.json` | `international_sources` | `_query_knowledge_base` (`nova.py:7575`) | indirect | ⚠ | "international hiring sources" |
| 30 | `international_benchmarks_2026.json` | `international_benchmarks` | `_query_knowledge_base` (`nova.py:7593`), `_query_recruitment_benchmarks` country branch (`nova.py:9621`), `_query_market_demand` (`nova.py:8110`) | `query_recruitment_benchmarks` (country!="US") | ✅ | "CPA in Germany" |
| 31 | `linkedin_guidewire_data.json` | `linkedin_guidewire` | `_query_linkedin_guidewire` (`nova.py:9483`), `nova.py:20594` | `query_linkedin_guidewire` | ✅ | "Guidewire on LinkedIn" |
| 32 | `craigslist_performance_benchmarks.json` | `craigslist_benchmarks` | `_query_knowledge_base` (`nova.py:7629`) | indirect, only when `knowledge_base` queried | ⚠ | "Craigslist benchmarks" — single conditional read |
| 33 | `linkedin_performance_benchmarks.json` | `linkedin_benchmarks` | **NONE** in `_data_cache.get`. RAG-indexed only (`vector_search.py:1684`). | none direct | 🟦 | "LinkedIn apply rate by country" |
| 34 | `adzuna_benchmarks.json` | `adzuna_benchmarks` | one read inside `nova.py:12803` (inside a try-block in `_query_linkup_postings` fallback) | indirect | ⚠ | rarely fires |
| 35 | `competitor_careers.json` | `competitor_careers` | **NONE** | none direct (vector-indexed) | 🟦 | "Amazon careers page intel" |
| 36 | `fred_indicators.json` | `fred_indicators` | **NONE** | none direct (vector-indexed) | 🟦 | "FRED labor indicators" |
| 37 | `google_trends.json` | `google_trends` | **NONE** | none direct (vector-indexed) | 🟦 | "Google Trends for nursing jobs" |
| 38 | `job_density_metros.json` | `job_density_metros` | **NONE** | none direct (vector-indexed) | 🟦 | "metro job density" |
| 39 | `job_posting_volumes.json` | `job_posting_volumes` | **NONE** | none direct (vector-indexed) | 🟦 | |
| 40 | `live_market_data.json` | `live_market_data` | **NONE** | none direct (vector-indexed) | 🟦 | |
| 41 | `market_trends_live.json` | `market_trends_live` | **NONE** | none direct (vector-indexed) | 🟦 | |
| 42 | `platform_ad_specs.json` | `platform_ad_specs` | **NONE** | none direct (vector-indexed) | 🟦 | |
| 43 | `seasonal_hiring_trends.json` | `seasonal_hiring_trends` | one read at `nova.py:4404` (smart-defaults prompt builder, conditional) | indirect | ⚠ | "Q4 hiring trends" |
| 44 | `global_supply.json` | `global_supply` | `_query_global_supply` (`nova.py:6819`), `_query_ad_platform` (`nova.py:8649`) | `query_global_supply` | ✅ | covered by #4 |
| 45 | `partner_specialty_crosswalk.json` | `partner_specialty_crosswalk` | **NONE** | none, not vector-indexed | ❌ | "partners for cardiology nurses" — 191KB stranded |
| 46 | `partner_url_registry.json` | `partner_url_registry` | **NONE** | none, not vector-indexed | ❌ | URL O(1) lookup unused — 149KB stranded |
| 47 | `recruitment_benchmarks_2026_deep.json` | `recruitment_benchmarks_2026_deep` | **NONE** | none, not vector-indexed | ❌ | "deep recruitment benchmark" — 37KB S54 research stranded |
| 48 | `employer_career_intelligence_2026.json` | `employer_career_intelligence_2026` | **NONE** | none, not vector-indexed | ❌ | "top employer career strategies" — 47KB stranded |
| 49 | `top_employers_by_city_2026.json` | `top_employers_by_city` | **NONE** | none direct, vector-indexed | 🟦 | "top employers in Atlanta" |
| 50 | `salary_benchmarks_detailed_2026.json` | `salary_benchmarks_detailed` | **NONE** in `_data_cache.get`; mentioned in `_query_kb_deep` enum (broken). | (broken) | 💥 | covered via `query_salary_data` other path |
| 51 | `compliance_regulations_2026.json` | `compliance_regulations` | **NONE** | none direct, vector-indexed | 🟦 | |
| 52 | `agency_rpo_market_2026.json` | `agency_rpo_market` | **NONE** | none direct, vector-indexed | 🟦 | |
| 53 | `hr_tech_landscape_2026.json` | `hr_tech_landscape` | **NONE** | none direct, vector-indexed | 🟦 | |
| 54 | `publisher_benchmarks_2026.json` | `publisher_benchmarks` | **NONE** | none direct, vector-indexed | 🟦 | |
| 55 | `recruitment_marketing_trends_2026.json` | `recruitment_marketing_trends` | **NONE** | none direct, vector-indexed | 🟦 | |

---

## 4. Critical Findings

### 4.1 The `_query_kb_deep` bug (P0)
`nova.py:7233` reads `kb = self._kb or {}`. **`self._kb` is never assigned anywhere in `nova.py`** (verified with `grep -n "self\._kb\b"` — only the one read site, plus a defensive `getattr(self, "_kb", None) or {}` at `nova.py:15906`). Every call to `query_kb_deep` returns `{"error": "Dataset 'X' not found in knowledge base"}` regardless of which dataset is requested. The tool description (`nova.py:6411`) advertises 32 datasets — none reachable. This is the single biggest wiring failure: a tool was added in S50 specifically to surface the long tail of KB files, and it has never worked.

### 4.2 Joveo's own benchmarks are unreachable from chat (P0)
- `joveo_2026_benchmarks.json` (11KB, the headline 2026 benchmark file)
- `joveo_cpa_benchmarks_2026.json` (40KB, 304 real programmatic CPA categories)

Both are loaded by `kb_loader` and **mentioned in the system prompt** ("Use joveo_cpa_benchmarks from knowledge base for CPA data", `nova.py:4032`) — but no handler reads them. The LLM is being told to use data the dispatch layer can't return. When a user asks "what's Joveo's 2026 CPA for nursing in Texas", the LLM either invents a number or falls back to `recruitment_benchmarks_deep`, which is older general industry data.

### 4.3 `recruitment_benchmarks_comprehensive_2026.json` is structurally stranded (P0)
This is the 28-source compilation the user explicitly named. It is **not registered in `KB_FILES`** (`grep -n "comprehensive" kb_loader.py` returns nothing). `app.py:5145`, `roi_projector.py:84`, `data_synthesizer.py:4661` open it directly with `open()` — those are batch / plan-generation paths, not chat. The chat layer can only reach it through `_knowledge_search` (vector_search.py indexes it at line 1683), which means the data only appears when (a) the LLM picks `knowledge_search` instead of a specific `query_*` tool, AND (b) the embedding match is strong enough. Most fact lookups won't trigger it.

### 4.4 `industry_reports_2026.json` (110KB, created today) has zero readers (P0)
`grep -rn "industry_reports_2026" *.py` returns nothing. The F2 agent created the file but no integration code was written. Pure waste of disk and memory if loaded by accident.

### 4.5 Healthcare supply (S52) — 480KB of stranded curation (P1)
The user invested in `healthcare_supply_map_us.json` (194KB), `partner_specialty_crosswalk.json` (191KB), `partner_url_registry.json` (149KB), `category_to_partners.json` (133KB) — 667KB combined. All are in `KB_FILES`, all are loaded, none have `_data_cache.get(...)` readers, and only one (`healthcare_supply_map_us`) is even theoretically reachable via the `expanded_supply_repo` alias — except the alias points to `global_supply_repository`, not this map. **A user query "what healthcare partners cover cardiology" will not find this map.**

### 4.6 S54 deep research — 84KB stranded (P1)
`recruitment_benchmarks_2026_deep.json` and `employer_career_intelligence_2026.json` were added with the comment "Reusable across Nova chat, media plan generator, and all Plan/Intelligence/Compliance products" (`kb_loader.py:276-283`). Neither has a chat reader. Not indexed by vector_search either.

---

## 5. Stranded Data List (no reader path)

These are files we paid to create / curate that the chatbot **cannot** surface, even with vector RAG:

| File | Size | Why stranded |
|------|------|--------------|
| `industry_reports_2026.json` | 110KB | Not in `KB_FILES`, not in `vector_search`, zero `.py` references |
| `partner_specialty_crosswalk.json` | 191KB | In `KB_FILES`, but no reader and not vector-indexed |
| `partner_url_registry.json` | 149KB | same |
| `category_to_partners.json` | 133KB | same |
| `healthcare_supply_map_us.json` | 194KB | same |
| `healthcare_specialty_pay_2026.json` | 33KB | same |
| `recruitment_benchmarks_2026_deep.json` | 37KB | same |
| `employer_career_intelligence_2026.json` | 47KB | same |

**Subtotal: ~900KB of brain mass we cannot surface.**

---

## 6. Buried Data List (reader exists, no realistic trigger)

These files have direct readers, but the system prompt / tool descriptions do not steer realistic user phrasing at them:

| File | Reader | Why buried |
|------|--------|------------|
| `joveo_2026_benchmarks.json` | (referenced only in `query_kb_deep` enum which is **broken**) | Tool broken; no working path. |
| `joveo_cpa_benchmarks_2026.json` | same | same |
| `industry_white_papers.json` | `_query_white_papers` | Tool description is generic; LLM rarely picks it over `query_recruitment_benchmarks`. |
| `supply_ecosystem_intelligence.json` | `_query_supply_ecosystem` | Tool name unclear to LLM; almost always loses to `query_publishers` or `query_global_supply`. |
| `seasonal_hiring_trends.json` | one conditional read in smart-defaults | Surfaces only as a side-effect of plan generation, not from direct user questions. |
| `adzuna_benchmarks.json` | one conditional read inside LinkUp fallback | Hidden inside another tool's fallback chain. |

---

## 7. Top-5 Wiring Fixes (Ranked by Effort vs Impact)

### Fix #1: Repair `_query_kb_deep` (1-line, P0, ~5 min)
**File:** `nova.py:7233`
**Change:** Replace `kb = self._kb or {}` with `kb = self._data_cache or {}`.
**Impact:** Instantly unblocks 17 datasets listed in the tool's enum (`nova.py:7211-7230`) — including `joveo_2026_benchmarks`, `joveo_cpa_benchmarks`, `labor_market_outlook`, `industry_hiring_patterns`, `salary_benchmarks_detailed`, `h1b_salary_intelligence`, `linkedin_benchmarks`, `top_employers_by_city`, `seasonal_hiring_trends`, `job_density_metros`, `competitor_careers`. This single line resolves §4.1 entirely and partly resolves §4.2.

### Fix #2: Register `recruitment_benchmarks_comprehensive_2026.json` and `industry_reports_2026.json` in `KB_FILES` (P0, ~10 min)
**File:** `kb_loader.py:290` (append entries).
Add:
```
"recruitment_benchmarks_comprehensive": "recruitment_benchmarks_comprehensive_2026.json",
"industry_reports": "industry_reports_2026.json",
```
Then in `_query_kb_deep` add them to the dataset enum so the LLM knows they exist. **Impact:** Fixes §4.3 and §4.4. The 28-source comprehensive benchmark + the F2 agent's industry reports become reachable via `query_kb_deep`.

### Fix #3: Add direct readers for healthcare supply set (P1, ~2 hours)
**Files:** `nova.py` — extend `_query_publishers` (`nova.py:7048-7058`) and `_query_global_supply` (`nova.py:6914`) to also consult `healthcare_supply_map_us`, `partner_specialty_crosswalk`, and `category_to_partners` when query mentions healthcare/nursing/clinical/specialty keywords. Add new specialized tool `query_healthcare_supply(specialty, location)` mapping to `_query_publishers` with healthcare branch. **Impact:** Unlocks ~667KB of S52 curated mapping. Direct improvement to healthcare account quality (J&R Schugel, CareFirst flows).

### Fix #4: Surface `recruitment_benchmarks_2026_deep` and `employer_career_intelligence_2026` (P1, ~1 hour)
**File:** `nova.py:9667` — extend `_query_recruitment_benchmarks` to merge from `recruitment_benchmarks_2026_deep` for finer metro × vertical breakdowns. Add a new key in `_query_employer_branding` (`nova.py:9820`) to read `employer_career_intelligence_2026`. **Impact:** Unlocks the S54 "Web-researched authoritative benchmark KBs" (47 distinct sources covering 12 verticals × 40 US metros × 7 channels) that were explicitly marketed as "Reusable across Nova chat" but are not.

### Fix #5: Promote `query_kb_deep` in the system prompt + add explicit fallback rule (P1, ~30 min)
**File:** `nova.py` system prompt block around `nova.py:4082`. Once Fix #1 lands, append: *"For any benchmark, labor outlook, or hiring-pattern question you cannot answer with a more specific tool, call `query_kb_deep(dataset=<best_match>, search_term=<keywords>)` BEFORE falling back to web search or general knowledge."* **Impact:** Without this, even a fixed `query_kb_deep` will be rarely chosen because more specific tools exist; we need to nudge the LLM. Pairs with Fix #1.

---

## 8. Quick-Look Inventory of What the Chatbot Actually Reaches Today

**Tier-1 fully reachable (covered with confidence):**
- Industry knowledge core (`recruitment_industry_knowledge.json`)
- Recruitment benchmarks (`recruitment_benchmarks_deep.json`)
- Global supply boards + DEI (`global_supply.json`, `joveo_global_supply_repository.json`)
- Joveo publisher network (`joveo_publishers.json`, `channels_db.json`)
- Platform intelligence (`platform_intelligence_deep.json`)
- Regional hiring (`regional_hiring_intelligence.json`)
- Workforce trends (`workforce_trends_intelligence.json`)
- Client media plans (`client_media_plans_kb.json` + `client_plans/*.json`)
- Google Ads benchmarks (`google_ads_2025_benchmarks.json`)
- External benchmarks (`external_benchmarks_2025.json`)
- International benchmarks (`international_benchmarks_2026.json`)
- LinkedIn-Guidewire (`linkedin_guidewire_data.json`)

**Tier-2 RAG-only (low confidence — only via embedding match):**
- Most 2026 research files (channel_benchmarks_live, ad_benchmarks_recruitment, top_employers_by_city, compliance_regulations, agency_rpo_market, hr_tech_landscape, publisher_benchmarks, recruitment_marketing_trends, fred_indicators, google_trends, job_density_metros, job_posting_volumes, live_market_data, market_trends_live, platform_ad_specs)
- `recruitment_benchmarks_comprehensive_2026.json` (the headline 28-source compilation)
- `linkedin_performance_benchmarks.json`
- `h1b_salary_intelligence.json`

**Tier-3 broken / stranded (zero confidence):**
- All 17 datasets enumerated by the broken `query_kb_deep` (Fix #1 resolves this)
- Healthcare supply mapping set (S52, 667KB)
- S54 deep-research KBs (84KB)
- `industry_reports_2026.json` (110KB)

---

## 9. Bottom Line

The user's instinct is right. The brain layer is approximately **37% reachable with high confidence, 35% reachable only via RAG (probabilistic), and ~28% effectively dark.** The biggest contributor to the dark zone is a single-line bug in `_query_kb_deep` plus the absence of explicit readers for the S52/S54 curated assets. Fix #1 alone takes the high-confidence reachability from ~37% to ~55% in five minutes. The remaining fixes are progressively more involved but each unlocks specific known-stranded assets the team paid to build.

**No code or data was modified during this audit.**
