# Data Source Usage Audit — Nova Chatbot

**Date:** 2026-05-22
**Scope:** Verify which `data/*.json` files are ACTUALLY consumed by the running Nova chat path vs. loaded-into-memory-only.
**Method:** Read-only static analysis. `nova.py` was not modified.

---

## TL;DR

| Bucket | Count | Notes |
|---|---|---|
| Files on disk in `data/` | 65 JSON | Plus several non-recruitment operational files (audit_log, cache/, errors/). |
| Files registered in `kb_loader.KB_FILES` | 54 | These get loaded into the shared `_data_cache` singleton at startup. |
| Files **actively read** by chat handlers | 24 | "Active": at least one `self._data_cache.get(key)` or `kb.get(key)` on a chat path. |
| **STRANDED**: loaded but ZERO chat readers | **17** | Includes 6 of the 7 S52/S54 healthcare/employer files — high-value, never touched. |
| **MISSING**: code references with no registration | **3** | `industry_reports_2026.json`, `ta_leaders_curated_2026.json`, several `query_kb_deep` datasets that hit broken `self._kb`. |
| **CRITICAL BUG**: `_query_kb_deep` accesses `self._kb` which is never assigned | **1** | Lines 7189–7335. Renders 32-dataset dispatcher inert. |

**Headline answer to user's seed question:** The `international_benchmarks` fix is REAL and CORRECT. Six chat handlers now route through `_intl_country_data` (which reads `self._data_cache["international_benchmarks"]`). But the broader stranded-data problem is worse than the example suggested — at least 17 other loaded files have zero chat readers, and the `query_kb_deep` tool that was designed as the catch-all dispatcher is broken.

---

## 1. Loader registry (source of truth)

`kb_loader.py:211-290` defines `KB_FILES` — a `dict[cache_key, filename]` of 54 entries. `load_knowledge_base()` (line 518) opens every file, stores it under its key in a singleton `TrackedDict`, and exposes it as `self._data_cache` on Nova via `nova.py:3914`.

Key alias map (`KB_ALIASES`, line 301) — `"knowledge_base" → "core"` and `"expanded_supply_repo" → "global_supply_repository"`. This is correct and explains why `self._data_cache.get("knowledge_base")` works without a literal entry.

---

## 2. Reader census

I grepped every `*.py` for `self._data_cache.get(...)`, `self._data_cache[...]`, and `kb.get(...)` where `kb` is a load_knowledge_base() result. Unique chat-read keys found in `nova.py`:

```
adzuna_benchmarks               channels_db                 client_media_plans
craigslist_benchmarks           expanded_supply_repo        external_benchmarks
global_supply                   google_ads_benchmarks       international_benchmarks
international_sources           joveo_publishers            knowledge_base (= core)
linkedin_guidewire              platform_intelligence       recruitment_benchmarks
recruitment_strategy            regional_hiring             seasonal_hiring_trends
supply_ecosystem                white_papers                workforce_trends
healthcare_supply_map_us
```

24 keys total. Bookkeeping keys (`_nova_client_plans_merged`, `_freshness_warnings`) excluded.

---

## 3. Usage matrix (recruitment-relevant files only)

| File | Cache key | Hot path? | Last modified | Chat readers | Status |
|---|---|---|---|---|---|
| `recruitment_industry_knowledge.json` (114KB) | `core` / `knowledge_base` | YES (every chat) | Mar 22 | 9 handlers incl. `_query_market_demand`, `_query_budget_projection`, `_query_ad_platform`, `_query_collar_strategy`, `_query_knowledge_base` (lines 7450, 8159, 8196, 8474, 8701, 9271, 10530) | ACTIVE |
| `international_benchmarks_2026.json` (240KB, 38 countries) | `international_benchmarks` | Mixed | Apr 8 | 6 chat paths via `_intl_country_data` helper (nova.py:8095): `_query_knowledge_base` (7593), `_query_market_demand` (8110, 8478), `_query_ad_platform` (8680), `_query_collar_strategy` (8929), `_query_recruitment_benchmarks` (9621) | ACTIVE (fix confirmed) |
| `joveo_publishers.json` (94KB) | `joveo_publishers` | YES | Apr 3 | 5 readers (4072, 7000, 8660, 16182, …) | ACTIVE |
| `channels_db.json` (57KB) | `channels_db` | YES | Apr 7 | 3 readers (6916, 7001, 16183) | ACTIVE |
| `joveo_global_supply_repository.json` (2.8MB) | `expanded_supply_repo` (alias) / `global_supply_repository` | YES (supply queries) | Apr 3 | 3 readers (6820, 7050, 16184) | ACTIVE |
| `global_supply.json` (105KB) | `global_supply` | YES | Mar 22 | 2 readers (6819, 8649) | ACTIVE |
| `platform_intelligence_deep.json` (105KB) | `platform_intelligence` | YES | Mar 22 | 2 chat readers (6955, 9558) + data_synthesizer | ACTIVE |
| `recruitment_benchmarks_deep.json` (69KB) | `recruitment_benchmarks` | YES (industry questions) | Mar 22 | 1 chat reader (9667) + data_synthesizer (773, 831) | ACTIVE |
| `recruitment_strategy_intelligence.json` (95KB) | `recruitment_strategy` | YES | Mar 22 | 1 chat reader (9820) + data_synthesizer | ACTIVE |
| `regional_hiring_intelligence.json` (141KB) | `regional_hiring` | YES | Mar 22 | 1 chat reader (9851) + data_synthesizer | ACTIVE |
| `supply_ecosystem_intelligence.json` (100KB) | `supply_ecosystem` | YES | Mar 22 | 1 chat reader (9942) + data_synthesizer | ACTIVE |
| `workforce_trends_intelligence.json` (78KB) | `workforce_trends` | YES | Mar 22 | 1 chat reader (9976) + data_synthesizer | ACTIVE |
| `industry_white_papers.json` (114KB) | `white_papers` | YES (Appcast enrich) | Mar 22 | 3 readers (9704, 9704 incl. Appcast 2026 enrichment) + data_synthesizer | ACTIVE |
| `client_media_plans_kb.json` (84KB) | `client_media_plans` | YES (client_plans tool) | Mar 26 | 2 readers (3959, 10400) | ACTIVE |
| `external_benchmarks_2025.json` (57KB) | `external_benchmarks` | Rare | Mar 26 | 2 readers (9789, 10261) | ACTIVE |
| `google_ads_2025_benchmarks.json` (14KB) | `google_ads_benchmarks` | YES (Google Ads questions) | Mar 26 | 2 readers (9757, 10112) | ACTIVE |
| `international_sources.json` (7KB) | `international_sources` | Rare | Mar 26 | 1 reader (7575) | ACTIVE |
| `seasonal_hiring_trends.json` (2.7KB) | `seasonal_hiring_trends` | Conditional (system prompt) | Apr 7 | 1 reader (4404 — system prompt builder) | ACTIVE |
| `linkedin_guidewire_data.json` (1.2KB) | `linkedin_guidewire` | Rare (Guidewire client only) | Mar 26 | 2 readers (9483, 20594) | ACTIVE |
| `craigslist_performance_benchmarks.json` (15KB) | `craigslist_benchmarks` | Rare (CG questions) | Apr 8 | 1 reader (7629) | ACTIVE |
| `adzuna_benchmarks.json` (7KB) | `adzuna_benchmarks` | Rare | Apr 3 | 1 reader (12803) | ACTIVE |
| `healthcare_supply_map_us.json` (190KB, 350 partners) | `healthcare_supply_map_us` | Fast-path | Apr 24 | 1 fast-path reader (15910/15915) | ACTIVE |

### Stranded files (LOADED but ZERO chat readers)

| File | Cache key | Size | Last modified | Status |
|---|---|---|---|---|
| `recruitment_benchmarks_2026_deep.json` | `recruitment_benchmarks_2026_deep` | 38KB | Apr 24 | **STRANDED** |
| `employer_career_intelligence_2026.json` | `employer_career_intelligence_2026` | 48KB | Apr 24 | **STRANDED** |
| `healthcare_specialty_pay_2026.json` | `healthcare_specialty_pay_2026` | 33KB | Apr 24 | **STRANDED** |
| `partner_specialty_crosswalk.json` | `partner_specialty_crosswalk` | 187KB | Apr 24 | **STRANDED** |
| `partner_url_registry.json` | `partner_url_registry` | 146KB | Apr 24 | **STRANDED** |
| `category_to_partners.json` | `category_to_partners` | 130KB | Apr 24 | **STRANDED** |
| `hr_tech_landscape_2026.json` | `hr_tech_landscape` | 10KB | Mar 26 | STRANDED (only referenced as dataset name in broken `_query_kb_deep`) |
| `publisher_benchmarks_2026.json` | `publisher_benchmarks` | 6KB | Mar 26 | STRANDED |
| `recruitment_marketing_trends_2026.json` | `recruitment_marketing_trends` | 5KB | Mar 26 | STRANDED |
| `labor_market_outlook_2026.json` | `labor_market_outlook` | 5KB | Mar 26 | STRANDED |
| `salary_benchmarks_detailed_2026.json` | `salary_benchmarks_detailed` | 14KB | Mar 26 | STRANDED |
| `ad_benchmarks_recruitment_2026.json` | `ad_benchmarks_recruitment` | 6KB | Mar 26 | STRANDED |
| `industry_hiring_patterns_2026.json` | `industry_hiring_patterns` | 8KB | Mar 26 | STRANDED |
| `top_employers_by_city_2026.json` | `top_employers_by_city` | 11KB | Mar 26 | STRANDED |
| `compliance_regulations_2026.json` | `compliance_regulations` | 9KB | Mar 26 | STRANDED |
| `agency_rpo_market_2026.json` | `agency_rpo_market` | 9KB | Mar 26 | STRANDED |
| `joveo_2026_benchmarks.json` | `joveo_2026_benchmarks` | 11KB | Mar 22 | STRANDED (referenced only in system-prompt text and broken `_query_kb_deep`) |
| `joveo_cpa_benchmarks_2026.json` | `joveo_cpa_benchmarks` | 39KB | Apr 1 | STRANDED in chat (used in budget/ppt paths, indexed in vector_search) |
| `linkedin_performance_benchmarks.json` | `linkedin_benchmarks` | 87KB | Apr 4 | STRANDED in chat (used in slotops_engine, indexed in vector_search) |
| `rtx_*` (2 client_plans files) | `rtx_media_plan`, `rtx_aerospace_benchmarks` | — | various | Merged into `client_media_plans.plans` (3931) — usable via existing tool |

> Note: "Stranded in chat" means no direct `_data_cache.get(...)` from a chat handler. Some are read by ppt/excel generation, vector indexing, or supabase seeding — those are valid uses, but they don't make the chatbot smarter.

### Missing files (data on disk, NOT in registry, possibly referenced by code)

| File | Last modified | Size | Detected use |
|---|---|---|---|
| `industry_reports_2026.json` | **May 22, 2026 (today)** | 109KB | Newly created; not in `kb_loader.KB_FILES` nor `vector_search.kb_files`. ZERO `*.py` references. |
| `ta_leaders_curated_2026.json` | **May 22, 2026 (today)** | 72KB | Same — orphaned at birth. ZERO `*.py` references. |
| `recruitment_benchmarks_comprehensive_2026.json` | Apr 7 | 43KB | Read directly off disk by `app.py:5145`, `data_synthesizer.py:4661`, `roi_projector.py:84`, `vector_search.py:1683`, `supabase_data.py:182`. NOT registered in `kb_loader`. |

### Other non-recruitment data (correctly excluded from chat reads)

`audit_log.jsonl`, `request_log.json`, `enrichment_state.json`, `benchmark_drift_results.json`, `auto_qc_results.json`, `slotops_baseline_data.json` (7.4MB SlotOps source), `slotops_benchmarks_summary.json`, `linkedin_industry_benchmarks.json` (read by `slotops_engine.py`), `so_survey_2025_aggregates.json` (Stack Overflow survey), `linkedin_guidewire_data.json` (1.2KB, read in chat), `seed_*.json` (5 files, used for one-time Supabase seeding — see MEMORY.md), `nova_learned_answers.json` (read off disk by nova.py:3489, not via kb_loader).

These are correctly outside chat. Mentioned only so the audit is comprehensive.

---

## 4. The `_query_kb_deep` bug — root cause of half the stranding

`nova.py:7189-7335` defines `_query_kb_deep(self, params)` as the catch-all dispatcher for 18+ KB datasets including `hr_tech_landscape`, `labor_market_outlook`, `top_employers_by_city`, `salary_benchmarks_detailed`, `h1b_salary_intelligence`, `agency_rpo_market`, `joveo_cpa_benchmarks`, `linkedin_benchmarks`, `compliance_regulations`, `industry_hiring_patterns`, `competitor_careers`, `job_density_metros`, `seasonal_hiring_trends`, `joveo_2026_benchmarks`, etc.

Line 7233:

```python
kb = self._kb or {}
```

But `self._kb` is **never assigned** anywhere in `nova.py`. `__init__` (line 3860) and `_load_data_sources` (line 3886) only set `self._data_cache`. `grep -n "self\._kb\s*=" nova.py` returns zero matches.

**Effect:** Every call to `query_kb_deep` returns `{"error": "Dataset 'X' not found in knowledge base"}`. The 18 datasets it claims to expose are unreachable from the chat planner.

This single bug explains why so many richly-populated files appear stranded — they were SUPPOSED to be reachable via `query_kb_deep`, but the tool has been silently failing.

---

## 5. Categorization

### CRITICAL (working hot-path)
`core`, `joveo_publishers`, `channels_db`, `expanded_supply_repo`, `platform_intelligence`, `recruitment_benchmarks`, `white_papers`, `client_media_plans`, `international_benchmarks` (6 readers — fix CONFIRMED), `global_supply`, `healthcare_supply_map_us`. Roughly 11 files representing ~70% of chat-relevant intelligence.

### UNDERUSED (loaded, only 1 reader)
`craigslist_benchmarks`, `adzuna_benchmarks`, `seasonal_hiring_trends`, `external_benchmarks`, `google_ads_benchmarks` (only 2 readers, both light), `linkedin_guidewire` (Guidewire-only). These are fine — domain is narrow so 1 reader is appropriate.

### STRANDED (high-value, 0 chat readers — see table above)
17 files, ~750KB combined. The S52/S54 healthcare & employer files (Apr 24, fresh) alone represent 580KB of curated research that the chatbot can't touch. The 10 "2026 research" files from Mar 26 are smaller but still represent ~70KB of structured intelligence.

### STALE (read but data > 8 weeks old)
None among ACTIVE files. Most active files (`*intelligence.json`) are dated Mar 22, 2026 — exactly 8 weeks 5 days old as of May 22. They will start tripping the 90-day freshness threshold in `kb_loader._check_file_freshness_at_startup` (line 399) in ~2 weeks. Action: refresh the `*intelligence.json` set OR raise the threshold to 120 days.

### MISSING (registered nowhere, possibly meant to be loaded)
- `industry_reports_2026.json` (109KB, **created TODAY**) — register in `KB_FILES`.
- `ta_leaders_curated_2026.json` (72KB, **created TODAY**) — register or delete.
- `recruitment_benchmarks_comprehensive_2026.json` (43KB) — read 4x off disk by different modules without caching. Register in `KB_FILES` so it lives in memory once instead of being re-loaded.

---

## 6. Top-5 highest-value "wire it up" recommendations

### #1 — Fix `_query_kb_deep` to use `self._data_cache` (UNBLOCKS 18 DATASETS)
**File:** `nova.py:7233`
**Change:** `kb = self._kb or {}` → `kb = self._data_cache or {}`
**LoC:** 1 line.
**Impact:** Unblocks `hr_tech_landscape`, `labor_market_outlook`, `salary_benchmarks_detailed`, `h1b_salary_intelligence`, `top_employers_by_city`, `agency_rpo_market`, `compliance_regulations`, `industry_hiring_patterns`, `joveo_2026_benchmarks`, `joveo_cpa_benchmarks` (304 CPA categories!), `linkedin_benchmarks`, `competitor_careers`, `job_density_metros`. This is the single highest-ROI fix — one character changes 13+ datasets from stranded to active.
**Risk:** Trivial. `self._data_cache` is the shared KB dict; existing aliases mean both names point to the same data.

### #2 — Wire `healthcare_specialty_pay_2026` + `partner_specialty_crosswalk` + `category_to_partners` into `_query_recruitment_benchmarks` and `_query_supply_repository`
**Files:** `nova.py:9600` (`_query_recruitment_benchmarks`) and the healthcare fast-path at `nova.py:15896`.
**Change:** When industry == "healthcare" or specialty match, read `self._data_cache.get("healthcare_specialty_pay_2026")` and merge into the response under a new `specialty_pay` field; when listing partners, prefer `self._data_cache.get("category_to_partners")` (130KB pre-indexed) over the slow scan in `joveo_global_supply_repository`.
**LoC:** ~40 lines (3 new helper functions + 3 call sites).
**Impact:** 410KB of healthcare-specific data (350 partners, 49 specialty roles, URL registry) becomes queryable. Resolves the user's "Nova matches Claude.ai chat-quality for healthcare partner lookups" goal stated in `kb_loader.py:266`.

### #3 — Register and read `industry_reports_2026.json` + `ta_leaders_curated_2026.json` (TODAY's files)
**Files:** `kb_loader.py:290` (append to `KB_FILES`), plus add 2 keyword triggers in `_query_knowledge_base` (`nova.py:7411`).
**Change:**
```python
# kb_loader.py KB_FILES additions
"industry_reports_2026": "industry_reports_2026.json",
"ta_leaders_curated_2026": "ta_leaders_curated_2026.json",
```
Plus a small reader block: when user query mentions "report" / "industry report" / "TA leaders", surface highlights from these.
**LoC:** ~30 lines (2 KB_FILES entries + 2 keyword-driven readers).
**Impact:** 181KB of brand-new May 22 data is currently invisible to the chatbot the day it ships. Either wire it or delete it.

### #4 — Promote `employer_career_intelligence_2026` into `_query_market_demand` and `_query_collar_strategy`
**File:** `nova.py:8129` (`_query_market_demand`), `8903` (`_query_collar_strategy`).
**Change:** When `params["company"]` or company name detected, look up `self._data_cache.get("employer_career_intelligence_2026", {}).get(company.lower())` and attach `top_employer_signal: {careers_url, ats, hiring_volume, careers_focus}` to the response. The user's MEMORY notes "100 top employers" — that's a high-traffic question pattern.
**LoC:** ~25 lines (1 helper + 2 integration points).
**Impact:** Adds employer-level intelligence to two of the busiest chat handlers. Currently any "tell me about X company's hiring" question falls through to LLM hallucination.

### #5 — Replace `_query_ad_platform` and `_query_recruitment_benchmarks` "white_papers" lookups with `ad_benchmarks_recruitment_2026` first
**File:** `nova.py:8667` (`_query_ad_platform`), `9600` (`_query_recruitment_benchmarks`).
**Change:** Prefer `self._data_cache.get("ad_benchmarks_recruitment", {})` (6KB, Mar 26) for CPA/CPC/CPH benchmarks — it's the fresher 2026 dataset. Fall through to `white_papers.appcast_benchmark_2026` (current path) only if the 2026 file doesn't have the requested industry/occupation. Same logic for `salary_benchmarks_detailed_2026` over `recruitment_industry_knowledge.benchmarks` (which is from Mar 22).
**LoC:** ~20 lines (2 call sites, simple if-else).
**Impact:** Replaces sometimes-conflicting "8-week-old industry KB" with curated 2026 data on hot paths. Combined with #1 (which exposes the same data via `query_kb_deep`) this gives the planner both a direct tool path and a structured-query path to the same fresh benchmarks.

---

## Appendix A — Verification commands

To reproduce my findings without modifying nova.py:

```bash
# All cache-key reads in nova.py
grep -ohE "_data_cache(\[|\.get\()[\"'][^\"']+[\"']" nova.py | sort -u

# Confirm self._kb is never assigned
grep -nE "self\._kb\s*=" nova.py    # zero matches -> bug confirmed

# Confirm stranded S52/S54 keys have no readers
grep -rn "partner_specialty_crosswalk\|partner_url_registry\|category_to_partners\|recruitment_benchmarks_2026_deep\|healthcare_specialty_pay_2026\|employer_career_intelligence_2026" *.py | grep -v "kb_loader.py"
# zero matches -> stranded confirmed

# Confirm industry_reports_2026 / ta_leaders_curated are orphans
grep -rn "industry_reports_2026\|ta_leaders_curated" *.py    # zero matches
```

## Appendix B — Files I trust vs. don't

- **High-confidence ACTIVE list (11 files):** Confirmed via direct line-numbered grep hits on chat handlers.
- **High-confidence STRANDED list (17 files):** Confirmed via `grep -rn ... *.py` returning ZERO hits across the whole codebase outside `kb_loader.py` and (sometimes) `vector_search.py`. Vector indexing makes content semantically searchable via Chroma but doesn't expose structured data to the tool layer — different use case.
- **`_query_kb_deep` bug:** Verified by reading lines 7189-7335 and confirming `self._kb` has no assignment anywhere in nova.py. Confidence: high.

---

**End of audit.**
