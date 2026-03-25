# Tier 4 Audit Quality Bar Specification Index
## Complete Documentation Set

**Created**: 2026-03-26
**Project**: Media Plan Generator — Comprehensive Audit Quality Enhancement
**Status**: SPECIFICATION COMPLETE (Implementation NOT included)

---

## DOCUMENTS IN THIS SPECIFICATION SET

### 1. SPEC_EXECUTIVE_SUMMARY.md (START HERE)
**Length**: 5 pages | **Audience**: Executives, Product Managers
**Purpose**: High-level overview of the opportunity, timeline, and business impact

**Key sections**:
- The opportunity (Tier 1 → Tier 4 gap)
- Current state vs. gold standard (side-by-side comparison)
- What needs to change (5 areas)
- Implementation timeline (4-6 weeks)
- Business impact & success metrics
- Questions for alignment

**When to read**: First — understand the big picture
**Output**: Decision to proceed with implementation

---

### 2. SPEC_AUDIT_QUALITY_BAR.md (MAIN SPECIFICATION)
**Length**: 25 pages | **Audience**: Technical leads, architects
**Purpose**: Comprehensive gap analysis and detailed specification

**Key sections**:
- Current state analysis (data sources, synthesis, output schema)
- Gold standard specification (detailed tier 4 structure)
- Required data sources (5 new APIs)
- Synthesis pipeline changes (4 new functions)
- Output JSON schema (Tier 4 structure with all 10+ keys)
- LLM prompt changes (system prompt upgrade)
- Template rendering changes (Excel 8 sheets, dashboard 4 sections)
- Implementation checklist (line-by-line tasks)
- Data quality & confidence scoring
- Testing & validation plan
- Risks & mitigation

**When to read**: After executive summary — architect the full solution
**Output**: Detailed specification for implementation teams

---

### 3. SPEC_DATA_CONTRACTS_TIER4.md (TECHNICAL DEEP DIVE)
**Length**: 18 pages | **Audience**: Backend engineers, data engineers
**Purpose**: Function signatures, data contracts, and API specifications

**Key sections**:
- City-level salary bands (API calls, tier classification, caching)
- Hiring volume by difficulty tier (LinkedIn/Adzuna integration)
- Security clearance segmentation (keyword detection, industry baseline)
- Competitor salary premium mapping (Glassdoor/LinkedIn scraping)
- Activation events calendar (FRED, conference lookup, milestones)
- Integration with existing pipeline (data flow diagram)
- Caching strategy (L1/L2/L3, TTLs per data source)
- Error handling & graceful degradation

**When to read**: During Phase 1 (API enrichment) — implement these functions
**Output**: Code implementation guidance for each new function

---

### 4. SPEC_IMPLEMENTATION_ROADMAP.md (TACTICAL EXECUTION PLAN)
**Length**: 15 pages | **Audience**: Engineering leads, sprint planners
**Purpose**: Detailed breakdown of code changes, phases, and effort estimates

**Key sections**:
- File-by-file modification checklist (api_enrichment, data_synthesizer, app, excel_v2, templates)
- Per-file line counts and effort estimates (~200 hours total)
- 5 implementation phases (Week 1-6)
  - Phase 1: API enrichment (Week 1)
  - Phase 2: Synthesis layer (Week 2)
  - Phase 3: Plan generation (Week 3)
  - Phase 4: Excel & dashboard (Week 3-4)
  - Phase 5: QA & rollout (Week 5-6)
- Parallel work streams (independence diagram)
- Testing strategy (30 unit + 10 integration + 5 E2E tests)
- Deployment strategy (feature flag, gradual rollout)
- Success metrics (technical, user, financial)

**When to read**: During sprint planning — assign tasks and track progress
**Output**: Sprint backlog, task assignments, timeline

---

## HOW TO USE THIS SPECIFICATION

### For Executives / Product Managers
1. Read **SPEC_EXECUTIVE_SUMMARY.md** (5 min read)
2. Review the "Business Impact" section
3. Decide: Proceed with 4-6 week implementation?
4. If yes → assign engineers and resources

### For Technical Leads / Architects
1. Read **SPEC_EXECUTIVE_SUMMARY.md** (understand scope)
2. Read **SPEC_AUDIT_QUALITY_BAR.md** (full specification)
3. Review the "Implementation Changes Checklist" section
4. Create architecture diagram from "File-by-File Modification Checklist"
5. Identify dependencies and parallel work streams
6. Assign to Phase 1 lead (API enrichment)

### For Backend Engineers (Phase 1: API Enrichment)
1. Read **SPEC_DATA_CONTRACTS_TIER4.md** sections 1-5 (new functions)
2. Read **SPEC_IMPLEMENTATION_ROADMAP.md** → "File 1: api_enrichment.py"
3. Implement 5 new functions:
   - `_fetch_salary_by_city()`
   - `_fetch_hiring_volume_by_tier()`
   - `_fetch_security_clearance_data()`
   - `_fetch_competitor_salary_premium()`
   - `_fetch_activation_events()`
4. Write unit tests (15 tests total)
5. Modify `enrich_data()` to call 5 new functions

### For Data Science / Synthesis Engineers (Phase 2: Synthesis)
1. Wait for Phase 1 to complete (API enrichment)
2. Read **SPEC_AUDIT_QUALITY_BAR.md** → "Synthesis Pipeline Changes"
3. Read **SPEC_IMPLEMENTATION_ROADMAP.md** → "File 2: data_synthesizer.py"
4. Implement 4 new synthesis functions:
   - `synthesize_city_level_supply_demand()`
   - `synthesize_security_clearance()`
   - `synthesize_competitor_map()`
   - `synthesize_activation_events()`
5. Implement helper: `_calculate_difficulty_tier_detailed()`
6. Write unit tests (19 tests total)

### For Backend Engineers (Phase 3: Plan Generation)
1. Wait for Phase 2 to complete (synthesis)
2. Read **SPEC_AUDIT_QUALITY_BAR.md** → "LLM Prompt Changes"
3. Read **SPEC_IMPLEMENTATION_ROADMAP.md** → "File 3: app.py"
4. Update LLM prompt to Tier 4 spec
5. Rewrite `_extract_plan_json()` for new schema
6. Implement `_verify_plan_data_tier4()` validation
7. Write integration tests (5 tests)

### For Frontend Engineers (Phase 4: Excel & Dashboard)
1. Wait for Phase 3 to complete (plan generation)
2. Excel:
   - Read **SPEC_IMPLEMENTATION_ROADMAP.md** → "File 4: excel_v2.py"
   - Implement 4 new sheet functions (~500 lines each)
   - Modify `generate_excel_v2()` to call new sheets
3. Dashboard:
   - Read **SPEC_IMPLEMENTATION_ROADMAP.md** → "Files 5-6: templates/"
   - Add 4 new sections to index.html (~500 lines)
   - Add 4 new sections to dashboard.html (~500 lines)
4. Write visual tests (8 tests)

---

## KEY FACTS TO REMEMBER

### The Gap
- **Current (Tier 1)**: National-level channel recommendations (LinkedIn, Indeed, DSP)
- **Gold Standard (Tier 4)**: City-level supply-demand + security clearance + competitor salary analysis + seasonal calendar

### The Fix
- **Add 5 new APIs**: City salary, hiring by tier, clearance data, competitor premiums, activation events
- **Add 4 synthesis functions**: Combine APIs into city-level, clearance, competitor, and event insights
- **Expand output**: 4 keys → 10+ keys (add city data, clearance, competitors, events, audit metadata)
- **Expand Excel**: 5 sheets → 8 sheets
- **Expand Dashboard**: 3 sections → 7 sections

### The Timeline
- **Phase 1 (API)**: Week 1 (~40 hours)
- **Phase 2 (Synthesis)**: Week 2 (~50 hours)
- **Phase 3 (Plan gen)**: Week 3 (~30 hours)
- **Phase 4 (UI)**: Week 3-4 (~45 hours)
- **Phase 5 (QA)**: Week 5-6 (~35 hours)
- **Total**: 4-6 weeks, ~200 hours, 1-2 engineers

### The Impact
- **User quality**: 50% → 75% rate plans "very detailed" (+25%)
- **Excel adoption**: 60% → 80% download Excel (+20%)
- **NPS improvement**: +10 points
- **Premium pricing**: Viable for Tier 4 plans (+20-30%)

### The Risks
- City-level APIs unavailable → graceful fallback to national
- LLM prompt inconsistent → strict schema validation
- Performance impact → parallel execution + caching
- Scraping blocked → fallback to industry baseline data

---

## SPECIFICATION STATISTICS

| Metric | Value |
|--------|-------|
| Total lines of specification | ~57,000 words |
| Code changes required | ~10,000 lines (new/modified) |
| New API functions | 5 |
| New synthesis functions | 4 |
| New Excel sheets | 3 |
| New dashboard sections | 4 |
| New unit tests | 30 |
| New integration tests | 10 |
| New E2E tests | 5 |
| Total testing coverage | 45+ tests |
| Implementation phases | 5 |
| Engineering effort | ~200 hours |
| Timeline | 4-6 weeks |
| Recommended team size | 1-2 engineers |

---

## SUCCESS CHECKLIST

### Before Implementation Starts
- [ ] Executives have reviewed and approved SPEC_EXECUTIVE_SUMMARY.md
- [ ] Technical leads have reviewed SPEC_AUDIT_QUALITY_BAR.md
- [ ] Engineers have reviewed SPEC_DATA_CONTRACTS_TIER4.md
- [ ] Sprint planning done based on SPEC_IMPLEMENTATION_ROADMAP.md
- [ ] Resources allocated (1-2 engineers for 4-6 weeks)

### Phase 1 Complete (End of Week 1)
- [ ] All 5 API enrichment functions implemented
- [ ] 15 unit tests passing
- [ ] Data contracts match specification
- [ ] Caching working (L1/L2/L3)
- [ ] Error handling for API unavailability

### Phase 2 Complete (End of Week 2)
- [ ] All 4 synthesis functions implemented
- [ ] 19 unit tests passing
- [ ] City-level supply-demand working
- [ ] Clearance segmentation working
- [ ] Competitor mapping working
- [ ] Activation events working
- [ ] Confidence score calculation working

### Phase 3 Complete (End of Week 3)
- [ ] LLM prompt updated to Tier 4 spec
- [ ] `_extract_plan_json()` rewritten for new schema
- [ ] Schema validation implemented
- [ ] 5 integration tests passing
- [ ] End-to-end plan generation < 45s

### Phase 4 Complete (End of Week 4)
- [ ] 4 new Excel sheets implemented and tested
- [ ] 4 new dashboard sections implemented and tested
- [ ] Visual design matches existing brand
- [ ] All data displays correctly
- [ ] No layout issues on mobile/desktop

### Phase 5 Complete (End of Week 6)
- [ ] Load testing passed (10x concurrent requests)
- [ ] Gold standard validation against shared plan
- [ ] Feature flag working (ENABLE_TIER4_PLANS)
- [ ] A/B testing setup complete
- [ ] Monitoring dashboards in place

---

## QUICK REFERENCE: WHAT CHANGES

### api_enrichment.py
```
BEFORE: 25 API sources, enriched dict has 30+ keys
AFTER:  25 + 5 new API sources, enriched dict has 35+ keys
CHANGE: +2,000 lines (5 new functions)
```

### data_synthesizer.py
```
BEFORE: Synthesize salary, employment, competition, demand
AFTER:  + city-level supply-demand, clearance, competitors, events
CHANGE: +3,000 lines (4 new functions, 1 helper)
```

### app.py
```
BEFORE: Extract plan JSON with 4 keys
AFTER:  Extract plan JSON with 10+ keys (Tier 4 schema)
CHANGE: +950 lines (rewrite _extract_plan_json, add validation)
```

### excel_v2.py
```
BEFORE: 5 sheets (summary, channels, market, sources, roi)
AFTER:  8 sheets (+ city, clearance, competitors, activation)
CHANGE: +2,000 lines (4 new sheet functions)
```

### templates/index.html & dashboard.html
```
BEFORE: 3 main sections (plan overview, channels, budget)
AFTER:  7 sections (+ city-level, clearance, competitor, activation)
CHANGE: +2,000 lines (4 new dashboard sections)
```

---

## APPENDIX: FILE LOCATIONS

All specification documents are saved in the media-plan-generator root directory:

```
/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/
├── SPEC_INDEX.md (this file)
├── SPEC_EXECUTIVE_SUMMARY.md (start here)
├── SPEC_AUDIT_QUALITY_BAR.md (main specification)
├── SPEC_DATA_CONTRACTS_TIER4.md (technical deep dive)
└── SPEC_IMPLEMENTATION_ROADMAP.md (tactical execution)
```

**To access**: Open any file with your IDE or text editor
**To share**: Include all 5 files in code review / engineering kickoff

---

## FINAL NOTES

This specification is **complete and implementation-ready**. All details, data contracts, function signatures, testing strategies, and rollout plans are documented.

**Next action**: Assign engineer to Phase 1 (API enrichment) and begin implementation per SPEC_IMPLEMENTATION_ROADMAP.md.

**Questions or clarifications?** Review the relevant specification document or reach out to the architecture team.

---

**Document**: SPEC_INDEX.md
**Created**: 2026-03-26
**Status**: Complete
**Ready for**: Engineering kickoff
