# Tier 4 Implementation Roadmap
## Quick reference guide for developers

**Document Created**: 2026-03-26
**Companion to**: SPEC_AUDIT_QUALITY_BAR.md, SPEC_DATA_CONTRACTS_TIER4.md

---

## QUICK SUMMARY

### Current State (Tier 1)
```
Input: 4 fields (roles, locations, budget, industry)
  ↓
Enrichment: 25 APIs (salary, demographics, competitors, trends)
  ↓
Synthesis: Single difficulty score (easy/moderate/hard)
  ↓
Output: 4 keys (channels, budget, insights, recs)
  ↓
Result: National-level, generic recommendations
```

### Gold Standard (Tier 4)
```
Input: Same 4 fields
  ↓
Enrichment: 25 APIs + 5 NEW (city salary, hiring tier, clearance, competitor premium, events)
  ↓
Synthesis: City-level supply-demand, per-tier difficulty, clearance segments, competitor maps
  ↓
Output: 10+ keys (channels, budget, city data, clearance, competitors, events, audit metadata)
  ↓
Result: City-level, CFO-ready, auditable plans
```

---

## FILE MODIFICATION CHECKLIST

### 1. api_enrichment.py (~13,500 lines)
**Add 5 new functions (~2,000 lines total)**:

```python
# NEW FUNCTION 1: City-level salary bands
def _fetch_salary_by_city(soc_codes: list[str], cities: list[str], tier_rules: dict = None) -> dict[str, dict[str, dict]]:
    """
    Fetch median, 25th, 75th percentile wages by city MSA.
    - Query BLS OES API for each SOC code + MSA
    - Classify percentiles into tiers (junior/mid/senior/staff)
    - Cache for 90 days

    Complexity: ~400 lines
    Dependencies: BLS API, standardizer.py, cache layer
    Tests: 3 unit tests (normal case, MSA not found, API timeout)
    """

# NEW FUNCTION 2: Hiring volume by difficulty tier
def _fetch_hiring_volume_by_tier(locations: list[str], roles: list[str], days_lookback: int = 30) -> dict[str, dict[str, Any]]:
    """
    Estimate hiring volume by difficulty tier using LinkedIn + Adzuna.
    - Query LinkedIn Marketing API (if token available)
    - Query Adzuna API for job postings
    - Classify postings into tiers using keyword heuristics
    - Calculate months_to_fill based on avg_days_open
    - Cache for 7 days

    Complexity: ~500 lines
    Dependencies: LinkedIn API, Adzuna API, keyword heuristics
    Tests: 4 unit tests (API available, API timeout, mixed data, all fallback)
    """

# NEW FUNCTION 3: Security clearance segmentation
def _fetch_security_clearance_data(locations: list[str], industry: str, company_verticals: list[str] = None) -> dict[str, Any]:
    """
    Estimate hiring breakdown by clearance level (secret/TS/other).
    - Query ClearedJobs.Net API or scrape job postings
    - Keyword detection (Secret, TS, TS/SCI)
    - Industry vertical assessment (aerospace/defense = higher clearance baseline)
    - Geographic risk scoring
    - Cache for 30 days

    Complexity: ~450 lines
    Dependencies: ClearedJobs API, keyword detection, industry mapping
    Tests: 3 unit tests (high clearance industry, low, API unavailable)
    """

# NEW FUNCTION 4: Competitor salary premium mapping
def _fetch_competitor_salary_premium(competitors: list[str], locations: list[str], roles: list[str], your_median_salary: dict = None) -> dict[str, list[dict]]:
    """
    Map competitor salary premiums per city.
    - Scrape Glassdoor for competitor salary data
    - Query LinkedIn for competitor job postings
    - Calculate salary premium (comp_median / market_median)
    - Infer preferred channels from job posting distribution
    - Cache for 14 days

    Complexity: ~400 lines
    Dependencies: Glassdoor scraping, LinkedIn API, salary data
    Tests: 3 unit tests (scrape available, scrape blocked, no competitors)
    """

# NEW FUNCTION 5: Activation events calendar
def _fetch_activation_events(industry: str, locations: list[str], roles: list[str], company_fiscal_year_q1_month: int = 1) -> dict[str, Any]:
    """
    Generate seasonal hiring lift + conference calendar + milestones.
    - Query FRED API for monthly hiring by industry (BLS JOLTS)
    - Lookup industry conferences from curated table
    - Extract company milestones from enriched data
    - Calculate recommended spend multipliers
    - Cache for 90 days

    Complexity: ~350 lines
    Dependencies: FRED API, industry conference lookup, company data
    Tests: 2 unit tests (data available, partial data)
    """

# MODIFY EXISTING FUNCTION: enrich_data()
def enrich_data(data: Dict[str, Any], request_id: str = "") -> Dict[str, Any]:
    """
    ADD 5 new tasks to concurrent execution:
    - ("city_salary_bands", "BLS-City", lambda: _fetch_salary_by_city(...))
    - ("hiring_by_tier", "LinkedIn-Adzuna", lambda: _fetch_hiring_volume_by_tier(...))
    - ("clearance_data", "ClearedJobs", lambda: _fetch_security_clearance_data(...))
    - ("competitor_premiums", "Glassdoor-LinkedIn", lambda: _fetch_competitor_salary_premium(...))
    - ("activation_events", "FRED-Conferences", lambda: _fetch_activation_events(...))

    ADD 5 new keys to enriched result dict:
    enriched["city_salary_bands"] = result from task 1
    enriched["hiring_by_tier"] = result from task 2
    enriched["clearance_data"] = result from task 3
    enriched["competitor_premiums"] = result from task 4
    enriched["activation_events"] = result from task 5

    Lines changed: ~150 (new tasks + keys)
    """
```

**Subtotal**: ~2,000 lines added
**Testing**: 15 unit tests
**Estimated effort**: 8-10 days

---

### 2. data_synthesizer.py (~3,400 lines)
**Add 4 new functions (~3,000 lines total)**:

```python
# NEW FUNCTION 1: City-level supply-demand synthesis
def synthesize_city_level_supply_demand(enriched: dict, kb: dict, cities: list[str], roles: list[str]) -> dict[str, dict[str, Any]]:
    """
    Synthesize per-city supply-demand matrix with tiers.
    - Input: city_salary_bands, hiring_by_tier from enriched
    - Cross-reference with KB benchmarks
    - Calculate supply/demand ratio
    - Generate per-city recommendations
    - Add confidence scores per city

    Returns:
    {
        "San Francisco, CA": {
            "supply": {...},
            "demand": {...},
            "salary_bands": {...},
            "employment_trend": str,
            "recommendation": str,
            "confidence": 0.85
        }
    }

    Complexity: ~700 lines
    Dependencies: enriched city_salary_bands, hiring_by_tier
    Tests: 5 unit tests (all data, partial data, no cities, single city, multiple cities)
    """

# NEW FUNCTION 2: Security clearance synthesis
def synthesize_security_clearance(enriched: dict, kb: dict, roles: list[str], locations: list[str]) -> dict[str, Any]:
    """
    Synthesize clearance-segmented hiring breakdown.
    - Input: clearance_data from enriched
    - Validate against industry baseline (kb)
    - Calculate difficulty scores, salary premiums
    - Recommend channels per clearance level
    - Flag geographic risk zones

    Returns:
    {
        "clearance_mix": {
            "secret": {...},
            "top_secret": {...},
            "other": {...}
        },
        "total_cleared_pct": 23.5,
        "geographic_risk": {...}
    }

    Complexity: ~500 lines
    Dependencies: enriched clearance_data, industry mapping
    Tests: 4 unit tests (high clearance, low, mixed, no data)
    """

# NEW FUNCTION 3: Competitor mapping synthesis
def synthesize_competitor_map(enriched: dict, kb: dict, locations: list[str], roles: list[str]) -> dict[str, list[dict]]:
    """
    Synthesize per-city competitor landscape.
    - Input: competitor_premiums from enriched
    - Cross-reference with hiring volumes
    - Calculate competitive advantage/disadvantage
    - Recommend channel mix to differentiate
    - Flag salary gaps requiring premium positioning

    Returns:
    {
        "San Francisco, CA": [
            {
                "company": "Salesforce",
                "salary_premium": 1.15,
                "competitive_gap": "disadvantage",
                "recommendation": "Use niche boards to reach passive talent"
            }
        ]
    }

    Complexity: ~400 lines
    Dependencies: enriched competitor_premiums, your_salary_bands
    Tests: 4 unit tests (clear advantage, disadvantage, tied, no competitors)
    """

# NEW FUNCTION 4: Activation events synthesis
def synthesize_activation_events(enriched: dict, kb: dict, industry: str, locations: list[str]) -> dict[str, Any]:
    """
    Synthesize seasonal hiring lift + conference calendar.
    - Input: activation_events from enriched
    - Cross-reference with company fiscal calendar
    - Calculate recommended spend adjustments per event
    - Generate creative angle suggestions
    - Flag high-impact events (conferences, funding)

    Returns:
    {
        "seasonal_hiring_peaks": [...],
        "industry_conferences": [...],
        "company_milestones": [...],
        "calendar_events_summary": "5 activations in next 6 months"
    }

    Complexity: ~400 lines
    Dependencies: enriched activation_events, company data
    Tests: 3 unit tests (with events, no events, mixed)
    """

# MODIFY EXISTING FUNCTION: synthesize()
def synthesize(enriched: dict, kb: dict, data: dict) -> dict[str, Any]:
    """
    CALL 4 new functions:
    result["city_supply_demand"] = synthesize_city_level_supply_demand(enriched, kb, cities, roles)
    result["clearance_segmentation"] = synthesize_security_clearance(enriched, kb, roles, locations)
    result["competitors"] = synthesize_competitor_map(enriched, kb, locations, roles)
    result["activation_calendar"] = synthesize_activation_events(enriched, kb, industry, locations)

    ADD calculation:
    result["confidence_score"] = calculate_overall_confidence(result)
    result["data_sources_used"] = extract_data_sources(enriched, kb)

    Lines changed: ~200
    """

# NEW HELPER FUNCTION: Difficulty tier calculation per location
def _calculate_difficulty_tier_detailed(location: str, role: str, enriched: dict, kb: dict) -> dict[str, float]:
    """
    Calculate difficulty scores per tier (junior/mid/senior/staff).
    - Input: city_salary_bands, hiring_by_tier, competitor_premiums, clearance_data
    - Factors: salary competitiveness, hiring volume, competitor density, clearance requirement
    - Output: difficulty_score per tier (1-5 scale)

    Returns:
    {
        "junior": 1.2,
        "mid": 2.5,
        "senior": 3.8,
        "staff": 4.5
    }

    Complexity: ~200 lines
    Dependencies: Existing difficulty calculation logic
    Tests: 3 unit tests (easy, moderate, hard locations)
    """
```

**Subtotal**: ~3,000 lines added
**Testing**: 19 unit tests
**Estimated effort**: 12-14 days

---

### 3. app.py (~15,000 lines)
**Modifications**:

```python
# MODIFY: Plan generation flow (_async_generate)
def _async_generate(jid, gen_data, rid):
    """
    NO CHANGES to existing flow, but ensure NEW enriched data is passed through:
    - enriched = enrich_data(gen_data, request_id=rid) [UNCHANGED, but returns more keys]
    - synthesized = data_synthesize(enriched, kb, gen_data) [UNCHANGED, but returns more keys]
    - All new enrichment + synthesis data flows through existing pipeline

    No new code needed here, but verify data flow works correctly.
    Lines changed: 0 (compatible with existing code)
    """

# MODIFY: Output extraction (_extract_plan_json)
def _extract_plan_json(data: dict) -> dict:
    """
    REPLACE with new Tier 4 extraction:
    - Extract city_supply_demand from data["_synthesized"]
    - Extract security_clearance_segmentation
    - Extract competitor_mapping
    - Extract activation_events
    - Extract confidence_score, data_sources_used, methodology

    Build complete Tier 4 JSON schema:
    {
        "plan_metadata": {...},
        "city_level_supply_demand": {...},
        "security_clearance_segmentation": {...},
        "competitor_mapping": {...},
        "channel_strategy": {...},
        "budget_breakdown_multi_tier": {...},
        "activation_events": {...}
    }

    Lines changed: ~800 (full rewrite of function)
    """

# NEW: Plan verification for Tier 4 schema
def _verify_plan_data_tier4(data: dict) -> dict:
    """
    Validate that plan output matches Tier 4 JSON schema.
    - Check required keys present
    - Validate data types (city_supply_demand must be dict, etc.)
    - Check confidence_score in [0, 1]
    - Ensure city_level_supply_demand has ≥1 city

    Complexity: ~150 lines
    """
```

**Subtotal**: ~950 lines modified/added
**Testing**: 5 integration tests
**Estimated effort**: 3-5 days

---

### 4. excel_v2.py (~1,500 lines)
**Add 4 new sheet functions (~2,000 lines)**:

```python
# NEW SHEET 1: City-level analysis
def _write_city_analysis_sheet(wb: Workbook, data: dict) -> None:
    """
    Sheet: "City-Level Analysis"
    - Table: City | Supply (talent count) | Demand (hiring volume) | S/D Ratio | Salary Bands (junior-staff) | Trend | Recommendation
    - Expand/collapse per city to show tier breakdown
    - ~400 lines
    """

# NEW SHEET 2: Security clearance breakdown
def _write_clearance_sheet(wb: Workbook, data: dict) -> None:
    """
    Sheet: "Security Clearance"
    - Table: Clearance Level | Hiring Volume | % Total | Difficulty | Months to Fill | Salary Premium | Channels
    - Add risk warnings (e.g., "High export control risk in San Jose")
    - ~350 lines
    """

# NEW SHEET 3: Competitor landscape
def _write_competitor_sheet(wb: Workbook, data: dict) -> None:
    """
    Sheet: "Competitor Landscape"
    - Table per city: Competitor | Hiring Volume | Salary Median | Premium vs Market | Open Positions | Preferred Channels | Rating
    - Highlight if premium > 1.15 (you're at disadvantage)
    - Recommend differentiation strategy
    - ~400 lines
    """

# NEW SHEET 4: Activation calendar
def _write_activation_sheet(wb: Workbook, data: dict) -> None:
    """
    Sheet: "Activation Calendar"
    - Table: Event Type | Period/Date | Hiring Lift | Recommended Spend | Creative Angle | Channel Focus | Confidence
    - Seasonal peaks: Q1, Q3
    - Industry conferences: AWS re:Invent, KubeCon, etc.
    - Company milestones: Series C, acquisition, etc.
    - ~400 lines
    """

# MODIFY: generate_excel_v2() main function
def generate_excel_v2(data: dict, research_mod=None) -> bytes:
    """
    ADD 4 new sheet calls in correct order:
    1. Executive Summary (existing)
    2. Channels & Strategy (existing)
    3. Market Intelligence (existing)
    4. City Analysis (NEW)
    5. Clearance Breakdown (NEW)
    6. Competitor Landscape (NEW)
    7. Activation Calendar (NEW)
    8. Sources & Confidence (existing, expanded)

    Lines changed: ~100
    """
```

**Subtotal**: ~2,000 lines added
**Testing**: 4 sheet rendering tests
**Estimated effort**: 5-7 days

---

### 5. templates/index.html & dashboard.html (~17,000 lines combined)
**Add 4 new dashboard sections (~2,000 lines)**:

```html
<!-- NEW SECTION 1: City-Level Analysis -->
<section id="cities-analysis">
    <h3>City-Level Supply & Demand</h3>
    <table>
        <tr>
            <th>City</th>
            <th>Supply (talent)</th>
            <th>Demand (hiring)</th>
            <th>Salary Bands</th>
            <th>Trend</th>
        </tr>
        <!-- Rows generated from plan_data.city_level_supply_demand -->
    </table>
</section>

<!-- NEW SECTION 2: Security Clearance -->
<section id="clearance-segment">
    <h3>Security Clearance Breakdown</h3>
    <tabs>
        <tab name="Secret">Secret: 14% | Channels: ClearedJobs.Net</tab>
        <tab name="TS">Top Secret: 9% | Channels: ClearedJobs.Net</tab>
        <tab name="Other">Other: 77% | Channels: LinkedIn, Indeed</tab>
    </tabs>
</section>

<!-- NEW SECTION 3: Competitor Landscape -->
<section id="competitor-map">
    <h3>Competitor Landscape by City</h3>
    <table per-city>
        <tr>
            <th>Competitor</th>
            <th>Hiring Volume</th>
            <th>Salary Premium</th>
            <th>Status</th>
        </tr>
        <!-- Color-coded: red if premium > 1.15, green if < 0.95 -->
    </table>
</section>

<!-- NEW SECTION 4: Activation Calendar -->
<section id="activation-calendar">
    <h3>Activation Events & Seasonality</h3>
    <timeline>
        <!-- Events: Q1 peak, AWS re:Invent, Series C, Q3 peak -->
        <!-- Each event shows: date, hiring lift, spend recommendation -->
    </timeline>
</section>
```

**Subtotal**: ~2,000 lines added (HTML + inline CSS + minimal JS)
**Testing**: 4 visual tests
**Estimated effort**: 3-4 days

---

## IMPLEMENTATION PHASES

### Phase 1: API Enrichment Layer (Week 1)
**Goal**: Get 5 new data sources working, tested with mock data

- [ ] Day 1-2: Implement `_fetch_salary_by_city()` + unit tests
- [ ] Day 2-3: Implement `_fetch_hiring_volume_by_tier()` + unit tests
- [ ] Day 3: Implement `_fetch_security_clearance_data()` + unit tests
- [ ] Day 4: Implement `_fetch_competitor_salary_premium()` + unit tests
- [ ] Day 5: Implement `_fetch_activation_events()` + unit tests
- [ ] Day 5-6: Modify `enrich_data()` to call 5 new functions
- [ ] Day 6: Integration tests with mock data

**Deliverable**: api_enrichment.py with 5 new functions, all unit tests passing
**Success Criteria**: All 5 functions return expected schema when called with test data

---

### Phase 2: Synthesis Layer (Week 2)
**Goal**: Build 4 new synthesis functions, integrate into pipeline

- [ ] Day 1-2: Implement `synthesize_city_level_supply_demand()` + unit tests
- [ ] Day 2-3: Implement `synthesize_security_clearance()` + unit tests
- [ ] Day 3: Implement `synthesize_competitor_map()` + unit tests
- [ ] Day 4: Implement `synthesize_activation_events()` + unit tests
- [ ] Day 4-5: Implement `_calculate_difficulty_tier_detailed()` helper
- [ ] Day 5: Modify `synthesize()` main function to call 4 new functions
- [ ] Day 6: Integration tests with real enriched data

**Deliverable**: data_synthesizer.py with 4 new functions, all unit tests passing
**Success Criteria**: Synthesis output matches Tier 4 schema for all test cases

---

### Phase 3: Plan Generation & Output (Week 3)
**Goal**: Wire Tier 4 synthesis into plan generation, implement new output extraction

- [ ] Day 1: Update LLM prompt for Tier 4 output
- [ ] Day 1-2: Implement `_extract_plan_json()` rewrite for Tier 4
- [ ] Day 2: Implement `_verify_plan_data_tier4()` schema validation
- [ ] Day 3: Modify `_store_plan_result()` to use new extraction
- [ ] Day 3-4: Integration tests: full end-to-end plan generation
- [ ] Day 4: Parallel run Tier 1 + Tier 4 outputs for validation
- [ ] Day 5: Performance profiling and latency optimization

**Deliverable**: app.py with Tier 4 plan generation, integration tests passing
**Success Criteria**: Full plan generation < 45s, Tier 4 JSON schema validated

---

### Phase 4: Excel & Dashboard (Week 3-4)
**Goal**: Generate 8-sheet Excel and update dashboard with Tier 4 sections

- [ ] Day 1-2: Implement 4 new Excel sheets in excel_v2.py
- [ ] Day 2: Modify `generate_excel_v2()` to call 4 new sheets
- [ ] Day 2-3: Integration tests: Excel generation with Tier 4 data
- [ ] Day 3-4: Add 4 new dashboard sections to index.html
- [ ] Day 4: Add 4 new dashboard sections to dashboard.html
- [ ] Day 5: Visual tests (Chrome MCP): verify layout, colors, data display
- [ ] Day 5-6: A/B testing setup (Tier 1 vs Tier 4 to 50% of users)

**Deliverable**: Excel + dashboard with Tier 4 output, visual tests passing
**Success Criteria**: Excel readable, no formatting errors. Dashboard displays all 10+ sections correctly.

---

## SUCCESS METRICS

### Data Quality Metrics
- [ ] City-level coverage: ≥85% of plans have ≥2 cities enriched
- [ ] Confidence score: Mean 0.80+, Std Dev <0.15
- [ ] API success rate: ≥90% for new functions

### Performance Metrics
- [ ] Plan generation latency: <45s (including new API calls)
- [ ] Cache hit rate: ≥70% for city salary data
- [ ] API cost: <$0.10 per plan

### User Satisfaction Metrics (after A/B test)
- [ ] Plan actionability: ≥75% rate "very detailed"
- [ ] Data transparency: ≥60% cite data sources
- [ ] Tier 4 adoption: ≥80% download Excel after seeing Tier 4 plan

---

## TESTING STRATEGY

### Unit Tests (per-function, ~30 tests)
- Test normal cases (good data)
- Test edge cases (missing data, empty lists)
- Test error handling (API timeout, network error)

### Integration Tests (per-module, ~10 tests)
- api_enrichment: Call all 5 functions in sequence
- data_synthesizer: Call all 4 functions with real enriched data
- app.py: Full plan generation end-to-end
- excel_v2.py: 8-sheet generation without errors

### E2E Tests (~5 tests)
- Full generation with real APIs (gated behind feature flag)
- Parallel Tier 1 + Tier 4 output validation
- Confidence score calculation correctness
- Excel rendering without corruption

### Gold Standard Validation
- Compare generated plan against gold standard manually
- Verify city-level breakdowns match audit
- Verify clearance segmentation reasonable for industry
- Verify competitor premiums within expected range

---

## RISK MITIGATIONS

| Risk | Mitigation |
|------|-----------|
| City-level API unavailable | Graceful fallback to national aggregates |
| LLM prompt inconsistent | Strict schema validation, retry with simpler prompt |
| Performance impact | Parallel execution (already in place), aggressive caching, load testing |
| Competitor salary data unavailable | Fall back to BLS percentiles + industry baseline |
| User confusion over new sections | Feature flag for gradual rollout, user docs, in-app tooltips |

---

## DEPLOYMENT STRATEGY

### Pre-Deployment
- [ ] Code review by 2+ engineers
- [ ] All tests passing (unit, integration, E2E)
- [ ] Performance benchmarks acceptable
- [ ] Load test with 10x concurrent requests

### Deployment
- [ ] Feature flag: `ENABLE_TIER4_PLANS` = false by default
- [ ] Gradual rollout: 5% users for 1 day, 25% for 1 day, 100%
- [ ] Monitor: Plan generation latency, error rates, user feedback
- [ ] Parallel run: Generate both Tier 1 + Tier 4 for validation (7 days)

### Post-Deployment
- [ ] Collect user feedback via in-app surveys
- [ ] Monitor confidence scores (flag < 0.70)
- [ ] Fine-tune synthesis prompts based on real outputs
- [ ] Optimize API call latency (cache, batch requests)

---

## FILES TO MODIFY SUMMARY

| File | Lines | Type | Effort |
|------|-------|------|--------|
| api_enrichment.py | +2,000 | New functions | 8-10 days |
| data_synthesizer.py | +3,000 | New functions | 12-14 days |
| app.py | +950 | Modifications | 3-5 days |
| excel_v2.py | +2,000 | New sheets | 5-7 days |
| templates/*.html | +2,000 | New sections | 3-4 days |
| **TOTAL** | **+10,000** | **New/Modified** | **31-40 days** |

**Total Effort**: 4-6 weeks full-time (1 senior + 1 mid-level engineer)

---

## PARALLEL WORK STREAMS

**Can be done in parallel**:
- API enrichment functions 1-5 (independent)
- Synthesis functions 1-4 (independent, wait for API layer)
- Excel sheets 1-4 (independent, wait for synthesis)
- Dashboard sections 1-4 (independent, wait for synthesis)

**Dependency order**:
1. API enrichment (blocking all downstream work)
2. Data synthesis (blocking output + Excel + dashboard)
3. App/output extraction (blocking everything else)
4. Excel + dashboard (parallel, last mile)

---

**Document Status**: ROADMAP COMPLETE
**Ready for**: Developer assignment and sprint planning
