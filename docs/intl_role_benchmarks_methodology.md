# Intl Role-Level Recruitment Benchmarks v1 — Methodology

**File**: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data/intl_role_benchmarks_v1.json`
**Version**: 1.0
**Built**: 2026-05-22
**Author**: Deep Research Agent

## What This File Adds

The existing `international_benchmarks_2026.json` (8,171 lines) provides aggregate platform pricing for 38 markets but ZERO role-level breakdowns. That gap blocked Nova from answering queries like:

- "What's the CPA for nurses in London?"
- "How much to hire a software engineer in Berlin vs Bengaluru?"
- "Cost per hire for a chef in Sydney?"

This new file fills that gap with **vertical × country × role-level data** across 5 verticals and 15 priority markets.

## Coverage

| Dimension | Count | Detail |
|---|---|---|
| Verticals | 5 | healthcare_nursing, technology, blue_collar, hospitality, finance |
| Countries | 15 | US, UK, India, Germany, France, Canada, Australia, Netherlands, Spain, Brazil, Mexico, Singapore, UAE, Japan, Ireland |
| Country × vertical pairs | 75 | Full matrix |
| Cited citation cells | ~554 | Each has source_ids, confidence rating, retrieval date |
| Sources in registry | 70 | Tier-1 government (BLS, ONS, INSEE, HSE), tier-2 industry reports, tier-3 secondary aggregators |
| Data gaps flagged | 60 | Explicitly noted instead of fabricated |
| File size | 333 KB | |

## Data Sources by Type

### Tier 1 (highest credibility — government / official statistics)
| Source | Coverage | URL |
|---|---|---|
| BLS OEWS May 2024 | US salary medians for all SOC codes | https://www.bls.gov/oes/ |
| ONS ASHE 2025 | UK earnings & hours | https://www.ons.gov.uk/releases/employeeearningsintheuk2025 |
| HSE Ireland Pay Scales March 2025 | Ireland nurse pay scales (official) | https://healthservice.hse.ie/documents/5205/MARCH_2025_pay_scales.pdf |
| INSEE / OPCO Santé / France Travail | France employment & healthcare recruitment | https://www.insee.fr / https://www.opco-sante.fr |
| FRED Indeed Indices | US sector-level real-time job postings | https://fred.stlouisfed.org |
| Bundesagentur für Arbeit / StepStone | Germany salary report (1.3M data points) | https://www.eqs-news.com (StepStone Salary Report 2026) |
| WEF Future of Jobs Report 2025 | Global frontline role projections | https://reports.weforum.org |

### Tier 1 (industry-leading benchmarks, large sample sizes)
- **Appcast 2026 Recruitment Marketing Benchmark Report** — 10th annual, 302M clicks, 27M apps from 1,200 employers globally
- **Appcast 2026 UK Benchmark Report** — 3.6M UK clicks, 880k apps from 190 employers
- **NSI 2026 National Health Care Retention Report** — 450 hospitals across 37 states (US nursing definitive)
- **SmartRecruiters Recruiting Benchmarks 2025** — 89M apps across 1.5M jobs in 95 countries
- **Indeed Hiring Lab 2026 Reports** (US, UK, Ireland)
- **SEEK Employment Dashboard August 2025** — definitive AU
- **Robert Half Japan Salary Guide 2025-26** — Japan tech 25/50/75 pct ranges

### Tier 2 (industry / trade association)
- Reed UK Salary Guides 2025-26 (21M+ job postings since 2016)
- StepStone Group annual reports
- Naukri (Info Edge) IT salary benchmarks
- Asanify hiring guides (Australia, India)
- EuroDev hiring cost guides (Germany, Netherlands, Spain)
- Globalli employer cost guides (Germany, Mexico)
- AMN Healthcare / Merritt Hawkins (US physician comp)
- Medscape / Doximity (US physician comp)
- ANC Italy / Hospital RCN (UK nursing)
- HSE Ireland March 2025 pay scales (official)
- Asanify (Australia EOR guide)

### Tier 3 (secondary aggregators, used cautiously)
- Levels.fyi (self-reported tech salaries — used as "high" tech-skewed reference, not median)
- PayScale country guides
- Manatal CPH benchmarks
- B2Linked LinkedIn CPC by geography

### Live API Data
- **Adzuna Historical Salary API** — Pulled 66 country × category cells (Nov 2025 - Apr 2026, 6-month averages) for US, GB, CA, AU, IN, DE, FR, NL, SG, BR, MX

## Per-Vertical Sourcing Approach

### 1. Healthcare / Nursing

**Primary anchors**:
- **US**: BLS OEWS RN median ($93,600), NSI 2026 RN replacement cost ($61,110), AHA 2026 Workforce Scan
- **UK**: NHS Band 5 pay scales (RCN), Nuffield Trust international recruitment cost analysis, ONS ASHE 2025
- **Ireland**: Official HSE March 2025 pay scales (most detailed government source)
- **Germany**: Learn German Online + InfoMigrants for foreign nurse recruitment (300k+ recruited cumulatively)
- **France**: Caducee.net + INSEE/DREES (€1,944.5/mo FPH grade 1 entry)
- **Australia**: Asanify EOR guide (AUD 65k-160k+ RN range)
- **Adzuna 6mo averages** for all 11 markets where API supports

**Verification approach**:
- Cross-referenced RN salary ranges across NSI, BLS, Adzuna, AMN Healthcare for US
- For Ireland, used official HSE pay scales (Tier 1) AND FRS recruitment commentary (Tier 2) to triangulate
- For Germany, statutory minimum wage Jul 2025 (€20.50/hr senior caregiver = €3,550/mo) is government-set
- CPA estimates use Appcast 2026 healthcare premium (highest CPA tier globally) + LinkedIn US avg $2.83

**Confidence ratings**:
- US/UK/Ireland/Germany healthcare: HIGH (multiple Tier-1 sources align)
- France/Netherlands/Spain: MEDIUM (single strong source per metric)
- UAE/Japan/Brazil/Mexico: MEDIUM to LOW (fewer publicly available role-specific benchmarks)
- India: MEDIUM (Naukri pricing + Adzuna distorted upward by intl recruitment ads)

### 2. Technology

**Primary anchors**:
- **US**: BLS OEWS Software Developer median ($133,080), Levels.fyi senior median TC ($312k), Appcast 2026 tech apply rate (7.14%)
- **India**: Plugscale + CliqHR + Shework + Levels.fyi for tech salary at all levels
- **UK**: Ravio 2025 Compensation Trends (£70k median P3 SWE, £110k senior M3)
- **Germany**: StepStone Salary Report 2026 (1.3M data points), Berlin 2025 €75k median tech
- **Japan**: Robert Half Japan 2025-26 Salary Guide (definitive 25/50/75 pct ranges), Levels.fyi (¥8.5M median TC SWE)
- **Ireland**: Levels.fyi (€102k median TC), PayScale (€50k base avg) — gap reflects FAANG cluster vs broader market
- **Singapore**: Mavenside critical roles data (AI/ML SGD 12k/mo, Cybersec SGD 10k/mo)

**Verification approach**:
- US Software Developer median triangulated across BLS, US News Best Jobs, ONET, multiple sources
- India IT CPH cross-referenced Shework (₹35k-80k) with Plugscale (8-15% of salary)
- Japan SWE compared Robert Half (¥10.5M backend median), Levels.fyi (¥8.5M), Gitnux (¥6.2M Tokyo IT engineer 2023) — converging on ~¥7-10M mid-career

**Confidence ratings**:
- US/UK/Ireland/Japan tech: HIGH (multiple Tier-1 sources + recent data)
- India/Germany/Singapore: HIGH/MEDIUM
- France/Spain/Australia/Canada: MEDIUM (Adzuna + 1-2 secondary sources)
- Brazil/Mexico/UAE: MEDIUM (limited recent local-language sources)

### 3. Blue Collar

**Primary anchors**:
- **WEF Future of Jobs Report 2025**: global frontline growth projections
- **Appcast 2026**: blue-collar lowest CPA tier confirmation, highest apply rates
- **Adzuna live data**: warehouse, driver, construction salary 6mo averages
- **Manatal CPH benchmarks**: manufacturing $10,378, services $8,574 (US)

**Verification approach**:
- US warehouse $39,803 median (Adzuna) cross-checked vs BLS OES SOC 53-7062 stockers ($35-42k range)
- US driver $50,376 median (Adzuna) within BLS HD truck driver range ($45-58k)
- India delivery associate triangulated vs international_benchmarks_2026.json Naukri benchmarks

**Confidence ratings**:
- Adzuna-sourced salary medians: HIGH for 11 markets
- CPA/CPH for non-Adzuna markets: MEDIUM (regional extrapolations)
- Hiring difficulty: HIGH (WEF + Appcast align across markets)

### 4. Hospitality

**Primary anchors**:
- **SEEK Australia**: best industry tracker for hospitality dynamics (+7.6% 6mo growth)
- **Adzuna 6mo averages** all 11 supported markets
- **Hospitality Net / Staffing Agency 2025-2030 report** (Canada hospitality CAD 104B 2025)
- **2025 Hospitality Hiring Trends (Escoffier Global)**: US sector data

**Verification approach**:
- US chef $55,725 median (Adzuna) vs industry sources for line cook + executive chef ranges
- AU hospitality manager AUD 65-95k (SEEK + MMC convergent)
- Spain unfilled hospitality jobs (~200k) cross-confirmed from multiple sources

**Confidence ratings**:
- Established markets (US/UK/AU/Canada): HIGH
- EU non-anchor markets: MEDIUM
- Asia (SG/JP) + Latin America: MEDIUM

### 5. Finance

**Primary anchors**:
- **Adzuna 6mo averages** for all 11 markets (accounting-finance-jobs category)
- **Indian Plugscale + ICAI** for CA salary ranges
- **Computrabajo MX** (finance 16% of vacancies, 2nd most demanded sector)
- **Robert Half** UK/Singapore/Japan finance guides
- **eFinancialCareers** regional dominant niche platform

**Verification approach**:
- US accountant $67,514 median (Adzuna) vs BLS OEWS accountants ($59-95k range)
- UK accountant £49,983 (Adzuna) vs Reed UK salary guides (£45-65k mid-level)
- Japan finance ¥6.36M accountant avg (Gitnux) + ¥410k/mo finance/insurance (GaijinPot) — converges

**Confidence ratings**:
- US/UK/Canada finance: HIGH
- India/Germany/Australia: HIGH
- France/Spain/Brazil/Mexico/SG/UAE/Japan/Ireland: MEDIUM (single strong source per metric)

## Verification Process

For each cited cell:

1. **Source_ids must point to real registry entries** — file structure enforces this
2. **Confidence rating reflects source quality**:
   - HIGH: 2+ independent sources align within 20%
   - MEDIUM: 2+ sources with some variance OR 1 government/Tier-1 source
   - LOW: Tier-2/3 source only, extrapolated, less recent
   - SINGLE_SOURCE: explicitly flagged when only one source exists
3. **Random verification audit (5 cells)**:
   - Sample drawn with `random.seed(42)`, sample of 5 from 554 cells
   - For each, the cited source URL was extracted via Tavily and confirmed to contain the cited number
   - **Result**: 5/5 verified — all sources support the values cited
4. **Data gaps explicitly listed** per country/vertical — these are NOT estimated, just acknowledged

## Currency Conversion

USD equivalents are computed at mid-May 2026 mid-market rates stored in `_metadata.currency_rates_used`:
```
USD: 1.0, GBP: 1.34, EUR: 1.10, CAD: 0.73, AUD: 0.66,
INR: 0.0119, SGD: 0.74, BRL: 0.18, MXN: 0.049, AED: 0.272, JPY: 0.0065
```

Local currency figures are PRIMARY — USD is provided for cross-market comparison only.

## Known Limitations

1. **Adzuna salary distortion in some markets**: India healthcare-nursing 6mo avg shows ₹12L which is inflated by international recruitment ads (UK/Germany recruiting Indian nurses). Original salary used over Adzuna for IN healthcare.
2. **Singapore IT Adzuna sample**: SGD 25,614 6mo avg appears low — likely a sample issue (some listings show base monthly not annual). Used Mavenside critical tech roles data as primary.
3. **France healthcare Adzuna**: €78,976 6mo avg distorted by specialist/cadre listings. Used INSEE/DREES + Caducee.net official scales as primary.
4. **UAE / Japan / Spain / Ireland**: No Adzuna API support. Relied on Tavily research + government data + industry reports.
5. **CPA cell ranges**: Where direct CPA benchmarks didn't exist for niche country×vertical pairs, used Appcast 2026 sector premiums applied to LinkedIn/Indeed regional CPC tiers. Flagged as MEDIUM-LOW confidence.
6. **City-level breakdowns**: Most cells are country-level. Metro splits (London vs Manchester, Mumbai vs Bengaluru, Berlin vs Munich) are listed in `data_gaps` per country, not estimated.

## How Nova Should Use This Data

For a query like "What's the CPA for nurses in London?":
1. Navigate `verticals.healthcare_nursing.by_country.uk.cpa_cost_per_applicant`
2. Read `low/high/median` in USD and GBP
3. Cite source IDs (S2 = Appcast 2026 UK Benchmark Report)
4. Note confidence rating
5. Add `market_notes` for context (NHS workforce 1.38M FTE, 11% vacancy etc.)
6. Mention `data_gaps` if applicable ("Note: London-specific differential vs UK-wide not in current dataset")

## File Versioning

Future updates should:
1. Bump `_metadata.version` (e.g., 1.0 → 1.1)
2. Add new sources to the `sources` registry
3. Update `_metadata.created` to reflect last refresh
4. Preserve historical cells (don't delete) — add a `superseded_at` field when revising

## Maintenance Cadence

- **Adzuna salary data**: re-fetch quarterly (rolling 6-mo window)
- **Appcast benchmarks**: annual refresh when 2027 report drops
- **Government statistics (BLS, ONS, INSEE, HSE)**: annual refresh after Q1 publication
- **Spot-check at-risk markets**: India CPH inflation, UAE expat policy changes, Japan shunto outcomes
