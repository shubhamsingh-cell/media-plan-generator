# ComplianceGuard -- Product Brief

## Problem Statement
Recruitment organizations face a regulatory tsunami in 2026:
- **EU AI Act (Aug 2, 2026):** AI in hiring = "high-risk". Conformity assessments, human oversight, explainable scoring, audit trails. Fines: 35M EUR or 7% global turnover.
- **EU Pay Transparency Directive (June 7, 2026):** Salary ranges mandatory in all EU job postings. Gender pay gap >5% triggers mandatory remediation.
- **US State Patchwork:** Colorado SB 24-205 (Feb 2026), NYC LL-144 (active), California/Illinois/Texas AI hiring bills in progress.

No single tool covers job posting compliance + AI bias auditing + pay equity + multi-jurisdiction tracking.

## Target User
- Primary: TA leaders, HR Compliance officers at 500+ employee companies recruiting across US + EU
- Secondary: Staffing agencies, RPOs, federal contractors
- Buyer: CHRO, VP People, General Counsel

## Competitive Landscape
- **Holistic AI** -- NYC bias audits, AI governance. Strong on algorithmic auditing, weak on job posting compliance.
- **Credo AI** -- AI governance, LL-144 compliance. Model-level, not recruitment workflow.
- **Syndio/Trusaic** -- Pay equity leaders. No job posting compliance.
- **ATS add-ons** (Greenhouse, iCIMS) -- Surface-level compliance features.
- **Gap ComplianceGuard fills:** Unified platform for all compliance surfaces, integrated into recruitment intelligence suite.

## MVP Features (Target: Late May 2026)

### 1. Job Posting Compliance Scanner
- Real-time analysis for gender-coded language, age/disability bias, missing pay disclosures
- Jurisdiction-aware rules (EU, Colorado, NYC, California)
- One-click LLM fix mode
- Batch scanning via CSV
- **Already partially built:** `/api/compliance/analyze`, `/api/compliance/audit` in app.py

### 2. Multi-Jurisdiction Compliance Tracker
- Dashboard showing applicable regulations by recruiting location
- Timeline of upcoming deadlines
- Auto-updated regulatory feed (extends `scrape_compliance_updates`)
- Jurisdiction-specific checklists

### 3. Pay Transparency Module
- Salary range validation against Adzuna + BLS market data
- Flag unrealistic ranges (EU directive prohibits artificially wide ranges)
- Salary history ban enforcement
- EU Pay Transparency Directive readiness scorecard

### 4. Compliance Report Generator
- Auto-generate PDF/DOCX compliance audit reports
- EEO-1 report data preparation
- Audit trail documentation for EU AI Act Article 86

## V2 Features (Q4 2026)
- AI Bias Audit Engine (disparate impact, 4/5ths rule, LL-144 output)
- Pay Equity Analysis (multi-factor regression, 5% gap alerting)
- Continuous Monitoring & Slack Alerts
- ATS Integration Webhooks (pre-publish compliance gate)

## Technical Dependencies

### Already available in Nova:
- LLM router (24 providers) -- analysis & fix rewrites
- Adzuna + BLS salary data -- pay range validation
- O*NET -- role-based compliance mapping
- FRED, BEA, Census -- economic context
- Firecrawl (+ 6-tier fallback) -- regulatory feed scraping
- Document generation -- compliance reports
- Slack integration -- alerts (pending setup)
- Supabase -- audit trails

### New integrations needed:
- EU regulatory database API (EUR-Lex)
- State legislature tracking (LegiScan/OpenStates)
- Pay equity statistical engine (scipy/statsmodels or LLM-assisted) -- V2
- ATS webhook receivers -- V2

## Timeline
| Phase | Scope | Duration | Target |
|-------|-------|----------|--------|
| Phase 0 | Foundation (schema, Firecrawl, EEOC data) | 1 week | April 2026 |
| Phase 1 | MVP Core (scanner, tracker, pay transparency) | 3-4 weeks | April-May 2026 |
| Phase 2 | Reports & Polish | 2 weeks | May 2026 |
| **MVP Launch** | Before EU deadlines | -- | **Late May 2026** |
| Phase 3 | V2 (bias audit, pay equity, ATS webhooks) | 6-8 weeks | Q3-Q4 2026 |

## Go-to-Market
Positioning: "The only recruitment compliance platform combining real-time job posting scanning, pay transparency validation, and multi-jurisdiction tracking -- powered by the same labor market intelligence engine behind your media plans."
