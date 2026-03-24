# Nova AI Suite -- Product Consolidation Plan

**Author**: Shubham Singh Chandel
**Date**: 2026-03-24
**Status**: Proposal

---

## 1. Current State: 21 Standalone Products

The Nova AI Suite currently ships 21 separate products, each with its own template, route, and endpoint. While this demonstrates breadth, it creates fragmentation for users who must navigate between many tools to accomplish related tasks. Users do not think in terms of "ROI Calculator" vs "Performance Tracker" vs "Post-Campaign Analysis" -- they think in terms of "how did my campaign perform?"

### Full Product Inventory

| # | Product | Route | Category | Status |
|---|---------|-------|----------|--------|
| 1 | Media Plan Generator | /media-plan | Planning | Flagship |
| 2 | Nova Chat | /nova | Tools | Flagship |
| 3 | Budget Simulator | /simulator | Planning | Live |
| 4 | Performance Tracker | /tracker | Analytics | Live |
| 5 | Competitive Intel | /competitive | Intelligence | Live |
| 6 | Quick Plan | /quick-plan | Planning | Live |
| 7 | Media Plan Audit | /audit | Analytics | Live |
| 8 | Market Pulse | /market-pulse | Intelligence | Live |
| 9 | Social Plan Generator | /social-plan | Planning | New |
| 10 | HireSignal | /hire-signal | Analytics | New |
| 11 | ApplyFlow | /applyflow | Tools | New |
| 12 | API Portal | /api-portal | Tools | New |
| 13 | Talent Heat Map | /talent-heatmap | Intelligence | New |
| 14 | Market Intel Reports | /market-intel | Intelligence | Beta |
| 15 | SkillTarget | /skill-target | Planning | Beta |
| 16 | ROI Calculator | /roi-calculator | Analytics | New |
| 17 | A/B Test Lab | /ab-testing | Intelligence | New |
| 18 | Quick Brief | /quick-brief | Intelligence | New |
| 19 | Post-Campaign Analysis | /post-campaign | Analytics | New |
| 20 | PayScale Sync | /payscale-sync | Intelligence | New |
| 21 | ComplianceGuard | /compliance-guard | Compliance | New |
| 22 | CreativeAI | /creative-ai | Tools | New |
| 23 | VendorIQ | /vendor-iq | Analytics | New |

**Problem**: 23 products is overwhelming. Users get decision fatigue. Many products overlap (Quick Plan is a subset of Media Plan Generator; Post-Campaign Analysis, Performance Tracker, and ROI Calculator all analyze campaign results). This is the opposite of how Semrush, Ahrefs, and HubSpot organize their suites.

---

## 2. Proposed Consolidated State: 6 Products

### Product Architecture

```
Nova AI Suite
|
+-- Media Planner (flagship)          -- Plan creation, budgeting, channel mix
+-- Campaign Analytics (flagship)     -- Track, analyze, optimize campaigns
+-- Market Intelligence               -- Market data, hiring signals, talent maps
+-- Data Hub                          -- Vendors, compliance, salary benchmarks
+-- CreativeAI                        -- Ad copy, brief parsing, A/B variants
+-- Nova Chat                         -- Conversational interface to everything
```

---

## 3. Consolidation Details

### Product A: Media Planner (Flagship)

**Absorbs**: Media Plan Generator, Quick Plan, Social Plan Generator, Budget Simulator, SkillTarget, Quick Brief, Media Plan Audit

**How it works**: One unified planning interface with progressive complexity. The entry point is a simple form (like the current Quick Plan). Users can expand to full mode (current Media Plan Generator) when they need detailed control. Social channels, budget simulation, skill-based targeting, and plan auditing become tabs or sections within the same product.

| Merged Product | Becomes | Location in New Product |
|---|---|---|
| Media Plan Generator | Core engine | Main interface -- full planning mode |
| Quick Plan | Quick mode | Toggle at top: "Quick" / "Full" mode switch |
| Quick Brief | NLP input | Text input option: "Describe your hiring need" |
| Social Plan Generator | Social tab | Channel selection: "Social Channels" tab |
| Budget Simulator | Budget section | "Budget Modeling" panel within plan builder |
| SkillTarget | Skill filter | "Target by Skills" filter in channel selection |
| Media Plan Audit | Audit mode | "Audit Existing Plan" tab -- upload and score |

**Features carried over**:
- 91+ job board database with channel recommendations
- 5-tier data cascading for job board selection
- AI-powered budget allocation across channels
- Collar-aware budget splits (blue/white/grey collar)
- 9 social platforms + 3 search engines
- Budget scenario modeling and forecasting
- Skill-to-platform mapping with rarity scoring
- Plan scoring with gap analysis
- NLP brief parsing (plain text to structured plan)

**Route**: `/planner` (redirects from all legacy routes)

---

### Product B: Campaign Analytics (Flagship)

**Absorbs**: Performance Tracker, Post-Campaign Analysis, ROI Calculator, A/B Test Lab, HireSignal

**How it works**: A single analytics dashboard with multiple views. Users upload campaign data once and get performance tracking, post-campaign reports, ROI projections, A/B test results, and quality-of-hire metrics in one place. Tabs switch between views. Data flows between them automatically.

| Merged Product | Becomes | Location in New Product |
|---|---|---|
| Performance Tracker | Dashboard | Main view -- real-time KPI dashboard |
| Post-Campaign Analysis | Reports tab | "Campaign Report" tab with grading |
| ROI Calculator | ROI panel | "ROI & Projections" panel in sidebar |
| A/B Test Lab | Testing tab | "A/B Testing" tab for variant comparison |
| HireSignal | Quality tab | "Quality of Hire" tab with QoH scoring |

**Features carried over**:
- CPA, CPC, conversion rate tracking
- AI-powered performance analysis with recommendations
- Campaign grading (A+ through F)
- Cost-per-hire savings projections
- Time-to-fill improvement estimates
- A/B test configuration and variant generation
- Benefits-focused vs culture-focused copy comparison
- Composite QoH scoring by source
- Funnel analysis and retention tracking
- Cost-per-quality-hire metrics

**Route**: `/analytics` (redirects from all legacy routes)

---

### Product C: Market Intelligence

**Absorbs**: Market Pulse, Competitive Intel, Talent Heat Map, Market Intel Reports

**How it works**: One intelligence platform with four lenses. Users enter a role, industry, or company and get a unified view: market trends (Market Pulse), competitor analysis (Competitive Intel), geographic talent data (Talent Heat Map), and full research reports (Market Intel). Each is a tab within the same interface.

| Merged Product | Becomes | Location in New Product |
|---|---|---|
| Market Pulse | Trends view | "Market Trends" tab with real-time data |
| Competitive Intel | Competitors view | "Competitor Analysis" tab |
| Talent Heat Map | Geography view | "Talent Map" tab with metro data |
| Market Intel Reports | Reports view | "Research Report" tab with full export |

**Features carried over**:
- Real-time hiring trend monitoring
- Salary movement tracking across industries
- Platform performance monitoring
- Deep competitive analysis of hiring strategies
- Market positioning analysis
- 60+ metro talent supply visualization
- Competition index and cost-of-living factors
- Salary benchmarks by role and geography
- Comprehensive industry research reports
- FRED, Adzuna, BLS, BEA, Census API data
- Jooble competitive data

**Route**: `/intelligence` (redirects from all legacy routes)

---

### Product D: Data Hub

**Absorbs**: VendorIQ, ComplianceGuard, PayScale Sync

**How it works**: A centralized data and compliance platform. Three modules: Vendor Scorecard (compare job boards), Compliance Scanner (audit job postings), and Salary Benchmarks (market-rate salary data). These share underlying data about markets, regulations, and vendor performance.

| Merged Product | Becomes | Location in New Product |
|---|---|---|
| VendorIQ | Vendors module | "Vendor Scorecard" tab |
| ComplianceGuard | Compliance module | "Compliance Scanner" tab |
| PayScale Sync | Salary module | "Salary Benchmarks" tab |

**Features carried over**:
- CPC, CPA, quality score comparison across vendors
- Smart budget allocation recommendations by vendor
- Pay transparency gap scanning (EU + US state laws)
- Biased language detection
- EEO/OFCCP violation flagging
- Age discrimination risk detection
- Market-competitive salary range generation
- BLS and Adzuna salary data integration

**Route**: `/data-hub` (redirects from all legacy routes)

---

### Product E: CreativeAI (Standalone)

**Stays standalone** but gains features from Quick Brief's NLP parsing.

**Features**:
- 5 distinct recruitment ad copy variants
- Benefits-first, culture-forward, growth-focused angles
- Platform-optimized copy (LinkedIn vs Indeed vs social)
- A/B testing suggestions
- NLP brief parsing for context

**Route**: `/creative-ai` (unchanged)

---

### Product F: Nova Chat (Standalone)

**Stays standalone** as the conversational gateway to the entire suite.

**Enhancement**: Nova becomes the unified entry point. Users can ask "create a media plan for a senior engineer in NYC" and Nova routes to Media Planner. Ask "how did my Indeed campaign perform?" and it routes to Campaign Analytics.

**Features**:
- Natural language interface to all 5 products
- O*NET, Adzuna, FRED data access
- Strategy recommendations
- Campaign insights
- RAG-powered knowledge base
- Voice input, dark mode, file upload, share links

**Route**: `/nova` (unchanged)

---

### Products Not Consolidated (Infrastructure)

These are not user-facing products but infrastructure tools. They remain as-is:
- **ApplyFlow** (`/applyflow`) -- Candidate-facing, different audience
- **API Portal** (`/api-portal`) -- Developer-facing, different audience

---

## 4. Migration Plan

### Phase 1: Backend Unification (Week 1-2)

1. Create unified endpoint handlers that combine related API routes
2. Build shared data models that feed multiple views
3. Ensure all existing API endpoints continue to work (backward compatibility)
4. Add internal routing so `/tracker`, `/roi-calculator`, etc. all resolve to `/analytics`

### Phase 2: Frontend Consolidation (Week 3-4)

1. Build tabbed interfaces for each consolidated product
2. Each tab corresponds to a former standalone product
3. Implement progressive disclosure: simple by default, detailed on demand
4. Add cross-product data sharing (upload data once, see it everywhere)

### Phase 3: Hub Redesign (Week 5)

1. Reduce hub.html from 23 cards to 8 cards (6 products + ApplyFlow + API Portal)
2. Each card becomes larger and richer (more detail, better imagery)
3. Add "Explore features" sections showing what each product contains

### Phase 4: Route Migration (Week 6)

1. Add 301 redirects from all legacy routes to new consolidated routes
2. Update all internal links, PostHog tracking, and Sentry error references
3. Update API documentation and external references
4. Keep legacy routes working for 90 days with deprecation headers

### Phase 5: Cleanup (Week 7-8)

1. Remove deprecated standalone templates
2. Consolidate duplicate API endpoint logic
3. Update tests to reflect new product structure
4. Update CLAUDE.md, MEMORY.md, and all documentation

---

## 5. Industry Comparison

### How Semrush Does It

Semrush has 55+ tools but organizes them into 5 toolkits:
- **SEO Toolkit**: Keyword research, site audit, rank tracking, backlink analysis
- **Advertising Toolkit**: PPC keyword research, ad builder, display ads
- **Social Media Toolkit**: Posting, analytics, social ads
- **Content Marketing Toolkit**: Topic research, SEO writing assistant, content audit
- **Competitive Research Toolkit**: Traffic analytics, market explorer, keyword gap

Each toolkit is one entry point with multiple tools inside. Users never see 55 separate products. They see 5 doors.

### How Ahrefs Does It

Ahrefs has 4 core tools:
- **Site Explorer**: Backlink checker, organic traffic research, paid traffic research
- **Keywords Explorer**: Keyword difficulty, SERP analysis, keyword ideas
- **Content Explorer**: Content research, find link prospects
- **Site Audit**: Technical SEO audit, internal links

Everything else (rank tracker, web analytics, SEO toolbar) is secondary. The 4 core tools are the product.

### How HubSpot Does It

HubSpot has 6 hubs:
- **Marketing Hub**: Email, ads, social, SEO, landing pages
- **Sales Hub**: CRM, deals, sequences, quotes
- **Service Hub**: Tickets, knowledge base, feedback
- **CMS Hub**: Website builder, themes, SEO
- **Operations Hub**: Data sync, workflow automation
- **Commerce Hub**: Quotes, invoices, payments

Each hub contains dozens of features. Users buy a hub, not individual features.

### Nova AI Suite Alignment

Our proposed 6-product structure follows the same pattern:

| Industry Pattern | Nova Equivalent |
|---|---|
| Semrush SEO Toolkit | Media Planner |
| Semrush Advertising Toolkit | Campaign Analytics |
| Semrush Competitive Research | Market Intelligence |
| HubSpot Operations Hub | Data Hub |
| Standalone creative tools | CreativeAI |
| HubSpot AI Assistant | Nova Chat |

This gives Nova the same professional, organized feel as enterprise SaaS leaders while reducing cognitive load from 23 choices to 6.

---

## 6. Impact Summary

| Metric | Before | After |
|---|---|---|
| Products on hub page | 23 | 8 |
| User decision points | 23 | 6 (+ 2 infra) |
| Templates to maintain | 23 | 8 |
| API endpoint groups | ~23 | ~8 |
| Feature count | Same | Same (all features preserved) |
| User experience | Fragmented | Unified |
| Perceived quality | "Lots of small tools" | "Powerful integrated platform" |

The goal is not to remove features. Every feature from every product carries over. The goal is to present them as coherent, powerful products instead of scattered point tools.
