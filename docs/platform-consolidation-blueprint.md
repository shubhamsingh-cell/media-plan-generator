# Nova AI Suite -- Platform Consolidation Blueprint

## Summary
**Before:** 24 separate product pages accessed from a flat catalog hub.
**After:** 17 tools organized into 6 toolkits, with a live dashboard and persistent AI assistant.

## 6 Toolkits

### TOOLKIT 1: PLAN (Campaign Planning)
- Media Plan Generator (absorbs Quick Plan + Quick Brief as modes)
- Social & Search Planner
- A/B Testing Lab
- CreativeAI Studio

### TOOLKIT 2: INTELLIGENCE (Market & Competitive Research)
- Competitive Intelligence
- Market Pulse (absorbs Market Intel Reports as "Reports" tab)
- Talent Supply Heat Map
- SkillTarget
- VendorIQ

### TOOLKIT 3: OPTIMIZE (Budget & Performance)
- Budget Simulator (absorbs ROI Calculator as "ROI Projection" tab)
- Campaign Tracker (absorbs Post-Campaign Analysis as tab)

### TOOLKIT 4: TALENT (Talent Intelligence)
- HireSignal
- PayScale Sync
- ApplyFlow

### TOOLKIT 5: COMPLIANCE (Regulatory & Audit)
- ComplianceGuard (absorbs Recruitment Ad Audit as "Ad Audit" mode)

### TOOLKIT 6: NOVA AI (Platform Intelligence)
- Nova Chatbot (persistent sidebar/overlay, not a separate page)
- API Portal

## Navigation Structure (Left Sidebar, Semrush-style)
```
[Nova Logo]
PLAN        > Media Plan | Social Plan | A/B Testing | CreativeAI
INTELLIGENCE > Competitive | Market Pulse | Heat Map | SkillTarget | VendorIQ
OPTIMIZE    > Budget Simulator | Campaign Tracker
TALENT      > HireSignal | PayScale Sync | ApplyFlow
COMPLIANCE  > ComplianceGuard
---
[Nova AI] (persistent chat)
[API Portal]
[Settings]
```

## Dashboard (/hub) -- Ahrefs-style
1. Active Campaigns Widget
2. Budget Health (spend vs plan)
3. Market Signals (latest trends)
4. Compliance Score
5. Quick Actions ("Create Plan", "Run Audit", "Check ROI")
6. Recent Activity feed

## Products Merged (7 merges)
- Quick Plan + Quick Brief --> Media Plan Generator (as modes)
- Market Intel Reports --> Market Pulse (as tab)
- ROI Calculator --> Budget Simulator (as tab)
- Post-Campaign --> Campaign Tracker (as tab)
- Ad Audit --> ComplianceGuard (as mode)
- Nova --> persistent overlay (not standalone page)

## Implementation Phases
1. Navigation Shell (1-2 weeks) -- sidebar + layout frame
2. Merge Quick Products (1 week) -- 24 down to 20 pages
3. Merge Paired Products (1 week) -- 20 down to 18 pages
4. Nova AI Overlay (1-2 weeks) -- persistent sidebar
5. Dashboard Widgets (1-2 weeks) -- live data on /hub
6. Polish (ongoing) -- role-based views, favorites, search

## Key Patterns from Semrush/HubSpot
- Toolkit picker in left sidebar (one expanded at a time)
- Shared campaign/client context bar at top
- Projects system (all tools share campaign context)
- "Use in Plan" cross-toolkit data transfer buttons
- Progressive disclosure via contextual tooltips
