# MCP Server Integration Matrix

**Nova AI Suite -- 3-Module Consolidated Platform**
**Date**: 2026-03-24
**Total MCP Servers**: 22 (18 working, 4 missing keys)

---

## Module Mapping

### Command Center (Campaign Planning, Budget Optimization, Compliance)

| MCP Server | Status | Role in Module | Integration Notes |
|---|---|---|---|
| Supabase | Active | Campaign persistence, data cache, user preferences | Primary database for all CRUD operations |
| PostHog | Active | Campaign analytics, funnel tracking | Track campaign creation, plan generation, export events |
| Sentry | Active | Error monitoring, self-healing | Catch campaign generation failures, LLM routing errors |
| Render | Active | Deployment management | Deploy campaign template updates, monitor uptime |
| GitHub | Active | Version control, CI/CD | Track changes to campaign logic, templates |
| Todoist | Available | Task management for campaign workflows | Could track campaign review/approval tasks |
| Google Drive (no key) | Unavailable | Campaign document export | Export media plans directly to Google Drive |
| Notion (no key) | Unavailable | Campaign documentation | Sync campaign briefs with Notion workspace |

### Intelligence Hub (Market Analysis, Competitor Scanning, Talent Mapping)

| MCP Server | Status | Role in Module | Integration Notes |
|---|---|---|---|
| Context7 | Active | Library documentation lookup | Get current API docs for data source integrations |
| DeepWiki | Active | Deep research on companies/industries | Power competitor scanning and market research |
| US-Gov-Open-Data | Active | Federal data access (BLS, Census, BEA) | Direct access to government economic datasets |
| Chroma | Active | Vector store for market intelligence | Store and query market research embeddings |
| RAG-Memory | Active | Persistent research context | Remember cross-session research findings |
| Apify (no key) | Unavailable | Web scraping at scale | Scrape competitor career pages, job boards, pricing |
| Qdrant (no key) | Unavailable | Vector database for embeddings | Production-grade vector search for market intel |

### Nova AI (Chat, Actions, Context Management)

| MCP Server | Status | Role in Module | Integration Notes |
|---|---|---|---|
| Sequential Thinking | Active | Multi-step reasoning for complex queries | Power chain-of-thought analysis in chat |
| ElevenLabs | Active | Text-to-speech, speech-to-text | Voice interface for Nova AI chat |
| Playwright | Active | Browser automation for actions | Execute web-based actions from chat commands |
| Playwright-MS | Active | Alternate browser automation | Fallback browser automation |
| Excalidraw | Active | Diagram generation | Create visual diagrams from chat requests |
| Resend | Active | Email delivery | Send campaign reports, alerts via email from chat |
| Serena | Active | Code analysis and refactoring | Analyze codebase from chat commands |

---

## Status Summary

### Actively Used (18)
1. **Supabase** -- Primary database (all modules)
2. **PostHog** -- Analytics events (all modules)
3. **Sentry** -- Error tracking and self-healing (all modules)
4. **Render** -- Deployment management
5. **GitHub** -- Version control and CI/CD
6. **Context7** -- Documentation lookups
7. **Sequential Thinking** -- Multi-step reasoning
8. **DeepWiki** -- Deep research
9. **US-Gov-Open-Data** -- Federal data APIs
10. **ElevenLabs** -- TTS/STT voice interface
11. **Playwright** -- Browser automation
12. **Playwright-MS** -- Alternate browser automation
13. **Chroma** -- Vector embeddings
14. **RAG-Memory** -- Persistent memory
15. **Resend** -- Email delivery
16. **Serena** -- Code analysis
17. **Excalidraw** -- Diagram generation
18. **Todoist** -- Task management

### Missing API Keys (4)
1. **Apify** -- Web scraping at scale
2. **Qdrant** -- Production vector database
3. **Google Drive** -- Document export
4. **Notion** -- Documentation sync

---

## Integration Opportunities

### High-Impact Integrations

| MCP Server | Opportunity | Module | Effort | Impact |
|---|---|---|---|---|
| Apify | Scrape competitor career pages, job board pricing, salary data at scale. Replace manual scraping in firecrawl_enrichment.py. | Intelligence Hub | Medium | High |
| Qdrant | Replace in-memory vector store in vector_search.py with production Qdrant. Persistent embeddings survive restarts. | All modules | Medium | High |
| Google Drive | Auto-export generated media plans, reports, and analytics to client Google Drive folders. | Command Center | Low | Medium |
| Notion | Sync campaign briefs, research reports, and platform documentation with Notion workspace. | Intelligence Hub | Low | Medium |
| PostHog | Expand from basic events to full product analytics: funnels, cohorts, feature flags per module. | All modules | Medium | High |
| Chroma | Extend beyond KB indexing to store user query patterns, creating a learning feedback loop. | Nova AI | Medium | Medium |

### Cross-Module Synergies

1. **Supabase + PostHog**: Track module usage in nova_module_usage table AND PostHog for correlated analytics
2. **Qdrant + RAG-Memory**: Qdrant for production vector search, RAG-Memory for cross-session context
3. **Apify + DeepWiki**: Apify scrapes raw data, DeepWiki provides structured analysis
4. **ElevenLabs + Sequential Thinking**: Voice-driven multi-step campaign planning
5. **Sentry + Render**: Sentry detects errors, auto-triggers Render redeployment for self-healing

---

## Missing Key Priority Ranking

| Priority | MCP Server | Why | Estimated Effort | Estimated Cost |
|---|---|---|---|---|
| 1 | **Qdrant** | Replaces fragile in-memory vector store; enables persistent semantic search across restarts | Sign up at qdrant.tech, get cloud API key | Free tier: 1GB |
| 2 | **Apify** | Unlocks scalable web scraping for Intelligence Hub; current scraping limited to Firecrawl/Jina | Sign up at apify.com, create API token | Free tier: $5/mo credits |
| 3 | **Google Drive** | Enables one-click export of media plans to Drive; high user-facing value | Google Cloud Console OAuth setup | Free |
| 4 | **Notion** | Nice-to-have for documentation sync; lower priority since docs can live in-app | Notion integration setup | Free |

---

## Architecture Notes

- All MCP servers connect via the Claude Code agent's MCP protocol
- Server-side code (app.py) does NOT directly call MCP servers -- they are dev/agent tools
- Production integrations (Supabase, Sentry, PostHog, Render) have dedicated Python modules
- Browser automation (Playwright) is used for testing and development, not production
- Vector search (Chroma/Qdrant) is accessed via vector_search.py which uses Voyage AI HTTP API
