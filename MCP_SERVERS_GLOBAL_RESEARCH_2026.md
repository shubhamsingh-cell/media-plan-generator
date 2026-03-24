# Global MCP Server Research -- March 2026

Exhaustive research on all publicly available MCP (Model Context Protocol) servers with free tiers.
Compiled for Nova AI Suite project evaluation.

**Research Date:** 2026-03-24
**Sources:** Official MCP Registry, Smithery, PulseMCP, mcpservers.org, mcp-awesome.com, GitHub awesome lists, vendor docs
**Ecosystem Size:** 7,000+ MCP servers cataloged across registries (TensorBlock count as of May 2025)

---

## Registries and Directories

| Registry | URL | Notes |
|----------|-----|-------|
| Official MCP Registry | https://registry.modelcontextprotocol.io | Backed by Anthropic, GitHub, Microsoft. API at v0.1 freeze. |
| Smithery | https://smithery.ai | Hosted + local servers. CLI installer. Semantic search. |
| PulseMCP | https://www.pulsemcp.com | Curated listings with quality metadata |
| mcpservers.org | https://mcpservers.org | Web companion to wong2/awesome-mcp-servers |
| mcp-awesome.com | https://mcp-awesome.com | 1,200+ servers indexed |
| mcp.so | https://mcp.so | Community directory |
| Composio MCP | https://mcp.composio.dev | 500+ app integrations, 10,000+ tools via single endpoint |
| Apify MCP | https://apify.com/mcp | 7,000+ Actors as MCP tools |
| Glama | https://glama.ai/mcp/servers | Server discovery + quality ratings |
| MCPlane | https://mcplane.com | Server details + integration guides |
| FastMCP | https://fastmcp.me | Server details + alternatives |

---

## TIER A: HIGH VALUE -- ADD NOW

These servers provide significant capability uplift for Nova AI Suite with minimal setup.

### A1. US Government Open Data MCP
- **URL:** https://github.com/lzinga/us-gov-open-data-mcp
- **What:** 40+ US gov APIs, 250+ tools -- Treasury, FRED, BLS, BEA, Census, FEC, Congress, SEC, CDC, NOAA, USDA, and more
- **Free Tier:** 18 APIs need no key at all; rest use free keys (under 1 min to get)
- **Install:** `npx us-gov-open-data-mcp`
- **API Keys:** Most free; some require registration (FRED, BLS, Census -- you already have these)
- **Quality:** HIGH -- built-in caching, retry, rate limiting, cross-referencing, WASM sandboxed JS execution
- **Nova Value:** MASSIVE -- replaces your 8 separate API clients in api_integrations.py with a single unified MCP server. Covers FRED, BLS, BEA, Census, USAJobs and adds 30+ more data sources. This is the single highest-value addition.

### A2. Apify MCP Server
- **URL:** https://github.com/apify/apify-mcp-server
- **What:** 7,000+ web scrapers/automation Actors -- social media, search engines, maps, e-commerce, job boards
- **Free Tier:** Free plan available (no credit card required). Includes compute units for running Actors.
- **Install:** `npx @apify/actors-mcp-server` or remote via `https://mcp.apify.com`
- **API Keys:** APIFY_TOKEN (free registration)
- **Quality:** HIGH -- production-grade, OAuth support, Docker available
- **Nova Value:** HIGH -- LinkedIn scraping, Google Maps data extraction, Instagram/Facebook scraping for social media plans, Google SERP scraping for competitive intel, job board scraping across Indeed/LinkedIn/Glassdoor. Replaces Firecrawl for many structured extraction tasks.

### A3. Notion MCP Server (Official)
- **URL:** https://github.com/makenotion/notion-mcp-server
- **What:** Full Notion workspace access -- pages, databases, blocks, search. Read + write.
- **Free Tier:** Notion free plan + MCP server is free/open-source
- **Install:** Hosted at Notion or self-host via npx
- **API Keys:** Notion OAuth (free)
- **Quality:** HIGH -- official, actively maintained, v2.0.0
- **Nova Value:** HIGH -- knowledge base management, client workspace integration, content planning, project documentation

### A4. Qdrant MCP Server (Official)
- **URL:** https://github.com/qdrant/mcp-server-qdrant
- **What:** Vector database as semantic memory backend. Tools: qdrant-store, qdrant-find.
- **Free Tier:** Qdrant Cloud free tier (1GB), or self-hosted (unlimited, open-source)
- **Install:** Docker or pip (`pip install mcp-server-qdrant`)
- **API Keys:** QDRANT_URL + optional QDRANT_API_KEY; needs embedding provider (OpenAI/local)
- **Quality:** HIGH -- official, Docker-ready, auto-embedding with sentence-transformers
- **Nova Value:** HIGH -- semantic search for Nova chatbot RAG, knowledge base embeddings, persistent memory across sessions. Upgrades nova_rag.py significantly.

### A5. ElevenLabs MCP Server (Official)
- **URL:** https://github.com/elevenlabs/elevenlabs-mcp
- **What:** TTS, voice cloning, audio transcription, sound effects generation
- **Free Tier:** ElevenLabs free tier: 10,000 characters/month TTS
- **Install:** `npx @elevenlabs/mcp-server`
- **API Keys:** ELEVENLABS_API_KEY (free registration)
- **Quality:** HIGH -- official, industry-leading voice quality
- **Nova Value:** HIGH -- powers nova_voice.py with production-grade TTS. Voice-enabled Nova chatbot for presentations and accessibility.

### A6. Excalidraw MCP Server (Official)
- **URL:** https://github.com/excalidraw/excalidraw-mcp
- **What:** Create hand-drawn diagrams, flowcharts, architecture diagrams via AI
- **Free Tier:** Completely free (open-source)
- **Install:** MCP App (works with Claude, VS Code, etc.)
- **API Keys:** None
- **Quality:** HIGH -- official from Excalidraw team
- **Nova Value:** HIGH -- auto-generate media plan diagrams, org charts, workflow visualizations, architecture diagrams for client presentations

### A7. Google Docs/Drive/Sheets MCP Server
- **URL:** https://github.com/a-bonus/google-docs-mcp
- **What:** Full Google Workspace access -- Docs (read/write/format), Sheets (read/write), Drive (search/manage)
- **Free Tier:** Free (uses Google Workspace free tier)
- **Install:** `npx google-docs-mcp`
- **API Keys:** Google OAuth (CLIENT_ID + CLIENT_SECRET + REFRESH_TOKEN)
- **Quality:** MEDIUM-HIGH -- community but well-maintained, 3 separate Google APIs
- **Nova Value:** HIGH -- direct Google Sheets export (replaces sheets_export.py complexity), Google Docs report generation, Drive file management for client deliverables

### A8. Todoist MCP Server
- **URL:** https://www.pulsemcp.com/servers/todoist
- **What:** Full Todoist REST API v2 + Sync API as MCP. Task management, projects, labels, batch ops.
- **Free Tier:** Todoist free plan + MCP is free/open-source
- **Install:** npx
- **API Keys:** TODOIST_API_TOKEN (free)
- **Quality:** MEDIUM-HIGH -- complete API coverage
- **Nova Value:** MEDIUM-HIGH -- project task tracking, client deliverable management, sprint planning integration

### A9. Playwright MCP Server (Microsoft Official)
- **URL:** https://github.com/microsoft/playwright-mcp
- **What:** AI-driven browser automation using accessibility tree (not pixels). Cross-browser.
- **Free Tier:** Completely free (open-source, MIT license)
- **Install:** `npx @playwright/mcp`
- **API Keys:** None
- **Quality:** HIGH -- Microsoft official, snapshot-based (fast, deterministic)
- **Nova Value:** MEDIUM-HIGH -- automated testing of Nova templates, competitor website analysis, screenshot generation for reports. Already have Playwright MCP but this is Microsoft's improved version.

### A10. RAG Memory MCP Server
- **URL:** https://github.com/ttommyth/rag-memory-mcp
- **What:** Knowledge graph + vector search + document processing. Hybrid retrieval. SQLite backend.
- **Free Tier:** Completely free (open-source, local)
- **Install:** pip or npx
- **API Keys:** None (uses local sentence-transformers)
- **Quality:** MEDIUM-HIGH -- active development, zero external dependencies
- **Nova Value:** HIGH -- persistent memory for Nova chatbot across sessions, semantic search over uploaded documents, knowledge graph for client data. Major upgrade to nova_persistence.py and nova_rag.py.

---

## TIER B: WORTH ADDING LATER

These provide good value but are lower priority or need more evaluation.

### B1. GitLab MCP Server (Official)
- **URL:** https://docs.gitlab.com/user/gitlab_duo/model_context_protocol/mcp_server/
- **What:** Full GitLab API access -- projects, MRs, issues, CI/CD, pipelines
- **Free Tier:** GitLab free tier + MCP server is free
- **Install:** Official GitLab package
- **API Keys:** GitLab personal access token (free)
- **Quality:** HIGH -- official GitLab product
- **Nova Value:** MEDIUM -- useful if you move repos to GitLab or need GitLab CI/CD integration

### B2. Linear MCP Server
- **URL:** https://mcp.composio.dev/linear
- **What:** Issue tracking, project management, sprint planning via AI
- **Free Tier:** Linear free for small teams + Composio free tier (20K calls/mo)
- **Install:** Via Composio MCP
- **API Keys:** Linear API key (free)
- **Quality:** MEDIUM-HIGH
- **Nova Value:** MEDIUM -- alternative to Jira for project tracking

### B3. HubSpot MCP Server (Official)
- **URL:** https://developers.hubspot.com/mcp
- **What:** CRM data access -- contacts, companies, deals, tickets, invoices. Read + write.
- **Free Tier:** HubSpot free CRM + MCP server is free
- **Install:** Remote MCP at `https://mcp.hubspot.com`
- **API Keys:** HubSpot OAuth
- **Quality:** HIGH -- official HubSpot, public beta
- **Nova Value:** MEDIUM -- CRM integration for client management, deal tracking, lead scoring

### B4. Stripe MCP Server (Official)
- **URL:** https://docs.stripe.com/mcp
- **What:** Stripe API access + knowledge base search. Payments, invoices, subscriptions.
- **Free Tier:** Stripe test mode is free; MCP server is free
- **Install:** Official Stripe package
- **API Keys:** STRIPE_SECRET_KEY
- **Quality:** HIGH -- official Stripe
- **Nova Value:** MEDIUM -- payment integration if Nova goes SaaS

### B5. Ahrefs MCP Server
- **URL:** Available via Ahrefs platform
- **What:** SEO data -- rankings, backlinks, content analysis, competitor tracking
- **Free Tier:** Ahrefs Webmaster Tools (free, limited); paid plans from $29/mo
- **Install:** Official Ahrefs connector
- **API Keys:** Ahrefs API key
- **Quality:** HIGH -- official
- **Nova Value:** MEDIUM -- SEO analysis for Nova website, competitive intelligence for clients

### B6. Google Search Console MCP Server
- **URL:** https://github.com/AminForou/mcp-gsc
- **What:** GSC data -- search analytics, URL inspection, sitemap management
- **Free Tier:** Completely free (GSC is free)
- **Install:** pip/npx
- **API Keys:** Google OAuth
- **Quality:** MEDIUM -- community maintained
- **Nova Value:** MEDIUM -- SEO monitoring for Nova website and client sites

### B7. Semrush MCP Server
- **URL:** https://github.com/mrkooblu/semrush-mcp or https://mcp.semrush.com/v1/mcp
- **What:** SEO + competitive data -- rankings, traffic analysis, keyword research
- **Free Tier:** Semrush free account (limited queries); official hosted MCP available
- **Install:** Self-hosted or official remote
- **API Keys:** Semrush API key
- **Quality:** MEDIUM-HIGH
- **Nova Value:** MEDIUM -- competitive analysis for recruitment advertising research

### B8. Datadog MCP Server (Official)
- **URL:** https://docs.datadoghq.com/bits_ai/mcp_server/
- **What:** Full observability access -- metrics, logs, traces, error tracking, feature flags, security
- **Free Tier:** Datadog free tier (5 hosts, limited retention)
- **Install:** Official Datadog package
- **API Keys:** Datadog API key + App key
- **Quality:** HIGH -- official, GA
- **Nova Value:** MEDIUM -- production monitoring upgrade from current Sentry-only setup

### B9. Grafana Cloud MCP Server
- **URL:** https://grafana.com/docs/grafana-cloud/monitor-applications/ai-observability/mcp-observability/
- **What:** MCP observability dashboards, distributed tracing via TraceQL
- **Free Tier:** Grafana Cloud free tier (10K metrics, 50GB logs, 50GB traces)
- **Install:** Via Grafana Cloud setup
- **API Keys:** Grafana Cloud API key
- **Quality:** HIGH -- official Grafana
- **Nova Value:** MEDIUM -- complements Sentry with metrics dashboards and tracing

### B10. Terraform MCP Server (HashiCorp Official)
- **URL:** https://github.com/hashicorp/terraform-mcp-server
- **What:** Terraform Registry APIs, workspace management, IaC automation
- **Free Tier:** Free (open-source, MPL-2.0)
- **Install:** npx or Docker
- **API Keys:** Optional HCP Terraform token
- **Quality:** HIGH -- official HashiCorp
- **Nova Value:** LOW-MEDIUM -- useful if infrastructure grows beyond Render

### B11. AWS MCP Servers (Official)
- **URL:** https://awslabs.github.io/mcp/
- **What:** AWS service management -- Workers, S3, Lambda, CloudFormation, etc.
- **Free Tier:** AWS free tier applies
- **Install:** Official AWS packages
- **API Keys:** AWS credentials
- **Quality:** HIGH -- official AWS Labs
- **Nova Value:** LOW-MEDIUM -- relevant if migrating from Render to AWS

### B12. Cloudflare MCP Server (Official)
- **URL:** https://fastmcp.me/mcp/details/742/cloudflare
- **What:** 2,500+ Cloudflare API endpoints -- DNS, Workers, R2, D1, Zero Trust
- **Free Tier:** Cloudflare free tier (100K Workers requests/day, 10GB R2)
- **Install:** npx or Cloudflare template
- **API Keys:** Cloudflare API token
- **Quality:** HIGH -- official
- **Nova Value:** MEDIUM -- CDN management, edge caching, R2 storage for assets

### B13. Clockify MCP Server
- **URL:** https://lobehub.com/mcp/aslamanver-mcp_clockify
- **What:** Time tracking -- entries, projects, tasks, workspaces
- **Free Tier:** Clockify free plan (unlimited tracking)
- **Install:** npx
- **API Keys:** CLOCKIFY_API_KEY (free)
- **Quality:** MEDIUM
- **Nova Value:** LOW-MEDIUM -- time tracking for project management

### B14. Composio MCP (Meta-Platform)
- **URL:** https://mcp.composio.dev
- **What:** 500+ app integrations through single MCP endpoint. Dynamic tool router.
- **Free Tier:** 20,000 tool calls/month free
- **Install:** Remote MCP endpoint or npx
- **API Keys:** Composio API key (free registration)
- **Quality:** HIGH -- well-funded startup, active development
- **Nova Value:** MEDIUM-HIGH -- single integration point for dozens of tools. Could replace multiple individual MCP servers.

### B15. MiniMax MCP Server
- **URL:** https://github.com/MiniMax-AI/MiniMax-MCP
- **What:** TTS, voice cloning, image generation, video generation (Chinese AI company)
- **Free Tier:** MiniMax free tier available
- **Install:** npx or pip
- **API Keys:** MiniMax API key
- **Quality:** MEDIUM-HIGH -- official from MiniMax (Shanghai)
- **Nova Value:** MEDIUM -- alternative TTS/image gen provider, good for cost optimization

### B16. Confluence MCP Server
- **URL:** Community-maintained (KS-GEN-AI/confluence-mcp-server)
- **What:** CQL queries, page content retrieval, wiki management
- **Free Tier:** Confluence free plan (10 users) + MCP is open-source
- **Install:** npx
- **API Keys:** Atlassian API token
- **Quality:** MEDIUM
- **Nova Value:** LOW-MEDIUM -- useful for enterprise client documentation

### B17. Obsidian MCP Server
- **URL:** https://github.com/cyanheads/obsidian-mcp-server
- **What:** Full vault management -- read/write/search notes, tags, frontmatter
- **Free Tier:** Completely free (Obsidian + MCP both free)
- **Install:** npx
- **API Keys:** None (local, uses Obsidian REST API plugin)
- **Quality:** MEDIUM-HIGH -- 26 tools, well-documented
- **Nova Value:** MEDIUM -- personal knowledge management, project notes integration

### B18. arXiv MCP Server
- **URL:** https://github.com/blazickjp/arxiv-mcp-server
- **What:** Search, retrieve, analyze academic papers from arXiv
- **Free Tier:** Completely free (arXiv is open access)
- **Install:** pip
- **API Keys:** None
- **Quality:** MEDIUM -- community maintained
- **Nova Value:** LOW-MEDIUM -- research papers for recruitment industry trends, labor economics

### B19. Paper Search MCP (Multi-Source Academic)
- **URL:** https://github.com/openags/paper-search-mcp
- **What:** 20+ academic sources -- arXiv, PubMed, Google Scholar, Semantic Scholar, SSRN, Crossref, OpenAlex, CORE, etc.
- **Free Tier:** Completely free (all open-access sources)
- **Install:** pip
- **API Keys:** None for most sources
- **Quality:** MEDIUM
- **Nova Value:** LOW-MEDIUM -- deep research for white papers and industry reports

### B20. Asana MCP Server
- **URL:** https://www.pulsemcp.com/servers/roychri-asana
- **What:** 80 tools -- tasks, projects, portfolios, goals, custom fields, teams
- **Free Tier:** Asana Basic (free for up to 10 users)
- **Install:** npx
- **API Keys:** Asana personal access token (free)
- **Quality:** MEDIUM-HIGH
- **Nova Value:** LOW-MEDIUM -- project management alternative

### B21. DevSecOps MCP Server
- **URL:** https://github.com/jmstar85/DevSecOps-MCP
- **What:** SAST + DAST + SCA in one MCP -- Semgrep, Bandit, OWASP ZAP, Trivy
- **Free Tier:** Free (all integrated tools are open-source)
- **Install:** Docker or pip
- **API Keys:** None
- **Quality:** MEDIUM -- detects 80+ vulnerability types
- **Nova Value:** MEDIUM -- automated security scanning for Nova codebase

### B22. Draw.io Diagram Generator MCP
- **URL:** https://www.pulsemcp.com/servers/simonkurtz-msft-drawio-diagram-generator
- **What:** Creates draw.io XML diagrams -- flowcharts, UML, ER, C4, BPMN, architecture, network
- **Free Tier:** Free (open-source)
- **Install:** npx
- **API Keys:** None
- **Quality:** MEDIUM
- **Nova Value:** MEDIUM -- alternative diagramming for technical documentation

### B23. Mermaid-to-Excalidraw MCP
- **URL:** https://github.com/yannick-cw/mermaid-to-excalidraw-mcp
- **What:** Converts Mermaid diagrams to styled Excalidraw files with semantic colors
- **Free Tier:** Free (open-source)
- **Install:** npx
- **API Keys:** None
- **Quality:** MEDIUM -- lightweight, focused
- **Nova Value:** MEDIUM -- quick diagram generation from text descriptions

---

## TIER C: NICHE -- ADD IF SPECIFIC NEED ARISES

### Communication & Messaging

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Twilio MCP | SMS/MMS/WhatsApp via Twilio API | Twilio trial ($15 credit) | npx | LOW -- notifications |
| Telegram MCP | Bot API, 35 tools, messaging + media | Free (Telegram is free) | npx | LOW -- bot channel |
| Discord MCP | Channel messaging, server management | Free (Discord is free) | npx | LOW -- community |
| MiniMail MCP | Unified email: Gmail, SendGrid, Resend, SES, Mailgun | Free (open-source) | npx | MEDIUM -- multi-provider email |
| SendGrid MCP | Email marketing + transactional via SendGrid v3 | SendGrid free (100/day) | npx | LOW-MEDIUM |

### Databases & Storage

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| PostgreSQL MCP Pro | Index tuning, explain plans, health checks, safe SQL | Free (open-source) | npx | MEDIUM -- DB optimization |
| Neon MCP | Serverless Postgres with branching | Free tier generous | npx | LOW -- already using Supabase |
| Turso MCP | SQLite at the edge, 5GB free | Free (500M reads/mo) | npx | LOW -- edge use case |
| Redis MCP (Official) | Cache/KV management, all data structures | Redis Cloud free (30MB) | npx | LOW -- already using Upstash |
| MongoDB Atlas MCP | Document DB + vector search | Free tier (512MB) | npx | LOW |
| Neo4j MCP | Graph database for knowledge graphs | Community edition free | pip | LOW-MEDIUM |

### Content & Social Media

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| YouTube MCP | Video management, Shorts, analytics, transcripts | Free (YouTube API free tier) | npx | LOW |
| Spotify MCP | Playlist management, music discovery | Free (Spotify API free) | npx | LOW |
| Pod Engine MCP | Podcast intelligence API | Free tier available | npx | LOW |
| Social Media Sync MCP | Cross-post to Twitter/Mastodon/LinkedIn | Free (open-source) | npx | LOW-MEDIUM |

### E-Commerce & Payments

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Shopify MCP | Order management, products, fulfillment | Shopify partner (free dev) | npx | LOW |
| WooCommerce MCP | Products, categories, reviews (read-only) | Free (open-source) | npx | LOW |

### Maps & Geolocation

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Baidu Maps MCP | Chinese map APIs, geolocation | Free tier available | npx | LOW |
| IP2Location MCP | IP geolocation, network info | Free tier (30K/mo) | npx | LOW |
| OpenStreetMap MCP | Open map data, geocoding | Free (open data) | npx | LOW-MEDIUM |

### DevOps & Infrastructure

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Kubernetes MCP | kubectl via AI, cluster management | Free (open-source) | npx | LOW |
| Docker/Portainer MCP | Container management via AI | Free (open-source) | npx | LOW |
| Argo CD MCP | GitOps deployment management | Free (open-source) | npx | LOW |
| Pulumi MCP | IaC with multiple languages | Free tier available | npx | LOW |
| CircleCI MCP | CI/CD pipeline management | Free tier (6K min/mo) | npx | LOW |
| Netlify MCP | Serverless deployment platform | Free tier available | npx | LOW |

### Patent & Legal

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| USPTO Patent MCP | 51 tools across 6 USPTO data sources | Free (public API) | pip | LOW |
| Korean Patent (KIPRIS) MCP | Korean patent search + analysis | Free (public API) | npx | LOW |

### Calendar & Scheduling

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Calendly MCP | Event scheduling, invitee management | Calendly free plan | npx | LOW |
| Toggl Track MCP | Time tracking, reports, timer control | Toggl free plan | npx | LOW |

### AI & ML Tools

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Replicate MCP | Run ML models via API | Free tier (some models) | npx | LOW-MEDIUM |
| Hugging Face MCP | Model inference, datasets | Free tier available | npx | LOW-MEDIUM |

### International / Non-US

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| Alibaba Cloud MCP | AnalyticDB, DataWorks, OpenSearch | Alibaba Cloud free tier | Various | LOW |
| Ant Group/Alipay MCP | Payment services for Chinese market | Via Alipay dev account | npx | LOW |
| data.gouv.fr MCP | French national open data platform | Free (open data) | npx | LOW |
| Swiss Open Data MCP | 68 tools: transport, weather, companies, parliament | Free (open data) | npx | LOW |
| OLX MCP | European marketplace scraping (5 domains) | Free (open-source) | npx | LOW |
| Korean DART MCP | Korean financial statements + disclosures | Free (public API) | npx | LOW |
| Korean Law MCP | Korean statutes, precedents, admin rules | Free (public API) | npx | LOW |

### Miscellaneous Niche

| Server | What | Free Tier | Install | Nova Value |
|--------|------|-----------|---------|------------|
| OpenGov/Socrata MCP | Open data from any Socrata portal | Free (no key needed) | npx | LOW-MEDIUM |
| Data.gov MCP | US gov open data portal search | Free (open data) | npx | LOW |
| Coupler.io MCP | 70+ integrations hub (Calendar, CRM, PM) | Free tier available | Remote | LOW-MEDIUM |
| Context Portal (ConPort) | Project knowledge graph, decisions, progress | Free (open-source) | npx | MEDIUM |
| Basic Memory MCP | Markdown-based semantic graph, local-first | Free (open-source) | pip | LOW-MEDIUM |
| CodeRabbit MCP | AI code review with cross-tool context | Free for open-source | npx | LOW-MEDIUM |
| OWASP ZAP MCP | Web app security scanning | Free (open-source) | Docker | LOW-MEDIUM |

---

## ALREADY HAVE (For Reference)

| Server | Status | Notes |
|--------|--------|-------|
| Figma MCP | ACTIVE | Design-to-code, screenshots, FigJam, Make files |
| GitHub MCP | ACTIVE | Repo management, issues, PRs, Actions |
| Firecrawl | ACTIVE | Web scraping, search, extraction, browser sessions |
| Slack MCP | ACTIVE | Channels, messages, search, canvas, users |
| Apollo MCP | ACTIVE | CRM, contacts, companies, sequences |
| Clay MCP | ACTIVE | Contact/company enrichment, Salesforce integration |
| Google Calendar MCP | ACTIVE | Events, free time, meeting scheduling |
| Granola MCP | ACTIVE | Meeting notes, transcripts, queries |
| Vercel MCP | ACTIVE | Deployments, projects, logs, toolbar |
| Gamma MCP | ACTIVE | Presentation generation |
| Kiwi MCP | ACTIVE | Flight search |
| Sentry | ACTIVE (custom) | Error tracking (via sentry_integration.py) |
| PostHog | ACTIVE (custom) | Product analytics (via templates) |
| Supabase | ACTIVE (custom) | Database (via supabase_data.py) |
| Playwright | ACTIVE | Browser automation (have both custom + MCP) |
| Scheduled Tasks | ACTIVE | Cron/one-time task scheduling |
| Claude Preview | ACTIVE | Dev server preview + inspection |
| Claude in Chrome | ACTIVE | Browser automation in Chrome |

---

## RECOMMENDED INSTALLATION ORDER

### Phase 1: Immediate (This Week)
1. **US Government Open Data MCP** -- consolidates your 8 API clients into one
2. **Qdrant MCP** -- semantic search for Nova RAG
3. **Excalidraw MCP** -- diagram generation for presentations
4. **RAG Memory MCP** -- persistent memory for Nova chatbot

### Phase 2: Next Sprint
5. **Apify MCP** -- structured web scraping (social, jobs, maps)
6. **ElevenLabs MCP** -- production TTS for Nova voice
7. **Notion MCP** -- knowledge base + project docs
8. **Google Docs/Drive MCP** -- Sheets/Docs/Drive integration

### Phase 3: Growth
9. **Composio MCP** -- meta-platform for 500+ tools
10. **HubSpot MCP** -- CRM if going enterprise
11. **DevSecOps MCP** -- automated security scanning
12. **Todoist MCP** -- task management integration

---

## KEY INSIGHTS

1. **Consolidation Opportunity:** The US Gov Open Data MCP alone can replace most of api_integrations.py (FRED, BLS, BEA, Census, USAJobs -- all in one server with better caching/retry than your custom code).

2. **Composio as Meta-Layer:** Instead of adding 10+ individual MCP servers, Composio provides 500+ integrations via a single endpoint with 20K free calls/month. Worth evaluating as a "one ring to rule them all" approach.

3. **Vector/RAG Stack:** Qdrant MCP + RAG Memory MCP together give Nova chatbot enterprise-grade semantic search + persistent memory -- a major upgrade from current nova_rag.py.

4. **Chinese Tech Adoption:** Alibaba, Tencent, Baidu, ByteDance, and Ant Group all have MCP servers. The protocol is becoming a global standard, not just a US one.

5. **Free Tier Reality:** Most MCP servers are open-source and free. The costs come from the underlying services (API rate limits, compute). Your existing API keys (FRED, BLS, Census, etc.) work directly with the Gov Data MCP.

6. **Security Consideration:** Academic research found 7.2% of open-source MCP servers contain general vulnerabilities and 5.5% exhibit MCP-specific tool poisoning. Stick to official/verified servers from the registries.

---

## COST ANALYSIS

| Server | Monthly Cost | Notes |
|--------|-------------|-------|
| US Gov Open Data MCP | $0 | All free APIs (you already have keys) |
| Qdrant MCP | $0 | Free tier 1GB or self-hosted |
| Excalidraw MCP | $0 | Fully open-source |
| RAG Memory MCP | $0 | Local SQLite, no API needed |
| Apify MCP | $0-49 | Free tier, then usage-based |
| ElevenLabs MCP | $0-5 | 10K chars/mo free |
| Notion MCP | $0 | Free plan |
| Google Docs MCP | $0 | Free (uses Google free tier) |
| Composio MCP | $0 | 20K calls/mo free |
| **Total Phase 1-3** | **$0-54/mo** | Mostly free |
