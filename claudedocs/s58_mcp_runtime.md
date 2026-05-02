# S58 — MCP Runtime Audit (Nova AI Suite)

**Date**: 2026-04-24
**Scope**: `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/` (production Nova)
**Method**: read-only codebase grep for MCP protocol signatures

## Verdict (one line)

**Zero (0) MCPs are runtime-wired into Nova today. MCPs add no value to the shipped product — they are 100% dev-session tooling.**

## Counts

| Category | Count | Evidence |
|---|---|---|
| MCPs reaching end-user output in prod Nova | **0** | No `mcp.ClientSession`, `stdio_client`, `FastMCP`, `model_context_protocol`, `from mcp` imports anywhere in `*.py` |
| MCPs available to Claude Code dev session | ~40+ | Listed in user memory; deferred-tool registry shows ruflo, context7, firecrawl, playwright, chrome-devtools, notion, slack, gmail, etc. |
| Python files matching `mcp` (case-insensitive) | 3 | `app.py`, `resilience_router.py`, `slack_alerts.py` — all **string/comment** matches, not imports |
| `requirements.txt` entries for MCP | 0 | Only: openpyxl, python-pptx, matplotlib, reportlab, google-api-python-client, sentry-sdk, pytrends, python-jobspy, Pillow, chromadb, cryptography, supabase, gunicorn, gevent |

## Evidence: the 3 Python "mcp" hits are labels, not protocol

- `app.py:228` — comment: `# SLACK ALERTS MCP INTEGRATION` (it's a webhook POST via `urllib.request`)
- `app.py:243` — comment: `# CALENDAR SYNC MCP INTEGRATION` (it's Google Calendar REST API via `google-api-python-client`)
- `app.py:11787,20370` — comment: `# ── Chroma RAG MCP ──` (it's direct `chromadb` Python SDK calls)
- `resilience_router.py:1087` — string literal `"Gamma MCP"` (a provider name in a routing table)
- `slack_alerts.py:1` — docstring says `"MCP integration"` (it's `urllib.request` to a Slack webhook)

None of these are MCP protocol clients. They are SDK/HTTP integrations that the team informally branded "MCP" in comments. The naming is misleading but the runtime behavior is plain HTTP/SDK.

## How Nova actually reaches external services at runtime

All runtime integrations use direct SDK or stdlib HTTP — **no MCP middleware**:

| Service | Mechanism | Confirmed in |
|---|---|---|
| 23 LLM providers (Haiku, Gemini, GPT-4o, Groq, etc.) | `urllib.request` | `llm_router.py` (imports: stdlib only — `urllib.request`, `json`, `threading`) |
| Supabase (cache, conversations, plans) | `supabase` SDK | `requirements.txt`; `nova.py` |
| PostHog | `urllib.request` | per MEMORY.md: "PostHog use stdlib urllib.request (no pip packages needed)" |
| Upstash Redis | `urllib.request` | requirements.txt comment confirms this |
| Sentry | `sentry-sdk` | requirements.txt |
| Slack | `urllib.request` → webhook | `slack_alerts.py` |
| Google (Calendar/Ads/Maps/Slides/etc.) | `google-api-python-client` | requirements.txt |
| Chroma RAG | `chromadb` Python SDK | requirements.txt |
| Job scraping (LinkedIn/Indeed/etc.) | `python-jobspy` | requirements.txt |
| Trends | `pytrends` | requirements.txt |

## Why the user's memory says "40+ MCP servers configured"

That number is correct — **but it refers to MCP servers registered in the user's Claude Code client (`~/.claude/`), not in the Nova server process**. Two separate runtimes:

1. **Claude Code dev session** (local, on the user's machine): ~40+ MCP servers loaded via the deferred-tool registry — ruflo, context7, firecrawl, playwright, chrome-devtools, notion, slack, gmail, supabase MCP, sketchfab, nano-banana, etc. These are tools **Claude (the AI assistant)** can call while coding/debugging. They never execute in production.

2. **Nova production server** (gunicorn on Render.com): A Python WSGI app serving end users. It calls vendor APIs directly (SDK + urllib). It has no MCP client library, no stdio subprocess spawning, and no `mcp://` connections.

The confusion likely comes from:
- Comments in `app.py` labeling SDK integrations as "MCP INTEGRATION" (inaccurate branding)
- MEMORY.md listing MCPs alongside Nova infrastructure in the same document
- The file `MCP_SERVERS_GLOBAL_RESEARCH_2026.md` at repo root is a research doc about MCPs, not an implementation

## Implication

- MCP server count is **not** a useful proxy for Nova's production capabilities.
- The 40+ MCPs accelerate the user's *development velocity* (Claude can call Slack, Notion, Playwright, Supabase, etc. while building features) but do not appear in the deployed product's dependency graph.
- If the goal is to evaluate shipped-product capability, the relevant inventory is: **22+ data APIs, 23 LLM providers, Supabase (31 tables), Chroma vector store, PostHog, Sentry, Upstash Redis, Slack webhooks, Google APIs** — all called via SDK/HTTP, all visible in `requirements.txt` and `*.py` imports.

## Files reviewed (read-only)

- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/requirements.txt`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/nova.py` (imports only, lines 18-29)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/llm_router.py` (imports only, lines 57-71)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/app.py` (grep "mcp" — comment/label hits only)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/resilience_router.py` (grep "mcp" — 1 string literal)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/slack_alerts.py` (grep "mcp" — 1 docstring)
