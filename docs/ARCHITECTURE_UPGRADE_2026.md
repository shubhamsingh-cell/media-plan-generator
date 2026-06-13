# Nova AI Suite — Architecture Upgrade Plan (2026)

**Author:** Claude (S89 architecture deep-dive)
**Date:** 2026-06-13
**Scope:** Data architecture, information fetching (APIs/MCP), and the LLM→output
pipeline for the **Media Plan Generator** and **Nova chatbot**.
**Grounded in:** live Supabase schema inspection (project `trpynqjatlhatxpzrvgt`)
+ code review of the production pipeline.

---

## 0. TL;DR — the one finding that matters most

The product has a **real first-party campaign-performance warehouse it does not
use.** The Supabase project carries:

| Table | Rows | What it is |
|---|---:|---|
| `cg_daily_raw` | **520,771** | Daily post-level performance (media_cost, impressions/clicks/applies, by date/location/title/category/template), Dec 2025 → May 2026 |
| `cg_benchmarks` | **6,175** | Aggregated real outcomes: avg cost, avg applies, avg profit %, avg multiplier, sample size — by client / location / title / day-of-week. 72 titles × 412 locations, last updated 2026-05-12 |
| `cg_action_plans` | 1,502 | Generated action plans |

**The Media Plan Generator and Nova reference _none_ of these tables** (grepped:
zero `cg_*` references in product `.py`). Plans are instead built from a
**39-row** `channel_benchmarks` table + static JSON KB estimates.

> **The single highest-leverage upgrade in this entire plan is to ground the
> budget engine and Nova's benchmark answers in `cg_benchmarks` real outcomes
> when the requested title/location matches — with graceful fallback to the
> static estimates.** This turns "industry-estimate media plan" into "media plan
> calibrated to Joveo's own measured campaign results." That is a defensible,
> world-class differentiator no competitor can copy without the data.

Caveat to size honestly: `cg_benchmarks` currently covers **2 clients**, 72
titles, 412 locations. It is high-trust where it overlaps the request and silent
where it doesn't — exactly the shape that suits a "use-real-when-available,
fall-back-to-estimate" accessor.

---

## 1. Current-state architecture (as built)

### 1.1 Data layer
- **Two coexisting stores with overlap (no declared source of truth):**
  - **Static JSON KB** — 66 files in `data/`, ~1 MB, loaded into memory by
    `kb_loader.py` (explicit section→file map, hot-reload daemon, freshness +
    vintage warnings). ~72 live sections.
  - **Supabase Postgres** — `knowledge_base` (129), `channel_benchmarks` (39),
    `salary_data` (300), `compliance_rules` (8), `market_trends` (16),
    `vendor_profiles` (752), `supply_repository` (1,423). Plus Nova runtime
    tables (`nova_conversations` 194, `nova_memory` 325, `cache` 11,
    `nova_generated_plans` 30, `nova_conversation_state` 17) and the unused
    `cg_*` warehouse above.
  - **Overlap examples:** channel benchmarks live in `channel_benchmarks`
    (table) *and* `ad_benchmarks_recruitment_2026.json` / `benchmark_registry`
    (files); supply lives in `supply_repository` (table, 1,423 rows) *and*
    `joveo_global_supply_repository.json` (file — note: older docs claim
    "10,238 publishers"; the table holds 1,423). Salary, compliance, market
    trends likewise dual-homed.
- **3-tier cache** (`data_orchestrator.py`): L1 in-memory (500-key) → L2 Upstash
  Redis → L3 Supabase `cache` table. Solid; **no automatic invalidation.**
- **`benchmark_registry.py`** overlays `live_market_data.json` on static
  benchmarks — but that file is **static** (loaded at startup/per-call); there
  is no pipeline that refreshes its underlying data from a live source.
- **Enrichment payload is an untyped `dict`** threaded enrich → synthesize →
  budget → validate → generators. No schema contract; drift is uncaught.

### 1.2 Information fetching
- **~25 free public APIs** (`api_enrichment.py`) via `ThreadPoolExecutor` with
  per-call timeouts, circuit breaking, and a confidence score. Healthy design.
- **No MCP on either side:** the product neither consumes external MCP servers
  nor exposes its own. (The MCP servers in the dev environment are tooling, not
  product wiring.)
- Web research via Tavily (Firecrawl was removed in S72).

### 1.3 LLM → output
- **`llm_router.py`** — 27 providers (Haiku 4.5 primary, Sonnet 4.6, Opus 4.8
  fallback as of S89; ~22 free providers first), circuit breakers, health
  scoring, rate limiting, task classification. Strong resilience.
- **Nova RAG** (`vector_search.py`) — Voyage `voyage-3-lite` (512-dim) + BM25 +
  Reciprocal Rank Fusion + `rerank-2.5-lite`, on Qdrant. **Bottleneck:** Voyage
  free tier = **10 req/min, 6.5 s forced delay** between requests; the code
  itself notes a pending migration to `voyage-4-lite` (cheaper + better, but a
  different embedding space → full reindex required).
- **No structured outputs anywhere** — no `output_config.format`; all LLM JSON
  is parsed ad-hoc with retries. Reliability risk + wasted retry cost.
- **Nova "streaming" is simulated** — it chunks a fully pre-computed response,
  not true token streaming.
- **MPG generation is a fixed pipeline** (enrich→synthesize→budget→validate→
  render); the LLM augments narrative but does not orchestrate.
- `eval_framework.py` + golden datasets exist but **do not gate** deploys.

---

## 2. The upgrade program (three layers)

### Layer 1 — Data architecture
| # | Item | Why | Needs user? |
|---|---|---|---|
| **L1.0** | **Wire products to `cg_*` warehouse** (keystone) | Plans grounded in real Joveo outcomes, not estimates | No (read-only accessor; user later decides breadth/expansion) |
| L1.1 | Live benchmark refresh + cache invalidation | `live_market_data.json` never refreshes | **Yes** — pick sources + keys |
| L1.2 | Declare source-of-truth per domain (KB vs Supabase) | Two stores drift | **Yes** — ratify ownership |
| L1.3 | Typed pipeline schema (pydantic) | Catch drift; testable | No |
| L1.4 | End-to-end provenance in deliverables | Every figure → source/vintage/confidence (S89 KB work already tags the data) | No |

### Layer 2 — Fetching / MCP
| # | Item | Why | Needs user? |
|---|---|---|---|
| L2.1 | **Expose Nova/MPG as an MCP server** | Products become a callable platform (generate_media_plan, get_benchmark, query_supply) | Build: no · Expose/auth: **yes** |
| L2.2 | Consume external data MCPs | Replace bespoke integrations with maintained connectors | **Yes** — choose + authorize |

### Layer 3 — LLM → output
| # | Item | Why | Needs user? |
|---|---|---|---|
| L3.1 | Structured outputs (`output_config.format`) | Schema-guaranteed JSON, fewer retries | No |
| L3.2 | Voyage `voyage-4-lite` upgrade + remove RPM cap | Removes Nova's #1 search bottleneck | **Yes** — paid tier + reindex creds |
| L3.3 | True Nova streaming | Real per-token UX, interruptible | No |
| L3.4 | Eval gating in CI | Stop silent quality regressions | No |
| L3.5 | Agentic media-plan generation | LLM orchestrates engine/benchmarks/validators as tools | **Yes** — sign off design first |

---

## 3. Recommended sequencing

Ordered by **(value ÷ effort) × independence**, with dependencies noted.

**Phase A — Foundations & reliability (no user input; do now)**
1. **L3.1 Structured outputs** — small, high-ROI, de-risks every other LLM change.
2. **L1.3 Typed pipeline schema** — the contract everything else builds on;
   prerequisite for clean provenance (L1.4) and warehouse wiring (L1.0).
3. **L1.4 Provenance in deliverables** — leverages the S89 KB tagging + L1.3.

**Phase B — The intelligence leap (keystone)**
4. **L1.0 Wire the `cg_*` warehouse** — depends on L1.3 (clean schema to merge
   real outcomes into). Build a cached `cg_warehouse` accessor → feed
   `budget_engine` CPC/CPA/apply-rate and Nova's benchmark tools when
   title/location matches; provenance marks the figure "Joveo measured."

**Phase C — Platform & speed (some need you)**
5. **L2.1 MCP server façade** — build now; flip exposure on once you pick auth.
6. **L3.2 Voyage upgrade** — *blocked on you* (paid tier + reindex). Highest
   Nova-speed win; I'll stage the migration + reindex script.
7. **L3.3 True streaming** — independent; pairs well with the Nova speed work.

**Phase D — Durability & ambition**
8. **L3.4 Eval CI gate** — lock in quality before the bigger bet.
9. **L1.1 Live refresh pipeline** — *blocked on you* (sources/keys). With L1.0
   done, "refresh" may largely mean re-aggregating `cg_daily_raw` on a schedule.
10. **L1.2 Source-of-truth ratification** — *decision*; I'll bring a proposal
    from the schema (recommendation below).
11. **L3.5 Agentic generation** — *design + sign-off first*; the capstone.
12. **L2.2 External data MCPs** — opportunistic.

---

## 4. Decision register — what I need from you

| Ref | Decision / resource | Recommendation |
|---|---|---|
| L3.2 | Paid Voyage AI tier + Qdrant reindex creds | Approve — biggest Nova-speed unlock |
| L1.1 | Which live sources + API keys for refresh | After L1.0, prefer re-aggregating `cg_daily_raw` on a cron over re-scraping |
| L1.2 | System-of-record per data domain | **Recommend:** Supabase = source of truth for benchmarks/supply/salary/compliance (it's queryable + has the `cg_*` warehouse); JSON KB = curated/editorial + offline fallback. Stop dual-writing the same numbers. |
| L2.1 | MCP server exposure + auth | Internal-first (API-key gated), partner-facing later |
| L3.5 | Sign-off on agentic-generation design | I'll deliver a design doc before building |
| L2.2 | External data MCPs to adopt | Defer until L2.1 ships |
| L1.0 | Warehouse breadth | Currently 2 clients — more client data = more coverage; product works fine with fallback meanwhile |

---

## 5. Risks & mitigations
- **Warehouse coverage is partial (2 clients).** → Use-real-when-matched,
  fallback-to-estimate; never block a plan on warehouse data; label provenance.
- **Source-of-truth migration could regress reads.** → Dual-read with parity
  checks before cutover; keep JSON as fallback.
- **Voyage reindex is a hard cutover (embedding space changes).** → Reindex into
  a new Qdrant collection, verify recall on the golden set, then swap.
- **Agentic generation is a real architecture change.** → Design doc + eval gate
  (L3.4) in place first; ship behind a flag.

---

## 6. Status as of this writing (S89, 2026-06-13)
Already shipped & live this cycle: Excel numeric/totals/freeze overhaul, scorecard
/PDF P0 image fixes, PPTX + MPG brand-color cleanup, Nova chat-UI fixes, KB
enrichment + gap-fill (4 files, cited 2026 data), Opus 4.8 bump. This doc covers
the **architectural** program that remains; it is tracked task-by-task in the
session task list (#2–#16).
