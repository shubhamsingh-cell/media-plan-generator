# S58 — CTO Strategic Review: Nova AI Suite

**Reviewer posture:** CTO of Anthropic, reviewing a B2B recruitment intelligence platform built on the Anthropic API.
**Reader:** Joveo executive — P&L owner for 40-50% of Joveo revenue.
**Date:** 2026-04-24
**Evidence base:** S56 data-utilization audit, S56 security audit, S56 self-healing check, structural grep of `nova.py` (23,949 lines), `app.py` (20,953 lines), `llm_router.py` (4,251 lines), file inventory of `data/` (84 JSON files).
**Read time:** 10 minutes.

---

## The 3 sharpest findings (read these even if you skip the rest)

1. **The platform lies to users in its own transparency panel.** `_SOURCE_TO_KB_FILES` at `nova.py:22619-22685` displays filenames like `salary_benchmarks_detailed_2026.json` to the user as "consulted sources" — but grep shows zero code paths ever open that file. 39 of 54 KB files have zero read sites. If a pharma or regulated-industry customer audits the chat log and sees a file cited that was never read, you have not a bug but a **compliance incident and a fraud-adjacent trust failure**. This single fix is the highest-leverage work in the codebase.

2. **"23 LLM providers" is marketing fiction; the business runs on Haiku.** Of 23 entries in `PROVIDER_CONFIG` (`llm_router.py:526-826`), three handle essentially all traffic (Haiku, Gemini, GPT-4o). The other 20 are complexity tax — circuit breakers, rate limiters, health scores, env vars — with no corresponding risk reduction. Worse, the routing theater is expensive to maintain and is the primary reason `llm_router.py` is 4,251 lines. A CTO would ship the same reliability in <500 lines with 4 providers.

3. **The moat is narrower than the roadmap implies, and the wrong things are being 10x'd.** Claude.ai with a system prompt, the Joveo publisher JSON, and BLS/Adzuna as tools replicates ~70% of the chatbot in a weekend. The real moats — 350-partner healthcare supply map (`data/healthcare_supply_map_us.json`), joveo_publishers proprietary data, and the PPT/Excel generation pipeline that executives actually hand to clients — are underinvested. The chatbot is cosmetic to those moats; the chatbot shouldn't be the product, it should be the thin surface over the real asset.

Everything below is evidence for these three claims and the implications.

---

## 1. Architectural sustainability — can this scale 10 → 10,000 users without a rewrite?

**Short answer:** No, not without surgical extraction of three modules. Yes, if you do that extraction in the next 6 months.

### The monolith facts
- `nova.py`: **23,949 lines**, 233 top-level + indented function definitions, **138 lazy `from X import Y` statements inside function bodies** (grep `^\s+from|^\s+import` = 138). Lazy imports at this scale are a circular-import workaround, not a performance technique.
- `app.py`: **20,953 lines** — a single `BaseHTTPRequestHandler` subclass with every route, auth check, rate limiter, and response hook inlined.
- `api_enrichment.py`: **16,315 lines**. `data_synthesizer.py`: 4,948. `ppt_generator.py`: 9,164. `excel_v2.py`: 6,981. `llm_router.py`: 4,251.
- Project CLAUDE.md rule: "Keep files under 500 lines." **nova.py exceeds this by 48x.**

### What compounds
| Debt vector | Current cost | Cost at 10× scale |
|---|---|---|
| Lazy-import sprawl in nova.py | 5-10s first-request penalty (S56 self-healing note) | Still 5-10s, but now every cold start under autoscaling multiplies it across dozens of instances |
| Single 20K-line request handler (`app.py`) | Changes to one route require reading the whole file | Merge conflicts, impossible code review, no safe parallel work |
| Duplicate KB loader (`kb_loader.py` vs `nova.py:3698-3761`) | 20 MB RAM waste across 4 workers | 200 MB per instance at scale; will OOM on the Standard tier |
| 23-provider PROVIDER_CONFIG | Circuit-breaker state bloat, 17 env vars managed | Every new deployment env has to handle all 17; any missing key still gets a "provider down" alert |
| `api_cache/` 213 stale files (S56 audit) | Disk growth, no eviction | Reaches Render's disk quota in months at 10× users |
| Python threads with no cancellation (`nova.py:119-177`) | Under Voyage throttling, threads accumulate (S56 security #9) | Guaranteed OOM during a demo where Voyage hits its rate limit |

### What scales fine today
- **The LLM circuit-breaker mesh** (`circuit_breaker_mesh.py` wired from `llm_router.py:2067-3673`) is genuinely production-grade. It's the only self-healing subsystem that sees real traffic (S56 self-healing verdict).
- **Data matrix monitor** does real 12h probes and re-imports modules when they fail. Verified running (check #1 today: 38 ok, 0 error, 7 partial).
- **Supabase persistence** for `nova_conversations`, `plan_events`, `metrics_snapshot` is clean.
- **Thread-safe KB loader** (`kb_loader.py:302-640`) is well-written.

### The verdict
The codebase can go from 10 to ~500 users without engineering pain. Past 500, three things break:
1. `app.py` becomes un-reviewable (it already is; only one human understands it).
2. `nova.py` import graph becomes a blocker on any structural change — every tool fix risks cascading breakage because 138 functions import from each other lazily.
3. Render's Standard tier RAM (2 GB) gets exhausted by duplicate KB copies + stale cache + idle provider state.

**A CTO would budget one eng-quarter to split `app.py` into `routes/*.py` properly (15 files, <1000 lines each) and extract nova.py's tools into `tools/*.py` (~30 files). This work is straightforward, safe (tests exist), and saves the next three years.**

---

## 2. Economic model — per-request cost and whether the 23-provider chain ever pays for itself

### Unit economics
`TASK_CONVERSATIONAL` routes to Haiku first (`llm_router.py:888-905`). Claude Haiku 4.5 pricing: **$0.25/M input, $1.25/M output** (per llm_router.py:10 comment; the file treats Haiku as "cheap enough to justify").

Typical chat turn (observed from `_compute_max_turns` logic, `nova.py:452`):
- Input: 4-8K tokens (system prompt + KB snippets + history + user message + tool schemas)
- Output: 300-800 tokens
- Per-turn cost: **$0.0016 - $0.0032 on Haiku** (call it ~$0.002 average)

Plan generation (TASK_PLAN_NARRATIVE + TASK_PLAN_STRUCTURED):
- Input: 30-60K tokens (full KB + synthesized research + user inputs)
- Output: 2-4K tokens
- Per-plan cost: **$0.008 - $0.020 on Haiku** (~$0.015 average)

### Traffic scenarios
| Scenario | Monthly spend on Anthropic |
|---|---|
| $0/mo usage (demo only, 200 chats, 50 plans) | **$1.15** |
| $100/mo usage (~5K chats, 300 plans) | **~$15** |
| $1K/mo usage (~40K chats, 3K plans) | **~$125** |
| $10K/mo usage (~400K chats, 30K plans) | **~$1,250** |

**Observation:** At the $10K/mo usage scenario you're paying Anthropic ~$1.2K. That's a 12.5% COGS line for the LLM — reasonable but not cheap. The gross margin on this product (ignoring infra, ignoring the builder's time) is ~88%. That's SaaS-grade.

### Does the 23-provider fallback ever save money?
**Almost never, and its maintenance cost exceeds its savings.** Here's the math:

- Haiku uptime over the last 12 months (per Anthropic public incident log): ~99.9% availability on the Messages API.
- Of the 0.1% downtime, transient 5xx resolved by retry covers >95%. Remaining ~0.005% would fall through to Gemini.
- Gemini 2.5 Flash is free. So in the rare fallback, the user's cost goes from ~$0.002 to $0. That's **not savings — it's degraded quality at 99.995% availability events**.
- For the remaining fraction after Gemini, you fall to GPT-4o ($2.50/M input — more expensive than Haiku), then to 20 "free tier" providers that often return worse answers and frequently 404 on wrong model IDs (per llm_router.py:530-533 S53 fix note).

**Conclusion: the fallback chain below the top 3 is complexity theater.** It does not save money; it increases maintenance cost; it ships with known fragility (model-ID drift); and it creates fake reliability metrics (circuit breakers showing "green" on providers that have never actually been called — S56 self-healing: `total_successes=0` across all resilience_router tiers).

A CTO would ship: **Haiku (primary) → Gemini 2.5 Flash (free backup) → GPT-4o (paid backup)**. Three providers. Delete the other 20. Net impact on user experience: ~0. Net impact on eng velocity: meaningful.

---

## 3. Moats & defensibility — what couldn't Claude.ai + a weekend of API calls replicate?

**Let's be honest.** Most of the surface area of Nova can be replicated in 1-2 weekends by any recruitment analyst with a Claude.ai Pro subscription and basic Python:

| Nova capability | Weekend-replicable on Claude.ai? |
|---|---|
| Chat interface for recruitment questions | **Yes** — system prompt + 3 web searches |
| BLS/Adzuna/FRED/ONET API calls | **Yes** — these are free public APIs |
| "Insight" synthesis ("here's the CPC for RNs in NYC") | **Yes** — Claude does this natively with web search |
| PPT/Excel generation | **Partial** — python-pptx/openpyxl are trivial; the 6-sheet template is the asset |
| 3-layer cache, 23-provider routing, auto-QC | **No, but so what** — Claude.ai already does this behind the scenes for free |
| The "AI" part of the platform | **Yes** — this is literally the Anthropic API |

### The real moats (ranked by defensibility)

1. **Joveo's publisher relationships and historical performance data.** `data/joveo_publishers.json` encodes publishers by category and country, backed by real Joveo billing relationships. This is **the only thing no competitor can replicate on day one**. It's not the 6 entries in that JSON that matter — it's that Joveo's GSCM/supply team can update it weekly with actual performance, CPC negotiation leverage, and blackout dates.

2. **The 350-partner healthcare supply map** (`data/healthcare_supply_map_us.json` — confirmed 350 partners via `json.load` → `partners` list length). This is hand-curated domain knowledge — RN supply by MSA, specialty-to-board mapping, rate cards. If this is kept current, it is a **genuine 1-2 year moat**. If it's static, it's a 6-month moat. Today it is unknown when it was last refreshed.

3. **The PPT/Excel pipeline** (`ppt_generator.py` 9,164 lines, `excel_v2.py` 6,981 lines). The fact that a Joveo exec can send a client-branded 10-slide deck in 90 seconds is **not a technical moat but a workflow moat**. Competitors have to hire consultants to produce this. That saves Joveo account managers 4-6 hours per client pitch. This is real, measurable, defensible.

4. **100 client media plans** — **this is a myth** (S56 audit confirmed). The actual count is 7, all aerospace/defense (RTX variants + BAE + Peraton + Rolls-Royce + Amazon CS India). The "100 client plans" narrative in MEMORY.md is false. If you make this real (ingest real historical plans from whatever Joveo BI system holds them), it becomes the strongest moat on the list — because a model tuned on Joveo's actual plan-to-outcome data is genuinely differentiated.

5. **54 KB files** — **most of this is not a moat**. 39 of the 54 are cold (never read; S56 audit). Of the 15 read, ~8 are public-domain benchmarks that anyone can buy or scrape. The moat-worthy files are `joveo_publishers.json`, `healthcare_supply_map_us.json`, `expanded_supply_repo` (via `joveo_global_supply_repository.json` at 2.7 MB), and `client_media_plans_kb.json` **if** the 7 real plans become 100 real plans.

### What this means
The **chatbot is the wrong primary surface**. The chatbot is cosmetic — table stakes in 2026 — and doesn't encode Joveo's unfair advantage. **The media plan generator is the real product**, because it packages the defensible assets (publisher relationships, healthcare map, client plans) into an output that competitors cannot easily match.

If I were framing this externally (to an investor, an acquirer, or an internal promotion committee), I would lead with the plan generator and treat the chatbot as a feature of it, not its peer.

---

## 4. The "data utilization lie" — how bad is this from a trust + compliance standpoint?

**Very bad. In a regulated-industry audit, this is the one thing that stops the sale.**

### The mechanic
`_SOURCE_TO_KB_FILES` at `nova.py:22619-22685` is a dict mapping human-friendly labels (e.g. "salary benchmarks") to filenames (e.g. `salary_benchmarks_detailed_2026.json`). When Nova returns a chat response, `_map_sources_to_kb_files` at `nova.py:22688` echoes back the "matched" filenames as `kb_files_queried` in the response metadata.

**The problem:** if the LLM mentions "salary benchmarks" in its citation list (because the prompt told it salary data exists in the KB), the transparency panel tells the user `salary_benchmarks_detailed_2026.json` was consulted — **even though no code path opened that file during this request**. The file exists on disk, but `grep kb.get("salary_benchmarks_detailed")` returns **0 reads** (S56 data audit, section "The 'Transparency panel' lie").

The number quoted to the user came from the LLM's prior training, or a hallucinated value, or a distant KB chunk retrieved by vector search — **not** from the named file.

### The compliance implications
- **Pharma / healthcare / regulated industries:** The FDA, HIPAA, and SOC 2 all require that claims about data provenance be accurate. A customer audit that finds "we cited X, but X was never opened by the code during that request" is a **material misstatement**. This fails the audit.
- **Financial services:** Fiduciary duty + SEC + Reg BI. Citing a source you didn't consult is prohibited.
- **Procurement at F500 companies:** Most enterprise procurement decks require a "data lineage" certification. Nova cannot sign this today.
- **GDPR Article 22 (automated decision-making):** The user has the right to an explanation of the data used. Giving them a false list of sources violates the spirit, arguably the letter, of Article 22.

### What makes this worse
- It's performative — the system *looks* transparent. That's **worse than no transparency**, because it builds false trust.
- It's easy to detect — any savvy user can click "Why this answer?" and compare with the actual numbers.
- It's trivial to fix — log the *actual* files read per-turn and populate `kb_files_queried` from that log. 1-2 days of work.

### The severity ranking a CTO would assign
**P0-compliance.** Not in the security sense (no attacker can exploit it directly), but in the business-continuity sense — **one enterprise procurement review will kill the deal**, and the blast radius is "every pitch to regulated industries." This outranks the P0 auth bugs in the S56 security audit by business impact, because the auth bugs are fixable before any attacker notices, but this is **already being shown to every user today** and a single screenshot from a savvy prospect ends the conversation.

### The fix (non-negotiable, do before any sales call to a regulated industry)
1. In each tool handler, append the actual file(s) touched to a request-scoped list (pass via `threadlocal` or explicit arg).
2. At response time, populate `kb_files_queried` from that list only — not from `_SOURCE_TO_KB_FILES`.
3. Delete all entries in `_SOURCE_TO_KB_FILES` that don't correspond to a real read site.
4. Add a CI check (`scripts/check_kb_integrity.py`) that fails the build if any `_SOURCE_TO_KB_FILES` value is unreachable via `grep kb.get|_data_cache.get`.

---

## 5. Competitive position — who builds this, what would make it 10x better

### The competitive set
| Competitor | What they ship | Weakness |
|---|---|---|
| **Appcast** (Stepstone) | Programmatic job ad optimization, CPC bidding, ~$50M ARR | No chat surface; UI is ops-engineer-grade, not exec-grade; no "narrative" output |
| **Joveo sales team (internal)** | Human-produced plans using Excel + client calls | Slow (days), expensive, non-scalable, but high-trust |
| **LinkedIn Talent Insights** | LinkedIn-native supply/demand analytics | Locked to LinkedIn data; no cross-channel; $30K/seat |
| **ZipRecruiter ZipIntel** | Market data + ATS integration for SMB | US-only; no enterprise polish; no plan artifacts |
| **Claude.ai + Anthropic docs + a Python notebook** | Everything you can do manually | No productization, no saved state, no client-branded deliverables |
| **Horsefly Analytics / Greenhouse Insights** | HR analytics + candidate supply | Expensive; no plan generation; no chat |

### Where Nova competes today
- **Vs. Appcast**: Nova wins on executive deliverables (PPT/Excel). Appcast wins on programmatic bidding execution. **Not directly competitive** — Appcast is an ops tool, Nova is a planning tool.
- **Vs. internal Joveo sales team**: Nova is a **productivity multiplier for them**, not a competitor. This is Nova's strongest positioning.
- **Vs. LinkedIn TI / ZipIntel**: Nova is cheaper and more comprehensive (22+ APIs vs. 1). But LinkedIn TI has the LinkedIn data moat that Nova cannot match.
- **Vs. Claude.ai + weekend**: This is the **real threat**. A customer asking "why pay Joveo for Nova when I can ask Claude directly" is already a real question. The answer has to be: "because we encode our supply graph, our publisher relationships, and our historical client outcomes — none of which Claude has." **Today, that answer is 60% true.** The healthcare supply map and joveo_publishers are real. The client plans are not (7, not 100). The benchmarks are half-loaded (39/54 cold).

### What would make it 10x better, not 10%

A 10% improvement adds UI polish, more benchmarks, more tools. A 10x improvement changes what the product *is*.

1. **Plan → Execution loop.** Today Nova generates a plan. Tomorrow Nova should **ship the plan into JAX (Joveo's ad platform) and measure performance weekly**. The difference between "here's your plan" and "here's your plan, we launched it, here's how it performed vs. forecast" is the difference between a report and a product. This is the only way to defend against Claude.ai.

2. **Proprietary data flywheel.** Every plan produces an outcome. Every outcome should feed `client_media_plans_kb.json` (and Supabase tables) so the next plan is better than the last. **Today this loop is broken** — `plan_events` is a write-only log, nothing reads it back into the generator. The architecture is there (`plan_events.py`, `outcome_engine.py`, `outcome_pipeline.py` all exist) but nothing ties outcomes to future plan quality.

3. **The healthcare vertical as a standalone product.** The 350-partner healthcare map is genuinely differentiated. Spinning this into a "Joveo Healthcare Intelligence" product with its own pricing, its own UI, and its own sales motion is probably a **$10-30M ARR opportunity** distinct from the general platform. The general platform is crowded; the healthcare vertical is not.

4. **Real embeddings, real RAG.** Today: 685 Qdrant vectors. Real: ingest every job posting Joveo has ever served (tens of millions), every client outcome, every publisher's historical performance. Voyage AI at 512-dim × 10M docs = 20 GB. Cheap on Qdrant. Would dwarf any competitor's retrieval quality.

5. **Kill the chatbot as a peer product; promote it to a feature of the plan generator.** The chatbot's 83 tools (per MEMORY.md S48) is a liability — most are cold (S56 audit: many one-call-site integrations). A 10-tool chatbot focused on "iterate on your plan" beats an 83-tool chatbot focused on "answer any recruitment question."

---

## 6. Top 5 moves in 6 months with the current team

Assume the current team is 1 builder (Shubham) + occasional QC/review help. Six months = ~24 engineering weeks.

### Move 1 — Week 1-3: Ship the compliance fix (the data-utilization lie)
- Rewire `kb_files_queried` to come from per-turn read tracking.
- Delete all dead entries in `_SOURCE_TO_KB_FILES`.
- Add CI check.
- **Why first:** Every week this ships to enterprise prospects, the risk of one savvy procurement review killing a deal grows. This is a cheap fix for an expensive exposure.
- **Outcome:** Nova becomes defensible in a regulated-industry audit.

### Move 2 — Week 4-6: Close the P0 auth bugs (S56 security audit #1-4)
- HMAC-signed session cookie replacing the forgeable `nova_user_email`.
- Verify Supabase JWTs with `SUPABASE_JWT_SECRET`, not base64-decode them.
- Add auth + Origin check to `/ws/chat`.
- Replace substring Origin/Referer matching with exact hostname match.
- **Why second:** These are trivial to exploit ("curl with fake email burns your Anthropic credits"). A bored attacker or a hostile competitor could 10x your LLM bill in a week. Three of the four bugs are <1 day each.
- **Outcome:** Nova stops being a free public endpoint for the internet.

### Move 3 — Week 7-14: Refactor the monolith (targeted, not top-down)
- Extract `app.py` routes into `routes/*.py` (pattern already started — 15 files exist in `routes/` — just finish it).
- Extract `nova.py` tool definitions into `tools/*.py` (~30 files, ~400 lines each).
- Delete duplicate KB loader — one loader, one `_data_cache`.
- Delete the 39 cold KB entries from `KB_FILES` (or, for the 10 most valuable, wire them to real tools).
- Trim `PROVIDER_CONFIG` from 23 to 4 providers.
- **Why third:** This unlocks everything else. Until nova.py is under 3K lines, every new feature takes 3x longer than it should. Two months of refactoring buys the next 2 years of velocity.
- **Outcome:** A new engineer can read the code in a week.

### Move 4 — Week 15-20: Build the plan → outcome flywheel
- Wire `plan_events` writes to trigger a weekly rollup into `nova_generated_plans` performance.
- Ingest JAX platform outcomes (impression, click, apply, hire counts) back into the plan KB.
- Train a lightweight Haiku-based reranker on plan-channel-outcome triples.
- **Why fourth:** This is the only real long-term moat. Claude.ai cannot copy your outcome data.
- **Outcome:** Every new plan is measurably better than the last. That is a product story investors (and internal promotion committees) can credit.

### Move 5 — Week 21-24: Spin out Joveo Healthcare Intelligence as a standalone
- Take `healthcare_supply_map_us.json` (350 partners), `healthcare_specialty_pay_2026.json`, `partner_specialty_crosswalk.json`, plus healthcare-relevant BLS/OES calls.
- Ship a dedicated UI (`/healthcare` route, not a tab inside the chat).
- Separate pricing. Separate sales motion. Separate Linear project.
- **Why fifth:** This is a growth bet. Healthcare is the largest recruitment vertical by spend. Joveo owns real data there. A focused product beats a general platform for enterprise sales.
- **Outcome:** A second revenue line, with defensibility the general platform can't achieve.

### What gets archived
- `resilience_router.py` (S56: zero production callers; dashboard shows `total_successes=0`).
- `elevenlabs_integration.py` (988 lines, returns "not available" to users).
- `google_bigquery_integration.py` (399 lines, 1 call site), `google_cloud_storage.py` (304 lines, no production call site), `google_vision_integration.py` (271 lines, 1 call site in `file_processor.py:109`).
- `google_ads_analytics.py` (494 lines, only reachable via health check).
- 18 of 23 entries in `PROVIDER_CONFIG`.
- 39 of 54 `KB_FILES` entries.
- `api_cache/` 213 stale files (add cron eviction).
- The "MCP servers (40+ total)" marketing claim — these are dev tools, not product capabilities.

Total: **~8,000 lines of dead or half-dead code.** Deletion is work, but the velocity dividend is permanent.

### What gets 10x'd
- **Per-turn data-read logging** (transparency truth).
- **The PPT/Excel pipeline** (client-facing deliverables — the actual moat).
- **Healthcare supply map freshness** (from static to live-updated weekly).
- **Plan → outcome → better-plan flywheel** (the flywheel is the moat).
- **Joveo publisher data depth** (this is the hardest thing for a competitor to acquire).

---

## 7. Kill criteria — when to sunset or spin out

**Sunset the chatbot product if:**
- Quarterly chat-turn volume fails to 3x YoY. The chatbot scales with usage; without usage growth, its 83 tools and 23 providers are pure complexity tax.
- Haiku pricing rises 5x (unlikely but would make per-turn economics require premium positioning).
- Joveo's BI team ships a competing internal tool and internal users migrate — at that point the chatbot is a duplicate.
- The data-utilization lie (Finding #4) is not fixed within 90 days of this memo. If it ships to a regulated customer and generates a formal complaint, the reputational cost exceeds any remaining platform value.

**Spin the Healthcare Intelligence product into a separate P&L if:**
- Healthcare pipeline ARR exceeds general platform ARR. (Check in Q4.)
- Joveo Healthcare account executives can name the product by its own name within 6 months — a marker that it has its own identity.
- The 350-partner supply map passes 500 partners with <6 month update cadence — a proof of content moat depth.

**Sunset the media plan generator if:**
- Plans-per-month drops below 50 after 12 months. This is a leading indicator that the client-facing deliverable isn't pulling its weight.
- A new Joveo GTM motion (e.g. AI-first client services) obsoletes pre-generated plans in favor of live collaborative sessions.

**Kill the whole Nova AI Suite if (all of the following):**
- Revenue attribution from Nova is < $500K ARR after 18 months.
- The builder (Shubham) moves to a different charter without a replacement — the bus factor is currently 1.
- Anthropic / Google / OpenAI ship a vertical recruitment product that bundles plans, chat, and data for free with existing enterprise contracts. This is <24 months away. The defense is the Joveo data flywheel (Move #4); without it, survival window shrinks.

---

## Bottom line (5 sentences)

Nova AI Suite is **a real product with real moats that are being systematically under-invested in**, wrapped in an architectural monolith that will stop scaling at ~500 users and a transparency layer that actively deceives users. The engineering debt is expensive but surgically addressable in one eng-quarter. The economic model is fine — at $10K/mo usage, Anthropic COGS are ~12%; gross margin ~88% — but the 23-provider fallback chain is maintenance theater that saves no money. The real moats (Joveo publisher data, 350-partner healthcare map, PPT/Excel pipeline) are 10x more valuable than the chatbot they live under, and the next 6 months should promote them — chatbot becomes a feature, plan generator becomes the flagship, healthcare intelligence becomes a standalone P&L. **The #1 fix is the data-utilization lie — ship that in the next 3 weeks or stop pitching to regulated industries.**

---

## Evidence index (absolute paths)

- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/nova.py` — 23,949 lines, 138 lazy imports
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/app.py` — 20,953 lines
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/llm_router.py` — 4,251 lines, 23 providers
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/api_enrichment.py` — 16,315 lines
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data/healthcare_supply_map_us.json` — 350 partners, confirmed via `json.load`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data/client_media_plans_kb.json` — **7 plans**, not "100s"
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data/joveo_publishers.json` — publisher moat, 6 top-level keys (totals + categories + country breakdowns)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/kb_loader.py` — `KB_FILES` dict (54 keys), 39 of which are cold per S56 audit
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/claudedocs/s56_data_utilization_audit.md` — source for the transparency-lie finding
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/claudedocs/s56_security_audit.md` — source for the P0 auth and SSRF findings
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/claudedocs/s56_self_healing_check.md` — source for the "resilience router has zero callers" and "auto-QC deadlock" findings
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/docs/Nova_AI_Suite_Product_Capabilities.md` — marketing claims to cross-check against reality
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/docs/Nova_AI_Suite_Technical_Architecture.md` — claims of "23 providers", "685 Qdrant vectors"
