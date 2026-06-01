# SCOUT: New LLM/Platform Capabilities for Joveo Nova (2026-06)

**Date:** 2026-06-02 | **Scope:** Analysis only — no code modified.
**Subject:** Nova chatbot (`nova.py` ~27K lines, ~97 tools, Tier-3 layer) + `llm_router.py` (~4.5K lines, Claude Haiku 4.5 primary).
**Method:** Grepped actual code; verified each capability against live Anthropic docs (June 2026).

---

## What Nova already uses (grep evidence)

| Capability | Status | Evidence |
|---|---|---|
| **Prompt caching** | PARTIAL — 1 of 5 call sites | `nova.py:20349` (system), `:20472` (tools), beta header only at `:20565` |
| **Parallel tool calls** | YES | `nova.py:20664–20695` ThreadPoolExecutor, max 3 workers, dedupe at `:20621` |
| **Tier-3 planner** (separate Haiku call) | YES | `_run_planner_step` `nova.py:17499`; `_call_planner_llm` `:24298` (Haiku, 350 tok, 2.0s timeout) |
| **Manual citations** | YES | `_build_citations_block` `nova.py:24541` (regex/markdown, no source spans) |
| **Number verifier** | YES — pure Python | `nova.py:24603+` regex normalization, **no LLM call** |
| **Extended thinking** | NO | grep `thinking`/`budget_tokens` → 0 hits in nova.py |
| **Strict / structured JSON output** | NO | grep `response_format`/`json_schema`/`tool_choice` → 0 in nova.py (router has `tool_choice:"auto"` only, `:2725`) |
| **Batch API** | NO | 0 hits |
| **Native Citations API** | NO | manual block only |
| **Files API / PDF ingest** | NO | Nova has no document-upload→LLM path |

**Measured prompt sizes (load-bearing for cache math):**
- `get_tool_definitions` body: **119,577 chars ≈ 34,000 tokens** (89 tool schemas).
- `get_system_prompt` body: **27,081 chars ≈ 7,700 tokens** (core + extensions).
- **Static cacheable prefix per request ≈ 38,000–42,000 tokens** (system core + all 89 tool defs). This is sent on **every iteration of every chat turn** (up to 3 iterations).

**Verified pricing (Anthropic docs, June 2026):**
- Claude Haiku 4.5: **$1.00/MTok base input · $1.25/MTok 5m cache-write · $0.10/MTok cache-hit** · $5/MTok output. *(Cache hit = 10× cheaper than base input.)*
- Claude Opus 4.8: $5 base · $6.25 cache-write · $0.50 cache-hit · $25 output.
- 1M context: GA on Sonnet/Opus; **2× input + 2× output above 200K tokens**.
- Batch API: **50% discount**, async, 24h window.
- Citations API: `cited_text` **does not count toward output tokens**; "better citation reliability."
- Extended thinking: adaptive on 4.5+ models; min budget 1,024 tokens; interleaved-with-tools available.

---

## 1. Prompt caching — FIX THE GAPS (the #1 quick win)

**Already partially used**, but with three defects that leave most of the savings on the table:

**Defect A — beta header missing on 4 of 5 Anthropic calls.** The cache only activates when `cache_control` blocks AND the request reaches a cache-enabled path. The header `anthropic-beta: prompt-caching-2024-07-31` appears ONLY at `nova.py:20565` (main tool loop). It is **absent** at:
- `:20516` (deadline-forced synthesis) — reuses `system_content` but no header
- `:21135` (S23 synthesis-force) — reuses `system_content` (`:21130`) but no header
- `:20144` (Haiku micro-path)
- `:24368` (planner) — small, low value

> **Important nuance:** As of 2026 prompt caching is **GA** — the 2024-07-31 beta header is legacy and the docs note caching "no longer requires the beta header" on current models. So today the cache likely *does* fire on the synthesis calls too (they reuse the same `system_content` object with `cache_control`). **The real fix is verification, not just adding headers.** The `cache_read_input_tokens`/`cache_creation_input_tokens` are already logged (`nova.py:20590–20601`) — the action is to *read the logs* and confirm `cache_read` > 0 on iteration 2+ and on synthesis calls.

**Defect B — 1-hour TTL not used.** Default cache TTL is 5 minutes (`nova.py:20349` uses bare `{"type":"ephemeral"}`). For a 38–42K-token prefix that is identical across ALL users, switching to `{"type":"ephemeral","ttl":"1h"}` keeps the cache warm across the gaps between conversations. Cost: 1h cache-write is 2× base (one-time); but with Nova's traffic the prefix is re-read hundreds of times/hour, so the warm cache pays for itself many times over.

**Defect C — dynamic context ordering is correct** (`:20352` appends non-cached parts after the cached block) — no change needed. Good.

**Savings math (per chat turn, Haiku, conservative):**
- Without cache: ~40K static prefix × 3 iterations × $1.00/MTok = **$0.12/turn** on prefix alone.
- With warm cache (iter 2–3 + synthesis are cache hits): ~40K × $0.10/MTok for hits → prefix cost ≈ **$0.016/turn**.
- **~87% reduction on input-prefix cost**, plus **latency**: cache reads are processed far faster than re-tokenizing 40K tokens, shaving an estimated **0.5–1.5s per cached iteration** (material against Nova's <30s budget at `nova.py:20326`).

**Effort:** ~15 LOC (add `ttl:"1h"`; add header to `:20516`/`:21135` defensively; add a one-line metric assertion). **Breaking-change risk: LOW** — caching is transparent; if a cache write fails the API silently falls back to normal billing. **Verify via existing `_nova_metrics.record_claude_call` logs.**

---

## 2. Native Citations API — replaces fragile manual block (high value)

**Today:** `_build_citations_block` (`nova.py:24541`) post-hoc regexes `[n]` markers and appends a markdown "Sources" list from a `sources` set. It cannot guarantee the cited number actually came from the cited tool — it's string-matching, not grounding. The number-verifier (`:24603`) is a separate regex pass trying to compensate.

**Capability:** Anthropic's Citations API returns `cited_text` spans grounded to source documents/search-results, and **`cited_text` does not count toward output tokens** (verified in docs). Tool results can be passed as `search_result` content blocks; the model emits citations tied to exact spans.

**Application point:** wrap tool results returned in the loop (`nova.py:20717+`, where `_claude_par` results become `tool_result` blocks) as citation-eligible content, and let the API emit grounded citations — then `_build_citations_block` becomes a fallback rather than the primary path.

**Benefit:** **reliability** (citations provably tied to source data → directly attacks the number-verifier's job and the "invented CPC/salary" risk that Rule 3 at `nova.py:4129` exists to prevent) + small **cost** win (cited spans are free output).

**Effort:** ~60–120 LOC (re-shape tool_result blocks; keep manual block as fallback). **Breaking-change risk: MED** — changes the tool-result message structure; must be gated behind a flag and A/B'd against the existing citation block. Keep the regex block as the safety net.

---

## 3. Strict / structured tool output — remove fragile parsing (moderate)

**Today:** the planner (`_call_planner_llm`, `nova.py:24360`) asks Haiku for "compact JSON … no markdown, no code fences" and then parses free text — a classic fragile path (any stray fence breaks it; it already swallows parse errors and returns `None` at `:24386`). No `tool_choice`/strict schema anywhere in nova.py.

**Capability:** "Strict tool use" (verified in Tools docs) guarantees the model's tool input matches your JSON schema. The planner's intent dict is a perfect fit — make the planner a single strict-schema tool call instead of a parse-the-text call.

**Application point:** `_call_planner_llm` (`nova.py:24298–24398`). Define the intent schema as a tool, set `tool_choice` to force it, read the structured `tool_use.input` directly — no `json.loads` on free text.

**Benefit:** **reliability** — eliminates planner parse failures (today silently degrades to "no hint"). Marginal quality lift because the planner is only a hint.

**Effort:** ~30 LOC. **Breaking-change risk: LOW** — isolated function, already fails safe to `None`, easy to A/B.

---

## 4. Batch API — move background jobs to 50%-cheaper async (cost only)

**Today:** enrichment and golden-eval run as background tasks (`api_enrichment.py`, `eval_framework.py` 56KB). These are NOT latency-sensitive.

**Capability:** Batch API = **50% discount**, 24h async window (verified). Stacks with prompt caching.

**Application point:** the **golden-eval suite** (`eval_framework.py`) and any bulk enrichment that re-summarizes via LLM. **NOT** the chat path — batch is async, useless for live turns.

**Benefit:** **cost** — ~50% on eval/enrichment LLM spend. Zero user-facing impact.

**Effort:** ~80–150 LOC (submit batch, poll, collect). **Breaking-change risk: LOW** (offline jobs only) but **moderate effort** for low strategic value — **defer unless eval/enrichment LLM spend is material.**

---

## 5. Extended thinking — DO NOT adopt for the chat path (honest "no")

**Tempting:** replace the separate Haiku planner (`_run_planner_step`) with native adaptive thinking on complex queries.

**Why it's NOT worth it for Nova:**
- Nova's hard constraint is the **<30s chat budget** (`nova.py:20326`, `_LOOP_BUDGET_C` `:20480`). Extended thinking *adds* latency (thinking tokens are generated before the answer) and **counts toward `max_tokens`** as a strict limit on 4.5+ models — it fights the deadline-aware loop directly.
- The planner is already cached in-process (`_intent_cache_get` `nova.py:17521`) and capped at 2.0s (`:24282`); a thinking pass cannot beat a cache hit.
- Thinking helps multi-step *reasoning*, but Nova's hard part is *tool orchestration*, which the parallel tool loop already handles.

**Verdict:** Skip on chat. Only candidate: an **offline** deep-analysis product (e.g., RFP teardown) where latency is irrelevant. **Risk if forced into chat: HIGH** (timeout regressions).

---

## 6. 1M context window — DO NOT replace RAG (honest "no")

**Tempting:** stuff the entire KB instead of `_bounded_vector_search(top_k=3)` (`nova.py:20433`).

**Why it's NOT worth it:**
- 1M context bills **2× input above 200K tokens**. Loading a multi-MB KB every turn would cost dollars/turn vs. the current cents — the opposite of the caching win.
- Nova already does **hybrid RAG** (vector + the 22+ KB files indexed) with a 3s bound. Phase-2 RAG covers the retrieval use case far more cheaply.
- Long-context quality **degrades** ("lost in the middle") — the docs explicitly warn about this. Top-k retrieval of the *right* 3 snippets beats 200K of mostly-irrelevant tokens.

**Verdict:** Keep RAG. 1M context is the wrong tool here. The narrow exception is the offline RFP/document-analysis product (see #8), where one large document genuinely exceeds chunking benefits. **Risk if adopted for chat: HIGH** (cost blowout + quality regression).

---

## 7. Memory tool — marginal over existing `nova_memory.py` (low priority)

Nova already injects cross-session memory (`nova.py:20446–20453`) and conversation memory (`:20359`). Anthropic's server-side Memory tool would offload this to the API but **duplicates working infrastructure** and adds a new dependency/failure mode. **Not worth the swap** while `nova_memory.py` works. **Risk: MED** (replaces a working subsystem). Skip.

---

## 8. Files API / PDF support — NEW capability, not an upgrade (optional)

Nova has **no** document-upload→LLM path (grep confirmed). The Files API + PDF support would enable a genuinely new feature: "analyze this RFP/brief" — upload a PDF, have Nova extract requirements and draft a media plan. This is **net-new product surface**, not a reliability/cost upgrade to the existing chatbot. **Effort: HIGH** (upload route, file lifecycle, new prompt flow). **Risk: LOW** (additive, isolated). **Defer to a roadmap decision** — out of scope for "upgrade the working chatbot."

---

## Prioritized table — top 5 by (impact ÷ risk)

| # | Capability | Impact | Effort (LOC) | Break risk | Impact÷Risk | Action |
|---|---|---|---|---|---|---|
| **1** | **Prompt caching: verify + 1h TTL** | **~87% prefix cost ↓, ~0.5–1.5s/iter ↓** | **~15** | **LOW** | **HIGHEST** | **Do now** |
| 2 | Strict structured planner output | Reliability ↑ (no parse fails) | ~30 | LOW | HIGH | Do next |
| 3 | Native Citations API | Reliability ↑↑ (grounded sources) | ~60–120 | MED | MED-HIGH | A/B behind flag |
| 4 | Batch API for eval/enrichment | ~50% offline cost ↓ | ~80–150 | LOW | MED | Defer unless spend material |
| 5 | Files API → RFP analysis (net-new) | New product | HIGH | LOW | MED (additive) | Roadmap decision |

**Explicitly NOT worth it:** extended thinking on chat (HIGH timeout risk), 1M-context-stuffing (RAG already covers it cheaper, HIGH cost/quality risk), Memory tool swap (duplicates working `nova_memory.py`).

---

## ⭐ Single highest-confidence quick win

**Verify prompt caching is actually firing on every Claude call, and set `ttl:"1h"` on the static prefix** (`nova.py:20349`).

- The ~40K-token static prefix (7.7K system + 34K tool defs) is sent on every iteration of every turn.
- Caching is GA in 2026; Nova *has* the `cache_control` blocks but only proves it works on one call site, uses the short 5-min TTL, and the synthesis calls (`:20516`, `:21135`) lack the legacy header.
- The metrics to confirm this **already exist** (`cache_read_input_tokens` logged at `nova.py:20590–20601`) — the first step is literally reading those logs to confirm `cache_read > 0`, then flipping the TTL.
- **~15 LOC, LOW risk** (caching falls back silently on failure), **~87% input-prefix cost reduction + sub-second latency per cached iteration**, verifiable from existing logs.

---

## Sources

- Anthropic — Prompt caching (pricing table: Haiku 4.5 $1 base / $0.10 cache-hit; Opus 4.8 $5/$0.50; 5-min default + 1h TTL at 2×; min cacheable length): `docs.anthropic.com/en/docs/build-with-claude/prompt-caching` (retrieved 2026-06-02)
- Anthropic — Context windows (1M token capacity; 2× pricing >200K; long-context degradation warning): `.../context-windows` (retrieved 2026-06-02)
- Anthropic — Extended thinking (adaptive 4.5+; min 1,024 budget; counts toward max_tokens as strict limit; interleaved-with-tools): `.../extended-thinking` (retrieved 2026-06-02)
- Anthropic — Tool use overview (Strict tool use; Parallel tool use; Tool use with prompt caching): `.../tool-use/overview` (retrieved 2026-06-02)
- Anthropic — Citations (cited_text not counted as output; better reliability; search_result blocks): `.../citations` (retrieved 2026-06-02)
- Anthropic — Batch processing (50% discount, async, 24h): `.../batch-processing` (retrieved 2026-06-02)
- Code: `nova.py` (lines cited inline), `llm_router.py:875–913` (model config)
