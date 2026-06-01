# Nova Suite — Upgrade Opportunities (June 2026)

**Author**: Claude (Opus 4.8), strategic research pass
**Method**: Live web research (mid-2026 market) cross-referenced against the *actual* installed stack (`llm_router.py`, `data/`, `deck_generator.py`, MEMORY.md).
**Constraint**: Recommendations are additive and risk-rated. **None applied yet** — a parallel agent is actively editing `app.py`/`nova.py`/`api_enrichment.py` + building RAG v2, so this is a plan, not a change.

---

## TL;DR — top 3 highest-value, lowest-risk

1. **Wire Deel Salary Insights + WageIndicator (free, global salary APIs)** → makes the international salary feature shipped this session *live* instead of static. Directly extends `intl_benchmark_lookup.py`. **Highest ROI, lowest risk.**
2. **Adopt programmatic tool calling (Opus 4.5+ capability)** for the Nova chatbot's multi-tool path → cuts latency on the 45s SLO and kills round-trip timeouts (the OECD 90s override becomes unnecessary). **Real performance win.**
3. **Add a 1M–2M context tier (Kimi K2.6 / MiniMax M2.7)** to the router for "whole-KB-in-context" Nova answers → reduces RAG dependence for big-context queries. **Strategic, additive.**

---

## What I verified about the current stack (so I don't recommend what you already have)

| Area | Already installed | Gap |
|---|---|---|
| LLM router | DeepSeek **V3.2**, Qwen3-235b, Qwen3-coder-480b, GLM-4.7, Kimi, Zhipu, Groq, Cerebras, SambaNova (23 providers) | **MiniMax absent**; Kimi/Qwen/GLM are ~1 minor version behind latest; no 2M-context tier wired |
| Embeddings/rerank | Jina (`JINA_API_KEY`, 10M tokens), Voyage rerank | Jina **Reranker v2** (100+ langs, function calling) not used; could replace Voyage at zero marginal cost |
| Salary/labor data | BLS, O*NET, Adzuna, FRED, static `intl_role_benchmarks_v1.json` | **No live global salary API** (Deel, WageIndicator, Levels.fyi) |
| Deck generation | Tiered: presenton, gamma, magicslides, alai, flashdocs + `python-pptx`/Slides | SlideSpeak (MCP-native PPTX) not a tier |

**Conclusion:** the LLM router is already excellent — adding more base models is *low* marginal value. The leverage is in **data freshness, retrieval quality, latency, and my own newer capabilities.**

---

## Tier 1 — do these first (high value · low risk · additive)

### 1.1 Live global salary APIs → supercharge the intl salary feature
- **What**: Add Deel Salary Insights (free, real-time global salary from payroll/contractor activity) and WageIndicator (free, non-profit, multi-country wages) as fallback sources behind `get_local_salary_summary()`.
- **Why**: This session shipped local-currency salary from a *static* dataset (`intl_role_benchmarks_v1.json`). These APIs make it *live* and expand country coverage well beyond the 15 dataset countries.
- **Integration point**: new `data_sources` collector → cache layer → `intl_benchmark_lookup.get_local_salary_summary()` as a freshness overlay (same pattern as `benchmark_registry._load_live_data()`).
- **Risk**: Low — additive, behind try/except + cache, with the static dataset as fallback. No schema change to existing callers.
- **Effort**: ~0.5 day. **Skill/agent**: `data-engineer`, `recruitment-supply-iq`.

### 1.2 Programmatic tool calling for Nova's multi-tool path
- **What**: Use Opus 4.5+ programmatic tool calling — the model writes code that calls your tools inside a container instead of one round-trip per tool.
- **Why**: Nova's chatbot fans out to many tools; each is a model round-trip today. Programmatic calling collapses that into one execution, cutting p50/p99 latency and removing the need for per-tool timeout hacks (the new OECD 90s override).
- **Integration point**: `nova.py` tool-dispatch loop (the `_PER_TOOL_TIMEOUT` / parallel executor section).
- **Risk**: Medium — it's a real change to the dispatch path. Gate behind a flag, A/B against the current path. **Do NOT touch while the parallel agent is in `nova.py`.**
- **Effort**: ~1–2 days. **Skill/agent**: `performance-engineer`, `ai-engineer`.

### 1.3 Jina Reranker v2 (you already pay for Jina)
- **What**: Switch the RAG rerank stage from Voyage to Jina Reranker v2 (100+ languages, function-calling, 6× faster than v1).
- **Why**: You already hold a `JINA_API_KEY` with 10M tokens; multilingual rerank aligns with the international push. One fewer vendor.
- **Integration point**: `rag_pipeline.py` rerank stage (the RAG v2 the parallel agent is building — coordinate, don't collide).
- **Risk**: Low–medium — swap one provider; keep Voyage as fallback. **Effort**: ~0.5 day. **Skill/agent**: `vector-database-engineer`.

---

## Tier 2 — strong value, moderate effort

### 2.1 2M-context tier for "whole-KB-in-context" Nova
- **What**: Add Kimi K2.6 (2M context) and/or MiniMax M2.7 (1M) to the router as a "huge-context" tier. Both are OpenAI-compatible and 15–30× cheaper than flagships.
- **Why**: For broad questions, you can put the *entire* relevant KB in-context and skip retrieval — higher recall, fewer "RAG missed it" failures. Complements (not replaces) RAG v2.
- **Integration point**: `llm_router.py` provider registry + a routing rule "if estimated context > N, use huge-context tier."
- **Risk**: Low (additive provider). **Effort**: ~0.5 day. **Skill/agent**: `llm-architect`.

### 2.2 SlideSpeak as a deck tier (MCP-native PPTX)
- **What**: Add SlideSpeak (native editable .pptx, auto charts, MCP workflow) to the existing deck tier ladder.
- **Why**: You already have a 5-tier deck fallback system; SlideSpeak is MCP-native and produces charts from structured data — a fit for the media-plan tables.
- **Risk**: Low (additive tier, behind env flag like the others). **Effort**: ~0.5 day.

### 2.3 `effort` parameter for cost control
- **What**: Opus 4.5+ supports an `effort` parameter to cap tokens per response. Apply on high-volume Haiku/Sonnet calls where terse output is fine (e.g., classification, intent detection).
- **Why**: Direct cost reduction with no quality loss on short-answer tasks.
- **Risk**: Low. **Effort**: ~2 hours.

---

## Tier 3 — strategic / watch list

- **Apify Job Market Analyzer** (no API key, 4 free sources, dedup + salary benchmarks) — a zero-setup enrichment source for competitive intel.
- **Levels.fyi / Coresignal** — tech-comp depth (Coresignal = 399M postings) if you expand beyond the current verticals.
- **Context compaction (Opus 4.6+)** — for long-running Nova sessions, automatic mid-session compaction keeps quality on multi-turn threads.
- **GLM-5.1 / Qwen 3.6 / Kimi K2.6 version bumps** — minor refreshes of models you already route to; low urgency.

---

## Explicitly do NOT do right now

- **No edits to `nova.py`, `app.py`, `api_enrichment.py`, `rag_pipeline.py`** — a parallel agent is mid-build (F4 ESCO + RAG v2, generating `ruvector.db`). Touching these now risks merge conflicts and broken state.
- **Don't commit the parallel agent's untracked files** (`scripts/rag_backfill.py`, `docs/F4_ESCO_Integration_Report.md`, `ruvector.db`). `ruvector.db` should likely be `.gitignore`d once that work lands.
- **Don't rip out the LLM router** — it's already strong; only *add* a context tier + MiniMax.

## Suggested sequence (after the parallel agent lands)
1. Tier 1.1 (live salary APIs) — standalone, no collision with RAG work.
2. Tier 1.3 + 2.1 (Jina rerank + huge-context tier) — coordinate with RAG v2.
3. Tier 1.2 (programmatic tool calling) — biggest perf win, do last, behind a flag, with A/B.

---

## Sources
- [Chinese LLM stack comparison (Q2 2026)](https://tokenmix.ai/blog/best-chinese-ai-models-2026-comparison-guide) · [DeepSeek/Kimi/Qwen/GLM/MiniMax late-April 2026](https://dev.to/bean_bean/the-late-april-2026-chinese-llm-stack-qwen-36-deepseek-v4plus-kimi-k26-minimax-m27-glm-51-2bif) · [free LLM API resources](https://github.com/cheahjs/free-llm-api-resources)
- [Claude Opus 4.8 (Anthropic)](https://www.anthropic.com/news/claude-opus-4-8) · [Opus 4.6 context compaction (InfoQ)](https://www.infoq.com/news/2026/03/opus-4-6-context-compaction/) · [programmatic tool calling — Claude docs](https://platform.claude.com/docs/en/about-claude/models/whats-new-claude-4-5)
- [13 free salary data sources 2026 (Ravio)](https://ravio.com/blog/free-salary-data) · [best job APIs 2026 (Bright Data)](https://brightdata.com/blog/web-data/best-job-apis) · [Deel Salary Insights, WageIndicator, Levels.fyi]
- [Jina embeddings](https://jina.ai/embeddings/) · [Jina Reranker v2 multilingual](https://jina.ai/models/jina-reranker-v2-base-multilingual/) · [embedding model price/perf 2026](https://tokenmix.ai/blog/text-embedding-models-comparison)
- [SlideSpeak API](https://slidespeak.co/features/slidespeak-api) · [Presenton (open-source)](https://github.com/presenton/presenton)
