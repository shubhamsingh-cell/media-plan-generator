# LLM Scout Report — New Models & Free Inference Providers

**Date:** 2026-06-02 · **Author:** Research scout (no code modified)
**Scope:** Candidates to add/remove from Nova's `llm_router.py` (27 providers, Haiku-primary, quality-first).
**Verification rule:** Every pricing/limit claim below is cited with a URL + retrieved date. Where my training data conflicted with live pages, the **live page wins** (flagged inline).

---

## TL;DR — what changed since the router was last tuned

1. **DeepSeek moved to V4 (V4-Flash / V4-Pro, 1M context).** "V3.2" is now legacy. There is **no standing free tier** — only a 5M-token / 30-day expiring grant. This confirms why the router's stale `deepseek-v3.2:free` OpenRouter slug 404s: **OpenRouter currently lists no `:free` DeepSeek slug at all.** → REMOVE the dead entry.
2. **Cerebras free tier got smaller, not bigger.** Live docs show the free trial is now **5 RPM / 30K TPM / 1M TPD**, models `gpt-oss-120b` and `zai-glm-4.7`, with an **8,192-token context cap**. The router's `cerebras_scout` comment (Qwen-3 235B / Llama 4 Scout, "30 RPM", "1M ctx") is **outdated** — needs re-labeling, not removal.
3. **The single best new free add is GLM-4.7-Flash (Zhipu / z.ai):** genuinely free (not trial), 203K context, OpenAI-compatible, no Chinese phone needed. Direct upgrade path from the router's existing GLM-4-Flash.
4. **Gemini 3 Flash** is live on AI Studio free tier at 10 RPM / 250K TPM / 1,500 RPD — the router already targets Gemini, so this is a model-string refresh, not a new provider.
5. **New genuinely-free OpenAI-compatible entrants worth adding:** GitHub Models and NVIDIA NIM. Cohere and Chutes are situational.

---

## Part 1 — Chinese LLMs

### DeepSeek — V4-Flash / V4-Pro (current); R1 still available
- **Release/version:** V4 generation is current; legacy names `deepseek-chat`/`deepseek-reasoner` being phased out. [api-docs.deepseek.com/quick_start/pricing, retrieved 2026-06-02]
- **Context:** 1M tokens, max output 384K. [same]
- **Paid pricing:** V4-Flash $0.14/M in (cache-miss), $0.28/M out; V4-Pro $0.435/M in, $0.87/M out (currently discounted). [same]
- **Free tier:** **None on the official page.** A secondary source reports a 5M-token / 30-day expiring grant for new accounts. [awesomeagents.ai/tools/free-ai-inference-providers-2026, retrieved 2026-06-02] — treat as expiring credit, **not** a sustainable fallback.
- **OpenRouter `:free` slug:** **Does not exist.** OpenRouter lists 22 DeepSeek models, none with `:free`; V3.2 is paid ($0.23/M in, 131K ctx), V4-Pro paid. [openrouter.ai/deepseek, retrieved 2026-06-02]
- **GEO GOTCHA:** DeepSeek restricted *consumer* signup to mainland-China phone numbers after a 2025 attack; **API access for users outside China works via email registration** (no phone). [en.wikipedia.org/wiki/DeepSeek_(chatbot); aiapi-pro.com/blog/deepseek-v4-api-access-guide, both retrieved 2026-06-02]
- **Verdict:** The stale OpenRouter free slug is **dead — REMOVE.** DeepSeek-R1 reasoning is still reachable as a *paid* OpenRouter route (`deepseek/deepseek-r1`, $0.70/$2.50, 164K ctx) if a cheap reasoning fallback is wanted, but it is no longer free.

### Qwen 3 (Alibaba)
- **Qwen3-Coder-480B-A35B:** **FREE on OpenRouter** (`qwen/qwen3-coder:free`), **1M context**, active (28.3B weekly tokens, released Jul 2025). [openrouter.ai/qwen/qwen3-coder:free, retrieved 2026-06-02]
- **Qwen3-Max (DashScope):** paid $0.78/M in; new-account free grant of 1M input + 1M output tokens. [openrouter.ai/qwen/qwen3-max; remoteopenclaw.com/blog/best-qwen-models-2026, retrieved 2026-06-02]
- **GOTCHA — Qwen OAuth free tier discontinued 2026-04-15.** Free access now only via OpenRouter / Fireworks / BYOK. [qwenlm.github.io/qwen-code-docs/en/users/configuration/model-providers/, retrieved 2026-06-02]
- **Reliability caveat:** high-demand Qwen free slugs on OpenRouter throw 429s under load; `:free` daily cap is 50 req/day (see Part 4). [help.apiyi.com/en/qwen3-6-plus-429-error-fix-api-guide-en.html, retrieved 2026-06-02]
- **Verdict:** Router already has `openrouter_qwen`. **Pin it to `qwen/qwen3-coder:free`** (1M ctx, confirmed $0) if not already — that is the durable free Qwen route. Strong for JSON/code-shaped output.

### GLM (Zhipu / z.ai)
- **GLM-4.7-Flash:** **genuinely free (not trial-limited), 203K context**, OpenAI-compatible, sign up at z.ai. Free tier ~**5 RPM, concurrency 1**. Announced 2026-01-19. [wavespeed.ai/blog/posts/glm-4-7-flash/; yangmao.ai/en/providers/zhipu/; vibecoding.app/blog/zhipu-ai-glm-pricing-2026, all retrieved 2026-06-02]
- **GLM-4.5-Flash:** also free, lighter. [vibecoding.app/blog/zhipu-ai-glm-pricing-2026, retrieved 2026-06-02]
- **GLM-5:** 744B MoE, 200K ctx, **paid only** ($1.00/$3.20), Pro/Max plans. [llm-stats.com/blog/research/glm-4-6-launch; glm-5.org, retrieved 2026-06-02]
- **GEO:** "one of the easiest China-access AI APIs," OpenAI-compatible, **no phone gotcha noted** for international API signup. [vibecoding.app/blog/zhipu-ai-glm-pricing-2026, retrieved 2026-06-02]
- **Verdict:** Router's existing `zhipu` provider runs GLM-4-Flash. **Upgrade the model string to `glm-4.7-flash`** for 203K context (vs the older Flash) at the same $0. Best multilingual/CN free option. **Watch the 5 RPM ceiling** — fine as a fallback, not a primary.

### Kimi K2 (Moonshot)
- **Kimi K2.6:** released 2026-04-20, trillion-param MoE, **256K context all variants**, native vision. [rits.shanghai.nyu.edu/ai/...kimi-k2-6...; openrouter.ai/moonshotai/kimi-k2.6, retrieved 2026-06-02]
- **Free tier:** **Web only (kimi.com).** **No free API tier** — API is pay-as-you-go ($0.95/M cache-miss in, $4.00/M out). [tokenmix.ai/blog/kimi-k2-api-pricing; minnano-rakuraku.com/...kimik26..., retrieved 2026-06-02]
- **OpenRouter:** a `moonshotai/kimi-k2.6:free` slug is listed but inherits OpenRouter's 50 req/day free ceiling and is demand-throttled. [openrouter.ai/moonshotai/kimi-k2.6:free, retrieved 2026-06-02]
- **Verdict:** **Do not add as a direct provider** (no free API). Only reachable free via OpenRouter's shared free pool — redundant with Qwen3-Coder there. Skip.

### MiniMax (M-series / abab)
- **Free API tier:** **None.** Pay-as-you-go only; flagship M1 $0.40/M in, $2.20/M out. Consumer credits (Hailuo) are separate and non-API. [costbench.com/software/llm-api-providers/minimax-api/; platform.minimax.io/docs/pricing/overview, retrieved 2026-06-02]
- **Verdict:** **Skip** — no free API.

### Yi (01.AI), Baichuan, Doubao (ByteDance)
- **Doubao:** Seed 2.0 family reachable internationally via **Volcano Engine .com endpoint (English UI, no VPN)** — but **no free API tier** identified; pay-as-you-go. [tokenmix.ai/blog/doubao-api-international-access-guide-2026, retrieved 2026-06-02]
- **Yi / Baichuan:** limited OpenRouter presence, **domestic-focused**, no clear free international API. [digitalapplied.com/blog/chinese-ai-models-q2-2026-market-share-report, retrieved 2026-06-02]
- **Verdict:** **Skip all three** — no usable free international tier.

---

## Part 2 — Global LLMs / Free Inference

### Llama 4 (Meta) — Scout & Maverick
- **Scout:** 17B active / 16 experts, **10M context** (largest open-weight ever). **Maverick:** 17B active / 128 experts, **1M context**. Both MoE, natively multimodal. [ai.meta.com/blog/llama-4-multimodal-intelligence/, retrieved 2026-06-02]
- **Free hosts:**
  - **Groq:** Scout + Maverick free; **Maverick at half quota — 15 RPM / 3K TPM / 500 RPD.** [tokenmix.ai/blog/groq-free-tier-limits-2026, retrieved 2026-06-02]
  - **SambaNova:** Scout at ~697 tok/s (fastest). [sambanova.ai/blog/sambanova-partners-with-meta..., retrieved 2026-06-02]
  - **Cerebras:** **was** experimental-access; **now NOT on the free menu** (free tier is gpt-oss-120b + zai-glm-4.7, see below). [inference-docs.cerebras.ai/support/rate-limits, retrieved 2026-06-02]
  - **OpenRouter:** Llama 4 Maverick/Scout free variants exist (shared 50 req/day pool). [awesomeagents.ai/..., retrieved 2026-06-02]
- **Verdict:** Router already reaches Llama via Groq/SambaNova/Cerebras/Cloudflare on Llama 3.3 70B. **Refresh Groq's model string to a Llama 4 Scout slug** (1M+ ctx, same free tier) for the biggest context-window win at zero cost.

### Mistral (Large 3 / Small 3.x)
- **La Plateforme free "experiment" tier:** free across all models, **~1B tokens/month**, **no credit card** — BUT **~2 RPM**, explicitly "not viable for production." [pricepertoken.com/endpoints/mistral/free; awesomeagents.ai/..., retrieved 2026-06-02]
- **Paid:** Large 3 $2/$6, Small 3.1 $0.20/$0.60. [tokenmix.ai/blog/mistral-api-pricing, retrieved 2026-06-02]
- **Verdict:** Router already has Mistral for EU/translation. Keep as-is. **2 RPM makes it a niche fallback, not a workhorse** — honest flag: its "free" tier is rate-limited near uselessness for a live chatbot.

### Gemini (3 Flash)
- **Free tier (AI Studio):** Gemini 3 Flash = **10 RPM / 250K TPM / 1,500 RPD**, no card. [tokenmix.ai/blog/gemini-api-free-tier-limits; pecollective.com/tools/gemini-free-tier-guide/, retrieved 2026-06-02]
- **Note:** Google cut free quotas in Dec 2025; still the most generous by total token volume. RPM binds before TPM. [awesomeagents.ai/..., retrieved 2026-06-02]
- **Verdict:** Router already targets Gemini (3-flash-preview + flash-lite). **Promote `gemini-3-flash` to the stable string** as the GA model lands; 1,500 RPD is the best sustained free volume available.

### Groq — newest + current free limits
- **Free tier:** default **30 RPM / 6K TPM / 1,000 RPD** (Llama 4 Maverick is half: 15/3K/500). Gemma 2 9B gets 15K TPM but only 8K ctx. **RPD is the binding constraint.** [tokenmix.ai/blog/groq-free-tier-limits-2026; console.groq.com/docs/rate-limits, retrieved 2026-06-02]
- **Verdict:** Already in router (`groq`). Solid. Refresh model to Llama 4 Scout (above).

### Cerebras — newest + current free limits ⚠️ CHANGED
- **Live free trial (docs):** **5 RPM / 30K TPM / 1M TPD**, models `gpt-oss-120b` and `zai-glm-4.7`, **8,192-token context cap.** [inference-docs.cerebras.ai/support/rate-limits, retrieved 2026-06-02]
- **Conflicting older claims** (Qwen3 235B, Llama 4 Scout, 30 RPM, "~2,600 tok/s") appear in blogs but are **not on the current rate-limit docs page** — treat router's `cerebras_scout` label as **stale**. [adam.holter.com/cerebras-opens...; pricepertoken.com/endpoints/cerebras/free, retrieved 2026-06-02]
- **GOTCHA:** 8K context cap on free tier breaks any long-prompt routing — Cerebras free is now **short-prompt / speed only**.
- **Verdict:** Keep `cerebras` + `cerebras_scout` but **re-label and constrain to ≤8K-token tasks**; do not route long plan-narrative prompts there. The "1M ctx" assumption is wrong on free tier.

### New free inference entrants since early 2026
- **GitHub Models:** GPT-4o, o3, Grok-3 on free tier, **OpenAI-compatible**, GitHub account only, **10–15 RPM / 50–150 req/day.** [awesomeagents.ai/..., retrieved 2026-06-02]
- **NVIDIA NIM:** 100+ open models, **40 RPM**, no credit card (signup), OpenAI-compatible, includes DeepSeek-R1 + Llama. [awesomeagents.ai/...; klymentiev.com/blog/free-llm-api, retrieved 2026-06-02]
- **Cohere:** Command R+ / Embed 4, OpenAI-compatible, **20 RPM but only 1,000 req/MONTH** (very tight). [awesomeagents.ai/..., retrieved 2026-06-02]
- **Chutes (Bittensor decentralized):** OpenAI-compatible (`https://llm.chutes.ai/v1`), some models genuinely free, hosts DeepSeek-R1 distills + Qwen3 + Mistral. **Reliability = best-effort / variable** (subnet economics). [tokenmix.ai/blog/chutes-ai-api-keys-access-pricing-2026; chutes.ai, retrieved 2026-06-02]
- **AI21 / Fireworks / xAI:** credit-based, expiring — not durable free. [awesomeagents.ai/..., retrieved 2026-06-02]

---

## Part 3 — "Free but useless?" honesty check

| Provider | Free limit | Usable as production fallback? |
|---|---|---|
| Gemini 3 Flash | 10 RPM / 1,500 RPD | **Yes** — best sustained volume |
| Groq (Llama 4 Scout) | 30 RPM / 1,000 RPD | **Yes** — fast, durable |
| NVIDIA NIM | 40 RPM / 1K credits | **Yes** — highest RPM of new entrants |
| GLM-4.7-Flash | 5 RPM, concurrency 1 | **Marginal** — fallback only, throttles fast |
| OpenRouter `:free` (Qwen/Llama/DeepSeek) | **50 req/day** (1K if $10 deposited) | **Weak** — 50/day is near-useless for a live chatbot unless you deposit $10 |
| Cerebras free | 5 RPM / 8K ctx cap | **Marginal** — short-prompt/speed only |
| Mistral experiment | ~2 RPM | **No** — rate-limited near uselessness |
| Cohere | 1,000 req/**month** | **No** — monthly cap too tight |
| Chutes | variable/best-effort | **No** — unreliable for production SLO |

**Key gotcha for the router:** OpenRouter free slugs (which back the router's `openrouter_qwen` and `openrouter_deepseek_r1`) are capped at **50 requests/day per account** on the free tier, rising to 1,000/day only after a one-time **$10 credit deposit**. [costbench.com/software/llm-api-providers/openrouter/free-plan/; openrouter.zendesk.com/...OpenRouter-Rate-Limits, retrieved 2026-06-02] **Recommendation: deposit $10 once** to unlock 1,000/day on all `:free` routes — cheapest reliability upgrade available.

**Output-token gotcha:** none of the genuinely-free tiers above were found to hard-cap output at 4K tokens; the binding limit is the per-minute TPM (e.g. Gemini 250K TPM, Groq 6K TPM). Cerebras free's real limiter is the **8K total context cap**, not an output cap.

---

## Part 4 — Ranked recommendation table

Router integration cost model: OpenAI-compatible providers = add a `PROVIDER_CONFIG` entry (`api_style: "openai"`, endpoint, model, env key) + `_RATE_LIMITS` line ≈ **~20 LOC**. Gemini-style ≈ ~30 LOC.

### TOP 5 TO ADD / UPGRADE

| Rank | Action | Provider / model | Ctx | Free limit | API style | Effort | Why |
|---|---|---|---|---|---|---|---|
| 1 | **UPGRADE** | Zhipu → **`glm-4.7-flash`** | 203K | free, 5 RPM, conc 1 | OpenAI | ~5 LOC (model string) | Same provider, 203K ctx (vs old Flash), best free multilingual/CN, no phone gotcha |
| 2 | **UPGRADE** | Gemini → **`gemini-3-flash`** (GA string) | ~1M | 10 RPM / 1,500 RPD | Gemini | ~5 LOC | Best sustained free volume; router already wired to Gemini |
| 3 | **UPGRADE** | Groq → **Llama 4 Scout** slug | 10M | 30 RPM / 1,000 RPD | OpenAI | ~5 LOC | Largest free context window on the fastest free host |
| 4 | **ADD** | **NVIDIA NIM** (e.g. Llama / DeepSeek-R1) | model-dep | 40 RPM | OpenAI | ~20 LOC | Highest RPM of new entrants; adds a reasoning route to replace the dead DeepSeek slug |
| 5 | **ADD** | **GitHub Models** (GPT-4o / o3) | model-dep | 10–15 RPM / 50–150 RPD | OpenAI | ~20 LOC | Frontier-class models on a genuinely free tier; useful quality-tier fallback |

**Honorable mention — config tweak, not a provider:** deposit **$10 on OpenRouter** to raise all `:free` slugs from 50 → 1,000 req/day. Highest reliability-per-dollar action in the whole report.

### TO REMOVE / FIX

| Action | Entry | Reason |
|---|---|---|
| **REMOVE** | OpenRouter `deepseek/deepseek-v3.2:free` (stale) | OpenRouter has **no `:free` DeepSeek slug** — 404 confirmed at source. [openrouter.ai/deepseek, 2026-06-02] |
| **RE-LABEL + CONSTRAIN** | `cerebras_scout` ("Qwen-3 235B/Llama 4 Scout, 1M ctx") | Live docs: free tier is `gpt-oss-120b`/`zai-glm-4.7`, **8K ctx cap, 5 RPM**. Route only ≤8K-token tasks. [inference-docs.cerebras.ai/support/rate-limits, 2026-06-02] |
| **PIN** | `openrouter_qwen` → `qwen/qwen3-coder:free` | Durable confirmed-free 1M-ctx Qwen route; avoids OAuth tier that died 2026-04-15. [openrouter.ai/qwen/qwen3-coder:free, 2026-06-02] |
| **VERIFY (paid)** | `openrouter_deepseek_r1` | DeepSeek-R1 on OpenRouter is **paid** ($0.70/$2.50), not free. If the router assumes free, fix the cost tag. [openrouter.ai/deepseek, 2026-06-02] |

### SKIP (no usable free international API)
Kimi K2.6 (web-only free), MiniMax (no free API), Doubao/Yi/Baichuan (paid/domestic), DeepSeek direct (no standing free tier; expiring 5M grant only), Cohere (1K/month too tight), Chutes (best-effort reliability), Mistral as workhorse (2 RPM).

---

## Sources (all retrieved 2026-06-02)
- DeepSeek pricing: https://api-docs.deepseek.com/quick_start/pricing
- DeepSeek on OpenRouter (no free slug): https://openrouter.ai/deepseek
- DeepSeek geo/registration: https://en.wikipedia.org/wiki/DeepSeek_(chatbot) · https://aiapi-pro.com/blog/deepseek-v4-api-access-guide
- Qwen3-Coder free: https://openrouter.ai/qwen/qwen3-coder:free · Qwen OAuth EOL: https://qwenlm.github.io/qwen-code-docs/en/users/configuration/model-providers/
- GLM-4.7-Flash: https://wavespeed.ai/blog/posts/glm-4-7-flash/ · https://yangmao.ai/en/providers/zhipu/ · https://vibecoding.app/blog/zhipu-ai-glm-pricing-2026 · GLM-5: https://llm-stats.com/blog/research/glm-4-6-launch
- Kimi K2.6: https://openrouter.ai/moonshotai/kimi-k2.6 · https://tokenmix.ai/blog/kimi-k2-api-pricing
- MiniMax: https://costbench.com/software/llm-api-providers/minimax-api/ · https://platform.minimax.io/docs/pricing/overview
- Doubao/Chinese landscape: https://tokenmix.ai/blog/doubao-api-international-access-guide-2026 · https://www.digitalapplied.com/blog/chinese-ai-models-q2-2026-market-share-report
- Llama 4: https://ai.meta.com/blog/llama-4-multimodal-intelligence/ · https://sambanova.ai/blog/sambanova-partners-with-meta-to-deliver-lightning-fast-inference-on-llama-4
- Mistral free: https://pricepertoken.com/endpoints/mistral/free · https://tokenmix.ai/blog/mistral-api-pricing
- Gemini free: https://tokenmix.ai/blog/gemini-api-free-tier-limits · https://pecollective.com/tools/gemini-free-tier-guide/
- Groq free: https://tokenmix.ai/blog/groq-free-tier-limits-2026 · https://console.groq.com/docs/rate-limits
- Cerebras free: https://inference-docs.cerebras.ai/support/rate-limits · https://pricepertoken.com/endpoints/cerebras/free
- New entrants / free-tier survey: https://awesomeagents.ai/tools/free-ai-inference-providers-2026/ · https://klymentiev.com/blog/free-llm-api
- OpenRouter free limits/404s: https://costbench.com/software/llm-api-providers/openrouter/free-plan/ · https://openrouter.zendesk.com/hc/en-us/articles/39501163636379-OpenRouter-Rate-Limits-What-You-Need-to-Know · https://dev.to/josh_green_dev/free-llms-on-openrouter-keep-going-404-i-fixed-it-with-120-lines-of-python-43i1
- Chutes: https://tokenmix.ai/blog/chutes-ai-api-keys-access-pricing-2026 · https://chutes.ai/
