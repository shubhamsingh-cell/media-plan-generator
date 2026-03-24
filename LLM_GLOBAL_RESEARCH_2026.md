# Global LLM Free Tier Research -- March 2026

> Exhaustive research on every LLM API with a free tier globally, organized by integration priority for the Nova AI Suite 13-provider router.

## Current Router Providers (13)

| # | Provider | Free Tier |
|---|----------|-----------|
| 1 | Gemini Flash (Google) | ~1,500 req/day free |
| 2 | Groq (Meta Llama) | ~14,400 req/day free |
| 3 | Cerebras | 30 req/min, 1M tokens/day free |
| 4 | Mistral | ~1B tokens/month free |
| 5 | NVIDIA NIM | 40 req/min free |
| 6 | SambaNova | 200K tokens/day free |
| 7 | Cohere | 1,000 calls/month trial |
| 8 | Together AI | $25 free credits |
| 9 | Fireworks AI | $1 free credit |
| 10 | Anyscale | Free tier (deprecated, now part of Fireworks) |
| 11 | DeepSeek | 5M token signup bonus |
| 12 | Perplexity | Paid only (no free tier) |
| 13 | OpenRouter | Aggregator, 29 free models |

---

## TIER A: Add NOW -- Easy Integration, High Free Limits, Good Quality

These providers have generous free tiers, OpenAI-compatible APIs, work internationally, and offer strong model quality. Each should be added to the router immediately.

---

### A1. Zhipu AI (GLM-4) -- China

- **Country:** China
- **Models:** GLM-4.7-Flash (FREE), GLM-4.5-Flash (FREE), GLM-4.6V-Flash (FREE vision), GLM-4.7 (paid), GLM-5 (paid)
- **Free Tier:** GLM-4.7-Flash and GLM-4.5-Flash are completely free with no daily quota. GLM-4.6V-Flash (9B vision) is also free.
- **API Endpoint:** `https://open.bigmodel.cn/api/paas/v4/chat/completions` (OpenAI-compatible)
- **Auth:** API key from open.bigmodel.cn
- **Signup:** Email only, international accounts supported
- **International Access:** YES -- works from US/global, higher latency from non-China locations
- **Best Use Case:** General chat, coding, vision tasks
- **Quality:** 7/10 (GLM-4.7-Flash comparable to GPT-4o-mini)
- **stdlib Compatible:** YES (OpenAI-compatible REST API, works with urllib)
- **Why Add:** Unlimited free flash models with no daily caps is unmatched. The quality is solid for a zero-cost option.

---

### A2. SiliconFlow -- China (Aggregator)

- **Country:** China
- **Models:** DeepSeek-V3.2, Qwen 2.5 family, GLM-4, Llama 3.1, Mistral, and dozens more
- **Free Tier:** Free to start with pay-as-you-go; prices from $0.05/M tokens for budget models
- **API Endpoint:** `https://api.siliconflow.cn/v1/chat/completions` (OpenAI-compatible)
- **Auth:** API key from siliconflow.com
- **Signup:** Email, international accounts supported
- **International Access:** YES -- global endpoint available at siliconflow.com
- **Best Use Case:** Cost-optimized inference for any open-source model
- **Quality:** Varies by model (7-9/10 depending on model choice)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** Chinese aggregator with the cheapest rates globally. Even paid rates are often cheaper than other providers' free tiers. Great fallback for when free tiers exhaust.

---

### A3. Cloudflare Workers AI -- USA

- **Country:** USA (edge-deployed globally)
- **Models:** Llama 3.3 70B, Llama 3.2 family, Mistral 7B, Gemma 7B/2B, DeepSeek-R1-Distill-Qwen-32B, Llama Guard 3, and many more
- **Free Tier:** 10,000 Neurons/day free (approx 100-200 LLM responses/day). Beta models are unlimited and free.
- **API Endpoint:** `https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/meta/llama-3.3-70b-instruct-fp8-fast`
- **Auth:** Cloudflare API token + Account ID
- **Signup:** Email only, free Cloudflare account
- **International Access:** YES -- edge-deployed in 300+ cities globally
- **Best Use Case:** Low-latency edge inference, embedding, classification
- **Quality:** 7-8/10 (depends on model; Llama 3.3 70B is strong)
- **stdlib Compatible:** YES (REST API with Bearer token)
- **Why Add:** 10K neurons/day is solid for a fallback. Beta models are free with no limits. Edge deployment means low latency globally.

---

### A4. Hugging Face Inference Providers -- USA

- **Country:** USA (global)
- **Models:** 300+ models including Llama, Mistral, Qwen, Gemma, BERT, and more
- **Free Tier:** Monthly free credits for inference (exact amount varies; PRO gives 20x more). Many small models are free via hf-inference provider.
- **API Endpoint:** `https://router.huggingface.co/v1/chat/completions` (OpenAI-compatible via Inference Providers)
- **Auth:** HF API token
- **Signup:** Email only, no credit card
- **International Access:** YES
- **Best Use Case:** Model variety, experimentation, embeddings, specialized models
- **Quality:** Varies (6-9/10 depending on model)
- **stdlib Compatible:** YES (OpenAI-compatible or custom REST endpoints)
- **Why Add:** Unmatched model variety. Good for specialized tasks where you need a specific model type (code, medical, multilingual).

---

### A5. Alibaba Qwen (via OpenRouter or SiliconFlow) -- China

- **Country:** China
- **Models:** Qwen3-Coder, Qwen3-Next 80B, Qwen 2.5 family (0.5B to 72B), QwQ (reasoning)
- **Free Tier:** Available free on OpenRouter (Qwen3-Coder:free, Qwen3-4B:free, Qwen3-Next 80B:free). Official Dashscope has limited free quota in Singapore region only.
- **API Endpoint:** Via OpenRouter `https://openrouter.ai/api/v1` or SiliconFlow
- **Auth:** OpenRouter API key (already have) or Alibaba Cloud API key
- **Signup:** Email for OpenRouter; Alibaba Cloud requires more verification
- **International Access:** YES via OpenRouter/SiliconFlow; official Dashscope is region-limited
- **Best Use Case:** Code generation (Qwen3-Coder), general reasoning, multilingual
- **Quality:** 8/10 (Qwen3 series is competitive with GPT-4o)
- **stdlib Compatible:** YES (OpenAI-compatible via OpenRouter)
- **Why Add:** Qwen3-Coder is currently the strongest free coding model on OpenRouter. QwQ is an excellent free reasoning model.

---

### A6. Arcee Trinity -- USA

- **Country:** USA
- **Models:** Trinity Large (400B MoE, 13B active), Trinity Mini (26B), Trinity Nano (6B)
- **Free Tier:** All Trinity models are free on OpenRouter with no hidden usage caps. API pricing for Mini is $0.045/$0.15 with rate-limited free tier.
- **API Endpoint:** Via OpenRouter `https://openrouter.ai/api/v1` (model: `arcee-ai/trinity-large-preview:free`)
- **Auth:** OpenRouter API key
- **Signup:** Email only via OpenRouter
- **International Access:** YES
- **Best Use Case:** Creative writing, instruction following, coding, agent harnesses
- **Quality:** 7.5/10 (400B MoE, competitive with Llama 3.3)
- **stdlib Compatible:** YES (OpenAI-compatible via OpenRouter)
- **Why Add:** Free 400B parameter model with no caps. Strong for creative and coding tasks.

---

### A7. Liquid AI LFM -- USA

- **Country:** USA
- **Models:** LFM 2.5 1.2B Instruct, LFM 2.5 1.2B Thinking
- **Free Tier:** Free on OpenRouter (`liquid/lfm-2.5-1.2b-thinking:free`)
- **API Endpoint:** Via OpenRouter
- **Auth:** OpenRouter API key
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Chain-of-thought reasoning, agentic workflows, lightweight inference
- **Quality:** 6/10 (small model but unique architecture)
- **stdlib Compatible:** YES
- **Why Add:** Unique non-transformer architecture with built-in reasoning. Good for testing agentic workflows at zero cost.

---

## TIER B: Worth Adding -- Moderate Limits or Some Signup Friction

These providers have useful free tiers but may require more setup, have geo-restrictions, or have lower free-tier limits.

---

### B1. Moonshot AI (Kimi) -- China

- **Country:** China
- **Models:** Kimi K2 (1T params, 32B active), Kimi K2.5 (multimodal, 256K context)
- **Free Tier:** Free on OpenRouter (`moonshotai/kimi-k2:free`). Official API is paid (from $0.55/M input tokens).
- **API Endpoint:** Official: `https://api.moonshot.cn/v1/chat/completions` (OpenAI-compatible); Free via OpenRouter
- **Auth:** API key from platform.moonshot.ai or OpenRouter
- **Signup:** Email for OpenRouter; Chinese phone may be needed for official platform
- **International Access:** YES via OpenRouter; official platform may have restrictions
- **Best Use Case:** Long-context tasks, research, legal analysis (256K context)
- **Quality:** 8/10 (Kimi K2.5 is competitive with Claude)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** K2.5 has a massive 256K context window and strong multimodal capabilities. Free via OpenRouter.

---

### B2. AI21 Labs (Jamba) -- Israel

- **Country:** Israel
- **Models:** Jamba 1.7 Large (256K context), Jamba 1.6 Large, Jamba 1.6 Mini
- **Free Tier:** $10 trial credits valid for 3 months. 200 RPM, 10 RPS.
- **API Endpoint:** `https://api.ai21.com/studio/v1/chat/completions` (OpenAI-compatible)
- **Auth:** API key from studio.ai21.com
- **Signup:** Email only, no credit card
- **International Access:** YES
- **Best Use Case:** Long document processing (256K context), enterprise use cases
- **Quality:** 7/10 (hybrid Mamba-Transformer architecture, fast inference)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** $10 free credits is generous. The Mamba-Transformer hybrid is extremely fast (181 tokens/sec for Mini).

---

### B3. Reka AI -- USA/Singapore

- **Country:** USA/Singapore
- **Models:** Reka Flash 3, Reka Core, Reka Spark (lightweight)
- **Free Tier:** Free playground; 3 free hours of indexed video for Vision API. Pay-as-you-go for chat API.
- **API Endpoint:** `https://api.reka.ai/v1/chat` (custom API)
- **Auth:** API key from platform.reka.ai
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Multimodal tasks (text, vision, speech, video)
- **Quality:** 7/10 (Reka Flash 3 at $0.35/M tokens is cost-effective)
- **stdlib Compatible:** YES (REST API)
- **Why Add:** Strong multimodal capabilities at very low cost. Vision API free trial is unique.

---

### B4. Sarvam AI -- India

- **Country:** India
- **Models:** Sarvam-30B, Sarvam-105B ("Indus"), Bulbul (TTS), Mayura (Translation)
- **Free Tier:** Rs 1,000 free credits (~$12 USD). No credit card needed.
- **API Endpoint:** `https://api.sarvam.ai/v1/chat/completions`
- **Auth:** API key from sarvam.ai
- **Signup:** Email only
- **International Access:** YES (API available globally)
- **Best Use Case:** Indian language tasks (Hindi, Tamil, Telugu, etc.), translation, TTS for Indian languages
- **Quality:** 6/10 for English, 8/10 for Indian languages (best-in-class for Hindi/Indic)
- **stdlib Compatible:** YES (REST API)
- **Why Add:** If your users include Indian language speakers, Sarvam is unmatched for Indic language quality. Free credits are decent.

---

### B5. DeepSeek (Official) -- China

- **Country:** China
- **Models:** DeepSeek-V3.2, DeepSeek-R1 (reasoning), DeepSeek-Coder
- **Free Tier:** 5M free tokens on signup (valid 30 days). Off-peak 75% discount on R1.
- **API Endpoint:** `https://api.deepseek.com/v1/chat/completions` (OpenAI-compatible)
- **Auth:** API key from platform.deepseek.com
- **Signup:** Email only
- **International Access:** YES, but geopolitical restrictions in some government/enterprise contexts
- **Best Use Case:** Reasoning (R1), coding, general chat at rock-bottom prices
- **Quality:** 9/10 (V3.2 rivals GPT-4o; R1 rivals o1)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** Already in router but worth maximizing. V3.2 at $0.14/$0.28 per M tokens with cache discount is cheapest high-quality option on Earth.

---

### B6. Hyperbolic -- USA

- **Country:** USA
- **Models:** Open-source models (Llama, Qwen, DeepSeek, etc.)
- **Free Tier:** Free to start, pay-as-you-go. Up to 80% cheaper than AWS/Azure.
- **API Endpoint:** `https://api.hyperbolic.xyz/v1/chat/completions` (OpenAI-compatible)
- **Auth:** API key from hyperbolic.xyz
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Cost-optimized inference for open-source models
- **Quality:** Varies by model (7-9/10)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** Competitive alternative to Together AI/Fireworks with potentially lower prices.

---

### B7. StepFun -- China

- **Country:** China
- **Models:** Step 3.5 Flash (196B MoE, 11B active), Step3 (321B MoE, 38B active)
- **Free Tier:** Step 3.5 Flash is FREE on OpenRouter ($0/$0 per M tokens). 256K context.
- **API Endpoint:** Via OpenRouter or `platform.stepfun.ai`
- **Auth:** OpenRouter API key or StepFun API key
- **Signup:** Email for OpenRouter
- **International Access:** YES via OpenRouter
- **Best Use Case:** General chat, coding, long-context tasks
- **Quality:** 7.5/10 (196B MoE is powerful despite small active params)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** Free 196B MoE model on OpenRouter with 256K context. Very capable for zero cost.

---

### B8. MiniMax -- China

- **Country:** China
- **Models:** M2.5 (open-source), M2.7, M2
- **Free Tier:** M2.7 trial credits on official platform. M2.5 is free on OpenRouter. Open-source M2.5 available on HuggingFace.
- **API Endpoint:** `https://api.minimax.chat/v1/chat/completions` or via OpenRouter
- **Auth:** API key from platform.minimax.io or OpenRouter
- **Signup:** Email
- **International Access:** YES via OpenRouter; official platform may have restrictions
- **Best Use Case:** Office productivity, long-context, general chat
- **Quality:** 7.5/10 (M2.5 is 5-7x cheaper than GPT-5 at comparable benchmarks)
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why Add:** Free on OpenRouter, strong cost-performance ratio.

---

### B9. NVIDIA Nemotron (via NIM or OpenRouter) -- USA

- **Country:** USA
- **Models:** Nemotron 3 Super 120B (MoE), Nemotron 3 Nano 30B
- **Free Tier:** Free on OpenRouter (262K context, tool support). Also free via NVIDIA NIM (40 RPM).
- **API Endpoint:** Via OpenRouter or `https://integrate.api.nvidia.com/v1`
- **Auth:** OpenRouter or NVIDIA Build API key
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Agent tasks, tool use, code generation (Nemotron 3 Super is excellent)
- **Quality:** 8/10 (120B MoE with tool support)
- **stdlib Compatible:** YES
- **Why Add:** NVIDIA's own models are free and support tool calling natively.

---

### B10. OpenAI GPT-OSS (via OpenRouter/Cerebras) -- USA

- **Country:** USA
- **Models:** GPT-OSS 120B, GPT-OSS 20B (Apache 2.0 open-weight)
- **Free Tier:** Free on OpenRouter and Cerebras
- **API Endpoint:** Via OpenRouter (`openai/gpt-oss-120b:free`) or Cerebras
- **Auth:** OpenRouter or Cerebras API key
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** General chat, code, reasoning (OpenAI's first open model)
- **Quality:** 8/10 (competitive with GPT-4o-mini)
- **stdlib Compatible:** YES
- **Why Add:** OpenAI's first open-weight model, available free via multiple providers.

---

## TIER C: Niche/Specialized -- Add for Specific Use Cases

---

### C1. Voyage AI -- USA (Embeddings)

- **Country:** USA (now part of MongoDB)
- **Models:** voyage-3.5, voyage-3-large, voyage-code-3, voyage-multimodal-3.5
- **Free Tier:** First 200M tokens free for main models; 150B pixels free for multimodal
- **API Endpoint:** `https://api.voyageai.com/v1/embeddings`
- **Auth:** API key from dash.voyageai.com
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Embeddings for RAG, code search, semantic search
- **Quality:** 9/10 for embeddings (top MTEB scores)
- **stdlib Compatible:** YES
- **Why Add:** 200M free tokens for embeddings is extremely generous. Best-in-class for code search.

---

### C2. Jina AI -- Germany (Embeddings + Reranking)

- **Country:** Germany
- **Models:** jina-embeddings-v5, jina-embeddings-v4 (multimodal), jina-reranker
- **Free Tier:** Free tier available (specific limits vary; generous for development)
- **API Endpoint:** `https://api.jina.ai/v1/embeddings`
- **Auth:** API key from jina.ai
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Embeddings, reranking, document understanding
- **Quality:** 8.5/10 for embeddings (89 languages, 32K context)
- **stdlib Compatible:** YES
- **Why Add:** Multi-language embeddings for 89 languages. Complements Voyage AI for non-English content.

---

### C3. Krutrim (by Ola) -- India

- **Country:** India
- **Models:** Krutrim-2 (12B, Mistral-NeMo based), Krutrim-1 (7B)
- **Free Tier:** Free consumer access via Kruti assistant. Developer API details not fully public.
- **API Endpoint:** Via Krutrim Cloud (ai-labs.olakrutrim.com)
- **Auth:** API key from Krutrim Cloud
- **Signup:** May require Indian phone number
- **International Access:** Limited (primarily India-focused infrastructure)
- **Best Use Case:** Indian language tasks (13 languages), multilingual chat
- **Quality:** 6.5/10 for English, 7.5/10 for Indic languages
- **stdlib Compatible:** Likely YES (REST API)
- **Why Add:** Only if targeting Indian market specifically. Sarvam AI is more accessible internationally.

---

### C4. Writer AI (Palmyra) -- USA

- **Country:** USA
- **Models:** Palmyra X5 (1M context), Palmyra Med, Palmyra Fin, Palmyra Creative
- **Free Tier:** Free trial available (no credit card). Paid starts at $0.60/M tokens.
- **API Endpoint:** `https://api.writer.com/v1/chat`
- **Auth:** API key from dev.writer.com
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Enterprise content generation, medical/financial specialized models
- **Quality:** 7.5/10 (specialized models are excellent in their domains)
- **stdlib Compatible:** YES
- **Why Add:** Specialized domain models (Med, Fin) are rare to find with free access.

---

### C5. Lepton AI -- USA (GPU Platform)

- **Country:** USA
- **Models:** Llama, Mixtral, Stable Diffusion, Whisper, and custom models
- **Free Tier:** Basic plan is free (up to 4 CPUs, 16GB memory, 1 GPU). Pay-per-use.
- **API Endpoint:** Custom Lepton endpoints
- **Auth:** API key from lepton.ai
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Custom model hosting, self-hosted inference
- **Quality:** Depends on model
- **stdlib Compatible:** YES
- **Why Add:** Good for self-hosting custom models. Free basic plan includes GPU access.

---

### C6. Novita AI -- USA (Budget Inference)

- **Country:** USA
- **Models:** 200+ models including Llama, Qwen, GPT-OSS, Gemma
- **Free Tier:** Pay-as-you-go only (no explicit free tier found). Prices from $0.02/M tokens.
- **API Endpoint:** OpenAI-compatible REST API
- **Auth:** API key from novita.ai
- **Signup:** Email only
- **International Access:** YES
- **Best Use Case:** Ultra-cheap inference for open-source models
- **Quality:** Varies by model
- **stdlib Compatible:** YES
- **Why Add:** At $0.02/M tokens for budget models, even the paid tier is essentially free. Good backup.

---

### C7. Aleph Alpha -- Germany (European Sovereign AI)

- **Country:** Germany
- **Models:** Pharia-1-LLM (7B), Luminous (13B/30B/70B)
- **Free Tier:** Free credits on signup; startups/academia can apply for more
- **API Endpoint:** `https://api.aleph-alpha.com/complete`
- **Auth:** API key from app.aleph-alpha.com
- **Signup:** Email only
- **International Access:** YES (EU-hosted, GDPR-compliant)
- **Best Use Case:** European compliance requirements, German/French/Italian NLP
- **Quality:** 6/10 (smaller models, but strong for European languages)
- **stdlib Compatible:** YES (REST API)
- **Why Add:** Only if you need GDPR-compliant, EU-hosted inference. Limited model quality.

---

## TIER D: Not Recommended -- Geo-Restricted, Low Quality, or Too Much Friction

---

### D1. Baidu ERNIE -- China

- **Country:** China
- **Models:** ERNIE 4.5, ERNIE X1 (reasoning), ERNIE Bot
- **Free Tier:** Free ERNIE Bot for consumers. API pricing: $0.55/$2.20 per M tokens (ERNIE 4.5).
- **API Endpoint:** Qianfan platform (ark.cn-beijing.volces.com)
- **Signup:** Requires Chinese phone number for API access
- **International Access:** NO -- geo-blocked, requires VPN or third-party access
- **Best Use Case:** Chinese-language tasks
- **Quality:** 7.5/10 for Chinese
- **stdlib Compatible:** Custom API (not OpenAI-compatible natively)
- **Why NOT:** Geo-restricted, requires Chinese phone, non-standard API. Open-source version (Apache 2.0) is coming but not yet available via standard API.

---

### D2. ByteDance Doubao -- China

- **Country:** China
- **Models:** Doubao 2.0 Pro, Doubao-Seed-Code, Doubao 1.5 Pro
- **Free Tier:** 2T tokens free for academic/research use. Consumer app has daily free generations.
- **API Endpoint:** Volcano Engine (ark.cn-beijing.volces.com)
- **Signup:** Requires Chinese phone number
- **International Access:** NO -- primarily China-only infrastructure
- **Best Use Case:** Chinese market, extremely cheap inference ($0.11/M tokens)
- **Quality:** 8/10 (Doubao 2.0 Pro is strong, 46% market share in China)
- **stdlib Compatible:** Custom API
- **Why NOT:** Requires Chinese phone, China-only registration, non-standard API. Extremely cheap but inaccessible from outside China.

---

### D3. SenseTime SenseNova -- China

- **Country:** China
- **Models:** SenseNova V6 (600B+ MoE), SenseNova V6.5 Omni (multimodal)
- **Free Tier:** 10M free tokens on signup
- **API Endpoint:** SenseTime platform API
- **Signup:** Likely requires Chinese verification
- **International Access:** LIMITED -- scaling back international presence
- **Best Use Case:** Multimodal Chinese-language tasks
- **Quality:** 8/10 for Chinese multimodal
- **stdlib Compatible:** Custom API
- **Why NOT:** Limited international access, Chinese-centric, non-standard API.

---

### D4. 01.AI (Yi) -- China

- **Country:** China
- **Models:** Yi-Large, Yi-Lightning, Yi-1.5 (6B/9B/34B, Apache 2.0)
- **Free Tier:** Free tokens on signup (amount unspecified). Pay-as-you-go thereafter.
- **API Endpoint:** `https://api.01.ai/v1/chat/completions` (OpenAI-compatible)
- **Signup:** Registration at platform.01.ai (international) or platform.lingyiwanwu.com (China)
- **International Access:** YES (platform.01.ai serves international users)
- **Best Use Case:** General chat, coding
- **Quality:** 7/10
- **stdlib Compatible:** YES (OpenAI-compatible)
- **Why NOT:** Lower profile in 2026, unclear free tier limits, better alternatives available (Qwen, DeepSeek). Models available free via OpenRouter anyway.

---

### D5. Perplexity -- USA

- **Country:** USA
- **Models:** Sonar (various sizes)
- **Free Tier:** None for API (search product is free, but API is paid)
- **API Endpoint:** `https://api.perplexity.ai/chat/completions`
- **Why NOT:** No free API tier. Remove from router free-tier strategy or use only as paid fallback.

---

### D6. Anyscale -- USA (Deprecated)

- **Country:** USA
- **Models:** Was offering Llama, Mistral via serverless
- **Free Tier:** Service deprecated / merged into Fireworks AI ecosystem
- **Why NOT:** No longer a standalone service. Remove from router.

---

## SPECIALIZED MODELS (Free Tiers)

### Code Models

| Model | Provider | Free Access | Quality |
|-------|----------|-------------|---------|
| Qwen3-Coder | OpenRouter | FREE (unlimited) | 9/10 |
| GPT-OSS 120B | OpenRouter/Cerebras | FREE | 8/10 |
| DeepSeek-Coder | DeepSeek API | 5M tokens free | 8.5/10 |
| Codestral | Mistral | Included in free tier | 8/10 |
| GLM-4.7-Flash | Zhipu AI | FREE (unlimited) | 7/10 |

### Reasoning/Thinking Models

| Model | Provider | Free Access | Quality |
|-------|----------|-------------|---------|
| DeepSeek R1 | OpenRouter | FREE | 9/10 |
| QwQ | OpenRouter/SiliconFlow | Varies | 8.5/10 |
| LFM 2.5 Thinking | OpenRouter | FREE | 6/10 |
| Gemini 2.5 Pro | Google AI Studio | 100 req/day free | 9/10 |
| Kimi K2.5 | NVIDIA NIM | FREE (40 RPM) | 8/10 |

### Embedding Models

| Model | Provider | Free Tier | Quality |
|-------|----------|-----------|---------|
| Voyage-3.5 | Voyage AI | 200M tokens free | 9/10 |
| jina-embeddings-v5 | Jina AI | Free tier available | 8.5/10 |
| Gemini Embedding 2 | Google | Free (rate-limited) | 8/10 |
| Mistral Embed | Mistral | Included in free tier | 7.5/10 |

### Vision/Multimodal Models

| Model | Provider | Free Access | Quality |
|-------|----------|-------------|---------|
| GLM-4.6V-Flash | Zhipu AI | FREE (unlimited) | 7/10 |
| Gemini 2.5 Flash | Google AI Studio | 250 req/day | 9/10 |
| Kimi VL | OpenRouter | FREE | 7.5/10 |
| Llama 3.2 Vision | Cloudflare/NIM | Free tier | 7/10 |

---

## RECOMMENDED ROUTER EXPANSION PLAN

### Phase 1: Immediate (add these 7 providers)

1. **Zhipu AI** -- Unlimited free flash models, OpenAI-compatible
2. **Cloudflare Workers AI** -- 10K neurons/day free, edge-deployed
3. **Hugging Face** -- 300+ models, monthly free credits
4. **Voyage AI** -- 200M free embedding tokens (for RAG)
5. **Jina AI** -- Free multilingual embeddings (for RAG)
6. **Cerebras** (UPDATE) -- Now has 1M tokens/day free and GPT-OSS
7. **OpenRouter** (UPDATE) -- Maximize 29 free models (Arcee, StepFun, Kimi, Liquid, etc.)

### Phase 2: Next Sprint (add these 5 providers)

8. **SiliconFlow** -- Chinese aggregator, cheapest rates globally
9. **AI21 Labs** -- $10 free credits, fast Jamba models
10. **Hyperbolic** -- Budget inference platform
11. **Moonshot (Kimi)** -- Via OpenRouter for 256K context tasks
12. **Reka AI** -- Multimodal capabilities

### Phase 3: Specialized (add as needed)

13. **Sarvam AI** -- Indian language tasks
14. **Writer Palmyra** -- Medical/Financial specialized models
15. **Lepton AI** -- Custom model hosting
16. **Novita AI** -- Ultra-cheap backup ($0.02/M tokens)

---

## TOTAL FREE CAPACITY ESTIMATE (After Full Integration)

| Resource | Daily Free Capacity |
|----------|-------------------|
| **Chat completions** | ~5,000-10,000 requests/day |
| **Tokens processed** | ~50M-100M tokens/day |
| **Embeddings** | ~200M tokens (Voyage) + Jina free tier |
| **Vision/Multimodal** | ~500-1,000 requests/day |
| **Code generation** | ~2,000-5,000 requests/day |

### Breakdown by Provider (Daily Free)

| Provider | Est. Daily Free Requests | Tokens/Day |
|----------|-------------------------|------------|
| Google Gemini | 1,350 (across 3 models) | ~250K TPM |
| Groq | ~14,400 | Varies by model |
| Cerebras | 1M tokens/day | 1M |
| Mistral | ~33M tokens/day (1B/mo) | 33M |
| NVIDIA NIM | ~57,600 (40 RPM) | N/A |
| SambaNova | N/A | 200K |
| OpenRouter (29 free) | ~200/day per model | Varies |
| Zhipu AI (NEW) | Unlimited | Unlimited |
| Cloudflare (NEW) | ~150 LLM responses | 10K neurons |
| Hugging Face (NEW) | Monthly credits | Varies |
| Cohere | ~33/day (1K/mo) | N/A |
| Together AI | Until $25 exhausted | N/A |
| Fireworks AI | Until $1 exhausted | N/A |
| DeepSeek | Until 5M tokens exhausted | N/A |

---

## API COMPATIBILITY MATRIX

All Tier A and most Tier B providers support OpenAI-compatible APIs, meaning integration requires only changing the base URL and API key. This is critical for the router.

| Provider | OpenAI-Compatible | Base URL |
|----------|-------------------|----------|
| Zhipu AI | YES | `https://open.bigmodel.cn/api/paas/v4` |
| SiliconFlow | YES | `https://api.siliconflow.cn/v1` |
| Cloudflare | NO (custom) | `https://api.cloudflare.com/client/v4/accounts/{id}/ai/run/` |
| Hugging Face | YES | `https://router.huggingface.co/v1` |
| OpenRouter | YES | `https://openrouter.ai/api/v1` |
| AI21 Labs | YES | `https://api.ai21.com/studio/v1` |
| Moonshot | YES | `https://api.moonshot.cn/v1` |
| Hyperbolic | YES | `https://api.hyperbolic.xyz/v1` |
| DeepSeek | YES | `https://api.deepseek.com/v1` |
| Cerebras | YES | `https://api.cerebras.ai/v1` |
| Reka | NO (custom) | `https://api.reka.ai/v1` |
| Voyage AI | Custom (embeddings) | `https://api.voyageai.com/v1` |
| Jina AI | Custom (embeddings) | `https://api.jina.ai/v1` |

---

## KEY FINDINGS

1. **Zhipu AI is the biggest win** -- Unlimited free GLM-4.7-Flash and GLM-4.5-Flash with no daily caps, OpenAI-compatible API, works internationally. This alone could handle a significant portion of our traffic at zero cost.

2. **OpenRouter now has 29 free models** -- Our existing OpenRouter integration should be expanded to rotate through ALL free models (Arcee Trinity, StepFun, Kimi, Liquid AI, GPT-OSS, Nemotron, etc.).

3. **Cerebras upgraded to 1M tokens/day** -- Our existing Cerebras integration should be updated to reflect this improved limit.

4. **Chinese providers dominate free/cheap tiers** -- Zhipu, SiliconFlow, Moonshot, and StepFun all offer generous free access with OpenAI-compatible APIs that work internationally.

5. **Embedding models have separate generous free tiers** -- Voyage AI (200M tokens) and Jina AI both offer substantial free embedding allowances that should be integrated for RAG.

6. **Several "free" providers require credit cards or have been deprecated** -- Anyscale (deprecated), Perplexity (no free API), and several Chinese providers (Baidu, ByteDance) are geo-restricted and should be deprioritized.

7. **Together AI increased free credits to $25** -- Up from the previously documented $5.

8. **Google Gemini reduced free limits in Dec 2025** -- Down 50-80% from previous levels. Still generous but not as dominant as before.

---

## SOURCES

- [Alibaba Cloud Model Studio Pricing](https://www.alibabacloud.com/help/en/model-studio/model-pricing)
- [Zhipu AI Pricing](https://open.bigmodel.cn/pricing)
- [SiliconFlow Pricing](https://www.siliconflow.com/pricing)
- [Moonshot AI / Kimi Pricing](https://platform.moonshot.ai/docs/pricing/chat)
- [Cloudflare Workers AI Pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/)
- [Hugging Face Pricing](https://huggingface.co/pricing)
- [OpenRouter Free Models](https://openrouter.ai/collections/free-models)
- [AI21 Labs Pricing](https://www.ai21.com/pricing/)
- [Reka AI Pricing](https://docs.reka.ai/pricing)
- [Sarvam AI Pricing](https://www.sarvam.ai/api-pricing)
- [Krutrim AI Labs](https://ai-labs.olakrutrim.com/)
- [DeepSeek Pricing](https://api-docs.deepseek.com/quick_start/pricing)
- [Cerebras Pricing](https://www.cerebras.ai/pricing)
- [Groq Pricing](https://groq.com/pricing)
- [Mistral AI Pricing](https://mistral.ai/pricing)
- [NVIDIA NIM](https://developer.nvidia.com/nim)
- [Fireworks AI Pricing](https://fireworks.ai/pricing)
- [Together AI Pricing](https://www.together.ai/pricing)
- [Cohere Rate Limits](https://docs.cohere.com/docs/rate-limits)
- [Voyage AI Pricing](https://docs.voyageai.com/docs/pricing)
- [Jina AI Embeddings](https://jina.ai/embeddings/)
- [Hyperbolic Pricing](https://docs.hyperbolic.xyz/docs/hyperbolic-pricing)
- [Aleph Alpha](https://app.aleph-alpha.com/)
- [Writer AI Pricing](https://dev.writer.com/home/pricing)
- [Free LLM API Directory](https://free-llm.com/)
- [GitHub: free-llm-api-resources](https://github.com/cheahjs/free-llm-api-resources)
- [Gemini API Rate Limits](https://ai.google.dev/gemini-api/docs/rate-limits)
- [Google AI Studio](https://aistudio.google.dev/)
- [StepFun Pricing](https://platform.stepfun.ai/docs/en/pricing/details)
- [MiniMax Pricing](https://platform.minimax.io/docs/pricing/overview)
- [Arcee AI Trinity](https://www.arcee.ai/trinity)
- [Novita AI Pricing](https://novita.ai/pricing)
- [Lepton AI Pricing](https://www.lepton.ai/pricing)
- [SambaNova Rate Limits](https://docs.sambanova.ai/cloud/docs/get-started/rate-limits)

---

*Last updated: March 24, 2026*
*Research conducted for Nova AI Suite LLM Router expansion*
