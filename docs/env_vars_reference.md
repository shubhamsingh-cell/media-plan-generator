# Render Env Vars Reference (S50)

**Scope:** every env var the LLM router, recruitment APIs, and S50 feature flags consult.
**Source of truth:** model strings come from `os.environ.get(...) or "<default>"` in `llm_router.py`, `app.py:8592`, and `nova.py:518`. Read this doc first before changing any of them on Render.

---

## 1. Active LLM models (S50 overrides)

Each row corresponds to a single LLM provider config. The default is what production runs when the env var is unset; the rollback value reverts to the pre-S50 model with no redeploy.

| Env var | File:line | Controls | Default (post-S50) | Rollback value |
|---|---|---|---|---|
| `OPENAI_MODEL` | `llm_router.py:823` | OpenAI model used by the `GPT4O` provider in `PROVIDER_CONFIG` | `gpt-5.4-mini` | `gpt-4o` |
| `CLAUDE_SONNET_MODEL` | `llm_router.py:890` | Anthropic Sonnet model used by the `CLAUDE` provider | `claude-sonnet-4-6` | `claude-sonnet-4-20250514` |
| `CLAUDE_OPUS_MODEL` | `llm_router.py:904` | Anthropic Opus model used by the `CLAUDE_OPUS` provider | `claude-opus-4-7` | `claude-opus-4-20250514` |
| `OPENROUTER_MODEL` | `llm_router.py:625` | OpenRouter model for the primary `OPENROUTER` provider entry | `qwen/qwen3-coder:free` | `meta-llama/llama-4-maverick:free` |
| `OPENROUTER_GPT_OSS_MODEL` | `llm_router.py:839` | OpenRouter model for the `OPENROUTER_GPT_OSS` provider (S50 addition) | `openai/gpt-oss-120b:free` | Unset and remove provider from routing lists |
| `CEREBRAS_SCOUT_MODEL` | `llm_router.py:865` | Cerebras model for the `CEREBRAS_SCOUT` provider (S50 addition) | `qwen-3-235b-a22b-instruct-2507` | `gpt-oss-120b` (alt verified Cerebras slug) |
| `MISTRAL_MODEL` | `llm_router.py:609` | Mistral model used by the `MISTRAL` provider | `mistral-small-2603` (pinned) | `mistral-small-latest` (rolling alias) |
| `APP_FALLBACK_SONNET_MODEL` | `app.py:8592` | Direct Anthropic API call in `app.py` that bypasses the LLM router | `claude-sonnet-4-6` | `claude-sonnet-4-20250514` |
| `CLAUDE_MODEL_COMPLEX` | `nova.py:518` | Direct Anthropic API call in `nova.py` for complex queries (bypass router) | `claude-sonnet-4-6` | `claude-sonnet-4-20250514` |

**Why two non-router callsites exist (`app.py:8592`, `nova.py:518`):** these are intentional bypasses of the LLM router for direct Anthropic API use. They originally hardcoded the May-2025 Claude string. S50 made them env-overridable. If you migrate them onto the router in a future session, drop the env vars at the same time.

**Recovery commands** (paste into Render env settings; restart the service):

```bash
# Full rollback of every Tier-1 LLM string to pre-S50 defaults
OPENAI_MODEL=gpt-4o
CLAUDE_SONNET_MODEL=claude-sonnet-4-20250514
CLAUDE_OPUS_MODEL=claude-opus-4-20250514
OPENROUTER_MODEL=meta-llama/llama-4-maverick:free
MISTRAL_MODEL=mistral-small-latest
APP_FALLBACK_SONNET_MODEL=claude-sonnet-4-20250514
CLAUDE_MODEL_COMPLEX=claude-sonnet-4-20250514
```

---

## 2. API keys for new tools (S50)

These keys gate the four S50 tool integrations that require external accounts. All four degrade gracefully when unset (return stub responses, do not throw).

| Env var | Required by | Behavior when unset | Where to obtain |
|---|---|---|---|
| `CRUNCHBASE_API_KEY` | `recruitment_apis.lookup_company_crunchbase()` | Returns `{"company": ..., "source": "Crunchbase", "note": "CRUNCHBASE_API_KEY not set; sign up at crunchbase.com/api"}` | crunchbase.com/api (paid) |
| `PDL_API_KEY` | `recruitment_apis.enrich_person_pdl()` | Returns `{"person": ..., "source": "PeopleDataLabs", "note": "PDL_API_KEY not set; sign up free at peopledatalabs.com (100 lookups/mo)"}` | peopledatalabs.com (freemium, 100 lookups/mo) |
| `LANGFUSE_PUBLIC_KEY` | Langfuse self-hosted observability (if enabled by future plumbing) | Langfuse client initialization is skipped | langfuse.com or self-hosted |
| `LANGFUSE_SECRET_KEY` | Langfuse self-hosted observability | Langfuse client initialization is skipped | langfuse.com or self-hosted |
| `LANGFUSE_HOST` | Langfuse self-hosted observability — base URL of the Langfuse server | Skipped | Self-hosted Langfuse instance URL |
| `LITELLM_API_KEY` | LiteLLM proxy (if proxy is fronting LLM calls) | LiteLLM proxy disabled, direct calls used | LiteLLM proxy admin |
| `VOYAGE_API_KEY` | `vector_search._rerank_with_voyage()` and embeddings | `_get_api_key()` returns `None`; rerank falls back to keyword overlap, embeddings cannot run | voyageai.com |

**Verification:** all four `lookup_company_crunchbase`, `enrich_person_pdl`, `_rerank_with_voyage` paths return a clean stub or `None` when their key is missing — no traceback, no 500, no production user-visible failure.

---

## 3. Feature flags introduced in S50

Boolean / string flags that alter S50-era code paths. All default to `false` / off.

| Env var | Controls | Default | Truthy values |
|---|---|---|---|
| `CRAWL4AI_ENABLED` | Optional Crawl4AI extractor path (planned future plumbing) | unset (`false`) | `1`, `true`, `yes` |
| `STAGEHAND_ENABLED` | Stagehand browser automation feature flag | unset (`false`) | `1`, `true`, `yes` |
| `STAGEHAND_API_URL` | Stagehand server URL when `STAGEHAND_ENABLED=true` | unset | Full URL (`https://...`) |
| `STAGEHAND_API_KEY` | Stagehand auth header value | unset | API key string |

**Pattern:** any code path consuming these flags must check via `os.environ.get(name, "").lower() in ("1", "true", "yes")` and degrade to the existing path when unset. No S50 production code currently activates these — they are reserved for future feature work.

---

## 4. Pre-existing env vars referenced by S50 code

These were already present before S50 but are read by new S50 callsites; documented here so a future operator does not delete them by mistake.

| Env var | S50 consumer | Original consumer |
|---|---|---|
| `OPENAI_API_KEY` | `GPT4O.env_key` (router) | All OpenAI chat completions |
| `ANTHROPIC_API_KEY` | `CLAUDE.env_key`, `CLAUDE_OPUS.env_key`, `CLAUDE_HAIKU.env_key` (router); `app.py:8602`; `nova.py` | All Anthropic Messages API calls |
| `OPENROUTER_API_KEY` | `OPENROUTER.env_key`, `OPENROUTER_GPT_OSS.env_key` | All OpenRouter chat completions |
| `CEREBRAS_API_KEY` | `CEREBRAS.env_key`, `CEREBRAS_SCOUT.env_key` | Cerebras chat completions |
| `MISTRAL_API_KEY` | `MISTRAL.env_key` | Mistral chat completions |
| `VOYAGE_API_KEY` | `vector_search._rerank_with_voyage()` (S50 new) and existing embedding path | Existing Voyage embeddings |
| `QDRANT_URL`, `QDRANT_API_KEY` | `scripts/migrate_voyage_4.py` reads both | Existing Qdrant integration |

---

## 5. Quick verification checklist after env var changes

After any change to the variables in section 1, run the following to confirm the live PROVIDER_CONFIG took the override:

```bash
# Local sanity check (run from media-plan-generator/)
python3 -c "
import os
import llm_router
from llm_router import (
    PROVIDER_CONFIG, GPT4O, CLAUDE, CLAUDE_OPUS, OPENROUTER,
    OPENROUTER_GPT_OSS, CEREBRAS_SCOUT, MISTRAL,
)
for pid in (GPT4O, CLAUDE, CLAUDE_OPUS, OPENROUTER, OPENROUTER_GPT_OSS,
            CEREBRAS_SCOUT, MISTRAL):
    print(f'{pid:20} -> {PROVIDER_CONFIG[pid][\"model\"]}')
"
```

Expected post-S50 default output:
```
gpt4o                -> gpt-5.4-mini
claude               -> claude-sonnet-4-6
claude_opus          -> claude-opus-4-7
openrouter           -> qwen/qwen3-coder:free
openrouter_gpt_oss   -> openai/gpt-oss-120b:free
cerebras_scout       -> qwen-3-235b-a22b-instruct-2507
mistral              -> mistral-small-2603
```

**On Render:** set the env var, redeploy is **not required** — Render restarts the service on env change, which re-imports `llm_router` and picks up the new value. Confirm via `/api/health` (admin auth) which surfaces the active model strings.

---

## 6. Things that are NOT env-controllable

For visibility — a future operator should not waste time looking for an override that doesn't exist:

- **Voyage rerank model** (`_VOYAGE_RERANK_MODEL = "rerank-2.5-lite"` at `vector_search.py:82`): hardcoded. To disable, unset `VOYAGE_API_KEY` to force the keyword-overlap fallback path.
- **Voyage embedding model** (`_VOYAGE_MODEL = "voyage-3-lite"` at `vector_search.py:73`): hardcoded. The voyage-4 migration uses `scripts/migrate_voyage_4.py` — see `docs/voyage_4_migration_runbook.md`.
- **Claude Haiku model** (`claude-haiku-4-5-20251001` at `llm_router.py:876`): hardcoded. Bumping Haiku is a code change, not an env-var flip — it is the LLM router PRIMARY provider and changing it warrants a deliberate redeploy.
- **Edge router display labels** (`edge_router.py:130-147`): cosmetic strings only, used for the public health endpoint. No env override.
- **Health endpoint display labels** (`routes/health.py:818, 823`): same — cosmetic only.
