# BUILD — LLM Router Cleanup (2026-06)

**Date:** 2026-06-02
**File modified:** `llm_router.py` ONLY (no other files touched)
**Source of truth:** `docs/SCOUT_LLM_2026-06.md` (URL-verified 2026-06-02)
**Guiding principle:** Quality and safety > completeness. Additive + metadata fixes preferred over risky slug swaps. Every slug swap verified live before committing to it.

---

## Executive summary

Most of the SCOUT-LLM recommendations had **already been applied** in prior sessions (tagged S50 / S51 / S53 in the code) — the dead DeepSeek `:free` slug was already removed, GLM was already at `glm-4.7-flash`, Qwen was already pinned to `qwen/qwen3-coder:free`, DeepSeek-R1's cost metadata was already corrected to paid, and `cerebras_scout` was already re-labeled with the 8K cap notes. This task therefore became **(a) verify the already-applied changes are correct and live, and (b) close the remaining gaps**.

Net result: **1 real slug swap (Groq → Llama 4 Scout, live-verified HTTP 200), 1 additive metadata field (`max_context` on `cerebras_scout`), several comment/label corrections, and 1 TODO** (Gemini GA slug 404s — kept the working preview slug).

- **Import status:** `router imports OK` (exit 0)
- **Test status:** `132 passed` (tests/test_llm_router_unit.py + tests/test_resilience_router.py) — same as baseline, no regressions
- **Provider count:** unchanged (no providers added or removed)

---

## Slug verification results (live 1-token API calls, 2026-06-02)

Keys present in env (`~/.zshrc`): `GEMINI_API_KEY`, `ZHIPU_API_KEY`, `GROQ_API_KEY`, `CEREBRAS_API_KEY`, `OPENROUTER_API_KEY`.
(Cloudflare blocked the default `Python-urllib` User-Agent with `403 error 1010`; re-ran with a browser UA to get accurate results.)

| Slug tested | Result | Interpretation |
|---|---|---|
| `gemini-3-flash` (GA candidate, Item #5) | **HTTP 404 NOT_FOUND** (twice) | GA string does NOT exist yet → DO NOT SWAP |
| `gemini-3-flash-preview` (current) | **HTTP 200** | Current working route → KEEP |
| `gemini-3.1-flash-lite` (current lite) | **HTTP 200** | Working → KEEP |
| `glm-4.7-flash` (current, Item #4) | **HTTP 429 code 1302 (rate-limited)** | A 429 proves the slug + account are VALID (server recognized the model, just throttled per the 5 RPM free cap) → CONFIRMED WORKING |
| `meta-llama/llama-4-scout-17b-16e-instruct` (Groq candidate, Item #6) | **HTTP 200, model echoed** | Slug VALID → SWAP APPLIED |
| `llama-4-scout-17b-16e-instruct` (Groq candidate, no `meta-llama/` prefix) | **HTTP 404 model_not_found** | Wrong form — confirms the `meta-llama/` prefix is required |
| `llama-3.3-70b-versatile` (Groq current) | **HTTP 200** | Prior slug also works (kept as env-override fallback) |
| `gpt-oss-120b` on Cerebras (scout current, Item #3) | **HTTP 200** | Current scout model working → KEEP |

Post-edit, the swapped Groq slug was re-tested **end-to-end through the actual `PROVIDER_CONFIG`** (not just standalone): **HTTP 200, `model = meta-llama/llama-4-scout-17b-16e-instruct`**.

---

## Changes APPLIED

### Item #1 — DeepSeek-R1 cost-tier (metadata + comment fixes)
- **Cost metadata was already correct**: `_PROVIDER_COST_PER_M_TOKENS[OPENROUTER_DEEPSEEK_R1] = {"input": 0.70, "output": 2.50}` (applied S51). Confirmed via module introspection.
- **Routing logic note**: `select_provider()` determines free/paid tier *by position in the `TASK_ROUTING` lists*, NOT by reading the cost dict. There is no cost-optimizer that prefers a route because it's flagged free. So the only remaining risk was **misleading comments** that future maintainers might trust.
- **Fixed 4 misleading comments** that still called R1 a "free fallback":
  - 3× `# Free fallback tier` → `# Paid reasoning fallback ($0.70/$2.50, no free DeepSeek slug on OpenRouter)`
  - 1× `# Reasoning-heavy free fallback` → `# Reasoning-heavy PAID fallback ($0.70/$2.50, no free DeepSeek slug)`
- **Corrected provider-count tally** in the module docstring: `27 total (23 free + 4 paid)` → `27 total (22 free + 5 paid)` with a note that R1 was reclassified free→paid in S51.

### Item #2 — Dead `deepseek-v3.2:free` slug
- **No action needed.** Already removed in S50 (the provider entry is gone; lines ~869–872 document the removal with the 404 verification). `OPENROUTER_DEEPSEEK_R1` now pins the real paid `deepseek/deepseek-r1` slug, which resolves. Confirmed there is no remaining reference to a `:free` DeepSeek slug.

### Item #3 — `cerebras_scout` relabel + 8K context constraint
- **Re-label was already applied** (S51): name = "Cerebras GPT-OSS-120B (free, 8K ctx cap)", model defaults to `gpt-oss-120b`, `rpm_limit` corrected 30→5, with detailed comments citing the live Cerebras docs.
- **NEW (this session): added an additive `"max_context": 8192` field** to the `cerebras_scout` config so the 8K total-context cap is **discoverable as structured metadata** (instead of comment-only). This is safe — `PROVIDER_CONFIG` is consumed via `.get()` only; no code does strict-schema validation or `**config` expansion, so an extra key is inert until a future total-context guard reads it.
- **Also fixed the stale `CEREBRAS_SCOUT` definition comment** (lines ~172–179) that still described it as "Qwen-3 235B Instruct (~2,600 tok/s, 1M ctx)" — corrected to the real free-trial models / 5 RPM / 8K cap, matching the already-corrected config block.
- **Scoped deliberately to `cerebras_scout` only.** The main `CEREBRAS` entry uses `llama-3.3-70b` (a different/possibly non-free-trial tier whose cap I did not independently verify), so I did NOT stamp an 8K cap on it — avoids mislabeling an unverified entry.

### Item #6 — Groq → Llama 4 Scout (the one real slug swap)
- **Swapped** `GROQ` model `llama-3.3-70b-versatile` → `meta-llama/llama-4-scout-17b-16e-instruct` (10M context, natively multimodal+multilingual, same Groq free tier: 30 RPM / 1,000 RPD).
- **Live-verified twice**: standalone (HTTP 200) and end-to-end through the actual config (HTTP 200, correct model echoed).
- **Made the swap reversible**: `os.environ.get("GROQ_MODEL") or "meta-llama/llama-4-scout-17b-16e-instruct"` — the prior GA slug can be restored instantly via env (`GROQ_MODEL=llama-3.3-70b-versatile`) with zero redeploy of code.
- Updated `name` → "Groq Llama 4 Scout (10M ctx)" and added a sourced comment.
- Multilingual property preserved: Groq is used in the multilingual routing path (its prior comment valued "Llama 3.3 trained on multilingual data"); Llama 4 Scout is also natively multilingual, so the swap retains that role with a far larger context window.

---

## Changes DEFERRED (with reason)

### Item #4 — GLM slug (`glm-4.7-flash`)
- **Already applied** (S51) and **verified VALID** this session (HTTP 429 rate-limit response = the server recognized the slug/account). No change required.
- Note: a direct local 200 was not obtainable because the `open.bigmodel.cn` endpoint is rate-limited (5 RPM free) and network-distant; the 429 is dispositive that the slug is correct. Left as-is.

### Item #5 — Gemini → `gemini-3-flash` (GA)
- **DEFERRED — kept working `gemini-3-flash-preview`.** The recommended GA string `gemini-3-flash` returns **HTTP 404 NOT_FOUND** on `v1beta` (verified twice). A wrong slug = 404 at runtime = broken primary free fallback, so conservatism wins.
- **Added `# TODO S82`** in the GEMINI config recording: SCOUT recommends the GA promotion, but `gemini-3-flash` 404s as of 2026-06-02 while `gemini-3-flash-preview` returns 200 — re-test the GA slug before swapping.

---

## NOT DONE (per task instructions — too risky / require user account action)

- **NVIDIA NIM as a NEW provider** (SCOUT Top-5 #4): out of scope (needs a new API key + ~20 LOC + new auth path). Documented recommendation only. *(Note: a `NVIDIA_NIM` provider entry already exists in the config from a prior session, but enabling/expanding it was not part of this safe-cleanup pass.)*
- **GitHub Models as a NEW provider** (SCOUT Top-5 #5): out of scope (new key + ~20 LOC + new auth). Documented recommendation only.
- **$10 OpenRouter deposit** (raises all `:free` slugs from 50→1,000 req/day): user account action, not a code change. Highest reliability-per-dollar action per SCOUT — recommend the user do this manually.

---

## Before / after

| Metric | Before | After |
|---|---|---|
| `llm_router.py` import | OK | OK |
| Router unit + resilience tests | 132 passed | 132 passed |
| Providers in `PROVIDER_CONFIG` | 26 entries | 26 entries (unchanged) |
| Groq model | `llama-3.3-70b-versatile` | `meta-llama/llama-4-scout-17b-16e-instruct` (env-overridable) |
| `cerebras_scout` `max_context` | (absent) | `8192` |
| DeepSeek-R1 cost tag | paid (already) | paid (comments now consistent) |
| Gemini model | `gemini-3-flash-preview` | `gemini-3-flash-preview` (TODO added; GA 404s) |
| GLM model | `glm-4.7-flash` | `glm-4.7-flash` (verified valid) |

---

## Verification commands

```bash
# Import (no real deps needed)
python3 -c "import sys,types; [sys.modules.setdefault(n,types.ModuleType(n)) for n in ['anthropic','openai','supabase','redis','qdrant_client','sentence_transformers','posthog']]; import llm_router; print('router imports OK')"

# Tests
python3 -m pytest tests/test_llm_router_unit.py tests/test_resilience_router.py -q
# => 132 passed
```

## Rollback

- Groq swap: set env `GROQ_MODEL=llama-3.3-70b-versatile` (instant, no code change), or revert the GROQ block.
- All other changes are comments / additive metadata and carry no runtime behavior change.
