# Pending — Needs Shubham's Action (for next session)

**Created:** 2026-06-02. These items were deliberately **deferred** because they
depend on a key, credential, signup, or a data decision only you can make. Each
was held back to honor "do whatever is safe without my intervention." Pick them
up in a future session once you've provided the prerequisite.

---

## 1. RAG Phase 2 — confirm embeddings key / provider
- **Status:** RAG was enabled (S83) then the Render build was hotfixed (S84,
  removed SDK deps that broke Python 3.13). Verify `query_kb_semantic` actually
  returns embeddings in production, not a `rag_disabled` / cold path.
- **Needs you:** decide the embeddings provider + key:
  - **Voyage AI** (`VOYAGE_API_KEY`) — 200M free account tokens, voyage-4-lite,
    512 dims (best retrieval quality for the cost), OR
  - reuse the existing **`JINA_API_KEY`** (10M free) with Jina v3, OR
  - self-host `sentence-transformers` (no key, but adds Render memory load).
- **Vector store:** Supabase **pgvector** needs no new key (you already pay for
  Supabase) — `CREATE EXTENSION vector;` + a VECTOR column. Qdrant Cloud free
  tier also works.
- **Action:** provide the chosen embeddings key, then say "wire RAG embeddings."

## 2. (PAID — NOT requesting) DeepSeek V4-Flash router slot
- Skipped per your "free-tier only" rule. DeepSeek has no free tier as of
  2026-06, so this is **not** an ask. Listed only for awareness: if you ever
  decide to add a paid `DEEPSEEK_API_KEY`, it is a cheap tool-use fallback.
- The free side is already handled: GLM is current (`glm-4.7-flash`, free) and
  the router has multiple free providers (Groq, Cerebras, Gemini Flash, etc.).

## 3. Refresh 4 stale-vintage benchmark sections (data decision)
The new vintage-aware KB check (shipped today) flagged these as **2024/2025
vintage feeding 2026 plans** (they passed the old mtime check, so this was
invisible before):
| Section | Newest vintage | File |
|---|---|---|
| `google_ads_benchmarks` | 2025 | `data/google_ads_2025_benchmarks.json` |
| `recruitment_benchmarks_2026_deep` | 2025 | (verify source file) |
| `employer_career_intelligence_2026` | 2025 | (verify source file) |
| `healthcare_specialty_pay_2026` | 2024 | (verify source file) |
- **Needs you (or a sourcing pass):** verified 2026 figures. I did **not**
  fabricate replacements. `external_benchmarks_2025.json` already contains an
  `appcast_benchmark_2026_preview` block that could be promoted to primary once
  you confirm it's complete.
- **Action:** approve a sourcing pass (BLS/Appcast/Indeed Hiring Lab 2026) or
  point me at the authoritative 2026 numbers.

## 4. Optional free-API signups (no payment, but need registration)
Recommended in research; each needs a free self-serve key before wiring:
- **Lightcast Open Skills** (OAuth client creds) — 50 skill extractions/mo free.
- **JobTech Dev / Platsbanken** (free key) — Nordic job feed + taxonomy.
- **OpenCorporates** (free key, ~500 calls/mo) — employer firmographics.
- **Companies House UK** (`COMPANIES_HOUSE_API_KEY`, free) — function already
  built + wired; just unset, returns a graceful "register a key" message today.
- No-key APIs already shipped/wired: DBnomics, ILOSTAT, World Bank, ESCO,
  Frankfurter-class FX is still TODO (no key, can wire anytime).

---

---

## Deferred for RISK, not forgotten (medium-risk code — do in a focused session)
These were intentionally NOT done late in a busy session to honor "without
breaking anything." Each is safe in isolation but touches sensitive/shared code
and deserves its own test pass. No key needed for any of them:
- **nova_memory bounded queue** (R4): replace per-write thread spawning with one
  bounded queue+worker — reduces thread spikes on the dyno.
- **http_pool adoption** (P2): route hot same-host collectors (BLS/Census/FRED/
  Adzuna) through the existing keep-alive pool — ~100-200ms/call saved.
- **Few-shot exemplars in cached router prompt** (Q5): inject 1-2 gold plans
  from `gold_standard.py` into the now-cached system block (free tokens, better
  consistency). Touches the plan-gen prompt — verify output first.
- **PPT native charts** (Q4): swap matplotlib-PNG charts for editable pptx
  charts. Higher effort.
- **Status colors -> CSS vars** (D2): centralize `#34d399/#f87171/#fbbf24` as
  `--success/--danger/--warning` tokens across templates.

## Free, NO-KEY future builds (no action from you; just build time)
- **Frankfurter FX** (api.frankfurter.dev, no key): live currency rates for
  intl salary/CPC normalization. Touches the shared currency path, so test both
  products. No key, free.
- **ATS hiring-signal feeds** (Greenhouse/Lever/Ashby public JSON, no key):
  real-time hiring velocity for competitive intel.
- **Cloudflare Workers cache** (free tier): edge caching proxy in front of
  Render — cuts cold-start exposure. Needs a CF account (free) but no paid plan.
- **QStash cron/queue** (your existing Upstash account, free tier): managed
  cron for KB refresh / report jobs.

---

## Already DONE autonomously (2026-06-02, no action needed)
- Reliability: `self._conversations` crash fix, rule-based fallback guard,
  conversation-cache LRU cap.
- Cost: Anthropic prompt caching on the plan-gen router path.
- Output parity: cited 2026 block now in the python-pptx fallback deck (was
  Google-Slides-only); silent deck-tier degradation now logs loudly.
- Latest-content visibility: vintage-aware KB freshness check (this surfaced §3).
- Best-in-class output: native editable PieChart in the Excel workbook.
- Shipped pending new-API layers (DBnomics/ILOSTAT/ESCO/Companies House) + tests.
