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

## 2. DeepSeek V4-Flash router slot (cheap tool-use fallback)
- **Why:** $0.14/$0.28 per Mtok, 1M context, OpenAI-compatible — a near-free
  fallback for classification/JSON/tool-use steps now using Haiku budget.
- **Needs you:** a **`DEEPSEEK_API_KEY`** (paid; no free tier exists as of
  2026-06). The model-ID/router wiring is ~15 min once the key is set.
- Note: GLM is already current (`glm-4.7-flash`, free) — no action needed there.

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

## Already DONE autonomously (2026-06-02, no action needed)
- Reliability: `self._conversations` crash fix, rule-based fallback guard,
  conversation-cache LRU cap.
- Cost: Anthropic prompt caching on the plan-gen router path.
- Output parity: cited 2026 block now in the python-pptx fallback deck (was
  Google-Slides-only); silent deck-tier degradation now logs loudly.
- Latest-content visibility: vintage-aware KB freshness check (this surfaced §3).
- Best-in-class output: native editable PieChart in the Excel workbook.
- Shipped pending new-API layers (DBnomics/ILOSTAT/ESCO/Companies House) + tests.
