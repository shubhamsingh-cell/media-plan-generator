# Nova Quality Regression Eval — 2026-05-22

**Endpoint:** `https://media-plan-generator.onrender.com/api/chat`
**Build under test:** Post S76+S77 (country-awareness + brain-layer wiring + Tier 3 intelligence)
**Methodology:** 18 golden queries, sequential HTTP POST, 2s gap, unique conversation IDs, no caching, observational only (no code changes).
**Run timestamp:** 2026-05-22 (UTC)

---

## Headline Verdict

**Pass rate: 15/18 (83%)** — Solid baseline with two clear country-localization regressions and one timeout-induced fail.

| Aggregate | Value |
|-----------|-------|
| Pass | 15/18 (83%) |
| Fail | 3/18 (17%) |
| Latency p50 (200 OK only, n=16) | 14.4 s |
| Latency p90 (200 OK only) | 36.8 s |
| Latency p99 / max (200 OK only) | 55.8 s |
| Budget breaches | 4/18 (22%) |
| HTTP timeouts | 2/18 (11%) |
| Tier 3 `sources` block present | 15/18 (83%) |
| Tier 3 `<!-- t3-citations -->` marker | 10/18 (56%) |
| Tier 3 `<!-- t3-numver -->` (number verifier) | 10/18 (56%) |
| `[unverified]` tags leaked into prose | 4/18 (22%) |
| `API key not configured` leaks | 0/18 (0%) ✅ |

---

## Per-Test Result Table

| # | Query | Category | HTTP | Latency / Budget | Must-have hits | Leaks | Tier 3 | Pass |
|---|-------|----------|------|------------------|----------------|-------|--------|------|
| 1 | whats the average cpa in the uk for registered nurses | UK country (orig bug) | 200 | 2.6 s / 30 s | £, Indeed UK | — | sources | PASS |
| 2 | cpa for nurses in london | UK city alone | 200 | 11.3 s / 30 s | £, GBP, Reed, Indeed UK | — | sources, t3-cit, t3-numver | PASS |
| 3 | salary for software engineer in mumbai | India currency | 200 | 0.9 s / 30 s | ₹ | — | sources | PASS |
| 4 | cpc for tech in berlin | Germany country | 200 | 14.7 s / 30 s | **none** (expected € or EUR) | — | sources, t3-cit, t3-numver, **[unverified]** | **FAIL** |
| 5 | cpa for healthcare in dubai | UAE | 200 | 12.7 s / 30 s | AED, Dubai | — | sources, t3-cit, t3-numver | PASS |
| 6 | cpa for nursing | US default | 200 | 1.6 s / 15 s | US national | — | sources | PASS |
| 7 | cpa for nursing in chicago | US metro | 200 | 1.1 s / 15 s | Chicago | — | sources | PASS |
| 8 | compare nurse CPA in UK vs Germany | Multi-country | 200 | 14.4 s / 45 s | £, €, UK, Germany | — | sources, t3-cit, t3-numver | PASS |
| 9 | best job boards in india for tech | Country-specific listings | 200 | 1.9 s / 30 s | Naukri | — | sources | PASS |
| 10 | plan a $50K hiring budget for engineers in india | Currency + country mix | 200 | 34.6 s / 45 s | Naukri, India | — | sources, t3-cit, t3-numver, **[unverified]** | PASS |
| 11 | engineer in cambridge | Ambiguous (UK/MA) | 200 | 2.2 s / 30 s | (clarify check) | — | none | PASS |
| 12 | what's the labor force participation rate in germany 2026 | OECD-related | **timeout** | **61.1 s** / 30 s | — | — | — | **FAIL** (timeout) |
| 13 | how is nova's data sourced? | Meta-Q transparency | 200 | 55.8 s / 30 s | (2 sources mentioned) | — | sources, t3-cit, t3-numver, **[unverified]** | PASS (over budget) |
| 14 | what's the cpc for healthcare workers in canada | Canada CAD | **timeout** | **60.2 s** / 30 s | **none on retry** (no CAD) | — | — | **FAIL** |
| 15 | demand for python developers in singapore | Singapore + role | 200 | 15.4 s / 30 s | Singapore | — | sources, t3-cit, t3-numver | PASS |
| 16 | build a media plan for hiring 5 RNs in dallas tx | US plan generation | 200 | 31.9 s / 60 s | TX, Dallas | — | sources, t3-cit, t3-numver, **[unverified]** | PASS |
| 17 | what are the top TA leaders to follow? | TA-leader KB query | 200 | 18.7 s / 30 s | (Matt Charney cited) | — | sources, t3-cit, t3-numver | PASS |
| 18 | show me past client media plans for healthcare | Client plans KB | 200 | 36.8 s / 30 s | (graceful, no fabrication) | — | sources, t3-cit, t3-numver | PASS (over budget) |

---

## Top 3 Issues Found

### 1. CRITICAL — Country-aware currency localization regresses on Germany & Canada CPC tool path

Two of the three failures are the same shape: when a country is named in the query but the answer goes through the **CPC Trend Tracker / Adzuna tool**, the localizer is bypassed and the response stays in USD ($).

**Test 4 — Berlin, full body excerpt (re-fetched, 1,769 chars):**
> "Cost-per-click (CPC) for tech roles in Berlin has surged 45%… with LinkedIn reaching a premium high of **$7.63**.
>
> | Platform | Current CPC | … |
> | **LinkedIn** | **$7.63** | … |
> | **ZipRecruiter** | **$1.96** | … |
> | **Indeed** | **$1.33** | … |
>
> *Data Source: Adzuna API + Benchmarks*"

Zero `€` or `EUR` in the entire response — only `$` (11 occurrences).

**Test 14 — Canada (on 22.5 s retry, 1,162 chars):**
> "indeed: 0.78, linkedin: 4.48, ziprecruiter: 1.15… data_source: Adzuna API + benchmarks"

Zero CAD / C$ symbols. The CPC numbers are clearly US default benchmarks (Indeed 0.92, LinkedIn 5.26, ZipRecruiter 1.35) with a flat ‑15% modifier — same shape as the Berlin output, suggesting a single shared CPC tool ignoring the country argument.

**Compare with what works:** Tests 1, 2, 5, 8 (UK CPA, UK CPA, Dubai CPA, UK vs Germany CPA) all localize correctly. The CPA tool path is country-aware; the CPC tool path is not.

**Root-cause hypothesis (to verify next session):** `cpc_trend_tracker` / Adzuna handler in the brain layer is being called without a `country` argument, or it discards it and falls back to its US benchmark dict. The S76+S77 country routing covers CPA but not CPC.

### 2. HIGH — Latency tail breaches 30 s budget on KB / OECD / transparency queries

Four tests breached their 30 s budget. Two timed out the 60 s client timeout entirely (Tests 12 and 14). Looking at successful retries: the OECD SDMX path (Test 12) genuinely takes ~55 s — it's working, but the user-facing budget is unrealistic.

| Test | Path | Actual | Budget |
|------|------|--------|--------|
| 12 | OECD SDMX `labour_force` | 55.4 s (on retry) | 30 s |
| 13 | KB self-introspection | 55.8 s | 30 s |
| 14 | CPC tool (Canada) | 22.5 s (on retry) | 30 s |
| 18 | KB client-plans search | 36.8 s | 30 s |

P50 at 14.4 s and p90 at 36.8 s are healthy, but the **p99 of 55.8 s** drops below the documented SLO for `chat P99 80s` so this is **within SLO** — yet user-perceived because the original timeout used by the test harness (60 s on first run) clipped two queries.

### 3. MEDIUM — `[unverified]` tags leaking into user-visible prose on 4/18 (22%)

Examples from Tests 4, 10, 13, 16:
- Test 4: `surged **4**5%** above historical benchmarks** _[unverified]_ across all major platforms`
- Test 10: budget plan response contains `_[unverified]_` inline
- Test 16: Dallas RN plan contains `_[unverified]_` annotations next to numbers

These appear to be number-verifier markers (`t3-numver`) that should be stripped or replaced with citation references before final render. They look like internal QA debug output bleeding through. On a related note, Test 4 also shows a markdown rendering bug: `**4**5%**` (broken bold) suggests a sanitizer is mid-process or string concat is double-wrapping.

---

## Tier 3 Feature Usage Rate

Out of 18 responses:

- **83%** include a `Sources:` / footer citations block (15/18)
- **56%** carry the `<!-- t3-numver -->` HTML comment (10/18) — number verifier is firing
- **56%** carry the `<!-- t3-citations -->` HTML comment (10/18) — citations rail is firing
- **22%** leak literal `[unverified]` tags into prose (4/18) — see Issue #3
- **0%** leak API keys / "API key not configured" strings (0/18) ✅

Tier 3 feature **coverage looks correct**: complex, multi-tool answers (Tests 2, 4, 5, 8, 10, 13, 15, 16, 17, 18) get full Tier 3 treatment; fast simple lookups (Tests 1, 3, 6, 7, 9) only get the basic `Sources:` block. Test 11 (clarifier) skips Tier 3 entirely, which is correct.

Test 11 is also a strong PASS for product-quality polish: response is exactly 295 chars, asks "Cambridge, MA (US) or Cambridge, UK?" and suggests three relevant follow-ups. Clean clarifier behavior.

---

## What's Working Well (Worth Preserving)

1. **UK country routing is fully fixed** — Tests 1 (£10–£20 / Indeed UK) and 2 (£, GBP, Reed, Indeed UK all hit) close out the original S76 regression cleanly. Test 8 (UK vs Germany multi-country comparison) correctly returns £12 vs €25 medians side by side, which means localization works when the tool selected is CPA, even for Germany.
2. **India currency localization** — Tests 3, 9, 10 all properly use ₹ / Naukri / INR. Test 10 even returns a full media plan denominated in `₹42,00,000 (~$50,000 USD)` — correct Indian numbering format.
3. **No API key leaks anywhere.** LinkUp / Revelio failures are silent as intended.
4. **Sub-second fast path is real** — Tests 3 (0.9 s), 7 (1.1 s), 6 (1.6 s), 9 (1.9 s) all under 2 seconds with proper data. The Haiku-first router is doing its job.
5. **Honesty on missing data** — Test 12 says "I cannot calculate or project a 2026 LFPR without the complete 2026 labour force and working-age population figures, which were not returned by the tools." Test 18 says "Joveo's reference portfolio doesn't have published healthcare client plans" rather than hallucinating one. Both are excellent — exactly the behavior the contradiction protocol was added for.
6. **Cambridge ambiguity handled** — Test 11 returns a clean clarifier instead of guessing.

---

## Recommendations for Next Session (S78)

### Priority 1 — Fix CPC tool country localization
- Trace the path: `cpc for tech in berlin` → which tool? Likely `cpc_trend_tracker` or `query_adzuna_cpc`. Confirm whether the country argument from the brain layer is being passed in.
- If passed, the tool's internal benchmark dict has no Germany / Canada entries — add country-specific defaults (€ for DE/EU, CAD for CA) or fall back to applying a currency conversion + label change.
- Add 4 fixed regression tests for this: Berlin, Toronto, Paris, Tokyo CPC.

### Priority 2 — Strip `[unverified]` markers from user-visible prose
- Either (a) remove the marker entirely before render, or (b) convert to a footnote `[?]` linked to the verifier diagnostic. Currently `_[unverified]_` reads as a bug to end users.
- Also fix the `**4**5%**` markdown double-bold issue visible in Test 4 — looks like a verifier is wrapping a number that was already bolded.

### Priority 3 — Adjust latency budgets / client timeouts
- OECD SDMX path is ~55 s of real work; either accept that (raise the soft budget to 60 s for that intent) or stream partial results. Currently a vanilla `urllib` 60 s timeout kills the request before the server finishes.
- KB introspection (Test 13) at 55 s is suspicious — should be cheap. Worth profiling whether it's calling external sources unnecessarily.

### Priority 4 — Tier 3 marker hygiene
- `<!-- t3-numver -->` and `<!-- t3-citations -->` are HTML comments and invisible in rendered chat. Fine to keep but document them. Currently they're leaking into the raw API response in `<!-- t3-numver -->` form at the very start of the message body, which means any non-HTML client will display them.

### Priority 5 — Test 17 (TA leaders) only cites 1 name out of typical 10+
- "Matt Charney" gets cited as `[1]` but the typical TA-influencer list (Hung Lee, Greg Savage, John Sumser, Tim Sackett, William Tincup, Bill Boorman, Glen Cathey, Stacy Zapar, Lou Adler…) is missing. Verify the TA-leader KB document is actually indexed and being retrieved — response says "industry research" generically rather than citing the KB.

---

## Test Artifacts

- Raw test harness: `/tmp/nova-eval/run_eval.py` (375 lines, observational only, no code paths touched)
- Raw results JSON: `/tmp/nova-eval/results.json`
- Run log: `/tmp/nova-eval/run_log.txt`

Eval methodology was pure HTTP POST against the live endpoint. No code, data, or configuration in the repo was modified.
