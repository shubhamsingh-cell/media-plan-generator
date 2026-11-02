# Nova Chatbot: Next Session Backlog

Rewritten 2026-11-03 after a local chat sweep (branch `nova-batch2-1103`).
Transcripts, verbatim, are in `evals/.cache/chat_sweep_2026-11-03/` (git-ignored):
`before/` is the first pass, `after/` the re-runs after each fix round.

## How the sweep ran, and what it could not reach

- Local server `python3 app.py <port>` from the worktree; requests sent the way
  `static/nova-chat.js` does (client-minted `nova-<uuid>` conversation id,
  session token adopted from the done event, page Origin). The re-runs also
  ran with `NOVA_CHAT_GATE=signed`: mint, `X-Nova-Widget-Token`, token adopted
  from each reply; a request without a token got 401 `WIDGET_TOKEN_REQUIRED`.
- No LLM provider keys exist locally (no Anthropic, OpenRouter, Groq, ...), so
  every turn ran in Nova's **rule-based mode** and the **direct tool dispatch**
  path with synthesis failing. Spend: $0.
- No Supabase locally, so session tokens are empty and every turn starts a new
  conversation; multi-turn context could only come from the `history` field.
- Therefore NOT exercised: LLM-written answers, the plan-figure guard and the
  number verifier (both run on model text), saved-plan questions, the
  `_[unverified]_` / `**4**5%**` paths as produced by the verifier, real
  Tavily results. Re-run `after/` with a key-bearing environment before
  trusting those rows.

## Sweep table

| # | Question | Expected | Observed (before) | Fixed? / after |
|---|---|---|---|---|
| 1 | cpc for tech in Berlin | EUR symbol, local magnitude, never `$7.63` | Tool returns EUR-converted values (Indeed 1.36, LinkedIn 2.18) but, with no model, Nova printed the raw JSON: no euro sign, "-**5%**" bolded inside JSON, the internal "do NOT use $" instruction shown; degraded answer cached | Fixed (c5666e1, 33d4905): plain lines "Indeed €1.36, LinkedIn €2.18", says the writing model is down, not cached. Earlier cached copies still serve until TTL (see R1) |
| 2-5 | best job boards in Germany / Poland / UAE / India | supply repository only, local-relevance order | Asks for the industry first (fine) | No change needed |
| 6 | Germany, then "Tech & Engineering" | Germany tech boards | Generic "Market Demand: Software Developer", country lost | Not fixed (R4); partly local (no persistence) |
| 7 | job boards in Poland for software developers | Poland-confirmed boards, local order | Whole global Tech category: Technojobs, DEV.BG, Engineering Careers.ca, Dice ... | Fixed (c5666e1): No Fluff Jobs, Just Join IT (in network for Poland) |
| 8 | job boards in UAE for nurses | no US-only boards | 15 US healthcare boards (Jama, IntelyCare, AMN ...) | Fixed: "No Health boards are confirmed ... for United Arab Emirates" plus the market's top funded boards from `channel_funding.funding_order` |
| 9 | job boards in India for warehouse workers | local-relevance order | File order (Sercanto, eFinancialCareers, ... College Recruiter) | Fixed: `channel_funding.rank_names` order (Shine, JobRapido, ...) |
| 10 | healthcare apply rate + which report | benchmark with citation | Routed to LinkedIn SlotOps predictor: all zeros, raw JSON | Partly: no raw JSON when uncached; routing not fixed (R3) |
| 11 | $500 for one warehouse associate in Dallas | small-budget honesty | "**$50**0"; "Recommended Channels for CDL Driver"; "all channels low ROI" | Bolding fixed; role mismatch and blanket low-ROI line not fixed (R5) |
| 12 | weather in Berlin tomorrow | polite out-of-scope | Correct deflection | OK |
| 13 | Germany vs France unemployment (OECD) | OECD figures, bounded latency | Rule-based mode answered with ad-spend comparison; KB context leaked "[international_sources.json] sources.eurostat..." | KB leak fixed (c5666e1). Tool checked directly: 2.9 s, but rows came oldest first (France 2003-01 first, Germany 2026-09 mid-list) and results are cut before the model reads them. Fixed (f848a8e): newest first + `latest` per country |
| 14 | latest news this week on Indeed pricing (no Tavily key) | say live news is unavailable | Generic capabilities blurb as if answered; tool path labelled Jina/DDG hits "tavily" | Fixed (c5666e1, 33d4905): rule-based answer leads with "Live news search isn't available right now"; `web_search` labels fallback hits `web_fallback` with a provider note and tells the model to say so on no results |
| 15 | CPC for nursing jobs in London | GBP | Raw JSON (same as 1) | Fixed (same path; uncached run shows £) |
| 16 | cost per application, engineers in Bangalore | India figures or an honest "US proxy" note | US report figures, "-**23.09%**", "**$209**K", "$15- $35" | Bolding fixed; no India-specific CPA in KB (R6) |
| 17 | salary for an RN in Houston | Houston range | US range 45-120K; follow-up "compare to {nearby_city}?" | Placeholder fixed (33d4905); city precision not fixed |
| 18 | media plan, 10 engineers, Berlin, "50000 budget" | plan | Asked "How much is the total budget?" | Fixed (33d4905): bare budgets read |
| 19 | Indeed vs LinkedIn for engineering | comparison | "Apply Rate: 3-**6%**" | Fixed (33d4905): "**3-6%**" |
| 20 | plan for 20 nurses in Dallas, then "how many hires / CPH?" | figures traced to the plan | Turn 1 fine (4-8 hires, ranges); turn 2 generic SHRM CPH | Not exercisable without a model (guard runs on model text) |
| 21 | hi | proof-claim stats | Computed stats, now "23 AI models" | OK; test denylist updated (a09a9a1) |
| 22 | POST /api/chat (non-stream), German nurses | same as stream | Works; widget token returned | OK |
| 23-24 | uncached CPC Munich / retail Warsaw | € / clarifier | €1.36 plain lines; Warsaw clarifier | OK |
| 25-28 | re-runs of 14, 17, 18, 19 with new phrasing | as above | as in "after" | Confirmed fixed |
| - | every tool answer | one Sources footer | Two: dated citations block plus "**Sources:** ..." | Fixed (c5666e1) |
| - | first message right after page load (handshake) | no lost message | Not reproducible headless; under `signed` the widget now waits for its token before sending | Covered by `tests/test_nova_widget_chat_gate_runtime.py` (all transports) |
| - | zero-hire channel note | plan currency | `lambda v: f"${v:,.0f}"` hardcoded | Fixed (c5666e1): `_plan_money_formatter(result)`; the chat tool's plans are USD today, so latent |

## Fixed this session (evidence = commit + regression test, each failing first)

- Metric bolding never splits a number or doubles a bold (`$500` -> `**$50**0`,
  `**45%**` -> `**4**5%**`, `-5%`, `$209K`, ranges, inside code): c5666e1,
  33d4905; `tests/test_nova_markdown_number_bold.py`.
- Degraded direct dispatch renders plain local-currency lines and is not
  cached: c5666e1; `tests/test_nova_direct_dispatch_degraded.py`.
- Publisher listing follows the supply repository for the market and the
  plan's local-relevance score (imported from `channel_funding`, not
  duplicated): c5666e1; `tests/test_nova_publisher_listing_local.py`.
- Honest web search provenance / no-result instruction: c5666e1;
  `tests/test_nova_web_search_honest.py`. Rule-based news asks: 33d4905;
  `tests/test_nova_rule_based_freshness.py`.
- KB "Additional Context" without file/key paths: c5666e1;
  `tests/test_nova_kb_context_clean.py`.
- Single Sources footer: c5666e1; `tests/test_nova_single_sources_footer.py`.
- Zero-hire note currency: c5666e1; `tests/test_nova_zero_hire_currency.py`.
- Follow-up placeholders, percent ranges, bare budgets, forecast money:
  33d4905; `tests/test_nova_markdown_number_bold.py`,
  `tests/test_nova_extract_budget_bare.py`.
- OECD newest first: f848a8e; `tests/test_nova_oecd_newest_first.py`.
- Reference-plans tool description no longer names customers (public repo).
- Chat gate (`NOVA_CHAT_GATE`, ships off): f9299c2; see `docs/NOVA_CHAT_GATE.md`.

Commit ids are pre-rebase; after the rebase onto `nova-ai-stack-1103` match by
subject line.

Status of the previous backlog's P1 items: (1) Berlin USD: the tool now returns
EUR values and symbol (verified in transcripts); LLM rendering unverified
here. (2) `**4**5%**`: the formatter half is fixed and tested; the verifier
half needs a model run. (3) `_[unverified]_` leak: current code converts the
tag to a dagger plus one footer (`nova.py` T3-3); no leak seen in this sweep,
but no model text was produced. (4) handshake race: see table. (5) OECD
latency: 2.9 s today for DEU+FRA; fetch timeout is 30 s.

## Remaining (not fixed)

- **R1 Cached degraded answers.** Answers cached before the fix during an LLM
  outage (raw JSON) keep serving until their TTL. Flush the response cache
  once after deploy (`data/nova_response_cache.json` locally; Upstash in prod).
- **R2 Rule-based "Monthly Spend: $200,000†"** for Poland/India: a KB figure in
  USD carries the "estimate not traced to Nova's planning engine" dagger,
  which reads as invented. Label KB market-spend figures as KB, in USD.
- **R3 Apply-rate questions route to `slotops_predict`** (LinkedIn timing) and
  return zeros; route to benchmarks with the report name.
- **R4 Clarifier answers lose the country** ("Tech & Engineering" after
  "best job boards in Germany"); the rule-based path ignores `history`.
- **R5 Small budgets**: role mismatch ("CDL Driver" for a warehouse associate)
  and the blanket "channels with low ROI scores" line naming every channel;
  "Optimised allocation could improve projected hires by 646%" also reaches
  chat from the budget engine (plan-engine owners).
- **R6 Non-US CPA** questions answer with US report figures without saying
  they are a US proxy.
- **R7 Thin salary answers** (pharmacist in Phoenix: "Range: N/A") in
  rule-based mode.
- **R8 Third-party blog URLs** surface in "Additional Context" (for example a
  pricing-comparison blog); KB source quality, not formatting.
- **Pre-existing test failures not caused by this branch** (fail on the base
  commit too, both in plan-engine code): `test_projection_ranges_everywhere.py::
  test_nova_formatter_prints_ranges_and_the_basis` (engine CPA now $33.37 vs
  pinned $34.97) and `test_cph_benchmark.py::test_a_budget_too_small_for_one_hire_states_no_cost_per_hire[$9,000]`.

## Carried over, not re-verified this session

RAG phase 2 backfill and A/B eval; Tier 3 production monitoring (planner,
citation rate, verifier mark rate); enrichment cron failure rate; curated
TA-leader URL drift; low-confidence intl role benchmark cells; planner
latency. See git history of this file for the original detail.
