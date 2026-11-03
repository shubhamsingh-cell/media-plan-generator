# Session Handoff

**Last updated:** 2026-11-03 (end of the overnight run). Replaces the S89 record
(June 2026); nothing from that file is carried over.
**Source of truth for what shipped:** `git log origin/main`, then
`/api/deploy/ready`. This file is a map, not a ledger. Verify before acting.

## Resume in one line

Main is live at `fb89347` with nothing pending on a branch (all four slices of
RUN 2026-11-03 shipped); next real work is the goal-metric model with a real
install model and the metro/site split, and the owner-only list below gates
the production switches.

## Live system

| Item | Value |
|---|---|
| URL | https://media-plan-generator.onrender.com |
| Served SHA | `fb89347` (`/api/deploy/ready` returned `git_commit` fb89347033f875dc8e497f7a93130e293bfe3f77, `ready: true`, read 2026-11-03 at the time of writing) |
| Deploy | Render auto-deploys from `main`. Ship only through `scripts/ship_from_worktree.sh` from a linked worktree on a feature branch (rebases, runs the full suite, waits a stability window, fast-forward pushes, never force). See `CLAUDE.md` section 11 and 12 |
| CI | GitHub Actions workflow "Nova AI Suite CI" (`.github/workflows/ci.yml`); jobs include `nova-retrieval-eval` and `nova-retrieval-eval-live` |
| Python | 3.11 in production (`render.yaml`) and CI; local interpreter is 3.9.6, so run anything version-sensitive in CI before trusting it |
| Health | `/api/deploy/ready` reports embedding provider/model/collection, `qdrant_attach`, `keyword_indexes`, `search_mode`, `embedder_configured`, `embedding.rag_v2`. Read it for embedding and retrieval state |

## RUN 2026-11-03

One bullet per shipped slice. "Done means live" was checked against
`/api/deploy/ready` for each of the first three.

- **Hygiene and clock pin `4b9729a`.** The enrichment-log read per cycle is
  bounded (14-day window, one retry, a WARNING on failure, 91-day boot window,
  boot window used when any source lacks state). KB backups are serialised
  across forked workers with a flock, and a backup removed mid-prune no longer
  breaks the prune. Dashboard widgets are pre-warmed in the wsgi master with a
  6 h stale window, so health probes stop waiting on a rebuild. The campaign
  month default lives in one helper, `budget_engine.default_campaign_month()`,
  and the test suite pins it to October for the whole run (see Decisions).
- **International honesty `37c50ae`.** A platform a market's government blocks
  never reaches that market's plan (`data/platform_market_blocks.json`). A US$
  plan whose markets are all outside the US is not graded against the US cost
  per hire. Non-US plan numbers carry their basis and agree across surfaces.
  Country medians carry a sourced currency; 16 near-parity countries come from
  national statistics offices (`data/country_median_earnings.json`) and
  unsourced ones print "No local benchmark". The Talent Heatmap shows each
  salary in its own currency, never ranks across currencies, scores only
  on-scale terms, and shows partially scored locations as a labelled tier on
  every surface. The heatmap analyze endpoint no longer fails on a method that
  never existed. The no-hires ROI methodology line keeps its suppressed
  cost-per-hire opening.
- **Nova base `5d5668b`.** `evals/nova_retrieval_eval.py` (72 queries, baseline
  bm25 recall@10 96.77 and tfidf 95.16) with a live Qdrant tier. Retrieval
  health in `/api/deploy/ready` is truthful: `search_mode` is gated on the
  embedder, `search_methods_served` shows what actually answered, and a
  vector-dimension guard refuses a mismatched collection.
  `vector_search.warm_retrieval()` runs in the wsgi master at boot. The
  OpenRouter `deepseek-r1` slug (delisted) is replaced by V4 Flash and cost is
  booked from the configured slug. A missing `TAVILY_API_KEY` is logged once at
  boot. Eval client-name checks read only git-tracked data files.
- **Nova batch 2 `1d8f39e`** (23 commits). A signed widget-token gate for the
  chat endpoint (`NOVA_CHAT_GATE=signed`) ships OFF; `docs/NOVA_CHAT_GATE.md`
  describes it as plumbing with roughly zero protection on its own until a bot
  check sits on the mint. 13 chat-sweep defects fixed with 99 failing-first
  tests: currency symbols on CPC answers; board listings faithful to the supply
  repository and honouring client exclusions; markdown number-bolding splits;
  OECD rows newest-first per series; KB context keeps the entity and drops
  other markets' facts; years are not read as budgets; no currency on
  ratio, count or factor keys; Tavily-unset honesty; a double Sources footer;
  a template placeholder leak; the zero-hire note's currency; bare cities with
  a US namesake are asked about, never guessed. Seven delisted OpenRouter free
  routes re-pointed to listed free models, the small Liquid model moved after
  Claude in every chat chain, Groq and Cerebras ids updated, plus
  `scripts/check_openrouter_free_routes.py`. The template-source proof-claim
  check stays strict. `docs/Next_Session_Chatbot_Backlog.md` rewritten from the
  sweep; `docs/RETRIEVAL_EVAL.md` carries the chunker short-string gap
  analysis (variant B is about 1,780 chunks, about $0 inside the Voyage grant;
  blue/green needs a collection-suffix hook).
- **Backlog A `fb89347`** (29 commits). Input-role integrity gate: every role
  the user typed must reach the plan, deck and workbook or the bundle fails
  with the role named; deck role rollups ("+N more") are honoured, matching is
  whole-word, and substitutions inside the same standardizer bucket are
  flagged. Wizard role categories "Warehouse & Fulfillment" and "Courier /
  Delivery Drivers", plus an "App Install" goal that carries the disclosure
  "App installs are not modeled; figures are applications" on deck slide 2,
  the workbook Campaign Overview and the wizard preview. Joblift is recorded
  as Adzuna-owned since Jul 2026 (insolvency and asset-deal dates with source
  in the workbook supply-status column) and is never funded beside Adzuna.
  Explicit country suffixes win over US city-name matches (Birmingham,
  Manchester, Cambridge, Richmond, Portland, Paris, London, Berlin;
  "Belfast, Northern Ireland" prices in pounds). OLX.pl takes the dataset's
  "OLX Praca" signal; Yolo and Talent Portugal match by exact name, and brand
  exclusions no longer bypass exact-name boards. The CPQA timeline bullet is
  kept without displacing plan-specific bullets. Internal-draft plans are a
  warn finding with an action, shown as an amber box under Quick answer, and
  only allowlisted findings reach users (no debug text). Industrial Painter
  and same-occupation paint titles score consistently. Slide-2 card heights
  are measured with real Poppins glyph widths so cited market-data lines
  survive. Talent Heatmap Location Rankings headers fixed, the UK row is
  labelled by city with national figures, and cost-of-living is n/a outside
  the US on every sheet.

## Decisions taken (owner may reverse)

- **No local benchmark, not a guess.** A country median with no source prints
  "No local benchmark" instead of a US$ estimate. Near-parity medians are
  sourced from national statistics offices in
  `data/country_median_earnings.json` (URL, quote, period, retrieval date per
  row). Reason: a US$ relabel of GBP or CHF medians would have understated UK
  pay by about 25%.
- **Heatmap never ranks across currencies.** Each currency is its own group;
  partially scored rows are a labelled tier, not silently dropped or blended.
- **Tests pin the campaign month to October** for the whole run via
  `MPG_DEFAULT_CAMPAIGN_MONTH` (set in `tests/conftest.py`;
  `@pytest.mark.real_clock` opts out). Reason: month-dependent expectations
  went red on 1 Nov in three unrelated places.
- **`search_mode` is gated on the embedder.** It reports `hybrid_rrf` only when
  an embedder is configured; otherwise it names the keyword mode that served.
- **The retrieval eval CI job is a signal** until branch protection requires
  `nova-retrieval-eval` on `main`. Do not describe it as a gate.
- **Joblift is recorded as Adzuna-owned** and is never funded beside Adzuna in
  the same plan.
- **Bare city names with a US namesake are asked about, never guessed.**
- **`NOVA_CHAT_GATE` ships OFF.** Do not flip it to `signed` until a bot check
  sits on the token mint; `docs/NOVA_CHAT_GATE.md` describes the gate as
  plumbing with roughly zero protection on its own.
- **Backlog A shipped at a design-panel median of 7** with zero vetoes and only
  polish left, under the same precedent as 2026-10-23 (production was worse on
  trust). The bar is 8; revisit the polish items if a reviewer raises them.

## Known gaps / follow-ups

Not done tonight, roughly in priority order.

1. **Selectable goal metric with a real install model** (applies, qualified
   applies, hires, app installs, leads). The goal must be mandatory so
   "applies unset" is impossible. The allocator in `budget_engine.py` still
   takes hires, applications and clicks only.
2. **Metro / site split** outside the enterprise pack: country, then metro or
   DMA, then site or commute radius, with weights from a named driver.
3. **Zero-hire defunding.** Needs an owner decision first (channels are
   labelled today, not defunded) and a rewrite of about 13 fixture tests.
4. **Performance notes at scale** (supply team): a per-country list of boards
   that perform well, in the shape of `data/supply_performance_notes.json`, and
   vendor lists for the non-job-board channels (DSP, audio, social, staffing,
   influencer).
5. **Google Slides tier check.** `deck_generator.py` tries Google Slides before
   python-pptx. If the production Render env sets the Slides credentials, none
   of the `ppt_generator.py` changes reach the client deck. Check which tier
   production uses (never print the value).
6. **Chunker short-string gap and reindex.** Analysis is in
   `docs/RETRIEVAL_EVAL.md` (variant B is about 1,780 chunks, about $0 inside
   the Voyage grant; blue/green needs a collection-suffix hook). Build the
   hook, then reindex Qdrant (needs the owner-only secrets below).
7. **Backlog A leftovers.** The critical-findings dialog still shows raw
   finding text (prefer `user_message`). One test should force the Poppins
   fallback for the slide-2 card heights. `CANONICAL_ROLES` has no
   display-title field, so abbreviation expansion could create false
   positives later. Remaining matcher-key collisions: Remote.co/Remote,
   Engineering.com/Engineering.jobs, Alpha/alpha.ch.
8. **Remaining wall-clock month reads.** These bypass the pin and will go
   stale the same way: `excel_v2.py` (near 8012 and 14108), `ppt_generator.py`
   near 2890, `gold_standard.py` near 6006 and 6112, `nova.py` near 5614,
   11520 and 13463. Route them through `default_campaign_month()` or pass
   `campaign_start_month` explicitly. Line numbers drift; grep for
   `datetime.now().month` and `date.today().month`.
9. **Narrow except-tuples in `app.py`**: the Supabase and JobSpy calls in the
   heatmap analyze endpoint catch a short exception tuple and can leak other
   failures as a 500.
10. Older backlog still open: Poland/UAE/India board scoring evidence,
    audience segments with their own KPI, reserve and kill rules, pacing curve
    with a stated reason, measurement prerequisites as a deliverable.
    Chat-side items live in `docs/Next_Session_Chatbot_Backlog.md`.

## Owner-only

Nothing here is for an agent to do.

- **Prod Supabase index** (run by the owner, outside a transaction):
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_enrichment_log_table_created ON enrichment_log (table_name, created_at DESC)`
- **Branch protection** on `main` requiring the `nova-retrieval-eval` check.
- **Qdrant and Voyage secrets** in CI for the live eval tier, then run the
  eval with `--update-baseline`.
- **`TAVILY_API_KEY`** in the Render env (boot currently logs that it is unset).
- **`NOVA_CHAT_GATE=signed`** only after a bot check sits on the mint. Ships OFF.
- **GEMINI embed-method unblock.** The Gemini embedding path was last blocked
  on the provider key (403 in June); serving stays on Voyage `voyage-4-lite`
  until the owner clears it.

## Hazards for the next session

- **The ship script rebases.** A branch holding merge commits with conflict
  resolutions must be squashed first. Exit 2 means a rebase conflict and the
  tree is already clean.
- **Month time-bomb class.** Any test or code path that reads the wall-clock
  month goes stale when the month turns. The pin is an env var read at call
  time (`MPG_DEFAULT_CAMPAIGN_MONTH=10` from conftest), which covers
  module-scoped fixtures, import-time caches, threads and subprocesses; a
  per-test monkeypatch does not, because module fixtures are built before
  autouse fixtures run. Before adding a month-sensitive expectation, pass
  `campaign_start_month` explicitly.
- **Test state leak.** `build_index()` in three test modules leaves
  `vector_search._index_built` True. Fix at the test-file level with an autouse
  reset of every module global the code reads. Suite flakes here are state
  leaks, not timing.
- **`wsgi.py::_run_deferred_startup` conflicts.** Two branches that each add a
  warm-up call there will conflict. Keep every call, with the retrieval
  warm-up before `gc.freeze`. Keyword indexes cost about 30 MB per process;
  refcount writes defeat copy-on-write.
- **Renders need Poppins** in a private LibreOffice profile, or glyphs and
  widths drift. See `scripts/deck_qa/README.md`. Deck and UI changes are
  rendered and pixel-checked before shipping.
- **Auto-mode classifier refusals.** It refuses production reads, PR merges
  and local destruction (branch or worktree cleanup). Plan for the owner to do
  those, or to run them from a session that is allowed.
- **Runtime churn files** (`data/.embedding_cache.json`,
  `data/nova_memory_default.json`, `data/nova_response_cache.json`) are
  rewritten by tests and servers. Restore them, never commit them.
- The repo is public. No client names, contact details, private paths or
  ledger content in tracked files; private notes live under the gitignored
  `.claude/`.
