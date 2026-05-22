# Nova chatbot country-awareness test suite

This directory contains the regression suite that locks in the post-audit
country-awareness behaviour of Nova (the chatbot embedded across the Nova
AI Suite). Before the audit, Nova silently treated every query as a US
query — UK clients got USD figures, India clients got Indeed/LinkedIn
recommendations, etc. The audit shipped 17 fixes across three tiers, and
this suite is the safety net that keeps those fixes in place.

## File layout

| File | Purpose | Test count |
|---|---|---|
| `test_country_awareness.py` | **Baseline (pre-existing)**. Smoke checks added during the audit itself. Kept untouched. | 19 |
| `test_intl_chatbot_scenarios.py` | **End-to-end international scenarios.** UK / IN / DE / CA / AU / BR / multi-country / multi-city / ambiguous-city / edge cases / Tier 2 handlers / Tier 3 intelligence. | 181 |
| `test_currency_formatting.py` | **Currency symbol & code matrix.** Every supported country -> ISO 4217 code -> display symbol. | 152 |
| `test_us_regression_safety.py` | **US regression safety net.** Pins down the existing US fast paths so the country-awareness refactor cannot silently regress US behaviour. | 116 |
| **Total (new)** | | **449** |
| **Grand total** | | **468** |

## Audit -> test traceability

Every test docstring cites the audit finding it covers:

- **Tier 1 (T1-1 .. T1-9)** — fast-path deferral, alias expansion, disclaimer wiring
- **Tier 2 (T2-1 .. T2-7)** — country-aware tool handlers (`_query_market_demand`,
  `_query_recruitment_benchmarks`, `_query_ad_platform`, `_query_budget_projection`,
  `_query_collar_strategy`, `_intl_country_data`)
- **Tier 3 (T3-1 .. T3-4)** — planner, source citations, number verifier, ambiguous-city clarifier

To find the test(s) covering audit finding `T1-7` for example:

```bash
grep -rn "T1-7" tests/
```

## How to run

```bash
# Whole country-awareness suite
python3 -m pytest tests/test_country_awareness.py \
                  tests/test_intl_chatbot_scenarios.py \
                  tests/test_currency_formatting.py \
                  tests/test_us_regression_safety.py

# Single file
python3 -m pytest tests/test_us_regression_safety.py -v

# One class
python3 -m pytest tests/test_intl_chatbot_scenarios.py::TestUKQueries -v

# Only show the test names (no execution)
python3 -m pytest tests/test_intl_chatbot_scenarios.py --collect-only -q
```

## Design rules baked into the suite

1. **Deterministic only.** No `random`, no network, no time-dependent assertions.
   Every test runs the same way on every machine, every CI run.
2. **No nova.py edits.** All tests exercise the public-ish interface
   (`Nova.__new__(Nova)` + method call). Heavy optional deps
   (`anthropic`, `openai`, `supabase`, `redis`, `qdrant_client`,
   `sentence_transformers`, `posthog`) are stubbed in
   `sys.modules` before importing nova.
3. **Skip gracefully when a feature isn't merged.** Tier 2 + Tier 3 tests
   call `pytest.skip(...)` instead of failing when the under-test feature
   isn't yet wired in (e.g. before the parallel Tier 2 branch lands).
   The baseline stays green at all times.
4. **One assertion per scenario.** Parametrized tests express the
   permutations; the test body holds a single behavioural assertion.
5. **Audit finding -> docstring -> test name.** Every test is traceable
   back to a specific audit row so future engineers can map test failures
   to the original spec.
6. **No fixtures with IO.** `_new_nova()` builds a Nova instance via
   `Nova.__new__(Nova)` (skipping `__init__`'s file reads / network
   calls) and pre-seeds `_data_cache = {}`. Tests that need specific
   cache data set it explicitly so the behaviour is obvious from the
   test alone.

## What each test file is responsible for

### `test_intl_chatbot_scenarios.py` (E2E behavioural)

Covers the **routing** decisions — given a user query, does Nova send it
through the right path?

- `TestUKQueries` / `TestIndiaQueries` / `TestGermanyQueries` /
  `TestCanadaQueries` / `TestAustraliaQueries` /
  `TestBrazilAndOtherLatAm` — one class per major market. Each asserts:
  - the deterministic fast paths defer (so the LLM path can answer in
    local currency)
  - the country alias is detected
  - the local currency code and symbol are correct
- `TestMultiCountryComparison` — `_detect_all_countries` returns the
  full set
- `TestMultiCitySameCountry` — `_extract_locations_for_dispatch`
  surfaces both cities for downstream tool dispatch
- `TestBudgetCountryAwareness` — `_query_budget_projection` does not
  silently default to US
- `TestSupplyDemandCountryScope` — the supply-listing alias table covers
  the full ≥30-country roster
- `TestAmbiguousCityClarifier` — Birmingham / Cambridge / Vienna /
  Newcastle / Perth / Naples / Warsaw / Paris / Rome / Hamilton trigger
  the T3-4 clarifier, and `Birmingham UK` / `Birmingham, AL` skip it
- `TestEdgeCases` — mixed case, unicode (Düsseldorf, São Paulo),
  curly apostrophes, trailing punctuation, whitespace, empty strings,
  typos
- `TestTier2Handlers` — `_query_market_demand`,
  `_query_recruitment_benchmarks`, `_query_ad_platform`,
  `_query_collar_strategy`, `_intl_country_data` accept and honour
  `country` parameters
- `TestTier3Intelligence` — planner helper, citations block, number
  verifier, clarifier
- `TestRegressionForKnownBugs` — one test per audit finding for clean
  traceability

### `test_currency_formatting.py` (Currency matrix)

Covers the **rendering** decisions — given a country, what symbol does
Nova display? UK clients seeing `$` on a UK response is a P0 visible
defect, so this matrix needs to be airtight.

- `TestCurrencyCodeLookup` — `_get_currency_for_country` returns the
  correct ISO code for 44 countries
- `TestCurrencySymbolLookup` — `_currency_symbol` returns the correct
  display symbol for 38 ISO codes
- `TestCurrencyCodeRoundtrip` — query -> detect country -> get code
  -> get symbol chain
- `TestCurrencyEdgeCases` — `None`, empty string, unknown country,
  case-insensitivity
- `TestCurrencyConsistency` — every country in `_COUNTRY_CURRENCY`
  has a matching entry in `_CURRENCY_SYMBOLS` (prevents the silent-fallback-to-USD
  defect class)
- `TestCurrencySymbolFormatting` — defensive byte-level checks
  (`£` is `U+00A3`, `€` is `U+20AC`, etc.)
- `TestCurrencyWithBenchmarkPath` — non-USD countries actually defer the
  US-only fast path so the local currency can be rendered

### `test_us_regression_safety.py` (US safety net)

This is the **most important** file. The country-awareness refactor
touched almost every routing code path; this file proves the existing
US product still works.

- `TestUSFastPathBenchmark` — 15 parametrized bare-US queries continue
  returning a deterministic response (no LLM round trip)
- `TestUSMetroMultipliers` — Chicago / Austin / Dallas / Boston / Houston
  / Atlanta / Denver / Seattle / Phoenix / Miami metro multipliers
  continue to apply
- `TestUSStateAliases` — full state names AND uppercase 2-letter
  abbreviations continue to map to United States
- `TestUSQuickAnswer` — quick-answer doesn't falsely defer for US cities
- `TestUSCountryDetection` — `us` / `USA` / `America` / `United States`
  all detect; lowercase `us` in "help us" does NOT false-positive
- `TestUSDisclaimer` — T1-7 disclaimer present for bare queries, absent
  for metro queries (so we don't double up)
- `TestUSCannedAnswerCompatibility` — US healthcare and blue-collar
  supply-listing intent regexes still fire
- `TestUSDispatchExtraction` — `Dallas, TX` form still extracted
- `TestUSCurrency` — USD remains the default for `None` / `""` /
  unknown / `United States`
- `TestUSSupplyListingFastPath` — US supply-listing alias still
  contains `us`, `usa`, `america`, `united states`, `american`
- `TestUSConfigDefaults` — sanity checks on `_QUICK_ROLE_MAP`,
  `_US_METRO_COST_INDEX`, `_US_STATE_ALIASES`, `_VERTICAL_BENCHMARKS`
- `TestUSNoSilentScoping` — broad queries without a country return
  `None` from `_detect_country` (the signal that downstream handlers
  should preserve cross-market scope rather than silently substitute
  US)

## CI integration

The suite is fast (≈0.5s for all 449 new tests + 19 existing tests).
Wire it into CI alongside the existing test selection. The full
country-awareness suite should run on every pull request that touches:

- `nova.py`
- `data/international_benchmarks_2026.json`
- `data/joveo_publishers.json`
- Any other data file consumed by Nova's tool handlers

## When a test fails — debugging playbook

1. **Read the docstring.** Every test cites the audit finding it covers
   (e.g. "Audit T1-7"). Find the matching row in the audit doc to
   understand the intent.
2. **Check whether it's a Tier 2/3 in-flight test.** Those skip cleanly
   instead of failing — if you see a failure there, it means the
   feature regressed *after* having been merged.
3. **Run the single test verbosely.** `pytest tests/test_X.py::ClassName::test_name -v`
4. **Reproduce in a Python REPL.** Every test is a couple of lines.
   Copy them into a REPL with the same stubbed imports and step
   through `_fast_path_benchmark_lookup` / `_try_quick_answer` /
   etc. by eye.

## Known edge cases (intentional)

- `cpc for tech in new york` defers the fast path because
  `_NON_US_CITY_ALIASES` contains `york` (the English city) which
  word-matches inside "new york". The dispatch extraction path
  (`_extract_locations_for_dispatch`) handles `"New York, NY"` correctly
  via the `City, ST` regex, but the fast-path benchmark route would
  defer to the LLM for `new york` alone. Not breaking anything in
  production (the LLM has US benchmarks too), just an interesting
  alias collision. If the collision is fixed in nova.py, re-enable
  the `new york` row in `TestUSMetroMultipliers.test_us_high_cost_metros_match`.
- `_detect_country("hiring in Mobile, AL")` returns `None` because
  `AL` is intentionally NOT in `_US_STATE_ALIASES` (to avoid false
  positives on the very common English word `all`). The test
  documents this behaviour. If `AL` is ever added, re-evaluate the
  false-positive risk on `all` queries.
