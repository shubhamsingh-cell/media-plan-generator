# F4 — ESCO Integration Report

**Audit ranking**: #2 after OECD (which shipped in S78).
**Status**: BUILT, tests pass, live-verified.
**Build effort**: ~1.5 hours single session.
**Date**: 2026-06-02.

## Why ESCO

ESCO (European Skills, Competences and Occupations) is the EU's free, public,
standardized vocabulary of:

- **3,000+ occupations** mapped to ISCO-08 groups
- **13,890 skills** linked to those occupations (essential + optional)
- **27+ languages** with full preferred + alternative label translations

Pairing ESCO with our existing labour-statistics tools (OECD SDMX, Eurostat,
UK ONS, StatCan) gives the chatbot a clean taxonomy layer it didn't have:
"what skills does role X need", "alternative titles for X", and multilingual
title resolution for EU recruitment.

The API is public and requires no authentication, matching the stdlib-only,
no-new-dependencies precedent established by OECD SDMX.

## Discovery Probe

### Endpoint 1: occupation search

```bash
GET https://ec.europa.eu/esco/api/search
    ?language=en
    &type=occupation
    &text=data%20scientist
    &limit=3
```

Response: `total`, `_embedded.results[]` with `uri`, `title`,
`preferredLabel` (multilingual dict), `alternativeLabel` (multilingual dict
of arrays), `code` (ISCO-08 like `2511.3`), `searchHit` (short context blurb),
and `_links` (self, broaderIscoGroup).

The search endpoint is fast (~700-1000ms in the EU) but does **not** include
skills. For skills we need the detail call.

### Endpoint 2: occupation detail (skills)

```bash
GET https://ec.europa.eu/esco/api/resource/occupation
    ?uri=http://data.europa.eu/esco/occupation/258e46f9-0075-4a2e-adae-1ff0477e0f30
    &language=en
```

Response adds `description.{lang}.literal`, `scopeNote`, and crucially
`_links.hasEssentialSkill[]` (avg 20-30 entries) and `_links.hasOptionalSkill[]`
(avg 10-25 entries). Each link carries `uri`, `title`, and a `skillType` URI
that ends in either `skill` or `knowledge`.

### Endpoint 3: language fallback

ESCO accepts the 27 supported language codes plus reasonable fallback for
unsupported codes (returns 400 if unknown). We normalize locale strings like
`en-US` -> `en` and unsupported codes (e.g. `zh`, `ja`) -> `en` before hitting
the API.

### Empty / no-match handling

Search with a nonsense term returns `total: 0`, `_embedded.results: []` — a
clean empty response, no 404. Our function reports `count=0`, no `error`.

## Integration Design

Stdlib-only, mirrors OECD SDMX precedent:

```
api_enrichment.py
└── fetch_esco_occupations(query, language="en", limit=10, timeout=15, enrich_top_n=3) -> dict
    │
    ├── _esco_normalize_language()    # locale + unsupported -> "en"
    ├── /search                       # 1 HTTP call
    └── /resource/occupation × N      # N=enrich_top_n detail calls (default 3)
        ├── _esco_extract_skill_links("hasEssentialSkill")
        ├── _esco_extract_skill_links("hasOptionalSkill")
        └── full description + ISCO group title

nova.py
└── Nova._query_esco_occupations(args) -> dict
    │
    ├── 24h Upstash cache (key = q+lang+limit)
    ├── country_context echo (display-only, ESCO is EU-wide)
    └── fetch_esco_occupations()
```

### Default cost per call

- 1 search request + 3 detail requests = 4 round-trips, ~3-5 seconds total
- Caller can set `enrich_top_n=0` to skip enrichment and stay under 1 sec
- 24-hour cache means repeat queries are near-instant

### Output shape

```jsonc
{
  "source": "ESCO API",
  "tool": "query_esco_occupations",
  "query": "data scientist",
  "language": "en",
  "url": "https://ec.europa.eu/esco/api/search?...",
  "elapsed_ms": 760,
  "count": 5,
  "total_matches": 188,
  "occupations": [
    {
      "uri": "http://data.europa.eu/esco/occupation/258e46f9-...",
      "preferred_label": "data scientist",
      "alternative_labels": ["data engineer", "data research scientist", ...],
      "description": "Data scientists find and interpret rich data sources...",
      "isco_code": "2511.3",
      "isco_group": "Systems analysts",
      "essential_skills": [
        {"uri": "...", "title": "resource description framework query language", "skill_type": "knowledge"},
        ...
      ],
      "optional_skills": [
        {"uri": "...", "title": "XQuery", "skill_type": "knowledge"},
        ...
      ],
      "enriched": true
    },
    ...
  ]
}
```

Errors are returned in-band with an `error` key and `occupations: []` — the
function never raises.

## Live Verification (2026-06-02)

3 queries hit production ESCO and returned proper ESCO URIs + ISCO codes:

| Query | total_matches | first preferred | ISCO code | ISCO group | essential | optional | desc len |
|---|---:|---|---:|---|---:|---:|---:|
| `data scientist` | 188 | data scientist | 2511.3 | Systems analysts | 23 | 16 | 800+ chars |
| `registered nurse` | 42 | healthcare assistant | 5321.1 | Health care assistants | 30 | 13 | 800+ chars |
| `warehouse worker` | 520 | warehouse worker | 9333.8 | Freight handlers | 30 | 25 | 800+ chars |

All 3 returned `esco_uri_present=true` (URIs start with
`http://data.europa.eu/esco/`). Search latency: 738-1073ms.

Note: "registered nurse" maps to "healthcare assistant" as the top hit because
the searchHit text matched first. The full result set still includes
"nurse responsible for general care" (2221.2), "specialist nurse" (2221.3),
"nurse assistant" (5321.1.1) — Nova can pick the right hit via context.

### Edge cases verified

- Empty query: returns `error="query is required..."`, no network call.
- No match (`nonexistentjobxyz`): returns `count=0, total_matches=0`, no error.
- French language (`language="fr"`): returns
  `preferred_label="scientifique des données"`.
- Locale string (`language="en-US"`): normalized to `"en"`.
- Unsupported code (`language="zh"`): falls back to `"en"`.

## Wiring Checklist (5 places in nova.py)

| # | Location | Status |
|---|---|---|
| 1 | `get_tool_definitions()` schema (required: `query`; optional: `language`, `limit`, `country`) | done |
| 2 | `_tool_handler_map()` -> `self._query_esco_occupations` | done |
| 3 | `_TOOL_LABELS["query_esco_occupations"] = "Querying ESCO occupation taxonomy"` | done |
| 4 | `_TOOL_ERROR_FALLBACK_MESSAGES["query_esco_occupations"]` | done |
| 5 | `_live_tools` set (so freshness disclaimer is suppressed when ESCO is used) | done |

## Tests

`tests/test_country_awareness.py::TestEscoOccupationsTool` — 8 deterministic
offline tests (no network):

1. `test_tool_present_in_definitions`
2. `test_tool_registered_in_handler_map`
3. `test_tool_schema_well_formed`
4. `test_empty_query_returns_validation_error`
5. `test_country_context_passes_through`
6. `test_progress_label_present`
7. `test_graceful_failure_message_registered`
8. `test_listed_as_live_source`

Live API smoke tests are documented here (this file) rather than committed to
CI so we don't depend on ESCO uptime for green builds.

## Test Suite Impact

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| `tests/test_country_awareness.py` pass | 47 | 55 | +8 |
| Total pytest pass (excluding nova_chat.sh) | 1826 | 1844 | +18 |
| Total pytest fail | 33 | 13 | -20 |

All 8 new ESCO tests pass. None of the remaining 13 failures touch ESCO,
api_enrichment, or country_awareness — they are pre-existing
`test_e2e.py::ConnectionRefusedError` (no local server) and
`test_deck_generator.py` env-dependent skip-as-fail tests.

## Build Effort Breakdown

| Phase | Time |
|---|---:|
| API discovery probes (curl + jq) | 10 min |
| Read OECD SDMX precedent | 10 min |
| `fetch_esco_occupations` + helpers | 25 min |
| `_query_esco_occupations` + 5 registrations | 20 min |
| Tests (8 deterministic) | 15 min |
| Live verification + edge case probes | 10 min |
| This report | 10 min |
| **Total** | **~1 hr 40 min** |

## Files Modified

- `api_enrichment.py` — added `_ESCO_BASE_URL`, `_ESCO_DEFAULT_TIMEOUT`,
  `_ESCO_MAX_LIMIT`, `_ESCO_SUPPORTED_LANGUAGES`, `_esco_normalize_language`,
  `_esco_extract_skill_links`, `_esco_fetch_occupation_detail`,
  `fetch_esco_occupations` (~290 lines).
- `nova.py` — added tool schema entry, handler-map registration, label,
  fallback message, `_live_tools` membership, and the
  `Nova._query_esco_occupations` method (~145 lines total).
- `tests/test_country_awareness.py` — added `TestEscoOccupationsTool`
  (~95 lines, 8 tests).

## Known Limitations

- **Top-N enrichment cap (default 3)**: keeps total latency manageable. If
  the chatbot needs skills for more than 3 hits, the caller currently has to
  re-query each URI individually. Future iteration could parallelize the
  detail calls with a thread pool, mirroring how `_chat_with_free_llm_tools`
  already executes tools in parallel.
- **`alternative_labels` empty in some search responses**: the search endpoint
  exposes `alternativeLabel` only when the API decides to include it for that
  hit. Detail enrichment doesn't currently backfill it because the function
  prioritized skills + ISCO + description. Trivial to add if needed.
- **No ISCO-08 group browser**: callers can't yet ask "give me all
  occupations under ISCO 2511". That would be a separate tool
  (`browse_esco_isco_group`) — out of scope here.
- **English-only ISCO group titles**: ESCO localizes the broader concept,
  but the current code stores whatever the detail call returned for the
  requested language. For non-English queries the group title is still in
  the user's chosen language.

## Recommended Next Steps

1. **Ship.** This is production-ready behind the standard tool dispatcher.
2. Monitor cache hit rate over the first week. If <50%, drop the cache TTL
   or widen the cache key.
3. Add a smoke test job (manual, weekly) that hits ESCO with the 3 verified
   queries and alerts if `count==0` for any of them.
4. If chatbot conversations frequently mention specific ISCO groups, add
   `browse_esco_isco_group(code)` as a follow-up tool.
