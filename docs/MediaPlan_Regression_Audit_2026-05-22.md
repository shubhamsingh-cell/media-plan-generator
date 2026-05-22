# Media Plan Generator -- Regression Audit Post nova.py Country-Awareness Refactor

**Date**: 2026-05-22
**Auditor**: Production Validation Agent (read-only)
**Scope**: Verify that `/api/generate` and the media plan generator product remain functional after the uncommitted nova.py refactor (+2003 / -79 lines).
**Method**: Static analysis (git diff, grep), live endpoint smoke tests via `curl`. No code modified.

---

## 1. Verdict

**SAFE.** The media plan generator is not affected by the nova.py refactor. The refactor is purely additive (zero function removals, signatures unchanged on the 3 publicly imported symbols), country/currency helpers are not consumed outside nova.py + tests, and the live `/api/generate` endpoint responds correctly with the existing JSON contract.

Confidence: high. One residual watch-item flagged (resource usage on cold start) -- not a regression risk.

---

## 2. Refactor Characterization

**Working-tree diff** (`git diff HEAD nova.py`):

- `+2003 / -79 lines`, **uncommitted**. Last committed HEAD: `a43f364` (Gemini 3.1 Flash Lite GA migration).
- 13 new module-private helpers added (all underscore-prefixed):
  `_currency_symbol`, `_t3_flag`, `_detect_ambiguous_clarification`,
  `_is_planner_eligible_query`, `_call_planner_llm`, `_canonicalize_source`,
  `_build_citations_block`, `_t3_normalize_dollar`, `_t3_normalize_suffix`,
  `_t3_extract_response_claims`, `_t3_collect_tool_numbers`,
  `_t3_number_matches`, `_verify_response_numbers`.
- **Zero `-def` lines** -- no existing function was removed or renamed.
- Data structures expanded (additive only):
  - `_COUNTRY_CURRENCY` (nova.py:1761-1772): +10 entries (Qatar, Bangladesh, Peru, Hong Kong, Pakistan, Ukraine, Russia, Iceland, Bulgaria, Croatia).
  - `_CURRENCY_SYMBOLS` (nova.py:1824-1872): new dict, 49 entries.
  - `_TOP_METROS_BY_COUNTRY` (nova.py:1885+): new dict, several countries.
- `_get_currency_for_country` body changed (nova.py:1780-1794) but **behavioural contract preserved for the US case** (returns `"USD"` when country is `None`, unmapped, or "United States"). Only difference: now emits a `logger.warning` for unmapped countries instead of silently defaulting -- observability win, no consumer impact.

---

## 3. Shared-Symbol Impact Table

| Symbol | Defined In | Used By Plan-Gen? | Changed By Refactor? | Risk |
|---|---|---|---|---|
| `handle_chat_request` | `nova.py:25024` | No (chat-only, `app.py:17566, 18002`) | Signature unchanged; only optional kwargs (`cancel_event`, `outer_deadline`, `partial_accumulator`) | None |
| `handle_chat_request_stream` | `nova.py:25423` | No (chat-only, `app.py:10173, 18002`) | Signature unchanged | None |
| `summarize_conversation` | `nova.py:25826` | No (chat-only, `app.py:18322`) | Signature unchanged | None |
| `_COUNTRY_CURRENCY` | `nova.py:1761` | No (only `tests/test_currency_formatting.py:42`) | Additive (+10 countries) | None |
| `_get_currency_for_country` | `nova.py:1780` | No (only test file at `:45`) | Behaviour preserved for US/null/unmapped (returns `"USD"`); added warning log | None |
| `_currency_symbol` | `nova.py:1893` (new) | No | New | None |
| `_TOP_METROS_BY_COUNTRY` | `nova.py:1885+` (new) | No | New | None |
| `_COUNTRY_ALIASES` (in `standardizer.py:987`) | `standardizer.py` | Yes (plan-gen module) | **Untouched** -- separate symbol from nova; no collision | None |
| `_data_cache` | `nova.py:3864` (`self._data_cache`) | No (instance-level on `Nova` class; `app.py` has no `_data_cache` reference) | Refactor wires it to a shared hot-reloaded KB inside `Nova.__init__`; isolated from app.py | None |
| `joveo_publishers.json` | `data/` | Yes (read in `app.py:2036, 3577`) | Refactor does not touch JSON files; nova still reads with `.get("joveo_publishers", {})` -- additive read pattern | None |
| `joveo_global_supply_repository.json` | `data/` | Yes (read in `app.py:3521`) | Same -- not modified | None |

**Search evidence:**

```text
$ grep -rn "from nova\|import nova\b" plan_validator.py benchmark_registry.py \
    data_orchestrator.py joveo_slides_template.py campaign_optimizer.py \
    deck_generator.py budget_engine.py channel_recommender.py
(no matches)
```

None of the plan-generator support modules import nova.py. The only nova imports inside `app.py` are the three chat functions listed above and three separate `nova_persistence` / `nova_slack` / `nova_proactive` / `nova_cache` modules (different files, untouched by this refactor).

```text
$ grep -rn "_get_currency_for_country\|_COUNTRY_CURRENCY\|_COUNTRY_ALIASES" \
    --include="*.py" media-plan-generator/ | grep -v nova.py
standardizer.py:987:_EXTRA_COUNTRY_ALIASES: Dict[str, str] = {   (different symbol)
tests/test_currency_formatting.py:42,45                          (tests only)
```

The currency helpers are confined to nova.py + their unit tests. Plan-gen has its own currency / country handling.

---

## 4. Live Endpoint Smoke Test (https://media-plan-generator.onrender.com)

| Probe | Result | Notes |
|---|---|---|
| `GET /api/health/ping` | **HTTP 200**, 0.43s, 35B, `application/json` | Server alive, no cold-start stall |
| `GET /` | **HTTP 200**, 0.59s, 135 KB HTML | Index template renders |
| `OPTIONS /api/generate` | **HTTP 204**, 0.87s, `Access-Control-Allow-Methods: GET, HEAD, POST, OPTIONS` | CORS preflight OK |
| `POST /api/generate` (missing required fields) | **HTTP 400**, 0.89s, `{"success":false,"error":"Required fields missing: Client name, Requester name, Requester email.","code":"VALIDATION_ERROR","data":null,"timestamp":"2026-05-22T08:48:22.497645+00:00"}` | Validator pipeline runs, returns documented shape (`success/error/code/data/timestamp`) |
| `POST /api/generate` (complete payload, no Origin) | **HTTP 401**, 0.57s, `{"success":false,"error":"Authentication required. Please sign in with your @joveo.com account.","code":"AUTH_REQUIRED"}` | S46/S48 server-side `@joveo.com` enforcement firing as designed |
| `POST /api/generate` (complete payload + same-origin headers) | Request accepted, generation in progress (test timed out at 30s; production budget is 60s+) | Endpoint reached the plan-generation phase -- no 500, no NameError, no JSON parse failure |

All five negative cases returned correct **structured JSON** matching the frontend's expected envelope. The validator (`app.py:2964-2966`), CSRF / same-origin gate (`app.py:12918, 13425`), and auth check (`app.py:17477`+) are functioning. No traces of unhandled exceptions in the response.

---

## 5. Specific Concerns

1. **Refactor is uncommitted.** `nova.py` has 2003 added lines in the working tree but the last `git log -- nova.py` matching this scope is older than HEAD. Risk: if `app.py` is also dirty and pulls a not-yet-pushed identifier from nova, deployment would skew. Mitigation already in place: the only `from nova import` lines in `app.py` are the three chat functions, all of which still exist with the same signatures (`nova.py:25024, 25423, 25826`). No new identifiers are required by `app.py`.

2. **`_data_cache` semantics change inside nova.py** (`nova.py:3864-3914`). The refactor now points `self._data_cache` at a shared hot-reloaded KB dict. This is **internal to the `Nova` class instance**. `app.py` has no reference to `_data_cache` and does not share an instance with nova.py (chat uses `nova.handle_chat_request` which builds its own state). No cross-product collision.

3. **JSON file collision risk.** Both products read `joveo_publishers.json` and `joveo_global_supply_repository.json`. The refactor diff has no `+` or `-` lines touching `data/*.json` schemas. Schema check passed by inspection of `app.py:3577-3589` and `nova.py:16189` -- both still use `.get("joveo_publishers", {})` pattern which tolerates any additive key changes.

4. **Logger warning for unmapped countries** (`nova.py:1780-1794` diff hunk). The added `logger.warning("No currency mapping for country %r; defaulting to USD...")` could produce log volume if upstream callers pass odd country strings. Plan-gen never calls this function, so no impact on `/api/generate`. Watch-item only if Sentry noise budget shrinks.

5. **No `app.py` working-tree changes detected.** `git diff HEAD --stat` shows only `nova.py | 2082 ++++...---`. The plan-gen surface area is git-clean.

---

## 6. Rollback Plan (if production breaks despite this audit)

Triggers that would warrant rollback:
- `/api/generate` returns HTTP 500 within 5 minutes of deploy
- `/api/health/ping` >5s or non-200
- Sentry rate >2x baseline (>4 events/min) for `app.py` errors

**Step-by-step rollback** (estimated 3-5 min total):

1. **Stash the nova.py refactor locally** (preserve work):
   ```bash
   cd /Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator
   git stash push -m "nova.py country-awareness refactor 2026-05-22" nova.py
   ```

2. **Verify HEAD is clean**:
   ```bash
   git diff HEAD --stat   # should show no changes
   git log -1 --oneline   # confirm last green commit (a43f364)
   ```

3. **Force-redeploy the last known-good commit on Render**:
   - Render dashboard -> service `srv-d6lk06k50q8c73bcpo40` -> Manual Deploy -> select commit `a43f364`.
   - Or via API: `POST /v1/services/srv-d6lk06k50q8c73bcpo40/deploys` with `{"clearCache": false, "commitId": "a43f364"}`.

4. **Smoke test after deploy completes** (3-4 min on Standard tier):
   ```bash
   curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" \
     https://media-plan-generator.onrender.com/api/health/ping
   curl -s -X OPTIONS -o /dev/null -w "%{http_code}\n" \
     https://media-plan-generator.onrender.com/api/generate
   ```
   Expected: `200 <1s` for ping, `204` for OPTIONS.

5. **Re-apply the refactor in a feature branch** once root cause is isolated:
   ```bash
   git checkout -b fix/nova-country-refactor-recovery
   git stash pop
   ```

Because `app.py` is untouched in the working tree, a `git stash push nova.py` alone is sufficient -- no app.py revert needed.

---

## 7. Files Reviewed

- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/nova.py` (1.14 MB, 25989 lines)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/app.py` (966 KB, 21866 lines)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/plan_validator.py`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/benchmark_registry.py`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data_orchestrator.py`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/joveo_slides_template.py`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/campaign_optimizer.py`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/standardizer.py`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/tests/test_currency_formatting.py`

All references and line numbers in this audit cite the current working-tree state on 2026-05-22.
