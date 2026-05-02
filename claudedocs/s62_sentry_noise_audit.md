# S62 Sentry Noise Audit — Nova AI Suite

**Context**: Sentry dashboard at 4,179 / 5,000 (84%). Over the cap, all new errors are dropped. Need to cut volume aggressively while keeping real bugs visible.

**Scope**: Read-only audit of `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/`. No source changes.

---

## 1. Root Cause — One Line

**Sentry captures every `logger.error(...)` call as an event** because `sentry_sdk.init(...)` in `app.py:1579` does not pass a `LoggingIntegration` with `event_level` set. Default SDK behavior: `ERROR` log records auto-create events. The project has **433 `logger.error(...)` call-sites across 50 files**, many inside per-request loops.

The `before_send` filter in `app.py:1480-1577` does catch some patterns but operates on message substrings only — it cannot see the logger name or module, and many noisy messages (e.g., `"Tool %s failed"`, `"market_pulse: cpc_trends collection failed"`) slip past every one of its 7 rules.

---

## 2. Baseline Counts

| Metric | Count |
|---|---|
| `logger.error(...)` call-sites | **433** across 50 files |
| `logger.exception(...)` call-sites | 12 across 6 files |
| `sentry_sdk.capture_exception` (manual) | 1 (`resilience_router.py:1614`) |
| Files with >15 error call-sites | nova.py (108), nova_persistence.py (29), data_enrichment.py (29), api_integrations.py (19), nova_slack.py (18), firecrawl_enrichment.py (16) |

**5,000 events / 30 days = ~167 events/day.** At that budget, even 10 noisy error sites each firing 20 times/day will eat the entire quota.

---

## 3. Top 10 Noisiest Error Call-Sites (ranked by fires-per-typical-request)

| Rank | File:Line | Pattern | Fires/request | Why noisy | Correct level |
|---|---|---|---|---|---|
| **1** | `nova.py:6476` | `logger.error("Tool %s failed: %s", tool_name, e, exc_info=True)` inside per-tool exception handler in chat loop | **0.2-2 per chat turn** (fires on any of 83 chatbot tools timing out or hitting a 429) | Every chat hits ~5-8 tools; each tool can fail independently — timeouts, rate limits, optional modules not installed. The error is already gracefully handled with a fallback message. | **WARNING** (expected partial failure, has fallback) |
| **2** | `nova.py:18456` / `18478` | `logger.error("Claude parallel tool %s failed: %s", ...)` in Claude tool-use loop | **0.1-0.5 per chat turn** with tool use | Runs in a `ThreadPoolExecutor` with 8s timeout — any slow/failed tool becomes an error event. Fallback already swallows the result. | **WARNING** |
| **3** | `nova.py:18352` / `18357` | `logger.error("Claude API HTTP error (iter %d): %s", ...)` | **0.05-0.3 per chat turn** (every Anthropic 429/503 or timeout) | Inside a retry/iteration loop. Anthropic 429s during traffic spikes fire this repeatedly across iterations. Retry logic handles it. | **WARNING** (transient upstream) |
| **4** | `market_pulse.py:987-1030` (7 sites) | `logger.error("market_pulse: <section> collection failed: %s", exc)` | **0-7 per pulse generation** (scheduled daily + on-demand) | Each of 7 sections has its own try/except. A single network blip during the scheduled tick logs 7 errors. No `before_send` pattern matches. | **WARNING** (each section is independent; aggregate counts enough) |
| **5** | `nova.py:10138-11633` (~40 sites, pattern: `logger.error("<tool_name> failed: %s", e, exc_info=True)`) | Chatbot-tool-wrapper exceptions | **0.1-0.5 per chat turn** (cumulative across all tools) | The per-tool wrappers already return `{"error": ...}` JSON to Claude. Double-logging: each tool's internal error + the wrapper's error + line 6476 = 3 events per failure. | **WARNING** for wrappers (keep only bottom frame as ERROR) |
| **6** | `firecrawl_enrichment.py:224-241` (4 sites in same handler) | `logger.error("Firecrawl API <error_type> for ...")` | **High** during 402 credit exhaustion and 429 rate limit | Credits are exhausted (per MEMORY: "Firecrawl #3, no credits"). Every scrape attempt logs a URL error. 402 path correctly uses WARNING already; 429 and URLError paths still log ERROR. | **WARNING** (Firecrawl is #3 fallback, expected to fail often) |
| **7** | `tavily_search.py:187, 196, 204, 267, 272, 277, 363, 368, 373` (9 sites) | HTTPError / URLError / JSONDecodeError per tier (Tavily, Jina, DDG) | **Medium** during rate limits | Multi-tier search — tier 1 failing just means tier 2 is tried. Only failure of ALL tiers is actionable. `before_send` catches `"http 429"` but not all tier-failure messages. | **WARNING** for per-tier; keep ERROR only on all-tiers-failed |
| **8** | `nova_persistence.py:201, 247, 284, 288, 390, 441, 471, 691, 1033, 1130, 1273, 1332, 1359, 1386, 1408, 1465, 1518, 1551, 1575, 1619` (20+ sites) | Supabase upsert/insert/fetch failures, each followed by `_log_persistence_error(...)` | **0-3 per chat turn** during Supabase latency spikes | Each failure logs to logger (-> Sentry) AND writes to `nova_persistence_errors` table. Transient Supabase blips flood both. `exc_info=True` attaches stacktrace, bloating event payloads. | **WARNING** with dedup by operation name |
| **9** | `data_enrichment.py:444, 547, 635, 983, 1034, 1779, 1886, 1950, 2091` (9 sites inside scheduled enrichment cycle) | `logger.error("... enrichment failed: %s", ...)` | **0-9 per enrichment cycle** (runs hourly) | `before_send` only filters these when `_DEPLOY_WARMUP_COMPLETE` is False. After warmup completes, all these fire through to Sentry. The cycle's own top-level `"Enrichment cycle crashed"` (line 2091) catches everything — the per-step errors are redundant. | **WARNING** for per-step; keep ERROR only on cycle-crashed |
| **10** | `nova.py:3072, 3110, 3129, 3162, 3189, 3224, 3263` (7 sites) | Gold Standard Gates 1-7 — `logger.error("Gold Standard chat Gate N (...) failed")` | **0-7 per chat turn that triggers Gold Standard** | Each gate is optional enrichment. Timeouts correctly use WARNING — but non-timeout exceptions all log ERROR. Gates 1-7 have no shared dedup; one bad NeonDB latency spike fires 7 events. | **WARNING** (gates are redundant enrichment, each has fallback) |

Also worth calling out (Tier 2 noise, ~5-15 events each per day):

- `nova_slack.py` — 18 logger.error sites in token-refresh/Slack-post loops. Slack rate limits (429s) and token expiry fire these repeatedly.
- `calendar_sync.py` — 9 sites in OAuth token exchange. Every 401 cycles through all 9.
- `google_cloud_storage.py` — 13 sites, many fire on missing-config (`"upload_file requires bucket"`) which is a validation error, not a runtime bug.
- `api_integrations.py` — 19 sites, BLS/Census/BEA/RemoteOK per-endpoint failures; already partially filtered but `"fetch failed"` substring isn't in `before_send`.
- `skill_target.py` — 13 sites, all `f"<function_name> error: {e}"` with bare exception catch. No `exc_info` means stacktraces are missing (reducing event quality).

---

## 4. Non-Actionable Errors That Should Be WARNING / INFO

### (a) Already-handled failures with fallbacks (should be WARNING)

- **All chatbot-tool wrappers in nova.py** (lines ~10138-12012, ~40 sites) — each returns `{"error": ...}` JSON that Claude handles. The outer `nova.py:6476 "Tool %s failed"` already logs it — per-tool wrappers are duplicates.
- **All `market_pulse.py:987-1017` section collectors** — each has its own fallback content.
- **All `google_*.py` API-unavailable paths** — e.g., `nova.py:11281 "google_knowledge_graph module not available"` is an ImportError for an optional module. Should be INFO or DEBUG once per boot, not ERROR per call.
- **Firecrawl URLError / JSONError (credits exhausted)** — known state per project memory.

### (b) External API expected failures (should be WARNING)

- Anthropic API 429/503 / timeout (`nova.py:18352, 18357`) — every retry fires ERROR.
- Tavily/Jina/DuckDuckGo per-tier failures (`tavily_search.py` 9 sites) — multi-tier design means single-tier failures are expected.
- Firecrawl 429 (`firecrawl_enrichment.py:224`) — 402 is already WARNING; 429 should match.
- Supabase transient timeouts (`nova_persistence.py` 20+ sites) — transient latency spikes, no user impact when fallback exists.

### (c) User input / validation failures (should be WARNING, not ERROR)

- `nova_persistence.py:247` — `logger.error("Invalid user_id: %s", user_id)` — this is input validation. A bad user_id is not a bug in Nova.
- `nova_persistence.py:284` — `logger.error("Failed to create conversation: empty result")` — empty DB result isn't necessarily a bug.
- `google_cloud_storage.py:94, 117, 129, 159, 194` — all `"<operation> requires <param>"` validation errors.
- `elevenlabs_integration.py:113, 185, 344` — `"ELEVENLABS_API_KEY is not set"` — config state, not a bug. Should log once at boot.
- `elevenlabs_integration.py:127, 218, 378` — concurrency-limit reached is backpressure, not an error.

### (d) ImportError for optional modules (should be INFO at boot, not ERROR per call)

- `nova.py:11281, 11337, 11505, 11555, 11599` — `"<module> not available"` for optional Google/Meta modules. Currently ERROR on every tool call where the module is absent.
- `api_integrations.py:12444, 12477, 12506, 12535, 12574` — `"<API module> not available"`.
- `skill_target.py:2063` — `"openpyxl not installed"` — should log once, not on every Excel export attempt.
- `skill_target.py:2280` — `"python-pptx not installed"` — same.
- `hire_signal.py:1622, 2206` — same pattern.

### (e) Circuit-breaker state transitions (should be WARNING)

- `resilience_router.py:1614` — the sole `sentry_sdk.capture_exception(error)` — fires on a user-facing error the router already handled with a fallback response. Bypasses `before_send` entirely. Should be WARNING, not captured.

### (f) Already-duplicate logging (single failure triggers 2-3 events)

- **Chat request path**: `nova.py:23290 "Chat request failed"` + `nova.py:6476 "Tool X failed"` + `nova.py:<tool_wrapper> failed` = 3 ERROR events for one user-facing failure.
- **Enrichment cycle**: `data_enrichment.py:2091 "Enrichment cycle crashed"` + per-step errors (9 lines) = up to 10 events per crashed cycle.
- **Gold Standard gates**: `nova.py:3072` + Gate-level errors (7 lines) = up to 8 events.

---

## 5. `before_send` Filter Analysis

**Strengths** (7 rules active, catch a decent chunk):
1. 401/403 credential errors
2. FRED 400 errors (bad series IDs)
3. 429 rate limits (generic)
4. Transient network (timeout / connection reset / DNS)
5. API-integration test failures (startup probes)
6. Startup/warmup noise (conditional on `_DEPLOY_WARMUP_COMPLETE`)
7. "API key not set / missing"

**Weaknesses**:

- **Rule 5** (`_api_test_patterns`) only matches exact "X test failed" — misses `"FRED enrichment failed"`, `"BEA query failed"`, `"Census demographics fetch failed"`, etc.
- **Rule 4** (transient) misses common Anthropic API patterns like `"Claude API HTTP error"` and `"Claude API error"`. Misses Supabase patterns like `"Old-schema insert failed"`.
- **No dedup**: the same fingerprint can fire 100 times in an hour and create 100 events. Sentry's server-side grouping helps, but each one still counts against quota.
- **No fingerprint customization**: the default fingerprint includes the formatted message — `"Tool salary_lookup failed: HTTPError 429"` and `"Tool demand_lookup failed: HTTPError 429"` get separate groups, multiplying event count.
- **No LoggingIntegration configured** — no `level` or `event_level` set, so the SDK defaults apply: any `logger.warning` creates a breadcrumb, any `logger.error` creates an event. No way to selectively drop by logger name.
- **`resilience_router.py:1614`** calls `sentry_sdk.capture_exception` directly — bypasses nothing if `before_send` also runs on manual captures, but these are currently ERROR-level by default.

---

## 6. Three Filter Additions That Cut Volume 50%+

### Addition 1 — Wire a `LoggingIntegration` with `event_level=logging.CRITICAL` (single-line change, biggest impact)

```python
from sentry_sdk.integrations.logging import LoggingIntegration

_sentry_logging = LoggingIntegration(
    level=logging.INFO,          # breadcrumbs for INFO+ (keeps context)
    event_level=logging.CRITICAL # only CRITICAL becomes an event
)
sentry_sdk.init(..., integrations=[_sentry_logging], ...)
```

Effect: **stops the auto-capture of every `logger.error(...)`**. Only explicit `sentry_sdk.capture_exception()` calls and `logger.critical()` create events. Each noisy call-site keeps its local log but stops paging Sentry. Errors the app author marks `.critical` still come through.

**Estimated volume cut: 80-90% immediately.** Real, bubbled-up errors would need explicit `sentry_sdk.capture_exception(e)` calls at the top of the request handler (there's already one in `resilience_router.py`). This is the nuclear option that guarantees the quota stays under cap — and it's what production apps at scale typically do.

### Addition 2 — Add pattern rules to the existing `before_send` for known-benign messages (if you want to keep the `event_level=ERROR` default)

Add to `_sentry_before_send` in `app.py`:

```python
# 8. Drop tool-wrapper failures -- already handled with fallback
_tool_wrapper_patterns = (
    "tool ",              # catches "Tool X failed: ..." (nova.py:6476)
    "failed for ",        # catches "BEA query failed for ..."
    "claude parallel tool ",
    "claude tool ",
    "nova: ",             # catches nova_slack.py:369+
    "market_pulse:",
    "gold standard chat gate",
    "degraded mode:",
    "enrichment failed",
    "supabase error",
    "old-schema insert failed",
    "ga4 ",
    "firecrawl api",
    "gcs ",
    "vision api",
    "claude api http error",
    "claude api error",
    "openpyxl not",
    "python-pptx not",
    "elevenlabs_api_key is not set",
    "elevenlabs concurrency limit",
    "not available",      # catches all "<module> not available" ImportErrors
)
if any(pat in combined for pat in _tool_wrapper_patterns):
    return None

# 9. Drop validation errors (not bugs)
_validation_patterns = (
    "invalid user_id",
    "requires bucket",
    "requires a bucket",
    "requires blob_name",
    "empty access_token",
    "empty result",
    "empty table",
)
if any(pat in combined for pat in _validation_patterns):
    return None
```

**Estimated volume cut: 50-60%** if `event_level` stays at ERROR.

### Addition 3 — Server-side group fingerprinting + rate-limiting per fingerprint

```python
# In _sentry_before_send, after existing filters:

# 10. Normalize fingerprint so "Tool X failed" and "Tool Y failed"
#     collapse into ONE group (don't explode cardinality).
_exc = (hint or {}).get("exc_info")
if _exc and _exc[0]:
    exc_type = _exc[0].__name__
    # Fingerprint by exception type + source logger, not by formatted message
    logger_name = (event.get("logger") or "unknown").split(".")[0]
    event["fingerprint"] = [logger_name, exc_type]

# 11. Simple in-process rate limit: max 5 events per fingerprint per hour
import threading, time
_rl_lock = getattr(_sentry_before_send, "_rl_lock", None)
if _rl_lock is None:
    _sentry_before_send._rl_lock = threading.Lock()
    _sentry_before_send._rl_state = {}  # fp -> [timestamps]
with _sentry_before_send._rl_lock:
    fp = tuple(event.get("fingerprint") or [event.get("logger", ""), ""])
    state = _sentry_before_send._rl_state
    now = time.time()
    timestamps = state.get(fp) or []
    timestamps = [t for t in timestamps if now - t < 3600]
    if len(timestamps) >= 5:
        return None  # same fingerprint fired 5+ times this hour -- drop
    timestamps.append(now)
    state[fp] = timestamps
```

**Estimated volume cut: additional 30-40%** on top of other additions. Traffic spikes that would otherwise drown the quota now contribute at most 5 events per unique error type per hour.

---

## 7. Estimated Reduction if All Recommendations Applied

| Change | Isolated impact | Cumulative |
|---|---|---|
| Addition 1 alone (`event_level=CRITICAL`) | -85% | 85% |
| Addition 1 + explicit capture_exception in ~10 critical paths | -85% + re-add ~5% signal | ~80% net reduction |
| Skip Addition 1, do Addition 2 + Addition 3 | -55% + -35% of remainder | ~70% reduction |
| All three combined (1 + 2 as safety net + 3 as backstop) | belt-and-suspenders | **~90% reduction** |

Projected monthly events: **4,179/month → ~400-800/month** depending on approach. Quota headroom: ~80%. Real bugs still visible. Self-healing bridge (`sentry_integration.py`) keeps functioning because it reads from the Sentry webhook, not the SDK pipeline.

---

## 8. Secondary Recommendations (beyond the 3 filter additions)

- **Demote the ~140 duplicate "X failed" wrappers** in `nova.py` (lines 10138-12012) to `logger.warning`. Most are chatbot-tool wrappers where the tool itself already logs and the wrapper just re-raises a fallback.
- **Gate optional-module-missing messages** behind a module-level `_logged_once` set so they fire once per process rather than per call.
- **Remove the `exc_info=True`** from warnings that are intentionally non-actionable (e.g., all 429 paths). Stack traces inflate event payload size and push Sentry over its per-project storage limit even at low event count.
- **Swap the `resilience_router.py:1614 sentry_sdk.capture_exception(error)`** to `capture_exception(error, level="warning")` — or remove entirely, since the router has fallback logic.
- **Consider a `sentry-sdk` sample rate**: set `sample_rate=0.25` in `sentry_sdk.init` for error events during traffic spikes. Clean 75% reduction at the SDK level with no code changes elsewhere, but loses per-issue visibility. Lower priority than the three additions above.

---

## 9. Files Referenced

Absolute paths for follow-up changes:

- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/app.py` (lines 1472-1598) — `before_send` filter + `sentry_sdk.init`
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/sentry_integration.py` — webhook + healing bridge (read-only here; functions correctly)
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/nova.py` — 108 logger.error sites, top noise source
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/nova_persistence.py` — 29 sites, Supabase transient errors
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/data_enrichment.py` — 29 sites, scheduled cycle errors
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/market_pulse.py` (lines 987-1030) — 7 sites in one collector function
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/tavily_search.py` (lines 187, 196, 204, 267, 272, 277, 363, 368, 373) — per-tier errors
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/firecrawl_enrichment.py` (lines 224, 229, 234, 239) — credits exhausted state
- `/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/resilience_router.py:1614` — the one manual `sentry_sdk.capture_exception` call
