# S56 Typo Hang — Root Cause Analysis

**Query:** `"whats the normal cpc for healtchare jobs in Washington dc"` (typo: "healtchare")
**Endpoint:** `POST /api/chat/stream` on `https://media-plan-generator.onrender.com`
**Symptom:** 50 s elapsed, only ~429 bytes received, no `done` event, client times out.
**Status:** Reproduces after S55 (`a3fba93`) + S56 (`9cc6a5f`) deployed.

---

## TL;DR — Root cause

`app.py::_enrich_chat_context` (chat request pre-processing, runs *before* any SSE body event
is streamed) uses a `with ThreadPoolExecutor(...) as pool:` context manager. Its exit calls
`shutdown(wait=True)`, which **blocks until every worker thread finishes**. One of the submitted
tasks is `_enrich_vector`, which calls the **unbounded** `vector_search.search(...)`
(imported as `_vector_search` at `app.py:215-218`). Under Voyage-AI free-tier rate-limit pressure
(10 RPM, min 6.5 s inter-request delay, blocking `time.sleep()` in `embed_batch`), that call
can sleep for **up to 60 s**, so the whole `_enrich_chat_context` call — and therefore the whole
`/api/chat/stream` request — stalls for the entire rate-limit wait.

This is literally the same bug S56 fixed in `nova.py::_bounded_vector_search` (S56 switched
from `ThreadPoolExecutor(...)` context manager to a bare daemon `threading.Thread` + `join(timeout=...)`
specifically because `shutdown(wait=True)` defeats the per-future timeout). **S56 missed this
instance in `app.py::_enrich_chat_context`.**

The typo query is a specific victim because the query class normally resolves on a fast path
(`"cpc for healthcare in DC"` hits `_fast_path_benchmark_lookup` in ~2.3 s with zero LLM calls
and zero vector search), but the misspelling **"healtchare"** does not match
`Nova._VERTICAL_KEYWORDS_BM` (`nova.py:14563`), so `_fast_path_benchmark_lookup` returns
`None` and the request falls through to the long path that includes the Voyage-bound enrichment.
Correctly-spelled data queries and greetings never exercise this code path long enough to
notice — they either short-circuit on the fast path or finish before Voyage's sliding window
fills.

---

## Annotated code path for the typo query

`/Users/shubhamsinghchandel/Downloads/Claude/media-plan-generator/`

1. **Request arrives** at `app.py:16866` (`POST /api/chat/stream` handler).
2. **SSE headers flushed** at `app.py:17108-17117`. *No body yet — the client only knows the
   connection is open.*
3. **`_enrich_chat_context(data, message)`** is called at `app.py:17122`. **This is the hang.**
   Inside (`app.py:6443-6664`):
   - `_enrich_onet` — enabled because message contains `"jobs"` (gate at `app.py:6504-6507`).
   - `_enrich_adzuna` — skipped (no "salary/pay/compensation" keyword).
   - `_enrich_fred` — skipped.
   - `_enrich_jobspy` — enabled (gate on "jobs").
   - `_enrich_tavily` — skipped.
   - `_enrich_vector` — **always enabled, no keyword gate** (`app.py:6621-6632`).
     - Calls `_vector_search(_chat_msg, top_k=3)` where `_vector_search = vector_search.search`
       imported at `app.py:215-218`.
     - `vector_search.search` (`vector_search.py:1237`) calls `embed_text(query)` at line 1265
       when the Qdrant / in-memory index is loaded.
     - `embed_text` → `embed_batch` (`vector_search.py:452-`) enforces Voyage-AI rate limits
       with **blocking `time.sleep(wait_time)`** calls at lines 540 and 557, where `wait_time`
       can be **up to ~60 s** when the 10-RPM sliding window is saturated
       (`vector_search.py:549-557`).
   - Tasks are dispatched via `ThreadPoolExecutor(max_workers=4)` (`app.py:6645-6651`).
   - Each future has a `fut.result(timeout=8)` cap (`app.py:6651`). That **only** bounds the
     wait; it does NOT cancel the worker thread (Python futures can't preempt a blocking call).
   - **Line 6645: `with ThreadPoolExecutor(...) as pool:` — on `__exit__` the context manager
     calls `pool.shutdown(wait=True)`, which blocks until every worker thread returns. The
     `_enrich_vector` worker is still `sleep`-ing inside Voyage rate-limit wait, so
     `_enrich_chat_context` does not return for the full duration of that sleep (up to ~60 s).**
4. **Only after `_enrich_chat_context` returns** does the handler create the `_ka_thread`
   (keepalive, `app.py:17199-17202`) and emit the first status event
   `{"type":"status","status":"Thinking..."}` at `app.py:17205`, then enter
   `handle_chat_request_stream` at `app.py:17215`, which yields its own
   `"Analyzing your question..."` status and the plan event
   (`nova.py:23449`, `nova.py:23459`).
5. The observed ~429 bytes = the post-hang burst: ~60 B initial status + ~70 B "Analyzing"
   status + ~200–280 B plan + maybe ~80 B `tool_start` for `track_cpc` that *just* begins
   before the client's 50 s read cutoff. No keepalives appear in that window because the
   keepalive thread doesn't start until *after* the hang (`app.py:17202`).
6. Had the client waited longer, the rest of the path would have run:
   - `Nova.chat` (`nova.py:12757`) falls through the fast paths:
     - `_fast_path_benchmark_lookup` (`nova.py:14618`) matches
       `_BENCHMARK_QUESTION_INTENT` on "normal" + "cpc", but returns `None` at line 14647
       because `_VERTICAL_KEYWORDS_BM` has no entry for the typo `"healtchare"` (verified by
       regex test).
     - `_fast_path_supply_listing` — no match (no "list/share/give…" verb).
     - `_try_direct_tool_dispatch` (`nova.py:15466`) matches `_CPC_TRACK_INTENT`
       (`nova.py:14092`, `\bcpc\b` hits) and fires `track_cpc({"role": "healtchare jobs"})`
       (`nova.py:15942-15961`).
   - `execute_tool` wraps the handler in a bounded 5 s `_PER_TOOL_TIMEOUT`
     (`nova.py:6370, 6388-6432`), so `_adzuna_live_cpc` (`market_signals.py:1081-1119`,
     8 s `urlopen` timeout) is capped at 5 s.
   - Synthesis via `call_llm(..., timeout_budget=20.0)` (`nova.py:16291-16297`): concurrency
     semaphore (≤10 s wait, `llm_router.py:2987`) + inner loop (≤20 s budget, line 3091-3112).
     Worst case ~30 s.
   - Post-processing (`_filter_competitor_names`, `_sanitize_refusal_language`,
     `_enrich_response_quality`, `_append_follow_ups_to_response`) is all pure in-memory.
   - Back in `handle_chat_request` (`nova.py:23033`), `_enrich_response` runs another pass
     that uses `vector_search.search_bounded(timeout_s=3)` (post-S56, safe).

So the *reasonable* end-to-end budget for this query is:
- `_enrich_chat_context`: **currently unbounded; intended ~8 s**
- `_check_learned_answers` + `_intelligent_cache_get`: ~2 s
- `track_cpc` tool: ≤5 s
- synthesis `call_llm`: ≤30 s (10 s semaphore + 20 s budget)
- post-enrichment: ≤3 s
- **Total intended: ~48 s. Total observed on a bad Voyage window: ~48 s + 50+ s of
  `_enrich_chat_context` stall = > 90 s, hit by 75 s `_STREAM_TIMEOUT` in `handle_chat_request_stream`.**

---

## Why other queries do NOT reproduce the hang

| Query | Fast path match | Reaches `_enrich_chat_context`? | Observed |
|-------|-----------------|---------------------------------|----------|
| `"hi nova"` | Greeting early-exit (`nova.py:13010-13147`) returns before SSE starts streaming tokens. But `_enrich_chat_context` still runs first on the SSE path! | Yes | 2 s — meaning Voyage window was *cold* at that moment |
| `"cpc for healthcare in DC"` | `_fast_path_benchmark_lookup` hits (`nova.py:14622`, vertical=`healthcare` matches) | Yes | 2.3 s — Voyage window was cold |
| `"what is the best time to post jobs on indeed"` | No early exit; falls to LLM tool loop. | Yes | 26 s — Voyage likely returned in ≤ few seconds |
| `"whats the normal cpc for healtchare jobs in Washington dc"` | Benchmark fast path MISSES (typo kills vertical match); direct-dispatch path runs `track_cpc`. **Crucially, this query is being submitted repeatedly — so Voyage's 10 RPM sliding window fills up, and each new request in this class stalls `_enrich_chat_context` for ~50 s while Voyage's rate-limit `time.sleep()` drains.** | Yes | 50+ s hangs |

The reproducibility of the typo hang is therefore a function of **request ordering + Voyage
rate-limit state**, not the typo per se. Any query that reaches this endpoint while the Voyage
sliding window is full will hang; the typo query just happens to be the one being tested.

Queries that *don't* hit `_enrich_chat_context` because they're served by the static
`/api/chat/stream` pre-validation path (validation errors, auth failures, oversize message)
are not affected.

---

## Why S55 and S56 did not close the gap

- **S55** bounded the `nova.py` pipeline's direct vector_search calls via
  `_bounded_vector_search`. That fixed the *main chat thread's* 60 s blocking behavior
  for vector lookups inside `Nova.chat` and `_enrich_response`.
- **S56** generalized the pattern with `vector_search.search_bounded(query, top_k, timeout_s)`
  and migrated five more call sites in `nova.py` plus the `data_orchestrator.py` source handler
  (see commit `9cc6a5f` description). It also correctly pointed out the
  `ThreadPoolExecutor(...) as pool` mistake and fixed it in `nova.py::_bounded_vector_search`.
- **S56 missed `app.py::_enrich_chat_context`**, which predates the chat path and runs on
  every `/api/chat` and `/api/chat/stream` request. The `_enrich_vector` inner function
  still calls the raw `search` import (aliased `_vector_search`) with no bounding. And the
  outer `with ThreadPoolExecutor(max_workers=4) as pool:` in that function has the same
  `shutdown(wait=True)` bug S56 called out.

Net: the chat pipeline now degrades gracefully when Voyage is throttled, but the *request
pre-processing layer* in front of it does not. Since pre-processing runs before any body byte
goes on the wire, from the client's point of view it looks like the whole request is hung.

---

## Evidence

Regex tests verifying the typo path (reproduced locally with Python 3):

```
msg = "whats the normal cpc for healtchare jobs in Washington dc"
_BENCHMARK_QUESTION_INTENT.search(msg)       -> True   (catches "normal ... cpc")
_VERTICAL_KEYWORDS_BM lookup                 -> None   (typo skips 'healthcare')
  => _fast_path_benchmark_lookup returns None (nova.py:14647)
_SUPPLY_LISTING_INTENT.search(msg)           -> False
  => _fast_path_supply_listing returns None
_SUPPLY_DEMAND_INTENT / _GEOCODE_INTENT /
_AUDIT_INTENT / _EMPLOYER_BRAND_INTENT /
_SLOTOPS_PREDICT_INTENT / _SLOTOPS_OPTIMIZE_INTENT /
_CAMPAIGN_OPTIMIZE_INTENT / _POSTING_DECAY_INTENT /
_ROI_PROJECT_INTENT                          -> all False
_CPC_TRACK_INTENT.search(msg)                -> True   (hits \bcpc\b)
  => direct dispatch fires track_cpc(role="healtchare jobs")
```

Byte-budget accounting for the observed 429 bytes at 50 s:

```
  60  status event  {"type":"status","status":"Thinking..."}       (app.py:17205)
  70  status event  {"status":"Analyzing your question...", ...}   (nova.py:23449)
 250  plan event    {"type":"plan","plan":"I'll pull ..."}         (nova.py:23459)
  80  tool_start    {"type":"tool_start","tool":"track_cpc", ...}  (nova.py:6362-6374)
-----
 460  ≈ observed 429 (the tool_start partially trimmed mid-write or hasn't fully flushed yet)
```

Keepalive events (`{"keepalive":true}`, ~30 B each) do NOT fit here because the
`_ka_thread` is not started until `app.py:17202`, which runs AFTER the stalled
`_enrich_chat_context`. That's consistent with the observation that no keepalive framing
appears in the 429 bytes.

Additional evidence:

- `app.py:6621-6632`: `_enrich_vector` calls `_vector_search` (the unbounded
  `vector_search.search`) with NO outer timeout or daemon-thread wrapper.
- `app.py:6645-6651`: the `with ThreadPoolExecutor(...) as pool:` pattern S56 explicitly
  warned about is still used here, so `shutdown(wait=True)` waits for `_enrich_vector` to
  return.
- `vector_search.py:530-557`: blocking `time.sleep(wait_time)` where `wait_time` can be up to
  60 s under rate-limit pressure. `time.sleep` is not cancellable and will not honour any
  outer `fut.result(timeout=8)`.

---

## Proposed fix

**Minimal surgical change:** apply the same S56 pattern to `_enrich_vector` and replace the
`ThreadPoolExecutor` context manager with explicit daemon threads + `join(timeout=...)`. No
behavior change when Voyage is healthy; worst case without the fix goes from ~60 s to ~3 s
silent degradation.

### Patch 1 — `app.py:6621-6632` (use bounded vector search)

```python
    def _enrich_vector() -> None:
        if not (_vector_search_available and _vector_search):
            return
        try:
            # S56 follow-up: use bounded variant so Voyage rate-limit can't stall
            # the enrichment thread (and by extension the whole request).
            from vector_search import search_bounded as _bounded_vs
            results = _bounded_vs(_chat_msg, top_k=3, timeout_s=3.0)
            _merge("vector_kb_results", results)
            if results:
                logger.info(
                    "Enriched chat with vector search (%d KB matches)", len(results)
                )
        except (ValueError, TypeError, OSError) as exc:
            logger.error("Vector search enrichment failed: %s", exc, exc_info=True)
```

### Patch 2 — `app.py:6644-6659` (replace `with ThreadPoolExecutor(...)` with daemon threads)

Swap the context-manager executor for a bare `ThreadPoolExecutor` and explicitly abandon
pending work instead of waiting on shutdown:

```python
    # -- Dispatch all enrichment tasks concurrently (8s combined hard ceiling) --
    _enrich_deadline = time.time() + 8.0
    _enrich_threads: list[threading.Thread] = []
    for _fn in enrichment_fns:
        _t = threading.Thread(target=_fn, daemon=True, name="chat-enrich")
        _t.start()
        _enrich_threads.append(_t)
    for _t in _enrich_threads:
        _remaining = max(0.0, _enrich_deadline - time.time())
        _t.join(timeout=_remaining)
        if _t.is_alive():
            logger.warning(
                "Chat enrichment task %s still running after 8s cap — abandoning",
                _t.name,
            )
    # No shutdown(wait=True): threads are daemon, they die with the process if stalled.
```

This mirrors the daemon-thread pattern S56 adopted in `vector_search.py::search_bounded`
and `nova.py::_bounded_vector_search`. The outer 8 s ceiling is now honoured even if an
individual worker is blocked in a non-cancellable `time.sleep`.

### Optional defensive change

Consider adding a keyword gate to `_enrich_vector` similar to the other enrichment fns (e.g.
only run when the message contains a recruitment keyword or exceeds 20 characters). This
reduces Voyage API usage per request and lowers the chance of the sliding window filling up.
But this is an efficiency change, not a correctness fix — Patch 1 + Patch 2 are sufficient
to stop the hang.

### Verification plan

1. Locally: rate-limit mock the Voyage endpoint to always 429, confirm
   `_enrich_chat_context` returns in ≤ 8 s instead of ~60 s.
2. Deploy to staging / Render, issue the exact hanging query 12× in a minute (enough to
   saturate Voyage's 10 RPM window). Every response should now arrive within the
   `_STREAM_TIMEOUT` of 75 s, and the `"whats the normal cpc for healtchare jobs in
   Washington dc"` request should finish in < 35 s regardless of Voyage state.
3. (Independent improvement) Consider broadening `Nova._VERTICAL_KEYWORDS_BM` to be fuzzy
   (Levenshtein distance ≤ 2) or running a cheap typo-correction before the regex. This is
   *not* required to fix the hang, but it would keep this query class on the fast path.
