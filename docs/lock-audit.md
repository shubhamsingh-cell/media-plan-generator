# Nova AI Suite -- Threading Lock Audit

Date: 2026-03-25

## Summary

- **Total lock declarations**: 137
- **Total lock acquisitions** (with statements): 378
- **RLock usage**: 1 (llm_router.py only; all others use plain Lock)
- **Nested lock patterns detected**: 0 (no obvious deadlock risk)

## Lock Count by File (top 15)

| File | Lock Count | Purpose |
|------|-----------|---------|
| resilience_router.py | 12 | Circuit breakers, retries, fallback chains |
| app.py | 11 | Generation jobs, API keys, rate limits, chat, streams |
| monitoring.py | 10 | Metrics collectors, persistence, request tracking |
| applyflow.py | 7 | Module imports, sessions, completed jobs |
| sentry_integration.py | 7 | Error tracking, heal stats, bridge |
| tavily_search.py | 6 | Rate limiters for Tavily/Jina/DDG, cache |
| nova_persistence.py | 6 | Per-conversation locks, retry queue, Supabase init |
| nova.py | 6 | Response cache, orchestrator/engine/intel init |
| nova_slack.py | 5 | Bot init, token refresh, thread history, context |
| llm_router.py | 5 | Provider health, A/B testing, response cache |
| web_scraper_router.py | 4 | LRU cache, scraper instances |
| vector_search.py | 4 | Index, rate limiter, Qdrant client, TF-IDF |
| posthog_integration.py | 4 | Client init, stats, rate limiter |
| api_enrichment.py | 4 | Cache, circuit breaker, auth failure, Jooble rate |
| elevenlabs_integration.py | 3 | Credit tracking, TTS cache, rate limiter |

## Lock Categories

### 1. Singleton / Lazy Init Locks (approx. 40)
Guard one-time module initialization. Low contention, no deadlock risk.
Examples: `_llm_router_lock`, `_nova_init_lock`, `_orchestrator_lock`

### 2. Cache Locks (approx. 25)
Protect in-memory caches (dict/list). Short critical sections.
Examples: `_cache_lock`, `_response_cache_lock`, `_fragment_cache_lock`

### 3. Rate Limiter Locks (approx. 20)
Guard request-count lists for per-API throttling.
Examples: `_tavily_rate_lock`, `_jooble_rate_lock`, `_voyage_rate_lock`

### 4. State Tracker Locks (approx. 30)
Protect counters, stats dicts, health scores.
Examples: `_stats_lock`, `_monitor_lock`, `_module_tracker_lock`

### 5. I/O Coordination Locks (approx. 15)
Serialize file writes, buffer flushes, DB operations.
Examples: `_file_lock`, `_write_lock`, `_persistence_init_lock`

### 6. Per-Entity Locks (approx. 7)
Dynamic lock-per-conversation pattern in nova_persistence.py.
Pattern: `_conversation_locks` dict guarded by `_conversation_locks_guard`.

## Deadlock Risk Assessment

**Risk: LOW**

- No nested lock acquisitions detected across the codebase.
- All locks are module-scoped singletons (not passed between modules).
- No lock ordering violations found -- each lock guards an independent resource.
- The only RLock (llm_router.py ResponseCache) is used correctly for re-entrant access.
- Per-conversation locks in nova_persistence.py use a guard lock pattern which is safe as long as the guard is released before acquiring the conversation lock (verified: it is).

## Recommendations

1. **No immediate action required.** The lock count is high but each lock is narrowly scoped and independently used.

2. **Consider consolidation in resilience_router.py** (12 locks). Several per-component locks (CircuitBreaker, RetryPolicy, etc.) could potentially share a single class-level lock if they never need concurrent access.

3. **Consider consolidation in monitoring.py** (10 locks). Multiple metric collectors could use a single lock if they are always updated together.

4. **Add lock acquisition timeouts** for long-running operations. Python's `Lock.acquire(timeout=N)` can prevent indefinite blocking if a deadlock were ever introduced.

5. **Document lock ordering** if cross-module lock acquisition is ever introduced. Currently not needed since all locks are module-local.

6. **Periodic re-audit** recommended when adding new modules or cross-module data flows (e.g., Phase 3 shared campaign context).
