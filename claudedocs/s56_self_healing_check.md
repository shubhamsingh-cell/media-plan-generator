# Nova AI Suite — Self-Healing / Self-QC / Self-Upgrade Deep Check (S56)

**Date**: 2026-04-24
**Scope**: Verify what's wired, what's firing, and what's dead code.
**Method**: Read-only static analysis + live probes against https://media-plan-generator.onrender.com with admin key.

---

## TL;DR Verdict

Nova has a **real, running self-healing spine** for three things — data-matrix probes, circuit breakers on LLM providers, and statistical anomaly detection on request latency. Everything else the user remembers is either (a) a dashboard with no production callers, (b) a library function nobody imports, or (c) logs that never reach a human.

The single most damaging finding is a **non-reentrant-lock deadlock** in `auto_qc.get_status()` that causes `/api/health/auto-qc` to hang forever in production — the observability endpoint you'd check to confirm "is self-healing working?" is itself broken.

---

## Summary Table

| Module | Wired to runtime? | Firing? | Alerts reaching a human? | Notes |
|---|---|---|---|---|
| `auto_qc.py` | YES (`app.py:5641-5648` starts background thread) | YES (60s probe loop, 90s grace, 5-check warmup) | Only if RESEND_API_KEY set; otherwise logfile (`/tmp/nova_alerts.log`, wiped on Render restart) | **DEADLOCK BUG**: `get_status()` holds `_lock` and then calls `get_sla_report()` which re-acquires same `threading.Lock()`. `/api/health/auto-qc` hangs 90+s (verified). Comment claims "weekly self-upgrade" but **NO self-upgrade code exists in file**. |
| `resilience_router.py` | Lazy singleton, only built when dashboard endpoint hit | NO production traffic. Router registered, circuits initialized, but `resilient_fetch()` has **zero non-test callers** | N/A (no failures to alert on) | Dashboard endpoint `/api/resilience/status` returns 19KB of "OK" data with total_successes=0 across all tiers. Well-built library that products don't use. |
| `circuit_breaker_mesh.py` | YES — `llm_router.py:2067-2072` registers 23 providers at import; `llm_router.py:3581,3584,3594,3597,3626,3670,3673` records success/failure on every LLM call | YES (live data on `/api/health` main endpoint includes `circuit_breaker_mesh` block with states, health scores) | Indirect — bad providers trigger `llm_router` fallback silently; no alert fires until a higher-level module complains | Only self-healing module with genuine production-hot-path wiring. This is the real deal. |
| `data_matrix_monitor.py` | YES (`app.py:5630-5637`) | YES (confirmed 2026-04-24 probe: check #1, 100% healthy, 38 OK / 0 error / 7 partial / 2.325s duration) | Yes if check returns errors >1 (first check suppressed), via `alert_manager.send_alert()`. **Startup suppression ON** for first check. | 12-hour interval + 5-min startup delay. Self-heals by reimporting modules, resetting Nova `_orchestrator` sentinel, clearing API cache. Heals are logged + written to `_heal_log` (max 20 entries, memory only). Extended health probes real APIs (FRED, BLS, Adzuna, ONET) — but these spawn sync network calls every 12h. |
| `alert_manager.py` | YES — imported by `data_matrix_monitor.py:39`, `resilience_router.py:1440`, `monitoring.py:2392`, `data_enrichment.py:50`, `sentry_integration.py:46` | YES (dedup, rate limit, 4-tier fallback) | **Tier 1 Resend: YES iff RESEND_API_KEY set on Render (per render.yaml config). Tier 2 Slack: NO — slack_alerter uses `SLACK_ALERTS_WEBHOOK_URL` which per MEMORY.md is NOT SET. Tier 4 logfile: always succeeds but writes to `/tmp/nova_alerts.log` which is ephemeral on Render (lost on restart/deploy).** | Severity mapping includes a Tier 3 reference in docstring but no Tier 3 exists — the comment says "Tier 4: logfile" skipping a number. Rate limit 10/hour, dedup 1h/subject. |
| `anomaly_detector.py` | YES — `app.py:9751, 11907` records `METRIC_REQUEST_LATENCY` on every GET + POST response; `nova.py:10658` exposes as chatbot tool | YES — verified: `/api/health/anomalies` returns `tracked_metrics: 1` live | **NO alert path**. The detector flags anomalies (3-sigma) into `_active_anomalies` dict and logs at WARNING level, but nothing reads `get_active_anomalies()` outside the HTTP endpoint. No bridge to `alert_manager`. | Detector defines 4 metrics (`REQUEST_LATENCY`, `ERROR_RATE`, `MEMORY_USAGE`, `RESPONSE_SIZE`). Only latency is ever recorded. 3 of 4 metrics are dead. |
| `auto_feedback_trainer.py` | **DOES NOT EXIST**. `grep -r 'auto_feedback\|feedback_trainer' --include='*.py'` returns zero hits in any .py. | N/A | N/A | User's memory of building one is incorrect OR the file was deleted and never replaced. No evidence it ever shipped. |
| `data/benchmark_drift_results.json` | YES — populated by `data_enrichment._enrich_benchmark_drift_check()` | EMPTY (146 bytes, `total_checked: 0, results: []`, last modified 2026-04-05). Scheduled every 2160h (90 days) — so the single run that happened produced zero comparisons. | No — drift alerts go to `alert_manager.send_alert()` but there are no drifts to alert on. | The underlying drift check is real code, but it needs **stored benchmarks AND live Adzuna CPC/CPA** to compare. The last run had nothing to compare, so zero rows. This is a file that looks scheduled but has never produced a useful output. |

### Scheduled / Background Threads

| Thread Name | Registered by | Frequency | Actually started? |
|---|---|---|---|
| `auto-qc` | `auto_qc.start()` called from `app.py:5644` (`_auto_qc.start_background()`) | Every 60s after 90s grace + 5-cycle warmup | YES |
| `data-matrix-monitor` | `data_matrix_monitor.start_background()` called from `app.py:5633` | Every 12h after 5-min initial delay | YES (verified check #1 ran at t+~5min) |
| `monitor-alert-bridge` | `monitoring.start_alert_bridge()` called from `app.py:5654` | Every 60s | YES |
| `enrichment` | `data_enrichment.start_enrichment()` called from `app.py:5663` | Hourly freshness checks | YES |
| `heal-cache-staleness` | On-demand from `data_matrix_monitor._check_cache_staleness()` when stale sources found | Fires only inside 12h matrix cycle | YES (conditional) |
| **NONE** for `resilience_router` | — | — | No periodic thread; lazy-init on first call |
| **NONE** for `anomaly_detector` | — | — | No alert thread; detection is passive (fires only if someone calls `check_anomaly()`) |

### Live Health Endpoint Probes (2026-04-24, admin key applied)

| Endpoint | Status | Time | Body |
|---|---|---|---|
| `/api/health/auto-qc` | **TIMEOUT (90s+, bytes=0)** | >90s | Nothing returned. **Deadlock confirmed.** |
| `/api/health/anomalies` | 200 | 0.48s | `{anomaly_count:0, active_anomalies:{}, tracked_metrics:1, checked_at:"2026-04-24T07:29:42Z"}` |
| `/api/health/eval` | 200 | 1.76s | Overall 93.75%, 120/128 cases pass, categories: Budget 96.67%, Collar 88.24%, Geo 100%, CPA 96.15% |
| `/api/health/data-matrix` | 200 | 0.49s (after 5-min warmup) | 100% healthy, 38 ok, 0 error, 7 partial, check #1, 2.325s probe duration |
| `/api/health/slos` | 200 | 0.50s | All SLOs compliant (in grace period, sample_size=0 for most, err_rate 0.174% of target 1.0%) |
| `/api/health/enrichment` | **500** | 0.47s | `{"error":"Enrichment status check failed: 'NoneType' object is not callable"}` — wired but broken |
| `/api/observability/platform` | **503** | 0.44s | `{"error":"Platform observability check failed"}` — wired but broken |
| `/api/resilience/status` | 200 | 0.48s | 19KB of all-green circuit data; **every total_successes=0, total_failures=0** → nobody is routing through this |

### Environment Variable Status (per MEMORY.md + render.yaml)

| Var | Purpose | Set? |
|---|---|---|
| `RESEND_API_KEY` | Tier 1 Resend email alerts | Listed in render.yaml; likely set (MEMORY shows 64 total Render env vars) |
| `ALERT_EMAIL` / `ALERT_EMAIL_TO` | Recipient | Set |
| `SLACK_WEBHOOK_URL` | Plan notifications (not alerts) | Set |
| **`SLACK_ALERTS_WEBHOOK_URL`** | **System alerts via slack_alerter** | **NOT SET** (confirmed in MEMORY.md: "NOT SET (system alerts disabled)") |
| `SLACK_BOT_TOKEN` | Bot posting | Set |
| `SENTRY_DSN` | Breadcrumbs + error capture | Set |

---

## Top 5 Issues (user thinks it works, it doesn't)

1. **`/api/health/auto-qc` deadlocks forever.** `auto_qc.get_status()` (line 291) acquires `_lock` then calls `get_sla_report()` (line 306) which tries to acquire the same non-reentrant `threading.Lock()` at line 271. Verified: 3 parallel curls all timed out at 30s, one waited 90s and got zero bytes. This is the primary observability endpoint for self-QC and it is silently broken in production right now. **Fix is 1 line: change `_lock = threading.Lock()` to `_lock = threading.RLock()` OR inline the SLA math into `get_status()` without re-acquiring the lock.**

2. **"Weekly self-upgrade" is a lie.** `app.py:5645` logs `"AutoQC engine started (tests every 12h, self-upgrade weekly)"` — but `auto_qc.py` contains **zero** self-upgrade logic. The module does health probes every 60s and nothing else. No code rewrites itself, no model re-tuning, no config drift auto-correction. The log line is aspirational and misleading.

3. **`resilient_fetch()` has no production callers.** The resilience router wraps 6 service categories (caching, database, email, analytics, errors, logging) with tiered fallback — but `grep -rn resilient_fetch` finds only the definition, the dashboard, and tests. Nothing in `app.py`, `nova.py`, `api_enrichment.py`, `data_orchestrator.py`, or any other production module imports `resilient_fetch`. The dashboard shows `total_successes=0` across all 6 tiers for all services — that's not a startup artifact, it's truth. It is a fully-built parallel universe that products don't use.

4. **Anomaly detector has no alerting path and 3 of 4 metrics are dead.** The module defines `METRIC_REQUEST_LATENCY`, `METRIC_ERROR_RATE`, `METRIC_MEMORY_USAGE`, `METRIC_RESPONSE_SIZE`. Only `REQUEST_LATENCY` is ever recorded (`app.py:9753, 11909`). When an anomaly IS detected, it is logged at WARNING level only — there is no bridge to `alert_manager`. `/api/health/anomalies` reports `tracked_metrics: 1` correctly, but a 10x latency spike at 3am would log a warning that no human sees.

5. **`alert_manager` Tier 2 is a dead pipe; Tier 4 is ephemeral.** Tier 2 (Slack fallback) delegates to `slack_alerter.send_slack_alert()` which needs `SLACK_ALERTS_WEBHOOK_URL` (note the "ALERTS" — different from `SLACK_WEBHOOK_URL`). Per your own MEMORY.md this env var is NOT SET. Tier 4 writes `/tmp/nova_alerts.log` which gets wiped on every Render deploy/restart. So: if RESEND_API_KEY is unset or Resend rate-limits, **no alert ever reaches a human** and the only evidence is gone on next restart. There is also a gap in the comment block (Tier 3 is mentioned in `_try_slack` docstring but the code only has Tiers 1, 2, 4 — the numbering is broken and implies a Tier 3 that was removed or planned).

---

## Top 5 Opportunities (almost wired, one small fix to activate)

1. **Fix the auto_qc deadlock.** One-line change: `_lock = threading.Lock()` → `threading.RLock()` in `auto_qc.py:35`. Restores visibility into the system's own self-QC. Zero risk; `threading.RLock` has identical semantics with re-entrancy allowed.

2. **Wire anomaly_detector → alert_manager.** In `anomaly_detector.AnomalyDetector.check_anomaly()` around line 210 (the `is_anomaly` branch after the cooldown check), add: `from alert_manager import send_alert; send_alert(subject=f"Anomaly: {name}", body=f"value={latest_value} vs baseline={mean}±{std_dev}", severity="warning")`. The detector already has cooldown logic (`ALERT_COOLDOWN_SECONDS=300`) so no extra rate-limiting needed.

3. **Emit the 3 missing anomaly metrics.** Add `record_metric(METRIC_ERROR_RATE, ...)` in the same two `app.py` response hooks (lines 9751, 11907) using `1.0 if _response_status >= 500 else 0.0`. Add `record_metric(METRIC_MEMORY_USAGE, ...)` inside `data_matrix_monitor._check_memory_pressure()` (it already computes `rss_mb`). Add `record_metric(METRIC_RESPONSE_SIZE, ...)` from the same response hooks using `Content-Length`. ~12 lines total, activates 3x more visibility.

4. **Set `SLACK_ALERTS_WEBHOOK_URL` on Render.** This single env var unlocks Tier 2 of `alert_manager` (for all 5 callers: data_matrix_monitor, auto_qc, monitoring bridge, data_enrichment, sentry_integration). You already have a Slack workspace and a separate `SLACK_WEBHOOK_URL` for plan deliveries — just create a second webhook to `#alerts` or similar and set `SLACK_ALERTS_WEBHOOK_URL`. No code changes.

5. **Migrate ONE high-traffic path to `resilient_fetch()`.** The easiest target is `supabase_cache.get()` / `set()` — wrap it in `resilient_fetch("caching", "get", key=...)`. This gives the router real traffic, populates the circuit breakers, and makes the dashboard at `/api/resilience/dashboard` stop lying about zero usage. It also validates the whole resilience_router stack with production traffic before you lean on it for anything mission-critical.

---

## Verdict

Nova has **partial self-healing**: the circuit-breaker mesh for LLM providers is production-grade and running hot, and the data-matrix monitor does real self-heal (reimport, cache clear, cycle trigger) on a 12h cadence with 100% healthy results today. Those two systems earn the "self-healing" label.

But the **self-QC loop is blind** because `/api/health/auto-qc` deadlocks on every request, the **self-upgrade is fictional** (no code behind the log message), the **resilience router is a shelf-ware library** (fully built, zero production callers), the **anomaly detector flags things into a void** (no alert path wired), and the **alert pipe itself is half-open** (Slack ALERTS webhook missing, logfile is ephemeral).

So: "does Nova have self-healing today?" → Yes for LLM routing and data-matrix probes. No for everything else. The good news is 4 of the 5 top opportunities are 1-10 line fixes that would make the user's mental model match reality.
