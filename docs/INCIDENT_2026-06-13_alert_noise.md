# Incident: false alert-rate pages during deploy storm (2026-06-13)

**Status:** Root-caused and fixed. Primary fix deployed (`ae0b59a`, live on prod
as `4.0.0-ae0b59a9`). Follow-ups below are open for review.

---

## TL;DR

Production emailed a burst of `[CRITICAL] [Nova] High error rate (>10%)` (17.6%,
11.1%), `[WARNING] Elevated error rate (>5%)`, and `[WARNING] SLO Violation:
error_rate_pct` alerts between **4:06–4:24 PM EDT**. **Nothing was actually
wrong.** They fired during a storm of ~10 rapid `main` auto-deploys: each
gunicorn worker restart cold-starts, so a handful of 5xx over a tiny
rolling-window request count made `error_rate_pct = window_errors /
window_requests * 100` read as a large percentage, then decay as traffic
normalized (17.6 → 11.1 → 9.0 → 5.4 → clear).

Fixed by giving the error-rate alert a post-deploy grace + a minimum request
volume, the same way the latency SLOs already worked.

---

## Timeline (EDT, 2026-06-13)

| Time | Event |
|------|-------|
| 15:05–16:24 | ~10 pushes to `main` (S89 frontend brand pass, hero demo, parity-audit #9, the wsgi auth fix) — each auto-deploys on Render, restarting both gevent workers |
| 16:06–16:09 | `[WARNING] SLO Violation: error_rate_pct` (Current 1.258 / 1.613 / 7.692, Target 1.0) |
| 16:09, 16:11 | `[CRITICAL] High error rate (>10%)` (17.6%, then 11.1% — two CRITICALs 2 min apart) |
| 16:22 | `0474f38 fix(prod): wsgi never set _auth_module_loaded -> all /api/admin/* 503'd` — real 503s on admin endpoints, contributing 5xx |
| 16:15, 16:24 | `[WARNING] Elevated error rate (>5%)` (9.0%, 5.4%) — decaying as traffic normalizes |
| later | Two more deploys superseded everything; prod healthy since |

---

## Root cause

`error_rate_pct` is a **windowed ratio**: 5xx in the rolling window ÷ requests in
the rolling window × 100 (`monitoring.py`, `MetricsCollector.get_metrics`). Two
compounding factors made it page on noise:

1. **Cold-start, tiny denominator.** Each deploy restarts the workers
   (`--preload`, 2 gevent workers). A freshly started worker has only a handful
   of requests in its window; a few cold-start 5xx (a dependency not yet warm, an
   admin 503, a slow first LLM call) read as 17.6% on ~11 requests. `2 / 11 =
   18%`.
2. **No post-deploy grace on the error path.** The latency SLOs
   (`generate_p99_ms`, `chat_p99_ms`, …) each carried `grace_after_deploy_s=300`
   and were skipped during the first 5 min after a restart. `error_rate_pct` was
   the **only SLO without it**, and the global error-rate page
   (`MonitoringAlertBridge._check_cycle`) had **neither** a grace nor a
   minimum-volume guard. So both alert families fired on cold-start noise.

A secondary effect explains the **two CRITICALs two minutes apart** despite the
30-min cooldown: the cooldown (and every dedup cache) is **in-memory per
process**, so each worker restart wiped it and let the next worker re-page. See
follow-up #1.

(`0474f38`'s real admin-503 bug added genuine 5xx on top, amplifying the spike —
but it was already fixed mid-incident and is unrelated to the alerting logic.)

---

## Fix (deployed — `ae0b59a`)

`monitoring.py`:

- `SLO_TARGETS["error_rate_pct"]`: added `grace_after_deploy_s=300` (+`severity`),
  and `check_slo_compliance` now sets `in_grace_period` on its result — so the
  SLO-violation loop skips it during the post-deploy window, exactly like the
  latency SLOs.
- New pure, unit-tested `evaluate_error_rate_alert(error_rate_pct,
  window_requests, uptime_seconds)`: gates the global error-rate page by the
  **300 s grace** AND a **10-request minimum window volume** (aligned with
  `check_slo_compliance`'s existing `_ERR_MIN_SAMPLES`).
- `get_metrics()` now exposes `window_requests` / `window_errors` for the gate.
- Alert **keys and subjects are byte-identical**, so the S63 dedup still matches.

**Behavioural contract:** a real, *sustained* outage still pages once grace
expires (≤ ~6 min after a restart that coincides with the outage). Per-module
health scores (not volume-gated) still catch failures on low-traffic endpoints.

Tests: `tests/test_alert_bridge_grace.py`. Full suite: **2148 passed, 59 skipped**.
Verified live: `GET /api/health` → `4.0.0-ae0b59a9`, healthy.

## Companion PR (this branch — not deployed)

`harden/alert-observability`:

- Error-rate alert body now includes the **request denominator** ("… across N
  requests in the last hour …") so on-call can tell a real incident from a
  tiny-sample artifact at a glance. Subjects unchanged (dedup intact).
- `evaluate_error_rate_alert` logs *why* it suppressed (grace vs volume) at
  DEBUG, so "why didn't it page?" / "is the guard working?" is answerable from
  logs.
- This incident report.

---

## Open follow-ups (prioritized, NOT yet done)

### P1 — Alert dedup/cooldown is wiped on every restart
Every dedup layer is in-memory per process:
- `MonitoringAlertBridge._alert_cooldowns` (30 min)
- `alert_manager._dedup_cache` (4 h)
- `email_alerts._dedup_cache` (30 min → 4 h exponential backoff)

A gunicorn worker restart (deploy, OOM, `--max-requests` recycle) zeros all
three, so during a deploy storm the same alert can re-send per worker. The S90
grace fix covers the **error-rate** path specifically; other alert types
(health-score, latency SLO violations, burn rate) still rely on the cooldown that
resets.

**Recommendation:** persist cooldown timestamps to a shared store (the existing
Supabase `metrics_snapshot` mechanism is the natural home — add a small
`alert_cooldowns(alert_key PK, last_fired_ts)` table) and load on bridge init,
**fail-open** (if the store is unreachable, allow the alert — never silently
suppress a real page). Degrades to today's in-memory behaviour when Supabase env
vars are absent. Suppression-only; cannot add pages.

### P2 — `compute_burn_rate()` is a silent no-op for `error_rate_pct`
`MetricsCollector.compute_burn_rate` computes
`allowed = 1.0 - target if target <= 1.0 else target`. For `error_rate_pct`,
`target = 1.0`, so `allowed = 0`. It also forces `actual = 0` for any
`current > 1.0` (and `current` is stored as a **percentage**, e.g. `17.6`). Both
bugs drive `burn_rate = 0` → status `"ok"` for *any* error rate, including a 100%
outage. So the burn-rate path never provided real defense-in-depth for errors.

**Recommendation (needs review — it enables a pager):** for a percentage error
SLO, `burn_rate = actual_pct / target_pct` (e.g. `17.6 / 1.0 = 17.6x`). But note
the existing thresholds (`>5x` critical, `>2x` warning) would then fire CRITICAL
at >5% error rate — **more aggressive than the error-rate page itself** — so it
MUST also be grace-gated + volume-gated, and the thresholds re-tuned, before
enabling. Deliberately deferred rather than shipped unsupervised.

### P3 — Module-health / "Module DOWN" alerts are not grace-gated
`_check_cycle` fires `health_degraded` (score < 70), `health_critical` (< 40),
and `Module DOWN` (< 20) with no post-deploy grace. They did **not** misfire in
this incident (a freshly started `ModuleHealthTracker` reports healthy / has
insufficient data), but for symmetry they should honour the same grace. Low risk,
suppression-only. Confirm the tracker's cold-start default first.

### P4 — Deploy detection is effectively dead code (minor)
`_check_cycle`'s deploy detection compares `VERSION` to `self._last_known_version`,
but each deploy is a fresh process where `_last_known_version` is *initialized to
the new* `VERSION`, so the value never changes within a process and the INFO
"Deploy detected" alert can't fire. Harmless (INFO), but either wire it to a
persisted previous-version (pairs with P1) or remove it.

---

## Appendix — alert inventory after S90

| Alert (subject) | Severity | Source | Post-deploy grace? | Volume guard? |
|---|---|---|---|---|
| High error rate (>10%) | CRITICAL | bridge / `evaluate_error_rate_alert` | ✅ 300 s (S90) | ✅ 10 req (S90) |
| Elevated error rate (>5%) | WARNING | bridge / `evaluate_error_rate_alert` | ✅ 300 s (S90) | ✅ 10 req (S90) |
| SLO Violation: error_rate_pct | WARNING | bridge / SLO loop | ✅ 300 s (S90) | ✅ `_ERR_MIN_SAMPLES=10` |
| SLO Violation: *_p99_ms / _p50_ms | WARNING | bridge / SLO loop | ✅ 300 s (pre-existing) | n/a (sample-size gated) |
| Burn rate critical/elevated | CRIT/WARN | bridge / `compute_burn_rate` | ❌ | ❌ (currently a no-op — P2) |
| Anomaly detected | INFO | bridge / `check_anomalies` | ❌ (slow sensor, ≥10 samples) | n/a |
| Module degraded/critical/DOWN | WARN/CRIT | bridge / module health | ❌ (P3) | n/a |
| Deploy detected | INFO | bridge | n/a (dead — P4) | n/a |

Dedup windows (all in-memory — P1): bridge cooldown 30 min · `alert_manager` 4 h ·
`email_alerts` 30 min→4 h backoff.
