"""Shared request quota for the Adzuna API key.

Every Adzuna HTTP call -- api_enrichment.fetch_job_market, the
api_integrations.AdzunaClient (the /api/integrations/market-snapshot
route, data_orchestrator, data_refresh), market_signals'
``_adzuna_live_cpc`` and the data_matrix_monitor health probe -- uses ONE key,
so they all draw from ONE counter here.

Provider terms (per key): 250 requests/day, 1,000/week, 2,500/month.

Primary counter: Upstash Redis (the same REST client the response cache uses,
upstash_cache). Atomic INCR on a per-UTC-day key and a per-UTC-month key, so
the count is shared by every gunicorn worker and survives restarts and
deploys:

    nova:quota:adzuna:d:YYYYMMDD   <= GLOBAL_DAILY_CAP   (200; ToS 250/day)
    nova:quota:adzuna:m:YYYYMM     <= GLOBAL_MONTHLY_CAP (2,300; ToS 2,500/month)

The caps sit under the ToS ceilings to leave room for manual use of the same
key. 200/day alone could exceed the weekly ceiling (200 x 7 = 1,400 > 1,000),
so a rolling 7-UTC-day total (the sum of the last seven daily keys, one MGET)
is also held under GLOBAL_WEEKLY_CAP (900). A request is charged BEFORE it is
sent (INCR, then refuse + DECR when over a cap), so two workers racing for the
last slot cannot both send.

Fallback counter: when Upstash is not configured or a call to it fails, the
in-process counter below is enforced instead, with PER-WORKER caps that are
deliberately small (DAILY_CAP / MONTHLY_CAP = the 80/day and 2,400/month
service budget split over WORKERS_ASSUMED = 4 workers). render.yaml and the
Procfile start 2 workers, the owner reports the production dashboard runs 4;
the fallback sizing assumes 4, so with 2 it is simply more conservative. The
in-process counter resets when the worker restarts and its windows are fixed
(a window opens at the first request after the previous one closed), so it is
only the degraded mode, never the plan of record.

Never raises into a caller; thread- and gevent-safe (no lock is held across
the network call).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Shared (Upstash) caps: service-wide, across all workers and restarts.
GLOBAL_DAILY_CAP = 200
GLOBAL_MONTHLY_CAP = 2300
GLOBAL_WEEKLY_CAP = 900  # rolling 7 UTC days; ToS 1,000/week
KEY_PREFIX = "nova:quota:adzuna"
_DAILY_KEY_TTL_S = 8 * 86400  # kept 8 days: the weekly sum reads 7 of them
_MONTHLY_KEY_TTL_S = 35 * 86400

# Per-worker fallback caps (see module docstring).
WORKERS_ASSUMED = 4
SERVICE_DAILY_BUDGET = 80
SERVICE_MONTHLY_BUDGET = 2400
DAILY_CAP = SERVICE_DAILY_BUDGET // WORKERS_ASSUMED  # 20
MONTHLY_CAP = SERVICE_MONTHLY_BUDGET // WORKERS_ASSUMED  # 600
DAILY_WINDOW_S = 86400
MONTHLY_WINDOW_S = 30 * 86400

# (name, max_requests, window_seconds). Order is only for reporting.
_WINDOWS = (
    ("daily", DAILY_CAP, DAILY_WINDOW_S),
    ("monthly", MONTHLY_CAP, MONTHLY_WINDOW_S),
)

_lock = threading.Lock()
# window name -> {"count": int, "start": float}
_state: Dict[str, Dict[str, float]] = {}
# True once a shared-counter call failed and the in-process caps are in force.
_degraded = False


def _roll(name: str, window_s: int, now: float) -> Dict[str, float]:
    """Return the live state of one window, opening a fresh one if it expired.
    Caller holds ``_lock``."""
    st = _state.get(name)
    if st is None or (now - st["start"]) >= window_s:
        st = {"count": 0, "start": now}
        _state[name] = st
    return st


def _redis(command: list) -> Any:
    """Run one Upstash REST command; ``None`` when Upstash is not configured,
    unreachable, rate-limited or its circuit breaker is open (upstash_cache
    swallows all of those). Never raises. The test seam for the shared path."""
    try:
        import upstash_cache

        if not getattr(upstash_cache, "_ENABLED", False):
            return None
        return upstash_cache._execute(command)
    except Exception:  # noqa: BLE001 - quota code must never raise into callers
        logger.debug("adzuna_quota: Upstash call failed", exc_info=True)
        return None


def _redis_configured() -> bool:
    """True when the shared (Upstash) counter can be attempted at all."""
    try:
        import upstash_cache

        return bool(getattr(upstash_cache, "_ENABLED", False))
    except Exception:  # noqa: BLE001
        return False


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc_keys(now: float) -> tuple:
    """(daily key, monthly key, [the 7 rolling daily keys, oldest first])."""
    day = time.strftime("%Y%m%d", time.gmtime(now))
    month = time.strftime("%Y%m", time.gmtime(now))
    week = [
        f"{KEY_PREFIX}:d:{time.strftime('%Y%m%d', time.gmtime(now - i * 86400))}"
        for i in range(6, -1, -1)
    ]
    return f"{KEY_PREFIX}:d:{day}", f"{KEY_PREFIX}:m:{month}", week


def _local_charge(now: float) -> bool:
    """Charge the in-process counter; False (nothing counted) when a window is
    full. This is the per-worker fallback."""
    with _lock:
        states = [(_roll(n, w, now), cap) for n, cap, w in _WINDOWS]
        if any(st["count"] >= cap for st, cap in states):
            return False
        for st, _cap in states:
            st["count"] += 1
    return True


def _local_count_only(now: float) -> None:
    """Mirror a shared charge into the in-process counter (diagnostics only)."""
    with _lock:
        for n, _cap, w in _WINDOWS:
            _roll(n, w, now)["count"] += 1


def _local_exhausted(now: float) -> bool:
    with _lock:
        for name, cap, window_s in _WINDOWS:
            if _roll(name, window_s, now)["count"] >= cap:
                return True
    return False


def _refund(*keys: str) -> None:
    for key in keys:
        _redis(["DECR", key])


def _shared_acquire(now: float) -> Optional[bool]:
    """Charge the shared counter. True = charged, False = refused (a global
    cap is full, nothing left charged), None = the shared counter could not be
    used (caller falls back to the per-worker caps)."""
    dkey, mkey, week_keys = _utc_keys(now)
    day = _as_int(_redis(["INCR", dkey]))
    if day is None:
        return None
    if day == 1:
        _redis(["EXPIRE", dkey, str(_DAILY_KEY_TTL_S)])
    if day > GLOBAL_DAILY_CAP:
        _refund(dkey)
        return False
    month = _as_int(_redis(["INCR", mkey]))
    if month is None:
        _refund(dkey)
        return None
    if month == 1:
        _redis(["EXPIRE", mkey, str(_MONTHLY_KEY_TTL_S)])
    if month > GLOBAL_MONTHLY_CAP:
        _refund(dkey, mkey)
        return False
    raw_week = _redis(["MGET"] + week_keys)
    if isinstance(raw_week, list):
        week_total = sum(_as_int(v) or 0 for v in raw_week)
        if week_total > GLOBAL_WEEKLY_CAP:
            _refund(dkey, mkey)
            return False
    return True


def _set_degraded(value: bool) -> None:
    global _degraded
    if value and not _degraded:
        logger.warning(
            "Adzuna quota: shared counter unavailable; enforcing per-worker caps"
        )
    _degraded = value


def exhausted() -> bool:
    """True when no further request is allowed right now (read-only, advisory:
    try_acquire is what actually gates a request)."""
    now = time.time()
    try:
        if _redis_configured():
            dkey, mkey, _week = _utc_keys(now)
            day = _as_int(_redis(["GET", dkey])) or 0
            month = _as_int(_redis(["GET", mkey])) or 0
            if day >= GLOBAL_DAILY_CAP or month >= GLOBAL_MONTHLY_CAP:
                return True
            if not _degraded:
                return False
        return _local_exhausted(now)
    except Exception:  # noqa: BLE001
        logger.debug("adzuna_quota.exhausted failed", exc_info=True)
        return _local_exhausted(now)


def try_acquire() -> bool:
    """Count one Adzuna request. Returns False (and counts nothing) when a cap
    is full -- the caller must then NOT send the request.

    The shared Upstash counter decides when it is reachable; otherwise the
    per-worker in-process caps do. Charged before the send; never raises.
    """
    now = time.time()
    try:
        if _redis_configured():
            shared = _shared_acquire(now)
            if shared is not None:
                _set_degraded(False)
                if shared:
                    _local_count_only(now)
                return shared
            _set_degraded(True)
        return _local_charge(now)
    except Exception:  # noqa: BLE001
        logger.debug("adzuna_quota.try_acquire failed", exc_info=True)
        return _local_charge(now)


def status() -> Dict[str, Any]:
    """Per-window usage, for diagnostics. Reports the shared counters when
    Upstash answers, else this worker's own."""
    now = time.time()
    out: Dict[str, Any] = {}
    shared: Dict[str, Optional[int]] = {}
    if _redis_configured():
        dkey, mkey, _week = _utc_keys(now)
        shared = {
            "daily": _as_int(_redis(["GET", dkey])),
            "monthly": _as_int(_redis(["GET", mkey])),
        }
    shared_caps = {"daily": GLOBAL_DAILY_CAP, "monthly": GLOBAL_MONTHLY_CAP}
    with _lock:
        for name, cap, window_s in _WINDOWS:
            st = _roll(name, window_s, now)
            entry = {
                "calls_used": int(st["count"]),
                "max_calls": cap,
                "window_seconds": window_s,
                "window_remaining_seconds": round(
                    max(0.0, window_s - (now - st["start"])), 1
                ),
                "scope": "worker",
            }
            if shared.get(name) is not None and not _degraded:
                entry["calls_used"] = int(shared[name] or 0)
                entry["max_calls"] = shared_caps[name]
                entry["scope"] = "global"
            out[name] = entry
    return out


def reset() -> None:
    """Forget all in-process usage (tests). Shared keys are left alone."""
    global _degraded
    with _lock:
        _state.clear()
    _degraded = False
