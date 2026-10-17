"""Process-wide request quota for the shared Adzuna API key.

Every Adzuna HTTP call in this process -- api_enrichment.fetch_job_market,
the api_integrations.AdzunaClient (the sync /api/generate path's
``_call_adzuna``, data_orchestrator, data_refresh), market_signals'
``_adzuna_live_cpc`` and the data_matrix_monitor health probe -- uses ONE
key, so they all draw from ONE counter here.

Provider terms (per key): 250 requests/day, 1,000/week, 2,500/month. The
service-wide budget this module is sized for is 80/day and 2,400/month
(about a third of the daily and just under the monthly ceiling, leaving room
for manual use of the same key).

The counter is PER PROCESS. Production runs 4 gunicorn workers, each with
its own counter, so the worst case is 4x one worker's cap. The per-worker
caps below are therefore the service-wide budget divided by 4:

    20/day   x 4 workers = 80/day   (<= 250/day ToS)
    600/month x 4 workers = 2,400/month (<= 2,500/month ToS)

The weekly ToS ceiling (1,000) cannot be reached: 80/day x 7 = 560.

Limits of an in-memory counter, deliberately accepted: it resets when the
process restarts (a deploy), so a restart-heavy day can exceed the daily cap
(never the provider's 250/day while restarts stay under ~3 per day per
worker); windows are fixed (a window opens at the first request after the
previous one closed), not calendar-aligned.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict

# Per-worker caps (see module docstring for the 4-worker arithmetic).
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


def _roll(name: str, window_s: int, now: float) -> Dict[str, float]:
    """Return the live state of one window, opening a fresh one if it expired.
    Caller holds ``_lock``."""
    st = _state.get(name)
    if st is None or (now - st["start"]) >= window_s:
        st = {"count": 0, "start": now}
        _state[name] = st
    return st


def exhausted() -> bool:
    """True when no further request is allowed right now (read-only)."""
    now = time.time()
    with _lock:
        for name, cap, window_s in _WINDOWS:
            if _roll(name, window_s, now)["count"] >= cap:
                return True
    return False


def try_acquire() -> bool:
    """Count one Adzuna request. Returns False (and counts nothing) when any
    window is full -- the caller must then NOT send the request.

    All windows are checked before any is charged, so a refusal never burns
    quota in the other window.
    """
    now = time.time()
    with _lock:
        states = [(_roll(n, w, now), cap) for n, cap, w in _WINDOWS]
        if any(st["count"] >= cap for st, cap in states):
            return False
        for st, _cap in states:
            st["count"] += 1
    return True


def status() -> Dict[str, Any]:
    """Per-window usage, for diagnostics."""
    now = time.time()
    out: Dict[str, Any] = {}
    with _lock:
        for name, cap, window_s in _WINDOWS:
            st = _roll(name, window_s, now)
            out[name] = {
                "calls_used": int(st["count"]),
                "max_calls": cap,
                "window_seconds": window_s,
                "window_remaining_seconds": round(
                    max(0.0, window_s - (now - st["start"])), 1
                ),
            }
    return out


def reset() -> None:
    """Forget all usage (tests)."""
    with _lock:
        _state.clear()
