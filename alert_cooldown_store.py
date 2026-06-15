"""Shared, fail-open cooldown store for the monitoring alert bridge (S90 P1).

The alert bridge dedups repeated pages with a per-key cooldown. That state was
in-memory per process (``MonitoringAlertBridge._alert_cooldowns``), so it was
wiped on every gunicorn worker restart (deploy, OOM, ``--max-requests`` recycle)
AND not shared across workers -- so a deploy storm, or simply running >1 worker,
could re-page the same alert. (The 2026-06-13 incident showed two identical
CRITICALs two minutes apart; see ``docs/INCIDENT_2026-06-13_alert_noise.md``.)

This store backs the cooldown with Supabase -- shared across workers and durable
across restarts -- when ``SUPABASE_URL`` / ``SUPABASE_SERVICE_ROLE_KEY`` are set,
and otherwise falls back to the exact pre-P1 in-memory behavior.

**FAIL-OPEN.** Any backend error is treated as "no cooldown -> fire the alert".
A transient store outage must never silently swallow a real page; a duplicate
page costs far less than a missed one.

Table (see ``docs/sql/alert_cooldowns.sql``)::

    alert_cooldowns(alert_key text primary key,
                    last_fired_ts double precision not null,
                    updated_at timestamptz default now())

Note: ``should_fire`` is read-then-write, which is not perfectly atomic across
workers -- a sub-second race between two workers' check cycles could still
double-fire. Check cycles run every 60s and are not synchronized, so this is
rare; strict atomicity would need a conditional-upsert RPC (documented as a
further follow-up). This still removes the dominant duplicate source: cooldown
loss on restart.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional, Protocol

logger = logging.getLogger(__name__)


class CooldownBackend(Protocol):
    """Storage backend for alert cooldown timestamps."""

    def get_last_fired(self, key: str) -> Optional[float]: ...
    def record_fired(self, key: str, ts: float) -> None: ...
    def active_count(self) -> int: ...

    @property
    def name(self) -> str: ...


class InMemoryCooldownBackend:
    """Process-local cooldown state -- the exact pre-P1 behavior."""

    def __init__(self) -> None:
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def get_last_fired(self, key: str) -> Optional[float]:
        with self._lock:
            return self._last.get(key)

    def record_fired(self, key: str, ts: float) -> None:
        with self._lock:
            self._last[key] = ts

    def active_count(self) -> int:
        with self._lock:
            return len(self._last)

    @property
    def name(self) -> str:
        return "memory"


class SupabaseCooldownBackend:
    """Cooldown state shared across workers/restarts via Supabase REST.

    Mirrors ``monitoring.SupabasePersistence``'s HTTP pattern (same env vars,
    headers, upsert semantics). Every network path is fail-open: read errors
    return ``None`` (-> allow the alert), write errors are swallowed.
    """

    _TABLE = "alert_cooldowns"
    _TIMEOUT = 5.0

    def __init__(self) -> None:
        self._base_url = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
        self._api_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
        self.enabled = bool(self._base_url and self._api_key)

    def _url(self) -> str:
        return f"{self._base_url}/rest/v1/{self._TABLE}"

    def _headers(self) -> dict:
        return {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }

    def get_last_fired(self, key: str) -> Optional[float]:
        if not self.enabled:
            return None
        try:
            q = urllib.parse.quote(key, safe="")
            url = f"{self._url()}?alert_key=eq.{q}&select=last_fired_ts"
            req = urllib.request.Request(url, headers=self._headers(), method="GET")
            with urllib.request.urlopen(req, timeout=self._TIMEOUT) as resp:
                rows = json.loads(resp.read().decode("utf-8"))
            if isinstance(rows, list) and rows and isinstance(
                rows[0].get("last_fired_ts"), (int, float)
            ):
                last = float(rows[0]["last_fired_ts"])
                # Defense-in-depth: a non-positive or far-future timestamp is
                # corruption (a future value would otherwise SUPPRESS the alert).
                # Reject it -> caller treats as "no cooldown" -> fail-open (fire).
                if last <= 0 or last > time.time() + 60:
                    logger.warning(
                        "[cooldown] ignoring implausible last_fired_ts=%r for %s "
                        "(fail-open)",
                        last,
                        key,
                    )
                    return None
                return last
            return None
        except Exception as e:  # fail-open: any error -> allow the alert
            logger.debug("[cooldown] Supabase read failed (fail-open): %s", e)
            return None

    def record_fired(self, key: str, ts: float) -> None:
        if not self.enabled:
            return
        try:
            payload = json.dumps([{"alert_key": key, "last_fired_ts": ts}])
            req = urllib.request.Request(
                self._url(),
                data=payload.encode("utf-8"),
                headers=self._headers(),
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self._TIMEOUT) as resp:
                resp.read()
        except Exception as e:
            logger.debug("[cooldown] Supabase write failed: %s", e)

    def active_count(self) -> int:
        return -1  # shared store; not locally knowable

    @property
    def name(self) -> str:
        return "supabase"


class AlertCooldownStore:
    """Decides ``should_fire(key, cooldown_s)`` against a shared, fail-open backend."""

    def __init__(self, backend: Optional[CooldownBackend] = None) -> None:
        self._backend: CooldownBackend = backend if backend is not None else _default_backend()

    def should_fire(self, key: str, cooldown_s: float, now: Optional[float] = None) -> bool:
        """Return True if ``key`` is allowed to fire now (and record the fire).

        Fail-open at every step: a backend error, a missing entry, OR a corrupt
        timestamp all resolve to "fire". No lock is held: the backends are
        individually thread-safe and the bridge is the sole caller, so the only
        thing a lock would add is serializing network I/O (which would stall the
        bridge cycle). The cross-worker read-then-write race is inherent and
        documented in the module header.
        """
        ts = time.time() if now is None else now
        try:
            last = self._backend.get_last_fired(key)
        except Exception as e:  # defensive: backend should already fail-open
            logger.debug("[cooldown] get_last_fired raised (fail-open): %s", e)
            last = None
        # Suppress ONLY for a genuine, recent prior fire. A future timestamp
        # (clock skew / corruption) yields a negative age -> we must NOT treat
        # that as "still cooling down", or a real alert would be silenced.
        if last is not None and 0 <= (ts - last) < cooldown_s:
            return False
        try:
            self._backend.record_fired(key, ts)
        except Exception as e:
            logger.debug("[cooldown] record_fired raised: %s", e)
        return True

    def active_count(self) -> int:
        try:
            return self._backend.active_count()
        except Exception:
            return -1

    @property
    def backend_name(self) -> str:
        return self._backend.name


def _default_backend() -> CooldownBackend:
    sb = SupabaseCooldownBackend()
    if sb.enabled:
        logger.info("[cooldown] alert cooldowns backed by Supabase (shared/durable)")
        return sb
    return InMemoryCooldownBackend()
