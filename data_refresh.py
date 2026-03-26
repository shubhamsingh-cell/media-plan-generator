#!/usr/bin/env python3
"""Weekly data refresh pipeline for Nova AI Suite.

Refreshes job market data from live APIs (BLS, Adzuna, FRED, Google Trends)
on a configurable schedule (default: 7 days).  Runs as a background daemon
thread.  Each data source is isolated: a failure in one source does not
block the others.

Rate limiting: max 1 API call per 2 seconds across all sources.
Priority order: BLS (most stable) -> Adzuna -> FRED -> Google Trends.

Persistence:
  - Writes JSON files to data/ directory (consumed by benchmark_registry)
  - Stores refresh timestamp in Supabase ``cache`` table
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_REFRESH_INTERVAL = 7 * 24 * 3600  # 7 days in seconds
_STARTUP_DELAY = 1800  # 30 minutes after boot before first refresh
_API_CALL_DELAY = 2.0  # seconds between individual API calls (rate limit)
_DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "data"

# Top roles used for salary + job-count enrichment
_BENCHMARK_ROLES: list[str] = [
    "Software Engineer",
    "Registered Nurse",
    "Marketing Manager",
    "Sales Representative",
    "Data Analyst",
    "Warehouse Associate",
    "Truck Driver",
    "Financial Analyst",
    "Project Manager",
    "Customer Service Representative",
]

# Top cities for Adzuna job-count snapshots
_BENCHMARK_CITIES: list[str] = [
    "us",  # national
    "gb",  # UK
    "ca",  # Canada
]


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------


class DataRefreshPipeline:
    """Background pipeline that refreshes job-market data weekly."""

    def __init__(self, interval: int = _REFRESH_INTERVAL) -> None:
        """Initialize the data refresh pipeline.

        Args:
            interval: Seconds between refresh cycles (default 7 days).
        """
        self._interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_refresh: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._cycle_count = 0

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> None:
        """Start the background refresh thread (idempotent)."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="data-refresh-weekly"
        )
        self._thread.start()
        logger.info(
            "[DataRefresh] Weekly pipeline started (interval: %ds / %.1f days)",
            self._interval,
            self._interval / 86400,
        )

    def stop(self) -> None:
        """Signal the refresh thread to stop."""
        self._running = False

    # -- main loop -----------------------------------------------------------

    def _run_loop(self) -> None:
        """Main loop: wait after startup, then refresh on schedule."""
        time.sleep(_STARTUP_DELAY)
        while self._running:
            cycle_start = time.monotonic()
            self._cycle_count += 1
            logger.info("[DataRefresh] Starting refresh cycle #%d", self._cycle_count)
            try:
                self._refresh_all()
            except Exception as exc:
                logger.error(
                    "[DataRefresh] Cycle #%d error: %s",
                    self._cycle_count,
                    exc,
                    exc_info=True,
                )
            elapsed = time.monotonic() - cycle_start
            logger.info(
                "[DataRefresh] Cycle #%d complete in %.1fs",
                self._cycle_count,
                elapsed,
            )
            self._persist_timestamp_to_supabase()
            time.sleep(self._interval)

    # -- orchestrator --------------------------------------------------------

    def _refresh_all(self) -> None:
        """Run all data refresh tasks in priority order.

        Priority: BLS -> Adzuna -> FRED -> Google Trends.
        Each source is wrapped in its own try/except so failures are isolated.
        """
        self._refresh_bls_salary()
        self._rate_limit_pause()

        self._refresh_adzuna_benchmarks()
        self._rate_limit_pause()

        self._refresh_fred_indicators()
        self._rate_limit_pause()

        self._refresh_google_trends()
        self._rate_limit_pause()

        self._refresh_channel_benchmarks()

    # -- source: BLS ---------------------------------------------------------

    def _refresh_bls_salary(self) -> None:
        """Refresh salary benchmarks from BLS via fetch_salary_data."""
        source = "bls_salary"
        try:
            enrichment = self._get_enrichment_module()
            if enrichment is None:
                return
            fetch_fn = getattr(enrichment, "fetch_salary_data", None)
            if fetch_fn is None:
                logger.debug("[DataRefresh] fetch_salary_data not found")
                return

            data = fetch_fn(_BENCHMARK_ROLES)
            if data:
                self._save_refresh(source, data)
                logger.info(
                    "[DataRefresh] BLS salary refreshed: %d/%d roles",
                    len(data),
                    len(_BENCHMARK_ROLES),
                )
            else:
                logger.warning("[DataRefresh] BLS salary returned empty data")
                self._record_failure(source, "empty response")
        except Exception as exc:
            logger.warning("[DataRefresh] BLS salary refresh failed: %s", exc)
            self._record_failure(source, str(exc))

    # -- source: Adzuna ------------------------------------------------------

    def _refresh_adzuna_benchmarks(self) -> None:
        """Refresh job counts and salary histograms from Adzuna."""
        source = "adzuna_benchmarks"
        try:
            integrations = self._get_integrations_module()
            if integrations is None:
                return
            adzuna_cls = getattr(integrations, "AdzunaClient", None)
            if adzuna_cls is None:
                logger.debug("[DataRefresh] AdzunaClient not found")
                return

            client = adzuna_cls()
            if not client._is_configured():
                logger.debug("[DataRefresh] Adzuna credentials not configured")
                return

            result: dict[str, Any] = {"roles": {}, "refreshed_at": _utc_iso()}
            for role in _BENCHMARK_ROLES:
                role_data: dict[str, Any] = {}

                # Job count per country
                for country in _BENCHMARK_CITIES:
                    self._rate_limit_pause()
                    try:
                        count_data = client.get_job_count(role, country)
                        if count_data:
                            role_data[f"count_{country}"] = count_data
                    except Exception as exc:
                        logger.debug(
                            "[DataRefresh] Adzuna count %s/%s failed: %s",
                            role,
                            country,
                            exc,
                        )

                # Salary histogram (US only)
                self._rate_limit_pause()
                try:
                    hist = client.get_salary_histogram(role, "us")
                    if hist:
                        role_data["salary_histogram_us"] = hist
                except Exception as exc:
                    logger.debug(
                        "[DataRefresh] Adzuna histogram %s failed: %s", role, exc
                    )

                if role_data:
                    result["roles"][role] = role_data

            if result["roles"]:
                self._save_refresh(source, result)
                logger.info(
                    "[DataRefresh] Adzuna benchmarks refreshed: %d roles",
                    len(result["roles"]),
                )
            else:
                self._record_failure(source, "no role data returned")
        except Exception as exc:
            logger.warning("[DataRefresh] Adzuna refresh failed: %s", exc)
            self._record_failure(source, str(exc))

    # -- source: FRED --------------------------------------------------------

    def _refresh_fred_indicators(self) -> None:
        """Refresh economic indicators from FRED."""
        source = "fred_indicators"
        try:
            enrichment = self._get_enrichment_module()
            if enrichment is None:
                return
            fetch_fn = getattr(enrichment, "fetch_fred_indicators", None)
            if fetch_fn is None:
                logger.debug("[DataRefresh] fetch_fred_indicators not found")
                return

            data = fetch_fn()
            if data:
                self._save_refresh(source, data)
                # Also write to the existing market_trends_live.json path
                self._save_refresh("market_trends_live", data)
                logger.info(
                    "[DataRefresh] FRED indicators refreshed: %d series",
                    len(data) - 1,  # minus the "source" key
                )
            else:
                logger.warning("[DataRefresh] FRED returned empty data")
                self._record_failure(source, "empty response")
        except Exception as exc:
            logger.warning("[DataRefresh] FRED refresh failed: %s", exc)
            self._record_failure(source, str(exc))

    # -- source: Google Trends -----------------------------------------------

    def _refresh_google_trends(self) -> None:
        """Refresh search interest data from Google Trends."""
        source = "google_trends"
        try:
            enrichment = self._get_enrichment_module()
            if enrichment is None:
                return
            fetch_fn = getattr(enrichment, "fetch_google_trends", None)
            if fetch_fn is None:
                logger.debug("[DataRefresh] fetch_google_trends not found")
                return

            result: dict[str, Any] = {"roles": {}, "refreshed_at": _utc_iso()}
            for role in _BENCHMARK_ROLES[:5]:  # limit to 5 to avoid rate-limit
                self._rate_limit_pause()
                try:
                    keyword = f"{role} jobs"
                    trend_data = fetch_fn(keyword, "today 3-m")
                    if trend_data:
                        result["roles"][role] = trend_data
                except Exception as exc:
                    logger.debug("[DataRefresh] Google Trends %s failed: %s", role, exc)

            if result["roles"]:
                self._save_refresh(source, result)
                logger.info(
                    "[DataRefresh] Google Trends refreshed: %d roles",
                    len(result["roles"]),
                )
            else:
                self._record_failure(source, "no trend data returned")
        except Exception as exc:
            logger.warning("[DataRefresh] Google Trends refresh failed: %s", exc)
            self._record_failure(source, str(exc))

    # -- source: channel benchmarks ------------------------------------------

    def _refresh_channel_benchmarks(self) -> None:
        """Refresh channel/job board benchmarks from Supabase."""
        source = "channel_benchmarks"
        try:
            from supabase_data import get_channel_benchmarks

            data = get_channel_benchmarks()
            if data:
                self._save_refresh("channel_benchmarks_live", data)
                logger.info("[DataRefresh] Channel benchmarks refreshed")
            else:
                self._record_failure(source, "empty response")
        except ImportError:
            logger.debug(
                "[DataRefresh] supabase_data not available for channel benchmarks"
            )
        except Exception as exc:
            logger.warning("[DataRefresh] Channel benchmarks failed: %s", exc)
            self._record_failure(source, str(exc))

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _get_enrichment_module() -> Any:
        """Get the api_enrichment module if loaded."""
        mod = sys.modules.get("api_enrichment")
        if mod is None:
            try:
                import api_enrichment as mod  # type: ignore[no-redef]
            except ImportError:
                logger.debug("[DataRefresh] api_enrichment not importable")
                return None
        return mod

    @staticmethod
    def _get_integrations_module() -> Any:
        """Get the api_integrations module if loaded."""
        mod = sys.modules.get("api_integrations")
        if mod is None:
            try:
                import api_integrations as mod  # type: ignore[no-redef]
            except ImportError:
                logger.debug("[DataRefresh] api_integrations not importable")
                return None
        return mod

    @staticmethod
    def _rate_limit_pause() -> None:
        """Sleep to respect rate limits between API calls."""
        time.sleep(_API_CALL_DELAY)

    def _save_refresh(self, key: str, data: Any) -> None:
        """Save refreshed data to disk and update in-memory status.

        Args:
            key: Identifier for the refreshed dataset.
            data: The refreshed data to persist.
        """
        now = time.time()
        record_count = len(data) if isinstance(data, (list, dict)) else 1
        with self._lock:
            self._last_refresh[key] = {
                "ts": now,
                "iso": _utc_iso(),
                "record_count": record_count,
                "success": True,
            }
        # Persist to data/ directory
        try:
            filepath = _DATA_DIR / f"{key}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(
                    {"data": data, "_refreshed_at": now, "_refreshed_iso": _utc_iso()},
                    f,
                    indent=2,
                    default=str,
                )
            logger.debug("[DataRefresh] Saved %s (%d records)", key, record_count)
        except Exception as exc:
            logger.warning("[DataRefresh] Save failed for %s: %s", key, exc)

    def _record_failure(self, key: str, reason: str) -> None:
        """Record a refresh failure in the status map.

        Args:
            key: Source identifier.
            reason: Human-readable failure reason.
        """
        with self._lock:
            self._last_refresh[key] = {
                "ts": time.time(),
                "iso": _utc_iso(),
                "success": False,
                "error": reason,
            }

    def _persist_timestamp_to_supabase(self) -> None:
        """Write refresh timestamp to Supabase cache table."""
        try:
            from supabase_data import _cache_set as supa_set

            supa_set(
                "data_refresh_timestamp",
                {
                    "last_refresh": _utc_iso(),
                    "cycle": self._cycle_count,
                    "interval_days": self._interval / 86400,
                },
            )
            logger.debug("[DataRefresh] Persisted timestamp to Supabase cache")
        except ImportError:
            logger.debug("[DataRefresh] supabase_data not available for timestamp")
        except Exception as exc:
            logger.debug("[DataRefresh] Supabase timestamp write failed: %s", exc)

    def get_status(self) -> dict[str, Any]:
        """Return current pipeline status.

        Returns:
            Dict with running state, interval, cycle count, next refresh
            estimate, and per-source last-refresh timestamps.
        """
        with self._lock:
            refreshes = dict(self._last_refresh)

        # Calculate next refresh estimate
        latest_ts = max((r.get("ts", 0) for r in refreshes.values()), default=0)
        if latest_ts > 0:
            next_refresh_ts = latest_ts + self._interval
            next_refresh_iso = datetime.fromtimestamp(
                next_refresh_ts, tz=timezone.utc
            ).isoformat()
        else:
            next_refresh_iso = "pending (first cycle not yet run)"

        return {
            "running": self._running,
            "interval_seconds": self._interval,
            "interval_days": round(self._interval / 86400, 1),
            "cycle_count": self._cycle_count,
            "next_refresh": next_refresh_iso,
            "sources": refreshes,
        }


# ---------------------------------------------------------------------------
# Module-level singleton + public API
# ---------------------------------------------------------------------------

_pipeline: Optional[DataRefreshPipeline] = None


def start_data_refresh() -> None:
    """Start the global weekly data refresh pipeline (idempotent)."""
    global _pipeline
    if _pipeline is None:
        _pipeline = DataRefreshPipeline()
    _pipeline.start()


def get_refresh_status() -> dict[str, Any]:
    """Get current status of the data refresh pipeline.

    Returns:
        Dict with running state, schedule info, and per-source statuses.
    """
    if _pipeline:
        return _pipeline.get_status()
    return {"running": False, "message": "Pipeline not initialized"}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _utc_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()
