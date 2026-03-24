#!/usr/bin/env python3
"""Scheduled data refresh pipeline for Nova AI Suite.

Refreshes knowledge base data from live APIs on a configurable schedule.
Runs as a background thread, checks every 6 hours by default.
"""

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_REFRESH_INTERVAL = 21600  # 6 hours
_DATA_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "data"


class DataRefreshPipeline:
    """Background pipeline that refreshes KB data from live sources."""

    def __init__(self, interval: int = _REFRESH_INTERVAL):
        """Initialize the data refresh pipeline.

        Args:
            interval: Seconds between refresh cycles (default 6 hours).
        """
        self._interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_refresh: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the background refresh thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="data-refresh"
        )
        self._thread.start()
        logger.info("[DataRefresh] Pipeline started (interval: %ds)", self._interval)

    def stop(self) -> None:
        """Signal the refresh thread to stop."""
        self._running = False

    def _run_loop(self) -> None:
        """Main loop: wait after startup, then refresh periodically."""
        time.sleep(300)  # Wait 5 min after startup before first refresh
        while self._running:
            try:
                self._refresh_all()
            except Exception as e:
                logger.error("[DataRefresh] Cycle error: %s", e, exc_info=True)
            time.sleep(self._interval)

    def _refresh_all(self) -> None:
        """Run all data refresh tasks."""
        self._refresh_market_trends()
        self._refresh_salary_benchmarks()
        self._refresh_channel_benchmarks()

    def _refresh_market_trends(self) -> None:
        """Refresh market trends from live APIs."""
        try:
            import sys

            if "api_enrichment" in sys.modules:
                enrichment = sys.modules["api_enrichment"]
                if hasattr(enrichment, "fetch_fred_indicators"):
                    data = enrichment.fetch_fred_indicators()
                    if data:
                        self._save_refresh("market_trends_live", data)
                        logger.info(
                            "[DataRefresh] Market trends refreshed: %d records",
                            len(data) if isinstance(data, list) else 1,
                        )
        except Exception as e:
            logger.debug("[DataRefresh] Market trends refresh failed: %s", e)

    def _refresh_salary_benchmarks(self) -> None:
        """Refresh salary data from Adzuna/BLS."""
        try:
            import sys

            if "api_enrichment" in sys.modules:
                enrichment = sys.modules["api_enrichment"]
                if hasattr(enrichment, "fetch_salary_data"):
                    data = enrichment.fetch_salary_data()
                    if data:
                        self._save_refresh("salary_benchmarks_live", data)
                        logger.info("[DataRefresh] Salary data refreshed")
        except Exception as e:
            logger.debug("[DataRefresh] Salary refresh failed: %s", e)

    def _refresh_channel_benchmarks(self) -> None:
        """Refresh channel/job board benchmarks."""
        try:
            from supabase_data import get_channel_benchmarks

            data = get_channel_benchmarks()
            if data:
                self._save_refresh("channel_benchmarks_live", data)
                logger.info("[DataRefresh] Channel benchmarks refreshed")
        except ImportError:
            logger.debug(
                "[DataRefresh] supabase_data not available for channel benchmarks"
            )
        except Exception as e:
            logger.debug("[DataRefresh] Channel benchmarks refresh failed: %s", e)

    def _save_refresh(self, key: str, data: Any) -> None:
        """Save refreshed data with timestamp.

        Args:
            key: Identifier for the refreshed dataset.
            data: The refreshed data to persist.
        """
        with self._lock:
            self._last_refresh[key] = {
                "ts": time.time(),
                "record_count": len(data) if isinstance(data, (list, dict)) else 1,
            }
        # Persist to data/ directory
        try:
            filepath = _DATA_DIR / f"{key}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({"data": data, "_refreshed_at": time.time()}, f)
        except Exception as e:
            logger.debug("[DataRefresh] Save failed for %s: %s", key, e)

    def get_status(self) -> dict[str, Any]:
        """Return current pipeline status.

        Returns:
            Dict with running state, interval, and last refresh timestamps.
        """
        with self._lock:
            return {
                "running": self._running,
                "interval_s": self._interval,
                "last_refreshes": dict(self._last_refresh),
            }


_pipeline: Optional[DataRefreshPipeline] = None


def start_data_refresh() -> None:
    """Start the global data refresh pipeline (idempotent)."""
    global _pipeline
    if _pipeline is None:
        _pipeline = DataRefreshPipeline()
    _pipeline.start()


def get_refresh_status() -> dict[str, Any]:
    """Get current status of the data refresh pipeline.

    Returns:
        Dict with running state and last refresh info.
    """
    if _pipeline:
        return _pipeline.get_status()
    return {"running": False}
