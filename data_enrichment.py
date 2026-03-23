"""Data Enrichment Engine for Nova AI Suite.

Third pillar of self-maintaining infrastructure:
- auto_qc.py: "Is the CODE working?"
- data_matrix_monitor.py: "Can products ACCESS data?"
- data_enrichment.py: "Is the DATA fresh?"

Runs as a background daemon thread, checking data freshness hourly
and refreshing stale sources automatically.

Schedule:
    - Hourly: Check if any enrichment task is due
    - Daily: Live market data (job board pricing via Firecrawl)
    - Twice daily: Recruitment news via Firecrawl
    - Weekly: BLS salary data, FRED economic indicators, Adzuna job volumes
    - Monthly: Census demographics, compliance law updates

Data flow:
    1. Hourly tick checks each source against its freshness threshold
    2. Stale sources trigger their enrichment function
    3. Results are written to data/ JSON files
    4. State (last_runs, stats) persisted to data/enrichment_state.json
    5. Status exposed via GET /api/health/enrichment (admin-protected)

Dependencies: stdlib + existing project modules (api_enrichment, firecrawl_enrichment).
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Scheduling ──────────────────────────────────────────────────────────────
ENRICHMENT_INTERVAL = 3600  # Check every hour
_INITIAL_DELAY = 300  # 5 minutes after startup (let services warm up)
_MAX_LOG_ENTRIES = 100  # Keep last N enrichment log entries

# ── Paths ───────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent / "data"
ENRICHMENT_STATE_FILE = DATA_DIR / "enrichment_state.json"

# ── Freshness thresholds (hours) ────────────────────────────────────────────
# Each source is considered stale after this many hours without a refresh.
FRESHNESS_THRESHOLDS: dict[str, int] = {
    "live_market_data": 24,  # Daily -- job board pricing via Firecrawl
    "firecrawl_news": 12,  # Twice daily -- recruitment industry news
    "bls_salary": 168,  # Weekly (7 days) -- BLS salary benchmarks
    "fred_economic": 168,  # Weekly -- FRED economic indicators
    "adzuna_jobs": 168,  # Weekly -- Adzuna job volume data
    "census_demographics": 720,  # Monthly (30 days)
    "compliance_updates": 720,  # Monthly -- regulatory changes
}

# ── Top roles for salary enrichment ─────────────────────────────────────────
_TOP_SALARY_ROLES: list[str] = [
    "Software Engineer",
    "Registered Nurse",
    "Marketing Manager",
    "Sales Representative",
    "Data Analyst",
    "Product Manager",
    "Truck Driver",
    "Warehouse Associate",
    "Customer Service Rep",
    "Accountant",
]

# ── News topics for Firecrawl enrichment ────────────────────────────────────
_NEWS_TOPICS: list[str] = [
    "recruitment advertising",
    "talent acquisition technology",
    "hiring trends",
]

# ── Job boards for pricing enrichment ───────────────────────────────────────
_JOB_BOARDS: list[str] = [
    "indeed",
    "linkedin",
    "ziprecruiter",
    "glassdoor",
    "monster",
]


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE
# ═══════════════════════════════════════════════════════════════════════════════


class DataEnrichmentEngine:
    """Scheduled data enrichment engine.

    Checks data freshness hourly and refreshes stale sources automatically.
    Thread-safe: all shared state is protected by a lock.
    """

    def __init__(self) -> None:
        """Initialize the enrichment engine."""
        self._lock = threading.Lock()
        self._state: dict = self._load_state()
        self._running = False
        self._timer: Optional[threading.Timer] = None
        self._enrichment_log: list[dict] = []

    # ── State persistence ───────────────────────────────────────────────────

    def _load_state(self) -> dict:
        """Load enrichment state from disk."""
        try:
            if ENRICHMENT_STATE_FILE.exists():
                raw = ENRICHMENT_STATE_FILE.read_text(encoding="utf-8")
                return json.loads(raw)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to load enrichment state: %s", e, exc_info=True)
        return {
            "last_runs": {},
            "stats": {"total_enrichments": 0, "total_failures": 0},
        }

    def _save_state(self) -> None:
        """Persist enrichment state to disk."""
        try:
            ENRICHMENT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            ENRICHMENT_STATE_FILE.write_text(
                json.dumps(self._state, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError as e:
            logger.error("Failed to save enrichment state: %s", e, exc_info=True)

    # ── Freshness checks ────────────────────────────────────────────────────

    def _is_stale(self, source: str) -> bool:
        """Check if a data source needs refreshing.

        Args:
            source: Key from FRESHNESS_THRESHOLDS.

        Returns:
            True if the source has never been refreshed or its last refresh
            exceeds the configured threshold.
        """
        threshold_hours = FRESHNESS_THRESHOLDS.get(source, 24)
        last_run = self._state.get("last_runs", {}).get(source)
        if not last_run:
            return True
        try:
            last_dt = datetime.fromisoformat(last_run)
            # Ensure timezone-aware comparison
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - last_dt > timedelta(
                hours=threshold_hours
            )
        except (ValueError, TypeError):
            return True

    def _mark_refreshed(
        self, source: str, success: bool = True, records: int = 0
    ) -> None:
        """Mark a source as refreshed and update stats.

        Args:
            source: Data source key.
            success: Whether the refresh succeeded.
            records: Number of records enriched (for logging).
        """
        with self._lock:
            if "last_runs" not in self._state:
                self._state["last_runs"] = {}
            if "stats" not in self._state:
                self._state["stats"] = {
                    "total_enrichments": 0,
                    "total_failures": 0,
                }

            self._state["last_runs"][source] = datetime.now(timezone.utc).isoformat()

            if success:
                self._state["stats"]["total_enrichments"] = (
                    self._state["stats"].get("total_enrichments") or 0
                ) + 1
            else:
                self._state["stats"]["total_failures"] = (
                    self._state["stats"].get("total_failures") or 0
                ) + 1

            self._save_state()

            self._enrichment_log.append(
                {
                    "source": source,
                    "success": success,
                    "records": records,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            # Trim log to prevent unbounded growth
            if len(self._enrichment_log) > _MAX_LOG_ENTRIES:
                self._enrichment_log = self._enrichment_log[-_MAX_LOG_ENTRIES:]

    # ── Individual enrichment tasks ─────────────────────────────────────────

    def _enrich_live_market_data(self) -> None:
        """Refresh live market data -- job board pricing via Firecrawl."""
        if not self._is_stale("live_market_data"):
            return
        try:
            from firecrawl_enrichment import scrape_job_board_pricing

            results: dict = {}
            for board in _JOB_BOARDS:
                try:
                    data = scrape_job_board_pricing(board)
                    if data and data.get("cpc_range"):
                        results[board] = data
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Firecrawl pricing scrape failed for %s: %s",
                        board,
                        e,
                        exc_info=True,
                    )

            if results:
                live_path = DATA_DIR / "live_market_data.json"
                existing: dict = {}
                try:
                    if live_path.exists():
                        existing = json.loads(live_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    pass

                existing["job_boards"] = {
                    **existing.get("job_boards", {}),
                    **results,
                }
                existing["scraped_at"] = datetime.now(timezone.utc).isoformat()

                live_path.write_text(
                    json.dumps(existing, indent=2, default=str),
                    encoding="utf-8",
                )

                self._mark_refreshed("live_market_data", True, len(results))
                logger.info("Enriched live market data: %d boards", len(results))
            else:
                self._mark_refreshed("live_market_data", False)

        except ImportError:
            logger.warning("firecrawl_enrichment not available for live market data")
            self._mark_refreshed("live_market_data", False)

    def _enrich_firecrawl_news(self) -> None:
        """Refresh recruitment industry news via Firecrawl."""
        if not self._is_stale("firecrawl_news"):
            return
        try:
            from firecrawl_enrichment import fetch_recruitment_news

            all_news: list[dict] = []
            for topic in _NEWS_TOPICS:
                try:
                    news = fetch_recruitment_news(topic)
                    if news:
                        all_news.extend(news)
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "News fetch failed for '%s': %s", topic, e, exc_info=True
                    )

            if all_news:
                trends_path = DATA_DIR / "market_trends_live.json"
                trends_path.write_text(
                    json.dumps(
                        {
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                            "articles": all_news[:20],  # Keep top 20
                        },
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )
                self._mark_refreshed("firecrawl_news", True, len(all_news))
                logger.info(
                    "Enriched news: %d articles across %d topics",
                    len(all_news),
                    len(_NEWS_TOPICS),
                )
            else:
                self._mark_refreshed("firecrawl_news", False)

        except ImportError:
            logger.warning("firecrawl_enrichment not available for news")
            self._mark_refreshed("firecrawl_news", False)

    def _enrich_bls_salary(self) -> None:
        """Refresh BLS salary data for top roles."""
        if not self._is_stale("bls_salary"):
            return
        try:
            from api_enrichment import fetch_salary_data

            # fetch_salary_data accepts List[str] and returns Dict[str, Any]
            data = fetch_salary_data(_TOP_SALARY_ROLES)
            refreshed = len(data) if data else 0

            self._mark_refreshed("bls_salary", refreshed > 0, refreshed)
            logger.info(
                "Enriched BLS salary data: %d/%d roles refreshed",
                refreshed,
                len(_TOP_SALARY_ROLES),
            )

        except ImportError:
            logger.warning("api_enrichment not available for BLS salary data")
            self._mark_refreshed("bls_salary", False)
        except (ValueError, KeyError, TypeError, OSError) as e:
            logger.error("BLS salary enrichment failed: %s", e, exc_info=True)
            self._mark_refreshed("bls_salary", False)

    def _enrich_fred_economic(self) -> None:
        """Refresh FRED economic indicators."""
        if not self._is_stale("fred_economic"):
            return
        try:
            from api_enrichment import fetch_fred_indicators

            data = fetch_fred_indicators()
            if data:
                self._mark_refreshed("fred_economic", True, len(data))
                logger.info("Enriched FRED economic indicators: %d series", len(data))
            else:
                self._mark_refreshed("fred_economic", False)

        except ImportError:
            logger.warning("api_enrichment.fetch_fred_indicators not available")
            self._mark_refreshed("fred_economic", False)
        except (ValueError, KeyError, TypeError, OSError) as e:
            logger.error("FRED enrichment failed: %s", e, exc_info=True)
            self._mark_refreshed("fred_economic", False)

    def _enrich_adzuna_jobs(self) -> None:
        """Refresh Adzuna job volume data."""
        if not self._is_stale("adzuna_jobs"):
            return
        try:
            from api_enrichment import fetch_adzuna_data

            # Fetch for a representative set of categories
            categories = [
                "engineering",
                "healthcare",
                "sales",
                "marketing",
                "logistics",
            ]
            total_records = 0
            for category in categories:
                try:
                    data = fetch_adzuna_data(category)
                    if data:
                        total_records += 1
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Adzuna fetch failed for '%s': %s",
                        category,
                        e,
                        exc_info=True,
                    )

            self._mark_refreshed("adzuna_jobs", total_records > 0, total_records)
            logger.info(
                "Enriched Adzuna job data: %d/%d categories",
                total_records,
                len(categories),
            )

        except (ImportError, AttributeError):
            logger.warning("api_enrichment.fetch_adzuna_data not available")
            self._mark_refreshed("adzuna_jobs", False)

    def _enrich_census_demographics(self) -> None:
        """Refresh Census demographic data (monthly)."""
        if not self._is_stale("census_demographics"):
            return
        try:
            from api_enrichment import fetch_census_data

            data = fetch_census_data()
            if data:
                self._mark_refreshed("census_demographics", True, len(data))
                logger.info("Enriched Census demographics: %d records", len(data))
            else:
                self._mark_refreshed("census_demographics", False)

        except (ImportError, AttributeError):
            logger.warning("api_enrichment.fetch_census_data not available")
            self._mark_refreshed("census_demographics", False)
        except (ValueError, KeyError, TypeError, OSError) as e:
            logger.error("Census enrichment failed: %s", e, exc_info=True)
            self._mark_refreshed("census_demographics", False)

    def _enrich_compliance_updates(self) -> None:
        """Check for compliance/regulatory updates (monthly)."""
        if not self._is_stale("compliance_updates"):
            return
        try:
            from firecrawl_enrichment import fetch_recruitment_news

            compliance_topics = [
                "recruitment compliance regulations",
                "hiring law changes",
                "employment advertising compliance",
            ]
            all_updates: list[dict] = []
            for topic in compliance_topics:
                try:
                    updates = fetch_recruitment_news(topic)
                    if updates:
                        all_updates.extend(updates)
                except (ValueError, KeyError, TypeError, OSError) as e:
                    logger.error(
                        "Compliance fetch failed for '%s': %s",
                        topic,
                        e,
                        exc_info=True,
                    )

            if all_updates:
                compliance_path = DATA_DIR / "compliance_updates.json"
                compliance_path.write_text(
                    json.dumps(
                        {
                            "fetched_at": datetime.now(timezone.utc).isoformat(),
                            "updates": all_updates[:15],
                        },
                        indent=2,
                        default=str,
                    ),
                    encoding="utf-8",
                )
                self._mark_refreshed("compliance_updates", True, len(all_updates))
                logger.info("Enriched compliance updates: %d items", len(all_updates))
            else:
                self._mark_refreshed("compliance_updates", False)

        except ImportError:
            logger.warning("firecrawl_enrichment not available for compliance")
            self._mark_refreshed("compliance_updates", False)

    # ── Cycle runner ────────────────────────────────────────────────────────

    def run_cycle(self) -> dict:
        """Run one enrichment cycle. Check all sources and refresh stale ones.

        Returns:
            Dict with cycle results: checked, refreshed, skipped, failed counts.
        """
        logger.info("Data enrichment cycle starting...")
        cycle_start = time.time()

        tasks: list[tuple[str, callable]] = [
            ("live_market_data", self._enrich_live_market_data),
            ("firecrawl_news", self._enrich_firecrawl_news),
            ("bls_salary", self._enrich_bls_salary),
            ("fred_economic", self._enrich_fred_economic),
            ("adzuna_jobs", self._enrich_adzuna_jobs),
            ("census_demographics", self._enrich_census_demographics),
            ("compliance_updates", self._enrich_compliance_updates),
        ]

        results: dict = {
            "checked": 0,
            "refreshed": 0,
            "skipped": 0,
            "failed": 0,
        }

        for source, task_fn in tasks:
            results["checked"] += 1
            if not self._is_stale(source):
                results["skipped"] += 1
                continue
            try:
                task_fn()
                # Task internally calls _mark_refreshed; check if it succeeded
                # by looking at the last log entry
                if self._enrichment_log and self._enrichment_log[-1].get("success"):
                    results["refreshed"] += 1
                else:
                    results["failed"] += 1
            except (ValueError, KeyError, TypeError, OSError, RuntimeError) as e:
                results["failed"] += 1
                logger.error(
                    "Enrichment task '%s' failed: %s", source, e, exc_info=True
                )

        elapsed = round(time.time() - cycle_start, 2)
        results["elapsed_seconds"] = elapsed
        results["timestamp"] = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Data enrichment cycle complete: %d checked, %d refreshed, "
            "%d skipped, %d failed (%.1fs)",
            results["checked"],
            results["refreshed"],
            results["skipped"],
            results["failed"],
            elapsed,
        )
        return results

    # ── Background loop ─────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background enrichment loop.

        First cycle runs after a 5-minute delay to let services warm up.
        Subsequent cycles run every ENRICHMENT_INTERVAL seconds (1 hour).
        """
        if self._running:
            return
        self._running = True
        self._timer = threading.Timer(_INITIAL_DELAY, self._loop)
        self._timer.daemon = True
        self._timer.name = "data-enrichment"
        self._timer.start()
        logger.info(
            "Data enrichment engine started (first run in %d seconds)", _INITIAL_DELAY
        )

    def stop(self) -> None:
        """Stop the background enrichment loop."""
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None
        logger.info("Data enrichment engine stopped")

    def _loop(self) -> None:
        """Background loop -- run cycle, then schedule next."""
        if not self._running:
            return
        try:
            self.run_cycle()
        except (ValueError, KeyError, TypeError, OSError, RuntimeError) as e:
            logger.error("Enrichment cycle crashed: %s", e, exc_info=True)

        if self._running:
            self._timer = threading.Timer(ENRICHMENT_INTERVAL, self._loop)
            self._timer.daemon = True
            self._timer.name = "data-enrichment"
            self._timer.start()

    # ── Status reporting ────────────────────────────────────────────────────

    def get_status(self) -> dict:
        """Get current enrichment status for health endpoint.

        Returns:
            Dict with running state, persisted state, recent log entries,
            and per-source freshness details.
        """
        with self._lock:
            freshness: dict = {}
            for source, hours in FRESHNESS_THRESHOLDS.items():
                last_run = self._state.get("last_runs", {}).get(source)
                freshness[source] = {
                    "stale": self._is_stale(source),
                    "threshold_hours": hours,
                    "last_run": last_run,
                }

            return {
                "running": self._running,
                "state": {
                    "last_runs": dict(self._state.get("last_runs", {})),
                    "stats": dict(self._state.get("stats", {})),
                },
                "recent_log": list(self._enrichment_log[-10:]),
                "freshness": freshness,
            }


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════

_engine: Optional[DataEnrichmentEngine] = None
_engine_lock = threading.Lock()


def get_engine() -> DataEnrichmentEngine:
    """Get or create the singleton enrichment engine.

    Returns:
        The DataEnrichmentEngine singleton instance.
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = DataEnrichmentEngine()
    return _engine


def start_enrichment() -> None:
    """Start the enrichment engine (convenience wrapper)."""
    get_engine().start()


def get_enrichment_status() -> dict:
    """Get enrichment status for health endpoints.

    Returns:
        Dict with engine status, freshness, and recent activity.
    """
    return get_engine().get_status()
