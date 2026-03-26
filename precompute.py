"""
precompute.py -- Nightly pre-computation of salary and demand data.

Pre-computes salary + market demand data for top 50 cities x 20 roles
(1,000 combos) so Nova can answer instantly without live API calls.

Data is stored in Supabase cache table with 7-day TTL. A background
thread runs pre-computation on startup (after 10-minute warm-up) and
then every 24 hours.

Stdlib-only, thread-safe.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Top 50 US cities by hiring volume
# ---------------------------------------------------------------------------

TOP_CITIES: List[str] = [
    "San Francisco",
    "New York",
    "Los Angeles",
    "Chicago",
    "Austin",
    "Seattle",
    "Boston",
    "Denver",
    "Atlanta",
    "Dallas",
    "Houston",
    "Phoenix",
    "Portland",
    "San Diego",
    "Miami",
    "Philadelphia",
    "Minneapolis",
    "Detroit",
    "Raleigh",
    "Nashville",
    "Charlotte",
    "Tampa",
    "Orlando",
    "Salt Lake City",
    "Pittsburgh",
    "San Jose",
    "Washington DC",
    "Baltimore",
    "Indianapolis",
    "Columbus",
    "San Antonio",
    "Jacksonville",
    "Fort Worth",
    "Kansas City",
    "Las Vegas",
    "Milwaukee",
    "Oklahoma City",
    "Memphis",
    "Louisville",
    "Richmond",
    "New Orleans",
    "Hartford",
    "Cincinnati",
    "Cleveland",
    "St. Louis",
    "Sacramento",
    "Virginia Beach",
    "Tucson",
    "Boise",
    "Des Moines",
]

# ---------------------------------------------------------------------------
# Top 20 high-demand roles across industries
# ---------------------------------------------------------------------------

TOP_ROLES: List[str] = [
    "Software Engineer",
    "Data Scientist",
    "Product Manager",
    "DevOps Engineer",
    "Nurse",
    "Marketing Manager",
    "Sales Representative",
    "Accountant",
    "Graphic Designer",
    "Project Manager",
    "Business Analyst",
    "UX Designer",
    "Mechanical Engineer",
    "Teacher",
    "Pharmacist",
    "Financial Analyst",
    "HR Manager",
    "Cybersecurity Analyst",
    "Cloud Architect",
    "Physical Therapist",
]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_PRECOMPUTE_TTL = 604800  # 7 days in seconds
_BATCH_SIZE = 10  # combos per batch
_BATCH_DELAY = 2.0  # seconds between batches
_API_DELAY = 1.0  # seconds between individual API calls
_STARTUP_DELAY = 600  # 10 minutes -- let server warm up first
_CYCLE_INTERVAL = 86400  # 24 hours between full runs
_CACHE_CATEGORY = "precompute"
_CACHE_KEY_PREFIX = "precompute:"

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_lock = threading.Lock()

# In-memory mirror for instant lookups (avoids Supabase round-trip on hot path)
_memory_cache: Dict[str, Tuple[float, dict]] = {}
_memory_cache_lock = threading.Lock()
_MEMORY_TTL = _PRECOMPUTE_TTL  # same as Supabase TTL


def _cache_key(role: str, city: str) -> str:
    """Build a deterministic cache key for a role+city combo."""
    return f"{_CACHE_KEY_PREFIX}{role.lower().strip()}:{city.lower().strip()}"


# ---------------------------------------------------------------------------
# Public API -- fast lookups
# ---------------------------------------------------------------------------


def get_precomputed_data(role: str, city: str) -> Optional[dict]:
    """Retrieve pre-computed salary + demand data for a role+city combo.

    Checks in-memory cache first (zero-latency), then falls back to
    Supabase L3 cache. Returns None if no pre-computed data exists or
    if data has expired.

    Args:
        role: Job title (e.g. "Software Engineer").
        city: City name (e.g. "San Francisco").

    Returns:
        Dict with salary and demand data, or None if not available.
    """
    key = _cache_key(role, city)

    # L0: in-memory mirror (fastest)
    with _memory_cache_lock:
        entry = _memory_cache.get(key)
        if entry is not None:
            ts, data = entry
            if time.monotonic() - ts < _MEMORY_TTL:
                return data
            else:
                del _memory_cache[key]

    # L3: Supabase persistent cache
    try:
        from supabase_cache import cache_get

        cached = cache_get(key)
        if cached is not None:
            # Backfill in-memory mirror
            with _memory_cache_lock:
                _memory_cache[key] = (time.monotonic(), cached)
            return cached
    except ImportError:
        logger.debug("[precompute] supabase_cache not available")
    except Exception as e:
        logger.debug("[precompute] Supabase cache_get failed: %s", e)

    return None


def get_precomputed_salary(role: str, city: str) -> Optional[dict]:
    """Retrieve only the salary portion of pre-computed data.

    Args:
        role: Job title.
        city: City name.

    Returns:
        Dict with salary data, or None.
    """
    data = get_precomputed_data(role, city)
    if data is not None:
        return data.get("salary")
    return None


def get_precomputed_demand(role: str, city: str) -> Optional[dict]:
    """Retrieve only the demand portion of pre-computed data.

    Args:
        role: Job title.
        city: City name.

    Returns:
        Dict with demand data, or None.
    """
    data = get_precomputed_data(role, city)
    if data is not None:
        return data.get("demand")
    return None


# ---------------------------------------------------------------------------
# Data fetching -- salary + demand for a single combo
# ---------------------------------------------------------------------------


def _fetch_salary_data(role: str, city: str) -> dict:
    """Fetch salary data for a role+city from orchestrator/APIs.

    Args:
        role: Job title.
        city: City name.

    Returns:
        Dict with salary information.
    """
    try:
        import data_orchestrator

        orch = data_orchestrator
        enriched = orch.enrich_salary(role, city)
        if enriched and enriched.get("data"):
            return {
                "source": enriched.get("source", "orchestrator"),
                "salary_range": enriched.get("salary_range", "N/A"),
                "role_tier": enriched.get("role_tier", "Professional"),
                "bls_percentiles": enriched.get("bls_percentiles"),
                "coli": enriched.get("coli"),
                "confidence": enriched.get("confidence"),
                "data_freshness": enriched.get("data_freshness"),
                "sources_used": enriched.get("sources_used", []),
                "raw": enriched.get("data", {}),
            }
    except ImportError:
        logger.debug("[precompute] data_orchestrator not available for salary")
    except Exception as e:
        logger.debug("[precompute] Salary fetch failed for %s/%s: %s", role, city, e)

    return {}


def _fetch_demand_data(role: str, city: str) -> dict:
    """Fetch market demand data for a role+city from orchestrator/APIs.

    Args:
        role: Job title.
        city: City name.

    Returns:
        Dict with demand information.
    """
    try:
        import data_orchestrator

        orch = data_orchestrator
        enriched = orch.enrich_market_demand(role, city, "")
        if enriched and enriched.get("data"):
            return {
                "source": enriched.get("source", "orchestrator"),
                "job_count": enriched.get("current_posting_count"),
                "competitors": enriched.get("competitors"),
                "seasonal": enriched.get("seasonal"),
                "confidence": enriched.get("confidence"),
                "data_freshness": enriched.get("data_freshness"),
                "sources_used": enriched.get("sources_used", []),
                "raw": enriched.get("data", {}),
            }
    except ImportError:
        logger.debug("[precompute] data_orchestrator not available for demand")
    except Exception as e:
        logger.debug("[precompute] Demand fetch failed for %s/%s: %s", role, city, e)

    return {}


def _fetch_combo(role: str, city: str) -> dict:
    """Fetch both salary and demand data for a single role+city combo.

    Rate-limits with a 1-second delay between API calls to avoid
    throttling upstream services.

    Args:
        role: Job title.
        city: City name.

    Returns:
        Dict with 'salary' and 'demand' sub-dicts plus metadata.
    """
    salary = _fetch_salary_data(role, city)
    time.sleep(_API_DELAY)  # rate limit between API calls
    demand = _fetch_demand_data(role, city)

    return {
        "role": role,
        "city": city,
        "salary": salary,
        "demand": demand,
        "precomputed_at": datetime.now(timezone.utc).isoformat(),
        "ttl_seconds": _PRECOMPUTE_TTL,
    }


# ---------------------------------------------------------------------------
# Batch pre-computation engine
# ---------------------------------------------------------------------------


def precompute_salary_demand(
    cities: Optional[List[str]] = None,
    roles: Optional[List[str]] = None,
) -> dict:
    """Fetch and cache salary + demand data for all city x role combos.

    Processes in batches of BATCH_SIZE with BATCH_DELAY between batches
    to avoid overwhelming APIs. Stores results in both in-memory cache
    and Supabase L3 cache with 7-day TTL.

    Args:
        cities: List of cities (defaults to TOP_CITIES).
        roles: List of roles (defaults to TOP_ROLES).

    Returns:
        Summary dict with counts of successes, failures, and skipped.
    """
    cities = cities or TOP_CITIES
    roles = roles or TOP_ROLES
    total = len(cities) * len(roles)

    stats = {
        "total": total,
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "[precompute] Starting pre-computation: %d cities x %d roles = %d combos",
        len(cities),
        len(roles),
        total,
    )

    # Build all combos
    combos: List[Tuple[str, str]] = []
    for city in cities:
        for role in roles:
            combos.append((role, city))

    # Process in batches
    batch_entries: List[Dict[str, Any]] = []
    processed = 0

    for i, (role, city) in enumerate(combos):
        if _stop_event.is_set():
            logger.info("[precompute] Stop requested, halting at %d/%d", i, total)
            stats["skipped"] = total - i
            break

        # Check if we already have fresh data (skip if so)
        existing = get_precomputed_data(role, city)
        if existing is not None:
            stats["skipped"] += 1
            processed += 1
            continue

        try:
            data = _fetch_combo(role, city)

            # Only cache if we got meaningful data
            if data.get("salary") or data.get("demand"):
                key = _cache_key(role, city)

                # Store in memory cache
                with _memory_cache_lock:
                    _memory_cache[key] = (time.monotonic(), data)

                # Queue for batch Supabase write
                batch_entries.append(
                    {
                        "key": key,
                        "data": data,
                        "ttl": _PRECOMPUTE_TTL,
                        "category": _CACHE_CATEGORY,
                    }
                )

                stats["success"] += 1
            else:
                stats["failed"] += 1

        except Exception as e:
            logger.warning(
                "[precompute] Failed combo %s/%s: %s",
                role,
                city,
                e,
                exc_info=True,
            )
            stats["failed"] += 1

        processed += 1

        # Flush batch to Supabase every BATCH_SIZE entries
        if len(batch_entries) >= _BATCH_SIZE:
            _flush_batch(batch_entries)
            batch_entries = []
            time.sleep(_BATCH_DELAY)

        # Progress logging every 50 combos
        if processed % 50 == 0:
            logger.info(
                "[precompute] Pre-computed %d/%d combos (success=%d, failed=%d, skipped=%d)",
                processed,
                total,
                stats["success"],
                stats["failed"],
                stats["skipped"],
            )

    # Flush remaining entries
    if batch_entries:
        _flush_batch(batch_entries)

    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    logger.info(
        "[precompute] Pre-computation complete: %d/%d success, %d failed, %d skipped",
        stats["success"],
        total,
        stats["failed"],
        stats["skipped"],
    )

    return stats


def _flush_batch(entries: List[Dict[str, Any]]) -> None:
    """Write a batch of entries to Supabase L3 cache.

    Args:
        entries: List of cache entry dicts with key, data, ttl, category.
    """
    if not entries:
        return

    try:
        from supabase_cache import cache_set_many

        written = cache_set_many(entries)
        logger.debug(
            "[precompute] Flushed batch of %d entries to Supabase (%d written)",
            len(entries),
            written,
        )
    except ImportError:
        logger.debug("[precompute] supabase_cache not available for batch write")
    except Exception as e:
        logger.warning("[precompute] Batch flush failed: %s", e)


# ---------------------------------------------------------------------------
# Background thread -- runs on startup and every 24 hours
# ---------------------------------------------------------------------------


def _background_loop() -> None:
    """Background loop that runs pre-computation on schedule.

    Waits STARTUP_DELAY seconds after server start (to let KB load),
    then runs pre-computation. Repeats every CYCLE_INTERVAL seconds.
    """
    logger.info(
        "[precompute] Background thread started, waiting %ds for server warm-up...",
        _STARTUP_DELAY,
    )

    # Wait for server to warm up (KB loading, etc.)
    if _stop_event.wait(_STARTUP_DELAY):
        logger.info("[precompute] Stop requested during warm-up, exiting")
        return

    while not _stop_event.is_set():
        logger.info("[precompute] Starting scheduled pre-computation cycle")

        try:
            stats = precompute_salary_demand()
            logger.info(
                "[precompute] Cycle complete: %s",
                json.dumps(stats, default=str),
            )
        except Exception as e:
            logger.error(
                "[precompute] Pre-computation cycle failed: %s",
                e,
                exc_info=True,
            )

        # Sleep until next cycle (interruptible)
        logger.info(
            "[precompute] Next cycle in %d seconds (%d hours)",
            _CYCLE_INTERVAL,
            _CYCLE_INTERVAL // 3600,
        )
        if _stop_event.wait(_CYCLE_INTERVAL):
            break

    logger.info("[precompute] Background thread stopped")


def start_precompute_thread() -> bool:
    """Start the background pre-computation thread.

    Safe to call multiple times -- will not start a second thread if
    one is already running. Thread is started as daemon so it won't
    prevent server shutdown.

    Returns:
        True if thread was started, False if already running.
    """
    global _thread

    with _lock:
        if _thread is not None and _thread.is_alive():
            logger.info("[precompute] Background thread already running")
            return False

        _stop_event.clear()
        _thread = threading.Thread(
            target=_background_loop,
            daemon=True,
            name="precompute-salary-demand",
        )
        _thread.start()
        logger.info("[precompute] Background pre-computation thread started")
        return True


def stop_precompute_thread() -> None:
    """Signal the background thread to stop gracefully."""
    _stop_event.set()
    logger.info("[precompute] Stop signal sent to background thread")


# ---------------------------------------------------------------------------
# Status / health check
# ---------------------------------------------------------------------------


def get_precompute_status() -> dict:
    """Get current status of the pre-computation system.

    Returns:
        Dict with thread status, cache stats, and coverage info.
    """
    with _memory_cache_lock:
        cache_size = len(_memory_cache)
        # Count non-expired entries
        now = time.monotonic()
        fresh_count = sum(
            1 for ts, _ in _memory_cache.values() if now - ts < _MEMORY_TTL
        )

    total_combos = len(TOP_CITIES) * len(TOP_ROLES)

    return {
        "thread_alive": _thread is not None and _thread.is_alive(),
        "memory_cache_size": cache_size,
        "fresh_entries": fresh_count,
        "total_possible_combos": total_combos,
        "coverage_pct": (
            round(fresh_count / total_combos * 100, 1) if total_combos else 0
        ),
        "config": {
            "cities": len(TOP_CITIES),
            "roles": len(TOP_ROLES),
            "ttl_days": _PRECOMPUTE_TTL // 86400,
            "batch_size": _BATCH_SIZE,
            "cycle_hours": _CYCLE_INTERVAL // 3600,
            "startup_delay_minutes": _STARTUP_DELAY // 60,
        },
    }
