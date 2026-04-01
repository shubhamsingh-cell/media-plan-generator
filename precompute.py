"""
precompute.py -- Pre-computation of salary, demand, and city-comparison data.

Pre-computes salary + demand for 50 cities x 20 roles (1,000 combos) plus
city-pair comparisons for 20 pairs x 5 roles (100 combos). Stored in
Supabase cache + in-memory with 7-day TTL. Stdlib-only, thread-safe.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Top 50 US cities by hiring volume
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

# Top 20 high-demand roles across industries
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

# Top 20 most commonly compared city pairs
TOP_CITY_PAIRS: List[Tuple[str, str]] = [
    ("San Francisco", "New York"),
    ("Austin", "Denver"),
    ("Seattle", "Boston"),
    ("Chicago", "Dallas"),
    ("Los Angeles", "San Diego"),
    ("Atlanta", "Charlotte"),
    ("Houston", "Dallas"),
    ("Phoenix", "Denver"),
    ("Miami", "Atlanta"),
    ("Portland", "Seattle"),
    ("Raleigh", "Charlotte"),
    ("Nashville", "Austin"),
    ("San Francisco", "Austin"),
    ("New York", "Boston"),
    ("Denver", "Salt Lake City"),
    ("San Jose", "San Francisco"),
    ("Washington DC", "Philadelphia"),
    ("Minneapolis", "Chicago"),
    ("Tampa", "Orlando"),
    ("Detroit", "Columbus"),
]

# Top 5 most compared roles for city-pair comparisons
TOP_COMPARISON_ROLES: List[str] = [
    "Software Engineer",
    "Data Scientist",
    "Nurse",
    "Marketing Manager",
    "Product Manager",
]

# Configuration
_PRECOMPUTE_TTL = 604800  # 7 days in seconds
_BATCH_SIZE = 10  # combos per batch
_BATCH_DELAY = 2.0  # seconds between batches
_API_DELAY = 1.0  # seconds between individual API calls
_STARTUP_DELAY = 600  # 10 minutes -- let server warm up first
_CYCLE_INTERVAL = 86400  # 24 hours between full runs
_CACHE_CATEGORY = "precompute"
_CACHE_KEY_PREFIX = "precompute:"
_COMPARISON_KEY_PREFIX = "precompute:compare:"

# Module state
_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_lock = threading.Lock()

# In-memory mirror for instant lookups (avoids Supabase round-trip on hot path)
_memory_cache: Dict[str, Tuple[float, dict]] = {}
_memory_cache_lock = threading.Lock()
_MEMORY_TTL = _PRECOMPUTE_TTL  # same as Supabase TTL

# Comparison pre-cache stats (last run)
_comparison_stats: Dict[str, Any] = {}
_comparison_stats_lock = threading.Lock()


def _cache_key(role: str, city: str) -> str:
    """Build a deterministic cache key for a role+city combo."""
    return f"{_CACHE_KEY_PREFIX}{role.lower().strip()}:{city.lower().strip()}"


# Public API -- fast lookups


def get_precomputed_data(role: str, city: str) -> Optional[dict]:
    """Retrieve pre-computed salary + demand data for a role+city combo.

    Checks in-memory cache first (zero-latency), then Supabase L3.
    Returns None if no data exists or has expired.
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
    """Retrieve only the salary portion of pre-computed data."""
    data = get_precomputed_data(role, city)
    if data is not None:
        return data.get("salary")
    return None


def get_precomputed_demand(role: str, city: str) -> Optional[dict]:
    """Retrieve only the demand portion of pre-computed data."""
    data = get_precomputed_data(role, city)
    if data is not None:
        return data.get("demand")
    return None


# City comparison -- public API


def _cache_comparison_key(role: str, city1: str, city2: str) -> str:
    """Build deterministic cache key for a city-pair comparison (alpha-sorted)."""
    c1, c2 = sorted([city1.lower().strip(), city2.lower().strip()])
    return f"{_COMPARISON_KEY_PREFIX}{role.lower().strip()}:{c1}:{c2}"


def get_precomputed_comparison(role: str, city1: str, city2: str) -> Optional[dict]:
    """Retrieve pre-computed comparison data for a role across two cities.

    Checks in-memory cache first, then Supabase L3. Returns None if
    no data exists or has expired.
    """
    key = _cache_comparison_key(role, city1, city2)

    with _memory_cache_lock:
        entry = _memory_cache.get(key)
        if entry is not None:
            ts, data = entry
            if time.monotonic() - ts < _MEMORY_TTL:
                return data
            else:
                del _memory_cache[key]

    try:
        from supabase_cache import cache_get

        cached = cache_get(key)
        if cached is not None:
            with _memory_cache_lock:
                _memory_cache[key] = (time.monotonic(), cached)
            return cached
    except ImportError:
        pass
    except Exception as e:
        logger.debug("[precompute] Comparison cache_get failed: %s", e)

    return None


def _extract_salary_midpoint(data: Optional[dict]) -> Optional[float]:
    """Extract salary midpoint from precomputed data (BLS p50 > raw median > range)."""
    if data is None:
        return None
    salary = data.get("salary") or {}
    raw = salary.get("raw") or {}
    # BLS percentiles first
    bls = salary.get("bls_percentiles") or {}
    p50 = bls.get("p50") or bls.get("median")
    if p50 is not None:
        try:
            return float(p50)
        except (ValueError, TypeError):
            pass
    # Raw median keys
    for key in ("median", "median_salary", "annual_median"):
        val = raw.get(key)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                pass
    # Salary range midpoint
    sr = salary.get("salary_range") or ""
    if "-" in str(sr):
        parts = str(sr).replace("$", "").replace(",", "").split("-")
        try:
            return (float(parts[0].strip()) + float(parts[1].strip())) / 2.0
        except (ValueError, TypeError, IndexError):
            pass
    return None


def _extract_job_count(data: Optional[dict]) -> Optional[int]:
    """Extract job posting count from precomputed data."""
    if data is None:
        return None
    jc = (data.get("demand") or {}).get("job_count")
    if jc is not None:
        try:
            return int(jc)
        except (ValueError, TypeError):
            pass
    return None


def _extract_coli(data: Optional[dict]) -> Optional[float]:
    """Extract cost-of-living index from precomputed data."""
    if data is None:
        return None
    coli = (data.get("salary") or {}).get("coli")
    if coli is not None:
        try:
            return float(coli)
        except (ValueError, TypeError):
            pass
    return None


def _build_comparison_recommendation(
    city1: str,
    city2: str,
    salary_diff_pct: Optional[float],
    demand_diff: Optional[int],
    coli_ratio: Optional[float],
) -> str:
    """Build a short recommendation string from comparison metrics."""
    signals: List[str] = []
    if salary_diff_pct is not None:
        if salary_diff_pct > 5:
            signals.append(f"{city2} offers higher salaries (+{salary_diff_pct}%)")
        elif salary_diff_pct < -5:
            signals.append(f"{city1} offers higher salaries (+{abs(salary_diff_pct)}%)")
    if demand_diff is not None:
        if demand_diff > 50:
            signals.append(f"{city2} has stronger job demand")
        elif demand_diff < -50:
            signals.append(f"{city1} has stronger job demand")
    if coli_ratio is not None:
        if coli_ratio > 1.1:
            signals.append(f"{city1} is more affordable (COLI ratio {coli_ratio})")
        elif coli_ratio < 0.9:
            signals.append(f"{city2} is more affordable (COLI ratio {coli_ratio})")
    if not signals:
        return f"{city1} and {city2} are comparable for this role"
    return "; ".join(signals)


def _fetch_comparison(role: str, city1: str, city2: str) -> dict:
    """Fetch both cities' data and compute comparison metrics.

    Reuses get_precomputed_data when available, falls back to _fetch_combo.
    Computes salary_diff_pct, demand_diff, COLI ratio, and recommendation.
    """
    data1 = get_precomputed_data(role, city1) or _fetch_combo(role, city1)
    data2 = get_precomputed_data(role, city2) or _fetch_combo(role, city2)

    s1, s2 = _extract_salary_midpoint(data1), _extract_salary_midpoint(data2)
    salary_diff_pct: Optional[float] = None
    if s1 and s2 and s1 > 0:
        salary_diff_pct = round(((s2 - s1) / s1) * 100, 1)

    d1, d2 = _extract_job_count(data1), _extract_job_count(data2)
    demand_diff: Optional[int] = None
    if d1 is not None and d2 is not None:
        demand_diff = d2 - d1

    c1, c2 = _extract_coli(data1), _extract_coli(data2)
    coli_ratio: Optional[float] = None
    if c1 and c2 and c1 > 0:
        coli_ratio = round(c2 / c1, 2)

    return {
        "role": role,
        "city1": city1,
        "city2": city2,
        "city1_data": data1,
        "city2_data": data2,
        "salary_diff_pct": salary_diff_pct,
        "demand_diff": demand_diff,
        "cost_of_living_index": {"city1": c1, "city2": c2, "ratio": coli_ratio},
        "recommendation": _build_comparison_recommendation(
            city1, city2, salary_diff_pct, demand_diff, coli_ratio
        ),
        "precomputed_at": datetime.now(timezone.utc).isoformat(),
        "ttl_seconds": _PRECOMPUTE_TTL,
    }


# Data fetching -- salary + demand for a single combo


def _fetch_salary_data(role: str, city: str) -> dict:
    """Fetch salary data for a role+city from orchestrator/APIs."""
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
    """Fetch market demand data for a role+city from orchestrator/APIs."""
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
    """Fetch both salary and demand for a role+city with rate limiting."""
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


# Batch pre-computation engine


def precompute_salary_demand(
    cities: Optional[List[str]] = None,
    roles: Optional[List[str]] = None,
) -> dict:
    """Fetch and cache salary + demand for all city x role combos.

    Processes in batches with delays to avoid API throttling. Stores
    in both in-memory and Supabase L3 cache with 7-day TTL.
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
    """Write a batch of entries to Supabase L3 cache."""
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


# City comparison batch pre-computation


def precompute_city_comparisons(
    city_pairs: Optional[List[Tuple[str, str]]] = None,
    roles: Optional[List[str]] = None,
) -> dict:
    """Pre-compute comparison data for top city-pairs x comparison roles.

    Processes combos (default 20 pairs x 5 roles = 100) with same
    batch/delay strategy as salary_demand. Stores in memory + Supabase.
    """
    city_pairs = city_pairs or TOP_CITY_PAIRS
    roles = roles or TOP_COMPARISON_ROLES
    total = len(city_pairs) * len(roles)
    stats: Dict[str, Any] = {
        "total": total,
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        "[precompute] Starting comparison pre-computation: %d pairs x %d roles = %d",
        len(city_pairs),
        len(roles),
        total,
    )
    combos = [(role, c1, c2) for c1, c2 in city_pairs for role in roles]
    batch_entries: List[Dict[str, Any]] = []
    processed = 0

    for i, (role, city1, city2) in enumerate(combos):
        if _stop_event.is_set():
            stats["skipped"] = total - i
            break
        if get_precomputed_comparison(role, city1, city2) is not None:
            stats["skipped"] += 1
            processed += 1
            continue
        try:
            data = _fetch_comparison(role, city1, city2)
            if data.get("city1_data") or data.get("city2_data"):
                key = _cache_comparison_key(role, city1, city2)
                with _memory_cache_lock:
                    _memory_cache[key] = (time.monotonic(), data)
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
                "[precompute] Failed comparison %s: %s vs %s: %s",
                role,
                city1,
                city2,
                e,
                exc_info=True,
            )
            stats["failed"] += 1
        processed += 1
        if len(batch_entries) >= _BATCH_SIZE:
            _flush_batch(batch_entries)
            batch_entries = []
            time.sleep(_BATCH_DELAY)
        if processed % 25 == 0:
            logger.info(
                "[precompute] Comparisons %d/%d (ok=%d fail=%d skip=%d)",
                processed,
                total,
                stats["success"],
                stats["failed"],
                stats["skipped"],
            )

    if batch_entries:
        _flush_batch(batch_entries)
    stats["finished_at"] = datetime.now(timezone.utc).isoformat()
    with _comparison_stats_lock:
        _comparison_stats.clear()
        _comparison_stats.update(stats)
    logger.info(
        "[precompute] Comparisons complete: %d/%d ok, %d fail, %d skip",
        stats["success"],
        total,
        stats["failed"],
        stats["skipped"],
    )
    return stats


# Background thread -- runs on startup and every 24 hours


def _background_loop() -> None:
    """Background loop: waits for warm-up, then runs pre-computation every 24h."""
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
                "[precompute] Salary/demand cycle complete: %s",
                json.dumps(stats, default=str),
            )
        except Exception as e:
            logger.error(
                "[precompute] Salary/demand cycle failed: %s",
                e,
                exc_info=True,
            )

        # City comparisons run after salary/demand so individual data is warm
        try:
            comp_stats = precompute_city_comparisons()
            logger.info(
                "[precompute] City comparison cycle complete: %s",
                json.dumps(comp_stats, default=str),
            )
        except Exception as e:
            logger.error(
                "[precompute] City comparison cycle failed: %s",
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
    """Start the background pre-computation daemon thread (idempotent)."""
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


# Status / health check


def get_precompute_status() -> dict:
    """Get current status of pre-computation system (thread, cache, coverage)."""
    with _memory_cache_lock:
        cache_size = len(_memory_cache)
        # Count non-expired entries (separate salary/demand from comparisons)
        now = time.monotonic()
        fresh_count = 0
        comparison_fresh = 0
        for k, (ts, _) in _memory_cache.items():
            if now - ts < _MEMORY_TTL:
                if k.startswith(_COMPARISON_KEY_PREFIX):
                    comparison_fresh += 1
                else:
                    fresh_count += 1

    total_combos = len(TOP_CITIES) * len(TOP_ROLES)
    total_comparisons = len(TOP_CITY_PAIRS) * len(TOP_COMPARISON_ROLES)

    with _comparison_stats_lock:
        comp_stats = dict(_comparison_stats) if _comparison_stats else {}

    return {
        "thread_alive": _thread is not None and _thread.is_alive(),
        "memory_cache_size": cache_size,
        "fresh_entries": fresh_count,
        "total_possible_combos": total_combos,
        "coverage_pct": (
            round(fresh_count / total_combos * 100, 1) if total_combos else 0
        ),
        "comparisons": {
            "fresh_entries": comparison_fresh,
            "total_possible": total_comparisons,
            "coverage_pct": (
                round(comparison_fresh / total_comparisons * 100, 1)
                if total_comparisons
                else 0
            ),
            "city_pairs": len(TOP_CITY_PAIRS),
            "comparison_roles": len(TOP_COMPARISON_ROLES),
            "last_run": comp_stats,
        },
        "config": {
            "cities": len(TOP_CITIES),
            "roles": len(TOP_ROLES),
            "ttl_days": _PRECOMPUTE_TTL // 86400,
            "batch_size": _BATCH_SIZE,
            "cycle_hours": _CYCLE_INTERVAL // 3600,
            "startup_delay_minutes": _STARTUP_DELAY // 60,
        },
    }
