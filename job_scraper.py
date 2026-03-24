#!/usr/bin/env python3
"""Multi-platform job scraper using python-jobspy.

Scrapes LinkedIn, Indeed, Glassdoor, ZipRecruiter, and Google Jobs
simultaneously. Returns structured job data for use across products.

All functions:
    - Return None on failure (never raise)
    - Cache results for 1 hour
    - Log errors with exc_info=True
    - Use type hints on all signatures

Environment:
    pip install python-jobspy (optional -- module degrades gracefully)
"""

from __future__ import annotations

import hashlib
import logging
import statistics
import threading
import time
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)

# ── Lazy import of jobspy ────────────────────────────────────────────────────
_jobspy = None
_jobspy_checked = False
_jobspy_lock = threading.Lock()

_DEFAULT_SITES = ["indeed", "linkedin", "glassdoor", "zip_recruiter", "google"]


def _ensure_jobspy() -> bool:
    """Lazy-load python-jobspy. Returns True if available."""
    global _jobspy, _jobspy_checked
    if _jobspy_checked:
        return _jobspy is not None
    with _jobspy_lock:
        if _jobspy_checked:
            return _jobspy is not None
        try:
            from jobspy import scrape_jobs as _scrape  # noqa: F401
            import jobspy as _mod

            _jobspy = _mod
        except ImportError:
            logger.warning(
                "python-jobspy not installed; job scraping disabled. "
                "Install with: pip install python-jobspy"
            )
            _jobspy = None
        _jobspy_checked = True
    return _jobspy is not None


# ── In-memory cache with TTL ─────────────────────────────────────────────────
_cache: dict[str, tuple[Any, float]] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600.0  # 1 hour


def _cache_key(prefix: str, *parts: str) -> str:
    """Generate a deterministic cache key from string parts."""
    raw = f"{prefix}:{'|'.join(str(p) for p in parts)}"
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()


def _cache_get(key: str) -> Any | None:
    """Return cached value if not expired, else None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        value, ts = entry
        if time.time() - ts > _CACHE_TTL:
            del _cache[key]
            return None
        return value


def _cache_set(key: str, value: Any) -> None:
    """Store value in cache with current timestamp."""
    with _cache_lock:
        _cache[key] = (value, time.time())


# ── Public API ───────────────────────────────────────────────────────────────


def scrape_jobs(
    role: str,
    location: str = "USA",
    site_names: list[str] | None = None,
    results_wanted: int = 20,
) -> list[dict] | None:
    """Scrape job listings from multiple job boards concurrently.

    Args:
        role: Job title or search query (e.g. "Software Engineer").
        location: Geographic filter (e.g. "USA", "New York, NY").
        site_names: List of sites to scrape. Defaults to all 5 supported.
        results_wanted: Target number of results per site.

    Returns:
        List of dicts with keys: title, company, location, salary_min,
        salary_max, description, url, date_posted, site.
        Returns None on failure.
    """
    if not _ensure_jobspy():
        return None

    sites = site_names or _DEFAULT_SITES
    ck = _cache_key(
        "scrape", role, location, ",".join(sorted(sites)), str(results_wanted)
    )
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    try:
        jobs_df = _jobspy.scrape_jobs(
            site_name=sites,
            search_term=role,
            location=location,
            results_wanted=results_wanted,
            country_indeed="USA" if "usa" in location.lower() else location,
        )

        if jobs_df is None or jobs_df.empty:
            logger.info(
                "JobSpy returned no results for role=%s location=%s", role, location
            )
            return []

        results: list[dict] = []
        for _, row in jobs_df.iterrows():
            results.append(
                {
                    "title": str(row.get("title") or ""),
                    "company": str(row.get("company_name") or row.get("company") or ""),
                    "location": str(row.get("location") or ""),
                    "salary_min": _safe_float(
                        row.get("min_amount") or row.get("salary_min")
                    ),
                    "salary_max": _safe_float(
                        row.get("max_amount") or row.get("salary_max")
                    ),
                    "description": str(row.get("description") or "")[:500],
                    "url": str(row.get("job_url") or row.get("url") or ""),
                    "date_posted": str(row.get("date_posted") or ""),
                    "site": str(row.get("site") or ""),
                }
            )

        _cache_set(ck, results)
        logger.info(
            "JobSpy scraped %d jobs for role=%s location=%s sites=%s",
            len(results),
            role,
            location,
            sites,
        )
        return results

    except Exception as e:
        logger.error("JobSpy scrape failed for role=%s: %s", role, e, exc_info=True)
        return None


def get_job_market_stats(
    role: str,
    location: str = "USA",
) -> dict | None:
    """Scrape 50+ jobs and compute market statistics.

    This is the power function -- gives real market data from live job boards.

    Args:
        role: Job title to analyze (e.g. "Data Scientist").
        location: Geographic filter.

    Returns:
        Dict with keys: avg_salary, median_salary, total_postings,
        top_companies, top_locations, salary_range, sites_breakdown.
        Returns None on failure.
    """
    ck = _cache_key("market_stats", role, location)
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    jobs = scrape_jobs(role=role, location=location, results_wanted=50)
    if jobs is None:
        return None
    if not jobs:
        return {
            "total_postings": 0,
            "avg_salary": None,
            "median_salary": None,
            "top_companies": [],
            "top_locations": [],
            "salary_range": None,
            "sites_breakdown": {},
            "role": role,
            "location": location,
        }

    # Salary analysis
    salaries: list[float] = []
    for j in jobs:
        sal_min = j.get("salary_min")
        sal_max = j.get("salary_max")
        if sal_min and sal_max:
            salaries.append((sal_min + sal_max) / 2)
        elif sal_min:
            salaries.append(sal_min)
        elif sal_max:
            salaries.append(sal_max)

    avg_salary = round(statistics.mean(salaries), 2) if salaries else None
    median_salary = round(statistics.median(salaries), 2) if salaries else None
    salary_range = (
        {"min": round(min(salaries), 2), "max": round(max(salaries), 2)}
        if salaries
        else None
    )

    # Top companies
    company_counts = Counter(
        j.get("company") or "Unknown" for j in jobs if j.get("company")
    )
    top_companies = [
        {"name": name, "postings": count}
        for name, count in company_counts.most_common(10)
    ]

    # Top locations
    location_counts = Counter(
        j.get("location") or "Unknown" for j in jobs if j.get("location")
    )
    top_locations = [
        {"location": loc, "postings": count}
        for loc, count in location_counts.most_common(10)
    ]

    # Sites breakdown
    site_counts = Counter(j.get("site") or "unknown" for j in jobs)
    sites_breakdown = dict(site_counts)

    result = {
        "total_postings": len(jobs),
        "avg_salary": avg_salary,
        "median_salary": median_salary,
        "salary_range": salary_range,
        "top_companies": top_companies,
        "top_locations": top_locations,
        "sites_breakdown": sites_breakdown,
        "salary_data_points": len(salaries),
        "role": role,
        "location": location,
    }

    _cache_set(ck, result)
    return result


def get_salary_benchmarks(
    role: str,
    locations: list[str] | None = None,
) -> dict | None:
    """Scrape salary data across multiple locations for comparison.

    Args:
        role: Job title to benchmark (e.g. "Registered Nurse").
        locations: List of locations to compare. Defaults to major US metros.

    Returns:
        Dict with per-location salary data and overall summary.
        Returns None on failure.
    """
    if locations is None:
        locations = [
            "New York, NY",
            "San Francisco, CA",
            "Chicago, IL",
            "Austin, TX",
            "Seattle, WA",
        ]

    ck = _cache_key("salary_bench", role, ",".join(sorted(locations)))
    cached = _cache_get(ck)
    if cached is not None:
        return cached

    benchmarks: list[dict] = []
    all_salaries: list[float] = []

    for loc in locations:
        stats = get_job_market_stats(role=role, location=loc)
        if stats is None:
            benchmarks.append(
                {
                    "location": loc,
                    "avg_salary": None,
                    "median_salary": None,
                    "postings": 0,
                    "error": "scrape_failed",
                }
            )
            continue

        entry = {
            "location": loc,
            "avg_salary": stats.get("avg_salary"),
            "median_salary": stats.get("median_salary"),
            "postings": stats.get("total_postings") or 0,
            "salary_range": stats.get("salary_range"),
            "top_companies": stats.get("top_companies", [])[:5],
        }
        benchmarks.append(entry)

        if stats.get("avg_salary"):
            all_salaries.append(stats["avg_salary"])

    result = {
        "role": role,
        "locations": benchmarks,
        "national_avg": (
            round(statistics.mean(all_salaries), 2) if all_salaries else None
        ),
        "highest_paying": (
            max(benchmarks, key=lambda b: b.get("avg_salary") or 0).get("location")
            if all_salaries
            else None
        ),
        "locations_compared": len(locations),
    }

    _cache_set(ck, result)
    return result


# ── Helpers ──────────────────────────────────────────────────────────────────


def _safe_float(val: Any) -> float | None:
    """Convert value to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


def get_status() -> dict:
    """Return status dict for health/diagnostics endpoints."""
    available = _ensure_jobspy()
    return {
        "jobspy_available": available,
        "cache_entries": len(_cache),
        "default_sites": _DEFAULT_SITES,
    }
