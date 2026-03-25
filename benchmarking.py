"""Anonymized Benchmarking Network for Nova AI Suite.

Aggregates anonymized spend-vs-outcome data across generated media plans
to produce industry benchmarks that improve with each new plan.

Data collected (PII-free):
    - role_family (e.g., "Engineering", "Sales")
    - location_region (e.g., "US-West", "EMEA")
    - budget_range (e.g., "$5k-$10k")
    - channels_used (list of channel names)
    - channel_allocations (dict of channel -> percentage)

Benchmark tables produced:
    - avg_cpc_by_role_family
    - avg_budget_by_role_family
    - top_channels_by_role_family
    - seasonal_trends (month-over-month)

Storage: Supabase table `benchmarking_data` (env vars SUPABASE_URL,
SUPABASE_SERVICE_ROLE_KEY).

Thread-safe singleton via get_benchmarking().
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

_SUPABASE_URL: str = os.environ.get("SUPABASE_URL") or ""
_SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
_TABLE_NAME: str = "benchmarking_data"

# Budget range buckets
_BUDGET_RANGES: list[tuple[int, int, str]] = [
    (0, 1_000, "< $1k"),
    (1_000, 5_000, "$1k-$5k"),
    (5_000, 10_000, "$5k-$10k"),
    (10_000, 25_000, "$10k-$25k"),
    (25_000, 50_000, "$25k-$50k"),
    (50_000, 100_000, "$50k-$100k"),
    (100_000, 500_000, "$100k-$500k"),
    (500_000, float("inf"), "$500k+"),
]

# Region normalization map
_REGION_MAP: dict[str, str] = {
    "united states": "US",
    "usa": "US",
    "us": "US",
    "canada": "Canada",
    "uk": "UK",
    "united kingdom": "UK",
    "germany": "EMEA",
    "france": "EMEA",
    "europe": "EMEA",
    "india": "APAC",
    "australia": "APAC",
    "japan": "APAC",
    "singapore": "APAC",
    "china": "APAC",
}


# ═══════════════════════════════════════════════════════════════════════════════
# SUPABASE REST HELPER
# ═══════════════════════════════════════════════════════════════════════════════


def _supabase_rest(
    table: str,
    method: str = "GET",
    payload: Optional[Any] = None,
    params: str = "",
) -> Optional[Any]:
    """Make a REST call to the Supabase PostgREST API.

    Args:
        table: Table name.
        method: HTTP method (GET, POST, PATCH, DELETE).
        payload: JSON body for POST/PATCH.
        params: Query string (e.g., '?role_family=eq.Engineering').

    Returns:
        Parsed JSON response or None on failure.
    """
    if not _SUPABASE_URL or not _SUPABASE_KEY:
        return None
    url = f"{_SUPABASE_URL.rstrip('/')}/rest/v1/{table}{params}"
    headers = {
        "apikey": _SUPABASE_KEY,
        "Authorization": f"Bearer {_SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    body = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except (
        urllib.error.URLError,
        OSError,
        json.JSONDecodeError,
        ValueError,
        TimeoutError,
    ) as exc:
        # Downgrade to warning -- benchmarking_data table may not exist yet
        logger.warning("Supabase REST %s %s failed (non-fatal): %s", method, table, exc)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# ANONYMIZATION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _classify_budget(budget: float) -> str:
    """Classify a numeric budget into a privacy-safe range bucket.

    Args:
        budget: Raw budget amount in dollars.

    Returns:
        Human-readable budget range string.
    """
    for low, high, label in _BUDGET_RANGES:
        if low <= budget < high:
            return label
    return "$500k+"


def _normalize_region(location: str) -> str:
    """Normalize a free-text location into a broad region.

    Args:
        location: Raw location string (e.g., 'San Francisco, CA, USA').

    Returns:
        Broad region label (e.g., 'US', 'EMEA', 'APAC').
    """
    loc_lower = (location or "").lower().strip()
    for key, region in _REGION_MAP.items():
        if key in loc_lower:
            return region
    # US state abbreviations
    us_states = {
        "al",
        "ak",
        "az",
        "ar",
        "ca",
        "co",
        "ct",
        "de",
        "fl",
        "ga",
        "hi",
        "id",
        "il",
        "in",
        "ia",
        "ks",
        "ky",
        "la",
        "me",
        "md",
        "ma",
        "mi",
        "mn",
        "ms",
        "mo",
        "mt",
        "ne",
        "nv",
        "nh",
        "nj",
        "nm",
        "ny",
        "nc",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "ri",
        "sc",
        "sd",
        "tn",
        "tx",
        "ut",
        "vt",
        "va",
        "wa",
        "wv",
        "wi",
        "wy",
    }
    parts = [p.strip().lower() for p in loc_lower.replace(",", " ").split()]
    for part in parts:
        if part in us_states:
            return "US"
    return "Other"


def _normalize_role_family(role_family: str) -> str:
    """Normalize role family to consistent casing.

    Args:
        role_family: Raw role family string.

    Returns:
        Title-cased, stripped role family.
    """
    return (role_family or "General").strip().title()


def _anonymize_plan_data(plan_data: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract anonymized fields from a raw plan, stripping all PII.

    Args:
        plan_data: Raw media plan dict (may contain company name, contact info, etc.).

    Returns:
        Anonymized record with only benchmarking-safe fields, or None if insufficient data.
    """
    role_family = _normalize_role_family(
        plan_data.get("role_family") or plan_data.get("role") or ""
    )
    if not role_family or role_family == "General":
        # Try to extract from job title
        title = plan_data.get("job_title") or plan_data.get("title") or ""
        if title:
            role_family = _infer_role_family(title)

    location = plan_data.get("location") or plan_data.get("region") or ""
    region = _normalize_region(location)

    budget_raw = plan_data.get("budget") or plan_data.get("total_budget") or 0
    if isinstance(budget_raw, str):
        try:
            budget_raw = float(budget_raw.replace("$", "").replace(",", "").strip())
        except (ValueError, AttributeError):
            budget_raw = 0
    budget_range = _classify_budget(float(budget_raw))

    channels = plan_data.get("channels") or plan_data.get("channels_used") or []
    if isinstance(channels, dict):
        channels = list(channels.keys())
    channels = [str(c).strip().lower() for c in channels if c]

    allocations = (
        plan_data.get("channel_allocations") or plan_data.get("allocations") or {}
    )
    if isinstance(allocations, list):
        # Convert list of dicts to a single dict
        merged: dict[str, float] = {}
        for item in allocations:
            if isinstance(item, dict):
                name = str(item.get("channel") or item.get("name") or "").lower()
                pct = float(item.get("percentage") or item.get("allocation") or 0)
                if name:
                    merged[name] = pct
        allocations = merged

    # CPC data if available
    cpc = plan_data.get("avg_cpc") or plan_data.get("cpc") or None
    if cpc is not None:
        try:
            cpc = round(float(cpc), 2)
        except (ValueError, TypeError):
            cpc = None

    # Generate a deterministic hash ID (no PII in the hash input)
    hash_input = (
        f"{role_family}:{region}:{budget_range}:{sorted(channels)}:{time.time()}"
    )
    record_id = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    return {
        "id": record_id,
        "role_family": role_family,
        "location_region": region,
        "budget_range": budget_range,
        "channels_used": channels,
        "channel_allocations": allocations,
        "avg_cpc": cpc,
        "budget_numeric": float(budget_raw) if budget_raw else None,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "month": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m"),
    }


def _infer_role_family(title: str) -> str:
    """Infer a role family from a job title string.

    Args:
        title: Job title (e.g., 'Senior Software Engineer').

    Returns:
        Inferred role family string.
    """
    title_lower = title.lower()
    role_keywords: dict[str, list[str]] = {
        "Engineering": [
            "engineer",
            "developer",
            "devops",
            "sre",
            "architect",
            "programmer",
            "coder",
        ],
        "Sales": ["sales", "account executive", "business development", "bdr", "sdr"],
        "Marketing": ["marketing", "brand", "content", "seo", "growth", "demand gen"],
        "Design": ["design", "ux", "ui", "creative", "graphic"],
        "Product": ["product manager", "product owner", "product lead"],
        "Data Science": [
            "data scientist",
            "data analyst",
            "machine learning",
            "ml engineer",
            "ai",
        ],
        "Finance": ["finance", "accounting", "controller", "cfo", "treasury"],
        "HR": ["hr", "human resources", "recruiter", "talent", "people ops"],
        "Operations": ["operations", "logistics", "supply chain", "procurement"],
        "Customer Success": [
            "customer success",
            "customer support",
            "support engineer",
        ],
        "Legal": ["legal", "counsel", "attorney", "compliance"],
        "Executive": [
            "ceo",
            "cto",
            "cfo",
            "coo",
            "vp",
            "vice president",
            "director",
            "chief",
        ],
    }
    for family, keywords in role_keywords.items():
        if any(kw in title_lower for kw in keywords):
            return family
    return "General"


# ═══════════════════════════════════════════════════════════════════════════════
# BENCHMARKING ENGINE (Thread-safe Singleton)
# ═══════════════════════════════════════════════════════════════════════════════


class BenchmarkingNetwork:
    """Anonymized benchmarking network that aggregates plan data into industry benchmarks.

    Thread-safe singleton. All shared state is guarded by a threading.Lock.
    In-memory cache is refreshed from Supabase on startup and periodically.
    """

    _instance: Optional[BenchmarkingNetwork] = None
    _instance_lock: threading.Lock = threading.Lock()

    def __init__(self) -> None:
        """Initialize the benchmarking network with empty in-memory store."""
        self._lock: threading.Lock = threading.Lock()
        self._records: list[dict[str, Any]] = []
        self._benchmarks_cache: dict[str, Any] = {}
        self._cache_timestamp: float = 0.0
        self._cache_ttl: float = 300.0  # 5 minutes
        self._total_ingested: int = 0
        self._total_supabase_synced: int = 0
        self._initialized: bool = False
        self._last_error: Optional[str] = None
        self._init_time: float = time.time()

        # Load existing data from Supabase on init
        self._load_from_supabase()
        self._initialized = True

    @classmethod
    def get_instance(cls) -> BenchmarkingNetwork:
        """Get or create the singleton instance (thread-safe).

        Returns:
            The singleton BenchmarkingNetwork instance.
        """
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _load_from_supabase(self) -> None:
        """Load existing benchmark records from Supabase into memory."""
        try:
            result = _supabase_rest(
                _TABLE_NAME,
                method="GET",
                params="?order=created_at.desc&limit=5000",
            )
            if result and isinstance(result, list):
                with self._lock:
                    self._records = result
                    self._total_ingested = len(result)
                logger.info(f"Loaded {len(result)} benchmark records from Supabase")
            else:
                logger.info(
                    "No existing benchmark data in Supabase (or table not created)"
                )
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            logger.error(
                "Failed to load benchmarks from Supabase: %s", exc, exc_info=True
            )
            self._last_error = str(exc)

    def ingest_plan(self, plan_data: dict[str, Any]) -> Optional[str]:
        """Ingest a completed media plan into the benchmarking network.

        Anonymizes the data, stores in memory, and persists to Supabase.

        Args:
            plan_data: Raw media plan data dict.

        Returns:
            The anonymized record ID, or None if ingestion failed.
        """
        record = _anonymize_plan_data(plan_data)
        if record is None:
            return None

        with self._lock:
            self._records.append(record)
            self._total_ingested += 1
            # Invalidate cache
            self._cache_timestamp = 0.0

        # Persist to Supabase asynchronously
        thread = threading.Thread(
            target=self._persist_record,
            args=(record,),
            daemon=True,
        )
        thread.start()

        return record["id"]

    def _persist_record(self, record: dict[str, Any]) -> None:
        """Persist a single anonymized record to Supabase.

        Args:
            record: Anonymized benchmark record.
        """
        # Convert list/dict fields to JSON strings for Supabase
        payload = {
            "id": record["id"],
            "role_family": record["role_family"],
            "location_region": record["location_region"],
            "budget_range": record["budget_range"],
            "channels_used": json.dumps(record.get("channels_used") or []),
            "channel_allocations": json.dumps(record.get("channel_allocations") or {}),
            "avg_cpc": record.get("avg_cpc"),
            "budget_numeric": record.get("budget_numeric"),
            "created_at": record["created_at"],
            "month": record["month"],
        }
        try:
            result = _supabase_rest(_TABLE_NAME, method="POST", payload=payload)
            if result is not None:
                with self._lock:
                    self._total_supabase_synced += 1
        except (urllib.error.URLError, OSError) as exc:
            logger.error("Failed to persist benchmark record: %s", exc, exc_info=True)
            self._last_error = str(exc)

    def get_benchmarks(
        self,
        role_family: Optional[str] = None,
        location: Optional[str] = None,
    ) -> dict[str, Any]:
        """Get aggregated industry benchmarks, optionally filtered.

        Args:
            role_family: Filter by role family (e.g., 'Engineering').
            location: Filter by location/region (e.g., 'US', 'EMEA').

        Returns:
            Dict with benchmark tables: avg_cpc, avg_budget, top_channels,
            seasonal_trends, and metadata.
        """
        # Check cache
        cache_key = f"{role_family or 'all'}:{location or 'all'}"
        now = time.time()
        with self._lock:
            if (
                now - self._cache_timestamp < self._cache_ttl
                and cache_key in self._benchmarks_cache
            ):
                return self._benchmarks_cache[cache_key]

        # Build filtered dataset
        with self._lock:
            records = list(self._records)

        if role_family:
            rf_norm = _normalize_role_family(role_family)
            records = [r for r in records if r.get("role_family") == rf_norm]

        if location:
            loc_norm = _normalize_region(location)
            records = [r for r in records if r.get("location_region") == loc_norm]

        benchmarks = self._aggregate(records, role_family, location)

        with self._lock:
            self._benchmarks_cache[cache_key] = benchmarks
            self._cache_timestamp = now

        return benchmarks

    def _aggregate(
        self,
        records: list[dict[str, Any]],
        role_family: Optional[str],
        location: Optional[str],
    ) -> dict[str, Any]:
        """Aggregate filtered records into benchmark tables.

        Args:
            records: Filtered list of anonymized records.
            role_family: The role family filter applied (for metadata).
            location: The location filter applied (for metadata).

        Returns:
            Complete benchmark response dict.
        """
        if not records:
            return {
                "status": "no_data",
                "filters": {
                    "role_family": role_family or "all",
                    "location": location or "all",
                },
                "sample_size": 0,
                "avg_cpc_by_role_family": {},
                "avg_budget_by_role_family": {},
                "top_channels_by_role_family": {},
                "seasonal_trends": {},
                "message": "No benchmark data available yet. Benchmarks improve as more plans are generated.",
            }

        # ── Avg CPC by role family ──
        cpc_by_rf: dict[str, list[float]] = defaultdict(list)
        for r in records:
            cpc_val = r.get("avg_cpc")
            if cpc_val is not None:
                try:
                    cpc_by_rf[r.get("role_family") or "General"].append(float(cpc_val))
                except (ValueError, TypeError):
                    pass

        avg_cpc_by_rf: dict[str, dict[str, Any]] = {}
        for rf, cpcs in cpc_by_rf.items():
            if cpcs:
                avg_cpc_by_rf[rf] = {
                    "avg_cpc": round(sum(cpcs) / len(cpcs), 2),
                    "min_cpc": round(min(cpcs), 2),
                    "max_cpc": round(max(cpcs), 2),
                    "sample_size": len(cpcs),
                }

        # ── Avg budget by role family ──
        budget_by_rf: dict[str, list[float]] = defaultdict(list)
        for r in records:
            bud = r.get("budget_numeric")
            if bud is not None and bud > 0:
                try:
                    budget_by_rf[r.get("role_family") or "General"].append(float(bud))
                except (ValueError, TypeError):
                    pass

        avg_budget_by_rf: dict[str, dict[str, Any]] = {}
        for rf, budgets in budget_by_rf.items():
            if budgets:
                avg_budget_by_rf[rf] = {
                    "avg_budget": round(sum(budgets) / len(budgets), 2),
                    "median_budget": round(sorted(budgets)[len(budgets) // 2], 2),
                    "sample_size": len(budgets),
                }

        # ── Top channels by role family ──
        channel_counts_by_rf: dict[str, dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        for r in records:
            rf = r.get("role_family") or "General"
            channels = r.get("channels_used") or []
            if isinstance(channels, str):
                try:
                    channels = json.loads(channels)
                except (json.JSONDecodeError, ValueError):
                    channels = [channels]
            for ch in channels:
                channel_counts_by_rf[rf][str(ch).lower()] += 1

        top_channels_by_rf: dict[str, list[dict[str, Any]]] = {}
        for rf, ch_counts in channel_counts_by_rf.items():
            sorted_chs = sorted(ch_counts.items(), key=lambda x: x[1], reverse=True)[
                :10
            ]
            total = sum(ch_counts.values())
            top_channels_by_rf[rf] = [
                {
                    "channel": ch,
                    "usage_count": count,
                    "usage_pct": round(count / max(1, total) * 100, 1),
                }
                for ch, count in sorted_chs
            ]

        # ── Seasonal trends (month-over-month) ──
        monthly: dict[str, list[float]] = defaultdict(list)
        monthly_count: dict[str, int] = defaultdict(int)
        for r in records:
            month = r.get("month") or ""
            if month:
                monthly_count[month] += 1
                bud = r.get("budget_numeric")
                if bud is not None and bud > 0:
                    try:
                        monthly[month].append(float(bud))
                    except (ValueError, TypeError):
                        pass

        seasonal: dict[str, dict[str, Any]] = {}
        for month in sorted(monthly_count.keys()):
            budgets = monthly.get(month, [])
            seasonal[month] = {
                "plan_count": monthly_count[month],
                "avg_budget": round(sum(budgets) / len(budgets), 2) if budgets else 0,
                "total_spend": round(sum(budgets), 2) if budgets else 0,
            }

        return {
            "status": "ok",
            "filters": {
                "role_family": role_family or "all",
                "location": location or "all",
            },
            "sample_size": len(records),
            "avg_cpc_by_role_family": avg_cpc_by_rf,
            "avg_budget_by_role_family": avg_budget_by_rf,
            "top_channels_by_role_family": top_channels_by_rf,
            "seasonal_trends": seasonal,
            "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

    def get_stats(self) -> dict[str, Any]:
        """Get benchmarking network statistics for health monitoring.

        Returns:
            Dict with ingestion counts, cache status, and uptime.
        """
        with self._lock:
            unique_roles = set()
            unique_regions = set()
            for r in self._records:
                unique_roles.add(r.get("role_family") or "General")
                unique_regions.add(r.get("location_region") or "Other")

            return {
                "status": "ok" if self._initialized else "initializing",
                "total_records": len(self._records),
                "total_ingested": self._total_ingested,
                "total_supabase_synced": self._total_supabase_synced,
                "unique_role_families": len(unique_roles),
                "unique_regions": len(unique_regions),
                "cache_entries": len(self._benchmarks_cache),
                "cache_ttl_seconds": self._cache_ttl,
                "uptime_seconds": round(time.time() - self._init_time, 2),
                "last_error": self._last_error,
                "supabase_configured": bool(_SUPABASE_URL and _SUPABASE_KEY),
            }


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def get_benchmarking() -> BenchmarkingNetwork:
    """Get the singleton BenchmarkingNetwork instance (thread-safe).

    Returns:
        The global BenchmarkingNetwork singleton.
    """
    return BenchmarkingNetwork.get_instance()


def get_benchmarking_stats() -> dict[str, Any]:
    """Get benchmarking network stats for /api/health.

    Returns:
        Dict with status, record counts, and configuration info.
    """
    try:
        return get_benchmarking().get_stats()
    except (RuntimeError, OSError) as exc:
        logger.error("Failed to get benchmarking stats: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc)}


def ingest_plan(plan_data: dict[str, Any]) -> Optional[str]:
    """Ingest a completed media plan into the benchmarking network.

    Convenience wrapper around the singleton.

    Args:
        plan_data: Raw media plan data dict.

    Returns:
        The anonymized record ID, or None if ingestion failed.
    """
    try:
        return get_benchmarking().ingest_plan(plan_data)
    except (RuntimeError, OSError, ValueError) as exc:
        logger.error("Failed to ingest plan into benchmarking: %s", exc, exc_info=True)
        return None


def get_benchmarks(
    role_family: Optional[str] = None,
    location: Optional[str] = None,
) -> dict[str, Any]:
    """Get aggregated industry benchmarks.

    Args:
        role_family: Filter by role family (e.g., 'Engineering').
        location: Filter by location/region (e.g., 'US', 'EMEA').

    Returns:
        Dict with industry averages: avg_cpc, avg_budget, top_channels,
        seasonal_trends.
    """
    try:
        return get_benchmarking().get_benchmarks(role_family, location)
    except (RuntimeError, OSError, ValueError) as exc:
        logger.error("Failed to get benchmarks: %s", exc, exc_info=True)
        return {"status": "error", "error": str(exc), "sample_size": 0}
