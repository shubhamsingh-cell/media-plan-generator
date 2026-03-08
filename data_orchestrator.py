"""
data_orchestrator.py -- Unified Data Access Layer (v2)

Single entry point for enriched data queries that cascade through all
available data sources in order of cost and speed:

    1. research.py embedded data   (free, instant, 40+ countries, 100+ metros)
    2. Selective live API calls     (individual APIs, cached 24h)
    3. Static KB fallback           (JSON files, always available)

v2 upgrades (DeepMind AI + Meta Data Science perspectives):
    - Additive cascade (merges best data from all sources, not first-wins)
    - Confidence scoring (0.0-1.0) on every data point for AI reasoning
    - Data freshness metadata on all returns
    - Tier-aware salary fallbacks (executive/professional/skilled/entry)
    - API return validation before caching (prevents cache poisoning)
    - Parallel fetches within enrich_* functions (ThreadPoolExecutor)
    - Input normalization for cache keys (prevents case-sensitivity misses)
    - LRU cache with access tracking and batch eviction
    - Request deduplication (in-flight coalescing)
    - Session-scoped enrichment context (cross-tool data sharing)
    - Computed insights layer (hiring difficulty, salary competitiveness,
      days_until_next_peak_hiring)
    - Employer brand intelligence (30+ major employers)
    - Real-time job posting volume surfacing
    - Ad platform benchmark passthrough (Google/Meta/LinkedIn CPC/CPM/CTR)
    - Fallback telemetry (tracks which queries hit generic fallback)

Thread-safe, lazy-loading, cached.  Never crashes -- all errors are caught
and the caller always receives a usable dict.

Consumers:
    - nova.py       (chatbot tool handlers)
    - nova_slack.py (Slack bot)
    - ppt_generator.py
    - app.py        (generation pipeline -- also has its own richer bulk flow)
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# LAZY MODULE LOADING (thread-safe, avoids circular imports)
# ═══════════════════════════════════════════════════════════════════════════════

_research = None
_standardizer = None
_api_enrichment = None
_budget_engine = None
_load_lock = threading.Lock()

# Sentinel for "tried to import and failed"
_IMPORT_FAILED = object()


def _lazy_research():
    """Thread-safe lazy import of research.py."""
    global _research
    if _research is None:
        with _load_lock:
            if _research is None:
                try:
                    import research as _r
                    _research = _r
                    logger.info("data_orchestrator: research module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: research import failed: %s", e)
                    _research = _IMPORT_FAILED
    return _research if _research is not _IMPORT_FAILED else None


def _lazy_standardizer():
    """Thread-safe lazy import of standardizer.py."""
    global _standardizer
    if _standardizer is None:
        with _load_lock:
            if _standardizer is None:
                try:
                    import standardizer as _s
                    _standardizer = _s
                    logger.info("data_orchestrator: standardizer module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: standardizer import failed: %s", e)
                    _standardizer = _IMPORT_FAILED
    return _standardizer if _standardizer is not _IMPORT_FAILED else None


def _lazy_api():
    """Thread-safe lazy import of api_enrichment.py."""
    global _api_enrichment
    if _api_enrichment is None:
        with _load_lock:
            if _api_enrichment is None:
                try:
                    import api_enrichment as _a
                    _api_enrichment = _a
                    logger.info("data_orchestrator: api_enrichment module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: api_enrichment import failed: %s", e)
                    _api_enrichment = _IMPORT_FAILED
    return _api_enrichment if _api_enrichment is not _IMPORT_FAILED else None


def _lazy_budget():
    """Thread-safe lazy import of budget_engine.py."""
    global _budget_engine
    if _budget_engine is None:
        with _load_lock:
            if _budget_engine is None:
                try:
                    import budget_engine as _b
                    _budget_engine = _b
                    logger.info("data_orchestrator: budget_engine module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: budget_engine import failed: %s", e)
                    _budget_engine = _IMPORT_FAILED
    return _budget_engine if _budget_engine is not _IMPORT_FAILED else None


# ═══════════════════════════════════════════════════════════════════════════════
# LRU CACHE (access-tracked, normalized keys, batch eviction)
# ═══════════════════════════════════════════════════════════════════════════════

_api_result_cache: Dict[str, Dict[str, Any]] = {}
_api_cache_lock = threading.Lock()
_API_CACHE_TTL = 24 * 3600  # 24 hours
_MAX_CACHE_ENTRIES = 500


def _normalize_cache_key(raw: str) -> str:
    """Normalize raw input for cache key consistency.

    Prevents misses like 'Software Engineer' vs 'software engineer'
    vs '  Software  Engineer '.
    """
    return " ".join(raw.lower().split())


def _cache_get(domain: str, key: str) -> Optional[Any]:
    """Get cached API result with LRU access tracking.  Returns None if miss."""
    full_key = f"{domain}:{_normalize_cache_key(key)}"
    with _api_cache_lock:
        entry = _api_result_cache.get(full_key)
        if entry and time.time() < entry.get("expires", 0):
            entry["last_access"] = time.time()
            entry["access_count"] = entry.get("access_count", 0) + 1
            return entry["data"]
        elif entry:
            _api_result_cache.pop(full_key, None)
    return None


def _cache_get_with_age(domain: str, key: str) -> tuple:
    """Like _cache_get but also returns cache age in seconds.

    Returns (data, age_seconds) or (None, 0).
    """
    full_key = f"{domain}:{_normalize_cache_key(key)}"
    with _api_cache_lock:
        entry = _api_result_cache.get(full_key)
        if entry and time.time() < entry.get("expires", 0):
            entry["last_access"] = time.time()
            entry["access_count"] = entry.get("access_count", 0) + 1
            age = time.time() - entry.get("created", time.time())
            return entry["data"], age
        elif entry:
            _api_result_cache.pop(full_key, None)
    return None, 0


def _cache_set(domain: str, key: str, data: Any,
               ttl: int = _API_CACHE_TTL) -> None:
    """Cache an API result with TTL.  LRU eviction on overflow."""
    full_key = f"{domain}:{_normalize_cache_key(key)}"
    now = time.time()
    with _api_cache_lock:
        _api_result_cache[full_key] = {
            "data": data,
            "expires": now + ttl,
            "created": now,
            "last_access": now,
            "access_count": 1,
        }
        if len(_api_result_cache) > _MAX_CACHE_ENTRIES:
            # Batch evict: remove 10% least-recently-used entries
            to_evict = max(1, _MAX_CACHE_ENTRIES // 10)
            sorted_keys = sorted(
                _api_result_cache,
                key=lambda k: _api_result_cache[k].get("last_access", 0),
            )
            for ek in sorted_keys[:to_evict]:
                _api_result_cache.pop(ek, None)


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST DEDUPLICATION (in-flight coalescing)
# ═══════════════════════════════════════════════════════════════════════════════

_inflight: Dict[str, threading.Event] = {}
_inflight_results: Dict[str, Any] = {}
_inflight_lock = threading.Lock()


def _dedup_fetch(domain: str, key: str, fetch_fn):
    """If an identical request is already in-flight, wait for its result.

    Prevents duplicate API calls when two threads request the same data
    concurrently (e.g. two chat sessions asking about the same role).
    """
    full_key = f"{domain}:{_normalize_cache_key(key)}"
    is_owner = False
    event = None

    with _inflight_lock:
        if full_key in _inflight:
            # Another thread is already fetching this -- wait for it
            event = _inflight[full_key]
        else:
            # We are the first -- register and proceed
            event = threading.Event()
            _inflight[full_key] = event
            is_owner = True

    if not is_owner:
        # Wait up to 30s for the other thread to finish
        event.wait(timeout=30)
        with _inflight_lock:
            return _inflight_results.get(full_key)

    # We are the owner -- execute the fetch
    try:
        result = fetch_fn()
        with _inflight_lock:
            _inflight_results[full_key] = result
        return result
    except Exception as e:
        logger.debug("_dedup_fetch %s failed: %s", full_key, e)
        return None
    finally:
        with _inflight_lock:
            _inflight.pop(full_key, None)
            event.set()  # Wake all waiters (inside lock to prevent TOCTOU)
            # Deferred cleanup: remove result after waiters have read it
            # Use a short-lived thread to avoid blocking
            _result_key = full_key

            def _cleanup(rk=_result_key):
                time.sleep(2)
                with _inflight_lock:
                    _inflight_results.pop(rk, None)
            t = threading.Thread(target=_cleanup, daemon=True)
            t.start()


# ═══════════════════════════════════════════════════════════════════════════════
# FALLBACK TELEMETRY (tracks which queries hit generic fallback)
# ═══════════════════════════════════════════════════════════════════════════════

_fallback_counts: Dict[str, int] = {}
_fallback_lock = threading.Lock()


def _record_fallback(function_name: str, query: str) -> None:
    """Track which queries hit generic fallback for data expansion prioritization."""
    with _fallback_lock:
        fb_key = f"{function_name}:{_normalize_cache_key(query)}"
        _fallback_counts[fb_key] = _fallback_counts.get(fb_key, 0) + 1


def get_fallback_telemetry() -> Dict[str, Any]:
    """Return fallback hit counts sorted by frequency (highest first).

    Useful for identifying which roles/locations/industries need expanded
    data coverage.
    """
    with _fallback_lock:
        sorted_items = sorted(
            _fallback_counts.items(), key=lambda x: x[1], reverse=True,
        )
        return {
            "total_fallbacks": sum(v for _, v in sorted_items),
            "unique_queries": len(sorted_items),
            "top_fallbacks": dict(sorted_items[:20]),
        }


def get_cache_stats() -> Dict[str, Any]:
    """Return cache statistics for monitoring."""
    with _api_cache_lock:
        now = time.time()
        total = len(_api_result_cache)
        expired = sum(
            1 for e in _api_result_cache.values()
            if now >= e.get("expires", 0)
        )
        total_accesses = sum(
            e.get("access_count", 0) for e in _api_result_cache.values()
        )
        return {
            "total_entries": total,
            "expired_entries": expired,
            "active_entries": total - expired,
            "total_accesses": total_accesses,
            "max_entries": _MAX_CACHE_ENTRIES,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# API RETURN DATA VALIDATION (prevents cache poisoning from bad API responses)
# ═══════════════════════════════════════════════════════════════════════════════

def _is_valid_salary_data(data: Any) -> bool:
    """Validate salary API response has meaningful content before caching."""
    if not isinstance(data, dict):
        return False
    for key in ("median", "p50", "annual_salary", "salary",
                "p25", "p75", "p10", "p90", "mean"):
        val = data.get(key)
        if isinstance(val, (int, float)) and val > 0:
            return True
    return False


def _is_valid_location_data(data: Any) -> bool:
    """Validate location API response before caching."""
    if not isinstance(data, dict):
        return False
    pop = data.get("population")
    if isinstance(pop, (int, float)) and pop > 0:
        return True
    if data.get("country") or data.get("state") or data.get("median_income"):
        return True
    return False


def _is_valid_job_market_data(data: Any) -> bool:
    """Validate job market API response before caching."""
    if not isinstance(data, dict):
        return False
    for key in ("count", "total_jobs", "average_salary", "cpc", "cpa",
                "results", "current_posting_count", "posting_count"):
        if data.get(key) is not None:
            return True
    # Reject error responses that happen to have 2+ keys
    if data.get("error") or data.get("status") in (400, 500, 502, 503):
        return False
    return len(data) >= 2


def _is_valid_company_data(data: Any) -> bool:
    """Validate company metadata before caching."""
    if not isinstance(data, dict) or not data:
        return False
    return bool(data.get("name") or data.get("company_name")
                or data.get("description") or data.get("ticker")
                or data.get("industry") or data.get("summary"))


# ═══════════════════════════════════════════════════════════════════════════════
# THREAD POOL FOR PARALLEL FETCHES
# ═══════════════════════════════════════════════════════════════════════════════

_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="orch")


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION-SCOPED ENRICHMENT CONTEXT
# ═══════════════════════════════════════════════════════════════════════════════

class EnrichmentContext:
    """Accumulates enrichment data across tool calls within a single conversation.

    Enables cross-enrichment synthesis: budget tool can access salary and market
    data discovered by earlier tool calls in the same session, producing more
    accurate allocations without redundant API calls.

    Usage:
        ctx = EnrichmentContext()
        salary = enrich_salary("nurse", "Houston", context=ctx)
        market = enrich_market_demand("nurse", "Houston", context=ctx)
        budget = enrich_budget(50000, roles, locations, context=ctx)
        # budget now has access to salary + market data from this session
    """

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._created = time.time()

    def store(self, key: str, value: Any) -> None:
        """Store enrichment result under a key."""
        with self._lock:
            self._data[key] = {"value": value, "stored_at": time.time()}

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve enrichment result by key."""
        with self._lock:
            entry = self._data.get(key)
            return entry["value"] if entry else default

    def get_all(self) -> Dict[str, Any]:
        """Return all stored enrichment data (values only)."""
        with self._lock:
            return {k: v["value"] for k, v in self._data.items()}

    @property
    def salary_data(self) -> Optional[Dict]:
        return self.get("salary")

    @property
    def location_data(self) -> Optional[Dict]:
        return self.get("location")

    @property
    def market_data(self) -> Optional[Dict]:
        return self.get("market_demand")

    @property
    def competitive_data(self) -> Optional[Dict]:
        return self.get("competitive")

    @property
    def employer_brand(self) -> Optional[Dict]:
        return self.get("employer_brand")

    @property
    def age_seconds(self) -> float:
        return time.time() - self._created


# ═══════════════════════════════════════════════════════════════════════════════
# TIER-AWARE SALARY FALLBACK RANGES (replaces hardcoded $45k-$80k)
# ═══════════════════════════════════════════════════════════════════════════════

_TIER_SALARY_RANGES = {
    "executive":    {"low": 120000, "high": 250000, "median": 175000,
                     "label": "$120,000 - $250,000"},
    "professional": {"low": 65000,  "high": 120000, "median": 90000,
                     "label": "$65,000 - $120,000"},
    "skilled":      {"low": 40000,  "high": 70000,  "median": 52000,
                     "label": "$40,000 - $70,000"},
    "entry":        {"low": 28000,  "high": 45000,  "median": 35000,
                     "label": "$28,000 - $45,000"},
    # Fallback when tier itself is unknown
    "":             {"low": 40000,  "high": 80000,  "median": 55000,
                     "label": "$40,000 - $80,000"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# AD PLATFORM BENCHMARK DATA (for Nova passthrough -- previously bulk-only)
# ═══════════════════════════════════════════════════════════════════════════════

_AD_PLATFORM_BENCHMARKS = {
    "healthcare_medical": {
        "google_ads":   {"cpc_range": "$1.50-$4.50", "cpm_range": "$15-$35",
                         "avg_ctr": "3.2%"},
        "meta_ads":     {"cpc_range": "$0.80-$2.50", "cpm_range": "$12-$28",
                         "avg_ctr": "1.8%"},
        "linkedin_ads": {"cpc_range": "$3.00-$8.00", "cpm_range": "$25-$55",
                         "avg_ctr": "0.5%"},
        "indeed":       {"cpc_range": "$0.25-$1.50", "cpa_range": "$8-$35",
                         "avg_ctr": "4.5%"},
        "programmatic": {"cpc_range": "$0.15-$0.80", "cpa_range": "$5-$25",
                         "avg_ctr": "0.8%"},
    },
    "tech_engineering": {
        "google_ads":   {"cpc_range": "$2.00-$6.00", "cpm_range": "$20-$45",
                         "avg_ctr": "2.8%"},
        "meta_ads":     {"cpc_range": "$1.00-$3.50", "cpm_range": "$15-$35",
                         "avg_ctr": "1.5%"},
        "linkedin_ads": {"cpc_range": "$5.00-$12.00", "cpm_range": "$30-$70",
                         "avg_ctr": "0.4%"},
        "indeed":       {"cpc_range": "$0.50-$2.50", "cpa_range": "$15-$50",
                         "avg_ctr": "3.8%"},
        "programmatic": {"cpc_range": "$0.20-$1.20", "cpa_range": "$8-$35",
                         "avg_ctr": "0.7%"},
    },
    "finance_banking": {
        "google_ads":   {"cpc_range": "$2.50-$7.00", "cpm_range": "$22-$50",
                         "avg_ctr": "2.5%"},
        "meta_ads":     {"cpc_range": "$1.20-$3.80", "cpm_range": "$18-$40",
                         "avg_ctr": "1.4%"},
        "linkedin_ads": {"cpc_range": "$4.50-$10.00", "cpm_range": "$28-$65",
                         "avg_ctr": "0.45%"},
        "indeed":       {"cpc_range": "$0.40-$2.00", "cpa_range": "$12-$45",
                         "avg_ctr": "4.0%"},
        "programmatic": {"cpc_range": "$0.18-$1.00", "cpa_range": "$7-$30",
                         "avg_ctr": "0.75%"},
    },
    "retail_consumer": {
        "google_ads":   {"cpc_range": "$0.80-$2.50", "cpm_range": "$8-$20",
                         "avg_ctr": "3.8%"},
        "meta_ads":     {"cpc_range": "$0.50-$1.80", "cpm_range": "$8-$18",
                         "avg_ctr": "2.2%"},
        "linkedin_ads": {"cpc_range": "$2.50-$6.00", "cpm_range": "$18-$40",
                         "avg_ctr": "0.5%"},
        "indeed":       {"cpc_range": "$0.15-$0.80", "cpa_range": "$5-$20",
                         "avg_ctr": "5.0%"},
        "programmatic": {"cpc_range": "$0.10-$0.50", "cpa_range": "$3-$15",
                         "avg_ctr": "0.9%"},
    },
    "blue_collar_trades": {
        "google_ads":   {"cpc_range": "$0.60-$2.00", "cpm_range": "$6-$18",
                         "avg_ctr": "4.0%"},
        "meta_ads":     {"cpc_range": "$0.40-$1.50", "cpm_range": "$6-$15",
                         "avg_ctr": "2.5%"},
        "linkedin_ads": {"cpc_range": "$2.00-$5.00", "cpm_range": "$15-$35",
                         "avg_ctr": "0.5%"},
        "indeed":       {"cpc_range": "$0.12-$0.60", "cpa_range": "$4-$18",
                         "avg_ctr": "5.2%"},
        "programmatic": {"cpc_range": "$0.08-$0.40", "cpa_range": "$3-$12",
                         "avg_ctr": "1.0%"},
    },
    "aerospace_defense": {
        "google_ads":   {"cpc_range": "$2.00-$5.50", "cpm_range": "$18-$40",
                         "avg_ctr": "2.6%"},
        "meta_ads":     {"cpc_range": "$1.00-$3.00", "cpm_range": "$12-$30",
                         "avg_ctr": "1.3%"},
        "linkedin_ads": {"cpc_range": "$4.00-$9.00", "cpm_range": "$25-$60",
                         "avg_ctr": "0.45%"},
        "indeed":       {"cpc_range": "$0.40-$2.00", "cpa_range": "$12-$40",
                         "avg_ctr": "3.5%"},
        "programmatic": {"cpc_range": "$0.15-$0.90", "cpa_range": "$6-$28",
                         "avg_ctr": "0.7%"},
    },
    "pharma_biotech": {
        "google_ads":   {"cpc_range": "$2.50-$6.50", "cpm_range": "$20-$45",
                         "avg_ctr": "2.4%"},
        "meta_ads":     {"cpc_range": "$1.00-$3.50", "cpm_range": "$14-$32",
                         "avg_ctr": "1.5%"},
        "linkedin_ads": {"cpc_range": "$4.50-$10.00", "cpm_range": "$28-$60",
                         "avg_ctr": "0.4%"},
        "indeed":       {"cpc_range": "$0.40-$2.00", "cpa_range": "$12-$40",
                         "avg_ctr": "3.8%"},
        "programmatic": {"cpc_range": "$0.18-$1.00", "cpa_range": "$7-$30",
                         "avg_ctr": "0.7%"},
    },
    "logistics_supply_chain": {
        "google_ads":   {"cpc_range": "$0.80-$2.80", "cpm_range": "$8-$22",
                         "avg_ctr": "3.5%"},
        "meta_ads":     {"cpc_range": "$0.50-$1.80", "cpm_range": "$7-$18",
                         "avg_ctr": "2.0%"},
        "linkedin_ads": {"cpc_range": "$2.50-$6.00", "cpm_range": "$18-$40",
                         "avg_ctr": "0.5%"},
        "indeed":       {"cpc_range": "$0.15-$0.80", "cpa_range": "$5-$22",
                         "avg_ctr": "4.8%"},
        "programmatic": {"cpc_range": "$0.10-$0.50", "cpa_range": "$3-$15",
                         "avg_ctr": "0.9%"},
    },
    # Default for unrecognized industries
    "_default": {
        "google_ads":   {"cpc_range": "$1.00-$4.00", "cpm_range": "$12-$30",
                         "avg_ctr": "3.0%"},
        "meta_ads":     {"cpc_range": "$0.70-$2.50", "cpm_range": "$10-$25",
                         "avg_ctr": "1.8%"},
        "linkedin_ads": {"cpc_range": "$3.50-$8.00", "cpm_range": "$22-$50",
                         "avg_ctr": "0.5%"},
        "indeed":       {"cpc_range": "$0.25-$1.50", "cpa_range": "$8-$30",
                         "avg_ctr": "4.2%"},
        "programmatic": {"cpc_range": "$0.12-$0.70", "cpa_range": "$5-$22",
                         "avg_ctr": "0.8%"},
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# SEASONAL PEAK CALENDAR (for computed insights)
# ═══════════════════════════════════════════════════════════════════════════════

_INDUSTRY_PEAK_MONTHS = {
    "healthcare_medical":       [1, 2, 9, 10],
    "tech_engineering":         [1, 2, 3, 9, 10],
    "retail_consumer":          [8, 9, 10],
    "finance_banking":          [1, 2, 3, 9],
    "blue_collar_trades":       [3, 4, 5, 9, 10],
    "aerospace_defense":        [1, 2, 10, 11],
    "general_entry_level":      [1, 5, 6, 9],
    "pharma_biotech":           [1, 2, 3, 9, 10],
    "logistics_supply_chain":   [8, 9, 10, 11],
    "hospitality_tourism":      [2, 3, 4, 9],
    "mental_health":            [1, 2, 9, 10],
    "legal_services":           [1, 8, 9],
    "maritime_marine":          [3, 4, 5, 9],
}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA FRESHNESS CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_freshness(sources: List[str]) -> str:
    """Classify overall data freshness from source list."""
    if not sources:
        return "fallback"
    source_str = " ".join(sources).lower()
    if "live" in source_str:
        return "live_api"
    if "cache" in source_str and ("research" in source_str
                                  or "curated" in source_str):
        return "curated+cached_api"
    if "cache" in source_str:
        return "cached_api"
    if "research" in source_str or "curated" in source_str:
        return "curated"
    if "fallback" in source_str or "generic" in source_str:
        return "fallback"
    return "mixed"


# ═══════════════════════════════════════════════════════════════════════════════
# COMPUTED INSIGHTS HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_hiring_difficulty_index(
    unemployment_rate: Optional[float] = None,
    competition_count: int = 0,
    role_tier: str = "",
    supply_demand_ratio: Optional[float] = None,
) -> float:
    """Compute 0.0 (easy) to 1.0 (extremely difficult) hiring difficulty index.

    Factors:
      - Low unemployment = harder to hire
      - More competitors = harder
      - Higher-tier roles = harder
      - Low supply vs demand = harder
    """
    score = 0.0
    signals = 0

    # Unemployment factor (lower unemployment = harder hiring)
    if unemployment_rate is not None and unemployment_rate > 0:
        unemp_factor = max(0.0, min(1.0, 1.0 - (unemployment_rate - 2.0) / 8.0))
        score += unemp_factor
        signals += 1

    # Competition factor
    if competition_count > 0:
        comp_factor = min(1.0, competition_count / 6.0)
        score += comp_factor
        signals += 1

    # Tier factor (executive roles are hardest to fill)
    tier_difficulty = {
        "executive": 0.9, "professional": 0.6,
        "skilled": 0.4, "entry": 0.2,
    }
    tier_lower = role_tier.lower() if role_tier else ""
    if tier_lower in tier_difficulty:
        score += tier_difficulty[tier_lower]
        signals += 1

    # Supply/demand ratio (< 1.0 means more demand than supply = harder)
    if supply_demand_ratio is not None and supply_demand_ratio > 0:
        sd_factor = max(0.0, min(1.0, 1.0 - (supply_demand_ratio - 0.5) / 2.0))
        score += sd_factor
        signals += 1

    if signals == 0:
        return 0.5  # No data -> neutral

    return round(min(1.0, score / signals), 2)


def _compute_salary_competitiveness(
    offered_low: float, offered_high: float,
    market_median: float,
) -> float:
    """0.0 (far below market) to 1.0 (well above market) competitiveness score.

    0.5 = exactly at market median.
    """
    if market_median <= 0:
        return 0.5
    offered_mid = (offered_low + offered_high) / 2.0
    ratio = offered_mid / market_median
    # Map ratio: 0.7 -> 0.0, 1.0 -> 0.5, 1.3 -> 1.0
    score = (ratio - 0.7) / 0.6
    return round(max(0.0, min(1.0, score)), 2)


def _days_until_next_peak(industry: str) -> Optional[int]:
    """Days until next peak hiring window for the industry.  None if unknown."""
    peaks = _INDUSTRY_PEAK_MONTHS.get(industry)
    if not peaks:
        return None

    today = datetime.now()
    current_month = today.month

    # Find next peak month
    for month in sorted(peaks):
        if month > current_month:
            target = today.replace(month=month, day=1)
            return max(0, (target - today).days)
        if month == current_month and today.day <= 15:
            return 0  # We are in a peak month

    # Wrap to next year's first peak
    first_peak = min(peaks)
    target = today.replace(year=today.year + 1, month=first_peak, day=1)
    return max(0, (target - today).days)


# ═══════════════════════════════════════════════════════════════════════════════
# INPUT NORMALIZATION (uses standardizer.py)
# ═══════════════════════════════════════════════════════════════════════════════

def normalize(industry: str = "", location: str = "",
              role: str = "") -> Dict[str, Any]:
    """Normalize raw user inputs to canonical taxonomy forms.

    Returns dict with canonical values for each provided input:
        industry  -> canonical industry key
        location  -> {city, state, country}
        role      -> canonical role name
        soc_code  -> SOC code (if role given)
        role_tier -> tier classification (if role given)
        channels_key -> key for channels_db.json lookup (if industry given)
    """
    std = _lazy_standardizer()
    result: Dict[str, Any] = {}

    if industry:
        if std:
            try:
                result["industry"] = std.normalize_industry(industry)
                result["channels_key"] = std.get_channels_key(result["industry"])
            except Exception:
                result["industry"] = industry
        else:
            result["industry"] = industry

    if location:
        if std:
            try:
                result["location"] = std.normalize_location(location)
            except Exception:
                result["location"] = {"city": location, "state": "", "country": ""}
        else:
            result["location"] = {"city": location, "state": "", "country": ""}

    if role:
        if std:
            try:
                result["role"] = std.normalize_role(role)
                result["soc_code"] = std.get_soc_code(role)
                result["role_tier"] = std.get_role_tier(role)
            except Exception:
                result["role"] = role
        else:
            result["role"] = role

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SALARY INTELLIGENCE (v2: confidence, tier-aware fallback, parallel, validation)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_salary(role: str, location: str = "",
                  industry: str = "",
                  context: Optional[EnrichmentContext] = None) -> Dict[str, Any]:
    """Enriched salary data with confidence scoring.

    Cascades additively:
        research.py (COLI-adjusted, BLS-augmented) + live BLS API -> tier fallback.

    Returns:
        {role, location, salary_range, median_salary, coli, role_tier,
         bls_percentiles, source, confidence, data_freshness, sources_used}
    """
    result: Dict[str, Any] = {"role": role, "location": location or "National"}
    sources_used: List[str] = []
    confidence = 0.0

    # -- 1. Role tier from standardizer (needed for fallback) ------------------
    std = _lazy_standardizer()
    role_tier = ""
    if std:
        try:
            role_tier = std.get_role_tier(role)
        except Exception:
            pass
    result["role_tier"] = role_tier or "Professional"

    # -- 2. Location context + BLS API in parallel ----------------------------
    res = _lazy_research()
    api = _lazy_api()

    # Submit parallel tasks
    location_future = None
    bls_future = None

    if res and location:
        location_future = _executor.submit(_fetch_location_context, res, location)

    bls_data: Optional[Dict] = None
    cache_key = role.lower().strip()
    cached_sal, cache_age = _cache_get_with_age("salary", cache_key)
    if cached_sal is not None:
        bls_data = cached_sal
        sources_used.append("BLS API (cached)")
        confidence = max(confidence, 0.75 if cache_age < 43200 else 0.65)
    elif api:
        bls_future = _executor.submit(
            _dedup_fetch, "salary_fetch", cache_key,
            lambda: _fetch_bls_salary(api, role),
        )

    # Collect location context
    coli = 100
    if location_future:
        try:
            location_meta = location_future.result(timeout=15) or {}
            if location_meta:
                coli = location_meta.get("coli", 100)
                result["coli"] = coli
                result["country"] = location_meta.get("country", "United States")
                result["metro_name"] = location_meta.get("metro_name", location)
                result["currency"] = location_meta.get("currency", "USD")
                sources_used.append("Research Intelligence (location)")
        except Exception as e:
            logger.debug("enrich_salary: location context failed: %s", e)
    result.setdefault("coli", coli)

    # Collect BLS data
    if bls_future:
        try:
            bls_result = bls_future.result(timeout=20)
            if bls_result and _is_valid_salary_data(bls_result):
                bls_data = bls_result
                _cache_set("salary", cache_key, bls_data)
                sources_used.append("BLS API (live)")
                confidence = max(confidence, 0.85)
            elif bls_result:
                logger.debug("enrich_salary: BLS data failed validation, not caching")
        except Exception as e:
            logger.debug("enrich_salary: BLS fetch failed: %s", e)

    # -- 3. Build salary range using research.py cascade -----------------------
    if res:
        try:
            enrichment_map = {role: bls_data} if bls_data else None
            salary_range = res.get_role_salary_range(
                role, location_coli=coli,
                enrichment_salary_data=enrichment_map,
            )
            result["salary_range"] = salary_range
            if bls_data:
                result["source"] = "BLS API + COLI-adjusted"
                confidence = max(confidence, 0.90)
            else:
                result["source"] = "Curated Industry Data + COLI-adjusted"
                confidence = max(confidence, 0.80)
            sources_used.append("Research salary ranges")
        except Exception as e:
            logger.debug("enrich_salary: get_role_salary_range failed: %s", e)

    # Tier-aware fallback if research.py didn't produce a range
    if "salary_range" not in result:
        if bls_data and bls_data.get("median"):
            median = int(bls_data["median"] * (coli / 100.0))
            low, high = int(median * 0.75), int(median * 1.30)
            result["salary_range"] = f"${low:,} - ${high:,}"
            result["median_salary"] = median
            result["source"] = "BLS API"
            confidence = max(confidence, 0.80)
        else:
            # U3: Tier-aware fallback instead of hardcoded $45k-$80k
            tier_key = role_tier.lower() if role_tier else ""
            tier_range = _TIER_SALARY_RANGES.get(
                tier_key, _TIER_SALARY_RANGES[""])
            adjusted_low = int(tier_range["low"] * (coli / 100.0))
            adjusted_high = int(tier_range["high"] * (coli / 100.0))
            adjusted_median = int(tier_range["median"] * (coli / 100.0))
            result["salary_range"] = f"${adjusted_low:,} - ${adjusted_high:,}"
            result["median_salary"] = adjusted_median
            result["source"] = f"Tier-based Estimate ({result['role_tier']})"
            confidence = max(confidence, 0.35)
            _record_fallback("enrich_salary", f"{role}|{location}")
            sources_used.append(f"Tier fallback ({result['role_tier']})")

    # -- 4. BLS percentile data (compact, for Claude to reason over) -----------
    if bls_data:
        bls_compact: Dict[str, Any] = {}
        for k in ("median", "p10", "p25", "p75", "p90",
                   "employment", "soc_code"):
            v = bls_data.get(k)
            if v is not None:
                bls_compact[k] = v
        if bls_compact:
            result["bls_percentiles"] = bls_compact

    # -- 5. Confidence and freshness metadata ----------------------------------
    result["confidence"] = round(confidence, 2)
    result["data_freshness"] = _classify_freshness(sources_used)
    result["sources_used"] = sources_used

    if context is not None:
        context.store("salary", result)

    return result


def _fetch_location_context(res, location: str) -> Optional[Dict]:
    """Helper for parallel location info fetch."""
    try:
        return res.get_location_info(location) or {}
    except Exception:
        return {}


def _fetch_bls_salary(api, role: str) -> Optional[Dict]:
    """Helper for parallel BLS salary fetch."""
    try:
        raw = api.fetch_salary_data([role])
        if isinstance(raw, dict) and raw:
            data = raw.get(role)
            if not data:
                first_key = next(iter(raw), None)
                if first_key:
                    data = raw[first_key]
            return data
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION INTELLIGENCE (v2: ADDITIVE cascade, confidence, parallel)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_location(location: str,
                    context: Optional[EnrichmentContext] = None) -> Dict[str, Any]:
    """Enriched location profile with ADDITIVE cascade.

    Merges data from ALL available sources (research.py base + API overlay)
    instead of returning early from the first hit.

    Returns:
        {location, metro_name, country, region, coli, population,
         median_salary, unemployment, currency, major_employers,
         top_boards, is_international, recommended_boards, source,
         confidence, data_freshness, sources_used}
    """
    result: Dict[str, Any] = {"location": location}
    sources_used: List[str] = []
    confidence = 0.0

    # -- 1. research.py (rich embedded data -- always the base layer) ----------
    res = _lazy_research()
    if res:
        try:
            info = res.get_location_info(location)
            if info:
                result.update(info)
                sources_used.append("Research Intelligence")
                confidence = max(confidence, 0.88)
                # Also get recommended boards
                try:
                    boards = res.get_location_boards([location])
                    if boards:
                        result["recommended_boards"] = boards
                except Exception:
                    pass
                # U1: DO NOT return here -- continue to overlay API data
        except Exception as e:
            logger.debug("enrich_location: get_location_info failed: %s", e)

    # -- 2. Census / World Bank API (additive overlay, parallel) ---------------
    api = _lazy_api()
    if api:
        loc_key = location.lower().strip()
        cached_loc, cache_age = _cache_get_with_age("location", loc_key)
        if cached_loc:
            # Merge cached API data (don't overwrite research.py base)
            for k, v in cached_loc.items():
                if v and (k not in result or not result[k]
                          or result.get(k) == "Data not available"):
                    result[k] = v
            sources_used.append("API Cache (location)")
            confidence = max(confidence, 0.72 if cache_age < 43200 else 0.60)
        else:
            # Submit Census and World Bank fetches in parallel
            census_future = _executor.submit(
                _fetch_census_data, api, location)
            wb_future = _executor.submit(
                _fetch_world_bank_data, api, location)

            api_overlay: Dict[str, Any] = {}

            try:
                census_result = census_future.result(timeout=15)
                if census_result and _is_valid_location_data(census_result):
                    api_overlay.update(census_result)
                    sources_used.append("US Census API")
                    confidence = max(confidence, 0.82)
            except Exception as e:
                logger.debug("enrich_location: census fetch failed: %s", e)

            try:
                wb_result = wb_future.result(timeout=15)
                if wb_result and _is_valid_location_data(wb_result):
                    for k, v in wb_result.items():
                        if v and k not in api_overlay:
                            api_overlay[k] = v
                    if "US Census API" not in sources_used:
                        sources_used.append("World Bank API")
                        confidence = max(confidence, 0.78)
            except Exception as e:
                logger.debug("enrich_location: world bank fetch failed: %s", e)

            # Merge API overlay (don't overwrite research.py base)
            if api_overlay:
                for k, v in api_overlay.items():
                    if v and (k not in result or not result[k]
                              or result.get(k) == "Data not available"):
                        result[k] = v
                if _is_valid_location_data(api_overlay):
                    _cache_set("location", loc_key, api_overlay)

    # -- 3. Generic fallback for missing fields --------------------------------
    defaults = {
        "coli": 100,
        "metro_name": location,
        "country": "United States",
    }
    for dk, dv in defaults.items():
        result.setdefault(dk, dv)

    if not sources_used:
        result["source"] = "Generic Estimate"
        confidence = 0.20
        _record_fallback("enrich_location", location)
        sources_used.append("Generic fallback")
    else:
        result["source"] = " + ".join(sources_used)

    result["confidence"] = round(confidence, 2)
    result["data_freshness"] = _classify_freshness(sources_used)
    result["sources_used"] = sources_used

    if context is not None:
        context.store("location", result)

    return result


def _fetch_census_data(api, location: str) -> Optional[Dict]:
    """Helper for parallel Census API fetch."""
    try:
        demo = api.fetch_location_demographics([location])
        if isinstance(demo, dict):
            for _k, ld in demo.items():
                if isinstance(ld, dict) and ld.get("population"):
                    return {
                        "population": ld.get("population"),
                        "median_salary": ld.get("median_income", 0),
                        "country": "United States",
                    }
    except Exception:
        pass
    return None


def _fetch_world_bank_data(api, location: str) -> Optional[Dict]:
    """Helper for parallel World Bank API fetch.

    fetch_global_indicators returns {country: {unemployment_rate, gdp_growth,
    labor_force}} -- NOT population.  Population comes from
    fetch_location_demographics which handles non-US internally.
    """
    try:
        wb = api.fetch_global_indicators([location])
        if isinstance(wb, dict):
            for _k, ld in wb.items():
                if isinstance(ld, dict) and (
                    ld.get("labor_force") or ld.get("unemployment_rate")
                ):
                    return {
                        "unemployment_rate": ld.get("unemployment_rate"),
                        "gdp_growth": ld.get("gdp_growth"),
                        "labor_force": ld.get("labor_force"),
                        "country": _k,
                        "is_international": True,
                    }
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DEMAND INTELLIGENCE (v2: confidence, parallel, job posting volume)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_market_demand(role: str = "", location: str = "",
                         industry: str = "",
                         context: Optional[EnrichmentContext] = None,
                         ) -> Dict[str, Any]:
    """Enriched job market demand signals with confidence scoring.

    Returns:
        {role, location, industry, labour_market, api_job_market,
         competitors, seasonal, current_posting_count, source,
         confidence, data_freshness, sources_used}
    """
    result: Dict[str, Any] = {
        "role": role or "General",
        "location": location or "National",
        "industry": industry or "General",
    }
    sources_used: List[str] = []
    confidence = 0.0

    res = _lazy_research()
    api = _lazy_api()

    # -- Submit parallel tasks -------------------------------------------------
    lmi_future = None
    seasonal_future = None
    jm_future = None
    comp_future = None

    if res and industry:
        lmi_future = _executor.submit(
            _fetch_labour_market_intel, res, industry, location)
        seasonal_future = _executor.submit(
            _fetch_seasonal_data, res, industry)
        comp_future = _executor.submit(
            _fetch_competitors, res, industry, location)

    if api and role:
        cache_key = f"{role}:{location}"
        cached_jm, cache_age = _cache_get_with_age("market_demand", cache_key)
        if cached_jm is not None:
            result["api_job_market"] = cached_jm
            sources_used.append("API Cache (job market)")
            confidence = max(confidence, 0.70 if cache_age < 43200 else 0.55)
            # U6: Extract current posting count from cached data
            _extract_posting_count(result, cached_jm)
        else:
            jm_future = _executor.submit(
                _dedup_fetch, "market_fetch", f"{role}:{location}",
                lambda: _fetch_job_market_api(api, role, location),
            )

    # -- Collect results -------------------------------------------------------
    if lmi_future:
        try:
            lmi = lmi_future.result(timeout=15)
            if lmi:
                result["labour_market"] = lmi
                sources_used.append("Research Intelligence (labor market)")
                confidence = max(confidence, 0.85)
        except Exception as e:
            logger.debug("enrich_market_demand: LMI future failed: %s", e)

    if seasonal_future:
        try:
            seasonal = seasonal_future.result(timeout=10)
            if seasonal:
                result["seasonal"] = seasonal
                sources_used.append("Research Intelligence (seasonal)")
        except Exception as e:
            logger.debug("enrich_market_demand: seasonal future failed: %s", e)

    if jm_future:
        try:
            jm_raw = jm_future.result(timeout=20)
            if isinstance(jm_raw, dict):
                jm = jm_raw.get("job_market", jm_raw)
                if jm and _is_valid_job_market_data(jm):
                    result["api_job_market"] = jm
                    _cache_set("market_demand", f"{role}:{location}", jm)
                    sources_used.append("Adzuna/Jooble API (live)")
                    confidence = max(confidence, 0.82)
                    _extract_posting_count(result, jm)
                elif jm:
                    logger.debug(
                        "enrich_market_demand: job market data failed validation")
        except Exception as e:
            logger.debug("enrich_market_demand: job market future failed: %s", e)

    if comp_future:
        try:
            comps = comp_future.result(timeout=10)
            if comps:
                result["competitors"] = comps[:5]
                sources_used.append("Research Intelligence (competitors)")
        except Exception as e:
            logger.debug("enrich_market_demand: competitors future failed: %s", e)

    if not sources_used:
        result["source"] = "Generic Market Data"
        confidence = 0.20
        _record_fallback("enrich_market_demand", f"{role}|{location}|{industry}")
        sources_used.append("Generic fallback")
    else:
        result["source"] = " + ".join(sources_used)

    result["confidence"] = round(confidence, 2)
    result["data_freshness"] = _classify_freshness(sources_used)
    result["sources_used"] = sources_used

    if context is not None:
        context.store("market_demand", result)

    return result


def _extract_posting_count(result: Dict, jm_data: Dict) -> None:
    """U6: Extract real-time job posting volume from job market API data."""
    for key in ("count", "total_jobs", "current_posting_count",
                "results_count", "total_results", "total"):
        val = jm_data.get(key)
        if isinstance(val, (int, float)) and val > 0:
            result["current_posting_count"] = int(val)
            return
    # Check nested structures
    if isinstance(jm_data.get("results"), list):
        count = len(jm_data["results"])
        if count > 0:
            result["current_posting_count"] = count


def _fetch_labour_market_intel(res, industry: str, location: str):
    """Helper for parallel labour market fetch."""
    try:
        return res.get_labour_market_intelligence(
            industry, [location] if location else [])
    except Exception:
        return None


def _fetch_seasonal_data(res, industry: str):
    """Helper for parallel seasonal data fetch."""
    try:
        return res.get_seasonal_hiring_advice(industry)
    except Exception:
        return None


def _fetch_competitors(res, industry: str, location: str):
    """Helper for parallel competitors fetch."""
    try:
        return res.get_competitors(
            industry, [location] if location else [])
    except Exception:
        return None


def _fetch_job_market_api(api, role: str, location: str):
    """Helper for parallel job market API fetch."""
    try:
        locs = [location] if location else []
        return api.fetch_job_market([role], locs)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# COMPETITIVE INTELLIGENCE (v2: confidence, parallel, employer brand)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_competitive(company: str, industry: str = "",
                       locations: Optional[List[str]] = None,
                       context: Optional[EnrichmentContext] = None,
                       ) -> Dict[str, Any]:
    """Enriched competitive intelligence with confidence scoring.

    Returns:
        {company, company_info, competitors, company_metadata,
         employer_brand, source, confidence, data_freshness, sources_used}
    """
    result: Dict[str, Any] = {"company": company}
    sources_used: List[str] = []
    confidence = 0.0

    res = _lazy_research()
    api = _lazy_api()

    # -- Submit parallel tasks -------------------------------------------------
    ci_future = None
    brand_future = None
    comp_future = None
    meta_future = None
    sec_future = None

    if res and company:
        ci_future = _executor.submit(_fetch_company_intel, res, company)
        brand_future = _executor.submit(
            _fetch_employer_brand_data, res, company)

    if res and industry:
        comp_future = _executor.submit(
            _fetch_competitors, res, industry,
            locations[0] if locations else "")

    use_api_cache = False
    if api and company:
        cached_comp, cache_age = _cache_get_with_age(
            "competitive", company.lower().strip())
        if cached_comp is not None:
            result.update(cached_comp)
            sources_used.append("API Cache (competitive)")
            confidence = max(confidence, 0.68 if cache_age < 43200 else 0.55)
            use_api_cache = True
        else:
            meta_future = _executor.submit(
                _dedup_fetch, "meta_fetch", company,
                lambda: _fetch_company_metadata_api(api, company),
            )
            sec_future = _executor.submit(
                _dedup_fetch, "sec_fetch", company,
                lambda: _fetch_sec_data_api(api, company),
            )

    # -- Collect results -------------------------------------------------------
    if ci_future:
        try:
            ci = ci_future.result(timeout=10)
            if ci:
                result["company_info"] = ci
                sources_used.append("Research Intelligence (company)")
                confidence = max(confidence, 0.85)
        except Exception:
            pass

    # U7: Employer brand data
    if brand_future:
        try:
            brand = brand_future.result(timeout=10)
            if brand:
                result["employer_brand"] = brand
                sources_used.append("Employer Brand Intelligence")
                confidence = max(confidence, 0.88)
        except Exception:
            pass

    comps: list = []
    if comp_future:
        try:
            comps = comp_future.result(timeout=10) or []
            if comps:
                result["competitors"] = comps[:5]
                sources_used.append("Research Intelligence (competitors)")
        except Exception:
            pass

    if res and industry and comps:
        try:
            comp_intel = res.get_client_competitor_intelligence(
                comps[:3], industry)
            if comp_intel:
                result["competitor_intelligence"] = comp_intel
        except Exception:
            pass

    # API results (parallel)
    if not use_api_cache and (meta_future or sec_future):
        api_results: Dict[str, Any] = {}
        if meta_future:
            try:
                meta = meta_future.result(timeout=15)
                if isinstance(meta, dict) and meta and _is_valid_company_data(meta):
                    api_results["company_metadata"] = meta
                    sources_used.append("Company Metadata API")
                    confidence = max(confidence, 0.78)
            except Exception:
                pass
        if sec_future:
            try:
                sec = sec_future.result(timeout=15)
                if isinstance(sec, dict) and sec and _is_valid_company_data(sec):
                    api_results["sec_data"] = sec
                    sources_used.append("SEC EDGAR API")
                    confidence = max(confidence, 0.80)
            except Exception:
                pass
        if api_results:
            result.update(api_results)
            _cache_set("competitive", company.lower().strip(), api_results)

    if not sources_used:
        result["source"] = "Generic Competitive Data"
        confidence = 0.20
        _record_fallback("enrich_competitive", company)
        sources_used.append("Generic fallback")
    else:
        result["source"] = " + ".join(sources_used)

    result["confidence"] = round(confidence, 2)
    result["data_freshness"] = _classify_freshness(sources_used)
    result["sources_used"] = sources_used

    if context is not None:
        context.store("competitive", result)

    return result


def _fetch_company_intel(res, company: str):
    """Helper for parallel company intelligence fetch."""
    try:
        return res.get_company_intelligence(company)
    except Exception:
        return None


def _fetch_employer_brand_data(res, company: str) -> Optional[Dict]:
    """U7: Extract employer brand data from KNOWN_EMPLOYER_PROFILES."""
    try:
        profiles = getattr(res, "KNOWN_EMPLOYER_PROFILES", {})
        profile = profiles.get(company.lower().strip())
        if profile:
            return {
                "company": company,
                "industry": profile.get("industry", ""),
                "company_size": profile.get("size", ""),
                "primary_hiring_channels": profile.get("hiring_channels", ""),
                "employer_brand_strength": profile.get("employer_brand", ""),
                "known_recruitment_strategies": profile.get(
                    "known_strategies", ""),
                "glassdoor_rating": profile.get("glassdoor_rating", ""),
                "talent_focus": profile.get("talent_focus", ""),
                "source": "Curated Employer Brand Intelligence",
            }
    except Exception:
        pass
    return None


def _fetch_company_metadata_api(api, company: str):
    """Helper for parallel company metadata API fetch."""
    try:
        return api.fetch_company_metadata(company)
    except Exception:
        return None


def _fetch_sec_data_api(api, company: str):
    """Helper for parallel SEC data API fetch."""
    try:
        return api.fetch_sec_company_data(company)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# EMPLOYER BRAND INTELLIGENCE (NEW -- U7 dedicated endpoint)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_employer_brand(
    company: str,
    context: Optional[EnrichmentContext] = None,
) -> Dict[str, Any]:
    """Dedicated employer brand intelligence lookup.

    Accesses KNOWN_EMPLOYER_PROFILES from research.py for 30+ major employers.
    Falls back to generic industry recommendations for unknown companies.

    Returns:
        {company, employer_brand_strength, glassdoor_rating, hiring_channels,
         known_strategies, talent_focus, company_size, source,
         confidence, data_freshness, sources_used}
    """
    result: Dict[str, Any] = {"company": company}
    res = _lazy_research()

    if res:
        brand = _fetch_employer_brand_data(res, company)
        if brand:
            result.update(brand)
            result["confidence"] = 0.92
            result["data_freshness"] = "curated"
            result["sources_used"] = ["Curated Employer Brand Intelligence"]
            if context is not None:
                context.store("employer_brand", result)
            return result

    # Fallback: generic brand intelligence
    safe_name = company.replace(" ", "-")
    result.update({
        "employer_brand_strength": (
            f"No curated profile for {company}. Check Glassdoor, "
            "LinkedIn company page, and career site for brand assessment."
        ),
        "glassdoor_rating": f"Visit glassdoor.com/Reviews/{safe_name}-Reviews",
        "hiring_channels": (
            "Likely Indeed, LinkedIn, Direct Career Site, Glassdoor, "
            "employee referrals"
        ),
        "known_strategies": (
            "Research recommended -- audit their career site and active "
            "job postings"
        ),
        "source": "Generic Employer Brand Guidance",
        "confidence": 0.25,
        "data_freshness": "fallback",
        "sources_used": ["Generic fallback"],
    })
    _record_fallback("enrich_employer_brand", company)

    if context is not None:
        context.store("employer_brand", result)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# AD PLATFORM BENCHMARKS (NEW -- passthrough for Nova/Slack)
# ═══════════════════════════════════════════════════════════════════════════════

def get_ad_platform_benchmarks(
    industry: str = "",
    context: Optional[EnrichmentContext] = None,
) -> Dict[str, Any]:
    """Return CPC/CPM/CTR benchmarks by ad platform for the given industry.

    Exposes data previously only available in the bulk pipeline, now accessible
    to Nova chatbot and Slack bot through the orchestrator.

    Returns:
        {industry, platforms: {google_ads: {...}, meta_ads: {...}, ...},
         platform_audiences, source, confidence}
    """
    std = _lazy_standardizer()
    norm_industry = industry
    if std and industry:
        try:
            norm_industry = std.normalize_industry(industry)
        except Exception:
            pass

    has_specific = norm_industry in _AD_PLATFORM_BENCHMARKS
    benchmarks = _AD_PLATFORM_BENCHMARKS.get(
        norm_industry, _AD_PLATFORM_BENCHMARKS["_default"])

    result: Dict[str, Any] = {
        "industry": industry,
        "normalized_industry": norm_industry,
        "platforms": benchmarks,
        "source": "Industry Ad Platform Benchmarks",
        "confidence": 0.82 if has_specific else 0.55,
        "data_freshness": "curated",
        "sources_used": [
            "Curated Ad Benchmarks" if has_specific
            else "Default Ad Benchmarks"
        ],
    }

    # Merge platform audience data from research.py if available
    res = _lazy_research()
    if res and industry:
        try:
            audiences = res.get_media_platform_audiences(industry)
            if audiences:
                result["platform_audiences"] = audiences
                result["confidence"] = min(
                    1.0, result["confidence"] + 0.08)
                result["sources_used"].append(
                    "Research Intelligence (audiences)")
        except Exception:
            pass

    if context is not None:
        context.store("ad_benchmarks", result)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# COMPUTED INSIGHTS (DeepMind perspective)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_insights(
    role: str = "", location: str = "", industry: str = "",
    context: Optional[EnrichmentContext] = None,
) -> Dict[str, Any]:
    """Compute derived insights from accumulated enrichment data.

    Best called after enrich_salary + enrich_market_demand + enrich_location
    have populated the context.

    Returns:
        {hiring_difficulty_index, market_median_salary,
         salary_competitiveness_at_market, days_until_next_peak_hiring,
         peak_hiring_months, confidence, source}
    """
    insights: Dict[str, Any] = {}

    # Gather data from context or fresh lookups
    salary_data = (context.salary_data if context else None) or {}
    market_data = (context.market_data if context else None) or {}
    location_data = (context.location_data if context else None) or {}

    # -- Hiring difficulty index --
    unemployment = None
    if location_data.get("unemployment"):
        try:
            unemp_str = str(location_data["unemployment"]).replace(
                "%", "").replace("~", "").strip()
            unemployment = float(unemp_str)
        except (ValueError, TypeError):
            pass

    competitor_count = len(market_data.get("competitors", []))
    role_tier = salary_data.get("role_tier", "")

    insights["hiring_difficulty_index"] = _compute_hiring_difficulty_index(
        unemployment_rate=unemployment,
        competition_count=competitor_count,
        role_tier=role_tier,
    )

    # -- Salary competitiveness --
    bls = salary_data.get("bls_percentiles", {})
    market_median = bls.get("median", 0)
    if not market_median:
        market_median = salary_data.get("median_salary", 0)
    if not market_median:
        # Try extracting from salary_range string
        sr = salary_data.get("salary_range", "")
        if isinstance(sr, str) and " - " in sr:
            try:
                parts = sr.replace("$", "").replace(",", "").split(" - ")
                low_val, high_val = float(parts[0]), float(parts[1])
                market_median = (low_val + high_val) / 2
            except (ValueError, IndexError):
                pass

    if market_median > 0:
        insights["market_median_salary"] = int(market_median)
        insights["salary_competitiveness_at_market"] = 0.5

    # -- Job posting volume (from market data) --
    posting_count = market_data.get("current_posting_count")
    if posting_count:
        insights["current_posting_count"] = posting_count

    # -- Days until next peak hiring window --
    std = _lazy_standardizer()
    norm_industry = industry
    if std and industry:
        try:
            norm_industry = std.normalize_industry(industry)
        except Exception:
            pass

    peak_days = _days_until_next_peak(norm_industry)
    if peak_days is not None:
        insights["days_until_next_peak_hiring"] = peak_days
        insights["peak_hiring_months"] = _INDUSTRY_PEAK_MONTHS.get(
            norm_industry, [])

    insights["confidence"] = 0.7 if (salary_data or market_data) else 0.3
    insights["source"] = "Computed Insights Layer"

    return insights


# ═══════════════════════════════════════════════════════════════════════════════
# BUDGET ALLOCATION (v2: session context integration)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_budget(budget: float, roles: List[Dict], locations: List[Dict],
                  industry: str = "",
                  knowledge_base: Optional[Dict] = None,
                  context: Optional[EnrichmentContext] = None,
                  ) -> Dict[str, Any]:
    """Calculate budget allocation using session context + cached data.

    U8: When context is provided, pulls salary, market demand, and competitive
    data from the current session for more accurate allocations.
    """
    be = _lazy_budget()
    if not be:
        return {"error": "Budget engine not available", "confidence": 0.0}

    # Build synthesized data from session context + cache
    synthesized: Dict[str, Any] = {}

    # U8: Pull from session context first (freshest, most relevant)
    if context:
        ctx_salary = context.salary_data
        if ctx_salary:
            role_key = ctx_salary.get("role", "")
            if role_key:
                synthesized.setdefault(
                    "salary_intelligence", {})[role_key] = ctx_salary

        ctx_market = context.market_data
        if ctx_market:
            role_key = ctx_market.get("role", "")
            if role_key:
                synthesized.setdefault(
                    "job_market_demand", {})[role_key] = ctx_market

        ctx_competitive = context.competitive_data
        if ctx_competitive:
            synthesized["competitive_intelligence"] = ctx_competitive

        ctx_brand = context.employer_brand
        if ctx_brand:
            synthesized["employer_brand"] = ctx_brand

    # Supplement with cached data for roles/locations not in context
    for r in roles:
        title = r.get("title", "")
        if not title:
            continue
        if title not in synthesized.get("salary_intelligence", {}):
            cached_sal = _cache_get("salary", title)
            if cached_sal:
                synthesized.setdefault(
                    "salary_intelligence", {})[title] = cached_sal

        for loc in locations:
            loc_str = loc.get("city", "")
            ck = f"{title}:{loc_str}"
            if title not in synthesized.get("job_market_demand", {}):
                cached_demand = _cache_get("market_demand", ck)
                if cached_demand:
                    synthesized.setdefault(
                        "job_market_demand", {})[title] = cached_demand

    channel_pcts = {
        "Programmatic & DSP": 30,
        "Global Job Boards": 25,
        "Niche & Industry Boards": 15,
        "Social Media Channels": 15,
        "Regional & Local Boards": 10,
        "Employer Branding": 5,
    }

    try:
        result = be.calculate_budget_allocation(
            total_budget=budget,
            roles=roles,
            locations=locations,
            industry=industry,
            channel_percentages=channel_pcts,
            synthesized_data=synthesized if synthesized else None,
            knowledge_base=knowledge_base,
        )
        # Add confidence based on data availability
        data_sources = len(synthesized)
        result["confidence"] = round(
            min(0.95, 0.50 + data_sources * 0.10), 2)
        result["data_freshness"] = "computed"
        result["sources_used"] = (
            ["Session context", "Cache"] if context
            else ["Cache"] if synthesized else ["Default parameters"]
        )
        return result
    except Exception as e:
        logger.error("enrich_budget failed: %s", e, exc_info=True)
        return {"error": "Budget calculation failed", "confidence": 0.0}


# ═══════════════════════════════════════════════════════════════════════════════
# ADDITIONAL RESEARCH.PY ACCESSORS (thin wrappers for Nova tools)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_hiring_regulations(locations: List[str]) -> List:
    """Hiring regulations from research.py.  Returns list of regulation dicts."""
    res = _lazy_research()
    if res:
        try:
            return res.get_hiring_regulations(locations) or []
        except Exception as e:
            logger.debug("enrich_hiring_regulations failed: %s", e)
    return []


def enrich_seasonal(industry: str) -> Dict[str, Any]:
    """Seasonal hiring advice from research.py."""
    res = _lazy_research()
    if res and industry:
        try:
            return res.get_seasonal_hiring_advice(industry) or {}
        except Exception as e:
            logger.debug("enrich_seasonal failed: %s", e)
    return {}


def enrich_campus(locations: List[str], roles: Optional[List[str]] = None,
                  industry: str = "") -> List:
    """Campus recruiting recommendations from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_campus_recruiting_recommendations(
                locations, roles, industry,
            ) or []
        except Exception as e:
            logger.debug("enrich_campus failed: %s", e)
    return []


def enrich_events(locations: List[str], industry: str = "") -> List:
    """Industry events from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_events(locations, industry) or []
        except Exception as e:
            logger.debug("enrich_events failed: %s", e)
    return []


def enrich_platform_audiences(industry: str) -> Dict[str, Any]:
    """Platform audience data from research.py."""
    res = _lazy_research()
    if res and industry:
        try:
            return res.get_media_platform_audiences(industry) or {}
        except Exception as e:
            logger.debug("enrich_platform_audiences failed: %s", e)
    return {}


def enrich_global_supply(locations: List[str],
                         industry: str = "") -> Dict[str, Any]:
    """Global supply data from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_global_supply_data(locations, industry) or {}
        except Exception as e:
            logger.debug("enrich_global_supply failed: %s", e)
    return {}


def enrich_educational_partners(locations: List[str],
                                industry: str = "") -> List:
    """Educational partners from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_educational_partners(locations, industry) or []
        except Exception as e:
            logger.debug("enrich_educational_partners failed: %s", e)
    return []


def enrich_radio_podcasts(locations: List[str],
                          industry: str = "") -> List:
    """Radio and podcast advertising data from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_radio_podcasts(locations, industry) or []
        except Exception as e:
            logger.debug("enrich_radio_podcasts failed: %s", e)
    return []
