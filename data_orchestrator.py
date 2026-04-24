"""
data_orchestrator.py -- Unified Data Orchestration Layer for Nova Platform (v4.0)

Coordinates data fetching across all sources (APIs, web scrapers, search,
vector search, knowledge base) with module-aware routing, parallel execution,
caching, result merging, and data freshness tracking.

Modules:
    - Command Center: Campaign planning, budget optimization, compliance
    - Intelligence Hub: Market analysis, competitor scanning, talent mapping
    - Nova AI: General chat, action execution, context summarization

Data Sources:
    1. Knowledge Base (data/ directory, in-memory at startup)
    2. API Integrations (FRED, Adzuna, Jooble, O*NET, BEA, Census, USAJobs, BLS)
    3. Web Scraper Router (6-tier: Firecrawl -> Jina -> Tavily -> LLM -> Cache -> stdlib)
    4. Tavily Search (4-tier: Tavily -> Jina -> DuckDuckGo -> LLM)
    5. Vector Search (Voyage AI + TF-IDF fallback)
    6. Supabase (persistent storage + data cache)
    7. Upstash Redis (L2 cache layer)

Caching hierarchy:
    L1: In-memory dict with TTL per data type
    L2: Upstash Redis with configurable TTL
    L3: Supabase nova_data_cache table (long-lived structured data)

Thread-safe, stdlib-only (no Flask/Django).
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import urllib.error
from concurrent.futures import (
    ThreadPoolExecutor,
    Future,
)
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# =============================================================================
# MODULE DEFINITIONS
# =============================================================================


class PlatformModule(str, Enum):
    """Platform super-modules for data routing."""

    COMMAND_CENTER = "command_center"
    INTELLIGENCE_HUB = "intelligence_hub"
    NOVA_AI = "nova_ai"


class DataSourceType(str, Enum):
    """Supported data source types."""

    KNOWLEDGE_BASE = "knowledge_base"
    ADZUNA = "adzuna"
    BLS = "bls"
    FRED = "fred"
    BEA = "bea"
    CENSUS = "census"
    ONET = "onet"
    USAJOBS = "usajobs"
    JOOBLE = "jooble"
    WEB_SCRAPER = "web_scraper"
    TAVILY_SEARCH = "tavily_search"
    VECTOR_SEARCH = "vector_search"
    SUPABASE = "supabase"
    LLM = "llm"


# =============================================================================
# DATA FRESHNESS TTLs (seconds)
# =============================================================================

# TTL per data type category -- used for both L1 and L2 cache
DATA_TTL: Dict[str, int] = {
    "jobs": 3600,  # 1 hour -- job listings change frequently
    "salary": 3600,  # 1 hour -- salary data updates daily
    "economic": 86400,  # 24 hours -- FRED/BEA/BLS update monthly/quarterly
    "skills": 604800,  # 7 days -- O*NET occupational data is stable
    "demographics": 604800,  # 7 days -- Census data is annual
    "knowledge_base": 604800,  # 7 days -- KB refreshes are manual
    "web_search": 1800,  # 30 min -- web results are ephemeral
    "web_scrape": 3600,  # 1 hour -- scraped pages change moderately
    "vector_search": 1800,  # 30 min -- depends on query freshness
    "compliance": 86400,  # 24 hours -- compliance rules change slowly
    "campaign": 300,  # 5 min -- active campaign data should be fresh
    "default": 3600,  # 1 hour -- fallback
}


# =============================================================================
# MODULE-AWARE DATA SOURCE PRIORITY
# =============================================================================

MODULE_SOURCE_PRIORITY: Dict[PlatformModule, List[DataSourceType]] = {
    PlatformModule.COMMAND_CENTER: [
        DataSourceType.ADZUNA,
        DataSourceType.BLS,
        DataSourceType.KNOWLEDGE_BASE,
        DataSourceType.LLM,
        DataSourceType.ONET,
        DataSourceType.USAJOBS,
        DataSourceType.JOOBLE,
        DataSourceType.VECTOR_SEARCH,
        DataSourceType.SUPABASE,
    ],
    PlatformModule.INTELLIGENCE_HUB: [
        DataSourceType.WEB_SCRAPER,
        DataSourceType.TAVILY_SEARCH,
        DataSourceType.FRED,
        DataSourceType.BEA,
        DataSourceType.CENSUS,
        DataSourceType.KNOWLEDGE_BASE,
        DataSourceType.BLS,
        DataSourceType.VECTOR_SEARCH,
        DataSourceType.SUPABASE,
        DataSourceType.LLM,
    ],
    PlatformModule.NOVA_AI: [
        DataSourceType.VECTOR_SEARCH,
        DataSourceType.KNOWLEDGE_BASE,
        DataSourceType.ADZUNA,
        DataSourceType.BLS,
        DataSourceType.FRED,
        DataSourceType.ONET,
        DataSourceType.TAVILY_SEARCH,
        DataSourceType.WEB_SCRAPER,
        DataSourceType.BEA,
        DataSourceType.CENSUS,
        DataSourceType.USAJOBS,
        DataSourceType.JOOBLE,
        DataSourceType.SUPABASE,
        DataSourceType.LLM,
    ],
}


# =============================================================================
# INTENT -> DATA SOURCE MAPPING
# =============================================================================

INTENT_SOURCE_MAP: Dict[str, List[DataSourceType]] = {
    "salary": [
        DataSourceType.ADZUNA,
        DataSourceType.BLS,
        DataSourceType.KNOWLEDGE_BASE,
    ],
    "compensation": [
        DataSourceType.ADZUNA,
        DataSourceType.BLS,
        DataSourceType.KNOWLEDGE_BASE,
    ],
    "wage": [DataSourceType.BLS, DataSourceType.ADZUNA],
    "job": [
        DataSourceType.ADZUNA,
        DataSourceType.JOOBLE,
        DataSourceType.USAJOBS,
        DataSourceType.ONET,
    ],
    "hire": [DataSourceType.ADZUNA, DataSourceType.JOOBLE, DataSourceType.BLS],
    "hiring": [DataSourceType.ADZUNA, DataSourceType.JOOBLE, DataSourceType.BLS],
    "recruit": [
        DataSourceType.ADZUNA,
        DataSourceType.KNOWLEDGE_BASE,
        DataSourceType.TAVILY_SEARCH,
    ],
    "talent": [DataSourceType.ADZUNA, DataSourceType.ONET, DataSourceType.BLS],
    "skill": [DataSourceType.ONET, DataSourceType.KNOWLEDGE_BASE],
    "occupation": [DataSourceType.ONET, DataSourceType.BLS],
    "career": [DataSourceType.ONET, DataSourceType.ADZUNA],
    "economy": [DataSourceType.FRED, DataSourceType.BEA],
    "economic": [DataSourceType.FRED, DataSourceType.BEA],
    "unemployment": [DataSourceType.FRED, DataSourceType.BLS],
    "inflation": [DataSourceType.FRED, DataSourceType.BLS],
    "gdp": [DataSourceType.BEA, DataSourceType.FRED],
    "market": [
        DataSourceType.FRED,
        DataSourceType.TAVILY_SEARCH,
        DataSourceType.WEB_SCRAPER,
    ],
    "trend": [
        DataSourceType.TAVILY_SEARCH,
        DataSourceType.WEB_SCRAPER,
        DataSourceType.FRED,
    ],
    "news": [DataSourceType.TAVILY_SEARCH, DataSourceType.WEB_SCRAPER],
    "population": [DataSourceType.CENSUS, DataSourceType.BEA],
    "demographic": [DataSourceType.CENSUS, DataSourceType.BEA],
    "competitor": [DataSourceType.WEB_SCRAPER, DataSourceType.TAVILY_SEARCH],
    "compliance": [DataSourceType.KNOWLEDGE_BASE, DataSourceType.TAVILY_SEARCH],
    "federal": [DataSourceType.USAJOBS, DataSourceType.BLS],
    "government": [DataSourceType.USAJOBS, DataSourceType.CENSUS],
    "benchmark": [
        DataSourceType.KNOWLEDGE_BASE,
        DataSourceType.BLS,
        DataSourceType.ADZUNA,
    ],
    "budget": [DataSourceType.KNOWLEDGE_BASE, DataSourceType.ADZUNA],
    "campaign": [DataSourceType.KNOWLEDGE_BASE, DataSourceType.SUPABASE],
    "channel": [DataSourceType.KNOWLEDGE_BASE, DataSourceType.ADZUNA],
}

# Data type category for TTL lookup, keyed by DataSourceType
SOURCE_TTL_CATEGORY: Dict[DataSourceType, str] = {
    DataSourceType.ADZUNA: "jobs",
    DataSourceType.BLS: "economic",
    DataSourceType.FRED: "economic",
    DataSourceType.BEA: "economic",
    DataSourceType.CENSUS: "demographics",
    DataSourceType.ONET: "skills",
    DataSourceType.USAJOBS: "jobs",
    DataSourceType.JOOBLE: "jobs",
    DataSourceType.WEB_SCRAPER: "web_scrape",
    DataSourceType.TAVILY_SEARCH: "web_search",
    DataSourceType.VECTOR_SEARCH: "vector_search",
    DataSourceType.KNOWLEDGE_BASE: "knowledge_base",
    DataSourceType.SUPABASE: "default",
    DataSourceType.LLM: "default",
}


# =============================================================================
# L1 IN-MEMORY CACHE
# =============================================================================


class _L1Cache:
    """Thread-safe in-memory cache with per-key TTL based on data type."""

    def __init__(self, max_size: int = 500) -> None:
        self._lock = threading.Lock()
        self._store: Dict[str, Tuple[float, int, Any]] = {}
        self._max_size = max_size
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value. Returns None if missing or expired."""
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            ts, ttl, value = entry
            if now - ts > ttl:
                del self._store[key]
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, key: str, value: Any, ttl_category: str = "default") -> None:
        """Store a value with TTL based on data type category."""
        ttl = DATA_TTL.get(ttl_category, DATA_TTL["default"])
        now = time.time()
        with self._lock:
            if len(self._store) >= self._max_size:
                oldest_key = min(self._store, key=lambda k: self._store[k][0])
                del self._store[oldest_key]
            self._store[key] = (now, ttl, value)

    def invalidate(self, key: str) -> None:
        """Remove a specific key from cache."""
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        """Clear entire cache."""
        with self._lock:
            self._store.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Return cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100.0) if total > 0 else 0.0
            return {
                "size": len(self._store),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_pct": round(hit_rate, 1),
            }


# =============================================================================
# DATA FETCH RESULT
# =============================================================================


class DataFetchResult:
    """Container for a single data source fetch result."""

    __slots__ = (
        "source",
        "data",
        "success",
        "latency_ms",
        "from_cache",
        "error",
        "ttl_category",
    )

    def __init__(
        self,
        source: DataSourceType,
        data: Any = None,
        success: bool = False,
        latency_ms: float = 0.0,
        from_cache: bool = False,
        error: str = "",
        ttl_category: str = "default",
    ) -> None:
        self.source = source
        self.data = data
        self.success = success
        self.latency_ms = latency_ms
        self.from_cache = from_cache
        self.error = error
        self.ttl_category = ttl_category

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for logging / API responses."""
        return {
            "source": self.source.value,
            "success": self.success,
            "latency_ms": round(self.latency_ms, 1),
            "from_cache": self.from_cache,
            "error": self.error,
            "has_data": self.data is not None,
        }


# =============================================================================
# ORCHESTRATION RESULT
# =============================================================================


class OrchestrationResult:
    """Container for the merged result of all data fetches."""

    def __init__(self) -> None:
        self.data: Dict[str, Any] = {}
        self.fetch_results: List[DataFetchResult] = []
        self.total_latency_ms: float = 0.0
        self.module: Optional[PlatformModule] = None
        self.sources_queried: int = 0
        self.sources_succeeded: int = 0

    def merge(self, result: DataFetchResult) -> None:
        """Merge a single fetch result into the orchestration result."""
        self.fetch_results.append(result)
        self.sources_queried += 1
        if result.success and result.data is not None:
            self.sources_succeeded += 1
            key = result.source.value
            if key in self.data:
                existing = self.data[key]
                if isinstance(existing, dict) and isinstance(result.data, dict):
                    existing.update(result.data)
                elif isinstance(existing, list) and isinstance(result.data, list):
                    seen = {
                        json.dumps(item, sort_keys=True, default=str)
                        for item in existing
                    }
                    for item in result.data:
                        item_key = json.dumps(item, sort_keys=True, default=str)
                        if item_key not in seen:
                            existing.append(item)
                            seen.add(item_key)
                else:
                    self.data[key] = result.data
            else:
                self.data[key] = result.data

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for API response / logging."""
        return {
            "module": self.module.value if self.module else None,
            "sources_queried": self.sources_queried,
            "sources_succeeded": self.sources_succeeded,
            "total_latency_ms": round(self.total_latency_ms, 1),
            "data_keys": list(self.data.keys()),
            "fetch_details": [r.to_dict() for r in self.fetch_results],
        }


# =============================================================================
# DATA ORCHESTRATOR
# =============================================================================


class DataOrchestrator:
    """Coordinates data fetching across all sources with module-aware routing.

    Usage:
        orchestrator = DataOrchestrator()
        result = orchestrator.fetch(
            module=PlatformModule.COMMAND_CENTER,
            query="Software Engineer salary in New York",
            context={"role": "Software Engineer", "location": "New York"},
        )
        print(result.data)
    """

    def __init__(self) -> None:
        self._l1_cache = _L1Cache(max_size=500)
        self._lock = threading.Lock()
        self._source_handlers: Dict[DataSourceType, Callable[..., Any]] = {}

        # Metrics
        self._total_fetches = 0
        self._total_cache_hits = 0
        self._source_error_counts: Dict[str, int] = {}

        # Redis L2 cache (lazy import)
        self._redis_get: Optional[Callable] = None
        self._redis_set: Optional[Callable] = None
        self._redis_available = False
        self._init_redis()

    def _init_redis(self) -> None:
        """Initialize Upstash Redis L2 cache if available."""
        try:
            from upstash_cache import cache_get, cache_set

            self._redis_get = cache_get
            self._redis_set = cache_set
            self._redis_available = True
            logger.info("DataOrchestrator: Upstash Redis L2 cache enabled")
        except ImportError:
            logger.info("DataOrchestrator: Upstash Redis L2 cache not available")

    def register_source(
        self,
        source_type: DataSourceType,
        handler: Callable[..., Any],
    ) -> None:
        """Register a data source handler function.

        Args:
            source_type: The data source type to register.
            handler: Callable that accepts (query: str, context: dict) and
                     returns data (dict, list, or None).
        """
        self._source_handlers[source_type] = handler
        logger.info(
            f"DataOrchestrator: registered source handler for {source_type.value}"
        )

    # ── Cache key generation ──

    @staticmethod
    def _cache_key(
        source: DataSourceType, query: str, context: Optional[Dict] = None
    ) -> str:
        """Generate a deterministic cache key from source + query + context."""
        ctx_str = json.dumps(context or {}, sort_keys=True, default=str)
        raw = f"{source.value}|{query.strip().lower()[:200]}|{ctx_str[:200]}"
        return f"orch:{hashlib.sha256(raw.encode()).hexdigest()[:24]}"

    # ── L2 Redis helpers ──

    def _redis_l2_get(self, key: str) -> Optional[Any]:
        """Check Upstash Redis L2 cache."""
        if not self._redis_available or not self._redis_get:
            return None
        try:
            return self._redis_get(f"orch:{key}")
        except Exception as exc:
            logger.debug(f"Redis L2 get failed for {key}: {exc}")
            return None

    def _redis_l2_set(self, key: str, value: Any, ttl_category: str) -> None:
        """Store in Upstash Redis L2 cache."""
        if not self._redis_available or not self._redis_set:
            return
        ttl = DATA_TTL.get(ttl_category, DATA_TTL["default"])
        try:
            self._redis_set(
                f"orch:{key}", value, ttl_seconds=ttl, category="orchestrator"
            )
        except Exception as exc:
            logger.debug(f"Redis L2 set failed for {key}: {exc}")

    # ── Intent detection ──

    @staticmethod
    def detect_relevant_sources(
        query: str,
        module: PlatformModule,
    ) -> List[DataSourceType]:
        """Detect which data sources are relevant for a query based on intent keywords.

        Args:
            query: User query string.
            module: Platform module making the request.

        Returns:
            Priority-ordered list of data sources to query.
        """
        query_lower = query.lower()
        matched_sources: List[DataSourceType] = []
        seen: set[DataSourceType] = set()

        for keyword, sources in INTENT_SOURCE_MAP.items():
            if keyword in query_lower:
                for src in sources:
                    if src not in seen:
                        matched_sources.append(src)
                        seen.add(src)

        if not matched_sources:
            matched_sources = list(
                MODULE_SOURCE_PRIORITY.get(
                    module, MODULE_SOURCE_PRIORITY[PlatformModule.NOVA_AI]
                )
            )
        else:
            for src in MODULE_SOURCE_PRIORITY.get(module, []):
                if src not in seen:
                    matched_sources.append(src)
                    seen.add(src)

        return matched_sources

    # ── Single source fetch (with caching) ──

    def _fetch_single_source(
        self,
        source: DataSourceType,
        query: str,
        context: Optional[Dict] = None,
    ) -> DataFetchResult:
        """Fetch data from a single source with L1/L2 cache check.

        Args:
            source: Data source to fetch from.
            query: Query string.
            context: Additional context (role, location, etc.).

        Returns:
            DataFetchResult with data or error.
        """
        ttl_category = SOURCE_TTL_CATEGORY.get(source, "default")
        cache_key = self._cache_key(source, query, context)
        start_ts = time.time()

        # L1 check
        cached = self._l1_cache.get(cache_key)
        if cached is not None:
            with self._lock:
                self._total_cache_hits += 1
            return DataFetchResult(
                source=source,
                data=cached,
                success=True,
                latency_ms=(time.time() - start_ts) * 1000,
                from_cache=True,
                ttl_category=ttl_category,
            )

        # L2 check (Redis)
        redis_val = self._redis_l2_get(cache_key)
        if redis_val is not None:
            self._l1_cache.set(cache_key, redis_val, ttl_category)
            with self._lock:
                self._total_cache_hits += 1
            return DataFetchResult(
                source=source,
                data=redis_val,
                success=True,
                latency_ms=(time.time() - start_ts) * 1000,
                from_cache=True,
                ttl_category=ttl_category,
            )

        # Fetch from source handler
        handler = self._source_handlers.get(source)
        if handler is None:
            return DataFetchResult(
                source=source,
                success=False,
                error=f"No handler registered for {source.value}",
                latency_ms=(time.time() - start_ts) * 1000,
            )

        try:
            data = handler(query, context or {})
            elapsed_ms = (time.time() - start_ts) * 1000

            if data is not None:
                self._l1_cache.set(cache_key, data, ttl_category)
                self._redis_l2_set(cache_key, data, ttl_category)
                with self._lock:
                    self._total_fetches += 1
                return DataFetchResult(
                    source=source,
                    data=data,
                    success=True,
                    latency_ms=elapsed_ms,
                    ttl_category=ttl_category,
                )
            else:
                return DataFetchResult(
                    source=source,
                    success=False,
                    error="Handler returned None",
                    latency_ms=elapsed_ms,
                )
        except (urllib.error.URLError, OSError, ValueError, TypeError, KeyError) as exc:
            elapsed_ms = (time.time() - start_ts) * 1000
            with self._lock:
                err_key = source.value
                self._source_error_counts[err_key] = (
                    self._source_error_counts.get(err_key, 0) + 1
                )
            logger.error(
                f"DataOrchestrator: fetch from {source.value} failed: {exc}",
                exc_info=True,
            )
            return DataFetchResult(
                source=source,
                success=False,
                error=str(exc),
                latency_ms=elapsed_ms,
            )

    # ── Main orchestration entry point ──

    def fetch(
        self,
        module: PlatformModule,
        query: str,
        context: Optional[Dict] = None,
        max_sources: int = 6,
        timeout_seconds: float = 10.0,
        sources_override: Optional[List[DataSourceType]] = None,
    ) -> OrchestrationResult:
        """Fetch and merge data from multiple sources in parallel.

        Args:
            module: Which platform module is making the request.
            query: User query string.
            context: Additional context (role, location, industry, etc.).
            max_sources: Max number of sources to query concurrently.
            timeout_seconds: Max wall-clock time for all fetches.
            sources_override: If provided, use these sources instead of auto-detection.

        Returns:
            OrchestrationResult with merged data from all successful sources.
        """
        result = OrchestrationResult()
        result.module = module
        wall_start = time.time()

        if sources_override:
            sources = sources_override[:max_sources]
        else:
            sources = self.detect_relevant_sources(query, module)[:max_sources]

        available_sources = [s for s in sources if s in self._source_handlers]

        if not available_sources:
            logger.warning(
                f"DataOrchestrator: no available sources for module={module.value}, "
                f"query={query[:60]}"
            )
            result.total_latency_ms = (time.time() - wall_start) * 1000
            return result

        logger.info(
            f"DataOrchestrator: fetching from {len(available_sources)} sources "
            f"for module={module.value}: {[s.value for s in available_sources]}"
        )

        try:
            with ThreadPoolExecutor(
                max_workers=min(len(available_sources), 8),
                thread_name_prefix="orch-fetch",
            ) as pool:
                futures: Dict[Future, DataSourceType] = {}
                for source in available_sources:
                    fut = pool.submit(self._fetch_single_source, source, query, context)
                    futures[fut] = source

                for fut in futures:
                    try:
                        fetch_result = fut.result(timeout=timeout_seconds)
                        result.merge(fetch_result)
                    except TimeoutError:
                        source = futures[fut]
                        logger.warning(
                            f"DataOrchestrator: {source.value} timed out ({timeout_seconds}s)"
                        )
                        result.merge(
                            DataFetchResult(
                                source=source,
                                success=False,
                                error=f"Timeout ({timeout_seconds}s)",
                            )
                        )
                    except (ValueError, TypeError, OSError) as exc:
                        source = futures[fut]
                        logger.error(
                            f"DataOrchestrator: {source.value} future error: {exc}",
                            exc_info=True,
                        )
                        result.merge(
                            DataFetchResult(
                                source=source,
                                success=False,
                                error=str(exc),
                            )
                        )
        except (RuntimeError, OSError) as exc:
            logger.error(
                f"DataOrchestrator: ThreadPoolExecutor failed: {exc}",
                exc_info=True,
            )

        result.total_latency_ms = (time.time() - wall_start) * 1000

        logger.info(
            f"DataOrchestrator: completed in {result.total_latency_ms:.0f}ms -- "
            f"{result.sources_succeeded}/{result.sources_queried} sources succeeded, "
            f"data keys: {list(result.data.keys())}"
        )

        return result

    # ── Convenience methods for specific modules ──

    def fetch_for_command_center(
        self,
        query: str,
        context: Optional[Dict] = None,
    ) -> OrchestrationResult:
        """Fetch data optimized for Command Center (campaign planning).

        Args:
            query: Campaign planning query.
            context: Role, location, budget context.

        Returns:
            OrchestrationResult prioritizing job market + economic data.
        """
        return self.fetch(
            module=PlatformModule.COMMAND_CENTER,
            query=query,
            context=context,
            max_sources=6,
            timeout_seconds=8.0,
        )

    def fetch_for_intelligence_hub(
        self,
        query: str,
        context: Optional[Dict] = None,
    ) -> OrchestrationResult:
        """Fetch data optimized for Intelligence Hub (market analysis).

        Args:
            query: Market research / analysis query.
            context: Industry, region, competitor context.

        Returns:
            OrchestrationResult prioritizing web + economic data.
        """
        return self.fetch(
            module=PlatformModule.INTELLIGENCE_HUB,
            query=query,
            context=context,
            max_sources=8,
            timeout_seconds=12.0,
        )

    def fetch_for_nova_ai(
        self,
        query: str,
        context: Optional[Dict] = None,
    ) -> OrchestrationResult:
        """Fetch data for Nova AI chat (general, intent-driven).

        Args:
            query: Chat message.
            context: Conversation context.

        Returns:
            OrchestrationResult using all relevant sources based on intent.
        """
        return self.fetch(
            module=PlatformModule.NOVA_AI,
            query=query,
            context=context,
            max_sources=6,
            timeout_seconds=8.0,
        )

    # ── Enrich methods (used by Nova tool handlers) ──

    def enrich_salary(
        self, role: str = "", location: str = "", industry: str = "", **kwargs: Any
    ) -> dict:
        """Enrich salary data using live API sources (Adzuna, BLS, Supabase).

        Args:
            role: Job title or role name.
            location: Geographic location.
            industry: Industry vertical.

        Returns:
            Dict with enriched data and sources_used list.
        """
        result: Dict[str, Any] = {
            "source": "orchestrator",
            "data": {},
            "sources_used": [],
        }
        query = f"salary {role} {location}".strip()
        context = {"role": role, "location": location, "industry": industry}

        # Try Adzuna for real-time salary data
        handler = self._source_handlers.get(DataSourceType.ADZUNA)
        if handler:
            try:
                adzuna_data = handler(query, {**context, "type": "salary"})
                if adzuna_data:
                    result["data"]["adzuna"] = adzuna_data
                    result["sources_used"].append("adzuna")
            except Exception as e:
                logger.debug("Adzuna salary enrichment failed: %s", e)

        # Try BLS for occupational stats
        bls_handler = self._source_handlers.get(DataSourceType.BLS)
        if bls_handler:
            try:
                bls_data = bls_handler(f"occupational stats {role}", context)
                if bls_data:
                    result["data"]["bls"] = bls_data
                    result["sources_used"].append("bls")
            except Exception as e:
                logger.debug("BLS salary enrichment failed: %s", e)

        # Try Supabase salary data
        try:
            from supabase_data import get_salary_data

            supa_data = get_salary_data()
            if supa_data:
                result["data"]["supabase"] = supa_data
                result["sources_used"].append("supabase")
        except Exception as e:
            logger.debug("Supabase salary enrichment failed: %s", e)

        return result

    def enrich_market_demand(
        self, role: str = "", location: str = "", industry: str = "", **kwargs: Any
    ) -> dict:
        """Enrich market demand data using live API sources (Adzuna, Jooble, FRED).

        Args:
            role: Job title or role name.
            location: Geographic location.
            industry: Industry vertical.

        Returns:
            Dict with enriched data and sources_used list.
        """
        result: Dict[str, Any] = {
            "source": "orchestrator",
            "data": {},
            "sources_used": [],
        }
        query = f"job demand {role} {location}".strip()
        context = {"role": role, "location": location, "industry": industry}

        # Try Adzuna for job posting counts
        handler = self._source_handlers.get(DataSourceType.ADZUNA)
        if handler:
            try:
                adzuna_data = handler(f"job {role} {location}", context)
                if adzuna_data:
                    result["data"]["adzuna"] = adzuna_data
                    result["sources_used"].append("adzuna")
            except Exception as e:
                logger.debug("Adzuna market demand failed: %s", e)

        # Try Jooble for international job data
        jooble_handler = self._source_handlers.get(DataSourceType.JOOBLE)
        if jooble_handler:
            try:
                jooble_data = jooble_handler(f"job {role} {location}", context)
                if jooble_data:
                    result["data"]["jooble"] = jooble_data
                    result["sources_used"].append("jooble")
            except Exception as e:
                logger.debug("Jooble market demand failed: %s", e)

        # Try FRED for economic indicators
        fred_handler = self._source_handlers.get(DataSourceType.FRED)
        if fred_handler:
            try:
                fred_data = fred_handler("unemployment labor market", context)
                if fred_data:
                    result["data"]["fred"] = fred_data
                    result["sources_used"].append("fred")
            except Exception as e:
                logger.debug("FRED market demand failed: %s", e)

        # Try Supabase market trends
        try:
            from supabase_data import get_market_trends

            trends = get_market_trends()
            if trends:
                result["data"]["supabase_trends"] = trends
                result["sources_used"].append("supabase")
        except Exception as e:
            logger.debug("Supabase trends enrichment failed: %s", e)

        return result

    def enrich_budget(
        self,
        budget: float = 0,
        channels: Optional[List] = None,
        industry: str = "",
        **kwargs: Any,
    ) -> dict:
        """Enrich budget projection data.

        Args:
            budget: Budget amount.
            channels: List of channels.
            industry: Industry vertical.

        Returns:
            Dict with enriched data and sources_used list.
        """
        result: Dict[str, Any] = {
            "source": "orchestrator",
            "data": {},
            "sources_used": [],
        }
        context = {"industry": industry, "budget": budget}

        # Try Adzuna for CPC benchmarks
        handler = self._source_handlers.get(DataSourceType.ADZUNA)
        if handler:
            try:
                cpc_data = handler(f"salary benchmark {industry}", context)
                if cpc_data:
                    result["data"]["cpc_benchmarks"] = cpc_data
                    result["sources_used"].append("adzuna")
            except Exception as e:
                logger.debug("Budget enrichment (Adzuna) failed: %s", e)

        # Try Supabase channel benchmarks
        try:
            from supabase_data import get_channel_benchmarks

            benchmarks = get_channel_benchmarks()
            if benchmarks:
                result["data"]["channel_benchmarks"] = benchmarks
                result["sources_used"].append("supabase")
        except Exception as e:
            logger.debug("Budget enrichment (Supabase) failed: %s", e)

        return result

    def enrich_location(
        self, location: str = "", role: str = "", **kwargs: Any
    ) -> dict:
        """Enrich location/regional data using Census, BEA, BLS.

        Args:
            location: Geographic location.
            role: Job title for regional context.

        Returns:
            Dict with enriched data and sources_used list.
        """
        result: Dict[str, Any] = {
            "source": "orchestrator",
            "data": {},
            "sources_used": [],
        }
        context = {"location": location, "role": role}

        # Try Census for demographic data
        census_handler = self._source_handlers.get(DataSourceType.CENSUS)
        if census_handler:
            try:
                census_data = census_handler(
                    f"population demographics {location}", context
                )
                if census_data:
                    result["data"]["census"] = census_data
                    result["sources_used"].append("census")
            except Exception as e:
                logger.debug("Census location enrichment failed: %s", e)

        # Try BEA for regional economic data
        bea_handler = self._source_handlers.get(DataSourceType.BEA)
        if bea_handler:
            try:
                bea_data = bea_handler(f"regional gdp {location}", context)
                if bea_data:
                    result["data"]["bea"] = bea_data
                    result["sources_used"].append("bea")
            except Exception as e:
                logger.debug("BEA location enrichment failed: %s", e)

        # Try USAJobs for federal job data
        usajobs_handler = self._source_handlers.get(DataSourceType.USAJOBS)
        if usajobs_handler:
            try:
                usajobs_data = usajobs_handler(
                    f"federal jobs {role} {location}", context
                )
                if usajobs_data:
                    result["data"]["usajobs"] = usajobs_data
                    result["sources_used"].append("usajobs")
            except Exception as e:
                logger.debug("USAJobs location enrichment failed: %s", e)

        return result

    def enrich_skills_gap(
        self, role: str = "", industry: str = "", **kwargs: Any
    ) -> dict:
        """Enrich skills gap data using O*NET.

        Args:
            role: Job title or role name.
            industry: Industry vertical.

        Returns:
            Dict with enriched data and sources_used list.
        """
        result: Dict[str, Any] = {
            "source": "orchestrator",
            "data": {},
            "sources_used": [],
        }

        onet_handler = self._source_handlers.get(DataSourceType.ONET)
        if onet_handler:
            try:
                onet_data = onet_handler(
                    f"skills occupation {role}", {"role": role, "industry": industry}
                )
                if onet_data:
                    result["data"]["onet"] = onet_data
                    result["sources_used"].append("onet")
            except Exception as e:
                logger.debug("O*NET skills enrichment failed: %s", e)

        return result

    def enrich_geopolitical_risk(self, location: str = "", **kwargs: Any) -> dict:
        """Enrich geopolitical/economic risk data.

        Args:
            location: Geographic location for risk assessment.

        Returns:
            Dict with enriched data and sources_used list.
        """
        result: Dict[str, Any] = {
            "source": "orchestrator",
            "data": {},
            "sources_used": [],
        }

        fred_handler = self._source_handlers.get(DataSourceType.FRED)
        if fred_handler:
            try:
                fred_data = fred_handler(
                    f"economy economic indicators {location}", {"location": location}
                )
                if fred_data:
                    result["data"]["fred"] = fred_data
                    result["sources_used"].append("fred")
            except Exception as e:
                logger.debug("FRED geopolitical enrichment failed: %s", e)

        return result

    def enrich_market_trends(self, industry: str = "", **kwargs: Any) -> dict:
        """Enrich market trends data.

        Args:
            industry: Industry vertical.

        Returns:
            Dict with enriched data and sources_used list.
        """
        return self.enrich_market_demand(industry=industry, **kwargs)

    # ── Cache management ──

    def clear_cache(self) -> None:
        """Clear L1 in-memory cache."""
        self._l1_cache.clear()
        logger.info("DataOrchestrator: L1 cache cleared")

    def invalidate_source(
        self,
        source: DataSourceType,
        query: str,
        context: Optional[Dict] = None,
    ) -> None:
        """Invalidate cached data for a specific source + query."""
        key = self._cache_key(source, query, context)
        self._l1_cache.invalidate(key)
        logger.info(f"DataOrchestrator: invalidated cache for {source.value}")

    # ── Status / diagnostics ──

    def get_status(self) -> Dict[str, Any]:
        """Return orchestrator status and metrics."""
        with self._lock:
            return {
                "registered_sources": [s.value for s in self._source_handlers],
                "registered_count": len(self._source_handlers),
                "total_fetches": self._total_fetches,
                "total_cache_hits": self._total_cache_hits,
                "l1_cache": self._l1_cache.get_stats(),
                "redis_l2_available": self._redis_available,
                "source_errors": dict(self._source_error_counts),
                "modules": [m.value for m in PlatformModule],
                "data_ttl_config": DATA_TTL,
            }


# =============================================================================
# MODULE-LEVEL SINGLETON
# =============================================================================

_orchestrator: Optional[DataOrchestrator] = None
_orchestrator_lock = threading.Lock()


def get_orchestrator() -> DataOrchestrator:
    """Get or create the module-level DataOrchestrator singleton.

    Returns:
        Shared DataOrchestrator instance.
    """
    global _orchestrator
    if _orchestrator is None:
        with _orchestrator_lock:
            if _orchestrator is None:
                _orchestrator = DataOrchestrator()
                _register_default_handlers(_orchestrator)
    return _orchestrator


def _register_default_handlers(orch: DataOrchestrator) -> None:
    """Register data source handlers from existing modules.

    Each handler wraps the corresponding module's API call with
    error isolation and consistent return types.
    """
    # ── Knowledge Base ──
    try:
        from app import load_knowledge_base

        def _kb_handler(query: str, context: Dict) -> Optional[Dict]:
            """Fetch relevant KB sections based on query keywords."""
            kb = load_knowledge_base()
            if not kb:
                return None
            relevant: Dict[str, Any] = {}
            query_lower = query.lower()
            for section_key, section_data in kb.items():
                if section_key.startswith("_"):
                    continue
                if not isinstance(section_data, dict):
                    continue
                section_str = json.dumps(section_data, default=str)[:500].lower()
                if any(word in section_str for word in query_lower.split()[:5]):
                    relevant[section_key] = section_data
            return relevant if relevant else None

        orch.register_source(DataSourceType.KNOWLEDGE_BASE, _kb_handler)
    except ImportError:
        logger.info("DataOrchestrator: app.load_knowledge_base not available")

    # ── API Integrations ──
    try:
        from api_integrations import (
            adzuna,
            bls,
            fred,
            bea,
            census,
            onet,
            usajobs,
            jooble,
        )

        if adzuna:

            def _adzuna_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch job/salary data from Adzuna."""
                role = context.get("role") or query[:50]
                country = context.get("country") or "us"
                if any(
                    kw in query.lower()
                    for kw in ("salary", "pay", "compensation", "wage")
                ):
                    return adzuna.get_salary_histogram(role, country)
                return adzuna.search_jobs(role, country)

            orch.register_source(DataSourceType.ADZUNA, _adzuna_handler)

        if bls:

            def _bls_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch labor statistics from BLS."""
                if "projection" in query.lower():
                    return bls.get_employment_projections()
                soc_code = context.get("soc_code") or ""
                if soc_code:
                    return bls.get_occupational_employment(soc_code)
                return bls.get_employment_projections()

            orch.register_source(DataSourceType.BLS, _bls_handler)

        if fred:

            def _fred_handler(query: str, context: Dict) -> Optional[Dict]:
                """Fetch economic indicators from FRED."""
                result: Dict[str, Any] = {}
                if any(kw in query.lower() for kw in ("unemployment", "job", "labor")):
                    result["unemployment"] = fred.get_unemployment_rate()
                if any(kw in query.lower() for kw in ("inflation", "cpi", "price")):
                    result["cpi"] = fred.get_cpi_data(months=6)
                if any(kw in query.lower() for kw in ("gdp", "growth", "economy")):
                    result["gdp"] = fred.get_gdp_data()
                return result if result else fred.get_unemployment_rate()

            orch.register_source(DataSourceType.FRED, _fred_handler)

        if bea:

            def _bea_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch economic data from BEA."""
                state = context.get("state") or ""
                metro_fips = context.get("metro_fips") or ""
                if state or metro_fips:
                    return bea.query_regional_economics(
                        state=state,
                        metro_fips=metro_fips,
                        metric_type="all",
                    )
                return bea.get_gdp_by_state_all()

            orch.register_source(DataSourceType.BEA, _bea_handler)

        if census:

            def _census_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch demographic data from US Census."""
                state = context.get("state_fips") or ""
                return census.get_population_data(state)

            orch.register_source(DataSourceType.CENSUS, _census_handler)

        if onet:

            def _onet_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch occupational data from O*NET."""
                soc_code = context.get("soc_code")
                if soc_code:
                    return onet.get_skills(soc_code)
                role = context.get("role") or query[:50]
                results = onet.search_occupations(role)
                return results[:5] if results else None

            orch.register_source(DataSourceType.ONET, _onet_handler)

        if usajobs:

            def _usajobs_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch federal job listings from USAJobs."""
                keyword = context.get("role") or query[:50]
                return usajobs.search_jobs(keyword)

            orch.register_source(DataSourceType.USAJOBS, _usajobs_handler)

        if jooble:

            def _jooble_handler(query: str, context: Dict) -> Optional[Any]:
                """Fetch international job data from Jooble."""
                role = context.get("role") or query[:50]
                location = context.get("location") or ""
                return jooble.search_jobs(role, location)

            orch.register_source(DataSourceType.JOOBLE, _jooble_handler)

    except ImportError:
        logger.info("DataOrchestrator: api_integrations not available")

    # ── Web Scraper ──
    try:
        from web_scraper_router import scrape_url

        def _scraper_handler(query: str, context: Dict) -> Optional[Any]:
            """Scrape a URL for content."""
            url = context.get("url")
            if url:
                return scrape_url(url)
            return None

        orch.register_source(DataSourceType.WEB_SCRAPER, _scraper_handler)
    except ImportError:
        logger.info("DataOrchestrator: web_scraper_router not available")

    # ── Tavily Search ──
    try:
        from tavily_search import search as tavily_search_fn

        def _tavily_handler(query: str, context: Dict) -> Optional[Any]:
            """Search the web via Tavily."""
            max_results = context.get("max_results", 5)
            return tavily_search_fn(query, max_results=max_results)

        orch.register_source(DataSourceType.TAVILY_SEARCH, _tavily_handler)
    except ImportError:
        logger.info("DataOrchestrator: tavily_search not available")

    # ── Vector Search (S56: bounded to prevent Voyage rate-limit hang) ──
    try:
        from vector_search import search_bounded as vector_search_fn

        def _vector_handler(query: str, context: Dict) -> Optional[Any]:
            """Search knowledge base via vector similarity (3s bounded)."""
            top_k = context.get("top_k", 5)
            timeout_s = context.get("vector_timeout_s", 3.0)
            return vector_search_fn(query, top_k=top_k, timeout_s=timeout_s)

        orch.register_source(DataSourceType.VECTOR_SEARCH, _vector_handler)
    except ImportError:
        logger.info("DataOrchestrator: vector_search not available")

    logger.info(
        f"DataOrchestrator: registered {len(orch._source_handlers)} default handlers: "
        f"{[s.value for s in orch._source_handlers]}"
    )
