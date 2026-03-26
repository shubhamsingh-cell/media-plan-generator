"""Intelligent query caching for Nova AI chat responses.

Provides a Supabase-backed cache layer that intercepts frequent queries
and returns instant responses instead of expensive 35s+ LLM calls.

Cache hierarchy:
    1. In-memory dict (fastest, lost on restart)
    2. Supabase ``cache`` table (persistent, survives deploys, shared)
    3. Upstash Redis fallback (if Supabase unavailable)

Key features:
    - Semantic query normalization (lowercase, strip articles, extract entities)
    - TTL-aware: 24h for salary/market data, 1h for real-time data
    - Hit-count tracking for analytics
    - Pre-warm with top 20 common queries on startup
    - Thread-safe with minimal lock contention

Stdlib-only (no third-party dependencies beyond supabase_cache).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Supabase cache backend (optional, falls back gracefully)
# ---------------------------------------------------------------------------

try:
    from supabase_cache import (
        cache_get as _supabase_get,
        cache_set as _supabase_set,
        cache_stats as _supabase_stats,
        _ENABLED as _supabase_enabled,
    )
except ImportError:
    _supabase_enabled = False

    def _supabase_get(key: str) -> Optional[Any]:
        """Stub when supabase_cache is not available."""
        return None

    def _supabase_set(
        key: str, data: Any, ttl_seconds: int = 86400, category: str = "general"
    ) -> bool:
        """Stub when supabase_cache is not available."""
        return False

    def _supabase_stats() -> Dict[str, Any]:
        """Stub when supabase_cache is not available."""
        return {"enabled": False}


# Upstash Redis fallback (optional)
try:
    from upstash_cache import (
        cache_get as _upstash_get,
        cache_set as _upstash_set,
        _ENABLED as _upstash_enabled,
    )
except ImportError:
    _upstash_enabled = False

    def _upstash_get(key: str) -> Optional[Any]:
        """Stub when upstash_cache is not available."""
        return None

    def _upstash_set(
        key: str, data: Any, ttl_seconds: int = 86400, category: str = "api"
    ) -> None:
        """Stub when upstash_cache is not available."""
        pass


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CACHE_PREFIX = "nova_chat:"
_DEFAULT_TTL_HOURS = 24
_REALTIME_TTL_HOURS = 1
_MAX_MEMORY_CACHE = 500
_MIN_QUERY_LENGTH = 10  # skip caching very short queries

# Stop words removed during normalization
_STOP_WORDS = frozenset(
    {
        "what",
        "is",
        "the",
        "a",
        "an",
        "how",
        "does",
        "can",
        "for",
        "in",
        "of",
        "to",
        "and",
        "or",
        "my",
        "our",
        "we",
        "do",
        "are",
        "it",
        "this",
        "that",
        "which",
        "with",
        "about",
        "on",
        "at",
        "be",
        "by",
        "from",
        "has",
        "have",
        "i",
        "me",
        "you",
        "your",
        "they",
        "their",
        "was",
        "were",
        "been",
        "being",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "shall",
        "not",
        "no",
        "so",
        "if",
        "but",
        "up",
        "out",
        "there",
        "here",
        "tell",
        "give",
        "show",
        "get",
        "find",
        "please",
        "help",
        "need",
        "want",
        "know",
        "think",
        "like",
        "also",
        "just",
        "very",
        "really",
        "much",
        "some",
        "any",
        "all",
        "each",
        "every",
        "most",
        "many",
        "few",
        "more",
        "less",
    }
)

# Keywords that indicate real-time data (shorter TTL)
_REALTIME_KEYWORDS = frozenset(
    {
        "today",
        "current",
        "latest",
        "now",
        "live",
        "real-time",
        "realtime",
        "right now",
        "this week",
        "this month",
        "trending",
        "breaking",
        "recent",
    }
)

# Keywords that indicate stable data (longer TTL)
_STABLE_KEYWORDS = frozenset(
    {
        "average",
        "median",
        "salary",
        "benchmark",
        "compare",
        "versus",
        "vs",
        "difference",
        "best",
        "top",
        "typical",
        "standard",
        "industry",
        "market",
        "general",
    }
)

# Entity extraction patterns
_LOCATION_PATTERN = re.compile(
    r"\b(?:in|near|around|at)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
    re.IGNORECASE,
)
_ROLE_PATTERN = re.compile(
    r"\b(software engineer|data scientist|nurse|driver|accountant|"
    r"teacher|mechanic|electrician|plumber|cashier|warehouse worker|"
    r"security guard|pharmacist|dentist|therapist|physician|doctor|"
    r"paralegal|welder|project manager|product manager|designer|"
    r"analyst|developer|consultant|recruiter|sales|marketing|"
    r"registered nurse|cdl driver|forklift operator|chef|cook|"
    r"hr manager|devops|frontend|backend|full stack|fullstack)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# In-memory L1 cache
# ---------------------------------------------------------------------------

_memory_cache: Dict[str, Dict[str, Any]] = {}
_memory_cache_lock = threading.Lock()

# Stats tracking
_stats_lock = threading.Lock()
_stats: Dict[str, int] = {
    "hits_memory": 0,
    "hits_supabase": 0,
    "hits_upstash": 0,
    "misses": 0,
    "writes": 0,
    "skipped_short": 0,
    "skipped_followup": 0,
    "skipped_error": 0,
    "prewarm_count": 0,
}


def _stat_inc(key: str, amount: int = 1) -> None:
    """Increment a stats counter thread-safely."""
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + amount


# ---------------------------------------------------------------------------
# Query normalization
# ---------------------------------------------------------------------------


def _normalize_query(query: str) -> str:
    """Normalize query for cache key generation.

    Applies: lowercase, strip whitespace, expand contractions, remove
    articles/stop words, extract key entities (role, location), and
    sort remaining words alphabetically for order-invariant matching.

    Args:
        query: Raw user query string.

    Returns:
        Canonical normalized form, or empty string if query is too short.
    """
    if not query or len(query.strip()) < _MIN_QUERY_LENGTH:
        return ""

    text = query.lower().strip()

    # Expand common contractions
    contractions = {
        "what's": "what is",
        "how's": "how is",
        "it's": "it is",
        "who's": "who is",
        "where's": "where is",
        "there's": "there is",
        "that's": "that is",
        "doesn't": "does not",
        "don't": "do not",
        "can't": "cannot",
        "won't": "will not",
        "isn't": "is not",
        "aren't": "are not",
        "wasn't": "was not",
        "weren't": "were not",
        "couldn't": "could not",
        "shouldn't": "should not",
        "wouldn't": "would not",
    }
    for contraction, expansion in contractions.items():
        text = text.replace(contraction, expansion)

    # Strip punctuation (keep alphanumeric and spaces)
    text = re.sub(r"[^\w\s]", "", text)

    # Tokenize and remove stop words
    words = text.split()
    filtered = [w for w in words if w not in _STOP_WORDS and len(w) > 1]

    if not filtered:
        return ""

    # Sort alphabetically for order-invariant key
    filtered.sort()
    return " ".join(filtered)


def _make_cache_key(query: str, location: str = "", role: str = "") -> str:
    """Create a deterministic cache key from normalized query + context.

    Args:
        query: Normalized query string.
        location: Extracted location entity (optional).
        role: Extracted role entity (optional).

    Returns:
        Cache key string prefixed with ``nova_chat:``.
    """
    parts = [query]
    if location:
        parts.append(f"loc:{location.lower().strip()}")
    if role:
        parts.append(f"role:{role.lower().strip()}")

    combined = "|".join(parts)
    # Use SHA256 hash for consistent key length and URL-safe characters
    key_hash = hashlib.sha256(combined.encode("utf-8")).hexdigest()[:24]
    return f"{_CACHE_PREFIX}{key_hash}"


def _extract_location(query: str) -> str:
    """Extract location entity from query string.

    Args:
        query: Raw user query.

    Returns:
        Location string if found, empty string otherwise.
    """
    match = _LOCATION_PATTERN.search(query)
    return match.group(1).strip() if match else ""


def _extract_role(query: str) -> str:
    """Extract job role entity from query string.

    Args:
        query: Raw user query.

    Returns:
        Role string if found, empty string otherwise.
    """
    match = _ROLE_PATTERN.search(query)
    return match.group(1).strip().lower() if match else ""


def _determine_ttl(query: str) -> int:
    """Determine appropriate TTL based on query content.

    Real-time queries (today, current, trending) get 1 hour TTL.
    Stable data queries (salary, benchmark, compare) get 24 hour TTL.

    Args:
        query: Raw user query.

    Returns:
        TTL in seconds.
    """
    query_lower = query.lower()

    # Check for real-time indicators first (takes priority)
    if any(kw in query_lower for kw in _REALTIME_KEYWORDS):
        return _REALTIME_TTL_HOURS * 3600

    # Default: stable data TTL
    return _DEFAULT_TTL_HOURS * 3600


def _should_skip_caching(
    query: str,
    conversation_history: Optional[list] = None,
    response: Optional[Dict[str, Any]] = None,
) -> tuple[bool, str]:
    """Determine if a query/response should skip caching.

    Skip caching for:
    - Very short queries (< 10 chars)
    - Follow-up questions (need conversation context)
    - Error responses
    - Low-confidence responses

    Args:
        query: Raw user query.
        conversation_history: Previous messages for context detection.
        response: The response dict to evaluate (for cache-write decisions).

    Returns:
        Tuple of (should_skip: bool, reason: str).
    """
    # Too short
    if len(query.strip()) < _MIN_QUERY_LENGTH:
        return True, "short_query"

    # Follow-up detection: if history has 3+ messages, likely a follow-up
    history = conversation_history or []
    if len(history) > 2:
        return True, "followup"

    # Check response quality for write decisions
    if response is not None:
        # Error responses
        if response.get("error") or response.get("error_type"):
            return True, "error_response"

        # Low confidence
        if (response.get("confidence") or 0) < 0.5:
            return True, "low_confidence"

        # Empty response
        resp_text = response.get("response") or ""
        if len(resp_text.strip()) < 20:
            return True, "empty_response"

    return False, ""


# ---------------------------------------------------------------------------
# Cache operations
# ---------------------------------------------------------------------------


def get_cached_response(
    query: str,
    conversation_history: Optional[list] = None,
) -> Optional[Dict[str, Any]]:
    """Check all cache layers for a matching query response.

    Lookup order: memory -> Supabase -> Upstash Redis.
    On hit from a lower layer, promotes to memory for faster subsequent reads.

    Args:
        query: Raw user query string.
        conversation_history: Previous messages (used to skip follow-ups).

    Returns:
        Cached response dict with ``cached: True`` flag, or None on miss.
    """
    # Pre-flight checks
    should_skip, reason = _should_skip_caching(query, conversation_history)
    if should_skip:
        _stat_inc(f"skipped_{reason}")
        return None

    # Normalize and build cache key
    normalized = _normalize_query(query)
    if not normalized:
        _stat_inc("skipped_short")
        return None

    location = _extract_location(query)
    role = _extract_role(query)
    cache_key = _make_cache_key(normalized, location, role)

    now = time.time()

    # 1) Memory check (fastest)
    with _memory_cache_lock:
        entry = _memory_cache.get(cache_key)
        if entry is not None:
            if (entry.get("expires") or 0) > now:
                _stat_inc("hits_memory")
                result = entry.get("data", {}).copy()
                result["cached"] = True
                result["cache_layer"] = "memory"
                logger.info(
                    "Nova intelligent cache HIT (memory) key=%s", cache_key[:32]
                )
                return result
            else:
                del _memory_cache[cache_key]

    # 2) Supabase check (persistent)
    if _supabase_enabled:
        try:
            cached = _supabase_get(cache_key)
            if cached and isinstance(cached, dict):
                _stat_inc("hits_supabase")
                # Promote to memory
                ttl = _determine_ttl(query)
                with _memory_cache_lock:
                    _memory_cache[cache_key] = {
                        "data": cached,
                        "expires": now + ttl,
                        "created": now,
                    }
                    _evict_memory_if_needed()
                result = cached.copy()
                result["cached"] = True
                result["cache_layer"] = "supabase"
                logger.info(
                    "Nova intelligent cache HIT (supabase) key=%s", cache_key[:32]
                )
                return result
        except Exception as exc:
            logger.warning("Supabase cache read failed (non-fatal): %s", exc)

    # 3) Upstash Redis fallback
    if _upstash_enabled:
        try:
            redis_key = f"nova_icache:{cache_key}"
            cached = _upstash_get(redis_key)
            if cached and isinstance(cached, dict):
                _stat_inc("hits_upstash")
                # Promote to memory
                ttl = _determine_ttl(query)
                with _memory_cache_lock:
                    _memory_cache[cache_key] = {
                        "data": cached,
                        "expires": now + ttl,
                        "created": now,
                    }
                    _evict_memory_if_needed()
                result = cached.copy()
                result["cached"] = True
                result["cache_layer"] = "upstash"
                logger.info(
                    "Nova intelligent cache HIT (upstash) key=%s", cache_key[:32]
                )
                return result
        except Exception as exc:
            logger.warning("Upstash cache read failed (non-fatal): %s", exc)

    _stat_inc("misses")
    return None


def cache_response(
    query: str,
    response: Dict[str, Any],
    conversation_history: Optional[list] = None,
    ttl_hours: Optional[int] = None,
) -> bool:
    """Store a response in all available cache layers.

    Writes to: memory + Supabase + Upstash Redis (if available).
    Automatically determines TTL based on query content unless overridden.

    Args:
        query: Raw user query string.
        response: Response dict to cache (response, sources, confidence, tools_used).
        conversation_history: Previous messages (used to skip follow-ups).
        ttl_hours: Override TTL in hours (auto-detected if None).

    Returns:
        True if cached successfully in at least one layer.
    """
    # Pre-flight checks
    should_skip, reason = _should_skip_caching(query, conversation_history, response)
    if should_skip:
        _stat_inc(f"skipped_{reason}")
        return False

    # Normalize and build cache key
    normalized = _normalize_query(query)
    if not normalized:
        _stat_inc("skipped_short")
        return False

    location = _extract_location(query)
    role = _extract_role(query)
    cache_key = _make_cache_key(normalized, location, role)

    # Determine TTL
    if ttl_hours is not None:
        ttl_seconds = ttl_hours * 3600
    else:
        ttl_seconds = _determine_ttl(query)

    # Strip transient fields before caching
    cache_data = {
        k: v
        for k, v in response.items()
        if k not in ("cached", "cache_layer", "follow_ups")
    }

    now = time.time()
    success = False

    # 1) Memory write
    with _memory_cache_lock:
        _memory_cache[cache_key] = {
            "data": cache_data,
            "expires": now + ttl_seconds,
            "created": now,
        }
        _evict_memory_if_needed()
    success = True

    # 2) Supabase write (background thread to avoid blocking)
    if _supabase_enabled:

        def _write_supabase() -> None:
            try:
                _supabase_set(
                    key=cache_key,
                    data=cache_data,
                    ttl_seconds=ttl_seconds,
                    category="nova_chat",
                )
            except Exception as exc:
                logger.warning("Supabase cache write failed (non-fatal): %s", exc)

        threading.Thread(target=_write_supabase, daemon=True).start()

    # 3) Upstash Redis write (background thread)
    if _upstash_enabled:

        def _write_upstash() -> None:
            try:
                redis_key = f"nova_icache:{cache_key}"
                _upstash_set(
                    redis_key, cache_data, ttl_seconds=ttl_seconds, category="nova_chat"
                )
            except Exception as exc:
                logger.warning("Upstash cache write failed (non-fatal): %s", exc)

        threading.Thread(target=_write_upstash, daemon=True).start()

    if success:
        _stat_inc("writes")
        logger.info(
            "Nova intelligent cache WRITE key=%s ttl=%ds layers=[memory%s%s]",
            cache_key[:32],
            ttl_seconds,
            "+supabase" if _supabase_enabled else "",
            "+upstash" if _upstash_enabled else "",
        )

    return success


# ---------------------------------------------------------------------------
# Cache stats
# ---------------------------------------------------------------------------


def get_cache_stats() -> Dict[str, Any]:
    """Return comprehensive cache statistics.

    Includes hit rates, cache size, layer availability, and top queries.

    Returns:
        Dict with hit_rate, size, layers, counters, and supabase stats.
    """
    with _stats_lock:
        counters = dict(_stats)

    total_hits = (
        counters.get("hits_memory", 0)
        + counters.get("hits_supabase", 0)
        + counters.get("hits_upstash", 0)
    )
    total_lookups = total_hits + counters.get("misses", 0)
    hit_rate = (total_hits / total_lookups * 100) if total_lookups > 0 else 0.0

    with _memory_cache_lock:
        memory_size = len(_memory_cache)
        # Count non-expired entries
        now = time.time()
        active_entries = sum(
            1 for v in _memory_cache.values() if (v.get("expires") or 0) > now
        )

    result: Dict[str, Any] = {
        "hit_rate_percent": round(hit_rate, 1),
        "total_hits": total_hits,
        "total_lookups": total_lookups,
        "memory_size": memory_size,
        "memory_active": active_entries,
        "memory_max": _MAX_MEMORY_CACHE,
        "counters": counters,
        "layers": {
            "memory": True,
            "supabase": _supabase_enabled,
            "upstash": _upstash_enabled,
        },
    }

    # Include Supabase stats if available
    if _supabase_enabled:
        try:
            sb_stats = _supabase_stats()
            result["supabase_stats"] = sb_stats
        except Exception as exc:
            result["supabase_stats"] = {"error": str(exc)}

    return result


# ---------------------------------------------------------------------------
# Memory eviction
# ---------------------------------------------------------------------------


def _evict_memory_if_needed() -> None:
    """Evict oldest entries from memory cache if over capacity.

    Must be called while holding ``_memory_cache_lock``.
    """
    if len(_memory_cache) <= _MAX_MEMORY_CACHE:
        return

    # Remove expired entries first
    now = time.time()
    expired_keys = [
        k for k, v in _memory_cache.items() if (v.get("expires") or 0) <= now
    ]
    for k in expired_keys:
        del _memory_cache[k]

    # If still over capacity, evict oldest
    while len(_memory_cache) > _MAX_MEMORY_CACHE:
        oldest_key = min(
            _memory_cache, key=lambda k: _memory_cache[k].get("created") or 0
        )
        del _memory_cache[oldest_key]


# ---------------------------------------------------------------------------
# Pre-warm cache with common queries
# ---------------------------------------------------------------------------

# Top 20 common queries to pre-warm on startup
_PREWARM_QUERIES: List[Dict[str, str]] = [
    {
        "query": "average salary for software engineer in San Francisco",
        "role": "software engineer",
        "location": "San Francisco",
    },
    {
        "query": "average salary for nurse in New York",
        "role": "nurse",
        "location": "New York",
    },
    {
        "query": "average salary for data scientist in Austin",
        "role": "data scientist",
        "location": "Austin",
    },
    {"query": "compare Indeed vs LinkedIn", "role": "", "location": ""},
    {"query": "best job boards for hiring nurses", "role": "nurse", "location": ""},
    {"query": "recruitment marketing budget allocation", "role": "", "location": ""},
    {"query": "cost per applicant benchmarks", "role": "", "location": ""},
    {"query": "cost per hire industry average", "role": "", "location": ""},
    {
        "query": "best channels for hiring truck drivers",
        "role": "driver",
        "location": "",
    },
    {
        "query": "salary benchmark for accountant in Chicago",
        "role": "accountant",
        "location": "Chicago",
    },
    {"query": "how to reduce cost per applicant", "role": "", "location": ""},
    {"query": "job board performance comparison", "role": "", "location": ""},
    {
        "query": "average salary for registered nurse in Texas",
        "role": "registered nurse",
        "location": "Texas",
    },
    {"query": "Indeed vs ZipRecruiter vs LinkedIn", "role": "", "location": ""},
    {"query": "recruitment marketing trends 2026", "role": "", "location": ""},
    {
        "query": "best programmatic job advertising platforms",
        "role": "",
        "location": "",
    },
    {
        "query": "average time to fill for software engineer",
        "role": "software engineer",
        "location": "",
    },
    {"query": "hiring difficulty index by role", "role": "", "location": ""},
    {
        "query": "media plan for hiring warehouse workers",
        "role": "warehouse worker",
        "location": "",
    },
    {
        "query": "salary comparison by city for developers",
        "role": "developer",
        "location": "",
    },
]


def prewarm_cache() -> int:
    """Pre-warm the memory cache with common query keys.

    This does NOT pre-fill responses -- it only registers the normalized
    keys so the first real response for each query gets cached properly.
    The actual pre-warming happens when real responses flow through.

    Called during server startup in a background thread.

    Returns:
        Number of queries registered for pre-warming.
    """
    count = 0
    for entry in _PREWARM_QUERIES:
        try:
            query = entry["query"]
            normalized = _normalize_query(query)
            if not normalized:
                continue

            location = entry.get("location") or _extract_location(query)
            role = entry.get("role") or _extract_role(query)
            cache_key = _make_cache_key(normalized, location, role)

            # Check Supabase for existing cached responses to promote to memory
            if _supabase_enabled:
                try:
                    cached = _supabase_get(cache_key)
                    if cached and isinstance(cached, dict):
                        ttl = _determine_ttl(query)
                        with _memory_cache_lock:
                            _memory_cache[cache_key] = {
                                "data": cached,
                                "expires": time.time() + ttl,
                                "created": time.time(),
                            }
                        count += 1
                        logger.debug("Pre-warm HIT from Supabase: %s", query[:50])
                        continue
                except Exception:
                    pass

            # Check Upstash for existing cached responses
            if _upstash_enabled:
                try:
                    redis_key = f"nova_icache:{cache_key}"
                    cached = _upstash_get(redis_key)
                    if cached and isinstance(cached, dict):
                        ttl = _determine_ttl(query)
                        with _memory_cache_lock:
                            _memory_cache[cache_key] = {
                                "data": cached,
                                "expires": time.time() + ttl,
                                "created": time.time(),
                            }
                        count += 1
                        logger.debug("Pre-warm HIT from Upstash: %s", query[:50])
                        continue
                except Exception:
                    pass

        except Exception as exc:
            logger.debug("Pre-warm skip for query: %s", exc)

    _stat_inc("prewarm_count", count)
    logger.info("Nova intelligent cache pre-warm complete: %d queries loaded", count)
    return count


def start_prewarm_thread() -> None:
    """Start cache pre-warming in a background thread.

    Safe to call during server startup -- non-blocking, daemon thread.
    """

    def _prewarm() -> None:
        # Small delay to let the server finish starting
        time.sleep(5)
        try:
            prewarm_cache()
        except Exception as exc:
            logger.warning("Cache pre-warm failed (non-fatal): %s", exc)

    t = threading.Thread(target=_prewarm, name="nova-cache-prewarm", daemon=True)
    t.start()
    logger.info("Nova intelligent cache pre-warm thread started")
