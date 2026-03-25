"""Edge-First LLM Routing -- intelligent provider selection.

Routes queries to the optimal LLM based on query complexity, cost,
and latency requirements.  Thread-safe with rolling-window latency
tracking and cost optimization.

Complexity tiers:
    SIMPLE   -- greetings, basic facts        (<1s target)
    MODERATE -- single-tool queries           (<3s target)
    COMPLEX  -- multi-tool, analysis          (<8s target)
    EXPERT   -- deep research, comparisons    (<20s target)
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# COMPLEXITY TIERS
# ═══════════════════════════════════════════════════════════════════════════════


class ComplexityTier(str, Enum):
    """Query complexity classification."""

    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    EXPERT = "expert"


# Latency targets per tier (seconds)
TIER_LATENCY_TARGETS: dict[ComplexityTier, float] = {
    ComplexityTier.SIMPLE: 1.0,
    ComplexityTier.MODERATE: 3.0,
    ComplexityTier.COMPLEX: 8.0,
    ComplexityTier.EXPERT: 20.0,
}

# ═══════════════════════════════════════════════════════════════════════════════
# PROVIDER DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════

# Per-provider metadata: cost_per_1k_tokens, tier (free/paid), avg baseline latency
PROVIDER_CATALOG: dict[str, dict[str, Any]] = {
    # --- Fastest / cheapest (SIMPLE tier) ---
    "groq": {
        "name": "Groq Llama 3.3 70B",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 0.4,
    },
    "cerebras": {
        "name": "Cerebras Llama 3.3 70B",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 0.5,
    },
    "sambanova": {
        "name": "SambaNova Llama 3.1 405B",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 0.6,
    },
    "cloudflare": {
        "name": "Cloudflare Workers AI",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 0.7,
    },
    # --- Balanced (MODERATE tier) ---
    "gemini": {
        "name": "Gemini 2.5 Flash",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 1.2,
    },
    "mistral": {
        "name": "Mistral Small",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 1.5,
    },
    "siliconflow": {
        "name": "SiliconFlow Qwen2.5 7B",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 1.8,
    },
    "together": {
        "name": "Together AI",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 1.6,
    },
    "zhipu": {
        "name": "Zhipu GLM-4",
        "cost_per_1k": 0.0,
        "tier": "free",
        "baseline_latency_s": 2.0,
    },
    # --- Capable (COMPLEX tier) ---
    "xiaomi_mimo": {
        "name": "Xiaomi MiMo V2 Flash",
        "cost_per_1k": 0.0001,
        "tier": "free",
        "baseline_latency_s": 2.5,
    },
    "claude_haiku": {
        "name": "Claude Haiku 4.5",
        "cost_per_1k": 0.001,
        "tier": "paid",
        "baseline_latency_s": 1.8,
    },
    "gpt4o": {
        "name": "GPT-4o",
        "cost_per_1k": 0.005,
        "tier": "paid",
        "baseline_latency_s": 2.5,
    },
    "xai": {
        "name": "xAI Grok",
        "cost_per_1k": 0.005,
        "tier": "paid",
        "baseline_latency_s": 2.0,
    },
    # --- Best quality (EXPERT tier) ---
    "claude": {
        "name": "Claude Sonnet 4",
        "cost_per_1k": 0.015,
        "tier": "paid",
        "baseline_latency_s": 4.0,
    },
    "claude_opus": {
        "name": "Claude Opus 4.6",
        "cost_per_1k": 0.075,
        "tier": "paid",
        "baseline_latency_s": 8.0,
    },
}

# Tier -> ordered provider preferences (first = most preferred)
TIER_PROVIDERS: dict[ComplexityTier, list[str]] = {
    ComplexityTier.SIMPLE: ["groq", "cerebras", "sambanova", "cloudflare"],
    ComplexityTier.MODERATE: [
        "gemini",
        "mistral",
        "siliconflow",
        "together",
        "zhipu",
    ],
    ComplexityTier.COMPLEX: ["xiaomi_mimo", "claude_haiku", "gpt4o", "xai"],
    ComplexityTier.EXPERT: ["claude", "gpt4o", "claude_opus"],
}

# ═══════════════════════════════════════════════════════════════════════════════
# QUERY CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════════

# Regex patterns for classification
_GREETING_PATTERNS = re.compile(
    r"^(hi|hello|hey|thanks|thank you|ok|okay|bye|goodbye|good morning"
    r"|good evening|good night|howdy|sup|yo|what's up|cheers)\b",
    re.IGNORECASE,
)

_EXPERT_KEYWORDS = re.compile(
    r"\b(detailed|comprehensive|in-depth|thorough|exhaustive|deep dive"
    r"|step by step|break down|explain everything|full analysis"
    r"|complete overview|end to end)\b",
    re.IGNORECASE,
)

_COMPLEX_KEYWORDS = re.compile(
    r"\b(compare|contrast|analyze|breakdown|versus|vs\.|trade-?off"
    r"|pros and cons|evaluate|benchmark|assess|correlat|impact of"
    r"|relationship between|how does .+ affect)\b",
    re.IGNORECASE,
)

_MULTI_ENTITY_PATTERN = re.compile(
    r"(?:\b(?:and|,|vs\.?|versus|or)\b.*){2,}",
    re.IGNORECASE,
)


def classify_query(query: str) -> ComplexityTier:
    """Classify a query into a complexity tier.

    Uses keyword patterns, query length, and structural heuristics
    to determine the optimal processing tier.

    Args:
        query: The user's natural-language query.

    Returns:
        The determined ComplexityTier.
    """
    text = query.strip()
    word_count = len(text.split())

    # Very short or greeting -> SIMPLE
    if word_count <= 4 and _GREETING_PATTERNS.match(text):
        return ComplexityTier.SIMPLE

    # Short queries with a question mark are at least MODERATE
    has_question = "?" in text

    # Very short non-questions -> SIMPLE
    if word_count <= 3 and not has_question:
        return ComplexityTier.SIMPLE

    # Expert signals: explicit depth keywords OR very long multi-part queries
    if _EXPERT_KEYWORDS.search(text):
        return ComplexityTier.EXPERT

    # Long queries with multiple questions (delimited by ?)
    question_count = text.count("?")
    if question_count >= 3 or word_count >= 80:
        return ComplexityTier.EXPERT

    # Complex signals: comparison/analysis keywords + multi-entity references
    if _COMPLEX_KEYWORDS.search(text):
        if _MULTI_ENTITY_PATTERN.search(text) or word_count >= 8:
            return ComplexityTier.COMPLEX
        return ComplexityTier.MODERATE

    # Multi-entity without explicit analysis -> MODERATE at least
    if _MULTI_ENTITY_PATTERN.search(text) and word_count >= 15:
        return ComplexityTier.COMPLEX

    # Medium-length single questions -> MODERATE
    if word_count >= 8:
        return ComplexityTier.MODERATE

    # Short question-form queries -> MODERATE
    if has_question:
        return ComplexityTier.MODERATE

    return ComplexityTier.SIMPLE


# ═══════════════════════════════════════════════════════════════════════════════
# LATENCY TRACKER (Thread-safe rolling window)
# ═══════════════════════════════════════════════════════════════════════════════

_LATENCY_WINDOW_SIZE = 50  # keep last N measurements per provider
_LATENCY_MAX_AGE_S = 600.0  # discard measurements older than 10 minutes


class _LatencyTracker:
    """Thread-safe rolling-window latency tracker per provider."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # provider_id -> deque of (timestamp, latency_seconds)
        self._windows: dict[str, deque[tuple[float, float]]] = {}

    def record(self, provider_id: str, latency_s: float) -> None:
        """Record a latency measurement for a provider."""
        now = time.time()
        with self._lock:
            window = self._windows.setdefault(
                provider_id, deque(maxlen=_LATENCY_WINDOW_SIZE)
            )
            window.append((now, latency_s))

    def get_avg_latency(self, provider_id: str) -> float | None:
        """Get rolling-average latency for a provider.

        Returns None if no recent measurements exist.
        """
        now = time.time()
        cutoff = now - _LATENCY_MAX_AGE_S
        with self._lock:
            window = self._windows.get(provider_id)
            if not window:
                return None
            recent = [lat for ts, lat in window if ts >= cutoff]
        if not recent:
            return None
        return sum(recent) / len(recent)

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get latency stats for all tracked providers."""
        now = time.time()
        cutoff = now - _LATENCY_MAX_AGE_S
        result: dict[str, dict[str, Any]] = {}
        with self._lock:
            for pid, window in self._windows.items():
                recent = [lat for ts, lat in window if ts >= cutoff]
                if recent:
                    result[pid] = {
                        "avg_s": round(sum(recent) / len(recent), 3),
                        "min_s": round(min(recent), 3),
                        "max_s": round(max(recent), 3),
                        "samples": len(recent),
                    }
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# COST TRACKER
# ═══════════════════════════════════════════════════════════════════════════════


class _CostTracker:
    """Thread-safe cost-per-tier tracker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # tier -> list of (timestamp, cost_usd)
        self._history: dict[str, deque[tuple[float, float]]] = {}
        self._totals: dict[str, float] = {}
        self._counts: dict[str, int] = {}

    def record(self, tier: str, cost_usd: float) -> None:
        """Record a cost for a tier."""
        now = time.time()
        with self._lock:
            dq = self._history.setdefault(tier, deque(maxlen=200))
            dq.append((now, cost_usd))
            self._totals[tier] = self._totals.get(tier, 0.0) + cost_usd
            self._counts[tier] = self._counts.get(tier, 0) + 1

    def get_avg_cost(self, tier: str) -> float:
        """Get average cost per query for a tier."""
        with self._lock:
            count = self._counts.get(tier, 0)
            total = self._totals.get(tier, 0.0)
        if count == 0:
            return 0.0
        return total / count

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get cost stats for all tiers."""
        with self._lock:
            result: dict[str, dict[str, Any]] = {}
            for tier in self._totals:
                count = self._counts.get(tier, 0)
                total = self._totals.get(tier, 0.0)
                result[tier] = {
                    "total_cost_usd": round(total, 6),
                    "query_count": count,
                    "avg_cost_usd": round(total / max(1, count), 6),
                }
            return result


# ═══════════════════════════════════════════════════════════════════════════════
# ROUTING STATS TRACKER
# ═══════════════════════════════════════════════════════════════════════════════


class _RoutingStats:
    """Thread-safe routing decision statistics."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tier_counts: dict[str, int] = {}
        self._provider_counts: dict[str, int] = {}
        self._total_routes: int = 0
        self._start_time: float = time.time()

    def record_route(self, tier: str, provider: str) -> None:
        """Record a routing decision."""
        with self._lock:
            self._tier_counts[tier] = self._tier_counts.get(tier, 0) + 1
            self._provider_counts[provider] = self._provider_counts.get(provider, 0) + 1
            self._total_routes += 1

    def get_stats(self) -> dict[str, Any]:
        """Get all routing stats."""
        with self._lock:
            return {
                "total_routes": self._total_routes,
                "uptime_s": round(time.time() - self._start_time, 1),
                "tier_distribution": dict(self._tier_counts),
                "provider_usage": dict(self._provider_counts),
            }


# ═══════════════════════════════════════════════════════════════════════════════
# SINGLETON INSTANCES
# ═══════════════════════════════════════════════════════════════════════════════

_latency_tracker = _LatencyTracker()
_cost_tracker = _CostTracker()
_routing_stats = _RoutingStats()


# ═══════════════════════════════════════════════════════════════════════════════
# CORE ROUTING LOGIC
# ═══════════════════════════════════════════════════════════════════════════════


def _estimate_latency(provider_id: str) -> float:
    """Estimate latency for a provider using tracked data or baseline.

    Args:
        provider_id: The provider identifier.

    Returns:
        Estimated latency in seconds.
    """
    tracked = _latency_tracker.get_avg_latency(provider_id)
    if tracked is not None:
        return tracked
    catalog_entry = PROVIDER_CATALOG.get(provider_id, {})
    return catalog_entry.get("baseline_latency_s", 5.0)


def _estimate_cost(provider_id: str, token_estimate: int = 500) -> float:
    """Estimate cost for a query routed to a provider.

    Args:
        provider_id: The provider identifier.
        token_estimate: Estimated token count for the query+response.

    Returns:
        Estimated cost in USD.
    """
    catalog_entry = PROVIDER_CATALOG.get(provider_id, {})
    cost_per_1k = catalog_entry.get("cost_per_1k", 0.0)
    return cost_per_1k * (token_estimate / 1000.0)


def _find_cheaper_alternative(
    tier: ComplexityTier,
    current_provider: str,
) -> str | None:
    """Suggest a cheaper alternative provider within the same tier.

    Args:
        tier: The current complexity tier.
        current_provider: The currently selected provider.

    Returns:
        A cheaper provider ID, or None if no cheaper option exists.
    """
    candidates = TIER_PROVIDERS.get(tier, [])
    current_cost = _estimate_cost(current_provider)

    for candidate in candidates:
        if candidate == current_provider:
            continue
        candidate_cost = _estimate_cost(candidate)
        candidate_latency = _estimate_latency(candidate)
        target_latency = TIER_LATENCY_TARGETS.get(tier, 10.0)

        # Must meet latency target and be cheaper
        if candidate_cost < current_cost and candidate_latency <= target_latency:
            return candidate

    return None


def get_optimal_route(
    query: str,
    user_tier: str = "free",
) -> dict[str, Any]:
    """Select the optimal LLM provider for a query.

    Classifies query complexity, selects the best provider based on
    latency targets and cost, and returns a complete routing decision.

    Args:
        query: The user's natural-language query.
        user_tier: User access tier -- "free" or "paid".

    Returns:
        Dict with keys: provider, provider_name, tier, expected_latency_s,
        expected_cost_usd, latency_target_s, meets_target, cheaper_alternative.
    """
    complexity = classify_query(query)
    candidates = list(TIER_PROVIDERS.get(complexity, []))
    latency_target = TIER_LATENCY_TARGETS[complexity]

    # Filter by user tier: free users can only use free providers
    if user_tier == "free":
        candidates = [
            p for p in candidates if PROVIDER_CATALOG.get(p, {}).get("tier") == "free"
        ]

    # Rank candidates by: (meets_target, estimated_latency, cost)
    scored: list[tuple[float, str]] = []
    for provider_id in candidates:
        est_lat = _estimate_latency(provider_id)
        est_cost = _estimate_cost(provider_id)
        # Prefer providers that meet the latency target
        meets = 0.0 if est_lat <= latency_target else 1.0
        scored.append((meets * 1000 + est_lat + est_cost * 10, provider_id))

    scored.sort(key=lambda x: x[0])

    if not scored:
        # Fallback: use first available from any tier
        best_provider = "gemini"
    else:
        best_provider = scored[0][1]

    est_latency = _estimate_latency(best_provider)
    est_cost = _estimate_cost(best_provider)
    meets_target = est_latency <= latency_target

    # Find a cheaper option if available
    cheaper = _find_cheaper_alternative(complexity, best_provider)

    # Record the routing decision
    _routing_stats.record_route(complexity.value, best_provider)

    catalog_entry = PROVIDER_CATALOG.get(best_provider, {})

    return {
        "provider": best_provider,
        "provider_name": catalog_entry.get("name", best_provider),
        "tier": complexity.value,
        "expected_latency_s": round(est_latency, 3),
        "expected_cost_usd": round(est_cost, 6),
        "latency_target_s": latency_target,
        "meets_target": meets_target,
        "cheaper_alternative": cheaper,
        "user_tier": user_tier,
        "candidates_evaluated": len(scored),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FEEDBACK / RECORDING
# ═══════════════════════════════════════════════════════════════════════════════


def record_completion(
    provider_id: str,
    tier: str,
    latency_s: float,
    cost_usd: float,
) -> None:
    """Record the outcome of a routed query for future optimization.

    Call this after a query completes to feed latency and cost data
    back into the router's predictors.

    Args:
        provider_id: The provider that handled the query.
        tier: The complexity tier that was used.
        latency_s: Actual wall-clock latency in seconds.
        cost_usd: Actual cost in USD.
    """
    try:
        _latency_tracker.record(provider_id, latency_s)
        _cost_tracker.record(tier, cost_usd)
    except (ValueError, TypeError, KeyError) as exc:
        logger.error(f"Failed to record edge router completion: {exc}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# STATS / HEALTH
# ═══════════════════════════════════════════════════════════════════════════════


def get_router_stats() -> dict[str, Any]:
    """Get comprehensive edge router statistics for /api/health.

    Returns:
        Dict with routing stats, latency data, cost data, and provider catalog.
    """
    routing = _routing_stats.get_stats()
    latency = _latency_tracker.get_all_stats()
    cost = _cost_tracker.get_stats()

    return {
        "status": "ok",
        "routing": routing,
        "latency_tracking": latency,
        "cost_tracking": cost,
        "tiers": {
            tier.value: {
                "target_latency_s": TIER_LATENCY_TARGETS[tier],
                "providers": TIER_PROVIDERS[tier],
            }
            for tier in ComplexityTier
        },
        "total_providers": len(PROVIDER_CATALOG),
    }
