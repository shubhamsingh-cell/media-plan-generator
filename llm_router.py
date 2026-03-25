"""
llm_router.py -- Smart LLM Provider Router for Nova Chat (v4.0)

Routes LLM API calls to the optimal provider based on task type,
with automatic fallback, circuit breaker, rate-aware routing,
response caching, and provider health scoring.

Provider priority (free-first, then paid by cost-efficiency):
    FREE TIER:
    1.  Gemini 2.0 Flash  -- free, structured data, JSON output, code
    2.  Groq Llama 3.3 70B -- free, conversational, complex reasoning
    3.  Zhipu AI (GLM-4-Flash) -- free unlimited, strong multilingual (Chinese + English)
    4.  Cerebras Llama 3.3 70B -- free 1M tokens/day, hot spare (independent infra)
    5.  Mistral Small -- free tier, strong JSON + multilingual
    6.  NVIDIA NIM (Nemotron 30B) -- free dev program, NVIDIA-optimized inference
    7.  SambaNova (Llama 3.1 405B) -- free, largest open model, fastest inference (RDU)
    8.  SiliconFlow (Qwen2.5 7B) -- free $0.05/M tokens, OpenAI-compatible
    9.  Cloudflare Workers AI (Llama 3.3 70B) -- free 10K neurons/day, edge-distributed
    10. Together AI (Llama 3.3 70B Turbo) -- $25 free credit, fast inference
    11. Moonshot Kimi (moonshot-v1-8k) -- limited free tier, strong Asian market coverage
    12. OpenRouter (Llama 4 Maverick) -- free models via single gateway
    13. OpenRouter (Qwen3 Coder) -- free, code generation specialist
    14. OpenRouter (Arcee Trinity) -- free, complex reasoning
    15. OpenRouter (Liquid LFM 2.5) -- free, novel architecture
    16. OpenRouter (01.AI Yi Large) -- free, good general purpose
    17. OpenRouter (DeepSeek R1 Reasoning) -- free, strong reasoning/research
    18. OpenRouter (Google Gemma 3 27B) -- free, structured output + verification
    19. xAI Grok (grok-3-mini-fast) -- free signup credits ($25), strong reasoning
    20. HuggingFace Inference (Mistral 7B) -- free rate-limited, fallback

    PAID TIER:
    21. Claude Haiku 4.5 (Anthropic) -- paid, fast + cheap
    22. GPT-4o (OpenAI) -- paid, strong at structured + conversational + reasoning
    23. Claude Sonnet 4 (Anthropic) -- paid, high quality, strong tool_use
    24. Claude Opus 4.6 (Anthropic) -- paid, last resort, highest quality

Task classification (8 types):
    - STRUCTURED:     benchmark lookups, CPC/CPA queries, JSON output
    - CONVERSATIONAL: explain strategy, general Q&A, advisory
    - COMPLEX:        what-if scenarios, role decomposition, multi-step analysis
    - CODE:           formula generation, calculations, data transforms
    - VERIFICATION:   fact-checking, grounding verification, accuracy validation
    - RESEARCH:       market research, geopolitical analysis, macro-economic outlook
    - NARRATIVE:      long-form text, executive summaries, report writing
    - BATCH:          high-throughput bulk operations, comprehensive reports

Features (v4.0):
    - Circuit breaker: 5 consecutive failures -> 60s cooldown (hard cutoff)
    - Health scoring: 0.0-1.0 per provider, decays on failure, influences routing order
    - Rate-aware routing: sliding window rate limiter per provider, skip without penalty
    - Response cache: LRU with task-aware TTL (5-min default, 15-min for verification/compliance)

Each provider has independent circuit breaker (5 failures -> 60s cooldown)
and per-minute rate tracking.  24 total providers, 20 free + 4 paid.

Stdlib-only, thread-safe.
"""

from __future__ import annotations

import collections
import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Generator, List, Optional, Tuple

# L3 persistent cache (Upstash Redis) -- graceful no-op when not configured
try:
    from upstash_cache import (
        cache_get as _upstash_get,
        cache_set as _upstash_set,
        _ENABLED as _UPSTASH_ENABLED,
    )
except ImportError:
    _UPSTASH_ENABLED = False

    def _upstash_get(key: str) -> Optional[Any]:
        return None  # noqa: E731

    def _upstash_set(
        key: str, data: Any, ttl_seconds: int = 86400, category: str = "api"
    ) -> None:
        pass  # noqa: E731


logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# PROVIDER CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Task types for routing
TASK_STRUCTURED = "structured"
TASK_CONVERSATIONAL = "conversational"
TASK_COMPLEX = "complex"
TASK_CODE = "code"
TASK_VERIFICATION = "verification"  # Fact-checking, grounding verification
TASK_RESEARCH = "research"  # Market research, geopolitical analysis
TASK_NARRATIVE = "narrative"  # Long-form text, executive summaries
TASK_BATCH = "batch"  # High-throughput, latency-tolerant

# v4.0 Platform module task types
TASK_CAMPAIGN_PLAN = "campaign_plan"  # Command Center: full campaign generation
TASK_BUDGET_OPTIMIZE = "budget_optimize"  # Command Center: budget allocation
TASK_COMPLIANCE_CHECK = "compliance_check"  # Command Center: regulatory compliance
TASK_MARKET_ANALYSIS = "market_analysis"  # Intelligence Hub: market deep-dive
TASK_COMPETITOR_SCAN = "competitor_scan"  # Intelligence Hub: competitive intel
TASK_TALENT_MAP = "talent_map"  # Intelligence Hub: talent landscape
TASK_CHAT_RESPONSE = "chat_response"  # Nova AI: conversational response
TASK_ACTION_EXECUTE = "action_execute"  # Nova AI: action execution
TASK_CONTEXT_SUMMARIZE = "context_summarize"  # Nova AI: context compression

# Provider IDs
GEMINI = "gemini"
GEMINI_FLASH_LITE = "gemini_flash_lite"
GROQ = "groq"
CEREBRAS = "cerebras"
MISTRAL = "mistral"
OPENROUTER = "openrouter"
XAI = "xai"
SAMBANOVA = "sambanova"
NVIDIA_NIM = "nvidia_nim"
CLOUDFLARE = "cloudflare"
GPT4O = "gpt4o"
ZHIPU = "zhipu"
SILICONFLOW = "siliconflow"
HUGGINGFACE = "huggingface"
OPENROUTER_QWEN = "openrouter_qwen"
OPENROUTER_ARCEE = "openrouter_arcee"
OPENROUTER_LIQUID = "openrouter_liquid"
TOGETHER = "together"
MOONSHOT = "moonshot"
OPENROUTER_YI = "openrouter_yi"
OPENROUTER_DEEPSEEK_R1 = "openrouter_deepseek_r1"
OPENROUTER_GEMMA = "openrouter_gemma"
XIAOMI_MIMO = "xiaomi_mimo"
CLAUDE_HAIKU = "claude_haiku"
CLAUDE = "claude"
CLAUDE_OPUS = "claude_opus"

# Global timeout budget: max total wall-clock seconds for the entire call_llm()
# fallback loop.  Individual per-provider timeouts are dynamically capped to the
# remaining budget so the caller never waits longer than this.
GLOBAL_TIMEOUT_BUDGET = 35.0  # seconds -- allows retry logic to complete
_MIN_REMAINING_BUDGET = 5.0  # don't start a new attempt with < 5s left


# ═══════════════════════════════════════════════════════════════════════════════
# RATE-AWARE ROUTING CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Per-provider rate limits for the sliding window tracker.
# These are the *known* free-tier RPM limits.  Providers not listed here
# fall back to the rpm_limit in PROVIDER_CONFIG.
_RATE_LIMITS: dict[str, dict[str, int]] = {
    "groq": {"rpm": 30, "window": 60},
    "cerebras": {"rpm": 30, "window": 60},
    "gemini": {"rpm": 30, "window": 60},
    "gemini_flash_lite": {"rpm": 30, "window": 60},
    "together": {"rpm": 60, "window": 60},
    "huggingface": {"rpm": 10, "window": 60},
    "mistral": {"rpm": 30, "window": 60},
    "sambanova": {"rpm": 20, "window": 60},
    "siliconflow": {"rpm": 30, "window": 60},
    "nvidia_nim": {"rpm": 30, "window": 60},
    "zhipu": {"rpm": 30, "window": 60},
    # OpenRouter variants: each has its own tracking bucket, but a combined
    # 20 RPM cap (_openrouter_combined) enforces the shared API key limit.
    # Individual per-variant limits are set generously; the combined cap is
    # the real constraint.
    "_openrouter_combined": {"rpm": 20, "window": 60},
    "openrouter": {"rpm": 20, "window": 60},
    "openrouter_qwen": {"rpm": 20, "window": 60},
    "openrouter_arcee": {"rpm": 20, "window": 60},
    "openrouter_liquid": {"rpm": 20, "window": 60},
    "openrouter_yi": {"rpm": 20, "window": 60},
    "openrouter_deepseek_r1": {"rpm": 20, "window": 60},
    "openrouter_gemma": {"rpm": 20, "window": 60},
    "xiaomi_mimo": {"rpm": 30, "window": 60},
    "moonshot": {"rpm": 15, "window": 60},
    "cloudflare": {"rpm": 300, "window": 60},
    # Paid tiers -- higher limits
    "claude_haiku": {"rpm": 50, "window": 60},
    "claude": {"rpm": 50, "window": 60},
    "claude_opus": {"rpm": 40, "window": 60},
    "gpt4o": {"rpm": 60, "window": 60},
    "xai": {"rpm": 60, "window": 60},
}


class _RateTracker:
    """Thread-safe sliding window rate tracker for all providers.

    Tracks request timestamps per provider in a sliding window and
    provides O(1)-amortized rate-limit checks.  Rate-limited providers
    are skipped WITHOUT burning a circuit breaker failure.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # provider_id -> list of timestamps (ascending order)
        self._windows: dict[str, list[float]] = {}

    def record_request(self, provider_id: str) -> None:
        """Record that a request was sent to a provider.

        For OpenRouter variants, also records to the shared
        '_openrouter_combined' bucket so the 20 RPM API key limit
        is enforced across all variants.
        """
        now = time.time()
        with self._lock:
            if provider_id not in self._windows:
                self._windows[provider_id] = []
            self._windows[provider_id].append(now)
            # Track combined OpenRouter usage across all variants
            if provider_id.startswith("openrouter"):
                combined = "_openrouter_combined"
                if combined not in self._windows:
                    self._windows[combined] = []
                self._windows[combined].append(now)

    def is_rate_limited(self, provider_id: str) -> bool:
        """Check if a provider has exceeded its RPM in the current window.

        Returns True if the provider should be skipped (rate limited).
        """
        limits = _RATE_LIMITS.get(provider_id)
        if not limits:
            return False  # No known limit -- allow through

        rpm = limits["rpm"]
        window = limits["window"]
        now = time.time()
        cutoff = now - window

        with self._lock:
            timestamps = self._windows.get(provider_id)
            if not timestamps:
                return False

            # Prune expired entries (older than window)
            while timestamps and timestamps[0] < cutoff:
                timestamps.pop(0)

            return len(timestamps) >= rpm

    def get_counts(self) -> dict[str, int]:
        """Return current request counts per provider (for diagnostics)."""
        now = time.time()
        result: dict[str, int] = {}
        with self._lock:
            for pid, timestamps in self._windows.items():
                limits = _RATE_LIMITS.get(pid, {"window": 60})
                cutoff = now - limits["window"]
                # Count without modifying (read-only for stats)
                count = sum(1 for t in timestamps if t >= cutoff)
                result[pid] = count
        return result


# Module-level rate tracker instance
_rate_tracker = _RateTracker()


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE CACHE (Semantic Dedup)
# ═══════════════════════════════════════════════════════════════════════════════

_CACHE_MAX_SIZE = 200
_CACHE_TTL_SECONDS = 900.0  # 15 minutes (default for general queries)
_CACHE_TTL_REALTIME_SECONDS = 300.0  # 5 minutes (real-time / volatile data queries)
# Legacy alias kept for backward compatibility with get_stats()
_CACHE_TTL_EXTENDED_SECONDS = _CACHE_TTL_SECONDS

# Task types that get REDUCED cache TTL (real-time data, changes frequently)
_REALTIME_TTL_TASK_TYPES: set[str] = {
    "market_analysis",
    "competitor_scan",
    "structured",  # CPC/CPA lookups, benchmark data
    "research",  # market research with live data
}

# Legacy alias -- kept for backward compatibility
_EXTENDED_TTL_TASK_TYPES: set[str] = {
    "verification",
    "compliance_check",
}


class _ResponseCache:
    """Thread-safe LRU response cache with task-type-aware TTL for semantic dedup.

    Cache key is derived from a normalized hash of (task_type, system_prompt
    prefix, user_message prefix).  Only successful responses are cached.

    Verification and compliance tasks get a 15-minute TTL (their results
    are stable and expensive to recompute).  All other tasks get 5 minutes.
    """

    def __init__(
        self, max_size: int = _CACHE_MAX_SIZE, ttl: float = _CACHE_TTL_SECONDS
    ) -> None:
        self._lock = threading.Lock()
        self._max_size = max_size
        self._ttl = ttl
        # key -> (timestamp, ttl, response_dict)
        self._store: collections.OrderedDict[
            str, tuple[float, float, dict[str, Any]]
        ] = collections.OrderedDict()
        # Stats
        self._hits = 0
        self._misses = 0

    @staticmethod
    def _ttl_for_task(task_type: str) -> float:
        """Return the cache TTL for a given task type.

        Real-time data tasks (market analysis, competitor scans, benchmarks)
        get a shorter 5-minute TTL.  All other tasks get 15 minutes.
        """
        if task_type in _REALTIME_TTL_TASK_TYPES:
            return _CACHE_TTL_REALTIME_SECONDS
        return _CACHE_TTL_SECONDS

    @staticmethod
    def _make_key(task_type: str, system_prompt: str, user_message: str) -> str:
        """Build a normalized cache key from prompt components."""
        normalized = (
            f"{task_type.strip().lower()}|"
            f"{system_prompt[:200].strip().lower()}|"
            f"{user_message[:500].strip().lower()}"
        )
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def get(
        self, task_type: str, system_prompt: str, user_message: str
    ) -> Optional[dict[str, Any]]:
        """Look up a cached response.  L1 (in-memory) first, then L3 (Upstash Redis)."""
        key = self._make_key(task_type, system_prompt, user_message)
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if entry is not None:
                ts, entry_ttl, response = entry
                if now - ts <= entry_ttl:
                    self._store.move_to_end(key)
                    self._hits += 1
                    return response
                # Expired -- evict from L1
                del self._store[key]

        # L1 miss -- try L3 (Upstash Redis) read-through
        if _UPSTASH_ENABLED:
            try:
                l3_data = _upstash_get(f"llm:{key}")
                if l3_data and isinstance(l3_data, dict) and l3_data.get("text"):
                    # Promote to L1
                    entry_ttl = self._ttl_for_task(task_type)
                    with self._lock:
                        while len(self._store) >= self._max_size:
                            self._store.popitem(last=False)
                        self._store[key] = (now, entry_ttl, l3_data)
                        self._hits += 1
                    logger.debug("LLM cache L3 HIT (Upstash) for key=%s...", key[:12])
                    return l3_data
            except Exception:
                pass  # L3 failure is non-fatal

        with self._lock:
            self._misses += 1
        return None

    def put(
        self,
        task_type: str,
        system_prompt: str,
        user_message: str,
        response: dict[str, Any],
    ) -> None:
        """Store a successful response in L1 (in-memory) and L3 (Upstash Redis).

        Uses task-type-aware TTL: real-time tasks get 5 min, others get 15 min.
        L3 gets 2x the TTL for persistence across deploys.
        """
        key = self._make_key(task_type, system_prompt, user_message)
        entry_ttl = self._ttl_for_task(task_type)
        now = time.time()
        with self._lock:
            # If key exists, update it
            if key in self._store:
                self._store.move_to_end(key)
                self._store[key] = (now, entry_ttl, response)
            else:
                # Evict oldest if at capacity
                while len(self._store) >= self._max_size:
                    self._store.popitem(last=False)
                self._store[key] = (now, entry_ttl, response)

        # Write-through to L3 (Upstash Redis) -- fire-and-forget in background
        if _UPSTASH_ENABLED:
            l3_ttl = int(entry_ttl * 2)  # 2x TTL for persistence across deploys
            threading.Thread(
                target=self._l3_write,
                args=(key, response, l3_ttl),
                daemon=True,
                name="upstash-cache-write",
            ).start()

    @staticmethod
    def _l3_write(key: str, response: dict[str, Any], ttl: int) -> None:
        """Write to Upstash Redis L3 cache (background thread)."""
        try:
            _upstash_set(f"llm:{key}", response, ttl_seconds=ttl, category="llm")
        except Exception as exc:
            logger.debug("L3 cache write failed: %s", exc)

    def get_stats(self) -> dict[str, Any]:
        """Return cache statistics including hit/miss rates and TTL config."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100.0) if total > 0 else 0.0
            return {
                "cache_size": len(self._store),
                "cache_max_size": self._max_size,
                "cache_ttl_default_seconds": _CACHE_TTL_SECONDS,
                "cache_ttl_realtime_seconds": _CACHE_TTL_REALTIME_SECONDS,
                "cache_realtime_ttl_tasks": sorted(_REALTIME_TTL_TASK_TYPES),
                # Legacy fields for backward compat
                "cache_ttl_seconds": self._ttl,
                "cache_ttl_extended_seconds": _CACHE_TTL_EXTENDED_SECONDS,
                "cache_extended_ttl_tasks": sorted(_EXTENDED_TTL_TASK_TYPES),
                "cache_hits": self._hits,
                "cache_misses": self._misses,
                "cache_hit_rate_pct": round(hit_rate, 1),
                "l3_upstash_enabled": _UPSTASH_ENABLED,
            }


# Module-level response cache instance
_response_cache = _ResponseCache()


# Provider configs: endpoint, model, auth header, rate limits
PROVIDER_CONFIG: Dict[str, Dict[str, Any]] = {
    GEMINI: {
        "name": "Gemini 2.5 Flash",
        "api_style": "gemini",  # Google-specific format
        "endpoint": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
        "model": "gemini-2.5-flash",
        "env_key": "GEMINI_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 1500,
        "timeout": 30,
        "max_tokens": 8192,
    },
    GEMINI_FLASH_LITE: {
        "name": "Gemini 2.5 Flash Lite",
        "api_style": "gemini",
        "endpoint": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent",
        "model": "gemini-2.5-flash-lite",
        "env_key": "GEMINI_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 1500,
        "timeout": 20,
        "max_tokens": 8192,
    },
    GROQ: {
        "name": "Groq Llama 3.3 70B",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.groq.com/openai/v1/chat/completions",
        "model": "llama-3.3-70b-versatile",
        "env_key": "GROQ_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 14400,
        "timeout": 30,
        "max_tokens": 8192,
    },
    CEREBRAS: {
        "name": "Cerebras Llama 3.3 70B",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.cerebras.ai/v1/chat/completions",
        "model": "llama-3.3-70b",
        "env_key": "CEREBRAS_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 14400,  # 1M tokens/day free
        "timeout": 30,
        "max_tokens": 8192,
    },
    MISTRAL: {
        "name": "Mistral Small",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.mistral.ai/v1/chat/completions",
        "model": "mistral-small-latest",
        "env_key": "MISTRAL_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 14400,
        "timeout": 30,
        "max_tokens": 8192,
    },
    OPENROUTER: {
        "name": "OpenRouter (Llama 4 Maverick)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "meta-llama/llama-4-maverick:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,  # Conservative -- free tier has daily limits
        "timeout": 30,
        "max_tokens": 4096,
        "extra_headers": {  # OpenRouter requires/recommends these
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    XAI: {
        "name": "xAI Grok",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.x.ai/v1/chat/completions",
        "model": "grok-3-mini-fast",
        "env_key": "XAI_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 14400,
        "timeout": 30,
        "max_tokens": 8192,
    },
    SAMBANOVA: {
        "name": "SambaNova (Llama 3.3 70B)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.sambanova.ai/v1/chat/completions",
        "model": "Meta-Llama-3.3-70B-Instruct",
        "env_key": "SAMBANOVA_API_KEY",
        "rpm_limit": 10,  # Free tier RPM limit
        "rpd_limit": 1000,
        "timeout": 25,  # Capped to fit within 30s global budget
        "max_tokens": 4096,
    },
    NVIDIA_NIM: {
        "name": "NVIDIA NIM (Nemotron Nano 30B)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://integrate.api.nvidia.com/v1/chat/completions",
        "model": "nvidia/nemotron-3-nano-30b-a3b",
        "env_key": "NVIDIA_NIM_API_KEY",
        "rpm_limit": 40,
        "rpd_limit": 5000,
        "timeout": 30,
        "max_tokens": 4096,
    },
    CLOUDFLARE: {
        "name": "Cloudflare Workers AI (Llama 3.3 70B)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1/chat/completions",
        "model": "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
        "env_key": "CLOUDFLARE_AI_TOKEN",
        "env_key_account": "CLOUDFLARE_ACCOUNT_ID",  # Extra env var for account ID in URL
        "rpm_limit": 300,  # Cloudflare has high RPM but neuron-based daily limit
        "rpd_limit": 500,  # Conservative -- 10K neurons/day ~ 100-500 requests
        "timeout": 30,
        "max_tokens": 4096,
    },
    ZHIPU: {
        "name": "Zhipu AI (GLM-4-Flash)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://open.bigmodel.cn/api/paas/v4/chat/completions",
        "model": "glm-4-flash",
        "env_key": "ZHIPU_API_KEY",
        "rpm_limit": 60,
        "rpd_limit": 50000,  # Unlimited free tier
        "timeout": 30,
        "max_tokens": 4096,
    },
    SILICONFLOW: {
        "name": "SiliconFlow (Qwen2.5 7B)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.siliconflow.cn/v1/chat/completions",
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "env_key": "SILICONFLOW_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 10000,  # Free tier: $0.05/M tokens
        "timeout": 30,
        "max_tokens": 4096,
    },
    HUGGINGFACE: {
        "name": "HuggingFace Inference (Mistral 7B)",
        "api_style": "huggingface",  # HuggingFace-specific format
        "endpoint": "https://api-inference.huggingface.co/models/mistralai/Mistral-7B-Instruct-v0.3",
        "model": "mistralai/Mistral-7B-Instruct-v0.3",
        "env_key": "HUGGINGFACE_API_KEY",
        "rpm_limit": 10,  # Rate-limited free tier
        "rpd_limit": 1000,
        "timeout": 25,  # Capped to fit within 30s global budget
        "max_tokens": 1024,
    },
    XIAOMI_MIMO: {
        "name": "Xiaomi MiMo V2 Flash",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.xiaomimimo.com/v1/chat/completions",
        "model": "mimo-v2-flash",
        "env_key": "XIAOMI_MIMO_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 1500,
        "timeout": 30,
        "max_tokens": 8192,
    },
    OPENROUTER_QWEN: {
        "name": "OpenRouter (Qwen3 Coder)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "alibaba/qwen3-coder:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,
        "timeout": 30,
        "max_tokens": 4096,
        "extra_headers": {
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    OPENROUTER_ARCEE: {
        "name": "OpenRouter (Arcee Trinity)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "arcee-ai/arcee-trinity-v1:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,
        "timeout": 30,
        "max_tokens": 4096,
        "extra_headers": {
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    OPENROUTER_LIQUID: {
        "name": "OpenRouter (Liquid LFM 2.5)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "liquid/lfm-2.5:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,
        "timeout": 30,
        "max_tokens": 4096,
        "extra_headers": {
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    TOGETHER: {
        "name": "Together AI (Llama 3.3 70B Turbo)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.together.xyz/v1/chat/completions",
        "model": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
        "env_key": "TOGETHER_API_KEY",
        "rpm_limit": 60,
        "rpd_limit": 10000,  # $25 free credit on signup
        "timeout": 30,
        "max_tokens": 4096,
    },
    MOONSHOT: {
        "name": "Moonshot Kimi (moonshot-v1-8k)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.moonshot.cn/v1/chat/completions",
        "model": "moonshot-v1-8k",
        "env_key": "MOONSHOT_API_KEY",
        "rpm_limit": 15,
        "rpd_limit": 1000,  # Limited free tier
        "timeout": 30,
        "max_tokens": 4096,
    },
    OPENROUTER_YI: {
        "name": "OpenRouter (01.AI Yi Large)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "01-ai/yi-large:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,
        "timeout": 30,
        "max_tokens": 4096,
        "extra_headers": {
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    OPENROUTER_DEEPSEEK_R1: {
        "name": "OpenRouter (DeepSeek R1 Reasoning)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "deepseek/deepseek-r1:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,
        "timeout": 25,  # Capped to fit within 30s global budget
        "max_tokens": 4096,
        "extra_headers": {
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    OPENROUTER_GEMMA: {
        "name": "OpenRouter (Google Gemma 3 27B)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://openrouter.ai/api/v1/chat/completions",
        "model": "google/gemma-3-27b-it:free",
        "env_key": "OPENROUTER_API_KEY",
        "rpm_limit": 20,
        "rpd_limit": 1000,
        "timeout": 30,
        "max_tokens": 4096,
        "extra_headers": {
            "HTTP-Referer": "https://media-plan-generator.onrender.com",
            "X-Title": "Nova AI Suite",
        },
    },
    GPT4O: {
        "name": "GPT-4o (OpenAI)",
        "api_style": "openai",  # OpenAI native format
        "endpoint": "https://api.openai.com/v1/chat/completions",
        "model": "gpt-4o",
        "env_key": "OPENAI_API_KEY",
        "rpm_limit": 60,
        "rpd_limit": 10000,
        "timeout": 25,  # Capped to fit within 30s global budget
        "max_tokens": 4096,
    },
    CLAUDE_HAIKU: {
        "name": "Claude Haiku 4.5 (Anthropic)",
        "api_style": "anthropic",  # Anthropic-specific format
        "endpoint": "https://api.anthropic.com/v1/messages",
        "model": "claude-haiku-4-5-20251001",
        "env_key": "ANTHROPIC_API_KEY",
        "rpm_limit": 100,  # Haiku is fast + cheap, generous limits
        "rpd_limit": 20000,
        "timeout": 25,  # Haiku is very fast
        "max_tokens": 4096,
    },
    CLAUDE: {
        "name": "Claude Sonnet 4 (Anthropic)",
        "api_style": "anthropic",  # Anthropic-specific format
        "endpoint": "https://api.anthropic.com/v1/messages",
        "model": "claude-sonnet-4-20250514",
        "env_key": "ANTHROPIC_API_KEY",
        "rpm_limit": 50,
        "rpd_limit": 10000,
        "timeout": 25,  # Capped to fit within 30s global budget
        "max_tokens": 4096,
    },
    CLAUDE_OPUS: {
        "name": "Claude Opus 4.6 (Anthropic)",
        "api_style": "anthropic",  # Anthropic-specific format
        "endpoint": "https://api.anthropic.com/v1/messages",
        "model": "claude-opus-4-20250514",
        "env_key": "ANTHROPIC_API_KEY",  # Same API key, different model
        "rpm_limit": 25,  # Conservative -- most expensive model
        "rpd_limit": 2000,
        "timeout": 25,  # Capped to fit within 30s global budget (was 90s)
        "max_tokens": 4096,
    },
}

# Task -> provider priority order
# Strategy: 20 free providers first, then paid by cost-efficiency, Opus absolute last
#
# Free tier strengths:
#   Gemini: structured JSON, code, verification
#   Groq/Cerebras (Llama 3.3 70B): conversational, complex reasoning
#   Zhipu AI (GLM-4-Flash): unlimited free, strong multilingual
#   Mistral Small: structured JSON, multilingual, code
#   NVIDIA NIM: NVIDIA-optimized inference, diverse model catalog
#   SambaNova (Llama 3.1 405B): largest open model, fastest inference (RDU hardware)
#   SiliconFlow (Qwen2.5 7B): OpenAI-compatible, cheap tokens
#   Cloudflare Workers AI (Llama 3.3 70B): edge-distributed, low latency, 10K neurons/day
#   Together AI (Llama 3.3 70B Turbo): $25 free credit, fast inference, general purpose
#   Moonshot Kimi: DISABLED (no API key) -- strong for Asian/Chinese market queries
#   OpenRouter (Llama 4 Maverick): strong general purpose
#   OpenRouter (Qwen3 Coder): code generation specialist
#   OpenRouter (Arcee Trinity): complex reasoning
#   OpenRouter (Liquid LFM 2.5): novel architecture
#   OpenRouter (01.AI Yi Large): good general purpose via OpenRouter free tier
#   OpenRouter (DeepSeek R1): strong reasoning/research, HIGH priority for COMPLEX/RESEARCH
#   OpenRouter (Google Gemma 3 27B): structured output, verification
#   xAI Grok: strong reasoning (credits-based: $25 signup, $2/$10 per M tokens in/out)
#   HuggingFace (Mistral 7B): rate-limited fallback
#
# Paid tier strengths (cost order: Haiku << GPT-4o < Sonnet < Opus):
#   Claude Haiku: fast + cheap paid fallback, good for simple tasks
#   GPT-4o: structured JSON, general reasoning, calculations
#   Claude Sonnet: complex multi-step tool_use chains
#   Claude Opus 4.6: last resort, highest quality
TASK_ROUTING: Dict[str, List[str]] = {
    TASK_STRUCTURED: [
        GEMINI,
        GEMINI_FLASH_LITE,  # Lighter Gemini variant for simple structured queries
        MISTRAL,
        OPENROUTER_GEMMA,  # Gemma 3 27B -- strong structured output
        GROQ,
        ZHIPU,
        OPENROUTER_QWEN,  # Spaced: non-OR providers between OR variants
        CEREBRAS,
        NVIDIA_NIM,
        OPENROUTER,
        SAMBANOVA,
        SILICONFLOW,
        OPENROUTER_YI,
        TOGETHER,
        CLOUDFLARE,
        OPENROUTER_ARCEE,
        XAI,
        OPENROUTER_DEEPSEEK_R1,
        HUGGINGFACE,
        OPENROUTER_LIQUID,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_CONVERSATIONAL: [
        XIAOMI_MIMO,  # MiMo V2 Flash -- 309B MoE, best free-tier quality
        GROQ,
        ZHIPU,
        CEREBRAS,
        GEMINI,
        MISTRAL,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        NVIDIA_NIM,
        SAMBANOVA,
        OPENROUTER_YI,
        SILICONFLOW,
        TOGETHER,
        OPENROUTER_ARCEE,
        CLOUDFLARE,
        XAI,
        OPENROUTER_GEMMA,
        HUGGINGFACE,
        OPENROUTER_LIQUID,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_COMPLEX: [
        XIAOMI_MIMO,  # MiMo V2 Flash -- 309B MoE, best free-tier quality
        OPENROUTER_DEEPSEEK_R1,  # DeepSeek R1 -- strong reasoning
        SAMBANOVA,
        GROQ,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        ZHIPU,
        CEREBRAS,
        OPENROUTER_ARCEE,
        GEMINI,
        MISTRAL,
        OPENROUTER_YI,
        TOGETHER,
        NVIDIA_NIM,
        OPENROUTER_GEMMA,
        SILICONFLOW,
        CLOUDFLARE,
        OPENROUTER_LIQUID,
        XAI,
        HUGGINGFACE,
        CLAUDE_HAIKU,
        CLAUDE,
        GPT4O,
        CLAUDE_OPUS,
    ],
    TASK_CODE: [
        GEMINI,
        OPENROUTER_QWEN,  # Qwen3 Coder -- code specialist, top priority OR variant
        MISTRAL,
        GROQ,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        ZHIPU,
        CEREBRAS,
        OPENROUTER_DEEPSEEK_R1,
        TOGETHER,
        NVIDIA_NIM,
        OPENROUTER_YI,
        SAMBANOVA,
        SILICONFLOW,
        OPENROUTER_GEMMA,
        CLOUDFLARE,
        XAI,
        HUGGINGFACE,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_VERIFICATION: [
        GEMINI,
        MISTRAL,
        OPENROUTER_GEMMA,  # Gemma 3 -- good for verification tasks
        GROQ,
        ZHIPU,
        OPENROUTER_DEEPSEEK_R1,  # Spaced: non-OR providers between OR variants
        CEREBRAS,
        NVIDIA_NIM,
        OPENROUTER,
        TOGETHER,
        SAMBANOVA,
        OPENROUTER_YI,
        SILICONFLOW,
        CLOUDFLARE,
        OPENROUTER_ARCEE,
        XAI,
        HUGGINGFACE,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_RESEARCH: [
        OPENROUTER_DEEPSEEK_R1,  # DeepSeek R1 -- strong reasoning, HIGH priority
        XAI,
        SAMBANOVA,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        GEMINI,
        GROQ,
        OPENROUTER_ARCEE,
        ZHIPU,
        TOGETHER,
        OPENROUTER_YI,
        CEREBRAS,
        MISTRAL,
        OPENROUTER_GEMMA,
        NVIDIA_NIM,
        SILICONFLOW,
        OPENROUTER_LIQUID,
        CLOUDFLARE,
        HUGGINGFACE,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_NARRATIVE: [
        GROQ,
        GEMINI,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        ZHIPU,
        CEREBRAS,
        OPENROUTER_YI,
        MISTRAL,
        TOGETHER,
        OPENROUTER_GEMMA,
        SAMBANOVA,
        NVIDIA_NIM,
        OPENROUTER_DEEPSEEK_R1,
        SILICONFLOW,
        CLOUDFLARE,
        OPENROUTER_LIQUID,
        XAI,
        HUGGINGFACE,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_BATCH: [
        CLOUDFLARE,
        CEREBRAS,
        GROQ,
        ZHIPU,
        GEMINI,
        GEMINI_FLASH_LITE,
        MISTRAL,
        TOGETHER,
        NVIDIA_NIM,
        SAMBANOVA,
        SILICONFLOW,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        XAI,
        OPENROUTER_QWEN,
        HUGGINGFACE,
        OPENROUTER_YI,
        CLAUDE_HAIKU,
        OPENROUTER_ARCEE,
        GPT4O,
        OPENROUTER_DEEPSEEK_R1,
        CLAUDE,
        OPENROUTER_GEMMA,
        CLAUDE_OPUS,
        OPENROUTER_LIQUID,
    ],
    # ── v4.0 Platform Module Task Types ──────────────────────────────────
    # Command Center: fast for quick plans, Claude for full plans
    TASK_CAMPAIGN_PLAN: [
        XIAOMI_MIMO,  # MiMo V2 Flash -- 309B MoE, best plan generation quality
        GROQ,
        CEREBRAS,
        GEMINI,
        ZHIPU,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        MISTRAL,
        SAMBANOVA,
        OPENROUTER_DEEPSEEK_R1,
        NVIDIA_NIM,
        TOGETHER,
        XAI,
        SILICONFLOW,
        CLOUDFLARE,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_BUDGET_OPTIMIZE: [
        GEMINI,
        MISTRAL,
        OPENROUTER_GEMMA,
        GROQ,
        CEREBRAS,
        ZHIPU,
        NVIDIA_NIM,
        SAMBANOVA,
        TOGETHER,
        SILICONFLOW,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_COMPLIANCE_CHECK: [
        GEMINI,
        OPENROUTER_GEMMA,
        MISTRAL,
        GROQ,
        ZHIPU,
        CEREBRAS,
        OPENROUTER_DEEPSEEK_R1,
        SAMBANOVA,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    # Intelligence Hub: prefer structured data / analysis providers
    TASK_MARKET_ANALYSIS: [
        XIAOMI_MIMO,  # MiMo V2 Flash -- 309B MoE, best analysis quality
        GEMINI,
        OPENROUTER_DEEPSEEK_R1,
        XAI,
        SAMBANOVA,
        GROQ,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        ZHIPU,
        MISTRAL,
        OPENROUTER_ARCEE,
        TOGETHER,
        CEREBRAS,
        NVIDIA_NIM,
        SILICONFLOW,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_COMPETITOR_SCAN: [
        OPENROUTER_DEEPSEEK_R1,
        XAI,
        GEMINI,
        OPENROUTER,
        GROQ,
        ZHIPU,
        SAMBANOVA,
        MISTRAL,
        TOGETHER,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_TALENT_MAP: [
        GEMINI,
        GROQ,
        CEREBRAS,
        ZHIPU,
        MISTRAL,
        SAMBANOVA,
        NVIDIA_NIM,
        TOGETHER,
        SILICONFLOW,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    # Nova AI chat: lowest latency first, streaming-capable
    TASK_CHAT_RESPONSE: [
        XIAOMI_MIMO,  # MiMo V2 Flash -- 309B MoE, best chat quality
        GROQ,
        CEREBRAS,
        GEMINI,
        ZHIPU,
        MISTRAL,
        OPENROUTER,  # Spaced: non-OR providers between OR variants
        NVIDIA_NIM,
        SAMBANOVA,
        OPENROUTER_YI,
        SILICONFLOW,
        TOGETHER,
        CLOUDFLARE,
        XAI,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_ACTION_EXECUTE: [
        GEMINI,
        GROQ,
        CEREBRAS,
        MISTRAL,
        ZHIPU,
        NVIDIA_NIM,
        SAMBANOVA,
        TOGETHER,
        OPENROUTER_QWEN,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
        CLAUDE_OPUS,
    ],
    TASK_CONTEXT_SUMMARIZE: [
        GROQ,
        CEREBRAS,
        GEMINI,
        GEMINI_FLASH_LITE,
        ZHIPU,
        MISTRAL,
        NVIDIA_NIM,
        TOGETHER,
        SAMBANOVA,
        SILICONFLOW,
        CLOUDFLARE,
        OPENROUTER,
        CLAUDE_HAIKU,
        GPT4O,
        CLAUDE,
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE-SPECIFIC LLM ROUTING PREFERENCES
# ═══════════════════════════════════════════════════════════════════════════════

# Maps platform module -> default task type + preferred provider overrides
MODULE_LLM_PREFERENCES: Dict[str, Dict[str, Any]] = {
    "command_center": {
        "default_task": TASK_CAMPAIGN_PLAN,
        "quick_task": TASK_CHAT_RESPONSE,
        "preferred_providers": [GROQ, CEREBRAS, GEMINI],
        # NOTE: full_plan_providers removed (unused by callers). Use
        # preferred_providers override with task_type=TASK_CAMPAIGN_PLAN instead.
        "description": "Fast providers for quick plans, Claude for full plans",
    },
    "intelligence_hub": {
        "default_task": TASK_MARKET_ANALYSIS,
        "preferred_providers": [GEMINI, OPENROUTER_DEEPSEEK_R1, XAI],
        "description": "Providers good at structured data and analysis",
    },
    "nova_ai": {
        "default_task": TASK_CHAT_RESPONSE,
        "preferred_providers": [GROQ, CEREBRAS, GEMINI],
        "description": "Streaming-capable, lowest latency first",
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# COST TRACKING (estimated token costs per 1M tokens)
# ═══════════════════════════════════════════════════════════════════════════════

# Estimated cost per 1M tokens (USD) -- input/output
_PROVIDER_COST_PER_M_TOKENS: Dict[str, Dict[str, float]] = {
    GEMINI: {"input": 0.0, "output": 0.0},
    GEMINI_FLASH_LITE: {"input": 0.0, "output": 0.0},
    GROQ: {"input": 0.0, "output": 0.0},
    CEREBRAS: {"input": 0.0, "output": 0.0},
    ZHIPU: {"input": 0.0, "output": 0.0},
    MISTRAL: {"input": 0.0, "output": 0.0},
    NVIDIA_NIM: {"input": 0.0, "output": 0.0},
    SAMBANOVA: {"input": 0.0, "output": 0.0},
    SILICONFLOW: {"input": 0.05, "output": 0.05},
    CLOUDFLARE: {"input": 0.0, "output": 0.0},
    TOGETHER: {"input": 0.0, "output": 0.0},
    MOONSHOT: {"input": 0.0, "output": 0.0},
    XIAOMI_MIMO: {"input": 0.1, "output": 0.3},  # $0.1/M in, $0.3/M out
    HUGGINGFACE: {"input": 0.0, "output": 0.0},
    OPENROUTER: {"input": 0.0, "output": 0.0},
    OPENROUTER_QWEN: {"input": 0.0, "output": 0.0},
    OPENROUTER_ARCEE: {"input": 0.0, "output": 0.0},
    OPENROUTER_LIQUID: {"input": 0.0, "output": 0.0},
    OPENROUTER_YI: {"input": 0.0, "output": 0.0},
    OPENROUTER_DEEPSEEK_R1: {"input": 0.0, "output": 0.0},
    OPENROUTER_GEMMA: {"input": 0.0, "output": 0.0},
    # xAI: credits-based ($25 free signup), NOT truly free -- track actual costs
    XAI: {"input": 2.0, "output": 10.0},
    CLAUDE_HAIKU: {"input": 1.0, "output": 5.0},
    GPT4O: {"input": 2.5, "output": 10.0},
    CLAUDE: {"input": 3.0, "output": 15.0},
    CLAUDE_OPUS: {"input": 15.0, "output": 75.0},
}


class _CostTracker:
    """Thread-safe daily cost tracker for LLM API usage.

    Estimates token costs per provider per request and tracks cumulative
    daily spend to enable budget alerting and provider selection optimization.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._daily_costs: Dict[str, float] = {}  # provider -> USD
        self._daily_tokens: Dict[str, Dict[str, int]] = (
            {}
        )  # provider -> {input, output}
        self._day_start: float = time.time()
        self._total_cost: float = 0.0

    def _maybe_reset_day(self) -> None:
        """Reset daily counters if a new day has started (24h rolling window)."""
        now = time.time()
        if now - self._day_start > 86400:
            self._daily_costs.clear()
            self._daily_tokens.clear()
            self._day_start = now

    def record_usage(
        self,
        provider_id: str,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        """Record token usage for a provider and return estimated cost.

        Args:
            provider_id: LLM provider ID.
            input_tokens: Number of input tokens.
            output_tokens: Number of output tokens.

        Returns:
            Estimated cost in USD for this request.
        """
        costs = _PROVIDER_COST_PER_M_TOKENS.get(
            provider_id, {"input": 0.0, "output": 0.0}
        )
        cost = (
            input_tokens * costs["input"] + output_tokens * costs["output"]
        ) / 1_000_000

        with self._lock:
            self._maybe_reset_day()
            self._daily_costs[provider_id] = (
                self._daily_costs.get(provider_id, 0.0) + cost
            )
            self._total_cost += cost

            if provider_id not in self._daily_tokens:
                self._daily_tokens[provider_id] = {"input": 0, "output": 0, "calls": 0}
            self._daily_tokens[provider_id]["input"] += input_tokens
            self._daily_tokens[provider_id]["output"] += output_tokens
            self._daily_tokens[provider_id]["calls"] = (
                self._daily_tokens[provider_id].get("calls", 0) + 1
            )

        return cost

    def get_daily_spend(self) -> Dict[str, Any]:
        """Return daily spend summary."""
        with self._lock:
            self._maybe_reset_day()
            total_tokens = sum(
                v.get("input", 0) + v.get("output", 0)
                for v in self._daily_tokens.values()
            )
            total_calls = sum(v.get("calls", 0) for v in self._daily_tokens.values())
            return {
                "period_start": self._day_start,
                "total_daily_cost_usd": round(sum(self._daily_costs.values()), 4),
                "total_all_time_cost_usd": round(self._total_cost, 4),
                "total_tokens": total_tokens,
                "total_calls": total_calls,
                "per_provider": {
                    pid: {
                        "cost_usd": round(cost, 4),
                        "tokens": self._daily_tokens.get(
                            pid, {"input": 0, "output": 0, "calls": 0}
                        ),
                    }
                    for pid, cost in self._daily_costs.items()
                },
            }


# Module-level cost tracker instance
_cost_tracker = _CostTracker()


def get_cost_report() -> Dict[str, Any]:
    """Get LLM cost tracking report.

    Returns:
        Dict with daily cost summary, total tokens, and per-provider breakdown.
    """
    return _cost_tracker.get_daily_spend()


# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY SCORING
# ═══════════════════════════════════════════════════════════════════════════════


def compute_quality_score(response_text: str, task_type: str = "") -> Dict[str, Any]:
    """Compute a simple quality heuristic for an LLM response.

    Evaluates response based on length, structure, data presence, and
    formatting. Returns a score from 0.0 to 1.0 with component breakdown.

    Args:
        response_text: The LLM response text.
        task_type: The task type used for routing.

    Returns:
        Dict with overall score and component scores.
    """
    if not response_text:
        return {"score": 0.0, "components": {}, "flags": ["empty_response"]}

    text = response_text.strip()
    flags: List[str] = []
    components: Dict[str, float] = {}

    # Length score (0-1): penalize very short or very long responses
    length = len(text)
    if length < 50:
        components["length"] = 0.2
        flags.append("very_short")
    elif length < 200:
        components["length"] = 0.5
    elif length < 2000:
        components["length"] = 1.0
    elif length < 5000:
        components["length"] = 0.9
    else:
        components["length"] = 0.7
        flags.append("very_long")

    # Structure score: presence of headers, bullets, numbered lists
    has_headers = bool(re.search(r"^#{1,3}\s|\*\*[A-Z]", text, re.MULTILINE))
    has_bullets = bool(re.search(r"^\s*[-*]\s", text, re.MULTILINE))
    has_numbers = bool(re.search(r"^\s*\d+[.)]\s", text, re.MULTILINE))
    structure_signals = sum([has_headers, has_bullets, has_numbers])
    components["structure"] = min(1.0, structure_signals * 0.4 + 0.2)

    # Data presence: numbers, percentages, dollar amounts
    has_pct = bool(re.search(r"\d+\.?\d*%", text))
    has_dollar = bool(re.search(r"\$[\d,]+", text))
    has_numbers_inline = bool(re.search(r"\b\d{2,}\b", text))
    data_signals = sum([has_pct, has_dollar, has_numbers_inline])
    components["contains_data"] = min(1.0, data_signals * 0.35 + 0.1)

    # Coherence: check for common LLM failure patterns
    if "I cannot" in text or "I'm unable" in text or "I don't have" in text:
        components["coherence"] = 0.3
        flags.append("refusal_detected")
    elif text.count("...") > 5:
        components["coherence"] = 0.5
        flags.append("ellipsis_heavy")
    else:
        components["coherence"] = 1.0

    # Task-type bonus: structured tasks should have data
    if task_type in (TASK_STRUCTURED, TASK_BUDGET_OPTIMIZE, TASK_MARKET_ANALYSIS):
        if data_signals == 0:
            components["task_fit"] = 0.3
            flags.append("missing_data_for_structured_task")
        else:
            components["task_fit"] = 1.0
    elif task_type in (TASK_NARRATIVE, TASK_CONTEXT_SUMMARIZE):
        if length > 100:
            components["task_fit"] = 1.0
        else:
            components["task_fit"] = 0.4
    else:
        components["task_fit"] = 0.8

    # Overall: weighted average
    weights = {
        "length": 0.15,
        "structure": 0.20,
        "contains_data": 0.25,
        "coherence": 0.25,
        "task_fit": 0.15,
    }
    overall = sum(components.get(k, 0) * w for k, w in weights.items())

    return {
        "score": round(overall, 3),
        "components": {k: round(v, 3) for k, v in components.items()},
        "flags": flags,
    }


def _score_response_quality(response_text: str, task_type: str = "") -> float:
    """Score response quality 0.0-1.0 based on fast heuristics.

    Lightweight version of compute_quality_score() for inline use during
    provider health updates. Penalizes empty, very short, and refusal responses.

    Args:
        response_text: The LLM response text.
        task_type: The task type (unused, reserved for future weighting).

    Returns:
        Quality score from 0.0 to 1.0.
    """
    if not response_text:
        return 0.0
    score = 1.0
    # Penalize very short responses
    if len(response_text) < 20:
        score *= 0.3
    elif len(response_text) < 50:
        score *= 0.6
    # Penalize error-like responses
    error_signals = [
        "i cannot",
        "i'm sorry",
        "as an ai",
        "i don't have",
        "no data",
        "unable to",
    ]
    lower = response_text.lower()
    for signal in error_signals:
        if signal in lower:
            score *= 0.5
            break
    # Penalize responses that are just the prompt repeated
    if len(response_text) > 100:
        score = min(score, 1.0)
    return round(score, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST PRIORITY LEVELS
# ═══════════════════════════════════════════════════════════════════════════════


class RequestPriority:
    """Priority levels for LLM requests.

    HIGH priority requests skip providers near their rate limits.
    MEDIUM and LOW allow providers at higher utilization.
    """

    HIGH = "high"  # User-facing chat
    MEDIUM = "medium"  # Background enrichment
    LOW = "low"  # Batch jobs


# ═══════════════════════════════════════════════════════════════════════════════
# A/B ROUTING
# ═══════════════════════════════════════════════════════════════════════════════

import random as _random

_ab_routes: Dict[str, Dict[str, Any]] = {}
_ab_lock = threading.Lock()


def set_ab_test(
    name: str, provider_a: str, provider_b: str, split_pct: float = 0.1
) -> None:
    """Route split_pct of traffic to provider_b for comparison.

    Args:
        name: Experiment name.
        provider_a: Primary provider ID.
        provider_b: Test provider ID.
        split_pct: Fraction of traffic (0.0-1.0) routed to provider_b.
    """
    with _ab_lock:
        _ab_routes[name] = {
            "a": provider_a,
            "b": provider_b,
            "split": split_pct,
            "results": {"a": [], "b": []},
        }
    logger.info(
        "LLM A/B test '%s': %s vs %s (%.0f%% to B)",
        name,
        provider_a,
        provider_b,
        split_pct * 100,
    )


def _resolve_ab_provider(name: str) -> Optional[str]:
    """Pick provider for an A/B test by name. Returns None if test not found."""
    with _ab_lock:
        test = _ab_routes.get(name)
        if not test:
            return None
        if _random.random() < test["split"]:
            return test["b"]
        return test["a"]


def record_ab_result(
    name: str, variant: str, quality_score: float, latency_ms: int
) -> None:
    """Record an A/B test result for later analysis.

    Args:
        name: Experiment name.
        variant: 'a' or 'b'.
        quality_score: Quality score of the response.
        latency_ms: Latency in milliseconds.
    """
    with _ab_lock:
        test = _ab_routes.get(name)
        if test and variant in test["results"]:
            test["results"][variant].append(
                {
                    "quality": quality_score,
                    "latency_ms": latency_ms,
                    "ts": time.time(),
                }
            )
            # Keep last 100 results per variant
            if len(test["results"][variant]) > 100:
                test["results"][variant] = test["results"][variant][-100:]


def get_ab_results(name: str) -> Dict[str, Any]:
    """Get A/B test results with summary statistics.

    Args:
        name: Experiment name.

    Returns:
        Dict with per-variant averages and raw results, or empty dict.
    """
    with _ab_lock:
        test = _ab_routes.get(name)
        if not test:
            return {}
        summary: Dict[str, Any] = {
            "provider_a": test["a"],
            "provider_b": test["b"],
            "split_pct": test["split"],
        }
        for variant in ("a", "b"):
            results = test["results"][variant]
            if results:
                avg_q = sum(r["quality"] for r in results) / len(results)
                avg_l = sum(r["latency_ms"] for r in results) / len(results)
                summary[f"variant_{variant}"] = {
                    "count": len(results),
                    "avg_quality": round(avg_q, 3),
                    "avg_latency_ms": round(avg_l, 1),
                }
            else:
                summary[f"variant_{variant}"] = {"count": 0}
        return summary


def list_ab_tests() -> Dict[str, Dict[str, Any]]:
    """List all active A/B tests with their summary stats.

    Returns:
        Dict mapping test name to summary.
    """
    with _ab_lock:
        return {name: get_ab_results(name) for name in _ab_routes}


# Keywords for task classification
_STRUCTURED_KEYWORDS = re.compile(
    r"\b(benchmark|cpc|cpa|cpm|ctr|salary|cost|rate|percentage|"
    r"data|statistics|metrics|numbers|compare|table|json|list)\b",
    re.IGNORECASE,
)
_COMPLEX_KEYWORDS = re.compile(
    r"\b(what.if|scenario|simulate|decompos|breakdown|break.down|"
    r"analysis|optimize|recommend|strategy|plan|allocat|project)\b",
    re.IGNORECASE,
)
_CODE_KEYWORDS = re.compile(
    r"\b(formula|calculate|compute|function|code|equation|"
    r"algorithm|logic|derive|transform)\b",
    re.IGNORECASE,
)
_VERIFICATION_KEYWORDS = re.compile(
    r"\b(verify|check|validate|confirm|accurate|correct|true|false|"
    r"fact.check|grounding|cross.check)\b",
    re.IGNORECASE,
)
_RESEARCH_KEYWORDS = re.compile(
    r"\b(research|investigate|geopolitical|market.analysis|risk.assessment|"
    r"macro|economic.outlook|political|immigration|policy|"
    r"war|recession|inflation|tariff|sanctions|economy|regulation|"
    r"uptick|downtick|disruption|impact|causation|correlation)\b",
    re.IGNORECASE,
)
_NARRATIVE_KEYWORDS = re.compile(
    r"\b(write|draft|compose|narrative|summary|executive.summary|"
    r"report|paragraph|describe|explain.in.detail|overview)\b",
    re.IGNORECASE,
)
_BATCH_KEYWORDS = re.compile(
    r"\b(batch|bulk|multiple|all.industries|all.locations|"
    r"comprehensive|full.report)\b",
    re.IGNORECASE,
)

# v4.0 Platform module task keywords
_CAMPAIGN_PLAN_KEYWORDS = re.compile(
    r"\b(campaign|media.plan|channel.mix|recruitment.plan|"
    r"launch.plan|hiring.plan|staffing.plan)\b",
    re.IGNORECASE,
)
_BUDGET_OPTIMIZE_KEYWORDS = re.compile(
    r"\b(budget|spend|allocat|roi|roas|cost.per|" r"efficiency|spend.optimization)\b",
    re.IGNORECASE,
)
_COMPLIANCE_CHECK_KEYWORDS = re.compile(
    r"\b(compliance|regulat|eeoc|ofccp|gdpr|ccpa|ada|"
    r"fair.hiring|discrimination|diversity.requirement)\b",
    re.IGNORECASE,
)
_MARKET_ANALYSIS_KEYWORDS = re.compile(
    r"\b(market.analysis|market.report|industry.analysis|"
    r"market.size|market.share|tam|sam|som)\b",
    re.IGNORECASE,
)
_COMPETITOR_SCAN_KEYWORDS = re.compile(
    r"\b(competitor|competitive|rival|competing|versus|vs\b|"
    r"compared.to|swot|differentiat)\b",
    re.IGNORECASE,
)
_TALENT_MAP_KEYWORDS = re.compile(
    r"\b(talent.map|talent.landscape|talent.pool|"
    r"candidate.pipeline|supply.demand|workforce.planning)\b",
    re.IGNORECASE,
)
_CONTEXT_SUMMARIZE_KEYWORDS = re.compile(
    r"\b(summarize.this|tldr|key.takeaway|recap|" r"brief.me|condense|distill)\b",
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER + RATE TRACKER (per provider)
# ═══════════════════════════════════════════════════════════════════════════════


class _ProviderState:
    """Thread-safe state tracker for a single LLM provider.

    Combines a hard circuit breaker (5 consecutive failures -> 60s cooldown)
    with a soft health score (0.0 to 1.0) that influences provider ordering.
    """

    def __init__(self, provider_id: str):
        self.provider_id = provider_id
        self.lock = threading.RLock()
        # Circuit breaker (hard cutoff)
        self.consecutive_failures = 0
        self.circuit_open_until = 0.0  # timestamp
        self.circuit_threshold = 5
        self.circuit_cooldown = 60.0  # seconds
        # Health score (soft signal, 0.0 to 1.0)
        self.health_score = 1.0
        # Rate tracking
        self.minute_calls: List[float] = []  # timestamps
        self.day_calls: List[float] = []  # timestamps
        # Stats
        self.total_calls = 0
        self.total_failures = 0
        self.total_rate_limits = 0
        self.total_latency_ms = 0.0

    def is_available(self) -> bool:
        """Check if provider is available (circuit not open, rate not exceeded, health not dead)."""
        now = time.time()
        with self.lock:
            # Circuit breaker (hard cutoff)
            if now < self.circuit_open_until:
                return False
            # Half-open recovery: reset counter so a single failure doesn't
            # immediately re-open the circuit after cooldown expires.
            if self.consecutive_failures >= self.circuit_threshold:
                self.consecutive_failures = self.circuit_threshold - 1
            # Health score cutoff: if score is too low, skip provider
            if self.health_score < 0.1:
                return False
            # Rate limiting (legacy per-provider tracking)
            config = PROVIDER_CONFIG.get(self.provider_id, {})
            rpm_limit = config.get("rpm_limit", 30)
            rpd_limit = config.get("rpd_limit", 10000)
            # Clean old entries
            self.minute_calls = [t for t in self.minute_calls if now - t < 60]
            self.day_calls = [t for t in self.day_calls if now - t < 86400]
            if len(self.minute_calls) >= rpm_limit:
                return False
            if len(self.day_calls) >= rpd_limit:
                return False
            return True

    def get_health_score(self) -> float:
        """Return the current health score (thread-safe read)."""
        with self.lock:
            return self.health_score

    def record_call(self) -> None:
        """Record an API call attempt."""
        now = time.time()
        with self.lock:
            self.minute_calls.append(now)
            self.day_calls.append(now)
            self.total_calls += 1
            # Inline cleanup: keep only last 24h, but cap at 10000 entries
            # to prevent unbounded growth under sustained high load.
            if len(self.day_calls) > 10000:
                cutoff = now - 86400
                self.day_calls = [t for t in self.day_calls if t > cutoff]

    def record_success(self, latency_ms: float) -> None:
        """Record a successful API call.  Health score moves toward 1.0."""
        with self.lock:
            self.consecutive_failures = 0
            self.total_latency_ms += latency_ms
            # EWMA toward 1.0: score = score * 0.8 + 0.2
            self.health_score = self.health_score * 0.8 + 0.2

    def record_failure(self) -> None:
        """Record a failed API call.  Health score drops.  May open circuit."""
        with self.lock:
            self.consecutive_failures += 1
            self.total_failures += 1
            # Health score drops: score = score * 0.8
            self.health_score = self.health_score * 0.8
            if self.consecutive_failures >= self.circuit_threshold:
                self.circuit_open_until = time.time() + self.circuit_cooldown
                logger.warning(
                    "LLM Router: Circuit breaker OPEN for %s (cooldown %.0fs, health=%.2f)",
                    self.provider_id,
                    self.circuit_cooldown,
                    self.health_score,
                )
                # Alert via email when circuit breaker opens
                try:
                    from email_alerts import send_circuit_breaker_alert

                    send_circuit_breaker_alert(
                        self.provider_id, self.consecutive_failures
                    )
                except Exception:
                    pass  # email alerts are best-effort

    def record_rate_limit(self) -> None:
        """Record a rate-limit response (429/403).

        Less penalty than a real error -- the provider isn't broken, just busy.
        """
        with self.lock:
            self.total_rate_limits += 1
            # Mild penalty: score = score * 0.9
            self.health_score = self.health_score * 0.9

    def get_stats(self) -> Dict[str, Any]:
        """Get provider stats including health score."""
        now = time.time()
        with self.lock:
            self.minute_calls = [t for t in self.minute_calls if now - t < 60]
            self.day_calls = [t for t in self.day_calls if now - t < 86400]
            avg_latency = self.total_latency_ms / max(
                1, self.total_calls - self.total_failures
            )
            return {
                "provider": self.provider_id,
                "name": PROVIDER_CONFIG.get(self.provider_id, {}).get("name") or "",
                "total_calls": self.total_calls,
                "total_failures": self.total_failures,
                "total_rate_limits": self.total_rate_limits,
                "health_score": round(self.health_score, 3),
                "calls_this_minute": len(self.minute_calls),
                "calls_today": len(self.day_calls),
                "circuit_open": now < self.circuit_open_until,
                "avg_latency_ms": round(avg_latency, 1),
                "available": self.is_available(),
            }


# Module-level provider states
_provider_states: Dict[str, _ProviderState] = {
    pid: _ProviderState(pid) for pid in PROVIDER_CONFIG
}

# ---- Circuit Breaker Mesh registration ----
try:
    from circuit_breaker_mesh import get_circuit_mesh as _get_cb_mesh

    _circuit_mesh = _get_cb_mesh()
    for _pid in PROVIDER_CONFIG:
        _circuit_mesh.register_provider(_pid)
    logger.info(f"CircuitBreakerMesh: registered {len(PROVIDER_CONFIG)} providers")
except ImportError:
    _circuit_mesh = None
    logger.debug("circuit_breaker_mesh module not available, skipping mesh init")


# ═══════════════════════════════════════════════════════════════════════════════
# TASK CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════


def classify_task(query: str, module: str = "") -> str:
    """Classify a user query into a task type for provider routing.

    Supports both the original 8 task types and the v4.0 platform module
    task types. Module-specific keywords take priority when matched.

    Args:
        query: User query string.
        module: Optional platform module hint (command_center, intelligence_hub, nova_ai).

    Returns:
        Task type string for routing.
    """
    try:
        q = query.lower().strip()

        # v4.0 platform module task types (highest priority -- most specific)
        module_scores = {
            TASK_CAMPAIGN_PLAN: len(_CAMPAIGN_PLAN_KEYWORDS.findall(q)) * 2.5,
            TASK_BUDGET_OPTIMIZE: len(_BUDGET_OPTIMIZE_KEYWORDS.findall(q)) * 2.5,
            TASK_COMPLIANCE_CHECK: len(_COMPLIANCE_CHECK_KEYWORDS.findall(q)) * 2.5,
            TASK_MARKET_ANALYSIS: len(_MARKET_ANALYSIS_KEYWORDS.findall(q)) * 2.5,
            TASK_COMPETITOR_SCAN: len(_COMPETITOR_SCAN_KEYWORDS.findall(q)) * 2.5,
            TASK_TALENT_MAP: len(_TALENT_MAP_KEYWORDS.findall(q)) * 2.5,
            TASK_CONTEXT_SUMMARIZE: len(_CONTEXT_SUMMARIZE_KEYWORDS.findall(q)) * 2.2,
        }

        # Original task types
        scores = {
            TASK_VERIFICATION: len(_VERIFICATION_KEYWORDS.findall(q)) * 2.0,
            TASK_RESEARCH: len(_RESEARCH_KEYWORDS.findall(q)) * 2.0,
            TASK_NARRATIVE: len(_NARRATIVE_KEYWORDS.findall(q)) * 1.8,
            TASK_BATCH: len(_BATCH_KEYWORDS.findall(q)) * 1.8,
            TASK_STRUCTURED: len(_STRUCTURED_KEYWORDS.findall(q)),
            TASK_COMPLEX: len(_COMPLEX_KEYWORDS.findall(q)) * 1.5,
            TASK_CODE: len(_CODE_KEYWORDS.findall(q)),
            TASK_CONVERSATIONAL: 0,
        }

        # Merge all scores
        all_scores = {**scores, **module_scores}

        best = max(all_scores, key=all_scores.get)  # type: ignore[arg-type]
        if all_scores[best] == 0:
            # No keyword match -- use module default if available
            if module:
                prefs = MODULE_LLM_PREFERENCES.get(module, {})
                default_task = prefs.get("default_task")
                if default_task:
                    return default_task
            return TASK_CONVERSATIONAL
        return best
    except Exception:
        return TASK_CONVERSATIONAL


def select_provider(
    task_type: str, exclude: Optional[List[str]] = None
) -> Optional[str]:
    """Select the best available provider for a task type.

    Uses a two-pass approach:
    1. Determine tier membership (free=0, paid=1) from task routing order.
       Free providers are indices 0..N-4, paid are the last 4 in each list.
    2. Within each tier, sort by health score (descending) so healthier
       providers are preferred.
    3. Check rate-aware limiter BEFORE circuit breaker -- rate-limited
       providers are skipped without penalty.

    Returns provider ID or None if all providers are unavailable.
    """
    exclude_set = set(exclude or [])
    priority = TASK_ROUTING.get(task_type, TASK_ROUTING[TASK_CONVERSATIONAL])

    # Determine paid provider set (last 4 in the routing list are paid)
    paid_providers = {CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS}

    # Build candidate list with (tier, negative_health, index, pid)
    # so sorting gives: free first, then paid; within tier, highest health first;
    # ties broken by original routing order index.
    candidates: list[tuple[int, float, int, str]] = []
    for idx, pid in enumerate(priority):
        if pid in exclude_set:
            continue
        config = PROVIDER_CONFIG.get(pid, {})
        env_key = config.get("env_key") or ""
        if not os.environ.get(env_key, "").strip():
            continue
        state = _provider_states.get(pid)
        if not state:
            continue
        tier = 1 if pid in paid_providers else 0
        health = state.get_health_score()
        candidates.append((tier, -health, idx, pid))

    candidates.sort()

    for _tier, _neg_health, _idx, pid in candidates:
        # Rate-aware check: skip if we've hit the sliding window limit
        if _rate_tracker.is_rate_limited(pid):
            logger.debug(
                "LLM Router: %s rate-limited (sliding window), skipping without penalty",
                pid,
            )
            continue
        # Shared rate limit for OpenRouter variants (all share one API key)
        if pid.startswith("openrouter"):
            if _rate_tracker.is_rate_limited("_openrouter_combined"):
                logger.debug(
                    "LLM Router: %s skipped -- combined openrouter key rate-limited (20 RPM shared)",
                    pid,
                )
                continue
        # Circuit breaker mesh check (skip if mesh blocks this provider)
        if _circuit_mesh is not None and not _circuit_mesh.can_use(pid):
            logger.debug("LLM Router: %s blocked by circuit mesh, skipping", pid)
            continue
        # Circuit breaker + health score check
        state = _provider_states.get(pid)
        if state and state.is_available():
            return pid

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# API CALL ADAPTERS (normalize request/response across providers)
# ═══════════════════════════════════════════════════════════════════════════════


def _build_gemini_request(
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    tools: Optional[List[Dict]] = None,
    provider_id: str = GEMINI,
) -> Tuple[str, Dict[str, str], bytes]:
    """Build a Gemini API request.

    Supports both gemini-2.0-flash and gemini-2.0-flash-lite via provider_id.
    """
    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    config = PROVIDER_CONFIG.get(provider_id) or PROVIDER_CONFIG[GEMINI]
    url = f"{config['endpoint']}?key={api_key}"

    # Convert messages to Gemini format
    contents = []
    for msg in messages:
        role = "model" if msg["role"] == "assistant" else "user"
        text = msg.get("content") or ""
        if isinstance(text, str):
            contents.append({"role": role, "parts": [{"text": text}]})

    payload: Dict[str, Any] = {
        "contents": contents,
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.7,
        },
    }

    # System instruction
    if system_prompt:
        payload["systemInstruction"] = {"parts": [{"text": system_prompt}]}

    headers = {"Content-Type": "application/json"}
    return url, headers, json.dumps(payload).encode("utf-8")


def _convert_tools_anthropic_to_openai(tools: List[Dict]) -> List[Dict]:
    """Convert Anthropic tool definitions to OpenAI function-calling format.

    Anthropic format:
        {"name": "X", "description": "Y", "input_schema": {...}, "cache_control": ...}

    OpenAI format:
        {"type": "function", "function": {"name": "X", "description": "Y", "parameters": {...}}}

    Strips Anthropic-specific keys (cache_control) that would cause 400 errors on
    OpenAI-compatible endpoints.
    """
    openai_tools = []
    for tool in tools:
        name = tool.get("name") or ""
        if not name:
            continue
        fn: Dict[str, Any] = {
            "name": name,
            "description": tool.get("description") or "",
        }
        # Anthropic uses 'input_schema', OpenAI uses 'parameters'
        schema = tool.get("input_schema") or tool.get("parameters")
        if schema:
            # Strip Anthropic-specific keys from schema copy
            clean_schema = {k: v for k, v in schema.items() if k != "cache_control"}
            fn["parameters"] = clean_schema
        else:
            fn["parameters"] = {"type": "object", "properties": {}}
        openai_tools.append({"type": "function", "function": fn})
    return openai_tools


def _build_openai_request(
    provider_id: str,
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    tools: Optional[List[Dict]] = None,
) -> Tuple[str, Dict[str, str], bytes]:
    """Build an OpenAI-compatible API request (Groq, Cerebras, Mistral, xAI, OpenRouter, SambaNova, NVIDIA NIM, Cloudflare, Zhipu, SiliconFlow)."""
    config = PROVIDER_CONFIG[provider_id]
    api_key = os.environ.get(config["env_key"], "").strip()

    # Build messages with system prompt
    api_messages = []
    if system_prompt:
        api_messages.append({"role": "system", "content": system_prompt})

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content") or ""

        # Pass through tool result messages as-is (role="tool")
        if role == "tool":
            api_messages.append(
                {
                    "role": "tool",
                    "tool_call_id": msg.get("tool_call_id") or "",
                    "content": str(msg.get("content") or ""),
                }
            )
        # Pass through assistant messages that contain tool_calls
        elif role == "assistant" and msg.get("tool_calls"):
            assistant_msg: Dict[str, Any] = {"role": "assistant"}
            # OpenAI spec: content can be null for assistant+tool_calls
            assistant_msg["content"] = content if content else None
            assistant_msg["tool_calls"] = msg["tool_calls"]
            api_messages.append(assistant_msg)
        # Regular text messages
        elif isinstance(content, str) and content:
            api_messages.append({"role": role, "content": content})

    payload: Dict[str, Any] = {
        "model": config["model"],
        "messages": api_messages,
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }

    # Add tools to payload if provided (convert Anthropic -> OpenAI format)
    if tools:
        openai_tools = _convert_tools_anthropic_to_openai(tools)
        if openai_tools:
            payload["tools"] = openai_tools
            payload["tool_choice"] = "auto"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    # Merge any provider-specific extra headers (e.g., OpenRouter's HTTP-Referer)
    extra = config.get("extra_headers")
    if extra and isinstance(extra, dict):
        headers.update(extra)

    # Resolve dynamic endpoint variables (e.g., Cloudflare {account_id})
    endpoint = config["endpoint"]
    if "{account_id}" in endpoint:
        acct_key = config.get("env_key_account", "CLOUDFLARE_ACCOUNT_ID")
        account_id = os.environ.get(acct_key, "").strip()
        if not account_id:
            raise ValueError(f"Missing {acct_key} for Cloudflare Workers AI")
        endpoint = endpoint.replace("{account_id}", account_id)

    return endpoint, headers, json.dumps(payload).encode("utf-8")


def _build_anthropic_request(
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    tools: Optional[List[Dict]] = None,
    provider_id: str = CLAUDE,
) -> Tuple[str, Dict[str, str], bytes]:
    """Build an Anthropic API request (works for both Sonnet and Opus)."""
    config = PROVIDER_CONFIG[provider_id]
    api_key = os.environ.get(config["env_key"], "").strip()

    # Filter messages to only user/assistant with string content
    api_messages = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content") or ""
        if role in ("user", "assistant") and isinstance(content, str) and content:
            api_messages.append({"role": role, "content": content})

    payload: Dict[str, Any] = {
        "model": config["model"],
        "max_tokens": max_tokens,
        "messages": api_messages,
    }

    if system_prompt:
        payload["system"] = system_prompt

    if tools:
        payload["tools"] = tools

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    return config["endpoint"], headers, json.dumps(payload).encode("utf-8")


def _build_huggingface_request(
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
) -> Tuple[str, Dict[str, str], bytes]:
    """Build a HuggingFace Inference API request.

    HuggingFace Inference uses a text-generation format:
        Input:  {"inputs": "prompt", "parameters": {"max_new_tokens": N}}
        Output: [{"generated_text": "..."}]
    """
    config = PROVIDER_CONFIG[HUGGINGFACE]
    api_key = (os.environ.get(config["env_key"]) or "").strip()

    # Build prompt from messages (HF expects a single string)
    prompt_parts: List[str] = []
    if system_prompt:
        prompt_parts.append(f"[INST] <<SYS>>\n{system_prompt}\n<</SYS>>\n")
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content") or ""
        if not isinstance(content, str) or not content:
            continue
        if role == "user":
            prompt_parts.append(f"[INST] {content} [/INST]")
        elif role == "assistant":
            prompt_parts.append(content)

    prompt = "\n".join(prompt_parts)

    payload: Dict[str, Any] = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": min(max_tokens, 1024),
            "temperature": 0.7,
            "return_full_text": False,
        },
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    return config["endpoint"], headers, json.dumps(payload).encode("utf-8")


def _parse_huggingface_response(resp_data: Any) -> Dict[str, Any]:
    """Parse HuggingFace Inference API response to normalized format.

    HuggingFace returns: [{"generated_text": "..."}]
    """
    try:
        if isinstance(resp_data, list) and resp_data:
            text = resp_data[0].get("generated_text") or ""
        elif isinstance(resp_data, dict):
            # Some models return a dict with "generated_text"
            text = resp_data.get("generated_text") or ""
        else:
            text = ""
        return {
            "text": text.strip(),
            "input_tokens": 0,  # HF doesn't report token usage
            "output_tokens": 0,
            "model": "mistralai/Mistral-7B-Instruct-v0.3",
            "stop_reason": "stop",
        }
    except Exception as e:
        return {"text": "", "error": str(e)}


def _parse_gemini_response(resp_data: Dict) -> Dict[str, Any]:
    """Parse Gemini API response to normalized format."""
    try:
        candidates = resp_data.get("candidates") or []
        if not candidates:
            return {"text": "", "error": "No candidates in response"}
        content = candidates[0].get("content", {})
        parts = content.get("parts") or []
        text = " ".join(p.get("text") or "" for p in parts if "text" in p)
        usage = resp_data.get("usageMetadata", {})
        # Use modelVersion from response if available, fallback to generic name
        model_name = resp_data.get("modelVersion") or "gemini-2.5-flash"
        return {
            "text": text.strip(),
            "input_tokens": usage.get("promptTokenCount") or 0,
            "output_tokens": usage.get("candidatesTokenCount") or 0,
            "model": model_name,
            "stop_reason": candidates[0].get("finishReason", "STOP"),
        }
    except Exception as e:
        return {"text": "", "error": str(e)}


def _parse_openai_response(resp_data: Dict) -> Dict[str, Any]:
    """Parse OpenAI-compatible response to normalized format.

    Handles both regular text responses and tool_calls responses.
    When tool_calls are present, result includes:
        - tool_calls: list of OpenAI tool_call objects
        - raw_message: full assistant message for conversation threading
        - stop_reason: "tool_calls" (indicating tools need to be executed)
    """
    try:
        choices = resp_data.get("choices") or []
        if not choices:
            return {"text": "", "error": "No choices in response"}
        message = choices[0].get("message", {})
        text = message.get("content") or "" or ""
        usage = resp_data.get("usage", {})

        result: Dict[str, Any] = {
            "text": text.strip(),
            "input_tokens": usage.get("prompt_tokens") or 0,
            "output_tokens": usage.get("completion_tokens") or 0,
            "model": resp_data.get("model") or "",
            "stop_reason": choices[0].get("finish_reason", "stop"),
        }

        # Handle tool calls in response
        tool_calls = message.get("tool_calls")
        if tool_calls:
            result["tool_calls"] = tool_calls
            # Preserve full message so caller can append it to conversation history
            result["raw_message"] = message

        return result
    except Exception as e:
        return {"text": "", "error": str(e)}


def _parse_anthropic_response(resp_data: Dict) -> Dict[str, Any]:
    """Parse Anthropic API response to normalized format."""
    try:
        content_blocks = resp_data.get("content") or []
        text_parts = []
        for block in content_blocks:
            if block.get("type") == "text":
                text_parts.append(block.get("text") or "")
        usage = resp_data.get("usage", {})
        return {
            "text": " ".join(text_parts).strip(),
            "input_tokens": usage.get("input_tokens") or 0,
            "output_tokens": usage.get("output_tokens") or 0,
            "model": resp_data.get("model") or "",
            "stop_reason": resp_data.get("stop_reason", "end_turn"),
            # Preserve raw for tool_use compatibility
            "raw_content": content_blocks,
            "raw_stop_reason": resp_data.get("stop_reason") or "",
        }
    except Exception as e:
        return {"text": "", "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ROUTER FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════


def call_llm(
    messages: List[Dict],
    system_prompt: str = "",
    max_tokens: int = 4096,
    task_type: str = "",
    tools: Optional[List[Dict]] = None,
    force_provider: str = "",
    query_text: str = "",
    preferred_providers: Optional[List[str]] = None,
    use_cache: bool = True,
    priority: str = RequestPriority.MEDIUM,
) -> Dict[str, Any]:
    """Route an LLM call to the best available provider.

    Flow:  rate check -> priority filter -> health-score ordering -> cache check -> API call

    Args:
        messages: Conversation messages [{role, content}, ...]
        system_prompt: System prompt string
        max_tokens: Max output tokens
        task_type: Override task classification (or auto-detect from query_text)
        tools: Tool definitions (Anthropic format, auto-converted for OpenAI providers)
        force_provider: Force a specific provider (skip routing)
        query_text: User query for task classification (if task_type not given)
        preferred_providers: Optional list of provider IDs to try first before
            falling back to the standard routing order. Useful for requesting
            a specific provider (e.g., ["gemini"] for verification) while still
            allowing fallback if that provider is unavailable.
        use_cache: If True (default), check the response cache before calling
            a provider, and store successful responses.  Set to False for
            tasks that need fresh data (e.g., real-time queries).
        priority: Request priority level (RequestPriority.HIGH, MEDIUM, LOW).
            HIGH priority requests skip providers near their rate limits.

    Returns:
        {
            "text": "response text",
            "provider": "gemini|groq|cerebras|claude|claude_opus",
            "provider_name": "Gemini 2.5 Flash",
            "model": "gemini-2.5-flash",
            "task_type": "conversational",
            "input_tokens": 100,
            "output_tokens": 200,
            "latency_ms": 450,
            "fallback_used": False,
            "cache_hit": False,
            "attempts": [{"provider": "gemini", "status": "success", "latency_ms": 450}],
        }
    """
    # Classify task if not provided
    if not task_type and query_text:
        task_type = classify_task(query_text)
    task_type = task_type or TASK_CONVERSATIONAL

    # --- Response cache check (before any API calls) ---
    # Extract user message for cache key (last user message in the list)
    _user_msg_for_cache = ""
    if use_cache and not tools:
        for msg in reversed(messages):
            if msg.get("role") == "user" and isinstance(msg.get("content"), str):
                _user_msg_for_cache = msg["content"]
                break
        if _user_msg_for_cache:
            cached = _response_cache.get(task_type, system_prompt, _user_msg_for_cache)
            if cached is not None:
                logger.info(
                    "LLM Router: cache HIT for task_type=%s (provider=%s)",
                    task_type,
                    cached.get("provider") or "unknown",
                )
                # Return a copy with cache_hit flag
                result = dict(cached)
                result["cache_hit"] = True
                result["task_type"] = task_type
                result["fallback_used"] = False
                result["attempts"] = []
                return result

    # Tools are now supported by all OpenAI-compatible providers (auto-converted
    # from Anthropic format in _build_openai_request).  No longer force Claude.

    attempts: List[Dict[str, Any]] = []
    excluded: List[str] = []

    # Force-provider mode
    if force_provider:
        _rate_tracker.record_request(force_provider)
        result = _call_single_provider(
            force_provider, messages, system_prompt, max_tokens, tools
        )
        result["task_type"] = task_type
        result["fallback_used"] = False
        result["cache_hit"] = False
        result["attempts"] = [
            {
                "provider": force_provider,
                "status": "success" if result.get("text") else "failed",
                "latency_ms": result.get("latency_ms") or 0,
            }
        ]
        # Cache successful forced-provider results too
        if use_cache and not tools and result.get("text") and _user_msg_for_cache:
            _response_cache.put(task_type, system_prompt, _user_msg_for_cache, result)
        return result

    # Build custom routing order if preferred_providers given
    if preferred_providers:
        base_route = TASK_ROUTING.get(task_type, TASK_ROUTING[TASK_CONVERSATIONAL])
        # Preferred providers first, then the rest in standard order
        custom_route = list(preferred_providers)
        for pid in base_route:
            if pid not in custom_route:
                custom_route.append(pid)
    else:
        custom_route = None

    # Smart routing with fallback
    max_attempts = len(PROVIDER_CONFIG)
    _wall_start = time.time()
    for attempt_num in range(max_attempts):
        # --- Global timeout budget check ---
        elapsed = time.time() - _wall_start
        if elapsed > GLOBAL_TIMEOUT_BUDGET:
            logger.warning(
                "LLM Router: global timeout budget (%.1fs) exceeded after %d attempts",
                GLOBAL_TIMEOUT_BUDGET,
                attempt_num,
            )
            break
        remaining = GLOBAL_TIMEOUT_BUDGET - elapsed
        if remaining < _MIN_REMAINING_BUDGET:
            logger.warning(
                "LLM Router: only %.1fs remaining in budget, stopping early",
                remaining,
            )
            break

        if custom_route:
            # Use custom routing order with rate-aware + health-score checks
            provider = None
            for pid in custom_route:
                if pid in excluded:
                    continue
                config = PROVIDER_CONFIG.get(pid, {})
                env_key = config.get("env_key") or ""
                if not os.environ.get(env_key, "").strip():
                    continue
                # Rate-aware check first (skip without penalty)
                if _rate_tracker.is_rate_limited(pid):
                    logger.debug(
                        "LLM Router: %s rate-limited (sliding window), skipping",
                        pid,
                    )
                    continue
                # Shared rate limit for OpenRouter variants (all share one API key)
                if pid.startswith("openrouter_"):
                    if _rate_tracker.is_rate_limited("openrouter"):
                        logger.debug(
                            "LLM Router: %s skipped -- parent openrouter key rate-limited",
                            pid,
                        )
                        continue
                # HIGH priority: also skip providers near rate limits (>80% utilization)
                if priority == RequestPriority.HIGH:
                    limits = _RATE_LIMITS.get(pid)
                    if limits:
                        rate_counts = _rate_tracker.get_counts()
                        current = rate_counts.get(pid, 0)
                        if current > limits["rpm"] * 0.8:
                            logger.debug(
                                "LLM Router: %s near rate limit (%d/%d), skipping for HIGH priority",
                                pid,
                                current,
                                limits["rpm"],
                            )
                            continue
                # Circuit breaker mesh check
                if _circuit_mesh is not None and not _circuit_mesh.can_use(pid):
                    logger.debug(
                        "LLM Router: %s blocked by circuit mesh (custom route), skipping",
                        pid,
                    )
                    continue
                state = _provider_states.get(pid)
                if state and state.is_available():
                    provider = pid
                    break
        else:
            provider = select_provider(task_type, exclude=excluded)
        if provider is None:
            break

        # Record in rate tracker before calling
        _rate_tracker.record_request(provider)

        result = _call_single_provider(
            provider,
            messages,
            system_prompt,
            max_tokens,
            tools,
            timeout_override=remaining,
        )
        _has_response = bool(
            result.get("text") or result.get("raw_content") or result.get("tool_calls")
        )
        attempts.append(
            {
                "provider": provider,
                "status": "success" if _has_response else "failed",
                "latency_ms": result.get("latency_ms") or 0,
                "error": result.get("error") or "",
            }
        )

        if _has_response:
            result["task_type"] = task_type
            result["fallback_used"] = attempt_num > 0
            result["cache_hit"] = False
            result["attempts"] = attempts
            # Cost tracking
            _input_tok = result.get("input_tokens") or 0
            _output_tok = result.get("output_tokens") or 0
            _est_cost = _cost_tracker.record_usage(provider, _input_tok, _output_tok)
            result["estimated_cost_usd"] = round(_est_cost, 6)
            # Quality scoring
            _resp_text = result.get("text") or ""
            if _resp_text:
                result["quality_score"] = compute_quality_score(_resp_text, task_type)
                result["quality_score_fast"] = _score_response_quality(
                    _resp_text, task_type
                )
            result["priority"] = priority
            # Cache successful responses (only text-based, not tool_calls)
            if use_cache and not tools and result.get("text") and _user_msg_for_cache:
                _response_cache.put(
                    task_type, system_prompt, _user_msg_for_cache, result
                )
            return result

        # Failed -- exclude and try next
        excluded.append(provider)
        logger.warning(
            "LLM Router: %s failed (attempt %d), trying next provider. Error: %s",
            provider,
            attempt_num + 1,
            result.get("error", "unknown"),
        )

    # All providers failed
    return {
        "text": "",
        "provider": "",
        "provider_name": "",
        "model": "",
        "task_type": task_type,
        "input_tokens": 0,
        "output_tokens": 0,
        "latency_ms": 0,
        "fallback_used": True,
        "cache_hit": False,
        "attempts": attempts,
        "error": "All LLM providers unavailable or failed",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# STREAMING LLM CALL (real token-level SSE streaming)
# ═══════════════════════════════════════════════════════════════════════════════

# Providers that do NOT support streaming -- fall back to non-streaming
_NO_STREAM_PROVIDERS = frozenset({HUGGINGFACE, CLOUDFLARE})

# Streaming timeout for the HTTP connection (longer than normal to keep alive)
_STREAM_TIMEOUT = 90


def _stream_openai_compatible(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
) -> Generator[str, None, None]:
    """Stream tokens from an OpenAI-compatible SSE endpoint.

    Sets stream=True, reads the response line-by-line, parses SSE
    data events, and yields delta content tokens as they arrive.
    """
    payload["stream"] = True
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    resp = urllib.request.urlopen(req, timeout=_STREAM_TIMEOUT)
    try:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            if line == "data: [DONE]":
                break
            if not line.startswith("data: "):
                continue
            try:
                chunk = json.loads(line[6:])
                choices = chunk.get("choices") or []
                if choices:
                    delta = choices[0].get("delta") or {}
                    content = delta.get("content") or ""
                    if content:
                        yield content
            except (json.JSONDecodeError, KeyError, IndexError):
                continue
    finally:
        try:
            resp.close()
        except Exception:
            pass


def _stream_gemini(
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    provider_id: str = GEMINI,
) -> Generator[str, None, None]:
    """Stream tokens from the Gemini streaming endpoint.

    Uses the streamGenerateContent endpoint which returns newline-delimited
    JSON objects, each containing partial candidates.  Supports both
    gemini-2.0-flash and gemini-2.0-flash-lite via provider_id.
    """
    api_key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    config = PROVIDER_CONFIG.get(provider_id) or PROVIDER_CONFIG[GEMINI]
    # Swap generateContent -> streamGenerateContent
    base_endpoint = config["endpoint"]
    stream_endpoint = base_endpoint.replace(
        ":generateContent", ":streamGenerateContent"
    )
    url = f"{stream_endpoint}?alt=sse&key={api_key}"

    # Build Gemini payload
    contents = []
    for msg in messages:
        role = "model" if msg["role"] == "assistant" else "user"
        text = msg.get("content") or ""
        if isinstance(text, str):
            contents.append({"role": role, "parts": [{"text": text}]})

    payload: Dict[str, Any] = {
        "contents": contents,
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.7,
        },
    }
    if system_prompt:
        payload["systemInstruction"] = {"parts": [{"text": system_prompt}]}

    headers = {"Content-Type": "application/json"}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    resp = urllib.request.urlopen(req, timeout=_STREAM_TIMEOUT)
    try:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            if not line.startswith("data: "):
                continue
            try:
                chunk = json.loads(line[6:])
                candidates = chunk.get("candidates") or []
                if candidates:
                    content = candidates[0].get("content") or {}
                    parts = content.get("parts") or []
                    for part in parts:
                        text = part.get("text") or ""
                        if text:
                            yield text
            except (json.JSONDecodeError, KeyError, IndexError):
                continue
    finally:
        try:
            resp.close()
        except Exception:
            pass


def _stream_anthropic(
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    provider_id: str = CLAUDE,
) -> Generator[str, None, None]:
    """Stream tokens from the Anthropic Messages API.

    Anthropic uses a different SSE format with event types:
      event: content_block_delta
      data: {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "..."}}
    """
    config = PROVIDER_CONFIG[provider_id]
    api_key = os.environ.get(config["env_key"], "").strip()

    api_messages = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content") or ""
        if role in ("user", "assistant") and isinstance(content, str) and content:
            api_messages.append({"role": role, "content": content})

    payload: Dict[str, Any] = {
        "model": config["model"],
        "max_tokens": max_tokens,
        "messages": api_messages,
        "stream": True,
    }
    if system_prompt:
        payload["system"] = system_prompt

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        config["endpoint"], data=data, headers=headers, method="POST"
    )
    resp = urllib.request.urlopen(req, timeout=_STREAM_TIMEOUT)
    try:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            if not line.startswith("data: "):
                continue
            try:
                chunk = json.loads(line[6:])
                chunk_type = chunk.get("type") or ""
                if chunk_type == "content_block_delta":
                    delta = chunk.get("delta") or {}
                    text = delta.get("text") or ""
                    if text:
                        yield text
            except (json.JSONDecodeError, KeyError):
                continue
    finally:
        try:
            resp.close()
        except Exception:
            pass


def _stream_single_provider(
    provider_id: str,
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
) -> Generator[str, None, None]:
    """Stream tokens from a single provider.

    Dispatches to the appropriate streaming implementation based on
    the provider's api_style. For providers that don't support streaming,
    falls back to a non-streaming call and yields the full response.
    """
    config = PROVIDER_CONFIG.get(provider_id)
    if not config:
        return

    api_style = config.get("api_style") or ""

    # Providers without streaming support: full response as single yield
    if provider_id in _NO_STREAM_PROVIDERS:
        result = _call_single_provider(provider_id, messages, system_prompt, max_tokens)
        text = result.get("text") or ""
        if text:
            yield text
        return

    if api_style == "gemini":
        yield from _stream_gemini(
            messages, system_prompt, max_tokens, provider_id=provider_id
        )
    elif api_style == "anthropic":
        yield from _stream_anthropic(
            messages, system_prompt, max_tokens, provider_id=provider_id
        )
    elif api_style == "openai":
        # Build the OpenAI-compatible request, then stream
        url, headers, body_bytes = _build_openai_request(
            provider_id, messages, system_prompt, max_tokens
        )
        payload = json.loads(body_bytes.decode("utf-8"))
        yield from _stream_openai_compatible(url, headers, payload)
    else:
        # Unknown style -- non-streaming fallback
        result = _call_single_provider(provider_id, messages, system_prompt, max_tokens)
        text = result.get("text") or ""
        if text:
            yield text


def call_llm_stream(
    messages: List[Dict],
    system_prompt: str = "",
    max_tokens: int = 2048,
    task_type: str = "",
    query_text: str = "",
    preferred_providers: Optional[List[str]] = None,
) -> Generator[str, None, None]:
    """Stream LLM tokens from the best available provider.

    Uses the same provider selection logic as call_llm (circuit breaker,
    health scoring, rate limits) but returns a generator that yields
    text chunks as they arrive from the provider's SSE stream.

    If streaming fails mid-stream for a provider, catches the exception
    and falls back to the next available provider using non-streaming
    call_llm(), yielding the full response as a single chunk.

    Args:
        messages: Conversation messages [{role, content}, ...]
        system_prompt: System prompt string
        max_tokens: Max output tokens
        task_type: Override task classification
        query_text: User query for task classification
        preferred_providers: Optional list of provider IDs to try first

    Yields:
        Text chunks (tokens) as they arrive from the LLM provider.
    """
    # Classify task if not provided
    if not task_type and query_text:
        task_type = classify_task(query_text)
    task_type = task_type or TASK_CONVERSATIONAL

    # Build routing order
    if preferred_providers:
        base_route = TASK_ROUTING.get(task_type, TASK_ROUTING[TASK_CONVERSATIONAL])
        custom_route = list(preferred_providers)
        for pid in base_route:
            if pid not in custom_route:
                custom_route.append(pid)
    else:
        custom_route = None

    excluded: List[str] = []
    max_attempts = len(PROVIDER_CONFIG)
    _wall_start = time.time()

    for attempt_num in range(max_attempts):
        # Global timeout budget check
        elapsed = time.time() - _wall_start
        if elapsed > GLOBAL_TIMEOUT_BUDGET:
            logger.warning(
                "LLM Stream: global timeout budget (%.1fs) exceeded after %d attempts",
                GLOBAL_TIMEOUT_BUDGET,
                attempt_num,
            )
            break
        remaining = GLOBAL_TIMEOUT_BUDGET - elapsed
        if remaining < _MIN_REMAINING_BUDGET:
            logger.warning(
                "LLM Stream: only %.1fs remaining in budget, stopping early",
                remaining,
            )
            break

        # Select provider
        if custom_route:
            provider = None
            for pid in custom_route:
                if pid in excluded:
                    continue
                config = PROVIDER_CONFIG.get(pid, {})
                env_key = config.get("env_key") or ""
                if not os.environ.get(env_key, "").strip():
                    continue
                # Circuit breaker mesh check (streaming path)
                if _circuit_mesh is not None and not _circuit_mesh.can_use(pid):
                    logger.debug(
                        "LLM Stream: %s blocked by circuit mesh, skipping", pid
                    )
                    continue
                state = _provider_states.get(pid)
                if state and state.is_available():
                    provider = pid
                    break
        else:
            provider = select_provider(task_type, exclude=excluded)

        if provider is None:
            break

        state = _provider_states.get(provider)
        if state:
            state.record_call()
        # Also record in the rate tracker
        _rate_tracker.record_request(provider)

        start_time = time.time()
        any_tokens = False

        try:
            logger.info(
                "LLM Stream: attempting %s (attempt %d)", provider, attempt_num + 1
            )
            for token in _stream_single_provider(
                provider, messages, system_prompt, max_tokens
            ):
                any_tokens = True
                yield token

            if any_tokens:
                latency_ms = round((time.time() - start_time) * 1000, 1)
                if state:
                    state.record_success(latency_ms)
                # Circuit breaker mesh: record streaming success
                if _circuit_mesh is not None:
                    _circuit_mesh.record_success(provider, latency_ms)
                logger.info(
                    "LLM Stream: %s completed streaming in %.0fms",
                    provider,
                    latency_ms,
                )
                return  # Success -- done

            # No tokens yielded -- treat as failure
            if state:
                state.record_failure()
            # Circuit breaker mesh: record streaming failure
            if _circuit_mesh is not None:
                _circuit_mesh.record_failure(provider, "no tokens yielded")
            excluded.append(provider)
            logger.warning(
                "LLM Stream: %s yielded no tokens (attempt %d), trying next",
                provider,
                attempt_num + 1,
            )

        except (urllib.error.HTTPError, urllib.error.URLError) as http_err:
            if state:
                state.record_failure()
            # Circuit breaker mesh: record streaming HTTP failure
            if _circuit_mesh is not None:
                _circuit_mesh.record_failure(provider, f"stream HTTP error: {http_err}")
            excluded.append(provider)
            logger.error(
                "LLM Stream: %s HTTP error: %s (attempt %d)",
                provider,
                http_err,
                attempt_num + 1,
                exc_info=True,
            )

            # If we already yielded some tokens, fall back to non-streaming
            # for a COMPLETE response from the next provider
            if any_tokens:
                logger.warning(
                    "LLM Stream: mid-stream failure on %s after partial tokens, "
                    "falling back to non-streaming call_llm()",
                    provider,
                )
                fallback_result = call_llm(
                    messages=messages,
                    system_prompt=system_prompt,
                    max_tokens=max_tokens,
                    task_type=task_type,
                    query_text=query_text,
                )
                fallback_text = fallback_result.get("text") or ""
                if fallback_text:
                    yield fallback_text
                return

        except Exception as exc:
            if state:
                state.record_failure()
            # Circuit breaker mesh: record streaming exception
            if _circuit_mesh is not None:
                _circuit_mesh.record_failure(
                    provider, f"stream error: {str(exc)[:200]}"
                )
            excluded.append(provider)
            logger.error(
                "LLM Stream: %s error: %s (attempt %d)",
                provider,
                exc,
                attempt_num + 1,
                exc_info=True,
            )

            if any_tokens:
                logger.warning(
                    "LLM Stream: mid-stream failure on %s, "
                    "falling back to non-streaming call_llm()",
                    provider,
                )
                fallback_result = call_llm(
                    messages=messages,
                    system_prompt=system_prompt,
                    max_tokens=max_tokens,
                    task_type=task_type,
                    query_text=query_text,
                )
                fallback_text = fallback_result.get("text") or ""
                if fallback_text:
                    yield fallback_text
                return

    # All providers failed -- yield nothing (caller should handle empty stream)
    logger.error("LLM Stream: all providers failed, no tokens yielded")


def _call_single_provider(
    provider_id: str,
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    tools: Optional[List[Dict]] = None,
    timeout_override: Optional[float] = None,
) -> Dict[str, Any]:
    """Make a single API call to a specific provider.

    Args:
        timeout_override: If provided, the per-provider config timeout is
            capped to this value so the global budget is respected.
    """
    config = PROVIDER_CONFIG.get(provider_id)
    if not config:
        return {"text": "", "provider": provider_id, "error": "Unknown provider"}

    state = _provider_states.get(provider_id)
    api_style = config.get("api_style") or ""
    timeout = config.get("timeout", 30)
    if timeout_override is not None:
        timeout = min(timeout, timeout_override)

    try:
        # Build request based on API style
        if api_style == "gemini":
            url, headers, body = _build_gemini_request(
                messages, system_prompt, max_tokens, tools, provider_id=provider_id
            )
        elif api_style == "openai":
            url, headers, body = _build_openai_request(
                provider_id, messages, system_prompt, max_tokens, tools
            )
        elif api_style == "anthropic":
            url, headers, body = _build_anthropic_request(
                messages, system_prompt, max_tokens, tools, provider_id=provider_id
            )
        elif api_style == "huggingface":
            url, headers, body = _build_huggingface_request(
                messages, system_prompt, max_tokens
            )
        else:
            return {
                "text": "",
                "provider": provider_id,
                "error": f"Unknown API style: {api_style}",
            }

        # Record call attempt
        if state:
            state.record_call()

        # Make HTTP request
        start = time.time()
        req = urllib.request.Request(url, data=body, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
        latency_ms = round((time.time() - start) * 1000, 1)

        # Parse response based on API style
        if api_style == "gemini":
            parsed = _parse_gemini_response(resp_data)
        elif api_style == "openai":
            parsed = _parse_openai_response(resp_data)
        elif api_style == "anthropic":
            parsed = _parse_anthropic_response(resp_data)
        elif api_style == "huggingface":
            parsed = _parse_huggingface_response(resp_data)
        else:
            parsed = {"text": "", "error": "Unknown parse style"}

        # Record success/failure (tool_calls count as success even without text)
        if parsed.get("text") or parsed.get("raw_content") or parsed.get("tool_calls"):
            if state:
                state.record_success(latency_ms)
            # Circuit breaker mesh: record success
            if _circuit_mesh is not None:
                _circuit_mesh.record_success(provider_id, latency_ms)
            logger.info(
                "LLM Router: %s responded in %.0fms (in=%d, out=%d)",
                provider_id,
                latency_ms,
                parsed.get("input_tokens") or 0,
                parsed.get("output_tokens") or 0,
            )
        else:
            if state:
                state.record_failure()
            # Circuit breaker mesh: record failure (empty response)
            if _circuit_mesh is not None:
                _circuit_mesh.record_failure(provider_id, "empty response")

        parsed["provider"] = provider_id
        parsed["provider_name"] = config.get("name") or ""
        parsed["latency_ms"] = latency_ms
        return parsed

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8")[:500]
        except Exception:
            pass

        # Rate-limit responses (429 Too Many Requests, 403 Forbidden)
        # get a softer health penalty -- the provider isn't broken, just busy.
        if state:
            if http_err.code in (429, 403):
                state.record_rate_limit()
                logger.warning(
                    "LLM Router: %s rate-limited (HTTP %d), soft penalty applied",
                    provider_id,
                    http_err.code,
                )
            else:
                state.record_failure()

        # Circuit breaker mesh: record failure for non-rate-limit errors
        if _circuit_mesh is not None and http_err.code not in (429, 403):
            _circuit_mesh.record_failure(
                provider_id, f"HTTP {http_err.code}: {error_body[:100]}"
            )

        logger.error(
            "LLM Router: %s HTTP %d: %s", provider_id, http_err.code, error_body[:200]
        )
        # ── PostHog: Track provider failure ──
        try:
            from posthog_integration import track_event as _ph_track_evt

            _ph_track_evt(
                "server",
                "llm_provider_failure",
                {
                    "provider": provider_id,
                    "error_type": "HTTPError",
                    "status_code": http_err.code,
                },
            )
        except Exception:
            pass
        return {
            "text": "",
            "provider": provider_id,
            "provider_name": config.get("name") or "",
            "error": f"HTTP {http_err.code}: {error_body[:200]}",
            "latency_ms": 0,
        }
    except Exception as exc:
        if state:
            state.record_failure()
        # Circuit breaker mesh: record failure
        if _circuit_mesh is not None:
            _circuit_mesh.record_failure(provider_id, str(exc)[:200])
        logger.error("LLM Router: %s error: %s", provider_id, exc, exc_info=True)
        # ── PostHog: Track provider failure ──
        try:
            from posthog_integration import track_event as _ph_track_evt

            _ph_track_evt(
                "server",
                "llm_provider_failure",
                {
                    "provider": provider_id,
                    "error_type": type(exc).__name__,
                    "status_code": 0,
                },
            )
        except Exception:
            pass
        return {
            "text": "",
            "provider": provider_id,
            "provider_name": config.get("name") or "",
            "error": str(exc),
            "latency_ms": 0,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS & DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════════════════


def get_router_status() -> Dict[str, Any]:
    """Return status of all providers and routing configuration."""
    providers = {}
    rate_counts = _rate_tracker.get_counts()
    for pid in PROVIDER_CONFIG:
        config = PROVIDER_CONFIG[pid]
        state = _provider_states.get(pid)
        has_key = bool(os.environ.get(config.get("env_key") or "", "").strip())
        provider_info = {
            "name": config.get("name") or "",
            "configured": has_key,
            "api_style": config.get("api_style") or "",
            "model": config.get("model") or "",
            "rate_window_count": rate_counts.get(pid, 0),
            "rate_limited": _rate_tracker.is_rate_limited(pid),
        }
        if state:
            provider_info.update(state.get_stats())
        providers[pid] = provider_info

    return {
        "providers": providers,
        "routing": TASK_ROUTING,
        "task_types": [
            TASK_STRUCTURED,
            TASK_CONVERSATIONAL,
            TASK_COMPLEX,
            TASK_CODE,
            TASK_VERIFICATION,
            TASK_RESEARCH,
            TASK_NARRATIVE,
            TASK_BATCH,
            TASK_CAMPAIGN_PLAN,
            TASK_BUDGET_OPTIMIZE,
            TASK_COMPLIANCE_CHECK,
            TASK_MARKET_ANALYSIS,
            TASK_COMPETITOR_SCAN,
            TASK_TALENT_MAP,
            TASK_CHAT_RESPONSE,
            TASK_ACTION_EXECUTE,
            TASK_CONTEXT_SUMMARIZE,
        ],
        "module_preferences": MODULE_LLM_PREFERENCES,
        "cost_tracking": _cost_tracker.get_daily_spend(),
        **_response_cache.get_stats(),
    }


def get_provider_health() -> Dict[str, Any]:
    """Provider health dashboard -- health scores, availability, and rate status.

    Returns a dict per provider with health_score (0.0-1.0), available (bool),
    rate_limited (bool), circuit_open (bool), and uptime_pct.
    """
    result: Dict[str, Any] = {}
    for pid in PROVIDER_CONFIG:
        config = PROVIDER_CONFIG[pid]
        has_key = bool(os.environ.get(config.get("env_key") or "", "").strip())
        state = _provider_states.get(pid)
        now = time.time()
        health = round(state.get_health_score(), 3) if state else 0.0
        result[pid] = {
            "name": config.get("name") or "",
            "configured": has_key,
            "health_score": health,
            "available": has_key and (state.is_available() if state else False),
            "rate_limited": _rate_tracker.is_rate_limited(pid),
            "circuit_open": (now < state.circuit_open_until) if state else False,
            "uptime_pct": round(health * 100, 1),
        }
    return result


def get_provider_uptime(provider_id: str, hours: int = 24) -> float:
    """Get rolling uptime percentage for a provider.

    Uses the health score as a proxy for availability over the rolling window.

    Args:
        provider_id: LLM provider ID.
        hours: Look-back window (unused, uses health score decay as proxy).

    Returns:
        Uptime percentage (0.0-100.0).
    """
    state = _provider_states.get(provider_id)
    if not state:
        return 0.0
    score = state.get_health_score()
    return round(score * 100, 1)


def get_router_stats() -> Dict[str, Any]:
    """Comprehensive router diagnostics for the admin dashboard.

    Returns current health scores, rate counters, cache hit rate,
    and circuit breaker states for all providers.
    """
    health_scores: Dict[str, float] = {}
    circuit_breakers: Dict[str, bool] = {}
    rate_counts = _rate_tracker.get_counts()
    now = time.time()

    for pid in PROVIDER_CONFIG:
        state = _provider_states.get(pid)
        if state:
            health_scores[pid] = round(state.get_health_score(), 3)
            with state.lock:
                circuit_breakers[pid] = now < state.circuit_open_until
        else:
            health_scores[pid] = 0.0
            circuit_breakers[pid] = False

    cache_stats = _response_cache.get_stats()

    return {
        "health_scores": health_scores,
        "rate_counts": rate_counts,
        "rate_limited": {
            pid: _rate_tracker.is_rate_limited(pid) for pid in PROVIDER_CONFIG
        },
        "circuit_breakers": circuit_breakers,
        "cache": cache_stats,
        "total_providers": len(PROVIDER_CONFIG),
        "available_providers": sum(
            1
            for pid in PROVIDER_CONFIG
            if (
                bool(
                    os.environ.get(
                        PROVIDER_CONFIG[pid].get("env_key") or "", ""
                    ).strip()
                )
                and _provider_states.get(pid)
                and _provider_states[pid].is_available()
                and not _rate_tracker.is_rate_limited(pid)
            )
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CLI DEMO
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("  LLM Router Status")
    print("=" * 60)

    status = get_router_status()
    for pid, info in status["providers"].items():
        configured = "YES" if info.get("configured") else "NO "
        print(f"  [{configured}] {info['name']:<28s} model={info.get('model', 'N/A')}")

    print()
    print("Task routing priorities:")
    for task, providers in status["routing"].items():
        names = [PROVIDER_CONFIG[p]["name"] for p in providers]
        print(f"  {task:<20s} -> {' -> '.join(names)}")

    # Quick test if an argument is provided
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        task = classify_task(query)
        provider = select_provider(task)
        print(f"\nQuery: {query}")
        print(f"Task type: {task}")
        print(f"Selected provider: {provider or 'NONE (no keys configured)'}")

        if provider:
            print(f"\nCalling {PROVIDER_CONFIG[provider]['name']}...")
            result = call_llm(
                messages=[{"role": "user", "content": query}],
                system_prompt="You are a helpful recruitment marketing assistant.",
                task_type=task,
                query_text=query,
            )
            print(f"Provider: {result.get('provider_name', 'N/A')}")
            print(f"Latency: {result.get('latency_ms') or 0:.0f}ms")
            print(f"Response: {result.get('text') or ''[:500]}")
