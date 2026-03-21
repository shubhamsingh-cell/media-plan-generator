"""
llm_router.py -- Smart LLM Provider Router for Nova Chat (v3.3)

Routes LLM API calls to the optimal provider based on task type,
with automatic fallback and circuit breaker per provider.

Provider priority (free-first, then paid by cost-efficiency):
    FREE TIER:
    1. Gemini 2.0 Flash  -- free, structured data, JSON output, code
    2. Groq Llama 3.3 70B -- free, conversational, complex reasoning
    3. Cerebras Llama 3.3 70B -- free, hot spare (same model, independent infra)
    4. Mistral Small -- free tier, strong JSON + multilingual
    5. OpenRouter (Llama 4 Maverick) -- free models via single gateway
    6. xAI Grok -- free signup credits ($25), strong reasoning
    7. SambaNova (Llama 3.1 405B) -- free, largest open model, fastest inference (RDU)
    8. NVIDIA NIM (Llama 3.1 70B) -- free dev program, NVIDIA-optimized inference
    9. Cloudflare Workers AI (Llama 3.3 70B) -- free 10K neurons/day, edge-distributed

    PAID TIER:
    10. GPT-4o (OpenAI) -- paid, strong at structured + conversational + reasoning
    11. Claude Sonnet (Anthropic) -- paid, high quality, strong tool_use
    12. Claude Opus (Anthropic) -- paid, last resort, highest quality, most expensive

Routing strategy for paid models:
    - STRUCTURED (JSON/benchmarks): GPT-4o before Claude (excellent JSON adherence)
    - CONVERSATIONAL (Q&A): GPT-4o before Claude (strong general reasoning)
    - COMPLEX (multi-step): Claude Sonnet before GPT-4o (better tool_use chains)
    - CODE (formulas): GPT-4o before Claude (strong at calculations)
    - VERIFICATION (fact-check): GPT-4o before Claude (precision + grounding)
    - RESEARCH (geopolitical): GPT-4o before Claude (broad knowledge)
    - NARRATIVE (long-form): GPT-4o before Claude (fluent generation)
    - BATCH (high-throughput): GPT-4o before Claude (cost-efficient at scale)
    - Claude Opus is ALWAYS last -- only used when all others fail

Task classification (8 types):
    - STRUCTURED:     benchmark lookups, CPC/CPA queries, JSON output
    - CONVERSATIONAL: explain strategy, general Q&A, advisory
    - COMPLEX:        what-if scenarios, role decomposition, multi-step analysis
    - CODE:           formula generation, calculations, data transforms
    - VERIFICATION:   fact-checking, grounding verification, accuracy validation
    - RESEARCH:       market research, geopolitical analysis, macro-economic outlook
    - NARRATIVE:      long-form text, executive summaries, report writing
    - BATCH:          high-throughput bulk operations, comprehensive reports

Each provider has independent circuit breaker (5 failures -> 60s cooldown)
and per-minute rate tracking.  13 total providers, 9 free + 4 paid.

Stdlib-only, thread-safe.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# PROVIDER CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# Task types for routing
TASK_STRUCTURED = "structured"
TASK_CONVERSATIONAL = "conversational"
TASK_COMPLEX = "complex"
TASK_CODE = "code"
TASK_VERIFICATION = "verification"    # Fact-checking, grounding verification
TASK_RESEARCH = "research"            # Market research, geopolitical analysis
TASK_NARRATIVE = "narrative"          # Long-form text, executive summaries
TASK_BATCH = "batch"                  # High-throughput, latency-tolerant

# Provider IDs
GEMINI = "gemini"
GROQ = "groq"
CEREBRAS = "cerebras"
MISTRAL = "mistral"
OPENROUTER = "openrouter"
XAI = "xai"
SAMBANOVA = "sambanova"
NVIDIA_NIM = "nvidia_nim"
CLOUDFLARE = "cloudflare"
GPT4O = "gpt4o"
CLAUDE_HAIKU = "claude_haiku"
CLAUDE = "claude"
CLAUDE_OPUS = "claude_opus"

# Global timeout budget: max total wall-clock seconds for the entire call_llm()
# fallback loop.  Individual per-provider timeouts are dynamically capped to the
# remaining budget so the caller never waits longer than this.
GLOBAL_TIMEOUT_BUDGET = 60.0           # seconds
_MIN_REMAINING_BUDGET = 5.0            # don't start a new attempt with < 5s left

# Provider configs: endpoint, model, auth header, rate limits
PROVIDER_CONFIG: Dict[str, Dict[str, Any]] = {
    GEMINI: {
        "name": "Gemini 2.0 Flash",
        "api_style": "gemini",  # Google-specific format
        "endpoint": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent",
        "model": "gemini-2.0-flash",
        "env_key": "GEMINI_API_KEY",
        "rpm_limit": 15,
        "rpd_limit": 1500,
        "timeout": 30,
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
        "rpd_limit": 14400,
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
        "model": "grok-2-latest",
        "env_key": "XAI_API_KEY",
        "rpm_limit": 30,
        "rpd_limit": 14400,
        "timeout": 30,
        "max_tokens": 8192,
    },
    SAMBANOVA: {
        "name": "SambaNova (Llama 3.1 405B)",
        "api_style": "openai",  # OpenAI-compatible
        "endpoint": "https://api.sambanova.ai/v1/chat/completions",
        "model": "Meta-Llama-3.1-405B-Instruct",
        "env_key": "SAMBANOVA_API_KEY",
        "rpm_limit": 10,  # 405B model has 10 RPM on free tier
        "rpd_limit": 1000,
        "timeout": 45,  # 405B is larger, allow more time
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
    GPT4O: {
        "name": "GPT-4o (OpenAI)",
        "api_style": "openai",  # OpenAI native format
        "endpoint": "https://api.openai.com/v1/chat/completions",
        "model": "gpt-4o",
        "env_key": "OPENAI_API_KEY",
        "rpm_limit": 60,
        "rpd_limit": 10000,
        "timeout": 45,
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
        "timeout": 45,
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
        "timeout": 90,  # Opus is slower but more thorough
        "max_tokens": 4096,
    },
}

# Task -> provider priority order
# Strategy: 9 free providers first, then paid by cost-efficiency, Opus absolute last
#
# Free tier strengths:
#   Gemini: structured JSON, code, verification
#   Groq/Cerebras (Llama 3.3 70B): conversational, complex reasoning
#   Mistral Small: structured JSON, multilingual, code
#   OpenRouter (Llama 4 Maverick): strong general purpose (free models)
#   xAI Grok: strong reasoning (free $25 signup credits)
#   SambaNova (Llama 3.1 405B): largest open model, fastest inference (RDU hardware)
#   NVIDIA NIM (Llama 3.1 70B): NVIDIA-optimized inference, diverse model catalog
#   Cloudflare Workers AI (Llama 3.3 70B): edge-distributed, low latency, 10K neurons/day
#
# Paid tier strengths (cost order: Haiku << GPT-4o < Sonnet < Opus):
#   Claude Haiku: fast + cheap paid fallback, good for simple tasks
#   GPT-4o: structured JSON, general reasoning, calculations
#   Claude Sonnet: complex multi-step tool_use chains
#   Claude Opus 4.6: last resort, highest quality
TASK_ROUTING: Dict[str, List[str]] = {
    TASK_STRUCTURED:     [GEMINI, MISTRAL, NVIDIA_NIM, GROQ, CEREBRAS, OPENROUTER, XAI, SAMBANOVA, CLOUDFLARE, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
    TASK_CONVERSATIONAL: [GROQ, CEREBRAS, GEMINI, MISTRAL, OPENROUTER, XAI, SAMBANOVA, NVIDIA_NIM, CLOUDFLARE, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
    TASK_COMPLEX:        [SAMBANOVA, OPENROUTER, GROQ, CEREBRAS, GEMINI, MISTRAL, XAI, NVIDIA_NIM, CLOUDFLARE, CLAUDE_HAIKU, CLAUDE, GPT4O, CLAUDE_OPUS],
    TASK_CODE:           [GEMINI, MISTRAL, NVIDIA_NIM, GROQ, CEREBRAS, OPENROUTER, XAI, SAMBANOVA, CLOUDFLARE, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
    TASK_VERIFICATION:   [GEMINI, MISTRAL, GROQ, CEREBRAS, NVIDIA_NIM, OPENROUTER, XAI, SAMBANOVA, CLOUDFLARE, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
    TASK_RESEARCH:       [XAI, OPENROUTER, SAMBANOVA, GEMINI, GROQ, CEREBRAS, MISTRAL, NVIDIA_NIM, CLOUDFLARE, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
    TASK_NARRATIVE:      [GROQ, OPENROUTER, GEMINI, CEREBRAS, MISTRAL, XAI, SAMBANOVA, NVIDIA_NIM, CLOUDFLARE, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
    TASK_BATCH:          [CLOUDFLARE, CEREBRAS, GROQ, GEMINI, MISTRAL, NVIDIA_NIM, OPENROUTER, XAI, SAMBANOVA, CLAUDE_HAIKU, GPT4O, CLAUDE, CLAUDE_OPUS],
}

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
    r"macro|economic.outlook|political|immigration|policy)\b",
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


# ═══════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER + RATE TRACKER (per provider)
# ═══════════════════════════════════════════════════════════════════════════════

class _ProviderState:
    """Thread-safe state tracker for a single LLM provider."""

    def __init__(self, provider_id: str):
        self.provider_id = provider_id
        self.lock = threading.RLock()
        # Circuit breaker
        self.consecutive_failures = 0
        self.circuit_open_until = 0.0  # timestamp
        self.circuit_threshold = 5
        self.circuit_cooldown = 60.0  # seconds
        # Rate tracking
        self.minute_calls: List[float] = []  # timestamps
        self.day_calls: List[float] = []  # timestamps
        # Stats
        self.total_calls = 0
        self.total_failures = 0
        self.total_latency_ms = 0.0

    def is_available(self) -> bool:
        """Check if provider is available (circuit not open, rate not exceeded)."""
        now = time.time()
        with self.lock:
            # Circuit breaker
            if now < self.circuit_open_until:
                return False
            # Half-open recovery: reset counter so a single failure doesn't
            # immediately re-open the circuit after cooldown expires.
            if self.consecutive_failures >= self.circuit_threshold:
                self.consecutive_failures = self.circuit_threshold - 1
            # Rate limiting
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
        """Record a successful API call."""
        with self.lock:
            self.consecutive_failures = 0
            self.total_latency_ms += latency_ms

    def record_failure(self) -> None:
        """Record a failed API call and potentially open circuit."""
        with self.lock:
            self.consecutive_failures += 1
            self.total_failures += 1
            if self.consecutive_failures >= self.circuit_threshold:
                self.circuit_open_until = time.time() + self.circuit_cooldown
                logger.warning(
                    "LLM Router: Circuit breaker OPEN for %s (cooldown %.0fs)",
                    self.provider_id, self.circuit_cooldown,
                )
                # Alert via email when circuit breaker opens
                try:
                    from email_alerts import send_circuit_breaker_alert
                    send_circuit_breaker_alert(self.provider_id, self.consecutive_failures)
                except Exception:
                    pass  # email alerts are best-effort

    def get_stats(self) -> Dict[str, Any]:
        """Get provider stats."""
        now = time.time()
        with self.lock:
            self.minute_calls = [t for t in self.minute_calls if now - t < 60]
            self.day_calls = [t for t in self.day_calls if now - t < 86400]
            avg_latency = (
                self.total_latency_ms / max(1, self.total_calls - self.total_failures)
            )
            return {
                "provider": self.provider_id,
                "name": PROVIDER_CONFIG.get(self.provider_id, {}).get("name", ""),
                "total_calls": self.total_calls,
                "total_failures": self.total_failures,
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


# ═══════════════════════════════════════════════════════════════════════════════
# TASK CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

def classify_task(query: str) -> str:
    """Classify a user query into a task type for provider routing.

    Returns one of: TASK_STRUCTURED, TASK_CONVERSATIONAL, TASK_COMPLEX,
    TASK_CODE, TASK_VERIFICATION, TASK_RESEARCH, TASK_NARRATIVE, TASK_BATCH.
    """
    try:
        q = query.lower().strip()
        # Score each task type by keyword matches.
        # New specialised types are checked alongside the originals;
        # specificity is handled via the boost multipliers.
        scores = {
            TASK_VERIFICATION: len(_VERIFICATION_KEYWORDS.findall(q)) * 2.0,  # boost: most specific
            TASK_RESEARCH:     len(_RESEARCH_KEYWORDS.findall(q)) * 2.0,
            TASK_NARRATIVE:    len(_NARRATIVE_KEYWORDS.findall(q)) * 1.8,
            TASK_BATCH:        len(_BATCH_KEYWORDS.findall(q)) * 1.8,
            TASK_STRUCTURED:   len(_STRUCTURED_KEYWORDS.findall(q)),
            TASK_COMPLEX:      len(_COMPLEX_KEYWORDS.findall(q)) * 1.5,  # boost complex
            TASK_CODE:         len(_CODE_KEYWORDS.findall(q)),
            TASK_CONVERSATIONAL: 0,
        }
        best = max(scores, key=scores.get)
        if scores[best] == 0:
            return TASK_CONVERSATIONAL  # default
        return best
    except Exception:
        return TASK_CONVERSATIONAL


def select_provider(task_type: str, exclude: Optional[List[str]] = None) -> Optional[str]:
    """Select the best available provider for a task type.

    Follows the priority order for the task type, skipping providers
    that are unavailable (circuit open or rate-limited) or excluded.

    Returns provider ID or None if all providers are unavailable.
    """
    exclude = exclude or []
    priority = TASK_ROUTING.get(task_type, TASK_ROUTING[TASK_CONVERSATIONAL])

    for pid in priority:
        if pid in exclude:
            continue
        # Check API key exists
        config = PROVIDER_CONFIG.get(pid, {})
        env_key = config.get("env_key", "")
        if not os.environ.get(env_key, "").strip():
            continue
        # Check availability
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
) -> Tuple[str, Dict[str, str], bytes]:
    """Build a Gemini API request."""
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    config = PROVIDER_CONFIG[GEMINI]
    url = f"{config['endpoint']}?key={api_key}"

    # Convert messages to Gemini format
    contents = []
    for msg in messages:
        role = "model" if msg["role"] == "assistant" else "user"
        text = msg.get("content", "")
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
        name = tool.get("name", "")
        if not name:
            continue
        fn: Dict[str, Any] = {
            "name": name,
            "description": tool.get("description", ""),
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
    """Build an OpenAI-compatible API request (Groq, Cerebras, Mistral, xAI, OpenRouter, SambaNova, NVIDIA NIM, Cloudflare)."""
    config = PROVIDER_CONFIG[provider_id]
    api_key = os.environ.get(config["env_key"], "").strip()

    # Build messages with system prompt
    api_messages = []
    if system_prompt:
        api_messages.append({"role": "system", "content": system_prompt})

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")

        # Pass through tool result messages as-is (role="tool")
        if role == "tool":
            api_messages.append({
                "role": "tool",
                "tool_call_id": msg.get("tool_call_id", ""),
                "content": str(msg.get("content", "")),
            })
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
        content = msg.get("content", "")
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


def _parse_gemini_response(resp_data: Dict) -> Dict[str, Any]:
    """Parse Gemini API response to normalized format."""
    try:
        candidates = resp_data.get("candidates", [])
        if not candidates:
            return {"text": "", "error": "No candidates in response"}
        content = candidates[0].get("content", {})
        parts = content.get("parts", [])
        text = " ".join(p.get("text", "") for p in parts if "text" in p)
        usage = resp_data.get("usageMetadata", {})
        return {
            "text": text.strip(),
            "input_tokens": usage.get("promptTokenCount", 0),
            "output_tokens": usage.get("candidatesTokenCount", 0),
            "model": "gemini-2.0-flash",
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
        choices = resp_data.get("choices", [])
        if not choices:
            return {"text": "", "error": "No choices in response"}
        message = choices[0].get("message", {})
        text = message.get("content", "") or ""
        usage = resp_data.get("usage", {})

        result: Dict[str, Any] = {
            "text": text.strip(),
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
            "model": resp_data.get("model", ""),
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
        content_blocks = resp_data.get("content", [])
        text_parts = []
        for block in content_blocks:
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
        usage = resp_data.get("usage", {})
        return {
            "text": " ".join(text_parts).strip(),
            "input_tokens": usage.get("input_tokens", 0),
            "output_tokens": usage.get("output_tokens", 0),
            "model": resp_data.get("model", ""),
            "stop_reason": resp_data.get("stop_reason", "end_turn"),
            # Preserve raw for tool_use compatibility
            "raw_content": content_blocks,
            "raw_stop_reason": resp_data.get("stop_reason", ""),
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
) -> Dict[str, Any]:
    """Route an LLM call to the best available provider.

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

    Returns:
        {
            "text": "response text",
            "provider": "gemini|groq|cerebras|claude|claude_opus",
            "provider_name": "Gemini 2.0 Flash",
            "model": "gemini-2.0-flash",
            "task_type": "conversational",
            "input_tokens": 100,
            "output_tokens": 200,
            "latency_ms": 450,
            "fallback_used": False,
            "attempts": [{"provider": "gemini", "status": "success", "latency_ms": 450}],
        }
    """
    # Classify task if not provided
    if not task_type and query_text:
        task_type = classify_task(query_text)
    task_type = task_type or TASK_CONVERSATIONAL

    # Tools are now supported by all OpenAI-compatible providers (auto-converted
    # from Anthropic format in _build_openai_request).  No longer force Claude.

    attempts: List[Dict[str, Any]] = []
    excluded: List[str] = []

    # Force-provider mode
    if force_provider:
        result = _call_single_provider(
            force_provider, messages, system_prompt, max_tokens, tools
        )
        result["task_type"] = task_type
        result["fallback_used"] = False
        result["attempts"] = [
            {"provider": force_provider, "status": "success" if result.get("text") else "failed",
             "latency_ms": result.get("latency_ms", 0)}
        ]
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
                GLOBAL_TIMEOUT_BUDGET, attempt_num,
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
            # Use custom routing order
            provider = None
            for pid in custom_route:
                if pid in excluded:
                    continue
                config = PROVIDER_CONFIG.get(pid, {})
                env_key = config.get("env_key", "")
                if not os.environ.get(env_key, "").strip():
                    continue
                state = _provider_states.get(pid)
                if state and state.is_available():
                    provider = pid
                    break
        else:
            provider = select_provider(task_type, exclude=excluded)
        if provider is None:
            break

        result = _call_single_provider(
            provider, messages, system_prompt, max_tokens, tools,
            timeout_override=remaining,
        )
        _has_response = bool(
            result.get("text") or result.get("raw_content") or result.get("tool_calls")
        )
        attempts.append({
            "provider": provider,
            "status": "success" if _has_response else "failed",
            "latency_ms": result.get("latency_ms", 0),
            "error": result.get("error", ""),
        })

        if _has_response:
            result["task_type"] = task_type
            result["fallback_used"] = attempt_num > 0
            result["attempts"] = attempts
            return result

        # Failed -- exclude and try next
        excluded.append(provider)
        logger.warning(
            "LLM Router: %s failed (attempt %d), trying next provider. Error: %s",
            provider, attempt_num + 1, result.get("error", "unknown"),
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
        "attempts": attempts,
        "error": "All LLM providers unavailable or failed",
    }


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
    api_style = config.get("api_style", "")
    timeout = config.get("timeout", 30)
    if timeout_override is not None:
        timeout = min(timeout, timeout_override)

    try:
        # Build request based on API style
        if api_style == "gemini":
            url, headers, body = _build_gemini_request(
                messages, system_prompt, max_tokens, tools
            )
        elif api_style == "openai":
            url, headers, body = _build_openai_request(
                provider_id, messages, system_prompt, max_tokens, tools
            )
        elif api_style == "anthropic":
            url, headers, body = _build_anthropic_request(
                messages, system_prompt, max_tokens, tools, provider_id=provider_id
            )
        else:
            return {"text": "", "provider": provider_id, "error": f"Unknown API style: {api_style}"}

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
        else:
            parsed = {"text": "", "error": "Unknown parse style"}

        # Record success/failure (tool_calls count as success even without text)
        if parsed.get("text") or parsed.get("raw_content") or parsed.get("tool_calls"):
            if state:
                state.record_success(latency_ms)
            logger.info(
                "LLM Router: %s responded in %.0fms (in=%d, out=%d)",
                provider_id, latency_ms,
                parsed.get("input_tokens", 0), parsed.get("output_tokens", 0),
            )
        else:
            if state:
                state.record_failure()

        parsed["provider"] = provider_id
        parsed["provider_name"] = config.get("name", "")
        parsed["latency_ms"] = latency_ms
        return parsed

    except urllib.error.HTTPError as http_err:
        if state:
            state.record_failure()
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8")[:500]
        except Exception:
            pass
        logger.error(
            "LLM Router: %s HTTP %d: %s", provider_id, http_err.code, error_body[:200]
        )
        return {
            "text": "",
            "provider": provider_id,
            "provider_name": config.get("name", ""),
            "error": f"HTTP {http_err.code}: {error_body[:200]}",
            "latency_ms": 0,
        }
    except Exception as exc:
        if state:
            state.record_failure()
        logger.error("LLM Router: %s error: %s", provider_id, exc)
        return {
            "text": "",
            "provider": provider_id,
            "provider_name": config.get("name", ""),
            "error": str(exc),
            "latency_ms": 0,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS & DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════════════════

def get_router_status() -> Dict[str, Any]:
    """Return status of all providers and routing configuration."""
    providers = {}
    for pid in PROVIDER_CONFIG:
        config = PROVIDER_CONFIG[pid]
        state = _provider_states.get(pid)
        has_key = bool(os.environ.get(config.get("env_key", ""), "").strip())
        providers[pid] = {
            "name": config.get("name", ""),
            "configured": has_key,
            "api_style": config.get("api_style", ""),
            "model": config.get("model", ""),
            **(state.get_stats() if state else {}),
        }

    return {
        "providers": providers,
        "routing": TASK_ROUTING,
        "task_types": [TASK_STRUCTURED, TASK_CONVERSATIONAL, TASK_COMPLEX, TASK_CODE,
                       TASK_VERIFICATION, TASK_RESEARCH, TASK_NARRATIVE, TASK_BATCH],
    }


def get_provider_health() -> Dict[str, bool]:
    """Quick health check -- which providers are available right now?"""
    result = {}
    for pid in PROVIDER_CONFIG:
        config = PROVIDER_CONFIG[pid]
        has_key = bool(os.environ.get(config.get("env_key", ""), "").strip())
        state = _provider_states.get(pid)
        available = has_key and (state.is_available() if state else False)
        result[pid] = available
    return result


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
            print(f"Latency: {result.get('latency_ms', 0):.0f}ms")
            print(f"Response: {result.get('text', '')[:500]}")
