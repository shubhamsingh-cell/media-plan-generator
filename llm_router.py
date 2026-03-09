"""
llm_router.py -- Smart LLM Provider Router for Nova Chat (v3.1)

Routes LLM API calls to the optimal provider based on task type,
with automatic fallback and circuit breaker per provider.

Provider priority (free-first strategy):
    1. Gemini 2.0 Flash  -- structured data, JSON output, code, general
    2. Groq Llama 3.3 70B -- conversational, complex reasoning
    3. Cerebras Llama 3.3 70B -- hot spare (same model, independent infra)
    4. Claude (Anthropic) -- last resort, highest quality, paid

Task classification:
    - STRUCTURED:  benchmark lookups, CPC/CPA queries, JSON output
    - CONVERSATIONAL: explain strategy, general Q&A, advisory
    - COMPLEX: what-if scenarios, role decomposition, multi-step analysis
    - CODE: formula generation, calculations, data transforms

Each provider has independent circuit breaker (5 failures -> 60s cooldown)
and per-minute rate tracking.

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

# Provider IDs
GEMINI = "gemini"
GROQ = "groq"
CEREBRAS = "cerebras"
CLAUDE = "claude"

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
    CLAUDE: {
        "name": "Claude (Anthropic)",
        "api_style": "anthropic",  # Anthropic-specific format
        "endpoint": "https://api.anthropic.com/v1/messages",
        "model": "claude-sonnet-4-20250514",
        "env_key": "ANTHROPIC_API_KEY",
        "rpm_limit": 50,
        "rpd_limit": 10000,
        "timeout": 45,
        "max_tokens": 4096,
    },
}

# Task -> provider priority order (free-first)
TASK_ROUTING: Dict[str, List[str]] = {
    TASK_STRUCTURED: [GEMINI, GROQ, CEREBRAS, CLAUDE],
    TASK_CONVERSATIONAL: [GROQ, CEREBRAS, GEMINI, CLAUDE],
    TASK_COMPLEX: [GROQ, CEREBRAS, GEMINI, CLAUDE],
    TASK_CODE: [GEMINI, GROQ, CEREBRAS, CLAUDE],
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

    Returns one of: TASK_STRUCTURED, TASK_CONVERSATIONAL, TASK_COMPLEX, TASK_CODE.
    """
    try:
        q = query.lower().strip()
        # Score each task type by keyword matches
        scores = {
            TASK_STRUCTURED: len(_STRUCTURED_KEYWORDS.findall(q)),
            TASK_COMPLEX: len(_COMPLEX_KEYWORDS.findall(q)) * 1.5,  # boost complex
            TASK_CODE: len(_CODE_KEYWORDS.findall(q)),
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


def _build_openai_request(
    provider_id: str,
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    tools: Optional[List[Dict]] = None,
) -> Tuple[str, Dict[str, str], bytes]:
    """Build an OpenAI-compatible API request (Groq, Cerebras)."""
    config = PROVIDER_CONFIG[provider_id]
    api_key = os.environ.get(config["env_key"], "").strip()

    # Build messages with system prompt
    api_messages = []
    if system_prompt:
        api_messages.append({"role": "system", "content": system_prompt})

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, str) and content:
            api_messages.append({"role": role, "content": content})

    payload: Dict[str, Any] = {
        "model": config["model"],
        "messages": api_messages,
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    return config["endpoint"], headers, json.dumps(payload).encode("utf-8")


def _build_anthropic_request(
    messages: List[Dict],
    system_prompt: str,
    max_tokens: int,
    tools: Optional[List[Dict]] = None,
) -> Tuple[str, Dict[str, str], bytes]:
    """Build an Anthropic API request."""
    config = PROVIDER_CONFIG[CLAUDE]
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
    """Parse OpenAI-compatible response to normalized format."""
    try:
        choices = resp_data.get("choices", [])
        if not choices:
            return {"text": "", "error": "No choices in response"}
        message = choices[0].get("message", {})
        text = message.get("content", "") or ""
        usage = resp_data.get("usage", {})
        return {
            "text": text.strip(),
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
            "model": resp_data.get("model", ""),
            "stop_reason": choices[0].get("finish_reason", "stop"),
        }
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
) -> Dict[str, Any]:
    """Route an LLM call to the best available provider.

    Args:
        messages: Conversation messages [{role, content}, ...]
        system_prompt: System prompt string
        max_tokens: Max output tokens
        task_type: Override task classification (or auto-detect from query_text)
        tools: Tool definitions (only supported by Claude provider)
        force_provider: Force a specific provider (skip routing)
        query_text: User query for task classification (if task_type not given)

    Returns:
        {
            "text": "response text",
            "provider": "gemini|groq|cerebras|claude",
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

    # If tools are provided, force Claude (only provider with tool_use support)
    if tools:
        force_provider = CLAUDE

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

    # Smart routing with fallback
    max_attempts = len(PROVIDER_CONFIG)
    for attempt_num in range(max_attempts):
        provider = select_provider(task_type, exclude=excluded)
        if provider is None:
            break

        result = _call_single_provider(
            provider, messages, system_prompt, max_tokens, tools
        )
        attempts.append({
            "provider": provider,
            "status": "success" if result.get("text") else "failed",
            "latency_ms": result.get("latency_ms", 0),
            "error": result.get("error", ""),
        })

        if result.get("text"):
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
) -> Dict[str, Any]:
    """Make a single API call to a specific provider."""
    config = PROVIDER_CONFIG.get(provider_id)
    if not config:
        return {"text": "", "provider": provider_id, "error": "Unknown provider"}

    state = _provider_states.get(provider_id)
    api_style = config.get("api_style", "")
    timeout = config.get("timeout", 30)

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
                messages, system_prompt, max_tokens, tools
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

        # Record success/failure
        if parsed.get("text") or parsed.get("raw_content"):
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
        "task_types": [TASK_STRUCTURED, TASK_CONVERSATIONAL, TASK_COMPLEX, TASK_CODE],
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
