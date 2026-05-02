"""langfuse_integration.py -- stdlib-only fire-and-forget Langfuse tracer.

Lightweight wrapper for the Joveo Nova AI Suite to send LLM traces to a
self-hosted Langfuse instance without adding the heavy ``langfuse`` SDK as
a hard dependency. Uses ``urllib.request`` from the standard library.

Design goals:
    * Zero new pip dependencies (Render image stays slim).
    * Graceful no-op when ``LANGFUSE_*`` env vars are missing.
    * Daemon-thread POST so the LLM hot path is never blocked.
    * Sampling via ``LANGFUSE_SAMPLE_RATE`` (default 1.0).
    * Errors are logged, never raised -- observability must never break prod.

Wiring example (see docs/observability_setup.md for the full snippet):

    from langfuse_integration import trace_llm_call

    result = call_llm(messages=msgs, ...)
    trace_llm_call(
        model=result["model"],
        input_messages=msgs,
        output=result["text"],
        latency_ms=result["latency_ms"],
        cost_usd=None,  # llm_router computes cost separately if needed
        metadata={
            "provider": result["provider"],
            "task_type": result["task_type"],
            "input_tokens": result["input_tokens"],
            "output_tokens": result["output_tokens"],
            "cache_hit": result["cache_hit"],
            "fallback_used": result["fallback_used"],
        },
        user_id=session.get("user_id"),
        session_id=session.get("session_id"),
    )

Required env vars:
    LANGFUSE_HOST           Base URL, e.g. https://obs.joveo.com (no trailing /)
    LANGFUSE_PUBLIC_KEY     Public (pk-) key from Langfuse project settings
    LANGFUSE_SECRET_KEY     Secret (sk-) key from Langfuse project settings

Optional env vars:
    LANGFUSE_SAMPLE_RATE    Float in [0.0, 1.0]. Default 1.0 (trace everything).
    LANGFUSE_TIMEOUT_S      Float seconds for HTTP timeout. Default 5.0.
    LANGFUSE_ENABLED        Set to "false"/"0" to hard-disable even if keys set.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuration -- read once at import; re-read with reload_config() if needed
# ──────────────────────────────────────────────────────────────────────────────

_INGESTION_PATH = "/api/public/ingestion"
_DEFAULT_TIMEOUT_S = 5.0
_DEFAULT_SAMPLE_RATE = 1.0
_USER_AGENT = "joveo-nova-langfuse/1.0 (stdlib)"


class _Config:
    """Lazy, thread-safe config holder. Reads env vars on first access."""

    __slots__ = (
        "host",
        "public_key",
        "secret_key",
        "sample_rate",
        "timeout_s",
        "enabled",
    )

    def __init__(self) -> None:
        self.host: str = ""
        self.public_key: str = ""
        self.secret_key: str = ""
        self.sample_rate: float = _DEFAULT_SAMPLE_RATE
        self.timeout_s: float = _DEFAULT_TIMEOUT_S
        self.enabled: bool = False
        self._load()

    def _load(self) -> None:
        host = (os.environ.get("LANGFUSE_HOST") or "").strip().rstrip("/")
        public_key = (os.environ.get("LANGFUSE_PUBLIC_KEY") or "").strip()
        secret_key = (os.environ.get("LANGFUSE_SECRET_KEY") or "").strip()

        try:
            sample_rate = float(
                os.environ.get("LANGFUSE_SAMPLE_RATE") or _DEFAULT_SAMPLE_RATE
            )
        except (TypeError, ValueError):
            sample_rate = _DEFAULT_SAMPLE_RATE
        sample_rate = max(0.0, min(1.0, sample_rate))

        try:
            timeout_s = float(
                os.environ.get("LANGFUSE_TIMEOUT_S") or _DEFAULT_TIMEOUT_S
            )
        except (TypeError, ValueError):
            timeout_s = _DEFAULT_TIMEOUT_S
        timeout_s = max(0.5, min(60.0, timeout_s))

        kill_switch = (os.environ.get("LANGFUSE_ENABLED") or "true").strip().lower()
        explicitly_disabled = kill_switch in ("false", "0", "no", "off")

        self.host = host
        self.public_key = public_key
        self.secret_key = secret_key
        self.sample_rate = sample_rate
        self.timeout_s = timeout_s
        self.enabled = (
            bool(host and public_key and secret_key) and not explicitly_disabled
        )


_config_lock = threading.Lock()
_config: Optional[_Config] = None


def _get_config() -> _Config:
    """Return the cached config; build it lazily on first call."""
    global _config
    if _config is None:
        with _config_lock:
            if _config is None:
                _config = _Config()
    return _config


def reload_config() -> None:
    """Force a re-read of LANGFUSE_* env vars. Useful for tests + hot reloads."""
    global _config
    with _config_lock:
        _config = _Config()


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


def trace_llm_call(
    *,
    model: str,
    input_messages: List[Dict[str, Any]],
    output: Optional[str],
    latency_ms: float,
    cost_usd: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Fire-and-forget trace to Langfuse. Logs but doesn't raise.

    Args:
        model: Model identifier (e.g. ``"claude-haiku-4-5"``).
        input_messages: OpenAI/Anthropic-style message list.
        output: Generated text. ``None`` if the call errored before producing
            any output.
        latency_ms: Wall-clock latency in milliseconds.
        cost_usd: Optional pre-computed cost (the llm_router already estimates
            this; pass it through if available).
        metadata: Arbitrary tags for filtering inside Langfuse, e.g.
            ``{"provider": "claude", "task_type": "research"}``.
        user_id: Stable user identifier (e.g. Joveo email or hashed user id).
        session_id: Stable conversation/session identifier so multi-turn chats
            group together in the dashboard.
        error: Stringified error message if the call failed.

    Returns:
        ``None`` -- always. Failures are logged at WARNING level.
    """
    cfg = _get_config()
    if not cfg.enabled:
        return None

    # Sampling: skip a fraction of traces to control volume.
    if cfg.sample_rate < 1.0 and random.random() >= cfg.sample_rate:
        return None

    try:
        payload = _build_ingestion_payload(
            model=model,
            input_messages=input_messages,
            output=output,
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            metadata=metadata,
            user_id=user_id,
            session_id=session_id,
            error=error,
        )
    except (TypeError, ValueError) as exc:
        # Building the payload failed (e.g. non-serialisable metadata).
        # Drop the trace silently rather than break the LLM call site.
        logger.warning("Langfuse: failed to build payload: %s", exc, exc_info=False)
        return None

    # Hand off to a daemon thread so the caller returns immediately.
    thread = threading.Thread(
        target=_post_payload,
        args=(cfg, payload),
        name="langfuse-trace",
        daemon=True,
    )
    try:
        thread.start()
    except RuntimeError as exc:
        # Interpreter shutting down, or thread limit reached. Drop and log.
        logger.warning(
            "Langfuse: could not start trace thread: %s", exc, exc_info=False
        )
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────────────


def _utc_iso(timestamp: Optional[float] = None) -> str:
    """Return ISO 8601 UTC timestamp with millisecond precision + Z suffix."""
    ts = time.time() if timestamp is None else timestamp
    millis = int((ts - int(ts)) * 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + f".{millis:03d}Z"


def _safe_json_value(value: Any) -> Any:
    """Coerce arbitrary Python values into JSON-serialisable shapes."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _safe_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_safe_json_value(v) for v in value]
    # Fallback: stringify anything else (datetime, pathlib, etc.).
    return repr(value)


def _build_ingestion_payload(
    *,
    model: str,
    input_messages: List[Dict[str, Any]],
    output: Optional[str],
    latency_ms: float,
    cost_usd: Optional[float],
    metadata: Optional[Dict[str, Any]],
    user_id: Optional[str],
    session_id: Optional[str],
    error: Optional[str],
) -> Dict[str, Any]:
    """Build the Langfuse public-ingestion payload (trace + generation)."""
    now_ts = time.time()
    end_iso = _utc_iso(now_ts)
    start_iso = _utc_iso(now_ts - max(0.0, latency_ms) / 1000.0)

    trace_id = uuid.uuid4().hex
    generation_id = uuid.uuid4().hex
    safe_metadata = _safe_json_value(metadata) if metadata else {}
    if not isinstance(safe_metadata, dict):
        safe_metadata = {"raw": safe_metadata}

    # Derive a stable trace name for filtering in the dashboard.
    trace_name = "nova.llm.call"
    if isinstance(safe_metadata, dict):
        provider = safe_metadata.get("provider")
        task_type = safe_metadata.get("task_type")
        if provider or task_type:
            trace_name = f"nova.llm.{task_type or 'call'}"

    trace_event = {
        "id": uuid.uuid4().hex,
        "type": "trace-create",
        "timestamp": end_iso,
        "body": {
            "id": trace_id,
            "name": trace_name,
            "userId": user_id,
            "sessionId": session_id,
            "metadata": safe_metadata,
            "tags": ["nova-ai-suite", "stdlib-tracer"],
            "release": os.environ.get("RENDER_GIT_COMMIT")
            or os.environ.get("GIT_SHA")
            or "",
        },
    }

    generation_body: Dict[str, Any] = {
        "id": generation_id,
        "traceId": trace_id,
        "name": "nova.generation",
        "model": model,
        "startTime": start_iso,
        "endTime": end_iso,
        "input": _safe_json_value(input_messages or []),
        "metadata": safe_metadata,
    }

    if output is not None:
        generation_body["output"] = output

    if cost_usd is not None:
        generation_body["usageDetails"] = {"total": float(cost_usd)}

    # Pull token counts out of metadata if present so Langfuse's native
    # cost/usage analytics light up.
    if isinstance(safe_metadata, dict):
        usage: Dict[str, Any] = {}
        if "input_tokens" in safe_metadata:
            usage["input"] = safe_metadata["input_tokens"]
        if "output_tokens" in safe_metadata:
            usage["output"] = safe_metadata["output_tokens"]
        if usage:
            usage.setdefault("unit", "TOKENS")
            generation_body["usage"] = usage

    if error:
        generation_body["level"] = "ERROR"
        generation_body["statusMessage"] = error

    generation_event = {
        "id": uuid.uuid4().hex,
        "type": "generation-create",
        "timestamp": end_iso,
        "body": generation_body,
    }

    return {"batch": [trace_event, generation_event]}


def _basic_auth_header(public_key: str, secret_key: str) -> str:
    """Build the HTTP ``Authorization: Basic ...`` header value."""
    raw = f"{public_key}:{secret_key}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _post_payload(cfg: _Config, payload: Dict[str, Any]) -> None:
    """POST the ingestion payload. Runs inside a daemon thread."""
    url = f"{cfg.host}{_INGESTION_PATH}"
    try:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    except (TypeError, ValueError) as exc:
        logger.warning("Langfuse: payload not serialisable: %s", exc, exc_info=False)
        return

    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": _basic_auth_header(cfg.public_key, cfg.secret_key),
            "User-Agent": _USER_AGENT,
            "X-Langfuse-Sdk-Name": "joveo-stdlib",
            "X-Langfuse-Sdk-Version": "1.0.0",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=cfg.timeout_s) as response:
            status = getattr(response, "status", 0) or response.getcode()
            if status >= 400:
                # Read a small slice of the body for diagnostics, then move on.
                snippet = ""
                try:
                    snippet = response.read(512).decode("utf-8", errors="replace")
                except OSError:
                    snippet = ""
                logger.warning(
                    "Langfuse: ingestion HTTP %s -- %s", status, snippet[:200]
                )
    except urllib.error.HTTPError as exc:
        logger.warning(
            "Langfuse: HTTPError %s on POST %s -- %s",
            exc.code,
            url,
            str(exc)[:200],
            exc_info=False,
        )
    except urllib.error.URLError as exc:
        logger.warning(
            "Langfuse: URLError on POST %s -- %s",
            url,
            str(exc.reason)[:200],
            exc_info=False,
        )
    except (TimeoutError, OSError) as exc:
        logger.warning(
            "Langfuse: network error on POST %s -- %s",
            url,
            str(exc)[:200],
            exc_info=False,
        )
    except (
        Exception
    ) as exc:  # noqa: BLE001 -- belt-and-suspenders, observability must never raise
        logger.warning(
            "Langfuse: unexpected error on POST %s -- %s",
            url,
            repr(exc)[:200],
            exc_info=False,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Optional helpers (used by tests, also handy for ad-hoc smoke checks)
# ──────────────────────────────────────────────────────────────────────────────


def is_enabled() -> bool:
    """Return True if env vars are present and the integration will trace."""
    return _get_config().enabled


def health_fingerprint() -> str:
    """Stable, non-sensitive fingerprint of the active config (for /health).

    Returns the SHA-256 prefix of ``host|public_key`` so the user can verify
    they're pointing at the right instance without leaking the secret key.
    """
    cfg = _get_config()
    if not cfg.enabled:
        return "disabled"
    raw = f"{cfg.host}|{cfg.public_key}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:12]


__all__ = [
    "trace_llm_call",
    "reload_config",
    "is_enabled",
    "health_fingerprint",
]
