"""Thin Stagehand v3 wrapper for Joveo Nova AI Suite (S50 -- May 2026).

Stagehand v3 (Browserbase, Feb 2026 rewrite) is a higher-level layer that
sits on top of Playwright and adds three AI-native primitives:

    * ``act``     -- natural-language interaction (click, type, select,
                     scroll) with built-in self-healing selectors.
    * ``extract`` -- structured extraction against a JSON schema.
    * ``observe`` -- enumerate the DOM elements / actions that the agent
                     considers candidates for the next step.

Joveo already runs Playwright via the Playwright MCP server; this wrapper
gives server-side Python code (Nova chatbot tools, CG Automation, audits)
a thin RPC client so we do not have to ship the full Node.js Stagehand
SDK inside our stdlib-only Python services.

Design contract (mirrors recruitment_apis.py):
    * stdlib only -- urllib.request, threading, json, logging.
    * Every public function returns
      ``{"data": Any, "source": "stagehand", "elapsed_ms": int,
        "error": str | None}``.
    * No bare ``except:`` -- only the explicit ``_NET_ERRORS`` tuple.
    * Type hints on every signature (3.9-compatible via
      ``from __future__ import annotations``).
    * Feature flag: ``STAGEHAND_ENABLED`` env var (default ``false``).
    * Endpoint:    ``STAGEHAND_API_URL`` env var
                   (Stagehand has *no* stable public hosted REST API as
                   of May 2026, so this wrapper assumes a self-hosted
                   Stagehand server. When the env var is absent, all
                   functions return an explicit "integration gap" error.)
    * Auth:        ``STAGEHAND_API_KEY`` env var
                   (forwarded as ``X-API-Key`` header; also accepts
                   ``BROWSERBASE_API_KEY`` for hosted Browserbase
                   deployments where the same key is reused).

Action caching: Stagehand v3 supports server-side action caching so that
identical prompts on stable pages reuse a previously-resolved selector
(claimed 44 percent latency reduction). We expose this via the
``cache_actions`` parameter on ``stagehand_act``; the parameter is
forwarded as part of the JSON-RPC payload so the *server* makes the
caching decision (this client stays stateless).

INTEGRATION NOTE: As of May 2026 Stagehand publishes only a Node.js SDK
(``@browserbasehq/stagehand``) plus a JSON-RPC server you can self-host.
There is no documented public hosted REST endpoint. Until Browserbase
publishes one, every public function in this module short-circuits with
a clear "Stagehand wrapper requires self-hosted endpoint at
STAGEHAND_API_URL" error when the endpoint env var is missing. This is
intentional: callers get a deterministic, debuggable failure mode rather
than silent fallbacks.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


# ─── Configuration ─────────────────────────────────────────────────────────────

_DEFAULT_USER_AGENT = "Joveo-Nova-Stagehand-Wrapper/1.0"
_STAGEHAND_SOURCE = "stagehand"

# Standard exception tuple captured by every public function. Bare ``except:``
# is forbidden by project rules; this tuple covers realistic urllib + json
# failure modes without masking programming errors.
_NET_ERRORS = (
    urllib.error.HTTPError,
    urllib.error.URLError,
    TimeoutError,
    json.JSONDecodeError,
    ValueError,
    OSError,
)

# Feature-flag values that should be interpreted as "enabled". Anything else
# (including unset, empty string, "0", "no") leaves the wrapper disabled.
_TRUTHY = frozenset({"1", "true", "yes", "on", "enabled"})

_ENV_LOCK = threading.Lock()


def _is_enabled() -> bool:
    """Return True iff ``STAGEHAND_ENABLED`` is set to a truthy value."""
    raw = os.environ.get("STAGEHAND_ENABLED") or ""
    return raw.strip().lower() in _TRUTHY


def _get_endpoint() -> str | None:
    """Return the configured Stagehand JSON-RPC base URL or None.

    The endpoint must be HTTP/HTTPS and is normalized to drop a trailing
    slash so callers can safely concatenate ``/v1/act`` etc.
    """
    with _ENV_LOCK:
        raw = (os.environ.get("STAGEHAND_API_URL") or "").strip()
    if not raw:
        return None
    if not (raw.startswith("http://") or raw.startswith("https://")):
        logger.error(
            "stagehand_wrapper: STAGEHAND_API_URL must be http(s); got %r",
            raw,
        )
        return None
    return raw.rstrip("/")


def _get_api_key(explicit: str | None) -> str | None:
    """Resolve the Stagehand API key.

    Precedence: explicit argument -> ``STAGEHAND_API_KEY`` ->
    ``BROWSERBASE_API_KEY``. Empty strings are treated as absent.
    """
    if explicit:
        return explicit
    with _ENV_LOCK:
        for env_var in ("STAGEHAND_API_KEY", "BROWSERBASE_API_KEY"):
            value = (os.environ.get(env_var) or "").strip()
            if value:
                return value
    return None


def _err(message: str, started_ms: float) -> dict:
    """Build a normalized error response."""
    elapsed = int((time.monotonic() - started_ms) * 1000)
    return {
        "data": None,
        "source": _STAGEHAND_SOURCE,
        "elapsed_ms": elapsed,
        "error": message,
    }


def _ok(data: Any, started_ms: float) -> dict:
    """Build a normalized success response."""
    elapsed = int((time.monotonic() - started_ms) * 1000)
    return {
        "data": data,
        "source": _STAGEHAND_SOURCE,
        "elapsed_ms": elapsed,
        "error": None,
    }


def _post_json(
    path: str,
    payload: dict,
    api_key: str | None,
    timeout: int,
) -> dict:
    """POST JSON to ``<endpoint><path>`` and return parsed JSON.

    Raises ``ValueError`` (caught upstream as a network error) for HTTP
    error codes and JSON decode failures so the public wrappers can use a
    single ``_NET_ERRORS`` catch.
    """
    base = _get_endpoint()
    if not base:
        raise ValueError(
            "Stagehand wrapper requires self-hosted endpoint at " "STAGEHAND_API_URL"
        )

    url = f"{base}{path}"
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": _DEFAULT_USER_AGENT,
    }
    if api_key:
        headers["X-API-Key"] = api_key

    request = urllib.request.Request(
        url,
        data=body,
        headers=headers,
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        status = getattr(response, "status", 200)
        raw = response.read().decode("utf-8", errors="replace")

    if status >= 400:
        raise ValueError(f"Stagehand HTTP {status}: {raw[:200]}")

    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Stagehand returned non-object payload (type={type(parsed).__name__})"
        )
    return parsed


def _check_preconditions(
    url: str,
    started_ms: float,
) -> dict | None:
    """Return an error dict if the wrapper cannot proceed, else None."""
    if not _is_enabled():
        return _err(
            "Stagehand wrapper disabled (set STAGEHAND_ENABLED=true to enable)",
            started_ms,
        )

    if not isinstance(url, str) or not url.strip():
        return _err("url must be a non-empty string", started_ms)

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return _err(
            f"url must be http(s); got scheme={parsed.scheme!r}",
            started_ms,
        )

    if not _get_endpoint():
        return _err(
            "Stagehand wrapper requires self-hosted endpoint at " "STAGEHAND_API_URL",
            started_ms,
        )

    return None


# ─── Public API ────────────────────────────────────────────────────────────────


def stagehand_act(
    url: str,
    instruction: str,
    *,
    api_key: str | None = None,
    timeout: int = 60,
    cache_actions: bool = True,
) -> dict:
    """Perform a natural-language action on a page via Stagehand ``act``.

    Args:
        url: Page to load before executing the instruction.
        instruction: Plain-English action, e.g. ``"click the search button"``.
        api_key: Override for ``STAGEHAND_API_KEY`` /
            ``BROWSERBASE_API_KEY`` env vars.
        timeout: Per-request HTTP timeout in seconds.
        cache_actions: When True, request that Stagehand reuse cached
            selectors for identical instructions on stable pages
            (Stagehand v3 server-side action caching).

    Returns:
        ``{"data": <server-response>, "source": "stagehand",
          "elapsed_ms": int, "error": str | None}``.
        On any failure (disabled flag, missing endpoint, HTTP error,
        timeout) ``data`` is None and ``error`` carries a human-readable
        message.
    """
    started = time.monotonic()

    pre = _check_preconditions(url, started)
    if pre is not None:
        return pre

    if not isinstance(instruction, str) or not instruction.strip():
        return _err("instruction must be a non-empty string", started)

    payload = {
        "url": url,
        "instruction": instruction,
        "cache_actions": bool(cache_actions),
    }

    try:
        result = _post_json(
            path="/v1/act",
            payload=payload,
            api_key=_get_api_key(api_key),
            timeout=timeout,
        )
    except _NET_ERRORS as exc:
        logger.error(
            "stagehand_act failed url=%s instruction=%r",
            url,
            instruction[:80],
            exc_info=True,
        )
        return _err(f"stagehand_act error: {exc}", started)

    return _ok(result, started)


def stagehand_extract(
    url: str,
    schema: dict,
    *,
    instruction: str | None = None,
    api_key: str | None = None,
    timeout: int = 60,
) -> dict:
    """Extract structured data from a page via Stagehand ``extract``.

    Args:
        url: Page to load.
        schema: JSON Schema describing the desired output. Must be a
            non-empty dict; it is forwarded verbatim to the server.
        instruction: Optional natural-language hint, e.g.
            ``"extract the job title and salary range from the posting"``.
        api_key: Override for ``STAGEHAND_API_KEY`` /
            ``BROWSERBASE_API_KEY`` env vars.
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        ``{"data": <extracted-object>, "source": "stagehand",
          "elapsed_ms": int, "error": str | None}``.
    """
    started = time.monotonic()

    pre = _check_preconditions(url, started)
    if pre is not None:
        return pre

    if not isinstance(schema, dict) or not schema:
        return _err("schema must be a non-empty dict", started)

    payload: dict[str, Any] = {"url": url, "schema": schema}
    if instruction:
        payload["instruction"] = instruction

    try:
        result = _post_json(
            path="/v1/extract",
            payload=payload,
            api_key=_get_api_key(api_key),
            timeout=timeout,
        )
    except _NET_ERRORS as exc:
        logger.error(
            "stagehand_extract failed url=%s",
            url,
            exc_info=True,
        )
        return _err(f"stagehand_extract error: {exc}", started)

    return _ok(result, started)


def stagehand_observe(
    url: str,
    instruction: str | None = None,
    *,
    api_key: str | None = None,
    timeout: int = 30,
) -> dict:
    """Enumerate candidate elements / actions on a page.

    Args:
        url: Page to load.
        instruction: Optional hint, e.g. ``"find the apply button"``.
        api_key: Override for ``STAGEHAND_API_KEY`` /
            ``BROWSERBASE_API_KEY`` env vars.
        timeout: Per-request HTTP timeout in seconds.

    Returns:
        ``{"data": <observation-list>, "source": "stagehand",
          "elapsed_ms": int, "error": str | None}``.
    """
    started = time.monotonic()

    pre = _check_preconditions(url, started)
    if pre is not None:
        return pre

    payload: dict[str, Any] = {"url": url}
    if instruction:
        payload["instruction"] = instruction

    try:
        result = _post_json(
            path="/v1/observe",
            payload=payload,
            api_key=_get_api_key(api_key),
            timeout=timeout,
        )
    except _NET_ERRORS as exc:
        logger.error(
            "stagehand_observe failed url=%s",
            url,
            exc_info=True,
        )
        return _err(f"stagehand_observe error: {exc}", started)

    return _ok(result, started)
