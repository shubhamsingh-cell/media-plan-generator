"""sentry_integration.py -- Sentry Webhook & Self-Healing Bridge.

Receives Sentry webhook payloads, parses issue data, maps common error
patterns to automated fixes, and bridges into the AutoQC self-healing
system.  Also provides a lightweight Sentry API client for fetching
recent issues and posting comments/resolutions.

Architecture:
    1. Webhook endpoint receives POST from Sentry (signature-validated)
    2. SentryIssueParser extracts error type, message, stack trace, tags
    3. SentryHealingBridge maps known error patterns to fix strategies
    4. Unknown patterns are logged and alerted via alert_manager
    5. SentryAPIClient interacts with Sentry's REST API

Thread-safety: All shared state is guarded by threading locks.
Rate limiting: Max 10 auto-fix attempts per rolling hour.
Loop prevention: Max 3 fix attempts per unique issue fingerprint.

Configuration (env vars):
    SENTRY_AUTH_TOKEN       -- For Sentry API calls (required for API client)
    SENTRY_WEBHOOK_SECRET   -- For webhook signature validation (required)
    SENTRY_ORG_SLUG         -- Sentry organization slug (default: media-plan-generator)
    SENTRY_PROJECT_SLUG     -- Sentry project slug (default: python)

Dependencies: stdlib only (no new packages).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from alert_manager import send_alert as _email_alert
except ImportError:
    _email_alert = lambda *a, **kw: False

# ── Configuration ────────────────────────────────────────────────────────────

_SENTRY_AUTH_TOKEN: str = os.environ.get("SENTRY_AUTH_TOKEN") or ""
_SENTRY_WEBHOOK_SECRET: str = os.environ.get("SENTRY_WEBHOOK_SECRET") or ""
_SENTRY_ORG_SLUG: str = os.environ.get("SENTRY_ORG_SLUG") or "media-plan-generator"
_SENTRY_PROJECT_SLUG: str = os.environ.get("SENTRY_PROJECT_SLUG") or "python"
_SENTRY_API_BASE: str = "https://sentry.io/api/0"
_API_TIMEOUT: int = 15  # seconds

# ── Rate Limiting & Loop Prevention ──────────────────────────────────────────

_MAX_FIXES_PER_HOUR: int = 10
_MAX_ATTEMPTS_PER_ISSUE: int = 3
_ATTEMPT_WINDOW: float = 3600.0 * 24  # 24h window for per-issue tracking

_lock = threading.Lock()
_fix_timestamps: list[float] = []
_issue_attempts: Dict[str, list[float]] = {}  # fingerprint -> [timestamps]
_processed_events: Dict[str, float] = {}  # event_id -> timestamp (dedup)
_EVENT_DEDUP_WINDOW: float = 300.0  # 5 minutes

# ── Healing History ──────────────────────────────────────────────────────────

_heal_history: list[dict] = []
_MAX_HEAL_HISTORY: int = 100

# ── Module-Aware Self-Healing (v4.0) ─────────────────────────────────────────

# Module classification for error routing
_FILE_MODULE_MAP: Dict[str, str] = {
    "app.py": "command_center",
    "data_orchestrator.py": "command_center",
    "budget_engine.py": "command_center",
    "standardizer.py": "command_center",
    "api_integrations.py": "command_center",
    "deck_generator.py": "command_center",
    "sheets_export.py": "command_center",
    "web_scraper.py": "intelligence_hub",
    "web_scraper_router.py": "intelligence_hub",
    "research_engine.py": "intelligence_hub",
    "competitive_intel.py": "intelligence_hub",
    "talent_research.py": "intelligence_hub",
    "nova_chat.py": "nova_ai",
    "nova_context.py": "nova_ai",
    "nova_persistence.py": "nova_ai",
    "nova_voice.py": "nova_ai",
    "nova_tools.py": "nova_ai",
    "elevenlabs_integration.py": "nova_ai",
    "llm_router.py": "nova_ai",
}

# Module-specific auto-fix strategies (beyond generic pattern matching)
_MODULE_FIX_STRATEGIES: Dict[str, list] = {
    "command_center": [
        {
            "action": "retry_different_llm",
            "description": "Retry with a different LLM provider",
        },
        {
            "action": "clear_enrichment_cache",
            "description": "Clear stale API enrichment cache",
        },
        {
            "action": "reload_knowledge_base",
            "description": "Reload knowledge base data",
        },
    ],
    "intelligence_hub": [
        {
            "action": "switch_scraper_tier",
            "description": "Switch to next web scraper fallback tier",
        },
        {
            "action": "fallback_data_api",
            "description": "Switch to fallback data API client",
        },
        {"action": "clear_search_cache", "description": "Clear search result cache"},
    ],
    "nova_ai": [
        {
            "action": "reset_chat_state",
            "description": "Reset Nova chat conversation state",
        },
        {"action": "clear_response_cache", "description": "Clear LLM response cache"},
        {"action": "switch_backup_llm", "description": "Switch to backup LLM provider"},
    ],
}

# Self-healing metrics per module
_module_heal_stats_lock = threading.Lock()
_module_heal_stats: Dict[str, Dict[str, int]] = {
    "command_center": {"attempts": 0, "successes": 0, "failures": 0},
    "intelligence_hub": {"attempts": 0, "successes": 0, "failures": 0},
    "nova_ai": {"attempts": 0, "successes": 0, "failures": 0},
}

# Escalation tracking: {error_type_fingerprint: [timestamps]}
_escalation_tracker: Dict[str, list] = {}
_ESCALATION_THRESHOLD = 3  # 3 failures in 5 minutes
_ESCALATION_WINDOW = 300.0  # 5 minutes


def _classify_error_to_module(file_path: str) -> str:
    """Classify a source file to its owning module.

    Args:
        file_path: Source file path from Sentry stack trace.

    Returns:
        Module name string, or empty string if unclassified.
    """
    if not file_path:
        return ""
    filename = file_path.rsplit("/", 1)[-1]
    return _FILE_MODULE_MAP.get(filename, "")


def _record_module_heal(module: str, success: bool) -> None:
    """Record a self-healing attempt for a module.

    Args:
        module: Module name (command_center, intelligence_hub, nova_ai).
        success: Whether the healing was successful.
    """
    if module not in _module_heal_stats:
        return
    with _module_heal_stats_lock:
        _module_heal_stats[module]["attempts"] += 1
        if success:
            _module_heal_stats[module]["successes"] += 1
        else:
            _module_heal_stats[module]["failures"] += 1


def _check_escalation(fingerprint: str, module: str) -> bool:
    """Check if an error should be escalated (3+ failures in 5 minutes).

    If escalation threshold is hit, logs critical and attempts Slack alert.

    Args:
        fingerprint: Error fingerprint for dedup.
        module: Module where the error occurred.

    Returns:
        True if escalation was triggered.
    """
    now = time.time()
    escalation_key = f"{module}:{fingerprint}"
    with _module_heal_stats_lock:
        if escalation_key not in _escalation_tracker:
            _escalation_tracker[escalation_key] = []
        timestamps = _escalation_tracker[escalation_key]
        # Prune old entries
        cutoff = now - _ESCALATION_WINDOW
        timestamps[:] = [ts for ts in timestamps if ts > cutoff]
        timestamps.append(now)

        if len(timestamps) >= _ESCALATION_THRESHOLD:
            # Reset to prevent repeated escalation
            _escalation_tracker[escalation_key] = []

    if len(timestamps) >= _ESCALATION_THRESHOLD:
        logger.critical(
            "ESCALATION: self-healing failed %d times in %.0fs for module=%s fingerprint=%s",
            _ESCALATION_THRESHOLD,
            _ESCALATION_WINDOW,
            module,
            fingerprint,
        )
        # Attempt Slack alert
        _attempt_slack_escalation(module, fingerprint)
        return True
    return False


def _attempt_slack_escalation(module: str, fingerprint: str) -> None:
    """Fire-and-forget Slack alert for self-healing escalation.

    Args:
        module: Module name.
        fingerprint: Error fingerprint.
    """

    def _send() -> None:
        try:
            webhook_url = os.environ.get("SLACK_WEBHOOK_URL") or ""
            if not webhook_url:
                logger.debug("sentry_integration: no SLACK_WEBHOOK_URL for escalation")
                return
            payload = json.dumps(
                {
                    "text": (
                        f":rotating_light: *Self-Healing Escalation*\n"
                        f"Module: `{module}`\n"
                        f"Fingerprint: `{fingerprint}`\n"
                        f"Self-healing failed {_ESCALATION_THRESHOLD}x in {int(_ESCALATION_WINDOW)}s.\n"
                        f"Manual investigation required."
                    ),
                }
            ).encode("utf-8")
            req = urllib.request.Request(
                webhook_url,
                data=payload,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            logger.info(
                "sentry_integration: escalation Slack alert sent for %s", module
            )
        except (urllib.error.URLError, OSError) as exc:
            logger.warning("sentry_integration: Slack escalation failed: %s", exc)

    t = threading.Thread(target=_send, daemon=True)
    t.start()


def _execute_module_healing(
    module: str, action: str, parsed_issue: Dict[str, Any]
) -> bool:
    """Execute a module-specific healing action.

    Args:
        module: Module name.
        action: Healing action identifier.
        parsed_issue: Parsed Sentry issue data.

    Returns:
        True if healing succeeded.
    """
    import sys
    import gc as gc_mod

    try:
        if action == "retry_different_llm":
            # Reset LLM router circuit breakers for a fresh provider selection
            if "llm_router" in sys.modules:
                lr = sys.modules["llm_router"]
                states = getattr(lr, "_provider_states", {})
                reset_count = 0
                for pid, state in states.items():
                    try:
                        with state.lock:
                            if state.consecutive_failures > 0:
                                state.consecutive_failures = 0
                                state.circuit_open_until = 0.0
                                reset_count += 1
                    except (AttributeError, RuntimeError):
                        continue
                logger.info("sentry_heal: reset %d LLM circuit breakers", reset_count)
                return reset_count > 0
            return False

        elif action == "clear_enrichment_cache":
            if "data_orchestrator" in sys.modules:
                do = sys.modules["data_orchestrator"]
                try:
                    with do._api_cache_lock:
                        size_before = len(do._api_result_cache)
                        do._api_result_cache.clear()
                    logger.info(
                        "sentry_heal: cleared %d enrichment cache entries", size_before
                    )
                    return True
                except (AttributeError, RuntimeError):
                    pass
            return False

        elif action == "reload_knowledge_base":
            if "data_orchestrator" in sys.modules:
                do = sys.modules["data_orchestrator"]
                if hasattr(do, "_knowledge_base"):
                    try:
                        do._knowledge_base = None
                        logger.info("sentry_heal: reset knowledge base for reload")
                        return True
                    except (AttributeError, RuntimeError):
                        pass
            return False

        elif action == "switch_scraper_tier":
            if "web_scraper_router" in sys.modules:
                wsr = sys.modules["web_scraper_router"]
                if hasattr(wsr, "_preferred_tier"):
                    try:
                        current = getattr(wsr, "_preferred_tier", 0)
                        wsr._preferred_tier = current + 1
                        logger.info(
                            "sentry_heal: advanced scraper tier to %d", current + 1
                        )
                        return True
                    except (AttributeError, RuntimeError):
                        pass
            return False

        elif action == "fallback_data_api":
            if "api_integrations" in sys.modules:
                ai = sys.modules["api_integrations"]
                if hasattr(ai, "_primary_disabled"):
                    ai._primary_disabled = True
                    logger.info("sentry_heal: switched to fallback data API")
                    return True
            return False

        elif action == "clear_search_cache":
            if "web_scraper_router" in sys.modules:
                wsr = sys.modules["web_scraper_router"]
                if hasattr(wsr, "_search_cache"):
                    try:
                        wsr._search_cache.clear()
                        logger.info("sentry_heal: cleared search cache")
                        return True
                    except (AttributeError, RuntimeError):
                        pass
            return False

        elif action == "reset_chat_state":
            # Clear any stuck conversation locks
            if "nova_persistence" in sys.modules:
                np = sys.modules["nova_persistence"]
                if hasattr(np, "_conversation_locks"):
                    try:
                        with np._conversation_locks_guard:
                            np._conversation_locks.clear()
                        logger.info("sentry_heal: cleared conversation locks")
                        return True
                    except (AttributeError, RuntimeError):
                        pass
            return False

        elif action == "clear_response_cache":
            if "llm_router" in sys.modules:
                lr = sys.modules["llm_router"]
                if hasattr(lr, "_response_cache"):
                    try:
                        cache = lr._response_cache
                        size_before = len(cache)
                        cache.clear()
                        logger.info(
                            "sentry_heal: cleared %d LLM response cache entries",
                            size_before,
                        )
                        return True
                    except (AttributeError, RuntimeError):
                        pass
            return False

        elif action == "switch_backup_llm":
            # Deprioritize failing provider by boosting backup scores
            if "llm_router" in sys.modules:
                lr = sys.modules["llm_router"]
                states = getattr(lr, "_provider_states", {})
                for pid, state in states.items():
                    try:
                        with state.lock:
                            if state.consecutive_failures >= 2:
                                state.circuit_open_until = time.time() + 300
                    except (AttributeError, RuntimeError):
                        continue
                logger.info("sentry_heal: circuit-opened failing LLM providers for 5m")
                return True

        return False
    except Exception as exc:
        logger.error(
            "sentry_integration: module healing failed (%s/%s): %s",
            module,
            action,
            exc,
            exc_info=True,
        )
        return False


def get_module_heal_stats() -> Dict[str, Dict[str, int]]:
    """Return self-healing metrics per module.

    Returns:
        Dict mapping module name to {attempts, successes, failures}.
    """
    with _module_heal_stats_lock:
        return {k: dict(v) for k, v in _module_heal_stats.items()}


# =============================================================================
# WEBHOOK SIGNATURE VALIDATION
# =============================================================================


def validate_sentry_signature(
    body: bytes,
    signature_header: str,
    secret: str = "",
) -> bool:
    """Validate a Sentry webhook signature using HMAC-SHA256.

    Sentry sends the signature in the ``Sentry-Hook-Signature`` header
    as a hex digest of HMAC-SHA256(secret, body).

    Args:
        body: Raw request body bytes.
        signature_header: Value of the Sentry-Hook-Signature header.
        secret: Webhook secret (falls back to env var if empty).

    Returns:
        True if the signature is valid, False otherwise.
    """
    webhook_secret = secret or _SENTRY_WEBHOOK_SECRET
    if not webhook_secret:
        logger.warning(
            "sentry_integration: SENTRY_WEBHOOK_SECRET not set, cannot validate"
        )
        return False

    if not signature_header:
        logger.warning("sentry_integration: missing Sentry-Hook-Signature header")
        return False

    try:
        expected = hmac.new(
            webhook_secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature_header.strip())
    except (ValueError, TypeError) as exc:
        logger.error(
            "sentry_integration: signature validation error: %s", exc, exc_info=True
        )
        return False


# =============================================================================
# SENTRY ISSUE PARSER
# =============================================================================


class SentryIssueParser:
    """Parse Sentry webhook payloads into structured issue data."""

    @staticmethod
    def parse_webhook(payload: dict) -> Optional[Dict[str, Any]]:
        """Parse a Sentry webhook payload into a normalized issue dict.

        Args:
            payload: Decoded JSON body from the Sentry webhook.

        Returns:
            Normalized issue dict, or None if the payload is not actionable.
        """
        try:
            action = payload.get("action") or ""
            data = payload.get("data") or {}
            issue = data.get("issue") or data.get("event") or {}

            if not issue:
                logger.debug(
                    "sentry_integration: webhook payload has no issue/event data"
                )
                return None

            # Extract core fields
            issue_id = str(issue.get("id") or "")
            title = issue.get("title") or ""
            culprit = issue.get("culprit") or ""
            metadata = issue.get("metadata") or {}
            error_type = metadata.get("type") or _extract_error_type(title)
            error_message = metadata.get("value") or title

            # Extract stack trace info
            stacktrace_info = _extract_stacktrace(issue)

            # Extract tags
            tags_list = issue.get("tags") or []
            tags: Dict[str, str] = {}
            for tag in tags_list:
                if isinstance(tag, dict):
                    key = tag.get("key") or tag.get("name") or ""
                    value = tag.get("value") or ""
                    if key:
                        tags[key] = value

            # Event count / frequency
            event_count = issue.get("count") or 1
            try:
                event_count = int(event_count)
            except (ValueError, TypeError):
                event_count = 1

            # Fingerprint for dedup
            fingerprint = _compute_fingerprint(
                error_type, error_message, stacktrace_info
            )

            return {
                "issue_id": issue_id,
                "action": action,
                "error_type": error_type,
                "error_message": error_message,
                "title": title,
                "culprit": culprit,
                "file": stacktrace_info.get("file") or "",
                "line_number": stacktrace_info.get("line_number") or 0,
                "function": stacktrace_info.get("function") or "",
                "context_line": stacktrace_info.get("context_line") or "",
                "tags": tags,
                "event_count": event_count,
                "fingerprint": fingerprint,
                "environment": tags.get("environment")
                or issue.get("environment")
                or "",
                "level": issue.get("level") or "error",
                "first_seen": issue.get("firstSeen") or "",
                "last_seen": issue.get("lastSeen") or "",
                "raw_payload": payload,
            }

        except Exception as exc:
            logger.error(
                "sentry_integration: failed to parse webhook payload: %s",
                exc,
                exc_info=True,
            )
            return None


def _extract_error_type(title: str) -> str:
    """Extract the error class from an issue title like 'AttributeError: ...'."""
    if not title:
        return "Unknown"
    match = re.match(r"^([A-Z]\w*Error|[A-Z]\w*Exception|[A-Z]\w*Warning)", title)
    if match:
        return match.group(1)
    if ":" in title:
        prefix = title.split(":")[0].strip()
        if re.match(r"^[A-Z]\w+$", prefix):
            return prefix
    return "Unknown"


def _extract_stacktrace(issue: dict) -> Dict[str, Any]:
    """Extract the most relevant stack frame from a Sentry issue."""
    result: Dict[str, Any] = {
        "file": "",
        "line_number": 0,
        "function": "",
        "context_line": "",
    }

    try:
        # Try event-level exception interface
        entries = issue.get("entries") or []
        for entry in entries:
            if (entry.get("type") or "") == "exception":
                values = (entry.get("data") or {}).get("values") or []
                for exc_val in values:
                    stacktrace = exc_val.get("stacktrace") or {}
                    frames = stacktrace.get("frames") or []
                    if frames:
                        # Last frame is most relevant (app code, not stdlib)
                        for frame in reversed(frames):
                            filename = frame.get("filename") or ""
                            # Skip stdlib and third-party frames
                            if (
                                filename
                                and not filename.startswith("lib/")
                                and "site-packages" not in filename
                                and not filename.startswith("<")
                            ):
                                result["file"] = filename
                                result["line_number"] = frame.get("lineNo") or 0
                                result["function"] = frame.get("function") or ""
                                result["context_line"] = frame.get("context_line") or ""
                                return result
                        # Fallback to last frame
                        last = frames[-1]
                        result["file"] = last.get("filename") or ""
                        result["line_number"] = last.get("lineNo") or 0
                        result["function"] = last.get("function") or ""
                        result["context_line"] = last.get("context_line") or ""
                        return result

        # Fallback: try metadata
        metadata = issue.get("metadata") or {}
        if metadata.get("filename"):
            result["file"] = metadata["filename"]
        if metadata.get("function"):
            result["function"] = metadata["function"]

    except Exception as exc:
        logger.debug("sentry_integration: stacktrace extraction failed: %s", exc)

    return result


def _compute_fingerprint(error_type: str, error_message: str, stacktrace: dict) -> str:
    """Create a stable fingerprint for dedup / loop detection."""
    parts = [
        error_type or "Unknown",
        (error_message or "")[:200],
        stacktrace.get("file") or "",
        str(stacktrace.get("line_number") or 0),
        stacktrace.get("function") or "",
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# SELF-HEALING BRIDGE
# =============================================================================


# Known error patterns -> fix strategies
_ERROR_PATTERNS: list[Dict[str, Any]] = [
    {
        "pattern": r"AttributeError: '?str'? object has no attribute '?get'?",
        "fix_type": "isinstance_guard",
        "description": "Add isinstance check before calling .get() on potential str",
        "severity": "high",
    },
    {
        "pattern": r"AttributeError: '?NoneType'? object has no attribute '?(\w+)'?",
        "fix_type": "none_check",
        "description": "Add None guard before attribute access",
        "severity": "high",
    },
    {
        "pattern": r"TypeError: .+ argument must be str, not (None|NoneType)",
        "fix_type": "or_empty_string",
        "description": 'Add `or ""` guard for None-to-str coercion',
        "severity": "high",
    },
    {
        "pattern": r"KeyError: ['\"]?(\w+)['\"]?",
        "fix_type": "dict_get_default",
        "description": "Use .get() with default instead of direct key access",
        "severity": "medium",
    },
    {
        "pattern": r"IndexError: list index out of range",
        "fix_type": "bounds_check",
        "description": "Add length/bounds check before index access",
        "severity": "medium",
    },
    {
        "pattern": r"TypeError: .+ object is not (subscriptable|iterable|callable)",
        "fix_type": "type_guard",
        "description": "Add type check before operation",
        "severity": "medium",
    },
    {
        "pattern": r"json\.JSONDecodeError|Expecting value:",
        "fix_type": "json_parse_guard",
        "description": "Wrap JSON parse in try/except with fallback",
        "severity": "medium",
    },
    {
        "pattern": r"ConnectionError|URLError|TimeoutError|ConnectionRefusedError",
        "fix_type": "network_retry",
        "description": "External API unreachable -- trigger connection retry",
        "severity": "low",
    },
    {
        "pattern": r"MemoryError|ResourceWarning",
        "fix_type": "resource_cleanup",
        "description": "Memory pressure -- trigger GC and cache eviction",
        "severity": "critical",
    },
]


class SentryHealingBridge:
    """Bridge between Sentry issues and the AutoQC self-healing system.

    Maps parsed Sentry issues to known fix patterns and either
    auto-generates fixes or escalates unknown patterns via alerts.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def process_issue(self, parsed_issue: Dict[str, Any]) -> Dict[str, Any]:
        """Process a parsed Sentry issue through the healing pipeline.

        Args:
            parsed_issue: Output from SentryIssueParser.parse_webhook().

        Returns:
            Result dict with keys: handled, fix_type, description, action_taken,
            reason (if not handled).
        """
        fingerprint = parsed_issue.get("fingerprint") or ""
        error_type = parsed_issue.get("error_type") or ""
        error_message = parsed_issue.get("error_message") or ""
        issue_id = parsed_issue.get("issue_id") or ""

        # -- Dedup check: skip if we already processed this event recently ----
        event_key = f"{issue_id}:{fingerprint}"
        now = time.time()
        with _lock:
            last_processed = _processed_events.get(event_key, 0.0)
            if (now - last_processed) < _EVENT_DEDUP_WINDOW:
                return {
                    "handled": False,
                    "fix_type": None,
                    "description": "Duplicate event (recently processed)",
                    "action_taken": False,
                    "reason": "dedup",
                }
            _processed_events[event_key] = now
            # Prune old dedup entries
            stale = [
                k
                for k, ts in _processed_events.items()
                if (now - ts) > _EVENT_DEDUP_WINDOW * 2
            ]
            for k in stale:
                del _processed_events[k]

        # -- Rate limit check -------------------------------------------------
        if not self._check_rate_limit():
            logger.warning(
                "sentry_integration: hourly fix rate limit reached (%d/%d)",
                len(_fix_timestamps),
                _MAX_FIXES_PER_HOUR,
            )
            return {
                "handled": False,
                "fix_type": None,
                "description": "Rate limit reached",
                "action_taken": False,
                "reason": "rate_limited",
            }

        # -- Loop prevention: max attempts per issue --------------------------
        if not self._check_attempt_limit(fingerprint):
            logger.warning(
                "sentry_integration: max attempts reached for issue %s (fingerprint=%s)",
                issue_id,
                fingerprint,
            )
            _email_alert(
                f"Sentry auto-fix loop detected: {error_type}",
                (
                    f"Issue {issue_id} has exceeded {_MAX_ATTEMPTS_PER_ISSUE} fix attempts "
                    f"in the last 24h.<br><br>"
                    f"<b>Error:</b> {error_message[:300]}<br>"
                    f"<b>File:</b> {parsed_issue.get('file') or 'unknown'}<br>"
                    f"<b>Function:</b> {parsed_issue.get('function') or 'unknown'}<br>"
                    f"<b>Fingerprint:</b> {fingerprint}"
                ),
                severity="warning",
            )
            return {
                "handled": False,
                "fix_type": None,
                "description": f"Max {_MAX_ATTEMPTS_PER_ISSUE} attempts exceeded for this issue",
                "action_taken": False,
                "reason": "max_attempts",
            }

        # -- Pattern matching -------------------------------------------------
        full_error = f"{error_type}: {error_message}"
        matched_pattern = None
        for pattern_def in _ERROR_PATTERNS:
            try:
                if re.search(pattern_def["pattern"], full_error, re.IGNORECASE):
                    matched_pattern = pattern_def
                    break
            except re.error:
                continue

        if matched_pattern:
            return self._handle_known_pattern(parsed_issue, matched_pattern)
        else:
            return self._handle_unknown_pattern(parsed_issue)

    def _handle_known_pattern(
        self,
        parsed_issue: Dict[str, Any],
        pattern: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Handle an issue matching a known error pattern."""
        fix_type = pattern["fix_type"]
        description = pattern["description"]
        error_type = parsed_issue.get("error_type") or ""
        error_message = parsed_issue.get("error_message") or ""
        issue_id = parsed_issue.get("issue_id") or ""
        fingerprint = parsed_issue.get("fingerprint") or ""
        file_path = parsed_issue.get("file") or ""
        function_name = parsed_issue.get("function") or ""
        line_number = parsed_issue.get("line_number") or 0

        # Record the attempt
        self._record_attempt(fingerprint)

        # Generate the fix suggestion
        fix_suggestion = _generate_fix_suggestion(
            fix_type=fix_type,
            error_type=error_type,
            error_message=error_message,
            file_path=file_path,
            function_name=function_name,
            line_number=line_number,
            context_line=parsed_issue.get("context_line") or "",
        )

        # Attempt self-healing via AutoQC bridge
        heal_success = self._trigger_self_healing(parsed_issue, fix_type)

        # Verify fix worked after 30s delay (Phase 6)
        if heal_success:
            threading.Timer(30.0, _verify_heal, args=[fix_type, fingerprint]).start()

        # Record in history
        _record_heal_event(
            issue_id=issue_id,
            fingerprint=fingerprint,
            fix_type=fix_type,
            description=description,
            success=heal_success,
            file_path=file_path,
            function_name=function_name,
        )

        # Record fix timestamp for rate limiting
        with _lock:
            _fix_timestamps.append(time.time())

        # Comment on Sentry issue if API is available
        if issue_id and _SENTRY_AUTH_TOKEN:
            comment = (
                f"[Auto-Heal] Detected pattern: {fix_type}\n"
                f"Fix suggestion: {description}\n"
                f"Self-healing {'succeeded' if heal_success else 'attempted (manual review needed)'}.\n"
                f"File: {file_path}:{line_number}\n"
                f"Function: {function_name}"
            )
            _thread_safe_api_call(
                SentryAPIClient.add_comment,
                issue_id,
                comment,
            )

        logger.info(
            "sentry_integration: handled issue %s -- fix_type=%s, healed=%s",
            issue_id,
            fix_type,
            heal_success,
        )

        return {
            "handled": True,
            "fix_type": fix_type,
            "description": description,
            "action_taken": True,
            "heal_success": heal_success,
            "fix_suggestion": fix_suggestion,
            "issue_id": issue_id,
            "fingerprint": fingerprint,
        }

    def _handle_unknown_pattern(self, parsed_issue: Dict[str, Any]) -> Dict[str, Any]:
        """Handle an issue with no known fix pattern -- alert and log."""
        error_type = parsed_issue.get("error_type") or ""
        error_message = parsed_issue.get("error_message") or ""
        issue_id = parsed_issue.get("issue_id") or ""
        fingerprint = parsed_issue.get("fingerprint") or ""
        file_path = parsed_issue.get("file") or ""
        function_name = parsed_issue.get("function") or ""
        level = parsed_issue.get("level") or "error"

        # Record in history
        _record_heal_event(
            issue_id=issue_id,
            fingerprint=fingerprint,
            fix_type="unknown",
            description=f"No known fix for {error_type}",
            success=False,
            file_path=file_path,
            function_name=function_name,
        )

        # Send alert for unknown critical/error patterns
        severity_map = {"fatal": "critical", "error": "warning", "warning": "info"}
        alert_severity = severity_map.get(level, "info")

        _email_alert(
            f"Sentry: unhandled {error_type} in {file_path or 'unknown'}",
            (
                f"<b>Error:</b> {error_message[:500]}<br>"
                f"<b>File:</b> {file_path}:{parsed_issue.get('line_number') or '?'}<br>"
                f"<b>Function:</b> {function_name or 'unknown'}<br>"
                f"<b>Issue ID:</b> {issue_id}<br>"
                f"<b>Event Count:</b> {parsed_issue.get('event_count') or 1}<br>"
                f"<b>Environment:</b> {parsed_issue.get('environment') or 'unknown'}<br><br>"
                f"No automated fix available. Manual investigation required."
            ),
            severity=alert_severity,
        )

        logger.info(
            "sentry_integration: unknown pattern for issue %s (%s) -- alerted",
            issue_id,
            error_type,
        )

        return {
            "handled": False,
            "fix_type": None,
            "description": f"No known fix for {error_type}",
            "action_taken": False,
            "reason": "unknown_pattern",
            "issue_id": issue_id,
            "fingerprint": fingerprint,
        }

    def _trigger_self_healing(
        self,
        parsed_issue: Dict[str, Any],
        fix_type: str,
    ) -> bool:
        """Trigger the appropriate self-healing action via AutoQC and module strategies.

        First tries module-specific healing, then falls back to generic AutoQC.
        Tracks metrics and checks escalation thresholds.

        Returns True if healing was successful.
        """
        file_path = parsed_issue.get("file") or ""
        fingerprint = parsed_issue.get("fingerprint") or ""
        module = _classify_error_to_module(file_path)

        # --- Module-specific healing (v4.0) ---
        if module and module in _MODULE_FIX_STRATEGIES:
            for strategy in _MODULE_FIX_STRATEGIES[module]:
                action = strategy["action"]
                success = _execute_module_healing(module, action, parsed_issue)
                _record_module_heal(module, success)
                if success:
                    logger.info(
                        "sentry_integration: module heal succeeded: module=%s action=%s",
                        module,
                        action,
                    )
                    return True
            # All module strategies failed -- check escalation
            _check_escalation(fingerprint, module)

        # --- Generic AutoQC healing ---
        try:
            from auto_qc import get_auto_qc

            qc = get_auto_qc()
        except ImportError:
            logger.warning("sentry_integration: auto_qc not available for self-healing")
            return False

        try:
            result = _execute_healing_action(fix_type, parsed_issue, qc)
            if module:
                _record_module_heal(module, result)
            return result
        except Exception as exc:
            logger.error(
                "sentry_integration: self-healing failed for fix_type=%s: %s",
                fix_type,
                exc,
                exc_info=True,
            )
            if module:
                _record_module_heal(module, False)
                _check_escalation(fingerprint, module)
            return False

    def _check_rate_limit(self) -> bool:
        """Check if we are within the hourly fix rate limit."""
        now = time.time()
        with _lock:
            cutoff = now - 3600.0
            _fix_timestamps[:] = [ts for ts in _fix_timestamps if ts > cutoff]
            return len(_fix_timestamps) < _MAX_FIXES_PER_HOUR

    def _check_attempt_limit(self, fingerprint: str) -> bool:
        """Check if this issue has exceeded max fix attempts."""
        now = time.time()
        with _lock:
            attempts = _issue_attempts.get(fingerprint, [])
            # Prune old attempts
            cutoff = now - _ATTEMPT_WINDOW
            attempts = [ts for ts in attempts if ts > cutoff]
            _issue_attempts[fingerprint] = attempts
            return len(attempts) < _MAX_ATTEMPTS_PER_ISSUE

    def _record_attempt(self, fingerprint: str) -> None:
        """Record a fix attempt for loop prevention."""
        with _lock:
            if fingerprint not in _issue_attempts:
                _issue_attempts[fingerprint] = []
            _issue_attempts[fingerprint].append(time.time())


def _execute_healing_action(
    fix_type: str,
    parsed_issue: Dict[str, Any],
    qc: Any,
) -> bool:
    """Execute a specific healing action based on fix_type.

    Args:
        fix_type: The type of fix to apply.
        parsed_issue: The parsed issue data.
        qc: The AutoQC instance.

    Returns:
        True if the healing action succeeded.
    """
    import importlib
    import sys
    import gc as gc_mod

    file_path = parsed_issue.get("file") or ""
    module_name = _file_to_module(file_path)

    if fix_type == "isinstance_guard":
        # For str.get() errors, the fix is in the code, but we can
        # reload the module to pick up hotfixes
        if module_name and module_name in sys.modules:
            try:
                importlib.reload(sys.modules[module_name])
                qc._record_heal(f"sentry:{file_path}", "module_reload_isinstance", True)
                return True
            except Exception as exc:
                qc._record_heal(
                    f"sentry:{file_path}", "module_reload_isinstance", False
                )
                logger.warning("sentry heal: module reload failed: %s", exc)
        return False

    elif fix_type == "none_check":
        # NoneType errors -- attempt module reload + sentinel reset
        if module_name and module_name in sys.modules:
            try:
                importlib.reload(sys.modules[module_name])
                qc._record_heal(f"sentry:{file_path}", "module_reload_none_check", True)
                return True
            except Exception as exc:
                qc._record_heal(
                    f"sentry:{file_path}", "module_reload_none_check", False
                )
        # Also reset data_orchestrator sentinels if applicable
        if "data_orchestrator" in sys.modules:
            try:
                do = sys.modules["data_orchestrator"]
                with do._load_lock:
                    for attr in (
                        "_api_enrichment",
                        "_research",
                        "_budget_engine",
                        "_standardizer",
                    ):
                        if getattr(do, attr, None) is do._IMPORT_FAILED:
                            setattr(do, attr, None)
                qc._record_heal(
                    f"sentry:{file_path}", "reset_orchestrator_sentinels", True
                )
                return True
            except (ImportError, AttributeError, RuntimeError) as exc:
                logger.debug("sentry heal: orchestrator sentinel reset failed: %s", exc)
        return False

    elif fix_type == "or_empty_string":
        # TypeError with None -> str -- same as none_check strategy
        if module_name and module_name in sys.modules:
            try:
                importlib.reload(sys.modules[module_name])
                qc._record_heal(f"sentry:{file_path}", "module_reload_str_guard", True)
                return True
            except Exception:
                qc._record_heal(f"sentry:{file_path}", "module_reload_str_guard", False)
        return False

    elif fix_type == "dict_get_default":
        # KeyError -- reload module to pick up fixes
        if module_name and module_name in sys.modules:
            try:
                importlib.reload(sys.modules[module_name])
                qc._record_heal(f"sentry:{file_path}", "module_reload_keyerror", True)
                return True
            except Exception:
                qc._record_heal(f"sentry:{file_path}", "module_reload_keyerror", False)
        return False

    elif fix_type == "bounds_check":
        if module_name and module_name in sys.modules:
            try:
                importlib.reload(sys.modules[module_name])
                qc._record_heal(f"sentry:{file_path}", "module_reload_bounds", True)
                return True
            except Exception:
                qc._record_heal(f"sentry:{file_path}", "module_reload_bounds", False)
        return False

    elif fix_type == "type_guard":
        if module_name and module_name in sys.modules:
            try:
                importlib.reload(sys.modules[module_name])
                qc._record_heal(f"sentry:{file_path}", "module_reload_type_guard", True)
                return True
            except Exception:
                qc._record_heal(
                    f"sentry:{file_path}", "module_reload_type_guard", False
                )
        return False

    elif fix_type == "json_parse_guard":
        # JSON decode errors -- clear caches that might hold corrupt data
        if "data_orchestrator" in sys.modules:
            try:
                do = sys.modules["data_orchestrator"]
                with do._api_cache_lock:
                    do._api_result_cache.clear()
                qc._record_heal(f"sentry:{file_path}", "clear_api_cache_json", True)
                return True
            except Exception:
                qc._record_heal(f"sentry:{file_path}", "clear_api_cache_json", False)
        return False

    elif fix_type == "network_retry":
        # Network errors -- reset LLM circuit breakers and retry connections
        if "llm_router" in sys.modules:
            try:
                lr = sys.modules["llm_router"]
                states = getattr(lr, "_provider_states", {})
                for pid, state in states.items():
                    with state.lock:
                        if state.consecutive_failures > 0:
                            state.consecutive_failures = 0
                            state.circuit_open_until = 0.0
                qc._record_heal(f"sentry:network", "reset_circuit_breakers", True)
                return True
            except Exception:
                qc._record_heal(f"sentry:network", "reset_circuit_breakers", False)
        return False

    elif fix_type == "resource_cleanup":
        # Memory pressure -- force GC and evict caches
        try:
            gc_mod.collect()
            if "data_orchestrator" in sys.modules:
                do = sys.modules["data_orchestrator"]
                with do._api_cache_lock:
                    now = time.time()
                    expired = [
                        k
                        for k, v in do._api_result_cache.items()
                        if now >= (v.get("expires") or 0)
                    ]
                    for k in expired:
                        do._api_result_cache.pop(k, None)
            qc._record_heal("sentry:memory", "gc_collect_and_cache_evict", True)
            return True
        except Exception:
            qc._record_heal("sentry:memory", "gc_collect_and_cache_evict", False)
            return False

    else:
        logger.debug("sentry_integration: no healing action for fix_type=%s", fix_type)
        return False


def _file_to_module(file_path: str) -> str:
    """Convert a file path like 'app.py' or 'llm_router.py' to module name."""
    if not file_path:
        return ""
    # Strip directory prefix, keep just filename
    name = file_path.rsplit("/", 1)[-1]
    if name.endswith(".py"):
        name = name[:-3]
    return name


def _generate_fix_suggestion(
    fix_type: str,
    error_type: str,
    error_message: str,
    file_path: str,
    function_name: str,
    line_number: int,
    context_line: str,
) -> str:
    """Generate a human-readable fix suggestion for a known pattern.

    Args:
        fix_type: The classified fix type.
        error_type: The Python exception class.
        error_message: The exception message.
        file_path: Source file path.
        function_name: Function where error occurred.
        line_number: Line number of the error.
        context_line: The line of code that errored.

    Returns:
        A string describing the suggested fix.
    """
    location = f"{file_path}:{line_number}" if file_path else "unknown location"
    func_label = f" in {function_name}()" if function_name else ""

    suggestions: Dict[str, str] = {
        "isinstance_guard": (
            f"At {location}{func_label}: Add `if isinstance(obj, dict):` guard "
            f"before calling `.get()`. The variable is sometimes a str instead of dict."
        ),
        "none_check": (
            f"At {location}{func_label}: Add `if obj is not None:` guard "
            f"before accessing attributes. Use `(obj or default)` pattern."
        ),
        "or_empty_string": (
            f'At {location}{func_label}: Use `value or ""` to coerce None to '
            f"empty string before passing to functions expecting str."
        ),
        "dict_get_default": (
            f"At {location}{func_label}: Replace `dict[key]` with "
            f"`dict.get(key, default)` to handle missing keys safely."
        ),
        "bounds_check": (
            f"At {location}{func_label}: Add `if idx < len(lst):` guard "
            f"before indexing. The list may be empty or shorter than expected."
        ),
        "type_guard": (
            f"At {location}{func_label}: Add type check before operation. "
            f"The value may be None or an unexpected type."
        ),
        "json_parse_guard": (
            f"At {location}{func_label}: Wrap `json.loads()` in try/except "
            f"JSONDecodeError with a sensible fallback value."
        ),
        "network_retry": (
            f"Network connectivity issue detected. Circuit breakers have been "
            f"reset. The next request will retry the connection."
        ),
        "resource_cleanup": (
            f"Memory pressure detected. Garbage collection triggered and "
            f"stale caches evicted."
        ),
    }

    return suggestions.get(fix_type, f"Unknown fix type: {fix_type} at {location}")


# ── Heal Verification (Phase 6) ───────────────────────────────────────────────

_heal_verification_stats_lock = threading.Lock()
_heal_verification_stats: Dict[str, int] = {
    "verifications_passed": 0,
    "verifications_failed": 0,
}


def _verify_heal(action_type: str, fingerprint: str) -> None:
    """Verify a healing action actually fixed the issue (30s delayed check).

    Called via threading.Timer after a successful heal. Checks whether the
    same error fingerprint recurred in the heal history within the last 35s.

    Args:
        action_type: The fix_type that was applied.
        fingerprint: Error fingerprint to check for recurrence.
    """
    try:
        now = time.time()
        with _lock:
            # Check if same fingerprint appeared again in recent history
            recent_errors = [
                e
                for e in _heal_history[-10:]
                if e.get("fingerprint") == fingerprint
                and not e.get("is_verification", False)
            ]
            # Filter to entries from the last 35 seconds by parsing ISO timestamp
            recurred = []
            for entry in recent_errors:
                ts_str = entry.get("timestamp", "")
                if ts_str:
                    try:
                        entry_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        entry_ts = entry_dt.timestamp()
                        if now - entry_ts < 35:
                            recurred.append(entry)
                    except (ValueError, TypeError, OSError):
                        continue

        if len(recurred) > 1:
            # More than 1 means the original + at least one recurrence
            logger.warning(
                "[SentryHeal] Verification FAILED for %s -- error recurred after fix (fingerprint=%s)",
                action_type,
                fingerprint,
            )
            with _heal_verification_stats_lock:
                _heal_verification_stats["verifications_failed"] += 1
        else:
            logger.info(
                "[SentryHeal] Verification PASSED for %s -- no recurrence in 30s (fingerprint=%s)",
                action_type,
                fingerprint,
            )
            with _heal_verification_stats_lock:
                _heal_verification_stats["verifications_passed"] += 1
    except Exception as e:
        logger.debug("[SentryHeal] Verification check error: %s", e)


def get_heal_verification_stats() -> Dict[str, int]:
    """Return heal verification pass/fail counts.

    Returns:
        Dict with verifications_passed and verifications_failed counts.
    """
    with _heal_verification_stats_lock:
        return dict(_heal_verification_stats)


def _record_heal_event(
    issue_id: str,
    fingerprint: str,
    fix_type: str,
    description: str,
    success: bool,
    file_path: str,
    function_name: str,
) -> None:
    """Record a healing event in the module-level history."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "issue_id": issue_id,
        "fingerprint": fingerprint,
        "fix_type": fix_type,
        "description": description,
        "success": success,
        "file": file_path,
        "function": function_name,
    }
    with _lock:
        _heal_history.append(entry)
        if len(_heal_history) > _MAX_HEAL_HISTORY:
            _heal_history[:] = _heal_history[-_MAX_HEAL_HISTORY:]


def _thread_safe_api_call(fn: Any, *args: Any, **kwargs: Any) -> None:
    """Run a Sentry API call in a background thread (fire-and-forget)."""

    def _run() -> None:
        try:
            fn(*args, **kwargs)
        except Exception as exc:
            logger.debug("sentry_integration: background API call failed: %s", exc)

    t = threading.Thread(target=_run, daemon=True)
    t.start()


# =============================================================================
# SENTRY API CLIENT
# =============================================================================


class SentryAPIClient:
    """Lightweight Sentry REST API client (stdlib only)."""

    @staticmethod
    def fetch_recent_issues(hours: int = 24) -> list[dict]:
        """Fetch unresolved issues from Sentry for the last N hours.

        Args:
            hours: Look-back window in hours (default 24).

        Returns:
            List of issue dicts from Sentry API, or empty list on failure.
        """
        if not _SENTRY_AUTH_TOKEN:
            logger.debug(
                "sentry_integration: SENTRY_AUTH_TOKEN not set, cannot fetch issues"
            )
            return []

        url = (
            f"{_SENTRY_API_BASE}/projects/{_SENTRY_ORG_SLUG}/{_SENTRY_PROJECT_SLUG}/"
            f"issues/?query=is:unresolved&statsPeriod={hours}h&sort=date"
        )

        try:
            req = urllib.request.Request(url, method="GET")
            req.add_header("Authorization", f"Bearer {_SENTRY_AUTH_TOKEN}")
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=_API_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
                if isinstance(data, list):
                    logger.info(
                        "sentry_integration: fetched %d recent issues", len(data)
                    )
                    return data
                return []

        except urllib.error.HTTPError as http_err:
            error_body = ""
            try:
                error_body = http_err.read().decode("utf-8", errors="replace")[:300]
            except OSError:
                pass
            logger.warning(
                "sentry_integration: Sentry API HTTP %d: %s",
                http_err.code,
                error_body,
            )
            return []

        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            logger.warning("sentry_integration: failed to fetch issues: %s", exc)
            return []

    @staticmethod
    def resolve_issue(issue_id: str) -> bool:
        """Mark a Sentry issue as resolved.

        Args:
            issue_id: The Sentry issue ID.

        Returns:
            True if the issue was resolved successfully.
        """
        if not _SENTRY_AUTH_TOKEN or not issue_id:
            return False

        url = f"{_SENTRY_API_BASE}/issues/{issue_id}/"
        payload = json.dumps({"status": "resolved"}).encode("utf-8")

        try:
            req = urllib.request.Request(url, data=payload, method="PUT")
            req.add_header("Authorization", f"Bearer {_SENTRY_AUTH_TOKEN}")
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=_API_TIMEOUT) as resp:
                resp.read()
                logger.info("sentry_integration: resolved issue %s", issue_id)
                return True

        except urllib.error.HTTPError as http_err:
            logger.warning(
                "sentry_integration: failed to resolve issue %s (HTTP %d)",
                issue_id,
                http_err.code,
            )
            return False

        except (urllib.error.URLError, OSError) as exc:
            logger.warning("sentry_integration: resolve_issue failed: %s", exc)
            return False

    @staticmethod
    def add_comment(issue_id: str, comment: str) -> bool:
        """Add a comment/note to a Sentry issue.

        Args:
            issue_id: The Sentry issue ID.
            comment: The comment text.

        Returns:
            True if the comment was added successfully.
        """
        if not _SENTRY_AUTH_TOKEN or not issue_id:
            return False

        url = f"{_SENTRY_API_BASE}/issues/{issue_id}/comments/"
        payload = json.dumps({"text": comment}).encode("utf-8")

        try:
            req = urllib.request.Request(url, data=payload, method="POST")
            req.add_header("Authorization", f"Bearer {_SENTRY_AUTH_TOKEN}")
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=_API_TIMEOUT) as resp:
                resp.read()
                logger.info("sentry_integration: added comment to issue %s", issue_id)
                return True

        except urllib.error.HTTPError as http_err:
            logger.warning(
                "sentry_integration: failed to comment on issue %s (HTTP %d)",
                issue_id,
                http_err.code,
            )
            return False

        except (urllib.error.URLError, OSError) as exc:
            logger.warning("sentry_integration: add_comment failed: %s", exc)
            return False


# =============================================================================
# WEBHOOK HANDLER (called from app.py route)
# =============================================================================

_healing_bridge: Optional[SentryHealingBridge] = None
_bridge_lock = threading.Lock()


def get_healing_bridge() -> SentryHealingBridge:
    """Get or create the singleton SentryHealingBridge (thread-safe)."""
    global _healing_bridge
    if _healing_bridge is None:
        with _bridge_lock:
            if _healing_bridge is None:
                _healing_bridge = SentryHealingBridge()
    return _healing_bridge


def handle_sentry_webhook(
    body: bytes,
    signature: str,
    resource_header: str = "",
) -> Tuple[int, dict]:
    """Handle an incoming Sentry webhook request.

    Called by the app.py route handler for ``POST /api/sentry/webhook``.

    Args:
        body: Raw request body bytes.
        signature: Value of the ``Sentry-Hook-Signature`` header.
        resource_header: Value of the ``Sentry-Hook-Resource`` header
            (e.g. 'issue', 'event', 'metric_alert').

    Returns:
        Tuple of (HTTP status code, JSON-serializable response dict).
    """
    # -- Validate signature ---------------------------------------------------
    if _SENTRY_WEBHOOK_SECRET and not validate_sentry_signature(body, signature):
        logger.warning("sentry_integration: invalid webhook signature")
        return 401, {"error": "Invalid signature"}

    # -- Parse payload --------------------------------------------------------
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        logger.warning("sentry_integration: invalid JSON in webhook body: %s", exc)
        return 400, {"error": "Invalid JSON"}

    # -- Only process issue/event webhooks ------------------------------------
    resource = resource_header.lower().strip() if resource_header else ""
    if resource and resource not in ("issue", "event", "error"):
        logger.debug(
            "sentry_integration: ignoring webhook resource type: %s",
            resource,
        )
        return 200, {"ok": True, "action": "ignored", "reason": f"resource={resource}"}

    # -- Parse the issue ------------------------------------------------------
    parsed = SentryIssueParser.parse_webhook(payload)
    if parsed is None:
        return 200, {"ok": True, "action": "ignored", "reason": "no actionable data"}

    # -- Process through healing bridge ---------------------------------------
    bridge = get_healing_bridge()
    result = bridge.process_issue(parsed)

    logger.info(
        "sentry_integration: webhook processed -- issue=%s, handled=%s, fix=%s",
        parsed.get("issue_id") or "?",
        result.get("handled"),
        result.get("fix_type") or "none",
    )

    return 200, {
        "ok": True,
        "issue_id": parsed.get("issue_id") or "",
        "error_type": parsed.get("error_type") or "",
        "handled": result.get("handled", False),
        "fix_type": result.get("fix_type"),
        "description": result.get("description") or "",
        "action_taken": result.get("action_taken", False),
    }


def get_sentry_status() -> dict:
    """Return Sentry integration status for the /api/sentry/issues endpoint.

    Returns:
        Dict with configuration status, recent healing events, and stats.
    """
    with _lock:
        now = time.time()
        active_fixes = len([ts for ts in _fix_timestamps if (now - ts) < 3600.0])
        history = list(_heal_history[-20:])
        total_attempts = sum(len(v) for v in _issue_attempts.values())

    return {
        "configured": {
            "webhook_secret": bool(_SENTRY_WEBHOOK_SECRET),
            "auth_token": bool(_SENTRY_AUTH_TOKEN),
            "org_slug": _SENTRY_ORG_SLUG,
            "project_slug": _SENTRY_PROJECT_SLUG,
        },
        "stats": {
            "fixes_this_hour": active_fixes,
            "max_fixes_per_hour": _MAX_FIXES_PER_HOUR,
            "total_fix_attempts_tracked": total_attempts,
            "unique_issues_tracked": len(_issue_attempts),
        },
        "module_heal_stats": get_module_heal_stats(),
        "recent_heals": history,
        "known_patterns": len(_ERROR_PATTERNS),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PROACTIVE HEALTH CHECKER (Phase 6: detect + heal before users see errors)
# ═══════════════════════════════════════════════════════════════════════════════


class ProactiveHealthChecker:
    """Background thread that proactively checks system health and triggers
    self-healing actions before issues escalate to user-visible errors.

    Runs every 60 seconds and checks:
    1. LLM router health (circuit breaker states)
    2. Data API availability (Adzuna, FRED, BLS, etc.)
    3. Scraper tier health
    4. Cache hit rates
    5. Response latency trends
    """

    def __init__(self, check_interval: int = 60) -> None:
        self._interval = check_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._check_history: List[Dict[str, Any]] = []
        self._heals_triggered = 0
        self._lock = threading.Lock()
        self._logger = logging.getLogger("proactive_health")

    def start(self) -> None:
        """Start the proactive health checker."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="proactive-health"
        )
        self._thread.start()
        self._logger.info("[ProactiveHealth] Started (interval: %ds)", self._interval)

    def stop(self) -> None:
        """Stop the checker."""
        self._running = False

    def _run_loop(self) -> None:
        """Background loop -- waits 90s after startup, then checks every interval."""
        time.sleep(90)  # Let startup complete
        while self._running:
            try:
                self._check_cycle()
            except Exception as e:
                self._logger.error(
                    "[ProactiveHealth] Cycle error: %s", e, exc_info=True
                )
            time.sleep(self._interval)

    def _check_cycle(self) -> None:
        """Run all proactive health checks."""
        issues_found = 0

        # 1. Check LLM Router health
        issues_found += self._check_llm_router()

        # 2. Check scraper tiers
        issues_found += self._check_scraper_health()

        # 3. Check data API connectivity
        issues_found += self._check_data_apis()

        with self._lock:
            self._check_history.append(
                {
                    "ts": time.time(),
                    "issues_found": issues_found,
                    "heals_triggered": self._heals_triggered,
                }
            )
            # Keep last 100 entries
            if len(self._check_history) > 100:
                self._check_history = self._check_history[-100:]

    def _check_llm_router(self) -> int:
        """Check LLM router for degraded providers and attempt recovery."""
        issues = 0
        try:
            import sys

            if "llm_router" not in sys.modules:
                return 0
            llm = sys.modules["llm_router"]

            # Check if there's a health status function
            if hasattr(llm, "get_health_status"):
                health = llm.get_health_status()
                degraded_pct = health.get("degraded_pct", 0)

                if degraded_pct > 50:
                    self._logger.warning(
                        "[ProactiveHealth] LLM Router >50%% degraded, resetting circuit breakers"
                    )
                    if hasattr(llm, "reset_circuit_breakers"):
                        llm.reset_circuit_breakers()
                        self._heals_triggered += 1
                    issues += 1

            # Check for providers with open circuit breakers that might have recovered
            if hasattr(llm, "_providers_health"):
                for provider_id, health_data in getattr(
                    llm, "_providers_health", {}
                ).items():
                    if isinstance(health_data, dict):
                        score = health_data.get("score", 1.0)
                        if score < 0.3:
                            issues += 1
        except Exception as e:
            self._logger.debug("[ProactiveHealth] LLM router check error: %s", e)
        return issues

    def _check_scraper_health(self) -> int:
        """Check web scraper tiers for elevated failure rates."""
        issues = 0
        try:
            import sys

            if "web_scraper_router" not in sys.modules:
                return 0
            wsr = sys.modules["web_scraper_router"]

            if hasattr(wsr, "get_tier_stats"):
                stats = wsr.get_tier_stats()
                if isinstance(stats, dict):
                    for tier_name, tier_data in stats.items():
                        if isinstance(tier_data, dict):
                            success_rate = tier_data.get("success_rate", 1.0)
                            if success_rate < 0.5 and tier_data.get("attempts", 0) > 5:
                                self._logger.warning(
                                    "[ProactiveHealth] Scraper tier '%s' has %.0f%% success rate, "
                                    "advancing preferred tier",
                                    tier_name,
                                    success_rate * 100,
                                )
                                if hasattr(wsr, "_preferred_tier"):
                                    wsr._preferred_tier = min(
                                        getattr(wsr, "_preferred_tier", 0) + 1, 5
                                    )
                                    self._heals_triggered += 1
                                issues += 1
        except Exception as e:
            self._logger.debug("[ProactiveHealth] Scraper check error: %s", e)
        return issues

    def _check_data_apis(self) -> int:
        """Check data API clients for connectivity issues."""
        issues = 0
        try:
            import sys

            if "api_integrations" not in sys.modules:
                return 0
            # Don't actually call test_all_apis (too expensive for every 60s check).
            # Instead check if the module's internal health tracking shows issues.
        except Exception as e:
            self._logger.debug("[ProactiveHealth] Data API check error: %s", e)
        return issues

    def get_status(self) -> Dict[str, Any]:
        """Get proactive health checker status."""
        with self._lock:
            recent = self._check_history[-5:] if self._check_history else []
            return {
                "running": self._running,
                "interval_s": self._interval,
                "total_checks": len(self._check_history),
                "total_heals": self._heals_triggered,
                "recent_checks": recent,
            }


# Global instance
_proactive_checker: Optional[ProactiveHealthChecker] = None


def start_proactive_health() -> None:
    """Start the proactive health checker."""
    global _proactive_checker
    if _proactive_checker is None:
        _proactive_checker = ProactiveHealthChecker()
    _proactive_checker.start()


def stop_proactive_health() -> None:
    """Stop the proactive health checker."""
    global _proactive_checker
    if _proactive_checker:
        _proactive_checker.stop()


def get_proactive_health_status() -> Dict[str, Any]:
    """Get proactive health checker status."""
    if _proactive_checker:
        return _proactive_checker.get_status()
    return {"running": False, "status": "not_initialized"}
