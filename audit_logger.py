#!/usr/bin/env python3
"""Audit Logger for Nova AI Suite.

Records security-relevant events: API access, data exports,
admin actions, configuration changes, auth failures.
"""

import hashlib
import json
import logging
import os
import threading
import time
from collections import deque
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_AUDIT_LOG: deque = deque(maxlen=5000)
_lock = threading.Lock()
_LOG_FILE = Path(os.environ.get("AUDIT_LOG_PATH", "data/audit_log.jsonl"))


def log_event(
    action: str,
    actor: str = "system",
    resource: str = "",
    details: Optional[dict] = None,
    ip_address: str = "",
    severity: str = "info",
) -> None:
    """Record an audit event.

    Args:
        action: What happened (e.g., "api.access", "data.export", "auth.failure")
        actor: Who did it (user ID, API key name, "system")
        resource: What was affected (endpoint path, file name, etc.)
        details: Additional context
        ip_address: Client IP (hashed for privacy)
        severity: info, warning, critical
    """
    event = {
        "ts": time.time(),
        "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "actor": actor,
        "resource": resource,
        "severity": severity,
        "ip_hash": (
            hashlib.sha256(ip_address.encode()).hexdigest()[:12] if ip_address else ""
        ),
    }
    if details:
        event["details"] = details

    with _lock:
        _AUDIT_LOG.append(event)

    # Async file write
    try:
        _LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        logger.debug("Audit log file write failed: %s", e)

    # Log critical events
    if severity == "critical":
        logger.warning("[AUDIT] CRITICAL: %s by %s on %s", action, actor, resource)


def get_recent_events(limit: int = 100, action_filter: Optional[str] = None) -> list:
    """Get recent audit events."""
    with _lock:
        events = list(_AUDIT_LOG)

    if action_filter:
        events = [e for e in events if action_filter in e.get("action", "")]

    return events[-limit:]


def get_audit_summary() -> dict:
    """Get audit event summary."""
    with _lock:
        events = list(_AUDIT_LOG)

    if not events:
        return {"total_events": 0}

    action_counts: dict[str, int] = {}
    severity_counts = {"info": 0, "warning": 0, "critical": 0}
    for e in events:
        action = e.get("action", "unknown")
        action_counts[action] = action_counts.get(action, 0) + 1
        sev = e.get("severity", "info")
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    return {
        "total_events": len(events),
        "by_action": dict(sorted(action_counts.items(), key=lambda x: -x[1])[:10]),
        "by_severity": severity_counts,
        "oldest_event": events[0].get("iso", ""),
        "newest_event": events[-1].get("iso", ""),
    }
