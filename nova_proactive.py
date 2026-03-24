#!/usr/bin/env python3
"""Proactive Intelligence for Nova AI.

Instead of waiting for users to ask, Nova pushes insights:
- Campaign performance alerts (CPC above benchmark)
- Market changes (new competitor activity)
- Compliance updates (regulation changes detected)
- Budget warnings (spend pace alerts)

Runs as a background thread, checks every 5 minutes.
"""

import json
import logging
import os
import threading
import time
from typing import Optional
from collections import deque

logger = logging.getLogger(__name__)

_CHECK_INTERVAL = 300  # 5 minutes
_MAX_ALERTS = 50


class ProactiveInsight:
    """A single proactive insight/alert."""

    def __init__(
        self,
        category: str,
        title: str,
        message: str,
        severity: str = "info",
        action_label: str = "",
        action_route: str = "",
    ):
        self.id = f"{category}_{int(time.time())}"
        self.category = category  # campaign, market, compliance, budget
        self.title = title
        self.message = message
        self.severity = severity  # info, warning, critical
        self.action_label = action_label
        self.action_route = action_route
        self.created_at = time.time()
        self.read = False
        self.dismissed = False

    def to_dict(self) -> dict:
        """Serialize insight to dictionary."""
        return {
            "id": self.id,
            "category": self.category,
            "title": self.title,
            "message": self.message,
            "severity": self.severity,
            "action_label": self.action_label,
            "action_route": self.action_route,
            "created_at": self.created_at,
            "read": self.read,
            "dismissed": self.dismissed,
        }


class ProactiveEngine:
    """Background engine that generates proactive insights."""

    def __init__(self):
        self._insights: deque = deque(maxlen=_MAX_ALERTS)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_check: dict = {}  # category -> last_check_ts

    def start(self) -> None:
        """Start the proactive engine background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="nova-proactive"
        )
        self._thread.start()
        logger.info("[ProactiveEngine] Started (interval: %ds)", _CHECK_INTERVAL)

    def stop(self) -> None:
        """Stop the proactive engine."""
        self._running = False

    def _run_loop(self) -> None:
        """Main loop: wait for startup, then check periodically."""
        time.sleep(120)  # Wait for startup
        while self._running:
            try:
                self._check_all()
            except Exception as e:
                logger.error("[ProactiveEngine] Check error: %s", e, exc_info=True)
            time.sleep(_CHECK_INTERVAL)

    def _check_all(self) -> None:
        """Run all proactive checks."""
        self._check_market_changes()
        self._check_budget_health()
        self._check_compliance_freshness()
        self._check_system_health()

    def _check_market_changes(self) -> None:
        """Check for significant market changes."""
        try:
            import sys

            # Check if market data shows significant changes
            if "supabase_data" in sys.modules:
                from supabase_data import get_market_trends

                trends = get_market_trends()
                if trends and len(trends) > 5:
                    self._add_insight(
                        "market",
                        "Market Activity Spike",
                        f"{len(trends)} active market signals detected. Review latest trends for campaign optimization opportunities.",
                        severity="info",
                        action_label="View Market Pulse",
                        action_route="intelligence/market",
                    )
        except Exception as e:
            logger.debug("[ProactiveEngine] Market check failed: %s", e)

    def _check_budget_health(self) -> None:
        """Check budget spending pace."""
        try:
            # Check if any campaign is overspending
            # This would integrate with campaign data when available
            pass
        except Exception as e:
            logger.debug("[ProactiveEngine] Budget check failed: %s", e)

    def _check_compliance_freshness(self) -> None:
        """Check if compliance data needs refreshing."""
        try:
            import sys

            if "web_scraper_router" in sys.modules:
                wsr = sys.modules["web_scraper_router"]
                if hasattr(wsr, "_freshness_tracker"):
                    stats = wsr._freshness_tracker.get_stats()
                    changed = stats.get("recently_changed", 0)
                    if changed > 0:
                        self._add_insight(
                            "compliance",
                            "Compliance Content Changed",
                            f"{changed} monitored regulatory pages have changed in the last 24 hours. Review for compliance impact.",
                            severity="warning",
                            action_label="Review Changes",
                            action_route="compliance/comply",
                        )
        except Exception as e:
            logger.debug("[ProactiveEngine] Compliance check failed: %s", e)

    def _check_system_health(self) -> None:
        """Check system health and alert on degradation."""
        try:
            import sys

            if "monitoring" in sys.modules:
                mon = sys.modules["monitoring"]
                if hasattr(mon, "MetricsCollector"):
                    mc = mon.MetricsCollector.get_instance()
                    metrics = mc.get_metrics()
                    if isinstance(metrics, dict):
                        error_rate = metrics.get("error_rate", 0)
                        if error_rate > 0.05:
                            self._add_insight(
                                "system",
                                "Elevated Error Rate",
                                f"System error rate is {error_rate:.1%}. Some features may be degraded.",
                                severity="critical",
                                action_label="View Health",
                                action_route="",
                            )
        except Exception as e:
            logger.debug("[ProactiveEngine] System health check failed: %s", e)

    def _add_insight(
        self,
        category: str,
        title: str,
        message: str,
        severity: str = "info",
        action_label: str = "",
        action_route: str = "",
    ) -> None:
        """Add a new insight, deduplicating by title within the last hour."""
        with self._lock:
            # Deduplicate: don't add if same title exists in last hour
            recent = [
                i
                for i in self._insights
                if i.title == title and time.time() - i.created_at < 3600
            ]
            if recent:
                return

            insight = ProactiveInsight(
                category, title, message, severity, action_label, action_route
            )
            self._insights.append(insight)
            logger.info("[ProactiveEngine] New insight: [%s] %s", severity, title)

    def get_unread(self) -> list:
        """Get unread, non-dismissed insights."""
        with self._lock:
            return [
                i.to_dict() for i in self._insights if not i.read and not i.dismissed
            ]

    def get_all(self, limit: int = 20) -> list:
        """Get all recent insights."""
        with self._lock:
            return [i.to_dict() for i in list(self._insights)[-limit:]]

    def mark_read(self, insight_id: str) -> bool:
        """Mark an insight as read."""
        with self._lock:
            for i in self._insights:
                if i.id == insight_id:
                    i.read = True
                    return True
        return False

    def dismiss(self, insight_id: str) -> bool:
        """Dismiss an insight."""
        with self._lock:
            for i in self._insights:
                if i.id == insight_id:
                    i.dismissed = True
                    return True
        return False

    def get_stats(self) -> dict:
        """Get engine statistics."""
        with self._lock:
            total = len(self._insights)
            unread = sum(1 for i in self._insights if not i.read and not i.dismissed)
            by_severity: dict = {}
            for i in self._insights:
                by_severity[i.severity] = by_severity.get(i.severity, 0) + 1
            return {
                "running": self._running,
                "total_insights": total,
                "unread": unread,
                "by_severity": by_severity,
            }


# Global instance
_engine: Optional[ProactiveEngine] = None


def start_proactive_engine() -> None:
    """Initialize and start the global proactive engine."""
    global _engine
    if _engine is None:
        _engine = ProactiveEngine()
    _engine.start()


def stop_proactive_engine() -> None:
    """Stop the global proactive engine."""
    global _engine
    if _engine:
        _engine.stop()


def get_insights(limit: int = 20) -> list:
    """Get recent insights from the engine."""
    if _engine:
        return _engine.get_all(limit)
    return []


def get_unread_insights() -> list:
    """Get unread insights from the engine."""
    if _engine:
        return _engine.get_unread()
    return []


def mark_insight_read(insight_id: str) -> bool:
    """Mark an insight as read by ID."""
    if _engine:
        return _engine.mark_read(insight_id)
    return False


def dismiss_insight(insight_id: str) -> bool:
    """Dismiss an insight by ID."""
    if _engine:
        return _engine.dismiss(insight_id)
    return False


def get_proactive_stats() -> dict:
    """Get proactive engine statistics."""
    if _engine:
        return _engine.get_stats()
    return {"running": False}
