"""
ab_testing.py -- A/B Testing for LLM Provider Quality (v1.0)

Compares response quality across LLM providers to continuously find
the best provider for each query type.  Uses session-ID-based hashing
for deterministic, consistent variant assignment so a given session
always sees the same provider throughout the experiment.

Thread-safe, stdlib-only.

Experiments:
    - chat_provider:    claude_haiku vs gemini vs groq (standard queries)
    - complex_provider: claude_haiku vs claude (complex queries)
    - Traffic: 10% of queries participate in experiments

Usage:
    from ab_testing import get_ab_manager

    mgr = get_ab_manager()
    variant = mgr.get_variant("chat_provider", session_id="abc123")
    if variant:
        # Override provider selection with experiment variant
        result = call_llm(..., force_provider=variant)
        mgr.record_result("chat_provider", variant, {
            "quality_score": 0.85,
            "response_time_ms": 450,
            "tools_used": 3,
            "citations_count": 4,
            "query_type": "salary",
        })
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Maximum results per experiment:variant to keep in memory (cap for safety)
_MAX_RESULTS_PER_VARIANT: int = 500


class ABTestManager:
    """Manages A/B test experiments for LLM provider quality comparison.

    Thread-safe.  Each experiment randomly assigns a percentage of traffic
    to test variants, and records quality metrics for analysis.
    """

    def __init__(self) -> None:
        self._experiments: dict[str, dict[str, Any]] = {}
        self._results: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._lock: threading.Lock = threading.Lock()
        self._created_at: str = datetime.now(timezone.utc).isoformat()

    def create_experiment(
        self,
        name: str,
        variants: list[str],
        traffic_pct: float = 0.1,
        description: str = "",
    ) -> None:
        """Create an A/B test experiment.

        Args:
            name: Experiment name (e.g., 'chat_provider').
            variants: List of provider IDs to test.
            traffic_pct: Fraction of traffic to include (0.1 = 10%).
            description: Human-readable description of the experiment.
        """
        if not name or not variants:
            logger.warning(
                "AB Test: cannot create experiment with empty name or variants"
            )
            return
        if not 0.0 < traffic_pct <= 1.0:
            logger.warning(
                "AB Test: traffic_pct %.2f out of range (0, 1], clamping to 0.1",
                traffic_pct,
            )
            traffic_pct = 0.1

        with self._lock:
            self._experiments[name] = {
                "variants": list(variants),
                "traffic_pct": traffic_pct,
                "description": description or f"A/B test: {' vs '.join(variants)}",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "enabled": True,
            }
        logger.info(
            "AB Test: created experiment '%s' with variants=%s, traffic=%.0f%%",
            name,
            variants,
            traffic_pct * 100,
        )

    def disable_experiment(self, name: str) -> bool:
        """Disable an experiment (stops assigning new sessions).

        Args:
            name: Experiment name.

        Returns:
            True if found and disabled, False if not found.
        """
        with self._lock:
            exp = self._experiments.get(name)
            if not exp:
                return False
            exp["enabled"] = False
        logger.info("AB Test: disabled experiment '%s'", name)
        return True

    def enable_experiment(self, name: str) -> bool:
        """Re-enable a disabled experiment.

        Args:
            name: Experiment name.

        Returns:
            True if found and enabled, False if not found.
        """
        with self._lock:
            exp = self._experiments.get(name)
            if not exp:
                return False
            exp["enabled"] = True
        logger.info("AB Test: enabled experiment '%s'", name)
        return True

    def get_variant(self, experiment_name: str, session_id: str) -> Optional[str]:
        """Get the assigned variant for a session, or None if not in experiment.

        Uses a deterministic hash of session_id + experiment_name so the same
        session always gets the same variant assignment across calls.

        Args:
            experiment_name: Name of the experiment.
            session_id: Unique session identifier.

        Returns:
            Provider ID string if session is in the experiment, None otherwise.
        """
        with self._lock:
            exp = self._experiments.get(experiment_name)
            if not exp or not exp.get("enabled", True):
                return None
            variants = exp["variants"]
            traffic_pct = exp["traffic_pct"]

        if not variants or not session_id:
            return None

        # Deterministic hash for traffic gating
        gate_hash = int(
            hashlib.sha256(f"{session_id}:gate".encode("utf-8")).hexdigest()[:8],
            16,
        )
        if (gate_hash % 1000) >= int(traffic_pct * 1000):
            return None  # Not in experiment

        # Deterministic hash for variant assignment
        variant_hash = int(
            hashlib.sha256(
                f"{session_id}:{experiment_name}".encode("utf-8")
            ).hexdigest()[:8],
            16,
        )
        variant_idx = variant_hash % len(variants)
        return variants[variant_idx]

    def record_result(
        self,
        experiment_name: str,
        variant: str,
        metrics: dict[str, Any],
    ) -> None:
        """Record quality metrics for an experiment variant.

        Args:
            experiment_name: Name of the experiment.
            variant: The provider ID variant that was used.
            metrics: Dict with quality signals:
                - quality_score (float): 0.0-1.0 from _enrich_response_quality
                - response_time_ms (float): end-to-end latency in ms
                - tools_used (int): number of tools called
                - citations_count (int): number of source citations
                - query_type (str): classified query type (salary, media_plan, etc.)
                - word_count (int): response word count
                - has_tables (bool): whether response contains markdown tables
        """
        key = f"{experiment_name}:{variant}"
        record = {
            "timestamp": time.time(),
            **metrics,
        }
        with self._lock:
            bucket = self._results[key]
            bucket.append(record)
            # Cap to prevent unbounded memory growth
            if len(bucket) > _MAX_RESULTS_PER_VARIANT:
                self._results[key] = bucket[-_MAX_RESULTS_PER_VARIANT:]

    def get_experiment_names(self) -> list[str]:
        """Return list of all experiment names.

        Returns:
            List of experiment name strings.
        """
        with self._lock:
            return list(self._experiments.keys())

    def get_report(self) -> dict[str, Any]:
        """Get a full A/B test results report.

        Returns:
            Dict with experiment metadata and per-variant statistics including
            sample size, average quality score, response time, tool usage, etc.
        """
        report: dict[str, Any] = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "experiments": {},
        }

        with self._lock:
            for exp_name, exp_config in self._experiments.items():
                exp_report: dict[str, Any] = {
                    "description": exp_config.get("description") or "",
                    "traffic_pct": exp_config["traffic_pct"],
                    "enabled": exp_config.get("enabled", True),
                    "created_at": exp_config.get("created_at") or "",
                    "variants": {},
                }
                for variant in exp_config["variants"]:
                    key = f"{exp_name}:{variant}"
                    results = self._results.get(key, [])
                    exp_report["variants"][variant] = _compute_variant_stats(results)

                # Determine winner (variant with highest avg quality)
                best_variant = ""
                best_quality = -1.0
                for v_name, v_stats in exp_report["variants"].items():
                    if (
                        v_stats["sample_size"] >= 5
                        and v_stats["avg_quality_score"] > best_quality
                    ):
                        best_quality = v_stats["avg_quality_score"]
                        best_variant = v_name
                exp_report["leading_variant"] = best_variant or "insufficient_data"
                exp_report["leading_quality"] = (
                    round(best_quality, 3) if best_variant else 0.0
                )

                report["experiments"][exp_name] = exp_report

        return report

    def get_report_by_query_type(self) -> dict[str, Any]:
        """Get A/B test results broken down by query type.

        Returns:
            Dict mapping query_type -> experiment -> variant -> stats.
        """
        by_type: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: defaultdict(list)
        )

        with self._lock:
            for key, results in self._results.items():
                for r in results:
                    qt = r.get("query_type") or "unknown"
                    by_type[qt][key].append(r)

        report: dict[str, Any] = {}
        for qt, variant_data in by_type.items():
            report[qt] = {}
            for key, results in variant_data.items():
                report[qt][key] = _compute_variant_stats(results)

        return report

    def reset_experiment(self, name: str) -> bool:
        """Clear all results for an experiment (keeps config).

        Args:
            name: Experiment name.

        Returns:
            True if experiment exists and was reset, False otherwise.
        """
        with self._lock:
            if name not in self._experiments:
                return False
            variants = self._experiments[name]["variants"]
            for v in variants:
                key = f"{name}:{v}"
                self._results.pop(key, None)
        logger.info("AB Test: reset results for experiment '%s'", name)
        return True


def _compute_variant_stats(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute aggregate statistics for a list of experiment results.

    Args:
        results: List of metric dicts recorded for a single variant.

    Returns:
        Dict with sample_size, averages, and distribution info.
    """
    n = len(results)
    if n == 0:
        return {
            "sample_size": 0,
            "avg_quality_score": 0.0,
            "avg_response_time_ms": 0.0,
            "avg_tools_used": 0.0,
            "avg_citations": 0.0,
            "avg_word_count": 0.0,
            "pct_with_tables": 0.0,
            "query_type_distribution": {},
        }

    def _safe_avg(key: str) -> float:
        vals = [r.get(key, 0) for r in results if isinstance(r.get(key), (int, float))]
        return round(sum(vals) / max(len(vals), 1), 3)

    # Query type distribution
    qt_counts: dict[str, int] = {}
    for r in results:
        qt = r.get("query_type") or "unknown"
        qt_counts[qt] = qt_counts.get(qt, 0) + 1

    table_count = sum(1 for r in results if r.get("has_tables"))

    # Quality score percentiles (simple approach)
    quality_scores = sorted(
        [
            r.get("quality_score", 0)
            for r in results
            if isinstance(r.get("quality_score"), (int, float))
        ]
    )
    p25 = quality_scores[len(quality_scores) // 4] if quality_scores else 0.0
    p50 = quality_scores[len(quality_scores) // 2] if quality_scores else 0.0
    p75 = quality_scores[(len(quality_scores) * 3) // 4] if quality_scores else 0.0

    return {
        "sample_size": n,
        "avg_quality_score": _safe_avg("quality_score"),
        "avg_response_time_ms": _safe_avg("response_time_ms"),
        "avg_tools_used": _safe_avg("tools_used"),
        "avg_citations": _safe_avg("citations_count"),
        "avg_word_count": _safe_avg("word_count"),
        "pct_with_tables": round(table_count / n * 100, 1) if n else 0.0,
        "quality_p25": round(p25, 3),
        "quality_p50": round(p50, 3),
        "quality_p75": round(p75, 3),
        "query_type_distribution": qt_counts,
        "oldest_result": min((r.get("timestamp", 0) for r in results), default=0),
        "newest_result": max((r.get("timestamp", 0) for r in results), default=0),
    }


# ---------------------------------------------------------------------------
# Module-level singleton + default experiments
# ---------------------------------------------------------------------------

_ab_manager: Optional[ABTestManager] = None
_ab_init_lock: threading.Lock = threading.Lock()


def get_ab_manager() -> ABTestManager:
    """Get the singleton ABTestManager instance with default experiments.

    Creates default experiments on first call:
        - chat_provider: claude_haiku vs gemini vs groq (10% traffic)
        - complex_provider: claude_haiku vs claude (10% traffic)

    Returns:
        The singleton ABTestManager instance.
    """
    global _ab_manager
    if _ab_manager is not None:
        return _ab_manager

    with _ab_init_lock:
        if _ab_manager is not None:
            return _ab_manager

        mgr = ABTestManager()

        # Default experiment 1: Standard chat queries
        mgr.create_experiment(
            name="chat_provider",
            variants=["claude_haiku", "gemini", "groq"],
            traffic_pct=0.1,
            description="Compare claude_haiku vs gemini vs groq for standard chat queries",
        )

        # Default experiment 2: Complex/analytical queries
        mgr.create_experiment(
            name="complex_provider",
            variants=["claude_haiku", "claude"],
            traffic_pct=0.1,
            description="Compare claude_haiku vs claude_sonnet for complex analytical queries",
        )

        _ab_manager = mgr
        logger.info("AB Test: initialized manager with 2 default experiments")
        return _ab_manager
