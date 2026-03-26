"""Anomaly Detector -- Statistical baseline tracker with 3-sigma alerting.

Tracks key metrics (request latency, error rate, memory usage, response size)
using a rolling window. Flags values that exceed mean + 3 standard deviations.

Thread-safe: all shared state guarded by a single lock.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WINDOW_SECONDS: int = 3600  # 1-hour rolling window
MAX_SAMPLES: int = 5000  # Max data points per metric
SIGMA_THRESHOLD: float = 3.0  # Alert when value > mean + 3*std
MIN_SAMPLES_FOR_DETECTION: int = 30  # Need enough data for meaningful stats
ALERT_COOLDOWN_SECONDS: float = 300.0  # 5-minute cooldown between duplicate alerts
STARTUP_GRACE_SECONDS: float = 300.0  # Suppress anomaly alerts for 5 min after startup

# Well-known metric names
METRIC_REQUEST_LATENCY = "request_latency_ms"
METRIC_ERROR_RATE = "error_rate_pct"
METRIC_MEMORY_USAGE = "memory_usage_mb"
METRIC_RESPONSE_SIZE = "response_size_bytes"


# ---------------------------------------------------------------------------
# Anomaly Detector
# ---------------------------------------------------------------------------


class _MetricWindow:
    """Rolling window of timestamped values for a single metric."""

    __slots__ = ("values", "last_alert_time")

    def __init__(self) -> None:
        self.values: deque[tuple[float, float]] = deque(maxlen=MAX_SAMPLES)
        self.last_alert_time: float = 0.0

    def add(self, value: float, now: float) -> None:
        """Append a value with its timestamp."""
        self.values.append((now, value))

    def prune(self, cutoff: float) -> None:
        """Remove entries older than cutoff."""
        while self.values and self.values[0][0] < cutoff:
            self.values.popleft()

    def stats(self) -> tuple[float, float, int]:
        """Compute mean and standard deviation.

        Returns:
            Tuple of (mean, std_dev, sample_count).
        """
        if not self.values:
            return 0.0, 0.0, 0
        vals = [v for _, v in self.values]
        n = len(vals)
        if n == 0:
            return 0.0, 0.0, 0
        mean = sum(vals) / n
        if n < 2:
            return mean, 0.0, n
        variance = sum((x - mean) ** 2 for x in vals) / (n - 1)
        return mean, math.sqrt(variance), n


class AnomalyDetector:
    """Thread-safe singleton for statistical anomaly detection.

    Usage:
        detector = AnomalyDetector.instance()
        detector.record_metric("request_latency_ms", 1250.0)
        result = detector.check_anomaly("request_latency_ms")
        baselines = detector.get_baselines()
    """

    _instance: Optional["AnomalyDetector"] = None
    _init_lock = threading.Lock()

    def __new__(cls) -> "AnomalyDetector":
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    inst = super().__new__(cls)
                    inst._initialized = False
                    cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._lock = threading.Lock()
        self._metrics: dict[str, _MetricWindow] = defaultdict(_MetricWindow)
        self._active_anomalies: dict[str, dict[str, Any]] = {}
        self._startup_time: float = time.time()

    @classmethod
    def instance(cls) -> "AnomalyDetector":
        """Return the singleton instance."""
        return cls()

    def record_metric(self, name: str, value: float) -> None:
        """Record a data point for a named metric.

        Args:
            name: Metric name (e.g. 'request_latency_ms').
            value: Numeric value to record.
        """
        now = time.time()
        with self._lock:
            window = self._metrics[name]
            window.add(value, now)
            # Prune old data
            cutoff = now - WINDOW_SECONDS
            window.prune(cutoff)

    def check_anomaly(self, name: str) -> dict[str, Any]:
        """Check if the latest values for a metric are anomalous.

        Uses mean + 3 sigma from the rolling window baseline.

        Args:
            name: Metric name to check.

        Returns:
            Dict with is_anomaly, value, mean, std_dev, threshold, etc.
        """
        now = time.time()
        with self._lock:
            window = self._metrics.get(name)
            if window is None:
                return {
                    "metric": name,
                    "is_anomaly": False,
                    "reason": "no_data",
                    "sample_count": 0,
                }

            cutoff = now - WINDOW_SECONDS
            window.prune(cutoff)
            mean, std_dev, count = window.stats()

            if count < MIN_SAMPLES_FOR_DETECTION:
                return {
                    "metric": name,
                    "is_anomaly": False,
                    "reason": "insufficient_samples",
                    "sample_count": count,
                    "min_required": MIN_SAMPLES_FOR_DETECTION,
                    "mean": round(mean, 2),
                }

            # Suppress anomaly detection during startup grace period
            if now - self._startup_time < STARTUP_GRACE_SECONDS:
                return {
                    "metric": name,
                    "is_anomaly": False,
                    "reason": "startup_grace_period",
                    "sample_count": count,
                    "mean": round(mean, 2),
                    "grace_remaining_s": round(
                        STARTUP_GRACE_SECONDS - (now - self._startup_time), 1
                    ),
                }

            # Get most recent value
            latest_value = window.values[-1][1] if window.values else 0.0
            threshold = mean + (SIGMA_THRESHOLD * std_dev)
            is_anomaly = latest_value > threshold and std_dev > 0

            result: dict[str, Any] = {
                "metric": name,
                "is_anomaly": is_anomaly,
                "latest_value": round(latest_value, 2),
                "mean": round(mean, 2),
                "std_dev": round(std_dev, 2),
                "threshold": round(threshold, 2),
                "sigma_multiplier": SIGMA_THRESHOLD,
                "sample_count": count,
            }

            if is_anomaly:
                deviation = (latest_value - mean) / std_dev if std_dev > 0 else 0.0
                result["deviation_sigmas"] = round(deviation, 2)

                # Update active anomalies (with cooldown logging)
                if now - window.last_alert_time > ALERT_COOLDOWN_SECONDS:
                    window.last_alert_time = now
                    logger.warning(
                        f"Anomaly detected: {name}={latest_value:.2f} "
                        f"(threshold={threshold:.2f}, "
                        f"mean={mean:.2f}, std={std_dev:.2f}, "
                        f"{deviation:.1f} sigmas)"
                    )

                self._active_anomalies[name] = {
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                    "value": round(latest_value, 2),
                    "threshold": round(threshold, 2),
                    "deviation_sigmas": round(deviation, 2),
                }
            else:
                # Clear resolved anomaly
                self._active_anomalies.pop(name, None)

            return result

    def check_all_anomalies(self) -> dict[str, Any]:
        """Check all tracked metrics for anomalies.

        Returns:
            Dict with per-metric results and summary.
        """
        results: dict[str, dict[str, Any]] = {}
        with self._lock:
            metric_names = list(self._metrics.keys())

        # Release lock, then check each metric individually
        for name in metric_names:
            results[name] = self.check_anomaly(name)

        anomalies = {k: v for k, v in results.items() if v.get("is_anomaly")}
        return {
            "anomaly_count": len(anomalies),
            "total_metrics": len(results),
            "anomalies": anomalies,
            "all_metrics": results,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "window_seconds": WINDOW_SECONDS,
            "sigma_threshold": SIGMA_THRESHOLD,
        }

    def get_baselines(self) -> dict[str, dict[str, Any]]:
        """Get current baseline statistics for all tracked metrics.

        Returns:
            Dict mapping metric name to {mean, std_dev, sample_count, window}.
        """
        now = time.time()
        cutoff = now - WINDOW_SECONDS
        baselines: dict[str, dict[str, Any]] = {}

        with self._lock:
            for name, window in self._metrics.items():
                window.prune(cutoff)
                mean, std_dev, count = window.stats()
                baselines[name] = {
                    "mean": round(mean, 2),
                    "std_dev": round(std_dev, 2),
                    "sample_count": count,
                    "upper_threshold": round(mean + SIGMA_THRESHOLD * std_dev, 2),
                    "lower_threshold": round(
                        max(0, mean - SIGMA_THRESHOLD * std_dev), 2
                    ),
                }

        return {
            "baselines": baselines,
            "window_seconds": WINDOW_SECONDS,
            "sigma_threshold": SIGMA_THRESHOLD,
            "min_samples": MIN_SAMPLES_FOR_DETECTION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def get_active_anomalies(self) -> dict[str, dict[str, Any]]:
        """Get currently active (unresolved) anomalies.

        Returns:
            Dict of metric_name -> anomaly details.
        """
        with self._lock:
            return dict(self._active_anomalies)


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------


def get_anomaly_detector() -> AnomalyDetector:
    """Return the global AnomalyDetector singleton."""
    return AnomalyDetector.instance()


def record_metric(name: str, value: float) -> None:
    """Record a metric data point (convenience wrapper).

    Args:
        name: Metric name.
        value: Numeric value.
    """
    get_anomaly_detector().record_metric(name, value)


def check_anomaly(name: str) -> dict[str, Any]:
    """Check a single metric for anomalies (convenience wrapper).

    Args:
        name: Metric name.

    Returns:
        Anomaly check result dict.
    """
    return get_anomaly_detector().check_anomaly(name)


def get_baselines() -> dict[str, dict[str, Any]]:
    """Get all baselines (convenience wrapper)."""
    return get_anomaly_detector().get_baselines()
