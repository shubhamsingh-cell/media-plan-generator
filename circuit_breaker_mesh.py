"""Circuit Breaker Mesh for LLM providers.

Provides per-provider health scoring with three-state circuit breakers
(CLOSED, HALF_OPEN, OPEN) and exponential backoff for retry timing.
Thread-safe and designed as a global singleton via get_circuit_mesh().

Health score (0-100) is computed from:
  - Success rate (60% weight)
  - Latency percentile (20% weight)
  - Recency of last success (20% weight)
"""

from __future__ import annotations

import enum
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CircuitState(enum.Enum):
    """Three-state circuit breaker model."""

    CLOSED = "closed"  # Healthy -- requests flow normally
    HALF_OPEN = "half_open"  # Testing -- allow limited requests after cooldown
    OPEN = "open"  # Tripped -- all requests blocked until cooldown expires


@dataclass
class ProviderCircuit:
    """Per-provider circuit breaker with health scoring.

    Tracks success/failure counts, latency history, and manages
    state transitions with exponential backoff on repeated failures.
    """

    name: str

    # Circuit state
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    # Timestamps
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    last_state_change: float = field(default_factory=time.time)
    circuit_opened_at: float = 0.0

    # Latency tracking (rolling window of recent latencies in ms)
    _latencies: deque = field(default_factory=lambda: deque(maxlen=50))

    # Configurable thresholds
    failure_threshold: int = 5  # Consecutive failures to trip OPEN
    success_threshold: int = 2  # Consecutive successes in HALF_OPEN to close
    open_timeout: float = 30.0  # Base cooldown in seconds before HALF_OPEN
    max_open_timeout: float = 300.0  # Cap on exponential backoff (5 min)
    latency_threshold_ms: float = 5000.0  # Latency above this is "slow"

    # Lock for thread safety
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def health_score(self) -> float:
        """Compute health score 0-100 based on success rate, latency, and recency.

        Weighting:
          - Success rate: 60% (ratio of successes to total calls)
          - Latency: 20% (how fast relative to threshold)
          - Recency: 20% (how recently the last success occurred)

        Returns:
            Float between 0.0 and 100.0.
        """
        with self._lock:
            # --- OPEN circuit is definitionally unhealthy ---
            if self.state == CircuitState.OPEN:
                return 0.0

            total = self.success_count + self.failure_count
            if total == 0:
                # No data yet -- assume healthy (benefit of the doubt)
                return 80.0

            # 1) Success rate component (0-60)
            success_rate = self.success_count / total
            success_component = success_rate * 60.0

            # 2) Latency component (0-20)
            if self._latencies:
                avg_latency = sum(self._latencies) / len(self._latencies)
                # Score inversely proportional to latency vs threshold
                # At 0ms -> 20, at threshold -> 10, at 2x threshold -> 0
                latency_ratio = min(
                    avg_latency / max(self.latency_threshold_ms, 1.0), 2.0
                )
                latency_component = max(0.0, 20.0 * (1.0 - latency_ratio / 2.0))
            else:
                latency_component = 15.0  # No data -- moderate score

            # 3) Recency component (0-20)
            now = time.time()
            if self.last_success_time > 0:
                seconds_since_success = now - self.last_success_time
                # Full marks if success within 60s, decays over 10 minutes
                recency_ratio = min(seconds_since_success / 600.0, 1.0)
                recency_component = 20.0 * (1.0 - recency_ratio)
            else:
                recency_component = 5.0  # Never succeeded -- low but non-zero

            # HALF_OPEN penalty: reduce score by 30% to prefer healthy providers
            score = success_component + latency_component + recency_component
            if self.state == CircuitState.HALF_OPEN:
                score *= 0.7

            return round(min(100.0, max(0.0, score)), 1)

    def _compute_backoff(self) -> float:
        """Compute exponential backoff: open_timeout * 2^(failures/threshold).

        Capped at max_open_timeout.

        Returns:
            Backoff duration in seconds.
        """
        exponent = self.consecutive_failures / max(self.failure_threshold, 1)
        backoff = self.open_timeout * (2.0**exponent)
        return min(backoff, self.max_open_timeout)

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition circuit to a new state (caller must hold lock)."""
        old_state = self.state
        if old_state == new_state:
            return
        self.state = new_state
        self.last_state_change = time.time()
        if new_state == CircuitState.OPEN:
            self.circuit_opened_at = time.time()
        logger.info(
            f"CircuitMesh: {self.name} transitioned {old_state.value} -> {new_state.value} "
            f"(failures={self.consecutive_failures}, health={self.health_score():.1f})"
        )

    def should_allow_request(self) -> bool:
        """Check if a request should be allowed through this circuit.

        Returns:
            True if the request can proceed, False if blocked.
        """
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True

            if self.state == CircuitState.OPEN:
                # Check if cooldown has expired -> transition to HALF_OPEN
                elapsed = time.time() - self.circuit_opened_at
                backoff = self._compute_backoff()
                if elapsed >= backoff:
                    self._transition_to(CircuitState.HALF_OPEN)
                    return True
                return False

            if self.state == CircuitState.HALF_OPEN:
                # Allow limited requests for testing
                return True

            return False

    def record_success(self, latency_ms: float) -> None:
        """Record a successful provider call.

        Args:
            latency_ms: Response latency in milliseconds.
        """
        with self._lock:
            self.success_count += 1
            self.consecutive_successes += 1
            self.consecutive_failures = 0
            self.last_success_time = time.time()
            self._latencies.append(latency_ms)

            if self.state == CircuitState.HALF_OPEN:
                if self.consecutive_successes >= self.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            elif self.state == CircuitState.OPEN:
                # Shouldn't happen (requests blocked), but handle gracefully
                self._transition_to(CircuitState.HALF_OPEN)

    def record_failure(self, error: str = "") -> None:
        """Record a failed provider call.

        Args:
            error: Optional error description for logging.
        """
        with self._lock:
            self.failure_count += 1
            self.consecutive_failures += 1
            self.consecutive_successes = 0
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                # Any failure in HALF_OPEN immediately re-opens
                self._transition_to(CircuitState.OPEN)
                logger.warning(
                    f"CircuitMesh: {self.name} re-opened from HALF_OPEN "
                    f"(error: {error[:200]})"
                )
            elif self.state == CircuitState.CLOSED:
                if self.consecutive_failures >= self.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
                    logger.warning(
                        f"CircuitMesh: {self.name} circuit OPENED after "
                        f"{self.consecutive_failures} consecutive failures "
                        f"(backoff={self._compute_backoff():.1f}s, error: {error[:200]})"
                    )

    def get_status(self) -> Dict:
        """Get a snapshot of this circuit's status.

        Returns:
            Dict with state, counts, health score, and timing info.
        """
        with self._lock:
            now = time.time()
            avg_latency = (
                round(sum(self._latencies) / len(self._latencies), 1)
                if self._latencies
                else 0.0
            )
            p95_latency = 0.0
            if self._latencies:
                sorted_lats = sorted(self._latencies)
                p95_idx = int(len(sorted_lats) * 0.95)
                p95_latency = round(sorted_lats[min(p95_idx, len(sorted_lats) - 1)], 1)

            return {
                "name": self.name,
                "state": self.state.value,
                "health_score": self.health_score(),
                "success_count": self.success_count,
                "failure_count": self.failure_count,
                "consecutive_failures": self.consecutive_failures,
                "consecutive_successes": self.consecutive_successes,
                "avg_latency_ms": avg_latency,
                "p95_latency_ms": p95_latency,
                "last_success_ago_s": (
                    round(now - self.last_success_time, 1)
                    if self.last_success_time > 0
                    else None
                ),
                "last_failure_ago_s": (
                    round(now - self.last_failure_time, 1)
                    if self.last_failure_time > 0
                    else None
                ),
                "current_backoff_s": round(self._compute_backoff(), 1),
            }


class CircuitBreakerMesh:
    """Mesh of circuit breakers for all LLM providers.

    Provides a unified interface to register, query, and update
    per-provider circuit breakers with health-score-based routing.
    """

    def __init__(self) -> None:
        self._circuits: Dict[str, ProviderCircuit] = {}
        self._lock = threading.Lock()
        self._created_at = time.time()

    def register_provider(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        open_timeout: float = 30.0,
        max_open_timeout: float = 300.0,
        latency_threshold_ms: float = 5000.0,
    ) -> None:
        """Register a provider in the mesh.

        If already registered, this is a no-op to allow safe re-registration.

        Args:
            name: Unique provider identifier.
            failure_threshold: Consecutive failures to trip circuit OPEN.
            success_threshold: Consecutive successes in HALF_OPEN to close.
            open_timeout: Base cooldown seconds before HALF_OPEN.
            max_open_timeout: Maximum backoff cap in seconds.
            latency_threshold_ms: Latency threshold for health scoring.
        """
        with self._lock:
            if name in self._circuits:
                return
            self._circuits[name] = ProviderCircuit(
                name=name,
                failure_threshold=failure_threshold,
                success_threshold=success_threshold,
                open_timeout=open_timeout,
                max_open_timeout=max_open_timeout,
                latency_threshold_ms=latency_threshold_ms,
            )
            logger.debug(f"CircuitMesh: registered provider '{name}'")

    def can_use(self, name: str) -> bool:
        """Check if a provider can accept requests.

        Args:
            name: Provider identifier.

        Returns:
            True if the provider's circuit allows requests, False if blocked.
            Returns True for unknown providers (fail-open).
        """
        with self._lock:
            circuit = self._circuits.get(name)
        if circuit is None:
            return True  # Unknown provider -- fail open
        return circuit.should_allow_request()

    def record_success(self, name: str, latency_ms: float) -> None:
        """Record a successful call to a provider.

        Args:
            name: Provider identifier.
            latency_ms: Response latency in milliseconds.
        """
        with self._lock:
            circuit = self._circuits.get(name)
        if circuit is not None:
            circuit.record_success(latency_ms)

    def record_failure(self, name: str, error: str = "") -> None:
        """Record a failed call to a provider.

        Args:
            name: Provider identifier.
            error: Optional error description.
        """
        with self._lock:
            circuit = self._circuits.get(name)
        if circuit is not None:
            circuit.record_failure(error)

    def get_healthy_providers(self, min_score: float = 10.0) -> List[Tuple[str, float]]:
        """Get providers whose circuits allow requests, sorted by health score.

        Args:
            min_score: Minimum health score to include (0-100).

        Returns:
            List of (provider_name, health_score) tuples, sorted descending by score.
        """
        healthy: List[Tuple[str, float]] = []
        with self._lock:
            circuits = list(self._circuits.values())

        for circuit in circuits:
            if circuit.should_allow_request():
                score = circuit.health_score()
                if score >= min_score:
                    healthy.append((circuit.name, score))

        healthy.sort(key=lambda x: x[1], reverse=True)
        return healthy

    def get_provider_score(self, name: str) -> float:
        """Get the health score for a specific provider.

        Args:
            name: Provider identifier.

        Returns:
            Health score 0-100, or 0.0 if provider is unknown.
        """
        with self._lock:
            circuit = self._circuits.get(name)
        if circuit is None:
            return 0.0
        return circuit.health_score()

    def get_mesh_status(self) -> Dict:
        """Get the full mesh status with all provider circuits.

        Returns:
            Dict with mesh-level summary and per-provider details.
        """
        with self._lock:
            circuits = list(self._circuits.values())

        provider_statuses = []
        total_open = 0
        total_half_open = 0
        total_closed = 0

        for circuit in circuits:
            status = circuit.get_status()
            provider_statuses.append(status)
            state = circuit.state
            if state == CircuitState.OPEN:
                total_open += 1
            elif state == CircuitState.HALF_OPEN:
                total_half_open += 1
            else:
                total_closed += 1

        total = len(circuits)
        avg_health = (
            round(sum(p["health_score"] for p in provider_statuses) / total, 1)
            if total > 0
            else 0.0
        )

        return {
            "mesh_uptime_s": round(time.time() - self._created_at, 1),
            "total_providers": total,
            "states": {
                "closed": total_closed,
                "half_open": total_half_open,
                "open": total_open,
            },
            "avg_health_score": avg_health,
            "providers": provider_statuses,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Global singleton
# ═══════════════════════════════════════════════════════════════════════════════

_mesh_instance: Optional[CircuitBreakerMesh] = None
_mesh_lock = threading.Lock()


def get_circuit_mesh() -> CircuitBreakerMesh:
    """Get or create the global CircuitBreakerMesh singleton.

    Returns:
        The shared CircuitBreakerMesh instance.
    """
    global _mesh_instance
    if _mesh_instance is None:
        with _mesh_lock:
            if _mesh_instance is None:
                _mesh_instance = CircuitBreakerMesh()
                logger.info("CircuitMesh: global singleton initialized")
    return _mesh_instance
