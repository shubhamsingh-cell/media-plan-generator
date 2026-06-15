"""Tests for the S90/P2 burn-rate correctness fix.

Before P2, `compute_burn_rate` was a silent no-op for `error_rate_pct`
(`allowed = 1.0 - 1.0 = 0` plus a percent/fraction unit mismatch forced
`burn_rate` to 0 / status "ok" for *any* error rate). These tests pin the
corrected error-budget math, the rate-SLO-only scope, and the S90 grace /
insufficient-sample gating.
"""

from __future__ import annotations

import time

import pytest

import monitoring
from monitoring import MetricsCollector


# --------------------------------------------------------------------------- #
# _error_budget_burn_rate -- pure helper
# --------------------------------------------------------------------------- #


def test_error_rate_burn_is_actual_over_target() -> None:
    # 17.6% observed against a 1.0% budget -> 17.6x.
    assert MetricsCollector._error_budget_burn_rate("error_rate_pct", 17.6, 1.0) == 17.6
    # Under budget -> <1x.
    assert MetricsCollector._error_budget_burn_rate("error_rate_pct", 0.5, 1.0) == 0.5


def test_availability_burn_uses_unavailability_budget() -> None:
    # 99.0% observed against a 99.5% target: unavail 1.0% / allowed 0.5% = 2x.
    assert (
        MetricsCollector._error_budget_burn_rate("availability_pct", 99.0, 99.5) == 2.0
    )
    # Perfectly available -> 0x burn.
    assert (
        MetricsCollector._error_budget_burn_rate("availability_pct", 100.0, 99.5) == 0.0
    )


def test_latency_and_unknown_slos_have_no_burn_rate() -> None:
    assert (
        MetricsCollector._error_budget_burn_rate("generate_p99_ms", 30000.0, 45000.0)
        is None
    )
    assert MetricsCollector._error_budget_burn_rate("something_else", 5.0, 1.0) is None


def test_zero_or_negative_target_is_safe() -> None:
    assert MetricsCollector._error_budget_burn_rate("error_rate_pct", 5.0, 0.0) is None


# --------------------------------------------------------------------------- #
# compute_burn_rate -- integration (gated, rate-SLO-only)
# --------------------------------------------------------------------------- #


@pytest.fixture()
def clean_collector() -> MetricsCollector:
    c = MetricsCollector()
    with c._req_lock:
        c._recent_requests.clear()
        c._recent_errors.clear()
    return c


def _feed(c: MetricsCollector, total: int, errors: int) -> None:
    for i in range(total):
        c.record_request("/api/test", "GET", 500 if i < errors else 200, 12.0)


def test_burn_computed_past_grace_with_volume(
    clean_collector: MetricsCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(monitoring, "_START_TIME", time.time() - 100_000)
    _feed(clean_collector, total=20, errors=4)  # 20% error rate vs 1% budget
    burn = clean_collector.compute_burn_rate()
    assert "error_rate_pct" in burn
    assert burn["error_rate_pct"]["burn_rate"] == pytest.approx(20.0, abs=0.5)
    assert burn["error_rate_pct"]["status"] == "critical"
    # Latency SLOs are thresholds, not budgets -> never in the burn map.
    assert "generate_p99_ms" not in burn
    assert "chat_p99_ms" not in burn


def test_burn_skipped_during_grace(
    clean_collector: MetricsCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(monitoring, "_START_TIME", time.time())  # uptime ~0 -> grace
    _feed(clean_collector, total=20, errors=10)
    assert "error_rate_pct" not in clean_collector.compute_burn_rate()


def test_burn_skipped_on_insufficient_samples(
    clean_collector: MetricsCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(monitoring, "_START_TIME", time.time() - 100_000)
    _feed(clean_collector, total=5, errors=3)  # < _ERR_MIN_SAMPLES (10)
    assert "error_rate_pct" not in clean_collector.compute_burn_rate()
