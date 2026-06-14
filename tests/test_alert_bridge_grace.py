"""Tests for the S90 error-rate alert guards (post-deploy grace + min volume).

Regression cover for the 2026-06-13 deploy-storm false-pages: a flurry of
auto-deploys restarted gunicorn workers, and each cold-started worker served a
handful of 5xx over a tiny request window -> windowed error_rate spiked to
17.6% -> CRITICAL page, even though nothing was actually wrong. These tests
lock in the two guards that suppress that noise while still paging on a real,
sustained outage.
"""

from __future__ import annotations

import time

import pytest

import monitoring
from monitoring import (
    ERROR_RATE_ALERT_GRACE_S,
    ERROR_RATE_ALERT_MIN_REQUESTS,
    MetricsCollector,
    MonitoringAlertBridge,
    evaluate_error_rate_alert,
)

PAST_GRACE = ERROR_RATE_ALERT_GRACE_S + 1000  # comfortably out of the grace window
ENOUGH = ERROR_RATE_ALERT_MIN_REQUESTS + 50  # comfortably above the volume floor


# --------------------------------------------------------------------------- #
# evaluate_error_rate_alert -- pure decision function
# --------------------------------------------------------------------------- #


def test_within_grace_never_pages_even_at_huge_error_rate() -> None:
    """A freshly restarted worker (low uptime) must not page on cold-start 5xx."""
    assert evaluate_error_rate_alert(99.0, ENOUGH, uptime_seconds=10) is None


def test_low_volume_never_pages_even_at_huge_error_rate() -> None:
    """A couple of 5xx over a tiny denominator is noise, not an incident."""
    assert (
        evaluate_error_rate_alert(50.0, window_requests=5, uptime_seconds=PAST_GRACE)
        is None
    )


def test_critical_pages_after_grace_with_volume() -> None:
    decision = evaluate_error_rate_alert(17.6, ENOUGH, uptime_seconds=PAST_GRACE)
    assert decision is not None
    alert_key, severity, subject, body = decision
    assert alert_key == "global_error_rate"
    assert severity == "CRITICAL"
    assert subject == "[Nova] High error rate (>10%)"
    assert "17.6%" in body
    assert "/api/health/integrations" in body


def test_warning_band_pages_warning() -> None:
    decision = evaluate_error_rate_alert(7.0, ENOUGH, uptime_seconds=PAST_GRACE)
    assert decision is not None
    alert_key, severity, _subject, _body = decision
    assert alert_key == "elevated_error_rate"
    assert severity == "WARNING"


def test_healthy_rate_never_pages() -> None:
    assert evaluate_error_rate_alert(2.0, ENOUGH, uptime_seconds=PAST_GRACE) is None


def test_boundary_uptime_exactly_at_grace_is_out_of_grace() -> None:
    """uptime == grace is no longer 'within grace' (strict `<`), so it can page."""
    decision = evaluate_error_rate_alert(
        50.0, ENOUGH, uptime_seconds=ERROR_RATE_ALERT_GRACE_S
    )
    assert decision is not None and decision[1] == "CRITICAL"


def test_boundary_min_requests_exact_passes_below_fails() -> None:
    at_floor = evaluate_error_rate_alert(
        50.0, ERROR_RATE_ALERT_MIN_REQUESTS, uptime_seconds=PAST_GRACE
    )
    below_floor = evaluate_error_rate_alert(
        50.0, ERROR_RATE_ALERT_MIN_REQUESTS - 1, uptime_seconds=PAST_GRACE
    )
    assert at_floor is not None
    assert below_floor is None


# --------------------------------------------------------------------------- #
# check_slo_compliance -- error_rate_pct now carries in_grace_period
# --------------------------------------------------------------------------- #


@pytest.fixture()
def clean_collector() -> MetricsCollector:
    """Singleton collector with a cleared rolling window."""
    c = MetricsCollector()
    with c._req_lock:
        c._recent_requests.clear()
        c._recent_errors.clear()
    return c


def _feed(c: MetricsCollector, total: int, errors: int) -> None:
    for i in range(total):
        status = 500 if i < errors else 200
        c.record_request("/api/test", "GET", status, 12.0)


def test_error_rate_slo_in_grace_is_compliant(
    clean_collector: MetricsCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Heavy error load, but the process "just started" -> grace -> compliant.
    monkeypatch.setattr(monitoring, "_START_TIME", time.time())
    _feed(clean_collector, total=20, errors=10)  # 50% error rate
    slo = clean_collector.check_slo_compliance()["slos"]["error_rate_pct"]
    assert slo["in_grace_period"] is True
    assert slo["compliant"] is True


def test_error_rate_slo_past_grace_is_noncompliant(
    clean_collector: MetricsCollector, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Same load, but well past the grace window -> a real, alertable violation.
    monkeypatch.setattr(monitoring, "_START_TIME", time.time() - PAST_GRACE)
    _feed(clean_collector, total=20, errors=10)  # 50% error rate
    slo = clean_collector.check_slo_compliance()["slos"]["error_rate_pct"]
    assert slo["in_grace_period"] is False
    assert slo["compliant"] is False


# --------------------------------------------------------------------------- #
# _should_alert cooldown (unchanged behaviour, locked in for regression)
# --------------------------------------------------------------------------- #


def test_should_alert_cooldown_suppresses_repeat() -> None:
    bridge = MonitoringAlertBridge()
    assert bridge._should_alert("global_error_rate") is True
    assert bridge._should_alert("global_error_rate") is False  # within cooldown
