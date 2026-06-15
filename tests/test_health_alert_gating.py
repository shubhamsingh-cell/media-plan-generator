"""Tests for the S90/P3 module-health alert gating.

Module-health pages (degraded / critical / DOWN) previously had no post-deploy
grace or volume floor, so a freshly-started module with a couple of 5xx over a
tiny window (e.g. 1 error / 2 requests -> health score ~60) could fire a WARNING
on cold-start noise. ``suppress_health_alert`` applies the same gating S90 gave
the error-rate page.
"""

from __future__ import annotations

from monitoring import (
    ERROR_RATE_ALERT_GRACE_S,
    MODULE_HEALTH_ALERT_MIN_REQUESTS,
    suppress_health_alert,
)

PAST_GRACE = ERROR_RATE_ALERT_GRACE_S + 1000
ENOUGH = MODULE_HEALTH_ALERT_MIN_REQUESTS + 50


def test_within_grace_suppresses() -> None:
    assert suppress_health_alert(ENOUGH, uptime_seconds=10) == "post-deploy grace"


def test_low_volume_suppresses_past_grace() -> None:
    assert (
        suppress_health_alert(3, uptime_seconds=PAST_GRACE) == "too few requests"
    )


def test_real_problem_past_grace_with_volume_is_allowed() -> None:
    assert suppress_health_alert(ENOUGH, uptime_seconds=PAST_GRACE) is None


def test_boundaries() -> None:
    # uptime == grace is no longer "in grace" (strict <).
    assert (
        suppress_health_alert(ENOUGH, uptime_seconds=ERROR_RATE_ALERT_GRACE_S) is None
    )
    # exactly at the volume floor is allowed; one below is suppressed.
    assert (
        suppress_health_alert(MODULE_HEALTH_ALERT_MIN_REQUESTS, PAST_GRACE) is None
    )
    assert (
        suppress_health_alert(MODULE_HEALTH_ALERT_MIN_REQUESTS - 1, PAST_GRACE)
        == "too few requests"
    )
