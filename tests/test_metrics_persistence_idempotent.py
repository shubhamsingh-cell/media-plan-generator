"""Tests for the metrics-persistence apply-once guard.

SupabasePersistence._apply_snapshot ADDITIVELY merges the saved counters into
the collector -- correct for restoring cumulative-across-restart counters ONCE
at startup (when current == 0). But a second load() in the same process (e.g. a
double init_metrics_persistence(), the double-init footgun that bit the
auth/bridge paths) would apply the merge twice and DOUBLE-COUNT. load() is now
apply-once.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

import monitoring
from monitoring import MetricsCollector, SupabasePersistence


def _snapshot_resp(payload) -> MagicMock:
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode("utf-8")
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_load_is_idempotent_no_double_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    c = MetricsCollector()
    p = SupabasePersistence(c)
    assert p.is_enabled

    snap = [
        {"metric_key": "total_requests", "metric_value": 1000},
        {"metric_key": "total_errors", "metric_value": 50},
    ]
    before_req = c.total_requests
    before_err = c.total_errors
    try:
        with patch(
            "monitoring.urllib.request.urlopen", return_value=_snapshot_resp(snap)
        ):
            assert p.load() is True  # first load restores
            after_first_req = c.total_requests
            after_first_err = c.total_errors
            assert p.load() is False  # second load is a no-op (idempotent)
            after_second_req = c.total_requests
            after_second_err = c.total_errors
        # First load applied the snapshot exactly once...
        assert after_first_req == before_req + 1000
        assert after_first_err == before_err + 50
        # ...and the second load did NOT add it again (no double-count).
        assert after_second_req == after_first_req
        assert after_second_err == after_first_err
        assert p._snapshot_loaded is True
    finally:
        with c._req_lock:  # restore the shared singleton
            c.total_requests = before_req
            c.total_errors = before_err
