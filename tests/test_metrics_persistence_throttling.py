"""Regression coverage for idle Supabase metrics persistence."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

import monitoring
from monitoring import MetricsCollector, SupabasePersistence


def _save_resp() -> MagicMock:
    resp = MagicMock()
    resp.read.return_value = b""
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_background_save_skips_idle_ticks_until_periodic_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unchanged process writes once, then refreshes only after 30 minutes."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    persistence = SupabasePersistence(MetricsCollector())
    clock = [0.0]

    def fake_time() -> float:
        return clock[0]

    def fake_sleep(seconds: float) -> None:
        clock[0] += seconds
        if clock[0] > 2100:
            raise SystemExit

    with patch("monitoring.time.time", side_effect=fake_time), patch(
        "monitoring.time.sleep", side_effect=fake_sleep
    ), patch("monitoring.time.monotonic", side_effect=fake_time), patch(
        "monitoring.urllib.request.urlopen", return_value=_save_resp()
    ) as open_mock:
        with pytest.raises(SystemExit):
            persistence._save_loop()

    assert open_mock.call_count == 2


def test_background_save_writes_changes_retries_failures_and_keeps_health_on_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only successful remote writes advance the idle snapshot marker."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    collector = MetricsCollector()
    persistence = SupabasePersistence(collector)
    with patch(
        "monitoring.urllib.request.urlopen", return_value=_save_resp()
    ) as open_mock:
        assert persistence._save_if_needed() is True
        saved_at = persistence._last_save_ts
        assert persistence._save_if_needed() is False
        assert persistence._last_save_ts == saved_at

        with collector._req_lock:
            before = collector.total_requests
            collector.total_requests += 1
        try:
            assert persistence._save_if_needed() is True
            assert open_mock.call_count == 2
        finally:
            with collector._req_lock:
                collector.total_requests = before


def test_explicit_save_always_writes_and_failed_background_save_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manual saves retain their forced-write contract; idle failures retry."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    persistence = SupabasePersistence(MetricsCollector())
    with patch(
        "monitoring.urllib.request.urlopen", return_value=_save_resp()
    ) as open_mock:
        assert persistence.save() is True
        assert persistence.save() is True
        assert open_mock.call_count == 2

    with patch(
        "monitoring.urllib.request.urlopen",
        side_effect=[monitoring.urllib.error.URLError("temporary"), _save_resp()],
    ) as open_mock:
        persistence._last_saved_values = None
        assert persistence._save_if_needed() is False
        assert persistence._save_if_needed() is True
        assert open_mock.call_count == 2


def test_concurrent_idle_checks_issue_one_changed_snapshot_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second tick waits for the first remote write before deciding to skip."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    collector = MetricsCollector()
    persistence = SupabasePersistence(collector)
    with patch("monitoring.urllib.request.urlopen", return_value=_save_resp()):
        assert persistence._save_if_needed() is True
    with collector._req_lock:
        before = collector.total_requests
        collector.total_requests += 1

    entered = threading.Event()
    release = threading.Event()
    results = []

    def blocking_response(*_args, **_kwargs):
        entered.set()
        assert release.wait(timeout=1)
        return _save_resp()

    def save_if_needed() -> None:
        results.append(persistence._save_if_needed())

    try:
        with patch(
            "monitoring.urllib.request.urlopen", side_effect=blocking_response
        ) as open_mock:
            first = threading.Thread(target=save_if_needed)
            second = threading.Thread(target=save_if_needed)
            first.start()
            assert entered.wait(timeout=1)
            second.start()
            release.set()
            first.join(timeout=1)
            second.join(timeout=1)
        assert not first.is_alive()
        assert not second.is_alive()
        assert sorted(results) == [False, True]
        assert open_mock.call_count == 1
    finally:
        with collector._req_lock:
            collector.total_requests = before


def test_failed_changed_snapshot_retries_after_counters_return_to_prior_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed B write cannot be hidden by returning counters to saved A."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    collector = MetricsCollector()
    persistence = SupabasePersistence(collector)
    with collector._req_lock:
        before = collector.total_requests

    try:
        with patch(
            "monitoring.urllib.request.urlopen",
            side_effect=[
                _save_resp(),
                monitoring.urllib.error.URLError("temporary"),
                _save_resp(),
            ],
        ) as open_mock:
            assert persistence._save_if_needed() is True  # A succeeds
            with collector._req_lock:
                collector.total_requests = before + 1  # B fails
            assert persistence._save_if_needed() is False
            with collector._req_lock:
                collector.total_requests = before  # return to A
            assert persistence._save_if_needed() is True
            assert open_mock.call_count == 3
    finally:
        with collector._req_lock:
            collector.total_requests = before


def test_save_handles_invalid_snapshot_values_as_a_failed_health_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Payload construction failures preserve save()'s False-return contract."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    persistence = SupabasePersistence(MetricsCollector())
    persistence._last_save_ok = True
    with patch.object(
        persistence, "_build_rows", return_value=[{"metric_value": None}]
    ):
        assert persistence.save() is False
    assert persistence._last_save_ok is False


def test_save_handles_infinite_collector_counter_as_a_failed_health_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real collector counter that cannot be converted must not escape save()."""
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")
    collector = MetricsCollector()
    persistence = SupabasePersistence(collector)
    with collector._req_lock:
        before = collector.total_requests
        collector.total_requests = float("inf")
    try:
        persistence._last_save_ok = True
        assert persistence.save() is False
        assert persistence._last_save_ok is False
    finally:
        with collector._req_lock:
            collector.total_requests = before
