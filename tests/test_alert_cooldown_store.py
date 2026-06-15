"""Tests for the shared, fail-open alert cooldown store (S90 P1).

Covers the cooldown timing, key isolation, the critical fail-open contract
(a backend outage must never suppress a real page), the Supabase backend's
disabled/enabled/error paths, and the bridge wiring.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from alert_cooldown_store import (
    AlertCooldownStore,
    InMemoryCooldownBackend,
    SupabaseCooldownBackend,
    _default_backend,
)


# --------------------------------------------------------------------------- #
# Cooldown timing + key isolation (in-memory backend)
# --------------------------------------------------------------------------- #


def test_first_fire_allowed_then_suppressed_then_allowed_again() -> None:
    store = AlertCooldownStore(InMemoryCooldownBackend())
    assert store.should_fire("k", 100, now=1000.0) is True  # first time
    assert store.should_fire("k", 100, now=1050.0) is False  # within cooldown
    assert store.should_fire("k", 100, now=1101.0) is True  # cooldown elapsed


def test_distinct_keys_have_independent_cooldowns() -> None:
    store = AlertCooldownStore(InMemoryCooldownBackend())
    assert store.should_fire("a", 100, now=1000.0) is True
    assert store.should_fire("b", 100, now=1000.0) is True  # different key, fires
    assert store.should_fire("a", 100, now=1000.0) is False


def test_active_count_and_backend_name() -> None:
    store = AlertCooldownStore(InMemoryCooldownBackend())
    store.should_fire("a", 100, now=1.0)
    store.should_fire("b", 100, now=1.0)
    assert store.active_count() == 2
    assert store.backend_name == "memory"


# --------------------------------------------------------------------------- #
# FAIL-OPEN: a broken backend must never suppress an alert
# --------------------------------------------------------------------------- #


class _ExplodingBackend:
    name = "exploding"

    def get_last_fired(self, key: str):
        raise RuntimeError("boom-read")

    def record_fired(self, key: str, ts: float) -> None:
        raise RuntimeError("boom-write")

    def active_count(self) -> int:
        raise RuntimeError("boom-count")


def test_fail_open_when_backend_raises_everywhere() -> None:
    store = AlertCooldownStore(_ExplodingBackend())
    # Must allow the alert (fail-open) and never raise, even on repeated calls.
    assert store.should_fire("k", 100, now=1000.0) is True
    assert store.should_fire("k", 100, now=1000.0) is True
    assert store.active_count() == -1  # degrades gracefully


class _FutureBackend:
    name = "future"

    def get_last_fired(self, key: str):
        return 10_000_000_000.0  # far-future timestamp (year ~2286)

    def record_fired(self, key: str, ts: float) -> None:
        pass

    def active_count(self) -> int:
        return 1


def test_future_timestamp_does_not_suppress_alert() -> None:
    # A future "last fired" yields a negative age; it must NOT be treated as
    # "still cooling down" -- fail-open requires the alert to fire.
    store = AlertCooldownStore(_FutureBackend())
    assert store.should_fire("k", 1800, now=1000.0) is True


# --------------------------------------------------------------------------- #
# Supabase backend: disabled / enabled / error paths
# --------------------------------------------------------------------------- #


def test_supabase_backend_disabled_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    be = SupabaseCooldownBackend()
    assert be.enabled is False
    assert be.get_last_fired("k") is None  # no-op, no exception
    be.record_fired("k", 1.0)  # no-op, no exception


def _enable_supabase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-key")


def test_supabase_backend_reads_last_fired(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    assert be.enabled is True
    resp = MagicMock()
    resp.read.return_value = b'[{"last_fired_ts": 1234.5}]'
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    with patch("alert_cooldown_store.urllib.request.urlopen", return_value=resp):
        assert be.get_last_fired("global_error_rate") == 1234.5


def test_supabase_backend_empty_result_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    resp = MagicMock()
    resp.read.return_value = b"[]"
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    with patch("alert_cooldown_store.urllib.request.urlopen", return_value=resp):
        assert be.get_last_fired("k") is None


def test_supabase_backend_read_failure_is_fail_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    with patch(
        "alert_cooldown_store.urllib.request.urlopen",
        side_effect=OSError("network down"),
    ):
        assert be.get_last_fired("k") is None  # fail-open, no raise


def test_supabase_backend_write_failure_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    with patch(
        "alert_cooldown_store.urllib.request.urlopen",
        side_effect=OSError("network down"),
    ):
        be.record_fired("k", 1.0)  # must not raise


def test_supabase_rejects_implausible_future_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    resp = MagicMock()
    resp.read.return_value = b'[{"last_fired_ts": 99999999999999.0}]'
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    with patch("alert_cooldown_store.urllib.request.urlopen", return_value=resp):
        # Implausible future timestamp -> None (fail-open), never suppresses.
        assert be.get_last_fired("k") is None


def test_supabase_non_list_response_is_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    resp = MagicMock()
    resp.read.return_value = b'{"unexpected": "object"}'
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    with patch("alert_cooldown_store.urllib.request.urlopen", return_value=resp):
        assert be.get_last_fired("k") is None


def test_default_backend_is_memory_without_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    assert _default_backend().name == "memory"


# --------------------------------------------------------------------------- #
# Bridge wiring
# --------------------------------------------------------------------------- #


def test_bridge_should_alert_uses_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    from monitoring import MonitoringAlertBridge

    bridge = MonitoringAlertBridge()
    assert bridge._cooldown_store is not None
    assert bridge._should_alert("global_error_rate") is True
    assert bridge._should_alert("global_error_rate") is False  # within cooldown
    status = bridge.get_status()
    assert status["cooldown_backend"] == "memory"
    assert status["active_cooldowns"] >= 1
