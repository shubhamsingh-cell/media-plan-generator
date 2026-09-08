"""Tests for the shared, fail-open alert cooldown store (S90 P1).

Covers the cooldown timing, key isolation, the critical fail-open contract
(a backend outage must never suppress a real page), the Supabase backend's
disabled/enabled/error paths, and the bridge wiring.
"""

from __future__ import annotations

import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from alert_cooldown_store import (
    AlertCooldownStore,
    CooldownBackendMissing,
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
# Atomic claim path (try_claim) -- closes the cross-worker race
# --------------------------------------------------------------------------- #


def test_in_memory_try_claim_atomic_timing() -> None:
    be = InMemoryCooldownBackend()
    assert be.try_claim("k", 1000.0, 100.0) is True  # first claim
    assert be.try_claim("k", 1050.0, 100.0) is False  # within cooldown
    assert be.try_claim("k", 1101.0, 100.0) is True  # elapsed -> reclaim


def test_in_memory_try_claim_future_timestamp_fails_open() -> None:
    be = InMemoryCooldownBackend()
    be.record_fired("k", 10_000_000_000.0)  # future
    assert be.try_claim("k", 1000.0, 100.0) is True  # negative age -> claim


def test_store_prefers_try_claim_and_fail_opens_on_none() -> None:
    class _NoneClaim:
        name = "noneclaim"

        def try_claim(self, key, now, cooldown_s):
            return None  # simulate RPC error

        def get_last_fired(self, key):  # should NOT be consulted
            raise AssertionError("fallback used despite try_claim present")

        def record_fired(self, key, ts):
            pass

        def active_count(self):
            return 0

    store = AlertCooldownStore(_NoneClaim())
    assert store.should_fire("k", 100, now=1000.0) is True  # fail-open


def test_supabase_try_claim_disabled_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    assert SupabaseCooldownBackend().try_claim("k", 1.0, 100.0) is None


def _claim_resp(body: bytes) -> MagicMock:
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_supabase_try_claim_parses_boolean(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    with patch(
        "alert_cooldown_store.urllib.request.urlopen",
        return_value=_claim_resp(b"true"),
    ):
        assert be.try_claim("k", 1.0, 100.0) is True
    with patch(
        "alert_cooldown_store.urllib.request.urlopen",
        return_value=_claim_resp(b"false"),
    ):
        assert be.try_claim("k", 1.0, 100.0) is False


def test_supabase_try_claim_error_fail_opens_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    with patch(
        "alert_cooldown_store.urllib.request.urlopen",
        side_effect=OSError("rpc down"),
    ):
        assert be.try_claim("k", 1.0, 100.0) is None


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


# --------------------------------------------------------------------------- #
# Missing table/RPC (PostgREST 404): demote to in-memory, do NOT fail open
# --------------------------------------------------------------------------- #
#
# SUPABASE_URL/SERVICE_ROLE_KEY are set in prod, so the Supabase backend is
# selected whether or not docs/sql/alert_cooldowns.sql was ever applied. Before
# this guard, a missing RPC made every claim fail OPEN -> the 1800s cooldown was
# effectively ZERO and a sustained condition re-paged every 60s bridge cycle.


def _http_404() -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "https://example.supabase.co/rest/v1/rpc/claim_alert_cooldown",
        404,
        "Not Found",
        None,
        None,
    )


def test_supabase_404_raises_backend_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    with patch("alert_cooldown_store.urllib.request.urlopen", side_effect=_http_404()):
        with pytest.raises(CooldownBackendMissing):
            be.try_claim("k", 1.0, 100.0)
        with pytest.raises(CooldownBackendMissing):
            be.get_last_fired("k")
        with pytest.raises(CooldownBackendMissing):
            be.record_fired("k", 1.0)


def test_supabase_non_404_http_error_still_fails_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx is transient: fire once (fail-open), keep the backend."""
    _enable_supabase(monkeypatch)
    be = SupabaseCooldownBackend()
    err = urllib.error.HTTPError(
        "https://example.supabase.co/x", 503, "Unavailable", None, None
    )
    with patch("alert_cooldown_store.urllib.request.urlopen", side_effect=err):
        assert be.try_claim("k", 1.0, 100.0) is None
        assert be.get_last_fired("k") is None
        be.record_fired("k", 1.0)  # swallowed


def test_store_demotes_on_missing_rpc_and_cooldown_holds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE regression: with the RPC absent the 2nd fire inside the window must
    be suppressed. Previously this returned True, True (cooldown = 0)."""
    _enable_supabase(monkeypatch)
    store = AlertCooldownStore(SupabaseCooldownBackend())
    assert store.backend_name == "supabase"
    assert store.demoted_from is None
    with patch("alert_cooldown_store.urllib.request.urlopen", side_effect=_http_404()):
        assert store.should_fire("k", 1800, now=1000.0) is True  # first page
        assert store.should_fire("k", 1800, now=1060.0) is False  # 60s later: HELD
        assert store.should_fire("k", 1800, now=2900.0) is True  # window elapsed
    assert store.backend_name == "memory"
    assert store.demoted_from == "supabase"


def test_store_get_and_record_demote_on_missing_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """alert_manager uses the split get/record path; it must demote too."""
    _enable_supabase(monkeypatch)
    store = AlertCooldownStore(SupabaseCooldownBackend())
    with patch("alert_cooldown_store.urllib.request.urlopen", side_effect=_http_404()):
        assert store.get_last_fired("subj") is None  # demoted, empty memory
        store.record_fired("subj", 1000.0)  # lands in memory, no raise
    assert store.backend_name == "memory"
    assert store.get_last_fired("subj") == 1000.0


def test_demotion_logs_warning_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    _enable_supabase(monkeypatch)
    store = AlertCooldownStore(SupabaseCooldownBackend())
    with caplog.at_level(logging.WARNING, logger="alert_cooldown_store"):
        with patch(
            "alert_cooldown_store.urllib.request.urlopen", side_effect=_http_404()
        ):
            store.should_fire("a", 100, now=1.0)
            store.should_fire("b", 100, now=1.0)
    warnings = [r for r in caplog.records if "demoted to in-memory" in r.getMessage()]
    assert len(warnings) == 1
    assert "alert_cooldowns.sql" in warnings[0].getMessage()


def test_bridge_demotes_when_rpc_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end at the bridge: _should_alert must hold the cooldown and
    get_status must report the truth (memory), not a phantom 'supabase'."""
    _enable_supabase(monkeypatch)
    from monitoring import MonitoringAlertBridge

    with patch("alert_cooldown_store.urllib.request.urlopen", side_effect=_http_404()):
        bridge = MonitoringAlertBridge()
        assert bridge._cooldown_store.backend_name == "supabase"
        assert bridge._should_alert("global_error_rate") is True
        assert bridge._should_alert("global_error_rate") is False  # HELD
    assert bridge.get_status()["cooldown_backend"] == "memory"
