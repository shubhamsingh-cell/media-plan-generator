"""Regression tests: PostHog personal-key guard, auth-failure backoff, warn-once.

Prod telemetry 2026-10-01 (ledger A_prod_telemetry section 2 row 1): the
configured PostHog key has the ``phx_`` prefix -- a PERSONAL api key, which the
capture endpoint rejects with HTTP 401 ("API key is not valid:
personal_api_key"). ``PostHogClient._flush`` already dropped the batch without
dead-lettering, but it still POSTed and logged an ERROR for EVERY flush, i.e.
~286 ERROR lines per hour (~48k/week = 99.8% of the error stream) while 100% of
analytics was dropped.

Fixes pinned here:

* a ``phx_`` key used as the capture key disables the client (no flush thread,
  no network) after exactly ONE clear WARNING that names the env var, says a
  PROJECT key (``phc_...``) is required and never prints the key;
* a permanent 401/403 flush failure backs off exponentially (cap 1 h) and logs
  once per backoff window, not per attempt;
* the "non-standard event name" warning fires once per distinct event name.
"""

from __future__ import annotations

import io
import logging
import urllib.error
from email.message import Message
from typing import Any
from unittest import mock

import pytest

import posthog_integration as ph

_PERSONAL_KEY = "phx_SECRETSECRETSECRET0123456789"
_PROJECT_KEY = "phc_projectkey0123456789"


class _Clock:
    """Controllable replacement for the ``time`` module as seen by ph."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now

    def sleep(self, _s: float) -> None:  # pragma: no cover - never reached
        return None


def _http_error(code: int, body: bytes = b"API key is not valid") -> Exception:
    return urllib.error.HTTPError(
        url="https://us.i.posthog.com/batch/",
        code=code,
        msg="error",
        hdrs=Message(),
        fp=io.BytesIO(body),
    )


class _OkResponse:
    def __enter__(self) -> "_OkResponse":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def getcode(self) -> int:
        return 200

    def read(self, n: int = -1) -> bytes:
        return b""


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> Any:
    ph._dead_letter_queue.clear()
    if hasattr(ph, "_warned_once"):
        ph._warned_once.clear()
    yield
    ph._dead_letter_queue.clear()
    if hasattr(ph, "_warned_once"):
        ph._warned_once.clear()


def _enabled_client(monkeypatch: pytest.MonkeyPatch, key: str) -> ph.PostHogClient:
    """A client as if ``key`` were configured, with the flush timer stubbed out."""
    monkeypatch.setattr(ph, "POSTHOG_API_KEY", key)
    monkeypatch.setattr(ph, "_POSTHOG_KEY_ENV", "POSTHOG_API_KEY", raising=False)
    with mock.patch.object(ph.PostHogClient, "_start_flush_thread") as start:
        client = ph.PostHogClient()
    client._start_mock = start  # type: ignore[attr-defined]
    return client


# ---------------------------------------------------------------------------
# 1. Personal (phx_) key is detected and disables the client after ONE warning
# ---------------------------------------------------------------------------


class TestPersonalKeyGuard:
    def test_phx_key_disables_client_and_never_starts_flush_thread(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _enabled_client(monkeypatch, _PERSONAL_KEY)
        assert client._enabled is False
        client._start_mock.assert_not_called()  # type: ignore[attr-defined]

    def test_single_warning_names_env_var_and_project_key_never_the_key(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(ph, "POSTHOG_API_KEY", _PERSONAL_KEY)
        monkeypatch.setattr(ph, "_POSTHOG_KEY_ENV", "POSTHOG_API_KEY", raising=False)
        with caplog.at_level(logging.DEBUG, logger="posthog_integration"):
            with mock.patch.object(ph.PostHogClient, "_start_flush_thread"):
                client = ph.PostHogClient()
            # simulate a busy app: events keep coming
            for i in range(25):
                client.track_event("u", f"plan.evt{i}", {"k": i})
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1, [r.getMessage() for r in warnings]
        msg = warnings[0].getMessage()
        assert "POSTHOG_API_KEY" in msg
        assert "phc_" in msg
        assert "personal" in msg.lower()
        assert warnings[0].levelno == logging.WARNING
        # the key (or any 8-char prefix of it) must never appear in any log line
        for rec in caplog.records:
            text = rec.getMessage()
            assert _PERSONAL_KEY not in text
            assert _PERSONAL_KEY[:8] not in text

    def test_phx_client_never_touches_the_network(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _enabled_client(monkeypatch, _PERSONAL_KEY)
        with mock.patch.object(ph.urllib.request, "urlopen") as urlopen:
            client.track_event("u", "plan.generated", {"plan_type": "x"})
            client._flush()
            client.shutdown()
        urlopen.assert_not_called()
        assert client._queue_size() == 0

    def test_feature_flag_lookup_skips_network_with_personal_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ph, "POSTHOG_API_KEY", _PERSONAL_KEY)
        with mock.patch.object(ph.urllib.request, "urlopen") as urlopen:
            assert ph.is_feature_enabled("any_flag", "u", default=True) is True
        urlopen.assert_not_called()

    def test_project_key_keeps_client_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        assert client._enabled is True
        client._start_mock.assert_called_once()  # type: ignore[attr-defined]

    def test_stats_report_the_disabled_reason(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _enabled_client(monkeypatch, _PERSONAL_KEY)
        stats = client.get_stats()
        assert stats["enabled"] is False
        assert stats.get("disabled_reason") == "personal_api_key"


# ---------------------------------------------------------------------------
# 2. Permanent 401/403: exponential backoff (cap 1 h), one log per window
# ---------------------------------------------------------------------------


class TestAuthFailureBackoff:
    def _flush_once(
        self, client: ph.PostHogClient, err: Exception | None = None
    ) -> mock.MagicMock:
        client._queue.append({"event": "plan.generated"})
        if err is None:
            patched = mock.patch.object(
                ph.urllib.request, "urlopen", return_value=_OkResponse()
            )
        else:
            patched = mock.patch.object(ph.urllib.request, "urlopen", side_effect=err)
        with patched as urlopen:
            client._flush()
        return urlopen

    @pytest.mark.parametrize("code", [401, 403])
    def test_second_flush_inside_window_makes_no_request_and_no_log(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        code: int,
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(ph, "time", clock)
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        with caplog.at_level(logging.DEBUG, logger="posthog_integration"):
            first = self._flush_once(client, _http_error(code))
            assert first.call_count == 1
            errors_after_first = [
                r for r in caplog.records if r.levelno >= logging.ERROR
            ]
            assert len(errors_after_first) == 1
            # 10 more flush attempts 1 s apart (well inside the first window)
            for _ in range(10):
                clock.now += 1.0
                again = self._flush_once(client, _http_error(code))
                assert again.call_count == 0, "no HTTP attempt inside the window"
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 1, [r.getMessage() for r in errors]
        assert client._queue_size() == 0, "events inside the window are dropped"
        assert len(ph._dead_letter_queue) == 0

    def test_window_doubles_and_is_capped_at_one_hour(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(ph, "time", clock)
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        windows: list[float] = []
        for _ in range(14):
            urlopen = self._flush_once(client, _http_error(401))
            assert urlopen.call_count == 1
            windows.append(client._backoff_until - clock.now)
            clock.now = client._backoff_until + 0.001  # window just expired
        assert windows[0] < windows[1] < windows[2]
        assert windows[1] == pytest.approx(windows[0] * 2)
        assert windows[2] == pytest.approx(windows[0] * 4)
        assert max(windows) == pytest.approx(3600.0)
        assert windows[-1] == pytest.approx(3600.0), "capped, never above 1 h"

    def test_attempt_resumes_after_window_and_logs_again_once(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(ph, "time", clock)
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        with caplog.at_level(logging.DEBUG, logger="posthog_integration"):
            self._flush_once(client, _http_error(401))
            clock.now = client._backoff_until + 1.0
            second = self._flush_once(client, _http_error(401))
        assert second.call_count == 1
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 2, "one log per backoff window"

    def test_success_resets_the_backoff_streak(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(ph, "time", clock)
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        self._flush_once(client, _http_error(401))
        first_window = client._backoff_until - clock.now
        clock.now = client._backoff_until + 1.0
        self._flush_once(client, _http_error(401))  # 2nd failure -> 2x window
        clock.now = client._backoff_until + 1.0
        ok = self._flush_once(client)  # success
        assert ok.call_count == 1
        clock.now += 1.0
        self._flush_once(client, _http_error(403))
        assert client._backoff_until - clock.now == pytest.approx(
            first_window, abs=1.5
        ), "streak reset: window is back to the base delay"

    def test_enqueue_inside_window_is_dropped_not_queued(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(ph, "time", clock)
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        self._flush_once(client, _http_error(401))
        for i in range(50):
            client.track_event("u", f"plan.e{i}", {})
        assert client._queue_size() == 0, "queue must not grow during a backoff window"

    def test_transient_5xx_is_not_backed_off_and_still_dead_letters(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = _Clock()
        monkeypatch.setattr(ph, "time", clock)
        client = _enabled_client(monkeypatch, _PROJECT_KEY)
        urlopen = self._flush_once(client, _http_error(503, b"down"))
        assert urlopen.call_count == 1
        assert len(ph._dead_letter_queue) == 1
        assert client._backoff_until <= clock.now


# ---------------------------------------------------------------------------
# 3. 'Non-standard event name' warning fires once per distinct name
# ---------------------------------------------------------------------------


class TestWarnOnce:
    def test_non_standard_event_name_warned_once_per_name(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(ph, "POSTHOG_API_KEY", "")
        with caplog.at_level(logging.WARNING, logger="posthog_integration"):
            for _ in range(20):
                ph.track_event("u", "weird_event_a")
            for _ in range(20):
                ph.track_event("u", "weird_event_b")
        a = [r for r in caplog.records if "weird_event_a" in r.getMessage()]
        b = [r for r in caplog.records if "weird_event_b" in r.getMessage()]
        assert len(a) == 1, [r.getMessage() for r in a]
        assert len(b) == 1

    def test_standard_event_names_do_not_warn(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(ph, "POSTHOG_API_KEY", "")
        with caplog.at_level(logging.WARNING, logger="posthog_integration"):
            ph.track_event(
                "u", "plan.generated", {"plan_type": "x", "budget": 1, "channels": []}
            )
        assert not [r for r in caplog.records if "Non-standard" in r.getMessage()]

    def test_warned_set_is_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A hostile/buggy caller inventing unique names must not grow memory forever."""
        monkeypatch.setattr(ph, "POSTHOG_API_KEY", "")
        for i in range(ph._WARN_ONCE_MAX + 50):
            ph.track_event("u", f"bogus_{i}")
        assert len(ph._warned_once) <= ph._WARN_ONCE_MAX
