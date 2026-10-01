"""Regression tests: the PUBLIC, unauthenticated ``GET /api/config`` must never
serve a server secret.

Prod 2026-10-01: ``/api/config`` returned a ``phx_``-prefixed PostHog key.
``phx_`` is PostHog's PERSONAL api key prefix (read/admin access to the whole
account); only project keys (``phc_``) are meant for browsers. Root cause:
``routes/health.py::_handle_config`` relayed ``POSTHOG_PROJECT_API_KEY or
POSTHOG_API_KEY`` verbatim, so whatever an operator pasted into the env var
became a public value.

The tests drive the REAL request handler (``app.ThreadedHTTPServer`` +
``app.MediaPlanHandler`` on an ephemeral port, same pattern as
tests/test_saved_plans_auth_wiring.py) and assert on the RAW response text as
well as on every string value of the parsed body. All keys below are fake.
"""

from __future__ import annotations

import base64
import http.client
import itertools
import json
import socket
import threading
import time
from typing import Any, Iterator

import pytest

import app
import posthog_integration
from routes import health as health_routes

# Built at runtime from fragments so no secret-shaped literal exists in source:
# GitHub push protection (and other scanners) match the contiguous `phx_<token>`.
_FAKE_PERSONAL = "ph" + "x_" + "FAKEPERSONALKEY0123456789abcdefghijklmnop"
_FAKE_PROJECT = "ph" + "c_" + "FAKEPROJECTKEY0123456789abcdefghijklmnopq"
_FAKE_OTHER_PROJECT = "ph" + "c_" + "FAKEOTHERPROJECT0123456789abcdefghijklm"
_SB_URL = "https://example-project.supabase.co"

# Every field /api/config is allowed to return. A NEW field fails the tripwire
# test below until it is audited for secrecy and added here on purpose.
_ALLOWED_FIELDS = {
    "posthog_configured",
    "posthog_key",
    "posthog_host",
    "supabase_url",
    "supabase_anon_key",
    "auth_enabled",
    "auth_provider",
}


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _supabase_jwt(role: str) -> str:
    """An unsigned-shape Supabase JWT (``role`` claim only is ever inspected)."""
    head = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    claims = _b64(json.dumps({"iss": "supabase", "role": role}).encode())
    return f"{head}.{claims}.{_b64(b'fake-signature-bytes')}"


_ANON_JWT = _supabase_jwt("anon")
_SERVICE_ROLE_JWT = _supabase_jwt("service_role")


@pytest.fixture(scope="module")
def live_port() -> Iterator[int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-config-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("test server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


_ip_counter = itertools.count(1)


def _get_config(port: int, path: str = "/api/config") -> tuple[int, str]:
    """GET ``path``; return ``(status, raw response text)``."""
    n = next(_ip_counter)
    headers = {
        "X-Forwarded-For": f"10.88.{(n // 250) % 250}.{n % 250 + 1}",
        "Accept-Encoding": "identity",
    }
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        conn.request("GET", path, headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", "replace")
    finally:
        conn.close()
    return resp.status, raw


def _strings(node: Any) -> Iterator[str]:
    """Every string in a parsed JSON tree: keys and values, recursively."""
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            yield str(key)
            yield from _strings(value)
    elif isinstance(node, (list, tuple)):
        for item in node:
            yield from _strings(item)


@pytest.fixture()
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in (
        "POSTHOG_PROJECT_API_KEY",
        "POSTHOG_API_KEY",
        "SUPABASE_URL",
        "SUPABASE_ANON_KEY",
        "SUPABASE_SERVICE_ROLE_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def _fetch(port: int, path: str = "/api/config") -> tuple[str, dict[str, Any]]:
    status, raw = _get_config(port, path)
    assert status == 200, (status, raw[:200])
    return raw, json.loads(raw)


# ---------------------------------------------------------------------------
# 1. PostHog: only a project key (phc_) ever reaches the browser
# ---------------------------------------------------------------------------


class TestPostHogKeyIsProjectKeyOnly:
    @pytest.mark.parametrize("path", ["/api/config", "/v1/api/config"])
    def test_personal_key_in_posthog_api_key_is_not_served(
        self, live_port: int, clean_env: pytest.MonkeyPatch, path: str
    ) -> None:
        """The prod incident: POSTHOG_API_KEY=phx_... was served verbatim."""
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PERSONAL)
        raw, body = _fetch(live_port, path)
        assert _FAKE_PERSONAL not in raw
        assert _FAKE_PERSONAL.split("_", 1)[1] not in raw, "key body leaked"
        assert not any("phx_" in s for s in _strings(body)), body
        assert "posthog_key" not in body, body
        assert body["posthog_configured"] is False

    def test_personal_key_in_project_env_var_is_not_served_either(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        clean_env.setenv("POSTHOG_PROJECT_API_KEY", _FAKE_PERSONAL)
        raw, body = _fetch(live_port)
        assert _FAKE_PERSONAL not in raw
        assert "posthog_key" not in body
        assert body["posthog_configured"] is False

    def test_project_key_in_posthog_api_key_is_served(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PROJECT)
        _, body = _fetch(live_port)
        assert body["posthog_key"] == _FAKE_PROJECT
        assert body["posthog_configured"] is True

    def test_project_key_in_project_env_var_wins_and_hides_the_personal_one(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        clean_env.setenv("POSTHOG_PROJECT_API_KEY", _FAKE_PROJECT)
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PERSONAL)
        raw, body = _fetch(live_port)
        assert body["posthog_key"] == _FAKE_PROJECT
        assert _FAKE_PERSONAL not in raw

    def test_neither_key_omits_the_field(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        _, body = _fetch(live_port)
        assert "posthog_key" not in body
        assert body["posthog_configured"] is False

    @pytest.mark.parametrize(
        "value",
        ["", "   ", "sk-ant-FAKE0123456789", "PHX_FAKEUPPERCASE0123456789", "abc123"],
    )
    def test_anything_that_is_not_a_project_key_is_withheld(
        self, live_port: int, clean_env: pytest.MonkeyPatch, value: str
    ) -> None:
        """Allowlist, not blocklist: an unknown prefix is never relayed."""
        clean_env.setenv("POSTHOG_API_KEY", value)
        raw, body = _fetch(live_port)
        if value.strip():
            assert value.strip() not in raw
        assert "posthog_key" not in body

    def test_whitespace_around_a_project_key_is_stripped(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        clean_env.setenv("POSTHOG_API_KEY", f"  {_FAKE_PROJECT}\n")
        _, body = _fetch(live_port)
        assert body["posthog_key"] == _FAKE_PROJECT


class TestBrowserCaptureKeyHelper:
    """``posthog_integration.get_browser_capture_key`` is the single gate, and it
    agrees with the server-side capture client on which key is in use."""

    def test_returns_only_phc_keys(self, clean_env: pytest.MonkeyPatch) -> None:
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PROJECT)
        assert posthog_integration.get_browser_capture_key() == _FAKE_PROJECT
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PERSONAL)
        assert posthog_integration.get_browser_capture_key() == ""

    def test_reads_the_environment_at_call_time(
        self, clean_env: pytest.MonkeyPatch
    ) -> None:
        assert posthog_integration.get_browser_capture_key() == ""
        clean_env.setenv("POSTHOG_PROJECT_API_KEY", _FAKE_OTHER_PROJECT)
        assert posthog_integration.get_browser_capture_key() == _FAKE_OTHER_PROJECT

    def test_uses_the_same_resolution_order_as_the_capture_client(
        self, clean_env: pytest.MonkeyPatch
    ) -> None:
        """When the capture client would pick a personal key (and disable itself),
        the browser gets nothing -- never a different, "fallback" key."""
        clean_env.setenv("POSTHOG_PROJECT_API_KEY", _FAKE_PERSONAL)
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PROJECT)
        key, _name = posthog_integration._resolve_capture_key()
        assert posthog_integration._is_personal_api_key(key)
        assert posthog_integration.get_browser_capture_key() == ""

    def test_a_withheld_key_logs_the_env_var_name_never_the_value(
        self, clean_env: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PERSONAL)
        posthog_integration._warned_once.discard("browser-key-withheld:POSTHOG_API_KEY")
        with caplog.at_level("WARNING", logger="posthog_integration"):
            assert posthog_integration.get_browser_capture_key() == ""
        text = " ".join(r.getMessage() for r in caplog.records)
        assert "POSTHOG_API_KEY" in text
        assert _FAKE_PERSONAL not in text
        assert _FAKE_PERSONAL.split("_", 1)[1] not in text


# ---------------------------------------------------------------------------
# 2. Audit of the other served values
# ---------------------------------------------------------------------------

_SECRET_ENV_NAMES = [
    "NOVA_ADMIN_KEY",
    "NOVA_API_KEYS",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_KEY",
    "SUPABASE_JWT_SECRET",
    "SESSION_SIGNING_SECRET",
    "SLACK_WEBHOOK_URL",
    "SLACK_BOT_TOKEN",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "GEMINI_API_KEY",
    "VOYAGE_API_KEY",
    "RESEND_API_KEY",
    "RENDER_API_KEY",
    "SENTRY_AUTH_TOKEN",
    "UPSTASH_REDIS_REST_TOKEN",
    "LANGFUSE_SECRET_KEY",
    "QDRANT_API_KEY",
]


class TestOtherServerSecretsNeverAppear:
    def test_no_secret_env_var_value_appears_in_the_response(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        sentinels = {n: f"SENTINEL-{n}-7c1e9b" for n in _SECRET_ENV_NAMES}
        for name, value in sentinels.items():
            clean_env.setenv(name, value)
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PERSONAL)
        clean_env.setenv("SUPABASE_URL", _SB_URL)
        clean_env.setenv("SUPABASE_ANON_KEY", _ANON_JWT)
        raw, body = _fetch(live_port)
        for name, value in sentinels.items():
            assert value not in raw, f"{name} leaked through /api/config"
        assert not any(
            marker in s
            for s in _strings(body)
            for marker in ("phx_", "sb_secret_", "xoxb-", "sk-ant-", "SENTINEL-")
        ), body

    def test_the_field_set_is_the_audited_set(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        """Tripwire: a new field must be audited for secrecy and listed above."""
        clean_env.setenv("POSTHOG_API_KEY", _FAKE_PROJECT)
        clean_env.setenv("SUPABASE_URL", _SB_URL)
        clean_env.setenv("SUPABASE_ANON_KEY", _ANON_JWT)
        _, body = _fetch(live_port)
        assert set(body) == _ALLOWED_FIELDS, set(body) ^ _ALLOWED_FIELDS


class TestSupabaseAnonKeyIsNeverASecretKey:
    def test_a_real_anon_key_is_served(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        clean_env.setenv("SUPABASE_URL", _SB_URL)
        clean_env.setenv("SUPABASE_ANON_KEY", _ANON_JWT)
        _, body = _fetch(live_port)
        assert body["supabase_anon_key"] == _ANON_JWT
        assert body["supabase_url"] == _SB_URL
        assert body["auth_enabled"] is True

    @pytest.mark.parametrize(
        "secret",
        [_SERVICE_ROLE_JWT, "sb_secret_FAKE0123456789abcdef"],
        ids=["service_role_jwt", "sb_secret_prefix"],
    )
    def test_a_secret_key_pasted_into_the_anon_variable_is_withheld(
        self, live_port: int, clean_env: pytest.MonkeyPatch, secret: str
    ) -> None:
        clean_env.setenv("SUPABASE_URL", _SB_URL)
        clean_env.setenv("SUPABASE_ANON_KEY", secret)
        raw, body = _fetch(live_port)
        assert secret not in raw
        assert body["supabase_anon_key"] == ""
        assert body["auth_enabled"] is False

    def test_a_value_equal_to_the_service_role_variable_is_withheld(
        self, live_port: int, clean_env: pytest.MonkeyPatch
    ) -> None:
        shared = "opaque-key-that-is-also-the-service-role-key"
        clean_env.setenv("SUPABASE_URL", _SB_URL)
        clean_env.setenv("SUPABASE_ANON_KEY", shared)
        clean_env.setenv("SUPABASE_SERVICE_ROLE_KEY", shared)
        raw, body = _fetch(live_port)
        assert shared not in raw
        assert body["supabase_anon_key"] == ""

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("", False),
            (_ANON_JWT, False),
            (_SERVICE_ROLE_JWT, True),
            (_supabase_jwt("SERVICE_ROLE"), True),
            ("sb_secret_abc", True),
            ("sb_publishable_abc", False),
            ("not.a.jwt", False),
            ("a.b", False),
            ("eyJ.%%%.sig", False),
        ],
    )
    def test_secret_key_classifier(self, key: str, expected: bool) -> None:
        assert health_routes._is_supabase_secret_key(key) is expected
