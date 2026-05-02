"""Tests for apis.browser.stagehand_wrapper.

Covers the deterministic, non-network behaviours that matter most in CI:
    1. Graceful degradation when env vars are missing.
    2. Disabled feature flag returns a normalized error dict (never raises).
    3. Bad input (empty url, non-http scheme, empty instruction, bad schema)
       produces a normalized error dict.
    4. Timeouts and HTTP errors surface as ``error`` strings, not exceptions.
    5. A mocked successful POST returns the expected ``{data, source,
       elapsed_ms, error}`` shape, including the cache_actions flag flowing
       through to the server payload for ``stagehand_act``.

The wrapper uses stdlib ``urllib.request.urlopen`` so we patch that symbol
(via ``mock.patch.object``) inside the wrapper's own module namespace -- this
mirrors the patching strategy already used by other tests in this repo
(see test_web_scraper_router.py).
"""

from __future__ import annotations

import io
import json
import os
import sys
import urllib.error
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apis.browser import stagehand_wrapper  # noqa: E402
from apis.browser.stagehand_wrapper import (  # noqa: E402
    stagehand_act,
    stagehand_extract,
    stagehand_observe,
)

# ─── Shared fixtures ───────────────────────────────────────────────────────────

_ENV_KEYS = (
    "STAGEHAND_ENABLED",
    "STAGEHAND_API_URL",
    "STAGEHAND_API_KEY",
    "BROWSERBASE_API_KEY",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wipe Stagehand env vars before every test so suites are independent."""
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _enabled_env(monkeypatch: pytest.MonkeyPatch, **extra: str) -> None:
    """Helper: turn on the feature flag and a working endpoint."""
    monkeypatch.setenv("STAGEHAND_ENABLED", "true")
    monkeypatch.setenv("STAGEHAND_API_URL", "https://stagehand.example.com")
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


class _FakeResponse:
    """Minimal urlopen() context manager stand-in."""

    def __init__(self, body: bytes, status: int = 200) -> None:
        self._buf = io.BytesIO(body)
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *_args: Any) -> None:  # noqa: D401
        self._buf.close()

    def read(self) -> bytes:
        return self._buf.read()


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Disabled / missing-config behaviour
# ═══════════════════════════════════════════════════════════════════════════════


class TestDisabledFlag:
    """When STAGEHAND_ENABLED is false, every call short-circuits cleanly."""

    def test_act_returns_disabled_error_when_flag_unset(self) -> None:
        result = stagehand_act("https://example.com", "click submit")
        assert result["data"] is None
        assert result["source"] == "stagehand"
        assert isinstance(result["elapsed_ms"], int)
        assert "disabled" in (result["error"] or "").lower()

    def test_extract_returns_disabled_error_when_flag_unset(self) -> None:
        result = stagehand_extract(
            "https://example.com",
            {"type": "object", "properties": {"title": {"type": "string"}}},
        )
        assert result["data"] is None
        assert result["error"] is not None
        assert "disabled" in result["error"].lower()

    def test_observe_returns_disabled_error_when_flag_unset(self) -> None:
        result = stagehand_observe("https://example.com")
        assert result["data"] is None
        assert "disabled" in (result["error"] or "").lower()

    def test_explicit_false_value_is_treated_as_disabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("STAGEHAND_ENABLED", "0")
        monkeypatch.setenv("STAGEHAND_API_URL", "https://stagehand.example.com")
        result = stagehand_act("https://example.com", "click submit")
        assert "disabled" in (result["error"] or "").lower()


class TestMissingEndpoint:
    """When the feature flag is on but no endpoint is configured."""

    def test_missing_url_returns_integration_gap_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("STAGEHAND_ENABLED", "true")
        result = stagehand_act("https://example.com", "click submit")
        assert result["data"] is None
        assert result["error"] is not None
        assert "STAGEHAND_API_URL" in result["error"]

    def test_non_http_endpoint_is_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("STAGEHAND_ENABLED", "true")
        monkeypatch.setenv("STAGEHAND_API_URL", "ftp://stagehand.example.com")
        result = stagehand_observe("https://example.com")
        assert result["error"] is not None
        assert "STAGEHAND_API_URL" in result["error"]


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Input validation
# ═══════════════════════════════════════════════════════════════════════════════


class TestInputValidation:

    def test_empty_url_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _enabled_env(monkeypatch)
        result = stagehand_act("", "click submit")
        assert result["error"] is not None
        assert "url" in result["error"]

    def test_non_http_url_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        result = stagehand_act("file:///etc/passwd", "read")
        assert result["error"] is not None
        assert "http" in result["error"].lower()

    def test_empty_instruction_rejected_for_act(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        result = stagehand_act("https://example.com", "   ")
        assert result["error"] is not None
        assert "instruction" in result["error"]

    def test_empty_schema_rejected_for_extract(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        result = stagehand_extract("https://example.com", {})
        assert result["error"] is not None
        assert "schema" in result["error"]

    def test_non_dict_schema_rejected_for_extract(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        result = stagehand_extract(
            "https://example.com",
            ["not", "a", "dict"],  # type: ignore[arg-type]
        )
        assert result["error"] is not None
        assert "schema" in result["error"]


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Network failure modes
# ═══════════════════════════════════════════════════════════════════════════════


class TestNetworkFailures:

    def test_timeout_returns_error_dict(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)

        def _raise_timeout(*_args: Any, **_kwargs: Any) -> Any:
            raise TimeoutError("urlopen timed out")

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_raise_timeout,
        ):
            result = stagehand_act(
                "https://example.com",
                "click submit",
                timeout=1,
            )

        assert result["data"] is None
        assert result["error"] is not None
        assert "timed out" in result["error"].lower()
        assert isinstance(result["elapsed_ms"], int)

    def test_url_error_returns_error_dict(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)

        def _raise_urlerror(*_args: Any, **_kwargs: Any) -> Any:
            raise urllib.error.URLError("dns failure")

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_raise_urlerror,
        ):
            result = stagehand_extract(
                "https://example.com",
                {"type": "object"},
            )

        assert result["data"] is None
        assert "dns failure" in (result["error"] or "")

    def test_http_error_status_surfaces_as_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        body = b'{"detail":"unauthorized"}'

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            return_value=_FakeResponse(body, status=401),
        ):
            result = stagehand_observe("https://example.com")

        assert result["data"] is None
        assert "401" in (result["error"] or "")

    def test_invalid_json_response_returns_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            return_value=_FakeResponse(b"not-json", status=200),
        ):
            result = stagehand_act("https://example.com", "click")

        assert result["data"] is None
        assert result["error"] is not None


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Happy path with mocked POST
# ═══════════════════════════════════════════════════════════════════════════════


class TestSuccessShape:

    def test_act_returns_data_and_metadata(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch, STAGEHAND_API_KEY="sk-test")
        body = json.dumps(
            {
                "ok": True,
                "selector": "#submit-btn",
                "cached": True,
            }
        ).encode("utf-8")

        captured: dict[str, Any] = {}

        def _capture(request: Any, timeout: int = 0) -> _FakeResponse:
            captured["url"] = request.full_url
            captured["headers"] = dict(request.header_items())
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            captured["timeout"] = timeout
            return _FakeResponse(body)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_capture,
        ):
            result = stagehand_act(
                "https://example.com/jobs",
                "click apply now",
                timeout=12,
            )

        assert result["error"] is None
        assert result["source"] == "stagehand"
        assert isinstance(result["elapsed_ms"], int)
        assert result["data"] == {
            "ok": True,
            "selector": "#submit-btn",
            "cached": True,
        }

        assert captured["url"].endswith("/v1/act")
        assert captured["timeout"] == 12
        assert captured["payload"]["url"] == "https://example.com/jobs"
        assert captured["payload"]["instruction"] == "click apply now"
        assert captured["payload"]["cache_actions"] is True
        # Header keys are case-insensitive in urllib.request
        header_lower = {k.lower(): v for k, v in captured["headers"].items()}
        assert header_lower.get("x-api-key") == "sk-test"
        assert header_lower.get("content-type") == "application/json"

    def test_act_cache_actions_false_propagates(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        body = json.dumps({"ok": True}).encode("utf-8")
        captured: dict[str, Any] = {}

        def _capture(request: Any, timeout: int = 0) -> _FakeResponse:
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(body)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_capture,
        ):
            stagehand_act(
                "https://example.com",
                "click apply",
                cache_actions=False,
            )

        assert captured["payload"]["cache_actions"] is False

    def test_extract_forwards_schema_and_instruction(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        body = json.dumps({"title": "Senior Engineer"}).encode("utf-8")
        schema = {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "salary": {"type": "string"},
            },
            "required": ["title"],
        }
        captured: dict[str, Any] = {}

        def _capture(request: Any, timeout: int = 0) -> _FakeResponse:
            captured["url"] = request.full_url
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(body)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_capture,
        ):
            result = stagehand_extract(
                "https://example.com/job/123",
                schema,
                instruction="extract title and salary",
            )

        assert result["error"] is None
        assert result["data"] == {"title": "Senior Engineer"}
        assert captured["url"].endswith("/v1/extract")
        assert captured["payload"]["schema"] == schema
        assert captured["payload"]["instruction"] == "extract title and salary"

    def test_observe_works_without_instruction(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch)
        body = json.dumps(
            {"candidates": [{"selector": "#apply"}, {"selector": "#save"}]}
        ).encode("utf-8")
        captured: dict[str, Any] = {}

        def _capture(request: Any, timeout: int = 0) -> _FakeResponse:
            captured["url"] = request.full_url
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeResponse(body)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_capture,
        ):
            result = stagehand_observe("https://example.com")

        assert result["error"] is None
        assert result["data"]["candidates"][0]["selector"] == "#apply"
        assert captured["url"].endswith("/v1/observe")
        # No instruction was passed -- it should not appear in the payload.
        assert "instruction" not in captured["payload"]

    def test_browserbase_api_key_fallback(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If only BROWSERBASE_API_KEY is set, it is used as the auth header."""
        monkeypatch.setenv("STAGEHAND_ENABLED", "true")
        monkeypatch.setenv("STAGEHAND_API_URL", "https://stagehand.example.com/")
        monkeypatch.setenv("BROWSERBASE_API_KEY", "bb-secret")
        body = json.dumps({"ok": True}).encode("utf-8")
        captured_headers: dict[str, str] = {}

        def _capture(request: Any, timeout: int = 0) -> _FakeResponse:
            captured_headers.update(dict(request.header_items()))
            return _FakeResponse(body)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_capture,
        ):
            result = stagehand_act("https://example.com", "click submit")

        assert result["error"] is None
        header_lower = {k.lower(): v for k, v in captured_headers.items()}
        assert header_lower.get("x-api-key") == "bb-secret"

    def test_explicit_api_key_overrides_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enabled_env(monkeypatch, STAGEHAND_API_KEY="env-key")
        body = json.dumps({"ok": True}).encode("utf-8")
        captured_headers: dict[str, str] = {}

        def _capture(request: Any, timeout: int = 0) -> _FakeResponse:
            captured_headers.update(dict(request.header_items()))
            return _FakeResponse(body)

        with mock.patch.object(
            stagehand_wrapper.urllib.request,
            "urlopen",
            side_effect=_capture,
        ):
            stagehand_observe(
                "https://example.com",
                api_key="explicit-key",
            )

        header_lower = {k.lower(): v for k, v in captured_headers.items()}
        assert header_lower.get("x-api-key") == "explicit-key"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Smoke
# ═══════════════════════════════════════════════════════════════════════════════


def test_public_api_is_re_exported() -> None:
    """Sanity check: the package-level imports match the wrapper module."""
    from apis import browser as pkg

    assert pkg.stagehand_act is stagehand_act
    assert pkg.stagehand_extract is stagehand_extract
    assert pkg.stagehand_observe is stagehand_observe
