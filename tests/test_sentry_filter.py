"""Tests for the Sentry before_send PII / secret redaction filter.

Covers the S50 (May 2026) addition of explicit env-var, URL, and header
scrubbing in ``app._redact_sentry_event``.

All tests run offline -- no Sentry SDK is initialised because SENTRY_DSN is
left empty during import. The redaction helper is defined at module scope
specifically so it is testable without spinning up the SDK.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

# Ensure project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Module surface
# =============================================================================


class TestRedactionSurface:
    """The redaction symbols must be importable from app at module scope."""

    def test_helper_is_importable(self) -> None:
        from app import _redact_sentry_event

        assert callable(_redact_sentry_event)

    def test_constants_present(self) -> None:
        from app import (
            _REDACTED,
            _SENSITIVE_ENV_VARS,
            _SENSITIVE_KEY_FRAGMENTS,
        )

        assert isinstance(_REDACTED, str) and _REDACTED
        assert isinstance(_SENSITIVE_ENV_VARS, frozenset)
        assert isinstance(_SENSITIVE_KEY_FRAGMENTS, tuple)
        # spot-check a handful of names from the agreed S50 set
        for name in (
            "ANTHROPIC_API_KEY",
            "LANGFUSE_SECRET_KEY",
            "LITELLM_MASTER_KEY",
            "STAGEHAND_API_KEY",
            "BROWSERBASE_API_KEY",
            "STRIPE_SECRET_KEY",
            "PDL_API_KEY",
            "CRUNCHBASE_API_KEY",
        ):
            assert (
                name in _SENSITIVE_ENV_VARS
            ), f"{name} missing from _SENSITIVE_ENV_VARS"


# =============================================================================
# Env-var redaction (extras / contexts)
# =============================================================================


class TestEnvVarRedactionInExtras:
    """Sensitive env-var names appearing as keys in event['extra'] must be redacted."""

    @pytest.mark.parametrize(
        "var_name",
        [
            "LANGFUSE_PUBLIC_KEY",
            "LANGFUSE_SECRET_KEY",
            "LANGFUSE_HOST",
            "LITELLM_API_KEY",
            "LITELLM_MASTER_KEY",
            "CRAWL4AI_API_KEY",
            "CRAWL4AI_API_TOKEN",
            "STAGEHAND_API_KEY",
            "STAGEHAND_API_URL",
            "BROWSERBASE_API_KEY",
            "STRIPE_SECRET_KEY",
            "PDL_API_KEY",
            "CRUNCHBASE_API_KEY",
        ],
    )
    def test_each_new_s50_var_redacted_from_extras(self, var_name: str) -> None:
        from app import _REDACTED, _redact_sentry_event

        original = f"super-secret-value-for-{var_name.lower()}"
        event: dict[str, Any] = {"extra": {var_name: original}}
        result = _redact_sentry_event(event)

        assert result is not None
        assert result["extra"][var_name] == _REDACTED

    def test_nested_sensitive_key_is_redacted(self) -> None:
        """Sensitive keys nested deep in contexts should still be scrubbed."""
        from app import _REDACTED, _redact_sentry_event

        event: dict[str, Any] = {
            "contexts": {
                "runtime": {
                    "config": {
                        "auth": {"BROWSERBASE_API_KEY": "real-key-abc-1234567"},
                    }
                }
            }
        }
        result = _redact_sentry_event(event)
        assert result is not None
        assert (
            result["contexts"]["runtime"]["config"]["auth"]["BROWSERBASE_API_KEY"]
            == _REDACTED
        )

    def test_non_sensitive_key_survives(self) -> None:
        from app import _redact_sentry_event

        event = {"extra": {"user_count": 42, "build_id": "abc123"}}
        result = _redact_sentry_event(event)

        assert result is not None
        assert result["extra"]["user_count"] == 42
        assert result["extra"]["build_id"] == "abc123"

    def test_value_appearing_in_arbitrary_string_is_redacted(self) -> None:
        """A secret value placed into the env should be scrubbed even when it
        appears inside a non-sensitive key (e.g. inside a free-text message)."""
        from app import _REDACTED, _redact_sentry_event

        secret = "lf_pk_real_value_abcdef1234567890"
        with mock.patch.dict(os.environ, {"LANGFUSE_PUBLIC_KEY": secret}):
            event = {
                "extra": {
                    "debug_note": f"Investigating crash; key was {secret} at the time.",
                },
            }
            result = _redact_sentry_event(event)

        assert result is not None
        out = result["extra"]["debug_note"]
        assert secret not in out
        assert _REDACTED in out


# =============================================================================
# URL redaction
# =============================================================================


class TestUrlRedaction:
    """Auth credentials in URLs (query strings + basic auth) must be scrubbed."""

    @pytest.mark.parametrize(
        "url, leaked",
        [
            ("https://api.example.com/x?api_key=sk-12345abcde", "sk-12345abcde"),
            ("https://api.example.com/x?apikey=sk-12345abcde", "sk-12345abcde"),
            ("https://api.example.com/x?token=abc-1234-token", "abc-1234-token"),
            (
                "https://api.crunchbase.com/v4/x?user_key=secret-cb-key-99",
                "secret-cb-key-99",
            ),
            (
                "https://api.example.com/x?langfuse_public_key=lf_pk_xyz",
                "lf_pk_xyz",
            ),
            (
                "https://api.example.com/x?langfuse_secret_key=lf_sk_xyz",
                "lf_sk_xyz",
            ),
            (
                "https://api.example.com/x?stripe_secret_key=sk_live_xyz",
                "sk_live_xyz",
            ),
            (
                "https://api.example.com/x?pdl_api_key=pdl-real-key",
                "pdl-real-key",
            ),
            (
                "https://api.example.com/x?access_token=at-abc-1234",
                "at-abc-1234",
            ),
        ],
    )
    def test_query_string_secrets_are_redacted(self, url: str, leaked: str) -> None:
        from app import _REDACTED, _redact_sentry_event

        event = {"request": {"url": url}}
        result = _redact_sentry_event(event)
        assert result is not None

        new_url = result["request"]["url"]
        assert leaked not in new_url, f"Secret leaked in: {new_url}"
        assert _REDACTED in new_url

    def test_non_sensitive_query_params_survive(self) -> None:
        from app import _redact_sentry_event

        url = "https://api.example.com/v1/items?page=2&sort=desc&q=hello"
        event = {"request": {"url": url}}
        result = _redact_sentry_event(event)
        assert result is not None
        # Nothing should change for benign query strings.
        assert result["request"]["url"] == url

    def test_basic_auth_in_url_is_redacted(self) -> None:
        from app import _REDACTED, _redact_sentry_event

        event = {
            "request": {
                "url": "https://admin:hunter2-real-pw@db.internal.example.com/path"
            }
        }
        result = _redact_sentry_event(event)
        assert result is not None
        new_url = result["request"]["url"]
        assert "hunter2-real-pw" not in new_url
        assert _REDACTED in new_url
        # Username should still be visible (not a secret on its own)
        assert "admin" in new_url


# =============================================================================
# Header redaction
# =============================================================================


class TestHeaderRedaction:
    """Authorization-style headers must be scrubbed regardless of casing."""

    def test_authorization_bearer_in_string_is_redacted(self) -> None:
        from app import _REDACTED, _redact_sentry_event

        event = {
            "extra": {
                "raw_curl": (
                    "curl -H 'Authorization: Bearer sk-very-secret-token-1234567' "
                    "https://api.example.com/x"
                ),
            }
        }
        result = _redact_sentry_event(event)
        assert result is not None
        assert "sk-very-secret-token-1234567" not in result["extra"]["raw_curl"]
        assert _REDACTED in result["extra"]["raw_curl"]

    def test_authorization_header_dict_value_is_redacted_by_key(self) -> None:
        from app import _REDACTED, _redact_sentry_event

        event = {
            "request": {
                "headers": {
                    "Authorization": "Bearer sk-leaked-token",
                    "X-Trace-Id": "trace-123",
                }
            }
        }
        result = _redact_sentry_event(event)
        assert result is not None
        # 'Authorization' is a sensitive key fragment, value should be fully replaced
        assert result["request"]["headers"]["Authorization"] == _REDACTED
        # Non-sensitive header survives
        assert result["request"]["headers"]["X-Trace-Id"] == "trace-123"

    def test_cookie_header_redacted_by_key(self) -> None:
        from app import _REDACTED, _redact_sentry_event

        event = {"request": {"headers": {"Cookie": "session=abc123; csrf=xyz"}}}
        result = _redact_sentry_event(event)
        assert result is not None
        assert result["request"]["headers"]["Cookie"] == _REDACTED


# =============================================================================
# Robustness
# =============================================================================


class TestRedactionRobustness:
    """Scrub must be safe on edge-case inputs and never raise."""

    def test_none_event_returns_none(self) -> None:
        from app import _redact_sentry_event

        assert _redact_sentry_event(None) is None

    def test_non_dict_event_passthrough(self) -> None:
        from app import _redact_sentry_event

        # Defensive: Sentry will only ever pass dicts, but we shouldn't crash.
        result = _redact_sentry_event("not-a-dict")  # type: ignore[arg-type]
        assert result == "not-a-dict"

    def test_deeply_nested_structure_does_not_recurse_forever(self) -> None:
        """Bounded recursion (depth <= 12) prevents stack-overflow on deep trees."""
        from app import _redact_sentry_event

        deep: dict[str, Any] = {}
        cur = deep
        for i in range(40):
            nxt: dict[str, Any] = {"level": i}
            cur["next"] = nxt
            cur = nxt
        cur["LANGFUSE_SECRET_KEY"] = "way-too-deep-to-redact"
        # Should not raise; keys above depth 12 stay as-is, but it must complete.
        result = _redact_sentry_event(deep)
        assert isinstance(result, dict)

    def test_smoke_round_trip_matches_documented_example(self) -> None:
        """Reproduces the inline smoke test from the S50 task brief."""
        from app import _redact_sentry_event

        with mock.patch.dict(
            os.environ,
            {"LANGFUSE_SECRET_KEY": "sk-real-key-1234567890abcdef"},
        ):
            test_event = {
                "request": {
                    "url": "https://api.example.com/?api_key=sk-abc1234567",
                },
                "extra": {"LANGFUSE_SECRET_KEY": "sk-real-key-1234567890abcdef"},
            }
            filtered = _redact_sentry_event(test_event)

        assert filtered is not None
        as_text = json.dumps(filtered)
        assert "sk-abc1234567" not in as_text
        assert "sk-real-key-1234567890abcdef" not in as_text


# =============================================================================
# Compatibility with the noise filter
# =============================================================================


class TestExistingNoiseFilterPreserved:
    """Adding the redaction step must not regress any existing drop semantics
    of the original before_send filter (logger-name drops, transient errors,
    rate-limits, etc.). We only verify a couple of representative cases here;
    full coverage lives in the operational logs."""

    def test_redaction_does_not_remove_top_level_event_keys(self) -> None:
        from app import _redact_sentry_event

        event = {
            "level": "error",
            "logger": "app",
            "message": "Something went wrong",
            "request": {"url": "https://example.com/?ok=1"},
            "extra": {"foo": "bar"},
        }
        result = _redact_sentry_event(event)

        assert result is not None
        for key in ("level", "logger", "message", "request", "extra"):
            assert key in result
        assert result["level"] == "error"
        assert result["logger"] == "app"
