"""Tests for sentry_integration.py -- webhook handler, parser, API client, healing bridge.

All tests run offline (no Sentry API calls). External interactions are mocked.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pytest

# ── Ensure project root is importable ────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Module import tests
# =============================================================================


class TestModuleImport:
    """Verify the sentry_integration module is importable."""

    def test_module_importable(self) -> None:
        """sentry_integration module must import without errors."""
        import sentry_integration

        assert sentry_integration is not None

    def test_key_classes_exist(self) -> None:
        """Key classes and functions must be available."""
        from sentry_integration import (
            SentryIssueParser,
            SentryHealingBridge,
            SentryAPIClient,
            handle_sentry_webhook,
            get_sentry_status,
            validate_sentry_signature,
            get_healing_bridge,
        )

        assert callable(SentryIssueParser.parse_webhook)
        assert callable(handle_sentry_webhook)
        assert callable(get_sentry_status)
        assert callable(validate_sentry_signature)
        assert callable(get_healing_bridge)


# =============================================================================
# Webhook Signature Validation
# =============================================================================


class TestSignatureValidation:
    """Tests for HMAC-SHA256 webhook signature validation."""

    def test_valid_signature(self) -> None:
        """A correct HMAC-SHA256 signature should validate."""
        from sentry_integration import validate_sentry_signature

        secret = "test-secret-key-12345"
        body = b'{"action":"created","data":{"issue":{"id":"123"}}}'
        expected_sig = hmac.new(
            secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()

        assert validate_sentry_signature(body, expected_sig, secret=secret) is True

    def test_invalid_signature(self) -> None:
        """An incorrect signature should fail validation."""
        from sentry_integration import validate_sentry_signature

        secret = "test-secret-key-12345"
        body = b'{"action":"created"}'

        assert validate_sentry_signature(body, "deadbeef", secret=secret) is False

    def test_empty_signature_header(self) -> None:
        """Missing signature header should fail."""
        from sentry_integration import validate_sentry_signature

        assert validate_sentry_signature(b"body", "", secret="secret") is False

    def test_empty_secret(self) -> None:
        """Missing secret should fail."""
        from sentry_integration import validate_sentry_signature

        assert validate_sentry_signature(b"body", "sig", secret="") is False


# =============================================================================
# Issue Parser
# =============================================================================


def _make_sentry_payload(
    error_type: str = "AttributeError",
    error_message: str = "'NoneType' object has no attribute 'get'",
    filename: str = "app.py",
    line_no: int = 42,
    function: str = "do_GET",
    issue_id: str = "12345",
    action: str = "created",
    event_count: int = 5,
    tags: list | None = None,
) -> dict:
    """Build a minimal Sentry webhook payload for testing."""
    return {
        "action": action,
        "data": {
            "issue": {
                "id": issue_id,
                "title": f"{error_type}: {error_message}",
                "culprit": f"{filename} in {function}",
                "metadata": {
                    "type": error_type,
                    "value": error_message,
                    "filename": filename,
                    "function": function,
                },
                "count": event_count,
                "level": "error",
                "firstSeen": "2026-03-24T00:00:00Z",
                "lastSeen": "2026-03-24T12:00:00Z",
                "tags": tags
                or [
                    {"key": "environment", "value": "production"},
                    {"key": "logger", "value": "django"},
                ],
                "entries": [
                    {
                        "type": "exception",
                        "data": {
                            "values": [
                                {
                                    "type": error_type,
                                    "value": error_message,
                                    "stacktrace": {
                                        "frames": [
                                            {
                                                "filename": "lib/python3.11/threading.py",
                                                "lineNo": 100,
                                                "function": "run",
                                            },
                                            {
                                                "filename": filename,
                                                "lineNo": line_no,
                                                "function": function,
                                                "context_line": "    result = data.get('key')",
                                            },
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                ],
            }
        },
    }


class TestSentryIssueParser:
    """Tests for SentryIssueParser.parse_webhook()."""

    def test_parse_basic_issue(self) -> None:
        """Parse a standard AttributeError issue payload."""
        from sentry_integration import SentryIssueParser

        payload = _make_sentry_payload()
        result = SentryIssueParser.parse_webhook(payload)

        assert result is not None
        assert result["issue_id"] == "12345"
        assert result["error_type"] == "AttributeError"
        assert "NoneType" in result["error_message"]
        assert result["file"] == "app.py"
        assert result["line_number"] == 42
        assert result["function"] == "do_GET"
        assert result["event_count"] == 5
        assert result["fingerprint"]  # non-empty

    def test_parse_keyerror(self) -> None:
        """Parse a KeyError issue."""
        from sentry_integration import SentryIssueParser

        payload = _make_sentry_payload(
            error_type="KeyError",
            error_message="'missing_key'",
            filename="llm_router.py",
            function="route_request",
        )
        result = SentryIssueParser.parse_webhook(payload)

        assert result is not None
        assert result["error_type"] == "KeyError"
        assert result["file"] == "llm_router.py"
        assert result["function"] == "route_request"

    def test_parse_typeerror(self) -> None:
        """Parse a TypeError issue."""
        from sentry_integration import SentryIssueParser

        payload = _make_sentry_payload(
            error_type="TypeError",
            error_message="argument must be str, not None",
        )
        result = SentryIssueParser.parse_webhook(payload)

        assert result is not None
        assert result["error_type"] == "TypeError"

    def test_parse_empty_payload(self) -> None:
        """Empty payload should return None."""
        from sentry_integration import SentryIssueParser

        assert SentryIssueParser.parse_webhook({}) is None
        assert (
            SentryIssueParser.parse_webhook({"action": "created", "data": {}}) is None
        )

    def test_parse_extracts_tags(self) -> None:
        """Tags should be extracted as a dict."""
        from sentry_integration import SentryIssueParser

        payload = _make_sentry_payload(
            tags=[
                {"key": "environment", "value": "production"},
                {"key": "release", "value": "4.0.0"},
            ]
        )
        result = SentryIssueParser.parse_webhook(payload)
        assert result is not None
        assert result["tags"]["environment"] == "production"
        assert result["tags"]["release"] == "4.0.0"

    def test_parse_stacktrace_skips_stdlib(self) -> None:
        """Parser should prefer app frames over stdlib frames."""
        from sentry_integration import SentryIssueParser

        payload = _make_sentry_payload(filename="data_orchestrator.py", line_no=200)
        result = SentryIssueParser.parse_webhook(payload)
        assert result is not None
        assert result["file"] == "data_orchestrator.py"
        assert result["line_number"] == 200

    def test_fingerprint_stability(self) -> None:
        """Same inputs should produce the same fingerprint."""
        from sentry_integration import SentryIssueParser

        payload = _make_sentry_payload()
        r1 = SentryIssueParser.parse_webhook(payload)
        r2 = SentryIssueParser.parse_webhook(payload)
        assert r1 is not None and r2 is not None
        assert r1["fingerprint"] == r2["fingerprint"]

    def test_fingerprint_differs_for_different_errors(self) -> None:
        """Different errors should produce different fingerprints."""
        from sentry_integration import SentryIssueParser

        p1 = _make_sentry_payload(error_type="AttributeError")
        p2 = _make_sentry_payload(error_type="KeyError")
        r1 = SentryIssueParser.parse_webhook(p1)
        r2 = SentryIssueParser.parse_webhook(p2)
        assert r1 is not None and r2 is not None
        assert r1["fingerprint"] != r2["fingerprint"]


# =============================================================================
# Healing Bridge - Pattern Matching
# =============================================================================


class TestHealingBridgePatterns:
    """Tests for SentryHealingBridge error pattern matching."""

    def setup_method(self) -> None:
        """Reset module-level state between tests."""
        import sentry_integration as si

        with si._lock:
            si._fix_timestamps.clear()
            si._issue_attempts.clear()
            si._processed_events.clear()
            si._heal_history.clear()

    def test_attributeerror_str_get_matches(self) -> None:
        """AttributeError: 'str' object has no attribute 'get' should match."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="AttributeError",
            error_message="'str' object has no attribute 'get'",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch(
            "sentry_integration._execute_healing_action", return_value=True
        ):
            result = bridge.process_issue(parsed)

        assert result["handled"] is True
        assert result["fix_type"] == "isinstance_guard"

    def test_nonetype_attribute_matches(self) -> None:
        """AttributeError: 'NoneType' object has no attribute X should match."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="AttributeError",
            error_message="'NoneType' object has no attribute 'items'",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch(
            "sentry_integration._execute_healing_action", return_value=True
        ):
            result = bridge.process_issue(parsed)

        assert result["handled"] is True
        assert result["fix_type"] == "none_check"

    def test_typeerror_str_none_matches(self) -> None:
        """TypeError: argument must be str, not None should match."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="TypeError",
            error_message="first argument must be str, not None",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch(
            "sentry_integration._execute_healing_action", return_value=True
        ):
            result = bridge.process_issue(parsed)

        assert result["handled"] is True
        assert result["fix_type"] == "or_empty_string"

    def test_keyerror_matches(self) -> None:
        """KeyError should match dict_get_default pattern."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="KeyError",
            error_message="'missing_key'",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch(
            "sentry_integration._execute_healing_action", return_value=True
        ):
            result = bridge.process_issue(parsed)

        assert result["handled"] is True
        assert result["fix_type"] == "dict_get_default"

    def test_indexerror_matches(self) -> None:
        """IndexError should match bounds_check pattern."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="IndexError",
            error_message="list index out of range",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch(
            "sentry_integration._execute_healing_action", return_value=True
        ):
            result = bridge.process_issue(parsed)

        assert result["handled"] is True
        assert result["fix_type"] == "bounds_check"

    def test_unknown_pattern_not_handled(self) -> None:
        """An unknown error pattern should NOT be handled."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="CustomWeirdError",
            error_message="something very unusual happened",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch("sentry_integration._email_alert", return_value=True):
            result = bridge.process_issue(parsed)

        assert result["handled"] is False
        assert result["reason"] == "unknown_pattern"

    def test_connectionerror_matches_network_retry(self) -> None:
        """ConnectionError should match network_retry pattern."""
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(
            error_type="ConnectionError",
            error_message="Connection refused",
        )
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        with mock.patch(
            "sentry_integration._execute_healing_action", return_value=True
        ):
            result = bridge.process_issue(parsed)

        assert result["handled"] is True
        assert result["fix_type"] == "network_retry"


# =============================================================================
# Rate Limiting and Loop Prevention
# =============================================================================


class TestRateLimitingAndLoopPrevention:
    """Tests for rate limiting and fix attempt tracking."""

    def setup_method(self) -> None:
        """Reset module-level state."""
        import sentry_integration as si

        with si._lock:
            si._fix_timestamps.clear()
            si._issue_attempts.clear()
            si._processed_events.clear()
            si._heal_history.clear()

    def test_rate_limit_blocks_after_max(self) -> None:
        """After MAX_FIXES_PER_HOUR, further fixes should be blocked."""
        import sentry_integration as si
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        # Fill up the rate limit
        with si._lock:
            si._fix_timestamps[:] = [time.time()] * si._MAX_FIXES_PER_HOUR

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(issue_id="rate-test-1")
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        result = bridge.process_issue(parsed)
        assert result["handled"] is False
        assert result["reason"] == "rate_limited"

    def test_loop_prevention_blocks_after_max_attempts(self) -> None:
        """After MAX_ATTEMPTS_PER_ISSUE, further attempts should be blocked."""
        import sentry_integration as si
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(issue_id="loop-test-1")
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None
        fingerprint = parsed["fingerprint"]

        # Fill up attempts for this fingerprint
        with si._lock:
            si._issue_attempts[fingerprint] = [time.time()] * si._MAX_ATTEMPTS_PER_ISSUE

        with mock.patch("sentry_integration._email_alert", return_value=True):
            result = bridge.process_issue(parsed)

        assert result["handled"] is False
        assert result["reason"] == "max_attempts"

    def test_dedup_suppresses_recent_events(self) -> None:
        """Same event processed within dedup window should be suppressed."""
        import sentry_integration as si
        from sentry_integration import SentryHealingBridge, SentryIssueParser

        bridge = SentryHealingBridge()
        payload = _make_sentry_payload(issue_id="dedup-test-1")
        parsed = SentryIssueParser.parse_webhook(payload)
        assert parsed is not None

        # Pre-populate the dedup cache
        event_key = f"{parsed['issue_id']}:{parsed['fingerprint']}"
        with si._lock:
            si._processed_events[event_key] = time.time()

        result = bridge.process_issue(parsed)
        assert result["handled"] is False
        assert result["reason"] == "dedup"


# =============================================================================
# Webhook Handler
# =============================================================================


class TestWebhookHandler:
    """Tests for the handle_sentry_webhook() top-level function."""

    def setup_method(self) -> None:
        """Reset module-level state."""
        import sentry_integration as si

        with si._lock:
            si._fix_timestamps.clear()
            si._issue_attempts.clear()
            si._processed_events.clear()
            si._heal_history.clear()

    def test_invalid_signature_returns_401(self) -> None:
        """Invalid signature should return 401."""
        from sentry_integration import handle_sentry_webhook

        body = json.dumps(_make_sentry_payload()).encode()

        with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", "my-secret"):
            status, result = handle_sentry_webhook(body, "wrong-sig")

        assert status == 401
        assert "Invalid signature" in result.get("error", "")

    def test_valid_signature_processes_issue(self) -> None:
        """Valid signature should process the issue."""
        from sentry_integration import handle_sentry_webhook

        secret = "test-secret"
        payload = _make_sentry_payload()
        body = json.dumps(payload).encode()
        sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", secret):
            with mock.patch(
                "sentry_integration._execute_healing_action", return_value=True
            ):
                status, result = handle_sentry_webhook(body, sig)

        assert status == 200
        assert result.get("ok") is True

    def test_no_secret_skips_validation(self) -> None:
        """When no webhook secret is configured, skip validation."""
        from sentry_integration import handle_sentry_webhook

        payload = _make_sentry_payload()
        body = json.dumps(payload).encode()

        with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", ""):
            with mock.patch(
                "sentry_integration._execute_healing_action", return_value=True
            ):
                status, result = handle_sentry_webhook(body, "")

        assert status == 200

    def test_invalid_json_returns_400(self) -> None:
        """Invalid JSON body should return 400."""
        from sentry_integration import handle_sentry_webhook

        with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", ""):
            status, result = handle_sentry_webhook(b"not-json{{{", "")

        assert status == 400
        assert "Invalid JSON" in result.get("error", "")

    def test_non_issue_resource_ignored(self) -> None:
        """Non-issue webhook resources (e.g. metric_alert) should be ignored."""
        from sentry_integration import handle_sentry_webhook

        body = json.dumps({"action": "triggered"}).encode()

        with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", ""):
            status, result = handle_sentry_webhook(
                body, "", resource_header="metric_alert"
            )

        assert status == 200
        assert result.get("action") == "ignored"

    def test_empty_payload_ignored(self) -> None:
        """Empty payload with no issue data should be ignored gracefully."""
        from sentry_integration import handle_sentry_webhook

        body = json.dumps({"action": "created", "data": {}}).encode()

        with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", ""):
            status, result = handle_sentry_webhook(body, "")

        assert status == 200
        assert result.get("action") == "ignored"


# =============================================================================
# Status Endpoint
# =============================================================================


class TestSentryStatus:
    """Tests for the get_sentry_status() function."""

    def test_status_returns_expected_keys(self) -> None:
        """Status dict should have all expected keys."""
        from sentry_integration import get_sentry_status

        status = get_sentry_status()
        assert "configured" in status
        assert "stats" in status
        assert "recent_heals" in status
        assert "known_patterns" in status
        assert isinstance(status["configured"], dict)
        assert isinstance(status["stats"], dict)
        assert isinstance(status["recent_heals"], list)
        assert status["known_patterns"] > 0

    def test_status_stats_structure(self) -> None:
        """Stats should have fix rate and tracking info."""
        from sentry_integration import get_sentry_status

        stats = get_sentry_status()["stats"]
        assert "fixes_this_hour" in stats
        assert "max_fixes_per_hour" in stats
        assert stats["max_fixes_per_hour"] == 10


# =============================================================================
# Fix Suggestion Generator
# =============================================================================


class TestFixSuggestionGenerator:
    """Tests for _generate_fix_suggestion()."""

    def test_isinstance_guard_suggestion(self) -> None:
        """isinstance_guard should mention isinstance check."""
        from sentry_integration import _generate_fix_suggestion

        result = _generate_fix_suggestion(
            fix_type="isinstance_guard",
            error_type="AttributeError",
            error_message="str has no get",
            file_path="app.py",
            function_name="do_GET",
            line_number=42,
            context_line="",
        )
        assert "isinstance" in result
        assert "app.py:42" in result

    def test_none_check_suggestion(self) -> None:
        """none_check should mention None guard."""
        from sentry_integration import _generate_fix_suggestion

        result = _generate_fix_suggestion(
            fix_type="none_check",
            error_type="AttributeError",
            error_message="NoneType has no X",
            file_path="llm_router.py",
            function_name="route",
            line_number=100,
            context_line="",
        )
        assert "None" in result

    def test_or_empty_string_suggestion(self) -> None:
        """or_empty_string should mention or '' pattern."""
        from sentry_integration import _generate_fix_suggestion

        result = _generate_fix_suggestion(
            fix_type="or_empty_string",
            error_type="TypeError",
            error_message="must be str, not None",
            file_path="app.py",
            function_name="handle",
            line_number=50,
            context_line="",
        )
        assert 'or ""' in result

    def test_unknown_fix_type(self) -> None:
        """Unknown fix type should return a fallback message."""
        from sentry_integration import _generate_fix_suggestion

        result = _generate_fix_suggestion(
            fix_type="mystery_fix",
            error_type="X",
            error_message="Y",
            file_path="z.py",
            function_name="w",
            line_number=1,
            context_line="",
        )
        assert "Unknown fix type" in result


# =============================================================================
# Sentry API Client (mocked)
# =============================================================================


class TestSentryAPIClient:
    """Tests for SentryAPIClient (all API calls are mocked)."""

    def test_fetch_recent_issues_no_token(self) -> None:
        """Without auth token, should return empty list."""
        from sentry_integration import SentryAPIClient

        with mock.patch("sentry_integration._SENTRY_AUTH_TOKEN", ""):
            result = SentryAPIClient.fetch_recent_issues()

        assert result == []

    @mock.patch("urllib.request.urlopen")
    def test_fetch_recent_issues_success(self, mock_urlopen: mock.Mock) -> None:
        """Successful API call should return issue list."""
        from sentry_integration import SentryAPIClient

        mock_response = mock.MagicMock()
        mock_response.read.return_value = json.dumps(
            [{"id": "1", "title": "Error"}, {"id": "2", "title": "Another"}]
        ).encode()
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = mock.Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        with mock.patch("sentry_integration._SENTRY_AUTH_TOKEN", "fake-token"):
            result = SentryAPIClient.fetch_recent_issues()

        assert len(result) == 2
        assert result[0]["id"] == "1"

    def test_resolve_issue_no_token(self) -> None:
        """Without auth token, resolve should return False."""
        from sentry_integration import SentryAPIClient

        with mock.patch("sentry_integration._SENTRY_AUTH_TOKEN", ""):
            assert SentryAPIClient.resolve_issue("123") is False

    def test_add_comment_no_token(self) -> None:
        """Without auth token, add_comment should return False."""
        from sentry_integration import SentryAPIClient

        with mock.patch("sentry_integration._SENTRY_AUTH_TOKEN", ""):
            assert SentryAPIClient.add_comment("123", "test") is False

    def test_resolve_issue_empty_id(self) -> None:
        """Empty issue ID should return False."""
        from sentry_integration import SentryAPIClient

        with mock.patch("sentry_integration._SENTRY_AUTH_TOKEN", "fake"):
            assert SentryAPIClient.resolve_issue("") is False


# =============================================================================
# Helper Functions
# =============================================================================


class TestHelperFunctions:
    """Tests for internal helper functions."""

    def test_file_to_module_basic(self) -> None:
        """Basic .py file should convert to module name."""
        from sentry_integration import _file_to_module

        assert _file_to_module("app.py") == "app"
        assert _file_to_module("llm_router.py") == "llm_router"
        assert _file_to_module("path/to/data_orchestrator.py") == "data_orchestrator"

    def test_file_to_module_empty(self) -> None:
        """Empty input should return empty string."""
        from sentry_integration import _file_to_module

        assert _file_to_module("") == ""

    def test_extract_error_type_standard(self) -> None:
        """Standard error class names should be extracted."""
        from sentry_integration import _extract_error_type

        assert _extract_error_type("AttributeError: 'NoneType'...") == "AttributeError"
        assert _extract_error_type("KeyError: 'key'") == "KeyError"
        assert _extract_error_type("TypeError: ...") == "TypeError"
        assert (
            _extract_error_type("IndexError: list index out of range") == "IndexError"
        )

    def test_extract_error_type_unknown(self) -> None:
        """Non-standard titles should return 'Unknown'."""
        from sentry_integration import _extract_error_type

        assert _extract_error_type("something went wrong") == "Unknown"
        assert _extract_error_type("") == "Unknown"

    def test_compute_fingerprint_deterministic(self) -> None:
        """Same inputs should always produce the same fingerprint."""
        from sentry_integration import _compute_fingerprint

        fp1 = _compute_fingerprint(
            "TypeError", "msg", {"file": "a.py", "line_number": 1, "function": "f"}
        )
        fp2 = _compute_fingerprint(
            "TypeError", "msg", {"file": "a.py", "line_number": 1, "function": "f"}
        )
        assert fp1 == fp2
        assert len(fp1) == 16


# =============================================================================
# AutoQC Sentry Integration
# =============================================================================


class TestAutoQCSentryIntegration:
    """Tests for AutoQC + Sentry integration (refactored interface)."""

    def test_autoqc_importable(self) -> None:
        """AutoQC class should be importable."""
        from auto_qc import AutoQC

        qc = AutoQC()
        assert qc is not None

    def test_autoqc_status_returns_dict(self) -> None:
        """AutoQC get_status should return a dict with status key."""
        from auto_qc import get_status

        status = get_status()
        assert isinstance(status, dict)
        assert "status" in status

    def test_sentry_healing_bridge_exists(self) -> None:
        """SentryHealingBridge should be importable from sentry_integration."""
        from sentry_integration import SentryHealingBridge

        bridge = SentryHealingBridge()
        assert bridge is not None
        assert hasattr(bridge, "process_issue")

    def test_sentry_status_returns_dict(self) -> None:
        """get_sentry_status should return a dict."""
        from sentry_integration import get_sentry_status

        status = get_sentry_status()
        assert isinstance(status, dict)
        assert "configured" in status


# =============================================================================
# Thread Safety
# =============================================================================


class TestThreadSafety:
    """Basic thread safety tests for concurrent access."""

    def setup_method(self) -> None:
        """Reset module-level state."""
        import sentry_integration as si

        with si._lock:
            si._fix_timestamps.clear()
            si._issue_attempts.clear()
            si._processed_events.clear()
            si._heal_history.clear()

    def test_concurrent_webhook_processing(self) -> None:
        """Multiple threads processing webhooks should not crash."""
        from sentry_integration import handle_sentry_webhook

        errors: list[Exception] = []

        def _process(idx: int) -> None:
            try:
                payload = _make_sentry_payload(issue_id=f"thread-{idx}")
                body = json.dumps(payload).encode()
                with mock.patch("sentry_integration._SENTRY_WEBHOOK_SECRET", ""):
                    with mock.patch(
                        "sentry_integration._execute_healing_action", return_value=True
                    ):
                        status, result = handle_sentry_webhook(body, "")
                assert status == 200
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_process, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0, f"Thread errors: {errors}"

    def test_concurrent_status_reads(self) -> None:
        """Multiple threads reading status should not crash."""
        from sentry_integration import get_sentry_status

        errors: list[Exception] = []

        def _read() -> None:
            try:
                status = get_sentry_status()
                assert "configured" in status
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_read) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(errors) == 0
