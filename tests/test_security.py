"""Security checks for the Nova AI Suite codebase."""

import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestAdminKeySecurity:
    """Admin key must not be hardcoded as a default value."""

    def test_no_hardcoded_admin_key_default(self, app_source: str) -> None:
        """app.py should not use 'Chandel13' as a hardcoded default value.

        Acceptable: os.environ.get("ADMIN_KEY") comparisons.
        Not acceptable: ADMIN_KEY = "Chandel13" or default="Chandel13".
        """
        # Look for assignment patterns with the hardcoded key
        dangerous_patterns = [
            r'=\s*["\']Chandel13["\']',
            r'default\s*=\s*["\']Chandel13["\']',
        ]
        for pattern in dangerous_patterns:
            matches = re.findall(pattern, app_source)
            assert len(matches) == 0, f"Found hardcoded admin key pattern: {matches}"


class TestBareExcepts:
    """No bare except clauses allowed in any Python file."""

    def test_no_bare_except_in_py_files(self, python_files: list[Path]) -> None:
        """All .py files must catch specific exceptions, never bare except:."""
        violations: list[str] = []
        bare_except_re = re.compile(r"^\s+except\s*:\s*$", re.MULTILINE)

        for py_file in python_files:
            content = py_file.read_text(encoding="utf-8")
            matches = bare_except_re.findall(content)
            if matches:
                violations.append(f"{py_file.name}: {len(matches)} bare except(s)")

        assert not violations, f"Bare except: found in: {'; '.join(violations)}"


class TestSecurityHeaders:
    """Required security headers must be defined in the server."""

    REQUIRED_HEADERS = [
        "X-Content-Type-Options",
        "X-Frame-Options",
        "X-XSS-Protection",
    ]

    @pytest.mark.parametrize("header", REQUIRED_HEADERS)
    def test_security_header_defined(self, header: str, app_source: str) -> None:
        """Each security header must appear in app.py."""
        assert header in app_source, f"Security header {header} not found in app.py"

    def test_content_security_policy_defined(self, app_source: str) -> None:
        """Content-Security-Policy header must be set."""
        assert (
            "Content-Security-Policy" in app_source
        ), "Content-Security-Policy header not found in app.py"


class TestCORSPolicy:
    """CORS must not use wildcard origin."""

    def test_no_wildcard_cors(self, app_source: str) -> None:
        """CORS Access-Control-Allow-Origin must not be set to '*'."""
        # Check that there is no wildcard CORS
        wildcard_cors = re.findall(
            r'Access-Control-Allow-Origin["\'],\s*["\']\*["\']', app_source
        )
        assert (
            len(wildcard_cors) == 0
        ), "CORS is using wildcard '*' origin -- must use allowlist"

    def test_allowed_origins_is_set(self, app_source: str) -> None:
        """An explicit CORS allowlist must be defined."""
        assert (
            "_ALLOWED_ORIGINS" in app_source or "ALLOWED_ORIGINS" in app_source
        ), "No CORS allowlist (ALLOWED_ORIGINS) found in app.py"


class TestCSRFDoubleSubmit:
    """CSRF must use the cookie-based double-submit pattern (not IP-based)."""

    def test_no_ip_based_csrf_store(self, app_source: str) -> None:
        """Server-side per-IP CSRF token storage must not exist."""
        assert (
            "_csrf_tokens" not in app_source
        ), "Found _csrf_tokens dict -- IP-based CSRF store should be removed"
        assert (
            "_csrf_lock" not in app_source
        ), "Found _csrf_lock -- IP-based CSRF lock should be removed"

    def test_csrf_cookie_is_set(self, app_source: str) -> None:
        """The /api/csrf-token endpoint must set a Set-Cookie header."""
        assert (
            "Set-Cookie" in app_source
        ), "Set-Cookie header not found -- CSRF cookie must be set"
        assert "csrf_token=" in app_source, "csrf_token= cookie not found in app.py"

    def test_csrf_cookie_flags(self, app_source: str) -> None:
        """CSRF cookie must have HttpOnly and SameSite=Strict flags."""
        assert "HttpOnly" in app_source, "CSRF cookie missing HttpOnly flag"
        assert (
            "SameSite=Strict" in app_source
        ), "CSRF cookie missing SameSite=Strict flag"

    def test_csrf_double_submit_validation(self, app_source: str) -> None:
        """POST validation must compare cookie token with header token."""
        assert (
            "_validate_csrf_double_submit" in app_source
        ), "Double-submit validation function not found"
        assert (
            "compare_digest" in app_source
        ), "Constant-time comparison (hmac.compare_digest) not used"

    def test_csrf_functions_unit(self) -> None:
        """Unit test the CSRF helper functions directly."""
        from app import (
            _build_csrf_cookie,
            _generate_csrf_token,
            _parse_cookie_value,
            _validate_csrf_double_submit,
        )

        # Token generation
        t1 = _generate_csrf_token()
        t2 = _generate_csrf_token()
        assert len(t1) == 64, "Token should be 64 hex chars"
        assert t1 != t2, "Tokens must be unique"

        # Cookie building -- HTTPS
        cookie = _build_csrf_cookie("abc123", secure=True)
        assert "csrf_token=abc123" in cookie
        assert "HttpOnly" in cookie
        assert "SameSite=Strict" in cookie
        assert "Secure" in cookie

        # Cookie building -- HTTP (no Secure flag)
        cookie_http = _build_csrf_cookie("abc123", secure=False)
        assert "Secure" not in cookie_http

        # Cookie parsing
        assert _parse_cookie_value("csrf_token=abc; other=1", "csrf_token") == "abc"
        assert _parse_cookie_value("other=1; csrf_token=xyz", "csrf_token") == "xyz"
        assert _parse_cookie_value("other=1", "csrf_token") == ""
        assert _parse_cookie_value("", "csrf_token") == ""

        # Double-submit validation
        assert _validate_csrf_double_submit("tok", "tok") is True
        assert _validate_csrf_double_submit("tok", "bad") is False
        assert _validate_csrf_double_submit("", "tok") is False
        assert _validate_csrf_double_submit("tok", "") is False
        assert _validate_csrf_double_submit("", "") is False
