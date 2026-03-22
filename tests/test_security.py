"""Security checks for the Nova AI Suite codebase."""

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


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
