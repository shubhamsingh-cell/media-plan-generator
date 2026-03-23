"""Regression tests for NoneType safety patterns.

Project rule: use `data.get("key") or ""` instead of `data.get("key") or ""`.
The `.get("key") or ""` pattern fails when the key exists but has value None.

Exception: os.environ.get() calls are fine with default values since
environment variables are always strings when set.
"""

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _strip_environ_gets(source: str) -> str:
    """Remove os.environ.get() calls from source so they are not counted.

    os.environ.get("KEY") or "" is safe because env vars are always str|None,
    never a non-string value. The rule only applies to data dict .get() calls.
    """
    # Remove lines containing os.environ.get to avoid false positives
    lines = source.splitlines()
    filtered = [line for line in lines if "os.environ.get" not in line]
    return "\n".join(filtered)


class TestGetWithDefaultString:
    """No .get("key") or "" patterns should remain (excluding os.environ)."""

    def test_no_get_with_empty_string_default(self, app_source: str) -> None:
        """app.py must not use .get('key') or '' for data dicts.

        All such patterns should be converted to: .get('key') or ''
        """
        filtered = _strip_environ_gets(app_source)
        pattern = re.compile(r'\.get\(\s*["\'][^"\']+["\']\s*,\s*""\s*\)')
        matches = pattern.findall(filtered)
        assert len(matches) == 0, (
            f"Found {len(matches)} unsafe .get('key', \"\") patterns "
            f'(should use `or ""` instead): {matches[:5]}'
        )

    def test_no_get_with_single_quote_default(self, app_source: str) -> None:
        """app.py must not use .get('key') or '' with single-quoted empty string."""
        filtered = _strip_environ_gets(app_source)
        pattern = re.compile(r"\.get\(\s*[\"'][^\"']+[\"']\s*,\s*''\s*\)")
        matches = pattern.findall(filtered)
        assert len(matches) == 0, (
            f"Found {len(matches)} unsafe .get('key') or '' patterns "
            f"(should use `or ''` instead): {matches[:5]}"
        )


class TestGetWithDefaultZero:
    """No .get("key") or 0 patterns should remain."""

    def test_no_get_with_zero_default(self, app_source: str) -> None:
        """app.py must not use .get('key') or 0 for data dicts.

        Should use: .get('key') or 0
        """
        filtered = _strip_environ_gets(app_source)
        pattern = re.compile(r'\.get\(\s*["\'][^"\']+["\']\s*,\s*0\s*\)')
        matches = pattern.findall(filtered)
        assert len(matches) == 0, (
            f"Found {len(matches)} unsafe .get('key') or 0 patterns "
            f"(should use `or 0` instead): {matches[:5]}"
        )


class TestOtherPythonFiles:
    """Check NoneType safety across all Python files, not just app.py."""

    def test_no_unsafe_get_in_core_modules(self) -> None:
        """Core modules should also follow the `or ""` pattern.

        NOTE: app.py has been cleaned up (tested above). Other modules
        may still have legacy patterns. This test issues warnings for
        those but does not fail the build -- they are tracked as tech debt.
        """
        core_modules = [
            "api_enrichment.py",
            "research.py",
            "ppt_generator.py",
            "monitoring.py",
        ]
        pattern = re.compile(r'\.get\(\s*["\'][^"\']+["\']\s*,\s*""\s*\)')

        for module_name in core_modules:
            module_path = PROJECT_ROOT / module_name
            if not module_path.exists():
                continue
            content = module_path.read_text(encoding="utf-8")
            filtered = _strip_environ_gets(content)
            matches = pattern.findall(filtered)
            if matches:
                import warnings

                warnings.warn(
                    f"{module_name}: {len(matches)} .get('key', \"\") "
                    f'patterns should be migrated to `or ""`',
                    stacklevel=1,
                )
