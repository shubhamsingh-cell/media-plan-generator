"""Shared fixtures for Nova AI Suite test suite.

The server uses http.server.HTTPServer which blocks on serve_forever(),
so tests focus on static analysis, template validation, data integrity,
and security checks rather than live HTTP testing.
"""

import os
import sys
from pathlib import Path
from typing import Generator

import pytest

# Ensure the project root is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

TEMPLATES_DIR = PROJECT_ROOT / "templates"
DATA_DIR = PROJECT_ROOT / "data"


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def templates_dir() -> Path:
    """Return the templates directory."""
    return TEMPLATES_DIR


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Return the data directory."""
    return DATA_DIR


@pytest.fixture(scope="session")
def template_files() -> list[Path]:
    """Return all HTML template files."""
    return sorted(TEMPLATES_DIR.glob("*.html"))


@pytest.fixture(scope="session")
def python_files() -> list[Path]:
    """Return all .py files in the project root (non-recursive)."""
    return sorted(PROJECT_ROOT.glob("*.py"))


@pytest.fixture(scope="session")
def app_source() -> str:
    """Read and return app.py + routes/pages.py source code (cached for the session).

    Routes were decomposed from app.py into routes/pages.py, so tests that
    check for route strings need both files concatenated.
    """
    source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
    pages_path = PROJECT_ROOT / "routes" / "pages.py"
    if pages_path.exists():
        source += "\n" + pages_path.read_text(encoding="utf-8")
    return source


def pytest_configure(config) -> None:  # noqa: ANN001
    """Register custom markers used by individual test modules."""
    config.addinivalue_line(
        "markers",
        "live: live-network smoke tests; auto-skipped when API keys are absent",
    )
