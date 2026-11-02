"""TAVILY_API_KEY unset must be announced once at boot, never silently."""

from __future__ import annotations

import ast
import logging
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import tavily_search as ts  # noqa: E402


@pytest.fixture
def fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ts, "_freshness_status_logged", False)
    monkeypatch.setattr(ts, "_TAVILY_API_KEY", None)


def _lines(caplog: pytest.LogCaptureFixture) -> list:
    return [
        r for r in caplog.records
        if r.name == "tavily_search" and "freshness search is OFF" in r.getMessage()
    ]


def test_unset_key_logs_exactly_one_line(
    fresh: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("TAVILY_API_KEY", raising=False)
    with caplog.at_level(logging.INFO, logger="tavily_search"):
        assert ts.log_freshness_status_once() is False
        assert ts.log_freshness_status_once() is False  # idempotent
    lines = _lines(caplog)
    assert len(lines) == 1
    assert lines[0].levelno == logging.WARNING
    assert "TAVILY_API_KEY" in lines[0].getMessage()


def test_set_key_logs_nothing(
    fresh: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("TAVILY_API_KEY", "tvly-test")
    with caplog.at_level(logging.INFO, logger="tavily_search"):
        assert ts.log_freshness_status_once() is True
    assert _lines(caplog) == []


def test_wsgi_boot_calls_it() -> None:
    """A real call in _run_deferred_startup (AST: a commented-out call fails)."""
    tree = ast.parse((PROJECT_ROOT / "wsgi.py").read_text("utf-8"))
    fn = next(
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "_run_deferred_startup"
    )
    calls = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "log_freshness_status_once"
    ]
    assert len(calls) == 1
