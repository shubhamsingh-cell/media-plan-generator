"""The last three ``TimeoutError`` sites that missed concurrent.futures'
TimeoutError on Python < 3.11 (2026-10-16; same class as 932bb65).

``Future.result(timeout=)`` and ``as_completed(timeout=)`` raise
``concurrent.futures.TimeoutError``, which is a separate class from the
builtin until 3.11 (this repo's dev/CI interpreter is 3.9; prod pins 3.11+).
So on 3.9:

* data_orchestrator.DataOrchestrator.fetch: one slow source's timeout
  escaped fetch() entirely (it is not an OSError either);
* llm_router._parallel_consensus: a slow panel fell to the generic handler
  ("aggregation error") and skipped the collect-finished-replies pass;
* app.py /api/chat: ``isinstance(err, TimeoutError)`` read every chat
  timeout as a failure and skipped the partial-results reply.
"""

from __future__ import annotations

import ast
import concurrent.futures
import logging
import sys
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import data_orchestrator  # noqa: E402
import llm_router  # noqa: E402


def test_orchestrator_slow_source_times_out_instead_of_raising():
    orch = data_orchestrator.DataOrchestrator()
    orch._redis_available = False
    source = data_orchestrator.DataSourceType.ADZUNA

    def _slow(query, context):
        time.sleep(0.6)
        return {"jobs": 1}

    orch.register_source(source, _slow)
    result = orch.fetch(
        data_orchestrator.PlatformModule.COMMAND_CENTER,
        "nurse jobs",
        timeout_seconds=0.1,
        sources_override=[source],
    )
    assert result.sources_queried == 1 and result.sources_succeeded == 0
    assert [r.error for r in result.fetch_results] == ["Timeout (0.1s)"]


def test_consensus_timeout_is_handled_as_a_timeout(monkeypatch, caplog):
    def _fake_call_llm(*args, force_provider=None, **kwargs):
        if force_provider == "slow":
            time.sleep(0.6)
        return {"text": f"answer from {force_provider}"}

    monkeypatch.setattr(llm_router, "call_llm", _fake_call_llm)
    with caplog.at_level(logging.WARNING, logger=llm_router.logger.name):
        out = llm_router._parallel_consensus(
            messages=[{"role": "user", "content": "hi"}],
            providers=["fast", "slow"],
            timeout=0.2,
        )
    assert not [
        r for r in caplog.records if "aggregation error" in r.getMessage()
    ], "as_completed timeout fell through to the generic handler"
    assert "fast" in (out.get("providers_used") or []), out
    assert "slow" not in (out.get("providers_used") or []), out


_FILES = ("app.py", "data_orchestrator.py", "llm_router.py")


def _waits_on_futures(nodes) -> bool:
    """True when the statements call ``<x>.result(timeout=...)`` or
    ``as_completed(..., timeout=...)`` -- the calls that raise the futures
    TimeoutError. (A socket/urlopen timeout IS the builtin; those handlers
    are right as they are.)"""
    for stmt in nodes:
        for sub in ast.walk(stmt):
            if not isinstance(sub, ast.Call):
                continue
            name = getattr(sub.func, "attr", None) or getattr(sub.func, "id", "")
            if name in {"result", "as_completed"} and any(
                k.arg == "timeout" for k in sub.keywords
            ):
                return True
    return False


def _builtin_only_timeout_checks(path: Path) -> list:
    """In a ``try`` that waits on futures: ``except TimeoutError`` /
    ``except (TimeoutError, ...)`` and ``isinstance(x, TimeoutError)`` whose
    type list names the builtin but not the futures alias -- the shape that
    misses Future timeouts on 3.9."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits = []

    def names(node) -> set:
        elts = node.elts if isinstance(node, ast.Tuple) else [node]
        return {e.id for e in elts if isinstance(e, ast.Name)}

    def builtin_only(type_node) -> bool:
        got = names(type_node)
        return "TimeoutError" in got and "_FutureTimeoutError" not in got

    for node in ast.walk(tree):
        if not isinstance(node, ast.Try) or not _waits_on_futures(node.body):
            continue
        for handler in node.handlers:
            if handler.type is not None and builtin_only(handler.type):
                hits.append(handler.lineno)
            for sub in ast.walk(handler):
                if (
                    isinstance(sub, ast.Call)
                    and getattr(sub.func, "id", "") == "isinstance"
                    and len(sub.args) == 2
                    and builtin_only(sub.args[1])
                ):
                    hits.append(sub.lineno)
    return sorted(set(hits))


@pytest.mark.parametrize("filename", _FILES)
def test_no_builtin_only_timeout_checks_left(filename):
    hits = _builtin_only_timeout_checks(PROJECT_ROOT / filename)
    assert not hits, f"{filename}: builtin-only TimeoutError check at lines {hits}"


def test_the_alias_is_the_futures_class():
    assert data_orchestrator._FutureTimeoutError is concurrent.futures.TimeoutError
