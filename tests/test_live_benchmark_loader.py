"""Regression tests for budget_engine._load_channel_benchmarks_live's
no-freeze semantics + the module's data_seeds self-seed wiring (2026-07-16).

Background: _load_channel_benchmarks_live used to cache its FIRST read at
module level unconditionally -- including when data/channel_benchmarks_live.json
was absent, corrupt, or empty. The local dev daemon writes that file
non-atomically ~30 minutes after boot, so a process that read too early (or
mid-write) would freeze an empty/fallback-tier result for its entire
lifetime, silently serving fallback-tier CPCs forever. The loader now only
caches a successful, non-empty parse; absent/corrupt/empty reads return {}
without touching the cache, so a later call (once the file is present and
valid) can still succeed.

Separately, budget_engine.py now self-seeds the gitignored runtime data
files at module scope (mirroring what app.py's import already does via
data_seeds.seed_runtime_data_files() -- see tests/test_data_seeds_wiring.py)
so that scripts importing budget_engine directly (tools_regen_bundles,
eval_framework, scripts/render_sample_*) don't bypass seeding and drift off
the owner-approved calibration in a fresh checkout.

Test groups:
    1. Absent file -- returns {} and does NOT cache; a later valid write is
       picked up on the next call.
    2. Corrupt file -- returns {} and does NOT cache; a later valid write is
       picked up on the next call.
    3. Valid file -- loads, caches, and repeat calls return the identical
       cached object (no re-read).
    4. Source guard -- budget_engine.py imports+calls
       data_seeds.seed_runtime_data_files() at module scope, BEFORE
       _load_channel_benchmarks_live is defined.

Runs under pytest, or standalone:
``python3 tests/test_live_benchmark_loader.py``.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine as be  # noqa: E402

BUDGET_ENGINE_PATH = PROJECT_ROOT / "budget_engine.py"

# The frozen, owner-approved snapshot also used by
# tests/test_funnel_calibration.py::TestHeadlineInvariance and
# tests/test_channel_bench_seed.py -- indeed cpc_range {0.97, 2.71},
# linkedin cpc_range {1.50, 4.50}; ziprecruiter/glassdoor/monster/
# careerbuilder carry no cpc_range by design (see the fixture file's
# per-entry "notes").
_FUNNEL_INVARIANT_FIXTURE_DIR = (
    Path(__file__).resolve().parent / "fixtures" / "funnel_invariant"
)


@pytest.fixture(autouse=True)
def _reset_live_bench_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts with a clean module-level cache -- otherwise test
    order (or an earlier import elsewhere in the suite) could leak a cached
    result into a test that expects a fresh read."""
    monkeypatch.setattr(be, "_channel_bench_live_cache", None)


# ---------------------------------------------------------------------------
# 1. Absent file
# ---------------------------------------------------------------------------
def test_absent_file_returns_empty_and_does_not_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(be, "_DATA_DIR", tmp_path)

    result = be._load_channel_benchmarks_live()

    assert result == {}
    assert be._channel_bench_live_cache is None

    # The file shows up later (e.g. the dev daemon finishes its ~30min-post-boot
    # write) -- the NEXT call must succeed, proving nothing was frozen above.
    payload = {
        "data": [
            {
                "channel": "indeed",
                "pricing_model": "CPC",
                "metadata": {
                    "board_name": "Indeed",
                    "cpc_range": {"min": 1.0, "max": 4.0},
                },
            }
        ]
    }
    (tmp_path / "channel_benchmarks_live.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )

    result2 = be._load_channel_benchmarks_live()

    assert "indeed" in result2
    assert result2["indeed"]["cpc_typical"] == pytest.approx(2.0)  # geomean(1.0, 4.0)
    assert be._channel_bench_live_cache is result2


# ---------------------------------------------------------------------------
# 2. Corrupt file
# ---------------------------------------------------------------------------
def test_corrupt_file_returns_empty_and_does_not_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(be, "_DATA_DIR", tmp_path)
    live_path = tmp_path / "channel_benchmarks_live.json"
    # Truncated mid-write JSON -- the shape a non-atomic daemon rewrite would
    # leave on disk if a reader hits it mid-flight.
    live_path.write_text('{"data": [{"chan', encoding="utf-8")

    result = be._load_channel_benchmarks_live()

    assert result == {}
    assert be._channel_bench_live_cache is None

    # The daemon finishes its rewrite -- the NEXT call must succeed.
    valid_payload = {
        "data": [
            {
                "channel": "linkedin",
                "pricing_model": "CPC",
                "metadata": {
                    "board_name": "LinkedIn",
                    "cpc_range": {"min": 1.5, "max": 4.5},
                },
            }
        ]
    }
    live_path.write_text(json.dumps(valid_payload), encoding="utf-8")

    result2 = be._load_channel_benchmarks_live()

    assert "linkedin" in result2
    assert result2["linkedin"]["cpc_typical"] == pytest.approx(2.6)  # geomean(1.5, 4.5)
    assert be._channel_bench_live_cache is result2


# ---------------------------------------------------------------------------
# 3. Valid file -- loads AND caches
# ---------------------------------------------------------------------------
def test_valid_fixture_loads_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "_DATA_DIR", _FUNNEL_INVARIANT_FIXTURE_DIR)

    result = be._load_channel_benchmarks_live()

    assert set(result.keys()) == {
        "indeed",
        "linkedin",
        "ziprecruiter",
        "glassdoor",
        "monster",
        "careerbuilder",
    }
    # geomean(0.97, 2.71) and geomean(1.50, 4.50) -- see fixture provenance.
    assert result["indeed"]["cpc_typical"] == pytest.approx(1.62)
    assert result["linkedin"]["cpc_typical"] == pytest.approx(2.6)
    # These four channels carry no cpc_range in the 2026-07-16 refresh (their
    # 2026 pricing models don't support a citable CPC figure) -- confirm the
    # loader leaves cpc_typical unset rather than fabricating one.
    assert result["ziprecruiter"]["cpc_typical"] is None
    assert result["glassdoor"]["cpc_typical"] is None
    assert result["monster"]["cpc_typical"] is None
    assert result["careerbuilder"]["cpc_typical"] is None

    assert be._channel_bench_live_cache is result

    # Repeat call must return the SAME object -- no re-read/re-parse.
    result2 = be._load_channel_benchmarks_live()
    assert result2 is result


# ---------------------------------------------------------------------------
# 4. Source guard -- self-seed wiring
# ---------------------------------------------------------------------------
# Mirrors the source-inspection pattern in tests/test_data_seeds_wiring.py:
# parse budget_engine.py's source directly rather than trying to observe
# import-time side effects, since the self-seed call only matters the FIRST
# time the module is imported in a process (pytest has already imported
# budget_engine by the time any test runs).
#
# Uses ``ast`` rather than regex/string matching, and walks ONLY
# ``tree.body`` (module top-level statements) -- never descending into any
# FunctionDef/ClassDef body. A regex or "does this substring appear
# somewhere" check can be satisfied by wrapping the import+call inside a
# helper function that is never invoked (e.g. ``def _maybe_seed(): ...``),
# which would fully defeat the point: the self-seed MUST run
# unconditionally at import time, not merely exist as dead code somewhere
# in the file. Restricting the walk to ``tree.body`` makes that
# unreachable-code shape structurally unable to satisfy the test.
def _self_seed_statement_lineno(tree: ast.Module) -> Optional[int]:
    """Return the line number of the top-level statement that both imports
    ``seed_runtime_data_files`` from ``data_seeds`` and calls it, or
    ``None`` if no such statement exists at module scope.

    Two shapes are accepted, both restricted to ``tree.body`` (module
    top-level) or the immediate body of a top-level ``Try`` -- never a
    nested FunctionDef/ClassDef body:
      1. A ``try: from data_seeds import seed_runtime_data_files as X; X()
         except ImportError: pass`` block (the shipped shape) -- returns
         the ``Try`` node's own lineno.
      2. A bare top-level ``from data_seeds import seed_runtime_data_files``
         (optionally aliased) plus a bare top-level call to that name
         anywhere else in ``tree.body`` -- returns the import statement's
         lineno.
    """

    def _seeded_name_from_import(node: ast.AST) -> Optional[str]:
        if not isinstance(node, ast.ImportFrom) or node.module != "data_seeds":
            return None
        for alias in node.names:
            if alias.name == "seed_runtime_data_files":
                return alias.asname or alias.name
        return None

    def _is_call_to(node: ast.AST, name: str) -> bool:
        return (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == name
        )

    # Shape 1: a top-level try/except wrapping both the import and the call.
    for stmt in tree.body:
        if not isinstance(stmt, ast.Try):
            continue
        seeded_name = None
        for node in stmt.body:
            seeded_name = _seeded_name_from_import(node) or seeded_name
        if seeded_name and any(_is_call_to(node, seeded_name) for node in stmt.body):
            return stmt.lineno

    # Shape 2: a bare top-level import + a bare top-level call (no try/except).
    seeded_name = None
    import_lineno = None
    for stmt in tree.body:
        name = _seeded_name_from_import(stmt)
        if name:
            seeded_name, import_lineno = name, stmt.lineno
    if seeded_name and any(_is_call_to(node, seeded_name) for node in tree.body):
        return import_lineno

    return None


def _top_level_funcdef_lineno(tree: ast.Module, func_name: str) -> Optional[int]:
    for stmt in tree.body:
        if isinstance(stmt, ast.FunctionDef) and stmt.name == func_name:
            return stmt.lineno
    return None


def test_module_scope_self_seed_import_and_call_precede_loader_def() -> None:
    src = BUDGET_ENGINE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(BUDGET_ENGINE_PATH))

    seed_lineno = _self_seed_statement_lineno(tree)
    loader_lineno = _top_level_funcdef_lineno(tree, "_load_channel_benchmarks_live")

    assert seed_lineno is not None, (
        "budget_engine.py no longer has a module-top-level statement that "
        "both imports seed_runtime_data_files from data_seeds AND calls "
        "it unconditionally -- scripts that import budget_engine directly "
        "(tools_regen_bundles, eval_framework, scripts/render_sample_*) "
        "would silently bypass seeding again in a fresh checkout. Note: a "
        "call wrapped inside a helper function that is never itself "
        "invoked at module scope does NOT satisfy this -- the self-seed "
        "must run at import time, not merely exist as dead code."
    )
    assert loader_lineno is not None, (
        "budget_engine.py no longer defines _load_channel_benchmarks_live "
        "as a top-level function"
    )

    assert seed_lineno < loader_lineno, (
        f"the data_seeds self-seed (top-level statement at line "
        f"{seed_lineno}) must run BEFORE _load_channel_benchmarks_live is "
        f"defined (line {loader_lineno}) -- _load_channel_benchmarks_live "
        "caches its file read, so a caller that reaches it before seeding "
        "runs would bake in an empty read for the process's entire "
        "lifetime (the exact class of bug this module's self-seed exists "
        "to prevent)"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
