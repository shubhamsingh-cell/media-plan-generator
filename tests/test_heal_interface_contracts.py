"""Interface contracts between self-healing code and the modules it heals.

sentry_integration._execute_healing_action and data_matrix_monitor's
self-heal strategies operate on OTHER modules at runtime via sys.modules.
When commit 68e211a refactored data_orchestrator to its class-based design,
the healing code kept poking module attrs that no longer existed
(_api_result_cache, _api_cache_lock, _load_lock, _IMPORT_FAILED), so those
heal branches raised AttributeError and silently recorded failure on every
run. These tests make that class of rot loud at test time instead:

- the public heal hook (data_orchestrator.clear_caches) must exist and work
- llm_router's circuit-breaker state must keep the attrs network_retry uses
- neither healing module may reference the dead private attrs again
- every fix_type must execute against the REAL imported modules (not mocks)
  without raising
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# ── Ensure project root is importable ────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import auto_qc
import data_orchestrator
import llm_router
from sentry_integration import _execute_healing_action

ALL_FIX_TYPES = [
    "isinstance_guard",
    "none_check",
    "or_empty_string",
    "dict_get_default",
    "bounds_check",
    "type_guard",
    "json_parse_guard",
    "network_retry",
    "resource_cleanup",
    "unknown_fix_type",
]

# Attrs data_orchestrator lost in the 68e211a refactor; healing code must
# never depend on them again -- it goes through clear_caches() instead.
DEAD_PRIVATE_ATTRS = [
    "_api_result_cache",
    "_api_cache_lock",
    "_load_lock",
    "_IMPORT_FAILED",
]


def _clear_heal_log() -> None:
    with auto_qc._lock:
        auto_qc._heal_log.clear()


def _swap_in_orchestrator():
    """Install a fresh DataOrchestrator singleton; return the prior one."""
    saved = data_orchestrator._orchestrator
    orch = data_orchestrator.DataOrchestrator()
    with data_orchestrator._orchestrator_lock:
        data_orchestrator._orchestrator = orch
    return saved, orch


def _restore_orchestrator(saved) -> None:
    with data_orchestrator._orchestrator_lock:
        data_orchestrator._orchestrator = saved


class TestPublicHealHook:
    """data_orchestrator.clear_caches() -- the one sanctioned heal surface."""

    def test_clear_caches_exists_and_is_callable(self) -> None:
        assert callable(getattr(data_orchestrator, "clear_caches", None))

    def test_clear_caches_without_singleton_is_honest_noop(self) -> None:
        saved = data_orchestrator._orchestrator
        try:
            with data_orchestrator._orchestrator_lock:
                data_orchestrator._orchestrator = None
            assert data_orchestrator.clear_caches() is False
        finally:
            _restore_orchestrator(saved)

    def test_clear_caches_clears_existing_singleton(self) -> None:
        saved, orch = _swap_in_orchestrator()
        try:
            orch._l1_cache.set("contract-test-key", {"v": 1})
            assert data_orchestrator.clear_caches() is True
            assert orch._l1_cache.get("contract-test-key") is None
        finally:
            _restore_orchestrator(saved)


class TestLlmRouterBreakerContract:
    """network_retry pokes these attrs on the real provider states."""

    def test_provider_states_shape(self) -> None:
        states = llm_router._provider_states
        assert isinstance(states, dict) and states
        for state in states.values():
            assert hasattr(state, "lock")
            assert hasattr(state, "consecutive_failures")
            assert hasattr(state, "circuit_open_until")


class TestNoDeadPrivateAttrReferences:
    """Static tripwire: the dead-attr pattern must not creep back in."""

    @pytest.mark.parametrize(
        "module_file", ["sentry_integration.py", "data_matrix_monitor.py"]
    )
    def test_dead_attrs_not_referenced(self, module_file: str) -> None:
        src = (PROJECT_ROOT / module_file).read_text(encoding="utf-8")
        for attr in DEAD_PRIVATE_ATTRS:
            assert attr not in src, (
                f"{module_file} references {attr}, which data_orchestrator "
                f"no longer defines -- use data_orchestrator.clear_caches()"
            )


class TestEveryFixTypeExecutesAgainstRealModules:
    """The whole healing dispatch must run against real modules, not mocks."""

    def setup_method(self) -> None:
        _clear_heal_log()

    @pytest.mark.parametrize("fix_type", ALL_FIX_TYPES)
    def test_no_exception_with_real_modules(self, fix_type: str) -> None:
        # file="" skips module-reload branches; data_orchestrator and
        # llm_router are genuinely imported, so cache/breaker branches
        # exercise the real interfaces.
        qc = auto_qc.get_auto_qc()
        result = _execute_healing_action(fix_type, {"file": ""}, qc)
        assert isinstance(result, bool)

    def test_json_parse_guard_succeeds_with_live_orchestrator(self) -> None:
        saved, _orch = _swap_in_orchestrator()
        try:
            qc = auto_qc.get_auto_qc()
            result = _execute_healing_action("json_parse_guard", {"file": "app.py"}, qc)
            assert result is True
            heals = qc.get_status().get("recent_heals") or []
            assert any(
                h["action"] == "clear_api_cache_json" and h["success"] for h in heals
            )
        finally:
            _restore_orchestrator(saved)

    def test_none_check_cache_clear_fallback_succeeds(self) -> None:
        saved, _orch = _swap_in_orchestrator()
        try:
            qc = auto_qc.get_auto_qc()
            # file="" skips the reload branch; the orchestrator-cache
            # fallback is the path under test
            result = _execute_healing_action("none_check", {"file": ""}, qc)
            assert result is True
            heals = qc.get_status().get("recent_heals") or []
            assert any(
                h["action"] == "clear_orchestrator_cache" and h["success"]
                for h in heals
            )
        finally:
            _restore_orchestrator(saved)

    def test_network_retry_succeeds_with_real_llm_router(self) -> None:
        qc = auto_qc.get_auto_qc()
        assert _execute_healing_action("network_retry", {"file": ""}, qc) is True
        heals = qc.get_status().get("recent_heals") or []
        assert any(
            h["action"] == "reset_circuit_breakers" and h["success"] for h in heals
        )
