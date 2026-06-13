"""Tests for Nova's query_joveo_real_benchmarks keystone tool (S89).

Verifies the tool handler that wraps supabase_data.get_real_outcomes():
- matched  -> returns the measured figures from the warehouse as-is
- no-match -> returns a clear "no measured data; using estimates" message
- empty role -> graceful no-match without hitting the accessor
- the tool is registered (definition + handler map + dispatch)

The accessor (supabase_data.get_real_outcomes) is mocked, so these run
offline -- no network, no Supabase. We use ``Nova.__new__(Nova)`` to get a
real instance WITHOUT running the heavy ``__init__`` (which loads the full
KB); the bound methods we exercise are class attributes, so they resolve
fine, and the handler under test never touches instance state.

Runs under pytest, or standalone: ``python3 tests/test_nova_keystone.py``.
"""

import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import nova  # noqa: E402
import supabase_data  # noqa: E402

TOOL = "query_joveo_real_benchmarks"


def _nova():
    """A real Nova instance without the heavy __init__ (KB load)."""
    return nova.Nova.__new__(nova.Nova)


def _call(params):
    """Invoke the handler in isolation on a no-init Nova instance."""
    return _nova()._query_joveo_real_benchmarks(params)


# ── matched path ───────────────────────────────────────────────────────────
def test_matched_returns_measured_figures():
    measured = {
        "matched": True,
        "title_query": "Registered Nurse",
        "location_query": "Dallas",
        "avg_cost": 4.06,
        "avg_applies": 7.94,
        "cost_per_apply": 0.51,
        "sample_size": 303,
        "locations_covered": 2,
        "last_updated": "2026-05-12",
        "source": "Joveo Campaign Warehouse (cg_benchmarks)",
        "confidence": "measured",
    }
    with mock.patch.object(
        supabase_data, "get_real_outcomes", return_value=measured
    ) as m:
        out = _call({"role": "Registered Nurse", "location": "Dallas"})

    # Accessor called with the exact role/location.
    m.assert_called_once_with("Registered Nurse", "Dallas")
    # Measured figures are surfaced as-is.
    assert out["matched"] is True
    assert out["cost_per_apply"] == 0.51
    assert out["avg_cost"] == 4.06
    assert out["sample_size"] == 303
    assert out["confidence"] == "measured"
    assert "cg_benchmarks" in out["source"]


def test_matched_without_location():
    measured = {"matched": True, "cost_per_apply": 1.23, "source": "cg_benchmarks"}
    with mock.patch.object(
        supabase_data, "get_real_outcomes", return_value=measured
    ) as m:
        out = _call({"role": "CDL Driver"})
    m.assert_called_once_with("CDL Driver", "")
    assert out["matched"] is True
    assert out["cost_per_apply"] == 1.23


# ── no-match (fallback) path ───────────────────────────────────────────────
def test_no_match_returns_fallback_message():
    with mock.patch.object(
        supabase_data, "get_real_outcomes", return_value={"matched": False}
    ) as m:
        out = _call({"role": "Underwater Basket Weaver", "location": "Mars"})

    m.assert_called_once_with("Underwater Basket Weaver", "Mars")
    assert out["matched"] is False
    assert out["role"] == "Underwater Basket Weaver"
    assert out["location"] == "Mars"
    # Clear "no measured data; using estimates" guidance.
    msg = out["message"].lower()
    assert "no measured data" in msg
    assert "estimates" in msg
    assert "underwater basket weaver" in msg
    assert "mars" in msg


def test_no_match_without_location_omits_where_clause():
    with mock.patch.object(
        supabase_data, "get_real_outcomes", return_value={"matched": False}
    ):
        out = _call({"role": "Sandwich Artist"})
    assert out["matched"] is False
    assert out["location"] is None
    assert "no measured data" in out["message"].lower()


def test_empty_role_is_graceful_no_match_without_accessor_call():
    with mock.patch.object(supabase_data, "get_real_outcomes") as m:
        out = _call({"role": "   "})
    # Empty role short-circuits -- accessor is never called.
    m.assert_not_called()
    assert out["matched"] is False
    assert "specify a job title" in out["message"].lower()


def test_missing_role_key_is_graceful_no_match():
    with mock.patch.object(supabase_data, "get_real_outcomes") as m:
        out = _call({})
    m.assert_not_called()
    assert out["matched"] is False


# ── registration / dispatch wiring ─────────────────────────────────────────
def test_tool_is_registered_in_handler_map():
    handlers = _nova()._tool_handler_map()
    assert TOOL in handlers
    assert handlers[TOOL].__name__ == "_query_joveo_real_benchmarks"


def test_tool_definition_present_and_well_formed():
    defs = _nova().get_tool_definitions()
    by_name = {d["name"]: d for d in defs}
    assert TOOL in by_name
    spec = by_name[TOOL]
    assert spec["input_schema"]["required"] == ["role"]
    props = spec["input_schema"]["properties"]
    assert "role" in props and "location" in props
    # Description signals this is REAL/measured first-party data.
    assert "cg_benchmarks" in spec["description"]


def test_tool_in_essential_set_for_free_llms():
    assert TOOL in nova.TOOLS_ESSENTIAL


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
    sys.exit(1 if _failures else 0)
