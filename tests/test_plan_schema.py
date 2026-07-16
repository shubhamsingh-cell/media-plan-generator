"""Tests for the Layer-1 typed pipeline contract (plan_schema).

Covers the tolerant ``from_dict`` constructors, the round-trip ``to_dict``
fidelity, and ``validate_and_normalize`` (never raises; reports drift as
warnings). No network / no LLM / no Supabase -- this is a pure-Python module.

Runs under pytest, or standalone: ``python3 tests/test_plan_schema.py``.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import plan_schema  # noqa: E402
from plan_schema import (  # noqa: E402
    BudgetAllocation,
    ChannelAllocation,
    PlanData,
    validate_and_normalize,
)


def _full_channel():
    """A realistic per-channel dict as emitted by budget_engine."""
    return {
        "dollar_amount": 12500.0,
        "percentage": 25.0,
        "cpc": 0.85,
        "cpa": 18.5,
        "projected_clicks": 14705,
        "projected_applications": 675,
        "projected_hires": 12,
        "cost_per_hire": 1041.67,
        "roi_score": 7.2,
        "category": "aggregator",
        "confidence": "high",
        "source": "Joveo Campaign Warehouse (cg_benchmarks)",
        "vintage": "2026-05-12",
        # extra pipeline keys must survive a round-trip
        "cpc_source": "trend_engine",
        "apply_rate": 0.0459,
        "trend_direction": "rising",
    }


# --------------------------------------------------------------------------- #
# ChannelAllocation
# --------------------------------------------------------------------------- #
def test_channel_from_dict_full():
    ch = ChannelAllocation.from_dict(_full_channel(), name="Indeed")
    assert ch.name == "Indeed"
    assert ch.dollar_amount == 12500.0
    assert ch.percentage == 25.0
    assert ch.projected_hires == 12
    assert ch.roi_score == 7.2
    assert ch.confidence == "high"
    assert ch.source.startswith("Joveo Campaign Warehouse")
    assert ch.vintage == "2026-05-12"
    # unknown keys land in extra
    assert ch.extra["cpc_source"] == "trend_engine"
    assert ch.extra["trend_direction"] == "rising"


def test_channel_from_empty_uses_defaults():
    ch = ChannelAllocation.from_dict(None, name="LinkedIn")
    assert ch.name == "LinkedIn"
    assert ch.dollar_amount == 0.0
    assert ch.projected_hires == 0
    assert ch.confidence == "low"  # default
    assert ch.source == ""
    assert ch.extra == {}


def test_channel_coerces_dirty_string_numbers():
    ch = ChannelAllocation.from_dict(
        {"dollar_amount": "$12,500.50", "percentage": "25%", "projected_hires": "12"}
    )
    assert ch.dollar_amount == 12500.5
    assert ch.percentage == 25.0
    assert ch.projected_hires == 12


def test_channel_dollars_alias_and_last_updated_alias():
    # legacy "dollars" -> dollar_amount; "last_updated" -> vintage
    ch = ChannelAllocation.from_dict({"dollars": 999.0, "last_updated": "2026-04-01"})
    assert ch.dollar_amount == 999.0
    assert ch.vintage == "2026-04-01"


def test_channel_placeholder_and_bad_values_default_to_zero():
    ch = ChannelAllocation.from_dict(
        {"dollar_amount": "—", "cpc": "N/A", "roi_score": "not-a-number"}
    )
    assert ch.dollar_amount == 0.0
    assert ch.cpc == 0.0
    assert ch.roi_score == 0.0


def test_channel_bool_does_not_become_number():
    ch = ChannelAllocation.from_dict({"dollar_amount": True})
    assert ch.dollar_amount == 0.0


def test_channel_round_trip_is_nonlossy():
    src = _full_channel()
    ch = ChannelAllocation.from_dict(src, name="Indeed")
    out = ch.to_dict()
    # all original keys preserved with equal values
    for k, v in src.items():
        assert out[k] == v, f"key {k} drifted: {out.get(k)!r} != {v!r}"
    # name is the parent-map key, not an emitted field
    assert "name" not in out


# --------------------------------------------------------------------------- #
# BudgetAllocation
# --------------------------------------------------------------------------- #
def test_budget_allocation_from_dict():
    payload = {
        "channel_allocations": {
            "Indeed": _full_channel(),
            "LinkedIn": {"dollar_amount": 7500.0, "percentage": 15.0},
        },
        "total_projected": {"hires": 30, "cost_per_hire": 1200.0},
        "warnings": ["budget tight"],
        "metadata": {"industry": "healthcare"},
    }
    ba = BudgetAllocation.from_dict(payload)
    assert set(ba.channel_allocations) == {"Indeed", "LinkedIn"}
    assert isinstance(ba.channel_allocations["Indeed"], ChannelAllocation)
    assert ba.channel_allocations["Indeed"].name == "Indeed"
    assert ba.total_projected["hires"] == 30
    # non-structured top-level keys + the explicit metadata block both captured
    assert ba.metadata["warnings"] == ["budget tight"]
    assert ba.metadata["industry"] == "healthcare"


def test_budget_allocation_total_dollars_property():
    ba = BudgetAllocation.from_dict(
        {
            "channel_allocations": {
                "A": {"dollar_amount": 100.0},
                "B": {"dollar_amount": 250.5},
            }
        }
    )
    assert ba.total_dollars == 350.5


def test_budget_allocation_empty():
    ba = BudgetAllocation.from_dict(None)
    assert ba.channel_allocations == {}
    assert ba.total_projected == {}
    assert ba.total_dollars == 0.0


def test_budget_allocation_round_trip():
    payload = {
        "channel_allocations": {"Indeed": _full_channel()},
        "total_projected": {"hires": 12},
        "warnings": ["x"],
    }
    out = BudgetAllocation.from_dict(payload).to_dict()
    assert out["channel_allocations"]["Indeed"]["dollar_amount"] == 12500.0
    assert out["total_projected"]["hires"] == 12
    assert out["warnings"] == ["x"]


# --------------------------------------------------------------------------- #
# PlanData
# --------------------------------------------------------------------------- #
def test_plan_data_from_dict_full():
    payload = {
        "client_name": "Acme Health",
        "industry": "healthcare",
        "budget": 50000,
        "roles": ["Registered Nurse", "Physician"],
        "locations": ["Dallas, TX"],
        "_budget_allocation": {"channel_allocations": {"Indeed": _full_channel()}},
        "_enriched": {"salary": {"value": 80000}},
        "_validation": {"checks_run": 5, "findings": []},
        "company_name": "Acme",
        "_synthesized": {"foo": "bar"},
    }
    plan = PlanData.from_dict(payload)
    assert plan.client_name == "Acme Health"
    assert plan.industry == "healthcare"
    assert plan.budget == 50000
    assert plan.roles == ["Registered Nurse", "Physician"]
    assert plan.locations == ["Dallas, TX"]
    assert isinstance(plan.budget_allocation, BudgetAllocation)
    assert plan.budget_allocation.channel_allocations["Indeed"].projected_hires == 12
    assert plan.enriched["salary"]["value"] == 80000
    assert plan.validation["checks_run"] == 5
    # unmodeled keys preserved in extra
    assert plan.extra["company_name"] == "Acme"
    assert plan.extra["_synthesized"] == {"foo": "bar"}


def test_plan_data_defaults_and_company_name_fallback():
    plan = PlanData.from_dict({"company_name": "FallbackCo"})
    assert plan.client_name == "FallbackCo"  # falls back to company_name
    assert plan.industry == "general_entry_level"
    assert plan.budget == "Not specified"
    assert plan.roles == []
    assert plan.locations == []


def test_plan_data_polymorphic_roles_locations():
    # roles as bare string; locations as list of dicts
    plan = PlanData.from_dict(
        {
            "roles": "Driver",
            "locations": [{"city": "Austin", "state": "TX"}, "Remote"],
        }
    )
    assert plan.roles == ["Driver"]
    assert plan.locations == ["Austin", "Remote"]


def test_plan_data_target_roles_alias():
    plan = PlanData.from_dict({"target_roles": ["RN"]})
    assert plan.roles == ["RN"]


def test_plan_data_round_trip_keys():
    payload = {
        "client_name": "Acme",
        "industry": "healthcare",
        "budget": 50000,
        "roles": ["RN"],
        "locations": ["Dallas"],
        "_budget_allocation": {"channel_allocations": {"Indeed": _full_channel()}},
        "_enriched": {"a": 1},
        "_validation": {"b": 2},
        "company_name": "Acme Corp",
    }
    out = PlanData.from_dict(payload).to_dict()
    assert out["client_name"] == "Acme"
    assert out["_enriched"] == {"a": 1}
    assert out["_validation"] == {"b": 2}
    assert out["company_name"] == "Acme Corp"  # extra preserved
    assert "_budget_allocation" in out
    assert (
        out["_budget_allocation"]["channel_allocations"]["Indeed"]["dollar_amount"]
        == 12500.0
    )


# --------------------------------------------------------------------------- #
# validate_and_normalize -- never raises, reports drift
# --------------------------------------------------------------------------- #
def test_validate_clean_plan_no_warnings():
    payload = {
        "client_name": "Acme",
        "industry": "healthcare",
        "budget": 50000,
        "roles": ["RN"],
        "locations": ["Dallas"],
        "_budget_allocation": {
            "channel_allocations": {
                "Indeed": {**_full_channel(), "percentage": 60.0},
                "LinkedIn": {
                    "percentage": 40.0,
                    "source": "benchmark_registry",
                    "confidence": "medium",
                },
            },
            "total_projected": {"hires": 30},
        },
    }
    normalized, warnings = validate_and_normalize(payload)
    assert isinstance(normalized, dict)
    assert warnings == [], f"unexpected warnings: {warnings}"


def test_validate_returns_tuple_and_dict():
    out = validate_and_normalize({})
    assert isinstance(out, tuple) and len(out) == 2
    normalized, warnings = out
    assert isinstance(normalized, dict)
    assert isinstance(warnings, list)


def test_validate_warns_on_missing_core_fields():
    _, warnings = validate_and_normalize({})
    joined = " | ".join(warnings)
    assert "client_name" in joined
    assert "industry" in joined
    assert "budget" in joined
    assert "roles" in joined
    assert "locations" in joined


def test_validate_warns_on_percentage_drift():
    payload = {
        "client_name": "Acme",
        "industry": "healthcare",
        "budget": 1,
        "roles": ["RN"],
        "locations": ["Dallas"],
        "_budget_allocation": {
            "channel_allocations": {
                "Indeed": {
                    "percentage": 40.0,
                    "source": "x",
                    "confidence": "high",
                }
            },
            "total_projected": {"hires": 1},
        },
    }
    _, warnings = validate_and_normalize(payload)
    assert any("percentages sum to" in w for w in warnings)


def test_validate_warns_on_missing_provenance_and_bad_confidence():
    payload = {
        "client_name": "Acme",
        "industry": "healthcare",
        "budget": 1,
        "roles": ["RN"],
        "locations": ["Dallas"],
        "_budget_allocation": {
            "channel_allocations": {
                "Indeed": {"percentage": 100.0, "confidence": "wildly-unsure"}
            },
            "total_projected": {"hires": 1},
        },
    }
    _, warnings = validate_and_normalize(payload)
    assert any("missing provenance source" in w for w in warnings)
    assert any("unrecognized confidence" in w for w in warnings)


def test_validate_warns_on_empty_allocations():
    payload = {
        "client_name": "Acme",
        "industry": "healthcare",
        "budget": 1,
        "roles": ["RN"],
        "locations": ["Dallas"],
        "_budget_allocation": {},
    }
    _, warnings = validate_and_normalize(payload)
    assert any("no channel_allocations" in w for w in warnings)


def test_validate_never_raises_on_non_dict():
    for bad in (None, 42, "a string", ["a", "list"]):
        normalized, warnings = validate_and_normalize(bad)
        assert isinstance(normalized, dict)
        assert any("expected dict" in w for w in warnings)


def test_validate_never_raises_on_garbage_nested_shapes():
    # channel_allocations is a list, total_projected is a string -- must not crash
    payload = {
        "client_name": "Acme",
        "industry": "x",
        "budget": 1,
        "roles": ["r"],
        "locations": ["l"],
        "_budget_allocation": {
            "channel_allocations": ["not", "a", "dict"],
            "total_projected": "nope",
        },
    }
    normalized, warnings = validate_and_normalize(payload)
    assert isinstance(normalized, dict)
    # garbage channel_allocations coerces to empty -> warns, never raises
    assert any("no channel_allocations" in w for w in warnings)


def test_validate_output_is_round_trippable():
    payload = {
        "client_name": "Acme",
        "industry": "healthcare",
        "budget": 50000,
        "roles": ["RN"],
        "locations": ["Dallas"],
        "_budget_allocation": {"channel_allocations": {"Indeed": _full_channel()}},
        "extra_key": "kept",
    }
    normalized, _ = validate_and_normalize(payload)
    # feeding normalized output back in must be stable
    again, _ = validate_and_normalize(normalized)
    assert again["client_name"] == "Acme"
    assert again["extra_key"] == "kept"
    assert (
        again["_budget_allocation"]["channel_allocations"]["Indeed"]["dollar_amount"]
        == 12500.0
    )


def test_module_exports():
    for name in (
        "ChannelAllocation",
        "BudgetAllocation",
        "PlanData",
        "validate_and_normalize",
    ):
        assert hasattr(plan_schema, name)
        assert name in plan_schema.__all__


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
