"""Tests for the cg_benchmarks warehouse accessor (S89 keystone).

Verifies the sample-size-weighted aggregation and the graceful no-match
fallback in supabase_data.get_real_outcomes(), without hitting the network
(the Supabase query layer is mocked).

Runs under pytest, or standalone: ``python3 tests/test_cg_warehouse.py``.
"""

import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import supabase_data  # noqa: E402


def _patched(rows):
    """Patch the Supabase query + cache so the accessor runs offline."""
    return mock.patch.multiple(
        supabase_data,
        _query_supabase=mock.Mock(return_value=rows),
        _cache_get=mock.Mock(return_value=None),
        _cache_set=mock.Mock(),
    )


def test_empty_title_is_no_match():
    assert supabase_data.get_real_outcomes("") == {"matched": False}
    assert supabase_data.get_real_outcomes("   ") == {"matched": False}


def test_no_rows_is_graceful_no_match():
    with _patched([]):
        out = supabase_data.get_real_outcomes("Nonexistent Role")
    assert out == {"matched": False}


def test_weighted_aggregation_and_cost_per_apply():
    rows = [
        # high sample dominates
        {
            "title": "RN - ICU",
            "location": "Dallas, TX",
            "avg_cost": 4.0,
            "avg_applies": 8.0,
            "avg_multiplier": 1.2,
            "sample_size": 300,
            "last_updated": "2026-05-12T00:00:00Z",
        },
        {
            "title": "RN - ER",
            "location": "Houston, TX",
            "avg_cost": 10.0,
            "avg_applies": 2.0,
            "avg_multiplier": 1.0,
            "sample_size": 3,
            "last_updated": "2026-05-01T00:00:00Z",
        },
    ]
    with _patched(rows):
        out = supabase_data.get_real_outcomes("Registered Nurse", "TX")

    assert out["matched"] is True
    # weighted avg_cost ≈ (4*300 + 10*3)/303 = 4.06 (dominated by the 300-run row)
    assert 4.0 <= out["avg_cost"] <= 4.2
    assert out["avg_applies"] > 7.0  # high-sample row dominates
    assert out["cost_per_apply"] == round(out["avg_cost"] / out["avg_applies"], 2)
    assert out["sample_size"] == 303
    assert out["locations_covered"] == 2
    assert out["last_updated"] == "2026-05-12"
    assert out["confidence"] == "measured"
    assert "cg_benchmarks" in out["source"]


def test_rows_with_no_usable_metrics_fall_back():
    rows = [
        {
            "title": "X",
            "location": "",
            "avg_cost": 0,
            "avg_applies": 0,
            "avg_multiplier": 0,
            "sample_size": 0,
            "last_updated": "",
        },
    ]
    with _patched(rows):
        out = supabase_data.get_real_outcomes("X")
    assert out == {"matched": False}


def test_zero_applies_yields_none_cost_per_apply():
    rows = [
        {
            "title": "Driver",
            "location": "Austin",
            "avg_cost": 3.5,
            "avg_applies": 0.0,
            "avg_multiplier": 1.0,
            "sample_size": 50,
            "last_updated": "2026-04-01T00:00:00Z",
        },
    ]
    with _patched(rows):
        out = supabase_data.get_real_outcomes("Driver")
    assert out["matched"] is True
    assert out["cost_per_apply"] is None
    assert out["avg_cost"] == 3.5


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
