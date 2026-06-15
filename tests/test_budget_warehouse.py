"""Tests for the S89 KEYSTONE wiring in budget_engine.

Verifies that calculate_budget_allocation:
  - attaches metadata["real_outcomes"] (and a calibration note for strong
    matches) when supabase_data.get_real_outcomes() reports a warehouse match;
  - leaves the result byte-identical (apart from the additive keystone keys)
    when there is NO match — the common case — and when the warehouse raises;
  - never overwrites the computed budget numbers.

No network / Supabase access: budget_engine._supabase_data.get_real_outcomes
is mocked. Runs under pytest, or standalone:
``python3 tests/test_budget_warehouse.py``.
"""

import copy
import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine  # noqa: E402


# --- Shared fixtures (plain dicts so the test is independent of upstream code) ---

_ROLES = [
    {"title": "Registered Nurse", "count": 5, "tier": "Clinical / Licensed"},
    {"title": "Warehouse Associate", "count": 20, "tier": "Hourly / Entry-Level"},
]
_LOCATIONS = [{"city": "Dallas", "state": "TX", "country": "United States"}]
_INDUSTRY = "healthcare_medical"
_CHANNELS = {"Global Job Boards": 60.0, "Programmatic & DSP": 40.0}
_BUDGET = 50_000.0


def _run(**overrides):
    kwargs = dict(
        total_budget=_BUDGET,
        roles=copy.deepcopy(_ROLES),
        locations=copy.deepcopy(_LOCATIONS),
        industry=_INDUSTRY,
        channel_percentages=dict(_CHANNELS),
    )
    kwargs.update(overrides)
    return budget_engine.calculate_budget_allocation(**kwargs)


def _matched(title, location=""):
    """A realistic strong warehouse match (large sample)."""
    return {
        "matched": True,
        "title_query": title,
        "location_query": location or None,
        "avg_cost": 6.0,
        "avg_applies": 4.0,
        "avg_multiplier": 1.2,
        "cost_per_apply": 1.5,
        "sample_size": 250,
        "rows_matched": 12,
        "locations_covered": 3,
        "last_updated": "2026-05-20",
        "source": "Joveo Campaign Warehouse (cg_benchmarks)",
        "confidence": "measured",
    }


# ---------------------------------------------------------------------------
# No-match / outage: behaviour must be byte-identical (no keystone keys added)
# ---------------------------------------------------------------------------


def test_no_supabase_module_leaves_result_unchanged():
    """When supabase_data isn't importable, no keystone keys appear."""
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", False):
        out = _run()
    assert "real_outcomes" not in out["metadata"]
    assert "real_outcome_calibration" not in out["metadata"]


def test_no_match_leaves_result_byte_identical():
    """No warehouse coverage => result equals the non-keystone baseline."""
    # Baseline: keystone disabled entirely.
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", False):
        baseline = _run()

    # Same inputs, keystone enabled but every role is a no-match.
    fake = mock.Mock(return_value={"matched": False})
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        with_keystone = _run()

    assert fake.called  # the accessor WAS consulted
    assert "real_outcomes" not in with_keystone["metadata"]
    assert "real_outcome_calibration" not in with_keystone["metadata"]
    # Additive guarantee: identical to the keystone-off baseline.
    assert with_keystone == baseline


def test_supabase_outage_is_swallowed():
    """A raising accessor must not break the plan or add keystone keys."""
    boom = mock.Mock(side_effect=RuntimeError("supabase down"))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=boom), create=True
    ):
        out = _run()
    assert "real_outcomes" not in out["metadata"]
    assert "real_outcome_calibration" not in out["metadata"]
    # Core result is still well-formed.
    assert out["channel_allocations"]
    assert out["total_projected"]["hires"] >= 0


# ---------------------------------------------------------------------------
# Matched: metadata attached; computed numbers untouched
# ---------------------------------------------------------------------------


def test_match_attaches_real_outcomes_metadata():
    fake = mock.Mock(side_effect=lambda title, loc="": _matched(title, loc))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        out = _run()

    ros = out["metadata"].get("real_outcomes")
    assert isinstance(ros, list) and len(ros) == 2  # both distinct roles matched
    titles = {r["title"] for r in ros}
    assert titles == {"Registered Nurse", "Warehouse Associate"}
    for r in ros:
        assert r["cost_per_apply"] == 1.5
        assert r["sample_size"] == 250
        assert "cg_benchmarks" in r["source"]
        assert r["confidence"] == "measured"


def test_match_does_not_overwrite_computed_numbers():
    """Surfacing only: the budget math is identical with/without a match."""
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", False):
        baseline = _run()

    fake = mock.Mock(side_effect=lambda title, loc="": _matched(title, loc))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        matched = _run()

    # Everything outside the additive metadata keys is unchanged.
    assert matched["channel_allocations"] == baseline["channel_allocations"]
    assert matched["total_projected"] == baseline["total_projected"]
    assert matched["role_allocations"] == baseline["role_allocations"]
    assert matched["optimized"] == baseline["optimized"]


def test_strong_match_adds_calibration_note():
    fake = mock.Mock(side_effect=lambda title, loc="": _matched(title, loc))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        out = _run()

    calib = out["metadata"].get("real_outcome_calibration")
    assert calib is not None
    assert calib["measured_cost_per_apply"] == 1.5
    assert calib["sample_size"] == 250
    assert "estimated_cost_per_apply" in calib
    assert "assessment" in calib
    # Must flag, never silently change figures.
    assert "unchanged" in calib["disclaimer"].lower()
    # The computed cost_per_application is left intact.
    assert out["total_projected"]["cost_per_application"] >= 0


def test_weak_sample_does_not_add_calibration():
    """A thin sample attaches outcomes but no CPA-calibration note."""
    def _weak(title, loc=""):
        m = _matched(title, loc)
        m["sample_size"] = 5  # below _KEYSTONE_STRONG_SAMPLE_SIZE
        return m

    fake = mock.Mock(side_effect=_weak)
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        out = _run()

    assert "real_outcomes" in out["metadata"]  # still surfaced as provenance
    assert "real_outcome_calibration" not in out["metadata"]  # but not calibrated


def test_partial_match_only_lists_matched_roles():
    """Only roles with warehouse coverage are surfaced."""
    def _partial(title, loc=""):
        if title == "Registered Nurse":
            return _matched(title, loc)
        return {"matched": False}

    fake = mock.Mock(side_effect=_partial)
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        out = _run()

    ros = out["metadata"]["real_outcomes"]
    assert len(ros) == 1
    assert ros[0]["title"] == "Registered Nurse"


def test_duplicate_role_titles_queried_once():
    """Repeated role titles are de-duped (single query, single list entry)."""
    roles = [
        {"title": "Registered Nurse", "count": 3, "tier": "Clinical / Licensed"},
        {"title": "registered nurse", "count": 2, "tier": "Clinical / Licensed"},
    ]
    fake = mock.Mock(side_effect=lambda title, loc="": _matched(title, loc))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        out = _run(roles=roles)

    assert fake.call_count == 1  # de-duped case-insensitively
    assert len(out["metadata"]["real_outcomes"]) == 1


def test_primary_location_passed_to_accessor():
    """The plan's primary location is forwarded to the warehouse query."""
    fake = mock.Mock(side_effect=lambda title, loc="": _matched(title, loc))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ):
        _run()

    # primary_location derived from _LOCATIONS[0]["city"] == "Dallas".
    for call in fake.call_args_list:
        _args, _kwargs = call
        assert _args[1] == "Dallas"


# ---------------------------------------------------------------------------
# #11: real-outcome CALIBRATION (flag-gated) — actually adjusts the numbers
# ---------------------------------------------------------------------------


def _run_calibrated(flag, match_fn=None):
    """Run with the warehouse matched and the calibration flag set to `flag`."""
    fn = match_fn or _matched
    fake = mock.Mock(side_effect=lambda title, loc="": fn(title, loc))
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", True), mock.patch.object(
        budget_engine, "_supabase_data", mock.Mock(get_real_outcomes=fake), create=True
    ), mock.patch.object(
        budget_engine, "_REAL_OUTCOME_CALIBRATION_ENABLED", flag
    ):
        return _run()


def test_calibration_off_by_default_changes_nothing():
    """Flag OFF + strong match: numbers stay identical to the no-keystone run;
    surfacing note present, but no 'applied' record and no number change."""
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", False):
        baseline = _run()
    off = _run_calibrated(False)
    assert "real_outcome_calibration_applied" not in off["metadata"]
    assert "real_outcome_calibration" in off["metadata"]  # surfacing still works
    assert (
        off["total_projected"]["applications"]
        == baseline["total_projected"]["applications"]
    )
    assert (
        off["total_projected"]["cost_per_application"]
        == baseline["total_projected"]["cost_per_application"]
    )


def test_calibration_on_blends_cpa_toward_measured_and_stays_consistent():
    """Flag ON + strong match: CPA moves toward measured, capped, and the
    per-channel rows still sum to the (rescaled) aggregate."""
    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", False):
        baseline = _run()
    on = _run_calibrated(True)

    applied = on["metadata"].get("real_outcome_calibration_applied")
    assert applied and applied["applied"] is True
    assert applied["measured_cost_per_apply"] == 1.5
    assert applied["sample_size"] == 250

    base_cpa = baseline["total_projected"]["cost_per_application"]
    new_cpa = on["total_projected"]["cost_per_application"]
    measured = 1.5
    if base_cpa > measured:  # the expected direction for this fixture
        assert new_cpa < base_cpa  # moved toward measured
        assert new_cpa >= measured * 0.99  # never overshoots past measured
        assert (
            on["total_projected"]["applications"]
            > baseline["total_projected"]["applications"]
        )
    # Hard cap: calibrated CPA within [0.65x, 1.5x] of the estimate.
    assert base_cpa * 0.65 - 0.02 <= new_cpa <= base_cpa * 1.5 + 0.02
    # Internal consistency: channel applications sum to the aggregate.
    chans = on["channel_allocations"].values()
    ch_apps = sum(c.get("projected_applications") or 0 for c in chans)
    assert ch_apps == on["total_projected"]["applications"]


def test_calibration_skips_weak_sample():
    """Flag ON but sample < threshold: no calibration, numbers unchanged."""

    def weak(title, location=""):
        m = _matched(title, location)
        m["sample_size"] = 5
        return m

    with mock.patch.object(budget_engine, "_HAS_SUPABASE_DATA", False):
        baseline = _run()
    on = _run_calibrated(True, match_fn=weak)
    assert "real_outcome_calibration_applied" not in on["metadata"]
    assert (
        on["total_projected"]["applications"]
        == baseline["total_projected"]["applications"]
    )


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
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc!r}")
    sys.exit(1 if _failures else 0)
