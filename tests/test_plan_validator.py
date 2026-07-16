"""Unit tests for plan_validator -- cross-validation post-processing layer.

Focus: Check 7 (budget_allocation_sum) added in S89, plus a regression
guard that the full validate_plan() entry point runs every registered
check without raising.

Runs under pytest, or standalone: ``python3 tests/test_plan_validator.py``.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from plan_validator import (  # noqa: E402
    _check_budget_allocation_sum,
    validate_plan,
)


def _make_plan(channel_dollars: dict, total_budget: float) -> dict:
    """Build a minimal plan dict with the given channel dollar amounts."""
    allocations = {}
    for name, dollars in channel_dollars.items():
        # CPC $2, apply rate 10%, hire rate 2% -- linear projections
        clicks = int(dollars / 2)
        apps = int(clicks * 0.10)
        hires = max(1, int(apps * 0.02))
        allocations[name] = {
            "dollar_amount": float(dollars),
            "projected_clicks": clicks,
            "projected_applications": apps,
            "projected_hires": hires,
            "cpa": round(dollars / max(apps, 1), 2),
        }
    total_clicks = sum(c["projected_clicks"] for c in allocations.values())
    total_apps = sum(c["projected_applications"] for c in allocations.values())
    total_hires = sum(c["projected_hires"] for c in allocations.values())
    return {
        "_budget_allocation": {
            "channel_allocations": allocations,
            "total_projected": {
                "clicks": total_clicks,
                "applications": total_apps,
                "hires": total_hires,
                "cost_per_hire": round(total_budget / max(total_hires, 1), 2),
                "cost_per_application": round(total_budget / max(total_apps, 1), 2),
                "cost_per_click": round(total_budget / max(total_clicks, 1), 2),
            },
            "metadata": {"total_budget": float(total_budget)},
        }
    }


def test_allocation_within_tolerance_is_clean():
    """Sum within +/-2% of budget should produce no findings."""
    data = _make_plan({"Indeed": 60_000, "LinkedIn": 39_500}, 100_000)
    findings = _check_budget_allocation_sum(data)
    assert findings == []


def test_over_allocation_is_rescaled_proportionally():
    """Sum 50% over budget must be scaled down to the total budget."""
    data = _make_plan({"Indeed": 90_000, "LinkedIn": 60_000}, 100_000)
    findings = _check_budget_allocation_sum(data)

    assert len(findings) == 1
    finding = findings[0]
    assert finding["check"] == "budget_allocation_sum"
    assert finding["severity"] == "high"
    assert finding["auto_corrected"] is True

    allocs = data["_budget_allocation"]["channel_allocations"]
    new_sum = sum(c["dollar_amount"] for c in allocs.values())
    assert abs(new_sum - 100_000) < 1.0  # rescaled to budget (rounding slack)
    # 90K/150K and 60K/150K ratios preserved
    assert abs(allocs["Indeed"]["dollar_amount"] - 60_000) < 1.0
    assert abs(allocs["LinkedIn"]["dollar_amount"] - 40_000) < 1.0
    assert allocs["Indeed"]["_validator_corrected_allocation"] is True


def test_over_allocation_preserves_cpa():
    """Projections scale with dollars, so per-channel CPA must not drift."""
    data = _make_plan({"Indeed": 90_000, "LinkedIn": 60_000}, 100_000)
    allocs = data["_budget_allocation"]["channel_allocations"]
    cpa_before = {n: c["cpa"] for n, c in allocs.items()}

    _check_budget_allocation_sum(data)

    for name, ch in allocs.items():
        recomputed = ch["dollar_amount"] / max(ch["projected_applications"], 1)
        # Within 5% of stated CPA (integer rounding on projections)
        assert abs(recomputed - cpa_before[name]) / cpa_before[name] < 0.05


def test_over_allocation_rescales_aggregate_totals():
    """total_projected clicks/apps/hires and unit costs follow the rescale."""
    data = _make_plan({"Indeed": 90_000, "LinkedIn": 60_000}, 100_000)
    totals_before = dict(data["_budget_allocation"]["total_projected"])

    _check_budget_allocation_sum(data)

    totals = data["_budget_allocation"]["total_projected"]
    assert totals["clicks"] < totals_before["clicks"]
    assert totals["applications"] < totals_before["applications"]
    assert abs(totals["cost_per_application"] - 100_000 / totals["applications"]) < 0.5


def test_under_allocation_is_flagged_not_corrected():
    """Sum 20% under budget is flagged low-severity, channels untouched."""
    data = _make_plan({"Indeed": 50_000, "LinkedIn": 30_000}, 100_000)
    findings = _check_budget_allocation_sum(data)

    assert len(findings) == 1
    finding = findings[0]
    assert finding["severity"] == "low"
    assert finding["auto_corrected"] is False
    assert abs(finding["unallocated"] - 20_000) < 1.0

    allocs = data["_budget_allocation"]["channel_allocations"]
    assert allocs["Indeed"]["dollar_amount"] == 50_000.0  # unchanged


def test_missing_budget_or_channels_is_noop():
    """Zero budget, empty allocations, or missing keys produce no findings."""
    assert _check_budget_allocation_sum({}) == []
    assert (
        _check_budget_allocation_sum(
            {"_budget_allocation": {"channel_allocations": {}, "metadata": {}}}
        )
        == []
    )
    data = _make_plan({"Indeed": 50_000}, 100_000)
    data["_budget_allocation"]["metadata"]["total_budget"] = 0
    assert _check_budget_allocation_sum(data) == []


def test_validate_plan_runs_all_checks_and_reports():
    """Entry point runs all 7 checks; over-allocation surfaces in summary."""
    data = _make_plan({"Indeed": 90_000, "LinkedIn": 60_000}, 100_000)
    summary = validate_plan(data)

    assert summary["checks_run"] == 7
    assert summary["checks_failed"] == 0
    assert summary["auto_corrections"] >= 1
    assert any(f["check"] == "budget_allocation_sum" for f in summary["findings"])
    assert data["_validation"] is summary


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
