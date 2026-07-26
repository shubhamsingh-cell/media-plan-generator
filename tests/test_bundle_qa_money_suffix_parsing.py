"""Regression tests for currency-integrity defect 4: a FALSE POSITIVE in
bundle_qa's own QA gate.

INCIDENT: regenerating the real Uber bundle (GBP 2,000,000 plan) and
running ``bundle_qa.run_bundle_qa`` on the output reported

    exec_summary_budget_footing_mismatch: "Executive Summary channel
    Amount column sums to 2,000,000.00 but the stated Total Budget is
    2.00"

The stated total cell literally contains "£2.0M" (excel_v2's abbreviated-
money format for the Executive Summary's headline stat -- see
excel_v2._fmt_currency's abbreviated form). bundle_qa._parse_money_str
stripped every non-digit/./- character before parsing, so "£2.0M" silently
became "2.0" -- about 1,000,000x smaller than the real total, and the
footing check (correctly) flagged the resulting ~2,000,000x "mismatch" as
a critical, on a plan that was in fact correctly footed.

This matters as much as the 3 real defects (see
tests/test_currency_integrity_uber_gbp_defects.py): a critical that cries
wolf on every single plan trains people to ignore the gate, which is
precisely how the original 11 findings behind this whole incident got
shipped anyway.

THE IMPORTANT HALF: the fix must not weaken the check into a no-op. A
genuinely mis-footed M/K/B-shorthand plan must still fire -- see
test_footing_check_still_fires_on_genuinely_mismatched_gbp_plan below.

VACUOUSNESS: run against a pre-fix throwaway worktree (``git worktree add
--detach HEAD``) with only bundle_qa.py's fix reverted -- see the task
report for the observed pre-fix failures.
"""

from __future__ import annotations

import os
import sys

import pytest
from openpyxl import Workbook

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import bundle_qa  # noqa: E402


# ---------------------------------------------------------------------------
# Unit tests: _parse_money_str direct
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("£2.0M", 2_000_000.0),
        ("$2.0M", 2_000_000.0),
        ("2,000,000", 2_000_000.0),
        ("£150K", 150_000.0),
        ("42", 42.0),
    ],
)
def test_parse_money_str_handles_suffix_and_plain_values(raw, expected):
    assert bundle_qa._parse_money_str(raw) == expected


def test_parse_money_str_numeric_types_unchanged():
    assert bundle_qa._parse_money_str(2_000_000) == 2_000_000.0
    assert bundle_qa._parse_money_str(2_000_000.5) == 2_000_000.5


def test_parse_money_str_non_money_stays_none():
    assert bundle_qa._parse_money_str(None) is None
    assert bundle_qa._parse_money_str("TBD") is None
    assert bundle_qa._parse_money_str("") is None
    assert bundle_qa._parse_money_str("-") is None


def test_parse_money_str_lowercase_and_billion_suffix():
    assert bundle_qa._parse_money_str("$1.5m") == 1_500_000.0
    assert bundle_qa._parse_money_str("$1.2B") == 1_200_000_000.0


# ---------------------------------------------------------------------------
# Integration: _check_executive_summary_budget_footing
# ---------------------------------------------------------------------------


def _build_exec_summary_wb(
    total_budget_str: str, channel_amounts: list[float]
) -> Workbook:
    """Minimal Executive Summary sheet matching the exact shape
    ``_check_executive_summary_budget_footing`` scans for: a KV-block
    ("Total Budget" value directly above its label, same column -- this
    sheet's ``excel_v2._write_kv_row`` convention) and a Channel/Amount
    table terminated by a "Total" row."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Executive Summary"

    ws.append([None] * 6)  # row 1: filler
    ws.append([None, total_budget_str, None, "640", None, "£3,125"])  # row 2: values
    ws.append(
        [None, "Total Budget", None, "Projected Hires", None, "Cost / Hire"]
    )  # row 3: labels
    ws.append([None] * 6)  # row 4: blank
    ws.append([None, "Channel", "%", "Amount", "Proj. Clicks", "Proj. Apps"])  # row 5
    for i, amt in enumerate(channel_amounts):
        ws.append([None, f"Channel {i}", round(1.0 / len(channel_amounts), 3), amt])
    ws.append([None, "Total", None, sum(channel_amounts)])
    return wb


def test_footing_check_false_positive_fixed_on_correctly_footed_gbp_plan():
    """The exact incident: Total Budget '£2.0M', channel Amounts summing to
    2,000,000.00 (real per-channel splits from the Uber bundle) -- must NOT
    fire after the fix."""
    wb = _build_exec_summary_wb(
        "£2.0M",
        [573493.71, 451355.43, 426555.43, 391155.43, 100000.0, 57440.0],
    )
    findings: list = []
    bundle_qa._check_executive_summary_budget_footing(wb, findings)
    assert findings == [], f"false positive fired: {findings}"


def test_footing_check_still_fires_on_genuinely_mismatched_gbp_plan():
    """THE IMPORTANT HALF: a GENUINELY mis-footed M-shorthand plan (channel
    Amounts sum to 1.5M against a stated 2.0M total) must still fire --
    fixing the false positive must not turn this into a silent no-op."""
    wb = _build_exec_summary_wb("£2.0M", [500000.0, 500000.0, 500000.0])
    findings: list = []
    bundle_qa._check_executive_summary_budget_footing(wb, findings)
    assert len(findings) == 1
    assert findings[0]["code"] == "exec_summary_budget_footing_mismatch"
    assert findings[0]["severity"] == "critical"
    assert "1,500,000.00" in findings[0]["message"]
    assert "2,000,000.00" in findings[0]["message"]


def test_footing_check_plain_number_total_unaffected():
    """Non-abbreviated totals (no K/M/B suffix) must keep working exactly
    as before -- no regression introduced by the new suffix branch."""
    wb_ok = _build_exec_summary_wb("$150,000", [75000.0, 75000.0])
    findings_ok: list = []
    bundle_qa._check_executive_summary_budget_footing(wb_ok, findings_ok)
    assert findings_ok == []

    wb_bad = _build_exec_summary_wb("$150,000", [50000.0, 50000.0])
    findings_bad: list = []
    bundle_qa._check_executive_summary_budget_footing(wb_bad, findings_bad)
    assert len(findings_bad) == 1
    assert findings_bad[0]["code"] == "exec_summary_budget_footing_mismatch"


def test_footing_check_k_suffix_total():
    """A K-shorthand total ("£150K") parses correctly too, not just M."""
    wb_ok = _build_exec_summary_wb("£150K", [75000.0, 75000.0])
    findings_ok: list = []
    bundle_qa._check_executive_summary_budget_footing(wb_ok, findings_ok)
    assert findings_ok == []

    wb_bad = _build_exec_summary_wb("£150K", [50000.0, 50000.0])
    findings_bad: list = []
    bundle_qa._check_executive_summary_budget_footing(wb_bad, findings_bad)
    assert len(findings_bad) == 1


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
