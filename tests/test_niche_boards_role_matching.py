"""Plan-output audit (2026-10-11) -- niche board defects N1-N5.

Reference payloads: RTI International (healthcare_medical; Coordinator /
Worker / RN) and The Hershey Company (food_beverage; five plant roles).
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402

import excel_v2  # noqa: E402

HERSHEY_ROLES = [
    "Production Operator",
    "Maintenance Technician",
    "Food Scientist",
    "Warehouse Associate",
    "Quality Assurance Technician",
]


def _sheet_text(ws) -> str:
    return "\n".join(
        str(c.value) for row in ws.iter_rows() for c in row if c.value not in (None, "")
    )


def _niche_sheet_text(roles, industry) -> str:
    wb = openpyxl.Workbook()
    ws = wb.active
    excel_v2._build_sheet_niche_board_matching(
        ws,
        {
            "industry": industry,
            "target_roles": roles,
            "locations": ["Chicago, IL"],
            "client_name": "Audit Co",
        },
    )
    return _sheet_text(ws)


# -- N1 ---------------------------------------------------------------------


def test_n1_every_hershey_role_gets_a_niche_board():
    matches = excel_v2._match_roles_to_niche_boards(HERSHEY_ROLES, "food_beverage")
    missing = [r for r in HERSHEY_ROLES if not matches.get(r)]
    assert not missing, f"roles with no niche board: {missing} (got {matches})"


def test_n1_food_boards_are_the_expected_ones():
    matches = excel_v2._match_roles_to_niche_boards(HERSHEY_ROLES, "food_beverage")
    names = lambda role: {b["name"] for b in matches[role]}  # noqa: E731
    assert {"iHireManufacturing", "CareersInFood"} <= names("Production Operator")
    assert {"iHireManufacturing", "CareersInFood"} <= names("Maintenance Technician")
    assert {"IFT Career Center", "CareersInFood"} <= names("Food Scientist")
    assert {"IFT Career Center", "CareersInFood"} <= names("Quality Assurance Technician")


def test_n1_every_plan_industry_can_reach_its_own_role_boards():
    """The compatibility table must not silently strand a plan industry
    whose board tag is spelled differently (logistics_supply_chain vs
    transportation_logistics, legal_services vs professional_services)."""
    cases = [
        ("logistics_supply_chain", "Warehouse Associate"),
        ("logistics_supply_chain", "CDL Truck Driver"),
        ("legal_services", "Paralegal"),
        ("blue_collar_trades", "Warehouse Associate"),
    ]
    for industry, role in cases:
        assert excel_v2._match_roles_to_niche_boards([role], industry), (industry, role)


# -- N3 / N5 ----------------------------------------------------------------

_DEAD_OR_MISDESCRIBED = ("NurseFly", "Stack Overflow Jobs", "GitHub Jobs", "SeniorJobBank")


def test_n3_n5_dead_and_misdescribed_boards_absent_from_tables():
    blob = repr(excel_v2.ROLE_NICHE_BOARDS) + repr(excel_v2.INDUSTRY_NICHE_CHANNELS)
    present = [n for n in _DEAD_OR_MISDESCRIBED if n.lower() in blob.lower()]
    assert not present, present
    assert "nursefly.com" not in blob and "seniorjobbank.com" not in blob
