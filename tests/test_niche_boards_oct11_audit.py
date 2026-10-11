"""Niche-board role gating must keep the boards real roles need (review 2026-10-11)."""

from __future__ import annotations


def test_cna_role_keeps_mycnajobs_after_role_gating():
    """Review finding 2026-10-11: gating myCNAjobs.com on senior_care alone hid
    the most relevant board from CNA plans (a CNA role matches 'nursing')."""
    import excel_v2

    for role in ("CNA", "Certified Nursing Assistant"):
        boards = excel_v2._industry_niche_boards_for_roles("healthcare_medical", [role])
        names = [b.get("name") if isinstance(b, dict) else b for b in boards]
        assert any("myCNAjobs" in str(n) for n in names), (role, names)


def test_rn_only_plan_does_not_list_mycnajobs():
    """Review 2026-10-11: gating myCNAjobs.com on the broad 'nursing' category
    showed a CNA board for every RN / ICU nurse / NP / LPN plan. The gate is
    CNA-specific (role tokens), not the nursing category."""
    import excel_v2

    for role in ("Registered Nurse", "RN", "ICU Nurse", "Nurse Practitioner", "LPN"):
        boards = excel_v2._industry_niche_boards_for_roles("healthcare_medical", [role])
        names = [b.get("name") if isinstance(b, dict) else b for b in boards]
        assert not any("myCNAjobs" in str(n) for n in names), (role, names)
    # other nursing boards are untouched
    boards = excel_v2._industry_niche_boards_for_roles("healthcare_medical", ["Registered Nurse"])
    assert any("Nurse.com" in str(b) for b in boards)
    # a CNA among other roles still gets it
    mixed = excel_v2._industry_niche_boards_for_roles(
        "healthcare_medical", ["Registered Nurse", "Nursing Assistant"]
    )
    assert any("myCNAjobs" in str(b) for b in mixed)


def test_certified_nurse_midwife_is_not_a_cna():
    """Review 2026-10-11: the CNA pattern matched any 'certified nurs...' title."""
    import excel_v2

    for role in ("Certified Nurse Midwife", "CRNA", "Certified Registered Nurse Anesthetist"):
        boards = excel_v2._industry_niche_boards_for_roles("healthcare_medical", [role])
        assert not any("myCNAjobs" in str(b) for b in boards), role
    for role in ("Certified Nursing Assistant", "Certified Nurse Aide", "CNA"):
        boards = excel_v2._industry_niche_boards_for_roles("healthcare_medical", [role])
        assert any("myCNAjobs" in str(b) for b in boards), role
