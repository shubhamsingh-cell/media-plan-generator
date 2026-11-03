"""Input-role gate on large briefs: the deck rolls extra roles into
"+N more roles", which is not a missing role.

Independent review 2026-11-03: a 17-role brief gated 6 x
input_role_missing@deck and a 20-role brief 9, with nothing wrong (the plan
role set and the workbook named every role). A role the deck leaves out is
now excused only while the deck shows a role rollup big enough to hold it AND
the plan role set and the workbook both name it. A deck-only absence without
a rollup, and a role genuinely dropped from the plan, stay critical.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import bundle_qa  # noqa: E402
import excel_v2  # noqa: E402
import ppt_generator  # noqa: E402
import research  # noqa: E402
import tools_regen_bundles as T  # noqa: E402

ROLES = [
    "Warehouse Associate", "Forklift Operator", "Picker Packer", "Shipping Clerk",
    "Receiving Clerk", "Inventory Specialist", "Maintenance Technician", "Electrician",
    "Welder", "Machine Operator", "Quality Inspector", "Production Supervisor",
    "Truck Driver", "Delivery Driver", "Dispatcher", "Customer Service Representative",
    "Registered Nurse", "Security Guard", "Janitor", "Office Manager",
]


def _bundle(roles):
    brief = {
        "client_name": "Rollup Test Co",
        "budget": "$400,000",
        "campaign_duration": "3 months",
        "campaign_start_month": 10,
        "industry": "logistics_supply_chain",
        "locations": ["Columbus, OH"],
        "roles": list(roles),
        "target_roles": [{"title": r, "count": 2} for r in roles],
    }
    data = T.build_plan_data(brief)
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research)
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    pptx = ppt_generator.generate_pptx(dict(data))
    pptx = pptx.getvalue() if hasattr(pptx, "getvalue") else pptx
    return data, pptx, xlsx


@pytest.fixture(scope="module")
def twenty():
    return _bundle(ROLES)


def _missing(findings):
    return [f for f in findings if f.get("code") == "input_role_missing"]


def test_a_correct_twenty_role_bundle_is_not_critical(twenty):
    data, pptx, xlsx = twenty
    data = dict(data, input_roles=list(ROLES))
    findings = bundle_qa.run_bundle_qa(pptx, xlsx, data)
    assert _missing(findings) == [], _missing(findings)[:3]
    assert "input_role_missing" not in bundle_qa.summarize_findings(findings)["codes"]


def test_a_genuinely_dropped_role_in_a_twenty_role_brief_stays_critical(twenty):
    data, pptx, xlsx = twenty
    typed = list(ROLES) + ["Battery Supervisor"]
    findings = bundle_qa.run_bundle_qa(pptx, xlsx, dict(data, input_roles=typed))
    missing = _missing(findings)
    assert missing and all(f["severity"] == "critical" for f in missing)
    assert {f["location"] for f in missing} >= {"plan", "workbook"}
    assert all("Battery Supervisor" in f["message"] for f in missing)
    assert not any("Battery Supervisor" not in f["message"] for f in missing)


def test_without_a_rollup_a_deck_only_absence_stays_critical():
    data = {"input_roles": ["Electrician", "Welder"], "target_roles": ["Electrician", "Welder"]}
    found = bundle_qa.input_role_findings(
        data, deck_text="Electrician hiring plan", workbook_text="Electrician Welder"
    )
    assert [(f["location"], "Welder" in f["message"]) for f in found] == [("deck", True)]


def test_a_rollup_excuses_no_more_roles_than_it_counts():
    roles = ["Electrician", "Welder", "Machinist", "Painter"]
    data = {"input_roles": roles, "target_roles": roles}
    found = bundle_qa.input_role_findings(
        data,
        deck_text="Electrician, Welder (+1 more role)",
        workbook_text=" ".join(roles),
    )
    assert len([f for f in found if f["location"] == "deck"]) == 1


def test_a_short_role_is_matched_on_word_boundaries_not_inside_words():
    data = {"input_roles": ["RN"], "target_roles": ["RN"]}
    found = bundle_qa.input_role_findings(
        data, deck_text="Applicants return to work", workbook_text="RN shifts"
    )
    assert [f["location"] for f in found] == ["deck"]
    assert bundle_qa.input_role_findings(
        data, deck_text="RN hiring", workbook_text="RN shifts"
    ) == []


def test_the_plan_message_counts_the_roles_that_actually_reached_it():
    data = {"input_roles": ["Welder", "Truck Driver"], "target_roles": ["Welder", "Registered Nurse"]}
    found = [f for f in bundle_qa.input_role_findings(data) if f["location"] == "plan"]
    assert len(found) == 1
    assert "Truck Driver" in found[0]["message"]
    assert "(1 of 2 roles reached the plan)" in found[0]["message"]


def test_a_different_trade_in_the_same_standardizer_bucket_is_not_the_role():
    import standardizer

    assert standardizer.normalize_role("Electrician") == standardizer.normalize_role("Plumber")
    data = {"input_roles": ["Electrician"], "target_roles": ["Plumber"]}
    found = [f for f in bundle_qa.input_role_findings(data) if f["location"] == "plan"]
    assert found and "Electrician" in found[0]["message"]


def test_the_standardized_title_itself_still_satisfies_an_alias():
    for typed, plan in (("RN", "Registered Nurse"), ("rn", "registered nurse"),
                        ("Electrician", "Construction Worker")):
        data = {"input_roles": [typed], "target_roles": [plan]}
        assert [f for f in bundle_qa.input_role_findings(data) if f["location"] == "plan"] == [], typed
