"""classify_industry(): specific manufacturing sectors vs the generic bucket.

Client incident (Hershey, 2026-09-24): Step 4 scores sectors by
sum(len(keyword)), and the generic "manufacturing" sector's "manufactur"
(10) beat the product word, so "chocolate manufacturing", "beverage
manufacturing", "pharmaceutical manufacturing", "aircraft manufacturing"
etc. all resolved to Manufacturing & Industrial (legacy_key "automotive",
automotive niche boards).

Two earlier fixes failed review. This table keeps every case either of them
broke, so a later change cannot reintroduce them:
  - attempt 1 deferred manufacturing whenever ANY sector scored >= 3, so
    role titles ("Warehouse Associate", "Maintenance Technician") pulled
    real manufacturing plans to logistics / tech.
  - attempt 2 limited that to keywords found in the industry text, but
    matched bare substrings ("port" in "sporting", "agri" in
    "agricultural", "electric", "power", "space", "gas") against every
    sector. It still let role text decide the winner, and its
    pharma/healthcare tie-break fired on a coincidental equal-length tie.

The GUARD rows already pass on main (main never had those bugs). They exist
to fail on the two failed attempts, and they do.
"""

from __future__ import annotations

import logging

import pytest

import app


@pytest.fixture(autouse=True)
def _quiet():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


HERSHEY_ROLES = [
    "Production Supervisor",
    "Packaging Operator",
    "Maintenance Technician",
    "Food Safety Specialist",
]

# (raw_industry, company_name, roles, expected legacy_key)
FIXED_CASES = [
    # Food & beverage manufacturers (the Hershey incident).
    ("chocolate manufacturing", "", [], "food_beverage"),
    ("beverage manufacturing", "", [], "food_beverage"),
    ("food manufacturing", "", [], "food_beverage"),
    ("candy manufacturing", "", [], "food_beverage"),
    ("snack food manufacturing", "Frito-Lay", ["Machine Operator"], "food_beverage"),
    ("confectionery", "", [], "food_beverage"),
    ("confectionery manufacturing", "The Hershey Company", HERSHEY_ROLES, "food_beverage"),
    ("chocolate manufacturing", "", ["Warehouse Associate", "Machine Operator"], "food_beverage"),
    ("chocolate manufacturing", "", ["Maintenance Technician"], "food_beverage"),
    ("bakery", "Acme Manufacturing", ["Assembler"], "food_beverage"),
    # Pharma / biotech / medical devices.
    ("pharmaceutical manufacturing", "", [], "pharma_biotech"),
    ("pharma manufacturing", "", [], "pharma_biotech"),
    ("biotech manufacturing", "", [], "pharma_biotech"),
    ("vaccine manufacturing", "", [], "pharma_biotech"),
    ("drug manufacturing", "", [], "pharma_biotech"),
    ("medical device manufacturing", "", [], "pharma_biotech"),
    # Genuine shared-keyword tie: healthcare's only hits are "pharma"/"biotech",
    # which the pharma profile also has.
    ("pharmaceutical", "", [], "pharma_biotech"),
    ("biotech", "", [], "pharma_biotech"),
    ("Pharma & Biotech", "", [], "pharma_biotech"),
    ("pharmaceutical", "Pfizer", ["Quality Analyst"], "pharma_biotech"),
    # Aerospace / defense.
    ("aircraft manufacturing", "", [], "aerospace_defense"),
    ("defense manufacturing", "", [], "aerospace_defense"),
    ("satellite manufacturing", "", ["Maintenance Technician"], "aerospace_defense"),
]

GUARD_CASES = [
    # Attempt 1: role titles / company must not pull a manufacturing plan away.
    ("steel manufacturing", "", ["Warehouse Associate", "Machine Operator"], "automotive"),
    ("automotive manufacturing", "", ["Maintenance Technician"], "automotive"),
    ("industrial manufacturing", "Caterpillar", ["Welder", "Machine Operator"], "automotive"),
    ("", "Acme Manufacturing", ["Maintenance Technician"], "automotive"),
    ("automotive manufacturing", "General Motors", ["Assembly Line Worker"], "automotive"),
    ("textile manufacturing", "", ["Sewing Machine Operator"], "automotive"),
    ("electronics manufacturing", "Jabil", ["Electronics Assembler", "Test Technician"], "automotive"),
    # Attempt 2(a): incidental substrings / unrelated sectors must never win.
    ("sporting goods manufacturing", "Wilson Sporting Goods", ["Production Associate"], "automotive"),
    ("sporting goods manufacturing", "", [], "automotive"),
    ("agricultural equipment manufacturing", "John Deere", ["Assembler", "Welder"], "automotive"),
    ("agricultural equipment manufacturing", "", [], "automotive"),
    ("electric vehicle manufacturing", "Rivian", ["Production Associate"], "automotive"),
    ("electric vehicle manufacturing", "", [], "automotive"),
    ("power tools manufacturing", "Stanley Black & Decker", ["Assembler"], "automotive"),
    ("power tools manufacturing", "", [], "automotive"),
    ("space heater manufacturing", "Lasko", ["Assembler"], "automotive"),
    ("space heater manufacturing", "", [], "automotive"),
    ("gas cylinder manufacturing", "Worthington Industries", ["Welder"], "automotive"),
    ("gas cylinder manufacturing", "", [], "automotive"),
    # Attempt 2(b): a role's substring hit must not decide the winner.
    ("trailer manufacturing", "", ["Maintenance Technician"], "automotive"),
    ("paint manufacturing", "", ["Maintenance Technician"], "automotive"),
    # Attempt 2(c): a coincidental equal-length tie ("medical" 7 vs
    # "vaccine" 7) is not the shared pharma/biotech keyword tie.
    ("medical", "Mayo Clinic", ["Vaccine Coordinator"], "healthcare_medical"),
    # Only the explicit industry text can trigger the override: product
    # words in roles or the company name do not.
    ("steel manufacturing", "", ["Food Safety Specialist"], "automotive"),
    ("steel manufacturing", "Tyson Foods", ["Machine Operator"], "automotive"),
    ("industrial manufacturing", "Hershey Chocolate", [], "automotive"),
    # Brand bonus with no industry text: manufacturing's +1000 company hit
    # wins and is never overridden.
    ("", "Acme Food Manufacturing", [], "automotive"),
    # Equipment / packaging makers are generic manufacturing.
    ("food processing equipment manufacturing", "", [], "automotive"),
    ("food packaging manufacturing", "", [], "automotive"),
    ("beverage can manufacturing", "", [], "automotive"),
    # "drug store" is retail, not a pharma product term.
    ("drug store", "", ["Production Supervisor"], "automotive"),
    # "pharmacy" is not a pharma-industry term; retail pharmacies stay healthcare.
    ("retail pharmacy", "", [], "healthcare_medical"),
    ("pharmacy", "", [], "healthcare_medical"),
    # Already-correct results that must stay put.
    ("food & beverage manufacturing", "", [], "food_beverage"),
    ("aerospace manufacturing", "", [], "aerospace_defense"),
    ("manufacturing", "", [], "automotive"),
    ("semiconductor manufacturing", "", [], "automotive"),
    ("Food Service", "Sodexo", ["Cook"], "hospitality_travel"),
    ("restaurant", "Olive Garden", ["Line Cook", "Server"], "hospitality_travel"),
    ("healthcare", "Mercy General Hospital", ["Registered Nurse"], "healthcare_medical"),
    ("retail", "Target", ["Store Associate", "Cashier"], "retail_consumer"),
    ("technology", "Acme Software", ["Software Engineer", "DevOps Engineer"], "tech_engineering"),
    ("construction", "ABC Construction Co", ["Site Supervisor", "General Laborer"], "construction_real_estate"),
    ("logistics", "FastFreight Logistics", ["Warehouse Associate", "CDL Driver"], "logistics_supply_chain"),
    ("hospitality", "Marriott Hotels", ["Front Desk Agent", "Housekeeper"], "hospitality_travel"),
    ("software", "Sweet Treats Chocolate Co", [], "tech_engineering"),
    ("hospital", "Sweet Treats Chocolate Co", [], "healthcare_medical"),
]


def _case_id(case):
    raw, company, roles, expected = case
    return f"{raw or '<empty>'}|{company or '-'}|{len(roles)}roles->{expected}"


@pytest.mark.parametrize(
    "raw,company,roles,expected", FIXED_CASES, ids=[_case_id(c) for c in FIXED_CASES]
)
def test_specific_manufacturing_sector_wins(raw, company, roles, expected):
    r = app.classify_industry(raw, company, list(roles))
    assert r.get("legacy_key") == expected, (raw, company, roles, r.get("sector"))


@pytest.mark.parametrize(
    "raw,company,roles,expected", GUARD_CASES, ids=[_case_id(c) for c in GUARD_CASES]
)
def test_prior_attempt_regressions_stay_fixed(raw, company, roles, expected):
    r = app.classify_industry(raw, company, list(roles))
    assert r.get("legacy_key") == expected, (raw, company, roles, r.get("sector"))


def test_free_text_hershey_plan_matches_the_dropdown_pick():
    """Typing the industry must give the same result as picking Food &
    Beverage from the dropdown (legacy key), including the conflict field."""
    typed = app.classify_industry(
        "chocolate manufacturing", "The Hershey Company", HERSHEY_ROLES
    )
    picked = app.classify_industry("food_beverage", "The Hershey Company", HERSHEY_ROLES)
    assert typed.get("legacy_key") == picked.get("legacy_key") == "food_beverage"
    assert typed.get("industry_conflict") == picked.get("industry_conflict")


def test_override_never_returns_a_non_allowlisted_sector():
    """Only food_beverage, pharma and aerospace can replace the generic
    manufacturing bucket, whatever the industry text says."""
    allowed = {"food_beverage", "pharma", "aerospace"}
    assert {key for key, _, _ in app._PRODUCT_SECTOR_PATTERNS} == allowed
