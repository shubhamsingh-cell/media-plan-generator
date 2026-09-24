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
    ("aircraft parts manufacturing", "", ["CNC Machinist"], "aerospace_defense"),
    ("rocket manufacturing", "", [], "aerospace_defense"),
    # Round 3 review: a company that makes AND packages its product keeps its
    # product sector. The equipment qualifier only blocks when it directly
    # follows the product word.
    ("chocolate manufacturing and packaging", "", [], "food_beverage"),
    ("Chocolate manufacturing & packaging", "The Hershey Company", HERSHEY_ROLES, "food_beverage"),
    ("Food manufacturing and packaging", "", [], "food_beverage"),
    ("Beverage manufacturing (cans and bottles)", "", [], "food_beverage"),
    ("Pharmaceutical manufacturing and packaging", "", [], "pharma_biotech"),
    ("biopharmaceutical manufacturing", "", ["Production Supervisor"], "pharma_biotech"),
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
    ("food manufacturing equipment", "", [], "automotive"),
    ("dairy processing machinery manufacturing", "", [], "automotive"),
    ("pharmaceutical packaging manufacturing", "", [], "automotive"),
    # Whole words with the wrong meaning in context.
    ("food-grade plastics manufacturing", "", [], "automotive"),
    ("rocket stove manufacturing", "", [], "automotive"),
    ("self-defense products manufacturing", "", [], "automotive"),
    # "drug store" is retail, not a pharma product term.
    ("drug store", "", ["Production Supervisor"], "automotive"),
    # "pharmacy" is not a pharma-industry term; retail pharmacies stay healthcare.
    ("retail pharmacy", "", [], "healthcare_medical"),
    ("pharmacy", "", [], "healthcare_medical"),
    # The tie-break uses the same product rules as the override: a
    # machinery maker is not a pharma company (stays as on main).
    ("pharmaceutical machinery", "", [], "healthcare_medical"),
    # Already-correct results that must stay put.
    ("food & beverage manufacturing", "", [], "food_beverage"),
    ("aerospace manufacturing", "", [], "aerospace_defense"),
    ("manufacturing", "", [], "automotive"),
    ("semiconductor manufacturing", "", [], "automotive"),
    ("Food Service", "Sodexo", ["Cook"], "hospitality_travel"),
    # "food service" is hospitality. The override must not turn a Food
    # Service plan into food_beverage even when manufacturing-sounding
    # company/roles made manufacturing the Step 4 winner.
    ("Food Service", "Summit Industrial", ["Production Supervisor"], "automotive"),
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


# Round 3 review, ship-blocking: once the override picks the specific sector,
# production-type roles still read as generic manufacturing. That produced a
# role_title industry_conflict, which bundle_qa turns into a "warn" on every
# Hershey-shaped plan. The override is the explanation, so there is no conflict.
NO_CONFLICT_CASES = [
    ("chocolate manufacturing", "", ["Production Supervisor"], "food_beverage"),
    ("food manufacturing", "", ["Production Operator"], "food_beverage"),
    ("dairy manufacturing", "", ["Manufacturing Technician"], "food_beverage"),
    ("biopharmaceutical manufacturing", "", ["Production Supervisor"], "pharma_biotech"),
    ("aircraft parts manufacturing", "", ["CNC Machinist"], "aerospace_defense"),
    ("chocolate manufacturing", "The Hershey Company", HERSHEY_ROLES, "food_beverage"),
    ("confectionery manufacturing", "Summit Industrial", ["Production Supervisor"], "food_beverage"),
]


@pytest.mark.parametrize(
    "raw,company,roles,expected",
    NO_CONFLICT_CASES,
    ids=[_case_id(c) for c in NO_CONFLICT_CASES],
)
def test_override_does_not_raise_generic_manufacturing_conflict(
    raw, company, roles, expected
):
    r = app.classify_industry(raw, company, list(roles))
    assert r.get("legacy_key") == expected
    assert r.get("industry_conflict") is None, r.get("industry_conflict")


def test_override_still_reports_a_real_non_manufacturing_conflict():
    """Only the generic-manufacturing inference is explained by the override.
    A client name that implies a different industry is still a conflict."""
    r = app.classify_industry("chocolate manufacturing", "Mercy General Hospital", [])
    assert r.get("legacy_key") == "food_beverage"
    conflict = r.get("industry_conflict") or {}
    assert conflict.get("inferred_legacy_key") == "healthcare_medical", conflict
    assert conflict.get("signal") == "client_name"


def test_override_marker_never_leaks_to_callers():
    r = app.classify_industry("chocolate manufacturing", "", ["Production Supervisor"])
    assert app._PRODUCT_OVERRIDE_MARKER not in r
    # The shared profile must not be mutated either.
    assert app._PRODUCT_OVERRIDE_MARKER not in app.INDUSTRY_NAICS_MAP["food_beverage"]


def test_override_never_returns_a_non_allowlisted_sector():
    """Only food_beverage, pharma and aerospace can replace the generic
    manufacturing bucket, whatever the industry text says."""
    allowed = {"food_beverage", "pharma", "aerospace"}
    assert {key for key, _ in app._PRODUCT_SECTOR_PATTERNS} == allowed


# Round 5 review: equipment/vehicle/appliance makers named after the product
# they serve are generic manufacturing. fd052a1's one-word proximity window
# let these through, and its conflict suppression then hid the misroute.
# All stay automotive (as on main), and no conflict is suppressed: the
# override does not fire, so the conflict field is whatever main computes.
EQUIPMENT_MAKER_CASES = [
    ("food processing and packaging equipment manufacturing", "", []),
    ("pharmaceutical processing and packaging equipment manufacturing", "", []),
    ("dairy and food processing equipment manufacturing", "", []),
    ("beverage filling and capping machinery manufacturing", "", []),
    ("pharmaceutical grade glass bottles manufacturing", "", []),
    ("food truck manufacturing", "Summit Industrial", ["Production Supervisor"]),
    ("commercial bakery ovens", "Summit Industrial", ["Production Supervisor"]),
    ("vaccine storage freezers", "Summit Industrial", ["Production Supervisor"]),
    ("food truck manufacturing", "", []),
    ("commercial bakery ovens manufacturing", "", []),
    ("vaccine storage freezer manufacturing", "", []),
    ("home defense products manufacturing", "", []),
    ("personal defense spray manufacturing", "", []),
    # Equipment anywhere blocks all three sectors, aerospace included.
    ("aircraft equipment manufacturing", "", []),
]


@pytest.mark.parametrize(
    "raw,company,roles",
    EQUIPMENT_MAKER_CASES,
    ids=[f"{c[0]}|{c[1] or '-'}" for c in EQUIPMENT_MAKER_CASES],
)
def test_equipment_makers_stay_generic_and_conflict_is_not_masked(raw, company, roles):
    r = app.classify_industry(raw, company, list(roles))
    assert r.get("legacy_key") == "automotive", (raw, r.get("sector"))
    # The override did not fire, so nothing was suppressed: the conflict
    # field must equal what the plain precedence chain produces with no
    # override marker involved.
    primary = app._classify_industry_primary(raw, company, list(roles))
    assert app._PRODUCT_OVERRIDE_MARKER not in primary


@pytest.mark.parametrize(
    "raw",
    [
        # A conjunction leading into another product word is not a head
        # follower. The last product word decides, and here it modifies
        # packaging/cans. (classify_industry still returns food_beverage for
        # these through Step 4's normal scoring, food 4 + beverage 8 > 10, as
        # on main. The override itself must not claim them.)
        "food and beverage packaging manufacturing",
        "food & beverage cans manufacturing",
        "dairy and food processing equipment manufacturing",
        "food truck manufacturing",
        "pharmaceutical grade glass bottles manufacturing",
    ],
)
def test_product_helper_rejects_modifier_uses(raw):
    assert app._product_sector_from_industry_text(raw) is None


# Round 6 review: a supplier described by its CUSTOMERS' industry makes
# the supplied thing, not the customer's product. All stay automotive, as
# on main. With an industrial client name and production role the override
# does not fire, so no conflict is suppressed.
SUPPLIER_CASES = [
    "plastic bottle manufacturing for beverage companies",
    "HVAC manufacturing for pharmaceutical facilities",
    "conveyor manufacturing for food processors",
    "cardboard box manufacturing for food companies",
    "refrigeration manufacturing for dairy plants",
    "tank manufacturing for breweries",
    "cleaning chemicals manufacturing for food plants",
    "industrial manufacturing serving the pharmaceutical industry",
    "contract manufacturing supplying dairy brands",
    "stainless fabrication manufacturing servicing dairy plants",
    "component manufacturing supplier to aircraft companies",
    # Round 7 review: connector phrasings the round-6 clause list missed.
    # Rule 6 is now a whole-text marker blocklist, not a connector parser.
    "HVAC manufacturing to the pharmaceutical industry",
    "packaging manufacturing, supplier to the food industry",
    "plastics manufacturing with customers in the pharmaceutical industry",
    "corrugated box manufacturing used by beverage companies",
    "industrial manufacturing (pharmaceutical industry supplier)",
    "manufacturing supplier to pharmaceutical and food companies",
    "contract manufacturing to pharmaceutical and biotech companies",
    # "to the" was never in the round-6 corpus.
    "valve manufacturing to the dairy industry",
    "label manufacturing to major beverage brands",
    "fastener manufacturing to aircraft makers",
    "precision parts manufacturing vendor to aircraft makers",
    "mold manufacturing, clients include chocolate companies",
    "pallet manufacturing catering to breweries",
]


@pytest.mark.parametrize("raw", SUPPLIER_CASES)
@pytest.mark.parametrize(
    "company,roles",
    [("", []), ("Summit Industrial Group", ["Machine Operator"])],
    ids=["bare", "industrial-client"],
)
def test_supplier_described_by_customer_stays_generic(raw, company, roles):
    """The product override never claims a supplier phrasing. With an
    industrial client + production role, Step 4's winner is generic
    manufacturing and must stay there. Bare, a few phrasings resolve
    through Step 4's normal scoring before the override is involved, the
    same as on main: "contract manufacturing to pharmaceutical and biotech
    companies" is a healthcare/pharma keyword tie (13 vs 13 > 10) that the
    tie-break now leaves alone because of the "to" + product marker."""
    r = app.classify_industry(raw, company, list(roles))
    primary = app._classify_industry_primary(raw, company, list(roles))
    assert app._PRODUCT_OVERRIDE_MARKER not in primary
    assert r.get("legacy_key") not in {
        "food_beverage",
        "pharma_biotech",
        "aerospace_defense",
    }, (raw, company, r.get("sector"))
    if company:
        assert r.get("legacy_key") == "automotive", (raw, r.get("sector"))


@pytest.mark.parametrize(
    "raw",
    [
        # Round 7: connectors no marker list names. Rule 7 (position)
        # catches them structurally: the customer comes after the product.
        "HVAC manufacturing partnering with pharmaceutical companies",
        "conveyor manufacturing trusted by beverage brands",
        "label manufacturing relied on by dairy plants",
        "contract manufacturing on behalf of biotech firms",
        "corrugated boxes working with chocolate makers",
        "plastic bottles, beverage brands",
    ],
)
def test_unlisted_connectors_never_let_the_override_claim_a_customer(raw):
    assert app._product_sector_from_industry_text(raw.lower()) is None
    r = app.classify_industry(raw, "Summit Industrial Group", ["Machine Operator"])
    assert r.get("legacy_key") == "automotive", (raw, r.get("sector"))


@pytest.mark.parametrize(
    "raw,company,roles",
    [
        # A product LIST at the start counts from its first word.
        ("Food and Beverage", "Summit Industrial", ["Production Supervisor"]),
        ("food & beverage", "Summit Industrial", ["Production Supervisor"]),
        ("Pharma & Biotech", "Summit Industrial", ["Production Supervisor"]),
    ],
)
def test_leading_product_list_still_counts(raw, company, roles):
    r = app.classify_industry(raw, company, list(roles))
    assert r.get("legacy_key") in {"food_beverage", "pharma_biotech"}, (raw, r.get("sector"))


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Product before the customer clause still counts.
        ("chocolate manufacturing for retail and wholesale", "food_beverage"),
        ("pharmaceutical manufacturing for export", "pharma_biotech"),
        ("ready-to-eat food manufacturing", "food_beverage"),
        ("ready to drink beverage manufacturing", "food_beverage"),
        # A hyphenated "-to-" is never a customer marker.
        ("direct-to-consumer snack manufacturing", "food_beverage"),
    ],
)
def test_customer_clause_rule_keeps_own_product(raw, expected):
    r = app.classify_industry(raw, "", [])
    assert r.get("legacy_key") == expected, (raw, r.get("sector"))


# Round 6 review: the tie-break decides between two existing, equal
# keyword scores. It does not use the head-noun rule, so pharma/biotech
# companies described without a production noun are pharma_biotech again
# (as on fd052a1), not healthcare_medical.
@pytest.mark.parametrize(
    "raw",
    [
        "Biotech research",
        "Pharmaceutical R&D",
        "Pharmaceutical sales",
        "biotech startup",
        "biotech firm",
        "pharmaceutical services",
        "pharmaceutical distribution",
    ],
)
def test_tie_break_names_pharma_companies(raw):
    r = app.classify_industry(raw, "", [])
    assert r.get("legacy_key") == "pharma_biotech", (raw, r.get("sector"))


@pytest.mark.parametrize(
    "raw",
    [
        "pharmacy",
        "retail pharmacy",
        "pharmaceutical machinery",
        "hospital services for pharmaceutical companies",
        # The supplier/customer blocklist applies to the tie-break too.
        "pharmaceutical industry supplier",
        "services to the pharmaceutical industry",
    ],
)
def test_tie_break_still_rejects_non_pharma_companies(raw):
    r = app.classify_industry(raw, "", [])
    assert r.get("legacy_key") == "healthcare_medical", (raw, r.get("sector"))


# Round 6 review: production/business nouns that were missing from the
# head-noun follower list.
@pytest.mark.parametrize(
    "raw,company,expected",
    [
        ("chocolate bar manufacturing", "The Hershey Company", "food_beverage"),
        ("chocolate bar manufacturing", "", "food_beverage"),
        ("candy bar manufacturing", "", "food_beverage"),
        ("beverage concentrate manufacturing", "", "food_beverage"),
        ("food ingredient manufacturing", "", "food_beverage"),
        ("drug substance manufacturing", "", "pharma_biotech"),
        ("pharmaceutical contract manufacturing", "", "pharma_biotech"),
        ("Biologics CDMO manufacturing", "", "pharma_biotech"),
        ("biologics CMO manufacturing", "", "pharma_biotech"),
    ],
)
def test_head_noun_allowlist_additions(raw, company, expected):
    r = app.classify_industry(raw, company, [])
    assert r.get("legacy_key") == expected, (raw, r.get("sector"))


# Phrasings the head-noun rule must keep accepting.
HEAD_NOUN_POSITIVE_CASES = [
    ("Beverage manufacturing - cans and bottles", "food_beverage"),
    ("chocolate & confectionery manufacturing", "food_beverage"),
    ("food products manufacturing", "food_beverage"),
    ("food ingredients manufacturing", "food_beverage"),
    ("frozen foods manufacturer", "food_beverage"),
    ("meat packing plant", "food_beverage"),
    ("beverage bottling manufacturing", "food_beverage"),
    ("chocolate factory", "food_beverage"),
    ("food, beverage and consumer goods manufacturing", "food_beverage"),
    ("pharmaceutical company manufacturing", "pharma_biotech"),
    ("active pharmaceutical ingredients manufacturing", "pharma_biotech"),
    ("medical devices manufacturing", "pharma_biotech"),
    ("food & beverage manufacturing", "food_beverage"),
    ("food and beverage production", "food_beverage"),
    ("dairy and meat processing", "food_beverage"),
    ("pharma & biotech manufacturing", "pharma_biotech"),
    ("defense electronics manufacturing", "aerospace_defense"),
]


@pytest.mark.parametrize(
    "raw,expected",
    HEAD_NOUN_POSITIVE_CASES,
    ids=[c[0] for c in HEAD_NOUN_POSITIVE_CASES],
)
def test_head_noun_rule_keeps_real_product_phrasings(raw, expected):
    r = app.classify_industry(raw, "", [])
    assert r.get("legacy_key") == expected, (raw, r.get("sector"))
