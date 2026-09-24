"""Regression: the role-text fuzzy scan matches whole words only.

When no role-title vote reaches a majority, _infer_industry_from_signals
falls back to a keyword scan of the role text. That scan used bare
substring containment, so "tech" matched inside "technician" and "ai"
inside "maintenance". The atria senior-living reference brief (nurses,
cooks, housekeepers, a Maintenance Technician) therefore "conflicted" as
Technology & Software, and bundle_qa put an industry_client_conflict warning
on a correctly classified healthcare plan. Detector 1 (company name) already
had this word-boundary guard; the fallback now matches it.
"""

from __future__ import annotations

import logging

import pytest

import app
import tools_regen_bundles as regen


@pytest.fixture(autouse=True)
def _quiet():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def test_technician_is_not_a_tech_signal():
    assert (
        app._infer_industry_from_signals(
            "", ["Maintenance Technician"], "healthcare_medical"
        )
        is None
    )


def test_atria_roster_has_no_industry_conflict():
    r = app.classify_industry(
        "Healthcare & Medical", "atria Senior living", regen.ATRIA_ROLE_TITLES
    )
    assert not r.get("industry_conflict"), r.get("industry_conflict")


def test_fuzzy_path_still_detects_real_tech_roles():
    r = app._infer_industry_from_signals(
        "", ["Senior Software Developer", "Cloud Architect"], "healthcare_medical"
    )
    assert r is not None and r.get("legacy_key") == "tech_engineering"
    assert r.get("_role_vote_ratio") is None  # came from the fuzzy scan


@pytest.mark.parametrize(
    "company,roles,selected,expected",
    [
        (
            "Uber",
            ["commercial cab driver"],
            "Hospitality & Travel",
            "logistics_supply_chain",
        ),
        (
            "Acme",
            ["Software Engineer", "Data Scientist", "DevOps Engineer"],
            "Retail & E-Commerce",
            "tech_engineering",
        ),
        (
            "Acme",
            ["Registered Nurse", "ICU Nurse", "Nurse Practitioner"],
            "Technology & Software",
            "healthcare_medical",
        ),
        (
            "Acme",
            ["Warehouse Associate", "Forklift Operator", "Picker Packer"],
            "Healthcare & Medical",
            "logistics_supply_chain",
        ),
    ],
)
def test_real_conflicts_still_fire(company, roles, selected, expected):
    c = app.classify_industry(selected, company, roles).get("industry_conflict")
    assert c and c.get("inferred_legacy_key") == expected, c
