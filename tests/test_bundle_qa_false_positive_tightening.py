"""Regression tests for two bundle_qa false positives found in the
2026-10-16 26-bundle sweep (ledger/2026-10-16/B_sweep/qa/).

1. ``_check_client_name_casing`` flagged the lowercase client name inside
   a URL / domain / email: "Domain:  www.adp.com" (b_ent_software, slide 8
   and Market Intelligence!D52) and "www.3m.com" (n3_3m_nametrap). A domain
   is not a brand mention; its casing is lowercase by definition.
2. ``_check_industry_client_conflict`` read "Riverbend Community Food Bank"
   (k_tiny_nonprofit) as finance_banking from the token "Bank" and emitted
   a CRITICAL. "Food Bank", "Blood Bank", "Bank Street" and similar
   compounds are not financial institutions.

Every "does not fire" test here fails on the pre-fix code; every
"still fires" test is a positive control proving the fix did not turn the
rule off.

Runs under pytest, or standalone:
``python3 tests/test_bundle_qa_false_positive_tightening.py``.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import bundle_qa  # noqa: E402


# ---------------------------------------------------------------------------
# 1. client_name_wrong_casing must ignore URLs / domains / emails
# ---------------------------------------------------------------------------
def _casing_findings(client: str, *texts: str) -> list[dict]:
    units = [
        bundle_qa._TextUnit(t, f"X!A{i + 1}") for i, t in enumerate(texts)
    ]
    findings: list[dict] = []
    bundle_qa._check_client_name_casing(units, {"client_name": client}, findings)
    return [f for f in findings if f["code"] == "client_name_wrong_casing"]


@pytest.mark.parametrize(
    "client,text",
    [
        # The exact strings from the sweep.
        ("ADP", "Domain:  www.adp.com"),
        ("ADP", "www.adp.com"),
        ("3M", "Domain:  www.3m.com"),
        ("3M", "www.3m.com"),
        # Same defect class: scheme, bare domain, path, email, trailing punct.
        ("ADP", "https://www.adp.com/careers"),
        ("ADP", "http://adp.com"),
        ("ADP", "adp.com"),
        ("ADP", "Visit adp.com/jobs for openings"),
        ("ADP", "Careers site (www.adp.com)."),
        ("ADP", "Apply at https://jobs.adp.com/search?q=analyst"),
        ("ADP", "Write to hr@adp.com"),
        ("3M", "careers@3m.com"),
        # Reserved TLDs the wizard's own example briefs use.
        ("Riverbend Food Bank", "https://www.riverbendfoodbank.example"),
    ],
)
def test_casing_rule_ignores_urls_domains_and_emails(client, text):
    got = _casing_findings(client, text)
    # Riverbend's canonical form has spaces so it can never match inside a
    # domain; it is in the matrix only to pin that nothing odd happens.
    assert not got, f"false positive on {text!r}: {got}"


@pytest.mark.parametrize(
    "client,text,expected_found",
    [
        # Plain-prose wrong casing must still fire.
        ("ADP", "Plan for adp", "adp"),
        ("3M", "3m is hiring", "3m"),
        ("ADP", "adp.", "adp"),  # sentence-final period is not a TLD
        ("ADP", "Prepared for Adp today", "Adp"),
        # A name that merely sits next to a dot-separated word that is not
        # a real TLD is still prose.
        ("ADP", "adp.Next quarter", "adp"),
    ],
)
def test_casing_rule_still_fires_on_plain_prose(client, text, expected_found):
    got = _casing_findings(client, text)
    assert len(got) == 1, got
    assert repr(expected_found) in got[0]["message"]


def test_casing_rule_flags_prose_mention_next_to_a_url():
    """A bundle can carry both a legitimate domain and a genuinely
    mis-cased prose mention. Only the prose mention may be reported."""
    got = _casing_findings("ADP", "adp is hiring - see www.adp.com")
    assert len(got) == 1, got
    assert "'adp'" in got[0]["message"]


def test_casing_rule_canonical_prose_next_to_url_is_clean():
    assert not _casing_findings("ADP", "ADP careers: www.adp.com")


# ---------------------------------------------------------------------------
# 1b. ... and through the real entry point on real (in-memory) artifacts
# ---------------------------------------------------------------------------
def _tiny_pptx(text: str) -> bytes:
    pptx = pytest.importorskip("pptx")
    prs = pptx.Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[5])
    box = slide.shapes.add_textbox(0, 0, 3_000_000, 500_000)
    box.text_frame.text = text
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def _tiny_xlsx(text: str) -> bytes:
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    wb.active.title = "Market Intelligence"
    wb.active["D52"] = text
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_run_bundle_qa_domain_in_deck_and_workbook_is_not_a_casing_finding():
    data = {"client_name": "ADP"}
    findings = bundle_qa.run_bundle_qa(
        _tiny_pptx("Domain:  www.adp.com"), _tiny_xlsx("www.adp.com"), data
    )
    assert not [f for f in findings if f["code"] == "client_name_wrong_casing"]


def test_run_bundle_qa_prose_miscasing_in_workbook_still_fires():
    data = {"client_name": "ADP"}
    findings = bundle_qa.run_bundle_qa(
        None, _tiny_xlsx("Prepared for adp"), data
    )
    assert [f for f in findings if f["code"] == "client_name_wrong_casing"]


# ---------------------------------------------------------------------------
# 2. industry_client_conflict must not read non-financial "Bank" as finance
# ---------------------------------------------------------------------------
def _conflicts(client: str, industry: str, roles=None) -> list[dict]:
    findings: list[dict] = []
    bundle_qa._check_industry_client_conflict(
        {
            "client_name": client,
            "industry": industry,
            "target_roles": roles or [],
        },
        findings,
    )
    return [f for f in findings if f["code"] == "industry_client_conflict"]


def test_riverbend_food_bank_is_not_a_finance_conflict():
    """The exact k_tiny_nonprofit brief: wizard has no nonprofit card, so a
    food bank can only select General / Entry-Level. Pre-fix this was a
    CRITICAL 'implies Financial Services & Insurance' from the token Bank."""
    got = _conflicts(
        "Riverbend Community Food Bank",
        "general_entry_level",
        ["Volunteer Coordinator", "Warehouse Associate"],
    )
    # Nothing may block delivery and nothing may cite finance. (A separate,
    # pre-existing, non-blocking role-title "warn" -- "Warehouse Associate"
    # reads as Transportation & Logistics -- is app.classify_industry's own
    # role-vote behaviour and is out of scope for this client-name fix; it
    # was previously hidden behind the false critical.)
    assert not [f for f in got if f["severity"] == "critical"], got
    assert not [f for f in got if "finance_banking" in f["message"]], got
    assert not [f for f in got if "client_name" in f["message"]], got


@pytest.mark.parametrize(
    "client,industry",
    [
        ("Riverbend Community Food Bank", "general_entry_level"),
        ("Riverbend Community Food Bank", "retail_consumer"),
        ("Regional Food Banks Network", "general_entry_level"),
        ("Riverbend Community Blood Bank", "general_entry_level"),
        ("Riverbend Community Blood Bank", "healthcare_medical"),
        ("Heartland Seed Bank", "general_entry_level"),
        ("Bank Street College of Education", "education"),
        ("Bank Street Hotel", "hospitality_travel"),
        ("Bank St Cafe", "hospitality_travel"),
        ("Bank Road Veterinary Clinic", "healthcare_medical"),
    ],
)
def test_non_financial_bank_compounds_are_not_a_finance_signal(client, industry):
    got = [
        f
        for f in _conflicts(client, industry)
        if "finance_banking" in f["message"]
    ]
    assert not got, f"false finance conflict for {client!r}/{industry}: {got}"


@pytest.mark.parametrize(
    "client",
    [
        "First National Bank",
        "Bank of America",
        "Community Bank",  # a real bank name; "Community" must not exempt it
        "Chase Bank",
        "Riverbend Savings Bank",
        "Food Bank Credit Union",  # real finance signal survives the compound
    ],
)
def test_real_banks_are_still_flagged_critical(client):
    got = _conflicts(client, "retail_consumer")
    assert got, f"real bank {client!r} no longer flagged"
    assert got[0]["severity"] == "critical"
    assert "finance_banking" in got[0]["message"]


def test_neutralizer_is_identity_for_names_without_a_compound():
    """By construction the fix cannot change behaviour for any name that
    does not contain a non-financial bank compound."""
    for name in (
        "Uber",
        "First National Bank",
        "Bank of America",
        "Mercy Health System",
        "",
        "Banking Solutions Inc",
    ):
        assert bundle_qa._neutralize_non_financial_bank(name) == name


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
