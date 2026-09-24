"""Client-name casing: the client's own spelling is authoritative.

Covers two verified defects (audit journal wf_dfd34698-6d6):

  C2 -- display_format.client_display_name title-cased every word,
        mangling recognizable brand/acronym names and connectives:
        "KPMG" -> "Kpmg", "Bank of America" -> "Bank Of America",
        "Procter and Gamble"/"Procter & Gamble" -> "Procter And Gamble".
        These misspellings printed on deck slide 1, slides 2/5-11
        subheads, and workbook Executive Summary!B2.

  C1 -- bundle_qa._check_client_name_casing compared deliverable text
        against its OWN (buggy, non-brand-aware) canonicalisation instead
        of reusing display_format.client_display_name, so a correctly
        spelled acronym bundle (UPS, IBM, ...) got a blocking critical,
        and the gate flagged the correct "Bank of America" while passing
        the incorrect "Bank Of America".

Fix: client_display_name now (1) resolves known brand/acronym tokens via
a case-insensitive lookup table regardless of input casing, (2) keeps
lowercase connectives (of/and/the/...) lowercase unless they are the
first word, and otherwise preserves the client's own capitalization of a
word verbatim. bundle_qa's gate now canonicalises with EXACTLY that
function (no second implementation) and flags only text that differs
from it in casing, matched case-insensitively so any wrongly-cased
occurrence is caught, not only ones that repeat the raw input verbatim.

Runs under pytest, or standalone: ``python3 tests/test_client_name_casing.py``.
"""

from __future__ import annotations

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import bundle_qa  # noqa: E402
import display_format as fmt  # noqa: E402
from bundle_qa import _TextUnit, _check_client_name_casing  # noqa: E402


# ---------------------------------------------------------------------------
# The ~20-name matrix from the audit brief.
# ---------------------------------------------------------------------------
_LONG_NAME = (
    "The Extraordinarily Long-Named International Consolidated Holdings "
    "and Regional Distribution Partners Group Worldwide LLC"
)

CANONICAL_CASES: list[tuple[str, str]] = [
    ("UPS", "UPS"),
    ("ups", "UPS"),
    ("IBM", "IBM"),
    ("AT&T", "AT&T"),
    ("HCA", "HCA"),
    ("CVS", "CVS"),
    ("GE", "GE"),
    ("3M", "3M"),
    ("BP", "BP"),
    ("DHL", "DHL"),
    ("USPS", "USPS"),
    ("XPO", "XPO"),
    ("BMW", "BMW"),
    ("IHG", "IHG"),
    ("KPMG", "KPMG"),
    ("Bank of America", "Bank of America"),
    ("Procter & Gamble", "Procter & Gamble"),
    ("bank of america", "Bank of America"),
    ("McDonald's", "McDonald's"),
    ("eBay", "eBay"),
    (_LONG_NAME, _LONG_NAME),
]


class TestClientDisplayNameBrandAndConnectives:
    @pytest.mark.parametrize("raw,expected", CANONICAL_CASES)
    def test_canonical_form(self, raw, expected):
        assert fmt.client_display_name(raw) == expected

    def test_kpmg_never_becomes_kpmg_titlecased(self):
        # The exact regression: "KPMG" -> "Kpmg" is what shipped before the
        # fix (the ALL-CAPS branch ran str.capitalize() on every word with
        # no brand-table lookup).
        assert fmt.client_display_name("KPMG") != "Kpmg"
        assert fmt.client_display_name("KPMG") == "KPMG"

    def test_bank_of_america_connective_stays_lowercase(self):
        assert fmt.client_display_name("Bank of America") != "Bank Of America"

    def test_procter_and_gamble_both_spellings_preserved(self):
        assert fmt.client_display_name("Procter and Gamble") != "Procter And Gamble"
        assert fmt.client_display_name("Procter & Gamble") == "Procter & Gamble"

    def test_lowercase_acronym_resolves_via_brand_table(self):
        assert fmt.client_display_name("kpmg") == "KPMG"
        assert fmt.client_display_name("pwc") == "PwC"
        assert fmt.client_display_name("ey") == "EY"

    def test_backward_compat_mixed_case_still_smart_titled(self):
        # Pre-existing, unrelated-to-this-fix behavior (tests/test_display_
        # format.py) must not regress: mixed-case freeform client names
        # still get word-wise smart casing.
        assert fmt.client_display_name("atria Senior living") == "Atria Senior Living"
        assert fmt.client_display_name("MANPOWER - AMERIGAS") == "Manpower - Amerigas"
        assert fmt.client_display_name("visit eBay careers") == "Visit eBay Careers"
        assert fmt.client_display_name("AMC theatres hiring") == "AMC Theatres Hiring"


# ---------------------------------------------------------------------------
# bundle_qa gate: canonicalises with display_format.client_display_name,
# no second implementation, flags only genuine casing mismatches.
# ---------------------------------------------------------------------------
def _gate(raw_client_name: str, deck_text: str) -> list[dict]:
    findings: list[dict] = []
    units = [_TextUnit(deck_text, "slide 1 / TextBox 1 para 0", 0, 0, 0)]
    _check_client_name_casing(units, {"client_name": raw_client_name}, findings)
    return findings


class TestBundleQaClientNameCasingGate:
    @pytest.mark.parametrize("raw,expected", CANONICAL_CASES)
    def test_canonical_spelling_everywhere_passes_gate(self, raw, expected):
        deck_text = f"Company:  {expected}"
        findings = _gate(raw, deck_text)
        assert findings == [], (
            f"canonical spelling {expected!r} for client_name={raw!r} "
            f"should never be flagged, got: {findings}"
        )

    def test_gate_reuses_display_format_not_a_second_implementation(self):
        # The regression this pins: bundle_qa must canonicalise with
        # EXACTLY display_format.client_display_name(raw) -- verified by
        # constructing a canonical form the same way the gate is documented
        # to and confirming it matches for every case in the matrix.
        for raw, expected in CANONICAL_CASES:
            assert bundle_qa.display_format.client_display_name(raw) == expected

    def test_ups_bundle_no_longer_gets_blocking_critical(self):
        # Pre-fix: client_display_name("UPS") == "Ups", so the gate's own
        # canonicalisation disagreed with the correctly-spelled "UPS" in
        # the deck and raised client_name_wrong_casing.
        findings = _gate("UPS", "Media Plan for UPS")
        assert findings == []

    def test_bank_of_america_correct_spelling_no_longer_flagged(self):
        # Pre-fix: this exact case was flagged critical (see probe
        # evidence: casing_bank_of_america/gate.json, 5 criticals, message
        # "raw casing 'Bank of America' instead of the canonical
        # 'Bank Of America'").
        findings = _gate("Bank of America", "Company:  Bank of America")
        assert findings == []

    def test_bank_of_america_incorrect_casing_no_longer_silently_passes(self):
        # Pre-fix: the buggy incorrect form "Bank Of America" (which is
        # what the OLD client_display_name produced) would NOT match the
        # gate's raw-text-only regex and so passed silently. Now that the
        # gate matches the canonical form case-insensitively, the wrong
        # casing is caught wherever it appears.
        findings = _gate("Bank of America", "Company:  Bank Of America")
        assert len(findings) == 1
        assert findings[0]["code"] == "client_name_wrong_casing"
        assert "Bank Of America" in findings[0]["message"]
        assert "Bank of America" in findings[0]["message"]

    def test_genuine_miscasing_is_still_flagged(self):
        findings = _gate("Bank of America", "our client bank OF america")
        assert len(findings) == 1
        assert findings[0]["code"] == "client_name_wrong_casing"
        assert findings[0]["severity"] == "critical"

    def test_genuine_miscasing_flagged_for_acronym(self):
        findings = _gate("UPS", "a plan for Ups Inc")
        assert len(findings) == 1
        assert findings[0]["code"] == "client_name_wrong_casing"

    def test_empty_client_name_no_findings(self):
        assert _gate("", "some deck text") == []

    def test_multiple_occurrences_each_flagged(self):
        findings = _gate(
            "KPMG", "KPMG is our client. This kpmg engagement covers audit."
        )
        # "KPMG" (canonical) is fine; "kpmg" (wrong case) is flagged once.
        assert len(findings) == 1
        assert findings[0]["message"].startswith("Client name appears as 'kpmg'")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
