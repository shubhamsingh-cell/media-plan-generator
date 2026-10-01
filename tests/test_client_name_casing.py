"""Client-name casing: the client's own spelling is authoritative.

Covers three verified defects:

  F1 -- (independent adversarial review of 55bf330/ec676f5, 2026-09-24)
        client_display_name detected an all-caps input via
        collapsed.isupper() then lowercased every word before re-title-
        casing it, so any all-caps client name NOT in the ~16-entry brand
        table came out wrong: "ADP" -> "Adp", "SAP" -> "Sap",
        "NASA" -> "Nasa", "GM" -> "Gm", "HP" -> "Hp",
        "JPMORGAN CHASE" -> "Jpmorgan Chase". This shipped silently wrong
        to clients because bundle_qa's own casing gate uses this exact
        function as its ground truth, so the gate could never catch its
        own bug. Fixed by inverting the precedence: an all-caps word not
        in the brand table is now preserved verbatim as a likely acronym
        by default (short words), only flattening to Title Case when it
        is long enough to read as raw shouted prose rather than a real
        acronym (see _SHOUT_ACRONYM_MAX_LEN in display_format.py).

  F2 -- (client report, 2026-10-01) the all-caps branch kept every word of
        <= 4 letters verbatim and title-cased every longer word, so FILLER
        words stayed shouted while real words were flattened:
        "THE HERSHEY COMPANY" -> "THE Hershey Company",
        "THE HOME DEPOT" -> "THE HOME Depot",
        "BANK OF AMERICA" -> "BANK of America",
        "MACHINE WORKS INC" -> "Machine Works INC",
        "JPMORGAN CHASE & CO." -> "JPMorgan Chase & CO.".
        It also knew nothing about Mc/O'/L' prefixes ("MCDONALD'S" ->
        "Mcdonald's", "L'OREAL" -> "L'oreal", "O'REILLY" -> "O'reilly"),
        hyphen/digit words ("7-ELEVEN" -> "7-eleven") or corporate suffix
        conventions. The fix is a per-word classifier (see the display_format
        docstring); the EXPLICIT_CASES table below is the independent safety
        net -- it spells out every expected string by hand and never calls
        the function under test to derive one, because bundle_qa's casing
        gate uses client_display_name as its own ground truth and so cannot
        catch this class of bug.

Plus two earlier verified defects (audit journal wf_dfd34698-6d6):

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
import random
import sys
import time

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
    # F1 regression (audit journal wf_dfd34698-6d6): an all-caps client name
    # NOT in the brand table used to get lowercased-then-titlecased instead
    # of preserved as a likely acronym -- "ADP" -> "Adp", "NASA" -> "Nasa".
    ("ADP", "ADP"),
    ("SAP", "SAP"),
    ("NASA", "NASA"),
    ("GM", "GM"),
    ("HP", "HP"),
    ("ADT", "ADT"),
    ("KFC", "KFC"),
    ("CNN", "CNN"),
    # Needs a specific brand-table spelling no length heuristic can derive.
    ("JPMORGAN CHASE", "JPMorgan Chase"),
    ("FEDEX", "FedEx"),
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

    def test_unrecognized_all_caps_acronym_never_lowercased(self):
        # F1 regression (mpg-ship-ec676f5-review-and-client-complaint-triage
        # finding F1): client_display_name detected all-caps input then
        # lowercased every word before re-title-casing it, so any acronym
        # NOT in the ~16-entry brand table came out wrong -- "ADP" -> "Adp",
        # "SAP" -> "Sap", "NASA" -> "Nasa", "GM" -> "Gm", "HP" -> "Hp". This
        # shipped silently wrong to clients because bundle_qa's own casing
        # gate uses this exact function as its ground truth.
        for name in ("ADP", "SAP", "NASA", "GM", "HP", "ADT", "KFC", "CNN"):
            assert fmt.client_display_name(name) == name, (
                f"{name!r} should be preserved verbatim as a likely "
                f"acronym, got {fmt.client_display_name(name)!r}"
            )

    def test_jpmorgan_chase_resolves_via_brand_table(self):
        assert fmt.client_display_name("JPMORGAN CHASE") == "JPMorgan Chase"

    def test_manpower_amerigas_still_flattens_despite_acronym_fix(self):
        # The length-based default that preserves short all-caps acronyms
        # must not regress the pre-existing flattening of long shouted
        # words that are not acronyms.
        assert fmt.client_display_name("MANPOWER - AMERIGAS") == "Manpower - Amerigas"

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

    def test_awp_is_canonical_and_awp_title_cased_is_the_miscasing(self):
        # mpg-content-gate: the gate canonicalised "AWP" to "Awp" and so
        # flagged (and its repair path trusted) the wrong spelling.
        assert _gate("AWP", "Media Plan for AWP | Company:  AWP") == []
        findings = _gate("AWP", "Media Plan for Awp")
        assert len(findings) == 1
        assert findings[0]["code"] == "client_name_wrong_casing"

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


# ---------------------------------------------------------------------------
# F2: EXPLICIT expectation table.
#
# Every expected string below is typed by hand from the documented rules
# (and from how the client writes its own name). NOTHING here is computed by
# calling client_display_name -- that is the whole point.
#
# Rules under test:
#   1. A string that is not purely upper-case is kept as typed (only
#      whitespace is collapsed), except that a word the client wrote in all
#      lowercase is repaired (brand table, corporate-suffix convention,
#      Mc/O' prefix, Title Case, connectives stay lower).
#   2. A purely upper-case string is converted word by word: brand table
#      wins; the/of/and/for... go lower (The at the start); corporate
#      suffixes get their conventional casing; acronym-shaped tokens
#      (known list, or <= 4 letters with no vowel) are kept; Mc/Mac/O'/L'
#      prefixes are handled; everything else is Title Case.
# ---------------------------------------------------------------------------
_EXPLICIT_ALL_CAPS: list[tuple[str, str]] = [
    # -- the reported defect and its siblings --
    ("THE HERSHEY COMPANY", "The Hershey Company"),
    ("THE HOME DEPOT", "The Home Depot"),
    ("BANK OF AMERICA", "Bank of America"),
    ("BANK OF THE WEST", "Bank of the West"),
    ("PROCTER & GAMBLE", "Procter & Gamble"),
    ("JOHNSON AND JOHNSON", "Johnson and Johnson"),
    ("WALMART INC", "Walmart Inc"),
    ("MACHINE WORKS INC", "Machine Works Inc"),
    ("FORD MOTOR COMPANY", "Ford Motor Company"),
    ("TARGET CORPORATION", "Target Corporation"),
    ("GENERAL MOTORS CO", "General Motors Co"),
    ("HERSHEY CO.", "Hershey Co."),
    ("WELLS FARGO & COMPANY", "Wells Fargo & Company"),
    ("NEW YORK LIFE", "New York Life"),
    ("DELTA AIR LINES", "Delta Air Lines"),
    ("MANPOWER", "Manpower"),
    ("MANPOWER - AMERIGAS", "Manpower - Amerigas"),
    # -- Mc / Mac / O' / L' prefixes, without mangling ordinary words --
    ("MCKESSON CORP", "McKesson Corp"),
    ("MCDONALD'S", "McDonald's"),
    ("MCDONALD’S", "McDonald’s"),  # typographic apostrophe, kept as typed
    ("MCKINSEY & COMPANY", "McKinsey & Company"),
    ("MACY'S", "Macy's"),
    ("O'REILLY AUTO PARTS", "O'Reilly Auto Parts"),
    ("L'ORÉAL", "L'Oréal"),
    (
        "MACDONALD, DETTWILER AND ASSOCIATES LTD.",
        "MacDonald, Dettwiler and Associates Ltd.",
    ),
    ("DEUTSCHE BANK AG", "Deutsche Bank AG"),
    # -- apostrophes, ampersands, hyphens, digits, dots, accents --
    ("AT&T", "AT&T"),
    ("AT&T INC.", "AT&T Inc."),
    ("3M", "3M"),
    ("3M COMPANY", "3M Company"),
    ("7-ELEVEN", "7-Eleven"),
    ("COCA-COLA CO", "Coca-Cola Co"),
    ("T-MOBILE US, INC.", "T-Mobile US, Inc."),
    ("ROLLS-ROYCE HOLDINGS PLC", "Rolls-Royce Holdings plc"),
    ("AMAZON.COM, INC.", "Amazon.com, Inc."),
    ("J.P. MORGAN", "J.P. Morgan"),
    ("JPMORGAN CHASE & CO.", "JPMorgan Chase & Co."),
    ("JPMORGAN CHASE", "JPMorgan Chase"),
    ("U.S. BANCORP", "U.S. Bancorp"),
    ("ST. JUDE MEDICAL", "St. Jude Medical"),
    ("21ST CENTURY FOX", "21st Century Fox"),
    ("A&W", "A&W"),
    ("M&T BANK", "M&T Bank"),
    ("L.L.BEAN", "L.L.Bean"),
    ("CHICK-FIL-A", "Chick-fil-A"),
    ("ROCK'N'ROLL INC", "Rock'n'Roll Inc"),
    ("ANHEUSER-BUSCH INBEV SA/NV", "Anheuser-Busch Inbev SA/NV"),
    ("UNITEDHEALTH GROUP INCORPORATED", "UnitedHealth Group Incorporated"),
    ("ZÜRICH INSURANCE GROUP", "Zürich Insurance Group"),
    ("ÉCOLE POLYTECHNIQUE", "École Polytechnique"),
    # -- legal-suffix variants --
    ("PEPSICO, INC.", "PepsiCo, Inc."),
    ("FEDEX CORPORATION", "FedEx Corporation"),
    ("EBAY INC.", "eBay Inc."),
    ("ACME HOLDINGS LLC", "Acme Holdings LLC"),
    ("KPMG LLP", "KPMG LLP"),
    ("BDO USA, LLP", "BDO USA, LLP"),
    ("SIEMENS AG", "Siemens AG"),
    ("SAP SE", "SAP SE"),
    ("VOLKSWAGEN GMBH", "Volkswagen GmbH"),
    ("NESTLÉ S.A.", "Nestlé S.A."),
    ("HEINEKEN N.V.", "Heineken N.V."),
    ("ACME PTY LTD", "Acme Pty Ltd"),
    ("BHP GROUP LIMITED", "BHP Group Limited"),
    ("CVS HEALTH CORPORATION", "CVS Health Corporation"),
    ("HP INC.", "HP Inc."),
    ("IBM CORP.", "IBM Corp."),
    ("US STEEL CORP", "US Steel Corp"),
    ("LG ELECTRONICS", "LG Electronics"),
    ("BNY MELLON", "BNY Mellon"),
    # -- lone acronyms / tickers stay as typed --
    ("ADP", "ADP"),
    ("SAP", "SAP"),
    ("NASA", "NASA"),
    ("GM", "GM"),
    ("HP", "HP"),
    ("IBM", "IBM"),
    ("UPS", "UPS"),
    ("CVS", "CVS"),
    ("KPMG", "KPMG"),
    ("AMD", "AMD"),
    ("USAA", "USAA"),
    ("CDW", "CDW"),
    ("CBRE", "CBRE"),
    ("KFC", "KFC"),
    ("CNN", "CNN"),
    ("IKEA", "IKEA"),
    ("PWC", "PwC"),
    # -- mpg-content-gate (prod client "AWP" printed "Awp" on 15 surfaces):
    #    ONE all-caps token of 2-4 letters is kept as typed, vowels or not --
    ("AWP", "AWP"),
    ("ABC", "ABC"),
    ("BMW", "BMW"),
    ("NBC", "NBC"),
    ("ACE", "ACE"),
    ("TJX", "TJX"),
    ("PNC", "PNC"),
    ("UBS", "UBS"),
    # ...while multi-word / longer shouted names keep per-word casing
    ("BIG LOTS", "Big Lots"),
    ("HOME DEPOT", "Home Depot"),
    ("AWP SAFETY", "Awp Safety"),
]

_EXPLICIT_MIXED: list[tuple[str, str]] = [
    # -- not purely upper-case: preserved exactly as typed --
    ("FORD Motor Company", "FORD Motor Company"),
    ("Walmart INC", "Walmart INC"),
    ("Mckesson CORP", "Mckesson CORP"),
    ("O'reilly AUTO Parts", "O'reilly AUTO Parts"),
    ("THE Hershey Company", "THE Hershey Company"),
    ("Bank Of America", "Bank Of America"),
    ("iPhone Corp", "iPhone Corp"),
    ("eBay", "eBay"),
    ("McKesson Corp", "McKesson Corp"),
    ("O'Reilly Auto Parts", "O'Reilly Auto Parts"),
    ("DHL Supply Chain", "DHL Supply Chain"),
    ("PricewaterhouseCoopers LLP", "PricewaterhouseCoopers LLP"),
    ("Johnson & Johnson", "Johnson & Johnson"),
    ("Procter and Gamble", "Procter and Gamble"),
    # -- a word the client typed in all lowercase is still repaired --
    ("atria Senior living", "Atria Senior Living"),
    ("bank of america", "Bank of America"),
    ("hershey company", "Hershey Company"),
    ("mckesson corp", "McKesson Corp"),
    ("Hershey Company, inc.", "Hershey Company, Inc."),
    ("kpmg", "KPMG"),
    ("ups", "UPS"),
]

_EXPLICIT_EDGE: list[tuple[object, str]] = [
    (None, ""),
    ("", ""),
    ("   ", ""),
    ("\t\n ", ""),
    (123, ""),
    ("  THE   HERSHEY\tCOMPANY \n", "The Hershey Company"),
    ("  Walmart   INC  ", "Walmart INC"),
    # decomposed accent (NFD) must still compose to one letter and cap right
    ("L'ORE\u0301AL", "L'Oréal"),
    # uncased scripts pass through untouched
    ("日本電産", "日本電産"),
    # 200-character inputs (app.py rejects client_name > 200): upper and mixed
    (("ACME " * 40).strip(), ("Acme " * 40).strip()),
    (("Acme Corp " * 20).strip(), ("Acme Corp " * 20).strip()),
]

EXPLICIT_CASES: list[tuple[object, str]] = (
    list(_EXPLICIT_ALL_CAPS) + list(_EXPLICIT_MIXED) + list(_EXPLICIT_EDGE)
)


class TestExplicitExpectationTable:
    def test_table_is_large_enough(self):
        # The brief asks for >= 60 independent cases.
        assert len(EXPLICIT_CASES) >= 60

    @pytest.mark.parametrize(
        "raw,expected", _EXPLICIT_ALL_CAPS, ids=[c[0] for c in _EXPLICIT_ALL_CAPS]
    )
    def test_all_caps_input(self, raw, expected):
        assert fmt.client_display_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected", _EXPLICIT_MIXED, ids=[c[0] for c in _EXPLICIT_MIXED]
    )
    def test_mixed_case_input(self, raw, expected):
        assert fmt.client_display_name(raw) == expected

    @pytest.mark.parametrize(
        "raw,expected", _EXPLICIT_EDGE, ids=[repr(c[0])[:30] for c in _EXPLICIT_EDGE]
    )
    def test_edge_input(self, raw, expected):
        assert fmt.client_display_name(raw) == expected


class TestClientNameInvariants:
    def test_idempotent_over_table_inputs_and_outputs(self):
        # f(f(x)) == f(x): app.py, ppt_generator, excel_v2 and bundle_qa all
        # re-apply the function to a name that was already normalised.
        for raw, expected in EXPLICIT_CASES:
            once = fmt.client_display_name(raw)
            assert fmt.client_display_name(once) == once, (raw, once)
            # the hand-written expectation is itself a fixed point
            assert fmt.client_display_name(expected) == expected, (raw, expected)

    @pytest.mark.parametrize(
        "raw",
        [
            # repairing the lowercase words leaves an all-caps string, which a
            # second pass used to re-case as shouted data (found by fuzzing)
            "THE HERSHEY llc",
            "OF a.b. MCKESSON PLC a.b. OF",
            # capitalising a letter next to a combining mark (found by fuzzing)
            "ß\u0301 X",
            # Turkish dotted capital I lowercases to i + a combining dot
            "日CİOS",
        ],
    )
    def test_idempotent_on_fuzz_found_shapes(self, raw):
        once = fmt.client_display_name(raw)
        assert fmt.client_display_name(once) == once

    def test_repair_that_leaves_only_capitals_is_settled_as_shouted_data(self):
        assert fmt.client_display_name("THE HERSHEY llc") == "The Hershey LLC"

    def test_idempotent_and_never_raises_on_random_text(self):
        rng = random.Random(20261001)
        alphabet = (
            list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            + list("abcdefghijklmnopqrstuvwxyz")
            + list("0123456789")
            + list(" &'-.,/()_+!@#")
            + [
                "’",
                "É",
                "é",
                "ß",
                "İ",
                "ǅ",
                "Ω",
                "日",
                "\u0301",
                "\u200b",
                "\u00a0",
                "\n",
                "\t",
                "\x00",
                "\ud800",
                "🙂",
            ]
        )
        words = [
            "THE",
            "OF",
            "INC",
            "CO.",
            "MC",
            "MCKESSON",
            "O'",
            "AT&T",
            "3M",
            "of",
            "and",
            "corp",
            "ltd",
            "PLC",
            "eBay",
            "FORD",
            "a.b.",
        ]
        for _ in range(4000):
            if rng.random() < 0.5:
                raw = "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 40)))
            else:
                raw = " ".join(rng.choice(words) for _ in range(rng.randint(1, 8)))
            once = fmt.client_display_name(raw)
            assert isinstance(once, str)
            assert fmt.client_display_name(once) == once, (raw, once)

    def test_output_has_no_repeated_or_edge_whitespace(self):
        for raw in ("  A  B ", "THE\t\tHOME\nDEPOT", "x\u00a0\u00a0y"):
            out = fmt.client_display_name(raw)
            assert out == out.strip()
            assert "  " not in out

    def test_very_long_input_passes_through_sanely_and_fast(self):
        long_upper = "THE HERSHEY COMPANY " * 5000  # 100k chars
        start = time.monotonic()
        out = fmt.client_display_name(long_upper)
        elapsed = time.monotonic() - start
        assert elapsed < 2.0, f"took {elapsed:.2f}s"
        # beyond the bound the name is returned whitespace-collapsed, uncased
        assert out == long_upper.strip()
        assert fmt.client_display_name(out) == out

    def test_adversarial_punctuation_is_linear(self):
        # alternating word/non-word characters in one token is the shape
        # that makes an edge-stripping regex go quadratic
        raw = "A!" * 400
        start = time.monotonic()
        fmt.client_display_name(raw)
        assert time.monotonic() - start < 1.0


class TestCallSitesAgreeOnTheFixedName:
    """ppt_generator, excel_v2 and bundle_qa must all produce/accept the same
    corrected name (they each call the shared function)."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("THE HERSHEY COMPANY", "The Hershey Company"),
            ("JPMORGAN CHASE & CO.", "JPMorgan Chase & Co."),
            ("MCKESSON CORP", "McKesson Corp"),
        ],
    )
    def test_deck_and_workbook_helpers_agree(self, raw, expected):
        import excel_v2
        import ppt_generator

        assert ppt_generator._proper_client_name(raw) == expected
        assert excel_v2._proper_client_name(raw) == expected

    def test_gate_accepts_corrected_name(self):
        assert _gate("THE HERSHEY COMPANY", "Media plan for The Hershey Company") == []

    def test_gate_flags_the_old_half_shouted_name(self):
        findings = _gate("THE HERSHEY COMPANY", "Media plan for THE Hershey Company")
        assert len(findings) == 1
        assert findings[0]["code"] == "client_name_wrong_casing"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
