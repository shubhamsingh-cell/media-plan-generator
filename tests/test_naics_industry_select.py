"""Tests for the NAICS 2022 industry typeahead (S94, Jesse Ofner feedback).

The wizard's Industry step only offered the 21 curated internal industry
keys (shared_utils.INDUSTRY_LABEL_MAP), which the whole benchmark stack
keys off. This adds a searchable typeahead over the full US NAICS 2022
index (data/naics_2022.json, 2,125 codes) WITHOUT changing those 21 keys.

Covers:
    1. Data-contract invariant -- every code in data/naics_2022.json
       resolves (via naics_lookup.resolve_internal_key) to a key that
       exists in shared_utils.INDUSTRY_LABEL_MAP. This is the guarantee
       the whole feature depends on: a NAICS pick must always map to a
       working internal key, never an orphan.
    2. naics_search() ranking -- numeric code-prefix match, multi-token
       text match, no-match, and the `limit` cap.
    3. format_industry_label() -- the single-source label suffix helper
       used by both gen_data paths in app.py and routes/export.py, incl.
       the empty-string-safe "missing naics -> unchanged label" contract.
    4. /api/naics/search live HTTP smoke test (200 shape + 400 on missing
       q), using the same in-process app.ThreadedHTTPServer fixture
       pattern as tests/test_api_estimate.py / test_generate_concurrency.py.
    5. Plan-payload round-trip -- plan_schema.PlanData preserves
       naics_selected_code/title (via its non-lossy `extra` dict) when
       present, and plans without them behave identically to today.
    6. Typeahead affordances (design panel 2026-07-31, iteration 3;
       retreated from warning to the INFO affordance + Escape made a
       full search-abandon at the 2026-07-31 final gate; restructured
       2026-07-31 cont'd to fix dropdown occlusion of the chip/notice)
       -- template/string assertions pinning the chip/notice's shared
       superseded dimming while a query is in flight, the substitution
       notice's info/teal styling and category-level copy, Escape's
       query-and-filter abandon behavior, the usage-hint/live-region
       a11y pair, and the chip+notice-precede-the-input DOM order that
       keeps the dropdown from ever covering them. Includes a regression
       guard that the .naics-filtered-out final-gate fix (see class 1's
       sibling comment) is still the last .naics- rule in
       head_styles.html.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Iterator

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402
import naics_lookup  # noqa: E402
from shared_utils import INDUSTRY_LABEL_MAP, format_industry_label  # noqa: E402
from plan_schema import PlanData  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════
# 1. Data-contract invariant
# ═══════════════════════════════════════════════════════════════════════


class TestEveryCodeResolves:
    def test_dataset_loaded(self) -> None:
        assert naics_lookup.is_loaded(), "data/naics_2022.json failed to load"
        assert len(naics_lookup._CODES) > 2000

    def test_every_code_resolves_to_a_valid_internal_key(self) -> None:
        bad = []
        for c in naics_lookup._CODES:
            key = naics_lookup.resolve_internal_key(c["code"])
            if key not in INDUSTRY_LABEL_MAP:
                bad.append((c["code"], key))
        assert (
            not bad
        ), f"{len(bad)} NAICS codes resolved to an unmapped key: {bad[:10]}"

    def test_default_internal_key_is_itself_valid(self) -> None:
        assert naics_lookup._DEFAULT_INTERNAL_KEY in INDUSTRY_LABEL_MAP

    def test_ranged_sector_code_resolves(self) -> None:
        # "31-33" (Manufacturing) must strip to "31" before prefix matching.
        rec = naics_lookup.naics_lookup("31-33")
        assert rec is not None
        assert rec["internal_key"] in INDUSTRY_LABEL_MAP

    def test_unknown_code_falls_back_to_default(self) -> None:
        assert (
            naics_lookup.resolve_internal_key("999999")
            == naics_lookup._DEFAULT_INTERNAL_KEY
        )

    def test_empty_code_falls_back_to_default(self) -> None:
        assert (
            naics_lookup.resolve_internal_key("") == naics_lookup._DEFAULT_INTERNAL_KEY
        )
        assert (
            naics_lookup.resolve_internal_key(None)
            == naics_lookup._DEFAULT_INTERNAL_KEY
        )


# ═══════════════════════════════════════════════════════════════════════
# 2. naics_search() ranking
# ═══════════════════════════════════════════════════════════════════════


class TestNaicsSearch:
    def test_numeric_exact_code_match(self) -> None:
        results = naics_lookup.naics_search("621330")
        assert results
        assert results[0]["code"] == "621330"
        assert results[0]["internal_key"] == "mental_health"

    def test_numeric_prefix_match(self) -> None:
        results = naics_lookup.naics_search("5411")
        assert results
        codes = [r["code"] for r in results]
        assert any(c.startswith("5411") for c in codes)

    def test_multi_token_text_match_all_tokens_required(self) -> None:
        results = naics_lookup.naics_search("law office")
        assert results
        for r in results:
            title_lower = r["title"].lower()
            assert "law" in title_lower or "lawyer" in title_lower
            assert "office" in title_lower

    def test_text_match_returns_internal_key_and_level(self) -> None:
        results = naics_lookup.naics_search("mental health")
        assert results
        for r in results:
            assert set(r.keys()) == {"code", "title", "level", "internal_key"}
            assert r["internal_key"] in INDUSTRY_LABEL_MAP

    def test_no_match_returns_empty(self) -> None:
        assert naics_lookup.naics_search("zzznotarealindustryxyz") == []

    def test_empty_query_returns_empty(self) -> None:
        assert naics_lookup.naics_search("") == []
        assert naics_lookup.naics_search(None) == []

    def test_limit_is_respected(self) -> None:
        results = naics_lookup.naics_search("office", limit=3)
        assert len(results) <= 3

    def test_limit_default_caps_at_reasonable_size(self) -> None:
        results = naics_lookup.naics_search("services", limit=500)
        # search() itself clamps an oversized limit request
        assert len(results) <= 50

    def test_exact_code_ranks_above_prefix(self) -> None:
        # "5411" should surface the exact-length code before longer
        # prefix-only matches when both are present.
        results = naics_lookup.naics_search("54")
        assert results[0]["code"] == "54"  # exact sector code, top rank

    # ── Design panel 2026-07-31, iteration 2 (mechanism, accepted defect) ──
    # A parent code that aggregates a single child duplicates that child's
    # title verbatim (92213 "Legal Counsel and Prosecution" == 922130's
    # title; 54111 "Offices of Lawyers" == 541110's). Showing both rows in
    # a typeahead is pure noise -- only the deepest row should survive.

    def test_legal_counsel_query_has_no_duplicate_titles(self) -> None:
        results = naics_lookup.naics_search("legal counsel")
        titles = [r["title"] for r in results]
        assert len(titles) == len(set(titles)), f"duplicate titles: {titles}"

    def test_law_office_query_has_no_duplicate_titles(self) -> None:
        results = naics_lookup.naics_search("law office")
        titles = [r["title"] for r in results]
        assert len(titles) == len(set(titles)), f"duplicate titles: {titles}"

    def test_dedupe_keeps_the_deepest_row(self) -> None:
        # 922130 (level 6) and 92213 (level 5) share a title; the search
        # result for the pair must resolve to the 6-digit code.
        results = naics_lookup.naics_search("legal counsel")
        matches = [r for r in results if r["title"] == "Legal Counsel and Prosecution"]
        assert len(matches) == 1
        assert matches[0]["code"] == "922130"
        assert matches[0]["level"] == 6

        results2 = naics_lookup.naics_search("law office")
        matches2 = [r for r in results2 if r["title"] == "Offices of Lawyers"]
        assert len(matches2) == 1
        assert matches2[0]["code"] == "541110"
        assert matches2[0]["level"] == 6

    def test_dedupe_does_not_remove_genuinely_distinct_titles(self) -> None:
        # Sanity check the dedupe is title-scoped, not a blunt cap --
        # a broad query should still return multiple distinct titles.
        results = naics_lookup.naics_search("services", limit=20)
        titles = {r["title"] for r in results}
        assert len(titles) > 1

    # ── Design panel 2026-07-31, mechanism lens VETO (fixed 2026-08-02) ──
    # q=nurse returned SIX horticulture codes and ZERO healthcare ones,
    # because plain substring matching makes "nurse" a prefix of "nursery"
    # and the horticulture cluster outranked "nursing". The wizard ships an
    # example chip reading "Nurse in LA" and the typeahead tells users --
    # including screen-reader users who cannot see that the rows say
    # "Floriculture" -- to "pick a result to attach its NAICS code", so the
    # product actively directed people to a result set that was wrong on
    # its own flagship query.

    def test_nurse_returns_healthcare_not_horticulture(self) -> None:
        """The reported defect: q=nurse must surface nursing, not nurseries."""
        results = naics_lookup.naics_search("nurse", limit=6)
        assert results, "q=nurse must not be empty"

        top = results[0]
        assert top["internal_key"] in {"healthcare_medical", "mental_health"}, (
            f"top result for q=nurse is {top['code']} {top['title']!r} "
            f"({top['internal_key']}) -- expected a healthcare industry"
        )
        assert "nursing" in top["title"].lower(), (
            f"top result for q=nurse is {top['title']!r} -- expected a "
            "nursing industry"
        )

        # The specific six horticulture codes from the 2026-08-02 report
        # must no longer occupy the whole result set.
        horticulture = {"111421", "444240", "11142", "113210", "424930", "1114"}
        returned = {r["code"] for r in results}
        assert (
            not returned <= horticulture
        ), f"q=nurse still returns only horticulture codes: {returned}"

    def test_nurse_phrase_queries_are_not_empty(self) -> None:
        """q=registered nurse / nurse practitioner returned nothing at all."""
        for query in ("registered nurse", "nurse practitioner", "rn", "cna"):
            results = naics_lookup.naics_search(query, limit=5)
            assert results, f"q={query!r} returned no results"
            assert results[0]["internal_key"] in {
                "healthcare_medical",
                "mental_health",
            }, (
                f"q={query!r} top result is {results[0]['title']!r} "
                f"({results[0]['internal_key']}) -- expected healthcare"
            )

    def test_nursing_spelling_still_works(self) -> None:
        """q=nursing was the one spelling that already worked -- keep it."""
        results = naics_lookup.naics_search("nursing", limit=5)
        assert results
        assert results[0]["code"] == "623110"
        assert results[0]["internal_key"] == "healthcare_medical"

    def test_mid_word_substring_is_not_a_match(self) -> None:
        """q=rn used to return Corn Farming / Furniture Retailers.

        "rn" appears mid-word in "Corn", "Furniture" and "International",
        which is meaningless -- a match must land on a word boundary.
        """
        results = naics_lookup.naics_search("rn", limit=10)
        for r in results:
            words = r["title"].lower().replace(",", " ").split()
            assert any(w.startswith("rn") for w in words) or r["internal_key"] in {
                "healthcare_medical",
                "mental_health",
            }, f"q=rn returned {r['title']!r} on a mid-word substring"
        titles = " | ".join(r["title"] for r in results)
        for junk in ("Corn Farming", "Furniture Retailers", "International Affairs"):
            assert junk not in titles, f"q=rn still returns {junk!r}"

    def test_title_prefix_match_must_end_on_a_word_boundary(self) -> None:
        """ "Nursery..." starts with "nurse", but mid-word -- not a prefix hit.

        This is the specific scoring rule that let the horticulture cluster
        claim the top tier for q=nurse.
        """
        assert naics_lookup._starts_on_word_boundary(
            "nursing care facilities", "nursing"
        )
        assert not naics_lookup._starts_on_word_boundary(
            "nursery and tree production", "nurse"
        )

    def test_whole_word_match_outranks_word_prefix_match(self) -> None:
        """q=air must rank "Air Transportation" above "Aircraft ...".

        The generic form of the nurse/nursery defect: a title where the
        query is a whole word must beat one where it is only the start of
        a longer word. Pre-fix, "Aircraft Manufacturing" landed at index 1
        and "Air Transportation" at index 6.
        """
        titles = [r["title"] for r in naics_lookup.naics_search("air", limit=50)]
        air_transport = titles.index("Air Transportation")
        first_aircraft = next(i for i, t in enumerate(titles) if "Aircraft" in t)
        assert air_transport < first_aircraft, (
            f"'Air Transportation' (whole word, idx {air_transport}) must "
            f"outrank 'Aircraft...' (word prefix, idx {first_aircraft})"
        )

    def test_occupation_queries_return_the_industry_that_employs_them(
        self,
    ) -> None:
        """Occupations the corpus contains no word for at all.

        No NAICS title contains "driver", "cashier" or "forklift" -- these
        returned nothing before the occupation alias layer existed.
        """
        expectations = {
            "driver": "logistics_supply_chain",
            "truck driver": "logistics_supply_chain",
            "cdl": "logistics_supply_chain",
            "forklift": "logistics_supply_chain",
            "line cook": "food_beverage",
            "teacher": "education",
            # 6244 Child Care Services maps to education in
            # internal_key_map, not healthcare.
            "daycare": "education",
        }
        for query, expected_key in expectations.items():
            results = naics_lookup.naics_search(query, limit=5)
            assert results, f"q={query!r} returned no results"
            assert results[0]["internal_key"] == expected_key, (
                f"q={query!r} -> {results[0]['code']} {results[0]['title']!r} "
                f"({results[0]['internal_key']}), expected {expected_key}"
            )

    def test_numeric_prefix_search_requires_an_all_digit_query(self) -> None:
        """A stray digit must not turn a text query into a code search.

        Pre-fix, the code path triggered on ANY digit anywhere in the
        query, so "top 5 legal" stripped to "5" and prefix-matched every
        code starting with 5 -- returning Locksmiths, Credit Unions and
        Pension Funds, none of which contain any query word.
        """
        results = naics_lookup.naics_search("top 5 legal", limit=10)
        for r in results:
            assert not (
                r["code"].startswith("5") and "legal" not in r["title"].lower()
            ), f"digit fragment leaked into a code-prefix search: {r}"
        titles = [r["title"] for r in results]
        for junk in ("Locksmiths", "Credit Unions", "Pension Funds"):
            assert junk not in titles, f"q='top 5 legal' still returns {junk!r}"

    # ── Ranged sector codes (defect found 2026-08-02) ──────────────────
    # Three sectors are stored with a hyphen: "31-33" Manufacturing,
    # "44-45" Retail Trade, "48-49" Transportation and Warehousing.
    # naics_search reduced every code query to its digits, so "31-33"
    # became "3133" and matched the unrelated 4-digit Textile Mills code
    # while never reaching Manufacturing, and "44-45"/"48-49" matched
    # nothing at all. A 2-digit query was broken the other way: it did
    # reach the sector row by prefix, but the -level tie-break sorts a
    # level-2 sector below every one of its level-6 descendants, so the
    # sector came LAST -- off the end of a 20-row typeahead -- while
    # "54", an un-ranged sector, came first.

    RANGED_SECTORS = [
        ("31-33", "Manufacturing", ("31", "32", "33")),
        ("44-45", "Retail Trade", ("44", "45")),
        ("48-49", "Transportation and Warehousing", ("48", "49")),
    ]

    def test_ranged_sector_code_is_the_top_exact_match(self) -> None:
        for code, title, _members in self.RANGED_SECTORS:
            results = naics_lookup.naics_search(code)
            assert results, f"{code} returned no matches"
            assert results[0]["code"] == code, f"{code} -> {results[0]['code']}"
            assert results[0]["title"] == title
            assert results[0]["level"] == 2

    def test_ranged_query_never_collapses_into_a_concatenated_code(self) -> None:
        # "31-33" must not be read as "3133" (Textile and Fabric
        # Finishing and Fabric Coating Mills) -- a different industry.
        codes = [r["code"] for r in naics_lookup.naics_search("31-33", limit=50)]
        assert "3133" not in codes
        assert not any(c.startswith("3133") for c in codes)

    def test_concatenated_code_still_resolves_to_itself(self) -> None:
        # The converse guard: "3133" is a real code and must keep
        # matching Textile Mills, not the Manufacturing sector.
        results = naics_lookup.naics_search("3133")
        assert results[0]["code"] == "3133"
        assert "31-33" not in [r["code"] for r in results]

    def test_any_member_of_a_range_surfaces_its_sector_first(self) -> None:
        # Typing "33" or "45" means "the sector that owns these codes" --
        # the same contract "54" already had for the un-ranged sectors.
        for code, title, members in self.RANGED_SECTORS:
            for member in members:
                results = naics_lookup.naics_search(member)
                assert results, f"{member} returned no matches"
                assert results[0]["code"] == code, f"{member} -> {results[0]['code']}"
                assert results[0]["title"] == title

    def test_sector_row_does_not_crowd_out_its_descendants(self) -> None:
        # The sector is added at the top, not substituted for the deep
        # codes that used to fill the list.
        results = naics_lookup.naics_search("31", limit=10)
        assert results[0]["code"] == "31-33"
        rest = [r["code"] for r in results[1:]]
        assert len(rest) >= 5
        assert all(c.startswith("31") for c in rest)

    def test_partially_typed_range_matches_only_the_sector(self) -> None:
        # Typeahead sends every keystroke. Once an internal hyphen shows
        # up the query commits to the range reading for good -- "31-3"
        # is mid-way through "31-33" and must not fall back to a
        # digits-only reading ("313" Textile Mills, an unrelated code
        # family), even though "31-3" briefly looks ambiguous.
        #
        # This is a deliberate call, not an oversight: re-admitting the
        # digits-only reading once a hyphen is present is not free.
        # Digit-stripping the *complete* query "31-33" also produces
        # "3133", a real code -- so any rule that blends the two
        # readings back together either resurrects the exact bug this
        # module fixes for the complete-range query, or has to
        # special-case "complete" vs. "partial" range to avoid it. Both
        # cost more than the one keystroke of typeahead gap this trades
        # away, since the very next keystroke resolves it anyway.
        partials = {
            "31-3": "31-33",
            "44-4": "44-45",
            "48-4": "48-49",
        }
        for partial, sector in partials.items():
            codes = [r["code"] for r in naics_lookup.naics_search(partial)]
            assert codes == [sector], f"{partial!r} -> {codes}, expected [{sector!r}]"

    def test_range_query_tolerates_spacing_and_unicode_dashes(self) -> None:
        for variant in ("31 - 33", "31\u201333", "31\u201433", " 31-33 "):
            results = naics_lookup.naics_search(variant)
            assert results, f"{variant!r} returned no matches"
            assert results[0]["code"] == "31-33", f"{variant!r} -> {results[0]}"

    def test_ranged_sector_results_resolve_to_a_valid_internal_key(self) -> None:
        # Same data-contract invariant as class 1, asserted on the rows
        # the wizard actually receives for a ranged-sector query.
        for code, _title, members in self.RANGED_SECTORS:
            for query in (code,) + members:
                for r in naics_lookup.naics_search(query, limit=20):
                    assert r["internal_key"] in INDUSTRY_LABEL_MAP, (query, r)

    def test_hyphenated_title_query_is_unaffected(self) -> None:
        # A hyphen only means "range" for codes -- hyphenated *titles*
        # must keep matching by text.
        results = naics_lookup.naics_search("full-service")
        assert results
        assert results[0]["code"] == "722511"
        assert results[0]["title"] == "Full-Service Restaurants"

    def test_code_match_keys_expands_only_well_formed_ranges(self) -> None:
        # The expansion helper is defensive: a plain code stands only for
        # itself, and a malformed/absurd range degrades to its endpoints
        # instead of raising or exploding at import.
        assert naics_lookup._code_match_keys("541110") == ("541110",)
        assert naics_lookup._code_match_keys("31-33") == ("31-33", "31", "32", "33")
        assert naics_lookup._code_match_keys("31-3a") == ("31-3a", "31")
        assert naics_lookup._code_match_keys("33-31") == ("33-31", "33", "31")
        assert naics_lookup._code_match_keys("10-999999") == ("10-999999", "10")
        assert naics_lookup._code_match_keys("10-99") == ("10-99", "10", "99")


# ═══════════════════════════════════════════════════════════════════════
# 2c. Occupation -> industry alias map integrity (added 2026-08-02)
# ═══════════════════════════════════════════════════════════════════════


class TestOccupationAliasMap:
    """The alias map is hand-curated, so it can rot silently.

    NAICS retitles codes between vintages -- "Child Day Care Services"
    became "Child Care Services" in the 2022 vintage, and that stale phrase
    silently sent q=daycare to "Elementary and Secondary Schools" during
    development. These tests make a stale or invented industry phrase fail
    the suite instead of shipping a wrong (or empty) typeahead.
    """

    def test_every_expansion_phrase_matches_a_real_naics_title(self) -> None:
        dead = []
        for phrases, terms in naics_lookup._ALIAS_GROUPS:
            for phrase in phrases:
                tokens = naics_lookup._words(phrase)
                hit = any(
                    naics_lookup._phrase_strength(tokens, words, words_set)
                    for _c, _t, _tl, words, words_set, _l in naics_lookup._SEARCH_ROWS
                )
                if not hit:
                    dead.append((phrase, terms[0]))
        assert not dead, (
            "alias expansion phrases matching no NAICS 2022 title "
            f"(stale or invented): {dead}"
        )

    def test_every_alias_key_returns_results(self) -> None:
        empty = [
            key
            for key in naics_lookup._ALIAS_INDEX
            if not naics_lookup.naics_search(key, limit=3)
        ]
        assert not empty, f"alias keys returning an empty typeahead: {empty}"

    def test_every_alias_result_resolves_to_a_valid_internal_key(self) -> None:
        """The data-contract invariant must hold through the alias path too."""
        for key in naics_lookup._ALIAS_INDEX:
            for r in naics_lookup.naics_search(key, limit=10):
                assert r["internal_key"] in INDUSTRY_LABEL_MAP, (
                    f"q={key!r} -> {r['code']} resolved to orphan key "
                    f"{r['internal_key']!r}"
                )

    def test_multi_word_alias_beats_the_union_of_its_tokens(self) -> None:
        """ "truck driver" uses the trucking expansion, not truck+driver."""
        results = naics_lookup.naics_search("truck driver", limit=3)
        assert results
        assert (
            "trucking" in results[0]["title"].lower()
        ), f"q='truck driver' -> {results[0]['title']!r}"
        # ...and must not surface truck MANUFACTURING, which hires no drivers.
        titles = " | ".join(r["title"] for r in results)
        assert "Manufacturing" not in titles

    def test_aliases_do_not_suppress_the_literal_matches(self) -> None:
        """Nothing is hidden -- horticulture still appears for q=nurse.

        The fix is a ranking fix, not a filter: an intent heuristic that
        dropped the agriculture cluster would be wrong for a user who
        really did mean nurseries.
        """
        results = naics_lookup.naics_search("nurse", limit=50)
        titles = [r["title"] for r in results]
        assert any(
            "Nursery" in t for t in titles
        ), "horticulture rows should still be reachable, just ranked below"
        nursing_idx = next(i for i, t in enumerate(titles) if "Nursing" in t)
        nursery_idx = next(i for i, t in enumerate(titles) if "Nursery" in t)
        assert nursing_idx < nursery_idx


# ═══════════════════════════════════════════════════════════════════════
# 2b. Mapping fix: 922130/92213 "Legal Counsel and Prosecution" (design
# panel 2026-07-31, iteration 2, accepted defect -- used to fall through
# the "92" catch-all to general_entry_level instead of legal_services)
# ═══════════════════════════════════════════════════════════════════════


class TestLegalCounselMapping:
    def test_922130_resolves_to_legal_services(self) -> None:
        assert naics_lookup.resolve_internal_key("922130") == "legal_services"

    def test_92213_resolves_to_legal_services(self) -> None:
        assert naics_lookup.resolve_internal_key("92213") == "legal_services"

    def test_92211_courts_resolves_to_legal_services(self) -> None:
        assert naics_lookup.resolve_internal_key("92211") == "legal_services"

    def test_unrelated_92_sibling_still_falls_back_to_default(self) -> None:
        # Only 92211/92213 were added -- a sibling under "922" with no
        # specific override (e.g. a made-up/unmapped code under the
        # "92" public-administration catch-all) must still fall back to
        # default_internal_key, proving the fix is scoped, not a blanket
        # "92" -> legal_services change.
        assert (
            naics_lookup.resolve_internal_key("9229")
            == naics_lookup._DEFAULT_INTERNAL_KEY
        )


# ═══════════════════════════════════════════════════════════════════════
# 3. format_industry_label()
# ═══════════════════════════════════════════════════════════════════════


class TestFormatIndustryLabel:
    def test_no_naics_returns_label_unchanged(self) -> None:
        assert format_industry_label("Legal Services") == "Legal Services"
        assert format_industry_label("Legal Services", "", "") == "Legal Services"
        assert format_industry_label("Legal Services", None, None) == "Legal Services"

    def test_naics_code_and_title_appended(self) -> None:
        out = format_industry_label("Legal Services", "5411", "Law Offices")
        assert out == "Legal Services · NAICS 5411 — Law Offices"

    def test_naics_code_only_no_title(self) -> None:
        out = format_industry_label("Legal Services", "5411", "")
        assert out == "Legal Services · NAICS 5411"

    def test_empty_label_with_naics_still_produces_suffix(self) -> None:
        out = format_industry_label("", "5411", "Law Offices")
        assert out == "NAICS 5411 — Law Offices"

    def test_whitespace_only_naics_code_is_treated_as_absent(self) -> None:
        assert (
            format_industry_label("Legal Services", "   ", "Title") == "Legal Services"
        )

    def test_block_comment_does_not_falsely_claim_excel_picks_up_suffix(
        self,
    ) -> None:
        """The block comment directly above format_industry_label()
        (shared_utils.py) used to read "...so every deliverable that
        reads data["industry_label"] (Excel, PPTX, PDF/HTML report, Nova
        chat context) picks up the precise NAICS automatically" -- which
        is false for Excel: excel_v2.py:2075 _get_industry_label()
        re-derives the industry label from the industry KEY via
        INDUSTRY_LABEL_MAP and never reads data["industry_label"], so the
        suffix this function appends never reaches the workbook (callers
        at excel_v2.py:4461, 5749, 6295, 11077). That false claim was
        copied into a user-facing substitution notice this session and
        had to be retracted -- this pins the comment so the mistake can't
        silently come back. Scoped to the comment block itself (between
        its opening line and the `def` it documents), not the whole file,
        because shared_utils.py mentions "excel_v2.py" elsewhere
        (unrelated docstrings) for reasons that have nothing to do with
        this claim -- an unscoped check would pass without the fix."""
        source = (PROJECT_ROOT / "shared_utils.py").read_text(encoding="utf-8")
        comment_start = source.index(
            "# format_industry_label() is the single place that appends"
        )
        comment_end = source.index("\ndef format_industry_label(", comment_start)
        comment_block = source[comment_start:comment_end]

        assert (
            "(Excel, PPTX, PDF/HTML report, Nova chat context) picks up "
            "the precise NAICS"
        ) not in comment_block, (
            "the false Excel claim must not be re-seeded into this comment"
        )
        assert "excel_v2.py" in comment_block, (
            "the comment must document the Excel exclusion by name "
            "(excel_v2.py), not just silently drop the old false claim"
        )


# ═══════════════════════════════════════════════════════════════════════
# 4. /api/naics/search live HTTP smoke test
# ═══════════════════════════════════════════════════════════════════════


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-naics-http-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("live_server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


def _http_get(port: int, path: str, timeout: float = 5.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, body
    finally:
        conn.close()


class TestNaicsSearchEndpointLive:
    def test_missing_q_returns_400(self, live_server: int) -> None:
        status, body = _http_get(live_server, "/api/naics/search")
        assert status == 400
        data = json.loads(body)
        assert "error" in data

    def test_valid_query_returns_200_with_expected_shape(
        self, live_server: int
    ) -> None:
        status, body = _http_get(live_server, "/api/naics/search?q=621330")
        assert status == 200
        data = json.loads(body)
        assert "results" in data
        assert isinstance(data["results"], list)
        assert data["results"], "expected at least one match for an exact NAICS code"
        first = data["results"][0]
        assert set(first.keys()) == {
            "code",
            "title",
            "level",
            "internal_key",
            "internal_label",
        }
        assert first["internal_label"] == INDUSTRY_LABEL_MAP.get(first["internal_key"])

    def test_no_match_returns_empty_results_not_error(self, live_server: int) -> None:
        status, body = _http_get(
            live_server, "/api/naics/search?q=zzzznotarealindustryxyz"
        )
        assert status == 200
        data = json.loads(body)
        assert data["results"] == []

    def test_limit_param_respected(self, live_server: int) -> None:
        status, body = _http_get(live_server, "/api/naics/search?q=services&limit=2")
        assert status == 200
        data = json.loads(body)
        assert len(data["results"]) <= 2

    def test_nurse_query_over_http_returns_healthcare(self, live_server: int) -> None:
        """The verbatim reproduction from the 2026-08-02 report.

            curl "/api/naics/search?q=nurse&limit=6"

        returned six horticulture codes and zero healthcare ones. Pinned at
        the HTTP layer, not just the function, because that is the surface
        the wizard's typeahead and its aria-live announcement consume.
        """
        status, body = _http_get(live_server, "/api/naics/search?q=nurse&limit=6")
        assert status == 200
        results = json.loads(body)["results"]
        assert results, "q=nurse returned no results over HTTP"
        assert results[0]["internal_key"] in {"healthcare_medical", "mental_health"}
        assert (
            "Nursing" in results[0]["title"]
        ), f"top HTTP result for q=nurse is {results[0]['title']!r}"
        # internal_label is what the wizard renders next to each row -- a
        # screen-reader user hears this, so it must be resolved, not raw.
        assert results[0]["internal_label"] == INDUSTRY_LABEL_MAP.get(
            results[0]["internal_key"]
        )


# ═══════════════════════════════════════════════════════════════════════
# 5. Plan-payload round-trip
# ═══════════════════════════════════════════════════════════════════════


class TestPlanPayloadRoundTrip:
    def test_round_trip_with_naics_fields(self) -> None:
        payload = {
            "client_name": "Acme Legal",
            "industry": "legal_services",
            "industry_label": "Legal Services · NAICS 5411 — Law Offices",
            "naics_selected_code": "5411",
            "naics_selected_title": "Law Offices",
            "budget": "$50,000",
            "roles": ["Paralegal"],
            "locations": ["Chicago, IL"],
        }
        plan = PlanData.from_dict(payload)
        out = plan.to_dict()
        assert out["naics_selected_code"] == "5411"
        assert out["naics_selected_title"] == "Law Offices"
        assert out["industry_label"] == "Legal Services · NAICS 5411 — Law Offices"
        assert out["industry"] == "legal_services"

    def test_round_trip_without_naics_fields_unchanged(self) -> None:
        """Backward compatibility: a plan with no naics fields must
        round-trip identically to today (no naics keys invented)."""
        payload = {
            "client_name": "Acme Corp",
            "industry": "tech_engineering",
            "industry_label": "Technology & Engineering",
            "budget": "$50,000",
            "roles": ["Software Engineer"],
            "locations": ["Austin, TX"],
        }
        plan = PlanData.from_dict(payload)
        out = plan.to_dict()
        assert "naics_selected_code" not in out
        assert "naics_selected_title" not in out
        assert out["industry_label"] == "Technology & Engineering"

    def test_missing_naics_fields_do_not_block_construction(self) -> None:
        """A minimal/legacy payload with no naics fields at all must still
        construct a valid PlanData (never raise)."""
        plan = PlanData.from_dict({"industry": "general_entry_level"})
        assert plan.industry == "general_entry_level"


# ═══════════════════════════════════════════════════════════════════════
# 6. Typeahead affordances (design panel 2026-07-31, iteration 3)
# ═══════════════════════════════════════════════════════════════════════

_JS_PATH = "templates/partials/index/body_app_js.html"
_CSS_PATH = "templates/partials/index/head_styles.html"
_HTML_PATH = "templates/partials/index/body_content.html"


class TestTypeaheadAffordances:
    """No browser/DOM is available in this suite -- these are template
    source assertions. They exist to pin the exact markers a future edit
    could silently delete (a class renamed, an !important dropped, a
    display value flipped back to "block"), not to render or execute the
    JS/CSS. See the vacuousness check run alongside this task's report
    for proof these fail against the pre-change templates.
    """

    @staticmethod
    def _read(rel_path: str) -> str:
        return (PROJECT_ROOT / rel_path).read_text(encoding="utf-8")

    # ── Item 1: superseded chip / notice ────────────────────────────────

    def test_superseded_classes_in_css_and_js(self) -> None:
        css = self._read(_CSS_PATH)
        js = self._read(_JS_PATH)
        for marker in ("naics-chip--superseded", "naics-hint--superseded"):
            assert marker in css, f"{marker} missing from head_styles.html"
            assert marker in js, f"{marker} missing from body_app_js.html"

    def test_superseded_hint_rule_carries_dim(self) -> None:
        """Restructure 2026-07-31 (dropdown-occlusion fix): the notice
        used to collapse via `display: none !important` while a query
        was in flight (naics-hint--suppressed). Now that the chip/notice
        lead .naics-search-wrap instead of trailing the input (Change A),
        collapsing the notice would shift the #naicsSearch input itself
        upward mid-typing -- worse than the stacking problem the
        collapse used to solve, which no longer exists once the chip/
        notice are out of the dropdown's path. It dims instead, mirroring
        .naics-chip--superseded, and the class is renamed to
        naics-hint--superseded since it no longer suppresses anything.
        The old display:none assertion is deliberately gone, not just
        loosened -- it would be actively wrong post-restructure."""
        css = self._read(_CSS_PATH)
        start = css.index(".naics-mapping-hint.naics-hint--superseded {")
        end = css.index("\n  }", start)
        rule_body = css[start:end]
        assert "opacity: 0.55" in rule_body
        assert "filter: saturate(0.7)" in rule_body
        assert "display: none" not in rule_body
        # !important is no longer needed: nothing else in the stylesheet
        # sets opacity/filter on .naics-mapping-hint or .form-hint
        # (verified by hand -- see the CSS comment directly above this
        # rule). If a future rule adds one, the fix is raising
        # specificity like the color override above does, not silently
        # losing the cascade and reintroducing !important out of habit.
        assert "!important" not in rule_body

    def test_mapping_hint_never_collapses_via_display_none(self) -> None:
        """Explicit regression guard (distinct from the dim assertion
        above, which only scopes the renamed --superseded rule): scans
        the ENTIRE .naics-mapping-hint rule family -- the base rule, the
        .studio color-cascade override, and the --superseded state rule,
        which are contiguous in the stylesheet -- and asserts none of
        them carry `display: none !important` anywhere. Change B
        deliberately removed the only rule that ever collapsed this
        element; this pins that it cannot come back under some other
        selector in the same family without this test catching it."""
        css = self._read(_CSS_PATH)
        start = css.index("\n  .naics-mapping-hint {")
        state_rule_start = css.index(".naics-mapping-hint.naics-hint--superseded {")
        end = css.index("\n  }", state_rule_start)
        region = css[start:end]
        assert "display: none !important" not in region, (
            "a .naics-mapping-hint rule still collapses via "
            "display: none !important -- Change B replaced the collapse "
            "with a dim so the notice leading the block (Change A) never "
            "shifts the #naicsSearch input"
        )

    def test_set_naics_search_in_flight_defined_and_called(self) -> None:
        js = self._read(_JS_PATH)
        assert "function _setNaicsSearchInFlight(active)" in js
        # 1 definition + at least the input-listener, Escape, and
        # closeDropdown() call sites.
        call_sites = js.count("_setNaicsSearchInFlight(")
        assert (
            call_sites >= 4
        ), f"expected >=4 occurrences (def + >=3 calls), found {call_sites}"

    def test_escape_handled_before_matches_guard(self) -> None:
        """Defect 2 (verified in-browser, 2026-07-31): the keydown handler
        used to open with `if (resultsEl.style.display !== "block" ||
        !_matches.length) return;`, which early-returns whenever the
        dropdown is showing the empty "No matching NAICS industries" state
        (_matches.length === 0) -- exactly the stale-chip-vs-no-results
        case Escape exists to handle. The old Escape branch sat AFTER that
        guard and was therefore dead code in that scenario. Pins the fix
        by string position: `e.key === "Escape"` must appear before the
        `_matches.length` guard inside the same keydown handler."""
        js = self._read(_JS_PATH)
        handler_start = js.index('input.addEventListener("keydown", function (e) {')
        handler_end = js.index("\n    });", handler_start)
        handler_body = js[handler_start:handler_end]
        escape_idx = handler_body.index('e.key === "Escape"')
        guard_idx = handler_body.index('resultsEl.style.display !== "block"')
        assert escape_idx < guard_idx, (
            "Escape must be handled before the _matches.length guard, or "
            "Escape is unreachable while the dropdown shows the empty state"
        )

    def test_escape_clears_query_and_refilters_cards(self) -> None:
        """Final-gate divergence (design panel 2026-07-31, mechanism lens):
        Escape used to only remove the chip dimming and restore the
        substitution notice -- signalling "all clear" -- while the typed
        query stayed in the box AND filterIndustryCards() kept up to 22 of
        23 quick-pick cards hidden. Escape is the explicit abandon
        gesture, so it must clear the query text and re-run the filter
        with an empty string too, not just undo the dimming. Scoped to
        the Escape branch specifically (same handler_body extraction as
        test_escape_handled_before_matches_guard) so this can't pass by
        matching some other filterIndustryCards( call site."""
        js = self._read(_JS_PATH)
        handler_start = js.index('input.addEventListener("keydown", function (e) {')
        handler_end = js.index("\n    });", handler_start)
        handler_body = js[handler_start:handler_end]
        escape_start = handler_body.index('e.key === "Escape"')
        guard_idx = handler_body.index('resultsEl.style.display !== "block"')
        escape_body = handler_body[escape_start:guard_idx]
        assert (
            'input.value = ""' in escape_body
        ), "Escape must clear the typed query, not just restore the chip"
        assert 'filterIndustryCards("")' in escape_body, (
            "Escape must re-run the card filter with an empty query, or "
            "quick-pick cards stay hidden behind a query that no longer "
            "shows anywhere"
        )

    def test_blur_restores_chip_and_hint_on_abandonment(self) -> None:
        """Defect 3 (verified in-browser, 2026-07-31): clicking away after
        typing a new query -- without picking a row or pressing Escape --
        left the chip dimmed and the substitution notice display:none
        permanently, even though a NAICS code was still attached and its
        disclosure should still be visible. The blur listener must call
        _setNaicsSearchInFlight(false) to restore both on abandonment.
        Picking a row does not blur (each option's mousedown calls
        preventDefault to hold focus), so this cannot race a pick."""
        js = self._read(_JS_PATH)
        blur_start = js.index('input.addEventListener("blur", function () {')
        blur_end = js.index("\n    });", blur_start)
        blur_body = js[blur_start:blur_end]
        assert "_setNaicsSearchInFlight(false)" in blur_body

    # ── Item 2: substitution notice as a real info affordance ──────────

    def test_mapping_hint_carries_info_tokens(self) -> None:
        css = self._read(_CSS_PATH)
        start = css.index("\n  .naics-mapping-hint {")
        end = css.index("\n  }", start)
        rule_body = css[start:end]
        # Same tokens as .copilot-nudge--info: rgba(...) background
        # AND border, plus the #93d4e8 text color -- not invented colors.
        # Final-gate retreat (2026-07-31): the notice fires on 100% of
        # successful picks, permanently -- an amber warning that never
        # *not* fires is a mislabel, so this moved off the warning tokens
        # (rgba(251, 191, 36...) / #fcd34d) onto the wizard's INFO ones.
        assert rule_body.count("rgba(107, 179, 205") == 2
        assert "#93d4e8" in rule_body

    def test_mapping_hint_studio_override_wins_cascade(self) -> None:
        """Defect 1 (verified in-browser via computed style, 2026-07-31):
        the plain .naics-mapping-hint rule's `color: #93d4e8` (asserted
        above) is not sufficient on its own -- the rendered element also
        carries class="form-hint" and sits inside .studio, where the
        cockpit reskin's `.studio .form-hint { color: var(--np-faint)
        !important; }` (specificity 0,2,0) beat it and rendered the
        notice muted lavender instead of info/teal. A higher-specificity
        `.studio`-prefixed override (0,3,0) is required to actually win
        the cascade; source-text presence of "#93d4e8" alone (the test
        above) cannot see this and stayed green the whole time the
        rendered color was wrong."""
        css = self._read(_CSS_PATH)
        selector = ".studio .naics-search-wrap .naics-mapping-hint"
        assert (
            selector in css
        ), f"{selector} override selector missing from head_styles.html"
        start = css.index(selector)
        end = css.index("}", start)
        rule_body = css[start:end]
        assert "color: #93d4e8 !important" in rule_body

    def test_mapping_hint_copy_is_category_level(self) -> None:
        """Design panel 2026-07-31 (false-claim fix): benchmarks are keyed
        off the 21 quick-pick internal keys, never off the NAICS code
        (shared_utils.py ~90-104), so "benchmarks are category-level" is a
        standing architectural fact, not a per-pick failure -- that part
        of the copy is accurate. A later revision of this same notice
        additionally claimed the NAICS code "is recorded on the
        deliverables", which is false for the Excel deliverable:
        excel_v2.py:2075 _get_industry_label() re-derives the industry
        label from the industry key and never reads
        data["industry_label"], so the NAICS suffix format_industry_label()
        appends is discarded before the workbook is written (callers at
        excel_v2.py:4461, 5749, 6295, 11077; PPTX and PDF/HTML do carry
        the suffix, Excel does not). Pins the corrected copy -- states the
        category-level mechanism, keeps the explicit substitution signal
        ("using the closest category") instead of over-neutralizing it
        away, and makes no promise about deliverables -- and hard-guards
        against both the deliverables false-claim and the older
        failure-framed copy ever coming back."""
        js = self._read(_JS_PATH)
        assert "No code-level benchmark" not in js, (
            "old failure-framed copy must be fully removed, not just " "superseded"
        )
        assert "is recorded on the deliverables" not in js, (
            "false deliverables claim must stay removed -- the NAICS code "
            "is dropped from the Excel deliverable at excel_v2.py:2075, "
            "so the notice must never promise it reaches 'the deliverables'"
        )
        assert "Benchmarks are category-level, not per-code" in js
        assert "using the closest category: " in js

    def test_pickresult_announcement_matches_visual_copy_substance(self) -> None:
        """Trust/proof re-judge (2026-07-31): pickResult()'s screen-reader
        announcement paraphrased the substitution notice as "Benchmarks
        are category-level -- closest category: ..." -- dropping "not
        per-code", the sharpest clause and the one the visual copy (pinned
        above by test_mapping_hint_copy_is_category_level) leads with. A
        sighted user and a screen-reader user must get the same substance,
        not two independently-drifting paraphrases of the same fact.
        Scoped to pickResult()'s own function body (up to the next
        function, announceMatchCount) rather than a bare file-wide
        substring -- the visual copy elsewhere in this same file already
        contains "not per-code", so an unscoped check would pass even if
        pickResult's announcement were never fixed."""
        js = self._read(_JS_PATH)
        start = js.index("function pickResult(match) {")
        end = js.index("\n    function announceMatchCount()", start)
        body = js[start:end]
        assert "not per-code" in body, (
            "pickResult()'s SR announcement must include 'not per-code', "
            "matching the visual substitution notice's sharpest clause"
        )

    def test_render_naics_chip_uses_inline_flex_not_block(self) -> None:
        js = self._read(_JS_PATH)
        start = js.index("function _renderNaicsChip()")
        end = js.index("\n  function ", start + len("function _renderNaicsChip()"))
        body = js[start:end]
        assert 'hint.style.display = "inline-flex"' in body
        assert 'hint.style.display = "block"' not in body

    # ── Item 3: mechanism/a11y -- usage hint + live region ──────────────

    def test_markup_has_live_region_and_usage_hint(self) -> None:
        html = self._read(_HTML_PATH)
        for marker in (
            'id="naicsLiveRegion"',
            'role="status"',
            'aria-live="polite"',
            'id="naicsUsageHint"',
            'aria-describedby="naicsUsageHint naicsMappingHint"',
        ):
            assert marker in html, f"{marker} missing from body_content.html"

    def test_naics_search_input_describedby_includes_mapping_hint(self) -> None:
        """Trust/proof re-judge (2026-07-31): #naicsMappingHint (the
        substitution notice) was never programmatically associated with
        #naicsSearch -- aria-describedby only listed naicsUsageHint. On a
        page restore with a saved NAICS code, the notice renders (via
        _renderNaicsChip()) without any live-region announcement (that
        only fires from pickResult()'s explicit call on a fresh pick), so
        a screen-reader user landed on the input with no accessible-
        description route to it, only browse-mode discovery. A hidden
        referenced id is simply skipped per the aria-describedby spec, so
        listing naicsMappingHint unconditionally is correct -- it becomes
        part of the input's description exactly when the notice is
        visible. Scoped to the #naicsSearch <input> tag itself (not a
        bare file-wide substring) so this can't pass by matching some
        unrelated aria-describedby elsewhere in the file."""
        html = self._read(_HTML_PATH)
        input_start = html.index('id="naicsSearch"')
        input_end = html.index("/>", input_start)
        input_tag = html[input_start:input_end]
        attr_marker = 'aria-describedby="'
        attr_start = input_tag.index(attr_marker) + len(attr_marker)
        attr_end = input_tag.index('"', attr_start)
        describedby_ids = input_tag[attr_start:attr_end].split()
        assert "naicsUsageHint" in describedby_ids
        assert "naicsMappingHint" in describedby_ids

    def test_sr_only_utility_exists(self) -> None:
        css = self._read(_CSS_PATH)
        assert ".sr-only {" in css

    # ── Regression guard: final-gate CSS ordering (064cfa0) ─────────────

    def test_naics_filtered_out_guard_still_last_naics_rule(self) -> None:
        """The .naics-filtered-out block (head_styles.html tail) must
        stay the LAST rule touching .naics- anything -- it's what makes
        filterIndustryCards()'s class toggle beat the .studio
        `display: flex !important` cockpit reskin on specificity+source
        order both. A test that didn't check this could pass while a
        future edit (e.g. this task's own CSS additions) silently
        reordered a `.naics-` rule after it and broke that guarantee
        again."""
        css = self._read(_CSS_PATH)
        filtered_out_idx = css.rindex("naics-filtered-out")
        mapping_hint_idx = css.rindex(".naics-mapping-hint")
        assert filtered_out_idx > mapping_hint_idx

        after = css[filtered_out_idx + len("naics-filtered-out") :]
        assert (
            ".naics-" not in after
        ), "a .naics- rule was added after the .naics-filtered-out guard"

    # ── Design panel 2026-07-31 (craft + mechanism lenses, round 2) ─────

    def test_input_and_results_share_a_dedicated_anchor(self) -> None:
        """.naics-results is `position: absolute; top: calc(100% + 6px)`,
        which resolves against its offsetParent -- the nearest positioned
        ancestor. That used to be .naics-search-wrap, whose rendered
        height also includes the usage hint, the chip and the
        substitution notice, so the dropdown opened ~78px below the
        input it belongs to (per aria-controls) and landed on the
        quick-pick cards; it only looked correct in the empty state.
        Wrapping ONLY the input and the results listbox in a dedicated
        positioned ancestor (.naics-input-anchor) fixes the offsetParent
        without touching layout of anything else in the wrap. Bounded by
        the anchor markup's own extent (up to the next sibling's id) so
        this can't pass by matching naicsSearch/naicsResults/
        naicsSelectedChip anywhere else in the file."""
        html = self._read(_HTML_PATH)
        assert 'class="naics-input-anchor"' in html
        anchor_start = html.index('class="naics-input-anchor"')
        anchor_end = html.index('id="naicsUsageHint"', anchor_start)
        anchor_span = html[anchor_start:anchor_end]
        assert (
            'id="naicsSearch"' in anchor_span
        ), "#naicsSearch must live inside .naics-input-anchor"
        assert 'id="naicsResults"' in anchor_span, (
            "#naicsResults must live inside .naics-input-anchor -- "
            "otherwise `top: 100%` still resolves against the wrap"
        )
        assert 'id="naicsSelectedChip"' not in anchor_span, (
            "the chip must stay OUTSIDE .naics-input-anchor -- pulling it "
            "in would reintroduce its height into the anchor's offsetHeight"
        )

    def test_naics_input_anchor_is_positioned(self) -> None:
        css = self._read(_CSS_PATH)
        start = css.index(".naics-input-anchor {")
        end = css.index("}", start)
        rule_body = css[start:end]
        assert "position: relative" in rule_body

    def test_naics_chip_capped_to_column_width(self) -> None:
        """The input/notice/dropdown all sit in a 400px column; the chip
        had no max-width of its own and measured 471.9px, overhanging
        the column by 36px per side.

        Design panel 2026-07-31 (craft lens, top change): max-width
        alone wasn't enough -- inline-flex made the chip hug its own
        content, so under the wrap's text-align: center its LEFT edge
        drifted with the title length instead of lining up with the
        input above it (measured +76.7px vs the input for a short
        title, +16.8px for a medium one, 0 only once the title hit the
        max-width cap). width: 100% makes the chip fill the same
        column the input's own `width: 100%; max-width: 400px` does,
        so the two always share a left edge regardless of title
        length."""
        css = self._read(_CSS_PATH)
        start = css.index("\n  .naics-chip {")
        end = css.index("\n  }", start)
        rule_body = css[start:end]
        assert "max-width: 400px" in rule_body
        assert "width: 100%" in rule_body

    def test_naics_results_left_aligned(self) -> None:
        """.naics-search-wrap sets text-align: center for the chip/notice
        below the input, which leaked into the dropdown too -- each
        row's two lines started at a different x than the input's own
        left-aligned query text (the notice already opts back into left
        explicitly)."""
        css = self._read(_CSS_PATH)
        start = css.index("\n  .naics-results {")
        end = css.index("\n  }", start)
        rule_body = css[start:end]
        assert "text-align: left" in rule_body

    # ── Design panel 2026-07-31 (chip max-width regressions) ────────────

    def test_naics_chip_clear_button_flex_basis_pinned(self) -> None:
        """Regression (verified by measurement): once the chip's
        max-width: 400px (pinned above by
        test_naics_chip_capped_to_column_width) binds, flex distributes
        shrink across BOTH children of .naics-chip -- the label span AND
        this clear (x) button, which defaulted to `flex: 0 1 auto`. A
        73-char NAICS title alone measured the button down to
        clientWidth 8 vs scrollWidth 9, clipping the glyph; longer
        titles shrink it further toward zero. Scoping ellipsis to the
        label span (the sibling rule above) does not protect a sibling
        that is itself shrinkable -- the button needs its own pinned
        basis so only the label ever gives up width."""
        css = self._read(_CSS_PATH)
        start = css.index("\n  .naics-chip button {")
        end = css.index("\n  }", start)
        rule_body = css[start:end]
        assert "flex: 0 0 auto" in rule_body

    def test_render_naics_chip_tooltip_is_dom_property_not_html_attribute(
        self,
    ) -> None:
        """Final-gate veto fix: _escHtml() (see its own definition,
        `function _escHtml(str)`) is a TEXT-context escaper only -- it
        round-trips the input through `.textContent` and reads back
        `.innerHTML`, which escapes `& < >` but leaves `"` completely
        untouched. _renderNaicsChip() used to interpolate an
        _escHtml()'d string straight into `<span title="...">`, an
        ATTRIBUTE context, so a NAICS title containing a quote (e.g.
        `Evil" onmouseover="alert(1)" data-pwned="yes`) broke out of
        the attribute and injected real attributes/event handlers onto
        the span -- confirmed live in-browser: span.hasAttribute(
        'onmouseover') was true, and title read back truncated at the
        quote instead of the intended tooltip. Values today come from
        the server-side NAICS dataset (no title in
        data/naics_2022.json contains a quote) and restored
        sessionStorage, so this was latent rather than exploited in
        practice, but it is a real injection primitive and must never
        come back. The fix sets the tooltip as a DOM property
        (`labelSpan.title = ...`) after the markup is built instead of
        inside the HTML string -- a property assignment takes plain
        text verbatim, involves no HTML parsing, and has nothing to
        break out of."""
        js = self._read(_JS_PATH)
        start = js.index("function _renderNaicsChip()")
        end = js.index("\n  function ", start + len("function _renderNaicsChip()"))
        body = js[start:end]

        assert '<span title="' not in body, (
            "the tooltip must never be interpolated into an HTML "
            "attribute string -- _escHtml() does not escape quotes, so "
            "doing so is an attribute-injection primitive"
        )
        assert ".title =" in body, (
            "the tooltip must be assigned as a DOM property "
            "(labelSpan.title = ...), which needs no escaping and "
            "cannot inject markup"
        )

    def test_render_naics_chip_label_span_has_full_title_tooltip(self) -> None:
        """Regression: the label span truncates with a CSS ellipsis once
        the chip's max-width binds, so a cut landing on the parenthetical
        that disambiguates near-identical codes (e.g. 621330 "...(except
        Physicians)" vs 621111) left a user with no way to recover which
        code was actually attached. The tooltip must live on the label
        span specifically -- putting it on the chip container would also
        cover the clear (x) button and read as a misleading tooltip over
        the x.

        Rewritten at the final gate (see test_render_naics_chip_
        tooltip_is_dom_property_not_html_attribute immediately above for
        the injection this guards against): the tooltip is now a DOM
        property assignment on the label span, built from the RAW
        selectedNaicsCode/selectedNaicsTitle values rather than their
        _escHtml()'d copies -- a property assignment takes plain text
        verbatim, so running it through the HTML-string escaper first
        would double-escape it (e.g. a literal `&` in a title would
        render in the tooltip as the literal text `&amp;` instead of
        `&`) -- and a real em dash character rather than the &mdash;
        entity, which likewise only means anything in an HTML-parsing
        context. Scoped to _renderNaicsChip()'s own function body so
        this can't pass on some unrelated title= elsewhere in the
        file."""
        js = self._read(_JS_PATH)
        start = js.index("function _renderNaicsChip()")
        end = js.index("\n  function ", start + len("function _renderNaicsChip()"))
        body = js[start:end]

        assert "labelSpan.title =" in body, (
            "the tooltip must be set via a labelSpan.title property " "assignment"
        )
        title_idx = body.index("labelSpan.title =")
        button_idx = body.index("<button")
        assert button_idx < title_idx, (
            "the button markup is built into chip.innerHTML first; "
            "labelSpan is then looked up (via chip.querySelector) and "
            "given its title afterward"
        )
        assert 'chip.setAttribute("title"' not in body and "chip.title =" not in body, (
            "the tooltip must be attached to the label span specifically, "
            "not set on the chip container element (which would also "
            "cover the clear (x) button)"
        )

        # The property assignment must use the RAW values -- a DOM
        # property takes plain text verbatim, so running it through
        # _escHtml() (the copy meant for the HTML-string visible text)
        # would double-escape it.
        title_stmt_end = body.index(";", title_idx)
        title_stmt = body[title_idx:title_stmt_end]
        assert "selectedNaicsCode" in title_stmt
        assert "_escHtml(selectedNaicsCode)" not in title_stmt, (
            "the tooltip must use the raw selectedNaicsCode, not the "
            "_escHtml()'d copy built for the HTML-string visible text"
        )
        assert "selectedNaicsTitle" in title_stmt
        assert "_escHtml(selectedNaicsTitle)" not in title_stmt, (
            "the tooltip must use the raw selectedNaicsTitle, not the "
            "_escHtml()'d copy built for the HTML-string visible text"
        )
        assert "—" in title_stmt, (
            "the tooltip is a DOM property, not HTML, so it must use a "
            "real em dash character, not the &mdash; entity"
        )

    # ── Restructure 2026-07-31 (dropdown-occlusion fix) ──────────────────
    #
    # #naicsResults opens at `top: calc(100% + 6px)` off .naics-input-anchor.
    # With the chip/notice previously rendered BELOW the input (between it
    # and the quick-pick cards), the open dropdown was tall enough to reach
    # past the input and fully cover them: measured chip 488.7-521.9px vs
    # dropdown 468-529.2px, and document.elementFromPoint() at the clear
    # (x) button's centre resolved to the dropdown's own .naics-empty div,
    # not the button. The clear (x) was pointer-dead, and item 1's
    # superseded dimming (pinned above) was invisible in exactly the state
    # it exists to communicate. Moving the chip/notice ABOVE the input so
    # the dropdown opens downward into space below them, instead of over
    # them, is a DOM-order fix -- these tests pin that order directly.

    def test_chip_and_notice_precede_input_anchor_hint_and_live_region_follow(
        self,
    ) -> None:
        """Chip and substitution notice must render BEFORE
        .naics-input-anchor (the input + dropdown) inside
        .naics-search-wrap, not after -- otherwise the open dropdown,
        which is tall enough to reach past the input, covers them again
        exactly like it did pre-restructure (see the section comment
        above for the measured overlap and the dead clear-(x)-button
        repro). The usage hint and live region must stay AFTER the
        anchor -- the usage hint describes the input directly above it,
        and the live region is pinned last elsewhere (test_markup_has_
        live_region_and_usage_hint's sibling test below documents why).
        Scoped to .naics-search-wrap specifically (bounded by the next
        sibling's id, industryGrid) using the same slicing approach as
        test_input_and_results_share_a_dedicated_anchor, so this can't
        pass by matching ids anywhere else in the file. Compares string
        indices directly rather than re-parsing the DOM -- there is no
        HTML parser in this suite (see the class docstring)."""
        html = self._read(_HTML_PATH)
        wrap_start = html.index('class="naics-search-wrap"')
        wrap_end = html.index('id="industryGrid"', wrap_start)
        wrap = html[wrap_start:wrap_end]

        anchor_idx = wrap.index('class="naics-input-anchor"')
        chip_idx = wrap.index('id="naicsSelectedChip"')
        hint_idx = wrap.index('id="naicsMappingHint"')
        usage_idx = wrap.index('id="naicsUsageHint"')
        live_idx = wrap.index('id="naicsLiveRegion"')

        assert chip_idx < anchor_idx, (
            "naicsSelectedChip must precede .naics-input-anchor, or the "
            "open dropdown can cover it again"
        )
        assert hint_idx < anchor_idx, (
            "naicsMappingHint must precede .naics-input-anchor, or the "
            "open dropdown can cover it again"
        )
        assert usage_idx > anchor_idx, (
            "naicsUsageHint must stay after .naics-input-anchor -- it "
            "describes the input directly above it"
        )
        assert live_idx > anchor_idx, (
            "naicsLiveRegion must stay after .naics-input-anchor -- it "
            "stays the last child of the wrap"
        )

    def test_chip_precedes_mapping_hint(self) -> None:
        """Within the leading pair, the chip must still come before the
        substitution notice that explains it -- reading order should
        stay "here's your selection" then "here's the caveat about it",
        unchanged from before the restructure."""
        html = self._read(_HTML_PATH)
        wrap_start = html.index('class="naics-search-wrap"')
        wrap_end = html.index('id="industryGrid"', wrap_start)
        wrap = html[wrap_start:wrap_end]
        assert wrap.index('id="naicsSelectedChip"') < wrap.index(
            'id="naicsMappingHint"'
        )
