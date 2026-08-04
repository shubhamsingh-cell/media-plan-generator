"""archive/excel_legacy.py CPC-citation containment (2026-08-04).

Scope of this test file: verify the CONTAINMENT fix on excel_legacy.py's two
industry x region CPA/CPC tables (line ~1131 ``cpa_cpc_benchmarks`` and line
~3842 ``peer_industries``, 77 CPC_UNCITED_SURFACE dollar figures total) --
NOT a refresh of the 77 figures themselves. Per the task scope, refreshing
those is a separate, larger, escalated piece of work; this commit only:

1. Pins the previously date-derived ``_current_year`` / ``_bench_year_label``
   (was ``datetime.date.today().year``-based, so every citation string that
   referenced it silently relabeled itself "current" on every year's first
   render while the underlying numbers never moved -- a false-freshness bug
   independent of the numbers' actual staleness) to the figures' true
   vintage: ``_current_year = 2026`` / ``_bench_year_label = "2025"``.
   (2026-08-04 correction: an earlier pass pinned this to 2023, which was
   itself fabricated -- nothing in this file or in data/*.json supports a
   2023 vintage. The corrected pin reproduces, verbatim, the citation years
   this file used before the 2023 regression: "SHRM 2025" / "Appcast 2025
   ... 379M clicks" / "Appcast 2026 ... 10th annual" -- all independently
   confirmed against data/*.json KB attributions and this file's own
   "$4,700 (SHRM 2025)" note.)
2. Sharpens the wording of the existing blanket source-disclosure (the one
   near the top of the "Recruitment Marketing Benchmarks" section) to
   explicitly say "LEGACY" / "not refreshed since 2023" instead of quietly
   implying current-year sourcing.
3. Adds an equivalent disclosure to the SECOND table ("Peer Industry
   Benchmark Comparison", ~2700 lines further down the same function/sheet)
   which previously had no citation or vintage marker of its own at all.
4. Changes NO dollar literal in either table.

Reachability finding (why this file matters at all): archive/excel_legacy.py
is wired in at app.py ~line 5199 and called at two sites -- app.py ~19317
(the /api/generate SYNC path, which is the OpenAPI-documented DEFAULT mode,
as a runtime-exception fallback when excel_v2.generate_excel_v2 raises
mid-generation) and app.py ~17162 (the async X-Async:true path used by the
wizard frontend, but only reached if excel_v2 fails to IMPORT entirely,
which isn't happening today since excel_v2 imports cleanly). So the sync
path's exception-fallback is the one that's actually live-reachable from a
real, currently-functioning brief.

IMPORTANT CAVEAT, disclosed rather than hidden: archive/excel_legacy.py's
generate_excel(data) is ITSELF currently broken by a separate, pre-existing,
unrelated bug -- a chain of NameErrors (load_channels_db, then
classify_role_tier, then fetch_client_logo, and very likely more; all names
that existed in app.py before this file was extracted from it in commit
0f6b70a7 but were never carried over) that crashes the function on every
single call, before it ever reaches ANY of the code this commit touches.
This means that, as of today, this fix's disclosure text cannot actually
reach a real rendered workbook -- the fallback path crashes before
rendering anything at all, dollar figures and disclosures alike. That crash
bug is flagged separately (out of scope for a CPC-citation containment
commit; it's an import/dependency bug, not a citation one) and is NOT fixed
here. Because of it, the tests below verify this fix at the SOURCE level
(the actual file text, which is what will render once/if that separate bug
is fixed) rather than by rendering a live workbook end-to-end.

Runs under pytest, or standalone: ``python3 tests/test_excel_legacy_containment.py``.
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

EXCEL_LEGACY_PATH = PROJECT_ROOT / "archive" / "excel_legacy.py"

# The 77 CPC_UNCITED_SURFACE dollar-range literals from the original
# classification sweep, in file order. 76 are `"cpc": "$X - $Y",` entries
# inside the two industry x region tables; the 77th is the bare
# `"$0.35 - $2.50",` ALL-INDUSTRY AVG row entry (no "cpc": key prefix,
# since avg_values is a plain list, not a dict).
EXPECTED_CPC_SNIPPETS = [
    '"cpc": "$0.90 - $3.50",', '"cpc": "$0.70 - $2.80",', '"cpc": "$0.30 - $1.50",',
    '"cpc": "$0.20 - $1.00",', '"cpc": "$0.40 - $1.60",', '"cpc": "$0.35 - $1.30",',
    '"cpc": "$0.10 - $0.70",', '"cpc": "$0.08 - $0.55",', '"cpc": "$1.20 - $4.50",',
    '"cpc": "$0.90 - $3.50",', '"cpc": "$0.25 - $1.80",', '"cpc": "$0.20 - $1.40",',
    '"cpc": "$0.35 - $1.30",', '"cpc": "$0.28 - $1.10",', '"cpc": "$0.08 - $0.50",',
    '"cpc": "$0.05 - $0.40",', '"cpc": "$0.90 - $3.50",', '"cpc": "$0.75 - $2.80",',
    '"cpc": "$0.30 - $1.50",', '"cpc": "$0.18 - $0.90",', '"cpc": "$0.25 - $1.00",',
    '"cpc": "$0.20 - $0.85",', '"cpc": "$0.08 - $0.40",', '"cpc": "$0.05 - $0.35",',
    '"cpc": "$1.50 - $5.00",', '"cpc": "$1.20 - $4.00",', '"cpc": "$0.40 - $2.00",',
    '"cpc": "$0.25 - $1.40",', '"cpc": "$0.90 - $3.00",', '"cpc": "$0.75 - $2.50",',
    '"cpc": "$0.30 - $1.50",', '"cpc": "$0.20 - $1.00",', '"cpc": "$0.40 - $1.80",',
    '"cpc": "$0.30 - $1.30",', '"cpc": "$0.10 - $0.60",', '"cpc": "$0.08 - $0.45",',
    '"cpc": "$0.60 - $2.20",', '"cpc": "$0.50 - $1.80",', '"cpc": "$0.15 - $0.85",',
    '"cpc": "$0.12 - $0.65",', '"cpc": "$0.85 - $3.20",', '"cpc": "$0.70 - $2.60",',
    '"cpc": "$0.25 - $1.30",', '"cpc": "$0.18 - $0.85",', '"cpc": "$0.22 - $1.00",',
    '"cpc": "$0.18 - $0.80",', '"cpc": "$0.06 - $0.40",', '"cpc": "$0.04 - $0.35",',
    '"cpc": "$0.75 - $2.80",', '"cpc": "$0.60 - $2.20",', '"cpc": "$0.18 - $1.00",',
    '"cpc": "$0.14 - $0.75",', '"cpc": "$0.18 - $0.75",', '"cpc": "$0.15 - $0.65",',
    '"cpc": "$0.05 - $0.30",', '"cpc": "$0.04 - $0.25",', '"cpc": "$0.70 - $2.80",',
    '"cpc": "$0.55 - $2.20",', '"cpc": "$0.15 - $0.90",', '"cpc": "$0.12 - $0.70",',
    '"cpc": "$0.65 - $2.50",', '"cpc": "$0.50 - $2.00",', '"cpc": "$0.15 - $0.80",',
    '"cpc": "$0.10 - $0.60",', '"cpc": "$0.55 - $2.20",', '"cpc": "$0.45 - $1.80",',
    '"cpc": "$0.12 - $0.75",', '"cpc": "$0.08 - $0.50",', '"cpc": "$0.90 - $3.50",',
    '"cpc": "$1.20 - $4.50",', '"cpc": "$0.25 - $1.00",', '"cpc": "$0.90 - $3.50",',
    '"cpc": "$0.40 - $1.80",', '"cpc": "$0.22 - $1.00",', '"cpc": "$1.50 - $5.00",',
    '"cpc": "$0.35 - $1.30",', '"$0.35 - $2.50",',
]


def _src() -> str:
    return EXCEL_LEGACY_PATH.read_text(encoding="utf-8")


def test_no_dollar_literals_changed_in_the_two_legacy_tables():
    """Containment proof: every one of the 77 originally-classified dollar
    literals must still appear in the file, the SAME number of times each
    (a multiset comparison, immune to the line-number drift this fix's own
    added comment/disclosure lines introduce). This commit must not add,
    remove, or change any of them -- that refresh is explicitly out of
    scope."""
    src = _src()
    expected_counts = Counter(EXPECTED_CPC_SNIPPETS)
    for snippet, expected_n in expected_counts.items():
        actual_n = src.count(snippet)
        assert actual_n == expected_n, (
            f"{snippet!r}: expected {expected_n} occurrence(s) in "
            f"archive/excel_legacy.py, found {actual_n} -- a dollar literal "
            "was added/removed/changed; this containment commit must not "
            "touch the 77 CPC figures themselves"
        )


def test_bench_year_label_is_pinned_not_date_derived():
    """The old `_current_year = datetime.date.today().year` /
    `_bench_year_label = f"{_current_year - 1}-{_current_year}"` computation
    made every citation string that used it silently relabel itself
    "current" every year while the numbers never moved. Both must now be
    pinned literals reflecting the true vintage: _current_year = 2026 (so
    that `_current_year - 1` / `_current_year` reproduce the "Appcast 2025
    ... 379M clicks" / "Appcast 2026 ... 10th annual" footnote pair) and
    _bench_year_label = "2025" (matching this file's own "$4,700
    (SHRM 2025)" note -- see test_citation_years_agree_with_the_files_own_
    shrm_cph_note below for the drift-guard on that specific claim).

    2026-08-04: a prior pass pinned both to 2023, which was itself
    fabricated -- nothing in this file or in data/*.json supports a 2023
    vintage. This test now asserts the corrected values and would fail
    again if either drifted back to 2023 (or to any other unverified
    value)."""
    src = _src()
    assert "_current_year = 2026" in src, (
        "_current_year is no longer pinned to the figures' true vintage "
        "(2026, so that _current_year - 1 == 2025 for the Appcast "
        "379M-click citation)"
    )
    assert '_bench_year_label = "2025"' in src, (
        "_bench_year_label is no longer pinned to the figures' true vintage "
        "(2025, matching this file's own 'SHRM 2025' / '$4,700' note)"
    )
    assert "_current_year = 2023" not in src, (
        "_current_year has regressed to the fabricated 2023 vintage"
    )
    assert '_bench_year_label = "2023"' not in src, (
        "_bench_year_label has regressed to the fabricated 2023 vintage"
    )
    assert "_current_year = datetime.date.today().year" not in src, (
        "the old date-derived _current_year computation is still present -- "
        "it would silently re-imply 'current year' sourcing again"
    )
    assert '_bench_year_label = f"{_current_year - 1}-{_current_year}"' not in src, (
        "the old date-derived _bench_year_label computation is still present"
    )


def test_citation_years_agree_with_the_files_own_shrm_cph_note():
    """Drift guard (2026-08-04): this file states, in its own hardcoded
    data (the general_entry_level industry's yoy_trend note), a specific,
    literal, non-interpolated fact -- "Avg US CPH $4,700 (SHRM 2025)". The
    SHRM citation footnote a few hundred lines below cites the *same*
    "avg US CPH $4,700" figure via the interpolated _bench_year_label. If
    the two ever disagree on the year, the file is citing two different
    vintages for the identical $4,700 number -- exactly the kind of
    self-contradiction that let the 2023 fabrication ship undetected
    (nothing checked the interpolated citation against the file's own
    literal facts). This test fails if that ever happens again, regardless
    of what specific year _bench_year_label is pinned to."""
    src = _src()
    literal_note_idx = src.index("Avg US CPH $4,700 (SHRM ")
    literal_year = src[literal_note_idx:].split("(SHRM ", 1)[1].split(")", 1)[0]

    footnote_idx = src.index("SHRM {_bench_year_label} Benchmarking Reports")
    # Confirm the footnote also asserts the $4,700 figure, so we know it's
    # the same claim, not a coincidentally-matching year on an unrelated one.
    footnote_line_end = src.index("\n", footnote_idx)
    footnote_line = src[footnote_idx:footnote_line_end]
    assert "$4,700" in footnote_line, (
        "expected the SHRM footnote to cite the same 'avg US CPH $4,700' "
        "figure as the file's own literal note -- if the footnote text "
        "changed, re-verify this test still checks the same claim"
    )

    rendered_footnote_year = _bench_year_label_value(src)
    assert rendered_footnote_year == literal_year, (
        f"citation-year mismatch: the file's own literal note says "
        f"'SHRM {literal_year}' for the $4,700 CPH figure, but the "
        f"interpolated SHRM footnote would render as "
        f"'SHRM {rendered_footnote_year}' -- these must agree, or the "
        "file is citing two different vintages for the same number"
    )


def _bench_year_label_value(src: str) -> str:
    """Extract the pinned _bench_year_label literal from the source text
    (source-level, matching this file's existing convention of testing
    the .py text rather than executing generate_excel(), since that
    function crashes on the separate pre-existing bug documented below).

    2026-08-04 fix: the regex must be anchored to line-start (``^\\s*``,
    ``re.M``). Unanchored, ``re.search`` returns the FIRST match anywhere
    in the file -- and the comment block immediately above the real
    assignment (added by the same commit that introduced this test) itself
    contains the literal text ``_bench_year_label = "2025"`` while
    explaining the pin (see archive/excel_legacy.py line ~1127, one line
    before the real assignment at line ~1132). An unanchored search always
    reads that comment's copy, never the actual code, so it could never
    catch a real drift in the assignment. Anchoring to line-start (after
    only leading whitespace) skips the ``# ...`` comment line, since a
    ``#`` there fails the anchor, and matches only the real assignment
    statement. See test_drift_guard_regex_reads_the_assignment_not_the_
    comment_near_it below for the regression proof."""
    import re

    match = re.search(r'^\s*_bench_year_label = "([^"]+)"', src, re.M)
    assert match, "could not find a pinned _bench_year_label = \"...\" literal"
    return match.group(1)


def test_drift_guard_regex_reads_the_assignment_not_the_comment_near_it():
    """Negative control for the drift-guard helper above (2026-08-04).

    Proves the anchored regex is actually reading the real code assignment
    and not the explanatory comment that sits one line above it (which
    happens to contain the identical literal text ``_bench_year_label =
    "2025"`` while documenting the pin). Mutates ONLY the real assignment
    line in an in-memory copy of the source (leaving the comment line
    untouched) and asserts the extracted value follows the mutation -- if
    the extractor were reading the comment instead of the code, this
    mutation would have no effect on its output.

    Also demonstrates, by re-implementing the OLD unanchored pattern
    inline, that the pre-fix regex fails this exact proof: it keeps
    reporting the comment's "2025" regardless of what the real assignment
    line says, because the comment appears first in the file.
    """
    import re

    src = _src()

    real_assignment_line = '    _bench_year_label = "2025"'
    assert src.count(real_assignment_line) == 1, (
        "expected exactly one real _bench_year_label assignment line to "
        "mutate -- source layout changed, re-verify this test"
    )
    comment_line_fragment = '_bench_year_label = "2025"'
    # Sanity: the comment line (unindented-to-4-spaces, prefixed with '#')
    # containing this same literal text must still precede the real
    # assignment, or this negative control is no longer proving anything.
    comment_idx = src.index("# `_current_year - 1`")
    real_idx = src.index(real_assignment_line)
    assert comment_idx < real_idx, (
        "sanity: expected the explanatory comment to appear BEFORE the "
        "real assignment in the file -- if this ordering changed, the "
        "unanchored regex might no longer reproduce the bug this test "
        "guards against"
    )

    mutated_src = src.replace(
        real_assignment_line, '    _bench_year_label = "1999"', 1
    )
    # Confirm the mutation touched only the real assignment, not the
    # comment above it (which still reads "2025").
    assert comment_line_fragment in mutated_src, (
        "sanity: the comment's copy of the literal text should be "
        "untouched by this targeted mutation"
    )

    # --- OLD unanchored regex: fails to track the mutation (reads the
    # comment's stale "2025" instead of the mutated assignment). This is
    # the exact bug being fixed; kept inline (not calling the helper)
    # specifically so this proof survives even after the helper itself is
    # fixed.
    old_pattern_match = re.search(r'_bench_year_label = "([^"]+)"', mutated_src)
    assert old_pattern_match, "old pattern should still match something"
    assert old_pattern_match.group(1) == "2025", (
        "expected the OLD unanchored regex to still read the comment's "
        "stale '2025' after mutating only the real assignment -- if this "
        "fails, the old-regex characterization below is no longer accurate"
    )

    # --- NEW anchored regex (the fixed helper): tracks the mutation.
    new_value = _bench_year_label_value(mutated_src)
    assert new_value == "1999", (
        f"expected the fixed, line-anchored regex to read the MUTATED "
        f"real assignment ('1999'), not the untouched comment ('2025') "
        f"-- got {new_value!r}. The drift guard is not reading actual "
        "code."
    )

    # --- Unmutated source: fixed helper still returns the correct,
    # current value.
    assert _bench_year_label_value(src) == "2025"


def test_first_table_disclosure_flags_legacy_and_points_to_current_source():
    """The blanket disclosure above the first ('Recruitment Marketing
    Benchmarks') table must now explicitly say LEGACY / not-refreshed-since,
    and point the reader at the current cited source, not just cite a
    vintage-less source line as if it were current."""
    src = _src()
    assert "LEGACY -- NOT REFRESHED" in src, (
        "first table's section header no longer flags itself as legacy"
    )
    # Source-level check: the disclosure is an f-string interpolating
    # _bench_year_label (pinned to "2023" above), so the literal .py source
    # contains the placeholder token, not the rendered "2023" value.
    assert "LEGACY DATA, not refreshed since {_bench_year_label}" in src, (
        "first table's disclosure paragraph no longer states its vintage plainly"
    )
    assert "data/recruitment_industry_knowledge.json via benchmark_registry.py" in src, (
        "first table's disclosure no longer points the reader at the current, "
        "cited source for up-to-date figures"
    )


def test_second_table_now_carries_its_own_legacy_disclosure():
    """The second table ('Peer Industry Benchmark Comparison', ~2700 lines
    below the first) previously had a plain description sentence and NO
    citation or vintage marker of its own at all -- a reader who scrolled
    straight to it saw bare dollar figures with no source. It must now carry
    the same LEGACY / not-refreshed disclosure, positioned between the
    section header and the peer_industries data so it renders directly
    above the table."""
    src = _src()
    header_idx = src.index(
        'value="Your industry\'s recruitment marketing costs compared to peer '
        'industries and the all-industry average. Helps identify relative '
        'competitiveness and budget calibration.",'
    )
    table_idx = src.index("peer_industries = {")
    assert header_idx < table_idx, "sanity: expected ordering not found"
    between = src[header_idx:table_idx]
    assert "LEGACY DATA, not refreshed since {_bench_year_label}" in between, (
        "no legacy disclosure was inserted between the second table's "
        "description and its data -- it would still render with zero "
        "citation of its own"
    )


def test_generator_still_wired_at_the_documented_app_py_call_site():
    """Structural pin: app.py still imports generate_excel from this exact
    module at the documented single-source call site, and still uses it as
    the sync-path runtime-exception fallback for excel_v2. If this import
    line or the fallback wiring moves, the reachability finding in this
    file's module docstring needs re-verification, not silent staleness."""
    app_src = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
    assert "from archive.excel_legacy import generate_excel" in app_src
    assert "excel_v2 failed, falling back to legacy" in app_src, (
        "the sync-path runtime-exception fallback to the legacy generator "
        "appears to have been removed or reworded -- re-verify reachability"
    )


def test_generate_excel_still_crashes_on_a_separate_pre_existing_bug():
    """Documents (does not fix) the separate, pre-existing NameError bug
    this fix's disclosure text is currently blocked behind, so nobody
    mistakes this containment commit for having made the legacy generator
    functional. Confirms the crash happens at ``load_channels_db`` -- i.e.
    at line ~83, more than 1000 lines before any code this commit touches
    -- so this fix's own correctness does not depend on that bug being
    fixed. If this test ever fails because generate_excel() stops raising
    here, that's good news (someone fixed the separate bug) -- update or
    remove this test rather than being alarmed."""
    import importlib

    excel_legacy = importlib.import_module("archive.excel_legacy")
    # Ensure a clean-room NameError (not an artifact of a previous test in
    # this process having monkeypatched the module).
    for _name in (
        "load_channels_db",
        "load_joveo_publishers",
        "global_supply_data",
        "research",
    ):
        assert not hasattr(excel_legacy, _name), (
            f"archive.excel_legacy already has {_name!r} defined -- either "
            "the separate systemic bug was fixed (update this test) or a "
            "prior test in this process leaked a monkeypatch"
        )
    try:
        excel_legacy.generate_excel(
            {
                "client_name": "Acme Corp",
                "industry": "healthcare_medical",
                "locations": ["United States"],
                "roles": ["Registered Nurse"],
                "budget": "100000",
                "hire_volume": "20",
            }
        )
    except NameError as exc:
        assert "load_channels_db" in str(exc)
        return
    raise AssertionError(
        "generate_excel() no longer crashes on the separate pre-existing "
        "load_channels_db NameError -- if that bug was actually fixed, "
        "great, but this test needs to be updated/removed to reflect it "
        "rather than silently passing for the wrong reason"
    )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
