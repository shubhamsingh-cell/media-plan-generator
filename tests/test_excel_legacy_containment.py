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
   vintage (2023).
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
    pinned literals reflecting the true (2023) vintage."""
    src = _src()
    assert "_current_year = 2023" in src, (
        "_current_year is no longer pinned to the figures' true vintage"
    )
    assert '_bench_year_label = "2023"' in src, (
        "_bench_year_label is no longer pinned to the figures' true vintage"
    )
    assert "_current_year = datetime.date.today().year" not in src, (
        "the old date-derived _current_year computation is still present -- "
        "it would silently re-imply 'current year' sourcing again"
    )
    assert '_bench_year_label = f"{_current_year - 1}-{_current_year}"' not in src, (
        "the old date-derived _bench_year_label computation is still present"
    )


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
