"""Regression test for campaign-duration resolution wiring in app.py.

Two generations of the same bug class:

  * S91 (agent A): app.py's campaign_weeks fallback for free-text "N
    months" durations had to go through
    ``display_format.parse_duration_to_weeks`` (52/12 weeks-per-month), not
    the old ``int(months) * 4`` formula. Verified defect (July-10 bundles):
    "18 months" silently became "17 months" because ``int(18) * 4 = 72``
    weeks, and the downstream canonical-duration label re-derived months
    from weeks via ``/4.33`` -- ``round(72 / 4.33) == 17``.
    ``display_format.parse_duration_to_weeks("18 months") == 78``, and
    ``round(78 / 4.33) == 18``, so the label round-trips correctly.

  * This wave: the S91 fix only patched the "N months" branch, leaving
    app.py with THREE independently maintained duration resolvers -- its
    own inline phrase ladder, a call out to
    ``display_format.parse_duration_to_weeks`` for the one branch S91
    touched, and a third, separate duration-LABEL formatter a few lines
    below (diverging from ``display_format.weeks_to_duration_label``: e.g.
    80 weeks read back as "1.5 years (~18 months)" in app.py's old closure
    but "18 months (~80 weeks)" via ``weeks_to_duration_label`` -- the
    latter is what every other regression test in this repo actually
    asserts). A real shipped bundle (Uber, campaign_duration="1-3 months")
    stated its duration FOUR different ways across the workbook and deck
    because of exactly this kind of drift. app.py's duration block now
    delegates BOTH the week count and the label to ONE shared function each
    in display_format.py (``resolve_campaign_weeks`` /
    ``resolve_campaign_duration_label``) -- the same functions excel_v2.py
    and ppt_generator.py delegate to -- so there is only one place left
    that can drift.

app.py's request handler is ~26k lines deep inside a single function (not a
standalone testable unit -- extracting the whole handler is out of scope
for this bounded change), so this test verifies the WIRING two ways:
    1. Source inspection: app.py's duration block calls
       ``display_format.resolve_campaign_weeks`` and
       ``display_format.resolve_campaign_duration_label`` -- never a
       locally re-implemented ladder or label formatter.
    2. ``app.display_format`` is the real module (not a stub/None) and
       produces the correct, non-buggy value for the exact string that
       shipped broken.

Runs under pytest, or standalone:
``python3 tests/test_app_duration_wiring.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import display_format  # noqa: E402


def _duration_block_source() -> str:
    """Extract the campaign_weeks/campaign_duration_canonical duration
    block from app.py's source (from the "Compute campaign_weeks" comment
    through the ``data["campaign_duration_canonical"]`` assignment)."""
    src = (PROJECT_ROOT / "app.py").read_text()
    start = src.index("# Compute campaign_weeks from campaign_duration")
    end = src.index("# ── Phase 0: Canonical Taxonomy Normalization", start)
    return src[start:end]


def test_campaign_weeks_delegates_to_shared_resolver():
    block = _duration_block_source()
    assert "display_format.resolve_campaign_weeks(" in block, (
        "app.py's campaign_weeks resolution no longer delegates to "
        "display_format.resolve_campaign_weeks -- a locally re-implemented "
        "ladder here can silently drift from excel_v2.py/ppt_generator.py "
        "again (the S91 bug class)"
    )
    # The old inline phrase ladder / bare "N months" *4 formula must be
    # gone from this block -- not just guarded, gone -- since both are now
    # display_format.resolve_campaign_weeks's job.
    assert "int(mo_match.group(1)) * 4" not in block
    assert '"1-3 month" in dur_lower' not in block


def test_canonical_label_delegates_to_shared_resolver():
    block = _duration_block_source()
    assert "display_format.resolve_campaign_duration_label(" in block, (
        "app.py's campaign_duration_canonical no longer delegates to "
        "display_format.resolve_campaign_duration_label -- a locally "
        "re-implemented label formatter here can silently disagree with "
        "the one excel_v2.py/ppt_generator.py use (the exact bug this "
        "fixes: 80 weeks read back as '1.5 years (~18 months)' from "
        "app.py's old closure vs. '18 months (~80 weeks)' from "
        "display_format.weeks_to_duration_label)"
    )
    # The old local closure must be gone, not just unused.
    assert "def _canonical_duration_label(" not in block


def test_app_display_format_module_is_wired():
    assert app.display_format is not None, (
        "app.display_format failed to import -- duration parsing silently "
        "falls back to a 12-week default in production"
    )
    assert app.display_format is display_format


def test_18_months_resolves_to_78_weeks_not_72():
    """The exact shipped-broken input: '18 months' must resolve to 78
    weeks (round-trip safe with weeks_to_duration_label), not 72 (the old
    18*4 result that re-derived as '17 months')."""
    assert app.display_format.resolve_campaign_weeks("18 months") == 78
    assert app.display_format.parse_duration_to_weeks("18 months") == 78
    assert app.display_format.resolve_campaign_weeks("18 months") != 18 * 4


def test_80_weeks_label_matches_weeks_to_duration_label_not_a_local_variant():
    """80 weeks must read as "18 months (~80 weeks)" (display_format.
    weeks_to_duration_label's own convention, asserted by
    tests/test_channel_recommendations_reconcile.py::
    test_duration_is_consistent_across_sheets), never app.py's old local
    closure's "1.5 years (~18 months)" phrasing for the same week count."""
    label = app.display_format.weeks_to_duration_label(80)
    assert label == "18 months (~80 weeks)"
    assert "1.5 years" not in label


def test_wizard_duration_buckets_resolve_through_one_function():
    """Every one of app.py's original phrase-ladder buckets must still
    resolve to the SAME week counts now that they live in
    display_format.resolve_campaign_weeks instead of app.py's own inline
    ladder -- the refactor must not silently change any bucket's value."""
    cases = {
        "1-3 months": 12,
        "3-6 months": 24,
        "6-12 months": 48,
        "1-2 years": 80,
        "2-5 years": 156,
        "Long-term": 156,
        "Ongoing": 52,
        "4 weeks": 4,
        "18 months": 78,
    }
    for duration, expected_weeks in cases.items():
        assert app.display_format.resolve_campaign_weeks(duration) == expected_weeks, (
            f"{duration!r} resolved to "
            f"{app.display_format.resolve_campaign_weeks(duration)} weeks, "
            f"expected {expected_weeks}"
        )


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
