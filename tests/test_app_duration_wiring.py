"""Regression test for S91 (agent A): app.py's campaign_weeks fallback for
free-text "N months" durations must go through
``display_format.parse_duration_to_weeks`` (52/12 weeks-per-month), not the
old ``int(months) * 4`` formula.

Verified defect (July-10 bundles): "18 months" silently became "17 months"
because ``int(18) * 4 = 72`` weeks, and the downstream canonical-duration
label re-derives months from weeks via ``/4.33`` -- ``round(72 / 4.33) ==
17``. ``display_format.parse_duration_to_weeks("18 months") == 78``, and
``round(78 / 4.33) == 18``, so the label round-trips correctly.

app.py's duration ladder is ~26k lines deep inside a single request-handler
function (not a standalone testable unit -- extracting it is out of scope
for this bounded change), so this test verifies the WIRING two ways:
    1. Source inspection: the "N months" fallback branch in app.py calls
       ``display_format.parse_duration_to_weeks`` and no longer computes
       ``* 4`` as its primary path.
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


def _duration_ladder_source() -> str:
    """Extract the campaign_weeks duration-ladder block from app.py's
    source (from the "Compute campaign_weeks" comment through the
    ``data["campaign_weeks"] = campaign_weeks`` assignment)."""
    src = (PROJECT_ROOT / "app.py").read_text()
    start = src.index("# Compute campaign_weeks from campaign_duration")
    end = src.index('data["campaign_weeks"] = campaign_weeks', start)
    return src[start:end]


def test_month_fallback_calls_parse_duration_to_weeks():
    block = _duration_ladder_source()
    assert "display_format.parse_duration_to_weeks(" in block, (
        "app.py's 'N months' fallback branch no longer calls "
        "display_format.parse_duration_to_weeks -- duration parsing may "
        "have regressed to the buggy *4 formula"
    )


def test_month_fallback_primary_path_no_longer_bare_times_4():
    """The buggy ``int(mo_match.group(1)) * 4`` line may still exist as a
    defensive fallback (only reached if display_format failed to import),
    but it must be gated behind an ``if display_format is not None`` check
    -- never the unconditional primary path."""
    block = _duration_ladder_source()
    assert "mo_match = re.search" in block

    mo_idx = block.index("mo_match = re.search")
    parse_idx = block.index("display_format.parse_duration_to_weeks(")
    guard_idx = block.index("if display_format is not None:")

    # The parse_duration_to_weeks call and its is-not-None guard must both
    # appear AFTER the mo_match regex (i.e. inside that fallback branch),
    # and the guard must come before the call it protects.
    assert mo_idx < guard_idx < parse_idx

    times_4_idx = block.find("int(mo_match.group(1)) * 4")
    if times_4_idx != -1:
        # If the old formula still exists at all, it must be textually
        # AFTER the guarded parse_duration_to_weeks call (i.e. the "else"
        # defensive branch), never the unconditional primary path.
        assert times_4_idx > parse_idx


def test_app_display_format_module_is_wired():
    assert app.display_format is not None, (
        "app.display_format failed to import -- duration parsing silently "
        "falls back to the buggy *4 formula in production"
    )


def test_18_months_resolves_to_78_weeks_not_72():
    """The exact shipped-broken input: '18 months' must resolve to 78
    weeks (round-trip safe with weeks_to_duration_label), not 72 (the old
    18*4 result that re-derived as '17 months')."""
    assert app.display_format.parse_duration_to_weeks("18 months") == 78
    assert app.display_format.parse_duration_to_weeks("18 months") != 18 * 4


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
