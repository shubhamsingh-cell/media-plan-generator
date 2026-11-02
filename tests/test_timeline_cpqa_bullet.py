"""Phase 2's "steered by CPQA" bullet survives the timeline fit loop.

Handoff 2026-10-23 section 4 item 11: the Implementation Timeline's fit
loop dropped generic bullets first, and the CPQA bullets (the plan's quality
KPI, reviewer defect 4) counted as generic, so on a plan whose comparison
panel leaves the phase cards one bullet of room Phase 2 showed only "Scale
top performers" and the CPQA steering never rendered (seen on a local UK
plan 2026-11-03). Revised after design review the same day: plan-specific
bullets are kept first and Phase 2's "Scale top performers" bullet carries
"steered by CPQA" itself, so a one-bullet card shows both.
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS = os.path.dirname(os.path.abspath(__file__))
for _p in (PROJECT_ROOT, TESTS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ppt_generator as ppt  # noqa: E402
from test_ppt_w4b_bundle_quality import _all_slide_text, _base_plan, _new_prs  # noqa: E402


def _cards_with_room_for_one_bullet(monkeypatch):
    real = ppt._estimate_lines

    def _tall(text, *a, **k):
        # every timeline bullet needs 4 lines: one fits a card, two never do
        return 4 if str(text).startswith("✓") else real(text, *a, **k)

    monkeypatch.setattr(ppt, "_estimate_lines", _tall)


def _phase_bullets(prs):
    return [t for t in _all_slide_text(prs) if t.startswith("✓")]


def test_a_one_bullet_phase_2_card_keeps_the_cpqa_bullet(monkeypatch):
    _cards_with_room_for_one_bullet(monkeypatch)
    prs = _new_prs()
    ppt._build_slide_comparison_timeline(prs, _base_plan())
    cards = _phase_bullets(prs)
    assert len(cards) == 3
    assert all("\n" not in c for c in cards), cards  # one bullet per card
    assert "steered by CPQA" in cards[1], cards
    # plan-specific bullets outrank the CPQA lines (design review 2026-11-03):
    # Phase 1 keeps the client, Phase 2's scale bullet carries the CPQA steering
    assert "Manpower - Amerigas" in cards[0] and "CPQA" in cards[2], cards


def test_with_room_the_plan_specific_bullets_still_render():
    prs = _new_prs()
    ppt._build_slide_comparison_timeline(prs, _base_plan())
    blob = "\n".join(_phase_bullets(prs))
    assert "steered by CPQA" in blob
    assert "Scale top performers: Programmatic" in blob
