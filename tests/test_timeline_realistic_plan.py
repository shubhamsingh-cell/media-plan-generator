"""Timeline cards on a realistic plan keep their plan-specific bullets.

Design review 2026-11-03 (near-miss veto): on a realistic 4-role Germany
plan the comparison panel leaves each phase card one bullet of room, and the
CPQA-first fit loop replaced "Campaign setup & publisher activation for
<client>" and "Scale top performers: <channels>" with generic CPQA lines.
The fit loop now shortens plan-specific bullets at a clause break before it
drops anything, keeps them over the CPQA lines, and the Phase 2 "scale"
bullet carries the CPQA steering itself.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest
from pptx import Presentation

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import ppt_generator  # noqa: E402
import tools_regen_bundles as T  # noqa: E402

CLIENT = "Test Automotive Co"


@pytest.fixture(scope="module")
def cards():
    data = T.build_plan_data(
        {
            "client_name": CLIENT,
            "budget": "€250,000",
            "campaign_duration": "3 months",
            "campaign_start_month": 10,
            "industry": "automotive",
            "locations": ["Berlin, Germany"],
            "roles": [
                "Maintenance Technician", "Battery Supervisor",
                "Drive Unit Supervisor", "Industrial Painter",
            ],
            "target_roles": [
                {"title": t, "count": 5}
                for t in ("Maintenance Technician", "Battery Supervisor",
                          "Drive Unit Supervisor", "Industrial Painter")
            ],
            "hire_volume": "50-100 hires",
        }
    )
    pptx = ppt_generator.generate_pptx(dict(data))
    pptx = pptx.getvalue() if hasattr(pptx, "getvalue") else pptx
    for slide in Presentation(io.BytesIO(pptx)).slides:
        texts = [sh.text_frame.text for sh in slide.shapes if sh.has_text_frame]
        if "PHASE 2" in texts:
            return [t for t in texts if t.startswith("✓")]
    pytest.fail("no timeline slide")


def test_phase_1_names_the_client(cards):
    assert CLIENT in cards[0], cards


def test_phase_2_scales_named_channels_steered_by_cpqa(cards):
    assert "Scale top performers:" in cards[1], cards
    assert "CPQA" in cards[1], cards


def test_phase_3_keeps_the_cpqa_reallocation(cards):
    assert "CPQA" in cards[2], cards
