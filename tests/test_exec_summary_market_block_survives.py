"""Slide 2 keeps its cited market-data block on an App Install plan.

Design re-judge 2026-11-03 (near-veto): the App Install disclosure made the
protected goal line wrap, and the RESOLUTION card's height estimate (an
average-glyph count) over-counted so far that it trimmed the sourced "2026
Market Data" block while ~100px of card stayed empty. The estimate now uses
real Poppins advances; the block survives whenever the card has room, and
the slide stays collision-free.
"""

from __future__ import annotations

import io
import subprocess
import sys
from pathlib import Path

import pytest
from pptx import Presentation

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import ppt_generator  # noqa: E402
import tools_regen_bundles as T  # noqa: E402


@pytest.fixture(scope="module")
def deck_path(tmp_path_factory):
    # the re-judge's App Install brief: two roles, two US markets, no hire goal
    data = T.build_plan_data(
        {
            "client_name": "Contoso Delivery",
            "budget": "$250,000",
            "campaign_duration": "3 months",
            "campaign_start_month": 10,
            "industry": "logistics_supply_chain",
            "locations": ["Dallas, TX", "Houston, TX"],
            "roles": ["Delivery Driver", "Rider"],
            "target_roles": [
                {"title": "Delivery Driver", "count": 1},
                {"title": "Rider", "count": 1},
            ],
        }
    )
    data["campaign_goals"] = ["App Install"]
    pptx = ppt_generator.generate_pptx(dict(data))
    pptx = pptx.getvalue() if hasattr(pptx, "getvalue") else pptx
    path = tmp_path_factory.mktemp("deck") / "app_install.pptx"
    path.write_bytes(pptx)
    return path


def test_the_cited_market_block_survives_beside_the_disclosure(deck_path):
    slide = Presentation(str(deck_path)).slides[1]
    text = "\n".join(sh.text_frame.text for sh in slide.shapes if sh.has_text_frame)
    assert "App installs are not modeled" in text
    assert "2026 Market Data:" in text, text[-600:]
    assert "Appcast" in text, text[-600:]


def test_slide_2_has_no_collisions(deck_path):
    out = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "deck_qa" / "collide.py"),
         str(deck_path), "--slides", "2"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=120,
    )
    assert "TOTAL FINDINGS: 0" in out.stdout, out.stdout[-800:] + out.stderr[-400:]
