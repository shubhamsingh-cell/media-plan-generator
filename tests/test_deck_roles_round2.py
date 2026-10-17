"""Slide 5 / slide 8 trust and layout fixes (design-judge round 2).

DJ5  slide 8 withholds hires like slides 2 and 6 do on a no-local-CPH market
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pytest
from pptx import Presentation
from pptx.util import Inches

import data_synthesizer
import ppt_generator
import tools_regen_bundles as T
from kb_loader import load_knowledge_base

logging.disable(logging.CRITICAL)

TODAY = date.today().isoformat()

# The deck receives the LEGACY industry key from app.py; that key (not the
# normalised "healthcare_medical") is what routes slide 5 through the generic
# ad-platform range layer that used to stamp confidence "live_api".
US_TWO_ROLE = {
    "client_name": "Lone Star Health Network",
    "industry": "healthcare",
    "budget": "$250,000",
    "campaign_duration": "3 months",
    "hire_volume": "100 hires",
    "work_environment": "onsite",
    "locations": ["Houston, TX", "Phoenix, AZ"],
    "roles": ["Registered Nurse", "Travel Nurse"],
    "target_roles": ["Registered Nurse", "Travel Nurse"],
}
UK_WAREHOUSE = {
    "client_name": "Northern Fulfilment Group",
    "industry": "logistics",
    "budget": "£400,000",
    "campaign_duration": "3 months",
    "hire_volume": "100 hires",
    "work_environment": "onsite",
    "locations": ["Manchester, United Kingdom"],
    "roles": ["Warehouse Associate"],
    "target_roles": ["Warehouse Associate"],
}
US_ONE_MARKET = {
    "client_name": "Buckeye Logistics",
    "industry": "logistics",
    "budget": "$150,000",
    "campaign_duration": "3 months",
    "hire_volume": "20 hires",
    "work_environment": "onsite",
    "locations": ["Columbus, OH"],
    "roles": ["Warehouse Associate"],
    "target_roles": ["Warehouse Associate"],
}


def _plan(brief: Dict[str, Any], observed: bool, extra_jm: Optional[dict] = None):
    data = T.build_plan_data(brief)
    # legacy industry key / label, as app.py hands them to the deck
    legacy = {"healthcare_medical": "healthcare", "logistics_supply_chain": "logistics"}
    data["industry"] = legacy.get(data["industry"], data["industry"])
    data["industry_label"] = data["industry"].title()
    roles = brief["roles"]
    if observed:
        jm = {
            role: {
                "posting_count": 7000,
                "avg_salary": 52000,
                "competition": "medium",
                "source": "Adzuna",
                "posting_observation": "observed",
                "posting_provider_id": "adzuna",
                "posting_observed_at": TODAY,
                **(extra_jm or {}),
            }
            for role in roles
        }
    else:
        import api_enrichment

        jm = api_enrichment._adzuna_benchmark_fallback(roles, brief["locations"])
    enriched = {"job_market": jm}
    data["_enriched"] = enriched
    data["_synthesized"] = data_synthesizer.synthesize(
        enriched, data.get("_knowledge_base") or load_knowledge_base(), data
    )
    if extra_jm:
        # posting_scope is read from the synthesized role block
        for role_block in data["_synthesized"]["job_market_demand"].values():
            if isinstance(role_block, dict):
                role_block.update(extra_jm)
    return data


def _deck(data: dict) -> Presentation:
    return Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(data))))


def _slide(prs: Presentation, title: str):
    for slide in prs.slides:
        texts = [s.text_frame.text for s in slide.shapes if s.has_text_frame]
        if title in texts:
            return slide
    raise AssertionError(f"no slide titled {title!r}")


def _slide5(prs):
    return _slide(prs, "Channel Strategy & Investment")


def _slide8(prs):
    return _slide(prs, "Plan Comparison & Implementation")


def _shapes_with_text(slide) -> List[Any]:
    return [s for s in slide.shapes if s.has_text_frame and s.text_frame.text.strip()]


def _run_rgb(shape) -> Optional[Any]:
    for para in shape.text_frame.paragraphs:
        for run in para.runs:
            try:
                return run.font.color.rgb
            except (AttributeError, TypeError):
                return None
    return None


def _inch(shape, attr: str) -> float:
    return getattr(shape, attr) / 914400.0


# ---------------------------------------------------------------------- DJ5


def _value_after(slide, label: str, *, left_panel: bool = True) -> Optional[str]:
    shapes = _shapes_with_text(slide)
    for i, shape in enumerate(shapes):
        on_left = _inch(shape, "left") < 6.5
        if shape.text_frame.text == label and on_left == left_panel:
            return shapes[i + 1].text_frame.text
    return None


def test_slide8_withholds_hires_on_a_market_with_no_local_cph() -> None:
    prs = _deck(_plan(UK_WAREHOUSE, observed=False))
    slide8 = _slide8(prs)
    shown = _value_after(slide8, "Projected Hires")
    assert shown is not None, "fixture must render the Projected Hires row"
    assert not re.search(r"\d", shown), shown
    assert shown == "n/a"
    # slides 2 and 6 say the same thing
    all2 = " ".join(s.text_frame.text for s in _shapes_with_text(_slide_by_index(prs, 2)))
    assert "n/a" in all2
    all6 = " ".join(s.text_frame.text for s in _shapes_with_text(_slide_by_index(prs, 6)))
    assert "hires not projected" in all6
    # and no client-goal band computed from a count that is not projected
    assert "this plan projects" not in " ".join(
        s.text_frame.text for s in _shapes_with_text(slide8)
    )


def _slide_by_index(prs, n: int):
    return list(prs.slides)[n - 1]


def test_slide8_still_projects_hires_when_a_local_benchmark_exists() -> None:
    slide8 = _slide8(_deck(_plan(US_ONE_MARKET, observed=False)))
    shown = _value_after(slide8, "Projected Hires")
    assert shown is not None
    assert re.fullmatch(r"[\d,]+", shown.strip()), shown
