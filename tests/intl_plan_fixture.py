"""Shared plan builders for the international plan-number tests.

``_bundle(key)`` builds one headless plan (the pipeline ``/api/generate`` runs,
minus live enrichment) and returns its data, workbook and, on request, the deck
text.  ``SPECS`` names the markets the tests use; each is paired with a US plan
that must stay as it was.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402
from pptx import Presentation  # noqa: E402

import data_synthesizer  # noqa: E402
import excel_v2  # noqa: E402
import gold_standard as gs  # noqa: E402
import ppt_generator  # noqa: E402
import research  # noqa: E402
import tools_regen_bundles as T  # noqa: E402

SPECS: Dict[str, Tuple[str, str, str]] = {
    "vn": ("$80,000", "Ho Chi Minh City, Vietnam", "general_entry_level"),
    "in": ("₹8,000,000", "Bengaluru, India", "general_entry_level"),
    "jp": ("¥12,000,000", "Tokyo, Japan", "general_entry_level"),
    "fr": ("€100,000", "Paris, France", "retail_consumer"),
    "uk": ("£60,000", "Manchester, UK", "healthcare_medical"),
    "ca": ("C$90,000", "Toronto, ON", "general_entry_level"),
    "ae": ("AED 300,000", "Dubai, UAE", "retail_consumer"),
    "us": ("$150,000", "Dallas, TX", "healthcare_medical"),
}

_CACHE: Dict[Tuple[str, bool], Dict[str, Any]] = {}


def _bundle(key: str, deck: bool = False) -> Dict[str, Any]:
    """{"data", "wb" (openpyxl), "deck_text"} for one SPECS plan; cached."""
    cached = _CACHE.get((key, deck))
    if cached is not None:
        return cached
    budget, location, industry = SPECS[key]
    brief = {
        "client_name": "Test Co",
        "budget": budget,
        "campaign_duration": "3 months",
        "industry": industry,
        "locations": [location],
        "roles": ["Agent"],
        "target_roles": [{"title": "Agent", "count": 5}],
        "hire_volume": "50-100 hires",
    }
    data = T.build_plan_data(brief)
    # kb_loader omits the SlotOps summary app.py's own loader indexes; the
    # LinkedIn benchmark gate (gold_standard Gate 8) reads it from here
    data["_knowledge_base"]["slotops_benchmarks"] = json.loads(
        (PROJECT_ROOT / "data" / "slotops_benchmarks_summary.json").read_text(
            encoding="utf-8"
        )
    )
    data["_synthesized"] = data_synthesizer.synthesize({}, {}, data)
    data["_gold_standard"] = gs.apply_all_quality_gates(dict(data))
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research)
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    out: Dict[str, Any] = {
        "data": data,
        "wb": openpyxl.load_workbook(io.BytesIO(xlsx)),
        "deck_text": "",
    }
    if deck:
        pptx = ppt_generator.generate_pptx(dict(data))
        pptx = pptx.getvalue() if hasattr(pptx, "getvalue") else pptx
        out["deck_text"] = _deck_text(pptx)
    _CACHE[(key, deck)] = out
    return out


def _deck_text(pptx: bytes) -> str:
    parts: List[str] = []

    def walk(shapes) -> None:
        for sh in shapes:
            if sh.shape_type == 6:  # group
                walk(sh.shapes)
                continue
            if sh.has_text_frame and sh.text_frame.text.strip():
                parts.append(sh.text_frame.text)
            if getattr(sh, "has_table", False) and sh.has_table:
                for r in sh.table.rows:
                    parts.append(" ; ".join(c.text for c in r.cells))

    for slide in Presentation(io.BytesIO(pptx)).slides:
        walk(slide.shapes)
        if slide.has_notes_slide:
            parts.append(slide.notes_slide.notes_text_frame.text)
    return "\n".join(parts)


def _cells(wb, sheet: str) -> List[Any]:
    if sheet not in wb.sheetnames:
        return []
    return [c for row in wb[sheet].iter_rows() for c in row if c.value is not None]


def _sheet_text(wb, sheet: str) -> str:
    return "\n".join(str(c.value) for c in _cells(wb, sheet))


def _workbook_text(wb) -> str:
    return "\n".join(_sheet_text(wb, name) for name in wb.sheetnames)


def _row_cells(ws, label: str, col: int = 2):
    """The cells of the first row whose column ``col`` equals ``label``."""
    for row in ws.iter_rows():
        for c in row:
            if c.column == col and c.value == label:
                return [
                    ws.cell(row=c.row, column=k) for k in range(1, ws.max_column + 1)
                ]
    return []
