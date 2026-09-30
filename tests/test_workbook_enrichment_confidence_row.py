"""Regression test: the workbook's live-API provenance row uses the one metric.

VERIFIER FINDING (2026-10-01): the "Sources & Confidence" sheet's data-source
row ("Live market APIs (n/m responded)") computed its OWN ratio,
apis_succeeded / apis_called with "High" only at >= 60%. For a prod-shaped
enrichment (18 dispatched, 8 with data, 8 not applicable, 2 failed) it read
"Live market APIs (8/18 responded) | Medium" while the plan's enrichment
confidence (api_enrichment.enrichment_confidence: sources with data / sources
attempted and applicable) is 0.80 -- the same client deliverable stating two
different data-quality readings.

THE FIX: the row reports the applicable count and labels itself from
enrichment_confidence(summary), with the same 0.40 / 0.60 boundaries as
app.py's quality warning (>= 0.60 High, >= 0.40 Medium, else Low).

Drives the REAL generate_excel_v2 and reads the bytes back with openpyxl.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import openpyxl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import excel_v2  # noqa: E402
from tests.test_excel_provenance import _minimal_data  # noqa: E402


def _summary(n_called: int, n_ok: int, n_na: int) -> Dict[str, List[str]]:
    labels = [f"S{i}" for i in range(n_called)]
    return {
        "apis_called": labels,
        "apis_succeeded": labels[:n_ok],
        "apis_not_applicable": labels[n_ok : n_ok + n_na],
        "apis_skipped": labels[n_ok : n_ok + n_na],
        "apis_failed": labels[n_ok + n_na :],
    }


def _live_api_row(summary: Dict[str, Any]) -> Tuple[str, str]:
    """(source text, confidence label) of the live-API row, as rendered."""
    data = _minimal_data(_enriched={"enrichment_summary": summary})
    wb = openpyxl.load_workbook(io.BytesIO(excel_v2.generate_excel_v2(data)))
    ws = wb["Sources & Confidence"]
    for row in ws.iter_rows(values_only=True):
        cells = [c for c in row if c is not None]
        if cells and str(cells[0]).startswith("Live market APIs"):
            return str(cells[0]), str(cells[2])
    raise AssertionError("live-API provenance row not rendered")


@pytest.mark.parametrize(
    "n_called, n_ok, n_na, expected_source, expected_label",
    [
        # The verifier's prod shape: 8 of 10 applicable = 0.80.
        (18, 8, 8, "Live market APIs (8/10 applicable sources returned data)", "High"),
        # The telemetry run: 10 of 12 applicable = 0.833.
        (18, 10, 6, "Live market APIs (10/12 applicable sources returned data)", "High"),
        # 5 of 10 applicable = 0.50.
        (18, 5, 8, "Live market APIs (5/10 applicable sources returned data)", "Medium"),
        # 3 of 10 applicable = 0.30.
        (18, 3, 8, "Live market APIs (3/10 applicable sources returned data)", "Low"),
    ],
)
def test_live_api_row_uses_the_one_enrichment_metric(
    n_called: int, n_ok: int, n_na: int, expected_source: str, expected_label: str
) -> None:
    source, label = _live_api_row(_summary(n_called, n_ok, n_na))
    assert (source, label) == (expected_source, expected_label)
