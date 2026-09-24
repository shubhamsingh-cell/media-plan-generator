"""US-only macro/holiday vocabulary markers, shared by bundle_qa.py (the
``us_data_on_non_us_plan`` linter rule) and excel_v2.py (the Workforce
Trends section writer, which must omit these markers at the SOURCE on a
plan with no US location rather than let bundle_qa catch them after the
fact).

Split out into its own dependency-free module (2026-09-24, DEFECT B fix)
specifically so excel_v2 can import it without dragging in bundle_qa.py's
own heavier imports, and so both call sites always agree on exactly which
strings count as "US-only" -- a single list, matched exactly, never
duplicated by hand in two places.
"""

from __future__ import annotations

import re

_US_ONLY_MARKERS: tuple[str, ...] = (
    "Fed Funds",
    "CPI Index",
    "BLS",
    "JOLTS",
    "Bls Sector Code",
    "Total Employment Us",
    "Job Openings Rate Jolts",
    "Quits Rate Jolts",
    "federal minimum wage",
    "Thanksgiving",
    "Memorial Day",
    "Spring break",
)

_US_ONLY_MARKER_RES: tuple[re.Pattern, ...] = tuple(
    re.compile(r"\b" + re.escape(m) + r"\b", re.IGNORECASE) for m in _US_ONLY_MARKERS
)


def has_us_only_marker(text: str) -> bool:
    """True if ``text`` contains any US-only marker (same word-bounded,
    case-insensitive match bundle_qa's ``us_data_on_non_us_plan`` rule
    uses)."""
    if not text:
        return False
    return any(pattern.search(text) for pattern in _US_ONLY_MARKER_RES)
