"""A BLS wage result that carries its provenance, for tests that stub
``api_enrichment.fetch_salary_data``.

Since the 2026-10-18 refresh fix a wage is written only when its series IDs are
the national wage series of the SOC it names and it carries its reference
year/period (``bls_oews.guard_refreshed_wages``), so a bare
``{"median": 82000}`` stub is rightly refused.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import bls_oews


def wage(
    soc: str,
    median: int,
    p10: Optional[int] = None,
    p90: Optional[int] = None,
    mean: Optional[int] = None,
    year: int = 2025,
) -> Dict[str, Any]:
    """The dict ``fetch_salary_data`` returns for one role, built through the
    same parser the live path uses."""
    ids = bls_oews.wage_series_ids(soc)
    fields = {"median": median, "p10": p10, "p90": p90, "mean": mean}
    series = [
        {
            "seriesID": ids[field],
            "data": [{"year": str(year), "period": "A01", "value": str(value)}],
        }
        for field, value in fields.items()
        if value is not None
    ]
    result, reason = bls_oews.build_wage_result(soc, series)
    assert result is not None, reason
    return result
