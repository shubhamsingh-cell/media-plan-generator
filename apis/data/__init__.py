"""apis.data -- bulk recruitment data integrations (S50 -- May 2026).

This sub-package hosts adapters for free recruitment / labour-market datasets
that ship as bulk dumps (CSV/JSON) rather than per-request HTTP APIs.

Two access tiers for the SO Survey:

A. **Fast path (production-default, ~130 KB resident)**
   ``so_survey_consumer`` consumes a pre-aggregated JSON produced by
   ``scripts/aggregate_so_survey_2025.py``. Use these for hot-path Nova
   chatbot calls -- O(1) memory, sub-millisecond lookups.

B. **Full-fidelity path (~70 MB resident, lazy-loaded)**
   ``stack_overflow_survey`` lazy-loads the raw 49k-row CSV into an
   in-memory index. Use for ad-hoc analytical queries that need
   per-respondent slices the JSON aggregate doesn't pre-compute.

Lightcast Open Skills (``lightcast_skills``) is a smart loader: it returns
graceful "data not available" errors until the user manually drops the
Lightcast CSV/JSON into ``data/lightcast_open_skills.{csv,json}``
(Lightcast gates downloads behind their request-access form).

Coding contract: stdlib only (no pandas), type hints, specific exceptions,
graceful degradation on download/parse failures (returns
``{"error": ..., "source": ...}`` rather than raising at the boundary).
"""

from __future__ import annotations

# Production-fast pre-aggregated JSON consumers (~130 KB resident, O(1) lookups)
from apis.data.so_survey_consumer import (
    so_survey_status,
    so_survey_top_languages,
    so_survey_top_ai_models,
    so_survey_salary,
    so_survey_country_count,
)

__all__ = [
    # Fast path -- pre-aggregated JSON
    "so_survey_status",
    "so_survey_top_languages",
    "so_survey_top_ai_models",
    "so_survey_salary",
    "so_survey_country_count",
    # Full-fidelity path -- lazy-loaded CSV (import as `apis.data.stack_overflow_survey`)
    "stack_overflow_survey",
    # Lightcast smart loader (import as `apis.data.lightcast_skills`)
    "lightcast_skills",
]
