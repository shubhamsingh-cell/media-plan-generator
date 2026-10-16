"""Cost-per-hire quartiles for the wizard's "High / Efficient" read.

The live preview used to grade ``est_cph`` against a hand-typed per-role-class
table in the browser (``BENCH = {executive: 14000, senior: 6500, tech: 5000,
skilled: 3500, ...}``): nothing in the repo carried those numbers, and the
repo's SHRM-sourced data disagreed (nursing median $5,400 / p75 $7,800, tech
median $5,800 / p75 $9,500), so a $7K nurse hire was flagged "High".

``lookup`` returns the SHRM-sourced quartiles from
``data/recruitment_benchmarks_2026_deep.json`` -> ``by_industry.<key>.cph_range_usd``
for the plan's industry / roles, or ``None`` when there is no benchmark that
clearly fits (the wizard then grades nothing -- it never guesses).
``/api/estimate`` carries the result as ``cph_benchmark``; the preview
words "High" / "Efficient" only against it.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)

DATA_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data",
    "recruitment_benchmarks_2026_deep.json",
)

# Engine industry key (budget_engine / classify_industry legacy_key) ->
# by_industry key. EXPLICIT on purpose: an industry absent here has no
# benchmark that clearly fits, so nothing is graded (insurance rides with
# finance, as the data's own finance description says).
_INDUSTRY_TO_BENCHMARK: Dict[str, str] = {
    "healthcare_medical": "healthcare",
    "tech_engineering": "tech",
    "retail_consumer": "retail",
    "logistics_supply_chain": "logistics",
    "finance_banking": "finance",
    "insurance": "finance",
    "blue_collar_trades": "skilled_trades",
    "hospitality_travel": "hospitality",
    "government": "government",
    "education": "education",
    "pharma_biotech": "pharma_biotech",
}

# A role that names a narrower benchmark than its industry's.
_ROLE_OVERRIDES = (
    # RN / LPN / CNA / travel nurses are what the nursing row describes; a
    # nurse practitioner is not listed, so it falls back to the industry row.
    (
        re.compile(
            r"\bnurs(?:e|es|ing)\b(?!\s+practitioner)|\b(?:rn|lpn|cna)\b", re.I
        ),
        "nursing",
    ),
    # a physician assistant is not a physician
    (re.compile(r"\bphysicians?\b(?!\s+assistant)|\bsurgeon\b|\bmd\b", re.I), "physician"),
)

_lock = threading.Lock()
_cache: Optional[Dict[str, Any]] = None


def _load() -> Dict[str, Any]:
    global _cache
    with _lock:
        if _cache is None:
            try:
                with open(DATA_FILE, encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, ValueError):
                logger.error("cph_benchmark: %s unreadable", DATA_FILE, exc_info=True)
                data = {}
            years = sorted(
                {
                    str(s.get("publication_year"))
                    for s in (data.get("metadata") or {}).get("sources") or []
                    if str(s.get("id") or "").startswith("shrm_cph")
                    and s.get("publication_year")
                }
            )
            _cache = {
                "by_industry": data.get("by_industry") or {},
                "source": "SHRM cost-per-hire benchmarks"
                + (f" ({'/'.join(years)})" if years else ""),
            }
        return _cache


def benchmark_key(industry_key: str, role_titles: Iterable[str]) -> Optional[str]:
    """The by_industry key every role maps to, or None (unmapped / mixed)."""
    keys = set()
    base = _INDUSTRY_TO_BENCHMARK.get(industry_key or "")
    titles = [str(t) for t in role_titles if str(t).strip()]
    for title in titles or [""]:
        key = None
        for pattern, role_key in _ROLE_OVERRIDES:
            if pattern.search(title):
                key = role_key
                break
        keys.add(key or base)
    if len(keys) != 1:
        return None  # roles from different benchmarks: no single yardstick
    return next(iter(keys))


def lookup(
    industry_key: str, role_titles: Iterable[str], currency_code: str = "USD"
) -> Optional[Dict[str, Any]]:
    """``{"label", "p25", "median", "p75", "source"}`` in USD, or None.

    Only for USD plans: the figures are US dollars, and an FX-translated
    comparison would be a different claim.
    """
    if (currency_code or "USD").upper() != "USD":
        return None
    key = benchmark_key(industry_key, role_titles)
    if not key:
        return None
    store = _load()
    rng = ((store["by_industry"].get(key) or {}).get("cph_range_usd")) or {}
    figures = []
    for name in ("p25", "median", "p75"):
        value = rng.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            return None
        figures.append(float(value))
    return {
        "label": key.replace("_", " "),
        "p25": figures[0],
        "median": figures[1],
        "p75": figures[2],
        "source": store["source"],
    }
