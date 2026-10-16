"""Cost-per-hire yardsticks for the wizard's cost-per-hire read.

The live preview used to grade ``est_cph`` against a hand-typed per-role-class
table in the browser (``BENCH = {executive: 14000, senior: 6500, ...}``), and
later against per-industry quartiles in
``data/recruitment_benchmarks_2026_deep.json`` -> ``by_industry.<key>.cph_range_usd``.
Those quartiles are Nova working estimates typed in on 2026-04-24: no public
per-industry cost-per-hire quartiles exist (provenance audit 2026-10-17), so
each row is marked ``status: illustrative_unverified`` and ``lookup`` returns
None for it -- the wizard grades nothing against them.

The one verified figure is the SHRM 2025 US national average
(``cph_national_usd``, a mean, not a quartile), exposed by ``national``.
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
            registry = {
                str(s.get("id")): s
                for s in (data.get("metadata") or {}).get("sources") or []
                if s.get("id")
            }
            _cache = {
                "data": data,
                "by_industry": data.get("by_industry") or {},
                "registry": registry,
            }
        return _cache


def data() -> Dict[str, Any]:
    """The parsed benchmark file ({} when unreadable). One cached read shared by
    the estimate's cost-per-hire grade and /api/pricing/live."""
    return _load()["data"]


def source_label(source_ids: Iterable[str]) -> str:
    """The sources a data row lists, named as the file's own registry names
    them ("Appcast 2025, SHRM 2025"): publisher + year, in the row's order,
    every one of them. Never a single publisher standing in for the row."""
    registry = _load()["registry"]
    names = []
    for sid in source_ids:
        meta = registry.get(str(sid)) or {}
        # "Appcast (StepStone Group)" -> "Appcast": drop the parenthetical
        publisher = re.sub(r"\s*\([^)]*\)", "", str(meta.get("publisher") or "")).strip()
        year = meta.get("publication_year")
        label = f"{publisher} {year}".strip() if publisher else str(sid)
        if label not in names:
            names.append(label)
    return ", ".join(names)


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

    None for every row marked ``status: illustrative_unverified`` (all of the
    per-industry quartiles today): an unsourced figure is never a benchmark.
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
    if str(rng.get("status") or "") == "illustrative_unverified":
        return None
    source_ids = [str(i) for i in rng.get("source_ids") or []]
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
        "source": source_label(source_ids),
        "source_ids": source_ids,
    }


def national(currency_code: str = "USD") -> Optional[Dict[str, Any]]:
    """The SHRM 2025 US national average cost per hire (a mean), or None.

    ``{"mean", "statistic", "label", "source", "source_ids", "url", "year"}``.
    USD only, for the same reason as ``lookup``. None when the block or the
    figure is missing or not a positive number.
    """
    if (currency_code or "USD").upper() != "USD":
        return None
    block = _load()["data"].get("cph_national_usd") or {}
    value = block.get("non_executive_mean")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        return None
    source_ids = [str(i) for i in block.get("source_ids") or []] or ["shrm_cph_2025"]
    return {
        "mean": float(value),
        "statistic": "mean",
        "label": "SHRM 2025 US national average cost per hire (non-executive, all industries)",
        "source": source_label(source_ids),
        "source_ids": source_ids,
        "url": str(block.get("url") or ""),
        "year": block.get("publication_year") or 2025,
    }
