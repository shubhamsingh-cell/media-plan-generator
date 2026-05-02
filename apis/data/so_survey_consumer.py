"""so_survey_consumer.py -- Stack Overflow 2025 Developer Survey lookup tools.

S50 (May 2026): Read-only consumer for the pre-aggregated JSON produced by
`scripts/aggregate_so_survey_2025.py`. Returns dicts shaped for Nova chatbot
tools.

Source: https://survey.stackoverflow.co/2025/  (Open Database License)
N: 49,191 respondents, 111 countries, 32 devtypes, 22,227 valid USD salaries.

Usage:
    from apis.data.so_survey_consumer import so_survey_salary
    so_survey_salary(country="United States of America", devtype="AI/ML engineer")
    # -> {"country": "...", "devtype": "...", "p25": 130000, "p50": 189500,
    #     "p75": 250000, "n": 80, "source": "Stack Overflow 2025 Developer Survey"}
"""

from __future__ import annotations

import functools
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_AGG_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data"
    / "so_survey_2025_aggregates.json"
)
_SOURCE = "Stack Overflow 2025 Developer Survey"


@functools.lru_cache(maxsize=1)
def _load() -> dict[str, Any]:
    """Load the pre-aggregated JSON once per process."""
    if not _AGG_PATH.exists():
        return {}
    try:
        with _AGG_PATH.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        logger.error("Failed to load SO Survey aggregates: %s", exc, exc_info=True)
        return {}


def _err(msg: str) -> dict[str, Any]:
    return {"error": msg, "source": _SOURCE}


def so_survey_status() -> dict[str, Any]:
    """Return aggregate metadata: row counts, generated_at, etc."""
    data = _load()
    if not data:
        return _err(
            "Aggregates not found. Run: python3 scripts/aggregate_so_survey_2025.py"
        )
    return {
        "available": True,
        "metadata": data.get("metadata") or {},
        "n_countries": len(data.get("country_counts") or {}),
        "n_devtypes": len(data.get("devtype_counts") or {}),
        "n_country_devtype_salary_buckets": len(
            data.get("salary_by_country_devtype_usd") or {}
        ),
        "source": _SOURCE,
    }


def so_survey_top_languages(*, limit: int = 10) -> dict[str, Any]:
    """Top languages by used / admired %."""
    data = _load()
    if not data:
        return _err("Aggregates not loaded")
    pairs = list((data.get("language_use") or {}).items())[: max(1, min(limit, 50))]
    return {
        "languages": [
            {
                "name": name,
                "used_pct": info.get("used_pct"),
                "admired_pct": info.get("admired_pct"),
                "n_used": info.get("n_used"),
            }
            for name, info in pairs
        ],
        "source": _SOURCE,
    }


def so_survey_top_ai_models(*, limit: int = 10) -> dict[str, Any]:
    """Top AI models by admired %."""
    data = _load()
    if not data:
        return _err("Aggregates not loaded")
    pairs = list((data.get("ai_models_admired") or {}).items())[
        : max(1, min(limit, 50))
    ]
    return {
        "ai_models": [
            {
                "name": name,
                "admired_pct": info.get("admired_pct"),
                "n": info.get("n"),
            }
            for name, info in pairs
        ],
        "source": _SOURCE,
    }


def _resolve_country_name(query: str, all_countries: list[str]) -> str | None:
    """Loose country match. Prefers exact then case-insensitive substring."""
    if not query:
        return None
    q = query.strip()
    if q in all_countries:
        return q
    q_low = q.lower()
    # Exact case-insensitive
    for c in all_countries:
        if c.lower() == q_low:
            return c
    # Substring (e.g., "United States" → "United States of America")
    for c in all_countries:
        if q_low in c.lower() or c.lower() in q_low:
            return c
    return None


def _resolve_devtype(query: str, all_devtypes: list[str]) -> str | None:
    """Loose devtype match. Substring case-insensitive."""
    if not query:
        return None
    q_low = query.strip().lower()
    for d in all_devtypes:
        if d.lower() == q_low:
            return d
    for d in all_devtypes:
        if q_low in d.lower():
            return d
    return None


def so_survey_salary(
    *,
    country: str | None = None,
    devtype: str | None = None,
) -> dict[str, Any]:
    """Median + IQR USD salary for a country, devtype, or country+devtype combo.

    Args:
        country: Country name (loose match -- "USA" → "United States of America").
        devtype: DevType (loose match -- "ML" → "AI/ML engineer").
        At least one of country or devtype must be provided.

    Returns:
        dict with p25/p50/p75/mean/n, plus the resolved country/devtype names.
        Returns {"error": ...} if no matching bucket has n >= 5 respondents.
    """
    data = _load()
    if not data:
        return _err("Aggregates not loaded")
    if not country and not devtype:
        return _err("Provide at least one of: country, devtype")

    countries = list((data.get("country_counts") or {}).keys())
    devtypes = list((data.get("devtype_counts") or {}).keys())

    resolved_country = _resolve_country_name(country or "", countries)
    resolved_devtype = _resolve_devtype(devtype or "", devtypes)

    if country and not resolved_country:
        return _err(
            f"No matching country for '{country}'. Try: "
            f"{', '.join(countries[:5])}..."
        )
    if devtype and not resolved_devtype:
        return _err(
            f"No matching devtype for '{devtype}'. Try: "
            f"{', '.join(devtypes[:5])}..."
        )

    if resolved_country and resolved_devtype:
        key = f"{resolved_country}|{resolved_devtype}"
        bucket = (data.get("salary_by_country_devtype_usd") or {}).get(key)
        if not bucket or bucket.get("n", 0) < 5:
            return _err(
                f"No salary data for {resolved_country} × {resolved_devtype} "
                f"with n>=5 respondents."
            )
        return {
            "country": resolved_country,
            "devtype": resolved_devtype,
            **bucket,
            "source": _SOURCE,
        }

    if resolved_country:
        bucket = (data.get("salary_by_country_usd") or {}).get(resolved_country)
        if not bucket:
            return _err(f"No salary data for country {resolved_country}")
        return {"country": resolved_country, **bucket, "source": _SOURCE}

    if resolved_devtype:
        bucket = (data.get("salary_by_devtype_usd") or {}).get(resolved_devtype)
        if not bucket:
            return _err(f"No salary data for devtype {resolved_devtype}")
        return {"devtype": resolved_devtype, **bucket, "source": _SOURCE}

    return _err("No matching bucket")


def so_survey_country_count(*, limit: int = 20) -> dict[str, Any]:
    """Top countries by respondent count."""
    data = _load()
    if not data:
        return _err("Aggregates not loaded")
    counts = list((data.get("country_counts") or {}).items())[: max(1, min(limit, 200))]
    return {
        "countries": [{"name": c, "n_respondents": n} for c, n in counts],
        "total_countries": len(data.get("country_counts") or {}),
        "source": _SOURCE,
    }
