"""Cited 2026 industry-report lookup for media plan generation.

Thin reader over ``data/industry_reports_2026.json`` (80 verified 2026 reports
from Appcast, BLS, Eurostat, Indeed Hiring Lab, LinkedIn Workforce, ADP, etc.,
each carrying ``key_metrics[]`` with value/unit/source).

Plan-gen uses this to enrich slides with cited market stats (e.g. apply-rate,
time-to-fill) instead of leaving "TBD" placeholders. Read-only; chatbot owns
writes and may extend keys, so all accessors use defensive ``.get()``.

Usage:
    from industry_reports_lookup import get_cited_metrics_for_country
    cites = get_cited_metrics_for_country("United Kingdom", limit=2)
    for c in cites:
        # c = {"metric": "...", "value": ..., "unit": "...",
        #      "publisher": "...", "year": "2026", "source": "..."}
        print(f"{c['metric']}: {c['value']}{c['unit']} ({c['publisher']})")
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DATA_PATH: Path = Path(__file__).parent / "data" / "industry_reports_2026.json"

# Geography label normalization. The dataset uses ISO-2-letter-ish codes (US,
# UK, IN, DE) as well as "Global" / "EU-27". Plan-gen passes free-form country
# names. We normalize both sides to a set of acceptable aliases per ISO code.
_GEO_ALIASES: dict[str, set[str]] = {
    "US": {
        "us",
        "usa",
        "u.s.",
        "u.s.a.",
        "united states",
        "united states of america",
        "america",
    },
    "UK": {
        "uk",
        "u.k.",
        "united kingdom",
        "great britain",
        "britain",
        "england",
        "gb",
        "gbr",
    },
    "Canada": {"ca", "can", "canada"},
    "Australia": {"au", "aus", "australia"},
    "New Zealand": {"nz", "nzl", "new zealand"},
    "EU-27": {"eu", "eu-27", "europe", "european union"},
    "India": {"in", "ind", "india", "bharat"},
    "Germany": {"de", "deu", "germany", "deutschland"},
    "France": {"fr", "fra", "france"},
    "Spain": {"es", "esp", "spain", "espana"},
    "Netherlands": {"nl", "nld", "netherlands", "holland", "the netherlands"},
    "Singapore": {"sg", "sgp", "singapore"},
    "Indonesia": {"id", "idn", "indonesia"},
    "Malaysia": {"my", "mys", "malaysia"},
    "Philippines": {"ph", "phl", "philippines"},
    "Thailand": {"th", "tha", "thailand"},
    "Vietnam": {"vn", "vnm", "vietnam"},
    "Brazil": {"br", "bra", "brazil", "brasil"},
    "UAE": {
        "ae",
        "are",
        "uae",
        "u.a.e.",
        "united arab emirates",
        "emirates",
        "dubai",
        "abu dhabi",
    },
    "Global": {"global", "worldwide", "international", "world"},
}

_data: dict[str, Any] = {}
_loaded: bool = False


def _load() -> dict[str, Any]:
    """Load and cache the industry reports JSON. Returns {} on any failure."""
    global _data, _loaded
    if _loaded:
        return _data
    _loaded = True
    try:
        if _DATA_PATH.exists():
            with open(_DATA_PATH, "r", encoding="utf-8") as f:
                _data = json.load(f)
            reports = _data.get("reports") or []
            logger.info(
                "Loaded industry reports from %s (%d reports)",
                _DATA_PATH.name,
                len(reports),
            )
        else:
            logger.warning(
                "industry_reports_2026.json not found at %s -- "
                "cited-metric lookups will return empty",
                _DATA_PATH,
            )
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load industry reports: %s", exc, exc_info=True)
        _data = {}
    return _data


def _normalize_country_to_iso(country: str | None) -> str | None:
    """Map a free-form country name to one of the dataset geography keys."""
    if not country or not isinstance(country, str):
        return None
    key = country.strip().lower()
    # Try comma-separated last token first ("London, UK" -> "uk")
    if "," in key:
        last = key.rsplit(",", 1)[-1].strip()
        for iso, aliases in _GEO_ALIASES.items():
            if last in aliases:
                return iso
    for iso, aliases in _GEO_ALIASES.items():
        if key in aliases:
            return iso
    # Substring fallback ONLY for aliases >= 5 chars to avoid spurious
    # matches like "antarctica" matching the "ca" Canada alias.
    for iso, aliases in _GEO_ALIASES.items():
        for alias in sorted(aliases, key=len, reverse=True):
            if len(alias) >= 5 and alias in key:
                return iso
    return None


def get_cited_metrics_for_country(
    country: str | None,
    limit: int = 2,
    prefer_tiers: tuple[str, ...] = (
        "tier_1_government",
        "tier_2_platform_data",
    ),
) -> list[dict[str, Any]]:
    """Return up to ``limit`` cited 2026 metrics whose geography matches.

    Falls back to "Global" reports when no country-specific match exists.
    Prefers higher-tier sources (government / platform data) over surveys.

    Returns a list of dicts: ``{metric, value, unit, publisher, year, source,
    report_id, url}``. Empty list on miss or load failure. Never raises.
    """
    iso = _normalize_country_to_iso(country)
    data = _load()
    reports = data.get("reports") or []
    if not reports:
        return []

    out: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []

    for tier in prefer_tiers + ("",):
        # Tier "" means "any tier" -- last-resort pass
        for r in reports:
            if not isinstance(r, dict):
                continue
            if tier and r.get("tier") != tier:
                continue
            geos = r.get("geography") or []
            if not isinstance(geos, list):
                continue
            geo_match = iso in geos if iso else False
            global_match = (
                "Global" in geos or "Global (international benchmarks)" in geos
            )
            if not (geo_match or global_match):
                continue
            metrics = r.get("key_metrics") or []
            if not isinstance(metrics, list):
                continue
            published = r.get("published_date") or ""
            year = published.split("-", 1)[0] if "-" in published else published
            for m in metrics:
                if not isinstance(m, dict):
                    continue
                row = {
                    "metric": m.get("metric") or "",
                    "value": m.get("value"),
                    "unit": m.get("unit") or "",
                    "publisher": r.get("publisher") or "",
                    "year": year,
                    "source": m.get("source") or "",
                    "report_id": r.get("report_id") or "",
                    "url": r.get("url_primary") or r.get("url_press") or "",
                    "geo_match": geo_match,
                }
                if geo_match:
                    out.append(row)
                else:
                    fallback.append(row)
                if len(out) >= limit:
                    return out[:limit]
        if len(out) >= limit:
            return out[:limit]

    # Top up with global fallbacks if country-specific came up short
    while len(out) < limit and fallback:
        out.append(fallback.pop(0))
    return out[:limit]


def is_available() -> bool:
    data = _load()
    return bool(data.get("reports"))
