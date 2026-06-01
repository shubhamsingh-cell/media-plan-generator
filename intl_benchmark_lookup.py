"""International role/country benchmark lookup for media plan generation.

Thin reader over ``data/intl_role_benchmarks_v1.json`` (built in chatbot session
S76-S79b -- 5 verticals x 15 countries x role levels with cited CPA/CPH/apply
rate/time-to-fill data). Mirrors the lazy-load pattern from
``benchmark_registry.py`` so plan-gen pays the parse cost at most once per
worker, on first access.

Usage:
    from intl_benchmark_lookup import get_role_country_benchmarks
    bench = get_role_country_benchmarks("healthcare", "United Kingdom")
    if bench:
        cpa_med = bench["cpa_cost_per_applicant"]["median"]
        currency = bench["cpa_cost_per_applicant"]["currency"]
        cpa_usd = bench["cpa_cost_per_applicant"].get("median_usd")
        sources = bench["cpa_cost_per_applicant"].get("source_ids", [])

Returns ``None`` when the (industry, country) pair has no match -- callers MUST
fall back to existing channel-average / TBD logic. Never raises on data issues;
errors are logged and the function returns ``None``.

The dataset is read-only from plan-gen's perspective. The chatbot owns writes
and may extend keys; this module's accessors use ``.get(...)`` so additive
schema changes never break plan generation.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATA_PATH: Path = Path(__file__).parent / "data" / "intl_role_benchmarks_v1.json"

# Industry label -> dataset vertical key.
# Dataset verticals: healthcare_nursing, technology, blue_collar, hospitality,
# finance. Keys here are lowercase, single-token-friendly.
_INDUSTRY_TO_VERTICAL: dict[str, str] = {
    # healthcare_nursing
    "healthcare": "healthcare_nursing",
    "health care": "healthcare_nursing",
    "healthcare_nursing": "healthcare_nursing",
    "nursing": "healthcare_nursing",
    "nurse": "healthcare_nursing",
    "medical": "healthcare_nursing",
    "hospital": "healthcare_nursing",
    "clinical": "healthcare_nursing",
    # technology
    "tech": "technology",
    "technology": "technology",
    "software": "technology",
    "engineering": "technology",
    "it": "technology",
    "saas": "technology",
    # blue_collar
    "blue collar": "blue_collar",
    "blue_collar": "blue_collar",
    "warehouse": "blue_collar",
    "logistics": "blue_collar",
    "manufacturing": "blue_collar",
    "skilled trades": "blue_collar",
    "trades": "blue_collar",
    "construction": "blue_collar",
    "transport": "blue_collar",
    "trucking": "blue_collar",
    "driver": "blue_collar",
    # hospitality
    "hospitality": "hospitality",
    "restaurant": "hospitality",
    "food service": "hospitality",
    "hotel": "hospitality",
    "qsr": "hospitality",
    "retail": "hospitality",
    # finance
    "finance": "finance",
    "financial services": "finance",
    "banking": "finance",
    "insurance": "finance",
    "accounting": "finance",
}

# Country name -> dataset country key (lowercase slug).
# Dataset countries: us, uk, india, germany, france, canada, australia,
# netherlands, spain, brazil, mexico, singapore, uae, japan, ireland.
_COUNTRY_TO_SLUG: dict[str, str] = {
    # US
    "us": "us",
    "usa": "us",
    "u.s.": "us",
    "u.s.a.": "us",
    "united states": "us",
    "united states of america": "us",
    "america": "us",
    # UK
    "uk": "uk",
    "u.k.": "uk",
    "united kingdom": "uk",
    "great britain": "uk",
    "britain": "uk",
    "england": "uk",
    "gb": "uk",
    "gbr": "uk",
    # India
    "in": "india",
    "ind": "india",
    "india": "india",
    "bharat": "india",
    # Germany
    "de": "germany",
    "deu": "germany",
    "germany": "germany",
    "deutschland": "germany",
    # France
    "fr": "france",
    "fra": "france",
    "france": "france",
    # Canada
    "ca": "canada",
    "can": "canada",
    "canada": "canada",
    # Australia
    "au": "australia",
    "aus": "australia",
    "australia": "australia",
    # Netherlands
    "nl": "netherlands",
    "nld": "netherlands",
    "netherlands": "netherlands",
    "holland": "netherlands",
    "the netherlands": "netherlands",
    # Spain
    "es": "spain",
    "esp": "spain",
    "spain": "spain",
    "espana": "spain",
    # Brazil
    "br": "brazil",
    "bra": "brazil",
    "brazil": "brazil",
    "brasil": "brazil",
    # Mexico
    "mx": "mexico",
    "mex": "mexico",
    "mexico": "mexico",
    # Singapore
    "sg": "singapore",
    "sgp": "singapore",
    "singapore": "singapore",
    # UAE
    "ae": "uae",
    "are": "uae",
    "uae": "uae",
    "u.a.e.": "uae",
    "united arab emirates": "uae",
    "emirates": "uae",
    "dubai": "uae",
    "abu dhabi": "uae",
    # Japan
    "jp": "japan",
    "jpn": "japan",
    "japan": "japan",
    "nippon": "japan",
    # Ireland
    "ie": "ireland",
    "irl": "ireland",
    "ireland": "ireland",
    "republic of ireland": "ireland",
}

# ---------------------------------------------------------------------------
# Lazy loader
# ---------------------------------------------------------------------------

_data: dict[str, Any] = {}
_loaded: bool = False


def _load() -> dict[str, Any]:
    """Load and cache the intl benchmarks JSON. Returns {} on any failure."""
    global _data, _loaded
    if _loaded:
        return _data
    _loaded = True
    try:
        if _DATA_PATH.exists():
            with open(_DATA_PATH, "r", encoding="utf-8") as f:
                _data = json.load(f)
            verticals = _data.get("verticals") or {}
            logger.info(
                "Loaded intl role benchmarks from %s (%d verticals)",
                _DATA_PATH.name,
                len(verticals),
            )
        else:
            logger.warning(
                "intl_role_benchmarks_v1.json not found at %s -- "
                "intl lookups will return None",
                _DATA_PATH,
            )
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load intl role benchmarks: %s", exc, exc_info=True)
        _data = {}
    return _data


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _normalize_industry(industry: str | None) -> str | None:
    """Map a free-form industry label to a dataset vertical key, or None."""
    if not industry or not isinstance(industry, str):
        return None
    key = industry.strip().lower().replace("-", " ")
    # Direct hit
    if key in _INDUSTRY_TO_VERTICAL:
        return _INDUSTRY_TO_VERTICAL[key]
    # Substring fallback: longest alias first (e.g. "blue collar" before "blue")
    for alias in sorted(_INDUSTRY_TO_VERTICAL, key=len, reverse=True):
        if alias in key:
            return _INDUSTRY_TO_VERTICAL[alias]
    return None


def _normalize_country(country: str | None) -> str | None:
    """Map a free-form country label to a dataset country slug, or None."""
    if not country or not isinstance(country, str):
        return None
    key = country.strip().lower()
    # Strip common city-prefix patterns: "London, UK" -> "uk"
    if "," in key:
        # Try the rightmost token first (likely country)
        last = key.rsplit(",", 1)[-1].strip()
        if last in _COUNTRY_TO_SLUG:
            return _COUNTRY_TO_SLUG[last]
    if key in _COUNTRY_TO_SLUG:
        return _COUNTRY_TO_SLUG[key]
    # Substring fallback ONLY for aliases >= 5 chars to avoid spurious
    # matches like "antarctica" matching the "ca" Canada alias.
    for alias in sorted(_COUNTRY_TO_SLUG, key=len, reverse=True):
        if len(alias) >= 5 and alias in key:
            return _COUNTRY_TO_SLUG[alias]
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_role_country_benchmarks(
    industry: str | None, country: str | None
) -> dict[str, Any] | None:
    """Return the per-country benchmark block for (industry, country).

    Args:
        industry: Free-form industry label (e.g. "Healthcare", "tech",
            "blue collar"). Mapped to one of 5 dataset verticals.
        country: Free-form country label (e.g. "United Kingdom", "UK",
            "London, UK"). Mapped to one of 15 dataset country slugs.

    Returns:
        The dict at ``verticals.<vertical>.by_country.<slug>`` -- contains keys
        like ``country_label``, ``currency``, ``primary_roles``,
        ``annual_salary``, ``cpa_cost_per_applicant``, ``cph_cost_per_hire``,
        ``apply_rate_pct``, ``time_to_fill_days``, ``hiring_difficulty``,
        ``top_platforms``, ``market_notes``, ``data_gaps``. Returns ``None`` if
        either input does not map or the data file is unavailable. Never
        raises.
    """
    vertical = _normalize_industry(industry)
    if not vertical:
        return None
    slug = _normalize_country(country)
    if not slug:
        return None
    data = _load()
    try:
        block = (
            data.get("verticals", {}).get(vertical, {}).get("by_country", {}).get(slug)
        )
    except AttributeError:
        # Defensive: malformed nested types
        return None
    if not isinstance(block, dict):
        return None
    return block


def get_cpa_median_usd(industry: str | None, country: str | None) -> float | None:
    """Convenience: median CPA in USD for (industry, country), or None.

    Falls back through ``median_usd`` -> ``median`` (currency-noted) -> midpoint
    of (low_usd, high_usd) -> ``None``.
    """
    block = get_role_country_benchmarks(industry, country)
    if not block:
        return None
    cpa = block.get("cpa_cost_per_applicant") or {}
    if not isinstance(cpa, dict):
        return None
    # Prefer USD-normalized fields
    for key in ("median_usd", "median"):
        val = cpa.get(key)
        if isinstance(val, (int, float)) and val > 0:
            currency = cpa.get("currency") or ""
            # Only return raw "median" if currency is USD (avoid mixing units)
            if key == "median" and currency.upper() not in ("", "USD"):
                continue
            return float(val)
    low = cpa.get("low_usd")
    high = cpa.get("high_usd")
    if isinstance(low, (int, float)) and isinstance(high, (int, float)):
        return float((low + high) / 2.0)
    return None


def get_top_platforms(industry: str | None, country: str | None) -> list[str]:
    """Convenience: top recruitment platforms for (industry, country)."""
    block = get_role_country_benchmarks(industry, country)
    if not block:
        return []
    platforms = block.get("top_platforms") or []
    if not isinstance(platforms, list):
        return []
    # Each entry may be a string or a dict {"name": "...", "share": ...}
    out: list[str] = []
    for p in platforms:
        if isinstance(p, str) and p:
            out.append(p)
        elif isinstance(p, dict):
            name = p.get("name") or p.get("platform") or ""
            if isinstance(name, str) and name:
                out.append(name)
    return out


def is_available() -> bool:
    """True if the benchmark dataset is loaded and has at least one vertical."""
    data = _load()
    return bool((data.get("verticals") or {}))
