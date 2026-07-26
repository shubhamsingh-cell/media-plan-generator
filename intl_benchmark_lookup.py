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


def _extract_salary_points(
    annual_salary: dict[str, Any],
) -> tuple[list[float], list[float], str | None, list[str]]:
    """Collect (local_values, usd_values, currency, source_ids) from a country's
    ``annual_salary`` block.

    Entries are heterogeneous: some are point estimates
    ``{value, currency, value_usd}`` and some are ranges
    ``{low, high, median, currency, low_usd, high_usd}``. Keys whose name
    contains "distorted" are skipped (the dataset flags Adzuna outliers).
    """
    local_vals: list[float] = []
    usd_vals: list[float] = []
    currency: str | None = None
    source_ids: list[str] = []
    for key, entry in annual_salary.items():
        if not isinstance(entry, dict):
            continue
        key_l = key.lower()
        # Skip Adzuna outliers (flagged "distorted") and monthly figures so we
        # don't mix monthly + annual into one range (e.g. JP ¥306,900/mo base
        # vs ¥4.8M/yr). We want annual ranges only.
        if "distorted" in key_l or "monthly" in key_l or "_mo_" in key_l:
            continue
        cur = entry.get("currency")
        if isinstance(cur, str) and cur and currency is None:
            currency = cur
        # Range entry
        for lk, uk in (("low", "low_usd"), ("high", "high_usd")):
            lv = entry.get(lk)
            if isinstance(lv, (int, float)) and not isinstance(lv, bool) and lv > 0:
                local_vals.append(float(lv))
                uv = entry.get(uk)
                if isinstance(uv, (int, float)) and not isinstance(uv, bool):
                    usd_vals.append(float(uv))
        # Point entry
        pv = entry.get("value")
        if isinstance(pv, (int, float)) and not isinstance(pv, bool) and pv > 0:
            local_vals.append(float(pv))
            uv = entry.get("value_usd")
            if isinstance(uv, (int, float)) and not isinstance(uv, bool):
                usd_vals.append(float(uv))
        sids = entry.get("source_ids")
        if isinstance(sids, list):
            for s in sids:
                if isinstance(s, str) and s not in source_ids:
                    source_ids.append(s)
    return local_vals, usd_vals, currency, source_ids


def get_local_salary_summary(
    industry: str | None, country: str | None
) -> dict[str, Any] | None:
    """Return a localized salary range for (industry, country).

    Pulls the dataset's ``annual_salary`` block, which stores values in LOCAL
    currency (GBP/EUR/INR/JPY...) alongside USD equivalents. Returns a dict
    suitable for direct rendering on a compensation slide::

        {
          "currency": "GBP",
          "symbol": "£",
          "low": 28407.0, "high": 46339.0,
          "local_display": "£28,407 - £46,339",
          "usd_display": "$38,065 - $46,339",   # omitted when currency is USD
          "source_ids": ["S11", "S12"],
        }

    Returns ``None`` when the (industry, country) pair has no salary data.
    Never raises.
    """
    try:
        from plan_currency import format_money, symbol_for_code
    except ImportError:  # pragma: no cover
        return None
    block = get_role_country_benchmarks(industry, country)
    if not block:
        return None
    annual_salary = block.get("annual_salary")
    if not isinstance(annual_salary, dict) or not annual_salary:
        return None

    local_vals, usd_vals, currency, source_ids = _extract_salary_points(annual_salary)
    if not local_vals:
        return None
    currency = (currency or block.get("currency") or "USD").upper()

    low, high = min(local_vals), max(local_vals)
    summary: dict[str, Any] = {
        "currency": currency,
        "symbol": symbol_for_code(currency),
        "low": low,
        "high": high,
        "local_display": (
            format_money(low, currency)
            if low == high
            else f"{format_money(low, currency)} - {format_money(high, currency)}"
        ),
        "source_ids": source_ids,
    }
    # Add USD equivalent only when the local currency isn't already USD.
    if currency != "USD" and usd_vals:
        u_low, u_high = min(usd_vals), max(usd_vals)
        summary["usd_display"] = (
            format_money(u_low, "USD")
            if u_low == u_high
            else f"{format_money(u_low, 'USD')} - {format_money(u_high, 'USD')}"
        )
    return summary


def is_available() -> bool:
    """True if the benchmark dataset is loaded and has at least one vertical."""
    data = _load()
    return bool((data.get("verticals") or {}))


# ═══════════════════════════════════════════════════════════════════════════
# Non-US locale CPC/CPA calibration (budget_engine Fix 1)
#
# Thin reader over ``data/international_benchmarks_2026.json`` -- a
# DIFFERENT, larger dataset than the one above (38 countries x per-platform
# CPC/CPA in local currency + USD, market share, CPH by tier), used by
# budget_engine.compute_channel_dollar_amounts to give non-US plans a real,
# country-specific CPC basis instead of pricing every market off the US
# cost curves in BASE_BENCHMARKS/trend_engine/the KB. Follows the SAME
# lazy-load-once, never-raise, ``.get()``-defensive pattern as the loader
# above so the two coexist without surprises.
# ═══════════════════════════════════════════════════════════════════════════

_INTL_2026_PATH: Path = Path(__file__).parent / "data" / "international_benchmarks_2026.json"

_intl_2026_countries: dict[str, Any] = {}
_intl_2026_loaded: bool = False


def _load_intl_2026_countries() -> dict[str, Any]:
    """Load and cache ``international_benchmarks_2026.json``'s ``countries``
    block. Returns ``{}`` on any failure -- callers fall back to the
    existing US-calibrated cascade, they never see an exception."""
    global _intl_2026_countries, _intl_2026_loaded
    if _intl_2026_loaded:
        return _intl_2026_countries
    _intl_2026_loaded = True
    try:
        if _INTL_2026_PATH.exists():
            with open(_INTL_2026_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            countries = raw.get("countries")
            _intl_2026_countries = countries if isinstance(countries, dict) else {}
            logger.info(
                "Loaded international_benchmarks_2026 countries (%d)",
                len(_intl_2026_countries),
            )
        else:
            logger.warning(
                "international_benchmarks_2026.json not found at %s -- "
                "non-US locale CPC calibration will return None",
                _INTL_2026_PATH,
            )
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(
            "Failed to load international_benchmarks_2026.json: %s",
            exc,
            exc_info=True,
        )
        _intl_2026_countries = {}
    return _intl_2026_countries


# Abbreviations/aliases the generic name-match below can't catch on its own
# (the generic match already handles every multi-word country name, e.g.
# "New Zealand" -> new_zealand, "South Korea" -> south_korea, "Saudi
# Arabia" -> saudi_arabia, via a straight lowercase name comparison).
_COUNTRY_2026_EXTRA_ALIASES: dict[str, str] = {
    "united kingdom": "uk",
    "great britain": "uk",
    "britain": "uk",
    "england": "uk",
    "gb": "uk",
    "gbr": "uk",
    "united arab emirates": "uae",
    "emirates": "uae",
    "dubai": "uae",
    "abu dhabi": "uae",
    "korea": "south_korea",
    "rok": "south_korea",
    "kr": "south_korea",
    "saudi": "saudi_arabia",
    "ksa": "saudi_arabia",
    "nz": "new_zealand",
    "nzl": "new_zealand",
    "aotearoa": "new_zealand",
    "za": "south_africa",
    "rsa": "south_africa",
    "holland": "netherlands",
    "the netherlands": "netherlands",
}

# international_benchmarks_2026.json platform ``type`` -> budget_engine's
# internal channel-category taxonomy (job_board, social, niche_board,
# regional, ... -- see budget_engine.CHANNEL_NAME_TO_CATEGORY). There is
# deliberately no entry for categories this dataset has no platform type
# for (programmatic/DSP, search, display, career_site, referral, events,
# staffing, email, employer_branding) -- those categories fall through to
# budget_engine's existing US-calibrated cascade even on a non-US plan
# rather than fabricating a local figure with no source.
_INTL_PLATFORM_TYPE_TO_CATEGORY: dict[str, str] = {
    "job_board": "job_board",
    "aggregator": "job_board",
    "government_job_board": "job_board",
    "newspaper_job_board": "regional",
    "association_board": "niche_board",
    "professional_network": "social",
}


def _normalize_country_2026(country: str | None) -> str | None:
    """Map a free-form country/location string to one of the 38
    ``international_benchmarks_2026.json`` country slugs, or ``None``.

    Tries, in order: the exact slug, the slug with spaces/hyphens swapped
    for underscores (covers "new zealand" -> "new_zealand", "south korea"
    -> "south_korea", "czech republic" -> "czech_republic" for free), the
    small extra-alias table above, and finally a case-insensitive match
    against each country's own ``name`` field. Never raises.
    """
    if not country or not isinstance(country, str):
        return None
    key = country.strip().lower()
    if not key:
        return None
    countries = _load_intl_2026_countries()
    if not countries:
        return None
    if key in countries:
        return key
    underscored = key.replace(" ", "_").replace("-", "_")
    if underscored in countries:
        return underscored
    alias = _COUNTRY_2026_EXTRA_ALIASES.get(key)
    if alias and alias in countries:
        return alias
    for slug, entry in countries.items():
        name = entry.get("name") if isinstance(entry, dict) else None
        if isinstance(name, str) and name.strip().lower() == key:
            return slug
    return None


def get_locale_cpc_basis(
    countries: list[Any] | None,
    plan_currency: str | None = None,
) -> dict[str, Any] | None:
    """Derive a non-US CPC basis, per budget_engine channel category, from
    ``data/international_benchmarks_2026.json``.

    This is intentionally NOT gated on US-ness itself -- callers (budget_
    engine.calculate_budget_allocation) should only invoke it once they've
    established the plan is non-US via the canonical ``plan_geo.is_us_plan``
    (this module has no opinion on that; it just answers "given these
    countries and this plan currency, what should CPC be calibrated to?").

    Weighting:
        - WITHIN a country, platforms are weighted by their own
          ``market_share_pct`` -- so e.g. UK's job_board figure reflects
          Indeed/Reed/Totaljobs/CV-Library's real mix, not a flat average.
        - ACROSS matched countries, each market is weighted EQUALLY. The
          engine has no per-country budget/headcount split at CPC-
          resolution time (that split doesn't exist yet in the pipeline),
          so an equal blend across the plan's markets is the only
          defensible default absent that signal.

    Currency (no exchange rate is ever fabricated here):
        - When exactly ONE country matches and its own currency equals
          ``plan_currency`` (e.g. a GBP plan whose only non-US market is
          the UK), each platform's ``cpc_local`` median is used directly --
          the plan's dollar amounts are already denominated in that same
          currency, so genuinely local calibration needs no conversion.
        - Otherwise (multiple countries, and/or a plan currency that
          doesn't match the single market -- e.g. a GBP plan spanning
          GBP/AUD/MXN/ARS/CAD/NZD markets), every country's ``cpc_usd``
          median is used as the common cross-market unit and blended.
          ``cpc_usd`` is a value already present in the source dataset for
          every platform -- nothing is converted or invented here.

    Args:
        countries: Free-form country/location strings (e.g. the plan's
            non-US location signals from ``plan_geo.non_us_signals``).
        plan_currency: ISO code the plan's dollar amounts are denominated
            in (e.g. "GBP"), if known. ``None`` skips the local-currency
            path and always uses the USD-blend basis.

    Returns:
        ``None`` when no country matches or none of its platforms map to a
        known category (caller falls through to the existing cascade), else::

            {
                "categories": {"job_board": 0.66, "social": 4.45, ...},
                "basis": "local" | "usd_blend",
                "matched_countries": ["uk", ...],
                "source": "intl_local:uk" | "intl_usd_blend:uk,australia,...",
            }

        Never raises.
    """
    if not countries:
        return None
    countries_data = _load_intl_2026_countries()
    if not countries_data:
        return None

    matched: list[str] = []
    seen: set[str] = set()
    for raw in countries:
        raw_str = raw if isinstance(raw, str) else str(raw)
        slug = _normalize_country_2026(raw_str)
        if slug and slug not in seen:
            seen.add(slug)
            matched.append(slug)
    if not matched:
        return None

    plan_cur = (plan_currency or "").strip().upper()
    use_local = False
    if len(matched) == 1 and plan_cur:
        entry = countries_data.get(matched[0]) or {}
        country_currency = str(entry.get("currency") or "").strip().upper()
        use_local = bool(country_currency) and country_currency == plan_cur

    # category -> one blended CPC per matched country (already
    # share-weighted WITHIN that country) -- averaged equally across
    # countries below.
    per_category: dict[str, list[float]] = {}
    for slug in matched:
        entry = countries_data.get(slug) or {}
        platforms = entry.get("platforms")
        if not isinstance(platforms, list):
            continue
        by_cat: dict[str, list[tuple[float, float]]] = {}
        for p in platforms:
            if not isinstance(p, dict):
                continue
            category = _INTL_PLATFORM_TYPE_TO_CATEGORY.get(p.get("type") or "")
            if not category:
                continue
            share = p.get("market_share_pct")
            share = (
                float(share)
                if isinstance(share, (int, float))
                and not isinstance(share, bool)
                and share > 0
                else 0.0
            )
            if share <= 0:
                continue
            cpc_block = p.get("cpc_local") if use_local else p.get("cpc_usd")
            cpc = (cpc_block or {}).get("median") if isinstance(cpc_block, dict) else None
            if not isinstance(cpc, (int, float)) or isinstance(cpc, bool) or cpc <= 0:
                continue
            by_cat.setdefault(category, []).append((float(cpc), share))
        for category, pairs in by_cat.items():
            weight_sum = sum(w for _, w in pairs)
            if weight_sum <= 0:
                continue
            country_cpc = sum(c * w for c, w in pairs) / weight_sum
            per_category.setdefault(category, []).append(country_cpc)

    if not per_category:
        return None

    categories = {
        cat: round(sum(vals) / len(vals), 4) for cat, vals in per_category.items()
    }
    basis = "local" if use_local else "usd_blend"
    tag = "intl_local" if use_local else "intl_usd_blend"
    return {
        "categories": categories,
        "basis": basis,
        "matched_countries": matched,
        "source": f"{tag}:{','.join(matched)}",
    }
