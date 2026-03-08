"""
data_orchestrator.py -- Unified Data Access Layer

Single entry point for enriched data queries that cascade through all
available data sources in order of cost and speed:

    1. research.py embedded data   (free, instant, 40+ countries, 100+ metros)
    2. Selective live API calls     (individual APIs, cached 24h)
    3. Static KB fallback           (JSON files, always available)

Thread-safe, lazy-loading, cached.  Never crashes -- all errors are caught
and the caller always receives a usable dict.

Consumers:
    - nova.py       (chatbot tool handlers)
    - nova_slack.py (Slack bot)
    - ppt_generator.py
    - app.py        (generation pipeline -- also has its own richer bulk flow)
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# LAZY MODULE LOADING (thread-safe, avoids circular imports)
# ═══════════════════════════════════════════════════════════════════════════════

_research = None
_standardizer = None
_api_enrichment = None
_budget_engine = None
_load_lock = threading.Lock()

# Sentinel for "tried to import and failed"
_IMPORT_FAILED = object()


def _lazy_research():
    """Thread-safe lazy import of research.py."""
    global _research
    if _research is None:
        with _load_lock:
            if _research is None:
                try:
                    import research as _r
                    _research = _r
                    logger.info("data_orchestrator: research module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: research import failed: %s", e)
                    _research = _IMPORT_FAILED
    return _research if _research is not _IMPORT_FAILED else None


def _lazy_standardizer():
    """Thread-safe lazy import of standardizer.py."""
    global _standardizer
    if _standardizer is None:
        with _load_lock:
            if _standardizer is None:
                try:
                    import standardizer as _s
                    _standardizer = _s
                    logger.info("data_orchestrator: standardizer module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: standardizer import failed: %s", e)
                    _standardizer = _IMPORT_FAILED
    return _standardizer if _standardizer is not _IMPORT_FAILED else None


def _lazy_api():
    """Thread-safe lazy import of api_enrichment.py."""
    global _api_enrichment
    if _api_enrichment is None:
        with _load_lock:
            if _api_enrichment is None:
                try:
                    import api_enrichment as _a
                    _api_enrichment = _a
                    logger.info("data_orchestrator: api_enrichment module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: api_enrichment import failed: %s", e)
                    _api_enrichment = _IMPORT_FAILED
    return _api_enrichment if _api_enrichment is not _IMPORT_FAILED else None


def _lazy_budget():
    """Thread-safe lazy import of budget_engine.py."""
    global _budget_engine
    if _budget_engine is None:
        with _load_lock:
            if _budget_engine is None:
                try:
                    import budget_engine as _b
                    _budget_engine = _b
                    logger.info("data_orchestrator: budget_engine module loaded")
                except Exception as e:
                    logger.warning("data_orchestrator: budget_engine import failed: %s", e)
                    _budget_engine = _IMPORT_FAILED
    return _budget_engine if _budget_engine is not _IMPORT_FAILED else None


# ═══════════════════════════════════════════════════════════════════════════════
# API RESULT CACHE (shared, thread-safe, 24h TTL)
# ═══════════════════════════════════════════════════════════════════════════════

_api_result_cache: Dict[str, Dict[str, Any]] = {}
_api_cache_lock = threading.Lock()
_API_CACHE_TTL = 24 * 3600  # 24 hours
_MAX_CACHE_ENTRIES = 500


def _cache_get(domain: str, key: str) -> Optional[Any]:
    """Get cached API result.  Returns None if expired or missing."""
    full_key = f"{domain}:{key}"
    with _api_cache_lock:
        entry = _api_result_cache.get(full_key)
        if entry and time.time() < entry.get("expires", 0):
            return entry["data"]
        elif entry:
            # Expired -- evict
            _api_result_cache.pop(full_key, None)
    return None


def _cache_set(domain: str, key: str, data: Any,
               ttl: int = _API_CACHE_TTL) -> None:
    """Cache an API result with TTL.  Auto-evicts oldest on overflow."""
    full_key = f"{domain}:{key}"
    with _api_cache_lock:
        _api_result_cache[full_key] = {
            "data": data,
            "expires": time.time() + ttl,
        }
        # Evict oldest entries if cache grows too large
        if len(_api_result_cache) > _MAX_CACHE_ENTRIES:
            oldest = min(_api_result_cache,
                         key=lambda k: _api_result_cache[k].get("expires", 0))
            _api_result_cache.pop(oldest, None)


# ═══════════════════════════════════════════════════════════════════════════════
# INPUT NORMALIZATION (uses standardizer.py)
# ═══════════════════════════════════════════════════════════════════════════════

def normalize(industry: str = "", location: str = "",
              role: str = "") -> Dict[str, Any]:
    """Normalize raw user inputs to canonical taxonomy forms.

    Returns dict with canonical values for each provided input:
        industry  -> canonical industry key
        location  -> {city, state, country}
        role      -> canonical role name
        soc_code  -> SOC code (if role given)
        role_tier -> tier classification (if role given)
        channels_key -> key for channels_db.json lookup (if industry given)
    """
    std = _lazy_standardizer()
    result: Dict[str, Any] = {}

    if industry:
        if std:
            try:
                result["industry"] = std.normalize_industry(industry)
                result["channels_key"] = std.get_channels_key(result["industry"])
            except Exception:
                result["industry"] = industry
        else:
            result["industry"] = industry

    if location:
        if std:
            try:
                result["location"] = std.normalize_location(location)
            except Exception:
                result["location"] = {"city": location, "state": "", "country": ""}
        else:
            result["location"] = {"city": location, "state": "", "country": ""}

    if role:
        if std:
            try:
                result["role"] = std.normalize_role(role)
                result["soc_code"] = std.get_soc_code(role)
                result["role_tier"] = std.get_role_tier(role)
            except Exception:
                result["role"] = role
        else:
            result["role"] = role

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SALARY INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_salary(role: str, location: str = "",
                  industry: str = "") -> Dict[str, Any]:
    """Enriched salary data.  Cascades:
        research.py (COLI-adjusted, BLS-augmented) -> live BLS API -> generic.

    Returns:
        {role, location, salary_range, median_salary, coli, role_tier,
         bls_percentiles, source}
    """
    result: Dict[str, Any] = {"role": role, "location": location or "National"}

    # -- 1. Location context from research.py (COLI, country detection) ------
    res = _lazy_research()
    coli = 100
    location_meta: Dict[str, Any] = {}
    if res and location:
        try:
            location_meta = res.get_location_info(location) or {}
            coli = location_meta.get("coli", 100)
            result["coli"] = coli
            result["country"] = location_meta.get("country", "United States")
            result["metro_name"] = location_meta.get("metro_name", location)
            result["currency"] = location_meta.get("currency", "USD")
        except Exception as e:
            logger.debug("enrich_salary: get_location_info failed: %s", e)
    result.setdefault("coli", coli)

    # -- 2. BLS API salary data (cached 24h) ---------------------------------
    api = _lazy_api()
    bls_data: Optional[Dict] = None
    cache_key = role.lower().strip()
    cached = _cache_get("salary", cache_key)
    if cached is not None:
        bls_data = cached
    elif api:
        try:
            raw = api.fetch_salary_data([role])
            if isinstance(raw, dict) and raw:
                # fetch_salary_data returns {role_name: {median, p10, p25, ...}}
                bls_data = raw.get(role)
                if not bls_data:
                    # Try case-insensitive match on first key
                    first_key = next(iter(raw), None)
                    if first_key:
                        bls_data = raw[first_key]
                if bls_data:
                    _cache_set("salary", cache_key, bls_data)
        except Exception as e:
            logger.debug("enrich_salary: fetch_salary_data failed: %s", e)

    # -- 3. Build salary range using research.py cascade ---------------------
    if res:
        try:
            enrichment_map = {role: bls_data} if bls_data else None
            salary_range = res.get_role_salary_range(
                role, location_coli=coli,
                enrichment_salary_data=enrichment_map,
            )
            result["salary_range"] = salary_range
            result["source"] = (
                "BLS API + COLI-adjusted" if bls_data
                else "Curated Industry Data + COLI-adjusted"
            )
        except Exception as e:
            logger.debug("enrich_salary: _get_role_salary_range failed: %s", e)

    # Fallback if research.py didn't produce a range
    if "salary_range" not in result:
        if bls_data and bls_data.get("median"):
            median = int(bls_data["median"] * (coli / 100.0))
            low, high = int(median * 0.75), int(median * 1.30)
            result["salary_range"] = f"${low:,} - ${high:,}"
            result["median_salary"] = median
            result["source"] = "BLS API"
        else:
            result["salary_range"] = "$45,000 - $80,000"
            result["source"] = "Generic Estimate"

    # -- 4. Role tier from standardizer --------------------------------------
    std = _lazy_standardizer()
    if std:
        try:
            result["role_tier"] = std.get_role_tier(role)
        except Exception:
            pass
    result.setdefault("role_tier", "Professional")

    # -- 5. BLS percentile data (compact, for Claude to reason over) ---------
    if bls_data:
        bls_compact: Dict[str, Any] = {}
        for k in ("median", "p10", "p25", "p75", "p90", "employment", "soc_code"):
            v = bls_data.get(k)
            if v is not None:
                bls_compact[k] = v
        if bls_compact:
            result["bls_percentiles"] = bls_compact

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# LOCATION INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_location(location: str) -> Dict[str, Any]:
    """Enriched location profile.  Cascades:
        research.py (40+ countries, 100+ US metros) -> Census/World Bank API -> generic.

    Returns:
        {location, metro_name, country, region, coli, population,
         median_salary, unemployment, currency, major_employers,
         top_boards, is_international, recommended_boards, source}
    """
    result: Dict[str, Any] = {"location": location}

    # -- 1. research.py (rich embedded data) ---------------------------------
    res = _lazy_research()
    if res:
        try:
            info = res.get_location_info(location)
            if info:
                result.update(info)
                result["source"] = "Research Intelligence"
                # Also get recommended boards for the location
                try:
                    boards = res.get_location_boards([location])
                    if boards:
                        result["recommended_boards"] = boards
                except Exception:
                    pass
                return result
        except Exception as e:
            logger.debug("enrich_location: get_location_info failed: %s", e)

    # -- 2. Census / World Bank API (cached) ---------------------------------
    api = _lazy_api()
    if api:
        loc_key = location.lower().strip()
        cached = _cache_get("location", loc_key)
        if cached:
            result.update(cached)
            result["source"] = "API Cache"
            return result

        # Try US Census first
        try:
            demo = api.fetch_location_demographics([location])
            if isinstance(demo, dict):
                for _k, ld in demo.items():
                    if isinstance(ld, dict) and ld.get("population"):
                        result["population"] = ld.get("population")
                        result["median_salary"] = ld.get("median_income", 0)
                        result["country"] = "United States"
                        result["source"] = "US Census API"
                        _cache_set("location", loc_key, dict(result))
                        return result
        except Exception as e:
            logger.debug("enrich_location: fetch_location_demographics failed: %s", e)

        # Try World Bank for international
        try:
            wb = api.fetch_global_indicators([location])
            if isinstance(wb, dict):
                for _k, ld in wb.items():
                    if isinstance(ld, dict) and ld.get("population"):
                        result["population"] = ld.get("population")
                        result["country"] = _k
                        result["is_international"] = True
                        result["source"] = "World Bank API"
                        _cache_set("location", loc_key, dict(result))
                        return result
        except Exception as e:
            logger.debug("enrich_location: fetch_global_indicators failed: %s", e)

    # -- 3. Generic fallback -------------------------------------------------
    result.update({
        "coli": 100,
        "population": "Data not available",
        "median_salary": 60000,
        "unemployment": "~3.5%",
        "metro_name": location,
        "country": "United States",
        "source": "Generic Estimate",
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MARKET DEMAND INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_market_demand(role: str = "", location: str = "",
                         industry: str = "") -> Dict[str, Any]:
    """Enriched job market demand signals.  Cascades:
        research.py labor market intel -> Adzuna/Jooble API -> generic.

    Returns:
        {role, location, industry, labour_market, api_job_market,
         competitors, seasonal, source}
    """
    result: Dict[str, Any] = {
        "role": role or "General",
        "location": location or "National",
        "industry": industry or "General",
    }

    # -- 1. research.py labor market intelligence ----------------------------
    res = _lazy_research()
    if res and industry:
        try:
            lmi = res.get_labour_market_intelligence(
                industry, [location] if location else [],
            )
            if lmi:
                result["labour_market"] = lmi
                result["source"] = "Research Intelligence"
        except Exception as e:
            logger.debug("enrich_market_demand: labour_market_intelligence failed: %s", e)

        # Seasonal hiring patterns
        try:
            seasonal = res.get_seasonal_hiring_advice(industry)
            if seasonal:
                result["seasonal"] = seasonal
        except Exception as e:
            logger.debug("enrich_market_demand: seasonal_hiring_advice failed: %s", e)

    # -- 2. Adzuna / Jooble API for live job market data ---------------------
    api = _lazy_api()
    if api and role:
        cache_key = f"{role}:{location}".lower().strip()
        cached = _cache_get("market_demand", cache_key)
        if cached is not None:
            result["api_job_market"] = cached
            result.setdefault("source", "API Cache (Job Market)")
        else:
            try:
                locs = [location] if location else []
                jm_raw = api.fetch_job_market([role], locs)
                if isinstance(jm_raw, dict):
                    jm = jm_raw.get("job_market", {})
                    if jm:
                        result["api_job_market"] = jm
                        _cache_set("market_demand", cache_key, jm)
                        result.setdefault("source", "Adzuna/Jooble API")
            except Exception as e:
                logger.debug("enrich_market_demand: fetch_job_market failed: %s", e)

    # -- 3. Competitor landscape from research.py ----------------------------
    if res and industry:
        try:
            comps = res.get_competitors(
                industry, [location] if location else [],
            )
            if comps:
                result["competitors"] = comps[:5]
        except Exception as e:
            logger.debug("enrich_market_demand: get_competitors failed: %s", e)

    result.setdefault("source", "Generic Market Data")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# COMPETITIVE INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_competitive(company: str, industry: str = "",
                       locations: Optional[List[str]] = None) -> Dict[str, Any]:
    """Enriched competitive intelligence.  Cascades:
        research.py -> SEC/Wikipedia API -> generic.

    Returns:
        {company, company_info, competitors, company_metadata, source}
    """
    result: Dict[str, Any] = {"company": company}

    # -- 1. research.py company intelligence ---------------------------------
    res = _lazy_research()
    if res and company:
        try:
            ci = res.get_company_intelligence(company)
            if ci:
                result["company_info"] = ci
                result["source"] = "Research Intelligence"
        except Exception as e:
            logger.debug("enrich_competitive: get_company_intelligence failed: %s", e)

    # -- 2. research.py competitor landscape ---------------------------------
    comps: list = []
    if res and industry:
        try:
            comps = res.get_competitors(industry, locations or []) or []
            if comps:
                result["competitors"] = comps[:5]
        except Exception as e:
            logger.debug("enrich_competitive: get_competitors failed: %s", e)

        try:
            comp_intel = res.get_client_competitor_intelligence(
                comps[:3] if comps else [], industry,
            )
            if comp_intel:
                result["competitor_intelligence"] = comp_intel
        except Exception as e:
            logger.debug("enrich_competitive: get_client_competitor_intelligence failed: %s", e)

    # -- 3. API enrichment (company metadata, SEC data) ----------------------
    api = _lazy_api()
    if api and company:
        cached = _cache_get("competitive", company.lower().strip())
        if cached is not None:
            result.update(cached)
        else:
            api_results: Dict[str, Any] = {}
            try:
                meta = api.fetch_company_metadata(company)
                if isinstance(meta, dict) and meta:
                    api_results["company_metadata"] = meta
            except Exception as e:
                logger.debug("enrich_competitive: fetch_company_metadata failed: %s", e)

            try:
                sec = api.fetch_sec_company_data(company)
                if isinstance(sec, dict) and sec:
                    api_results["sec_data"] = sec
            except Exception as e:
                logger.debug("enrich_competitive: fetch_sec_company_data failed: %s", e)

            if api_results:
                result.update(api_results)
                _cache_set("competitive", company.lower().strip(), api_results)

    result.setdefault("source", "Generic Competitive Data")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# BUDGET ALLOCATION (with synthesized data from cache)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_budget(budget: float, roles: List[Dict], locations: List[Dict],
                  industry: str = "",
                  knowledge_base: Optional[Dict] = None) -> Dict[str, Any]:
    """Calculate budget allocation using cached enrichment data.

    Unlike the basic Nova implementation (which passes synthesized_data=None),
    this pulls any cached salary and market demand data to improve accuracy.
    """
    be = _lazy_budget()
    if not be:
        return {"error": "Budget engine not available"}

    # Build minimal synthesized data from cached API results
    synthesized: Dict[str, Any] = {}

    for r in roles:
        title = r.get("title", "")
        if not title:
            continue
        cached_sal = _cache_get("salary", title.lower().strip())
        if cached_sal:
            synthesized.setdefault("salary_intelligence", {})[title] = cached_sal

        for loc in locations:
            loc_str = loc.get("city", "")
            ck = f"{title}:{loc_str}".lower().strip()
            cached_demand = _cache_get("market_demand", ck)
            if cached_demand:
                synthesized.setdefault("job_market_demand", {})[title] = cached_demand

    channel_pcts = {
        "Programmatic & DSP": 30,
        "Global Job Boards": 25,
        "Niche & Industry Boards": 15,
        "Social Media Channels": 15,
        "Regional & Local Boards": 10,
        "Employer Branding": 5,
    }

    try:
        return be.calculate_budget_allocation(
            total_budget=budget,
            roles=roles,
            locations=locations,
            industry=industry,
            channel_percentages=channel_pcts,
            synthesized_data=synthesized if synthesized else None,
            knowledge_base=knowledge_base,
        )
    except Exception as e:
        logger.error("enrich_budget failed: %s", e, exc_info=True)
        return {"error": "Budget calculation failed"}


# ═══════════════════════════════════════════════════════════════════════════════
# ADDITIONAL RESEARCH.PY ACCESSORS (thin wrappers for Nova tools)
# ═══════════════════════════════════════════════════════════════════════════════

def enrich_hiring_regulations(locations: List[str]) -> List:
    """Hiring regulations from research.py.  Returns list of regulation dicts."""
    res = _lazy_research()
    if res:
        try:
            return res.get_hiring_regulations(locations) or []
        except Exception as e:
            logger.debug("enrich_hiring_regulations failed: %s", e)
    return []


def enrich_seasonal(industry: str) -> Dict[str, Any]:
    """Seasonal hiring advice from research.py."""
    res = _lazy_research()
    if res and industry:
        try:
            return res.get_seasonal_hiring_advice(industry) or {}
        except Exception as e:
            logger.debug("enrich_seasonal failed: %s", e)
    return {}


def enrich_campus(locations: List[str], roles: Optional[List[str]] = None,
                  industry: str = "") -> List:
    """Campus recruiting recommendations from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_campus_recruiting_recommendations(
                locations, roles, industry,
            ) or []
        except Exception as e:
            logger.debug("enrich_campus failed: %s", e)
    return []


def enrich_events(locations: List[str], industry: str = "") -> List:
    """Industry events from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_events(locations, industry) or []
        except Exception as e:
            logger.debug("enrich_events failed: %s", e)
    return []


def enrich_platform_audiences(industry: str) -> Dict[str, Any]:
    """Platform audience data from research.py."""
    res = _lazy_research()
    if res and industry:
        try:
            return res.get_media_platform_audiences(industry) or {}
        except Exception as e:
            logger.debug("enrich_platform_audiences failed: %s", e)
    return {}


def enrich_global_supply(locations: List[str],
                         industry: str = "") -> Dict[str, Any]:
    """Global supply data from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_global_supply_data(locations, industry) or {}
        except Exception as e:
            logger.debug("enrich_global_supply failed: %s", e)
    return {}


def enrich_educational_partners(locations: List[str],
                                industry: str = "") -> List:
    """Educational partners from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_educational_partners(locations, industry) or []
        except Exception as e:
            logger.debug("enrich_educational_partners failed: %s", e)
    return []


def enrich_radio_podcasts(locations: List[str],
                          industry: str = "") -> List:
    """Radio and podcast advertising data from research.py."""
    res = _lazy_research()
    if res:
        try:
            return res.get_radio_podcasts(locations, industry) or []
        except Exception as e:
            logger.debug("enrich_radio_podcasts failed: %s", e)
    return []
