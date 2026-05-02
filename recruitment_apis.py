"""Free recruitment data API clients for Nova chatbot tools (S50 -- May 2026).

This module provides 11 stdlib-only HTTP clients for free (or freemium)
recruitment-related data sources. Each public function returns a dict with
a ``"source"`` key and either structured data or an error/note key.

Integrated sources:
    1.  ESCO (skill lookup)             -- EU Commission, free, no key
    2.  ESCO (occupation lookup)        -- EU Commission, free, no key
    3.  NPPES NPI Registry              -- US healthcare providers, free
    4.  FMCSA QC Mobile                 -- US trucking carriers, free
    5.  ILOSTAT SDMX                    -- ILO labour stats (WB fallback)
    6.  World Bank Open Data v2         -- 1500+ indicators, free
    7.  WARNTracker                     -- US layoffs, URL stub
    8.  HN "Who is hiring" (Algolia)    -- Tech jobs, free
    9.  Levels.fyi                      -- Compensation, embed-URL stub
    10. Crunchbase v4                   -- Companies, paid, key-gated stub
    11. People Data Labs Person Enrich  -- freemium, key-gated stub

Coding contract: stdlib only; all public functions accept ``timeout: int = 10``
and return a dict with at least a ``"source"`` key; errors return
``{"error": str, "source": str}``.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)


# ─── Shared HTTP helper ────────────────────────────────────────────────────────

_ssl_ctx = ssl.create_default_context()
_ssl_ctx_unverified = ssl._create_unverified_context()
_DEFAULT_USER_AGENT = "Joveo-Nova-Recruitment-APIs/1.0"

# Standard exception tuple captured by every API wrapper. Bare `except:` is
# forbidden by project rules; this tuple covers realistic urllib + json
# failure modes without masking programming errors.
_NET_ERRORS = (
    urllib.error.HTTPError,
    urllib.error.URLError,
    TimeoutError,
    json.JSONDecodeError,
    ValueError,
    OSError,
)


def _http_get_json(
    url: str,
    timeout: int,
    headers: dict[str, str] | None = None,
) -> dict:
    """GET a URL and return parsed JSON as a dict.

    Falls back to an unverified SSL context once on cert failures (some gov
    endpoints have incomplete chains). JSON arrays are wrapped as
    ``{"_list": [...]}`` so the return type stays dict-shaped.

    Args:
        url: Fully-qualified URL.
        timeout: Per-call timeout in seconds.
        headers: Optional HTTP headers; default User-Agent + Accept added.

    Raises:
        urllib.error.HTTPError, urllib.error.URLError, TimeoutError,
        json.JSONDecodeError, ValueError, OSError.
    """
    if not url:
        raise ValueError("url must be non-empty")
    final_headers = dict(headers or {})
    final_headers.setdefault("User-Agent", _DEFAULT_USER_AGENT)
    final_headers.setdefault("Accept", "application/json")
    req = urllib.request.Request(url, headers=final_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as ssl_exc:
        msg = str(ssl_exc)
        if "CERTIFICATE_VERIFY_FAILED" in msg or "SSL" in msg:
            logger.warning(
                f"SSL verify failed for {url}; retrying unverified", exc_info=True
            )
            req2 = urllib.request.Request(url, headers=final_headers)
            with urllib.request.urlopen(
                req2, timeout=timeout, context=_ssl_ctx_unverified
            ) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        else:
            raise
    parsed = json.loads(raw)
    if isinstance(parsed, list):
        return {"_list": parsed}
    return parsed


def _http_post_json(
    url: str,
    body: dict,
    timeout: int,
    headers: dict[str, str] | None = None,
) -> dict:
    """POST a JSON body and return parsed JSON as a dict (lists wrapped)."""
    if not url:
        raise ValueError("url must be non-empty")
    final_headers = dict(headers or {})
    final_headers.setdefault("User-Agent", _DEFAULT_USER_AGENT)
    final_headers.setdefault("Content-Type", "application/json")
    final_headers.setdefault("Accept", "application/json")
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload, headers=final_headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    parsed = json.loads(raw)
    if isinstance(parsed, list):
        return {"_list": parsed}
    return parsed


def _err(source: str, exc: Exception | str) -> dict:
    """Return the uniform error shape {'error': str, 'source': str}."""
    return {"error": str(exc), "source": source}


def _esco_description(desc_obj: object, lang: str) -> str:
    """Pull a literal description string out of an ESCO description object."""
    if isinstance(desc_obj, dict):
        lit = desc_obj.get(lang) or desc_obj.get("en") or {}
        if isinstance(lit, dict):
            return lit.get("literal") or ""
    elif isinstance(desc_obj, str):
        return desc_obj
    return ""


def _esco_search(text: str, kind: str, lang: str, timeout: int) -> dict:
    """Internal ESCO search helper used by both skill and occupation lookups."""
    qs = urllib.parse.urlencode(
        {"text": text, "type": kind, "language": lang, "limit": 5}
    )
    url = f"https://ec.europa.eu/esco/api/search?{qs}"
    data = _http_get_json(url, timeout=timeout)
    embedded = data.get("_embedded") or {}
    results = embedded.get("results") or []
    out: list[dict] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "uri": item.get("uri") or "",
                "title": item.get("title") or item.get("preferredLabel") or "",
                "description": _esco_description(item.get("description"), lang),
            }
        )
    return {"_results": out}


# ─── 1. ESCO skill lookup ──────────────────────────────────────────────────────


def lookup_skill_esco(skill: str, lang: str = "en", timeout: int = 10) -> dict:
    """Look up a skill in the ESCO taxonomy (European Commission, free).

    Args:
        skill: Free-text skill name (e.g. "python", "welding").
        lang: ISO 639-1 language code. Defaults to "en".
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"skills": [{"uri","title","description"}], "source": "ESCO",
        "count": int}`` on success; ``{"error": str, "source": "ESCO"}`` on error.
    """
    if not skill or not skill.strip():
        return _err("ESCO", "skill must be non-empty")
    try:
        result = _esco_search(skill.strip(), "skill", lang, timeout)
    except _NET_ERRORS as exc:
        logger.error(f"ESCO skill lookup failed for '{skill}'", exc_info=True)
        return _err("ESCO", exc)
    skills = result["_results"]
    return {"skills": skills, "source": "ESCO", "count": len(skills)}


# ─── 2. ESCO occupation lookup ────────────────────────────────────────────────


def lookup_occupation_esco(
    occupation: str, lang: str = "en", timeout: int = 10
) -> dict:
    """Look up an occupation in the ESCO taxonomy.

    Args:
        occupation: Free-text occupation name (e.g. "software developer").
        lang: ISO 639-1 language code. Defaults to "en".
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"occupations": [...], "source": "ESCO", "count": int}`` on success;
        ``{"error": str, "source": "ESCO"}`` on error.
    """
    if not occupation or not occupation.strip():
        return _err("ESCO", "occupation must be non-empty")
    try:
        result = _esco_search(occupation.strip(), "occupation", lang, timeout)
    except _NET_ERRORS as exc:
        logger.error(f"ESCO occupation lookup failed for '{occupation}'", exc_info=True)
        return _err("ESCO", exc)
    occs = result["_results"]
    return {"occupations": occs, "source": "ESCO", "count": len(occs)}


# ─── 3. NPPES NPI Registry ────────────────────────────────────────────────────


def lookup_healthcare_npi(
    name_or_npi: str,
    state: str = "",
    limit: int = 10,
    timeout: int = 10,
) -> dict:
    """Look up US healthcare providers via the NPPES NPI Registry v2.1.

    A 10-digit numeric input is treated as an NPI number; otherwise the
    first whitespace-separated token is used as first_name and the rest
    as last_name.

    Args:
        name_or_npi: Provider name or 10-digit NPI number.
        state: Optional 2-letter US state filter.
        limit: Max results (1-200; CMS caps at 200).
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"providers": [{"npi","name","taxonomy","addresses":[...]}],
        "source": "NPI Registry", "count": int}`` on success;
        ``{"error": str, "source": "NPI Registry"}`` on error.
    """
    if not name_or_npi or not name_or_npi.strip():
        return _err("NPI Registry", "name_or_npi must be non-empty")
    cleaned = name_or_npi.strip()
    params: dict[str, str | int] = {
        "version": "2.1",
        "limit": max(1, min(limit, 200)),
    }
    if cleaned.isdigit() and len(cleaned) == 10:
        params["number"] = cleaned
    else:
        parts = cleaned.split(None, 1)
        params["first_name"] = parts[0]
        if len(parts) > 1:
            params["last_name"] = parts[1]
    if state:
        params["state"] = state.upper()[:2]
    url = f"https://npiregistry.cms.hhs.gov/api/?{urllib.parse.urlencode(params)}"
    try:
        data = _http_get_json(url, timeout=timeout)
    except _NET_ERRORS as exc:
        logger.error(f"NPI Registry lookup failed for '{name_or_npi}'", exc_info=True)
        return _err("NPI Registry", exc)

    providers = [
        _npi_record(item)
        for item in data.get("results") or []
        if isinstance(item, dict)
    ]
    return {"providers": providers, "source": "NPI Registry", "count": len(providers)}


_NPI_ADDR_FIELDS = (
    "address_1",
    "address_2",
    "city",
    "state",
    "postal_code",
    "country_code",
    "telephone_number",
    "address_purpose",
)


def _npi_record(item: dict) -> dict:
    """Flatten a single NPI Registry result into Nova's slim shape."""
    basic = item.get("basic") or {}
    if basic.get("organization_name"):
        display = basic.get("organization_name") or ""
    else:
        first = basic.get("first_name") or ""
        last = basic.get("last_name") or ""
        credential = basic.get("credential") or ""
        display = f"{first} {last}".strip()
        if credential:
            display = f"{display}, {credential}"
    taxonomies = item.get("taxonomies") or []
    primary_tax = ""
    for tax in taxonomies:
        if isinstance(tax, dict) and tax.get("primary"):
            primary_tax = tax.get("desc") or tax.get("code") or ""
            break
    if not primary_tax and taxonomies and isinstance(taxonomies[0], dict):
        primary_tax = taxonomies[0].get("desc") or taxonomies[0].get("code") or ""
    addr_out = [
        {f: a.get(f) or "" for f in _NPI_ADDR_FIELDS}
        for a in item.get("addresses") or []
        if isinstance(a, dict)
    ]
    return {
        "npi": item.get("number") or "",
        "name": display,
        "taxonomy": primary_tax,
        "addresses": addr_out,
    }


# ─── 4. FMCSA trucking carriers ───────────────────────────────────────────────


def lookup_trucking_carrier(dot_or_name: str, timeout: int = 10) -> dict:
    """Look up US trucking carriers via the FMCSA QC Mobile API.

    Empty webKey is intentional: FMCSA's public-mode endpoint accepts it.
    Digits-only input is treated as a USDOT number; otherwise as a name.

    Args:
        dot_or_name: USDOT number (digits only) or carrier name.
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"carriers": [...], "source": "FMCSA", "count": int}`` on success;
        ``{"error": str, "source": "FMCSA"}`` on error.
    """
    if not dot_or_name or not dot_or_name.strip():
        return _err("FMCSA", "dot_or_name must be non-empty")
    cleaned = dot_or_name.strip()
    if cleaned.isdigit():
        url = (
            f"https://mobile.fmcsa.dot.gov/qc/services/carriers/"
            f"{urllib.parse.quote(cleaned, safe='')}?webKey="
        )
    else:
        url = (
            f"https://mobile.fmcsa.dot.gov/qc/services/carriers/name/"
            f"{urllib.parse.quote(cleaned, safe='')}?webKey="
        )
    try:
        data = _http_get_json(url, timeout=timeout)
    except _NET_ERRORS as exc:
        logger.error(f"FMCSA lookup failed for '{dot_or_name}'", exc_info=True)
        return _err("FMCSA", exc)

    content = data.get("content")
    if isinstance(content, list):
        items = content
    elif isinstance(content, dict):
        items = [content]
    else:
        items = data.get("_list") or []
    carriers = [_fmcsa_record(item) for item in items if isinstance(item, dict)]
    return {"carriers": carriers, "source": "FMCSA", "count": len(carriers)}


def _fmcsa_record(item: dict) -> dict:
    """Flatten a single FMCSA carrier record into Nova's slim shape."""
    carrier = item.get("carrier") if isinstance(item.get("carrier"), dict) else item
    return {
        "dot_number": carrier.get("dotNumber") or "",
        "legal_name": carrier.get("legalName") or "",
        "dba_name": carrier.get("dbaName") or "",
        "phy_state": carrier.get("phyState") or "",
        "phy_city": carrier.get("phyCity") or "",
        "total_drivers": carrier.get("totalDrivers") or 0,
        "total_power_units": carrier.get("totalPowerUnits") or 0,
        "allowed_to_operate": carrier.get("allowedToOperate") or "",
    }


# ─── 5+6. World Bank (used as fallback for ILOSTAT, and as its own API) ───────


def _worldbank_fetch(
    country_iso3: str,
    indicator: str,
    timeout: int,
    date_range: str = "2020:2024",
    per_page: int = 20,
) -> dict:
    """Fetch a World Bank indicator series (internal helper)."""
    url = (
        f"https://api.worldbank.org/v2/country/"
        f"{urllib.parse.quote(country_iso3, safe='')}/indicator/"
        f"{urllib.parse.quote(indicator, safe='')}"
        f"?format=json&date={date_range}&per_page={per_page}"
    )
    data = _http_get_json(url, timeout=timeout)
    series = data.get("_list") or []
    observations: list[dict] = []
    if len(series) >= 2 and isinstance(series[1], list):
        for row in series[1]:
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            if value is not None:
                observations.append(
                    {"year": str(row.get("date") or ""), "value": value}
                )
    return {
        "country": country_iso3,
        "indicator": indicator,
        "observations": observations,
    }


def _parse_ilostat_observations(data: dict) -> list[dict]:
    """Extract (year, value) tuples from an ILOSTAT SDMX-JSON response."""
    out: list[dict] = []
    try:
        payload = data.get("data") or {}
        ds = payload.get("dataSets") or []
        if not ds or not isinstance(ds[0], dict):
            return out
        # Pull the time-axis values from the observation dimensions
        obs_dims = ((payload.get("structure") or {}).get("dimensions") or {}).get(
            "observation"
        ) or []
        time_values: list[str] = []
        for dim in obs_dims:
            if isinstance(dim, dict) and (dim.get("id") or "").upper() in (
                "TIME_PERIOD",
                "TIME",
            ):
                time_values = [
                    (v.get("id") or v.get("name") or "")
                    for v in (dim.get("values") or [])
                    if isinstance(v, dict)
                ]
                break
        # Observations may live under series.<key>.observations OR dataSet.observations
        series_map = ds[0].get("series") or {}
        if isinstance(series_map, dict) and series_map:
            first = next(iter(series_map.values()), {}) or {}
            observations = first.get("observations") or {}
        else:
            observations = ds[0].get("observations") or {}
        for idx_str, vals in observations.items():
            if not isinstance(vals, list) or not vals:
                continue
            idx = int(idx_str) if str(idx_str).isdigit() else -1
            year = time_values[idx] if 0 <= idx < len(time_values) else str(idx_str)
            out.append({"year": year, "value": vals[0]})
    except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
        logger.warning(f"ILOSTAT SDMX parse failed: {exc}")
    return out


def lookup_country_labour_ilostat(
    country_iso3: str,
    indicator: str = "UNE_DEAP_SEX_AGE_RT_A",
    timeout: int = 15,
) -> dict:
    """Fetch a country's labour-market series from ILOSTAT, with WB fallback.

    Tries ILOSTAT SDMX REST first. On any failure (or empty observations),
    falls back to the World Bank unemployment series (SL.UEM.TOTL.ZS) and
    tags the source as "WorldBank fallback".

    Args:
        country_iso3: ISO 3-letter country code (e.g. "USA").
        indicator: ILOSTAT dataflow ID. Defaults to annual unemployment rate.
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"observations": [{"year","value"}], "indicator": str, "country": str,
        "source": "ILOSTAT" | "WorldBank fallback"}`` on success;
        ``{"error": str, "source": "ILOSTAT"}`` on total failure.
    """
    if not country_iso3 or len(country_iso3) != 3:
        return _err("ILOSTAT", "country_iso3 must be a 3-letter code")
    iso = country_iso3.upper()

    sdmx_url = (
        f"https://www.ilo.org/sdmx/rest/data/ILO,DF_{indicator}/"
        f"A.{iso}.....?format=jsondata&lastNObservations=5"
    )
    try:
        data = _http_get_json(sdmx_url, timeout=timeout)
        observations = _parse_ilostat_observations(data)
        if observations:
            return {
                "observations": observations,
                "indicator": indicator,
                "country": iso,
                "source": "ILOSTAT",
            }
        logger.info(f"ILOSTAT empty for {iso}/{indicator}; using World Bank fallback")
    except _NET_ERRORS as exc:
        logger.warning(
            f"ILOSTAT failed for {iso}/{indicator}, using WB fallback: {exc}",
            exc_info=True,
        )

    try:
        wb = _worldbank_fetch(
            iso, "SL.UEM.TOTL.ZS", timeout=timeout, date_range="2020:2024", per_page=5
        )
    except _NET_ERRORS as exc:
        logger.error(f"World Bank fallback failed for {iso}", exc_info=True)
        return _err("ILOSTAT", exc)
    return {
        "observations": wb["observations"],
        "indicator": "SL.UEM.TOTL.ZS",
        "country": iso,
        "source": "WorldBank fallback",
    }


def lookup_country_indicator_worldbank(
    country_iso3: str, indicator: str, timeout: int = 10
) -> dict:
    """Fetch a World Bank Open Data v2 indicator series for a country.

    Args:
        country_iso3: ISO 3-letter country code (e.g. "USA").
        indicator: World Bank indicator code (e.g. "SL.UEM.TOTL.ZS").
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"country": str, "indicator": str,
        "observations": [{"year","value"}], "source": "World Bank"}`` on success;
        ``{"error": str, "source": "World Bank"}`` on error.
    """
    if not country_iso3 or len(country_iso3) != 3:
        return _err("World Bank", "country_iso3 must be a 3-letter code")
    if not indicator or not indicator.strip():
        return _err("World Bank", "indicator must be non-empty")
    iso = country_iso3.upper()
    try:
        wb = _worldbank_fetch(
            iso, indicator.strip(), timeout=timeout, date_range="2020:2024", per_page=20
        )
    except _NET_ERRORS as exc:
        logger.error(f"World Bank lookup failed for {iso}/{indicator}", exc_info=True)
        return _err("World Bank", exc)
    return {
        "country": wb["country"],
        "indicator": wb["indicator"],
        "observations": wb["observations"],
        "source": "World Bank",
    }


# ─── 7. WARN Tracker (URL stub; no JSON API) ──────────────────────────────────


def lookup_layoffs_warntracker(
    state: str = "", since_year: int = 2026, timeout: int = 15
) -> dict:
    """Return a citable WARNTracker URL (no documented JSON API).

    WARNTracker.com aggregates US WARN Act layoff notices but exposes no
    JSON API. Returns a stable URL the chatbot can cite instead of
    attempting fragile HTML scraping.

    Args:
        state: Optional 2-letter US state filter (e.g. "CA").
        since_year: Year filter for the returned URL.
        timeout: Unused; kept for signature uniformity.

    Returns:
        ``{"layoffs": [], "source": "WARNTracker", "note": str, "url": str}``.
    """
    _ = timeout  # signature uniformity; no live HTTP call
    state_clean = (state or "").strip().upper()[:2]
    qs = urllib.parse.urlencode({"year": since_year, "state": state_clean})
    url = f"https://www.warntracker.com/?{qs}"
    note = (
        "Live scraping not implemented. "
        f"Use https://www.warntracker.com/?year={since_year}&state={state_clean}"
    )
    return {"layoffs": [], "source": "WARNTracker", "note": note, "url": url}


# ─── 8. HN "Who is hiring" (Algolia) ──────────────────────────────────────────


def lookup_tech_jobs_hnhiring(query: str, limit: int = 10, timeout: int = 10) -> dict:
    """Search Hacker News "Who is hiring" threads via the Algolia public API.

    Filters down to hits whose story_text or comment_text contains "hiring".

    Args:
        query: Free-text query (e.g. "python remote").
        limit: Maximum hits returned (capped at 50).
        timeout: Per-call timeout in seconds.

    Returns:
        ``{"jobs": [{"title","url","comments_url","created_at"}],
        "source": "HN Algolia", "count": int}`` on success;
        ``{"error": str, "source": "HN Algolia"}`` on error.
    """
    if not query or not query.strip():
        return _err("HN Algolia", "query must be non-empty")
    safe_limit = max(1, min(limit, 50))
    qs = urllib.parse.urlencode(
        {
            "query": query.strip(),
            "tags": "story,(story,author_whoishiring)",
            "hitsPerPage": safe_limit,
        }
    )
    url = f"https://hn.algolia.com/api/v1/search?{qs}"
    try:
        data = _http_get_json(url, timeout=timeout)
    except _NET_ERRORS as exc:
        logger.error(f"HN Algolia search failed for '{query}'", exc_info=True)
        return _err("HN Algolia", exc)

    jobs: list[dict] = []
    for h in data.get("hits") or []:
        if not isinstance(h, dict):
            continue
        story_text = h.get("story_text") or ""
        comment_text = h.get("comment_text") or ""
        if "hiring" not in f"{story_text} {comment_text}".lower():
            continue
        oid = h.get("objectID") or ""
        jobs.append(
            {
                "title": h.get("title") or h.get("story_title") or "",
                "url": h.get("url") or h.get("story_url") or "",
                "comments_url": (
                    f"https://news.ycombinator.com/item?id={oid}" if oid else ""
                ),
                "created_at": h.get("created_at") or "",
            }
        )
    return {"jobs": jobs, "source": "HN Algolia", "count": len(jobs)}


# ─── 9. Levels.fyi (embed-URL stub; no public JSON API) ───────────────────────


def lookup_compensation_levels(
    role: str, location: str = "", timeout: int = 10
) -> dict:
    """Return a Levels.fyi public embed URL for compensation data.

    Levels.fyi has no public-tier JSON API. Returns the citable embed URL
    so the chatbot can route the user there.

    Args:
        role: Job title (e.g. "Software Engineer").
        location: Optional location filter.
        timeout: Unused; kept for signature uniformity.

    Returns:
        ``{"role", "location", "embed_url", "source": "Levels.fyi", "note"}``.
    """
    _ = timeout  # signature uniformity; no live HTTP call
    role_clean = (role or "").strip()
    loc_clean = (location or "").strip()
    qs = urllib.parse.urlencode({"title": role_clean, "location": loc_clean})
    note = (
        "Public embed available; programmatic API requires application "
        "at levels.fyi/api-access"
    )
    return {
        "role": role_clean,
        "location": loc_clean,
        "embed_url": f"https://www.levels.fyi/comp.html?{qs}",
        "source": "Levels.fyi",
        "note": note,
    }


# ─── 10. Crunchbase v4 (paid, key-gated) ──────────────────────────────────────


def lookup_company_crunchbase(name: str, timeout: int = 10) -> dict:
    """Look up a company in Crunchbase v4 (paid; key-gated stub if missing).

    If CRUNCHBASE_API_KEY is unset, returns a stub directing the caller to
    crunchbase.com/api. Otherwise issues a real Crunchbase v4 search.

    Args:
        name: Company name to search.
        timeout: Per-call timeout in seconds.

    Returns:
        Stub: ``{"company": str, "source": "Crunchbase", "note": "..."}``;
        success: ``{"results": [...], "source": "Crunchbase", "count": int}``;
        error: ``{"error": str, "source": "Crunchbase"}``.
    """
    if not name or not name.strip():
        return _err("Crunchbase", "name must be non-empty")
    api_key = os.environ.get("CRUNCHBASE_API_KEY")
    if not api_key:
        return {
            "company": name,
            "source": "Crunchbase",
            "note": "CRUNCHBASE_API_KEY not set; sign up at crunchbase.com/api",
        }
    url = (
        "https://api.crunchbase.com/api/v4/searches/organizations"
        f"?user_key={urllib.parse.quote(api_key, safe='')}"
    )
    body = {
        "field_ids": [
            "identifier",
            "short_description",
            "website",
            "funding_total",
            "layoffs_count",
        ],
        "query": [
            {
                "type": "predicate",
                "field_id": "identifier",
                "operator_id": "contains",
                "values": [name.strip()],
            }
        ],
        "limit": 5,
    }
    try:
        data = _http_post_json(url, body=body, timeout=timeout)
    except _NET_ERRORS as exc:
        logger.error(f"Crunchbase lookup failed for '{name}'", exc_info=True)
        return _err("Crunchbase", exc)

    results: list[dict] = []
    for ent in data.get("entities") or []:
        if not isinstance(ent, dict):
            continue
        props = ent.get("properties") or {}
        ident = props.get("identifier") or {}
        results.append(
            {
                "uuid": ent.get("uuid") or "",
                "name": ident.get("value") if isinstance(ident, dict) else "",
                "permalink": (
                    ident.get("permalink") if isinstance(ident, dict) else ""
                ),
                "short_description": props.get("short_description") or "",
                "website": props.get("website") or "",
                "funding_total": props.get("funding_total") or {},
                "layoffs_count": props.get("layoffs_count") or 0,
            }
        )
    return {"results": results, "source": "Crunchbase", "count": len(results)}


# ─── 11. People Data Labs Person Enrichment (freemium, key-gated) ─────────────


def enrich_person_pdl(linkedin_or_email: str, timeout: int = 10) -> dict:
    """Enrich a person via People Data Labs v5 (freemium; key-gated stub).

    If PDL_API_KEY is unset, returns a stub. Otherwise calls the v5
    /person/enrich endpoint with email= or profile= depending on the input.

    Args:
        linkedin_or_email: Email address ("@" present) or LinkedIn URL/handle.
        timeout: Per-call timeout in seconds.

    Returns:
        Stub: ``{"person": str, "source": "PeopleDataLabs", "note": "..."}``;
        success: ``{"person": {...}, "source": "PeopleDataLabs"}``;
        error: ``{"error": str, "source": "PeopleDataLabs"}``.
    """
    if not linkedin_or_email or not linkedin_or_email.strip():
        return _err("PeopleDataLabs", "linkedin_or_email must be non-empty")
    api_key = os.environ.get("PDL_API_KEY")
    if not api_key:
        return {
            "person": linkedin_or_email,
            "source": "PeopleDataLabs",
            "note": (
                "PDL_API_KEY not set; sign up free at peopledatalabs.com "
                "(100 lookups/mo)"
            ),
        }
    cleaned = linkedin_or_email.strip()
    param = "email" if "@" in cleaned else "profile"
    qs = urllib.parse.urlencode({param: cleaned, "api_key": api_key})
    url = f"https://api.peopledatalabs.com/v5/person/enrich?{qs}"
    try:
        data = _http_get_json(url, timeout=timeout)
    except _NET_ERRORS as exc:
        logger.error(f"PDL enrichment failed for '{linkedin_or_email}'", exc_info=True)
        return _err("PeopleDataLabs", exc)

    record = data.get("data") or {}
    if not isinstance(record, dict):
        record = {}
    skills = record.get("skills") or []
    if not isinstance(skills, list):
        skills = []
    return {
        "person": {
            "full_name": record.get("full_name") or "",
            "job_title": record.get("job_title") or "",
            "job_company_name": record.get("job_company_name") or "",
            "linkedin_url": record.get("linkedin_url") or "",
            "skills": skills,
        },
        "source": "PeopleDataLabs",
    }
