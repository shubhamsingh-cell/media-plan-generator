"""Google Knowledge Graph Search API integration for entity enrichment.

Uses API key authentication (GOOGLE_KG_API_KEY or GOOGLE_MAPS_API_KEY env var).
Endpoint: https://kgsearch.googleapis.com/v1/entities:search

Free tier: 100,000 calls/day.

Functions:
    enrich_company   -- Look up a company by name.
    enrich_job_title -- Look up an occupation / job title.
    enrich_location  -- Look up a city or region.
    batch_enrich     -- Batch enrichment with rate limiting (10 req/sec).
    get_status       -- Health check for the integration.
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_KG_ENDPOINT = "https://kgsearch.googleapis.com/v1/entities:search"
_REQUEST_TIMEOUT = 10  # seconds
_RATE_LIMIT = 10  # requests per second
_rate_lock = threading.Lock()
_last_request_times: List[float] = []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_api_key() -> Optional[str]:
    """Return the configured API key, or None if missing."""
    return (
        os.environ.get("GOOGLE_KG_API_KEY")
        or os.environ.get("GOOGLE_MAPS_API_KEY")
        or None
    )


def _rate_limit_wait() -> None:
    """Block until a request slot is available (10 req/sec sliding window)."""
    with _rate_lock:
        now = time.monotonic()
        # Prune timestamps older than 1 second
        while _last_request_times and _last_request_times[0] < now - 1.0:
            _last_request_times.pop(0)
        if len(_last_request_times) >= _RATE_LIMIT:
            sleep_for = 1.0 - (now - _last_request_times[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
        _last_request_times.append(time.monotonic())


def _kg_search(
    query: str,
    types: Optional[List[str]] = None,
    limit: int = 1,
    languages: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Execute a Knowledge Graph Search API call.

    Args:
        query: Free-text search query.
        types: Schema.org type filters (e.g. ["Organization", "Person"]).
        limit: Maximum number of results to return.
        languages: Language codes for results (default ["en"]).

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.warning("Google Knowledge Graph API key not configured")
        return None

    params: Dict[str, str] = {
        "query": query,
        "key": api_key,
        "limit": str(limit),
        "indent": "false",
    }
    if types:
        params["types"] = ",".join(types)
    if languages:
        params["languages"] = ",".join(languages)
    else:
        params["languages"] = "en"

    url = f"{_KG_ENDPOINT}?{urllib.parse.urlencode(params)}"

    _rate_limit_wait()

    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, context=ctx, timeout=_REQUEST_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error(
            "Knowledge Graph API returned %d: %s",
            exc.code,
            error_body,
            exc_info=True,
        )
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Knowledge Graph API request failed: %s", exc, exc_info=True)
        return None


def _extract_result(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Extract the first itemListElement result from a KG response."""
    if not data:
        return None
    elements = data.get("itemListElement") or []
    if not elements:
        return None
    return elements[0].get("result") or None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich_company(company_name: str) -> Dict[str, Any]:
    """Look up a company in Google Knowledge Graph.

    Args:
        company_name: Company name to search for.

    Returns:
        Dict with keys: name, description, url, industry, employee_count,
        founded, logo_url, knowledge_graph_id. Missing fields default to
        empty string or None.
    """
    empty: Dict[str, Any] = {
        "name": company_name,
        "description": "",
        "url": "",
        "industry": "",
        "employee_count": None,
        "founded": "",
        "logo_url": "",
        "knowledge_graph_id": "",
    }

    try:
        raw = _kg_search(company_name, types=["Organization", "Corporation"], limit=1)
        result = _extract_result(raw)
        if not result:
            return empty

        detailed = result.get("detailedDescription") or {}
        image = result.get("image") or {}

        return {
            "name": result.get("name") or company_name,
            "description": (
                detailed.get("articleBody") or result.get("description") or ""
            ),
            "url": detailed.get("url") or result.get("url") or "",
            "industry": result.get("description") or "",
            "employee_count": None,  # KG does not reliably provide this
            "founded": "",  # KG does not reliably provide this
            "logo_url": image.get("contentUrl") or "",
            "knowledge_graph_id": result.get("@id") or "",
        }
    except Exception as exc:
        logger.error(
            "enrich_company failed for %r: %s", company_name, exc, exc_info=True
        )
        return empty


def enrich_job_title(title: str) -> Dict[str, Any]:
    """Look up an occupation / job title in Google Knowledge Graph.

    Args:
        title: Job title or occupation to search for.

    Returns:
        Dict with keys: name, description, related_titles, category.
    """
    empty: Dict[str, Any] = {
        "name": title,
        "description": "",
        "related_titles": [],
        "category": "",
    }

    try:
        raw = _kg_search(title, types=["JobPosting", "Occupation"], limit=3)
        if not raw:
            # Fallback: search without type filter
            raw = _kg_search(title, limit=3)

        elements = (raw.get("itemListElement") or []) if raw else []
        if not elements:
            return empty

        primary = elements[0].get("result") or {}
        detailed = primary.get("detailedDescription") or {}

        related: List[str] = []
        for elem in elements[1:]:
            r = elem.get("result") or {}
            name = r.get("name") or ""
            if name and name.lower() != title.lower():
                related.append(name)

        return {
            "name": primary.get("name") or title,
            "description": (
                detailed.get("articleBody") or primary.get("description") or ""
            ),
            "related_titles": related,
            "category": primary.get("description") or "",
        }
    except Exception as exc:
        logger.error("enrich_job_title failed for %r: %s", title, exc, exc_info=True)
        return empty


def enrich_location(location: str) -> Dict[str, Any]:
    """Look up a city or region in Google Knowledge Graph.

    Args:
        location: City, region, or place name to search for.

    Returns:
        Dict with keys: name, description, population, coordinates, country.
    """
    empty: Dict[str, Any] = {
        "name": location,
        "description": "",
        "population": None,
        "coordinates": None,
        "country": "",
    }

    try:
        raw = _kg_search(
            location,
            types=["City", "AdministrativeArea", "Place", "Country"],
            limit=1,
        )
        result = _extract_result(raw)
        if not result:
            return empty

        detailed = result.get("detailedDescription") or {}

        # Try to extract coordinates from the result
        coords = None
        geo = result.get("geo") or {}
        lat = geo.get("latitude")
        lon = geo.get("longitude")
        if lat is not None and lon is not None:
            coords = {"lat": lat, "lng": lon}

        return {
            "name": result.get("name") or location,
            "description": (
                detailed.get("articleBody") or result.get("description") or ""
            ),
            "population": None,  # KG does not reliably provide this
            "coordinates": coords,
            "country": result.get("description") or "",
        }
    except Exception as exc:
        logger.error("enrich_location failed for %r: %s", location, exc, exc_info=True)
        return empty


def batch_enrich(entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Batch enrichment with rate limiting (10 req/sec).

    Each entity dict must have a "type" key ("company", "job_title", or
    "location") and a "name" key with the query string.

    Args:
        entities: List of dicts, each with "type" and "name" keys.

    Returns:
        List of enrichment result dicts in the same order as input.
    """
    results: List[Dict[str, Any]] = []
    dispatch = {
        "company": enrich_company,
        "job_title": enrich_job_title,
        "location": enrich_location,
    }

    for entity in entities:
        entity_type = (entity.get("type") or "").lower()
        entity_name = entity.get("name") or ""

        if not entity_name:
            results.append({"error": "missing name", "input": entity})
            continue

        handler = dispatch.get(entity_type)
        if not handler:
            results.append({"error": f"unknown type: {entity_type}", "input": entity})
            continue

        try:
            enriched = handler(entity_name)
            enriched["_input_type"] = entity_type
            results.append(enriched)
        except Exception as exc:
            logger.error(
                "batch_enrich failed for %r (%s): %s",
                entity_name,
                entity_type,
                exc,
                exc_info=True,
            )
            results.append({"error": str(exc), "input": entity})

    return results


def get_status() -> Dict[str, Any]:
    """Health check for the Google Knowledge Graph integration.

    Returns:
        Status dict with "configured", "api_key_source", "endpoint",
        and "daily_limit" keys.
    """
    kg_key = os.environ.get("GOOGLE_KG_API_KEY") or ""
    maps_key = os.environ.get("GOOGLE_MAPS_API_KEY") or ""

    if kg_key:
        source = "GOOGLE_KG_API_KEY"
        configured = True
    elif maps_key:
        source = "GOOGLE_MAPS_API_KEY"
        configured = True
    else:
        source = None
        configured = False

    return {
        "configured": configured,
        "api_key_source": source,
        "endpoint": _KG_ENDPOINT,
        "daily_limit": 100_000,
        "rate_limit_per_sec": _RATE_LIMIT,
    }
