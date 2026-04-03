"""Google Maps integration for geocoding, places, and location services.

Uses Google Maps Geocoding API and Places API with stdlib only (no
google-api-python-client).  Auth: GOOGLE_MAPS_API_KEY (preferred) or
service account via GOOGLE_SLIDES_CREDENTIALS_B64 (shared JWT flow).
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
_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
_PLACES_AUTOCOMPLETE_URL = (
    "https://maps.googleapis.com/maps/api/place/autocomplete/json"
)
_PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

_lock = threading.Lock()
_ssl_ctx = ssl.create_default_context()
_BATCH_DELAY_S = 0.05  # 50 ms between requests (stays under 50 QPS)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _get_api_key() -> Optional[str]:
    """Return the Google Maps API key from environment, or None."""
    return os.environ.get("GOOGLE_MAPS_API_KEY") or None


def _get_service_account_token() -> Optional[str]:
    """Obtain an OAuth2 bearer token via the shared service account."""
    try:
        from sheets_export import _get_access_token

        return _get_access_token()
    except ImportError:
        logger.warning("sheets_export not available for service account auth")
        return None
    except Exception as exc:
        logger.error("Service account token failed: %s", exc, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Internal request helper
# ---------------------------------------------------------------------------


def _maps_request(url: str, params: Dict[str, str]) -> Optional[dict]:
    """Make an authenticated GET to a Google Maps API endpoint.

    Tries API key first, falls back to service account bearer token.
    Returns parsed JSON or None on failure.
    """
    api_key = _get_api_key()
    headers: Dict[str, str] = {}
    if api_key:
        params["key"] = api_key
    else:
        token = _get_service_account_token()
        if not token:
            logger.error("Google Maps: no API key or service account configured")
            return None
        headers["Authorization"] = f"Bearer {token}"

    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(full_url, headers=headers)

    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        status = data.get("status") or ""
        if status not in ("OK", "ZERO_RESULTS"):
            logger.error(
                "Google Maps API error: %s", data.get("error_message") or status
            )
            return None
        return data
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            pass
        logger.error("Google Maps HTTP %d: %s", exc.code, body, exc_info=True)
        return None
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("Google Maps request failed: %s", exc, exc_info=True)
        return None


def _parse_geocode_result(result: dict) -> dict:
    """Extract a clean location dict from a Geocoding API result."""
    geometry = result.get("geometry") or {}
    location = geometry.get("location") or {}
    return {
        "formatted_address": result.get("formatted_address") or "",
        "lat": location.get("lat"),
        "lng": location.get("lng"),
        "place_id": result.get("place_id") or "",
        "types": result.get("types") or [],
        "address_components": result.get("address_components") or [],
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def geocode_address(address: str) -> dict:
    """Convert a street address to latitude/longitude coordinates.

    Args:
        address: Human-readable address (e.g. "1600 Amphitheatre Pkwy").

    Returns:
        Dict with formatted_address, lat, lng, place_id, types,
        address_components.  Contains 'error' key on failure.
    """
    if not address or not address.strip():
        return {"error": "address is required"}
    data = _maps_request(_GEOCODE_URL, {"address": address.strip()})
    if not data:
        return {"error": "geocoding request failed"}
    results = data.get("results") or []
    if not results:
        return {"error": "no results found", "address": address}
    return _parse_geocode_result(results[0])


def reverse_geocode(lat: float, lng: float) -> dict:
    """Convert coordinates to a human-readable address.

    Args:
        lat: Latitude (-90 to 90).
        lng: Longitude (-180 to 180).

    Returns:
        Dict with formatted_address, lat, lng, place_id, types,
        address_components.  Contains 'error' key on failure.
    """
    data = _maps_request(_GEOCODE_URL, {"latlng": f"{lat},{lng}"})
    if not data:
        return {"error": "reverse geocoding request failed"}
    results = data.get("results") or []
    if not results:
        return {"error": "no results found", "lat": lat, "lng": lng}
    return _parse_geocode_result(results[0])


def places_autocomplete(query: str) -> list[dict]:
    """Return location autocomplete suggestions for a partial query.

    Useful for media plan generator input fields where users type a
    city or region name.

    Args:
        query: Partial location string (e.g. "San Fran").

    Returns:
        List of dicts with description, place_id, structured_formatting.
    """
    if not query or not query.strip():
        return []
    data = _maps_request(
        _PLACES_AUTOCOMPLETE_URL,
        {"input": query.strip(), "types": "(regions)"},
    )
    if not data:
        return []
    return [
        {
            "description": p.get("description") or "",
            "place_id": p.get("place_id") or "",
            "structured_formatting": p.get("structured_formatting") or {},
        }
        for p in (data.get("predictions") or [])
    ]


def places_details(place_id: str) -> dict:
    """Retrieve full details for a place by its place_id.

    Args:
        place_id: Google Maps place ID (from geocoding or autocomplete).

    Returns:
        Dict with name, formatted_address, lat, lng, types, url,
        website, phone.  Contains 'error' key on failure.
    """
    if not place_id or not place_id.strip():
        return {"error": "place_id is required"}
    fields = "name,formatted_address,geometry,type,url,website,formatted_phone_number"
    data = _maps_request(
        _PLACES_DETAILS_URL,
        {"place_id": place_id.strip(), "fields": fields},
    )
    if not data:
        return {"error": "places details request failed"}
    result = data.get("result") or {}
    location = (result.get("geometry") or {}).get("location") or {}
    return {
        "name": result.get("name") or "",
        "formatted_address": result.get("formatted_address") or "",
        "lat": location.get("lat"),
        "lng": location.get("lng"),
        "types": result.get("types") or [],
        "url": result.get("url") or "",
        "website": result.get("website") or "",
        "phone": result.get("formatted_phone_number") or "",
    }


def batch_geocode(addresses: list[str]) -> list[dict]:
    """Geocode multiple addresses with rate-limiting.

    Designed for CG Automation's 397-location dataset.  Thread-safe
    via module lock with 50 ms delay between requests.

    Args:
        addresses: List of address strings to geocode.

    Returns:
        List of geocode result dicts (one per input, in order).
        Failed lookups include an 'error' key.
    """
    if not addresses:
        return []
    results: list[dict] = []
    total = len(addresses)
    succeeded = 0
    failed = 0

    with _lock:
        for idx, addr in enumerate(addresses):
            if not addr or not str(addr).strip():
                results.append({"error": "empty address", "index": idx})
                failed += 1
                continue
            try:
                result = geocode_address(str(addr))
                results.append(result)
                if "error" in result:
                    failed += 1
                else:
                    succeeded += 1
            except Exception as exc:
                logger.error(
                    "batch_geocode[%d/%d] '%s': %s",
                    idx + 1,
                    total,
                    addr,
                    exc,
                    exc_info=True,
                )
                results.append({"error": str(exc), "address": str(addr)})
                failed += 1
            if idx < total - 1:
                time.sleep(_BATCH_DELAY_S)
            if (idx + 1) % 50 == 0:
                logger.info(
                    "batch_geocode: %d/%d (ok=%d, fail=%d)",
                    idx + 1,
                    total,
                    succeeded,
                    failed,
                )

    logger.info(
        "batch_geocode complete: %d total, %d ok, %d fail", total, succeeded, failed
    )
    return results


def get_status() -> dict:
    """Health check for the Google Maps integration.

    Returns:
        Dict with configured flag, auth_mode, and endpoint availability.
    """
    api_key = _get_api_key()
    has_sa = False
    if not api_key:
        try:
            from sheets_export import _load_credentials

            has_sa = _load_credentials() is not None
        except ImportError:
            pass

    configured = bool(api_key or has_sa)
    auth_mode = "api_key" if api_key else ("service_account" if has_sa else "none")
    return {
        "configured": configured,
        "auth_mode": auth_mode,
        "endpoints": {
            "geocoding": configured,
            "reverse_geocoding": configured,
            "places_autocomplete": configured,
            "places_details": configured,
            "batch_geocode": configured,
        },
    }
