"""Pricing and VendorIQ route handlers.

Extracted from app.py to reduce its size.  Handles:
- GET  /api/pricing/live
- POST /api/vendor-iq/live-pricing
- POST /api/payscale-sync/salary
"""

import datetime
import json
import logging
import sys
import time
import urllib.error
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_pricing_get_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch pricing-related GET routes.  Returns True if handled."""
    if path == "/api/pricing/live":
        _handle_pricing_live(handler, path, parsed)
        return True
    if path == "/api/pricing/models":
        _handle_pricing_models(handler, path, parsed)
        return True
    return False


def handle_pricing_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch pricing-related POST routes.  Returns True if handled."""
    _fn = _PRICING_POST_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# GET handlers
# ---------------------------------------------------------------------------


def _handle_pricing_live(handler: Any, path: str, parsed: Any) -> None:
    """GET /api/pricing/live -- live CPC/CPA pricing for media plan wizard."""
    _app = sys.modules.get("app") or sys.modules.get("__main__")
    import urllib.parse

    try:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(handler.path).query)
        _pl_role = (params.get("role", [""])[0] or "").strip()
        _pl_location = (params.get("location", [""])[0] or "").strip()
        _pl_industry = (params.get("industry", [""])[0] or "").strip()

        # Check in-memory cache (5-min TTL per role+location+industry)
        _pl_cache_key = f"pricing:{_pl_role}:{_pl_location}:{_pl_industry}"
        _pl_cached = getattr(handler.server, "_pricing_cache", {}).get(_pl_cache_key)
        if _pl_cached and (time.time() - _pl_cached.get("_ts", 0)) < 300:
            handler._send_json(_pl_cached["data"])
            return

        # Build channel pricing from embedded benchmarks
        _pl_channels: list = []

        # Map industry string to benchmark key
        _pl_ind_key = (
            (_pl_industry or "general_entry_level")
            .lower()
            .replace(" ", "_")
            .replace("&", "")
            .replace("/", "_")
        )
        _pl_ind_aliases: dict = {
            "technology": "technology_software",
            "tech": "technology_software",
            "software": "technology_software",
            "healthcare": "healthcare_medical",
            "medical": "healthcare_medical",
            "nursing": "healthcare_medical",
            "finance": "finance_banking",
            "banking": "finance_banking",
            "manufacturing": "manufacturing_industrial",
            "industrial": "manufacturing_industrial",
            "retail": "retail_ecommerce",
            "ecommerce": "retail_ecommerce",
            "logistics": "logistics_transportation",
            "transportation": "logistics_transportation",
            "hospitality": "hospitality_food_service",
            "restaurant": "hospitality_food_service",
            "education": "education_training",
            "training": "education_training",
            "construction": "construction_real_estate",
            "real_estate": "construction_real_estate",
            "energy": "energy_utilities",
            "utilities": "energy_utilities",
            "government": "government_public_sector",
            "public_sector": "government_public_sector",
            "blue_collar": "blue_collar_trades",
            "trades": "blue_collar_trades",
            "skilled_trades": "blue_collar_trades",
        }
        _pl_ind_key = _pl_ind_aliases.get(_pl_ind_key, _pl_ind_key)

        # Hardcoded channel benchmarks
        _pl_channel_base: list = [
            {
                "name": "Indeed",
                "cpc_base": 0.50,
                "cpa_base": 25.0,
                "cph_base": 3500,
                "trend": "stable",
                "multiplier": 1.0,
            },
            {
                "name": "LinkedIn",
                "cpc_base": 2.50,
                "cpa_base": 55.0,
                "cph_base": 5000,
                "trend": "up",
                "multiplier": 1.8,
            },
            {
                "name": "ZipRecruiter",
                "cpc_base": 0.80,
                "cpa_base": 30.0,
                "cph_base": 3800,
                "trend": "down",
                "multiplier": 1.1,
            },
            {
                "name": "Glassdoor",
                "cpc_base": 1.20,
                "cpa_base": 35.0,
                "cph_base": 4200,
                "trend": "stable",
                "multiplier": 1.3,
            },
            {
                "name": "CareerBuilder",
                "cpc_base": 0.65,
                "cpa_base": 28.0,
                "cph_base": 3600,
                "trend": "down",
                "multiplier": 1.05,
            },
            {
                "name": "Monster",
                "cpc_base": 0.55,
                "cpa_base": 27.0,
                "cph_base": 3700,
                "trend": "stable",
                "multiplier": 1.0,
            },
            {
                "name": "Google for Jobs",
                "cpc_base": 0.35,
                "cpa_base": 20.0,
                "cph_base": 3200,
                "trend": "down",
                "multiplier": 0.8,
            },
            {
                "name": "Programmatic (Joveo)",
                "cpc_base": 0.40,
                "cpa_base": 18.0,
                "cph_base": 2800,
                "trend": "down",
                "multiplier": 0.7,
            },
        ]

        # Industry multipliers
        _pl_industry_mult: dict = {
            "healthcare_medical": 2.2,
            "technology_software": 1.5,
            "finance_banking": 1.4,
            "blue_collar_trades": 0.7,
            "hospitality_food_service": 0.6,
            "retail_ecommerce": 0.65,
            "logistics_transportation": 0.75,
            "general_entry_level": 0.8,
            "education_training": 0.85,
            "construction_real_estate": 0.9,
            "energy_utilities": 1.3,
            "government_public_sector": 1.1,
            "manufacturing_industrial": 0.85,
        }
        _pl_mult = _pl_industry_mult.get(_pl_ind_key, 1.0)

        # Region multiplier based on location
        _pl_loc_lower = _pl_location.lower()
        _pl_region_mult = 1.0
        _pl_region_name = "North America"
        if any(
            x in _pl_loc_lower
            for x in [
                "india",
                "asia",
                "singapore",
                "australia",
                "japan",
                "korea",
                "apac",
            ]
        ):
            _pl_region_mult = 0.6
            _pl_region_name = "APAC"
        elif any(
            x in _pl_loc_lower
            for x in [
                "uk",
                "united kingdom",
                "germany",
                "france",
                "europe",
                "london",
                "berlin",
            ]
        ):
            _pl_region_mult = 0.85
            _pl_region_name = "Europe"
        elif any(
            x in _pl_loc_lower
            for x in ["brazil", "mexico", "latin", "latam", "colombia", "argentina"]
        ):
            _pl_region_mult = 0.45
            _pl_region_name = "LATAM"

        for _ch in _pl_channel_base:
            _pl_channels.append(
                {
                    "name": _ch["name"],
                    "cpc": round(
                        _ch["cpc_base"]
                        * _ch["multiplier"]
                        * _pl_mult
                        * _pl_region_mult,
                        2,
                    ),
                    "cpa": round(
                        _ch["cpa_base"]
                        * _ch["multiplier"]
                        * _pl_mult
                        * _pl_region_mult,
                        2,
                    ),
                    "cph": int(_ch["cph_base"] * _pl_mult * _pl_region_mult),
                    "trend": _ch["trend"],
                }
            )

        _pl_source_parts = ["benchmarks"]

        # Enrich with Supabase channel_benchmarks if available
        _supabase_data_available = getattr(_app, "_supabase_data_available", False)
        if _supabase_data_available:
            get_channel_benchmarks = getattr(_app, "get_channel_benchmarks", None)
            if get_channel_benchmarks:
                try:
                    _pl_sb_benchmarks = get_channel_benchmarks(industry=_pl_industry)
                    if _pl_sb_benchmarks:
                        _pl_source_parts.append("supabase")
                        for _sb_row in _pl_sb_benchmarks[:10]:
                            _sb_name = (
                                _sb_row.get("channel") or _sb_row.get("name") or ""
                            )
                            if not _sb_name:
                                continue
                            _found = False
                            for _existing in _pl_channels:
                                if _existing["name"].lower() == _sb_name.lower():
                                    if _sb_row.get("cpc"):
                                        _existing["cpc"] = round(
                                            float(_sb_row["cpc"]), 2
                                        )
                                    if _sb_row.get("cpa"):
                                        _existing["cpa"] = round(
                                            float(_sb_row["cpa"]), 2
                                        )
                                    if _sb_row.get("cph"):
                                        _existing["cph"] = int(float(_sb_row["cph"]))
                                    if _sb_row.get("apply_rate"):
                                        _existing["apply_rate"] = str(
                                            _sb_row["apply_rate"]
                                        )
                                    _found = True
                                    break
                            if not _found and _sb_name:
                                _pl_channels.append(
                                    {
                                        "name": _sb_name,
                                        "cpc": round(float(_sb_row.get("cpc") or 0), 2)
                                        or None,
                                        "cpa": round(float(_sb_row.get("cpa") or 0), 2)
                                        or None,
                                        "cph": int(float(_sb_row.get("cph") or 0))
                                        or None,
                                        "apply_rate": str(
                                            _sb_row.get("apply_rate") or ""
                                        ),
                                        "trend": "stable",
                                    }
                                )
                except (
                    urllib.error.URLError,
                    OSError,
                    ValueError,
                    TypeError,
                ) as _sb_err:
                    logger.error(
                        f"Supabase benchmarks for /api/pricing/live failed: {_sb_err}",
                        exc_info=True,
                    )

        # Enrich with Adzuna live data if available
        _api_integrations_available = getattr(
            _app, "_api_integrations_available", False
        )
        _api_adzuna = getattr(_app, "_api_adzuna", None)
        if _api_integrations_available and _api_adzuna and _pl_role:
            try:
                _pl_adzuna_count = _api_adzuna.get_job_count(_pl_role, "us")
                if _pl_adzuna_count:
                    _pl_source_parts.append("adzuna")
                    _pl_comp_factor = 1.0
                    if isinstance(_pl_adzuna_count, (int, float)):
                        if _pl_adzuna_count > 50000:
                            _pl_comp_factor = 1.15
                        elif _pl_adzuna_count > 20000:
                            _pl_comp_factor = 1.05
                        elif _pl_adzuna_count < 5000:
                            _pl_comp_factor = 0.9
                        for _ch_item in _pl_channels:
                            if _ch_item.get("cpc"):
                                _ch_item["cpc"] = round(
                                    _ch_item["cpc"] * _pl_comp_factor, 2
                                )
            except (urllib.error.URLError, OSError, ValueError, TypeError) as _ae:
                logger.error(
                    f"Adzuna enrichment for /api/pricing/live failed: {_ae}",
                    exc_info=True,
                )

        # Sort channels: lowest CPA first
        _pl_channels.sort(key=lambda c: c.get("cpa") or 9999)

        _pl_response: dict = {
            "channels": _pl_channels,
            "source": "+".join(_pl_source_parts),
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
            "role": _pl_role,
            "location": _pl_location or "US (default)",
            "industry": _pl_ind_key,
            "region": _pl_region_name,
        }

        # Cache the response
        if not hasattr(handler.server, "_pricing_cache"):
            handler.server._pricing_cache = {}
        handler.server._pricing_cache[_pl_cache_key] = {
            "data": _pl_response,
            "_ts": time.time(),
        }

        handler._send_json(_pl_response)
    except Exception as e:
        logger.error("Live pricing API error: %s", e, exc_info=True)
        handler._send_json({"error": str(e), "channels": []}, status_code=500)


def _handle_pricing_models(handler: Any, path: str, parsed: Any) -> None:
    """GET /api/pricing/models -- outcome pricing tiers and per-role pricing.

    Query params: role_family (optional), location (optional), seniority (optional).
    If role_family is provided, returns specific pricing for that role.
    Otherwise, returns all pricing tiers.
    """
    import urllib.parse

    try:
        from outcome_engine import (
            calculate_outcome_price,
            get_all_pricing_tiers,
        )

        params = urllib.parse.parse_qs(urllib.parse.urlparse(handler.path).query)
        role_family = (params.get("role_family", [""])[0] or "").strip()
        location = (params.get("location", [""])[0] or "").strip()
        seniority = (params.get("seniority", [""])[0] or "").strip()

        if role_family:
            result = calculate_outcome_price(
                role_family=role_family,
                location=location,
                seniority=seniority or "mid",
            )
            handler._send_json(
                {
                    "pricing": result,
                    "all_tiers": get_all_pricing_tiers(),
                }
            )
        else:
            handler._send_json(
                {
                    "tiers": get_all_pricing_tiers(),
                    "note": "Add ?role_family=engineering&location=San Francisco&seniority=senior for specific pricing",
                }
            )
    except ImportError:
        handler._send_json(
            {"error": "Outcome engine not available", "tiers": {}},
            status_code=503,
        )
    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"Pricing models error: {e}", exc_info=True)
        handler._send_json({"error": str(e)}, status_code=400)


# ---------------------------------------------------------------------------
# POST handlers
# ---------------------------------------------------------------------------


def _handle_pricing_estimate(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/pricing/estimate -- estimate outcomes and pricing from plan data.

    Request body (JSON):
        budget (float, required): Total media budget in USD.
        role_family (str, required): Role category (engineering, healthcare, etc.).
        seniority (str, optional): entry, mid, senior, executive. Default: mid.
        location (str, optional): Geographic location.
        channels (list[str], optional): Channel list. Default: [indeed, linkedin, google].
        impressions (int, optional): Override estimated impressions.
        cpc (float, optional): Override average CPC.
        compare (bool, optional): If true, include pricing model comparison.

    Returns:
        Outcome estimate with funnel breakdown, pricing, and optional model comparison.
    """
    try:
        from outcome_engine import estimate_outcomes, compare_pricing_models

        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)

        # Validate required fields
        budget = data.get("budget")
        role_family = data.get("role_family") or data.get("role")
        if not budget:
            handler._send_json(
                {"error": "Missing required field: budget"},
                status_code=400,
            )
            return
        if not role_family:
            handler._send_json(
                {"error": "Missing required field: role_family"},
                status_code=400,
            )
            return

        result = estimate_outcomes(data)

        # Optionally include pricing model comparison
        if data.get("compare"):
            result["model_comparison"] = compare_pricing_models(data)

        handler._send_json(result)
    except json.JSONDecodeError:
        handler._send_json({"error": "Invalid JSON body"}, status_code=400)
    except ImportError:
        handler._send_json(
            {"error": "Outcome engine not available"},
            status_code=503,
        )
    except (ValueError, TypeError) as e:
        logger.error(f"Pricing estimate error: {e}", exc_info=True)
        handler._send_json({"error": str(e)}, status_code=400)


def _handle_vendor_iq_pricing(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/vendor-iq/live-pricing -- VendorIQ live pricing."""
    _app = sys.modules.get("app") or sys.modules.get("__main__")
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        vendors_requested = data.get("vendors") or ["indeed", "linkedin", "glassdoor"]

        # S72: Firecrawl scrape_job_board_pricing removed (module deleted).
        # Live pricing is no longer available; the Supabase vendor_profiles
        # block below still returns useful data, so we return an empty
        # vendors_result and let the frontend show "live pricing unavailable".
        vendors_result: dict = {
            v: {
                "error": "live_pricing_disabled",
                "source": "disabled",
                "note": "Firecrawl removed in S72; consult Supabase vendor profiles below.",
            }
            for v in vendors_requested
        }

        # Enrich with Supabase vendor profiles and benchmarks
        _supabase_data_available = getattr(_app, "_supabase_data_available", False)
        _viq_sb_profiles: list = []
        _viq_sb_benchmarks: list = []
        if _supabase_data_available:
            get_vendor_profiles = getattr(_app, "get_vendor_profiles", None)
            get_channel_benchmarks = getattr(_app, "get_channel_benchmarks", None)
            if get_vendor_profiles:
                try:
                    _viq_sb_profiles = get_vendor_profiles()
                except (urllib.error.URLError, OSError, ValueError) as sb_err:
                    logger.error(
                        f"Supabase vendor profiles fetch failed: {sb_err}",
                        exc_info=True,
                    )
            if get_channel_benchmarks:
                try:
                    _viq_industry = data.get("industry") or ""
                    _viq_sb_benchmarks = get_channel_benchmarks(industry=_viq_industry)
                except (urllib.error.URLError, OSError, ValueError) as sb_err:
                    logger.error(
                        f"Supabase benchmarks fetch for VendorIQ failed: {sb_err}",
                        exc_info=True,
                    )

        _viq_response: dict = {"vendors": vendors_result, "status": "ok"}
        if _viq_sb_profiles:
            _viq_response["supabase_vendor_profiles"] = _viq_sb_profiles
        if _viq_sb_benchmarks:
            _viq_response["supabase_channel_benchmarks"] = _viq_sb_benchmarks

        # Enrich with Adzuna top companies
        _api_integrations_available = getattr(
            _app, "_api_integrations_available", False
        )
        _api_adzuna = getattr(_app, "_api_adzuna", None)
        if _api_integrations_available and _api_adzuna:
            try:
                _viq_role = data.get("role") or data.get("keyword") or ""
                if _viq_role:
                    _viq_companies = _api_adzuna.get_top_companies(_viq_role, "us")
                    if _viq_companies:
                        _viq_response["adzuna_top_companies"] = _viq_companies
                        logger.info(
                            "Enriched /api/vendor-iq with adzuna top_companies data"
                        )
            except (urllib.error.URLError, OSError, ValueError, TypeError) as _ae:
                logger.error(
                    "Adzuna enrichment for vendor-iq failed: %s", _ae, exc_info=True
                )

        handler._send_json(_viq_response)
    except json.JSONDecodeError:
        handler.send_response(400)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
    except Exception as e:
        logger.error("VendorIQ live-pricing error: %s", e, exc_info=True)
        handler._send_json({"error": "Internal server error", "status": "error"})


def _handle_payscale_salary(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/payscale-sync/salary -- PayScale salary data."""
    _app = sys.modules.get("app") or sys.modules.get("__main__")
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)

        # S72: Firecrawl scrape_salary_data removed (module deleted).
        # The Supabase salary_data block below + API-integrations block (BLS/
        # Adzuna) still provide salary signal, so we start with an empty
        # PayScale result and let downstream enrichment populate it.
        result: dict = {
            "role": data.get("role") or "",
            "location": data.get("location") or "",
            "source": "disabled",
            "note": "PayScale (Firecrawl) removed in S72; using Supabase + BLS only.",
        }

        # Enrich with Supabase salary data
        _supabase_data_available = getattr(_app, "_supabase_data_available", False)
        if _supabase_data_available and isinstance(result, dict):
            get_salary_data = getattr(_app, "get_salary_data", None)
            if get_salary_data:
                try:
                    _ps_role = data.get("role") or ""
                    _ps_location = data.get("location") or ""
                    _ps_salaries = get_salary_data(role=_ps_role, location=_ps_location)
                    if _ps_salaries:
                        result["supabase_salary_data"] = _ps_salaries
                except (urllib.error.URLError, OSError, ValueError) as sb_err:
                    logger.error(
                        f"Supabase salary data fetch for PayScale failed: {sb_err}",
                        exc_info=True,
                    )

        # Enrich with API integrations
        _api_integrations_available = getattr(
            _app, "_api_integrations_available", False
        )
        if _api_integrations_available and isinstance(result, dict):
            _ps_api_data: dict = {}
            _ps_role_str = data.get("role") or ""

            # BLS occupational employment/wage data
            _api_bls = getattr(_app, "_api_bls", None)
            if _api_bls and _ps_role_str:
                try:
                    _ps_soc = data.get("soc_code") or ""
                    if _ps_soc:
                        _bls_oes = _api_bls.get_occupational_employment(_ps_soc)
                        if _bls_oes:
                            _ps_api_data["bls_occupational_employment"] = _bls_oes
                            logger.info("Enriched /api/payscale-sync with bls OES data")
                except (urllib.error.URLError, OSError, ValueError, TypeError) as _be:
                    logger.error(
                        "BLS enrichment for payscale-sync failed: %s",
                        _be,
                        exc_info=True,
                    )

            # Adzuna salary histogram
            _api_adzuna = getattr(_app, "_api_adzuna", None)
            if _api_adzuna and _ps_role_str:
                try:
                    _ps_salary_hist = _api_adzuna.get_salary_histogram(
                        _ps_role_str, "us"
                    )
                    if _ps_salary_hist:
                        _ps_api_data["adzuna_salary_histogram"] = _ps_salary_hist
                        logger.info(
                            "Enriched /api/payscale-sync with adzuna salary histogram"
                        )
                except (urllib.error.URLError, OSError, ValueError, TypeError) as _ae:
                    logger.error(
                        "Adzuna enrichment for payscale-sync failed: %s",
                        _ae,
                        exc_info=True,
                    )

            if _ps_api_data:
                result["api_enrichment"] = _ps_api_data

        handler._send_json(result)
    except json.JSONDecodeError:
        handler.send_response(400)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
    except Exception as e:
        logger.error("PayScale salary error: %s", e, exc_info=True)
        handler._send_json({"error": "Internal server error"})


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_PRICING_POST_ROUTE_MAP: dict[str, Any] = {
    "/api/pricing/estimate": _handle_pricing_estimate,
    "/api/vendor-iq/live-pricing": _handle_vendor_iq_pricing,
    "/api/payscale-sync/salary": _handle_payscale_salary,
}
