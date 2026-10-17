"""Pricing and VendorIQ route handlers.

Extracted from app.py to reduce its size.  Handles:
- GET  /api/pricing/live
- POST /api/vendor-iq/live-pricing
- POST /api/payscale-sync/salary
"""

import json
import logging
import sys
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


# Display names for by_channel keys in data/recruitment_benchmarks_2026_deep.json
_CHANNEL_NAMES = {
    "indeed": "Indeed",
    "linkedin": "LinkedIn",
    "ziprecruiter": "ZipRecruiter",
    "monster": "Monster",
    "glassdoor": "Glassdoor",
    "dice": "Dice",
    "programmatic_appcast_jobtarget": "Programmatic (Appcast / JobTarget)",
}


def _usd_range(node: Any, low: str, high: str) -> Any:
    """{"low", "median", "high"} from a data node, or None when any is missing."""
    if not isinstance(node, dict):
        return None
    out = {}
    for key, src in (("low", low), ("median", "median"), ("high", high)):
        value = node.get(src)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        out[key] = float(value)
    return out


NOVA_ESTIMATE_LABEL = "Nova working estimate"
APPCAST_AGGREGATE_LABEL = "Appcast all-publisher aggregate (not board-specific)"


def _ids_label(ids: list, board_note: bool = True) -> str:
    import cph_benchmark

    if not ids:
        return NOVA_ESTIMATE_LABEL
    appcast = [i for i in ids if i.startswith("appcast_")]
    if len(appcast) == len(ids):
        return APPCAST_AGGREGATE_LABEL if board_note else "Appcast all-publisher aggregate"
    label = cph_benchmark.source_label(ids)
    return label + "; Appcast figures are all-publisher aggregates" if appcast else label


def channel_source_label(cpc_ids: list, cpa_ids: list) -> str:
    """What a channel card says its CPC / CPA ranges rest on, from the ids that
    remain on each cell (not the row's wider list).

    No ids on either cell -> "Nova working estimate" (never a stand-in
    publisher). Only Appcast ids -> its all-publisher aggregate, which is not a
    per-board figure. Anything else is named from the registry, with the same
    aggregate caveat when Appcast is among them. When ONE cell kept a source and
    the other kept none, the label names the cell ("CPA: Appcast all-publisher
    aggregate; CPC: Nova working estimate") so the sourced figure never lends
    its provenance to the estimate beside it."""
    cpc = list(dict.fromkeys(str(i) for i in cpc_ids))
    cpa = list(dict.fromkeys(str(i) for i in cpa_ids))
    if bool(cpc) != bool(cpa):
        sourced, bare = ("CPC", "CPA") if cpc else ("CPA", "CPC")
        return (
            f"{sourced}: {_ids_label(cpc or cpa, board_note=False)}; "
            f"{bare}: {NOVA_ESTIMATE_LABEL}"
        )
    return _ids_label(list(dict.fromkeys(cpc + cpa)))


def build_live_pricing(role: str, location: str, industry: str) -> dict:
    """What the wizard's "Market Pricing Benchmarks" panel shows (step 4).

    Everything here is READ from ``data/recruitment_benchmarks_2026_deep.json``
    (the file step 1's metro nudges and the preview's cost-per-hire grade read):

      * ``channels`` -- ``by_channel.*.typical_cpc_usd`` / ``typical_cpa_usd``,
        US ranges (low / median / high), NOT adjusted per industry, role or city;
      * ``industry`` -- ``by_industry.<key>`` CPC / CPA quartiles (p25 / median /
        p75) for the industry / role, when one row clearly fits. The cost-per-hire
        quartiles are Nova working estimates marked ``illustrative_unverified``
        (provenance audit 2026-10-17), so ``cph`` is None with an empty source;
      * ``metro`` -- the SAME ``by_metro`` multipliers step 1 quotes
        (plan_copilot.metro_benchmark), shown beside the ranges, never applied.

    It used to be a hand-typed table (per-channel bases x an industry multiplier
    of 2.2 for healthcare x a city index with Los Angeles at 1.20 x a seniority
    multiplier), which disagreed with the benchmark file (Los Angeles 1.24) and
    with the preview, plus Adzuna / Supabase overlays scaled by typed factors.
    Those are gone, and the preview no longer grades cost per hire against
    per-industry quartiles (cph_benchmark.lookup returns None for them).
    """
    import cph_benchmark
    import plan_copilot

    data = cph_benchmark.data()
    channels = []
    source_ids: list = []
    for key, node in (data.get("by_channel") or {}).items():
        if key.startswith("_") or not isinstance(node, dict):
            continue
        cpc = _usd_range(node.get("typical_cpc_usd"), "low", "high")
        cpa = _usd_range(node.get("typical_cpa_usd"), "low", "high")
        if cpc is None and cpa is None:
            continue
        # the ids on the cells the card prints (CPC / CPA), not the row's wider
        # list: a row id that backs only another claim must not label them
        cell_ids: dict = {}
        for cell in ("typical_cpc_usd", "typical_cpa_usd"):
            raw = node.get(cell)
            cell_ids[cell] = [
                str(i) for i in ((raw.get("source_ids") if isinstance(raw, dict) else None) or [])
            ]
        ids = list(dict.fromkeys(cell_ids["typical_cpc_usd"] + cell_ids["typical_cpa_usd"]))
        source_ids.extend(ids)
        channels.append(
            {
                "name": _CHANNEL_NAMES.get(key, key.replace("_", " ").title()),
                "cpc": cpc,
                "cpa": cpa,
                "source": channel_source_label(
                    cell_ids["typical_cpc_usd"], cell_ids["typical_cpa_usd"]
                ),
            }
        )
    channels.sort(key=lambda c: (c["cpa"] or {}).get("median", float("inf")))

    industry_block: Any = None
    ind_key = cph_benchmark.benchmark_key((industry or "").strip().lower(), [role])
    row = (data.get("by_industry") or {}).get(ind_key or "")
    if isinstance(row, dict):
        industry_block = {"label": (ind_key or "").replace("_", " ")}
        for field, name in (
            ("cpc_range_usd", "cpc"),
            ("cpa_range_usd", "cpa"),
            ("cph_range_usd", "cph"),
        ):
            raw = row.get(field)
            node: dict = raw if isinstance(raw, dict) else {}
            # a row the data marks illustrative_unverified is never shown as a benchmark
            if str(node.get("status") or "") == "illustrative_unverified":
                industry_block[name] = None
                industry_block[name + "_source"] = ""
                continue
            rng = _usd_range(raw, "p25", "p75")
            industry_block[name] = rng
            ids = [str(i) for i in node.get("source_ids") or []]
            industry_block[name + "_source"] = cph_benchmark.source_label(ids)
            source_ids.extend(ids)

    metro_block = None
    metro = plan_copilot.metro_benchmark(location) if location else None
    if metro:
        metro_block = {
            "name": metro["name"],
            "cpa_mult": metro["mult"],
            "cpc_mult": metro["cpc_mult"],
            "source": metro["label"],
        }
        source_ids.extend(metro["source_ids"])

    meta = data.get("metadata") or {}
    return {
        "channels": channels,
        "industry": industry_block,
        "metro": metro_block,
        "source": "Benchmark data, sources listed in the file: "
        + cph_benchmark.source_label(dict.fromkeys(source_ids)),
        # What the cells DESCRIBE (their data period) is not when the file was
        # last edited: a provenance edit moves last_updated without moving a
        # single figure, so "Data last updated <edit date>" read as fresh data.
        "data_vintage": str(meta.get("data_period") or ""),
        "reviewed": str(meta.get("last_updated") or ""),
        # kept for clients that still read it; same value as ``reviewed``
        "data_updated": str(meta.get("last_updated") or ""),
        "role": role,
        "location": location,
    }


def _handle_pricing_live(handler: Any, path: str, parsed: Any) -> None:
    """GET /api/pricing/live -- market pricing benchmarks for the media plan wizard."""
    import urllib.parse

    try:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(handler.path).query)
        role = (params.get("role", [""])[0] or "").strip()[:200]
        location = (params.get("location", [""])[0] or "").strip()[:200]
        industry = (params.get("industry", [""])[0] or "").strip()[:100]
        handler._send_json(build_live_pricing(role, location, industry))
    except Exception as e:
        logger.error("Live pricing API error: %s", e, exc_info=True)
        handler._send_json({"error": "Pricing unavailable", "channels": []}, status_code=500)


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
