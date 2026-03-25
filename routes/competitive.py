"""Competitive intelligence POST route handlers.

Extracted from app.py to reduce its size.  Handles:
- POST /api/competitive/scrape
- POST /api/competitive/analyze
- POST /api/competitive/download/excel
- POST /api/competitive/download/ppt
"""

import json
import logging
import sys
import urllib.error
from typing import Any

from routes.utils import read_json_body

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_competitive_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch competitive intelligence POST routes.  Returns True if handled."""
    _fn = _COMPETITIVE_POST_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _handle_competitive_scrape(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/competitive/scrape -- career page scrape via Firecrawl."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        domain = data.get("domain") or ""
        if not domain:
            handler._send_json({"error": "Missing 'domain' field", "status": "error"})
            return

        _app = sys.modules.get("__main__") or sys.modules.get("app")
        _firecrawl_available = getattr(_app, "_firecrawl_available", False)

        if not _firecrawl_available:
            handler._send_json({"error": "Firecrawl not available", "status": "error"})
            return

        analyze_competitor_careers = getattr(_app, "analyze_competitor_careers", None)
        result = analyze_competitor_careers(company_domain=domain)

        # Enrich with Jooble market data
        _api_integrations_available = getattr(
            _app, "_api_integrations_available", False
        )
        _api_jooble = getattr(_app, "_api_jooble", None)
        if _api_integrations_available and _api_jooble and isinstance(result, dict):
            try:
                _ci_role = data.get("role") or data.get("keyword") or ""
                _ci_location = data.get("location") or ""
                if _ci_role:
                    _jooble_jobs = _api_jooble.search_jobs(_ci_role, _ci_location)
                    if _jooble_jobs:
                        result["jooble_market_comparison"] = _jooble_jobs
                        logger.info("Enriched /api/competitive/scrape with jooble data")
            except (urllib.error.URLError, OSError, ValueError, TypeError) as _je:
                logger.error(
                    "Jooble enrichment for competitive/scrape failed: %s",
                    _je,
                    exc_info=True,
                )

        # Enrich with JobSpy hiring data
        _jobspy_available = getattr(_app, "_jobspy_available", False)
        _jobspy_scrape_jobs = getattr(_app, "_jobspy_scrape_jobs", None)
        if _jobspy_available and _jobspy_scrape_jobs and isinstance(result, dict):
            try:
                _ci_company = data.get("company") or domain.split(".")[0]
                _ci_js_jobs = _jobspy_scrape_jobs(_ci_company, "USA", results_wanted=15)
                if _ci_js_jobs:
                    result["jobspy_hiring_data"] = {
                        "postings_found": len(_ci_js_jobs),
                        "sample_postings": _ci_js_jobs[:5],
                        "sources": list({j.get("site") or "" for j in _ci_js_jobs}),
                    }
                    logger.info(
                        "Enriched /api/competitive/scrape with jobspy hiring data"
                    )
            except (ValueError, TypeError, KeyError, OSError) as _jse:
                logger.error(
                    "JobSpy enrichment for competitive/scrape failed: %s",
                    _jse,
                    exc_info=True,
                )

        # Enrich with Tavily company research
        _tavily_available = getattr(_app, "_tavily_available", False)
        _tavily_research_company = getattr(_app, "_tavily_research_company", None)
        if _tavily_available and _tavily_research_company and isinstance(result, dict):
            try:
                _ci_company_name = data.get("company") or domain.split(".")[0]
                _tav_research = _tavily_research_company(_ci_company_name)
                if _tav_research:
                    result["tavily_company_research"] = _tav_research
                    logger.info(
                        "Enriched /api/competitive/scrape with tavily company research"
                    )
            except (urllib.error.URLError, OSError, ValueError, TypeError) as _te:
                logger.error(
                    "Tavily enrichment for competitive/scrape failed: %s",
                    _te,
                    exc_info=True,
                )

        handler._send_json(result)
        # PostHog tracking
        if hasattr(handler, "_ph_track"):
            handler._ph_track(
                "competitive_analysis_run",
                {"domain": domain, "endpoint": "/api/competitive/scrape"},
            )
    except Exception as e:
        logger.error("Competitive scrape error: %s", e, exc_info=True)
        handler._send_json({"error": "Internal server error", "status": "error"})


def _handle_competitive_analyze(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/competitive/analyze -- full competitive analysis."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        from competitive_intel import run_full_analysis

        result = run_full_analysis(
            company_name=data.get("company_name") or "",
            competitors=data.get("competitors") or [],
            industry=data.get("industry", "general_entry_level"),
            roles=data.get("roles"),
        )
        handler._send_json(result)
        if hasattr(handler, "_ph_track"):
            handler._ph_track(
                "competitive_analysis_run",
                {
                    "company_name": data.get("company_name") or "",
                    "endpoint": "/api/competitive/analyze",
                },
            )
    except Exception as e:
        logger.error("Competitive analysis error: %s", e, exc_info=True)
        handler._send_json({"error": "Internal server error", "status": "error"})


def _handle_competitive_download_excel(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/competitive/download/excel -- Excel download."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        from competitive_intel import generate_competitive_excel

        excel_bytes = generate_competitive_excel(
            brief=data.get("brief", data),
            company_name=data.get("company_name", "Company"),
        )
        handler.send_response(200)
        handler.send_header(
            "Content-Type",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        handler.send_header(
            "Content-Disposition",
            "attachment; filename=competitive_intelligence.xlsx",
        )
        handler.send_header("Content-Length", str(len(excel_bytes)))
        handler.end_headers()
        handler.wfile.write(excel_bytes)
    except Exception as e:
        logger.error("Competitive Excel error: %s", e, exc_info=True)
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Excel export failed"}).encode())


def _handle_competitive_download_ppt(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/competitive/download/ppt -- PPT download."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        from competitive_intel import generate_competitive_ppt

        ppt_bytes = generate_competitive_ppt(
            brief=data.get("brief", data),
            company_name=data.get("company_name", "Company"),
        )
        handler.send_response(200)
        handler.send_header(
            "Content-Type",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        )
        handler.send_header(
            "Content-Disposition",
            "attachment; filename=competitive_intelligence.pptx",
        )
        handler.send_header("Content-Length", str(len(ppt_bytes)))
        handler.end_headers()
        handler.wfile.write(ppt_bytes)
    except Exception as e:
        logger.error("Competitive PPT error: %s", e, exc_info=True)
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "PPT export failed"}).encode())


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_COMPETITIVE_POST_ROUTE_MAP: dict[str, Any] = {
    "/api/competitive/scrape": _handle_competitive_scrape,
    "/api/competitive/analyze": _handle_competitive_analyze,
    "/api/competitive/download/excel": _handle_competitive_download_excel,
    "/api/competitive/download/ppt": _handle_competitive_download_ppt,
}
