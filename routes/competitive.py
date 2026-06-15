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
    """POST /api/competitive/scrape -- career page scrape (S72: scraper removed).

    The previous implementation called firecrawl_enrichment.analyze_competitor_careers,
    which has been deleted with the rest of the Firecrawl module. The endpoint
    still serves the Tavily / Jooble / JobSpy enrichments below as graceful
    fallbacks so the frontend continues to render *something*.
    """
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        domain = str(data.get("domain") or "").strip()
        if not domain:
            handler._send_json({"error": "Missing 'domain' field", "status": "error"})
            return

        _app = sys.modules.get("app") or sys.modules.get("__main__")
        # S72: analyze_competitor_careers was Firecrawl-only; module deleted.
        # Skip the primary scrape and let the downstream enrichments populate
        # `result` instead. If every enrichment also fails, we still return 200
        # with an explanatory note rather than a hard error.
        result: dict = {
            "domain": domain,
            "primary_source": "disabled",
            "note": "Career-page scraping removed in S72. Returning enrichment-only data.",
        }
        # S72: primary career-page scrape removed (was firecrawl_enrichment.
        # analyze_competitor_careers). `result` was pre-populated above with
        # a "disabled" stub; the enrichments below add ATS + Jooble + JobSpy +
        # Tavily data so the frontend still renders signal.

        # ── S90: Primary open-role count from public ATS feeds ──────────────
        # This is the REAL, uncapped "open roles" number for the employer.
        # It replaces the previous behaviour where the card's "open roles"
        # figure came from a JobSpy *keyword sample* capped at results_wanted
        # (so every competitor showed the same small, rounded number that
        # reflected the scrape limit, not actual openings). resolve_competitor_ats
        # maps the company -> its Greenhouse/Lever/Ashby board and reads
        # total_available (true count before any display cap). Employers on a
        # private ATS (e.g. Google/Meta on Workday) have no public board, so we
        # flag ats_status="no_public_board" and the frontend shows an honest
        # note instead of a fabricated count.
        try:
            from api_enrichment import resolve_competitor_ats

            _ats_company = data.get("company") or domain.split(".")[0]
            _ats = resolve_competitor_ats(_ats_company)
            if (
                isinstance(_ats, dict)
                and not _ats.get("no_public_board")
                and (_ats.get("total_available") or 0) > 0
            ):
                _ats_total = int(_ats.get("total_available") or 0)
                # `total_jobs` is the field the frontend reads as "open roles".
                result["total_jobs"] = _ats_total
                result["primary_source"] = f"ATS:{_ats.get('provider') or 'public'}"
                result["ats_hiring_data"] = {
                    "provider": _ats.get("provider"),
                    "board": _ats.get("board"),
                    "total_available": _ats_total,
                    "job_count": _ats.get("job_count") or 0,
                    "source_url": _ats.get("url"),
                }
                # Real hiring locations from the ATS location histogram (top 6).
                _loc_summary = _ats.get("locations_summary") or {}
                if _loc_summary:
                    result["locations"] = [
                        loc
                        for loc, _cnt in sorted(
                            _loc_summary.items(),
                            key=lambda kv: kv[1],
                            reverse=True,
                        )
                    ][:6]
                # Real departments/categories from the sampled jobs (top 8).
                _dept_counts: dict = {}
                for _j in _ats.get("jobs") or []:
                    _dept = (_j.get("department") or "").strip()
                    if _dept:
                        _dept_counts[_dept] = (_dept_counts.get(_dept) or 0) + 1
                if _dept_counts:
                    result["job_categories"] = [
                        dept
                        for dept, _cnt in sorted(
                            _dept_counts.items(),
                            key=lambda kv: kv[1],
                            reverse=True,
                        )
                    ][:8]
                logger.info(
                    "Enriched /api/competitive/scrape via ATS: %d open roles for "
                    "'%s' (%s)",
                    _ats_total,
                    _ats_company,
                    _ats.get("provider"),
                )
            else:
                # No public ATS board -> tell the frontend to be honest about it
                # rather than dressing up a keyword sample as "open roles".
                result["ats_status"] = "no_public_board"
                logger.info(
                    "No public ATS board for '%s'; flagged no_public_board",
                    _ats_company,
                )
        except ImportError:
            logger.warning(
                "resolve_competitor_ats unavailable; skipping ATS enrichment"
            )
        except (ValueError, TypeError, KeyError, OSError) as _ae:
            logger.error(
                "ATS enrichment for competitive/scrape failed: %s", _ae, exc_info=True
            )

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
                    # S90: JobSpy is a keyword search, so narrow the sample to
                    # postings whose employer actually matches the competitor.
                    # This is a labeled market *sample* (bounded by results_wanted)
                    # -- NOT the headline open-role count, which comes from the
                    # ATS feed above. Fall back to the unfiltered set when the
                    # filter is empty so the card still shows market signal.
                    _needle = (_ci_company or "").lower().strip()
                    _emp_jobs = [
                        j
                        for j in _ci_js_jobs
                        if _needle and _needle in (j.get("company") or "").lower()
                    ]
                    _sample = _emp_jobs or _ci_js_jobs
                    result["jobspy_hiring_data"] = {
                        "postings_found": len(_sample),
                        "is_sample": True,
                        "employer_matched": bool(_emp_jobs),
                        "sample_postings": _sample[:5],
                        "sources": list({j.get("site") or "" for j in _sample}),
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
