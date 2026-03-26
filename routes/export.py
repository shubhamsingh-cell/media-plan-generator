"""Export and delivery route handlers (GET + POST).

Extracted from app.py to reduce its size.  Handles:
- POST /api/export/sheets
- POST /api/export/status (GET-like, via POST)
- POST /api/export/pdf  -- generate PDF report from plan data
- POST /api/deliver
- POST /api/report/html
- POST /api/nova/export
- GET  /api/plan/export/pdf?plan_id=xxx  -- download PDF for stored plan
"""

import json
import logging
import re
import sys
import time
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_export_get_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch export-related GET routes.  Returns True if handled."""
    if path == "/api/plan/export/pdf":
        _handle_pdf_export_get(handler, path, parsed)
        return True
    return False


def handle_export_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch export/delivery POST routes.  Returns True if handled."""
    _fn = _EXPORT_POST_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _track_export_event(plan_data: dict, export_format: str) -> None:
    """Fire PostHog media_plan_exported event (non-blocking)."""
    try:
        from posthog_tracker import track_plan_export

        track_plan_export(
            email=plan_data.get("requester_email", "anonymous"),
            export_format=export_format,
            client=plan_data.get("client_name") or "",
        )
    except Exception:
        pass


def _handle_export_sheets(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/export/sheets -- Sheets/CSV/XLSX export."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)

        plan_data = data.get("plan_data") or data
        export_format = data.get("format") or "sheets"

        # PostHog: track export event
        _track_export_event(plan_data, export_format)

        from sheets_export import (
            export_media_plan,
            export_to_csv,
            export_to_xlsx,
        )

        if export_format == "sheets":
            sheet_url = export_media_plan(plan_data)
            if sheet_url:
                handler._send_json({"url": sheet_url, "format": "sheets"})
            else:
                # Fall back to XLSX or CSV
                xlsx_bytes = export_to_xlsx(plan_data)
                if xlsx_bytes:
                    client_name = re.sub(
                        r"[^a-zA-Z0-9_\-]",
                        "_",
                        plan_data.get("client_name") or "Client",
                    )
                    handler.send_response(200)
                    handler.send_header(
                        "Content-Type",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                    handler.send_header(
                        "Content-Disposition",
                        f'attachment; filename="{client_name}_Media_Plan_Export.xlsx"',
                    )
                    handler.send_header("Content-Length", str(len(xlsx_bytes)))
                    handler.send_header("X-Export-Fallback", "xlsx")
                    handler.end_headers()
                    handler.wfile.write(xlsx_bytes)
                else:
                    _send_csv_fallback(handler, plan_data)

        elif export_format == "xlsx":
            xlsx_bytes = export_to_xlsx(plan_data)
            if xlsx_bytes:
                client_name = re.sub(
                    r"[^a-zA-Z0-9_\-]",
                    "_",
                    plan_data.get("client_name") or "Client",
                )
                handler.send_response(200)
                handler.send_header(
                    "Content-Type",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                handler.send_header(
                    "Content-Disposition",
                    f'attachment; filename="{client_name}_Media_Plan_Export.xlsx"',
                )
                handler.send_header("Content-Length", str(len(xlsx_bytes)))
                handler.end_headers()
                handler.wfile.write(xlsx_bytes)
            else:
                _send_csv_fallback(handler, plan_data)

        elif export_format == "csv":
            _send_csv_response(handler, plan_data)

        else:
            handler.send_response(400)
            handler.send_header("Content-Type", "application/json")
            handler.end_headers()
            handler.wfile.write(
                json.dumps(
                    {
                        "error": f"Unknown format: {export_format}. Use 'sheets', 'xlsx', or 'csv'."
                    }
                ).encode()
            )

    except json.JSONDecodeError:
        handler.send_response(400)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Invalid JSON body"}).encode())
    except Exception as e:
        logger.error("Export endpoint error: %s", e, exc_info=True)
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Export failed"}).encode())


def _send_csv_fallback(handler: Any, plan_data: dict) -> None:
    """Send CSV as fallback when XLSX is unavailable."""
    from sheets_export import export_to_csv

    csv_bytes = export_to_csv(plan_data)
    client_name = re.sub(
        r"[^a-zA-Z0-9_\-]",
        "_",
        plan_data.get("client_name") or "Client",
    )
    handler.send_response(200)
    handler.send_header("Content-Type", "text/csv; charset=utf-8")
    handler.send_header(
        "Content-Disposition",
        f'attachment; filename="{client_name}_Media_Plan_Export.csv"',
    )
    handler.send_header("Content-Length", str(len(csv_bytes)))
    handler.send_header("X-Export-Fallback", "csv")
    handler.end_headers()
    handler.wfile.write(csv_bytes)


def _send_csv_response(handler: Any, plan_data: dict) -> None:
    """Send a CSV export response."""
    from sheets_export import export_to_csv

    csv_bytes = export_to_csv(plan_data)
    client_name = re.sub(
        r"[^a-zA-Z0-9_\-]",
        "_",
        plan_data.get("client_name") or "Client",
    )
    handler.send_response(200)
    handler.send_header("Content-Type", "text/csv; charset=utf-8")
    handler.send_header(
        "Content-Disposition",
        f'attachment; filename="{client_name}_Media_Plan_Export.csv"',
    )
    handler.send_header("Content-Length", str(len(csv_bytes)))
    handler.end_headers()
    handler.wfile.write(csv_bytes)


def _handle_export_status(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/export/status -- check export service status."""
    try:
        from sheets_export import get_status as sheets_status

        handler._send_json(sheets_status())
    except Exception as e:
        logger.error("Export status error: %s", e, exc_info=True)
        handler._send_json({"configured": False, "error": str(e)}, status_code=500)


def _handle_deliver(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/deliver -- email delivery of media plan."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        from plan_delivery import send_plan_email

        client_ip = handler._get_client_ip()
        result = send_plan_email(
            recipient_email=data.get("email") or "",
            client_name=data.get("client_name") or "",
            plan_summary=data.get("plan_summary", {}),
            zip_file_path=data.get("zip_file_path"),
            sender_ip=client_ip,
        )
        handler._send_json(result)
    except Exception as e:
        logger.error("Plan delivery error: %s", e, exc_info=True)
        handler._send_json({"success": False, "message": "Delivery failed"})


def _handle_report_html(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/report/html -- generate HTML report."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        from pdf_generator import generate_plan_html_report

        html_content = generate_plan_html_report(
            plan_data=data.get("plan_data", data),
            client_name=data.get("client_name", "Client"),
            industry=data.get("industry", "Technology"),
        )
        html_bytes = html_content.encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header(
            "Content-Disposition", "attachment; filename=media_plan_report.html"
        )
        handler.send_header("Content-Length", str(len(html_bytes)))
        handler.end_headers()
        handler.wfile.write(html_bytes)
    except Exception as e:
        logger.error("HTML report error: %s", e, exc_info=True)
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Report generation failed"}).encode())


def _handle_nova_export(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/nova/export -- export Nova chat conversation as HTML."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        from nova_export import export_conversation_html

        html_content = export_conversation_html(
            data.get("conversation_history") or [],
            data.get("metadata", {}),
        )
        html_bytes = html_content.encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header(
            "Content-Disposition",
            "attachment; filename=nova-conversation-export.html",
        )
        handler.send_header("Content-Length", str(len(html_bytes)))
        cors_origin = (
            handler._get_cors_origin() if hasattr(handler, "_get_cors_origin") else None
        )
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.end_headers()
        handler.wfile.write(html_bytes)
    except Exception as e:
        logger.error("Nova export error: %s", e, exc_info=True)
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Export failed"}).encode())


# ---------------------------------------------------------------------------
# PDF Export handlers
# ---------------------------------------------------------------------------


def _handle_pdf_export_get(handler: Any, path: str, parsed: Any) -> None:
    """GET /api/plan/export/pdf?plan_id=xxx -- download PDF for a stored plan.

    Looks up plan data from _plan_results_store by plan_id, generates a
    branded PDF report, and returns it as a file download.
    """
    try:
        from urllib.parse import parse_qs

        qs = parse_qs(parsed.query) if hasattr(parsed, "query") else {}
        plan_id = (qs.get("plan_id") or [""])[0]

        if not plan_id or not re.match(r"^[a-f0-9]{1,12}$", plan_id):
            handler._send_json({"error": "Missing or invalid plan_id"}, status_code=400)
            return

        # Look up plan data from the in-memory store
        _app = sys.modules.get("app") or sys.modules.get("__main__")
        _plan_results_store = getattr(_app, "_plan_results_store", {})
        _plan_results_lock = getattr(_app, "_plan_results_lock", None)

        entry = None
        if _plan_results_lock:
            with _plan_results_lock:
                entry = _plan_results_store.get(plan_id)
                if entry and time.time() - entry.get("created", 0) > 86400:
                    _plan_results_store.pop(plan_id, None)
                    entry = None
        else:
            entry = _plan_results_store.get(plan_id)

        if not entry:
            handler._send_json({"error": "Plan not found or expired"}, status_code=404)
            return

        plan_json = entry.get("data") or {}
        metadata = plan_json.get("metadata") or {}
        summary = plan_json.get("summary") or {}
        client_name = metadata.get("client_name") or "Client"
        industry = (
            metadata.get("industry_label") or metadata.get("industry") or "Technology"
        )

        # Build plan_data in the format pdf_report expects
        plan_data = {
            "budget": metadata.get("total_budget") or summary.get("total_budget") or 0,
            "channels": summary.get("channels") or [],
            "roles": metadata.get("roles") or [],
            "locations": metadata.get("locations") or [],
            "market_intelligence": plan_json.get("market_intelligence") or {},
            "recommendations": summary.get("recommendations")
            or plan_json.get("recommendations")
            or [],
            "timeline": plan_json.get("timeline") or [],
            "risk_analysis": plan_json.get("risk_analysis") or [],
            "competitive_landscape": plan_json.get("competitive_landscape") or [],
        }

        _generate_and_send_pdf(handler, plan_data, client_name, industry)

    except Exception as e:
        logger.error("PDF export GET error: %s", e, exc_info=True)
        handler._send_json({"error": "PDF export failed"}, status_code=500)


def _handle_pdf_export_post(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/export/pdf -- generate PDF from posted plan data."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)

        plan_data = data.get("plan_data") or data
        client_name = (
            data.get("client_name") or plan_data.get("client_name") or "Client"
        )
        industry = data.get("industry") or plan_data.get("industry") or "Technology"

        # PostHog: track export event
        _track_export_event(plan_data, "pdf")

        _generate_and_send_pdf(handler, plan_data, client_name, industry)

    except json.JSONDecodeError:
        handler.send_response(400)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Invalid JSON body"}).encode())
    except ImportError:
        handler.send_response(501)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(
            json.dumps(
                {"error": "PDF export unavailable -- reportlab not installed"}
            ).encode()
        )
    except Exception as e:
        logger.error("PDF export POST error: %s", e, exc_info=True)
        handler.send_response(500)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "PDF generation failed"}).encode())


def _generate_and_send_pdf(
    handler: Any,
    plan_data: dict,
    client_name: str,
    industry: str,
) -> None:
    """Generate PDF and send as download response.

    Args:
        handler: HTTP request handler instance.
        plan_data: Plan data dict.
        client_name: Client/company name.
        industry: Industry vertical.
    """
    from pdf_report import generate_pdf_report

    pdf_bytes = generate_pdf_report(
        plan_data=plan_data,
        client_name=client_name,
        industry=industry,
    )

    safe_name = re.sub(r"[^a-zA-Z0-9_\-]", "_", client_name)
    filename = f"{safe_name}_Media_Plan_Report.pdf"

    handler.send_response(200)
    handler.send_header("Content-Type", "application/pdf")
    handler.send_header(
        "Content-Disposition",
        f'attachment; filename="{filename}"',
    )
    handler.send_header("Content-Length", str(len(pdf_bytes)))
    handler.end_headers()
    handler.wfile.write(pdf_bytes)


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_EXPORT_POST_ROUTE_MAP: dict[str, Any] = {
    "/api/export/sheets": _handle_export_sheets,
    "/api/export/status": _handle_export_status,
    "/api/export/pdf": _handle_pdf_export_post,
    "/api/deliver": _handle_deliver,
    "/api/report/html": _handle_report_html,
    "/api/nova/export": _handle_nova_export,
}
