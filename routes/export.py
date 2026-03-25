"""Export and delivery POST route handlers.

Extracted from app.py to reduce its size.  Handles:
- POST /api/export/sheets
- POST /api/export/status (GET-like, via POST)
- POST /api/deliver
- POST /api/report/html
- POST /api/nova/export
"""

import json
import logging
import re
import sys
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


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


def _handle_export_sheets(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/export/sheets -- Sheets/CSV/XLSX export."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)

        plan_data = data.get("plan_data") or data
        export_format = data.get("format") or "sheets"

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
# Route map
# ---------------------------------------------------------------------------

_EXPORT_POST_ROUTE_MAP: dict[str, Any] = {
    "/api/export/sheets": _handle_export_sheets,
    "/api/export/status": _handle_export_status,
    "/api/deliver": _handle_deliver,
    "/api/report/html": _handle_report_html,
    "/api/nova/export": _handle_nova_export,
}
