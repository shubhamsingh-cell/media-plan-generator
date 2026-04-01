"""Professional PDF report generator for AI Media Plan Generator.

Uses reportlab to generate a branded, multi-section PDF report from
media plan data. Sections: Executive Summary, Channel Allocation,
Market Analysis, Budget Breakdown (pie chart), Timeline,
Risk Analysis, Competitive Landscape.

Brand colors: PORT_GORE=#202058, BLUE_VIOLET=#5A54BD, DOWNY_TEAL=#6BB3CD
"""

from __future__ import annotations

import io
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Brand Colors (RGB tuples for reportlab)
# ---------------------------------------------------------------------------
PORT_GORE = (0x20 / 255, 0x20 / 255, 0x58 / 255)
BLUE_VIOLET = (0x5A / 255, 0x54 / 255, 0xBD / 255)
DOWNY_TEAL = (0x6B / 255, 0xB3 / 255, 0xCD / 255)
TAPESTRY_PINK = (0xB5 / 255, 0x66 / 255, 0x9C / 255)
RAW_SIENNA = (0xCE / 255, 0x90 / 255, 0x47 / 255)
WHITE = (1.0, 1.0, 1.0)
TEXT_DARK = (0.1, 0.1, 0.18)
TEXT_MUTED = (0.33, 0.33, 0.4)
BG_LIGHT = (0.96, 0.96, 0.97)
BORDER_COLOR = (0.82, 0.82, 0.88)

# Pie chart color cycle
PIE_COLORS = [
    BLUE_VIOLET,
    DOWNY_TEAL,
    TAPESTRY_PINK,
    RAW_SIENNA,
    PORT_GORE,
    (0.49, 0.42, 0.77),
    (0.29, 0.61, 0.71),
]


def _safe_str(value: Any) -> str:
    """Convert value to safe string."""
    if value is None:
        return ""
    return str(value)


def _format_currency(value: Any) -> str:
    """Format a numeric value as currency string."""
    try:
        num = float(value)
        if num >= 1_000_000:
            return f"${num / 1_000_000:,.1f}M"
        if num >= 1_000:
            return f"${num:,.0f}"
        return f"${num:,.2f}"
    except (TypeError, ValueError):
        return _safe_str(value)


def _format_number(value: Any) -> str:
    """Format a numeric value with commas."""
    try:
        num = float(value)
        if num == int(num):
            return f"{int(num):,}"
        return f"{num:,.2f}"
    except (TypeError, ValueError):
        return _safe_str(value)


def _format_pct(value: Any) -> str:
    """Format a percentage value."""
    try:
        num = float(value)
        return f"{num:.1f}%"
    except (TypeError, ValueError):
        return _safe_str(value)


def generate_pdf_report(
    plan_data: Dict[str, Any],
    client_name: str = "Client",
    industry: str = "Technology",
) -> bytes:
    """Generate a professional branded PDF report from media plan data.

    Args:
        plan_data: Dict containing budget, channels, roles, locations,
                   market_intelligence, recommendations, timeline,
                   risk_analysis, competitive_landscape.
        client_name: Company name for the report header.
        industry: Industry vertical label.

    Returns:
        PDF file content as bytes.
    """
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.lib.enums import TA_CENTER
        from reportlab.platypus import (
            SimpleDocTemplate,
            Paragraph,
            Spacer,
            Table,
            TableStyle,
            PageBreak,
            HRFlowable,
        )
        from reportlab.graphics.shapes import Drawing, Wedge, String, Line
    except ImportError:
        logger.error("reportlab not installed -- PDF export unavailable")
        raise ImportError(
            "reportlab is required for PDF export. Install with: pip install reportlab"
        )

    buf = io.BytesIO()
    now_utc = datetime.now(timezone.utc)
    report_date = now_utc.strftime("%B %d, %Y")
    report_timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Colors for reportlab
    c_port_gore = colors.Color(*PORT_GORE)
    c_blue_violet = colors.Color(*BLUE_VIOLET)
    c_downy_teal = colors.Color(*DOWNY_TEAL)
    c_white = colors.Color(*WHITE)
    c_text_dark = colors.Color(*TEXT_DARK)
    c_text_muted = colors.Color(*TEXT_MUTED)
    c_bg_light = colors.Color(*BG_LIGHT)
    c_border = colors.Color(*BORDER_COLOR)

    # Safely extract fields
    budget = plan_data.get("budget") or 0
    channels = plan_data.get("channels") or []
    roles = plan_data.get("roles") or []
    locations = plan_data.get("locations") or []
    market_intel = plan_data.get("market_intelligence") or {}
    recommendations = plan_data.get("recommendations") or []
    timeline = plan_data.get("timeline") or plan_data.get("campaign_timeline") or []
    risk_analysis = plan_data.get("risk_analysis") or plan_data.get("risks") or []
    competitive = (
        plan_data.get("competitive_landscape") or plan_data.get("competitors") or []
    )

    # Create styles
    styles = getSampleStyleSheet()
    style_title = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=24,
        leading=30,
        textColor=c_port_gore,
        alignment=TA_CENTER,
        spaceAfter=4,
    )
    style_subtitle = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=14,
        leading=18,
        textColor=c_blue_violet,
        alignment=TA_CENTER,
        spaceAfter=4,
    )
    style_date = ParagraphStyle(
        "ReportDate",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=c_text_muted,
        alignment=TA_CENTER,
        spaceAfter=20,
    )
    style_section = ParagraphStyle(
        "SectionHeader",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=22,
        textColor=c_port_gore,
        spaceBefore=20,
        spaceAfter=10,
        borderColor=c_blue_violet,
        borderWidth=0,
        borderPadding=0,
    )
    style_body = ParagraphStyle(
        "BodyText",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=c_text_dark,
        spaceAfter=6,
    )
    style_body_bold = ParagraphStyle(
        "BodyBold",
        parent=style_body,
        fontName="Helvetica-Bold",
    )
    style_metric_label = ParagraphStyle(
        "MetricLabel",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        textColor=c_text_muted,
        alignment=TA_CENTER,
    )
    style_metric_value = ParagraphStyle(
        "MetricValue",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=18,
        textColor=c_port_gore,
        alignment=TA_CENTER,
    )
    style_footer = ParagraphStyle(
        "Footer",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        textColor=c_text_muted,
        alignment=TA_CENTER,
    )
    style_bullet = ParagraphStyle(
        "BulletItem",
        parent=style_body,
        leftIndent=20,
        bulletIndent=8,
        bulletFontName="Helvetica",
        bulletFontSize=10,
    )

    # Page setup
    page_w, page_h = A4

    def _footer_func(canvas_obj: Any, doc: Any) -> None:
        """Draw page footer with page number and generation date."""
        canvas_obj.saveState()
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColor(c_text_muted)
        canvas_obj.drawString(
            40 * mm,
            12 * mm,
            f"Generated by Nova AI Suite  |  {report_timestamp}",
        )
        canvas_obj.drawRightString(
            page_w - 20 * mm,
            12 * mm,
            f"Page {doc.page}",
        )
        # Top header line
        canvas_obj.setStrokeColor(c_border)
        canvas_obj.setLineWidth(0.5)
        canvas_obj.line(20 * mm, page_h - 18 * mm, page_w - 20 * mm, page_h - 18 * mm)
        canvas_obj.restoreState()

    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=22 * mm,
        bottomMargin=20 * mm,
        title=f"Media Plan Report - {client_name}",
        author="Nova AI Suite",
    )

    elements: List[Any] = []

    # ── Header ──
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("Media Plan Report", style_title))
    elements.append(Paragraph(client_name, style_subtitle))
    elements.append(Paragraph(report_date, style_date))
    elements.append(
        HRFlowable(
            width="100%",
            thickness=2,
            color=c_port_gore,
            spaceAfter=20,
            spaceBefore=4,
        )
    )

    # ── Executive Summary ──
    elements.append(Paragraph("Executive Summary", style_section))
    elements.append(
        HRFlowable(
            width="40%",
            thickness=1.5,
            color=c_blue_violet,
            spaceAfter=12,
            spaceBefore=0,
            hAlign="LEFT",
        )
    )

    roles_display = ", ".join(str(r) for r in roles) if roles else "Not specified"
    locations_display = (
        ", ".join(str(loc) for loc in locations) if locations else "Not specified"
    )

    # Summary metrics cards
    total_spend = sum(
        float(ch.get("spend") or ch.get("budget") or 0)
        for ch in channels
        if isinstance(ch, dict)
    )
    total_clicks = sum(
        float(ch.get("projected_clicks") or ch.get("estimated_clicks") or 0)
        for ch in channels
        if isinstance(ch, dict)
    )
    total_applies = sum(
        float(ch.get("projected_applies") or ch.get("estimated_applies") or 0)
        for ch in channels
        if isinstance(ch, dict)
    )
    total_hires = sum(
        float(ch.get("projected_hires") or ch.get("estimated_hires") or 0)
        for ch in channels
        if isinstance(ch, dict)
    )

    summary_data = [
        ["Total Budget", "Industry", "Channels", "Target Roles"],
        [
            _format_currency(budget),
            _safe_str(industry),
            str(len(channels)),
            roles_display[:40] + ("..." if len(roles_display) > 40 else ""),
        ],
    ]
    summary_table = Table(summary_data, colWidths=[doc.width / 4] * 4)
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), c_bg_light),
                ("TEXTCOLOR", (0, 0), (-1, 0), c_text_muted),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 1), (-1, 1), 12),
                ("TEXTCOLOR", (0, 1), (-1, 1), c_port_gore),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("GRID", (0, 0), (-1, -1), 0.5, c_border),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("ROUNDEDCORNERS", [4, 4, 4, 4]),
            ]
        )
    )
    elements.append(summary_table)
    elements.append(Spacer(1, 8))

    elements.append(
        Paragraph(
            f"<b>Target Locations:</b> {locations_display}",
            style_body,
        )
    )
    elements.append(Spacer(1, 12))

    # ── Channel Allocation Table ──
    elements.append(PageBreak())
    elements.append(Paragraph("Channel Allocation", style_section))
    elements.append(
        HRFlowable(
            width="40%",
            thickness=1.5,
            color=c_blue_violet,
            spaceAfter=12,
            spaceBefore=0,
            hAlign="LEFT",
        )
    )

    ch_header = [
        "Channel",
        "Alloc %",
        "Spend",
        "CPC",
        "CPA",
        "Clicks",
        "Applies",
        "Hires",
    ]
    ch_rows = [ch_header]
    for ch in channels:
        if not isinstance(ch, dict):
            continue
        ch_rows.append(
            [
                _safe_str(ch.get("name") or ch.get("channel") or "N/A"),
                _format_pct(ch.get("allocation_pct") or 0),
                _format_currency(ch.get("spend") or ch.get("budget") or 0),
                _format_currency(ch.get("cpc") or ch.get("cost_per_click") or 0),
                _format_currency(ch.get("cpa") or ch.get("cost_per_apply") or 0),
                _format_number(
                    ch.get("projected_clicks") or ch.get("estimated_clicks") or 0
                ),
                _format_number(
                    ch.get("projected_applies") or ch.get("estimated_applies") or 0
                ),
                _format_number(
                    ch.get("projected_hires") or ch.get("estimated_hires") or 0
                ),
            ]
        )
    # Totals row
    ch_rows.append(
        [
            "TOTAL",
            "100%",
            _format_currency(total_spend),
            "--",
            "--",
            _format_number(total_clicks),
            _format_number(total_applies),
            _format_number(total_hires),
        ]
    )

    col_widths = [
        doc.width * w for w in [0.18, 0.09, 0.12, 0.10, 0.10, 0.13, 0.13, 0.13]
    ]
    ch_table = Table(ch_rows, colWidths=col_widths, repeatRows=1)
    ch_styles = [
        ("BACKGROUND", (0, 0), (-1, 0), c_port_gore),
        ("TEXTCOLOR", (0, 0), (-1, 0), c_white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("TEXTCOLOR", (0, 1), (-1, -1), c_text_dark),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
        ("ALIGN", (0, 0), (0, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.5, c_border),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        # Totals row
        ("BACKGROUND", (0, -1), (-1, -1), c_bg_light),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("LINEABOVE", (0, -1), (-1, -1), 1.5, c_port_gore),
    ]
    # Zebra striping
    for i in range(2, len(ch_rows) - 1, 2):
        ch_styles.append(("BACKGROUND", (0, i), (-1, i), c_bg_light))
    ch_table.setStyle(TableStyle(ch_styles))
    elements.append(ch_table)
    elements.append(Spacer(1, 20))

    # ── Budget Breakdown Pie Chart ──
    elements.append(Paragraph("Budget Breakdown", style_section))
    elements.append(
        HRFlowable(
            width="40%",
            thickness=1.5,
            color=c_blue_violet,
            spaceAfter=12,
            spaceBefore=0,
            hAlign="LEFT",
        )
    )

    if channels:
        drawing = Drawing(doc.width, 200)
        cx, cy = doc.width / 2, 100
        radius = 70
        start_angle = 90
        pie_data = []
        for ch in channels:
            if isinstance(ch, dict):
                pct = float(ch.get("allocation_pct") or 0)
                name = _safe_str(ch.get("name") or ch.get("channel") or "")
                pie_data.append((name, pct))

        total_pct = sum(p[1] for p in pie_data) or 1
        for i, (name, pct) in enumerate(pie_data):
            angle = (pct / total_pct) * 360
            if angle < 0.5:
                start_angle += angle
                continue
            color_idx = i % len(PIE_COLORS)
            rc = colors.Color(*PIE_COLORS[color_idx])
            wedge = Wedge(
                cx,
                cy,
                radius,
                startangledegrees=start_angle,
                endangledegrees=start_angle + angle,
                fillColor=rc,
                strokeColor=c_white,
                strokeWidth=1.5,
            )
            drawing.add(wedge)

            # Label
            mid_angle = start_angle + angle / 2
            label_r = radius + 18
            lx = cx + label_r * math.cos(math.radians(mid_angle))
            ly = cy + label_r * math.sin(math.radians(mid_angle))
            label_text = f"{name} ({pct:.0f}%)"
            if len(label_text) > 20:
                label_text = f"{name[:15]}.. ({pct:.0f}%)"
            label = String(
                lx,
                ly,
                label_text,
                fontName="Helvetica",
                fontSize=7,
                fillColor=c_text_dark,
                textAnchor="middle",
            )
            drawing.add(label)
            start_angle += angle

        elements.append(drawing)
    else:
        elements.append(Paragraph("No channel data available for chart.", style_body))

    elements.append(Spacer(1, 12))

    # ── Market Analysis ──
    if market_intel:
        elements.append(PageBreak())
        elements.append(Paragraph("Market Analysis", style_section))
        elements.append(
            HRFlowable(
                width="40%",
                thickness=1.5,
                color=c_blue_violet,
                spaceAfter=12,
                spaceBefore=0,
                hAlign="LEFT",
            )
        )
        if isinstance(market_intel, dict):
            for key, value in market_intel.items():
                label = str(key).replace("_", " ").title()
                if isinstance(value, list):
                    val_str = ", ".join(str(v) for v in value)
                elif isinstance(value, dict):
                    parts = [f"{k}: {v}" for k, v in value.items()]
                    val_str = "; ".join(parts)
                else:
                    val_str = str(value)
                elements.append(
                    Paragraph(
                        f"<b>{label}:</b> {val_str}",
                        style_body,
                    )
                )
        elif isinstance(market_intel, str):
            elements.append(Paragraph(market_intel, style_body))
        elements.append(Spacer(1, 12))

    # ── Timeline ──
    if timeline:
        elements.append(Paragraph("Campaign Timeline", style_section))
        elements.append(
            HRFlowable(
                width="40%",
                thickness=1.5,
                color=c_blue_violet,
                spaceAfter=12,
                spaceBefore=0,
                hAlign="LEFT",
            )
        )
        tl_header = ["Period", "Phase", "Channels", "Budget", "Key Actions"]
        tl_rows = [tl_header]
        for entry in timeline:
            if isinstance(entry, dict):
                tl_rows.append(
                    [
                        _safe_str(entry.get("week") or entry.get("period") or ""),
                        _safe_str(entry.get("phase") or ""),
                        _safe_str(
                            entry.get("channels") or entry.get("channels_active") or ""
                        ),
                        _format_currency(
                            entry.get("budget") or entry.get("spend") or 0
                        ),
                        _safe_str(
                            entry.get("actions") or entry.get("key_actions") or ""
                        ),
                    ]
                )
        if len(tl_rows) > 1:
            tl_col_w = [doc.width * w for w in [0.12, 0.12, 0.22, 0.14, 0.40]]
            tl_table = Table(tl_rows, colWidths=tl_col_w, repeatRows=1)
            tl_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), c_port_gore),
                        ("TEXTCOLOR", (0, 0), (-1, 0), c_white),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 8),
                        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                        ("FONTSIZE", (0, 1), (-1, -1), 9),
                        ("GRID", (0, 0), (-1, -1), 0.5, c_border),
                        ("TOPPADDING", (0, 0), (-1, -1), 5),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ]
                )
            )
            elements.append(tl_table)
        elements.append(Spacer(1, 12))

    # ── Risk Analysis ──
    if risk_analysis:
        elements.append(PageBreak())
        elements.append(Paragraph("Risk Analysis", style_section))
        elements.append(
            HRFlowable(
                width="40%",
                thickness=1.5,
                color=c_blue_violet,
                spaceAfter=12,
                spaceBefore=0,
                hAlign="LEFT",
            )
        )
        if isinstance(risk_analysis, list):
            for item in risk_analysis:
                if isinstance(item, dict):
                    risk_name = _safe_str(item.get("risk") or item.get("name") or "")
                    impact = _safe_str(item.get("impact") or item.get("severity") or "")
                    mitigation = _safe_str(
                        item.get("mitigation") or item.get("action") or ""
                    )
                    elements.append(
                        Paragraph(
                            f"<b>{risk_name}</b> (Impact: {impact})",
                            style_body_bold,
                        )
                    )
                    if mitigation:
                        elements.append(
                            Paragraph(f"Mitigation: {mitigation}", style_body)
                        )
                    elements.append(Spacer(1, 4))
                elif isinstance(item, str):
                    elements.append(Paragraph(f"- {item}", style_bullet))
        elif isinstance(risk_analysis, dict):
            for key, value in risk_analysis.items():
                elements.append(
                    Paragraph(
                        f"<b>{key.replace('_', ' ').title()}:</b> {_safe_str(value)}",
                        style_body,
                    )
                )
        elements.append(Spacer(1, 12))

    # ── Competitive Landscape ──
    if competitive:
        elements.append(Paragraph("Competitive Landscape", style_section))
        elements.append(
            HRFlowable(
                width="40%",
                thickness=1.5,
                color=c_blue_violet,
                spaceAfter=12,
                spaceBefore=0,
                hAlign="LEFT",
            )
        )
        if isinstance(competitive, list):
            for item in competitive:
                if isinstance(item, dict):
                    comp_name = _safe_str(
                        item.get("name") or item.get("competitor") or ""
                    )
                    comp_detail = _safe_str(
                        item.get("strategy")
                        or item.get("details")
                        or item.get("notes")
                        or ""
                    )
                    elements.append(
                        Paragraph(f"<b>{comp_name}:</b> {comp_detail}", style_body)
                    )
                elif isinstance(item, str):
                    elements.append(Paragraph(f"- {item}", style_bullet))
        elif isinstance(competitive, dict):
            for key, value in competitive.items():
                elements.append(
                    Paragraph(
                        f"<b>{key.replace('_', ' ').title()}:</b> {_safe_str(value)}",
                        style_body,
                    )
                )
        elements.append(Spacer(1, 12))

    # ── Recommendations ──
    if recommendations:
        elements.append(Paragraph("Recommendations", style_section))
        elements.append(
            HRFlowable(
                width="40%",
                thickness=1.5,
                color=c_blue_violet,
                spaceAfter=12,
                spaceBefore=0,
                hAlign="LEFT",
            )
        )
        for i, rec in enumerate(recommendations, 1):
            elements.append(Paragraph(f"{i}. {_safe_str(rec)}", style_body))
        elements.append(Spacer(1, 12))

    # ── Footer ──
    elements.append(Spacer(1, 20))
    elements.append(
        HRFlowable(
            width="100%",
            thickness=1,
            color=c_border,
            spaceAfter=8,
            spaceBefore=8,
        )
    )
    elements.append(
        Paragraph(
            f"Generated by <b>Nova AI Suite</b>  |  {report_timestamp}  |  "
            f"<a href='https://www.linkedin.com/in/chandel13/' color='#5A54BD'>linkedin.com/in/chandel13</a>",
            style_footer,
        )
    )

    # Build PDF
    try:
        doc.build(elements, onFirstPage=_footer_func, onLaterPages=_footer_func)
    except Exception as exc:
        logger.error("PDF build failed: %s", exc, exc_info=True)
        raise

    return buf.getvalue()
