"""Professional PDF report generator for AI Media Plan Generator.

Uses reportlab to generate a branded, multi-section PDF report from
media plan data. Sections: Executive Summary, Channel Allocation,
Market Analysis, Budget Breakdown (pie chart), Timeline,
Risk Analysis, Competitive Landscape.

Brand colors: PORT_GORE=#202058, BLUE_VIOLET=#5A54BE, DOWNY_TEAL=#6BB5CE
"""

from __future__ import annotations

import io
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Brand Colors (RGB tuples for reportlab)
# Canonical hex values imported from joveo_brand_2026 and converted to the
# 0..1 RGB float tuples reportlab expects, then aliased to the local names the
# rest of this module already uses (so downstream code is untouched). Values
# stay byte-identical to the previous hand-defined literals (all deck-exact).
# ---------------------------------------------------------------------------
from joveo_brand_2026 import (
    INDIGO,
    PURPLE,
    TEAL,
    MAGENTA,
    TEAL_DEEP,
    INK,
    MUTED,
    BORDER,
    LAVENDER_50,
    LAVENDER_100,
    hex_to_rgb_tuple,
)


def _rgb01(hex_str: str) -> tuple[float, float, float]:
    """'#5A54BE' -> (0.353.., 0.329.., 0.745..) for reportlab colors.Color."""
    r, g, b = hex_to_rgb_tuple(hex_str)
    return (r / 255, g / 255, b / 255)


PORT_GORE = _rgb01(INDIGO)  # INDIGO #202058
BLUE_VIOLET = _rgb01(PURPLE)  # PURPLE #5A54BE
DOWNY_TEAL = _rgb01(TEAL)  # TEAL #6BB5CE
TAPESTRY_PINK = _rgb01(MAGENTA)  # MAGENTA #B7669E
RAW_SIENNA = _rgb01(TEAL_DEEP)  # TEAL_DEEP #3E8FAB
WHITE = (1.0, 1.0, 1.0)
TEXT_DARK = _rgb01(INK)  # INK #1F2937 (deck-exact)
TEXT_MUTED = _rgb01(MUTED)  # MUTED #6E6E8C (deck-exact)
BG_LIGHT = _rgb01(LAVENDER_50)  # LAVENDER_50 #F4F4FF (deck-exact)
BG_LAVENDER = _rgb01(LAVENDER_100)  # LAVENDER_100 #ECEAF7 (deck)
BORDER_COLOR = _rgb01(BORDER)  # BORDER #E3E1F1 (deck-exact)

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


def _is_ai_training_plan(plan_data: Dict[str, Any]) -> bool:
    """Return True if the plan's industry/roles plausibly match an AI-training
    engagement (AI trainers, data annotators, language/data-labeling experts).

    The CPA-reference and sample-pricing deck sections carry data that is
    specific to an AI-trainer engagement ('Cost / Trainer', per-language audio
    specialists, etc.). Rendering them for an unrelated client would
    misrepresent that client's plan, so these sections are gated on this check.
    """
    if not isinstance(plan_data, dict):
        return False
    markers = ("ai", "train", "annotat", "language", "data label", "data-label")
    haystack_parts: List[str] = []
    industry = plan_data.get("industry")
    if industry:
        haystack_parts.append(str(industry))
    roles = plan_data.get("roles") or []
    if isinstance(roles, list):
        for r in roles:
            if isinstance(r, dict):
                haystack_parts.append(str(r.get("name") or r.get("title") or ""))
            else:
                haystack_parts.append(str(r))
    elif roles:
        haystack_parts.append(str(roles))
    haystack = " ".join(haystack_parts).lower()
    return any(m in haystack for m in markers)


def _load_deck_kb() -> Dict[str, Any]:
    """Load the Joveo media-plan deck content (methodology, push/pull, CPA
    reference, sample pricing, case study, next steps) from the KB JSON.

    Returns ``{}`` if the file is unavailable so the report degrades gracefully.
    """
    try:
        path = (
            Path(__file__).resolve().parent
            / "data"
            / "joveo_media_plan_deck_2026.json"
        )
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as exc:  # missing file / malformed JSON
        logger.warning("deck KB unavailable, skipping narrative sections: %s", exc)
        return {}


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
    c_bg_lavender = colors.Color(*BG_LAVENDER)
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

    # ── Deck narrative KB + client-safety gate (MPG-F3) ──
    # CPA-reference and sample-pricing carry AI-trainer-specific data
    # ('Cost / Trainer', per-language audio specialists). Only render them when
    # the plan plausibly matches an AI-training engagement; for any other client
    # both sections are omitted entirely so the AI-trainer data is never
    # mistaken for that client's actual plan. The methodology, push/pull,
    # case-study and next-steps sections are client-agnostic and always render.
    deck = _load_deck_kb()
    is_ai_training = _is_ai_training_plan({"industry": industry, "roles": roles})

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
    style_card_title = ParagraphStyle(
        "CardTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=10,
        leading=13,
        textColor=c_port_gore,
        spaceAfter=2,
    )
    style_card_tag = ParagraphStyle(
        "CardTag",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=7,
        leading=10,
        textColor=c_blue_violet,
        spaceAfter=2,
    )
    style_card_body = ParagraphStyle(
        "CardBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=11,
        textColor=c_text_muted,
    )

    def _section_header(title: str, page_break: bool = False) -> List[Any]:
        """Build a deck-styled section header (optionally preceded by a break)."""
        parts: List[Any] = []
        if page_break:
            parts.append(PageBreak())
        parts.append(Paragraph(title, style_section))
        parts.append(
            HRFlowable(
                width="40%",
                thickness=1.5,
                color=c_blue_violet,
                spaceAfter=12,
                spaceBefore=0,
                hAlign="LEFT",
            )
        )
        return parts

    # ── Deck narrative section builders (ported from pdf_generator deck KB) ──

    def _deck_methodology(deck: Dict[str, Any]) -> List[Any]:
        """Joveo 6-step campaign methodology (deck 'Our Methodology')."""
        meth = (deck.get("campaign_methodology") or {}) if isinstance(deck, dict) else {}
        steps = meth.get("steps") or []
        if not steps:
            return []
        out = _section_header("Our Methodology", page_break=True)
        # Render as a 2-column table of step cells
        cells: List[Any] = []
        for s in steps:
            if not isinstance(s, dict):
                continue
            inner: List[Any] = [
                Paragraph(
                    f"{_safe_str(s.get('n') or '')}. {_safe_str(s.get('name') or '')}",
                    style_card_title,
                )
            ]
            ai = _safe_str(s.get("ai") or "")
            if ai:
                inner.append(Paragraph(ai.upper(), style_card_tag))
            inner.append(Paragraph(_safe_str(s.get("detail") or ""), style_card_body))
            cells.append(inner)
        # Pack into rows of 2
        table_rows: List[List[Any]] = []
        for i in range(0, len(cells), 2):
            row = cells[i : i + 2]
            if len(row) == 1:
                row.append("")
            table_rows.append(row)
        meth_table = Table(table_rows, colWidths=[doc.width / 2] * 2)
        meth_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), c_bg_light),
                    ("BOX", (0, 0), (-1, -1), 0.5, c_border),
                    ("INNERGRID", (0, 0), (-1, -1), 0.5, c_border),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ]
            )
        )
        out.append(meth_table)
        out.append(Spacer(1, 12))
        return out

    def _deck_push_pull(deck: Dict[str, Any]) -> List[Any]:
        """Push Meets Pull framework (deck slide)."""
        pp = (deck.get("push_meets_pull") or {}) if isinstance(deck, dict) else {}
        push = pp.get("push") or {}
        pull = pp.get("pull") or {}
        if not push and not pull:
            return []
        out = _section_header("Push Meets Pull")
        summary = _safe_str(pp.get("summary") or "")
        if summary:
            out.append(Paragraph(summary, style_body))
            out.append(Spacer(1, 6))

        def _cell(obj: Dict[str, Any]) -> List[Any]:
            return [
                Paragraph(_safe_str(obj.get("name") or ""), style_card_title),
                Paragraph(_safe_str(obj.get("detail") or ""), style_card_body),
            ]

        pp_table = Table(
            [[_cell(push), _cell(pull)]], colWidths=[doc.width / 2] * 2
        )
        pp_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (0, 0), c_bg_lavender),
                    ("BACKGROUND", (1, 0), (1, 0), c_bg_light),
                    ("BOX", (0, 0), (-1, -1), 0.5, c_border),
                    ("INNERGRID", (0, 0), (-1, -1), 0.5, c_border),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("TOPPADDING", (0, 0), (-1, -1), 10),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                    ("LEFTPADDING", (0, 0), (-1, -1), 10),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ]
            )
        )
        out.append(pp_table)
        out.append(Spacer(1, 12))
        return out

    def _deck_cpa_reference(deck: Dict[str, Any], is_ai_training: bool) -> List[Any]:
        """CPA reference guide table (deck slide).

        Carries AI-trainer-specific benchmark data, so it is omitted entirely
        for any plan that does not match an AI-training engagement.
        """
        if not is_ai_training:
            return []
        cpa = (deck.get("cpa_reference") or {}) if isinstance(deck, dict) else {}
        rows = cpa.get("roles") or []
        if not rows:
            return []
        out = _section_header("CPA Reference Guide", page_break=True)
        header = ["Role Category", "Est. CPA Range", "Benchmark Basis"]
        data_rows = [header]
        for r in rows:
            if not isinstance(r, dict):
                continue
            data_rows.append(
                [
                    _safe_str(r.get("category") or ""),
                    f"${_safe_str(r.get('cpa_low') or '')} - "
                    f"${_safe_str(r.get('cpa_high') or '')}",
                    Paragraph(_safe_str(r.get("basis") or ""), style_card_body),
                ]
            )
        cpa_table = Table(
            data_rows,
            colWidths=[doc.width * w for w in [0.32, 0.22, 0.46]],
            repeatRows=1,
        )
        cpa_styles = [
            ("BACKGROUND", (0, 0), (-1, 0), c_port_gore),
            ("TEXTCOLOR", (0, 0), (-1, 0), c_white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 8),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 9),
            ("TEXTCOLOR", (0, 1), (-1, -1), c_text_dark),
            ("ALIGN", (1, 0), (1, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.5, c_border),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]
        for i in range(2, len(data_rows), 2):
            cpa_styles.append(("BACKGROUND", (0, i), (-1, i), c_bg_light))
        cpa_table.setStyle(TableStyle(cpa_styles))
        out.append(cpa_table)
        note = _safe_str(cpa.get("description") or "")
        if note:
            out.append(Spacer(1, 6))
            out.append(Paragraph(note, style_metric_label))
        out.append(Spacer(1, 12))
        return out

    def _deck_sample_pricing(deck: Dict[str, Any], is_ai_training: bool) -> List[Any]:
        """Sample campaign pricing table (deck slide).

        Carries AI-trainer-specific pricing data ('Cost / Trainer'), so it is
        omitted entirely for any plan that does not match an AI-training
        engagement.
        """
        if not is_ai_training:
            return []
        sp = (
            (deck.get("sample_pricing_model") or {}) if isinstance(deck, dict) else {}
        )
        camps = sp.get("campaigns") or []
        if not camps:
            return []
        out = _section_header("Sample Campaign Pricing")
        header = ["Campaign", "Targeting", "CPA", "Conv.", "Cost / Trainer", "Budget"]
        data_rows = [header]
        for c in camps:
            if not isinstance(c, dict):
                continue
            data_rows.append(
                [
                    Paragraph(_safe_str(c.get("campaign") or ""), style_card_body),
                    Paragraph(_safe_str(c.get("targeting") or ""), style_card_body),
                    f"${_safe_str(c.get('cpa_low') or '')}-"
                    f"${_safe_str(c.get('cpa_high') or '')}",
                    f"{_safe_str(c.get('historical_conversion_pct') or '')}%",
                    f"${_safe_str(c.get('cost_per_trainer_low') or '')}-"
                    f"${_safe_str(c.get('cost_per_trainer_high') or '')}",
                    f"{_format_currency(c.get('budget_low') or 0)}-"
                    f"{_format_currency(c.get('budget_high') or 0)}",
                ]
            )
        blended = sp.get("blended_total") or {}
        has_blended = bool(blended)
        if has_blended:
            data_rows.append(
                [
                    "Blended Total",
                    "",
                    "",
                    "",
                    f"${_safe_str(blended.get('cost_per_trainer_low') or '')}-"
                    f"${_safe_str(blended.get('cost_per_trainer_high') or '')}",
                    f"{_format_currency(blended.get('budget_low') or 0)}-"
                    f"{_format_currency(blended.get('budget_high') or 0)}",
                ]
            )
        sp_table = Table(
            data_rows,
            colWidths=[doc.width * w for w in [0.20, 0.24, 0.11, 0.09, 0.16, 0.20]],
            repeatRows=1,
        )
        sp_styles = [
            ("BACKGROUND", (0, 0), (-1, 0), c_port_gore),
            ("TEXTCOLOR", (0, 0), (-1, 0), c_white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 7),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 8),
            ("TEXTCOLOR", (0, 1), (-1, -1), c_text_dark),
            ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.5, c_border),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ]
        zebra_stop = len(data_rows) - 1 if has_blended else len(data_rows)
        for i in range(2, zebra_stop, 2):
            sp_styles.append(("BACKGROUND", (0, i), (-1, i), c_bg_light))
        if has_blended:
            sp_styles.extend(
                [
                    ("BACKGROUND", (0, -1), (-1, -1), c_bg_light),
                    ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                    ("LINEABOVE", (0, -1), (-1, -1), 1.5, c_port_gore),
                ]
            )
        sp_table.setStyle(TableStyle(sp_styles))
        out.append(sp_table)
        note = _safe_str(sp.get("note") or sp.get("description") or "")
        if note:
            out.append(Spacer(1, 6))
            out.append(Paragraph(note, style_metric_label))
        out.append(Spacer(1, 12))
        return out

    def _deck_case_study(deck: Dict[str, Any]) -> List[Any]:
        """Case study: outcome stats + challenges/solution (deck slides)."""
        cs = (deck.get("case_study") or {}) if isinstance(deck, dict) else {}
        if not cs:
            return []
        title = _safe_str(cs.get("title") or "Case Study")
        out = _section_header(f"Case Study - {title}", page_break=True)
        results = cs.get("results") or []
        stat_cells = []
        for r in results:
            if not isinstance(r, dict):
                continue
            stat_cells.append(
                [
                    Paragraph(_safe_str(r.get("value") or ""), style_metric_value),
                    Paragraph(
                        _safe_str(r.get("detail") or r.get("metric") or ""),
                        style_metric_label,
                    ),
                ]
            )
        if stat_cells:
            stat_table = Table(
                [stat_cells], colWidths=[doc.width / len(stat_cells)] * len(stat_cells)
            )
            stat_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, -1), c_bg_light),
                        ("BOX", (0, 0), (-1, -1), 0.5, c_border),
                        ("INNERGRID", (0, 0), (-1, -1), 0.5, c_border),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("TOPPADDING", (0, 0), (-1, -1), 10),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                    ]
                )
            )
            out.append(stat_table)
            out.append(Spacer(1, 10))

        challenges = cs.get("challenges") or []
        solution = cs.get("solution") or []

        def _list_cell(heading: str, items: List[Any], color: Any) -> List[Any]:
            head_style = ParagraphStyle(
                "CSHead", parent=style_card_title, textColor=color
            )
            cell: List[Any] = [Paragraph(heading, head_style)]
            for x in items:
                cell.append(Paragraph(f"- {_safe_str(x)}", style_card_body))
            return cell

        if challenges or solution:
            cs_table = Table(
                [
                    [
                        _list_cell("Challenges", challenges, c_port_gore),
                        _list_cell("The Joveo Solution", solution, c_blue_violet),
                    ]
                ],
                colWidths=[doc.width / 2] * 2,
            )
            cs_table.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("TOPPADDING", (0, 0), (-1, -1), 4),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                        ("LEFTPADDING", (0, 0), (0, -1), 0),
                        ("RIGHTPADDING", (1, 0), (1, -1), 0),
                    ]
                )
            )
            out.append(cs_table)
        out.append(Spacer(1, 12))
        return out

    def _deck_next_steps(deck: Dict[str, Any]) -> List[Any]:
        """Next steps checklist (deck slide)."""
        steps = (deck.get("next_steps") or []) if isinstance(deck, dict) else []
        if not steps:
            return []
        out = _section_header("Next Steps")
        for i, s in enumerate(steps, 1):
            out.append(Paragraph(f"{i}. {_safe_str(s)}", style_body))
        out.append(Spacer(1, 12))
        return out

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

    # ── Our Methodology (deck) ──
    elements.extend(_deck_methodology(deck))

    # ── Push Meets Pull (deck) ──
    elements.extend(_deck_push_pull(deck))

    # ── Benchmarking & Market Research (deck order: before Channel) ──
    if market_intel:
        elements.extend(_section_header("Benchmarking & Market Research", page_break=True))
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
                    Paragraph(f"<b>{label}:</b> {val_str}", style_body)
                )
        elif isinstance(market_intel, str):
            elements.append(Paragraph(market_intel, style_body))
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
                _safe_str(ch.get("name") or ch.get("channel") or "—"),
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

    # ── CPA Reference Guide (deck; AI-training plans only, else omitted) ──
    elements.extend(_deck_cpa_reference(deck, is_ai_training))

    # ── Sample Campaign Pricing (deck; AI-training plans only, else omitted) ──
    elements.extend(_deck_sample_pricing(deck, is_ai_training))

    # ── Case Study (deck) ──
    elements.extend(_deck_case_study(deck))

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

    # ── Next Steps (deck) ──
    elements.extend(_deck_next_steps(deck))

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
            f"<a href='https://www.linkedin.com/in/chandel13/' color='#5A54BE'>linkedin.com/in/chandel13</a>",
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
