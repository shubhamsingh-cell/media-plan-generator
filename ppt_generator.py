#!/usr/bin/env python3
"""
McKinsey-style 2-page PowerPoint generator for AI Media Planner.

Generates a professional, data-driven .pptx presentation using python-pptx.
Designed to match McKinsey's visual standards: minimal, structured, insight-led.
"""

import io
import datetime
from typing import Any, Dict, List, Optional

from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE


# ---------------------------------------------------------------------------
# Constants & Color Palette
# ---------------------------------------------------------------------------

NAVY = RGBColor(0x1B, 0x2A, 0x4A)
ACCENT_BLUE = RGBColor(0x2E, 0x75, 0xB6)
LIGHT_BLUE = RGBColor(0xD6, 0xE4, 0xF0)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
DARK_TEXT = RGBColor(0x33, 0x33, 0x33)
MUTED_TEXT = RGBColor(0x59, 0x67, 0x80)
GREEN = RGBColor(0x00, 0xB0, 0x50)
ORANGE = RGBColor(0xED, 0x7D, 0x31)

FONT_FAMILY = "Calibri"

# Slide dimensions (16:9 widescreen)
SLIDE_WIDTH = Inches(13.333)
SLIDE_HEIGHT = Inches(7.5)

# ---------------------------------------------------------------------------
# Industry Benchmark Data
# ---------------------------------------------------------------------------

BENCHMARKS: Dict[str, Dict[str, str]] = {
    "healthcare_medical": {
        "cpa": "$35 - $85",
        "cpc": "$0.90 - $3.50",
        "cph": "$9K - $12K",
        "apply_rate": "3.2% - 4.5%",
    },
    "tech_engineering": {
        "cpa": "$25 - $75",
        "cpc": "$1.20 - $4.50",
        "cph": "$6K - $22K",
        "apply_rate": "6.41%",
    },
    "retail_consumer": {
        "cpa": "$8 - $21",
        "cpc": "$0.25 - $1.00",
        "cph": "$2.7K - $4K",
        "apply_rate": "4.5% - 5.8%",
    },
    "general_entry_level": {
        "cpa": "$10 - $25",
        "cpc": "$0.35 - $1.30",
        "cph": "$2K - $4.7K",
        "apply_rate": "5.5% - 6.1%",
    },
    "finance_banking": {
        "cpa": "$21 - $65",
        "cpc": "$0.90 - $3.50",
        "cph": "$5K - $12K",
        "apply_rate": "5.0% - 6.0%",
    },
    "logistics_supply_chain": {
        "cpa": "$15 - $52",
        "cpc": "$0.40 - $1.80",
        "cph": "$4.5K - $8K",
        "apply_rate": "4.0% - 5.2%",
    },
    "hospitality_travel": {
        "cpa": "$8 - $25",
        "cpc": "$0.22 - $1.00",
        "cph": "$2.5K - $4K",
        "apply_rate": "4.0% - 5.0%",
    },
    "blue_collar_trades": {
        "cpa": "$12 - $35",
        "cpc": "$0.40 - $1.60",
        "cph": "$3.5K - $5.6K",
        "apply_rate": "4.0% - 5.5%",
    },
    "pharma_biotech": {
        "cpa": "$40 - $110",
        "cpc": "$1.50 - $5.00",
        "cph": "$8K - $18K",
        "apply_rate": "3.8% - 5.2%",
    },
}

# ---------------------------------------------------------------------------
# Industry-Specific Hiring Challenges (Complication column)
# ---------------------------------------------------------------------------

COMPLICATIONS: Dict[str, List[str]] = {
    "healthcare_medical": [
        "Clinical talent shortages persist nationally",
        "CPA exceeds $35+ for standing-up roles",
        "Burnout driving 18% higher churn vs. 2023",
        "Credentialing requirements slow time-to-fill",
    ],
    "tech_engineering": [
        "White-collar recession creating surplus but CPCs remain high",
        "AI/ML roles still command premium sourcing costs",
        "6.41% apply rate highest of all sectors",
        "Remote-first expectations complicate geo-targeting",
    ],
    "retail_consumer": [
        "64,000 retail jobs shed in 2025 Q1",
        "CPA up 55% YoY despite market softening",
        "5,800+ store closures accelerating talent displacement",
        "Seasonal demand creates volatile cost spikes",
    ],
    "hospitality_travel": [
        "CPA surging +225% YoY across hospitality",
        "Extreme seasonal demand swings in Q2/Q4",
        "High turnover-driven churn exceeds 73%",
        "Hourly wage competition from adjacent industries",
    ],
    "logistics_supply_chain": [
        "CPA up 131% YoY across logistics roles",
        "CDL/last-mile roles most expensive at $52+ CPA",
        "Automation creating new hybrid role types",
        "Warehouse labor competing with gig economy",
    ],
    "finance_banking": [
        "Finance CPA surged +33.3% MoM",
        "Compliance-heavy hiring extends cycles by 2-3 weeks",
        "Extensive background checks inflate cost-per-hire",
        "Fintech competition drawing mid-career talent",
    ],
    "general_entry_level": [
        "CPCs rose 27% in 2024; trend continuing",
        "Seasonal Q4 spikes compress planning windows",
        "Apply rates improving but quality remains a challenge",
        "High-volume funnels require aggressive top-of-funnel spend",
    ],
    "blue_collar_trades": [
        "Skilled trades gap widening as workforce ages",
        "CPA up 40% for certified/licensed positions",
        "Geographic mismatch between supply and demand",
        "Apprenticeship pipelines insufficient for near-term needs",
    ],
    "pharma_biotech": [
        "Regulatory talent scarcity drives $110+ CPAs",
        "Clinical trial staffing requires hyper-niche sourcing",
        "PhD-level roles average 60+ days to fill",
        "Compliance training costs add $3K-$5K per hire",
    ],
}

# ---------------------------------------------------------------------------
# Default Channel Allocations
# ---------------------------------------------------------------------------

CHANNEL_ALLOC: Dict[str, Dict[str, Any]] = {
    "programmatic_dsp":  {"label": "Programmatic DSP",      "pct": 35, "color": NAVY},
    "global_boards":     {"label": "Global Job Boards",     "pct": 20, "color": ACCENT_BLUE},
    "niche_boards":      {"label": "Niche / Industry Boards","pct": 15, "color": RGBColor(0x4A, 0x90, 0xD9)},
    "social_media":      {"label": "Social Media",          "pct": 12, "color": RGBColor(0x5B, 0xA8, 0xE0)},
    "regional_boards":   {"label": "Regional Boards",       "pct": 8,  "color": RGBColor(0x7F, 0xBF, 0xE8)},
    "employer_branding": {"label": "Employer Branding",     "pct": 5,  "color": RGBColor(0xA3, 0xD1, 0xF0)},
    "apac_regional":     {"label": "APAC Regional",         "pct": 3,  "color": RGBColor(0xBD, 0xDE, 0xF5)},
    "emea_regional":     {"label": "EMEA Regional",         "pct": 2,  "color": RGBColor(0xD6, 0xE9, 0xFA)},
}

# Human-readable goal labels
GOAL_LABELS: Dict[str, str] = {
    "brand_awareness": "Brand Awareness",
    "high_volume": "High-Volume Hiring",
    "diversity_hiring": "Diversity & Inclusion",
    "cost_efficiency": "Cost Efficiency",
    "quality_candidates": "Quality Candidates",
    "passive_talent": "Passive Talent Reach",
    "employer_branding": "Employer Branding",
    "retention": "Retention Focus",
    "speed_to_hire": "Speed to Hire",
    "geographic_expansion": "Geographic Expansion",
}

WORK_ENV_LABELS: Dict[str, str] = {
    "hybrid": "Hybrid",
    "remote": "Remote",
    "on_site": "On-Site",
    "on-site": "On-Site",
    "flexible": "Flexible",
}


# ===================================================================
# Helper utilities
# ===================================================================

def _set_font(
    run,
    size: int = 10,
    bold: bool = False,
    italic: bool = False,
    color: RGBColor = DARK_TEXT,
    name: str = FONT_FAMILY,
):
    """Configure font properties on a text run."""
    run.font.name = name
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = color


def _add_textbox(
    slide,
    left,
    top,
    width,
    height,
    text: str = "",
    font_size: int = 10,
    bold: bool = False,
    color: RGBColor = DARK_TEXT,
    alignment=PP_ALIGN.LEFT,
    anchor=MSO_ANCHOR.TOP,
    word_wrap: bool = True,
):
    """Add a text box to a slide and return (shape, text_frame)."""
    txBox = slide.shapes.add_textbox(left, top, width, height)
    tf = txBox.text_frame
    tf.word_wrap = word_wrap
    tf.auto_size = None
    try:
        tf.paragraphs[0].alignment = alignment
    except Exception:
        pass
    # Anchor
    txBox.text_frame.paragraphs[0].space_before = Pt(0)
    txBox.text_frame.paragraphs[0].space_after = Pt(0)
    # Vertical anchor
    try:
        txBox.text_frame._txBody.bodyPr.set("anchor", {
            MSO_ANCHOR.TOP: "t",
            MSO_ANCHOR.MIDDLE: "ctr",
            MSO_ANCHOR.BOTTOM: "b",
        }.get(anchor, "t"))
    except Exception:
        pass

    if text:
        p = tf.paragraphs[0]
        p.alignment = alignment
        run = p.add_run()
        run.text = text
        _set_font(run, size=font_size, bold=bold, color=color)

    return txBox, tf


def _add_filled_rect(slide, left, top, width, height, fill_color: RGBColor):
    """Add a rectangle shape with a solid fill and no border."""
    shape = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, left, top, width, height)
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    shape.line.fill.background()
    return shape


def _add_paragraph(tf, text, font_size=10, bold=False, color=DARK_TEXT, alignment=PP_ALIGN.LEFT, space_before=0, space_after=2):
    """Append a paragraph to an existing text frame."""
    p = tf.add_paragraph()
    p.alignment = alignment
    p.space_before = Pt(space_before)
    p.space_after = Pt(space_after)
    run = p.add_run()
    run.text = text
    _set_font(run, size=font_size, bold=bold, color=color)
    return p


def _get_benchmarks(industry: str) -> Dict[str, str]:
    """Return benchmark data for the given industry, falling back to general."""
    return BENCHMARKS.get(industry, BENCHMARKS["general_entry_level"])


def _get_complications(industry: str) -> List[str]:
    """Return complication bullets for the industry."""
    return COMPLICATIONS.get(
        industry,
        [
            "Talent acquisition costs rising across sectors",
            "Competition for qualified candidates intensifying",
            "Traditional sourcing channels showing diminishing returns",
            "Time-to-fill expanding, impacting operational capacity",
        ],
    )


def _selected_channels(data: Dict) -> Dict[str, Dict[str, Any]]:
    """Return only the channels the user toggled on, with redistributed percentages."""
    cats = data.get("channel_categories", {})
    # Support both list format (from frontend) and dict format
    if isinstance(cats, list):
        cats = {k: True for k in cats}
    selected = {}
    for key, meta in CHANNEL_ALLOC.items():
        if cats.get(key, False):
            selected[key] = dict(meta)  # copy

    if not selected:
        # Fallback: show programmatic + global + social if nothing selected
        for key in ("programmatic_dsp", "global_boards", "social_media"):
            selected[key] = dict(CHANNEL_ALLOC[key])

    # Redistribute to sum to 100
    raw_total = sum(v["pct"] for v in selected.values())
    if raw_total > 0:
        for v in selected.values():
            v["pct"] = round(v["pct"] / raw_total * 100)
        # Fix rounding so total = 100
        diff = 100 - sum(v["pct"] for v in selected.values())
        if diff != 0:
            first_key = next(iter(selected))
            selected[first_key]["pct"] += diff

    return selected


def _goal_labels(data: Dict) -> List[str]:
    """Return human-readable campaign goal labels."""
    goals = data.get("campaign_goals", [])
    return [GOAL_LABELS.get(g, g.replace("_", " ").title()) for g in goals]


# ===================================================================
# SLIDE 1 - Executive Summary
# ===================================================================

def _build_slide_1(prs: Presentation, data: Dict):
    """Build the Executive Summary slide (Slide 1)."""
    slide_layout = prs.slide_layouts[6]  # blank layout
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    industry_label = data.get("industry_label", industry.replace("_", " ").title())
    locations = data.get("locations", [])
    roles = data.get("roles", [])
    budget = data.get("budget", "TBD")
    work_env = data.get("work_environment", "hybrid")
    goals = _goal_labels(data)
    channels = _selected_channels(data)

    # ---- TOP BAND (Navy) ----
    band_h = Inches(0.72)
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, band_h, NAVY)

    # "AI MEDIA PLANNER" left
    _add_textbox(
        slide, Inches(0.45), Inches(0.15), Inches(4), Inches(0.45),
        text="AI MEDIA PLANNER", font_size=14, bold=True, color=WHITE,
    )

    # Client name right
    _add_textbox(
        slide, Inches(9), Inches(0.15), Inches(4), Inches(0.45),
        text=client.upper(), font_size=14, bold=True, color=WHITE,
        alignment=PP_ALIGN.RIGHT,
    )

    # ---- ACTION TITLE ----
    role_summary = ", ".join(roles[:3]) if roles else "key roles"
    loc_count = len(locations)
    action_text = (
        f"Joveo's programmatic strategy targets {role_summary} across "
        f"{loc_count} location{'s' if loc_count != 1 else ''} to optimize "
        f"{client}'s recruitment spend in {industry_label}"
    )
    _add_textbox(
        slide, Inches(0.55), Inches(0.92), Inches(12.2), Inches(0.55),
        text=action_text, font_size=16, bold=True, color=NAVY,
    )

    # ---- THREE-COLUMN SCR BODY ----
    col_top = Inches(1.65)
    col_height = Inches(3.8)
    col_gap = Inches(0.3)
    accent_bar_w = Inches(0.06)

    col_w = Inches(3.9)
    col1_left = Inches(0.55)
    col2_left = col1_left + col_w + col_gap
    col3_left = col2_left + col_w + col_gap

    # ---- SITUATION (left) ----
    _add_filled_rect(slide, col1_left, col_top, accent_bar_w, col_height, ACCENT_BLUE)

    sit_left = col1_left + Inches(0.2)
    sit_w = col_w - Inches(0.2)

    box, tf = _add_textbox(slide, sit_left, col_top, sit_w, Inches(0.35),
                           text="SITUATION", font_size=11, bold=True, color=ACCENT_BLUE)

    body_top = col_top + Inches(0.42)
    work_label = WORK_ENV_LABELS.get(work_env, work_env.replace("_", " ").title())
    role_display = ", ".join(roles[:5]) if roles else "Multiple roles"
    if len(roles) > 5:
        role_display += f" (+{len(roles) - 5} more)"

    sit_items = [
        ("Industry", industry_label),
        ("Locations", f"{loc_count} market{'s' if loc_count != 1 else ''}"),
        ("Target Roles", role_display),
        ("Work Environment", work_label),
        ("Budget", budget),
    ]

    box2, tf2 = _add_textbox(slide, sit_left, body_top, sit_w, col_height - Inches(0.5))
    # Remove default empty paragraph text
    tf2.paragraphs[0].space_before = Pt(0)
    tf2.paragraphs[0].space_after = Pt(0)

    first = True
    for label, value in sit_items:
        if first:
            p = tf2.paragraphs[0]
            first = False
        else:
            p = tf2.add_paragraph()
        p.space_before = Pt(2)
        p.space_after = Pt(6)
        p.alignment = PP_ALIGN.LEFT

        run_label = p.add_run()
        run_label.text = f"{label}:  "
        _set_font(run_label, size=10, bold=True, color=DARK_TEXT)

        run_val = p.add_run()
        run_val.text = value
        _set_font(run_val, size=10, bold=False, color=MUTED_TEXT)

    # ---- COMPLICATION (middle) ----
    _add_filled_rect(slide, col2_left, col_top, accent_bar_w, col_height, ORANGE)

    comp_left = col2_left + Inches(0.2)
    comp_w = col_w - Inches(0.2)

    _add_textbox(slide, comp_left, col_top, comp_w, Inches(0.35),
                 text="COMPLICATION", font_size=11, bold=True, color=ORANGE)

    complications = _get_complications(industry)
    box3, tf3 = _add_textbox(slide, comp_left, body_top, comp_w, col_height - Inches(0.5))
    tf3.paragraphs[0].space_before = Pt(0)
    tf3.paragraphs[0].space_after = Pt(0)

    for i, item in enumerate(complications):
        if i == 0:
            p = tf3.paragraphs[0]
        else:
            p = tf3.add_paragraph()
        p.space_before = Pt(2)
        p.space_after = Pt(8)
        p.alignment = PP_ALIGN.LEFT

        # Bullet marker
        run_bullet = p.add_run()
        run_bullet.text = "\u25B8  "  # small triangle
        _set_font(run_bullet, size=10, bold=False, color=ORANGE)

        run_text = p.add_run()
        run_text.text = item
        _set_font(run_text, size=10, bold=False, color=DARK_TEXT)

    # ---- RESOLUTION (right) ----
    _add_filled_rect(slide, col3_left, col_top, accent_bar_w, col_height, GREEN)

    res_left = col3_left + Inches(0.2)
    res_w = col_w - Inches(0.2)

    _add_textbox(slide, res_left, col_top, res_w, Inches(0.35),
                 text="RESOLUTION", font_size=11, bold=True, color=GREEN)

    box4, tf4 = _add_textbox(slide, res_left, body_top, res_w, col_height - Inches(0.5))
    tf4.paragraphs[0].space_before = Pt(0)
    tf4.paragraphs[0].space_after = Pt(0)

    # Sub-header
    p0 = tf4.paragraphs[0]
    r0 = p0.add_run()
    r0.text = "Joveo Programmatic Strategy"
    _set_font(r0, size=10, bold=True, color=NAVY)
    p0.space_after = Pt(6)

    # Channel bullets
    for ch in channels.values():
        p = tf4.add_paragraph()
        p.space_before = Pt(1)
        p.space_after = Pt(4)
        rb = p.add_run()
        rb.text = "\u2713  "
        _set_font(rb, size=9, bold=False, color=GREEN)
        rt = p.add_run()
        rt.text = ch["label"]
        _set_font(rt, size=9, bold=False, color=DARK_TEXT)

    # ML line
    _add_paragraph(tf4, "\u2713  ML-optimized bidding across 1,200+ publishers",
                   font_size=9, color=DARK_TEXT, space_before=1, space_after=4)
    _add_paragraph(tf4, "\u2713  CPQA-focused: quality over volume",
                   font_size=9, color=DARK_TEXT, space_before=1, space_after=6)

    # Campaign goals
    if goals:
        _add_paragraph(tf4, "Campaign Goals:", font_size=9, bold=True, color=NAVY,
                       space_before=4, space_after=2)
        for g in goals[:4]:
            p = tf4.add_paragraph()
            p.space_before = Pt(1)
            p.space_after = Pt(3)
            rb = p.add_run()
            rb.text = "\u25CF  "
            _set_font(rb, size=8, color=ACCENT_BLUE)
            rt = p.add_run()
            rt.text = g
            _set_font(rt, size=9, color=DARK_TEXT)

    # ---- BOTTOM METRICS BAR ----
    bar_top = Inches(5.7)
    bar_h = Inches(0.95)
    _add_filled_rect(slide, Inches(0.55), bar_top, Inches(12.2), bar_h, LIGHT_BLUE)

    metrics = [
        (str(len(channels)), "Channels Selected"),
        (str(loc_count), "Target Locations"),
        (str(len(roles)), "Target Roles"),
        (str(len(goals)), "Campaign Goals"),
    ]

    metric_w = Inches(2.8)
    metric_gap = Inches(0.25)
    metric_start = Inches(0.95)

    for i, (value, label) in enumerate(metrics):
        mx = metric_start + i * (metric_w + metric_gap)

        # Value
        _add_textbox(
            slide, mx, bar_top + Inches(0.08), metric_w, Inches(0.5),
            text=value, font_size=26, bold=True, color=NAVY,
            alignment=PP_ALIGN.CENTER,
        )
        # Label
        _add_textbox(
            slide, mx, bar_top + Inches(0.55), metric_w, Inches(0.3),
            text=label, font_size=9, bold=False, color=MUTED_TEXT,
            alignment=PP_ALIGN.CENTER,
        )

    # ---- Thin dividers between metrics ----
    for i in range(1, 4):
        div_x = metric_start + i * (metric_w + metric_gap) - metric_gap / 2
        _add_filled_rect(slide, div_x, bar_top + Inches(0.15), Inches(0.015), Inches(0.65), ACCENT_BLUE)

    # ---- FOOTER ----
    footer_top = Inches(6.95)
    _add_filled_rect(slide, Inches(0), footer_top, SLIDE_WIDTH, Inches(0.03), NAVY)
    today = datetime.date.today().strftime("%B %d, %Y")
    _add_textbox(
        slide, Inches(0.55), footer_top + Inches(0.08), Inches(12.2), Inches(0.3),
        text=f"Powered by Joveo  |  AI Media Planner  |  {today}",
        font_size=7, color=MUTED_TEXT, alignment=PP_ALIGN.CENTER,
    )


# ===================================================================
# SLIDE 2 - Channel Strategy & Investment
# ===================================================================

def _build_slide_2(prs: Presentation, data: Dict):
    """Build the Channel Strategy & Investment slide (Slide 2)."""
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    industry_label = data.get("industry_label", industry.replace("_", " ").title())
    channels = _selected_channels(data)
    benchmarks = _get_benchmarks(industry)
    today = datetime.date.today().strftime("%B %d, %Y")

    # ---- TOP BAND ----
    band_h = Inches(0.72)
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, band_h, NAVY)

    _add_textbox(
        slide, Inches(0.45), Inches(0.15), Inches(7), Inches(0.45),
        text="CHANNEL STRATEGY & INVESTMENT", font_size=14, bold=True, color=WHITE,
    )
    _add_textbox(
        slide, Inches(9), Inches(0.15), Inches(4), Inches(0.45),
        text=today, font_size=12, bold=False, color=RGBColor(0xA0, 0xB0, 0xCC),
        alignment=PP_ALIGN.RIGHT,
    )

    # ---- ACTION TITLE ----
    n_cats = len(channels)
    action_text = (
        f"Optimized channel mix across {n_cats} categories delivers targeted "
        f"reach for {client}'s {industry_label} hiring priorities"
    )
    _add_textbox(
        slide, Inches(0.55), Inches(0.92), Inches(12.2), Inches(0.5),
        text=action_text, font_size=16, bold=True, color=NAVY,
    )

    # ==== LEFT HALF: Channel Mix (~55%) ====
    left_col_left = Inches(0.55)
    left_col_w = Inches(6.8)
    section_top = Inches(1.65)

    _add_textbox(
        slide, left_col_left, section_top, left_col_w, Inches(0.35),
        text="CHANNEL MIX", font_size=11, bold=True, color=ACCENT_BLUE,
    )
    # Underline
    _add_filled_rect(slide, left_col_left, section_top + Inches(0.33), Inches(1.3), Inches(0.025), ACCENT_BLUE)

    bar_area_top = section_top + Inches(0.55)
    bar_max_w = Inches(4.0)
    bar_h = Inches(0.32)
    bar_spacing = Inches(0.45)
    label_w = Inches(2.3)

    # Sort channels by pct descending for visual impact
    sorted_channels = sorted(channels.values(), key=lambda c: c["pct"], reverse=True)

    for idx, ch in enumerate(sorted_channels):
        row_y = bar_area_top + idx * bar_spacing

        # Category label
        _add_textbox(
            slide, left_col_left, row_y, label_w, bar_h,
            text=ch["label"], font_size=9, bold=True, color=DARK_TEXT,
            alignment=PP_ALIGN.RIGHT, anchor=MSO_ANCHOR.MIDDLE,
        )

        # Horizontal bar
        pct = ch["pct"]
        bar_w_val = bar_max_w * pct / 100
        if bar_w_val < Inches(0.15):
            bar_w_val = Inches(0.15)

        bar_left = left_col_left + label_w + Inches(0.15)
        bar_color = ch.get("color", ACCENT_BLUE)
        _add_filled_rect(slide, bar_left, row_y + Inches(0.04), bar_w_val, bar_h - Inches(0.08), bar_color)

        # Percentage label
        _add_textbox(
            slide, bar_left + bar_w_val + Inches(0.1), row_y, Inches(0.6), bar_h,
            text=f"{pct}%", font_size=10, bold=True, color=NAVY,
            anchor=MSO_ANCHOR.MIDDLE,
        )

    # ==== RIGHT HALF: Benchmark Data (~45%) ====
    right_col_left = Inches(7.7)
    right_col_w = Inches(5.1)

    _add_textbox(
        slide, right_col_left, section_top, right_col_w, Inches(0.35),
        text="2025 BENCHMARK DATA", font_size=11, bold=True, color=ACCENT_BLUE,
    )
    _add_filled_rect(slide, right_col_left, section_top + Inches(0.33), Inches(2.0), Inches(0.025), ACCENT_BLUE)

    # Benchmark table via shapes
    table_top = section_top + Inches(0.55)
    table_left = right_col_left
    table_w = Inches(4.8)
    row_h = Inches(0.42)

    bench_rows = [
        ("Industry CPA", benchmarks["cpa"]),
        ("Industry CPC", benchmarks["cpc"]),
        ("Estimated CPH", benchmarks["cph"]),
        ("Industry Apply Rate", benchmarks["apply_rate"]),
    ]

    # Table header
    _add_filled_rect(slide, table_left, table_top, table_w, row_h, NAVY)
    _add_textbox(
        slide, table_left + Inches(0.15), table_top, Inches(2.2), row_h,
        text="Metric", font_size=9, bold=True, color=WHITE, anchor=MSO_ANCHOR.MIDDLE,
    )
    _add_textbox(
        slide, table_left + Inches(2.4), table_top, Inches(2.2), row_h,
        text=f"{industry_label} Range", font_size=9, bold=True, color=WHITE,
        anchor=MSO_ANCHOR.MIDDLE,
    )

    for i, (metric, value) in enumerate(bench_rows):
        ry = table_top + row_h * (i + 1)
        bg = WHITE if i % 2 == 0 else LIGHT_BLUE
        _add_filled_rect(slide, table_left, ry, table_w, row_h, bg)
        # Border line at bottom
        _add_filled_rect(slide, table_left, ry + row_h - Inches(0.01), table_w, Inches(0.01),
                         RGBColor(0xCC, 0xCC, 0xCC))

        _add_textbox(
            slide, table_left + Inches(0.15), ry, Inches(2.2), row_h,
            text=metric, font_size=9, bold=True, color=DARK_TEXT, anchor=MSO_ANCHOR.MIDDLE,
        )
        _add_textbox(
            slide, table_left + Inches(2.4), ry, Inches(2.2), row_h,
            text=value, font_size=10, bold=True, color=NAVY, anchor=MSO_ANCHOR.MIDDLE,
        )

    # Source footnote under table
    source_top = table_top + row_h * 5 + Inches(0.08)
    _add_textbox(
        slide, table_left, source_top, table_w, Inches(0.2),
        text="Sources: Appcast 2025, Recruitics TMI, SHRM 2025",
        font_size=7, color=MUTED_TEXT,
    )

    # ==== BOTTOM: Implementation Timeline ====
    timeline_top = Inches(5.15)
    timeline_h = Inches(1.45)

    # Section header
    _add_textbox(
        slide, Inches(0.55), timeline_top, Inches(12.2), Inches(0.32),
        text="IMPLEMENTATION TIMELINE", font_size=11, bold=True, color=ACCENT_BLUE,
    )
    _add_filled_rect(slide, Inches(0.55), timeline_top + Inches(0.3), Inches(2.2), Inches(0.025), ACCENT_BLUE)

    phases = [
        {
            "phase": "PHASE 1",
            "weeks": "Weeks 1-2",
            "title": "Launch & Calibrate",
            "bullets": [
                "Set up campaigns & initial publisher mix",
                "Establish baseline measurement",
                "Configure tracking & attribution",
            ],
            "color": ACCENT_BLUE,
        },
        {
            "phase": "PHASE 2",
            "weeks": "Weeks 3-6",
            "title": "Optimize & Scale",
            "bullets": [
                "ML bid optimization across channels",
                "A/B testing creative & targeting",
                "Expand top-performing publishers",
            ],
            "color": GREEN,
        },
        {
            "phase": "PHASE 3",
            "weeks": "Weeks 7-12",
            "title": "Maximize & Report",
            "bullets": [
                "Full CPQA optimization active",
                "ROI analysis & budget reallocation",
                "Quarterly performance review",
            ],
            "color": NAVY,
        },
    ]

    phase_w = Inches(3.85)
    phase_gap = Inches(0.25)
    phase_top = timeline_top + Inches(0.45)
    phase_h = Inches(1.0)

    for i, ph in enumerate(phases):
        px = Inches(0.55) + i * (phase_w + phase_gap)

        # Top accent bar
        _add_filled_rect(slide, px, phase_top, phase_w, Inches(0.05), ph["color"])

        # Light background
        _add_filled_rect(slide, px, phase_top + Inches(0.05), phase_w, phase_h - Inches(0.05),
                         RGBColor(0xF5, 0xF7, 0xFA))

        # Phase & weeks header
        box, tf = _add_textbox(
            slide, px + Inches(0.12), phase_top + Inches(0.1), phase_w - Inches(0.24), Inches(0.22),
        )
        p = tf.paragraphs[0]
        p.alignment = PP_ALIGN.LEFT
        r1 = p.add_run()
        r1.text = f"{ph['phase']}  "
        _set_font(r1, size=8, bold=True, color=ph["color"])
        r2 = p.add_run()
        r2.text = ph["weeks"]
        _set_font(r2, size=8, bold=False, color=MUTED_TEXT)

        # Title
        _add_textbox(
            slide, px + Inches(0.12), phase_top + Inches(0.3), phase_w - Inches(0.24), Inches(0.22),
            text=ph["title"], font_size=10, bold=True, color=DARK_TEXT,
        )

        # Bullets
        bx, btf = _add_textbox(
            slide, px + Inches(0.12), phase_top + Inches(0.52), phase_w - Inches(0.24), Inches(0.48),
        )
        btf.paragraphs[0].space_before = Pt(0)
        btf.paragraphs[0].space_after = Pt(0)

        for j, bullet in enumerate(ph["bullets"]):
            if j == 0:
                bp = btf.paragraphs[0]
            else:
                bp = btf.add_paragraph()
            bp.space_before = Pt(0)
            bp.space_after = Pt(2)
            bp.alignment = PP_ALIGN.LEFT

            br = bp.add_run()
            br.text = f"\u2022  {bullet}"
            _set_font(br, size=8, color=MUTED_TEXT)

    # Arrow connectors between phases
    for i in range(2):
        ax = Inches(0.55) + (i + 1) * (phase_w + phase_gap) - phase_gap / 2 - Inches(0.07)
        ay = phase_top + phase_h / 2 - Inches(0.08)
        _add_textbox(
            slide, ax, ay, Inches(0.18), Inches(0.18),
            text="\u25B6", font_size=10, bold=True, color=ACCENT_BLUE,
            alignment=PP_ALIGN.CENTER,
        )

    # ---- FOOTER ----
    footer_top = Inches(6.95)
    _add_filled_rect(slide, Inches(0), footer_top, SLIDE_WIDTH, Inches(0.03), NAVY)

    _add_textbox(
        slide, Inches(0.55), footer_top + Inches(0.08), Inches(6), Inches(0.3),
        text=f"Powered by Joveo  |  AI Media Planner  |  {today}",
        font_size=7, color=MUTED_TEXT,
    )
    _add_textbox(
        slide, Inches(6.5), footer_top + Inches(0.08), Inches(6.3), Inches(0.3),
        text="Data sources: Appcast 2025, Recruitics TMI, SHRM 2025",
        font_size=7, color=MUTED_TEXT, alignment=PP_ALIGN.RIGHT,
    )


# ===================================================================
# Public API
# ===================================================================

def generate_pptx(data: Dict[str, Any]) -> bytes:
    """
    Generate a McKinsey-style 2-page PowerPoint presentation.

    Args:
        data: Dictionary containing client information, industry details,
              channel selections, and campaign parameters. Expected keys
              mirror those used by the existing Excel generator.

    Returns:
        bytes: The .pptx file content as bytes, suitable for streaming
               to a client or writing to disk.

    Raises:
        ValueError: If required data fields are missing.
        RuntimeError: If presentation generation fails.
    """
    if data is None or not isinstance(data, dict):
        raise ValueError("Data must be a non-null dictionary.")

    # Ensure minimum required fields have sensible defaults
    data.setdefault("client_name", "Client")
    data.setdefault("industry", "general_entry_level")
    data.setdefault("industry_label", data["industry"].replace("_", " ").title())
    data.setdefault("locations", [])
    # Frontend sends "target_roles" but PPT uses "roles" — normalize
    if "target_roles" in data and "roles" not in data:
        data["roles"] = data["target_roles"]
    data.setdefault("roles", [])
    data.setdefault("campaign_goals", [])
    data.setdefault("channel_categories", {})
    data.setdefault("budget", "TBD")
    # Frontend sends work_environment as array — normalize to string
    we = data.get("work_environment", "hybrid")
    if isinstance(we, list):
        data["work_environment"] = we[0] if we else "hybrid"
    data.setdefault("work_environment", "hybrid")

    try:
        prs = Presentation()

        # Set 16:9 widescreen dimensions
        prs.slide_width = SLIDE_WIDTH
        prs.slide_height = SLIDE_HEIGHT

        _build_slide_1(prs, data)
        _build_slide_2(prs, data)

        buffer = io.BytesIO()
        prs.save(buffer)
        buffer.seek(0)
        return buffer.getvalue()

    except Exception as exc:
        raise RuntimeError(f"Failed to generate PowerPoint presentation: {exc}") from exc


# ===================================================================
# CLI entry point for testing
# ===================================================================

if __name__ == "__main__":
    sample_data = {
        "client_name": "Acme Healthcare",
        "client_website": "https://www.acmehealthcare.com",
        "industry": "healthcare_medical",
        "industry_label": "Healthcare & Medical",
        "locations": ["New York, NY", "Chicago, IL", "Houston, TX", "Phoenix, AZ", "San Diego, CA"],
        "roles": ["Registered Nurse", "Medical Assistant", "Physical Therapist", "Lab Technician", "Pharmacist"],
        "job_categories": ["Clinical", "Allied Health", "Administrative"],
        "use_case": "Scaling clinical hiring across 5 metro areas to meet Q3 demand surge",
        "campaign_goals": ["high_volume", "cost_efficiency", "speed_to_hire"],
        "work_environment": "on_site",
        "budget": "$75,000 / month",
        "competitors": ["HCA Healthcare", "UnitedHealth Group"],
        "channel_categories": {
            "regional_boards": True,
            "global_boards": True,
            "niche_boards": True,
            "social_media": True,
            "programmatic_dsp": True,
            "employer_branding": False,
            "apac_regional": False,
            "emea_regional": False,
        },
        "include_dei": True,
        "include_innovative": False,
        "include_budget_guide": True,
        "include_global_supply": False,
    }

    pptx_bytes = generate_pptx(sample_data)
    output_path = "media_plan_sample.pptx"
    with open(output_path, "wb") as f:
        f.write(pptx_bytes)
    print(f"Generated {output_path} ({len(pptx_bytes):,} bytes)")
