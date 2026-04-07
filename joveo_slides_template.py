"""Joveo-branded Google Slides template builder.

Creates a 15-slide presentation matching the official Joveo Media Plan
template (2026). Each slide is built via Google Slides batchUpdate API
requests with precise positioning, tables, and Joveo brand styling.

Slide structure:
  1.  Title -- "Media Planning Approach <Company>"
  2.  Our Methodology -- 5-step service table
  3.  Campaign Management -- 6-step process flow
  4.  What We've Heard & Assumptions -- requirements + assumptions tables
  5.  Push Meets Pull -- balanced approach
  6.  Benchmarking & Market Research (1/2) -- supply/demand, competition, difficulty
  7.  Benchmarking & Market Research (2/2) -- diversity, compensation
  8.  Campaign Targeting Strategy -- 5-column targeting grid
  9.  Media Plan Development -- channel/week Gantt table
  10. Scenario Planner -- spend estimation flow
  11. Omnichannel Distribution -- channel logos grid
  12. AI-Powered Monitoring & Optimizations -- 8-card grid
  13. Ad Platform Analysis (NEW) -- per-channel benchmark comparison
  14. Reporting & Insights -- analytics + advisory
  15. Thank You -- social links
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EMU = 914400  # 1 inch in EMU
SLIDE_W = 10 * EMU  # 10 inches
SLIDE_H = int(7.5 * EMU)  # 7.5 inches

# Joveo brand colours (RGB 0-1)
PORT_GORE = {"red": 0.125, "green": 0.125, "blue": 0.345}
BLUE_VIOLET = {"red": 0.353, "green": 0.329, "blue": 0.741}
DOWNY_TEAL = {"red": 0.420, "green": 0.702, "blue": 0.804}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
BLACK = {"red": 0.0, "green": 0.0, "blue": 0.0}
LIGHT_PURPLE = {"red": 0.831, "green": 0.816, "blue": 0.941}  # ~#D4D0F0
LIGHTER_PURPLE = {"red": 0.925, "green": 0.918, "blue": 0.965}  # ~#ECEAF7
PINK_ACCENT = {"red": 0.784, "green": 0.345, "blue": 0.612}  # ~#C8589C
LIGHT_PINK = {"red": 0.957, "green": 0.847, "blue": 0.910}  # ~#F4D8E8
LIGHT_GREY = {"red": 0.95, "green": 0.95, "blue": 0.95}
DARK_TEXT = {"red": 0.12, "green": 0.12, "blue": 0.14}


def _uid() -> str:
    """Generate a short unique object ID for Slides API."""
    return f"obj_{uuid.uuid4().hex[:12]}"


def _inches(n: float) -> int:
    """Convert inches to EMU."""
    return int(n * EMU)


def _pt(n: float) -> dict:
    """Font size in points."""
    return {"magnitude": n, "unit": "PT"}


def _emu_size(w: float, h: float) -> dict:
    """Create a size dict from inches."""
    return {
        "width": {"magnitude": _inches(w), "unit": "EMU"},
        "height": {"magnitude": _inches(h), "unit": "EMU"},
    }


def _emu_transform(x: float, y: float, w: float, h: float) -> dict:
    """Create a transform dict (position + size) from inches."""
    return {
        "scaleX": 1,
        "scaleY": 1,
        "translateX": _inches(x),
        "translateY": _inches(y),
        "unit": "EMU",
    }


def _text_box(
    slide_id: str, obj_id: str, x: float, y: float, w: float, h: float
) -> list[dict]:
    """Create a text box shape on a slide."""
    return [
        {
            "createShape": {
                "objectId": obj_id,
                "shapeType": "TEXT_BOX",
                "elementProperties": {
                    "pageObjectId": slide_id,
                    "size": _emu_size(w, h),
                    "transform": _emu_transform(x, y, w, h),
                },
            }
        }
    ]


def _insert_text(obj_id: str, text: str) -> dict:
    """Insert text into a shape."""
    return {"insertText": {"objectId": obj_id, "text": text, "insertionIndex": 0}}


def _style_text(
    obj_id: str,
    bold: bool = False,
    size: float = 12,
    color: dict | None = None,
    font: str = "Inter",
) -> dict:
    """Style all text in a shape."""
    style: dict[str, Any] = {
        "fontFamily": font,
        "fontSize": _pt(size),
        "bold": bold,
    }
    if color:
        style["foregroundColor"] = {"opaqueColor": {"rgbColor": color}}
    fields = "fontFamily,fontSize,bold"
    if color:
        fields += ",foregroundColor"
    return {
        "updateTextStyle": {
            "objectId": obj_id,
            "style": style,
            "textRange": {"type": "ALL"},
            "fields": fields,
        }
    }


def _shape_fill(obj_id: str, color: dict) -> dict:
    """Set shape background fill."""
    return {
        "updateShapeProperties": {
            "objectId": obj_id,
            "shapeProperties": {
                "shapeBackgroundFill": {
                    "solidFill": {"color": {"rgbColor": color}, "alpha": 1.0}
                }
            },
            "fields": "shapeBackgroundFill",
        }
    }


def _shape_border(obj_id: str, color: dict, width: float = 1.0) -> dict:
    """Set shape outline."""
    return {
        "updateShapeProperties": {
            "objectId": obj_id,
            "shapeProperties": {
                "outline": {
                    "outlineFill": {"solidFill": {"color": {"rgbColor": color}}},
                    "weight": {"magnitude": width, "unit": "PT"},
                }
            },
            "fields": "outline",
        }
    }


def _slide_bg(slide_id: str, color: dict) -> dict:
    """Set slide background colour."""
    return {
        "updatePageProperties": {
            "objectId": slide_id,
            "pageProperties": {
                "pageBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}
            },
            "fields": "pageBackgroundFill",
        }
    }


def _create_table(
    slide_id: str,
    table_id: str,
    rows: int,
    cols: int,
    x: float,
    y: float,
    w: float,
    h: float,
) -> dict:
    """Create a table element on a slide."""
    return {
        "createTable": {
            "objectId": table_id,
            "elementProperties": {
                "pageObjectId": slide_id,
                "size": _emu_size(w, h),
                "transform": _emu_transform(x, y, w, h),
            },
            "rows": rows,
            "columns": cols,
        }
    }


def _table_text(table_id: str, row: int, col: int, text: str) -> dict:
    """Insert text into a table cell."""
    return {
        "insertText": {
            "objectId": table_id,
            "cellLocation": {"rowIndex": row, "columnIndex": col},
            "text": text,
            "insertionIndex": 0,
        }
    }


def _table_cell_bg(
    table_id: str,
    row_start: int,
    row_end: int,
    col_start: int,
    col_end: int,
    color: dict,
) -> dict:
    """Set background for a range of table cells."""
    return {
        "updateTableCellProperties": {
            "objectId": table_id,
            "tableRange": {
                "location": {"rowIndex": row_start, "columnIndex": col_start},
                "rowSpan": row_end - row_start,
                "columnSpan": col_end - col_start,
            },
            "tableCellProperties": {
                "tableCellBackgroundFill": {"solidFill": {"color": {"rgbColor": color}}}
            },
            "fields": "tableCellBackgroundFill",
        }
    }


def _footer_requests(slide_id: str) -> list[dict]:
    """Add Joveo footer (copyright text) to a slide."""
    fid = _uid()
    reqs = _text_box(slide_id, fid, 0.4, 6.9, 4.0, 0.4)
    reqs.append(_insert_text(fid, "\u00a9 2026 Joveo, Inc. All Rights Reserved"))
    reqs.append(_style_text(fid, size=8, color=PORT_GORE))
    return reqs


# ---------------------------------------------------------------------------
# Individual slide builders
# ---------------------------------------------------------------------------


def _slide_title(data: dict) -> tuple[str, list[dict]]:
    """Slide 1: Title -- Media Planning Approach <Company>."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    client = data.get("client_name") or data.get("company_name") or "Client"

    # Main title
    t1 = _uid()
    reqs += _text_box(sid, t1, 0.6, 1.0, 8.8, 1.0)
    reqs.append(_insert_text(t1, "Media Planning Approach"))
    reqs.append(_style_text(t1, bold=True, size=36, color=BLUE_VIOLET))

    # Company name subtitle
    t2 = _uid()
    reqs += _text_box(sid, t2, 0.6, 1.9, 8.8, 0.8)
    reqs.append(_insert_text(t2, f"<{client}>"))
    reqs.append(_style_text(t2, bold=True, size=28, color=PORT_GORE))

    # Date
    t3 = _uid()
    generated = datetime.now(timezone.utc).strftime("%B %Y")
    reqs += _text_box(sid, t3, 0.6, 2.8, 4.0, 0.4)
    reqs.append(_insert_text(t3, generated))
    reqs.append(_style_text(t3, size=14, color=DARK_TEXT))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_methodology(data: dict) -> tuple[str, list[dict]]:
    """Slide 2: Our Methodology -- 5-step service table."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    # Title
    t1 = _uid()
    reqs += _text_box(sid, t1, 0.6, 0.3, 8.0, 0.7)
    reqs.append(_insert_text(t1, "Our Methodology"))
    reqs.append(_style_text(t1, bold=True, size=28, color=PORT_GORE))

    # 5-step methodology table (4 cols: #, Service, What, Teams)
    tid = _uid()
    reqs.append(_create_table(sid, tid, 6, 3, 0.6, 1.2, 8.8, 5.2))

    headers = ["Service Provided", "What does it involve?", "Teams Involved"]
    for ci, h in enumerate(headers):
        reqs.append(_table_text(tid, 0, ci, h))
    reqs.append(_table_cell_bg(tid, 0, 1, 0, 3, LIGHT_PURPLE))

    steps = [
        (
            "Benchmarking & Market Research",
            "Talent supply & demand analysis\nKey competitor analysis\nDetermining hiring difficulty",
            "Labor Economist\nData Sciences",
        ),
        (
            "Campaign Targeting Strategy",
            "Companies\nJob titles & Skills\nSearch Keywords & Audience Targeting",
            "Account Manager\nMedia & Campaign Manager\nSocial Media Manager",
        ),
        (
            "Media Plan Development",
            "Publisher mix determination\nSpend allocation with timelines\nJob distribution and campaign launch",
            "Account Manager\nMedia & Campaign Manager\nSocial Media Manager",
        ),
        (
            "Monitoring & Optimization",
            "Down the funnel tracking in real-time\nTrading optimization\nSocial media optimization\nApply optimization",
            "Account Manager\nMedia & Campaign Manager\nSocial Media Manager",
        ),
        (
            "Reporting & Insights",
            "Professional advisory services\nWeekly reviews to align on objectives",
            "Account Manager",
        ),
    ]

    for ri, (svc, involves, teams) in enumerate(steps, start=1):
        reqs.append(_table_text(tid, ri, 0, f"{ri}. {svc}"))
        reqs.append(_table_text(tid, ri, 1, involves))
        reqs.append(_table_text(tid, ri, 2, teams))
        bg = LIGHTER_PURPLE if ri % 2 == 0 else WHITE
        reqs.append(_table_cell_bg(tid, ri, ri + 1, 0, 3, bg))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_campaign_mgmt(data: dict) -> tuple[str, list[dict]]:
    """Slide 3: Campaign Management -- 6-step process flow."""
    sid = _uid()
    client = data.get("client_name") or "Client"
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.4, 0.3, 9.2, 1.0)
    reqs.append(
        _insert_text(
            t1, f"Campaign Management Matches {client} Objectives to Ensure Success"
        )
    )
    reqs.append(_style_text(t1, bold=True, size=24, color=PORT_GORE))

    steps = [
        "1. Benchmarking & market research",
        "2. Develop Media Strategy",
        "3. Job distribution & campaign launch",
        "4. AI-Powered publisher selection, bid & budget optimization",
        "5. AI-Powered job content optimization & expansions",
        "6. Optimize to down-funnel conversions",
    ]

    # Render as 6 text boxes in a zigzag layout
    positions = [
        (0.5, 1.8),
        (2.0, 3.5),
        (3.5, 1.8),
        (5.0, 3.5),
        (6.5, 1.8),
        (8.0, 3.5),
    ]
    for i, (step_text, (px, py)) in enumerate(zip(steps, positions)):
        box_id = _uid()
        reqs += _text_box(sid, box_id, px, py, 1.8, 1.5)
        reqs.append(_insert_text(box_id, step_text))
        reqs.append(_style_text(box_id, bold=False, size=10, color=PORT_GORE))
        reqs.append(_shape_fill(box_id, LIGHTER_PURPLE))
        reqs.append(_shape_border(box_id, BLUE_VIOLET, 1.5))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_requirements(data: dict) -> tuple[str, list[dict]]:
    """Slide 4: What We've Heard and Our Assumptions."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.6, 0.3, 8.0, 0.7)
    reqs.append(_insert_text(t1, "What We've Heard and Our Assumptions"))
    reqs.append(_style_text(t1, bold=True, size=28, color=PORT_GORE))

    # Extract data fields
    roles = data.get("roles") or data.get("target_roles") or []
    locations = data.get("locations") or []
    budget = data.get("budget") or data.get("budget_range") or "TBD"
    duration = data.get("duration") or data.get("campaign_duration") or "4 weeks"
    industry = data.get("industry_label") or data.get("industry") or "General"
    goals = data.get("campaign_goals") or []
    channels = data.get("channels") or data.get("channel_recommendations") or []

    total_hires = data.get("total_hires") or data.get("hiring_volume") or "TBD"
    category = (
        industry.replace("_", " ").title()
        if isinstance(industry, str)
        else str(industry)
    )

    # Left table: Your Requirement Summary
    lt = _uid()
    reqs.append(_create_table(sid, lt, 6, 2, 0.4, 1.3, 4.2, 3.8))
    reqs.append(_table_text(lt, 0, 0, "Your Requirement Summary"))
    reqs.append(_table_text(lt, 0, 1, ""))
    reqs.append(_table_cell_bg(lt, 0, 1, 0, 2, LIGHT_PURPLE))

    req_rows = [
        ("Total Hires", str(total_hires)),
        ("Category of Hires", category),
        ("Campaign Timeline", str(duration)),
        ("# Job Titles", str(len(roles)) if roles else "TBD"),
        ("# Markets", str(len(locations)) if locations else "TBD"),
    ]
    for ri, (label, val) in enumerate(req_rows, 1):
        reqs.append(_table_text(lt, ri, 0, label))
        reqs.append(_table_text(lt, ri, 1, val))

    # Right table: Our Key Assumptions
    rt = _uid()
    reqs.append(_create_table(sid, rt, 6, 2, 5.0, 1.3, 4.6, 3.8))
    reqs.append(_table_text(rt, 0, 0, "Our Key Assumptions"))
    reqs.append(_table_text(rt, 0, 1, ""))
    reqs.append(_table_cell_bg(rt, 0, 1, 0, 2, LIGHT_PURPLE))

    # Compute CPA estimate from channels
    cpa_est = "TBD"
    if channels:
        cpas = [
            c.get("cpa") or c.get("cost_per_apply") or 0
            for c in channels
            if isinstance(c, dict)
        ]
        cpas = [float(c) for c in cpas if c]
        if cpas:
            cpa_est = f"${sum(cpas) / len(cpas):,.2f}"

    assumptions = [
        ("Target Hires", str(total_hires)),
        ("CPA Estimate", cpa_est),
        ("ATH%", data.get("ath_pct") or "TBD"),
        ("Applications needed", data.get("applications_needed") or "TBD"),
        ("Campaign Duration", str(duration)),
    ]
    for ri, (label, val) in enumerate(assumptions, 1):
        reqs.append(_table_text(rt, ri, 0, label))
        reqs.append(_table_text(rt, ri, 1, str(val)))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_push_pull(data: dict) -> tuple[str, list[dict]]:
    """Slide 5: Push Meets Pull -- balanced approach."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.6, 0.3, 8.0, 0.7)
    reqs.append(_insert_text(t1, "Push Meets Pull..."))
    reqs.append(_style_text(t1, bold=True, size=28, color=PORT_GORE))

    # Left label
    lb = _uid()
    reqs += _text_box(sid, lb, 0.6, 2.5, 2.5, 1.5)
    reqs.append(_insert_text(lb, "A Balanced\nApproach"))
    reqs.append(_style_text(lb, bold=True, size=22, color=PORT_GORE))

    concepts = [
        (
            "Push = Active Outreach",
            'Targeted ads, job alerts, social media and email campaigns that proactively "push" opportunities to candidates based on behavior, demographics, or intent signals.',
            BLUE_VIOLET,
        ),
        (
            "Pull = Employer Brand Magnetism",
            'Content, reputation, social media and SEO-optimized career pages that "pull" candidates in by building long-term interest and trust.',
            PORT_GORE,
        ),
        (
            "Symbiosis Drives Results",
            "The best recruitment outcomes happen when push efforts spark interest and pull strategies convert it into action.",
            PINK_ACCENT,
        ),
    ]

    for i, (title, desc, border_color) in enumerate(concepts):
        bx = _uid()
        y_pos = 1.3 + i * 1.7
        reqs += _text_box(sid, bx, 3.5, y_pos, 6.0, 1.4)
        reqs.append(_insert_text(bx, f"{title}: {desc}"))
        reqs.append(_style_text(bx, size=11, color=DARK_TEXT))
        reqs.append(_shape_border(bx, border_color, 2.0))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_benchmarking_1(data: dict) -> tuple[str, list[dict]]:
    """Slide 6: Benchmarking & Market Research (1/2)."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "Benchmarking & Market Research (1/2)"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    # Section label
    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "1"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    insights = data.get("market_insights") or {}
    difficulty = data.get("difficulty_framework") or {}
    competitor = data.get("competitor_mapping") or {}

    # A) Talent Supply vs. Demand
    a_box = _uid()
    reqs += _text_box(sid, a_box, 0.4, 1.0, 4.4, 1.8)
    supply_text = "A) Talent Supply vs. Demand\n\n"
    hiring_diff = (
        insights.get("hiring_difficulty") or difficulty.get("score") or "moderate"
    )
    supply_text += f"Hiring difficulty: {hiring_diff}\n"
    supply_text += f"Demand trend: {insights.get('demand_trend') or 'stable'}\n"
    supply_text += f"Competition level: {insights.get('competition_level') or 'medium'}"
    reqs.append(_insert_text(a_box, supply_text))
    reqs.append(_style_text(a_box, size=11, color=DARK_TEXT))
    reqs.append(_shape_border(a_box, BLUE_VIOLET, 1.0))

    # B) Competitive Landscape
    b_box = _uid()
    reqs += _text_box(sid, b_box, 5.2, 1.0, 4.4, 1.8)
    comp_text = "B) Competitive Landscape & Concentration\n\n"
    competitors = data.get("competitors") or []
    if competitor and isinstance(competitor, dict):
        for comp_name, comp_data in list(competitor.items())[:5]:
            if isinstance(comp_data, dict):
                share = (
                    comp_data.get("market_share")
                    or comp_data.get("posting_share")
                    or ""
                )
                comp_text += f"{comp_name}: {share}\n"
            else:
                comp_text += f"{comp_name}: {comp_data}\n"
    elif competitors:
        for c in competitors[:5]:
            comp_text += f"- {c}\n"
    else:
        comp_text += "Competitor data will be populated during engagement."
    reqs.append(_insert_text(b_box, comp_text))
    reqs.append(_style_text(b_box, size=11, color=DARK_TEXT))
    reqs.append(_shape_border(b_box, BLUE_VIOLET, 1.0))

    # C) Hiring Difficulty
    c_box = _uid()
    reqs += _text_box(sid, c_box, 0.4, 3.2, 9.2, 2.8)
    diff_text = "C) Hiring Difficulty\n\n"
    if isinstance(difficulty, dict):
        for k, v in list(difficulty.items())[:6]:
            label = k.replace("_", " ").title()
            diff_text += f"{label}: {v}\n"
    else:
        diff_text += f"Relative Supply: {insights.get('competition_level') or 'TBD'}\n"
        diff_text += f"Hiring Difficulty Score: {hiring_diff}\n"
        diff_text += f"Salary Range: {insights.get('salary_range') or 'TBD'}"
    reqs.append(_insert_text(c_box, diff_text))
    reqs.append(_style_text(c_box, size=11, color=DARK_TEXT))
    reqs.append(_shape_border(c_box, BLUE_VIOLET, 1.0))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_benchmarking_2(data: dict) -> tuple[str, list[dict]]:
    """Slide 7: Benchmarking & Market Research (2/2) -- diversity & compensation."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "Benchmarking & Market Research (2/2)"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "1"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    insights = data.get("market_insights") or {}
    salary = insights.get("salary_range") or "Market data to be provided"

    # Left: Diversity Benchmarking
    d_box = _uid()
    reqs += _text_box(sid, d_box, 0.4, 1.0, 4.4, 5.0)
    div_text = "Diversity Benchmarking\n\n"
    div_text += "Gender Diversity for this position\n"
    div_text += "Data to be populated during engagement.\n\n"
    div_text += "Ethnic Diversity for this position\n"
    div_text += "Data to be populated during engagement."
    reqs.append(_insert_text(d_box, div_text))
    reqs.append(_style_text(d_box, size=12, color=DARK_TEXT))
    reqs.append(_shape_border(d_box, BLUE_VIOLET, 1.0))

    # Right: Compensation Benchmarking
    c_box = _uid()
    reqs += _text_box(sid, c_box, 5.2, 1.0, 4.4, 5.0)
    comp_text = "Compensation Benchmarking\n\n"
    comp_text += f"Salary Range: {salary}\n\n"
    comp_text += "Market Followers | Market Payers | Market Leaders\n"
    comp_text += "Detailed salary benchmarks to be provided during engagement."
    reqs.append(_insert_text(c_box, comp_text))
    reqs.append(_style_text(c_box, size=12, color=DARK_TEXT))
    reqs.append(_shape_border(c_box, BLUE_VIOLET, 1.0))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_targeting(data: dict) -> tuple[str, list[dict]]:
    """Slide 8: Campaign Targeting Strategy -- 5-column grid."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "2"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "Campaign Targeting Strategy"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    roles = data.get("roles") or data.get("target_roles") or []
    if roles and isinstance(roles[0], dict):
        roles = [r.get("title") or str(r) for r in roles]
    competitors = data.get("competitors") or []
    goals = data.get("campaign_goals") or []

    # 5-column table
    tid = _uid()
    max_rows = max(len(roles), len(competitors), 4) + 1
    reqs.append(_create_table(sid, tid, max_rows, 5, 0.4, 1.0, 9.2, 4.8))

    col_headers = [
        "Target Employers",
        "Skills",
        "Job Titles",
        "Search Keywords",
        "Audience Targeting",
    ]
    for ci, h in enumerate(col_headers):
        reqs.append(_table_text(tid, 0, ci, h))
    reqs.append(_table_cell_bg(tid, 0, 1, 0, 5, LIGHTER_PURPLE))

    # Populate with available data
    for ri, role in enumerate(roles[: max_rows - 1], 1):
        reqs.append(_table_text(tid, ri, 2, str(role)))  # Job Titles column

    for ri, comp in enumerate(competitors[: max_rows - 1], 1):
        reqs.append(_table_text(tid, ri, 0, str(comp)))  # Target Employers

    # Audience Targeting (static best practices)
    targeting_text = "1. Based on Interests\n2. Custom Audiences\n3. Retargeting"
    if max_rows > 1:
        reqs.append(_table_text(tid, 1, 4, targeting_text))

    # Competitive Insight box at bottom
    ci_box = _uid()
    reqs += _text_box(sid, ci_box, 0.4, 6.0, 9.2, 0.5)
    insight = (
        data.get("competitive_insight")
        or "Competitive insight to be populated during engagement."
    )
    reqs.append(_insert_text(ci_box, f"Competitive Insight: {insight}"))
    reqs.append(_style_text(ci_box, size=10, color=PORT_GORE))
    reqs.append(_shape_fill(ci_box, LIGHT_PINK))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_media_plan_table(data: dict) -> tuple[str, list[dict]]:
    """Slide 9: Media Plan Development -- channel/week Gantt table."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "3"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "Media Plan Development"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    channels = data.get("channels") or data.get("channel_recommendations") or []

    # Table: Stage | Type | Channel | Target Audience | Details | Week 1-5 | Total Cost
    num_channels = min(len(channels), 8) + 1  # +1 for header
    cols = 11  # Stage, Type, Channel, Target Audience, Details, W1-W5, Total Cost
    tid = _uid()
    reqs.append(_create_table(sid, tid, num_channels, cols, 0.2, 1.0, 9.6, 5.2))

    headers = [
        "Stage",
        "Type",
        "Channel",
        "Target\nAudience",
        "Details",
        "Week 1",
        "Week 2",
        "Week 3",
        "Week 4",
        "Week 5",
        "Total\nCost",
    ]
    for ci, h in enumerate(headers):
        reqs.append(_table_text(tid, 0, ci, h))
    reqs.append(_table_cell_bg(tid, 0, 1, 0, cols, LIGHTER_PURPLE))

    for ri, ch in enumerate(channels[:8], 1):
        if not isinstance(ch, dict):
            continue
        name = ch.get("name") or ch.get("channel") or ""
        category = ch.get("category") or ch.get("type") or ""
        budget_val = ch.get("budget") or ch.get("allocation") or 0
        try:
            budget_num = float(str(budget_val).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            budget_num = 0

        reqs.append(_table_text(tid, ri, 0, "Launch"))
        reqs.append(_table_text(tid, ri, 1, category))
        reqs.append(_table_text(tid, ri, 2, name))
        reqs.append(_table_text(tid, ri, 3, "Job seekers"))
        reqs.append(
            _table_text(tid, ri, 4, ch.get("notes") or ch.get("rationale") or "")
        )
        # Mark active weeks with budget
        weekly = budget_num / 4 if budget_num else 0
        for wk in range(5, 10):
            if wk < 9:  # weeks 1-4 active
                reqs.append(
                    _table_text(tid, ri, wk, f"${weekly:,.0f}" if weekly else "")
                )
            else:
                reqs.append(_table_text(tid, ri, wk, ""))
        reqs.append(
            _table_text(tid, ri, 10, f"${budget_num:,.0f}" if budget_num else "TBD")
        )

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_scenario_planner(data: dict) -> tuple[str, list[dict]]:
    """Slide 10: Media Plan Development -- Scenario Planner."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "3"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    title_text = "Media Plan Development \u2013 enabled by Scenario Planner"
    reqs.append(_insert_text(t1, title_text))
    reqs.append(_style_text(t1, bold=True, size=20, color=PORT_GORE))

    channels = data.get("channels") or []
    budget = data.get("budget") or data.get("budget_range") or "TBD"
    roles = data.get("roles") or data.get("target_roles") or []
    locations = data.get("locations") or []

    # Input box
    inp = _uid()
    reqs += _text_box(sid, inp, 0.4, 1.2, 2.5, 2.5)
    input_text = "Input\n\n"
    input_text += (
        f"Job Title: {', '.join(str(r) for r in roles[:3]) if roles else 'TBD'}\n\n"
    )
    input_text += f"Location: {', '.join(str(l) for l in locations[:3]) if locations else 'TBD'}\n\n"
    input_text += f"Target Timeline: {data.get('duration') or '4 weeks'}"
    reqs.append(_insert_text(inp, input_text))
    reqs.append(_style_text(inp, size=11, color=DARK_TEXT))
    reqs.append(_shape_border(inp, BLUE_VIOLET, 1.5))

    # Estimated Spend box
    spend = _uid()
    reqs += _text_box(sid, spend, 3.3, 1.2, 3.0, 2.5)
    spend_text = "Estimated Spend\n\n"
    spend_text += f"Cost per Qualified Apply\n"
    cpas = [
        float(c.get("cpa") or c.get("cost_per_apply") or 0)
        for c in channels
        if isinstance(c, dict) and (c.get("cpa") or c.get("cost_per_apply"))
    ]
    avg_cpa = sum(cpas) / len(cpas) if cpas else 0
    spend_text += f"${avg_cpa:,.2f}\n\n"
    spend_text += f"Applies and Total Spend\n"
    total_applies = sum(
        int(float(c.get("projected_applies") or c.get("estimated_applies") or 0))
        for c in channels
        if isinstance(c, dict)
    )
    spend_text += f"{total_applies:,} applies | Budget: {budget}"
    reqs.append(_insert_text(spend, spend_text))
    reqs.append(_style_text(spend, size=11, color=DARK_TEXT))
    reqs.append(_shape_border(spend, BLUE_VIOLET, 1.5))

    # Optimal Distribution box
    dist = _uid()
    reqs += _text_box(sid, dist, 3.3, 4.0, 3.0, 2.5)
    dist_text = "Optimal Distribution\n\n"
    dist_text += "Publisher/channel selection\n"
    dist_text += "Budget Allocation\n"
    dist_text += "AI recommended strategy"
    reqs.append(_insert_text(dist, dist_text))
    reqs.append(_style_text(dist, size=11, color=DARK_TEXT))
    reqs.append(_shape_border(dist, BLUE_VIOLET, 1.5))

    # AI Strategy box
    ai_box = _uid()
    reqs += _text_box(sid, ai_box, 6.6, 3.5, 3.0, 3.0)
    ai_text = "Are you flexible with your timeline?\n"
    ai_text += "Here's our AI recommended strategies\n\n"
    recs = data.get("recommendations") or []
    for rec in recs[:3]:
        ai_text += f"\u2192 {rec}\n"
    reqs.append(_insert_text(ai_box, ai_text))
    reqs.append(_style_text(ai_box, size=10, color=DARK_TEXT))
    reqs.append(_shape_border(ai_box, BLUE_VIOLET, 1.0))
    reqs.append(_shape_fill(ai_box, LIGHTER_PURPLE))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_omnichannel(data: dict) -> tuple[str, list[dict]]:
    """Slide 11: Media Plan Development -- Omnichannel Distribution."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "3"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(
        _insert_text(
            t1, "Media Plan Development \u2013 enabled by Omnichannel Distribution"
        )
    )
    reqs.append(_style_text(t1, bold=True, size=20, color=PORT_GORE))

    # Four category boxes
    categories = [
        (
            "Thousands of Job Boards",
            "LinkedIn, ZipRecruiter, Indeed, Talent.com, CareerBuilder, Jobcase, Job.com, FairyGodBoss, CareerGaller, JobCloud, TopUSAJobs, Google Ads",
        ),
        ("Social Media", "Facebook, Instagram, X, Snapchat, TikTok"),
        ("Search and Display Ads", "Google Ads, YouTube Ads, Google Display Network"),
        ("Other Channels", "Outdoor Media, OTT, Radio and Audio"),
    ]

    positions = [
        (0.4, 1.0, 4.4, 2.5),
        (5.0, 1.0, 4.6, 2.5),
        (0.4, 3.8, 4.4, 2.5),
        (5.0, 3.8, 4.6, 2.5),
    ]

    for (title, items), (bx, by, bw, bh) in zip(categories, positions):
        box = _uid()
        reqs += _text_box(sid, box, bx, by, bw, bh)
        reqs.append(_insert_text(box, f"{title}\n\n{items}"))
        reqs.append(_style_text(box, size=11, color=PORT_GORE))
        reqs.append(_shape_border(box, BLUE_VIOLET, 1.5))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_monitoring(data: dict) -> tuple[str, list[dict]]:
    """Slide 12: AI-Powered Monitoring & Optimizations -- 8-card grid."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "4"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "AI-Powered Monitoring & Optimizations"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    cards = [
        ("Bid Optimization", "Automated bid management to drive ROI"),
        (
            "Job Title & Content",
            "A/B testing of job titles to reach the right job seekers",
        ),
        ("Location Expansions", "Hyper-target job seekers at the zipcode level"),
        (
            "Retargeting & Re-engagement",
            "Reach talent in your database via multiple channels",
        ),
        (
            "Funnel Optimization",
            "Optimize campaigns at every single stage of your funnel",
        ),
        (
            "Apply Optimization",
            "Improve candidate experience by streamlining apply process",
        ),
        (
            "Social & Search Targeting",
            "Keyword & demographic targeting based on performance",
        ),
        ("Budget Distribution", "Continuous budget reallocation based on performance"),
    ]

    for i, (title, desc) in enumerate(cards):
        row, col = divmod(i, 4)
        x = 0.4 + col * 2.35
        y = 1.2 + row * 2.8
        box = _uid()
        reqs += _text_box(sid, box, x, y, 2.1, 2.2)
        reqs.append(_insert_text(box, f"{title}\n\n{desc}"))
        reqs.append(_style_text(box, size=10, color=PORT_GORE))
        reqs.append(_shape_border(box, LIGHT_GREY, 1.0))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_ad_platform_analysis(data: dict) -> tuple[str, list[dict]]:
    """Slide 13 (NEW): Ad Platform Analysis -- per-channel benchmarks."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.6, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "Ad Platform Analysis"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    channels = data.get("channels") or data.get("channel_recommendations") or []

    num_rows = min(len(channels), 10) + 1
    tid = _uid()
    reqs.append(_create_table(sid, tid, num_rows, 7, 0.3, 1.0, 9.4, 5.2))

    headers = [
        "Channel",
        "Category",
        "Budget",
        "CPC",
        "CPA",
        "Est. Clicks",
        "Est. Applies",
    ]
    for ci, h in enumerate(headers):
        reqs.append(_table_text(tid, 0, ci, h))
    reqs.append(_table_cell_bg(tid, 0, 1, 0, 7, LIGHT_PURPLE))

    for ri, ch in enumerate(channels[:10], 1):
        if not isinstance(ch, dict):
            continue
        name = ch.get("name") or ch.get("channel") or ""
        cat = ch.get("category") or ch.get("type") or ""
        budget_val = ch.get("budget") or ch.get("allocation") or 0
        cpc = ch.get("cpc") or ch.get("cost_per_click") or 0
        cpa = ch.get("cpa") or ch.get("cost_per_apply") or 0
        clicks = ch.get("projected_clicks") or ch.get("estimated_clicks") or 0
        applies = ch.get("projected_applies") or ch.get("estimated_applies") or 0

        try:
            reqs.append(_table_text(tid, ri, 0, name))
            reqs.append(_table_text(tid, ri, 1, cat))
            reqs.append(_table_text(tid, ri, 2, f"${float(budget_val):,.0f}"))
            reqs.append(_table_text(tid, ri, 3, f"${float(cpc):,.2f}"))
            reqs.append(_table_text(tid, ri, 4, f"${float(cpa):,.2f}"))
            reqs.append(_table_text(tid, ri, 5, f"{int(float(clicks)):,}"))
            reqs.append(_table_text(tid, ri, 6, f"{int(float(applies)):,}"))
        except (ValueError, TypeError):
            reqs.append(_table_text(tid, ri, 0, name))

        bg = LIGHTER_PURPLE if ri % 2 == 0 else WHITE
        reqs.append(_table_cell_bg(tid, ri, ri + 1, 0, 7, bg))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_reporting(data: dict) -> tuple[str, list[dict]]:
    """Slide 14: Reporting & Insights."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    num = _uid()
    reqs += _text_box(sid, num, 0.3, 0.2, 0.4, 0.4)
    reqs.append(_insert_text(num, "5"))
    reqs.append(_style_text(num, bold=True, size=16, color=WHITE))
    reqs.append(_shape_fill(num, BLUE_VIOLET))

    t1 = _uid()
    reqs += _text_box(sid, t1, 0.8, 0.2, 8.0, 0.6)
    reqs.append(_insert_text(t1, "Reporting & Insights"))
    reqs.append(_style_text(t1, bold=True, size=22, color=PORT_GORE))

    # Subtitle
    sub = _uid()
    reqs += _text_box(sid, sub, 0.4, 0.9, 9.2, 0.5)
    reqs.append(
        _insert_text(
            sub,
            "Get detailed reporting and relevant insights through the right combination of Technology and People",
        )
    )
    reqs.append(_style_text(sub, size=11, color=DARK_TEXT))
    reqs.append(_shape_fill(sub, LIGHTER_PURPLE))

    # Left: Unified Analytics
    ua = _uid()
    reqs += _text_box(sid, ua, 0.4, 1.7, 4.4, 4.5)
    ua_text = "Unified Analytics\n\n"
    ua_text += "Predictive Job Level Insights\n"
    ua_text += "360\u00b0 View across your funnel\n"
    ua_text += "Recommendations to improve ROI\n\n"
    ua_text += "Quality of jobs scoring\n"
    ua_text += "At-risk category identification\n"
    ua_text += "Performance benchmarking"
    reqs.append(_insert_text(ua, ua_text))
    reqs.append(_style_text(ua, size=12, color=PORT_GORE))
    reqs.append(_shape_border(ua, BLUE_VIOLET, 1.5))

    # Right: Professional Advisory Services
    pa = _uid()
    reqs += _text_box(sid, pa, 5.2, 1.7, 4.4, 4.5)
    pa_text = "Professional Advisory Services\n\n"
    pa_text += "Dedicated Account Strategy Team\n\n"
    pa_text += "Proactive Project Management & Joint Success Planning\n\n"
    pa_text += "Weekly and Quarterly Business Reviews\n\n"
    pa_text += "Best-in-class response & resolution SLAs\n\n"
    pa_text += "Consultative advice curated to your needs"
    reqs.append(_insert_text(pa, pa_text))
    reqs.append(_style_text(pa, size=12, color=PORT_GORE))
    reqs.append(_shape_fill(pa, LIGHTER_PURPLE))

    reqs += _footer_requests(sid)
    return sid, reqs


def _slide_thank_you(data: dict) -> tuple[str, list[dict]]:
    """Slide 15: Thank You -- social links."""
    sid = _uid()
    reqs: list[dict] = [
        {
            "createSlide": {
                "objectId": sid,
                "slideLayoutReference": {"predefinedLayout": "BLANK"},
            }
        },
        _slide_bg(sid, WHITE),
    ]

    # Thank You text
    ty = _uid()
    reqs += _text_box(sid, ty, 1.5, 2.0, 7.0, 1.5)
    reqs.append(_insert_text(ty, "Thank You!"))
    reqs.append(_style_text(ty, bold=True, size=40, color=BLUE_VIOLET))

    # Subtitle
    sub = _uid()
    reqs += _text_box(sid, sub, 2.5, 3.5, 5.0, 0.5)
    reqs.append(_insert_text(sub, "We look forward to our next conversation"))
    reqs.append(_style_text(sub, size=14, color=DARK_TEXT))

    # Social links box
    links = _uid()
    reqs += _text_box(sid, links, 1.5, 4.5, 7.0, 1.0)
    links_text = "https://x.com/joveoinc    |    www.linkedin.com/company/joveo/    |    www.joveo.com"
    reqs.append(_insert_text(links, links_text))
    reqs.append(_style_text(links, size=11, color=PORT_GORE))
    reqs.append(_shape_fill(links, LIGHTER_PURPLE))

    reqs += _footer_requests(sid)
    return sid, reqs


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_joveo_slides(data: dict, presentation: dict) -> list[dict]:
    """Build all 15 Joveo-branded slides as batchUpdate requests.

    Args:
        data: Media plan data dict with channels, budget, roles, etc.
        presentation: The presentation object returned by presentations().create().

    Returns:
        List of batchUpdate request dicts ready for the Slides API.
    """
    requests_list: list[dict] = []

    # Delete the default blank slide
    slides = presentation.get("slides") or []
    if slides:
        requests_list.append({"deleteObject": {"objectId": slides[0]["objectId"]}})

    # Build all 15 slides in order
    builders = [
        _slide_title,
        _slide_methodology,
        _slide_campaign_mgmt,
        _slide_requirements,
        _slide_push_pull,
        _slide_benchmarking_1,
        _slide_benchmarking_2,
        _slide_targeting,
        _slide_media_plan_table,
        _slide_scenario_planner,
        _slide_omnichannel,
        _slide_monitoring,
        _slide_ad_platform_analysis,  # NEW slide
        _slide_reporting,
        _slide_thank_you,
    ]

    for builder in builders:
        _sid, slide_reqs = builder(data)
        requests_list.extend(slide_reqs)

    return requests_list
