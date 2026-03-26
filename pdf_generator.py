"""
Print-optimized HTML report generator for AI Media Plan Generator.

Generates a standalone HTML document with @media print CSS for clean
A4 PDF output via Ctrl+P / browser print. No external dependencies --
all CSS is inline, all fonts are system fonts.

Color scheme: Brand (Port Gore navy #202058, Blue Violet #5A54BD)
adapted for print-friendly output (white background, dark text).
"""

from __future__ import annotations

import html
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Brand Colors (print-friendly adaptations)
# ---------------------------------------------------------------------------
PORT_GORE = "#202058"  # Navy -- headings
BLUE_VIOLET = "#5A54BD"  # Purple accent
DOWNY_TEAL = "#6BB3CD"  # Secondary accent
TAPESTRY_PINK = "#B5669C"  # Tertiary accent
RAW_SIENNA = "#CE9047"  # Warm accent
TEXT_DARK = "#1a1a2e"  # Body text
TEXT_MUTED = "#555566"  # Secondary text
BORDER_LIGHT = "#d0d0e0"  # Table borders
BG_ZEBRA = "#f4f4f9"  # Zebra row background
BG_WHITE = "#ffffff"

# Bar chart colors (cycle through brand palette)
BAR_COLORS = [
    BLUE_VIOLET,
    DOWNY_TEAL,
    TAPESTRY_PINK,
    RAW_SIENNA,
    PORT_GORE,
    "#7C6BC4",
    "#4A9CB5",
]


def _safe(value: Any) -> str:
    """HTML-escape any user-provided value."""
    if value is None:
        return ""
    return html.escape(str(value))


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
        return _safe(value)


def _format_number(value: Any) -> str:
    """Format a numeric value with commas."""
    try:
        num = float(value)
        if num == int(num):
            return f"{int(num):,}"
        return f"{num:,.2f}"
    except (TypeError, ValueError):
        return _safe(value)


def _format_pct(value: Any) -> str:
    """Format a percentage value."""
    try:
        num = float(value)
        return f"{num:.1f}%"
    except (TypeError, ValueError):
        return _safe(value)


def generate_plan_html_report(
    plan_data: Dict[str, Any],
    client_name: str,
    industry: str,
) -> str:
    """Generate a print-optimized HTML media plan report.

    Parameters
    ----------
    plan_data : dict
        Must contain:
        - budget : float or str
        - channels : list of dicts with keys: name, allocation_pct, spend,
          cpc, cpa, projected_clicks, projected_applies, projected_hires
        - roles : list of str
        - locations : list of str
        - market_intelligence : dict (optional, arbitrary key-value pairs)
        - recommendations : list of str (optional)

    client_name : str
        Client / company name for the report header.

    industry : str
        Industry vertical label.

    Returns
    -------
    str
        Complete, standalone HTML document string.
    """
    now_utc = datetime.now(timezone.utc)
    report_date = now_utc.strftime("%B %d, %Y")
    report_timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Safely extract fields
    budget = plan_data.get("budget") or 0
    channels = plan_data.get("channels") or []
    roles = plan_data.get("roles") or []
    locations = plan_data.get("locations") or []
    market_intel = plan_data.get("market_intelligence", {})
    recommendations = plan_data.get("recommendations") or []

    # ── Build HTML sections ──

    # Executive Summary
    roles_display = ", ".join(_safe(r) for r in roles) if roles else "Not specified"
    locations_display = (
        ", ".join(_safe(loc) for loc in locations) if locations else "Not specified"
    )

    exec_summary_html = f"""
    <div class="section">
      <h2>Executive Summary</h2>
      <div class="summary-grid">
        <div class="summary-card">
          <div class="summary-label">Total Budget</div>
          <div class="summary-value">{_safe(_format_currency(budget))}</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">Industry</div>
          <div class="summary-value">{_safe(industry)}</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">Channels</div>
          <div class="summary-value">{len(channels)}</div>
        </div>
        <div class="summary-card">
          <div class="summary-label">Target Roles</div>
          <div class="summary-value-sm">{roles_display}</div>
        </div>
      </div>
      <div class="detail-row">
        <span class="detail-label">Target Locations:</span>
        <span class="detail-value">{locations_display}</span>
      </div>
    </div>
    """

    # Channel Allocation Table
    channel_rows = []
    for i, ch in enumerate(channels):
        bg = f' style="background-color: {BG_ZEBRA};"' if i % 2 == 1 else ""
        channel_rows.append(
            f"""
        <tr{bg}>
          <td style="font-weight: 600;">{_safe(ch.get('name', 'N/A'))}</td>
          <td class="num">{_format_pct(ch.get('allocation_pct') or 0)}</td>
          <td class="num">{_format_currency(ch.get('spend') or 0)}</td>
          <td class="num">{_format_currency(ch.get('cpc') or 0)}</td>
          <td class="num">{_format_currency(ch.get('cpa') or 0)}</td>
          <td class="num">{_format_number(ch.get('projected_clicks') or 0)}</td>
          <td class="num">{_format_number(ch.get('projected_applies') or 0)}</td>
          <td class="num">{_format_number(ch.get('projected_hires') or 0)}</td>
        </tr>"""
        )

    # Compute totals
    total_spend = sum(float(ch.get("spend") or 0 or 0) for ch in channels)
    total_clicks = sum(float(ch.get("projected_clicks") or 0 or 0) for ch in channels)
    total_applies = sum(float(ch.get("projected_applies") or 0 or 0) for ch in channels)
    total_hires = sum(float(ch.get("projected_hires") or 0 or 0) for ch in channels)

    channel_table_html = f"""
    <div class="section page-break-before">
      <h2>Channel Allocation</h2>
      <table class="data-table">
        <thead>
          <tr>
            <th>Channel</th>
            <th class="num">Allocation</th>
            <th class="num">Spend</th>
            <th class="num">CPC</th>
            <th class="num">CPA</th>
            <th class="num">Clicks</th>
            <th class="num">Applies</th>
            <th class="num">Hires</th>
          </tr>
        </thead>
        <tbody>
          {''.join(channel_rows)}
        </tbody>
        <tfoot>
          <tr>
            <td style="font-weight: 700;">Total</td>
            <td class="num">100%</td>
            <td class="num" style="font-weight: 700;">{_format_currency(total_spend)}</td>
            <td class="num">&mdash;</td>
            <td class="num">&mdash;</td>
            <td class="num" style="font-weight: 700;">{_format_number(total_clicks)}</td>
            <td class="num" style="font-weight: 700;">{_format_number(total_applies)}</td>
            <td class="num" style="font-weight: 700;">{_format_number(total_hires)}</td>
          </tr>
        </tfoot>
      </table>
    </div>
    """

    # Budget Breakdown (CSS horizontal bar chart)
    bar_items = []
    max_pct = (
        max((float(ch.get("allocation_pct") or 0 or 0) for ch in channels), default=1)
        or 1
    )
    for i, ch in enumerate(channels):
        pct = float(ch.get("allocation_pct") or 0 or 0)
        bar_width = (pct / max_pct) * 100  # Relative to largest bar
        color = BAR_COLORS[i % len(BAR_COLORS)]
        bar_items.append(
            f"""
        <div class="bar-row">
          <div class="bar-label">{_safe(ch.get('name', 'N/A'))}</div>
          <div class="bar-track">
            <div class="bar-fill" style="width: {bar_width:.1f}%; background-color: {color};"></div>
          </div>
          <div class="bar-value">{_format_pct(pct)} &middot; {_format_currency(ch.get('spend') or 0)}</div>
        </div>"""
        )

    budget_chart_html = f"""
    <div class="section">
      <h2>Budget Breakdown</h2>
      <div class="bar-chart">
        {''.join(bar_items)}
      </div>
    </div>
    """

    # Market Intelligence
    intel_items = []
    if isinstance(market_intel, dict):
        for key, value in market_intel.items():
            label = _safe(str(key).replace("_", " ").title())
            if isinstance(value, list):
                val_str = ", ".join(_safe(str(v)) for v in value)
            elif isinstance(value, dict):
                parts = [f"{_safe(str(k))}: {_safe(str(v))}" for k, v in value.items()]
                val_str = "; ".join(parts)
            else:
                val_str = _safe(str(value))
            intel_items.append(f"<li><strong>{label}:</strong> {val_str}</li>")

    market_intel_html = ""
    if intel_items:
        market_intel_html = f"""
        <div class="section page-break-before">
          <h2>Market Intelligence</h2>
          <ul class="intel-list">
            {''.join(intel_items)}
          </ul>
        </div>
        """

    # Recommendations
    rec_html = ""
    if recommendations:
        rec_items = "".join(f"<li>{_safe(str(r))}</li>" for r in recommendations)
        rec_html = f"""
        <div class="section">
          <h2>Recommendations</h2>
          <ol class="rec-list">
            {rec_items}
          </ol>
        </div>
        """

    # ── Assemble full HTML document ──
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Media Plan Report - {_safe(client_name)}</title>
<style>
  /* ── Reset & Base ── */
  *, *::before, *::after {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 14px;
    line-height: 1.6;
    color: {TEXT_DARK};
    background: {BG_WHITE};
    padding: 40px;
    max-width: 1000px;
    margin: 0 auto;
  }}

  /* ── Print Styles ── */
  @media print {{
    @page {{
      size: A4;
      margin: 15mm 12mm;
    }}
    body {{
      padding: 0;
      font-size: 11px;
      line-height: 1.5;
      max-width: none;
    }}
    .page-break-before {{
      page-break-before: always;
    }}
    .no-print {{
      display: none !important;
    }}
    .section {{
      page-break-inside: avoid;
    }}
    .data-table {{
      page-break-inside: auto;
    }}
    .data-table tr {{
      page-break-inside: avoid;
    }}
  }}

  /* ── Header ── */
  .report-header {{
    text-align: center;
    padding-bottom: 24px;
    margin-bottom: 32px;
    border-bottom: 3px solid {PORT_GORE};
  }}
  .report-logo {{
    width: 140px;
    height: auto;
    margin-bottom: 12px;
  }}
  .report-title {{
    font-size: 28px;
    font-weight: 800;
    color: {PORT_GORE};
    letter-spacing: -0.5px;
    margin-bottom: 4px;
  }}
  .report-client {{
    font-size: 18px;
    font-weight: 600;
    color: {BLUE_VIOLET};
    margin-bottom: 4px;
  }}
  .report-date {{
    font-size: 13px;
    color: {TEXT_MUTED};
  }}

  /* ── Sections ── */
  .section {{
    margin-bottom: 32px;
  }}
  .section h2 {{
    font-size: 18px;
    font-weight: 700;
    color: {PORT_GORE};
    margin-bottom: 16px;
    padding-bottom: 6px;
    border-bottom: 2px solid {BLUE_VIOLET};
  }}

  /* ── Summary Grid ── */
  .summary-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 16px;
  }}
  .summary-card {{
    background: {BG_ZEBRA};
    border: 1px solid {BORDER_LIGHT};
    border-radius: 8px;
    padding: 16px;
    text-align: center;
  }}
  .summary-label {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: {TEXT_MUTED};
    margin-bottom: 6px;
    font-weight: 600;
  }}
  .summary-value {{
    font-size: 20px;
    font-weight: 700;
    color: {PORT_GORE};
  }}
  .summary-value-sm {{
    font-size: 13px;
    font-weight: 600;
    color: {PORT_GORE};
    line-height: 1.4;
  }}
  .detail-row {{
    font-size: 13px;
    margin-top: 8px;
  }}
  .detail-label {{
    font-weight: 600;
    color: {TEXT_MUTED};
  }}
  .detail-value {{
    color: {TEXT_DARK};
  }}

  /* ── Data Table ── */
  .data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  .data-table th {{
    background: {PORT_GORE};
    color: #ffffff;
    font-weight: 600;
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: 0.8px;
    padding: 10px 12px;
    text-align: left;
    border: 1px solid {PORT_GORE};
  }}
  .data-table th.num {{
    text-align: right;
  }}
  .data-table td {{
    padding: 9px 12px;
    border: 1px solid {BORDER_LIGHT};
    vertical-align: middle;
  }}
  .data-table td.num {{
    text-align: right;
    font-variant-numeric: tabular-nums;
  }}
  .data-table tfoot td {{
    background: {BG_ZEBRA};
    border-top: 2px solid {PORT_GORE};
    font-weight: 600;
  }}

  /* ── Bar Chart ── */
  .bar-chart {{
    display: flex;
    flex-direction: column;
    gap: 10px;
  }}
  .bar-row {{
    display: flex;
    align-items: center;
    gap: 12px;
  }}
  .bar-label {{
    width: 150px;
    flex-shrink: 0;
    font-size: 13px;
    font-weight: 600;
    color: {TEXT_DARK};
    text-align: right;
  }}
  .bar-track {{
    flex: 1;
    height: 22px;
    background: {BG_ZEBRA};
    border-radius: 4px;
    overflow: hidden;
    border: 1px solid {BORDER_LIGHT};
  }}
  .bar-fill {{
    height: 100%;
    border-radius: 3px;
    transition: width 0.3s;
    min-width: 2px;
  }}
  .bar-value {{
    width: 160px;
    flex-shrink: 0;
    font-size: 12px;
    color: {TEXT_MUTED};
    font-variant-numeric: tabular-nums;
  }}

  /* ── Lists ── */
  .intel-list, .rec-list {{
    padding-left: 20px;
  }}
  .intel-list li, .rec-list li {{
    margin-bottom: 8px;
    font-size: 13px;
    line-height: 1.6;
    color: {TEXT_DARK};
  }}
  .intel-list li strong {{
    color: {PORT_GORE};
  }}

  /* ── Footer ── */
  .report-footer {{
    margin-top: 40px;
    padding-top: 16px;
    border-top: 2px solid {BORDER_LIGHT};
    text-align: center;
    font-size: 11px;
    color: {TEXT_MUTED};
  }}
  .report-footer .brand {{
    font-weight: 700;
    color: {BLUE_VIOLET};
  }}

  /* ── Print button (screen only) ── */
  .print-btn {{
    position: fixed;
    bottom: 24px;
    right: 24px;
    background: {BLUE_VIOLET};
    color: #fff;
    border: none;
    padding: 12px 24px;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    box-shadow: 0 4px 16px rgba(90, 84, 189, 0.3);
    z-index: 1000;
  }}
  .print-btn:hover {{
    background: {PORT_GORE};
  }}

  @media print {{
    .print-btn {{
      display: none !important;
    }}
    .summary-grid {{
      grid-template-columns: repeat(4, 1fr);
    }}
    .bar-label {{
      width: 120px;
    }}
    .bar-value {{
      width: 140px;
    }}
  }}

  /* ── Responsive ── */
  @media screen and (max-width: 768px) {{
    body {{ padding: 16px; }}
    .summary-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .bar-label {{ width: 100px; font-size: 11px; }}
    .bar-value {{ width: 120px; font-size: 11px; }}
  }}
</style>
</head>
<body>

  <!-- Print Button (hidden in print) -->
  <button class="print-btn no-print" onclick="window.print()">
    &#128424; Print / Save as PDF
  </button>

  <!-- Report Header -->
  <div class="report-header">
    <img src="/assets/nova-logo.png" alt="Nova AI Suite" class="report-logo"
         onerror="this.style.display='none'">
    <div class="report-title">Media Plan Report</div>
    <div class="report-client">{_safe(client_name)}</div>
    <div class="report-date">{_safe(report_date)}</div>
  </div>

  <!-- Executive Summary -->
  {exec_summary_html}

  <!-- Channel Allocation Table -->
  {channel_table_html}

  <!-- Budget Breakdown Chart -->
  {budget_chart_html}

  <!-- Market Intelligence -->
  {market_intel_html}

  <!-- Recommendations -->
  {rec_html}

  <!-- Footer -->
  <div class="report-footer">
    Generated by <span class="brand">Nova AI Suite</span>
    &nbsp;&middot;&nbsp; {_safe(report_timestamp)}
  </div>

</body>
</html>"""
