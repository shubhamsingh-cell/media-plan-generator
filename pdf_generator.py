"""
Print-optimized HTML report generator for AI Media Plan Generator.

Generates a standalone HTML document with @media print CSS for clean
A4 PDF output via Ctrl+P / browser print. No external dependencies --
all CSS is inline, all fonts are system fonts.

Color scheme: Brand (Port Gore navy #202058, Blue Violet #5A54BE)
adapted for print-friendly output (white background, dark text).
"""

from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Brand Colors (print-friendly adaptations)
# ---------------------------------------------------------------------------
PORT_GORE = "#202058"  # Navy -- headings
BLUE_VIOLET = "#5A54BE"  # Purple accent
DOWNY_TEAL = "#6BB5CE"  # Secondary accent
TAPESTRY_PINK = "#B7669E"  # Tertiary accent
RAW_SIENNA = "#3E8FAB"  # Teal-deep accent (deck)
TEXT_DARK = "#1F2937"  # Ink body text (deck-exact)
TEXT_MUTED = "#6E6E8C"  # Muted secondary text (deck-exact)
BORDER_LIGHT = "#E3E1F1"  # Table / card borders (deck-exact)
BG_ZEBRA = "#F4F4FF"  # Lavender-50 zebra row background (deck-exact)
BG_LAVENDER = "#ECEAF7"  # Lavender-100 alt surface (deck)
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


def _load_deck_kb() -> Dict[str, Any]:
    """Load the Joveo media-plan deck content (methodology, push/pull, CPA
    reference, sample pricing, why-Joveo, case study, next steps) from the KB.

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


def _deck_methodology_html(deck: Dict[str, Any]) -> str:
    """Joveo 6-step campaign methodology cards (deck slide 'Our Methodology')."""
    meth = (deck.get("campaign_methodology") or {}) if isinstance(deck, dict) else {}
    steps = meth.get("steps") or []
    if not steps:
        return ""
    cards = []
    for s in steps:
        ai = _safe(str(s.get("ai") or ""))
        ai_tag = (
            f'<div style="font-size:10px;font-weight:600;color:{BLUE_VIOLET};'
            f'text-transform:uppercase;letter-spacing:0.4px;margin-top:4px;">{ai}</div>'
            if ai
            else ""
        )
        cards.append(
            f"""
        <div style="background:{BG_ZEBRA};border:1px solid {BORDER_LIGHT};border-radius:10px;padding:14px;">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <span style="display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:50%;background:{BLUE_VIOLET};color:#fff;font-weight:700;font-size:12px;">{_safe(str(s.get('n') or ''))}</span>
            <span style="font-family:'Poppins',sans-serif;font-weight:600;font-size:13px;color:{PORT_GORE};">{_safe(str(s.get('name') or ''))}</span>
          </div>
          {ai_tag}
          <div style="font-size:11px;color:{TEXT_MUTED};line-height:1.5;margin-top:4px;">{_safe(str(s.get('detail') or ''))}</div>
        </div>"""
        )
    return f"""
    <div class="section page-break-before">
      <h2>Our Methodology</h2>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;">
        {''.join(cards)}
      </div>
    </div>
    """


def _deck_push_pull_html(deck: Dict[str, Any]) -> str:
    """Push Meets Pull framework (deck slide)."""
    pp = (deck.get("push_meets_pull") or {}) if isinstance(deck, dict) else {}
    push = pp.get("push") or {}
    pull = pp.get("pull") or {}
    if not push and not pull:
        return ""

    def _card(obj: Dict[str, Any], pill_bg: str, pill_color: str) -> str:
        return f"""
        <div style="background:{BG_ZEBRA};border:1px solid {BORDER_LIGHT};border-radius:10px;padding:16px;">
          <span style="display:inline-block;background:{pill_bg};color:{pill_color};font-weight:600;font-size:11px;padding:3px 12px;border-radius:999px;">{_safe(str(obj.get('name') or ''))}</span>
          <div style="font-size:12px;color:{TEXT_MUTED};line-height:1.6;margin-top:10px;">{_safe(str(obj.get('detail') or ''))}</div>
        </div>"""

    summary = _safe(str(pp.get("summary") or ""))
    summary_html = (
        f'<p style="font-size:13px;color:{TEXT_MUTED};margin-bottom:14px;">{summary}</p>'
        if summary
        else ""
    )
    return f"""
    <div class="section">
      <h2>Push Meets Pull</h2>
      {summary_html}
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;">
        {_card(push, BG_LAVENDER, BLUE_VIOLET)}
        {_card(pull, '#EEF6FF', RAW_SIENNA)}
      </div>
    </div>
    """


def _deck_cpa_reference_html(deck: Dict[str, Any]) -> str:
    """CPA reference guide table (deck slide)."""
    cpa = (deck.get("cpa_reference") or {}) if isinstance(deck, dict) else {}
    rows = cpa.get("roles") or []
    if not rows:
        return ""
    body = []
    for i, r in enumerate(rows):
        bg = f' style="background-color: {BG_ZEBRA};"' if i % 2 == 1 else ""
        body.append(
            f"""
        <tr{bg}>
          <td style="font-weight:600;">{_safe(str(r.get('category') or ''))}</td>
          <td class="num">${_safe(str(r.get('cpa_low') or ''))} &ndash; ${_safe(str(r.get('cpa_high') or ''))}</td>
          <td style="color:{TEXT_MUTED};">{_safe(str(r.get('basis') or ''))}</td>
        </tr>"""
        )
    note = _safe(str(cpa.get("description") or ""))
    note_html = (
        f'<p style="font-size:11px;color:{TEXT_MUTED};margin-top:8px;">{note}</p>'
        if note
        else ""
    )
    return f"""
    <div class="section page-break-before">
      <h2>CPA Reference Guide</h2>
      <table class="data-table">
        <thead>
          <tr><th>Role Category</th><th class="num">Est. CPA Range</th><th>Benchmark Basis</th></tr>
        </thead>
        <tbody>{''.join(body)}</tbody>
      </table>
      {note_html}
    </div>
    """


def _deck_sample_pricing_html(deck: Dict[str, Any]) -> str:
    """Sample campaign pricing table (deck slide)."""
    sp = (deck.get("sample_pricing_model") or {}) if isinstance(deck, dict) else {}
    camps = sp.get("campaigns") or []
    if not camps:
        return ""
    body = []
    for i, c in enumerate(camps):
        bg = f' style="background-color: {BG_ZEBRA};"' if i % 2 == 1 else ""
        body.append(
            f"""
        <tr{bg}>
          <td style="font-weight:600;">{_safe(str(c.get('campaign') or ''))}</td>
          <td style="color:{TEXT_MUTED};font-size:11px;">{_safe(str(c.get('targeting') or ''))}</td>
          <td class="num">${_safe(str(c.get('cpa_low') or ''))}&ndash;${_safe(str(c.get('cpa_high') or ''))}</td>
          <td class="num">{_safe(str(c.get('historical_conversion_pct') or ''))}%</td>
          <td class="num">${_safe(str(c.get('cost_per_trainer_low') or ''))}&ndash;${_safe(str(c.get('cost_per_trainer_high') or ''))}</td>
          <td class="num">{_format_currency(c.get('budget_low') or 0)}&ndash;{_format_currency(c.get('budget_high') or 0)}</td>
        </tr>"""
        )
    blended = sp.get("blended_total") or {}
    foot = ""
    if blended:
        foot = f"""
        <tfoot><tr>
          <td style="font-weight:700;">Blended Total</td>
          <td></td><td></td><td></td>
          <td class="num" style="font-weight:700;">${_safe(str(blended.get('cost_per_trainer_low') or ''))}&ndash;${_safe(str(blended.get('cost_per_trainer_high') or ''))}</td>
          <td class="num" style="font-weight:700;">{_format_currency(blended.get('budget_low') or 0)}&ndash;{_format_currency(blended.get('budget_high') or 0)}</td>
        </tr></tfoot>"""
    note = _safe(str(sp.get("note") or ""))
    note_html = (
        f'<p style="font-size:11px;color:{TEXT_MUTED};margin-top:8px;">{note}</p>'
        if note
        else ""
    )
    return f"""
    <div class="section">
      <h2>Sample Campaign Pricing</h2>
      <table class="data-table">
        <thead>
          <tr><th>Campaign</th><th>Targeting</th><th class="num">CPA</th><th class="num">Conv.</th><th class="num">Cost / Trainer</th><th class="num">Budget</th></tr>
        </thead>
        <tbody>{''.join(body)}</tbody>
        {foot}
      </table>
      {note_html}
    </div>
    """


def _deck_case_study_html(deck: Dict[str, Any]) -> str:
    """Case study: outcome stats + challenges/solution (deck slides)."""
    cs = (deck.get("case_study") or {}) if isinstance(deck, dict) else {}
    if not cs:
        return ""
    results = cs.get("results") or []
    stat_cards = "".join(
        f"""
        <div class="summary-card">
          <div class="summary-value" style="color:{BLUE_VIOLET};">{_safe(str(r.get('value') or ''))}</div>
          <div class="summary-label" style="margin-top:6px;">{_safe(str(r.get('detail') or r.get('metric') or ''))}</div>
        </div>"""
        for r in results
    )
    stats_html = (
        f'<div class="summary-grid" style="grid-template-columns:repeat({max(len(results),1)},1fr);margin-bottom:16px;">{stat_cards}</div>'
        if results
        else ""
    )

    def _list(items, color):
        lis = "".join(
            f'<li style="margin-bottom:6px;font-size:12px;color:{TEXT_MUTED};line-height:1.5;">{_safe(str(x))}</li>'
            for x in items
        )
        return f'<ul style="list-style:none;padding:0;">{lis}</ul>'

    challenges = cs.get("challenges") or []
    solution = cs.get("solution") or []
    cols = f"""
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
        <div>
          <div style="font-family:'Poppins',sans-serif;font-weight:600;font-size:13px;color:{PORT_GORE};margin-bottom:8px;">Challenges</div>
          {_list(challenges, TEXT_MUTED)}
        </div>
        <div>
          <div style="font-family:'Poppins',sans-serif;font-weight:600;font-size:13px;color:{BLUE_VIOLET};margin-bottom:8px;">The Joveo Solution</div>
          {_list(solution, TEXT_MUTED)}
        </div>
      </div>
    """
    title = _safe(str(cs.get("title") or "Case Study"))
    return f"""
    <div class="section page-break-before">
      <h2>Case Study &mdash; {title}</h2>
      {stats_html}
      {cols}
    </div>
    """


def _deck_next_steps_html(deck: Dict[str, Any]) -> str:
    """Next steps checklist (deck slide)."""
    steps = (deck.get("next_steps") or []) if isinstance(deck, dict) else []
    if not steps:
        return ""
    items = "".join(f"<li>{_safe(str(s))}</li>" for s in steps)
    return f"""
    <div class="section">
      <h2>Next Steps</h2>
      <ol class="rec-list">{items}</ol>
    </div>
    """


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
      <h2>Channel Mix</h2>
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
          <h2>Benchmarking &amp; Market Research</h2>
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

    # ── Deck narrative sections (Joveo media-plan deck KB) ──
    _deck = _load_deck_kb()
    methodology_html = _deck_methodology_html(_deck)
    push_pull_html = _deck_push_pull_html(_deck)
    cpa_reference_html = _deck_cpa_reference_html(_deck)
    sample_pricing_html = _deck_sample_pricing_html(_deck)
    case_study_html = _deck_case_study_html(_deck)
    next_steps_html = _deck_next_steps_html(_deck)

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
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
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
    font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
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
    font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
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

  <!-- Our Methodology (deck) -->
  {methodology_html}

  <!-- Push Meets Pull (deck) -->
  {push_pull_html}

  <!-- Benchmarking & Market Research -->
  {market_intel_html}

  <!-- Channel Mix -->
  {channel_table_html}

  <!-- Budget Breakdown Chart -->
  {budget_chart_html}

  <!-- CPA Reference Guide (deck) -->
  {cpa_reference_html}

  <!-- Sample Campaign Pricing (deck) -->
  {sample_pricing_html}

  <!-- Case Study (deck) -->
  {case_study_html}

  <!-- Recommendations -->
  {rec_html}

  <!-- Next Steps (deck) -->
  {next_steps_html}

  <!-- Footer -->
  <div class="report-footer">
    Generated by <span class="brand">Nova AI Suite</span>
    &nbsp;&middot;&nbsp; {_safe(report_timestamp)}
  </div>

</body>
</html>"""
