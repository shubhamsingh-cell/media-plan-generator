"""Shareable Plan Scorecard Generator.

Generates beautiful one-page HTML scorecards for media plans with dark theme,
brand colors, OG meta tags for LinkedIn sharing, and responsive layout.
All CSS is inline -- the output is a single self-contained HTML page.
"""

import hashlib
import html
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Brand colors
PORT_GORE = "#202058"
BLUE_VIOLET = "#5A54BD"
DOWNY_TEAL = "#6BB3CD"


def generate_share_id(plan_data: dict[str, Any]) -> str:
    """Generate a deterministic share ID from plan data using SHA-256.

    Args:
        plan_data: The media plan data dictionary.

    Returns:
        First 12 characters of the SHA-256 hex digest.
    """
    serialized = json.dumps(plan_data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:12]


def _safe(value: Any, default: str = "--") -> str:
    """Safely convert a value to an HTML-escaped string."""
    text = str(value) if value else default
    return html.escape(text)


def _format_budget(budget: Any) -> str:
    """Format a budget value as a human-readable dollar string."""
    if not budget:
        return "--"
    if isinstance(budget, str):
        # Already formatted (e.g. "$50,000")
        return html.escape(budget)
    try:
        amount = float(budget)
        if amount >= 1_000_000:
            return f"${amount / 1_000_000:,.1f}M"
        if amount >= 1_000:
            return f"${amount:,.0f}"
        return f"${amount:,.2f}"
    except (ValueError, TypeError):
        return html.escape(str(budget))


def _extract_channels(plan_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract channel allocation data from various plan data formats."""
    channels: list[dict[str, Any]] = []

    # Try budget_allocation.channel_allocations first
    budget_alloc = (
        plan_data.get("_budget_allocation") or plan_data.get("budget_allocation") or {}
    )
    ch_allocs = budget_alloc.get("channel_allocations") or {}

    if ch_allocs and isinstance(ch_allocs, dict):
        meta = budget_alloc.get("metadata") or {}
        total_budget = float(meta.get("total_budget") or 0)
        for ch_name, ch_data in ch_allocs.items():
            if not isinstance(ch_data, dict):
                continue
            dollar_amt = float(ch_data.get("dollar_amount") or 0)
            pct = float(ch_data.get("percentage") or ch_data.get("pct") or 0)
            if pct == 0 and total_budget > 0 and dollar_amt > 0:
                pct = round(dollar_amt / total_budget * 100, 1)
            channels.append(
                {
                    "name": ch_name,
                    "percentage": pct,
                    "dollar_amount": dollar_amt,
                }
            )
        return sorted(channels, key=lambda c: c["percentage"], reverse=True)

    # Fallback: try summary.channels or plan_data.channels
    summary = plan_data.get("summary") or plan_data.get("plan_summary") or {}
    ch_list = (
        summary.get("channels")
        or summary.get("recommended_channels")
        or plan_data.get("channels")
        or []
    )

    if isinstance(ch_list, list):
        for ch in ch_list:
            if isinstance(ch, dict):
                name = ch.get("name") or ch.get("channel") or "Unknown"
                pct = float(
                    ch.get("percentage") or ch.get("pct") or ch.get("allocation") or 0
                )
                dollar_amt = float(ch.get("dollar_amount") or ch.get("budget") or 0)
                channels.append(
                    {
                        "name": name,
                        "percentage": pct,
                        "dollar_amount": dollar_amt,
                    }
                )
            elif isinstance(ch, str):
                channels.append({"name": ch, "percentage": 0, "dollar_amount": 0})

    return sorted(channels, key=lambda c: c["percentage"], reverse=True)


def _extract_total_budget(plan_data: dict[str, Any]) -> str:
    """Extract and format the total budget from plan data."""
    budget_alloc = (
        plan_data.get("_budget_allocation") or plan_data.get("budget_allocation") or {}
    )
    meta = budget_alloc.get("metadata") or {}
    total = meta.get("total_budget")
    if total:
        return _format_budget(total)

    summary = plan_data.get("summary") or plan_data.get("plan_summary") or {}
    budget = (
        summary.get("total_budget")
        or summary.get("budget_range")
        or plan_data.get("budget_range")
        or plan_data.get("total_budget")
        or plan_data.get("budget")
    )
    return _format_budget(budget)


def _extract_job_info(plan_data: dict[str, Any]) -> tuple[str, str]:
    """Extract job title and location from plan data.

    Returns:
        Tuple of (job_title, location).
    """
    roles = plan_data.get("target_roles") or plan_data.get("roles") or []
    if isinstance(roles, list) and roles:
        job_title = (
            roles[0] if isinstance(roles[0], str) else (roles[0].get("title") or "")
        )
    elif isinstance(roles, str):
        job_title = roles
    else:
        job_title = plan_data.get("job_title") or plan_data.get("title") or ""

    locations = plan_data.get("locations") or []
    if isinstance(locations, list) and locations:
        location = locations[0] if isinstance(locations[0], str) else str(locations[0])
    elif isinstance(locations, str):
        location = locations
    else:
        location = plan_data.get("location") or ""

    return (job_title or "Media Plan", location or "Global")


def generate_scorecard_html(plan_data: dict[str, Any], share_id: str) -> str:
    """Generate a complete, self-contained HTML scorecard page.

    Args:
        plan_data: The media plan data dictionary.
        share_id: The unique share identifier for the scorecard.

    Returns:
        Complete HTML page as a string with inline CSS, OG meta tags,
        responsive layout, and dark theme using brand colors.
    """
    job_title, location = _extract_job_info(plan_data)
    total_budget = _extract_total_budget(plan_data)
    channels = _extract_channels(plan_data)
    num_channels = len(channels)
    industry = _safe(
        plan_data.get("industry_label")
        or plan_data.get("industry")
        or (plan_data.get("summary") or {}).get("industry")
        or ""
    )

    # Build channel bars HTML
    channel_bars_html = ""
    for ch in channels[:10]:  # Cap at 10 channels
        name = _safe(ch["name"])
        pct = ch["percentage"]
        dollar = _format_budget(ch["dollar_amount"]) if ch["dollar_amount"] else ""
        bar_width = max(pct, 3)  # Minimum 3% width for visibility
        channel_bars_html += f"""
        <div style="margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
            <span style="font-size:14px;font-weight:500;color:#e2e8f0;">{name}</span>
            <span style="font-size:13px;color:#94a3b8;">{pct:.0f}%{f' ({dollar})' if dollar else ''}</span>
          </div>
          <div style="background:rgba(255,255,255,0.06);border-radius:6px;height:10px;overflow:hidden;">
            <div style="width:{bar_width}%;height:100%;border-radius:6px;background:linear-gradient(90deg,{BLUE_VIOLET},{DOWNY_TEAL});transition:width 0.6s ease;"></div>
          </div>
        </div>"""

    if not channel_bars_html:
        channel_bars_html = '<p style="color:#64748b;font-size:14px;text-align:center;padding:20px 0;">No channel data available</p>'

    # OG meta description
    og_description = (
        f"AI-optimized media plan for {_safe(job_title)} in {_safe(location)}"
    )
    if total_budget != "--":
        og_description += f" | Budget: {total_budget}"
    og_description += f" | {num_channels} channels"

    base_url = "https://media-plan-generator.onrender.com"
    scorecard_url = f"{base_url}/scorecard/{share_id}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Media Plan Scorecard - {_safe(job_title)} | Nova AI Suite</title>
<meta name="description" content="{og_description}">
<meta property="og:type" content="website">
<meta property="og:title" content="Media Plan Scorecard - {_safe(job_title)}">
<meta property="og:description" content="{og_description}">
<meta property="og:url" content="{scorecard_url}">
<meta property="og:site_name" content="Nova AI Suite">
<meta property="og:image" content="{base_url}/static/og-scorecard.png">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="Media Plan Scorecard - {_safe(job_title)}">
<meta name="twitter:description" content="{og_description}">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
  body{{
    font-family:'Inter',system-ui,-apple-system,sans-serif;
    background:{PORT_GORE};
    background-image:radial-gradient(ellipse at 20% 50%,rgba(90,84,189,0.15) 0%,transparent 60%),
                     radial-gradient(ellipse at 80% 20%,rgba(107,179,205,0.10) 0%,transparent 50%);
    color:#e2e8f0;
    min-height:100vh;
    display:flex;
    flex-direction:column;
    align-items:center;
    padding:24px 16px 48px;
  }}
  .card{{
    background:rgba(255,255,255,0.04);
    border:1px solid rgba(255,255,255,0.08);
    border-radius:16px;
    padding:24px;
    backdrop-filter:blur(12px);
    -webkit-backdrop-filter:blur(12px);
  }}
  .cta-btn{{
    display:inline-flex;
    align-items:center;
    gap:8px;
    padding:12px 28px;
    background:linear-gradient(135deg,{BLUE_VIOLET},{DOWNY_TEAL});
    color:#fff;
    text-decoration:none;
    border-radius:10px;
    font-weight:600;
    font-size:15px;
    transition:transform 0.2s,box-shadow 0.2s;
    box-shadow:0 4px 20px rgba(90,84,189,0.3);
  }}
  .cta-btn:hover{{transform:translateY(-2px);box-shadow:0 6px 28px rgba(90,84,189,0.45);}}
  @media(max-width:640px){{
    body{{padding:16px 12px 32px;}}
    .card{{padding:16px;border-radius:12px;}}
    .stat-grid{{flex-direction:column!important;gap:12px!important;}}
  }}
  @media(prefers-reduced-motion:reduce){{
    .cta-btn{{transition:none;}}
  }}
</style>
</head>
<body>

<!-- Header -->
<div style="width:100%;max-width:640px;margin-bottom:24px;">
  <div style="display:flex;align-items:center;justify-content:space-between;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div style="width:40px;height:40px;background:linear-gradient(135deg,{BLUE_VIOLET},{DOWNY_TEAL});border-radius:10px;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:20px;color:#fff;flex-shrink:0;">N</div>
      <span style="font-size:18px;font-weight:600;color:#e2e8f0;">Nova AI Suite</span>
    </div>
    <div style="display:flex;align-items:center;gap:6px;padding:5px 12px;background:rgba(90,84,189,0.2);border:1px solid rgba(90,84,189,0.3);border-radius:20px;">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="{DOWNY_TEAL}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
      <span style="font-size:12px;font-weight:600;color:{DOWNY_TEAL};letter-spacing:0.5px;">AI Generated</span>
    </div>
  </div>
</div>

<!-- Main Card -->
<div class="card" style="width:100%;max-width:640px;margin-bottom:20px;">

  <!-- Title Section -->
  <div style="margin-bottom:24px;">
    <h1 style="font-size:22px;font-weight:700;color:#fff;margin-bottom:6px;line-height:1.3;">{_safe(job_title)}</h1>
    <div style="display:flex;align-items:center;gap:6px;color:#94a3b8;font-size:14px;">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>
      <span>{_safe(location)}</span>
      {f'<span style="margin:0 4px;opacity:0.4;">|</span><span>{industry}</span>' if industry and industry != "--" else ""}
    </div>
  </div>

  <!-- Stat Cards -->
  <div class="stat-grid" style="display:flex;gap:16px;margin-bottom:28px;">
    <div style="flex:1;background:rgba(90,84,189,0.12);border:1px solid rgba(90,84,189,0.2);border-radius:12px;padding:16px;text-align:center;">
      <div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">Total Budget</div>
      <div style="font-size:24px;font-weight:700;color:#fff;">{total_budget}</div>
    </div>
    <div style="flex:1;background:rgba(107,179,205,0.10);border:1px solid rgba(107,179,205,0.18);border-radius:12px;padding:16px;text-align:center;">
      <div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">Channels</div>
      <div style="font-size:24px;font-weight:700;color:#fff;">{num_channels}</div>
    </div>
    <div style="flex:1;background:rgba(90,84,189,0.08);border:1px solid rgba(90,84,189,0.15);border-radius:12px;padding:16px;text-align:center;">
      <div style="font-size:12px;font-weight:500;color:#94a3b8;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">Optimization</div>
      <div style="font-size:18px;font-weight:700;color:{DOWNY_TEAL};">AI Optimized</div>
    </div>
  </div>

  <!-- Channel Allocation -->
  <div style="margin-bottom:8px;">
    <h2 style="font-size:15px;font-weight:600;color:#e2e8f0;margin-bottom:16px;">Channel Allocation</h2>
    {channel_bars_html}
  </div>

</div>

<!-- Footer CTA -->
<div style="width:100%;max-width:640px;text-align:center;">
  <div class="card" style="padding:28px 24px;">
    <p style="font-size:14px;color:#94a3b8;margin-bottom:16px;">Powered by Nova AI Suite</p>
    <a href="/media-plan" class="cta-btn">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
      Create Your Plan
    </a>
    <p style="font-size:11px;color:#475569;margin-top:14px;">
      <a href="https://www.linkedin.com/in/chandel13/" target="_blank" rel="noopener" style="color:#64748b;text-decoration:none;">Built by Shubham Singh Chandel</a>
    </p>
  </div>
</div>

</body>
</html>"""
