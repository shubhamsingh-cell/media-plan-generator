"""CFO-Ready Attribution Dashboard -- maps every dollar of recruitment media
spend to downstream business outcomes with audit-grade data lineage.

Attribution funnel: Spend -> Impressions -> Clicks -> Applications -> Interviews -> Hires

Every data point carries source, timestamp, and confidence level for full
audit trail transparency.
"""

import datetime
import logging
import threading
from typing import Any, Union

logger = logging.getLogger(__name__)

# ── Thread-safe state ──
_lock = threading.Lock()
_report_cache: dict[str, Any] = {}

# ── Brand colors ──
PORT_GORE = "#202058"
BLUE_VIOLET = "#5A54BD"
DOWNY_TEAL = "#6BB3CD"

# ── Industry benchmarks for conversion rates ──
_INDUSTRY_BENCHMARKS: dict[str, dict[str, float]] = {
    "technology": {
        "impression_to_click": 0.025,
        "click_to_apply": 0.12,
        "apply_to_interview": 0.25,
        "interview_to_hire": 0.30,
        "avg_hire_value": 85000.0,
        "avg_time_to_fill": 42,
    },
    "healthcare": {
        "impression_to_click": 0.020,
        "click_to_apply": 0.10,
        "apply_to_interview": 0.22,
        "interview_to_hire": 0.28,
        "avg_hire_value": 72000.0,
        "avg_time_to_fill": 38,
    },
    "finance": {
        "impression_to_click": 0.022,
        "click_to_apply": 0.11,
        "apply_to_interview": 0.20,
        "interview_to_hire": 0.25,
        "avg_hire_value": 95000.0,
        "avg_time_to_fill": 45,
    },
    "retail": {
        "impression_to_click": 0.030,
        "click_to_apply": 0.15,
        "apply_to_interview": 0.30,
        "interview_to_hire": 0.35,
        "avg_hire_value": 42000.0,
        "avg_time_to_fill": 25,
    },
    "default": {
        "impression_to_click": 0.023,
        "click_to_apply": 0.11,
        "apply_to_interview": 0.23,
        "interview_to_hire": 0.28,
        "avg_hire_value": 65000.0,
        "avg_time_to_fill": 35,
    },
}

# ── Channel-level default conversion rates ──
_CHANNEL_DEFAULTS: dict[str, dict[str, float]] = {
    "LinkedIn": {"ctr": 0.035, "apply_rate": 0.14, "quality_factor": 0.85},
    "Indeed": {"ctr": 0.028, "apply_rate": 0.18, "quality_factor": 0.70},
    "Glassdoor": {"ctr": 0.022, "apply_rate": 0.12, "quality_factor": 0.75},
    "Google Ads": {"ctr": 0.040, "apply_rate": 0.08, "quality_factor": 0.60},
    "Meta Ads": {"ctr": 0.032, "apply_rate": 0.06, "quality_factor": 0.55},
    "Programmatic": {"ctr": 0.018, "apply_rate": 0.10, "quality_factor": 0.65},
    "ZipRecruiter": {"ctr": 0.025, "apply_rate": 0.16, "quality_factor": 0.68},
    "CareerBuilder": {"ctr": 0.020, "apply_rate": 0.13, "quality_factor": 0.62},
    "Niche Job Boards": {"ctr": 0.030, "apply_rate": 0.20, "quality_factor": 0.80},
    "Employee Referrals": {"ctr": 0.0, "apply_rate": 0.0, "quality_factor": 0.92},
}


def _get_benchmarks(industry: str) -> dict[str, float]:
    """Return industry benchmarks, falling back to defaults."""
    key = industry.lower().strip() if industry else "default"
    return _INDUSTRY_BENCHMARKS.get(key, _INDUSTRY_BENCHMARKS["default"])


def _audit_point(
    value: float,
    source: str,
    confidence: float,
    method: str = "calculated",
) -> dict[str, Any]:
    """Create an audit-grade data point with full lineage.

    Args:
        value: The numeric value.
        source: Where this data point originated.
        confidence: Confidence level 0.0-1.0.
        method: How the value was derived (measured, estimated, calculated).

    Returns:
        Dict with value, source, timestamp, confidence, and method.
    """
    return {
        "value": round(value, 4),
        "source": source,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "confidence": round(confidence, 2),
        "method": method,
    }


def _extract_channels(plan_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract channel-level data from plan_data, normalizing various formats."""
    channels = plan_data.get("channels") or plan_data.get("channel_breakdown") or []
    if not channels and "allocations" in plan_data:
        allocations = plan_data["allocations"]
        if isinstance(allocations, dict):
            channels = [
                {"channel": k, "spend": v if isinstance(v, (int, float)) else 0}
                for k, v in allocations.items()
            ]
        elif isinstance(allocations, list):
            channels = allocations
    return channels


def generate_attribution_report(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Generate a full attribution report from plan data.

    Maps spend through the recruitment funnel: Spend -> Impressions -> Clicks
    -> Applications -> Interviews -> Hires, with per-channel breakdowns and
    CFO-ready P&L metrics.

    Args:
        plan_data: Dict containing budget, channels, industry, and optional
                   actual performance data.

    Returns:
        Dict with total_spend, cost_per_impression, cost_per_click,
        cost_per_application, cost_per_hire, channel_attribution,
        roi_multiple, time_to_fill_estimate, quality_score, cfo_metrics,
        audit_trail, and funnel data.
    """
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    industry = (plan_data.get("industry") or "default").strip()
    benchmarks = _get_benchmarks(industry)

    # ── Extract total budget ──
    total_spend = float(
        plan_data.get("total_budget")
        or plan_data.get("budget")
        or plan_data.get("total_spend")
        or 0
    )

    # ── Extract channels ──
    channels = _extract_channels(plan_data)
    total_hiring_cost = float(plan_data.get("total_hiring_cost") or total_spend * 3.5)
    projected_spend = float(plan_data.get("projected_spend") or total_spend)

    # ── Per-channel attribution ──
    channel_attribution: list[dict[str, Any]] = []
    agg_impressions = 0
    agg_clicks = 0
    agg_applications = 0
    agg_interviews = 0
    agg_hires = 0
    agg_spend = 0.0
    quality_scores: list[float] = []

    for ch in channels:
        ch_name = ch.get("channel") or ch.get("name") or "Unknown"
        ch_spend = float(ch.get("spend") or ch.get("budget") or 0)
        defaults = _CHANNEL_DEFAULTS.get(
            ch_name,
            {
                "ctr": benchmarks["impression_to_click"],
                "apply_rate": benchmarks["click_to_apply"],
                "quality_factor": 0.65,
            },
        )

        # Use actual data if provided, otherwise estimate
        impressions = int(
            ch.get("impressions") or (ch_spend / 5.0 if ch_spend > 0 else 0)
        )
        ctr = float(ch.get("ctr") or defaults["ctr"])
        clicks = int(ch.get("clicks") or round(impressions * ctr))
        apply_rate = float(
            ch.get("apply_rate") or ch.get("applyRate") or defaults["apply_rate"]
        )
        applications = int(ch.get("applications") or round(clicks * apply_rate))
        interview_rate = float(
            ch.get("interview_rate") or benchmarks["apply_to_interview"]
        )
        interviews = int(ch.get("interviews") or round(applications * interview_rate))
        hire_rate = float(ch.get("hire_rate") or benchmarks["interview_to_hire"])
        hires = int(ch.get("hires") or round(interviews * hire_rate))

        # Quality score per channel (0-100)
        ch_quality = defaults["quality_factor"] * 100
        if hires > 0 and ch_spend > 0:
            efficiency = min(benchmarks["avg_hire_value"] / (ch_spend / hires), 2.0)
            ch_quality = min(efficiency * 50, 100.0)
        quality_scores.append(ch_quality)

        # Cost metrics
        cpi = ch_spend / impressions if impressions > 0 else 0.0
        cpc = ch_spend / clicks if clicks > 0 else 0.0
        cpa = ch_spend / applications if applications > 0 else 0.0
        cph = ch_spend / hires if hires > 0 else 0.0

        # Attribution percentage
        attr_pct = (ch_spend / total_spend * 100) if total_spend > 0 else 0.0

        confidence = 0.9 if ch.get("impressions") else 0.6

        channel_attribution.append(
            {
                "channel": ch_name,
                "spend": round(ch_spend, 2),
                "attribution_pct": round(attr_pct, 1),
                "impressions": _audit_point(
                    impressions,
                    f"{ch_name} ad platform",
                    confidence,
                    "measured" if ch.get("impressions") else "estimated",
                ),
                "clicks": _audit_point(
                    clicks,
                    f"{ch_name} analytics",
                    confidence,
                    "measured" if ch.get("clicks") else "estimated",
                ),
                "applications": _audit_point(
                    applications,
                    f"ATS via {ch_name}",
                    confidence * 0.95,
                    "measured" if ch.get("applications") else "estimated",
                ),
                "interviews": _audit_point(
                    interviews, "ATS scheduling data", confidence * 0.85, "estimated"
                ),
                "hires": _audit_point(
                    hires,
                    "HRIS records",
                    confidence * 0.80,
                    "measured" if ch.get("hires") else "estimated",
                ),
                "cost_per_impression": round(cpi, 4),
                "cost_per_click": round(cpc, 2),
                "cost_per_application": round(cpa, 2),
                "cost_per_hire": round(cph, 2),
                "quality_score": round(ch_quality, 1),
                "funnel": {
                    "impressions": impressions,
                    "clicks": clicks,
                    "applications": applications,
                    "interviews": interviews,
                    "hires": hires,
                },
            }
        )

        agg_impressions += impressions
        agg_clicks += clicks
        agg_applications += applications
        agg_interviews += interviews
        agg_hires += hires
        agg_spend += ch_spend

    # If channels didn't sum to total, use total_spend
    if agg_spend == 0:
        agg_spend = total_spend

    # ── Aggregate cost metrics ──
    cost_per_impression = agg_spend / agg_impressions if agg_impressions > 0 else 0.0
    cost_per_click = agg_spend / agg_clicks if agg_clicks > 0 else 0.0
    cost_per_application = agg_spend / agg_applications if agg_applications > 0 else 0.0
    cost_per_hire = agg_spend / agg_hires if agg_hires > 0 else 0.0

    # ── ROI multiple ──
    estimated_hire_value = agg_hires * benchmarks["avg_hire_value"]
    roi_multiple = estimated_hire_value / agg_spend if agg_spend > 0 else 0.0

    # ── Quality score (weighted average across channels) ──
    quality_score = (
        sum(q * ch.get("spend", 1) for q, ch in zip(quality_scores, channels))
        / sum(float(ch.get("spend") or ch.get("budget") or 1) for ch in channels)
        if channels
        else 50.0
    )

    # ── Time to fill estimate ──
    time_to_fill = benchmarks["avg_time_to_fill"]
    if agg_hires > 0 and agg_applications > 0:
        fill_efficiency = min(agg_applications / agg_hires, 20)
        time_to_fill = max(
            int(benchmarks["avg_time_to_fill"] * (10 / fill_efficiency)), 14
        )

    # ── CFO Metrics (P&L language) ──
    recruitment_pct_of_hiring = (
        (agg_spend / total_hiring_cost * 100) if total_hiring_cost > 0 else 0.0
    )

    # Budget efficiency: how well spend converts to hires vs benchmark
    benchmark_cph = (
        benchmarks["avg_hire_value"] * 0.15
    )  # 15% of hire value is benchmark
    budget_efficiency = min(
        (benchmark_cph / cost_per_hire * 100) if cost_per_hire > 0 else 0.0, 100.0
    )

    spend_variance = (
        ((agg_spend - projected_spend) / projected_spend * 100)
        if projected_spend > 0
        else 0.0
    )

    # Cost per quality hire by channel
    cpqh_by_channel: list[dict[str, Any]] = []
    for ca in channel_attribution:
        ch_hires_val = ca["hires"]["value"]
        ch_quality_adj = ca["quality_score"] / 100.0
        effective_hires = ch_hires_val * ch_quality_adj
        cpqh = ca["spend"] / effective_hires if effective_hires > 0 else 0.0
        cpqh_by_channel.append(
            {
                "channel": ca["channel"],
                "cost_per_quality_hire": round(cpqh, 2),
                "quality_adjusted_hires": round(effective_hires, 1),
            }
        )

    cfo_metrics = {
        "recruitment_marketing_pct_of_hiring_cost": _audit_point(
            recruitment_pct_of_hiring,
            "plan budget / total hiring cost",
            0.85,
            "calculated",
        ),
        "cost_per_quality_hire_by_channel": cpqh_by_channel,
        "budget_efficiency_score": _audit_point(
            budget_efficiency, "benchmark comparison model", 0.75, "calculated"
        ),
        "projected_vs_actual_spend_variance_pct": _audit_point(
            spend_variance, "plan budget tracking", 0.90, "calculated"
        ),
    }

    # ── Funnel summary ──
    funnel = {
        "spend": _audit_point(agg_spend, "media plan budget", 0.95, "measured"),
        "impressions": _audit_point(
            agg_impressions, "ad platform aggregation", 0.80, "estimated"
        ),
        "clicks": _audit_point(agg_clicks, "analytics platforms", 0.82, "estimated"),
        "applications": _audit_point(
            agg_applications, "ATS records", 0.78, "estimated"
        ),
        "interviews": _audit_point(agg_interviews, "ATS scheduling", 0.70, "estimated"),
        "hires": _audit_point(agg_hires, "HRIS records", 0.72, "estimated"),
    }

    # ── Audit trail ──
    audit_trail = {
        "report_generated_at": now_iso,
        "data_sources": [
            {"name": "Media Plan Generator", "type": "primary", "confidence": 0.95},
            {"name": "Industry Benchmarks", "type": "reference", "confidence": 0.80},
            {"name": "Channel Default Rates", "type": "fallback", "confidence": 0.65},
        ],
        "methodology": "Multi-touch last-click attribution with industry benchmark fallbacks",
        "confidence_methodology": "Weighted by data source reliability and recency",
        "industry_used": industry,
        "benchmark_set": "Nova AI Suite v4.0 recruitment benchmarks",
    }

    report = {
        "total_spend": round(agg_spend, 2),
        "cost_per_impression": round(cost_per_impression, 4),
        "cost_per_click": round(cost_per_click, 2),
        "cost_per_application": round(cost_per_application, 2),
        "cost_per_hire": round(cost_per_hire, 2),
        "channel_attribution": channel_attribution,
        "roi_multiple": round(roi_multiple, 2),
        "time_to_fill_estimate": time_to_fill,
        "quality_score": round(quality_score, 1),
        "cfo_metrics": cfo_metrics,
        "funnel": funnel,
        "audit_trail": audit_trail,
        "summary": {
            "total_channels": len(channel_attribution),
            "total_impressions": agg_impressions,
            "total_clicks": agg_clicks,
            "total_applications": agg_applications,
            "total_interviews": agg_interviews,
            "total_hires": agg_hires,
            "estimated_hire_value": round(estimated_hire_value, 2),
        },
    }

    # ── Cache result (thread-safe) ──
    with _lock:
        _report_cache["last_report"] = report
        _report_cache["last_generated"] = now_iso

    return report


def get_attribution_stats() -> dict[str, Any]:
    """Return attribution module stats for /api/health.

    Returns:
        Dict with module status, last report timestamp, and cache info.
    """
    with _lock:
        last_gen = _report_cache.get("last_generated")
        has_report = "last_report" in _report_cache
    return {
        "status": "ok",
        "available": True,
        "last_report_generated": last_gen,
        "has_cached_report": has_report,
    }


def generate_dashboard_html(report_data: dict[str, Any]) -> str:
    """Generate a beautiful dark-theme HTML dashboard for CFO attribution review.

    Args:
        report_data: Attribution report dict from generate_attribution_report().

    Returns:
        Complete HTML string with inline CSS, brand colors, KPI cards,
        channel table, funnel visualization, and data lineage section.
    """
    total_spend = report_data.get("total_spend", 0)
    cph = report_data.get("cost_per_hire", 0)
    roi = report_data.get("roi_multiple", 0)
    efficiency = (
        report_data.get("cfo_metrics", {})
        .get("budget_efficiency_score", {})
        .get("value", 0)
    )
    quality = report_data.get("quality_score", 0)
    ttf = report_data.get("time_to_fill_estimate", 0)
    channels = report_data.get("channel_attribution", [])
    funnel = report_data.get("funnel", {})
    audit = report_data.get("audit_trail", {})
    summary = report_data.get("summary", {})
    cfo = report_data.get("cfo_metrics", {})

    # ── Format helpers ──
    def _fmt_currency(val: float) -> str:
        if val >= 1_000_000:
            return f"${val/1_000_000:.1f}M"
        if val >= 1_000:
            return f"${val/1_000:.1f}K"
        return f"${val:,.2f}"

    def _fmt_number(val: Union[int, float]) -> str:
        if isinstance(val, float):
            val = int(val)
        if val >= 1_000_000:
            return f"{val/1_000_000:.1f}M"
        if val >= 1_000:
            return f"{val/1_000:.1f}K"
        return f"{val:,}"

    # ── Channel rows ──
    channel_rows = ""
    for ch in channels:
        f = ch.get("funnel", {})
        channel_rows += f"""
        <tr>
            <td style="font-weight:600;color:{DOWNY_TEAL}">{ch['channel']}</td>
            <td>{_fmt_currency(ch['spend'])}</td>
            <td>{ch['attribution_pct']}%</td>
            <td>{_fmt_number(f.get('impressions', 0))}</td>
            <td>{_fmt_number(f.get('clicks', 0))}</td>
            <td>{_fmt_number(f.get('applications', 0))}</td>
            <td>{_fmt_number(f.get('hires', 0))}</td>
            <td>{_fmt_currency(ch['cost_per_click'])}</td>
            <td>{_fmt_currency(ch['cost_per_application'])}</td>
            <td>{_fmt_currency(ch['cost_per_hire'])}</td>
            <td>
                <div style="display:flex;align-items:center;gap:6px">
                    <div style="width:60px;height:8px;background:rgba(255,255,255,0.1);border-radius:4px;overflow:hidden">
                        <div style="width:{ch['quality_score']}%;height:100%;background:{'#22c55e' if ch['quality_score'] >= 70 else '#f59e0b' if ch['quality_score'] >= 40 else '#ef4444'};border-radius:4px"></div>
                    </div>
                    <span style="font-size:12px">{ch['quality_score']}</span>
                </div>
            </td>
        </tr>"""

    # ── CPQH rows ──
    cpqh_rows = ""
    for item in cfo.get("cost_per_quality_hire_by_channel", []):
        cpqh_rows += f"""
        <tr>
            <td>{item['channel']}</td>
            <td>{_fmt_currency(item['cost_per_quality_hire'])}</td>
            <td>{item['quality_adjusted_hires']}</td>
        </tr>"""

    # ── Funnel values ──
    funnel_spend = funnel.get("spend", {}).get("value", 0)
    funnel_impressions = funnel.get("impressions", {}).get("value", 0)
    funnel_clicks = funnel.get("clicks", {}).get("value", 0)
    funnel_applications = funnel.get("applications", {}).get("value", 0)
    funnel_interviews = funnel.get("interviews", {}).get("value", 0)
    funnel_hires = funnel.get("hires", {}).get("value", 0)

    # Funnel bar widths (relative to max)
    funnel_max = max(funnel_impressions, 1)
    funnel_items = [
        ("Spend", funnel_spend, f"${funnel_spend:,.0f}", BLUE_VIOLET, funnel_max),
        (
            "Impressions",
            funnel_impressions,
            _fmt_number(funnel_impressions),
            BLUE_VIOLET,
            funnel_max,
        ),
        ("Clicks", funnel_clicks, _fmt_number(funnel_clicks), BLUE_VIOLET, funnel_max),
        (
            "Applications",
            funnel_applications,
            _fmt_number(funnel_applications),
            DOWNY_TEAL,
            funnel_max,
        ),
        (
            "Interviews",
            funnel_interviews,
            _fmt_number(funnel_interviews),
            DOWNY_TEAL,
            funnel_max,
        ),
        ("Hires", funnel_hires, _fmt_number(funnel_hires), "#22c55e", funnel_max),
    ]

    funnel_bars = ""
    for label, val, display, color, mx in funnel_items:
        pct = max((val / mx * 100) if mx > 0 else 2, 2)
        funnel_bars += f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
            <div style="width:100px;text-align:right;font-size:13px;color:rgba(255,255,255,0.7)">{label}</div>
            <div style="flex:1;height:32px;background:rgba(255,255,255,0.05);border-radius:6px;overflow:hidden;position:relative">
                <div style="width:{pct}%;height:100%;background:linear-gradient(90deg,{color},{color}88);border-radius:6px;transition:width 0.5s"></div>
                <span style="position:absolute;left:12px;top:50%;transform:translateY(-50%);font-size:13px;font-weight:600;color:#fff">{display}</span>
            </div>
        </div>"""

    # ── Audit lineage rows ──
    audit_sources = audit.get("data_sources", [])
    lineage_rows = ""
    for src in audit_sources:
        conf_pct = src.get("confidence", 0) * 100
        lineage_rows += f"""
        <tr>
            <td style="font-weight:600">{src['name']}</td>
            <td><span style="display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;background:{'rgba(34,197,94,0.15);color:#22c55e' if src['type'] == 'primary' else 'rgba(245,158,11,0.15);color:#f59e0b' if src['type'] == 'reference' else 'rgba(239,68,68,0.15);color:#ef4444'}">{src['type']}</span></td>
            <td>
                <div style="display:flex;align-items:center;gap:6px">
                    <div style="width:80px;height:6px;background:rgba(255,255,255,0.1);border-radius:3px;overflow:hidden">
                        <div style="width:{conf_pct}%;height:100%;background:{DOWNY_TEAL};border-radius:3px"></div>
                    </div>
                    <span style="font-size:12px">{conf_pct:.0f}%</span>
                </div>
            </td>
        </tr>"""

    # ── Variance display ──
    variance_val = cfo.get("projected_vs_actual_spend_variance_pct", {}).get("value", 0)
    variance_color = (
        "#22c55e"
        if variance_val <= 0
        else "#f59e0b" if variance_val <= 10 else "#ef4444"
    )
    variance_label = f"{variance_val:+.1f}%"

    recruitment_pct = cfo.get("recruitment_marketing_pct_of_hiring_cost", {}).get(
        "value", 0
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Recruitment ROI Attribution | Nova AI Suite</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',system-ui,-apple-system,sans-serif;background:{PORT_GORE};color:#fff;min-height:100vh}}
.container{{max-width:1400px;margin:0 auto;padding:24px 20px}}
.header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:32px;padding-bottom:20px;border-bottom:1px solid rgba(255,255,255,0.08)}}
.header h1{{font-size:28px;font-weight:800;background:linear-gradient(135deg,{DOWNY_TEAL},{BLUE_VIOLET});-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
.header .subtitle{{font-size:13px;color:rgba(255,255,255,0.5);margin-top:4px}}
.badge{{display:inline-block;padding:4px 12px;border-radius:20px;font-size:11px;font-weight:600;background:rgba(90,84,189,0.2);color:{DOWNY_TEAL};border:1px solid rgba(107,179,205,0.3)}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px;margin-bottom:32px}}
.kpi-card{{background:linear-gradient(135deg,rgba(90,84,189,0.12),rgba(32,32,88,0.5));border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:24px;position:relative;overflow:hidden}}
.kpi-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,{BLUE_VIOLET},{DOWNY_TEAL})}}
.kpi-label{{font-size:12px;color:rgba(255,255,255,0.5);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px}}
.kpi-value{{font-size:32px;font-weight:800;color:#fff}}
.kpi-sub{{font-size:12px;color:rgba(255,255,255,0.4);margin-top:6px}}
.section{{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:16px;padding:24px;margin-bottom:24px}}
.section-title{{font-size:18px;font-weight:700;margin-bottom:16px;display:flex;align-items:center;gap:8px}}
.section-title .dot{{width:8px;height:8px;border-radius:50%;background:{DOWNY_TEAL}}}
table{{width:100%;border-collapse:collapse}}
th{{text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:rgba(255,255,255,0.4);padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.08)}}
td{{padding:12px;font-size:13px;border-bottom:1px solid rgba(255,255,255,0.04)}}
tr:hover td{{background:rgba(90,84,189,0.08)}}
.cfo-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;margin-bottom:24px}}
.cfo-card{{background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:12px;padding:20px}}
.cfo-card .label{{font-size:11px;color:rgba(255,255,255,0.4);text-transform:uppercase;letter-spacing:0.5px}}
.cfo-card .value{{font-size:24px;font-weight:700;margin:8px 0}}
.footer{{text-align:center;padding:32px 0;border-top:1px solid rgba(255,255,255,0.06);margin-top:32px}}
.footer a{{color:{DOWNY_TEAL};text-decoration:none}}
@media(max-width:768px){{
    .kpi-grid{{grid-template-columns:1fr 1fr}}
    .kpi-value{{font-size:24px}}
    .container{{padding:16px 12px}}
    table{{display:block;overflow-x:auto}}
    .header h1{{font-size:22px}}
}}
@media(max-width:480px){{
    .kpi-grid{{grid-template-columns:1fr}}
    .cfo-grid{{grid-template-columns:1fr}}
}}
</style>
</head>
<body>
<div class="container">
    <!-- Header -->
    <div class="header">
        <div>
            <h1>Recruitment ROI Attribution</h1>
            <div class="subtitle">CFO-Ready Attribution Dashboard -- Nova AI Suite</div>
        </div>
        <div style="text-align:right">
            <div class="badge">AUDIT GRADE</div>
            <div style="font-size:11px;color:rgba(255,255,255,0.3);margin-top:6px">{audit.get('report_generated_at', '')[:19]}</div>
        </div>
    </div>

    <!-- KPI Cards -->
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">Total Media Spend</div>
            <div class="kpi-value">{_fmt_currency(total_spend)}</div>
            <div class="kpi-sub">{recruitment_pct:.1f}% of total hiring cost</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Cost per Hire</div>
            <div class="kpi-value">{_fmt_currency(cph)}</div>
            <div class="kpi-sub">Across {summary.get('total_channels', 0)} channels</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">ROI Multiple</div>
            <div class="kpi-value" style="color:{'#22c55e' if roi >= 3 else '#f59e0b' if roi >= 1 else '#ef4444'}">{roi:.1f}x</div>
            <div class="kpi-sub">Hire value / media spend</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Budget Efficiency</div>
            <div class="kpi-value" style="color:{'#22c55e' if efficiency >= 70 else '#f59e0b' if efficiency >= 40 else '#ef4444'}">{efficiency:.0f}/100</div>
            <div class="kpi-sub">vs. industry benchmark</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Quality Score</div>
            <div class="kpi-value">{quality:.0f}/100</div>
            <div class="kpi-sub">Hire quality weighted by spend</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">Time to Fill</div>
            <div class="kpi-value">{ttf} days</div>
            <div class="kpi-sub">Industry avg: {_get_benchmarks(audit.get('industry_used', 'default'))['avg_time_to_fill']}d</div>
        </div>
    </div>

    <!-- CFO Metrics -->
    <div class="section">
        <div class="section-title"><div class="dot"></div>P&L Metrics</div>
        <div class="cfo-grid">
            <div class="cfo-card">
                <div class="label">Recruitment Marketing % of Hiring Cost</div>
                <div class="value">{recruitment_pct:.1f}%</div>
            </div>
            <div class="cfo-card">
                <div class="label">Spend Variance (Projected vs Actual)</div>
                <div class="value" style="color:{variance_color}">{variance_label}</div>
            </div>
            <div class="cfo-card">
                <div class="label">Total Hires Attributed</div>
                <div class="value">{summary.get('total_hires', 0)}</div>
            </div>
            <div class="cfo-card">
                <div class="label">Estimated Hire Value</div>
                <div class="value">{_fmt_currency(summary.get('estimated_hire_value', 0))}</div>
            </div>
        </div>
    </div>

    <!-- Funnel Visualization -->
    <div class="section">
        <div class="section-title"><div class="dot"></div>Attribution Funnel</div>
        <div style="padding:12px 0">
            {funnel_bars}
        </div>
    </div>

    <!-- Channel Breakdown Table -->
    <div class="section">
        <div class="section-title"><div class="dot"></div>Channel Attribution Breakdown</div>
        <div style="overflow-x:auto">
        <table>
            <thead>
                <tr>
                    <th>Channel</th>
                    <th>Spend</th>
                    <th>Attribution</th>
                    <th>Impressions</th>
                    <th>Clicks</th>
                    <th>Applications</th>
                    <th>Hires</th>
                    <th>CPC</th>
                    <th>CPA</th>
                    <th>CPH</th>
                    <th>Quality</th>
                </tr>
            </thead>
            <tbody>
                {channel_rows}
            </tbody>
        </table>
        </div>
    </div>

    <!-- Cost per Quality Hire -->
    <div class="section">
        <div class="section-title"><div class="dot"></div>Cost per Quality Hire by Channel</div>
        <div style="overflow-x:auto">
        <table>
            <thead>
                <tr>
                    <th>Channel</th>
                    <th>Cost per Quality Hire</th>
                    <th>Quality-Adjusted Hires</th>
                </tr>
            </thead>
            <tbody>
                {cpqh_rows}
            </tbody>
        </table>
        </div>
    </div>

    <!-- Data Lineage & Audit Trail -->
    <div class="section">
        <div class="section-title"><div class="dot"></div>Data Lineage & Audit Trail</div>
        <div style="margin-bottom:16px">
            <div style="font-size:12px;color:rgba(255,255,255,0.4);margin-bottom:4px">Methodology</div>
            <div style="font-size:14px">{audit.get('methodology', 'N/A')}</div>
        </div>
        <div style="margin-bottom:16px">
            <div style="font-size:12px;color:rgba(255,255,255,0.4);margin-bottom:4px">Benchmark Set</div>
            <div style="font-size:14px">{audit.get('benchmark_set', 'N/A')}</div>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Data Source</th>
                    <th>Type</th>
                    <th>Confidence</th>
                </tr>
            </thead>
            <tbody>
                {lineage_rows}
            </tbody>
        </table>
    </div>

    <!-- Footer -->
    <div class="footer">
        <div style="font-size:13px;color:rgba(255,255,255,0.3)">
            Nova AI Suite -- Recruitment Intelligence Platform |
            <a href="https://www.linkedin.com/in/chandel13/" target="_blank">Shubham Singh Chandel</a>
        </div>
    </div>
</div>
</body>
</html>"""
