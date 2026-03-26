"""Campaign and plan sharing route handlers.

Extracted from app.py to reduce its size.  Handles:
- POST /api/campaign/save
- GET  /api/campaign/list
- POST /api/plan/share
- POST /api/plan/feedback
- POST /api/plan/scorecard
- GET  /plan/shared/<id>  (read-only shared plan view)
- GET  /scorecard/<id>    (shareable plan scorecard)
"""

import datetime
import html
import json
import logging
import sys
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_campaign_get_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch campaign-related GET routes.  Returns True if handled."""
    if path == "/api/campaign/list":
        _handle_campaign_list(handler, path, parsed)
        return True
    if path.startswith("/plan/shared/"):
        _handle_shared_plan_view(handler, path, parsed)
        return True
    # /plan/{id} -- shareable read-only plan view (24h TTL)
    if path.startswith("/plan/") and not path.startswith("/plan/shared/"):
        _handle_plan_direct_view(handler, path, parsed)
        return True
    if path.startswith("/scorecard/"):
        _handle_scorecard_view(handler, path, parsed)
        return True
    return False


def handle_campaign_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch campaign-related POST routes.  Returns True if handled."""
    _fn = _CAMPAIGN_POST_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# GET handlers
# ---------------------------------------------------------------------------


def _handle_campaign_list(handler: Any, path: str, parsed: Any) -> None:
    """GET /api/campaign/list -- list saved campaigns."""
    try:
        campaigns = getattr(handler.server, "_campaigns", {})
        campaign_list = sorted(
            campaigns.values(),
            key=lambda c: c.get("_saved_at") or "",
            reverse=True,
        )
        handler._send_json({"campaigns": campaign_list[:50]})
    except Exception as e:
        logger.error("Campaign list error: %s", e, exc_info=True)
        handler._send_json({"error": str(e)}, status_code=500)


def _handle_shared_plan_view(handler: Any, path: str, parsed: Any) -> None:
    """GET /plan/shared/<id> -- read-only dashboard view with OG tags and feedback."""
    import html as _html_mod

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _shared_plans = getattr(_app, "_shared_plans", {})
    _shared_plans_lock = getattr(_app, "_shared_plans_lock", None)
    _plan_feedback = getattr(_app, "_plan_feedback", {})
    _plan_feedback_lock = getattr(_app, "_plan_feedback_lock", None)

    share_id = path.split("/plan/shared/")[-1].rstrip("/")
    if not share_id:
        handler.send_error(404, "Share ID required")
        return

    if _shared_plans_lock:
        with _shared_plans_lock:
            shared = _shared_plans.get(share_id)
    else:
        shared = _shared_plans.get(share_id)

    if not shared:
        handler.send_error(404, "Shared plan not found or expired")
        return

    plan_data = shared.get("plan_data") or {}
    client_name = shared.get("client") or "Unnamed"
    created_at = shared.get("created_at") or 0
    created_str = (
        time.strftime("%B %d, %Y at %H:%M UTC", time.gmtime(created_at))
        if created_at
        else "Unknown"
    )

    # Extract plan metrics
    summary = plan_data.get("summary") or plan_data.get("plan_summary") or plan_data
    channels = (
        summary.get("channels")
        or summary.get("recommended_channels")
        or plan_data.get("channels")
        or []
    )
    num_channels = len(channels) if isinstance(channels, list) else channels
    budget_val = (
        summary.get("total_budget")
        or summary.get("budget_range")
        or plan_data.get("budget_range")
        or "--"
    )
    industry_val = (
        summary.get("industry")
        or plan_data.get("industry")
        or plan_data.get("industry_label")
        or "--"
    )
    est_applications = str(
        summary.get("est_applications") or summary.get("estimated_applications") or "--"
    )
    est_hires = str(
        summary.get("est_hires")
        or summary.get("estimated_hires")
        or plan_data.get("hire_volume")
        or "--"
    )

    # Build channel table rows
    ch_table_html = ""
    ch_bar_html = ""
    if isinstance(channels, list) and channels:
        for i, ch in enumerate(channels[:15]):
            if not isinstance(ch, dict):
                continue
            ch_name = _html_mod.escape(
                str(ch.get("name") or ch.get("channel") or "N/A")
            )
            ch_spend = ch.get("spend") or ch.get("budget") or 0
            ch_alloc = ch.get("allocation_pct") or 0
            ch_cpc = ch.get("cpc") or ch.get("cost_per_click") or "--"
            ch_cpa = ch.get("cpa") or ch.get("cost_per_apply") or "--"
            try:
                spend_fmt = f"${float(ch_spend):,.0f}"
            except (ValueError, TypeError):
                spend_fmt = str(ch_spend)
            try:
                alloc_fmt = f"{float(ch_alloc):.1f}%"
            except (ValueError, TypeError):
                alloc_fmt = str(ch_alloc)
            try:
                cpc_fmt = f"${float(ch_cpc):,.2f}"
            except (ValueError, TypeError):
                cpc_fmt = str(ch_cpc)
            try:
                cpa_fmt = f"${float(ch_cpa):,.2f}"
            except (ValueError, TypeError):
                cpa_fmt = str(ch_cpa)

            zebra = "background:rgba(255,255,255,0.02);" if i % 2 == 1 else ""
            ch_table_html += f'<tr style="{zebra}"><td style="padding:10px 12px;font-weight:500;">{ch_name}</td><td style="padding:10px 12px;text-align:right;">{spend_fmt}</td><td style="padding:10px 12px;text-align:right;">{alloc_fmt}</td><td style="padding:10px 12px;text-align:right;">{cpc_fmt}</td><td style="padding:10px 12px;text-align:right;">{cpa_fmt}</td></tr>'

            # Bar chart data
            bar_colors = [
                "#5A54BD",
                "#6BB3CD",
                "#B5669C",
                "#CE9047",
                "#202058",
                "#7C6BC4",
                "#4A9CB5",
            ]
            bar_color = bar_colors[i % len(bar_colors)]
            try:
                bar_w = float(ch_alloc)
            except (ValueError, TypeError):
                bar_w = 0
            ch_bar_html += f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;"><span style="width:120px;font-size:12px;text-align:right;color:rgba(255,255,255,0.7);flex-shrink:0;">{ch_name}</span><div style="flex:1;height:20px;background:rgba(255,255,255,0.05);border-radius:4px;overflow:hidden;"><div style="height:100%;width:{bar_w}%;background:{bar_color};border-radius:4px;min-width:2px;"></div></div><span style="width:80px;font-size:11px;color:rgba(255,255,255,0.5);">{alloc_fmt}</span></div>'

    # Existing feedback
    if _plan_feedback_lock:
        with _plan_feedback_lock:
            feedbacks = _plan_feedback.get(share_id, [])
    else:
        feedbacks = _plan_feedback.get(share_id, [])

    feedback_html = ""
    for fb in feedbacks:
        fb_name = fb.get("name") or "Anonymous"
        fb_comment = fb.get("comment") or ""
        fb_time = time.strftime(
            "%b %d, %Y %H:%M", time.gmtime(fb.get("created_at") or 0)
        )
        feedback_html += f'<div style="padding:12px;background:rgba(255,255,255,0.03);border-radius:8px;margin-bottom:8px;"><div style="font-size:12px;color:rgba(255,255,255,0.5);margin-bottom:4px;">{_html_mod.escape(str(fb_name))} -- {fb_time}</div><div style="font-size:13px;color:rgba(255,255,255,0.8);">{_html_mod.escape(str(fb_comment))}</div></div>'

    # OG meta description
    og_desc = f"Media plan for {_html_mod.escape(str(client_name))} - {_html_mod.escape(str(industry_val))} industry, {num_channels} channels, budget: {_html_mod.escape(str(budget_val))}"
    og_title = f"Media Plan: {_html_mod.escape(str(client_name))} | Nova AI Suite"

    page_html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{og_title}</title>
<!-- Open Graph meta tags for Slack/LinkedIn previews -->
<meta property="og:title" content="{og_title}">
<meta property="og:description" content="{og_desc}">
<meta property="og:type" content="article">
<meta property="og:site_name" content="Nova AI Suite">
<meta name="twitter:card" content="summary">
<meta name="twitter:title" content="{og_title}">
<meta name="twitter:description" content="{og_desc}">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',system-ui,sans-serif;background:#0a0a1e;color:#e2e8f0;min-height:100vh;padding:0}}
.hero{{background:linear-gradient(135deg,#202058 0%,#1a1a40 50%,#0f0f2e 100%);padding:48px 20px 32px;border-bottom:1px solid rgba(90,84,189,0.3)}}
.hero-inner{{max-width:900px;margin:0 auto}}
.hero h1{{font-size:28px;font-weight:700;color:#fff;margin-bottom:6px}}
.hero .meta{{color:rgba(255,255,255,0.5);font-size:13px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}}
.badge{{display:inline-block;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;background:rgba(107,179,205,0.15);color:#6BB3CD}}
.container{{max-width:900px;margin:0 auto;padding:24px 20px 48px}}
.metrics-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:28px}}
.metric-card{{background:rgba(20,20,45,0.8);border:1px solid rgba(255,255,255,0.08);border-radius:12px;padding:20px;text-align:center}}
.metric-value{{font-size:26px;font-weight:700;color:#fff;margin-bottom:4px}}
.metric-label{{font-size:11px;color:rgba(255,255,255,0.4);text-transform:uppercase;letter-spacing:1px}}
.card{{background:rgba(20,20,45,0.8);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:24px;margin-bottom:24px}}
.card h2{{font-size:16px;font-weight:600;color:#fff;margin-bottom:16px;padding-bottom:8px;border-bottom:1px solid rgba(90,84,189,0.2)}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.1);color:rgba(255,255,255,0.4);font-size:11px;text-transform:uppercase;letter-spacing:0.5px;font-weight:600}}
th.r{{text-align:right}}
td{{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.04);color:rgba(255,255,255,0.8)}}
.feedback-form{{display:flex;flex-direction:column;gap:12px}}
input,textarea{{background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.1);border-radius:8px;padding:10px 14px;color:white;font-size:13px;font-family:inherit}}
textarea{{resize:vertical;min-height:80px}}
input:focus,textarea:focus{{outline:none;border-color:rgba(90,84,189,0.5)}}
.submit-btn{{background:linear-gradient(135deg,#5a54bd,#4f46e5);color:white;border:none;border-radius:8px;padding:12px 24px;font-size:14px;cursor:pointer;font-weight:500;transition:opacity 0.2s}}
.submit-btn:hover{{opacity:0.9}}
.submit-btn:disabled{{opacity:0.5;cursor:not-allowed}}
.brand{{text-align:center;margin-top:32px;color:rgba(255,255,255,0.3);font-size:12px}}
.brand a{{color:rgba(90,84,189,0.7);text-decoration:none}}
.toast{{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#4ade80;color:#0a0a1e;padding:10px 20px;border-radius:8px;font-size:13px;font-weight:500;opacity:0;transition:opacity 0.3s;z-index:999}}
.toast.show{{opacity:1}}
@media(max-width:600px){{.metrics-grid{{grid-template-columns:repeat(2,1fr)}}.hero h1{{font-size:22px}}}}
</style></head><body>
<div class="hero"><div class="hero-inner">
  <h1>Media Plan: {_html_mod.escape(str(client_name))}</h1>
  <div class="meta">
    <span>{_html_mod.escape(str(industry_val))}</span>
    <span>Budget: {_html_mod.escape(str(budget_val))}</span>
    <span>Shared on {created_str}</span>
    <span class="badge">Read-only Dashboard</span>
  </div>
</div></div>
<div class="container">
  <!-- Key Metrics Cards -->
  <div class="metrics-grid">
    <div class="metric-card"><div class="metric-value">{num_channels}</div><div class="metric-label">Channels</div></div>
    <div class="metric-card"><div class="metric-value">{_html_mod.escape(str(budget_val))}</div><div class="metric-label">Total Budget</div></div>
    <div class="metric-card"><div class="metric-value">{_html_mod.escape(est_applications)}</div><div class="metric-label">Est. Applications</div></div>
    <div class="metric-card"><div class="metric-value">{_html_mod.escape(est_hires)}</div><div class="metric-label">Est. Hires</div></div>
  </div>

  <!-- Channel Allocation Chart -->
  {f'<div class="card"><h2>Channel Allocation</h2>{ch_bar_html}</div>' if ch_bar_html else ''}

  <!-- Channel Strategy Table -->
  <div class="card">
    <h2>Channel Strategy</h2>
    <table>
      <thead><tr><th>Channel</th><th class="r">Spend</th><th class="r">Allocation</th><th class="r">CPC</th><th class="r">CPA</th></tr></thead>
      <tbody>{ch_table_html or '<tr><td colspan="5" style="text-align:center;color:rgba(255,255,255,0.4);">No channel data</td></tr>'}</tbody>
    </table>
  </div>

  <!-- Feedback Section -->
  <div class="card">
    <h2>Feedback</h2>
    {feedback_html if feedback_html else '<p style="color:rgba(255,255,255,0.4);font-size:13px;margin-bottom:16px;">No feedback yet.</p>'}
    <div class="feedback-form">
      <input type="text" id="fbName" placeholder="Your name" maxlength="100">
      <textarea id="fbComment" placeholder="Leave your feedback or suggestions..." maxlength="2000"></textarea>
      <button class="submit-btn" id="fbSubmit" onclick="submitFeedback()">Submit Feedback</button>
    </div>
  </div>

  <div class="brand">Powered by <a href="https://www.linkedin.com/in/chandel13/" target="_blank">Nova AI Suite</a></div>
</div>
<div class="toast" id="fbToast"></div>
<script>
async function submitFeedback() {{
  var btn = document.getElementById('fbSubmit');
  var name = document.getElementById('fbName').value.trim();
  var comment = document.getElementById('fbComment').value.trim();
  if (!comment) {{ showFbToast('Please enter a comment'); return; }}
  btn.disabled = true;
  btn.textContent = 'Submitting...';
  try {{
    var resp = await fetch('/api/plan/feedback', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{ share_id: '{share_id}', name: name || 'Anonymous', comment: comment }})
    }});
    var result = await resp.json();
    if (result.ok) {{
      showFbToast('Feedback submitted!');
      document.getElementById('fbComment').value = '';
      setTimeout(function() {{ location.reload(); }}, 1200);
    }} else {{
      showFbToast(result.error || 'Failed to submit');
    }}
  }} catch(e) {{
    showFbToast('Network error');
  }}
  btn.disabled = false;
  btn.textContent = 'Submit Feedback';
}}
function showFbToast(msg) {{
  var t = document.getElementById('fbToast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(function() {{ t.classList.remove('show'); }}, 3000);
}}
</script></body></html>"""

    body_bytes = page_html.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Cache-Control", "private, max-age=300")
    handler.send_header("Content-Length", str(len(body_bytes)))
    handler.end_headers()
    handler.wfile.write(body_bytes)


def _handle_plan_direct_view(handler: Any, path: str, parsed: Any) -> None:
    """GET /plan/{id} -- read-only shareable plan view via plan_id (24h TTL)."""
    import html as _html_m
    import re as _re_m

    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _plan_results_store = getattr(_app, "_plan_results_store", {})
    _plan_results_lock = getattr(_app, "_plan_results_lock", None)

    _pv_id = path.split("/plan/")[-1].rstrip("/").split("?")[0]
    if not _pv_id or not _re_m.match(r"^[a-f0-9]{1,12}$", _pv_id):
        handler.send_error(404)
        return

    _pv_entry = None
    if _plan_results_lock:
        with _plan_results_lock:
            _pv_entry = _plan_results_store.get(_pv_id)
            if _pv_entry and time.time() - _pv_entry["created"] > 86400:
                _plan_results_store.pop(_pv_id, None)
                _pv_entry = None
    else:
        _pv_entry = _plan_results_store.get(_pv_id)

    if not _pv_entry:
        handler.send_response(404)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.end_headers()
        handler.wfile.write(
            b'<html><body style="font-family:system-ui;background:#0a0a1e;color:#e2e8f0;'
            b'display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0">'
            b'<div style="text-align:center"><h1>Plan Expired</h1>'
            b'<p style="color:#94a3b8">This link expired (24h limit).</p>'
            b'<a href="/" style="color:#6BB3CD">Generate New Plan</a></div></body></html>'
        )
        return

    try:
        _pd = _pv_entry.get("data") or {}
        _ps = _pd.get("summary") or {}
        _pch = _ps.get("channels") or []
        _mt = _pd.get("metadata") or {}
        _cl = _html_m.escape(_mt.get("client_name") or "Client")
        _ind = _html_m.escape(_mt.get("industry_label") or "")
        _bud = _html_m.escape(str(_mt.get("total_budget") or ""))
        _gen = _html_m.escape((_mt.get("generated_at") or "")[:10])
        _rows = ""
        for _c in (_pch if isinstance(_pch, list) else [])[:20]:
            if isinstance(_c, dict):
                _rows += (
                    f"<tr><td>{_html_m.escape(str(_c.get('name', '')))}</td>"
                    f"<td>${_c.get('budget', 0):,.0f}</td>"
                    f"<td>{_c.get('allocation_pct', 0)}%</td>"
                    f"<td>{_html_m.escape(str(_c.get('cpc_range', '--')))}</td>"
                    f"<td>{_html_m.escape(str(_c.get('cpa_range', '--')))}</td></tr>"
                )
        _nc = _ps.get("total_channels") or (len(_pch) if isinstance(_pch, list) else 0)
        _ea = _ps.get("est_applications") or "--"
        _eh = _ps.get("est_hires") or "--"
        _empty_row = '<tr><td colspan="5" style="text-align:center;color:#94a3b8">No data</td></tr>'
        _body = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Plan: {_cl} | Nova AI Suite</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<style>*{{box-sizing:border-box}}body{{font-family:Inter,system-ui;background:#0a0a1e;color:#e2e8f0;margin:0;padding:20px}}.w{{max-width:900px;margin:0 auto}}h1{{font-size:22px;margin:0 0 8px}}.mt{{color:#94a3b8;font-size:13px}}.cd{{background:rgba(20,20,45,.8);border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:20px;margin-bottom:16px}}.cd h3{{margin:0 0 12px;font-size:15px;color:#7b75d4}}.sg{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:24px}}.si{{background:rgba(255,255,255,.04);border-radius:8px;padding:16px;text-align:center}}.sv{{font-size:24px;font-weight:700;color:#fff}}.sl{{font-size:11px;color:#94a3b8;text-transform:uppercase;margin-top:4px}}table{{width:100%;border-collapse:collapse;font-size:13px}}th{{text-align:left;padding:10px;border-bottom:1px solid rgba(255,255,255,.1);color:rgba(255,255,255,.5);font-size:11px;text-transform:uppercase}}td{{padding:10px;border-bottom:1px solid rgba(255,255,255,.05)}}.bd{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;background:rgba(107,179,205,.15);color:#6BB3CD}}.ft{{text-align:center;padding:24px 0;color:#64748b;font-size:12px}}a{{color:#6BB3CD}}</style>
</head><body><div class="w">
<div style="padding:24px 0;border-bottom:1px solid rgba(255,255,255,.1);margin-bottom:24px">
<h1>Media Plan: {_cl}</h1>
<div class="mt">{_ind} &middot; Budget: {_bud} &middot; {_gen} <span class="bd">Read-only</span></div></div>
<div class="sg"><div class="si"><div class="sv">{_nc}</div><div class="sl">Channels</div></div>
<div class="si"><div class="sv">{_ea}</div><div class="sl">Est. Applications</div></div>
<div class="si"><div class="sv">{_eh}</div><div class="sl">Est. Hires</div></div></div>
<div class="cd"><h3>Channel Strategy</h3><table><thead><tr><th>Channel</th><th>Budget</th><th>Alloc</th><th>CPC</th><th>CPA</th></tr></thead>
<tbody>{_rows or _empty_row}</tbody></table></div>
<div class="ft">Shared via <a href="/">Nova AI Suite</a> &middot; Expires 24h</div>
</div></body></html>"""
        body_bytes = _body.encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header("Cache-Control", "private, max-age=300")
        handler.send_header("Content-Length", str(len(body_bytes)))
        handler.end_headers()
        handler.wfile.write(body_bytes)
    except Exception as _pv_exc:
        logger.error("Plan direct view render failed: %s", _pv_exc, exc_info=True)
        handler.send_error(500)


# ---------------------------------------------------------------------------
# POST handlers
# ---------------------------------------------------------------------------


def _handle_campaign_save(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/campaign/save -- save campaign context."""
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        campaign_id = data.get("id") or str(uuid.uuid4())[:8]
        data["id"] = campaign_id
        data["_saved_at"] = datetime.datetime.utcnow().isoformat()
        # Store in memory
        if not hasattr(handler.server, "_campaigns"):
            handler.server._campaigns = {}
        handler.server._campaigns[campaign_id] = data
        handler._send_json({"ok": True, "id": campaign_id})
    except Exception as e:
        logger.error("Campaign save error: %s", e, exc_info=True)
        handler._send_json({"error": str(e)}, status_code=500)


def _handle_plan_share(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/plan/share -- create a shareable plan link."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _shared_plans = getattr(_app, "_shared_plans", {})
    _shared_plans_lock = getattr(_app, "_shared_plans_lock", None)

    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        plan_data = data.get("plan_data") or {}
        client = data.get("client") or "Unnamed"

        # ── Gold Standard: Reject empty/null share creation ──
        if not plan_data or (
            isinstance(plan_data, dict) and not any(plan_data.values())
        ):
            handler._send_json(
                {"error": "Cannot share an empty plan. Generate a plan first."},
                status_code=400,
            )
            return
        if not isinstance(plan_data, dict):
            handler._send_json(
                {"error": "plan_data must be a JSON object"},
                status_code=400,
            )
            return

        share_id = uuid.uuid4().hex[:8]
        entry = {
            "plan_data": plan_data,
            "client": client,
            "created_at": time.time(),
        }
        if _shared_plans_lock:
            with _shared_plans_lock:
                _shared_plans[share_id] = entry
        else:
            _shared_plans[share_id] = entry
        handler._send_json(
            {
                "share_id": share_id,
                "url": f"/plan/shared/{share_id}",
            }
        )
    except json.JSONDecodeError:
        handler._send_json({"error": "Invalid JSON"}, status_code=400)
    except Exception as e:
        logger.error("Plan share endpoint error: %s", e, exc_info=True)
        handler._send_json({"error": "Failed to create share link"}, status_code=500)


def _handle_plan_feedback(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/plan/feedback -- submit feedback on a shared plan."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _shared_plans = getattr(_app, "_shared_plans", {})
    _plan_feedback = getattr(_app, "_plan_feedback", {})
    _plan_feedback_lock = getattr(_app, "_plan_feedback_lock", None)

    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        share_id = data.get("share_id") or ""
        name = data.get("name") or "Anonymous"
        comment = data.get("comment") or ""

        if not share_id or share_id not in _shared_plans:
            handler._send_json({"error": "Invalid share ID"}, status_code=404)
            return
        if not comment.strip():
            handler._send_json({"error": "Comment is required"}, status_code=400)
            return

        feedback_entry = {
            "name": name[:100],
            "comment": comment[:2000],
            "created_at": time.time(),
        }
        _plan_feedback_ts = getattr(_app, "_plan_feedback_ts", {})
        if _plan_feedback_lock:
            with _plan_feedback_lock:
                if share_id not in _plan_feedback:
                    _plan_feedback[share_id] = []
                _plan_feedback[share_id].append(feedback_entry)
                _plan_feedback_ts[share_id] = time.time()
                count = len(_plan_feedback[share_id])
        else:
            if share_id not in _plan_feedback:
                _plan_feedback[share_id] = []
            _plan_feedback[share_id].append(feedback_entry)
            _plan_feedback_ts[share_id] = time.time()
            count = len(_plan_feedback[share_id])

        handler._send_json({"ok": True, "feedback_count": count})
    except json.JSONDecodeError:
        handler._send_json({"error": "Invalid JSON"}, status_code=400)
    except Exception as e:
        logger.error("Plan feedback endpoint error: %s", e, exc_info=True)
        handler._send_json({"error": "Failed to submit feedback"}, status_code=500)


# ---------------------------------------------------------------------------
# Scorecard handlers
# ---------------------------------------------------------------------------


def _handle_plan_scorecard(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/plan/scorecard -- generate a shareable plan scorecard."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _scorecards = getattr(_app, "_scorecards", {})
    _scorecards_lock = getattr(_app, "_scorecards_lock", None)

    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(content_len) if content_len > 0 else b"{}"
        data = json.loads(body)
        plan_data = data.get("plan_data") or {}

        if not plan_data:
            handler._send_json({"error": "plan_data is required"}, status_code=400)
            return

        from scorecard_generator import generate_share_id, generate_scorecard_html

        share_id = generate_share_id(plan_data)
        scorecard_html = generate_scorecard_html(plan_data, share_id)

        # Store in memory as (html, timestamp) tuple
        if _scorecards_lock:
            with _scorecards_lock:
                _scorecards[share_id] = (scorecard_html, time.time())
        else:
            _scorecards[share_id] = (scorecard_html, time.time())

        # Persist to Supabase if available
        try:
            _supabase_rest = getattr(_app, "_supabase_rest", None)
            if _supabase_rest:
                _supabase_rest(
                    "scorecards",
                    method="POST",
                    data={
                        "share_id": share_id,
                        "html": scorecard_html,
                        "plan_data": json.dumps(plan_data, default=str),
                        "created_at": time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                        ),
                    },
                )
        except Exception as e:
            logger.warning("Supabase scorecard persist failed (non-fatal): %s", e)

        base_url = "https://media-plan-generator.onrender.com"
        handler._send_json(
            {
                "success": True,
                "share_id": share_id,
                "share_url": f"{base_url}/scorecard/{share_id}",
            }
        )
    except json.JSONDecodeError:
        handler._send_json({"error": "Invalid JSON"}, status_code=400)
    except Exception as e:
        logger.error("Scorecard generation error: %s", e, exc_info=True)
        handler._send_json({"error": "Failed to generate scorecard"}, status_code=500)


def _handle_scorecard_view(handler: Any, path: str, parsed: Any) -> None:
    """GET /scorecard/<share_id> -- serve a shareable plan scorecard."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _scorecards = getattr(_app, "_scorecards", {})
    _scorecards_lock = getattr(_app, "_scorecards_lock", None)

    share_id = path.split("/scorecard/")[-1].rstrip("/")
    if not share_id:
        handler.send_error(404, "Scorecard ID required")
        return

    # Look up in memory first -- values are (html, timestamp) tuples
    html_content = None
    if _scorecards_lock:
        with _scorecards_lock:
            _sc_entry = _scorecards.get(share_id)
            if _sc_entry:
                html_content = (
                    _sc_entry[0] if isinstance(_sc_entry, tuple) else _sc_entry
                )
    else:
        _sc_entry = _scorecards.get(share_id)
        if _sc_entry:
            html_content = _sc_entry[0] if isinstance(_sc_entry, tuple) else _sc_entry

    # Fallback: try Supabase
    if not html_content:
        try:
            _supabase_rest = getattr(_app, "_supabase_rest", None)
            if _supabase_rest:
                result = _supabase_rest(
                    "scorecards",
                    method="GET",
                    params=f"?share_id=eq.{share_id}&select=html&limit=1",
                )
                if result and isinstance(result, list) and result[0].get("html"):
                    html_content = result[0]["html"]
                    # Cache in memory for subsequent requests as (html, ts) tuple
                    if _scorecards_lock:
                        with _scorecards_lock:
                            _scorecards[share_id] = (html_content, time.time())
                    else:
                        _scorecards[share_id] = (html_content, time.time())
        except Exception as e:
            logger.warning("Supabase scorecard lookup failed: %s", e)

    if not html_content:
        handler.send_error(404, "Scorecard not found")
        return

    body_bytes = html_content.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body_bytes)))
    handler.send_header("Cache-Control", "public, max-age=3600")
    handler.end_headers()
    handler.wfile.write(body_bytes)


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_CAMPAIGN_POST_ROUTE_MAP: dict[str, Any] = {
    "/api/campaign/save": _handle_campaign_save,
    "/api/plan/share": _handle_plan_share,
    "/api/plan/feedback": _handle_plan_feedback,
    "/api/plan/scorecard": _handle_plan_scorecard,
}
