"""Campaign and plan sharing route handlers.

Extracted from app.py to reduce its size.  Handles:
- POST /api/campaign/save
- GET  /api/campaign/list
- POST /api/plan/share
- POST /api/plan/feedback
- GET  /plan/shared/<id>  (read-only shared plan view)
"""

import datetime
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
            key=lambda c: c.get("_saved_at", ""),
            reverse=True,
        )
        handler._send_json({"campaigns": campaign_list[:50]})
    except Exception as e:
        logger.error("Campaign list error: %s", e, exc_info=True)
        handler._send_json({"error": str(e)}, status_code=500)


def _handle_shared_plan_view(handler: Any, path: str, parsed: Any) -> None:
    """GET /plan/shared/<id> -- read-only shared plan view with feedback."""
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

    # Build summary rows from plan data
    summary = plan_data.get("summary") or plan_data.get("plan_summary") or plan_data
    channels = (
        summary.get("channels")
        or summary.get("recommended_channels")
        or plan_data.get("channels")
        or []
    )
    num_channels = len(channels) if isinstance(channels, list) else channels

    rows_html = ""
    detail_fields = [
        ("Client", client_name),
        ("Created", created_str),
        (
            "Budget",
            summary.get("total_budget")
            or summary.get("budget_range")
            or plan_data.get("budget_range")
            or "--",
        ),
        (
            "Industry",
            summary.get("industry")
            or plan_data.get("industry")
            or plan_data.get("industry_label")
            or "--",
        ),
        ("# Channels", str(num_channels)),
        (
            "Est. Applications",
            str(
                summary.get("est_applications")
                or summary.get("estimated_applications")
                or "--"
            ),
        ),
        (
            "Est. Hires",
            str(
                summary.get("est_hires")
                or summary.get("estimated_hires")
                or plan_data.get("hire_volume")
                or "--"
            ),
        ),
    ]
    for label, val in detail_fields:
        rows_html += f'<tr><td style="padding:10px 16px;color:rgba(255,255,255,0.5);font-size:13px;">{label}</td><td style="padding:10px 16px;color:rgba(255,255,255,0.85);font-size:13px;">{val}</td></tr>'

    # Channel detail
    if isinstance(channels, list) and channels:
        for ch in channels[:20]:
            ch_name = (
                ch.get("name") or ch.get("channel") or str(ch)
                if isinstance(ch, dict)
                else str(ch)
            )
            ch_spend = (
                ch.get("spend") or ch.get("budget") or ""
                if isinstance(ch, dict)
                else ""
            )
            rows_html += f'<tr><td style="padding:6px 16px 6px 32px;color:rgba(255,255,255,0.4);font-size:12px;">Channel</td><td style="padding:6px 16px;color:rgba(255,255,255,0.7);font-size:12px;">{ch_name}{(" -- " + str(ch_spend)) if ch_spend else ""}</td></tr>'

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
        feedback_html += f'<div style="padding:12px;background:rgba(255,255,255,0.03);border-radius:8px;margin-bottom:8px;"><div style="font-size:12px;color:rgba(255,255,255,0.5);margin-bottom:4px;">{fb_name} -- {fb_time}</div><div style="font-size:13px;color:rgba(255,255,255,0.8);">{fb_comment}</div></div>'

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Shared Plan -- {client_name} | Nova</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',system-ui,sans-serif;background:#0a0a1e;color:white;min-height:100vh;padding:40px 20px}}
.container{{max-width:720px;margin:0 auto}}
h1{{font-size:24px;margin-bottom:8px}}
.subtitle{{color:rgba(255,255,255,0.5);font-size:14px;margin-bottom:32px}}
.card{{background:rgba(20,20,45,0.8);border:1px solid rgba(255,255,255,0.08);border-radius:16px;padding:24px;margin-bottom:24px}}
table{{width:100%;border-collapse:collapse}}
tr:not(:last-child) td{{border-bottom:1px solid rgba(255,255,255,0.05)}}
h2{{font-size:18px;margin-bottom:16px;color:white}}
h3{{font-size:16px;margin-bottom:12px;color:white}}
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
</style></head><body>
<div class="container">
  <h1>Media Plan: {client_name}</h1>
  <p class="subtitle">Shared on {created_str}</p>
  <div class="card">
    <h2>Plan Summary</h2>
    <table>{rows_html}</table>
  </div>
  <div class="card">
    <h3>Feedback</h3>
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

    body_bytes = html.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
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
        body = handler._read_body(max_size=50_000)
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
        if _plan_feedback_lock:
            with _plan_feedback_lock:
                if share_id not in _plan_feedback:
                    _plan_feedback[share_id] = []
                _plan_feedback[share_id].append(feedback_entry)
                count = len(_plan_feedback[share_id])
        else:
            if share_id not in _plan_feedback:
                _plan_feedback[share_id] = []
            _plan_feedback[share_id].append(feedback_entry)
            count = len(_plan_feedback[share_id])

        handler._send_json({"ok": True, "feedback_count": count})
    except json.JSONDecodeError:
        handler._send_json({"error": "Invalid JSON"}, status_code=400)
    except Exception as e:
        logger.error("Plan feedback endpoint error: %s", e, exc_info=True)
        handler._send_json({"error": "Failed to submit feedback"}, status_code=500)


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_CAMPAIGN_POST_ROUTE_MAP: dict[str, Any] = {
    "/api/campaign/save": _handle_campaign_save,
    "/api/plan/share": _handle_plan_share,
    "/api/plan/feedback": _handle_plan_feedback,
}
