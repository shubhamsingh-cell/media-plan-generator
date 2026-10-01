"""The results screen shows the generated plan's own figures or no tile at
all (design-judge panel on mpg-input-parity @f159b3d, 2026-10-01, item 7:
a fabricated figure on a client-facing screen).

Pre-fix the async path always called buildPlanDashboard(payload, null), so:
  - "Avg CPC ~$1.50" came from a hardcoded last-resort fallback (on a £ plan
    too);
  - "Channels Selected 0" counted the untouched channel toggles while the
    plan funded six channels;
  - "Hiring Difficulty Moderate", "Competition Level Medium", "Salary Range
    Varies by role", "Demand Trend Stable" were defaults, and a "Seasonal
    Impact" card printed a typed-in per-quarter table ("Expect 15-20% higher
    costs", "Budget adjustment factor -12%").
The job now returns results_summary (app._plan_results_summary, read from
the engine output the workbook prints) and the screen renders only that.
"""

from __future__ import annotations

import io
import json
import re
from pathlib import Path

import openpyxl
import pytest

import app
import tools_regen_bundles as trb
from tests.wizard_js_harness import APP_JS, NODE, PARTIALS, inputs_functions_js, js_block, run_node

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def uk_plan(tmp_path_factory):
    """A real plan, generated offline exactly as /api/generate builds it:
    request boundary -> budget block -> engine -> deck + workbook."""
    payload = dict(trb.MANPOWER_BRIEF, client_name="Brightwell Logistics UK",
                   budget="£75,000", budget_range="£75,000", campaign_duration="3 months",
                   locations=["London, UK"])
    data = app._sanitize_generate_request(payload)
    app._normalize_request_budget(data, log=False)
    result = trb.generate_bundle(data, tmp_path_factory.mktemp("uk"), "uk")
    assert not result["errors"], result["errors"]
    return result


def _exec_summary_channel_rows(xlsx_bytes: bytes) -> list:
    ws = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=True)["Executive Summary"]
    rows, in_table = [], False
    for r in ws.iter_rows(values_only=True):
        cells = list(r)
        if "Proj. Clicks" in cells:
            in_table = True
            continue
        if in_table:
            if not cells[1] or cells[1] == "Total":
                break
            rows.append({"name": cells[1], "amount": cells[3], "clicks": cells[4]})
    return rows


def test_summary_is_the_workbooks_executive_summary(uk_plan):
    rows = _exec_summary_channel_rows(uk_plan["xlsx_bytes"])
    summary = app._plan_results_summary(uk_plan["data"])
    assert summary["channels"] == [r["name"] for r in rows]
    assert summary["channels_funded"] == len(rows) >= 5
    spend = sum(r["amount"] for r in rows)
    clicks = sum(r["clicks"] for r in rows)
    cpc = summary["blended_cpc"]
    assert cpc["value"] == round(spend / clicks, 2)
    assert cpc["currency"] == "GBP" and cpc["display"] == f"£{cpc['value']:.2f}"


def _render(payload: dict) -> dict:
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            inputs_functions_js(),
            "var out = { innerHTML: '' }, shown = false;",
            "var document = { getElementById: function (id) {",
            "  if (id === 'planDashboardContent') return out;",
            "  if (id === 'planDashboardSection') return { classList: { add: function () { shown = true; } } };",
            "  return null; } };",
            "function novaPlanCurrency() { return null; }",
            # the page's escapeHtml uses a DOM text node; same output for this text
            "function escapeHtml(s) { return String(s).replace(/&/g, '&amp;')"
            ".replace(/</g, '&lt;').replace(/>/g, '&gt;'); }",
            js_block(src, "function novaPlanBudgetLabel("),
            js_block(src, "function buildPlanDashboard("),
            "buildPlanDashboard(" + json.dumps(payload) + ", null);",
            "var html = out.innerHTML;",
            "var tiles = []; html.replace(/plan-metric-label\">([^<]*)<\\/div><div class=\"plan-metric-value[^\"]*\">([^<]*)</g,",
            "  function (_, l, v) { tiles.push([l, v]); });",
            "process.stdout.write(JSON.stringify({ html: html, tiles: tiles, shown: shown }));",
        ]
    )
    return run_node(script)


_BASE = {
    "client_name": "Brightwell Logistics UK", "budget_range": "£75,000",
    "budget_period": "campaign", "campaign_duration": "3 months",
    "locations": ["London, UK"], "target_roles": ["Warehouse Operative", "HGV Driver"],
    "_plan_budget": {"display": "£75,000", "total": 75000, "currency": "GBP"},
}


@needs_node
def test_results_tiles_equal_the_workbook_values(uk_plan):
    summary = app._plan_results_summary(uk_plan["data"])
    rows = _exec_summary_channel_rows(uk_plan["xlsx_bytes"])
    got = _render(dict(_BASE, _plan_results=json.loads(json.dumps(summary))))
    spend = sum(r["amount"] for r in rows)
    clicks = sum(r["clicks"] for r in rows)
    assert got["tiles"] == [
        ["Total Budget", "£75,000"],
        ["Channels Funded", str(len(rows))],
        ["Blended CPC", f"£{spend / clicks:.2f}"],
    ]
    funded = re.search(r'plan-funded-channels">(.*?)</div>', got["html"]).group(1)
    assert [r["name"] for r in rows] == [
        n.strip() for n in re.sub(r"<[^>]+>", "", funded).replace("Funded:", "").split("&middot;")
    ]
    assert got["shown"]


@needs_node
def test_missing_figures_hide_their_tile_never_a_placeholder():
    got = _render(dict(_BASE, _plan_results=None))
    assert got["tiles"] == [["Total Budget", "£75,000"]]
    html = got["html"]
    for invented in ("~$1.50", "Avg CPC", "Channels Selected", "Hiring Difficulty",
                     "Varies by role", "Competition Level", "Demand Trend",
                     "Seasonal Impact", "adjustment factor", "15-20%", "role(s)"):
        assert invented not in html, invented
    assert "Targeting 2 roles across 1 location." in html  # real counts, plain English


def test_summary_is_empty_without_engine_output():
    assert app._plan_results_summary({}) == {}
    no_clicks = {"_budget_allocation": {"channel_allocations": {
        "global_boards": {"dollar_amount": 5000, "projected_clicks": 0}}}}
    s = app._plan_results_summary(no_clicks)
    assert s == {"channels": ["Global Job Boards"], "channels_funded": 1}  # no CPC tile


def test_async_job_carries_the_summary_on_every_poll_path():
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    assert "_results_summary = _plan_results_summary(gen_data)" in src
    assert '"results_summary": _results_summary,' in src  # job record
    mirror = src[src.index("def _mirror_job(") :]
    mirror = mirror[: mirror.index("\ndef ")]
    assert 'snapshot["results_summary"] = _results_summary' in mirror  # cross-worker
    assert '"results_summary": job.get("results_summary") or {},' in src  # in-process poll
    assert '"results_summary": _mirror_data.get(' in src  # mirror poll
    assert src.count('"results_summary": {},') >= 1  # Supabase fallback: hidden tiles


def test_cross_worker_record_carries_the_summary(tmp_path, monkeypatch):
    """A poll answered by another gunicorn worker reads the shared job
    record: it must render the same tiles (the kill-switch legacy mirror
    keeps its byte-identical whitelist -- tests/test_shared_state_app.py)."""
    import time
    import uuid

    monkeypatch.setattr(app._generate_slots, "_slot_dir", str(tmp_path))
    for var in ("NOVA_STATE_SUPABASE_URL", "NOVA_STATE_SUPABASE_KEY", "SUPABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    summary = {"channels": ["Global Job Boards"], "channels_funded": 1,
               "blended_cpc": {"value": 0.8, "currency": "GBP", "display": "£0.80"}}
    job_id = uuid.uuid4().hex
    with app._generation_jobs_lock:
        app._generation_jobs[job_id] = {
            "status": "completed", "progress_pct": 100, "created": time.time(),
            "result_bytes": b"PK\x03\x04zip", "result_filename": "p.zip",
            "result_content_type": "application/zip", "_session_token": "tok",
            "results_summary": summary,
        }
    try:
        app._mirror_job(job_id)
        assert app._job_record_get(job_id)["results_summary"] == summary
    finally:
        with app._generation_jobs_lock:
            app._generation_jobs.pop(job_id, None)


def test_wizard_reads_the_summary_from_the_completed_poll():
    gen = js_block(APP_JS.read_text(encoding="utf-8"), "async function generatePlan(")
    assert "payload._plan_results =" in gen and "_completedPollData.results_summary" in gen


def test_saved_plan_comparison_has_no_invented_cpc():
    footer = (PARTIALS / "body_footer_scripts.html").read_text(encoding="utf-8")
    assert "~$1.50" not in footer
    assert "~$1.50" not in APP_JS.read_text(encoding="utf-8")
