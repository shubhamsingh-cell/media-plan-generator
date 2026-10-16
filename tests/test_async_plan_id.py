"""Async generations are retrievable by plan id, like sync ones (2026-10-16).

Pre-fix only the sync /api/generate handler called ``_store_plan_result``
and sent ``X-Plan-Id``. Every wizard plan goes through the async worker
(``_run_async_generate``, X-Async: true), so /api/plan-results/<id>, the
dashboard and CSV/XLSX export by ``plan_id`` (routes/export.py reads
``_plan_result_lookup(plan_id)["data"]``) had nothing for a real plan.

Now the worker stores the plan under the job id BEFORE the job reads
"completed", and the /api/jobs/<id> download carries ``X-Plan-Id: <job id>``
while that stored result is live.

End to end against a real ``app.ThreadedHTTPServer``; Supabase is faked and
the conftest network guard keeps every external call local.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402
from tests.test_async_plan_events_logging import (  # noqa: E402,F401
    _COOKIE_HEADERS,
    _FakeSupabase,
    _http,
    _isolated_request_log,
    _join_worker,
    _poll_until_done,
    _submit_async,
    live_server,
)

# The fields routes/export.py's CSV/XLSX export reads from the stored plan.
_EXPORT_CHANNEL_KEYS = {
    "name",
    "allocation_pct",
    "budget",
    "cpc_range",
    "cpa_range",
    "projected_clicks",
    "recommended_reason",
}


@pytest.fixture
def fake_supabase():
    fake = _FakeSupabase()
    with mock.patch("supabase_client.get_client", return_value=fake):
        yield fake


def test_async_plan_is_stored_under_its_job_id_and_named_on_download(
    live_server, fake_supabase
):
    job_id = _submit_async(live_server, "plan-id")
    result = _poll_until_done(live_server, job_id)
    assert result["status"] == "completed", result

    # Stored before "completed" was published: no join, no race.
    entry = app_module._plan_result_lookup(job_id)
    assert entry is not None, "async plan not stored under its job id"
    assert time.time() - entry["created"] < 3600
    plan = entry["data"]
    channels = plan.get("channels") or []
    assert channels and all(_EXPORT_CHANNEL_KEYS <= set(c) for c in channels)
    assert (plan.get("metadata") or {}).get("total_budget")

    status, headers, body = _http(
        live_server, "GET", f"/api/jobs/{job_id}", headers=dict(_COOKIE_HEADERS)
    )
    _join_worker(job_id)
    assert status == 200 and body[:2] == b"PK", (status, body[:80])
    assert headers.get("X-Plan-Id") == job_id, headers
    assert "X-Plan-Id" in (headers.get("Access-Control-Expose-Headers") or "")

    # The id resolves where the sync X-Plan-Id resolves.
    status, _h, _b = _http(
        live_server, "GET", f"/api/plan-results/{job_id}", headers=dict(_COOKIE_HEADERS)
    )
    assert status == 200


def test_live_plan_id_only_names_a_stored_unexpired_plan(monkeypatch):
    stored = {"data": {"channels": []}, "created": time.time()}
    monkeypatch.setattr(app_module, "_plan_result_lookup", lambda pid: stored)
    assert app_module._live_plan_id("a" * 32) == "a" * 32
    stored["created"] = time.time() - app_module._PLAN_RESULTS_TTL_SECONDS - 5
    assert app_module._live_plan_id("a" * 32) == ""
    monkeypatch.setattr(app_module, "_plan_result_lookup", lambda pid: None)
    assert app_module._live_plan_id("b" * 32) == ""

    def _broken(pid):
        raise OSError("shared-state read failed")

    monkeypatch.setattr(app_module, "_plan_result_lookup", _broken)
    assert app_module._live_plan_id("c" * 32) == ""  # never breaks a download
