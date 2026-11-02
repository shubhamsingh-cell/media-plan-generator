"""Input-role integrity gate: every role the user typed reaches the plan.

Reviewer case (2026-10-16): a 4-role maintenance brief reached the generator
with 3 lower-cased roles; the fourth (a battery supervisor) silently
vanished and the bundle still shipped "clean". The fix:

  1. ``/api/generate`` logs the raw roles at intake (count + each role) and
     carries them as ``data["input_roles"]`` to the bundle gate.
  2. ``bundle_qa`` raises a CRITICAL ``input_role_missing`` finding, naming
     the role, when an input role is absent from the plan's role set, the
     deck or the workbook. Matching is case-insensitive and goes through
     the engine's standardizer (``standardizer.normalize_role``), one plan
     role per input role, so two supervisor titles that standardize to the
     same key cannot both be satisfied by one surviving title.
  3. The verdict can never read "clean" while a role is missing, including
     when the gate times out or crashes.
"""

from __future__ import annotations

import io
import logging
import sys
import time
from pathlib import Path
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent
for _p in (PROJECT_ROOT, TESTS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import app as app_module  # noqa: E402
import bundle_qa  # noqa: E402

FOUR_ROLES = [
    "Maintenance Technician",
    "Battery Supervisor",
    "Drive Unit Supervisor",
    "Electrician",
]


def _deck(*texts: str) -> bytes:
    from pptx import Presentation
    from pptx.util import Inches

    prs = Presentation()
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    for i, t in enumerate(texts):
        box = slide.shapes.add_textbox(
            Inches(0.5), Inches(0.5 + i), Inches(8), Inches(0.8)
        )
        box.text_frame.paragraphs[0].add_run().text = t
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def _workbook(*texts: str) -> bytes:
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Roles"
    for i, t in enumerate(texts, start=1):
        ws.cell(row=i, column=1, value=t)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def _role_findings(findings):
    return [f for f in findings if f.get("code") == "input_role_missing"]


# ---------------------------------------------------------------------------
# bundle_qa check
# ---------------------------------------------------------------------------
def test_dropped_role_is_a_critical_naming_the_role_on_every_surface():
    survivors = ["maintenance technician", "drive unit supervisor", "electrician"]
    data = {"input_roles": list(FOUR_ROLES), "target_roles": survivors}
    findings = bundle_qa.run_bundle_qa(
        _deck(*survivors), _workbook(*survivors), data
    )
    missing = _role_findings(findings)
    assert missing, "a dropped input role produced no finding"
    assert all(f["severity"] == "critical" for f in missing)
    assert {f["location"] for f in missing} == {"plan", "deck", "workbook"}
    assert all("Battery Supervisor" in f["message"] for f in missing)
    summary = bundle_qa.summarize_findings(findings)
    assert summary["qa_status"] == "critical"
    assert "input_role_missing" in summary["codes"]


def test_lower_cased_roles_that_all_survive_pass():
    lowered = [r.lower() for r in FOUR_ROLES]
    data = {"input_roles": list(FOUR_ROLES), "target_roles": lowered}
    findings = bundle_qa.run_bundle_qa(_deck(*lowered), _workbook(*lowered), data)
    assert _role_findings(findings) == []


def test_two_titles_with_one_standardized_key_need_two_plan_roles():
    # Both supervisor titles standardize to the same canonical key; one
    # surviving title must not satisfy both.
    import standardizer

    assert standardizer.normalize_role("Battery Supervisor") == (
        standardizer.normalize_role("Drive Unit Supervisor")
    )
    data = {
        "input_roles": ["Battery Supervisor", "Drive Unit Supervisor"],
        "target_roles": ["Drive Unit Supervisor"],
    }
    findings = bundle_qa.run_bundle_qa(None, None, data)
    missing = _role_findings(findings)
    assert [f["location"] for f in missing] == ["plan"]
    assert "Battery Supervisor" in missing[0]["message"]


def test_standardized_title_satisfies_the_typed_alias():
    data = {"input_roles": ["RN"], "target_roles": ["Registered Nurse"]}
    findings = bundle_qa.run_bundle_qa(
        _deck("Registered Nurse hiring plan"), _workbook("Registered Nurse"), data
    )
    assert _role_findings(findings) == []


def test_plans_without_an_intake_list_are_not_checked():
    findings = bundle_qa.run_bundle_qa(None, None, {"target_roles": ["Electrician"]})
    assert _role_findings(findings) == []


def test_role_dicts_at_intake_are_read_by_title():
    data = {
        "input_roles": [{"title": "Electrician", "count": 3}],
        "target_roles": [{"title": "Electrician", "count": 3}],
    }
    assert _role_findings(bundle_qa.run_bundle_qa(None, None, data)) == []


def test_gate_timeout_still_reports_a_missing_role_as_critical():
    data = {"input_roles": list(FOUR_ROLES), "target_roles": FOUR_ROLES[:3]}

    def _slow(*_a, **_k):
        time.sleep(1.5)
        return []

    with mock.patch.object(bundle_qa, "run_bundle_qa", side_effect=_slow):
        gate = bundle_qa.gate_bundle(None, None, data, budget_s=0.05)
    summary = gate["summary"]
    assert summary["qa_status"] != "clean"
    assert summary["critical_count"] >= 1
    assert "input_role_missing" in summary["codes"]


def test_gate_crash_still_reports_a_missing_role_as_critical():
    data = {"input_roles": list(FOUR_ROLES), "target_roles": FOUR_ROLES[:3]}
    with mock.patch.object(bundle_qa, "run_bundle_qa", side_effect=RuntimeError("x")):
        _p, _x, summary = app_module._run_bundle_qa_gate(None, None, data, "job-x")
    assert summary is not None, "a crashed gate hid a missing role as 'clean'"
    assert summary["qa_status"] == "critical"
    assert "input_role_missing" in summary["codes"]


# ---------------------------------------------------------------------------
# /api/generate intake -> gate (real server, real async worker)
# ---------------------------------------------------------------------------
from test_bundle_qa_gate import (  # noqa: E402
    _COOKIE_HEADERS,
    _GEN_PAYLOAD,
    _ORIGIN_HEADERS,
    _http,
    _poll_until_done,
    _reset_generate_rate_limit,
    _wait_for_a_free_generate_slot,
    live_server,  # noqa: F401 -- pytest fixture
)


def test_a_role_dropped_after_intake_fails_the_bundle_loudly(
    live_server, caplog  # noqa: F811
):
    """A 4-role brief whose fourth role a (monkeypatched) standardizer step
    drops after intake: the intake log names all four, and the job's QA
    verdict is critical with the dropped role named."""
    import json

    real_normalize = app_module._normalize_dict_roles

    def _dropping_standardizer(data):
        real_normalize(data)
        for key in ("target_roles", "roles"):
            if isinstance(data.get(key), list) and len(data[key]) == 4:
                data[key] = [r.lower() for r in data[key][:3]]

    payload = dict(_GEN_PAYLOAD)
    payload["client_name"] = "Role Integrity Test Co"
    # the same intake also validates the wizard's category picks (pinned below)
    payload["job_categories"] = ["warehouse_logistics", "not_a_category"]
    payload["target_roles"] = [
        "Maintenance Technician",
        "Drive Unit Supervisor",
        "Electrician",
        "Battery Supervisor",
    ]
    headers = dict(_ORIGIN_HEADERS)
    headers.update(_COOKIE_HEADERS)
    headers["X-Async"] = "true"
    _reset_generate_rate_limit()
    _wait_for_a_free_generate_slot()
    caplog.set_level(logging.INFO)
    with mock.patch.object(app_module, "_normalize_dict_roles", _dropping_standardizer):
        status, _, raw = _http(
            live_server, "POST", "/api/generate", body=payload, headers=headers
        )
        assert status == 200, raw[:400]
        result = _poll_until_done(live_server, json.loads(raw)["job_id"], timeout=60)

    picks = [r.getMessage() for r in caplog.records if "Unknown job categories" in r.getMessage()]
    assert picks and "not_a_category" in picks[-1], "intake did not validate the picks"
    intake = [r.getMessage() for r in caplog.records if "intake roles" in r.getMessage()]
    assert intake, "no intake roles log line"
    assert "4 role(s)" in intake[-1]
    assert "Battery Supervisor" in intake[-1]

    assert result["status"] == "completed"
    assert result["qa_status"] == "critical"
    assert "input_role_missing" in result["qa_codes"]
    msgs = [
        f.get("message") or ""
        for f in result.get("qa_findings") or []
        if f.get("code") == "input_role_missing"
    ]
    assert msgs and all("Battery Supervisor" in m for m in msgs)


def test_the_app_fallback_reports_a_missing_role_when_the_gate_itself_raises():
    data = {"input_roles": list(FOUR_ROLES), "target_roles": FOUR_ROLES[:3]}
    with mock.patch.object(bundle_qa, "gate_bundle", side_effect=RuntimeError("x")):
        _p, _x, summary = app_module._run_bundle_qa_gate(None, None, data, "job-y")
    assert summary is not None, "a raising gate hid a missing role as 'clean'"
    assert summary["qa_status"] == "critical"
    assert "input_role_missing" in summary["codes"]


def test_client_cannot_supply_its_own_intake_list():
    data = {"target_roles": ["Electrician"], "input_roles": []}
    app_module._capture_input_roles(data)
    assert data["input_roles"] == ["Electrician"]
