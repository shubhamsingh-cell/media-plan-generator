"""S94: reliability tests for the Excel deliverable's Executive Strategic
Summary (LLM-generated narrative).

Root cause this guards against: the narrative call used to instantiate
``llm_router.LLMRouter()`` -- a class llm_router.py has never exported (every
other call site in the codebase uses the module-level ``call_llm()``
function directly). That raised an AttributeError on every single
invocation, which was swallowed by the code's own broad ``except
Exception``, so the narrative NEVER generated in ANY environment --
independent of API keys, timeouts, or provider latency. Confirmed against a
real prod-generated bundle: no "Executive Strategic Summary" section
anywhere in the workbook.

These tests exercise the fixed code path with a mocked ``llm_router.call_llm``
(no network) and assert:
  1. A successful, grounded call renders the LLM narrative section
     (``_narrative_status["status"] == "llm_grounded"``).
  2. Every LLM failure mode (import failure, empty response, exception) now
     renders a DETERMINISTIC, real-data-only fallback summary instead of
     silently skipping the section -- ``_narrative_status`` records
     ``status == "fallback_template"`` with the original failure reason, and
     ``generated`` stays True (app.py's job-record/audit-log block only
     treats ``generated is False`` as "nothing rendered").
  3. The call is actually routed through TASK_PLAN_NARRATIVE with the raised
     25s timeout budget (was 10s).

See tests/test_excel_narrative_grounding.py for the grounding-validator unit
tests and the fabrication-rejection -> deterministic-fallback path (house
rule: never fabricate data).

Runs under pytest, or standalone: ``python3 tests/test_excel_narrative_reliability.py``.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402

import excel_v2  # noqa: E402
import llm_router  # noqa: E402


def _minimal_data(**overrides) -> dict:
    data = {
        "client_name": "Amerigas Test",
        "company_name": "Amerigas Test",
        "industry": "logistics_supply_chain",
        "budget": "$150,000",
        "locations": ["Dallas, TX"],
        "roles": ["CDL Driver"],
        "target_roles": ["CDL Driver"],
        "campaign_duration": "3 months",
        "hire_volume": "50",
        "work_environment": "onsite",
        "_enriched": {},
        "_synthesized": {},
        "_budget_allocation": {},
    }
    data.update(overrides)
    return data


def _sheet_text(ws) -> str:
    parts = []
    for row in ws.iter_rows(values_only=True):
        for val in row:
            if val is not None:
                parts.append(str(val))
    return "\n".join(parts)


def _build(data: dict):
    raw = excel_v2.generate_excel_v2(data)
    assert isinstance(raw, (bytes, bytearray)) and len(raw) > 0
    wb = openpyxl.load_workbook(io.BytesIO(raw))
    return wb


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------
def test_narrative_renders_on_success():
    """A successful, grounded call_llm response renders the Executive
    Strategic Summary section verbatim and records status=llm_grounded with
    the provider/model used. The mocked text cites ONLY figures present in
    _minimal_data()'s FACTS (the $150,000 budget, the resolved 12-week
    duration -- "3 months" resolves to the wizard's fixed 12-week bucket,
    display_format.resolve_campaign_weeks, not a strict 52/12 conversion --
    the stated 50-hire goal) -- no invented ratios/percentages/dollar
    values -- so it passes `_narrative_is_grounded` untouched."""
    data = _minimal_data()
    _narrative_text = (
        "This $150,000 plan is built to fill CDL Driver roles across "
        "Dallas, TX over a 12-week campaign. It is calibrated against the "
        "stated hiring goal of 50, though the plan will need meaningful "
        "ramp-up to reach that goal. Key risk: regional driver capacity "
        "remains tight, which could slow early applicant flow. Recommended "
        "next step: launch priority channels within the first two weeks."
    )

    def _fake_call_llm(**kwargs):
        assert kwargs["task_type"] == llm_router.TASK_PLAN_NARRATIVE
        # S94: was 10.0 -- too tight for a real provider round trip.
        assert kwargs["timeout_budget"] == 25.0
        return {
            "text": _narrative_text,
            "provider": "deepseek",
            "model": "deepseek-v4-flash",
            "attempts": [{"provider": "deepseek", "status": "success"}],
        }

    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        wb = _build(data)

    all_text = "\n".join(_sheet_text(ws) for ws in wb.worksheets)
    assert "EXECUTIVE STRATEGIC SUMMARY" in all_text.upper()
    assert _narrative_text in all_text

    status = data.get("_narrative_status")
    assert status == {
        "generated": True,
        "status": "llm_grounded",
        "provider": "deepseek",
        "model": "deepseek-v4-flash",
    }


# ---------------------------------------------------------------------------
# Failure paths -- must never raise, must always render a real-data-only
# deterministic fallback and record why the LLM narrative wasn't used.
# ---------------------------------------------------------------------------
_FALLBACK_SENTENCE = (
    "This recruitment media plan for Amerigas Test with a $150,000 budget "
    "over 12 weeks, targeting the Logistics & Supply Chain sector."
)


def test_narrative_absent_records_reason_on_empty_response():
    """All providers exhausted (empty text) -- the section still renders,
    via the deterministic real-data-only fallback (never fabricates), and
    the LLM failure reason + attempted providers are recorded rather than
    silently dropped."""
    data = _minimal_data()

    def _fake_call_llm(**kwargs):
        return {
            "text": "",
            "provider": "",
            "error": "All LLM providers unavailable or failed",
            "attempts": [
                {"provider": "deepseek", "status": "failed", "error": "timeout"}
            ],
        }

    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        wb = _build(data)

    all_text = "\n".join(_sheet_text(ws) for ws in wb.worksheets)
    assert "EXECUTIVE STRATEGIC SUMMARY" in all_text.upper()
    assert _FALLBACK_SENTENCE in all_text

    status = data.get("_narrative_status")
    assert status["generated"] is True
    assert status["status"] == "fallback_template"
    assert status["reason"] == "All LLM providers unavailable or failed"
    assert status["providers_attempted"] == ["deepseek"]


def test_narrative_absent_records_reason_on_exception():
    """A raised exception (e.g. a timeout) is caught, non-fatal, the
    deterministic fallback still renders, and the exception type/message are
    recorded (truncated) instead of a bare skip."""
    data = _minimal_data()

    def _fake_call_llm(**kwargs):
        raise TimeoutError("deadline exceeded")

    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        wb = _build(data)  # must not raise -- generation stays non-fatal

    all_text = "\n".join(_sheet_text(ws) for ws in wb.worksheets)
    assert "EXECUTIVE STRATEGIC SUMMARY" in all_text.upper()
    assert _FALLBACK_SENTENCE in all_text

    status = data.get("_narrative_status")
    assert status["generated"] is True
    assert status["status"] == "fallback_template"
    assert "TimeoutError" in status["reason"]
    assert "deadline exceeded" in status["reason"]


def test_narrative_absent_records_reason_on_import_error():
    """If llm_router itself can't be imported, the deterministic fallback
    still renders and that specific ImportError is recorded rather than a
    generic 'skipped' with no context."""
    data = _minimal_data()

    real_import = __import__

    def _blocking_import(name, *args, **kwargs):
        if name == "llm_router":
            raise ImportError("no module named llm_router (simulated)")
        return real_import(name, *args, **kwargs)

    with mock.patch("builtins.__import__", side_effect=_blocking_import):
        wb = _build(data)

    all_text = "\n".join(_sheet_text(ws) for ws in wb.worksheets)
    assert "EXECUTIVE STRATEGIC SUMMARY" in all_text.upper()
    assert _FALLBACK_SENTENCE in all_text

    status = data.get("_narrative_status")
    assert status["generated"] is True
    assert status["status"] == "fallback_template"
    assert "ImportError" in status["reason"]


def test_narrative_status_always_a_dict_never_missing():
    """Regression guard: _narrative_status must always be set on `data`,
    success or failure, so callers never see a bare KeyError/None where they
    expect the observability contract."""
    data = _minimal_data()

    def _fake_call_llm(**kwargs):
        return {"text": "", "error": "boom", "attempts": []}

    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        _build(data)

    assert isinstance(data.get("_narrative_status"), dict)
    assert "generated" in data["_narrative_status"]


# ---------------------------------------------------------------------------
# Regression guard: LLMRouter must never be imported (used to be the bug)
# ---------------------------------------------------------------------------
def test_llm_router_has_no_llmrouter_class():
    """llm_router.py has never exported a class named LLMRouter -- the S94
    root cause. If someone reintroduces `from llm_router import LLMRouter`
    without adding that class, this documents why it fails."""
    assert not hasattr(llm_router, "LLMRouter")


def test_excel_v2_source_does_not_reference_llmrouter_class():
    """Static guard: excel_v2.py's narrative block must call the
    module-level call_llm() function, not a nonexistent LLMRouter class.
    (Explanatory comments are allowed to mention the old class name --
    only the actual import statement, the load-bearing part, is checked.)"""
    src = (PROJECT_ROOT / "excel_v2.py").read_text()
    assert "import LLMRouter" not in src, (
        "excel_v2.py imports the nonexistent llm_router.LLMRouter class "
        "again -- the narrative call will silently no-op (S94 regression)."
    )


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc}")
    sys.exit(1 if _failures else 0)
