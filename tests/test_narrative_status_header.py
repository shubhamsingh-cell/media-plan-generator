"""S94: X-Narrative-Status diagnostic response header (app.py).

Context: excel_v2.py sets ``data["_narrative_status"]`` on every Executive
Strategic Summary generation attempt (see
tests/test_excel_narrative_grounding.py for the grounding validator that
produces it). When the LLM-authored narrative is rejected as fabricated
(``status == "llm_rejected_fabrication"``) or falls back to a deterministic
template (``status == "fallback_template"``), the bundle still generates
successfully (``generated=True``) -- so the untraceable figures the live
model cited were previously visible only in Render server logs, not to
anyone verifying a generated bundle from outside.

This file covers the fix:
  1. ``app._build_narrative_status_header`` -- the pure helper that turns a
     ``_narrative_status`` dict into a compact, single-line,
     latin-1/ASCII-safe JSON string suitable for an HTTP response header
     (used on the sync ``/api/generate`` ZIP response's
     ``X-Narrative-Status`` header).
  2. End-to-end: excel_v2.generate_excel_v2 with a mocked
     ``llm_router.call_llm`` returning the exact fabrication shapes from the
     bug report (reusing the same mocking pattern as
     test_excel_narrative_grounding.py's rejection test) -- the resulting
     ``data["_narrative_status"]`` round-trips through the header builder
     and back to a dict via ``json.loads`` with the untraceable figures
     intact.

Runs under pytest, or standalone: ``python3 tests/test_narrative_status_header.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import excel_v2  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixture (mirrors test_excel_narrative_grounding.py's
# _minimal_data_with_allocation -- duplicated here rather than
# cross-imported so this file has no coupling to that module's internals).
# ---------------------------------------------------------------------------
def _minimal_data_with_allocation(**overrides) -> dict:
    data = {
        "client_name": "Acme Corp",
        "company_name": "Acme Corp",
        "industry": "logistics_supply_chain",
        "budget": "$150,000",
        "locations": ["Dallas, TX"],
        "roles": ["CDL Driver"],
        "target_roles": ["CDL Driver"],
        "campaign_duration": "6 months",
        "hire_volume": "400",
        "work_environment": "onsite",
        "_enriched": {},
        "_synthesized": {},
        "_budget_allocation": {
            "sufficiency": {"grade": "B"},
            "channel_allocations": {
                "programmatic_dsp": {
                    "dollar_amount": 90000,
                    "percentage": 60,
                    "projected_clicks": 45000,
                    "projected_applications": 3600,
                    "projected_hires": 210,
                    "cpc": 2.0,
                    "cpa": 25.0,
                    "roi_score": 8,
                },
                "niche_boards": {
                    "dollar_amount": 60000,
                    "percentage": 40,
                    "projected_clicks": 20000,
                    "projected_applications": 2000,
                    "projected_hires": 132,
                    "cpc": 3.0,
                    "cpa": 30.0,
                    "roi_score": 7,
                },
            },
        },
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# A. app._build_narrative_status_header in isolation
# ---------------------------------------------------------------------------
def test_none_and_empty_input_yield_empty_json_object():
    assert app._build_narrative_status_header(None) == "{}"
    assert app._build_narrative_status_header({}) == "{}"


def test_header_is_valid_single_line_json_with_expected_fields():
    status = {
        "generated": True,
        "status": "llm_rejected_fabrication",
        "reason": "untraceable figures: $5,000; 22%",
        "untraceable_figures": ["$5,000", "22%"],
        "provider": "deepseek",
        "model": "deepseek-v4-flash",
    }
    header = app._build_narrative_status_header(status)
    assert "\n" not in header and "\r" not in header
    parsed = json.loads(header)
    assert parsed["status"] == "llm_rejected_fabrication"
    assert parsed["reason"] == "untraceable figures: $5,000; 22%"
    assert parsed["untraceable_figures"] == ["$5,000", "22%"]
    assert parsed["provider"] == "deepseek"
    assert parsed["model"] == "deepseek-v4-flash"
    # Only the diagnostic fields are carried -- never the narrative text.
    assert "text" not in parsed
    assert "narrative" not in parsed


def test_header_never_carries_narrative_text_even_if_present_on_status():
    """Defense in depth: even if a caller passed a status dict that also had
    the full narrative text on it, the header builder must not leak it --
    it only ever reads the five allow-listed fields."""
    status = {
        "status": "llm_grounded",
        "reason": None,
        "untraceable_figures": [],
        "provider": "deepseek",
        "model": "deepseek-v4-pro",
        "text": "This is the full 2000-character narrative that must never "
        "appear in an HTTP response header...",
    }
    header = app._build_narrative_status_header(status)
    assert "narrative that must never appear" not in header


def test_untraceable_figures_capped_at_ten():
    figures = [f"${i},000" for i in range(25)]
    status = {"status": "llm_rejected_fabrication", "untraceable_figures": figures}
    header = app._build_narrative_status_header(status)
    parsed = json.loads(header)
    assert len(parsed["untraceable_figures"]) == 10
    assert parsed["untraceable_figures"] == figures[:10]


def test_header_is_ascii_safe_for_non_ascii_input():
    """HTTP header values must be latin-1/ASCII-safe; non-ASCII characters
    (e.g. from a model response) must be backslash-escaped, not raise or
    silently corrupt the header."""
    status = {
        "status": "llm_rejected_fabrication",
        "reason": "citation includes café and — an em dash",
        "untraceable_figures": ["€5,000"],
        "provider": "deepseek",
        "model": "deepseek-v4-flash",
    }
    header = app._build_narrative_status_header(status)
    header.encode("ascii")  # must not raise
    assert "\\u00e9" in header or "caf" in header  # backslashreplace-escaped


def test_header_is_capped_to_roughly_800_chars():
    status = {
        "status": "llm_rejected_fabrication",
        "reason": "x" * 5000,
        "untraceable_figures": [f"${i}" for i in range(10)],
        "provider": "deepseek",
        "model": "deepseek-v4-flash",
    }
    header = app._build_narrative_status_header(status)
    assert len(header) <= 800


def test_unserializable_status_falls_back_to_empty_object():
    class _NotJsonable:
        pass

    status = {"status": "llm_grounded", "reason": _NotJsonable()}
    assert app._build_narrative_status_header(status) == "{}"


# ---------------------------------------------------------------------------
# B. End-to-end: rejection produced by generate_excel_v2 survives the header
#    round-trip (reuses the same mocking pattern as
#    test_excel_narrative_grounding.py's fabrication-rejection test).
# ---------------------------------------------------------------------------
def test_rejected_narrative_status_round_trips_through_header():
    data = _minimal_data_with_allocation()
    fabricated_text = (
        "The industry average cost-per-hire for logistics roles is $5,000, "
        "and against that benchmark this plan achieves a 1:2.4 "
        "cost-to-value ratio, generating $360,000 in tangible value. "
        "Deploying at scale closes a 22% supply gap in this market via a "
        "38% reduction in time-to-fill, and every unfilled seat otherwise "
        "costs roughly $7,500 in lost revenue."
    )

    def _fake_call_llm(**kwargs):
        return {
            "text": fabricated_text,
            "provider": "deepseek",
            "model": "deepseek-v4-flash",
            "attempts": [],
        }

    with mock.patch("llm_router.call_llm", side_effect=_fake_call_llm):
        excel_v2.generate_excel_v2(data)

    status = data.get("_narrative_status")
    assert status is not None
    assert status["generated"] is True
    assert status["status"] == "llm_rejected_fabrication"

    header = app._build_narrative_status_header(status)
    assert "\n" not in header and "\r" not in header
    assert len(header) <= 800

    parsed = json.loads(header)
    assert parsed["status"] == "llm_rejected_fabrication"
    assert parsed["provider"] == "deepseek"
    assert parsed["model"] == "deepseek-v4-flash"
    # The exact untraceable figures the live model cited must be observable
    # from the header -- this is the whole point of S94.
    for fig in ("$5,000", "1:2.4", "$360,000", "22%", "38%", "$7,500"):
        assert fig in parsed["untraceable_figures"]
    assert "reason" in parsed and "untraceable figures" in parsed["reason"]
    # The fabricated narrative text itself must never leak into the header.
    assert fabricated_text not in header


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
