"""bundle_qa never calls an internal-draft plan "clean".

Handoff 2026-10-23 section 4 item 11: the deck and workbook stamp a plan
whose data support is below plan_validator.INTERNAL_DRAFT_BELOW_PCT (or that
has a core section at 0%) as an internal draft, yet the delivery gate still
reported qa_status "clean". The gate now carries an internal_draft_plan
finding whose message is the stamp itself (that it is a draft, and why).
"""

from __future__ import annotations

import bundle_qa
import plan_validator


def _draft(overall=0.31, sections=None):
    scores = {"overall": overall}
    if sections is not None:
        scores["sections"] = sections
    return {"client_name": "Draft Co", "_synthesized": {"confidence_scores": scores}}


def _draft_findings(findings):
    return [f for f in findings if f.get("code") == "internal_draft_plan"]


def test_a_low_confidence_plan_is_reported_as_an_internal_draft_with_the_reason():
    data = _draft(0.31)
    found = _draft_findings(bundle_qa.run_bundle_qa(None, None, data))
    assert len(found) == 1
    assert found[0]["message"] == plan_validator.internal_draft_stamp(data)
    assert "data confidence 31%" in found[0]["message"]
    summary = bundle_qa.summarize_findings(bundle_qa.run_bundle_qa(None, None, data))
    assert summary["qa_status"] != "clean"


def test_a_zero_core_section_is_the_reason_when_overall_is_high():
    data = _draft(0.8, {"salary_intelligence": 0.0, "location_profiles": 0.9})
    found = _draft_findings(bundle_qa.run_bundle_qa(None, None, data))
    assert found and "salary data missing (0%)" in found[0]["message"]


def test_a_supported_plan_carries_no_draft_finding():
    assert _draft_findings(bundle_qa.run_bundle_qa(None, None, _draft(0.82))) == []
    assert _draft_findings(bundle_qa.run_bundle_qa(None, None, {})) == []
