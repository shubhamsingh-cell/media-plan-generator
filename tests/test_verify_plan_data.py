"""Tests for app._verify_plan_data (S89: call_llm_json adoption).

_verify_plan_data is a non-blocking LLM cross-check of a generated plan. S89
routed it through llm_router.call_llm_json (schema-validated + 1 retry) instead
of a one-shot regex+json.loads. These tests pin the contract: the mapped return
shapes on success and the graceful "skipped" fallbacks -- with call_llm_json
mocked so they stay offline and deterministic.
"""

import llm_router
import app


# Minimal data that gets past the early guards (needs a dict _synthesized and a
# non-empty check_items, which a budget metadata block provides).
_DATA = {
    "_synthesized": {},
    "client_name": "Acme",
    "industry": "healthcare_medical",
    "_budget_allocation": {"metadata": {"total_budget": 30000}},
}


def _stub_call_llm_json(result):
    def _fake(**kwargs):
        # The adoption must pass a schema requiring "verified".
        assert "verified" in kwargs["schema"]["properties"], "schema wired"
        return result

    return _fake


def test_verified_ok(monkeypatch):
    monkeypatch.setattr(
        llm_router,
        "call_llm_json",
        _stub_call_llm_json(
            {
                "ok": True,
                "data": {"verified": True, "issues": [], "severity": "none"},
                "provider": "gemini",
                "error": "",
            }
        ),
    )
    out = app._verify_plan_data(dict(_DATA))
    assert out["status"] == "verified"
    assert out["issues"] == []
    assert out["severity"] == "none"
    assert out["provider"] == "gemini"


def test_issues_found_preserves_payload(monkeypatch):
    monkeypatch.setattr(
        llm_router,
        "call_llm_json",
        _stub_call_llm_json(
            {
                "ok": True,
                "data": {
                    "verified": False,
                    "issues": ["CPC too high", "CPA off"],
                    "severity": "major",
                },
                "provider": "groq",
                "error": "",
            }
        ),
    )
    out = app._verify_plan_data(dict(_DATA))
    assert out["status"] == "issues_found"
    assert out["issues"] == ["CPC too high", "CPA off"]
    assert out["severity"] == "major"
    assert out["provider"] == "groq"


def test_not_ok_falls_back_to_skipped(monkeypatch):
    monkeypatch.setattr(
        llm_router,
        "call_llm_json",
        _stub_call_llm_json(
            {"ok": False, "data": None, "provider": "gemini", "error": "parse fail"}
        ),
    )
    out = app._verify_plan_data(dict(_DATA))
    assert out == {"status": "skipped", "reason": "verification_failed"}


def test_non_dict_data_falls_back_to_skipped(monkeypatch):
    monkeypatch.setattr(
        llm_router,
        "call_llm_json",
        _stub_call_llm_json(
            {"ok": True, "data": ["not", "a", "dict"], "provider": "x", "error": ""}
        ),
    )
    out = app._verify_plan_data(dict(_DATA))
    assert out == {"status": "skipped", "reason": "verification_failed"}


def test_no_data_to_verify_short_circuits_without_llm(monkeypatch):
    # No _budget_allocation -> no check_items -> skip before any LLM call.
    def _boom(**kwargs):
        raise AssertionError("call_llm_json must not be called when there's no data")

    monkeypatch.setattr(llm_router, "call_llm_json", _boom)
    out = app._verify_plan_data({"_synthesized": {}})
    assert out == {"status": "skipped", "reason": "no_data_to_verify"}
