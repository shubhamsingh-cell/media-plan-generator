"""Tests for the S89 call_llm_json adoption across app.py LLM sites.

Covers _copilot_suggest_brief (array), _analyze_compliance (object + narrative
fallback) and _generate_post_campaign_summary (object + narrative fallback).
Each gets its `router` from app._lazy_llm_router(), so we patch that to return
a fake router whose call_llm_json is a stub -- keeping the tests offline and
deterministic while exercising the real mapping + fallback logic.
"""

import types

import app


def _fake_router(result):
    def _call_llm_json(**kwargs):
        # Every adoption must pass a JSON-Schema dict.
        assert isinstance(kwargs.get("schema"), dict), "schema passed"
        return result
    return types.SimpleNamespace(
        call_llm_json=_call_llm_json,
        TASK_PLAN_STRUCTURED="plan_structured",
    )


# ── _copilot_suggest_brief (array) ──────────────────────────────────────────
_BRIEF = "We need to hire 200 warehouse associates in Texas by Q3, big turnover."


def test_copilot_suggest_brief_ok_list(monkeypatch):
    res = {
        "ok": True,
        "data": [
            {"text": "Add a timeline", "confidence": 0.9, "reason": "enables seasonality"},
            {"text": "x" * 200, "confidence": "0.5", "reason": "y" * 200},
            {"no_text": "dropped"},  # no "text" -> skipped
            {"text": "4th", "confidence": 0.7, "reason": "over cap -> dropped"},
        ],
        "provider": "gemini",
        "error": "",
    }
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    out = app._copilot_suggest_brief(_BRIEF, {"industry": "logistics"})
    assert isinstance(out, list) and len(out) == 2  # 3rd skipped (no text), 4th over cap
    assert out[0]["text"] == "Add a timeline"
    assert len(out[1]["text"]) == 80 and len(out[1]["reason"]) == 120  # truncation
    assert out[1]["confidence"] == 0.5  # str coerced to float


def test_copilot_suggest_brief_not_ok_returns_empty(monkeypatch):
    res = {"ok": False, "data": None, "provider": "groq", "error": "parse fail"}
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    assert app._copilot_suggest_brief(_BRIEF, {}) == []


def test_copilot_suggest_brief_short_input_skips_llm(monkeypatch):
    monkeypatch.setattr(
        app, "_lazy_llm_router",
        lambda: (_ for _ in ()).throw(AssertionError("must not fetch router")),
    )
    out = app._copilot_suggest_brief("short", {})  # < 20 chars -> heuristic
    assert isinstance(out, list) and len(out) == 3


# ── _analyze_compliance (object + narrative + error) ─────────────────────────
_COMP = {"text": "Seeking a young, energetic recent grad.", "industry": "tech"}


def test_analyze_compliance_ok(monkeypatch):
    res = {
        "ok": True,
        "data": {"score": 72, "issues": [{"phrase": "young"}], "recommendations": ["fix age language"]},
        "provider": "gemini", "error": "",
    }
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    out = app._analyze_compliance(dict(_COMP))
    assert out["score"] == 72
    assert out["issues"] == [{"phrase": "young"}]
    assert out["recommendations"] == ["fix age language"]
    assert out["llm_provider"] == "gemini"


def test_analyze_compliance_non_json_wraps_raw_as_narrative(monkeypatch):
    res = {"ok": False, "data": None, "raw": "Here is my prose analysis...", "provider": "groq", "error": "no json"}
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    out = app._analyze_compliance(dict(_COMP))
    assert out["score"] == 50
    assert out["recommendations"] == ["Here is my prose analysis..."]
    assert out["llm_provider"] == "groq"


def test_analyze_compliance_empty_response_error_result(monkeypatch):
    res = {"ok": False, "data": None, "raw": "", "provider": "", "error": "down"}
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    out = app._analyze_compliance(dict(_COMP))
    assert out["score"] == 0 and out["llm_available"] is False


def test_analyze_compliance_no_text_skips_llm(monkeypatch):
    monkeypatch.setattr(
        app, "_lazy_llm_router",
        lambda: (_ for _ in ()).throw(AssertionError("must not fetch router")),
    )
    out = app._analyze_compliance({"text": ""})
    assert out["error"] == "Job description text is required"


# ── _generate_post_campaign_summary (object + narrative) ─────────────────────
_CAMP = {"channels": [{"name": "Indeed", "spend": 5000}], "spend": 5000, "hires": 3}


def test_post_campaign_summary_ok(monkeypatch):
    res = {
        "ok": True,
        "data": {"executive_summary": "ES with $5000", "channel_analysis": "CA", "recommendations": "REC"},
        "provider": "gemini", "error": "",
    }
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    out = app._generate_post_campaign_summary(dict(_CAMP))
    assert out["executive_summary"] == "ES with $5000"
    assert out["channel_analysis"] == "CA"
    assert out["recommendations"] == "REC"
    assert out["llm_provider"] == "gemini"


def test_post_campaign_summary_non_json_wraps_raw(monkeypatch):
    res = {"ok": False, "data": None, "raw": "prose summary", "provider": "groq", "error": "no json"}
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: _fake_router(res))
    out = app._generate_post_campaign_summary(dict(_CAMP))
    assert out["executive_summary"] == "prose summary"
    assert out["channel_analysis"] == ""
    assert out["llm_provider"] == "groq"
