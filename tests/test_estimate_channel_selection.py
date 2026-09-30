"""/api/estimate must fund only the channels the plan will fund.

Hershey plan (2026-09-24): the wizard's live preview called
_compute_plan_estimate, which built channel_pcts from the industry profile
and never applied the user's channel_categories toggles -- so an unticked
channel (Employer Branding) was still funded in the preview while
/api/generate (which runs _apply_channel_selection) zeroed it.
"""

from __future__ import annotations

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app  # noqa: E402

_ROOT = os.path.join(os.path.dirname(__file__), "..")

_BRIEF = {
    "budget": "$90,000",
    "industry": "food_beverage",
    "client_name": "The Hershey Company",
    "roles": ["Machine Operator", "Electrical Technician", "Production Supervisor"],
    "locations": ["Hershey, PA, US"],
}


@pytest.fixture
def captured(monkeypatch):
    seen: dict = {}
    real = app.calculate_budget_allocation

    def _spy(*args, **kwargs):
        seen["channel_percentages"] = dict(kwargs.get("channel_percentages") or {})
        return real(*args, **kwargs)

    monkeypatch.setattr(app, "calculate_budget_allocation", _spy)
    return seen


@pytest.mark.parametrize("off", [False, "False", "false"])
def test_estimate_does_not_fund_an_unselected_channel(captured, off):
    brief = dict(_BRIEF)
    brief["channel_categories"] = {
        "programmatic_dsp": True,
        "global_boards": True,
        "niche_boards": True,
        "social_media": True,
        "regional_boards": True,
        "employer_branding": off,
    }
    app._compute_plan_estimate(brief)
    pcts = captured["channel_percentages"]
    assert (pcts.get("employer_branding") or 0) == 0
    assert abs(sum(pcts.values()) - 100) < 1e-6


def test_estimate_uses_the_same_selection_as_generate(captured):
    brief = dict(_BRIEF)
    brief["channel_categories"] = {"social_media": True, "programmatic_dsp": False}
    app._compute_plan_estimate(brief)
    with_sel = captured["channel_percentages"]
    app._compute_plan_estimate(dict(_BRIEF))
    unfiltered = captured["channel_percentages"]
    assert with_sel == app._apply_channel_selection(
        unfiltered, {"channel_categories": brief["channel_categories"]}
    )
    assert "programmatic_dsp" not in with_sel


def test_estimate_without_selection_is_unchanged(captured):
    app._compute_plan_estimate(dict(_BRIEF))
    assert (captured["channel_percentages"].get("employer_branding") or 0) >= 0
    assert len(captured["channel_percentages"]) >= 5


def test_wizard_preview_sends_channel_categories_to_estimate():
    # 2026-10-01: the preview posts the generate payload's core inputs
    # (novaPlanCoreInputs), which carry channel_categories -- checked on the
    # real builders under node (tests/wizard_js_harness.py).
    from tests.wizard_js_harness import NODE, wizard_payloads

    if NODE is None:
        pytest.skip("node not installed")
    state = {"regionSelector": {"value": "us_only"}, "chSocial": {"checked": True}}
    got = wizard_payloads(state, touch={"chProgrammatic": False})
    assert got["estimate"]["channel_categories"] == got["generate"]["channel_categories"]
    assert got["estimate"]["channel_categories"]["programmatic_dsp"] is False


def test_wizard_preview_refetches_when_channel_toggles_change():
    with open(
        os.path.join(_ROOT, "templates", "partials", "index", "body_preview_js.html"),
        encoding="utf-8",
    ) as fh:
        js = fh.read()
    start = js.index("function fetchEstimate(m)")
    # The refetch signature is the whole request body, channel_categories
    # included, so a toggle change always refetches.
    assert "var sig = JSON.stringify(payload);" in js[start:]
