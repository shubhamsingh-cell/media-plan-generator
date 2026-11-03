"""Warehouse and gig/courier role categories, and App Install as a goal.

Backlog item (handoff 2026-10-23 section 4 item 4): the standardizer already
maps warehouse associates to SOC 53-7062 and couriers / gig delivery to
53-3033 / 43-5021, but the wizard's category picker had no "warehouse" or
"gig / courier" choice and the goal list had no app-install conversion.

Covered here:
  * wizard_inputs.ROLE_CATEGORIES defines the two categories once (label,
    SOC family, role hints) and every SOC agrees with the standardizer;
  * the picker's source (data/channels_db.json job_categories) lists both,
    under the same labels, built from existing repository channel lists;
  * "App Install" is a campaign goal in wizard_inputs.CAMPAIGN_GOALS and the
    wizard's checkbox group lists exactly those goals;
  * /api/generate validation keeps known category keys, drops unknown ones
    with a warning and canonicalises goal spellings;
  * the industry classifier and the role SOC lookup read the category, so a
    vague title ("Rider") with a gig/courier pick resolves to the logistics
    profile and SOC 53-3033 instead of the generic entry-level default, on
    the preview estimate and on generate alike;
  * the preview estimate body carries job_categories exactly as generate.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest import mock

import pytest

import app
import standardizer
import wizard_inputs
from tests.wizard_js_harness import NODE, PARTIALS, wizard_payloads

ROOT = Path(__file__).resolve().parent.parent
CONTENT = PARTIALS / "body_content.html"
CHANNELS_DB = ROOT / "data" / "channels_db.json"

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def test_categories_are_defined_once_with_standardizer_soc_codes():
    cats = wizard_inputs.ROLE_CATEGORIES
    assert cats["warehouse_logistics"]["label"] == "Warehouse & Fulfillment"
    assert cats["gig_courier_delivery"]["label"] == "Courier / Delivery Drivers"
    assert cats["warehouse_logistics"]["soc"] == ("53-7062",)
    assert cats["gig_courier_delivery"]["soc"] == ("53-3033", "43-5021")
    for key, cat in cats.items():
        hint_socs = {standardizer.get_soc_code(h) for h in cat["role_hints"]}
        assert hint_socs == set(cat["soc"]), key


def test_the_picker_source_lists_both_categories_under_the_same_labels():
    jc = json.loads(CHANNELS_DB.read_text(encoding="utf-8"))["job_categories"]
    for key, cat in wizard_inputs.ROLE_CATEGORIES.items():
        assert key in jc, f"{key} missing from the wizard's category picker"
        assert jc[key]["label"] == cat["label"]
        assert jc[key]["recommended_channels"]["primary"]
        # no unsourced CPA / time-to-fill figures on the new entries
        assert "avg_cpa_range" not in jc[key]
        assert "avg_time_to_fill" not in jc[key]


def test_app_install_is_a_goal_and_the_checkbox_group_matches():
    assert "App Install" in wizard_inputs.CAMPAIGN_GOALS
    html = CONTENT.read_text(encoding="utf-8")
    group = html[html.index('id="campaignGoalsGroup"') :]
    group = group[: group.index("</div>")]
    values = re.findall(r'name="campaignGoals"\s+value="([^"]+)"', group)
    assert tuple(values) == wizard_inputs.CAMPAIGN_GOALS


def test_generate_validation_keeps_known_categories_and_canonical_goals():
    data = {
        "job_categories": ["gig_courier_delivery", "not_a_category", "warehouse_logistics"],
        "campaign_goals": ["app install", "App_Install", "Direct Hiring", "high_volume"],
    }
    warnings = app._validate_wizard_picks(data)
    assert data["job_categories"] == ["gig_courier_delivery", "warehouse_logistics"]
    assert data["campaign_goals"] == ["App Install", "Direct Hiring", "high_volume"]
    assert any("not_a_category" in w for w in warnings)


def test_a_gig_pick_steers_a_vague_title_off_the_generic_default():
    plain = app.classify_industry("", "Acme", ["Rider"])
    assert plain["legacy_key"] == "general_entry_level"
    gig = {"job_categories": ["gig_courier_delivery"]}
    # blank, "general", the wizard's "General / Entry-Level" pick, and Phase
    # 0's rewrite of a blank pick (original kept under _industry_original)
    for raw, extra in (("", {}), ("general", {}), ("general_entry_level", {}),
                       ("general_entry_level", {"_industry_original": ""})):
        got = app._classify_for_request(raw, "Acme", ["Rider"], dict(gig, **extra))
        assert got["legacy_key"] == "logistics_supply_chain", raw
    wh = app._classify_for_request(
        "", "Acme", ["Associate"], {"job_categories": ["warehouse_logistics"]}
    )
    assert wh["legacy_key"] == "logistics_supply_chain"
    # an explicit industry pick is never overridden by the category
    hc = app._classify_for_request("healthcare_medical", "Acme", ["Rider"], gig)
    assert hc["legacy_key"] == "healthcare_medical"
    # no category: unchanged
    assert app._classify_for_request("", "Acme", ["Rider"], {})["legacy_key"] == (
        "general_entry_level"
    )


def test_a_category_fills_the_soc_code_of_a_title_the_standardizer_cannot_place():
    assert standardizer.get_soc_code("Rider") == ""
    data = {"job_categories": ["gig_courier_delivery"]}
    assert app._role_soc_for_request("Rider", data) == "53-3033"
    assert app._role_soc_for_request("Warehouse Associate", data) == "53-7062"
    assert app._role_soc_for_request("Rider", {}) == ""


def test_the_preview_estimate_classifies_with_the_category_too():
    brief = {
        "budget_range": "50000",
        "campaign_duration": "3 months",
        "campaign_start_month": 12,
        "industry": "",
        "client_name": "Acme",
        "target_roles": ["Rider"],
        "locations": ["Columbus, OH"],
        "target_region": "us_only",
        "job_categories": ["gig_courier_delivery"],
    }
    with mock.patch.object(app, "classify_industry", wraps=app.classify_industry) as spy:
        app._compute_plan_estimate(dict(brief))
    seen = [c.args for c in spy.call_args_list]
    assert seen and all(a[0] == "" and "Delivery Driver" in a[2] for a in seen), seen


@needs_node
def test_the_estimate_body_carries_job_categories_like_generate():
    got = wizard_payloads(
        {"clientName": {"value": "Acme"}, "budgetRange": {"value": "50000"},
         "campaignDuration": {"value": "3 months"}},
        roles=["Rider"], locations=["Columbus, OH"],
        job_categories=["gig_courier_delivery"],
    )
    assert got["generate"]["job_categories"] == ["gig_courier_delivery"]
    assert got["estimate"]["job_categories"] == ["gig_courier_delivery"]


def test_the_new_picker_cards_follow_the_others():
    jc = json.loads(CHANNELS_DB.read_text(encoding="utf-8"))["job_categories"]
    # Title Case like the other 18 cards
    for key in wizard_inputs.ROLE_CATEGORIES:
        label = jc[key]["label"]
        words = [w for w in re.split(r"[ /&]+", label) if w]
        assert all(w[0].isupper() for w in words), label
    # a box for the warehouse card; no two cards share a label
    assert jc["warehouse_logistics"]["icon"] == "\U0001F4E6"
    labels = [c["label"] for c in jc.values()]
    assert len(labels) == len(set(labels))
