"""The live preview estimates exactly the brief Generate sends (wizard audit
D-04, 2026-10-01).

Pre-fix the preview's /api/estimate body was hand-built: its own
pre-multiplied budget, NO target_region, no start month and the typed NAICS
text as the industry. With international cities and the default "US Only"
region the server log showed estimate `channels=8` (APAC+EMEA funded) while
generate logged `channels=6` + "US-only plan (region=us_only):
redistributed ... from APAC/EMEA" -- the preview and review listed channels
the plan did not fund. /api/estimate also inferred "international" from the
locations instead of honouring the region select.

Both request bodies are now built from one function (novaPlanCoreInputs);
these tests run the REAL builders under node from one form state and
require every estimate field to equal the generate payload's, and require
/api/estimate to fold regional channels by target_region exactly as the
generate paths do (_apply_target_region, shared).
"""

from __future__ import annotations

import re

import pytest

import app
from tests.wizard_js_harness import APP_JS, NODE, js_block, wizard_payloads

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

# Every field the server's estimate reads that the generate payload carries.
_ENGINE_FIELDS = {
    "budget_range", "budget_period", "campaign_duration", "target_region",
    "custom_countries", "locations", "target_roles", "industry", "client_name",
    "campaign_start_month",
}

_STATE = {
    "clientName": {"value": "Acme Fabrication"},
    "exactBudget": {"value": "1.5 million"},
    "budgetRange": {"value": "__exact__"},
    "budgetPeriod": {"value": "monthly"},
    "campaignDuration": {"value": "6-12 months"},
    "campaignStartMonth": {"value": "3"},
    "regionSelector": {"value": "us_only"},
    "chSocial": {"checked": True},
}
_LOCS = ["Zurich, Switzerland", "Toronto, ON", "Sao Paulo, Brazil"]
_ROLES = ["Welder", "Machinist"]


@needs_node
@pytest.mark.parametrize("region", ["us_only", "global", "emea", "custom"])
@pytest.mark.parametrize("touch", [None, {"chApac": True, "chEmployerBrand": False}])
def test_estimate_body_equals_generate_payload_fields(region, touch):
    state = dict(_STATE, regionSelector={"value": region})
    if region == "custom":
        state["__checked"] = {"customCountry": ["CH", "BR"]}
    got = wizard_payloads(
        state, selected_industry="blue_collar_trades", locations=_LOCS, roles=_ROLES,
        touch=touch,
    )
    gen, est = got["generate"], got["estimate"]
    missing = sorted(_ENGINE_FIELDS - set(est))
    assert not missing, f"preview omits fields generate sends: {missing}"
    diffs = {k: (est[k], gen.get(k)) for k in est if gen.get(k) != est[k]}
    assert not diffs, f"preview sends different values: {diffs}"
    assert est["target_region"] == region
    assert est["budget_range"] == "1.5 million"  # raw text: the server reads it
    if touch:
        assert est["channel_categories"] == gen["channel_categories"]
    else:
        assert "channel_categories" not in est and "channel_categories" not in gen


def test_generate_payload_takes_core_fields_from_the_shared_builder():
    """The generatePlan payload spreads novaPlanCoreInputs() FIRST and never
    re-declares one of its keys (a later key would silently override it)."""
    src = APP_JS.read_text(encoding="utf-8")
    core = js_block(src, "function novaPlanCoreInputs()")
    core_keys = set(re.findall(r"^\s{6}(\w+):", core, re.M))
    assert _ENGINE_FIELDS | {"channel_categories"} <= core_keys
    gen = src[src.index("// 28. Updated payload with new fields") :]
    literal = js_block(gen, "const payload = {")
    body = literal[literal.index("{") + 1 :].lstrip()
    assert body.startswith("...novaPlanCoreInputs(),")
    redeclared = {k for k in core_keys if re.search(rf"^\s{{6}}{k}:", literal, re.M)}
    assert not redeclared, f"generate payload overrides shared fields: {redeclared}"


_BRIEF = {
    "budget_range": "250000",
    "industry": "blue_collar_trades",
    "client_name": "Acme Fabrication",
    "target_roles": ["Welder"],
    "locations": _LOCS,
}


def _funded(brief: dict) -> set:
    return {c["key"] for c in app._compute_plan_estimate(dict(brief))["channels"]}


def test_us_only_region_strips_apac_emea_even_for_foreign_cities():
    funded = _funded(dict(_BRIEF, target_region="us_only"))
    assert not funded & {"apac_regional", "emea_regional"}, funded


def test_missing_region_is_us_only_like_generate():
    # /api/generate defaults a missing/unknown region to us_only.
    assert _funded(_BRIEF) == _funded(dict(_BRIEF, target_region="us_only"))
    assert _funded(dict(_BRIEF, target_region="mars")) == _funded(
        dict(_BRIEF, target_region="us_only")
    )


def test_global_region_keeps_regional_channels():
    funded = _funded(dict(_BRIEF, target_region="global"))
    assert {"apac_regional", "emea_regional"} <= funded, funded


def test_region_fold_is_the_generate_rule():
    pcts = {"programmatic_dsp": 30, "global_boards": 20, "apac_regional": 3,
            "emea_regional": 2, "social_media": 45}
    assert app._apply_target_region(pcts, "us_only")[0] == {
        "programmatic_dsp": 30, "global_boards": 20, "social_media": 50
    }
    assert app._apply_target_region(pcts, "global")[0] == pcts
    assert app._apply_target_region(pcts, "custom")[0] == pcts
    emea = app._apply_target_region(pcts, "emea")[0]
    assert "apac_regional" not in emea and emea["emea_regional"] == 5  # D-09: added
    apac = app._apply_target_region(pcts, "apac")[0]
    assert "emea_regional" not in apac and apac["apac_regional"] == 5
    assert pcts["apac_regional"] == 3  # caller's dict untouched


def test_both_generate_paths_use_the_shared_region_fold():
    src = (APP_JS.parent.parent.parent.parent / "app.py").read_text(encoding="utf-8")
    # sync, async and estimate: three call sites, no inline copies left
    assert src.count("_apply_target_region(") == 4  # definition + 3 calls
    assert "_all_us_async" not in src
    assert "Infer from locations if region not explicitly set" not in src


def test_estimate_reports_the_region_it_used():
    est = app._compute_plan_estimate(dict(_BRIEF, target_region="EMEA"))
    assert est["target_region"] == "emea"
