"""A regional job board is funded only when the plan has a market in its
region (2026-10-16 sweep).

Pre-fix ``app._apply_target_region`` left ``global`` and ``custom`` plans
untouched, so a Brazil-only custom plan put the industry profile's
``apac_regional`` + ``emea_regional`` shares (9% + 9% in some profiles) into
APAC and EMEA boards no Brazilian candidate reads -- Canada 20.8% and Mexico
17.6% of budget in that day's sweep. Now, for ``global`` / ``custom``, the
plan's markets (locations + custom-region countries) decide which regional
board stays; a dropped board's share is spread proportionally over the
remaining channels; mixed plans keep both; anything the resolver cannot
place keeps the old behaviour (fail open).
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
import plan_currency  # noqa: E402

_PCTS = {
    "programmatic_dsp": 32,
    "global_boards": 30,
    "social_media": 20,
    "apac_regional": 9,
    "emea_regional": 9,
}
_REGIONAL = {"apac_regional", "emea_regional"}


def _fold(region: str, plan: "dict | None") -> dict:
    return app._apply_target_region(dict(_PCTS), region, plan)[0]


# ---------------------------------------------------------------------------
# The fold itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "plan",
    [
        {"locations": ["Sao Paulo, Brazil"], "custom_countries": ["BR"]},
        {"locations": ["Toronto, ON"], "custom_countries": ["CA"]},
        {"locations": ["Mexico City, Mexico"], "custom_countries": ["MX"]},
        {"custom_countries": ["BR"]},  # countries alone are enough
    ],
)
def test_americas_only_custom_plan_funds_no_regional_board(plan):
    out = _fold("custom", plan)
    assert not _REGIONAL & set(out), out
    assert sum(out.values()) == pytest.approx(sum(_PCTS.values()))


def test_dropped_share_is_spread_proportionally():
    out = _fold("custom", {"locations": ["Sao Paulo, Brazil"]})
    kept = {k: v for k, v in _PCTS.items() if k not in _REGIONAL}
    kept_total = sum(kept.values())
    for key, share in kept.items():
        assert out[key] == pytest.approx(share + 18 * share / kept_total)
    # ratios between the remaining channels are unchanged
    assert out["programmatic_dsp"] / out["social_media"] == pytest.approx(32 / 20)


def test_emea_only_plan_keeps_emea_board_drops_apac():
    out = _fold(
        "custom", {"locations": ["Berlin, Germany"], "custom_countries": ["DE"]}
    )
    assert "apac_regional" not in out
    assert out["emea_regional"] > _PCTS["emea_regional"]  # took its proportional cut
    assert sum(out.values()) == pytest.approx(sum(_PCTS.values()))


def test_apac_only_plan_keeps_apac_board_drops_emea():
    out = _fold("custom", {"locations": ["Bengaluru, Karnataka, India"]})
    assert "emea_regional" not in out and "apac_regional" in out


def test_mixed_plan_keeps_both_boards():
    plan = {"locations": ["Berlin, Germany", "Pune, India", "Toronto, ON"]}
    assert _fold("custom", plan) == _PCTS
    assert _fold("global", plan) == _PCTS


def test_global_region_follows_markets_too():
    out = _fold("global", {"locations": ["Denver, CO", "Austin, TX"]})
    assert not _REGIONAL & set(out), out


@pytest.mark.parametrize(
    "plan",
    [
        None,  # legacy callers: no plan, no change
        {},
        {"locations": []},
        {"locations": ["Remote"]},  # names no market
        {"locations": ["Worldwide"]},  # cannot be placed
        {"locations": ["Toronto, ON", "Accra, Ghana"]},  # one unplaceable market
        # plan_location reads a bare "London" as several US towns -- not
        # proof the plan has no EMEA market
        {"locations": ["London"]},
    ],
)
def test_unknown_or_unplaceable_markets_keep_every_board(plan):
    assert _fold("custom", plan) == _PCTS
    assert _fold("global", plan) == _PCTS


@pytest.mark.parametrize(
    "plan",
    [
        # bare city names plan_location matches to ONE US town each
        {"locations": ["Munich"]},
        {"locations": ["Brisbane", "Toronto, ON"]},
        # ... and the same plan after the request boundary rewrote them
        {
            "locations": ["Munich, ND", "Brisbane, CA"],
            "_location_resolution": [
                {"input": "Munich", "matched_via": "place_city_unique"},
                {"input": "Brisbane", "matched_via": "place_city_unique"},
            ],
        },
        # a dict that names no place at all
        {"locations": [{"name": "Berlin, Germany"}, "Austin, TX"]},
    ],
)
def test_bare_city_guesses_keep_every_board(plan):
    """Verifier find (2026-10-16): "Munich" -> "Munich, ND" upstream, so a
    global plan for Munich + Brisbane lost both boards. A bare city name is a
    guess, not a market: fail open."""
    assert _fold("global", plan) == _PCTS
    assert _fold("custom", plan) == _PCTS


@pytest.mark.parametrize(
    "plan, kept",
    [
        ({"locations": ["Chicago"], "custom_countries": ["US"]}, set()),
        (
            {
                "locations": ["Chicago, IL"],
                "custom_countries": ["US"],
                "_location_resolution": [{"matched_via": "place_city_unique"}],
            },
            set(),
        ),
        ({"locations": ["London"], "custom_countries": ["UK"]}, {"emea_regional"}),
    ],
)
def test_ticked_custom_countries_outrank_a_bare_city_guess(plan, kept):
    """Verifier re-check: "Chicago" + US ticked is a US-only plan. With custom
    countries named, a bare-city guess neither adds nor blocks a region."""
    assert _REGIONAL & set(_fold("custom", plan)) == kept


@pytest.mark.parametrize(
    "plan, kept",
    [
        # bare foreign names nothing recognises: a guess, so the ticked
        # countries decide (verifier round 2 -- the brief's headline case)
        ({"locations": ["Sao Paulo"], "custom_countries": ["BR"]}, set()),
        ({"locations": ["Mumbai"], "custom_countries": ["IN"]}, {"apac_regional"}),
        # ... and with nothing ticked they keep every board -- for a name a US
        # town also carries ("Berlin": Berlin NH/WI/CT), which stays a guess
        ({"locations": ["Berlin"]}, _REGIONAL),
        # a bare name only a foreign city can be is its country's market, not a
        # guess (plan_geo reads it as non-US): Sao Paulo is the Americas, which
        # have no regional board, so both are dropped; Mumbai is APAC
        ({"locations": ["Sao Paulo"]}, set()),
        ({"locations": ["Mumbai"]}, {"apac_regional"}),
        # a comma string from an API caller is one entry per country
        (
            {"locations": ["Austin, TX"], "custom_countries": "DE,US"},
            {"emea_regional"},
        ),
        # a sidecar that no longer lines up with locations: fail open
        (
            {
                "locations": ["Munich, ND", "Austin, TX"],
                "_location_resolution": [{"matched_via": "place_city_unique"}],
            },
            _REGIONAL,
        ),
    ],
)
def test_bare_foreign_names_and_odd_shapes(plan, kept):
    assert _REGIONAL & set(_fold("custom", plan)) == kept


@pytest.mark.parametrize(
    "plan, kept",
    [
        # verifier round 3: a ticked code 533669f could not place ("TW", "GH")
        # counted as a guess and dropped its own region's board. Read as ISO
        # codes they are Taiwan (APAC) and Ghana (EMEA).
        (
            {"locations": ["Austin, TX"], "custom_countries": ["US", "TW"]},
            {"apac_regional"},
        ),
        (
            {"locations": ["Austin, TX"], "custom_countries": ["US", "GH"]},
            {"emea_regional"},
        ),
        # a code no table places never drops a board
        ({"locations": ["Austin, TX"], "custom_countries": ["US", "XX"]}, _REGIONAL),
        # ISO only: "MA" is Morocco, not Massachusetts
        ({"custom_countries": ["MA"]}, {"emea_regional"}),
        (
            {"locations": ["Casablanca, MA"], "custom_countries": ["MA"]},
            {"emea_regional"},
        ),
        # a country field: ISO code or name, never a guess
        ({"locations": ["Austin, TX"], "country": "MA"}, {"emea_regional"}),
        ({"locations": ["Austin, TX"], "country": "XX"}, _REGIONAL),
        # world-scope text is unplaceable even with countries ticked
        ({"locations": ["Worldwide"], "custom_countries": ["US"]}, _REGIONAL),
    ],
)
def test_ticked_codes_are_iso_and_unknown_codes_fail_open(plan, kept):
    assert _REGIONAL & set(_fold("custom", plan)) == kept


def test_iso_table_covers_every_wizard_custom_country():
    for code in "US UK CA DE FR AU IN SG JP BR MX AE NL SE PH".split():
        assert app._country_code_region(code), code


def test_bare_foreign_city_custom_preview_funds_no_regional_board():
    funded = _funded(
        dict(
            _BRIEF,
            target_region="custom",
            locations=["Sao Paulo"],
            custom_countries=["BR"],
        )
    )
    assert not _REGIONAL & set(funded), funded


def test_an_unplaceable_market_still_blocks_with_custom_countries():
    plan = {"locations": ["Accra, Ghana"], "custom_countries": ["US"]}
    assert _fold("custom", plan) == _PCTS


def test_bare_city_rewrite_keeps_boards_end_to_end():
    funded = _funded(
        dict(_BRIEF, target_region="global", locations=["Munich", "Brisbane"])
    )
    assert _REGIONAL <= set(funded), funded


def test_qualified_us_locations_still_count_as_us():
    plan = {
        "locations": ["Hershey, PA", "80202", "Texas", "Cook County, IL", "Nationwide"],
        "_location_resolution": [
            {"matched_via": "place_city_state"},
            {"matched_via": "zip_exact"},
            {"matched_via": "state_bare"},
            {"matched_via": "county_state_exact"},
            {"matched_via": "nationwide_keyword"},
        ],
    }
    assert not _REGIONAL & set(_fold("global", plan))


def test_bare_remote_does_not_block_the_named_markets():
    out = _fold("custom", {"locations": ["Toronto, ON", "Remote"]})
    assert not _REGIONAL & set(out), out


def test_dict_locations_are_placed_by_country():
    plan = {"locations": [{"city": "Curitiba", "state": "", "country": "Brazil"}]}
    assert not _REGIONAL & set(_fold("custom", plan))


def test_other_regions_and_callers_dict_are_untouched():
    pcts = dict(_PCTS)
    plan = {"locations": ["Sao Paulo, Brazil"]}
    app._apply_target_region(pcts, "custom", plan)
    assert pcts == _PCTS  # never mutates the caller's dict
    # us_only / emea / apac keep their own rules whatever the plan says
    assert app._apply_target_region(dict(_PCTS), "us_only", plan)[0] == (
        app._apply_target_region(dict(_PCTS), "us_only")[0]
    )
    assert app._apply_target_region(dict(_PCTS), "emea", plan)[0]["emea_regional"] == 18


def test_fold_reports_what_moved():
    _out, detail = app._apply_target_region(
        dict(_PCTS), "custom", {"locations": ["Sao Paulo, Brazil"]}
    )
    assert "18" in detail and "APAC" in detail and "EMEA" in detail
    # a board that held 0% is removed but not reported as a source
    _out, detail = app._apply_target_region(
        dict(_PCTS, apac_regional=0), "custom", {"locations": ["Sao Paulo, Brazil"]}
    )
    assert "from EMEA " in detail and "APAC" not in detail


def test_every_plan_currency_has_a_market_region():
    """A currency added to plan_currency must be placed here, or a plan in
    that market silently keeps both boards (fail open)."""
    currencies = set(plan_currency._COUNTRY_TO_CODE.values())
    missing = sorted(currencies - set(app._CURRENCY_MARKET_REGION))
    assert not missing, f"no market region for {missing}"


# ---------------------------------------------------------------------------
# End to end through the wizard preview, and the plan reaches every path
# ---------------------------------------------------------------------------


def _funded(brief: dict) -> dict:
    est = app._compute_plan_estimate(dict(brief))
    return {c["key"]: c["pct"] for c in est["channels"]}


_BRIEF = {
    "budget_range": "250000",
    "industry": "healthcare_medical",
    "client_name": "Acme Health",
    "target_roles": ["Registered Nurse"],
}


@pytest.mark.parametrize(
    "locations, countries",
    [
        (["Sao Paulo, Brazil"], ["BR"]),
        (["Toronto, ON"], ["CA"]),
        (["Mexico City, Mexico"], ["MX"]),
    ],
)
def test_single_country_custom_preview_funds_no_regional_board(locations, countries):
    funded = _funded(
        dict(
            _BRIEF,
            target_region="custom",
            locations=locations,
            custom_countries=countries,
        )
    )
    assert not _REGIONAL & set(funded), funded
    assert sum(funded.values()) == pytest.approx(100, abs=0.5)


def test_mixed_custom_preview_funds_both_boards():
    funded = _funded(
        dict(
            _BRIEF,
            target_region="custom",
            locations=["Berlin, Germany", "Pune, India"],
            custom_countries=["DE", "IN"],
        )
    )
    assert _REGIONAL <= set(funded), funded


def test_every_region_fold_call_site_passes_the_plan():
    """sync (data), async (gen_data) and /api/estimate (brief) all hand the
    request dict over -- without it the fold cannot see the plan's markets."""
    tree = ast.parse((PROJECT_ROOT / "app.py").read_text(encoding="utf-8"))
    plans = sorted(
        ast.unparse(node.args[2])
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "_apply_target_region"
        and len(node.args) >= 3
    )
    # + the headline projection (_attach_headline_projection), which rebuilds the
    # preview's mix from the request dict ("plan")
    assert plans == ["brief", "data", "gen_data", "plan"], plans
