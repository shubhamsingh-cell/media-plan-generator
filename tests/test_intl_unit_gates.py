"""International-accuracy gates, one unit at a time (the bundle-level proof is
tests/test_intl_boards_end_to_end.py).

Each assertion is a defect from the 2026-10-16 nine-market sweep that the
parent commit still has.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

FR = {
    "locations": ["Paris, France", "Lyon, France"],
    "industry": "logistics_supply_chain",
}
VN = {"locations": ["Ho Chi Minh City, Vietnam"], "industry": "logistics_supply_chain"}
US = {"locations": ["Houston, TX", "Dallas, TX"], "industry": "logistics_supply_chain"}


# --- B2.1 ad-platform table -------------------------------------------------


def _platform_keys(data: dict) -> set:
    import data_synthesizer

    res = data_synthesizer.fuse_ad_platform_analysis(
        {}, {}, dict(data, roles=["Driver"], budget="250000")
    )
    return {k for k in res if not k.startswith("_")}


@pytest.mark.parametrize(
    "data",
    [
        FR,
        VN,
        {"locations": ["Mexico City, Mexico"], "industry": "logistics_supply_chain"},
        {"locations": ["London, United Kingdom"], "industry": "logistics_supply_chain"},
    ],
)
def test_ziprecruiter_row_leaves_non_zip_markets(data):
    keys = _platform_keys(data)
    assert "ziprecruiter_sponsored" not in keys
    assert "indeed_sponsored_jobs" in keys  # a global board stays


@pytest.mark.parametrize(
    "data",
    [US, {"locations": ["Toronto, Canada"], "industry": "logistics_supply_chain"}],
)
def test_ziprecruiter_row_stays_in_us_and_canada(data):
    assert "ziprecruiter_sponsored" in _platform_keys(data)


def test_client_exclusion_removes_the_row_and_its_ranking_entry():
    import data_synthesizer

    data = dict(
        US, roles=["Driver"], budget="250000", excluded_publishers=["indeed.com"]
    )
    res = data_synthesizer.fuse_ad_platform_analysis({}, {}, data)
    assert "indeed_sponsored_jobs" not in res
    assert "indeed_sponsored_jobs" not in [
        r["platform"] for r in res["_platform_ranking"]
    ]


# --- B2.2 / B2.4 deck bullets ----------------------------------------------


def test_complication_bullets_are_us_only_content():
    import ppt_generator

    us = ppt_generator._get_complications("logistics_supply_chain", US)
    assert any("CDL" in b for b in us)
    for plan in (FR, VN):
        bullets = ppt_generator._get_complications("logistics_supply_chain", plan)
        assert bullets, plan
        assert not [b for b in bullets if re.search(r"\d|\$|\bCDL\b", b)], bullets
    # and every industry table, not just logistics
    for industry in ppt_generator.COMPLICATIONS:
        bullets = ppt_generator._get_complications(
            industry, dict(FR, industry=industry)
        )
        assert not [b for b in bullets if re.search(r"\d|\$|\bCDL\b", b)], (
            industry,
            bullets,
        )


def test_role_requirement_callouts_are_us_licence_content():
    import insight_composer

    args = ("logistics supply chain", ["CDL Driver"])
    assert insight_composer.role_requirements_callout(*args)  # US plan keeps it
    assert insight_composer.role_requirements_callout(*args, us_market=False) == []


# --- B2.3 inferred competitor rosters --------------------------------------


def _city_data(*cities):
    return {c: {"hiring_difficulty": 5.0} for c in cities}


def test_inferred_roster_is_never_us_companies_abroad():
    import gold_standard

    data = dict(VN, target_roles=["Driver"], competitors=[])
    cmap = gold_standard.build_competitor_map(data, _city_data("Ho Chi Minh City"))
    assert cmap["Ho Chi Minh City"]["top_employers"] == []
    assert "_national" not in cmap
    # brief-named competitors are still honoured
    named = gold_standard.build_competitor_map(
        dict(data, competitors=["DHL"]), _city_data("Ho Chi Minh City")
    )
    assert named["Ho Chi Minh City"]["top_employers"] == ["DHL"]


def test_us_plan_keeps_its_inferred_roster():
    import gold_standard

    data = dict(US, target_roles=["Driver"], competitors=[])
    cmap = gold_standard.build_competitor_map(data, _city_data("Houston"))
    assert cmap["Houston"]["top_employers"]
    assert "_national" in cmap


def test_trucking_creative_alert_quotes_us_earnings_only_on_us_plans():
    import gold_standard

    data = dict(VN, target_roles=["Driver"], competitors=[])
    cmap = gold_standard.build_competitor_map(data, _city_data("Ho Chi Minh City"))
    assert "$200-$400" not in json.dumps(cmap)
    us = gold_standard.build_competitor_map(
        dict(US, target_roles=["Driver"], competitors=[]), _city_data("Houston")
    )
    assert "$200-$400" in json.dumps(us)


# --- channel strategy (Quality Intelligence) -------------------------------


def _strategy(data: dict) -> dict:
    import gold_standard

    return gold_standard.build_channel_strategy(
        dict(data, target_roles=["Driver"]),
        [{"complexity_score": 3.0, "seniority_level": "entry"}],
    )


def _names(strategy: dict) -> set:
    return {
        c["name"]
        for key in ("traditional_channels", "non_traditional_channels")
        for c in strategy[key]
    }


def test_channel_strategy_drops_us_only_boards_on_a_non_us_plan():
    names = _names(_strategy(FR))
    for us_only in (
        "ZipRecruiter",
        "CareerBuilder",
        "CDLjobs.com",
        "TruckersReport Jobs",
    ):
        assert us_only not in names, us_only
    us_names = _names(_strategy(US))
    assert {"ZipRecruiter", "CareerBuilder"} <= us_names


def test_channel_strategy_honours_client_exclusions():
    names = _names(_strategy(dict(US, excluded_publishers=["indeed.com", "Monster"])))
    assert "Indeed" not in names and "Monster" not in names
    assert "LinkedIn" in names


# --- B2.4 / B2.6 benchmarks -------------------------------------------------


def test_local_cpa_range_is_the_markets_own_or_nothing():
    import intl_benchmark_lookup as ib

    fr = ib.get_plan_local_cpa_range(FR)
    assert fr and (fr["country"], fr["low_usd"], fr["high_usd"]) == (
        "France",
        6.0,
        22.0,
    )
    assert ib.get_plan_local_cpa_range(VN) is None  # no Vietnam range in the dataset
    assert ib.get_plan_local_cpa_range(US) is None
    assert (
        ib.get_plan_local_cpa_range(
            {
                "locations": ["Paris, France", "Berlin, Germany"],
                "industry": "logistics_supply_chain",
            }
        )
        is None
    )  # two countries have no single honest range
    assert ib.format_usd_range(6, 22) == "US$6-US$22"
    assert ib.format_usd_range(0.3, 2.5) == "US$0.30-US$2.50"


def test_usd_plan_in_a_non_us_market_discloses_the_us_benchmark():
    import budget_engine as be

    basis = be._resolve_intl_cpc_basis(["Bengaluru, India"], "USD", "$250000")
    cph = be.resolve_industry_cph("logistics_supply_chain", None, "USD", basis)
    assert cph["us_benchmark_non_us_market"] is True
    disp = be.industry_cph_display(cph, "USD")
    assert "US benchmark, no local data" in disp["text"]
    assert any("no local data" in v for v in disp["variants"])
    # a genuinely US plan is untouched
    us_cph = be.resolve_industry_cph("logistics_supply_chain", None, "USD", None)
    assert not us_cph.get("us_benchmark_non_us_market")
    assert "no local data" not in be.industry_cph_display(us_cph, "USD")["text"]
    # the math is unchanged -- only the claim is labelled
    assert cph["math_value"] == us_cph["math_value"]


# --- B2.8 / B3 data ---------------------------------------------------------


def test_stale_and_unavailable_names_are_gone_from_the_shipped_data():
    import research

    assert "Workopolis" not in research.COUNTRY_DATA["Canada"]["top_boards"]
    assert "CareerBuilder VN" not in research.COUNTRY_DATA["Vietnam"]["top_boards"]
    assert "CareerViet" in research.COUNTRY_DATA["Vietnam"]["top_boards"]
    assert "OCC" not in research.COUNTRY_DATA["Mexico"]["top_boards"]
    intl = json.loads(
        (ROOT / "data" / "international_benchmarks_2026.json").read_text()
    )
    names = [
        p["name"] for c in intl["countries"].values() for p in c.get("platforms", [])
    ]
    assert "CareerBuilder Vietnam" not in names and "CareerViet" in names
    assert not [n for n in names if "Workopolis" in n]


def test_global_supply_no_longer_claims_occ_or_hellowork_are_in_network():
    supply = json.loads((ROOT / "data" / "global_supply.json").read_text())
    boards = {
        b["name"]: b
        for country in ("France", "Mexico")
        for b in supply["country_job_boards"][country]["boards"]
    }
    for name in ("OCC Mundial", "HelloWork (formerly RegionsJob)"):
        entry = boards[name]
        assert entry["availability"] == "unavailable"
        assert "Confirmed part of Joveo network" not in entry["_verification_notes"]
        assert "not available through Joveo" in entry["availability_reason"]
    # the location-board lists the plan reads never carry them
    import research

    assert not [
        b for b in research.get_location_boards(["Mexico City, Mexico"]) if "OCC" in b
    ]
    for cb in research.get_global_supply_data(
        ["Mexico City, Mexico", "Paris, France"], "logistics_supply_chain"
    )["country_boards"]:
        assert not [
            b for b in cb["data"]["boards"] if re.search(r"OCC|HelloWork", b["name"])
        ]


# --- Nova tools -------------------------------------------------------------


@pytest.fixture(scope="module")
def nova_engine():
    import nova

    return nova.Nova()


def test_nova_global_supply_applies_exclusions_and_availability(nova_engine):
    def names(params):
        res = nova_engine._query_global_supply(params)
        return [b["name"] for b in res["country_boards"]["boards"]]

    assert "Indeed France" in names({"country": "France"})
    ex = names({"country": "France", "excluded_publishers": ["indeed.com"]})
    assert "Indeed France" not in ex and "Pôle Emploi" in ex
    assert "OCC Mundial" not in names({"country": "Mexico"})
    assert "Indeed Mexico" in names({"country": "Mexico"})


def test_nova_publisher_search_says_unavailable_instead_of_not_found(nova_engine):
    res = nova_engine._query_publishers({"search_term": "hellowork"})
    assert res["match_count"] == 0 and res["in_joveo_network"] is False
    assert "not available through Joveo" in res["availability_note"]
    res = nova_engine._query_publishers(
        {"search_term": "indeed", "excluded_publishers": ["indeed"]}
    )
    assert res["match_count"] == 0


def test_nova_tool_schemas_accept_the_exclusion_list(nova_engine):
    tools = {t["name"]: t for t in nova_engine.get_tool_definitions()}
    for name in ("query_global_supply", "query_publishers"):
        prop = tools[name]["input_schema"]["properties"]["excluded_publishers"]
        assert prop["type"] == "array" and prop["items"] == {"type": "string"}
