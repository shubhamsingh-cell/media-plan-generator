"""The Stepstone Group's job boards are in the Joveo network (owner
confirmation 2026-10-17; data/joveo_network_groups.json)."""

import json
import os

import pytest

import publisher_filter as pf

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def _fresh():
    pf.reset_caches()
    yield
    pf.reset_caches()


def _band(locations, **extra):
    return {
        g["country"]: g
        for g in pf.plan_market_boards({"locations": locations, **extra})
    }


def test_registry_members_each_carry_a_public_source_and_the_group_a_status():
    raw = json.load(open(os.path.join(ROOT, "data", "joveo_network_groups.json")))
    (group,) = raw["groups"]
    assert group["status"] == "In Joveo Network"
    assert group["as_of"] == "2026-10-17"
    assert group["status_source"] == "owner confirmation"
    assert "StepGroup" in group["aliases"]
    assert group["members"]
    for member in group["members"]:
        assert member["source"].startswith("https://"), member["name"]
        assert member["countries"], member["name"]
    # a programmatic platform is not a job board: not a member
    assert not pf.network_member("Appcast", "United States")


def test_group_members_resolve_scoped_to_their_country():
    assert (
        pf.network_member("Totaljobs", "United Kingdom")["group"]
        == "The Stepstone Group"
    )
    assert pf.network_member("StepStone", "Germany")
    assert pf.network_member("StepStone Netherlands", "Netherlands")
    assert pf.network_member("CareerStructure", "United Kingdom")
    # country-scoped: Jobs.ie is Ireland's, "Jobs.ch" is not a member
    assert pf.network_member("Jobs.ie", "Ireland")
    assert not pf.network_member("Jobs.ch", "Switzerland")
    assert not pf.network_member("Jobs.ch")
    assert not pf.network_member("StepStone", "Poland")
    assert pf.network_group_for_query("StepGroup")["name"] == "The Stepstone Group"
    assert pf.network_group_for_query("the stepstone group")


def test_group_boards_are_never_daggered_even_if_the_repository_says_otherwise(
    monkeypatch,
):
    # force the repository to list the board for the market as NOT in network
    pf._repo_index()
    pf._repo_index_cache["totaljobs"] = [(frozenset({"United Kingdom"}), "VONQ Only")]
    assert pf.network_member("Totaljobs", "United Kingdom")
    assert pf.relationship_unverified("Totaljobs", "United Kingdom") is False
    marked, flag = pf.annotate(["Totaljobs"], "United Kingdom")
    assert marked == ["Totaljobs"] and flag is False


def test_uk_and_germany_bands_list_the_group_board_without_dagger():
    bands = _band(["London, United Kingdom", "Berlin, Germany"])
    assert "Totaljobs" in bands["United Kingdom"]["boards"]
    assert "StepStone" in bands["Germany"]["boards"]
    for country in ("United Kingdom", "Germany"):
        assert not [b for b in bands[country]["boards"] if "†" in b]


def test_markets_whose_group_board_missed_the_cut_now_show_it():
    bands = _band(
        [
            "Amsterdam, Netherlands",
            "Copenhagen, Denmark",
            "Johannesburg, South Africa",
        ]
    )
    assert "StepStone Netherlands" in bands["Netherlands"]["boards"]
    assert "StepStone Denmark" in bands["Denmark"]["boards"]
    assert any("pnet" in b.lower() for b in bands["South Africa"]["boards"])
    for g in bands.values():
        assert len(g["boards"]) <= 3


def test_ireland_with_no_dataset_boards_offers_the_group_boards():
    bands = _band(["Dublin, Ireland"])
    assert bands["Ireland"]["boards"][0] == "IrishJobs"
    assert "Ireland" not in pf.plan_market_board_gaps(
        {"locations": ["Dublin, Ireland"]}
    )
    # a market with neither dataset boards nor a group board is still a gap
    assert "Spain" in pf.plan_market_board_gaps({"locations": ["Madrid, Spain"]})


def test_client_exclusion_still_removes_the_group_board_and_nothing_replaces_it_blindly():
    data = {
        "locations": ["London, United Kingdom"],
        "excluded_publishers": ["Totaljobs"],
    }
    boards = _band(data["locations"], excluded_publishers=["Totaljobs"])[
        "United Kingdom"
    ]["boards"]
    assert not [b for b in boards if "totaljobs" in b.lower()]
    # excluding the whole group by its main brand keeps the German band clean too
    de = _band(["Berlin, Germany"], excluded_publishers=["StepStone"])["Germany"][
        "boards"
    ]
    assert not [b for b in de if "stepstone" in b.lower()]
    # a client exclusion also empties the lead-board fallback
    assert (
        pf.with_network_board([], "Germany", {"excluded_publishers": ["StepStone"]}, 3)
        == []
    )


def test_us_plan_is_unchanged():
    assert pf.plan_market_boards({"locations": ["Austin, TX"]}) == []
    assert pf.network_lead_boards("United States") == []
    assert pf.with_network_board(["Indeed", "LinkedIn"], "United States", {}, 3) == [
        "Indeed",
        "LinkedIn",
    ]


def test_supply_repository_states_the_status_and_keeps_history():
    repo = json.load(
        open(os.path.join(ROOT, "data", "joveo_global_supply_repository.json"))
    )
    rows = {p["name"]: p for rd in repo["by_region"].values() for p in rd["publishers"]}
    pro = rows["StepStone Germany (Pro - Media Buy) - Selected Customers Only"]
    assert pro["status"] == "In Joveo Network" and pro["status_previous"] == "VONQ Only"
    assert pro["name_previous"].startswith("Stepstone Germa-ny")
    assert "Selected customers only" in pro["restriction"]
    assert (
        pro["network_as_of"] == "2026-10-17"
        and pro["network_source"] == "owner confirmation"
    )
    assert "United Kingdom" in rows["Totaljobs"]["countries"]
    assert rows["Totaljobs"]["countries_previous"] == "Ireland"
    for name in ("StepStone Germany", "CWJobs", "Milkround", "Pnet", "IrishJobs"):
        assert rows[name]["status"] == "In Joveo Network"
        assert rows[name]["network_group"] == "The Stepstone Group"


@pytest.fixture(scope="module")
def nova_engine():
    import nova

    return nova.Nova()


def test_nova_publisher_search_says_in_network(nova_engine):
    for term in ("totaljobs", "stepstone"):
        res = nova_engine._query_publishers({"search_term": term})
        assert res["in_joveo_network"] is True
        assert res["network"][0]["group"] == "The Stepstone Group"
        assert res["network"][0]["status"] == "In Joveo Network"
        assert "in the Joveo network" in res["network"][0]["note"]
        assert all(
            m.get("relationship") == "In Joveo Network"
            for m in res["search_results"]
            if "stepstone" in m["name"].lower() or "totaljobs" in m["name"].lower()
        )
    # the group name lists the members, including ones the older catalogs lack
    res = nova_engine._query_publishers({"search_term": "stepgroup"})
    names = [m["name"] for m in res["search_results"]]
    assert "Totaljobs" in names and "CareerStructure" in names


def test_nova_publisher_search_honours_client_exclusion(nova_engine):
    res = nova_engine._query_publishers(
        {"search_term": "totaljobs", "excluded_publishers": ["Totaljobs"]}
    )
    assert res["match_count"] == 0 and res["in_joveo_network"] is False
    res = nova_engine._query_publishers(
        {"search_term": "stepgroup", "excluded_publishers": ["Totaljobs"]}
    )
    assert "Totaljobs" not in [m["name"] for m in res["search_results"]]


def test_nova_global_supply_marks_uk_and_germany_group_boards(nova_engine):
    for country, name in (
        ("United Kingdom", "Totaljobs"),
        ("Germany", "StepStone Germany"),
    ):
        res = nova_engine._query_global_supply({"country": country})
        row = next(b for b in res["country_boards"]["boards"] if b["name"] == name)
        assert row["relationship"] == "In Joveo Network"
        assert res["network"][0]["group"] == "The Stepstone Group"
    res = nova_engine._query_global_supply(
        {"country": "United Kingdom", "excluded_publishers": ["Totaljobs"]}
    )
    assert "Totaljobs" not in [b["name"] for b in res["country_boards"]["boards"]]


def test_nova_country_listing_names_in_network_group_boards(nova_engine):
    res = nova_engine._query_publishers({"country": "Germany"})
    assert "StepStone" in res["in_network_group_boards"]


def test_availability_entry_beats_the_network_registry():
    pf._load_availability()["entries"].append(
        {
            "name": "Totaljobs",
            "_keys": frozenset({"totaljobs"}),
            "_countries": frozenset({"United Kingdom"}),
            "status": "unavailable",
            "reason": "test",
        }
    )
    assert pf.clean_names(["Totaljobs"], {}, "United Kingdom") == []
    band = pf.with_network_board(
        ["Indeed UK", "Reed.co.uk", "LinkedIn UK"], "United Kingdom", {}, 3
    )
    assert "Totaljobs" not in band
