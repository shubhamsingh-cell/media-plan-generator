"""Verifier findings on the supply upgrade as merged:

1. the supply-listing fast path shows only boards the supply data confirms in
   network for the market (not Deactivated / Paused / Churned P0/P1 rows);
2. a publisher search never reports ``in_joveo_network`` for a board the supply
   record does not confirm, whatever catalog the match came from;
3. the newer supply export wins over the 2026-10-17 network-group confirmation
   where it has an explicit contrary row: the builder writes
   ``network_conflicts`` and ``board_supply_record`` reads them.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

import publisher_filter as pf

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_JSON = os.path.join(ROOT, "data", "joveo_global_supply_repository.json")
MASTER = os.path.expanduser("~/Downloads/Joveo Global Supply Repository.xlsx")
TRACKER = os.path.expanduser("~/Downloads/Publisher-Onboarding-Tracker.xlsx")


@pytest.fixture(scope="module")
def engine():
    import nova

    return nova.Nova()


# -- 1. fast-path supply listing ---------------------------------------------

_DEACTIVATED = {
    "Germany": ("JobsInNetwork", "Karriere", "Wonderkind", "adway.ai"),
    "United Kingdom": ("Otta", "Harnham"),
}


@pytest.mark.parametrize("country", sorted(_DEACTIVATED))
def test_supply_listing_drops_boards_the_supply_data_does_not_confirm(engine, country):
    query = f"List all job boards and publishers for tech jobs in {country}"
    result = engine._fast_path_supply_listing(query, query.lower())
    assert result and result.get("response"), "listing did not render (vacuous)"
    text = result["response"]
    for name in _DEACTIVATED[country]:
        # the row really is not in network in the data (so the test can fail)
        assert (
            pf.board_supply_record(name, country)["state"] != pf.SUPPLY_IN_NETWORK
        ), name
        assert name not in text, name


# -- 2. publisher search: in_joveo_network follows the supply record ----------


@pytest.mark.parametrize(
    "term,country",
    [
        ("Vivian Health", None),
        ("Incredible Health", None),
        ("Kleinanzeigen", "Germany"),
        ("University of Copenhagen", None),
        ("Freshersworld", "India"),
    ],
)
def test_publisher_search_never_claims_network_without_a_confirming_record(
    engine, term, country
):
    args = {"search_term": term}
    if country:
        args["country"] = country
    res = engine._query_publishers(args)
    assert res["in_joveo_network"] is False, (term, res.get("note"))
    for m in res.get("search_results") or []:
        rec = pf.board_supply_record(m.get("name"), country)
        assert (
            rec["state"] != pf.SUPPLY_IN_NETWORK
            or term.lower() in (m.get("name") or "").lower()
        ), m


def test_publisher_search_still_says_yes_for_a_confirmed_board(engine):
    res = engine._query_publishers({"search_term": "StepStone", "country": "Germany"})
    assert res["in_joveo_network"] is True


# -- 3. the newer export wins over the group confirmation ---------------------

_EXPECTED = {
    ("JustEngineers.net", "United Kingdom"): "VONQ Only",
    ("Meine Stadt", "Germany"): "Deactivated",
    ("Meinestadt.de", "Germany"): "Deactivated",
    (
        "StepStone Germany (Pro - Media Buy) - Selected Customers Only",
        "Germany",
    ): "VONQ Only",
    ("STEPSTONE.be", "Belgium"): "Deactivated",
}


def test_repository_lists_the_network_conflicts_and_nothing_else_in_them():
    repo = json.load(open(REPO_JSON, encoding="utf-8"))
    conflicts = repo["network_conflicts"]
    assert {(c["name"], c["country"]): c["export_status"] for c in conflicts} == (
        _EXPECTED
    )
    for c in conflicts:
        assert set(c) == {"name", "country", "export_status", "as_of"}
        assert c["as_of"] == "2026-10-20"


@pytest.mark.parametrize(
    "name,country",
    [
        ("Meine Stadt", "Germany"),
        ("meinestadt.de", "Germany"),
        ("STEPSTONE.be", "Belgium"),
        ("StepStone Belgium", "Belgium"),
        ("JustEngineers.net", "United Kingdom"),
        ("JustEngineers", "United Kingdom"),
        ("StepStone Germany (Pro - Media Buy) - Selected Customers Only", "Germany"),
    ],
)
def test_a_conflicted_board_is_not_in_network_with_the_export_status(name, country):
    rec = pf.board_supply_record(name, country)
    assert rec["state"] == pf.SUPPLY_NOT_IN_NETWORK, name
    assert rec["status"].endswith(
        "in the 2026-10-20 supply export (network group confirmation superseded)"
    )
    assert rec["status"].split(" in the ")[0] in ("Deactivated", "VONQ Only")


def test_the_conflict_is_scoped_to_its_country_and_the_rest_of_the_group_stays():
    assert pf.board_supply_record("StepStone", "Germany")["state"] == (
        pf.SUPPLY_IN_NETWORK
    )
    assert pf.board_supply_record("StepStone Austria", "Austria")["state"] == (
        pf.SUPPLY_IN_NETWORK
    )


def test_meine_stadt_leaves_the_germany_funded_and_standby_lists():
    import channel_funding

    data = {"locations": ["Berlin, Germany"], "target_roles": ["Warehouse Associate"]}
    for b in channel_funding.ranked_boards(data, "Germany"):
        assert not pf.names_match(b["name"], "Meine Stadt"), b


def test_builder_reproduces_the_committed_repository_byte_for_byte(tmp_path):
    if not (os.path.exists(MASTER) and os.path.exists(TRACKER)):
        pytest.skip("source workbooks not on this machine")
    outs = []
    for i in range(2):
        out = tmp_path / f"repo{i}.json"
        proc = subprocess.run(
            [
                sys.executable,
                os.path.join(ROOT, "scripts", "build_supply_repository.py"),
                MASTER,
                TRACKER,
                "--repository-out",
                str(out),
                "--pipeline-out",
                str(tmp_path / f"pipe{i}.json"),
                "--no-sync-count",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        outs.append(out.read_bytes())
    assert outs[0] == outs[1], "builder is not deterministic"
    assert outs[0] == open(REPO_JSON, "rb").read()
    assert "network conflict: Meine Stadt (Germany)" in proc.stdout
