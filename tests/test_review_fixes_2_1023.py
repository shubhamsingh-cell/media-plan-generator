"""Pre-ship review fixes (round 2): countryless Nova supply answers honour the
export's network_conflicts, compound market-share labels never replace a row's
name, only public employment services are exempt from the role-fit gate, and the
client workbook carries no internal P0/P1/P2 tier."""

from __future__ import annotations

import openpyxl
import pytest

import channel_funding
import excel_v2
import nova
import publisher_filter
from tests.test_integ_followups_1020 import _alloc, _sheet_text

_NOVA = nova.Nova()
_NOVA = nova.Nova()
_CONFLICTED = [
    "Meine Stadt",
    "meinestadt.de",
    "STEPSTONE.be",
    "StepStone Belgium",
    "JustEngineers.net",
    "StepStone Pro — Selected Customers Only",
]
_BE = {"locations": ["Brussels, Belgium"], "target_roles": ["Nurse"], "industry": "healthcare"}
_DE = {"locations": ["Berlin, Germany"], "target_roles": ["Industrial Painter"], "industry": "automotive"}


def _split_names(data: dict) -> dict:
    entry = channel_funding.regional_budget_split(data, _alloc())["countries"][0]
    return {k: [b["name"] for b in entry.get(k) or []] for k in ("boards", "standby", "post_directly")}


@pytest.mark.parametrize("name", _CONFLICTED)
def test_conflicts_helper_names_the_board(name):
    assert publisher_filter.network_conflicts_for(name)


def test_conflicts_helper_is_empty_for_a_clean_board():
    assert publisher_filter.network_conflicts_for("Indeed") == []
    # a bare brand in network elsewhere is not the Belgian conflict row
    assert publisher_filter.network_conflicts_for("StepStone") == []
    # a bare brand in network elsewhere is not the Belgian conflict row
    assert publisher_filter.network_conflicts_for("StepStone") == []


@pytest.mark.parametrize("name", _CONFLICTED)
def test_nova_countryless_never_confirms_a_conflicted_board(name):
    out = _NOVA._query_publishers({"search_term": name})
    assert out.get("in_joveo_network") is False, out
    assert "not confirmed" in str(out.get("note"))
    assert "supply export" in str(out.get("note"))


def test_nova_countryless_clean_board_says_where_confirmed():
    out = _NOVA._query_publishers({"search_term": "Indeed"})
    assert out.get("in_joveo_network") is True
    assert "confirms it in" in out["note"] or "market is unconfirmed" in out["note"]


def test_belgium_names_are_never_compound_and_not_duplicated():
    names = _split_names(_BE)
    flat = [n for v in names.values() for n in v]
    assert not [n for n in flat if "/" in n or "&" in n], names
    assert len({n.casefold() for n in flat}) == len(flat), names
    for n in flat:
        assert publisher_filter.board_supply_record(n, "Belgium")["state"] == (
            publisher_filter.SUPPLY_IN_NETWORK
        ), n
    assert "Actiris" in flat and "Le Forem" in flat


def test_compound_label_shows_the_row_name():
    assert channel_funding._display_name("Actiris", {"ms_name": "VDAB / Forem / Actiris"}) == "Actiris"
    assert channel_funding._display_name("Xing", {"ms_name": "XING (onlyfy)"}) == "XING (onlyfy)"


def test_germany_post_directly_leads_with_the_employment_service_not_euraxess():
    post = _split_names(_DE)["post_directly"]
    assert post and "Bundesagentur" in post[0], post
    assert not [n for n in post if "uraxess" in n]
    assert not [
        b for b in channel_funding.post_directly_boards({}, "Germany", 40) if "uraxess" in b["name"]
    ]


def test_employment_service_regex_is_not_a_blanket_gov_pass():
    rx = channel_funding._EMPLOYMENT_SERVICE
    for ok in ("Le Forem", "Actiris", "Arbeitsagentur Germany", "werk.nl [Govt]", "JobCentrePlus [Govt]"):
        assert rx.search(ok), ok
    assert not rx.search("Euraxess European Commission website")


def test_workbook_has_no_internal_priority_codes():
    data = dict(_DE)
    data["_budget_allocation"] = _alloc()
    ws = openpyxl.Workbook().active
    excel_v2._build_sheet_channel_recommendations(ws, data)
    cells = [str(c.value) for row in ws.iter_rows() for c in row if c.value is not None]
    assert "Priority" not in cells
    assert not [c for c in cells if c in ("P0", "P1", "P2", "not recorded")]
    assert "Where the regional & aggregator budget goes — Germany" in _sheet_text(ws)
