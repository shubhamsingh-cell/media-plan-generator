"""Two Deactivated repository rows no longer borrow another board's key.

Handoff 2026-10-23 section 4 item 11: the board matcher strips domains and
corporate suffixes, so "Talent Inc." (Deactivated) and "Talent.com" (In Joveo
Network) shared the key "talent" (Talent Inc. read as in network, and an
exclusion of one excluded the other), and "Recruit.net" (Deactivated) shared
"recruit" with the Recruit row, so the Japanese "Recruit (Rikunabi / Indeed
Japan)" compound took Recruit.net's Deactivated Japan row. Those two boards
now match by their exact name only. Both directions are tested.
"""

from __future__ import annotations

import publisher_filter as pf


def test_talent_inc_and_talent_com_are_different_boards():
    assert not pf.names_match("Talent Inc.", "Talent.com")
    assert not pf.names_match("Talent.com", "Talent Inc.")
    assert pf.names_match("Talent Inc.", "Talent Inc")
    # Talent.com's own spellings still find it
    for alias in ("Talent.com (formerly Neuvoo)", "Talent.com Spain", "talent.com"):
        assert pf.names_match(alias, "Talent.com"), alias


def test_talent_inc_reads_as_its_own_deactivated_row():
    rec = pf.board_supply_record("Talent Inc.", "United States")
    assert rec["state"] == pf.SUPPLY_NOT_IN_NETWORK
    assert rec["status"] == "Deactivated"
    assert pf.board_supply_record("Talent.com", "United States")["state"] == (
        pf.SUPPLY_IN_NETWORK
    )


def test_excluding_one_talent_board_keeps_the_other():
    data = {"excluded_publishers": ["Talent Inc."]}
    assert pf.is_excluded("Talent Inc.", data)
    assert not pf.is_excluded("Talent.com", data)


def test_recruit_compound_does_not_take_recruit_net_s_row():
    rec = pf.board_supply_record("Recruit (Rikunabi / Indeed Japan)", "Japan")
    head = rec["parts"][0]
    assert head["name"].lower().startswith("recruit")
    assert head["state"] == pf.SUPPLY_NO_RECORD, rec
    assert not pf.names_match("Recruit (Rikunabi / Indeed Japan)", "Recruit.net")
    # Recruit.net still finds its own row, and only it
    assert pf.board_supply_record("Recruit.net", "Japan")["status"] == "Deactivated"
    assert not pf.names_match("Recruit.net", "Recruit")


def test_the_deactivated_yolo_row_does_not_borrow_yolo_japan_s_record():
    import channel_funding as cf

    assert not pf.names_match("Yolo", "YOLO JAPAN")
    rec = pf.board_supply_record("Yolo", "Japan")
    assert rec["state"] == pf.SUPPLY_NOT_IN_NETWORK and rec["status"] == "Deactivated"
    assert pf.board_supply_record("YOLO JAPAN", "Japan")["state"] == pf.SUPPLY_IN_NETWORK
    data = {"locations": ["Tokyo, Japan"], "target_roles": ["Warehouse Associate"]}
    rows = [b["row_name"] for b in cf.post_directly_boards(data, "Japan")] + [
        b["row_name"] for b in cf.ranked_boards(data, "Japan")
    ]
    assert "Yolo" not in rows, rows


def test_talent_portugal_is_its_own_board_not_talent_com():
    assert not pf.names_match("Talent Portugal", "Talent.com")
    # Talent.com's own Portugal market spelling still finds Talent.com
    assert pf.names_match("Talent.com Portugal", "Talent.com")
    names = [b["name"] for b in pf.in_network_job_boards("Portugal")]
    assert "Talent Portugal" in names and "Talent.com" in names, names


def test_excluding_talent_com_keeps_the_other_talent_boards_and_back():
    import channel_funding as cf

    excl_com = {"excluded_publishers": ["Talent.com"]}
    assert pf.is_excluded("Talent.com", excl_com)
    assert not pf.is_excluded("Talent Portugal", excl_com, "Portugal")
    assert not pf.is_excluded("Talent Inc.", excl_com)
    excl_pt = {"excluded_publishers": ["Talent Portugal"]}
    assert pf.is_excluded("Talent Portugal", excl_pt, "Portugal")
    assert not pf.is_excluded("Talent.com", excl_pt, "Portugal")
    data = dict(excl_com, locations=["Lisbon, Portugal"], target_roles=["Sales Associate"])
    rows = [b["row_name"] for b in cf.post_directly_boards(data, "Portugal", limit=20)]
    assert "Talent Portugal" in rows, rows


def test_the_pipeline_search_finds_an_exact_name_board(monkeypatch):
    # a fragment that is itself an exact-name board ("yolo" keys "=yolo")
    # must still find a pipeline partner keyed by the stripped key ("yolo")
    partner = {"name": "YOLO JAPAN", "regions": ["Japan"], "stage": "Contract signed",
               "status": "Done"}
    pipe = {"as_of": "2026-11-03", "stages": ["Contract signed", "Live"],
            "by_key": {pf.normalize("YOLO JAPAN"): [partner]}}
    monkeypatch.setattr(pf, "_pipeline", lambda: pipe)
    assert [r["name"] for r in pf.onboarding_pipeline("yolo")] == ["YOLO JAPAN"]
    assert [r["name"] for r in pf.onboarding_pipeline("Yolo")] == ["YOLO JAPAN"]
