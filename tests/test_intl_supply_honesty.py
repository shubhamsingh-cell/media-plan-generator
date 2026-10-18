"""Boards and supply honesty on non-US plans (international audit 2026-10-18).

What these pin, each of which failed before the fix:

* #1  a local board nothing in the supply data covers for the country carried
      NO marker ("unknown" was treated as "fine"); "Pole Emploi (France
      Travail)" and the repository's "France Travail France" were two
      unrelated names, so a VONQ-only board led the French list unmarked;
      the dated record behind a board was not exposed; no surface said how
      much of the in-network list a plan shows.
* #5  the footers said "supply network 2026-10-17" (the newest single-board
      edit) and the deck footer dropped the repository's own date.
* #10 the workbook had no Local Job Boards table for 4 of 23 industries
      (general_entry_level, legal_services, military_recruitment, rideshare)
      while deck slide 5 still named the boards.

Review round (2026-10-18, Opus verify): a barred board must never read as
"in network" (OCC Mundial / HelloWork), the depth figure counts only
in-network JOB BOARDS the repository names for the country, a combined entry
is confirmed only if every board it names is, statuses print with their date,
the Local Job Boards table follows the Global / Regional ticks, and a
disagreement between global_supply.json's notes and the repository is said.

Dates are read from the data files in the test, never typed.  The depth
counts are the one exception: they are hand-checked literals (see
test_depth_counts_are_hand_checked_literals), because a test that recomputes
the figure with the code's own method approves whatever the method gets wrong.
"""

from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402
from pptx import Presentation  # noqa: E402

import excel_v2  # noqa: E402
import plan_trust  # noqa: E402
import ppt_generator  # noqa: E402
import publisher_filter as pf  # noqa: E402
import research  # noqa: E402

DATA_DIR = PROJECT_ROOT / "data"
REPO = json.loads((DATA_DIR / "joveo_global_supply_repository.json").read_text())
GROUPS = json.loads((DATA_DIR / "joveo_network_groups.json").read_text())
INTL = json.loads((DATA_DIR / "international_benchmarks_2026.json").read_text())
IN_NETWORK = "In Joveo Network"


# ---------------------------------------------------------------------------
# helpers (independent of publisher_filter's own matching on purpose)
# ---------------------------------------------------------------------------


def _repo_rows() -> List[Dict[str, Any]]:
    return [p for r in REPO["by_region"].values() for p in r["publishers"]]


def _row_countries(row: Dict[str, Any]) -> Set[str]:
    raw = str(row.get("countries") or "")
    if raw.strip().lower() in ("", "nan"):
        return set()
    return {c.strip() for c in raw.split(",") if c.strip()}


def _repo_row(name: str) -> Dict[str, Any]:
    return next(p for p in _repo_rows() if p["name"] == name)


def _plan(industry: str = "general_entry_level", loc: str = "Hanoi, Vietnam") -> dict:
    plan = {
        "client_name": "Fixture Co",
        "company_name": "Fixture Co",
        "industry": industry,
        "budget": "$100,000",
        "locations": [loc],
        "roles": ["Agent"],
        "target_roles": ["Agent"],
        "campaign_duration": "3 months",
        "hire_volume": "50",
        "work_environment": "onsite",
        "_enriched": {},
        "_synthesized": {},
        "_budget_allocation": {},
        # what app.py hands the generators for a non-US market (adds the
        # Intl Benchmarks sheet)
        "_intl_benchmarks": INTL,
    }
    return plan


def _workbook(plan: dict):
    raw = excel_v2.generate_excel_v2(dict(plan), research_mod=research)
    return openpyxl.load_workbook(io.BytesIO(raw))


def _cells(ws) -> List[str]:
    return [
        str(v) for row in ws.iter_rows(values_only=True) for v in row if v is not None
    ]


def _deck_texts(plan: dict) -> List[str]:
    prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(plan))))
    out: List[str] = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.has_text_frame and shape.text_frame.text.strip():
                out.append(shape.text_frame.text)
    return out


# ---------------------------------------------------------------------------
# #1 (a): a named board with no supply record is marked, not assumed
# ---------------------------------------------------------------------------


def test_board_nothing_covers_is_unverified_everywhere():
    name, country = "Zzyzx Careers Portal", "Vietnam"
    record = pf.board_supply_record(name, country)
    assert record["state"] == pf.SUPPLY_NO_RECORD
    assert record["as_of"] == "" and record["status"] == ""
    assert pf.relationship_unverified(name, country) is True
    assert pf.annotate([name], country) == ([f"{name} {pf.RELATIONSHIP_MARK}"], True)


@pytest.mark.parametrize(
    "board,country,key",
    [
        ("TopCV.vn", "Vietnam", "topcv"),
        ("GulfTalent", "United Arab Emirates", "gulftalent"),
        ("InfoJobs Brazil", "Brazil", "infojobs"),
        ("MyCareersFuture (MCF)", "Singapore", "mycareersfuture"),
    ],
)
def test_the_audits_unrecorded_boards_are_marked(board, country, key):
    # guard: the repository really has no row for the board in that country
    covering = [
        p
        for p in _repo_rows()
        if re.sub(r"[^a-z0-9]", "", p["name"].lower()).startswith(key)
        and (country in _row_countries(p) or "Global" in _row_countries(p))
    ]
    assert not covering, covering
    assert pf.relationship_unverified(board, country) is True
    assert pf.annotate([board], country)[1] is True


def test_in_network_board_is_never_marked():
    assert pf.relationship_unverified("VietnamWorks", "Vietnam") is False
    assert pf.relationship_unverified("Totaljobs", "United Kingdom") is False
    assert pf.annotate(["Reed.co.uk"], "United Kingdom") == (["Reed.co.uk"], False)


def test_combined_entry_is_confirmed_only_if_every_board_it_names_is():
    # Indeed has a (global) in-network row, Recruit and Rikunabi have none:
    # one confirmed part must not clear the others
    name = "Recruit (Rikunabi / Indeed Japan)"
    record = pf.board_supply_record(name, "Japan")
    parts = {p["name"].lower(): p["state"] for p in record["parts"]}
    assert parts == {
        "recruit": pf.SUPPLY_NO_RECORD,
        "rikunabi": pf.SUPPLY_NO_RECORD,
        "indeed japan": pf.SUPPLY_IN_NETWORK,
    }
    assert record["state"] == pf.SUPPLY_NO_RECORD
    assert record["as_of"] == ""
    assert pf.relationship_unverified(name, "Japan") is True
    assert pf.annotate([name], "Japan") == ([f"{name} {pf.RELATIONSHIP_MARK}"], True)
    # the same rule for a slash compound (Belgium: only Actiris has a row)
    slash = pf.board_supply_record("VDAB / Forem / Actiris", "Belgium")
    assert slash["state"] == pf.SUPPLY_NO_RECORD
    assert pf.relationship_unverified("VDAB / Forem / Actiris", "Belgium") is True


def test_combined_entry_is_in_network_when_every_part_is():
    name = "Indeed Japan / LinkedIn Japan"
    record = pf.board_supply_record(name, "Japan")
    assert record["state"] == pf.SUPPLY_IN_NETWORK
    assert record["as_of"] == REPO["last_updated"]  # the oldest of its parts
    assert pf.relationship_unverified(name, "Japan") is False


def test_a_lone_parenthetical_is_a_note_not_a_second_board():
    # "(France Travail)" is the same board; it resolves through the alias
    assert pf._compound_parts("Pole Emploi (France Travail)") == []
    assert pf._compound_parts("Recruit (Rikunabi / Indeed Japan)") == [
        "Recruit",
        "Rikunabi",
        "Indeed Japan",
    ]


# ---------------------------------------------------------------------------
# #1 (b): a rename resolves to ONE record
# ---------------------------------------------------------------------------


def test_france_travail_and_pole_emploi_are_one_record():
    row = _repo_row("France Travail France")
    assert row["status"] != IN_NETWORK  # the data says so; the plan must too
    spellings = [
        "Pole Emploi (France Travail)",
        "Pôle Emploi",
        "France Travail",
        "France Travail France",
    ]
    records = [pf.board_supply_record(s, "France") for s in spellings]
    assert {r["state"] for r in records} == {pf.SUPPLY_NOT_IN_NETWORK}
    assert {r["status"] for r in records} == {row["status"]}
    assert pf.names_match("Pole Emploi (France Travail)", "France Travail France")
    assert pf.relationship_unverified("Pole Emploi (France Travail)", "France") is True


@pytest.mark.parametrize(
    "name,country,row_name",
    [
        ("Mynavi", "Japan", "Mynavi Corporation"),
        ("Bundesagentur fuer Arbeit (Jobboerse)", "Germany", "Arbeitsagentur Germany"),
    ],
)
def test_name_variants_with_a_repository_row_resolve_to_it(name, country, row_name):
    row = _repo_row(row_name)
    assert country in _row_countries(row) and row["status"] == IN_NETWORK
    record = pf.board_supply_record(name, country)
    assert record["state"] == pf.SUPPLY_IN_NETWORK
    assert pf.relationship_unverified(name, country) is False


def test_alias_groups_stay_sourced():
    raw = json.loads((DATA_DIR / "publisher_availability.json").read_text())
    for group in raw["alias_groups"]:
        assert str(group.get("source") or "").strip(), group


# ---------------------------------------------------------------------------
# #1 (c): the dated record is exposed, nothing invented
# ---------------------------------------------------------------------------


def test_record_carries_only_dates_the_data_holds():
    # a confirmed group: the group's own as_of
    group_as_of = next(
        g["as_of"] for g in GROUPS["groups"] if g["id"] == "stepstone_group"
    )
    confirmed = pf.board_supply_record("Totaljobs", "United Kingdom")
    assert confirmed["as_of"] == group_as_of
    assert pf.status_label(confirmed) == f"In Joveo Network, as of {group_as_of}"
    # a repository row: the repository's own last_updated
    plain = pf.board_supply_record("VietnamWorks", "Vietnam")
    assert plain["as_of"] == REPO["last_updated"] == pf.repository_date()
    # no record: no status and no date to show
    nothing = pf.board_supply_record("Zzyzx Careers Portal", "Vietnam")
    assert nothing["as_of"] == "" and pf.status_label(nothing) == ""


def test_the_date_always_follows_the_status_it_belongs_to():
    # a VONQ-only board must never read as "available as of <date>"
    row = _repo_row("France Travail France")
    vonq = pf.board_supply_record("Pole Emploi (France Travail)", "France")
    assert pf.status_label(vonq) == f"{row['status']}, as of {REPO['last_updated']}"
    assert pf.supply_status_phrase(vonq) == (
        f"Pole Emploi (France Travail): {row['status']}, as of "
        f"{REPO['last_updated']} (relationship unverified)"
    )
    for record in (vonq, pf.board_supply_record("VietnamWorks", "Vietnam")):
        assert "availability as of" not in pf.supply_status_phrase(record)
        assert "availability as of" not in pf.status_label(record)


def test_status_phrase_for_a_board_with_no_row_is_literally_true():
    phrase = pf.supply_status_phrase(
        pf.board_supply_record("Zzyzx Careers Portal", "Vietnam")
    )
    assert phrase == (
        "Zzyzx Careers Portal: relationship unverified for this country "
        "(repository has no row for Vietnam)"
    )
    # MyCareersFuture: the repository does hold a row whose PARENT names it,
    # filed under another country -- "no supply record" would be false
    mcf = pf.supply_status_phrase(
        pf.board_supply_record("MyCareersFuture (MCF)", "Singapore")
    )
    assert "no supply record" not in mcf
    assert "repository has no row for Singapore" in mcf


# ---------------------------------------------------------------------------
# a barred board is "unavailable", never "in network" (OCC Mundial, HelloWork)
# ---------------------------------------------------------------------------

AVAILABILITY = json.loads((DATA_DIR / "publisher_availability.json").read_text())


def _barred(name: str) -> Dict[str, Any]:
    return next(e for e in AVAILABILITY["entries"] if e["name"] == name)


@pytest.mark.parametrize(
    "board,country,entry",
    [
        ("OCC Mundial", "Mexico", "OCC Mundial"),
        ("OCC", "Mexico", "OCC Mundial"),
        ("HelloWork", "France", "HelloWork"),
        ("HelloWork (formerly RegionsJob)", "France", "HelloWork"),
    ],
)
def test_a_barred_board_is_unavailable_with_the_files_own_date(board, country, entry):
    barred = _barred(entry)
    # the repository still lists OCC as in network: the bar must win
    record = pf.board_supply_record(board, country)
    assert record["state"] == pf.SUPPLY_UNAVAILABLE
    assert record["status"] == "Not available through Joveo"
    assert record["as_of"] == barred["as_of"]
    assert (
        pf.status_label(record)
        == f"Not available through Joveo, as of {barred['as_of']}"
    )
    assert pf.relationship_unverified(board, country) is True
    assert "in network" not in pf.supply_status_phrase(record).lower()


def test_a_bar_is_scoped_to_its_country_and_exits_read_plainly():
    assert (
        pf.board_supply_record("OCC Mundial", "Canada")["state"]
        != pf.SUPPLY_UNAVAILABLE
    )
    exited = pf.board_supply_record("ZipRecruiter", "United Kingdom")
    assert exited["state"] == pf.SUPPLY_UNAVAILABLE
    assert exited["status"] == "No longer operating in this market"


def test_market_band_exposes_the_record_of_every_board_shown():
    (band,) = pf.plan_market_boards({"locations": ["Paris, France"]})
    assert len(band["records"]) == len(band["boards"])
    for record, shown in zip(band["records"], band["boards"]):
        assert shown.rstrip(f" {pf.RELATIONSHIP_MARK}") == record["name"]
        assert shown.endswith(pf.RELATIONSHIP_MARK) == (
            record["state"] != pf.SUPPLY_IN_NETWORK
        )


# ---------------------------------------------------------------------------
# #1 (d): confirmed boards first among peers; no leader is hidden
# ---------------------------------------------------------------------------


def _with_shares(monkeypatch, shares: Dict[str, float]) -> None:
    monkeypatch.setattr(
        pf,
        "_dataset_shares",
        lambda country: {pf.normalize(k): v for k, v in shares.items()},
    )


def test_confirmed_boards_precede_equal_rank_boards_without_a_record(monkeypatch):
    # synthetic shares: the #1 board (22%) has no confirmed record; Indeed
    # (20%) and LinkedIn (18%) are peers (within 5 points) and in network
    _with_shares(
        monkeypatch,
        {
            "Pole Emploi (France Travail)": 22,
            "Indeed France": 20,
            "LinkedIn France": 18,
        },
    )
    ordered = pf.supply_ordered(
        ["Pole Emploi (France Travail)", "Indeed France", "LinkedIn France"], "France"
    )
    assert ordered == [
        "Indeed France",
        "LinkedIn France",
        "Pole Emploi (France Travail)",
    ]


def test_a_board_that_is_not_a_peer_keeps_its_rank(monkeypatch):
    _with_shares(
        monkeypatch,
        {"VietnamWorks": 30, "TopCV.vn": 20, "LinkedIn Vietnam": 12},
    )
    names = ["VietnamWorks", "TopCV.vn", "LinkedIn Vietnam"]
    assert pf.supply_ordered(names, "Vietnam") == names


def test_market_leading_board_without_a_record_stays_and_is_marked():
    (band,) = pf.plan_market_boards({"locations": ["Paris, France"]})
    assert "Pole Emploi (France Travail) †" in band["boards"]
    (vn,) = pf.plan_market_boards({"locations": ["Hanoi, Vietnam"]})
    assert "TopCV.vn †" in vn["boards"]
    for band in (band, vn):
        for record, shown in zip(band["records"], band["boards"]):
            assert shown.endswith(pf.RELATIONSHIP_MARK) == (
                record["state"] != pf.SUPPLY_IN_NETWORK
            )


def test_ordering_never_adds_or_removes_a_board():
    names = ["MyCareersFuture (MCF)", "LinkedIn Singapore", "JobStreet Singapore"]
    assert sorted(pf.supply_ordered(names, "Singapore")) == sorted(names)
    assert pf.supply_ordered(names, "") == names  # no country -> untouched
    assert pf.supply_ordered(["One"], "Singapore") == ["One"]


# ---------------------------------------------------------------------------
# supply-depth line: counted from the repository or skipped
# ---------------------------------------------------------------------------


# Hand-checked from data/joveo_global_supply_repository.json (last_updated
# 2026-03-31): rows whose status is "In Joveo Network", whose countries column
# NAMES the market (no "Global" rows), and whose category is a job-board
# category (Universal, University Job Board, DEI, Govt, Health, Tech,
# Classifieds, Free, Veterinary).  Counted once per distinct name.
#   Vietnam 13   13 rows spell "Vietnam"; 2 more in-network rows naming it are
#                not boards (9cv9 = DSP, Athyna = Staffing Partner)
#   France 102   102 distinct names ("Job Matrix" / "The Job Matrix" and
#                "KelJob" / "The Network: Keljob (France)" are separate rows)
#   United Kingdom 240   235 rows spell "United Kingdom" + 5 spell "UK"
# Update these when the repository is refreshed.  Before the fix the line said
# 48 / 134 / 281: it added the repository's 33 global in-network rows (Spotify,
# Amazon Ads, Uber Ads, Taboola, influencer tools ...).
_HAND_COUNTED_BOARDS = {"Vietnam": 13, "France": 102, "United Kingdom": 240}


def test_depth_counts_are_hand_checked_literals():
    for country, total in _HAND_COUNTED_BOARDS.items():
        assert pf.supply_depth(country, [])["total"] == total, country


def test_depth_counts_no_global_platform_and_no_non_board():
    boards = pf._repo_network_boards("Vietnam")
    keys = set(boards.values())
    assert {"vietnamworks", "careerviet", "jobstreet"} <= keys
    # global ad / social / audio / influencer rows and the DSP and staffing
    # partner rows that DO name Vietnam are not job boards
    for platform in (
        "spotify",
        "amazon",
        "uber",
        "taboola",
        "creatoriq",
        "upfluence",
        "facebook",
        "9cv9",
        "athyna",
    ):
        assert platform not in keys, platform
    assert len(boards) == _HAND_COUNTED_BOARDS["Vietnam"]


def test_shown_counts_only_boards_the_repository_names_for_the_country():
    # VietnamWorks has a Vietnam row; LinkedIn runs everywhere (a Global row);
    # TopCV.vn has no row at all
    shown = ["VietnamWorks", "TopCV.vn", "LinkedIn Vietnam"]
    assert pf.supply_depth("Vietnam", shown) == {
        "country": "Vietnam",
        "shown": 1,
        "total": _HAND_COUNTED_BOARDS["Vietnam"],
    }
    assert pf.supply_depth("Vietnam", ["VietnamWorks", "CareerViet"])["shown"] == 2
    # a combined entry counts only if every board it names is counted
    assert pf.supply_depth("Japan", ["Recruit (Rikunabi / Indeed Japan)"])["shown"] == 0


def test_depth_line_says_how_boards_are_chosen_and_nothing_untrue():
    depth = pf.supply_depth("Vietnam", ["VietnamWorks", "TopCV.vn", "LinkedIn Vietnam"])
    line = pf.supply_depth_line([{"depth": depth}])
    assert line == (
        "Boards shown are chosen by market share, not matched to the role. "
        "The repository names 13 in-network job boards for Vietnam; 1 is among "
        "those shown."
    )
    assert "role-matched" not in line


def test_depth_figure_is_left_out_when_it_cannot_be_derived():
    assert pf.supply_depth("Atlantis", ["Anything"]) is None
    assert pf.supply_depth("", ["Anything"]) is None
    only = pf.supply_depth_line([{"depth": None}])
    assert only == "Boards shown are chosen by market share, not matched to the role."
    assert pf.supply_depth_line([]) == ""
    two = pf.supply_depth_line(
        [
            {"depth": {"country": "France", "shown": 2, "total": 9}},
            {"depth": {"country": "Germany", "shown": 3, "total": 12}},
        ]
    )
    assert "France 2 of 9" in two and "Germany 3 of 12" in two
    assert two.startswith(only) and "role-matched" not in two.replace(only, "")


# ---------------------------------------------------------------------------
# every surface: deck slide 5, workbook, Intl Benchmarks, Nova
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def vn_surfaces():
    plan = _plan("general_entry_level", "Hanoi, Vietnam")
    return {"wb": _workbook(plan), "deck": _deck_texts(plan)}


def test_deck_slide_five_marks_names_and_counts(vn_surfaces):
    deck = vn_surfaces["deck"]
    assert any(t.startswith("Vietnam:") and "TopCV.vn †" in t for t in deck)
    assert pf.RELATIONSHIP_NOTE in deck
    depth = pf.supply_depth(
        "Vietnam",
        [b.rstrip(f" {pf.RELATIONSHIP_MARK}") for b in _vn_deck_boards(deck)],
    )
    assert pf.supply_depth_line([{"depth": depth}]) in deck


def _vn_deck_boards(deck: List[str]) -> List[str]:
    line = next(t for t in deck if t.startswith("Vietnam:"))
    return [b.strip() for b in line.split(":", 1)[1].split(",")]


def test_workbook_local_boards_and_intl_benchmarks_carry_the_marker(vn_surfaces):
    wb = vn_surfaces["wb"]
    channels = _cells(wb["Channels & Strategy"])
    assert any("TopCV.vn †" in c for c in channels)
    assert pf.RELATIONSHIP_NOTE in channels
    intl = _cells(wb["Intl Benchmarks"])
    assert any("TopCV.vn †" in c for c in intl)
    assert any(pf.RELATIONSHIP_NOTE in c for c in intl)


def test_workbook_prints_each_status_with_its_date(vn_surfaces):
    channels = _cells(vn_surfaces["wb"]["Channels & Strategy"])
    status = next(c for c in channels if c.startswith("VietnamWorks: In Joveo Network"))
    assert f"VietnamWorks: In Joveo Network, as of {REPO['last_updated']}" in status
    assert (
        "TopCV.vn: relationship unverified for this country "
        "(repository has no row for Vietnam)" in status
    )
    assert "availability as of" not in status
    # no invented date on the board without a row
    assert not re.search(r"TopCV\.vn[^;.]*as of", status)


def test_workbook_depth_line_matches_the_deck_method(vn_surfaces):
    channels = _cells(vn_surfaces["wb"]["Channels & Strategy"])
    depth_cells = [c for c in channels if "in-network job boards for Vietnam" in c]
    assert len(depth_cells) == 1
    assert depth_cells[0].startswith(pf.SELECTION_NOTE)
    assert "role-matched" not in depth_cells[0]
    assert f"names {_HAND_COUNTED_BOARDS['Vietnam']} in-network job boards" in (
        depth_cells[0]
    )


def test_workbook_carries_the_key_to_the_statuses(vn_surfaces):
    channels = _cells(vn_surfaces["wb"]["Channels & Strategy"])
    assert pf.SUPPLY_STATUS_KEY in channels
    for phrase in (
        "In Joveo Network",
        "VONQ Only",
        "Relationship unverified",
        "Not available",
    ):
        assert phrase in pf.SUPPLY_STATUS_KEY


def _nova() -> Any:
    from nova import Nova

    nova = Nova.__new__(Nova)
    nova._data_cache = {"international_benchmarks": INTL}
    return nova


def test_nova_ad_platform_tool_carries_the_same_verdict():
    result = _nova()._query_ad_platform(
        {"role_type": "professional", "country": "Vietnam"}
    )
    status = {row["name"]: row for row in result["supply_status"]}
    assert status["TopCV.vn"]["state"] == pf.SUPPLY_NO_RECORD
    assert (
        status["TopCV.vn"]["as_of"] == "" and status["TopCV.vn"]["status_as_of"] == ""
    )
    assert status["TopCV.vn"]["relationship_note"] == (
        "relationship unverified for this country "
        "(repository has no row for Vietnam)"
    )
    assert status["VietnamWorks"]["state"] == pf.SUPPLY_IN_NETWORK
    assert status["VietnamWorks"]["as_of"] == REPO["last_updated"]
    assert status["VietnamWorks"]["status_as_of"] == (
        f"In Joveo Network, as of {REPO['last_updated']}"
    )
    assert "relationship_note" not in status["VietnamWorks"]
    assert result["supply_note"] == pf.RELATIONSHIP_NOTE
    # nothing is re-ranked or hidden by the annotation
    assert result["top_platforms"][0] == result["platforms_ranked"][0]["name"]


@pytest.mark.parametrize(
    "board,country,entry",
    [("OCC Mundial", "Mexico", "OCC Mundial"), ("HelloWork", "France", "HelloWork")],
)
def test_every_nova_tool_keeps_a_barred_board_out_and_says_it_is_unavailable(
    board, country, entry
):
    barred = _barred(entry)
    nova = _nova()
    ad = nova._query_ad_platform({"role_type": "hourly", "country": country})
    collar = nova._query_collar_strategy(
        {"role": "Retail Associate", "country": country}
    )
    bench = nova._query_recruitment_benchmarks(
        {"industry": "retail", "country": country}
    )
    named = {
        "ad_platform": [p["name"] for p in ad["platforms_ranked"]]
        + ad["top_platforms"],
        "collar": collar["recommended_strategy"]["preferred_platforms"],
        "benchmarks": [p["name"] for p in bench["top_platforms"]],
    }
    needle = board.split()[0].lower()
    for tool, names in named.items():
        assert not [n for n in names if needle in n.lower()], (tool, names)
    for result in (ad, collar, bench):
        # whatever the tool says about the board is the bar -- never "in network"
        assert all(needle not in r["name"].lower() for r in result["supply_status"])
        (note,) = [
            u for u in result["unavailable_boards"] if needle in u["name"].lower()
        ]
        assert note["note"] == f"Not available through Joveo, as of {barred['as_of']}"
    assert (
        "in network"
        not in json.dumps(
            [r for res in (ad, collar, bench) for r in res["unavailable_boards"]]
        ).lower()
    )


def test_knowledge_base_and_recommender_do_not_list_a_barred_board():
    import channel_recommender

    nova = _nova()
    blocks = [nova._intl_country_data("Mexico"), nova._intl_country_data("France")]
    assert not [
        p["name"]
        for b in blocks
        for p in b["platforms"]
        if "OCC" in p["name"] or "HelloWork" in p["name"]
    ]
    local = [
        p["name"]
        for loc in ("Monterrey, Mexico", "Paris, France")
        for p in channel_recommender._get_intl_local_platforms([loc])
    ]
    assert local and not [n for n in local if "OCC" in n or "HelloWork" in n]
    mx = channel_recommender._enrich_with_global_supply(["Mexico"])
    tier1 = mx["country_intelligence"]["Mexico"]["tier_1_boards"]
    assert not [n for n in tier1 if "OCC" in n]
    rec = channel_recommender.recommend_channels(
        "retail_consumer", "Retail Associate", 100000, ["Monterrey, Mexico"]
    )
    assert "OCC" not in json.dumps(rec, default=str)


def test_every_nova_tool_that_reads_the_local_board_list_carries_the_verdict():
    nova = _nova()
    benchmarks = nova._query_recruitment_benchmarks(
        {"industry": "retail", "country": "Vietnam"}
    )
    collar = nova._query_collar_strategy(
        {"role": "Call Centre Agent", "country": "Vietnam"}
    )
    for result in (benchmarks, collar):
        states = {r["name"]: r["state"] for r in result["supply_status"]}
        assert states["TopCV.vn"] == pf.SUPPLY_NO_RECORD, result.keys()
        assert states["VietnamWorks"] == pf.SUPPLY_IN_NETWORK
        assert result["supply_note"] == pf.RELATIONSHIP_NOTE


@pytest.mark.parametrize(
    "country,board",
    [("Singapore", "MyCareersFuture (Govt)"), ("Brazil", "InfoJobs Brazil")],
)
def test_global_supply_tool_surfaces_the_disagreement_with_the_repository(
    country, board
):
    # the notes file says "Confirmed part of Joveo network" for these boards;
    # the repository has no row for them in the country
    supply = json.loads((DATA_DIR / "global_supply.json").read_text())
    entry = next(
        b for b in supply["country_job_boards"][country]["boards"] if b["name"] == board
    )
    assert "Confirmed part of Joveo network" in entry["_verification_notes"]
    nova = _nova()
    nova._data_cache["global_supply"] = supply
    out = nova._query_global_supply({"country": country})
    row = next(b for b in out["country_boards"]["boards"] if b["name"] == board)
    assert row["relationship"] == "unverified"
    assert row["relationship_note"] == (
        "relationship unverified for this country (supply notes list it as "
        f"confirmed; repository has no row for {country})"
    )


def test_a_us_query_gets_no_supply_annotation():
    result = _nova()._query_ad_platform({"role_type": "professional", "country": ""})
    assert "supply_status" not in result and "supply_note" not in result


# ---------------------------------------------------------------------------
# new copy stays free of US-only vocabulary on every market
# ---------------------------------------------------------------------------


def test_new_copy_carries_no_us_only_marker():
    from us_only_markers import has_us_only_marker

    info = plan_trust.data_asof({})
    records = [
        pf.board_supply_record(n, "France")
        for n in ("Indeed France", "APEC", "Zzyzx Careers Portal")
    ]
    copy = [
        info["headline"],
        info["footer_headline"],
        pf.RELATIONSHIP_NOTE,
        pf.supply_depth_line([{"depth": pf.supply_depth("France", ["Indeed France"])}]),
        *[pf.supply_status_phrase(r) for r in records],
        *[pf.status_label(r) for r in records],
        pf.unverified_note(
            pf.board_supply_record("GulfTalent", "United Arab Emirates")
        ),
        pf.SUPPLY_STATUS_KEY,
    ]
    copy = [t for t in copy if t]  # a board with no record has no date label
    assert len(copy) >= 8
    assert not [t for t in copy if has_us_only_marker(t)]


# ---------------------------------------------------------------------------
# #10: the workbook lists local boards for every industry
# ---------------------------------------------------------------------------

_NO_NICHE_LIST = [
    "general_entry_level",
    "legal_services",
    "military_recruitment",
    "rideshare",
]


@pytest.mark.parametrize("industry", _NO_NICHE_LIST)
def test_workbook_lists_local_boards_for_industries_without_a_niche_list(industry):
    assert not excel_v2.INDUSTRY_NICHE_CHANNELS.get(industry)
    wb = _workbook(_plan(industry, "Paris, France"))
    ws = wb["Channels & Strategy"]
    cells = _cells(ws)
    boards = next(c for c in cells if "Indeed France" in c and "," in c)
    assert "Pole Emploi (France Travail) †" in boards
    assert any(c.upper() == "LOCAL JOB BOARDS" for c in cells)
    # no niche section is invented for an industry that has none
    assert not any("NICHE CHANNELS" in c.upper() for c in cells)
    assert not any("US-domiciled niche boards" in c for c in cells)


def test_industry_with_a_niche_list_keeps_its_niche_section_and_note():
    assert excel_v2.INDUSTRY_NICHE_CHANNELS.get("healthcare_medical")
    cells = _cells(
        _workbook(_plan("healthcare_medical", "Paris, France"))["Channels & Strategy"]
    )
    assert any(c.upper().startswith("NICHE CHANNELS: ") for c in cells)
    assert any("US-domiciled niche boards" in c for c in cells)
    assert any("Indeed France" in c and "," in c for c in cells)


def test_workbook_names_every_board_the_deck_names():
    plan = _plan("rideshare", "Hanoi, Vietnam")
    deck_boards = {
        b.rstrip(f" {pf.RELATIONSHIP_MARK}") for b in _vn_deck_boards(_deck_texts(plan))
    }
    row = next(
        c
        for c in _cells(_workbook(plan)["Channels & Strategy"])
        if "VietnamWorks" in c and "," in c
    )
    for board in deck_boards:
        assert board in row, (board, row)


def test_us_plan_gets_no_local_boards_block():
    cells = _cells(
        _workbook(_plan("general_entry_level", "Dallas, TX"))["Channels & Strategy"]
    )
    assert not any(c.upper() == "LOCAL JOB BOARDS" for c in cells)
    assert not any("in-network boards for" in c for c in cells)
    assert pf.plan_market_boards({"locations": ["Dallas, TX"]}) == []


# ---------------------------------------------------------------------------
# a barred board never reaches the deck or the workbook; a conflict is said
# ---------------------------------------------------------------------------

_BARRED_RE = re.compile(r"\bOCC\b|OCC Mundial|HelloWork|RegionsJob|occ\.com\.mx")


@pytest.mark.parametrize(
    "industry,loc",
    [("blue_collar_trades", "Monterrey, Mexico"), ("retail_consumer", "Paris, France")],
)
def test_deck_and_workbook_never_name_a_barred_board(industry, loc):
    plan = _plan(industry, loc)
    texts = _deck_texts(plan)
    wb = _workbook(plan)
    for ws in wb.worksheets:
        texts += _cells(ws)
    assert not [t for t in texts if _BARRED_RE.search(t)], [
        t[:80] for t in texts if _BARRED_RE.search(t)
    ]


def test_deck_marks_a_combined_entry_with_one_unconfirmed_board():
    deck = _deck_texts(_plan("education", "Tokyo, Japan"))
    line = next(t for t in deck if t.startswith("Japan:"))
    assert "Recruit (Rikunabi / Indeed Japan) †" in line


def test_workbook_says_when_the_notes_and_the_repository_disagree():
    wb = _workbook(_plan("finance_banking", "Singapore"))
    row = next(
        c
        for c in _cells(wb["Channels & Strategy"])
        if c.startswith("MyCareersFuture (MCF): relationship unverified")
        or "MyCareersFuture (MCF): relationship unverified" in c
    )
    assert (
        "MyCareersFuture (MCF): relationship unverified for this country "
        "(supply notes list it as confirmed; repository has no row for Singapore)"
    ) in row
    assert "no supply record" not in row
    cells = _cells(wb["Channels & Strategy"])
    assert not any("no supply record" in c for c in cells)


# ---------------------------------------------------------------------------
# Local Job Boards follows the Global / Regional ticks, never the Niche tick
# ---------------------------------------------------------------------------

_OTHERS_ON = {
    "programmatic_dsp": True,
    "social_media": True,
    "employer_branding": True,
}


def _local_table_printed(industry: str, **ticks: bool) -> bool:
    plan = _plan(industry, "Paris, France")
    plan["channel_categories"] = {**_OTHERS_ON, **ticks}
    cells = _cells(_workbook(plan)["Channels & Strategy"])
    return any(c.upper() == "LOCAL JOB BOARDS" for c in cells) or any(
        c.upper().startswith("NICHE CHANNELS: ") for c in cells
    )


@pytest.mark.parametrize("industry", _NO_NICHE_LIST + ["healthcare_medical"])
def test_unticking_niche_alone_no_longer_hides_the_local_boards(industry):
    assert _local_table_printed(
        industry, niche_boards=False, global_boards=True, regional_boards=True
    )


@pytest.mark.parametrize("industry", ["general_entry_level", "healthcare_medical"])
def test_a_plan_that_funds_no_job_board_category_prints_no_local_boards(industry):
    off = dict(niche_boards=False, global_boards=False, regional_boards=False)
    assert not _local_table_printed(industry, **off)
    # the Niche tick is ignored: global and regional off hides it with niche ON
    assert not _local_table_printed(
        industry, niche_boards=True, global_boards=False, regional_boards=False
    )


def test_one_of_global_or_regional_is_enough():
    assert _local_table_printed(
        "general_entry_level",
        niche_boards=False,
        global_boards=False,
        regional_boards=True,
    )
    assert _local_table_printed(
        "general_entry_level",
        niche_boards=False,
        global_boards=True,
        regional_boards=False,
    )


# ---------------------------------------------------------------------------
# the help page says the new states in plain words
# ---------------------------------------------------------------------------


def test_help_page_explains_the_statuses_and_the_selection_rule():
    html = (PROJECT_ROOT / "templates" / "help-media-plans.html").read_text()
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)
    for phrase in (
        "VONQ Only",
        "has no row for it in that market",
        "Not available through Joveo",
        "is never shown in a plan",
        "chosen by market share, not matched to the role",
        "As of".lower(),
    ):
        assert phrase.lower() in text.lower(), phrase
