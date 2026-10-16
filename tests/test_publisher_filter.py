"""publisher_filter: ONE matcher for client exclusions and availability.

Covers the matcher (spelling vs identity), the sourced alias/rename tables,
the availability entries (permanent, silent), market scope and the
relationship label.  The end-to-end "a France plan excluding two boards has
zero mentions" proof lives in tests/test_intl_boards_end_to_end.py.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import publisher_filter as pf  # noqa: E402


@pytest.mark.parametrize(
    "a,b",
    [
        ("indeed.com", "Indeed"),
        ("www.Indeed.com", "Indeed France"),
        ("Indeed Sponsored Jobs", "Indeed"),
        ("Indeed, Inc", "Indeed"),
        ("Hello Work", "HelloWork"),
        ("HelloWork (formerly RegionsJob)", "RegionsJob"),
        ("OCC", "OCC Mundial"),
        ("occ.com.mx", "OCC Mundial"),
        ("SEEK", "Seek New Zealand"),
        ("Pôle Emploi", "pole emploi"),
        ("TruckingJobs.com", "truckingjobs"),
    ],
)
def test_same_board_different_spelling(a, b):
    assert pf.names_match(a, b), (a, b)


@pytest.mark.parametrize(
    "a,b",
    [
        ("Job", "Jobcase"),
        ("Indeed", "Indeed Flex Staffing"),
        ("Monster", "Monsterboard"),
        ("OCC", "OCC Confidential Hiring Group"),
        ("LinkedIn", "Meta"),
    ],
)
def test_different_boards_never_match(a, b):
    assert not pf.names_match(a, b), (a, b)


def test_sanitize_excluded_dedupes_caps_and_strips_markup():
    out = pf.sanitize_excluded(["HelloWork", "hellowork.com", "<b>Indeed</b>", "", 5, "x" * 200])
    assert out == ["HelloWork", "Indeed"]
    assert pf.sanitize_excluded("Indeed, Seek ; Naukri") == ["Indeed", "Seek", "Naukri"]
    assert len(pf.sanitize_excluded([f"Board {i}" for i in range(60)])) == pf.MAX_EXCLUDED
    assert pf.sanitize_excluded(None) == []
    assert pf.sanitize_excluded({"a": 1}) == []


def test_client_exclusion_drops_every_spelling():
    data = {"excluded_publishers": ["indeed.com"]}
    names = ["Pole Emploi (France Travail)", "Indeed France", "Indeed Sponsored Jobs", "LinkedIn France"]
    assert pf.clean_names(names, data, "France") == [
        "Pole Emploi (France Travail)",
        "LinkedIn France",
    ]
    assert pf.is_user_excluded("Indeed Mexico", data)
    assert not pf.is_user_excluded("Indeed Mexico", {})


def test_compound_names_filter_part_wise():
    data = {"excluded_publishers": ["LinkedIn"]}
    assert pf.clean_names(["LinkedIn / Meta / TikTok Ads"], data) == ["Meta / TikTok Ads"]
    # a compound is excluded only when every part is
    assert not pf.is_excluded("LinkedIn / Meta", data)
    assert pf.is_excluded("LinkedIn / LinkedIn Ads", data)


def test_disclosure_only_lists_client_exclusions():
    assert pf.disclosure_line({"excluded_publishers": ["HelloWork", "indeed.com"]}) == (
        "Excluded at client request: HelloWork, indeed.com"
    )
    assert pf.disclosure_line({}) == ""
    # permanent availability exclusions are never disclosed
    assert pf.disclosure_line({"locations": ["Paris, France"]}) == ""


def test_next_allowed_substitutes_the_next_board():
    data = {"excluded_publishers": ["Indeed"]}
    assert pf.next_allowed(["Indeed Sponsored Jobs", "Glassdoor", "LinkedIn"], data) == "Glassdoor"
    assert pf.next_allowed(["Indeed"], data) is None


# ---- availability (permanent, silent) -------------------------------------


def test_hellowork_and_occ_are_unavailable_in_their_markets():
    assert pf.is_excluded("HelloWork (formerly RegionsJob)", None, "France")
    assert pf.is_excluded("RegionsJob", None, "France")
    assert pf.is_excluded("OCC Mundial", None, "Mexico")
    assert pf.is_excluded("OCC", None, "Mexico")
    entry = pf.availability_for("OCC Mundial", "Mexico")
    assert entry["status"] == "unavailable"
    assert entry["reason"] == "not available through Joveo — do not recommend"
    assert entry["source"] and entry["as_of"] == "2026-10-16"


def test_availability_is_country_scoped_when_the_country_is_known():
    assert not pf.is_excluded("OCC Mundial", None, "Canada")
    assert not pf.is_excluded("Indeed Mexico", None, "Mexico")


def test_zipecruiter_uk_exit_is_scoped_to_the_uk():
    assert pf.is_excluded("ZipRecruiter Sponsored", None, "United Kingdom")
    assert not pf.is_excluded("ZipRecruiter Sponsored", None, "United States")
    assert not pf.is_excluded("ZipRecruiter", None, None)  # no country, exit is UK-only


def test_market_scope_gates_us_only_platforms():
    assert pf.platform_runs_in("ZipRecruiter Sponsored", "United States")
    assert pf.platform_runs_in("ZipRecruiter Sponsored", "Canada")
    for country in ("France", "Germany", "India", "Vietnam", "Mexico", "Australia", "Singapore"):
        assert not pf.platform_runs_in("ZipRecruiter Sponsored", country), country
    assert not pf.platform_runs_in("ZipRecruiter Sponsored", "United Kingdom")
    assert pf.platform_runs_in("Indeed Sponsored Jobs", "France")
    assert pf.platform_runs_in("ZipRecruiter Sponsored", None)  # unknown -> fail open


def test_renames_and_folded_boards_are_sourced():
    assert pf.clean_names(["CareerBuilder Vietnam"], None, "Vietnam") == ["CareerViet"]
    assert pf.clean_names(["Workopolis / Monster Canada"], None, "Canada") == ["Monster Canada"]
    assert pf.clean_names(["Workopolis"], None, "Canada") == []
    raw = json.loads((ROOT / "data" / "publisher_availability.json").read_text())
    for section in ("entries", "alias_groups", "market_scope", "renames", "folded_boards"):
        for item in raw[section]:
            assert str(item.get("source") or "").strip(), (section, item)
    assert any("exited Vietnam" in t for t in raw["_todo"])


def test_relationship_label_never_invented():
    # Listed in the supply repository but not In Joveo Network -> unverified
    assert pf.relationship_unverified("Glassdoor", "India")
    # In network -> clean; absent from the repository -> unknown, no label
    assert not pf.relationship_unverified("VietnamWorks", "Vietnam")
    assert not pf.relationship_unverified("TotallyMadeUpBoard", "France")
    marked, any_marked = pf.annotate(["Glassdoor", "VietnamWorks"], "India")
    assert marked == [f"Glassdoor {pf.RELATIONSHIP_MARK}", "VietnamWorks"] and any_marked
    assert "confirm before quoting" in pf.RELATIONSHIP_NOTE
