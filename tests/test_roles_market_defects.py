"""Role-category mapping, supervisor tier, role-vs-market difficulty and the
non-US large-employer list (reviewer defects on a German automotive plan).

Each family test uses the exact titles the reviewer's plan carried. They fail
on the code before the fix: couriers and gig drivers resolved to the CDL
Heavy-and-Tractor-Trailer SOC, a factory painter to Construction Laborers,
plant supervisors to nothing (default tier), and every role in a "Tight 7/10"
German market printed at a flat 5/10.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import api_enrichment as ae
import collar_intelligence as ci
import gold_standard as gs
import standardizer as st

# --------------------------------------------------------------------------
# 1. Role -> occupation mapping (exact SOC 2018 codes)
# --------------------------------------------------------------------------

_FAMILIES = [
    # (title, canonical key, SOC)
    ("Courier", "courier", "43-5021"),
    ("Bicycle Courier", "courier", "43-5021"),
    ("Messenger", "courier", "43-5021"),
    ("Gig Delivery Driver", "delivery_driver", "53-3033"),
    ("Delivery Driver (no CDL)", "delivery_driver", "53-3033"),
    ("Last-Mile Delivery Driver", "delivery_driver", "53-3033"),
    ("Package Delivery Driver", "delivery_driver", "53-3033"),
    ("Delivery Associate", "delivery_driver", "53-3033"),
    ("Industrial Painter", "industrial_painter", "51-9124"),
    ("Paint Shop Technician", "industrial_painter", "51-9124"),
    ("Industrial Painter / Paint Shop Technician", "industrial_painter", "51-9124"),
    ("Spray Painter", "industrial_painter", "51-9124"),
    ("House Painter", "construction_painter", "47-2141"),
    ("Commercial Painter", "construction_painter", "47-2141"),
    ("Warehouse Associate", "warehouse_worker", "53-7062"),
    ("Warehouse Picker", "warehouse_worker", "53-7062"),
    ("Picker", "warehouse_worker", "53-7062"),
    ("Packer", "warehouse_worker", "53-7062"),
    ("Fulfillment Associate", "warehouse_worker", "53-7062"),
    ("Order Filler", "order_filler", "53-7065"),
    ("Stocker", "order_filler", "53-7065"),
    ("Battery Production Supervisor", "production_supervisor", "51-1011"),
    ("Drive Unit Supervisor", "production_supervisor", "51-1011"),
    ("Production Supervisor", "production_supervisor", "51-1011"),
    ("Manufacturing Team Lead", "production_supervisor", "51-1011"),
    ("Foreman, Assembly Plant", "production_supervisor", "51-1011"),
    ("Maintenance Supervisor", "maintenance_supervisor", "49-1011"),
    ("Supervisor Maintenance - Drive Unit", "maintenance_supervisor", "49-1011"),
    ("Maintenance Technician", "maintenance_technician", "49-9071"),
    ("Maintenance Technician - Paint Shop", "maintenance_technician", "49-9071"),
    ("Industrial Mechanic", "maintenance_technician", "49-9071"),
    # A CDL / tractor-trailer title keeps the heavy-truck SOC.
    ("CDL-A Driver", "truck_driver", "53-3032"),
    ("Delivery Driver CDL-A", "truck_driver", "53-3032"),
    ("Delivery Driver (Class A)", "truck_driver", "53-3032"),
]


@pytest.mark.parametrize("title,canon,soc", _FAMILIES)
def test_title_family_maps_to_exact_soc(title, canon, soc):
    assert st.normalize_role(title) == canon
    assert st.get_soc_code(title) == soc


@pytest.mark.parametrize(
    "title",
    [
        "Courier",
        "Gig Delivery Driver",
        "Delivery Driver (no CDL)",
        "Industrial Painter",
        "Paint Shop Technician",
        "Battery Production Supervisor",
        "Drive Unit Supervisor",
        "Warehouse Associate",
    ],
)
def test_reviewer_titles_never_land_on_the_wrong_neighbour(title):
    soc = st.get_soc_code(title)
    # CDL heavy truck, route sales driver, construction laborers, any military.
    assert soc not in {"53-3032", "53-3031", "47-2061"}
    assert not soc.startswith("55-")
    assert soc  # and never unmapped


def test_supervisor_rule_does_not_swallow_other_industries():
    # No industry word: stays unmatched rather than guessing Production.
    for title in ("Shift Supervisor", "Team Lead"):
        assert st.get_soc_code(title) == ""
    # Another occupation family keeps its own handling.
    for title in (
        "Retail Shift Supervisor",
        "Kitchen Supervisor",
        "Nursing Supervisor",
    ):
        assert st.get_soc_code(title) not in {"51-1011", "49-1011"}


def test_collar_of_a_courier_stays_blue():
    # 43-5021 is in the office-and-admin SOC group (pink); a courier was blue
    # collar while it resolved to a driver SOC and must stay so.
    assert ci.classify_collar("Courier")["collar_type"] == "blue_collar"
    assert ci.classify_collar("Industrial Painter")["collar_type"] == "blue_collar"


@pytest.mark.parametrize(
    "title,soc",
    [
        ("Courier", "43-5021"),
        ("Gig Delivery Driver", "53-3033"),
        ("Delivery Driver", "53-3033"),
        ("Delivery Driver (no CDL)", "53-3033"),
        ("Industrial Painter", "51-9124"),
        ("Paint Shop Technician", "51-9124"),
        ("Battery Production Supervisor", "51-1011"),
        ("Maintenance Supervisor", "49-1011"),
    ],
)
def test_wage_matcher_family_codes(title, soc):
    assert ae._soc_for_salary_role(title) == (soc, "")


def test_wage_matcher_stays_fail_closed_for_cdl_delivery():
    # A CDL delivery title is not a light-truck wage: no wage, not a wrong one.
    assert ae._soc_for_salary_role("Delivery Driver CDL-A")[0] == ""


# --------------------------------------------------------------------------
# 2. Supervisor seniority tier
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "title",
    [
        "Battery Production Supervisor",
        "Drive Unit Supervisor",
        "Shift Supervisor",
        "Production Team Lead",
        "Shift Lead",
        "Foreman",
        "Schichtleiter Produktion",
        "Industriemeister",
        "Vorarbeiter Montage",
    ],
)
def test_supervisor_words_are_a_seniority_level(title):
    assert gs._detect_seniority_level(title.lower()) == "supervisor"


def test_lead_alone_and_hausmeister_are_not_supervisors():
    assert gs._detect_seniority_level("lead software engineer") == "senior"
    assert gs._detect_seniority_level("hausmeister") != "supervisor"


def test_supervisors_never_fall_to_the_default_tier_or_score_below_their_people():
    plan = {
        "locations": ["Austin, TX"],
        "industry": "manufacturing",
        "target_roles": [
            "Maintenance Technician",
            "Battery Production Supervisor",
            "Drive Unit Supervisor",
        ],
    }
    rows = {r["role_title"]: r for r in gs.classify_difficulty(plan)}
    ic = rows["Maintenance Technician"]
    for title in ("Battery Production Supervisor", "Drive Unit Supervisor"):
        sup = rows[title]
        assert sup["classification_source"] == "seniority_keyword_match"
        assert sup["seniority_level"] == "supervisor"
        assert "Unclassified" not in sup["description"]
        assert sup["complexity_score"] > ic["complexity_score"]
        assert sup["budget_weight"] >= ic["budget_weight"]


def test_supervisor_tier_is_not_the_unclassified_default_and_not_below_frontline():
    tier, source = gs._lookup_role_tier("Battery Production Supervisor")
    assert source != "unclassified_default"
    assert tier == "professional"
    # A supervisor of a frontline profile is priced one band up.
    frontline_tier, _ = gs._lookup_role_tier("Production Worker")
    sup_tier, _ = gs._lookup_role_tier("Production Worker Supervisor")
    order = ["frontline", "skilled_trade", "professional"]
    assert order.index(sup_tier) > order.index(frontline_tier)


# --------------------------------------------------------------------------
# 3. Role difficulty is anchored on the market difficulty the plan prints
# --------------------------------------------------------------------------

_DE_ROLES = ["Maintenance Technician", "Battery Production Supervisor"]


def _plan(loc):
    return {
        "locations": [loc],
        "industry": "automotive",
        "target_roles": list(_DE_ROLES),
    }


def _market(loc):
    rows = gs.enrich_city_level_data(_plan(loc))
    return next(iter(rows.values()))


def test_bare_country_and_its_cities_print_one_market_band():
    # "Germany" read 7.0 / COL 80 while "Berlin, Germany" and "Grünheide,
    # Germany" read the flat generic 5.5 / COL 100: only the city part was
    # looked up and the country in the string was ignored.
    bare = _market("Germany")
    for loc in ("Berlin, Germany", "Grünheide, Germany", "Munich, Germany"):
        row = _market(loc)
        assert row["hiring_difficulty"] == bare["hiring_difficulty"] == 7.0
        assert row["supply_tier"] == bare["supply_tier"] == "tight"
        assert row["cost_of_living_index"] == bare["cost_of_living_index"]
        assert row["geo_basis"] == "country"


def test_two_letter_suffix_is_not_read_as_a_country():
    # "DE" is Delaware, not Germany.
    row = _market("Wilmington, DE")
    assert row["geo_basis"] != "country"


def test_tight_market_does_not_print_every_role_at_a_flat_default():
    market = _market("Germany")["hiring_difficulty"]
    for loc in ("Germany", "Berlin, Germany", "Grünheide, Germany"):
        rows = gs.classify_difficulty(_plan(loc))
        scores = {r["role_title"]: r["complexity_score"] for r in rows}
        assert scores["Maintenance Technician"] > 5.0, loc
        for r in rows:
            assert r["location_modifier"] == pytest.approx(
                min(gs._MARKET_MODIFIER_MAX, market - 5.0)
            )
            assert f"market {market:.1f}/10" in r["location_matched"]
    ic = scores["Maintenance Technician"]
    assert scores["Battery Production Supervisor"] > ic


def test_role_score_is_market_anchor_plus_stated_role_adjustment():
    rows = {r["role_title"]: r for r in gs.classify_difficulty(_plan("Germany"))}
    r = rows["Maintenance Technician"]
    # role base 5.0 (mid profile) + market adjustment +2.0 (7.0 - 5.0), stated.
    assert r["complexity_score"] == 7.0
    assert "market adjustment" in r["description"]
    assert "+2.0" in r["description"]


def test_easier_market_lowers_role_scores_and_modifier_is_bounded():
    low = gs.classify_difficulty(_plan("Mumbai, India"))[0]
    assert low["location_modifier"] == gs._MARKET_MODIFIER_MIN
    assert low["complexity_score"] < 5.0


def test_us_city_table_modifiers_are_unchanged():
    row = gs.classify_difficulty(_plan("Austin, TX"))[0]
    assert row["location_modifier"] == 1.0
    assert row["location_matched"] == "austin"
    assert "market adjustment" not in row["description"]


# --------------------------------------------------------------------------
# 4. Large employers for non-US plans
# --------------------------------------------------------------------------

_DATA = (
    Path(__file__).resolve().parent.parent / "data" / "major_employers_by_country.json"
)
# Spelled in pieces so this guard does not itself put a client name in git.
_PUBLIC_REPO_CLIENT_NAMES = ("tes" "la", "ama" "zon", "fl" "ex")


def _employer_rows():
    raw = json.loads(_DATA.read_text(encoding="utf-8"))
    for country, rows in raw["countries"].items():
        for row in rows:
            yield country, row


def test_every_employer_row_is_sourced_and_dated():
    seen = set()
    n = 0
    for country, row in _employer_rows():
        n += 1
        assert row["name"].strip(), country
        assert row["industry"] in gs.LARGE_EMPLOYER_INDUSTRY_LABELS, row
        assert re.match(r"^https?://\S+$", row["source_url"]), row
        assert str(row["as_of"]).strip(), row
        key = (country, row["industry"], row["name"].lower())
        assert key not in seen, key
        seen.add(key)
    assert n > 0


def test_priority_countries_are_covered():
    by_country = {}
    for country, row in _employer_rows():
        by_country.setdefault(country, set()).add(row["industry"])
    for country in ("Germany", "United Kingdom", "France", "Poland", "India"):
        assert len(by_country.get(country, ())) >= 5, country


def test_german_automotive_covers_the_obvious_large_employers():
    names = {
        row["name"].lower()
        for country, row in _employer_rows()
        if country == "Germany" and row["industry"] == "automotive_manufacturing"
    }
    for expected in (
        "volkswagen",
        "bosch",
        "mercedes",
        "bmw",
        "siemens",
        "zf",
        "continental",
    ):
        assert any(expected in n for n in names), expected


def test_employer_file_names_no_client():
    text = _DATA.read_text(encoding="utf-8").lower()
    for name in _PUBLIC_REPO_CLIENT_NAMES:
        assert not re.search(rf"\b{name}\b", text), name


def test_german_plan_shows_labelled_large_employers_not_competitors():
    data = _plan("Berlin, Germany")
    data["industry"] = "Automotive & Manufacturing"
    cities = gs.enrich_city_level_data(dict(data))
    cmap = gs.build_competitor_map(data, cities)
    entry = cmap["Berlin"]
    assert entry["large_employers_label"] == (
        "Largest employers in Germany (Automotive & Manufacturing)"
    )
    assert "not measured competition" in entry["large_employers_note"]
    assert len(entry["large_employers"]) >= 5
    for row in entry["large_employers"]:
        assert row["source_url"].startswith("http") and row["as_of"]
    # Never passed off as the brief's competitors.
    assert entry["top_employers"] == []


def test_brief_competitors_still_come_first_and_stay_separate():
    data = _plan("Germany")
    data["industry"] = "automotive"
    data["competitors"] = ["Acme Motors"]
    cities = gs.enrich_city_level_data(dict(data))
    entry = gs.build_competitor_map(data, cities)["Germany"]
    assert entry["top_employers"] == ["Acme Motors"]
    assert entry["large_employers"]


def test_us_plan_gets_no_large_employer_list():
    data = _plan("Austin, TX")
    cities = gs.enrich_city_level_data(dict(data))
    entry = gs.build_competitor_map(data, cities)["Austin"]
    assert "large_employers" not in entry


def test_unknown_industry_or_country_gives_nothing_not_a_guess():
    assert gs.large_employers_for("Germany", "staffing") == {}
    assert gs.large_employers_for("Atlantis", "automotive") == {}
    assert gs.large_employers_for("", "automotive") == {}


def test_rows_missing_a_source_are_dropped_at_load(tmp_path, monkeypatch):
    bad = {
        "countries": {
            "Germany": [
                {"name": "A", "industry": "retail", "source_url": "", "as_of": "2025"},
                {
                    "name": "B",
                    "industry": "retail",
                    "source_url": "https://x.example",
                    "as_of": "",
                },
                {
                    "name": "C",
                    "industry": "bogus",
                    "source_url": "https://x.example",
                    "as_of": "2025",
                },
                {
                    "name": "D",
                    "industry": "retail",
                    "source_url": "https://x.example/d",
                    "as_of": "2025",
                },
            ]
        }
    }
    f = tmp_path / "e.json"
    f.write_text(json.dumps(bad), encoding="utf-8")
    monkeypatch.setattr(gs, "_MAJOR_EMPLOYERS_PATH", f)
    monkeypatch.setattr(gs, "_MAJOR_EMPLOYERS_CACHE", None)
    got = gs.large_employers_for("Germany", "retail")
    assert [r["name"] for r in got["employers"]] == ["D"]
    monkeypatch.setattr(gs, "_MAJOR_EMPLOYERS_CACHE", None)
