"""A country with no COUNTRY_DATA row is itself -- never the United States.

Pre-fix finding (2026-10-17): ``research._detect_country`` knew only the 41
COUNTRY_DATA countries, so Turkey, Peru, Romania, Nigeria, Saudi Arabia,
Taiwan, Bangladesh (and Egypt, Pakistan, ...) came back ``None`` and every
caller read that as "a US location": Istanbul / Lima / Taipei plans printed
"United States", US$60,000 and ~3.5% as the market's economics, took the US
competitor roster and (Taiwan) a USD plan currency.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import plan_currency
import plan_geo
import research

_NO_ROW = {
    "Istanbul, Turkey": ("Turkey", "TRY"),
    "Lima, Peru": ("Peru", "PEN"),
    "Bucharest, Romania": ("Romania", "RON"),
    "Lagos, Nigeria": ("Nigeria", "NGN"),
    "Riyadh, Saudi Arabia": ("Saudi Arabia", "SAR"),
    "Taipei, Taiwan": ("Taiwan", "TWD"),
    "Dhaka, Bangladesh": ("Bangladesh", "BDT"),
}
_BARE = {
    "Istanbul": "Turkey",
    "Riyadh": "Saudi Arabia",
    "Lagos": "Nigeria",
    "Bucharest": "Romania",
    "Taipei": "Taiwan",
    "Dhaka": "Bangladesh",
}
_DATA = Path(__file__).resolve().parent.parent / "data"


@pytest.mark.parametrize("loc,expected", [(k, v[0]) for k, v in _NO_ROW.items()])
def test_city_country_strings_are_recognised(loc, expected):
    assert research._detect_country(loc) == expected
    assert research.get_location_info(loc)["country"] == expected


@pytest.mark.parametrize("loc,expected", _BARE.items())
def test_bare_foreign_cities_are_recognised(loc, expected):
    assert research._detect_country(loc) == expected
    assert plan_geo.location_is_us(loc) is False


@pytest.mark.parametrize("loc,expected", [(k, v) for k, v in _NO_ROW.items()])
def test_no_row_country_shows_no_country_data_not_us_figures(loc, expected):
    info = research.get_location_info(loc)
    assert info["is_international"] is True
    assert info["no_country_data"] is True
    assert info["median_salary"] is None and info["coli"] is None
    assert info["unemployment"] == research.NO_COUNTRY_DATA
    assert info["population"] == research.NO_COUNTRY_DATA
    assert info["currency"] == expected[1]
    row = research.get_labour_market_intelligence("logistics_supply_chain", [loc])[
        "location_contexts"
    ][0]
    assert row["country"] == expected[0]
    assert row["median_salary"] == research.NO_COUNTRY_DATA
    assert row["unemployment_rate"] == research.NO_COUNTRY_DATA
    assert "United States" not in json.dumps(row)
    assert "$" not in row["median_salary"]


def test_dataset_facts_are_used_where_a_dataset_covers_the_country():
    info = research.get_location_info("Istanbul, Turkey")
    assert "Kariyer.net" in info["top_boards"]  # international_benchmarks_2026.json
    assert info["intl_platforms"] and info["region"] == "EMEA"


@pytest.mark.parametrize(
    "loc",
    [
        "Lima, OH",
        "Lima, Ohio",
        "Peru, IN",
        "Peru, Indiana",
        "Turkey, TX",
        "Dallas, TX",
        "Jerome, ID",
        "Athens, GA",
        "Moscow, ID",
        "Indianapolis, IN",
    ],
)
def test_us_places_that_share_a_name_stay_us(loc):
    assert research._detect_country(loc) in (None, "United States")
    info = research.get_location_info(loc)
    assert info["country"] == "United States" and not info.get("no_country_data")
    assert plan_geo.location_is_us(loc) is True


def test_bare_jerome_is_not_italy():
    # the legacy city list matched "rome" inside "Jerome"
    assert research._detect_country("Jerome") is None


def test_cost_of_living_style_consumers_do_not_invent_figures():
    sd = research.get_supply_demand_ratio("Warehouse Operative", ["Istanbul, Turkey"])
    entry = next(iter(sd["metros"].values()))
    assert entry["no_country_data"] is True and "score" not in entry
    geo = research.optimize_geographic_strategy(
        "Warehouse Operative", ["Istanbul, Turkey", "Dallas, TX"], 100000
    )
    assert geo["unranked_locations"][0]["location"].startswith("Istanbul")
    assert all("Istanbul" not in r["location"] for r in geo["ranked_locations"])
    factors = research.get_market_trends(
        ["Istanbul, Turkey"], "general_entry_level", []
    )
    assert all(
        "No country-level data" in f["descriptions"]["Istanbul, Turkey"]
        for f in factors
    )


def test_currency_follows_the_country_even_for_a_bare_city():
    for loc, (_, code) in _NO_ROW.items():
        assert plan_currency.currency_for_country(loc) == code
    assert plan_currency.currency_for_country("Istanbul") == "TRY"
    assert plan_currency.currency_for_country("Lima, OH") == "USD"


def test_registry_covers_every_country_the_datasets_name():
    """Drift guard: a country any clean dataset names is a COUNTRY_DATA row or
    a registry entry (so it can never silently fall through to the US)."""
    import country_registry

    known = {c.lower() for c in research.COUNTRY_DATA}
    known |= {c.lower() for c in country_registry.extra_countries()}
    known |= {"united arab emirates", "uk", "us", "viet nam"}  # spelling aliases
    names: set = set()
    bench = json.loads((_DATA / "international_benchmarks_2026.json").read_text())
    names |= {v["name"] for v in bench["countries"].values()}
    supply = json.loads((_DATA / "global_supply.json").read_text())
    names |= set(supply["country_job_boards"])
    import standardizer

    names |= {v["name"] for v in standardizer.COUNTRY_MAP.values()}
    import publisher_filter

    names |= {w.title() for w in publisher_filter._COUNTRY_WORDS if len(w) > 3}
    missing = sorted(n for n in names if n.lower() not in known)
    assert not missing, missing


def test_every_dataset_country_resolves_to_its_dataset_currency():
    bench = json.loads((_DATA / "international_benchmarks_2026.json").read_text())
    for v in bench["countries"].values():
        assert plan_currency.currency_for_country(v["name"]) == v["currency"], v["name"]


# --------------------------------------------------------------------------
# End to end: real /api/generate bundles
# --------------------------------------------------------------------------

from tests.live_server import live_port, post_json  # noqa: E402,F401
from tests.test_intl_boards_end_to_end import _BASE, _everything, _read  # noqa: E402

_PLANS = {
    "dubai": ("Dubai, UAE", "AE"),
    "riyadh": ("Riyadh, Saudi Arabia", "SA"),
    "istanbul": ("Istanbul, Turkey", "TR"),
    "lagos": ("Lagos, Nigeria", "NG"),
    "lima": ("Lima, Peru", "PE"),
    "bucharest": ("Bucharest, Romania", "RO"),
    "taipei": ("Taipei, Taiwan", "TW"),
    "dhaka": ("Dhaka, Bangladesh", "BD"),
}


@pytest.fixture(scope="module")
def bundles(live_port):  # noqa: F811
    import io
    import zipfile

    import bundle_qa

    captured: dict = {}
    real_gate = bundle_qa.gate_bundle

    def spy(pptx, xlsx, data, *a, **k):
        out = real_gate(pptx, xlsx, data, *a, **k)
        captured[str((data.get("locations") or [""])[0])] = out
        return out

    bundle_qa.gate_bundle = spy
    try:
        out = {}
        for n, (tag, (loc, cc)) in enumerate(_PLANS.items(), 1):
            payload = dict(
                _BASE,
                client_name=f"Unknown Country Fixture {tag} {n}",
                locations=[loc],
                custom_countries=[cc],
                competitors=["DHL"],
            )
            status, raw = post_json(live_port, "/api/generate", payload, timeout=300.0)
            assert status == 200 and isinstance(raw, bytes), (tag, status)
            out[tag] = (_read(raw), captured.get(loc))
        return out
    finally:
        bundle_qa.gate_bundle = real_gate


@pytest.mark.parametrize("tag", list(_PLANS))
def test_plan_has_no_us_country_label_or_us_figures(bundles, tag):
    rows = _everything(bundles[tag][0])
    assert not [r for r in rows if r[1].strip() == "United States"], tag
    assert not [r for r in rows if re.search(r"\$\s?60,000|~3\.5%\*", r[1])], tag
    assert not [
        r
        for r in rows
        if re.search(
            r"\b(FedEx|UPS|XPO Logistics|C\.H\. Robinson|Amazon Logistics)\b", r[1]
        )
    ], tag
    assert not [r for r in rows if "national placeholder" in r[1]], tag


@pytest.mark.parametrize("tag", [t for t in _PLANS if t != "dubai"])
def test_location_table_says_no_country_data(bundles, tag):
    xlsx, _ = bundles[tag][0]
    texts = [t for w, t in xlsx if w.startswith("Market Intelligence!")]
    assert "No country data" in texts
    expected = _PLANS[tag][0].split(", ")[1]
    assert expected in texts


@pytest.mark.parametrize("tag", list(_PLANS))
def test_bundle_qa_raises_no_us_data_on_non_us_plan(bundles, tag):
    gate = bundles[tag][1]
    assert gate is not None, "bundle_qa gate did not run"
    codes = {f.get("code") for f in gate.get("findings") or []}
    assert "us_data_on_non_us_plan" not in codes, codes


# --------------------------------------------------------------------------
# Review round: bare names shared with US places, "Place State" forms, alias
# prefix collisions, METRO guard
# --------------------------------------------------------------------------

_SHARED_BARE = [
    "Lima",
    "Cairo",
    "Odessa",
    "Medina",
    "Mecca",
    "St Petersburg",
    "Saint Petersburg",
    "Casablanca",
    "Suez",
    "Tangier",
    "Cartago",
    "Athens",
    "Moscow",
    "Alexandria",
]


@pytest.mark.parametrize("loc", _SHARED_BARE)
def test_bare_name_shared_with_a_us_place_is_not_read_abroad(loc):
    assert research._detect_country(loc) is None
    assert country_registry_detect(loc) is None
    assert research.get_location_info(loc)["country"] == "United States"
    assert plan_geo.location_is_us(loc) is not False


def country_registry_detect(loc):
    import country_registry

    return country_registry.detect_extra(loc)


@pytest.mark.parametrize(
    "loc,expected",
    [
        ("Odessa, Ukraine", "Ukraine"),
        ("Medina, Saudi Arabia", "Saudi Arabia"),
        ("St Petersburg, Russia", "Russia"),
        ("Lima, Peru", "Peru"),
        ("Cairo, Egypt", "Egypt"),
        ("Casablanca, Morocco", "Morocco"),
    ],
)
def test_city_country_form_still_resolves_abroad(loc, expected):
    assert research._detect_country(loc) == expected


def test_every_bare_registry_city_is_absent_from_the_us_gazetteer():
    """Audit guard: a city name the US place gazetteer also holds must be in
    the denylist, so a US-region plan never reads it abroad."""
    import csv

    import country_registry

    def norm(text):
        text = country_registry._fold(text).replace("saint ", "st ").replace(".", "")
        return re.sub(r"[^a-z0-9]", "", text)

    us = set()
    with open(_DATA / "geo" / "us_places.tsv", encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            us.add(norm(row["display_name"]))
    metro = {norm(k.replace("_", " ")) for k in research.METRO_DATA}
    clash = sorted(c for c in country_registry._CITIES if norm(c) in us | metro)
    assert not clash, clash


@pytest.mark.parametrize(
    "loc",
    [
        "Peru Indiana",
        "Peru Illinois",
        "Greece New York",
        "Egypt Texas",
        "Turkey Texas",
        "Peru IN 46970",
        "Lima OH",
        "Egypt North Carolina",
        "Peru West Virginia",
    ],
)
def test_place_state_without_a_comma_is_a_us_place(loc):
    assert research._detect_country(loc) is None
    assert plan_geo.location_is_us(loc) is True


@pytest.mark.parametrize(
    "loc,expected",
    [
        ("Kyiv, Ukraine", "Ukraine"),
        ("Kharkiv, Ukraine", "Ukraine"),
        ("Belgrade, Serbia", None),
        ("Suva, Fiji", None),
        ("Yangon, Myanmar", None),
        ("Tallinn, Estonia", None),
        ("London, UK", "United Kingdom"),
        ("Mumbai, IN", None),
        ("Paris, Serbia", "France"),
    ],
)
def test_alias_must_be_the_whole_trailing_token(loc, expected):
    # ", uk" matched inside ", ukraine"; ", se" inside ", serbia"; ", fi" in
    # ", fiji"; ", my" in ", myanmar"; ", es" in ", estonia"
    assert research._detect_country(loc) == expected


def test_ukraine_is_not_the_united_kingdom():
    info = research.get_location_info("Kyiv, Ukraine")
    assert info["country"] == "Ukraine" and info["currency"] == "UAH"


@pytest.mark.parametrize(
    "loc,expected",
    [
        ("San Jose, Costa Rica", "Costa Rica"),
        ("San José, Costa Rica", "Costa Rica"),
        ("Valencia, Peru", "Peru"),
        ("Portland, Turkey", "Turkey"),
    ],
)
def test_registry_country_beats_the_us_metro_guard(loc, expected):
    assert research._detect_country(loc) == expected
    assert research.get_location_info(loc)["country"] == expected


def test_us_san_jose_stays_us():
    assert research._detect_country("San Jose, CA") is None
    assert research.get_location_info("San Jose, CA")["country"] == "United States"


@pytest.fixture(scope="module")
def us_bundles(live_port):  # noqa: F811
    import bundle_qa

    captured: dict = {}
    real_gate = bundle_qa.gate_bundle

    def spy(pptx, xlsx, data, *a, **k):
        out = real_gate(pptx, xlsx, data, *a, **k)
        captured[str((data.get("locations") or [""])[0])] = out
        return out

    bundle_qa.gate_bundle = spy
    try:
        out = {}
        for n, loc in enumerate(["Odessa", "Peru Indiana", "St Petersburg"], 1):
            payload = dict(
                _BASE,
                client_name=f"Us Shared Name Fixture {n}",
                locations=[loc],
                target_region="us_only",
                custom_countries=[],
                competitors=["DHL"],
            )
            status, raw = post_json(live_port, "/api/generate", payload, timeout=300.0)
            assert status == 200 and isinstance(raw, bytes), (loc, status)
            out[loc] = (_read(raw), captured.get(loc) or next(iter(captured.values())))
        return out
    finally:
        bundle_qa.gate_bundle = real_gate


@pytest.mark.parametrize("loc", ["Odessa", "Peru Indiana", "St Petersburg"])
def test_us_region_plan_with_a_shared_place_name_has_no_qa_criticals(us_bundles, loc):
    (xlsx, deck), gate = us_bundles[loc]
    summary = gate["summary"]
    criticals = [
        f for f in gate.get("findings") or [] if f.get("severity") == "critical"
    ]
    assert not criticals, [(f.get("code"), f.get("message")) for f in criticals]
    texts = [t for _, t in xlsx + deck]
    assert not [t for t in texts if t.strip() == "No country data"], summary
    assert not [t for t in texts if re.search(r"Ukraine|Russia|Saudi", t)]
