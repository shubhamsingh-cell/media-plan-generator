"""Follow-up defects listed "not done" after the role-source allowlist (722a014).

D1  SOC_CODES held six codes with no BLS OEWS series, so each lookup was an
    empty answer that was never cached.
D2  ONET_SOC_CODES was matched by substring plus a one-shared-word fallback
    ("Nursery Associate" took Registered Nurses). The O*NET curated fallback
    and the H-1B wage path need no O*NET credential, so an invalid key does not
    make that match unreachable.
D3  The standardizer gave Sales Representative / Account Executive / BDR /
    Sales Associate the Sales Managers SOC (11-2022); collar_intelligence
    reads it, so a retail Sales Associate printed as "White Collar".
D4  plan_geo read a bare foreign city nothing else resolved as a US market.

SOC titles quoted below were read on https://www.bls.gov/soc/2018/major_groups.htm
and https://www.bls.gov/oes/2025/may/oes_stru.htm (2026-10-18). No test touches
the network: BLS transports are stubbed and the O*NET key is removed.
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from typing import Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import collar_intelligence  # noqa: E402
import country_registry  # noqa: E402
import plan_geo  # noqa: E402
import plan_location  # noqa: E402
import research  # noqa: E402
import standardizer  # noqa: E402

_DATA = PROJECT_ROOT / "data"


def _soc_calls(monkeypatch: pytest.MonkeyPatch, role: str) -> List[str]:
    calls: List[str] = []
    monkeypatch.setattr(
        api_enrichment,
        "_fetch_bls_salary",
        lambda r, soc: calls.append(soc) or {"mean": 1, "source": "BLS OES"},
    )
    api_enrichment.fetch_salary_data([role])
    return calls


# --------------------------------------------------------------------- D1 ---

# Codes that are in neither the OEWS 2025 structure (bls.gov/oes/2025/may/
# oes_stru.htm, 852 published codes) nor, for 51-2098 and 25-1099, the SOC 2018
# structure. 31-1121, 31-1122, 29-2011 and 53-1042 ARE SOC 2018 detailed codes
# ("Home Health Aides", "Personal Care Aides", "Medical and Clinical Laboratory
# Technologists", "First-Line Supervisors of Helpers, Laborers, and Material
# Movers, Hand") but OEWS publishes them only as 31-1120, 29-2010 and 53-1047.
_NO_BLS_SERIES = frozenset(
    {"31-1121", "31-1122", "29-2011", "53-1042", "51-2098", "25-1099"}
)


def test_d1_no_soc_code_points_at_a_code_with_no_bls_series() -> None:
    assert not (_NO_BLS_SERIES & set(api_enrichment.SOC_CODES.values()))


@pytest.mark.parametrize(
    "role,soc",
    [
        # 31-1120 Home Health and Personal Care Aides
        ("Home Health Aide", "31-1120"),
        ("Per Diem Home Health Aide", "31-1120"),
        ("Caregiver", "31-1120"),
        # 29-2010 Clinical Laboratory Technologists and Technicians
        ("Medical Technologist", "29-2010"),
        # 51-2090 Miscellaneous Assemblers and Fabricators
        ("Assembly Worker", "51-2090"),
        ("Night Shift Assembly Worker", "51-2090"),
    ],
)
def test_d1_corrected_titles_query_the_code_bls_publishes(
    monkeypatch: pytest.MonkeyPatch, role: str, soc: str
) -> None:
    assert _soc_calls(monkeypatch, role) == [soc]


@pytest.mark.parametrize(
    "role",
    [
        # 25-1099 does not exist; 25-1000 is a heading with no series and
        # 25-1199 is "Postsecondary Teachers, All Other".
        "Professor",
        "Associate Professor",
        # 53-1042 is a first-line SUPERVISOR code; "Warehouse Manager" is not
        # established as that occupation, so no BLS wage rather than a guess.
        "Warehouse Manager",
        "Night Shift Warehouse Manager",
    ],
)
def test_d1_titles_without_a_confident_published_soc_get_no_bls_wage(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    assert _soc_calls(monkeypatch, role) == []


# --------------------------------------------------------------------- D2 ---


@pytest.fixture()
def onet_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """No O*NET key (the curated-fallback path), and no cache to answer for us."""
    monkeypatch.delenv("ONET_API_KEY", raising=False)
    monkeypatch.delenv("ONET_PASSWORD", raising=False)
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)


def _onet_socs(role: str) -> List[str]:
    out = api_enrichment.fetch_onet_occupation_data([role])
    return [v["soc_code"] for v in (out.get("occupations") or {}).values()]


def _h1b_socs(role: str) -> List[str]:
    out = api_enrichment.fetch_h1b_wage_benchmarks([role])
    return [v["soc_code"] for k, v in out.items() if isinstance(v, dict)]


_OTHER_OCCUPATION = [
    "Nursery Associate",  # "nurse" inside "nursery"
    "Nurse Practitioner",  # not a Registered Nurse
    "Licensed Practical Nurse",
    "Security Guard",  # shared word "security" with Information Security
    "Server Administrator",  # shared word "administrator"
    "Information Security Officer",
    "Supply Chain Analyst",
    # a bare generic word is contained in many titles and used to pick one
    "Engineer",
    "Data",
    "Analyst",
    "Manager",
]


@pytest.mark.parametrize("role", _OTHER_OCCUPATION)
def test_d2_onet_curated_fallback_never_guesses_another_occupation(
    onet_offline: None, role: str
) -> None:
    assert _onet_socs(role) == []


@pytest.mark.parametrize("role", _OTHER_OCCUPATION)
def test_d2_h1b_wage_benchmarks_never_guess_another_occupation(
    onet_offline: None, role: str
) -> None:
    assert _h1b_socs(role) == []


@pytest.mark.parametrize(
    "role,soc",
    [
        ("Software Engineer", "15-1252.00"),
        ("Senior Software Engineer", "15-1252.00"),
        ("Remote Data Scientist", "15-2051.00"),
        ("ICU Registered Nurse", "29-1141.00"),
        ("Regional Sales Manager", "11-2022.00"),
        # exact aliases for titles the old match got right
        ("Frontend Engineer", "15-1252.00"),
        ("Full Stack Developer", "15-1252.00"),
        ("Information Security Analyst", "15-1212.00"),
        ("Human Resources Manager", "11-3121.00"),
    ],
)
def test_d2_whole_word_titles_still_match(
    onet_offline: None, role: str, soc: str
) -> None:
    assert _onet_socs(role) == [soc]


@pytest.mark.parametrize(
    "role,soc",
    [
        ("Software Engineer", "15-1252.00"),
        ("Senior Software Engineer", "15-1252.00"),
        ("Regional Sales Manager", "11-2022.00"),
    ],
)
def test_d2_h1b_whole_word_titles_still_match(
    onet_offline: None, role: str, soc: str
) -> None:
    assert _h1b_socs(role) == [soc]


def test_d2_the_match_does_not_need_an_o_net_credential_to_be_reachable() -> None:
    # The premise "O*NET is a 401 on prod, so this is dead" is false: the curated
    # fallback and the H-1B path never call O*NET. They are off only because
    # ENRICH_ROLE_SOURCES defaults to BLS + Adzuna, which this pins.
    assert api_enrichment.DEFAULT_ROLE_SOURCE_LABELS == ("BLS", "Adzuna")
    assert "O*NET" not in api_enrichment.DEFAULT_ROLE_SOURCE_LABELS
    assert "H1B-Wages" not in api_enrichment.DEFAULT_ROLE_SOURCE_LABELS


# --------------------------------------------------------------------- D3 ---


@pytest.mark.parametrize(
    "role,soc",
    [
        # 41-4012 Sales Representatives, Wholesale and Manufacturing, Except
        # Technical and Scientific Products
        ("Sales Representative", "41-4012"),
        ("Sales Rep", "41-4012"),
        ("Senior Sales Representative", "41-4012"),
        ("Inside Sales Representative", "41-4012"),
        # 41-3091 Sales Representatives of Services, Except Advertising,
        # Insurance, Financial Services, and Travel
        ("Account Executive", "41-3091"),
        ("Business Development Representative", "41-3091"),
        ("BDR", "41-3091"),
        ("SDR", "41-3091"),
        # 41-2031 Retail Salespersons
        ("Sales Associate", "41-2031"),
        ("Retail Sales Associate", "41-2031"),
        # unchanged: managers are Sales Managers (11-2022)
        ("Sales Manager", "11-2022"),
        ("Regional Sales Manager", "11-2022"),
    ],
)
def test_d3_standardizer_soc_for_sales_roles(role: str, soc: str) -> None:
    assert standardizer.get_soc_code(role) == soc


@pytest.mark.parametrize(
    "role",
    [
        "Sales Representative",
        "Sales Rep",
        "Account Executive",
        "Business Development Representative",
        "BDR",
        "SDR",
        "Sales Associate",
    ],
)
def test_d3_no_sales_worker_is_a_sales_manager(role: str) -> None:
    assert standardizer.get_soc_code(role) != "11-2022"
    assert standardizer.normalize_role(role) != "sales_manager"


def test_d3_a_sales_report_is_not_a_sales_representative() -> None:
    # the new alias must not match inside a longer word run ("sales rep" in
    # "sales reporting")
    assert standardizer.get_soc_code("Sales Reporting Analyst") == ""


def test_d3_retail_sales_associate_is_blue_collar_like_other_retail_roles() -> None:
    for role in ("Sales Associate", "Retail Sales Associate"):
        got = collar_intelligence.classify_collar(role)
        assert got["collar_type"] == "blue_collar", (role, got)
    assert (
        collar_intelligence.classify_collar("Retail Associate")["collar_type"]
        == "blue_collar"
    )


@pytest.mark.parametrize(
    "role",
    [
        "Sales Representative",
        "Sales Rep",
        "Account Executive",
        "Business Development Representative",
        "BDR",
        "SDR",
        "Sales Manager",
    ],
)
def test_d3_b2b_sales_roles_keep_the_white_collar_they_had(role: str) -> None:
    # They reached white collar through the wrong Sales Managers SOC. The correct
    # 41-xxxx SOC must not flip them to blue collar (major group 41).
    got = collar_intelligence.classify_collar(role)
    assert got["collar_type"] == "white_collar", (role, got)
    assert got["confidence"] == 0.92 and got["method"] == "soc_code"


@pytest.mark.parametrize(
    "soc,collar",
    [
        ("41-4012", "white_collar"),
        ("414012", "white_collar"),
        ("41-3091", "white_collar"),
        ("41-2031", "blue_collar"),
        ("41-1011", "blue_collar"),
        ("53-3032", "blue_collar"),
        ("11-2022", "white_collar"),
    ],
)
def test_d3_collar_from_an_explicit_soc(soc: str, collar: str) -> None:
    assert (
        collar_intelligence.classify_collar("anything", soc_code=soc)["collar_type"]
        == collar
    )


# --------------------------------------------------------------------- D4 ---


def _plan(*locations: object, **extra: object) -> Dict[str, object]:
    return dict({"locations": list(locations)}, **extra)


def test_d4_a_bare_foreign_only_city_is_not_a_us_market() -> None:
    # Dortmund: in the shared world-city list, absent from the US gazetteer.
    assert plan_location.resolve_location("Dortmund").status == "unresolved"
    assert plan_geo.location_is_us("Dortmund") is False
    assert plan_geo.is_us_plan(_plan("Dortmund")) is False
    assert plan_geo.plan_has_us_market(_plan("Dortmund")) is False
    assert plan_geo.non_us_signals(_plan("Dortmund")) == ["Dortmund"]
    assert plan_geo.is_us_plan(_plan({"city": "dortmund"})) is False


@pytest.mark.parametrize("name", ["Berlin", "Munich", "Toronto", "Hamburg"])
def test_d4_a_name_the_us_also_has_stays_unresolved(name: str) -> None:
    # Berlin NH/WI/CT, Munich ND, Toronto OH, Hamburg NY are real US places:
    # guessing "foreign" would turn a Berlin, NH plan into a non-US one. The
    # name stays unresolved, so the existing back-compat default (US) applies.
    assert plan_location.resolve_location(name).status != "unresolved"
    assert plan_geo.location_is_us(name) is None
    assert plan_geo.is_us_plan(_plan(name)) is True


def test_d4_a_us_only_name_stays_us() -> None:
    res = plan_location.resolve_location("Springfield")
    assert res.status == "ambiguous"
    assert all(a.get("state_usps") for a in res.alternatives)
    assert plan_geo.location_is_us("Springfield") is None
    assert plan_geo.is_us_plan(_plan("Springfield")) is True
    assert plan_geo.plan_has_us_market(_plan("Springfield")) is True


def test_d4_the_explicit_forms_are_unchanged() -> None:
    assert plan_geo.location_is_us("Berlin, NH") is True
    assert plan_geo.location_is_us("Berlin, Germany") is False
    assert plan_geo.location_is_us("Dortmund, Germany") is False
    assert plan_geo.location_is_us("Dortmund, TX") is True
    assert plan_geo.location_is_us("Denver") is None
    assert plan_geo.location_is_us("Remote") is None


def test_d4_a_mixed_plan_keeps_its_us_market() -> None:
    plan = _plan("Denver, CO", "Dortmund")
    assert plan_geo.is_us_plan(plan) is False
    assert plan_geo.plan_has_us_market(plan) is True
    assert plan_geo.non_us_signals(plan) == ["Dortmund"]
    assert plan_geo.plan_has_us_market(_plan("Dortmund", "Mumbai")) is False


def test_d4_the_wizards_region_select_still_wins() -> None:
    assert plan_geo.is_us_plan(_plan("Dortmund", target_region="us_only")) is True
    assert plan_geo.is_us_plan(_plan("Denver, CO", target_region="global")) is False


def test_d4_plan_location_is_unchanged_and_does_not_recurse() -> None:
    # plan_location asks plan_geo._resolve_candidate whether a name is abroad
    # AFTER finding no US place; the new rule lives in a separate layer so that
    # call (and plan_geo -> plan_location -> plan_geo) cannot loop.
    res = plan_location.resolve_location("Dortmund")
    assert res.status == "unresolved" and not res.matched_via
    assert plan_geo._resolve_candidate("Dortmund") is None


def _norm_place(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", country_registry._fold(text))


def _us_place_names() -> set:
    names = set()
    with open(_DATA / "geo" / "us_places.tsv", encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            names.add(_norm_place(row["display_name"]))
    names.discard("")
    return names


def test_d4_every_city_the_new_rule_flips_has_no_us_namesake() -> None:
    """Audit: the cities the bare-foreign-city rule reads as abroad are exactly
    the world-city list entries the US place gazetteer does not hold at all,
    and research agrees they are not in the United States. (London, Dublin,
    Amsterdam, Madrid, Manila and Wellington are abroad through the older
    _INTL_TOKENS rule, US namesakes and all; they are not this rule's.)"""
    us = _us_place_names()
    new_rule = [
        city
        for city in country_registry.WORLD_CITY_COUNTRY
        if plan_geo._resolve_candidate(city) is None
        and plan_geo.location_is_us(city) is False
    ]
    assert {"dortmund", "mumbai", "tokyo", "bangalore"} <= set(new_rule)
    clash = [c for c in new_rule if _norm_place(c) in us]
    assert not clash, clash
    for city in new_rule:
        got = research._detect_country(city)
        assert got and got != "United States", (city, got)
    # a collision-free name must not be left unresolved by mistake: when the rule
    # skipped it, plan_location found a US place or a typo-match for it
    for city in country_registry.WORLD_CITY_COUNTRY:
        if city in new_rule or plan_geo._resolve_candidate(city) is False:
            continue
        assert plan_location.resolve_location(city).status != "unresolved", city


def test_d4_research_still_reads_the_moved_city_table_the_same_way() -> None:
    expected = {
        "Berlin": "Germany",
        "Munich": "Germany",
        "Toronto": "Canada",
        "Mumbai": "India",
        "Sydney": "Australia",
        "London": "United Kingdom",
        "Dortmund": "Germany",
    }
    for city, country in expected.items():
        assert research._detect_country(city) == country, city
    # US forms of a world-city name are still the US
    assert research._detect_country("Berlin, NH") in (None, "United States")


# ---------------------------------------------------------- F2 (review) ---


@pytest.mark.parametrize(
    "role,industry",
    [
        ("Inside Sales Associate", ""),
        ("Technical Sales Associate", "tech_engineering"),
        ("Financial Sales Associate", "finance_banking"),
        ("Sales Associate - B2B SaaS", ""),
    ],
)
def test_f2_a_qualified_sales_associate_is_not_a_retail_salesperson(
    role: str, industry: str
) -> None:
    # normalize_role matches aliases as substrings: "sales associate" as a plain
    # alias made these Retail Salespersons (41-2031, blue collar, volume).
    assert standardizer.normalize_role(role) != "retail_associate"
    assert standardizer.get_soc_code(role) != "41-2031"
    got = collar_intelligence.classify_collar(role, industry)
    assert got["collar_type"] == "white_collar", (role, got)
    assert got["channel_strategy"] == "targeted", (role, got)


@pytest.mark.parametrize(
    "role", ["Sales Associate", "Retail Sales Associate", "  sales   ASSOCIATE "]
)
def test_f2_the_bare_sales_associate_titles_are_still_retail(role: str) -> None:
    assert standardizer.get_soc_code(role) == "41-2031"
    assert collar_intelligence.classify_collar(role)["collar_type"] == "blue_collar"


def test_f2_sales_manager_is_unchanged() -> None:
    assert standardizer.get_soc_code("Sales Manager") == "11-2022"
    assert (
        collar_intelligence.classify_collar("Sales Manager")["collar_type"]
        == "white_collar"
    )


# ---------------------------------------------------------- F1 (review) ---


@pytest.fixture(scope="module")
def app_mod():
    import app

    return app


@pytest.mark.parametrize(
    "city,region",
    [
        ("Dortmund", "emea"),
        ("Düsseldorf", "emea"),
        ("Mumbai", "apac"),
        ("Tokyo", "apac"),
        ("Buenos Aires", "americas"),
        (" dortmund ", "emea"),
        ({"city": "Dortmund"}, "emea"),
    ],
)
def test_f1_a_bare_foreign_only_city_is_placed_in_its_region(
    app_mod, city: object, region: str
) -> None:
    assert app_mod._market_region(city) == region
    assert app_mod._location_market_hint(city) == region


@pytest.mark.parametrize(
    "name", ["Berlin", "Munich", "Toronto", "Hamburg", "Springfield", "Denver"]
)
def test_f1_a_name_with_a_us_namesake_stays_unknown(app_mod, name: str) -> None:
    assert app_mod._market_region(name) == app_mod._GUESS
    assert app_mod._location_market_hint(name) == "unknown"


def test_f1_the_explicit_forms_are_unchanged(app_mod) -> None:
    assert app_mod._location_market_hint("Berlin, Germany") == "emea"
    assert app_mod._location_market_hint("Berlin, NH") == "us"
    assert app_mod._location_market_hint("Denver, CO") == "us"
    assert app_mod._location_market_hint("Remote") == "none"


def test_f1_every_city_plan_geo_flips_has_a_market_region(app_mod) -> None:
    """The two classifiers must agree: every bare city plan_geo reads as abroad
    is placed in a region by the wizard's hint, never left as "unknown"."""
    flips = [
        c
        for c in country_registry.WORLD_CITY_COUNTRY
        if plan_geo._resolve_candidate(c) is None
        and plan_geo.location_is_us(c) is False
    ]
    assert len(flips) >= 20
    for city in flips:
        assert app_mod._location_market_hint(city) in (
            "emea",
            "apac",
            "americas",
        ), city
        assert plan_geo.bare_foreign_city_country(city), city
    # and no name with a US namesake is placed on a guess
    for city in ("Berlin", "Munich", "Toronto", "Hamburg", "Sydney", "Paris"):
        assert plan_geo.bare_foreign_city_country(city) is None, city


def _wizard_region(hints: List[str]) -> str:
    """templates/partials/index/body_app_js.html novaRegionFromMarkets: a single
    emea/apac market sets that region; an unplaceable one leaves the selector on
    its "US Only" default."""
    placed = sorted({h for h in hints if h != "none"})
    if not placed or "unknown" in placed:
        return "us_only"
    if placed == ["us"]:
        return "us_only"
    if len(placed) == 1 and placed[0] in ("emea", "apac"):
        return placed[0]
    return "global"


from tests.live_server import live_port, post_json  # noqa: E402,F401
from tests.test_intl_boards_end_to_end import _BASE, _read  # noqa: E402,F401


@pytest.mark.parametrize("city", ["Mumbai", "Dortmund"])
def test_f1_a_bare_foreign_city_brief_with_the_derived_region_has_clean_qa(
    live_port, app_mod, city: str  # noqa: F811
) -> None:
    import bundle_qa

    region = _wizard_region([app_mod._location_market_hint(city)])
    assert region in ("apac", "emea")

    captured: dict = {}
    real_gate = bundle_qa.gate_bundle

    def spy(pptx, xlsx, data, *a, **k):
        out = real_gate(pptx, xlsx, data, *a, **k)
        captured["gate"] = out
        captured["data"] = data
        return out

    bundle_qa.gate_bundle = spy
    try:
        payload = dict(
            _BASE,
            client_name=f"Roles Followups Fixture {city}",
            locations=[city],
            competitors=["DHL"],
            target_region=region,
        )
        status, raw = post_json(live_port, "/api/generate", payload, timeout=300.0)
    finally:
        bundle_qa.gate_bundle = real_gate
    assert status == 200 and isinstance(raw, bytes), status
    gate = captured.get("gate")
    assert gate is not None, "bundle_qa gate did not run"
    assert captured["data"].get("target_region") == region
    findings = [(f.get("severity"), f.get("code")) for f in gate.get("findings") or []]
    assert ("critical", "non_us_text_on_us_plan") not in findings, findings
    assert not [f for f in findings if f[0] == "critical"], findings
