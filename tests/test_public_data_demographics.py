"""Regression tests: US location demographics work, per place, and report honestly.

PROD DEFECTS (Render logs 2026-09-23..30, every real plan):

* ``Census ACS fetch failed for all years`` + ``No demographic data for:
  <every location>``. ROOT CAUSE (probed live 2026-10-01): api.census.gov now
  answers every keyless data query with ``302 -> /data/missing_key.html`` and an
  empty body; the old client read that as ``Expecting value`` and tried three
  ACS vintages serially at 10 s each (one run hung past the 20 s deadline). It
  also never tried the newest published vintage and only asked for STATE rows.

The tests drive the REAL ``api_enrichment.fetch_location_demographics`` and
``_safe_call``; only the network is faked, with the REAL provider responses
recorded under ``tests/fixtures/public_data_sources/``.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import public_data_sources as pds  # noqa: E402
from tests.public_data_fakes import FakeNet, Resp, load  # noqa: E402
from tests.test_enrich_geopolitical_budget import (  # noqa: E402,F401
    isolated_breakers,
)

PLAN = [
    "Hershey, PA",
    "Edgerton, KS",
    "Hazleton, PA",
    "Lancaster, PA",
    "Wheeling, WV",
]
# Real ACS 2020-2024 5-year values (recorded from the Tesseract API).
EXPECTED_POP = {
    "Hershey, PA": 14242,
    "Edgerton, KS": 1924,
    "Hazleton, PA": 30111,
    "Lancaster, PA": 57719,
    "Wheeling, WV": 26350,
}
EXPECTED_INCOME = {
    "Hershey, PA": 78587,
    "Edgerton, KS": 84838,
    "Hazleton, PA": 46177,
    "Lancaster, PA": 63690,
    "Wheeling, WV": 48590,
}


@pytest.fixture
def net(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    pds.clear_caches()
    fake = FakeNet().install(monkeypatch)
    yield fake
    pds.clear_caches()




# ---------------------------------------------------------------------------
# 1. Census / ACS: real place-level data, one distinct figure per city
# ---------------------------------------------------------------------------


def test_every_real_prod_location_gets_its_own_place_level_figures(net: FakeNet) -> None:
    out = api_enrichment.fetch_location_demographics(PLAN)

    assert {loc: out[loc]["population"] for loc in PLAN} == EXPECTED_POP
    assert {loc: out[loc]["median_income"] for loc in PLAN} == EXPECTED_INCOME
    assert all(out[loc]["geo_level"] == "Place" for loc in PLAN)
    assert all(out[loc]["geo_matches_location"] is True for loc in PLAN)
    assert "_unresolved" not in out
    # provenance names the real vintage, not just "Census"
    assert out["Hershey, PA"]["year"] == 2024
    assert out["Hershey, PA"]["vintage"] == "ACS 2020-2024 5-year"
    # three PA towns no longer share one number
    assert len({out[l]["population"] for l in ("Hershey, PA", "Hazleton, PA", "Lancaster, PA")}) == 3


def test_keyless_census_is_never_called_and_calls_are_not_serial(net: FakeNet) -> None:
    """The three serial 10 s ACS vintage tries are gone: api.census.gov is not
    contacted without a key, and the whole plan costs 1 vintage lookup + one
    name search per city + one batched population + one batched income query."""
    api_enrichment.fetch_location_demographics(PLAN)

    assert net.count("api.census.gov") == 0
    assert net.count("api.datausa.io") <= 1 + len(PLAN) + 2
    # batched: ONE data query per measure covers all five places
    data_calls = [u for u in net.urls("api.datausa.io") if "data.jsonrecords" in u]
    assert len(data_calls) == 2


def test_independent_geographies_run_in_parallel_under_a_bounded_budget(
    net: FakeNet,
) -> None:
    """With 0.5 s per upstream call a serial walk of the same 8 calls takes
    4 s; the pooled fetch finishes in ~3 round trips."""
    net.latency_s = 0.5
    started = time.monotonic()
    out = api_enrichment.fetch_location_demographics(PLAN)
    elapsed = time.monotonic() - started

    assert set(out) >= set(PLAN)
    assert elapsed < 3.0, f"demographics took {elapsed:.1f}s; geographies are serial"


def test_newest_vintage_lookup_is_cached_per_process(net: FakeNet) -> None:
    api_enrichment.fetch_location_demographics(["Hershey, PA"])
    api_enrichment.fetch_location_demographics(["Hazleton, PA"])

    year_lookups = [u for u in net.urls("api.datausa.io") if "level=Year" in u]
    assert len(year_lookups) == 1


def test_one_slow_call_cannot_exceed_the_source_budget(net: FakeNet) -> None:
    net.latency_s = 0.9
    targets = [
        pds.UsGeoTarget(
            location="Hershey, PA", kind="city", city="Hershey", state_usps="PA",
            state_name="Pennsylvania", state_fips="42", county_fips="42043",
            county_name="Dauphin County",
        )
    ]
    started = time.monotonic()
    result = pds.fetch_us_demographics(targets, budget_s=1.2)
    elapsed = time.monotonic() - started

    assert elapsed < 2.4, f"source overran its 1.2 s budget: {elapsed:.1f}s"
    assert "Hershey, PA" in result.unresolved  # explicit, not silently empty


# ---------------------------------------------------------------------------
# 2. Honest status: data / not applicable / failed
# ---------------------------------------------------------------------------


def test_unresolvable_location_is_an_explicit_per_city_failure(net: FakeNet) -> None:
    out = api_enrichment.fetch_location_demographics(["Hershey, PA", "Zzqxwvville, PA"])

    assert out["Hershey, PA"]["population"] == EXPECTED_POP["Hershey, PA"]
    assert "Zzqxwvville, PA" not in out
    assert out["_unresolved"]["Zzqxwvville, PA"]["reason"] == "location_not_recognised"


def test_provider_down_is_reported_as_failed_not_not_applicable(
    net: FakeNet, isolated_breakers
) -> None:
    """The pre-fix source returned {} on failure, which the confidence metric
    reads as 'not applicable'. A source that had work and got nothing must
    raise so ``_safe_call`` reports it as FAILED."""
    net.down("api.datausa.io", 503)

    result, status, meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_location_demographics(PLAN), "Census-ACS"
    )

    assert result is None
    assert status == "error"
    assert meta["success"] is False
    assert "resolved no demographics" in meta["error_message"]


def test_data_status_for_real_data(net: FakeNet, isolated_breakers) -> None:
    _result, status, meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_location_demographics(PLAN), "Census-ACS"
    )
    assert (status, meta["success"]) == ("ok", True)


def test_nothing_to_fetch_is_not_applicable(net: FakeNet, isolated_breakers) -> None:
    """'Remote' / 'Nationwide' are not places: no failure, no data -> {}."""
    result, status, _meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_location_demographics(["Remote", "Nationwide"]),
        "Census-ACS",
    )
    assert status == "empty"
    assert net.count("api.datausa.io") == 0


# ---------------------------------------------------------------------------
# 3. Fallbacks are labelled, never passed off as the city's own number
# ---------------------------------------------------------------------------


def test_place_missing_in_acs_falls_back_to_a_labelled_county(net: FakeNet) -> None:
    net.override(
        lambda r: r.host == "api.datausa.io" and r.path.endswith("/members") and r.params.get("search") == "Hershey",
        lambda r: Resp(200, {"members": []}),
    )
    entry = api_enrichment.fetch_location_demographics(["Hershey, PA"])["Hershey, PA"]

    assert entry["geo_level"] == "County"
    assert entry["geo_name"] == "Dauphin County, PA"
    assert entry["geo_matches_location"] is False
    assert entry["area_population"] == 289593
    assert entry["area_median_income"] == 76242
    assert entry["fallback_reason"] == "place_not_in_acs"
    # the county figure must NOT be published as the city's population
    assert "population" not in entry and "median_income" not in entry


def test_county_missing_too_falls_back_to_a_labelled_state(net: FakeNet) -> None:
    net.override(
        lambda r: r.host == "api.datausa.io" and r.path.endswith("/members") and r.params.get("search") == "Hershey",
        lambda r: Resp(200, {"members": []}),
    )
    net.override(
        lambda r: r.host == "api.datausa.io" and r.params.get("drilldowns") == "County",
        lambda r: Resp(200, {"columns": [], "data": []}),
    )
    entry = api_enrichment.fetch_location_demographics(["Hershey, PA"])["Hershey, PA"]

    assert entry["geo_level"] == "State"
    assert entry["area_population"] == 13018639
    assert "population" not in entry


def test_a_state_location_owns_its_state_figure(net: FakeNet) -> None:
    entry = api_enrichment.fetch_location_demographics(["Pennsylvania"])["Pennsylvania"]

    assert entry["geo_level"] == "State"
    assert entry["geo_matches_location"] is True
    assert entry["population"] == 13018639


def test_non_us_locations_get_country_level_population_only(net: FakeNet) -> None:
    out = api_enrichment.fetch_location_demographics(["London, UK", "Manchester, UK"])

    expected = load("worldbank_pop_gbr.json")["response"][1][0]["value"]
    for loc in ("London, UK", "Manchester, UK"):
        assert out[loc]["geo_level"] == "Country"
        assert out[loc]["country_population"] == int(expected)
        assert out[loc]["geo_matches_location"] is False
        # the national number must never be published as the CITY's population
        assert "population" not in out[loc]
    # one World Bank call per COUNTRY, not per city
    assert net.count("api.worldbank.org") == 1
    assert net.count("api.datausa.io") == 0


# ---------------------------------------------------------------------------
# 4. api.census.gov with a key: newest vintage first, 404-only fallback
# ---------------------------------------------------------------------------


def test_census_without_a_key_is_the_documented_302_not_json(net: FakeNet) -> None:
    """Pins the root cause the old client mis-read as 'Expecting value'."""
    with pytest.raises(pds.FetchError) as caught:
        pds.request_json("https://api.census.gov/data/2024/acs/acs5?get=NAME&for=state:*")
    assert caught.value.kind == "redirect"
    assert pds.census_key_problem(caught.value) == "census_key_missing"


def test_census_key_is_used_as_state_fallback_when_data_usa_is_down(
    net: FakeNet, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CENSUS_API_KEY", "TESTKEY")
    net.down("api.datausa.io", 503)

    entry = api_enrichment.fetch_location_demographics(["Hershey, PA"])["Hershey, PA"]

    assert entry["geo_level"] == "State"
    assert entry["source"] == "US Census ACS 5-year (api.census.gov)"
    assert entry["area_population"] == 13018639
    census_urls = net.urls("api.census.gov")
    assert len(census_urls) == 1 and "/data/2024/acs/acs5" in census_urls[0]
    assert "key=TESTKEY" in census_urls[0]


def test_unpublished_vintage_falls_back_once_and_is_remembered(
    net: FakeNet, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CENSUS_API_KEY", "TESTKEY")
    net.down("api.datausa.io", 503)
    net.override(
        lambda r: r.host == "api.census.gov" and "/data/2024/" in r.path,
        lambda r: Resp(404, "unknown dataset"),
    )
    api_enrichment.fetch_location_demographics(["Hershey, PA"])
    first = [u.split("?")[0] for u in net.urls("api.census.gov")]
    assert first == [
        "https://api.census.gov/data/2024/acs/acs5",
        "https://api.census.gov/data/2023/acs/acs5",
    ]
    # second plan in the same process goes straight to the vintage that worked
    api_enrichment.fetch_location_demographics(["Hazleton, PA"])
    second = [u.split("?")[0] for u in net.urls("api.census.gov")][2:]
    assert second == ["https://api.census.gov/data/2023/acs/acs5"]


def test_bad_census_key_is_one_round_trip_per_process(
    net: FakeNet, monkeypatch: pytest.MonkeyPatch, isolated_breakers
) -> None:
    monkeypatch.setenv("CENSUS_API_KEY", "BADKEY")
    net.down("api.datausa.io", 503)

    for _ in range(2):
        with pytest.raises(pds.SourceFailure) as caught:
            api_enrichment.fetch_location_demographics(["Hershey, PA"])
    assert net.count("api.census.gov") == 1
    assert "census_key_invalid" in str(caught.value)


def test_no_key_and_data_usa_down_says_why(net: FakeNet, isolated_breakers) -> None:
    net.down("api.datausa.io", 503)
    with pytest.raises(pds.SourceFailure) as caught:
        api_enrichment.fetch_location_demographics(["Hershey, PA"])
    assert "census_key_not_configured" in str(caught.value)
    assert net.count("api.census.gov") == 0
