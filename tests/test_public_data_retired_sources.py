"""Regression tests: DataUSA / RESTCountries / ILO repaired or retired.

PROD DEFECTS (every run, including US-only plans):

* ``datausa.io/api/data ... HTTP 404`` per state: the legacy API was removed; the
  old function then returned a curated state table AS IF live and stamped one
  state's population on a single city ("Lancaster, PA" = 12,961,683).
* ``restcountries ... Expecting value``: v1-v4 are switched off (301 -> a static
  "deprecated" JSON); v5 needs an account + bearer key.
* ``ILO sdmx 404``: the dataflow the client queried no longer exists; on failure
  it silently returned CURATED constants labelled as ILO data, and it matched
  countries by substring ("us" inside "Columbus, OH"), so US-only plans called it.

The tests drive the real source functions; only the network is faked with the
providers' REAL recorded responses.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import public_data_sources as pds  # noqa: E402
from tests.public_data_fakes import FakeNet, load  # noqa: E402
from tests.test_enrich_geopolitical_budget import (  # noqa: E402,F401
    isolated_breakers,
)

US_ONLY = ["Columbus, OH", "Houston, TX", "Hershey, PA"]


@pytest.fixture
def net(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, data: None)
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    pds.clear_caches()
    fake = FakeNet().install(monkeypatch)
    yield fake
    pds.clear_caches()



PLAN = ["Hershey, PA", "Edgerton, KS", "Hazleton, PA", "Lancaster, PA", "Wheeling, WV"]


# ---------------------------------------------------------------------------
# 5. DataUSA: the legacy per-state source no longer stamps a state on a city
# ---------------------------------------------------------------------------


def test_datausa_location_no_longer_stamps_a_state_figure_on_a_city(net: FakeNet) -> None:
    out = api_enrichment.fetch_datausa_location_data(PLAN)

    lancaster = out["locations"]["Lancaster, PA"]
    assert lancaster["population"] == 57719  # NOT Pennsylvania's 12,961,683
    assert lancaster["median_income"] == lancaster["median_household_income"] == 63690
    # every PA city gets its own entry (the old code kept only the last one)
    assert {"Hershey, PA", "Hazleton, PA", "Lancaster, PA"} <= set(out["locations"])
    assert net.count("datausa.io") == net.count("api.datausa.io")  # legacy host never called


def test_datausa_loc_and_restcountries_are_retired_in_the_registry() -> None:
    for label in ("DataUSA-Loc", "RESTCountries"):
        assert api_enrichment.is_source_retired(label)
        entry = api_enrichment.RETIRED_SOURCES[label]
        assert entry["status"] == "retired" and entry["reason"]


# ---------------------------------------------------------------------------
# ILO
# ---------------------------------------------------------------------------


def test_ilo_returns_live_values_from_the_current_dataflows(net: FakeNet) -> None:
    out = api_enrichment.fetch_ilo_labour_data(["London, UK"])

    gbr = out["countries"]["GBR"]
    assert gbr["source"] == "ILO ILOSTAT SDMX (live)"
    assert gbr["unemployment_rate"] == 4.9  # 4.896 (2025, recorded)
    assert gbr["youth_unemployment"] == 15.2
    assert gbr["labor_force_participation"] == 62.8
    assert gbr["year"] == "2025"
    urls = net.urls("sdmx.ilo.org")
    assert len(urls) == 3  # one single-series request per indicator
    assert not any("DF_STI_ALL_UNE_DEA1_SEX_AGE_RT" in u for u in urls)
    assert any("DF_UNE_DEAP_SEX_AGE_RT" in u for u in urls)
    assert any("DF_EAP_DWAP_SEX_AGE_RT" in u for u in urls)


def test_ilo_is_not_queried_for_a_us_only_plan(net: FakeNet) -> None:
    """The old substring match ("us" in "columbus"/"houston") sent US plans to ILO."""
    assert api_enrichment.fetch_ilo_labour_data(US_ONLY) == {}
    assert net.count("sdmx.ilo.org") == 0


def test_ilo_failure_is_failed_not_a_curated_fallback_dressed_as_live(
    net: FakeNet, isolated_breakers
) -> None:
    net.down("sdmx.ilo.org", 503)

    result, status, meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_ilo_labour_data(["London, UK"]), "ILO-ILOSTAT"
    )

    assert (result, status) == (None, "error")
    assert "ILOSTAT returned no data" in meta["error_message"]


def test_ilo_us_only_plan_status_is_not_applicable(net: FakeNet, isolated_breakers) -> None:
    _r, status, _m = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_ilo_labour_data(US_ONLY), "ILO-ILOSTAT"
    )
    assert status == "empty"


def test_ilo_every_call_is_bounded() -> None:
    assert pds.CALL_TIMEOUT_S <= 6.0 and pds.ILO_BUDGET_S <= 8.0


def test_ilo_parser_reads_the_recorded_sdmx_json_2_shape() -> None:
    payload = load("ilo_gbr_annual.json")["GBR:unemployment"]
    value, period = pds.parse_ilo_latest(payload)
    assert (round(value, 3), period) == (4.896, "2025")
    with pytest.raises(pds.FetchError) as bad:
        pds.parse_ilo_latest({"data": {}})
    assert bad.value.kind == "schema"


# ---------------------------------------------------------------------------
# RESTCountries (retired)
# ---------------------------------------------------------------------------


def test_restcountries_makes_no_request_and_is_retired(net: FakeNet) -> None:
    assert api_enrichment.fetch_country_data(["London, UK", "Toronto, Canada"]) == {}
    assert net.count("restcountries.com") == 0
    assert api_enrichment.RETIRED_SOURCES["RESTCountries"]["verified"] == "2026-10-01"


def test_restcountries_301_is_classified_not_mistaken_for_json(net: FakeNet) -> None:
    """Pins the root cause the old client logged as 'Expecting value'."""
    with pytest.raises(pds.FetchError) as caught:
        pds.request_json("https://restcountries.com/v3.1/alpha/GBR?fields=name,population")
    assert caught.value.kind == "redirect" and caught.value.status == 301
    # even when the redirect is followed the body is the 'deprecated' notice
    body = load("restcountries_legacy_removed.json")["followed_body"]
    assert body["success"] is False and "deprecated" in body["errors"][0]["message"]
