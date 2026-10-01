"""Regression tests: UK-ONS / StatCan type bug + country-gated dispatch.

PROD DEFECT (every run, including US-only plans): ``API 'UK-ONS' raised an
exception: 'list' object has no attribute 'lower'`` and ``API 'StatCan' ... 'list'
object has no attribute 'replace'``. ROOT CAUSE: ``enrich_data`` passed the plan's
location LIST as the dataset / table STRING argument of ``fetch_uk_ons_data`` /
``fetch_statcan_data``, and dispatched both for every plan, so a US-only plan
reported two failures on every run.

The tests drive the real source functions and the real ``enrich_data`` task list;
only the network is faked with the providers' REAL recorded responses.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

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




# ---------------------------------------------------------------------------
# UK-ONS / StatCan: the list-as-string TypeError + country gating
# ---------------------------------------------------------------------------


def test_uk_ons_data_never_raises_on_a_location_list(net: FakeNet) -> None:
    """The exact mistake enrich_data made: a list where the dataset name goes."""
    out = api_enrichment.fetch_uk_ons_data(["London, UK"])  # type: ignore[arg-type]
    assert out["source"] == "uk_ons" and "Unknown dataset" in out["error"]


def test_statcan_data_never_raises_on_a_location_list(net: FakeNet) -> None:
    out = api_enrichment.fetch_statcan_data(["Toronto, Canada"])  # type: ignore[arg-type]
    assert out["source"] == "statcan"
    assert out["vector_id"] == pds.STATCAN_UNEMPLOYMENT_VECTOR  # list -> default series


def test_uk_ons_for_a_uk_plan_returns_the_series(net: FakeNet) -> None:
    out = api_enrichment.fetch_uk_ons_for_locations(["London, UK", "Manchester, UK"])

    assert set(out["datasets"]) == {"employment", "unemployment", "vacancies", "earnings"}
    assert out["datasets"]["unemployment"]["latest_value"] is not None
    assert all(u.startswith("https://api.beta.ons.gov.uk/") for u in net.urls("ons.gov.uk"))
    assert net.count("ons.gov.uk") == 4  # four series, once each, for two UK cities


def test_statcan_for_a_canadian_plan_returns_the_unemployment_series(net: FakeNet) -> None:
    out = api_enrichment.fetch_statcan_for_locations(["Toronto, ON"])

    recorded = load("statcan_wds_unemployment.json")["response"][0]["object"]["vectorDataPoint"]
    assert out["unemployment_rate"] == recorded[-1]["value"]
    assert out["latest_period"] == recorded[-1]["refPer"]
    assert out["vector_id"] == 2062815
    assert net.count("statcan.gc.ca") == 1 and net.requests[-1].method == "POST"


@pytest.mark.parametrize("fetch", ["fetch_uk_ons_for_locations", "fetch_statcan_for_locations"])
def test_country_sources_do_not_touch_the_network_for_a_us_only_plan(
    net: FakeNet, fetch: str, isolated_breakers
) -> None:
    result, status, _m = api_enrichment._safe_call(
        lambda: getattr(api_enrichment, fetch)(US_ONLY), fetch
    )
    assert status == "empty"  # not applicable -- NOT failed
    assert net.requests == []


def test_uk_ons_failure_for_a_uk_plan_is_failed(net: FakeNet, isolated_breakers) -> None:
    net.down("ons.gov.uk", 503)
    _r, status, meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_uk_ons_for_locations(["London, UK"]), "UK-ONS"
    )
    assert status == "error" and "UK ONS returned no series" in meta["error_message"]


def test_statcan_failure_for_a_canadian_plan_is_failed(net: FakeNet, isolated_breakers) -> None:
    net.down("statcan.gc.ca", 500)
    _r, status, meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_statcan_for_locations(["Toronto, Canada"]), "StatCan"
    )
    assert status == "error" and "StatCan WDS" in meta["error_message"]


def test_statcan_no_longer_reports_a_404_as_a_populated_metadata_envelope(net: FakeNet) -> None:
    net.down("statcan.gc.ca", 404)
    out = api_enrichment.fetch_statcan_data()
    assert out.get("error") and "unemployment_rate" not in out


# ---------------------------------------------------------------------------
# Country gating at dispatch (the real enrich_data task list)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "locations, expected",
    [
        (["Hershey, PA", "Columbus, OH"], {"USA"}),
        (["London, UK", "Hershey, PA"], {"GBR", "USA"}),
        (["Toronto, ON"], {"CAN"}),
        (["Toronto, Canada", "Vancouver, BC"], {"CAN"}),
        (["Remote"], {"USA"}),
    ],
)
def test_plan_location_countries(locations: List[str], expected: set) -> None:
    assert api_enrichment.plan_location_countries(locations) == expected


def test_plan_location_countries_tolerates_non_strings() -> None:
    """locations may arrive as a list/dict where a string was expected."""
    assert api_enrichment.plan_location_countries(["London, UK", ["x"], None, 7]) == {"GBR"}
    assert api_enrichment.plan_location_countries({"city": "Toronto", "country": "Canada"}) == {"CAN"}


def _dispatched_labels(monkeypatch: pytest.MonkeyPatch, request: Dict[str, Any]) -> List[str]:
    """The labels enrich_data() dispatches (every source stubbed to {}), read
    from the summary the real orchestrator publishes."""
    from tests.test_enrich_geopolitical_budget import stub_sources

    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")
    fetchers = [
        n
        for n in dir(api_enrichment)
        if n.startswith("fetch_") and callable(getattr(api_enrichment, n))
    ]
    stub_sources(monkeypatch, fetchers)
    result = api_enrichment.enrich_data(request)
    return list(result["enrichment_summary"]["apis_called"])


def test_us_only_plan_dispatches_no_foreign_or_retired_source(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers
) -> None:
    called = _dispatched_labels(monkeypatch, {"locations": ["Hershey, PA", "Columbus, OH"]})

    for label in ("UK-ONS", "StatCan", "ILO-ILOSTAT", "RESTCountries", "DataUSA-Loc"):
        assert label not in called, f"{label} dispatched for a US-only plan"
    assert "Census-ACS" in called


def test_uk_plan_dispatches_uk_and_ilo_but_not_canada(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers
) -> None:
    called = _dispatched_labels(monkeypatch, {"locations": ["London, UK", "Manchester, UK"]})

    assert "UK-ONS" in called and "ILO-ILOSTAT" in called
    assert "StatCan" not in called
    assert "RESTCountries" not in called and "DataUSA-Loc" not in called


def test_canadian_plan_dispatches_statcan_but_not_uk(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers
) -> None:
    called = _dispatched_labels(monkeypatch, {"locations": ["Toronto, ON"]})

    assert "StatCan" in called and "ILO-ILOSTAT" in called
    assert "UK-ONS" not in called


def test_country_gated_sources_are_declared() -> None:
    assert api_enrichment.COUNTRY_GATED_SOURCES == {
        "UK-ONS": frozenset({"GBR"}),
        "StatCan": frozenset({"CAN"}),
        "ILO-ILOSTAT": frozenset({"*"}),
    }
    assert api_enrichment.source_applies_to_plan("UK-ONS", ["Hershey, PA"]) is False
    assert api_enrichment.source_applies_to_plan("ILO-ILOSTAT", ["Hershey, PA"]) is False
    assert api_enrichment.source_applies_to_plan("ILO-ILOSTAT", ["Hershey, PA", "Dublin, Ireland"]) is True
    assert api_enrichment.source_applies_to_plan("Census-ACS", ["London, UK"]) is True  # ungated
