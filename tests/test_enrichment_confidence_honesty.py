"""Regression tests: enrichment confidence is honest at snapshot time and final.

PROD DEFECT (Render logs, 5 of 5 generations 2026-09-24/25): every run logged
``Enrichment quality degraded: 0 APIs succeeded (min 3), confidence=0.00``
and set ``quality_warning = "Minimal data available. Plan is heavily
estimated..."`` -- including the post-fix AWP run where Wikipedia, Clearbit,
GeoNames, DataUSA, BLS-QCEW and IMF had all returned live data within 8 s.

ROOT CAUSE: app.py reads confidence from ``enrichment_summary``, but
``enrich_data()`` wrote that summary only once, at the very end (after the
slow geopolitical call). When app.py's 20 s deadline fired first, the partial
snapshot it adopted still held the ``enrichment_summary: {}`` placeholder, so
0 sources / 0.00 were read no matter what had arrived. Separately, the final
summary's S23 ratio dropped skipped/broken sources from its denominator while
still counting "empty" ones as successes, so it read 1.0 on runs where 2 of
18 sources had failed ("16/18 APIs ok ... [confidence=1.0]").

THE FIX: ``enrich_data()`` publishes a fresh summary as each source finishes
(and one before any starts), and ``api_enrichment.enrichment_confidence()``
is the one metric -- sources that returned data / sources attempted and
applicable -- read by app.py's quality warning, plan_validator and
data_synthesizer's data_quality.

The tests drive the REAL ``enrich_data()`` and the REAL
``app._run_enrich_data_with_partial_fallback`` (the async generate path);
only per-source ``fetch_*`` collaborators are stubbed.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import app as app_module  # noqa: E402
import data_synthesizer  # noqa: E402
from tests.test_enrich_geopolitical_budget import (  # noqa: E402
    HERSHEY_REQUEST,
    LOCATION_SOURCE_FETCHERS,
    TELEMETRY_SOURCE_FETCHERS,
    isolated_breakers,  # noqa: F401 -- pytest fixture
    stub_sources,
)

MINIMAL_DATA_WARNING = (
    "Minimal data available. Plan is heavily estimated. "
    "Recommend API enrichment before executing."
)


@pytest.fixture(autouse=True)
def _no_geopolitical_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    """The geopolitical LLM call is item 1's concern; keep it out of these
    timings entirely (0 disables it)."""
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")


def _data(label: str):
    return lambda *args, **kwargs: {"value": label}


def _boom(*args, **kwargs):
    raise RuntimeError("upstream HTTP 404")


@pytest.fixture
def stall():
    """A source that blocks until the test ends (released at teardown so
    the background enrich_data() thread can finish)."""
    gate = threading.Event()

    def _stalled(*args, **kwargs):
        gate.wait(timeout=15)
        return {"late": True}

    yield _stalled
    gate.set()


def test_partial_snapshot_at_deadline_reports_what_actually_arrived(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, stall
) -> None:
    """The AWP scenario: four live sources returned, one failed, one hangs
    past the deadline. The snapshot app.py adopts must carry a live summary,
    and the quality warning must NOT claim minimal data."""
    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {
            "fetch_location_demographics": _data("census"),
            "fetch_geonames_data": _data("geonames"),
            "fetch_imf_indicators": _data("imf"),
            "fetch_teleport_city_data": _data("teleport"),
            "fetch_eurostat_labour_data": _boom,  # Eurostat fails
            "fetch_global_indicators": stall,  # WorldBank hangs
        },
    )

    enriched, timed_out = app_module._run_enrich_data_with_partial_fallback(
        api_enrichment.enrich_data, dict(HERSHEY_REQUEST), None, 1.5
    )

    assert timed_out is True
    summary = enriched["enrichment_summary"]
    assert set(summary["apis_succeeded"]) == {
        "Census-ACS",
        "GeoNames",
        "IMF",
        "Teleport",
    }
    assert "Eurostat" in summary["apis_failed"]
    assert summary["apis_pending"] == ["WorldBank"]
    assert summary["complete"] is False
    # 8 dispatched, 2 "empty" (not applicable) -> 6 applicable, 4 with data.
    assert len(summary["apis_not_applicable"]) == 2
    assert summary["confidence_score"] == round(4 / 6, 3)

    warning, n_ok, confidence = app_module._enrichment_quality_warning(enriched)
    assert n_ok == 4
    assert confidence == round(4 / 6, 3)
    assert warning == ""


def test_thin_partial_snapshot_still_warns_with_real_counts(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, stall
) -> None:
    """Only 2 sources arrived before the deadline: below the min-3 bar, so a
    warning is still due -- tiered by the honest confidence (2 of 4
    applicable = 0.5 -> "Limited data"), not a blanket 0.00."""
    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {
            "fetch_location_demographics": _data("census"),
            "fetch_geonames_data": _data("geonames"),
            "fetch_eurostat_labour_data": _boom,
            "fetch_global_indicators": stall,
        },
    )

    enriched, timed_out = app_module._run_enrich_data_with_partial_fallback(
        api_enrichment.enrich_data, dict(HERSHEY_REQUEST), None, 1.5
    )

    assert timed_out is True
    warning, n_ok, confidence = app_module._enrichment_quality_warning(enriched)
    assert n_ok == 2
    assert confidence == 0.5
    assert warning.startswith("Limited data available.")


def test_genuinely_empty_enrichment_still_gets_the_minimal_data_warning(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, stall
) -> None:
    """Nothing returned data (every source failed or hung): the strongest
    warning must still fire."""
    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {name: _boom for name in LOCATION_SOURCE_FETCHERS[1:]}
        | {"fetch_location_demographics": stall},
    )

    enriched, timed_out = app_module._run_enrich_data_with_partial_fallback(
        api_enrichment.enrich_data, dict(HERSHEY_REQUEST), None, 1.5
    )

    assert timed_out is True
    assert enriched["enrichment_summary"]["apis_succeeded"] == []
    warning, n_ok, confidence = app_module._enrichment_quality_warning(enriched)
    assert (warning, n_ok, confidence) == (MINIMAL_DATA_WARNING, 0, 0.0)


def test_no_enrichment_at_all_gets_the_minimal_data_warning() -> None:
    for enriched in ({}, None, {"enrichment_summary": {}}):
        warning, n_ok, confidence = app_module._enrichment_quality_warning(enriched)
        assert (warning, n_ok, confidence) == (MINIMAL_DATA_WARNING, 0, 0.0)


def test_telemetry_run_final_confidence_counts_failures(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers
) -> None:
    """The prod summary line "16/18 APIs ok (10 data, 6 skipped, 2 failed)
    [confidence=1.0]": with the honest metric the 2 failures count against
    it -> 10 of 12 applicable = 0.833."""
    ok = [
        "fetch_company_info",
        "fetch_company_metadata",
        "fetch_geonames_data",
        "fetch_teleport_city_data",
        "fetch_industry_employment",
        "fetch_imf_indicators",
        "fetch_sec_company_data",
        "fetch_currency_rates",
        "fetch_fred_indicators",
        "fetch_competitor_logos",
    ]
    overrides: Dict[str, Any] = {name: _data(name) for name in ok}
    overrides["fetch_location_demographics"] = _boom  # Census-ACS
    overrides["fetch_eurostat_labour_data"] = _boom  # Eurostat
    stub_sources(monkeypatch, TELEMETRY_SOURCE_FETCHERS, overrides)

    result = api_enrichment.enrich_data(
        {
            "client_name": "AWP Safety",
            "industry": "construction_real_estate",
            "locations": ["Wheeling, WV"],
            "competitors": ["Flagger Force"],
        }
    )

    summary = result["enrichment_summary"]
    # 13 dispatched for a US-only plan (RESTCountries / DataUSA-Loc retired;
    # ILO / UK-ONS / StatCan country-gated): 10 data, 1 n/a (WorldBank), 2 failed.
    assert len(summary["apis_called"]) == 13
    assert len(summary["apis_succeeded"]) == 10
    assert len(summary["apis_not_applicable"]) == 1
    assert len(summary["apis_failed"]) == 2
    assert summary["apis_pending"] == []
    assert summary["complete"] is True
    assert summary["confidence_score"] == 0.833


def test_enrichment_confidence_keeps_broken_sources_in_the_denominator() -> None:
    summary: Dict[str, List[str]] = {
        "apis_called": ["A", "B", "C", "D", "E", "F"],
        "apis_succeeded": ["A", "B"],
        "apis_not_applicable": ["C"],
        # D circuit-broken, E rate-limited, F still pending: all applicable,
        # none delivered -> they stay in the denominator.
        "apis_failed": ["D"],
        "apis_circuit_broken": ["D", "E"],
        "apis_skipped": ["C", "E"],
    }
    assert api_enrichment.enrichment_confidence(summary) == 0.4  # 2 / 5
    assert api_enrichment.enrichment_confidence({}) == 0.0
    assert api_enrichment.enrichment_confidence(None) == 0.0
    assert (
        api_enrichment.enrichment_confidence(
            {"apis_called": ["C"], "apis_not_applicable": ["C"]}
        )
        == 0.0
    )


def test_data_quality_uses_the_same_metric() -> None:
    """data_synthesizer's data_quality (plan metadata, narrative prompt,
    synthesis log line) reads the one metric rather than its own ratio."""
    summary = {
        "apis_called": [f"S{i}" for i in range(18)],
        "apis_succeeded": [f"S{i}" for i in range(10)],
        "apis_not_applicable": [f"S{i}" for i in range(10, 16)],
        "apis_failed": ["S16", "S17"],
        "apis_skipped": [f"S{i}" for i in range(10, 16)],
    }
    dq = data_synthesizer._assess_data_quality({"enrichment_summary": summary})
    assert dq["success_rate"] == 83.3
    assert dq["apis_applicable"] == 12
    assert dq["quality_tier"] == "excellent"
