"""Regression tests: the geopolitical LLM call no longer stalls enrich_data().

PROD DEFECT (Render logs, 5 of 5 generations 2026-09-24/25): after the
parallel API pool finished (~8 s), ``api_enrichment.enrich_data()`` called
``fetch_geopolitical_context()`` SERIALLY -- an ``llm_router.call_llm``
(TASK_RESEARCH) with the router's default 35 s budget. Its provider chain
(deepseek read-timeout, then claude_haiku) never succeeded, so app.py's 20 s
enrichment deadline fired on every run and each plan waited ~12 s for nothing
("Enrichment complete in 37-43 s" logged after the job had already finished).

THE FIX: the geopolitical call starts on its own thread before the pool,
runs concurrently with it under a short hard budget
(``GEOPOLITICAL_TIMEOUT_DEFAULT_S`` <= 8 s, env ``NOVA_GEOPOLITICAL_TIMEOUT_S``)
that is also handed to llm_router as ``timeout_budget``; its content is used
only if it arrived within that budget.

These tests drive the REAL ``enrich_data()``. Only the boundaries are faked:
``llm_router.call_llm`` (the LLM router) and the per-source ``fetch_*``
collaborators (so the pool is deterministic and hermetic).
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
import llm_router  # noqa: E402

# The 8 source fetchers enrich_data() dispatches for a US-only, locations-only
# request. (It used to be 13: RESTCountries and DataUSA-Loc are retired
# (api_enrichment.RETIRED_SOURCES) and ILO / UK-ONS / StatCan are country-gated
# -- a US-only plan has no non-US / UK / Canadian location, so none of the five
# is dispatched any more.)
LOCATION_SOURCE_FETCHERS: List[str] = [
    "fetch_location_demographics",  # Census-ACS
    "fetch_global_indicators",  # WorldBank
    "fetch_currency_rates",  # CurrencyRates
    "fetch_fred_indicators",  # FRED
    "fetch_imf_indicators",  # IMF
    "fetch_geonames_data",  # GeoNames
    "fetch_teleport_city_data",  # Teleport
    "fetch_eurostat_labour_data",  # Eurostat
]

# The 13 dispatched for the telemetry-shaped request (industry + locations +
# client_name + competitors, no roles); prod logged "16/18 APIs ok" before the
# five retired / country-gated sources were removed from a US-only dispatch.
TELEMETRY_SOURCE_FETCHERS: List[str] = LOCATION_SOURCE_FETCHERS + [
    "fetch_industry_employment",  # BLS-QCEW
    "fetch_company_info",  # Wikipedia
    "fetch_company_metadata",  # Clearbit-Auto
    "fetch_sec_company_data",  # SEC-EDGAR
    "fetch_competitor_logos",  # Clearbit
]

HERSHEY_REQUEST: Dict[str, Any] = {"locations": ["Hershey, PA"]}


@pytest.fixture
def isolated_breakers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep these tests out of the shared circuit-breaker / rate-limiter
    state (both directions: stale state can't skip our sources, and our
    deliberate failures can't open a breaker for later tests)."""
    monkeypatch.setattr(api_enrichment, "_circuit_breaker_check", lambda label: False)
    monkeypatch.setattr(api_enrichment, "_rate_limit_check", lambda label: False)
    monkeypatch.setattr(
        api_enrichment, "_circuit_breaker_record_success", lambda label: None
    )
    monkeypatch.setattr(
        api_enrichment, "_circuit_breaker_record_failure", lambda label: None
    )
    monkeypatch.setattr(api_enrichment, "_rate_limit_record", lambda label: None)


def stub_sources(
    monkeypatch: pytest.MonkeyPatch,
    fetchers: List[str],
    overrides: Optional[Dict[str, Callable[..., Any]]] = None,
) -> None:
    """Every listed fetcher returns {} ("ran fine, nothing applicable")
    instantly unless *overrides* gives it a behaviour."""
    overrides = overrides or {}
    for name in fetchers:
        monkeypatch.setattr(
            api_enrichment,
            name,
            overrides.get(name, lambda *args, **kwargs: {}),
        )


def _geo_json(locations: List[str]) -> str:
    return json.dumps(
        {
            "overall_risk_score": 2.0,
            "locations": {
                loc: {"risk_score": 2.0, "events": [], "budget_adjustment_factor": 1.0}
                for loc in locations
            },
            "summary": "Stable.",
            "recommendations": ["Proceed."],
        }
    )


class _FakeRouter:
    """Stand-in for llm_router.call_llm: records each call's kwargs, can
    block (releasable at teardown so no thread outlives the test)."""

    def __init__(self, delay_s: float = 0.0) -> None:
        self.delay_s = delay_s
        self.calls: List[Dict[str, Any]] = []
        self.started_at: Optional[float] = None
        self.release = threading.Event()

    def __call__(self, **kwargs: Any) -> Dict[str, Any]:
        self.started_at = time.time()
        self.calls.append(kwargs)
        if self.delay_s:
            self.release.wait(timeout=self.delay_s)
        return {"text": _geo_json(["Hershey, PA"]), "provider": "fake_router"}


@pytest.fixture
def fake_router(monkeypatch: pytest.MonkeyPatch):
    routers: List[_FakeRouter] = []

    def _install(delay_s: float = 0.0) -> _FakeRouter:
        router = _FakeRouter(delay_s)
        routers.append(router)
        monkeypatch.setattr(llm_router, "call_llm", router)
        return router

    yield _install
    for router in routers:
        router.release.set()


def test_slow_geopolitical_llm_no_longer_stalls_enrichment(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, fake_router
) -> None:
    """The prod scenario, compressed: the LLM chain hangs far past the
    budget. enrich_data() must return within budget + epsilon with every
    other source's data intact, and must not use the late content."""
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0.3")
    fake_router(delay_s=6.0)
    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {
            "fetch_location_demographics": lambda locs: {
                "Hershey, PA": {"population": 14257}
            },
            "fetch_geonames_data": lambda locs: {"Hershey, PA": {"lat": 40.28}},
        },
    )

    t0 = time.time()
    result = api_enrichment.enrich_data(dict(HERSHEY_REQUEST))
    elapsed = time.time() - t0

    # Pre-fix this took the full 6 s LLM hang (serial call after the pool).
    assert elapsed < 2.0, f"enrich_data() blocked {elapsed:.2f}s on the geo LLM"
    assert result["location_demographics"] == {"Hershey, PA": {"population": 14257}}
    assert result["geonames_data"] == {"Hershey, PA": {"lat": 40.28}}
    assert result["geopolitical_context"] == {}
    assert result["enrichment_summary"]["geopolitical_status"] == "timeout"


def test_geopolitical_budget_is_handed_to_the_llm_router(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, fake_router
) -> None:
    """Passing the budget as timeout_budget makes the provider chain itself
    stop by then, instead of running on as a 35 s orphan thread."""
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "2.5")
    router = fake_router()
    stub_sources(monkeypatch, LOCATION_SOURCE_FETCHERS)

    api_enrichment.enrich_data(dict(HERSHEY_REQUEST))

    assert len(router.calls) == 1
    assert router.calls[0]["timeout_budget"] == 2.5


def test_geopolitical_call_overlaps_the_pool_and_is_used_when_on_time(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, fake_router
) -> None:
    """Healthy LLM: content arriving inside the budget is used exactly as
    before, and the call runs concurrently with the pool (it starts before
    the slowest source finishes) instead of after it."""
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "5")
    router = fake_router(delay_s=0.3)
    slow_source_done_at: Dict[str, float] = {}

    def _slow_census(locs: List[str]) -> Dict[str, Any]:
        time.sleep(0.8)
        slow_source_done_at["t"] = time.time()
        return {"Hershey, PA": {"population": 14257}}

    stub_sources(
        monkeypatch,
        LOCATION_SOURCE_FETCHERS,
        {"fetch_location_demographics": _slow_census},
    )

    t0 = time.time()
    result = api_enrichment.enrich_data(dict(HERSHEY_REQUEST))
    elapsed = time.time() - t0

    assert router.started_at is not None
    assert router.started_at < slow_source_done_at["t"], (
        "geopolitical call started only after the pool finished (serial)"
    )
    geo = result["geopolitical_context"]
    assert str(geo.get("source") or "").startswith("llm_geopolitical_analysis")
    assert geo["risk_level"] == "low"
    assert result["enrichment_summary"]["geopolitical_status"] == "ok"
    assert elapsed < 0.8 + 0.3 + 1.0  # overlapped, not pool + geo in series


def test_failed_llm_chain_placeholder_is_not_shipped(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers
) -> None:
    """The prod pattern once the wait is bounded: the provider chain fails
    INSIDE the budget, so fetch_geopolitical_context() returns its static
    placeholder ("... default low-risk assumption", risk_level "low") in
    time. excel_v2 renders any non-empty geopolitical_context as a client
    "Geopolitical Context" section, so the placeholder must not be stored
    -- prod never showed it (the deadline always won) and it asserts a risk
    level nobody assessed."""
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "2")
    monkeypatch.setattr(
        llm_router,
        "call_llm",
        lambda **kwargs: {"text": "", "error": "global timeout budget exceeded"},
    )
    stub_sources(monkeypatch, LOCATION_SOURCE_FETCHERS)

    result = api_enrichment.enrich_data(dict(HERSHEY_REQUEST))

    assert result["geopolitical_context"] == {}
    assert result["enrichment_summary"]["geopolitical_status"] == "fallback"


_DEFAULT = "default"


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, _DEFAULT),
        ("", _DEFAULT),
        ("3.5", 3.5),
        ("0", 0.0),
        ("30", 8.0),  # clamped: never longer than the 8 s ceiling
        ("abc", _DEFAULT),
        ("-1", _DEFAULT),
        ("nan", _DEFAULT),
    ],
)
def test_geopolitical_budget_env_override(
    monkeypatch: pytest.MonkeyPatch, raw: Optional[str], expected: Any
) -> None:
    if raw is None:
        monkeypatch.delenv("NOVA_GEOPOLITICAL_TIMEOUT_S", raising=False)
    else:
        monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", raw)
    default = api_enrichment.GEOPOLITICAL_TIMEOUT_DEFAULT_S
    assert 0 < default <= 8.0
    want = default if expected == _DEFAULT else expected
    assert api_enrichment._geopolitical_timeout_s() == want


def test_zero_budget_disables_the_geopolitical_call(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, fake_router
) -> None:
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")
    router = fake_router()
    stub_sources(monkeypatch, LOCATION_SOURCE_FETCHERS)

    result = api_enrichment.enrich_data(dict(HERSHEY_REQUEST))

    assert router.calls == []
    assert result["geopolitical_context"] == {}
    assert result["enrichment_summary"]["geopolitical_status"] == "disabled"
