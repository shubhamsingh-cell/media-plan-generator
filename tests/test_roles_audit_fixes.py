"""Fix round on the per-label role-source allowlist (2026-10-17 audit).

F1  US Adzuna counts were stamped "observed" on non-US plans, and a US city
    plan got the national total beside the city name.
F2  BLS mapped common roles to the wrong occupation on one shared word and ran
    on non-US plans.
F3  The Adzuna quota lived in one worker's memory: not shared across gunicorn
    workers, reset by every deploy.
F4  app.py's own /api/generate fan-out ignored ENRICH_ROLE_SOURCES.
F5  The BLS limiter counted dispatches, not POSTs.

Nothing here touches the network: every transport is patched and the suite's
external-network guard (tests/conftest.py) is the backstop.
"""

from __future__ import annotations

import http.client
import json
import sys
import threading
import time
import urllib.parse
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import adzuna_quota  # noqa: E402
import api_enrichment  # noqa: E402
import app  # noqa: E402
import upstash_cache  # noqa: E402
from tests.test_enrich_geopolitical_budget import (  # noqa: E402,F401
    isolated_breakers,
    stub_sources,
)
from tests.test_roles_enrichment_routing import (  # noqa: E402,F401
    _generate_server,
    _wait_for_generate_slots,
)


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch: pytest.MonkeyPatch):
    adzuna_quota.reset()
    monkeypatch.delenv("ENRICH_ROLE_SOURCES", raising=False)
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")
    monkeypatch.setattr(api_enrichment, "_rate_limiter_state", {})
    yield
    adzuna_quota.reset()


# ─── F1: Adzuna must be asked about the plan's own market ────────────────────


@pytest.fixture
def adzuna_urls(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """Keys present, the Adzuna transport patched; records every URL sent."""
    monkeypatch.setenv("ADZUNA_APP_ID", "test-id")
    monkeypatch.setenv("ADZUNA_APP_KEY", "test-key")
    sent: List[str] = []

    def _json(url: str, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        sent.append(url)
        return {"count": 4321, "mean": 51000}

    monkeypatch.setattr(api_enrichment, "_http_get_json", _json)
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)
    return sent


def _query(url: str) -> Dict[str, str]:
    return {
        k: v[0]
        for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(url).query).items()
    }


@pytest.mark.parametrize(
    "locations",
    [
        ["Dubai, UAE"],
        ["Tokyo, Japan"],
        ["Dublin, Ireland"],
        ["Riyadh"],
        ["Manila"],
        ["Remote"],
        [],
        ["Dubai, UAE", "London, UK"],  # the FIRST location decides the market
    ],
    ids=[
        "dubai",
        "tokyo",
        "dublin",
        "riyadh",
        "manila",
        "remote",
        "none",
        "dubai-first",
    ],
)
def test_f1_unsupported_or_unknown_market_sends_nothing_and_is_unavailable(
    adzuna_urls: List[str], locations: List[str]
) -> None:
    out = api_enrichment.fetch_job_market(["Warehouse Associate"], locations)

    assert adzuna_urls == []  # no request, so no US feed stood in
    entry = out["Warehouse Associate"]
    assert entry["posting_observation"] == "unavailable"
    assert entry["posting_count"] == 0
    assert "posting_observed_at" not in entry
    assert adzuna_quota.status()["daily"]["calls_used"] == 0  # nothing charged


def test_f1_us_city_plan_queries_the_city_and_records_the_scope(
    adzuna_urls: List[str],
) -> None:
    out = api_enrichment.fetch_job_market(["Warehouse Associate"], ["Chicago, IL"])

    assert len(adzuna_urls) == 1
    assert "/jobs/us/search/1" in adzuna_urls[0]
    assert _query(adzuna_urls[0])["where"] == "Chicago, Illinois"
    entry = out["Warehouse Associate"]
    assert entry["posting_observation"] == "observed"
    assert entry["posting_scope"] == "location"
    assert entry["posting_scope_label"] == "Chicago, Illinois"
    assert entry["posting_country"] == "us"


def test_f1_uk_city_plan_queries_gb_with_the_city(adzuna_urls: List[str]) -> None:
    out = api_enrichment.fetch_job_market(["Warehouse Associate"], ["London, UK"])

    assert "/jobs/gb/search/1" in adzuna_urls[0]
    assert _query(adzuna_urls[0])["where"] == "London"
    assert out["Warehouse Associate"]["posting_scope"] == "location"
    assert out["Warehouse Associate"]["posting_scope_label"] == "London"


def test_f1_multi_city_or_remote_us_plan_is_national_and_says_so(
    adzuna_urls: List[str],
) -> None:
    multi = api_enrichment.fetch_job_market(
        ["Warehouse Associate"], ["Chicago, IL", "Dallas, TX"]
    )
    remote_us = api_enrichment.fetch_job_market(["Cashier"], ["Remote, US"])

    assert all("where" not in _query(u) for u in adzuna_urls)
    for entry in (multi["Warehouse Associate"], remote_us["Cashier"]):
        assert entry["posting_observation"] == "observed"
        assert entry["posting_scope"] == "national"
        assert entry["posting_scope_label"] == "United States"


def test_f1_scope_and_city_are_part_of_the_cache_key(
    monkeypatch: pytest.MonkeyPatch, adzuna_urls: List[str]
) -> None:
    keys: List[str] = []
    monkeypatch.setattr(
        api_enrichment, "_get_cached", lambda key: keys.append(key) or None
    )
    api_enrichment.fetch_job_market(["Cashier"], ["Chicago, IL"])
    api_enrichment.fetch_job_market(["Cashier"], ["Dallas, TX"])
    assert len(set(keys)) == 2  # one city's count is never served for another


# ─── F2: BLS maps to the right occupation, and only on US plans ──────────────


def _soc_calls(monkeypatch: pytest.MonkeyPatch, role: str) -> List[str]:
    calls: List[str] = []
    monkeypatch.setattr(
        api_enrichment,
        "_fetch_bls_salary",
        lambda r, soc: calls.append(soc) or {"mean": 1, "source": "BLS OES"},
    )
    api_enrichment.fetch_salary_data([role])
    return calls


@pytest.mark.parametrize(
    "role,right_soc",
    [
        ("Sales Associate", "41-2031"),  # Retail Salespersons, not 11-2022
        ("Store Manager", "41-1011"),  # Retail first-line supervisors, not 11-2021
        ("Customer Service Representative", "43-4051"),  # not 35-3023 Fast Food
    ],
)
def test_f2_common_roles_map_to_the_right_soc_or_nothing(
    monkeypatch: pytest.MonkeyPatch, role: str, right_soc: str
) -> None:
    calls = _soc_calls(monkeypatch, role)
    wrong = {"11-2022", "11-2021", "35-3023"}
    assert not (set(calls) & wrong), calls
    assert calls in ([], [right_soc]), calls


def test_f2_a_single_shared_word_is_no_longer_a_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # "manager" is in many SOC titles; a title that merely shares it has no SOC.
    assert _soc_calls(monkeypatch, "Vibes Manager") == []
    assert _soc_calls(monkeypatch, "Warehouse Operations Associate") == []


def test_f2_exact_substring_and_standardizer_matches_still_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _soc_calls(monkeypatch, "Registered Nurse") == ["29-1141"]  # exact
    assert _soc_calls(monkeypatch, "Senior Software Engineer") == [
        "15-1252"
    ]  # substring


def _dispatch(
    monkeypatch: pytest.MonkeyPatch, plan: Dict[str, Any]
) -> "tuple[set, List[str]]":
    """Run enrich_data with every source stubbed; return (labels called, the
    roles fetch_salary_data actually received)."""
    fetchers = [
        n
        for n in dir(api_enrichment)
        if n.startswith("fetch_") and callable(getattr(api_enrichment, n))
    ]
    seen: List[str] = []
    stub_sources(
        monkeypatch,
        fetchers,
        {"fetch_salary_data": lambda roles: seen.extend(roles) or {}},
    )
    out = api_enrichment.enrich_data(dict(plan))
    return set(out["enrichment_summary"]["apis_called"]), seen


def _plan(locations: List[str]) -> Dict[str, Any]:
    return {
        "industry": "logistics",
        "target_roles": ["Warehouse Associate"],
        "locations": locations,
    }


@pytest.mark.parametrize(
    "locations",
    [["London, UK"], ["Mumbai, India"], ["London, UK", "Mumbai, India"]],
    ids=["london", "mumbai", "uk-and-india"],
)
def test_f2_bls_is_never_dispatched_on_a_plan_with_no_us_market(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, locations: List[str]
) -> None:
    called, seen_roles = _dispatch(monkeypatch, _plan(locations))
    assert "BLS" not in called
    # nothing fetched, so the resolver's "(US-basis, not locally benchmarked)"
    # label is not overwritten by a plain "BLS OES"
    assert seen_roles == []


@pytest.mark.parametrize(
    "locations",
    [["Denver, CO"], ["New York, NY", "London, UK"], []],
    ids=["us", "mixed", "no-locations"],
)
def test_f2_bls_still_runs_when_the_plan_has_a_us_market(
    monkeypatch: pytest.MonkeyPatch, isolated_breakers, locations: List[str]
) -> None:
    called, seen_roles = _dispatch(monkeypatch, _plan(locations))
    assert "BLS" in called
    assert seen_roles == ["Warehouse Associate"]


# ─── F5: BLS is metered per POST ─────────────────────────────────────────────


@pytest.fixture
def bls_posts(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """BLS key present; the v2 endpoint succeeds. Records every POST URL."""
    monkeypatch.setenv("BLS_API_KEY", "test-key")
    posts: List[str] = []

    def _post(url: str, payload: Any, timeout: int = 12) -> Dict[str, Any]:
        posts.append(url)
        sid = payload["seriesid"][0]
        return {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": [{"seriesID": sid, "data": [{"value": "50000"}]}]},
        }

    monkeypatch.setattr(api_enrichment, "_http_post_json", _post)
    monkeypatch.setattr(api_enrichment, "_get_cached", lambda key: None)
    monkeypatch.setattr(api_enrichment, "_set_cached", lambda key, value: None)
    monkeypatch.setattr(api_enrichment, "_circuit_breaker_check", lambda label: False)
    monkeypatch.setattr(
        api_enrichment, "_circuit_breaker_record_success", lambda label: None
    )
    return posts


def _bls_used() -> int:
    state = api_enrichment._rate_limiter_state.get("BLS") or {}
    return int(state.get("call_count") or 0)


def test_f5_one_dispatch_of_n_roles_charges_n_posts(bls_posts: List[str]) -> None:
    roles = ["Registered Nurse", "Software Engineer", "Truck Driver"]
    _result, status, _meta = api_enrichment._safe_call(
        lambda: api_enrichment.fetch_salary_data(roles), "BLS"
    )

    assert status == "ok"
    assert len(bls_posts) == 3
    assert _bls_used() == 3  # was 1: the dispatch, not the three POSTs


def test_f5_cached_roles_are_not_charged(
    monkeypatch: pytest.MonkeyPatch, bls_posts: List[str]
) -> None:
    monkeypatch.setattr(
        api_enrichment, "_get_cached", lambda key: {"mean": 1, "source": "BLS OES"}
    )
    api_enrichment.fetch_salary_data(["Registered Nurse", "Truck Driver"])
    assert bls_posts == [] and _bls_used() == 0


def test_f5_the_cap_stops_posts_mid_dispatch(
    monkeypatch: pytest.MonkeyPatch, bls_posts: List[str]
) -> None:
    monkeypatch.setitem(
        api_enrichment._API_RATE_LIMITS, "BLS", {"max_calls": 2, "window_seconds": 3600}
    )
    monkeypatch.setitem(
        api_enrichment._API_RATE_LIMITS,
        "BLS_v1",
        {"max_calls": 0, "window_seconds": 3600},
    )
    out = api_enrichment.fetch_salary_data(
        ["Registered Nurse", "Software Engineer", "Truck Driver"]
    )

    assert len(bls_posts) == 2  # the third role's POST was never sent
    assert set(out) == {"Registered Nurse", "Software Engineer"}


def test_f5_a_v2_failure_charges_the_v1_pool_separately(
    monkeypatch: pytest.MonkeyPatch, bls_posts: List[str]
) -> None:
    urls: List[str] = []

    def _post(url: str, payload: Any, timeout: int = 12) -> Optional[Dict[str, Any]]:
        urls.append(url)
        if "/v2/" in url:
            return {"status": "REQUEST_NOT_PROCESSED", "message": ["down"]}
        sid = payload["seriesid"][0]
        return {
            "status": "REQUEST_SUCCEEDED",
            "Results": {"series": [{"seriesID": sid, "data": [{"value": "50000"}]}]},
        }

    monkeypatch.setattr(api_enrichment, "_http_post_json", _post)
    api_enrichment.fetch_salary_data(["Registered Nurse"])

    assert [("/v2/" in u) for u in urls] == [True, False]
    assert api_enrichment._rate_limiter_state["BLS"]["call_count"] == 1
    assert api_enrichment._rate_limiter_state["BLS_v1"]["call_count"] == 1


# ─── F3: the Adzuna quota is shared and durable ──────────────────────────────


class _FakeUpstash:
    """An in-memory Upstash REST endpoint: INCR / DECR / GET / MGET / EXPIRE."""

    def __init__(self) -> None:
        self.store: Dict[str, int] = {}
        self.ttl: Dict[str, int] = {}
        self.log: List[List[str]] = []
        self.fail = False
        self.fail_on: Optional[str] = None  # fail only this command
        self._lock = threading.Lock()

    def execute(self, command: list) -> Any:
        with self._lock:
            self.log.append([str(c) for c in command])
            if self.fail or (self.fail_on and command[0] == self.fail_on):
                return None
            op = command[0]
            if op == "INCR":
                self.store[command[1]] = self.store.get(command[1], 0) + 1
                return self.store[command[1]]
            if op == "DECR":
                self.store[command[1]] = self.store.get(command[1], 0) - 1
                return self.store[command[1]]
            if op == "GET":
                value = self.store.get(command[1])
                return None if value is None else str(value)
            if op == "MGET":
                return [
                    None if k not in self.store else str(self.store[k])
                    for k in command[1:]
                ]
            if op == "EXPIRE":
                self.ttl[command[1]] = int(command[2])
                return 1
            return None


@pytest.fixture
def fake_upstash(monkeypatch: pytest.MonkeyPatch) -> _FakeUpstash:
    """Upstash configured (as in prod), backed by the in-memory fake."""
    fake = _FakeUpstash()
    monkeypatch.setattr(upstash_cache, "_ENABLED", True)
    monkeypatch.setattr(upstash_cache, "_execute", fake.execute)
    return fake


def _new_worker() -> None:
    """A gunicorn worker (re)start: all in-process state is gone."""
    adzuna_quota.reset()


def test_f3_the_daily_cap_is_global_across_workers_and_restarts(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_DAILY_CAP", 6, raising=False)
    granted = 0
    for i in range(12):
        if i % 2:
            _new_worker()  # alternate between "workers" / restart in between
        granted += bool(adzuna_quota.try_acquire())

    assert granted == 6  # per-process counters would have granted all 12 (cap 20)


def test_f3_the_monthly_cap_is_global(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_MONTHLY_CAP", 4, raising=False)
    granted = 0
    for _ in range(10):
        _new_worker()
        granted += bool(adzuna_quota.try_acquire())
    assert granted == 4


def test_f3_global_caps_match_the_brief() -> None:
    assert adzuna_quota.GLOBAL_DAILY_CAP == 200  # ToS 250/day
    assert adzuna_quota.GLOBAL_MONTHLY_CAP == 2300  # ToS 2,500/month
    assert adzuna_quota.GLOBAL_WEEKLY_CAP <= 1000  # ToS 1,000/week


def test_f3_a_rolling_week_is_held_under_the_weekly_cap(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_WEEKLY_CAP", 10, raising=False)
    now = time.time()
    # the six earlier days already spent 9 requests between them
    for i in range(1, 7):
        day = time.strftime("%Y%m%d", time.gmtime(now - i * 86400))
        fake_upstash.store[f"nova:quota:adzuna:d:{day}"] = 3 if i <= 3 else 0
    assert adzuna_quota.try_acquire() is True  # 9 + 1 = 10
    assert adzuna_quota.try_acquire() is False  # would be 11
    today = f"nova:quota:adzuna:d:{time.strftime('%Y%m%d', time.gmtime(now))}"
    assert fake_upstash.store[today] == 1  # the refused request was refunded


def test_f3_keys_are_per_utc_day_and_month_and_expire(
    fake_upstash: _FakeUpstash,
) -> None:
    now = time.time()
    adzuna_quota.try_acquire()

    day_key = f"nova:quota:adzuna:d:{time.strftime('%Y%m%d', time.gmtime(now))}"
    month_key = f"nova:quota:adzuna:m:{time.strftime('%Y%m', time.gmtime(now))}"
    assert fake_upstash.store[day_key] == 1
    assert fake_upstash.store[month_key] == 1
    assert fake_upstash.ttl[day_key] >= 7 * 86400  # kept for the weekly sum
    assert fake_upstash.ttl[month_key] >= 31 * 86400
    # charged with the atomic INCR, never a read-then-write
    assert ["INCR", day_key] in fake_upstash.log
    assert not any(c[0] == "SET" for c in fake_upstash.log)


def test_f3_a_refused_request_leaves_nothing_charged(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_DAILY_CAP", 3, raising=False)
    results = [adzuna_quota.try_acquire() for _ in range(6)]
    assert results == [True, True, True, False, False, False]
    assert sorted(fake_upstash.store.values()) == [3, 3]  # day=3, month=3


def test_f3_monthly_refusal_refunds_the_daily_charge(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_MONTHLY_CAP", 2, raising=False)
    assert [adzuna_quota.try_acquire() for _ in range(3)] == [True, True, False]
    day = [v for k, v in fake_upstash.store.items() if ":d:" in k]
    assert day == [2]


def test_f3_unreachable_upstash_falls_back_to_the_per_worker_caps(
    fake_upstash: _FakeUpstash,
) -> None:
    fake_upstash.fail = True
    granted = [adzuna_quota.try_acquire() for _ in range(adzuna_quota.DAILY_CAP + 5)]

    assert granted.count(True) == adzuna_quota.DAILY_CAP  # 20, the conservative cap
    assert adzuna_quota.exhausted() is True


def test_f3_a_monthly_incr_failure_refunds_the_daily_charge_and_falls_back(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    real = fake_upstash.execute

    def _flaky(command: list) -> Any:
        if command[0] == "INCR" and ":m:" in command[1]:
            return None
        return real(command)

    monkeypatch.setattr(upstash_cache, "_execute", _flaky)
    assert adzuna_quota.try_acquire() is True  # served by the per-worker fallback
    assert [v for k, v in fake_upstash.store.items() if ":d:" in k] == [0]  # refunded


def test_f3_a_failed_weekly_read_does_not_block_the_request(
    fake_upstash: _FakeUpstash,
) -> None:
    fake_upstash.fail_on = "MGET"
    assert adzuna_quota.try_acquire() is True


def test_f3_never_raises_into_a_caller(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    def _boom(command: list) -> Any:
        raise RuntimeError("redis exploded")

    monkeypatch.setattr(upstash_cache, "_execute", _boom)
    assert isinstance(adzuna_quota.try_acquire(), bool)
    assert isinstance(adzuna_quota.exhausted(), bool)
    assert isinstance(adzuna_quota.status(), dict)


def test_f3_concurrent_workers_cannot_both_take_the_last_slot(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_DAILY_CAP", 20, raising=False)
    results: List[bool] = []
    barrier = threading.Barrier(40)

    def _worker() -> None:
        barrier.wait()
        results.append(adzuna_quota.try_acquire())

    threads = [threading.Thread(target=_worker) for _ in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(True) == 20


def test_f3_exhausted_and_status_read_the_shared_counter(
    monkeypatch: pytest.MonkeyPatch, fake_upstash: _FakeUpstash
) -> None:
    monkeypatch.setattr(adzuna_quota, "GLOBAL_DAILY_CAP", 2, raising=False)
    adzuna_quota.try_acquire()
    adzuna_quota.try_acquire()
    _new_worker()  # a worker that has itself sent nothing

    assert adzuna_quota.exhausted() is True
    daily = adzuna_quota.status()["daily"]
    assert daily["calls_used"] == 2 and daily["max_calls"] == 2
    assert daily["scope"] == "global"


def test_f3_without_upstash_the_in_process_counter_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(upstash_cache, "_ENABLED", False)
    granted = [adzuna_quota.try_acquire() for _ in range(adzuna_quota.DAILY_CAP + 3)]
    assert granted.count(True) == adzuna_quota.DAILY_CAP
    assert adzuna_quota.status()["daily"]["scope"] == "worker"


def test_f3_docstring_states_the_real_worker_assumption() -> None:
    doc = adzuna_quota.__doc__ or ""
    assert "Upstash" in doc and "GLOBAL_DAILY_CAP" in doc
    assert "WORKERS_ASSUMED" in doc and "restart" in doc.lower()


# ─── F4: app.py's own fan-out obeys ENRICH_ROLE_SOURCES ──────────────────────


class _Spy:
    """Stands in for one api_integrations client; records the calls made."""

    def __init__(self, returns: Any = None) -> None:
        self.calls: List[tuple] = []
        self._returns = returns

    def __getattr__(self, name: str) -> Any:
        def _call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((name,) + args)
            return self._returns

        return _call


def _fanout(
    monkeypatch: pytest.MonkeyPatch,
    port: int,
    locations: List[str],
    env: Optional[str],
) -> Dict[str, _Spy]:
    """POST /api/generate with the sync fan-out's clients replaced by spies;
    the request stops right after enrichment."""
    if env is None:
        monkeypatch.delenv("ENRICH_ROLE_SOURCES", raising=False)
    else:
        monkeypatch.setenv("ENRICH_ROLE_SOURCES", env)
    spies = {
        "adzuna": _Spy({"count": 5}),
        "bls": _Spy({"employment": 1}),
        "jooble": _Spy({"totalCount": 5}),
        "onet": _Spy([{"code": "15-1252"}]),
    }

    def _stop() -> dict:
        raise RuntimeError("test stop after enrichment")

    monkeypatch.setattr(app, "_api_integrations_available", True)
    monkeypatch.setattr(app, "_api_adzuna", spies["adzuna"])
    monkeypatch.setattr(app, "_api_bls", spies["bls"])
    monkeypatch.setattr(app, "_api_jooble", spies["jooble"])
    monkeypatch.setattr(app, "_api_onet", spies["onet"])
    for other in ("_api_fred", "_api_bea", "_api_census", "_api_usajobs"):
        monkeypatch.setattr(app, other, None)
    monkeypatch.setattr(app, "enrich_data", None)
    monkeypatch.setattr(app, "_jobspy_available", False)
    monkeypatch.setattr(app, "_has_knowledge_graph", False)
    monkeypatch.setattr(app, "_has_google_maps", False)
    monkeypatch.setattr(app, "load_knowledge_base", _stop)
    # By the time the standardizer step runs, the fan-out holds plain role
    # titles, so no SOC is attached and _call_bls / _call_onet's SOC paths
    # never run. Re-wrap them as role dicts (the shape the standardizer
    # attaches a SOC to) so the BLS gate is observable.
    real_canonical = app._canonical_industry_for_request

    def _dict_roles_then_canonical(d: dict) -> str:
        d["target_roles"] = [
            {"title": t} if isinstance(t, str) else t
            for t in (d.get("target_roles") or [])
        ]
        return real_canonical(d)

    monkeypatch.setattr(
        app, "_canonical_industry_for_request", _dict_roles_then_canonical
    )
    app._rate_limit_store.clear()
    with app._rl_generate._lock:
        app._rl_generate._requests.clear()

    payload = {
        "client_name": f"Fanout Gate Test Co {uuid.uuid4().hex[:8]}",  # defeats the 60 s dedupe
        "requester_name": "QA Bot",
        "requester_email": "qa@joveo.com",
        "target_roles": ["Software Engineer"],
        "locations": locations,
        # the wizard sets this; the handler defaults an absent one to us_only
        "target_region": "us_only" if locations == ["Columbus, OH"] else "global",
        "budget": 20000,
        "campaign_duration": "4 weeks",
    }
    assert _wait_for_generate_slots(), "test setup: prior request still owns a slot"
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        conn.request(
            "POST",
            "/api/generate",
            body=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Origin": "http://localhost"},
        )
        assert conn.getresponse().status == 500  # stopped by _stop, after enrichment
    finally:
        conn.close()
    assert _wait_for_generate_slots(), "request leaked a generation slot"
    return spies


def _called(spies: Dict[str, _Spy]) -> set:
    return {name for name, spy in spies.items() if spy.calls}


def test_f4_default_set_runs_adzuna_and_bls_not_jooble_or_onet(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int
) -> None:
    spies = _fanout(monkeypatch, _generate_server, ["Columbus, OH"], None)
    assert _called(spies) == {"adzuna", "bls"}


def test_f4_kill_switch_stops_every_fanout_source(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int
) -> None:
    spies = _fanout(monkeypatch, _generate_server, ["Columbus, OH"], "0")
    assert _called(spies) == set()


def test_f4_an_allowlist_without_adzuna_stops_adzuna(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int
) -> None:
    spies = _fanout(monkeypatch, _generate_server, ["London, UK"], "Jooble,O*NET")
    assert _called(spies) == {"jooble", "onet"}


def test_f4_all_sources_still_available_when_opted_in(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int
) -> None:
    # London: Adzuna runs (gb), Jooble + O*NET run, BLS (US wages) does not.
    spies = _fanout(monkeypatch, _generate_server, ["London, UK"], "all")
    assert _called(spies) == {"adzuna", "jooble", "onet"}


def test_f4_adzuna_is_asked_about_the_plans_own_country(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int
) -> None:
    spies = _fanout(monkeypatch, _generate_server, ["London, UK"], None)
    assert [c[2] for c in spies["adzuna"].calls] == ["gb"]  # was always "us"
    assert spies["bls"].calls == []  # BLS is US data: not on a London plan


def test_f4_adzuna_is_not_called_for_a_market_it_does_not_cover(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int
) -> None:
    spies = _fanout(monkeypatch, _generate_server, ["Dubai, UAE"], None)
    assert spies["adzuna"].calls == []


def test_f4_role_source_gate_helper_follows_the_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ENRICH_ROLE_SOURCES", raising=False)
    assert app._role_source_on("Adzuna") and app._role_source_on("BLS")
    assert not app._role_source_on("Jooble") and not app._role_source_on("O*NET")
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "0")
    assert not any(
        app._role_source_on(label) for label in ("Adzuna", "BLS", "Jooble", "O*NET")
    )
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "jooble")
    assert app._role_source_on("Jooble") and not app._role_source_on("Adzuna")
