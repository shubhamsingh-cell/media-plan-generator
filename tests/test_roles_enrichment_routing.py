"""Regression coverage for wizard roles reaching bounded enrichment.

The /api/generate sync handler and its X-Async worker pass their request dict
straight to api_enrichment.enrich_data().  Wizard payloads use
``target_roles``; legacy callers use ``roles`` or ``job_titles``.  Keep all
three forms equivalent, and do not let executor-queued collectors begin after
the caller's monotonic deadline.
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from typing import Any, Iterator

import pytest

import api_enrichment
import app


_AUTH_HEADERS = {"Content-Type": "application/json", "Origin": "http://localhost"}
_WIZARD_PAYLOAD = {
    "client_name": "Role Routing Test Co",
    "requester_name": "QA Bot",
    "requester_email": "qa@joveo.com",
    "target_roles": ["Revenue Operations Manager"],
    "locations": ["Columbus, OH"],
    "budget": 20000,
    "campaign_duration": "4 weeks",
}


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def _generate_server() -> Iterator[int]:
    port = _free_port()
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield port
    server.shutdown()
    server.server_close()


def _post_generate(
    port: int, is_async: bool, role_key: str = "target_roles"
) -> tuple[int, dict[str, Any]]:
    headers = dict(_AUTH_HEADERS)
    if is_async:
        headers["X-Async"] = "true"
    payload = dict(_WIZARD_PAYLOAD)
    payload["client_name"] = (
        f"{payload['client_name']} ({'async' if is_async else 'sync'})"
    )
    payload[role_key] = payload.pop("target_roles")
    body = json.dumps(payload).encode("utf-8")
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        connection.request("POST", "/api/generate", body=body, headers=headers)
        response = connection.getresponse()
        raw = response.read()
    finally:
        connection.close()
    try:
        parsed = json.loads(raw)
    except ValueError:
        parsed = {}
    return response.status, parsed


def _wait_for_generate_slots(timeout: float = 3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        acquired = []
        for _ in range(app._MAX_CONCURRENT_GENERATE):
            if not app._generate_slots.acquire(blocking=False):
                break
            acquired.append(True)
        for _ in acquired:
            app._generate_slots.release()
        if len(acquired) == app._MAX_CONCURRENT_GENERATE:
            return True
        time.sleep(0.02)
    return False


@pytest.fixture(autouse=True)
def _disable_geopolitical_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")


@pytest.fixture
def _isolated_call_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_enrichment, "_circuit_breaker_check", lambda label: False)
    monkeypatch.setattr(api_enrichment, "_rate_limit_check", lambda label: False)
    monkeypatch.setattr(
        api_enrichment, "_circuit_breaker_record_success", lambda label: None
    )
    monkeypatch.setattr(api_enrichment, "_rate_limit_record", lambda label: None)


def test_wizard_target_roles_reach_role_collector(
    monkeypatch: pytest.MonkeyPatch, _isolated_call_state: None
) -> None:
    """A wizard-style target_roles payload reaches a real role collector.

    ``_safe_call`` keeps every unrelated external boundary mocked while the
    BLS collector itself runs and records the role supplied by enrich_data.
    This is the dict shape both /api/generate paths hand to enrich_data.
    """
    seen_roles: list[str] = []

    def _salary(roles: list[str]) -> dict[str, Any]:
        seen_roles.extend(roles)
        return {"role": roles[0]}

    def _safe_call(func: Any, label: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
        if label == "BLS":
            return (
                func(),
                "ok",
                {"elapsed_time": 0.0, "source": "test", "success": True},
            )
        return {}, "empty", {"elapsed_time": 0.0, "source": "test", "success": False}

    monkeypatch.setattr(api_enrichment, "fetch_salary_data", _salary)
    monkeypatch.setattr(api_enrichment, "_safe_call", _safe_call)

    result = api_enrichment.enrich_data(
        {"target_roles": ["Revenue Operations Manager"]}
    )

    assert seen_roles == ["Revenue Operations Manager"]
    assert result["salary_data"] == {"role": "Revenue Operations Manager"}


@pytest.mark.parametrize("role_key", ["roles", "job_titles"])
def test_legacy_role_aliases_still_reach_role_collector(
    monkeypatch: pytest.MonkeyPatch,
    _isolated_call_state: None,
    role_key: str,
) -> None:
    seen_roles: list[str] = []

    def _salary(roles: list[str]) -> dict[str, Any]:
        seen_roles.extend(roles)
        return {}

    def _safe_call(func: Any, label: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
        if label == "BLS":
            return (
                func(),
                "ok",
                {
                    "elapsed_time": 0.0,
                    "source": "test",
                    "success": True,
                },
            )
        return {}, "empty", {"elapsed_time": 0.0, "source": "test", "success": False}

    monkeypatch.setattr(api_enrichment, "fetch_salary_data", _salary)
    monkeypatch.setattr(api_enrichment, "_safe_call", _safe_call)

    api_enrichment.enrich_data({role_key: ["Revenue Operations Manager"]})

    assert seen_roles == ["Revenue Operations Manager"]


def test_async_generate_helper_forwards_target_roles_and_deadline() -> None:
    """The X-Async /api/generate path passes the wizard request and deadline."""
    seen: dict[str, Any] = {}

    def _enrich(
        data: dict,
        partial_result: dict,
        deadline: float,
        request_id: str = "",
    ) -> dict:
        seen.update(
            {
                "roles": data["target_roles"],
                "partial": partial_result,
                "deadline": deadline,
                "request_id": request_id,
            }
        )
        return {"enrichment_summary": {"apis_succeeded": []}}

    result, timed_out = app._run_enrich_data_with_partial_fallback(
        _enrich,
        {"target_roles": ["Revenue Operations Manager"]},
        "request-123",
        1.0,
    )

    assert timed_out is False
    assert result == {"enrichment_summary": {"apis_succeeded": []}}
    assert seen["roles"] == ["Revenue Operations Manager"]
    assert seen["partial"] is not None
    assert seen["deadline"] > time.monotonic()
    assert seen["request_id"] == "request-123"


def test_sync_generate_pool_preserves_target_role_partial_result(
    monkeypatch: pytest.MonkeyPatch, _isolated_call_state: None
) -> None:
    """The sync /api/generate pool retains target-role enrichment output."""
    seen_roles: list[str] = []

    def _salary(roles: list[str]) -> dict[str, Any]:
        seen_roles.extend(roles)
        return {"role": roles[0]}

    def _safe_call(func: Any, label: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
        if label == "BLS":
            return (
                func(),
                "ok",
                {
                    "elapsed_time": 0.0,
                    "source": "test",
                    "success": True,
                },
            )
        return {}, "empty", {"elapsed_time": 0.0, "source": "test", "success": False}

    monkeypatch.setattr(api_enrichment, "fetch_salary_data", _salary)
    monkeypatch.setattr(api_enrichment, "_safe_call", _safe_call)
    partial: dict = {}

    def _call_enrich_data() -> tuple[str, dict]:
        return (
            "api_enrichment",
            api_enrichment.enrich_data(
                {"target_roles": ["Revenue Operations Manager"]},
                partial_result=partial,
                deadline=time.monotonic() + 1.0,
            ),
        )

    enriched, market_context = app._run_parallel_enrichment_tasks(
        [_call_enrich_data], 1.0, partial
    )

    assert seen_roles == ["Revenue Operations Manager"]
    assert enriched["salary_data"] == {"role": "Revenue Operations Manager"}
    assert market_context == {}


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("role_key", ["target_roles", "roles", "job_titles"])
def test_http_generate_routes_wizard_target_roles_to_bounded_role_collector(
    monkeypatch: pytest.MonkeyPatch,
    _isolated_call_state: None,
    _generate_server: int,
    is_async: bool,
    role_key: str,
) -> None:
    """The real /api/generate handler reaches BLS in both request modes.

    Every external source boundary is mocked.  The request reaches the real
    ``enrich_data`` function and real BLS collector; the subsequent KB stage
    deliberately fails so document rendering cannot hide this routing test.
    """
    observed: dict[str, Any] = {}
    reached_collector = threading.Event()

    def _salary(roles: list[str]) -> dict[str, Any]:
        observed["roles"] = list(roles)
        reached_collector.set()
        return {"role": roles[0]}

    def _safe_call(func: Any, label: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
        if label == "BLS":
            return (
                func(),
                "ok",
                {
                    "elapsed_time": 0.0,
                    "source": "test",
                    "success": True,
                },
            )
        return {}, "empty", {"elapsed_time": 0.0, "source": "test", "success": False}

    def _stop_after_enrichment() -> dict:
        raise RuntimeError("test stop after enrichment")

    monkeypatch.setattr(api_enrichment, "fetch_salary_data", _salary)
    monkeypatch.setattr(api_enrichment, "_safe_call", _safe_call)
    monkeypatch.setattr(app, "load_knowledge_base", _stop_after_enrichment)
    monkeypatch.setattr(app, "_api_integrations_available", False)
    monkeypatch.setattr(app, "_jobspy_available", False)
    monkeypatch.setattr(app, "_has_knowledge_graph", False)
    monkeypatch.setattr(app, "_has_google_maps", False)
    app._rate_limit_store.clear()
    with app._rl_generate._lock:
        app._rl_generate._requests.clear()

    def _trace(frame: Any, event: str, arg: Any) -> Any:
        if (
            event == "line"
            and frame.f_code.co_name == "enrich_data"
            and frame.f_globals.get("__name__") == "api_enrichment"
        ):
            observed["deadline"] = frame.f_locals.get("deadline")
        return _trace

    threading.settrace(_trace)
    try:
        assert _wait_for_generate_slots(), "test setup: prior request still owns a slot"
        status, response = _post_generate(_generate_server, is_async, role_key)
        if is_async:
            assert status == 200
            job_id = response["job_id"]
            deadline = time.time() + 10
            while time.time() < deadline:
                with app._generation_jobs_lock:
                    job = dict(app._generation_jobs.get(job_id) or {})
                if job.get("status") in {"completed", "failed"}:
                    break
                time.sleep(0.02)
            assert job.get("status") == "failed"
            assert "test stop after enrichment" in (job.get("error") or "")
        else:
            assert status == 500
    finally:
        threading.settrace(None)

    assert reached_collector.wait(timeout=1), "BLS collector was never reached"
    assert observed["roles"] == ["Revenue Operations Manager"]
    assert observed["deadline"] is not None
    assert _wait_for_generate_slots(), "request leaked a generation slot"


def test_expired_deadline_never_enters_geopolitical_context_after_semaphore_wait(
    monkeypatch: pytest.MonkeyPatch, _isolated_call_state: None
) -> None:
    """A late semaphore slot must not start geo or collector work."""
    held_slots = []
    for _ in range(3):
        assert api_enrichment._enrichment_semaphore.acquire(blocking=False)
        held_slots.append(True)

    geo_started = threading.Event()
    errors: list[BaseException] = []

    def _geo(*args: Any, **kwargs: Any) -> dict[str, Any]:
        geo_started.set()
        return {"future": None, "executor": None, "status": "disabled"}

    monkeypatch.setattr(api_enrichment, "_start_geopolitical_context", _geo)
    monkeypatch.setattr(
        api_enrichment,
        "_safe_call",
        lambda func, label: (
            {},
            "empty",
            {"elapsed_time": 0.0, "source": "test", "success": False},
        ),
    )
    deadline = time.monotonic() + 0.1

    def _run() -> None:
        try:
            api_enrichment.enrich_data(
                {"target_roles": ["Revenue Operations Manager"]}, deadline=deadline
            )
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=_run, daemon=True)
    runner.start()
    time.sleep(0.2)
    api_enrichment._enrichment_semaphore.release()
    held_slots.pop()
    runner.join(timeout=2)
    for _ in held_slots:
        api_enrichment._enrichment_semaphore.release()

    assert not runner.is_alive()
    assert errors == []
    assert not geo_started.is_set()


def test_deadline_prevents_queued_role_collectors_from_starting(
    monkeypatch: pytest.MonkeyPatch, _isolated_call_state: None
) -> None:
    """Queued role collectors do not begin external work after deadline.

    Five controlled first-wave calls occupy enrich_data's five workers.  The
    GoogleAds role collector is queued behind them.  Once the caller deadline
    expires, releasing the first wave must not let that queued collector call
    its external boundary; already completed partial results remain available.
    """
    first_wave = {"BLS", "CurrencyRates", "FRED", "GoogleTrends", "O*NET"}
    started: list[str] = []
    started_lock = threading.Lock()
    first_wave_started = threading.Event()
    gate = threading.Event()
    errors: list[BaseException] = []
    partial: dict = {}
    outcome: dict = {}

    def _safe_call(func: Any, label: str) -> tuple[dict[str, Any], str, dict[str, Any]]:
        with started_lock:
            started.append(label)
            if first_wave.issubset(started):
                first_wave_started.set()
        if label in first_wave:
            gate.wait(timeout=2)
            if label == "BLS":
                return (
                    {"completed": True},
                    "ok",
                    {
                        "elapsed_time": 0.0,
                        "source": "test",
                        "success": True,
                    },
                )
        return {}, "empty", {"elapsed_time": 0.0, "source": "test", "success": False}

    monkeypatch.setattr(api_enrichment, "_safe_call", _safe_call)

    deadline = time.monotonic() + 0.1

    def _run_enrichment() -> None:
        try:
            outcome["result"] = api_enrichment.enrich_data(
                {"target_roles": ["Revenue Operations Manager"]},
                deadline=deadline,
                partial_result=partial,
            )
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=_run_enrichment, daemon=True)
    runner.start()
    assert first_wave_started.wait(timeout=1), "test setup: worker pool did not fill"
    time.sleep(0.2)
    gate.set()
    runner.join(timeout=2)

    assert not runner.is_alive()
    assert errors == []
    assert outcome["result"] is partial
    assert partial["salary_data"] == {"completed": True}
    assert partial["google_ads_data"] == {}
    assert "GoogleAds" not in started


def test_parallel_pool_deadline_skips_ninth_queued_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A freed worker cannot start a queued callable after the shared deadline."""
    clock = {"now": 0.0}
    monkeypatch.setattr(app.time, "monotonic", lambda: clock["now"])
    gate = threading.Event()
    first_wave_started = threading.Event()
    first_wave_count = {"value": 0}
    first_wave_lock = threading.Lock()
    ninth_called = threading.Event()
    errors: list[BaseException] = []

    def _blocked_task() -> tuple[str, None]:
        with first_wave_lock:
            first_wave_count["value"] += 1
            if first_wave_count["value"] == 8:
                first_wave_started.set()
        gate.wait(timeout=2)
        return ("blocked", None)

    def _ninth_task() -> tuple[str, dict]:
        ninth_called.set()
        return ("ninth", {"unexpected": True})

    result: dict = {}

    def _run() -> None:
        try:
            result["value"] = app._run_parallel_enrichment_tasks(
                [_blocked_task] * 8 + [_ninth_task],
                timeout=1.0,
                enrich_data_partial={},
                deadline=1.0,
            )
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=_run, daemon=True)
    runner.start()
    assert first_wave_started.wait(timeout=1), "test setup: eight workers did not start"
    clock["now"] = 2.0
    gate.set()
    runner.join(timeout=2)

    assert not runner.is_alive()
    assert errors == []
    assert not ninth_called.is_set()
    assert result["value"][1] == {}
