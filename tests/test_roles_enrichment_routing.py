"""Regression coverage for wizard roles reaching bounded enrichment.

The /api/generate sync handler and its X-Async worker pass their request dict
straight to api_enrichment.enrich_data().  Wizard payloads use
``target_roles``; legacy callers use ``roles`` or ``job_titles``.  With
ENRICH_ROLE_SOURCES on (the module fixture below), all of them reach the role
collectors.  The default-off gate and the request-deadline mechanism have their
own suites (test_enrich_role_sources_gate.py, test_enrich_deadline_cancels.py).
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


@pytest.fixture(autouse=True)
def _role_sources_on(monkeypatch: pytest.MonkeyPatch) -> None:
    """target_roles is only aliased to roles when ENRICH_ROLE_SOURCES opts in."""
    monkeypatch.setenv(api_enrichment.ROLE_SOURCES_ENV, "1")


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
@pytest.mark.parametrize("role_key", ["target_roles", "roles"])
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
