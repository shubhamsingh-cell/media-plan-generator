"""Final fix round on the roles deck / role-source work (2026-10-17).

V1  BLS still mapped sales roles to Sales Managers (11-2022) through the
    standardizer fallback, and "Manager" / "Associate" through the
    reverse-substring direction.
V2  app.py's /api/generate fan-out spent Adzuna requests on a result nothing read.
V3  /api/integrations/market-snapshot was an anonymous, ungated Adzuna spender.

No test touches the network: transports are patched and the suite's
external-network guard (tests/conftest.py) is the backstop.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402


def _soc_calls(monkeypatch: pytest.MonkeyPatch, role: str) -> List[str]:
    calls: List[str] = []
    monkeypatch.setattr(
        api_enrichment,
        "_fetch_bls_salary",
        lambda r, soc: calls.append(soc) or {"mean": 1, "source": "BLS OES"},
    )
    api_enrichment.fetch_salary_data([role])
    return calls


# --------------------------------------------------------------------- V1 ---

_SALES_TITLES = [
    "Sales Representative",
    "Sales Rep",
    "Outside Sales Representative",
    "Territory Sales Representative",
    "Account Executive",
    "Business Development Representative",
]


@pytest.mark.parametrize("role", _SALES_TITLES)
def test_v1_sales_roles_never_resolve_to_sales_managers(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    calls = _soc_calls(monkeypatch, role)
    assert "11-2022" not in calls, calls


@pytest.mark.parametrize(
    "role,soc",
    [
        ("Sales Representative", "41-4012"),
        ("Sales Rep", "41-4012"),
        ("Outside Sales Representative", "41-4012"),
        ("Territory Sales Representative", "41-4012"),
        ("Account Executive", "41-3091"),
        ("Business Development Representative", "41-3091"),
    ],
)
def test_v1_sales_roles_map_to_their_own_occupation(
    monkeypatch: pytest.MonkeyPatch, role: str, soc: str
) -> None:
    assert _soc_calls(monkeypatch, role) == [soc]


@pytest.mark.parametrize("role", ["Manager", "Associate", "manager", "ASSOCIATE"])
def test_v1_a_bare_generic_word_is_not_a_title(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    # "manager" used to hit "product manager" (11-2021) and "associate" hit
    # "warehouse associate" (53-7065) through the `role in title` direction.
    assert _soc_calls(monkeypatch, role) == []


@pytest.mark.parametrize("role", ["Business Development", "BDR", "SDR"])
def test_v1_no_confident_soc_means_no_standardizer_guess(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    # The standardizer's alias table maps these to Sales Managers (11-2022);
    # a role only it knows must not become a BLS wage.
    assert _soc_calls(monkeypatch, role) == []


def test_v1_longer_titles_still_match_by_known_title_substring(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _soc_calls(monkeypatch, "Senior Software Engineer") == ["15-1252"]
    assert _soc_calls(monkeypatch, "Regional Sales Manager") == ["11-2022"]


# Occupation-changing qualifiers: a longer role that merely CONTAINS a known
# title ("Nurse" in "Licensed Practical Nurse") is a different occupation and
# must not take the contained title's BLS wage. Codes below were read on
# https://www.bls.gov/oes/2025/may/oes_stru.htm.
@pytest.mark.parametrize(
    "role,soc",
    [
        ("Nurse Practitioner", "29-1171"),
        ("Senior Nurse Practitioner", "29-1171"),
        ("Licensed Practical Nurse", "29-2061"),
        ("LPN", "29-2061"),
        ("Licensed Vocational Nurse", "29-2061"),
        ("LVN", "29-2061"),
        ("Nurse Manager", "11-9111"),
        ("Registered Nurse Manager", "11-9111"),
        ("Physical Therapist Assistant", "31-2021"),
    ],
)
def test_v1b_qualified_nursing_and_therapy_titles_map_to_their_own_soc(
    monkeypatch: pytest.MonkeyPatch, role: str, soc: str
) -> None:
    assert _soc_calls(monkeypatch, role) == [soc]


@pytest.mark.parametrize(
    "role",
    [
        "Teacher Assistant",
        "Software Engineer Manager",
        "Data Analyst Intern",
        "Electrician Apprentice",
        "Truck Driver Trainer",
        "Registered Nurse Supervisor",
        "Software Engineering Intern",
        "Pharmacist Technician",
        "Nurse Aide",
        "Physical Therapist Aide",
    ],
)
def test_v1b_a_qualifier_that_changes_the_occupation_means_no_bls_wage(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    assert _soc_calls(monkeypatch, role) == []


@pytest.mark.parametrize(
    "role,soc",
    [
        ("Travel Nurse", "29-1141"),
        ("Senior Software Engineer", "15-1252"),
        ("Retail Sales Associate", "41-2031"),
        ("Regional Sales Manager", "11-2022"),
        ("Physician Assistant", "29-1071"),
    ],
)
def test_v1b_seniority_and_setting_modifiers_keep_their_mapping(
    monkeypatch: pytest.MonkeyPatch, role: str, soc: str
) -> None:
    assert _soc_calls(monkeypatch, role) == [soc]


# --------------------------------------------------------------------- V2 ---

from tests.test_roles_audit_fixes import (  # noqa: E402,F401
    _fanout,
    _generate_server,
    isolated_breakers,
)


@pytest.mark.parametrize(
    "locations,env",
    [
        (["Columbus, OH"], None),
        (["London, UK"], None),
        (["Columbus, OH"], "all"),
        (["London, UK"], "Adzuna"),
    ],
    ids=["us-default", "uk-default", "us-all", "uk-adzuna-only"],
)
def test_v2_generate_fanout_makes_no_adzuna_call(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int, locations, env
) -> None:
    # The fan-out's Adzuna result was read by nothing but a PostHog event and
    # each plan spent up to 5 requests of the shared 200/day cap on it.
    spies = _fanout(monkeypatch, _generate_server, locations, env)
    assert spies["adzuna"].calls == [], spies["adzuna"].calls


# --------------------------------------------------------------------- V3 ---

import http.client  # noqa: E402
import json  # noqa: E402
import threading  # noqa: E402

import app  # noqa: E402
from tests.test_roles_audit_fixes import _Spy  # noqa: E402


def _snapshot(port: int, query: str):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        conn.request("GET", f"/api/integrations/market-snapshot?{query}")
        resp = conn.getresponse()
        raw = resp.read()
    finally:
        conn.close()
    try:
        return resp.status, json.loads(raw)
    except ValueError:
        return resp.status, {}


def _reset_limiters() -> None:
    app._rate_limit_store.clear()
    limiter = getattr(app, "_rl_snapshot", None)  # absent before the V3 fix
    if limiter is not None:
        with limiter._lock:
            limiter._requests.clear()


@pytest.fixture
def snapshot_env(monkeypatch: pytest.MonkeyPatch):
    """Spies for every client the snapshot touches; fresh rate-limit state."""
    spies = {
        "adzuna": _Spy({"count": 5}),
        "bls": _Spy(None),
        "fred": _Spy(None),
        "onet": _Spy(None),
    }
    monkeypatch.setattr(app, "_api_integrations_available", True)
    monkeypatch.setattr(app, "_api_adzuna", spies["adzuna"])
    monkeypatch.setattr(app, "_api_bls", spies["bls"])
    monkeypatch.setattr(app, "_api_fred", spies["fred"])
    monkeypatch.setattr(app, "_api_onet", spies["onet"])
    for other in ("_api_bea", "_api_census", "_api_usajobs", "_api_jooble"):
        monkeypatch.setattr(app, other, None, raising=False)
    monkeypatch.delenv("ENRICH_ROLE_SOURCES", raising=False)
    _reset_limiters()
    yield spies
    _reset_limiters()


def test_v3_snapshot_spends_no_adzuna_request_when_adzuna_is_not_an_enabled_source(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int, snapshot_env
) -> None:
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "0")
    status, body = _snapshot(_generate_server, "role=Nurse")
    assert status == 200
    assert snapshot_env["adzuna"].calls == []
    assert "adzuna_job_count" not in body
    assert body.get("adzuna_unavailable_reason")


def test_v3_snapshot_is_rate_limited_per_ip(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int, snapshot_env
) -> None:
    monkeypatch.setattr(app, "_RATE_LIMIT_MAX", 3)
    statuses = [_snapshot(_generate_server, f"role=Role{i}")[0] for i in range(6)]
    assert statuses[:3] == [200, 200, 200], statuses
    assert set(statuses[3:]) == {429}, statuses
    # nothing past the cap reached a provider: 3 roles x 3 Adzuna requests
    assert len(snapshot_env["adzuna"].calls) <= 9


def test_v3_adzuna_spend_has_its_own_tighter_per_ip_cap(
    _generate_server: int, snapshot_env
) -> None:
    # 5 Adzuna snapshots per IP per minute, however generous the general cap.
    for i in range(8):
        _snapshot(_generate_server, f"role=Cap{i}")
    spent_roles = {c[1] for c in snapshot_env["adzuna"].calls}
    assert len(spent_roles) == 5, spent_roles


def test_v3_snapshot_follows_the_location_country_not_a_hardcoded_us(
    _generate_server: int, snapshot_env
) -> None:
    status, body = _snapshot(_generate_server, "role=Nurse&location=London%2C+UK")
    assert status == 200
    countries = {c[2] for c in snapshot_env["adzuna"].calls}
    assert countries == {"gb"}, countries
    assert body["adzuna_scope"] == {
        "country": "gb",
        "kind": "national",
        "label": "GB nationwide",
    }


def test_v3_no_location_is_national_us_and_says_so(
    _generate_server: int, snapshot_env
) -> None:
    status, body = _snapshot(_generate_server, "role=Nurse")
    assert status == 200
    assert {c[2] for c in snapshot_env["adzuna"].calls} == {"us"}
    assert body["adzuna_scope"]["label"] == "US nationwide"


def test_v3_unsupported_location_sends_no_adzuna_request(
    _generate_server: int, snapshot_env
) -> None:
    status, body = _snapshot(_generate_server, "role=Nurse&location=Dubai")
    assert status == 200
    assert snapshot_env["adzuna"].calls == []
    assert body.get("adzuna_unavailable_reason")
