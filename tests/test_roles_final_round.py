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
    # "warehouse associate" (53-7062) through the `role in title` direction.
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


# Fail-closed role -> SOC: a known title is taken from a longer role only when
# every OTHER word is an allowlisted seniority/schedule/setting modifier. The
# verifier's 174-title probe found these wrong under the qualifier blocklist.
# Codes read on https://www.bls.gov/oes/2025/may/oes_stru.htm.
@pytest.mark.parametrize(
    "role",
    [
        # nursing
        "LPN Charge Nurse",
        "Nursery Associate",
        "Nurse Educator",
        "Nurse Aide",
        # trades helpers / apprentices are not journey-level
        "Electrician Helper",
        "Plumber Helper",
        "Carpenter Helper",
        "Plumber Apprentice",
        # "server" is a waiter only as a bare title
        "Server Administrator",
        "Linux Server Administrator",
        "SQL Server DBA",
        # "security officer" is a guard only as a bare title
        "Information Security Officer",
        # recruiters are not software developers
        "Software Engineering Recruiter",
        "Technical Recruiter - Software Engineering",
        "Pharmaceutical Sales Representative",
        "Senior Sales Representative",
    ],
)
def test_v1c_unlisted_modifier_means_no_bls_wage(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    assert _soc_calls(monkeypatch, role) == []


@pytest.mark.parametrize(
    "role,soc",
    [
        ("CRNA", "29-1151"),
        ("Nurse Anesthetist", "29-1151"),
        ("Certified Registered Nurse Anesthetist", "29-1151"),
        ("Travel CRNA", "29-1151"),
        ("Nurse Midwife", "29-1161"),
        ("Certified Nurse Midwife", "29-1161"),
        ("Information Security Analyst", "15-1212"),
    ],
)
def test_v1c_anesthetist_midwife_and_security_analyst_have_their_own_soc(
    monkeypatch: pytest.MonkeyPatch, role: str, soc: str
) -> None:
    assert _soc_calls(monkeypatch, role) == [soc]


@pytest.mark.parametrize(
    "role,soc",
    [
        ("Travel Nurse", "29-1141"),
        ("Per Diem Registered Nurse", "29-1141"),
        ("ICU Registered Nurse", "29-1141"),
        ("Med Surg Nurse", "29-1141"),
        ("Labor and Delivery Nurse", "29-1141"),
        ("Night Shift Registered Nurse", "29-1141"),
        ("Registered Nurse Manager", "11-9111"),
        ("Senior Software Engineer", "15-1252"),
        ("Sr. Software Engineer II", "15-1252"),
        ("Remote Software Engineer", "15-1252"),
        ("Retail Sales Associate", "41-2031"),
        # O*NET lists Warehouse Associate under 53-7062.00; 53-7065 is stocker/order filler
        ("Night Shift Warehouse Associate", "53-7062"),
        ("Part Time Cashier", "41-2011"),
        ("CDL A Truck Driver", "53-3032"),
        ("OTR Truck Driver", "53-3032"),
        ("Regional Sales Manager", "11-2022"),
        ("Physician Assistant", "29-1071"),
        ("Licensed Practical Nurse (LPN)", "29-2061"),
    ],
)
def test_v1c_allowlisted_modifiers_keep_their_mapping(
    monkeypatch: pytest.MonkeyPatch, role: str, soc: str
) -> None:
    assert _soc_calls(monkeypatch, role) == [soc]


def test_v1c_specialty_words_are_scoped_to_nursing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # "care", "labor" and "school" whitelist nothing outside the nurse SOC.
    assert _soc_calls(monkeypatch, "Care Software Engineer") == []
    assert _soc_calls(monkeypatch, "Labor Electrician") == []
    assert _soc_calls(monkeypatch, "School Cook") == []
    # driver licence words are scoped to driver SOCs
    assert _soc_calls(monkeypatch, "CDL Software Engineer") == []


def test_v1c_the_longest_title_wins_and_disagreeing_ties_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # "nurse practitioner" (2 words) beats "nurse" (1 word)
    assert _soc_calls(monkeypatch, "Senior Nurse Practitioner") == ["29-1171"]
    monkeypatch.setitem(api_enrichment.SOC_CODES, "zz alpha", "11-1111")
    monkeypatch.setitem(api_enrichment.SOC_CODES, "alpha zz", "22-2222")
    assert _soc_calls(monkeypatch, "zz alpha zz") == []


# --------------------------------------------------------------------- V2 ---


@pytest.mark.parametrize(
    "role",
    [
        "Physician",
        "UX Designer",
        "Senior UX Designer",
        "Account Manager",
        "Data Analyst",
        "Senior Data Analyst",
        "DevOps Engineer",
        "Site Reliability Engineer",
        "Remote Cloud Engineer",
    ],
)
def test_v1d_titles_mapped_to_a_neighbouring_occupation_get_no_bls_wage(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    # Physician -> OB/GYN, UX Designer -> Graphic Designers, Account Manager
    # -> Sales Managers, Data Analyst -> Data Scientists, DevOps/SRE/Cloud ->
    # Systems Administrators: a neighbour's wage would blend into the client
    # salary, so these keep the benchmark instead.
    assert _soc_calls(monkeypatch, role) == []


@pytest.mark.parametrize(
    "role", ["RN", "Travel RN", "ICU RN", "Charge Nurse", "Night Shift Charge Nurse"]
)
def test_v1d_rn_and_charge_nurse_are_registered_nurses(
    monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    assert _soc_calls(monkeypatch, role) == ["29-1141"]


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
    for name in ("_rl_snapshot", "_rl_snapshot_daily"):
        limiter = getattr(app, name, None)  # absent before the V3 fixes
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


def test_v3_adzuna_spend_has_a_per_ip_daily_cap(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int, snapshot_env
) -> None:
    # One IP must not drain the 200/day shared Adzuna cap: 20 snapshots a day.
    assert app._SNAPSHOT_DAILY_MAX == 20
    monkeypatch.setattr(app, "_SNAPSHOT_DAILY_MAX", 3)
    bodies = []
    for i in range(5):
        _reset_minute_window()
        status, body = _snapshot(_generate_server, f"role=Day{i}")
        assert status == 200
        bodies.append(body)
    spent_roles = {c[1] for c in snapshot_env["adzuna"].calls}
    assert len(spent_roles) == 3, spent_roles
    assert "adzuna_job_count" in bodies[2]
    assert "adzuna_job_count" not in bodies[3]
    assert "daily limit" in bodies[3]["adzuna_unavailable_reason"]


def _reset_minute_window() -> None:
    with app._rl_snapshot._lock:
        app._rl_snapshot._requests.clear()


def test_v3_daily_cap_is_a_rolling_24h_window(
    monkeypatch: pytest.MonkeyPatch, _generate_server: int, snapshot_env
) -> None:
    import time as _time

    now = _time.time()
    with app._rl_snapshot_daily._lock:
        # 20 spends two hours ago still count; the same 20 from 25 h ago do not
        app._rl_snapshot_daily._requests["127.0.0.1"] = [now - 7200] * 20
    status, body = _snapshot(_generate_server, "role=Rolling1")
    assert status == 200 and "adzuna_job_count" not in body
    assert "daily limit" in body["adzuna_unavailable_reason"]
    with app._rl_snapshot_daily._lock:
        app._rl_snapshot_daily._requests["127.0.0.1"] = [now - 25 * 3600] * 20
    _reset_minute_window()
    status, body = _snapshot(_generate_server, "role=Rolling2")
    assert status == 200 and "adzuna_job_count" in body
