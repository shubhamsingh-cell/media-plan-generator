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
