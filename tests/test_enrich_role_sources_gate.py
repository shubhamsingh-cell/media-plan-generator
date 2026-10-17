"""The wizard's ``target_roles`` reach enrich_data() through the
ENRICH_ROLE_SOURCES allowlist (2026-10-16, allowlist semantics 2026-10-17).

enrich_data() reads ``roles`` / ``job_titles``; the wizard and both
/api/generate paths send ``target_roles``, so the 17 role-based sources
(BLS, O*NET, Adzuna, the ad platforms, ...) never ran for a wizard plan.
Only BLS and Adzuna are on by default now (unset == that pair); "0"/"off"
is the kill switch and "1"/"all" the old all-17 opt-in. The per-label
semantics (comma lists, unknown labels, quota) live in
test_role_source_allowlist.py; this file keeps the all-17 and kill-switch
behaviour of the original gate.

Every source is stubbed to {} (no network); dispatch is read from the
summary the real orchestrator publishes.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_enrichment  # noqa: E402
from tests.test_enrich_geopolitical_budget import (  # noqa: E402,F401
    isolated_breakers,
    stub_sources,
)

_WIZARD = {
    "client_name": "Acme Health",
    "industry": "healthcare_medical",
    "target_roles": ["Registered Nurse", "Medical Assistant"],
    "locations": ["Denver, CO"],
}
# Spelled out (not read from the module) so an opt-in test can never pass
# vacuously against an empty declaration.
_ALL_ROLE_SOURCES = {
    "BLS",
    "GoogleTrends",
    "O*NET",
    "DataUSA-Occ",
    "GoogleAds",
    "MetaAds",
    "BingAds",
    "TikTokAds",
    "LinkedInAds",
    "CareerOneStop",
    "COS-Projections",
    "H1B-Wages",
    "H1B-Salary-Intel",
    "Adzuna",
    "Jooble",
    "RegionalLabour",
    "CareerJet",
}


def _dispatched(monkeypatch: pytest.MonkeyPatch, request: Dict[str, Any]) -> List[str]:
    monkeypatch.setenv("NOVA_GEOPOLITICAL_TIMEOUT_S", "0")
    fetchers = [
        n
        for n in dir(api_enrichment)
        if (n.startswith("fetch_") or n == "_fetch_h1b_salary_intelligence")
        and callable(getattr(api_enrichment, n))
    ]
    stub_sources(monkeypatch, fetchers)
    result = api_enrichment.enrich_data(dict(request))
    return list(result["enrichment_summary"]["apis_called"])


def test_seventeen_role_sources_are_declared():
    assert len(_ALL_ROLE_SOURCES) == 17
    assert set(api_enrichment.role_sources_for(True)) == _ALL_ROLE_SOURCES


@pytest.mark.parametrize("value", ["0", "off"])
def test_kill_switch_runs_no_role_source_and_logs_what_would_have_run(
    monkeypatch, isolated_breakers, caplog, value
):
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", value)
    with caplog.at_level(logging.INFO, logger="api_enrichment"):
        called = _dispatched(monkeypatch, _WIZARD)
    assert not _ALL_ROLE_SOURCES & set(called), called
    lines = [
        r.getMessage()
        for r in caplog.records
        if "ENRICH_ROLE_SOURCES" in r.getMessage()
    ]
    assert len(lines) == 1, lines
    for label in _ALL_ROLE_SOURCES:
        assert label in lines[0], (label, lines[0])


def test_unset_runs_only_the_default_pair(monkeypatch, isolated_breakers):
    """Was ``test_default_off_runs_no_role_source...``: unset used to mean off."""
    monkeypatch.delenv("ENRICH_ROLE_SOURCES", raising=False)
    called = set(_dispatched(monkeypatch, _WIZARD))
    assert _ALL_ROLE_SOURCES & called == {"BLS", "Adzuna"}


@pytest.mark.parametrize("value", ["1", "true", "YES", "on"])
def test_opt_in_aliases_target_roles_to_roles(monkeypatch, isolated_breakers, value):
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", value)
    called = set(_dispatched(monkeypatch, _WIZARD))
    assert _ALL_ROLE_SOURCES <= called, sorted(_ALL_ROLE_SOURCES - called)


def test_opt_in_without_locations_runs_only_role_only_sources(
    monkeypatch, isolated_breakers
):
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "1")
    request = {k: v for k, v in _WIZARD.items() if k != "locations"}
    called = set(_dispatched(monkeypatch, request))
    assert set(api_enrichment.ROLE_ONLY_SOURCE_LABELS) <= called
    assert not set(api_enrichment.ROLE_AND_LOCATION_SOURCE_LABELS) & called


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "none", "nope"])
def test_anything_else_is_off(monkeypatch, value):
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", value)
    assert api_enrichment.role_sources_enabled() is False


def test_empty_value_means_the_default_not_off(monkeypatch):
    """Was in ``test_anything_else_is_off``: an empty value used to be off."""
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "")
    assert api_enrichment.role_sources_enabled() is True


def test_explicit_roles_key_follows_the_same_allowlist(monkeypatch, isolated_breakers):
    """Direct callers that send ``roles`` used to dispatch all 17 whatever the
    flag said; they now go through the allowlist like the wizard's key."""
    request = dict(_WIZARD, roles=_WIZARD["target_roles"])
    request.pop("target_roles")
    monkeypatch.delenv("ENRICH_ROLE_SOURCES", raising=False)
    default = set(_dispatched(monkeypatch, request))
    assert _ALL_ROLE_SOURCES & default == {"BLS", "Adzuna"}
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "all")
    assert _ALL_ROLE_SOURCES <= set(_dispatched(monkeypatch, request))


@pytest.mark.parametrize("with_locations", [True, False])
def test_declared_labels_match_what_roles_actually_dispatch(
    monkeypatch, isolated_breakers, with_locations
):
    """Drift guard: the "would have run" list is exactly the set of sources
    roles add to the dispatch (all 17 allowed, so nothing is filtered)."""
    monkeypatch.setenv("ENRICH_ROLE_SOURCES", "all")
    base = {k: v for k, v in _WIZARD.items() if k != "target_roles"}
    if not with_locations:
        base.pop("locations")
    without = set(_dispatched(monkeypatch, base))
    with_roles = set(_dispatched(monkeypatch, dict(base, roles=["Registered Nurse"])))
    assert with_roles - without == set(api_enrichment.role_sources_for(with_locations))
