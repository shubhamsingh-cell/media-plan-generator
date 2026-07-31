"""Integration tests for the S93 location-resolution wiring in app.py.

Covers three things:
  1. ``app._resolve_and_rewrite_locations`` -- the function that replaced
     the old hardcoded 32-entry ``_LOCATION_CORRECTIONS`` dict (S49 FIX /
     Issue 14) with a call into ``plan_location.resolve_locations``. Pins
     the exact contract: ``data["locations"]`` is rewritten to the
     canonical display form ONLY for resolved/corrected entries, ambiguous/
     unresolved entries are left byte-for-byte verbatim, the full
     structured result lands in the non-schema ``_location_resolution``
     sidecar, and any internal failure leaves ``data["locations"]``
     untouched and simply omits the sidecar (never breaks plan
     generation).
  2. That the sidecar key round-trips cleanly through
     ``plan_schema.validate_and_normalize`` without introducing new
     warnings (it's an unknown key, preserved via ``PlanData.extra``).
  3. The new ``POST /api/locations/resolve`` endpoint: happy path,
     over-cap, malformed JSON, wrong types, an XSS-ish string, and the
     CSRF behavior it shares with every other authenticated-adjacent
     ``/api/*`` POST route (double-submit cookie, see app.py's CSRF gate
     right before routing dispatch in ``_handle_POST``).

Live-HTTP tests use the same in-process ``app.ThreadedHTTPServer`` fixture
pattern as tests/test_naics_industry_select.py and
tests/test_bundle_qa_gate.py (duplicated, not imported, to keep this file
self-contained).
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from typing import Iterator

import pytest

import app as app_module
import plan_schema

# ---------------------------------------------------------------------------
# The 32-entry legacy dict this integration replaces. Copied verbatim from
# the pre-S93 app.py `_LOCATION_CORRECTIONS` (S49 FIX / Issue 14).
# ---------------------------------------------------------------------------
_LEGACY_LOCATION_CORRECTIONS = {
    "ithica": "Ithaca",
    "harrisburgh": "Harrisburg",
    "murfeesboro": "Murfreesboro",
    "pittsburg": "Pittsburgh",
    "albuquerque": "Albuquerque",
    "cincinatti": "Cincinnati",
    "colombus": "Columbus",
    "detriot": "Detroit",
    "houstan": "Houston",
    "philladelphia": "Philadelphia",
    "phoneix": "Phoenix",
    "sacremento": "Sacramento",
    "seatle": "Seattle",
    "tuscon": "Tucson",
    "milwakee": "Milwaukee",
    "minnapolis": "Minneapolis",
    "indianpolis": "Indianapolis",
    "nashvile": "Nashville",
    "lousville": "Louisville",
    "baltamore": "Baltimore",
    "richmand": "Richmond",
    "charlote": "Charlotte",
    "ralegh": "Raleigh",
    "memhpis": "Memphis",
    "knoxvile": "Knoxville",
    "cleavland": "Cleveland",
    "bufalo": "Buffalo",
    "rochestor": "Rochester",
    "siracuse": "Syracuse",
    "sprinfield": "Springfield",
    "talahassee": "Tallahassee",
    "jackonville": "Jacksonville",
}

# "pittsburg" and "albuquerque" are themselves real US place names (a real
# place in 5 states, and already-correctly-spelled, respectively) -- see
# plan_location.py's module docstring and tests/test_plan_location.py.
# They resolve via the exact bare-city lookup, not fuzzy correction, which
# is more honest than the legacy dict's blind overwrite. Pinned here as the
# ACTUAL (intentional) behavior, not fought.
_EXACT_MATCH_NOT_FUZZY = {"pittsburg", "albuquerque"}


# ═══════════════════════════════════════════════════════════════════════
# 1. app._resolve_and_rewrite_locations -- the sidecar + rewrite contract
# ═══════════════════════════════════════════════════════════════════════


def test_sidecar_populated_and_schema_validation_unaffected():
    data = {
        "client_name": "Acme Co",
        "industry": "general_entry_level",
        "roles": ["Warehouse Associate"],
        "locations": ["Cincinatti", "Springfield", "London, UK", "30303"],
        "budget": 20000,
    }
    app_module._resolve_and_rewrite_locations(data)

    assert "_location_resolution" in data
    assert len(data["_location_resolution"]) == 4
    assert [r["status"] for r in data["_location_resolution"]] == [
        "corrected",
        "ambiguous",
        "unresolved",
        "resolved",
    ]

    # plan_schema doesn't know this key -- it must round-trip through
    # PlanData.extra unchanged and introduce zero new warnings.
    baseline, baseline_warnings = plan_schema.validate_and_normalize(
        {k: v for k, v in data.items() if k != "_location_resolution"}
    )
    with_sidecar, sidecar_warnings = plan_schema.validate_and_normalize(data)

    assert sidecar_warnings == baseline_warnings
    assert with_sidecar["_location_resolution"] == data["_location_resolution"]
    # Nothing else in the normalized payload shifted because of the extra key.
    for key in ("client_name", "industry", "roles", "locations", "budget"):
        assert with_sidecar[key] == baseline[key]


def test_cincinatti_corrected_to_canonical_display():
    data = {"locations": ["Cincinatti"]}
    app_module._resolve_and_rewrite_locations(data)
    assert data["locations"] == ["Cincinnati, OH"]
    assert data["_location_resolution"][0]["status"] == "corrected"


def test_springfield_ambiguous_stays_verbatim():
    """Anti-corruption regression pin: plan_location.resolve_location has
    to pick SOME primary for an ambiguous bare city (alphabetical
    tiebreak -> Springfield, AR), but that guessed primary must NEVER be
    written into data["locations"] -- only the sidecar sees it. Silently
    rewriting a user's "Springfield" to "Springfield, AR" (or any other
    state) would be confidently-wrong data corruption."""
    data = {"locations": ["Springfield"]}
    app_module._resolve_and_rewrite_locations(data)

    assert data["locations"] == ["Springfield"], (
        "ambiguous location must be left byte-for-byte verbatim, "
        f"got {data['locations']!r}"
    )
    sidecar = data["_location_resolution"][0]
    assert sidecar["status"] == "ambiguous"
    assert sidecar["input"] == "Springfield"
    # The tiebreak still picked SOME primary internally -- just never
    # surfaced into the plan string.
    assert sidecar["display_name"]
    assert len(sidecar["alternatives"]) >= 2


def test_london_uk_unresolved_and_plan_unaffected():
    data = {
        "client_name": "Acme Co",
        "roles": ["Analyst"],
        "locations": ["London, UK"],
        "budget": 10000,
    }
    app_module._resolve_and_rewrite_locations(data)

    assert data["locations"] == ["London, UK"]
    sidecar = data["_location_resolution"][0]
    assert sidecar["status"] == "unresolved"
    assert sidecar["kind"] == "unknown"

    # Plan normalization proceeds normally -- geo failure/out-of-scope never
    # breaks the pipeline.
    normalized, warnings = plan_schema.validate_and_normalize(data)
    assert normalized["locations"] == ["London, UK"]
    assert "no locations provided" not in warnings


def test_cbsa_field_contract_for_locationresolution_frontend_row():
    """Pins the exact data contract `_buildLocationRow` (templates/partials/
    index/body_app_js.html) switches on for its "Metro area: {cbsa_title}"
    secondary line. This suite has no JS execution harness, so this test
    pins the backend contract the renderer depends on instead of the DOM
    output itself -- see that file's `_buildLocationRow` for the actual
    (createElement + textContent, never innerHTML) DOM-building code, and
    the manual live-render check in this branch's validation notes.

    The render rule in `_buildLocationRow`: append the metro line ONLY
    inside the `status === "resolved" || status === "corrected"` branch,
    and only when `res.cbsa_status === "available" && res.cbsa_title`.
    All four cases below are real resolutions, not synthetic stand-ins --
    see plan_location.py's `_apply_cbsa` and tests/test_plan_location.py's
    CBSA suite for the underlying pins.
    """
    data = {
        "locations": [
            "Atlanta, GA",  # known metro -- resolved, real CBSA
            "Bullock County, AL",  # resolved, but genuinely outside any CBSA
            "pittsburg",  # ambiguous -- tiebreak pick DOES sit in a real
            # CBSA internally; must stay hidden
            "London, UK",  # unresolved -- no county at all
        ]
    }
    app_module._resolve_and_rewrite_locations(data)
    sidecar = data["_location_resolution"]
    assert len(sidecar) == 4
    atlanta, bullock, pittsburg, london = sidecar

    # 1. Known metro, resolved -- the metro line renders.
    assert atlanta["status"] in ("resolved", "corrected")
    assert atlanta["cbsa_status"] == "available"
    assert atlanta["cbsa_title"] and "Atlanta" in atlanta["cbsa_title"]

    # 2. Resolved but genuinely outside any CBSA (rural county) -- silent,
    #    the same rule as the existing missing-county_name case.
    assert bullock["status"] == "resolved"
    assert bullock["cbsa_status"] == "unavailable"
    assert bullock["cbsa_title"] is None

    # 3. THE TRAP: an ambiguous bare-city resolution still runs its
    #    internally-picked tiebreak primary through _apply_cbsa, so
    #    cbsa_status/cbsa_title CAN be populated on an ambiguous row.
    #    "pittsburg"'s alphabetical tiebreak lands on Pittsburg, CA, which
    #    really does sit inside a real CBSA -- proving the frontend's
    #    status gate is load-bearing, not a no-op over an always-empty
    #    field.
    assert pittsburg["status"] == "ambiguous"
    assert pittsburg["cbsa_status"] == "available"
    assert pittsburg["cbsa_title"]  # populated internally...
    # ...but must never surface: _buildLocationRow only reads cbsa_title
    # inside the resolved/corrected branch, never the ambiguous one.

    # 4. Unresolved -- no county at all, cbsa_status stays unavailable.
    assert london["status"] == "unresolved"
    assert london["cbsa_status"] == "unavailable"
    assert london["cbsa_title"] is None


@pytest.mark.parametrize("misspelled,expected_city", sorted(_LEGACY_LOCATION_CORRECTIONS.items()))
def test_all_32_legacy_misspellings_still_resolve(misspelled, expected_city):
    data = {"locations": [misspelled]}
    app_module._resolve_and_rewrite_locations(data)
    sidecar = data["_location_resolution"][0]

    if misspelled in _EXACT_MATCH_NOT_FUZZY:
        # Real US place names in their own right -- resolved via the exact
        # bare-city lookup (rule 5), not fuzzy correction (rule 9). For
        # "pittsburg" that means ambiguous (5 states share the name) and
        # the ORIGINAL spelling is kept verbatim; for "albuquerque" it's a
        # unique exact match (resolved), and the corrected-case-only
        # rewrite ("Albuquerque") is applied since status == "resolved".
        if misspelled == "pittsburg":
            assert sidecar["status"] == "ambiguous"
            assert data["locations"] == ["pittsburg"]
        else:  # albuquerque
            assert sidecar["status"] == "resolved"
            assert data["locations"] == ["Albuquerque, NM"]
        return

    assert sidecar["status"] == "corrected", (
        f"{misspelled!r} -> status={sidecar['status']!r} (expected 'corrected')"
    )
    assert sidecar["display_name"] == expected_city, (
        f"{misspelled!r} -> {sidecar['display_name']!r}, expected {expected_city!r}"
    )
    assert data["locations"] == [f"{expected_city}, {sidecar['state_usps']}"]


def test_failure_isolation_leaves_locations_untouched(monkeypatch):
    """A geo failure must never break plan generation: data["locations"]
    stays exactly as it was, and the sidecar is simply omitted."""

    def _boom(_raw_list):
        raise RuntimeError("simulated plan_location outage")

    monkeypatch.setattr(app_module.plan_location, "resolve_locations", _boom)

    data = {
        "client_name": "Acme Co",
        "roles": ["Analyst"],
        "locations": ["Cincinatti", "Atlanta, GA"],
        "budget": 10000,
    }
    app_module._resolve_and_rewrite_locations(data)

    assert data["locations"] == ["Cincinatti", "Atlanta, GA"]
    assert "_location_resolution" not in data

    # Plan generation still proceeds normally afterward.
    normalized, _warnings = plan_schema.validate_and_normalize(data)
    assert normalized["locations"] == ["Cincinatti", "Atlanta, GA"]


def test_plan_location_module_unavailable_is_a_noop(monkeypatch):
    """If plan_location failed to import (plan_location is None at module
    scope), the block must no-op rather than crash."""
    monkeypatch.setattr(app_module, "plan_location", None)
    data = {"locations": ["Cincinatti"]}
    app_module._resolve_and_rewrite_locations(data)
    assert data["locations"] == ["Cincinatti"]
    assert "_location_resolution" not in data


def test_empty_and_missing_locations_are_noops():
    data_empty = {"locations": []}
    app_module._resolve_and_rewrite_locations(data_empty)
    assert data_empty["locations"] == []
    assert "_location_resolution" not in data_empty

    data_missing = {}
    app_module._resolve_and_rewrite_locations(data_missing)
    assert "locations" not in data_missing
    assert "_location_resolution" not in data_missing


# ═══════════════════════════════════════════════════════════════════════
# 2. POST /api/locations/resolve -- live HTTP endpoint
# ═══════════════════════════════════════════════════════════════════════


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-locresolve-http-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("live_server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


def _http(port: int, method: str, path: str, body=None, headers=None, timeout=10.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        data = None
        if body is not None:
            data = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode("utf-8")
        conn.request(method, path, body=data, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, dict(resp.getheaders()), raw
    finally:
        conn.close()


# CSRF finding (see app.py's `_handle_POST`, right before routing dispatch):
# the double-submit-cookie gate applies to EVERY path starting with "/api/"
# unless it's in `_CSRF_EXEMPT_PATHS` or the caller presents a Bearer token /
# admin auth. "/api/locations/resolve" was deliberately NOT added to that
# exempt list, so it validates CSRF exactly like its unauthenticated-adjacent
# neighbors (e.g. "/api/roi/calculate") -- no silent CSRF-exempt hole. The
# double-submit check itself is a pure cookie==header equality + embedded-
# expiry check (see `_validate_csrf_double_submit`), not a signature, so
# tests can mint their own token string as long as cookie and header match.
_CSRF = f"loc-resolve-test-token.{int(time.time()) + 3600}"
_CSRF_HEADERS = {
    "Content-Type": "application/json",
    "Cookie": f"csrf_token={_CSRF}",
    "X-CSRF-Token": _CSRF,
}


class TestLocationsResolveEndpoint:
    def test_happy_path_list(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"locations": ["Atlanta", "30303", "Cincinatti"]},
            headers=_CSRF_HEADERS,
        )
        assert status == 200, raw
        resp = json.loads(raw)
        assert resp["status"] == "ok"
        assert len(resp["resolutions"]) == 3
        assert resp["resolutions"][0]["input"] == "Atlanta"
        assert resp["resolutions"][1]["zip5"] == "30303"
        assert resp["resolutions"][2]["status"] == "corrected"
        assert resp["resolutions"][2]["display_name"] == "Cincinnati"

    def test_bare_string_input_accepted(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body="Atlanta",
            headers=_CSRF_HEADERS,
        )
        assert status == 200, raw
        resp = json.loads(raw)
        assert len(resp["resolutions"]) == 1
        assert resp["resolutions"][0]["input"] == "Atlanta"

    def test_single_location_key_accepted(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"location": "30303"},
            headers=_CSRF_HEADERS,
        )
        assert status == 200, raw
        resp = json.loads(raw)
        assert len(resp["resolutions"]) == 1
        assert resp["resolutions"][0]["zip5"] == "30303"

    def test_over_cap_count_rejected_400(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"locations": ["Atlanta"] * 51},
            headers=_CSRF_HEADERS,
        )
        assert status == 400, raw
        resp = json.loads(raw)
        assert "error" in resp or resp.get("success") is False

    def test_over_cap_length_rejected_400(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"locations": ["x" * 201]},
            headers=_CSRF_HEADERS,
        )
        assert status == 400, raw

    def test_malformed_json_rejected_400_not_500(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body=b"{not valid json!!",
            headers=_CSRF_HEADERS,
        )
        assert status == 400, raw

    @pytest.mark.parametrize(
        "bad_body",
        [
            b"null",
            b"5",
            b'{"locations": 5}',
            b'{"locations": [1, 2, 3]}',
            b'{"locations": [["nested", "list"]]}',
            b'{"locations": null}',
            b"[]",
        ],
    )
    def test_wrong_types_rejected_400_not_500(self, live_server, bad_body):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body=bad_body,
            headers=_CSRF_HEADERS,
        )
        assert status == 400, raw

    def test_xss_ish_string_is_sanitized_not_reflected(self, live_server):
        payload = "<script>alert(1)</script>Atlanta"
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"locations": [payload]},
            headers=_CSRF_HEADERS,
        )
        assert status == 200, raw
        assert b"<script>" not in raw
        resp = json.loads(raw)
        # Tags stripped, "Atlanta" survives as the resolvable remainder.
        assert "<" not in resp["resolutions"][0]["input"]
        assert "script" not in resp["resolutions"][0]["input"].lower() or (
            "alert" not in resp["resolutions"][0]["input"]
        )

    def test_missing_csrf_rejected_403(self, live_server):
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"locations": ["Atlanta"]},
            headers={"Content-Type": "application/json"},
        )
        assert status == 403, raw

    def test_mismatched_csrf_rejected_403(self, live_server):
        headers = {
            "Content-Type": "application/json",
            "Cookie": "csrf_token=one-token.9999999999",
            "X-CSRF-Token": "a-different-token.9999999999",
        }
        status, _, raw = _http(
            live_server,
            "POST",
            "/api/locations/resolve",
            body={"locations": ["Atlanta"]},
            headers=headers,
        )
        assert status == 403, raw
